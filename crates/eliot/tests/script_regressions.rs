#![cfg(any(windows, target_os = "linux"))]

mod support;

use eliot_core::api::{
    AgentResultRequest, AgentResultResponse, ProjectRegistrationView, RuleStateView,
};
use eliot_core::{
    Command as CoreCommand, CommandState, ScriptCustody, ScriptDirectExit, ScriptRunApplied,
    Turn as CoreTurn, TurnState,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};
use support::{CLI, Fixture, cli};

const PROJECT_ID: &str = "script-fixture";
const RULE_ID: &str = "project-script-on-completed";
const GLOBAL_RULE_ID: &str = "global-script-on-completed";
const SEND_TEXT: &str = "script-event-send";
const INVALID_SEND_TEXT: &str = "must-not-dispatch";

#[cfg(windows)]
const SCRIPT_FIXTURE: &str = include_str!("fixtures/scripts/project_run.ps1");
#[cfg(target_os = "linux")]
const SCRIPT_FIXTURE: &str = include_str!("fixtures/scripts/project_run.sh");

struct ApprovedInterpreter {
    reference: String,
    program: String,
    argv_prefix: Vec<String>,
    entry: &'static str,
}

#[cfg(windows)]
fn approved_interpreter() -> ApprovedInterpreter {
    let path = std::env::var_os("PATH").expect("Windows test PATH");
    let program = std::env::split_paths(&path)
        .map(|directory| directory.join("pwsh.exe"))
        .find(|candidate| candidate.is_file())
        .or_else(|| {
            std::env::var_os("ProgramFiles")
                .map(PathBuf::from)
                .map(|program_files| program_files.join("PowerShell").join("7").join("pwsh.exe"))
                .filter(|candidate| candidate.is_file())
        })
        .expect("PowerShell 7 pwsh.exe is required for the Windows script contour");
    let program = fs::canonicalize(program).expect("canonical pwsh.exe path");
    ApprovedInterpreter {
        reference: "pwsh".to_owned(),
        program: program.to_string_lossy().into_owned(),
        argv_prefix: vec![
            "-NoProfile".to_owned(),
            "-NonInteractive".to_owned(),
            "-File".to_owned(),
        ],
        entry: ".eliot/scripts/project_run.ps1",
    }
}

#[cfg(target_os = "linux")]
fn approved_interpreter() -> ApprovedInterpreter {
    let program = fs::canonicalize("/bin/sh").expect("canonical /bin/sh interpreter path");
    assert!(program.is_file(), "/bin/sh interpreter is required");
    ApprovedInterpreter {
        reference: "sh".to_owned(),
        program: program.to_string_lossy().into_owned(),
        argv_prefix: Vec::new(),
        entry: ".eliot/scripts/project_run.sh",
    }
}

fn toml_string(value: &str) -> String {
    serde_json::to_string(value).expect("encode TOML basic string")
}

fn toml_array(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|value| toml_string(value))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn setup_registered_project(fixture: &mut Fixture) -> ApprovedInterpreter {
    let interpreter = approved_interpreter();
    assert!(
        fixture
            .git(&["init", "--initial-branch=main"])
            .status
            .success(),
        "initialize registered Git project"
    );
    fs::write(fixture.workspace.join("seed.txt"), "script fixture\n").expect("write project seed");
    let scripts = fixture.workspace.join(".eliot").join("scripts");
    fs::create_dir_all(&scripts).expect("create registered script directory");
    fs::write(fixture.workspace.join(interpreter.entry), SCRIPT_FIXTURE)
        .expect("install script fixture");
    fs::create_dir_all(fixture.workspace.join(".eliot").join("rules"))
        .expect("create watched project rule directory");
    assert!(fixture.git(&["add", "--all"]).status.success());
    assert!(
        fixture
            .git(&["commit", "-m", "Project script fixture"])
            .status
            .success(),
        "commit registered project fixture"
    );

    // Initialize the private data root before installing owner project approvals.
    fixture.start();
    fixture.stop(false);
    let config = format!(
        concat!(
            "[projects.{project_id}]\n",
            "path = {path}\n",
            "profiles = [\"fake\"]\n",
            "actions = [\"run\", \"send\", \"stop\"]\n",
            "run_env = [\"CI\"]\n\n",
            "[projects.{project_id}.interpreters.{reference}]\n",
            "program = {program}\n",
            "argv_prefix = {argv_prefix}\n",
        ),
        path = toml_string(&fixture.workspace.display().to_string()),
        project_id = PROJECT_ID,
        reference = interpreter.reference,
        program = toml_string(&interpreter.program),
        argv_prefix = toml_array(&interpreter.argv_prefix),
    );
    fs::write(fixture.data.join("config.toml"), config)
        .expect("write owner project registration and interpreter grant");
    fixture.start();

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let registration: ProjectRegistrationView = loop {
        let reply = cli(&fixture.data, &["project", "show", PROJECT_ID]);
        if reply.status.success() {
            break serde_json::from_str(&reply.stdout).expect("typed project registration");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "project registration readback deadline; stderr={:?}",
            reply.stderr.chars().take(2048).collect::<String>()
        );
        thread::sleep(Duration::from_millis(25));
    };
    registration.validate().expect("valid project registration");
    assert_eq!(registration.id, PROJECT_ID);
    assert_eq!(
        PathBuf::from(&registration.checkout),
        fs::canonicalize(&fixture.workspace).expect("canonical registered checkout")
    );
    assert_eq!(registration.profiles, vec!["fake".to_owned()]);
    assert_eq!(
        registration.actions,
        vec!["run".to_owned(), "send".to_owned(), "stop".to_owned()]
    );
    assert_eq!(registration.run_env, vec!["CI".to_owned()]);
    let grant = registration
        .interpreters
        .get(&interpreter.reference)
        .expect("owner-approved interpreter reference");
    assert_eq!(grant.program, interpreter.program);
    assert_eq!(grant.argv_prefix, interpreter.argv_prefix);
    interpreter
}

fn setup_global_run_fixture(fixture: &mut Fixture, interpreter: &ApprovedInterpreter) -> PathBuf {
    assert!(
        fixture
            .git(&["init", "--initial-branch=main"])
            .status
            .success(),
        "initialize owner workspace"
    );
    fs::write(
        fixture.workspace.join("seed.txt"),
        "global script fixture\n",
    )
    .expect("write owner workspace seed");
    assert!(fixture.git(&["add", "--all"]).status.success());
    assert!(
        fixture
            .git(&["commit", "-m", "Global script fixture"])
            .status
            .success(),
        "commit owner workspace fixture"
    );

    // Initialize the private data root before installing owner Run approval.
    fixture.start();
    fixture.stop(false);

    let scripts = fixture.data.join("scripts");
    fs::create_dir_all(&scripts).expect("create private global script directory");
    fs::create_dir_all(fixture.data.join("rules")).expect("create private global rule directory");
    let entry = interpreter
        .entry
        .rsplit('/')
        .next()
        .expect("script entry filename");
    fs::write(scripts.join(entry), SCRIPT_FIXTURE).expect("install global script fixture");
    write_global_run_config(fixture, interpreter, true);
    fixture.start();

    fs::canonicalize(scripts).expect("canonical private global script root")
}

fn write_global_run_config(
    fixture: &Fixture,
    interpreter: &ApprovedInterpreter,
    approve_interpreter: bool,
) {
    let interpreter_table = if approve_interpreter {
        format!(
            "\n[automation.run.interpreters.{}]\nprogram = {}\nargv_prefix = {}\n",
            interpreter.reference,
            toml_string(&interpreter.program),
            toml_array(&interpreter.argv_prefix),
        )
    } else {
        String::new()
    };
    let config = format!("[automation.run]\nrun_env = [\"CI\"]\n{interpreter_table}");
    fs::write(fixture.data.join("config.toml"), config).expect("write owner global Run grant");
}

struct RunRuleCase<'a> {
    mode: &'a str,
    agent: &'a str,
    generation: i64,
    trigger_command_id: &'a str,
    match_only_trigger: bool,
    context_path: &'a std::path::Path,
    own_command_path: &'a std::path::Path,
}

fn write_run_rule(fixture: &Fixture, interpreter: &ApprovedInterpreter, case: RunRuleCase<'_>) {
    let generation = case.generation.to_string();
    let arguments = vec![
        interpreter.reference.clone(),
        interpreter.entry.to_owned(),
        case.mode.to_owned(),
        CLI.to_owned(),
        fixture.data.display().to_string(),
        case.agent.to_owned(),
        generation,
        case.trigger_command_id.to_owned(),
        case.context_path.display().to_string(),
        case.own_command_path.display().to_string(),
    ];
    let trigger_match = if case.match_only_trigger {
        format!(
            ", \"event.data.command_id\" = {}",
            toml_string(case.trigger_command_id)
        )
    } else {
        String::new()
    };
    let source = format!(
        concat!(
            "[[rule]]\n",
            "id = \"{rule_id}\"\n",
            "on = {{ event = \"turn.completed\", match = {{ \"subject.harness\" = \"fake\"{trigger_match} }} }}\n",
            "do = [{{ action = \"run\", cmd = {cmd}, env = {{ CI = \"script-approved\" }} }}]\n",
        ),
        trigger_match = trigger_match,
        cmd = toml_array(&arguments),
        rule_id = RULE_ID,
    );
    fs::write(
        fixture
            .workspace
            .join(".eliot")
            .join("rules")
            .join("project-script.toml"),
        source,
    )
    .expect("write project event rule");

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let reply = cli(
            &fixture.data,
            &["rule", "show", RULE_ID, "--project", PROJECT_ID],
        );
        if reply.status.success() {
            let state: RuleStateView =
                serde_json::from_str(&reply.stdout).expect("typed project rule state");
            assert_eq!(state.id, RULE_ID);
            assert_eq!(state.project.as_deref(), Some(PROJECT_ID));
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "project rule load deadline; stderr={:?}",
            reply.stderr.chars().take(2048).collect::<String>()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

struct GlobalRunRuleCase<'a> {
    agent: &'a str,
    generation: i64,
    trigger_command_id: &'a str,
    context_path: &'a Path,
    own_command_path: &'a Path,
    cwd: &'a Path,
}

fn write_global_run_rule(
    fixture: &Fixture,
    interpreter: &ApprovedInterpreter,
    case: GlobalRunRuleCase<'_>,
) {
    let entry = interpreter
        .entry
        .rsplit('/')
        .next()
        .expect("script entry filename");
    let arguments = vec![
        interpreter.reference.clone(),
        entry.to_owned(),
        "success".to_owned(),
        CLI.to_owned(),
        fixture.data.display().to_string(),
        case.agent.to_owned(),
        case.generation.to_string(),
        case.trigger_command_id.to_owned(),
        case.context_path.display().to_string(),
        case.own_command_path.display().to_string(),
        case.cwd.display().to_string(),
    ];
    let source = format!(
        concat!(
            "[[rule]]\n",
            "id = \"{rule_id}\"\n",
            "on = {{ event = \"turn.completed\", match = {{ \"subject.harness\" = \"fake\" }} }}\n",
            "do = [{{ action = \"run\", cmd = {cmd}, cwd = {cwd}, env = {{ CI = \"script-approved\" }} }}]\n",
        ),
        cmd = toml_array(&arguments),
        cwd = toml_string(&case.cwd.display().to_string()),
        rule_id = GLOBAL_RULE_ID,
    );
    fs::write(
        fixture.data.join("rules").join("global-script.toml"),
        source,
    )
    .expect("write owner global event rule");
    wait_for_global_rule(fixture);
}

fn owner_send(fixture: &Fixture, id: &str, agent: &str, generation: i64, text: &str) -> CoreTurn {
    let generation_arg = generation.to_string();
    let value = fixture.json(&[
        "--id",
        id,
        "send",
        agent,
        "--generation",
        &generation_arg,
        "--wait",
        "--timeout-seconds",
        "10",
        "--",
        text,
    ]);
    let turn: CoreTurn = serde_json::from_value(value).expect("typed Fake send turn");
    assert_eq!(turn.agent, agent);
    assert_eq!(turn.generation, generation);
    assert_eq!(turn.state, TurnState::Completed);
    assert_eq!(turn.final_text.as_deref(), Some(text));
    turn
}

fn exact_result(
    fixture: &Fixture,
    agent: &str,
    generation: i64,
    command: &CoreCommand,
    expected_text: &str,
) -> CoreTurn {
    assert_eq!(command.verb, "send");
    assert_eq!(command.target.as_deref(), Some(agent));
    assert_eq!(command.expected_generation, Some(generation));
    assert_eq!(command.generation, Some(generation));
    let turn_id = command.turn_id.as_deref().expect("send turn binding");
    let generation_arg = generation.to_string();
    let result_request = AgentResultRequest {
        id: agent.to_owned(),
        generation: Some(generation),
        turn_id: Some(turn_id.to_owned()),
        limit: 1,
    };
    let result: AgentResultResponse = serde_json::from_value(fixture.json(&[
        "result",
        agent,
        "--generation",
        &generation_arg,
        "--turn-id",
        turn_id,
        "--limit",
        "1",
    ]))
    .expect("typed exact Fake turn result");
    result_request
        .validate_response(&result)
        .expect("send and result retain the exact turn binding");
    assert_eq!(result.turns.len(), 1);
    let turn = result.turns[0].clone();
    assert_eq!(turn.turn_id, turn_id);
    assert_eq!(turn.agent, agent);
    assert_eq!(turn.generation, generation);
    assert_eq!(turn.state, TurnState::Completed);
    assert_eq!(turn.final_text.as_deref(), Some(expected_text));
    turn
}

fn wait_for_firing(fixture: &Fixture, trigger_command_id: &str) -> (Value, Vec<Value>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let events = fixture.events();
        if let Some(firing) = events.iter().find(|event| {
            event["kind"] == "rule.fired"
                && event["data"]["rule"] == RULE_ID
                && event["data"]["project"] == PROJECT_ID
        }) {
            let trigger = events.iter().find(|event| {
                event["kind"] == "turn.completed"
                    && event["data"]["command_id"] == trigger_command_id
            });
            if let Some(trigger) = trigger {
                assert_eq!(firing["data"]["event_seq"], trigger["seq"]);
                return (firing.clone(), events);
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "project event rule firing deadline"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_global_rule(fixture: &Fixture) -> RuleStateView {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let reply = cli(&fixture.data, &["rule", "show", GLOBAL_RULE_ID]);
        if reply.status.success() {
            let state: RuleStateView =
                serde_json::from_str(&reply.stdout).expect("typed global rule state");
            assert_eq!(state.id, GLOBAL_RULE_ID);
            assert_eq!(state.project, None);
            return state;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "global event rule load deadline; stderr={:?}",
            reply.stderr.chars().take(2048).collect::<String>()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_global_firing(fixture: &Fixture, trigger_command_id: &str) -> (Value, Vec<Value>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let events = fixture.events();
        let trigger_seq = events
            .iter()
            .find(|event| {
                event["kind"] == "turn.completed"
                    && event["data"]["command_id"] == trigger_command_id
            })
            .and_then(|event| event["seq"].as_i64());
        if let Some(trigger_seq) = trigger_seq
            && let Some(firing) = events.iter().find(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == GLOBAL_RULE_ID
                    && event["data"]["event_seq"] == trigger_seq
            })
        {
            return (firing.clone(), events);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "global event rule firing deadline"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_rule_cursor(fixture: &Fixture, through: i64) -> Vec<Value> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let events = fixture.events();
        let reply = cli(
            &fixture.data,
            &["rule", "show", RULE_ID, "--project", PROJECT_ID],
        );
        if reply.status.success() {
            let state: RuleStateView =
                serde_json::from_str(&reply.stdout).expect("typed project rule cursor");
            if state.current_cursor >= through {
                return events;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "project rule cursor did not reach event sequence {through}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_global_rule_cursor(fixture: &Fixture, through: i64) -> Vec<Value> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let events = fixture.events();
        let reply = cli(&fixture.data, &["rule", "show", GLOBAL_RULE_ID]);
        if reply.status.success() {
            let state: RuleStateView =
                serde_json::from_str(&reply.stdout).expect("typed global rule cursor");
            assert_eq!(state.project, None);
            if state.current_cursor >= through {
                return events;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "global rule cursor did not reach event sequence {through}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_turn_terminal(
    fixture: &Fixture,
    agent: &str,
    generation: i64,
    command_id: &str,
    turn_id: &str,
) -> (i64, Vec<Value>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let events = fixture.events();
        if let Some(sequence) = events
            .iter()
            .find(|event| {
                event["kind"] == "turn.completed"
                    && event["agent"] == agent
                    && event["generation"] == generation
                    && event["data"]["command_id"] == command_id
                    && event["data"]["turn_id"] == turn_id
            })
            .and_then(|event| event["seq"].as_i64())
        {
            return (sequence, events);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "Fake turn terminal event deadline for command {command_id}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn command_events<'a>(events: &'a [Value], kind: &str, command_id: &str) -> Vec<&'a Value> {
    events
        .iter()
        .filter(|event| event["kind"] == kind && event["data"]["command_id"] == command_id)
        .collect()
}

fn wait_for_run_outcome(fixture: &Fixture, run_id: &str) -> CoreCommand {
    serde_json::from_value(fixture.wait_command(run_id, "terminal"))
        .expect("typed settled Run command")
}

fn run_outcome_code(command: &CoreCommand, field: &str) -> String {
    command
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get(field))
        .and_then(Value::as_str)
        .filter(|value| {
            value.len() <= 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        })
        .unwrap_or("missing")
        .to_owned()
}

fn assert_run_state(command: &CoreCommand, expected: CommandState) {
    if command.state == expected {
        return;
    }

    let process = command
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("process"));
    let direct_exit = process.and_then(|process| process.get("direct_exit"));
    let stderr = process.and_then(|process| process.get("stderr"));
    let quiescent = process
        .and_then(|process| process.get("quiescent"))
        .and_then(Value::as_bool)
        .map_or_else(|| "missing".to_owned(), |value| value.to_string());
    let exit_success = direct_exit
        .and_then(|exit| exit.get("success"))
        .and_then(Value::as_bool)
        .map_or_else(|| "missing".to_owned(), |value| value.to_string());
    let exit_code = direct_exit
        .and_then(|exit| exit.get("exit_code"))
        .and_then(Value::as_i64)
        .map_or_else(|| "none".to_owned(), |value| value.to_string());
    let stderr_flag = |name| {
        stderr
            .and_then(|snapshot| snapshot.get(name))
            .and_then(Value::as_bool)
            .map_or_else(|| "missing".to_owned(), |value| value.to_string())
    };
    let custody = process
        .and_then(|process| process.get("custody"))
        .and_then(Value::as_str)
        .filter(|value| matches!(*value, "exact" | "degraded" | "unknown"))
        .unwrap_or("other");
    let exit_state = direct_exit
        .and_then(|exit| exit.get("state"))
        .and_then(Value::as_str)
        .filter(|value| matches!(*value, "unknown" | "exited"))
        .unwrap_or("other");

    panic!(
        "Run state mismatch: expected={expected:?} actual={:?} code={} cause={} custody={custody} quiescent={quiescent} direct_exit_state={exit_state} direct_exit_success={exit_success} direct_exit_code={exit_code} stderr_present={} stderr_truncated={} stderr_eof={} stderr_read_error={}",
        command.state,
        run_outcome_code(command, "code"),
        run_outcome_code(command, "cause"),
        stderr.is_some_and(|snapshot| !snapshot.is_null()),
        stderr_flag("truncated"),
        stderr_flag("eof"),
        stderr_flag("read_error"),
    );
}

fn script_context(
    path: &std::path::Path,
    trigger_command_id: &str,
    run_id: &str,
    agent: &str,
    generation: i64,
    expected_rule_id: &str,
) -> Value {
    let bytes = fs::read(path).expect("script's bounded stdin artifact");
    assert!(
        bytes.len() <= 1_048_576,
        "script stdin exceeded the 1 MiB bound"
    );
    let context: Value = serde_json::from_slice(&bytes).expect("script stdin JSON context");
    assert_eq!(context.as_object().expect("script input object").len(), 4);
    assert_eq!(context["event"]["kind"], "turn.completed");
    assert_eq!(context["event"]["data"]["command_id"], trigger_command_id);
    assert_eq!(context["event"]["agent"], agent);
    assert_eq!(context["event"]["generation"], generation);
    assert_eq!(context["subject"]["harness"], "fake");
    assert_eq!(context["rule"]["id"], expected_rule_id);
    assert_eq!(context["command"]["id"], run_id);
    assert_eq!(context["command"]["verb"], "run");
    context
}

#[test]
fn project_event_run_reads_own_command_and_admits_one_scoped_send() {
    let mut fixture = Fixture::new("project script run success");
    let interpreter = setup_registered_project(&mut fixture);
    let launch = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().expect("workspace path"),
    ]);
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let trigger_id = ulid::Ulid::new().to_string();
    let context_path = fixture.root.join("script context.json");
    let own_command_path = fixture.root.join("own run command.json");
    write_run_rule(
        &fixture,
        &interpreter,
        RunRuleCase {
            mode: "success",
            agent: &agent,
            generation,
            trigger_command_id: &trigger_id,
            match_only_trigger: false,
            context_path: &context_path,
            own_command_path: &own_command_path,
        },
    );

    let trigger = owner_send(&fixture, &trigger_id, &agent, generation, "script-trigger");
    let (firing, _) = wait_for_firing(&fixture, &trigger_id);
    let run_id = firing["data"]["child_command"]
        .as_str()
        .expect("firing's Run command")
        .to_owned();
    let rule_parent_id = firing["data"]["parent_command"]
        .as_str()
        .expect("firing's retained parent")
        .to_owned();
    let project_principal = format!("project:{PROJECT_ID}");

    let rule_parent: CoreCommand =
        serde_json::from_value(fixture.wait_command(&rule_parent_id, "applied"))
            .expect("typed project rule parent");
    assert_eq!(rule_parent.verb, "rule");
    assert_eq!(rule_parent.principal, project_principal);
    let rule_children = rule_parent
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("children"))
        .and_then(Value::as_array)
        .expect("retained rule action child set");
    assert_eq!(rule_children.len(), 1);
    assert_eq!(rule_children[0].as_str(), Some(run_id.as_str()));

    let run = wait_for_run_outcome(&fixture, &run_id);
    assert_eq!(run.id, run_id);
    assert_eq!(run.verb, "run");
    assert_eq!(run.target, None);
    assert_eq!(run.expected_generation, None);
    assert_eq!(run.generation, None);
    assert_eq!(run.principal, project_principal);
    assert_run_state(&run, CommandState::Applied);
    let applied: ScriptRunApplied =
        serde_json::from_value(run.outcome.as_ref().expect("applied Run outcome").clone())
            .expect("typed applied Run outcome");
    applied.validate().expect("valid per-Run process result");
    assert_eq!(applied.children.len(), 1);
    #[cfg(windows)]
    assert_eq!(applied.process.custody, ScriptCustody::Exact);
    #[cfg(target_os = "linux")]
    assert_eq!(applied.process.custody, ScriptCustody::Degraded);
    assert!(applied.process.quiescent);
    assert_eq!(
        applied.process.direct_exit,
        ScriptDirectExit::Exited {
            success: true,
            exit_code: Some(0),
        }
    );
    let send_id = &applied.children[0];
    let sent: CoreCommand = serde_json::from_value(fixture.wait_command(send_id, "applied"))
        .expect("typed ordinary SEND child");
    assert_eq!(sent.verb, "send");
    assert_eq!(sent.principal, project_principal);
    assert_eq!(sent.target.as_deref(), Some(agent.as_str()));
    assert_eq!(sent.expected_generation, Some(generation));
    assert_eq!(sent.generation, Some(generation));
    assert_eq!(sent.args, json!({"text": SEND_TEXT}));
    let child_turn = exact_result(&fixture, &agent, generation, &sent, SEND_TEXT);
    assert_eq!(trigger.state, TurnState::Completed);

    let own_command: CoreCommand = serde_json::from_slice(
        &fs::read(&own_command_path).expect("Run's own command.get stdout capture"),
    )
    .expect("standard Core Command DTO from first Run RPC");
    assert_eq!(own_command.id, run_id);
    assert_eq!(own_command.verb, "run");
    assert_eq!(own_command.principal, project_principal);
    assert_eq!(own_command.target, None);
    script_context(
        &context_path,
        &trigger_id,
        &run_id,
        &agent,
        generation,
        RULE_ID,
    );

    let (child_terminal, _) =
        wait_for_turn_terminal(&fixture, &agent, generation, send_id, &child_turn.turn_id);
    let events = wait_for_rule_cursor(&fixture, child_terminal);
    let firings: Vec<_> = events
        .iter()
        .filter(|event| {
            event["kind"] == "rule.fired"
                && event["data"]["rule"] == RULE_ID
                && event["data"]["project"] == PROJECT_ID
        })
        .collect();
    assert_eq!(
        firings.len(),
        1,
        "default self-suppression prevents an auto-loop"
    );
    assert_eq!(firings[0]["data"]["child_command"], run_id);
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event["kind"] == "turn.completed"
                    && event["agent"] == agent
                    && event["generation"] == generation
            })
            .count(),
        2,
        "one trigger turn and one emitted SEND turn were completed"
    );
    assert_eq!(
        command_events(&events, "command.dispatched", &run_id).len(),
        1
    );
    assert_eq!(
        command_events(&events, "command.dispatched", send_id).len(),
        1
    );
    assert!(!events.iter().any(|event| {
        event["kind"] == "rule.rejected"
            && event["data"]["rule"] == RULE_ID
            && event["data"]["project"] == PROJECT_ID
            && event["data"]["event_seq"] == child_terminal
    }));

    let generation_arg = generation.to_string();
    let stop = fixture.json(&["stop", &agent, "--generation", &generation_arg]);
    let stop_id = stop["command_id"].as_str().expect("Fake stop command");
    fixture.wait_command(stop_id, "applied");
    fixture.stop(false);
    fixture.start();

    let retained_run: CoreCommand = serde_json::from_value(fixture.json(&["cmd", "show", &run_id]))
        .expect("typed Run readback after daemon restart");
    assert_eq!(retained_run.state, CommandState::Applied);
    let retained_applied: ScriptRunApplied = serde_json::from_value(
        retained_run
            .outcome
            .as_ref()
            .expect("retained applied Run outcome")
            .clone(),
    )
    .expect("retained typed Run process result");
    assert_eq!(retained_applied, applied);
    let events_after_restart = fixture.events();
    assert_eq!(
        events_after_restart
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == RULE_ID
                    && event["data"]["project"] == PROJECT_ID
            })
            .count(),
        1,
        "restart retains the original Run without refiring its event"
    );
    assert_eq!(
        command_events(&events_after_restart, "command.dispatched", &run_id).len(),
        1
    );
}

#[test]
fn global_event_run_reads_own_command_and_admits_one_owner_send() {
    let mut fixture = Fixture::new("global script run success");
    let interpreter = approved_interpreter();
    let scripts_root = setup_global_run_fixture(&mut fixture, &interpreter);
    let owner_cwd = fs::canonicalize(&fixture.workspace).expect("canonical owner cwd");
    assert!(owner_cwd.is_absolute());
    assert!(
        !owner_cwd.starts_with(&scripts_root),
        "owner cwd must be outside the private script-entry root"
    );

    let launch = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().expect("workspace path"),
    ]);
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let trigger_id = ulid::Ulid::new().to_string();
    let context_path = fixture.root.join("global script context.json");
    let own_command_path = fixture.root.join("global own run command.json");
    write_global_run_rule(
        &fixture,
        &interpreter,
        GlobalRunRuleCase {
            agent: &agent,
            generation,
            trigger_command_id: &trigger_id,
            context_path: &context_path,
            own_command_path: &own_command_path,
            cwd: &owner_cwd,
        },
    );

    let trigger = owner_send(
        &fixture,
        &trigger_id,
        &agent,
        generation,
        "global-script-trigger",
    );
    let (firing, _) = wait_for_global_firing(&fixture, &trigger_id);
    let run_id = firing["data"]["child_command"]
        .as_str()
        .expect("firing's Run command")
        .to_owned();
    let rule_parent_id = firing["data"]["parent_command"]
        .as_str()
        .expect("firing's retained parent")
        .to_owned();

    let rule_parent: CoreCommand =
        serde_json::from_value(fixture.wait_command(&rule_parent_id, "applied"))
            .expect("typed global rule parent");
    assert_eq!(rule_parent.verb, "rule");
    assert_eq!(rule_parent.principal, "owner");
    let rule_children = rule_parent
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("children"))
        .and_then(Value::as_array)
        .expect("retained global rule action child set");
    assert_eq!(rule_children.len(), 1);
    assert_eq!(rule_children[0].as_str(), Some(run_id.as_str()));

    let run = wait_for_run_outcome(&fixture, &run_id);
    assert_eq!(run.id, run_id);
    assert_eq!(run.verb, "run");
    assert_eq!(run.target, None);
    assert_eq!(run.expected_generation, None);
    assert_eq!(run.generation, None);
    assert_eq!(run.principal, "owner");
    assert_run_state(&run, CommandState::Applied);
    let applied: ScriptRunApplied =
        serde_json::from_value(run.outcome.as_ref().expect("applied Run outcome").clone())
            .expect("typed applied Run outcome");
    applied.validate().expect("valid per-Run process result");
    assert_eq!(applied.children.len(), 1);
    #[cfg(windows)]
    assert_eq!(applied.process.custody, ScriptCustody::Exact);
    #[cfg(target_os = "linux")]
    assert_eq!(applied.process.custody, ScriptCustody::Degraded);
    assert!(applied.process.quiescent);
    assert_eq!(
        applied.process.direct_exit,
        ScriptDirectExit::Exited {
            success: true,
            exit_code: Some(0),
        }
    );

    let send_id = &applied.children[0];
    let sent: CoreCommand = serde_json::from_value(fixture.wait_command(send_id, "applied"))
        .expect("typed ordinary owner SEND child");
    assert_eq!(sent.verb, "send");
    assert_eq!(sent.principal, "owner");
    assert_eq!(sent.target.as_deref(), Some(agent.as_str()));
    assert_eq!(sent.expected_generation, Some(generation));
    assert_eq!(sent.generation, Some(generation));
    assert_eq!(sent.args, json!({"text": SEND_TEXT}));
    let child_turn = exact_result(&fixture, &agent, generation, &sent, SEND_TEXT);
    assert_eq!(trigger.state, TurnState::Completed);

    let own_command: CoreCommand = serde_json::from_slice(
        &fs::read(&own_command_path).expect("Run's own command.get stdout capture"),
    )
    .expect("standard Core Command DTO from first Run RPC");
    assert_eq!(own_command.id, run_id);
    assert_eq!(own_command.verb, "run");
    assert_eq!(own_command.principal, "owner");
    assert_eq!(own_command.target, None);
    let context = script_context(
        &context_path,
        &trigger_id,
        &run_id,
        &agent,
        generation,
        GLOBAL_RULE_ID,
    );
    assert_eq!(context["event"]["seq"], firing["data"]["event_seq"]);

    let (child_terminal, _) =
        wait_for_turn_terminal(&fixture, &agent, generation, send_id, &child_turn.turn_id);
    let events = wait_for_global_rule_cursor(&fixture, child_terminal);
    let firings: Vec<_> = events
        .iter()
        .filter(|event| event["kind"] == "rule.fired" && event["data"]["rule"] == GLOBAL_RULE_ID)
        .collect();
    assert_eq!(
        firings.len(),
        1,
        "default self-suppression prevents a global Run auto-loop"
    );
    assert_eq!(firings[0]["data"]["child_command"], run_id);
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event["kind"] == "turn.completed"
                    && event["agent"] == agent
                    && event["generation"] == generation
            })
            .count(),
        2,
        "one trigger turn and one emitted SEND turn were completed"
    );
    assert_eq!(
        command_events(&events, "command.dispatched", &run_id).len(),
        1
    );
    assert_eq!(
        command_events(&events, "command.dispatched", send_id).len(),
        1
    );
    assert!(!events.iter().any(|event| {
        event["kind"] == "rule.rejected"
            && event["data"]["rule"] == GLOBAL_RULE_ID
            && event["data"]["event_seq"] == child_terminal
    }));

    let generation_arg = generation.to_string();
    let stop = fixture.json(&["stop", &agent, "--generation", &generation_arg]);
    let stop_id = stop["command_id"].as_str().expect("Fake stop command");
    fixture.wait_command(stop_id, "applied");
    fixture.stop(false);
    fixture.start();

    let retained_run: CoreCommand = serde_json::from_value(fixture.json(&["cmd", "show", &run_id]))
        .expect("typed global Run readback after daemon restart");
    assert_eq!(retained_run.state, CommandState::Applied);
    let retained_applied: ScriptRunApplied = serde_json::from_value(
        retained_run
            .outcome
            .as_ref()
            .expect("retained applied Run outcome")
            .clone(),
    )
    .expect("retained typed Run process result");
    assert_eq!(retained_applied, applied);
    let events_after_restart = fixture.events();
    assert_eq!(
        events_after_restart
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired" && event["data"]["rule"] == GLOBAL_RULE_ID
            })
            .count(),
        1,
        "restart retains the original global Run without refiring its event"
    );
    assert_eq!(
        command_events(&events_after_restart, "command.dispatched", &run_id).len(),
        1
    );
}

#[test]
fn global_event_run_history_catch_up_emits_one_send_and_replays_quietly() {
    let mut fixture = Fixture::new("global script history catch-up");
    let interpreter = approved_interpreter();
    let scripts_root = setup_global_run_fixture(&mut fixture, &interpreter);
    let owner_cwd = fs::canonicalize(&fixture.workspace).expect("canonical owner cwd");
    assert!(owner_cwd.is_absolute());
    assert!(!owner_cwd.starts_with(&scripts_root));

    let launch = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().expect("workspace path"),
    ]);
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let trigger_id = ulid::Ulid::new().to_string();
    let trigger = owner_send(
        &fixture,
        &trigger_id,
        &agent,
        generation,
        "global-history-trigger",
    );
    let (trigger_seq, events_before_rule) =
        wait_for_turn_terminal(&fixture, &agent, generation, &trigger_id, &trigger.turn_id);
    let trigger_event = events_before_rule
        .iter()
        .find(|event| event["seq"] == trigger_seq)
        .cloned()
        .expect("captured Fake terminal event");
    assert_eq!(trigger_event["kind"], "turn.completed");
    assert_eq!(trigger_event["data"]["command_id"], trigger_id);

    let context_path = fixture.root.join("global catch-up script context.json");
    let own_command_path = fixture.root.join("global catch-up own command.json");
    write_global_run_rule(
        &fixture,
        &interpreter,
        GlobalRunRuleCase {
            agent: &agent,
            generation,
            trigger_command_id: &trigger_id,
            context_path: &context_path,
            own_command_path: &own_command_path,
            cwd: &owner_cwd,
        },
    );
    let rule_state = wait_for_global_rule(&fixture);
    assert!(rule_state.current_cursor >= trigger_seq);

    let after_seq = trigger_seq - 1;
    let after_arg = after_seq.to_string();
    let through_arg = trigger_seq.to_string();
    let catchup_id = ulid::Ulid::new().to_string();
    let catchup_args = [
        "--id",
        catchup_id.as_str(),
        "rule",
        "catch-up",
        GLOBAL_RULE_ID,
        "--after",
        after_arg.as_str(),
        "--through",
        through_arg.as_str(),
    ];
    let catchup_reply = cli(&fixture.data, &catchup_args);
    assert!(
        catchup_reply.status.success(),
        "global history catch-up failed; bounded stdout={:?}; bounded stderr={:?}",
        catchup_reply.stdout.chars().take(2048).collect::<String>(),
        catchup_reply.stderr.chars().take(2048).collect::<String>()
    );
    let catchup: CoreCommand =
        serde_json::from_str(&catchup_reply.stdout).expect("typed global catch-up parent");
    assert_eq!(catchup.id, catchup_id);
    assert_eq!(catchup.verb, "rule.catch_up");
    assert_eq!(catchup.principal, "owner");
    assert_eq!(catchup.state, CommandState::Applied);
    assert_eq!(catchup.args["rule"], GLOBAL_RULE_ID);
    assert_eq!(catchup.args["after_seq"], after_seq);
    assert_eq!(catchup.args["through_seq"], trigger_seq);
    assert_eq!(catchup.args["revision"], rule_state.revision);
    assert_eq!(
        catchup.outcome.as_ref().expect("catch-up outcome")["children"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );

    let (firing, _) = wait_for_global_firing(&fixture, &trigger_id);
    assert_eq!(firing["data"]["parent_command"], catchup_id);
    assert_eq!(firing["data"]["event_seq"], trigger_seq);
    assert_eq!(firing["data"]["revision"], rule_state.revision);
    let run_id = firing["data"]["child_command"]
        .as_str()
        .expect("catch-up firing's Run child")
        .to_owned();
    assert_eq!(
        catchup.outcome.as_ref().unwrap()["children"],
        json!([run_id])
    );

    let run = wait_for_run_outcome(&fixture, &run_id);
    assert_eq!(run.id, run_id);
    assert_eq!(run.verb, "run");
    assert_eq!(run.state, CommandState::Applied);
    assert_eq!(run.principal, "owner");
    assert_eq!(run.target, None);
    assert_eq!(run.args["context"]["event"], trigger_event);
    assert_eq!(run.args["context"]["rule"]["id"], GLOBAL_RULE_ID);
    assert_eq!(run.args["context"]["rule"]["revision"], rule_state.revision);
    let applied: ScriptRunApplied =
        serde_json::from_value(run.outcome.as_ref().expect("applied Run outcome").clone())
            .expect("typed global catch-up Run outcome");
    applied.validate().expect("valid per-Run process result");
    assert_eq!(applied.children.len(), 1);

    let send_id = &applied.children[0];
    let sent: CoreCommand = serde_json::from_value(fixture.wait_command(send_id, "applied"))
        .expect("typed catch-up SEND child");
    assert_eq!(sent.verb, "send");
    assert_eq!(sent.principal, "owner");
    assert_eq!(sent.target.as_deref(), Some(agent.as_str()));
    assert_eq!(sent.expected_generation, Some(generation));
    assert_eq!(sent.generation, Some(generation));
    assert_eq!(sent.args, json!({"text": SEND_TEXT}));
    let send_turn = exact_result(&fixture, &agent, generation, &sent, SEND_TEXT);

    let own_command: CoreCommand = serde_json::from_slice(
        &fs::read(&own_command_path).expect("Run's own command.get stdout capture"),
    )
    .expect("standard Core Command DTO from first Run RPC");
    assert_eq!(own_command.id, run_id);
    assert_eq!(own_command.principal, "owner");
    let context = script_context(
        &context_path,
        &trigger_id,
        &run_id,
        &agent,
        generation,
        GLOBAL_RULE_ID,
    );
    assert_eq!(context["event"], trigger_event);
    assert_eq!(context["rule"]["revision"], rule_state.revision);

    let (send_terminal_seq, _) =
        wait_for_turn_terminal(&fixture, &agent, generation, send_id, &send_turn.turn_id);
    let events = wait_for_global_rule_cursor(&fixture, send_terminal_seq);
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired" && event["data"]["rule"] == GLOBAL_RULE_ID
            })
            .count(),
        1,
        "the catch-up SEND does not self-trigger another Run"
    );
    assert_eq!(
        command_events(&events, "command.dispatched", &run_id).len(),
        1
    );
    assert_eq!(
        command_events(&events, "command.dispatched", send_id).len(),
        1
    );

    let replay_reply = cli(&fixture.data, &catchup_args);
    assert!(
        replay_reply.status.success(),
        "same-ID global catch-up replay failed; bounded stdout={:?}; bounded stderr={:?}",
        replay_reply.stdout.chars().take(2048).collect::<String>(),
        replay_reply.stderr.chars().take(2048).collect::<String>()
    );
    let replay: CoreCommand =
        serde_json::from_str(&replay_reply.stdout).expect("typed same-ID global catch-up replay");
    assert_eq!(replay, catchup);
    let after_replay = fixture.events();
    assert_eq!(
        after_replay
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == GLOBAL_RULE_ID
                    && event["data"]["parent_command"] == catchup_id
            })
            .count(),
        1,
        "same-ID replay did not repeat the catch-up firing"
    );
    assert_eq!(
        command_events(&after_replay, "command.dispatched", &run_id).len(),
        1
    );
    assert_eq!(
        command_events(&after_replay, "command.dispatched", send_id).len(),
        1
    );
}

#[test]
fn global_event_run_without_interpreter_grant_rejects_before_effects() {
    let mut fixture = Fixture::new("global script run revoked grant");
    let interpreter = approved_interpreter();
    let scripts_root = setup_global_run_fixture(&mut fixture, &interpreter);
    let owner_cwd = fs::canonicalize(&fixture.workspace).expect("canonical owner cwd");
    assert!(owner_cwd.is_absolute());
    assert!(
        !owner_cwd.starts_with(&scripts_root),
        "owner cwd must be outside the private script-entry root"
    );

    let trigger_id = ulid::Ulid::new().to_string();
    let context_path = fixture.root.join("revoked global script context.json");
    let own_command_path = fixture.root.join("revoked global own command.json");
    let unused_agent = ulid::Ulid::new().to_string();
    write_global_run_rule(
        &fixture,
        &interpreter,
        GlobalRunRuleCase {
            agent: &unused_agent,
            generation: 1,
            trigger_command_id: &trigger_id,
            context_path: &context_path,
            own_command_path: &own_command_path,
            cwd: &owner_cwd,
        },
    );

    fixture.stop(false);
    write_global_run_config(&fixture, &interpreter, false);
    fixture.start();
    wait_for_global_rule(&fixture);

    let launch = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().expect("workspace path"),
    ]);
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let trigger = owner_send(
        &fixture,
        &trigger_id,
        &agent,
        generation,
        "revoked-global-script-trigger",
    );
    assert_eq!(trigger.state, TurnState::Completed);

    let trigger_seq = fixture
        .events()
        .iter()
        .find(|event| {
            event["kind"] == "turn.completed" && event["data"]["command_id"] == trigger_id
        })
        .and_then(|event| event["seq"].as_i64())
        .expect("fresh Fake terminal event");
    let events = wait_for_global_rule_cursor(&fixture, trigger_seq);
    let rejected = events
        .iter()
        .find(|event| {
            event["kind"] == "rule.rejected"
                && event["data"]["rule"] == GLOBAL_RULE_ID
                && event["data"]["through_seq"] == trigger_seq
        })
        .expect("revoked owner grant rejects the global Run before admission");
    assert_eq!(rejected["data"]["code"], "not_allowed");
    assert!(!events.iter().any(|event| {
        event["kind"] == "rule.fired"
            && event["data"]["rule"] == GLOBAL_RULE_ID
            && event["data"]["event_seq"] == trigger_seq
    }));
    assert!(
        !context_path.exists(),
        "revoked Run never starts the script"
    );
    assert!(
        !own_command_path.exists(),
        "revoked Run has no Run-role RPC"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event["kind"] == "turn.completed"
                    && event["agent"] == agent
                    && event["generation"] == generation
            })
            .count(),
        1,
        "revoked Run creates no script SEND turn"
    );
}

#[test]
fn project_event_run_rejects_late_invalid_record_before_any_send_dispatch() {
    let mut fixture = Fixture::new("project script run invalid output");
    let interpreter = setup_registered_project(&mut fixture);
    let launch = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().expect("workspace path"),
    ]);
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let trigger_id = ulid::Ulid::new().to_string();
    let context_path = fixture.root.join("invalid script context.json");
    let own_command_path = fixture.root.join("invalid own run command.json");
    write_run_rule(
        &fixture,
        &interpreter,
        RunRuleCase {
            mode: "invalid",
            agent: &agent,
            generation,
            trigger_command_id: &trigger_id,
            match_only_trigger: true,
            context_path: &context_path,
            own_command_path: &own_command_path,
        },
    );

    let _trigger = owner_send(
        &fixture,
        &trigger_id,
        &agent,
        generation,
        "invalid-script-trigger",
    );
    let (firing, _) = wait_for_firing(&fixture, &trigger_id);
    let run_id = firing["data"]["child_command"]
        .as_str()
        .expect("firing's Run command")
        .to_owned();
    let run = wait_for_run_outcome(&fixture, &run_id);
    assert_eq!(run.verb, "run");
    assert_eq!(run.principal, format!("project:{PROJECT_ID}"));
    assert_run_state(&run, CommandState::Failed);
    assert_eq!(
        run.outcome
            .as_ref()
            .and_then(|outcome| outcome["code"].as_str()),
        Some("failed")
    );
    assert_eq!(
        run.outcome
            .as_ref()
            .and_then(|outcome| outcome["cause"].as_str()),
        Some("invalid"),
        "the second record's unknown authority field rejects the complete stream"
    );
    match run
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("children"))
    {
        None => {}
        Some(Value::Array(children)) => assert!(children.is_empty()),
        Some(other) => panic!("failed Run retained a nonempty or malformed child set: {other}"),
    }

    let own_command: CoreCommand = serde_json::from_slice(
        &fs::read(&own_command_path).expect("Run's own command.get stdout capture"),
    )
    .expect("standard Core Command DTO from first Run RPC");
    assert_eq!(own_command.id, run_id);
    assert_eq!(own_command.verb, "run");
    assert_eq!(own_command.principal, format!("project:{PROJECT_ID}"));
    script_context(
        &context_path,
        &trigger_id,
        &run_id,
        &agent,
        generation,
        RULE_ID,
    );

    let events = fixture.events();
    assert_eq!(
        events
            .iter()
            .filter(|event| { event["kind"] == "turn.completed" && event["agent"] == agent })
            .count(),
        1,
        "the valid first record was not dispatched before full-stream validation"
    );
    assert_eq!(
        command_events(&events, "command.dispatched", &run_id).len(),
        1
    );

    let followup_id = ulid::Ulid::new().to_string();
    let followup = owner_send(
        &fixture,
        &followup_id,
        &agent,
        generation,
        "fake-still-available",
    );
    assert_eq!(followup.final_text.as_deref(), Some("fake-still-available"));
    let followup_command: CoreCommand =
        serde_json::from_value(fixture.json(&["cmd", "show", &followup_id]))
            .expect("typed follow-up owner SEND command");
    exact_result(
        &fixture,
        &agent,
        generation,
        &followup_command,
        "fake-still-available",
    );
    let (followup_terminal, _) = wait_for_turn_terminal(
        &fixture,
        &agent,
        generation,
        &followup_id,
        &followup.turn_id,
    );
    let events = wait_for_rule_cursor(&fixture, followup_terminal);
    assert_eq!(
        events
            .iter()
            .filter(|event| { event["kind"] == "turn.completed" && event["agent"] == agent })
            .count(),
        2,
        "only the trigger and later owner SEND reached the Fake Agent"
    );
    let generation_arg = generation.to_string();
    let all_results: AgentResultResponse = serde_json::from_value(fixture.json(&[
        "result",
        &agent,
        "--generation",
        &generation_arg,
        "--limit",
        "10",
    ]))
    .expect("typed Fake result page after invalid stream");
    assert_eq!(all_results.turns.len(), 2);
    assert!(
        !all_results
            .turns
            .iter()
            .any(|turn| turn.final_text.as_deref() == Some(INVALID_SEND_TEXT))
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == RULE_ID
                    && event["data"]["project"] == PROJECT_ID
            })
            .count(),
        1,
        "the later owner SEND does not rerun the trigger-bound rule"
    );
    assert_eq!(
        command_events(&events, "command.dispatched", &run_id).len(),
        1
    );
}
