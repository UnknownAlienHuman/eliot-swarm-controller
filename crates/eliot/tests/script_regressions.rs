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
use std::{fs, path::PathBuf, thread, time::Duration};
use support::{CLI, Fixture, cli};

const PROJECT_ID: &str = "script-fixture";
const RULE_ID: &str = "project-script-on-completed";
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

fn script_context(
    path: &std::path::Path,
    trigger_command_id: &str,
    run_id: &str,
    agent: &str,
    generation: i64,
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
    assert_eq!(context["rule"]["id"], RULE_ID);
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

    let run: CoreCommand = serde_json::from_value(fixture.wait_command(&run_id, "applied"))
        .expect("typed successful targetless Run command");
    assert_eq!(run.id, run_id);
    assert_eq!(run.verb, "run");
    assert_eq!(run.target, None);
    assert_eq!(run.expected_generation, None);
    assert_eq!(run.generation, None);
    assert_eq!(run.principal, project_principal);
    assert_eq!(run.state, CommandState::Applied);
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
    script_context(&context_path, &trigger_id, &run_id, &agent, generation);

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
    let run: CoreCommand = serde_json::from_value(fixture.wait_command(&run_id, "failed"))
        .expect("typed failed Run command");
    assert_eq!(run.verb, "run");
    assert_eq!(run.principal, format!("project:{PROJECT_ID}"));
    assert_eq!(run.state, CommandState::Failed);
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
    script_context(&context_path, &trigger_id, &run_id, &agent, generation);

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
