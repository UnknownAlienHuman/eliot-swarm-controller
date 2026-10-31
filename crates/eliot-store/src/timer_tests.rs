use super::{
    ProjectRegistrationInput, RuleDefinitionInput, Store, TimerDefinitionInput, TimerSlotCommit,
};
use eliot_core::{
    CommandState, FrozenSetupFile, NativeEvent, ScriptCustody, ScriptDirectExit,
    ScriptProcessResult, SourceBatch, SubmitCommand,
    api::{
        FrozenScriptContext, FrozenScriptInterpreter, FrozenScriptRun, RunDefinitionArgs,
        ScriptInterpreter, ScriptRuleContext, ScriptTimerContext, ScriptTimerInvocation,
        TimerRunRequest, TimerSchedule,
    },
    canonical_json_sha256,
};
use serde_json::json;
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use ulid::Ulid;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn store() -> Store {
    Store::open(Path::new(":memory:")).unwrap()
}

fn rule_input(id: &str) -> RuleDefinitionInput {
    let value = json!({"id": id, "enabled": true});
    RuleDefinitionInput {
        id: id.to_owned(),
        canonical_definition: serde_json::to_string(&value).unwrap(),
        definition_digest: canonical_json_sha256(&value).unwrap(),
        enabled: true,
    }
}

fn rule_input_with_action(id: &str, target: &str) -> RuleDefinitionInput {
    let value = json!({
        "id": id,
        "enabled": true,
        "action": {"verb": "stop", "target": target}
    });
    RuleDefinitionInput {
        id: id.to_owned(),
        canonical_definition: serde_json::to_string(&value).unwrap(),
        definition_digest: canonical_json_sha256(&value).unwrap(),
        enabled: true,
    }
}

fn rule_input_with_limit(id: &str, max: u64) -> RuleDefinitionInput {
    let value = json!({
        "id": id,
        "enabled": true,
        "limit": {"max":max,"per":"rule","window_ms":60_000}
    });
    RuleDefinitionInput {
        id: id.to_owned(),
        canonical_definition: serde_json::to_string(&value).unwrap(),
        definition_digest: canonical_json_sha256(&value).unwrap(),
        enabled: true,
    }
}

fn project_run_rule_input(id: &str) -> RuleDefinitionInput {
    let value = json!({
        "id": id,
        "enabled": true,
        "do": [{"action":"run","cmd":["pwsh", ".eliot/scripts/timer.ps1"]}]
    });
    RuleDefinitionInput {
        id: id.to_owned(),
        canonical_definition: serde_json::to_string(&value).unwrap(),
        definition_digest: canonical_json_sha256(&value).unwrap(),
        enabled: true,
    }
}

fn timer_run_action(
    timer: &eliot_core::api::TimerStateView,
    rule_revision: i64,
    invocation: ScriptTimerInvocation,
) -> SubmitCommand {
    let executable = std::env::current_exe()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let entry = std::env::temp_dir()
        .join("eliot-timer-script-test.ps1")
        .display()
        .to_string();
    let run = FrozenScriptRun {
        definition: RunDefinitionArgs {
            cmd: vec!["pwsh".to_owned(), ".eliot/scripts/timer.ps1".to_owned()],
            cwd: None,
            timeout: None,
            env: Default::default(),
        },
        interpreter: FrozenScriptInterpreter {
            reference: "pwsh".to_owned(),
            executable: FrozenSetupFile {
                canonical_path: executable,
                size: 1,
                sha256: "ab".repeat(32),
            },
            argv_prefix: vec!["-File".to_owned()],
        },
        entry: FrozenSetupFile {
            canonical_path: entry,
            size: 1,
            sha256: "cd".repeat(32),
        },
        context: FrozenScriptContext::Timer {
            timer: ScriptTimerContext {
                schedule_revision: timer.schedule_revision,
                schedule: timer.schedule.clone(),
                anchor: timer.anchor,
                invocation,
            },
            rule: ScriptRuleContext {
                id: timer.id.clone(),
                revision: rule_revision,
            },
        },
    };
    let mut request = SubmitCommand::new(String::new(), "run");
    request.args = serde_json::to_value(run).unwrap();
    request
}

fn exact_script_result() -> ScriptProcessResult {
    ScriptProcessResult {
        custody: ScriptCustody::Exact,
        quiescent: true,
        direct_exit: ScriptDirectExit::Exited {
            success: true,
            exit_code: Some(0),
        },
        stderr: None,
    }
}

fn send_request(agent: &str) -> SubmitCommand {
    let mut request = SubmitCommand::new(String::new(), "send");
    request.target = Some(agent.to_owned());
    request.expected_generation = Some(1);
    request.args = json!({"text":"timer script output"});
    request
}

fn project_registration(id: &str, actions: Vec<String>) -> ProjectRegistrationInput {
    let executable = std::env::current_exe()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let checkout = std::env::temp_dir().join(format!("eliot-store-timer-{id}"));
    ProjectRegistrationInput {
        id: id.to_owned(),
        checkout: checkout.display().to_string(),
        repo_common_dir: checkout.join(".git").display().to_string(),
        profiles: Vec::new(),
        actions,
        interpreters: std::collections::BTreeMap::from([(
            "pwsh".to_owned(),
            ScriptInterpreter {
                program: executable,
                argv_prefix: vec!["-File".to_owned()],
            },
        )]),
        run_env: Vec::new(),
    }
}

fn timer_resume_rule_input(
    id: &str,
    agent: &str,
    expected_generation: i64,
    text: Option<&str>,
) -> RuleDefinitionInput {
    let mut action = json!({
        "action": "resume",
        "target": agent,
        "expected_generation": expected_generation,
    });
    if let Some(text) = text {
        action["text"] = json!(text);
    }
    let value = json!({
        "id": id,
        "enabled": true,
        "do": [action],
        "limit": {"max": 4, "per": "rule", "window_ms": 60_000},
    });
    RuleDefinitionInput {
        id: id.to_owned(),
        canonical_definition: serde_json::to_string(&value).unwrap(),
        definition_digest: canonical_json_sha256(&value).unwrap(),
        enabled: true,
    }
}

fn ensure_interval(
    store: &mut Store,
    id: &str,
    registration_ms: i64,
    period_ms: i64,
) -> eliot_core::api::TimerStateView {
    store
        .ensure_rule_state("owner", &rule_input(id), registration_ms)
        .unwrap();
    store
        .ensure_timer_state(
            "owner",
            &TimerDefinitionInput {
                id: id.to_owned(),
                schedule_revision: 1,
                schedule: TimerSchedule::Interval {
                    period_ms,
                    anchor: None,
                },
                enabled: true,
                initial_next_due_at: Some(registration_ms + period_ms),
            },
            registration_ms,
        )
        .unwrap()
}

fn commit_scheduled_timer_run(
    store: &mut Store,
    id: &str,
    schedule: TimerSchedule,
    registration_ms: i64,
    initial_next_due_at: Option<i64>,
    next_due_at_ms: Option<i64>,
) -> (
    eliot_core::api::TimerStateView,
    i64,
    super::TimerSlotAdmission,
) {
    let rule = store
        .ensure_rule_state("owner", &rule_input(id), registration_ms)
        .unwrap();
    let state = store
        .ensure_timer_state(
            "owner",
            &TimerDefinitionInput {
                id: id.to_owned(),
                schedule_revision: 1,
                schedule,
                enabled: true,
                initial_next_due_at,
            },
            registration_ms,
        )
        .unwrap();
    let scheduled_at_ms = state.next_due_at.expect("scheduled timer has a slot");
    let action = timer_run_action(
        &state,
        rule.revision,
        ScriptTimerInvocation::Scheduled { scheduled_at_ms },
    );
    let admission = store
        .commit_timer_slot(
            "owner",
            &state,
            TimerSlotCommit {
                scheduled_at_ms,
                next_due_at_ms,
                action: Some(action),
                skip_code: None,
            },
            scheduled_at_ms,
        )
        .unwrap();
    (state, scheduled_at_ms, admission)
}

fn rule_revision(store: &Store, id: &str) -> i64 {
    store
        .get_rule_state("owner", id)
        .unwrap()
        .expect("timer rule state")
        .revision
}

fn ready_agent(store: &mut Store, project: &str, now: i64) -> String {
    let workspace = format!("timer-{project}");
    ready_agent_with_workspace(store, project, &workspace, "fake", now)
}

fn ready_agent_with_workspace(
    store: &mut Store,
    project: &str,
    workspace: &str,
    profile: &str,
    now: i64,
) -> String {
    let launch_id = Ulid::new().to_string();
    let mut launch = SubmitCommand::new(launch_id.clone(), "launch");
    launch.args = json!({
        "project": project,
        "harness": "fake",
        "profile": profile,
        "workspace": workspace,
    });
    launch.expected_generation = Some(0);
    let admission = store.submit("owner", launch, now).unwrap();
    let agent = admission.command.target.unwrap();
    let native_scope = format!("timer-resume-{agent}-scope");
    let native_ref = format!("timer-resume-{agent}-subject");
    let ready_id = format!("{project}:{agent}:ready");
    store.dispatch(&launch_id, now).unwrap();
    store
        .put_batch(
            SourceBatch {
                batch_id: ready_id.clone(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    kind: "session.ready".to_owned(),
                    native_id: ready_id,
                    data: json!({
                        "command_id": launch_id,
                        "native_scope": native_scope,
                        "native_ref": native_ref,
                    }),
                    ts: None,
                }],
                cursor: None,
            },
            now,
        )
        .unwrap();
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .unwrap();
    agent
}

fn end_timer_agent(store: &mut Store, agent: &str, now: i64) {
    let generation = store.get_agent(agent).unwrap().unwrap().generation;
    let native_id = format!("timer-resume-ended-{agent}");
    store
        .put_batch(
            SourceBatch {
                batch_id: native_id.clone(),
                agent_id: agent.to_owned(),
                generation,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id,
                    data: json!({}),
                }],
                cursor: None,
            },
            now,
        )
        .unwrap();
}

fn apply_timer_resume(
    store: &mut Store,
    agent: &str,
    resume_id: &str,
    old_generation: i64,
    now: i64,
) {
    let retained = store.get_agent(agent).unwrap().unwrap();
    assert_eq!(retained.generation, old_generation);
    let native_scope = retained
        .native_scope
        .expect("retained timer Agent has a native scope");
    let native_ref = retained
        .native_ref
        .expect("retained timer Agent has a native subject");
    let generation = old_generation + 1;
    store.dispatch(resume_id, now).unwrap();
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("timer-resume-ready-{resume_id}"),
                agent_id: agent.to_owned(),
                generation,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: format!("timer-resume-ready-{resume_id}"),
                    data: json!({
                        "command_id": resume_id,
                        "native_scope": native_scope,
                        "native_ref": native_ref,
                    }),
                }],
                cursor: None,
            },
            now + 1,
        )
        .unwrap();
    store
        .settle(resume_id, CommandState::Applied, None, now + 2)
        .unwrap();
}

fn timer_launch_action(project: &str, text: Option<&str>) -> SubmitCommand {
    let mut action = SubmitCommand::new(Ulid::new().to_string(), "launch");
    action.args = json!({
        "project": project,
        "workspace": format!("synthetic-timer-{project}"),
    });
    if let Some(text) = text {
        action.args["text"] = json!(text);
    }
    action.expected_generation = Some(0);
    action
}

fn commit_scheduled_timer_launch(
    store: &mut Store,
    timer: &eliot_core::api::TimerStateView,
    scheduled_at_ms: i64,
    next_due_at_ms: Option<i64>,
    project: &str,
    text: Option<&str>,
    now: i64,
) -> super::TimerSlotAdmission {
    store
        .commit_timer_slot(
            "owner",
            timer,
            TimerSlotCommit {
                scheduled_at_ms,
                next_due_at_ms,
                action: Some(timer_launch_action(project, text)),
                skip_code: None,
            },
            now,
        )
        .unwrap()
}

fn ready_timer_launch(store: &mut Store, launch_id: &str, agent_id: &str, now: i64) {
    store.dispatch(launch_id, now).unwrap();
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("timer-launch-ready-{launch_id}"),
                agent_id: agent_id.to_owned(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: format!("timer-launch-ready-{launch_id}"),
                    data: json!({"command_id": launch_id}),
                }],
                cursor: None,
            },
            now + 1,
        )
        .unwrap();
    store
        .settle(launch_id, CommandState::Applied, None, now + 2)
        .unwrap();
}

fn apply_timer_launch_input_in_running_turn(
    store: &mut Store,
    agent_id: &str,
    send_id: &str,
    now: i64,
) -> String {
    let turn_id = format!("timer-launch-turn-{send_id}");
    store.dispatch(send_id, now).unwrap();
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("timer-launch-turn-started-{send_id}"),
                agent_id: agent_id.to_owned(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: format!("timer-launch-turn-started-{send_id}"),
                    data: json!({"command_id": send_id, "turn_id": turn_id}),
                }],
                cursor: None,
            },
            now + 1,
        )
        .unwrap();
    let send = store.get_command(send_id).unwrap().unwrap();
    assert_eq!(send.state, CommandState::Applied);
    assert_eq!(send.turn_id.as_deref(), Some(turn_id.as_str()));
    let turn = store
        .get_turn(agent_id, 1, &turn_id)
        .unwrap()
        .expect("turn.started associates the linked SEND");
    assert_eq!(turn.state, eliot_core::TurnState::Running);
    turn_id
}

fn stop_request(agent: &str) -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "stop");
    request.target = Some(agent.to_owned());
    request.expected_generation = Some(1);
    request
}

fn timer_resume_request(
    agent: &str,
    expected_generation: i64,
    text: Option<&str>,
) -> SubmitCommand {
    let mut request = SubmitCommand::new(String::new(), "resume");
    request.target = Some(agent.to_owned());
    request.expected_generation = Some(expected_generation);
    if let Some(text) = text {
        request.args = json!({"text": text});
    }
    request
}

fn scheduled_stop(slot_ms: i64, next_due_at_ms: Option<i64>, agent: &str) -> TimerSlotCommit {
    TimerSlotCommit {
        scheduled_at_ms: slot_ms,
        next_due_at_ms,
        action: Some(stop_request(agent)),
        skip_code: None,
    }
}

#[test]
fn omitted_interval_anchor_and_due_survive_reinitialization() {
    let mut store = store();
    let registration_ms = now_ms();
    let period_ms = 30_000;
    let first_due = registration_ms + period_ms;
    let initial = TimerDefinitionInput {
        id: "interval-stop".to_owned(),
        schedule_revision: 1,
        schedule: TimerSchedule::Interval {
            period_ms,
            anchor: None,
        },
        enabled: true,
        initial_next_due_at: Some(first_due),
    };
    store
        .ensure_rule_state("owner", &rule_input("interval-stop"), registration_ms)
        .unwrap();
    let first = store
        .ensure_timer_state("owner", &initial, registration_ms)
        .unwrap();
    assert_eq!(first.anchor, registration_ms);
    assert_eq!(first.next_due_at, Some(first_due));

    // Restart reads retain the original definition sample and cursor. The
    // initial-next-due candidate is deliberately unchanged; Store must not
    // revalidate it against a fresh registration clock.
    let restarted = store
        .ensure_timer_state("owner", &initial, registration_ms + 5 * period_ms)
        .unwrap();
    assert_eq!(restarted.anchor, registration_ms);
    assert_eq!(restarted.last_key, None);
    assert_eq!(restarted.next_due_at, Some(first_due));
}

#[test]
fn slot_child_and_cursor_roll_back_together_on_persistence_failure() {
    let mut store = store();
    let registration_ms = now_ms();
    let agent = ready_agent(&mut store, "timer-rollback", registration_ms);
    let state = ensure_interval(&mut store, "rollback-stop", registration_ms, 1_000);
    let first_due = state.next_due_at.unwrap();

    let committed = store
        .commit_timer_slot(
            "owner",
            &state,
            scheduled_stop(first_due, Some(first_due + 1_000), &agent),
            first_due,
        )
        .unwrap();
    let child = &committed
        .child
        .as_ref()
        .expect("ordinary stop child")
        .command;
    assert_eq!(committed.state.last_key, Some(first_due));
    assert_eq!(
        committed.parent.as_ref().unwrap().outcome.as_ref().unwrap()["children"][0],
        child.id
    );

    // Mark that child settled so the next due slot can reach its atomic write.
    store
        .connection
        .execute(
            "UPDATE commands SET state = 'applied' WHERE id = ?1",
            [&child.id],
        )
        .unwrap();
    store
        .connection
        .execute_batch(
            "CREATE TRIGGER fail_timer_cursor BEFORE UPDATE OF last_key ON timers
             BEGIN SELECT RAISE(ABORT, 'injected timer cursor failure'); END;",
        )
        .unwrap();

    let next_due = committed.state.next_due_at.unwrap();
    assert_eq!(
        store
            .commit_timer_slot(
                "owner",
                &committed.state,
                scheduled_stop(next_due, Some(next_due + 1_000), &agent),
                next_due,
            )
            .unwrap_err()
            .code,
        "internal"
    );
    let retained = store
        .get_timer_state("owner", "rollback-stop")
        .unwrap()
        .unwrap();
    assert_eq!(retained.last_key, Some(first_due));
    assert_eq!(retained.next_due_at, Some(next_due));
    let children: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE link_kind = 'rule_action'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children, 1);
}

#[test]
fn scheduled_cursor_is_monotonic_and_pending_children_block_next_slots() {
    let mut store = store();
    let registration_ms = now_ms();
    let agent = ready_agent(&mut store, "timer-blocking", registration_ms);
    let state = ensure_interval(&mut store, "blocking-stop", registration_ms, 1_000);
    let first_due = state.next_due_at.unwrap();
    let first = store
        .commit_timer_slot(
            "owner",
            &state,
            scheduled_stop(first_due, Some(first_due + 1_000), &agent),
            first_due,
        )
        .unwrap();
    let child = first
        .child
        .as_ref()
        .expect("ordinary stop child")
        .command
        .id
        .clone();
    let next_due = first.state.next_due_at.unwrap();

    let older = store
        .commit_timer_slot(
            "owner",
            &first.state,
            scheduled_stop(first_due - 1, Some(first_due), &agent),
            first_due,
        )
        .unwrap_err();
    assert_eq!(older.code, "bad_state");
    assert_eq!(
        store
            .get_timer_state("owner", "blocking-stop")
            .unwrap()
            .unwrap()
            .last_key,
        Some(first_due)
    );

    for status in ["accepted", "dispatched", "unknown"] {
        if status != "accepted" {
            store
                .connection
                .execute(
                    "UPDATE commands SET state = ?1 WHERE id = ?2",
                    rusqlite::params![status, child],
                )
                .unwrap();
        }
        let blocked = store
            .commit_timer_slot(
                "owner",
                &first.state,
                scheduled_stop(next_due, Some(next_due + 1_000), &agent),
                next_due,
            )
            .unwrap();
        assert!(blocked.blocked, "pending child in state {status}");
        assert_eq!(blocked.state.last_key, Some(first_due));
        assert_eq!(blocked.state.next_due_at, Some(next_due));
    }
}

#[test]
fn scheduled_owner_resume_uses_exact_generation_and_never_replays_uncertain_input() {
    let mut store = store();
    let registration_ms = now_ms();
    let project = "timer-owner-resume";
    let workspace = "synthetic-timer-owner-resume";
    let agent = ready_agent_with_workspace(&mut store, project, workspace, "fake", registration_ms);
    end_timer_agent(&mut store, &agent, registration_ms + 4);
    let retained = store.get_agent(&agent).unwrap().unwrap();
    let agent_count_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM agents", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    let period_ms = 1_000;
    let initial = ensure_interval(
        &mut store,
        "scheduled-owner-resume",
        registration_ms + 5,
        period_ms,
    );
    let first_due = initial.next_due_at.unwrap();
    let stale = store
        .commit_timer_slot(
            "owner",
            &initial,
            TimerSlotCommit {
                scheduled_at_ms: first_due,
                next_due_at_ms: Some(first_due + period_ms),
                action: Some(timer_resume_request(&agent, 2, None)),
                skip_code: None,
            },
            first_due,
        )
        .unwrap();
    assert!(stale.is_new);
    assert!(stale.child.is_none());
    assert_eq!(
        stale.parent.as_ref().unwrap().outcome.as_ref().unwrap()["code"],
        "stale_generation"
    );
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().generation, 1);

    let resume_at = stale.state.next_due_at.unwrap();
    let admitted = store
        .commit_timer_slot(
            "owner",
            &stale.state,
            TimerSlotCommit {
                scheduled_at_ms: resume_at,
                next_due_at_ms: Some(resume_at + period_ms),
                action: Some(timer_resume_request(
                    &agent,
                    1,
                    Some("new literal timer input"),
                )),
                skip_code: None,
            },
            resume_at,
        )
        .unwrap();
    let child = admitted.child.as_ref().expect("targeted Resume action");
    let resume_id = child.command.id.clone();
    let linked = child.linked_command.as_ref().expect("new linked SEND");
    let send_id = linked.id.clone();
    assert_eq!(child.command.verb, "resume");
    assert_eq!(child.command.target.as_deref(), Some(agent.as_str()));
    assert_eq!(child.command.expected_generation, Some(1));
    assert_eq!(child.command.generation, Some(1));
    assert_eq!(child.command.args, json!({}));
    assert_eq!(send_id, format!("{resume_id}/send"));
    assert_eq!(linked.target.as_deref(), Some(agent.as_str()));
    assert_eq!(linked.expected_generation, Some(2));
    assert_eq!(linked.generation, Some(2));
    assert_eq!(linked.state, CommandState::Queued);
    assert_eq!(linked.args["text"], "new literal timer input");
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().generation, 1);
    let blocked = store
        .commit_timer_slot(
            "owner",
            &admitted.state,
            scheduled_stop(
                resume_at + period_ms,
                Some(resume_at + 2 * period_ms),
                &agent,
            ),
            resume_at + period_ms,
        )
        .unwrap();
    assert!(
        blocked.blocked,
        "queued Resume input orders the next timer action"
    );
    assert_eq!(blocked.state.last_key, Some(resume_at));
    assert_eq!(blocked.state.next_due_at, Some(resume_at + period_ms));

    let dispatch_at = resume_at + period_ms + 1;
    store.dispatch(&resume_id, dispatch_at).unwrap();
    store
        .connection
        .execute(
            "UPDATE rule_state SET enabled = 0 WHERE principal = ?1 AND rule = ?2",
            rusqlite::params!["owner", "scheduled-owner-resume"],
        )
        .unwrap();
    let dispatched = store
        .commit_timer_slot(
            "owner",
            &admitted.state,
            TimerSlotCommit {
                scheduled_at_ms: resume_at,
                next_due_at_ms: Some(resume_at + period_ms),
                action: Some(timer_resume_request(&agent, 0, Some("replacement input"))),
                skip_code: None,
            },
            dispatch_at + 1,
        )
        .unwrap();
    assert!(!dispatched.is_new);
    assert_eq!(dispatched.child.as_ref().unwrap().command.id, resume_id);
    assert_eq!(
        dispatched.child.as_ref().unwrap().command.state,
        CommandState::Dispatched
    );
    store
        .settle(&resume_id, CommandState::Unknown, None, dispatch_at + 2)
        .unwrap();
    let replay = store
        .commit_timer_slot(
            "owner",
            &admitted.state,
            TimerSlotCommit {
                scheduled_at_ms: resume_at,
                next_due_at_ms: Some(resume_at + period_ms),
                action: Some(timer_resume_request(&agent, 0, Some("replacement input"))),
                skip_code: None,
            },
            dispatch_at + 3,
        )
        .unwrap();
    assert!(!replay.is_new);
    let replayed = replay.child.as_ref().unwrap();
    assert_eq!(replayed.command.id, resume_id);
    assert_eq!(replayed.command.state, CommandState::Unknown);
    assert_eq!(replayed.linked_command.as_ref().unwrap().id, send_id);
    assert_eq!(
        replayed.linked_command.as_ref().unwrap().state,
        CommandState::Queued
    );
    assert_eq!(
        replayed.linked_command.as_ref().unwrap().args["text"],
        "new literal timer input"
    );
    let dispatched_resume_events: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'
             AND json_extract(data, '$.command_id') = ?1",
            [&resume_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    let dispatched_send_events: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'
             AND json_extract(data, '$.command_id') = ?1",
            [&send_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(dispatched_resume_events, 1);
    assert_eq!(dispatched_send_events, 0);
    let after = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(after.generation, 2);
    assert_eq!(after.profile, retained.profile);
    assert_eq!(after.profile_snapshot, retained.profile_snapshot);
    assert_eq!(after.project, retained.project);
    assert_eq!(after.workspace, retained.workspace);
    assert_eq!(after.native_scope, retained.native_scope);
    assert_eq!(after.native_ref, retained.native_ref);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM agents", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        agent_count_before
    );
}

#[test]
fn manual_owner_resume_replays_before_rule_gate_and_advances_the_retained_agent() {
    let mut store = store();
    let registration_ms = now_ms();
    let project = "timer-manual-resume";
    let workspace = "synthetic-timer-manual-resume";
    let agent = ready_agent_with_workspace(&mut store, project, workspace, "fake", registration_ms);
    end_timer_agent(&mut store, &agent, registration_ms + 4);
    let retained = store.get_agent(&agent).unwrap().unwrap();
    let agent_count_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM agents", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    let before = ensure_interval(&mut store, "manual-resume", registration_ms + 5, 60_000);
    let expected_rule_revision = rule_revision(&store, "manual-resume");
    let request = TimerRunRequest::new(Ulid::new().to_string(), "manual-resume");
    let first = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &before,
            expected_rule_revision,
            timer_resume_request(&agent, 1, None),
            registration_ms + 6,
        )
        .unwrap();
    assert!(first.is_new);
    assert_eq!(first.parent.as_ref().unwrap().id, request.id);
    let child = first.child.as_ref().expect("manual Resume action");
    let resume_id = child.command.id.clone();
    assert_eq!(child.command.target.as_deref(), Some(agent.as_str()));
    assert_eq!(child.command.expected_generation, Some(1));
    assert_eq!(child.command.generation, Some(1));
    assert_eq!(child.command.args, json!({}));
    assert!(child.linked_command.is_none());
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().generation, 1);

    store
        .connection
        .execute(
            "UPDATE rule_state SET enabled = 0 WHERE principal = ?1 AND rule = ?2",
            rusqlite::params!["owner", "manual-resume"],
        )
        .unwrap();
    let high_water_before_replay = store.high_water().unwrap();
    let replay = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &before,
            expected_rule_revision,
            timer_resume_request(&agent, 0, None),
            registration_ms + 7,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert_eq!(replay.parent.as_ref().unwrap().id, request.id);
    let replayed = replay.child.as_ref().expect("retained manual Resume");
    assert_eq!(replayed.command.id, resume_id);
    assert_eq!(replayed.command.expected_generation, Some(1));
    assert_eq!(replayed.command.args, json!({}));
    assert_eq!(store.high_water().unwrap(), high_water_before_replay);
    let after_replay = store
        .get_timer_state("owner", "manual-resume")
        .unwrap()
        .unwrap();
    assert_eq!(after_replay.last_key, before.last_key);
    assert_eq!(after_replay.next_due_at, before.next_due_at);

    apply_timer_resume(&mut store, &agent, &resume_id, 1, registration_ms + 8);
    let resumed = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(resumed.generation, 2);
    assert_eq!(resumed.profile, retained.profile);
    assert_eq!(resumed.profile_snapshot, retained.profile_snapshot);
    assert_eq!(resumed.project, retained.project);
    assert_eq!(resumed.workspace, retained.workspace);
    assert_eq!(resumed.native_scope, retained.native_scope);
    assert_eq!(resumed.native_ref, retained.native_ref);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM agents", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        agent_count_before
    );
    assert_eq!(
        store.get_command(&resume_id).unwrap().unwrap().state,
        CommandState::Applied
    );
}

#[test]
fn manual_run_revision_fences_new_admission_but_not_retained_replay() {
    let mut store = store();
    let registration_ms = now_ms();
    let old_agent = ready_agent(&mut store, "timer-revision-old", registration_ms);
    let new_agent = ready_agent(&mut store, "timer-revision-new", registration_ms);
    let rule_id = "revisioned-manual-stop";
    let old_definition = rule_input_with_action(rule_id, &old_agent);
    let old_rule_state = store
        .ensure_rule_state("owner", &old_definition, registration_ms)
        .unwrap();
    let period_ms = 60_000;
    let timer_definition = TimerDefinitionInput {
        id: rule_id.to_owned(),
        schedule_revision: 1,
        schedule: TimerSchedule::Interval {
            period_ms,
            anchor: None,
        },
        enabled: true,
        initial_next_due_at: Some(registration_ms + period_ms),
    };
    let before = store
        .ensure_timer_state("owner", &timer_definition, registration_ms)
        .unwrap();
    let old_revision = old_rule_state.revision;

    let new_definition = rule_input_with_action(rule_id, &new_agent);
    let reconciled = store
        .reconcile_rule_definitions(
            "owner",
            std::slice::from_ref(&new_definition),
            std::slice::from_ref(&timer_definition),
            registration_ms + 1,
        )
        .unwrap();
    let new_revision = reconciled.value[0].view.revision;
    assert_eq!(new_revision, old_revision + 1);
    let after = store.get_timer_state("owner", rule_id).unwrap().unwrap();
    assert_eq!(after, before);

    let request = TimerRunRequest::new(Ulid::new().to_string(), rule_id);
    let old_action = stop_request(&old_agent);
    let command_count_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let high_water_before = store.high_water().unwrap();
    assert_eq!(
        store
            .admit_manual_timer_run(
                "owner",
                &request,
                &before,
                old_revision,
                old_action.clone(),
                registration_ms + 2,
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
    let request_rows: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE id = ?1 OR parent_command = ?1",
            [&request.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(request_rows, 0);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        command_count_before
    );
    assert_eq!(store.high_water().unwrap(), high_water_before);
    assert_eq!(
        store.get_timer_state("owner", rule_id).unwrap(),
        Some(before.clone())
    );
    assert_eq!(
        store.get_rule_state("owner", rule_id).unwrap(),
        Some(reconciled.value[0].view.clone())
    );

    let new_action = stop_request(&new_agent);
    let admitted = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &after,
            new_revision,
            new_action,
            registration_ms + 3,
        )
        .unwrap();
    assert!(admitted.is_new);
    let child = &admitted.child.as_ref().expect("new action child").command;
    assert_eq!(child.target.as_deref(), Some(new_agent.as_str()));
    assert_eq!(admitted.state, before);

    let high_water_after_admission = store.high_water().unwrap();
    let replay = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &before,
            old_revision,
            old_action,
            registration_ms + 4,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert_eq!(replay.parent.as_ref().unwrap().id, request.id);
    assert_eq!(replay.child.as_ref().unwrap().command.id, child.id);
    assert_eq!(store.high_water().unwrap(), high_water_after_admission);
}

#[test]
fn once_at_registration_allows_manual_run_without_consuming_schedule() {
    let mut store = store();
    let registration_ms = now_ms();
    let agent = ready_agent(&mut store, "timer-once-now", registration_ms);
    let rule_id = "once-now";
    store
        .ensure_rule_state("owner", &rule_input(rule_id), registration_ms)
        .unwrap();
    let initial = TimerDefinitionInput {
        id: rule_id.to_owned(),
        schedule_revision: 1,
        schedule: TimerSchedule::Once {
            at: registration_ms,
        },
        enabled: true,
        initial_next_due_at: None,
    };
    let timer = store
        .ensure_timer_state("owner", &initial, registration_ms)
        .unwrap();
    assert_eq!(timer.next_due_at, None);
    assert_eq!(timer.last_key, None);

    let request = TimerRunRequest::new(Ulid::new().to_string(), rule_id);
    let action = stop_request(&agent);
    let expected_rule_revision = rule_revision(&store, rule_id);
    let first = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &timer,
            expected_rule_revision,
            action.clone(),
            registration_ms,
        )
        .unwrap();
    assert!(first.is_new);
    assert_eq!(first.parent.as_ref().unwrap().id, request.id);
    let child_id = first
        .child
        .as_ref()
        .expect("one ordinary stop child")
        .command
        .id
        .clone();

    let retained_parent = store.replay_timer_run("owner", &request).unwrap().unwrap();
    assert_eq!(retained_parent.id, request.id);
    let children_before: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE parent_command = ?1",
            [&request.id],
            |row| row.get(0),
        )
        .unwrap();
    let replay = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &timer,
            expected_rule_revision,
            action,
            registration_ms + 1,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert_eq!(replay.parent.as_ref().unwrap().id, request.id);
    assert_eq!(replay.child.as_ref().unwrap().command.id, child_id);
    let children_after: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE parent_command = ?1",
            [&request.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children_after, children_before);

    let after = store.get_timer_state("owner", rule_id).unwrap().unwrap();
    assert_eq!(after.last_key, None);
    assert_eq!(after.next_due_at, None);
}

#[test]
fn rate_limit_counts_each_admitted_child_once_and_excludes_parents() {
    let mut store = store();
    let registration_ms = now_ms();
    let agent = ready_agent(&mut store, "timer-rate", registration_ms);
    let mut state = ensure_interval(&mut store, "rate-stop", registration_ms, 1_000);
    let mut rejected = None;

    for _ in 0..=10 {
        let slot_ms = state.next_due_at.unwrap();
        let admission = store
            .commit_timer_slot(
                "owner",
                &state,
                scheduled_stop(slot_ms, Some(slot_ms + 1_000), &agent),
                slot_ms,
            )
            .unwrap();
        if let Some(child) = admission.child.as_ref() {
            store
                .connection
                .execute(
                    "UPDATE commands SET state = 'applied' WHERE id = ?1",
                    [&child.command.id],
                )
                .unwrap();
        } else {
            rejected = Some(admission);
            break;
        }
        state = admission.state;
    }
    let rejected = rejected.expect("eleventh action is rate limited");
    assert_eq!(rejected.parent.unwrap().state, CommandState::Rejected);
    let children: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE link_kind = 'rule_action'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(children, 10);
}

fn assert_scheduled_timer_run_context(
    id: &str,
    schedule: TimerSchedule,
    registration_ms: i64,
    first_due: i64,
    next_due: Option<i64>,
) {
    let mut store = store();
    let (timer, scheduled_at_ms, admitted) = commit_scheduled_timer_run(
        &mut store,
        id,
        schedule,
        registration_ms,
        Some(first_due),
        next_due,
    );
    let parent = admitted.parent.expect("scheduled timer parent");
    let run = admitted.child.expect("scheduled timer Run child").command;
    let rule_revision = store
        .get_rule_state("owner", id)
        .unwrap()
        .expect("timer rule remains stored")
        .revision;
    let expected_context = FrozenScriptContext::Timer {
        timer: ScriptTimerContext {
            schedule_revision: timer.schedule_revision,
            schedule: timer.schedule.clone(),
            anchor: timer.anchor,
            invocation: ScriptTimerInvocation::Scheduled { scheduled_at_ms },
        },
        rule: ScriptRuleContext {
            id: id.to_owned(),
            revision: rule_revision,
        },
    };
    let saved_context: FrozenScriptContext =
        serde_json::from_value(parent.outcome.as_ref().unwrap()["script_context"].clone()).unwrap();
    assert_eq!(saved_context, expected_context);
    assert_eq!(parent.args["rule"], id);
    assert_eq!(parent.args["schedule_revision"], timer.schedule_revision);
    assert_eq!(parent.args["scheduled_at_ms"], scheduled_at_ms);
    assert!(parent.args.get("revision").is_none());
    let frozen: FrozenScriptRun = serde_json::from_value(run.args.clone()).unwrap();
    assert_eq!(frozen.context, saved_context);
    assert!(frozen.context.event().is_none());
    assert!(frozen.context.subject().is_none());

    let dispatched = store.dispatch(&run.id, scheduled_at_ms + 1).unwrap();
    assert_eq!(dispatched.value.state, CommandState::Dispatched);
    store
        .admit_script_output(
            &run.id,
            Vec::new(),
            exact_script_result(),
            scheduled_at_ms + 2,
        )
        .unwrap();

    let updated_rule = store
        .ensure_rule_state("owner", &rule_input_with_limit(id, 9), scheduled_at_ms + 3)
        .unwrap();
    assert_ne!(updated_rule.revision, rule_revision);
    assert_eq!(
        store
            .get_timer_state("owner", id)
            .unwrap()
            .unwrap()
            .schedule_revision,
        timer.schedule_revision
    );

    let high_water_before_replay = store.high_water().unwrap();
    let replay = store
        .replay_timer_slot("owner", &timer, scheduled_at_ms)
        .unwrap()
        .expect("retained slot replays before current rule gates");
    assert!(!replay.is_new);
    assert!(replay.events.is_empty());
    assert_eq!(replay.parent.as_ref().unwrap().id, parent.id);
    assert_eq!(replay.child.as_ref().unwrap().command.id, run.id);
    assert_eq!(replay.state.last_key, Some(scheduled_at_ms));
    let replay_context: FrozenScriptContext = serde_json::from_value(
        replay.parent.as_ref().unwrap().outcome.as_ref().unwrap()["script_context"].clone(),
    )
    .unwrap();
    assert_eq!(replay_context, saved_context);
    assert_eq!(store.high_water().unwrap(), high_water_before_replay);
}

#[test]
fn scheduled_timer_run_freezes_interval_cron_and_once_slots_and_replays_original_context() {
    let registration_ms = now_ms();
    let interval_ms = 60_000;
    assert_scheduled_timer_run_context(
        "timer-run-interval",
        TimerSchedule::Interval {
            period_ms: interval_ms,
            anchor: Some(registration_ms),
        },
        registration_ms,
        registration_ms + interval_ms,
        Some(registration_ms + interval_ms * 2),
    );
    assert_scheduled_timer_run_context(
        "timer-run-cron",
        TimerSchedule::Cron {
            expression: "* * * * *".to_owned(),
            tz: "UTC".to_owned(),
        },
        registration_ms,
        registration_ms + interval_ms,
        Some(registration_ms + interval_ms * 2),
    );
    assert_scheduled_timer_run_context(
        "timer-run-once",
        TimerSchedule::Once {
            at: registration_ms + interval_ms,
        },
        registration_ms,
        registration_ms + interval_ms,
        None,
    );
}

fn assert_forged_scheduled_timer_run_rejected(
    id: &str,
    mutate_context: fn(&mut serde_json::Value),
) {
    let mut store = store();
    let registration_ms = now_ms();
    let period_ms = 60_000;
    let timer = ensure_interval(&mut store, id, registration_ms, period_ms);
    let rule = store.get_rule_state("owner", id).unwrap().unwrap();
    let scheduled_at_ms = timer.next_due_at.unwrap();
    let mut action = timer_run_action(
        &timer,
        rule.revision,
        ScriptTimerInvocation::Scheduled { scheduled_at_ms },
    );
    mutate_context(&mut action.args["context"]);
    let admission = store
        .commit_timer_slot(
            "owner",
            &timer,
            TimerSlotCommit {
                scheduled_at_ms,
                next_due_at_ms: Some(scheduled_at_ms + period_ms),
                action: Some(action),
                skip_code: None,
            },
            scheduled_at_ms,
        )
        .unwrap();
    let parent = admission.parent.expect("durable binding rejection parent");
    assert_eq!(parent.state, CommandState::Rejected);
    assert_eq!(
        parent.outcome.as_ref().unwrap()["code"],
        "rule_action_binding_invalid"
    );
    assert_eq!(parent.outcome.as_ref().unwrap()["children"], json!([]));
    assert!(
        parent
            .outcome
            .as_ref()
            .unwrap()
            .get("script_context")
            .is_none()
    );
    assert!(admission.child.is_none());
    assert_eq!(
        store
            .get_timer_state("owner", id)
            .unwrap()
            .unwrap()
            .last_key,
        Some(scheduled_at_ms)
    );
}

#[test]
fn scheduled_timer_run_rejects_forged_slot_schedule_and_rule_revisions() {
    assert_forged_scheduled_timer_run_rejected("timer-forged-slot", |context| {
        context["timer"]["invocation"]["scheduled_at_ms"] = json!(i64::MAX);
    });
    assert_forged_scheduled_timer_run_rejected("timer-forged-schedule-revision", |context| {
        context["timer"]["schedule_revision"] = json!(2);
    });
    assert_forged_scheduled_timer_run_rejected("timer-forged-rule-revision", |context| {
        context["rule"]["revision"] = json!(999);
    });
}

#[test]
fn manual_timer_run_rejects_forged_parent_context_without_writes() {
    let mut store = store();
    let registration_ms = now_ms();
    let rule_id = "manual-timer-run-forged-context";
    let timer = ensure_interval(&mut store, rule_id, registration_ms, 60_000);
    let revision = rule_revision(&store, rule_id);
    let request = TimerRunRequest::new(Ulid::new().to_string(), rule_id);
    let action = timer_run_action(
        &timer,
        revision,
        ScriptTimerInvocation::Manual {
            parent_command_id: Ulid::new().to_string(),
        },
    );
    let commands_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let high_water_before = store.high_water().unwrap();
    let error = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &timer,
            revision,
            action,
            registration_ms + 1,
        )
        .unwrap_err();
    assert_eq!(error.code, "rule_action_binding_invalid");
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before
    );
    assert_eq!(store.high_water().unwrap(), high_water_before);
    assert_eq!(
        store.get_timer_state("owner", rule_id).unwrap().unwrap(),
        timer
    );
}

#[test]
fn manual_timer_run_uses_external_parent_id_and_replays_its_saved_context() {
    let mut store = store();
    let registration_ms = now_ms();
    let rule_id = "manual-timer-run";
    let period_ms = 60_000;
    let timer = ensure_interval(&mut store, rule_id, registration_ms, period_ms);
    let rule_revision = rule_revision(&store, rule_id);
    let request = TimerRunRequest::new(Ulid::new().to_string(), rule_id);
    let action = timer_run_action(
        &timer,
        rule_revision,
        ScriptTimerInvocation::Manual {
            parent_command_id: request.id.clone(),
        },
    );
    let first = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &timer,
            rule_revision,
            action.clone(),
            registration_ms + 1,
        )
        .unwrap();
    assert!(first.is_new);
    let parent = first.parent.unwrap();
    let run = first.child.unwrap().command;
    assert_eq!(parent.verb, "timer.run");
    assert_eq!(parent.id, request.id);
    assert_eq!(parent.args, json!({"rule": rule_id}));
    assert!(parent.args.get("revision").is_none());
    assert!(parent.args.get("schedule_revision").is_none());
    let saved_context = parent.outcome.as_ref().unwrap()["script_context"].clone();
    let parsed_context: FrozenScriptContext =
        serde_json::from_value(saved_context.clone()).unwrap();
    assert_eq!(
        parsed_context,
        FrozenScriptContext::Timer {
            timer: ScriptTimerContext {
                schedule_revision: timer.schedule_revision,
                schedule: timer.schedule.clone(),
                anchor: timer.anchor,
                invocation: ScriptTimerInvocation::Manual {
                    parent_command_id: request.id.clone(),
                },
            },
            rule: ScriptRuleContext {
                id: rule_id.to_owned(),
                revision: rule_revision,
            },
        }
    );

    store.dispatch(&run.id, registration_ms + 2).unwrap();
    store
        .admit_script_output(
            &run.id,
            Vec::new(),
            exact_script_result(),
            registration_ms + 3,
        )
        .unwrap();
    let updated = store
        .ensure_rule_state(
            "owner",
            &rule_input_with_limit(rule_id, 7),
            registration_ms + 4,
        )
        .unwrap();
    assert_ne!(updated.revision, rule_revision);
    let before_replay = store.high_water().unwrap();
    let replay = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &timer,
            updated.revision,
            timer_run_action(
                &timer,
                updated.revision,
                ScriptTimerInvocation::Manual {
                    parent_command_id: request.id.clone(),
                },
            ),
            registration_ms + 5,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert!(replay.events.is_empty());
    assert_eq!(
        replay.parent.unwrap().outcome.unwrap()["script_context"],
        saved_context
    );
    assert_eq!(replay.child.unwrap().command.id, run.id);
    assert_eq!(store.high_water().unwrap(), before_replay);
    assert_eq!(
        store
            .get_timer_state("owner", rule_id)
            .unwrap()
            .unwrap()
            .last_key,
        None
    );
}

#[test]
fn manual_timer_run_unknown_send_descendant_holds_scheduled_and_manual_firings() {
    let mut store = store();
    let registration_ms = now_ms();
    let agent = ready_agent(&mut store, "timer-run-nested-unknown", registration_ms);
    let rule_id = "timer-run-nested-unknown";
    let period_ms = 60_000;
    let timer = ensure_interval(&mut store, rule_id, registration_ms, period_ms);
    let expected_rule_revision = rule_revision(&store, rule_id);
    let request = TimerRunRequest::new(Ulid::new().to_string(), rule_id);
    let initial_run = timer_run_action(
        &timer,
        expected_rule_revision,
        ScriptTimerInvocation::Manual {
            parent_command_id: request.id.clone(),
        },
    );
    let admitted = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &timer,
            expected_rule_revision,
            initial_run.clone(),
            registration_ms + 1,
        )
        .unwrap();
    let parent_id = admitted.parent.as_ref().unwrap().id.clone();
    let run = admitted.child.unwrap().command;
    store.dispatch(&run.id, registration_ms + 2).unwrap();
    let output = store
        .admit_script_output(
            &run.id,
            vec![send_request(&agent)],
            exact_script_result(),
            registration_ms + 3,
        )
        .unwrap();
    let send = output.children[0].clone();
    store.dispatch(&send.id, registration_ms + 4).unwrap();
    store
        .settle(&send.id, CommandState::Unknown, None, registration_ms + 5)
        .unwrap();

    let high_water_before_replay = store.high_water().unwrap();
    let replay = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &timer,
            expected_rule_revision,
            initial_run,
            registration_ms + 6,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert!(replay.events.is_empty());
    assert_eq!(replay.parent.as_ref().unwrap().id, parent_id);
    assert_eq!(replay.child.as_ref().unwrap().command.id, run.id);
    assert_eq!(store.high_water().unwrap(), high_water_before_replay);

    let output_replay = store
        .admit_script_output(
            &run.id,
            Vec::new(),
            exact_script_result(),
            registration_ms + 7,
        )
        .unwrap();
    assert!(!output_replay.is_new);
    assert!(output_replay.events.is_empty());
    assert_eq!(output_replay.children[0].id, send.id);
    assert_eq!(
        store.get_command(&send.id).unwrap().unwrap().state,
        CommandState::Unknown
    );

    let scheduled_at_ms = timer.next_due_at.unwrap();
    let scheduled = store
        .commit_timer_slot(
            "owner",
            &timer,
            scheduled_stop(scheduled_at_ms, Some(scheduled_at_ms + period_ms), &agent),
            scheduled_at_ms,
        )
        .unwrap();
    assert!(scheduled.blocked);
    assert!(scheduled.parent.is_none());
    assert!(scheduled.child.is_none());
    assert_eq!(scheduled.state.last_key, timer.last_key);
    assert_eq!(scheduled.state.next_due_at, timer.next_due_at);

    let second_request = TimerRunRequest::new(Ulid::new().to_string(), rule_id);
    let manual = store
        .admit_manual_timer_run(
            "owner",
            &second_request,
            &timer,
            expected_rule_revision,
            stop_request(&agent),
            scheduled_at_ms + 1,
        )
        .unwrap();
    assert!(manual.blocked);
    assert!(manual.parent.is_none());
    assert!(manual.child.is_none());
}

#[test]
fn scheduled_and_manual_timer_launch_inputs_hold_following_firings() {
    let period_ms = 1_000;
    for manual_initial in [false, true] {
        for input_state in ["accepted", "queued", "dispatched", "unknown"] {
            let mut store = store();
            let registration_ms = now_ms();
            let mode = if manual_initial {
                "manual"
            } else {
                "scheduled"
            };
            let rule_id = format!("timer-launch-gate-{mode}-{input_state}");
            let timer = ensure_interval(&mut store, &rule_id, registration_ms, period_ms);
            let revision = rule_revision(&store, &rule_id);
            let first_due = timer.next_due_at.expect("first launch slot");
            let project = format!("{rule_id}-initial");
            let action = timer_launch_action(&project, Some("initial launch input"));
            assert!(action.target.is_none());
            assert_eq!(action.expected_generation, Some(0));

            let (timer_after_initial, launch_child) = if manual_initial {
                let request = TimerRunRequest::new(Ulid::new().to_string(), &rule_id);
                let admitted = store
                    .admit_manual_timer_run(
                        "owner",
                        &request,
                        &timer,
                        revision,
                        action,
                        registration_ms + 1,
                    )
                    .unwrap();
                assert!(!admitted.blocked);
                (
                    admitted.state,
                    admitted.child.expect("manual targetless launch child"),
                )
            } else {
                let admitted = commit_scheduled_timer_launch(
                    &mut store,
                    &timer,
                    first_due,
                    Some(first_due + period_ms),
                    &project,
                    Some("initial launch input"),
                    first_due,
                );
                assert!(!admitted.blocked);
                (
                    admitted.state,
                    admitted.child.expect("scheduled targetless launch child"),
                )
            };
            let launch_id = launch_child.command.id.clone();
            let input_id = format!("{launch_id}/send");
            let linked = launch_child
                .linked_command
                .as_ref()
                .expect("launch owns its initial SEND");
            assert_eq!(linked.id, input_id);
            let agent_id = launch_child
                .command
                .target
                .clone()
                .expect("Store binds the launch to its new Agent");
            ready_timer_launch(&mut store, &launch_id, &agent_id, first_due + 1);

            match input_state {
                "accepted" | "queued" => {
                    store
                        .connection
                        .execute(
                            "UPDATE commands SET state = ?1 WHERE id = ?2",
                            rusqlite::params![input_state, input_id],
                        )
                        .unwrap();
                }
                "dispatched" => {
                    assert_eq!(
                        store
                            .dispatch(&input_id, first_due + 4)
                            .unwrap()
                            .value
                            .state,
                        CommandState::Dispatched
                    );
                }
                "unknown" => {
                    store.dispatch(&input_id, first_due + 4).unwrap();
                    store
                        .settle(&input_id, CommandState::Unknown, None, first_due + 5)
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let expected_send_state = match input_state {
                "accepted" => CommandState::Accepted,
                "queued" => CommandState::Queued,
                "dispatched" => CommandState::Dispatched,
                "unknown" => CommandState::Unknown,
                _ => unreachable!(),
            };
            assert_eq!(
                store.get_command(&input_id).unwrap().unwrap().state,
                expected_send_state
            );
            assert_eq!(
                store.get_command(&launch_id).unwrap().unwrap().state,
                CommandState::Applied
            );

            let next_slot = timer_after_initial
                .next_due_at
                .expect("timer retains its next slot");
            let commands_before: i64 = store
                .connection
                .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
                .unwrap();
            let high_water_before = store.high_water().unwrap();
            let scheduled_project = format!("{rule_id}-scheduled-next");
            let scheduled = commit_scheduled_timer_launch(
                &mut store,
                &timer_after_initial,
                next_slot,
                Some(next_slot + period_ms),
                &scheduled_project,
                None,
                next_slot + 1,
            );
            assert!(scheduled.blocked, "{mode} launch input in {input_state}");
            assert!(scheduled.parent.is_none());
            assert!(scheduled.child.is_none());
            assert!(scheduled.events.is_empty());
            assert_eq!(scheduled.state.last_key, timer_after_initial.last_key);
            assert_eq!(scheduled.state.next_due_at, timer_after_initial.next_due_at);

            let request = TimerRunRequest::new(Ulid::new().to_string(), &rule_id);
            let manual_project = format!("{rule_id}-manual-next");
            let manual = store
                .admit_manual_timer_run(
                    "owner",
                    &request,
                    &timer_after_initial,
                    revision,
                    timer_launch_action(&manual_project, None),
                    next_slot + 2,
                )
                .unwrap();
            assert!(manual.blocked, "{mode} launch input in {input_state}");
            assert!(manual.parent.is_none());
            assert!(manual.child.is_none());
            assert!(manual.events.is_empty());
            assert_eq!(manual.state, timer_after_initial);
            assert_eq!(
                store
                    .connection
                    .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                        .get::<_, i64>(0))
                    .unwrap(),
                commands_before
            );
            assert_eq!(store.high_water().unwrap(), high_water_before);
        }
    }
}

#[test]
fn applied_timer_launch_input_releases_gate_while_its_turn_runs() {
    let mut store = store();
    let registration_ms = now_ms();
    let rule_id = "timer-launch-applied-input";
    let period_ms = 1_000;
    let timer = ensure_interval(&mut store, rule_id, registration_ms, period_ms);
    let revision = rule_revision(&store, rule_id);
    let first_due = timer.next_due_at.unwrap();
    let initial = commit_scheduled_timer_launch(
        &mut store,
        &timer,
        first_due,
        Some(first_due + period_ms),
        "timer-launch-applied-input",
        Some("initial launch input"),
        first_due,
    );
    let timer_after_initial = initial.state;
    let launch_child = initial.child.expect("scheduled launch child");
    let launch_id = launch_child.command.id.clone();
    let input_id = launch_child
        .linked_command
        .as_ref()
        .expect("launch owns its initial SEND")
        .id
        .clone();
    let agent_id = launch_child.command.target.unwrap();
    ready_timer_launch(&mut store, &launch_id, &agent_id, first_due + 1);
    let turn_id =
        apply_timer_launch_input_in_running_turn(&mut store, &agent_id, &input_id, first_due + 4);
    assert_eq!(
        store.get_command(&input_id).unwrap().unwrap().state,
        CommandState::Applied
    );

    let next_slot = timer_after_initial.next_due_at.unwrap();
    let scheduled = commit_scheduled_timer_launch(
        &mut store,
        &timer_after_initial,
        next_slot,
        Some(next_slot + period_ms),
        "timer-launch-after-applied-input",
        None,
        next_slot + 1,
    );
    assert!(!scheduled.blocked);
    assert!(scheduled.parent.is_some());
    let scheduled_child = scheduled.child.as_ref().expect("scheduled launch child");
    assert!(scheduled_child.linked_command.is_none());
    store
        .connection
        .execute(
            "UPDATE commands SET state = 'applied' WHERE id = ?1",
            [&scheduled_child.command.id],
        )
        .unwrap();

    let request = TimerRunRequest::new(Ulid::new().to_string(), rule_id);
    let manual = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &scheduled.state,
            revision,
            timer_launch_action("timer-launch-after-applied-input-manual", None),
            next_slot + 3,
        )
        .unwrap();
    assert!(!manual.blocked);
    assert!(manual.parent.is_some());
    assert!(manual.child.is_some());
    assert_eq!(
        store
            .get_turn(&agent_id, 1, &turn_id)
            .unwrap()
            .unwrap()
            .state,
        eliot_core::TurnState::Running
    );
}

#[test]
fn applied_timer_launch_recovery_returns_only_queued_linked_input() {
    for input_state in ["queued", "dispatched", "unknown"] {
        let mut store = store();
        let registration_ms = now_ms();
        let rule_id = format!("timer-launch-recovery-{input_state}");
        let period_ms = 1_000;
        let timer = ensure_interval(&mut store, &rule_id, registration_ms, period_ms);
        let first_due = timer.next_due_at.unwrap();
        let project = format!("{rule_id}-project");
        let initial = commit_scheduled_timer_launch(
            &mut store,
            &timer,
            first_due,
            Some(first_due + period_ms),
            &project,
            Some("recoverable launch input"),
            first_due,
        );
        let launch_child = initial.child.expect("scheduled launch child");
        let launch_id = launch_child.command.id.clone();
        let input_id = format!("{launch_id}/send");
        let agent_id = launch_child.command.target.unwrap();
        ready_timer_launch(&mut store, &launch_id, &agent_id, first_due + 1);

        match input_state {
            "queued" => {
                store
                    .connection
                    .execute(
                        "UPDATE commands SET state = 'queued' WHERE id = ?1",
                        [&input_id],
                    )
                    .unwrap();
            }
            "dispatched" => {
                assert_eq!(
                    store
                        .dispatch(&input_id, first_due + 4)
                        .unwrap()
                        .value
                        .state,
                    CommandState::Dispatched
                );
            }
            "unknown" => {
                store.dispatch(&input_id, first_due + 4).unwrap();
                store
                    .settle(&input_id, CommandState::Unknown, None, first_due + 5)
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let input_before = store.get_command(&input_id).unwrap().unwrap();
        let high_water_before = store.high_water().unwrap();
        let recovered = store.recover_rule_children(None, 100).unwrap();

        if input_state == "queued" {
            assert_eq!(recovered.len(), 1);
            let launch = &recovered[0];
            assert_eq!(launch.command.id, launch_id);
            assert_eq!(launch.command.state, CommandState::Applied);
            let input = launch
                .linked_command
                .as_ref()
                .expect("recovered launch retains its linked input");
            assert_eq!(input.id, format!("{launch_id}/send"));
            assert_eq!(input.state, CommandState::Queued);
            let (parent_id, link_kind, ordinal): (Option<String>, Option<String>, Option<i64>) =
                store
                    .connection
                    .query_row(
                        "SELECT parent_command, link_kind, ordinal FROM commands WHERE id = ?1",
                        [&input_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )
                    .unwrap();
            assert_eq!(parent_id.as_deref(), Some(launch_id.as_str()));
            assert_eq!(link_kind.as_deref(), Some("send"));
            assert_eq!(ordinal, Some(0));
        } else {
            assert!(
                recovered
                    .iter()
                    .all(|launch| launch.command.id != launch_id),
                "{input_state} launch input must not be replayed"
            );
        }
        assert_eq!(store.get_command(&input_id).unwrap().unwrap(), input_before);
        assert_eq!(store.high_water().unwrap(), high_water_before);
    }
}

#[test]
fn timer_launch_and_linked_input_count_as_one_rule_action() {
    let mut store = store();
    let registration_ms = now_ms();
    let rule_id = "timer-launch-count";
    let period_ms = 1_000;
    let timer = ensure_interval(&mut store, rule_id, registration_ms, period_ms);
    store
        .ensure_rule_state(
            "owner",
            &rule_input_with_limit(rule_id, 2),
            registration_ms + 1,
        )
        .unwrap();
    let first_due = timer.next_due_at.unwrap();
    let first = commit_scheduled_timer_launch(
        &mut store,
        &timer,
        first_due,
        Some(first_due + period_ms),
        "timer-launch-count-first",
        Some("first launch input"),
        first_due,
    );
    let timer_after_first = first.state;
    let first_child = first.child.expect("first launch action child");
    let first_launch_id = first_child.command.id.clone();
    let first_input_id = first_child
        .linked_command
        .as_ref()
        .expect("first launch owns one linked SEND")
        .id
        .clone();
    let first_agent = first_child.command.target.unwrap();
    ready_timer_launch(&mut store, &first_launch_id, &first_agent, first_due + 1);
    apply_timer_launch_input_in_running_turn(
        &mut store,
        &first_agent,
        &first_input_id,
        first_due + 4,
    );

    let action_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        action_count, 1,
        "the nested launch SEND is not another rule action"
    );

    let second_due = timer_after_first.next_due_at.unwrap();
    let second = commit_scheduled_timer_launch(
        &mut store,
        &timer_after_first,
        second_due,
        Some(second_due + period_ms),
        "timer-launch-count-second",
        None,
        second_due,
    );
    assert!(!second.blocked);
    let second_child = second.child.as_ref().expect("second launch action child");
    assert_eq!(second_child.command.verb, "launch");
    assert!(second_child.linked_command.is_none());
    let second_launch_id = second_child.command.id.clone();
    let action_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(action_count, 2);
    store
        .connection
        .execute(
            "UPDATE commands SET state = 'applied' WHERE id = ?1",
            [&second_launch_id],
        )
        .unwrap();

    let third_due = second.state.next_due_at.unwrap();
    let rejected = commit_scheduled_timer_launch(
        &mut store,
        &second.state,
        third_due,
        Some(third_due + period_ms),
        "timer-launch-count-third",
        None,
        third_due,
    );
    assert!(!rejected.blocked);
    assert!(rejected.child.is_none());
    let parent = rejected.parent.as_ref().expect("rate-limited timer parent");
    assert_eq!(parent.state, CommandState::Rejected);
    assert_eq!(parent.outcome.as_ref().unwrap()["code"], "rate_limited");
    let action_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(action_count, 2);
}

#[test]
fn scheduled_project_timer_launch_replay_precedes_current_profile_gate() {
    let mut store = store();
    let registration_ms = now_ms();
    let project = "timer-project-launch-replay";
    let rule_id = "timer-project-launch-replay";
    let mut registration =
        project_registration(project, vec!["launch".to_owned(), "send".to_owned()]);
    registration.profiles = vec!["fake".to_owned()];
    let checkout = registration.checkout.clone();
    store
        .upsert_project_registration("owner", &registration, registration_ms)
        .expect("register Project timer Launch and SEND");

    let principal = format!("project:{project}");
    let rule_value = json!({
        "id": rule_id,
        "enabled": true,
        "on": {"event": "turn.completed"},
        "do": [{
            "action": "launch",
            "profile": "fake",
            "workspace": checkout,
            "text": "initial Project timer input",
        }],
        "limit": {"max": 2, "per": "rule", "window_ms": 60_000},
    });
    let rule = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&rule_value).unwrap(),
        definition_digest: canonical_json_sha256(&rule_value).unwrap(),
        enabled: true,
    };
    let rule_state = store
        .ensure_rule_state(&principal, &rule, registration_ms + 1)
        .expect("ensure two-action Project timer rule");
    assert!(rule_state.enabled);
    let period_ms = 1_000;
    let timer = store
        .ensure_timer_state(
            &principal,
            &TimerDefinitionInput {
                id: rule_id.to_owned(),
                schedule_revision: 1,
                schedule: TimerSchedule::Interval {
                    period_ms,
                    anchor: Some(registration_ms),
                },
                enabled: true,
                initial_next_due_at: Some(registration_ms + period_ms),
            },
            registration_ms + 1,
        )
        .expect("ensure Project timer schedule");
    let launch_action = |text: Option<&str>| {
        let mut action = SubmitCommand::new(Ulid::new().to_string(), "launch");
        action.args = json!({
            "project": project,
            "harness": "fake",
            "profile": "fake",
            "profile_snapshot": {},
            "workspace": checkout.as_str(),
        });
        if let Some(text) = text {
            action.args["text"] = json!(text);
        }
        action.expected_generation = Some(0);
        action
    };
    let first_due = timer.next_due_at.expect("first scheduled slot");
    let first = store
        .commit_timer_slot(
            &principal,
            &timer,
            TimerSlotCommit {
                scheduled_at_ms: first_due,
                next_due_at_ms: Some(first_due + period_ms),
                action: Some(launch_action(Some("initial Project timer input"))),
                skip_code: None,
            },
            first_due,
        )
        .expect("admit first Project timer Launch");
    assert!(!first.blocked);
    let first_parent_id = first.parent.as_ref().unwrap().id.clone();
    let first_child = first.child.as_ref().expect("first Project Launch action");
    let first_launch_id = first_child.command.id.clone();
    let first_send_id = first_child
        .linked_command
        .as_ref()
        .expect("Project Launch has linked SEND")
        .id
        .clone();
    let first_agent = first_child.command.target.as_deref().unwrap().to_owned();
    assert_eq!(first_send_id, format!("{first_launch_id}/send"));
    ready_timer_launch(&mut store, &first_launch_id, &first_agent, first_due + 1);
    apply_timer_launch_input_in_running_turn(
        &mut store,
        &first_agent,
        &first_send_id,
        first_due + 4,
    );

    let second_due = first.state.next_due_at.expect("second scheduled slot");
    let second = store
        .commit_timer_slot(
            &principal,
            &first.state,
            TimerSlotCommit {
                scheduled_at_ms: second_due,
                next_due_at_ms: Some(second_due + period_ms),
                action: Some(launch_action(None)),
                skip_code: None,
            },
            second_due,
        )
        .expect("admit second Project timer Launch under max=2");
    assert!(!second.blocked);
    let second_child = second.child.as_ref().expect("second Project Launch action");
    let second_launch_id = second_child.command.id.clone();
    let second_agent = second_child.command.target.as_deref().unwrap().to_owned();
    assert!(second_child.linked_command.is_none());
    ready_timer_launch(&mut store, &second_launch_id, &second_agent, second_due + 1);
    let action_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE principal = ?1 AND link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [&principal],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(action_count, 2);

    registration.profiles.clear();
    store
        .upsert_project_registration("owner", &registration, second_due + 4)
        .expect("revoke the current Project profile grant");
    let third_due = second.state.next_due_at.expect("third scheduled slot");
    let denied = store
        .commit_timer_slot(
            &principal,
            &second.state,
            TimerSlotCommit {
                scheduled_at_ms: third_due,
                next_due_at_ms: Some(third_due + period_ms),
                action: Some(launch_action(None)),
                skip_code: None,
            },
            third_due,
        )
        .expect("record rejection under the changed current profile gate");
    assert!(!denied.blocked);
    assert!(denied.child.is_none());
    assert_eq!(
        denied.parent.as_ref().unwrap().state,
        CommandState::Rejected
    );
    assert_eq!(
        denied.parent.as_ref().unwrap().outcome.as_ref().unwrap()["code"],
        "not_allowed"
    );
    let commands_before_replay: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let high_water_before_replay = store.high_water().unwrap();
    let first_launch_dispatches: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'
             AND json_extract(data, '$.command_id') = ?1",
            [&first_launch_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    let first_send_dispatches: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'
             AND json_extract(data, '$.command_id') = ?1",
            [&first_send_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(first_launch_dispatches, 1);
    assert_eq!(first_send_dispatches, 1);

    let replay = store
        .replay_timer_slot(&principal, &timer, first_due)
        .unwrap()
        .expect("same scheduled Launch slot replays before changed current gates");
    assert!(!replay.is_new);
    assert!(replay.events.is_empty());
    assert_eq!(replay.parent.as_ref().unwrap().id, first_parent_id);
    let replay_launch = replay.child.as_ref().expect("retained Launch action");
    assert_eq!(replay_launch.command.id, first_launch_id);
    assert_eq!(replay_launch.command.state, CommandState::Applied);
    let replay_send = replay_launch
        .linked_command
        .as_ref()
        .expect("retained linked Project input");
    assert_eq!(replay_send.id, first_send_id);
    assert_eq!(replay_send.args["text"], "initial Project timer input");
    assert_eq!(replay_send.state, CommandState::Applied);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before_replay
    );
    assert_eq!(store.high_water().unwrap(), high_water_before_replay);
    assert_eq!(
        store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'
                 AND json_extract(data, '$.command_id') = ?1",
                [&first_launch_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        first_launch_dispatches
    );
    assert_eq!(
        store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'
                 AND json_extract(data, '$.command_id') = ?1",
                [&first_send_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        first_send_dispatches
    );
}

#[test]
fn project_timer_run_reuses_registration_authority_at_dispatch() {
    let mut store = store();
    let registration_ms = now_ms();
    let project = "timer-run-project-authority";
    let rule_id = "project-timer-run";
    store
        .upsert_project_registration(
            "owner",
            &project_registration(project, vec!["run".to_owned()]),
            registration_ms,
        )
        .unwrap();
    let principal = format!("project:{project}");
    let rule = store
        .ensure_rule_state(
            &principal,
            &project_run_rule_input(rule_id),
            registration_ms,
        )
        .unwrap();
    let period_ms = 60_000;
    let timer = store
        .ensure_timer_state(
            &principal,
            &TimerDefinitionInput {
                id: rule_id.to_owned(),
                schedule_revision: 1,
                schedule: TimerSchedule::Interval {
                    period_ms,
                    anchor: Some(registration_ms),
                },
                enabled: true,
                initial_next_due_at: Some(registration_ms + period_ms),
            },
            registration_ms,
        )
        .unwrap();
    let scheduled_at_ms = timer.next_due_at.unwrap();
    let action = timer_run_action(
        &timer,
        rule.revision,
        ScriptTimerInvocation::Scheduled { scheduled_at_ms },
    );
    let admitted = store
        .commit_timer_slot(
            &principal,
            &timer,
            TimerSlotCommit {
                scheduled_at_ms,
                next_due_at_ms: Some(scheduled_at_ms + period_ms),
                action: Some(action),
                skip_code: None,
            },
            scheduled_at_ms,
        )
        .unwrap();
    let run = admitted.child.unwrap().command;
    assert_eq!(run.principal, principal);
    assert!(run.target.is_none());

    store
        .upsert_project_registration(
            "owner",
            &project_registration(project, Vec::new()),
            scheduled_at_ms + 1,
        )
        .unwrap();
    let replay = store
        .replay_timer_slot(&principal, &timer, scheduled_at_ms)
        .unwrap()
        .expect("retained slot replays without the current project grant");
    assert!(!replay.is_new);
    assert_eq!(
        replay.parent.as_ref().unwrap().id,
        admitted.parent.unwrap().id
    );
    assert_eq!(replay.child.as_ref().unwrap().command.id, run.id);
    assert_eq!(
        store
            .dispatch(&run.id, scheduled_at_ms + 2)
            .unwrap_err()
            .code,
        "not_allowed"
    );
    assert_eq!(
        store.get_command(&run.id).unwrap().unwrap().state,
        CommandState::Accepted
    );
}

#[test]
fn project_scheduled_and_manual_timer_resume_keep_bindings_and_gate_new_input() {
    let mut store = store();
    let registration_ms = now_ms();
    let project = "timer-project-resume";
    let mut registration = project_registration(project, vec!["resume".to_owned()]);
    registration.profiles = vec!["fake".to_owned()];
    let checkout = registration.checkout.clone();
    store
        .upsert_project_registration("owner", &registration, registration_ms)
        .unwrap();
    let principal = format!("project:{project}");

    let scheduled_agent =
        ready_agent_with_workspace(&mut store, project, &checkout, "fake", registration_ms + 1);
    end_timer_agent(&mut store, &scheduled_agent, registration_ms + 5);
    let scheduled_retained = store.get_agent(&scheduled_agent).unwrap().unwrap();
    let manual_agent =
        ready_agent_with_workspace(&mut store, project, &checkout, "fake", registration_ms + 10);
    end_timer_agent(&mut store, &manual_agent, registration_ms + 14);
    let manual_retained = store.get_agent(&manual_agent).unwrap().unwrap();
    let agent_count_before: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM agents", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();

    let scheduled_rule_id = "project-scheduled-resume";
    let manual_rule_id = "project-manual-resume";
    let scheduled_rule = timer_resume_rule_input(scheduled_rule_id, &scheduled_agent, 1, None);
    let manual_text = "new literal Project timer input";
    let manual_rule = timer_resume_rule_input(manual_rule_id, &manual_agent, 1, Some(manual_text));
    assert!(
        store
            .ensure_rule_state(&principal, &scheduled_rule, registration_ms + 20)
            .unwrap()
            .enabled
    );
    let manual_rule_state = store
        .ensure_rule_state(&principal, &manual_rule, registration_ms + 21)
        .unwrap();
    let period_ms = 1_000;
    let scheduled_timer = store
        .ensure_timer_state(
            &principal,
            &TimerDefinitionInput {
                id: scheduled_rule_id.to_owned(),
                schedule_revision: 1,
                schedule: TimerSchedule::Interval {
                    period_ms,
                    anchor: Some(registration_ms + 20),
                },
                enabled: true,
                initial_next_due_at: Some(registration_ms + 20 + period_ms),
            },
            registration_ms + 20,
        )
        .unwrap();
    let manual_timer = store
        .ensure_timer_state(
            &principal,
            &TimerDefinitionInput {
                id: manual_rule_id.to_owned(),
                schedule_revision: 1,
                schedule: TimerSchedule::Interval {
                    period_ms,
                    anchor: Some(registration_ms + 21),
                },
                enabled: true,
                initial_next_due_at: Some(registration_ms + 21 + period_ms),
            },
            registration_ms + 21,
        )
        .unwrap();

    let scheduled_at_ms = scheduled_timer.next_due_at.unwrap();
    let scheduled = store
        .commit_timer_slot(
            &principal,
            &scheduled_timer,
            TimerSlotCommit {
                scheduled_at_ms,
                next_due_at_ms: Some(scheduled_at_ms + period_ms),
                action: Some(timer_resume_request(&scheduled_agent, 1, None)),
                skip_code: None,
            },
            scheduled_at_ms,
        )
        .unwrap();
    let scheduled_child = scheduled.child.as_ref().expect("Project timer Resume");
    let scheduled_resume_id = scheduled_child.command.id.clone();
    assert_eq!(scheduled_child.command.principal, principal);
    assert_eq!(
        scheduled_child.command.target.as_deref(),
        Some(scheduled_agent.as_str())
    );
    assert_eq!(scheduled_child.command.expected_generation, Some(1));
    assert_eq!(scheduled_child.command.generation, Some(1));
    assert_eq!(scheduled_child.command.args, json!({}));
    assert!(scheduled_child.linked_command.is_none());
    let scheduled_before_resume = store.get_agent(&scheduled_agent).unwrap().unwrap();
    assert_eq!(scheduled_before_resume.generation, 1);
    assert_eq!(scheduled_before_resume.profile, scheduled_retained.profile);
    assert_eq!(
        scheduled_before_resume.workspace.as_deref(),
        Some(checkout.as_str())
    );
    assert_eq!(
        scheduled_before_resume.native_scope,
        scheduled_retained.native_scope
    );
    assert_eq!(
        scheduled_before_resume.native_ref,
        scheduled_retained.native_ref
    );
    apply_timer_resume(
        &mut store,
        &scheduled_agent,
        &scheduled_resume_id,
        1,
        scheduled_at_ms + 1,
    );
    let scheduled_resumed = store.get_agent(&scheduled_agent).unwrap().unwrap();
    assert_eq!(scheduled_resumed.generation, 2);
    assert_eq!(scheduled_resumed.profile, scheduled_retained.profile);
    assert_eq!(
        scheduled_resumed.profile_snapshot,
        scheduled_retained.profile_snapshot
    );
    assert_eq!(scheduled_resumed.project, scheduled_retained.project);
    assert_eq!(scheduled_resumed.workspace, scheduled_retained.workspace);
    assert_eq!(
        scheduled_resumed.native_scope,
        scheduled_retained.native_scope
    );
    assert_eq!(scheduled_resumed.native_ref, scheduled_retained.native_ref);

    let manual_request =
        TimerRunRequest::new(Ulid::new().to_string(), manual_rule_id).in_project(project);
    let manual_action = timer_resume_request(&manual_agent, 1, Some(manual_text));
    let missing_send_grant = store
        .admit_manual_timer_run(
            &principal,
            &manual_request,
            &manual_timer,
            manual_rule_state.revision,
            manual_action.clone(),
            scheduled_at_ms + 4,
        )
        .unwrap_err();
    assert_eq!(missing_send_grant.code, "not_allowed");
    let failed_rows: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE id = ?1 OR parent_command = ?1",
            [&manual_request.id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(failed_rows, 0, "missing SEND grant leaves no retained run");

    registration.actions.push("send".to_owned());
    store
        .upsert_project_registration("owner", &registration, scheduled_at_ms + 5)
        .unwrap();
    let manual = store
        .admit_manual_timer_run(
            &principal,
            &manual_request,
            &manual_timer,
            manual_rule_state.revision,
            manual_action,
            scheduled_at_ms + 6,
        )
        .unwrap();
    assert!(manual.is_new);
    let manual_child = manual.child.as_ref().expect("manual Project Resume");
    let manual_resume_id = manual_child.command.id.clone();
    let linked = manual_child
        .linked_command
        .as_ref()
        .expect("Project Resume text is a separate SEND");
    let manual_send_id = linked.id.clone();
    assert_eq!(manual_child.command.principal, principal);
    assert_eq!(
        manual_child.command.target.as_deref(),
        Some(manual_agent.as_str())
    );
    assert_eq!(manual_child.command.expected_generation, Some(1));
    assert_eq!(manual_child.command.generation, Some(1));
    assert_eq!(manual_child.command.args, json!({}));
    assert_eq!(manual_send_id, format!("{manual_resume_id}/send"));
    assert_eq!(linked.target.as_deref(), Some(manual_agent.as_str()));
    assert_eq!(linked.expected_generation, Some(2));
    assert_eq!(linked.generation, Some(2));
    assert_eq!(linked.state, CommandState::Queued);
    assert_eq!(linked.args["text"], manual_text);
    let manual_after_admission = store.get_agent(&manual_agent).unwrap().unwrap();
    assert_eq!(
        manual_after_admission.generation,
        manual_retained.generation
    );
    assert_eq!(manual_after_admission.profile, manual_retained.profile);
    assert_eq!(
        manual_after_admission.profile_snapshot,
        manual_retained.profile_snapshot
    );
    assert_eq!(manual_after_admission.project, manual_retained.project);
    assert_eq!(manual_after_admission.workspace, manual_retained.workspace);
    assert_eq!(
        manual_after_admission.native_scope,
        manual_retained.native_scope
    );
    assert_eq!(
        manual_after_admission.native_ref,
        manual_retained.native_ref
    );

    let dispatch_at = scheduled_at_ms + 7;
    store.dispatch(&manual_resume_id, dispatch_at).unwrap();
    store
        .settle(
            &manual_resume_id,
            CommandState::Unknown,
            None,
            dispatch_at + 1,
        )
        .unwrap();
    registration.actions.clear();
    store
        .upsert_project_registration("owner", &registration, dispatch_at + 2)
        .unwrap();
    store
        .connection
        .execute(
            "UPDATE rule_state SET enabled = 0 WHERE principal = ?1 AND rule IN (?2, ?3)",
            rusqlite::params![&principal, scheduled_rule_id, manual_rule_id],
        )
        .unwrap();

    let command_count_before_replay: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
    let high_water_before_replay = store.high_water().unwrap();
    let scheduled_replay = store
        .commit_timer_slot(
            &principal,
            &scheduled.state,
            TimerSlotCommit {
                scheduled_at_ms,
                next_due_at_ms: Some(scheduled_at_ms + period_ms),
                action: Some(timer_resume_request(&manual_agent, 9, Some("replacement"))),
                skip_code: None,
            },
            dispatch_at + 3,
        )
        .unwrap();
    assert!(!scheduled_replay.is_new);
    assert_eq!(
        scheduled_replay.child.as_ref().unwrap().command.id,
        scheduled_resume_id
    );
    assert_eq!(
        scheduled_replay.child.as_ref().unwrap().command.state,
        CommandState::Applied
    );
    let manual_replay = store
        .admit_manual_timer_run(
            &principal,
            &manual_request,
            &manual_timer,
            manual_rule_state.revision,
            timer_resume_request(&scheduled_agent, 9, Some("replacement")),
            dispatch_at + 4,
        )
        .unwrap();
    assert!(!manual_replay.is_new);
    let replayed_manual = manual_replay.child.as_ref().unwrap();
    assert_eq!(replayed_manual.command.id, manual_resume_id);
    assert_eq!(replayed_manual.command.state, CommandState::Unknown);
    assert_eq!(replayed_manual.command.expected_generation, Some(1));
    let replayed_send = replayed_manual.linked_command.as_ref().unwrap();
    assert_eq!(replayed_send.id, manual_send_id);
    assert_eq!(replayed_send.state, CommandState::Queued);
    assert_eq!(replayed_send.args["text"], manual_text);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        command_count_before_replay
    );
    assert_eq!(store.high_water().unwrap(), high_water_before_replay);
    let resume_dispatches: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'
             AND json_extract(data, '$.command_id') = ?1",
            [&manual_resume_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    let send_dispatches: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'
             AND json_extract(data, '$.command_id') = ?1",
            [&manual_send_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap();
    assert_eq!(resume_dispatches, 1);
    assert_eq!(send_dispatches, 0);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM agents", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        agent_count_before
    );
}
