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
    let launch_id = Ulid::new().to_string();
    let mut launch = SubmitCommand::new(launch_id.clone(), "launch");
    launch.args = json!({"project": project, "workspace": format!("timer-{project}")});
    launch.expected_generation = Some(0);
    let admission = store.submit("owner", launch, now).unwrap();
    let agent = admission.command.target.unwrap();
    store.dispatch(&launch_id, now).unwrap();
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ready"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    kind: "session.ready".to_owned(),
                    native_id: format!("{project}:ready"),
                    data: json!({"command_id": launch_id}),
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

fn stop_request(agent: &str) -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "stop");
    request.target = Some(agent.to_owned());
    request.expected_generation = Some(1);
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
    let child = committed.child.as_ref().expect("ordinary stop child");
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
fn manual_replay_precedes_current_gates_and_keeps_schedule_cursor() {
    let mut store = store();
    let registration_ms = now_ms();
    let agent = ready_agent(&mut store, "timer-manual", registration_ms);
    let before = ensure_interval(&mut store, "manual-stop", registration_ms, 60_000);
    let expected_rule_revision = rule_revision(&store, "manual-stop");
    let request = TimerRunRequest::new(Ulid::new().to_string(), "manual-stop");
    let action = stop_request(&agent);
    let first = store
        .admit_manual_timer_run(
            "owner",
            &request,
            &before,
            expected_rule_revision,
            action,
            registration_ms,
        )
        .unwrap();
    assert!(first.is_new);
    assert_eq!(first.parent.as_ref().unwrap().id, request.id);
    assert!(first.child.is_some());

    // A same-ID retry returns its retained parent even after the rule is
    // disabled, and manual execution leaves scheduled cursor state untouched.
    store
        .connection
        .execute(
            "UPDATE rule_state SET enabled = 0 WHERE principal = ?1 AND rule = ?2",
            rusqlite::params!["owner", "manual-stop"],
        )
        .unwrap();
    let replay = store.replay_timer_run("owner", &request).unwrap().unwrap();
    assert_eq!(replay.id, request.id);
    let after = store
        .get_timer_state("owner", "manual-stop")
        .unwrap()
        .unwrap();
    assert_eq!(after.last_key, before.last_key);
    assert_eq!(after.next_due_at, before.next_due_at);
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
    let child = admitted.child.as_ref().expect("new action child");
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
    assert_eq!(replay.child.as_ref().unwrap().id, child.id);
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
    assert_eq!(replay.child.as_ref().unwrap().id, child_id);
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
                    [&child.id],
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
    let run = admitted.child.expect("scheduled timer Run child");
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
    assert_eq!(replay.child.as_ref().unwrap().id, run.id);
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
    let run = first.child.unwrap();
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
    assert_eq!(replay.child.unwrap().id, run.id);
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
    let run = admitted.child.unwrap();
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
    assert_eq!(replay.child.as_ref().unwrap().id, run.id);
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
    let run = admitted.child.unwrap();
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
    assert_eq!(replay.child.as_ref().unwrap().id, run.id);
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
