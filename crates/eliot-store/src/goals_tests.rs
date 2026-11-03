use super::{GoalCheckResult, GoalContinuation, GoalRoundCommit, Store};
use eliot_core::{
    AgentState, CommandState, NativeEvent, SourceBatch, SubmitCommand,
    api::{
        DEFAULT_GOAL_CHECK_TIMEOUT_MS, GoalCounters, GoalDefinition, GoalMarkerState, GoalMode,
        GoalStatus,
    },
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
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

fn ready_agent(store: &mut Store, project: &str, now: i64) -> String {
    let launch_id = Ulid::new().to_string();
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                verb: "launch".to_owned(),
                args: json!({"project":project, "workspace":"synthetic-workspace"}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now,
        )
        .expect("admit launch");
    let agent = admission.command.target.expect("launch target");
    store.dispatch(&launch_id, now).expect("dispatch launch");
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("{project}:ready"),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: format!("{project}:ready"),
                    data: json!({"command_id":launch_id,
                        "native_scope": format!("{project}:scope"),
                        "native_ref": format!("{project}:ref")}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit session ready");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle launch");
    agent
}

fn send_request(agent: &str, text: &str) -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "send");
    request.target = Some(agent.to_owned());
    request.expected_generation = Some(1);
    request.args = json!({"text": text});
    request
}

fn native_batch(agent: &str, generation: i64, kind: &str, data: Value) -> SourceBatch {
    SourceBatch {
        batch_id: Ulid::new().to_string(),
        agent_id: agent.to_owned(),
        generation,
        source: "stream".to_owned(),
        events: vec![NativeEvent {
            ts: None,
            kind: kind.to_owned(),
            native_id: Ulid::new().to_string(),
            data,
        }],
        cursor: None,
    }
}

fn attach_observer(store: &mut Store, now: i64) -> String {
    let request = SubmitCommand {
        id: Ulid::new().to_string(),
        target: None,
        verb: "attach".to_owned(),
        args: json!({"mode":"observe","profile":"fake","harness":"fake",
            "native_scope":"fixture:scope","native_ref":"fixture-native",
            "profile_snapshot":{"harness":"fake"}}),
        expected_generation: None,
        expected_turn_id: None,
        request_id: None,
        probe: false,
    };
    let admitted = store.submit("owner", request, now).expect("admit attach");
    let agent = admitted.command.target.expect("attach target");
    let attach_id = admitted.command.id.clone();
    store.dispatch(&attach_id, now).expect("dispatch attach");
    agent
}

fn definition(text: &str) -> GoalDefinition {
    GoalDefinition {
        text: text.to_owned(),
        check: vec!["check-goal".to_owned()],
        check_cwd: None,
        check_timeout_ms: DEFAULT_GOAL_CHECK_TIMEOUT_MS,
        max_turns: Some(4),
        max_rounds: Some(6),
        max_without_progress: Some(2),
    }
}

fn commit_for(
    agent: &str,
    revision: i64,
    round_key: i64,
    check: Option<GoalCheckResult>,
    blocked_reason: Option<&str>,
    continuation: Option<GoalContinuation>,
) -> GoalRoundCommit {
    GoalRoundCommit {
        agent: agent.to_owned(),
        mode: "eliot".to_owned(),
        expected_generation: 1,
        expected_revision: revision,
        round_key,
        check,
        blocked_reason: blocked_reason.map(ToOwned::to_owned),
        continuation,
    }
}

fn passed_check() -> GoalCheckResult {
    GoalCheckResult {
        exit_code: Some(0),
        output_complete: true,
        output_digest: Some("digest-pass".to_owned()),
        observation: Some("ok".to_owned()),
    }
}

fn failed_check(digest: &str) -> GoalCheckResult {
    GoalCheckResult {
        exit_code: Some(1),
        output_complete: true,
        output_digest: Some(digest.to_owned()),
        observation: Some("failing".to_owned()),
    }
}

fn claim(store: &mut Store, agent: &str, revision: i64, key: i64, now: i64) {
    store
        .goal_claim_round(agent, "eliot", key, revision, now)
        .expect("claim round");
}

/// Drive one admitted send through a full turn so the Agent is cleanly idle
/// again and a later round can claim a fresh key.
fn complete_send(store: &mut Store, agent: &str, send_id: &str, turn_id: &str, now: i64) {
    store.dispatch(send_id, now).expect("dispatch send");
    for (kind, data) in [
        (
            "turn.started",
            json!({"command_id": send_id, "turn_id": turn_id}),
        ),
        (
            "turn.completed",
            json!({"command_id": send_id, "turn_id": turn_id, "status": "completed"}),
        ),
    ] {
        store
            .put_batch(native_batch(agent, 1, kind, data), now)
            .expect("commit turn event");
    }
    store
        .settle(send_id, CommandState::Applied, None, now)
        .expect("settle send");
}

#[test]
fn goal_migration_installs_v19_goals_table() {
    let store = Store::open(Path::new(":memory:")).expect("open Store");
    let version: i64 = store
        .connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .expect("read Store migration version");
    assert_eq!(version, super::schema::CURRENT_SCHEMA_VERSION);
    assert_eq!(version, 19);
    let found: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'goals'",
            [],
            |row| row.get(0),
        )
        .expect("count goals table");
    assert_eq!(found, 1);
    let markers: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'goal_markers'",
            [],
            |row| row.get(0),
        )
        .expect("count goal_markers table");
    assert_eq!(markers, 1);
}

#[test]
fn goal_set_binds_the_current_generation_and_starts_at_revision_one() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-set", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    assert_eq!(view.agent, agent);
    assert_eq!(view.mode, GoalMode::Eliot);
    assert_eq!(view.generation, 1);
    assert_eq!(view.revision, 1);
    assert_eq!(view.status, GoalStatus::Active);
    assert_eq!(view.definition.text, "green");
    assert_eq!(view.counters, GoalCounters::default());
    assert!(view.last_round_key.is_none());
    let reread = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(reread, view);
    assert!(store.goal_get(&agent, "eliot").unwrap().is_some());
}

#[test]
fn goal_set_is_idempotent_for_identical_definitions() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-idempotent", now);
    let first = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let second = store
        .goal_set(&agent, "eliot", &definition("green"), now + 1)
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(second.revision, 1);
}

#[test]
fn goal_set_change_advances_revision_and_resets_counters() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-change", now);
    store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let changed = store
        .goal_set(&agent, "eliot", &definition("greener"), now + 1)
        .unwrap();
    assert_eq!(changed.revision, 2);
    assert_eq!(changed.status, GoalStatus::Active);
    assert_eq!(changed.definition.text, "greener");
    assert_eq!(changed.counters, GoalCounters::default());
}

#[test]
fn goal_set_rejects_missing_agents_and_bad_definitions() {
    let mut store = store();
    let now = now_ms();
    let error = store
        .goal_set("a9", "eliot", &definition("green"), now)
        .unwrap_err();
    assert_eq!(error.code, "not_found");
    let agent = ready_agent(&mut store, "goal-invalid", now);
    let mut blank = definition("green");
    blank.text = " ".to_owned();
    assert_eq!(
        store
            .goal_set(&agent, "eliot", &blank, now)
            .unwrap_err()
            .code,
        "invalid"
    );
    assert_eq!(
        store
            .goal_set("1", "eliot", &definition("green"), now)
            .unwrap_err()
            .code,
        "invalid"
    );
}

#[test]
fn goal_native_mode_is_unsupported_everywhere() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-native", now);
    assert_eq!(
        store
            .goal_set(&agent, "native", &definition("green"), now)
            .unwrap_err()
            .code,
        "unsupported"
    );
    assert_eq!(
        store.goal_get(&agent, "native").unwrap_err().code,
        "unsupported"
    );
    assert_eq!(
        store
            .goal_pause(&agent, "native", None, now)
            .unwrap_err()
            .code,
        "unsupported"
    );
    assert_eq!(
        store.goal_clear(&agent, "native", now).unwrap_err().code,
        "unsupported"
    );
    assert_eq!(
        store.goal_resume(&agent, "native", now).unwrap_err().code,
        "unsupported"
    );
    assert_eq!(
        store
            .goal_round_probe(&agent, "native", 0, now)
            .unwrap_err()
            .code,
        "unsupported"
    );
    let commit = GoalRoundCommit {
        mode: "native".to_owned(),
        ..commit_for(&agent, 1, 0, Some(passed_check()), None, None)
    };
    assert_eq!(
        store.goal_commit_round(&commit, now).unwrap_err().code,
        "unsupported"
    );
}

#[test]
fn goal_observe_only_agents_take_no_goals() {
    let mut store = store();
    let now = now_ms();
    let agent = attach_observer(&mut store, now);
    assert!(store.get_agent(&agent).unwrap().unwrap().observe_only);
    assert_eq!(
        store
            .goal_set(&agent, "eliot", &definition("green"), now)
            .unwrap_err()
            .code,
        "not_allowed"
    );
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", 0, now)
            .unwrap_err()
            .code,
        "not_allowed"
    );
}

#[test]
fn goal_pause_clear_resume_follow_revision_semantics() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-transitions", now);
    store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let paused = store
        .goal_pause(&agent, "eliot", Some("waiting"), now + 1)
        .unwrap();
    assert_eq!(paused.revision, 2);
    assert_eq!(paused.status, GoalStatus::Paused);
    assert_eq!(paused.paused_reason.as_deref(), Some("waiting"));
    let repaused = store
        .goal_pause(&agent, "eliot", Some("waiting"), now + 2)
        .unwrap();
    assert_eq!(repaused.revision, 2);
    let resumed = store.goal_resume(&agent, "eliot", now + 3).unwrap();
    assert_eq!(resumed.revision, 3);
    assert_eq!(resumed.status, GoalStatus::Active);
    assert!(resumed.paused_reason.is_none());
    let reread = store.goal_resume(&agent, "eliot", now + 4).unwrap();
    assert_eq!(reread.revision, 3);
    let cleared = store.goal_clear(&agent, "eliot", now + 5).unwrap();
    assert_eq!(cleared.revision, 4);
    assert_eq!(cleared.status, GoalStatus::Cleared);
    assert_eq!(
        store
            .goal_resume(&agent, "eliot", now + 6)
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(
        store
            .goal_pause(&agent, "eliot", None, now + 7)
            .unwrap_err()
            .code,
        "bad_state"
    );
    let reset = store
        .goal_set(&agent, "eliot", &definition("green"), now + 8)
        .unwrap();
    assert_eq!(reset.revision, 5);
    assert_eq!(reset.status, GoalStatus::Active);
}

#[test]
fn goal_probe_reports_idle_facts_for_a_clean_round() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-probe", now);
    store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let high_water = store.high_water().unwrap();
    assert!(high_water > 0);
    let probe = store
        .goal_round_probe(&agent, "eliot", high_water, now + 1)
        .unwrap();
    assert_eq!(probe.agent_state, AgentState::Idle);
    assert!(probe.active_turn.is_none());
    assert_eq!(probe.pending_count, 0);
    assert!(probe.unhealthy_sources.is_empty());
    assert_eq!(probe.open_commands, 0);
    assert_eq!(probe.unknown_commands, 0);
    assert_eq!(probe.max_seq, high_water);
    assert_eq!(probe.turn_count, 0);
    assert_eq!(probe.view.revision, 1);
}

#[test]
fn goal_probe_reports_open_and_unknown_gates() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-gates", now);
    store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let stuck = send_request(&agent, "stuck input");
    let stuck_id = stuck.id.clone();
    store.submit("owner", stuck, now + 1).expect("admit send");
    store.dispatch(&stuck_id, now + 1).expect("dispatch send");
    store
        .settle(&stuck_id, CommandState::Unknown, None, now + 2)
        .expect("settle unknown");
    let open = send_request(&agent, "queued input");
    store.submit("owner", open, now + 3).expect("admit send");
    let probe = store
        .goal_round_probe(&agent, "eliot", store.high_water().unwrap(), now + 4)
        .unwrap();
    assert_eq!(probe.open_commands, 1);
    assert_eq!(probe.unknown_commands, 1);
}

#[test]
fn goal_probe_reports_unhealthy_harness_sources_and_pending_requests() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-health", now);
    store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    // Monitor sources stay advisory: a stale process monitor never gates a
    // round, while the harness stream does.
    store
        .ensure_process_source(&agent, 1, now + 1)
        .expect("ensure process source");
    store
        .set_process_source_status(&agent, 1, "stale", false, now + 2)
        .expect("mark monitor stale");
    let idle_key = store.high_water().unwrap();
    let probe = store
        .goal_round_probe(&agent, "eliot", idle_key, now + 3)
        .unwrap();
    assert!(probe.unhealthy_sources.is_empty());
    store
        .connection
        .execute(
            "UPDATE source_state SET health = 'stale' WHERE agent = ?1 AND source = 'stream'",
            rusqlite::params![eliot_core::parse_agent_id(&agent).unwrap()],
        )
        .expect("mark harness stream stale");
    let send = send_request(&agent, "needs approval");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 3).expect("admit send");
    store.dispatch(&send_id, now + 3).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-1"}),
            ),
            now + 4,
        )
        .expect("start turn");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "request.opened",
                json!({"command_id": send_id, "turn_id": "turn-1",
                    "request_id": "req-1", "kind": "approval",
                    "details": {}}),
            ),
            now + 5,
        )
        .expect("open request");
    // The open request left a non-idle latest event: it cannot be a
    // round key. The older idle key still probes the live facts.
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", store.high_water().unwrap(), now + 6)
            .unwrap_err()
            .code,
        "stale_round"
    );
    let probe = store
        .goal_round_probe(&agent, "eliot", idle_key, now + 6)
        .unwrap();
    assert_eq!(probe.agent_state, AgentState::Waiting);
    assert_eq!(probe.active_turn.as_deref(), Some("turn-1"));
    assert_eq!(probe.pending_count, 1);
    assert_eq!(probe.unhealthy_sources, vec!["stream".to_owned()]);
}

#[test]
fn goal_probe_refuses_consumed_and_terminal_rounds() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-consumed", now);
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", 0, now)
            .unwrap_err()
            .code,
        "not_found"
    );
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let key = store.high_water().unwrap();
    let commit = commit_for(&agent, view.revision, key, Some(passed_check()), None, None);
    claim(&mut store, &agent, view.revision, key, now + 1);
    store.goal_commit_round(&commit, now + 1).unwrap();
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", key, now + 2)
            .unwrap_err()
            .code,
        "bad_state"
    );
}

#[test]
fn goal_generation_drift_pauses_on_probe() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-drift", now);
    store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    store
        .put_batch(native_batch(&agent, 1, "session.ended", json!({})), now + 1)
        .expect("commit terminal session fact");
    let resume = SubmitCommand {
        id: Ulid::new().to_string(),
        target: Some(agent.clone()),
        verb: "resume".to_owned(),
        args: json!({}),
        expected_generation: Some(1),
        expected_turn_id: None,
        request_id: None,
        probe: false,
    };
    let resume_id = resume.id.clone();
    store
        .submit("owner", resume, now + 3)
        .expect("admit resume");
    store
        .dispatch(&resume_id, now + 3)
        .expect("dispatch resume");
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().generation, 2);
    let error = store
        .goal_round_probe(&agent, "eliot", store.high_water().unwrap(), now + 4)
        .unwrap_err();
    assert_eq!(error.code, "stale_generation");
    let paused = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(paused.status, GoalStatus::Paused);
    assert_eq!(paused.revision, 2);
    assert_eq!(
        store
            .goal_resume(&agent, "eliot", now + 5)
            .unwrap_err()
            .code,
        "stale_generation"
    );
    let rebound = store
        .goal_set(&agent, "eliot", &definition("green"), now + 6)
        .unwrap();
    assert_eq!(rebound.generation, 2);
    assert_eq!(rebound.revision, 3);
    assert_eq!(rebound.status, GoalStatus::Active);
}

#[test]
fn goal_commit_completes_on_exit_zero() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-complete", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 1);
    let result = store
        .goal_commit_round(
            &commit_for(&agent, view.revision, key, Some(passed_check()), None, None),
            now + 1,
        )
        .unwrap();
    assert_eq!(result.outcome, super::GoalCommitOutcome::Completed);
    assert!(result.admission.is_none());
    assert_eq!(result.view.status, GoalStatus::Completed);
    assert_eq!(result.view.revision, 2);
    assert_eq!(result.view.counters.rounds, 1);
    assert_eq!(result.view.counters.last_exit_code, Some(0));
    assert_eq!(
        result.view.last_round_key.as_deref(),
        Some(format!("idle:{key}").as_str())
    );
}

#[test]
fn goal_commit_admits_one_ordinary_send_continuation() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-continue", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let key = store.high_water().unwrap();
    let send_id = Ulid::new().to_string();
    claim(&mut store, &agent, view.revision, key, now + 1);
    let result = store
        .goal_commit_round(
            &commit_for(
                &agent,
                view.revision,
                key,
                Some(failed_check("digest-a")),
                None,
                Some(GoalContinuation {
                    id: send_id.clone(),
                    text: "keep going".to_owned(),
                }),
            ),
            now + 1,
        )
        .unwrap();
    assert_eq!(
        result.outcome,
        super::GoalCommitOutcome::Continued {
            command_id: send_id.clone()
        }
    );
    let admission = result.admission.expect("continuation admission");
    assert_eq!(admission.command.id, send_id);
    assert_eq!(admission.command.verb, "send");
    assert!(!admission.events.is_empty());
    assert_eq!(result.view.status, GoalStatus::Active);
    assert_eq!(result.view.revision, 2);
    assert_eq!(result.view.counters.rounds, 1);
    let stored = store.get_command(&send_id).unwrap().unwrap();
    assert_eq!(stored.verb, "send");
    assert_eq!(stored.target.as_deref(), Some(agent.as_str()));
    assert_eq!(stored.args["text"], json!("keep going"));
}

#[test]
fn goal_commit_refuses_a_consumed_round_key() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-replay", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let key = store.high_water().unwrap();
    let commit = commit_for(&agent, view.revision, key, Some(passed_check()), None, None);
    claim(&mut store, &agent, view.revision, key, now + 1);
    store.goal_commit_round(&commit, now + 1).unwrap();
    let replay = store.goal_commit_round(&commit, now + 2).unwrap_err();
    assert_eq!(replay.code, "bad_state");
    // A continued round stays active but its key is consumed: the same key
    // can never commit twice.
    store
        .goal_set(&agent, "eliot", &definition("green"), now + 3)
        .unwrap();
    let current = store.goal_get(&agent, "eliot").unwrap().unwrap();
    let fresh_key = store.high_water().unwrap();
    let continued = commit_for(
        &agent,
        current.revision,
        fresh_key,
        Some(failed_check("digest-a")),
        None,
        Some(GoalContinuation {
            id: Ulid::new().to_string(),
            text: "keep going".to_owned(),
        }),
    );
    claim(&mut store, &agent, current.revision, fresh_key, now + 4);
    let first = store.goal_commit_round(&continued, now + 4).unwrap();
    assert_eq!(first.view.status, GoalStatus::Active);
    let repeat = commit_for(
        &agent,
        first.view.revision,
        fresh_key,
        Some(failed_check("digest-a")),
        None,
        Some(GoalContinuation {
            id: Ulid::new().to_string(),
            text: "keep going".to_owned(),
        }),
    );
    assert_eq!(
        store.goal_commit_round(&repeat, now + 5).unwrap_err().code,
        "stale_round"
    );
    // The refused replay admitted no second send.
    assert_eq!(
        store
            .list_unsettled_commands(100)
            .unwrap()
            .iter()
            .filter(|command| command.verb == "send")
            .count(),
        1
    );
}

#[test]
fn goal_commit_refuses_revision_and_event_drift() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-stale", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 1);
    store.goal_pause(&agent, "eliot", None, now + 1).unwrap();
    store.goal_resume(&agent, "eliot", now + 2).unwrap();
    let drifted = commit_for(&agent, view.revision, key, Some(passed_check()), None, None);
    assert_eq!(
        store.goal_commit_round(&drifted, now + 3).unwrap_err().code,
        "stale_round"
    );
    store
        .goal_abandon_round(&agent, "eliot", 1, view.revision, key, now + 3)
        .unwrap();
    let current = store.goal_get(&agent, "eliot").unwrap().unwrap();
    // The abandoned key stays burned across pause → resume: it can never
    // be claimed again, at any revision. Event drift is tested below on a
    // fresh key instead.
    assert_eq!(
        store
            .goal_claim_round(&agent, "eliot", key, current.revision, now + 4)
            .unwrap_err()
            .code,
        "stale_round"
    );
    let fresh_send = send_request(&agent, "advance the key");
    let fresh_id = fresh_send.id.clone();
    store
        .submit("owner", fresh_send, now + 4)
        .expect("admit send");
    complete_send(&mut store, &agent, &fresh_id, "turn-drift-0", now + 4);
    let fresh = store.high_water().unwrap();
    assert!(fresh > key);
    claim(&mut store, &agent, current.revision, fresh, now + 5);
    let late = send_request(&agent, "late input");
    let late_id = late.id.clone();
    store
        .submit("owner", late, now + 6)
        .expect("admit late send");
    store
        .dispatch(&late_id, now + 6)
        .expect("dispatch late send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": late_id, "turn_id": "turn-9"}),
            ),
            now + 6,
        )
        .expect("commit late event");
    assert!(store.high_water().unwrap() > fresh);
    let stale_key = commit_for(
        &agent,
        current.revision,
        fresh,
        Some(passed_check()),
        None,
        None,
    );
    assert_eq!(
        store
            .goal_commit_round(&stale_key, now + 7)
            .unwrap_err()
            .code,
        "stale_round"
    );
}

#[test]
fn goal_commit_blocks_without_a_check_for_an_exhausted_budget() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-budget", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 1);
    let result = store
        .goal_commit_round(
            &commit_for(&agent, view.revision, key, None, Some("round_budget"), None),
            now + 1,
        )
        .unwrap();
    assert_eq!(
        result.outcome,
        super::GoalCommitOutcome::Blocked {
            reason: "round_budget".to_owned()
        }
    );
    assert_eq!(result.view.status, GoalStatus::Blocked);
    assert_eq!(result.view.blocked_reason.as_deref(), Some("round_budget"));
    assert_eq!(result.view.counters.rounds, 0);
    assert!(result.admission.is_none());
}

#[test]
fn goal_commit_blocks_a_failed_check_against_frozen_limits() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-limits", now);
    let mut budgeted = definition("green");
    budgeted.max_rounds = Some(1);
    let view = store.goal_set(&agent, "eliot", &budgeted, now).unwrap();
    let send_id = Ulid::new().to_string();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 1);
    let blocked = store
        .goal_commit_round(
            &commit_for(
                &agent,
                view.revision,
                key,
                Some(failed_check("digest-a")),
                None,
                Some(GoalContinuation {
                    id: send_id.clone(),
                    text: "keep going".to_owned(),
                }),
            ),
            now + 1,
        )
        .unwrap();
    assert_eq!(
        blocked.outcome,
        super::GoalCommitOutcome::Blocked {
            reason: "round_budget".to_owned()
        }
    );
    assert!(blocked.admission.is_none());
    assert!(store.get_command(&send_id).unwrap().is_none());
}

#[test]
fn goal_commit_blocks_on_turn_limit_and_no_progress_streaks() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-turn-limit", now);
    let mut limited = definition("green");
    limited.max_turns = Some(1);
    limited.max_without_progress = None;
    limited.max_rounds = None;
    store.goal_set(&agent, "eliot", &limited, now).unwrap();
    let send = send_request(&agent, "drive a turn");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 1).expect("admit send");
    store.dispatch(&send_id, now + 1).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-1"}),
            ),
            now + 2,
        )
        .expect("start turn");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.completed",
                json!({"command_id": send_id, "turn_id": "turn-1",
                    "status": "completed"}),
            ),
            now + 3,
        )
        .expect("complete turn");
    store
        .settle(&send_id, CommandState::Applied, None, now + 3)
        .expect("settle send");
    let current = store.goal_get(&agent, "eliot").unwrap().unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, current.revision, key, now + 4);
    let blocked = store
        .goal_commit_round(
            &commit_for(
                &agent,
                current.revision,
                key,
                Some(failed_check("digest-a")),
                None,
                Some(GoalContinuation {
                    id: Ulid::new().to_string(),
                    text: "keep going".to_owned(),
                }),
            ),
            now + 4,
        )
        .unwrap();
    assert_eq!(
        blocked.outcome,
        super::GoalCommitOutcome::Blocked {
            reason: "turn_limit".to_owned()
        }
    );
    assert_eq!(blocked.view.counters.turns_used, 1);

    let mut streaky = definition("green");
    streaky.max_without_progress = Some(1);
    streaky.max_rounds = None;
    streaky.max_turns = None;
    let agent = ready_agent(&mut store, "goal-no-progress", now + 5);
    let view = store.goal_set(&agent, "eliot", &streaky, now + 5).unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 6);
    let first = store
        .goal_commit_round(
            &commit_for(
                &agent,
                view.revision,
                key,
                Some(failed_check("digest-a")),
                None,
                Some(GoalContinuation {
                    id: Ulid::new().to_string(),
                    text: "keep going".to_owned(),
                }),
            ),
            now + 6,
        )
        .unwrap();
    let super::GoalCommitOutcome::Continued { command_id } = &first.outcome else {
        panic!("first round continues, got {:?}", first.outcome);
    };
    complete_send(&mut store, &agent, command_id, "turn-np-1", now + 6);
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, first.view.revision, key, now + 7);
    let second = store
        .goal_commit_round(
            &commit_for(
                &agent,
                first.view.revision,
                key,
                Some(failed_check("digest-a")),
                None,
                Some(GoalContinuation {
                    id: Ulid::new().to_string(),
                    text: "keep going".to_owned(),
                }),
            ),
            now + 7,
        )
        .unwrap();
    assert_eq!(
        second.outcome,
        super::GoalCommitOutcome::Blocked {
            reason: "no_progress".to_owned()
        }
    );
}

#[test]
fn goal_commit_completes_despite_a_spent_budget() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-success", now);
    let mut budgeted = definition("green");
    budgeted.max_rounds = Some(1);
    budgeted.max_turns = Some(1);
    let view = store.goal_set(&agent, "eliot", &budgeted, now).unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 1);
    let completed = store
        .goal_commit_round(
            &commit_for(&agent, view.revision, key, Some(passed_check()), None, None),
            now + 1,
        )
        .unwrap();
    assert_eq!(completed.outcome, super::GoalCommitOutcome::Completed);
}

#[test]
fn goal_no_progress_counter_skips_incomplete_output() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-progress", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let continued =
        |store: &mut Store, revision: i64, key: i64, check: GoalCheckResult, now: i64| {
            claim(store, &agent, revision, key, now);
            let result = store
                .goal_commit_round(
                    &commit_for(
                        &agent,
                        revision,
                        key,
                        Some(check),
                        None,
                        Some(GoalContinuation {
                            id: Ulid::new().to_string(),
                            text: "keep going".to_owned(),
                        }),
                    ),
                    now,
                )
                .unwrap();
            let super::GoalCommitOutcome::Continued { command_id } = &result.outcome else {
                panic!("round continues, got {:?}", result.outcome);
            };
            let command_id = command_id.clone();
            (result, command_id)
        };
    let key = store.high_water().unwrap();
    let (first, send_id) = continued(
        &mut store,
        view.revision,
        key,
        failed_check("digest-a"),
        now + 1,
    );
    assert_eq!(first.view.counters.consecutive_same_output, 0);
    assert_eq!(
        first.view.counters.last_output_digest.as_deref(),
        Some("digest-a")
    );
    complete_send(&mut store, &agent, &send_id, "turn-pc-1", now + 1);
    let key = store.high_water().unwrap();
    let (second, send_id) = continued(
        &mut store,
        first.view.revision,
        key,
        failed_check("digest-a"),
        now + 2,
    );
    assert_eq!(second.view.counters.consecutive_same_output, 1);
    complete_send(&mut store, &agent, &send_id, "turn-pc-2", now + 2);
    let incomplete = GoalCheckResult {
        exit_code: Some(1),
        output_complete: false,
        output_digest: Some("digest-a".to_owned()),
        observation: Some("truncated".to_owned()),
    };
    let key = store.high_water().unwrap();
    let (third, send_id) = continued(&mut store, second.view.revision, key, incomplete, now + 3);
    // Incomplete output cannot prove no progress: the streak and the stored
    // digest stay untouched while the round still counts.
    assert_eq!(third.view.counters.consecutive_same_output, 1);
    assert_eq!(
        third.view.counters.last_output_digest.as_deref(),
        Some("digest-a")
    );
    assert_eq!(third.view.counters.rounds, 3);
    complete_send(&mut store, &agent, &send_id, "turn-pc-3", now + 3);
    let key = store.high_water().unwrap();
    let (changed, _) = continued(
        &mut store,
        third.view.revision,
        key,
        failed_check("digest-b"),
        now + 4,
    );
    assert_eq!(changed.view.counters.consecutive_same_output, 0);
    assert_eq!(
        changed.view.counters.last_output_digest.as_deref(),
        Some("digest-b")
    );
}

#[test]
fn goal_claim_refuses_a_second_round_while_one_is_in_flight() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-flight", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 1);
    for other in [key, key + 1] {
        assert_eq!(
            store
                .goal_claim_round(&agent, "eliot", other, view.revision, now + 2)
                .unwrap_err()
                .code,
            "busy"
        );
    }
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", key, now + 2)
            .unwrap_err()
            .code,
        "busy"
    );
    // Abandoning releases single flight but burns the key.
    store
        .goal_abandon_round(&agent, "eliot", 1, view.revision, key, now + 3)
        .unwrap();
    store
        .goal_abandon_round(&agent, "eliot", 1, view.revision, key, now + 3)
        .expect("abandon is idempotent");
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", key, now + 4)
            .unwrap_err()
            .code,
        "stale_round"
    );
    assert_eq!(
        store
            .goal_claim_round(&agent, "eliot", key, view.revision, now + 4)
            .unwrap_err()
            .code,
        "stale_round"
    );
    let shown = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(shown.status, GoalStatus::Active);
    assert_eq!(shown.counters.rounds, 0);
    let marker = shown.round_marker.expect("burned key stays visible");
    assert_eq!(marker.state, GoalMarkerState::Unknown);
    assert_eq!(marker.round_key, format!("idle:{key}"));
}

#[test]
fn goal_commit_requires_its_claimed_marker() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-marker", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let key = store.high_water().unwrap();
    let round_id = Ulid::new().to_string();
    let unclaimed = commit_for(
        &agent,
        view.revision,
        key,
        Some(failed_check("digest-a")),
        None,
        Some(GoalContinuation {
            id: round_id.clone(),
            text: "keep going".to_owned(),
        }),
    );
    assert_eq!(
        store
            .goal_commit_round(&unclaimed, now + 1)
            .unwrap_err()
            .code,
        "stale_round"
    );
    assert!(store.get_command(&round_id).unwrap().is_none());
    // A claim for a fresh key does not authorize the old commit.
    let send = send_request(&agent, "advance the key");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 2).expect("admit send");
    complete_send(&mut store, &agent, &send_id, "turn-marker-1", now + 2);
    let fresh = store.high_water().unwrap();
    assert!(fresh > key);
    claim(&mut store, &agent, view.revision, fresh, now + 3);
    assert_eq!(
        store
            .goal_commit_round(&unclaimed, now + 3)
            .unwrap_err()
            .code,
        "stale_round"
    );
    let continued = commit_for(
        &agent,
        view.revision,
        fresh,
        Some(failed_check("digest-a")),
        None,
        Some(GoalContinuation {
            id: Ulid::new().to_string(),
            text: "keep going".to_owned(),
        }),
    );
    store.goal_commit_round(&continued, now + 3).unwrap();
    let markers: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM goal_markers", [], |row| row.get(0))
        .expect("count markers");
    assert_eq!(markers, 0);
}

fn goal_crash_store_path() -> PathBuf {
    std::env::temp_dir().join(format!("eliot-goal-crash-{}.sqlite", Ulid::new()))
}

fn remove_goal_crash_store_files(path: &Path) {
    for suffix in ["", "-wal", "-shm"] {
        let mut candidate = path.as_os_str().to_os_string();
        candidate.push(suffix);
        let _ = fs::remove_file(PathBuf::from(candidate));
    }
}

#[test]
fn goal_crash_leaves_an_unknown_marker_and_burns_its_key() {
    let path = goal_crash_store_path();
    let now = now_ms();
    let (agent, key) = {
        let mut store = Store::open(&path).expect("open file Store");
        let agent = ready_agent(&mut store, "goal-crash", now);
        let view = store
            .goal_set(&agent, "eliot", &definition("green"), now)
            .unwrap();
        let key = store.high_water().unwrap();
        store
            .goal_round_probe(&agent, "eliot", key, now)
            .expect("probe before the crash");
        claim(&mut store, &agent, view.revision, key, now);
        // Crash: the handle drops with the marker `running`.
        (agent, key)
    };
    let mut store = Store::open(&path).expect("reopen file Store");
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", key, now + 1)
            .unwrap_err()
            .code,
        "stale_round"
    );
    let shown = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(shown.status, GoalStatus::Active);
    assert_eq!(shown.counters.rounds, 0);
    assert!(shown.last_round_key.is_none());
    let marker = shown.round_marker.expect("crashed marker is visible");
    assert_eq!(marker.state, GoalMarkerState::Unknown);
    assert_eq!(marker.round_key, format!("idle:{key}"));
    // The next round needs a new idle key.
    let send = send_request(&agent, "after the crash");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 2).expect("admit send");
    complete_send(&mut store, &agent, &send_id, "turn-crash-1", now + 2);
    let fresh = store.high_water().unwrap();
    assert!(fresh > key);
    let current = store.goal_get(&agent, "eliot").unwrap().unwrap();
    store
        .goal_round_probe(&agent, "eliot", fresh, now + 3)
        .expect("probe the fresh key");
    claim(&mut store, &agent, current.revision, fresh, now + 3);
    let state: String = store
        .connection
        .query_row(
            "SELECT state FROM goal_markers WHERE round_key = ?1",
            [key],
            |row| row.get(0),
        )
        .expect("read crashed marker");
    assert_eq!(state, "unknown");
    let done = store
        .goal_commit_round(
            &commit_for(
                &agent,
                current.revision,
                fresh,
                Some(passed_check()),
                None,
                None,
            ),
            now + 3,
        )
        .unwrap();
    assert_eq!(done.outcome, super::GoalCommitOutcome::Completed);
    remove_goal_crash_store_files(&path);
}

#[test]
fn goal_commit_refuses_activity_admitted_during_the_check() {
    let mut store = store();
    let now = now_ms();
    // An owner send admitted during the check: no continuation.
    let agent = ready_agent(&mut store, "goal-mid-open", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 1);
    let send = send_request(&agent, "owner input during the check");
    store.submit("owner", send, now + 1).expect("admit send");
    let round_id = Ulid::new().to_string();
    let raced = commit_for(
        &agent,
        view.revision,
        key,
        Some(failed_check("digest-a")),
        None,
        Some(GoalContinuation {
            id: round_id.clone(),
            text: "keep going".to_owned(),
        }),
    );
    assert_eq!(
        store.goal_commit_round(&raced, now + 2).unwrap_err().code,
        "stale_round"
    );
    assert!(store.get_command(&round_id).unwrap().is_none());
    let current = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(current.status, GoalStatus::Active);
    assert_eq!(current.counters.rounds, 0);
    assert!(current.last_round_key.is_none());
    store
        .goal_abandon_round(&agent, "eliot", 1, view.revision, key, now + 2)
        .unwrap();
    let shown = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(
        shown.round_marker.expect("raced key stays visible").state,
        GoalMarkerState::Unknown
    );
    // A turn started during the check.
    let agent = ready_agent(&mut store, "goal-mid-turn", now + 3);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now + 3)
        .unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 4);
    let send = send_request(&agent, "drive a turn");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 4).expect("admit send");
    store.dispatch(&send_id, now + 4).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-mid-1"}),
            ),
            now + 4,
        )
        .expect("start turn");
    let raced = commit_for(&agent, view.revision, key, Some(passed_check()), None, None);
    assert_eq!(
        store.goal_commit_round(&raced, now + 5).unwrap_err().code,
        "stale_round"
    );
    // A request opened during the check.
    let agent = ready_agent(&mut store, "goal-mid-request", now + 6);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now + 6)
        .unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 7);
    let send = send_request(&agent, "needs approval");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 7).expect("admit send");
    store.dispatch(&send_id, now + 7).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-mid-2"}),
            ),
            now + 7,
        )
        .expect("start turn");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "request.opened",
                json!({"command_id": send_id, "turn_id": "turn-mid-2",
                    "request_id": "req-mid-1", "kind": "approval", "details": {}}),
            ),
            now + 7,
        )
        .expect("open request");
    let raced = commit_for(&agent, view.revision, key, Some(passed_check()), None, None);
    assert_eq!(
        store.goal_commit_round(&raced, now + 8).unwrap_err().code,
        "stale_round"
    );
}

#[test]
fn goal_round_key_must_name_a_generation_event() {
    let mut store = store();
    let now = now_ms();
    // An idle Agent generation with no events at all: key 0 names nothing.
    let inserted = store
        .connection
        .execute(
            "INSERT INTO agents(generation, project, harness, profile, state, created_at, updated_at)
             VALUES (1, 'project-test', 'fake', 'fake', 'idle', 1, 1)",
            [],
        )
        .expect("insert idle agent");
    assert_eq!(inserted, 1);
    let agent = format!("a{}", store.connection.last_insert_rowid());
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", 0, now + 1)
            .unwrap_err()
            .code,
        "stale_round"
    );
    assert_eq!(
        store
            .goal_claim_round(&agent, "eliot", 0, view.revision, now + 1)
            .unwrap_err()
            .code,
        "stale_round"
    );
    // A key naming another Agent's event is refused too.
    let other = ready_agent(&mut store, "goal-key-owner", now + 2);
    let other_key = store.high_water().unwrap();
    assert!(other_key > 0);
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", other_key, now + 3)
            .unwrap_err()
            .code,
        "stale_round"
    );
    let _ = other;
}

#[test]
fn goal_show_reports_live_turns_used() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-live-turns", now);
    store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    assert_eq!(
        store
            .goal_get(&agent, "eliot")
            .unwrap()
            .unwrap()
            .counters
            .turns_used,
        0
    );
    let send = send_request(&agent, "outside any round");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 1).expect("admit send");
    complete_send(&mut store, &agent, &send_id, "turn-live-1", now + 1);
    let shown = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(shown.counters.rounds, 0);
    assert_eq!(shown.counters.turns_used, 1);
}

#[test]
fn goal_commit_refuses_a_check_without_an_exit_code() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-no-code", now);
    let view = store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 1);
    let codeless = GoalCheckResult {
        exit_code: None,
        output_complete: true,
        output_digest: Some("digest-a".to_owned()),
        observation: Some("killed by a signal".to_owned()),
    };
    let round_id = Ulid::new().to_string();
    let commit = commit_for(
        &agent,
        view.revision,
        key,
        Some(codeless),
        None,
        Some(GoalContinuation {
            id: round_id.clone(),
            text: "keep going".to_owned(),
        }),
    );
    assert_eq!(
        store.goal_commit_round(&commit, now + 2).unwrap_err().code,
        "unknown"
    );
    assert!(store.get_command(&round_id).unwrap().is_none());
    let current = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(current.counters.rounds, 0);
    assert!(current.last_round_key.is_none());
}

#[test]
fn goal_burned_key_survives_pause_and_resume() {
    let path = goal_crash_store_path();
    let now = now_ms();
    let (agent, key) = {
        let mut store = Store::open(&path).expect("open file Store");
        let agent = ready_agent(&mut store, "goal-pause-burn", now);
        let view = store
            .goal_set(&agent, "eliot", &definition("green"), now)
            .unwrap();
        let key = store.high_water().unwrap();
        claim(&mut store, &agent, view.revision, key, now);
        // Crash: the handle drops with the marker `running`.
        (agent, key)
    };
    let mut store = Store::open(&path).expect("reopen file Store");
    store.goal_pause(&agent, "eliot", None, now + 1).unwrap();
    let resumed = store.goal_resume(&agent, "eliot", now + 2).unwrap();
    assert_eq!(resumed.status, GoalStatus::Active);
    // The crashed key stays burned at the new revision: refused at probe
    // AND claim, and still visible in `goal show`.
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", key, now + 3)
            .unwrap_err()
            .code,
        "stale_round"
    );
    assert_eq!(
        store
            .goal_claim_round(&agent, "eliot", key, resumed.revision, now + 3)
            .unwrap_err()
            .code,
        "stale_round"
    );
    let shown = store.goal_get(&agent, "eliot").unwrap().unwrap();
    let marker = shown.round_marker.expect("burned key stays visible");
    assert_eq!(marker.state, GoalMarkerState::Unknown);
    assert_eq!(marker.round_key, format!("idle:{key}"));
    remove_goal_crash_store_files(&path);
}

#[test]
fn goal_round_key_must_name_an_idle_event() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-idle-key", now);
    store
        .goal_set(&agent, "eliot", &definition("green"), now)
        .unwrap();
    // Start a turn and leave it open: the latest event names a busy Agent.
    let send = send_request(&agent, "leave a turn open");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 1).expect("admit send");
    store.dispatch(&send_id, now + 1).expect("dispatch send");
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-busy-1"}),
            ),
            now + 1,
        )
        .expect("start turn");
    let key = store.high_water().unwrap();
    let subject: String = store
        .connection
        .query_row("SELECT subject FROM events WHERE seq = ?1", [key], |row| {
            row.get(0)
        })
        .expect("read latest event subject");
    assert!(
        !subject.contains("\"state\":\"idle\""),
        "latest event must be non-idle: {subject}"
    );
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", key, now + 2)
            .unwrap_err()
            .code,
        "stale_round"
    );
    // Completing the turn leaves an idle latest event: the probe passes.
    store
        .put_batch(
            native_batch(
                &agent,
                1,
                "turn.completed",
                json!({"command_id": send_id, "turn_id": "turn-busy-1",
                    "status": "completed"}),
            ),
            now + 3,
        )
        .expect("complete turn");
    store
        .settle(&send_id, CommandState::Applied, None, now + 3)
        .expect("settle send");
    let fresh = store.high_water().unwrap();
    assert!(fresh > key);
    store
        .goal_round_probe(&agent, "eliot", fresh, now + 4)
        .expect("idle latest event probes");
}
