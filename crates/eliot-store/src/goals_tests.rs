use super::{GoalCheckResult, GoalContinuation, GoalRoundCommit, GoalRoundKey, Store};
use eliot_core::{
    AgentState, CommandState, GitPathStatus, GitSnapshot, NativeEvent, SourceBatch, SubmitCommand,
    api::{
        DEFAULT_GOAL_CHECK_TIMEOUT_MS, GoalCounters, GoalDefinition, GoalMarkerState, GoalMode,
        GoalStatus, MIN_GOAL_CADENCE_MS,
    },
    parse_agent_id,
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
                selector: None,
                verb: "launch".to_owned(),
                args: json!({"project":project, "workspace":"synthetic-workspace", "harness":"fake", "profile":"fake", "profile_snapshot":{}}),
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
        selector: None,
        verb: "attach".to_owned(),
        args: json!({"mode":"observe","profile":"fixture","harness":"codex",
            "native_scope":"fixture:scope","native_ref":"fixture-native",
            "profile_snapshot":{"harness":"codex"}}),
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
        cadence_ms: None,
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
        round_key: GoalRoundKey::Idle(round_key),
        // Idle claims observe the key itself as the high-water.
        claim_seq: round_key,
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
    let probe = store
        .goal_round_probe(agent, "eliot", GoalRoundKey::Idle(key), now)
        .expect("probe before claim");
    store
        .goal_claim_round(
            agent,
            "eliot",
            GoalRoundKey::Idle(key),
            revision,
            probe.spent_by_goal_continuation,
            now,
        )
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
fn goal_migration_installs_v20_round_key_kind() {
    let store = Store::open(Path::new(":memory:")).expect("open Store");
    let version: i64 = store
        .connection
        .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
            row.get(0)
        })
        .expect("read Store migration version");
    assert_eq!(version, super::schema::CURRENT_SCHEMA_VERSION);
    assert_eq!(version, 26);
    let found: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'goals'",
            [],
            |row| row.get(0),
        )
        .expect("count goals table");
    assert_eq!(found, 1);
    let mut kinds = store
        .connection
        .prepare("SELECT name FROM pragma_table_info('goal_markers')")
        .expect("read goal_markers columns")
        .query_map([], |row| row.get::<_, String>(0))
        .expect("map goal_markers columns")
        .collect::<rusqlite::Result<Vec<String>>>()
        .expect("collect goal_markers columns");
    kinds.sort();
    assert!(
        kinds.contains(&"round_key_kind".to_owned()),
        "markers carry the key form: {kinds:?}"
    );
}

#[test]
fn goal_set_binds_the_current_generation_and_starts_at_revision_one() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-set", now);
    let view = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
        .unwrap();
    let second = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now + 1,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
        .unwrap();
    let changed = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("greener"),
            now + 1,
        )
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
        .goal_set(
            "a9",
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
        .unwrap_err();
    assert_eq!(error.code, "not_found");
    let agent = ready_agent(&mut store, "goal-invalid", now);
    let mut blank = definition("green");
    blank.text = " ".to_owned();
    assert_eq!(
        store
            .goal_set(
                &agent,
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                &blank,
                now
            )
            .unwrap_err()
            .code,
        "invalid"
    );
    assert_eq!(
        store
            .goal_set(
                "1",
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                &definition("green"),
                now
            )
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
            .goal_set(
                &agent,
                "native",
                &Ulid::new().to_string(),
                "owner",
                &definition("green"),
                now
            )
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
            .goal_pause(
                &agent,
                "native",
                &Ulid::new().to_string(),
                "owner",
                None,
                now
            )
            .unwrap_err()
            .code,
        "unsupported"
    );
    assert_eq!(
        store
            .goal_clear(&agent, "native", &Ulid::new().to_string(), "owner", now)
            .unwrap_err()
            .code,
        "unsupported"
    );
    assert_eq!(
        store
            .goal_resume(&agent, "native", &Ulid::new().to_string(), "owner", now)
            .unwrap_err()
            .code,
        "unsupported"
    );
    assert_eq!(
        store
            .goal_round_probe(&agent, "native", GoalRoundKey::Idle(0), now)
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
            .goal_set(
                &agent,
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                &definition("green"),
                now
            )
            .unwrap_err()
            .code,
        "not_allowed"
    );
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(0), now)
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
        .unwrap();
    let paused = store
        .goal_pause(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            Some("waiting"),
            now + 1,
        )
        .unwrap();
    assert_eq!(paused.revision, 2);
    assert_eq!(paused.status, GoalStatus::Paused);
    assert_eq!(paused.paused_reason.as_deref(), Some("waiting"));
    let repaused = store
        .goal_pause(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            Some("waiting"),
            now + 2,
        )
        .unwrap();
    assert_eq!(repaused.revision, 2);
    let resumed = store
        .goal_resume(&agent, "eliot", &Ulid::new().to_string(), "owner", now + 3)
        .unwrap();
    assert_eq!(resumed.revision, 3);
    assert_eq!(resumed.status, GoalStatus::Active);
    assert!(resumed.paused_reason.is_none());
    let reread = store
        .goal_resume(&agent, "eliot", &Ulid::new().to_string(), "owner", now + 4)
        .unwrap();
    assert_eq!(reread.revision, 3);
    let cleared = store
        .goal_clear(&agent, "eliot", &Ulid::new().to_string(), "owner", now + 5)
        .unwrap();
    assert_eq!(cleared.revision, 4);
    assert_eq!(cleared.status, GoalStatus::Cleared);
    assert_eq!(
        store
            .goal_resume(&agent, "eliot", &Ulid::new().to_string(), "owner", now + 6)
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(
        store
            .goal_pause(
                &agent,
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                None,
                now + 7
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
    let reset = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now + 8,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
        .unwrap();
    let high_water = store.high_water().unwrap();
    assert!(high_water > 0);
    let probe = store
        .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(high_water), now + 1)
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
        .goal_round_probe(
            &agent,
            "eliot",
            GoalRoundKey::Idle(store.high_water().unwrap()),
            now + 4,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
        .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(idle_key), now + 3)
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
            .goal_round_probe(
                &agent,
                "eliot",
                GoalRoundKey::Idle(store.high_water().unwrap()),
                now + 6
            )
            .unwrap_err()
            .code,
        "stale_round"
    );
    let probe = store
        .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(idle_key), now + 6)
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
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(0), now)
            .unwrap_err()
            .code,
        "not_found"
    );
    let view = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
        .unwrap();
    let key = store.high_water().unwrap();
    let commit = commit_for(&agent, view.revision, key, Some(passed_check()), None, None);
    claim(&mut store, &agent, view.revision, key, now + 1);
    store.goal_commit_round(&commit, now + 1).unwrap();
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(key), now + 2)
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
        .unwrap();
    crate::tests::install_live_custody_observations(&mut store, &agent, 1, now);
    store
        .put_batch(native_batch(&agent, 1, "session.ended", json!({})), now + 1)
        .expect("commit terminal session fact");
    crate::tests::simulate_normal_stop_proof(&mut store, &agent, 1, now + 1);
    let resume = SubmitCommand {
        id: Ulid::new().to_string(),
        target: Some(agent.clone()),
        selector: None,
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
        .goal_round_probe(
            &agent,
            "eliot",
            GoalRoundKey::Idle(store.high_water().unwrap()),
            now + 4,
        )
        .unwrap_err();
    assert_eq!(error.code, "stale_generation");
    let paused = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(paused.status, GoalStatus::Paused);
    assert_eq!(paused.revision, 2);
    assert_eq!(
        store
            .goal_resume(&agent, "eliot", &Ulid::new().to_string(), "owner", now + 5)
            .unwrap_err()
            .code,
        "stale_generation"
    );
    let rebound = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now + 6,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now + 3,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
        .unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 1);
    store
        .goal_pause(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            None,
            now + 1,
        )
        .unwrap();
    store
        .goal_resume(&agent, "eliot", &Ulid::new().to_string(), "owner", now + 2)
        .unwrap();
    let drifted = commit_for(&agent, view.revision, key, Some(passed_check()), None, None);
    assert_eq!(
        store.goal_commit_round(&drifted, now + 3).unwrap_err().code,
        "stale_round"
    );
    store
        .goal_abandon_round(
            &agent,
            "eliot",
            1,
            view.revision,
            GoalRoundKey::Idle(key),
            now + 3,
        )
        .unwrap();
    let current = store.goal_get(&agent, "eliot").unwrap().unwrap();
    // The abandoned key stays burned across pause → resume: it can never
    // be claimed again, at any revision. Event drift is tested below on a
    // fresh key instead.
    assert_eq!(
        store
            .goal_claim_round(
                &agent,
                "eliot",
                GoalRoundKey::Idle(key),
                current.revision,
                false,
                now + 4
            )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
    let view = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &budgeted,
            now,
        )
        .unwrap();
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
    store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &limited,
            now,
        )
        .unwrap();
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
    // The default turn/round bounds stay: automatic Goals need one, and
    // neither fires before the no-progress streak does.
    let agent = ready_agent(&mut store, "goal-no-progress", now + 5);
    let view = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &streaky,
            now + 5,
        )
        .unwrap();
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
    let view = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &budgeted,
            now,
        )
        .unwrap();
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
        .unwrap();
    let key = store.high_water().unwrap();
    claim(&mut store, &agent, view.revision, key, now + 1);
    for other in [key, key + 1] {
        assert_eq!(
            store
                .goal_claim_round(
                    &agent,
                    "eliot",
                    GoalRoundKey::Idle(other),
                    view.revision,
                    false,
                    now + 2
                )
                .unwrap_err()
                .code,
            "busy"
        );
    }
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(key), now + 2)
            .unwrap_err()
            .code,
        "busy"
    );
    // Abandoning releases single flight but burns the key.
    store
        .goal_abandon_round(
            &agent,
            "eliot",
            1,
            view.revision,
            GoalRoundKey::Idle(key),
            now + 3,
        )
        .unwrap();
    store
        .goal_abandon_round(
            &agent,
            "eliot",
            1,
            view.revision,
            GoalRoundKey::Idle(key),
            now + 3,
        )
        .expect("abandon is idempotent");
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(key), now + 4)
            .unwrap_err()
            .code,
        "stale_round"
    );
    assert_eq!(
        store
            .goal_claim_round(
                &agent,
                "eliot",
                GoalRoundKey::Idle(key),
                view.revision,
                false,
                now + 4
            )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
            .goal_set(
                &agent,
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                &definition("green"),
                now,
            )
            .unwrap();
        let key = store.high_water().unwrap();
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(key), now)
            .expect("probe before the crash");
        claim(&mut store, &agent, view.revision, key, now);
        // Crash: the handle drops with the marker `running`.
        (agent, key)
    };
    let mut store = Store::open(&path).expect("reopen file Store");
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(key), now + 1)
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
        .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(fresh), now + 3)
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
        .goal_abandon_round(
            &agent,
            "eliot",
            1,
            view.revision,
            GoalRoundKey::Idle(key),
            now + 2,
        )
        .unwrap();
    let shown = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(
        shown.round_marker.expect("raced key stays visible").state,
        GoalMarkerState::Unknown
    );
    // A turn started during the check.
    let agent = ready_agent(&mut store, "goal-mid-turn", now + 3);
    let view = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now + 3,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now + 6,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
        .unwrap();
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(0), now + 1)
            .unwrap_err()
            .code,
        "stale_round"
    );
    assert_eq!(
        store
            .goal_claim_round(
                &agent,
                "eliot",
                GoalRoundKey::Idle(0),
                view.revision,
                false,
                now + 1
            )
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
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(other_key), now + 3)
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
            .goal_set(
                &agent,
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                &definition("green"),
                now,
            )
            .unwrap();
        let key = store.high_water().unwrap();
        claim(&mut store, &agent, view.revision, key, now);
        // Crash: the handle drops with the marker `running`.
        (agent, key)
    };
    let mut store = Store::open(&path).expect("reopen file Store");
    store
        .goal_pause(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            None,
            now + 1,
        )
        .unwrap();
    let resumed = store
        .goal_resume(&agent, "eliot", &Ulid::new().to_string(), "owner", now + 2)
        .unwrap();
    assert_eq!(resumed.status, GoalStatus::Active);
    // The crashed key stays burned at the new revision: refused at probe
    // AND claim, and still visible in `goal show`.
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(key), now + 3)
            .unwrap_err()
            .code,
        "stale_round"
    );
    assert_eq!(
        store
            .goal_claim_round(
                &agent,
                "eliot",
                GoalRoundKey::Idle(key),
                resumed.revision,
                false,
                now + 3
            )
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
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now,
        )
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
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(key), now + 2)
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
        .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(fresh), now + 4)
        .expect("idle latest event probes");
}

fn cadenced_definition(text: &str) -> GoalDefinition {
    GoalDefinition {
        cadence_ms: Some(MIN_GOAL_CADENCE_MS),
        ..definition(text)
    }
}

fn slot_commit_for(
    agent: &str,
    revision: i64,
    slot: i64,
    claim_seq: i64,
    check: Option<GoalCheckResult>,
    blocked_reason: Option<&str>,
    continuation: Option<GoalContinuation>,
) -> GoalRoundCommit {
    GoalRoundCommit {
        agent: agent.to_owned(),
        mode: "eliot".to_owned(),
        expected_generation: 1,
        expected_revision: revision,
        round_key: GoalRoundKey::Slot(slot),
        claim_seq,
        check,
        blocked_reason: blocked_reason.map(ToOwned::to_owned),
        continuation,
    }
}

fn continued_send(text: &str) -> GoalContinuation {
    GoalContinuation {
        id: Ulid::new().to_string(),
        text: text.to_owned(),
    }
}

#[test]
fn goal_cadence_set_captures_a_stable_anchor() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-anchor", now);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &cadenced_definition("green"),
            now,
        )
        .unwrap();
    assert_eq!(set.cadence_anchor_ms, Some(now));
    let paused = store
        .goal_pause(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            None,
            now + 1,
        )
        .unwrap();
    assert_eq!(paused.cadence_anchor_ms, Some(now));
    let resumed = store
        .goal_resume(&agent, "eliot", &Ulid::new().to_string(), "owner", now + 2)
        .unwrap();
    assert_eq!(resumed.cadence_anchor_ms, Some(now));
    // An identical set is a no-op and keeps the grid.
    let same = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &cadenced_definition("green"),
            now + 3,
        )
        .unwrap();
    assert_eq!(same.revision, resumed.revision);
    assert_eq!(same.cadence_anchor_ms, Some(now));
    // A new definition re-anchors.
    let mut changed = cadenced_definition("greener");
    changed.cadence_ms = Some(2 * MIN_GOAL_CADENCE_MS);
    let reset = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &changed,
            now + 4,
        )
        .unwrap();
    assert_eq!(reset.cadence_anchor_ms, Some(now + 4));
    // Dropping the cadence drops the anchor.
    let plain = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            now + 5,
        )
        .unwrap();
    assert!(plain.cadence_anchor_ms.is_none());
}

#[test]
fn goal_slot_round_trip_continues_then_completes() {
    let mut store = store();
    let base = now_ms();
    let agent = ready_agent(&mut store, "goal-slot", base);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &cadenced_definition("green"),
            base,
        )
        .unwrap();
    assert_eq!(set.cadence_anchor_ms, Some(base));
    let first_due = base + MIN_GOAL_CADENCE_MS;
    let probe = store
        .goal_round_probe(&agent, "eliot", GoalRoundKey::Slot(1), first_due)
        .expect("first slot probes when due");
    let claim_seq = store
        .goal_claim_round(
            &agent,
            "eliot",
            GoalRoundKey::Slot(1),
            set.revision,
            probe.spent_by_goal_continuation,
            first_due,
        )
        .expect("claim first slot");
    assert_eq!(claim_seq, store.high_water().unwrap());
    let first = store
        .goal_commit_round(
            &slot_commit_for(
                &agent,
                set.revision,
                1,
                claim_seq,
                Some(failed_check("digest-a")),
                None,
                Some(continued_send("keep going")),
            ),
            first_due,
        )
        .unwrap();
    assert!(matches!(
        first.outcome,
        super::GoalCommitOutcome::Continued { .. }
    ));
    assert_eq!(first.view.last_round_key.as_deref(), Some("slot:1"));
    assert!(first.view.round_marker.is_none());
    let send_id = match &first.outcome {
        super::GoalCommitOutcome::Continued { command_id } => command_id.clone(),
        outcome => panic!("first slot must continue, got {outcome:?}"),
    };
    complete_send(&mut store, &agent, &send_id, "turn-slot-1", first_due + 1);
    let second_due = base + 2 * MIN_GOAL_CADENCE_MS;
    let probe = store
        .goal_round_probe(&agent, "eliot", GoalRoundKey::Slot(2), second_due)
        .expect("second slot probes when due");
    let claim_seq = store
        .goal_claim_round(
            &agent,
            "eliot",
            GoalRoundKey::Slot(2),
            first.view.revision,
            probe.spent_by_goal_continuation,
            second_due,
        )
        .expect("claim second slot");
    let second = store
        .goal_commit_round(
            &slot_commit_for(
                &agent,
                first.view.revision,
                2,
                claim_seq,
                Some(passed_check()),
                None,
                None,
            ),
            second_due,
        )
        .unwrap();
    assert_eq!(second.outcome, super::GoalCommitOutcome::Completed);
    assert_eq!(second.view.last_round_key.as_deref(), Some("slot:2"));
}

#[test]
fn goal_slot_keys_validate_form_range_and_due() {
    let mut store = store();
    let base = now_ms();
    let agent = ready_agent(&mut store, "goal-slot-gates", base);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &cadenced_definition("green"),
            base,
        )
        .unwrap();
    // Slot 0 is malformed, a future slot is not due.
    for key in [GoalRoundKey::Slot(0), GoalRoundKey::Slot(-3)] {
        assert_eq!(
            store
                .goal_round_probe(&agent, "eliot", key, base)
                .unwrap_err()
                .code,
            "invalid"
        );
        assert_eq!(
            store
                .goal_claim_round(&agent, "eliot", key, set.revision, false, base)
                .unwrap_err()
                .code,
            "invalid"
        );
    }
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Slot(9), base)
            .unwrap_err()
            .code,
        "stale_round"
    );
    assert_eq!(
        store
            .goal_claim_round(
                &agent,
                "eliot",
                GoalRoundKey::Slot(9),
                set.revision,
                false,
                base
            )
            .unwrap_err()
            .code,
        "stale_round"
    );
    // A cadenced Goal refuses idle keys.
    let key = store.high_water().unwrap();
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(key), base)
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(
        store
            .goal_claim_round(
                &agent,
                "eliot",
                GoalRoundKey::Idle(key),
                set.revision,
                false,
                base
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
    // A Goal without a cadence refuses slots.
    let plain = ready_agent(&mut store, "goal-slot-plain", base);
    store
        .goal_set(
            &plain,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            base,
        )
        .unwrap();
    assert_eq!(
        store
            .goal_round_probe(
                &plain,
                "eliot",
                GoalRoundKey::Slot(1),
                base + MIN_GOAL_CADENCE_MS
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
}

#[test]
fn goal_slot_burn_ignores_revision_but_separates_forms() {
    let mut store = store();
    let base = now_ms();
    let agent = ready_agent(&mut store, "goal-slot-burn", base);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &definition("green"),
            base,
        )
        .unwrap();
    let key = store.high_water().unwrap();
    let claim_seq = store
        .goal_claim_round(
            &agent,
            "eliot",
            GoalRoundKey::Idle(key),
            set.revision,
            false,
            base,
        )
        .expect("claim idle key");
    assert_eq!(claim_seq, key);
    store
        .goal_abandon_round(
            &agent,
            "eliot",
            1,
            set.revision,
            GoalRoundKey::Idle(key),
            base,
        )
        .expect("abandon idle key");
    // A new cadenced definition re-anchors; the same number as a slot is
    // a different key form and is not burned.
    let anchor = base + 1;
    let cadenced = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &cadenced_definition("green"),
            anchor,
        )
        .unwrap();
    let due = anchor + key * MIN_GOAL_CADENCE_MS;
    let probe = store
        .goal_round_probe(&agent, "eliot", GoalRoundKey::Slot(key), due)
        .expect("same number as a slot is not burned");
    store
        .goal_claim_round(
            &agent,
            "eliot",
            GoalRoundKey::Slot(key),
            cadenced.revision,
            probe.spent_by_goal_continuation,
            due,
        )
        .expect("claim slot");
    store
        .goal_abandon_round(
            &agent,
            "eliot",
            1,
            cadenced.revision,
            GoalRoundKey::Slot(key),
            due,
        )
        .expect("abandon slot");
    let paused = store
        .goal_pause(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            None,
            due + 1,
        )
        .unwrap();
    assert_eq!(paused.cadence_anchor_ms, Some(anchor));
    let resumed = store
        .goal_resume(&agent, "eliot", &Ulid::new().to_string(), "owner", due + 2)
        .unwrap();
    // The burned slot stays refused at probe and claim across the bump.
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Slot(key), due + 3)
            .unwrap_err()
            .code,
        "stale_round"
    );
    assert_eq!(
        store
            .goal_claim_round(
                &agent,
                "eliot",
                GoalRoundKey::Slot(key),
                resumed.revision,
                false,
                due + 3
            )
            .unwrap_err()
            .code,
        "stale_round"
    );
    let shown = store.goal_get(&agent, "eliot").unwrap().unwrap();
    let marker = shown.round_marker.expect("burned slot stays visible");
    assert_eq!(marker.state, GoalMarkerState::Unknown);
    assert_eq!(marker.round_key, format!("slot:{key}"));
}

#[test]
fn goal_slot_commit_refuses_events_during_the_check() {
    let mut store = store();
    let base = now_ms();
    let agent = ready_agent(&mut store, "goal-slot-race", base);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &cadenced_definition("green"),
            base,
        )
        .unwrap();
    let due = base + MIN_GOAL_CADENCE_MS;
    let claim_seq = store
        .goal_claim_round(
            &agent,
            "eliot",
            GoalRoundKey::Slot(1),
            set.revision,
            false,
            due,
        )
        .expect("claim slot");
    // A benign Git binding commits an Agent event without tripping any
    // gate; the slot commit still sees the high-water move.
    store
        .ensure_git_source(&agent, 1, "synthetic-workspace", due + 1)
        .expect("bind git source");
    assert!(store.high_water().unwrap() > claim_seq);
    let commit = slot_commit_for(
        &agent,
        set.revision,
        1,
        claim_seq,
        Some(passed_check()),
        None,
        None,
    );
    assert_eq!(
        store.goal_commit_round(&commit, due + 2).unwrap_err().code,
        "stale_round"
    );
    let shown = store.goal_get(&agent, "eliot").unwrap().unwrap();
    assert_eq!(shown.counters.rounds, 0);
    assert!(shown.last_round_key.is_none());
}

fn git_snapshot(head: &str, dirty: Vec<GitPathStatus>, observed_at: i64) -> GitSnapshot {
    GitSnapshot {
        workspace: "synthetic-workspace".to_owned(),
        head: Some(head.to_owned()),
        branch: None,
        dirty,
        commits: Vec::new(),
        clean_paths: Vec::new(),
        observed_at,
        complete: true,
        history_complete: true,
        history_gap: false,
        base_oid: None,
        committed_paths: Vec::new(),
        base_complete: false,
    }
}

fn observe_git(store: &mut Store, agent: &str, snapshot: &GitSnapshot, now: i64) {
    store
        .ensure_git_source(agent, 1, "synthetic-workspace", now)
        .expect("ensure git source");
    store
        .record_git_observation(agent, 1, snapshot, &[], 0, now)
        .expect("record git observation");
}

fn failing_round(
    store: &mut Store,
    agent: &str,
    revision: i64,
    digest: &str,
    now: i64,
) -> super::GoalRoundCommitResult {
    let key = store.high_water().unwrap();
    store
        .goal_round_probe(agent, "eliot", GoalRoundKey::Idle(key), now)
        .expect("probe idle round");
    claim(store, agent, revision, key, now);
    store
        .goal_commit_round(
            &commit_for(
                agent,
                revision,
                key,
                Some(failed_check(digest)),
                None,
                Some(continued_send("keep going")),
            ),
            now,
        )
        .expect("commit failing round")
}

#[test]
fn goal_fingerprint_head_change_is_progress() {
    let mut store = store();
    let base = now_ms();
    let agent = ready_agent(&mut store, "goal-fp-head", base);
    let mut limited = definition("green");
    limited.max_without_progress = Some(1);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &limited,
            base,
        )
        .unwrap();
    let head_a = "a".repeat(40);
    let head_b = "b".repeat(40);
    observe_git(
        &mut store,
        &agent,
        &git_snapshot(&head_a, Vec::new(), base + 1),
        base + 1,
    );
    let first = failing_round(&mut store, &agent, set.revision, "digest-same", base + 2);
    assert!(matches!(
        first.outcome,
        super::GoalCommitOutcome::Continued { .. }
    ));
    assert_eq!(first.view.counters.consecutive_same_output, 0);
    let before = first
        .view
        .counters
        .last_fingerprint
        .clone()
        .expect("fp stored");
    let send_id = match &first.outcome {
        super::GoalCommitOutcome::Continued { command_id } => command_id.clone(),
        outcome => panic!("first round must continue, got {outcome:?}"),
    };
    complete_send(&mut store, &agent, &send_id, "turn-fp-1", base + 3);
    // Same check output but a new HEAD: progress, not a streak.
    observe_git(
        &mut store,
        &agent,
        &git_snapshot(&head_b, Vec::new(), base + 4),
        base + 4,
    );
    let second = failing_round(
        &mut store,
        &agent,
        first.view.revision,
        "digest-same",
        base + 5,
    );
    assert!(
        matches!(second.outcome, super::GoalCommitOutcome::Continued { .. }),
        "head change must reset the streak: {:?}",
        second.outcome
    );
    assert_eq!(second.view.counters.consecutive_same_output, 0);
    let after = second
        .view
        .counters
        .last_fingerprint
        .clone()
        .expect("fp stored");
    assert_ne!(before, after);
    let send_id = match &second.outcome {
        super::GoalCommitOutcome::Continued { command_id } => command_id.clone(),
        outcome => panic!("second round must continue, got {outcome:?}"),
    };
    complete_send(&mut store, &agent, &send_id, "turn-fp-2", base + 6);
    // No workspace change now: the same output extends the streak to the block.
    let third = failing_round(
        &mut store,
        &agent,
        second.view.revision,
        "digest-same",
        base + 7,
    );
    assert_eq!(
        third.outcome,
        super::GoalCommitOutcome::Blocked {
            reason: "no_progress".to_owned()
        }
    );
    assert_eq!(
        third.view.counters.last_fingerprint.as_deref(),
        Some(after.as_str())
    );
}

#[test]
fn goal_fingerprint_dirty_change_is_progress() {
    let mut store = store();
    let base = now_ms();
    let agent = ready_agent(&mut store, "goal-fp-dirty", base);
    let mut limited = definition("green");
    limited.max_without_progress = Some(1);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &limited,
            base,
        )
        .unwrap();
    let head = "c".repeat(40);
    observe_git(
        &mut store,
        &agent,
        &git_snapshot(&head, Vec::new(), base + 1),
        base + 1,
    );
    let first = failing_round(&mut store, &agent, set.revision, "digest-same", base + 2);
    let send_id = match &first.outcome {
        super::GoalCommitOutcome::Continued { command_id } => command_id.clone(),
        outcome => panic!("first round must continue, got {outcome:?}"),
    };
    complete_send(&mut store, &agent, &send_id, "turn-fp-dirty-1", base + 3);
    // Same HEAD but a new dirty path: progress, not a streak.
    observe_git(
        &mut store,
        &agent,
        &git_snapshot(
            &head,
            vec![GitPathStatus {
                path: "src/lib.rs".to_owned(),
                status: "modified".to_owned(),
                original_path: None,
            }],
            base + 4,
        ),
        base + 4,
    );
    let second = failing_round(
        &mut store,
        &agent,
        first.view.revision,
        "digest-same",
        base + 5,
    );
    assert!(
        matches!(second.outcome, super::GoalCommitOutcome::Continued { .. }),
        "dirty change must reset the streak: {:?}",
        second.outcome
    );
    assert_eq!(second.view.counters.consecutive_same_output, 0);
}

#[test]
fn goal_incomplete_fingerprint_never_blocks_no_progress() {
    let mut store = store();
    let base = now_ms();
    let agent = ready_agent(&mut store, "goal-fp-partial", base);
    let mut limited = definition("green");
    limited.max_without_progress = Some(1);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &limited,
            base,
        )
        .unwrap();
    // A binding with no snapshot yet: incomplete.
    store
        .ensure_git_source(&agent, 1, "synthetic-workspace", base + 1)
        .expect("ensure git source");
    let first = failing_round(&mut store, &agent, set.revision, "digest-same", base + 2);
    assert!(matches!(
        first.outcome,
        super::GoalCommitOutcome::Continued { .. }
    ));
    assert_eq!(first.view.counters.consecutive_same_output, 0);
    assert!(first.view.counters.last_fingerprint.is_none());
    let send_id = match &first.outcome {
        super::GoalCommitOutcome::Continued { command_id } => command_id.clone(),
        outcome => panic!("first round must continue, got {outcome:?}"),
    };
    complete_send(&mut store, &agent, &send_id, "turn-fp-partial-1", base + 3);
    // A partial snapshot and then a gapped one: still incomplete.
    let mut partial = git_snapshot(&"d".repeat(40), Vec::new(), base + 4);
    partial.complete = false;
    observe_git(&mut store, &agent, &partial, base + 4);
    let second = failing_round(
        &mut store,
        &agent,
        first.view.revision,
        "digest-same",
        base + 5,
    );
    assert!(
        matches!(second.outcome, super::GoalCommitOutcome::Continued { .. }),
        "partial fingerprint must not block: {:?}",
        second.outcome
    );
    assert_eq!(second.view.counters.consecutive_same_output, 0);
    assert!(second.view.counters.last_fingerprint.is_none());
    let send_id = match &second.outcome {
        super::GoalCommitOutcome::Continued { command_id } => command_id.clone(),
        outcome => panic!("second round must continue, got {outcome:?}"),
    };
    complete_send(&mut store, &agent, &send_id, "turn-fp-partial-2", base + 6);
    let mut gapped = git_snapshot(&"e".repeat(40), Vec::new(), base + 7);
    gapped.history_gap = true;
    observe_git(&mut store, &agent, &gapped, base + 7);
    let third = failing_round(
        &mut store,
        &agent,
        second.view.revision,
        "digest-same",
        base + 8,
    );
    assert!(
        matches!(third.outcome, super::GoalCommitOutcome::Continued { .. }),
        "gapped fingerprint must not block: {:?}",
        third.outcome
    );
    assert_eq!(third.view.counters.consecutive_same_output, 0);
    assert!(third.view.counters.last_fingerprint.is_none());
}

#[test]
fn goal_unhealthy_git_source_resets_no_progress_streak() {
    let mut store = store();
    let base = now_ms();
    let agent = ready_agent(&mut store, "goal-fp-health", base);
    let mut limited = definition("green");
    limited.max_without_progress = Some(1);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &limited,
            base,
        )
        .unwrap();
    observe_git(
        &mut store,
        &agent,
        &git_snapshot(&"f".repeat(40), Vec::new(), base + 1),
        base + 1,
    );
    let first = failing_round(&mut store, &agent, set.revision, "digest-same", base + 2);
    assert!(first.view.counters.last_fingerprint.is_some());
    let send_id = match &first.outcome {
        super::GoalCommitOutcome::Continued { command_id } => command_id.clone(),
        outcome => panic!("first round must continue, got {outcome:?}"),
    };
    complete_send(&mut store, &agent, &send_id, "turn-fp-health-1", base + 3);
    // The monitor reports trouble: the frozen snapshot no longer proves stasis.
    store
        .set_git_source_status(&agent, 1, "error", false, base + 4)
        .expect("mark git source error");
    let second = failing_round(
        &mut store,
        &agent,
        first.view.revision,
        "digest-same",
        base + 5,
    );
    assert!(
        matches!(second.outcome, super::GoalCommitOutcome::Continued { .. }),
        "unhealthy source must reset the streak: {:?}",
        second.outcome
    );
    assert_eq!(second.view.counters.consecutive_same_output, 0);
    assert!(second.view.counters.last_fingerprint.is_none());
}

#[test]
fn goal_truncated_edit_page_is_an_incomplete_fingerprint() {
    let mut store = store();
    let base = now_ms();
    let agent = ready_agent(&mut store, "goal-fp-truncate", base);
    let mut limited = definition("green");
    limited.max_without_progress = Some(1);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &limited,
            base,
        )
        .unwrap();
    observe_git(
        &mut store,
        &agent,
        &git_snapshot(&"a1b2c3d4e5".repeat(4), Vec::new(), base + 1),
        base + 1,
    );
    let first = failing_round(&mut store, &agent, set.revision, "digest-same", base + 2);
    assert!(first.view.counters.last_fingerprint.is_some());
    let send_id = match &first.outcome {
        super::GoalCommitOutcome::Continued { command_id } => command_id.clone(),
        outcome => panic!("first round must continue, got {outcome:?}"),
    };
    complete_send(&mut store, &agent, &send_id, "turn-fp-truncate-1", base + 3);
    // More attributed paths than the fingerprint page holds: it cannot
    // prove stasis over a truncated set.
    let agent_num = parse_agent_id(&agent).expect("parse agent");
    for index in 0..1001 {
        store
            .connection
            .execute(
                "INSERT INTO agent_edits(agent, generation, workspace, path, updated_at)
                 VALUES (?1, 1, 'synthetic-workspace', ?2, ?3)",
                rusqlite::params![agent_num, format!("src/file{index:04}.rs"), base + 4],
            )
            .expect("insert attributed edit");
    }
    let second = failing_round(
        &mut store,
        &agent,
        first.view.revision,
        "digest-same",
        base + 5,
    );
    assert!(
        matches!(second.outcome, super::GoalCommitOutcome::Continued { .. }),
        "truncated edit page must reset the streak: {:?}",
        second.outcome
    );
    assert_eq!(second.view.counters.consecutive_same_output, 0);
    assert!(second.view.counters.last_fingerprint.is_none());
}

#[test]
fn goal_native_driving_reports_an_active_native_goal() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-native-driving", now);
    assert!(!store.goal_native_driving(&agent).unwrap());
    let agent_num = parse_agent_id(&agent).expect("parse agent");
    let definition = serde_json::to_string(&definition("green")).expect("encode definition");
    store
        .connection
        .execute(
            "INSERT INTO goals(agent, mode, generation, revision, definition, state,
                 last_round_key, updated_at)
             VALUES (?1, 'native', 1, 1, ?2, '{\"status\":\"active\"}', NULL, ?3)",
            rusqlite::params![agent_num, definition, now],
        )
        .expect("insert native goal");
    assert!(store.goal_native_driving(&agent).unwrap());
    store
        .connection
        .execute(
            "UPDATE goals SET state = '{\"status\":\"paused\"}'
             WHERE agent = ?1 AND mode = 'native'",
            rusqlite::params![agent_num],
        )
        .expect("pause native goal");
    assert!(!store.goal_native_driving(&agent).unwrap());
}

#[test]
fn goal_set_refuses_an_unbounded_automatic_goal() {
    let mut store = store();
    let now = now_ms();
    let agent = ready_agent(&mut store, "goal-unbounded", now);
    let mut unbounded = definition("green");
    unbounded.max_turns = None;
    unbounded.max_rounds = None;
    unbounded.max_without_progress = None;
    assert_eq!(
        store
            .goal_set(
                &agent,
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                &unbounded,
                now
            )
            .unwrap_err()
            .code,
        "invalid"
    );
    // Either bound enables automatic driving; max_without_progress alone
    // does not bound the driving itself.
    for (max_turns, max_rounds) in [(Some(4), None), (None, Some(6))] {
        let mut bounded = unbounded.clone();
        bounded.max_turns = max_turns;
        bounded.max_rounds = max_rounds;
        store
            .goal_set(
                &agent,
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                &bounded,
                now + 1,
            )
            .expect("bounded goal sets");
    }
    let mut progress_only = unbounded.clone();
    progress_only.max_without_progress = Some(2);
    assert_eq!(
        store
            .goal_set(
                &agent,
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                &progress_only,
                now + 2
            )
            .unwrap_err()
            .code,
        "invalid"
    );
    // Cadenced Goals round only on explicit slots: no bound needed.
    let mut cadenced = unbounded.clone();
    cadenced.cadence_ms = Some(MIN_GOAL_CADENCE_MS);
    store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &cadenced,
            now + 3,
        )
        .expect("cadenced goal sets without a bound");
}

#[test]
fn goal_committed_slots_are_monotonic() {
    let mut store = store();
    let base = now_ms();
    let agent = ready_agent(&mut store, "goal-slot-monotonic", base);
    let set = store
        .goal_set(
            &agent,
            "eliot",
            &Ulid::new().to_string(),
            "owner",
            &cadenced_definition("green"),
            base,
        )
        .unwrap();
    let ninth_due = base + 9 * MIN_GOAL_CADENCE_MS;
    let probe = store
        .goal_round_probe(&agent, "eliot", GoalRoundKey::Slot(9), ninth_due)
        .expect("slot 9 probes when due");
    let claim_seq = store
        .goal_claim_round(
            &agent,
            "eliot",
            GoalRoundKey::Slot(9),
            set.revision,
            probe.spent_by_goal_continuation,
            ninth_due,
        )
        .expect("claim slot 9");
    let committed = store
        .goal_commit_round(
            &slot_commit_for(
                &agent,
                set.revision,
                9,
                claim_seq,
                Some(failed_check("digest-9")),
                None,
                Some(continued_send("keep going")),
            ),
            ninth_due,
        )
        .unwrap();
    assert!(matches!(
        committed.outcome,
        super::GoalCommitOutcome::Continued { .. }
    ));
    assert_eq!(committed.view.last_round_key.as_deref(), Some("slot:9"));
    let send_id = match &committed.outcome {
        super::GoalCommitOutcome::Continued { command_id } => command_id.clone(),
        outcome => panic!("slot 9 must continue, got {outcome:?}"),
    };
    complete_send(&mut store, &agent, &send_id, "turn-slot-9", ninth_due + 1);
    // Slot 1 is long due but not newer than the committed slot 9.
    assert_eq!(
        store
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Slot(1), ninth_due)
            .unwrap_err()
            .code,
        "stale_round"
    );
    assert_eq!(
        store
            .goal_claim_round(
                &agent,
                "eliot",
                GoalRoundKey::Slot(1),
                committed.view.revision,
                false,
                ninth_due,
            )
            .unwrap_err()
            .code,
        "stale_round"
    );
    // A newer slot still rounds.
    let tenth_due = base + 10 * MIN_GOAL_CADENCE_MS;
    store
        .goal_round_probe(&agent, "eliot", GoalRoundKey::Slot(10), tenth_due)
        .expect("newer slot probes");
}

#[test]
fn goal_reopen_reads_idle_and_slot_markers_as_unknown() {
    let path = goal_crash_store_path();
    let base = now_ms();
    let (idle_agent, idle_key, slot_agent) = {
        let mut store = Store::open(&path).expect("open file Store");
        let idle_agent = ready_agent(&mut store, "goal-reopen-idle", base);
        let idle_key = store.high_water().unwrap();
        let slot_agent = ready_agent(&mut store, "goal-reopen-slot", base);
        let idle_view = store
            .goal_set(
                &idle_agent,
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                &definition("green"),
                base,
            )
            .unwrap();
        let slot_view = store
            .goal_set(
                &slot_agent,
                "eliot",
                &Ulid::new().to_string(),
                "owner",
                &cadenced_definition("green"),
                base,
            )
            .unwrap();
        let first_due = base + MIN_GOAL_CADENCE_MS;
        store
            .goal_round_probe(
                &idle_agent,
                "eliot",
                GoalRoundKey::Idle(idle_key),
                first_due,
            )
            .expect("probe idle key");
        claim(
            &mut store,
            &idle_agent,
            idle_view.revision,
            idle_key,
            first_due,
        );
        let probe = store
            .goal_round_probe(&slot_agent, "eliot", GoalRoundKey::Slot(1), first_due)
            .expect("probe first slot");
        store
            .goal_claim_round(
                &slot_agent,
                "eliot",
                GoalRoundKey::Slot(1),
                slot_view.revision,
                probe.spent_by_goal_continuation,
                first_due,
            )
            .expect("claim first slot");
        // Crash: the handle drops with both markers `running`.
        (idle_agent, idle_key, slot_agent)
    };
    let mut store = Store::open(&path).expect("reopen file Store");
    // Both key forms survived under the round_key_kind column.
    let mut kinds: Vec<(i64, String)> = store
        .connection
        .prepare("SELECT round_key, round_key_kind FROM goal_markers ORDER BY agent")
        .expect("prepare marker kinds")
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .expect("read marker kinds")
        .collect::<rusqlite::Result<Vec<(i64, String)>>>()
        .expect("collect marker kinds");
    kinds.sort();
    assert_eq!(
        kinds,
        vec![(1, "slot".to_owned()), (idle_key, "idle".to_owned())]
    );
    // Both crashed keys stay burned at probe AND claim.
    for (agent, key) in [
        (idle_agent.as_str(), GoalRoundKey::Idle(idle_key)),
        (slot_agent.as_str(), GoalRoundKey::Slot(1)),
    ] {
        let revision = store.goal_get(agent, "eliot").unwrap().unwrap().revision;
        assert_eq!(
            store
                .goal_round_probe(agent, "eliot", key, base + MIN_GOAL_CADENCE_MS + 1)
                .unwrap_err()
                .code,
            "stale_round"
        );
        assert_eq!(
            store
                .goal_claim_round(
                    agent,
                    "eliot",
                    key,
                    revision,
                    false,
                    base + MIN_GOAL_CADENCE_MS + 1
                )
                .unwrap_err()
                .code,
            "stale_round"
        );
        let shown = store.goal_get(agent, "eliot").unwrap().unwrap();
        let marker = shown.round_marker.expect("crashed marker is visible");
        assert_eq!(marker.state, GoalMarkerState::Unknown);
        assert_eq!(marker.round_key, key.text());
    }
    // Fresh keys still claim, durably rewriting the crashed rows.
    let send = send_request(&idle_agent, "after the crash");
    let send_id = send.id.clone();
    store.submit("owner", send, base + 2).expect("admit send");
    complete_send(&mut store, &idle_agent, &send_id, "turn-reopen-1", base + 2);
    let fresh = store.high_water().unwrap();
    assert!(fresh > idle_key);
    let current = store.goal_get(&idle_agent, "eliot").unwrap().unwrap();
    store
        .goal_round_probe(&idle_agent, "eliot", GoalRoundKey::Idle(fresh), base + 3)
        .expect("probe the fresh idle key");
    claim(&mut store, &idle_agent, current.revision, fresh, base + 3);
    let second_due = base + 2 * MIN_GOAL_CADENCE_MS;
    let current_slot = store.goal_get(&slot_agent, "eliot").unwrap().unwrap();
    let probe = store
        .goal_round_probe(&slot_agent, "eliot", GoalRoundKey::Slot(2), second_due)
        .expect("probe the fresh slot");
    store
        .goal_claim_round(
            &slot_agent,
            "eliot",
            GoalRoundKey::Slot(2),
            current_slot.revision,
            probe.spent_by_goal_continuation,
            second_due,
        )
        .expect("claim the fresh slot");
    let mut states: Vec<(String, String)> = store
        .connection
        .prepare(
            "SELECT round_key_kind, state FROM goal_markers
             WHERE (round_key_kind = 'idle' AND round_key = ?1)
                OR (round_key_kind = 'slot' AND round_key = 1)
             ORDER BY round_key_kind",
        )
        .expect("prepare crashed states")
        .query_map(rusqlite::params![idle_key], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .expect("read crashed states")
        .collect::<rusqlite::Result<Vec<(String, String)>>>()
        .expect("collect crashed states");
    states.sort();
    assert_eq!(
        states,
        vec![
            ("idle".to_owned(), "unknown".to_owned()),
            ("slot".to_owned(), "unknown".to_owned()),
        ]
    );
    remove_goal_crash_store_files(&path);
}
