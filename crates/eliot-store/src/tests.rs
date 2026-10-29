use super::{Store, contains_secret_marker};
use eliot_core::{
    AgentState, CommandState, NativeEvent, NativeStoreBinding, SourceBatch, SourceGap,
    SubmitCommand,
};
use eliot_core::{
    WorkspaceCancelOutcome, WorkspaceCreationOutcome, WorkspacePhase, WorkspacePreparationInput,
};
use serde_json::json;
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use ulid::Ulid;

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
                    data: json!({"command_id":launch_id}),
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

#[test]
fn session_end_keeps_dispatched_stop_pending_until_exact_reply() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "stop-session-ended", now);
    let stop_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent.clone()),
                verb: "stop".to_owned(),
                args: json!({}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
            },
            now + 1,
        )
        .expect("admit stop");
    store.dispatch(&stop_id, now + 1).expect("dispatch stop");

    store
        .put_batch(
            SourceBatch {
                batch_id: "stop-session-ended:session-ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "stop-session-ended:session-ended".to_owned(),
                    data: json!({}),
                }],
                cursor: None,
            },
            now + 2,
        )
        .expect("commit authoritative session end");

    assert_eq!(
        store
            .get_agent(&agent)
            .expect("Agent")
            .expect("owner")
            .state,
        AgentState::Stopped
    );
    assert_eq!(
        store
            .get_command(&stop_id)
            .expect("command")
            .expect("stop")
            .state,
        CommandState::Dispatched,
        "session end alone does not acknowledge the stop RPC"
    );

    let settled = store
        .settle(
            &stop_id,
            CommandState::Applied,
            Some(json!({"state":"applied"})),
            now + 3,
        )
        .expect("the exact stop RPC reply settles its command");
    assert_eq!(settled.value.state, CommandState::Applied);
    assert_eq!(
        store
            .get_agent(&agent)
            .expect("Agent")
            .expect("owner")
            .state,
        AgentState::Stopped
    );
}

#[test]
fn generic_generation_loss_keeps_dispatched_stop_unknown() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "stop-generation-lost", now);
    let stop_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent.clone()),
                verb: "stop".to_owned(),
                args: json!({}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
            },
            now + 1,
        )
        .expect("admit stop");
    store.dispatch(&stop_id, now + 1).expect("dispatch stop");

    store
        .mark_lost(&agent, 1, now + 2)
        .expect("generation loss remains uncertain");

    assert_eq!(
        store
            .get_command(&stop_id)
            .expect("command")
            .expect("stop")
            .state,
        CommandState::Unknown
    );
    assert_eq!(
        store
            .get_agent(&agent)
            .expect("Agent")
            .expect("owner")
            .state,
        AgentState::Lost
    );
    assert!(
        store.dispatch(&stop_id, now + 3).is_err(),
        "unknown stop cannot be replayed"
    );
}

#[test]
fn workspace_exceptions_stay_marker_bound_across_unknown_readback() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let start_id = Ulid::new().to_string();
    let input = WorkspacePreparationInput {
        project: "workspace-transitions".to_owned(),
        repo_path: "/synthetic-owner".to_owned(),
        repo_common_dir: "/synthetic-owner/.git".to_owned(),
        requested_branch: "codex/audit".to_owned(),
        base_ref: Some("HEAD".to_owned()),
        base_oid: "a".repeat(40),
        worktree_path: "/synthetic-worktree".to_owned(),
        frozen_setup: Vec::new(),
    };
    let start = store.submit_with_workspace("owner", SubmitCommand {
        id: start_id.clone(), target: None, verb: "launch".to_owned(),
        args: json!({"project":input.project,"workspace":input.repo_path,"worktree":input.requested_branch}),
        expected_generation: Some(0), expected_turn_id: None, request_id: None,
    }, input.clone(), now).expect("admit prepared launch");
    let agent = start.command.target.expect("workspace owner");
    for state in [CommandState::Failed, CommandState::Unknown] {
        assert_eq!(
            store
                .settle(&start_id, state, None, now)
                .expect_err("generic settlement cannot claim preparation exception")
                .code,
            "bad_state"
        );
    }
    assert_eq!(
        store
            .get_command(&start_id)
            .expect("command")
            .expect("start")
            .state,
        CommandState::Accepted
    );
    store
        .settle_workspace_creation(
            &start_id,
            1,
            WorkspaceCreationOutcome::Unknown {
                code: "creation_readback_unknown".to_owned(),
                message: "synthetic uncertainty".to_owned(),
            },
            now,
        )
        .expect("typed marker permits unknown before native dispatch");
    assert_eq!(
        store
            .get_command(&start_id)
            .expect("command")
            .expect("start")
            .state,
        CommandState::Unknown
    );
    assert_eq!(
        store
            .get_agent(&agent)
            .expect("Agent")
            .expect("owner")
            .generation,
        0
    );
    assert_eq!(store.get_agent_token(&agent, 0).expect("no token"), None);
    store
        .dispatch(&start_id, now)
        .expect_err("unknown native effect cannot be replayed");
    let ready = store
        .settle_workspace_creation(
            &start_id,
            1,
            WorkspaceCreationOutcome::Verified {
                repo_path: input.repo_path,
                repo_common_dir: input.repo_common_dir,
                worktree_path: input.worktree_path,
                branch: input.requested_branch,
                head: input.base_oid,
            },
            now,
        )
        .expect("exact creation readback restores only remaining work");
    assert_eq!(ready.value.phase, WorkspacePhase::Ready);
    assert_eq!(
        store
            .get_command(&start_id)
            .expect("command")
            .expect("start")
            .state,
        CommandState::Accepted
    );
    assert_eq!(
        store
            .get_agent(&agent)
            .expect("Agent")
            .expect("owner")
            .generation,
        0
    );

    let stop_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent.clone()),
                verb: "stop".to_owned(),
                args: json!({}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
            },
            now,
        )
        .expect("admit exact pre-native stop");
    store
        .dispatch(&stop_id, now)
        .expect("dispatch cancellation marker");
    store
        .recover_on_restart(now)
        .expect("recover pending cleanup without native dispatch");
    assert_eq!(
        store
            .get_command(&start_id)
            .expect("command")
            .expect("start")
            .state,
        CommandState::Unknown
    );
    assert_eq!(
        store
            .get_command(&stop_id)
            .expect("command")
            .expect("stop")
            .state,
        CommandState::Unknown
    );
    store
        .settle_workspace_cancel(&stop_id, &start_id, 1, WorkspaceCancelOutcome::Stopped, now)
        .expect("exact cleanup settles Unknown start cancellation and stop independently");
    assert_eq!(
        store
            .get_command(&start_id)
            .expect("command")
            .expect("start")
            .state,
        CommandState::Cancelled
    );
    assert_eq!(
        store
            .get_command(&stop_id)
            .expect("command")
            .expect("stop")
            .state,
        CommandState::Applied
    );
    let owner = store.get_agent(&agent).expect("Agent").expect("owner");
    assert_eq!(owner.generation, 0);
    assert_eq!(owner.state, AgentState::Stopped);
    assert_eq!(store.get_agent_token(&agent, 0).expect("no token"), None);
    assert!(
        store
            .list_source_states(&agent, 0)
            .expect("no native source installed")
            .is_empty()
    );
}

#[test]
fn send_association_emits_applied_once_with_native_batch_ack() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let launch_id = Ulid::new().to_string();
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                verb: "launch".to_owned(),
                args: json!({"project":"send-event", "workspace":"synthetic-workspace"}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
            },
            now,
        )
        .expect("admit fake launch");
    let agent = admission.command.target.expect("launch target");
    store.dispatch(&launch_id, now).expect("dispatch launch");
    store
        .put_batch(
            SourceBatch {
                batch_id: "ready".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "ready".to_owned(),
                    data: json!({"command_id":launch_id}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit ready");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle start");
    let send_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: send_id.clone(),
                target: Some(agent.clone()),
                verb: "send".to_owned(),
                args: json!({"text":"hello"}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
            },
            now,
        )
        .expect("admit send");
    store.dispatch(&send_id, now).expect("dispatch send");
    let before = store.high_water().expect("event boundary");
    let batch = SourceBatch {
        batch_id: "send".to_owned(),
        agent_id: agent.clone(),
        generation: 1,
        source: "stream".to_owned(),
        events: vec![
            NativeEvent {
                ts: None,
                kind: "turn.started".to_owned(),
                native_id: "turn-start".to_owned(),
                data: json!({"command_id":send_id, "turn_id":"turn-1"}),
            },
            NativeEvent {
                ts: None,
                kind: "message.completed".to_owned(),
                native_id: "message".to_owned(),
                data: json!({"command_id":send_id, "turn_id":"turn-1", "text":"hello"}),
            },
            NativeEvent {
                ts: None,
                kind: "turn.completed".to_owned(),
                native_id: "turn-end".to_owned(),
                data: json!({"command_id":send_id, "turn_id":"turn-1", "status":"completed"}),
            },
        ],
        cursor: Some("send:1".to_owned()),
    };
    let mut invalid = batch.clone();
    invalid.events[2].data["turn_id"] = json!("wrong-turn");
    store
        .put_batch(invalid, now)
        .expect_err("invalid terminal rolls back association and event");
    assert_eq!(
        store
            .get_command(&send_id)
            .expect("command")
            .expect("send")
            .state,
        CommandState::Dispatched
    );
    assert!(
        store
            .read_events(before, 100, None)
            .expect("rolled back events")
            .is_empty()
    );

    let committed = store
        .put_batch(batch.clone(), now)
        .expect("commit complete native batch");
    assert_eq!(committed.ack.last_seq - committed.ack.first_seq + 1, 3);
    let applied: Vec<_> = committed
        .events
        .iter()
        .filter(|event| event.kind == "command.applied" && event.data["command_id"] == send_id)
        .collect();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].source, "daemon");
    assert_eq!(applied[0].agent.as_deref(), Some(agent.as_str()));
    assert_eq!(applied[0].generation, Some(1));
    assert_eq!(applied[0].data["outcome"], json!({"turn_id":"turn-1"}));
    assert!(applied[0].seq > committed.ack.last_seq);
    let duplicate = store
        .put_batch(batch, now)
        .expect("duplicate batch readback");
    assert_eq!(duplicate.ack, committed.ack);
    assert!(duplicate.events.is_empty());
    let readback = store
        .settle(
            &send_id,
            CommandState::Applied,
            Some(json!({"turn_id":"turn-1"})),
            now,
        )
        .expect("same send settlement readback");
    assert!(readback.events.is_empty());
    assert_eq!(
        store
            .read_events(before, 100, None)
            .expect("persisted events")
            .iter()
            .filter(|event| event.kind == "command.applied" && event.data["command_id"] == send_id)
            .count(),
        1
    );
}

#[test]
fn secret_question_answer_is_rejected_before_command_persistence() {
    assert!(contains_secret_marker(
        &json!({"questions": [{"is_secret": true}]})
    ));
    assert!(contains_secret_marker(
        &json!({"native": {"question": {"isSecret": true}}})
    ));
    assert!(!contains_secret_marker(
        &json!({"questions": [{"is_secret": false}]})
    ));

    let launch_id = Ulid::new().to_string();
    let send_id = Ulid::new().to_string();
    let answer_id = Ulid::new().to_string();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after the Unix epoch")
        .as_millis()
        .try_into()
        .expect("system time in milliseconds fits in i64");
    let mut store = Store::open(Path::new(":memory:")).expect("open in-memory Store");
    let admission = store
        .submit(
            "owner",
            SubmitCommand {
                id: launch_id.clone(),
                target: None,
                verb: "launch".to_owned(),
                args: json!({
                    "project": "secret-answer-regression",
                    "workspace": "synthetic-workspace",
                    "harness": "codex",
                    "profile": "synthetic-codex",
                    "profile_snapshot": {}
                }),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
            },
            now,
        )
        .expect("admit Codex launch");
    let started = store
        .dispatch(&launch_id, now)
        .expect("dispatch Codex launch");
    let agent_id = admission
        .command
        .target
        .expect("launch admission returns its Agent");
    assert_eq!(started.value.generation, Some(1));
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "ready-1".to_owned(),
                    data: json!({
                        "native_scope": "synthetic-codex-home",
                        "native_ref": "synthetic-thread",
                        "effective": {}
                    }),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit synthetic session readiness");
    store
        .settle(
            &launch_id,
            CommandState::Applied,
            Some(json!({"state": "ready"})),
            now,
        )
        .expect("settle launch");
    store
        .submit(
            "owner",
            SubmitCommand {
                id: send_id.clone(),
                target: Some(agent_id.clone()),
                verb: "send".to_owned(),
                args: json!({"text": "trigger synthetic request"}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
            },
            now,
        )
        .expect("admit synthetic input");
    store
        .dispatch(&send_id, now)
        .expect("dispatch synthetic input");
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: "turn-started-1".to_owned(),
                    data: json!({"command_id": send_id, "turn_id": "turn-1"}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit synthetic turn start");
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "request.opened".to_owned(),
                    native_id: "request-opened-1".to_owned(),
                    data: json!({
                        "command_id": send_id,
                        "request_id": "public-request-1",
                        "turn_id": "turn-1",
                        "kind": "question",
                        "details": {
                            "questions": [{"id": "q1", "is_secret": true}]
                        }
                    }),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit synthetic secret question");

    let marker = "synthetic-answer-secret-marker";
    let error = store
        .submit(
            "owner",
            SubmitCommand {
                id: answer_id.clone(),
                target: Some(agent_id),
                verb: "answer".to_owned(),
                args: json!({"text": marker}),
                expected_generation: None,
                expected_turn_id: None,
                request_id: None,
            },
            now,
        )
        .expect_err("secret question answers must be rejected");
    assert_eq!(error.code, "secret_in_payload");
    assert!(
        store
            .get_command(&answer_id)
            .expect("read rejected answer command")
            .is_none(),
        "rejected answer args must not be persisted"
    );
}

#[test]
fn native_request_cleanup_does_not_acknowledge_reply() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after the Unix epoch")
        .as_millis()
        .try_into()
        .expect("system time in milliseconds fits in i64");
    let mut store = Store::open(Path::new(":memory:")).expect("open in-memory Store");
    let command = |verb: &str, target, args, request_id| SubmitCommand {
        id: Ulid::new().to_string(),
        target,
        verb: verb.to_owned(),
        args,
        expected_generation: None,
        expected_turn_id: None,
        request_id,
    };
    let launch = command(
        "launch",
        None,
        json!({
            "project": "native-request-cleanup",
            "workspace": "synthetic-workspace",
            "harness": "codex",
            "profile": "synthetic-codex",
            "profile_snapshot": {}
        }),
        None,
    );
    let launch_id = launch.id.clone();
    let agent_id = store
        .submit("owner", launch, now)
        .expect("admit launch")
        .command
        .target
        .expect("launch returns an Agent");
    store.dispatch(&launch_id, now).expect("dispatch launch");
    let batch = |kind: &str, data| SourceBatch {
        batch_id: Ulid::new().to_string(),
        agent_id: agent_id.clone(),
        generation: 1,
        source: "stream".to_owned(),
        events: vec![NativeEvent {
            ts: None,
            kind: kind.to_owned(),
            native_id: Ulid::new().to_string(),
            data,
        }],
        cursor: None,
    };
    store
        .put_batch(batch("session.ready", json!({})), now)
        .expect("commit session readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now)
        .expect("settle launch");
    let send = command(
        "send",
        Some(agent_id.clone()),
        json!({"text": "open native requests"}),
        None,
    );
    let send_id = send.id.clone();
    store.submit("owner", send, now).expect("admit input");
    store.dispatch(&send_id, now).expect("dispatch input");
    store
        .put_batch(
            batch(
                "turn.started",
                json!({"command_id": send_id, "turn_id": "turn-1"}),
            ),
            now,
        )
        .expect("commit exact turn association");
    for request_id in ["cleanup-only", "uncertain-reply"] {
        store
            .put_batch(
                batch(
                    "request.opened",
                    json!({
                        "command_id": send_id,
                        "request_id": request_id,
                        "turn_id": "turn-1",
                        "kind": "approval",
                        "details": {}
                    }),
                ),
                now,
            )
            .expect("commit pending request");
    }
    let reply = command(
        "approve",
        Some(agent_id.clone()),
        json!({}),
        Some("uncertain-reply".to_owned()),
    );
    let reply_id = reply.id.clone();
    store.submit("owner", reply, now).expect("admit reply");
    store.dispatch(&reply_id, now).expect("dispatch reply");
    let unknown_reply = store
        .settle(
            &reply_id,
            CommandState::Unknown,
            Some(json!({"reason": "native reply outcome unknown"})),
            now,
        )
        .expect("retain unknown reply outcome")
        .value;

    let close_data = json!({"request_id": "cleanup-only", "turn_id": "turn-1"});
    let mut wrong_source = batch("request.closed", close_data.clone());
    wrong_source.source = "unowned".to_owned();
    let mut wrong_generation = batch("request.closed", close_data.clone());
    wrong_generation.generation = 2;
    let before_rejections = store.high_water().expect("read event high water");
    for (invalid, code) in [
        (wrong_source, "source_integrity"),
        (wrong_generation, "stale_generation"),
        (
            batch(
                "request.closed",
                json!({"request_id": "missing", "turn_id": "turn-1"}),
            ),
            "source_integrity",
        ),
        (
            batch(
                "request.closed",
                json!({"request_id": "cleanup-only", "turn_id": "other-turn"}),
            ),
            "source_integrity",
        ),
        (batch("request.resolved", close_data.clone()), "invalid"),
        (
            batch(
                "request.resolved",
                json!({
                    "request_id": "cleanup-only", "turn_id": "turn-1",
                    "command_id": send_id
                }),
            ),
            "source_integrity",
        ),
        (
            batch(
                "request.closed",
                json!({
                    "request_id": "cleanup-only", "turn_id": "turn-1",
                    "command_id": reply_id
                }),
            ),
            "invalid",
        ),
    ] {
        let error = store
            .put_batch(invalid, now)
            .expect_err("reject wrong binding");
        assert_eq!(error.code, code);
    }
    assert_eq!(
        store.high_water().expect("read event high water"),
        before_rejections
    );

    store
        .record_source_gap(
            SourceGap {
                gap_id: "independent-gap".to_owned(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                families: vec!["request".to_owned()],
                reason: "synthetic source gap".to_owned(),
                from: None,
                to: None,
            },
            now,
        )
        .expect("record independent source gap");
    let source_before = store
        .read_source_state(&agent_id, 1, "stream")
        .expect("read source gap before cleanup");
    for (request_id, expected_state, remaining) in [
        ("cleanup-only", AgentState::Waiting, 1),
        ("uncertain-reply", AgentState::Busy, 0),
    ] {
        let cleanup = store
            .put_batch(
                batch(
                    "request.closed",
                    json!({"request_id": request_id, "turn_id": "turn-1"}),
                ),
                now,
            )
            .expect("commit native request cleanup without a reply acknowledgement");
        assert_eq!(cleanup.events.len(), 1);
        assert_eq!(cleanup.events[0].kind, "request.closed");
        assert!(cleanup.events[0].data.get("command_id").is_none());
        let agent = store
            .get_agent(&agent_id)
            .expect("read Agent")
            .expect("Agent exists");
        assert_eq!(agent.state, expected_state);
        assert_eq!(agent.pending.len(), remaining);
        assert!(!agent.pending.iter().any(|pending| pending.id == request_id));
        assert_eq!(agent.active_turn.as_deref(), Some("turn-1"));
        assert_eq!(
            store.get_command(&reply_id).expect("read reply"),
            Some(unknown_reply.clone())
        );

        let late_reply = command(
            "approve",
            Some(agent_id.clone()),
            json!({}),
            Some(request_id.to_owned()),
        );
        let late_reply_id = late_reply.id.clone();
        let error = store
            .submit("owner", late_reply, now)
            .expect_err("durable pending row must already be resolved");
        assert_eq!(error.code, "source_integrity");
        assert!(
            store
                .get_command(&late_reply_id)
                .expect("read late reply")
                .is_none()
        );
    }
    let source_after = store
        .read_source_state(&agent_id, 1, "stream")
        .expect("read source gap after cleanup");
    for field in ["gap_id", "gap_digest", "gap_seq", "health"] {
        assert_eq!(source_after[field], source_before[field]);
    }
}

fn native_store_request() -> SubmitCommand {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "launch");
    request.args = json!({
        "project":"native-store-binding", "workspace":"/synthetic-workspace",
        "harness":"codex", "profile":"codex-fixture",
        "profile_snapshot":{
            "harness":"codex", "model":"fixture", "pass_env":["CODEX_HOME"],
            "pass_env_values":{"CODEX_HOME":" raw-home ","CODEX_SQLITE_HOME":null}
        }
    });
    request
}

fn native_store_binding() -> NativeStoreBinding {
    NativeStoreBinding {
        home: "/verified-home".to_owned(),
        root: "/verified-store".to_owned(),
        native_scope: "verified-scope".to_owned(),
    }
}

#[test]
fn native_store_binding_commits_without_changing_admitted_input_or_retry_binding() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let request = native_store_request();
    let admitted = store.submit("owner", request.clone(), now).unwrap();
    let id = admitted.command.target.as_deref().unwrap();
    let expected = store.get_agent(id).unwrap().unwrap();
    let digest: Vec<u8> = store
        .connection
        .query_row(
            "SELECT payload_digest FROM commands WHERE id = ?1",
            [&request.id],
            |row| row.get(0),
        )
        .unwrap();
    let binding = native_store_binding();
    let dispatched = store
        .dispatch_with_native_store_binding(&request.id, now + 1, &expected, &binding)
        .unwrap();
    assert_eq!(dispatched.value.state, CommandState::Dispatched);
    assert_eq!(dispatched.value.args, admitted.command.args);
    let agent = store.get_agent(id).unwrap().unwrap();
    assert_eq!(agent.generation, 1);
    assert_eq!(agent.state, AgentState::Starting);
    assert_eq!(
        agent.profile_snapshot["native_store_binding"],
        json!(binding)
    );
    let mut input = agent.profile_snapshot.clone();
    input
        .as_object_mut()
        .unwrap()
        .remove("native_store_binding");
    assert_eq!(input, expected.profile_snapshot);
    assert_eq!(
        digest,
        store
            .connection
            .query_row(
                "SELECT payload_digest FROM commands WHERE id = ?1",
                [&request.id],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .unwrap()
    );
    let before = store.high_water().unwrap();
    let token = store.get_agent_token(id, 1).unwrap();
    let retained = store.submit("owner", request.clone(), now + 2).unwrap();
    assert!(!retained.is_new);
    assert!(retained.events.is_empty());
    assert_eq!(retained.command, dispatched.value);
    assert_eq!(
        store
            .dispatch_with_native_store_binding(&request.id, now + 2, &expected, &binding)
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(store.high_water().unwrap(), before);
    assert_eq!(store.get_agent_token(id, 1).unwrap(), token);

    for (kind, native_id, data) in [
        (
            "session.ready",
            "ready",
            json!({"command_id":request.id,"native_scope":binding.native_scope,"native_ref":"thread"}),
        ),
        ("session.ended", "ended", json!({})),
    ] {
        store
            .put_batch(
                SourceBatch {
                    batch_id: native_id.to_owned(),
                    agent_id: id.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    cursor: None,
                    events: vec![NativeEvent {
                        kind: kind.to_owned(),
                        native_id: native_id.to_owned(),
                        data,
                        ts: None,
                    }],
                },
                now + 3,
            )
            .unwrap();
    }
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(id.to_owned());
    resume.expected_generation = Some(1);
    store.submit("owner", resume.clone(), now + 4).unwrap();
    store.dispatch(&resume.id, now + 4).unwrap();
    assert_eq!(
        store.get_agent(id).unwrap().unwrap().profile_snapshot,
        agent.profile_snapshot
    );
}

#[test]
fn native_store_binding_rechecks_probe_identity_and_snapshot_before_dispatch() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let request = native_store_request();
    let admitted = store.submit("owner", request.clone(), now).unwrap();
    let id = admitted.command.target.as_deref().unwrap();
    let expected = store.get_agent(id).unwrap().unwrap();
    let before = store.high_water().unwrap();
    for (field, value) in [
        ("id", json!("a999")),
        ("workspace", json!("/changed")),
        ("profile_snapshot", json!({})),
        ("profile", json!("changed")),
        ("harness", json!("fake")),
        ("generation", json!(1)),
        ("state", json!("stopped")),
    ] {
        let mut changed = serde_json::to_value(&expected).unwrap();
        changed[field] = value;
        let changed = serde_json::from_value(changed).unwrap();
        assert_eq!(
            store
                .dispatch_with_native_store_binding(
                    &request.id,
                    now + 1,
                    &changed,
                    &native_store_binding()
                )
                .unwrap_err()
                .code,
            "stale_generation",
            "{field}"
        );
        assert_eq!(store.get_agent(id).unwrap().unwrap(), expected);
        assert_eq!(store.high_water().unwrap(), before);
    }
    assert_eq!(
        store.get_command(&request.id).unwrap().unwrap(),
        admitted.command
    );
    assert_eq!(store.get_agent_token(id, 0).unwrap(), None);
    let sources: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM source_state WHERE generation=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(sources, 0);
}

#[test]
fn native_store_binding_preserves_workspace_readiness_and_cancellation_fences() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let mut request = native_store_request();
    let input = WorkspacePreparationInput {
        project: "native-store-binding".to_owned(),
        repo_path: "/synthetic-owner".to_owned(),
        repo_common_dir: "/synthetic-owner/.git".to_owned(),
        requested_branch: "codex/binding".to_owned(),
        base_ref: Some("HEAD".to_owned()),
        base_oid: "a".repeat(40),
        worktree_path: "/synthetic-worktree".to_owned(),
        frozen_setup: Vec::new(),
    };
    request.args["workspace"] = json!(input.repo_path);
    request.args["worktree"] = json!(input.requested_branch);
    let admitted = store
        .submit_with_workspace("owner", request.clone(), input.clone(), now)
        .unwrap();
    let id = admitted.command.target.as_deref().unwrap();
    let expected = store.get_agent(id).unwrap().unwrap();
    let before = store.high_water().unwrap();
    assert_eq!(
        store
            .dispatch_with_native_store_binding(
                &request.id,
                now,
                &expected,
                &native_store_binding()
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(store.high_water().unwrap(), before);
    store
        .settle_workspace_creation(
            &request.id,
            1,
            WorkspaceCreationOutcome::Verified {
                repo_path: input.repo_path,
                repo_common_dir: input.repo_common_dir,
                worktree_path: input.worktree_path,
                branch: input.requested_branch,
                head: input.base_oid,
            },
            now + 1,
        )
        .unwrap();
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(id.to_owned());
    stop.expected_generation = Some(0);
    store.submit("owner", stop.clone(), now + 2).unwrap();
    store.dispatch(&stop.id, now + 2).unwrap();
    let before = store.high_water().unwrap();
    assert_eq!(
        store
            .dispatch_with_native_store_binding(
                &request.id,
                now + 3,
                &expected,
                &native_store_binding()
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
    assert_eq!(store.high_water().unwrap(), before);
    let current = store.get_agent(id).unwrap().unwrap();
    assert_eq!(current.generation, 0);
    assert_eq!(current.profile_snapshot, expected.profile_snapshot);
    assert_eq!(store.get_agent_token(id, 0).unwrap(), None);
    assert!(store.list_source_states(id, 1).unwrap().is_empty());
}

#[test]
fn native_store_binding_is_derived_only_after_new_admission() {
    for verb in ["launch", "attach"] {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let mut request = native_store_request();
        if verb == "attach" {
            request.verb = verb.to_owned();
            request.args.as_object_mut().unwrap().remove("workspace");
            request.args["mode"] = json!("observe");
            request.args["native_scope"] = json!("observe-scope");
            request.args["native_ref"] = json!("thread");
        }
        let admitted = store.submit("owner", request.clone(), now).unwrap();
        let before = store.high_water().unwrap();
        for binding in [json!(native_store_binding()), serde_json::Value::Null] {
            let mut supplied = request.clone();
            supplied.id = Ulid::new().to_string();
            supplied.args["profile_snapshot"]["native_store_binding"] = binding;
            assert_eq!(
                store
                    .submit("owner", supplied.clone(), now + 1)
                    .unwrap_err()
                    .code,
                "invalid",
                "{verb}"
            );
            assert!(store.get_command(&supplied.id).unwrap().is_none());
            assert_eq!(store.agent_count().unwrap(), 1);
            assert_eq!(store.high_water().unwrap(), before);
        }
        let retained = store.submit("owner", request.clone(), now + 2).unwrap();
        assert!(!retained.is_new);
        assert_eq!(retained.command, admitted.command);
        if verb == "attach" {
            // Attach's immutable semantic payload excludes the derived snapshot;
            // retained-ID resolution must still win before new-admission checks.
            request.args["profile_snapshot"]["native_store_binding"] =
                json!(native_store_binding());
            let retained = store.submit("owner", request, now + 2).unwrap();
            assert!(!retained.is_new);
            assert_eq!(retained.command, admitted.command);
            assert!(retained.events.is_empty());
        }
        assert_eq!(store.high_water().unwrap(), before);
    }
}

#[test]
fn native_store_binding_retains_identical_internal_binding_and_rejects_replacement() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let request = native_store_request();
    let admitted = store.submit("owner", request.clone(), now).unwrap();
    let id = admitted.command.target.as_deref().unwrap();
    let binding = native_store_binding();
    // Seed retained internal metadata, never a caller-supplied admission field.
    let mut snapshot = request.args["profile_snapshot"].clone();
    snapshot["native_store_binding"] = json!(binding);
    store
        .connection
        .execute(
            "UPDATE agents SET profile_snapshot=?1 WHERE id=?2",
            rusqlite::params![
                serde_json::to_string(&snapshot).unwrap(),
                eliot_core::parse_agent_id(id).unwrap()
            ],
        )
        .unwrap();
    let expected = store.get_agent(id).unwrap().unwrap();
    let mut replacement = binding.clone();
    replacement.root = "/different-store".to_owned();
    let before = store.high_water().unwrap();
    assert_eq!(
        store
            .dispatch_with_native_store_binding(&request.id, now + 1, &expected, &replacement)
            .unwrap_err()
            .code,
        "source_integrity"
    );
    assert_eq!(store.high_water().unwrap(), before);
    assert_eq!(store.get_agent(id).unwrap().unwrap(), expected);
    store
        .dispatch_with_native_store_binding(&request.id, now + 2, &expected, &binding)
        .unwrap();
    assert_eq!(
        store.get_agent(id).unwrap().unwrap().profile_snapshot,
        snapshot
    );
}

#[test]
fn native_store_binding_frame_and_persistence_failures_roll_back_entire_dispatch() {
    for oversized in [false, true] {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let mut request = native_store_request();
        let mut binding = native_store_binding();
        if oversized {
            binding.root = "r".repeat(1024);
            let mut enriched = request.args["profile_snapshot"].clone();
            enriched["native_store_binding"] = json!(binding);
            enriched["padding"] = json!("");
            let padding =
                super::MAX_FRAME_BYTES - 128 - serde_json::to_vec(&enriched).unwrap().len();
            request.args["profile_snapshot"]["padding"] = json!("p".repeat(padding));
            enriched["padding"] = request.args["profile_snapshot"]["padding"].clone();
            assert!(serde_json::to_vec(&enriched).unwrap().len() <= super::MAX_FRAME_BYTES);
        }
        let admitted = store.submit("owner", request.clone(), now).unwrap();
        let id = admitted.command.target.as_deref().unwrap();
        let expected = store.get_agent(id).unwrap().unwrap();
        if !oversized {
            store.connection.execute_batch("CREATE TRIGGER fail_native_store_dispatch BEFORE INSERT ON events WHEN NEW.kind='command.dispatched' BEGIN SELECT RAISE(ABORT,'synthetic persistence failure'); END;").unwrap();
        }
        let before = store.high_water().unwrap();
        let error = store
            .dispatch_with_native_store_binding(&request.id, now + 1, &expected, &binding)
            .unwrap_err();
        assert_eq!(
            error.code,
            if oversized {
                "frame_too_large"
            } else {
                "internal"
            }
        );
        assert_eq!(store.high_water().unwrap(), before);
        assert_eq!(store.get_agent(id).unwrap().unwrap(), expected);
        assert_eq!(
            store.get_command(&request.id).unwrap().unwrap(),
            admitted.command
        );
        assert_eq!(store.get_agent_token(id, 0).unwrap(), None);
        let sources: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM source_state WHERE generation=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sources, 0);
    }
}
