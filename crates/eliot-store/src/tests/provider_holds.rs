use super::*;

fn provider_send_request(agent: &str, id: &str, probe: bool) -> SubmitCommand {
    SubmitCommand {
        id: id.to_owned(),
        target: Some(agent.to_owned()),
        selector: None,
        verb: "send".to_owned(),
        args: json!({"text":"synthetic provider hold input"}),
        expected_generation: Some(1),
        expected_turn_id: None,
        request_id: None,
        probe,
    }
}

fn provider_native_event(
    store: &mut Store,
    agent: &str,
    native_id: &str,
    kind: &str,
    data: serde_json::Value,
    now: i64,
) -> BatchCommit {
    store
        .put_batch(
            SourceBatch {
                batch_id: format!("provider-batch-{native_id}"),
                agent_id: agent.to_owned(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    kind: kind.to_owned(),
                    native_id: native_id.to_owned(),
                    data,
                    ts: None,
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit provider test event")
}

fn start_provider_turn(
    store: &mut Store,
    agent: &str,
    command_id: &str,
    probe: bool,
    now: i64,
) -> String {
    let admission = store
        .submit(
            "owner",
            provider_send_request(agent, command_id, probe),
            now,
        )
        .expect("admit provider test send");
    assert_eq!(admission.command.probe, probe);
    store
        .dispatch(command_id, now + 1)
        .expect("dispatch provider test send");
    let turn_id = format!("turn-{command_id}");
    provider_native_event(
        store,
        agent,
        &format!("{command_id}-started"),
        "turn.started",
        json!({"command_id":command_id,"turn_id":turn_id}),
        now + 2,
    );
    turn_id
}

fn complete_provider_turn(
    store: &mut Store,
    agent: &str,
    command_id: &str,
    turn_id: &str,
    status: &str,
    restriction: Option<(ProviderRestrictionKind, Option<i64>)>,
    now: i64,
) -> BatchCommit {
    let mut data = json!({"command_id":command_id,"turn_id":turn_id,"status":status});
    if let Some((kind, retry_after)) = restriction {
        data["provider_restriction"] = json!({
            "kind": kind.as_str(),
            "retry_after": retry_after,
        });
    }
    provider_native_event(
        store,
        agent,
        &format!("{command_id}-completed"),
        "turn.completed",
        data,
        now,
    )
}

fn clear_provider_hold_request(
    scope: &str,
    expected_probe_command: Option<String>,
) -> ProviderClearHoldRequest {
    ProviderClearHoldRequest {
        id: Ulid::new().to_string(),
        scope: scope.to_owned(),
        corrective_evidence: ProviderCorrectiveEvidence::OwnerVerified,
        expected_probe_command,
    }
}

#[test]
fn store_probe_false_preserves_legacy_digest_and_probe_toggle_conflicts() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let agent = ready_agent(&mut store, "provider-probe-digest", now);
    let request = provider_send_request(&agent, &Ulid::new().to_string(), false);
    let request_id = request.id.clone();
    let admitted = store.submit("owner", request.clone(), now + 1).unwrap();
    assert!(!admitted.command.probe);

    let expected_payload = json!({
        "target": request.target,
        "verb": request.verb,
        "args": request.args,
        "expected_generation": request.expected_generation,
        "expected_turn_id": request.expected_turn_id,
    });
    let expected_digest = crate::canonical::digest(&expected_payload).unwrap();
    let stored_digest: Vec<u8> = store
        .connection
        .query_row(
            "SELECT payload_digest FROM commands WHERE id = ?1",
            [&request_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(stored_digest, expected_digest);

    let mut changed_probe = request;
    changed_probe.probe = true;
    assert_eq!(
        store
            .submit("owner", changed_probe, now + 2)
            .unwrap_err()
            .code,
        "id_conflict"
    );
}

#[test]
fn provider_probe_claim_survives_reopen_and_exact_success_clears_hold() {
    let path = capacity_store_path();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let (agent, probe_id, pending_agent, pending_send_id);
    {
        let mut store = Store::open(&path).unwrap();
        agent = ready_agent(&mut store, "provider-probe-reopen", now);
        pending_agent = ready_agent(&mut store, "provider-pending-dispatch", now + 1);

        pending_send_id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                provider_send_request(&pending_agent, &pending_send_id, false),
                now + 2,
            )
            .expect("admit send before hold arrives");

        let first_id = Ulid::new().to_string();
        let first_turn = start_provider_turn(&mut store, &agent, &first_id, false, now + 3);
        let restriction_batch = complete_provider_turn(
            &mut store,
            &agent,
            &first_id,
            &first_turn,
            "failed",
            Some((ProviderRestrictionKind::RateLimit, None)),
            now + 6,
        );
        assert!(
            restriction_batch
                .events
                .iter()
                .any(|event| event.kind == "provider.hold_updated")
        );
        let blocked = store.dispatch(&pending_send_id, now + 7).unwrap_err();
        assert_eq!(blocked.code, "provider_held");
        assert_eq!(
            store.get_command(&pending_send_id).unwrap().unwrap().state,
            CommandState::Accepted
        );

        let rejected_id = Ulid::new().to_string();
        assert_eq!(
            store
                .submit(
                    "owner",
                    provider_send_request(&agent, &rejected_id, false),
                    now + 8,
                )
                .unwrap_err()
                .code,
            "provider_held"
        );
        assert!(store.get_command(&rejected_id).unwrap().is_none());

        let count_before_launch = store.agent_count().unwrap();
        let launch_id = Ulid::new().to_string();
        let mut launch = SubmitCommand::new(launch_id, "launch");
        launch.args = json!({"project":"provider-held-launch", "workspace":"synthetic-held-launch", "harness":"codex", "profile":"fake", "profile_snapshot":{}});
        launch.expected_generation = Some(0);
        assert_eq!(
            store.submit("owner", launch, now + 9).unwrap_err().code,
            "provider_held"
        );
        assert_eq!(store.agent_count().unwrap(), count_before_launch);

        probe_id = Ulid::new().to_string();
        let probe_request = provider_send_request(&agent, &probe_id, true);
        let admitted = store
            .submit("owner", probe_request.clone(), now + 10)
            .expect("claim eligible provider probe");
        assert!(admitted.command.probe);
        assert!(
            !store
                .submit("owner", probe_request.clone(), now + 11)
                .expect("retain duplicate probe admission")
                .is_new
        );
        let mut changed_probe = probe_request;
        changed_probe.probe = false;
        assert_eq!(
            store
                .submit("owner", changed_probe, now + 11)
                .unwrap_err()
                .code,
            "id_conflict"
        );
        assert!(store.get_command(&probe_id).unwrap().unwrap().probe);
        store.dispatch(&probe_id, now + 12).unwrap();
        store
            .settle(&probe_id, CommandState::Unknown, None, now + 13)
            .expect("unknown probe keeps its claim");

        let clear = clear_provider_hold_request("profile:fake", Some(probe_id.clone()));
        assert_eq!(
            store
                .clear_provider_hold("owner", &clear, now + 14)
                .unwrap_err()
                .code,
            "bad_state"
        );
    }

    {
        let mut store = Store::open(&path).unwrap();
        let hold = store
            .list_provider_holds(None, 10)
            .unwrap()
            .holds
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(hold.state, ProviderHoldState::Probing);
        assert_eq!(hold.probe_command.as_deref(), Some(probe_id.as_str()));

        let turn_id = format!("turn-{probe_id}");
        provider_native_event(
            &mut store,
            &agent,
            &format!("{probe_id}-started"),
            "turn.started",
            json!({"command_id":probe_id,"turn_id":turn_id}),
            now + 15,
        );
        let completed = complete_provider_turn(
            &mut store,
            &agent,
            &probe_id,
            &turn_id,
            "completed",
            None,
            now + 16,
        );
        assert!(
            completed
                .events
                .iter()
                .any(|event| event.kind == "provider.hold_cleared")
        );
        assert!(
            store
                .list_provider_holds(None, 10)
                .unwrap()
                .holds
                .is_empty()
        );
    }
    remove_capacity_store_files(&path);
}

#[test]
fn failed_probe_command_without_exact_terminal_keeps_probe_claim() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let agent = ready_agent(&mut store, "provider-probe-failed-command", now);
    let first_id = Ulid::new().to_string();
    let first_turn = start_provider_turn(&mut store, &agent, &first_id, false, now + 1);
    complete_provider_turn(
        &mut store,
        &agent,
        &first_id,
        &first_turn,
        "failed",
        Some((ProviderRestrictionKind::RateLimit, None)),
        now + 4,
    );

    let probe_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            provider_send_request(&agent, &probe_id, true),
            now + 5,
        )
        .expect("admit probe");
    store.dispatch(&probe_id, now + 6).unwrap();
    store
        .settle(
            &probe_id,
            CommandState::Failed,
            Some(json!({"reason":"known failure may have partial effects"})),
            now + 7,
        )
        .expect("retain known failed command result");

    let command = store.get_command(&probe_id).unwrap().unwrap();
    assert_eq!(command.state, CommandState::Failed);
    assert!(command.probe);
    let hold = store.list_provider_holds(None, 10).unwrap().holds.remove(0);
    assert_eq!(hold.kind, ProviderRestrictionKind::RateLimit);
    assert_eq!(hold.state, ProviderHoldState::Probing);
    assert_eq!(hold.probe_command.as_deref(), Some(probe_id.as_str()));
}

#[test]
fn applied_probe_with_running_turn_is_not_treated_as_terminal() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let agent = ready_agent(&mut store, "provider-probe-running-turn", now);
    let first_id = Ulid::new().to_string();
    let first_turn = start_provider_turn(&mut store, &agent, &first_id, false, now + 1);
    complete_provider_turn(
        &mut store,
        &agent,
        &first_id,
        &first_turn,
        "failed",
        Some((ProviderRestrictionKind::RateLimit, None)),
        now + 4,
    );

    let probe_id = Ulid::new().to_string();
    let turn_id = start_provider_turn(&mut store, &agent, &probe_id, true, now + 5);
    assert_eq!(
        store.get_command(&probe_id).unwrap().unwrap().state,
        CommandState::Applied
    );
    assert_eq!(
        store.get_turn(&agent, 1, &turn_id).unwrap().unwrap().state,
        TurnState::Running
    );
    assert_eq!(
        store
            .settle(
                &probe_id,
                CommandState::Failed,
                Some(json!({"reason":"turn is still running"})),
                now + 8,
            )
            .unwrap_err()
            .code,
        "bad_state"
    );
    let hold = store.list_provider_holds(None, 10).unwrap().holds.remove(0);
    assert_eq!(hold.state, ProviderHoldState::Probing);
    assert_eq!(hold.probe_command.as_deref(), Some(probe_id.as_str()));
}

#[test]
fn provider_hold_gates_steer_at_admission_and_dispatch_only() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let busy_agent = ready_steer_agent(&mut store, "provider-steer-busy", now);
    let hold_agent = ready_steer_agent(&mut store, "provider-steer-hold", now + 1);
    let active_send = Ulid::new().to_string();
    let turn_id = start_provider_turn(&mut store, &busy_agent, &active_send, false, now + 2);

    let accepted_steer_id = Ulid::new().to_string();
    let mut accepted_steer = SubmitCommand::new(accepted_steer_id.clone(), "steer");
    accepted_steer.target = Some(busy_agent.clone());
    accepted_steer.args = json!({"text":"synthetic steer"});
    accepted_steer.expected_generation = Some(1);
    accepted_steer.expected_turn_id = Some(turn_id.clone());
    store
        .submit("owner", accepted_steer, now + 5)
        .expect("admit steer before scope becomes held");

    let restriction_send = Ulid::new().to_string();
    let restriction_turn =
        start_provider_turn(&mut store, &hold_agent, &restriction_send, false, now + 6);
    complete_provider_turn(
        &mut store,
        &hold_agent,
        &restriction_send,
        &restriction_turn,
        "failed",
        Some((ProviderRestrictionKind::RateLimit, None)),
        now + 9,
    );

    let rejected_steer_id = Ulid::new().to_string();
    let mut rejected_steer = SubmitCommand::new(rejected_steer_id.clone(), "steer");
    rejected_steer.target = Some(busy_agent.clone());
    rejected_steer.args = json!({"text":"synthetic steer"});
    rejected_steer.expected_generation = Some(1);
    rejected_steer.expected_turn_id = Some(turn_id.clone());
    let before_rejected_steer = store.high_water().unwrap();
    assert_eq!(
        store
            .submit("owner", rejected_steer, now + 10)
            .unwrap_err()
            .code,
        "provider_held"
    );
    assert!(store.get_command(&rejected_steer_id).unwrap().is_none());
    assert_eq!(store.high_water().unwrap(), before_rejected_steer);

    let before_blocked_dispatch = store.high_water().unwrap();
    assert_eq!(
        store
            .dispatch(&accepted_steer_id, now + 11)
            .unwrap_err()
            .code,
        "provider_held"
    );
    assert_eq!(store.high_water().unwrap(), before_blocked_dispatch);
    let retained_steer = store.get_command(&accepted_steer_id).unwrap().unwrap();
    assert_eq!(retained_steer.state, CommandState::Accepted);
    assert_eq!(retained_steer.expected_generation, Some(1));
    assert_eq!(retained_steer.turn_id.as_deref(), Some(turn_id.as_str()));

    let mut interrupt = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    interrupt.target = Some(busy_agent);
    interrupt.expected_generation = Some(1);
    interrupt.expected_turn_id = Some(turn_id.clone());
    let interrupt_id = interrupt.id.clone();
    store
        .submit("owner", interrupt, now + 12)
        .expect("interrupt remains available under a provider hold");
    assert_eq!(
        store.dispatch(&interrupt_id, now + 12).unwrap().value.state,
        CommandState::Dispatched
    );

    let clear = clear_provider_hold_request("profile:steer-profile", None);
    store
        .clear_provider_hold("owner", &clear, now + 13)
        .unwrap();
    assert_eq!(
        store
            .dispatch(&accepted_steer_id, now + 14)
            .unwrap()
            .value
            .state,
        CommandState::Dispatched
    );
}

#[test]
fn same_scope_restriction_during_probe_survives_exact_probe_completion() {
    for (index, (kind, expected_kind)) in [
        (
            ProviderRestrictionKind::Quota,
            ProviderRestrictionKind::Quota,
        ),
        (ProviderRestrictionKind::Auth, ProviderRestrictionKind::Auth),
        (
            ProviderRestrictionKind::ModelRemoved,
            ProviderRestrictionKind::ModelRemoved,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let agent_a = ready_agent(&mut store, &format!("provider-interleave-a-{index}"), now);
        let agent_b = ready_agent(
            &mut store,
            &format!("provider-interleave-b-{index}"),
            now + 1,
        );
        let agent_probe = ready_agent(
            &mut store,
            &format!("provider-interleave-p-{index}"),
            now + 2,
        );
        let command_a = Ulid::new().to_string();
        let turn_a = start_provider_turn(&mut store, &agent_a, &command_a, false, now + 3);
        let command_b = Ulid::new().to_string();
        let turn_b = start_provider_turn(&mut store, &agent_b, &command_b, false, now + 3);

        complete_provider_turn(
            &mut store,
            &agent_a,
            &command_a,
            &turn_a,
            "failed",
            Some((ProviderRestrictionKind::RateLimit, None)),
            now + 6,
        );
        let probe_id = Ulid::new().to_string();
        let turn_probe = start_provider_turn(&mut store, &agent_probe, &probe_id, true, now + 7);
        let probing_clear = clear_provider_hold_request("profile:fake", Some(probe_id.clone()));
        assert_eq!(
            store
                .clear_provider_hold("owner", &probing_clear, now + 8)
                .unwrap_err()
                .code,
            "bad_state"
        );

        complete_provider_turn(
            &mut store,
            &agent_b,
            &command_b,
            &turn_b,
            "failed",
            Some((kind, None)),
            now + 9,
        );
        let during_probe = store.list_provider_holds(None, 10).unwrap().holds.remove(0);
        assert_eq!(during_probe.kind, expected_kind);
        assert_eq!(during_probe.state, ProviderHoldState::Probing);
        assert_eq!(
            during_probe.probe_command.as_deref(),
            Some(probe_id.as_str())
        );

        complete_provider_turn(
            &mut store,
            &agent_probe,
            &probe_id,
            &turn_probe,
            "completed",
            None,
            now + 10,
        );
        let after_probe = store.list_provider_holds(None, 10).unwrap().holds.remove(0);
        assert_eq!(after_probe.kind, expected_kind);
        assert_eq!(after_probe.state, ProviderHoldState::Held);
        assert_eq!(after_probe.probe_command, None);
        if matches!(
            expected_kind,
            ProviderRestrictionKind::Auth | ProviderRestrictionKind::ModelRemoved
        ) {
            let forbidden_probe = Ulid::new().to_string();
            assert_eq!(
                store
                    .submit(
                        "owner",
                        provider_send_request(&agent_probe, &forbidden_probe, true),
                        now + 11,
                    )
                    .unwrap_err()
                    .code,
                "provider_held"
            );
            assert!(store.get_command(&forbidden_probe).unwrap().is_none());
        }

        let clear = clear_provider_hold_request("profile:fake", None);
        let cleared = store
            .clear_provider_hold("owner", &clear, now + 12)
            .unwrap();
        assert!(cleared.value.cleared);
        assert_eq!(cleared.events.len(), 1);
        assert_eq!(cleared.events[0].kind, "provider.hold_cleared");
        let replay = store
            .clear_provider_hold("owner", &clear, now + 13)
            .unwrap();
        assert_eq!(replay.value, cleared.value);
        assert!(replay.events.is_empty());
        let stale_clear =
            clear_provider_hold_request("profile:fake", Some("stale-probe-command".to_owned()));
        let absent = store
            .clear_provider_hold("owner", &stale_clear, now + 13)
            .unwrap();
        assert!(!absent.value.cleared);
        assert!(absent.events.is_empty());

        let renewed_id = Ulid::new().to_string();
        let renewed_turn =
            start_provider_turn(&mut store, &agent_probe, &renewed_id, false, now + 13);
        complete_provider_turn(
            &mut store,
            &agent_probe,
            &renewed_id,
            &renewed_turn,
            "failed",
            Some((ProviderRestrictionKind::RateLimit, None)),
            now + 16,
        );
        let stale_replay = store
            .clear_provider_hold("owner", &clear, now + 17)
            .unwrap();
        assert_eq!(stale_replay.value, cleared.value);
        assert!(stale_replay.events.is_empty());
        let renewed_hold = store.list_provider_holds(None, 10).unwrap();
        assert_eq!(renewed_hold.holds.len(), 1);
        assert_eq!(
            renewed_hold.holds[0].kind,
            ProviderRestrictionKind::RateLimit
        );
    }
}

#[test]
fn fresh_launch_rebinds_parent_startup_metadata_without_reselecting_profile() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let mut request = native_store_request();
    request.args["profile_snapshot"]["native_launch_metadata"] =
        json!({"command_id":"parent-launch", "data":{"servers":[], "plugins":[]}});
    let admission = store.submit("owner", request.clone(), now).unwrap();
    let id = admission.command.target.as_deref().unwrap();
    let expected = store.get_agent(id).unwrap().unwrap();
    let binding = native_store_binding();
    let startup = json!({"servers":[], "plugins":[]});
    store
        .dispatch_with_native_store_binding(
            &request.id,
            now + 1,
            &expected,
            &binding,
            Some(&startup),
        )
        .unwrap();
    let agent = store.get_agent(id).unwrap().unwrap();
    assert_eq!(
        agent.profile_snapshot["native_launch_metadata"],
        json!({"command_id":request.id, "data":startup})
    );
    assert_eq!(
        agent.profile_snapshot["native_store_binding"],
        json!(binding)
    );
    assert_eq!(
        store.get_command(&request.id).unwrap().unwrap().args,
        admission.command.args
    );
}
