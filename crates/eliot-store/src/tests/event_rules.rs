use super::*;

#[test]
fn owner_event_launch_links_one_send_counts_once_and_replays_unknown() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 1_000;
    let rule_id = "owner-launch-linked-input";
    let workspace = "synthetic-owner-rule-launch";
    let state = store
        .ensure_rule_state("owner", &rule_input_with_limit(rule_id, 2, workspace), now)
        .expect("ensure per-rule launch limit");
    let trigger_agent = ready_agent(&mut store, "launch-trigger", now + 1);
    let trigger_agent_num = eliot_core::parse_agent_id(&trigger_agent).expect("trigger Agent ID");
    let trigger_event_seq: i64 = store
        .connection
        .query_row(
            "SELECT seq FROM events
             WHERE agent = ?1 AND kind = 'command.accepted'
               AND json_extract(data, '$.verb') = 'launch'
             ORDER BY seq DESC LIMIT 1",
            [trigger_agent_num],
            |row| row.get(0),
        )
        .expect("read trigger launch event");

    let request =
        targetless_rule_launch_request(workspace, workspace, Some("initial launch input"));
    assert!(request.target.is_none());
    assert_eq!(request.expected_generation, Some(0));
    let first = store
        .admit_rule_action("owner", &state, trigger_event_seq, request.clone(), now + 2)
        .expect("admit targetless owner launch");
    assert!(first.is_new);
    let parent = first.parent.as_ref().expect("rule firing parent");
    assert_eq!(parent.principal, "owner");
    assert_eq!(parent.state, CommandState::Applied);
    let action = first.child.as_ref().expect("launch action admission");
    assert!(
        action.events.is_empty(),
        "events belong to the outer admission"
    );
    let launch = &action.command;
    let linked = action
        .linked_command
        .as_ref()
        .expect("nonblank launch text creates one linked SEND");
    let launch_id = launch.id.clone();
    let linked_id = linked.id.clone();
    let launch_target = launch.target.clone().expect("Store allocated new Agent");
    assert_eq!(launch.principal, "owner");
    assert_ne!(launch_target, trigger_agent);
    assert_eq!(launch.verb, "launch");
    assert_eq!(launch.expected_generation, Some(0));
    assert_eq!(launch.generation, Some(0));
    assert_eq!(launch.args["project"], workspace);
    assert_eq!(linked.id, format!("{launch_id}/send"));
    assert_eq!(linked.principal, "owner");
    assert_eq!(linked.target.as_deref(), Some(launch_target.as_str()));
    assert_eq!(linked.verb, "send");
    assert_eq!(linked.expected_generation, Some(1));
    assert_eq!(linked.generation, Some(1));
    assert_eq!(linked.args, json!({"text":"initial launch input"}));
    assert_eq!(linked.state, CommandState::Queued);
    assert_eq!(
        parent.outcome.as_ref().unwrap()["children"],
        json!([launch_id])
    );

    let (launch_parent, launch_link, launch_ordinal): (
        Option<String>,
        Option<String>,
        Option<i64>,
    ) = store
        .connection
        .query_row(
            "SELECT parent_command, link_kind, ordinal FROM commands WHERE id = ?1",
            [&launch_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("read rule launch link");
    assert_eq!(launch_parent.as_deref(), Some(parent.id.as_str()));
    assert_eq!(launch_link.as_deref(), Some("rule_action"));
    assert_eq!(launch_ordinal, Some(0));
    let (send_parent, send_link, send_ordinal): (Option<String>, Option<String>, Option<i64>) =
        store
            .connection
            .query_row(
                "SELECT parent_command, link_kind, ordinal FROM commands WHERE id = ?1",
                [&linked_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("read linked initial SEND link");
    assert_eq!(send_parent.as_deref(), Some(launch_id.as_str()));
    assert_eq!(send_link.as_deref(), Some("send"));
    assert_eq!(send_ordinal, Some(0));

    let command_cause = |command_id: &str| {
        first
            .events
            .iter()
            .find(|event| {
                event
                    .data
                    .get("command_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(command_id)
            })
            .and_then(|event| event.cause.as_ref())
            .expect("command event has its own causal identity")
            .clone()
    };
    for command_id in [&parent.id, &launch_id, &linked_id] {
        let cause = command_cause(command_id);
        assert_eq!(cause.command_id, *command_id);
        assert_eq!(cause.rule.as_deref(), Some(rule_id));
        assert_eq!(cause.depth, 1);
    }

    let configured_action_children: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE parent_command = ?1 AND link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [&parent.id],
            |row| row.get(0),
        )
        .expect("count configured launch action children");
    let global_action_children: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .expect("count global configured action children");
    assert_eq!(configured_action_children, 1);
    assert_eq!(global_action_children, 1);

    let queued_recovery = store
        .recover_rule_children(None, 100)
        .expect("recover accepted launch action");
    assert_eq!(
        queued_recovery
            .iter()
            .find(|command| command.command.id == launch_id)
            .expect("launch is recoverable while accepted")
            .command
            .state,
        CommandState::Accepted
    );
    assert!(
        !queued_recovery
            .iter()
            .any(|command| command.command.id == linked_id),
        "the linked SEND is carried by its launch admission, not its own recovery cursor"
    );

    let trigger_send = store
        .submit(
            "owner",
            send_request(&trigger_agent, 1, "next rule event"),
            now + 3,
        )
        .expect("create second trigger event");
    let second_event = trigger_send
        .events
        .iter()
        .find(|event| {
            event
                .data
                .get("command_id")
                .and_then(serde_json::Value::as_str)
                == Some(trigger_send.command.id.as_str())
        })
        .expect("trigger SEND command event");
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(trigger_agent.clone());
    stop.expected_generation = Some(1);
    let second_action = store
        .admit_rule_action("owner", &first.state, second_event.seq, stop, now + 4)
        .expect("per-rule limit counts the launch once, not its linked SEND");
    assert!(second_action.is_new);
    assert_eq!(second_action.child.as_ref().unwrap().command.verb, "stop");
    store
        .settle(
            &second_action.child.as_ref().unwrap().command.id,
            CommandState::Rejected,
            Some(json!({"reason": "close stop counter fixture"})),
            now + 5,
        )
        .expect("close second configured action");

    let commands_before_replay: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .expect("count commands before retained replay");
    let events_before_replay = store.high_water().unwrap();
    let replay = store
        .admit_rule_action(
            "owner",
            &second_action.state,
            trigger_event_seq,
            request.clone(),
            now + 6,
        )
        .expect("replay retained launch before current gates");
    assert!(!replay.is_new);
    assert!(replay.events.is_empty());
    let replay_action = replay.child.as_ref().expect("retained launch admission");
    assert!(replay_action.events.is_empty());
    assert_eq!(replay_action.command.id, launch_id);
    assert_eq!(replay_action.linked_command.as_ref().unwrap().id, linked_id);
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before_replay
    );
    assert_eq!(store.high_water().unwrap(), events_before_replay);

    store
        .dispatch(&launch_id, now + 7)
        .expect("dispatch admitted launch");
    install_fixture_sources(&mut store, &launch_target, &launch_id, 1, now + 7);
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: launch_target.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: Ulid::new().to_string(),
                    data: json!({"command_id":launch_id.clone()}),
                }],
                cursor: None,
            },
            now + 8,
        )
        .expect("commit launch readiness");
    store
        .settle(&launch_id, CommandState::Applied, None, now + 9)
        .expect("settle launch readiness");
    let applied_recovery = store
        .recover_rule_children(None, 100)
        .expect("recover applied launch with queued linked SEND");
    assert_eq!(
        applied_recovery
            .iter()
            .find(|command| command.command.id == launch_id)
            .expect("applied launch remains recoverable while linked SEND is queued")
            .command
            .state,
        CommandState::Applied
    );
    store
        .dispatch(&linked_id, now + 10)
        .expect("dispatch linked SEND");
    store
        .settle(
            &linked_id,
            CommandState::Unknown,
            Some(json!({"reason": "fixture uncertain initial input"})),
            now + 11,
        )
        .expect("retain uncertain linked SEND");

    let commands_before_unknown_replay: i64 = store
        .connection
        .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
        .unwrap();
    let events_before_unknown_replay = store.high_water().unwrap();
    let unknown_replay = store
        .admit_rule_action(
            "owner",
            &second_action.state,
            trigger_event_seq,
            request,
            now + 12,
        )
        .expect("replay launch with retained Unknown linked SEND");
    assert!(!unknown_replay.is_new);
    assert!(unknown_replay.events.is_empty());
    let retained = unknown_replay.child.as_ref().unwrap();
    assert_eq!(retained.command.state, CommandState::Applied);
    assert_eq!(
        retained.linked_command.as_ref().unwrap().state,
        CommandState::Unknown
    );
    let recovery_after_unknown = store.recover_rule_children(None, 100).unwrap();
    assert!(
        !recovery_after_unknown
            .iter()
            .any(|command| command.command.id == launch_id || command.command.id == linked_id)
    );
    assert_eq!(
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        commands_before_unknown_replay
    );
    assert_eq!(store.high_water().unwrap(), events_before_unknown_replay);
}

#[test]
fn targetless_launch_does_not_borrow_trigger_agent_for_agent_limit() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 1_000;
    let rule_id = "targetless-agent-limit";
    let value = json!({
        "id": rule_id,
        "enabled": true,
        "limit": {"max":1,"per":"agent","window_ms":60_000}
    });
    let definition = RuleDefinitionInput {
        id: rule_id.to_owned(),
        canonical_definition: serde_json::to_string(&value).unwrap(),
        definition_digest: eliot_core::canonical_json_sha256(&value).unwrap(),
        enabled: true,
    };
    let state = store
        .ensure_rule_state("owner", &definition, now)
        .expect("ensure per-agent rule limit");
    let trigger_agent = ready_agent(&mut store, "targetless-agent-trigger", now + 1);

    let first_trigger = store
        .submit(
            "owner",
            send_request(&trigger_agent, 1, "seed trigger-agent bucket"),
            now + 2,
        )
        .expect("admit first trigger event");
    let first_event = first_trigger
        .events
        .iter()
        .find(|event| {
            event
                .data
                .get("command_id")
                .and_then(serde_json::Value::as_str)
                == Some(first_trigger.command.id.as_str())
        })
        .expect("first trigger command event");
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(trigger_agent.clone());
    stop.expected_generation = Some(1);
    let prior = store
        .admit_rule_action("owner", &state, first_event.seq, stop, now + 3)
        .expect("admit trigger-agent stop");
    let prior_child = prior.child.as_ref().unwrap().command.id.clone();
    store
        .settle(
            &prior_child,
            CommandState::Rejected,
            Some(json!({"reason": "seed target bucket"})),
            now + 4,
        )
        .expect("close trigger-agent bucket action");

    let second_trigger = store
        .submit(
            "owner",
            send_request(&trigger_agent, 1, "targetless launch event"),
            now + 5,
        )
        .expect("admit second trigger event");
    let second_event = second_trigger
        .events
        .iter()
        .find(|event| {
            event
                .data
                .get("command_id")
                .and_then(serde_json::Value::as_str)
                == Some(second_trigger.command.id.as_str())
        })
        .expect("second trigger command event");
    let request = targetless_rule_launch_request(
        "synthetic-agent-limit-launch",
        "synthetic-agent-limit-launch",
        Some("linked input"),
    );
    assert!(request.target.is_none());
    let launch = store
        .admit_rule_action("owner", &prior.state, second_event.seq, request, now + 6)
        .expect("targetless launch has no Agent bucket to borrow");
    assert!(launch.is_new);
    let launch_action = launch.child.as_ref().expect("admitted targetless launch");
    let launch_command = &launch_action.command;
    let trigger_agent_num = eliot_core::parse_agent_id(&trigger_agent).unwrap();
    assert_ne!(
        launch_command.target.as_deref(),
        Some(trigger_agent.as_str())
    );
    let trigger_bucket_count: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM commands
             WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL AND target = ?1",
            [trigger_agent_num],
            |row| row.get(0),
        )
        .expect("read existing trigger-agent rule bucket");
    assert_eq!(trigger_bucket_count, 1);
    assert_eq!(
        launch_action
            .linked_command
            .as_ref()
            .expect("targetless launch linked SEND")
            .generation,
        Some(1)
    );
}

#[test]
fn event_rule_replies_bind_open_request_turn_and_replay_without_competing_reply() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 100;
    let (agent_id, _) =
        ready_managed_codex_agent(&mut store, "/synthetic/event-replies", "codex-fixture", now);
    let send = send_request(&agent_id, 1, "open request replies");
    let send_id = send.id.clone();
    store.submit("owner", send, now + 5).unwrap();
    store.dispatch(&send_id, now + 6).unwrap();
    let turn_id = format!("request-turn-{send_id}");
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
                    native_id: Ulid::new().to_string(),
                    data: json!({"command_id":send_id, "turn_id":turn_id}),
                }],
                cursor: None,
            },
            now + 7,
        )
        .expect("commit exact request turn");

    let rule_id = "event-request-replies";
    let rule_value = json!({"id":rule_id,"enabled":true});
    let rule_state = store
        .ensure_rule_state(
            "owner",
            &RuleDefinitionInput {
                id: rule_id.to_owned(),
                canonical_definition: serde_json::to_string(&rule_value).unwrap(),
                definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
                enabled: true,
            },
            now + 8,
        )
        .unwrap();
    let open_request = |native_id: &str, request_id: &str, kind: &str, details| NativeEvent {
        ts: None,
        kind: "request.opened".to_owned(),
        native_id: native_id.to_owned(),
        data: json!({
            "command_id":send_id,
            "request_id":request_id,
            "turn_id":turn_id,
            "kind":kind,
            "details":details,
        }),
    };
    let opened = store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![
                    open_request("reply-forged", "request-forged", "approval", json!({})),
                    open_request("reply-approve", "request-approve", "approval", json!({})),
                    open_request("reply-deny", "request-deny", "approval", json!({})),
                    open_request(
                        "reply-answer",
                        "request-answer",
                        "question",
                        json!({"questions":[{"id":"q1","is_secret":false}]}),
                    ),
                    open_request("reply-closed", "request-closed", "approval", json!({})),
                ],
                cursor: None,
            },
            now + 9,
        )
        .expect("commit pending request events");
    let event_seq = |request_id: &str| {
        opened
            .events
            .iter()
            .find(|event| event.kind == "request.opened" && event.data["request_id"] == request_id)
            .expect("request.opened event is retained")
            .seq
    };
    let reply = |verb: &str, request_id: &str, text: Option<&str>| {
        let mut request = SubmitCommand::new(String::new(), verb);
        request.target = Some(agent_id.clone());
        // Rule-driven answers carry the structured #327 answer form.
        request.args = match (verb, text) {
            ("answer", Some(text)) => json!({"answers": {"q1": {"answers": [text]}}}),
            (_, text) => text.map_or_else(|| json!({}), |text| json!({"text":text})),
        };
        request.expected_generation = Some(1);
        request.expected_turn_id = Some(turn_id.clone());
        request.request_id = Some(request_id.to_owned());
        request
    };

    let forged = store
        .admit_rule_action(
            "owner",
            &rule_state,
            event_seq("request-forged"),
            reply("approve", "request-approve", None),
            now + 10,
        )
        .unwrap();
    assert!(forged.child.is_none());
    assert_eq!(
        forged.parent.unwrap().outcome.unwrap()["code"],
        "rule_action_binding_invalid"
    );

    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent_id.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "request.closed".to_owned(),
                    native_id: "reply-closed-native".to_owned(),
                    data: json!({"request_id":"request-closed","turn_id":turn_id}),
                }],
                cursor: None,
            },
            now + 11,
        )
        .expect("close the captured request");
    let closed = store
        .admit_rule_action(
            "owner",
            &forged.state,
            event_seq("request-closed"),
            reply("approve", "request-closed", None),
            now + 12,
        )
        .unwrap_err();
    assert_eq!(closed.code, "source_integrity");

    let approved = store
        .admit_rule_action(
            "owner",
            &forged.state,
            event_seq("request-approve"),
            reply("approve", "request-approve", None),
            now + 13,
        )
        .unwrap();
    let approved_parent_id = approved.parent.as_ref().unwrap().id.clone();
    let approved_child = approved.child.as_ref().unwrap().command.clone();
    assert_eq!(approved_child.verb, "approve");
    assert_eq!(
        approved_child.request_id.as_deref(),
        Some("request-approve")
    );
    assert_eq!(approved_child.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(approved_child.target.as_deref(), Some(agent_id.as_str()));
    assert_eq!(approved_child.generation, Some(1));

    store.dispatch(&approved_child.id, now + 14).unwrap();
    store
        .settle(
            &approved_child.id,
            CommandState::Unknown,
            Some(json!({"reason":"reply result unknown"})),
            now + 15,
        )
        .unwrap();
    let high_water_before_replay = store.high_water().unwrap();
    let replay = store
        .admit_rule_action(
            "owner",
            &approved.state,
            event_seq("request-approve"),
            reply("approve", "request-approve", None),
            now + 16,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert!(replay.events.is_empty());
    assert_eq!(replay.parent.as_ref().unwrap().id, approved_parent_id);
    assert_eq!(replay.child.as_ref().unwrap().command.id, approved_child.id);
    assert_eq!(store.high_water().unwrap(), high_water_before_replay);

    let mut competing = reply("approve", "request-approve", None);
    competing.id = Ulid::new().to_string();
    let competing_id = competing.id.clone();
    assert_eq!(
        store.submit("owner", competing, now + 17).unwrap_err().code,
        "bad_state"
    );
    assert!(store.get_command(&competing_id).unwrap().is_none());

    let denied = store
        .admit_rule_action(
            "owner",
            &replay.state,
            event_seq("request-deny"),
            reply("deny", "request-deny", None),
            now + 18,
        )
        .unwrap();
    assert_eq!(denied.child.as_ref().unwrap().command.verb, "deny");
    assert_eq!(
        denied.child.as_ref().unwrap().command.request_id.as_deref(),
        Some("request-deny")
    );

    let answered = store
        .admit_rule_action(
            "owner",
            &denied.state,
            event_seq("request-answer"),
            reply("answer", "request-answer", Some("ordinary answer")),
            now + 19,
        )
        .unwrap();
    assert_eq!(answered.child.as_ref().unwrap().command.verb, "answer");
    assert_eq!(
        answered
            .child
            .as_ref()
            .unwrap()
            .command
            .request_id
            .as_deref(),
        Some("request-answer")
    );
    assert_eq!(
        answered.child.as_ref().unwrap().command.args,
        json!({"answers": {"q1": {"answers": ["ordinary answer"]}}})
    );
}

#[test]
fn project_event_reply_requires_the_explicit_per_verb_grant() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64") + 100;
    let project = "project-event-reply-grant";
    let principal = format!("project:{project}");
    let agent = ready_agent(&mut store, project, now);
    let turn_id = start_fake_turn(&mut store, &agent, now + 1);
    let send_id = turn_id
        .strip_prefix("fake-turn-")
        .expect("Fake turn id retains its source send id");
    let checkout = "synthetic-workspace";
    let mut registration = ProjectRegistrationInput {
        id: project.to_owned(),
        checkout: checkout.to_owned(),
        repo_common_dir: format!("{checkout}/.git"),
        git: None,
        profiles: vec!["fake".to_owned()],
        actions: vec!["approve".to_owned()],
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    store
        .upsert_project_registration("owner", &registration, now + 2)
        .unwrap();
    let rule_id = "project-reply-rule";
    let rule_value = json!({
        "id": rule_id,
        "enabled": true,
        "on": {"event":"request.opened"},
        "do": [{"action":"approve"}]
    });
    store
        .ensure_rule_state(
            &principal,
            &RuleDefinitionInput {
                id: rule_id.to_owned(),
                canonical_definition: serde_json::to_string(&rule_value).unwrap(),
                definition_digest: eliot_core::canonical_json_sha256(&rule_value).unwrap(),
                enabled: true,
            },
            now + 3,
        )
        .unwrap();
    let open_request = |native_id: &str, request_id: &str| NativeEvent {
        ts: None,
        kind: "request.opened".to_owned(),
        native_id: native_id.to_owned(),
        data: json!({
            "command_id": send_id,
            "request_id": request_id,
            "turn_id": turn_id,
            "kind": "approval",
            "details": {}
        }),
    };
    let first_open = store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![open_request("project-approval-1", "project-request-1")],
                cursor: None,
            },
            now + 5,
        )
        .unwrap()
        .events
        .into_iter()
        .find(|event| event.kind == "request.opened")
        .unwrap();
    registration.actions = vec!["stop".to_owned()];
    store
        .upsert_project_registration("owner", &registration, now + 6)
        .unwrap();

    let make_approval = |request_id: &str| {
        let mut request = SubmitCommand::new(String::new(), "approve");
        request.target = Some(agent.clone());
        request.args = json!({});
        request.expected_generation = Some(1);
        request.expected_turn_id = Some(turn_id.clone());
        request.request_id = Some(request_id.to_owned());
        request
    };
    let denied = store
        .admit_rule_action(
            &principal,
            &store.get_rule_state(&principal, rule_id).unwrap().unwrap(),
            first_open.seq,
            make_approval("project-request-1"),
            now + 7,
        )
        .unwrap();
    assert!(denied.child.is_none());
    assert_eq!(
        denied.parent.unwrap().outcome.unwrap()["code"],
        "not_allowed"
    );

    registration.actions = vec!["stop".to_owned(), "approve".to_owned()];
    store
        .upsert_project_registration("owner", &registration, now + 8)
        .unwrap();
    let second_open = store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![open_request("project-approval-2", "project-request-2")],
                cursor: None,
            },
            now + 9,
        )
        .unwrap()
        .events
        .into_iter()
        .find(|event| event.kind == "request.opened")
        .unwrap();
    let current = store.get_rule_state(&principal, rule_id).unwrap().unwrap();
    let approved = store
        .admit_rule_action(
            &principal,
            &current,
            second_open.seq,
            make_approval("project-request-2"),
            now + 10,
        )
        .unwrap();
    let child = approved
        .child
        .expect("approved project reply child")
        .command;
    assert_eq!(child.verb, "approve");
    assert_eq!(child.principal, principal);
    assert_eq!(child.turn_id.as_deref(), Some(turn_id.as_str()));
    assert_eq!(child.request_id.as_deref(), Some("project-request-2"));
    assert_eq!(
        store.dispatch(&child.id, now + 11).unwrap().value.state,
        CommandState::Dispatched
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
        selector: None,
        verb: verb.to_owned(),
        args,
        expected_generation: None,
        expected_turn_id: None,
        request_id,
        probe: false,
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
    install_fixture_sources(&mut store, &agent_id, &launch_id, 1, now);
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
