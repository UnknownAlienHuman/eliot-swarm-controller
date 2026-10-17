// Included inside driver::tests. All frames cross the existing MspTransport
// fixture; no provider, native executable, or alternate protocol reducer runs.
mod native_event_fences {
    use super::*;
    use tokio::sync::{Barrier, Notify, oneshot};

    async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(2), future)
            .await
            .expect("native event fence fixture deadline")
    }

    async fn wait_for(driver: &Driver, condition: impl Fn(&State) -> bool) {
        let waited = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let reached = {
                    let state = driver.lock().unwrap();
                    condition(&state)
                };
                if reached {
                    return;
                }
                tokio::task::yield_now().await;
            }
        }).await;
        if let Err(error) = waited {
            let state = driver.lock().unwrap();
            panic!(
                "native event fence fixture deadline: {error:?}; event_number={}; root_revision={}; diagnostic={:?}; transport_ended={}; ready={}",
                state.event_number, state.revision(ROOT), state.diagnostic,
                state.transport_ended, state.ready,
            );
        }
    }

    #[test]
    fn muse_driver_native_notification_definitions_match_pinned_schema() {
        let document: Value = serde_json::from_str(schema::MSP_SCHEMA_JSON).unwrap();
        let notifications = document["notifications"].as_object().unwrap();
        let mut definitions = 0;
        for (name, notification) in notifications {
            let Some(params) = notification.get("params") else {
                assert_eq!(name, "initialized");
                continue;
            };
            let expected = params["$ref"].as_str().unwrap()
                .strip_prefix("#/$defs/").unwrap();
            let selected: Vec<_> = NATIVE_NOTIFICATION_DEFINITIONS.iter()
                .filter(|entry| entry.0 == name.as_str()).collect();
            assert_eq!(selected.len(), 1, "notification mapping for {name}");
            assert_eq!(selected[0].1, expected, "notification definition for {name}");
            definitions += 1;
        }
        assert_eq!(definitions, NATIVE_NOTIFICATION_DEFINITIONS.len());

        let valid = json!({"sessionId":ROOT,"modelId":"changed-model","source":"user",
            "viewCursor":"model:1","sourceRange":{"stream":{"kind":"session","id":ROOT},
            "first":{"id":"synthetic-model-change","sequence":1},
            "last":{"id":"synthetic-model-change","sequence":1}}});
        validate_native_notification_parameters("session/modelChanged", &valid).unwrap();
        let mut invalid = valid;
        invalid.as_object_mut().unwrap().remove("sourceRange");
        assert!(matches!(
            validate_native_notification_parameters("session/modelChanged", &invalid),
            Err(schema::SchemaError::InvalidValue { .. })
        ));
    }

    #[derive(Debug)]
    enum PeerAction {
        HoldRead {
            armed: oneshot::Sender<()>,
            received: oneshot::Sender<()>,
        },
        Notification {
            frame: Value,
            flushed: oneshot::Sender<()>,
        },
        Close,
    }

    async fn gated_peer(
        peer: DuplexStream,
        workspace: PathBuf,
        calls: Arc<Mutex<Vec<Value>>>,
        mut actions: mpsc::Receiver<PeerAction>,
        release_read: Arc<Notify>,
    ) {
        let (read, mut write) = tokio::io::split(peer);
        let mut lines = BufReader::new(read).lines();
        let mut initialized = false;
        let mut current_session = session(&workspace);
        let mut child_sessions = BTreeMap::new();
        let mut hold_next_read: Option<oneshot::Sender<()>> = None;
        let mut held_response = None;
        let mut held_session: Option<String> = None;
        loop {
            tokio::select! {
                _ = release_read.notified(), if held_response.is_some() => {
                    send(&mut write, held_response.take().unwrap()).await;
                    held_session = None;
                }
                action = actions.recv() => {
                    match action {
                        Some(PeerAction::HoldRead { armed, received }) => {
                            assert!(held_response.is_none());
                            assert!(hold_next_read.replace(received).is_none());
                            armed.send(()).unwrap();
                        }
                        Some(PeerAction::Notification { frame, flushed }) => {
                            if frame["method"] == "item/completed"
                                && frame["params"]["item"]["kind"] == "subagent"
                            {
                                let child_id = frame["params"]["item"]["childSessionId"].as_str().unwrap();
                                let mut child = session(&workspace);
                                child["sessionId"] = json!(child_id);
                                child_sessions.insert(child_id.to_owned(), child);
                            }
                            // The held response was captured before this change.
                            // A subsequent real read must see the new peer state.
                            if frame["method"] == "session/modelChanged" {
                                let target = held_session.as_deref().unwrap_or(ROOT);
                                if target == ROOT {
                                    current_session["modelId"] = frame["params"]["modelId"].clone();
                                } else {
                                    child_sessions.get_mut(target).unwrap()["modelId"] = frame["params"]["modelId"].clone();
                                }
                            }
                            send(&mut write, frame).await;
                            flushed.send(()).unwrap();
                        }
                        Some(PeerAction::Close) | None => return,
                    }
                }
                line = lines.next_line() => {
                    let Some(line) = line.unwrap() else { return; };
                    let request: Value = serde_json::from_str(&line).unwrap();
                    calls.lock().unwrap().push(request.clone());
                    let method = request["method"].as_str().unwrap();
                    let result = match method {
                        "initialize" => {
                            let mut result = initialize(&workspace);
                            result["grantedCapabilities"] = json!(["sessionMcp"]);
                            result["sessionDurability"] = json!("durable");
                            result
                        }
                        "initialized" => {
                            assert!(request.get("id").is_none());
                            initialized = true;
                            continue;
                        }
                        "session/start" => {
                            assert!(initialized);
                            assert_eq!(request["params"]["sessionId"], ROOT);
                            json!({"session":current_session,"viewCursor":"root:0"})
                        }
                        "session/read" => {
                            let target = request["params"]["sessionId"].as_str().unwrap();
                            let snapshot = if target == ROOT {
                                &current_session
                            } else {
                                child_sessions.get(target).expect("read requires an emitted parent-item child edge")
                            };
                            json!({"session":snapshot,"viewCursor":"root:1",
                                "history":{"mode":"none","items":null,"snapshot":null},
                                "pendingRequests":[]})
                        }
                        "view/page" => json!({"events":[],"nextCursor":null}),
                        "view/subscribe" => json!({"viewCursor":"root:1"}),
                        "approval/listPending" => json!({"approvals":[],"userInputs":[]}),
                        "usage/read" => json!({"usage":null}),
                        _ => panic!("unexpected native effect in fence fixture: {method}"),
                    };
                    let response = json!({"jsonrpc":"2.0","id":request["id"],"result":result});
                    if method == "session/read"
                        && let Some(received) = hold_next_read.take()
                    {
                        assert!(held_response.replace(response).is_none());
                        held_session = Some(request["params"]["sessionId"].as_str().unwrap().to_owned());
                        received.send(()).unwrap();
                    } else {
                        send(&mut write, response).await;
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn muse_driver_malformed_native_events_revoke_actual_read_and_ticket() {
        let dir = Directory::new();
        let (driver, runtime, peer) = fixture(&dir.0);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (actions, receiver) = mpsc::channel(16);
        let release = Arc::new(Notify::new());
        let native = tokio::spawn(gated_peer(
            peer, dir.0.clone(), calls.clone(), receiver, release.clone(),
        ));
        bounded(driver.clone().execute(command(&driver, "01-open", "agent.open", json!({}))))
            .await
            .unwrap();
        let transport = driver.lock().unwrap().transport.clone().unwrap();
        let checkpoint = std::fs::read(dir.0.join("muse-rust-checkpoint.v1.json")).unwrap();

        // The first missing identity must also fence a root with no event entry.
        for (index, session_id) in [
            None, Some(json!(ROOT)), Some(json!(42)), Some(json!("")), Some(json!("outside-family")),
        ]
            .into_iter()
            .enumerate()
        {
            let (authority, revision, event_number, dirty) = {
                let state = driver.lock().unwrap();
                let authority = state.journal.as_ref().unwrap()
                    .authorize_session(state.scope.as_ref().unwrap(), ROOT).unwrap();
                assert!(state.journal.as_ref().unwrap()
                    .authority_is_current(state.scope.as_ref().unwrap(), &authority));
                (authority, state.revision(ROOT), state.event_number, state.dirty)
            };
            let (armed, armed_rx) = oneshot::channel();
            let (received, received_rx) = oneshot::channel();
            actions.send(PeerAction::HoldRead { armed, received }).await.unwrap();
            bounded(armed_rx).await.unwrap();
            let reading_driver = driver.clone();
            let reading_transport = transport.clone();
            let read = tokio::spawn(async move {
                reading_driver.snapshot(&reading_transport, ROOT).await
            });
            bounded(received_rx).await.unwrap();

            let model = format!("changed-model-{index}");
            let mut params = json!({"modelId":model,"viewCursor":"invalid:change"});
            if let Some(session_id) = session_id {
                params["sessionId"] = session_id;
            }
            // Missing required source/sourceRange, despite a valid RPC envelope.
            assert!(matches!(
                validate_native_notification_parameters("session/modelChanged", &params),
                Err(schema::SchemaError::InvalidValue { .. })
            ));
            let frame = json!({"jsonrpc":"2.0","method":"session/modelChanged","params":params});
            for repeated in 0..2 {
                let (flushed, flushed_rx) = oneshot::channel();
                actions.send(PeerAction::Notification { frame: frame.clone(), flushed }).await.unwrap();
                bounded(flushed_rx).await.unwrap();
                wait_for(&driver, |state| {
                    state.event_number > event_number + repeated
                        && state.diagnostic == Some("MUSE_NOTIFICATION_PROJECTION_UNAVAILABLE")
                }).await;
                let state = driver.lock().unwrap();
                assert_eq!(state.revision(ROOT), revision + repeated + 1);
                assert!(state.ready && state.initialized && !state.transport_ended);
                assert!(!state.versions.contains_key("outside-family"));
                assert_eq!(state.dirty, dirty + u64::from(index == 0));
                let journal = state.journal.as_ref().unwrap();
                let scope = state.scope.as_ref().unwrap();
                assert!(!journal.authority_is_current(scope, &authority));
                assert!(journal.authorize_session(scope, ROOT).is_err());
            }
            assert_eq!(std::fs::read(dir.0.join("muse-rust-checkpoint.v1.json")).unwrap(), checkpoint);
            release.notify_one();
            match bounded(read).await.unwrap() {
                Err(error) => assert_eq!(error.code, "MUSE_SESSION_READ_RACED"),
                Ok(_) => panic!("a pre-event actual read must not become current work"),
            }
            let fresh = bounded(driver.snapshot(&transport, ROOT)).await.unwrap();
            assert_eq!(fresh.value["modelId"], model);
            let state = driver.lock().unwrap();
            let journal = state.journal.as_ref().unwrap();
            let scope = state.scope.as_ref().unwrap();
            assert!(!journal.authority_is_current(scope, &authority));
            let fresh_authority = journal.authorize_session(scope, ROOT).unwrap();
            assert!(journal.authority_is_current(scope, &fresh_authority));
        }
        bounded(shutdown(&driver, &runtime)).await;
        bounded(native).await.unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(calls.iter().filter(|call| call["method"] == "session/start").count(), 1);
        assert!(!calls.iter().any(|call| call["method"] == "turn/start"));
        assert_eq!(runtime.starts.load(Ordering::SeqCst), 1);
    }

    async fn emit_valid(driver: &Driver, actions: &mpsc::Sender<PeerAction>, frame: Value) {
        validate_native_notification_parameters(
            frame["method"].as_str().unwrap(), &frame["params"],
        ).unwrap();
        let before = driver.lock().unwrap().event_number;
        let (flushed, flushed_rx) = oneshot::channel();
        bounded(actions.send(PeerAction::Notification { frame, flushed })).await.unwrap();
        bounded(flushed_rx).await.unwrap();
        wait_for(driver, |state| state.event_number > before).await;
    }

    fn opaque_cursor(index: usize) -> String {
        // Native cursors are opaque. Exercise the existing 1024-byte limit.
        format!("{index:04}:{}", "v".repeat(1019))
    }

    fn source_range(index: usize) -> Value {
        let record = format!("{index:04}:{}", "r".repeat(480));
        json!({"stream":{"kind":"session","id":ROOT},
            "first":{"id":record,"sequence":index + 1},
            "last":{"id":record,"sequence":index + 1}})
    }

    #[tokio::test]
    async fn muse_driver_publicly_omitted_child_actual_read_is_fenced_without_granting_authority() {
        let dir = Directory::new();
        let (driver, runtime, peer) = fixture(&dir.0);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (actions, receiver) = mpsc::channel(16);
        let release = Arc::new(Notify::new());
        let native = tokio::spawn(gated_peer(
            peer, dir.0.clone(), calls.clone(), receiver, release.clone(),
        ));
        bounded(driver.clone().execute(command(&driver, "01-open", "agent.open", json!({}))))
            .await.unwrap();
        let transport = driver.lock().unwrap().transport.clone().unwrap();
        let parent_turn = uuid::Uuid::now_v7().to_string();
        let mut children = Vec::new();
        let mut items = Vec::new();
        // Every child enters through a schema-valid native parent-item event.
        // No fabricated projection row or change to any reducer budget.
        for index in 0..96 {
            let child = uuid::Uuid::now_v7().to_string();
            let item = uuid::Uuid::now_v7().to_string();
            emit_valid(&driver, &actions, json!({"jsonrpc":"2.0","method":"item/completed",
                "params":{"sessionId":ROOT,"viewCursor":opaque_cursor(index),
                    "sourceRange":source_range(index),"item":{"itemId":item,
                        "kind":"subagent","status":"completed","revision":1,
                        "turnId":parent_turn,"childSessionId":child,"subagentId":child,
                        "result":{"artifactRefs":[],"evidenceRefs":[],"summary":"synthetic"}}}})).await;
            children.push(child);
            items.push(item);
        }
        let child = children[0].clone();
        {
            let state = driver.lock().unwrap();
            assert_eq!(state.children(), children);
            assert!(!state.versions.contains_key(&child));
            let public = state.projector.as_ref().unwrap().observation().unwrap();
            assert!(public["observed_children"].as_array().unwrap().iter()
                .any(|row| row["sessionId"] == child));
        }
        let (armed, armed_rx) = oneshot::channel();
        let (received, received_rx) = oneshot::channel();
        actions.send(PeerAction::HoldRead { armed, received }).await.unwrap();
        bounded(armed_rx).await.unwrap();
        let reading_driver = driver.clone();
        let reading_transport = transport.clone();
        let reading_child = child.clone();
        let read = tokio::spawn(async move {
            reading_driver.refresh_one(&reading_transport, &reading_child).await
        });
        bounded(received_rx).await.unwrap();

        // Valid retained history triggers the REAL public fallback, which
        // keeps only the latest 32 children. The outstanding child read has
        // no versions entry and root-only events must not change that.
        let mut omitted = false;
        for index in 96..96 + 256 {
            emit_valid(&driver, &actions, json!({"jsonrpc":"2.0","method":"turn/completed",
                "params":{"sessionId":ROOT,"turnId":uuid::Uuid::now_v7().to_string(),
                    "terminal":"completed","viewCursor":opaque_cursor(index),
                    "sourceRange":source_range(index)}})).await;
            let state = driver.lock().unwrap();
            let public = state.projector.as_ref().unwrap().observation().unwrap();
            if public["observation_projection_state"] == "partial" {
                assert!(!public["observed_children"].as_array().unwrap().iter()
                    .any(|row| row["sessionId"] == child));
                omitted = true;
                break;
            }
        }
        assert!(omitted, "existing bounded native history must reach public child omission");
        let event_number = {
            let state = driver.lock().unwrap();
            assert!(!state.versions.contains_key(&child));
            let projector = state.projector.as_ref().unwrap();
            assert_eq!(projector.observed_child_ids().len(), children.len());
            assert!(projector.observed_child_ids().any(|id| id == child));
            let edge = projector.observed_child_provenance(&child).unwrap();
            assert_eq!(edge.parent_session_id, ROOT);
            assert_eq!(edge.item_id, items[0]);
            assert_eq!(edge.parent_turn_id, Some(parent_turn.as_str()));
            assert_eq!(edge.item_revision, 1);
            // Keep this regression inside the existing Journal entry budget.
            assert!(serde_json::to_vec(&projector.checkpoint().unwrap()).unwrap().len() < 1024 * 1024);
            let journal = state.journal.as_ref().unwrap();
            assert!(journal.authorize_session(state.scope.as_ref().unwrap(), &child).is_err());
            state.event_number
        };
        let checkpoint = std::fs::read(dir.0.join("muse-rust-checkpoint.v1.json")).unwrap();
        // Deliberately unattributable AND rejected by the notification $def.
        // Receipt by the event consumer, rather than a desired child revision,
        // is the gate: the original omission bug must reach the read assertion.
        let params = json!({"modelId":"child-after-change","viewCursor":"invalid:child"});
        assert!(matches!(
            validate_native_notification_parameters("session/modelChanged", &params),
            Err(schema::SchemaError::InvalidValue { .. })
        ));
        let (flushed, flushed_rx) = oneshot::channel();
        actions.send(PeerAction::Notification {
            frame: json!({"jsonrpc":"2.0","method":"session/modelChanged","params":params}),
            flushed,
        }).await.unwrap();
        bounded(flushed_rx).await.unwrap();
        wait_for(&driver, |state| state.event_number > event_number
            && state.diagnostic == Some("MUSE_NOTIFICATION_PROJECTION_UNAVAILABLE")).await;
        assert_eq!(std::fs::read(dir.0.join("muse-rust-checkpoint.v1.json")).unwrap(), checkpoint);
        release.notify_one();
        match bounded(read).await.unwrap() {
            Err(error) => assert_eq!(error.code, "MUSE_SESSION_READ_RACED"),
            Ok(()) => panic!("a publicly omitted child's pre-event actual read must be rejected"),
        }
        // Fresh native data may be projected using the retained exact edge,
        // even while the PUBLIC child row is omitted. This grants no rights:
        // the parent's snapshot is still stale and Journal must refuse a ticket.
        let fresh = bounded(driver.snapshot(&transport, &child)).await.unwrap();
        assert_eq!(fresh.value["modelId"], "child-after-change");
        driver.apply_snapshot(&child, &fresh).unwrap();
        {
            let state = driver.lock().unwrap();
            assert_eq!(state.revision(&child), 1);
            assert_eq!(state.children(), children);
            assert!(children.iter().all(|child| state.revision(child) == 1));
            assert!(!state.member("outside-family"));
            assert!(!state.versions.contains_key("outside-family"));
            let projector = state.projector.as_ref().unwrap();
            let public = projector.observation().unwrap();
            assert!(!public["observed_children"].as_array().unwrap().iter()
                .any(|row| row["sessionId"] == child));
            let retained = projector.checkpoint().unwrap();
            let row = retained["state"]["observed_children"].as_array().unwrap().iter()
                .find(|row| row["sessionId"] == child).unwrap();
            assert_eq!(row["snapshot"]["modelId"], "child-after-change");
            assert_eq!(row["snapshot_freshness"], "fresh");
            assert_eq!(row["parentSessionId"], ROOT);
            assert_eq!(row["itemId"], items[0]);
            assert_eq!(row["parentTurnId"], parent_turn);
            assert_eq!(row["revision"], 1);
            let journal = state.journal.as_ref().unwrap();
            assert!(journal.authorize_session(state.scope.as_ref().unwrap(), &child).is_err());
        }
        bounded(shutdown(&driver, &runtime)).await;
        bounded(native).await.unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(calls.iter().filter(|call| call["method"] == "session/start").count(), 1);
        assert_eq!(calls.iter().filter(|call| call["method"] == "session/read"
            && call["params"]["sessionId"] == child).count(), 2);
        assert!(!calls.iter().any(|call| call["method"] == "turn/start"
            || call["method"] == "session/fork"
            || call["method"].as_str().unwrap().starts_with("subagent/")));
        assert_eq!(runtime.starts.load(Ordering::SeqCst), 1);
    }

    fn fresh_fixture(dir: &Path) -> (Arc<Driver>, Arc<PipeRuntime>, DuplexStream) {
        let (owned, peer) = tokio::io::duplex(64 * 1024);
        let runtime = Arc::new(PipeRuntime {
            pipe: Mutex::new(Some(owned)),
            starts: AtomicUsize::new(0),
            drained: AtomicBool::new(false),
            joined: AtomicBool::new(false),
        });
        let driver = Driver::with_state(bootstrap(dir), runtime.clone(), State::empty()).unwrap();
        (driver, runtime, peer)
    }

    #[tokio::test]
    async fn muse_driver_actual_initialized_flush_cannot_finalize_after_eof_or_stop() {
        for fresh_root in [false, true] {
            for eof in [true, false] {
                let dir = Directory::new();
                let (driver, runtime, peer) = if fresh_root {
                    fresh_fixture(&dir.0)
                } else {
                    fixture(&dir.0)
                };
                let checkpoint_path = dir.0.join("muse-rust-checkpoint.v1.json");
                let before = std::fs::read(&checkpoint_path).ok();
                assert_eq!(before.is_none(), fresh_root);
                let calls = Arc::new(Mutex::new(Vec::new()));
                let (actions, receiver) = mpsc::channel(16);
                let native = tokio::spawn(gated_peer(
                    peer, dir.0.clone(), calls.clone(), receiver, Arc::new(Notify::new()),
                ));
                let gate = Arc::new(Barrier::new(2));
                *driver.initialize_finalize_gate.lock().unwrap() = Some(gate.clone());
                let starting_driver = driver.clone();
                let open = command(&driver, "01-open", "agent.open", json!({}));
                let start = tokio::spawn(async move { starting_driver.start(&open).await });
                // This barrier is reached only after MspTransport's real flush
                // receipt. No state lock is held while the native consumer runs.
                bounded(gate.wait()).await;
                if eof {
                    actions.send(PeerAction::Close).await.unwrap();
                    wait_for(&driver, |state| state.transport_ended).await;
                } else {
                    driver.request_stop();
                    assert!(driver.stopping());
                }
                bounded(gate.wait()).await;
                match bounded(start).await.unwrap() {
                    Err(error) => assert_eq!(error.code, "MUSE_NATIVE_CONNECTION_ENDED"),
                    Ok(_) => panic!("ended/stopping initialization must not finalize"),
                }
                {
                    let state = driver.lock().unwrap();
                    assert!(!state.initialized && !state.ready && !state.session_mcp_granted);
                    assert_eq!(state.durability, commands::SessionDurability::Unrecognized);
                    assert!(state.contexts.is_empty());
                    assert!(state.confirmed_root.is_none());
                    assert_eq!(state.scope.is_none(), fresh_root);
                    if let (Some(journal), Some(scope)) = (&state.journal, &state.scope) {
                        assert!(journal.authorize_session(scope, ROOT).is_err());
                        let native_id = uuid::Uuid::now_v7().to_string();
                        assert!(matches!(journal.native_invocation(
                            scope, "session/start", &native_id,
                            object(json!({"sessionId":ROOT,"commandId":native_id,
                                "workspaceRoot":dir.0,"config":{"mcpServers":{}}})).unwrap(),
                        ), Err(journal::Fault::GrantRequired)));
                    }
                }
                assert_eq!(std::fs::read(&checkpoint_path).ok(), before);
                if fresh_root {
                    assert!(!dir.0.join("muse-rust-scope.v1.json").exists());
                }
                bounded(shutdown(&driver, &runtime)).await;
                bounded(native).await.unwrap();
                let calls = calls.lock().unwrap();
                assert_eq!(calls.iter().filter(|call| call["method"] == "initialize").count(), 1);
                assert!(!calls.iter().any(|call| call["method"] == "session/start"));
                assert_eq!(runtime.starts.load(Ordering::SeqCst), 1);
            }
        }
    }
}
