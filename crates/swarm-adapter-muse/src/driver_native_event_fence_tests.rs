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
        bounded(async {
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
        })
        .await;
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
        let mut hold_next_read: Option<oneshot::Sender<()>> = None;
        let mut held_response = None;
        loop {
            tokio::select! {
                _ = release_read.notified(), if held_response.is_some() => {
                    send(&mut write, held_response.take().unwrap()).await;
                }
                action = actions.recv() => {
                    match action {
                        Some(PeerAction::HoldRead { armed, received }) => {
                            assert!(held_response.is_none());
                            assert!(hold_next_read.replace(received).is_none());
                            armed.send(()).unwrap();
                        }
                        Some(PeerAction::Notification { frame, flushed }) => {
                            // The held response was captured before this change.
                            // A subsequent real read must see the new peer state.
                            current_session["modelId"] = frame["params"]["modelId"].clone();
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
                            assert_eq!(request["params"]["sessionId"], ROOT);
                            json!({"session":current_session,"viewCursor":"root:1",
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
            assert!(schema::validate_method_parameters("session/modelChanged", &params).is_err());
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
