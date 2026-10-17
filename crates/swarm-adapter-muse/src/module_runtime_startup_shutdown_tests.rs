// Included inside runtime_tests so these tests use the real RunOwner/Loop,
// ModuleLink and MSP transports. The external custodian here owns a duplex
// peer task, not an OS child: EOF/join prove this pipe boundary only.
#[derive(Clone, Copy)]
enum StartupGate {
    BeforeAttachment,
    BeforePublication,
    AfterPublication,
}

#[derive(Default)]
struct StartupPipeFacts {
    frames: Mutex<Vec<Value>>,
    frame_seen: Notify,
    eof: AtomicBool,
}

struct GatedStartupNative {
    gate: StartupGate,
    entered: Notify,
    release_start: Notify,
    starts: AtomicUsize,
    attachments: AtomicUsize,
    peer: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    facts: Arc<StartupPipeFacts>,
    drained: AtomicBool,
    joined: AtomicBool,
}
impl NativeRuntime for GatedStartupNative {
    fn start<'a>(
        &'a self,
        _: &'a NativeOptions,
        router: jsonrpsee_core::server::RpcModule<()>,
    ) -> NativeFuture<'a, NativeAttachment> {
        Box::pin(async move {
            assert_eq!(self.starts.fetch_add(1, Ordering::SeqCst), 0);
            self.entered.notify_one();
            if matches!(self.gate, StartupGate::BeforeAttachment) {
                self.release_start.notified().await;
            }
            let (owned, peer) = tokio::io::duplex(4096);
            let (read, write) = tokio::io::split(owned);
            let (transport, notifications) =
                MspTransport::attach_fixture(read, write, router, Options::default()).unwrap();
            let facts = self.facts.clone();
            let peer = tokio::spawn(async move {
                let mut lines = BufReader::new(peer).lines();
                while let Some(line) = lines.next_line().await.unwrap() {
                    let frame: Value = serde_json::from_str(&line).unwrap();
                    assert_eq!(frame["method"], "turn/start");
                    assert_eq!(frame["params"]["commandId"], STARTUP_COMMAND_ID);
                    {
                        let mut frames = facts.frames.lock().unwrap();
                        assert!(frames.is_empty(), "native input must never be replayed");
                        frames.push(frame);
                    }
                    // Deliberately withhold the native ACK. Admission/write is
                    // not result/Task completion, including during shutdown.
                    facts.frame_seen.notify_one();
                }
                facts.eof.store(true, Ordering::SeqCst);
            });
            // Retain the actual peer handle outside the abortable command
            // before returning the attachment; no post-creation await here.
            *self.peer.try_lock().unwrap() = Some(peer);
            self.attachments.fetch_add(1, Ordering::SeqCst);
            Ok(NativeAttachment {
                transport: Arc::new(transport),
                notifications,
            })
        })
    }
    fn drain_stdio(&self) -> NativeFuture<'_, ()> {
        Box::pin(async move {
            let mut peer = self.peer.lock().await;
            if let Some(task) = peer.as_mut() {
                task.await
                    .map_err(|_| fault("FIXTURE_STARTUP_PEER_FAILED"))?;
            }
            *peer = None;
            self.drained.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
    fn join_custodians(&self) -> NativeFuture<'_, ()> {
        Box::pin(async move {
            assert!(self.drained.load(Ordering::SeqCst));
            if self.attachments.load(Ordering::SeqCst) != 0 {
                assert!(self.facts.eof.load(Ordering::SeqCst));
            }
            assert!(self.peer.lock().await.is_none());
            self.joined.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}

const STARTUP_COMMAND_ID: &str = "019a0000-0000-7000-8000-000000000041";
struct GatedStartupDriver {
    native: Arc<GatedStartupNative>,
    options: NativeOptions,
    transport: Mutex<Option<Arc<MspTransport>>>,
    before_publication: Notify,
    release_publication: Notify,
    published_or_finished: Notify,
    published: AtomicBool,
    finished: AtomicBool,
    stopped: AtomicBool,
    panic_flush: AtomicBool,
}
struct StartupCommandLifetime(Arc<GatedStartupDriver>);
impl Drop for StartupCommandLifetime {
    fn drop(&mut self) {
        self.0.finished.store(true, Ordering::SeqCst);
        self.0.published_or_finished.notify_one();
    }
}
impl ConsumerDriver for GatedStartupDriver {
    fn hello(&self) -> Value {
        json!({"boot_id":"fixture-boot","module_artifact_id":ARTIFACT_ID,"native_ready":false})
    }
    fn request_stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
    fn execute(self: Arc<Self>, command: RuntimeCommand) -> LoopCall<'static, Result<()>> {
        Box::pin(async move {
            let _lifetime = StartupCommandLifetime(self.clone());
            assert_eq!(command.operation_id, "dispatch");
            let attachment = self
                .native
                .start(&self.options, jsonrpsee_core::server::RpcModule::new(()))
                .await?;
            if matches!(self.native.gate, StartupGate::BeforePublication) {
                self.before_publication.notify_one();
                self.release_publication.notified().await;
            }
            // Like Driver::start, a start already in progress can publish
            // after a stop request. RunOwner must join it before final close.
            let transport = attachment.transport.clone();
            *self.transport.lock().unwrap() = Some(transport.clone());
            self.published.store(true, Ordering::SeqCst);
            self.published_or_finished.notify_one();
            let request = transport
                .submit_request(
                    "turn/start",
                    json!({
                        "commandId":STARTUP_COMMAND_ID,"sessionId":"root",
                        "input":[{"type":"text","text":"synthetic startup input"}],"ifBusy":"queue"
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                )
                .map_err(|_| fault("FIXTURE_STARTUP_UNKNOWN"))?;
            request
                .submitted
                .await
                .map_err(|_| fault("FIXTURE_STARTUP_UNKNOWN"))?
                .map_err(|_| fault("FIXTURE_STARTUP_UNKNOWN"))?;
            let _ = request.response.await;
            // Never turn a cancelled/lost reply into a successful outcome.
            std::future::pending::<Result<()>>().await
        })
    }
    fn flush<'a>(&'a self, _: &'a mut ModuleLink) -> LoopCall<'a, Result<()>> {
        Box::pin(async move {
            assert!(
                !self.panic_flush.load(Ordering::SeqCst),
                "fixture consumer panic during startup"
            );
            Ok(())
        })
    }
    fn publish<'a>(&'a self, _: &'a mut ModuleLink) -> LoopCall<'a, bool> {
        Box::pin(async { false })
    }
    fn stop_native(&self) -> LoopCall<'_, ()> {
        Box::pin(async move {
            self.request_stop();
            let snapshot = self.transport.lock().unwrap().clone();
            // Force the bad interleaving if command fencing is moved AFTER
            // stop_native: release startup only after its close snapshot and
            // allow publication before returning. That leaves a real open MSP
            // pipe, so the external peer drain cannot fabricate EOF or join.
            self.native.release_start.notify_one();
            self.release_publication.notify_one();
            if !self.finished.load(Ordering::SeqCst) {
                self.published_or_finished.notified().await;
            }
            if let Some(transport) = snapshot {
                let _ = transport.close().await;
            }
        })
    }
}

async fn gated_startup_shutdown(gate: StartupGate, consumer_panic: bool) {
    let directory = Directory::new();
    let native = Arc::new(GatedStartupNative {
        gate,
        entered: Notify::new(),
        release_start: Notify::new(),
        starts: AtomicUsize::new(0),
        attachments: AtomicUsize::new(0),
        peer: tokio::sync::Mutex::new(None),
        facts: Arc::new(StartupPipeFacts::default()),
        drained: AtomicBool::new(false),
        joined: AtomicBool::new(false),
    });
    let driver = Arc::new(GatedStartupDriver {
        native: native.clone(),
        options: NativeOptions {
            workspace: directory.0.clone(),
            executable: env::current_exe().unwrap(),
            model: None,
            provider: None,
            effort: None,
            approval_mode: None,
            mcp_servers: None,
        },
        transport: Mutex::new(None),
        before_publication: Notify::new(),
        release_publication: Notify::new(),
        published_or_finished: Notify::new(),
        published: AtomicBool::new(false),
        finished: AtomicBool::new(false),
        stopped: AtomicBool::new(false),
        panic_flush: AtomicBool::new(false),
    });
    let facts = Arc::new(Mutex::new(HostFacts::default()));
    let host = host_task(&directory.0, facts.clone(), Arc::new(Notify::new()));
    let mut owner = RunOwner::start(bootstrap(&directory.0), driver.clone(), native.clone());
    tokio::time::timeout(DEADLINE, async {
        match gate {
            StartupGate::BeforeAttachment => native.entered.notified().await,
            StartupGate::BeforePublication => driver.before_publication.notified().await,
            StartupGate::AfterPublication => native.facts.frame_seen.notified().await,
        }
    })
    .await
    .unwrap();
    let attachments = (!matches!(gate, StartupGate::BeforeAttachment)) as usize;
    let published = matches!(gate, StartupGate::AfterPublication);
    assert_eq!(native.attachments.load(Ordering::SeqCst), attachments);
    assert_eq!(driver.published.load(Ordering::SeqCst), published);
    assert!(!native.joined.load(Ordering::SeqCst));
    // Cancellation of the outer JOIN borrows every handle; the consumer and
    // gated command are still alive, with custody retained externally.
    let mut join = Box::pin(owner.join());
    assert!(futures_util::poll!(join.as_mut()).is_pending());
    drop(join);
    assert!(!driver.finished.load(Ordering::SeqCst));
    assert!(!native.joined.load(Ordering::SeqCst));
    if consumer_panic {
        driver.panic_flush.store(true, Ordering::SeqCst);
    } else {
        owner.control().abort_consumer();
    }
    let error = tokio::time::timeout(DEADLINE, owner.join())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(
        error.code,
        if consumer_panic {
            "MUSE_CONSUMER_TASK_FAILED"
        } else {
            "MUSE_CONSUMER_TASK_ABORTED"
        }
    );
    host.await.unwrap();
    // Release both gates again after final close. There is no surviving
    // command that can now publish a transport or submit a second input.
    native.release_start.notify_one();
    driver.release_publication.notify_one();
    tokio::task::yield_now().await;
    assert!(owner.commands.lock().await.is_empty());
    assert!(driver.finished.load(Ordering::SeqCst));
    assert!(driver.stopped.load(Ordering::SeqCst));
    assert_eq!(native.starts.load(Ordering::SeqCst), 1);
    assert_eq!(native.attachments.load(Ordering::SeqCst), attachments);
    assert_eq!(driver.published.load(Ordering::SeqCst), published);
    assert_eq!(native.facts.frames.lock().unwrap().len(), published as usize);
    assert_eq!(native.facts.eof.load(Ordering::SeqCst), attachments != 0);
    assert!(
        native.peer.lock().await.is_none(),
        "external pipe owner must be joined"
    );
    assert!(native.joined.load(Ordering::SeqCst));
    let facts = facts.lock().unwrap();
    assert_eq!(facts.auth, 1);
    assert_eq!(facts.hello, 1);
    assert_eq!(
        facts.outcomes, 0,
        "an aborted native call has no fabricated outcome"
    );
}

#[tokio::test]
async fn muse_runtime_startup_shutdown_before_attachment_retains_owner() {
    gated_startup_shutdown(StartupGate::BeforeAttachment, false).await;
}
#[tokio::test]
async fn muse_runtime_startup_shutdown_before_publication_closes_retained_pipe() {
    gated_startup_shutdown(StartupGate::BeforePublication, false).await;
}
#[tokio::test]
async fn muse_runtime_startup_shutdown_after_publication_keeps_written_input_unknown() {
    gated_startup_shutdown(StartupGate::AfterPublication, false).await;
}
#[tokio::test]
async fn muse_runtime_startup_shutdown_consumer_panic_preserves_failure_and_pipe_custody() {
    gated_startup_shutdown(StartupGate::BeforePublication, true).await;
}
