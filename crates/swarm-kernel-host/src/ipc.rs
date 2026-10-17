use crate::{
    config::Ipc,
    error::{Error, Result},
    model::{self, Credential, Request},
    platform,
    store::{DisconnectReason, Store},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

pub trait IoStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> IoStream for T {}
pub type Stream = Box<dyn IoStream>;
pub struct Listener {
    #[cfg(unix)]
    inner: tokio::net::UnixListener,
    #[cfg(windows)]
    inner: tokio::net::windows::named_pipe::NamedPipeServer,
    endpoint: String,
}
impl Listener {
    /// Call only while holding the data directory's singleton lock.
    pub fn bind(root: &Path) -> Result<Self> {
        let endpoint = platform::endpoint(root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileTypeExt;
            match std::fs::symlink_metadata(&endpoint) {
                Ok(m) if m.file_type().is_socket() => std::fs::remove_file(&endpoint)?,
                Ok(_) => {
                    return Err(Error::new(
                        "ENDPOINT_CONFLICT",
                        "refusing to replace a non-socket endpoint",
                    ));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            let inner = tokio::net::UnixListener::bind(&endpoint)?;
            platform::private_permissions(Path::new(&endpoint), false)?;
            Ok(Self { inner, endpoint })
        }
        #[cfg(windows)]
        {
            let inner = platform::windows::create_pipe(&endpoint, true)?;
            Ok(Self { inner, endpoint })
        }
    }
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    pub async fn accept(&mut self) -> Result<Stream> {
        #[cfg(unix)]
        {
            let (stream, _) = self.inner.accept().await?;
            Ok(Box::new(stream))
        }
        #[cfg(windows)]
        {
            self.inner.connect().await?;
            let next = platform::windows::create_pipe(&self.endpoint, false)?;
            let stream = std::mem::replace(&mut self.inner, next);
            Ok(Box::new(stream))
        }
    }
}
fn encode(value: &Value, limit: usize) -> Result<String> {
    let line = serde_json::to_string(value)?;
    if line.len() > limit {
        return Err(Error::new(
            "FRAME_TOO_LARGE",
            "use smaller pages or artifact ranges",
        ));
    }
    Ok(line)
}
fn decode(line: &str) -> Result<Request> {
    let r: Request = serde_json::from_str(line)?;
    r.validate()?;
    Ok(r)
}

pub async fn serve(
    stream: Stream,
    store: Store,
    config: Arc<Ipc>,
    stopping: watch::Receiver<bool>,
) -> Result<()> {
    serve_connection(stream, store, config, stopping).await
}

pub(crate) async fn serve_connection(
    stream: Stream,
    store: Store,
    config: Arc<Ipc>,
    mut stopping: watch::Receiver<bool>,
) -> Result<()> {
    let (read, write) = tokio::io::split(stream);
    let mut reader = FramedRead::new(
        read,
        LinesCodec::new_with_max_length(config.max_frame_bytes),
    );
    let mut writer = FramedWrite::new(
        write,
        LinesCodec::new_with_max_length(config.max_frame_bytes),
    );
    let first = tokio::select! {
        _=stopping.changed()=>return Ok(()),
        value=tokio::time::timeout(Duration::from_secs(10),reader.next())=>value.map_err(|_|Error::new("AUTH_TIMEOUT","hello not received"))?,
    };
    let first = first
        .ok_or_else(|| Error::new("DISCONNECTED", "client closed before hello"))?
        .map_err(|e| Error::new("PROTOCOL_ERROR", e.to_string()))?;
    let hello = decode(&first)?;
    if hello.method != "client.hello" {
        return Err(Error::new(
            "UNAUTHORIZED",
            "first request must be client.hello",
        ));
    }
    let credential: Credential = serde_json::from_value(hello.params)?;
    let principal = store.authenticate(credential).await;
    let reply = model::response(
        json!(hello.id),
        principal
            .as_ref()
            .map(|p| json!({"client_id":p.client_id,"role":p.role,"protocol_version":1}))
            .map_err(Clone::clone),
    );
    tokio::time::timeout(
        Duration::from_secs(config.write_timeout_seconds),
        writer.send(encode(&reply, config.max_frame_bytes)?),
    )
    .await
    .map_err(|_| Error::new("WRITE_TIMEOUT", "hello output stalled"))?
    .map_err(|e| Error::new("PROTOCOL_ERROR", e.to_string()))?;
    let principal = principal?;
    let (output, mut responses) = mpsc::channel::<String>(config.max_inflight_per_connection);
    let timeout = Duration::from_secs(config.write_timeout_seconds);
    let mut writer_task = tokio::spawn(async move {
        while let Some(line) = responses.recv().await {
            match tokio::time::timeout(timeout, writer.send(line)).await {
                Ok(Ok(())) => {}
                _ => break,
            }
        }
    });
    let mut requests = JoinSet::new();
    let disconnect_reason = loop {
        tokio::select! {
            biased;
            _=stopping.changed()=>break DisconnectReason::HostStopping,
            _=&mut writer_task=>break DisconnectReason::ResponseWriterStopped,
            Some(_)=requests.join_next(), if !requests.is_empty()=>{},
            incoming=reader.next(), if requests.len()<config.max_inflight_per_connection=>{
                let line=match incoming {
                    Some(Ok(line))=>line,
                    Some(Err(_))=>break DisconnectReason::ReadFailure,
                    None=>break DisconnectReason::PeerEof,
                };
                let store=store.clone();let principal=principal.clone();let output=output.clone();let limit=config.max_frame_bytes;
                requests.spawn(async move{
                    let (id,result)=match decode(&line){
                        Ok(r)=>{
                            let id=json!(r.id);
                            let method=r.method;
                            let params=r.params;
                            let result=store.call(principal,method.clone(),params.clone()).await;
                            (id,result)
                        },
                        Err(e)=>(Value::Null,Err(e)),
                    };
                    let frame=match encode(&model::response(id.clone(),result),limit){
                        Ok(frame)=>frame,
                        Err(e)=>match encode(&model::response(id,Err(e)),limit){Ok(f)=>f,Err(_)=>return},
                    };
                    let _=output.send(frame).await;
                });
            }
        }
    };
    // Admitted application work drains even when the client stops reading/disconnects.
    while requests.join_next().await.is_some() {}
    let disconnect_result = store
        .disconnected_with_reason(principal, disconnect_reason)
        .await;
    drop(output);
    if !writer_task.is_finished() {
        let _ = writer_task.await;
    }
    disconnect_result
}

/// Root compatibility wrapper around the independently buildable IPC client.
pub struct Client(swarm_client::Client);

impl Client {
    pub async fn connect(root: &Path, credential: &Credential, config: &Ipc) -> Result<Self> {
        swarm_client::Client::connect(root, credential, config)
            .await
            .map(Self)
            .map_err(Into::into)
    }

    pub async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        self.0.request(method, params).await.map_err(Into::into)
    }
}

/// One CLI exchange; never retries a mutation after a lost reply.
pub async fn call(
    root: &Path,
    credential: &Credential,
    method: &str,
    params: Value,
    config: &Ipc,
) -> Result<Value> {
    Client::connect(root, credential, config)
        .await?
        .request(method, params)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        platform::{DataRoot, bootstrap_credential},
        store::StoreOwner,
    };
    use std::{
        io,
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Poll},
    };
    use tokio::io::{AsyncWrite, DuplexStream, ReadBuf};

    const TEST_FRAME_LIMIT: usize = 4096;

    #[derive(Clone, Copy)]
    enum HandshakeReply {
        Unauthorized,
        Malformed,
        MismatchedId,
    }

    async fn assert_rejected_handshake(reply_kind: HandshakeReply, expected_code: &str) {
        let directory =
            std::env::temp_dir().join(format!("swarm-ipc-handshake-{}", model::new_id()));
        std::fs::create_dir_all(&directory).expect("create unique fake endpoint root");
        let mut listener = Listener::bind(&directory).expect("bind fake peer endpoint");
        let peer = tokio::spawn(async move {
            let stream = listener.accept().await.expect("accept Client connection");
            let (read, write) = tokio::io::split(stream);
            let mut reader =
                FramedRead::new(read, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT));
            let mut writer =
                FramedWrite::new(write, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT));
            let line = reader
                .next()
                .await
                .expect("Client hello arrives")
                .expect("hello frame is valid");
            let hello: Value = serde_json::from_str(&line).expect("hello JSON");
            assert_eq!(hello["method"], "client.hello");
            let reply = match reply_kind {
                HandshakeReply::Unauthorized => serde_json::to_string(&model::response(
                    hello["id"].clone(),
                    Err(Error::new(
                        "UNAUTHORIZED",
                        "fake peer denied the credential",
                    )),
                ))
                .unwrap(),
                HandshakeReply::Malformed => "not-json".to_owned(),
                HandshakeReply::MismatchedId => serde_json::to_string(&json!({
                    "jsonrpc":"2.0",
                    "id":"another-handshake",
                    "result":{"client_id":"fake-client"},
                }))
                .unwrap(),
            };
            writer
                .send(reply)
                .await
                .expect("write fake handshake reply");
            if let Ok(Some(Ok(line))) =
                tokio::time::timeout(Duration::from_secs(1), reader.next()).await
            {
                panic!("Client sent application frame after rejected hello: {line}")
            }
        });
        let credential = Credential {
            client_id: "fake-client".into(),
            token: "test-token-with-no-authority".into(),
        };
        let error = match Client::connect(&directory, &credential, &Ipc::default()).await {
            Ok(_) => panic!("rejected hello must not return a Client"),
            Err(error) => error,
        };
        assert_eq!(error.code, expected_code);
        peer.await.expect("fake peer joins");
        std::fs::remove_dir_all(directory).expect("remove unique endpoint root");
    }

    #[tokio::test]
    async fn rejected_or_malformed_handshake_never_sends_application_request() {
        assert_rejected_handshake(HandshakeReply::Unauthorized, "UNAUTHORIZED").await;
        assert_rejected_handshake(HandshakeReply::Malformed, "HOST_HANDSHAKE_FAILED").await;
        assert_rejected_handshake(HandshakeReply::MismatchedId, "HOST_HANDSHAKE_FAILED").await;
    }

    struct FailWrites {
        inner: DuplexStream,
        fail: Arc<AtomicBool>,
    }

    impl tokio::io::AsyncRead for FailWrites {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_read(cx, buffer)
        }
    }

    impl AsyncWrite for FailWrites {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.fail.load(Ordering::Acquire) {
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "scripted IPC writer failure",
                )))
            } else {
                Pin::new(&mut this.inner).poll_write(cx, buffer)
            }
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    #[tokio::test]
    async fn writer_failure_stops_admission_after_draining_accepted_mutation() {
        let directory = std::env::temp_dir().join(format!("swarm-ipc-writer-{}", model::new_id()));
        std::fs::create_dir_all(&directory).expect("create unique Store root");
        let root = DataRoot::acquire(&directory).expect("acquire test data root");
        let credential = bootstrap_credential(&root.path).expect("bootstrap local credential");
        let mut config = Config::default();
        config.storage.data_dir = root.path.clone();
        config.ipc.max_inflight_per_connection = 1;
        config.ipc.write_timeout_seconds = 1;
        let owner = StoreOwner::start(root, Arc::new(config.clone()), credential.clone())
            .await
            .expect("start real Store");
        let store = owner.store.clone();
        let inspection_principal = store
            .authenticate(credential.clone())
            .await
            .expect("authenticate inspection principal");
        let (server_io, client_io) = tokio::io::duplex(16 * 1024);
        let fail_writes = Arc::new(AtomicBool::new(false));
        let server: Stream = Box::new(FailWrites {
            inner: server_io,
            fail: fail_writes.clone(),
        });
        let (_stop, stop_rx) = watch::channel(false);
        let server_task = tokio::spawn(serve(
            server,
            store.clone(),
            Arc::new(config.ipc.clone()),
            stop_rx,
        ));
        let (read, write) = tokio::io::split(client_io);
        let mut reader = FramedRead::new(read, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT));
        let mut writer = FramedWrite::new(write, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT));
        let hello = json!({
            "jsonrpc":"2.0",
            "id":"hello",
            "method":"client.hello",
            "params":credential,
        });
        writer
            .send(serde_json::to_string(&hello).unwrap())
            .await
            .expect("send hello");
        let reply: Value = serde_json::from_str(
            &reader
                .next()
                .await
                .expect("hello response arrives")
                .expect("hello response frame valid"),
        )
        .expect("hello response JSON");
        assert_eq!(reply["result"]["client_id"], "operator");

        fail_writes.store(true, Ordering::Release);
        let create = json!({
            "jsonrpc":"2.0",
            "id":"accepted-create",
            "method":"task.create",
            "params":{
                "client_request_id":"ipc-writer-first-create",
                "project_id":"ipc-writer-test",
                "spec":{
                    "objective":"Drain one accepted request after the peer stops reading",
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Persist the accepted Task before the failed reply"}],
                },
            },
        });
        writer
            .send(serde_json::to_string(&create).unwrap())
            .await
            .expect("send one admitted mutation");
        tokio::time::timeout(Duration::from_secs(5), server_task)
            .await
            .expect("writer failure terminates connection service")
            .expect("serve task joins")
            .expect("serve drains accepted Store work");

        let tasks = store
            .call(
                inspection_principal.clone(),
                "task.list".into(),
                json!({"after":0,"limit":10}),
            )
            .await
            .expect("read persisted Store state");
        assert_eq!(tasks["items"].as_array().unwrap().len(), 1);
        assert_eq!(
            tasks["items"][0]["spec"]["objective"],
            "Drain one accepted request after the peer stops reading"
        );

        let later = json!({
            "jsonrpc":"2.0",
            "id":"must-not-admit",
            "method":"task.create",
            "params":{
                "client_request_id":"ipc-writer-second-create",
                "project_id":"ipc-writer-test",
                "spec":{
                    "objective":"Must remain unadmitted after writer failure",
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Do not create this Task"}],
                },
            },
        });
        assert!(
            writer
                .send(serde_json::to_string(&later).unwrap())
                .await
                .is_err(),
            "server must close its input after writer failure"
        );
        let tasks = store
            .call(
                inspection_principal,
                "task.list".into(),
                json!({"after":0,"limit":10}),
            )
            .await
            .expect("read Store state after rejected post-close write");
        assert_eq!(tasks["items"].as_array().unwrap().len(), 1);

        drop(writer);
        drop(reader);
        drop(store);
        owner.close().await.expect("close Store");
        std::fs::remove_dir_all(directory).expect("remove unique Store root");
    }

    #[tokio::test]
    async fn ipc_mux_real_authenticated_host_pending_next_allows_same_link_publish_and_outcome() {
        use swarm_contracts::native_stream::{
            StreamPayload, StreamPublish, StreamScope, TextClass,
        };
        let directory = std::env::temp_dir().join(format!("swarm-ipc-mux-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let root = DataRoot::acquire(&directory).unwrap();
        let operator = bootstrap_credential(&root.path).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = root.path.clone();
        config.ipc.max_inflight_per_connection = 8;
        let owner = StoreOwner::start(root, Arc::new(config.clone()), operator)
            .await
            .unwrap();
        let store = owner.store.clone();
        // Same retained synthetic facts used by the real stream boundary
        // fixtures: seed through Store's owner, never native/model dispatch.
        store.run_fixture(|db|{
            for (id,role) in [("mux-manager","manager"),("mux-module","module")] {
                let mut registration=json!({"role":role,"disabled":false,"token_hash":model::digest(format!("{id}-token").as_bytes())});
                if role=="module" {registration["binding_id"]=json!("mux-binding");registration["binding_generation"]=json!(1);}
                crate::store::set_meta(db,&format!("client:{id}"),&registration)?;
            }
            db.execute("INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('mux-task','mux-project',1,'open','{}',1,1)",[])?;
            db.execute("INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) VALUES('mux-binding',1,'mux-lane','mux-instance','mux-artifact','ready','mux-scope','mux-root','{\"module_artifact_id\":\"mux-artifact\",\"runtime\":\"module\"}',?1,1)",[json!({"module_client_id":"mux-module","bridge_boot_id":"mux-boot","connection":"connected"}).to_string()])?;
            db.execute("INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,created_at_ms,updated_at_ms) VALUES('mux-attempt','mux-task',1,'{\"revision\":1}','mux-manager','controller','mux-binding',1,'running',1,1)",[])?;
            db.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES('mux-dispatch','mux-manager','mux-request','task.dispatch','{}','{}','mux-task','mux-attempt','mux-binding',1,'settled',?1,?2,1,1,1,1)",rusqlite::params![json!({"session_id":"mux-root","turn_id":"mux-turn"}).to_string(),json!({"outcome":"applied","details":{"artifact_refs":[]}}).to_string()])?;
            db.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,binding_id,binding_generation,state,due_at_ms,created_at_ms,updated_at_ms) VALUES('mux-outcome','mux-manager','mux-outcome-request','agent.refresh','{}','{}','mux-binding',1,'sending',1,1,1)",[])?;
            Ok(())
        }).await.unwrap();
        let mut listener = Listener::bind(&directory).unwrap();
        let (stop, stopping) = watch::channel(false);
        let serving_store = store.clone();
        let ipc = Arc::new(config.ipc.clone());
        // Exactly one accepted/authenticated physical connection. No second
        // listener remains available to disguise cloning as another hello.
        let server = tokio::spawn(async move {
            let stream = listener.accept().await.unwrap();
            drop(listener);
            serve_connection(stream, serving_store, ipc, stopping).await
        });
        let credential = Credential {
            client_id: "mux-module".into(),
            token: "mux-module-token".into(),
        };
        let mut link = swarm_client::ModuleLink::connect(&directory, &credential, &config.ipc)
            .await
            .unwrap();
        assert_eq!(link.next().await.unwrap_err().code, "MODULE_HELLO_REQUIRED");
        let hello =
            json!({"boot_id":"mux-boot","module_artifact_id":"mux-artifact","native_ready":true});
        let original = link.hello(hello.clone(), None).await.unwrap();
        let current_link=store.run_fixture(|db|{let raw:String=db.query_row("SELECT state_json FROM bindings WHERE binding_id='mux-binding' AND generation=1",[],|r|r.get(0))?;Ok(serde_json::from_str::<Value>(&raw)?["module_link_id"].clone())}).await.unwrap();
        let mut repeat = link.clone();
        assert_eq!(repeat.hello(hello, None).await.unwrap(), original);
        assert_eq!(
            repeat
                .hello(
                    json!({"boot_id":"foreign","module_artifact_id":"mux-artifact"}),
                    None
                )
                .await
                .unwrap_err()
                .code,
            "MODULE_HELLO_CONFLICT"
        );
        let mut next_link = link.clone();
        let mut next = Box::pin(next_link.next());
        tokio::select! {reply=&mut next=>panic!("next must be pending without queued work: {reply:?}"),_=tokio::time::sleep(Duration::from_millis(50))=>{}}
        let mut publish = link.clone();
        let mut outcome = link.clone();
        let mut observe = link.clone();
        let mut result = link.clone();
        let frame = StreamPublish {
            scope: StreamScope {
                binding_id: "mux-binding".into(),
                generation: 1,
                boot_id: "mux-boot".into(),
                task_id: "mux-task".into(),
                task_revision: 1,
                attempt_id: "mux-attempt".into(),
                operation_id: "mux-dispatch".into(),
                native_session_id: "mux-root".into(),
                turn_id: "mux-turn".into(),
            },
            source_sequence: 1,
            native_cursor: "opaque:one".into(),
            item_id: "item".into(),
            item_order: 0,
            part_index: 0,
            payload: StreamPayload::Text {
                class: TextClass::Assistant,
                text: "synthetic complete text".into(),
                final_record: true,
                revision: Some(1),
            },
        };
        let before = store
            .run_fixture(|db| {
                Ok(db.query_row("SELECT COUNT(*) FROM observations", [], |r| {
                    r.get::<_, i64>(0)
                })?)
            })
            .await
            .unwrap();
        let publication =
            tokio::time::timeout(Duration::from_secs(2), publish.publish_stream(&frame))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(publication["published"], true);
        let after = store
            .run_fixture(|db| {
                Ok(db.query_row("SELECT COUNT(*) FROM observations", [], |r| {
                    r.get::<_, i64>(0)
                })?)
            })
            .await
            .unwrap();
        assert_eq!(before, after, "stream publication writes no Observation");
        let (recorded,observed,refused)=tokio::time::timeout(Duration::from_secs(2),async {
            tokio::join!(
                outcome.outcome(json!({"operation_id":"mux-outcome","outcome":"unknown","native_scope_key":"mux-scope","native_root_id":"mux-root","turn_id":null,"details":{"diagnostic_code":"FIXTURE_READ_UNAVAILABLE"}})),
                observe.observe(json!({"event_id":"mux-observation","sequence":1,"state":{"boot_id":"mux-boot","execution":"unknown","family_completeness":"partial"}})),
                result.result(json!({"operation_id":"not-an-admitted-result","page":{}}))
            )
        }).await.unwrap();
        assert_eq!(recorded.unwrap()["recorded"], true);
        assert!(observed.is_ok());
        assert_ne!(refused.unwrap_err().code, "OUTCOME_UNKNOWN");
        assert!(link.is_connected());
        tokio::select! {reply=&mut next=>panic!("side calls must not replace/cancel the retained next: {reply:?}"),_=tokio::time::sleep(Duration::from_millis(50))=>{}}
        let same_link=store.run_fixture(|db|{let raw:String=db.query_row("SELECT state_json FROM bindings WHERE binding_id='mux-binding' AND generation=1",[],|r|r.get(0))?;Ok(serde_json::from_str::<Value>(&raw)?["module_link_id"].clone())}).await.unwrap();
        assert_eq!(same_link, current_link);
        // Retain this original future until the real Host's ten-second empty
        // next completes; cancellation is not used to unlock another lane.
        let empty = tokio::time::timeout(Duration::from_secs(12), next.as_mut())
            .await
            .unwrap()
            .unwrap();
        assert!(empty["command"].is_null());
        drop(next);
        link.close().await;
        let _ = stop.send(true);
        server.await.unwrap().unwrap();
        drop(store);
        owner.close().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
