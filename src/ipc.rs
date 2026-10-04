use crate::{
    config::Ipc,
    error::{Error, Result},
    model::{self, Credential, Request},
    platform,
    store::Store,
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
async fn connect(root: &Path) -> Result<Stream> {
    let endpoint = platform::endpoint(root)?;
    #[cfg(unix)]
    {
        Ok(Box::new(tokio::net::UnixStream::connect(&endpoint).await?))
    }
    #[cfg(windows)]
    {
        use tokio::net::windows::named_pipe::ClientOptions;
        // Retrying connection establishment has no application/native side effects.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match ClientOptions::new().open(&endpoint) {
                Ok(stream) => return Ok(Box::new(stream)),
                Err(e)
                    if e.raw_os_error() == Some(231) && tokio::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(25)).await
                }
                Err(e) => return Err(e.into()),
            }
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
    loop {
        tokio::select! {
            biased;
            _=stopping.changed()=>break,
            _=&mut writer_task=>break,
            Some(_)=requests.join_next(), if !requests.is_empty()=>{},
            incoming=reader.next(), if requests.len()<config.max_inflight_per_connection=>{
                let line=match incoming{Some(Ok(line))=>line,_=>break};
                let store=store.clone();let principal=principal.clone();let output=output.clone();let limit=config.max_frame_bytes;
                requests.spawn(async move{
                    let (id,result)=match decode(&line){
                        Ok(r)=>{let id=json!(r.id);(id,store.call(principal,r.method,r.params).await)},
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
    }
    // Admitted application work drains even when the client stops reading/disconnects.
    while requests.join_next().await.is_some() {}
    store.disconnected(principal).await;
    drop(output);
    if !writer_task.is_finished() {
        let _ = writer_task.await;
    }
    Ok(())
}

/// Sequential local client. Reuses authentication/framing during a large export.
/// A failed transport is never reused or retried as if its outcome were known.
pub struct Client {
    reader: FramedRead<tokio::io::ReadHalf<Stream>, LinesCodec>,
    writer: FramedWrite<tokio::io::WriteHalf<Stream>, LinesCodec>,
    limit: usize,
    write_timeout: Duration,
    usable: bool,
}
impl Client {
    pub async fn connect(root: &Path, credential: &Credential, config: &Ipc) -> Result<Self> {
        let stream = connect(root).await.map_err(|error| {
            if error.code == "IO_ERROR" {
                Error::new("HOST_UNAVAILABLE", "controller IPC is unavailable; no application request was sent; start or reconnect the host")
            } else { error }
        })?;
        let (read, write) = tokio::io::split(stream);
        let mut client = Self {
            reader: FramedRead::new(
                read,
                LinesCodec::new_with_max_length(config.max_frame_bytes),
            ),
            writer: FramedWrite::new(
                write,
                LinesCodec::new_with_max_length(config.max_frame_bytes),
            ),
            limit: config.max_frame_bytes,
            write_timeout: Duration::from_secs(config.write_timeout_seconds),
            usable: true,
        };
        client.exchange("client.hello", json!(credential)).await.map_err(|error| {
            if matches!(error.code.as_str(), "OUTCOME_UNKNOWN" | "PROTOCOL_ERROR" | "INVALID_PARAMS") {
                Error::new("HOST_HANDSHAKE_FAILED", "controller authentication handshake did not complete; no application request was sent; reconnect before dispatch")
            } else { error }
        })?;
        Ok(client)
    }
    pub async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        if method == "client.hello" {
            return Err(Error::invalid("client.hello is reserved for the transport"));
        }
        self.exchange(method, params).await
    }
    async fn exchange(&mut self, method: &str, params: Value) -> Result<Value> {
        if !self.usable {
            return Err(Error::new(
                "DISCONNECTED",
                "client link failed; reconnect before another request",
            ));
        }
        let id = model::new_id();
        let line = encode(
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
            self.limit,
        )?;
        self.usable = false;
        tokio::time::timeout(self.write_timeout, self.writer.send(line))
            .await
            .map_err(|_| {
                Error::new(
                    "OUTCOME_UNKNOWN",
                    "write timed out; preserve the logical request ID",
                )
            })?
            .map_err(|e| Error::new("OUTCOME_UNKNOWN", e.to_string()))?;
        let frame = tokio::time::timeout(Duration::from_secs(60), self.reader.next())
            .await
            .map_err(|_| {
                Error::new(
                    "OUTCOME_UNKNOWN",
                    "reply missing; inspect the operation before retrying",
                )
            })?
            .ok_or_else(|| Error::new("OUTCOME_UNKNOWN", "server closed before reply"))?
            .map_err(|_| unknown_response())?;
        let response: Value = serde_json::from_str(&frame).map_err(|_| unknown_response())?;
        if response["id"] != id || response["jsonrpc"] != "2.0" {
            return Err(unknown_response());
        }
        if let Some(error) = response.get("error") {
            if response.get("result").is_some()
                || !error.is_object()
                || error["code"].as_i64().is_none()
                || error["message"].as_str().is_none()
                || error["data"]["code"].as_str().is_none_or(str::is_empty)
            {
                return Err(unknown_response());
            }
            self.usable = true;
            return Err(Error::new(
                error["data"]["code"].as_str().unwrap_or("RPC_ERROR"),
                error["message"].as_str().unwrap_or("RPC error"),
            ));
        }
        let result = response
            .get("result")
            .cloned()
            .ok_or_else(unknown_response)?;
        self.usable = true;
        Ok(result)
    }
}

fn unknown_response() -> Error {
    Error::new(
        "OUTCOME_UNKNOWN",
        "reply was malformed or could not be matched; preserve the logical request ID and inspect the original operation before retrying",
    )
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

    fn client_on_duplex(stream: DuplexStream) -> Client {
        let stream: Stream = Box::new(stream);
        let (read, write) = tokio::io::split(stream);
        Client {
            reader: FramedRead::new(read, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT)),
            writer: FramedWrite::new(write, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT)),
            limit: TEST_FRAME_LIMIT,
            write_timeout: Duration::from_secs(1),
            usable: true,
        }
    }

    fn fake_peer_client(
        reply: impl FnOnce(&Value) -> String + Send + 'static,
    ) -> (Client, tokio::task::JoinHandle<(Value, Vec<Value>)>) {
        let (client_stream, peer_stream) = tokio::io::duplex(16 * 1024);
        let client = client_on_duplex(client_stream);
        let (read, write) = tokio::io::split(peer_stream);
        let peer = tokio::spawn(async move {
            let mut reader =
                FramedRead::new(read, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT));
            let mut writer =
                FramedWrite::new(write, LinesCodec::new_with_max_length(TEST_FRAME_LIMIT));
            let line = reader
                .next()
                .await
                .expect("Client sends one request")
                .expect("request frame is valid");
            let request: Value = serde_json::from_str(&line).expect("request JSON");
            writer
                .send(reply(&request))
                .await
                .expect("write fake reply");
            let mut following = Vec::new();
            while let Some(line) = reader.next().await {
                let Ok(line) = line else { break };
                following.push(serde_json::from_str(&line).expect("following request JSON"));
            }
            (request, following)
        });
        (client, peer)
    }

    async fn assert_poisoned_client(
        reply: impl FnOnce(&Value) -> String + Send + 'static,
    ) -> Value {
        let (mut client, peer) = fake_peer_client(reply);
        let error = client
            .request(
                "task.create",
                json!({"client_request_id":"ipc-logical-request","project_id":"fixture","spec":{}}),
            )
            .await
            .expect_err("invalid post-send reply must not be treated as settled");
        assert_eq!(error.code, "OUTCOME_UNKNOWN");
        let error = client
            .request("task.create", json!({"client_request_id":"must-not-send"}))
            .await
            .expect_err("uncertain link must refuse another request");
        assert_eq!(error.code, "DISCONNECTED");
        drop(client);
        let (first, following) = peer.await.expect("fake peer joins");
        assert_eq!(first["method"], "task.create");
        assert!(following.is_empty(), "poisoned Client wrote another frame");
        first
    }

    #[tokio::test]
    async fn malformed_and_mismatched_post_send_replies_poison_the_client() {
        assert_poisoned_client(|_| "not-json".to_owned()).await;
        assert_poisoned_client(|request| {
            serde_json::to_string(&json!({
                "jsonrpc":"2.0",
                "id":format!("{}-other", request["id"].as_str().unwrap()),
                "result":{"accepted":true},
            }))
            .unwrap()
        })
        .await;
    }

    #[tokio::test]
    async fn nonnumeric_rpc_error_code_is_unknown_and_poisoning() {
        let first = assert_poisoned_client(|request| {
            serde_json::to_string(&json!({
                "jsonrpc":"2.0",
                "id":request["id"],
                "error":{"code":"-32000","message":"malformed peer error","data":{"code":"FORBIDDEN"}},
            }))
            .unwrap()
        })
        .await;
        assert_eq!(first["id"].as_str().unwrap().len(), 36);
    }

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
            match tokio::time::timeout(Duration::from_secs(1), reader.next()).await {
                Ok(Some(Ok(line))) => {
                    panic!("Client sent application frame after rejected hello: {line}")
                }
                _ => {}
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
}
