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
    let writer_task = tokio::spawn(async move {
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
            _=stopping.changed()=>break,
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
    let _ = writer_task.await;
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
        let (read, write) = tokio::io::split(connect(root).await?);
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
        client.exchange("client.hello", json!(credential)).await?;
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
            .map_err(|e| Error::new("PROTOCOL_ERROR", e.to_string()))?;
        let response: Value = serde_json::from_str(&frame)?;
        if response["id"] != id || response["jsonrpc"] != "2.0" {
            return Err(Error::new("PROTOCOL_ERROR", "response ID/version mismatch"));
        }
        if let Some(error) = response.get("error") {
            self.usable = true;
            return Err(Error::new(
                error["data"]["code"].as_str().unwrap_or("RPC_ERROR"),
                error["message"].as_str().unwrap_or("RPC error"),
            ));
        }
        let result = response
            .get("result")
            .cloned()
            .ok_or_else(|| Error::new("PROTOCOL_ERROR", "missing result"))?;
        self.usable = true;
        Ok(result)
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
