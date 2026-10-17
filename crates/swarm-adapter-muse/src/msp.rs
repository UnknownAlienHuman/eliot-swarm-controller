//! Bounded, bidirectional MSP framing over caller-owned stdio.
//!
//! RPC correlation and server method dispatch are library-owned. The small
//! envelope switch separates the two independent RPC ID spaces. Native command
//! IDs inside params, retries, durable state and native scheduling are outside
//! this transport. A successful submission means flushed bytes, not admission.

use bytes::BytesMut;
use futures_util::StreamExt;
use jsonrpsee_core::{
    client::{
        Client, ClientBuilder, ClientT, IdKind, ReceivedMessage, TransportReceiverT,
        TransportSenderT,
    },
    server::RpcModule,
};
use serde_json::{Map, Value, json};
use std::{
    collections::HashSet,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinSet,
};
use tokio_util::codec::{Decoder, FramedRead, LinesCodec, LinesCodecError};

/// The pinned official Connection's default bound, excluding the newline.
pub const MAX_FRAME_BYTES: usize = 10_485_760;

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub max_frame_bytes: usize,
    pub queued_submissions: usize,
    pub queued_writes: usize,
    pub queued_notifications: usize,
    pub max_in_flight: usize,
    pub max_server_requests: usize,
    pub request_timeout: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_frame_bytes: MAX_FRAME_BYTES,
            queued_submissions: 8,
            queued_writes: 8,
            queued_notifications: 64,
            max_in_flight: 8,
            max_server_requests: 8,
            request_timeout: Duration::from_secs(60),
        }
    }
}

impl Options {
    fn validate(self) -> Result<Self, TransportEnd> {
        if !(128..=MAX_FRAME_BYTES).contains(&self.max_frame_bytes)
            || [
                self.queued_submissions,
                self.queued_writes,
                self.max_in_flight,
                self.max_server_requests,
            ]
            .iter()
            .any(|n| !(1..=64).contains(n))
            || !(1..=256).contains(&self.queued_notifications)
            || self.request_timeout.is_zero()
        {
            return Err(TransportEnd::InvalidOptions);
        }
        Ok(self)
    }
}

/// Transport evidence only. No variant means that a native child exited or an
/// admitted native command did/did not execute. No raw IO/config text is kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransportEnd {
    LocalClosed,
    Eof,
    TruncatedEof,
    FrameTooLarge,
    InvalidUtf8,
    InvalidEnvelope,
    ResponseIdMismatch,
    UncorrelatedError,
    ReadFailed,
    WriteFailed,
    Backpressure,
    DuplicateServerId,
    UnexpectedClientFrame,
    ClientDisconnected,
    InvalidOptions,
    OwnerUnverified,
}

impl fmt::Display for TransportEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MSP_{self:?}")
    }
}
impl Error for TransportEnd {}
impl From<std::io::Error> for TransportEnd {
    fn from(_: std::io::Error) -> Self {
        Self::ReadFailed
    }
}

#[derive(Debug)]
pub enum RequestError {
    Transport(TransportEnd),
    Rpc(jsonrpsee_core::client::Error),
}
impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(end) => write!(f, "{end}"),
            Self::Rpc(_) => f.write_str("MSP_RPC_RESPONSE_ERROR"),
        }
    }
}
impl Error for RequestError {}

#[derive(Debug)]
pub struct SubmissionReceipt {
    pub request_id: Option<Value>,
    pub frame_bytes: usize,
}

pub type Submission = oneshot::Receiver<Result<SubmissionReceipt, TransportEnd>>;

/// Dropping either receiver does not unsend an already admitted frame.
pub struct PendingRequest {
    pub submitted: Submission,
    pub response: oneshot::Receiver<Result<Value, RequestError>>,
}

/// The complete bounded native notification, including optional emittedAtMs.
#[derive(Debug)]
pub struct Notification {
    pub envelope: Value,
}

struct Ticket {
    serial: u64,
    method: String,
    params: Map<String, Value>,
    request: bool,
    submitted: oneshot::Sender<Result<SubmissionReceipt, TransportEnd>>,
}

struct Shared {
    end: watch::Sender<Option<TransportEnd>>,
    ticket: Mutex<Option<Ticket>>,
    limit: usize,
}

impl Shared {
    fn ended(&self) -> Option<TransportEnd> {
        *self.end.borrow()
    }

    fn fail(&self, reason: TransportEnd) {
        self.end.send_if_modified(|end| {
            if end.is_some() {
                false
            } else {
                *end = Some(reason);
                true
            }
        });
        if let Some(ticket) = self.ticket.lock().expect("ticket lock").take() {
            let _ = ticket.submitted.send(Err(self.ended().unwrap_or(reason)));
        }
    }
}

async fn ended(end: &mut watch::Receiver<Option<TransportEnd>>) -> TransportEnd {
    loop {
        if let Some(reason) = *end.borrow() {
            return reason;
        }
        if end.changed().await.is_err() {
            return TransportEnd::LocalClosed;
        }
    }
}

enum Write {
    Frame(String, oneshot::Sender<Result<(), TransportEnd>>),
    Close,
}

async fn write_frame(
    writer: &mpsc::Sender<Write>,
    shared: &Shared,
    frame: String,
) -> Result<(), TransportEnd> {
    if frame.len() > shared.limit {
        return Err(TransportEnd::FrameTooLarge);
    }
    let mut end = shared.end.subscribe();
    if let Some(reason) = shared.ended() {
        return Err(reason);
    }
    let (sent, written) = oneshot::channel();
    tokio::select! {
        biased;
        reason = ended(&mut end) => return Err(reason),
        result = writer.send(Write::Frame(frame, sent)) => {
            result.map_err(|_| shared.ended().unwrap_or(TransportEnd::WriteFailed))?;
        }
    }
    tokio::select! {
        biased;
        result = written => result.unwrap_or(Err(TransportEnd::WriteFailed)),
        reason = ended(&mut end) => Err(reason),
    }
}

struct Sender {
    writer: mpsc::Sender<Write>,
    shared: Arc<Shared>,
}

impl TransportSenderT for Sender {
    type Error = TransportEnd;

    async fn send(&mut self, frame: String) -> Result<(), Self::Error> {
        let ticket = self.shared.ticket.lock().expect("ticket lock").take();
        let Some(ticket) = ticket else {
            self.shared.fail(TransportEnd::UnexpectedClientFrame);
            return Err(TransportEnd::UnexpectedClientFrame);
        };
        let value = serde_json::from_str::<Value>(&frame).ok();
        let matching = value.as_ref().is_some_and(|value| {
            value["method"] == ticket.method
                && value.get("params").and_then(Value::as_object) == Some(&ticket.params)
                && value.get("id").is_some() == ticket.request
        });
        let request_id = value.as_ref().and_then(|value| value.get("id").cloned());
        let frame_bytes = frame.len();
        let result = if matching {
            write_frame(&self.writer, &self.shared, frame).await
        } else {
            Err(TransportEnd::UnexpectedClientFrame)
        };
        let receipt = result.map(|()| SubmissionReceipt {
            request_id,
            frame_bytes,
        });
        let _ = ticket.submitted.send(receipt);
        if let Err(reason) = result {
            self.shared.fail(reason);
        }
        result
    }
}

struct Receiver {
    responses: mpsc::Receiver<ReceivedMessage>,
    end: watch::Receiver<Option<TransportEnd>>,
}

impl TransportReceiverT for Receiver {
    type Error = TransportEnd;

    async fn receive(&mut self) -> Result<ReceivedMessage, Self::Error> {
        tokio::select! {
            biased;
            response = self.responses.recv() => response.ok_or_else(|| {
                self.end.borrow().unwrap_or(TransportEnd::Eof)
            }),
            reason = ended(&mut self.end) => Err(reason),
        }
    }
}

struct StrictLines(LinesCodec);
impl Decoder for StrictLines {
    type Item = String;
    type Error = TransportEnd;

    fn decode(&mut self, bytes: &mut BytesMut) -> Result<Option<String>, Self::Error> {
        self.0.decode(bytes).map_err(|error| match error {
            LinesCodecError::MaxLineLengthExceeded => TransportEnd::FrameTooLarge,
            LinesCodecError::Io(error) if error.kind() == std::io::ErrorKind::InvalidData => {
                TransportEnd::InvalidUtf8
            }
            LinesCodecError::Io(_) => TransportEnd::ReadFailed,
        })
    }

    fn decode_eof(&mut self, bytes: &mut BytesMut) -> Result<Option<String>, Self::Error> {
        // LinesCodec alone accepts a final unterminated line. MSP never promotes
        // such bytes to a response, even when they happen to parse as JSON.
        match self.decode(bytes)? {
            Some(line) => Ok(Some(line)),
            None if bytes.is_empty() => Ok(None),
            None => Err(TransportEnd::TruncatedEof),
        }
    }
}

fn id_key(id: &Value) -> Option<String> {
    match id {
        Value::String(value) => Some(format!("s:{value}")),
        Value::Number(value) => {
            if let Some(n) = value.as_i64() {
                return Some(format!("n:{n}"));
            }
            if let Some(n) = value.as_u64() {
                return Some(format!("n:{n}"));
            }
            let n = value.as_f64()?;
            (n.is_finite() && n.fract() == 0.0).then(|| {
                if n == 0.0 {
                    "n:0".to_owned()
                } else {
                    format!("n:{n:.0}")
                }
            })
        }
        _ => None,
    }
}

enum Envelope {
    Response,
    Notification,
    Request { id: Value, key: String },
}

fn envelope(value: &Value) -> Result<Envelope, TransportEnd> {
    let object = value.as_object().ok_or(TransportEnd::InvalidEnvelope)?;
    if object.get("jsonrpc") != Some(&json!("2.0")) {
        return Err(TransportEnd::InvalidEnvelope);
    }
    if let Some(method) = object.get("method") {
        if method.as_str().is_none_or(str::is_empty)
            || object.contains_key("result")
            || object.contains_key("error")
            || object.get("params").is_some_and(|p| !p.is_object())
        {
            return Err(TransportEnd::InvalidEnvelope);
        }
        return match object.get("id") {
            None => Ok(Envelope::Notification),
            Some(id) => Ok(Envelope::Request {
                id: id.clone(),
                key: id_key(id).ok_or(TransportEnd::InvalidEnvelope)?,
            }),
        };
    }
    if object.get("id").and_then(id_key).is_none()
        || object.contains_key("result") == object.contains_key("error")
    {
        if object.get("id") == Some(&Value::Null)
            && !object.contains_key("result")
            && object.get("error").is_some_and(|error| {
                error.get("code").and_then(Value::as_i64).is_some()
                    && error.get("message").and_then(Value::as_str).is_some()
            })
        {
            return Err(TransportEnd::UncorrelatedError);
        }
        return Err(TransportEnd::InvalidEnvelope);
    }
    Ok(Envelope::Response)
}

fn client_response(mut value: Value) -> Result<String, TransportEnd> {
    // jsonrpsee preserves typed IDs on single requests (its batch normalization
    // is not exposed here). Our client is explicitly numeric: reject unsolicited
    // string IDs before library routing. JSON spellings 0 and 0.0 are still the
    // same numeric ID, unlike the string "0".
    let Value::Number(number) = &value["id"] else {
        return Err(TransportEnd::ResponseIdMismatch);
    };
    let id = number
        .as_u64()
        .or_else(|| {
            let n = number.as_f64()?;
            ((0.0..u64::MAX as f64).contains(&n) && n.fract() == 0.0).then_some(n as u64)
        })
        .ok_or(TransportEnd::ResponseIdMismatch)?;
    value["id"] = json!(id);
    Ok(value.to_string())
}

struct ServerId {
    key: String,
    active: Arc<Mutex<HashSet<String>>>,
}
impl Drop for ServerId {
    fn drop(&mut self) {
        self.active
            .lock()
            .expect("server IDs lock")
            .remove(&self.key);
    }
}

async fn server_frame(
    writer: &mpsc::Sender<Write>,
    shared: &Shared,
    frame: String,
    guard: ServerId,
    slot: OwnedSemaphorePermit,
) -> Result<(), TransportEnd> {
    if frame.len() > shared.limit {
        return Err(TransportEnd::FrameTooLarge);
    }
    let mut end = shared.end.subscribe();
    let reserved = tokio::select! {
        biased;
        reason = ended(&mut end) => return Err(reason),
        reserved = writer.reserve() => reserved.map_err(|_| TransportEnd::WriteFailed)?,
    };
    // Release the completed handler's ID before its response can become visible
    // to the peer, which may immediately reuse that ID. Reserving output first
    // keeps waiting handlers bounded. The sole writer records subsequent errors.
    drop(guard);
    drop(slot);
    let (completion, _) = oneshot::channel();
    reserved.send(Write::Frame(frame, completion));
    Ok(())
}

async fn reader<R: AsyncRead + Unpin>(
    input: R,
    shared: Arc<Shared>,
    server: Arc<RpcModule<()>>,
    writer: mpsc::Sender<Write>,
    responses: mpsc::Sender<ReceivedMessage>,
    notifications: mpsc::Sender<Notification>,
    max_server: usize,
) {
    let mut frames = FramedRead::new(
        input,
        StrictLines(LinesCodec::new_with_max_length(shared.limit)),
    );
    let permits = Arc::new(Semaphore::new(max_server));
    let active = Arc::new(Mutex::new(HashSet::new()));
    let mut handlers = JoinSet::new();
    let mut end = shared.end.subscribe();
    loop {
        let next = tokio::select! {
            biased;
            _ = ended(&mut end) => break,
            result = handlers.join_next(), if !handlers.is_empty() => {
                if result.is_some_and(|result| result.is_err()) {
                    shared.fail(TransportEnd::InvalidEnvelope);
                }
                continue;
            }
            frame = frames.next() => frame,
        };
        let frame = match next {
            Some(Ok(frame)) => frame,
            Some(Err(reason)) => {
                shared.fail(reason);
                break;
            }
            None => {
                shared.fail(TransportEnd::Eof);
                break;
            }
        };
        let value = match serde_json::from_str::<Value>(&frame) {
            Ok(value) => value,
            Err(_) => {
                shared.fail(TransportEnd::InvalidEnvelope);
                break;
            }
        };
        match envelope(&value) {
            Ok(Envelope::Response) => {
                let frame = match client_response(value) {
                    Ok(frame) => frame,
                    Err(reason) => {
                        shared.fail(reason);
                        break;
                    }
                };
                if responses.try_send(ReceivedMessage::Text(frame)).is_err() {
                    shared.fail(TransportEnd::Backpressure);
                    break;
                }
            }
            Ok(Envelope::Notification) => {
                if notifications
                    .try_send(Notification { envelope: value })
                    .is_err()
                {
                    shared.fail(TransportEnd::Backpressure);
                    break;
                }
            }
            Ok(Envelope::Request { id, key }) => {
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    shared.fail(TransportEnd::Backpressure);
                    break;
                };
                if !active.lock().expect("server IDs lock").insert(key.clone()) {
                    shared.fail(TransportEnd::DuplicateServerId);
                    break;
                }
                let guard = ServerId {
                    key,
                    active: active.clone(),
                };
                let shared = shared.clone();
                let server = server.clone();
                let writer = writer.clone();
                handlers.spawn(async move {
                    // jsonrpsee uses unsigned numeric IDs. This stateless call
                    // translation also preserves signed native IDs and keeps
                    // the native server ID space separate from client IDs.
                    let mut request = value;
                    request["id"] = json!(0);
                    let result = server.raw_json_request(&request.to_string(), 1).await;
                    let result = match result {
                        Ok((response, _)) => serde_json::from_str::<Value>(response.get()),
                        Err(error) => Err(error),
                    };
                    let Ok(mut response) = result else {
                        shared.fail(TransportEnd::InvalidEnvelope);
                        return;
                    };
                    if response["id"] != 0 || response["jsonrpc"] != "2.0" {
                        shared.fail(TransportEnd::InvalidEnvelope);
                        return;
                    }
                    response["id"] = id;
                    if let Err(reason) =
                        server_frame(&writer, &shared, response.to_string(), guard, permit).await
                    {
                        shared.fail(reason);
                    }
                });
            }
            Err(reason) => {
                shared.fail(reason);
                break;
            }
        }
    }
    handlers.abort_all();
}

async fn writer<W: AsyncWrite + Unpin>(
    mut output: W,
    mut writes: mpsc::Receiver<Write>,
    shared: Arc<Shared>,
) {
    let mut end = shared.end.subscribe();
    loop {
        let write = tokio::select! {
            biased;
            _ = ended(&mut end) => break,
            write = writes.recv() => write,
        };
        match write {
            Some(Write::Frame(frame, completion)) => {
                let result = tokio::select! {
                    biased;
                    reason = ended(&mut end) => Err(reason),
                    result = async {
                        output.write_all(frame.as_bytes()).await?;
                        output.write_all(b"\n").await?;
                        output.flush().await
                    } => result.map_err(|_| TransportEnd::WriteFailed),
                };
                let _ = completion.send(result);
                if let Err(reason) = result {
                    shared.fail(reason);
                    break;
                }
            }
            Some(Write::Close) | None => {
                let result = output.shutdown().await;
                shared.fail(if result.is_ok() {
                    TransportEnd::LocalClosed
                } else {
                    TransportEnd::WriteFailed
                });
                break;
            }
        }
    }
}

struct Call {
    ticket: Ticket,
    response: Option<oneshot::Sender<Result<Value, RequestError>>>,
    _admission: OwnedSemaphorePermit,
}
enum Command {
    Call(Call),
    Close,
}

async fn coordinator(
    mut commands: mpsc::Receiver<Command>,
    client: Arc<Client>,
    shared: Arc<Shared>,
    writer: mpsc::Sender<Write>,
    in_flight: usize,
) {
    let pending = Arc::new(Semaphore::new(in_flight));
    let mut end = shared.end.subscribe();
    let mut serial = 0_u64;
    loop {
        let command = tokio::select! {
            biased;
            _ = ended(&mut end) => break,
            command = commands.recv() => command,
        };
        let Some(Command::Call(call)) = command else {
            if writer.send(Write::Close).await.is_err() {
                shared.fail(TransportEnd::WriteFailed);
            }
            break;
        };
        let Ok(permit) = pending.clone().try_acquire_owned() else {
            let _ = call.ticket.submitted.send(Err(TransportEnd::Backpressure));
            if let Some(response) = call.response {
                let _ = response.send(Err(RequestError::Transport(TransportEnd::Backpressure)));
            }
            continue;
        };
        let Ticket {
            method,
            params,
            request,
            submitted,
            ..
        } = call.ticket;
        let Some(next_serial) = serial.checked_add(1) else {
            shared.fail(TransportEnd::UnexpectedClientFrame);
            break;
        };
        serial = next_serial;
        // Wait for the real transport write, not the library's frontend enqueue.
        let (written, receipt) = oneshot::channel();
        *shared.ticket.lock().expect("ticket lock") = Some(Ticket {
            serial,
            method: method.clone(),
            params: params.clone(),
            request,
            submitted: written,
        });
        let task_client = client.clone();
        let task_shared = shared.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Some(response) = call.response {
                let result = task_client.request::<Value, _>(&method, params).await;
                let not_submitted = task_shared
                    .ticket
                    .lock()
                    .expect("ticket lock")
                    .as_ref()
                    .is_some_and(|ticket| ticket.serial == serial);
                if not_submitted {
                    task_shared.fail(TransportEnd::ClientDisconnected);
                }
                let _ = response.send(result.map_err(RequestError::Rpc));
            } else if task_client.notification(&method, params).await.is_err() {
                task_shared.fail(TransportEnd::ClientDisconnected);
            }
        });
        let result = receipt
            .await
            .unwrap_or(Err(TransportEnd::ClientDisconnected));
        let failed = result.is_err();
        let _ = submitted.send(result);
        drop(call._admission);
        if failed {
            shared.fail(TransportEnd::ClientDisconnected);
            break;
        }
    }
    // Queued callers receive an explicit transport failure, not a fabricated ACK.
    let reason = shared.ended().unwrap_or(TransportEnd::LocalClosed);
    commands.close();
    while let Some(command) = commands.recv().await {
        if let Command::Call(call) = command {
            let _ = call.ticket.submitted.send(Err(reason));
            if let Some(response) = call.response {
                let _ = response.send(Err(RequestError::Transport(reason)));
            }
        }
    }
}

struct Ingress {
    commands: mpsc::Sender<Command>,
    closing: bool,
}

pub struct MspTransport {
    ingress: Mutex<Ingress>,
    slots: Arc<Semaphore>,
    shared: Arc<Shared>,
}

impl MspTransport {
    /// Attach pipes of a child already launched by the verified module adapter.
    /// The existing owner retains process/Job lifetime; no ownership is created.
    pub fn attach_owned_stdio<R, W>(
        input: R,
        output: W,
        server: RpcModule<()>,
        options: Options,
    ) -> Result<(Self, mpsc::Receiver<Notification>), TransportEnd>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        swarm_process::module_owner::verify_current_adapter_from_env()
            .map_err(|_| TransportEnd::OwnerUnverified)?;
        Self::attach(input, output, server, options)
    }

    fn attach<R, W>(
        input: R,
        output: W,
        server: RpcModule<()>,
        options: Options,
    ) -> Result<(Self, mpsc::Receiver<Notification>), TransportEnd>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let options = options.validate()?;
        let (end, _) = watch::channel(None);
        let shared = Arc::new(Shared {
            end,
            ticket: Mutex::new(None),
            limit: options.max_frame_bytes,
        });
        let (writes, write_rx) = mpsc::channel(options.queued_writes);
        let (responses, response_rx) = mpsc::channel(options.max_in_flight);
        let (notifications, notification_rx) = mpsc::channel(options.queued_notifications);
        // A reserved physical slot makes close queueable even at admission bound.
        let (commands, command_rx) = mpsc::channel(options.queued_submissions + 1);
        let client = Arc::new(
            ClientBuilder::default()
                .id_format(IdKind::Number)
                .request_timeout(options.request_timeout)
                .max_concurrent_requests(options.max_in_flight)
                .max_buffer_capacity_per_subscription(1)
                .build_with_tokio(
                    Sender {
                        writer: writes.clone(),
                        shared: shared.clone(),
                    },
                    Receiver {
                        responses: response_rx,
                        end: shared.end.subscribe(),
                    },
                ),
        );
        tokio::spawn(writer(output, write_rx, shared.clone()));
        tokio::spawn(reader(
            input,
            shared.clone(),
            Arc::new(server),
            writes.clone(),
            responses,
            notifications,
            options.max_server_requests,
        ));
        tokio::spawn(coordinator(
            command_rx,
            client.clone(),
            shared.clone(),
            writes,
            options.max_in_flight,
        ));
        let monitor = shared.clone();
        tokio::spawn(async move {
            client.on_disconnect().await;
            monitor.fail(TransportEnd::ClientDisconnected);
        });
        Ok((
            Self {
                ingress: Mutex::new(Ingress {
                    commands,
                    closing: false,
                }),
                slots: Arc::new(Semaphore::new(options.queued_submissions)),
                shared,
            },
            notification_rx,
        ))
    }

    /// Admission is bounded and synchronous. No automatic retry, including after
    /// receipt cancellation. The caller must retain native intent before calling.
    pub fn submit_request(
        &self,
        method: &str,
        params: Map<String, Value>,
    ) -> Result<PendingRequest, TransportEnd> {
        let (response, receiver) = oneshot::channel();
        let submitted = self.enqueue(method, params, Some(response))?;
        Ok(PendingRequest {
            submitted,
            response: receiver,
        })
    }

    pub fn notify(
        &self,
        method: &str,
        params: Map<String, Value>,
    ) -> Result<Submission, TransportEnd> {
        self.enqueue(method, params, None)
    }

    fn enqueue(
        &self,
        method: &str,
        params: Map<String, Value>,
        response: Option<oneshot::Sender<Result<Value, RequestError>>>,
    ) -> Result<Submission, TransportEnd> {
        if method.is_empty() {
            return Err(TransportEnd::InvalidEnvelope);
        }
        // Includes method, envelope and the largest library-generated numeric ID.
        let size = json!({"jsonrpc":"2.0","method":method,"params":&params,"id":u64::MAX})
            .to_string()
            .len();
        if size > self.shared.limit {
            return Err(TransportEnd::FrameTooLarge);
        }
        let ingress = self.ingress.lock().expect("ingress lock");
        if let Some(reason) = self.shared.ended() {
            return Err(reason);
        }
        if ingress.closing {
            return Err(TransportEnd::LocalClosed);
        }
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| TransportEnd::Backpressure)?;
        let (submitted, receiver) = oneshot::channel();
        ingress
            .commands
            .try_send(Command::Call(Call {
                ticket: Ticket {
                    serial: 0,
                    method: method.to_owned(),
                    params,
                    request: response.is_some(),
                    submitted,
                },
                response,
                _admission: permit,
            }))
            .map_err(|_| self.shared.ended().unwrap_or(TransportEnd::Backpressure))?;
        Ok(receiver)
    }

    fn begin_close(&self) {
        let mut ingress = self.ingress.lock().expect("ingress lock");
        if !ingress.closing {
            ingress.closing = true;
            if ingress.commands.try_send(Command::Close).is_err() && self.shared.ended().is_none() {
                self.shared.fail(TransportEnd::WriteFailed);
            }
        }
    }

    /// Fences new submissions, drains accepted frames, then closes stdin. This
    /// may wait for pipe backpressure; it never times out into a kill or replay.
    /// Pending native replies may remain unknown. The owner still waits for the
    /// real process and its descendants separately.
    pub async fn close(&self) -> Result<(), TransportEnd> {
        self.begin_close();
        match self.wait_ended().await {
            TransportEnd::LocalClosed => Ok(()),
            reason => Err(reason),
        }
    }

    pub async fn wait_ended(&self) -> TransportEnd {
        ended(&mut self.shared.end.subscribe()).await
    }
}

impl Drop for MspTransport {
    fn drop(&mut self) {
        self.begin_close();
    }
}

#[cfg(test)]
mod tests {
    // Ported boundary vectors, not copied SDK implementation:
    // muse-code-sdk@a7c10c5d clients/sdk-ts/test/connection.test.ts:
    // "requests are newline-framed ... typed id" (69), INV-012 (89),
    // independent server ID space/notifications (123), oversized input (321),
    // duplicate ID (337), non-integer ID (348), write failure (356).
    // Malformed/oversized frames deliberately close this transport rather than
    // inherit the SDK's recover-and-continue policy. Command retry/facade vectors
    // are NOT claimed here. Extra vectors cover owned-pipe shutdown ordering.
    use super::*;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader, DuplexStream, ReadHalf, WriteHalf};
    use tokio::sync::Notify;
    use tokio::time::timeout;

    const DEADLINE: Duration = Duration::from_secs(2);
    type PeerRead = BufReader<ReadHalf<DuplexStream>>;
    type PeerWrite = WriteHalf<DuplexStream>;

    fn fixture(
        options: Options,
        server: RpcModule<()>,
        pipe_bytes: usize,
    ) -> (
        MspTransport,
        mpsc::Receiver<Notification>,
        PeerRead,
        PeerWrite,
    ) {
        let (owned, peer) = tokio::io::duplex(pipe_bytes);
        let (input, output) = tokio::io::split(owned);
        let (transport, events) = MspTransport::attach(input, output, server, options).unwrap();
        let (input, output) = tokio::io::split(peer);
        (transport, events, BufReader::new(input), output)
    }

    fn params(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    async fn read_frame(peer: &mut PeerRead) -> Value {
        let mut line = String::new();
        assert!(
            timeout(DEADLINE, peer.read_line(&mut line))
                .await
                .unwrap()
                .unwrap()
                > 0
        );
        assert!(line.ends_with('\n') && !line.ends_with("\r\n"));
        serde_json::from_str(&line).unwrap()
    }

    async fn send(peer: &mut PeerWrite, value: Value) {
        let line = format!("{value}\n");
        timeout(DEADLINE, peer.write_all(line.as_bytes()))
            .await
            .unwrap()
            .unwrap();
    }

    async fn end(transport: &MspTransport) -> TransportEnd {
        timeout(DEADLINE, transport.wait_ended()).await.unwrap()
    }

    #[tokio::test]
    async fn msp_transport_out_of_order_crlf_and_split_utf8() {
        let (transport, _events, mut input, mut output) =
            fixture(Options::default(), RpcModule::new(()), 16);
        let first = transport
            .submit_request("first/method", params(json!({"n":1})))
            .unwrap();
        let second = transport
            .submit_request("second/method", params(json!({"n":2})))
            .unwrap();
        let a = read_frame(&mut input).await;
        let b = read_frame(&mut input).await;
        assert_ne!(a["id"], b["id"]);
        assert_eq!(a["params"], json!({"n":1}));
        assert_eq!(b["params"], json!({"n":2}));
        let response = format!(
            "{}\r\n",
            json!({"jsonrpc":"2.0","id":b["id"],"result":{"text":"é😀"}})
        );
        let split = response.find('é').unwrap() + 1;
        output
            .write_all(&response.as_bytes()[..split])
            .await
            .unwrap();
        tokio::task::yield_now().await;
        output
            .write_all(&response.as_bytes()[split..])
            .await
            .unwrap();
        send(
            &mut output,
            json!({"jsonrpc":"2.0","id":a["id"],"result":{"one":true}}),
        )
        .await;
        let a_receipt = first.submitted.await.unwrap().unwrap();
        let b_receipt = second.submitted.await.unwrap().unwrap();
        assert_eq!(a_receipt.request_id, Some(a["id"].clone()));
        assert_eq!(b_receipt.request_id, Some(b["id"].clone()));
        assert_eq!(
            second.response.await.unwrap().unwrap(),
            json!({"text":"é😀"})
        );
        assert_eq!(first.response.await.unwrap().unwrap(), json!({"one":true}));
        assert_eq!(transport.close().await, Ok(()));
    }

    #[tokio::test]
    async fn msp_transport_server_id_spaces_ack_identity_and_notification() {
        let mut router = RpcModule::new(());
        router
            .register_method("approval/request", |_, _, _| {
                jsonrpsee_core::RpcResult::<Value>::Ok(json!({}))
            })
            .unwrap();
        let (transport, mut events, mut input, mut output) =
            fixture(Options::default(), router, 256);
        let pending = transport
            .submit_request("session/read", params(json!({"sessionId":"root"})))
            .unwrap();
        let sent = read_frame(&mut input).await;
        for id in [
            sent["id"].clone(),
            sent["id"].clone(),
            json!(sent["id"].to_string()),
            json!(-41),
        ] {
            send(&mut output, json!({"jsonrpc":"2.0","id":id,"method":"approval/request","params":{"approvalId":"a"}})).await;
            let ack = read_frame(&mut input).await;
            assert_eq!(ack, json!({"jsonrpc":"2.0","id":id,"result":{}}));
        }
        send(
            &mut output,
            json!({"jsonrpc":"2.0","method":"item/delta","params":{"delta":"x"},"emittedAtMs":1}),
        )
        .await;
        let notification = timeout(DEADLINE, events.recv()).await.unwrap().unwrap();
        assert_eq!(notification.envelope["emittedAtMs"], 1);
        assert_eq!(notification.envelope["params"]["delta"], "x");
        send(
            &mut output,
            json!({"jsonrpc":"2.0","id":sent["id"],"result":{"sessionId":"root"}}),
        )
        .await;
        pending.submitted.await.unwrap().unwrap();
        assert_eq!(
            pending.response.await.unwrap().unwrap()["sessionId"],
            "root"
        );
        transport.close().await.unwrap();
    }

    #[tokio::test]
    async fn msp_transport_unknown_server_method_is_correlated_library_error() {
        let (transport, _events, mut input, mut output) =
            fixture(Options::default(), RpcModule::new(()), 256);
        send(
            &mut output,
            json!({"jsonrpc":"2.0","id":"native-only","method":"future/unsupported","params":{}}),
        )
        .await;
        let response = read_frame(&mut input).await;
        assert_eq!(response["id"], "native-only");
        assert_eq!(response["error"]["code"], -32601);
        assert!(response.get("result").is_none());
        transport.close().await.unwrap();
    }

    #[tokio::test]
    async fn msp_transport_string_response_id_cannot_alias_numeric_request() {
        let (transport, _events, mut input, mut output) =
            fixture(Options::default(), RpcModule::new(()), 256);
        let pending = transport
            .submit_request("session/read", Map::new())
            .unwrap();
        let sent = read_frame(&mut input).await;
        assert!(sent["id"].is_number());
        send(
            &mut output,
            json!({"jsonrpc":"2.0","id":sent["id"].to_string(),"result":{"wrong":true}}),
        )
        .await;
        assert_eq!(end(&transport).await, TransportEnd::ResponseIdMismatch);
        assert!(
            timeout(DEADLINE, pending.response)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
    }

    #[tokio::test]
    async fn msp_transport_integer_json_number_spellings_keep_numeric_identity() {
        let (transport, _events, mut input, mut output) =
            fixture(Options::default(), RpcModule::new(()), 256);
        let pending = transport
            .submit_request("session/read", Map::new())
            .unwrap();
        let sent = read_frame(&mut input).await;
        let response = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{}.0,\"result\":{{\"exact\":true}}}}\n",
            sent["id"]
        );
        output.write_all(response.as_bytes()).await.unwrap();
        assert_eq!(
            timeout(DEADLINE, pending.response)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            json!({"exact":true})
        );
        transport.close().await.unwrap();
    }

    #[tokio::test]
    async fn msp_transport_null_id_error_is_uncorrelated_not_a_command_rejection() {
        let (transport, _events, _input, mut output) =
            fixture(Options::default(), RpcModule::new(()), 256);
        send(
            &mut output,
            json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}),
        )
        .await;
        assert_eq!(end(&transport).await, TransportEnd::UncorrelatedError);
    }

    #[tokio::test]
    async fn msp_transport_preserves_error_data_without_native_retry() {
        let (transport, _events, mut input, mut output) =
            fixture(Options::default(), RpcModule::new(()), 256);
        let pending = transport
            .submit_request(
                "turn/start",
                params(json!({"commandId":"retained","input":[]})),
            )
            .unwrap();
        let sent = read_frame(&mut input).await;
        let data = json!({"kind":"backpressured","retryable":true,"nativeField":"retained"});
        send(&mut output, json!({"jsonrpc":"2.0","id":sent["id"],"error":{"code":-32031,"message":"wording is not API","data":data}})).await;
        pending.submitted.await.unwrap().unwrap();
        match pending.response.await.unwrap() {
            Err(RequestError::Rpc(jsonrpsee_core::client::Error::Call(error))) => {
                assert_eq!(error.code(), -32031);
                assert_eq!(
                    serde_json::from_str::<Value>(error.data().unwrap().get()).unwrap(),
                    data
                );
            }
            other => panic!("expected exact native RPC error: {other:?}"),
        }
        transport.begin_close();
        let mut trailing = Vec::new();
        timeout(DEADLINE, input.read_to_end(&mut trailing))
            .await
            .unwrap()
            .unwrap();
        assert!(
            trailing.is_empty(),
            "no automatic command retry after native error"
        );
        assert_eq!(end(&transport).await, TransportEnd::LocalClosed);
    }

    fn slow_router(gate: Arc<Notify>) -> RpcModule<()> {
        let mut router = RpcModule::new(());
        router
            .register_async_method("slow/request", move |_, _, _| {
                let gate = gate.clone();
                async move {
                    gate.notified().await;
                    jsonrpsee_core::RpcResult::<Value>::Ok(json!({}))
                }
            })
            .unwrap();
        router
    }

    #[tokio::test]
    async fn msp_transport_pending_server_request_does_not_block_client_response() {
        let gate = Arc::new(Notify::new());
        let (transport, _events, mut input, mut output) =
            fixture(Options::default(), slow_router(gate.clone()), 256);
        let pending = transport
            .submit_request("session/read", Map::new())
            .unwrap();
        let sent = read_frame(&mut input).await;
        send(
            &mut output,
            json!({"jsonrpc":"2.0","id":-1,"method":"slow/request","params":{}}),
        )
        .await;
        send(
            &mut output,
            json!({"jsonrpc":"2.0","id":sent["id"],"result":{"fresh":true}}),
        )
        .await;
        pending.submitted.await.unwrap().unwrap();
        assert_eq!(
            timeout(DEADLINE, pending.response)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            json!({"fresh":true})
        );
        gate.notify_one();
        assert_eq!(read_frame(&mut input).await["id"], -1);
        transport.close().await.unwrap();
    }

    #[tokio::test]
    async fn msp_transport_duplicate_inbound_id_fails_closed_while_in_flight() {
        // Numeric -0.0 and 0 identify the same request even though their JSON
        // spellings differ. Neither form may bypass the in-flight ID fence.
        for (first_id, second_id) in [(json!(1), json!(1)), (json!(0), json!(-0.0))] {
            let (transport, _events, mut input, mut output) = fixture(
                Options::default(),
                slow_router(Arc::new(Notify::new())),
                512,
            );
            let first = json!({"jsonrpc":"2.0","id":first_id,"method":"slow/request","params":{}});
            let second =
                json!({"jsonrpc":"2.0","id":second_id,"method":"slow/request","params":{}});
            output
                .write_all(format!("{first}\n{second}\n").as_bytes())
                .await
                .unwrap();
            assert_eq!(end(&transport).await, TransportEnd::DuplicateServerId);
            let mut trailing = Vec::new();
            timeout(DEADLINE, input.read_to_end(&mut trailing))
                .await
                .unwrap()
                .unwrap();
            assert!(
                trailing.is_empty(),
                "no invented ACK or decision for a collision"
            );
        }
    }

    #[tokio::test]
    async fn msp_transport_server_pressure_is_bounded_and_not_an_approval() {
        let options = Options {
            max_server_requests: 1,
            ..Options::default()
        };
        let (transport, _events, mut input, mut output) =
            fixture(options, slow_router(Arc::new(Notify::new())), 512);
        for id in [1, 2] {
            send(
                &mut output,
                json!({"jsonrpc":"2.0","id":id,"method":"slow/request","params":{}}),
            )
            .await;
        }
        assert_eq!(end(&transport).await, TransportEnd::Backpressure);
        let mut trailing = Vec::new();
        timeout(DEADLINE, input.read_to_end(&mut trailing))
            .await
            .unwrap()
            .unwrap();
        assert!(trailing.is_empty());
    }

    #[tokio::test]
    async fn msp_transport_notification_pressure_is_explicit() {
        let options = Options {
            queued_notifications: 1,
            ..Options::default()
        };
        let (transport, mut events, _input, mut output) = fixture(options, RpcModule::new(()), 512);
        let first = json!({"jsonrpc":"2.0","method":"item/delta","params":{"n":1}});
        let second = json!({"jsonrpc":"2.0","method":"item/delta","params":{"n":2}});
        output
            .write_all(format!("{first}\n{second}\n").as_bytes())
            .await
            .unwrap();
        assert_eq!(end(&transport).await, TransportEnd::Backpressure);
        assert_eq!(events.recv().await.unwrap().envelope, first);
    }

    #[tokio::test]
    async fn msp_transport_malformed_and_fractional_ids_cannot_become_receipts() {
        for value in [
            json!({"jsonrpc":"2.0","id":1,"result":{},"error":{}}),
            json!({"jsonrpc":"2.0","id":1.5,"method":"approval/request","params":{}}),
            json!({"jsonrpc":"2.0","id":null,"method":"approval/request","params":{}}),
        ] {
            let (transport, _events, _input, mut output) =
                fixture(Options::default(), RpcModule::new(()), 512);
            send(&mut output, value).await;
            assert_eq!(end(&transport).await, TransportEnd::InvalidEnvelope);
        }
    }

    #[tokio::test]
    async fn msp_transport_oversize_and_invalid_utf8_are_bounded_failures() {
        let options = Options {
            max_frame_bytes: 128,
            ..Options::default()
        };
        let (transport, _events, _input, mut output) = fixture(options, RpcModule::new(()), 512);
        output.write_all(&[b'x'; 129]).await.unwrap();
        assert_eq!(end(&transport).await, TransportEnd::FrameTooLarge);
        let (transport, _events, _input, mut output) = fixture(options, RpcModule::new(()), 16);
        output.write_all(&[0xff, b'\n']).await.unwrap();
        assert_eq!(end(&transport).await, TransportEnd::InvalidUtf8);
    }

    #[tokio::test]
    async fn msp_transport_truncated_eof_is_not_a_response_or_child_exit() {
        let (transport, _events, mut input, mut output) =
            fixture(Options::default(), RpcModule::new(()), 256);
        let pending = transport
            .submit_request("session/read", Map::new())
            .unwrap();
        let sent = read_frame(&mut input).await;
        output
            .write_all(
                json!({"jsonrpc":"2.0","id":sent["id"],"result":{"fake":true}})
                    .to_string()
                    .as_bytes(),
            )
            .await
            .unwrap();
        output.shutdown().await.unwrap();
        assert_eq!(end(&transport).await, TransportEnd::TruncatedEof);
        assert!(
            timeout(DEADLINE, pending.response)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        let (transport, _events, _input, mut output) =
            fixture(Options::default(), RpcModule::new(()), 16);
        output.shutdown().await.unwrap();
        assert_eq!(end(&transport).await, TransportEnd::Eof);
    }

    #[tokio::test]
    async fn msp_transport_close_barrier_drains_admitted_mutation_before_eof() {
        let options = Options {
            queued_submissions: 1,
            ..Options::default()
        };
        let (transport, _events, mut input, _output) = fixture(options, RpcModule::new(()), 1);
        let pending = transport
            .submit_request(
                "turn/start",
                params(json!({"commandId":"retained-before-transport","input":[]})),
            )
            .unwrap();
        assert!(matches!(
            transport.submit_request("must/not/send", Map::new()),
            Err(TransportEnd::Backpressure)
        ));
        transport.begin_close();
        assert!(matches!(
            transport.notify("initialized", Map::new()),
            Err(TransportEnd::LocalClosed)
        ));
        let mut all = String::new();
        timeout(DEADLINE, input.read_to_string(&mut all))
            .await
            .unwrap()
            .unwrap();
        let lines: Vec<_> = all.lines().collect();
        assert_eq!(lines.len(), 1);
        assert!(all.ends_with('\n'));
        let frame: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(frame["method"], "turn/start");
        assert_eq!(frame["params"]["commandId"], "retained-before-transport");
        pending.submitted.await.unwrap().unwrap();
        assert!(
            timeout(DEADLINE, pending.response)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert_eq!(end(&transport).await, TransportEnd::LocalClosed);
    }

    #[tokio::test]
    async fn msp_transport_cancelled_receipt_does_not_unsend_or_replay() {
        let (transport, _events, mut input, mut output) =
            fixture(Options::default(), RpcModule::new(()), 256);
        let pending = transport
            .submit_request("turn/start", params(json!({"commandId":"one"})))
            .unwrap();
        drop(pending);
        let sent = read_frame(&mut input).await;
        send(
            &mut output,
            json!({"jsonrpc":"2.0","id":sent["id"],"result":{"status":"accepted"}}),
        )
        .await;
        transport.begin_close();
        let mut trailing = Vec::new();
        timeout(DEADLINE, input.read_to_end(&mut trailing))
            .await
            .unwrap()
            .unwrap();
        assert!(trailing.is_empty());
        assert_eq!(end(&transport).await, TransportEnd::LocalClosed);
    }

    #[tokio::test]
    async fn msp_transport_late_old_response_cannot_fail_new_submission() {
        let (transport, _events, mut input, mut output) =
            fixture(Options::default(), RpcModule::new(()), 1);
        let a = transport.submit_request("a", Map::new()).unwrap();
        let a_frame = read_frame(&mut input).await;
        a.submitted.await.unwrap().unwrap();
        let b = transport
            .submit_request("b", params(json!({"bytes":"x".repeat(512)})))
            .unwrap();
        // B's write is backpressured while A completes. A's completion must not
        // clear/fail B's distinct submission ticket.
        send(
            &mut output,
            json!({"jsonrpc":"2.0","id":a_frame["id"],"result":{"a":true}}),
        )
        .await;
        assert_eq!(a.response.await.unwrap().unwrap(), json!({"a":true}));
        let b_frame = read_frame(&mut input).await;
        b.submitted.await.unwrap().unwrap();
        send(
            &mut output,
            json!({"jsonrpc":"2.0","id":b_frame["id"],"result":{"b":true}}),
        )
        .await;
        assert_eq!(b.response.await.unwrap().unwrap(), json!({"b":true}));
        transport.close().await.unwrap();
    }

    struct FailAfter {
        captured: Arc<Mutex<Vec<u8>>>,
    }

    impl AsyncWrite for FailAfter {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            let mut captured = self.captured.lock().unwrap();
            if captured.len() == 3 {
                return std::task::Poll::Ready(Err(std::io::ErrorKind::BrokenPipe.into()));
            }
            let count = (3 - captured.len()).min(bytes.len());
            captured.extend_from_slice(&bytes[..count]);
            std::task::Poll::Ready(Ok(count))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn msp_transport_partial_write_failure_is_not_a_submission_receipt() {
        let (input, _peer) = tokio::io::duplex(64);
        let captured = Arc::new(Mutex::new(Vec::new()));
        let (transport, _events) = MspTransport::attach(
            input,
            FailAfter {
                captured: captured.clone(),
            },
            RpcModule::new(()),
            Options::default(),
        )
        .unwrap();
        let pending = transport
            .submit_request("turn/start", params(json!({"commandId":"one"})))
            .unwrap();
        assert_eq!(
            pending.submitted.await.unwrap().unwrap_err(),
            TransportEnd::WriteFailed
        );
        assert_eq!(end(&transport).await, TransportEnd::WriteFailed);
        assert!(
            timeout(DEADLINE, pending.response)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
        assert_eq!(
            captured.lock().unwrap().len(),
            3,
            "no full frame or replay after partial failure"
        );
    }

    #[tokio::test]
    async fn msp_transport_oversized_local_submission_never_reaches_pipe() {
        let options = Options {
            max_frame_bytes: 128,
            ..Options::default()
        };
        let (transport, _events, mut input, _output) = fixture(options, RpcModule::new(()), 16);
        assert!(matches!(
            transport.submit_request("turn/start", params(json!({"text":"x".repeat(129)}))),
            Err(TransportEnd::FrameTooLarge)
        ));
        transport.begin_close();
        let mut bytes = Vec::new();
        timeout(DEADLINE, input.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        assert!(bytes.is_empty());
        assert_eq!(end(&transport).await, TransportEnd::LocalClosed);
    }
}
