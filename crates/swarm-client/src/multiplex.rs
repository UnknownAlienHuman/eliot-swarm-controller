//! One authenticated physical transport: fixed reader/writer tasks, bounded
//! admission and exact correlation. This layer retries no application frame.

use crate::IpcConfig;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};
use swarm_contracts::error::{Error, Result};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

pub(crate) trait IoStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> IoStream for T {}
pub(crate) type Stream = Box<dyn IoStream>;
const QUEUED: u8 = 0;
const WRITING: u8 = 1;
const FLUSHED: u8 = 2;
const MATCHED: u8 = 3;
const COMPLETED: u8 = 4;
const MAX_ID: u64 = 0xffff_ffff_ffff;

struct Flight {
    stage: AtomicU8,
}
struct Pending {
    reply: oneshot::Sender<Result<Value>>,
    flight: Arc<Flight>,
    _permit: OwnedSemaphorePermit,
    #[cfg(feature = "qualification-faults")]
    method: String,
}
struct State {
    pending: BTreeMap<String, Pending>,
    failed: bool,
    prefix: String,
    next_id: u64,
}
struct Shared {
    state: Mutex<State>,
    ended: watch::Sender<bool>,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn poison(&self, error: Error) {
        let pending = {
            let mut state = self.lock();
            if state.failed {
                return;
            }
            state.failed = true;
            std::mem::take(&mut state.pending)
        };
        self.ended.send_replace(true);
        for (_, pending) in pending {
            let _ = pending.reply.send(Err(error.clone()));
        }
    }
}
struct Write {
    id: String,
    line: String,
}
struct CallGuard {
    shared: Arc<Shared>,
    id: String,
    flight: Arc<Flight>,
    complete: bool,
}
impl CallGuard {
    fn complete(&mut self) {
        self.complete = true;
        self.flight.stage.store(COMPLETED, Ordering::Release);
    }
}
impl Drop for CallGuard {
    fn drop(&mut self) {
        if self.complete {
            return;
        }
        let might_have_written = {
            let mut state = self.shared.lock();
            let stage = self.flight.stage.load(Ordering::Acquire);
            // The writer takes QUEUED->WRITING under this same lock. A queued
            // cancellation removes admission before the writer can touch it.
            if stage == QUEUED {
                state.pending.remove(&self.id);
                false
            } else {
                true
            }
        };
        if might_have_written {
            self.shared.poison(unknown(
                "request cancelled after write became possible; inspect the original operation",
            ));
        }
    }
}

pub(crate) struct Session {
    shared: Arc<Shared>,
    writes: mpsc::Sender<Write>,
    slots: Arc<Semaphore>,
    limit: usize,
    reply_timeout: Duration,
    // Retain both handles across cancellation of drain; tasks never own Session.
    tasks: tokio::sync::Mutex<Vec<JoinHandle<()>>>,
}
impl Session {
    pub(crate) fn attach(stream: Stream, config: &IpcConfig) -> Result<Arc<Self>> {
        if config.max_frame_bytes == 0
            || config.max_inflight_per_connection == 0
            || config.max_inflight_per_connection > Semaphore::MAX_PERMITS
            || config.write_timeout_seconds == 0
        {
            return Err(Error::invalid(
                "IPC frame, inflight and write timeout bounds must be positive",
            ));
        }
        let (ended, _) = watch::channel(false);
        let mut prefix = uuid::Uuid::new_v4().to_string();
        prefix.truncate(24);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                pending: BTreeMap::new(),
                failed: false,
                prefix,
                next_id: 0,
            }),
            ended,
        });
        let (writes, requests) = mpsc::channel(config.max_inflight_per_connection);
        let (read, write) = tokio::io::split(stream);
        let writer = tokio::spawn(write_frames(
            FramedWrite::new(
                write,
                LinesCodec::new_with_max_length(config.max_frame_bytes),
            ),
            requests,
            shared.clone(),
            Duration::from_secs(config.write_timeout_seconds),
        ));
        let reader = tokio::spawn(read_frames(
            FramedRead::new(
                read,
                LinesCodec::new_with_max_length(config.max_frame_bytes),
            ),
            shared.clone(),
        ));
        Ok(Arc::new(Self {
            shared,
            writes,
            slots: Arc::new(Semaphore::new(config.max_inflight_per_connection)),
            limit: config.max_frame_bytes,
            reply_timeout: Duration::from_secs(60),
            tasks: tokio::sync::Mutex::new(vec![reader, writer]),
        }))
    }
    fn submit(
        &self,
        method: &str,
        params: Value,
    ) -> Result<(CallGuard, oneshot::Receiver<Result<Value>>)> {
        if self.shared.lock().failed {
            return Err(disconnected());
        }
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| capacity())?;
        let writer = self.writes.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => capacity(),
            mpsc::error::TrySendError::Closed(_) => disconnected(),
        })?;
        let mut state = self.shared.lock();
        if state.failed {
            return Err(disconnected());
        }
        if state.next_id == MAX_ID {
            return Err(Error::new(
                "IPC_ID_EXHAUSTED",
                "the connection's request identity space is exhausted",
            ));
        }
        state.next_id += 1;
        // UUID-shaped string with a connection nonce and a monotonic 48-bit
        // suffix. No retired ID can ever be reused on this physical stream.
        let id = format!("{}{:012x}", state.prefix, state.next_id);
        let line = serde_json::to_string(
            &json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}),
        )?;
        if line.len() > self.limit {
            return Err(Error::new(
                "FRAME_TOO_LARGE",
                "use smaller pages or artifact ranges",
            ));
        }
        let (reply, response) = oneshot::channel();
        let flight = Arc::new(Flight {
            stage: AtomicU8::new(QUEUED),
        });
        state.pending.insert(
            id.clone(),
            Pending {
                reply,
                flight: flight.clone(),
                _permit: permit,
                #[cfg(feature = "qualification-faults")]
                method: method.into(),
            },
        );
        let guard = CallGuard {
            shared: self.shared.clone(),
            id: id.clone(),
            flight,
            complete: false,
        };
        writer.send(Write { id, line });
        Ok((guard, response))
    }
    pub(crate) async fn exchange(&self, method: &str, params: Value) -> Result<Value> {
        let (mut guard, response) = self.submit(method, params)?;
        let result = match tokio::time::timeout(self.reply_timeout, response).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                let error = unknown("reply channel closed; inspect the original operation");
                self.shared.poison(error.clone());
                Err(error)
            }
            Err(_) => {
                let error =
                    unknown("reply missing; inspect the original operation before retrying");
                self.shared.poison(error.clone());
                Err(error)
            }
        };
        guard.complete();
        result
    }
    pub(crate) fn connected(&self) -> bool {
        !self.shared.lock().failed
    }
    pub(crate) async fn close(&self) {
        self.shared.poison(unknown(
            "connection closed with unresolved delivery; inspect the original operation",
        ));
        let mut tasks = self.tasks.lock().await;
        while let Some(task) = tasks.last_mut() {
            let _ = task.await;
            tasks.pop();
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.shared.poison(unknown(
            "connection dropped with unresolved delivery; inspect the original operation",
        ));
    }
}
fn disconnected() -> Error {
    Error::new(
        "DISCONNECTED",
        "client link failed; reconnect before another request",
    )
}
fn capacity() -> Error {
    Error::new(
        "IPC_CAPACITY",
        "inflight or writer bound reached; this request was not submitted",
    )
}
fn unknown(message: &str) -> Error {
    Error::new("OUTCOME_UNKNOWN", message)
}

async fn write_frames<W: AsyncWrite + Unpin>(
    mut writer: FramedWrite<W, LinesCodec>,
    mut writes: mpsc::Receiver<Write>,
    shared: Arc<Shared>,
    timeout: Duration,
) {
    let mut ended = shared.ended.subscribe();
    loop {
        if *ended.borrow() {
            return;
        }
        let write = tokio::select! {biased;_=ended.changed()=>return,write=writes.recv()=>match write{Some(write)=>write,None=>return}};
        let flight = {
            let state = shared.lock();
            if state.failed {
                return;
            }
            state.pending.get(&write.id).map(|p| p.flight.clone())
        };
        let Some(flight) = flight else {
            continue;
        };
        {
            let state = shared.lock();
            if state.failed {
                return;
            }
            if !state.pending.contains_key(&write.id)
                || flight
                    .stage
                    .compare_exchange(QUEUED, WRITING, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
            {
                continue;
            }
        }
        let sent = tokio::select! {biased;_=ended.changed()=>return,result=tokio::time::timeout(timeout,writer.send(write.line))=>result};
        if !matches!(sent, Ok(Ok(()))) {
            shared.poison(unknown("request write did not complete; delivery is uncertain; inspect the original operation"));
            return;
        }
        let _ =
            flight
                .stage
                .compare_exchange(WRITING, FLUSHED, Ordering::AcqRel, Ordering::Acquire);
    }
}

fn decode(line: &str) -> Result<(String, Result<Value>)> {
    let response: Value = serde_json::from_str(line)
        .map_err(|_| unknown("reply was malformed or could not be matched"))?;
    if response["jsonrpc"] != "2.0" {
        return Err(unknown("reply was malformed or could not be matched"));
    }
    let id = response["id"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| unknown("reply was malformed or could not be matched"))?
        .to_owned();
    let result = match (response.get("result"), response.get("error")) {
        (Some(result), None) => Ok(result.clone()),
        (None, Some(error))
            if error.is_object()
                && error["code"].as_i64().is_some()
                && error["message"].is_string()
                && error["data"]["code"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty()) =>
        {
            Err(Error::new(
                error["data"]["code"].as_str().unwrap_or("RPC_ERROR"),
                error["message"].as_str().unwrap_or("RPC error"),
            ))
        }
        _ => return Err(unknown("reply was malformed or could not be matched")),
    };
    Ok((id, result))
}
async fn read_frames<R: AsyncRead + Unpin>(
    mut reader: FramedRead<R, LinesCodec>,
    shared: Arc<Shared>,
) {
    let mut ended = shared.ended.subscribe();
    loop {
        if *ended.borrow() {
            return;
        }
        let line = tokio::select! {biased;_=ended.changed()=>return,line=reader.next()=>match line{Some(Ok(line))=>line,_=>{shared.poison(unknown("server closed or returned an invalid frame before reply"));return;}}};
        let (id, result) = match decode(&line) {
            Ok(decoded) => decoded,
            Err(error) => {
                shared.poison(error);
                return;
            }
        };
        let pending = {
            let mut state = shared.lock();
            if state.failed {
                return;
            }
            state.pending.remove(&id)
        };
        let Some(pending) = pending else {
            shared.poison(unknown("reply ID was unknown, duplicated or retired"));
            return;
        };
        if pending.flight.stage.load(Ordering::Acquire) < WRITING {
            let error = unknown("reply arrived for an unwritten request");
            let _ = pending.reply.send(Err(error.clone()));
            shared.poison(error);
            return;
        }
        pending.flight.stage.store(MATCHED, Ordering::Release);
        #[cfg(feature = "qualification-faults")]
        if crate::qualification_faults::take_single_reply_drop_if_armed(&pending.method) {
            let error = Error::new(
                crate::qualification_faults::QUALIFICATION_REPLY_DROPPED,
                format!(
                    "qualified reply withheld at the transport seam; the request was sent once and is not reusable; withheld_frame_bytes={}; jsonrpc_request_id={}; jsonrpc_reply_id={}",
                    line.len(),
                    id,
                    id
                ),
            );
            let _ = pending.reply.send(Err(error));
            shared.poison(unknown(
                "a qualified response was withheld; inspect the original operation",
            ));
            return;
        }
        if pending.reply.send(result).is_err() {
            shared.poison(unknown(
                "response consumer was cancelled after write; inspect the original operation",
            ));
            return;
        }
    }
}
