//! Provider-neutral Task policy and bounded writer ownership for the Swarm kernel.
//!
//! The actor owns one FIFO queue and one OS thread. The caller supplies the
//! authoritative resource initializer and typed job handlers; this crate does
//! not own a database or a second source of durable state. Task policy operates
//! on supplied values while the Store retains authorization and transactions.

pub mod acceptance;
pub mod dispatch;
pub mod reviews;
pub mod tasks;

use std::{
    fmt,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    thread::{JoinHandle, Thread},
};

use tokio::sync::{mpsc, oneshot};

/// One ordinary writer operation or one explicitly batchable item.
pub enum WriterJob<Run, Batch> {
    Run(Run),
    Batch(Batch),
}

/// The bounded sender for the actor's single FIFO job queue.
pub type WriterSender<Run, Batch> = mpsc::Sender<WriterJob<Run, Batch>>;

/// Handles returned by [`spawn_writer_actor`].
#[must_use = "retain the sender and join the writer thread during owner shutdown"]
pub struct WriterActor<Run, Batch, InitError> {
    /// Submits work to the actor's one bounded FIFO queue.
    pub sender: WriterSender<Run, Batch>,
    /// The sole OS writer thread; the owner is responsible for joining it.
    pub thread: JoinHandle<()>,
    /// Completes only after initialization succeeds or returns its original error.
    pub ready: oneshot::Receiver<std::result::Result<(), InitError>>,
}

struct WriterExitSignal<Hold> {
    _hold: Hold,
    finished: Arc<AtomicBool>,
}

impl<Hold> Drop for WriterExitSignal<Hold> {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
    }
}

/// Starts one bounded writer thread and retains `hold` until that thread exits.
///
/// `initialize` runs on the writer thread before readiness is reported. Ordinary
/// jobs execute one at a time. A batch begins with the first batchable job and
/// includes only contiguous queued batchable jobs, up to `batch_capacity`; the
/// first non-batchable job stays pending and is executed next. This preserves
/// FIFO order while leaving transaction/domain policy to `process_batch`.
pub fn spawn_writer_actor<
    Database,
    Run,
    Batch,
    InitError,
    Hold,
    Initialize,
    ProcessRun,
    ProcessBatch,
>(
    thread_name: &'static str,
    queue_capacity: usize,
    batch_capacity: usize,
    hold: Hold,
    initialize: Initialize,
    process_run: ProcessRun,
    process_batch: ProcessBatch,
) -> std::io::Result<WriterActor<Run, Batch, InitError>>
where
    Run: Send + 'static,
    Batch: Send + 'static,
    InitError: Send + 'static,
    Hold: Send + 'static,
    Initialize: FnOnce() -> std::result::Result<Database, InitError> + Send + 'static,
    ProcessRun: FnMut(&mut Database, Run) + Send + 'static,
    ProcessBatch: FnMut(&mut Database, Vec<Batch>) + Send + 'static,
{
    spawn_writer_actor_inner(
        thread_name,
        queue_capacity,
        batch_capacity,
        hold,
        initialize,
        process_run,
        process_batch,
        None,
    )
}

// The public actor API's seven independent inputs plus an optional shutdown
// signal retain the same single-writer ownership boundary.
#[allow(clippy::too_many_arguments)]
fn spawn_writer_actor_inner<
    Database,
    Run,
    Batch,
    InitError,
    Hold,
    Initialize,
    ProcessRun,
    ProcessBatch,
>(
    thread_name: &'static str,
    queue_capacity: usize,
    batch_capacity: usize,
    hold: Hold,
    initialize: Initialize,
    process_run: ProcessRun,
    process_batch: ProcessBatch,
    stop_requested: Option<Arc<AtomicBool>>,
) -> std::io::Result<WriterActor<Run, Batch, InitError>>
where
    Run: Send + 'static,
    Batch: Send + 'static,
    InitError: Send + 'static,
    Hold: Send + 'static,
    Initialize: FnOnce() -> std::result::Result<Database, InitError> + Send + 'static,
    ProcessRun: FnMut(&mut Database, Run) + Send + 'static,
    ProcessBatch: FnMut(&mut Database, Vec<Batch>) + Send + 'static,
{
    let (sender, mut receiver) = mpsc::channel::<WriterJob<Run, Batch>>(queue_capacity);
    let (ready_sender, ready) = oneshot::channel();
    let thread = std::thread::Builder::new()
        .name(thread_name.to_owned())
        .spawn(move || {
            let _hold = hold;
            match initialize() {
                Ok(mut database) => {
                    if ready_sender.send(Ok(())).is_ok() {
                        let stop_requested = stop_requested.as_deref();
                        let mut pending: Option<WriterJob<Run, Batch>> = None;
                        let mut process_run = process_run;
                        let mut process_batch = process_batch;
                        loop {
                            let job = match pending.take() {
                                Some(job) => Some(job),
                                None => next_writer_job(&mut receiver, stop_requested),
                            };
                            let Some(job) = job else { break };
                            match job {
                                WriterJob::Run(job) => process_run(&mut database, job),
                                WriterJob::Batch(first) => {
                                    let mut batch = Vec::with_capacity(batch_capacity);
                                    batch.push(first);
                                    while batch.len() < batch_capacity {
                                        match receiver.try_recv() {
                                            Ok(WriterJob::Batch(item)) => batch.push(item),
                                            Ok(other) => {
                                                // Preserve the one queue's FIFO order: the
                                                // next non-batch job must run after this batch.
                                                pending = Some(other);
                                                break;
                                            }
                                            Err(_) => break,
                                        }
                                    }
                                    process_batch(&mut database, batch);
                                }
                            }
                        }
                    }
                }
                Err(error) => {
                    let _ = ready_sender.send(Err(error));
                }
            }
        })?;
    Ok(WriterActor {
        sender,
        thread,
        ready,
    })
}

fn next_writer_job<Run, Batch>(
    receiver: &mut mpsc::Receiver<WriterJob<Run, Batch>>,
    stop_requested: Option<&AtomicBool>,
) -> Option<WriterJob<Run, Batch>> {
    // The host shutdown path cannot wait for arbitrary sender clones to drop.
    // Checking the existing receiver before parking lets producers and the
    // owner publish work or stop with the writer thread's wake token, without
    // introducing another control queue or a periodic idle timer.
    let Some(stop_requested) = stop_requested else {
        return receiver.blocking_recv();
    };

    loop {
        if stop_requested.load(Ordering::Acquire) {
            return None;
        }
        // Stop before taking another queued payload. Dropping the receiver at
        // the loop boundary releases accepted jobs to the caller's existing
        // oneshot closure path.
        match receiver.try_recv() {
            Ok(job) => return Some(job),
            Err(mpsc::error::TryRecvError::Disconnected) => return None,
            Err(mpsc::error::TryRecvError::Empty) => {
                std::thread::park();
            }
        }
    }
}

/// Defaults for the provider-neutral kernel host boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelHostConfig {
    /// Capacity of the one FIFO writer queue.
    pub queue_capacity: usize,
    /// Maximum contiguous batch size for WriterJob::Batch.
    pub batch_capacity: usize,
}

impl Default for KernelHostConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 256,
            batch_capacity: 64,
        }
    }
}

impl KernelHostConfig {
    pub fn validate(self) -> std::result::Result<(), KernelHostConfigError> {
        if self.queue_capacity == 0 {
            return Err(KernelHostConfigError::QueueCapacityZero);
        }
        if self.batch_capacity == 0 {
            return Err(KernelHostConfigError::BatchCapacityZero);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelHostConfigError {
    QueueCapacityZero,
    BatchCapacityZero,
}

impl fmt::Display for KernelHostConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueCapacityZero => formatter.write_str("kernel queue capacity must be nonzero"),
            Self::BatchCapacityZero => formatter.write_str("kernel batch capacity must be nonzero"),
        }
    }
}

impl std::error::Error for KernelHostConfigError {}

/// Faults that close new kernel admissions while preserving the writer owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelAdmissionFault {
    InitializationFailed,
    StoreUnavailable,
    DurableJournalUnavailable,
    ShuttingDown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelAdmissionState {
    Starting,
    Open,
    Closed { fault: KernelAdmissionFault },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelHostLifecycle {
    Starting,
    Ready,
    AdmissionClosed { fault: KernelAdmissionFault },
    Stopping,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KernelHostSnapshot {
    pub admission: KernelAdmissionState,
    pub lifecycle: KernelHostLifecycle,
}

#[derive(Debug)]
pub enum KernelHostSpawnError {
    InvalidConfig(KernelHostConfigError),
    Thread(std::io::Error),
}

impl fmt::Display for KernelHostSpawnError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(error) => write!(formatter, "invalid kernel host config: {error}"),
            Self::Thread(error) => {
                write!(formatter, "kernel writer thread could not start: {error}")
            }
        }
    }
}

impl std::error::Error for KernelHostSpawnError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelSubmitError {
    NotReady,
    AdmissionClosed { fault: KernelAdmissionFault },
    WriterClosed,
}

#[derive(Debug)]
pub enum KernelHostReadyError<InitError> {
    AlreadyAwaited,
    ChannelClosed,
    Initialization(InitError),
}

/// A provider-neutral kernel host boundary around one authoritative writer.
///
/// The host owns queue admission and writer lifecycle, but deliberately does
/// not know the database type, Store transaction policy, adapter schemas, or
/// native process implementations. The caller supplies those through the
/// typed initialization and job callbacks.
#[must_use = "retain the host until its writer thread has been joined"]
pub struct KernelHost<Run, Batch, InitError> {
    sender: WriterSender<Run, Batch>,
    thread: JoinHandle<()>,
    ready: Option<oneshot::Receiver<std::result::Result<(), InitError>>>,
    status: Arc<Mutex<KernelHostSnapshot>>,
    writer_finished: Arc<AtomicBool>,
    stop_requested: Arc<AtomicBool>,
    writer_thread: Arc<Thread>,
}

/// Cloneable admission handle for Store facades that share one host owner.
///
/// The handle carries only the bounded queue sender and the host status view;
/// the owning [`KernelHost`] still retains the sole writer thread and must be
/// joined by its owner during shutdown.
pub struct KernelHostHandle<Run, Batch> {
    sender: WriterSender<Run, Batch>,
    status: Arc<Mutex<KernelHostSnapshot>>,
    writer_finished: Arc<AtomicBool>,
    writer_thread: Arc<Thread>,
}

// Cloning a queue handle does not clone its jobs. Store submits FnOnce
// callbacks, which must not acquire a Clone bound from this facade.
impl<Run, Batch> Clone for KernelHostHandle<Run, Batch> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            status: Arc::clone(&self.status),
            writer_finished: Arc::clone(&self.writer_finished),
            writer_thread: Arc::clone(&self.writer_thread),
        }
    }
}

fn refresh_writer_status(status: &Arc<Mutex<KernelHostSnapshot>>, writer_finished: &AtomicBool) {
    if !writer_finished.load(Ordering::Acquire) {
        return;
    }
    let mut snapshot = status
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let fault = match snapshot.lifecycle {
        KernelHostLifecycle::Ready => Some(KernelAdmissionFault::StoreUnavailable),
        KernelHostLifecycle::AdmissionClosed { fault } => {
            (!matches!(fault, KernelAdmissionFault::ShuttingDown)).then_some(fault)
        }
        KernelHostLifecycle::Starting
        | KernelHostLifecycle::Stopping
        | KernelHostLifecycle::Stopped
        | KernelHostLifecycle::Failed => None,
    };
    if let Some(fault) = fault {
        snapshot.admission = KernelAdmissionState::Closed { fault };
        snapshot.lifecycle = KernelHostLifecycle::Failed;
    }
}

impl<Run, Batch> KernelHostHandle<Run, Batch> {
    fn status_lock(&self) -> MutexGuard<'_, KernelHostSnapshot> {
        self.status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn snapshot(&self) -> KernelHostSnapshot {
        refresh_writer_status(&self.status, &self.writer_finished);
        *self.status_lock()
    }

    pub fn admission_state(&self) -> KernelAdmissionState {
        self.snapshot().admission
    }

    fn writer_closed_error(&self) -> KernelSubmitError {
        let mut status = self.status_lock();
        let fault = match status.admission {
            KernelAdmissionState::Closed { fault } => fault,
            KernelAdmissionState::Starting | KernelAdmissionState::Open => {
                let fault = KernelAdmissionFault::StoreUnavailable;
                status.admission = KernelAdmissionState::Closed { fault };
                status.lifecycle = KernelHostLifecycle::AdmissionClosed { fault };
                fault
            }
        };
        KernelSubmitError::AdmissionClosed { fault }
    }

    /// Submit one typed job while enforcing the shared host admission state.
    pub async fn submit(
        &self,
        job: WriterJob<Run, Batch>,
    ) -> std::result::Result<(), KernelSubmitError>
    where
        Run: Send + 'static,
        Batch: Send + 'static,
    {
        // The shared snapshot check is the admission linearization point;
        // work observed as Ready before close_admission may still drain.
        let snapshot = self.snapshot();
        if !matches!(snapshot.lifecycle, KernelHostLifecycle::Ready) {
            return Err(match snapshot.admission {
                KernelAdmissionState::Starting => KernelSubmitError::NotReady,
                KernelAdmissionState::Open => KernelSubmitError::NotReady,
                KernelAdmissionState::Closed { fault } => {
                    KernelSubmitError::AdmissionClosed { fault }
                }
            });
        }
        self.sender
            .send(job)
            .await
            .map_err(|_| self.writer_closed_error())?;
        self.writer_thread.unpark();
        Ok(())
    }
}

impl<Run, Batch, InitError> KernelHost<Run, Batch, InitError> {
    fn status_lock(&self) -> MutexGuard<'_, KernelHostSnapshot> {
        self.status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub fn snapshot(&self) -> KernelHostSnapshot {
        refresh_writer_status(&self.status, &self.writer_finished);
        *self.status_lock()
    }

    pub fn admission_state(&self) -> KernelAdmissionState {
        self.snapshot().admission
    }

    fn writer_closed_error(&self) -> KernelSubmitError {
        let mut status = self.status_lock();
        let fault = match status.admission {
            KernelAdmissionState::Closed { fault } => fault,
            KernelAdmissionState::Starting | KernelAdmissionState::Open => {
                let fault = KernelAdmissionFault::StoreUnavailable;
                status.admission = KernelAdmissionState::Closed { fault };
                status.lifecycle = KernelHostLifecycle::AdmissionClosed { fault };
                fault
            }
        };
        KernelSubmitError::AdmissionClosed { fault }
    }

    pub fn handle(&self) -> KernelHostHandle<Run, Batch> {
        KernelHostHandle {
            sender: self.sender.clone(),
            status: Arc::clone(&self.status),
            writer_finished: Arc::clone(&self.writer_finished),
            writer_thread: Arc::clone(&self.writer_thread),
        }
    }

    /// Close only new jobs. The writer and its retained hold continue to
    /// exist so the owning process can reconcile or drain accepted work.
    pub fn close_admission(&self, fault: KernelAdmissionFault) {
        let mut status = self.status_lock();
        if matches!(
            status.lifecycle,
            KernelHostLifecycle::Stopped | KernelHostLifecycle::Failed
        ) {
            return;
        }
        status.admission = KernelAdmissionState::Closed { fault };
        status.lifecycle = KernelHostLifecycle::AdmissionClosed { fault };
    }

    pub fn reopen_admission_after_recovery(&self) {
        let mut status = self.status_lock();
        if matches!(
            status.lifecycle,
            KernelHostLifecycle::AdmissionClosed { fault }
                if !matches!(fault, KernelAdmissionFault::ShuttingDown)
        ) {
            status.admission = KernelAdmissionState::Open;
            status.lifecycle = KernelHostLifecycle::Ready;
        }
    }

    /// Wait for the authoritative initialization callback before admitting
    /// jobs. Initialization failure closes admission and marks the host failed.
    pub async fn wait_ready(&mut self) -> std::result::Result<(), KernelHostReadyError<InitError>> {
        let Some(ready) = self.ready.take() else {
            return Err(KernelHostReadyError::AlreadyAwaited);
        };
        match ready.await {
            Ok(Ok(())) => {
                let mut status = self.status_lock();
                if matches!(status.lifecycle, KernelHostLifecycle::Starting) {
                    status.admission = KernelAdmissionState::Open;
                    status.lifecycle = KernelHostLifecycle::Ready;
                }
                Ok(())
            }
            Ok(Err(error)) => {
                let mut status = self.status_lock();
                status.admission = KernelAdmissionState::Closed {
                    fault: KernelAdmissionFault::InitializationFailed,
                };
                status.lifecycle = KernelHostLifecycle::Failed;
                Err(KernelHostReadyError::Initialization(error))
            }
            Err(_) => {
                let mut status = self.status_lock();
                status.admission = KernelAdmissionState::Closed {
                    fault: KernelAdmissionFault::InitializationFailed,
                };
                status.lifecycle = KernelHostLifecycle::Failed;
                Err(KernelHostReadyError::ChannelClosed)
            }
        }
    }

    /// Submit one typed job through the admission boundary.
    pub async fn submit(
        &self,
        job: WriterJob<Run, Batch>,
    ) -> std::result::Result<(), KernelSubmitError>
    where
        Run: Send + 'static,
        Batch: Send + 'static,
    {
        // The shared snapshot check is the admission linearization point;
        // work observed as Ready before close_admission may still drain.
        let snapshot = self.snapshot();
        if !matches!(snapshot.lifecycle, KernelHostLifecycle::Ready) {
            return Err(match snapshot.admission {
                KernelAdmissionState::Starting => KernelSubmitError::NotReady,
                KernelAdmissionState::Open => KernelSubmitError::NotReady,
                KernelAdmissionState::Closed { fault } => {
                    KernelSubmitError::AdmissionClosed { fault }
                }
            });
        }
        self.sender
            .send(job)
            .await
            .map_err(|_| self.writer_closed_error())?;
        self.writer_thread.unpark();
        Ok(())
    }

    /// Stop new admissions and join the sole writer thread.
    pub fn join(self) -> std::thread::Result<()> {
        let KernelHost {
            sender,
            thread,
            ready: _,
            status,
            writer_finished: _,
            stop_requested,
            writer_thread,
        } = self;
        {
            let mut snapshot = status
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            snapshot.admission = KernelAdmissionState::Closed {
                fault: KernelAdmissionFault::ShuttingDown,
            };
            if !matches!(snapshot.lifecycle, KernelHostLifecycle::Failed) {
                snapshot.lifecycle = KernelHostLifecycle::Stopping;
            }
        }
        stop_requested.store(true, Ordering::Release);
        writer_thread.unpark();
        drop(sender);
        let result = thread.join();
        let mut snapshot = status
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let failed = matches!(snapshot.lifecycle, KernelHostLifecycle::Failed);
        snapshot.admission = KernelAdmissionState::Closed {
            fault: KernelAdmissionFault::ShuttingDown,
        };
        snapshot.lifecycle = if result.is_ok() && !failed {
            KernelHostLifecycle::Stopped
        } else {
            KernelHostLifecycle::Failed
        };
        result
    }
}

/// Start the provider-neutral host boundary with one writer and one queue.
pub fn spawn_kernel_host<
    Database,
    Run,
    Batch,
    InitError,
    Hold,
    Initialize,
    ProcessRun,
    ProcessBatch,
>(
    config: KernelHostConfig,
    hold: Hold,
    initialize: Initialize,
    process_run: ProcessRun,
    process_batch: ProcessBatch,
) -> std::result::Result<KernelHost<Run, Batch, InitError>, KernelHostSpawnError>
where
    Run: Send + 'static,
    Batch: Send + 'static,
    InitError: Send + 'static,
    Hold: Send + 'static,
    Initialize: FnOnce() -> std::result::Result<Database, InitError> + Send + 'static,
    ProcessRun: FnMut(&mut Database, Run) + Send + 'static,
    ProcessBatch: FnMut(&mut Database, Vec<Batch>) + Send + 'static,
{
    config
        .validate()
        .map_err(KernelHostSpawnError::InvalidConfig)?;
    let writer_finished = Arc::new(AtomicBool::new(false));
    let stop_requested = Arc::new(AtomicBool::new(false));
    let WriterActor {
        sender,
        thread,
        ready,
    } = spawn_writer_actor_inner(
        "swarm-kernel-writer",
        config.queue_capacity,
        config.batch_capacity,
        WriterExitSignal {
            _hold: hold,
            finished: Arc::clone(&writer_finished),
        },
        initialize,
        process_run,
        process_batch,
        Some(Arc::clone(&stop_requested)),
    )
    .map_err(KernelHostSpawnError::Thread)?;
    let writer_thread = Arc::new(thread.thread().clone());
    Ok(KernelHost {
        sender,
        thread,
        ready: Some(ready),
        status: Arc::new(Mutex::new(KernelHostSnapshot {
            admission: KernelAdmissionState::Starting,
            lifecycle: KernelHostLifecycle::Starting,
        })),
        writer_finished,
        stop_requested,
        writer_thread,
    })
}
