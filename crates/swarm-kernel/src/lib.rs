//! Generic single-threaded bounded writer actor primitives for the Swarm kernel.
//!
//! The actor owns one FIFO queue and one OS thread. The caller supplies the
//! authoritative resource initializer and typed job handlers; this crate does
//! not own a database, domain policy, or a second source of durable state.

use std::thread::JoinHandle;

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
    let (sender, mut receiver) = mpsc::channel::<WriterJob<Run, Batch>>(queue_capacity);
    let (ready_sender, ready) = oneshot::channel();
    let thread = std::thread::Builder::new()
        .name(thread_name.to_owned())
        .spawn(move || {
            let _hold = hold;
            match initialize() {
                Ok(mut database) => {
                    if ready_sender.send(Ok(())).is_ok() {
                        let mut pending: Option<WriterJob<Run, Batch>> = None;
                        let mut process_run = process_run;
                        let mut process_batch = process_batch;
                        loop {
                            let job = match pending.take() {
                                Some(job) => Some(job),
                                None => receiver.blocking_recv(),
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
