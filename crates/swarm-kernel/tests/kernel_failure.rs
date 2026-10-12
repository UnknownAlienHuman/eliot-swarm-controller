use std::{convert::Infallible, sync::mpsc, time::Duration};

use swarm_kernel::{
    KernelAdmissionFault, KernelAdmissionState, KernelHostConfig, KernelHostFailure,
    KernelHostJoinError, KernelHostLifecycle, KernelHostReadyError, KernelHostSnapshot, WriterJob,
    spawn_kernel_host,
};
use tokio::{
    sync::{oneshot, watch},
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitFailure(&'static str);

struct WriterExitNotice(Option<oneshot::Sender<()>>);

impl Drop for WriterExitNotice {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

struct JoinPanicGate {
    started: Option<oneshot::Sender<()>>,
    release: mpsc::Receiver<()>,
}

impl Drop for JoinPanicGate {
    fn drop(&mut self) {
        if let Some(sender) = self.started.take() {
            let _ = sender.send(());
        }
        if self.release.recv().is_ok() {
            panic!("join failure fixture sentinel");
        }
    }
}

fn config() -> KernelHostConfig {
    KernelHostConfig {
        queue_capacity: 4,
        batch_capacity: 1,
    }
}

async fn next_status(receiver: &mut watch::Receiver<KernelHostSnapshot>) -> KernelHostSnapshot {
    timeout(WAIT, receiver.changed())
        .await
        .expect("kernel status watch update timed out")
        .expect("kernel status watch sender closed");
    *receiver.borrow_and_update()
}

async fn next_pending_status(
    receiver: &mut watch::Receiver<KernelHostSnapshot>,
    pending_sender: oneshot::Sender<()>,
) -> KernelHostSnapshot {
    let mut changed = Box::pin(receiver.changed());
    let mut pending_sender = Some(pending_sender);
    std::future::poll_fn(
        |context| match std::future::Future::poll(changed.as_mut(), context) {
            std::task::Poll::Pending => {
                if let Some(sender) = pending_sender.take() {
                    let _ = sender.send(());
                }
                std::task::Poll::Pending
            }
            ready => ready,
        },
    )
    .await
    .expect("kernel status watch sender closed");
    drop(changed);
    *receiver.borrow_and_update()
}

#[tokio::test]
async fn ready_initialization_publishes_ready_snapshot_to_watch() {
    let mut host = spawn_kernel_host(
        config(),
        (),
        || Ok::<(), Infallible>(()),
        |_: &mut (), _: ()| {},
        |_: &mut (), _: Vec<()>| {},
    )
    .expect("spawn kernel host");
    let handle = host.handle();
    let mut status = handle.status_receiver();

    timeout(WAIT, host.wait_ready())
        .await
        .expect("wait_ready timed out")
        .expect("successful initializer was rejected");
    let snapshot = next_status(&mut status).await;
    assert_eq!(
        snapshot,
        KernelHostSnapshot {
            admission: KernelAdmissionState::Open,
            lifecycle: KernelHostLifecycle::Ready,
            failure: None,
        }
    );

    drop(handle);
    host.join().expect("ready writer should join cleanly");
}

#[tokio::test]
async fn close_admission_publishes_once_to_a_pending_status_waiter() {
    let mut host = spawn_kernel_host(
        config(),
        (),
        || Ok::<(), Infallible>(()),
        |_: &mut (), _: ()| {},
        |_: &mut (), _: Vec<()>| {},
    )
    .expect("spawn kernel host");
    timeout(WAIT, host.wait_ready())
        .await
        .expect("wait_ready timed out")
        .expect("successful initializer was rejected");
    let handle = host.handle();
    let mut status = handle.status_receiver();
    let (pending_sender, pending_receiver) = oneshot::channel();
    let waiter = tokio::spawn(async move {
        let changed_result = {
            let mut changed = Box::pin(status.changed());
            let mut pending_sender = Some(pending_sender);
            std::future::poll_fn(|context| {
                match std::future::Future::poll(changed.as_mut(), context) {
                    std::task::Poll::Pending => {
                        if let Some(sender) = pending_sender.take() {
                            let _ = sender.send(());
                        }
                        std::task::Poll::Pending
                    }
                    ready => ready,
                }
            })
            .await
        };
        changed_result.expect("kernel status watch sender closed");
        let snapshot = *status.borrow_and_update();
        (snapshot, status)
    });
    timeout(WAIT, pending_receiver)
        .await
        .expect("status waiter did not become pending")
        .expect("status waiter pending signal was dropped");

    host.close_admission(KernelAdmissionFault::StoreUnavailable);
    let (closed, status) = timeout(WAIT, waiter)
        .await
        .expect("status waiter timed out after close_admission")
        .expect("status waiter task failed");
    assert_eq!(
        closed,
        KernelHostSnapshot {
            admission: KernelAdmissionState::Closed {
                fault: KernelAdmissionFault::StoreUnavailable,
            },
            lifecycle: KernelHostLifecycle::AdmissionClosed {
                fault: KernelAdmissionFault::StoreUnavailable,
            },
            failure: None,
        }
    );

    host.close_admission(KernelAdmissionFault::InitializationFailed);
    assert_eq!(handle.snapshot(), closed);
    assert!(
        !status
            .has_changed()
            .expect("kernel status watch sender remains open")
    );

    drop(handle);
    host.join().expect("closed writer should join cleanly");
}

#[tokio::test]
async fn join_publishes_stopping_and_stopped_to_pending_waiter_while_draining_running_job() {
    let (release_sender, release_receiver) = mpsc::channel();
    let (started_sender, started_receiver) = oneshot::channel();
    let (drained_sender, drained_receiver) = oneshot::channel();
    let mut started_sender = Some(started_sender);
    let mut drained_sender = Some(drained_sender);
    let mut host = spawn_kernel_host(
        config(),
        (),
        || Ok::<(), Infallible>(()),
        move |_: &mut (), _: ()| {
            started_sender
                .take()
                .expect("writer callback runs once")
                .send(())
                .expect("test is waiting for the callback");
            release_receiver
                .recv_timeout(WAIT)
                .expect("join must allow the running callback to finish");
            drained_sender
                .take()
                .expect("writer callback completes once")
                .send(())
                .expect("test is waiting for the drained callback");
        },
        |_: &mut (), _: Vec<()>| {},
    )
    .expect("spawn kernel host");
    timeout(WAIT, host.wait_ready())
        .await
        .expect("wait_ready timed out")
        .expect("successful initializer was rejected");

    let handle = host.handle();
    let mut status = handle.status_receiver();
    let ready = *status.borrow_and_update();
    assert_eq!(ready.lifecycle, KernelHostLifecycle::Ready);
    assert_eq!(ready.admission, KernelAdmissionState::Open);

    handle
        .submit(WriterJob::Run(()))
        .await
        .expect("ready writer rejected the callback job");
    timeout(WAIT, started_receiver)
        .await
        .expect("writer callback did not start")
        .expect("writer callback start signal was dropped");

    let (pending_stopping_sender, pending_stopping_receiver) = oneshot::channel();
    let (stopping_sender, stopping_receiver) = oneshot::channel();
    let (pending_stopped_sender, pending_stopped_receiver) = oneshot::channel();
    let (stopped_sender, stopped_receiver) = oneshot::channel();
    let waiter = tokio::spawn(async move {
        let stopping = next_pending_status(&mut status, pending_stopping_sender).await;
        let _ = stopping_sender.send(stopping);
        let stopped = next_pending_status(&mut status, pending_stopped_sender).await;
        let _ = stopped_sender.send(stopped);
        status
    });
    timeout(WAIT, pending_stopping_receiver)
        .await
        .expect("status waiter did not become pending before join")
        .expect("pending signal for Stopping was dropped");

    let joiner = std::thread::spawn(move || host.join());
    let stopping = timeout(WAIT, stopping_receiver)
        .await
        .expect("pending waiter did not receive Stopping")
        .expect("Stopping snapshot signal was dropped");
    assert_eq!(stopping.lifecycle, KernelHostLifecycle::Stopping);
    assert_eq!(
        stopping.admission,
        KernelAdmissionState::Closed {
            fault: KernelAdmissionFault::ShuttingDown,
        }
    );
    timeout(WAIT, pending_stopped_receiver)
        .await
        .expect("status waiter did not become pending before writer completion")
        .expect("pending signal for Stopped was dropped");

    release_sender
        .send(())
        .expect("running callback is waiting for the release");
    timeout(WAIT, drained_receiver)
        .await
        .expect("accepted callback did not drain")
        .expect("callback completion signal was dropped");
    let stopped = timeout(WAIT, stopped_receiver)
        .await
        .expect("pending waiter did not receive Stopped")
        .expect("Stopped snapshot signal was dropped");
    assert_eq!(stopped.lifecycle, KernelHostLifecycle::Stopped);
    assert_eq!(
        stopped.admission,
        KernelAdmissionState::Closed {
            fault: KernelAdmissionFault::ShuttingDown,
        }
    );
    assert_eq!(handle.snapshot(), stopped);

    let status = timeout(WAIT, waiter)
        .await
        .expect("status waiter did not finish")
        .expect("status waiter task failed");
    assert!(
        !status
            .has_changed()
            .expect("retained handle keeps the status channel open")
    );
    assert_eq!(
        joiner.join().expect("kernel join worker thread panicked"),
        Ok(())
    );
    drop(handle);
}

#[tokio::test]
async fn join_publishes_failed_snapshot_to_pending_waiter_after_writer_panics() {
    let (drop_started_sender, drop_started_receiver) = oneshot::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let mut host = spawn_kernel_host(
        config(),
        JoinPanicGate {
            started: Some(drop_started_sender),
            release: release_receiver,
        },
        || Ok::<(), Infallible>(()),
        |_: &mut (), _: ()| {},
        |_: &mut (), _: Vec<()>| {},
    )
    .expect("spawn kernel host");
    timeout(WAIT, host.wait_ready())
        .await
        .expect("wait_ready timed out")
        .expect("successful initializer was rejected");

    let handle = host.handle();
    let mut status = handle.status_receiver();
    let ready = *status.borrow_and_update();
    assert_eq!(ready.lifecycle, KernelHostLifecycle::Ready);

    let (pending_stopping_sender, pending_stopping_receiver) = oneshot::channel();
    let (stopping_sender, stopping_receiver) = oneshot::channel();
    let (pending_failed_sender, pending_failed_receiver) = oneshot::channel();
    let (failed_sender, failed_receiver) = oneshot::channel();
    let waiter = tokio::spawn(async move {
        let stopping = next_pending_status(&mut status, pending_stopping_sender).await;
        let _ = stopping_sender.send(stopping);
        let failed = next_pending_status(&mut status, pending_failed_sender).await;
        let _ = failed_sender.send(failed);
        status
    });
    timeout(WAIT, pending_stopping_receiver)
        .await
        .expect("status waiter did not become pending before join")
        .expect("pending signal for Stopping was dropped");

    let joiner = std::thread::spawn(move || host.join());
    let stopping = timeout(WAIT, stopping_receiver)
        .await
        .expect("pending waiter did not receive Stopping")
        .expect("Stopping snapshot signal was dropped");
    assert_eq!(stopping.lifecycle, KernelHostLifecycle::Stopping);
    timeout(WAIT, drop_started_receiver)
        .await
        .expect("writer did not reach its held shutdown drop")
        .expect("shutdown drop-start signal was dropped");
    assert_eq!(handle.snapshot().lifecycle, KernelHostLifecycle::Stopping);
    timeout(WAIT, pending_failed_receiver)
        .await
        .expect("status waiter did not become pending before writer panic")
        .expect("pending signal for Failed was dropped");

    release_sender
        .send(())
        .expect("writer shutdown drop is waiting for release");
    let failed = timeout(WAIT, failed_receiver)
        .await
        .expect("pending waiter did not receive Failed")
        .expect("Failed snapshot signal was dropped");
    assert_eq!(
        failed,
        KernelHostSnapshot {
            admission: KernelAdmissionState::Closed {
                fault: KernelAdmissionFault::ShuttingDown,
            },
            lifecycle: KernelHostLifecycle::Failed,
            failure: None,
        }
    );
    assert_eq!(handle.snapshot(), failed);

    let status = timeout(WAIT, waiter)
        .await
        .expect("status waiter did not finish")
        .expect("status waiter task failed");
    assert!(
        !status
            .has_changed()
            .expect("retained handle keeps the status channel open")
    );
    assert_eq!(
        joiner.join().expect("kernel join worker thread panicked"),
        Err(KernelHostJoinError::Panicked)
    );
    drop(handle);
}

#[tokio::test]
async fn initialization_error_preserves_initialization_fault_when_admission_preclosed() {
    let (release_sender, release_receiver) = mpsc::channel();
    let (exited_sender, exited_receiver) = oneshot::channel();
    let mut host = spawn_kernel_host(
        config(),
        WriterExitNotice(Some(exited_sender)),
        move || {
            release_receiver
                .recv_timeout(WAIT)
                .expect("preclosed admission must release the initializer");
            Err::<(), InitFailure>(InitFailure("initializer sentinel"))
        },
        |_: &mut (), _: ()| {},
        |_: &mut (), _: Vec<()>| {},
    )
    .expect("spawn kernel host");
    let handle = host.handle();
    let mut status = handle.status_receiver();
    host.close_admission(KernelAdmissionFault::StoreUnavailable);
    release_sender.send(()).expect("initializer is waiting");
    timeout(WAIT, exited_receiver)
        .await
        .expect("initializer did not exit")
        .expect("writer hold was not released");
    // Exercise refresh after confirmed writer exit but before consuming ready.
    // An initialization error must not become a post-ready WriterExited fault.
    let before_ready = handle.snapshot();
    assert_eq!(
        before_ready.lifecycle,
        KernelHostLifecycle::AdmissionClosed {
            fault: KernelAdmissionFault::StoreUnavailable,
        }
    );
    assert_eq!(before_ready.failure, None);

    let readiness = timeout(WAIT, host.wait_ready())
        .await
        .expect("wait_ready timed out");
    match readiness {
        Err(KernelHostReadyError::Initialization(error)) => {
            assert_eq!(error, InitFailure("initializer sentinel"));
        }
        other => panic!("initializer error was not retained: {other:?}"),
    }
    let snapshot = next_status(&mut status).await;
    assert_eq!(snapshot.lifecycle, KernelHostLifecycle::Failed);
    assert_eq!(
        snapshot.admission,
        KernelAdmissionState::Closed {
            fault: KernelAdmissionFault::InitializationFailed,
        }
    );
    assert_eq!(snapshot.failure, None);

    host.close_admission(KernelAdmissionFault::StoreUnavailable);
    assert_eq!(host.snapshot(), snapshot);
    assert!(
        !status
            .has_changed()
            .expect("kernel status watch sender remains open")
    );

    drop(handle);
    host.join()
        .expect("an initializer error is not a writer-exit join failure");
}

#[tokio::test]
async fn initializer_panic_returns_channel_closed_and_join_panicked() {
    let mut host = spawn_kernel_host(
        config(),
        (),
        || -> Result<(), InitFailure> { panic!("initializer panic sentinel") },
        |_: &mut (), _: ()| {},
        |_: &mut (), _: Vec<()>| {},
    )
    .expect("spawn kernel host");
    let handle = host.handle();
    let mut status = handle.status_receiver();

    let readiness = timeout(WAIT, host.wait_ready())
        .await
        .expect("wait_ready timed out");
    assert!(matches!(
        readiness,
        Err(KernelHostReadyError::ChannelClosed)
    ));
    let snapshot = next_status(&mut status).await;
    assert_eq!(snapshot.lifecycle, KernelHostLifecycle::Failed);
    assert_eq!(
        snapshot.admission,
        KernelAdmissionState::Closed {
            fault: KernelAdmissionFault::InitializationFailed,
        }
    );
    assert_eq!(snapshot.failure, None);

    drop(handle);
    assert_eq!(host.join(), Err(KernelHostJoinError::Panicked));
}

#[tokio::test]
async fn ready_writer_panic_wakes_failed_watch_and_join_reports_writer_exit() {
    let (started_sender, started_receiver) = oneshot::channel();
    let mut started_sender = Some(started_sender);
    let mut host = spawn_kernel_host(
        config(),
        (),
        || Ok::<(), Infallible>(()),
        move |_: &mut (), _: ()| -> () {
            started_sender
                .take()
                .expect("writer callback called once")
                .send(())
                .expect("test is still waiting for callback");
            panic!("writer callback panic sentinel");
        },
        |_: &mut (), _: Vec<()>| {},
    )
    .expect("spawn kernel host");
    let handle = host.handle();
    let mut status = handle.status_receiver();

    timeout(WAIT, host.wait_ready())
        .await
        .expect("wait_ready timed out")
        .expect("successful initializer was rejected");
    let ready = next_status(&mut status).await;
    assert_eq!(ready.lifecycle, KernelHostLifecycle::Ready);
    assert_eq!(ready.admission, KernelAdmissionState::Open);

    handle
        .submit(WriterJob::Run(()))
        .await
        .expect("ready writer rejected the callback job");
    timeout(WAIT, started_receiver)
        .await
        .expect("writer callback did not start")
        .expect("writer callback signal was dropped");

    let failed = next_status(&mut status).await;
    assert_eq!(failed.lifecycle, KernelHostLifecycle::Failed);
    assert_eq!(
        failed.admission,
        KernelAdmissionState::Closed {
            fault: KernelAdmissionFault::StoreUnavailable,
        }
    );
    assert_eq!(failed.failure, Some(KernelHostFailure::WriterExited));

    drop(handle);
    assert_eq!(
        host.join(),
        Err(KernelHostJoinError::Panicked),
        "the owner must retain the callback panic as a join failure"
    );
}
