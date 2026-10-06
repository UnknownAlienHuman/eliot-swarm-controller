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
