use std::{future::Future, time::Duration};

use eliot_core::{Error, Result};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

/// Observe a matching publication without polling. Keep the last retained
/// value visible if the publisher closes or the deadline arrives with it.
pub async fn wait_for_status<S, T>(
    status: &mut watch::Receiver<S>,
    deadline: Instant,
    read: impl Fn(&S) -> Option<T>,
) -> Option<T> {
    loop {
        if let Some(value) = read(&status.borrow_and_update()) {
            return Some(value);
        }
        if !matches!(timeout_at(deadline, status.changed()).await, Ok(Ok(()))) {
            return read(&status.borrow());
        }
    }
}

/// Leave time for the adapter to report a control timeout to the host
/// before the host's 30-second command reply window closes.
const CONTROL_DEADLINE: Duration = Duration::from_secs(25);

/// A vendor event loop's typed control mailbox and observable status.
pub struct EventActor<C, S> {
    control: mpsc::Sender<C>,
    status: watch::Receiver<S>,
}

impl<C, S> Clone for EventActor<C, S> {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
            status: self.status.clone(),
        }
    }
}

impl<C: Send + 'static, S: Send + Sync + 'static> EventActor<C, S> {
    pub fn spawn<F>(
        capacity: usize,
        initial: S,
        run: impl FnOnce(mpsc::Receiver<C>, watch::Sender<S>) -> F,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        Self::spawn_with_actor(capacity, initial, |_, controls, status| {
            run(controls, status)
        })
    }

    pub fn spawn_with_actor<F>(
        capacity: usize,
        initial: S,
        run: impl FnOnce(Self, mpsc::Receiver<C>, watch::Sender<S>) -> F,
    ) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (control, controls) = mpsc::channel(capacity);
        let (status_tx, status) = watch::channel(initial);
        let actor = Self { control, status };
        tokio::spawn(run(actor.clone(), controls, status_tx));
        actor
    }

    pub fn watch(&self) -> &watch::Receiver<S> {
        &self.status
    }

    pub fn is_closed(&self) -> bool {
        self.control.is_closed()
    }

    /// Internal notifications wait for admission while the event consumer is open.
    pub async fn deliver(&self, event: C) {
        let _ = self.control.send(event).await;
    }

    pub async fn call<T>(&self, build: impl FnOnce(oneshot::Sender<T>) -> C) -> Result<T> {
        self.call_until(Instant::now() + CONTROL_DEADLINE, build, false)
            .await
    }

    pub async fn call_by<T>(
        &self,
        deadline: Instant,
        build: impl FnOnce(oneshot::Sender<T>) -> C,
    ) -> Result<T> {
        self.call_until(deadline, build, false).await
    }

    /// Exposes definite pre-dispatch errors to a caller that distinguishes
    /// refusal from an admitted control whose effect is unknown.
    pub async fn call_with_dispatch<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<T>) -> C,
    ) -> Result<T> {
        self.call_until(Instant::now() + CONTROL_DEADLINE, build, true)
            .await
    }

    async fn call_until<T>(
        &self,
        deadline: Instant,
        build: impl FnOnce(oneshot::Sender<T>) -> C,
        distinguish_dispatch: bool,
    ) -> Result<T> {
        let (reply, response) = oneshot::channel();
        enqueue(&self.control, build(reply), deadline, distinguish_dispatch).await?;
        timeout_at(deadline, response)
            .await
            .map_err(|_| Error::new("unknown", "event-control reply deadline elapsed"))?
            .map_err(|_| Error::new("unknown", "event-control reply was lost"))
    }
}

async fn enqueue<C>(
    sender: &mpsc::Sender<C>,
    control: C,
    deadline: Instant,
    distinguish_dispatch: bool,
) -> Result<()> {
    let (full, closed) = if distinguish_dispatch {
        ("busy", "native_not_dispatched")
    } else {
        ("unknown", "unknown")
    };
    if Instant::now() >= deadline {
        return Err(Error::new(full, "event-control queue deadline elapsed"));
    }
    timeout_at(deadline, sender.send(control))
        .await
        .map_err(|_| Error::new(full, "event-control queue deadline elapsed"))?
        .map_err(|_| Error::new(closed, "event processor stopped"))
}
