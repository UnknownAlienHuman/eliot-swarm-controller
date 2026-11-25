//! Containment policy for an exact native turn.

use std::time::Duration;

use tokio::{
    sync::watch,
    time::{Instant, timeout_at},
};

/// Raw model evidence and its adapter-defined canonical identity.
pub struct ObservedModel<'a> {
    pub raw: &'a str,
    pub canonical: &'a str,
}

pub enum ModelDisposition<'a> {
    Unverified,
    Verified(&'a str),
    Contain {
        requested: &'a str,
        reported: &'a str,
    },
}

/// Classify a provider response from a surface that can echo the selection
/// when the actual model is absent. The adapter supplies canonical IDs.
pub fn model_disposition<'a>(
    requested: Option<ObservedModel<'a>>,
    reported: Option<ObservedModel<'a>>,
) -> ModelDisposition<'a> {
    let Some(reported) = reported else {
        return ModelDisposition::Unverified;
    };
    match requested {
        Some(requested) if requested.raw == reported.raw => ModelDisposition::Unverified,
        Some(requested) if requested.canonical != reported.canonical => ModelDisposition::Contain {
            requested: requested.raw,
            reported: reported.raw,
        },
        _ => ModelDisposition::Verified(reported.raw),
    }
}

pub struct ContainedTerminal<'a> {
    pub status: &'a str,
    pub outcome: &'a str,
    pub mismatch: Option<&'a str>,
}

/// A contained turn retains its native lifecycle proof and settles failed.
pub fn model_terminal<'a>(
    status: &'a str,
    outcome: &'a str,
    mismatch: Option<&'a str>,
) -> ContainedTerminal<'a> {
    if mismatch.is_some() {
        ContainedTerminal {
            status: "failed",
            outcome: "failure",
            mismatch,
        }
    } else {
        ContainedTerminal {
            status,
            outcome,
            mismatch,
        }
    }
}

pub enum CancellationProof {
    Accepted,
    Unconfirmed,
}

pub fn cancellation_deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

pub fn stop_after_cancel(deadline: Instant, proof: CancellationProof) -> Instant {
    match proof {
        CancellationProof::Accepted => deadline,
        CancellationProof::Unconfirmed => Instant::now(),
    }
}

/// Retire the owned host when the live containment deadline expires. A
/// committed terminal can clear the deadline while the caller waits.
pub async fn wait_for_containment_stop<S>(
    mut status: watch::Receiver<S>,
    stop_at: impl Fn(&S) -> Option<Instant>,
) {
    loop {
        let deadline = stop_at(&status.borrow_and_update());
        if let Some(deadline) = deadline {
            match timeout_at(deadline, status.changed()).await {
                Err(_) if stop_at(&status.borrow()) == Some(deadline) => return,
                Err(_) | Ok(Ok(())) => continue,
                Ok(Err(_)) => break,
            }
        } else if status.changed().await.is_err() {
            break;
        }
    }
    std::future::pending::<()>().await;
}
