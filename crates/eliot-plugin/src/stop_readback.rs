//! Settle a native Stop only after its ordered readback fence.

use eliot_core::{Error, Result};

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum StopProvenance {
    Active,
    Readback,
}

pub struct StopCandidate<'a> {
    pub turn_id: &'a str,
    pub opened_at: u64,
}

pub struct StopTarget {
    pub turn_id: String,
    pub provenance: StopProvenance,
}

pub fn bind_stop(
    observed_at: u64,
    active: Option<StopCandidate<'_>>,
    unresolved: Option<StopCandidate<'_>>,
) -> Option<StopTarget> {
    [
        (active, StopProvenance::Active),
        (unresolved, StopProvenance::Readback),
    ]
    .into_iter()
    .find_map(|(candidate, provenance)| {
        let candidate = candidate.filter(|turn| observed_at > turn.opened_at)?;
        Some(StopTarget {
            turn_id: candidate.turn_id.to_owned(),
            provenance,
        })
    })
}

pub struct OrderedStop<T> {
    observed: Option<(StopTarget, T)>,
    barrier: Option<String>,
}

impl<T> Default for OrderedStop<T> {
    fn default() -> Self {
        Self {
            observed: None,
            barrier: None,
        }
    }
}

impl<T> OrderedStop<T> {
    pub fn is_observed(&self) -> bool {
        self.observed.is_some()
    }

    pub fn is_waiting(&self) -> bool {
        self.is_observed() && self.barrier.is_some()
    }

    pub fn observe(&mut self, barrier: String, target: StopTarget, fact: T) {
        self.observed = Some((target, fact));
        self.barrier = Some(barrier);
    }

    /// Timeout cannot promote a hook to a terminal. Its exact later
    /// ordered acknowledgement may still reconcile an unknown turn.
    pub fn mark_turn_unresolved(&mut self, turn_id: &str) -> bool {
        if let Some((target, _)) = self.observed.as_mut() {
            if target.turn_id != turn_id {
                return false;
            }
            target.provenance = StopProvenance::Readback;
        }
        true
    }

    pub fn settle(
        &mut self,
        harness: &str,
        request_id: &str,
        barrier: Result<()>,
        active: Option<&str>,
        unresolved: Option<&str>,
    ) -> Result<(StopTarget, T)> {
        if self.barrier.as_deref() != Some(request_id) {
            return Err(Error::new(
                "source_integrity",
                format!("{harness} Stop barrier did not match its exact request"),
            ));
        }
        self.barrier = None;
        barrier.map_err(|error| error.with_code("unknown"))?;
        let (target, fact) = self.observed.take().ok_or_else(|| {
            Error::new(
                "source_integrity",
                format!("{harness} Stop fact disappeared"),
            )
        })?;
        let (bound, reason) = match target.provenance {
            StopProvenance::Active => (active, "Stop does not match the exact self-started turn"),
            StopProvenance::Readback => {
                (unresolved, "Stop did not match the unsettled native turn")
            }
        };
        if bound != Some(target.turn_id.as_str()) {
            return Err(Error::new("stale_turn", format!("{harness} {reason}")));
        }
        Ok((target, fact))
    }
}
