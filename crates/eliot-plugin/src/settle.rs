//! Shared settlement for one native input whose consumption is unproven.
//!
//! An uncertain external effect is never replayed. The command outcome is
//! always `unknown`; only proven removal of the native input lets the session
//! keep serving, and anything less latches the session unknown so no later
//! dispatch can run behind the unresolved input. Each plugin translates its
//! own cancel and readback into the removal proof; the settlement policy and
//! its diagnostic wording live here.

use eliot_core::Error;

/// One settled unknown outcome for an input whose consumption is unproven.
pub struct UnprovenInputSettlement {
    /// Whether the session must latch unknown because the input was not
    /// proven removed.
    pub latch_unknown: bool,
    /// The always-`unknown` command error.
    pub error: Error,
}

/// Settle an input whose consumption is unproven from its removal proof.
/// `message` is the vendor-translated reason; the latch wording is shared.
pub fn settle_unproven_input(removed: bool, message: &str) -> UnprovenInputSettlement {
    if removed {
        UnprovenInputSettlement {
            latch_unknown: false,
            error: Error::new("unknown", message.to_owned()),
        }
    } else {
        UnprovenInputSettlement {
            latch_unknown: true,
            error: Error::new(
                "unknown",
                format!("{message}; the session is latched unknown behind the unresolved input"),
            ),
        }
    }
}
