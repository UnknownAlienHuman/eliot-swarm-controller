// ============================================================================
// Qualification-only transport fault.
//
// Compiled only when the crate is built with the non-default
// qualification-faults feature. A release-default build contains no
// qualification activation path at all: no symbol, no runtime switch, no
// configuration key, and no environment variable.
//
// The fault acts at the real transport seam: after the request frame has been
// written exactly once and unaltered, and after the complete response frame has
// been received, parsed, and matched to its request id, but before that
// response is delivered to the caller. The connection is poisoned exactly as an
// uncertain reply poisons it, so the worker enters its existing uncertain
// branch, rereads the authoritative page, and never resubmits the same cut.
//
// Arming is one per worker lifetime and spent is terminal.
// ============================================================================

use std::sync::atomic::{AtomicU8, Ordering};

/// Error code surfaced when a qualification fault withheld one reply.
///
/// It is already in the scheduler worker retryable set, so the existing
/// uncertain branch handles it without any new control flow.
pub const QUALIFICATION_REPLY_DROPPED: &str = "QUALIFICATION_REPLY_DROPPED";

/// The only method this module can target.
///
/// Kept as a literal because this crate cannot depend on the automation crate
/// without a dependency cycle. The value is the worker admit method.
const ADMIT_METHOD: &str = "automation.scheduler.admit";

/// One arming state for the qualification reply fault.
///
/// This single type backs both the process static and the unit tests. It holds
/// a real atomic byte, so the transition table the tests exercise is the exact
/// transition table the live static uses. There is no separate mirror and
/// therefore no second algorithm that could drift from the first.
///
/// States are 0 never armed, 1 armed, 2 spent. State 2 is terminal, which is
/// what makes a consumed fault structurally impossible to rearm for the
/// lifetime of the process.
#[derive(Debug)]
pub struct QualificationArming {
    state: AtomicU8,
}

/// Never armed.
pub const QUALIFICATION_ARMING_NEVER: u8 = 0;
/// Armed, waiting for the one eligible reply.
pub const QUALIFICATION_ARMING_ARMED: u8 = 1;
/// Spent. Terminal; nothing can move the state forward.
pub const QUALIFICATION_ARMING_SPENT: u8 = 2;

impl QualificationArming {
    /// One instance in the never armed state.
    pub const fn new() -> Self {
        Self {
            state: AtomicU8::new(QUALIFICATION_ARMING_NEVER),
        }
    }

    /// Arm exactly once. Returns whether this call performed the arming.
    ///
    /// Only a never armed instance can move to armed, so at most one arm can
    /// ever succeed for any instance.
    pub fn arm(&self) -> bool {
        self.state
            .compare_exchange(
                QUALIFICATION_ARMING_NEVER,
                QUALIFICATION_ARMING_ARMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Consume the arm for the given method. Returns whether this call spent it.
    ///
    /// A method other than the automation scheduler admit method never spends
    /// the arm, so the hello handshake and the page read cannot consume it. A
    /// spent instance consumes nothing, so the drop cannot rearm.
    pub fn consume(&self, method: &str) -> bool {
        if method != ADMIT_METHOD {
            return false;
        }
        self.state
            .compare_exchange(
                QUALIFICATION_ARMING_ARMED,
                QUALIFICATION_ARMING_SPENT,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Current state value. Used by tests to assert the terminal invariant.
    pub fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }
}

impl Default for QualificationArming {
    fn default() -> Self {
        Self::new()
    }
}

/// The one process arming state, shared by every call in this process.
static ARMED: QualificationArming = QualificationArming::new();

/// Arm exactly one withheld reply for the admit method, for this process.
///
/// Returns true only for the first call. Because spent is terminal, later
/// calls cannot arm again, so one process can withhold at most one reply.
pub fn arm_single_reply_drop_for_admit() -> bool {
    ARMED.arm()
}

/// Consume the armed reply drop when it targets this method.
///
/// Returns true only when a drop was armed and this is the admit method, and
/// moves the state to spent in that same operation. Any other method returns
/// false and leaves the state untouched.
pub fn take_single_reply_drop_if_armed(method: &str) -> bool {
    ARMED.consume(method)
}

/// Marker the transport seam appends to its uncertain error message.
///
/// A live worker parses this to learn the exact number of frame bytes that were
/// withheld. The count is never invented: a missing or malformed tail makes the
/// parser return None and the witness is refused.
const WITHHELD_FRAME_BYTES_MARKER: &str = "withheld_frame_bytes=";

/// Parse the exact withheld frame length from a transport error message.
///
/// Returns None when the marker is absent, when the digits are malformed, or
/// when the count exceeds the transport frame bound. A fabricated zero is never
/// returned, so an unparsable message is distinguishable from a real count.
pub fn parse_withheld_frame_bytes(message: &str) -> Option<usize> {
    let (_, digits) = message.rsplit_once(WITHHELD_FRAME_BYTES_MARKER)?;
    if digits.is_empty() || !digits.bytes().all(|digit| digit.is_ascii_digit()) {
        return None;
    }
    let value = digits.parse::<usize>().ok()?;
    withheld_frame_length_is_bounded(value).then_some(value)
}

/// Report whether a withheld frame length fits the transport frame bound.
fn withheld_frame_length_is_bounded(value: usize) -> bool {
    value > 0 && value <= crate::IpcConfig::default().max_frame_bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arming_is_one_shot_because_spent_is_terminal() {
        // Each test owns its instance, so no shared static is asserted on and
        // parallel test execution cannot reorder the expectations.
        let instance = QualificationArming::new();
        assert!(instance.arm());
        assert!(!instance.arm(), "a second arm must be refused");
        assert!(instance.consume(ADMIT_METHOD));
        assert!(
            !instance.consume(ADMIT_METHOD),
            "a spent fault must never rearm"
        );
        assert_eq!(instance.state(), QUALIFICATION_ARMING_SPENT);
    }

    #[test]
    fn other_methods_never_consume_the_arm() {
        let instance = QualificationArming::new();
        assert!(instance.arm());
        assert!(!instance.consume("client.hello"));
        assert!(!instance.consume("automation.scheduler.page"));
        assert_eq!(instance.state(), QUALIFICATION_ARMING_ARMED);
        assert!(
            instance.consume(ADMIT_METHOD),
            "the arm survived unrelated calls"
        );
    }

    #[test]
    fn an_unarmed_instance_consumes_nothing() {
        let instance = QualificationArming::new();
        assert!(!instance.consume(ADMIT_METHOD));
        assert_eq!(instance.state(), QUALIFICATION_ARMING_NEVER);
    }

    #[test]
    fn default_instance_is_never_armed() {
        assert_eq!(
            QualificationArming::default().state(),
            QUALIFICATION_ARMING_NEVER
        );
    }
    #[test]
    fn a_present_frame_length_is_parsed_exactly() {
        let message = "qualified reply withheld at the transport seam; withheld_frame_bytes=4096";
        assert_eq!(parse_withheld_frame_bytes(message), Some(4096));
    }

    #[test]
    fn a_absent_frame_length_is_never_fabricated() {
        let message = "qualified reply withheld at the transport seam";
        assert_eq!(parse_withheld_frame_bytes(message), None);
    }

    #[test]
    fn a_malformed_frame_length_is_never_fabricated() {
        let message = "qualified reply withheld at the transport seam; withheld_frame_bytes=";
        assert_eq!(parse_withheld_frame_bytes(message), None);
        assert_eq!(parse_withheld_frame_bytes("withheld_frame_bytes=abc"), None);
        assert_eq!(
            parse_withheld_frame_bytes("withheld_frame_bytes=123junk"),
            None
        );
        assert_eq!(parse_withheld_frame_bytes("withheld_frame_bytes=0"), None);
    }

    #[test]
    fn an_oversized_frame_length_is_refused_rather_than_recorded() {
        let message = format!("withheld_frame_bytes={}", 1_048_576usize + 1);
        assert_eq!(parse_withheld_frame_bytes(&message), None);
    }

    #[test]
    fn a_default_frame_bound_accepts_a_normal_reply() {
        assert!(withheld_frame_length_is_bounded(1_048_576));
        assert!(!withheld_frame_length_is_bounded(1_048_577));
    }
}
