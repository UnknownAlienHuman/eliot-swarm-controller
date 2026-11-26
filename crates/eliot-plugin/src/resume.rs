//! Shared resume preflight for the stream-owning harness plugins.
//!
//! A resume may adopt a retained native session only when the native readback
//! proves it is resumable by this new generation: an idle/not-loaded runtime,
//! no active turn the vendor exposes, and — when the generation retained a
//! turn — the newest native turn is exactly that retained turn and is
//! terminal, except that an exact match with a retained `unknown` turn (for
//! example one interrupted by daemon death) also resumes: the resuming
//! generation continues with new input only, the retained unknown turn stays
//! unknown for Owner resolution, and the interrupted effect is never replayed.
//! A retained generation with no turn needs vendor proof that no
//! native turn ran. Each plugin translates its own readback into
//! [`ResumePreflight`]; the decision and its diagnostic wording live here.
//!
//! This is not proof the previous host generation is gone. Eliot guarantees
//! the old generation is terminal before a plugin runs (Store admission and
//! dispatch), so the preflight only checks the retained subject is resumable.

use eliot_core::{Error, Result};

/// Native evidence one plugin reads back before adopting a retained subject.
/// Each plugin reports the native fact — the newest native turn id and its
/// native state string — and the shared verifier below decides.
pub struct ResumePreflight<'a> {
    /// Vendor display name used in diagnostics (`"Codex"`, `"Muse"`).
    pub vendor: &'static str,
    /// Retained object the vendor names in diagnostics (`"thread"`,
    /// `"session"`).
    pub subject: &'static str,
    /// The adapter recognized a runtime state that can be resumed.
    pub runtime_resumable: bool,
    /// Exact retained turn id, when the generation retained one.
    pub retained_turn: Option<&'a str>,
    /// True when the retained turn is itself `unknown` (for example a turn
    /// interrupted by daemon death). An exact native id match then resumes
    /// for new input only; the retained turn stays unknown and is never
    /// replayed.
    pub retained_unknown: bool,
    /// Newest native turn as `(turn id, native state)`, when one is readable.
    pub newest_turn: Option<(&'a str, &'a str)>,
    /// The vendor reported an active turn.
    pub active_turn: bool,
    /// The native readback proves the retained subject ran no native turns.
    pub proves_no_turn: bool,
}

/// Native terminal states per vendor. Anything else (for example an
/// in-progress turn) is not terminal.
fn is_terminal(vendor: &str, state: &str) -> bool {
    match vendor {
        "Muse" => matches!(state, "completed" | "failed" | "cancelled"),
        "Codex" => matches!(state, "completed" | "interrupted" | "failed"),
        _ => matches!(state, "completed" | "failed" | "cancelled" | "interrupted"),
    }
}

impl ResumePreflight<'_> {
    /// Accept only a resumable retained subject with a matching terminal
    /// turn (or proven no-turn), plus the exact-match retained-`unknown`
    /// continuation. Every rejection stays `native_readback`.
    pub fn verify(self) -> Result<()> {
        let ResumePreflight {
            vendor,
            subject,
            runtime_resumable,
            retained_turn,
            retained_unknown,
            newest_turn,
            active_turn,
            proves_no_turn,
        } = self;
        if !runtime_resumable {
            return Err(Error::new(
                "native_readback",
                format!("{vendor} retained {subject} is active or has no resumable runtime status"),
            ));
        }
        if active_turn {
            return Err(Error::new(
                "native_readback",
                format!("{vendor} retained {subject} still has an active turn"),
            ));
        }
        match (retained_turn, newest_turn) {
            (Some(expected), Some((actual, state)))
                if actual == expected && (is_terminal(vendor, state) || retained_unknown) =>
            {
                Ok(())
            }
            (None, None) if proves_no_turn => Ok(()),
            (None, None) => Err(Error::new(
                "native_readback",
                format!("{vendor} cannot prove the retained {subject} ran no native turns"),
            )),
            _ => Err(Error::new(
                "native_readback",
                format!(
                    "{vendor} newest native turn does not match the exact retained terminal turn"
                ),
            )),
        }
    }
}
