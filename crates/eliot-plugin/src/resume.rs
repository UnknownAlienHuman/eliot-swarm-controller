//! Shared resume preflight for the stream-owning harness plugins.
//!
//! A resume may adopt a retained native session only when the native readback
//! proves it is resumable by this new generation: an idle/not-loaded runtime,
//! no active turn the vendor exposes, and — when the generation retained a
//! turn — the newest native turn is exactly that retained turn and is
//! terminal. A retained generation with no turn needs vendor proof that no
//! native turn ran. Each plugin translates its own readback into
//! [`ResumePreflight`]; the decision and its diagnostic wording live here.
//!
//! This is not proof the previous host generation is gone. Eliot guarantees
//! the old generation is terminal before a plugin runs (Store admission and
//! dispatch), so the preflight only checks the retained subject is resumable.

use eliot_core::{Error, Result};

/// Native evidence one plugin reads back before adopting a retained subject.
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
    /// Newest native turn as `(turn id, terminal)`, when one is readable.
    pub newest_turn: Option<(&'a str, bool)>,
    /// The vendor reported an active turn.
    pub active_turn: bool,
    /// The native readback proves the retained subject ran no native turns.
    pub proves_no_turn: bool,
}

impl ResumePreflight<'_> {
    /// Accept only a resumable retained subject with a matching terminal
    /// turn (or proven no-turn). Every rejection stays `native_readback`.
    pub fn verify(self) -> Result<()> {
        let ResumePreflight {
            vendor,
            subject,
            runtime_resumable,
            retained_turn,
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
            (Some(expected), Some((actual, true))) if actual == expected => Ok(()),
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
