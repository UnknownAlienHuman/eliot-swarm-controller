//! Conversation-scoped UI state: selection, per-Agent drafts, mutation ids
//! and click-time control bindings.
//!
//! These are the section-4 correctness rules as pure logic, shared by the
//! WinUI screen and unit-tested here:
//!
//! * one ULID per user action; a retry reuses the stored id;
//! * an acknowledgement clears only the draft revision it submitted, so a
//!   late reply can never erase newer text;
//! * every control binds its Agent, generation and turn/request at click
//!   time; a later selection change cannot retarget it;
//! * reopening the app performs no stop and no duplicate submit: selection
//!   and drafts are local only, and nothing here issues commands.

use std::collections::HashMap;

/// Project/Agent/profile selection. Local only; changing it never touches a
/// running Agent.
#[derive(Clone, Debug, Default)]
pub struct Selection {
    /// Selected project registration id.
    pub project: Option<String>,
    /// Selected Agent id.
    pub agent: Option<String>,
    /// Selected profile name for the launch picker.
    pub profile: Option<String>,
}

impl Selection {
    /// Select another Agent. Drafts and transcripts are keyed by Agent id, so
    /// the previous conversation's text is preserved, not moved.
    pub fn select_agent(&mut self, agent: Option<String>) {
        self.agent = agent;
    }
}

/// One composer draft per Agent plus a revision counter. The UI submits the
/// revision alongside the text; the submit acknowledgement clears the draft
/// only when the revision still matches.
#[derive(Clone, Debug, Default)]
pub struct DraftStore {
    drafts: HashMap<String, (String, u64)>,
}

impl DraftStore {
    /// Replace the draft for one Agent, bumping its revision.
    pub fn set(&mut self, agent: &str, text: String) -> u64 {
        let revision = self.revision(agent).wrapping_add(1);
        self.drafts.insert(agent.to_owned(), (text, revision));
        revision
    }

    /// Current draft text, empty when the Agent has none.
    pub fn get(&self, agent: &str) -> &str {
        self.drafts
            .get(agent)
            .map(|(text, _)| text.as_str())
            .unwrap_or_default()
    }

    /// Current draft revision, zero when the Agent has no draft.
    pub fn revision(&self, agent: &str) -> u64 {
        self.drafts.get(agent).map(|(_, rev)| *rev).unwrap_or(0)
    }

    /// Clear the draft only when `submitted` is still the current revision.
    /// Returns true when the draft was cleared. A late reply carrying an
    /// older revision leaves newer text untouched.
    pub fn ack(&mut self, agent: &str, submitted: u64) -> bool {
        if self.revision(agent) == submitted {
            self.drafts.remove(agent);
            true
        } else {
            false
        }
    }
}

/// One mutation id per user action. The first click mints a ULID; a retry of
/// the same action key reuses it, so the daemon can deduplicate instead of
/// admitting a second effect. Completed actions release their key.
#[derive(Clone, Debug, Default)]
pub struct MutationLedger {
    ids: HashMap<String, String>,
}

impl MutationLedger {
    /// ULID for this action key, minting once and reusing afterwards.
    pub fn id_for(&mut self, key: &str) -> String {
        if let Some(id) = self.ids.get(key) {
            return id.clone();
        }
        let id = crate::client::new_mutation_id();
        self.ids.insert(key.to_owned(), id.clone());
        id
    }

    /// Release the key after the command settles. A later action mints fresh.
    pub fn complete(&mut self, key: &str) {
        self.ids.remove(key);
    }
}

/// Exact control binding captured at click time. The UI moves these values
/// into the background submission; whatever the selection becomes afterwards
/// cannot change them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundAction {
    /// Target Agent id.
    pub agent: String,
    /// Agent generation observed when the control was armed.
    pub generation: i64,
    /// Turn the control acts on, when the verb needs one.
    pub turn: Option<String>,
    /// Pending request the control answers, when it answers one.
    pub request: Option<String>,
}

impl BoundAction {
    pub fn turn_control(agent: &str, generation: i64, turn: &str) -> Self {
        Self {
            agent: agent.to_owned(),
            generation,
            turn: Some(turn.to_owned()),
            request: None,
        }
    }

    pub fn request_control(agent: &str, generation: i64, request: &str) -> Self {
        Self {
            agent: agent.to_owned(),
            generation,
            turn: None,
            request: Some(request.to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_clears_only_the_submitted_revision() {
        let mut drafts = DraftStore::default();
        let first = drafts.set("a1", "hello".to_owned());
        assert!(drafts.ack("a1", first));
        assert_eq!(drafts.get("a1"), "");
    }

    #[test]
    fn late_ack_keeps_newer_draft() {
        let mut drafts = DraftStore::default();
        let first = drafts.set("a1", "hello".to_owned());
        let _second = drafts.set("a1", "hello, edited".to_owned());
        assert!(!drafts.ack("a1", first));
        assert_eq!(drafts.get("a1"), "hello, edited");
    }

    #[test]
    fn drafts_are_per_agent() {
        let mut drafts = DraftStore::default();
        drafts.set("a1", "one".to_owned());
        drafts.set("a2", "two".to_owned());
        assert_eq!(drafts.get("a1"), "one");
        assert_eq!(drafts.get("a2"), "two");
    }

    #[test]
    fn ledger_reuses_id_for_retry() {
        let mut ledger = MutationLedger::default();
        let first = ledger.id_for("send:a1:3");
        let retry = ledger.id_for("send:a1:3");
        assert_eq!(first, retry);
    }

    #[test]
    fn ledger_mints_fresh_id_after_complete() {
        let mut ledger = MutationLedger::default();
        let first = ledger.id_for("send:a1:3");
        ledger.complete("send:a1:3");
        let second = ledger.id_for("send:a1:3");
        assert_ne!(first, second);
    }

    #[test]
    fn bound_action_is_value_captured() {
        let bound = BoundAction::turn_control("a1", 7, "t1");
        let moved = bound.clone();
        assert_eq!(bound, moved);
        assert_eq!(moved.generation, 7);
    }
}
