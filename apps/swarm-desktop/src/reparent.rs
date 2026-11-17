//! Change an Agent's manager from the monitor (`swarm.reparent`).
//!
//! The binding is captured from the rows on screen when the button is
//! built: the exact source and destination generations go into the request,
//! so a click acts on exactly what was displayed. One ULID per action key,
//! reused only after an uncertain reply. The daemon owns every validation
//! (cycle, fanout, observe-only, generation, destination state); this module
//! shows its result.

use eliot_core::AgentState;
use eliot_core::api::{ReparentRequest, ReparentResponse};

use crate::monitor::MonitorRow;
use eliot_core::api::agent_state_name;

/// The two `swarm.reparent` source forms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MoveForm {
    /// The Agent and its whole subtree.
    Agent,
    /// Every direct child of the Agent (with subtrees); it stays put.
    Children,
}

/// A manager change bound to the generations shown at click time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoveBinding {
    pub form: MoveForm,
    pub source: String,
    pub source_generation: i64,
    pub parent: String,
    pub parent_generation: i64,
}

impl MoveBinding {
    /// Ledger key: the same action (form, both Agents and generations)
    /// reuses its mutation id; any difference is a new action.
    pub fn key(&self) -> String {
        format!(
            "reparent:{:?}:{}:{}:{}:{}",
            self.form, self.source, self.source_generation, self.parent, self.parent_generation
        )
    }

    pub fn request(&self, id: String) -> ReparentRequest {
        let (agent, children_of) = match self.form {
            MoveForm::Agent => (Some(self.source.clone()), None),
            MoveForm::Children => (None, Some(self.source.clone())),
        };
        ReparentRequest {
            id,
            agent,
            children_of,
            expected_generation: self.source_generation,
            parent: self.parent.clone(),
            parent_generation: self.parent_generation,
        }
    }

    pub fn describe(&self) -> String {
        match self.form {
            MoveForm::Agent => format!(
                "{} (run {}) and its subtree to {} (run {})",
                self.source, self.source_generation, self.parent, self.parent_generation
            ),
            MoveForm::Children => format!(
                "all children of {} (run {}) to {} (run {})",
                self.source, self.source_generation, self.parent, self.parent_generation
            ),
        }
    }
}

/// Possible new managers: every other shown row that is live. The daemon
/// still decides; a cycle or a full manager is its refusal to report.
pub fn candidates<'a>(rows: &'a [MonitorRow], selected: &str) -> Vec<&'a MonitorRow> {
    rows.iter()
        .filter(|row| row.id != selected)
        .filter(|row| {
            !matches!(
                row.state,
                AgentState::Stopped | AgentState::Failed | AgentState::Lost | AgentState::Unknown
            )
        })
        .collect()
}

pub fn candidate_label(row: &MonitorRow) -> String {
    format!(
        "{} · {} · {} · generation {}",
        row.id,
        agent_state_name(row.state),
        row.harness,
        row.generation
    )
}

/// Every moved row, or the empty all-children result.
pub fn result_lines(binding: &MoveBinding, reply: &ReparentResponse) -> Vec<String> {
    let mut lines = vec![format!("Moved {} ({}).", binding.describe(), reply.id)];
    if reply.moved.is_empty() {
        lines.push(format!("{} had no children to move.", binding.source));
    }
    for moved in &reply.moved {
        lines.push(format!(
            "{} gen {}: {} → {} · root {} · depth {}",
            moved.agent,
            moved.generation,
            moved.previous_parent.as_deref().unwrap_or("no manager"),
            moved.parent.as_deref().unwrap_or("no manager"),
            moved.root,
            moved.depth
        ));
    }
    lines
}

/// The daemon's refusal with what its code means for a manager change.
pub fn refusal_line(binding: &MoveBinding, error: &eliot_core::Error) -> String {
    format!(
        "Not moved: {} — {}",
        binding.describe(),
        error.user_message()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::ReparentedAgent;

    fn binding(form: MoveForm) -> MoveBinding {
        MoveBinding {
            form,
            source: "a2".to_owned(),
            source_generation: 1,
            parent: "a5".to_owned(),
            parent_generation: 3,
        }
    }

    #[test]
    fn request_carries_one_source_form_and_both_generations() {
        let request = binding(MoveForm::Agent).request("01ID".to_owned());
        assert_eq!(request.agent.as_deref(), Some("a2"));
        assert!(request.children_of.is_none());
        assert_eq!(
            (request.expected_generation, request.parent_generation),
            (1, 3)
        );
        request.validate().unwrap();
        let request = binding(MoveForm::Children).request("01ID".to_owned());
        assert_eq!(request.children_of.as_deref(), Some("a2"));
        assert!(request.agent.is_none());
        request.validate().unwrap();
    }

    #[test]
    fn keys_differ_by_form_and_generation() {
        let agent = binding(MoveForm::Agent);
        let mut later = agent.clone();
        later.parent_generation = 4;
        assert_ne!(agent.key(), binding(MoveForm::Children).key());
        assert_ne!(agent.key(), later.key());
        assert_eq!(agent.key(), binding(MoveForm::Agent).key());
    }

    #[test]
    fn results_and_refusals_keep_the_daemon_facts() {
        let reply = ReparentResponse {
            id: "01ID".to_owned(),
            moved: vec![ReparentedAgent {
                agent: "a2".to_owned(),
                generation: 1,
                previous_parent: Some("a1".to_owned()),
                parent: Some("a5".to_owned()),
                root: "a5".to_owned(),
                depth: 1,
            }],
        };
        let lines = result_lines(&binding(MoveForm::Agent), &reply);
        assert!(lines[1].contains("a2 gen 1: a1 → a5 · root a5 · depth 1"));
        let empty = ReparentResponse {
            id: "01ID".to_owned(),
            moved: Vec::new(),
        };
        assert!(result_lines(&binding(MoveForm::Children), &empty)[1].contains("no children"));
        let line = refusal_line(
            &binding(MoveForm::Agent),
            &eliot_core::Error::new("stale_generation", "internal generation details"),
        );
        assert!(line.ends_with(
            "The Agent started a new run; refresh and try again: internal generation details"
        ));
        assert!(!line.contains("stale_generation"));
        for (code, message) in [
            ("invalid", "reparent would create a cycle"),
            ("not_allowed", "observe-only Agent cannot be reparented"),
            ("bad_state", "new parent is not a live manager"),
        ] {
            assert!(
                refusal_line(
                    &binding(MoveForm::Agent),
                    &eliot_core::Error::new(code, message)
                )
                .ends_with(message)
            );
        }
    }
}
