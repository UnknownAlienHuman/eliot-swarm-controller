//! "Move under another task…" (`swarm.reparent`, `docs/spec/api.md`
//! Handoff methods): the task's menus open a picker of the tasks it can
//! work under; one confirmation moves it with its own workers.
//!
//! The binding is captured when the owner confirms: the exact source and
//! destination generations go into the request, so a click acts on exactly
//! what was displayed. One ULID per action key, reused only after an
//! uncertain reply. The picker only leaves out what the daemon would
//! refuse anyway (another project, an ended or observed task, the task
//! itself or its own workers); the daemon still owns every check (cycle,
//! fanout, observe-only, generation, destination state) and its refusal is
//! shown in words.

use eliot_core::AgentState;
use eliot_core::api::ReparentRequest;

use crate::nav::UiAgent;

/// A move of one Agent (with its subtree) under a new parent, bound to the
/// generations shown at click time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MoveBinding {
    pub source: String,
    pub source_generation: i64,
    pub parent: String,
    pub parent_generation: i64,
}

impl MoveBinding {
    /// Ledger key: the same move (both Agents and generations) reuses its
    /// mutation id; any difference is a new action.
    pub fn key(&self) -> String {
        format!(
            "reparent:{}:{}:{}:{}",
            self.source, self.source_generation, self.parent, self.parent_generation
        )
    }

    pub fn request(&self, id: String) -> ReparentRequest {
        ReparentRequest {
            id,
            agent: Some(self.source.clone()),
            children_of: None,
            expected_generation: self.source_generation,
            parent: self.parent.clone(),
            parent_generation: self.parent_generation,
        }
    }
}

/// The tasks `source` can be moved under, newest first: the same project
/// (the daemon compares the project label exactly), controlled and not
/// ended (`swarm.reparent` needs a live manager), neither `source` itself,
/// its current parent, nor one of its own workers (a cycle), each once.
pub fn eligible_parents<'a>(agents: &'a [UiAgent], source: &UiAgent) -> Vec<&'a UiAgent> {
    let parents: std::collections::HashMap<&str, Option<&str>> = agents
        .iter()
        .map(|agent| (agent.id.as_str(), agent.parent.as_deref()))
        .collect();
    // Whether `id` sits anywhere under `source` (bounded: links form a
    // tree, but rows read on different pages may disagree).
    let under_source = |id: &str| {
        let mut current = parents.get(id).copied().flatten();
        for _ in 0..parents.len() {
            match current {
                Some(parent) if parent == source.id => return true,
                Some(parent) => current = parents.get(parent).copied().flatten(),
                None => return false,
            }
        }
        false
    };
    let mut seen = std::collections::HashSet::new();
    let mut eligible: Vec<&UiAgent> = agents
        .iter()
        .filter(|agent| agent.id != source.id && seen.insert(agent.id.as_str()))
        .filter(|agent| agent.project == source.project && !agent.native_child)
        .filter(|agent| {
            !matches!(
                agent.state,
                AgentState::Stopped | AgentState::Failed | AgentState::Lost | AgentState::Unknown
            )
        })
        .filter(|agent| source.parent.as_deref() != Some(agent.id.as_str()))
        .filter(|agent| !under_source(&agent.id))
        .collect();
    eligible.sort_by(|a, b| b.updated_at.cmp(&a.updated_at).then(a.id.cmp(&b.id)));
    eligible
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(id: &str, parent: Option<&str>, state: AgentState, updated_at: i64) -> UiAgent {
        UiAgent {
            id: id.to_owned(),
            generation: 1,
            harness: "claude".to_owned(),
            state,
            project: "/w".to_owned(),
            parent: parent.map(str::to_owned),
            updated_at,
            title: id.to_owned(),
            native_child: false,
            tokens: None,
            chat: false,
            archived: false,
        }
    }

    #[test]
    fn request_moves_the_agent_with_both_generations() {
        let binding = MoveBinding {
            source: "a2".to_owned(),
            source_generation: 1,
            parent: "a5".to_owned(),
            parent_generation: 3,
        };
        let request = binding.request("01ID".to_owned());
        assert_eq!(request.agent.as_deref(), Some("a2"));
        assert!(request.children_of.is_none());
        assert_eq!(
            (request.expected_generation, request.parent_generation),
            (1, 3)
        );
        request.validate().unwrap();
        let mut later = binding.clone();
        later.parent_generation = 4;
        assert_ne!(binding.key(), later.key());
    }

    #[test]
    fn only_live_controlled_tasks_of_the_same_project_outside_the_subtree_qualify() {
        let source = agent("a2", Some("a1"), AgentState::Idle, 5);
        let mut other_project = agent("a9", None, AgentState::Idle, 9);
        other_project.project = "/elsewhere".to_owned();
        let mut native = agent("a8", None, AgentState::Busy, 8);
        native.native_child = true;
        let agents = vec![
            agent("a1", None, AgentState::Busy, 1),
            source.clone(),
            agent("a3", Some("a2"), AgentState::Idle, 3),
            agent("a4", Some("a3"), AgentState::Busy, 4),
            agent("a5", None, AgentState::Idle, 6),
            agent("a6", None, AgentState::Lost, 7),
            agent("a7", None, AgentState::Waiting, 10),
            agent("a5", None, AgentState::Idle, 6),
            native,
            other_project,
        ];
        let ids: Vec<&str> = eligible_parents(&agents, &source)
            .iter()
            .map(|agent| agent.id.as_str())
            .collect();
        // Not itself, its current parent a1, its workers a3/a4, the lost
        // a6, the native a8 or the other project's a9; newest first, once.
        assert_eq!(ids, ["a7", "a5"]);
    }
}
