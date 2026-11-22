use super::{Router, goal_scope::outside_scope};
use crate::git_monitor::normalize_hint;
use eliot_core::{Event, Result, api::truncate_bytes};
use serde_json::Value;
use std::collections::BTreeSet;

const MAX_FACTS: usize = 3;
const MAX_PATHS_PER_FACT: usize = 8;
const MAX_NOTE_BYTES: usize = 1_536;
const MAX_PATH_BYTES: usize = 384;
const MAX_AGENT_EDITS_READ: usize = 1_000;

impl Router {
    pub(super) async fn insertion_facts(&self, event: &Event) -> Result<Vec<(String, String)>> {
        if !matches!(
            event.kind.as_str(),
            "hook.after_tool"
                | "tool.completed"
                | "file.changed"
                | "git.changed"
                | "turn.completed"
        ) || (event.kind == "tool.completed" && event_edit_paths(event).is_empty())
        {
            return Ok(Vec::new());
        }
        let Some(agent_id) = event.agent.as_deref() else {
            return Ok(Vec::new());
        };
        let Some(generation) = event.generation else {
            return Ok(Vec::new());
        };

        let (agent, launch_goal, goal, edits, turn, evidence) = {
            let store = self.inner.store.lock().await;
            let Some(agent) = store.get_agent(agent_id)? else {
                return Ok(Vec::new());
            };
            if agent.generation != generation {
                return Ok(Vec::new());
            }
            let launch_goal = store.launch_goal_definition(agent_id, generation)?;
            let goal = store.goal_get(agent_id, "eliot")?;
            let terminal = event.kind == "turn.completed";
            let edits = if terminal {
                store.list_agent_edits(agent_id, generation, None, MAX_AGENT_EDITS_READ)?
            } else {
                Vec::new()
            };
            let turn = if terminal {
                event
                    .data
                    .get("turn_id")
                    .and_then(Value::as_str)
                    .map(|turn_id| store.get_turn(agent_id, generation, turn_id))
                    .transpose()?
                    .flatten()
            } else {
                None
            };
            let evidence = match (&turn, &launch_goal) {
                (Some(turn), Some(goal)) if !goal.check.is_empty() => {
                    store.turn_insertion_evidence(turn, event.seq, &goal.check)?
                }
                _ => (Vec::new(), false),
            };
            (agent, launch_goal, goal, edits, turn, evidence)
        };

        let workspace = agent.workspace.as_deref();
        let mut outside_worktree = BTreeSet::new();
        let mut changed_paths = BTreeSet::new();

        if let Some(workspace) = workspace {
            for raw_path in event_edit_paths(event) {
                if raw_path.is_empty() || raw_path.len() > 4_096 {
                    continue;
                }
                match normalize_hint(workspace, raw_path) {
                    Ok(path) => {
                        changed_paths.insert(path);
                    }
                    Err(error) if error.message.contains("escapes its worktree") => {
                        outside_worktree.insert(bounded_path(self, raw_path));
                    }
                    Err(_) => {}
                }
            }

            for raw_path in git_event_paths(event, workspace) {
                if let Ok(path) = normalize_hint(workspace, raw_path) {
                    changed_paths.insert(path);
                }
            }
        }

        let mut facts = Vec::with_capacity(MAX_FACTS);
        if !outside_worktree.is_empty() {
            facts.push((
                "outside_worktree".to_owned(),
                paths_note(
                    self,
                    "Native edit path is outside the Agent worktree",
                    &outside_worktree,
                ),
            ));
        }

        if let Some(definition) = launch_goal
            .as_ref()
            .filter(|goal| !goal.scope.is_empty() || !goal.policy_scope.is_empty())
        {
            let outside = outside_scope(definition, &changed_paths)
                .into_iter()
                .map(|path| bounded_path(self, path))
                .collect::<BTreeSet<_>>();
            if !outside.is_empty() {
                facts.push((
                    "outside_scope".to_owned(),
                    paths_note(self, "Edit is outside the frozen Goal scope", &outside),
                ));
            }
        }

        if event.kind == "turn.completed"
            && let (Some(definition), Some(goal), Some(turn)) =
                (launch_goal.as_ref(), goal.as_ref(), turn.as_ref())
            && !definition.check.is_empty()
            && goal.agent == agent_id
            && goal.generation == generation
            && goal.definition == *definition
            && let Some(started_at) = turn.started_at
        {
            let completed_at = turn.completed_at.unwrap_or(event.ts);
            let (native_edits, check_observed) = evidence;
            let edited = workspace.is_some_and(|root| {
                native_edits
                    .iter()
                    .any(|path| normalize_hint(root, path).is_ok())
            }) || edits.iter().any(|edit| {
                workspace == Some(edit.workspace.as_str())
                    && edit.updated_at >= started_at
                    && edit.updated_at <= completed_at
            });
            let check_ran = check_observed
                || goal.counters.last_check_at.is_some_and(|checked_at| {
                    checked_at >= started_at && checked_at <= completed_at
                });
            if edited && !check_ran {
                facts.push((
                    "no_check_before_end".to_owned(),
                    "The turn changed files but no configured Goal check is recorded before the turn ended."
                        .to_owned(),
                ));
            }
        }

        facts.truncate(MAX_FACTS);
        for (_, text) in &mut facts {
            *text = truncate_bytes(text, MAX_NOTE_BYTES).0.to_owned();
        }
        Ok(facts)
    }
}

fn event_edit_paths(event: &Event) -> Vec<&str> {
    if event.source != "stream" || event.native_id.is_none() {
        return Vec::new();
    }
    match event.kind.as_str() {
        "hook.after_tool" => event
            .data
            .get("edit_path")
            .and_then(Value::as_str)
            .into_iter()
            .collect(),
        "tool.completed"
            if event.data.get("tool").and_then(Value::as_str) == Some("fileChange")
                && event.data.get("status").and_then(Value::as_str) == Some("completed")
                && event.data.get("redacted").and_then(Value::as_bool) != Some(true) =>
        {
            event
                .data
                .get("changed_paths")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect()
        }
        _ => Vec::new(),
    }
}

fn git_event_paths<'a>(event: &'a Event, workspace: &str) -> Vec<&'a str> {
    if event.source != "git"
        || !matches!(event.kind.as_str(), "file.changed" | "git.changed")
        || event.data.get("workspace").and_then(Value::as_str) != Some(workspace)
        || event
            .subject
            .as_ref()
            .and_then(|subject| subject.workspace.as_deref())
            .is_some_and(|subject_workspace| subject_workspace != workspace)
    {
        return Vec::new();
    }

    if event.kind == "file.changed" {
        return event
            .data
            .get("path")
            .and_then(Value::as_str)
            .into_iter()
            .collect();
    }

    event
        .data
        .get("dirty")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            entry
                .get("path")
                .and_then(Value::as_str)
                .or_else(|| entry.as_str())
        })
        .collect()
}

fn bounded_path(router: &Router, path: &str) -> String {
    let redacted = router.secret_policy_snapshot().redact_text(path);
    truncate_bytes(&redacted, MAX_PATH_BYTES).0.to_owned()
}

fn paths_note(router: &Router, title: &str, paths: &BTreeSet<String>) -> String {
    let mut note = format!("{title} ({}):", paths.len());
    for path in paths.iter().take(MAX_PATHS_PER_FACT) {
        note.push_str("\n- ");
        note.push_str(&bounded_path(router, path));
    }
    if paths.len() > MAX_PATHS_PER_FACT {
        note.push_str(&format!(
            "\n- … and {} more",
            paths.len() - MAX_PATHS_PER_FACT
        ));
    }
    truncate_bytes(&note, MAX_NOTE_BYTES).0.to_owned()
}
