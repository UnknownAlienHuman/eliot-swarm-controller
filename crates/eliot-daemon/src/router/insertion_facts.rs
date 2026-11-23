use super::{Router, goal_scope::outside_scope};
use crate::git_monitor::{canonical_root, normalize_hint, reject_links};
use eliot_core::{ContextEditPath, Event, MAX_CONTEXT_EDIT_PATH_BYTES, Result};
use serde_json::Value;
use std::{collections::BTreeSet, fs, path::Path};

const MAX_AGENT_EDITS_READ: usize = 1_000;

pub(super) enum InsertionFact {
    Edits {
        kind: &'static str,
        paths: Vec<ContextEditPath>,
    },
    NoCheckBeforeEnd,
}

impl Router {
    pub(super) async fn insertion_facts(&self, event: &Event) -> Result<Vec<InsertionFact>> {
        if !matches!(
            event.kind.as_str(),
            "hook.after_tool"
                | "tool.file.edited"
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

        let policy = self.secret_policy_snapshot();
        if let Some(workspace) = workspace {
            if event.source == "stream"
                && event.native_id.is_some()
                && matches!(event.kind.as_str(), "hook.after_tool" | "tool.file.edited")
                && let Ok(ContextEditPath::OutsideOrUnverifiable {
                    sha256,
                    canonical_sha256: _,
                    basename,
                    path,
                    path_redacted,
                    ..
                }) = serde_json::from_value(event.data["edit_path"].clone())
                && valid_sha256(&sha256)
                && !basename.contains(['/', '\\'])
                && !basename.chars().any(char::is_control)
            {
                let path = path.map(|path| eliot_plugin::edit_path::display_outside(&path));
                let path_redacted = path_redacted
                    || path
                        .as_deref()
                        .is_some_and(|path| policy.redact_text(path) != path);
                let path = path.map(|path| policy.redact_text(&path)).filter(|path| {
                    !path.is_empty()
                        && path.len() <= MAX_CONTEXT_EDIT_PATH_BYTES
                        && !path.chars().any(char::is_control)
                });
                let canonical_sha256 = if path_redacted {
                    None
                } else {
                    path.as_deref()
                        .and_then(|path| eliot_plugin::edit_path::resolve_display(workspace, path))
                        .and_then(|path| {
                            verified_outside_path_key(workspace, &path.to_string_lossy())
                        })
                };
                outside_worktree.insert(ContextEditPath::OutsideOrUnverifiable {
                    sha256,
                    canonical_sha256,
                    basename: eliot_core::api::truncate_bytes(&policy.redact_text(&basename), 240)
                        .0
                        .to_owned(),
                    path,
                    path_redacted,
                });
            }
            for raw_path in event_edit_paths(event) {
                if raw_path.trim().is_empty() {
                    continue;
                }
                if raw_path.len() <= 4096
                    && let Ok(path) = normalize_hint(workspace, raw_path)
                {
                    changed_paths.insert(path);
                } else {
                    let path = Path::new(workspace).join(raw_path);
                    let path_text = path.to_string_lossy();
                    let mut edit = eliot_plugin::edit_path::outside_evidence(&path_text, &policy);
                    if let ContextEditPath::OutsideOrUnverifiable {
                        canonical_sha256,
                        path_redacted,
                        ..
                    } = &mut edit
                    {
                        *canonical_sha256 = if *path_redacted {
                            None
                        } else {
                            verified_outside_path_key(workspace, &path_text)
                        };
                    }
                    outside_worktree.insert(edit);
                }
            }

            for raw_path in git_event_paths(event, workspace) {
                if let Ok(path) = normalize_hint(workspace, raw_path) {
                    changed_paths.insert(path);
                }
            }
        }

        let mut facts = Vec::new();
        if !outside_worktree.is_empty() {
            facts.push(InsertionFact::Edits {
                kind: "outside_worktree",
                paths: outside_worktree.into_iter().collect(),
            });
        }

        if let Some(definition) = launch_goal
            .as_ref()
            .filter(|goal| !goal.scope.is_empty() || !goal.policy_scope.is_empty())
        {
            let paths = outside_scope(definition, &changed_paths)
                .into_iter()
                .map(|path| ContextEditPath::Relative {
                    path: policy.redact_text(path),
                })
                .collect::<Vec<_>>();
            if !paths.is_empty() {
                facts.push(InsertionFact::Edits {
                    kind: "outside_scope",
                    paths,
                });
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
            // No upper bound: a Goal round commits only while the Agent is
            // idle, so its check always lands after this turn and covers the
            // turn's final files. Bounding it by `completed_at` would report a
            // finished round for this idle point as a missing check.
            let check_ran = check_observed
                || goal
                    .counters
                    .last_check_at
                    .is_some_and(|checked_at| checked_at >= started_at);
            if edited && !check_ran && !self.goal_auto_round_will_check(agent_id, generation).await
            {
                facts.push(InsertionFact::NoCheckBeforeEnd);
            }
        }

        Ok(facts)
    }
}

fn event_edit_paths(event: &Event) -> Vec<&str> {
    if event.source != "stream" || event.native_id.is_none() {
        return Vec::new();
    }
    match event.kind.as_str() {
        "hook.after_tool" | "tool.file.edited" => {
            let path = &event.data["edit_path"];
            path.as_str()
                .or_else(|| {
                    (path["kind"] == "relative")
                        .then(|| path["path"].as_str())
                        .flatten()
                })
                .into_iter()
                .collect()
        }
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

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn verified_outside_path_key(workspace: &str, candidate: &str) -> Option<String> {
    let candidate_path = Path::new(candidate);
    if candidate.contains('\0')
        || candidate_path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
    {
        return None;
    }
    let Ok(root) = canonical_root(workspace) else {
        return None;
    };
    let candidate_path = if candidate_path.is_absolute() {
        candidate_path.to_path_buf()
    } else {
        root.join(candidate_path)
    };
    if reject_links(&candidate_path).is_err() {
        return None;
    }
    let Ok(candidate) = fs::canonicalize(candidate_path) else {
        return None;
    };
    let root = root.to_string_lossy();
    let candidate = candidate.to_string_lossy();
    let root_key = eliot_core::workspace_path_key(root.as_ref());
    let candidate_key = eliot_core::workspace_path_key(candidate.as_ref());
    if path_key_is_within(root_key.as_ref(), candidate_key.as_ref()) {
        None
    } else {
        Some(eliot_core::api::inference::sha256_hex(
            candidate_key.as_bytes(),
        ))
    }
}

fn path_key_is_within(root: &str, candidate: &str) -> bool {
    let root = root.trim_end_matches('/');
    if root.is_empty() {
        return candidate.starts_with('/');
    }
    candidate == root
        || candidate
            .strip_prefix(root)
            .is_some_and(|remainder| remainder.starts_with('/'))
}
