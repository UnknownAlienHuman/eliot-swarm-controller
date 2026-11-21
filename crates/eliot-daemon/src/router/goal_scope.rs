//! Change scope of a Goal. A Goal with `scope` or `check_inputs` gets a
//! starting point at `set`: HEAD and the content of every path that already
//! differed from it. Each round compares the workspace with that starting
//! point, so pre-existing work is never attributed to the Goal. A change
//! outside `scope` blocks the Goal before its check. A passing check after a
//! check input changed, or with the change set unverified, blocks it instead
//! of completing it. Swarm never reverts or deletes anything here.

use super::Router;
use crate::git_monitor::{GitProbe, scope_digests};
use eliot_core::{
    Error, Result,
    api::{
        GoalDefinition, GoalScopeBase, MAX_GOAL_SCOPE_BASE_PATHS, claim_glob_matches_normalized,
        normalize_claim_glob, truncate_bytes,
    },
};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

/// Most paths one scope finding names; the rest are counted.
const LISTED_PATHS: usize = 20;
/// Room a scope finding keeps in the 4 KiB Goal observation.
const FINDING_BYTES: usize = 1536;

/// The Goal's work since its starting point, or why it is not known.
pub(super) enum ScopeFinding {
    Verified(BTreeSet<String>),
    Unverified(String),
}

impl Router {
    /// Record the starting point of a Goal with `scope` or `check_inputs`.
    pub(super) async fn goal_scope_base(
        &self,
        agent: &str,
        definition: &GoalDefinition,
    ) -> Result<Option<GoalScopeBase>> {
        if !definition.tracks_changes() {
            return Ok(None);
        }
        let workspace = self
            .inner
            .store
            .lock()
            .await
            .get_agent(agent)?
            .and_then(|agent| agent.workspace)
            .ok_or_else(|| Error::new("invalid", "a scoped Goal needs an Agent workspace"))?;
        let unavailable = |error: Error| {
            Error::new(
                "unavailable",
                format!(
                    "the Goal's starting point cannot be recorded: {}",
                    error.message
                ),
            )
        };
        let state = GitProbe::new()
            .map_err(unavailable)?
            .scope_state(&workspace)
            .await
            .map_err(unavailable)?;
        if state.changed.len() > MAX_GOAL_SCOPE_BASE_PATHS {
            return Err(Error::new(
                "invalid",
                format!(
                    "{} paths already differ from HEAD; a scoped Goal records at most {MAX_GOAL_SCOPE_BASE_PATHS}",
                    state.changed.len()
                ),
            ));
        }
        let check_files = check_files(&state.root, definition, &workspace);
        let root = state.root.clone();
        let changed = tokio::task::spawn_blocking(move || {
            scope_digests(&root, state.changed.iter().map(String::as_str))
        })
        .await
        .map_err(|_| Error::new("internal", "scope read task failed"))?
        .map_err(unavailable)?;
        Ok(Some(GoalScopeBase {
            root: state
                .root
                .to_str()
                .ok_or_else(|| Error::new("unavailable", "workspace is not UTF-8"))?
                .to_owned(),
            head: state.head,
            changed,
            check_files,
        }))
    }

    /// Paths the Goal's work changed since `base`: committed since its HEAD,
    /// changed now, or a pre-existing change whose content moved.
    pub(super) async fn goal_scope_changes(
        &self,
        workspace: Option<&str>,
        base: &GoalScopeBase,
    ) -> ScopeFinding {
        match scope_changes(workspace, base).await {
            Ok(changed) => ScopeFinding::Verified(changed),
            Err(error) => ScopeFinding::Unverified(*error.message),
        }
    }
}

async fn scope_changes(workspace: Option<&str>, base: &GoalScopeBase) -> Result<BTreeSet<String>> {
    let workspace =
        workspace.ok_or_else(|| Error::new("unavailable", "the Agent has no workspace"))?;
    let git = GitProbe::new()?;
    let state = git.scope_state(workspace).await?;
    if state.root != Path::new(&base.root) {
        return Err(Error::new(
            "unavailable",
            "the workspace is not the Git root recorded at goal set",
        ));
    }
    let committed = match (&base.head, &state.head) {
        (Some(start), Some(head)) if start != head => {
            git.scope_committed(&state.root, start, head).await?
        }
        (Some(_), Some(_)) | (None, None) => Vec::new(),
        _ => {
            return Err(Error::new(
                "unavailable",
                "HEAD was unborn at goal set or is unborn now",
            ));
        }
    };
    let root = state.root.clone();
    let paths: Vec<String> = base.changed.keys().cloned().collect();
    let now =
        tokio::task::spawn_blocking(move || scope_digests(&root, paths.iter().map(String::as_str)))
            .await
            .map_err(|_| Error::new("internal", "scope read task failed"))??;
    let committed: BTreeSet<String> = committed.into_iter().collect();
    let mut changed: BTreeSet<String> = committed.iter().cloned().chain(state.changed).collect();
    // A pre-existing change is the Goal's work once its content moved or
    // the Goal committed it; unchanged and uncommitted, it is not.
    for (path, start) in &base.changed {
        if now.get(path) == Some(start) && !committed.contains(path) {
            changed.remove(path);
        } else {
            changed.insert(path.clone());
        }
    }
    Ok(changed)
}

/// Changed paths outside the Goal's `scope`; none without one.
pub(super) fn outside_scope<'a>(
    definition: &GoalDefinition,
    changed: &'a BTreeSet<String>,
) -> Vec<&'a str> {
    if definition.scope.is_empty() {
        return Vec::new();
    }
    let globs = normalized(&definition.scope);
    changed
        .iter()
        .map(String::as_str)
        .filter(|path| {
            !globs
                .iter()
                .any(|glob| claim_glob_matches_normalized(glob, path))
        })
        .collect()
}

/// Changed paths that are the check's own inputs.
pub(super) fn changed_check_inputs<'a>(
    definition: &GoalDefinition,
    base: &GoalScopeBase,
    changed: &'a BTreeSet<String>,
) -> Vec<&'a str> {
    let globs = normalized(&definition.check_inputs);
    changed
        .iter()
        .map(String::as_str)
        .filter(|path| {
            base.check_files.iter().any(|file| file == path)
                || globs
                    .iter()
                    .any(|glob| claim_glob_matches_normalized(glob, path))
        })
        .collect()
}

/// One bounded finding: a title, the path count and the first paths.
pub(super) fn finding(title: &str, paths: &[&str]) -> String {
    let mut text = format!("{title} ({} paths):", paths.len());
    for path in paths.iter().take(LISTED_PATHS) {
        text.push_str("\n- ");
        text.push_str(path);
    }
    if paths.len() > LISTED_PATHS {
        text.push_str(&format!("\n- … and {} more", paths.len() - LISTED_PATHS));
    }
    truncate_bytes(&text, FINDING_BYTES).0.to_owned()
}

/// The finding first, then as much of the check observation as fits.
pub(super) fn with_finding(finding: &str, observation: Option<&str>, bound: usize) -> String {
    match observation {
        Some(observation) => {
            let room = bound.saturating_sub(finding.len() + 2);
            format!("{finding}\n\n{}", truncate_bytes(observation, room).0)
        }
        None => finding.to_owned(),
    }
}

/// The scope line a continuation repeats so the limit survives every round.
pub(super) fn scope_reminder(definition: &GoalDefinition) -> Option<String> {
    (!definition.scope.is_empty()).then(|| {
        format!(
            "Change only these paths; Swarm blocks the Goal on any other change: {}",
            definition.scope.join(", ")
        )
    })
}

fn normalized(globs: &[String]) -> Vec<String> {
    globs
        .iter()
        .filter_map(|glob| normalize_claim_glob(glob).ok())
        .collect()
}

/// Workspace files the check argv names, relative to the Git root.
fn check_files(root: &Path, definition: &GoalDefinition, workspace: &str) -> Vec<String> {
    let cwd = PathBuf::from(definition.check_cwd.as_deref().unwrap_or(workspace));
    let mut files = BTreeSet::new();
    for arg in &definition.check {
        let candidate = Path::new(arg);
        let joined = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            cwd.join(candidate)
        };
        let Ok(canonical) = std::fs::canonicalize(&joined) else {
            continue;
        };
        if !canonical.is_file() {
            continue;
        }
        let Ok(relative) = canonical.strip_prefix(root) else {
            continue;
        };
        let parts: Option<Vec<&str>> = relative
            .components()
            .map(|component| component.as_os_str().to_str())
            .collect();
        if let Some(parts) = parts {
            files.insert(parts.join("/"));
        }
    }
    files.into_iter().collect()
}
