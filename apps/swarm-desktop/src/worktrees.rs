//! The worktrees Swarm manages, for Settings › Worktrees & cleanup: the
//! daemon's `workspace.list` rows (the retained managed-workspace
//! preparations, every page), grouped by project, and the Clean up finished
//! work review of one `cleanup.plan`, grouped by kind. Data only; nothing
//! here deletes: removal is the daemon's `cleanup.apply` of that plan
//! (`docs/spec/coordination.md`, Registered artifacts and cleanup).

use eliot_core::api::{
    ArtifactKind, CleanupApplyResponse, CleanupCandidate, CleanupPlan, DiskUsageDirectory,
    DiskUsageView, ManagedWorkspaceView,
};

use crate::nav;

/// One managed worktree as the page shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Worktree {
    pub path: String,
    pub branch: String,
    pub agent: String,
    pub phase: String,
}

/// Managed worktrees grouped by project name, `(project, worktrees)`
/// sorted by project then path: the logical name of each worktree's
/// project label ([`nav::project_of`] over `checkouts`), never a
/// registration ID.
pub fn grouped(
    rows: &[ManagedWorkspaceView],
    checkouts: &[nav::Checkout],
) -> Vec<(String, Vec<Worktree>)> {
    let mut groups: Vec<(String, Vec<Worktree>)> = Vec::new();
    for row in rows {
        let label = if row.project.is_empty() {
            &row.repo_path
        } else {
            &row.project
        };
        let project = nav::project_of(label, checkouts);
        let worktree = Worktree {
            path: nav::display_path(&row.worktree_path).to_owned(),
            branch: row.branch.clone(),
            agent: row.agent.clone(),
            phase: serde_json::to_value(row.phase)
                .ok()
                .and_then(|value| value.as_str().map(|phase| phase.replace('_', " ")))
                .unwrap_or_default(),
        };
        match groups.iter_mut().find(|(name, _)| *name == project) {
            Some((_, worktrees)) => worktrees.push(worktree),
            None => groups.push((project, vec![worktree])),
        }
    }
    for (_, worktrees) in &mut groups {
        worktrees.sort_by(|left, right| left.path.cmp(&right.path));
    }
    groups.sort_by(|left, right| left.0.cmp(&right.0));
    groups
}

/// The kinds the cleanup review groups a plan by, in the order shown.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CleanupGroup {
    Branches,
    Worktrees,
    Build,
    Temp,
    Chats,
    Sessions,
    ProjectRegistrations,
    Other,
}

impl CleanupGroup {
    pub const ALL: [Self; 8] = [
        Self::Branches,
        Self::Worktrees,
        Self::Build,
        Self::Temp,
        Self::Chats,
        Self::Sessions,
        Self::ProjectRegistrations,
        Self::Other,
    ];

    /// Native session cleanup and project settings changes are opt-in.
    pub fn checked_by_default(self) -> bool {
        !matches!(self, Self::Sessions | Self::ProjectRegistrations)
    }

    /// The groups a fresh review leaves unchecked.
    pub fn unchecked_by_default() -> std::collections::HashSet<Self> {
        Self::ALL
            .into_iter()
            .filter(|group| !group.checked_by_default())
            .collect()
    }

    pub fn title(self) -> &'static str {
        match self {
            Self::Branches => "Merged branches",
            Self::Worktrees => "Managed worktrees",
            Self::Build => "Build and target folders",
            Self::Temp => "Temporary folders",
            Self::Chats => "Chat folders",
            Self::Sessions => "Native sessions",
            Self::ProjectRegistrations => "Project registrations",
            Self::Other => "Other folders",
        }
    }

    /// The group of one plan item: its kind, and its purpose for the chat
    /// folders Swarm creates.
    pub fn of(candidate: &CleanupCandidate) -> Self {
        match candidate.artifact.kind {
            ArtifactKind::Branch => Self::Branches,
            ArtifactKind::Worktree => Self::Worktrees,
            ArtifactKind::Build => Self::Build,
            ArtifactKind::Temp => Self::Temp,
            ArtifactKind::NativeSession => Self::Sessions,
            ArtifactKind::ProjectRegistration => Self::ProjectRegistrations,
            ArtifactKind::Other if candidate.artifact.purpose == "Swarm chat directory" => {
                Self::Chats
            }
            ArtifactKind::Other | ArtifactKind::DataDir => Self::Other,
        }
    }
}

/// One group of the review: its items, those to remove first (each part in
/// plan order), what it would remove and how much.
#[derive(Clone, Debug)]
pub struct CleanupGroupView<'a> {
    pub group: CleanupGroup,
    pub items: Vec<&'a CleanupCandidate>,
    pub removable: usize,
    pub bytes: u64,
}

impl CleanupGroupView<'_> {
    /// The items Remove would take, then the kept ones.
    pub fn split(&self) -> (&[&CleanupCandidate], &[&CleanupCandidate]) {
        self.items.split_at(self.removable)
    }
}

/// The plan by group, groups without items left out.
pub fn cleanup_groups(plan: &CleanupPlan) -> Vec<CleanupGroupView<'_>> {
    CleanupGroup::ALL
        .into_iter()
        .filter_map(|group| {
            let mut items: Vec<&CleanupCandidate> = plan
                .items
                .iter()
                .filter(|item| CleanupGroup::of(item) == group)
                .collect();
            // Stable: plan order within the removable and the kept items.
            items.sort_by_key(|item| item.verdict != "safe");
            if items.is_empty() {
                return None;
            }
            let removable = items.iter().filter(|item| item.verdict == "safe").count();
            let bytes = items
                .iter()
                .filter(|item| item.verdict == "safe")
                .filter_map(|item| item.bytes)
                .sum();
            Some(CleanupGroupView {
                group,
                items,
                removable,
                bytes,
            })
        })
        .collect()
}

/// The safe items of the checked groups: what Apply removes.
pub fn selected_artifacts(
    plan: &CleanupPlan,
    unchecked: &std::collections::HashSet<CleanupGroup>,
) -> Vec<String> {
    plan.items
        .iter()
        .filter(|item| item.verdict == "safe" && !unchecked.contains(&CleanupGroup::of(item)))
        .map(|item| item.artifact.id.clone())
        .collect()
}

/// Bytes in words ("0 B", "12 KB", "3.4 MB", "1.2 GB").
pub fn size_words(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    let value = bytes as f64;
    if value < KB {
        format!("{bytes} B")
    } else if value < KB * KB {
        format!("{:.0} KB", value / KB)
    } else if value < KB * KB * KB {
        format!("{:.1} MB", value / (KB * KB))
    } else {
        format!("{:.1} GB", value / (KB * KB * KB))
    }
}

/// Disk totals and the worktree decisions shown with the cleanup review.
pub fn disk_usage_line(usage: &DiskUsageView) -> String {
    fn size(label: &str, directory: &DiskUsageDirectory) -> String {
        match (directory.bytes, &directory.error_code) {
            (Some(bytes), None) => format!("{label} {} ({bytes} bytes)", size_words(bytes)),
            (None, Some(code)) => format!("{label} unavailable ({code})"),
            _ => format!("{label} unavailable"),
        }
    }
    let decisions = match (
        usage.owner_decision_worktrees,
        &usage.owner_decision_error_code,
    ) {
        (Some(count), None) => format!("{count} worktrees awaiting Owner decision"),
        (None, Some(code)) => format!("Owner-decision worktrees unavailable ({code})"),
        _ => "Owner-decision worktrees unavailable".to_owned(),
    };
    format!(
        "Disk use: {} · {} · {} · {decisions}",
        size("artifacts", &usage.artifacts),
        size("worktrees", &usage.worktrees),
        size("plugins", &usage.plugins),
    )
}

/// A plan or apply reason code in words.
pub fn reason_words(reason: &str) -> String {
    match reason {
        "terminal_owned_artifact" => "its task finished".to_owned(),
        "native_archive_pending" => "archive the finished app session".to_owned(),
        "no_native_archive" => "the app keeps its chat history".to_owned(),
        "native_archive_refused" => "the app kept its session".to_owned(),
        "agent_not_quiescent" => {
            "its task is not finished: it is running or was interrupted".to_owned()
        }
        "merged" => "merged upstream".to_owned(),
        "merged_remote_deleted" => {
            "merged into the default branch; no remote branch left".to_owned()
        }
        "unmerged" => "not merged upstream".to_owned(),
        "branch_read_failed" => "the branch could not be read".to_owned(),
        "no_upstream_remote" => "the repository has no upstream remote".to_owned(),
        "fetch_failed" => "the upstream fetch failed".to_owned(),
        "remote_check_failed" | "merge_check_failed" => "the merge check failed".to_owned(),
        "unavailable" => "it could not be reached now".to_owned(),
        "ahead" => "has unpushed commits".to_owned(),
        "dirty" | "worktree_dirty" => "has uncommitted changes".to_owned(),
        "hard_link" => "shares files with another folder".to_owned(),
        "not_selected" => "its group was not selected".to_owned(),
        "prior_cleanup_attempt" => "an earlier cleanup already tried it".to_owned(),
        "identity_changed" | "conflict" => "it changed since the review".to_owned(),
        "project_path_present" => "the registered path still exists".to_owned(),
        "project_path_missing" => {
            "the registered path is gone and no live Agent uses it".to_owned()
        }
        "project_path_unverified" => "the registered path could not be checked".to_owned(),
        "project_in_use" => "an Agent or unsettled command still uses the registration".to_owned(),
        "branch_checked_out" => "the branch is checked out".to_owned(),
        "no_default_branch" => "the upstream default branch couldn't be found".to_owned(),
        "removed" => "already removed".to_owned(),
        "unknown" => "an earlier attempt couldn't be confirmed".to_owned(),
        other => other.replace('_', " "),
    }
}

/// One item's line: its name, then its size, its keep or remove reason and
/// where it is. A native session or chat folder is named by its task's
/// `title` when known, its Agent id then leading the second line.
pub fn item_line(item: &CleanupCandidate, title: Option<&str>) -> (String, String) {
    let path = &item.artifact.path;
    let tail = path
        .rsplit("::")
        .next()
        .map(|name| name.trim_start_matches("refs/heads/"))
        .unwrap_or(path);
    let folder = || {
        std::path::Path::new(tail).file_name().map_or_else(
            || tail.to_owned(),
            |name| name.to_string_lossy().into_owned(),
        )
    };
    let harness = crate::picker::harness_label(&item.artifact.purpose);
    let agent = &item.artifact.agent;
    let chat = CleanupGroup::of(item) == CleanupGroup::Chats;
    let name = match (item.artifact.kind, title) {
        (ArtifactKind::Branch, _) => tail.to_owned(),
        (ArtifactKind::ProjectRegistration, _) => item.artifact.project.clone(),
        // A native session is named by its task, not its store reference.
        (ArtifactKind::NativeSession, Some(title)) => title.to_owned(),
        // No title read: the app's session, its Agent on the second line.
        (ArtifactKind::NativeSession, None) => format!("{harness} session"),
        (_, Some(title)) if chat => title.to_owned(),
        (ArtifactKind::Worktree, _) => match &item.artifact.git {
            Some(git) => format!("{} · branch {}", folder(), git.branch),
            None => folder(),
        },
        _ => folder(),
    };
    let owner = match (item.artifact.kind, title) {
        (ArtifactKind::NativeSession, Some(_)) => format!("{harness} session · {agent} · "),
        (ArtifactKind::NativeSession, None) => format!("Agent {agent} · "),
        (_, Some(_)) if chat => format!("{agent} · "),
        _ => String::new(),
    };
    let size = match (item.artifact.kind, item.bytes) {
        (_, _) if !sized(CleanupGroup::of(item)) => String::new(),
        (_, Some(bytes)) => format!("{} · ", size_words(bytes)),
        (_, None) => "size not measured · ".to_owned(),
    };
    let verdict = if item.verdict == "safe" {
        "Remove"
    } else {
        "Keep"
    };
    let place = match item.artifact.kind {
        ArtifactKind::NativeSession => String::new(),
        _ => format!(" · {}", crate::nav::display_path(path)),
    };
    // The app is known here: name it rather than "its app".
    let reason = match item.reason.as_str() {
        "native_archive_pending" if item.artifact.purpose == "opencode" => {
            "delete the finished OpenCode session".to_owned()
        }
        "no_native_archive" => format!("{harness} keeps its chat history"),
        other => reason_words(other),
    };
    (name, format!("{owner}{size}{verdict}: {reason}{place}"))
}

/// Whether a group's size means anything (branches and archived native
/// sessions free no folder).
pub fn sized(group: CleanupGroup) -> bool {
    !matches!(
        group,
        CleanupGroup::Branches | CleanupGroup::Sessions | CleanupGroup::ProjectRegistrations
    )
}

/// The result of one apply, in words: what was removed, kept (with why) and
/// left unknown.
pub fn result_lines(
    plan: &CleanupPlan,
    reply: &CleanupApplyResponse,
    title: &dyn Fn(&str) -> Option<String>,
) -> Vec<String> {
    let name = |id: &str| {
        plan.items
            .iter()
            .find(|item| item.artifact.id == id)
            .map_or_else(
                || id.to_owned(),
                |item| item_line(item, title(&item.artifact.agent).as_deref()).0,
            )
    };
    let count = |outcome: &str| {
        reply
            .items
            .iter()
            .filter(|item| item.outcome == outcome)
            .count()
    };
    let state = reply
        .state
        .as_ref()
        .map(|state| match state {
            eliot_core::api::CleanupJobState::Running => "Running · ",
            eliot_core::api::CleanupJobState::Completed => "Completed · ",
            eliot_core::api::CleanupJobState::Failed => "Failed · ",
            eliot_core::api::CleanupJobState::Interrupted => "Interrupted · ",
        })
        .unwrap_or_default();
    let mut lines = vec![format!(
        "{state}Removed {} ({}) · kept {} · failed {} · couldn't confirm {}",
        count("removed"),
        size_words(reply.bytes_reclaimed),
        count("skipped"),
        count("failed"),
        count("unknown")
    )];
    for item in &reply.items {
        let reason = item.reason.as_deref().map(reason_words).unwrap_or_default();
        lines.push(match item.outcome.as_str() {
            "removed" => format!("Removed {}", name(&item.artifact_id)),
            "skipped" => format!("Kept {}: {reason}", name(&item.artifact_id)),
            _ => format!(
                "Couldn't confirm {}: {reason}; it is never retried automatically",
                name(&item.artifact_id)
            ),
        });
    }
    lines
}
