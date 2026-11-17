//! The worktrees Swarm manages, for Settings › Worktrees & cleanup: the
//! daemon's `workspace.list` rows (the retained managed-workspace
//! preparations, every page), grouped by project, and the Clean up finished
//! work review of one `cleanup.plan`, grouped by kind. Data only; nothing
//! here deletes: removal is the daemon's `cleanup.apply` of that plan
//! (`docs/spec/coordination.md`, Registered artifacts and cleanup).

use eliot_core::api::{
    ArtifactKind, CleanupApplyResponse, CleanupCandidate, CleanupPlan, ManagedWorkspaceView,
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
/// sorted by project then path. `registered` are the `(checkout, name)`
/// pairs that give a project label its name.
pub fn grouped(
    rows: &[ManagedWorkspaceView],
    registered: &[(String, String)],
) -> Vec<(String, Vec<Worktree>)> {
    let mut groups: Vec<(String, Vec<Worktree>)> = Vec::new();
    for row in rows {
        let label = if row.project.is_empty() {
            &row.repo_path
        } else {
            &row.project
        };
        let project = nav::project_name(label, registered);
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

    /// Native session archives and project settings changes are opt-in.
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
            Self::Sessions => "Native session archives",
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

/// A plan or apply reason code in words.
pub fn reason_words(reason: &str) -> String {
    match reason {
        "terminal_owned_artifact" => "its task finished".to_owned(),
        "native_archive_pending" => "archive the finished Codex session".to_owned(),
        "no_native_archive" => "its app cannot archive sessions".to_owned(),
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
        "project_in_use" => "an Agent still uses the registration".to_owned(),
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
        "no_native_archive" => format!("{harness} cannot archive its sessions"),
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
    let mut lines = vec![format!(
        "Removed {} ({}) · kept {} · couldn't confirm {}",
        count("removed"),
        size_words(reply.bytes_reclaimed),
        count("skipped"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::WorkspacePhase;

    fn row(agent: &str, project: &str, path: &str, phase: WorkspacePhase) -> ManagedWorkspaceView {
        ManagedWorkspaceView {
            agent: agent.to_owned(),
            project: project.to_owned(),
            repo_path: r"C:\src\app".to_owned(),
            branch: format!("fix/{agent}"),
            worktree_path: path.to_owned(),
            phase,
            updated_at: 1,
        }
    }

    #[test]
    fn rows_group_by_project_name_whatever_the_agent_labels_say() {
        // The worktree paths come from the daemon's preparations, so a
        // worktree outside any listed Agent page or outside <data>\worktrees
        // is still shown; Agent project labels play no part.
        let rows = vec![
            row(
                "a9",
                r"C:\src\app",
                r"D:\wt\app-b",
                WorkspacePhase::SetupRunning,
            ),
            row("a2", r"C:\src\app", r"D:\wt\app-a", WorkspacePhase::Ready),
            row("a5", r"C:\src\lib", r"D:\wt\lib-a", WorkspacePhase::Unknown),
        ];
        let registered = vec![(r"C:\src\app".to_owned(), "app".to_owned())];
        let groups = grouped(&rows, &registered);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, "app");
        assert_eq!(
            groups[0]
                .1
                .iter()
                .map(|worktree| worktree.path.as_str())
                .collect::<Vec<_>>(),
            vec![r"D:\wt\app-a", r"D:\wt\app-b"]
        );
        assert_eq!(groups[0].1[1].phase, "setup running");
        assert_eq!(groups[1].0, "lib");
        assert_eq!(groups[1].1[0].agent, "a5");
        assert!(grouped(&[], &registered).is_empty());
    }

    fn candidate(
        kind: ArtifactKind,
        purpose: &str,
        verdict: &str,
        bytes: Option<u64>,
    ) -> CleanupCandidate {
        CleanupCandidate {
            artifact: eliot_core::api::ArtifactView {
                id: format!("{purpose}-{}", bytes.unwrap_or(0)),
                agent: "a1".to_owned(),
                generation: 1,
                project: "p".to_owned(),
                tree: Vec::new(),
                path: format!("C:/scratch/{purpose}"),
                kind,
                purpose: purpose.to_owned(),
                identity: "x".to_owned(),
                created_at: 0,
                state: "registered".to_owned(),
                git: None,
            },
            bytes,
            verdict: verdict.to_owned(),
            reason: if verdict == "safe" {
                "terminal_owned_artifact".to_owned()
            } else {
                "agent_not_quiescent".to_owned()
            },
        }
    }

    #[test]
    fn a_plan_reads_by_kind_with_sizes_reasons_and_a_selection() {
        let plan = CleanupPlan {
            plan_id: "p".to_owned(),
            created_at: 0,
            items: vec![
                candidate(ArtifactKind::Temp, "temp", "safe", Some(2048)),
                candidate(ArtifactKind::Worktree, "wt", "safe", Some(3 * 1024 * 1024)),
                candidate(ArtifactKind::Other, "Swarm chat directory", "keep", None),
                candidate(ArtifactKind::Build, "target", "keep", Some(1)),
            ],
            truncated: false,
        };
        let groups = cleanup_groups(&plan);
        let titles: Vec<&str> = groups.iter().map(|group| group.group.title()).collect();
        assert_eq!(
            titles,
            [
                "Managed worktrees",
                "Build and target folders",
                "Temporary folders",
                "Chat folders"
            ]
        );
        assert_eq!((groups[0].removable, groups[0].bytes), (1, 3 * 1024 * 1024));
        assert_eq!(groups[1].removable, 0);
        let (name, line) = item_line(&plan.items[0], None);
        assert_eq!(name, "temp");
        assert!(
            line.starts_with("2 KB · Remove: its task finished"),
            "{line}"
        );
        let (_, kept) = item_line(&plan.items[3], None);
        assert!(kept.contains("Keep: its task is not finished"), "{kept}");
        let session = candidate(ArtifactKind::NativeSession, "codex", "safe", None);
        let (name, line) = item_line(&session, None);
        assert_eq!(name, "Codex session");
        assert_eq!(line, "Agent a1 · Remove: its task finished");
        let mut claude = candidate(ArtifactKind::NativeSession, "claude", "keep", None);
        claude.reason = "no_native_archive".to_owned();
        let (name, line) = item_line(&claude, None);
        assert_eq!(name, "Claude Code session");
        assert_eq!(
            line,
            "Agent a1 · Keep: Claude Code cannot archive its sessions"
        );
        let (name, line) = item_line(&session, Some("Fix the login bug"));
        assert_eq!(name, "Fix the login bug");
        assert_eq!(line, "Codex session · a1 · Remove: its task finished");
        assert_eq!(
            CleanupGroup::unchecked_by_default(),
            [CleanupGroup::Sessions, CleanupGroup::ProjectRegistrations]
                .into_iter()
                .collect()
        );
        assert!(!sized(CleanupGroup::Branches) && sized(CleanupGroup::Temp));
        let mut unchecked = std::collections::HashSet::new();
        assert_eq!(selected_artifacts(&plan, &unchecked).len(), 2);
        unchecked.insert(CleanupGroup::Temp);
        assert_eq!(
            selected_artifacts(&plan, &unchecked),
            [plan.items[1].artifact.id.clone()]
        );
        assert_eq!(size_words(0), "0 B");
        assert_eq!(size_words(5 * 1024 * 1024 * 1024), "5.0 GB");
    }

    #[test]
    fn an_apply_result_names_removed_kept_and_unknown_items() {
        let plan = CleanupPlan {
            plan_id: "p".to_owned(),
            created_at: 0,
            items: vec![
                candidate(ArtifactKind::Temp, "temp", "safe", Some(10)),
                candidate(ArtifactKind::Build, "target", "safe", Some(20)),
                candidate(ArtifactKind::Worktree, "wt", "keep", None),
            ],
            truncated: false,
        };
        let item = |index: usize, outcome: &str, reason: Option<&str>, bytes: u64| {
            eliot_core::api::CleanupItemOutcome {
                artifact_id: plan.items[index].artifact.id.clone(),
                outcome: outcome.to_owned(),
                reason: reason.map(str::to_owned),
                bytes_reclaimed: bytes,
            }
        };
        let reply = CleanupApplyResponse {
            plan_id: "p".to_owned(),
            items: vec![
                item(0, "removed", None, 10),
                item(1, "unknown", Some("unknown"), 0),
                item(2, "skipped", Some("agent_not_quiescent"), 0),
            ],
            bytes_reclaimed: 10,
        };
        let lines = result_lines(&plan, &reply, &|_| None);
        assert_eq!(lines[0], "Removed 1 (10 B) · kept 1 · couldn't confirm 1");
        assert_eq!(lines[1], "Removed temp");
        assert!(
            lines[2].starts_with("Couldn't confirm target"),
            "{}",
            lines[2]
        );
        assert_eq!(
            lines[3],
            "Kept wt: its task is not finished: it is running or was interrupted"
        );
    }
}
