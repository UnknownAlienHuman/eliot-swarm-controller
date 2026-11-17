//! The worktrees Swarm manages, for Settings › Worktrees & cleanup: the
//! daemon's `workspace.list` rows (the retained managed-workspace
//! preparations, every page), grouped by project. Data only; nothing here
//! deletes.

use eliot_core::api::ManagedWorkspaceView;

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
}
