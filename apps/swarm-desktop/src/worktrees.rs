//! The worktrees Swarm manages, for Settings › Worktrees & cleanup: the
//! checkouts under `<data>\worktrees` (where `launch --worktree` creates
//! them) among the registered projects and the listed Agents' workspaces,
//! grouped by repository. Data only; `cleanup.plan` / `cleanup.apply` are
//! not in this build, so nothing here deletes.

use crate::nav::{self, Checkout};

/// Managed worktree paths under `root`, `(repository, paths)` sorted by
/// repository then path; each path once. `checkouts` are the registered
/// `(checkout, git common dir)` pairs; `workspaces` the Agents' workspaces.
pub fn managed(
    root: &str,
    checkouts: &[(String, String)],
    workspaces: &[String],
) -> Vec<(String, Vec<String>)> {
    let root_key = nav::path_key(root);
    if root_key.is_empty() {
        return Vec::new();
    }
    let registered: Vec<Checkout> = checkouts
        .iter()
        .map(|(path, common_dir)| Checkout {
            path: path.clone(),
            common_dir: common_dir.clone(),
        })
        .collect();
    let mut seen = std::collections::HashSet::new();
    let mut groups: Vec<(String, Vec<String>)> = Vec::new();
    for path in checkouts
        .iter()
        .map(|(path, _)| path)
        .chain(workspaces.iter())
    {
        let key = nav::path_key(path);
        let inside = key
            .strip_prefix(root_key.as_str())
            .is_some_and(|rest| rest.starts_with('/') && rest.len() > 1);
        if !inside || !seen.insert(key) {
            continue;
        }
        let project = nav::project_of(path, &registered);
        let shown = nav::display_path(path).to_owned();
        match groups.iter_mut().find(|(name, _)| *name == project) {
            Some((_, paths)) => paths.push(shown),
            None => groups.push((project, vec![shown])),
        }
    }
    for (_, paths) in &mut groups {
        paths.sort();
    }
    groups.sort_by(|left, right| left.0.cmp(&right.0));
    groups
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_paths_under_the_root_once_grouped_by_repository() {
        let root = r"C:\data\worktrees";
        let checkouts = vec![
            (
                r"C:\data\worktrees\wt-b".to_owned(),
                r"C:\src\app\.git".to_owned(),
            ),
            (r"C:\src\app".to_owned(), r"C:\src\app\.git".to_owned()),
        ];
        let workspaces = vec![
            r"C:\DATA\worktrees\wt-b".to_owned(),
            r"C:\data\worktrees\wt-a".to_owned(),
            r"C:\data\worktrees-other\x".to_owned(),
        ];
        let groups = managed(root, &checkouts, &workspaces);
        assert_eq!(
            groups,
            vec![
                ("app".to_owned(), vec![r"C:\data\worktrees\wt-b".to_owned()]),
                (
                    "wt-a".to_owned(),
                    vec![r"C:\data\worktrees\wt-a".to_owned()]
                ),
            ]
        );
        assert!(managed("", &checkouts, &workspaces).is_empty());
    }
}
