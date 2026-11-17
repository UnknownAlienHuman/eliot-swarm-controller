//! Read-only Git checkout identities rooted in an existing registration.

use super::*;
use eliot_core::api::{ProjectCheckoutView, ProjectRegistrationView};
use futures_util::{StreamExt, stream};
use std::collections::BTreeMap;

const MAX_CHECKOUTS: usize = 1_000;
const CHECKOUT_READBACK_BYTES: usize = 900 * 1024;

impl WorkspaceGit {
    pub(crate) async fn fill_project_readbacks(registrations: &mut [ProjectRegistrationView]) {
        if registrations.is_empty() {
            return;
        }
        let Ok(git) = Self::new() else {
            return;
        };
        let mut groups = BTreeMap::<String, Vec<&mut ProjectRegistrationView>>::new();
        let deadline = Instant::now() + ORIGIN_READ_TIME;
        for registration in &mut *registrations {
            groups
                .entry(registration.repo_common_dir.clone())
                .or_default()
                .push(registration);
        }
        stream::iter(groups)
            .for_each_concurrent(4, |(common_dir, registrations)| {
                let git = &git;
                async move {
                    if Instant::now() >= deadline {
                        return;
                    }
                    let origin = git.origin_name(&common_dir, deadline).await;
                    let mut checkouts = Vec::new();
                    if let Ok(common) = canonical_root(&common_dir) {
                        let main = registrations
                            .iter()
                            .find_map(|registration| {
                                main_checkout(&common, Path::new(&registration.checkout))
                            })
                            .or_else(|| {
                                let parent = common.parent()?;
                                main_checkout(&common, parent)
                            });
                        let repositories = main.into_iter().chain(linked_checkouts(&common));
                        checkouts = stream::iter(repositories.take(MAX_CHECKOUTS))
                            .map(|repo| git.checkout_readback(repo, deadline))
                            .buffered(4)
                            .filter_map(std::future::ready)
                            .collect()
                            .await;
                    }
                    // One registration carries the shared inventory. Keep the
                    // response bounded when several registrations share a repo.
                    for registration in registrations {
                        registration.origin = origin.clone();
                        registration.checkouts = std::mem::take(&mut checkouts);
                    }
                }
            })
            .await;
        let mut remaining = CHECKOUT_READBACK_BYTES;
        for registration in registrations {
            registration.checkouts.retain(|checkout| {
                let Ok(encoded) = serde_json::to_vec(checkout) else {
                    return false;
                };
                let Some(left) = remaining.checked_sub(encoded.len() + 1) else {
                    return false;
                };
                remaining = left;
                true
            });
        }
    }

    async fn checkout_readback(
        &self,
        repo: Repository,
        deadline: Instant,
    ) -> Option<ProjectCheckoutView> {
        let head = repo.git_dir.join("HEAD");
        reject_links(&head).ok()?;
        if !fs::symlink_metadata(&head).is_ok_and(|metadata| metadata.is_file()) {
            return None;
        }
        let binding = GitBinding {
            git_dir: &repo.git_dir,
            common_dir: &repo.common_dir,
        };
        let output = self
            .git
            .workspace_command_bound(
                &repo.root,
                &["symbolic-ref", "--quiet", "--no-recurse", "HEAD"],
                binding,
                deadline,
            )
            .await
            .ok()?;
        let branch = match output.code {
            0 => Some(
                line(&output.stdout)
                    .ok()?
                    .strip_prefix("refs/heads/")?
                    .to_owned(),
            ),
            1 => None, // Detached HEAD.
            _ => return None,
        };
        if branch.as_ref().is_some_and(|name| {
            name == ".invalid"
                || name.is_empty()
                || name.len() > 1024
                || name.chars().any(char::is_control)
        }) {
            return None;
        }
        let path = path_text(&repo.root).ok()?;
        if path.len() > 4096 {
            return None;
        }
        Some(ProjectCheckoutView {
            path,
            main: repo.git_dir == repo.common_dir,
            branch,
        })
    }
}

/// The Store lock spans this filesystem read and the usage check at callers.
/// Managed worktrees also have a retained common directory in Store.
pub(crate) fn registration_linked_paths(common_dir: &str) -> Vec<String> {
    canonical_root(common_dir).map_or_else(
        |_| Vec::new(),
        |common| {
            linked_checkouts(&common)
                .into_iter()
                .filter_map(|repo| path_text(&repo.root).ok())
                .collect()
        },
    )
}

fn main_checkout(common: &Path, checkout: &Path) -> Option<Repository> {
    if !matches!(fs::symlink_metadata(common.join("commondir")), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
    {
        return None;
    }
    let root = canonical_root(checkout.to_str()?).ok()?;
    let dot_git = root.join(".git");
    reject_links(&dot_git).ok()?;
    let git_dir = if fs::symlink_metadata(&dot_git).ok()?.is_dir() {
        dot_git
    } else {
        gitfile_target(&root)?
    };
    same_path(&git_dir, common).then(|| Repository {
        root,
        git_dir: common.to_owned(),
        common_dir: common.to_owned(),
    })
}

fn linked_checkouts(common: &Path) -> Vec<Repository> {
    let registry = common.join("worktrees");
    if reject_links(&registry).is_err() {
        return Vec::new();
    }
    let Ok(entries) = fs::read_dir(&registry) else {
        return Vec::new();
    };
    entries
        .take(MAX_CHECKOUTS)
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if !entry.file_type().ok()?.is_dir() {
                return None;
            }
            let admin = entry.path();
            let common_pointer = metadata_line(&admin.join("commondir")).ok()?;
            if !same_path(&pointer_path(&admin, &common_pointer)?, common) {
                return None;
            }
            let gitdir = metadata_line(&admin.join("gitdir")).ok()?;
            let gitfile = pointer_path(&admin, &gitdir)?;
            if gitfile.file_name()?.to_str()? != ".git" {
                return None;
            }
            let root = canonical_root(gitfile.parent()?.to_str()?).ok()?;
            if !same_path(&gitfile, &root.join(".git"))
                || !same_path(&gitfile_target(&root)?, &admin)
            {
                return None;
            }
            Some(Repository {
                root,
                git_dir: admin,
                common_dir: common.to_owned(),
            })
        })
        .collect()
}

fn gitfile_target(root: &Path) -> Option<PathBuf> {
    let line = metadata_line(&root.join(".git")).ok()?;
    pointer_path(root, line.strip_prefix("gitdir: ")?)
}

/// Normalize a Git pointer lexically. No components of the pointer are opened.
/// The callers compare it to a pinned path or require a reciprocal backlink.
fn pointer_path(base: &Path, pointer: &str) -> Option<PathBuf> {
    let pointer = pointer.trim_end_matches('\r');
    if pointer.is_empty() || pointer.chars().any(char::is_control) {
        return None;
    }
    let path = Path::new(pointer);
    if !path.is_absolute() && path.has_root() {
        return None;
    }
    let mut result = if path.is_absolute() {
        PathBuf::new()
    } else {
        base.to_owned()
    };
    let mut normal = path.is_absolute();
    for component in path.components() {
        match component {
            Component::ParentDir if normal || !result.pop() => return None,
            Component::ParentDir | Component::CurDir => {}
            Component::Prefix(_) if !path.is_absolute() => return None,
            Component::Normal(name) => {
                normal = true;
                result.push(name);
            }
            part => result.push(part.as_os_str()),
        }
    }
    Some(result)
}

fn same_path(left: &Path, right: &Path) -> bool {
    if cfg!(windows) {
        match (git_path_argument(left), git_path_argument(right)) {
            (Ok(left), Ok(right)) => left.eq_ignore_ascii_case(&right),
            _ => false,
        }
    } else {
        left == right
    }
}
