//! Read-only Git checkout identities rooted in an existing registration.

use super::*;
use crate::fs_invalidation::{FilesystemRevision, WorkspaceInvalidation};
use crate::platform::is_reparse_point;
use eliot_core::api::{ProjectCheckoutView, ProjectRegistrationView};
use eliot_plugin::sync::LockExt;
use futures_util::{StreamExt, stream};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex},
};

const MAX_CHECKOUTS: usize = 1_000;
const CHECKOUT_READBACK_BYTES: usize = 900 * 1024;
#[cfg(any(windows, target_os = "linux"))]
const PROJECT_WATCH_CHECKOUTS: usize = 256;

#[derive(Default)]
pub(crate) struct ProjectReadCache {
    entries: Mutex<HashMap<String, Arc<CachedProjectReadback>>>,
    wake: Arc<tokio::sync::Notify>,
}

struct CachedProjectReadback {
    watched: WatchedProject,
    origin: Option<String>,
    checkouts: Vec<ProjectCheckoutView>,
}

struct WatchedProject {
    watcher: Arc<WorkspaceInvalidation>,
    revision: FilesystemRevision,
    registrations: Vec<String>,
    stamps: Vec<CheckoutReadStamp>,
    checkout_roots: HashSet<String>,
}

impl ProjectReadCache {
    /// Publish only the observation for the current watched revision. This
    /// projection performs no filesystem work and never joins a refresh.
    pub(crate) fn apply(
        &self,
        registrations: &mut [ProjectRegistrationView],
        single: bool,
        include_git_state: bool,
    ) {
        let mut groups = BTreeMap::<String, Vec<&mut ProjectRegistrationView>>::new();
        for registration in &mut *registrations {
            groups
                .entry(registration.repo_common_dir.clone())
                .or_default()
                .push(registration);
        }
        let mut missing = false;
        for (common, group) in groups {
            let entry = self.entries.lock_recover().get(&common).cloned();
            let paths = group.iter().map(|registration| &registration.checkout);
            let current = entry.filter(|entry| {
                entry.watched.current()
                    && if single {
                        paths
                            .clone()
                            .all(|path| entry.watched.registrations.contains(path))
                    } else {
                        entry.watched.registrations.iter().eq(paths)
                    }
            });
            if let Some(entry) = current {
                let origin = entry.origin.clone();
                let checkouts = entry.checkouts.clone();
                if entry.watched.current() {
                    assign_readbacks(group, origin, checkouts);
                    continue;
                }
            }
            for registration in group {
                registration.git_refreshing = include_git_state;
                registration.origin = None;
                registration.checkouts.clear();
            }
            missing = true;
        }
        if missing {
            self.wake.notify_one();
        }
        let mut remaining = CHECKOUT_READBACK_BYTES;
        for registration in registrations {
            bound_checkouts(&mut registration.checkouts, &mut remaining);
        }
    }

    pub(crate) async fn changed(&self) {
        self.wake.notified().await;
    }

    pub(crate) fn clear(&self) {
        let retired = std::mem::take(&mut *self.entries.lock_recover());
        tokio::task::spawn_blocking(move || drop(retired));
    }

    fn retain(&self, registrations: &BTreeMap<String, Vec<String>>) {
        let mut retired = Vec::new();
        self.entries.lock_recover().retain(|key, entry| {
            if registrations.contains_key(key) {
                return true;
            }
            retired.push(Arc::clone(entry));
            false
        });
        if !retired.is_empty() {
            tokio::task::spawn_blocking(move || drop(retired));
        }
    }

    async fn verified(
        &self,
        common: &str,
        registrations: &[String],
    ) -> Option<Arc<CachedProjectReadback>> {
        let entry = self.entries.lock_recover().get(common).cloned()?;
        if entry.watched.registrations != registrations || !entry.watched.current() {
            return None;
        }
        let paths = registrations.to_vec();
        let common = common.to_owned();
        let stamps = tokio::task::spawn_blocking(move || project_stamps(&paths, &common))
            .await
            .ok()??;
        (stamps == entry.watched.stamps && entry.watched.current()).then_some(entry)
    }

    fn live_watcher(
        &self,
        common: &str,
        registrations: &[String],
    ) -> Option<(Arc<WorkspaceInvalidation>, HashSet<String>)> {
        let entry = self.entries.lock_recover().get(common).cloned()?;
        if entry.watched.registrations != registrations || entry.watched.watcher.is_degraded() {
            return None;
        }
        Some((
            Arc::clone(&entry.watched.watcher),
            entry.watched.checkout_roots.clone(),
        ))
    }

    fn replace(&self, common: String, value: Option<CachedProjectReadback>) {
        let retired = {
            let mut entries = self.entries.lock_recover();
            match value {
                Some(value) => entries.insert(common, Arc::new(value)),
                None => entries.remove(&common),
            }
        };
        if retired.is_some() {
            tokio::task::spawn_blocking(move || drop(retired));
        }
    }
}

impl WatchedProject {
    fn current(&self) -> bool {
        self.watcher.revision().as_ref() == Some(&self.revision)
    }
}

fn project_stamps(paths: &[String], common: &str) -> Option<Vec<CheckoutReadStamp>> {
    paths
        .iter()
        .map(|path| {
            DiffCheckout::open(path, Some(common))
                .ok()?
                .read_stamp()
                .ok()
        })
        .collect()
}

/// The worktree and Git administration directories stay pinned through the
/// patch read. Resolve the files-format HEAD without another Git process.
pub(crate) struct DiffCheckout {
    pub root: Directory,
    git_dir: Directory,
    common_dir: Directory,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct CheckoutReadStamp {
    /// Keep the binding identities as well as the Git file values.
    directories: Vec<eliot_plugin::FileIdentity>,
    pub(crate) head: String,
    head_file: Vec<u8>,
    index: Option<[u8; 32]>,
}

impl DiffCheckout {
    pub(crate) fn open(workspace: &str, expected_common: Option<&str>) -> Result<Self> {
        let rejected = || problem("unavailable", "cannot bind the exact Git checkout for diff");
        let path = canonical_root(workspace)?;
        let root = Directory::open(&path)?;
        let dot_git = path.join(".git");
        let (git_dir, common) = if fs::symlink_metadata(&dot_git).is_ok_and(|m| m.is_dir()) {
            (dot_git.clone(), dot_git)
        } else {
            let admin = gitfile_target(&path).ok_or_else(rejected)?;
            let registry = admin.parent().ok_or_else(rejected)?;
            let common = registry.parent().ok_or_else(rejected)?.to_path_buf();
            let common_pointer = metadata_line(&admin.join("commondir"))?;
            let backlink = metadata_line(&admin.join("gitdir"))?;
            if registry.file_name().and_then(|name| name.to_str()) != Some("worktrees")
                || !pointer_path(&admin, &common_pointer).is_some_and(|p| same_path(&p, &common))
                || !pointer_path(&admin, &backlink).is_some_and(|p| same_path(&p, &dot_git))
            {
                return Err(rejected());
            }
            (admin, common)
        };
        if let Some(expected) = expected_common
            && !same_path(&canonical_root(expected)?, &common)
        {
            return Err(rejected());
        }
        Ok(Self {
            root,
            git_dir: Directory::open(&git_dir)?,
            common_dir: Directory::open(&common)?,
        })
    }

    pub(crate) fn binding(&self) -> GitBinding<'_> {
        GitBinding {
            git_dir: &self.git_dir.path,
            common_dir: &self.common_dir.path,
        }
    }

    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        [&self.root, &self.git_dir, &self.common_dir]
            .into_iter()
            .zip([&other.root, &other.git_dir, &other.common_dir])
            .all(|(left, right)| {
                same_path(&left.path, &right.path)
                    && left.directory_identities(1).is_ok_and(|identity| {
                        right
                            .directory_identities(1)
                            .is_ok_and(|other| identity == other)
                    })
            })
    }

    pub(crate) fn head(&self) -> Result<String> {
        let no_head = || problem("unavailable", "cannot resolve the checkout HEAD for diff");
        let head = line(&read_diff_metadata(self.git_dir.file("HEAD")?, 4096)?)?;
        if is_git_oid(&head) {
            return Ok(head);
        }
        let reference = head
            .strip_prefix("ref: ")
            .filter(|value| {
                value.starts_with("refs/heads/") && eliot_core::api::valid_diff_path(value)
            })
            .ok_or_else(no_head)?;
        self.reference_oid(reference)
    }

    pub(crate) fn read_stamp(&self) -> Result<CheckoutReadStamp> {
        use sha2::{Digest, Sha256};
        let head_file = read_diff_metadata(self.git_dir.file("HEAD")?, 4096)?;
        let head = self.head()?;
        let index = match fs::symlink_metadata(self.git_dir.path.join("index")) {
            Ok(_) => {
                let bytes = read_diff_metadata(self.git_dir.file("index")?, 4 * 1024 * 1024)?;
                Some(Sha256::digest(bytes).into())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) => return Err(problem("unavailable", "cannot stamp the Git index")),
        };
        let mut directories = Vec::new();
        for dir in [&self.root, &self.git_dir, &self.common_dir] {
            directories.extend(
                dir.directory_identities(1).map_err(|_| {
                    problem("unavailable", "cannot stamp the Git directory identity")
                })?,
            );
        }
        Ok(CheckoutReadStamp {
            directories,
            head,
            head_file,
            index,
        })
    }

    pub(crate) fn on_branch(&self, branch: &str, oid: &str) -> bool {
        let matches_branch = self
            .git_dir
            .file("HEAD")
            .and_then(|file| read_diff_metadata(file, 4096))
            .and_then(|bytes| line(&bytes))
            .is_ok_and(|head| head.strip_prefix("ref: refs/heads/") == Some(branch));
        matches_branch && self.head().is_ok_and(|head| head == oid)
    }

    pub(crate) fn base_dependency(
        &self,
        base: &str,
        oid: &str,
    ) -> Option<eliot_core::GitBaseDependency> {
        if base == "HEAD" || is_git_oid(base) {
            return None;
        }
        let reference = if base.starts_with("refs/") {
            base.to_owned()
        } else if base.starts_with("origin/") {
            format!("refs/remotes/{base}")
        } else {
            format!("refs/heads/{base}")
        };
        let mut dependency = eliot_core::GitBaseDependency {
            reference,
            base_oid: oid.to_owned(),
            tip: None,
            verified: false,
        };
        if !dependency.valid() {
            return None;
        }
        dependency.tip = self.read_reference_oid(&dependency.reference).ok()?;
        dependency.verified = dependency.tip.as_deref() == Some(oid);
        Some(dependency)
    }

    pub(crate) fn origin_default(&self) -> Result<eliot_core::GitUpstreamTip> {
        let (dir, name) =
            Directory::open(&self.common_dir.path)?.descend("refs/remotes/origin/HEAD")?;
        let head = line(&read_diff_metadata(dir.file(&name)?, 4096)?)?;
        let reference = head
            .strip_prefix("ref: ")
            .filter(|value| {
                value.starts_with("refs/remotes/origin/")
                    && value.len() <= 256
                    && eliot_core::api::valid_diff_path(value)
            })
            .ok_or_else(|| problem("unavailable", "cannot resolve the local origin default"))?;
        let tip = eliot_core::GitUpstreamTip {
            reference: reference.to_owned(),
            oid: self.reference_oid(reference)?,
        };
        tip.branch()
            .is_some()
            .then_some(tip)
            .ok_or_else(|| problem("unavailable", "invalid local origin default"))
    }

    fn reference_oid(&self, reference: &str) -> Result<String> {
        let no_head = || problem("unavailable", "cannot resolve the checkout Git ref");
        self.read_reference_oid(reference)?.ok_or_else(no_head)
    }

    fn read_reference_oid(&self, reference: &str) -> Result<Option<String>> {
        let no_head = || problem("unavailable", "cannot resolve the checkout Git ref");
        if self
            .common_dir
            .path
            .join("reftable")
            .try_exists()
            .map_err(|_| no_head())?
        {
            return Err(no_head());
        }
        let loose = self.common_dir.path.join(reference);
        let oid = match fs::symlink_metadata(&loose) {
            Ok(_) => {
                let (dir, name) = Directory::open(&self.common_dir.path)?.descend(reference)?;
                line(&read_diff_metadata(dir.file(&name)?, 4096)?)?
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                // Verify all existing parents before using a missing loose ref as evidence.
                let mut ancestor = loose.parent().ok_or_else(no_head)?;
                while ancestor != self.common_dir.path {
                    match fs::symlink_metadata(ancestor) {
                        Ok(meta) if !meta.is_dir() || is_reparse_point(&meta) => {
                            return Err(no_head());
                        }
                        Ok(_) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(_) => return Err(no_head()),
                    }
                    ancestor = ancestor.parent().ok_or_else(no_head)?;
                }
                let packed = self.common_dir.path.join("packed-refs");
                match fs::symlink_metadata(&packed) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Err(_) => return Err(no_head()),
                    Ok(_) => {}
                }
                let bytes = read_diff_metadata(self.common_dir.file("packed-refs")?, 1024 * 1024)?;
                let Some(oid) = std::str::from_utf8(&bytes)
                    .map_err(|_| no_head())?
                    .lines()
                    .filter_map(|entry| entry.split_once(' '))
                    .find_map(|(oid, name)| (name == reference).then_some(oid.to_owned()))
                else {
                    return Ok(None);
                };
                oid
            }
            Err(_) => return Err(no_head()),
        };
        is_git_oid(&oid).then_some(Some(oid)).ok_or_else(no_head)
    }
}

fn read_diff_metadata(file: fs::File, limit: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| problem("unavailable", "Git diff metadata could not be read"))?;
    if bytes.len() as u64 > limit {
        return Err(problem(
            "unavailable",
            "Git diff metadata exceeds its bound",
        ));
    }
    Ok(bytes)
}

impl WorkspaceGit {
    pub(crate) async fn refresh_project_readbacks(
        cache: &ProjectReadCache,
        registrations: &[ProjectRegistrationView],
    ) {
        let mut groups = BTreeMap::<String, Vec<String>>::new();
        for registration in registrations {
            groups
                .entry(registration.repo_common_dir.clone())
                .or_default()
                .push(registration.checkout.clone());
        }
        cache.retain(&groups);
        if groups.is_empty() {
            return;
        }
        let Ok(git) = Self::new() else {
            cache.clear();
            return;
        };
        stream::iter(groups)
            .for_each_concurrent(4, |(common_dir, registration_keys)| {
                let git = &git;
                async move {
                    if cache
                        .verified(&common_dir, &registration_keys)
                        .await
                        .is_some()
                    {
                        return;
                    }
                    let watch_common = common_dir.clone();
                    let watch_paths = registration_keys.clone();
                    let existing = cache.live_watcher(&common_dir, &registration_keys);
                    // Also fences a stamp mismatch detected before its
                    // filesystem notification reaches the watcher.
                    cache.replace(common_dir.clone(), None);
                    let wake = Arc::clone(&cache.wake);
                    let Some(watched) = tokio::task::spawn_blocking(move || {
                        let (watcher, checkout_roots) = match existing {
                            Some(watcher) => watcher,
                            None => {
                                let (watcher, roots) =
                                    project_watcher(&watch_common, &watch_paths, wake)?;
                                (Arc::new(watcher), roots)
                            }
                        };
                        let revision = watcher.revision()?;
                        let stamps = project_stamps(&watch_paths, &watch_common)?;
                        let state = WatchedProject {
                            watcher,
                            revision,
                            registrations: watch_paths,
                            stamps,
                            checkout_roots,
                        };
                        state.current().then_some(state)
                    })
                    .await
                    .ok()
                    .flatten() else {
                        return;
                    };
                    let deadline = Instant::now() + ORIGIN_READ_TIME;
                    let (origin, (mut checkouts, complete)) =
                        tokio::join!(git.project_origin(&common_dir, deadline), async {
                            let Ok(common) = canonical_root(&common_dir) else {
                                return (Vec::new(), false);
                            };
                            let registered = registration_keys.clone();
                            let (repositories, inventory) = tokio::join!(
                                tokio::task::spawn_blocking({
                                    let common = common.clone();
                                    move || {
                                        let main = registered
                                            .iter()
                                            .find_map(|checkout| {
                                                main_checkout(&common, Path::new(checkout))
                                            })
                                            .or_else(|| main_checkout(&common, common.parent()?));
                                        let linked = linked_checkouts(&common);
                                        main.into_iter()
                                            .chain(linked)
                                            .take(MAX_CHECKOUTS)
                                            .collect::<Vec<_>>()
                                    }
                                }),
                                git.project_entries(&common, deadline),
                            );
                            let mut complete = repositories.is_ok() && inventory.is_ok();
                            let inventory = inventory.unwrap_or_default();
                            complete &= {
                                // The first Git record is the repository's
                                // main checkout, which can be outside the
                                // registration. Only main_checkout selects
                                // it. All linked records must be watched.
                                inventory.iter().skip(1).all(|entry| {
                                    entry.path.to_str().is_some_and(|path| {
                                        watched
                                            .checkout_roots
                                            .contains(eliot_core::workspace_path_key(path).as_ref())
                                    })
                                })
                            };
                            let mut by_path = BTreeMap::new();
                            for entry in &inventory {
                                if let Some(path) = entry.path.to_str() {
                                    by_path
                                        .entry(eliot_core::workspace_path_key(path))
                                        .and_modify(|value| *value = None)
                                        .or_insert(Some(entry));
                                }
                            }
                            let repositories = repositories.unwrap_or_default();
                            let expected = repositories.len();
                            let checkouts: Vec<_> = stream::iter(repositories)
                                .map(|repo| {
                                    let entry = repo.root.to_str().and_then(|path| {
                                        by_path
                                            .get(&eliot_core::workspace_path_key(path))
                                            .copied()
                                            .flatten()
                                    });
                                    git.checkout_readback(repo, entry.cloned(), deadline)
                                })
                                .buffered(8)
                                .filter_map(std::future::ready)
                                .collect()
                                .await;
                            complete &= checkouts.len() == expected;
                            complete &= {
                                checkouts.iter().all(|checkout| {
                                    watched.checkout_roots.contains(
                                        eliot_core::workspace_path_key(&checkout.path).as_ref(),
                                    )
                                })
                            };
                            (checkouts, complete)
                        });
                    let cacheable = complete && origin.is_ok() && Instant::now() < deadline;
                    let origin = origin.ok().flatten();
                    let mut remaining = CHECKOUT_READBACK_BYTES;
                    bound_checkouts(&mut checkouts, &mut remaining);
                    let cached = if cacheable && watched.current() {
                        let common = common_dir.clone();
                        let paths = registration_keys;
                        let stamps =
                            tokio::task::spawn_blocking(move || project_stamps(&paths, &common))
                                .await
                                .ok()
                                .flatten();
                        (stamps.as_ref() == Some(&watched.stamps) && watched.current()).then(|| {
                            CachedProjectReadback {
                                watched,
                                origin,
                                checkouts,
                            }
                        })
                    } else {
                        None
                    };
                    cache.replace(common_dir, cached);
                }
            })
            .await;
    }

    async fn project_entries(
        &self,
        common: &Path,
        deadline: Instant,
    ) -> Result<Vec<WorktreeEntry>> {
        let output = self
            .git
            .project_git_metadata(
                common,
                &["worktree", "list", "--porcelain", "-z"],
                None,
                deadline,
            )
            .await?;
        if output.code != 0 {
            return Err(problem(
                "unavailable",
                "Git worktree registry could not be read",
            ));
        }
        Self::parse_worktree_entries(&output.stdout)
    }

    async fn checkout_readback(
        &self,
        repo: Repository,
        entry: Option<WorktreeEntry>,
        deadline: Instant,
    ) -> Option<ProjectCheckoutView> {
        let (repo, branch) = tokio::task::spawn_blocking(move || {
            let head = repo.git_dir.join("HEAD");
            let branch = entry
                .as_ref()
                .and_then(|entry| inventory_head(&repo, &head, entry));
            if branch.is_none() {
                reject_links(&head).ok()?;
                if !fs::symlink_metadata(&head).is_ok_and(|metadata| metadata.is_file()) {
                    return None;
                }
            }
            Some((repo, branch))
        })
        .await
        .ok()??;
        let branch = match branch {
            Some(branch) => branch,
            None => {
                let binding = GitBinding {
                    git_dir: &repo.git_dir,
                    common_dir: &repo.common_dir,
                };
                let output = self
                    .git
                    .project_git_metadata(
                        &repo.root,
                        &["symbolic-ref", "--quiet", "--no-recurse", "HEAD"],
                        Some(binding),
                        deadline,
                    )
                    .await
                    .ok()?;
                match output.code {
                    0 => Some(
                        line(&output.stdout)
                            .ok()?
                            .strip_prefix("refs/heads/")?
                            .to_owned(),
                    ),
                    1 => None,
                    _ => return None,
                }
            }
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

fn bound_checkouts(checkouts: &mut Vec<ProjectCheckoutView>, remaining: &mut usize) {
    checkouts.retain(|checkout| {
        let Ok(encoded) = serde_json::to_vec(checkout) else {
            return false;
        };
        let Some(left) = remaining.checked_sub(encoded.len() + 1) else {
            return false;
        };
        *remaining = left;
        true
    });
}

/// The first registration carries the shared inventory, even on a cache hit.
fn assign_readbacks(
    registrations: Vec<&mut ProjectRegistrationView>,
    origin: Option<String>,
    mut checkouts: Vec<ProjectCheckoutView>,
) {
    for registration in registrations {
        registration.git_refreshing = false;
        registration.origin = origin.clone();
        registration.checkouts = std::mem::take(&mut checkouts);
    }
}

/// Git resolves symbolic chains in the bulk inventory. Compare the first
/// reference from the already bound checkout so its result also agrees with
/// `symbolic-ref --no-recurse`. Unborn, changed or ambiguous entries use Git
/// directly for that checkout.
fn inventory_head(repo: &Repository, head: &Path, entry: &WorktreeEntry) -> Option<Option<String>> {
    if entry.incomplete
        || entry.prunable
        || !matches!(fs::symlink_metadata(repo.git_dir.join("config.worktree")), Err(error)
            if error.kind() == std::io::ErrorKind::NotFound)
    {
        return None;
    }
    let current = metadata_line(head).ok()?;
    if let Some(reference) = current.strip_prefix("ref: ")
        && entry.branch.as_deref() == Some(reference)
        && let Some(branch) = reference.strip_prefix("refs/heads/")
    {
        return Some(Some(branch.to_owned()));
    }
    (is_git_oid(&current) && entry.branch.is_none() && entry.head.as_deref() == Some(&current))
        .then_some(None)
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
    let mut pinned = Directory::open(&registry).ok();
    entries
        .take(MAX_CHECKOUTS)
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if !entry.file_type().ok()?.is_dir() {
                return None;
            }
            let admin = entry.path();
            let pointers = pinned.as_mut().and_then(|directory| {
                directory.push_entry_directory(&entry).ok()?;
                let read = |name| line(&read_diff_metadata(directory.file(name)?, METADATA_BYTES)?);
                let pointers = read("commondir").ok().zip(read("gitdir").ok());
                directory.pop_entry_directory();
                pointers
            });
            let (common_pointer, gitdir) = match pointers {
                Some(pointers) => pointers,
                None => (
                    metadata_line(&admin.join("commondir")).ok()?,
                    metadata_line(&admin.join("gitdir")).ok()?,
                ),
            };
            if !same_path(&pointer_path(&admin, &common_pointer)?, common) {
                return None;
            }
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

/// Locate a managed linked checkout's admin directory without starting Git.
/// The worktree's pointer can only select a direct child of its admitted
/// common directory's worktree registry with the reciprocal backlink.
#[cfg(any(windows, target_os = "linux"))]
pub(crate) fn linked_watch_admin(root: &Path, common: &Path) -> Option<PathBuf> {
    let admin = gitfile_target(root)?;
    if !same_path(admin.parent()?, &common.join("worktrees")) {
        return None;
    }
    let common_pointer = metadata_line(&admin.join("commondir")).ok()?;
    let backlink = metadata_line(&admin.join("gitdir")).ok()?;
    (same_path(&pointer_path(&admin, &common_pointer)?, common)
        && same_path(&pointer_path(&admin, &backlink)?, &root.join(".git")))
    .then_some(admin)
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

#[cfg(any(windows, target_os = "linux"))]
fn project_watcher(
    common: &str,
    registrations: &[String],
    wake: Arc<tokio::sync::Notify>,
) -> Option<(WorkspaceInvalidation, HashSet<String>)> {
    use notify::RecursiveMode;
    use std::collections::BTreeSet;

    let common = canonical_root(common).ok()?;
    let registry = common.join("worktrees");
    let mut paths = BTreeMap::from([(common.clone(), RecursiveMode::NonRecursive)]);
    let mut roots = BTreeSet::new();
    for registration in registrations {
        roots.insert(canonical_root(registration).ok()?);
    }
    roots.insert(common.parent()?.to_owned());
    match fs::symlink_metadata(&registry) {
        Ok(metadata) if metadata.is_dir() && !is_reparse_point(&metadata) => {
            paths.insert(registry.clone(), RecursiveMode::Recursive);
            for (i, entry) in fs::read_dir(&registry).ok()?.enumerate() {
                if i >= PROJECT_WATCH_CHECKOUTS {
                    return None;
                }
                let entry = entry.ok()?;
                if !entry.file_type().ok()?.is_dir() {
                    continue;
                }
                let admin = entry.path();
                let gitdir = metadata_line(&admin.join("gitdir")).ok()?;
                let pointer = pointer_path(&admin, &gitdir)?;
                if pointer.file_name()?.to_str()? != ".git" {
                    return None;
                }
                roots.insert(pointer.parent()?.to_owned());
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return None,
    }
    for name in ["refs", "info", "reftable"] {
        let path = common.join(name);
        if path.is_dir() {
            paths.insert(path, RecursiveMode::Recursive);
        }
    }

    // Watch every registered checkout pointer, including an absent root
    // that could appear without a Git administration change. Parent watches
    // cover deletion, replacement and .git changes without watching content.
    let mut bindings = HashSet::new();
    let key = |path: &Path| eliot_core::workspace_path_key(&path.to_string_lossy()).into_owned();
    for root in roots.iter().chain(std::iter::once(&common)) {
        bindings.insert(key(&root.join(".git")));
        for path in root.ancestors().filter(|path| path.has_root()) {
            bindings.insert(key(path));
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.is_dir() && !is_reparse_point(&metadata) => {
                    paths
                        .entry(path.to_owned())
                        .or_insert(RecursiveMode::NonRecursive);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                _ => return None,
            }
        }
    }
    if paths.len() > 4 * PROJECT_WATCH_CHECKOUTS {
        return None;
    }

    let checkout_roots = roots.iter().map(|root| key(root)).collect();
    let common = key(&common);
    let registry = key(&registry);
    WorkspaceInvalidation::watch_paths(paths.into_iter().collect(), Some(wake), move |path| {
        if path.to_str().is_none() {
            return (true, true);
        }
        let path = key(path);
        let admin = path
            .strip_prefix(&registry)
            .and_then(|tail| tail.strip_prefix('/'));
        let binding = bindings.contains(&path)
            || path == registry
            || admin.is_some_and(|tail| {
                !tail.contains('/') || tail.ends_with("/gitdir") || tail.ends_with("/commondir")
            });
        let tracked_file = |name: &str| {
            matches!(
                name,
                "HEAD"
                    | "head"
                    | "index"
                    | "config"
                    | "config.worktree"
                    | "packed-refs"
                    | "commondir"
                    | "gitdir"
                    | "locked"
                    | "objects"
            )
        };
        let common_metadata = path
            .strip_prefix(&common)
            .and_then(|tail| tail.strip_prefix('/'))
            .is_some_and(|tail| {
                tracked_file(tail)
                    || ["refs", "info", "reftable"].iter().any(|directory| {
                        tail == *directory
                            || tail
                                .strip_prefix(directory)
                                .is_some_and(|rest| rest.starts_with('/'))
                    })
            });
        let worktree_metadata = admin
            .and_then(|tail| tail.split_once('/'))
            .is_some_and(|(_, file)| tracked_file(file));
        (binding || common_metadata || worktree_metadata, binding)
    })
    .ok()
    .map(|watcher| (watcher, checkout_roots))
}

#[cfg(not(any(windows, target_os = "linux")))]
fn project_watcher(
    _common: &str,
    _registrations: &[String],
    _wake: Arc<tokio::sync::Notify>,
) -> Option<(WorkspaceInvalidation, HashSet<String>)> {
    None
}
