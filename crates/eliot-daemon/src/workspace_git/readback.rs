//! Read-only Git checkout identities rooted in an existing registration.

use super::*;
use crate::daemon::workers::WorkerReporter;
#[cfg(any(windows, target_os = "linux"))]
use crate::fs_invalidation::WatchChange;
use crate::fs_invalidation::{FilesystemRevision, WorkspaceInvalidation};
use crate::platform::is_reparse_point;
use eliot_core::api::{ProjectCheckoutView, ProjectRegistrationView};
use eliot_plugin::sync::LockExt;
use futures_util::{StreamExt, stream};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex, OnceLock},
};

const MAX_CHECKOUTS: usize = 1_000;
const CHECKOUT_READBACK_BYTES: usize = 900 * 1024;
const MAX_UNVERIFIED_PROJECT_READS: u8 = 3;
const PROJECT_WATCH_BINDINGS_UNAVAILABLE: &str =
    "project Git watcher cannot cover the checkout bindings";
#[cfg(any(windows, target_os = "linux"))]
const PROJECT_WATCH_CHECKOUTS: usize = 256;

#[derive(Default)]
pub(crate) struct ProjectReadCache {
    entries: Mutex<HashMap<String, Arc<ProjectObservation>>>,
    request_failures: Mutex<HashMap<String, Error>>,
    wake: Arc<tokio::sync::Notify>,
}

enum ProjectObservation {
    Current(CachedProjectReadback),
    Pending(PendingProjectWatch),
    Fallback(Error),
}

enum ObserveOutcome {
    Ready(CachedProjectReadback),
    Unverified(PendingProjectWatch),
}

struct CachedProjectReadback {
    watched: WatchedProject,
    origin: Option<String>,
    checkouts: Vec<ProjectCheckoutView>,
}

struct WatchedProject {
    watch: Arc<ProjectWatch>,
    revision: FilesystemRevision,
    stamps: Vec<CheckoutReadStamp>,
}

struct ProjectWatch {
    watcher: WorkspaceInvalidation,
    registrations: Vec<String>,
    checkout_roots: HashSet<String>,
    root_identity: Option<RootIdentity>,
}

/// The repository root behind a retained watcher. `None` when the root
/// could not be pinned: the watcher still observes, it is just never
/// reused, so an identity failure can never force a fallback by itself.
#[derive(Clone)]
struct RootIdentity {
    root: PathBuf,
    identity: Vec<eliot_plugin::FileIdentity>,
}

/// Retain the watcher and bound consecutive attempts at a verified read.
struct PendingProjectWatch {
    count: u8,
    watch: Arc<ProjectWatch>,
}

impl ProjectReadCache {
    pub(crate) async fn apply(
        &self,
        registrations: &mut [ProjectRegistrationView],
        single: bool,
        include_git_state: bool,
        force_fallback: bool,
        reporter: &WorkerReporter,
    ) {
        let mut groups = BTreeMap::<String, Vec<&mut ProjectRegistrationView>>::new();
        for registration in &mut *registrations {
            groups
                .entry(registration.repo_common_dir.clone())
                .or_default()
                .push(registration);
        }

        let deadline = Instant::now() + ORIGIN_READ_TIME;
        let git = OnceLock::new();
        stream::iter(groups)
            .for_each_concurrent(4, |(common, group)| {
                let git = &git;
                async move {
                    let entry = self.entries.lock_recover().get(&common).cloned();
                    if let Some(ProjectObservation::Current(entry)) = entry.as_deref()
                        && entry.watched.current()
                    {
                        let mut paths = group.iter().map(|registration| &registration.checkout);
                        let matched = if single {
                            paths.all(|path| entry.watched.watch.registrations.contains(path))
                        } else {
                            entry.watched.watch.registrations.iter().eq(paths)
                        };
                        if matched {
                            let origin = entry.origin.clone();
                            let checkouts = entry.checkouts.clone();
                            if entry.watched.current() {
                                assign_readbacks(group, origin, checkouts);
                                return;
                            }
                        }
                    }

                    let unsupported = force_fallback
                        || !cfg!(any(windows, target_os = "linux"))
                        || matches!(entry.as_deref(), Some(ProjectObservation::Fallback(_)));
                    if include_git_state && !unsupported {
                        for registration in group {
                            registration.git_refreshing = true;
                        }
                        self.wake.notify_one();
                        return;
                    }
                    let paths = group
                        .iter()
                        .map(|registration| registration.checkout.clone())
                        .collect();
                    let (origin, checkouts, request_failure) =
                        match git.get_or_init(WorkspaceGit::new) {
                            Ok(git) => git.read_project_for_request(&common, paths, deadline).await,
                            Err(error) => (None, Vec::new(), Some(error.clone())),
                        };
                    match request_failure {
                        Some(error) => self.report_request_failure(&common, &error, reporter),
                        None => self.clear_request_failure(&common),
                    };
                    let mut group = group;
                    for registration in &mut group {
                        registration.git_watch_unsupported = include_git_state && unsupported;
                    }
                    assign_readbacks(group, origin, checkouts);
                    if !unsupported {
                        self.wake.notify_one();
                    }
                }
            })
            .await;
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
        self.request_failures
            .lock_recover()
            .retain(|key, _| registrations.contains_key(key));
    }

    async fn verified(&self, common: &str, registrations: &[String]) -> bool {
        let entry = self.entries.lock_recover().get(common).cloned();
        let Some(ProjectObservation::Current(entry)) = entry.as_deref() else {
            return false;
        };
        if entry.watched.watch.registrations != registrations || !entry.watched.current() {
            return false;
        }
        let paths = registrations.to_vec();
        let common = common.to_owned();
        let stamps = tokio::task::spawn_blocking(move || project_stamps(&paths, &common)).await;
        matches!(stamps, Ok(Ok(stamps)) if stamps == entry.watched.stamps && entry.watched.current())
    }

    fn take_reusable(&self, common: &str) -> Option<Arc<ProjectObservation>> {
        let mut entries = self.entries.lock_recover();
        if matches!(
            entries.get(common).map(Arc::as_ref),
            Some(ProjectObservation::Current(_)) | Some(ProjectObservation::Pending(_))
        ) {
            entries.remove(common)
        } else {
            None
        }
    }

    fn record_fallback(&self, common: String, error: Error, reporter: &WorkerReporter) {
        {
            let mut entries = self.entries.lock_recover();
            if let Some(ProjectObservation::Fallback(previous)) =
                entries.get(&common).map(Arc::as_ref)
                && previous.code == error.code
                && previous.message == error.message
            {
                return;
            }
            entries.insert(
                common.clone(),
                Arc::new(ProjectObservation::Fallback(error.clone())),
            );
        }
        reporter.project_read_failed(&common, &error);
    }

    /// Log a request-path live Git read failure only on transition, mirroring
    /// [`Self::record_fallback`]. Legacy callers and opted-in polls in
    /// fallback otherwise log the same failure on every poll.
    fn report_request_failure(&self, common: &str, error: &Error, reporter: &WorkerReporter) {
        {
            let mut failures = self.request_failures.lock_recover();
            if let Some(previous) = failures.get(common)
                && previous.code == error.code
                && previous.message == error.message
            {
                return;
            }
            failures.insert(common.to_owned(), error.clone());
        }
        reporter.project_read_failed(common, error);
    }

    fn clear_request_failure(&self, common: &str) {
        self.request_failures.lock_recover().remove(common);
    }

    fn record_pending(
        &self,
        common: String,
        pending: PendingProjectWatch,
        reporter: &WorkerReporter,
    ) {
        {
            let mut entries = self.entries.lock_recover();
            if entries
                .get(&common)
                .is_some_and(|entry| matches!(entry.as_ref(), ProjectObservation::Fallback(_)))
            {
                return;
            }
            if pending.count < MAX_UNVERIFIED_PROJECT_READS {
                entries.insert(common, Arc::new(ProjectObservation::Pending(pending)));
                return;
            }
        }
        self.record_fallback(
            common,
            problem(
                "unavailable",
                "project Git checkout revision changed during three consecutive observations",
            ),
            reporter,
        );
    }
}

impl WatchedProject {
    fn current(&self) -> bool {
        self.watch.watcher.revision().as_ref() == Some(&self.revision)
    }
}

impl ProjectWatch {
    fn open(
        common: &str,
        registrations: Vec<String>,
        wake: Arc<tokio::sync::Notify>,
    ) -> Result<Self> {
        // Pin the root before arming the watch so reuse detects a swap during registration.
        let root_identity = project_root_identity(common);
        let (watcher, checkout_roots) = project_watcher(common, &registrations, wake)?;
        Ok(Self {
            watcher,
            registrations,
            checkout_roots,
            root_identity,
        })
    }

    fn reusable(&self, common: &str, registrations: &[String]) -> bool {
        self.registrations == registrations
            && self.watcher.revision().is_some()
            && self
                .root_identity
                .as_ref()
                .is_some_and(|root| reused_root_identity(common, root))
    }
}

fn project_stamps(paths: &[String], common: &str) -> Result<Vec<CheckoutReadStamp>> {
    paths
        .iter()
        .map(|path| DiffCheckout::open(path, Some(common))?.read_stamp())
        .collect()
}

/// Pin the repository root behind a retained `Pending` watcher. A replaced
/// or moved root must rebuild even when the checkout paths still match.
/// `None` when the root cannot be pinned; the caller then never reuses.
fn project_root_identity(common: &str) -> Option<RootIdentity> {
    let canonical = canonical_root(common).ok()?;
    let identity = Directory::open(&canonical)
        .ok()?
        .directory_identities(1)
        .ok()?;
    Some(RootIdentity {
        root: canonical,
        identity,
    })
}

fn reused_root_identity(common: &str, stored: &RootIdentity) -> bool {
    canonical_root(common).is_ok_and(|current| current.as_path() == stored.root.as_path())
        && Directory::open(&stored.root).is_ok_and(|directory| {
            directory
                .directory_identities(1)
                .is_ok_and(|identity| identity.as_slice() == stored.identity.as_slice())
        })
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
        reporter: &WorkerReporter,
    ) {
        let mut groups = BTreeMap::<String, Vec<String>>::new();
        for registration in registrations {
            groups
                .entry(registration.repo_common_dir.clone())
                .or_default()
                .push(registration.checkout.clone());
        }
        cache.retain(&groups);
        let git = OnceLock::new();
        stream::iter(groups)
            .for_each_concurrent(4, |(common, paths)| {
                let git = &git;
                async move {
                    if cache.verified(&common, &paths).await {
                        return;
                    }
                    let previous = cache.take_reusable(&common);
                    let result = match git.get_or_init(Self::new) {
                        Ok(git) => {
                            git.observe_project(&common, paths, &cache.wake, previous)
                                .await
                        }
                        Err(error) => {
                            tokio::task::spawn_blocking(move || drop(previous));
                            Err(error.clone())
                        }
                    };
                    match result {
                        Ok(ObserveOutcome::Ready(readback)) => {
                            cache
                                .entries
                                .lock_recover()
                                .insert(common, Arc::new(ProjectObservation::Current(readback)));
                        }
                        Ok(ObserveOutcome::Unverified(pending)) => {
                            cache.record_pending(common, pending, reporter);
                        }
                        Err(error) => cache.record_fallback(common, error, reporter),
                    }
                }
            })
            .await;
    }

    async fn read_project_for_request(
        &self,
        common: &str,
        paths: Vec<String>,
        deadline: Instant,
    ) -> (Option<String>, Vec<ProjectCheckoutView>, Option<Error>) {
        match tokio::time::timeout_at(deadline, self.read_project(common, paths, None, deadline))
            .await
        {
            Ok((origin, checkouts, complete)) => {
                if !complete {
                    (
                        origin,
                        checkouts,
                        Some(problem(
                            "unavailable",
                            "live project Git read could not verify the complete checkout inventory",
                        )),
                    )
                } else {
                    (origin, checkouts, None)
                }
            }
            Err(_) => (
                None,
                Vec::new(),
                Some(problem(
                    "unavailable",
                    "live project Git read exceeded its deadline",
                )),
            ),
        }
    }

    async fn observe_project(
        &self,
        common: &str,
        paths: Vec<String>,
        wake: &Arc<tokio::sync::Notify>,
        previous: Option<Arc<ProjectObservation>>,
    ) -> Result<ObserveOutcome> {
        let watch_common = common.to_owned();
        let watch_paths = paths.clone();
        let watch_wake = Arc::clone(wake);
        let (mut watch, mut rebuilt, attempt) = tokio::task::spawn_blocking(move || {
            let (old, attempt) = match previous.as_deref() {
                Some(ProjectObservation::Current(entry)) => (Some(&entry.watched.watch), 1),
                Some(ProjectObservation::Pending(pending)) => {
                    (Some(&pending.watch), pending.count.saturating_add(1))
                }
                _ => (None, 1),
            };
            if let Some(old) = old
                && old.reusable(&watch_common, &watch_paths)
            {
                return Ok((Arc::clone(old), false, attempt));
            }
            let mut recovering = old
                .is_some_and(|old| old.registrations == watch_paths && old.watcher.is_degraded());
            let fresh = match ProjectWatch::open(
                &watch_common,
                watch_paths.clone(),
                Arc::clone(&watch_wake),
            ) {
                Err(error)
                    if !recovering
                        && error.message.as_str() == PROJECT_WATCH_BINDINGS_UNAVAILABLE =>
                {
                    // A discovered path can disappear while the watch set is being armed.
                    recovering = true;
                    ProjectWatch::open(&watch_common, watch_paths, watch_wake)?
                }
                result => result?,
            };
            Ok::<_, Error>((Arc::new(fresh), recovering, attempt))
        })
        .await
        .map_err(|_| problem("internal", "project Git watcher task failed"))??;

        loop {
            let result = self.observe_project_revision(common, &watch, attempt).await;
            if !watch.watcher.is_degraded() {
                return result;
            }
            drop(result);
            if rebuilt {
                return Err(problem(
                    "unavailable",
                    "project Git watcher lost checkout coverage",
                ));
            }
            rebuilt = true;
            let common = common.to_owned();
            let paths = paths.clone();
            let wake = Arc::clone(wake);
            watch = tokio::task::spawn_blocking(move || {
                drop(watch);
                ProjectWatch::open(&common, paths, wake).map(Arc::new)
            })
            .await
            .map_err(|_| problem("internal", "project Git watcher task failed"))??;
        }
    }

    async fn observe_project_revision(
        &self,
        common: &str,
        watch: &Arc<ProjectWatch>,
        attempt: u8,
    ) -> Result<ObserveOutcome> {
        let unverified = || {
            ObserveOutcome::Unverified(PendingProjectWatch {
                count: attempt,
                watch: Arc::clone(watch),
            })
        };
        let Some(revision) = watch.watcher.revision() else {
            return Ok(unverified());
        };
        let read_stamps = || {
            let common = common.to_owned();
            let paths = watch.registrations.clone();
            tokio::task::spawn_blocking(move || project_stamps(&paths, &common))
        };
        let is_current = || watch.watcher.revision().as_ref() == Some(&revision);
        let stamps = read_stamps().await;
        if !is_current() {
            return Ok(unverified());
        }
        let stamps =
            stamps.map_err(|_| problem("internal", "project checkout stamp task failed"))??;
        let deadline = Instant::now() + ORIGIN_READ_TIME;
        let (origin, mut checkouts, complete) = self
            .read_project(
                common,
                watch.registrations.clone(),
                Some(&watch.checkout_roots),
                deadline,
            )
            .await;
        if !is_current() {
            return Ok(unverified());
        }
        if !complete {
            return Err(problem(
                "unavailable",
                "project Git observation could not verify the complete checkout inventory",
            ));
        }
        let after = read_stamps().await;
        let mut remaining = CHECKOUT_READBACK_BYTES;
        bound_checkouts(&mut checkouts, &mut remaining);
        if !is_current() {
            return Ok(unverified());
        }
        let after =
            after.map_err(|_| problem("internal", "project checkout stamp task failed"))??;
        if stamps != after {
            return Ok(unverified());
        }
        Ok(ObserveOutcome::Ready(CachedProjectReadback {
            watched: WatchedProject {
                watch: Arc::clone(watch),
                revision,
                stamps,
            },
            origin,
            checkouts,
        }))
    }

    async fn read_project(
        &self,
        common_dir: &str,
        registration_keys: Vec<String>,
        watched: Option<&HashSet<String>>,
        deadline: Instant,
    ) -> (Option<String>, Vec<ProjectCheckoutView>, bool) {
        let (origin, (checkouts, complete)) =
            tokio::join!(self.project_origin(common_dir, deadline), async {
                let Ok(common) = canonical_root(common_dir) else {
                    return (Vec::new(), false);
                };
                let registered = registration_keys;
                let (repositories, inventory) = tokio::join!(
                    tokio::task::spawn_blocking({
                        let common = common.clone();
                        move || {
                            let main = registered
                                .iter()
                                .find_map(|checkout| main_checkout(&common, Path::new(checkout)))
                                .or_else(|| main_checkout(&common, common.parent()?));
                            let linked = linked_checkouts(&common);
                            main.into_iter()
                                .chain(linked)
                                .take(MAX_CHECKOUTS)
                                .collect::<Vec<_>>()
                        }
                    }),
                    self.project_entries(&common, deadline),
                );
                let mut complete = repositories.is_ok() && inventory.is_ok();
                let inventory = inventory.unwrap_or_default();
                complete &= watched.is_none_or(|watched| {
                    // The first Git record is the repository's
                    // main checkout, which can be outside the
                    // registration. Only main_checkout selects
                    // it. All linked records must be watched.
                    inventory.iter().skip(1).all(|entry| {
                        entry.path.to_str().is_some_and(|path| {
                            watched.contains(eliot_core::workspace_path_key(path).as_ref())
                        })
                    })
                });
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
                        self.checkout_readback(repo, entry.cloned(), deadline)
                    })
                    .buffered(8)
                    .filter_map(std::future::ready)
                    .collect()
                    .await;
                complete &= checkouts.len() == expected;
                complete &= watched.is_none_or(|watched| {
                    checkouts.iter().all(|checkout| {
                        watched.contains(eliot_core::workspace_path_key(&checkout.path).as_ref())
                    })
                });
                (checkouts, complete)
            });
        let complete = complete && origin.is_ok() && Instant::now() < deadline;
        (origin.ok().flatten(), checkouts, complete)
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
) -> Result<(WorkspaceInvalidation, HashSet<String>)> {
    use notify::{EventKind, RecursiveMode, event::ModifyKind};
    use std::collections::BTreeSet;

    let unsupported = || problem("unsupported", PROJECT_WATCH_BINDINGS_UNAVAILABLE);
    let common = canonical_root(common).map_err(|_| unsupported())?;
    let registry = common.join("worktrees");
    let mut paths = BTreeMap::from([(common.clone(), RecursiveMode::NonRecursive)]);
    let mut roots = BTreeSet::new();
    for registration in registrations {
        roots.insert(canonical_root(registration).map_err(|_| unsupported())?);
    }
    roots.insert(common.parent().ok_or_else(unsupported)?.to_owned());
    let required_roots = roots.clone();
    match fs::symlink_metadata(&registry) {
        Ok(metadata) if metadata.is_dir() && !is_reparse_point(&metadata) => {
            paths.insert(registry.clone(), RecursiveMode::Recursive);
            for (i, entry) in fs::read_dir(&registry)
                .map_err(|_| unsupported())?
                .enumerate()
            {
                if i >= PROJECT_WATCH_CHECKOUTS {
                    return Err(problem(
                        "unsupported",
                        "project Git watcher checkout limit exceeded",
                    ));
                }
                let entry = entry.map_err(|_| unsupported())?;
                if !entry.file_type().map_err(|_| unsupported())?.is_dir() {
                    continue;
                }
                let admin = entry.path();
                let backlink = admin.join("gitdir");
                let gitdir = match metadata_line(&backlink) {
                    Ok(gitdir) => gitdir,
                    // Git can create the registry entry before its backlink,
                    // or remove the backlink before retiring the entry.
                    Err(_)
                        if matches!(fs::symlink_metadata(&backlink), Err(error)
                        if error.kind() == std::io::ErrorKind::NotFound) =>
                    {
                        continue;
                    }
                    Err(_) => return Err(unsupported()),
                };
                let pointer = pointer_path(&admin, &gitdir).ok_or_else(unsupported)?;
                if pointer.file_name().and_then(|name| name.to_str()) != Some(".git") {
                    return Err(unsupported());
                }
                roots.insert(pointer.parent().ok_or_else(unsupported)?.to_owned());
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        _ => return Err(unsupported()),
    }
    let metadata_roots = ["refs", "info", "reftable"].map(|name| common.join(name));
    for path in &metadata_roots {
        if path.is_dir() {
            paths.insert(path.clone(), RecursiveMode::Recursive);
        }
    }

    // Watch every registered checkout pointer, including an absent root
    // that could appear without a Git administration change. Parent watches
    // cover deletion, replacement and .git changes without watching content.
    let mut required_bindings = HashSet::new();
    let mut linked_bindings = HashSet::new();
    let mut directories = HashSet::new();
    let key = |path: &Path| eliot_core::workspace_path_key(&path.to_string_lossy()).into_owned();
    for root in roots.iter().chain(std::iter::once(&common)) {
        let bindings = if required_roots.contains(root) || *root == common {
            &mut required_bindings
        } else {
            &mut linked_bindings
        };
        bindings.insert(key(&root.join(".git")));
        if matches!(fs::symlink_metadata(root.join(".git")), Ok(metadata) if metadata.is_dir() && !is_reparse_point(&metadata))
        {
            directories.insert(key(&root.join(".git")));
        }
        for path in root.ancestors().filter(|path| path.has_root()) {
            bindings.insert(key(path));
            directories.insert(key(path));
            match fs::symlink_metadata(path) {
                Ok(metadata) if metadata.is_dir() && !is_reparse_point(&metadata) => {
                    paths
                        .entry(path.to_owned())
                        .or_insert(RecursiveMode::NonRecursive);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                _ => return Err(unsupported()),
            }
        }
    }
    if paths.len() > 4 * PROJECT_WATCH_CHECKOUTS {
        return Err(problem(
            "unsupported",
            "project Git watcher path limit exceeded",
        ));
    }

    let checkout_roots = roots.iter().map(|root| key(root)).collect();
    let scope = metadata_roots
        .into_iter()
        .chain(std::iter::once(registry.clone()))
        .map(|path| {
            let watched = paths.contains_key(&path);
            (path, watched)
        })
        .collect::<Vec<_>>();
    let common = key(&common);
    let registry = key(&registry);
    let watcher = WorkspaceInvalidation::watch_paths(
        paths.into_iter().collect(),
        Some(wake),
        move |path, kind| {
            if path.to_str().is_none() {
                return WatchChange::Lost;
            }
            let path = key(path);
            let admin = path
                .strip_prefix(&registry)
                .and_then(|tail| tail.strip_prefix('/'));
            let common_tail = path.strip_prefix(&common).and_then(|tail| tail.strip_prefix('/'));
            let metadata_root = matches!(common_tail, Some("refs" | "info" | "reftable"));
            let admin_root = admin.is_some_and(|tail| !tail.contains('/'));
            let registry_root = path == registry;
            let directory_modify =
                matches!(kind, EventKind::Modify(change) if !matches!(change, ModifyKind::Name(_)))
                    && (directories.contains(&path) || metadata_root || registry_root || admin_root);
            if !directory_modify {
                if required_bindings.contains(&path) || metadata_root {
                    return WatchChange::Lost;
                }
                if linked_bindings.contains(&path)
                    || registry_root
                    || admin_root
                    || admin.is_some_and(|tail| {
                        tail.ends_with("/gitdir") || tail.ends_with("/commondir")
                    })
                {
                    return WatchChange::Rebind;
                }
            }
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
            let common_metadata = common_tail.is_some_and(|tail| {
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
            if common_metadata || worktree_metadata || path == common || registry_root || admin_root {
                WatchChange::Content
            } else {
                WatchChange::Ignore
            }
        },
    )
    .map_err(|_| unsupported())?;
    // A scope that appeared before its parent watch was armed has no create event.
    if scope
        .iter()
        .any(|(path, watched)| path.is_dir() != *watched)
    {
        return Err(unsupported());
    }
    Ok((watcher, checkout_roots))
}

#[cfg(not(any(windows, target_os = "linux")))]
fn project_watcher(
    _common: &str,
    _registrations: &[String],
    _wake: Arc<tokio::sync::Notify>,
) -> Result<(WorkspaceInvalidation, HashSet<String>)> {
    Err(problem(
        "unsupported",
        "project Git watchers are unavailable on this platform",
    ))
}
