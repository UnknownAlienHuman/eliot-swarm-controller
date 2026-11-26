//! Filesystem observations and wake hints; Git readback remains authoritative.
//!
//! Keep one instance per admitted workspace. The monitor owns debounce,
//! bounded reconciliation, and fallback polling.
//! Uses notify 9.0.0 (CC0-1.0, Rust 1.91) with default features disabled.

use eliot_core::{self as core, Error};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
};

#[cfg(any(windows, target_os = "linux"))]
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
#[cfg(any(windows, target_os = "linux"))]
use std::path::{Path, PathBuf};

/// Only comparable inside one watcher lifetime. Consuming a dirty hint
/// cannot consume this observation or make an old revision current again.
#[derive(Clone)]
pub(crate) struct FilesystemRevision {
    source: Arc<AtomicU64>,
    observed: u64,
}

impl PartialEq for FilesystemRevision {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.source, &other.source) && self.observed == other.observed
    }
}
impl Eq for FilesystemRevision {}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum WatchChange {
    #[cfg(any(windows, target_os = "linux"))]
    Ignore = 0,
    #[cfg(any(windows, target_os = "linux"))]
    Content = 1,
    Rebind = 2,
    Lost = 3,
}

pub(crate) struct WorkspaceInvalidation {
    dirty: Arc<AtomicBool>,
    coverage: Arc<AtomicU8>,
    revision: Arc<AtomicU64>,
    #[cfg(any(windows, target_os = "linux"))]
    _watcher: RecommendedWatcher,
}

impl WorkspaceInvalidation {
    /// Watch the checkout and verified local Git metadata. A parent watch is
    /// nonrecursive and only invalidates when the watched root itself changes.
    #[cfg(any(windows, target_os = "linux"))]
    pub(crate) fn watch(workspace: &str, common: Option<&str>) -> core::Result<Self> {
        use crate::git_monitor::{canonical_root, reject_links};

        let root = canonical_root(workspace)?;
        let dot_git = root.join(".git");
        reject_links(&dot_git)?;
        let root_parent = root.parent().ok_or_else(watch_unavailable)?;
        let mut paths = vec![
            (root.clone(), RecursiveMode::Recursive),
            (root_parent.to_owned(), RecursiveMode::NonRecursive),
        ];
        let mut scope = WatchScope {
            root: root.clone(),
            dot_git,
            linked: None,
        };
        if !scope.dot_git.is_dir() {
            let common = canonical_root(common.ok_or_else(watch_unavailable)?)?;
            let admin = crate::workspace_git::linked_watch_admin(&root, &common)
                .ok_or_else(watch_unavailable)?;
            let admin = canonical_root(admin.to_str().ok_or_else(watch_unavailable)?)?;
            let registry = common.join("worktrees");
            paths.extend([
                (registry, RecursiveMode::NonRecursive),
                (admin.clone(), RecursiveMode::NonRecursive),
                (common.clone(), RecursiveMode::NonRecursive),
            ]);
            if let Some(common_parent) = common.parent()
                && !paths.iter().any(|(path, _)| path == common_parent)
            {
                paths.push((common_parent.to_owned(), RecursiveMode::NonRecursive));
            }
            for name in ["refs", "info", "reftable"] {
                let path = common.join(name);
                if path.is_dir() {
                    paths.push((path, RecursiveMode::Recursive));
                }
            }
            scope.linked = Some((admin, common));
        }
        Self::watch_paths(paths, None, move |path, _kind| {
            if scope.binding_changed(path) {
                WatchChange::Lost
            } else if scope.includes(path) {
                WatchChange::Content
            } else {
                WatchChange::Ignore
            }
        })
    }

    /// Used by the repository readback and the managed workspace monitor.
    /// A binding change invalidates the proof until the paths are registered again.
    #[cfg(any(windows, target_os = "linux"))]
    pub(crate) fn watch_paths(
        paths: Vec<(PathBuf, RecursiveMode)>,
        wake: Option<Arc<tokio::sync::Notify>>,
        classify: impl Fn(&Path, &EventKind) -> WatchChange + Send + 'static,
    ) -> core::Result<Self> {
        use crate::git_monitor::reject_links;
        let dirty = Arc::new(AtomicBool::new(true));
        let coverage = Arc::new(AtomicU8::new(0));
        let revision = Arc::new(AtomicU64::new(0));
        let callback_dirty = Arc::clone(&dirty);
        let callback_coverage = Arc::clone(&coverage);
        let callback_revision = Arc::clone(&revision);
        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<notify::Event>| {
                let change = match result {
                    Ok(event) if event.need_rescan() => WatchChange::Lost,
                    Ok(event) if !matches!(event.kind, EventKind::Access(_)) => event
                        .paths
                        .iter()
                        .map(|path| classify(path, &event.kind))
                        .max()
                        .unwrap_or(WatchChange::Content),
                    Ok(_) => WatchChange::Ignore,
                    Err(_) => WatchChange::Lost,
                };
                if change >= WatchChange::Rebind {
                    callback_coverage.fetch_max(change as u8, Ordering::Release);
                }
                if change != WatchChange::Ignore {
                    callback_revision.fetch_add(1, Ordering::AcqRel);
                    callback_dirty.store(true, Ordering::Release);
                    if let Some(wake) = &wake {
                        wake.notify_one();
                    }
                }
            },
            Config::default().with_follow_symlinks(false),
        )
        .map_err(|_| watch_unavailable())?;
        for (path, mode) in paths {
            reject_links(&path)?;
            watcher
                .watch(&path, mode)
                .map_err(|_| watch_unavailable())?;
        }
        Ok(Self {
            dirty,
            coverage,
            revision,
            _watcher: watcher,
        })
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    pub(crate) fn watch(_workspace: &str, _common: Option<&str>) -> core::Result<Self> {
        Err(Error::new(
            "unavailable",
            "native filesystem invalidation is unavailable on this platform; continue Git polling",
        ))
    }

    pub(crate) fn revision(&self) -> Option<FilesystemRevision> {
        let observed = self.revision.load(Ordering::Acquire);
        (self.coverage.load(Ordering::Acquire) < WatchChange::Rebind as u8).then(|| {
            FilesystemRevision {
                source: Arc::clone(&self.revision),
                observed,
            }
        })
    }

    /// Consume coalesced hints before the Git read. Changes during the read
    /// stay pending for the next debounced pass.
    pub(crate) fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::AcqRel)
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    /// Latched after a backend error, rescan notice or watched binding change.
    pub(crate) fn is_degraded(&self) -> bool {
        self.coverage.load(Ordering::Acquire) == WatchChange::Lost as u8
    }
}

#[cfg(any(windows, target_os = "linux"))]
struct WatchScope {
    root: PathBuf,
    dot_git: PathBuf,
    linked: Option<(PathBuf, PathBuf)>,
}

#[cfg(any(windows, target_os = "linux"))]
impl WatchScope {
    fn includes(&self, path: &Path) -> bool {
        path.starts_with(&self.root)
            || self.linked.as_ref().is_some_and(|(admin, common)| {
                path.starts_with(admin)
                    || [
                        "HEAD",
                        "config",
                        "config.worktree",
                        "packed-refs",
                        "refs",
                        "info",
                        "reftable",
                    ]
                    .iter()
                    .any(|name| path.starts_with(common.join(name)))
            })
    }

    fn binding_changed(&self, path: &Path) -> bool {
        path == self.root
            || path == self.dot_git
            || self.linked.as_ref().is_some_and(|(admin, common)| {
                path == admin
                    || path == common
                    || path == common.join("worktrees")
                    || path == admin.join("commondir")
                    || path == admin.join("gitdir")
                    || ["refs", "info", "reftable"]
                        .iter()
                        .any(|name| path == common.join(name))
            })
    }
}

#[cfg(any(windows, target_os = "linux"))]
fn watch_unavailable() -> Error {
    Error::new(
        "unavailable",
        "filesystem invalidation could not cover the checkout and Git metadata; continue Git polling",
    )
}
