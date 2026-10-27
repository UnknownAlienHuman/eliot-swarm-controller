//! Filesystem wake hints only; Git readback remains authoritative.
//!
//! Keep one instance per admitted workspace. The monitor owns the minimum
//! two-second debounce and polling schedule; watcher failure never replaces polling.
//! Uses notify 8.2.1 (CC0-1.0, Rust 1.85) with default features disabled.

use eliot_core::{self as core, Error};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

#[cfg(any(windows, target_os = "linux"))]
use notify::{Config, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
#[cfg(any(windows, target_os = "linux"))]
use std::path::Path;

pub(crate) struct WorkspaceInvalidation {
    dirty: Arc<AtomicBool>,
    degraded: Arc<AtomicBool>,
    #[cfg(any(windows, target_os = "linux"))]
    _watcher: RecommendedWatcher,
}

impl WorkspaceInvalidation {
    /// Watch exactly the already-admitted worktree root, without watching its parent.
    #[cfg(any(windows, target_os = "linux"))]
    pub(crate) fn watch(workspace: &str) -> core::Result<Self> {
        let root = Path::new(workspace);
        if !root.is_absolute() || !root.is_dir() {
            return Err(Error::new(
                "unavailable",
                "filesystem invalidation requires an available admitted worktree root",
            ));
        }
        let dirty = Arc::new(AtomicBool::new(true));
        let degraded = Arc::new(AtomicBool::new(false));
        let callback_dirty = Arc::clone(&dirty);
        let callback_degraded = Arc::clone(&degraded);
        let mut watcher = RecommendedWatcher::new(
            move |result: notify::Result<notify::Event>| match result {
                Ok(event) => {
                    let rescan = event.need_rescan();
                    if rescan {
                        callback_degraded.store(true, Ordering::Release);
                    }
                    // Git's own reads must not create a refresh feedback loop.
                    if rescan || !matches!(event.kind, EventKind::Access(_)) {
                        callback_dirty.store(true, Ordering::Release);
                    }
                }
                Err(_) => {
                    callback_degraded.store(true, Ordering::Release);
                    callback_dirty.store(true, Ordering::Release);
                }
            },
            Config::default().with_follow_symlinks(false),
        )
        .map_err(|_| {
            Error::new(
                "unavailable",
                "filesystem invalidation backend could not start; continue Git polling",
            )
        })?;
        watcher.watch(root, RecursiveMode::Recursive).map_err(|_| {
            Error::new(
                "unavailable",
                "filesystem invalidation could not watch the admitted root; continue Git polling",
            )
        })?;
        Ok(Self {
            dirty,
            degraded,
            _watcher: watcher,
        })
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    pub(crate) fn watch(_workspace: &str) -> core::Result<Self> {
        Err(Error::new(
            "unavailable",
            "native filesystem invalidation is unavailable on this platform; continue Git polling",
        ))
    }

    /// Consume coalesced hints. A false result never authorizes skipping polling.
    pub(crate) fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::AcqRel)
    }

    /// Latched after a backend error or rescan notice; polling remains authoritative.
    pub(crate) fn is_degraded(&self) -> bool {
        self.degraded.load(Ordering::Acquire)
    }
}
