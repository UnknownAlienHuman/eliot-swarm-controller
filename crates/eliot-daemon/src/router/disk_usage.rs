//! Logical disk usage measured in resumable, paced filesystem walks.

use super::Router;
use crate::platform::{is_reparse_point, script_file::Directory};
use eliot_core::api::{AutomaticCleanupView, DiskUsageDirectory, DiskUsageView};
use eliot_plugin::{FileIdentity, sync::LockExt};
use std::{
    collections::HashMap,
    fs::{self, ReadDir},
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tokio::sync::watch;

const REFRESH_TTL: Duration = Duration::from_secs(5 * 60);
const REFRESH_REST_MULTIPLIER: u32 = 64;
const FIRST_SNAPSHOT_WAIT: Duration = Duration::from_millis(500);
const SLICE_TIME: Duration = Duration::from_millis(100);
const SLICE_ENTRIES: usize = 25_000;
const SLICE_REST: Duration = Duration::from_millis(200);
const SLICE_REST_MULTIPLIER: u32 = 16;
const MAX_SCAN_DEPTH: usize = 128;
const CLEANUP_PREVIEW_ENTRIES: usize = 500_000;

static CACHE: OnceLock<Mutex<HashMap<PathBuf, Arc<CacheEntry>>>> = OnceLock::new();

struct CacheEntry {
    state: Mutex<CacheState>,
    changed: watch::Sender<u64>,
}

#[derive(Default)]
struct CacheState {
    snapshot: Option<Snapshot>,
    refreshing: bool,
}

#[derive(Clone)]
struct Snapshot {
    started_at: Instant,
    completed_at: Instant,
    refresh_delay: Duration,
    artifacts: DiskUsageDirectory,
    worktrees: DiskUsageDirectory,
    plugins: DiskUsageDirectory,
}

impl Snapshot {
    async fn scan(root: &Path) -> Self {
        let started_at = Instant::now();
        let (artifacts, artifact_time) = scan_bucket(root.join("artifacts")).await;
        let (worktrees, worktree_time) = scan_bucket(root.join("worktrees")).await;
        let (plugins, plugin_time) = scan_bucket(root.join("plugins")).await;
        let active_time = artifact_time + worktree_time + plugin_time;
        Self {
            started_at,
            completed_at: Instant::now(),
            refresh_delay: REFRESH_TTL.max(active_time.saturating_mul(REFRESH_REST_MULTIPLIER)),
            artifacts,
            worktrees,
            plugins,
        }
    }
}

impl Router {
    pub(super) async fn disk_usage_view(
        &self,
        cleanup: &AutomaticCleanupView,
        owner_decision_count: usize,
    ) -> DiskUsageView {
        let root = self.inner.data_root.clone();
        let entry = cache_entry(&root);
        let mut changed = entry.changed.subscribe();
        let (snapshot, refreshing) = {
            let mut state = entry.state.lock_recover();
            if !state.refreshing
                && state.snapshot.as_ref().is_none_or(|snapshot| {
                    snapshot.completed_at.elapsed() >= snapshot.refresh_delay
                })
            {
                state.refreshing = true;
                let entry = entry.clone();
                drop(tokio::spawn(async move {
                    let snapshot = Snapshot::scan(&root).await;
                    let mut state = entry.state.lock_recover();
                    state.snapshot = Some(snapshot);
                    state.refreshing = false;
                    drop(state);
                    entry
                        .changed
                        .send_modify(|version| *version = version.wrapping_add(1));
                }));
            }
            (state.snapshot.clone(), state.refreshing)
        };
        let (snapshot, refreshing) = if snapshot.is_none() && refreshing {
            let _ = tokio::time::timeout(FIRST_SNAPSHOT_WAIT, changed.changed()).await;
            let state = entry.state.lock_recover();
            (state.snapshot.clone(), state.refreshing)
        } else {
            (snapshot, refreshing)
        };
        let (artifacts, worktrees, plugins, snapshot_age_ms) = match snapshot {
            Some(snapshot) => (
                snapshot.artifacts,
                snapshot.worktrees,
                snapshot.plugins,
                Some(u64::try_from(snapshot.started_at.elapsed().as_millis()).unwrap_or(u64::MAX)),
            ),
            None => (failed("busy"), failed("busy"), failed("busy"), None),
        };
        let (owner_decision_worktrees, owner_decision_error_code) = match &cleanup.last_sweep {
            Some(sweep) if sweep.error_code.is_none() => (Some(owner_decision_count), None),
            Some(sweep) => (
                None,
                Some(
                    sweep
                        .error_code
                        .as_deref()
                        .filter(|code| !code.trim().is_empty() && code.len() <= 64)
                        .unwrap_or("unavailable")
                        .to_owned(),
                ),
            ),
            None => (None, Some("busy".to_owned())),
        };
        DiskUsageView {
            artifacts,
            worktrees,
            plugins,
            snapshot_age_ms,
            refreshing,
            owner_decision_worktrees,
            owner_decision_error_code,
        }
    }
}

fn cache_entry(root: &Path) -> Arc<CacheEntry> {
    CACHE
        .get_or_init(Mutex::default)
        .lock_recover()
        .entry(root.to_path_buf())
        .or_insert_with(|| {
            Arc::new(CacheEntry {
                state: Mutex::default(),
                changed: watch::channel(0).0,
            })
        })
        .clone()
}

fn failed(code: &str) -> DiskUsageDirectory {
    DiskUsageDirectory {
        bytes: None,
        error_code: Some(code.to_owned()),
    }
}

async fn scan_bucket(path: PathBuf) -> (DiskUsageDirectory, Duration) {
    let mut walk = DirectoryWalk::new(path);
    let mut active_time = Duration::ZERO;
    loop {
        let step = tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let result = walk.advance(None, started + SLICE_TIME, SLICE_ENTRIES);
            (walk, result, started.elapsed())
        })
        .await;
        let Ok((next, result, elapsed)) = step else {
            return (failed("unavailable"), active_time + SLICE_TIME);
        };
        active_time += elapsed;
        match result {
            Ok(Some(bytes)) => {
                return (
                    DiskUsageDirectory {
                        bytes: Some(bytes),
                        error_code: None,
                    },
                    active_time,
                );
            }
            Err(code) => return (failed(code), active_time),
            Ok(None) => walk = next,
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "The daemon paces filesystem inventory between completed scan steps."
        )]
        tokio::time::sleep(SLICE_REST.max(elapsed.saturating_mul(SLICE_REST_MULTIPLIER))).await;
    }
}

pub(super) fn disk_directory_bytes(
    path: &Path,
    expected_identity: &str,
    deadline: Instant,
) -> Result<u64, &'static str> {
    DirectoryWalk::new(path.to_path_buf())
        .advance(Some(expected_identity), deadline, CLEANUP_PREVIEW_ENTRIES)?
        .ok_or("busy")
}

struct DirectoryWalk {
    path: PathBuf,
    cursors: Vec<ReadDir>,
    paused_identities: Vec<FileIdentity>,
    bytes: u64,
}

impl DirectoryWalk {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            cursors: Vec::new(),
            paused_identities: Vec::new(),
            bytes: 0,
        }
    }

    fn advance(
        &mut self,
        expected_identity: Option<&str>,
        deadline: Instant,
        mut remaining: usize,
    ) -> Result<Option<u64>, &'static str> {
        if self.cursors.is_empty() {
            match fs::symlink_metadata(&self.path) {
                Ok(meta) if meta.file_type().is_symlink() || is_reparse_point(&meta) => {
                    return Err("unsafe_path");
                }
                Ok(meta) if !meta.is_dir() => return Err("not_directory"),
                Ok(_) => {}
                Err(error)
                    if error.kind() == ErrorKind::NotFound && expected_identity.is_none() =>
                {
                    return Ok(Some(0));
                }
                Err(_) => return Err("unavailable"),
            }
        }
        let mut directory = Directory::open(&self.path).map_err(|_| "unavailable")?;
        if self.cursors.is_empty() {
            if let Some(expected) = expected_identity
                && directory.identity().map_err(|_| "unavailable")? != expected
            {
                return Err("identity_changed");
            }
            self.cursors
                .push(directory.entries().map_err(|_| "unavailable")?);
        } else if directory
            .directory_identities(self.cursors.len())
            .map_err(|_| "unavailable")?
            != self.paused_identities
        {
            return Err("unavailable");
        }

        while let Some(entries) = self.cursors.last_mut() {
            if remaining == 0 || Instant::now() >= deadline {
                if expected_identity.is_none() {
                    // Hold iterator cursors between slices and release the ancestor pins.
                    // Resume only if the opened directory chain still has the same identities.
                    self.path.clone_from(&directory.path);
                    self.paused_identities = directory
                        .directory_identities(self.cursors.len())
                        .map_err(|_| "unavailable")?;
                }
                return Ok(None);
            }
            let Some(entry) = entries.next().transpose().map_err(|_| "unavailable")? else {
                self.cursors.pop();
                if !self.cursors.is_empty() {
                    directory.pop_entry_directory();
                }
                continue;
            };
            remaining -= 1;
            let metadata = directory
                .entry_metadata(&entry)
                .map_err(|_| "unavailable")?;
            if metadata.is_dir()
                && !metadata.file_type().is_symlink()
                && !is_reparse_point(&metadata)
            {
                if expected_identity.is_none() && self.cursors.len() >= MAX_SCAN_DEPTH {
                    return Err("depth_limit");
                }
                directory
                    .push_entry_directory(&entry)
                    .map_err(|_| "unavailable")?;
                self.cursors
                    .push(directory.entries().map_err(|_| "unavailable")?);
            } else {
                self.bytes = self.bytes.checked_add(metadata.len()).ok_or("overflow")?;
            }
        }
        if expected_identity.is_some() && Instant::now() >= deadline {
            return Ok(None);
        }
        Ok(Some(self.bytes))
    }
}
