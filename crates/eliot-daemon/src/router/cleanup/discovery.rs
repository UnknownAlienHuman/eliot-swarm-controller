//! Discover regenerable targets and private stands under durable artifact roots.
use super::*;
use eliot_plugin::FileIdentity;
use std::fs::ReadDir;
use std::time::UNIX_EPOCH;
use tokio::sync::oneshot;

const MAX_DISCOVERY_ENTRIES: usize = 1024;
const MAX_CARGO_DEPTH: usize = 3;

const MAX_OWNED_SCANS: usize = 32;
const OWNED_SCAN_TTL: Duration = Duration::from_secs(5 * 60);

struct OwnedRootWalk {
    path: PathBuf,
    cursors: Vec<ReadDir>,
    paused_identities: Vec<FileIdentity>,
}

pub(super) struct OwnedRootResume {
    after: i64,
    seq: i64,
    binding: String,
    walk: OwnedRootWalk,
    expires: Instant,
    _expiry: oneshot::Sender<()>,
}

impl Router {
    pub(super) async fn backfill_owned_cleanup_children(
        &self,
        page: &mut PlanContinuation,
        binding: &str,
        principal: &str,
        project: Option<&str>,
        issues: &mut Vec<CleanupInventoryIssue>,
    ) -> Result<bool> {
        let mut after = page.owned_backfill_after.unwrap_or(0);
        let token = page.owned_root_scan.take();
        let mut resume = token.as_ref().and_then(|id| {
            let mut scans = self.inner.automatic_cleanup.lock_recover();
            if scans
                .owned_scans
                .get(id)
                .is_some_and(|scan| scan.after == after && scan.binding == binding)
            {
                scans
                    .owned_scans
                    .remove(id)
                    .filter(|scan| scan.expires > Instant::now())
            } else {
                None
            }
        });
        let mut restarted = token.is_some() && resume.is_none();
        let deadline = Instant::now() + CLEANUP_BACKFILL_BUDGET;
        for _ in 0..4 {
            let roots = self
                .inner
                .store
                .lock()
                .await
                .cleanup_owned_roots_page(after)?;
            if roots.is_empty() {
                if std::mem::take(&mut restarted) {
                    let policy = self.secret_policy_snapshot();
                    issues.push(CleanupInventoryIssue {
                        agent: principal.to_owned(),
                        generation: 0,
                        path: None,
                        reason: "owned_root_scan_restarted".to_owned(),
                        detail: policy.redact_bounded(
                            "The saved scan was unavailable or stale; no owned roots remained to rescan",
                            384,
                        ),
                    });
                }
                page.owned_backfill_after = Some(0);
                return Ok(false);
            }
            for (seq, root) in roots {
                if resume.is_none() && !restarted && Instant::now() >= deadline {
                    page.owned_backfill_after = Some(after);
                    return Ok(true);
                }
                let authorized = project.is_none_or(|project| root.project == project)
                    && (principal == "owner"
                        || self
                            .inner
                            .store
                            .lock()
                            .await
                            .cleanup_authorized(principal, &root)?);
                if resume
                    .as_ref()
                    .is_some_and(|scan| scan.seq != seq || !authorized)
                {
                    resume = None;
                    restarted = true;
                }
                if authorized {
                    let walk = resume.take().map(|scan| scan.walk);
                    let mut result = self
                        .discover_owned_cleanup_children(&root, false, walk, deadline)
                        .await;
                    if result
                        .as_ref()
                        .is_err_and(|error| error.code == "owned_root_scan_stale")
                    {
                        restarted = true;
                        result = self
                            .discover_owned_cleanup_children(&root, false, None, deadline)
                            .await;
                    }
                    if std::mem::take(&mut restarted) {
                        issues.push(self.owned_scan_issue(
                            &root,
                            "owned_root_scan_restarted",
                            "The saved scan was unavailable or stale; discovery restarted in this root",
                        ));
                    }
                    match result {
                        Ok(Some(walk)) => {
                            page.owned_root_scan =
                                Some(self.park_owned_scan(after, seq, binding, walk));
                            page.owned_backfill_after = Some(after);
                            issues.push(self.owned_scan_issue(
                                &root,
                                "owned_root_scan_incomplete",
                                "Owned artifact discovery will resume within this root",
                            ));
                            return Ok(true);
                        }
                        Ok(None) => {}
                        Err(error) => issues.push(self.owned_scan_issue(
                            &root,
                            "owned_root_scan_failed",
                            &error.message,
                        )),
                    }
                }
                after = seq;
            }
        }
        if std::mem::take(&mut restarted) {
            let policy = self.secret_policy_snapshot();
            issues.push(CleanupInventoryIssue {
                agent: principal.to_owned(),
                generation: 0,
                path: None,
                reason: "owned_root_scan_restarted".to_owned(),
                detail: policy.redact_bounded(
                    "The saved scan was unavailable or stale; no authorized owned roots were rescanned",
                    384,
                ),
            });
        }
        page.owned_backfill_after = Some(after);
        Ok(true)
    }

    fn owned_scan_issue(
        &self,
        root: &ArtifactView,
        reason: &str,
        detail: &str,
    ) -> CleanupInventoryIssue {
        let policy = self.secret_policy_snapshot();
        CleanupInventoryIssue {
            agent: root.agent.clone(),
            generation: root.generation,
            path: Some(policy.redact_bounded(&root.path, 384)),
            reason: reason.to_owned(),
            detail: policy.redact_bounded(detail, 384),
        }
    }

    fn park_owned_scan(&self, after: i64, seq: i64, binding: &str, walk: OwnedRootWalk) -> String {
        let id = ulid::Ulid::new().to_string();
        let (expiry, cancel) = oneshot::channel();
        let expires = Instant::now() + OWNED_SCAN_TTL;
        {
            let mut state = self.inner.automatic_cleanup.lock_recover();
            if state.owned_scans.len() >= MAX_OWNED_SCANS
                && let Some(oldest) = state
                    .owned_scans
                    .iter()
                    .min_by_key(|(_, scan)| scan.expires)
                    .map(|(id, _)| id.clone())
            {
                state.owned_scans.remove(&oldest);
            }
            state.owned_scans.insert(
                id.clone(),
                OwnedRootResume {
                    after,
                    seq,
                    binding: binding.to_owned(),
                    walk,
                    expires,
                    _expiry: expiry,
                },
            );
        }
        let inner = Arc::downgrade(&self.inner);
        let parked = id.clone();
        drop(tokio::spawn(async move {
            if timeout_at(expires, cancel).await.is_err()
                && let Some(inner) = inner.upgrade()
            {
                inner
                    .automatic_cleanup
                    .lock_recover()
                    .owned_scans
                    .remove(&parked);
            }
        }));
        id
    }

    pub(super) async fn discover_terminal_cargo_children(
        &self,
        root: &ArtifactView,
    ) -> Option<CleanupInventoryIssue> {
        // The terminal hook is opportunistic. Periodic inventory starts its
        // own scan, with continuation, to cover anything beyond this slice.
        match self
            .discover_owned_cleanup_children(
                root,
                true,
                None,
                Instant::now() + CLEANUP_BACKFILL_BUDGET,
            )
            .await
        {
            Ok(Some(_)) => {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "daemon.cleanup_discovery_incomplete",
                    &[
                        ("agent", &root.agent),
                        ("generation", &root.generation.to_string()),
                        ("operation", "owned_root_scan_incomplete"),
                    ],
                    None,
                );
                Some(self.owned_scan_issue(
                root,
                "owned_root_scan_incomplete",
                "Terminal cache discovery ended after one slice; periodic inventory will scan this root",
            ))
            }
            Ok(None) => None,
            Err(error) => {
                Some(self.owned_scan_issue(root, "owned_root_scan_failed", &error.message))
            }
        }
    }

    async fn discover_owned_cleanup_children(
        &self,
        root: &ArtifactView,
        cargo_only: bool,
        walk: Option<OwnedRootWalk>,
        deadline: Instant,
    ) -> Result<Option<OwnedRootWalk>> {
        let path = self
            .verify_discovery_root(root)
            .map_err(|error| if walk.is_some() { stale_scan() } else { error })?;
        let daemon_root = self.inner.data_root.clone();
        let root = root.clone();
        let (candidates, remaining) = tokio::task::spawn_blocking(move || {
            let mut walk = walk.unwrap_or_else(|| OwnedRootWalk {
                path: path.clone(),
                cursors: Vec::new(),
                paused_identities: Vec::new(),
            });
            let mut candidates = Vec::new();
            let remaining = walk
                .advance(
                    &path,
                    &root,
                    &daemon_root,
                    cargo_only,
                    deadline,
                    &mut candidates,
                )
                .map(|finished| (!finished).then_some(walk));
            (candidates, remaining)
        })
        .await
        .map_err(|_| Error::new("internal", "artifact discovery worker did not settle"))?;
        let mut store = self.inner.store.lock().await;
        for artifact in candidates {
            if !store.cleanup_agent_quiescent(&artifact)? {
                continue;
            }
            match store.known_artifact_path(&artifact.path)? {
                None => store.record_known_artifact(artifact)?,
                Some(existing) if artifact.kind == ArtifactKind::Build => {
                    store.recognize_registered_cargo_target(&existing, &artifact)?;
                }
                Some(_) => {}
            }
        }
        remaining
    }

    fn verify_discovery_root(&self, root: &ArtifactView) -> Result<PathBuf> {
        let path = Path::new(&root.path);
        validate_cleanup_path(path)?;
        let directory = Directory::open(path)?;
        let identity = directory.identity()?;
        let canonical = path.canonicalize().map_err(discovery_io)?;
        let config = crate::config::load_daemon_config(&self.inner.data_root)?;
        let parent = config
            .artifact_roots
            .first()
            .map(PathBuf::from)
            .unwrap_or_else(|| self.inner.data_root.join("artifacts"));
        let parent = parent.canonicalize().map_err(discovery_io)?;
        if root.state != "ownership_root"
            || root.purpose != SWARM_ARTIFACT_ROOT_PURPOSE
            || canonical != parent.join(format!("{}-{}", root.agent, root.generation))
            || identity != root.identity
        {
            return Err(Error::new(
                "identity_changed",
                "artifact root ownership changed",
            ));
        }
        require_cleanup_outside_native_home(&canonical)?;
        Ok(canonical)
    }

    pub(super) async fn checked_owned_cleanup_child(
        &self,
        artifact: &ArtifactView,
    ) -> Result<PathBuf> {
        let root = self
            .inner
            .store
            .lock()
            .await
            .agent_artifact_root(&artifact.agent, artifact.generation)?
            .ok_or_else(|| Error::new("not_allowed", "cleanup child has no exact owned root"))?;
        let root = self.verify_discovery_root(&root)?;
        let source = Path::new(&artifact.path);
        validate_cleanup_path(source)?;
        // Hold the original path against renames while normalizing it. The
        // cleanup tree opens the normalized path again without following links
        // and checks its pinned identity before the first disposition.
        let directory = Directory::open(source)?;
        let identity = directory.identity()?;
        let canonical = source.canonicalize().map_err(discovery_io)?;
        if canonical == root || !canonical.starts_with(&root) || identity != artifact.identity {
            return Err(Error::new(
                "identity_changed",
                "owned cleanup child binding changed",
            ));
        }
        if artifact.purpose == SWARM_CARGO_TARGET_ARTIFACT_PURPOSE {
            check_cargo_ancestors(&canonical, &root)?;
            if !cargo_target(&directory) {
                return Err(Error::new("not_allowed", "Cargo target evidence changed"));
            }
        }
        Ok(canonical)
    }
}

fn stale_scan() -> Error {
    Error::new(
        "owned_root_scan_stale",
        "saved owned artifact scan is stale",
    )
}

impl OwnedRootWalk {
    fn advance(
        &mut self,
        path: &Path,
        root: &ArtifactView,
        daemon_root: &Path,
        cargo_only: bool,
        deadline: Instant,
        candidates: &mut Vec<ArtifactView>,
    ) -> Result<bool> {
        if !self.path.starts_with(path) {
            return Err(stale_scan());
        }
        let resumed = !self.cursors.is_empty();
        let mut directory = Directory::open(&self.path)
            .map_err(|error| if resumed { stale_scan() } else { error })?;
        if resumed {
            if directory
                .directory_identities(self.cursors.len())
                .map_err(discovery_io)?
                != self.paused_identities
            {
                return Err(stale_scan());
            }
        } else {
            if directory.identity()? != root.identity {
                return Err(Error::new(
                    "identity_changed",
                    "artifact root ownership changed",
                ));
            }
            self.cursors
                .push(directory.entries().map_err(discovery_io)?);
        }
        let mut remaining = MAX_DISCOVERY_ENTRIES;
        while !self.cursors.is_empty() {
            if remaining == 0 || Instant::now() >= deadline {
                // Save the native iterator position, release the path pins and
                // re-open the same directory identities before the next slice.
                self.path.clone_from(&directory.path);
                self.paused_identities = directory
                    .directory_identities(self.cursors.len())
                    .map_err(discovery_io)?;
                return Ok(false);
            }
            let depth = self.cursors.len();
            let Some(entry) = self
                .cursors
                .last_mut()
                .unwrap()
                .next()
                .transpose()
                .map_err(discovery_io)?
            else {
                self.cursors.pop();
                if depth > 1 {
                    directory.pop_entry_directory();
                }
                continue;
            };
            remaining -= 1;
            let path = directory.path.join(entry.file_name());
            if !entry.file_type().is_ok_and(|kind| kind.is_dir())
                || validate_cleanup_path(&path).is_err()
            {
                continue;
            }
            let modified = directory
                .entry_metadata(&entry)
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .and_then(|time| i64::try_from(time.as_millis()).ok())
                .unwrap_or(root.created_at);
            if directory.push_entry_directory(&entry).is_err() {
                continue;
            }
            let identity = directory.identity()?;
            let stand = if depth == 1 && !cargo_only {
                crate::stand_cleanup::owned_stand_artifact_times(daemon_root, &path, &identity)
                    .ok()
                    .flatten()
            } else {
                None
            };
            let class = if let Some((created, expires)) = stand {
                Some((
                    ArtifactKind::DataDir,
                    SWARM_STAND_ARTIFACT_PURPOSE,
                    created,
                    Some(expires),
                ))
            } else if protected_cargo_parent(&path) {
                None
            } else if cargo_target(&directory) {
                Some((
                    ArtifactKind::Build,
                    SWARM_CARGO_TARGET_ARTIFACT_PURPOSE,
                    modified.max(root.created_at),
                    None,
                ))
            } else if depth < MAX_CARGO_DEPTH {
                self.cursors
                    .push(directory.entries().map_err(discovery_io)?);
                continue;
            } else {
                None
            };
            directory.pop_entry_directory();
            if let Some((kind, purpose, created_at, expires_at)) = class {
                candidates.push(ArtifactView {
                    id: ulid::Ulid::new().to_string(),
                    agent: root.agent.clone(),
                    generation: root.generation,
                    project: root.project.clone(),
                    tree: Vec::new(),
                    path: path.to_string_lossy().into_owned(),
                    kind,
                    purpose: purpose.to_owned(),
                    identity,
                    created_at,
                    expires_at,
                    state: "registered".to_owned(),
                    git: None,
                });
            }
        }
        Ok(true)
    }
}

fn protected_cargo_parent(path: &Path) -> bool {
    [".git", "daemon.lock", crate::stand_cleanup::STAND_MARKER]
        .iter()
        .any(|name| {
            !matches!(
                std::fs::symlink_metadata(path.join(name)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound
            )
        })
}

fn check_cargo_ancestors(path: &Path, root: &Path) -> Result<()> {
    for parent in path.ancestors().take_while(|parent| *parent != root) {
        let _directory = Directory::open(parent)?;
        if protected_cargo_parent(parent) {
            return Err(Error::new(
                "not_allowed",
                "Cargo target crosses a repository or stand boundary",
            ));
        }
    }
    Ok(())
}

fn discovery_io(error: std::io::Error) -> Error {
    Error::new(
        "platform_io",
        format!("inspect owned artifact directory: {error}"),
    )
}

fn cargo_target(directory: &Directory) -> bool {
    let cache_tag = directory
        .file("CACHEDIR.TAG")
        .and_then(|mut file| crate::platform::script_file::read(&mut file))
        .is_ok_and(|bytes| bytes.starts_with(b"Signature: 8a477f597d28d172789f06886806bc55"));
    let rustc_metadata = directory
        .file(".rustc_info.json")
        .and_then(|mut file| crate::platform::script_file::read(&mut file))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .is_some_and(|value| {
            value["rustc_fingerprint"].is_u64()
                && value["outputs"].is_object()
                && value["successes"].is_object()
        });
    (cache_tag || rustc_metadata)
        && ["debug", "release"].into_iter().any(|profile| {
            let profile = directory.path.join(profile);
            Directory::open(&profile).is_ok_and(|dir| dir.file(".cargo-lock").is_ok())
                && ["deps", "build"]
                    .into_iter()
                    .any(|child| Directory::open(&profile.join(child)).is_ok())
        })
}
