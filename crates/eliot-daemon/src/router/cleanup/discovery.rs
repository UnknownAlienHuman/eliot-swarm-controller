//! Discover regenerable targets and private stands under durable artifact roots.
use super::*;
use std::collections::VecDeque;
use std::time::UNIX_EPOCH;

const MAX_DISCOVERY_ENTRIES: usize = 1024;
const MAX_CARGO_DEPTH: usize = 3;

impl Router {
    pub(super) async fn backfill_owned_cleanup_children(
        &self,
        mut after: i64,
        principal: &str,
        project: Option<&str>,
    ) -> Result<Option<i64>> {
        let deadline = Instant::now() + CLEANUP_BACKFILL_BUDGET;
        for _ in 0..4 {
            let roots = self
                .inner
                .store
                .lock()
                .await
                .cleanup_owned_roots_page(after)?;
            if roots.is_empty() {
                return Ok(None);
            }
            for (seq, root) in roots {
                if Instant::now() >= deadline {
                    return Ok(Some(after));
                }
                after = seq;
                if project.is_some_and(|project| root.project != project)
                    || (principal != "owner"
                        && !self
                            .inner
                            .store
                            .lock()
                            .await
                            .cleanup_authorized(principal, &root)?)
                {
                    continue;
                }
                if let Err(error) = self
                    .discover_owned_cleanup_children(&root, false, deadline)
                    .await
                    && matches!(error.code.as_str(), "internal" | "unavailable")
                {
                    crate::logging::record(
                        &self.secret_policy_snapshot(),
                        "daemon.cleanup_discovery_failed",
                        &[("agent", &root.agent)],
                        Some(&error),
                    );
                }
            }
        }
        Ok(Some(after))
    }

    pub(super) async fn discover_owned_cleanup_children(
        &self,
        root: &ArtifactView,
        cargo_only: bool,
        deadline: Instant,
    ) -> Result<()> {
        let path = self.verify_discovery_root(root)?;
        let daemon_root = self.inner.data_root.clone();
        let root = root.clone();
        let candidates = tokio::task::spawn_blocking(move || {
            discover_children(path, &root, &daemon_root, cargo_only, deadline)
        })
        .await
        .map_err(|_| Error::new("internal", "artifact discovery worker did not settle"))??;
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
        Ok(())
    }

    fn verify_discovery_root(&self, root: &ArtifactView) -> Result<PathBuf> {
        let path = Path::new(&root.path);
        validate_cleanup_path(path)?;
        let identity = directory_identity(path)?;
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
        let identity = directory_identity(source)?;
        let canonical = source.canonicalize().map_err(discovery_io)?;
        if canonical == root || !canonical.starts_with(&root) || identity != artifact.identity {
            return Err(Error::new(
                "identity_changed",
                "owned cleanup child binding changed",
            ));
        }
        if artifact.purpose == SWARM_CARGO_TARGET_ARTIFACT_PURPOSE {
            check_cargo_ancestors(&canonical, &root)?;
            if !cargo_target(&Directory::open(&canonical)?) {
                return Err(Error::new("not_allowed", "Cargo target evidence changed"));
            }
        }
        Ok(canonical)
    }
}

fn discover_children(
    path: PathBuf,
    root: &ArtifactView,
    daemon_root: &Path,
    cargo_only: bool,
    deadline: Instant,
) -> Result<Vec<ArtifactView>> {
    let mut directories = VecDeque::from([(path, 1)]);
    let mut candidates = Vec::new();
    let mut remaining = MAX_DISCOVERY_ENTRIES;
    'directories: while let Some((parent, depth)) = directories.pop_front() {
        let Ok(parent) = Directory::open(&parent) else {
            continue;
        };
        for entry in parent.entries().map_err(discovery_io)? {
            if remaining == 0 || Instant::now() >= deadline {
                break 'directories;
            }
            remaining -= 1;
            let entry = entry.map_err(discovery_io)?;
            let path = parent.path.join(entry.file_name());
            if !entry.file_type().is_ok_and(|kind| kind.is_dir())
                || validate_cleanup_path(&path).is_err()
            {
                continue;
            }
            let Ok(directory) = Directory::open(&path) else {
                continue;
            };
            let identity = directory.identity()?;
            let stand = if depth == 1 && !cargo_only {
                crate::stand_cleanup::owned_stand_artifact_times(daemon_root, &path, &identity)
                    .ok()
                    .flatten()
            } else {
                None
            };
            let (kind, purpose, created_at, expires_at) = if let Some((created, expires)) = stand {
                (
                    ArtifactKind::DataDir,
                    SWARM_STAND_ARTIFACT_PURPOSE,
                    created,
                    Some(expires),
                )
            } else if protected_cargo_parent(&path) {
                continue;
            } else if cargo_target(&directory) {
                let modified = entry
                    .metadata()
                    .ok()
                    .and_then(|metadata| metadata.modified().ok())
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                    .and_then(|time| i64::try_from(time.as_millis()).ok())
                    .unwrap_or(root.created_at);
                (
                    ArtifactKind::Build,
                    SWARM_CARGO_TARGET_ARTIFACT_PURPOSE,
                    modified.max(root.created_at),
                    None,
                )
            } else {
                if depth < MAX_CARGO_DEPTH {
                    directories.push_back((path, depth + 1));
                }
                continue;
            };
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
    Ok(candidates)
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
