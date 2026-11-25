//! Discover regenerable and private-stand children of durable artifact roots.
use super::*;
use std::time::UNIX_EPOCH;

const MAX_ROOT_ENTRIES: usize = 256;

impl Router {
    pub(super) async fn backfill_owned_cleanup_children(
        &self,
        mut after: i64,
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
                if let Err(error) = self.discover_owned_cleanup_children(&root).await {
                    if matches!(error.code.as_str(), "internal" | "unavailable") {
                        crate::logging::record(
                            &self.secret_policy_snapshot(),
                            "daemon.cleanup_discovery_failed",
                            &[("agent", &root.agent)],
                            Some(&error),
                        );
                    }
                }
            }
        }
        Ok(Some(after))
    }

    async fn discover_owned_cleanup_children(&self, root: &ArtifactView) -> Result<()> {
        let path = self.verify_discovery_root(root)?;
        let daemon_root = self.inner.data_root.clone();
        let root = root.clone();
        let candidates = tokio::task::spawn_blocking(move || -> Result<Vec<ArtifactView>> {
            let directory = Directory::open(&path)?;
            let entries = directory.entries().map_err(discovery_io)?;
            let mut candidates = Vec::new();
            for entry in entries.take(MAX_ROOT_ENTRIES) {
                let entry = entry.map_err(discovery_io)?;
                let path = path.join(entry.file_name());
                if validate_cleanup_path(&path).is_err() {
                    continue;
                }
                let Ok(metadata) = std::fs::symlink_metadata(&path) else {
                    continue;
                };
                if !metadata.is_dir()
                    || crate::platform::is_reparse_point(&metadata)
                    || metadata.file_type().is_symlink()
                {
                    continue;
                }
                let Ok(identity) = directory_identity(&path) else {
                    continue;
                };
                let stand = crate::stand_cleanup::owned_stand_artifact_times(
                    &daemon_root,
                    &path,
                    &identity,
                )
                .ok()
                .flatten();
                let (kind, purpose, created_at, expires_at) = match stand {
                    Some((created, expires)) => (
                        ArtifactKind::DataDir,
                        SWARM_STAND_ARTIFACT_PURPOSE,
                        created,
                        Some(expires),
                    ),
                    None if cargo_target(&path) => {
                        let Some(modified) = metadata
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                            .and_then(|d| i64::try_from(d.as_millis()).ok())
                        else {
                            continue;
                        };
                        (
                            ArtifactKind::Build,
                            SWARM_CARGO_TARGET_ARTIFACT_PURPOSE,
                            modified.max(root.created_at),
                            None,
                        )
                    }
                    None => continue,
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
            Ok(candidates)
        })
        .await
        .map_err(|_| Error::new("internal", "artifact discovery worker did not settle"))??;
        let mut store = self.inner.store.lock().await;
        for artifact in candidates {
            if store.known_artifact_path(&artifact.path)?.is_none()
                && store.cleanup_agent_quiescent(&artifact)?
            {
                store.record_known_artifact(artifact)?;
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
        if artifact.purpose == SWARM_CARGO_TARGET_ARTIFACT_PURPOSE && !cargo_target(&canonical) {
            return Err(Error::new("not_allowed", "Cargo target evidence changed"));
        }
        Ok(canonical)
    }
}

fn discovery_io(error: std::io::Error) -> Error {
    Error::new(
        "platform_io",
        format!("inspect owned artifact directory: {error}"),
    )
}

fn cargo_target(path: &Path) -> bool {
    let Ok(directory) = Directory::open(path) else {
        return false;
    };
    let Ok(mut tag) = directory.file("CACHEDIR.TAG") else {
        return false;
    };
    let Ok(bytes) = crate::platform::script_file::read(&mut tag) else {
        return false;
    };
    bytes.starts_with(b"Signature: 8a477f597d28d172789f06886806bc55")
        && directory.file(".rustc_info.json").is_ok()
        && ["debug", "release"]
            .into_iter()
            .any(|profile| Directory::open(&path.join(profile)).is_ok())
}
