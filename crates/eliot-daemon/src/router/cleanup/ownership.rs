use super::*;
use crate::workspaces::require_managed_cleanup_path;

pub(super) fn require_native_cleanup_ownership(
    store: &eliot_store::Store,
    artifact: &ArtifactView,
) -> Result<()> {
    if store
        .generation_custody_namespace(&artifact.agent, artifact.generation)?
        .as_deref()
        != Some(store.custody_namespace()?.as_str())
    {
        return Err(Error::new(
            "native_session_foreign",
            format!(
                "{} generation {} has foreign native custody; retry from the daemon that owns this generation",
                artifact.agent, artifact.generation
            ),
        ));
    }
    Ok(())
}

impl Router {
    pub(super) async fn require_cleanup_ownership(
        &self,
        artifact: &ArtifactView,
    ) -> Result<Option<CleanupPath>> {
        let data_root = &self.inner.data_root;
        match artifact.kind {
            ArtifactKind::Worktree => {
                return require_managed_cleanup_path(data_root, Path::new(&artifact.path))
                    .map(|()| None);
            }
            ArtifactKind::Branch => {
                let git = artifact
                    .git
                    .as_ref()
                    .ok_or_else(|| Error::new("source_integrity", "branch Git binding absent"))?;
                return require_managed_cleanup_path(data_root, Path::new(&git.worktree))
                    .map(|()| None);
            }
            ArtifactKind::NativeSession => {
                let store = self.inner.store.lock().await;
                require_native_cleanup_ownership(&store, artifact)?;
                return Ok(None);
            }
            ArtifactKind::ProjectRegistration => return Ok(None),
            _ => {}
        }
        let path = Path::new(&artifact.path);
        validate_cleanup_path(path)?;
        let path = CleanupPath::open(path)?;
        for local in ["artifacts", "plugins"] {
            if path.in_root(&data_root.join(local))? {
                return Ok(Some(path));
            }
        }
        // Implicit chat roots are derived from the daemon's data directory.
        // A shared configured chat root is only usable through a bound row.
        if artifact.purpose == "Swarm chat directory"
            && let Ok(root) = crate::config::validate_chats_location(data_root, None)
            && path.in_root(&root)?
        {
            return Ok(Some(path));
        }
        let root = {
            let store = self.inner.store.lock().await;
            if store.artifact_registered_here(artifact)? {
                return Ok(Some(path));
            }
            let root = store.agent_artifact_root(&artifact.agent, artifact.generation)?;
            match root {
                Some(root) if store.artifact_registered_here(&root)? => Some(root),
                _ => None,
            }
        };
        if let Some(root) = root
            && path.in_root(Path::new(&root.path))?
        {
            return Ok(Some(path));
        }
        Err(Error::new(
            "artifact_root_foreign",
            "artifact is outside this daemon's roots and has no local custody binding",
        ))
    }
}
