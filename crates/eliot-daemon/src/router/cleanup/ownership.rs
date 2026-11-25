use super::*;
use crate::platform::script_file::{canonical_cleanup_path, cleanup_canonical_path_in_root};
use crate::workspaces::require_managed_cleanup_path;

impl Router {
    pub(super) async fn require_cleanup_ownership(
        &self,
        artifact: &ArtifactView,
    ) -> Result<Option<PathBuf>> {
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
                if store
                    .generation_custody_namespace(&artifact.agent, artifact.generation)?
                    .as_deref()
                    != Some(store.custody_namespace()?.as_str())
                {
                    return Err(Error::new(
                        "native_session_foreign",
                        "native session belongs to another daemon binding",
                    ));
                }
                return Ok(None);
            }
            ArtifactKind::ProjectRegistration => return Ok(None),
            _ => {}
        }
        let path = Path::new(&artifact.path);
        validate_cleanup_path(path)?;
        let path = canonical_cleanup_path(path)?;
        for local in ["artifacts", "plugins"] {
            if cleanup_canonical_path_in_root(&path, &data_root.join(local))? {
                return Ok(Some(path));
            }
        }
        // Implicit chat roots are derived from the daemon's data directory.
        // A shared configured chat root is only usable through a bound row.
        if artifact.purpose == "Swarm chat directory"
            && let Ok(root) = crate::config::validate_chats_location(data_root, None)
            && cleanup_canonical_path_in_root(&path, &root)?
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
            && cleanup_canonical_path_in_root(&path, Path::new(&root.path))?
        {
            return Ok(Some(path));
        }
        Err(Error::new(
            "artifact_root_foreign",
            "artifact is outside this daemon's roots and has no local custody binding",
        ))
    }
}
