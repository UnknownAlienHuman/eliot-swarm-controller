use super::*;
use eliot_core::api::{CheckpointRestoreRequest, CheckpointRestoreResponse};

impl Router {
    pub(super) async fn checkpoint_restore(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: CheckpointRestoreRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let workspace = crate::git_monitor::canonical_root(&request.workspace)?;
        let workspace = workspace.to_string_lossy().into_owned();

        {
            let store = self.inner.store.lock().await;
            let truth = store
                .agent_work_truth(&request.agent, request.generation)?
                .ok_or_else(|| Error::new("not_found", "checkpoint generation was not retained"))?;
            let checkpoint_matches = truth.checkpoint.is_some_and(|checkpoint| {
                checkpoint.state == eliot_core::api::GitCheckpointState::Snapshot
                    && checkpoint.checkpoint_ref.as_deref() == Some(request.checkpoint_ref.as_str())
            });
            if !checkpoint_matches {
                return Err(Error::new(
                    "not_found",
                    "checkpoint reference is not current",
                ));
            }
            if store.workspace_has_active_agents(&workspace)? {
                return Err(Error::new("busy", "restore workspace has an active Agent"));
            }
        }
        let limits = crate::config::load_daemon_config(&self.inner.data_root)
            .unwrap_or_default()
            .checkpoint_limits();
        let restored = crate::work_truth::restore_checkpoint(
            &self.inner.data_root,
            &workspace,
            &request.agent,
            request.generation,
            &request.checkpoint_ref,
            &self.secret_policy_snapshot(),
            limits,
        )
        .await?;
        serde_json::to_value(CheckpointRestoreResponse {
            head: restored.head,
            paths: restored.paths,
        })
        .map_err(|_| Error::new("internal", "checkpoint restore result cannot be encoded"))
    }
}
