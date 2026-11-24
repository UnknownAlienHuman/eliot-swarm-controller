use super::*;
use eliot_core::api::{CheckpointRestoreRequest, CheckpointRestoreResponse};
use eliot_store::CheckpointRestoreAdmission;

impl Router {
    pub(super) async fn checkpoint_restore(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let mut request: CheckpointRestoreRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let (workspace, _) = crate::workspace_git::WorkspaceGit::new()?
            .repository_identity(&request.workspace)
            .await?;
        let _guard = self.workspace_check_lock(&workspace).lock_owned().await;
        let admission = self.inner.store.lock().await.reserve_checkpoint_restore(
            &request,
            &workspace,
            now_ms(),
        )?;
        let oid = match admission {
            CheckpointRestoreAdmission::Replay(outcome) => return encode_outcome(outcome?),
            CheckpointRestoreAdmission::Start { oid } => oid,
        };
        request.workspace = workspace;
        let limits = crate::config::load_daemon_config(&self.inner.data_root)
            .unwrap_or_default()
            .checkpoint_limits();
        let policy = self.secret_policy_snapshot();
        let outcome = crate::work_truth::restore_checkpoint(
            &self.inner.data_root,
            &request,
            &oid,
            &policy,
            limits,
        )
        .await
        .map(|restored| CheckpointRestoreResponse {
            head: restored.head,
            paths: restored.paths,
        })
        .map_err(|mut error| {
            error.message = Box::new(policy.redact_text(&error.message));
            error
        });
        self.inner
            .store
            .lock()
            .await
            .settle_checkpoint_restore(&request.id, &outcome)?;
        encode_outcome(outcome?)
    }
}

fn encode_outcome(outcome: CheckpointRestoreResponse) -> Result<Value> {
    serde_json::to_value(outcome)
        .map_err(|_| Error::new("internal", "checkpoint restore result cannot be encoded"))
}
