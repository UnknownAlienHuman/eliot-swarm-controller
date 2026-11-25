use super::*;
use eliot_core::api::{CheckpointGetRequest, CheckpointListRequest};

impl Router {
    pub(super) async fn checkpoint_list(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: CheckpointListRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let pending = self
            .checkpoint_read_binding(principal, &request.agent, request.generation)
            .await?;
        let root = crate::work_truth_repo::find(&self.inner.data_root).await?;
        let response = crate::work_truth::checkpoint_reads::list(
            root.as_deref(),
            &request,
            pending.as_deref(),
        )
        .await?;
        if pending
            != self
                .checkpoint_read_binding(principal, &request.agent, request.generation)
                .await?
        {
            return Err(Error::new(
                "stale_cursor",
                "checkpoint inventory changed during the read",
            ));
        }
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "checkpoint list response cannot be encoded"))
    }

    pub(super) async fn checkpoint_get(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: CheckpointGetRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let pending = self
            .checkpoint_read_binding(principal, &request.agent, request.generation)
            .await?;
        if pending.as_deref() == Some(request.checkpoint_ref.as_str()) {
            return Err(Error::new(
                "repository_busy",
                "checkpoint publication is still settling",
            ));
        }
        let root = crate::work_truth_repo::find(&self.inner.data_root)
            .await?
            .ok_or_else(|| Error::new("not_found", "checkpoint ref is no longer retained"))?;
        let response = crate::work_truth::checkpoint_reads::get(&root, &request).await?;
        let pending = self
            .checkpoint_read_binding(principal, &request.agent, request.generation)
            .await?;
        if pending.as_deref() == Some(request.checkpoint_ref.as_str()) {
            return Err(Error::new(
                "repository_busy",
                "checkpoint publication is still settling",
            ));
        }
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "checkpoint response cannot be encoded"))
    }

    async fn checkpoint_read_binding(
        &self,
        principal: &Principal,
        agent: &str,
        generation: i64,
    ) -> Result<Option<String>> {
        let store = self.inner.store.lock().await;
        hierarchy::authorize_retained_work_read(&store, principal, agent, generation)?;
        store.uncommitted_checkpoint_ref(agent, generation)
    }
}
