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
        {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(
                &store,
                principal,
                &request.agent,
                Some(request.generation),
            )?;
            if store.get_agent(&request.agent)?.is_none() {
                return Err(Error::new("not_found", "checkpoint Agent is missing"));
            }
        }
        let root = crate::work_truth_repo::existing(&self.inner.data_root).await?;
        let response = crate::work_truth::checkpoint_reads::list(&root, &request).await?;
        {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(
                &store,
                principal,
                &request.agent,
                Some(request.generation),
            )?;
            if store.get_agent(&request.agent)?.is_none() {
                return Err(Error::new("not_found", "checkpoint Agent is missing"));
            }
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
        {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(
                &store,
                principal,
                &request.agent,
                Some(request.generation),
            )?;
            if store.get_agent(&request.agent)?.is_none() {
                return Err(Error::new("not_found", "checkpoint Agent is missing"));
            }
        }
        let root = crate::work_truth_repo::existing(&self.inner.data_root).await?;
        let response = crate::work_truth::checkpoint_reads::get(&root, &request).await?;
        {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(
                &store,
                principal,
                &request.agent,
                Some(request.generation),
            )?;
            if store.get_agent(&request.agent)?.is_none() {
                return Err(Error::new("not_found", "checkpoint Agent is missing"));
            }
        }
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "checkpoint response cannot be encoded"))
    }
}
