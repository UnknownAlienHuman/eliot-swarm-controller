use super::*;
use eliot_core::api::{TaskGetRequest, TaskGetResponse, TaskListRequest, TaskListResponse};

impl Router {
    pub(super) async fn task_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: TaskListRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let response = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(
                &store,
                principal,
                &request.agent,
                Some(request.generation),
            )?;
            store
                .get_agent(&request.agent)?
                .ok_or_else(|| Error::new("not_found", "Agent not found"))?;
            let page = store.task_record_list(&request)?;
            TaskListResponse {
                agent: request.agent.clone(),
                generation: request.generation,
                revision: page.revision,
                tasks: page.tasks,
                next_cursor: page.next_cursor,
            }
        };
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "task list response encoding failed"))
    }

    pub(super) async fn task_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: TaskGetRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let offset = request.offset.unwrap_or(0);
        let (task_ref, store_revision) = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(
                &store,
                principal,
                &request.agent,
                Some(request.generation),
            )?;
            store
                .get_agent(&request.agent)?
                .ok_or_else(|| Error::new("not_found", "Agent not found"))?;
            let (task_ref, store_revision) = store
                .task_record_binding(&request.agent, request.generation, &request.task_command)?
                .ok_or_else(|| Error::new("not_found", "task record not found"))?;
            (task_ref, store_revision)
        };
        let policy = SecretPolicy::new(self.daemon_secrets());
        let (text, next_offset, revision, content_digest) =
            crate::work_truth::task_record_reads::read_task_record_section(
                &self.inner.data_root,
                &task_ref,
                &request,
                &policy,
            )
            .await?;
        {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(
                &store,
                principal,
                &request.agent,
                Some(request.generation),
            )?;
            let _agent = store
                .get_agent(&request.agent)?
                .ok_or_else(|| Error::new("not_found", "Agent not found"))?;
            let current = store.task_record_binding(
                &request.agent,
                request.generation,
                &request.task_command,
            )?;
            if current
                .as_ref()
                .is_none_or(|(r, version)| r != &task_ref || *version != store_revision)
            {
                return Err(Error::new(
                    "stale_revision",
                    "task record binding changed during read",
                ));
            }
        }
        let response = TaskGetResponse {
            agent: request.agent,
            generation: request.generation,
            task_command: request.task_command,
            section: request.section,
            revision,
            content_digest,
            offset,
            text,
            next_offset,
        };
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "task record response encoding failed"))
    }
}
