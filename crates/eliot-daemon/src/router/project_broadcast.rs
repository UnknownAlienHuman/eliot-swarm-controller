use super::*;
use eliot_core::api::{ProjectBroadcastRequest, discovery::decode_params};

impl Router {
    pub(super) async fn project_broadcast(
        &self,
        params: &Value,
        principal: &Principal,
        owner_process: Option<&OwnerProcess>,
    ) -> Result<Value> {
        let request: ProjectBroadcastRequest = decode_params(params.clone())?;
        request.validate()?;
        if self.secret_policy_snapshot().contains_value(params) {
            return Err(Error::new(
                "secret_in_payload",
                "broadcast contains a known credential",
            ));
        }
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.project_broadcast(
                &principal_name(principal),
                &request,
                owner_process,
                self.check_command_admission_mode("inject"),
                now_ms(),
            )?;
            self.publish_locked(&change.events);
            change
        };
        if !change.events.is_empty() {
            for recipient in &change.value.recipients {
                self.schedule_command(recipient.command_id.clone(), None);
            }
            self.signal_progress();
        }
        serde_json::to_value(change.value).map_err(|_| {
            Error::new(
                "internal",
                "project broadcast response could not be encoded",
            )
        })
    }
}
