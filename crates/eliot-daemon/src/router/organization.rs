//! Headless chat and work organization routing (step 4a).
//!
//! Placement is presentation, never authority: attach/detach/tag only
//! write the organizational overlay beside the Agent id through Owner-only
//! Store transactions.
use super::*;
use eliot_core::api::{
    ChatAttachRequest, ChatDetachRequest, ChatOrganizationRequest, ChatTagRequest,
};

impl Router {
    pub(super) async fn chat_organization(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ChatOrganizationRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let store = self.inner.store.lock().await;
        let response = store.chat_organization(&principal_name(principal), &request, now_ms())?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "organization could not be encoded"))
    }

    async fn organization_mutation(
        &self,
        params: &Value,
        principal: &Principal,
        method: &str,
    ) -> Result<Value> {
        require_owner(principal)?;
        let change = {
            let mut store = self.inner.store.lock().await;
            match method {
                "chat.attach" => {
                    let request: ChatAttachRequest =
                        eliot_core::api::discovery::decode_params(params.clone())?;
                    request.validate()?;
                    let change =
                        store.attach_chats(&principal_name(principal), &request, now_ms())?;
                    serde_json::to_value(change.value).map_err(|_| {
                        Error::new("internal", "organization mutation could not be encoded")
                    })?
                }
                "chat.detach" => {
                    let request: ChatDetachRequest =
                        eliot_core::api::discovery::decode_params(params.clone())?;
                    request.validate()?;
                    let change =
                        store.detach_chats(&principal_name(principal), &request, now_ms())?;
                    serde_json::to_value(change.value).map_err(|_| {
                        Error::new("internal", "organization mutation could not be encoded")
                    })?
                }
                _ => {
                    let request: ChatTagRequest =
                        eliot_core::api::discovery::decode_params(params.clone())?;
                    request.validate()?;
                    let change =
                        store.tag_agents(&principal_name(principal), &request, now_ms())?;
                    serde_json::to_value(change.value).map_err(|_| {
                        Error::new("internal", "organization mutation could not be encoded")
                    })?
                }
            }
        };
        Ok(change)
    }

    pub(super) async fn chat_attach(&self, params: &Value, principal: &Principal) -> Result<Value> {
        self.organization_mutation(params, principal, "chat.attach")
            .await
    }

    pub(super) async fn chat_detach(&self, params: &Value, principal: &Principal) -> Result<Value> {
        self.organization_mutation(params, principal, "chat.detach")
            .await
    }

    pub(super) async fn chat_tag(&self, params: &Value, principal: &Principal) -> Result<Value> {
        self.organization_mutation(params, principal, "chat.tag")
            .await
    }
}
