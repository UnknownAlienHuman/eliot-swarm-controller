use super::{Principal, Router, hierarchy};
use eliot_core::{
    Error, Result,
    api::{TurnPreviewWatchRequest, TurnPreviewWatchResponse, discovery::decode_params},
};
use eliot_plugin::sync::LockExt;
use serde_json::Value;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

impl Router {
    pub(super) async fn turn_preview_watch(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: TurnPreviewWatchRequest = decode_params(params.clone())?;
        request.validate()?;
        let previews = &self.inner.bus.previews;
        let mut updates = previews.subscribe();
        let deadline = Instant::now() + Duration::from_secs(20);
        let response = loop {
            let snapshot = {
                let store = self.inner.store.lock().await;
                hierarchy::authorize_agent_read(
                    &store,
                    principal,
                    &request.agent,
                    Some(request.generation),
                )?;
                let agent = store
                    .get_agent(&request.agent)?
                    .ok_or_else(|| Error::new("not_found", "preview Agent was not found"))?;
                if agent.generation != request.generation
                    || agent.active_turn.as_deref() != Some(&request.turn_id)
                {
                    break TurnPreviewWatchResponse {
                        revision: None,
                        preview: None,
                        ended: true,
                    };
                }
                previews.snapshot(&request)
            };
            let current = Some(snapshot.as_ref().map_or(0, |snapshot| snapshot.revision));
            if request.revision != current {
                let preview = snapshot.and_then(|snapshot| {
                    let secrets: Vec<_> =
                        self.inner.secrets.lock_recover().iter().cloned().collect();
                    snapshot.render(&secrets)
                });
                break TurnPreviewWatchResponse {
                    revision: current,
                    preview,
                    ended: false,
                };
            }
            if timeout_at(deadline, updates.changed()).await.is_err() {
                break TurnPreviewWatchResponse {
                    revision: None,
                    preview: None,
                    ended: false,
                };
            }
        };
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "turn preview could not be encoded"))
    }
}
