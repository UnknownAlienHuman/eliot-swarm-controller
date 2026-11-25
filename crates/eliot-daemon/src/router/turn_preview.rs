use super::{Principal, Router, hierarchy, principal_name};
use eliot_core::{
    Error, Result,
    api::{TurnPreviewWatchRequest, TurnPreviewWatchResponse, discovery::decode_params},
};
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
        let principal_key = match principal {
            Principal::Agent { id, .. } => format!("agent:{id}"),
            Principal::RemoteManager(grant) => format!("remote:{}", grant.instance),
            other => principal_name(other),
        };
        let (mut watch, mut snapshot) = {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_agent_read(
                &store,
                principal,
                &request.agent,
                Some(request.generation),
            )?;
            if !active(&store, &request)? {
                return encoded(ended());
            }
            // Reserve before releasing the Store read so a committed terminal
            // cannot slip between the active-turn check and subscription.
            (
                previews.subscribe(&request, principal_key)?,
                previews.snapshot(&request),
            )
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut waited = false;
        let mut response = loop {
            if *watch.updates.borrow() {
                break ended();
            }
            let current = Some(snapshot.as_ref().map_or(0, |snapshot| snapshot.revision));
            if request.revision != current {
                break TurnPreviewWatchResponse {
                    revision: current,
                    preview: snapshot.and_then(|snapshot| snapshot.render()),
                    ended: false,
                };
            }
            waited = true;
            if timeout_at(deadline, watch.updates.changed()).await.is_err() {
                break TurnPreviewWatchResponse {
                    revision: current,
                    preview: None,
                    ended: false,
                };
            }
            snapshot = previews.snapshot(&request);
        };
        // Delegations and remote grants can change during a long poll.
        // Recheck once at disclosure, independently of wake frequency.
        if waited && !matches!(principal, Principal::Owner) {
            let store = self.inner.store.lock().await;
            hierarchy::authorize_retained_work_read(
                &store,
                principal,
                &request.agent,
                request.generation,
            )?;
            if !active(&store, &request)? {
                response = ended();
            }
        } else if *watch.updates.borrow() {
            response = ended();
        }
        encoded(response)
    }
}

fn active(store: &eliot_store::Store, request: &TurnPreviewWatchRequest) -> Result<bool> {
    let agent = store
        .get_agent(&request.agent)?
        .ok_or_else(|| Error::new("not_found", "preview Agent was not found"))?;
    Ok(agent.generation == request.generation
        && agent.active_turn.as_deref() == Some(&request.turn_id))
}

fn ended() -> TurnPreviewWatchResponse {
    TurnPreviewWatchResponse {
        revision: None,
        preview: None,
        ended: true,
    }
}

fn encoded(response: TurnPreviewWatchResponse) -> Result<Value> {
    serde_json::to_value(response)
        .map_err(|_| Error::new("internal", "turn preview could not be encoded"))
}
