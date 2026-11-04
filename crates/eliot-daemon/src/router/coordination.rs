//! Addressed mail, mailbox reads and shared variables (L5.1).
//!
//! Queueing, bounds, acknowledgement and compare-and-swap live in the Store;
//! the Router only authenticates the caller, rejects known credentials in
//! payloads, publishes committed events, and fans out at most one generic
//! wake notice per routed recipient through the ordinary `send` path. A
//! notice carries only the unread count, never the body; every suppression
//! gate simply skips it.

use super::{Principal, Router, contains_known_secret_including_keys, principal_name};
use crate::common::now_ms;
use eliot_core::{
    Error, Result,
    api::{
        MailAckRequest, MailDelivery, MailInboxRequest, MailInboxResponse, MailReadRequest,
        MailSendRequest, MailSendResponse, VarGetRequest, VarGetResponse, VarSetRequest,
    },
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

pub(super) fn decode_coordination_request<T>(
    params: &Value,
    secrets: &crate::plugin_host::SecretSet,
    method: &str,
    validate: fn(&T) -> Result<()>,
) -> Result<T>
where
    T: DeserializeOwned,
{
    if contains_known_secret_including_keys(params, secrets) {
        return Err(Error::new(
            "secret_in_payload",
            format!("{method} request contains a known credential"),
        ));
    }
    let request: T = serde_json::from_value(params.clone())
        .map_err(|_| Error::new("invalid", format!("{method} params are invalid")))?;
    validate(&request)?;
    Ok(request)
}

fn wake_enabled(snapshot: &Value) -> bool {
    snapshot
        .get("wake_on_mail")
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

fn notice_text(unread: i64) -> String {
    format!("{unread} unread messages; run swarm inbox")
}

impl Router {
    pub(super) async fn mail_send(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: MailSendRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "mail.send",
            MailSendRequest::validate,
        )?;
        let name = principal_name(principal);
        let limits = self.inner.mail.limits();
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.mail_send(&name, &request, limits, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        request.validate_response(&change.value)?;
        self.wake_recipients(&change.value).await;
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "mail delivery could not be encoded"))
    }

    pub(super) async fn mail_inbox(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: MailInboxRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "mail.inbox",
            MailInboxRequest::validate,
        )?;
        let name = principal_name(principal);
        let response: MailInboxResponse =
            self.inner
                .store
                .lock()
                .await
                .mail_inbox(&name, &request, now_ms())?;
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "mail inbox could not be encoded"))
    }

    pub(super) async fn mail_read(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: MailReadRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "mail.read",
            MailReadRequest::validate,
        )?;
        let name = principal_name(principal);
        let response: MailDelivery =
            self.inner
                .store
                .lock()
                .await
                .mail_read(&name, &request, now_ms())?;
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "mail delivery could not be encoded"))
    }

    pub(super) async fn mail_ack(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: MailAckRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "mail.ack",
            MailAckRequest::validate,
        )?;
        let name = principal_name(principal);
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.mail_ack(&name, &request, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        request.validate_response(&change.value)?;
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "mail acknowledgement could not be encoded"))
    }

    pub(super) async fn var_set(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: VarSetRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "var.set",
            VarSetRequest::validate,
        )?;
        let name = principal_name(principal);
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.var_set(&name, &request, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        request.validate_response(&change.value)?;
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "variable write could not be encoded"))
    }

    pub(super) async fn var_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: VarGetRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "var.get",
            VarGetRequest::validate,
        )?;
        let name = principal_name(principal);
        let response: VarGetResponse = self.inner.store.lock().await.var_get(&name, &request)?;
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "variable read could not be encoded"))
    }

    async fn wake_recipients(&self, response: &MailSendResponse) {
        for delivery in &response.deliveries {
            self.wake_recipient(&delivery.recipient).await;
        }
    }

    /// Offer one generic wake to a routed recipient. Every gate — disabled
    /// flag, no live plugin owner, the Store's atomic claim, or the ordinary
    /// send admission (provider hold, drain, unknown gates) — skips the
    /// notice; a skipped wake never fails the queued delivery. When the
    /// claimed `send` is not admitted, the claim is released so the failure
    /// does not consume the notice cooldown.
    async fn wake_recipient(&self, recipient: &str) {
        let snapshot = {
            let store = self.inner.store.lock().await;
            match store.get_agent(recipient) {
                Ok(Some(agent)) => (wake_enabled(&agent.profile_snapshot), agent.generation),
                _ => return,
            }
        };
        // Generation 0 never dispatched: no live generation is routed yet.
        if !snapshot.0 || snapshot.1 < 1 {
            return;
        }
        if !self
            .inner
            .plugin_host
            .controls_generation(recipient, snapshot.1)
        {
            return;
        }
        let notice_id = ulid::Ulid::new().to_string();
        let claim = {
            let mut store = self.inner.store.lock().await;
            match store.mail_notice_claim(recipient, &notice_id, now_ms()) {
                Ok(claim) => claim,
                Err(_) => return,
            }
        };
        let Some(claim) = claim else {
            return;
        };
        let params = json!({
            "id": notice_id,
            "target": recipient,
            "verb": "send",
            "expected_generation": claim.generation,
            "args": {"text": notice_text(claim.unread)},
        });
        if self.submit(&params, &Principal::Owner).await.is_err() {
            let mut store = self.inner.store.lock().await;
            let _ = store.mail_notice_release(recipient, &notice_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Bus;
    use eliot_core::{
        NativeEvent, SourceBatch, SubmitCommand,
        api::{MailAckResponse, VarSetResponse},
    };
    use std::path::PathBuf;
    use tokio::sync::Mutex as AsyncMutex;

    fn router() -> Router {
        let store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        Router::new(
            PathBuf::from("synthetic-data-root"),
            "owner-credential".to_owned(),
            std::sync::Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        )
    }

    async fn ready_agent(router: &Router, project: &str) -> String {
        ready_agent_with_snapshot(router, project, json!({})).await
    }

    async fn ready_agent_with_snapshot(router: &Router, project: &str, snapshot: Value) -> String {
        let store_handle = std::sync::Arc::clone(&router.inner.store);
        let mut store = store_handle.lock().await;
        let now = now_ms();
        let launch_id = ulid::Ulid::new().to_string();
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({
                        "project": project,
                        "workspace": "synthetic-workspace",
                        "profile_snapshot": snapshot,
                    }),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch");
        let agent = admission.command.target.expect("launch target");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        store
            .put_batch(
                SourceBatch {
                    batch_id: ulid::Ulid::new().to_string(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: ulid::Ulid::new().to_string(),
                        data: json!({"command_id":launch_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, eliot_core::CommandState::Applied, None, now)
            .expect("settle launch");
        agent
    }

    fn agent_principal(agent: &str) -> Principal {
        Principal::Agent {
            id: agent.to_owned(),
            generation: 1,
        }
    }

    fn router_with_config(root: &std::path::Path, source: &str) -> Router {
        std::fs::create_dir_all(root).unwrap();
        std::fs::write(root.join("config.toml"), source).unwrap();
        // Loaded once at construction, like daemon startup: sends never
        // re-read the file.
        let mail = crate::config::load_mail_config(root).unwrap();
        let store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        Router::new_with_mail_config(
            PathBuf::from(root),
            "owner-credential".to_owned(),
            std::sync::Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
            mail,
        )
    }

    #[tokio::test]
    async fn router_mail_send_honors_configured_mail_limits() {
        let root = std::env::temp_dir().join(format!("eliot-mail-limits-{}", ulid::Ulid::new()));
        let router = router_with_config(&root, "[mail]\nmax_unread_messages_per_agent = 1\n");
        let a = ready_agent(&router, "app").await;
        let first = json!({
            "id": ulid::Ulid::new().to_string(),
            "to": [a.clone()],
            "mode": "queue",
            "text": "hi",
        });
        router
            .mail_send(&first, &Principal::Owner)
            .await
            .expect("first send fits the configured bound");
        let second = json!({
            "id": ulid::Ulid::new().to_string(),
            "to": [a.clone()],
            "mode": "queue",
            "text": "overflow",
        });
        let error = router
            .mail_send(&second, &Principal::Owner)
            .await
            .unwrap_err();
        assert_eq!(error.code, "mailbox_full");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn agent_mail_roundtrip_keeps_delivery_and_acknowledgement_separate() {
        let router = router();
        let a = ready_agent(&router, "app").await;
        let b = ready_agent(&router, "app").await;
        let send_id = ulid::Ulid::new().to_string();
        let sent: MailSendResponse = serde_json::from_value(
            router
                .mail_send(
                    &json!({
                        "id": send_id,
                        "to": [b],
                        "mode": "queue",
                        "text": "review the diff",
                        "thread": "review",
                    }),
                    &agent_principal(&a),
                )
                .await
                .expect("Agent sends mail"),
        )
        .expect("decode send response");
        assert_eq!(sent.deliveries.len(), 1);
        let delivery_id = sent.deliveries[0].delivery_id.clone();

        // The sender's own mailbox stays empty; delivery is per recipient.
        let empty: MailInboxResponse = serde_json::from_value(
            router
                .mail_inbox(&json!({}), &agent_principal(&a))
                .await
                .expect("sender inbox"),
        )
        .expect("decode inbox");
        assert!(empty.deliveries.is_empty());

        // Reads never mark: two reads return the same unread row.
        let first: MailInboxResponse = serde_json::from_value(
            router
                .mail_inbox(&json!({}), &agent_principal(&b))
                .await
                .expect("recipient inbox"),
        )
        .expect("decode inbox");
        assert_eq!(first.deliveries.len(), 1);
        assert_eq!(first.deliveries[0].delivery_id, delivery_id);
        assert_eq!(first.deliveries[0].sender, a);
        assert_eq!(first.deliveries[0].sender_generation, Some(1));
        assert_eq!(first.deliveries[0].thread.as_deref(), Some("review"));
        assert_eq!(first.deliveries[0].read_at, None);
        let again: MailInboxResponse = serde_json::from_value(
            router
                .mail_inbox(&json!({}), &agent_principal(&b))
                .await
                .expect("reread inbox"),
        )
        .expect("decode inbox");
        assert_eq!(again, first);
        let one: MailDelivery = serde_json::from_value(
            router
                .mail_read(&json!({"delivery_id": delivery_id}), &agent_principal(&b))
                .await
                .expect("read one delivery"),
        )
        .expect("decode delivery");
        assert_eq!(one.read_at, None);

        // Only the exact-id ack marks it, idempotently.
        let ack_id = ulid::Ulid::new().to_string();
        let acked: MailAckResponse = serde_json::from_value(
            router
                .mail_ack(
                    &json!({"id": ack_id, "deliveries": [delivery_id]}),
                    &agent_principal(&b),
                )
                .await
                .expect("ack delivery"),
        )
        .expect("decode ack");
        assert_eq!(acked.acked, vec![delivery_id.clone()]);
        let cleared: MailInboxResponse = serde_json::from_value(
            router
                .mail_inbox(&json!({}), &agent_principal(&b))
                .await
                .expect("inbox after ack"),
        )
        .expect("decode inbox");
        assert!(cleared.deliveries.is_empty());
        router
            .mail_ack(
                &json!({"id": ack_id, "deliveries": [delivery_id]}),
                &agent_principal(&b),
            )
            .await
            .expect("ack replays");

        // The owner reads the same mailbox only by naming it explicitly.
        let error = router
            .mail_inbox(&json!({}), &Principal::Owner)
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid");
        let owned: MailInboxResponse = serde_json::from_value(
            router
                .mail_inbox(&json!({"agent": b}), &Principal::Owner)
                .await
                .expect("owner explicit inbox"),
        )
        .expect("decode inbox");
        assert!(owned.deliveries.is_empty());
        let owned_read: MailDelivery = serde_json::from_value(
            router
                .mail_read(
                    &json!({"agent": b, "delivery_id": delivery_id}),
                    &Principal::Owner,
                )
                .await
                .expect("owner explicit read"),
        )
        .expect("decode delivery");
        assert!(owned_read.read_at.is_some());
    }

    #[tokio::test]
    async fn mail_authority_scopes_mailboxes_projects_and_secrets() {
        let router = router();
        let a = ready_agent(&router, "app").await;
        let b = ready_agent(&router, "other").await;

        let error = router
            .mail_send(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "to": [b],
                    "mode": "queue",
                    "text": "cross-project",
                }),
                &agent_principal(&a),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");

        let error = router
            .mail_inbox(&json!({"agent": b}), &agent_principal(&a))
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");

        let error = router
            .mail_send(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "to": [a],
                    "mode": "queue",
                    "text": "leak owner-credential here",
                }),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "secret_in_payload");

        let error = router
            .mail_send(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "to": [a],
                    "mode": "send",
                    "text": "present me",
                }),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "unsupported");
    }

    #[tokio::test]
    async fn unrouted_and_opted_out_recipients_stay_queue_only() {
        let router = router();
        let a = ready_agent(&router, "app").await;
        let quiet = ready_agent_with_snapshot(&router, "app", json!({"wake_on_mail": false})).await;
        // Admitted but never dispatched: generation 0 is not routed yet.
        let pending = {
            let mut store = router.inner.store.lock().await;
            let admission = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: ulid::Ulid::new().to_string(),
                        target: None,
                        selector: None,
                        verb: "launch".to_owned(),
                        args: json!({"project":"app", "workspace":"synthetic-workspace"}),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .expect("admit launch");
            admission.command.target.expect("launch target")
        };
        for recipient in [&a, &quiet, &pending] {
            router
                .mail_send(
                    &json!({
                        "id": ulid::Ulid::new().to_string(),
                        "to": [recipient],
                        "mode": "queue",
                        "text": "queued",
                    }),
                    &Principal::Owner,
                )
                .await
                .expect("queue mail");
        }
        // No live plugin owner exists in this harness, the second recipient
        // opted out, and the third never dispatched: no notice claim was
        // recorded for any of them.
        let store = router.inner.store.lock().await;
        for recipient in [&a, &quiet, &pending] {
            assert_eq!(store.mail_notice_state(recipient).unwrap(), None);
        }
        for recipient in [&a, &quiet, &pending] {
            let inbox = store
                .mail_inbox(
                    "owner",
                    &MailInboxRequest {
                        agent: Some(recipient.clone()),
                        limit: None,
                        after: None,
                    },
                    now_ms(),
                )
                .expect("owner reads queued mail");
            assert_eq!(inbox.deliveries.len(), 1);
        }
    }

    #[tokio::test]
    async fn var_cas_conflict_and_global_owner_only() {
        let router = router();
        let a = ready_agent(&router, "app").await;
        let principal = agent_principal(&a);

        let first: VarSetResponse = serde_json::from_value(
            router
                .var_set(
                    &json!({
                        "id": ulid::Ulid::new().to_string(),
                        "key": "branch",
                        "value": "main",
                        "expect_version": 0,
                    }),
                    &principal,
                )
                .await
                .expect("create variable with implied project scope"),
        )
        .expect("decode set response");
        assert_eq!(first.scope, "project:app");
        assert_eq!(first.version, 1);
        assert_eq!(first.set_by, format!("agent:{a}:1"));

        let error = router
            .var_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "scope": "project:app",
                    "key": "branch",
                    "value": "stale",
                    "expect_version": 0,
                }),
                &principal,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "version_conflict");

        let read: VarGetResponse = serde_json::from_value(
            router
                .var_get(
                    &json!({"scope": "project:app", "key": "branch"}),
                    &principal,
                )
                .await
                .expect("read variable"),
        )
        .expect("decode get response");
        assert_eq!(read.value, json!("main"));
        assert_eq!(read.version, 1);

        let error = router
            .var_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "scope": "global",
                    "key": "flag",
                    "value": true,
                }),
                &principal,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
        router
            .var_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "scope": "global",
                    "key": "flag",
                    "value": true,
                }),
                &Principal::Owner,
            )
            .await
            .expect("owner writes global");
        router
            .var_get(&json!({"scope": "global", "key": "flag"}), &principal)
            .await
            .expect("Agent reads global");

        let error = router
            .var_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "scope": "project:app",
                    "key": "leak",
                    "value": "owner-credential",
                }),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "secret_in_payload");
    }
}
