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
    AgentState, Error, Result,
    api::{
        MailAckRequest, MailDelivery, MailInboxRequest, MailReadRequest, MailSendRequest,
        MailSendResponse, VarGetRequest, VarSetRequest,
    },
};
use eliot_plugin::sync::LockExt;
use eliot_store::{MailSendOutcome, MailSendResult, NoticeKind, NoticePending};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::{
    sync::{oneshot, watch},
    time::{Duration, MissedTickBehavior, interval},
};

pub(super) struct PendingVarWrite {
    principal: String,
    request: VarSetRequest,
    reply: oneshot::Sender<Result<eliot_core::api::VarSetResponse>>,
}

#[derive(Default)]
pub(super) struct VarWriteQueue {
    pending: std::collections::VecDeque<PendingVarWrite>,
    active: bool,
    closed: bool,
}

struct VarWorkerGuard {
    router: Router,
    armed: bool,
}

impl Drop for VarWorkerGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut queue = self.router.inner.var_writes.lock_recover();
        queue.closed = true;
        queue.active = false;
        for write in queue.pending.drain(..) {
            let _ = write.reply.send(Err(Error::new(
                "unknown",
                "variable writer ended without a result",
            )));
        }
        drop(queue);
        self.router.signal_progress();
    }
}

pub(super) fn decode_coordination_request<T>(
    params: &Value,
    secrets: &crate::plugin_host::SecretSet,
    method: &str,
    validate: fn(&T) -> Result<()>,
) -> Result<T>
where
    T: DeserializeOwned + schemars::JsonSchema,
{
    if contains_known_secret_including_keys(params, secrets) {
        return Err(Error::new(
            "secret_in_payload",
            format!("{method} request contains a known credential"),
        ));
    }
    let request: T = eliot_core::api::discovery::decode_params(params.clone())?;
    validate(&request)?;
    Ok(request)
}

fn wake_enabled(snapshot: &Value) -> bool {
    snapshot
        .get("wake_on_mail")
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

/// One neutral wake text for every harness: the unread count plus the
/// coordination method path. Every harness resolves `mail.inbox`/`mail.ack`
/// through its own surface (the MCP swarm tool or the shell CLI), so the
/// notice no longer selects wording per harness.
fn notice_text(kind: NoticeKind, count: i64) -> String {
    match kind {
        NoticeKind::Mail => format!("{count} unread: mail.inbox, then mail.ack"),
        NoticeKind::Claim if count == 1 => "1 overlapping work claim: who".to_owned(),
        NoticeKind::Claim => format!("{count} overlapping work claims: who"),
    }
}

impl Router {
    pub(super) async fn mail_send(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: MailSendRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "mail.send",
            MailSendRequest::validate,
        )?;
        let change = {
            let mut store = self.inner.store.lock().await;
            self.mail_send_locked(&mut store, &principal_name(principal), &request, false)?
        };
        let response = self.mail_send_finish(&request, change).await?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "mail delivery could not be encoded"))
    }

    /// Admit one mail inside the caller's Store guard and publish its
    /// committed events, so a caller can keep that guard from an
    /// authorization read through the insert. `allow_digest_id` is true only
    /// for completion-digest delivery, whose mail id is the digest
    /// registration id by design; the public endpoint always rejects
    /// digest-bound ids.
    pub(super) fn mail_send_locked(
        &self,
        store: &mut eliot_store::Store,
        name: &str,
        request: &MailSendRequest,
        allow_digest_id: bool,
    ) -> Result<MailSendResult> {
        let limits = self.inner.mail.limits();
        let change = if allow_digest_id {
            store.mail_send_digest_delivery(name, request, limits, now_ms())?
        } else {
            store.mail_send(name, request, limits, now_ms())?
        };
        if change.outcome == MailSendOutcome::Fresh {
            self.publish_locked(&change.events);
        }
        Ok(change)
    }

    /// After the Store guard is released: signal progress, check the
    /// response binding and wake the recipients of a fresh mail.
    pub(super) async fn mail_send_finish(
        &self,
        request: &MailSendRequest,
        change: MailSendResult,
    ) -> Result<MailSendResponse> {
        let fresh = change.outcome == MailSendOutcome::Fresh;
        if fresh {
            self.signal_progress();
        }
        request.validate_response(&change.value)?;
        if fresh {
            self.wake_recipients(&change.value).await;
        }
        Ok(change.value)
    }

    pub(super) async fn mail_inbox(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: MailInboxRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "mail.inbox",
            MailInboxRequest::validate,
        )?;
        let name = principal_name(principal);
        // An Agent reads its own mailbox (it may omit `agent`); the owner
        // names it. The long-poll wakes on bodyless deliveries to it and the
        // page itself applies the `thread`/`reply_to` selection.
        let mailbox = match principal {
            Principal::Agent { id, .. } => Some(id.clone()),
            _ => request.agent.clone(),
        };
        let mut poll = self.long_poll(request.wait_ms);
        let response = loop {
            let page = self
                .inner
                .store
                .lock()
                .await
                .mail_inbox(&name, &request, now_ms())?;
            if !page.deliveries.is_empty()
                || !poll
                    .wake(|event| {
                        event.kind == "mail.delivered"
                            && event.agent.is_some()
                            && event.agent == mailbox
                    })
                    .await?
            {
                break page;
            }
        };
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
        let (reply, received) = oneshot::channel();
        {
            let mut pending = self.inner.var_writes.lock_recover();
            if pending.closed {
                return Err(Error::new(
                    "unavailable",
                    "variable write admission is closed",
                ));
            }
            if pending.pending.len() >= eliot_store::MAX_VAR_WRITE_GROUP * 2 {
                return Err(Error::new(
                    "busy",
                    "variable write queue is full; nothing admitted",
                ));
            }
            pending.pending.push_back(PendingVarWrite {
                principal: name,
                request: request.clone(),
                reply,
            });
            // One owned consumer for every queued group, including after caller
            // cancellation. Queue admission and worker retirement share this lock.
            if !pending.active {
                // A shutdown joiner first closes admission under this same
                // queue lock, so an open queue can always take this guard.
                let mut worker = self
                    .inner
                    .var_worker
                    .try_lock()
                    .expect("open variable queue has no shutdown joiner");
                let router = self.clone();
                pending.active = true;
                *worker = Some(tokio::spawn(async move {
                    router.flush_var_writes().await;
                }));
            }
        }
        let response = received
            .await
            .map_err(|_| Error::new("unknown", "variable write result was not delivered"))??;
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "variable write could not be encoded"))
    }

    async fn flush_var_writes(&self) {
        let mut failure = VarWorkerGuard {
            router: self.clone(),
            armed: true,
        };
        loop {
            let mut store = self.inner.store.lock().await;
            let pending: Vec<_> = {
                let mut queue = self.inner.var_writes.lock_recover();
                if queue.pending.is_empty() {
                    queue.active = false;
                    failure.armed = false;
                    return;
                }
                let count = queue.pending.len().min(eliot_store::MAX_VAR_WRITE_GROUP);
                queue.pending.drain(..count).collect()
            };
            let requests: Vec<_> = pending
                .iter()
                .map(|write| (write.principal.clone(), write.request.clone()))
                .collect();
            let results = store.var_set_group(&requests, now_ms());
            // Publish all committed rows in Store order before another writer can
            // publish a later sequence, then deliver each caller's own outcome.
            for change in results.iter().filter_map(|result| result.as_ref().ok()) {
                self.publish_locked(&change.events);
            }
            drop(store);
            self.signal_progress();
            for (write, result) in pending.into_iter().zip(results) {
                let _ = write.reply.send(result.map(|change| change.value));
            }
        }
    }

    pub(crate) async fn finish_var_writes(&self) {
        // Concurrent shutdown callers join the same completion boundary.
        {
            let mut queue = self.inner.var_writes.lock_recover();
            queue.closed = true;
        }
        // Borrow the owned handle while joining: cancellation of a shutdown
        // caller leaves the handle here for the daemon's final barrier.
        let mut owned = self.inner.var_worker.lock().await;
        if let Some(worker) = owned.as_mut()
            && worker.await.is_err()
        {
            let mut queue = self.inner.var_writes.lock_recover();
            queue.active = false;
            for write in queue.pending.drain(..) {
                let _ = write.reply.send(Err(Error::new(
                    "unknown",
                    "variable writer task ended without a result",
                )));
            }
        }
        owned.take();
    }

    pub(super) async fn var_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: VarGetRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "var.get",
            VarGetRequest::validate,
        )?;
        let name = principal_name(principal);
        // `after_version` waits until the version differs; an absent row
        // reads as version 0, so only `not_found` is a waitable state.
        let scope = self
            .inner
            .store
            .lock()
            .await
            .var_scope_encoded(&name, request.scope.as_deref())?;
        let mut poll = self.long_poll(request.wait_ms);
        let read = loop {
            let read = match self.inner.store.lock().await.var_get(&name, &request) {
                Err(error) if error.code != "not_found" => return Err(error),
                read => read,
            };
            let version = read.as_ref().map_or(0, |response| response.version);
            if request
                .after_version
                .is_none_or(|baseline| version != baseline)
                || !poll
                    .wake(|event| {
                        event.kind == "var.changed"
                            && event.data.get("scope").and_then(Value::as_str)
                                == Some(scope.as_str())
                            && event.data.get("key").and_then(Value::as_str)
                                == Some(request.key.as_str())
                    })
                    .await?
            {
                break read;
            }
        };
        let response = read?;
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "variable read could not be encoded"))
    }

    async fn wake_recipients(&self, response: &MailSendResponse) {
        for delivery in &response.deliveries {
            self.offer_notice(&delivery.recipient, NoticePending::Mail)
                .await;
        }
    }

    /// Offer one generic notice to a routed recipient through the shared
    /// Store gate. Every gate — the per-kind routing pre-check, no live
    /// plugin owner, the Store's atomic claim (mail-only busy/waiting plus
    /// pending send input), or the ordinary send admission (provider hold,
    /// drain, unknown gates, plus final mail-only busy/pending
    /// revalidation via structured kind) — skips the notice; a skipped
    /// notice never fails the admitted write. When the claimed `send` is
    /// not admitted, the claim is released so the failure does not consume
    /// the notice cooldown, which otherwise runs from native presentation.
    pub(super) async fn offer_notice(&self, recipient: &str, pending: NoticePending) {
        let agent = {
            let store = self.inner.store.lock().await;
            match store.get_agent(recipient) {
                Ok(Some(agent)) => agent,
                _ => return,
            }
        };
        // Generation 0 never dispatched: no live generation is routed yet.
        // Mail honors the recipient's wake flag and defers while busy or
        // waiting, since an ordinary queued send cannot notify mid-turn
        // anyway; claims stop at terminal state instead, since a terminal
        // holder's claims already released.
        match pending.kind() {
            NoticeKind::Mail
                if !wake_enabled(&agent.profile_snapshot)
                    || agent.generation < 1
                    || matches!(agent.state, AgentState::Busy | AgentState::Waiting) =>
            {
                return;
            }
            NoticeKind::Claim if agent.state.is_terminal() || agent.generation < 1 => {
                return;
            }
            _ => {}
        }
        if !self
            .inner
            .plugin_host
            .controls_generation(recipient, agent.generation)
        {
            return;
        }
        let notice_id = ulid::Ulid::new().to_string();
        let claim = {
            let mut store = self.inner.store.lock().await;
            match store.notice_claim(recipient, &notice_id, pending, now_ms()) {
                Ok(claim) => claim,
                Err(_) => return,
            }
        };
        let Some(claim) = claim else {
            return;
        };
        let notice_kind = match claim.kind {
            NoticeKind::Mail => "mail",
            NoticeKind::Claim => "claim",
        };
        let has_inline = claim.inline_text.is_some();
        let text = claim
            .inline_text
            .unwrap_or_else(|| notice_text(claim.kind, claim.count));
        let mut args = json!({"text": text, "notice_kind": notice_kind});
        if has_inline {
            args["inline_digest"] = json!(true);
        }
        let params = json!({
            "id": notice_id,
            "target": recipient,
            "verb": "send",
            "expected_generation": claim.generation,
            "args": args,
        });
        if self.submit(&params, &Principal::Owner).await.is_err() {
            let mut store = self.inner.store.lock().await;
            let _ = store.notice_release(recipient, &notice_id);
        }
    }

    /// Re-offer a deferred mail wake after the recipient idles. The shared
    /// Store gate revalidates unread, generation and every suppression at
    /// claim time, so idle events without mail are harmless no-ops and no
    /// cooldown is spent unless a wake is actually claimed.
    pub(super) async fn reoffer_mail_notice(&self, recipient: &str) {
        self.offer_notice(recipient, NoticePending::Mail).await;
    }
}

/// Normal notice cooldown re-page interval for the deferred mail wake driver.
const MAIL_REOFFER_INTERVAL: Duration = Duration::from_secs(2);
/// Bounded recipient page per driver tick.
const MAIL_REOFFER_PAGE: usize = 100;

/// Periodically re-offer deferred mail wakes to Idle recipients past the
/// normal notice cooldown. Each tick pages due recipients with unread mail
/// and re-offers through the existing atomic claim; the claim stays
/// authoritative, so races and suppressions are harmless no-ops.
pub(crate) async fn run_mail_reoffer(
    router: Router,
    mut shutdown: watch::Receiver<bool>,
    reporter: crate::daemon::workers::WorkerReporter,
) -> Result<()> {
    let mut ticks = interval(MAIL_REOFFER_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut after: Option<String> = None;
    reporter.running("mail_reoffer");
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = ticks.tick() => {}
        }
        let recipients = {
            let store = router.inner.store.lock().await;
            match store.due_mail_notice_recipients(after.as_deref(), MAIL_REOFFER_PAGE, now_ms()) {
                Ok(recipients) => recipients,
                Err(error) => {
                    crate::logging::record(
                        &eliot_core::SecretPolicy::default(),
                        "daemon.mail_reoffer_query_failed",
                        &[],
                        Some(&error),
                    );
                    after = None;
                    continue;
                }
            }
        };
        if recipients.is_empty() {
            after = None;
            continue;
        }
        let short = recipients.len() < MAIL_REOFFER_PAGE;
        for recipient in &recipients {
            router.reoffer_mail_notice(recipient).await;
        }
        after = if short {
            None
        } else {
            recipients.last().cloned()
        };
    }
}
