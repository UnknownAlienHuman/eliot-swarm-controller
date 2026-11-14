//! Durable completion digest: one coalesced parent mailbox result.
//!
//! A digest freezes a parent recipient plus a captured child command/turn
//! set. The registration id doubles as the later digest mail id, so a
//! reconnect or restart replays the same mail id and the existing mail
//! stable effect identity makes the retry a no-op. Delivery reuses ordinary
//! queued mail (and its generic wake); the thread is `completion-*`, never
//! `attention-*`, and no attention event is emitted.

use super::{Principal, Router, principal_name};
use crate::common::now_ms;
use eliot_core::{
    Error, Result,
    api::{
        MailSendRequest, MailSendResponse, SwarmDigestRequest, SwarmDigestResponse,
        SwarmWaitRequest, WaitEntryResult, digest_mode_completed, digest_text, digest_thread,
    },
};
use eliot_store::DigestRow;
use serde_json::Value;
use tokio::{
    sync::watch,
    time::{Duration, MissedTickBehavior, interval},
};

const DIGEST_POLL_INTERVAL: Duration = Duration::from_secs(2);
const DIGEST_POLL_PAGE: usize = 50;

fn wait_request_for_digest(request: &SwarmDigestRequest) -> SwarmWaitRequest {
    SwarmWaitRequest::new(
        request.commands.clone(),
        request.turns.clone(),
        request.mode.clone(),
        0,
    )
}

fn wait_request_for_row(row: &DigestRow) -> SwarmWaitRequest {
    let turns = row
        .turns
        .iter()
        .map(|t| {
            eliot_core::api::WaitTurnRef::new(t.agent.clone(), t.generation, t.turn_id.clone())
        })
        .collect();
    SwarmWaitRequest::new(row.commands.clone(), turns, row.mode.clone(), 0)
}

fn masked_results(results: Vec<WaitEntryResult>, hidden: &[bool]) -> Vec<WaitEntryResult> {
    results
        .into_iter()
        .enumerate()
        .map(|(i, mut entry)| {
            if hidden.get(i).copied().unwrap_or(false) && entry.status != "not_visible" {
                entry.status = "not_visible".to_owned();
            }
            entry
        })
        .collect()
}

fn pending_response(
    request: &SwarmDigestRequest,
    results: Vec<WaitEntryResult>,
) -> Result<SwarmDigestResponse> {
    let completed = digest_mode_completed(request.mode.as_str(), &results);
    let response = SwarmDigestResponse {
        id: request.id.clone(),
        to: request.to.clone(),
        mode: request.mode.clone(),
        state: "pending".to_owned(),
        delivery_id: None,
        completed,
        results,
    };
    response.validate()?;
    Ok(response)
}

fn delivered_response(
    row: &DigestRow,
    delivery_id: String,
    results: Vec<WaitEntryResult>,
) -> Result<SwarmDigestResponse> {
    let completed = digest_mode_completed(row.mode.as_str(), &results);
    let response = SwarmDigestResponse {
        id: row.id.clone(),
        to: row.parent.clone(),
        mode: row.mode.clone(),
        state: "delivered".to_owned(),
        delivery_id: Some(delivery_id),
        completed,
        results,
    };
    response.validate()?;
    Ok(response)
}

impl Router {
    pub(super) async fn swarm_digest(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: SwarmDigestRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        match principal {
            Principal::Owner | Principal::Agent { .. } => {}
            Principal::Run { .. } => {
                return Err(Error::new(
                    "not_allowed",
                    "Run credentials may call only command.get",
                ));
            }
            Principal::ProjectRule { .. } => {
                return Err(Error::new(
                    "not_allowed",
                    "project rule authority cannot register a digest",
                ));
            }
        }
        if let Principal::Agent { id, .. } = principal
            && request.to != *id
        {
            return Err(Error::new(
                "not_allowed",
                "Agent digest recipient must be its own mailbox",
            ));
        }
        // Freeze visibility with the caller's scope: entries outside scope
        // read `not_visible` now and stay masked at delivery.
        let wait_request = wait_request_for_digest(&request);
        let snapshot = self.wait_snapshot(&wait_request, principal).await?;
        let hidden: Vec<bool> = snapshot
            .iter()
            .map(|entry| entry.status == "not_visible")
            .collect();
        let requester = principal_name(principal);
        let now = now_ms();
        let row = {
            let mut store = self.inner.store.lock().await;
            store
                .digest_register(&requester, &request, &hidden, now)?
                .row
        };
        if let Some(mail_id) = row.delivered_mail_id.clone() {
            let results = self.digest_owner_results(&row).await?;
            let response = delivered_response(&row, mail_id, results)?;
            request.validate_response(&response)?;
            return serde_json::to_value(response)
                .map_err(|_| Error::new("internal", "swarm.digest response could not be encoded"));
        }
        if digest_mode_completed(request.mode.as_str(), &snapshot) {
            match self.deliver_digest(&row.id).await {
                Ok(Some(response)) => {
                    request.validate_response(&response)?;
                    return serde_json::to_value(response).map_err(|_| {
                        Error::new("internal", "swarm.digest response could not be encoded")
                    });
                }
                Ok(None) => {}
                Err(error) if error.code == "mailbox_full" => {}
                Err(error) => return Err(error),
            }
            // Completed but delivery deferred (mailbox full): report pending
            // with the settled snapshot; the driver retries.
            let pending = pending_response(&request, snapshot)?;
            request.validate_response(&pending)?;
            return serde_json::to_value(pending)
                .map_err(|_| Error::new("internal", "swarm.digest response could not be encoded"));
        }
        let pending = pending_response(&request, snapshot)?;
        request.validate_response(&pending)?;
        serde_json::to_value(pending)
            .map_err(|_| Error::new("internal", "swarm.digest response could not be encoded"))
    }

    async fn digest_owner_results(&self, row: &DigestRow) -> Result<Vec<WaitEntryResult>> {
        let wait_request = wait_request_for_row(row);
        let owner = Principal::Owner;
        let results = self.wait_snapshot(&wait_request, &owner).await?;
        Ok(masked_results(results, &row.hidden))
    }

    /// Deliver one pending digest when its mode condition holds. Returns
    /// `Some` when delivered (or already delivered), `None` when still
    /// pending. Reuses the digest id as the mail id for restart safety.
    pub(super) async fn deliver_digest(
        &self,
        digest_id: &str,
    ) -> Result<Option<SwarmDigestResponse>> {
        let row: DigestRow = {
            let store = self.inner.store.lock().await;
            match store.get_completion_digest(digest_id)? {
                Some(row) => row,
                None => {
                    return Err(Error::new("internal", "digest registration disappeared"));
                }
            }
        };
        if let Some(mail_id) = row.delivered_mail_id.clone() {
            let results = self.digest_owner_results(&row).await?;
            return Ok(Some(delivered_response(&row, mail_id, results)?));
        }
        let results = self.digest_owner_results(&row).await?;
        if !digest_mode_completed(row.mode.as_str(), &results) {
            return Ok(None);
        }
        let text = digest_text(&row.id, &row.parent, &row.mode, &results);
        let thread = digest_thread(&row.id);
        let mail = MailSendRequest {
            id: row.id.clone(),
            to: vec![row.parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text,
            thread: Some(thread),
            reply_to_delivery: None,
        };
        let mail_value = serde_json::to_value(&mail)
            .map_err(|_| Error::new("internal", "digest mail could not be encoded"))?;
        let owner = Principal::Owner;
        let reply = match self.mail_send_impl(&mail_value, &owner, true).await {
            Ok(reply) => reply,
            Err(error) => {
                // A retained binding means the mail already exists (possibly
                // acked and pruned since): treat as delivered without a
                // second row. Anything else stays pending for retry.
                if error.code == "mailbox_full" {
                    return Err(error);
                }
                return Err(error);
            }
        };
        let mail_response: MailSendResponse = serde_json::from_value(reply)
            .map_err(|_| Error::new("internal", "digest mail response is invalid"))?;
        let delivery_id = mail_response
            .deliveries
            .first()
            .map(|d| d.delivery_id.clone())
            .ok_or_else(|| Error::new("internal", "digest mail delivered nothing"))?;
        {
            let mut store = self.inner.store.lock().await;
            store.mark_digest_delivered(&row.id, &delivery_id, now_ms())?;
        }
        self.signal_progress();
        Ok(Some(delivered_response(&row, delivery_id, results)?))
    }

    /// Try every pending digest once; returns the count delivered. A full
    /// mailbox stays pending for the next tick.
    pub(crate) async fn poll_completion_digests_once(&self) -> Result<usize> {
        let pending: Vec<DigestRow> = {
            let store = self.inner.store.lock().await;
            store.pending_completion_digests(DIGEST_POLL_PAGE)?
        };
        let mut delivered = 0;
        for row in pending {
            match self.deliver_digest(&row.id).await {
                Ok(Some(_)) => delivered += 1,
                Ok(None) => {}
                Err(error) if error.code == "mailbox_full" => {}
                Err(error) => {
                    crate::logging::record(
                        &eliot_core::SecretPolicy::default(),
                        "daemon.digest_delivery_failed",
                        &[],
                        Some(&error),
                    );
                }
            }
        }
        Ok(delivered)
    }
}

/// Periodically deliver pending completion digests. Each tick pages pending
/// registrations and delivers those whose frozen set now satisfies `all`/`any`
/// through one ordinary queued mail; the mail id is the digest id, so a
/// restart replays the same id without duplicating the result.
pub(crate) async fn run_completion_digest(
    router: Router,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut ticks = interval(DIGEST_POLL_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Deliver immediately on startup so a digest completed before a restart
    // does not wait a full interval for its single parent mail.
    if let Err(error) = router.poll_completion_digests_once().await {
        crate::logging::record(
            &eliot_core::SecretPolicy::default(),
            "daemon.digest_poll_failed",
            &[],
            Some(&error),
        );
    }
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = ticks.tick() => {}
        }
        if let Err(error) = router.poll_completion_digests_once().await {
            crate::logging::record(
                &eliot_core::SecretPolicy::default(),
                "daemon.digest_poll_failed",
                &[],
                Some(&error),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::WaitTurnRef;
    use serde_json::json;

    #[test]
    fn digest_masks_scope_without_widening_reads() {
        let visible = vec![
            WaitEntryResult::new("command", "c1", "succeeded"),
            WaitEntryResult::new("command", "c2", "failed"),
        ];
        let masked = masked_results(visible, &[false, true]);
        assert_eq!(masked[0].status, "succeeded");
        assert_eq!(masked[1].status, "not_visible");
        // Owner recomputation never clears a registration-time mask.
        let masked = masked_results(masked, &[false, true]);
        assert_eq!(masked[1].status, "not_visible");
    }

    #[test]
    fn digest_request_validation_rejects_foreign_agent_mailbox() {
        let req = SwarmDigestRequest::new(
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "owner",
            "all",
            vec!["c1".to_owned()],
            vec![WaitTurnRef::new("a1", 1, "t1")],
        );
        assert!(req.validate().is_err());
    }

    #[test]
    fn digest_example_shape_stays_valid() {
        let example = json!({
            "id": "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "to": "a1",
            "mode": "all",
            "commands": ["01ARZ3NDEKTSV4RRFFQ69G5FAV"],
            "turns": []
        });
        let req: SwarmDigestRequest = serde_json::from_value(example).unwrap();
        req.validate().unwrap();
    }
}
