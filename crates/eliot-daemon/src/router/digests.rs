//! Durable completion digest: one coalesced parent mailbox result.
//!
//! A digest freezes a parent recipient plus a captured child command/turn
//! set. The registration id doubles as the later digest mail id, so a
//! reconnect or restart replays the same mail id and the existing mail
//! stable effect identity makes the retry a no-op. Delivery reuses ordinary
//! queued mail (and its generic wake); the thread is `completion-*`, never
//! `attention-*`, and no attention event is emitted.

use super::{
    Principal, Router, UnknownMarks, WaitScope, coordination::decode_coordination_request,
    principal_name,
};
use crate::common::now_ms;
use eliot_core::{
    Error, Result,
    api::{
        MailSendRequest, SWARM_MAIL_SENDER, SwarmDigestRequest, SwarmDigestResponse,
        SwarmWaitRequest, WaitEntryResult, digest_mode_completed, digest_text, digest_thread,
    },
};
use eliot_plugin::sync::LockExt;
use eliot_store::DigestRow;
use serde_json::Value;
use tokio::{
    sync::watch,
    time::{Duration, MissedTickBehavior, interval},
};

const DIGEST_POLL_INTERVAL: Duration = Duration::from_secs(2);
const DIGEST_POLL_PAGE: usize = 50;

/// Pending digest keyset position: `(created_at, id)`.
type DigestCursorKey = (i64, String);

/// Next sweep cursor and high-water after one page read. `last` is the last
/// key of the page just read (`None` for an empty page) and `end` is the
/// cycle high-water captured with the sweep's first page.
///
/// Wrap (reset both to `None`) when the page is empty, when the cursor
/// reaches or passes the captured high-water, or when the page is short
/// before the bound (rows delivered mid-sweep drained it): each of those ends
/// the cycle at the head. A non-empty page with no bound also wraps so the
/// next tick re-snapshots instead of chasing without a bound. Otherwise the
/// cursor advances past the page and the bound is kept. A sweep therefore
/// always ends within a bounded number of pages however fast new rows arrive,
/// and the head is revisited every cycle.
fn advance_digest_poll(
    last: Option<DigestCursorKey>,
    page_len: usize,
    end: Option<DigestCursorKey>,
) -> (Option<DigestCursorKey>, Option<DigestCursorKey>) {
    let Some(last_key) = last else {
        return (None, None);
    };
    if end.as_ref().is_none_or(|end| last_key >= *end) {
        return (None, None);
    }
    if page_len < DIGEST_POLL_PAGE {
        return (None, None);
    }
    (Some(last_key), end)
}

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

/// Delivery-time results inside the caller's Store guard: the persisted
/// registration mask plus the recipient's live scope over each entry.
fn digest_results_locked(
    store: &eliot_store::Store,
    row: &DigestRow,
    marks: &UnknownMarks,
) -> Result<Vec<WaitEntryResult>> {
    let scope = WaitScope {
        hidden: Some(&row.hidden),
        excerpt_recipient: Some(&row.parent),
        fair_budget: true,
    };
    Router::wait_snapshot_locked(
        store,
        &wait_request_for_row(row),
        &Principal::Owner,
        None,
        scope,
        marks,
    )
    .map(|snapshot| snapshot.results)
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
            Principal::Run { .. } | Principal::MemoryInference { .. } => {
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
            Principal::Swarm(_) => {
                return Err(Error::new(
                    "not_allowed",
                    "internal Swarm principals cannot register a digest",
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
        let requester = principal_name(principal);
        let marks = self.wait_unknown_marks().await;
        let (row, snapshot) = {
            let mut store = self.inner.store.lock().await;
            let scope = WaitScope {
                hidden: None,
                excerpt_recipient: Some(&request.to),
                fair_budget: true,
            };
            let mut snapshot =
                Self::wait_snapshot_locked(&store, &wait_request, principal, None, scope, &marks)?
                    .results;
            let hidden: Vec<bool> = snapshot
                .iter()
                .map(|entry| entry.status == "not_visible")
                .collect();
            let row = store
                .digest_register(&requester, &request, &hidden, now_ms())?
                .row;
            if let Some(mail_id) = row.delivered_mail_id.clone() {
                let results = digest_results_locked(&store, &row, &marks)?;
                let response = delivered_response(&row, mail_id, results)?;
                request.validate_response(&response)?;
                return serde_json::to_value(response).map_err(|_| {
                    Error::new("internal", "swarm.digest response could not be encoded")
                });
            }
            // A retry keeps the first admitted mask: an entry hidden then
            // stays hidden even after it moves into scope, so the persisted
            // mask applies before any excerpt is budgeted.
            if row.hidden != hidden {
                let scope = WaitScope {
                    hidden: Some(&row.hidden),
                    ..scope
                };
                snapshot = Self::wait_snapshot_locked(
                    &store,
                    &wait_request,
                    principal,
                    None,
                    scope,
                    &marks,
                )?
                .results;
            }
            (row, snapshot)
        };
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

    /// Deliver one pending digest when its mode condition holds. Returns
    /// `Some` when delivered (or already delivered), `None` when still
    /// pending. Reuses the digest id as the mail id for restart safety.
    ///
    /// One Store guard spans the recipient-scope authorization and the mail
    /// insert (which commits the delivered mark in its own transaction): no
    /// reparent can land between them, so an answer reaches the recipient
    /// only while it has scope over that worker at commit. Only the wake that
    /// follows the committed mail runs after the guard.
    pub(super) async fn deliver_digest(
        &self,
        digest_id: &str,
    ) -> Result<Option<SwarmDigestResponse>> {
        let marks = self.wait_unknown_marks().await;
        let (row, results, mail, change) = {
            let mut store = self.inner.store.lock().await;
            let row = store
                .get_completion_digest(digest_id)?
                .ok_or_else(|| Error::new("internal", "digest registration disappeared"))?;
            let results = digest_results_locked(&store, &row, &marks)?;
            if let Some(mail_id) = row.delivered_mail_id.clone() {
                return Ok(Some(delivered_response(&row, mail_id, results)?));
            }
            if !digest_mode_completed(row.mode.as_str(), &results) {
                return Ok(None);
            }
            let mail = MailSendRequest {
                wake: true,
                id: row.id.clone(),
                to: vec![row.parent.clone()],
                mode: eliot_core::api::MailMode::Queue,
                text: digest_text(&row.id, &row.parent, &row.mode, &results),
                thread: Some(digest_thread(&row.id)),
                reply_to_delivery: None,
            };
            let mail: MailSendRequest = decode_coordination_request(
                &serde_json::to_value(&mail)
                    .map_err(|_| Error::new("internal", "digest mail could not be encoded"))?,
                &self.inner.secrets,
                "mail.send",
                MailSendRequest::validate,
            )?;
            let change = self.mail_send_locked(&mut store, SWARM_MAIL_SENDER, &mail, true)?;
            (row, results, mail, change)
        };
        let delivery_id = self
            .mail_send_finish(&mail, change)
            .await?
            .deliveries
            .first()
            .map(|delivery| delivery.delivery_id.clone())
            .ok_or_else(|| Error::new("internal", "digest mail delivered nothing"))?;
        self.signal_progress();
        Ok(Some(delivered_response(&row, delivery_id, results)?))
    }

    /// Sweep one bounded pending-digest page; returns the count delivered. A
    /// full mailbox stays pending for the next tick. The page seeks after the
    /// last examined registration through the pending keyset cursor and the
    /// cursor advances past every examined row, wrapping once it passes the
    /// sweep high-water snapshotted with the first page of the sweep. The
    /// high-water and each cycle-start page are read in one Store transaction,
    /// so an empty high-water can never pair with a populated page (the race
    /// that left a sweep with no wrap bound while full pages of new
    /// registrations kept arriving). New registrations that arrive mid-sweep
    /// join the next sweep, so a continuously growing tail cannot starve an
    /// older registration that becomes eligible behind the cursor: every
    /// registration observed at sweep start is revisited once per sweep, and
    /// every sweep ends within one bounded cycle.
    pub(crate) async fn poll_completion_digests_once(&self) -> Result<usize> {
        let after: Option<(i64, String)> = self.inner.digest_poll_cursor.lock_recover().clone();
        // A new sweep snapshots its high-water together with its first page
        // in one Store transaction; continuation ticks keep the captured
        // bound and read only the page.
        let mut pending: Vec<DigestRow> = if after.is_none() {
            let snapshot = {
                let mut store = self.inner.store.lock().await;
                store.pending_completion_digests_page(None, DIGEST_POLL_PAGE)?
            };
            *self.inner.digest_sweep_end.lock_recover() = snapshot.high_water;
            snapshot.rows
        } else {
            let store = self.inner.store.lock().await;
            let after = after
                .as_ref()
                .map(|(created_at, id)| (*created_at, id.as_str()));
            store.pending_completion_digests_after(after, DIGEST_POLL_PAGE)?
        };
        if pending.is_empty() && after.is_some() {
            // Swept past the newest row: wrap to the oldest so registrations
            // behind the old cursor are examined on this same tick, starting
            // a new sweep with a fresh high-water below.
            let snapshot = {
                let mut store = self.inner.store.lock().await;
                store.pending_completion_digests_page(None, DIGEST_POLL_PAGE)?
            };
            *self.inner.digest_sweep_end.lock_recover() = snapshot.high_water;
            pending = snapshot.rows;
        }
        {
            let end = self.inner.digest_sweep_end.lock_recover().clone();
            let last = pending.last().map(|row| (row.created_at, row.id.clone()));
            let (cursor, sweep_end) = advance_digest_poll(last, pending.len(), end);
            *self.inner.digest_poll_cursor.lock_recover() = cursor;
            *self.inner.digest_sweep_end.lock_recover() = sweep_end;
        }
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

/// Periodically deliver pending completion digests. Each tick sweeps one
/// bounded pending-registration page through a progressing keyset cursor
/// (wrapping at the end) and delivers those whose frozen set now satisfies
/// `all`/`any`
/// through one ordinary queued mail; the mail id is the digest id, so a
/// restart replays the same id without duplicating the result.
pub(crate) async fn run_completion_digest(
    router: Router,
    mut shutdown: watch::Receiver<bool>,
    reporter: crate::daemon::workers::WorkerReporter,
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
    reporter.running("completion_digest");
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
