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

#[cfg(test)]
fn masked_results(results: Vec<WaitEntryResult>, hidden: &[bool]) -> Vec<WaitEntryResult> {
    results
        .into_iter()
        .enumerate()
        .map(|(i, entry)| {
            if hidden.get(i).copied().unwrap_or(false) {
                WaitEntryResult::new(entry.kind, entry.id, "not_visible")
            } else {
                entry
            }
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
        let snapshot = self
            .wait_snapshot_scoped(
                &wait_request,
                principal,
                None,
                None,
                Some(&request.to),
                true,
            )
            .await?
            .results;
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
            let results = self.digest_scoped_results(&row).await?;
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

    async fn digest_scoped_results(&self, row: &DigestRow) -> Result<Vec<WaitEntryResult>> {
        let wait_request = wait_request_for_row(row);
        self.wait_snapshot_scoped(
            &wait_request,
            &Principal::Owner,
            None,
            Some(&row.hidden),
            Some(&row.parent),
            true,
        )
        .await
        .map(|snapshot| snapshot.results)
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
            let results = self.digest_scoped_results(&row).await?;
            return Ok(Some(delivered_response(&row, mail_id, results)?));
        }
        let results = self.digest_scoped_results(&row).await?;
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
        let mut visible = vec![
            WaitEntryResult::new("command", "c1", "succeeded")
                .with_turn("a1", 1, "t1")
                .with_final_text("ok", 1024),
            WaitEntryResult::new("command", "c2", "failed")
                .with_turn("a2", 1, "t2")
                .with_final_text("secret out of scope", 1024),
        ];
        visible[1].resolution_source = Some(eliot_core::TurnResolutionSource::Owner);
        let masked = masked_results(visible, &[false, true]);
        assert_eq!(masked[0].status, "succeeded");
        assert_eq!(masked[0].final_text.as_deref(), Some("ok"));
        assert_eq!(masked[1].status, "not_visible");
        assert_eq!(masked[1].resolution_source, None);
        assert_eq!(masked[1].agent, None);
        assert_eq!(masked[1].generation, None);
        assert_eq!(masked[1].turn_id, None);
        assert_eq!(masked[1].final_text, None);
        assert!(!masked[1].truncated);
        // Owner recomputation never clears a registration-time mask.
        let masked = masked_results(masked, &[false, true]);
        assert_eq!(masked[1].status, "not_visible");
    }

    #[test]
    fn digest_poll_advance_wraps_at_bound_and_on_short_pages() {
        let key = |created_at: i64| Some((created_at, format!("id-{created_at:05}")));
        let end = || Some((120, "id-00120".to_owned()));
        // Full page below the bound advances with the bound kept.
        assert_eq!(
            advance_digest_poll(key(50), 50, end()),
            (key(50), end()),
            "full page below the bound advances"
        );
        // Reaching or passing the captured high-water wraps.
        assert_eq!(
            advance_digest_poll(key(120), 50, end()),
            (None, None),
            "cursor at the high-water wraps"
        );
        assert_eq!(
            advance_digest_poll(key(150), 50, end()),
            (None, None),
            "cursor past the high-water wraps"
        );
        // A short page before the bound wraps: deliveries drained rows
        // mid-sweep, so the head is revisited without waiting for an empty
        // page.
        assert_eq!(
            advance_digest_poll(key(80), 30, end()),
            (None, None),
            "short page wraps"
        );
        // Empty pages always wrap, and a page without a bound re-snapshots
        // instead of chasing without one.
        assert_eq!(
            advance_digest_poll(None, 0, end()),
            (None, None),
            "empty page wraps"
        );
        assert_eq!(
            advance_digest_poll(key(50), 50, None),
            (None, None),
            "page without a bound wraps"
        );
    }

    #[test]
    fn digest_sweep_reaches_eligible_registration_within_one_cycle() {
        // 120 registrations pending at sweep start; 60 more arrive after the
        // first tick. The sweep must still wrap within the pages covering the
        // sweep set and page the eligible row (key 110, seated behind the
        // first cursor) inside that same cycle.
        let mut rows: Vec<(i64, String)> = (1..=120).map(|i| (i, format!("id-{i:05}"))).collect();
        let eligible = (110, "id-00110".to_owned());
        let mut cursor: Option<(i64, String)> = None;
        let mut end: Option<(i64, String)> = None;
        let mut paged_eligible = false;
        let mut wrap_tick = None;
        for tick in 0..10 {
            if cursor.is_none() {
                end = rows.last().cloned();
            }
            let page: Vec<(i64, String)> = rows
                .iter()
                .filter(|row| cursor.as_ref().is_none_or(|at| **row > *at))
                .take(DIGEST_POLL_PAGE)
                .cloned()
                .collect();
            if page.contains(&eligible) {
                paged_eligible = true;
            }
            if tick == 0 {
                rows.extend((121..=180).map(|i| (i, format!("id-{i:05}"))));
            }
            let last = page.last().cloned();
            (cursor, end) = advance_digest_poll(last, page.len(), end);
            if cursor.is_none() {
                wrap_tick = Some(tick);
                break;
            }
        }
        assert!(paged_eligible, "eligible row paged within the cycle");
        // The sweep set holds 120 rows at 50 per page: the cycle ends on the
        // third tick even though 60 more rows arrived mid-sweep.
        assert_eq!(wrap_tick, Some(2), "sweep wraps within one bounded cycle");
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

    fn test_router() -> Router {
        use crate::bus::Bus;
        use std::path::PathBuf;
        use tokio::sync::Mutex as AsyncMutex;

        let store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        Router::new(
            PathBuf::from("synthetic-data-root"),
            "owner-credential".to_owned(),
            std::sync::Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        )
    }

    async fn spawn_ready_with_control(
        router: &Router,
        project: &str,
        parent: Option<String>,
        control: &str,
    ) -> String {
        use eliot_core::{NativeEvent, SourceBatch, SubmitCommand};

        let mut store = router.inner.store.lock().await;
        let now = now_ms();
        let launch_id = ulid::Ulid::new().to_string();
        let mut args = json!({
            "project": project,
            "workspace": "synthetic-workspace",
            "harness": "codex",
            "profile": "fixture",
            "profile_snapshot": {"control": control},
        });
        if let Some(parent_id) = parent {
            args["parent"] = json!(parent_id);
        }
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args,
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
        crate::test_support::install_fixture_sources(&mut store, &agent, &launch_id, 1, now);
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
                        data: json!({"command_id": launch_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("session ready");
        store
            .settle(&launch_id, eliot_core::CommandState::Applied, None, now)
            .expect("settle");
        drop(store);
        router
            .inner
            .plugin_host
            .mark_generation_controlled_for_test(&agent, 1);
        agent
    }

    async fn submit_worker_send(router: &Router, worker: &str) -> String {
        use eliot_core::SubmitCommand;

        let mut store = router.inner.store.lock().await;
        let now = now_ms();
        let send_id = ulid::Ulid::new().to_string();
        let mut send = SubmitCommand::new(send_id.clone(), "send");
        send.target = Some(worker.to_owned());
        send.expected_generation = Some(1);
        send.args = json!({"text": "compute"});
        store.submit("owner", send, now).expect("submit worker");
        store.dispatch(&send_id, now).expect("dispatch");
        send_id
    }

    async fn complete_worker_turn(
        router: &Router,
        worker: &str,
        send_id: &str,
        turn_id: &str,
        final_text: &str,
    ) {
        use eliot_core::{NativeEvent, SourceBatch};

        let mut store = router.inner.store.lock().await;
        let now = now_ms();
        store
            .put_batch(
                SourceBatch {
                    batch_id: ulid::Ulid::new().to_string(),
                    agent_id: worker.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![
                        NativeEvent {
                            ts: None,
                            kind: "turn.started".to_owned(),
                            native_id: ulid::Ulid::new().to_string(),
                            data: json!({"command_id": send_id, "turn_id": turn_id}),
                        },
                        NativeEvent {
                            ts: None,
                            kind: "turn.completed".to_owned(),
                            native_id: ulid::Ulid::new().to_string(),
                            data: json!({
                                "command_id": send_id,
                                "turn_id": turn_id,
                                "status": "completed",
                                "final_text": final_text,
                            }),
                        },
                    ],
                    cursor: None,
                },
                now,
            )
            .expect("complete worker");
    }

    #[tokio::test]
    async fn self_digest_wake_carries_answers_and_marks_read_on_turn_started() {
        use eliot_core::{NativeEvent, SourceBatch, api::MailInboxResponse};

        let router = test_router();
        let manager = spawn_ready_with_control(&router, "app", None, "tree").await;
        let worker = spawn_ready_with_control(&router, "app", Some(manager.clone()), "self").await;
        let worker_send_id = submit_worker_send(&router, &worker).await;
        complete_worker_turn(&router, &worker, &worker_send_id, "t-w1", "result is 4").await;

        let digest_id = ulid::Ulid::new().to_string();
        let manager_principal = Principal::Agent {
            id: manager.clone(),
            generation: 1,
        };
        let reply: SwarmDigestResponse = serde_json::from_value(
            router
                .swarm_digest(
                    &json!({
                        "id": digest_id,
                        "to": manager,
                        "mode": "all",
                        "commands": [worker_send_id],
                        "turns": []
                    }),
                    &manager_principal,
                )
                .await
                .expect("register and deliver digest"),
        )
        .expect("decode digest response");
        assert_eq!(reply.state, "delivered");

        // offer_notice inside mail_send_impl submitted the wake `send` directly
        // with the digest body (including agent, turn_id, final_text, truncated)
        // for the recipient's turn, while ordinary command.get readback redacts it.
        let open_notice_id = {
            let store = router.inner.store.lock().await;
            store
                .notice_state(&manager)
                .expect("notice state")
                .expect("notice claimed")
                .open_notice_command
                .expect("open notice command")
        };
        let notice_cmd = {
            let store = router.inner.store.lock().await;
            store
                .get_command(&open_notice_id)
                .expect("get command")
                .expect("notice command exists")
        };
        let wake_text = notice_cmd
            .args
            .get("text")
            .and_then(Value::as_str)
            .expect("wake text");
        assert!(wake_text.contains(&format!("agent {worker}, turn_id t-w1, truncated=false")));
        assert!(wake_text.contains("result is 4"));

        let readback: eliot_core::Command = serde_json::from_value(
            router
                .command_get(&json!({"id": open_notice_id}), &Principal::Owner)
                .await
                .expect("command.get"),
        )
        .expect("decode command");
        assert_eq!(
            readback.args.get("redacted").and_then(Value::as_bool),
            Some(true)
        );
        assert!(
            !readback
                .args
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or("")
                .contains("result is 4")
        );

        // Simulating plugin turn.started on the wake command marks the digest read.
        {
            let mut store = router.inner.store.lock().await;
            let now = now_ms();
            store
                .dispatch(&open_notice_id, now)
                .expect("dispatch notice");
            store
                .put_batch(
                    SourceBatch {
                        batch_id: ulid::Ulid::new().to_string(),
                        agent_id: manager.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "turn.started".to_owned(),
                            native_id: ulid::Ulid::new().to_string(),
                            data: json!({"command_id": open_notice_id, "turn_id": "t-mgr-wake"}),
                        }],
                        cursor: None,
                    },
                    now,
                )
                .expect("turn.started marks digest read");
        }

        let inbox: MailInboxResponse = serde_json::from_value(
            router
                .mail_inbox(&json!({}), &manager_principal)
                .await
                .expect("inbox"),
        )
        .expect("decode inbox");
        assert!(inbox.deliveries.is_empty());
    }

    #[tokio::test]
    async fn owner_digest_keeps_status_only_for_out_of_scope_workers() {
        use eliot_core::api::MailInboxResponse;

        let router = test_router();
        let recipient = spawn_ready_with_control(&router, "app", None, "tree").await;
        let in_scope =
            spawn_ready_with_control(&router, "app", Some(recipient.clone()), "self").await;
        let out_of_scope = spawn_ready_with_control(&router, "other", None, "self").await;

        let c_in = submit_worker_send(&router, &in_scope).await;
        let c_out = submit_worker_send(&router, &out_of_scope).await;
        complete_worker_turn(&router, &in_scope, &c_in, "t-in", "visible answer").await;
        complete_worker_turn(
            &router,
            &out_of_scope,
            &c_out,
            "t-out",
            "cross-scope secret",
        )
        .await;

        let digest_id = ulid::Ulid::new().to_string();
        let reply: SwarmDigestResponse = serde_json::from_value(
            router
                .swarm_digest(
                    &json!({
                        "id": digest_id,
                        "to": recipient,
                        "mode": "all",
                        "commands": [c_in, c_out],
                        "turns": []
                    }),
                    &Principal::Owner,
                )
                .await
                .expect("owner digest"),
        )
        .expect("decode digest");
        assert_eq!(reply.state, "delivered");
        assert_eq!(reply.results[0].status, "succeeded");
        assert_eq!(
            reply.results[0].final_text.as_deref(),
            Some("visible answer")
        );
        assert_eq!(reply.results[1].status, "succeeded");
        assert_eq!(reply.results[1].agent, None);
        assert_eq!(reply.results[1].turn_id, None);
        assert_eq!(reply.results[1].final_text, None);
        assert!(!reply.results[1].truncated);

        let inbox: MailInboxResponse = serde_json::from_value(
            router
                .mail_inbox(
                    &json!({}),
                    &Principal::Agent {
                        id: recipient.clone(),
                        generation: 1,
                    },
                )
                .await
                .expect("inbox"),
        )
        .expect("decode inbox");
        assert_eq!(inbox.deliveries.len(), 1);
        let body = &inbox.deliveries[0].text;
        assert!(body.contains("visible answer"));
        assert!(body.contains(&format!("- command {c_out}: succeeded\n")));
        assert!(!body.contains("cross-scope secret"));
        assert!(!body.contains(&out_of_scope));
        assert!(!body.contains("t-out"));
    }

    #[tokio::test]
    async fn reparented_worker_before_completion_delivers_status_only() {
        use eliot_core::api::MailInboxResponse;

        let router = test_router();
        let manager = spawn_ready_with_control(&router, "app", None, "tree").await;
        let new_parent = spawn_ready_with_control(&router, "app", None, "tree").await;
        let stayed = spawn_ready_with_control(&router, "app", Some(manager.clone()), "self").await;
        let moved = spawn_ready_with_control(&router, "app", Some(manager.clone()), "self").await;

        let c_stayed = submit_worker_send(&router, &stayed).await;
        let c_moved = submit_worker_send(&router, &moved).await;
        complete_worker_turn(&router, &stayed, &c_stayed, "t-stay", "stayed answer").await;

        let digest_id = ulid::Ulid::new().to_string();
        let manager_principal = Principal::Agent {
            id: manager.clone(),
            generation: 1,
        };
        let pending: SwarmDigestResponse = serde_json::from_value(
            router
                .swarm_digest(
                    &json!({
                        "id": digest_id,
                        "to": manager,
                        "mode": "all",
                        "commands": [c_stayed, c_moved],
                        "turns": []
                    }),
                    &manager_principal,
                )
                .await
                .expect("register pending digest"),
        )
        .expect("decode pending");
        assert_eq!(pending.state, "pending");

        // Owner reparents `moved` away before completion.
        router
            .swarm_reparent(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "agent": moved,
                    "expected_generation": 1,
                    "parent": new_parent,
                    "parent_generation": 1,
                }),
                &Principal::Owner,
            )
            .await
            .expect("reparent away");

        // Before `moved` finishes, `deliver_digest` still sees `still_running`.
        assert!(
            router
                .deliver_digest(&digest_id)
                .await
                .expect("poll")
                .is_none()
        );

        complete_worker_turn(&router, &moved, &c_moved, "t-moved", "moved secret answer").await;
        let delivered = router
            .deliver_digest(&digest_id)
            .await
            .expect("deliver")
            .expect("digest completed");
        assert_eq!(
            delivered.results[0].final_text.as_deref(),
            Some("stayed answer")
        );
        assert_eq!(delivered.results[1].status, "succeeded");
        assert_eq!(delivered.results[1].agent, None);
        assert_eq!(delivered.results[1].turn_id, None);
        assert_eq!(delivered.results[1].final_text, None);

        // Mark wake not read yet since we didn't emit turn.started; read inbox.
        let inbox: MailInboxResponse = serde_json::from_value(
            router
                .mail_inbox(&json!({}), &manager_principal)
                .await
                .expect("inbox"),
        )
        .expect("decode inbox");
        assert_eq!(inbox.deliveries.len(), 1);
        let body = &inbox.deliveries[0].text;
        assert!(body.contains("stayed answer"));
        assert!(body.contains(&format!("- command {c_moved}: succeeded\n")));
        assert!(!body.contains("moved secret answer"));
        assert!(!body.contains("t-moved"));
    }

    #[tokio::test]
    async fn reparented_recipient_wake_command_redacts_excerpts_from_new_parent() {
        let router = test_router();
        let manager = spawn_ready_with_control(&router, "app", None, "tree").await;
        let worker = spawn_ready_with_control(&router, "app", Some(manager.clone()), "self").await;
        let foreign_parent = spawn_ready_with_control(&router, "app", None, "tree").await;

        let c_worker = submit_worker_send(&router, &worker).await;
        complete_worker_turn(&router, &worker, &c_worker, "t-w", "private worker answer").await;

        let manager_principal = Principal::Agent {
            id: manager.clone(),
            generation: 1,
        };
        router
            .swarm_digest(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "to": manager,
                    "mode": "all",
                    "commands": [c_worker],
                    "turns": []
                }),
                &manager_principal,
            )
            .await
            .expect("digest");

        let open_notice_id = {
            let store = router.inner.store.lock().await;
            store
                .notice_state(&manager)
                .expect("notice state")
                .expect("notice claimed")
                .open_notice_command
                .expect("open notice")
        };

        // Reparent the recipient under `foreign_parent` (`worker` moves too, or
        // suppose `foreign_parent` is a separate manager who now owns `manager`).
        router
            .swarm_reparent(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "agent": manager,
                    "expected_generation": 1,
                    "parent": foreign_parent,
                    "parent_generation": 1,
                }),
                &Principal::Owner,
            )
            .await
            .expect("reparent recipient");

        let readback: eliot_core::Command = serde_json::from_value(
            router
                .command_get(
                    &json!({"id": open_notice_id}),
                    &Principal::Agent {
                        id: foreign_parent,
                        generation: 1,
                    },
                )
                .await
                .expect("foreign parent reads wake command"),
        )
        .expect("decode command");
        let text = readback
            .args
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("");
        assert!(!text.contains("private worker answer"));
        assert_eq!(
            readback.args.get("redacted").and_then(Value::as_bool),
            Some(true)
        );
    }

    #[tokio::test]
    async fn digest_shares_budget_fairly_so_fifth_short_answer_never_disappears() {
        use eliot_core::api::{MAX_WAIT_FINAL_TEXT_BYTES, MailInboxResponse};

        let router = test_router();
        let manager = spawn_ready_with_control(&router, "app", None, "tree").await;
        let big = "a".repeat(MAX_WAIT_FINAL_TEXT_BYTES);
        let mut cmds = Vec::new();
        for idx in 0..4 {
            let w = spawn_ready_with_control(&router, "app", Some(manager.clone()), "self").await;
            let c = submit_worker_send(&router, &w).await;
            complete_worker_turn(&router, &w, &c, &format!("t-{idx}"), &big).await;
            cmds.push(c);
        }
        let w5 = spawn_ready_with_control(&router, "app", Some(manager.clone()), "self").await;
        let c5 = submit_worker_send(&router, &w5).await;
        complete_worker_turn(&router, &w5, &c5, "t-5", "fifth short answer").await;
        cmds.push(c5);

        let manager_principal = Principal::Agent {
            id: manager.clone(),
            generation: 1,
        };
        let reply: SwarmDigestResponse = serde_json::from_value(
            router
                .swarm_digest(
                    &json!({
                        "id": ulid::Ulid::new().to_string(),
                        "to": manager,
                        "mode": "all",
                        "commands": cmds,
                        "turns": []
                    }),
                    &manager_principal,
                )
                .await
                .expect("digest"),
        )
        .expect("decode");
        assert_eq!(
            reply.results[4].final_text.as_deref(),
            Some("fifth short answer")
        );
        assert!(!reply.results[4].truncated);

        let inbox: MailInboxResponse = serde_json::from_value(
            router
                .mail_inbox(&json!({}), &manager_principal)
                .await
                .expect("inbox"),
        )
        .expect("decode inbox");
        assert_eq!(inbox.deliveries.len(), 1);
        assert!(inbox.deliveries[0].text.contains("fifth short answer"));
    }

    #[tokio::test]
    async fn digest_delivers_when_worker_answer_contains_nul_bytes() {
        use eliot_core::api::MailInboxResponse;

        let router = test_router();
        let manager = spawn_ready_with_control(&router, "app", None, "tree").await;
        let worker = spawn_ready_with_control(&router, "app", Some(manager.clone()), "self").await;
        let c = submit_worker_send(&router, &worker).await;
        complete_worker_turn(&router, &worker, &c, "t-nul", "alpha\0omega").await;

        let manager_principal = Principal::Agent {
            id: manager.clone(),
            generation: 1,
        };
        let reply: SwarmDigestResponse = serde_json::from_value(
            router
                .swarm_digest(
                    &json!({
                        "id": ulid::Ulid::new().to_string(),
                        "to": manager,
                        "mode": "all",
                        "commands": [c],
                        "turns": []
                    }),
                    &manager_principal,
                )
                .await
                .expect("digest with NUL delivers instead of blocking"),
        )
        .expect("decode");
        assert_eq!(reply.state, "delivered");

        let inbox: MailInboxResponse = serde_json::from_value(
            router
                .mail_inbox(&json!({}), &manager_principal)
                .await
                .expect("inbox"),
        )
        .expect("decode inbox");
        assert_eq!(inbox.deliveries.len(), 1);
        assert!(!inbox.deliveries[0].text.contains('\0'));
        assert!(inbox.deliveries[0].text.contains("alpha\\0omega"));
    }
}
