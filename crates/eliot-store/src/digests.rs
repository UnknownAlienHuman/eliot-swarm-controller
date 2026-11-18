//! Durable completion digests: one coalesced parent mailbox result per
//! captured child set.
//!
//! A digest registration freezes a parent recipient plus a command/turn set.
//! Delivery reuses ordinary queued mail with the registration id as the mail
//! id, so a reconnect or restart replays the same mail id and the existing
//! mail stable effect identity makes the retry a no-op. The digest row itself
//! carries the same idempotency: same id plus same payload replays the
//! retained row, same id plus a different payload rejects with `id_conflict`.

use super::{
    Store, canonical, command_binding, commit_transaction, encode_json, load_agent_by_num,
    sql_error, validate_id_age, write_binding,
};
use eliot_core::{Error, Result, api::SwarmDigestRequest, parse_agent_id};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// One stored digest registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigestRow {
    pub id: String,
    pub parent: String,
    pub mode: String,
    pub commands: Vec<String>,
    pub turns: Vec<DigestTurn>,
    pub requester: String,
    pub created_at: i64,
    pub delivered_mail_id: Option<String>,
    pub delivered_at: Option<i64>,
    pub payload_digest: Vec<u8>,
    /// Per-entry visibility at registration: true for entries that read
    /// `not_visible` for the registering principal. Delivery recomputes as
    /// owner and masks these back to `not_visible` so a digest never widens
    /// read scope.
    pub hidden: Vec<bool>,
}

/// Frozen turn binding inside a digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestTurn {
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
}

impl DigestTurn {
    pub fn canonical_id(&self) -> String {
        format!("{}/{}/{}", self.agent, self.generation, self.turn_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DigestRegisterOutcome {
    Fresh,
    Retained,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigestRegistered {
    pub row: DigestRow,
    pub outcome: DigestRegisterOutcome,
}

fn digest_payload(request: &SwarmDigestRequest) -> Result<Vec<u8>> {
    canonical::digest(&json!({
        "to": request.to,
        "mode": request.mode,
        "commands": request.commands,
        "turns": request.turns,
    }))
}

struct StoredDigestRow {
    id: String,
    parent: i64,
    mode: String,
    commands_json: String,
    turns_json: String,
    requester: String,
    created_at: i64,
    delivered_mail_id: Option<String>,
    delivered_at: Option<i64>,
    payload_digest: Vec<u8>,
    hidden_json: String,
}

fn parse_row(stored: StoredDigestRow) -> Result<DigestRow> {
    let commands: Vec<String> = serde_json::from_str(&stored.commands_json)
        .map_err(|e| Error::new("internal", e.to_string()))?;
    let turns: Vec<DigestTurn> = serde_json::from_str(&stored.turns_json)
        .map_err(|e| Error::new("internal", e.to_string()))?;
    let hidden: Vec<bool> = serde_json::from_str(&stored.hidden_json)
        .map_err(|e| Error::new("internal", e.to_string()))?;
    Ok(DigestRow {
        id: stored.id,
        parent: format!("a{}", stored.parent),
        mode: stored.mode,
        commands,
        turns,
        requester: stored.requester,
        created_at: stored.created_at,
        delivered_mail_id: stored.delivered_mail_id,
        delivered_at: stored.delivered_at,
        payload_digest: stored.payload_digest,
        hidden,
    })
}

impl Store {
    /// Register a digest. Same id plus same payload replays the retained row;
    /// same id plus a different payload (or principal) rejects `id_conflict`.
    /// The registration id doubles as the later digest mail id, so it must
    /// not collide with an existing command or write binding.
    pub fn digest_register(
        &mut self,
        principal: &str,
        request: &SwarmDigestRequest,
        hidden: &[bool],
        now: i64,
    ) -> Result<DigestRegistered> {
        self.ensure_committed_reads()?;
        request.validate()?;
        if hidden.len() != request.total() {
            return Err(Error::new("internal", "digest visibility mask is invalid"));
        }
        let payload_digest = digest_payload(request)?;
        if let Some(existing) = self.digest_row(&request.id)? {
            if existing.payload_digest != payload_digest || existing.requester != principal {
                return Err(Error::new(
                    "id_conflict",
                    "digest id is bound to another principal or payload",
                ));
            }
            // Same registration replays; hidden mask stays as first admitted.
            return Ok(DigestRegistered {
                row: existing,
                outcome: DigestRegisterOutcome::Retained,
            });
        }
        if command_binding(&self.connection, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "digest id is already used by a command",
            ));
        }
        if write_binding(&self.connection, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "digest id is already used by a retained write",
            ));
        }
        validate_id_age(&request.id, now)?;
        let parent_num = parse_agent_id(&request.to)?;
        let parent = load_agent_by_num(&self.connection, parent_num)?.ok_or_else(|| {
            Error::new("not_found", format!("Agent {} was not found", request.to))
        })?;
        let _ = parent;
        self.ensure_writer_healthy()?;
        let result = self.digest_insert(principal, request, hidden, &payload_digest, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn digest_insert(
        &mut self,
        principal: &str,
        request: &SwarmDigestRequest,
        hidden: &[bool],
        payload_digest: &[u8],
        now: i64,
    ) -> Result<DigestRegistered> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if command_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "digest id is already used by a command",
            ));
        }
        if write_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "digest id is already used by a retained write",
            ));
        }
        let parent_num = parse_agent_id(&request.to)?;
        if load_agent_by_num(&tx, parent_num)?.is_none() {
            return Err(Error::new(
                "not_found",
                format!("Agent {} was not found", request.to),
            ));
        }
        let turns: Vec<DigestTurn> = request
            .turns
            .iter()
            .map(|t| DigestTurn {
                agent: t.agent.clone(),
                generation: t.generation,
                turn_id: t.turn_id.clone(),
            })
            .collect();
        let commands_json =
            encode_json(&request.commands).map_err(|e| Error::new("internal", e.to_string()))?;
        let turns_json = encode_json(&turns).map_err(|e| Error::new("internal", e.to_string()))?;
        let hidden_json =
            encode_json(&hidden).map_err(|e| Error::new("internal", e.to_string()))?;
        db!(tx.execute(
            "INSERT INTO completion_digests(id, parent, mode, commands, turns, requester, created_at, delivered_mail_id, delivered_at, payload_digest, hidden)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, ?8, ?9)",
            params![
                request.id,
                parent_num,
                request.mode,
                commands_json,
                turns_json,
                principal,
                now,
                payload_digest,
                hidden_json,
            ],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        let row = self
            .digest_row(&request.id)?
            .ok_or_else(|| Error::new("internal", "digest registration disappeared"))?;
        Ok(DigestRegistered {
            row,
            outcome: DigestRegisterOutcome::Fresh,
        })
    }

    fn digest_row(&self, id: &str) -> Result<Option<DigestRow>> {
        self.ensure_committed_reads()?;
        let row: Option<StoredDigestRow> = self
            .connection
            .query_row(
                "SELECT id, parent, mode, commands, turns, requester, created_at, delivered_mail_id, delivered_at, payload_digest, hidden
                 FROM completion_digests WHERE id = ?1",
                params![id],
                |row| {
                    Ok(StoredDigestRow {
                        id: row.get(0)?,
                        parent: row.get(1)?,
                        mode: row.get(2)?,
                        commands_json: row.get(3)?,
                        turns_json: row.get(4)?,
                        requester: row.get(5)?,
                        created_at: row.get(6)?,
                        delivered_mail_id: row.get(7)?,
                        delivered_at: row.get(8)?,
                        payload_digest: row.get(9)?,
                        hidden_json: row.get(10)?,
                    })
                },
            )
            .optional()
            .map_err(sql_error)?;
        row.map(parse_row).transpose()
    }

    /// One stored digest registration, or `None` when absent.
    pub fn get_completion_digest(&self, id: &str) -> Result<Option<DigestRow>> {
        self.digest_row(id)
    }

    /// Frozen parent recipient of a still-pending digest registration, or
    /// `None` when the id is not a digest or its first delivery already
    /// committed. The digest driver proves the exact registered conversion
    /// with this: a pending registration plus the frozen recipient exempt the
    /// delivery mail from the new-write age gate (its age is the
    /// registration's age, not a new write's). Delivered or absent ids keep
    /// the ordinary write-id rules.
    pub(super) fn pending_digest_recipient(
        tx: &Transaction<'_>,
        id: &str,
    ) -> Result<Option<String>> {
        let row: Option<(i64, Option<i64>)> = tx
            .query_row(
                "SELECT parent, delivered_at FROM completion_digests WHERE id = ?1",
                params![id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        match row {
            Some((parent, None)) => Ok(Some(format!("a{parent}"))),
            _ => Ok(None),
        }
    }

    /// Bounded pending registrations, oldest first.
    pub fn pending_completion_digests(&self, limit: usize) -> Result<Vec<DigestRow>> {
        self.pending_completion_digests_after(None, limit)
    }

    /// Bounded pending registrations after a keyset cursor, oldest first.
    /// `after` is the last examined `(created_at, id)`; `None` starts at the
    /// oldest row. The driver advances the cursor past every examined row and
    /// wraps to `None` at the end, so pending or mailbox-blocked rows at the
    /// front never trap later eligible registrations behind one fixed page.
    /// High-water of the pending set: the newest `(created_at, id)` still
    /// awaiting delivery, or `None` when nothing is pending. The driver
    /// snapshots this at sweep start so the sweep wraps when the cursor
    /// passes the end of the eligible set observed at that tick, instead of
    /// chasing newly arriving registrations forever.
    pub fn pending_completion_digests_max(&self) -> Result<Option<(i64, String)>> {
        self.ensure_committed_reads()?;
        let row: Option<(i64, String)> = self
            .connection
            .query_row(
                "SELECT created_at, id FROM completion_digests INDEXED BY completion_digests_pending_cursor
                 WHERE delivered_at IS NULL
                 ORDER BY created_at DESC, id DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(row)
    }

    pub fn pending_completion_digests_after(
        &self,
        after: Option<(i64, &str)>,
        limit: usize,
    ) -> Result<Vec<DigestRow>> {
        self.ensure_committed_reads()?;
        let limit = limit.clamp(1, 256) as i64;
        let mut sql = String::from(
            "SELECT id, parent, mode, commands, turns, requester, created_at, delivered_mail_id, delivered_at, payload_digest, hidden
             FROM completion_digests INDEXED BY completion_digests_pending_cursor WHERE delivered_at IS NULL",
        );
        let mut params: Vec<rusqlite::types::Value> = Vec::new();
        if let Some((created_at, id)) = after {
            sql.push_str(" AND (created_at > ?1 OR (created_at = ?2 AND id > ?3))");
            params.push(rusqlite::types::Value::from(created_at));
            params.push(rusqlite::types::Value::from(created_at));
            params.push(rusqlite::types::Value::from(id.to_owned()));
            sql.push_str(" ORDER BY created_at ASC, id ASC LIMIT ?4");
        } else {
            sql.push_str(" ORDER BY created_at ASC, id ASC LIMIT ?1");
        }
        params.push(rusqlite::types::Value::from(limit));
        let mut stmt = self.connection.prepare(&sql).map_err(sql_error)?;
        let rows = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                Ok(StoredDigestRow {
                    id: row.get(0)?,
                    parent: row.get(1)?,
                    mode: row.get(2)?,
                    commands_json: row.get(3)?,
                    turns_json: row.get(4)?,
                    requester: row.get(5)?,
                    created_at: row.get(6)?,
                    delivered_mail_id: row.get(7)?,
                    delivered_at: row.get(8)?,
                    payload_digest: row.get(9)?,
                    hidden_json: row.get(10)?,
                })
            })
            .map_err(sql_error)?;
        let mut out = Vec::new();
        for row in rows {
            let stored = row.map_err(sql_error)?;
            out.push(parse_row(stored)?);
        }
        Ok(out)
    }

    /// Mark a pending digest delivered inside the caller's transaction.
    /// No-op when it was already delivered (restart/retry safe).
    pub(crate) fn mark_digest_delivered_in_tx(
        tx: &Transaction<'_>,
        id: &str,
        mail_delivery_id: &str,
        now: i64,
    ) -> Result<()> {
        db!(tx.execute(
            "UPDATE completion_digests SET delivered_mail_id = ?1, delivered_at = ?2
             WHERE id = ?3 AND delivered_at IS NULL",
            params![mail_delivery_id, now, id],
        ));
        Ok(())
    }

    /// Mark a pending digest delivered. Returns false when it was already
    /// delivered (restart/retry safe).
    pub fn mark_digest_delivered(
        &mut self,
        id: &str,
        mail_delivery_id: &str,
        now: i64,
    ) -> Result<bool> {
        self.ensure_committed_reads()?;
        self.ensure_writer_healthy()?;
        let result = (|| -> Result<bool> {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let changed = db!(tx.execute(
                "UPDATE completion_digests SET delivered_mail_id = ?1, delivered_at = ?2
                 WHERE id = ?3 AND delivered_at IS NULL",
                params![mail_delivery_id, now, id],
            ));
            commit_transaction(tx, &self.write_faulted)?;
            Ok(changed == 1)
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::{MailLimits, MailSendRequest, WaitTurnRef};
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    fn open_store() -> Store {
        Store::open(std::path::Path::new(":memory:")).expect("open memory store")
    }

    fn ready_agent(store: &mut Store, project: &str, now: i64) -> String {
        use eliot_core::{CommandState, NativeEvent, SourceBatch, SubmitCommand};
        let launch_id = ulid::Ulid::new().to_string();
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":project, "workspace":"synthetic-workspace", "harness":"codex", "profile":"fake", "profile_snapshot":{}}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch");
        let agent = admission.command.target.expect("launch target");
        store.dispatch(&launch_id, now).expect("dispatch");
        crate::tests::install_fixture_sources(store, &agent, &launch_id, 1, now);
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
            .settle(&launch_id, CommandState::Applied, None, now)
            .expect("settle");
        agent
    }

    fn digest_request(id: &str, to: &str) -> SwarmDigestRequest {
        SwarmDigestRequest::new(
            id.to_owned(),
            to.to_owned(),
            "all".to_owned(),
            vec!["c-missing".to_owned()],
            vec![],
        )
    }

    #[test]
    fn digest_register_replays_same_id_and_rejects_rebinding() {
        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        let id = ulid::Ulid::new().to_string();
        let req = digest_request(&id, &parent);
        let first = store
            .digest_register("owner", &req, &[false], now)
            .expect("register");
        assert_eq!(first.outcome, DigestRegisterOutcome::Fresh);
        let second = store
            .digest_register("owner", &req, &[false], now)
            .expect("replay");
        assert_eq!(second.outcome, DigestRegisterOutcome::Retained);
        assert_eq!(first.row, second.row);
        let mut rebound = req.clone();
        rebound.mode = "any".to_owned();
        let err = store
            .digest_register("owner", &rebound, &[false], now)
            .unwrap_err();
        assert_eq!(err.code, "id_conflict");
        let err = store
            .digest_register("agent:a9:1", &req, &[false], now)
            .unwrap_err();
        assert_eq!(err.code, "id_conflict");
    }

    #[test]
    fn mail_send_rejects_digest_bound_id() {
        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        let id = ulid::Ulid::new().to_string();
        let req = digest_request(&id, &parent);
        store
            .digest_register("owner", &req, &[false], now)
            .expect("register");
        // A public mail send must not steal the digest registration id:
        // digest delivery would later fail `id_conflict` permanently.
        let mail = MailSendRequest {
            id: id.clone(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "foreign mail reusing a digest id".to_owned(),
            thread: None,
            reply_to_delivery: None,
        };
        let err = store
            .mail_send("owner", &mail, MailLimits::default(), now)
            .unwrap_err();
        assert_eq!(err.code, "id_conflict");
        // The digest delivery path still delivers under the same id.
        let delivered = store
            .mail_send_digest_delivery("owner", &mail, MailLimits::default(), now)
            .expect("digest delivery");
        assert_eq!(delivered.outcome, crate::MailSendOutcome::Fresh);
    }

    #[test]
    fn submit_and_record_write_reject_digest_bound_id() {
        use eliot_core::SubmitCommand;

        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        let id = ulid::Ulid::new().to_string();
        let req = digest_request(&id, &parent);
        store
            .digest_register("owner", &req, &[false], now)
            .expect("register");
        // An ordinary command under a pending digest id would poison the
        // later delivery with `command_binding`: the shared namespace
        // reservation rejects it symmetrically to mail/handoff/switch.
        let mut launch = SubmitCommand::new(id.clone(), "launch");
        launch.expected_generation = Some(0);
        launch.args = json!({"project": "p", "workspace": "synthetic-workspace",
            "harness": "codex", "profile": "fake", "profile_snapshot": {}});
        let err = store.submit("owner", launch, now).unwrap_err();
        assert_eq!(err.code, "id_conflict");
        // A generic write under the same id would poison delivery with a
        // write binding instead; it rejects the same way.
        let err = store
            .record_write(
                &id,
                "owner",
                "probe.write",
                &json!({"probe": true}),
                &json!({"ok": true}),
                now,
            )
            .unwrap_err();
        assert_eq!(err.code, "id_conflict");
        // The digest registration itself survives both rejections and still
        // delivers through the exact registered conversion.
        let mail = MailSendRequest {
            id: id.clone(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "digest body".to_owned(),
            thread: None,
            reply_to_delivery: None,
        };
        store
            .mail_send_digest_delivery("owner", &mail, MailLimits::default(), now)
            .expect("digest delivery");
    }

    #[test]
    fn digest_restart_keeps_single_mail_identity() {
        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        let id = ulid::Ulid::new().to_string();
        let req = digest_request(&id, &parent);
        store
            .digest_register("owner", &req, &[false], now)
            .expect("register");
        // First delivery uses the registration id as the mail id.
        let mail = MailSendRequest {
            id: id.clone(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "digest body".to_owned(),
            thread: Some("completion-test".to_owned()),
            reply_to_delivery: None,
        };
        let first = store
            .mail_send_digest_delivery("owner", &mail, MailLimits::default(), now)
            .expect("first mail");
        assert_eq!(first.outcome, crate::MailSendOutcome::Fresh);
        store
            .mark_digest_delivered(&id, &first.value.deliveries[0].delivery_id, now)
            .expect("mark");
        // Restart replay: same digest id replays retained, same mail id
        // replays retained without a second delivery row.
        let replay = store
            .digest_register("owner", &req, &[false], now + 1)
            .expect("replay after restart");
        assert_eq!(replay.outcome, DigestRegisterOutcome::Retained);
        assert!(replay.row.delivered_at.is_some());
        let second = store
            .mail_send_digest_delivery("owner", &mail, MailLimits::default(), now + 1)
            .expect("second mail");
        assert_eq!(second.outcome, crate::MailSendOutcome::Retained);
        assert_eq!(first.value, second.value);
    }

    #[test]
    fn digest_delivery_survives_the_write_id_age_window() {
        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        let other = ready_agent(&mut store, "q", now);
        let id = ulid::Ulid::new().to_string();
        let req = digest_request(&id, &parent);
        store
            .digest_register("owner", &req, &[false], now)
            .expect("register");
        // Past the 24-hour new-write window the pending digest is still an
        // authorized conversion of its durable registration, not a new
        // write: the first delivery succeeds under the registration id.
        let late = now + 25 * 60 * 60 * 1000;
        let mail = MailSendRequest {
            id: id.clone(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "digest body".to_owned(),
            thread: Some(eliot_core::api::digest_thread(&id)),
            reply_to_delivery: None,
        };
        let delivered = store
            .mail_send_digest_delivery("owner", &mail, MailLimits::default(), late)
            .expect("late digest delivery");
        assert_eq!(delivered.outcome, crate::MailSendOutcome::Fresh);
        // A digest-path mail to anyone but the frozen parent recipient is
        // not the registered conversion: it rejects with id_conflict before
        // binding anything, so the mismatched request can never consume the
        // pending id (even past the age window, where the reservation still
        // takes precedence over the age rule).
        let other_id = ulid::Ulid::new().to_string();
        let other_req = digest_request(&other_id, &parent);
        store
            .digest_register("owner", &other_req, &[false], now)
            .expect("register");
        let diverted = MailSendRequest {
            id: other_id.clone(),
            to: vec![other.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "diverted digest body".to_owned(),
            thread: Some(eliot_core::api::digest_thread(&other_id)),
            reply_to_delivery: None,
        };
        let err = store
            .mail_send_digest_delivery("owner", &diverted, MailLimits::default(), late)
            .unwrap_err();
        assert_eq!(err.code, "id_conflict");
        // The diverted late request never consumed the registration: the
        // exact conversion still delivers afterwards under the frozen parent.
        let retry = MailSendRequest {
            id: other_id.clone(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "digest body".to_owned(),
            thread: Some(eliot_core::api::digest_thread(&other_id)),
            reply_to_delivery: None,
        };
        store
            .mail_send_digest_delivery("owner", &retry, MailLimits::default(), late)
            .expect("exact conversion still delivers after diverted attempt");
    }

    #[test]
    fn digest_id_reservation_covers_ack_and_var_writes() {
        use eliot_core::api::{MailAckRequest, VarSetRequest};
        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        let id = ulid::Ulid::new().to_string();
        let req = digest_request(&id, &parent);
        store
            .digest_register("owner", &req, &[false], now)
            .expect("register");
        // mail.ack under a pending digest id must respect the reservation:
        // it rejects with id_conflict instead of binding the id and
        // poisoning the later delivery. Seed one real delivery so the ack
        // request itself validates; the digest reservation still wins.
        let seed_mail = MailSendRequest {
            id: ulid::Ulid::new().to_string(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "seed".to_owned(),
            thread: None,
            reply_to_delivery: None,
        };
        let seed = store
            .mail_send("owner", &seed_mail, MailLimits::default(), now)
            .expect("seed mail");
        let ack = MailAckRequest {
            id: id.clone(),
            agent: Some(parent.clone()),
            deliveries: vec![seed.value.deliveries[0].delivery_id.clone()],
        };
        let err = store.mail_ack("owner", &ack, now).unwrap_err();
        assert_eq!(err.code, "id_conflict");
        // var.set under the same id rejects the same way (agent principal,
        // own project scope, fresh CAS).
        let agent_principal = format!("agent:{parent}:1");
        let var = VarSetRequest {
            id: id.clone(),
            scope: None,
            key: "k".to_owned(),
            value: serde_json::json!("v"),
            expect_version: Some(0),
            note: None,
        };
        let err = store.var_set(&agent_principal, &var, now).unwrap_err();
        assert_eq!(err.code, "id_conflict");
        // The registration still delivers through the exact conversion.
        let mail = MailSendRequest {
            id: id.clone(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "digest body".to_owned(),
            thread: Some(eliot_core::api::digest_thread(&id)),
            reply_to_delivery: None,
        };
        store
            .mail_send_digest_delivery("owner", &mail, MailLimits::default(), now)
            .expect("digest delivery after ack/var rejections");
    }

    #[test]
    fn digest_diverted_request_never_consumes_pending_id() {
        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        let other = ready_agent(&mut store, "q", now);
        let id = ulid::Ulid::new().to_string();
        let req = digest_request(&id, &parent);
        store
            .digest_register("owner", &req, &[false], now)
            .expect("register");
        // A fresh mismatched request through the digest path (wrong
        // recipient, still inside the write-id window) must reject with
        // id_conflict before binding anything.
        let diverted = MailSendRequest {
            id: id.clone(),
            to: vec![other.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "diverted body".to_owned(),
            thread: Some(eliot_core::api::digest_thread(&id)),
            reply_to_delivery: None,
        };
        let err = store
            .mail_send_digest_delivery("owner", &diverted, MailLimits::default(), now + 1)
            .unwrap_err();
        assert_eq!(err.code, "id_conflict");
        // The exact conversion still delivers afterwards.
        let mail = MailSendRequest {
            id: id.clone(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "digest body".to_owned(),
            thread: Some(eliot_core::api::digest_thread(&id)),
            reply_to_delivery: None,
        };
        let delivered = store
            .mail_send_digest_delivery("owner", &mail, MailLimits::default(), now + 2)
            .expect("exact delivery after diverted attempt");
        assert_eq!(delivered.outcome, crate::MailSendOutcome::Fresh);
        // Mail and delivered mark commit atomically: the registration reads
        // delivered immediately without a separate mark step.
        let row = store
            .get_completion_digest(&id)
            .expect("read digest")
            .expect("digest present");
        assert!(row.delivered_at.is_some());
    }

    #[test]
    fn digest_delivered_id_never_admits_changed_body() {
        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        let id = ulid::Ulid::new().to_string();
        let req = digest_request(&id, &parent);
        store
            .digest_register("owner", &req, &[false], now)
            .expect("register");
        let mail = MailSendRequest {
            id: id.clone(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "first body".to_owned(),
            thread: Some(eliot_core::api::digest_thread(&id)),
            reply_to_delivery: None,
        };
        let first = store
            .mail_send_digest_delivery("owner", &mail, MailLimits::default(), now)
            .expect("first delivery");
        assert_eq!(first.outcome, crate::MailSendOutcome::Fresh);
        // Mail and delivered mark commit atomically, so a restart sees the
        // registration as delivered and never recomputes a changed body
        // through the Store: an exact same-body replay returns retained,
        // while a changed body under the consumed id conflicts instead of
        // overwriting the committed mail.
        let same = store
            .mail_send_digest_delivery("owner", &mail, MailLimits::default(), now + 1)
            .expect("same-body replay returns retained");
        assert_eq!(same.outcome, crate::MailSendOutcome::Retained);
        assert_eq!(same.value, first.value);
        let recomputed = MailSendRequest {
            id: id.clone(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "changed body after restart".to_owned(),
            thread: Some(eliot_core::api::digest_thread(&id)),
            reply_to_delivery: None,
        };
        let err = store
            .mail_send_digest_delivery("owner", &recomputed, MailLimits::default(), now + 2)
            .unwrap_err();
        assert_eq!(err.code, "id_conflict");
    }

    #[test]
    fn pending_digests_page_past_a_blocked_head_with_wraparound() {
        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        // 51 pending registrations: a fixed oldest-50 page would revisit the
        // same head forever and never examine the 51st.
        let mut ids = Vec::new();
        for index in 0..51 {
            let id = ulid::Ulid::new().to_string();
            let req = digest_request(&id, &parent);
            store
                .digest_register("owner", &req, &[false], now + index)
                .expect("register");
            ids.push(id);
        }
        let head = store
            .pending_completion_digests_after(None, 50)
            .expect("first page");
        assert_eq!(head.len(), 50);
        let cursor = head.last().map(|row| (row.created_at, row.id.clone()));
        // The cursor advances past the blocked head: the next page reaches
        // the tail the fixed window could never examine.
        let (created_at, id) = cursor.expect("head cursor");
        let tail = store
            .pending_completion_digests_after(Some((created_at, id.as_str())), 50)
            .expect("second page");
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].id, ids[50]);
        // Past the newest row the page is empty; wrapping restarts at the
        // oldest row so the sweep cycles instead of stalling.
        let drained = store
            .pending_completion_digests_after(Some((tail[0].created_at, tail[0].id.as_str())), 50)
            .expect("drained page");
        assert!(drained.is_empty());
        let wrapped = store
            .pending_completion_digests_after(None, 50)
            .expect("wrapped page");
        assert_eq!(wrapped.len(), 50);
        assert_eq!(wrapped[0].id, ids[0]);
        // The legacy oldest-first entry point keeps its contract.
        let legacy = store.pending_completion_digests(50).expect("legacy page");
        assert_eq!(legacy.len(), 50);
        assert_eq!(legacy[0].id, ids[0]);
    }

    #[test]
    fn pending_digests_high_water_bounds_sweep_despite_new_arrivals() {
        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        // Snapshot the sweep high-water, then keep arriving: the sweep must
        // wrap once its cursor passes the observed end instead of chasing the
        // growing tail forever.
        let mut ids = Vec::new();
        for index in 0..3 {
            let id = ulid::Ulid::new().to_string();
            let req = digest_request(&id, &parent);
            store
                .digest_register("owner", &req, &[false], now + index)
                .expect("register");
            ids.push(id);
        }
        let end = store
            .pending_completion_digests_max()
            .expect("high-water")
            .expect("pending");
        let first = store
            .pending_completion_digests_after(None, 2)
            .expect("first page");
        assert_eq!(first.len(), 2);
        // New registrations arrive mid-sweep past the recorded end.
        for index in 3..6 {
            let id = ulid::Ulid::new().to_string();
            let req = digest_request(&id, &parent);
            store
                .digest_register("owner", &req, &[false], now + index)
                .expect("register");
        }
        // The continuation seeks past the cursor through the pending keyset
        // cursor (no full-partition scan): it reaches the row observed at
        // sweep start, not just the pending partition head.
        let cursor = first.last().map(|row| (row.created_at, row.id.clone()));
        let (created_at, id) = cursor.expect("cursor");
        let second = store
            .pending_completion_digests_after(Some((created_at, id.as_str())), 10)
            .expect("second page");
        assert!(second.iter().any(|row| row.id == end.1));
        // The high-water still names the sweep-start end, while the live max
        // has moved past it with the new arrivals.
        let live_max = store
            .pending_completion_digests_max()
            .expect("live max")
            .expect("pending");
        assert!(live_max >= end);
    }

    #[test]
    fn digest_mail_proves_an_ordinary_wake_not_advisory() {
        let mut store = open_store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "p", now);
        let id = ulid::Ulid::new().to_string();
        let req = digest_request(&id, &parent);
        store
            .digest_register("owner", &req, &[false], now)
            .expect("register");
        let text = eliot_core::api::digest_text(
            &id,
            &parent,
            "all",
            &[eliot_core::api::WaitEntryResult::new(
                "command",
                "c-missing",
                "not_found",
            )],
        );
        assert!(!text.contains("agent.attention"));
        assert!(!text.contains("attention-"));
        let thread = eliot_core::api::digest_thread(&id);
        assert!(thread.starts_with("completion-"));
        let mail = MailSendRequest {
            id: id.clone(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text,
            thread: Some(thread),
            reply_to_delivery: None,
        };
        store
            .mail_send_digest_delivery("owner", &mail, MailLimits::default(), now)
            .expect("digest mail");
        let claim = store
            .notice_claim(
                &parent,
                &ulid::Ulid::new().to_string(),
                crate::NoticePending::Mail,
                now,
            )
            .expect("claim")
            .expect("digest mail wakes");
        assert_eq!(claim.count, 1);
        // Advisory attention mail never wakes; digest mail must not share it.
        let advisory = MailSendRequest {
            id: ulid::Ulid::new().to_string(),
            to: vec![parent.clone()],
            mode: eliot_core::api::MailMode::Queue,
            text: "attention advisory".to_owned(),
            thread: Some("attention-abcdef0123456789-advisory".to_owned()),
            reply_to_delivery: None,
        };
        let _ = advisory;
        let _ = WaitTurnRef::new("a1", 1, "t1");
    }
}
