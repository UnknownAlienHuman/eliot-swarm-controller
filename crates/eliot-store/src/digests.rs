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
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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

    /// Bounded pending registrations, oldest first.
    pub fn pending_completion_digests(&self, limit: usize) -> Result<Vec<DigestRow>> {
        self.ensure_committed_reads()?;
        let limit = limit.clamp(1, 256) as i64;
        let mut stmt = self
            .connection
            .prepare(
                "SELECT id, parent, mode, commands, turns, requester, created_at, delivered_mail_id, delivered_at, payload_digest, hidden
                 FROM completion_digests WHERE delivered_at IS NULL ORDER BY created_at ASC, id ASC LIMIT ?1",
            )
            .map_err(sql_error)?;
        let rows = stmt
            .query_map(params![limit], |row| {
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

    /// Test helper: decode the stored request back to its wire shape.
    pub fn digest_request_json(&self, id: &str) -> Result<Option<Value>> {
        let Some(row) = self.digest_row(id)? else {
            return Ok(None);
        };
        let turns: Vec<Value> = row
            .turns
            .iter()
            .map(|t| {
                json!({
                    "agent": t.agent,
                    "generation": t.generation,
                    "turn_id": t.turn_id,
                })
            })
            .collect();
        Ok(Some(json!({
            "id": row.id,
            "to": row.parent,
            "mode": row.mode,
            "commands": row.commands,
            "turns": turns,
        })))
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
