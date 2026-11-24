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

// `a` is the recipient’s exact closed generation. Match both the retained
// restart loss and the unread, delivered result it registered while live.
pub(super) const CLOSED_COMPLETION_PARENT: &str = r#"
    a.state IN ('lost', 'stopped') AND a.archived = 0 AND a.observe_only = 0
    AND a.capacity_generation IS NULL
    AND EXISTS (
        SELECT 1 FROM events restart
        WHERE restart.agent = a.id AND restart.generation = a.generation
          AND ((restart.kind = 'agent.lost'
                AND json_extract(restart.data, '$.reason') = 'daemon_restart'
                AND json_extract(restart.data, '$.previous_state') = 'idle')
               OR (restart.kind = 'agent.stopped'
                   AND json_extract(restart.data, '$.reason') = 'owned_exit_before_restart'))
          AND NOT EXISTS (
              SELECT 1 FROM commands c
              WHERE c.target = a.id AND c.generation = a.generation
                AND (c.state IN ('accepted', 'queued', 'dispatched', 'unknown')
                     OR (c.verb = 'stop' AND c.principal = 'owner' AND c.state = 'applied')
                     OR (c.verb = 'resume' AND c.created_at >= restart.ts
                         AND c.state IN ('rejected', 'failed')))
          )
    )
    AND EXISTS (
        SELECT 1 FROM messages m
        JOIN completion_digests d ON d.delivered_mail_id = m.delivery_id
        WHERE m.recipient = a.id AND m.recipient_generation = a.generation
          AND m.read_at IS NULL AND m.wake = 1 AND m.sender = 'swarm'
          AND d.parent = a.id AND d.requester = 'agent:a' || a.id || ':' || a.generation
    )
"#;

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

/// One pending digest page plus the cycle high-water read in the same Store
/// transaction, so the bound and the rows can never disagree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingDigestPage {
    pub rows: Vec<DigestRow>,
    pub high_water: Option<(i64, String)>,
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

/// One pending page of stored rows after a keyset cursor, oldest first.
/// Takes the connection (or an open read transaction dereferencing to one) so
/// the standalone page read and the atomic high-water-plus-page snapshot
/// share one query shape: seek from the cursor by key, never from the start
/// of the pending partition.
fn pending_page_rows(
    connection: &rusqlite::Connection,
    after: Option<(i64, &str)>,
    limit: usize,
) -> Result<Vec<StoredDigestRow>> {
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
    let mut stmt = connection.prepare(&sql).map_err(sql_error)?;
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
        out.push(row.map_err(sql_error)?);
    }
    Ok(out)
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
    /// Recheck an exact completion recipient under the daemon Store writer.
    pub fn closed_completion_parent_eligible(&self, agent: &str, generation: i64) -> Result<bool> {
        let agent = parse_agent_id(agent)?;
        let candidate: bool = self
            .connection
            .query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM agents a WHERE a.id = ?1
                     AND a.generation = ?2 AND ({CLOSED_COMPLETION_PARENT}))"
                ),
                params![agent, generation],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        Ok(candidate
            && super::capacity::has_owned_exit_and_reader_drain_tx(
                &self.connection,
                agent,
                generation,
            )?)
    }

    /// Linked input for the exact automatic continuation of a restart-lost turn.
    pub fn completion_restart_input(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<Option<String>> {
        self.connection
            .query_row(
                "SELECT input.id
                 FROM commands resume
                 JOIN restart_resume_intents intent ON intent.command_id = resume.id
                 JOIN commands input ON input.parent_command = resume.id
                     AND input.id = resume.id || '/send' AND input.link_kind = 'send'
                     AND input.ordinal = 0 AND input.verb = 'send'
                     AND input.target = resume.target AND input.generation = ?2 + 1
                 WHERE resume.target = ?1 AND resume.expected_generation = ?2
                   AND resume.verb = 'resume'
                   AND resume.state IN ('accepted', 'queued', 'dispatched', 'applied')
                   AND EXISTS (
                       SELECT 1 FROM events lost
                       WHERE lost.agent = ?1 AND lost.generation = ?2
                         AND lost.kind = 'agent.lost'
                         AND json_extract(lost.data, '$.reason') = 'daemon_restart'
                         AND json_extract(lost.data, '$.turn_id') = ?3
                   )
                 ORDER BY resume.created_at DESC, resume.rowid DESC LIMIT 1",
                params![parse_agent_id(agent)?, generation, turn_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }

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

    /// Bounded pending registrations after a keyset cursor, oldest first.
    /// `after` is the last examined `(created_at, id)`; `None` starts at the
    /// oldest row. The driver advances the cursor past every examined row and
    /// wraps to `None` at the end, so pending or mailbox-blocked rows at the
    /// front never trap later eligible registrations behind one fixed page.
    pub fn pending_completion_digests_after(
        &self,
        after: Option<(i64, &str)>,
        limit: usize,
    ) -> Result<Vec<DigestRow>> {
        self.ensure_committed_reads()?;
        let mut out = Vec::new();
        for stored in pending_page_rows(&self.connection, after, limit)? {
            out.push(parse_row(stored)?);
        }
        Ok(out)
    }

    /// High-water plus one pending page in a single read transaction. The
    /// driver snapshots the cycle high-water and reads its first page here,
    /// and re-reads the head page here when a sweep wraps, so the bound and
    /// the rows can never disagree: a non-empty page always carries a
    /// high-water, and an empty queue always reads back empty. Continuation
    /// ticks within a sweep keep the captured bound and read only the page
    /// with [`Self::pending_completion_digests_after`].
    pub fn pending_completion_digests_page(
        &mut self,
        after: Option<(i64, &str)>,
        limit: usize,
    ) -> Result<PendingDigestPage> {
        self.ensure_committed_reads()?;
        let tx = self.connection.transaction().map_err(sql_error)?;
        let high_water: Option<(i64, String)> = tx
            .query_row(
                "SELECT created_at, id FROM completion_digests
                 WHERE delivered_at IS NULL
                 ORDER BY created_at DESC, id DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let mut rows = Vec::new();
        for stored in pending_page_rows(&tx, after, limit)? {
            rows.push(parse_row(stored)?);
        }
        // Read-only snapshot: rolling back releases the shared lock with no
        // writes to persist.
        drop(tx);
        Ok(PendingDigestPage { rows, high_water })
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
}
