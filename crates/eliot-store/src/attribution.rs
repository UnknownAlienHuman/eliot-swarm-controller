//! Advisory work claims and `who` worker/project reads (L5.2).
//!
//! Claims are project-relative globs with note/TTL, owned by an Agent
//! generation. They are advisory: a conflict never locks or reassigns work,
//! it only reports the overlapping holders in-band and routes one coalesced
//! generic notice to each holder through the same claim/suppression pattern
//! as the L5.1 mail wake. Liveness is computed, never a second ownership
//! database: a claim reads live only while its generation still matches the
//! Agent's current generation, the Agent is not terminal, and the TTL has
//! not passed. Generation change, terminal state and detach therefore
//! release by construction; expiry filters on read and is purged (bounded)
//! by the next claim write.

use super::coordination::{Sender, parse_sender, retain_write_tx};
use super::{
    Change, Error, Result, Store, canonical, command_binding, commit_transaction, decode_json,
    enforce_page_bytes, insert_daemon_event, load_agent_by_num, load_command_record, parse_enum,
    provider_holds, sql_error, turn_state_name, validate_id_age, write_binding,
};
use eliot_core::{
    AgentState, CommandState, TurnState,
    api::{
        CLAIM_NOTICE_COOLDOWN_MS, ClaimConflict, ClaimListRequest, ClaimListResponse,
        ClaimReleaseRequest, ClaimReleaseResponse, ClaimSetRequest, ClaimSetResponse, ClaimView,
        MAX_CLAIMS_PER_AGENT, MAX_WHO_WORKERS, claim_globs_overlap, normalize_claim_glob,
    },
    parse_agent_id,
};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::json;
use std::collections::HashMap;

const CLAIM_SET_METHOD: &str = "claim.set";
const CLAIM_RELEASE_METHOD: &str = "claim.release";
/// One claim write purges at most this many expired rows, so the cleanup
/// stays a bounded part of the admission transaction.
const CLAIM_EXPIRY_PURGE_ROWS: i64 = 1_000;
/// Conflict and list scans stop here; responses truncate honestly above it.
const CLAIM_SCAN_ROWS: i64 = 1_001;

/// A claimed conflict notice: the overlapping live-claim count for the
/// generic notice text and the holder generation the notice binds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimNoticeClaim {
    pub overlapping: i64,
    pub generation: i64,
}

/// Durable conflict-notice state for one holder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimNoticeState {
    pub last_notice_at: i64,
    pub open_notice_command: Option<String>,
}

/// One current non-terminal Agent in `who` scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WhoWorkerRow {
    pub id: String,
    pub num: i64,
    pub generation: i64,
    pub state: AgentState,
    pub project: String,
    pub workspace: Option<String>,
}

impl Store {
    /// Set (or refresh) one advisory claim for the caller's Agent generation.
    /// The response names every live overlapping claim held by other Agents
    /// in the same project; the Router routes at most one coalesced generic
    /// notice to each holder.
    pub fn claim_set(
        &mut self,
        principal: &str,
        request: &ClaimSetRequest,
        now: i64,
    ) -> Result<Change<ClaimSetResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.claim_set_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Release one live claim of the caller's Agent generation. Absent,
    /// expired and prior-generation claims read `not_found`.
    pub fn claim_release(
        &mut self,
        principal: &str,
        request: &ClaimReleaseRequest,
        now: i64,
    ) -> Result<Change<ClaimReleaseResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.claim_release_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// List live claims in scope: current generation, non-terminal holder,
    /// unexpired. Read-only.
    pub fn claim_list(
        &self,
        principal: &str,
        request: &ClaimListRequest,
        now: i64,
    ) -> Result<ClaimListResponse> {
        request.validate()?;
        let (project, agent) = self.resolve_claim_scope(principal, request)?;
        let mut claims = Self::live_claims(
            &self.connection,
            project.as_deref(),
            agent,
            now,
            CLAIM_SCAN_ROWS,
        )?;
        let scan_capped = claims.len() >= CLAIM_SCAN_ROWS as usize;
        claims.truncate((CLAIM_SCAN_ROWS - 1) as usize);
        let before_bytes = claims.len();
        enforce_page_bytes(&mut claims)?;
        let truncated = scan_capped || claims.len() != before_bytes;
        let response = ClaimListResponse { claims, truncated };
        request.validate_response(&response)?;
        Ok(response)
    }

    /// Current non-terminal Agents in `who` scope, oldest first, with a
    /// truncation flag instead of a cursor: the owner narrows by project.
    pub fn who_workers(
        &self,
        principal: &str,
        project: Option<&str>,
    ) -> Result<(Vec<WhoWorkerRow>, bool)> {
        let scope = self.resolve_who_scope(principal, project)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT id, generation, state, project, workspace FROM agents
                 WHERE state NOT IN ('stopped','failed','lost')
                   AND (?1 IS NULL OR project = ?1)
                 ORDER BY id ASC LIMIT ?2",
            )
            .map_err(sql_error)?;
        let limit = i64::try_from(MAX_WHO_WORKERS + 1).unwrap_or(i64::MAX);
        let rows = statement
            .query_map(params![scope, limit], |row| {
                let num: i64 = row.get(0)?;
                let state: String = row.get(2)?;
                Ok((num, row.get(1)?, state, row.get(3)?, row.get(4)?))
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<(i64, i64, String, String, Option<String>)>>>()
            .map_err(sql_error)?;
        let truncated = rows.len() > MAX_WHO_WORKERS;
        let mut workers = Vec::with_capacity(rows.len().min(MAX_WHO_WORKERS));
        for (num, generation, state, project, workspace) in rows.into_iter().take(MAX_WHO_WORKERS) {
            workers.push(WhoWorkerRow {
                id: format!("a{num}"),
                num,
                generation,
                state: parse_enum(&state, "Agent state")?,
                project,
                workspace,
            });
        }
        Ok((workers, truncated))
    }

    /// Non-terminal Agent counts per admitted workspace, across all
    /// projects: a worktree is singly owned only when exactly one
    /// non-terminal Agent admits it.
    pub fn workspace_owner_counts(&self) -> Result<HashMap<String, i64>> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT workspace, COUNT(*) FROM agents
                 WHERE workspace IS NOT NULL
                   AND state NOT IN ('stopped','failed','lost')
                 GROUP BY workspace",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<(String, i64)>>>()
            .map_err(sql_error)?;
        Ok(rows.into_iter().collect())
    }

    /// Atomically claim one generic conflict notice for a holder, or
    /// suppress it. Suppression (cooldown, an open or unknown notice
    /// command, an unknown command or turn, a pending request, or a
    /// provider hold) returns `Ok(None)` and records nothing.
    pub fn claim_notice_claim(
        &mut self,
        recipient: &str,
        notice_command: &str,
        overlapping: i64,
        now: i64,
    ) -> Result<Option<ClaimNoticeClaim>> {
        self.ensure_writer_healthy()?;
        let result = self.claim_notice_claim_inner(recipient, notice_command, overlapping, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Read durable conflict-notice state for one holder.
    pub fn claim_notice_state(&self, recipient: &str) -> Result<Option<ClaimNoticeState>> {
        let num = parse_agent_id(recipient)?;
        let row: Option<(i64, Option<String>)> = self
            .connection
            .query_row(
                "SELECT last_notice_at, open_notice_command FROM claim_notices WHERE recipient = ?1",
                params![num],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(
            row.map(|(last_notice_at, open_notice_command)| ClaimNoticeState {
                last_notice_at,
                open_notice_command,
            }),
        )
    }

    /// Release a claimed notice whose notice command was never admitted, so
    /// a failed `send` does not consume the notice cooldown. Only clears
    /// when the stored open command still names this notice.
    pub fn claim_notice_release(&mut self, recipient: &str, notice_command: &str) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = self.claim_notice_release_inner(recipient, notice_command);
        self.fault_on_persistence_error(&result);
        result
    }

    fn resolve_claim_agent(
        &self,
        principal: &str,
        agent: Option<&str>,
    ) -> Result<eliot_core::Agent> {
        match parse_sender(principal)? {
            Sender::Owner => {
                let Some(agent) = agent else {
                    return Err(Error::new(
                        "invalid",
                        "owner claim access must name the Agent explicitly",
                    ));
                };
                let num = parse_agent_id(agent)?;
                load_agent_by_num(&self.connection, num)?
                    .ok_or_else(|| Error::new("not_found", format!("Agent {agent} was not found")))
            }
            Sender::Agent {
                num,
                id,
                generation,
            } => {
                if agent.is_some_and(|target| target != id) {
                    return Err(Error::new(
                        "not_allowed",
                        "Agents claim only their own work area",
                    ));
                }
                let current = load_agent_by_num(&self.connection, num)?.ok_or_else(|| {
                    Error::new("stale_generation", "Agent generation is no longer current")
                })?;
                if current.generation != generation {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation is no longer current",
                    ));
                }
                Ok(current)
            }
        }
    }

    fn resolve_claim_scope(
        &self,
        principal: &str,
        request: &ClaimListRequest,
    ) -> Result<(Option<String>, Option<i64>)> {
        let agent = match request.agent.as_deref() {
            None => None,
            Some(agent) => {
                let num = parse_agent_id(agent)?;
                let row = load_agent_by_num(&self.connection, num)?.ok_or_else(|| {
                    Error::new("not_found", format!("Agent {agent} was not found"))
                })?;
                Some((num, row.project))
            }
        };
        match parse_sender(principal)? {
            Sender::Owner => {
                if let (Some(filter), Some((_, project))) = (request.project.as_deref(), &agent)
                    && filter != project
                {
                    return Err(Error::new(
                        "invalid",
                        "claim project filter does not match the Agent filter",
                    ));
                }
                Ok((request.project.clone(), agent.map(|(num, _)| num)))
            }
            Sender::Agent {
                num, generation, ..
            } => {
                let current = load_agent_by_num(&self.connection, num)?.ok_or_else(|| {
                    Error::new("stale_generation", "Agent generation is no longer current")
                })?;
                if current.generation != generation {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation is no longer current",
                    ));
                }
                if request
                    .project
                    .as_deref()
                    .is_some_and(|project| project != current.project)
                {
                    return Err(Error::new(
                        "not_allowed",
                        "Agents list only their own project claims",
                    ));
                }
                if agent
                    .as_ref()
                    .is_some_and(|(_, project)| project != &current.project)
                {
                    return Err(Error::new(
                        "not_allowed",
                        "Agents list only their own project claims",
                    ));
                }
                Ok((Some(current.project), agent.map(|(num, _)| num)))
            }
        }
    }

    fn resolve_who_scope(&self, principal: &str, project: Option<&str>) -> Result<Option<String>> {
        if project.is_some_and(|project| project.trim().is_empty()) {
            return Err(Error::new("invalid", "who project filter is invalid"));
        }
        match parse_sender(principal)? {
            Sender::Owner => Ok(project.map(str::to_owned)),
            Sender::Agent {
                num, generation, ..
            } => {
                let current = load_agent_by_num(&self.connection, num)?.ok_or_else(|| {
                    Error::new("stale_generation", "Agent generation is no longer current")
                })?;
                if current.generation != generation {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation is no longer current",
                    ));
                }
                if project.is_some_and(|project| project != current.project) {
                    return Err(Error::new(
                        "not_allowed",
                        "Agents read only their own project workers",
                    ));
                }
                Ok(Some(current.project))
            }
        }
    }

    fn live_claims(
        connection: &rusqlite::Connection,
        project: Option<&str>,
        agent: Option<i64>,
        now: i64,
        limit: i64,
    ) -> Result<Vec<ClaimView>> {
        let mut statement = connection
            .prepare(
                "SELECT c.agent, c.generation, c.glob, c.note, c.expires_at, c.created_at
                 FROM claims c JOIN agents a ON a.id = c.agent
                 WHERE c.expires_at > ?1
                   AND c.generation = a.generation
                   AND a.state NOT IN ('stopped','failed','lost')
                   AND (?2 IS NULL OR a.project = ?2)
                   AND (?3 IS NULL OR c.agent = ?3)
                 ORDER BY c.agent ASC, c.glob ASC LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![now, project, agent, limit], |row| {
                let num: i64 = row.get(0)?;
                Ok(ClaimView {
                    agent: format!("a{num}"),
                    generation: row.get(1)?,
                    glob: row.get(2)?,
                    note: row.get(3)?,
                    expires_at: row.get(4)?,
                    created_at: row.get(5)?,
                })
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<ClaimView>>>()
            .map_err(sql_error)?;
        for claim in &rows {
            claim.validate()?;
        }
        Ok(rows)
    }

    fn claim_set_inner(
        &mut self,
        principal: &str,
        request: &ClaimSetRequest,
        now: i64,
    ) -> Result<Change<ClaimSetResponse>> {
        request.validate()?;
        let glob = normalize_claim_glob(&request.glob)?;
        let target = self.resolve_claim_agent(principal, request.agent.as_deref())?;
        if target.state.is_terminal() {
            return Err(Error::new(
                "bad_state",
                "claims release on terminal state and cannot start there",
            ));
        }
        let target_num = parse_agent_id(&target.id)?;
        let ttl_ms = i64::try_from(request.effective_ttl_secs())
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000);
        let semantic_payload = json!({
            "agent": target.id,
            "glob": glob,
            "note": request.note,
            "ttl_secs": request.effective_ttl_secs(),
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if command_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "claim write id is already used by a command",
            ));
        }
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, &request.id)?
        {
            if old_principal != principal
                || old_method != CLAIM_SET_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "claim write id is bound to another principal, method, or payload",
                ));
            }
            let value = serde_json::from_value::<ClaimSetResponse>(decode_json(&old_result)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        purge_expired_claims_tx(&tx, now)?;
        db!(tx.execute(
            "DELETE FROM claims WHERE agent = ?1 AND generation != ?2",
            params![target_num, target.generation],
        ));
        let live: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM claims
                 WHERE agent = ?1 AND generation = ?2 AND expires_at > ?3 AND glob != ?4",
                params![target_num, target.generation, now, glob],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if live >= i64::try_from(MAX_CLAIMS_PER_AGENT).unwrap_or(i64::MAX) {
            return Err(Error::new(
                "claim_limit",
                format!(
                    "Agent {} already holds {MAX_CLAIMS_PER_AGENT} live claims",
                    target.id
                ),
            ));
        }
        let expires_at = now.saturating_add(ttl_ms);
        db!(tx.execute(
            "INSERT INTO claims(agent, generation, glob, note, expires_at, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(agent, generation, glob) DO UPDATE SET
                 note = excluded.note, expires_at = excluded.expires_at",
            params![
                target_num,
                target.generation,
                glob,
                request.note,
                expires_at,
                now
            ],
        ));
        let mut conflicts = Vec::new();
        let candidates = Self::live_claims_tx(&tx, &target.project, now)?;
        for candidate in candidates {
            if candidate.agent == target.id {
                continue;
            }
            if claim_globs_overlap(&glob, &candidate.glob)? && conflicts.len() < MAX_WHO_WORKERS {
                conflicts.push(ClaimConflict {
                    agent: candidate.agent,
                    generation: candidate.generation,
                    glob: candidate.glob,
                });
            }
        }
        let event = insert_daemon_event(
            &tx,
            now,
            "claim.changed",
            "coordination",
            Some(target_num),
            Some(target.generation),
            json!({
                "agent": target.id,
                "generation": target.generation,
                "glob": glob,
                "live": true,
            }),
        )?;
        let value = ClaimSetResponse {
            agent: target.id.clone(),
            generation: target.generation,
            glob,
            note: request.note.clone(),
            expires_at,
            admitted_at: now,
            conflicts,
        };
        request.validate_response(&value)?;
        retain_write_tx(
            &tx,
            &request.id,
            principal,
            CLAIM_SET_METHOD,
            &payload_digest,
            &serde_json::to_value(&value)
                .map_err(|error| Error::new("internal", error.to_string()))?,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value,
            events: vec![event],
        })
    }

    fn live_claims_tx(tx: &Transaction<'_>, project: &str, now: i64) -> Result<Vec<ClaimView>> {
        let mut statement = tx
            .prepare(
                "SELECT c.agent, c.generation, c.glob, c.note, c.expires_at, c.created_at
                 FROM claims c JOIN agents a ON a.id = c.agent
                 WHERE a.project = ?1 AND c.expires_at > ?2
                   AND c.generation = a.generation
                   AND a.state NOT IN ('stopped','failed','lost')
                 ORDER BY c.agent ASC, c.glob ASC LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![project, now, CLAIM_SCAN_ROWS], |row| {
                let num: i64 = row.get(0)?;
                Ok(ClaimView {
                    agent: format!("a{num}"),
                    generation: row.get(1)?,
                    glob: row.get(2)?,
                    note: row.get(3)?,
                    expires_at: row.get(4)?,
                    created_at: row.get(5)?,
                })
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<ClaimView>>>()
            .map_err(sql_error)?;
        for claim in &rows {
            claim.validate()?;
        }
        Ok(rows)
    }

    fn claim_release_inner(
        &mut self,
        principal: &str,
        request: &ClaimReleaseRequest,
        now: i64,
    ) -> Result<Change<ClaimReleaseResponse>> {
        request.validate()?;
        let glob = normalize_claim_glob(&request.glob)?;
        let target = self.resolve_claim_agent(principal, request.agent.as_deref())?;
        let target_num = parse_agent_id(&target.id)?;
        let semantic_payload = json!({
            "agent": target.id,
            "glob": glob,
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if command_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "claim write id is already used by a command",
            ));
        }
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, &request.id)?
        {
            if old_principal != principal
                || old_method != CLAIM_RELEASE_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "claim write id is bound to another principal, method, or payload",
                ));
            }
            let value = serde_json::from_value::<ClaimReleaseResponse>(decode_json(&old_result)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        purge_expired_claims_tx(&tx, now)?;
        db!(tx.execute(
            "DELETE FROM claims WHERE agent = ?1 AND generation != ?2",
            params![target_num, target.generation],
        ));
        let removed = db!(tx.execute(
            "DELETE FROM claims WHERE agent = ?1 AND generation = ?2 AND glob = ?3",
            params![target_num, target.generation, glob],
        ));
        if removed == 0 {
            return Err(Error::new("not_found", "claim was not found"));
        }
        let event = insert_daemon_event(
            &tx,
            now,
            "claim.changed",
            "coordination",
            Some(target_num),
            Some(target.generation),
            json!({
                "agent": target.id,
                "generation": target.generation,
                "glob": glob,
                "live": false,
            }),
        )?;
        let value = ClaimReleaseResponse {
            agent: target.id.clone(),
            generation: target.generation,
            glob,
        };
        request.validate_response(&value)?;
        retain_write_tx(
            &tx,
            &request.id,
            principal,
            CLAIM_RELEASE_METHOD,
            &payload_digest,
            &serde_json::to_value(&value)
                .map_err(|error| Error::new("internal", error.to_string()))?,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value,
            events: vec![event],
        })
    }

    fn claim_notice_claim_inner(
        &mut self,
        recipient: &str,
        notice_command: &str,
        overlapping: i64,
        now: i64,
    ) -> Result<Option<ClaimNoticeClaim>> {
        let num = parse_agent_id(recipient)?;
        if notice_command.trim().is_empty() || overlapping < 1 {
            return Err(Error::new(
                "invalid",
                "claim notice needs a command id and a conflict",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent = load_agent_by_num(&tx, num)?;
        let Some(agent) = agent else {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(None);
        };
        let notice: Option<(i64, Option<String>)> = tx
            .query_row(
                "SELECT last_notice_at, open_notice_command FROM claim_notices WHERE recipient = ?1",
                params![num],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        if notice
            .as_ref()
            .is_some_and(|(last, _)| now.saturating_sub(*last) < CLAIM_NOTICE_COOLDOWN_MS)
        {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(None);
        }
        if let Some((_, Some(open))) = notice.as_ref() {
            let blocking = load_command_record(&tx, open)?.is_some_and(|record| {
                matches!(
                    record.state,
                    CommandState::Accepted
                        | CommandState::Queued
                        | CommandState::Dispatched
                        | CommandState::Unknown
                )
            });
            if blocking {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(None);
            }
        }
        let unknown_command: bool = tx
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM commands WHERE target = ?1 AND state = 'unknown'
                 )",
                params![num],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if unknown_command {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(None);
        }
        let unknown_turn: bool = tx
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM turns WHERE agent = ?1 AND state = ?2
                 )",
                params![num, turn_state_name(TurnState::Unknown)],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if unknown_turn {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(None);
        }
        let pending_request: bool = tx
            .query_row(
                "SELECT EXISTS(
                     SELECT 1 FROM pending_requests
                     WHERE agent = ?1 AND generation = ?2 AND state = 'pending'
                 )",
                params![num, agent.generation],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if pending_request {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(None);
        }
        match provider_holds::require_unheld_tx(&tx, &agent) {
            Ok(()) => {}
            Err(error) if error.code == "provider_held" => {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
        db!(tx.execute(
            "INSERT INTO claim_notices(recipient, last_notice_at, open_notice_command)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(recipient) DO UPDATE SET
                 last_notice_at = excluded.last_notice_at,
                 open_notice_command = excluded.open_notice_command",
            params![num, now, notice_command],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Some(ClaimNoticeClaim {
            overlapping,
            generation: agent.generation,
        }))
    }

    fn claim_notice_release_inner(&mut self, recipient: &str, notice_command: &str) -> Result<()> {
        let num = parse_agent_id(recipient)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        db!(tx.execute(
            "DELETE FROM claim_notices WHERE recipient = ?1 AND open_notice_command = ?2",
            params![num, notice_command],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(())
    }
}

/// Delete expired claims, at most one bounded batch per write. Reads filter
/// expiry lazily, so a backlog only costs storage, never a wrong answer.
fn purge_expired_claims_tx(tx: &Transaction<'_>, now: i64) -> Result<()> {
    db!(tx.execute(
        "DELETE FROM claims WHERE rowid IN (
             SELECT rowid FROM claims WHERE expires_at <= ?1 LIMIT ?2
         )",
        params![now, CLAIM_EXPIRY_PURGE_ROWS],
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{NativeEvent, SourceBatch, SubmitCommand};
    use std::{
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    };
    use ulid::Ulid;

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    fn store() -> Store {
        Store::open(Path::new(":memory:")).unwrap()
    }

    fn ready_agent(store: &mut Store, project: &str, workspace: &str, now: i64) -> String {
        let launch_id = Ulid::new().to_string();
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":project, "workspace":workspace}),
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
                    batch_id: Ulid::new().to_string(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: Ulid::new().to_string(),
                        data: json!({"command_id":launch_id,
                            "native_scope": format!("{launch_id}:scope"),
                            "native_ref": format!("{launch_id}:ref")}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, CommandState::Applied, None, now)
            .expect("settle launch");
        agent
    }

    fn stop_agent(store: &mut Store, agent: &str, now: i64) {
        let stop_id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: stop_id.clone(),
                    target: Some(agent.to_owned()),
                    verb: "stop".to_owned(),
                    args: json!({}),
                    expected_generation: None,
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit stop");
        store.dispatch(&stop_id, now).expect("dispatch stop");
        store
            .settle(&stop_id, CommandState::Applied, None, now)
            .expect("settle stop");
        assert!(
            store
                .get_agent(agent)
                .expect("read stopped Agent")
                .expect("stopped Agent exists")
                .state
                .is_terminal()
        );
    }

    fn resume_agent(store: &mut Store, agent: &str, now: i64) {
        // A terminal native fact proves the old custody closed, so the
        // resume admits instead of blocking on its retained reservation.
        store
            .put_batch(
                SourceBatch {
                    batch_id: Ulid::new().to_string(),
                    agent_id: agent.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ended".to_owned(),
                        native_id: Ulid::new().to_string(),
                        data: json!({}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit terminal session fact");
        let resume_id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: resume_id.clone(),
                    target: Some(agent.to_owned()),
                    verb: "resume".to_owned(),
                    args: json!({}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit resume");
        store.dispatch(&resume_id, now).expect("dispatch resume");
        let current = store
            .get_agent(agent)
            .expect("read resumed Agent")
            .expect("resumed Agent exists");
        assert_eq!(current.generation, 2);
        // The resumed readiness retains the generation-1 native subject.
        let current = store
            .get_agent(agent)
            .expect("read resumed Agent")
            .expect("resumed Agent exists");
        store
            .put_batch(
                SourceBatch {
                    batch_id: Ulid::new().to_string(),
                    agent_id: agent.to_owned(),
                    generation: 2,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: Ulid::new().to_string(),
                        data: json!({"command_id":resume_id,
                            "native_scope": current.native_scope,
                            "native_ref": current.native_ref}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit resumed session ready");
        store
            .settle(&resume_id, CommandState::Applied, None, now)
            .expect("settle resume");
    }

    fn agent_principal(agent: &str, generation: i64) -> String {
        format!("agent:{agent}:{generation}")
    }

    fn set_request(glob: &str) -> ClaimSetRequest {
        ClaimSetRequest {
            id: Ulid::new().to_string(),
            agent: None,
            glob: glob.to_owned(),
            note: None,
            ttl_secs: None,
        }
    }

    fn claim_rows(store: &Store) -> i64 {
        store
            .connection
            .query_row("SELECT COUNT(*) FROM claims", [], |row| row.get(0))
            .unwrap()
    }

    #[test]
    fn claims_table_matches_the_spec_shape() {
        let store = store();
        let columns: Vec<(String, String)> = store
            .connection
            .prepare("SELECT name, type FROM pragma_table_info('claims') ORDER BY cid")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            columns
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            [
                "agent",
                "generation",
                "glob",
                "note",
                "expires_at",
                "created_at"
            ]
        );
        let ddl: String = store
            .connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'claims'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(ddl.contains("PRIMARY KEY(agent,generation,glob)"));
        let plan: String = store
            .connection
            .query_row(
                "EXPLAIN QUERY PLAN SELECT rowid FROM claims WHERE expires_at <= 1 LIMIT 1",
                [],
                |row| row.get(3),
            )
            .unwrap();
        assert!(
            plan.contains("claims_expiry"),
            "expiry purge must use its index: {plan}"
        );
    }

    #[test]
    fn claims_roundtrip_set_list_release() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", "ws-a", now);
        let b = ready_agent(&mut store, "app", "ws-b", now);

        let change = store
            .claim_set(&agent_principal(&a, 1), &set_request("src/**"), now)
            .expect("Agent claims its area");
        assert_eq!(change.value.agent, a);
        assert_eq!(change.value.generation, 1);
        assert_eq!(change.value.glob, "src/**");
        assert_eq!(change.value.admitted_at, now);
        assert_eq!(change.value.expires_at, now + 3_600_000);
        assert!(change.value.conflicts.is_empty());
        assert_eq!(change.events.len(), 1);
        assert_eq!(change.events[0].kind, "claim.changed");
        assert_eq!(change.events[0].data["live"], true);

        // Same-project B lists A's live claim; ordering is (Agent, glob).
        let listed = store
            .claim_list(
                &agent_principal(&b, 1),
                &ClaimListRequest {
                    project: None,
                    agent: None,
                },
                now,
            )
            .expect("same-project list");
        assert!(!listed.truncated);
        assert_eq!(listed.claims.len(), 1);
        assert_eq!(listed.claims[0].agent, a);
        assert_eq!(listed.claims[0].generation, 1);

        let release_id = Ulid::new().to_string();
        let released = store
            .claim_release(
                &agent_principal(&a, 1),
                &ClaimReleaseRequest {
                    id: release_id,
                    agent: None,
                    glob: "./src/**".to_owned(),
                },
                now,
            )
            .expect("explicit release normalizes the glob");
        assert_eq!(released.value.agent, a);
        assert_eq!(released.events.len(), 1);
        assert_eq!(released.events[0].data["live"], false);
        let listed = store
            .claim_list(
                "owner",
                &ClaimListRequest {
                    project: Some("app".to_owned()),
                    agent: None,
                },
                now,
            )
            .expect("owner list after release");
        assert!(listed.claims.is_empty());

        let error = store
            .claim_release(
                &agent_principal(&a, 1),
                &ClaimReleaseRequest {
                    id: Ulid::new().to_string(),
                    agent: None,
                    glob: "src/**".to_owned(),
                },
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "not_found");
    }

    #[test]
    fn claim_conflicts_report_overlapping_holders_only() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", "ws-a", now);
        let b = ready_agent(&mut store, "app", "ws-b", now);
        let c = ready_agent(&mut store, "other", "ws-c", now);
        store
            .claim_set(&agent_principal(&a, 1), &set_request("src/**"), now)
            .expect("A claims src");

        // A literal under A's glob conflicts; a disjoint glob does not.
        let change = store
            .claim_set(&agent_principal(&b, 1), &set_request("src/main.rs"), now)
            .expect("B claims an overlapping file");
        assert_eq!(change.value.conflicts.len(), 1);
        assert_eq!(change.value.conflicts[0].agent, a);
        assert_eq!(change.value.conflicts[0].glob, "src/**");
        let change = store
            .claim_set(&agent_principal(&b, 1), &set_request("other/**"), now)
            .expect("B claims a disjoint area");
        assert!(change.value.conflicts.is_empty());

        // Same glob text in another project is another namespace.
        let change = store
            .claim_set(&agent_principal(&c, 1), &set_request("src/main.rs"), now)
            .expect("C claims the same text elsewhere");
        assert!(change.value.conflicts.is_empty());

        // An Agent never conflicts with itself: B's new glob also matches
        // its own literal, but only A's rows are reported.
        let change = store
            .claim_set(&agent_principal(&b, 1), &set_request("src/**"), now)
            .expect("B claims the same area as A");
        assert_eq!(change.value.conflicts.len(), 1);
        assert!(
            change
                .value
                .conflicts
                .iter()
                .all(|conflict| conflict.agent == a),
            "self-overlap must be excluded: {:?}",
            change.value.conflicts
        );
    }

    #[test]
    fn claims_release_on_expiry_generation_change_and_terminal() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", "ws-a", now);
        let b = ready_agent(&mut store, "app", "ws-b", now);
        store
            .claim_set(
                &agent_principal(&a, 1),
                &ClaimSetRequest {
                    ttl_secs: Some(1),
                    ..set_request("src/**")
                },
                now,
            )
            .expect("A claims with a one-second TTL");

        // Live now, gone after the TTL: no list row, no conflict, and the
        // next write purges the expired row.
        let listed = store
            .claim_list(
                &agent_principal(&b, 1),
                &ClaimListRequest {
                    project: None,
                    agent: None,
                },
                now,
            )
            .expect("list before expiry");
        assert_eq!(listed.claims.len(), 1);
        let later = now + 1_001;
        let listed = store
            .claim_list(
                &agent_principal(&b, 1),
                &ClaimListRequest {
                    project: None,
                    agent: None,
                },
                later,
            )
            .expect("list after expiry");
        assert!(listed.claims.is_empty());
        let change = store
            .claim_set(&agent_principal(&b, 1), &set_request("src/**"), later)
            .expect("B claims after A's expiry");
        assert!(change.value.conflicts.is_empty());
        assert_eq!(claim_rows(&store), 1, "expired row purged by the write");

        // Terminal state releases: the stopped holder is not listed.
        stop_agent(&mut store, &b, later);
        let listed = store
            .claim_list(
                "owner",
                &ClaimListRequest {
                    project: Some("app".to_owned()),
                    agent: None,
                },
                later,
            )
            .expect("terminal holder is not listed");
        assert!(listed.claims.is_empty());

        // Generation change releases: the generation-1 row is never shown
        // for the resumed generation-2 Agent.
        let c = ready_agent(&mut store, "app", "ws-c", later);
        store
            .claim_set(&agent_principal(&c, 1), &set_request("src/**"), later)
            .expect("C claims in generation 1");
        resume_agent(&mut store, &c, later);
        let listed = store
            .claim_list(
                "owner",
                &ClaimListRequest {
                    project: Some("app".to_owned()),
                    agent: None,
                },
                later,
            )
            .expect("old generation is never presented");
        assert!(listed.claims.is_empty());
        let change = store
            .claim_set(&agent_principal(&c, 2), &set_request("src/**"), later)
            .expect("generation 2 claims fresh");
        assert!(change.value.conflicts.is_empty());
        assert_eq!(change.value.generation, 2);
    }

    #[test]
    fn claim_authority_scopes_agents_projects_and_generations() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", "ws-a", now);
        let b = ready_agent(&mut store, "other", "ws-b", now);

        let error = store
            .claim_set(
                &agent_principal(&a, 1),
                &ClaimSetRequest {
                    agent: Some(b.clone()),
                    ..set_request("src/**")
                },
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");

        let error = store
            .claim_list(
                &agent_principal(&a, 1),
                &ClaimListRequest {
                    project: Some("other".to_owned()),
                    agent: None,
                },
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");

        let error = store
            .claim_list(
                &agent_principal(&a, 1),
                &ClaimListRequest {
                    project: None,
                    agent: Some(b.clone()),
                },
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");

        let error = store
            .claim_set(&agent_principal(&a, 0), &set_request("src/**"), now)
            .unwrap_err();
        assert_eq!(error.code, "stale_generation");

        for principal in ["run:cmd-1", "project:app"] {
            let error = store
                .claim_set(principal, &set_request("src/**"), now)
                .unwrap_err();
            assert_eq!(error.code, "not_allowed");
        }

        let error = store
            .claim_set("owner", &set_request("src/**"), now)
            .unwrap_err();
        assert_eq!(error.code, "invalid");
        let error = store
            .claim_release(
                "owner",
                &ClaimReleaseRequest {
                    id: Ulid::new().to_string(),
                    agent: None,
                    glob: "src/**".to_owned(),
                },
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "invalid");

        let error = store
            .claim_set(
                "owner",
                &ClaimSetRequest {
                    agent: Some("a999".to_owned()),
                    ..set_request("src/**")
                },
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "not_found");

        // The owner claims on behalf of a named live Agent.
        let change = store
            .claim_set(
                "owner",
                &ClaimSetRequest {
                    agent: Some(a.clone()),
                    ..set_request("src/**")
                },
                now,
            )
            .expect("owner claims for a named Agent");
        assert_eq!(change.value.agent, a);

        stop_agent(&mut store, &b, now);
        let error = store
            .claim_set(
                "owner",
                &ClaimSetRequest {
                    agent: Some(b.clone()),
                    ..set_request("src/**")
                },
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "bad_state");
    }

    #[test]
    fn claim_writes_bind_ids_and_cap_live_claims() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", "ws-a", now);
        let principal = agent_principal(&a, 1);

        let id = Ulid::new().to_string();
        let first = store
            .claim_set(
                &principal,
                &ClaimSetRequest {
                    id: id.clone(),
                    ..set_request("src/**")
                },
                now,
            )
            .expect("first claim");
        let replay = store
            .claim_set(
                &principal,
                &ClaimSetRequest {
                    id: id.clone(),
                    ..set_request("src/**")
                },
                now,
            )
            .expect("exact replay returns the retained result");
        assert_eq!(replay.value, first.value);
        assert!(replay.events.is_empty());
        let error = store
            .claim_set(
                &principal,
                &ClaimSetRequest {
                    id: id.clone(),
                    ..set_request("other/**")
                },
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "id_conflict");

        for index in 1..MAX_CLAIMS_PER_AGENT {
            store
                .claim_set(&principal, &set_request(&format!("area-{index}/**")), now)
                .expect("claim under the cap");
        }
        let error = store
            .claim_set(&principal, &set_request("one-too-many/**"), now)
            .unwrap_err();
        assert_eq!(error.code, "claim_limit");
        // Refreshing an existing glob at the cap still works.
        store
            .claim_set(&principal, &set_request("src/**"), now)
            .expect("upsert at the cap refreshes");
    }

    #[test]
    fn claim_notices_coalesce_and_release() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", "ws-a", now);

        let first = Ulid::new().to_string();
        let claim = store
            .claim_notice_claim(&a, &first, 2, now)
            .expect("first notice claim");
        assert_eq!(
            claim,
            Some(ClaimNoticeClaim {
                overlapping: 2,
                generation: 1,
            })
        );
        let state = store.claim_notice_state(&a).expect("read notice state");
        assert_eq!(
            state,
            Some(ClaimNoticeState {
                last_notice_at: now,
                open_notice_command: Some(first.clone()),
            })
        );
        // The cooldown coalesces the immediate second conflict.
        let suppressed = store
            .claim_notice_claim(&a, &Ulid::new().to_string(), 1, now + 1)
            .expect("coalesced notice claim");
        assert_eq!(suppressed, None);

        // Releasing the wrong command disturbs nothing; releasing the
        // recorded one frees the next conflict after the cooldown.
        store
            .claim_notice_release(&a, &Ulid::new().to_string())
            .expect("release of a foreign command is a no-op");
        assert_eq!(
            store.claim_notice_state(&a).expect("state kept"),
            Some(ClaimNoticeState {
                last_notice_at: now,
                open_notice_command: Some(first.clone()),
            })
        );
        store
            .claim_notice_release(&a, &first)
            .expect("release the failed notice");
        assert_eq!(store.claim_notice_state(&a).expect("state gone"), None);
        let later = now + CLAIM_NOTICE_COOLDOWN_MS;
        let claim = store
            .claim_notice_claim(&a, &Ulid::new().to_string(), 1, later)
            .expect("notice after cooldown");
        assert_eq!(claim.map(|claim| claim.overlapping), Some(1));
    }

    #[test]
    fn claim_notice_claim_honors_suppression_gates() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", "ws-a", now);

        // An unknown command suppresses without recording.
        let send_id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: send_id.clone(),
                    target: Some(a.clone()),
                    verb: "send".to_owned(),
                    args: json!({"text":"hello"}),
                    expected_generation: None,
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit send");
        store.dispatch(&send_id, now).expect("dispatch send");
        store
            .settle(&send_id, CommandState::Unknown, None, now)
            .expect("settle unknown");
        let suppressed = store
            .claim_notice_claim(&a, &Ulid::new().to_string(), 1, now)
            .expect("unknown gate suppresses");
        assert_eq!(suppressed, None);
        assert_eq!(
            store.claim_notice_state(&a).expect("nothing recorded"),
            None
        );
    }

    #[test]
    fn who_workers_lists_current_scope_and_owner_counts() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", "ws-shared", now);
        let b = ready_agent(&mut store, "app", "ws-shared", now);
        let c = ready_agent(&mut store, "other", "ws-c", now);

        let (workers, truncated) = store.who_workers("owner", None).expect("owner reads all");
        assert!(!truncated);
        assert_eq!(
            workers
                .iter()
                .map(|worker| worker.id.as_str())
                .collect::<Vec<_>>(),
            [&a, &b, &c]
        );
        let (workers, _) = store
            .who_workers("owner", Some("app"))
            .expect("owner filters by project");
        assert_eq!(workers.len(), 2);
        let (workers, _) = store
            .who_workers(&agent_principal(&a, 1), None)
            .expect("Agent reads its own project");
        assert_eq!(workers.len(), 2);
        let error = store
            .who_workers(&agent_principal(&a, 1), Some("other"))
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
        let error = store
            .who_workers(&agent_principal(&a, 0), None)
            .unwrap_err();
        assert_eq!(error.code, "stale_generation");

        let counts = store.workspace_owner_counts().expect("owner counts");
        assert_eq!(counts.get("ws-shared"), Some(&2));
        assert_eq!(counts.get("ws-c"), Some(&1));

        // Terminal Agents leave both the roster and the ownership counts.
        stop_agent(&mut store, &b, now);
        stop_agent(&mut store, &c, now);
        let (workers, _) = store
            .who_workers("owner", None)
            .expect("roster after stops");
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].id, a);
        let counts = store.workspace_owner_counts().expect("counts after stops");
        assert_eq!(counts.get("ws-shared"), Some(&1));
        assert!(!counts.contains_key("ws-c"));
    }
}
