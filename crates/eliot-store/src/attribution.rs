//! Advisory work claims and `who` worker/project reads (L5.2).
//!
//! Claims are project-relative globs with note/TTL, owned by an Agent
//! generation. They are advisory: a conflict never locks or reassigns work,
//! it only reports the overlapping holders in-band and routes one coalesced
//! generic notice to each holder through the shared L5.1 notice gate.
//! Liveness is computed, never a second ownership
//! database: a claim reads live only while its generation still matches the
//! Agent's current generation, the Agent is not terminal, and the TTL has
//! not passed. Generation change, terminal state and detach therefore
//! release by construction; expiry filters on read and is purged (bounded)
//! by the next claim write.

use super::coordination::{Sender, parse_sender, retain_write_tx};
use super::{
    Change, Error, Result, Store, canonical, command_binding, commit_transaction, decode_json,
    enforce_page_bytes, insert_daemon_event, load_agent_by_num, parse_enum, sql_error,
    validate_id_age, write_binding,
};
use eliot_core::{
    AgentState,
    api::{
        ClaimConflict, ClaimListRequest, ClaimListResponse, ClaimReleaseRequest,
        ClaimReleaseResponse, ClaimSetRequest, ClaimSetResponse, ClaimView, MAX_CLAIMS_PER_AGENT,
        MAX_WHO_WORKERS, claim_globs_overlap_normalized, normalize_claim_glob,
    },
    parse_agent_id,
};
use rusqlite::{Transaction, TransactionBehavior, params};
use serde_json::json;
use std::collections::HashMap;

const CLAIM_SET_METHOD: &str = "claim.set";
const CLAIM_RELEASE_METHOD: &str = "claim.release";
/// One claim write purges at most this many expired rows, so the cleanup
/// stays a bounded part of the admission transaction.
const CLAIM_EXPIRY_PURGE_ROWS: i64 = 1_000;
/// Conflict and list scans stop here; responses truncate honestly above it.
const CLAIM_SCAN_ROWS: i64 = 1_001;

/// One current non-terminal Agent in `who` scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WhoWorkerRow {
    pub id: String,
    pub num: i64,
    pub generation: i64,
    pub state: AgentState,
    pub project: String,
    pub workspace: Option<String>,
    pub chat: bool,
}

impl Store {
    /// Set (or refresh) one advisory claim for the caller's Agent generation.
    /// The response names live overlapping claims held by other Agents in
    /// the same project, with `truncated` when the scan or report cap
    /// engaged; the Router routes at most one coalesced generic notice to
    /// each holder.
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
            false,
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
                "SELECT id, generation, state, COALESCE(project_scope, project), workspace, chat FROM agents
                 WHERE state NOT IN ('stopped','failed','lost')
                   AND (?1 IS NULL OR COALESCE(project_scope, project) = ?1)
                 ORDER BY id ASC LIMIT ?2",
            )
            .map_err(sql_error)?;
        let limit = i64::try_from(MAX_WHO_WORKERS + 1).unwrap_or(i64::MAX);
        let rows = statement
            .query_map(params![scope, limit], |row| {
                let num: i64 = row.get(0)?;
                let state: String = row.get(2)?;
                Ok((
                    num,
                    row.get(1)?,
                    state,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<(i64, i64, String, String, Option<String>, bool)>>>()
            .map_err(sql_error)?;
        let truncated = rows.len() > MAX_WHO_WORKERS;
        let mut workers = Vec::with_capacity(rows.len().min(MAX_WHO_WORKERS));
        for (num, generation, state, project, workspace, chat) in
            rows.into_iter().take(MAX_WHO_WORKERS)
        {
            workers.push(WhoWorkerRow {
                id: format!("a{num}"),
                num,
                generation,
                state: parse_enum(&state, "Agent state")?,
                project,
                workspace,
                chat,
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

    fn resolve_claim_agent(
        &self,
        principal: &str,
        agent: Option<&str>,
    ) -> Result<eliot_core::Agent> {
        match parse_sender(principal, "coordination")? {
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
                let current = load_current_claim_agent(&self.connection, num, generation)?;
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
                Some((num, row.project_scope().to_owned()))
            }
        };
        match parse_sender(principal, "coordination")? {
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
                let current = load_current_claim_agent(&self.connection, num, generation)?;
                let project_mismatch = request
                    .project
                    .as_deref()
                    .is_some_and(|project| project != current.project_scope())
                    || agent
                        .as_ref()
                        .is_some_and(|(_, project)| project != current.project_scope());
                if project_mismatch {
                    return Err(Error::new(
                        "not_allowed",
                        "Agents list only their own project claims",
                    ));
                }
                Ok((
                    Some(current.project_scope().to_owned()),
                    agent.map(|(num, _)| num),
                ))
            }
        }
    }

    fn resolve_who_scope(&self, principal: &str, project: Option<&str>) -> Result<Option<String>> {
        if project.is_some_and(|project| project.trim().is_empty()) {
            return Err(Error::new("invalid", "who project filter is invalid"));
        }
        match parse_sender(principal, "coordination")? {
            Sender::Owner => Ok(project.map(str::to_owned)),
            Sender::Agent {
                num, generation, ..
            } => {
                let current = load_current_claim_agent(&self.connection, num, generation)?;
                if project.is_some_and(|project| project != current.project_scope()) {
                    return Err(Error::new(
                        "not_allowed",
                        "Agents read only their own project workers",
                    ));
                }
                Ok(Some(current.project_scope().to_owned()))
            }
        }
    }

    fn live_claims(
        connection: &rusqlite::Connection,
        project: Option<&str>,
        agent: Option<i64>,
        now: i64,
        limit: i64,
        include_terminal: bool,
    ) -> Result<Vec<ClaimView>> {
        let mut statement = connection
            .prepare(
                "SELECT c.agent, c.generation, c.glob, c.note, c.expires_at, c.created_at
                 FROM claims c JOIN agents a ON a.id = c.agent
                 WHERE c.expires_at > ?1
                   AND c.generation = a.generation
                   AND (?5 OR a.state NOT IN ('stopped','failed','lost'))
                   AND (?2 IS NULL OR COALESCE(a.project_scope, a.project) = ?2)
                   AND (?3 IS NULL OR c.agent = ?3)
                 ORDER BY c.agent ASC, c.glob ASC LIMIT ?4",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![now, project, agent, limit, include_terminal],
                |row| {
                    let num: i64 = row.get(0)?;
                    Ok(ClaimView {
                        agent: format!("a{num}"),
                        generation: row.get(1)?,
                        glob: row.get(2)?,
                        note: row.get(3)?,
                        expires_at: row.get(4)?,
                        created_at: row.get(5)?,
                    })
                },
            )
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
        let ttl_secs = request.effective_ttl_secs();
        let semantic_payload = json!({
            "agent": target.id,
            "glob": glob,
            "note": request.note,
            "ttl_secs": ttl_secs,
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some(stored) = replay_claim_write(
            &tx,
            &request.id,
            principal,
            CLAIM_SET_METHOD,
            &payload_digest,
        )? {
            let value = serde_json::from_value::<ClaimSetResponse>(decode_json(&stored)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        super::project_assignment::authorize_coordination(
            &tx,
            principal,
            Some(parse_agent_id(&target.id)?),
        )?;
        let (target_num, expires_at) =
            Self::admit_claim_row(&tx, &target, &glob, request.note.as_deref(), ttl_secs, now)?;
        let (conflicts, truncated) =
            Self::scan_claim_conflicts(&tx, target.project_scope(), &target.id, &glob, now)?;
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
            truncated,
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

    /// Insert or refresh one live claim row after purging expired rows,
    /// dropping the holder's prior-generation rows and enforcing the
    /// per-generation cap. Returns the holder number and the new expiry.
    fn admit_claim_row(
        tx: &Transaction<'_>,
        target: &eliot_core::Agent,
        glob: &str,
        note: Option<&str>,
        ttl_secs: u64,
        now: i64,
    ) -> Result<(i64, i64)> {
        let target_num = parse_agent_id(&target.id)?;
        purge_expired_claims_tx(tx, now)?;
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
        let ttl_ms = i64::try_from(ttl_secs)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000);
        let expires_at = now.saturating_add(ttl_ms);
        db!(tx.execute(
            "INSERT INTO claims(agent, generation, glob, note, expires_at, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(agent, generation, glob) DO UPDATE SET
                 note = excluded.note, expires_at = excluded.expires_at",
            params![target_num, target.generation, glob, note, expires_at, now],
        ));
        Ok((target_num, expires_at))
    }

    /// Live claims of other Agents in the project that overlap `glob`. The
    /// scan stops at `CLAIM_SCAN_ROWS` rows and the report at
    /// `MAX_WHO_WORKERS` entries; `truncated` tells the caller a cap
    /// engaged instead of silently dropping holders. Both sides are already
    /// normalized — `glob` at admission, candidates at their admission —
    /// so the overlap check skips re-normalization per candidate.
    fn scan_claim_conflicts(
        tx: &Transaction<'_>,
        project: &str,
        holder: &str,
        glob: &str,
        now: i64,
    ) -> Result<(Vec<ClaimConflict>, bool)> {
        let candidates = Self::live_claims(tx, Some(project), None, now, CLAIM_SCAN_ROWS, false)?;
        let mut truncated = candidates.len() >= CLAIM_SCAN_ROWS as usize;
        let mut conflicts = Vec::new();
        for candidate in candidates {
            if candidate.agent == holder {
                continue;
            }
            if !claim_globs_overlap_normalized(glob, &candidate.glob) {
                continue;
            }
            if conflicts.len() >= MAX_WHO_WORKERS {
                truncated = true;
                break;
            }
            conflicts.push(ClaimConflict {
                agent: candidate.agent,
                generation: candidate.generation,
                glob: candidate.glob,
            });
        }
        Ok((conflicts, truncated))
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
        if let Some(stored) = replay_claim_write(
            &tx,
            &request.id,
            principal,
            CLAIM_RELEASE_METHOD,
            &payload_digest,
        )? {
            let value = serde_json::from_value::<ClaimReleaseResponse>(decode_json(&stored)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        super::project_assignment::authorize_coordination(&tx, principal, Some(target_num))?;
        purge_expired_claims_tx(&tx, now)?;
        db!(tx.execute(
            "DELETE FROM claims WHERE agent = ?1 AND generation != ?2",
            params![target_num, target.generation],
        ));
        let removed = db!(tx.execute(
            "DELETE FROM claims WHERE agent = ?1 AND generation = ?2 AND glob = ?3",
            params![target_num, target.generation, glob],
        ));
        let events = if removed == 0 {
            Vec::new()
        } else {
            vec![insert_daemon_event(
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
            )?]
        };
        let value = ClaimReleaseResponse {
            agent: target.id.clone(),
            generation: target.generation,
            glob,
            released: removed,
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
        Ok(Change { value, events })
    }
}

/// Move all live exact-generation claims without renewing TTL or overwriting B.
pub(super) fn transfer_claims_tx(
    tx: &Transaction<'_>,
    from: &eliot_core::Agent,
    to: &eliot_core::Agent,
    policy: &eliot_core::SecretPolicy,
    now: i64,
    events: &mut Vec<eliot_core::Event>,
) -> Result<(Vec<String>, Vec<ClaimConflict>, bool)> {
    purge_expired_claims_tx(tx, now)?;
    let from_num = parse_agent_id(&from.id)?;
    let to_num = parse_agent_id(&to.id)?;
    let claims = Store::live_claims(
        tx,
        Some(from.project_scope()),
        Some(from_num),
        now,
        MAX_CLAIMS_PER_AGENT as i64 + 1,
        true,
    )?;
    let destination = Store::live_claims(
        tx,
        Some(to.project_scope()),
        Some(to_num),
        now,
        MAX_CLAIMS_PER_AGENT as i64 + 1,
        false,
    )?;
    if claims.len() + destination.len() > MAX_CLAIMS_PER_AGENT {
        return Err(Error::new(
            "claim_limit",
            "handoff exceeds destination claim limit",
        ));
    }
    let mut moved = Vec::new();
    for claim in claims {
        if destination
            .iter()
            .any(|existing| existing.glob == claim.glob)
        {
            return Err(Error::new(
                "bad_state",
                "destination already holds this exact claim",
            ));
        }
        if policy.contains_text(&claim.glob) {
            return Err(Error::new(
                "secret_in_payload",
                "claim identity contains a known secret",
            ));
        }
        tx.execute(
            "DELETE FROM claims WHERE agent=?1 AND generation=?2 AND glob=?3",
            params![from_num, from.generation, claim.glob],
        )
        .map_err(sql_error)?;
        // Expired destination rows with this primary key do not own a live claim.
        tx.execute(
            "DELETE FROM claims WHERE agent=?1 AND generation=?2 AND glob=?3 AND expires_at<=?4",
            params![to_num, to.generation, claim.glob, now],
        )
        .map_err(sql_error)?;
        tx.execute("INSERT INTO claims(agent,generation,glob,note,expires_at,created_at) VALUES (?1,?2,?3,?4,?5,?6)",
            params![to_num,to.generation,claim.glob,claim.note.as_deref().map(|s|policy.redact_text(s)),claim.expires_at,claim.created_at]).map_err(sql_error)?;
        for (num, generation, live) in [
            (from_num, from.generation, false),
            (to_num, to.generation, true),
        ] {
            events.push(insert_daemon_event(
                tx,
                now,
                "claim.changed",
                "coordination",
                Some(num),
                Some(generation),
                json!({"glob":claim.glob,"live":live,"handoff_from":from.id,"handoff_to":to.id}),
            )?);
        }
        moved.push(claim.glob);
    }
    let mut conflicts = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut truncated = false;
    for glob in &moved {
        let (found, capped) =
            Store::scan_claim_conflicts(tx, to.project_scope(), &to.id, glob, now)?;
        truncated |= capped;
        for conflict in found {
            if seen.insert((
                conflict.agent.clone(),
                conflict.generation,
                conflict.glob.clone(),
            )) {
                if conflicts.len() < MAX_WHO_WORKERS {
                    conflicts.push(conflict);
                } else {
                    truncated = true;
                }
            }
        }
    }
    Ok((moved, conflicts, truncated))
}

/// Load one Agent row and prove the caller's generation is still
/// current; a missing row or a moved generation reads stale.
fn load_current_claim_agent(
    connection: &rusqlite::Connection,
    num: i64,
    generation: i64,
) -> Result<eliot_core::Agent> {
    let current = load_agent_by_num(connection, num)?
        .ok_or_else(|| Error::new("stale_generation", "Agent generation is no longer current"))?;
    if current.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "Agent generation is no longer current",
        ));
    }
    Ok(current)
}

/// Stored result of an exact idempotent replay, or `None` for a fresh id. A
/// reused command id, or a write id bound to another principal, method or
/// payload, is `id_conflict`.
fn replay_claim_write(
    tx: &Transaction<'_>,
    id: &str,
    principal: &str,
    method: &str,
    payload_digest: &[u8],
) -> Result<Option<String>> {
    if command_binding(tx, id)?.is_some() {
        return Err(Error::new(
            "id_conflict",
            "claim write id is already used by a command",
        ));
    }
    crate::ensure_no_digest_id(
        tx,
        id,
        "claim write id is already used by a completion digest",
    )?;
    let Some((old_principal, old_method, old_digest, old_result)) = write_binding(tx, id)? else {
        return Ok(None);
    };
    if old_principal != principal || old_method != method || old_digest.as_slice() != payload_digest
    {
        return Err(Error::new(
            "id_conflict",
            "claim write id is bound to another principal, method, or payload",
        ));
    }
    Ok(Some(old_result))
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
