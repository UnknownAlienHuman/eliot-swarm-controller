//! Durable provider restriction holds and exact SEND probe claims.

use super::{
    Change, Error, ProviderProbeCandidate, Result, Store, canonical, command_binding,
    commit_transaction, decode_json, encode_json, load_agent_by_num, require_nonempty, sql_error,
    validate_id_age, write_binding,
};
mod admin;
use admin::{clear_hold_tx, get, get_many, list_page};

use eliot_core::{
    self as core, Agent, Command, CommandState, ProviderHold, ProviderHoldSource,
    ProviderHoldState, ProviderRestriction, ProviderRestrictionKind, TurnState,
    api::AgentProviderHold,
    api::{ProviderClearHoldRequest, ProviderClearHoldResponse, ProviderHoldsResponse},
    parse_agent_id,
};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::collections::BTreeMap;

const CLEAR_HOLD_METHOD: &str = "provider.clear_hold";
const INTERVENING_RESTRICTION_BATCH: i64 = 64;
const AUTOMATIC_PROBE_BACKOFF_MS: [i64; 3] = [60_000, 300_000, 1_800_000];
const BACKGROUND_PROBE_DEFER_MS: i64 = 300_000;
const PROVIDER_HOLD_RELEASE_BATCH: usize = 64;

pub(super) fn migrate_legacy_holds(tx: &Transaction<'_>) -> Result<()> {
    tx.execute(
        "UPDATE provider_holds
         SET retry_after = NULL,
             next_probe_at = CASE WHEN kind IN ('rate_limit', 'quota') AND state = 'held'
                  AND probe_outcome_unknown = 0 AND automatic_probes < ?1
                  THEN updated_at + CASE probe_attempts WHEN 0 THEN ?2 WHEN 1 THEN ?3 ELSE ?4 END END
         WHERE retry_after - updated_at > ?5",
        params![
            core::MAX_AUTOMATIC_PROVIDER_PROBES,
            AUTOMATIC_PROBE_BACKOFF_MS[0],
            AUTOMATIC_PROBE_BACKOFF_MS[1],
            AUTOMATIC_PROBE_BACKOFF_MS[2],
            core::MAX_PROVIDER_RESET_HORIZON_MS,
        ],
    )
    .map_err(sql_error)?;
    // Only the last refusal on the same uninterrupted hold can attribute a
    // legacy account scope. A probe on the old shared scope may belong to a
    // different harness. Preserve that uncertainty without transferring its claim.
    tx.execute_batch(
        "WITH last_refusals AS (
            SELECT json_extract(data, '$.scope') AS scope, MAX(seq) AS seq
            FROM events
            WHERE kind = 'provider.hold_updated' AND json_type(data, '$.turn_id') = 'text'
              AND json_extract(data, '$.scope') LIKE 'account:%'
            GROUP BY 1
         ), attributable AS (
            SELECT hold.scope, hold.scope || '.' || producer.harness AS new_scope,
                   producer.harness,
                   hold.probe_command IS NOT NULL AND probe_owner.id IS NULL AS foreign_probe
            FROM provider_holds hold
            JOIN last_refusals refusal ON refusal.scope = hold.scope
            JOIN events event ON event.seq = refusal.seq
            JOIN agents producer ON producer.id = event.agent AND producer.generation = event.generation
            LEFT JOIN commands probe ON probe.id = hold.probe_command
            LEFT JOIN agents probe_owner ON probe_owner.id = probe.target
              AND probe_owner.generation = probe.generation AND probe_owner.harness = producer.harness
              AND json_extract(probe_owner.profile_snapshot, '$.account') = substr(hold.scope, 9)
            WHERE json_extract(event.data, '$.held_since') = hold.held_since
              AND json_extract(producer.profile_snapshot, '$.account') = substr(hold.scope, 9)
         )
         UPDATE provider_holds AS hold
         SET scope = attributable.new_scope,
             source = json_object('harness', attributable.harness),
             probe_command = CASE WHEN attributable.foreign_probe THEN NULL ELSE hold.probe_command END,
             state = CASE WHEN attributable.foreign_probe THEN 'held' ELSE hold.state END,
             probe_outcome_unknown = MAX(hold.probe_outcome_unknown, attributable.foreign_probe)
         FROM attributable WHERE hold.scope = attributable.scope;

         UPDATE provider_holds SET next_probe_at = NULL
         WHERE scope LIKE 'account:%' AND instr(substr(scope, 9), '.') = 0;",
    )
    .map_err(sql_error)?;
    Ok(())
}

impl Store {
    /// Use unambiguous configured accounts for unclaimed legacy holds whose
    /// original refusal could not be attributed from retained evidence.
    pub fn attribute_legacy_provider_holds(
        &mut self,
        configured_scopes: &BTreeMap<String, String>,
    ) -> Result<()> {
        if configured_scopes.is_empty() {
            return Ok(());
        }
        self.ensure_writer_healthy()?;
        let updated = self
            .connection
            .execute(
                "WITH candidates AS (
                SELECT hold.scope, MIN(profile.value) AS new_scope,
                       COALESCE(MAX(CASE WHEN profile.key = hold.probe_profile
                                         THEN profile.key END), MIN(profile.key)) AS probe_profile
                FROM provider_holds hold
                JOIN json_each(?1) profile
                  ON substr(profile.value, 1, instr(profile.value, '.') - 1) = hold.scope
                WHERE hold.state = 'held' AND hold.probe_command IS NULL
                GROUP BY hold.scope HAVING COUNT(DISTINCT profile.value) = 1
             )
             UPDATE provider_holds AS hold
             SET scope = candidates.new_scope,
                 probe_profile = candidates.probe_profile,
                 next_probe_at = CASE WHEN hold.kind IN ('rate_limit', 'quota')
                     AND hold.retry_after IS NULL AND hold.probe_outcome_unknown = 0
                     AND hold.automatic_probes < ?2
                     THEN hold.updated_at + CASE hold.probe_attempts
                         WHEN 0 THEN ?3 WHEN 1 THEN ?4 ELSE ?5 END END
             FROM candidates WHERE hold.scope = candidates.scope
               AND NOT EXISTS (SELECT 1 FROM provider_holds WHERE scope = candidates.new_scope)",
                params![
                    encode_json(configured_scopes)?,
                    core::MAX_AUTOMATIC_PROVIDER_PROBES,
                    AUTOMATIC_PROBE_BACKOFF_MS[0],
                    AUTOMATIC_PROBE_BACKOFF_MS[1],
                    AUTOMATIC_PROBE_BACKOFF_MS[2],
                ],
            )
            .map(|_| ())
            .map_err(sql_error);
        self.fault_on_persistence_error(&updated);
        updated
    }

    /// Bind the final owner notice to the live hold in the same transaction
    /// that admits its ordinary local notify command.
    pub fn admit_provider_attention_notices(
        &mut self,
        now: i64,
        policy: &core::SecretPolicy,
    ) -> Result<Vec<super::Admission>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let due = {
                let mut query = tx.prepare(
                    "SELECT scope, kind, probe_outcome_unknown FROM provider_holds
                     WHERE attention_notified = 0 AND state = 'held'
                       AND (probe_outcome_unknown = 1 OR (retry_after IS NULL AND automatic_probes >= ?1))
                     ORDER BY held_since, scope LIMIT 4",
                ).map_err(sql_error)?;
                query
                    .query_map([core::MAX_AUTOMATIC_PROVIDER_PROBES], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, bool>(2)?,
                        ))
                    })
                    .map_err(sql_error)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql_error)?
            };
            let mut admitted = Vec::with_capacity(due.len());
            for (scope, kind, unknown) in due {
                let reason = if unknown {
                    "last probe is unresolved"
                } else {
                    "probe budget exhausted"
                };
                let mut request = core::SubmitCommand::new(ulid::Ulid::new().to_string(), "notify");
                let message = policy.redact_text(&format!(
                    "Provider {kind} hold for {} needs attention ({reason}).",
                    core::bound_chars(&scope, 256),
                ));
                request.args = json!({"title": "Provider attention", "message": message});
                let admission = super::admission::submit_tx(
                    &tx,
                    "owner",
                    request,
                    None,
                    now,
                    false,
                    self.hierarchy_limits,
                    self.capacity_policy.as_deref(),
                )?;
                tx.execute(
                    "UPDATE provider_holds SET attention_notified = 1 WHERE scope = ?1",
                    [&scope],
                )
                .map_err(sql_error)?;
                admitted.push(admission);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(admitted)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Return the Core-owned bounded page, ordered by the frozen provider scope.
    pub fn list_provider_holds(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProviderHoldsResponse> {
        list_page(&self.connection, after, limit)
    }

    /// Read all committed holds for the supplied exact scopes in one query.
    pub fn provider_holds_for_scopes(
        &self,
        scopes: &[String],
    ) -> Result<BTreeMap<String, ProviderHold>> {
        self.ensure_committed_reads()?;
        get_many(&self.connection, scopes)
    }

    pub fn model_mismatch_hold(&self, profile: &str) -> Result<Option<ProviderHold>> {
        self.ensure_committed_reads()?;
        Ok(get(&self.connection, &core::model_hold_scope(profile))?
            .filter(|hold| hold.kind == ProviderRestrictionKind::ModelMismatch))
    }

    /// Hydrate a target only when its frozen profile has a mismatch hold.
    pub fn model_hold_target(&self, id: &str, generation: Option<i64>) -> Result<Option<Agent>> {
        self.ensure_committed_reads()?;
        let id = parse_agent_id(id)?;
        let held = self
            .connection
            .query_row(
                "SELECT EXISTS (
                SELECT 1 FROM agents AS a
                JOIN provider_holds AS h ON h.scope = ?3 || a.profile
                WHERE a.id = ?1 AND (?2 IS NULL OR a.generation = ?2)
                  AND a.observe_only = 0 AND h.kind = 'model_mismatch'
            )",
                params![id, generation, core::model_hold_scope("")],
                |row| row.get::<_, bool>(0),
            )
            .map_err(sql_error)?;
        if held {
            load_agent_by_num(&self.connection, id)
        } else {
            Ok(None)
        }
    }

    /// Check for a held selector member before hydrating the authorized tree.
    /// Admission expands and checks the selector again in its transaction.
    pub fn model_hold_selector_targets(
        &self,
        principal: &str,
        selector: &str,
    ) -> Result<Vec<Agent>> {
        self.ensure_committed_reads()?;
        let root = core::api::parse_tree_selector(selector)?;
        let ids = super::hierarchy::subtree_member_nums(
            &self.connection,
            root,
            self.hierarchy_limits.max_selector_targets,
        )?;
        let held = self
            .connection
            .query_row(
                "SELECT EXISTS (
                SELECT 1 FROM json_each(?1) AS member
                JOIN agents AS a ON a.id = member.value
                JOIN provider_holds AS h ON h.scope = ?2 || a.profile
                WHERE a.observe_only = 0 AND a.state NOT IN ('stopped', 'failed', 'lost')
                  AND h.kind = 'model_mismatch'
            )",
                params![encode_json(&ids)?, core::model_hold_scope("")],
                |row| row.get::<_, bool>(0),
            )
            .map_err(sql_error)?;
        if !held {
            return Ok(Vec::new());
        }
        let mut agents = super::hierarchy::snapshot_selector_members(
            &self.connection,
            principal,
            selector,
            self.hierarchy_limits,
        )?;
        agents.retain(|agent| !agent.state.is_terminal() && !agent.observe_only);
        Ok(agents)
    }

    /// Refresh only the exact mismatch row read before checking the
    /// installed runtime and settings.
    pub fn clear_changed_model_hold(
        &mut self,
        expected: &ProviderHold,
        revision: &str,
        now: i64,
    ) -> Result<Vec<core::Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let Some(mismatch) = expected
                .source
                .as_ref()
                .and_then(|source| source.model_mismatch.as_ref())
            else {
                return Ok(Vec::new());
            };
            // A hold observed without readable metadata gets one attempt
            // when a revision becomes available.
            if mismatch.revision.as_deref() == Some(revision) {
                return Ok(Vec::new());
            }
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if get(&tx, &expected.scope)?.as_ref() != Some(expected)
                || !admin::clear_hold_tx(&tx, &expected.scope, None)?
            {
                return Ok(Vec::new());
            }
            let event = super::insert_daemon_event(
                &tx,
                now,
                "provider.hold_cleared",
                "provider",
                None,
                None,
                json!({"scope": expected.scope, "reason": "model_route_changed"}),
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(vec![event])
        })();
        // Refresh is optional. A failed pre-commit operation can leave the
        // writer usable after rollback. Uncertain commits stay fenced.
        if !self.connection.is_autocommit() {
            self.write_faulted.set(true);
        }
        result
    }

    pub fn provider_probe_attention(&self, limit: usize) -> Result<Vec<ProviderHold>> {
        self.ensure_committed_reads()?;
        admin::attention(&self.connection, limit)
    }

    /// Keyset page of ended maintenance Agents, independent of the hold
    /// result. Successful probes also need their scratch and session removed.
    pub fn ended_provider_probes(&self, after: i64) -> Result<Vec<(i64, String)>> {
        self.ensure_committed_reads()?;
        let harnesses: Vec<_> = core::api::SUPPORTED_HARNESSES
            .iter()
            .copied()
            .filter(|harness| core::api::HarnessCapabilities::of(harness).provider_probe)
            .collect();
        let mut query = self
            .connection
            .prepare(
                "SELECT id FROM agents WHERE id > ?1 AND chat = 1 AND parent IS NULL
               AND observe_only = 0 AND project = ''
               AND harness IN (SELECT value FROM json_each(?2))
               AND state IN ('stopped','failed','lost')
               AND json_extract(profile_snapshot, '$._provider_probe') = 1
             ORDER BY id LIMIT 64",
            )
            .map_err(sql_error)?;
        let rows = query
            .query_map(params![after, encode_json(&json!(harnesses))?], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(sql_error)?;
        rows.map(|row| {
            row.map(|id| (id, super::format_agent_id(id)))
                .map_err(sql_error)
        })
        .collect()
    }

    /// With custody closed, detach the exact interrupted maintenance
    /// claim. A missing terminal fact pauses automation; it is not success.
    pub fn reconcile_ended_provider_probe(
        &mut self,
        agent_id: &str,
        now: i64,
    ) -> Result<Vec<core::Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let num = parse_agent_id(agent_id)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let Some(agent) = load_agent_by_num(&tx, num)? else {
                return Err(Error::new("not_found", "provider probe Agent disappeared"));
            };
            if !agent.is_automatic_provider_probe()
                || !matches!(
                    agent.state,
                    core::AgentState::Stopped | core::AgentState::Failed | core::AgentState::Lost
                )
                || agent.active_turn.is_some()
                || !agent.pending.is_empty()
            {
                return Err(Error::new("busy", "provider probe has not ended"));
            }
            if super::agent_lifecycle::custody_blocker_tx(&tx, num, false, None)?.is_some() {
                return Err(Error::new(
                    "busy",
                    "provider probe custody is still draining",
                ));
            }
            let linked: Option<(String, String, i64)> = tx.query_row(
                "SELECT c.id,c.state,c.generation FROM commands c JOIN commands p ON p.id=c.parent_command
                 WHERE c.target=?1 AND c.verb='send' AND c.probe=1 AND c.link_kind='send'
                   AND c.ordinal=0 AND p.target=c.target AND p.verb='launch'
                   AND json_type(p.args,'$._provider_probe')='text'",
                [num], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
            ).optional().map_err(sql_error)?;
            let mut events = Vec::new();
            if let Some((id, state, generation)) = linked {
                if matches!(state.as_str(), "accepted" | "queued") {
                    super::cancel_pending_command_tx(
                        &tx,
                        num,
                        generation,
                        &id,
                        "provider_probe_agent_ended",
                        now,
                        &mut events,
                    )?;
                }
                let scope = core::frozen_provider_scope(&agent)?;
                events.extend(pause_unknown_probe_tx(
                    &tx, &scope, &id, agent_id, generation, now,
                )?);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Pause only the exact linked probe after its generation has ended and
    /// its native custody has drained, or after Owner-proven turn inactivity.
    /// A task-bearing Agent and its commands, turn and workspace stay intact.
    pub fn pause_unresolved_launch_probes(&mut self, now: i64) -> Result<Vec<core::Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self.connection.transaction().map_err(sql_error)?;
            let candidates = {
                let mut statement = tx
                    .prepare(
                        "SELECT h.scope, c.id, c.target, c.generation
                     FROM provider_holds h
                     JOIN commands c ON c.id = h.probe_command
                     JOIN commands p ON p.id = c.parent_command AND p.target = c.target
                     JOIN agents a ON a.id = c.target
                     WHERE h.state = 'probing' AND c.verb = 'send' AND c.probe = 1
                       AND c.link_kind = 'send' AND c.ordinal = 0 AND c.generation = 1
                       AND p.verb = 'launch' AND p.generation = c.generation
                       AND json_type(p.args, '$._provider_probe') IS NULL
                       AND a.observe_only = 0
                       AND c.state IN ('dispatched','unknown','applied')
                       AND (
                           EXISTS (SELECT 1 FROM turns t WHERE t.agent = a.id
                             AND t.generation = c.generation AND t.turn_id = c.turn_id
                             AND t.state = 'completed' AND t.resolution_source = 'owner')
                           OR (
                               (a.generation > c.generation OR
                                (a.generation = c.generation AND a.active_turn IS NULL
                                 AND a.state IN ('stopped','failed','lost')))
                               AND NOT EXISTS (
                                   SELECT 1 FROM processes o WHERE o.state IN ('running','unknown')
                                   AND o.owner IN ('agent:a'||a.id||'#'||c.generation,
                                                   'workspace:a'||a.id||'#'||c.generation))
                               AND NOT EXISTS (
                                   SELECT 1 FROM source_state s WHERE s.agent = a.id
                                   AND s.generation = c.generation AND s.state = 'open'
                                   AND s.source NOT IN ('daemon','os','git','workspace_process'))
                           )
                       )
                     ORDER BY h.scope LIMIT 64",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                            row.get::<_, i64>(3)?,
                        ))
                    })
                    .map_err(sql_error)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql_error)?
            };
            let mut events = Vec::with_capacity(candidates.len());
            for (scope, id, target, generation) in candidates {
                let agent = super::format_agent_id(target);
                events.extend(pause_unknown_probe_tx(
                    &tx, &scope, &id, &agent, generation, now,
                )?);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Clear known rate-limit/quota resets in one bounded writer transaction.
    /// Every released scope receives a durable provider event in that commit.
    pub fn release_expired_provider_holds(
        &mut self,
        now: i64,
        limit: usize,
    ) -> Result<Vec<core::Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let limit = limit.min(PROVIDER_HOLD_RELEASE_BATCH);
            if limit == 0 {
                return Ok(Vec::new());
            }
            let due = {
                let mut statement = self
                    .connection
                    .prepare(
                        "SELECT scope, kind, retry_after FROM provider_holds
                         WHERE state = 'held' AND kind IN ('rate_limit','quota')
                           AND NOT (scope LIKE 'account:%' AND instr(substr(scope, 9), '.') = 0)
                           AND retry_after IS NOT NULL AND retry_after <= ?1
                         ORDER BY retry_after, scope LIMIT ?2",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(params![now, limit as i64], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, i64>(2)?,
                        ))
                    })
                    .map_err(sql_error)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql_error)?
            };
            if due.is_empty() {
                return Ok(Vec::new());
            }
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let mut events = Vec::with_capacity(due.len());
            for (scope, kind, retry_after) in due {
                let changed = tx
                    .execute(
                        "DELETE FROM provider_holds WHERE scope = ?1 AND state = 'held'
                           AND kind IN ('rate_limit','quota') AND retry_after = ?2
                           AND retry_after <= ?3",
                        params![scope, retry_after, now],
                    )
                    .map_err(sql_error)?;
                if changed == 1 {
                    events.push(super::insert_daemon_event(
                        &tx,
                        now,
                        "provider.hold_cleared",
                        "provider",
                        None,
                        None,
                        json!({
                            "scope": scope,
                            "kind": kind,
                            "retry_after": retry_after,
                            "reason": "provider_reset_elapsed",
                        }),
                    )?);
                }
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Return a bounded ordered page of unknown-reset holds due for an
    /// automatic native probe. The Store transaction that admits each
    /// linked probe SEND claims the scope before any process is spawned.
    pub fn due_provider_probes(
        &self,
        now: i64,
        limit: usize,
    ) -> Result<Vec<ProviderProbeCandidate>> {
        self.ensure_committed_reads()?;
        let limit = limit.min(super::PROVIDER_PROBE_BATCH_LIMIT);
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut statement = self
            .connection
            .prepare(
                "SELECT scope, probe_profile FROM provider_holds
                 WHERE state = 'held' AND probe_command IS NULL
                   AND kind IN ('rate_limit','quota') AND retry_after IS NULL
                   AND next_probe_at IS NOT NULL AND next_probe_at <= ?1
                   AND probe_profile IS NOT NULL
                   AND automatic_probes < ?3 AND probe_outcome_unknown = 0
                   AND (updated_at < next_probe_at OR updated_at <= ?4)
                 ORDER BY next_probe_at, scope LIMIT ?2",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![
                    now,
                    limit as i64,
                    core::MAX_AUTOMATIC_PROVIDER_PROBES,
                    now.saturating_sub(BACKGROUND_PROBE_DEFER_MS)
                ],
                |row| {
                    Ok(ProviderProbeCandidate {
                        scope: row.get(0)?,
                        profile: row.get(1)?,
                    })
                },
            )
            .map_err(sql_error)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)
    }

    /// Pace background preparation failures without moving the deadline for
    /// a caller's launch or counting the failure as a provider attempt.
    pub fn defer_provider_probe(
        &mut self,
        scope: &str,
        now: i64,
        reason: &str,
    ) -> Result<Vec<core::Event>> {
        self.ensure_writer_healthy()?;
        let reason = match reason {
            "profile_unavailable" | "unsafe_profile" | "probe_admission_failed" => reason,
            _ => "probe_unavailable",
        };
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let next_probe_at: Option<i64> = tx
                .query_row(
                    "UPDATE provider_holds SET updated_at = ?1
                     WHERE scope = ?2 AND state = 'held' AND probe_command IS NULL
                       AND kind IN ('rate_limit','quota') AND retry_after IS NULL
                       AND next_probe_at <= ?1
                       AND automatic_probes < ?3 AND probe_outcome_unknown = 0
                       AND (updated_at < next_probe_at OR updated_at <= ?4)
                     RETURNING next_probe_at",
                    params![
                        now,
                        scope,
                        core::MAX_AUTOMATIC_PROVIDER_PROBES,
                        now.saturating_sub(BACKGROUND_PROBE_DEFER_MS)
                    ],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            let mut events = Vec::new();
            if let Some(next_probe_at) = next_probe_at {
                events.push(super::insert_daemon_event(
                    &tx,
                    now,
                    "provider.hold_probe_deferred",
                    "provider",
                    None,
                    None,
                    json!({
                        "scope": scope,
                        "next_probe_at": next_probe_at,
                        "background_retry_at": now.saturating_add(BACKGROUND_PROBE_DEFER_MS),
                        "reason": reason,
                    }),
                )?);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Resolve the holds for the distinct scopes captured by one status page.
    /// Scope and probe-command identity remain owner-only.
    pub(super) fn agent_provider_holds(
        &self,
        scopes: &[String],
    ) -> Result<BTreeMap<String, AgentProviderHold>> {
        Ok(get_many(&self.connection, scopes)?
            .into_iter()
            .map(|(scope, hold)| {
                (
                    scope,
                    AgentProviderHold {
                        kind: hold.kind,
                        retry_after: hold.retry_after,
                        next_probe_at: hold.next_probe_at,
                        state: hold.state,
                        updated_at: hold.updated_at,
                        held_since: hold.held_since,
                        waiting_for_active_turn: false,
                    },
                )
            })
            .collect())
    }

    /// Clear a hold using the Core request's closed corrective evidence and
    /// compare-and-swap probe binding. The mutation result and delete commit
    /// together so retries return the original outcome.
    pub fn clear_provider_hold(
        &mut self,
        principal: &str,
        request: &ProviderClearHoldRequest,
        now: i64,
    ) -> Result<Change<ProviderClearHoldResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.clear_provider_hold_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn clear_provider_hold_inner(
        &mut self,
        principal: &str,
        request: &ProviderClearHoldRequest,
        now: i64,
    ) -> Result<Change<ProviderClearHoldResponse>> {
        request.validate()?;
        if principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "provider hold correction requires the owner principal",
            ));
        }
        require_nonempty(principal, "principal")?;
        let semantic_payload = json!({
            "scope": request.scope,
            "corrective_evidence": request.corrective_evidence.as_str(),
            "expected_probe_command": request.expected_probe_command,
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;

        if command_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "provider hold write id is already used by a command",
            ));
        }
        crate::ensure_no_digest_id(
            &tx,
            &request.id,
            "provider hold write id is already used by a completion digest",
        )?;
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, &request.id)?
        {
            if old_principal != principal
                || old_method != CLEAR_HOLD_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "provider hold write id is bound to another principal, method, or payload",
                ));
            }
            let value =
                serde_json::from_value::<ProviderClearHoldResponse>(decode_json(&old_result)?)
                    .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        let cleared = match clear_hold_tx(
            &tx,
            &request.scope,
            request.expected_probe_command.as_deref(),
        ) {
            Ok(cleared) => cleared,
            Err(error)
                if error.code == "bad_state"
                    && request.expected_probe_command.is_some()
                    && get(&tx, &request.scope)?.is_none() =>
            {
                false
            }
            Err(error) => return Err(error),
        };
        let value = ProviderClearHoldResponse {
            id: request.id.clone(),
            scope: request.scope.clone(),
            cleared,
        };
        request.validate_response(&value)?;
        let events = if cleared {
            vec![super::insert_daemon_event(
                &tx,
                now,
                "provider.hold_cleared",
                "provider",
                None,
                None,
                json!({
                    "scope": request.scope,
                    "write_id": request.id,
                    "corrective_evidence": request.corrective_evidence.as_str(),
                }),
            )?]
        } else {
            Vec::new()
        };
        let result_json = encode_json(&value)?;
        tx.execute(
            "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                request.id,
                principal,
                CLEAR_HOLD_METHOD,
                payload_digest,
                result_json,
                now,
            ],
        )
        .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change { value, events })
    }
}

pub(super) fn require_unheld_tx(tx: &Transaction<'_>, agent: &Agent) -> Result<()> {
    require_model_unheld(tx, agent)?;
    let scope = core::frozen_provider_scope(agent)?;
    if get(tx, &scope)?.is_some() {
        return Err(Error::new(
            "provider_held",
            format!("provider scope {scope} is held pending owner correction or an eligible probe"),
        ));
    }
    Ok(())
}

pub(super) fn require_model_unheld(connection: &rusqlite::Connection, agent: &Agent) -> Result<()> {
    if let Some(hold) = get(connection, &core::model_hold_scope(&agent.profile))?
        && let Some(mismatch) = hold.source.and_then(|source| source.model_mismatch)
    {
        return Err(
            Error::new("provider_held", mismatch.reason()).with_where("provider.model_mismatch")
        );
    }
    Ok(())
}

pub(super) fn require_automatic_probe_due_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    now: i64,
) -> Result<()> {
    require_model_unheld(tx, agent)?;
    let scope = core::frozen_provider_scope(agent)?;
    if !get(tx, &scope)?.is_some_and(|hold| automatic_probe_due(&hold, now)) {
        return Err(provider_held());
    }
    let probe_profile: Option<String> = tx
        .query_row(
            "SELECT probe_profile FROM provider_holds WHERE scope = ?1",
            [&scope],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?
        .flatten();
    if probe_profile.as_deref() != Some(agent.profile.as_str()) {
        return Err(provider_held());
    }
    Ok(())
}

/// Check a fresh launch against its frozen scope. A due time is supplied only
/// when admission can atomically bind the launch's linked SEND as the probe.
pub(super) fn launch_probe(
    connection: &rusqlite::Connection,
    agent: &Agent,
    at: Option<i64>,
) -> Result<bool> {
    require_model_unheld(connection, agent)?;
    let scope = core::frozen_provider_scope(agent)?;
    match get(connection, &scope)? {
        None => Ok(false),
        Some(hold) if at.is_some_and(|now| automatic_probe_due(&hold, now)) => Ok(true),
        Some(_) => Err(Error::new(
            "provider_held",
            format!("provider scope {scope} is held pending owner correction or an eligible probe"),
        )),
    }
}

fn automatic_probe_due(hold: &ProviderHold, now: i64) -> bool {
    matches!(
        hold.kind,
        ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota
    ) && hold.state == ProviderHoldState::Held
        && hold.probe_command.is_none()
        && hold.retry_after.is_none()
        && !automatic_probe_paused(hold)
        && hold.next_probe_at.is_some_and(|due_at| due_at <= now)
}

pub(super) fn profile_held(connection: &rusqlite::Connection, agent: &Agent) -> Result<bool> {
    Ok(
        get(connection, &core::model_hold_scope(&agent.profile))?.is_some()
            || get(connection, &core::frozen_provider_scope(agent)?)?.is_some(),
    )
}

/// Bind a probe claim only after its accepted command row exists in this same
/// admission transaction. Ordinary sends remain pre-binding rejections.
pub(super) fn admit_send_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    command_id: &str,
    probe: bool,
    automatic_generation: Option<i64>,
    now: i64,
) -> Result<Vec<core::Event>> {
    require_model_unheld(tx, agent)?;
    let automatic = automatic_generation.is_some();
    // A linked launch SEND belongs to the reserved generation even while the
    // Agent projection is still Created at generation zero.
    let generation = automatic_generation.unwrap_or(agent.generation);
    if !automatic
        && agent.profile_snapshot.get(core::PROVIDER_PROBE_LAUNCH_ARG) == Some(&Value::Bool(true))
    {
        return Err(Error::new(
            "not_allowed",
            "provider probe sessions accept only their bound input",
        ));
    }
    let scope = core::frozen_provider_scope(agent)?;
    let Some(hold) = get(tx, &scope)? else {
        return if probe {
            Err(Error::new(
                "bad_state",
                "a provider probe requires an eligible rate-limit or quota hold",
            ))
        } else {
            Ok(Vec::new())
        };
    };

    if !probe {
        return Err(provider_held());
    }
    if !matches!(
        hold.kind,
        ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota
    ) {
        return Err(provider_held());
    }
    if hold.state != ProviderHoldState::Held || hold.probe_command.is_some() {
        return Err(provider_held());
    }
    if hold
        .retry_after
        .is_some_and(|retry_after| retry_after > now)
    {
        return Err(Error::new(
            "bad_state",
            "provider probe is not eligible before retry_after",
        ));
    }
    if automatic && !automatic_probe_due(&hold, now) {
        return Err(Error::new(
            "bad_state",
            "automatic provider probe is not due",
        ));
    }

    let agent_num = parse_agent_id(&agent.id)?;
    if !automatic && automatic_probe_paused(&hold) {
        let owner: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM commands WHERE id = ?1 AND principal = 'owner')",
                [command_id],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if !owner {
            return Err(Error::new(
                "not_allowed",
                "this provider hold requires Owner action",
            ));
        }
    }
    let command_is_probe: bool = tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM commands
                 WHERE id = ?1 AND target = ?2 AND generation = ?3 AND verb = 'send'
                   AND probe = 1 AND state IN ('accepted','queued')
             )",
            params![command_id, agent_num, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if !command_is_probe {
        return Err(Error::new(
            "source_integrity",
            "provider probe claim is not bound to its admitted SEND",
        ));
    }
    let spent = automatic && hold.automatic_probes + 1 == core::MAX_AUTOMATIC_PROVIDER_PROBES;
    let changed = tx
        .execute(
            "UPDATE provider_holds
             SET state = 'probing', probe_command = ?1, updated_at = ?2,
                 automatic_probes = automatic_probes + ?4,
                 next_probe_at = CASE WHEN ?5 THEN NULL ELSE next_probe_at END
             WHERE scope = ?3 AND kind IN ('rate_limit','quota')
               AND state = 'held' AND probe_command IS NULL
               AND (retry_after IS NULL OR retry_after <= ?2)
               AND (?4 = 0 OR (automatic_probes < ?6 AND probe_outcome_unknown = 0))",
            params![
                command_id,
                now,
                scope,
                automatic,
                spent,
                core::MAX_AUTOMATIC_PROVIDER_PROBES
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(provider_held());
    }
    let mut events = vec![
        super::insert_daemon_event(
            tx,
            now,
            "provider.hold_updated",
            "provider",
            Some(agent_num),
            Some(generation),
            json!({
                "scope": scope,
                "kind": hold.kind.as_str(),
                "state": ProviderHoldState::Probing.as_str(),
                "probe_command": command_id,
            }),
        )?,
        super::insert_daemon_event(
            tx,
            now,
            "provider.hold_probe_started",
            "provider",
            None,
            None,
            json!({
                "scope": scope,
                "probe_command": command_id,
                "automatic": automatic,
            }),
        )?,
    ];
    if spent {
        events.push(super::insert_daemon_event(
            tx,
            now,
            "provider.hold_probe_budget_exhausted",
            "provider",
            None,
            None,
            json!({
                "scope": scope, "automatic_probes": core::MAX_AUTOMATIC_PROVIDER_PROBES,
                "probe_command": command_id, "owner_attention": true,
            }),
        )?);
    }
    Ok(events)
}

/// Recheck current hold state at the effect boundary. A probe must match its
/// already-persisted claim exactly; this function never reacquires a claim.
pub(super) fn require_dispatch_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    command: &Command,
) -> Result<()> {
    require_model_unheld(tx, agent)?;
    let scope = core::frozen_provider_scope(agent)?;
    let background_launch =
        command.verb == "launch" && command.args.get(core::PROVIDER_PROBE_LAUNCH_ARG).is_some();
    if background_launch
        && (command.principal != "owner"
            || !agent.chat
            || command.args[core::PROVIDER_PROBE_LAUNCH_ARG].as_str() != Some(scope.as_str()))
    {
        return Err(Error::new(
            "not_allowed",
            "provider probe launch lost its daemon-owned chat binding",
        ));
    }
    let Some(hold) = get(tx, &scope)? else {
        if command.probe || background_launch {
            return Err(Error::new(
                "bad_state",
                "provider probe lost its durable hold claim",
            ));
        }
        return Ok(());
    };

    if hold.state == ProviderHoldState::Probing
        && matches!(
            hold.kind,
            ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota
        )
    {
        if command.probe && hold.probe_command.as_deref() == Some(command.id.as_str()) {
            return Ok(());
        }
        if command.verb == "launch" {
            let owns_probe: bool = tx
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM commands WHERE id = ?1 AND parent_command = ?2
                           AND link_kind = 'send' AND verb = 'send' AND probe = 1
                           AND target = ?3 AND generation = 1 AND state IN ('accepted','queued')
                     )",
                    params![hold.probe_command, command.id, parse_agent_id(&agent.id)?],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if owns_probe {
                return Ok(());
            }
        }
    }
    Err(provider_held())
}

/// Persist structured restrictions only from the exact controlled terminal
/// effect; resolve a probe claim only from its exact terminal command binding.
pub(super) fn terminal_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    effect: &core::Effect,
    native_event: &core::NativeEvent,
    now: i64,
) -> Result<Vec<core::Event>> {
    let core::Effect::TurnTerminal {
        command_id,
        turn_id,
        state,
        status,
        outcome,
        ..
    } = effect
    else {
        return Ok(Vec::new());
    };
    // A live-host projection gap settles the turn unknown with no provider
    // signal: no restriction is recorded and no probe claim resolves.
    if native_event.kind == "turn.unknown"
        || *state == TurnState::Unknown
        || outcome.is_some_and(|outcome| outcome == core::TurnOutcome::Unknown)
        || (outcome.is_none()
            && core::TurnOutcome::classify_legacy_status(status.as_deref())
                == core::TurnOutcome::Unknown)
    {
        return Ok(Vec::new());
    }
    let agent_num = parse_agent_id(&agent.id)?;
    let generation = agent.generation;
    if native_event.kind != "turn.completed"
        || native_event.data.get("command_id").and_then(Value::as_str) != command_id.as_deref()
        || native_event.data.get("turn_id").and_then(Value::as_str) != Some(turn_id.as_str())
        || agent.observe_only
    {
        return Err(Error::new(
            "source_integrity",
            "provider hold fact is not bound to the controlled terminal turn",
        ));
    }
    // A self-started native turn has no command: its refusal still records
    // the restriction, and only a bound send can resolve a probe claim.
    let command = command_id
        .as_deref()
        .map(|command_id| -> Result<_> {
            let command = super::load_command_record(tx, command_id)?.ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "provider hold terminal references an unknown command",
                )
            })?;
            if command.target != Some(agent_num)
                || command.command.generation != Some(generation)
                || command.command.verb != "send"
                || command.command.turn_id.as_deref() != Some(turn_id.as_str())
                || command.state != CommandState::Applied
            {
                return Err(Error::new(
                    "source_integrity",
                    "provider hold terminal is not bound to the exact applied SEND",
                ));
            }
            Ok((command_id, command))
        })
        .transpose()?;

    let restriction_value = native_event.data.get("provider_restriction");
    let restriction = restriction_value
        .and_then(|value| serde_json::from_value::<ProviderRestriction>(value.clone()).ok())
        .filter(|restriction| {
            restriction.kind != ProviderRestrictionKind::ModelMismatch
                && restriction.validate().is_ok()
        });
    let mut events = Vec::new();
    if let Some(restriction) = &restriction
        && let Some(event) = record_restriction_tx(tx, agent, turn_id, restriction, None, now)?
    {
        let reaction = core::AttentionStep {
            incident_id: format!("provider-refusal-{}", event.seq),
            step: "hold_provider".to_owned(),
            outcome: event.data["state"].as_str().unwrap_or("unknown").to_owned(),
            command_id: None,
            turn_id: Some(turn_id.clone()),
        };
        events.push(event);
        events.push(super::attention_steps::insert_attention_step(
            tx, agent_num, generation, &reaction, now,
        )?);
    }

    let Some((command_id, command)) = command else {
        return Ok(events);
    };
    if command.command.probe {
        let scope = core::frozen_provider_scope(agent)?;
        let Some(hold) = get(tx, &scope)? else {
            return Ok(events);
        };
        if hold.state != ProviderHoldState::Probing
            || hold.probe_command.as_deref() != Some(command_id)
        {
            return Ok(events);
        }

        let successful = *state == TurnState::Completed
            && status.as_deref() == Some("completed")
            && restriction_value.is_none()
            && matches!(
                hold.kind,
                ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota
            )
            && !has_intervening_same_scope_restriction_tx(
                tx,
                &scope,
                command_id,
                agent_num,
                generation,
                native_event,
            )?;
        if successful {
            let changed = tx
                .execute(
                    "DELETE FROM provider_holds
                     WHERE scope = ?1 AND state = 'probing' AND probe_command = ?2
                       AND kind IN ('rate_limit','quota')",
                    params![scope, command_id],
                )
                .map_err(sql_error)?;
            if changed != 1 {
                return Err(Error::new(
                    "source_integrity",
                    "provider probe hold changed before successful terminal commit",
                ));
            }
            events.push(super::insert_daemon_event(
                tx,
                now,
                "provider.hold_cleared",
                "provider",
                None,
                None,
                json!({
                    "scope": scope,
                    "probe_command": command_id,
                    "reason": "probe_succeeded",
                }),
            )?);
            events.push(super::insert_daemon_event(
                tx,
                now,
                "provider.hold_probe_finished",
                "provider",
                None,
                None,
                json!({
                    "scope": scope,
                    "probe_command": command_id,
                    "outcome": "succeeded",
                }),
            )?);
        } else {
            if release_probe_claim_tx(tx, command_id, now)? {
                let next_probe_at = reschedule_failed_probe_tx(tx, &scope, now)?;
                events.push(super::insert_daemon_event(
                    tx,
                    now,
                    "provider.hold_updated",
                    "provider",
                    Some(agent_num),
                    Some(generation),
                    json!({
                        "scope": scope,
                        "probe_command": command_id,
                        "state": ProviderHoldState::Held.as_str(),
                        "reason": restriction.as_ref().map_or("probe_terminal", |r| r.kind.as_str()),
                        "next_probe_at": next_probe_at,
                    }),
                )?);
                events.push(super::insert_daemon_event(
                    tx,
                    now,
                    "provider.hold_probe_finished",
                    "provider",
                    None,
                    None,
                    json!({
                        "scope": scope,
                        "probe_command": command_id,
                        "outcome": "held",
                        "next_probe_at": next_probe_at,
                    }),
                )?);
            }
        }
    }
    Ok(events)
}

/// A known terminal command result releases only that exact probe claim. An
/// unknown command keeps the claim, and applied sends are resolved by turn fact.
pub(super) fn command_terminal_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    previous: CommandState,
    state: CommandState,
    now: i64,
) -> Result<()> {
    if state == CommandState::Rejected
        || state == CommandState::Failed
        || (state == CommandState::Cancelled
            && matches!(previous, CommandState::Accepted | CommandState::Queued))
    {
        let launch_probe: Option<String> = tx
            .query_row(
                "SELECT id FROM commands WHERE parent_command = ?1 AND link_kind = 'send'
                   AND probe = 1 LIMIT 1",
                [command_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        let (probe_id, outcome) = match launch_probe {
            Some(probe_id) => (probe_id, "launch_failed"),
            None => (command_id.to_owned(), "send_failed"),
        };
        let scope: Option<String> = tx
            .query_row(
                "SELECT scope FROM provider_holds WHERE probe_command = ?1 AND state = 'probing'",
                [&probe_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        if let Some(scope) = scope
            && release_probe_claim_tx(tx, &probe_id, now)?
        {
            let next_probe_at = reschedule_failed_probe_tx(tx, &scope, now)?;
            super::insert_daemon_event(
                tx,
                now,
                "provider.hold_probe_finished",
                "provider",
                None,
                None,
                json!({
                    "scope": scope,
                    "probe_command": probe_id,
                    "outcome": outcome,
                    "next_probe_at": next_probe_at,
                }),
            )?;
        }
    }
    Ok(())
}

pub(super) fn model_mismatch_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    event: &core::NativeEvent,
    now: i64,
) -> Result<Option<core::Event>> {
    let Some(evidence) = event.data.get("model_mismatch") else {
        return Ok(None);
    };
    let turn = event.data["turn_id"]
        .as_str()
        .filter(|turn| Some(*turn) == agent.active_turn.as_deref());
    if agent.observe_only
        || !core::api::HarnessCapabilities::of(&agent.harness).provider_response_model_verification
        || turn.is_none()
    {
        return Err(Error::new(
            "source_integrity",
            "model mismatch is not bound to the owned turn",
        ));
    }
    let mismatch: core::ProviderModelMismatch = serde_json::from_value(evidence.clone())
        .map_err(|_| Error::new("invalid", "invalid model mismatch hold evidence"))?;
    mismatch.validate()?;
    let turn_id = turn.unwrap_or_default();
    tx.execute(
        "UPDATE turns SET effective_model = NULL
         WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3",
        params![parse_agent_id(&agent.id)?, agent.generation, turn_id],
    )
    .map_err(sql_error)?;
    let restriction = ProviderRestriction {
        kind: ProviderRestrictionKind::ModelMismatch,
        retry_after: None,
        code: Some("model_mismatch".to_owned()),
        http_status: None,
    };
    record_restriction_tx(tx, agent, turn_id, &restriction, Some(mismatch), now)
}

fn record_restriction_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    turn_id: &str,
    restriction: &ProviderRestriction,
    model_mismatch: Option<core::ProviderModelMismatch>,
    now: i64,
) -> Result<Option<core::Event>> {
    let scope = if model_mismatch.is_some() {
        core::model_hold_scope(&agent.profile)
    } else {
        core::frozen_provider_scope(agent)?
    };
    let current = get(tx, &scope)?;
    if model_mismatch.is_some() && current.is_some() {
        return Ok(None);
    }
    if let Some(current) = &current
        && is_permanent(current.kind)
        && !is_permanent(restriction.kind)
    {
        // A delayed rate/quota result cannot downgrade a permanent hold.
        return Ok(None);
    }
    let permanent = is_permanent(restriction.kind);
    let latest_reset = now.saturating_add(core::MAX_PROVIDER_RESET_HORIZON_MS);
    let reset_ignored = restriction.retry_after.and_then(|at| {
        if at > latest_reset {
            Some("beyond_horizon")
        } else if at <= now {
            Some("elapsed")
        } else {
            None
        }
    });
    let vendor_reset = restriction.retry_after.filter(|_| reset_ignored.is_none());
    let retry_after = if permanent {
        vendor_reset
    } else {
        // Already-admitted turns may finish after a refusal set a future deadline.
        let active_reset = current
            .as_ref()
            .and_then(|hold| hold.retry_after)
            .filter(|at| *at > now && *at <= latest_reset);
        vendor_reset.max(active_reset)
    };
    let (automatic_probes, probe_outcome_unknown) = current.as_ref().map_or((0, false), |hold| {
        (hold.automatic_probes, hold.probe_outcome_unknown)
    });
    let (probe_command, state, attempts, held_since) = match current {
        Some(hold) => {
            let attempts: i64 = tx
                .query_row(
                    "SELECT probe_attempts FROM provider_holds WHERE scope = ?1",
                    [&scope],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            (hold.probe_command, hold.state, attempts, hold.held_since)
        }
        None => (None, ProviderHoldState::Held, 0, now),
    };
    let attempts = if retry_after.is_some() || permanent {
        0
    } else {
        attempts
    };
    let next_probe_at = if matches!(
        restriction.kind,
        ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota
    ) && retry_after.is_none()
        && state == ProviderHoldState::Held
        && !probe_outcome_unknown
        && automatic_probes < core::MAX_AUTOMATIC_PROVIDER_PROBES
    {
        Some(next_automatic_probe_at(now, attempts))
    } else {
        None
    };
    let hold = ProviderHold {
        scope: scope.clone(),
        kind: restriction.kind,
        source: Some(ProviderHoldSource {
            harness: agent.harness.clone(),
            code: restriction.code.clone(),
            http_status: restriction.http_status,
            model_mismatch,
        }),
        retry_after,
        next_probe_at,
        probe_command: probe_command.clone(),
        state,
        updated_at: now,
        held_since,
        automatic_probes,
        probe_outcome_unknown,
    };
    hold.validate()?;
    tx.execute(
        "INSERT INTO provider_holds(scope, kind, retry_after, next_probe_at, probe_attempts,
                                    probe_profile, probe_command, state, updated_at, held_since, source)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(scope) DO UPDATE SET kind = excluded.kind,
             retry_after = excluded.retry_after, next_probe_at = excluded.next_probe_at,
             probe_attempts = excluded.probe_attempts, probe_profile = excluded.probe_profile,
             probe_command = excluded.probe_command, state = excluded.state,
             updated_at = excluded.updated_at, held_since = excluded.held_since,
             source = excluded.source",
        params![
            hold.scope,
            hold.kind.as_str(),
            hold.retry_after,
            hold.next_probe_at,
            attempts,
            agent.profile,
            hold.probe_command,
            hold.state.as_str(),
            hold.updated_at,
            hold.held_since,
            encode_json(&hold.source)?,
        ],
    )
    .map_err(sql_error)?;
    Ok(Some(super::insert_daemon_event(
        tx,
        now,
        "provider.hold_updated",
        "provider",
        Some(parse_agent_id(&agent.id)?),
        Some(agent.generation),
        json!({
            "turn_id": turn_id,
            "scope": hold.scope,
            "kind": hold.kind.as_str(),
            "source": hold.source,
            "retry_after": hold.retry_after,
            "reset_ignored": reset_ignored,
            "next_probe_at": hold.next_probe_at,
            "held_since": hold.held_since,
            "probe_command": hold.probe_command,
            "state": hold.state.as_str(),
        }),
    )?))
}

fn release_probe_claim_tx(tx: &Transaction<'_>, command_id: &str, now: i64) -> Result<bool> {
    let changed = tx
        .execute(
            "UPDATE provider_holds
         SET probe_command = NULL, state = 'held', updated_at = ?1
         WHERE probe_command = ?2 AND state = 'probing'",
            params![now, command_id],
        )
        .map_err(sql_error)?;
    Ok(changed == 1)
}

fn pause_unknown_probe_tx(
    tx: &Transaction<'_>,
    scope: &str,
    command_id: &str,
    agent_id: &str,
    generation: i64,
    now: i64,
) -> Result<Option<core::Event>> {
    let changed = tx
        .execute(
            "UPDATE provider_holds SET probe_command=NULL, state='held',
             probe_outcome_unknown=1, next_probe_at=NULL, updated_at=?1
             WHERE scope=?2 AND state='probing' AND probe_command=?3",
            params![now, scope, command_id],
        )
        .map_err(sql_error)?;
    if changed == 0 {
        return Ok(None);
    }
    super::insert_daemon_event(
        tx,
        now,
        "provider.hold_probe_outcome_unknown",
        "provider",
        None,
        None,
        json!({"scope":scope, "probe_command":command_id, "agent":agent_id,
               "generation":generation, "owner_attention":true}),
    )
    .map(Some)
}

fn reschedule_failed_probe_tx(tx: &Transaction<'_>, scope: &str, now: i64) -> Result<Option<i64>> {
    let Some(hold) = get(tx, scope)? else {
        return Ok(None);
    };
    if !matches!(
        hold.kind,
        ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota
    ) || hold.retry_after.is_some()
    {
        tx.execute(
            "UPDATE provider_holds SET next_probe_at = NULL, probe_attempts = 0,
                 updated_at = ?1 WHERE scope = ?2 AND state = 'held'",
            params![now, scope],
        )
        .map_err(sql_error)?;
        return Ok(None);
    }
    let attempts: i64 = tx
        .query_row(
            "SELECT probe_attempts FROM provider_holds WHERE scope = ?1",
            [scope],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    let attempts = attempts.saturating_add(1);
    let next_probe_at =
        (!automatic_probe_paused(&hold)).then(|| next_automatic_probe_at(now, attempts));
    tx.execute(
        "UPDATE provider_holds SET next_probe_at = ?1, probe_attempts = ?2,
             updated_at = ?3 WHERE scope = ?4 AND state = 'held'",
        params![next_probe_at, attempts, now, scope],
    )
    .map_err(sql_error)?;
    Ok(next_probe_at)
}

fn automatic_probe_paused(hold: &ProviderHold) -> bool {
    hold.probe_outcome_unknown || hold.automatic_probes >= core::MAX_AUTOMATIC_PROVIDER_PROBES
}

fn has_intervening_same_scope_restriction_tx(
    tx: &Transaction<'_>,
    scope: &str,
    probe_command: &str,
    probe_agent: i64,
    generation: i64,
    terminal: &core::NativeEvent,
) -> Result<bool> {
    let accepted_seq: Option<i64> = tx
        .query_row(
            "SELECT MIN(seq) FROM events
             WHERE agent = ?1 AND generation = ?2
               AND (kind = 'command.accepted'
                    OR (kind = 'command.queued' AND json_type(data, '$.parent_command') = 'text'))
               AND json_extract(data, '$.command_id') = ?3",
            params![probe_agent, generation, probe_command],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    // A pruned admission event fails here instead of clearing a hold on
    // incomplete history: pruning deletes a contiguous prefix, so a surviving
    // accepted event means the whole (accepted, terminal] range survives and
    // the scan below is complete.
    let accepted_seq = accepted_seq.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "provider probe has no durable admission event",
        )
    })?;
    let terminal_seq: Option<i64> = tx
        .query_row(
            "SELECT seq FROM events
             WHERE agent = ?1 AND generation = ?2 AND kind = 'turn.completed'
               AND native_id = ?3 AND json_extract(data, '$.command_id') = ?4
               AND json_extract(data, '$.turn_id') = ?5
             ORDER BY seq DESC LIMIT 1",
            params![
                probe_agent,
                generation,
                terminal.native_id,
                probe_command,
                terminal.data.get("turn_id").and_then(Value::as_str),
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let terminal_seq = terminal_seq.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "provider probe terminal event was not stored before settlement",
        )
    })?;
    let mut after = accepted_seq;
    loop {
        let candidates = {
            let mut statement = tx
                .prepare(
                    "SELECT e.seq, e.agent, json_extract(e.data, '$.command_id'),
                            json_extract(e.data, '$.provider_restriction')
                     FROM events e JOIN agents a ON a.id = e.agent
                     WHERE e.seq > ?1 AND e.seq < ?2 AND e.kind = 'turn.completed'
                       AND e.native_id IS NOT NULL AND a.observe_only = 0
                       AND e.source NOT IN ('daemon','os','git','workspace_process')
                       AND json_type(e.data, '$.provider_restriction') = 'object'
                     ORDER BY e.seq LIMIT ?3",
                )
                .map_err(sql_error)?;
            let rows = statement
                .query_map(
                    params![after, terminal_seq, INTERVENING_RESTRICTION_BATCH],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, i64>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, Option<String>>(3)?,
                        ))
                    },
                )
                .map_err(sql_error)?;
            let mut candidates = Vec::new();
            for row in rows {
                candidates.push(row.map_err(sql_error)?);
            }
            candidates
        };
        if candidates.is_empty() {
            return Ok(false);
        }
        for (seq, candidate_agent_num, candidate_command, raw_restriction) in candidates {
            after = seq;
            if candidate_command.as_deref() == Some(probe_command) {
                continue;
            }
            let Some(raw_restriction) = raw_restriction else {
                continue;
            };
            let Ok(restriction) = serde_json::from_str::<ProviderRestriction>(&raw_restriction)
            else {
                continue;
            };
            if restriction.validate().is_err() {
                continue;
            }
            let Some(candidate_agent) = load_agent_by_num(tx, candidate_agent_num)? else {
                continue;
            };
            if super::observer::require_controlled(&candidate_agent).is_ok()
                && core::frozen_provider_scope(&candidate_agent)
                    .ok()
                    .as_deref()
                    == Some(scope)
            {
                return Ok(true);
            }
        }
    }
}

fn is_permanent(kind: ProviderRestrictionKind) -> bool {
    matches!(
        kind,
        ProviderRestrictionKind::Auth
            | ProviderRestrictionKind::ModelRemoved
            | ProviderRestrictionKind::ModelMismatch
    )
}

fn next_automatic_probe_at(now: i64, completed_probes: i64) -> i64 {
    let delay_index = usize::try_from(completed_probes)
        .unwrap_or(0)
        .min(AUTOMATIC_PROBE_BACKOFF_MS.len() - 1);
    now.saturating_add(AUTOMATIC_PROBE_BACKOFF_MS[delay_index])
}

fn provider_held() -> Error {
    Error::new(
        "provider_held",
        "provider scope is held pending owner correction or an eligible probe",
    )
}
