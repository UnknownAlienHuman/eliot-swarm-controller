//! Bounded usage totals with explicit coverage over one scope.
//!
//! Owning contracts: `docs/spec/core.md` (usage) and `docs/spec/api.md`
//! (`usage.summary`). Turn expenditure never merges native scopes: each
//! scope (`turn`, `main_loop_turn`, `message`, `unknown`) reports its own
//! subtotals with the turns counted and the per-counter gaps. Session facts
//! read the latest `session_usage` snapshot per Agent generation in scope;
//! each generation contributes its signed advance past the previously retained
//! cumulative for the same source (a new session counts from zero, an advance
//! adds its delta, and a same-session correction retracts the over-count by
//! moving the baseline down), and cost sums only within one verbatim unit,
//! never converted. Role context averages use retained session.usage events
//! in the authorized generation scope. Scope expansion reuses the hierarchy
//! fanout bound and reports truncation; the
//! retained-event watermark names the retention floor instead of implying
//! full totals.

mod role;

pub(crate) use role::{RoleUsage, manager_at_sql};

use super::*;
use super::{hierarchy, retention};
use eliot_core::api::harness::{
    MAX_QUOTA_USAGE_WINDOWS, QuotaUsageCoverage, QuotaUsageOrigin, QuotaUsageSplit,
    QuotaWindowBounds, QuotaWindowUsage,
};
use eliot_core::api::{
    FirstCallInput, MAX_FIRST_CALL_TURNS, PROFILE_FIRST_CALL_WINDOW, ProfileFirstCallInput,
    SessionCostTotal, SessionCumulativeTotals, SessionFacts, SessionTokenSubtotal,
    TurnCounterMissing, TurnCounterTotals, TurnExpenditure, TurnScopeTotals, UsageScopeView,
    UsageSummaryRequest, UsageSummaryResponse,
};
use rusqlite::{Connection, params, params_from_iter};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// One resolved usage scope: retained (agent, generation) pairs with the
/// expansion bound already applied.
pub struct ResolvedUsageScope {
    pub pairs: Vec<(i64, i64)>,
    pub agents_included: usize,
    pub truncated: bool,
}

/// Counter positions shared by the turn aggregation below.
const TURN_COUNTERS: usize = 7;

const MAX_QUOTA_RETAINED_TURNS: usize = 4096;
const QUOTA_ROUTE_MATCH: &str = "
    a.harness = ?1 AND a.profile = ?2 AND a.observe_only = 0
    AND json_extract(a.profile_snapshot, '$.native_route_identity') = ?4
    AND json_extract(a.profile_snapshot, '$.account') IS json_extract(?3, '$.account')
    AND json_extract(a.profile_snapshot, '$.auth_integration') IS json_extract(?3, '$.auth_integration')
    AND json_extract(a.profile_snapshot, '$.route_binding') IS json_extract(?3, '$.route_binding')
    AND json_extract(a.profile_snapshot, '$.local_provider') IS json_extract(?3, '$.local_provider')
    AND json_extract(a.profile_snapshot, '$.proxy') IS json_extract(?3, '$.proxy')
    AND json_extract(a.profile_snapshot, '$.api_key_env') IS json_extract(?3, '$.api_key_env')
    AND json_extract(a.profile_snapshot, '$.cmd') IS json_extract(?3, '$.cmd')";

fn turn_counter(usage: &TurnUsage, index: usize) -> Option<i64> {
    match index {
        0 => usage.input_tokens,
        1 => usage.output_tokens,
        2 => usage.reasoning_tokens,
        3 => usage.thinking_tokens,
        4 => usage.cache_read_tokens,
        5 => usage.cache_write_tokens,
        _ => usage.total_tokens,
    }
}

fn counter_totals(
    sums: &[i128; TURN_COUNTERS],
    supplied: &[usize; TURN_COUNTERS],
) -> TurnCounterTotals {
    let get = |index: usize| {
        if supplied[index] > 0 {
            Some(sums[index].clamp(0, i128::from(i64::MAX)) as i64)
        } else {
            None
        }
    };
    TurnCounterTotals {
        input_tokens: get(0),
        output_tokens: get(1),
        reasoning_tokens: get(2),
        thinking_tokens: get(3),
        cache_read_tokens: get(4),
        cache_write_tokens: get(5),
        total_tokens: get(6),
    }
}

fn counter_missing(counted: usize, supplied: &[usize; TURN_COUNTERS]) -> TurnCounterMissing {
    let missing = |index: usize| counted.saturating_sub(supplied[index]);
    TurnCounterMissing {
        input_tokens: missing(0),
        output_tokens: missing(1),
        reasoning_tokens: missing(2),
        thinking_tokens: missing(3),
        cache_read_tokens: missing(4),
        cache_write_tokens: missing(5),
        total_tokens: missing(6),
    }
}

#[derive(Default)]
struct ScopeAccum {
    counted: usize,
    sums: [i128; TURN_COUNTERS],
    supplied: [usize; TURN_COUNTERS],
}

#[derive(Default)]
struct QuotaOriginAccum {
    turns_counted: usize,
    turns_without_usage: usize,
    scopes: BTreeMap<String, ScopeAccum>,
}

#[derive(Default)]
struct QuotaSplitAccum {
    owner: QuotaOriginAccum,
    automation: QuotaOriginAccum,
    other: QuotaOriginAccum,
    unattributed: QuotaOriginAccum,
}

#[derive(Clone, Copy)]
enum QuotaOriginKind {
    Owner,
    Automation,
    Other,
    Unattributed,
}

impl QuotaSplitAccum {
    fn origin_mut(&mut self, kind: QuotaOriginKind) -> &mut QuotaOriginAccum {
        match kind {
            QuotaOriginKind::Owner => &mut self.owner,
            QuotaOriginKind::Automation => &mut self.automation,
            QuotaOriginKind::Other => &mut self.other,
            QuotaOriginKind::Unattributed => &mut self.unattributed,
        }
    }

    fn finish(self) -> QuotaUsageSplit {
        let finish_origin = |origin: QuotaOriginAccum| QuotaUsageOrigin {
            turns_counted: origin.turns_counted,
            turns_without_usage: origin.turns_without_usage,
            by_scope: origin
                .scopes
                .into_iter()
                .map(|(scope, accum)| TurnScopeTotals {
                    scope,
                    turns_counted: accum.counted,
                    totals: counter_totals(&accum.sums, &accum.supplied),
                    turns_missing: counter_missing(accum.counted, &accum.supplied),
                })
                .collect(),
        };
        QuotaUsageSplit {
            owner: finish_origin(self.owner),
            automation: finish_origin(self.automation),
            other: finish_origin(self.other),
            unattributed: finish_origin(self.unattributed),
        }
    }
}

struct QuotaTurn {
    state: String,
    started_at: Option<i64>,
    completed_at: Option<i64>,
    usage: Option<String>,
    command_id: Option<String>,
}

fn quota_turn_origin(connection: &Connection, command_id: &str) -> Result<QuotaOriginKind> {
    let (principal, has_rule_cause, has_agent_principal, reaches_root, commands_seen): (
        Option<String>,
        i64,
        i64,
        i64,
        i64,
    ) = connection
        .query_row(
            "WITH RECURSIVE command_chain(id, parent_command, verb, principal, cause, depth) AS (
                 SELECT id, parent_command, verb, principal, cause, 0
                   FROM commands WHERE id = ?1
                 UNION ALL
                 SELECT parent.id, parent.parent_command, parent.verb,
                        parent.principal, parent.cause, command_chain.depth + 1
                   FROM commands parent
                   JOIN command_chain ON parent.id = command_chain.parent_command
                  WHERE command_chain.depth < 64
             )
             SELECT MAX(CASE WHEN depth = 0 THEN principal END),
                    COALESCE(MAX(CASE WHEN verb IN ('rule','timer.run','rule.catch_up')
                                      OR json_extract(cause, '$.rule') IS NOT NULL
                                      THEN 1 ELSE 0 END), 0),
                    COALESCE(MAX(CASE WHEN principal LIKE 'agent:%'
                                      THEN 1 ELSE 0 END), 0),
                    COALESCE(MAX(CASE WHEN parent_command IS NULL
                                      THEN 1 ELSE 0 END), 0),
                    COUNT(*)
               FROM command_chain",
            [command_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .map_err(sql_error)?;
    if commands_seen == 0 {
        return Ok(QuotaOriginKind::Unattributed);
    }
    if has_rule_cause != 0 || has_agent_principal != 0 {
        return Ok(QuotaOriginKind::Automation);
    }
    if reaches_root == 0 {
        return Ok(QuotaOriginKind::Unattributed);
    }
    Ok(match principal.as_deref() {
        Some("owner") => QuotaOriginKind::Owner,
        Some(_) => QuotaOriginKind::Other,
        None => QuotaOriginKind::Unattributed,
    })
}

fn normalize_turn_scope(scope: &Option<String>) -> String {
    match scope.as_deref() {
        Some(scope) if !scope.trim().is_empty() => scope.to_owned(),
        _ => "unknown".to_owned(),
    }
}

impl Store {
    /// Resolve one usage scope to its retained (agent, generation) pairs.
    /// Agent scope covers every retained generation unless the request names
    /// one, and the native descendants observed under those generations.
    /// Project and tree scopes cover every generation of each included Agent.
    /// Expansion stops at the hierarchy fanout bound and
    /// reports truncation. No authorization happens here; the Router filters
    /// these pairs against the caller's frozen scope before summarizing.
    pub fn resolve_usage_scope(&self, request: &UsageSummaryRequest) -> Result<ResolvedUsageScope> {
        request.validate()?;
        let bound = self.hierarchy_limits.max_selector_targets;
        if let Some(agent) = request.agent.as_deref() {
            let num = parse_agent_id(agent)?;
            let row = load_agent_by_num(&self.connection, num)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {agent} was not found")))?;
            let mut pairs = if let Some(generation) = request.generation {
                vec![(num, generation)]
            } else {
                self.resolve_agent_scope(vec![(num, row.generation)])?.pairs
            };
            let mut children = self.usage_native_descendants(num, request.generation, bound)?;
            let truncated = children.len() as i64 >= bound;
            children.truncate(bound.saturating_sub(1).max(0) as usize);
            let children = self.resolve_agent_scope(children)?;
            let agents_included = 1 + children.agents_included;
            pairs.extend(children.pairs);
            return Ok(ResolvedUsageScope {
                pairs,
                agents_included,
                truncated,
            });
        }
        if let Some(project) = request.project.as_deref() {
            let (agents, truncated) = self.usage_agents_by_project(project, bound)?;
            let mut scope = self.resolve_agent_scope(agents)?;
            scope.truncated = truncated;
            return Ok(scope);
        }
        let root = request.root.as_deref().ok_or_else(|| {
            Error::new(
                "invalid",
                "usage summary needs exactly one of agent, project or root",
            )
        })?;
        let root_num = parse_agent_id(root)?;
        if load_agent_by_num(&self.connection, root_num)?.is_none() {
            return Err(Error::new(
                "not_found",
                format!("Agent {root} was not found"),
            ));
        }
        let mut member_nums = hierarchy::subtree_member_nums(&self.connection, root_num, bound)?;
        let truncated = member_nums.len() as i64 > bound;
        member_nums.truncate(bound.max(0) as usize);
        let mut agents = Vec::with_capacity(member_nums.len());
        for num in member_nums {
            let row = load_agent_by_num(&self.connection, num)?.ok_or_else(|| {
                Error::new("internal", "usage scope member disappeared in its read")
            })?;
            agents.push((num, row.generation));
        }
        let mut scope = self.resolve_agent_scope(agents)?;
        scope.truncated = truncated;
        Ok(scope)
    }

    fn resolve_agent_scope(&self, mut agents: Vec<(i64, i64)>) -> Result<ResolvedUsageScope> {
        agents.sort();
        agents.dedup();
        let nums: Vec<i64> = agents.iter().map(|(num, _)| *num).collect();
        let mut pairs: BTreeSet<(i64, i64)> = agents.into_iter().collect();
        for (num, generation) in self.retained_generations(&nums)? {
            pairs.insert((num, generation));
        }
        Ok(ResolvedUsageScope {
            pairs: pairs.into_iter().collect(),
            agents_included: nums.len(),
            truncated: false,
        })
    }

    fn usage_native_descendants(
        &self,
        parent: i64,
        generation: Option<i64>,
        limit: i64,
    ) -> Result<Vec<(i64, i64)>> {
        let mut statement = self
            .connection
            .prepare(
                "WITH RECURSIVE native(id) AS (
                SELECT r.child FROM native_child_registrations r
                WHERE r.parent = ?1 AND (?2 IS NULL OR r.parent_generation = ?2)
                UNION
                SELECT r.child FROM native_child_registrations r
                JOIN native n ON n.id = r.parent
             )
             SELECT a.id, a.generation FROM native n JOIN agents a ON a.id = n.id
             ORDER BY a.id LIMIT ?3",
            )
            .map_err(sql_error)?;
        statement
            .query_map(params![parent, generation, limit], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)
    }

    fn usage_agents_by_project(
        &self,
        project: &str,
        bound: i64,
    ) -> Result<(Vec<(i64, i64)>, bool)> {
        let probe = bound.saturating_add(1);
        let mut statement = self
            .connection
            .prepare("SELECT id, generation FROM agents WHERE project = ?1 ORDER BY id LIMIT ?2")
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![project, probe], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
            })
            .map_err(sql_error)?;
        let mut agents = Vec::new();
        for row in rows {
            agents.push(row.map_err(sql_error)?);
        }
        let truncated = agents.len() as i64 > bound;
        agents.truncate(bound.max(0) as usize);
        Ok((agents, truncated))
    }

    /// Distinct retained generations holding turns or session snapshots for
    /// these agents, so the Agent scope covers history beyond the current
    /// generation. Indexed by the `turns` primary-key prefix and the
    /// `session_usage` primary key; no new index needed.
    fn retained_generations(&self, nums: &[i64]) -> Result<Vec<(i64, i64)>> {
        if nums.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = nums.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let mut pairs = BTreeSet::new();
        for table in ["turns", "session_usage"] {
            let sql = format!(
                "SELECT agent, generation FROM {table} WHERE agent IN ({placeholders}) GROUP BY agent, generation"
            );
            let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
            let rows = statement
                .query_map(params_from_iter(nums.iter()), |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(sql_error)?;
            for row in rows {
                pairs.insert(row.map_err(sql_error)?);
            }
        }
        Ok(pairs.into_iter().collect())
    }

    /// Summarize retained usage for already-authorized (agent, generation)
    /// pairs. Runs under the caller's Store lock, so turn, event and session
    /// reads see one consistent view; every query rides an existing index.
    pub fn summarize_usage(
        &self,
        scope: UsageScopeView,
        pairs: &[(i64, i64)],
        agents_included: usize,
        truncated: bool,
    ) -> Result<UsageSummaryResponse> {
        let pruned_through = retention::coverage_pruned_through(&self.connection)?;
        let history_complete = pruned_through == 0;
        let history_reason = if history_complete {
            "no event history pruned; totals cover retained terminal turns and session.usage observations"
                .to_owned()
        } else {
            format!(
                "event history pruned through seq {pruned_through}; turn totals cover retained terminal turns and context averages cover retained session.usage observations only"
            )
        };
        let wanted: std::collections::HashSet<(i64, i64)> = pairs.iter().copied().collect();
        let agent_ids: BTreeSet<i64> = wanted.iter().map(|(agent, _)| *agent).collect();
        let mut role_usage = RoleUsage::default();
        let mut turns_counted = 0_usize;
        let mut turns_without_usage = 0_usize;
        let mut turns_unknown_excluded = 0_usize;
        let mut turns_running_excluded = 0_usize;
        let mut overall_supplied = [0_usize; TURN_COUNTERS];
        let mut overall_sums = [0_i128; TURN_COUNTERS];
        let mut scopes: BTreeMap<String, ScopeAccum> = BTreeMap::new();
        let mut first_calls = BTreeMap::new();
        let mut first_calls_truncated = false;
        let mut observed_native_agents = 0i64;
        if !agent_ids.is_empty() {
            let placeholders = agent_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            observed_native_agents = self
                .connection
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM agents
                          WHERE id IN ({placeholders}) AND origin = 'native' AND observe_only = 1"
                    ),
                    params_from_iter(agent_ids.iter()),
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let completed = "COALESCE(t.completed_at, t.started_at, 0)";
            let manager = manager_at_sql("t.agent", completed);
            let sql = format!(
                "SELECT t.agent, t.generation, t.state, t.usage, a.harness, t.turn_id,
                        {completed}, {manager}
                 FROM turns t JOIN agents a ON a.id = t.agent
                 WHERE t.agent IN ({placeholders})"
            );
            let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
            let rows = statement
                .query_map(params_from_iter(agent_ids.iter()), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, bool>(7)?,
                    ))
                })
                .map_err(sql_error)?;
            for row in rows {
                let (agent, generation, state, usage, harness, turn_id, settled_at, manager) =
                    row.map_err(sql_error)?;
                if !wanted.contains(&(agent, generation)) {
                    continue;
                }
                if state != "completed" {
                    // Sums stay over completed turns only, but the skipped
                    // turns stay visible: an unknown turn may have spent
                    // tokens this readback cannot see. Any other
                    // non-terminal state reads as unknown rather than
                    // vanishing from coverage.
                    match state.as_str() {
                        "running" => turns_running_excluded += 1,
                        _ => turns_unknown_excluded += 1,
                    }
                    continue;
                }
                turns_counted += 1;
                let Some(usage) = usage else {
                    turns_without_usage += 1;
                    continue;
                };
                let usage = decode_turn_usage(&usage, &harness)?;
                role_usage.observe_turn(manager, &usage)?;
                if let Some(input_tokens) = usage.first_call_input_tokens {
                    first_calls.insert((settled_at, agent, generation, turn_id), input_tokens);
                    if first_calls.len() > MAX_FIRST_CALL_TURNS {
                        first_calls.pop_first();
                        first_calls_truncated = true;
                    }
                }
                let accum = scopes
                    .entry(normalize_turn_scope(&usage.scope))
                    .or_default();
                accum.counted += 1;
                for index in 0..TURN_COUNTERS {
                    if let Some(value) = turn_counter(&usage, index) {
                        accum.sums[index] += i128::from(value);
                        accum.supplied[index] += 1;
                        overall_sums[index] += i128::from(value);
                        overall_supplied[index] += 1;
                    }
                }
            }
        }
        let by_scope = scopes
            .into_iter()
            .map(|(scope, accum)| TurnScopeTotals {
                scope,
                turns_counted: accum.counted,
                totals: counter_totals(&accum.sums, &accum.supplied),
                turns_missing: counter_missing(accum.counted, &accum.supplied),
            })
            .collect();
        self.accumulate_usage_context(&wanted, &agent_ids, &mut role_usage)?;
        let sessions = self.summarize_sessions(&wanted)?;
        let (swarm_mcp, swarm_mcp_truncated) = self.swarm_mcp_turn_usage(pairs)?;
        let by_role = role_usage.finish();
        Ok(UsageSummaryResponse {
            scope,
            agents_included,
            observed_native_agents: observed_native_agents as usize,
            generations_included: wanted.len(),
            truncated,
            history_complete,
            history_reason,
            pruned_through,
            turns: TurnExpenditure {
                turns_counted,
                turns_without_usage,
                turns_unknown_excluded,
                turns_running_excluded,
                turns_missing: counter_missing(turns_counted, &overall_supplied),
                by_scope,
            },
            sessions,
            by_role,
            first_calls: first_calls
                .into_iter()
                .rev()
                .map(
                    |((_, agent, generation, turn_id), input_tokens)| FirstCallInput {
                        agent: format_agent_id(agent),
                        generation,
                        turn_id,
                        input_tokens,
                    },
                )
                .collect(),
            first_calls_truncated,
            swarm_mcp,
            swarm_mcp_truncated,
        })
    }

    /// Attribute retained native turn units to one named profile's frozen
    /// route selectors and current Agent generations. Older generations are
    /// excluded because their immutable route snapshots are not retained.
    pub fn summarize_quota_usage(
        &self,
        harness: &str,
        profile_name: &str,
        snapshot: &serde_json::Value,
        windows: &[QuotaWindowBounds],
    ) -> Result<Vec<QuotaWindowUsage>> {
        self.ensure_committed_reads()?;
        if windows.len() > MAX_QUOTA_USAGE_WINDOWS {
            return Err(eliot_core::Error::new(
                "invalid",
                "quota usage window count exceeds its bound",
            ));
        }
        for window in windows {
            window.validate()?;
        }
        let route_identity = snapshot
            .get("native_route_identity")
            .and_then(serde_json::Value::as_str)
            .filter(|identity| !identity.trim().is_empty());
        let snapshot_json = encode_json(snapshot)?;
        let pruned_through = retention::coverage_pruned_through(&self.connection)?;
        let history_complete = pruned_through == 0;
        let history_reason = if history_complete {
            "Current-generation turns for the selected profile route. No event history pruned."
                .to_owned()
        } else {
            format!(
                "Current-generation turns for the selected profile route. Event history pruned through sequence {pruned_through}."
            )
        };
        let (agents_matched, older_generations_excluded) = if let Some(identity) = route_identity {
            let agents_matched: i64 = self
                .connection
                .query_row(
                    &format!("SELECT COUNT(*) FROM agents a WHERE {QUOTA_ROUTE_MATCH}"),
                    params![harness, profile_name, snapshot_json, identity],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let older_generations_excluded: i64 = self
                .connection
                .query_row(
                    &format!(
                        "SELECT EXISTS(
                         SELECT 1 FROM agents a
                         JOIN turns t ON t.agent = a.id AND t.generation <> a.generation
                         WHERE {QUOTA_ROUTE_MATCH}
                     )"
                    ),
                    params![harness, profile_name, snapshot_json, identity],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            (
                agents_matched.max(0) as usize,
                older_generations_excluded != 0,
            )
        } else {
            (0, false)
        };
        let mut result = Vec::with_capacity(windows.len());
        for window in windows {
            let mut coverage = QuotaUsageCoverage {
                route_identity_available: route_identity.is_some(),
                window_bounds_available: window.starts_at_ms.is_some(),
                agents_matched,
                older_generations_excluded,
                history_complete,
                history_reason: history_reason.clone(),
                ..QuotaUsageCoverage::default()
            };
            let mut usage_split = QuotaSplitAccum::default();
            let Some(starts_at_ms) = window.starts_at_ms else {
                coverage.history_complete = false;
                coverage.history_reason =
                    "provider quota window has no known start time; no turn interval was inferred"
                        .to_owned();
                result.push(QuotaWindowUsage {
                    label: window.label.clone(),
                    starts_at_ms: None,
                    through_ms: window.through_ms,
                    by_origin: usage_split.finish(),
                    coverage,
                });
                continue;
            };
            let Some(identity) = route_identity else {
                coverage.history_complete = false;
                coverage.history_reason =
                    "frozen profile has no canonical route identity; no turn usage was attributed"
                        .to_owned();
                result.push(QuotaWindowUsage {
                    label: window.label.clone(),
                    starts_at_ms: Some(starts_at_ms),
                    through_ms: window.through_ms,
                    by_origin: usage_split.finish(),
                    coverage,
                });
                continue;
            };
            if agents_matched == 0 {
                coverage.history_reason =
                    "No current Agent generation matched the selected profile route.".to_owned();
                result.push(QuotaWindowUsage {
                    label: window.label.clone(),
                    starts_at_ms: Some(starts_at_ms),
                    through_ms: window.through_ms,
                    by_origin: usage_split.finish(),
                    coverage,
                });
                continue;
            }

            let turns = {
                let mut statement = self
                    .connection
                    .prepare(&format!(
                        "SELECT t.state, t.started_at, t.completed_at, t.usage, c.id
                           FROM agents a
                           JOIN turns t ON t.agent = a.id AND t.generation = a.generation
                           LEFT JOIN commands c
                             ON c.id = t.start_command_id
                            AND c.target = t.agent
                            AND c.generation = t.generation
                            AND c.turn_id = t.turn_id
                            AND c.verb = 'send' AND c.state = 'applied'
                          WHERE {QUOTA_ROUTE_MATCH}
                            AND (
                                (t.started_at <= ?6
                                 AND (t.completed_at IS NULL OR t.completed_at >= ?5))
                                OR (t.started_at IS NULL
                                    AND t.completed_at BETWEEN ?5 AND ?6)
                                OR (t.started_at IS NULL AND t.completed_at IS NULL)
                            )
                          ORDER BY t.insertion_seq DESC, t.agent, t.turn_id
                          LIMIT ?7"
                    ))
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map(
                        params![
                            harness,
                            profile_name,
                            snapshot_json,
                            identity,
                            starts_at_ms,
                            window.through_ms,
                            (MAX_QUOTA_RETAINED_TURNS + 1) as i64,
                        ],
                        |row| {
                            Ok(QuotaTurn {
                                state: row.get(0)?,
                                started_at: row.get(1)?,
                                completed_at: row.get(2)?,
                                usage: row.get(3)?,
                                command_id: row.get(4)?,
                            })
                        },
                    )
                    .map_err(sql_error)?;
                let mut turns = Vec::new();
                for row in rows {
                    turns.push(row.map_err(sql_error)?);
                }
                turns
            };
            if turns.len() > MAX_QUOTA_RETAINED_TURNS {
                coverage.history_complete = false;
                coverage.history_reason.push_str(&format!(
                    " Read limited to the latest {MAX_QUOTA_RETAINED_TURNS} matching turns."
                ));
            }
            for turn in turns.into_iter().take(MAX_QUOTA_RETAINED_TURNS) {
                if turn.started_at.is_none() && turn.completed_at.is_none() {
                    coverage.turns_missing_time += 1;
                    continue;
                }
                let overlaps = match (turn.started_at, turn.completed_at) {
                    (Some(start), Some(end)) => start <= window.through_ms && end >= starts_at_ms,
                    (Some(start), None) => start <= window.through_ms,
                    (None, Some(end)) => end >= starts_at_ms && end <= window.through_ms,
                    (None, None) => false,
                };
                if !overlaps {
                    continue;
                }
                coverage.turns_overlapping += 1;
                if turn.state != "completed" {
                    if turn.state == "running" {
                        coverage.turns_running_excluded += 1;
                    } else {
                        coverage.turns_unknown_excluded += 1;
                    }
                    continue;
                }
                let (Some(start), Some(end)) = (turn.started_at, turn.completed_at) else {
                    coverage.turns_missing_time += 1;
                    continue;
                };
                if start > end {
                    coverage.turns_missing_time += 1;
                    continue;
                }
                if start < starts_at_ms || end > window.through_ms {
                    coverage.turns_crossing_window += 1;
                    continue;
                }
                coverage.turns_counted += 1;
                let origin_kind = match turn.command_id.as_deref() {
                    Some(command_id) => quota_turn_origin(&self.connection, command_id)?,
                    None => {
                        coverage.turns_missing_command += 1;
                        QuotaOriginKind::Unattributed
                    }
                };
                let origin = usage_split.origin_mut(origin_kind);
                origin.turns_counted += 1;
                let Some(usage) = turn.usage else {
                    origin.turns_without_usage += 1;
                    coverage.turns_without_usage += 1;
                    continue;
                };
                let usage = decode_turn_usage(&usage, harness)?;
                let accum = origin
                    .scopes
                    .entry(normalize_turn_scope(&usage.scope))
                    .or_default();
                accum.counted += 1;
                for index in 0..TURN_COUNTERS {
                    if let Some(value) = turn_counter(&usage, index) {
                        accum.sums[index] += i128::from(value);
                        accum.supplied[index] += 1;
                    }
                }
            }
            result.push(QuotaWindowUsage {
                label: window.label.clone(),
                starts_at_ms: Some(starts_at_ms),
                through_ms: window.through_ms,
                by_origin: usage_split.finish(),
                coverage,
            });
        }
        Ok(result)
    }

    /// The newest retained turn rows give doctor a fixed-size observation
    /// window. Only the current generation has the exact profile snapshot.
    pub fn profile_first_calls(&self) -> Result<(Vec<ProfileFirstCallInput>, bool)> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT a.profile, a.harness, json_extract(a.profile_snapshot, '$.mcp'),
                        t.usage, t.state
                 FROM (
                     SELECT rowid AS sequence, agent, generation, usage, state FROM turns
                     ORDER BY rowid DESC LIMIT ?1
                 ) t
                 LEFT JOIN agents a ON a.id = t.agent AND a.generation = t.generation
                      AND a.origin = 'eliot'
                 ORDER BY t.sequence DESC",
            )
            .map_err(sql_error)?;
        let mut rows = statement
            .query(params![PROFILE_FIRST_CALL_WINDOW as i64 + 1])
            .map_err(sql_error)?;
        let mut examined = 0;
        let mut truncated = false;
        let mut by_profile = BTreeMap::new();
        while let Some(row) = rows.next().map_err(sql_error)? {
            if examined == PROFILE_FIRST_CALL_WINDOW {
                truncated = true;
                break;
            }
            examined += 1;
            if row.get::<_, String>(4).map_err(sql_error)? != "completed" {
                continue;
            }
            let (Some(profile), Some(harness), Some(usage)) = (
                row.get::<_, Option<String>>(0).map_err(sql_error)?,
                row.get::<_, Option<String>>(1).map_err(sql_error)?,
                row.get::<_, Option<String>>(3).map_err(sql_error)?,
            ) else {
                continue;
            };
            let Some(input) = decode_turn_usage(&usage, &harness)?.first_call_input_tokens else {
                continue;
            };
            let policy = row.get::<_, Option<String>>(2).map_err(sql_error)?;
            let (sum, count) = by_profile
                .entry((profile, harness, policy))
                .or_insert((0_i128, 0));
            *sum += i128::from(input);
            *count += 1;
        }
        let profiles = by_profile
            .into_iter()
            .map(
                |((profile, harness, mcp_policy), (sum, count))| ProfileFirstCallInput {
                    profile,
                    harness,
                    mcp_policy,
                    sampled_turns: count,
                    average_input_tokens: (sum / count as i128) as i64,
                },
            )
            .collect();
        Ok((profiles, truncated))
    }
}

/// Baseline namespace for resume-chain deltas: the snapshot source up to any
/// event suffix (`muse:session/tokenUsage` and `muse:session/contextUsage`
/// share `muse:session`). Different event kinds from one native subject must
/// not double-count; different provider namespaces must never subtract.
fn normalize_baseline_source(source: Option<&str>) -> String {
    let source = source.unwrap_or_default();
    source.split('/').next().unwrap_or_default().to_owned()
}

impl Store {
    fn accumulate_usage_context(
        &self,
        wanted: &std::collections::HashSet<(i64, i64)>,
        agents: &BTreeSet<i64>,
        role_usage: &mut RoleUsage,
    ) -> Result<()> {
        if agents.is_empty() {
            return Ok(());
        }
        let placeholders = agents.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let manager = manager_at_sql("e.agent", "e.ts");
        let sql = format!(
            "SELECT e.agent, e.generation, e.data, {manager}
             FROM events e
             WHERE e.kind = 'session.usage' AND e.agent IN ({placeholders})
             ORDER BY e.seq"
        );
        let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
        let rows = statement
            .query_map(params_from_iter(agents.iter()), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                ))
            })
            .map_err(sql_error)?;
        for row in rows {
            let (agent, generation, data, manager) = row.map_err(sql_error)?;
            if !wanted.contains(&(agent, generation)) {
                continue;
            }
            let event: serde_json::Value = serde_json::from_str(&data)
                .map_err(|_| Error::new("internal", "retained session usage event is malformed"))?;
            let mut usage: SessionUsage =
                serde_json::from_value(event.get("usage").cloned().ok_or_else(|| {
                    Error::new("internal", "retained session usage event has no usage")
                })?)
                .map_err(|_| Error::new("internal", "retained session usage data is malformed"))?;
            usage.validate()?;
            usage.retain_valid_context();
            role_usage.observe_context(manager, usage.context_used_tokens);
        }
        Ok(())
    }

    fn summarize_sessions(
        &self,
        wanted: &std::collections::HashSet<(i64, i64)>,
    ) -> Result<SessionFacts> {
        let mut snapshots = 0_usize;
        let mut supplied = [0_usize; 4];
        let mut sums = [0_i128; 4];
        let mut occupancy = 0_usize;
        let mut capacity = 0_usize;
        let mut costs: BTreeMap<String, (f64, usize)> = BTreeMap::new();
        let mut snapshots_without_cost = 0_usize;
        // Consecutive generations of one Agent continue the same native
        // subject (resume enforces subject equality at its transaction
        // boundary), so each snapshot contributes only its advance past the
        // previously retained cumulative. Collect first, then walk oldest
        // generation first.
        let mut chain: Vec<(i64, i64, SessionUsage)> = Vec::new();
        if !wanted.is_empty() {
            let nums: BTreeSet<i64> = wanted.iter().map(|(num, _)| *num).collect();
            let nums: Vec<i64> = nums.into_iter().collect();
            let placeholders = nums.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "SELECT s.agent, s.generation, s.snapshot, a.harness
                     FROM session_usage s JOIN agents a ON a.id = s.agent
                     WHERE s.agent IN ({placeholders})"
            );
            let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
            let rows = statement
                .query_map(params_from_iter(nums.iter()), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })
                .map_err(sql_error)?;
            for row in rows {
                let (agent, generation, snapshot, harness) = row.map_err(sql_error)?;
                if !wanted.contains(&(agent, generation)) {
                    continue;
                }
                let usage = decode_session_usage(&snapshot, &harness)?;
                chain.push((agent, generation, usage));
            }
        }
        chain.sort_by_key(|first| (first.0, first.1));
        // Signed advances past the retained baseline: a first observation for
        // a session counts from zero, an advance adds its delta, and a lower
        // observation in the same session is a correction that retracts the
        // over-count by moving the baseline down (it contributes no new
        // usage). Only a genuinely new native subject starts a fresh baseline.
        // Baselines split by normalized provider namespace (the snapshot
        // source up to any event suffix), so cumulative namespaces from
        // different provider pipelines never subtract, while different event
        // kinds from the same native subject (for example Muse
        // `session/tokenUsage` and `session/contextUsage`, where the latter
        // carries the former's cumulative forward) share one baseline instead
        // of double-counting the same subject.
        let mut token_baseline: HashMap<(i64, String, usize), i64> = HashMap::new();
        let mut cost_baseline: HashMap<(i64, String, String), f64> = HashMap::new();
        for (agent, _generation, usage) in &chain {
            snapshots += 1;
            if usage.context_used_tokens.is_some() {
                occupancy += 1;
            }
            if usage.context_window_tokens.is_some() {
                capacity += 1;
            }
            let source = normalize_baseline_source(usage.source.as_deref());
            let counters = [
                usage
                    .cumulative_tokens
                    .as_ref()
                    .and_then(|cumulative| cumulative.input_tokens),
                usage
                    .cumulative_tokens
                    .as_ref()
                    .and_then(|cumulative| cumulative.output_tokens),
                usage
                    .cumulative_tokens
                    .as_ref()
                    .and_then(|cumulative| cumulative.cache_read_tokens),
                usage
                    .cumulative_tokens
                    .as_ref()
                    .and_then(|cumulative| cumulative.cache_write_tokens),
            ];
            for (index, value) in counters.into_iter().enumerate() {
                if let Some(value) = value {
                    // Signed advance past the retained baseline: a new session
                    // counts from zero, an advance adds its delta, and a
                    // same-session correction retracts the over-count by
                    // moving the baseline down to the corrected cumulative.
                    let baseline = token_baseline
                        .get(&(*agent, source.clone(), index))
                        .copied()
                        .unwrap_or(0);
                    token_baseline.insert((*agent, source.clone(), index), value);
                    sums[index] += i128::from(value - baseline);
                    supplied[index] += 1;
                }
            }
            match usage.cumulative_cost.as_ref() {
                Some(cost)
                    if cost.unit.as_ref().is_some_and(|unit| !unit.is_empty())
                        && cost.amount.is_some() =>
                {
                    let unit = cost.unit.clone().expect("cost unit present");
                    let amount = cost.amount.expect("cost amount present");
                    let baseline = cost_baseline
                        .get(&(*agent, source.clone(), unit.clone()))
                        .copied()
                        .unwrap_or(0.0);
                    cost_baseline.insert((*agent, source.clone(), unit.clone()), amount);
                    let entry = costs.entry(unit).or_insert((0.0, 0));
                    entry.0 += amount - baseline;
                    entry.1 += 1;
                }
                _ => snapshots_without_cost += 1,
            }
        }
        let subtotal = |index: usize| {
            let missing = snapshots.saturating_sub(supplied[index]);
            SessionTokenSubtotal {
                total: if supplied[index] > 0 {
                    Some(sums[index].clamp(0, i128::from(i64::MAX)) as i64)
                } else {
                    None
                },
                supplied: supplied[index],
                missing,
                complete: missing == 0 && supplied[index] > 0,
            }
        };
        Ok(SessionFacts {
            snapshots,
            generations_without_snapshot: wanted.len().saturating_sub(snapshots),
            cumulative_tokens: SessionCumulativeTotals {
                input_tokens: subtotal(0),
                output_tokens: subtotal(1),
                cache_read_tokens: subtotal(2),
                cache_write_tokens: subtotal(3),
            },
            cost_by_unit: costs
                .into_iter()
                .map(|(unit, (total_amount, snapshots))| SessionCostTotal {
                    unit,
                    total_amount,
                    snapshots,
                })
                .collect(),
            snapshots_without_cost,
            context_occupancy_supplied: occupancy,
            context_capacity_supplied: capacity,
        })
    }
}
