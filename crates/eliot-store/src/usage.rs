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

mod assignment;
pub(crate) mod context;
mod role;

pub(super) use assignment::send_assignment_usage;
pub(crate) use role::{RoleUsage, manager_at_sql};

use super::*;
use super::{hierarchy, retention};
use eliot_core::api::harness::{
    MAX_QUOTA_USAGE_WINDOWS, QuotaUsageCoverage, QuotaUsageOrigin, QuotaUsageSplit,
    QuotaWindowBounds, QuotaWindowUsage,
};
use eliot_core::api::{
    FirstCallInput, MAX_FIRST_CALL_TURNS, MAX_NATIVE_CHILD_TURN_MARKERS, NativeChildTurnMarker,
    PROFILE_FIRST_CALL_WINDOW, ProfileFirstCallInput, ProfileNativeChildUsage, ProjectUsageView,
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

type NativeChildTurnKey = (i64, i64, String);

#[derive(Default)]
struct NativeChildBindings {
    by_turn: HashMap<NativeChildTurnKey, usize>,
    unbound_by_generation: HashMap<(i64, i64), usize>,
    unbound_total: usize,
}

#[derive(Default)]
struct ProfileNativeChildAccum {
    sampled_turns: usize,
    turns_with_children: usize,
    turns_without_children: usize,
    turns_with_unknown_children: usize,
    tokens_with_children: Vec<i64>,
    tokens_without_children: Vec<i64>,
}

fn median_tokens(tokens: &mut [i64]) -> Option<i64> {
    if tokens.is_empty() {
        return None;
    }
    tokens.sort_unstable();
    let middle = tokens.len() / 2;
    if tokens.len().is_multiple_of(2) {
        Some(((i128::from(tokens[middle - 1]) + i128::from(tokens[middle])) / 2) as i64)
    } else {
        Some(tokens[middle])
    }
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
struct TurnExpenditureAccum {
    counted: usize,
    without_usage: usize,
    unknown: usize,
    running: usize,
    supplied: [usize; TURN_COUNTERS],
    scopes: BTreeMap<String, ScopeAccum>,
}

impl TurnExpenditureAccum {
    fn observe(
        &mut self,
        state: &str,
        usage: Option<&str>,
        harness: &str,
    ) -> Result<Option<TurnUsage>> {
        match state {
            "completed" => self.counted += 1,
            "running" => {
                self.running += 1;
                return Ok(None);
            }
            _ => {
                self.unknown += 1;
                return Ok(None);
            }
        }
        let Some(usage) = usage else {
            self.without_usage += 1;
            return Ok(None);
        };
        let usage = decode_turn_usage(usage, harness)?;
        let scope = self
            .scopes
            .entry(normalize_turn_scope(&usage.scope))
            .or_default();
        scope.counted += 1;
        for index in 0..TURN_COUNTERS {
            if let Some(value) = turn_counter(&usage, index) {
                scope.sums[index] += i128::from(value);
                scope.supplied[index] += 1;
                self.supplied[index] += 1;
            }
        }
        Ok(Some(usage))
    }

    fn finish(self) -> TurnExpenditure {
        TurnExpenditure {
            turns_counted: self.counted,
            turns_without_usage: self.without_usage,
            turns_unknown_excluded: self.unknown,
            turns_running_excluded: self.running,
            turns_missing: counter_missing(self.counted, &self.supplied),
            by_scope: self
                .scopes
                .into_iter()
                .map(|(scope, accum)| TurnScopeTotals {
                    scope,
                    turns_counted: accum.counted,
                    totals: counter_totals(&accum.sums, &accum.supplied),
                    turns_missing: counter_missing(accum.counted, &accum.supplied),
                })
                .collect(),
        }
    }
}

struct ScopedUsageTurn {
    agent: i64,
    generation: i64,
    state: String,
    usage: Option<String>,
    harness: String,
    turn_id: String,
    settled_at: i64,
    manager: bool,
}

/// Seek the exact authorized Agent/generation pairs on the turns index.
fn for_each_scoped_usage_turn(
    conn: &Connection,
    pairs: &[(i64, i64)],
    with_manager: bool,
    mut visit: impl FnMut(ScopedUsageTurn) -> Result<()>,
) -> Result<()> {
    if pairs.is_empty() {
        return Ok(());
    }
    let completed = "COALESCE(t.completed_at, t.started_at, 0)";
    let manager = if with_manager {
        manager_at_sql("t.agent", completed)
    } else {
        "0".to_owned()
    };
    let mut statement = conn
        .prepare_cached(&format!(
            "SELECT t.agent, t.generation, t.state, t.usage, a.harness, t.turn_id,
                    {completed}, {manager}
             FROM json_each(?1) p
             CROSS JOIN turns t ON t.agent = json_extract(p.value, '$[0]')
                               AND t.generation = json_extract(p.value, '$[1]')
             CROSS JOIN agents a ON a.id = t.agent"
        ))
        .map_err(sql_error)?;
    let rows = statement
        .query_map([encode_json(&pairs)?], |row| {
            Ok(ScopedUsageTurn {
                agent: row.get(0)?,
                generation: row.get(1)?,
                state: row.get(2)?,
                usage: row.get(3)?,
                harness: row.get(4)?,
                turn_id: row.get(5)?,
                settled_at: row.get(6)?,
                manager: row.get(7)?,
            })
        })
        .map_err(sql_error)?;
    for row in rows {
        visit(row.map_err(sql_error)?)?;
    }
    Ok(())
}

pub(super) fn overview_project_usage(
    conn: &Connection,
    project: &str,
    bound: i64,
    members: (Option<i64>, Option<&str>, bool),
) -> Result<ProjectUsageView> {
    let scope = resolve_project_usage(conn, project, bound, members)?;
    let pruned_through = retention::coverage_pruned_through(conn)?;
    let mut turns = TurnExpenditureAccum::default();
    for_each_scoped_usage_turn(conn, &scope.pairs, false, |row| {
        turns.observe(&row.state, row.usage.as_deref(), &row.harness)?;
        Ok(())
    })?;
    Ok(ProjectUsageView {
        agents_included: scope.agents_included,
        generations_included: scope.pairs.len(),
        truncated: scope.truncated,
        history_complete: pruned_through == 0,
        pruned_through,
        turns: turns.finish(),
    })
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
    native_commandless: bool,
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
            "WITH RECURSIVE command_chain(id, parent_command, verb, principal, cause, automated_switch, depth) AS (
                 SELECT id, parent_command, verb, principal, cause,
                        verb='switch' AND (json_extract(args,'$.owner_automation')=1
                            OR json_extract(args,'$.session_growth_authority') IS NOT NULL), 0
                   FROM commands WHERE id = ?1
                 UNION ALL
                 SELECT parent.id, parent.parent_command, parent.verb,
                        parent.principal, parent.cause,
                        parent.verb='switch' AND (json_extract(parent.args,'$.owner_automation')=1
                            OR json_extract(parent.args,'$.session_growth_authority') IS NOT NULL),
                        command_chain.depth + 1
                   FROM commands parent
                   JOIN command_chain ON parent.id = command_chain.parent_command
                  WHERE command_chain.depth < 64
             )
             SELECT MAX(CASE WHEN depth = 0 THEN principal END),
                    COALESCE(MAX(CASE WHEN verb IN ('rule','timer.run','rule.catch_up')
                                      OR json_extract(cause, '$.rule') IS NOT NULL
                                      OR automated_switch=1
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

fn resolve_project_usage(
    conn: &Connection,
    project: &str,
    bound: i64,
    (root, scope, slf): (Option<i64>, Option<&str>, bool),
) -> Result<ResolvedUsageScope> {
    let mut statement = conn
        .prepare_cached(&format!(
            "SELECT a.id, a.generation FROM agents a
         WHERE a.project = ?1 AND {} ORDER BY a.id LIMIT ?2",
            overview::member_sql("a", 3, 4, 5),
        ))
        .map_err(sql_error)?;
    let mut agents = statement
        .query_map(
            params![project, bound.saturating_add(1), root, scope, slf],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    let truncated = agents.len() as i64 > bound;
    agents.truncate(bound.max(0) as usize);
    let mut scope = resolve_agent_scope(conn, agents)?;
    scope.truncated = truncated;
    Ok(scope)
}

fn resolve_agent_scope(
    conn: &Connection,
    mut agents: Vec<(i64, i64)>,
) -> Result<ResolvedUsageScope> {
    agents.sort();
    agents.dedup();
    let nums: Vec<i64> = agents.iter().map(|(num, _)| *num).collect();
    let mut pairs: BTreeSet<(i64, i64)> = agents.into_iter().collect();
    for (num, generation) in retained_generations(conn, &nums)? {
        pairs.insert((num, generation));
    }
    Ok(ResolvedUsageScope {
        pairs: pairs.into_iter().collect(),
        agents_included: nums.len(),
        truncated: false,
    })
}

/// Distinct retained generations holding turns or session snapshots for
/// these agents, so the Agent scope covers history beyond the current
/// generation. Indexed by the `turns` primary-key prefix and the
/// `session_usage` primary key; no new index needed.
fn retained_generations(conn: &Connection, nums: &[i64]) -> Result<Vec<(i64, i64)>> {
    if nums.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = nums.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let mut pairs = BTreeSet::new();
    for table in ["turns", "session_usage"] {
        let sql = format!(
            "SELECT agent, generation FROM {table} WHERE agent IN ({placeholders}) GROUP BY agent, generation"
        );
        let mut statement = conn.prepare_cached(&sql).map_err(sql_error)?;
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

impl Store {
    /// Read only registrations whose parent generation is already in the
    /// caller-authorized usage scope.
    fn native_child_bindings(&self, pairs: &[(i64, i64)]) -> Result<NativeChildBindings> {
        if pairs.is_empty() {
            return Ok(NativeChildBindings::default());
        }
        let pairs = encode_json(&pairs)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT r.parent, r.parent_generation, r.parent_turn_id, COUNT(DISTINCT r.child)
                 FROM native_child_registrations r
                 JOIN (
                     SELECT DISTINCT
                         CAST(json_extract(value, '$[0]') AS INTEGER) AS parent,
                         CAST(json_extract(value, '$[1]') AS INTEGER) AS generation
                     FROM json_each(?1)
                 ) scope ON scope.parent = r.parent
                       AND scope.generation = r.parent_generation
                 GROUP BY r.parent, r.parent_generation, r.parent_turn_id",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([pairs], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(sql_error)?;
        let mut bindings = NativeChildBindings::default();
        for row in rows {
            let (parent, generation, turn_id, child_count) = row.map_err(sql_error)?;
            let child_count = usize::try_from(child_count)
                .map_err(|_| Error::new("internal", "native-child count is invalid"))?;
            if let Some(turn_id) = turn_id {
                bindings
                    .by_turn
                    .insert((parent, generation, turn_id), child_count);
            } else {
                *bindings
                    .unbound_by_generation
                    .entry((parent, generation))
                    .or_default() += child_count;
                bindings.unbound_total = bindings.unbound_total.saturating_add(child_count);
            }
        }
        Ok(bindings)
    }

    /// Revision of this Store writer's committed database view. The pair is
    /// sampled on the same connection so local writes and external commits
    /// both invalidate usage read memoization.
    pub fn usage_revision(&self) -> Result<(i64, i64)> {
        self.ensure_committed_reads()?;
        let total_changes = self
            .connection
            .query_row("SELECT total_changes()", [], |row| row.get(0))
            .map_err(sql_error)?;
        let data_version = self
            .connection
            .query_row("PRAGMA data_version", [], |row| row.get(0))
            .map_err(sql_error)?;
        Ok((total_changes, data_version))
    }

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
                resolve_agent_scope(&self.connection, vec![(num, row.generation)])?.pairs
            };
            let mut children = self.usage_native_descendants(num, request.generation, bound)?;
            let truncated = children.len() as i64 >= bound;
            children.truncate(bound.saturating_sub(1).max(0) as usize);
            let children = resolve_agent_scope(&self.connection, children)?;
            let agents_included = 1 + children.agents_included;
            pairs.extend(children.pairs);
            return Ok(ResolvedUsageScope {
                pairs,
                agents_included,
                truncated,
            });
        }
        if let Some(project) = request.project.as_deref() {
            return resolve_project_usage(&self.connection, project, bound, (None, None, false));
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
        let mut scope = resolve_agent_scope(&self.connection, agents)?;
        scope.truncated = truncated;
        Ok(scope)
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
        self.ensure_committed_reads()?;
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
        let native_child_bindings = self.native_child_bindings(pairs)?;
        let agent_ids: BTreeSet<i64> = wanted.iter().map(|(agent, _)| *agent).collect();
        let mut role_usage = RoleUsage::default();
        let mut turns = TurnExpenditureAccum::default();
        let mut first_calls = BTreeMap::new();
        let mut first_calls_truncated = false;
        let mut native_child_turns = BTreeMap::new();
        let mut native_child_turns_truncated = false;
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
            for_each_scoped_usage_turn(&self.connection, pairs, true, |row| {
                let ScopedUsageTurn {
                    agent,
                    generation,
                    state,
                    usage,
                    harness,
                    turn_id,
                    settled_at,
                    manager,
                } = row;
                let has_native_children = if native_child_bindings.by_turn.contains_key(&(
                    agent,
                    generation,
                    turn_id.clone(),
                )) {
                    Some(true)
                } else if native_child_bindings
                    .unbound_by_generation
                    .contains_key(&(agent, generation))
                {
                    None
                } else {
                    Some(false)
                };
                native_child_turns.insert(
                    (settled_at, agent, generation, turn_id.clone()),
                    has_native_children,
                );
                if native_child_turns.len() > MAX_NATIVE_CHILD_TURN_MARKERS {
                    native_child_turns.pop_first();
                    native_child_turns_truncated = true;
                }
                let Some(usage) = turns.observe(&state, usage.as_deref(), &harness)? else {
                    return Ok(());
                };
                role_usage.observe_turn(manager, &usage)?;
                if let Some(input_tokens) = usage.first_call_input_tokens {
                    first_calls.insert((settled_at, agent, generation, turn_id), input_tokens);
                    if first_calls.len() > MAX_FIRST_CALL_TURNS {
                        first_calls.pop_first();
                        first_calls_truncated = true;
                    }
                }
                Ok(())
            })?;
        }
        self.accumulate_usage_context(pairs, &mut role_usage)?;
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
            turns: turns.finish(),
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
            native_child_turns: native_child_turns
                .into_iter()
                .rev()
                .map(|((_, agent, generation, turn_id), has_native_children)| {
                    NativeChildTurnMarker {
                        agent: format_agent_id(agent),
                        generation,
                        turn_id,
                        has_native_children,
                    }
                })
                .collect(),
            native_child_turns_truncated,
            native_child_registrations_unbound: native_child_bindings.unbound_total,
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
                        "SELECT t.state, t.started_at, t.completed_at, t.usage, c.id,
                                t.native_trigger IS NOT NULL AND t.start_command_id IS NULL
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
                                native_commandless: row.get(5)?,
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
                        if turn.native_commandless {
                            // The exact Agent generation is the native actor
                            // when a self-started turn has no SEND command.
                            QuotaOriginKind::Automation
                        } else {
                            QuotaOriginKind::Unattributed
                        }
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

    /// Both Doctor usage summaries over the newest retained turn rows: the
    /// average first-call input tokens by exact profile snapshot, and the
    /// parent-turn native-child comparison over that window. One turn scan
    /// serves both summaries, with the row cap applied before filtering, the
    /// current-generation join, and one usage decode per row; rows decode
    /// only when completed with a current-generation profile and harness, as
    /// before. Unknown legacy child attribution is counted separately;
    /// medians use only the parent's turn-level usage.
    pub fn doctor_usage_summaries(
        &self,
    ) -> Result<(
        Vec<ProfileFirstCallInput>,
        bool,
        Vec<ProfileNativeChildUsage>,
        bool,
    )> {
        let mut statement = self
            .connection
            .prepare(
                "SELECT a.profile, a.harness, json_extract(a.profile_snapshot, '$.mcp'),
                        t.agent, t.generation, t.turn_id, t.usage, t.state
                 FROM (
                     SELECT rowid AS sequence, agent, generation, turn_id, usage, state
                     FROM turns ORDER BY rowid DESC LIMIT ?1
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
        let mut by_first_call = BTreeMap::new();
        let mut pairs = BTreeSet::new();
        let mut samples = Vec::new();
        while let Some(row) = rows.next().map_err(sql_error)? {
            if examined == PROFILE_FIRST_CALL_WINDOW {
                truncated = true;
                break;
            }
            examined += 1;
            if row.get::<_, String>(7).map_err(sql_error)? != "completed" {
                continue;
            }
            let (Some(profile), Some(harness)) = (
                row.get::<_, Option<String>>(0).map_err(sql_error)?,
                row.get::<_, Option<String>>(1).map_err(sql_error)?,
            ) else {
                continue;
            };
            let agent = row.get::<_, i64>(3).map_err(sql_error)?;
            let generation = row.get::<_, i64>(4).map_err(sql_error)?;
            pairs.insert((agent, generation));
            let mcp_policy = row.get::<_, Option<String>>(2).map_err(sql_error)?;
            let usage = row
                .get::<_, Option<String>>(6)
                .map_err(sql_error)?
                .map(|usage| decode_turn_usage(&usage, &harness))
                .transpose()?;
            if let Some(input) = usage
                .as_ref()
                .and_then(|usage| usage.first_call_input_tokens)
            {
                let (sum, count) = by_first_call
                    .entry((profile.clone(), harness.clone(), mcp_policy.clone()))
                    .or_insert((0_i128, 0));
                *sum += i128::from(input);
                *count += 1;
            }
            samples.push((
                profile,
                harness,
                mcp_policy,
                agent,
                generation,
                row.get::<_, String>(5).map_err(sql_error)?,
                usage,
            ));
        }
        drop(rows);
        drop(statement);

        let pairs: Vec<_> = pairs.into_iter().collect();
        let bindings = self.native_child_bindings(&pairs)?;
        let mut by_profile = BTreeMap::new();
        for (profile, harness, mcp_policy, agent, generation, turn_id, usage) in samples {
            let has_children =
                if bindings
                    .by_turn
                    .contains_key(&(agent, generation, turn_id.clone()))
                {
                    Some(true)
                } else if bindings
                    .unbound_by_generation
                    .contains_key(&(agent, generation))
                {
                    None
                } else {
                    Some(false)
                };
            let sample = by_profile
                .entry((profile, harness, mcp_policy))
                .or_insert_with(ProfileNativeChildAccum::default);
            sample.sampled_turns += 1;
            match has_children {
                Some(true) => sample.turns_with_children += 1,
                Some(false) => sample.turns_without_children += 1,
                None => sample.turns_with_unknown_children += 1,
            }
            let Some(usage) = usage else {
                continue;
            };
            if !matches!(usage.scope.as_deref(), Some("turn" | "main_loop_turn")) {
                continue;
            }
            let Some(tokens) = usage.reported_total() else {
                continue;
            };
            match has_children {
                Some(true) => sample.tokens_with_children.push(tokens),
                Some(false) => sample.tokens_without_children.push(tokens),
                None => {}
            }
        }
        let profile_first_calls = by_first_call
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
        let profile_native_child_usage = by_profile
            .into_iter()
            .map(|((profile, harness, mcp_policy), mut sample)| {
                let median_parent_tokens_with_children =
                    median_tokens(&mut sample.tokens_with_children);
                let median_parent_tokens_without_children =
                    median_tokens(&mut sample.tokens_without_children);
                ProfileNativeChildUsage {
                    profile,
                    harness,
                    mcp_policy,
                    sampled_turns: sample.sampled_turns,
                    turns_with_children: sample.turns_with_children,
                    turns_without_children: sample.turns_without_children,
                    turns_with_unknown_children: sample.turns_with_unknown_children,
                    token_samples_with_children: sample.tokens_with_children.len(),
                    median_parent_tokens_with_children,
                    token_samples_without_children: sample.tokens_without_children.len(),
                    median_parent_tokens_without_children,
                }
            })
            .collect();
        Ok((
            profile_first_calls,
            truncated,
            profile_native_child_usage,
            truncated,
        ))
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
        pairs: &[(i64, i64)],
        role_usage: &mut RoleUsage,
    ) -> Result<()> {
        if pairs.is_empty() {
            return Ok(());
        }
        let sql = "WITH wanted AS MATERIALIZED (
                 SELECT DISTINCT json_extract(value, '$[0]') AS agent,
                                 json_extract(value, '$[1]') AS generation FROM json_each(?1)
             ), roles AS MATERIALIZED (
                 SELECT w.agent, w.generation,
                        (SELECT MIN(child.created_at)
                         FROM agents AS child INDEXED BY autonomy_child_creation
                         WHERE child.parent = w.agent) AS first_child
                 FROM wanted AS w
             )
             SELECT o.tokens, o.error, COALESCE(r.first_child <= o.ts, 0)
             FROM roles AS r CROSS JOIN usage_context_observations AS o
               ON o.agent = r.agent AND o.generation = r.generation
             ORDER BY o.seq";
        let mut statement = self.connection.prepare_cached(sql).map_err(sql_error)?;
        let mut rows = statement.query([encode_json(&pairs)?]).map_err(sql_error)?;
        while let Some(row) = rows.next().map_err(sql_error)? {
            if let Some(error) = row.get::<_, Option<String>>(1).map_err(sql_error)? {
                return Err(serde_json::from_str(&error).map_err(|_| {
                    Error::new("source_integrity", "invalid context projection error")
                })?);
            }
            role_usage.observe_context(
                row.get(2).map_err(sql_error)?,
                row.get(0).map_err(sql_error)?,
            );
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
