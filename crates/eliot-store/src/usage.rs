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
//! never converted. Scope
//! expansion reuses the hierarchy fanout bound and reports truncation; the
//! retained-event watermark names the retention floor instead of implying
//! full totals.

use super::*;
use super::{hierarchy, retention};
use eliot_core::api::{
    FirstCallInput, MAX_FIRST_CALL_TURNS, PROFILE_FIRST_CALL_WINDOW, ProfileFirstCallInput,
    SessionCostTotal, SessionCumulativeTotals, SessionFacts, SessionTokenSubtotal,
    TurnCounterMissing, TurnCounterTotals, TurnExpenditure, TurnScopeTotals, UsageScopeView,
    UsageSummaryRequest, UsageSummaryResponse,
};
use rusqlite::params_from_iter;
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
    /// pairs. Runs under the caller's Store lock, so the turn and session
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
            "no event history pruned; totals cover all retained terminal turns".to_owned()
        } else {
            format!(
                "event history pruned through seq {pruned_through}; totals cover retained terminal turns only"
            )
        };
        let wanted: std::collections::HashSet<(i64, i64)> = pairs.iter().copied().collect();
        let mut turns_counted = 0_usize;
        let mut turns_without_usage = 0_usize;
        let mut turns_unknown_excluded = 0_usize;
        let mut turns_running_excluded = 0_usize;
        let mut overall_supplied = [0_usize; TURN_COUNTERS];
        let mut overall_sums = [0_i128; TURN_COUNTERS];
        let mut scopes: BTreeMap<String, ScopeAccum> = BTreeMap::new();
        let mut first_calls = Vec::new();
        let mut first_calls_truncated = false;
        let mut observed_native_agents = 0i64;
        if !wanted.is_empty() {
            let nums: BTreeSet<i64> = wanted.iter().map(|(num, _)| *num).collect();
            let nums: Vec<i64> = nums.into_iter().collect();
            let placeholders = nums.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            observed_native_agents = self
                .connection
                .query_row(
                    &format!(
                        "SELECT COUNT(*) FROM agents
                          WHERE id IN ({placeholders}) AND origin = 'native' AND observe_only = 1"
                    ),
                    params_from_iter(nums.iter()),
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let sql = format!(
                "SELECT t.agent, t.generation, t.state, t.usage, a.harness, t.turn_id
                 FROM turns t JOIN agents a ON a.id = t.agent
                 WHERE t.agent IN ({placeholders})
                 ORDER BY t.agent, t.generation, t.turn_id"
            );
            let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
            let rows = statement
                .query_map(params_from_iter(nums.iter()), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, String>(5)?,
                    ))
                })
                .map_err(sql_error)?;
            for row in rows {
                let (agent, generation, state, usage, harness, turn_id) = row.map_err(sql_error)?;
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
                if let Some(input_tokens) = usage.first_call_input_tokens {
                    if first_calls.len() < MAX_FIRST_CALL_TURNS {
                        first_calls.push(FirstCallInput {
                            agent: format_agent_id(agent),
                            generation,
                            turn_id,
                            input_tokens,
                        });
                    } else {
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
        let sessions = self.summarize_sessions(&wanted)?;
        let (swarm_mcp, swarm_mcp_truncated) = self.swarm_mcp_turn_usage(pairs)?;
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
            first_calls,
            first_calls_truncated,
            swarm_mcp,
            swarm_mcp_truncated,
        })
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
