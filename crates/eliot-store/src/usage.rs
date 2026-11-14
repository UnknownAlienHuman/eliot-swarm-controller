//! Bounded usage totals with explicit coverage over one scope.
//!
//! Owning contracts: `docs/spec/core.md` (usage) and `docs/spec/api.md`
//! (`usage.summary`). Turn expenditure never merges native scopes: each
//! scope (`turn`, `main_loop_turn`, `message`, `unknown`) reports its own
//! subtotals with the turns counted and the per-counter gaps. Session facts
//! read the latest `session_usage` snapshot per Agent generation in scope;
//! cumulative counters sum only where every included snapshot supplies them,
//! and cost sums only within one verbatim unit, never converted. Scope
//! expansion reuses the hierarchy fanout bound and reports truncation; the
//! retained-event watermark names the retention floor instead of implying
//! full totals.

use super::*;
use super::{hierarchy, retention};
use eliot_core::api::{
    SessionCostTotal, SessionCumulativeTotals, SessionFacts, SessionTokenSubtotal,
    TurnCounterMissing, TurnCounterTotals, TurnExpenditure, TurnScopeTotals, UsageScopeView,
    UsageSummaryRequest, UsageSummaryResponse,
};
use rusqlite::params_from_iter;
use std::collections::{BTreeMap, BTreeSet};

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
    /// one; project and tree scopes cover every retained generation of each
    /// included Agent. Expansion stops at the hierarchy fanout bound and
    /// reports truncation. No authorization happens here; the Router filters
    /// these pairs against the caller's frozen scope before summarizing.
    pub fn resolve_usage_scope(&self, request: &UsageSummaryRequest) -> Result<ResolvedUsageScope> {
        request.validate()?;
        let bound = self.hierarchy_limits.max_selector_targets;
        if let Some(agent) = request.agent.as_deref() {
            let num = parse_agent_id(agent)?;
            let row = load_agent_by_num(&self.connection, num)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {agent} was not found")))?;
            if let Some(generation) = request.generation {
                return Ok(ResolvedUsageScope {
                    pairs: vec![(num, generation)],
                    agents_included: 1,
                    truncated: false,
                });
            }
            let mut generations = BTreeSet::new();
            generations.insert(row.generation);
            for generation in self.retained_generations(&[num])? {
                generations.insert(generation.1);
            }
            return Ok(ResolvedUsageScope {
                pairs: generations
                    .into_iter()
                    .map(|generation| (num, generation))
                    .collect(),
                agents_included: 1,
                truncated: false,
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
        let mut overall_supplied = [0_usize; TURN_COUNTERS];
        let mut overall_sums = [0_i128; TURN_COUNTERS];
        let mut scopes: BTreeMap<String, ScopeAccum> = BTreeMap::new();
        if !wanted.is_empty() {
            let nums: BTreeSet<i64> = wanted.iter().map(|(num, _)| *num).collect();
            let nums: Vec<i64> = nums.into_iter().collect();
            let placeholders = nums.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "SELECT agent, generation, usage FROM turns \
                 WHERE state = 'completed' AND agent IN ({placeholders})"
            );
            let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
            let rows = statement
                .query_map(params_from_iter(nums.iter()), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                })
                .map_err(sql_error)?;
            for row in rows {
                let (agent, generation, usage) = row.map_err(sql_error)?;
                if !wanted.contains(&(agent, generation)) {
                    continue;
                }
                turns_counted += 1;
                let Some(usage) = usage else {
                    turns_without_usage += 1;
                    continue;
                };
                let usage = decode_turn_usage(&usage)?;
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
        Ok(UsageSummaryResponse {
            scope,
            agents_included,
            generations_included: wanted.len(),
            truncated,
            history_complete,
            history_reason,
            pruned_through,
            turns: TurnExpenditure {
                turns_counted,
                turns_without_usage,
                turns_missing: counter_missing(turns_counted, &overall_supplied),
                by_scope,
            },
            sessions,
        })
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
        if !wanted.is_empty() {
            let nums: BTreeSet<i64> = wanted.iter().map(|(num, _)| *num).collect();
            let nums: Vec<i64> = nums.into_iter().collect();
            let placeholders = nums.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!(
                "SELECT agent, generation, snapshot FROM session_usage WHERE agent IN ({placeholders})"
            );
            let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
            let rows = statement
                .query_map(params_from_iter(nums.iter()), |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })
                .map_err(sql_error)?;
            for row in rows {
                let (agent, generation, snapshot) = row.map_err(sql_error)?;
                if !wanted.contains(&(agent, generation)) {
                    continue;
                }
                let usage = decode_session_usage(&snapshot)?;
                snapshots += 1;
                if usage.context_used_tokens.is_some() {
                    occupancy += 1;
                }
                if usage.context_window_tokens.is_some() {
                    capacity += 1;
                }
                let counters = [
                    usage
                        .cumulative_tokens
                        .as_ref()
                        .and_then(|tokens| tokens.input_tokens),
                    usage
                        .cumulative_tokens
                        .as_ref()
                        .and_then(|tokens| tokens.output_tokens),
                    usage
                        .cumulative_tokens
                        .as_ref()
                        .and_then(|tokens| tokens.cache_read_tokens),
                    usage
                        .cumulative_tokens
                        .as_ref()
                        .and_then(|tokens| tokens.cache_write_tokens),
                ];
                for (index, value) in counters.into_iter().enumerate() {
                    if let Some(value) = value {
                        sums[index] += i128::from(value);
                        supplied[index] += 1;
                    }
                }
                match usage.cumulative_cost.as_ref() {
                    Some(cost)
                        if cost
                            .amount
                            .is_some_and(|amount| amount.is_finite() && amount >= 0.0)
                            && cost
                                .unit
                                .as_deref()
                                .is_some_and(|unit| !unit.trim().is_empty()) =>
                    {
                        let entry = costs
                            .entry(cost.unit.clone().expect("cost unit present"))
                            .or_insert((0.0, 0));
                        entry.0 += cost.amount.expect("cost amount present");
                        entry.1 += 1;
                    }
                    _ => snapshots_without_cost += 1,
                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{CommandState, NativeEvent, SourceBatch, SubmitCommand};
    use serde_json::json;
    use std::path::Path;
    use ulid::Ulid;

    fn now_ms() -> i64 {
        i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64")
    }

    fn launch(store: &mut Store, project: &str, now: i64) -> String {
        let id = Ulid::new().to_string();
        let agent = store
            .submit(
                "owner",
                SubmitCommand {
                    id: id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({
                        "project": project,
                        "workspace": format!("synthetic-workspace-{id}"),
                        "harness": "codex",
                        "profile": "codex-fixture",
                        "profile_snapshot": {},
                    }),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch")
            .command
            .target
            .expect("launch target");
        store.dispatch(&id, now).expect("dispatch launch");
        let plan: BTreeMap<String, String> = ["session", "turn", "message", "tool", "request"]
            .into_iter()
            .map(|family| (family.to_owned(), "stream".to_owned()))
            .collect();
        store
            .install_plugin_sources(&agent, 1, &id, &plan, now)
            .expect("install sources");
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{project}:ready"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: format!("{project}:ready"),
                        data: json!({"command_id": id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&id, CommandState::Applied, None, now)
            .expect("settle launch");
        agent
    }

    fn insert_turn(
        store: &Store,
        agent: &str,
        generation: i64,
        turn_id: &str,
        usage: Option<serde_json::Value>,
    ) {
        let num = parse_agent_id(agent).expect("valid agent id");
        let usage = usage.map(|usage| serde_json::to_string(&usage).expect("encode usage"));
        store
            .connection
            .execute(
                "INSERT INTO turns(agent, generation, turn_id, state, status, outcome, usage, started_at)
                 VALUES (?1, ?2, ?3, 'completed', 'completed', 'success', ?4, 1)",
                params![num, generation, turn_id, usage],
            )
            .expect("insert turn");
    }

    fn insert_session(store: &Store, agent: &str, generation: i64, snapshot: serde_json::Value) {
        let num = parse_agent_id(agent).expect("valid agent id");
        store
            .connection
            .execute(
                "INSERT INTO session_usage(agent, generation, snapshot, observed_at)
                 VALUES (?1, ?2, ?3, 1)",
                params![
                    num,
                    generation,
                    serde_json::to_string(&snapshot).expect("encode snapshot")
                ],
            )
            .expect("insert session snapshot");
    }

    fn agent_scope(agent: &str) -> UsageSummaryRequest {
        UsageSummaryRequest {
            agent: Some(agent.to_owned()),
            generation: None,
            project: None,
            root: None,
        }
    }

    fn summarize(store: &Store, request: &UsageSummaryRequest) -> UsageSummaryResponse {
        let resolved = store
            .resolve_usage_scope(request)
            .expect("resolve usage scope");
        let scope = UsageScopeView {
            agent: request.agent.clone(),
            generation: request.generation,
            project: request.project.clone(),
            root: request.root.clone(),
        };
        let pairs = resolved.pairs.clone();
        let agents = resolved.agents_included;
        let truncated = resolved.truncated;
        let response = store
            .summarize_usage(scope, &pairs, agents, truncated)
            .expect("summarize usage");
        request
            .validate_response(&response)
            .expect("response binds the request");
        response
    }

    #[test]
    fn turn_expenditure_groups_scopes_and_reports_gaps() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = now_ms();
        let agent = launch(&mut store, "usage-mixed", now);
        insert_turn(
            &store,
            &agent,
            1,
            "t1",
            Some(json!({"scope": "turn", "input_tokens": 10, "output_tokens": 5})),
        );
        insert_turn(
            &store,
            &agent,
            1,
            "t2",
            Some(json!({"scope": "turn", "input_tokens": 7})),
        );
        insert_turn(
            &store,
            &agent,
            1,
            "t3",
            Some(json!({"scope": "message", "input_tokens": 3, "total_tokens": 9})),
        );
        insert_turn(&store, &agent, 1, "t4", None);

        let response = summarize(&store, &agent_scope(&agent));
        assert_eq!(response.turns.turns_counted, 4);
        assert_eq!(response.turns.turns_without_usage, 1);
        assert_eq!(response.turns.turns_missing.input_tokens, 1);
        assert_eq!(response.turns.turns_missing.output_tokens, 3);
        assert_eq!(response.turns.turns_missing.total_tokens, 3);

        // Scopes never merge: each keeps its own subtotal.
        assert_eq!(response.turns.by_scope.len(), 2);
        let turn = response
            .turns
            .by_scope
            .iter()
            .find(|scope| scope.scope == "turn")
            .expect("turn scope");
        assert_eq!(turn.turns_counted, 2);
        assert_eq!(turn.totals.input_tokens, Some(17));
        assert_eq!(turn.totals.output_tokens, Some(5));
        assert_eq!(turn.totals.total_tokens, None);
        assert_eq!(turn.turns_missing.input_tokens, 0);
        assert_eq!(turn.turns_missing.output_tokens, 1);
        let message = response
            .turns
            .by_scope
            .iter()
            .find(|scope| scope.scope == "message")
            .expect("message scope");
        assert_eq!(message.turns_counted, 1);
        assert_eq!(message.totals.input_tokens, Some(3));
        assert_eq!(message.totals.output_tokens, None);
        assert_eq!(message.totals.total_tokens, Some(9));

        // No cross-scope total exists: the message input never joins the
        // turn input, and the turn output never joins the message scope.
        assert!(
            response
                .turns
                .by_scope
                .iter()
                .all(|scope| { scope.scope == "turn" || scope.scope == "message" })
        );
    }

    #[test]
    fn session_snapshots_sum_only_where_supplied_and_group_cost() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = now_ms();
        let first = launch(&mut store, "usage-session", now);
        let second = launch(&mut store, "usage-session", now);
        let third = launch(&mut store, "usage-session", now);
        insert_session(
            &store,
            &first,
            1,
            json!({
                "context_used_tokens": 10,
                "context_window_tokens": 1000,
                "cumulative_tokens": {"input_tokens": 100, "output_tokens": 50},
                "cumulative_cost": {"amount": 0.5, "unit": "usd_estimate"},
                "source": "codex:thread/tokenUsage/updated",
            }),
        );
        insert_session(
            &store,
            &second,
            1,
            json!({
                "cumulative_tokens": {"input_tokens": 30},
                "cumulative_cost": {"amount": 2.0, "unit": "credits"},
                "source": "antigravity:usage_update",
            }),
        );

        let request = UsageSummaryRequest {
            agent: None,
            generation: None,
            project: Some("usage-session".to_owned()),
            root: None,
        };
        let response = summarize(&store, &request);
        assert_eq!(response.agents_included, 3);
        assert_eq!(response.generations_included, 3);
        assert!(!response.truncated);
        assert_eq!(response.sessions.snapshots, 2);
        assert_eq!(response.sessions.generations_without_snapshot, 1);

        let tokens = &response.sessions.cumulative_tokens;
        assert_eq!(tokens.input_tokens.total, Some(130));
        assert!(tokens.input_tokens.complete);
        assert_eq!(tokens.output_tokens.total, Some(50));
        assert_eq!(tokens.output_tokens.supplied, 1);
        assert_eq!(tokens.output_tokens.missing, 1);
        assert!(!tokens.output_tokens.complete);
        assert_eq!(tokens.cache_read_tokens.total, None);
        assert_eq!(tokens.cache_read_tokens.supplied, 0);

        // Cost groups by verbatim unit and never converts.
        assert_eq!(response.sessions.cost_by_unit.len(), 2);
        let usd = response
            .sessions
            .cost_by_unit
            .iter()
            .find(|cost| cost.unit == "usd_estimate")
            .expect("usd cost");
        assert_eq!(usd.total_amount, 0.5);
        assert_eq!(usd.snapshots, 1);
        let credits = response
            .sessions
            .cost_by_unit
            .iter()
            .find(|cost| cost.unit == "credits")
            .expect("credits cost");
        assert_eq!(credits.total_amount, 2.0);
        assert_eq!(response.sessions.snapshots_without_cost, 0);

        // Occupancy reads back as coverage only, never summed.
        assert_eq!(response.sessions.context_occupancy_supplied, 1);
        assert_eq!(response.sessions.context_capacity_supplied, 1);
        let _ = third;
    }

    #[test]
    fn project_and_tree_scopes_resolve_the_right_agents() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = now_ms();
        let parent = launch(&mut store, "usage-tree", now);
        let child = launch(&mut store, "usage-tree", now);
        let outsider = launch(&mut store, "usage-other", now);
        let parent_num = parse_agent_id(&parent).expect("parent num");
        let child_num = parse_agent_id(&child).expect("child num");
        store
            .connection
            .execute(
                "UPDATE agents SET root = ?1 WHERE id = ?1",
                params![parent_num],
            )
            .expect("root the parent");
        store
            .connection
            .execute(
                "UPDATE agents SET parent = ?1, root = ?1 WHERE id = ?2",
                params![parent_num, child_num],
            )
            .expect("attach the child");
        insert_turn(
            &store,
            &child,
            1,
            "t1",
            Some(json!({"scope": "turn", "input_tokens": 4})),
        );

        let project = summarize(
            &store,
            &UsageSummaryRequest {
                agent: None,
                generation: None,
                project: Some("usage-tree".to_owned()),
                root: None,
            },
        );
        assert_eq!(project.agents_included, 2);
        assert_eq!(project.generations_included, 2);
        assert_eq!(project.turns.turns_counted, 1);

        let tree = summarize(
            &store,
            &UsageSummaryRequest {
                agent: None,
                generation: None,
                project: None,
                root: Some(parent.clone()),
            },
        );
        assert_eq!(tree.agents_included, 2);
        assert_eq!(tree.turns.turns_counted, 1);

        // The outsider project sees only itself.
        let other = summarize(
            &store,
            &UsageSummaryRequest {
                agent: None,
                generation: None,
                project: Some("usage-other".to_owned()),
                root: None,
            },
        );
        assert_eq!(other.agents_included, 1);
        assert_eq!(other.turns.turns_counted, 0);

        // An unknown project resolves empty, never an error.
        let missing = summarize(
            &store,
            &UsageSummaryRequest {
                agent: None,
                generation: None,
                project: Some("usage-absent".to_owned()),
                root: None,
            },
        );
        assert_eq!(missing.agents_included, 0);
        assert_eq!(missing.generations_included, 0);
        let _ = outsider;
    }

    #[test]
    fn agent_scope_covers_every_generation_or_one_named() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = now_ms();
        let agent = launch(&mut store, "usage-generations", now);
        insert_turn(
            &store,
            &agent,
            1,
            "t1",
            Some(json!({"scope": "turn", "input_tokens": 10})),
        );
        insert_turn(
            &store,
            &agent,
            2,
            "t2",
            Some(json!({"scope": "turn", "input_tokens": 20})),
        );

        let all = summarize(&store, &agent_scope(&agent));
        assert_eq!(all.generations_included, 2);
        assert_eq!(all.turns.turns_counted, 2);

        let one = summarize(
            &store,
            &UsageSummaryRequest {
                agent: Some(agent.clone()),
                generation: Some(2),
                project: None,
                root: None,
            },
        );
        assert_eq!(one.generations_included, 1);
        assert_eq!(one.turns.turns_counted, 1);
        let turn = one
            .turns
            .by_scope
            .iter()
            .find(|scope| scope.scope == "turn")
            .expect("turn scope");
        assert_eq!(turn.totals.input_tokens, Some(20));
    }

    #[test]
    fn scope_expansion_truncates_at_the_hierarchy_bound() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        store
            .set_hierarchy_limits(eliot_core::api::HierarchyLimits {
                max_depth: 32,
                max_children_per_agent: 16,
                max_tree_agents: 100,
                max_selector_targets: 2,
            })
            .expect("tighten the bound");
        let now = now_ms();
        launch(&mut store, "usage-truncated", now);
        launch(&mut store, "usage-truncated", now);
        launch(&mut store, "usage-truncated", now);

        let request = UsageSummaryRequest {
            agent: None,
            generation: None,
            project: Some("usage-truncated".to_owned()),
            root: None,
        };
        let resolved = store
            .resolve_usage_scope(&request)
            .expect("resolve usage scope");
        assert!(resolved.truncated);
        assert_eq!(resolved.agents_included, 2);
        let pairs = resolved.pairs.clone();
        let agents = resolved.agents_included;
        let response = store
            .summarize_usage(
                UsageScopeView {
                    agent: None,
                    generation: None,
                    project: Some("usage-truncated".to_owned()),
                    root: None,
                },
                &pairs,
                agents,
                resolved.truncated,
            )
            .expect("summarize usage");
        assert!(response.truncated);
        assert_eq!(response.agents_included, 2);
    }

    #[test]
    fn retention_watermark_marks_pruned_history() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = now_ms();
        let agent = launch(&mut store, "usage-retention", now);

        let fresh = summarize(&store, &agent_scope(&agent));
        assert!(fresh.history_complete);
        assert_eq!(fresh.pruned_through, 0);

        store
            .connection
            .execute(
                "UPDATE rule_history_coverage SET pruned_through = 5 WHERE singleton = 1",
                [],
            )
            .expect("advance the watermark");
        let pruned = summarize(&store, &agent_scope(&agent));
        assert!(!pruned.history_complete);
        assert_eq!(pruned.pruned_through, 5);
        assert!(pruned.history_reason.contains('5'));
    }
}
