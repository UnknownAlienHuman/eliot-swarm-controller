//! Read-only agent views and their table rendering.
use super::args::{
    EditsArgs, EventsArgs, GenerationArgs, GitStatusArgs, HistoryArgs, ListFilterArgs, LsArgs,
    PageArgs, ProcessesArgs, ReadGenerationArgs, ResultArgs, ShowArgs, StatsArgs, TopArgs,
    UsageArgs, WatchArgs,
};
use super::support::read_and_print;
use super::support::{Failure, PAGE_SIZE, print_value, validate_limit};
use eliot_core::api::{
    AgentEditsRequest, AgentEditsResponse, AgentGetRequest, AgentGetResponse, AgentGitRequest,
    AgentGitResponse, AgentHistoryRequest, AgentListRequest, AgentListResponse,
    AgentProcessesRequest, AgentProcessesResponse, AgentResultRequest, AgentResultResponse,
    AgentStatsRequest, AgentStatsResponse, EventsBoundsRequest, EventsBoundsResponse, EventsFilter,
    EventsReadRequest, EventsReadResponse, EventsSubscribeRequest, EventsSubscribeResponse,
    ObservationPage, TurnCounterMissing, UsageSummaryRequest, UsageSummaryResponse,
};
use eliot_core::{AgentState, NativeChildActivityState};
use serde_json::Value;
use std::path::{Path, PathBuf};

pub(crate) async fn run_ls(
    LsArgs {
        after,
        page: PageArgs { limit },
        filter,
    }: LsArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    validate_limit(limit)?;
    let params = list_params(after, limit, false, filter)?;
    if json_output {
        read_and_print!(
            &data_dir,
            json_output,
            "agent.list",
            params,
            AgentListResponse
        )
    } else {
        let response = read_agent_list(&data_dir, &params).await?;
        print_agent_table(&response, false);
        Ok(0)
    }
}

pub(crate) async fn run_top(
    TopArgs {
        after,
        page: PageArgs { limit },
        filter,
    }: TopArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    validate_limit(limit)?;
    let params = list_params(after, limit, true, filter)?;
    if json_output {
        read_and_print!(
            &data_dir,
            json_output,
            "agent.list",
            params,
            AgentListResponse
        )
    } else {
        let response = read_agent_list(&data_dir, &params).await?;
        print_agent_table(&response, true);
        Ok(0)
    }
}

pub(crate) async fn run_show(
    ShowArgs { agent }: ShowArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    read_and_print!(
        &data_dir,
        json_output,
        "agent.get",
        AgentGetRequest::new(agent),
        AgentGetResponse
    )
}

pub(crate) async fn run_processes(
    ProcessesArgs {
        agent,
        generation: ReadGenerationArgs { generation },
        scope,
        after,
        page: PageArgs { limit },
    }: ProcessesArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    validate_limit(limit)?;
    let params = AgentProcessesRequest {
        id: agent,
        generation,
        scope: Some(scope),
        after,
        limit,
    };
    read_and_print!(
        &data_dir,
        json_output,
        "agent.processes",
        params,
        AgentProcessesResponse
    )
}

pub(crate) async fn run_stats(
    StatsArgs {
        agent,
        generation: ReadGenerationArgs { generation },
    }: StatsArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let params = AgentStatsRequest {
        id: agent,
        generation,
    };
    read_and_print!(
        &data_dir,
        json_output,
        "agent.stats",
        params,
        AgentStatsResponse
    )
}

pub(crate) async fn run_usage(
    UsageArgs {
        agent,
        generation: ReadGenerationArgs { generation },
        project,
        root,
    }: UsageArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let params = UsageSummaryRequest {
        agent,
        generation,
        project,
        root,
    };
    if json_output {
        read_and_print!(
            &data_dir,
            json_output,
            "usage.summary",
            params,
            UsageSummaryResponse
        )
    } else {
        params
            .validate()
            .map_err(|error| Failure::from_core(error, false))?;
        let response: UsageSummaryResponse =
            eliot_daemon::client_call_typed(&data_dir, "usage.summary", &params)
                .await
                .map_err(|error| Failure::from_core(error, false))?;
        params
            .validate_response(&response)
            .map_err(|error| Failure::from_core(error, false))?;
        print_usage_table(&response);
        Ok(0)
    }
}

pub(crate) async fn run_git_status(
    GitStatusArgs {
        agent,
        generation: ReadGenerationArgs { generation },
    }: GitStatusArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let params = AgentGitRequest {
        id: agent,
        generation,
    };
    read_and_print!(
        &data_dir,
        json_output,
        "agent.git",
        params,
        AgentGitResponse
    )
}

pub(crate) async fn run_edits(
    EditsArgs {
        agent,
        generation: ReadGenerationArgs { generation },
        after,
        page: PageArgs { limit },
    }: EditsArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    validate_limit(limit)?;
    let params = AgentEditsRequest {
        id: agent,
        generation,
        after,
        limit,
    };
    read_and_print!(
        &data_dir,
        json_output,
        "agent.edits",
        params,
        AgentEditsResponse
    )
}

pub(crate) async fn run_watch(
    WatchArgs { agent, after }: WatchArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    watch(&data_dir, agent.as_deref(), after, json_output).await
}

pub(crate) async fn run_result(
    ResultArgs {
        agent,
        generation: GenerationArgs { generation },
        turn_id,
        page: PageArgs { limit },
    }: ResultArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    validate_limit(limit)?;
    let params = AgentResultRequest {
        id: agent,
        generation,
        turn_id,
        limit,
    };
    read_and_print!(
        &data_dir,
        json_output,
        "agent.result",
        params,
        AgentResultResponse
    )
}

pub(crate) async fn run_history(
    HistoryArgs {
        agent,
        generation: ReadGenerationArgs { generation },
        source,
        view,
        turn_id,
        cursor,
        page: PageArgs { limit },
    }: HistoryArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    validate_limit(limit)?;
    let params = AgentHistoryRequest {
        id: agent,
        generation,
        source,
        view,
        turn_id,
        cursor,
        limit,
    };
    read_and_print!(
        &data_dir,
        json_output,
        "agent.history",
        params,
        ObservationPage
    )
}

pub(crate) async fn run_events(
    EventsArgs {
        since,
        page: PageArgs { limit },
    }: EventsArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    validate_limit(limit)?;
    let params = EventsReadRequest {
        after: since.unwrap_or(0),
        limit,
        filter: EventsFilter::default(),
    };
    params
        .validate()
        .map_err(|error| Failure::from_core(error, false))?;
    let response: EventsReadResponse =
        eliot_daemon::client_call_typed(&data_dir, "events.read", &params)
            .await
            .map_err(|error| Failure::from_core(error, false))?;
    response
        .validate(params.after)
        .map_err(|error| Failure::from_core(error, false))?;
    let result = serde_json::to_value(response)
        .map_err(|_| Failure::new(1, "unavailable: cannot render typed events.read response"))?;
    print_value(&result, json_output);
    Ok(0)
}

pub(crate) async fn watch(
    data_dir: &Path,
    agent: Option<&str>,
    after: Option<i64>,
    json_output: bool,
) -> Result<u8, Failure> {
    let filter = EventsFilter {
        agents: agent.into_iter().map(str::to_owned).collect(),
        ..EventsFilter::default()
    };
    let mut after = match after {
        Some(after) => after,
        None => {
            let params = EventsBoundsRequest {
                filter: filter.clone(),
            };
            params
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let bounds: EventsBoundsResponse =
                eliot_daemon::client_call_typed(data_dir, "events.bounds", &params)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            bounds
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            bounds.hi
        }
    };
    loop {
        let params = EventsSubscribeRequest {
            after,
            limit: PAGE_SIZE,
            wait_ms: 1_000,
            filter: filter.clone(),
        };
        params
            .validate()
            .map_err(|error| Failure::from_core(error, false))?;
        let response: EventsSubscribeResponse = tokio::select! {
            result = eliot_daemon::client_call_typed(data_dir, "events.subscribe", &params) => {
                result.map_err(|error| Failure::from_core(error, false))?
            }
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|error| Failure::new(1, format!("cannot listen for Ctrl+C: {error}")))?;
                return Ok(0);
            }
        };
        response
            .validate(params.after)
            .map_err(|error| Failure::from_core(error, false))?;
        for event in &response.events {
            let value = serde_json::to_value(event).map_err(|_| {
                Failure::new(1, "unavailable: cannot render typed events.subscribe event")
            })?;
            print_value(&value, json_output);
        }
        // The server's scan cursor includes filtered-out records as well.
        after = response.next;
    }
}

/// Build a compact `agent.list` request from CLI paging, scope filters and the
/// observations flag behind `swarm top`. Unknown state names reject here so a
/// typo never reads back as an empty fleet.
pub(crate) fn list_params(
    after: Option<String>,
    limit: usize,
    observations: bool,
    filter: ListFilterArgs,
) -> Result<AgentListRequest, Failure> {
    let state = filter
        .state
        .map(|value| {
            serde_json::from_value::<AgentState>(Value::String(value)).map_err(|_| {
                Failure::new(
                    2,
                    "state must be unknown, created, starting, idle, busy, waiting, stopped, failed or lost",
                )
            })
        })
        .transpose()?;
    Ok(AgentListRequest {
        after,
        limit,
        observations,
        state,
        project: filter.project,
        harness: filter.harness,
        parent: filter.parent,
        root: filter.root,
    })
}

pub(crate) async fn read_agent_list(
    data_dir: &Path,
    params: &AgentListRequest,
) -> Result<AgentListResponse, Failure> {
    params
        .validate()
        .map_err(|error| Failure::from_core(error, false))?;
    let response: AgentListResponse =
        eliot_daemon::client_call_typed(data_dir, "agent.list", params)
            .await
            .map_err(|error| Failure::from_core(error, false))?;
    params
        .validate_response(&response)
        .map_err(|error| Failure::from_core(error, false))?;
    Ok(response)
}

/// Readable fleet table for `swarm ls` (summaries) and `swarm top` (plus
/// child activity, last-terminal usage and observation data availability).
/// `ACT_TURN` is the live active turn; `LAST_OUT`/`LAST_TOK` belong to the
/// last terminal turn (`last_turn_id`), not the active one. `DATA` reports
/// observation data availability only, never freshness or liveness. `--json`
/// keeps printing the raw response; this table never replaces it.
pub(crate) fn print_agent_table(response: &AgentListResponse, with_observations: bool) {
    if with_observations {
        println!(
            "{:<6} {:<4} {:<9} {:<11} {:<12} {:<6} {:<11} {:<8} {:<12}",
            "ID", "GEN", "STATE", "ACTIVITY", "ACT_TURN", "AGE", "LAST_OUT", "LAST_TOK", "DATA"
        );
    } else {
        println!(
            "{:<6} {:<4} {:<9} {:<14} {:<9} {:<12} {:<6} {:<11} {:<6}",
            "ID", "GEN", "STATE", "PROJECT", "HARNESS", "ACT_TURN", "AGE", "LAST_OUT", "SEEN"
        );
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0);
    for row in &response.agents {
        let summary = &row.summary;
        let turn = summary.active_turn.as_deref().unwrap_or("-");
        let age = age_cell(summary.progress_age_ms);
        let outcome = summary
            .last_outcome
            .map(|outcome| outcome.as_str())
            .unwrap_or("-");
        if with_observations {
            let activity = summary
                .activity
                .as_ref()
                .map(|activity| activity_state_name(activity.state))
                .unwrap_or("-");
            let tokens = summary
                .usage
                .as_ref()
                .and_then(|usage| usage.total_tokens)
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "-".to_owned());
            let observation = row
                .observation
                .as_ref()
                .map(|view| match view {
                    eliot_core::api::ObservationView::Data(_) => "data".to_owned(),
                    eliot_core::api::ObservationView::Unavailable { error_code, .. } => {
                        truncate_cell(error_code, 12)
                    }
                })
                .unwrap_or_else(|| "-".to_owned());
            println!(
                "{:<6} {:<4} {:<9} {:<11} {:<12} {:<6} {:<11} {:<8} {:<12}",
                truncate_cell(&summary.id, 6),
                summary.generation,
                state_cell(summary.state),
                activity,
                truncate_cell(turn, 12),
                age,
                outcome,
                tokens,
                observation,
            );
        } else {
            let seen = age_cell(
                now_ms
                    .checked_sub(summary.updated_at)
                    .filter(|age| *age >= 0),
            );
            println!(
                "{:<6} {:<4} {:<9} {:<14} {:<9} {:<12} {:<6} {:<11} {:<6}",
                truncate_cell(&summary.id, 6),
                summary.generation,
                state_cell(summary.state),
                truncate_cell(&summary.project, 14),
                truncate_cell(&summary.harness, 9),
                truncate_cell(turn, 12),
                age,
                outcome,
                seen,
            );
        }
    }
    if response.agents.is_empty() {
        println!("(no Agents in scope for these filters)");
    }
    println!(
        "total {} shown {} truncated {} next {} (full record: swarm show <id>)",
        response.total,
        response.agents.len(),
        yes_no(response.truncated),
        response.next.as_deref().unwrap_or("-"),
    );
}

/// Readable usage summary for `swarm usage`. Turn scopes never merge: each
/// row is one native attribution scope with its own subtotals, and `--`
/// marks a counter no counted turn supplied. Session cumulative counters
/// carry their coverage; cost sums only within one verbatim unit. `--json`
/// keeps printing the raw response; this table never replaces it.
pub(crate) fn print_usage_table(response: &UsageSummaryResponse) {
    let scope = if let Some(agent) = &response.scope.agent {
        match response.scope.generation {
            Some(generation) => format!("agent {agent}#{generation}"),
            None => format!("agent {agent} (all generations)"),
        }
    } else if let Some(project) = &response.scope.project {
        format!("project {project}")
    } else if let Some(root) = &response.scope.root {
        format!("tree {root}")
    } else {
        "-".to_owned()
    };
    println!(
        "usage {scope} agents {} generations {} truncated {} history {} (pruned_through {})",
        response.agents_included,
        response.generations_included,
        yes_no(response.truncated),
        if response.history_complete {
            "complete"
        } else {
            "partial"
        },
        response.pruned_through,
    );
    println!("  {}", response.history_reason);
    println!(
        "  turns counted {} with_usage {} without_usage {} excluded unknown {} running {}",
        response.turns.turns_counted,
        response.turns.turns_counted - response.turns.turns_without_usage,
        response.turns.turns_without_usage,
        response.turns.turns_unknown_excluded,
        response.turns.turns_running_excluded,
    );
    println!(
        "  {:<14} {:<7} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "SCOPE", "COUNTED", "INPUT", "OUTPUT", "REASON", "THINK", "CACHE_R", "CACHE_W", "TOTAL"
    );
    for entry in &response.turns.by_scope {
        println!(
            "  {:<14} {:<7} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
            truncate_cell(&entry.scope, 14),
            entry.turns_counted,
            counter_cell(entry.totals.input_tokens),
            counter_cell(entry.totals.output_tokens),
            counter_cell(entry.totals.reasoning_tokens),
            counter_cell(entry.totals.thinking_tokens),
            counter_cell(entry.totals.cache_read_tokens),
            counter_cell(entry.totals.cache_write_tokens),
            counter_cell(entry.totals.total_tokens),
        );
    }
    println!(
        "  turns missing counters: {}",
        missing_cell(&response.turns.turns_missing)
    );
    println!(
        "  sessions snapshots {} without_snapshot {} occupancy {} capacity {}",
        response.sessions.snapshots,
        response.sessions.generations_without_snapshot,
        response.sessions.context_occupancy_supplied,
        response.sessions.context_capacity_supplied,
    );
    for (name, subtotal) in [
        ("input", &response.sessions.cumulative_tokens.input_tokens),
        ("output", &response.sessions.cumulative_tokens.output_tokens),
        (
            "cache_read",
            &response.sessions.cumulative_tokens.cache_read_tokens,
        ),
        (
            "cache_write",
            &response.sessions.cumulative_tokens.cache_write_tokens,
        ),
    ] {
        println!(
            "    cumulative {name}: total {} supplied {} missing {} complete {}",
            subtotal
                .total
                .map(|total| total.to_string())
                .as_deref()
                .unwrap_or("--"),
            subtotal.supplied,
            subtotal.missing,
            yes_no(subtotal.complete),
        );
    }
    if response.sessions.cost_by_unit.is_empty() {
        println!(
            "    cost: none (snapshots without cost {})",
            response.sessions.snapshots_without_cost
        );
    } else {
        for cost in &response.sessions.cost_by_unit {
            println!(
                "    cost [{}]: {} over {} snapshots",
                cost.unit, cost.total_amount, cost.snapshots
            );
        }
        println!(
            "    snapshots without cost {}",
            response.sessions.snapshots_without_cost
        );
    }
}

pub(crate) fn counter_cell(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "--".to_owned())
}

pub(crate) fn missing_cell(missing: &TurnCounterMissing) -> String {
    format!(
        "input {} output {} reasoning {} thinking {} cache_read {} cache_write {} total {}",
        missing.input_tokens,
        missing.output_tokens,
        missing.reasoning_tokens,
        missing.thinking_tokens,
        missing.cache_read_tokens,
        missing.cache_write_tokens,
        missing.total_tokens,
    )
}

pub(crate) fn state_cell(state: AgentState) -> &'static str {
    match state {
        AgentState::Unknown => "unknown",
        AgentState::Created => "created",
        AgentState::Starting => "starting",
        AgentState::Idle => "idle",
        AgentState::Busy => "busy",
        AgentState::Waiting => "waiting",
        AgentState::Stopped => "stopped",
        AgentState::Failed => "failed",
        AgentState::Lost => "lost",
    }
}

pub(crate) fn activity_state_name(state: NativeChildActivityState) -> &'static str {
    match state {
        NativeChildActivityState::Running => "running",
        NativeChildActivityState::Idle => "idle",
        NativeChildActivityState::Interrupted => "interrupted",
        NativeChildActivityState::Lost => "lost",
        NativeChildActivityState::Unknown => "unknown",
    }
}

pub(crate) fn age_cell(age_ms: Option<i64>) -> String {
    let Some(age_ms) = age_ms else {
        return "-".to_owned();
    };
    if age_ms < 0 {
        return "-".to_owned();
    }
    let seconds = age_ms / 1_000;
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3_600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3_600)
    } else {
        format!("{}d", seconds / 86_400)
    }
}

pub(crate) fn truncate_cell(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_owned();
    }
    if width == 0 {
        return String::new();
    }
    value
        .chars()
        .take(width.saturating_sub(1))
        .collect::<String>()
        + "~"
}

pub(crate) fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use super::{
        activity_state_name, age_cell, list_params, print_usage_table, state_cell, truncate_cell,
    };
    use crate::cli::args::ListFilterArgs;
    use eliot_core::api::{TurnCounterMissing, UsageSummaryRequest, UsageSummaryResponse};
    use eliot_core::{AgentState, NativeChildActivityState};
    #[test]
    fn list_params_rejects_unknown_state_before_any_call() {
        let filter = |state: Option<&str>| ListFilterArgs {
            state: state.map(str::to_owned),
            project: None,
            harness: None,
            parent: None,
            root: None,
        };
        let params = list_params(None, 10, false, filter(Some("busy"))).expect("busy parses");
        assert_eq!(params.state, Some(AgentState::Busy));
        assert!(list_params(None, 10, false, filter(Some("done"))).is_err());
        assert!(list_params(None, 10, false, filter(Some("BUSY"))).is_err());
    }

    #[test]
    fn fleet_table_cells_stay_compact() {
        assert_eq!(age_cell(None), "-");
        assert_eq!(age_cell(Some(-1)), "-");
        assert_eq!(age_cell(Some(45_000)), "45s");
        assert_eq!(age_cell(Some(180_000)), "3m");
        assert_eq!(age_cell(Some(7_200_000)), "2h");
        assert_eq!(age_cell(Some(172_800_000)), "2d");
        assert_eq!(truncate_cell("a1", 6), "a1");
        assert_eq!(truncate_cell("alongprojectname", 6), "along~");
        assert_eq!(state_cell(AgentState::Busy), "busy");
        assert_eq!(activity_state_name(NativeChildActivityState::Lost), "lost");
    }

    #[test]
    fn usage_table_renders_a_bounded_summary() {
        use eliot_core::api::{
            SessionCostTotal, SessionCumulativeTotals, SessionFacts, SessionTokenSubtotal,
            TurnCounterTotals, TurnExpenditure, TurnScopeTotals, UsageScopeView,
        };

        let subtotal = |total: Option<i64>, supplied: usize, missing: usize| SessionTokenSubtotal {
            total,
            supplied,
            missing,
            complete: missing == 0 && supplied > 0,
        };
        let response = UsageSummaryResponse {
            scope: UsageScopeView {
                agent: Some("a1".to_owned()),
                generation: None,
                project: None,
                root: None,
            },
            agents_included: 1,
            generations_included: 1,
            truncated: false,
            history_complete: true,
            history_reason: "no event history pruned".to_owned(),
            pruned_through: 0,
            turns: TurnExpenditure {
                turns_counted: 2,
                turns_without_usage: 1,
                turns_unknown_excluded: 1,
                turns_running_excluded: 1,
                turns_missing: TurnCounterMissing {
                    input_tokens: 1,
                    output_tokens: 2,
                    reasoning_tokens: 2,
                    thinking_tokens: 2,
                    cache_read_tokens: 2,
                    cache_write_tokens: 2,
                    total_tokens: 2,
                },
                by_scope: vec![TurnScopeTotals {
                    scope: "turn".to_owned(),
                    turns_counted: 1,
                    totals: TurnCounterTotals {
                        input_tokens: Some(10),
                        ..Default::default()
                    },
                    turns_missing: TurnCounterMissing {
                        input_tokens: 0,
                        output_tokens: 1,
                        reasoning_tokens: 1,
                        thinking_tokens: 1,
                        cache_read_tokens: 1,
                        cache_write_tokens: 1,
                        total_tokens: 1,
                    },
                }],
            },
            sessions: SessionFacts {
                snapshots: 1,
                generations_without_snapshot: 0,
                cumulative_tokens: SessionCumulativeTotals {
                    input_tokens: subtotal(Some(100), 1, 0),
                    output_tokens: subtotal(None, 0, 1),
                    cache_read_tokens: subtotal(None, 0, 1),
                    cache_write_tokens: subtotal(None, 0, 1),
                },
                cost_by_unit: vec![SessionCostTotal {
                    unit: "usd_estimate".to_owned(),
                    total_amount: 0.5,
                    snapshots: 1,
                }],
                snapshots_without_cost: 0,
                context_occupancy_supplied: 1,
                context_capacity_supplied: 1,
            },
        };
        UsageSummaryRequest {
            agent: Some("a1".to_owned()),
            generation: None,
            project: None,
            root: None,
        }
        .validate_response(&response)
        .expect("table fixture binds its request");
        print_usage_table(&response);
    }
}
