//! Read-only agent views and their table rendering.
use super::args::{
    EditsArgs, EventsArgs, GenerationArgs, GitStatusArgs, HeavyArgs, HistoryArgs, ListFilterArgs,
    LsArgs, PageArgs, ProcessesArgs, ReadGenerationArgs, ResultArgs, ShowArgs, StatsArgs, TopArgs,
    UsageArgs, WatchArgs, WhyArgs,
};
use super::support::read_and_print;
use super::support::{Failure, PAGE_SIZE, print_value, validate_limit};
use eliot_core::api::{
    AgentEditsRequest, AgentEditsResponse, AgentGetRequest, AgentGetResponse, AgentGitRequest,
    AgentGitResponse, AgentHistoryRequest, AgentListRequest, AgentListResponse,
    AgentProcessesRequest, AgentProcessesResponse, AgentResultRequest, AgentResultResponse,
    AgentStatsRequest, AgentStatsResponse, DiagnoseRequest, DiagnoseResponse, EventsBoundsRequest,
    EventsBoundsResponse, EventsFilter, EventsReadRequest, EventsReadResponse,
    EventsSubscribeRequest, EventsSubscribeResponse, HarnessDoctorRequest, HarnessDoctorResponse,
    ObservationPage, ObserveHeavyRequest, ObserveHeavyResponse, TurnCounterMissing,
    UsageSummaryRequest, UsageSummaryResponse,
};
use eliot_core::api::{AgentSummary, activity_state_name};
use eliot_core::{AgentState, Error as CoreError};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::Duration;

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
    if json_output {
        read_and_print!(
            &data_dir,
            json_output,
            "agent.get",
            AgentGetRequest::new(agent),
            AgentGetResponse
        )
    } else {
        let params = AgentGetRequest::new(agent);
        params
            .validate()
            .map_err(|error| Failure::from_core(error, false))?;
        let response: AgentGetResponse =
            eliot_daemon::client_call_typed(&data_dir, "agent.get", &params)
                .await
                .map_err(|error| Failure::from_core(error, false))?;
        params
            .validate_response(&response)
            .map_err(|error| Failure::from_core(error, false))?;
        if let Some(activity) = response.agent.activity.as_ref() {
            if response.agent.state == AgentState::Stopped {
                println!(
                    "state: stopped (ended with parent; last activity {})",
                    activity_state_name(activity.state)
                );
            } else {
                println!(
                    "state: {} (observed) (lifecycle state=unknown)",
                    activity_state_name(activity.state)
                );
            }
        }
        let value = serde_json::to_value(response)
            .map_err(|_| Failure::new(4, "agent.get response is invalid"))?;
        print_value(&value, false);
        Ok(0)
    }
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
    if json_output {
        read_and_print!(
            &data_dir,
            json_output,
            "agent.stats",
            params,
            AgentStatsResponse
        )
    } else {
        params
            .validate()
            .map_err(|error| Failure::from_core(error, false))?;
        let response: AgentStatsResponse =
            eliot_daemon::client_call_typed(&data_dir, "agent.stats", &params)
                .await
                .map_err(|error| Failure::from_core(error, false))?;
        params
            .validate_response(&response)
            .map_err(|error| Failure::from_core(error, false))?;
        print_stats_table(&response);
        Ok(0)
    }
}

/// Observe readback for `swarm stats`: the heavy-work rollup and the live
/// session-growth state. `--json` keeps the raw response; this table never
/// replaces it.
pub(crate) fn print_stats_table(response: &AgentStatsResponse) {
    println!(
        "stats {}#{} heavy {} growth {}",
        response.agent,
        response.generation,
        heavy_cell(response.heavy.as_ref()),
        growth_cell(response.session_growth.as_ref()),
    );
    if let Some(heavy) = &response.heavy {
        println!(
            "  heavy episodes {} latest {} cpu_ms {}",
            heavy.episodes,
            heavy.latest_category.as_deref().unwrap_or("-"),
            heavy.total_cpu_ms,
        );
    }
    if let Some(growth) = &response.session_growth {
        println!(
            "  growth basis {} message {}",
            growth.basis.as_deref().unwrap_or("-"),
            growth.message.as_deref().unwrap_or("-"),
        );
    }
}

/// Compact heavy cell for agent tables: - when no heavy episode was ever
/// recorded, otherwise `<episodes>:<category>` (for example `3:cargo-build`).
pub(crate) fn heavy_cell(heavy: Option<&eliot_core::api::HeavySummaryView>) -> String {
    match heavy {
        None => "-".to_owned(),
        Some(heavy) => format!(
            "{}{}",
            heavy.episodes,
            heavy
                .latest_category
                .as_deref()
                .map(|category| format!(":{category}"))
                .unwrap_or_default(),
        ),
    }
}

/// Compact growth cell for agent tables: - while the retained session
/// signal sits below every threshold, otherwise `!<basis>` (for example
/// `!tokens`).
pub(crate) fn growth_cell(growth: Option<&eliot_core::api::SessionGrowthView>) -> String {
    match growth {
        None => "-".to_owned(),
        Some(growth) => format!("!{}", growth.basis.as_deref().unwrap_or("growth")),
    }
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

pub(crate) async fn run_doctor(data_dir: PathBuf, json_output: bool) -> Result<u8, Failure> {
    let params = HarnessDoctorRequest {};
    if json_output {
        read_and_print!(
            &data_dir,
            json_output,
            "harness.doctor",
            params,
            HarnessDoctorResponse
        )
    } else {
        params
            .validate()
            .map_err(|error| Failure::from_core(error, false))?;
        let response: HarnessDoctorResponse =
            eliot_daemon::client_call_typed(&data_dir, "harness.doctor", &params)
                .await
                .map_err(|error| Failure::from_core(error, false))?;
        params
            .validate_response(&response)
            .map_err(|error| Failure::from_core(error, false))?;
        print_doctor_table(&response);
        Ok(0)
    }
}

pub(crate) fn print_doctor_table(response: &HarnessDoctorResponse) {
    println!(
        "{:<12} {:<9} {:<14} {:<16} {:<5} DOC",
        "HARNESS", "INSTALLED", "VERSION", "VERIFIED", "DRIFT"
    );
    for entry in &response.harnesses {
        println!(
            "{:<12} {:<9} {:<14} {:<16} {:<5} {}",
            entry.harness,
            yes_no(entry.installed),
            entry.version.as_deref().unwrap_or("-"),
            entry.verified_version,
            yes_no(entry.drift),
            entry.doc_section,
        );
        if let Some(message) = &entry.drift_message {
            println!("  drift: {message}");
        }
        if let Some(mismatch) = &entry.contract_mismatch {
            println!("  contract mismatch: {mismatch}");
        }
    }
}

pub(crate) async fn run_heavy(
    HeavyArgs {
        agent,
        generation: ReadGenerationArgs { generation },
        window_secs,
    }: HeavyArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let params = ObserveHeavyRequest {
        agent,
        generation,
        window_secs,
    };
    if json_output {
        read_and_print!(
            &data_dir,
            json_output,
            "observe.heavy",
            params,
            ObserveHeavyResponse
        )
    } else {
        params
            .validate()
            .map_err(|error| Failure::from_core(error, false))?;
        let response: ObserveHeavyResponse =
            eliot_daemon::client_call_typed(&data_dir, "observe.heavy", &params)
                .await
                .map_err(|error| Failure::from_core(error, false))?;
        params
            .validate_response(&response)
            .map_err(|error| Failure::from_core(error, false))?;
        print_heavy_table(&response);
        Ok(0)
    }
}

pub(crate) fn print_heavy_table(response: &ObserveHeavyResponse) {
    println!(
        "Heavy work over the last {} s (since {}):",
        response.window_secs, response.since
    );
    println!(
        "{:<8} {:<8} {:<10} {:<10} {:<12} LATEST",
        "AGENT", "EPISODES", "WALL_S", "CPU_S", "PEAK_RSS_B"
    );
    for totals in &response.agents {
        println!(
            "{:<8} {:<8} {:<10} {:<10} {:<12} {}",
            totals.agent,
            totals.episodes.len(),
            totals.total_duration_ms / 1000,
            totals.total_cpu_ms / 1000,
            totals.peak_rss_bytes,
            totals
                .episodes
                .first()
                .map(|episode| episode.category.as_str())
                .unwrap_or("-"),
        );
    }
    if response.truncated {
        println!("truncated: the window holds more episodes than shown");
    }
    if response.agents.is_empty() {
        println!("no heavy episodes in this window");
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

/// Consecutive transient transport failures tolerated before one watch
/// stream gives up with a typed error naming its resume cursor.
pub(crate) const WATCH_TRANSIENT_RETRIES: u32 = 30;
/// First retry delay; doubles per consecutive failure up to WATCH_MAX_BACKOFF.
const WATCH_RETRY_BASE: Duration = Duration::from_millis(200);
const WATCH_MAX_BACKOFF: Duration = Duration::from_secs(5);

/// How a watch polling step ends without a usable page.
enum WatchAttempt<T> {
    /// A fresh page (or the initial cursor) is ready.
    Ready(T),
    /// Ctrl-C arrived; exit 0.
    Cancelled,
    /// A typed domain error; the caller decides how to report it.
    Fatal(CoreError),
    /// The transient budget ran out; already converted to a typed failure
    /// naming the resume cursor when one is known.
    Drained(Failure),
}

/// Transient transport failures a watch rides out by re-issuing
/// `events.subscribe` from its last cursor: the client-side absolute
/// deadline, a dropped connection, a failed frame write, an undecodable
/// frame, or the daemon closing the live event bus while draining. Domain
/// rejections (including `history_gap`) are never transient.
pub(crate) fn is_transient_watch_error(error: &CoreError) -> bool {
    match error.code.as_str() {
        "unknown" => [
            "daemon RPC exceeded its absolute deadline",
            "daemon closed the RPC connection",
            "daemon response frame is invalid",
        ]
        .iter()
        .any(|marker| error.message.contains(marker)),
        "unavailable" => ["daemon RPC write failed", "event bus is closed"]
            .iter()
            .any(|marker| error.message.contains(marker)),
        _ => false,
    }
}

/// The daemon is unreachable (not running, refused endpoint, unreadable data
/// directory): fail fast, but keep the stream's resume cursor.
fn is_watch_unreachable(error: &CoreError) -> bool {
    matches!(
        error.code.as_str(),
        "unavailable" | "platform_io" | "data_dir_unavailable"
    ) && error.message.contains("hint: data_dir=")
}

/// Backoff before re-issuing a poll after `consecutive` transient failures.
pub(crate) fn watch_retry_delay(consecutive: u32) -> Duration {
    let shift = consecutive.min(5);
    WATCH_RETRY_BASE
        .checked_mul(1 << shift)
        .unwrap_or(WATCH_MAX_BACKOFF)
        .min(WATCH_MAX_BACKOFF)
}

/// Append the stream's resume cursor to a typed watch failure, so a dead
/// watch never strands its position behind a generic error.
fn with_watch_resume_cursor(mut failure: Failure, after: i64) -> Failure {
    failure.message = format!("{}; resume with --after {after}", failure.message);
    failure
}

/// Bounded retry around one watch RPC. Transient transport failures back off
/// and re-issue from the same cursor; only Ctrl-C, typed domain errors, or
/// an exhausted retry budget end the poll.
async fn watch_poll<T, Fut>(
    mut attempt: impl FnMut() -> Fut,
    resume_after: Option<i64>,
) -> WatchAttempt<T>
where
    Fut: std::future::Future<Output = Result<T, CoreError>>,
{
    let mut consecutive = 0_u32;
    loop {
        let poll = tokio::select! {
            result = attempt() => result,
            signal = tokio::signal::ctrl_c() => {
                return match signal {
                    Ok(()) => WatchAttempt::Cancelled,
                    Err(error) => WatchAttempt::Drained(Failure::new(
                        1,
                        format!("cannot listen for Ctrl+C: {error}"),
                    )),
                };
            }
        };
        match poll {
            Ok(value) => return WatchAttempt::Ready(value),
            Err(error) if is_transient_watch_error(&error) => {
                consecutive += 1;
                if consecutive > WATCH_TRANSIENT_RETRIES {
                    let failure = Failure::from_core(error, false);
                    return WatchAttempt::Drained(match resume_after {
                        Some(after) => with_watch_resume_cursor(failure, after),
                        None => failure,
                    });
                }
                let cancelled = tokio::select! {
                    () = tokio::time::sleep(watch_retry_delay(consecutive)) => false,
                    signal = tokio::signal::ctrl_c() => {
                        match signal {
                            Ok(()) => true,
                            Err(error) => {
                                return WatchAttempt::Drained(Failure::new(
                                    1,
                                    format!("cannot listen for Ctrl+C: {error}"),
                                ));
                            }
                        }
                    }
                };
                if cancelled {
                    return WatchAttempt::Cancelled;
                }
            }
            Err(error) => return WatchAttempt::Fatal(error),
        }
    }
}

/// Translate a pruned watch cursor into the retained resume cursor: the
/// journal's current prune floor replays exactly the retained history.
async fn watch_gap_failure(data_dir: &Path, after: i64, error: CoreError) -> Failure {
    let params = EventsBoundsRequest {
        filter: EventsFilter::default(),
    };
    let cursor = match eliot_daemon::client_call_typed::<_, EventsBoundsResponse>(
        data_dir,
        "events.bounds",
        &params,
    )
    .await
    {
        Ok(bounds) if bounds.validate().is_ok() && after < bounds.pruned_through => {
            Some(bounds.pruned_through)
        }
        _ => None,
    };
    let failure = Failure::from_core(error, false);
    match cursor {
        Some(cursor) => with_watch_resume_cursor(failure, cursor),
        None => with_watch_resume_cursor(failure, after),
    }
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
            let filter = filter.clone();
            match watch_poll(
                || async {
                    let params = EventsBoundsRequest {
                        filter: filter.clone(),
                    };
                    params.validate()?;
                    let bounds: EventsBoundsResponse =
                        eliot_daemon::client_call_typed(data_dir, "events.bounds", &params).await?;
                    bounds.validate()?;
                    Ok(bounds.hi)
                },
                None,
            )
            .await
            {
                WatchAttempt::Ready(hi) => hi,
                WatchAttempt::Cancelled => return Ok(0),
                WatchAttempt::Fatal(error) => {
                    return Err(Failure::from_core(error, false));
                }
                WatchAttempt::Drained(failure) => return Err(failure),
            }
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
        let data_dir = data_dir.to_path_buf();
        let response = match watch_poll(
            || {
                let params = EventsSubscribeRequest {
                    after,
                    limit: PAGE_SIZE,
                    wait_ms: 1_000,
                    filter: filter.clone(),
                };
                let data_dir = data_dir.clone();
                async move {
                    eliot_daemon::client_call_typed::<_, EventsSubscribeResponse>(
                        &data_dir,
                        "events.subscribe",
                        &params,
                    )
                    .await
                }
            },
            Some(after),
        )
        .await
        {
            WatchAttempt::Ready(response) => response,
            WatchAttempt::Cancelled => return Ok(0),
            WatchAttempt::Fatal(error) if error.code == "history_gap" => {
                return Err(watch_gap_failure(&data_dir, after, error).await);
            }
            WatchAttempt::Fatal(error) if is_watch_unreachable(&error) => {
                return Err(with_watch_resume_cursor(
                    Failure::from_core(error, false),
                    after,
                ));
            }
            WatchAttempt::Fatal(error) => return Err(Failure::from_core(error, false)),
            WatchAttempt::Drained(failure) => return Err(failure),
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
        archived: serde_json::from_value(Value::String(filter.archived))
            .map_err(|_| Failure::new(2, "invalid archived filter"))?,
        after,
        limit,
        observations,
        state,
        states: None,
        project: filter.project,
        project_scope: None,
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
/// `ACT_TURN` is the live active turn; `LAST_OUT`/`USAGE` belong to the
/// fleet usage cell (`last_turn_id` turn usage, or the session-snapshot
/// fallback), not the active turn. The usage cell shows the native total
/// when supplied, otherwise the input+output sum, with a scope suffix:
/// `(t)` turn-backed, `(s)` session fallback (fleet signal, never turn
/// expenditure; full session facts stay on `swarm usage`/`swarm show`).
/// `DATA` reports observation data availability only, never freshness or
/// liveness. `--json` keeps printing the raw response; this table never
/// replaces it.
/// Compact observe cell for `swarm top`: `-` when no heavy episode was ever
/// recorded and growth sits below every threshold, otherwise the heavy and
/// growth cells (for example `3:cargo-build -` or `- !tokens`).
pub(crate) fn observe_cell(summary: &AgentSummary) -> String {
    let heavy = heavy_cell(summary.heavy.as_ref());
    let growth = growth_cell(summary.session_growth.as_ref());
    if heavy == "-" && growth == "-" {
        "-".to_owned()
    } else {
        format!("{heavy} {growth}")
    }
}

pub(crate) fn print_agent_table(response: &AgentListResponse, with_observations: bool) {
    if with_observations {
        println!(
            "{:<6} {:<4} {:<22} {:<11} {:<12} {:<6} {:<11} {:<11} {:<12} {:<14}",
            "ID", "GEN", "STATE", "ACTIVITY", "ACT_TURN", "AGE", "LAST_OUT", "USAGE", "DATA", "OBS"
        );
    } else {
        println!(
            "{:<6} {:<4} {:<22} {:<14} {:<9} {:<12} {:<6} {:<11} {:<11} {:<6}",
            "ID",
            "GEN",
            "STATE",
            "PROJECT",
            "HARNESS",
            "ACT_TURN",
            "AGE",
            "LAST_OUT",
            "USAGE",
            "SEEN"
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
                "{:<6} {:<4} {:<22} {:<11} {:<12} {:<6} {:<11} {:<11} {:<12} {:<14}",
                truncate_cell(&summary.id, 6),
                summary.generation,
                truncate_cell(&summary.display_state(), 22),
                activity,
                truncate_cell(turn, 12),
                age,
                outcome,
                fleet_usage_cell(summary),
                observation,
                truncate_cell(&observe_cell(summary), 14),
            );
        } else {
            let seen = age_cell(
                now_ms
                    .checked_sub(summary.updated_at)
                    .filter(|age| *age >= 0),
            );
            println!(
                "{:<6} {:<4} {:<22} {:<14} {:<9} {:<12} {:<6} {:<11} {:<11} {:<6}",
                truncate_cell(&summary.id, 6),
                summary.generation,
                truncate_cell(&summary.display_state(), 22),
                truncate_cell(&summary.project, 14),
                truncate_cell(&summary.harness, 9),
                truncate_cell(turn, 12),
                age,
                outcome,
                fleet_usage_cell(summary),
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

/// Fleet usage cell for `swarm ls`/`swarm top`: the shared
/// [`AgentSummary::fleet_usage`] count with a scope suffix. `(t)` marks
/// turn-backed usage; `(s)` marks the session-snapshot fallback, which is
/// visibility only and never turn expenditure. No usage reads as `-`.
pub(crate) fn fleet_usage_cell(summary: &AgentSummary) -> String {
    match summary.fleet_usage() {
        None => "-".to_owned(),
        Some((tokens, scope)) => {
            format!("{tokens}{}", if scope == "session" { "(s)" } else { "(t)" })
        }
    }
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

pub(crate) async fn run_why(
    WhyArgs { target }: WhyArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let params = DiagnoseRequest { target };
    if json_output {
        read_and_print!(&data_dir, json_output, "diagnose", params, DiagnoseResponse)
    } else {
        params
            .validate()
            .map_err(|error| Failure::from_core(error, false))?;
        let response: DiagnoseResponse =
            eliot_daemon::client_call_typed(&data_dir, "diagnose", &params)
                .await
                .map_err(|error| Failure::from_core(error, false))?;
        params
            .validate_response(&response)
            .map_err(|error| Failure::from_core(error, false))?;
        print_why_diagnosis(&response);
        Ok(0)
    }
}

fn why_heading(response: &DiagnoseResponse) -> String {
    let Some(step) = eliot_core::api::diagnosis_reason(&response.steps) else {
        return format!("No failure details are available for {}", response.target);
    };
    let in_flight = step.kind == "command"
        && matches!(
            step.code.as_deref(),
            Some("accepted" | "queued" | "dispatched")
        );
    let healthy = step.kind == "agent"
        && step.message.is_none()
        && matches!(
            step.code.as_deref(),
            Some("created" | "starting" | "idle" | "busy" | "waiting" | "stopped" | "unknown")
        )
        && step
            .origin
            .as_deref()
            .is_none_or(|origin| origin == "unknown");
    let label = if healthy || in_flight {
        response.target.as_str()
    } else {
        eliot_core::origin_words(step.origin.as_deref().unwrap_or("unknown"))
    };
    format!("{label}: {}", step.user_message())
}

fn print_why_diagnosis(response: &DiagnoseResponse) {
    println!("{}", why_heading(response));
    for step in &response.steps {
        let mut line = format!("  [{}]", step.kind);
        line.push_str(&format!(" {}", step.user_message()));
        if let Some(where_) = step.failure_where.as_deref() {
            line.push_str(&format!(" at {where_}"));
        }
        if let Some(exit) = step.exit_code {
            line.push_str(&format!(" exit {exit}"));
        }
        if let Some(ntstatus) = step.ntstatus.as_deref() {
            line.push_str(&format!(" {ntstatus}"));
        }
        println!("{line}");
        if let Some(origin) = step.origin.as_deref() {
            println!("    {}", eliot_core::origin_words(origin));
        }
    }
    if let Some(preceded_by) = response.preceded_by.as_deref() {
        println!("  preceded by: {preceded_by}");
    }
    if let Some(crash) = response.previous_crash.as_ref() {
        println!("  previous crash: {}", crash.summary());
    }
}

pub(crate) fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use super::{
        activity_state_name, age_cell, fleet_usage_cell, is_transient_watch_error, list_params,
        print_usage_table, truncate_cell, watch_retry_delay, with_watch_resume_cursor,
    };
    use crate::cli::args::ListFilterArgs;
    use crate::cli::support::Failure;
    use eliot_core::api::{
        AgentSummary, TurnCounterMissing, UsageSummaryRequest, UsageSummaryResponse,
    };
    use eliot_core::{AgentState, Error as CoreError, NativeChildActivityState, TurnUsage};
    use std::time::Duration;

    #[test]
    fn healthy_why_is_a_state_and_failure_prose_is_not_reparsed() {
        let reply = |code, origin, message| {
            serde_json::from_value::<eliot_core::api::DiagnoseResponse>(serde_json::json!({
                "target": "a2", "summary": "not used: origin: provider",
                "steps": [{"kind": "agent", "code": code, "origin": origin, "message": message}]
            }))
            .unwrap()
        };
        assert_eq!(
            super::why_heading(&reply("idle", "unknown", None)),
            "a2: Ready for a message"
        );
        assert_eq!(
            super::why_heading(&reply("busy", "unknown", None)),
            "a2: Working"
        );
        assert_eq!(
            super::why_heading(&reply(
                "failure",
                "provider",
                Some("origin: request: refused")
            )),
            "Model service: The Agent could not finish: origin: request: refused"
        );
    }

    #[test]
    fn starting_why_with_an_in_flight_launch_has_no_failure_heading() {
        for (state, words) in [
            ("accepted", "Request accepted"),
            ("queued", "Waiting to run"),
            ("dispatched", "Sent to the Agent"),
        ] {
            let reply =
                serde_json::from_value::<eliot_core::api::DiagnoseResponse>(serde_json::json!({
                    "target": "a2", "summary": "unused",
                    "steps": [
                        {"kind": "agent", "code": "starting"},
                        {"kind": "command", "code": state}
                    ]
                }))
                .unwrap();
            assert_eq!(super::why_heading(&reply), format!("a2: {words}"));
        }
    }

    #[test]
    fn watch_rides_out_transport_timeouts_but_not_domain_rejections() {
        for message in [
            "daemon RPC exceeded its absolute deadline",
            "daemon closed the RPC connection",
            "daemon response frame is invalid",
        ] {
            assert!(
                is_transient_watch_error(&CoreError::new("unknown", message)),
                "{message}"
            );
        }
        for message in [
            "daemon RPC write failed; hint: data_dir=C:\\x",
            "event bus is closed; hint: data_dir=C:\\x",
        ] {
            assert!(
                is_transient_watch_error(&CoreError::new("unavailable", message)),
                "{message}"
            );
        }
        for (code, message) in [
            (
                "history_gap",
                "requested event history is no longer retained",
            ),
            ("invalid", "after must be a nonnegative integer"),
            ("unknown", "daemon response is malformed"),
            ("unknown", "daemon response does not match the request"),
            ("unavailable", "daemon is not running; hint: data_dir=C:\\x"),
            ("not_allowed", "agent filter excludes the caller"),
        ] {
            assert!(
                !is_transient_watch_error(&CoreError::new(code, message)),
                "{code}:{message}"
            );
        }
    }

    #[test]
    fn watch_backoff_stays_bounded() {
        assert_eq!(watch_retry_delay(0), Duration::from_millis(200));
        assert_eq!(watch_retry_delay(1), Duration::from_millis(400));
        assert_eq!(watch_retry_delay(1_000), Duration::from_secs(5));
        for consecutive in [5, 30, u32::MAX] {
            assert!(
                watch_retry_delay(consecutive) <= Duration::from_secs(5),
                "{consecutive}"
            );
        }
    }

    #[test]
    fn watch_failures_name_the_resume_cursor() {
        let failure = with_watch_resume_cursor(
            Failure::new(1, "unknown: daemon RPC exceeded its absolute deadline"),
            12_345,
        );
        assert_eq!(failure.exit_code, 1);
        assert!(
            failure.message.contains("--after 12345"),
            "{}",
            failure.message
        );
    }
    #[test]
    fn list_params_rejects_unknown_state_before_any_call() {
        let filter = |state: Option<&str>| ListFilterArgs {
            archived: "exclude".to_owned(),

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

    fn fleet_summary(usage: Option<TurnUsage>, usage_scope: Option<&str>) -> AgentSummary {
        AgentSummary {
            archived: false,
            chat: false,

            id: "a1".to_owned(),
            generation: 0,
            project: "p".to_owned(),
            repo_common_dir: None,
            workspace: None,
            harness: "muse".to_owned(),
            name: None,
            profile: None,
            preview: None,
            state: AgentState::Idle,
            parent: None,
            root: "a1".to_owned(),
            active_turn: None,
            last_progress_at: None,
            progress_age_ms: None,
            last_turn_id: None,
            last_outcome: None,
            last_resolution_source: None,
            activity: None,
            usage,
            usage_scope: usage_scope.map(str::to_owned),
            updated_at: 7,
            project_scope: None,
            heavy: None,
            session_growth: None,
        }
    }

    fn turn_usage(input: Option<i64>, output: Option<i64>, total: Option<i64>) -> TurnUsage {
        TurnUsage {
            scope: Some("turn".to_owned()),
            message_id: None,
            input_tokens: input,
            output_tokens: output,
            reasoning_tokens: None,
            thinking_tokens: None,
            cache_read_tokens: None,
            cache_write_tokens: None,
            total_tokens: total,
        }
    }

    #[test]
    fn fleet_usage_cell_suffixes_the_shared_provenance() {
        // The count derivation lives on `AgentSummary::fleet_usage` (tested
        // in eliot-core); the cell only adds the scope suffix.
        assert_eq!(fleet_usage_cell(&fleet_summary(None, None)), "-");
        assert_eq!(
            fleet_usage_cell(&fleet_summary(
                Some(turn_usage(Some(10), Some(5), Some(15))),
                Some("turn")
            )),
            "15(t)"
        );
        let mut session = turn_usage(Some(47_024), Some(207), None);
        session.scope = Some("session".to_owned());
        assert_eq!(
            fleet_usage_cell(&fleet_summary(Some(session), Some("session"))),
            "47231(s)"
        );
    }

    #[test]
    fn observed_rows_display_activity_with_marker() {
        use eliot_core::NativeChildActivity;
        let mut observed = fleet_summary(None, None);
        observed.state = AgentState::Unknown;
        observed.activity = Some(NativeChildActivity {
            state: NativeChildActivityState::Idle,
            has_open_turn: false,
            last_progress_at: None,
        });
        assert_eq!(observed.display_state(), "idle (observed)");
        assert_eq!(observed.fleet_bucket(), "idle");
        observed.activity.as_mut().unwrap().state = NativeChildActivityState::Running;
        observed.activity.as_mut().unwrap().has_open_turn = true;
        assert_eq!(observed.display_state(), "running (observed)");
        assert_eq!(observed.fleet_bucket(), "running");
        observed.activity.as_mut().unwrap().state = NativeChildActivityState::Lost;
        assert_eq!(observed.display_state(), "lost (observed)");
        assert_eq!(observed.fleet_bucket(), "unknown");
        let controlled = fleet_summary(None, None);
        assert_eq!(controlled.display_state(), "idle");
        assert_eq!(
            eliot_core::api::agent_state_name(AgentState::Unknown),
            "unknown"
        );
        assert_eq!(activity_state_name(NativeChildActivityState::Idle), "idle");
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
        assert_eq!(eliot_core::api::agent_state_name(AgentState::Busy), "busy");
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
