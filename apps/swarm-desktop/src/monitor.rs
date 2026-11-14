//! Swarm monitor: demand-driven rows, honest counters and UI-only metric
//! history, as pure logic plus one bounded refresh read.
//!
//! One refresh is a fixed set of existing reads: `agent.list` per attention
//! state with `observations=true` (rows), `agent.list` with `limit=1` for
//! the remaining states (their `total` is an exact count), and, for the
//! selected Agent only, `agent.get` and `usage.summary`. No per-Agent
//! timers, no `agent.stats` per row, at most `MAX_CONCURRENT_READS` reads at
//! once, and only while the monitor is open.
//!
//! `ProcessStatsView` carries cumulative `cpu_time_ns`, not a rate. A rate
//! is derived here only between two distinct samples of the same process
//! identities (boot id, pid, start time), as core-equivalent percent
//! (`100 × ΔCPU / Δelapsed`, may exceed 100). A changed process set marks the
//! point partial; an unusable interval stays unknown. Repeating a cached
//! sample adds no point. The history is UI-only and capped.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

use eliot_core::AgentState;
use eliot_core::api::{
    AgentGetResponse, AgentListRequest, AgentListResponse, AgentListRow, ObservationView,
    ProcessStatsView, UsageSummaryResponse,
};
use futures_util::StreamExt;

use crate::client::{self, MAX_CONCURRENT_READS};

/// Refresh period while the monitor is open.
pub const PERIOD_MS: u64 = 2_000;
/// Rows read per attention state.
pub const ROWS_PER_STATE: usize = 20;
/// UI-only samples kept per Agent series.
pub const RING_CAPACITY: usize = 600;
/// Bars drawn per graph; never more than useful at this width.
pub const GRAPH_BARS: usize = 60;

/// States shown as rows, attention first; idle sessions still hold processes.
pub const ROW_STATES: [AgentState; 7] = [
    AgentState::Waiting,
    AgentState::Failed,
    AgentState::Lost,
    AgentState::Unknown,
    AgentState::Busy,
    AgentState::Starting,
    AgentState::Idle,
];
/// States only counted.
pub const COUNT_STATES: [AgentState; 2] = [AgentState::Created, AgentState::Stopped];

pub fn state_name(state: AgentState) -> &'static str {
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

/// Lower ranks need attention first: pending requests, then unknown
/// outcomes and failures, then active work.
pub fn attention_rank(state: AgentState) -> u8 {
    match state {
        AgentState::Waiting => 0,
        AgentState::Failed => 1,
        AgentState::Lost => 2,
        AgentState::Unknown => 3,
        AgentState::Busy => 4,
        AgentState::Starting => 5,
        AgentState::Created => 6,
        AgentState::Idle => 7,
        AgentState::Stopped => 8,
    }
}

/// Observation summary of one row; unknown stays unknown.
#[derive(Clone, Debug, PartialEq)]
pub enum Obs {
    Unavailable(String),
    Data {
        rss_bytes: Option<u64>,
        cpu_time_ns: Option<u64>,
        sampled_at: Option<i64>,
        age_ms: Option<i64>,
        ttl_ms: i64,
        coverage: String,
        clock_regressed: bool,
        error: Option<String>,
        processes: usize,
        processes_truncated: bool,
    },
}

impl Obs {
    fn from_view(view: Option<&ObservationView>) -> Self {
        match view {
            None => Self::Unavailable("missing from the reply".to_owned()),
            Some(ObservationView::Unavailable { error_code, .. }) => {
                Self::Unavailable(error_code.clone())
            }
            Some(ObservationView::Data(data)) => {
                let stats = &data.stats;
                Self::Data {
                    rss_bytes: stats.rss_bytes,
                    cpu_time_ns: stats.cpu_time_ns,
                    sampled_at: stats.sampled_at,
                    age_ms: stats.age_ms,
                    ttl_ms: stats.ttl_ms,
                    coverage: format!("{:?}", stats.coverage).to_lowercase(),
                    clock_regressed: stats.clock_regressed,
                    error: stats.error.clone(),
                    processes: data.processes.len(),
                    processes_truncated: data.processes_truncated,
                }
            }
        }
    }

    /// Freshness as the daemon reported it: sample age against its TTL,
    /// coverage, clock regression; never inferred from the refresh time.
    pub fn freshness(&self) -> String {
        match self {
            Self::Unavailable(code) => format!("observation unavailable ({code})"),
            Self::Data {
                age_ms,
                ttl_ms,
                coverage,
                clock_regressed,
                error,
                sampled_at,
                ..
            } => {
                if sampled_at.is_none() {
                    return format!("no process sample yet ({coverage})");
                }
                let mut text = match age_ms {
                    Some(age) if *age > *ttl_ms => {
                        format!(
                            "stale: sample {} old, TTL {}",
                            seconds(*age),
                            seconds(*ttl_ms)
                        )
                    }
                    Some(age) => format!("sample {} old · {coverage}", seconds(*age)),
                    None => format!("sample age unknown · {coverage}"),
                };
                if *clock_regressed {
                    text.push_str(" · clock regressed");
                }
                if let Some(error) = error {
                    text.push_str(&format!(" · {error}"));
                }
                text
            }
        }
    }

    pub fn rss_bytes(&self) -> Option<u64> {
        match self {
            Self::Data { rss_bytes, .. } => *rss_bytes,
            Self::Unavailable(_) => None,
        }
    }

    pub fn cpu_time_ns(&self) -> Option<u64> {
        match self {
            Self::Data { cpu_time_ns, .. } => *cpu_time_ns,
            Self::Unavailable(_) => None,
        }
    }
}

fn seconds(ms: i64) -> String {
    format!("{:.1} s", ms as f64 / 1000.0)
}

pub fn bytes_label(bytes: Option<u64>) -> String {
    match bytes {
        None => "—".to_owned(),
        Some(bytes) if bytes >= 1 << 30 => format!("{:.2} GiB", bytes as f64 / (1u64 << 30) as f64),
        Some(bytes) => format!("{:.0} MiB", bytes as f64 / (1u64 << 20) as f64),
    }
}

pub fn cpu_time_label(ns: Option<u64>) -> String {
    match ns {
        None => "—".to_owned(),
        Some(ns) => format!("{:.1} s CPU", ns as f64 / 1e9),
    }
}

/// One monitor row: identity, state and the observation summary.
#[derive(Clone, Debug, PartialEq)]
pub struct MonitorRow {
    pub id: String,
    pub generation: i64,
    pub state: AgentState,
    pub harness: String,
    pub active_turn: Option<String>,
    pub activity: Option<String>,
    pub progress_age_ms: Option<i64>,
    pub last_outcome: Option<String>,
    pub tokens: Option<i64>,
    pub obs: Obs,
}

impl MonitorRow {
    fn from_list(row: &AgentListRow) -> Self {
        let summary = &row.summary;
        Self {
            id: summary.id.clone(),
            generation: summary.generation,
            state: summary.state,
            harness: summary.harness.clone(),
            active_turn: summary.active_turn.clone(),
            activity: summary
                .activity
                .as_ref()
                .map(|activity| format!("{:?}", activity.state).to_lowercase()),
            progress_age_ms: summary.progress_age_ms,
            last_outcome: summary
                .last_outcome
                .map(|outcome| outcome.as_str().to_owned()),
            tokens: summary.usage.as_ref().and_then(|usage| usage.total_tokens),
            obs: Obs::from_view(row.observation.as_ref()),
        }
    }

    /// Compact operation label: current turn or last outcome.
    pub fn operation(&self) -> String {
        match (&self.active_turn, &self.last_outcome) {
            (Some(_), _) => match self.progress_age_ms {
                Some(age) => format!("turn active · progress {} ago", seconds(age)),
                None => "turn active · no model progress yet".to_owned(),
            },
            (None, Some(outcome)) => format!("last turn {outcome}"),
            (None, None) => "no turn".to_owned(),
        }
    }
}

/// Exact count for one state (`agent.list` `total`), and how many rows of
/// it are shown.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StateCount {
    pub state: AgentState,
    pub total: Option<usize>,
    pub shown: usize,
}

/// One graph point. `cpu_percent` is core-equivalent and `None` when the
/// interval cannot be measured.
#[derive(Clone, Debug, PartialEq)]
pub struct Point {
    pub sampled_at: i64,
    pub cpu_percent: Option<f64>,
    pub rss_bytes: Option<u64>,
    pub partial: bool,
}

#[derive(Clone, Debug, PartialEq)]
struct Sample {
    sampled_at: i64,
    per_process: HashMap<String, u64>,
    identities: HashSet<String>,
    total_cpu: Option<u64>,
}

/// UI-only metric history for one Agent.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Series {
    last: Option<Sample>,
    points: VecDeque<Point>,
}

fn identity_key(record: &eliot_core::ProcessRecord) -> String {
    format!(
        "{}:{}:{}",
        record.identity.host_boot_id, record.identity.pid, record.identity.start_time
    )
}

impl Series {
    /// Add one observation. Returns true only when a new distinct sample
    /// produced a point.
    pub fn observe(
        &mut self,
        stats: &ProcessStatsView,
        processes: &[eliot_core::ProcessRecord],
    ) -> bool {
        let Some(sampled_at) = stats.sampled_at else {
            return false;
        };
        if stats.clock_regressed {
            // Intervals across a clock regression are not measurable.
            self.last = None;
            return false;
        }
        match self.last.as_ref().map(|last| last.sampled_at) {
            // The same cached sample again: no new point.
            Some(last) if sampled_at == last => return false,
            // Time went backwards without a flag: the interval is unusable,
            // so the series restarts with an unknown point, not a freeze.
            Some(last) if sampled_at < last => self.last = None,
            _ => {}
        }
        let mut per_process = HashMap::new();
        if let Some(samples) = stats.samples.as_ref() {
            for sample in samples {
                if let Some(cpu) = sample.cpu_time_ns {
                    per_process.insert(identity_key(&sample.record), cpu);
                }
            }
        }
        let identities: HashSet<String> = match stats.samples.as_ref() {
            Some(samples) => samples
                .iter()
                .map(|sample| identity_key(&sample.record))
                .collect(),
            None => processes.iter().map(identity_key).collect(),
        };
        let sample = Sample {
            sampled_at,
            per_process,
            identities,
            total_cpu: stats.cpu_time_ns,
        };
        let incomplete = stats.complete != Some(true);
        let (cpu_percent, partial) = match self.last.as_ref() {
            Some(previous) => {
                let (percent, changed) = rate(previous, &sample);
                (percent, changed || incomplete)
            }
            None => (None, incomplete),
        };
        self.points.push_back(Point {
            sampled_at,
            cpu_percent,
            rss_bytes: stats.rss_bytes,
            partial,
        });
        while self.points.len() > RING_CAPACITY {
            self.points.pop_front();
        }
        self.last = Some(sample);
        true
    }

    pub fn points(&self) -> &VecDeque<Point> {
        &self.points
    }

    /// The newest `GRAPH_BARS` points for drawing.
    pub fn recent(&self) -> Vec<&Point> {
        let skip = self.points.len().saturating_sub(GRAPH_BARS);
        self.points.iter().skip(skip).collect()
    }
}

/// Core-equivalent CPU percent between two samples of matching processes.
fn rate(previous: &Sample, current: &Sample) -> (Option<f64>, bool) {
    let elapsed_ms = current.sampled_at - previous.sampled_at;
    if elapsed_ms <= 0 {
        return (None, false);
    }
    // A changed process set, or a process whose counter went missing, makes
    // the point partial: only matching identities are differenced.
    let lost_counter = previous.per_process.keys().any(|identity| {
        current.identities.contains(identity) && !current.per_process.contains_key(identity)
    });
    let changed = previous.identities != current.identities || lost_counter;
    let delta_ns: Option<u64> =
        if !current.per_process.is_empty() && !previous.per_process.is_empty() {
            let mut sum = 0_u64;
            let mut common = 0;
            for (identity, cpu) in &current.per_process {
                if let Some(before) = previous.per_process.get(identity)
                    && cpu >= before
                {
                    sum += cpu - before;
                    common += 1;
                }
            }
            (common > 0).then_some(sum)
        } else if !changed && !current.identities.is_empty() {
            // Aggregate counters only across an identical process set.
            match (previous.total_cpu, current.total_cpu) {
                (Some(before), Some(now)) if now >= before => Some(now - before),
                _ => None,
            }
        } else {
            None
        };
    let percent = delta_ns.map(|delta| 100.0 * delta as f64 / (elapsed_ms as f64 * 1e6));
    (percent, changed)
}

/// Selected-Agent detail read.
#[derive(Clone, Debug)]
pub struct DetailRead {
    pub agent: AgentGetResponse,
    pub usage: Result<UsageSummaryResponse, String>,
}

/// Everything one refresh read.
#[derive(Clone, Debug)]
pub struct MonitorSnapshot {
    pub groups: Vec<(AgentState, Result<AgentListResponse, String>)>,
    pub counts: Vec<(AgentState, Result<usize, String>)>,
    pub detail: Option<Result<DetailRead, String>>,
    pub read_at_ms: i64,
}

/// Monitor model: rows (attention first), counters and per-Agent series.
#[derive(Clone, Debug, Default)]
pub struct Monitor {
    pub rows: Vec<MonitorRow>,
    pub counts: Vec<StateCount>,
    pub series: HashMap<String, Series>,
    pub detail: Option<Result<DetailRead, String>>,
    pub read_at_ms: Option<i64>,
    pub errors: Vec<String>,
    pub refreshes: u64,
}

impl Monitor {
    pub fn apply(&mut self, snapshot: MonitorSnapshot) {
        self.refreshes += 1;
        self.errors.clear();
        let mut rows = Vec::new();
        let mut counts = Vec::new();
        for (state, group) in &snapshot.groups {
            match group {
                Ok(page) => {
                    for row in &page.agents {
                        let monitor_row = MonitorRow::from_list(row);
                        if let Some(ObservationView::Data(data)) = row.observation.as_ref() {
                            self.series
                                .entry(monitor_row.id.clone())
                                .or_default()
                                .observe(&data.stats, &data.processes);
                        }
                        rows.push(monitor_row);
                    }
                    counts.push(StateCount {
                        state: *state,
                        total: Some(page.total),
                        shown: page.agents.len(),
                    });
                }
                Err(error) => {
                    self.errors.push(format!("{}: {error}", state_name(*state)));
                    counts.push(StateCount {
                        state: *state,
                        total: None,
                        shown: 0,
                    });
                }
            }
        }
        for (state, count) in &snapshot.counts {
            match count {
                Ok(total) => counts.push(StateCount {
                    state: *state,
                    total: Some(*total),
                    shown: 0,
                }),
                Err(error) => {
                    self.errors.push(format!("{}: {error}", state_name(*state)));
                    counts.push(StateCount {
                        state: *state,
                        total: None,
                        shown: 0,
                    });
                }
            }
        }
        // State reads are not one atomic read: an Agent that changed state
        // between them appears once, under its most urgent state.
        rows.sort_by(|a, b| {
            a.id.cmp(&b.id)
                .then_with(|| attention_rank(a.state).cmp(&attention_rank(b.state)))
        });
        rows.dedup_by(|later, kept| later.id == kept.id);
        rows.sort_by(|a, b| {
            attention_rank(a.state)
                .cmp(&attention_rank(b.state))
                .then_with(|| a.id.cmp(&b.id))
        });
        // Series follow the rows; history is pruned only after a complete
        // refresh, so a failed state read does not erase it.
        if self.errors.is_empty() {
            let present: HashSet<&str> = rows.iter().map(|row| row.id.as_str()).collect();
            self.series.retain(|id, _| present.contains(id.as_str()));
        }
        self.rows = rows;
        self.counts = counts;
        if snapshot.detail.is_some() {
            self.detail = snapshot.detail;
        }
        self.read_at_ms = Some(snapshot.read_at_ms);
    }

    /// Fleet counter line: exact totals per state; "?" when a read failed.
    pub fn counter_line(&self) -> String {
        let mut counts = self.counts.clone();
        counts.sort_by_key(|count| attention_rank(count.state));
        counts
            .iter()
            .map(|count| match count.total {
                Some(total) if count.shown < total && count.shown > 0 => {
                    format!(
                        "{} {total} ({} shown)",
                        state_name(count.state),
                        count.shown
                    )
                }
                Some(total) => format!("{} {total}", state_name(count.state)),
                None => format!("{} ?", state_name(count.state)),
            })
            .collect::<Vec<_>>()
            .join(" · ")
    }
}

type ReadFuture<'a> = Pin<Box<dyn Future<Output = Read> + 'a>>;

enum Read {
    Group(AgentState, Result<AgentListResponse, String>),
    Count(AgentState, Result<usize, String>),
    Detail(Box<Result<DetailRead, String>>),
    Cancelled,
}

fn list_request(state: AgentState, limit: usize, observations: bool) -> AgentListRequest {
    AgentListRequest {
        limit,
        observations,
        state: Some(state),
        ..AgentListRequest::default()
    }
}

/// One refresh: the fixed read set, at most `MAX_CONCURRENT_READS` at once.
pub async fn refresh(
    data_dir: &Path,
    selected: Option<String>,
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Option<MonitorSnapshot> {
    let mut reads: Vec<ReadFuture<'_>> = Vec::new();
    for state in ROW_STATES {
        reads.push(Box::pin(async move {
            if cancelled() {
                return Read::Cancelled;
            }
            let request = list_request(state, ROWS_PER_STATE, true);
            Read::Group(
                state,
                client::list_agents(data_dir, &request)
                    .await
                    .map_err(|error| error.to_string()),
            )
        }));
    }
    for state in COUNT_STATES {
        reads.push(Box::pin(async move {
            if cancelled() {
                return Read::Cancelled;
            }
            let request = list_request(state, 1, false);
            Read::Count(
                state,
                client::list_agents(data_dir, &request)
                    .await
                    .map(|page| page.total)
                    .map_err(|error| error.to_string()),
            )
        }));
    }
    if let Some(agent) = selected.clone() {
        reads.push(Box::pin(async move {
            if cancelled() {
                return Read::Cancelled;
            }
            let detail = async {
                let read = client::get_agent(data_dir, &agent)
                    .await
                    .map_err(|error| error.to_string())?;
                let usage = client::usage_summary(data_dir, &agent)
                    .await
                    .map_err(|error| error.to_string());
                Ok(DetailRead { agent: read, usage })
            };
            Read::Detail(Box::new(detail.await))
        }));
    }
    let results: Vec<Read> = futures_util::stream::iter(reads)
        .buffer_unordered(MAX_CONCURRENT_READS)
        .collect()
        .await;
    let mut snapshot = MonitorSnapshot {
        groups: Vec::new(),
        counts: Vec::new(),
        detail: None,
        read_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as i64)
            .unwrap_or(0),
    };
    if cancelled() || results.iter().any(|read| matches!(read, Read::Cancelled)) {
        return None;
    }
    for result in results {
        match result {
            Read::Group(state, page) => snapshot.groups.push((state, page)),
            Read::Count(state, total) => snapshot.counts.push((state, total)),
            Read::Detail(detail) => snapshot.detail = Some(*detail),
            Read::Cancelled => {}
        }
    }
    snapshot
        .groups
        .sort_by_key(|(state, _)| attention_rank(*state));
    Some(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::StatsCoverage;
    use eliot_core::{ProcessIdentity, ProcessRecord, ProcessSample, ProcessState};

    fn record(pid: u32) -> ProcessRecord {
        ProcessRecord {
            identity: ProcessIdentity {
                host_boot_id: "boot".to_owned(),
                pid,
                start_time: 100 + u64::from(pid),
            },
            owner: "agent:a1#1".to_owned(),
            role: "harness".to_owned(),
            state: ProcessState::Running,
            exit_code: None,
            updated_at: 0,
        }
    }

    fn stats(sampled_at: i64, samples: &[(u32, u64)]) -> ProcessStatsView {
        ProcessStatsView {
            available: true,
            sampled_at: Some(sampled_at),
            age_ms: Some(100),
            ttl_ms: 5_000,
            clock_regressed: false,
            coverage: StatsCoverage::Complete,
            complete: Some(true),
            cpu_time_ns: Some(samples.iter().map(|(_, cpu)| cpu).sum()),
            rss_bytes: Some(1 << 20),
            samples: Some(
                samples
                    .iter()
                    .map(|(pid, cpu)| ProcessSample {
                        record: record(*pid),
                        cpu_time_ns: Some(*cpu),
                        rss_bytes: Some(1 << 20),
                    })
                    .collect(),
            ),
            error: None,
        }
    }

    #[test]
    fn rate_is_core_equivalent_between_matching_identities() {
        let mut series = Series::default();
        assert!(series.observe(&stats(1_000, &[(1, 0)]), &[]));
        assert!(series.observe(&stats(2_000, &[(1, 1_500_000_000)]), &[]));
        let point = series.points().back().unwrap();
        assert_eq!(point.cpu_percent, Some(150.0));
        assert!(!point.partial);
    }

    #[test]
    fn repeated_cached_sample_adds_no_point() {
        let mut series = Series::default();
        series.observe(&stats(1_000, &[(1, 0)]), &[]);
        assert!(!series.observe(&stats(1_000, &[(1, 0)]), &[]));
        assert_eq!(series.points().len(), 1);
    }

    #[test]
    fn changed_process_set_is_partial_and_ignores_new_processes() {
        let mut series = Series::default();
        series.observe(&stats(1_000, &[(1, 0)]), &[]);
        series.observe(&stats(2_000, &[(1, 500_000_000), (2, 9_000_000_000)]), &[]);
        let point = series.points().back().unwrap();
        assert_eq!(point.cpu_percent, Some(50.0));
        assert!(point.partial);
    }

    #[test]
    fn reused_pid_with_new_start_time_is_not_differenced() {
        let mut series = Series::default();
        series.observe(&stats(1_000, &[(1, 5_000_000_000)]), &[]);
        let mut next = stats(2_000, &[(1, 1_000)]);
        if let Some(samples) = next.samples.as_mut() {
            samples[0].record.identity.start_time = 999;
        }
        series.observe(&next, &[]);
        assert_eq!(series.points().back().unwrap().cpu_percent, None);
    }

    #[test]
    fn clock_regression_breaks_the_interval() {
        let mut series = Series::default();
        series.observe(&stats(1_000, &[(1, 0)]), &[]);
        let mut regressed = stats(2_000, &[(1, 10)]);
        regressed.clock_regressed = true;
        assert!(!series.observe(&regressed, &[]));
        series.observe(&stats(3_000, &[(1, 20)]), &[]);
        assert_eq!(series.points().back().unwrap().cpu_percent, None);
    }

    #[test]
    fn backwards_time_without_flag_restarts_instead_of_freezing() {
        let mut series = Series::default();
        series.observe(&stats(5_000, &[(1, 0)]), &[]);
        assert!(series.observe(&stats(4_000, &[(1, 10)]), &[]));
        assert_eq!(series.points().back().unwrap().cpu_percent, None);
        assert!(series.observe(&stats(5_000, &[(1, 1_000_000_000)]), &[]));
        assert!(series.points().back().unwrap().cpu_percent.is_some());
    }

    #[test]
    fn incomplete_snapshot_marks_the_point_partial() {
        let mut series = Series::default();
        series.observe(&stats(1_000, &[(1, 0)]), &[]);
        let mut incomplete = stats(2_000, &[(1, 100)]);
        incomplete.complete = Some(false);
        series.observe(&incomplete, &[]);
        assert!(series.points().back().unwrap().partial);
    }

    #[test]
    fn an_agent_seen_in_two_state_reads_is_one_row() {
        use eliot_core::api::{AgentListResponse, AgentSummary};
        let row = |state: AgentState| AgentListRow {
            summary: AgentSummary {
                id: "a7".to_owned(),
                generation: 1,
                project: "p".to_owned(),
                harness: "codex".to_owned(),
                state,
                parent: None,
                root: "a7".to_owned(),
                active_turn: None,
                last_progress_at: None,
                progress_age_ms: None,
                last_turn_id: None,
                last_outcome: None,
                activity: None,
                usage: None,
                updated_at: 0,
            },
            observation: None,
        };
        let page = |state: AgentState| AgentListResponse {
            agents: vec![row(state)],
            next: Some("a7".to_owned()),
            total: 1,
            truncated: false,
        };
        let mut monitor = Monitor::default();
        monitor.apply(MonitorSnapshot {
            groups: vec![
                (AgentState::Waiting, Ok(page(AgentState::Waiting))),
                (AgentState::Busy, Ok(page(AgentState::Busy))),
            ],
            counts: Vec::new(),
            detail: None,
            read_at_ms: 0,
        });
        assert_eq!(monitor.rows.len(), 1);
        assert_eq!(monitor.rows[0].state, AgentState::Waiting);
    }

    #[test]
    fn ring_is_capped() {
        let mut series = Series::default();
        for index in 0..(RING_CAPACITY as i64 + 10) {
            series.observe(&stats(1_000 + index, &[(1, index as u64)]), &[]);
        }
        assert_eq!(series.points().len(), RING_CAPACITY);
        assert_eq!(series.recent().len(), GRAPH_BARS);
    }

    #[test]
    fn attention_orders_waiting_before_busy_before_idle() {
        let mut states = vec![
            AgentState::Idle,
            AgentState::Busy,
            AgentState::Waiting,
            AgentState::Failed,
        ];
        states.sort_by_key(|state| attention_rank(*state));
        assert_eq!(
            states,
            vec![
                AgentState::Waiting,
                AgentState::Failed,
                AgentState::Busy,
                AgentState::Idle
            ]
        );
    }

    #[test]
    fn freshness_reports_stale_samples_against_ttl() {
        let obs = Obs::Data {
            rss_bytes: None,
            cpu_time_ns: None,
            sampled_at: Some(1),
            age_ms: Some(9_000),
            ttl_ms: 5_000,
            coverage: "complete".to_owned(),
            clock_regressed: false,
            error: None,
            processes: 1,
            processes_truncated: false,
        };
        assert!(obs.freshness().starts_with("stale"));
    }

    #[test]
    fn counter_line_marks_partial_rows_and_failed_reads() {
        let monitor = Monitor {
            counts: vec![
                StateCount {
                    state: AgentState::Busy,
                    total: Some(30),
                    shown: 20,
                },
                StateCount {
                    state: AgentState::Idle,
                    total: None,
                    shown: 0,
                },
            ],
            ..Monitor::default()
        };
        assert_eq!(monitor.counter_line(), "busy 30 (20 shown) · idle ?");
    }
}
