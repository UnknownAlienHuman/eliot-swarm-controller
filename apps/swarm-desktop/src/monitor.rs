//! Swarm monitor: demand-driven rows, honest counters and UI-only metric
//! history, as pure logic plus one bounded refresh read.
//!
//! One refresh reads one bounded multi-state `agent.list` page with joined
//! observations and scoped state totals (and, every `STOPPED_EVERY`
//! refreshes, the stopped rows for Recently stopped), the
//! new `x.user.agent.attention` reports after the last cursor (the first
//! read starts `ATTENTION_WINDOW_MS` back), and, for the selected Agent
//! only, `agent.get`, `usage.summary` and `diagnose`. No per-Agent timers,
//! no `agent.stats` per row, at most `MAX_CONCURRENT_READS` reads at once,
//! and only while the monitor is open.
//!
//! The panel shows what matters: Needs attention (waiting for the owner,
//! failed, lost, a controlled Agent whose state is not confirmed, or an open
//! attention report), Working (busy or starting), Idle, and a collapsed
//! Recently stopped. Children nest under their parent row, collapsed:
//! native observed subagents and Swarm workers alike. Every row sits in its
//! own group, as the charts count it; a worker that needs attention is
//! named on its parent's row.
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

use eliot_core::api::{
    AgentGetResponse, AgentListRequest, AgentListResponse, AgentListRow, DiagnoseResponse,
    ObservationView, ProcessStatsView, UsageSummaryResponse, stats_coverage_name,
};
use eliot_core::{AgentState, NativeChildActivityState};
use futures_util::StreamExt;

use crate::client::{self, MAX_CONCURRENT_READS};

/// Refresh period while the monitor is open.
pub const PERIOD_MS: u64 = 2_000;
/// Rows displayed per live state from the bounded fleet page.
pub const ROWS_PER_STATE: usize = 100;
/// Total live rows read in one page; per-state counts reveal any shortfall.
pub const FLEET_ROWS: usize = ROWS_PER_STATE * ROW_STATES.len();
/// Stopped rows read for Recently stopped (one page).
pub const STOPPED_ROWS: usize = 1_000;
/// The stopped rows are read again every this many refreshes (the total
/// every refresh).
pub const STOPPED_EVERY: u64 = 10;
/// Recently stopped: last changed within a day.
pub const RECENT_MS: i64 = 24 * 60 * 60 * 1000;
/// How far back the first attention read starts.
pub const ATTENTION_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;
/// The daemon's typed attention report kind (`docs/spec/monitoring.md`).
pub const ATTENTION_KIND: &str = "x.user.agent.attention";
/// `events.read` calls one refresh spends on attention reports (each scans
/// at most 1,000 journal rows server-side).
pub const ATTENTION_SCAN_PAGES: usize = 4;
/// Loss origins (`swarm why`) read per refresh; more continue next time.
pub const LOSS_READS: usize = 4;
/// UI-only samples kept per Agent series.
pub const RING_CAPACITY: usize = 600;
/// Bars drawn per graph; never more than useful at this width.
pub const GRAPH_BARS: usize = 60;

/// Live states read as rows, attention first; idle sessions still hold
/// processes.
pub const ROW_STATES: [AgentState; 8] = [
    AgentState::Waiting,
    AgentState::Failed,
    AgentState::Lost,
    AgentState::Unknown,
    AgentState::Busy,
    AgentState::Starting,
    AgentState::Created,
    AgentState::Idle,
];
/// States only counted every refresh.
pub const COUNT_STATES: [AgentState; 1] = [AgentState::Stopped];

/// The monitor's sections, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Group {
    Attention,
    Working,
    Idle,
    UnparentedSubagents,
    Stopped,
}

impl Group {
    /// The state filter's word for the group.
    pub fn filter_label(self) -> &'static str {
        match self {
            Self::Stopped => "Stopped",
            other => other.title(),
        }
    }

    pub fn title(self) -> &'static str {
        match self {
            Self::Attention => "Needs attention",
            Self::Working => "Working",
            Self::Idle => "Idle",
            Self::UnparentedSubagents => "Subagents without a visible parent",
            Self::Stopped => "Recently stopped",
        }
    }
}

/// One open attention report: its incident and reason code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incident {
    pub incident_id: String,
    pub reason: String,
}

/// An attention reason code in words.
pub fn attention_words(reason: &str) -> String {
    eliot_core::reason_words(reason)
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
        /// Processes not known to have exited (running or unknown).
        live_processes: usize,
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
                    coverage: stats_coverage_name(stats.coverage).to_owned(),
                    clock_regressed: stats.clock_regressed,
                    error: stats.error.clone(),
                    processes: data.processes.len(),
                    live_processes: data
                        .processes
                        .iter()
                        .filter(|process| process.state != eliot_core::ProcessState::Exited)
                        .count(),
                    processes_truncated: data.processes_truncated,
                }
            }
        }
    }

    /// Freshness as the daemon reported it, in words: sample age against
    /// its TTL, partial coverage and clock regression only when they apply;
    /// never inferred from the refresh time.
    pub fn freshness(&self) -> String {
        match self {
            Self::Unavailable(_) => "Process activity is not observed for this Agent.".to_owned(),
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
                    return "Not sampled yet.".to_owned();
                }
                let mut text = match age_ms {
                    Some(age) if *age > *ttl_ms => {
                        format!("Out of date: last sampled {} ago.", seconds(*age))
                    }
                    Some(age) => format!("Sampled {} ago.", seconds(*age)),
                    None => "Sampled; its age was not reported.".to_owned(),
                };
                if coverage == "partial" {
                    text.push_str(" Some processes could not be read.");
                }
                if *clock_regressed {
                    text.push_str(" The clock went back; the rate restarts.");
                }
                if let Some(error) = error {
                    text.push_str(&format!(" {error}"));
                }
                text
            }
        }
    }

    /// The Agent's processes not known to have exited (0 when unobserved).
    pub fn live_processes(&self) -> usize {
        match self {
            Self::Data { live_processes, .. } => *live_processes,
            Self::Unavailable(_) => 0,
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
    let secs = ms / 1000;
    match secs {
        s if s < 60 => format!("{s} s"),
        s if s < 3600 => format!("{} min", s / 60),
        s => format!("{} h {} min", s / 3600, (s % 3600) / 60),
    }
}

/// What is live, in words: "2 working · 1 needs attention · 3 idle", each
/// part only when it has Agents; "All quiet" when nothing is live.
pub fn live_words(working: usize, attention: usize, idle: usize) -> String {
    let mut parts = Vec::new();
    if working > 0 {
        parts.push(format!("{working} working"));
    }
    if attention > 0 {
        parts.push(format!(
            "{attention} {}",
            if attention == 1 {
                "needs attention"
            } else {
                "need attention"
            }
        ));
    }
    if idle > 0 {
        parts.push(format!("{idle} idle"));
    }
    if parts.is_empty() {
        "All quiet".to_owned()
    } else {
        parts.join(" · ")
    }
}

/// How long ago, compactly: "12 s", "4 min", "3 h", "2 d".
pub fn elapsed_label(ms: i64) -> String {
    let secs = ms.max(0) / 1000;
    match secs {
        s if s < 60 => format!("{s} s"),
        s if s < 3600 => format!("{} min", s / 60),
        s if s < 86_400 => format!("{} h", s / 3600),
        s => format!("{} d", s / 86_400),
    }
}

/// A restart loss is identified by structured facts, never summary prose.
pub fn restart_loss(reply: &DiagnoseResponse) -> bool {
    eliot_core::api::diagnosis_reason(&reply.steps).is_some_and(|step| {
        step.origin.as_deref() == Some("swarm")
            && step.failure_where.as_deref() == Some("daemon.restart")
    })
}

pub const RESTART_LOSS_WORDS: &str = "Interrupted by a Swarm restart";

/// A turn outcome in words ("completed", "interrupted", "failed",
/// "ended without a confirmed result").
pub fn outcome_words(outcome: &str) -> String {
    match outcome {
        "success" | "completed" => "completed".to_owned(),
        "unknown" => "ended without a confirmed result".to_owned(),
        other => other.replace('_', " "),
    }
}

/// An Agent state in words, for people.
pub fn state_words(state: AgentState) -> &'static str {
    match state {
        AgentState::Unknown => "State couldn't be confirmed",
        AgentState::Created | AgentState::Starting => "Starting",
        AgentState::Idle => "Ready",
        AgentState::Busy => "Working",
        AgentState::Waiting => "Waiting for you",
        AgentState::Stopped => "Stopped",
        AgentState::Failed => "Failed",
        AgentState::Lost => "Lost",
    }
}

/// The selected structured diagnosis reason, in words; healthy state alone
/// does not create a failure notice.
pub fn diagnosis_line(reply: &DiagnoseResponse) -> Option<String> {
    if restart_loss(reply) {
        return Some(RESTART_LOSS_WORDS.to_owned());
    }
    let step = eliot_core::api::diagnosis_reason(&reply.steps)?;
    let code = step.code.as_deref().unwrap_or("unknown");
    let origin = step.origin.as_deref().unwrap_or("unknown");
    if origin == "unknown"
        && step.message.is_none()
        && !matches!(
            code,
            "failure"
                | "failed"
                | "lost"
                | "agent.failed"
                | "agent.lost"
                | "interrupted"
                | "refused"
                | "crashed"
        )
    {
        return None;
    }
    Some(format!(
        "{}: {}",
        eliot_core::origin_words(origin),
        step.user_message()
    ))
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
    /// The sidebar's title for the Agent ([`crate::nav::summary_title`]).
    pub title: String,
    pub generation: i64,
    /// Current manager (durable parent link), if any.
    pub parent: Option<String>,
    pub state: AgentState,
    pub harness: String,
    /// The model the harness last reported ([`AgentSummary::model`]).
    pub model: Option<String>,
    pub active_turn: Option<String>,
    pub activity: Option<NativeChildActivityState>,
    pub last_progress_at: Option<i64>,
    pub progress_age_ms: Option<i64>,
    pub last_outcome: Option<String>,
    /// Fleet token count from the shared [`AgentSummary::fleet_usage`]
    /// derivation: the native total when supplied, otherwise the
    /// input+output fleet signal. `None` when the summary carries no usage.
    pub tokens: Option<i64>,
    /// Provenance of [`MonitorRow::tokens`]: `"turn"` or `"session"`,
    /// matching the summary `usage_scope`. `None` exactly when `tokens`
    /// is `None`; a session count never renders without its marker.
    pub tokens_scope: Option<String>,
    pub obs: Obs,
    /// A native observed child (the daemon reports `activity` only for
    /// those): it nests under its parent.
    pub native_child: bool,
    /// Last change of the Agent record (ms since the epoch).
    pub updated_at: i64,
    /// The Agent's project label (its workspace path, as listed).
    pub project: String,
    /// The authorization scope the server compares for moves and handoffs,
    /// when it differs from [`Self::project`].
    pub project_scope: Option<String>,
    /// A chat without a project (listed under Chats, not a project).
    pub chat: bool,
}

/// `1234567` as `1,234,567`.
pub fn group(value: i64) -> String {
    let digits = value.abs().to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    if value < 0 { format!("-{out}") } else { out }
}

/// Fleet token label for one monitor row: the grouped count and, in
/// words, what it covers ("1,234 tokens · last turn", "47,272 tokens ·
/// whole session"), or an em dash when the summary carries no usage. The
/// words are what stop a session fallback from passing as turn usage.
pub fn tokens_label(row: &MonitorRow) -> String {
    match (row.tokens, row.tokens_scope.as_deref()) {
        (Some(tokens), Some(scope)) => format!(
            "{} tokens · {}",
            group(tokens),
            if scope == "session" {
                "whole session"
            } else {
                "last turn"
            }
        ),
        _ => "—".to_owned(),
    }
}

impl MonitorRow {
    fn from_list(row: &AgentListRow) -> Self {
        let summary = &row.summary;
        let (tokens, tokens_scope) = match summary.fleet_usage() {
            Some((tokens, scope)) => (Some(tokens), Some(scope.to_owned())),
            None => (None, None),
        };
        Self {
            id: summary.id.clone(),
            title: crate::nav::summary_title(summary),
            generation: summary.generation,
            parent: summary.parent.clone(),
            state: summary.state,
            harness: summary.harness.clone(),
            model: summary.model.clone(),
            active_turn: summary.active_turn.clone(),
            activity: summary.activity.as_ref().map(|activity| activity.state),
            last_progress_at: summary.last_progress_at,
            progress_age_ms: summary.progress_age_ms,
            last_outcome: summary
                .last_outcome
                .map(|outcome| outcome.as_str().to_owned()),
            tokens,
            tokens_scope,
            obs: Obs::from_view(row.observation.as_ref()),
            native_child: summary.activity.is_some(),
            updated_at: summary.updated_at,
            project: summary.project.clone(),
            project_scope: summary.project_scope.clone(),
            chat: summary.chat,
        }
    }

    /// The row's status in words. A native child reads as its observed
    /// activity, and as "Ended with parent" once its parent's session ended
    /// (`ended_with_parent`, `docs/spec/core.md`).
    pub fn status_words(&self) -> &'static str {
        if !self.native_child {
            return state_words(self.state);
        }
        match (self.state, self.activity) {
            (AgentState::Stopped, _) => "Ended with parent",
            (_, Some(NativeChildActivityState::Running)) => "Working",
            (_, Some(NativeChildActivityState::Idle)) => "Idle",
            (_, Some(NativeChildActivityState::Interrupted)) => "Interrupted",
            (_, Some(NativeChildActivityState::Lost)) => "Lost",
            _ => state_words(self.state),
        }
    }

    /// The Agent ended (stopped, failed or lost): it has no live process.
    pub fn ended(&self) -> bool {
        matches!(
            self.state,
            AgentState::Stopped | AgentState::Failed | AgentState::Lost
        )
    }

    /// A lost or failed Agent Swarm controls, with no open turn: Resume
    /// starts its next generation.
    pub fn resumable(&self) -> bool {
        matches!(self.state, AgentState::Lost | AgentState::Failed)
            && !self.native_child
            && self.active_turn.is_none()
    }

    /// The row's own state group, also used for nested child summaries.
    pub fn group(&self, attention: Option<&Incident>) -> Group {
        if self.native_child {
            return match (self.state, self.activity) {
                (AgentState::Stopped | AgentState::Failed | AgentState::Lost, _) => Group::Stopped,
                (_, Some(NativeChildActivityState::Running)) => Group::Working,
                _ => Group::Idle,
            };
        }
        match self.state {
            AgentState::Waiting | AgentState::Failed | AgentState::Lost | AgentState::Unknown => {
                Group::Attention
            }
            AgentState::Stopped => Group::Stopped,
            _ if attention.is_some() => Group::Attention,
            AgentState::Busy | AgentState::Starting | AgentState::Created => Group::Working,
            AgentState::Idle => Group::Idle,
        }
    }

    /// It works on a turn now. A native child's turn is its observed
    /// activity, not a controlled active turn.
    fn in_turn(&self) -> bool {
        self.active_turn.is_some()
            || (self.activity == Some(NativeChildActivityState::Running)
                && self.state != AgentState::Stopped)
    }

    /// How long the row has been quiet, for its list line: a working
    /// Agent's last progress (the daemon's `last_progress_at`: turns,
    /// messages, tools and requests, `docs/spec/monitoring.md` Liveness),
    /// else the last change of its record.
    pub fn quiet(&self, now_ms: i64) -> Quiet {
        let changed = now_ms.saturating_sub(self.updated_at);
        match (self.in_turn(), self.progress_age_ms) {
            (true, Some(age)) => Quiet::Progress(age),
            (true, None) if self.last_progress_at.is_some() => Quiet::ProgressAgeUnavailable,
            (true, None) => Quiet::NoProgress(changed),
            (false, _) => Quiet::Changed(changed),
        }
    }

    /// What the Agent does, in words: the turn it works on, or how its
    /// last turn ended.
    pub fn operation(&self) -> String {
        let working = self.in_turn();
        let ended = matches!(
            self.state,
            AgentState::Stopped | AgentState::Failed | AgentState::Lost
        );
        match (working, &self.last_outcome) {
            (true, _) => match self.progress_age_ms {
                Some(age) => format!("Working · last progress {} ago", seconds(age)),
                None if self.last_progress_at.is_some() => {
                    "Working · progress age unavailable".to_owned()
                }
                None => "Working · no progress reported yet".to_owned(),
            },
            (false, Some(outcome)) => format!("Last turn {}", outcome_words(outcome)),
            // An open turn cut off by the end has no result; nothing is
            // invented for it.
            (false, None) if ended && self.activity == Some(NativeChildActivityState::Lost) => {
                "Its open turn has no result".to_owned()
            }
            (false, None) if ended || self.activity.is_some() => "No finished turn".to_owned(),
            (false, None) => "No turns yet".to_owned(),
        }
    }
}

/// How long a list row has been quiet ([`MonitorRow::quiet`]), in ms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Quiet {
    /// A working Agent's last progress.
    Progress(i64),
    /// The Agent reported progress, but the read could not measure its age.
    ProgressAgeUnavailable,
    /// A working Agent with no progress reported: its record's last change.
    NoProgress(i64),
    /// Any other Agent: its record's last change.
    Changed(i64),
}

impl Quiet {
    /// What the row shows ("2 s", "4 min", "no progress yet").
    pub fn label(self) -> String {
        match self {
            Self::Progress(ms) | Self::Changed(ms) => elapsed_label(ms),
            Self::ProgressAgeUnavailable => "progress age unavailable".to_owned(),
            Self::NoProgress(_) => "no progress yet".to_owned(),
        }
    }

    /// The same in words, for the row's accessible name.
    pub fn words(self) -> String {
        match self {
            Self::Progress(ms) => format!("last progress {} ago", elapsed_label(ms)),
            Self::ProgressAgeUnavailable => "progress age unavailable".to_owned(),
            Self::NoProgress(ms) => {
                format!(
                    "no progress reported yet, changed {} ago",
                    elapsed_label(ms)
                )
            }
            Self::Changed(ms) => format!("changed {} ago", elapsed_label(ms)),
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
    pub diagnosis: Result<DiagnoseResponse, String>,
}

/// Last successful reads of the selected Agent and its supplemental data.
#[derive(Clone, Debug)]
pub struct CachedDetail {
    pub read: DetailRead,
    identity_ms: i64,
    usage_ms: i64,
    diagnosis_ms: i64,
}

impl CachedDetail {
    pub fn oldest_read_ms(&self) -> i64 {
        self.identity_ms.min(self.usage_ms).min(self.diagnosis_ms)
    }
}

/// One attention report as read: its binding, phase and reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttentionReport {
    pub agent: String,
    pub generation: i64,
    pub incident_id: String,
    /// `incident` or `recovery`.
    pub phase: String,
    pub reason: String,
}

impl AttentionReport {
    /// The typed report of one `x.user.agent.attention` event (at
    /// `data.data`); `None` for anything else.
    pub fn from_event(event: &eliot_core::Event) -> Option<Self> {
        if event.kind != ATTENTION_KIND {
            return None;
        }
        let report = event.data.get("data")?;
        let text = |key: &str| report.get(key)?.as_str().map(str::to_owned);
        Some(Self {
            agent: text("agent_id").or_else(|| event.agent.clone())?,
            generation: report
                .get("generation")
                .and_then(serde_json::Value::as_i64)
                .or(event.generation)?,
            incident_id: text("incident_id")?,
            phase: text("phase")?,
            reason: text("reason").unwrap_or_default(),
        })
    }
}

/// The attention reports one refresh read and the cursor after them.
#[derive(Clone, Debug)]
pub struct AttentionRead {
    pub reports: Vec<AttentionReport>,
    pub next: i64,
}

/// Everything one refresh read.
#[derive(Clone, Debug)]
pub struct MonitorSnapshot {
    pub groups: Vec<(AgentState, Result<AgentListResponse, String>)>,
    pub counts: Vec<(AgentState, Result<usize, String>)>,
    /// The stopped rows, when this refresh read them.
    pub stopped: Option<Result<AgentListResponse, String>>,
    pub attention: Option<Result<AttentionRead, String>>,
    pub detail: Option<Result<DetailRead, String>>,
    /// `swarm why` summaries of the lost or failed generations it read.
    pub losses: Vec<((String, i64), Result<DiagnoseResponse, String>)>,
    pub read_at_ms: i64,
}

/// One monitor section: its top-level rows (each with its nested
/// children, see [`Tree`]).
#[derive(Clone, Debug)]
pub struct Section<'a> {
    pub group: Group,
    pub rows: Vec<&'a MonitorRow>,
}

/// A stopped row is shown only while recent.
fn visible(row: &MonitorRow, now_ms: i64) -> bool {
    row.state != AgentState::Stopped || now_ms.saturating_sub(row.updated_at) <= RECENT_MS
}

/// The rows shown at one instant: the top-level rows and, under each
/// parent, its children (native subagents and Swarm workers alike), in id
/// order. Built once per render.
pub struct Tree<'a> {
    monitor: &'a Monitor,
    top: Vec<&'a MonitorRow>,
    children: HashMap<&'a str, Vec<&'a MonitorRow>>,
}

impl<'a> Tree<'a> {
    /// The children shown under `parent`.
    pub fn children(&self, parent: &str) -> &[&'a MonitorRow] {
        self.children.get(parent).map_or(&[], Vec::as_slice)
    }

    /// The workers anywhere under `parent` that need attention themselves,
    /// in id order (each Agent once, even if links changed between reads).
    pub fn attention_workers(&self, parent: &str) -> Vec<&'a MonitorRow> {
        let mut found = Vec::new();
        let mut seen: HashSet<&str> = HashSet::from([parent]);
        let mut stack: Vec<&str> = vec![parent];
        while let Some(id) = stack.pop() {
            for child in self.children(id) {
                if !seen.insert(child.id.as_str()) {
                    continue;
                }
                if self.monitor.needs_attention(child) {
                    found.push(*child);
                }
                stack.push(child.id.as_str());
            }
        }
        found.sort_by(|a, b| id_order(&a.id, &b.id));
        found
    }

    /// The sections in order, each with its top-level rows in their own
    /// group: attention by urgency, working, idle and subagents without a
    /// visible parent by id, recently stopped newest first. A parent whose worker
    /// needs attention stays in its own section; its row names the worker
    /// ([`Tree::worker_attention_words`]).
    pub fn sections(&self) -> Vec<Section<'a>> {
        let mut sections: Vec<Section> = [
            Group::Attention,
            Group::Working,
            Group::Idle,
            Group::UnparentedSubagents,
            Group::Stopped,
        ]
        .into_iter()
        .map(|group| Section {
            group,
            rows: Vec::new(),
        })
        .collect();
        for row in &self.top {
            let group = if row.native_child {
                Group::UnparentedSubagents
            } else {
                row.group(self.monitor.incident(row))
            };
            if let Some(section) = sections.iter_mut().find(|section| section.group == group) {
                section.rows.push(row);
            }
        }
        for section in &mut sections {
            match section.group {
                Group::Attention => section.rows.sort_by(|a, b| {
                    attention_rank(a.state)
                        .cmp(&attention_rank(b.state))
                        .then_with(|| id_order(&a.id, &b.id))
                }),
                Group::Stopped => section.rows.sort_by(|a, b| {
                    b.updated_at
                        .cmp(&a.updated_at)
                        .then_with(|| id_order(&a.id, &b.id))
                }),
                Group::Working | Group::Idle | Group::UnparentedSubagents => {
                    section.rows.sort_by(|a, b| id_order(&a.id, &b.id));
                }
            }
        }
        sections
    }

    /// The line under a parent row that opens its children: how many
    /// workers and subagents, then their states ("22 workers · 2 working ·
    /// 20 idle"). Native-only children keep their own words ("2 subagents ·
    /// ended with parent").
    pub fn children_label(&self, parent: &str) -> String {
        let children = self.children(parent);
        let subagents = children.iter().filter(|row| row.native_child).count();
        let workers = children.len() - subagents;
        let plural = |count: usize, one: &str| {
            if count == 1 {
                format!("1 {one}")
            } else {
                format!("{count} {one}s")
            }
        };
        if workers == 0 {
            let ended = children
                .iter()
                .filter(|row| row.state == AgentState::Stopped)
                .count();
            let label = plural(subagents, "subagent");
            return if ended == subagents {
                format!("{label} · ended with parent")
            } else {
                label
            };
        }
        let mut parts = vec![plural(workers, "worker")];
        if subagents > 0 {
            parts.push(plural(subagents, "subagent"));
        }
        let (mut working, mut attention, mut idle, mut stopped) = (0, 0, 0, 0);
        for row in children {
            match row.group(self.monitor.incident(row)) {
                Group::Attention => attention += 1,
                Group::Working => working += 1,
                Group::Idle => idle += 1,
                Group::Stopped => stopped += 1,
                Group::UnparentedSubagents => unreachable!("display-only group"),
            }
        }
        if working + attention + idle > 0 {
            parts.push(live_words(working, attention, idle));
        }
        if stopped > 0 {
            parts.push(format!("{stopped} stopped"));
        }
        parts.join(" · ")
    }

    /// The parent row's attention line when its workers need attention
    /// and it does not itself: the first worker by id, named, with what it
    /// needs ("Waiting for you · worker a59 · Reply with exactly…").
    pub fn worker_attention_words(&self, parent: &str) -> Option<String> {
        let workers = self.attention_workers(parent);
        let first = workers.first()?;
        let words = match self.monitor.incident(first) {
            Some(incident) if !first.ended() && first.state != AgentState::Waiting => {
                attention_words(&incident.reason)
            }
            _ => first.status_words().to_owned(),
        };
        let more = match workers.len() - 1 {
            0 => String::new(),
            count => format!(" and {count} more"),
        };
        Some(format!(
            "{words} · worker {}{more} · {}",
            first.id, first.title
        ))
    }
}

/// A project as the sidebar names it: its section key and display name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectRef {
    pub key: String,
    pub name: String,
}

/// Live Agents of one scope (the fleet or one project) by group.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LiveCounts {
    pub working: usize,
    pub attention: usize,
    pub idle: usize,
}

impl LiveCounts {
    pub fn get(self, group: Group) -> usize {
        match group {
            Group::Working => self.working,
            Group::Attention => self.attention,
            Group::Idle => self.idle,
            Group::UnparentedSubagents | Group::Stopped => 0,
        }
    }

    pub fn total(self) -> usize {
        self.working + self.attention + self.idle
    }

    fn add(&mut self, group: Group) {
        match group {
            Group::Working => self.working += 1,
            Group::Attention => self.attention += 1,
            Group::Idle => self.idle += 1,
            Group::UnparentedSubagents | Group::Stopped => {}
        }
    }
}

/// One Agent the overview counts: its row, its own live group and its
/// project.
#[derive(Clone, Debug)]
pub struct Counted<'a> {
    pub row: &'a MonitorRow,
    pub group: Group,
    pub project: ProjectRef,
}

/// One project of the overview and its live Agents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectLive {
    pub project: ProjectRef,
    pub counts: LiveCounts,
}

/// What the list below the overview is narrowed to: one live group, one
/// project (matched by key), or both.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Filter {
    pub group: Option<Group>,
    pub project: Option<ProjectRef>,
    /// The search field's text: a row matches when its title, id, harness,
    /// model or project contains it, case-insensitively (k195 P3-2).
    pub text: String,
}

/// The states the Monitor's state filter offers, in its order (`None`:
/// every state).
pub const STATE_FILTERS: [Option<Group>; 5] = [
    None,
    Some(Group::Working),
    Some(Group::Attention),
    Some(Group::Idle),
    Some(Group::Stopped),
];

impl Filter {
    pub fn is_active(&self) -> bool {
        self.group.is_some() || self.project.is_some() || !self.text.trim().is_empty()
    }

    /// Whether `row`, in the project named `project`, matches the search
    /// text (an empty search matches every row).
    pub fn matches_text(&self, row: &MonitorRow, project: &str) -> bool {
        let wanted = self.text.trim().to_lowercase();
        if wanted.is_empty() {
            return true;
        }
        [
            row.title.as_str(),
            row.id.as_str(),
            row.harness.as_str(),
            crate::picker::harness_label(&row.harness),
            row.model.as_deref().unwrap_or_default(),
            project,
        ]
        .iter()
        .any(|field| field.to_lowercase().contains(&wanted))
    }

    /// Whether a recently stopped `row` stays under this filter: the state
    /// filter is every state or Stopped, the project and the text match.
    pub fn keeps_stopped(&self, row: &MonitorRow, project: &ProjectRef) -> bool {
        self.group.is_none_or(|group| group == Group::Stopped)
            && self
                .project
                .as_ref()
                .is_none_or(|kept| kept.key == project.key)
            && self.matches_text(row, &project.name)
    }

    /// The filter after a complete refresh (no failed read, no partial
    /// count): a group with no live Agent and a project with none are
    /// cleared, each on its own, so a kept group and a kept project stay
    /// combined even when nothing matches both; a kept project takes its
    /// current name. A partial overview keeps the filter as it is.
    pub fn reconciled(&self, overview: &Overview) -> Self {
        if overview.partial {
            return self.clone();
        }
        Self {
            // Stopped Agents are not live counts: that filter stays.
            group: self
                .group
                .filter(|group| *group == Group::Stopped || overview.counts.get(*group) > 0),
            project: self.project.as_ref().and_then(|project| {
                overview
                    .projects
                    .iter()
                    .find(|live| live.project.key == project.key)
                    .map(|live| live.project.clone())
            }),
            text: self.text.clone(),
        }
    }

    fn keeps(&self, counted: &Counted) -> bool {
        self.group.is_none_or(|group| counted.group == group)
            && self
                .project
                .as_ref()
                .is_none_or(|project| counted.project.key == project.key)
            && self.matches_text(counted.row, &counted.project.name)
    }
}

/// The Monitor overview at one instant: the counted rows, the fleet's
/// counts, its projects (most working first, then most live) and how many
/// live Agents the bounded read left out.
#[derive(Clone, Debug)]
pub struct Overview<'a> {
    pub rows: Vec<Counted<'a>>,
    pub counts: LiveCounts,
    pub projects: Vec<ProjectLive>,
    /// A live state holds more Agents than the read returned (its
    /// `state_totals` count exceeds its rows): every count is partial.
    /// Those totals include native children the chart leaves out, so no
    /// number of missing Agents is claimed.
    pub partial: bool,
}

impl<'a> Overview<'a> {
    /// Keep the selected filter on a partial read, but show all loaded
    /// chart rows if none match it.
    pub fn list_for_filter(&self, filter: &Filter) -> (Vec<Section<'a>>, bool) {
        let showing_loaded = self.partial && !self.rows.iter().any(|row| filter.keeps(row));
        let sections = if showing_loaded {
            self.filtered(&Filter::default())
        } else {
            self.filtered(filter)
        };
        (sections, showing_loaded)
    }

    /// The list under a filter: exactly the counted rows it keeps, flat,
    /// in the list's section order (attention by urgency, the others by
    /// id), so the list always matches the number that was clicked.
    fn filtered(&self, filter: &Filter) -> Vec<Section<'a>> {
        [Group::Attention, Group::Working, Group::Idle]
            .into_iter()
            .map(|group| {
                let mut rows: Vec<&'a MonitorRow> = self
                    .rows
                    .iter()
                    .filter(|counted| counted.group == group && filter.keeps(counted))
                    .map(|counted| counted.row)
                    .collect();
                rows.sort_by(|a, b| match group {
                    Group::Attention => attention_rank(a.state)
                        .cmp(&attention_rank(b.state))
                        .then_with(|| id_order(&a.id, &b.id)),
                    _ => id_order(&a.id, &b.id),
                });
                Section { group, rows }
            })
            .collect()
    }
}

/// Monitor model: rows, counters, open attention reports and per-Agent
/// series.
#[derive(Clone, Debug, Default)]
pub struct Monitor {
    pub rows: Vec<MonitorRow>,
    /// The last stopped rows read (refreshed every `STOPPED_EVERY`).
    pub stopped: Vec<MonitorRow>,
    pub counts: Vec<StateCount>,
    pub series: HashMap<String, Series>,
    /// Open attention reports by Agent and generation.
    pub incidents: HashMap<(String, i64), Incident>,
    /// `swarm why` summary of each lost or failed generation shown: read
    /// once, since the end of a generation does not change.
    pub losses: HashMap<(String, i64), DiagnoseResponse>,
    /// The attention read's cursor (`None` until the first read).
    pub attention_cursor: Option<i64>,
    /// A live row left the live states: read the stopped rows next, so a
    /// just-stopped Agent does not vanish until the periodic read.
    pub stopped_due: bool,
    pub detail: Option<CachedDetail>,
    pub detail_errors: Vec<String>,
    pub read_at_ms: Option<i64>,
    pub errors: Vec<String>,
    pub refreshes: u64,
}

impl Monitor {
    /// Whether the next refresh reads the stopped rows.
    pub fn wants_stopped(&self) -> bool {
        self.stopped_due || self.refreshes.is_multiple_of(STOPPED_EVERY) || self.stopped.is_empty()
    }

    /// The open attention report of a row's current generation.
    pub fn incident(&self, row: &MonitorRow) -> Option<&Incident> {
        self.incidents.get(&(row.id.clone(), row.generation))
    }

    /// A controlled row that needs the owner itself (its own section would
    /// be Needs attention); an observed native child never does.
    pub fn needs_attention(&self, row: &MonitorRow) -> bool {
        !row.native_child && row.group(self.incident(row)) == Group::Attention
    }

    /// The shown row of one Agent, live or recently stopped.
    pub fn row(&self, id: &str) -> Option<&MonitorRow> {
        self.all_rows().find(|row| row.id == id)
    }

    /// The row's words about why a resumable Agent ended, from its loss
    /// origin as the conversation words it ("Interrupted by a Swarm
    /// restart").
    /// `None` until read, and for rows that did not end that way.
    pub fn loss_words(&self, row: &MonitorRow) -> Option<String> {
        if !row.resumable() {
            return None;
        }
        self.losses
            .get(&(row.id.clone(), row.generation))
            .and_then(diagnosis_line)
    }

    /// Lost or failed generations shown whose loss origin is not read yet,
    /// at most `LOSS_READS` per refresh.
    pub fn loss_reads(&self) -> Vec<(String, i64)> {
        self.all_rows()
            .filter(|row| row.resumable())
            .map(|row| (row.id.clone(), row.generation))
            .filter(|key| !self.losses.contains_key(key))
            .take(LOSS_READS)
            .collect()
    }

    fn take_attention(&mut self, read: AttentionRead) {
        for report in read.reports {
            let key = (report.agent.clone(), report.generation);
            match report.phase.as_str() {
                "incident" => {
                    self.incidents.insert(
                        key,
                        Incident {
                            incident_id: report.incident_id,
                            reason: report.reason,
                        },
                    );
                }
                "recovery"
                    if self
                        .incidents
                        .get(&key)
                        .is_some_and(|open| open.incident_id == report.incident_id) =>
                {
                    self.incidents.remove(&key);
                }
                _ => {}
            }
        }
        self.attention_cursor = Some(read.next);
    }

    /// Every row shown: the live rows, then the stopped ones not already
    /// listed live.
    fn all_rows(&self) -> impl Iterator<Item = &MonitorRow> {
        let live: HashSet<&str> = self.rows.iter().map(|row| row.id.as_str()).collect();
        self.rows.iter().chain(
            self.stopped
                .iter()
                .filter(move |row| !live.contains(row.id.as_str())),
        )
    }

    /// Who sits under whom among the rows shown at `now_ms`: a child
    /// (native subagent or Swarm worker) nests under its shown parent.
    pub fn tree(&self, now_ms: i64) -> Tree<'_> {
        let rows: Vec<&MonitorRow> = self.all_rows().collect();
        let shown: HashMap<&str, &MonitorRow> = rows
            .iter()
            .filter(|row| visible(row, now_ms))
            .map(|row| (row.id.as_str(), *row))
            .collect();
        let mut tree = Tree {
            monitor: self,
            top: Vec::new(),
            children: HashMap::new(),
        };
        for row in rows {
            let parent = row.parent.as_deref().and_then(|id| shown.get(id));
            // A native child sits under its shown parent, ended or not. A
            // worker does while shown, except a live worker of a stopped
            // parent: it stays at the top, never hidden in Recently stopped.
            let nests = parent.is_some_and(|parent| {
                row.native_child
                    || (visible(row, now_ms)
                        && (row.ended() || parent.state != AgentState::Stopped))
            });
            if let Some(parent) = row.parent.as_deref().filter(|_| nests) {
                tree.children.entry(parent).or_default().push(row);
            } else if visible(row, now_ms)
                // A stopped native child whose parent is not shown ended.
                && !(row.native_child && row.state == AgentState::Stopped)
            {
                tree.top.push(row);
            }
        }
        for children in tree.children.values_mut() {
            children.sort_by(|a, b| id_order(&a.id, &b.id));
        }
        tree
    }

    /// The overview: every shown controlled Agent in a live group, by its
    /// own state (workers nested under a row included, native subagents
    /// not, as the sidebar's fleet line counts them), with its project from
    /// `project_of` (the sidebar's name). The headline, the state bar, the
    /// project rows and a filtered list all read these same rows.
    pub fn overview(
        &self,
        now_ms: i64,
        project_of: &dyn Fn(&MonitorRow) -> ProjectRef,
    ) -> Overview<'_> {
        let rows: Vec<Counted> = self
            .all_rows()
            .filter(|row| !row.native_child && visible(row, now_ms))
            .filter_map(|row| {
                let group = row.group(self.incident(row));
                (group != Group::Stopped).then(|| Counted {
                    row,
                    group,
                    project: project_of(row),
                })
            })
            .collect();
        let mut counts = LiveCounts::default();
        let mut projects: Vec<ProjectLive> = Vec::new();
        for counted in &rows {
            counts.add(counted.group);
            let index = match projects
                .iter()
                .position(|project| project.project.key == counted.project.key)
            {
                Some(index) => index,
                None => {
                    projects.push(ProjectLive {
                        project: counted.project.clone(),
                        counts: LiveCounts::default(),
                    });
                    projects.len() - 1
                }
            };
            projects[index].counts.add(counted.group);
        }
        projects.sort_by(|a, b| {
            b.counts
                .working
                .cmp(&a.counts.working)
                .then_with(|| b.counts.total().cmp(&a.counts.total()))
                .then_with(|| a.project.name.cmp(&b.project.name))
                .then_with(|| a.project.key.cmp(&b.project.key))
        });
        // A live state with more Agents than the bounded read returned,
        // including one with no rows in the page at all.
        let partial = self.counts.iter().any(|count| {
            count.state != AgentState::Stopped
                && count.total.is_some_and(|total| total > count.shown)
        });
        Overview {
            rows,
            counts,
            projects,
            partial,
        }
    }

    pub fn apply(&mut self, snapshot: MonitorSnapshot) {
        self.refreshes += 1;
        self.errors.clear();
        match snapshot.stopped {
            Some(Ok(page)) => {
                self.stopped = page.agents.iter().map(MonitorRow::from_list).collect();
                self.stopped_due = false;
            }
            Some(Err(error)) => self.errors.push(format!("stopped: {error}")),
            None => {}
        }
        match snapshot.attention {
            Some(Ok(read)) => self.take_attention(read),
            Some(Err(error)) => self.errors.push(format!("attention reports: {error}")),
            None => {}
        }
        self.take_fleet(&snapshot.groups, &snapshot.counts, snapshot.read_at_ms);
        if let Some(detail) = snapshot.detail {
            self.take_detail(detail, snapshot.read_at_ms);
        }
        // A failed loss read is retried by a later refresh.
        for (key, summary) in snapshot.losses {
            if let Ok(summary) = summary {
                self.losses.insert(key, summary);
            }
        }
        let shown: HashSet<(String, i64)> = self
            .all_rows()
            .map(|row| (row.id.clone(), row.generation))
            .collect();
        self.losses.retain(|key, _| shown.contains(key));
    }

    fn take_fleet(
        &mut self,
        groups: &[(AgentState, Result<AgentListResponse, String>)],
        totals: &[(AgentState, Result<usize, String>)],
        read_at_ms: i64,
    ) {
        // Rows and counters are projections of one fleet read. Only a
        // successful page replaces them or advances their freshness.
        if let Some(error) = groups
            .iter()
            .find_map(|(_, page)| page.as_ref().err())
            .or_else(|| totals.iter().find_map(|(_, count)| count.as_ref().err()))
        {
            self.errors.push(format!("fleet: {error}"));
            return;
        }
        let mut rows = Vec::new();
        let mut counts = Vec::new();
        for (state, page) in groups
            .iter()
            .filter_map(|(state, page)| Some((state, page.as_ref().ok()?)))
        {
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
        for (state, total) in totals {
            if let Ok(total) = total {
                counts.push(StateCount {
                    state: *state,
                    total: Some(*total),
                    shown: 0,
                });
            }
        }
        // Keep overlapping model inputs to one row per Agent, under its
        // most urgent state. The fleet RPC itself returns distinct rows.
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
        let live: HashSet<&str> = rows.iter().map(|row| row.id.as_str()).collect();
        if self.errors.is_empty() && self.rows.iter().any(|row| !live.contains(row.id.as_str())) {
            self.stopped_due = true;
        }
        self.rows = rows;
        self.counts = counts;
        self.read_at_ms = Some(read_at_ms);
    }

    fn take_detail(&mut self, result: Result<DetailRead, String>, read_at_ms: i64) {
        self.detail_errors.clear();
        let mut read = match result {
            Ok(read) => read,
            Err(error) => {
                self.detail_errors.push(error);
                return;
            }
        };
        if let Err(error) = &read.usage {
            self.detail_errors.push(format!("usage: {error}"));
        }
        if let Err(error) = &read.diagnosis {
            self.detail_errors.push(format!("diagnosis: {error}"));
        }
        let identity = &read.agent.agent;
        let previous = self.detail.take().filter(|last| {
            last.read.agent.agent.id == identity.id
                && last.read.agent.agent.generation == identity.generation
        });
        let (mut usage_ms, mut diagnosis_ms) = (read_at_ms, read_at_ms);
        if let Some(last) = previous {
            if read.usage.is_err() && last.read.usage.is_ok() {
                read.usage = last.read.usage;
                usage_ms = last.usage_ms;
            }
            if read.diagnosis.is_err() && last.read.diagnosis.is_ok() {
                read.diagnosis = last.read.diagnosis;
                diagnosis_ms = last.diagnosis_ms;
            }
        }
        self.detail = Some(CachedDetail {
            read,
            identity_ms: read_at_ms,
            usage_ms,
            diagnosis_ms,
        });
    }
}

/// Agent ids in creation order (`a9` before `a10`).
fn id_order(a: &str, b: &str) -> std::cmp::Ordering {
    (a.len(), a).cmp(&(b.len(), b))
}

type ReadFuture<'a> = Pin<Box<dyn Future<Output = Read> + 'a>>;

enum Read {
    Fleet(Result<AgentListResponse, String>),
    Stopped(Result<AgentListResponse, String>),
    Attention(Result<AttentionRead, String>),
    Detail(Box<Result<DetailRead, String>>),
    Loss((String, i64), Result<DiagnoseResponse, String>),
    Cancelled,
}

/// The attention reports after `cursor` (or, on the first read, from
/// `ATTENTION_WINDOW_MS` back) toward the current high-water mark, at most
/// `ATTENTION_SCAN_PAGES` reads per refresh. A backlog larger than that
/// continues from where this refresh stopped, so a busy journal never holds
/// a refresh for thousands of calls.
async fn read_attention(
    data_dir: &Path,
    cursor: Option<i64>,
    now_ms: i64,
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Result<AttentionRead, String> {
    let filter = eliot_core::api::EventsFilter {
        agents: Vec::new(),
        kinds: vec![ATTENTION_KIND.to_owned()],
        sources: Vec::new(),
    };
    let bounds = client::events_bounds(data_dir, &filter)
        .await
        .map_err(|error| error.to_string())?;
    let from = match cursor {
        // A cursor below the retention floor restarts at the floor.
        Some(cursor) => cursor.max(bounds.pruned_through),
        None => client::seq_before(
            data_dir,
            now_ms - ATTENTION_WINDOW_MS,
            bounds.pruned_through,
            bounds.hi,
            cancelled,
        )
        .await
        .map_err(|error| error.to_string())?,
    };
    let scan = client::scan_events_budgeted(
        data_dir,
        from,
        bounds.hi,
        &filter,
        ATTENTION_SCAN_PAGES,
        cancelled,
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(AttentionRead {
        reports: scan
            .events
            .iter()
            .filter_map(AttentionReport::from_event)
            .collect(),
        next: scan.next,
    })
}

fn list_request(state: AgentState, limit: usize, observations: bool) -> AgentListRequest {
    AgentListRequest {
        limit,
        observations,
        state: Some(state),
        ..AgentListRequest::default()
    }
}

fn fleet_request() -> AgentListRequest {
    AgentListRequest {
        states: Some(ROW_STATES.to_vec()),
        limit: FLEET_ROWS,
        observations: true,
        ..Default::default()
    }
}

/// Adapt one validated fleet page into the existing model input. Totals keep
/// row/byte truncation visible even when an entire state's rows are outside
/// the page. Stopped rows and attention retain their independent cadence.
fn take_fleet(snapshot: &mut MonitorSnapshot, page: Result<AgentListResponse, String>) {
    let page = page.and_then(|page| {
        fleet_request()
            .validate_response(&page)
            .map_err(|error| error.to_string())?;
        Ok(page)
    });
    match page {
        Ok(page) => {
            let totals = page.state_totals.as_ref().expect("validated state totals");
            for state in ROW_STATES {
                let agents: Vec<_> = page
                    .agents
                    .iter()
                    .filter(|row| row.summary.state == state)
                    .take(ROWS_PER_STATE)
                    .cloned()
                    .collect();
                let total = totals
                    .iter()
                    .find(|entry| entry.state == state)
                    .expect("validated state total")
                    .total;
                snapshot.groups.push((
                    state,
                    Ok(AgentListResponse {
                        state_totals: None,
                        next: agents.last().map(|row| row.summary.id.clone()),
                        truncated: agents.len() < total,
                        total,
                        agents,
                    }),
                ));
            }
            for state in COUNT_STATES {
                snapshot.counts.push((
                    state,
                    Ok(totals
                        .iter()
                        .find(|entry| entry.state == state)
                        .expect("validated state total")
                        .total),
                ));
            }
        }
        Err(error) => {
            snapshot.groups.extend(
                ROW_STATES
                    .into_iter()
                    .map(|state| (state, Err(error.clone()))),
            );
            snapshot.counts.extend(
                COUNT_STATES
                    .into_iter()
                    .map(|state| (state, Err(error.clone()))),
            );
        }
    }
}

/// What one refresh reads besides the fixed live set.
#[derive(Clone, Debug, Default)]
pub struct RefreshPlan {
    pub selected: Option<String>,
    pub stopped: bool,
    pub attention_cursor: Option<i64>,
    /// Lost or failed generations whose loss origin to read.
    pub losses: Vec<(String, i64)>,
}

/// One refresh: the fixed read set, at most `MAX_CONCURRENT_READS` at once.
pub async fn refresh(
    data_dir: &Path,
    plan: RefreshPlan,
    cancelled: &(dyn Fn() -> bool + Sync),
) -> Option<MonitorSnapshot> {
    let read_at_ms = crate::handoff::now_ms();
    let RefreshPlan {
        selected,
        stopped,
        attention_cursor,
        losses,
    } = plan;
    let mut reads: Vec<ReadFuture<'_>> = Vec::new();
    if stopped {
        reads.push(Box::pin(async move {
            if cancelled() {
                return Read::Cancelled;
            }
            let request = list_request(AgentState::Stopped, STOPPED_ROWS, false);
            Read::Stopped(
                client::list_agents(data_dir, &request)
                    .await
                    .map_err(|error| error.to_string()),
            )
        }));
    }
    reads.push(Box::pin(async move {
        if cancelled() {
            return Read::Cancelled;
        }
        Read::Attention(read_attention(data_dir, attention_cursor, read_at_ms, cancelled).await)
    }));
    reads.push(Box::pin(async move {
        if cancelled() {
            return Read::Cancelled;
        }
        Read::Fleet(
            client::list_agents(data_dir, &fleet_request())
                .await
                .map_err(|error| error.to_string()),
        )
    }));
    if let Some(agent) = selected {
        reads.push(Box::pin(async move {
            if cancelled() {
                return Read::Cancelled;
            }
            let read = match client::get_agent(data_dir, &agent).await {
                Ok(read) => read,
                Err(error) => return Read::Detail(Box::new(Err(error.to_string()))),
            };
            if cancelled() {
                return Read::Cancelled;
            }
            let usage = client::usage_summary(data_dir, &agent)
                .await
                .map_err(|error| error.to_string());
            if cancelled() {
                return Read::Cancelled;
            }
            let diagnosis = client::diagnose(data_dir, &agent)
                .await
                .map_err(|error| error.to_string());
            Read::Detail(Box::new(Ok(DetailRead {
                agent: read,
                usage,
                diagnosis,
            })))
        }));
    }
    for key in losses {
        reads.push(Box::pin(async move {
            if cancelled() {
                return Read::Cancelled;
            }
            let summary = client::diagnose(data_dir, &key.0)
                .await
                .map_err(|error| error.to_string());
            Read::Loss(key, summary)
        }));
    }
    let results: Vec<Read> = futures_util::stream::iter(reads)
        .buffer_unordered(MAX_CONCURRENT_READS)
        .collect()
        .await;
    let mut snapshot = MonitorSnapshot {
        groups: Vec::new(),
        counts: Vec::new(),
        stopped: None,
        attention: None,
        detail: None,
        losses: Vec::new(),
        read_at_ms,
    };
    if cancelled() || results.iter().any(|read| matches!(read, Read::Cancelled)) {
        return None;
    }
    for result in results {
        match result {
            Read::Fleet(page) => take_fleet(&mut snapshot, page),
            Read::Stopped(page) => snapshot.stopped = Some(page),
            Read::Attention(read) => snapshot.attention = Some(read),
            Read::Detail(detail) => snapshot.detail = Some(*detail),
            Read::Loss(key, summary) => snapshot.losses.push((key, summary)),
            Read::Cancelled => {}
        }
    }
    snapshot
        .groups
        .sort_by_key(|(state, _)| attention_rank(*state));
    Some(snapshot)
}

/// How old cached panel data must be before a panel says when it was
/// read: several missed refreshes, not the one in flight, so a healthy
/// panel never shows the line.
pub const STALE_AFTER_MS: i64 = 5 * PERIOD_MS as i64;

/// "Last read <local date and time>" for a cached read.
pub fn last_read_label(at_ms: i64) -> Option<String> {
    let at = chrono::DateTime::from_timestamp_millis(at_ms)?.with_timezone(&chrono::Local);
    Some(format!("Last read {}", at.format("%b %-d, %H:%M:%S")))
}

/// The read-age line under a cached panel. It appears only when the data
/// is old, so routine background refreshes never add, remove or rewrap a
/// line (each one used to shift the whole panel).
pub fn read_status(read_at_ms: Option<i64>, now_ms: i64) -> Option<String> {
    let read = read_at_ms?;
    if now_ms.saturating_sub(read) > STALE_AFTER_MS {
        last_read_label(read)
    } else {
        None
    }
}
