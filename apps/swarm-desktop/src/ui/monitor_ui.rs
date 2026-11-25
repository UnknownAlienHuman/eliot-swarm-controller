//! The right panel: the monitor (an overview of what is live, by state
//! and by project, above every task as one compact row, its workers and
//! subagents nested under it) and the open Agent's details (settings,
//! coverage, resources, usage), on one refresh chain that runs only while
//! the panel is open.

use std::collections::BTreeMap;

use super::*;
use crate::monitor::{Filter, MonitorRow, ProjectRef, Tree};
use crate::nav::Checkout;
use crate::ui::chat_ui::DraftAction;
use eliot_core::AgentState;

#[derive(Clone, Debug)]
pub(super) enum MonitorMsg {
    MonitorLoaded(u64, Option<Box<MonitorSnapshot>>),
    /// A monitor row: open its conversation and its details.
    RowOpened(String),
    /// A lost or failed row's Resume, bound to the generation shown: open
    /// its conversation and resume it there.
    ResumeRequested(String, i64),
    /// A live row's Stop, bound to the generation shown.
    StopRequested(String, i64),
    /// Show or hide the children (workers and subagents) of one row.
    ChildrenToggled(String),
    /// Show or hide Recently stopped.
    StoppedToggled,
    /// A pane tab or the header's details button: select that tab, never
    /// close the pane.
    PanelChosen(Panel),
    /// The search field's text.
    SearchChanged(String),
    /// A filter chip of the Agents page.
    FleetChosen(FleetFilter),
    /// Group the tree by project (true) or by state.
    GroupBy(bool),
    /// The swarm's wait classes (`daemon.status` `liveness`): the read's
    /// sequence and what it found.
    LivenessLoaded(u64, Box<Fallible<eliot_core::api::SwarmLiveness>>),
    /// Show or hide Autonomy's lines by source.
    AutonomyToggled,
    /// Swarm autonomy's window: the last 7 days (true) or 24 hours.
    AutonomyWindow(bool),
    /// Read Swarm autonomy again now.
    AutonomyRefresh,
    /// A Swarm autonomy read (its sequence, for the window it was asked for).
    AutonomyLoaded(u64, Box<Fallible<eliot_core::api::AutonomyResponse>>),
}

/// Swarm autonomy in the Monitor ([`crate::autonomy_view`]).
#[derive(Default)]
pub(super) struct AutonomyState {
    pub(super) open: bool,
    pub(super) week: bool,
    /// Tags reads: a reply for an older request is dropped.
    seq: u64,
    reading: bool,
    pub(super) report: Option<crate::autonomy_view::Report>,
}

/// The panel's refresh chain and the Agent whose details it reads.
#[derive(Default)]
pub(super) struct MonitorState {
    pub(super) data: Monitor,
    /// Tags refreshes; a reply for an older chain is dropped.
    pub(super) epoch: u64,
    task: Option<ComponentTask>,
    refresh_gate: Arc<tokio::sync::Mutex<()>>,
    refreshing: bool,
    /// Refreshes in a row that reported read errors: the next waits longer.
    pace: live::Pace,
    /// The Agent whose details (and usage) each refresh reads.
    pub(super) selected: Option<String>,
    /// Rows whose children are shown (collapsed by default).
    pub(super) expanded: HashSet<String>,
    /// Recently stopped is open (collapsed by default).
    pub(super) stopped_open: bool,
    /// What the list is narrowed to (a group, a project, or both).
    pub(super) filter: Filter,
    /// The event cursor the next scheduled read waits from.
    pub(super) event_cursor: Option<i64>,
    /// Read the selected Agent's usage and diagnosis at the next read.
    pub(super) detail_due: bool,
    /// The Agents page's filter chip and grouping.
    pub(super) fleet: FleetFilter,
    pub(super) by_project: bool,
    /// The wait classes as last read (`None` until read).
    pub(super) liveness: Option<Fallible<eliot_core::api::SwarmLiveness>>,
    /// Tags wait-class reads; an older reply is dropped.
    pub(super) liveness_seq: u64,
    /// The sidebar's read of attention reports is running (only while the
    /// panel, and so its refresh, is closed).
    pub(super) sidebar_reading: bool,
    pub(super) autonomy: AutonomyState,
    /// The hierarchy root the chain last planned the Agents tab's tree
    /// for (`None`: no tree planned).
    pub(super) tree_requested: Option<String>,
    /// When the last tree read started (or is planned to): the next waits
    /// [`monitor::LIST_PERIOD_MS`] after it, as the Agents page's reads do.
    tree_started_ms: Option<i64>,
}

/// The Agents page's filter chips (`Agents.dc.html`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum FleetFilter {
    #[default]
    All,
    Working,
    Waiting,
    /// An open attention report (no progress, no code progress…).
    Attention,
    Failed,
    Chats,
}

impl FleetFilter {
    const ALL: [Self; 6] = [
        Self::All,
        Self::Working,
        Self::Waiting,
        Self::Attention,
        Self::Failed,
        Self::Chats,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Working => "Working",
            Self::Waiting => "Waiting for you",
            Self::Attention => "Needs attention",
            Self::Failed => "Failed or lost",
            Self::Chats => "Chats",
        }
    }

    /// Whether `row` (with an open attention report or not) is listed.
    fn keeps(self, row: &MonitorRow, incident: bool) -> bool {
        match self {
            Self::All => true,
            Self::Working => FleetGroup::of(row.state) == FleetGroup::Working,
            Self::Waiting => row.state == AgentState::Waiting,
            Self::Attention => incident,
            Self::Failed => FleetGroup::of(row.state) == FleetGroup::Failed,
            Self::Chats => row.chat,
        }
    }
}

/// The tree's groups by state, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FleetGroup {
    Working,
    Waiting,
    /// Held by its provider, whatever its state.
    Held,
    Ready,
    Failed,
    /// Native subagents whose parent is not shown (observed, not
    /// controlled).
    Unparented,
    Stopped,
}

impl FleetGroup {
    const ALL: [Self; 7] = [
        Self::Working,
        Self::Waiting,
        Self::Held,
        Self::Ready,
        Self::Failed,
        Self::Unparented,
        Self::Stopped,
    ];

    fn of(state: AgentState) -> Self {
        match state {
            AgentState::Busy | AgentState::Starting | AgentState::Created => Self::Working,
            AgentState::Waiting => Self::Waiting,
            AgentState::Idle => Self::Ready,
            AgentState::Failed | AgentState::Lost | AgentState::Unknown => Self::Failed,
            AgentState::Stopped => Self::Stopped,
        }
    }

    /// A top-level row's group: a native subagent without its parent, a
    /// provider hold, else its state's.
    fn of_row(row: &MonitorRow) -> Self {
        if row.native_child && row.state != AgentState::Stopped {
            Self::Unparented
        } else if page_ui::held(row.status_line.as_deref()) {
            Self::Held
        } else {
            Self::of(row.state)
        }
    }

    fn states(self) -> &'static [AgentState] {
        match self {
            Self::Held | Self::Unparented => &[],
            Self::Working => &[AgentState::Busy, AgentState::Starting, AgentState::Created],
            Self::Waiting => &[AgentState::Waiting],
            Self::Ready => &[AgentState::Idle],
            Self::Failed => &[AgentState::Failed, AgentState::Lost, AgentState::Unknown],
            Self::Stopped => &[AgentState::Stopped],
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::Working => "Working",
            Self::Waiting => "Waiting for you",
            Self::Held => "On provider hold",
            Self::Ready => "Ready",
            Self::Failed => "Failed or lost",
            Self::Unparented => "Subagents without a visible parent",
            Self::Stopped => "Recently stopped",
        }
    }
}

impl DesktopApp {
    pub(super) fn monitor_update(&mut self, msg: MonitorMsg, context: &ComponentContext<Self>) {
        match msg {
            MonitorMsg::MonitorLoaded(epoch, snapshot) => {
                if epoch != self.monitor.epoch || !self.monitor_panel() {
                    return;
                }
                self.monitor.refreshing = false;
                if let Some(snapshot) = snapshot {
                    if snapshot.tree.is_some() {
                        self.monitor.tree_started_ms = Some(snapshot.read_at_ms);
                    }
                    self.monitor.event_cursor = snapshot.event_cursor;
                    let fleet = snapshot.fleet;
                    let tree = snapshot.tree.is_some();
                    // Any read of the refresh that failed (fleet, stopped,
                    // attention, tree, Details, losses) asks whether the
                    // daemon runs (itself paced) and waits longer before the
                    // next, with the same growing pauses as every retry.
                    let failed = snapshot.failed();
                    let rows = monitor::sidebar_rows(&snapshot);
                    self.monitor.data.apply(*snapshot);
                    if let Some(rows) = rows {
                        // The fleet read stands in for the sidebar's own: a
                        // clean one is a clean sidebar round too.
                        self.nav.pace.reset();
                        self.take_fleet_rows(rows);
                    }
                    // A refresh that read a list (the fleet, the task's tree)
                    // comes again at the sidebar's cadence; Details alone at
                    // the Monitor's.
                    let period = if fleet || tree {
                        monitor::LIST_PERIOD_MS
                    } else {
                        monitor::PERIOD_MS
                    };
                    let delay = self.monitor.pace.next(!failed, period);
                    if failed {
                        self.link_suspect(context);
                    } else {
                        self.link_answered();
                    }
                    // The next read waits on its own worker: a timer message
                    // would render the whole window for nothing.
                    self.read_monitor(delay, context);
                }
            }
            MonitorMsg::RowOpened(id) => {
                let agent = self
                    .nav
                    .agents
                    .iter()
                    .find(|agent| agent.id == id)
                    .cloned()
                    .or_else(|| {
                        self.monitor
                            .data
                            .rows
                            .iter()
                            .find(|row| row.id == id)
                            .map(row_agent)
                    });
                if let Some(agent) = agent {
                    self.shell_update(ShellMsg::AgentOpened(agent.clone()), context);
                    self.show_details(&agent.id, context);
                }
            }
            MonitorMsg::ResumeRequested(id, generation) => {
                if self.submitting() {
                    return;
                }
                self.monitor_update(MonitorMsg::RowOpened(id.clone()), context);
                self.chat_update(
                    ChatMsg::DraftRequested(DraftAction::Resume {
                        agent: id,
                        generation,
                    }),
                    context,
                );
            }
            MonitorMsg::StopRequested(id, generation) => {
                self.stop_agent(&id, generation, context);
            }
            MonitorMsg::ChildrenToggled(id) => {
                if !self.monitor.expanded.remove(&id) {
                    self.monitor.expanded.insert(id);
                }
            }
            MonitorMsg::StoppedToggled => self.monitor.stopped_open = !self.monitor.stopped_open,
            MonitorMsg::SearchChanged(text) => {
                // A rebuilt field reports the text it already shows.
                if self.monitor.filter.text != text {
                    self.monitor.filter.text = text;
                }
            }
            MonitorMsg::FleetChosen(filter) => self.monitor.fleet = filter,
            MonitorMsg::GroupBy(by_project) => self.monitor.by_project = by_project,
            MonitorMsg::LivenessLoaded(seq, read) => {
                if seq == self.monitor.liveness_seq {
                    self.monitor.liveness = Some(*read);
                }
            }
            MonitorMsg::AutonomyToggled => {
                let autonomy = &mut self.monitor.autonomy;
                autonomy.open = !autonomy.open;
            }
            MonitorMsg::AutonomyWindow(week) => {
                if self.monitor.autonomy.week != week {
                    self.monitor.autonomy.week = week;
                    self.monitor.autonomy.report = None;
                    self.read_autonomy(0, context);
                }
            }
            MonitorMsg::AutonomyRefresh => {
                self.read_autonomy(0, context);
                self.read_liveness(context);
            }
            MonitorMsg::AutonomyLoaded(seq, read) => {
                if seq != self.monitor.autonomy.seq {
                    return;
                }
                self.monitor.autonomy.reading = false;
                let now = crate::handoff::now_ms();
                let checkouts = self.checkouts();
                // A registration by the sidebar's name for it, else its id.
                let project_name = |id: &str| {
                    self.nav
                        .projects
                        .iter()
                        .find(|project| project.id == id)
                        .map_or_else(
                            || id.to_owned(),
                            |project| self.place_title_in(&project.workspace, &checkouts),
                        )
                };
                let report = match *read {
                    Ok(read) => crate::autonomy_view::Report::Read(views::Shared(Rc::new(
                        crate::autonomy_view::read(
                            &read,
                            |agent| self.agent_title(agent),
                            project_name,
                            now,
                        ),
                    ))),
                    Err(error) => crate::autonomy_view::Report::Failed(error),
                };
                self.monitor.autonomy.report = Some(report);
                if self.main == Main::Agents {
                    self.read_autonomy(crate::autonomy_view::REFRESH_MS, context);
                }
            }
            MonitorMsg::PanelChosen(panel) => self.choose_panel(panel, context),
        }
    }

    /// Select a pane tab. Details shows the open task's Agent, else the one
    /// chosen in the Monitor (without either it stays where it is); the
    /// other tabs show the open task. Only [`Self::close_panel`] closes the
    /// pane.
    pub(super) fn choose_panel(&mut self, panel: Panel, context: &ComponentContext<Self>) {
        match panel {
            Panel::Details => {
                if let Some(agent) = self.details_agent() {
                    self.show_details(&agent, context);
                }
            }
            Panel::Changes | Panel::Agents | Panel::Terminal => self.open_tab(panel, context),
            Panel::Closed => {}
        }
    }

    /// The Agent the Details tab shows: the open task's, else the one
    /// chosen in the Monitor.
    fn details_agent(&self) -> Option<String> {
        self.selected_agent_id()
            .or_else(|| self.monitor.selected.clone())
    }

    /// Open the details of `agent` in the right panel.
    pub(super) fn show_details(&mut self, agent: &str, context: &ComponentContext<Self>) {
        let changed = self.monitor.selected.as_deref() != Some(agent);
        if changed {
            self.monitor.selected = Some(agent.to_owned());
            self.monitor.data.detail_errors.clear();
            self.monitor.detail_due = true;
        }
        // Another tab ran the chain with its own reads: read Details now.
        let was_closed = !self.monitor_panel() || self.panel != Panel::Details;
        self.panel = Panel::Details;
        if was_closed || changed {
            self.start_monitor(context);
        }
    }

    /// Whether what shows runs the Monitor's refresh chain: the Agents
    /// page, or the pane's Details or Agents tab (the Inbox reads with the
    /// sidebar's poll; Changes and Terminal read on their own).
    pub(super) fn monitor_panel(&self) -> bool {
        self.main == Main::Agents
            || (self.panel_shown() && matches!(self.panel, Panel::Details | Panel::Agents))
    }

    /// After every update: neither the Agents page nor Details shows any
    /// more, so the running read (a wait or a read) is cancelled now
    /// rather than at its next result.
    pub(super) fn sync_monitor(&mut self) {
        if !self.monitor_panel()
            && let Some(task) = self.monitor.task.take()
        {
            task.cancel();
            self.monitor.epoch += 1;
            self.monitor.refreshing = false;
        }
    }

    /// Close the panel: no refresh, no render work.
    pub(super) fn close_panel(&mut self) {
        self.panel = Panel::Closed;
        self.monitor.refreshing = false;
        self.monitor.epoch += 1;
        if let Some(task) = self.monitor.task.take() {
            task.cancel();
        }
    }

    /// Read while the panel is open, keeping the last successful data visible.
    pub(super) fn start_monitor(&mut self, context: &ComponentContext<Self>) {
        self.read_monitor(0, context);
    }

    /// Read Swarm autonomy for the chosen window after `delay_ms` (the
    /// shown report stays until the read lands). The daemon runs one report
    /// at a time on a read-only snapshot; a shown report is read again
    /// every [`crate::autonomy_view::REFRESH_MS`] while the Monitor shows it.
    pub(super) fn read_autonomy(&mut self, delay_ms: u64, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let autonomy = &mut self.monitor.autonomy;
        autonomy.seq += 1;
        autonomy.reading = delay_ms == 0;
        let (seq, week) = (autonomy.seq, autonomy.week);
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 && !feed::backoff(delay_ms, &cancelled) {
                return Message::Monitor(MonitorMsg::AutonomyLoaded(
                    seq,
                    Box::new(Err("cancelled".to_owned())),
                ));
            }
            let since = week.then(|| crate::handoff::now_ms() - crate::autonomy_view::WEEK_MS);
            let read = client::block_on(client::autonomy(&data_dir, since))
                .map_err(|error| error.user_message());
            Message::Monitor(MonitorMsg::AutonomyLoaded(seq, Box::new(read)))
        });
    }

    /// One refresh after `delay_ms` (the next one is planned when it lands).
    /// While no daemon answers nothing is read; the reconnect reads again.
    pub(super) fn read_monitor(&mut self, delay_ms: u64, context: &ComponentContext<Self>) {
        if let Some(task) = self.monitor.task.take() {
            task.cancel();
        }
        // The task's tree only while the pane's Agents tab shows; noted
        // first, so the pane does not ask again while the daemon is down.
        let shown = self.panel_shown();
        let tree = self
            .tree_root()
            .filter(|_| shown && self.panel == Panel::Agents);
        self.monitor.tree_requested = tree.clone();
        if self.daemon_down() {
            self.monitor.refreshing = false;
            return;
        }
        self.monitor.epoch += 1;
        let data_dir = match self.data_dir.clone() {
            Ok(data_dir) => data_dir,
            Err(error) => {
                self.monitor.refreshing = false;
                self.monitor.data.errors = vec![format!("no data directory: {error}")];
                return;
            }
        };
        let epoch = self.monitor.epoch;
        // The fleet only while the Agents page shows; the selected Agent
        // only while its Details show; its usage and diagnosis only when
        // the selection or its generation changed.
        let fleet = self.main == Main::Agents;
        let selected = self
            .monitor
            .selected
            .clone()
            .filter(|_| shown && self.panel == Panel::Details);
        // A tree read starts at least LIST_PERIOD_MS after the last one,
        // however it is asked for (an event, the tab, a selection,
        // Refresh), as the Agents page's scheduled reads do; a scheduled
        // delay counts.
        let now = crate::handoff::now_ms();
        let spacing_ms = match (&tree, self.monitor.tree_started_ms) {
            (Some(_), Some(started)) => {
                u64::try_from(started.saturating_add(monitor::LIST_PERIOD_MS as i64) - now)
                    .unwrap_or(0)
                    .saturating_sub(delay_ms)
            }
            _ => 0,
        };
        if tree.is_some() {
            self.monitor.tree_started_ms = Some(now + (delay_ms + spacing_ms) as i64);
        }
        let cached = self.monitor.data.detail.as_ref().map(|detail| {
            (
                detail.read.agent.agent.id.clone(),
                detail.read.agent.agent.generation,
            )
        });
        let current = selected.as_ref().and_then(|id| {
            self.monitor
                .data
                .rows
                .iter()
                .find(|row| &row.id == id)
                .map(|row| row.generation)
        });
        let detail_full = std::mem::take(&mut self.monitor.detail_due)
            || selected.as_ref().is_some_and(|id| {
                cached.as_ref().is_none_or(|(cached_id, generation)| {
                    cached_id != id || current.is_some_and(|current| current != *generation)
                })
            });
        let filter = match &tree {
            Some(root) if self.monitor.data.tree.root.as_ref() == Some(root) => {
                monitor::tree_filter(self.monitor.data.tree.agents())
            }
            Some(root) => monitor::tree_filter(vec![root.clone()]),
            None => monitor::fleet_filter(if fleet { None } else { selected.as_deref() }),
        };
        let plan = monitor::RefreshPlan {
            fleet,
            selected,
            detail_full,
            stopped: self.monitor.data.wants_stopped(),
            attention_cursor: self.monitor.data.attention_cursor,
            losses: if fleet {
                self.monitor.data.loss_reads()
            } else {
                Vec::new()
            },
            tree,
        };
        let cursor = self.monitor.event_cursor;
        let refresh_gate = self.monitor.refresh_gate.clone();
        self.monitor.refreshing = true;
        self.monitor.task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if spacing_ms > 0 && !feed::backoff(spacing_ms, &cancelled) {
                return Message::Monitor(MonitorMsg::MonitorLoaded(epoch, None));
            }
            // A scheduled read (`delay_ms` > 0) waits at least that long,
            // then for one of the fleet's events, at most QUIET_PERIOD_MS
            // in all. Nothing is read while the window is minimized or
            // hidden; the first read after it shows again is immediate.
            if delay_ms > 0 && !feed::backoff(delay_ms, &cancelled) {
                return Message::Monitor(MonitorMsg::MonitorLoaded(epoch, None));
            }
            let paused = |cancelled: &dyn Fn() -> bool| -> bool {
                let mut paused = false;
                while crate::window_place::hidden() {
                    paused = true;
                    if !feed::backoff(monitor::HIDDEN_CHECK_MS, cancelled) {
                        break;
                    }
                }
                paused
            };
            let woke = paused(&cancelled);
            let snapshot = client::block_on(async {
                // A cancelled epoch may still have an IPC read in flight.
                let _guard = refresh_gate.lock().await;
                if cancelled() {
                    return None;
                }
                let wait = monitor::QUIET_PERIOD_MS.saturating_sub(delay_ms);
                // A delay that already covers the quiet period reads now.
                if delay_ms > 0 && !woke && wait > 0 {
                    // Wait from the tip read before the last read: an event
                    // committed since wakes it at once. Without a cursor, or
                    // when the wait fails, the quiet period passes instead.
                    // Cancellable: leaving the page drops the long-poll.
                    let waited = match cursor {
                        Some(after) => {
                            match feed::cancellable(
                                client::events_wait(&data_dir, after, &filter, wait),
                                &cancelled,
                            )
                            .await
                            {
                                None => return None,
                                Some(reply) => reply.is_ok(),
                            }
                        }
                        None => false,
                    };
                    // Inside the runtime: `pause`, never `backoff` (it
                    // starts its own runtime).
                    if !waited && !feed::pause(wait, &cancelled).await {
                        return None;
                    }
                    while crate::window_place::hidden() {
                        if !feed::pause(monitor::HIDDEN_CHECK_MS, &cancelled).await {
                            return None;
                        }
                    }
                }
                if cancelled() {
                    return None;
                }
                // The tip before this read: the next wait starts there, so
                // nothing committed during the read is missed.
                let tip = client::events_bounds(&data_dir, &filter)
                    .await
                    .ok()
                    .map(|bounds| bounds.hi);
                monitor::refresh(&data_dir, plan, &cancelled)
                    .await
                    .map(|mut snapshot| {
                        snapshot.event_cursor = tip;
                        snapshot
                    })
            });
            Message::Monitor(MonitorMsg::MonitorLoaded(epoch, snapshot.map(Box::new)))
        }));
    }

    /// A monitor row's project as the sidebar names it: its section key
    /// ([`nav::project_key`]) and the owner's display name for that
    /// project, else the sidebar's name ([`nav::project_of`]); a chat
    /// lists under Chats, as in the sidebar.
    fn monitor_project(&self, row: &MonitorRow, checkouts: &[Checkout]) -> ProjectRef {
        if row.chat {
            return ProjectRef {
                key: "chats".to_owned(),
                name: "Chats".to_owned(),
            };
        }
        let key = nav::project_key(&row.project, checkouts);
        let name = self
            .prefs
            .display_name(&format!("project:{key}"))
            .map_or_else(|| nav::project_of(&row.project, checkouts), str::to_owned);
        ProjectRef { key, name }
    }

    /// One tree row, and under it (when `tree` is given and its children
    /// are shown) its workers and subagents one step in (recursively).
    /// `selected` is the open Agent's id; `now` is the snapshot's read time,
    /// so the entries change only with a snapshot.
    #[allow(
        clippy::too_many_arguments,
        reason = "The operation's distinct inputs remain explicit at its established call boundary."
    )]
    pub(super) fn monitor_entries(
        &self,
        row: &MonitorRow,
        depth: usize,
        tree: Option<&Tree>,
        selected: Option<&str>,
        project: String,
        now: i64,
        entries: &mut Vec<monitor_view::MonitorEntry>,
    ) {
        let data = &self.monitor.data;
        let title = self.agent_title(&row.id);
        let incident = data
            .incident(row)
            .filter(|_| row.state != eliot_core::AgentState::Stopped);
        // The activity column: the open attention report, why a lost or
        // failed Agent ended, the first worker that needs you, the status
        // line, else its state and how long it has been quiet.
        let (activity, attention) = if let Some(incident) = incident {
            (incident.words(now), true)
        } else if row.resumable() {
            (
                data.loss_words(row)
                    .unwrap_or_else(|| row.status_words().to_owned()),
                false,
            )
        } else if let Some(words) = tree
            .filter(|_| !data.needs_attention(row))
            .and_then(|tree| tree.worker_attention_words(&row.id))
        {
            (words, true)
        } else if let Some(status) = &row.status_line {
            (status.clone(), data.needs_attention(row))
        } else if row.native_child {
            (row.operation(), false)
        } else {
            (
                format!("{} · {}", row.status_words(), row.quiet(now).label()),
                false,
            )
        };
        let point = data
            .series
            .get(&row.id)
            .and_then(|series| series.points().back());
        let cpu = point
            .and_then(|point| point.cpu_percent)
            .map_or_else(|| "–".to_owned(), |cpu| format!("{cpu:.0}%"));
        let memory = point
            .and_then(|point| point.rss_bytes)
            .map_or_else(|| "–".to_owned(), |bytes| monitor::bytes_label(Some(bytes)));
        // A top-level row whose parent is not shown names its parent task.
        let under = row
            .parent
            .as_deref()
            .filter(|_| depth == 0)
            .map(|parent| self.agent_title(parent))
            .filter(|parent| Some(parent.as_str()) != row.parent.as_deref())
            .map_or_else(String::new, |parent| format!(", under {parent}"));
        let children = tree
            .map(|tree| tree.children(&row.id))
            .filter(|children| !children.is_empty() && depth <= 4);
        let open = self.monitor.expanded.contains(&row.id);
        entries.push(monitor_view::MonitorEntry::Row(Box::new(
            monitor_view::RowEntry {
                row: row.clone(),
                title,
                project,
                model: crate::picker::harness_model_label(&row.harness, row.model.as_deref()),
                activity,
                attention,
                cpu,
                memory,
                words: format!(
                    ", {}{under}{}",
                    row.quiet(now).words(),
                    children
                        .map(|_| format!(
                            ", {}",
                            tree.map(|tree| tree.children_label(&row.id))
                                .unwrap_or_default()
                        ))
                        .unwrap_or_default()
                ),
                selected: selected == Some(row.id.as_str()),
                children: children.map(|_| open),
                kid: depth > 0,
                observed: row.native_child,
                resume: row.resumable().then(|| !self.submitting()),
                stop: (!row.native_child && !row.ended())
                    .then(|| self.stop_button(&row.id, row.generation))
                    .flatten(),
            },
        )));
        if let Some(children) = children.filter(|_| open) {
            for child in children {
                self.monitor_entries(
                    child,
                    depth + 1,
                    tree,
                    selected,
                    String::new(),
                    now,
                    entries,
                );
            }
        }
    }

    /// Read the swarm's wait classes (`daemon.status` `liveness`) once:
    /// when the Agents page opens and on its Refresh, never on a timer.
    pub(super) fn read_liveness(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.monitor.liveness_seq += 1;
        let seq = self.monitor.liveness_seq;
        let _task = context.spawn_background(move |_| {
            let read = client::block_on(client::daemon_status(&data_dir))
                .map(|status| status.liveness)
                .map_err(|error| error.user_message());
            Message::Monitor(MonitorMsg::LivenessLoaded(seq, Box::new(read)))
        });
    }

    /// The Agents page (`Agents.dc.html`): search and grouping, the
    /// summary, the filter chips with counts, the wait classes, the tree
    /// (managers and their workers; state mark, task, project, model,
    /// activity, CPU, memory, Stop) and Autonomy. The tree is its own
    /// component ([`monitor_view::MonitorList`]), redrawn only when a
    /// snapshot or a choice changes it.
    pub(super) fn monitor_view(&self, context: &mut ViewContext<Self>) -> View {
        let data = &self.monitor.data;
        // Ages and incidents as of the last read (the snapshot's time, not
        // the clock's), so the page changes only with a read.
        let read_at = data.read_at_ms.unwrap_or_default();
        let checkouts = self.checkouts();
        let project_of = |row: &MonitorRow| self.monitor_project(row, &checkouts);
        let partial = data.partial();
        let tree = data.tree(read_at);
        let fleet = self.monitor.fleet;
        let search = &self.monitor.filter;
        let incident = |row: &MonitorRow| data.incident(row).is_some();
        let live: Vec<&MonitorRow> = data.rows.iter().filter(|row| !row.native_child).collect();
        let count = |filter: FleetFilter| {
            live.iter()
                .filter(|row| filter.keeps(row, incident(row)))
                .count()
        };
        let mut items: Vec<KeyedView> = Vec::new();

        // Title row: search and grouping.
        let field = TextBox::new()
            .text(search.text.clone())
            .placeholder_text("Search Agents")
            .automation_name("Search Agents")
            .on_text_changed(crate::views::text_edits(
                context.callback(|text| Message::Monitor(MonitorMsg::SearchChanged(text))),
            ));
        let by_project = self.monitor.by_project;
        items.push(KeyedView::new(
            "title",
            page_ui::toolbar(
                page_ui::title("Agents"),
                vec![
                    KeyedView::new(
                        "search",
                        Border::new()
                            .width(tokens::page::SEARCH)
                            .content(controls::search_field(field)),
                    ),
                    KeyedView::new(
                        "group",
                        page_ui::choice(&[
                            (
                                "By state",
                                !by_project,
                                context.message(Message::Monitor(MonitorMsg::GroupBy(false))),
                            ),
                            (
                                "By project",
                                by_project,
                                context.message(Message::Monitor(MonitorMsg::GroupBy(true))),
                            ),
                        ]),
                    ),
                ],
            ),
        ));

        // The summary.
        let working = count(FleetFilter::Working);
        let waiting = count(FleetFilter::Waiting);
        let holds = live
            .iter()
            .filter(|row| page_ui::held(row.status_line.as_deref()))
            .count();
        let (tokens_today, reporting) = self.tokens_today();
        let partial_tip =
            partial.then(|| "Partial: more Agents are live than this read loaded.".to_owned());
        items.push(KeyedView::new(
            format!("stats:{working}:{waiting}:{holds}:{tokens_today}:{reporting}"),
            page_ui::grid(vec![
                page_ui::stat_panel(
                    page_ui::stat(working.to_string(), "Working", partial_tip.clone()),
                ),
                page_ui::stat_panel(
                    page_ui::stat(waiting.to_string(), "Waiting for you", partial_tip),
                ),
                page_ui::stat_panel(
                    page_ui::stat(
                        holds.to_string(),
                        "On provider hold",
                        Some("Agents their provider holds (quota, rate limit, sign-in)".to_owned()),
                    ),
                ),
                page_ui::stat_panel(
                    page_ui::stat(
                        crate::monitor::group(tokens_today),
                        "Tokens today",
                        Some(format!(
                            "Totals of {reporting} loaded Agents active today; each reports its whole total"
                        )),
                    ),
                ),
            ]),
        ));

        // The filter chips.
        let chips: Vec<KeyedView> = FleetFilter::ALL
            .into_iter()
            .map(|filter| {
                let selected = fleet == filter;
                let n = count(filter);
                KeyedView::new(
                    format!("{}:{selected}:{n}", filter.label()),
                    page_ui::filter_chip(
                        filter.label(),
                        Some(n),
                        selected,
                        context.message(Message::Monitor(MonitorMsg::FleetChosen(filter))),
                    ),
                )
            })
            .collect();
        items.push(KeyedView::new("filters", page_ui::chip_rows(chips, 6)));

        // The wait classes.
        let waits = match &self.monitor.liveness {
            None => "Waits: reading…".to_owned(),
            Some(Ok(liveness)) => format!(
                "Waits: Moving {} · Asked {} · Parked {} · Stuck {} · Long {}",
                liveness.moving, liveness.asked, liveness.parked, liveness.stuck, liveness.long
            ),
            Some(Err(error)) => format!("Waits: not read ({error})"),
        };
        items.push(KeyedView::new(
            format!("waits:{waits}"),
            controls::secondary(waits).text_wrapping(TextWrapping::Wrap),
        ));
        if data.read_at_ms.is_none() && self.monitor.refreshing && data.errors.is_empty() {
            items.push(KeyedView::new(
                "reading",
                page_ui::quiet("Reading the swarm…"),
            ));
        }
        if !data.errors.is_empty() {
            items.push(KeyedView::new(
                "errors",
                page_ui::error_line(format!("Some reads failed: {}", data.errors.join("; "))),
            ));
        }
        if !data.row_notes.is_empty() {
            items.push(KeyedView::new(
                "row-notes",
                page_ui::error_line(format!(
                    "Some rows were not read whole: {}",
                    data.row_notes
                        .iter()
                        .map(|note| note.text.as_str())
                        .collect::<Vec<_>>()
                        .join("; ")
                )),
            ));
        }

        // The tree.
        let searching = !search.text.trim().is_empty();
        let flat = searching || fleet != FleetFilter::All;
        let keeps = |row: &MonitorRow| {
            fleet.keeps(row, incident(row)) && search.matches_text(row, &project_of(row).name)
        };
        let top: Vec<&MonitorRow> = if flat {
            data.rows
                .iter()
                .filter(|row| !row.native_child && keeps(row))
                .chain(
                    data.stopped
                        .iter()
                        .filter(|row| searching && fleet == FleetFilter::All && keeps(row)),
                )
                .collect()
        } else {
            tree.sections()
                .into_iter()
                .flat_map(|section| section.rows)
                .collect()
        };
        let focused = self
            .selected_agent_id()
            .or_else(|| self.monitor.selected.clone());
        let nest = (!flat).then_some(&tree);
        let mut entries = Vec::new();
        let push_rows = |rows: &[&MonitorRow], entries: &mut Vec<monitor_view::MonitorEntry>| {
            for row in rows {
                let project = project_of(row).name;
                self.monitor_entries(row, 0, nest, focused.as_deref(), project, read_at, entries);
            }
        };
        if by_project {
            let mut groups: BTreeMap<String, Vec<&MonitorRow>> = BTreeMap::new();
            for row in &top {
                groups.entry(project_of(row).name).or_default().push(row);
            }
            for (name, rows) in &groups {
                entries.push(monitor_view::MonitorEntry::Section(
                    format!("project:{name}"),
                    format!("{name} · {}", rows.len()),
                ));
                push_rows(rows, &mut entries);
            }
        } else {
            for group in FleetGroup::ALL {
                let rows: Vec<&MonitorRow> = top
                    .iter()
                    .copied()
                    .filter(|row| FleetGroup::of_row(row) == group)
                    .collect();
                if rows.is_empty() {
                    continue;
                }
                // The state totals count held Agents in their state; they
                // list under On provider hold instead.
                let held_here = data
                    .rows
                    .iter()
                    .filter(|row| {
                        FleetGroup::of(row.state) == group
                            && page_ui::held(row.status_line.as_deref())
                    })
                    .count();
                let total = data
                    .counts
                    .iter()
                    .filter(|count| group.states().contains(&count.state))
                    .try_fold(0usize, |sum, count| count.total.map(|total| sum + total))
                    .map_or(rows.len(), |total| {
                        total.saturating_sub(held_here).max(rows.len())
                    });
                let label = format!("{} · {} of {total}", group.title(), rows.len());
                if group == FleetGroup::Stopped {
                    let open = self.monitor.stopped_open || searching;
                    entries.push(monitor_view::MonitorEntry::Stopped(label, open));
                    if !open {
                        continue;
                    }
                } else {
                    entries.push(monitor_view::MonitorEntry::Section(
                        format!("section:{group:?}"),
                        label,
                    ));
                }
                push_rows(&rows, &mut entries);
            }
        }
        if data.read_at_ms.is_some() && top.is_empty() {
            entries.push(monitor_view::MonitorEntry::Note(
                if flat {
                    "No Agents match this filter"
                } else {
                    "No Agents to show"
                }
                .to_owned(),
            ));
        }
        if partial {
            items.push(KeyedView::new(
                "partial",
                page_ui::quiet("Showing loaded Agents: more are live than this read loaded."),
            ));
        }
        items.push(KeyedView::new(
            "tree",
            Border::new()
                .background(ThemeBrush::CardBackground)
                .border_brush(ThemeBrush::CardStroke)
                .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
                .corner_radius(tokens::radius::CONTAINER)
                .padding(Thickness::uniform(
                    tokens::spacing::SM - tokens::layout::HAIRLINE,
                ))
                .automation_name("Agents")
                .content(View::component::<monitor_view::MonitorList>(
                    monitor_view::MonitorListInput {
                        entries,
                        open: context.callback(|id| Message::Monitor(MonitorMsg::RowOpened(id))),
                        resume: context.callback(|(id, generation)| {
                            Message::Monitor(MonitorMsg::ResumeRequested(id, generation))
                        }),
                        stop: context.callback(|(id, generation)| {
                            Message::Monitor(MonitorMsg::StopRequested(id, generation))
                        }),
                        children: context
                            .callback(|id| Message::Monitor(MonitorMsg::ChildrenToggled(id))),
                        stopped: context
                            .callback(|()| Message::Monitor(MonitorMsg::StoppedToggled)),
                    },
                )),
        ));

        // Autonomy.
        let autonomy = &self.monitor.autonomy;
        items.push(KeyedView::new(
            "autonomy",
            View::component::<crate::autonomy_view::Autonomy>(
                crate::autonomy_view::AutonomyInput {
                    open: autonomy.open,
                    week: autonomy.week,
                    report: autonomy
                        .report
                        .clone()
                        .unwrap_or(crate::autonomy_view::Report::Reading),
                    reading: autonomy.reading,
                    toggle: context.callback(|()| Message::Monitor(MonitorMsg::AutonomyToggled)),
                    window: context
                        .callback(|week: bool| Message::Monitor(MonitorMsg::AutonomyWindow(week))),
                    refresh: context.callback(|()| Message::Monitor(MonitorMsg::AutonomyRefresh)),
                },
            ),
        ));
        page_ui::page("Agents", tokens::page::WIDTH, items)
    }

    /// Details (skeleton 5.5, `Finished.dc.html`): the open Agent in
    /// collapsible sections (Summary, Task, Goal, Work, Mail, What went
    /// wrong, Activity & resources, Context, Heavy work, Technical). Ids and
    /// readback text live here, not in the conversation. Moving it under
    /// another task is a menu action ([`super::move_ui`]).
    pub(super) fn details_view(&self, context: &mut ViewContext<Self>) -> Option<View> {
        let id = self.monitor.selected.clone()?;
        let mut items: Vec<KeyedView> = Vec::new();
        let mut technical = vec![format!("ID {id}")];
        let data = &self.monitor.data;
        let now = crate::handoff::now_ms();

        // The Agent: from the open conversation's detail when it is this
        // Agent, else the monitor's read.
        let detail = self
            .chat
            .detail
            .as_ref()
            .filter(|_| self.selected_agent_id().as_deref() == Some(id.as_str()));
        let generation = detail
            .map(|detail| detail.generation)
            .or_else(|| self.find_agent(&id).map(|agent| agent.generation));
        let cached = data.detail.as_ref().filter(|cached| {
            let agent = &cached.read.agent.agent;
            agent.id == id && generation.is_none_or(|generation| agent.generation == generation)
        });
        let read = cached.map(|cached| &cached.read);
        let previous_generation = data
            .row(&id)
            .is_some_and(|row| generation.is_some_and(|generation| row.generation != generation));
        let resources_at = data
            .read_at_ms
            .filter(|_| !previous_generation && data.row(&id).is_some());
        let read_at = cached
            .map(monitor::CachedDetail::oldest_read_ms)
            .into_iter()
            .chain(resources_at)
            .min();
        if let Some(status) = monitor::read_status(read_at, now) {
            items.push(KeyedView::new(
                "refresh",
                controls::secondary(status).text_wrapping(TextWrapping::Wrap),
            ));
        }
        let errors: Vec<_> = data
            .detail_errors
            .iter()
            .chain(&data.errors)
            .map(String::as_str)
            .collect();
        if !errors.is_empty() {
            items.push(KeyedView::new(
                "read-errors",
                controls::secondary(format!("Some reads failed: {}", errors.join(" · ")))
                    .foreground(ThemeBrush::SystemCritical)
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
        // No cached read and no error yet: the first read is still due.
        let pending = data.detail_errors.is_empty();

        // Summary: state, harness, model and effort, access and preset, who
        // chose the model, the generation, when it started, its parent.
        // Effort words read short; `spoken` holds the same lines in full
        // words where they start, for the accessible name.
        let mut agent_lines: Vec<String> = Vec::new();
        let mut spoken: Option<(usize, Vec<String>)> = None;
        match (detail, read) {
            (Some(detail), _) => {
                let harness = crate::picker::harness_label(&detail.harness);
                agent_lines.push(format!("{harness} · {}", state_label(detail.state)));
                spoken = Some((agent_lines.len(), detail.settings.spoken_lines()));
                agent_lines.extend(detail.settings.lines());
                agent_lines.push(format!(
                    "Access: {}",
                    launch_ui::agent_access_label(&detail.settings)
                ));
                if !detail.settings.profile.starts_with("adhoc-") {
                    agent_lines.push(format!("Preset {}", detail.settings.profile));
                }
                if detail.observe_only {
                    agent_lines.push(format!(
                        "{harness} runs this one itself: Swarm shows it but cannot steer or stop it."
                    ));
                }
                if detail.active_turn.is_some() {
                    agent_lines.push("Working on a turn".to_owned());
                }
            }
            (None, Some(read)) => {
                spoken = Some((agent_lines.len(), monitor_view::identity_spoken_lines(read)));
                agent_lines.extend(monitor_view::identity_lines(read));
                let settings = configure::Settings::from_reply(&read.agent);
                agent_lines.push(format!(
                    "Access: {}",
                    launch_ui::agent_access_label(&settings)
                ));
            }
            (None, None) => agent_lines.push(
                if pending {
                    "Reading…"
                } else {
                    "Details unavailable."
                }
                .to_owned(),
            ),
        }
        if !self.configure.status.is_empty() && detail.is_some() {
            technical.push(format!("Last configure: {}", self.configure.status));
        }
        let mut agent_spoken = agent_lines.clone();
        if let Some((at, words)) = spoken {
            for (line, said) in agent_spoken.iter_mut().skip(at).zip(words) {
                *line = said;
            }
        }
        let body = controls::body(agent_lines.join("\n"))
            .text_wrapping(TextWrapping::Wrap)
            .is_text_selection_enabled(true);
        let body = if agent_spoken == agent_lines {
            body
        } else {
            body.automation_name(agent_spoken.join("\n"))
        };
        let mut summary: Vec<KeyedView> = vec![KeyedView::new("lines", body)];
        // How the profile was chosen: the routing decision frozen at
        // admission; the skipped candidates go to Technical.
        let choice = match (detail, read) {
            (Some(detail), _) => Some((detail.routing.clone(), detail.settings.profile.clone())),
            (None, Some(read)) => Some((
                read.agent.agent.profile_choice.clone(),
                read.agent.agent.profile.clone(),
            )),
            (None, None) => None,
        };
        if let Some((decision, profile)) = choice {
            let chosen = match decision {
                Some(decision) => {
                    let (headline, skipped) = crate::settings_model::decision_lines(&decision);
                    technical.extend(skipped);
                    headline.replacen("Routing picks", "Routing picked", 1)
                }
                None if profile.starts_with("adhoc-") => {
                    "You chose its app and model when you started it.".to_owned()
                }
                None => format!("You started it on the preset {profile}."),
            };
            summary.push(KeyedView::new(
                "chosen",
                page_ui::kv_text("Chosen by", chosen),
            ));
        }
        if let Some(generation) = generation {
            summary.push(KeyedView::new(
                "generation",
                page_ui::kv_text("Generation", generation.to_string()),
            ));
        }
        let created = detail
            .map(|detail| detail.created_at)
            .or_else(|| read.map(|read| read.agent.agent.created_at))
            .filter(|at| *at > 0);
        if let Some(created) = created.and_then(monitor::last_read_label) {
            summary.push(KeyedView::new(
                "started",
                page_ui::kv_text("Started", created.replacen("Last read ", "", 1)),
            ));
        }
        let parent = detail
            .and_then(|detail| detail.parent.clone())
            .or_else(|| read.and_then(|read| read.agent.agent.parent.clone()));
        if let Some(parent) = parent {
            summary.push(KeyedView::new(
                "parent",
                page_ui::kv_text("Parent", self.agent_title(&parent)),
            ));
        }
        items.push(KeyedView::new(
            "summary",
            self.details_section("summary", "Summary", summary, context),
        ));

        // Task: what the task record would hold, where the desktop can read
        // it (branch, base, preset, goal); the record itself is a daemon
        // read the desktop does not have yet.
        let goal = self
            .chat
            .goal
            .as_ref()
            .filter(|(agent, _)| *agent == id)
            .map(|(_, goal)| goal.as_ref());
        let mut task: Vec<KeyedView> = Vec::new();
        if let Some(branch) = detail.and_then(|detail| detail.branch.clone()) {
            task.push(KeyedView::new("branch", page_ui::kv_text("Branch", branch)));
        }
        if let Some(base) = detail.and_then(|detail| detail.base_ref.clone()) {
            task.push(KeyedView::new("base", page_ui::kv_text("Base", base)));
        }
        if let Some(detail) = detail {
            task.push(KeyedView::new(
                "profile",
                page_ui::kv_text("Profile", detail.settings.profile.clone()),
            ));
        }
        if let Some(goal) = goal.flatten() {
            task.push(KeyedView::new(
                "goal",
                page_ui::kv_text("Goal", goal.definition.text.clone()),
            ));
            if !goal.definition.check.is_empty() {
                task.push(KeyedView::new(
                    "check",
                    page_ui::kv_text("Check", goal.definition.check.join(" ")),
                ));
            }
        }
        task.push(KeyedView::new(
            "record",
            controls::secondary(
                "The task's text, card and journal live in its record in Git; the desktop cannot read that record yet.",
            )
            .text_wrapping(TextWrapping::Wrap),
        ));
        items.push(KeyedView::new(
            "task",
            self.details_section("task", "Task", task, context),
        ));

        // Goal: its state, rounds and turns of the budget, cost, the check
        // and its last result, cadence and scope, with the controls the
        // daemon has.
        let mut goal_rows: Vec<KeyedView> = Vec::new();
        match goal {
            None => goal_rows.push(KeyedView::new("reading", controls::secondary("Reading…"))),
            Some(None) => {
                goal_rows.push(KeyedView::new("none", controls::secondary("No goal.")));
                goal_rows.push(KeyedView::new(
                    "set",
                    Border::new()
                        .horizontal_alignment(HorizontalAlignment::Left)
                        .content(
                            controls::text_button()
                                .is_enabled(false)
                                .automation_name(
                                    "Set goal: setting a goal is not in the desktop yet",
                                )
                                .content(controls::label("Set goal…")),
                        )
                        .tooltip(
                            "Setting a goal is not in the desktop yet: swarm goal set does it",
                        ),
                ));
            }
            Some(Some(goal)) => {
                goal_rows.extend(goal_lines(goal, now));
                goal_rows.extend(self.goal_actions(goal, context));
            }
        }
        items.push(KeyedView::new(
            "goal",
            self.details_section("goal", "Goal", goal_rows, context),
        ));

        // Work: the worktree (opens in Explorer) and its project's
        // checkout, the branch and its commits, the Git-aware work summary
        // of this generation (an open `no_code_progress` report in the
        // attention tone), and the last checkpoint.
        let row = data
            .row(&id)
            .filter(|row| generation.is_none_or(|generation| row.generation == generation));
        let work = read
            .and_then(|read| read.agent.work.as_ref())
            .or_else(|| row.and_then(|row| row.work.as_ref()))
            .or_else(|| self.find_agent(&id).and_then(|agent| agent.work.as_deref()));
        let no_code = row
            .filter(|row| row.state != eliot_core::AgentState::Stopped)
            .and_then(|row| data.incident(row))
            .filter(|incident| incident.no_code());
        let (project_id, workspace) = match (detail, read) {
            (Some(detail), _) => (Some(detail.project.clone()), detail.workspace.clone()),
            (None, Some(read)) => (
                Some(read.agent.agent.project.clone()),
                read.agent.agent.workspace.clone(),
            ),
            (None, None) => {
                let agent = self.find_agent(&id);
                (
                    agent.map(|agent| agent.project_id.clone()),
                    agent.and_then(|agent| agent.workspace.clone()),
                )
            }
        };
        let project = project_id
            .as_deref()
            .and_then(|id| self.nav.projects.iter().find(|project| project.id == id))
            .map(|project| project.workspace.clone())
            .or_else(|| workspace.clone());
        let mut work_rows: Vec<KeyedView> = Vec::new();
        if let Some(project) = project.filter(|project| !project.is_empty()) {
            if let Some(worktree) = workspace
                .clone()
                .filter(|workspace| nav::path_key(workspace) != nav::path_key(&project))
            {
                work_rows.push(KeyedView::new(
                    "worktree",
                    self.path_row(context, "Worktree", "", &worktree),
                ));
            }
            work_rows.push(KeyedView::new(
                "project",
                self.path_row(context, "Project folder", "", &project),
            ));
        }
        if let Some(branch) = detail.and_then(|detail| detail.branch.clone()) {
            let ahead =
                work.and_then(|work| work.commits_ahead)
                    .map_or_else(String::new, |ahead| {
                        format!(
                            " · {} ahead",
                            eliot_core::count_words(ahead, "commit", "commits")
                        )
                    });
            work_rows.push(KeyedView::new(
                "branch",
                page_ui::kv_text("Branch", format!("{branch}{ahead}")),
            ));
        }
        if let Some(incident) = no_code {
            work_rows.push(KeyedView::new(
                "no-code",
                controls::body(incident.words(now))
                    .foreground(ThemeBrush::SystemCritical)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ));
        }
        if let Some(work) = work {
            let lines = monitor::work_detail_lines(work, now);
            if !lines.is_empty() {
                work_rows.push(KeyedView::new(
                    "summary",
                    controls::body(lines.join("\n"))
                        .text_wrapping(TextWrapping::Wrap)
                        .is_text_selection_enabled(true),
                ));
            }
        }
        work_rows.push(KeyedView::new(
            "checkpoint",
            Border::new()
                .content(page_ui::kv_text("Checkpoint", "\u{2014}"))
                .tooltip("The daemon keeps checkpoints but the desktop cannot read them yet"),
        ));
        items.push(KeyedView::new(
            "work",
            self.details_section("work", "Work", work_rows, context),
        ));

        // Mail: this Agent's unread mail, read-only (reading marks nothing).
        let mut mail: Vec<KeyedView> = Vec::new();
        match read.map(|read| &read.mail) {
            None => mail.push(KeyedView::new("reading", controls::secondary("Reading…"))),
            Some(Err(error)) if error.is_empty() => {
                mail.push(KeyedView::new("reading", controls::secondary("Reading…")));
            }
            Some(Err(error)) => mail.push(KeyedView::new(
                "error",
                page_ui::error_line(format!("Mail not read: {error}")),
            )),
            Some(Ok(inbox)) if inbox.deliveries.is_empty() => {
                mail.push(KeyedView::new(
                    "none",
                    controls::secondary("No unread mail."),
                ));
            }
            Some(Ok(inbox)) => {
                for delivery in &inbox.deliveries {
                    let sender = match delivery.sender.as_str() {
                        "owner" => "You".to_owned(),
                        sender if sender.starts_with('a') => self.agent_title(sender),
                        sender => sender.to_owned(),
                    };
                    let thread = delivery
                        .thread
                        .as_deref()
                        .map_or_else(String::new, |thread| format!(" · {thread}"));
                    let age = monitor::elapsed_label(now.saturating_sub(delivery.admitted_at));
                    mail.push(KeyedView::new(
                        delivery.delivery_id.clone(),
                        StackPanel::new()
                            .spacing(tokens::spacing::ROW_GAP)
                            .children((
                                controls::text(
                                    format!("{sender}{thread} · {age} ago"),
                                    tokens::text::LABEL,
                                ),
                                controls::body(delivery.text.clone())
                                    .text_wrapping(TextWrapping::Wrap)
                                    .max_lines(4)
                                    .is_text_selection_enabled(true),
                            )),
                    ));
                }
                if inbox.next.is_some() && inbox.deliveries.len() >= monitor::MAIL_ROWS {
                    mail.push(KeyedView::new(
                        "more",
                        controls::secondary(format!("The first {} are shown.", monitor::MAIL_ROWS)),
                    ));
                }
            }
        }
        items.push(KeyedView::new(
            "mail",
            self.details_section("mail", "Mail", mail, context),
        ));

        // What went wrong: why and where the Agent last failed, in one line
        // (the full chain lives behind `swarm why`).
        if let Some(line) = read
            .and_then(|read| read.diagnosis.as_ref().ok())
            .and_then(monitor::diagnosis_line)
        {
            items.push(KeyedView::new(
                "diagnosis",
                self.details_section(
                    "wrong",
                    "What went wrong",
                    vec![
                        KeyedView::new(
                            "line",
                            controls::body(line.clone())
                                .text_wrapping(TextWrapping::Wrap)
                                .is_text_selection_enabled(true),
                        ),
                        KeyedView::new(
                            "copy",
                            controls::text_button()
                                .horizontal_alignment(HorizontalAlignment::Left)
                                .automation_name("Copy diagnosis")
                                .on_click(context.message(Message::Pane(
                                    super::pane_ui::PaneMsg::Copy("the diagnosis".to_owned(), line),
                                )))
                                .content(controls::icon_label(
                                    controls::glyph::COPY,
                                    "Copy diagnosis",
                                )),
                        ),
                    ],
                    context,
                ),
            ));
        }

        // The conversation's retained coverage and the feed: only what is
        // unusual (older turns not loaded, pruned activity, a feed that is
        // not live).
        if let Some(convo) = self.chat.convo.as_ref().filter(|convo| convo.agent() == id) {
            technical.extend(convo.coverage().notes(convo.inputs_unavailable()));
            if !matches!(
                self.chat.live,
                LiveStatus::Live | LiveStatus::Idle | LiveStatus::Loading
            ) {
                technical.push(format!("Live updates: {}", self.chat.live.label()));
            }
        }

        let resources = if previous_generation {
            vec![KeyedView::new(
                "new-session",
                controls::secondary("No resource sample for this session yet."),
            )]
        } else {
            monitor_view::resource_views(data, &id)
        };
        items.push(KeyedView::new(
            "resources",
            self.details_section(
                "resources",
                "Activity & resources",
                vec![KeyedView::new(
                    "views",
                    StackPanel::new()
                        .spacing(tokens::spacing::MD)
                        .keyed_children(resources),
                )],
                context,
            ),
        ));

        // Context: the fill and the session's tokens, then what Swarm
        // loads (project instructions and the owner rules' delivery).
        let mut context_rows: Vec<KeyedView> = Vec::new();
        if let Some((used, window)) = detail.and_then(|detail| detail.context) {
            let percent = 100.0 * used as f64 / window as f64;
            context_rows.push(KeyedView::new(
                "used",
                page_ui::kv_text(
                    "Used",
                    format!(
                        "{} of {} tokens",
                        monitor::group(used),
                        monitor::group(window)
                    ),
                ),
            ));
            context_rows.push(KeyedView::new("meter", controls::meter(percent, false)));
        }
        let usage = match read {
            Some(read) => monitor_view::usage_lines(read),
            None if pending => vec!["Reading…".to_owned()],
            None => vec!["Usage unavailable.".to_owned()],
        };
        context_rows.push(KeyedView::new(
            "usage",
            controls::body(usage.join("\n"))
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
        ));
        if let Some(growth) = read.and_then(|read| read.agent.session_growth.as_ref()) {
            context_rows.push(KeyedView::new(
                "growth",
                controls::body(format!(
                    "The session grows fast: {}",
                    growth
                        .message
                        .as_deref()
                        .unwrap_or("its size crossed the warning threshold")
                ))
                .foreground(ThemeBrush::SystemCritical)
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        if let Some(rules) = read.and_then(|read| read.agent.owner_rules_delivery.as_ref()) {
            context_rows.push(KeyedView::new(
                "rules",
                page_ui::kv_text(
                    "Owner rules",
                    format!(
                        "{} · revision {}",
                        if rules.delivered {
                            "delivered"
                        } else {
                            "not delivered"
                        },
                        rules.revision
                    ),
                ),
            ));
        }
        if let Some(lines) = detail
            .and_then(|detail| detail.project_instructions.clone())
            .or_else(|| read.and_then(|read| monitor_view::project_instruction_lines(&read.agent)))
        {
            context_rows.push(KeyedView::new(
                "instructions-head",
                controls::text("Project instructions", tokens::text::LABEL),
            ));
            context_rows.push(KeyedView::new(
                "instructions",
                controls::body(lines.join("\n"))
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ));
        }
        items.push(KeyedView::new(
            "context",
            self.details_section("context", "Context", context_rows, context),
        ));

        // Heavy work: only when heavy episodes were recorded.
        let heavy: Vec<String> = read
            .map(monitor_view::observe_lines)
            .unwrap_or_default()
            .into_iter()
            .filter(|line| line.starts_with("Heavy work"))
            .collect();
        if !heavy.is_empty() {
            items.push(KeyedView::new(
                "heavy",
                self.details_section(
                    "heavy",
                    "Heavy work",
                    vec![KeyedView::new(
                        "lines",
                        controls::body(heavy.join("\n"))
                            .text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(true),
                    )],
                    context,
                ),
            ));
        }

        // Technical: ids, the generation, routing's skipped candidates,
        // coverage notes, with Copy.
        match (read, detail) {
            (Some(read), _) => {
                technical.push(format!("Generation {}", read.agent.agent.generation));
                if let Some(native) = &read.agent.agent.native_ref {
                    technical.push(format!("Native session {native}"));
                }
                technical.push(format!("Profile {}", read.agent.agent.profile));
            }
            (None, Some(detail)) => {
                technical.push(format!("Generation {}", detail.generation));
                technical.push(format!("Profile {}", detail.settings.profile));
            }
            (None, None) => {}
        }
        let text = technical.join("\n");
        items.push(KeyedView::new(
            "technical",
            self.details_section(
                "technical",
                "Technical",
                vec![
                    KeyedView::new(
                        "lines",
                        controls::body(text.clone())
                            .text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(true),
                    ),
                    KeyedView::new(
                        "copy",
                        controls::text_button()
                            .horizontal_alignment(HorizontalAlignment::Left)
                            .automation_name("Copy technical details")
                            .on_click(context.message(Message::Pane(
                                super::pane_ui::PaneMsg::Copy(
                                    "the technical details".to_owned(),
                                    text,
                                ),
                            )))
                            .content(controls::icon_label(controls::glyph::COPY, "Copy")),
                    ),
                ],
                context,
            ),
        ));

        Some(self.pane_body("Agent details", items))
    }
}

/// The Goal section's facts: its state, rounds and turns against the
/// budget, the turns without progress, cost, the check and its last
/// result, cadence and scope; facts it does not have are left out.
fn goal_lines(goal: &eliot_core::api::GoalView, now: i64) -> Vec<KeyedView> {
    use eliot_core::api::GoalStatus;
    let definition = &goal.definition;
    let counters = &goal.counters;
    let state = match goal.status {
        GoalStatus::Active => "Active".to_owned(),
        GoalStatus::Paused => match goal.paused_reason.as_deref() {
            Some(reason) => format!("Paused · {reason}"),
            None => "Paused".to_owned(),
        },
        GoalStatus::Blocked => match goal.blocked_reason.as_deref() {
            Some(reason) => format!("Blocked · {}", eliot_core::reason_words(reason)),
            None => "Blocked".to_owned(),
        },
        GoalStatus::Completed => "Met".to_owned(),
        GoalStatus::Cleared => "Cleared".to_owned(),
    };
    let mut rows = vec![KeyedView::new("state", page_ui::kv_text("State", state))];
    rows.push(KeyedView::new(
        "rounds",
        page_ui::kv_text(
            "Rounds",
            match definition.max_rounds {
                Some(max) => format!("{} of {max}", counters.rounds),
                None => counters.rounds.to_string(),
            },
        ),
    ));
    let turns = counters.turns_used;
    rows.push(KeyedView::new(
        "turns",
        page_ui::kv_text(
            "Turns",
            match definition.max_turns {
                Some(max) => format!("{turns} of {max}"),
                None => turns.to_string(),
            },
        ),
    ));
    if let Some(max) = definition.max_without_progress {
        rows.push(KeyedView::new(
            "progress",
            page_ui::kv_text(
                "Same result",
                format!("{} of {max} rounds", counters.consecutive_same_output),
            ),
        ));
    }
    if !definition.check.is_empty() {
        rows.push(KeyedView::new(
            "check",
            page_ui::kv_text("Check", definition.check.join(" ")),
        ));
    }
    if let Some(code) = counters.last_exit_code {
        let at = counters.last_check_at.map_or_else(String::new, |at| {
            format!(" · {} ago", monitor::elapsed_label(now.saturating_sub(at)))
        });
        rows.push(KeyedView::new(
            "last",
            page_ui::kv_text(
                "Last result",
                if code == 0 {
                    format!("Passed{at}")
                } else {
                    format!("Failed · exit {code}{at}")
                },
            ),
        ));
    }
    if goal.cost_status != "none" && !goal.cost_status.is_empty() {
        rows.push(KeyedView::new(
            "cost",
            page_ui::kv_text("Cost", goal.cost_status.replace('_', " ")),
        ));
    }
    if let Some(cadence) = definition.cadence_ms {
        rows.push(KeyedView::new(
            "cadence",
            page_ui::kv_text(
                "Cadence",
                format!("every {}", monitor::elapsed_label(cadence)),
            ),
        ));
    }
    if !definition.scope.is_empty() {
        rows.push(KeyedView::new(
            "scope",
            page_ui::kv_text("Scope", definition.scope.join(", ")),
        ));
    }
    rows
}

/// An Agent state code in words.
fn state_label(state: eliot_core::AgentState) -> &'static str {
    match state {
        eliot_core::AgentState::Created | eliot_core::AgentState::Starting => "Starting",
        eliot_core::AgentState::Idle => "Ready",
        eliot_core::AgentState::Busy => "Working",
        eliot_core::AgentState::Waiting => "Waiting for you",
        eliot_core::AgentState::Stopped => "Stopped",
        eliot_core::AgentState::Failed => "Failed",
        eliot_core::AgentState::Lost => "Lost",
        eliot_core::AgentState::Unknown => "State couldn't be confirmed",
    }
}

/// A monitor row as the menus, dialogs and conversation take it (the
/// sidebar's row when it is not listed there).
pub(super) fn row_agent(row: &MonitorRow) -> UiAgent {
    UiAgent {
        id: row.id.clone(),
        generation: row.generation,
        harness: row.harness.clone(),
        state: row.state,
        status_line: row.status_line.clone(),
        project_id: row.project.clone(),
        project_scope: row.project_scope.clone(),
        repo_common_dir: None,
        workspace: None,
        parent: row.parent.clone(),
        updated_at: 0,
        title: row.title.clone(),
        native_child: row.activity.is_some(),
        tokens: row.tokens,
        chat: row.chat,
        archived: false,
        pending: false,
        organization: None,
        tags: Vec::new(),
        organization_revision: None,
        model: row.model.clone(),
        work: row.work.clone().map(Box::new),
    }
}
