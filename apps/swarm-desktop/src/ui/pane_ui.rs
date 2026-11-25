//! The right pane (skeleton 3 and 5.4-5.6; `Decision.dc.html` for Changes,
//! `Finished.dc.html` for Details): one tab strip (Changes, Agents,
//! Details, Terminal) with the Wide and Full view toggles (component 55)
//! over the open tab, inside the main card.
//!
//! Reads (the Owner watches daemon load): a tab reads only while the pane
//! shows it, and nothing reads on render. Changes reads `agent.git` and
//! `agent.patch_digest` when it opens, when the open task changes and on
//! Refresh; the Terminal tab reads `agent.processes` the same way. Agents
//! and Details ride the Monitor's refresh chain, which follows
//! `events.wait` on their Agents' fleet events as the Agents page does.
//! Every read waits while the window is minimized.
//!
//! The daemon has no diff-text read (skeleton 9.2): Changes shows the
//! digest and the files, and says why the diff is not there.

use std::collections::HashSet;

use eliot_core::api::{
    AgentGitResponse, AgentPatchDigestResponse, AgentProcessesResponse, GitIntegrateOutcome,
    GitIntegrateStatus, GitView,
};

use super::*;
use crate::monitor::MonitorRow;
use crate::theme::winui::Palette;

/// What Changes compares (`.group` Compare).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum Source {
    /// The worktree against its HEAD.
    Uncommitted,
    /// The task's branch and worktree against the task's base.
    #[default]
    Branch,
    /// The files the last finished turn reported editing.
    LastTurn,
}

impl Source {
    const ALL: [Self; 3] = [Self::Uncommitted, Self::Branch, Self::LastTurn];

    fn label(self) -> &'static str {
        match self {
            Self::Uncommitted => "Uncommitted",
            Self::Branch => "Branch",
            Self::LastTurn => "Last turn",
        }
    }
}

/// One group of the review digest (`agent.patch_digest`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Digest {
    Packages,
    Dependencies,
    Interface,
    Checkers,
    Suppressions,
    OutsideClaims,
}

impl Digest {
    const ALL: [Self; 6] = [
        Self::Packages,
        Self::Dependencies,
        Self::Interface,
        Self::Checkers,
        Self::Suppressions,
        Self::OutsideClaims,
    ];

    fn words(self, count: usize) -> String {
        let (one, many) = match self {
            Self::Packages => ("changed package", "changed packages"),
            Self::Dependencies => ("new dependency", "new dependencies"),
            Self::Interface => ("public-interface line", "public-interface lines"),
            Self::Checkers => ("check or lint setting", "check or lint settings"),
            Self::Suppressions => ("suppression", "suppressions"),
            Self::OutsideClaims => ("path outside claims", "paths outside claims"),
        };
        eliot_core::count_words(count as u64, one, many)
    }

    fn entries(self, digest: &AgentPatchDigestResponse) -> &[String] {
        match self {
            Self::Packages => &digest.packages,
            Self::Dependencies => &digest.new_dependencies,
            Self::Interface => &digest.public_interface,
            Self::Checkers => &digest.checker_changes,
            Self::Suppressions => &digest.suppressions,
            // No declared claim scope: nothing to compare with, so no group.
            Self::OutsideClaims if digest.outside_claims_declared != Some(true) => &[],
            Self::OutsideClaims => &digest.outside_claims,
        }
    }
}

/// One Changes read: the Git snapshot and the review digest, each with its
/// own failure.
#[derive(Clone, Debug)]
pub(super) struct ChangesRead {
    git: Result<AgentGitResponse, String>,
    digest: Result<AgentPatchDigestResponse, String>,
    at_ms: i64,
}

/// Merge and check (`git.integrate`), bound to the Agent it was asked for.
#[derive(Clone, Debug, Default)]
enum Merge {
    #[default]
    Idle,
    Confirm(String),
    Running(String),
    Done(String, Box<Result<eliot_core::Command, String>>),
}

#[derive(Default)]
pub(super) struct ChangesState {
    source: Source,
    /// The Agent the shown read is of.
    agent: Option<String>,
    epoch: u64,
    task: Option<ComponentTask>,
    reading: bool,
    read: Option<Box<ChangesRead>>,
    filter: String,
    selected: Option<String>,
    open: Option<Digest>,
    merge: Merge,
}

#[derive(Default)]
pub(super) struct TerminalState {
    agent: Option<String>,
    epoch: u64,
    task: Option<ComponentTask>,
    reading: bool,
    read: Option<Result<AgentProcessesResponse, String>>,
    /// Why Open in Windows Terminal did not start.
    note: Option<String>,
}

/// Details sections closed until the owner opens them.
const CLOSED_AT_FIRST: [&str; 2] = ["task", "technical"];

/// The pane's width, its tabs' reads and the owner's choices in it.
#[derive(Default)]
pub(super) struct PaneState {
    /// Wide (`size-reading`) instead of `size-pane`.
    pub(super) wide: bool,
    /// Full view: the open tab over the whole main card.
    pub(super) full: bool,
    changes: ChangesState,
    terminal: TerminalState,
    /// Agents tab rows whose workers are folded away.
    folded: HashSet<String>,
    /// Details sections whose open state the owner flipped.
    flipped: HashSet<&'static str>,
    /// A Goal control is in flight.
    goal_busy: bool,
    goal_note: Option<String>,
}

impl PaneState {
    pub(super) fn section_open(&self, key: &'static str) -> bool {
        CLOSED_AT_FIRST.contains(&key) == self.flipped.contains(key)
    }
}

#[derive(Clone, Debug)]
pub(super) enum PaneMsg {
    /// A tab of the strip.
    Tab(Panel),
    /// The Wide toggle.
    Wide,
    /// The Full view toggle.
    Full,
    /// The open tab's Refresh (and Retry).
    Refresh,
    /// A Changes read landed (its epoch), or was dropped (`None`).
    ChangesLoaded(u64, Option<Box<ChangesRead>>),
    Source(Source),
    Filter(String),
    File(String),
    DigestGroup(Digest),
    OpenFile(String),
    CopyPath(String),
    /// The Edited files card's View changes: Changes on Last turn.
    ViewChanges,
    MergeAsked,
    MergeAnswered(bool),
    MergeSettled(String, Box<Result<eliot_core::Command, String>>),
    MergeDismissed,
    /// An Agents row: open its conversation.
    TreeOpen(String),
    TreeFold(String),
    TreeStop(String, i64),
    TreeHandoff(String, i64),
    ProcessesLoaded(u64, Option<Box<Result<AgentProcessesResponse, String>>>),
    OpenTerminal,
    /// Open or close a Details section.
    Section(&'static str),
    Goal(client::GoalControl),
    GoalDone(
        String,
        client::GoalControl,
        Box<MutationResult<eliot_core::api::GoalView>>,
    ),
    /// Copy text (what it is, the text).
    Copy(String, String),
}

impl DesktopApp {
    pub(super) fn pane_update(&mut self, msg: PaneMsg, context: &ComponentContext<Self>) {
        match msg {
            PaneMsg::Tab(panel) => self.choose_panel(panel, context),
            PaneMsg::Wide => self.pane.wide = !self.pane.wide,
            PaneMsg::Full => self.pane.full = !self.pane.full,
            PaneMsg::Refresh => match self.panel {
                Panel::Changes => self.read_changes(context),
                Panel::Terminal => self.read_processes(context),
                Panel::Details => {
                    self.monitor.detail_due = true;
                    self.read_monitor(0, context);
                }
                Panel::Agents => self.read_monitor(0, context),
                Panel::Closed => {}
            },
            PaneMsg::ChangesLoaded(epoch, read) => {
                let changes = &mut self.pane.changes;
                if epoch != changes.epoch {
                    return;
                }
                changes.reading = false;
                changes.task = None;
                if let Some(read) = read {
                    changes.read = Some(read);
                }
            }
            PaneMsg::Source(source) => self.pane.changes.source = source,
            PaneMsg::Filter(text) => {
                // A rebuilt field reports the text it already shows.
                if self.pane.changes.filter != text {
                    self.pane.changes.filter = text;
                }
            }
            PaneMsg::File(path) => {
                let changes = &mut self.pane.changes;
                changes.selected = if changes.selected.as_deref() == Some(&path) {
                    None
                } else {
                    Some(path)
                };
            }
            PaneMsg::DigestGroup(group) => {
                let changes = &mut self.pane.changes;
                changes.open = if changes.open == Some(group) {
                    None
                } else {
                    Some(group)
                };
            }
            PaneMsg::OpenFile(path) => match self.changes_file(&path) {
                Some(full) => {
                    if let Err(error) = super::reveal::open_with_default_app(&full) {
                        self.alert(error);
                    }
                }
                None => self.alert("This task has no worktree to open the file from."),
            },
            PaneMsg::CopyPath(path) => match crate::clipboard::set_text(&path) {
                Ok(()) => self.set_status(format!("Copied {path}.")),
                Err(error) => self.alert(format!("Copy failed: {error}")),
            },
            PaneMsg::ViewChanges => {
                self.pane.changes.source = Source::LastTurn;
                if self.panel == Panel::Changes && self.panel_shown() {
                    return;
                }
                self.choose_panel(Panel::Changes, context);
            }
            PaneMsg::MergeAsked => {
                if let Some(agent) = self.selected_agent_id() {
                    self.pane.changes.merge = Merge::Confirm(agent);
                }
            }
            PaneMsg::MergeAnswered(false) | PaneMsg::MergeDismissed => {
                self.pane.changes.merge = Merge::Idle;
            }
            PaneMsg::MergeAnswered(true) => self.merge_and_check(context),
            PaneMsg::MergeSettled(agent, result) => {
                if matches!(&self.pane.changes.merge, Merge::Running(running) if *running == agent)
                {
                    self.pane.changes.merge = Merge::Done(agent, result);
                }
            }
            PaneMsg::TreeOpen(id) => {
                if let Some(agent) = self.tree_agent(&id) {
                    self.shell_update(ShellMsg::AgentOpened(agent), context);
                }
            }
            PaneMsg::TreeFold(id) => {
                if !self.pane.folded.remove(&id) {
                    self.pane.folded.insert(id);
                }
            }
            PaneMsg::TreeStop(id, generation) => {
                // The same question as the header's and the sidebar's.
                if let Some(mut agent) = self.tree_agent(&id) {
                    agent.generation = generation;
                    self.menu.confirm = Some((
                        super::menu_ui::Target::Agent(agent),
                        super::menu_ui::Action::Stop,
                    ));
                }
            }
            PaneMsg::TreeHandoff(agent, generation) => {
                self.handoff_update(handoff_ui::HandoffMsg::Open { agent, generation }, context)
            }
            PaneMsg::ProcessesLoaded(epoch, read) => {
                let terminal = &mut self.pane.terminal;
                if epoch != terminal.epoch {
                    return;
                }
                terminal.reading = false;
                terminal.task = None;
                if let Some(read) = read {
                    terminal.read = Some(*read);
                }
            }
            PaneMsg::OpenTerminal => {
                let folder = self.open_workspace();
                self.pane.terminal.note = match folder {
                    Some(folder) => super::reveal::open_terminal(&folder).err(),
                    None => Some("This task has no worktree yet.".to_owned()),
                };
            }
            PaneMsg::Section(key) => {
                if !self.pane.flipped.remove(key) {
                    self.pane.flipped.insert(key);
                }
            }
            PaneMsg::Goal(control) => self.goal_control(control, context),
            PaneMsg::GoalDone(agent, control, result) => {
                self.pane.goal_busy = false;
                match *result {
                    Ok(goal) => {
                        self.pane.goal_note = None;
                        let goal =
                            (goal.status != eliot_core::api::GoalStatus::Cleared).then_some(goal);
                        if self.selected_agent_id().as_deref() == Some(agent.as_str()) {
                            self.chat.goal = Some((agent, goal));
                        }
                    }
                    Err(error) => {
                        self.pane.goal_note = Some(format!(
                            "{} did not go through: {}",
                            control.label(),
                            error.user_message()
                        ));
                    }
                }
            }
            PaneMsg::Copy(what, text) => match crate::clipboard::set_text(&text) {
                Ok(()) => self.set_status(format!("Copied {what}.")),
                Err(error) => self.alert(format!("Copy failed: {error}")),
            },
        }
    }

    /// Open a Changes, Agents or Terminal tab (Details: [`Self::show_details`]):
    /// each reads when it opens.
    pub(super) fn open_tab(&mut self, panel: Panel, context: &ComponentContext<Self>) {
        if self.selected_agent_id().is_none() {
            return;
        }
        self.panel = panel;
        match panel {
            Panel::Changes => self.read_changes(context),
            Panel::Terminal => self.read_processes(context),
            Panel::Agents => self.read_monitor(0, context),
            Panel::Details | Panel::Closed => {}
        }
    }

    /// The open task changed while the pane shows: its open tab reads the
    /// new task.
    pub(super) fn pane_selected(&mut self, agent: &str, context: &ComponentContext<Self>) {
        match self.panel {
            Panel::Details => self.show_details(agent, context),
            Panel::Agents => self.read_monitor(0, context),
            Panel::Changes => self.read_changes(context),
            Panel::Terminal => self.read_processes(context),
            Panel::Closed => {}
        }
    }

    /// The daemon answers again: the shown tab that reads on its own reads.
    pub(super) fn pane_reread(&mut self, context: &ComponentContext<Self>) {
        if !self.panel_shown() {
            return;
        }
        match self.panel {
            Panel::Changes => self.read_changes(context),
            Panel::Terminal => self.read_processes(context),
            _ => {}
        }
    }

    /// The open conversation's readback landed: the Agents tab follows its
    /// hierarchy root once it is known (a worker's tree is its task's).
    pub(super) fn pane_detail_landed(&mut self, context: &ComponentContext<Self>) {
        if self.panel == Panel::Agents
            && self.panel_shown()
            && self.monitor.data.tree.root != self.tree_root()
        {
            self.read_monitor(0, context);
        }
    }

    /// After every update: a tab that no longer shows stops its read.
    pub(super) fn sync_pane(&mut self) {
        let shown = self.panel_shown();
        if !(shown && self.panel == Panel::Changes)
            && let Some(task) = self.pane.changes.task.take()
        {
            task.cancel();
            self.pane.changes.epoch += 1;
            self.pane.changes.reading = false;
        }
        if !(shown && self.panel == Panel::Terminal)
            && let Some(task) = self.pane.terminal.task.take()
        {
            task.cancel();
            self.pane.terminal.epoch += 1;
            self.pane.terminal.reading = false;
        }
    }

    /// The hierarchy root whose tree the Agents tab shows: the open
    /// Agent's frozen root once its readback is in, else the open Agent.
    pub(super) fn tree_root(&self) -> Option<String> {
        let open = self.selected_agent_id()?;
        Some(
            self.chat
                .detail
                .as_ref()
                .filter(|detail| detail.agent == open && !detail.root.is_empty())
                .map_or(open, |detail| detail.root.clone()),
        )
    }

    /// The open Agent's worktree (its readback, else its sidebar row).
    fn open_workspace(&self) -> Option<String> {
        let open = self.selected_agent_id()?;
        self.chat
            .detail
            .as_ref()
            .filter(|detail| detail.agent == open)
            .and_then(|detail| detail.workspace.clone())
            .or_else(|| self.find_agent(&open).and_then(|row| row.workspace.clone()))
            .filter(|workspace| !workspace.is_empty())
    }

    /// A changed file's path on disk: the read's worktree joined with the
    /// repository-relative path.
    fn changes_file(&self, path: &str) -> Option<PathBuf> {
        let root = self
            .pane
            .changes
            .read
            .as_ref()
            .and_then(|read| match &read.git {
                Ok(reply) => match &reply.git {
                    GitView::Data(data) => Some(data.workspace.clone()),
                    _ => None,
                },
                Err(_) => None,
            })
            .or_else(|| self.open_workspace())?;
        Some(Path::new(&root).join(path.replace('/', "\\")))
    }

    /// The row of `id` as the menus and dialogs take it: the sidebar's, else
    /// built from the Agents tab's read.
    fn tree_agent(&self, id: &str) -> Option<UiAgent> {
        self.find_agent(id).cloned().or_else(|| {
            self.monitor
                .data
                .tree
                .rows
                .iter()
                .find(|row| row.id == id)
                .map(super::monitor_ui::row_agent)
        })
    }

    /// Read the open task's Git snapshot and review digest (together, on a
    /// worker), keeping the last read shown until it lands.
    fn read_changes(&mut self, context: &ComponentContext<Self>) {
        let (Some(agent), Ok(data_dir)) = (self.selected_agent_id(), self.data_dir.clone()) else {
            return;
        };
        if self.daemon_down() {
            return;
        }
        let changes = &mut self.pane.changes;
        if let Some(task) = changes.task.take() {
            task.cancel();
        }
        if changes.agent.as_deref() != Some(agent.as_str()) {
            changes.agent = Some(agent.clone());
            changes.read = None;
            changes.selected = None;
            changes.open = None;
            changes.merge = Merge::Idle;
        }
        changes.epoch += 1;
        changes.reading = true;
        let epoch = changes.epoch;
        changes.task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if !wait_shown(&cancelled) {
                return Message::Pane(PaneMsg::ChangesLoaded(epoch, None));
            }
            let (git, digest) = client::block_on(futures_util::future::join(
                client::agent_git(&data_dir, &agent),
                client::patch_digest(&data_dir, &agent),
            ));
            Message::Pane(PaneMsg::ChangesLoaded(
                epoch,
                Some(Box::new(ChangesRead {
                    git: git.map_err(|error| error.user_message()),
                    digest: digest.map_err(|error| error.user_message()),
                    at_ms: crate::handoff::now_ms(),
                })),
            ))
        }));
    }

    /// Read the open Agent's process records once (the Terminal tab).
    fn read_processes(&mut self, context: &ComponentContext<Self>) {
        let (Some(agent), Ok(data_dir)) = (self.selected_agent_id(), self.data_dir.clone()) else {
            return;
        };
        if self.daemon_down() {
            return;
        }
        let terminal = &mut self.pane.terminal;
        if let Some(task) = terminal.task.take() {
            task.cancel();
        }
        if terminal.agent.as_deref() != Some(agent.as_str()) {
            terminal.agent = Some(agent.clone());
            terminal.read = None;
            terminal.note = None;
        }
        terminal.epoch += 1;
        terminal.reading = true;
        let epoch = terminal.epoch;
        terminal.task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if !wait_shown(&cancelled) {
                return Message::Pane(PaneMsg::ProcessesLoaded(epoch, None));
            }
            let read = client::block_on(client::agent_processes(&data_dir, &agent))
                .map_err(|error| error.user_message());
            Message::Pane(PaneMsg::ProcessesLoaded(epoch, Some(Box::new(read))))
        }));
    }

    /// What Merge and check sends: the task's registered project, its base
    /// branch and its branch; `Err` says why it cannot be sent.
    fn merge_request(&self) -> Result<eliot_core::api::GitIntegrateDefinitionArgs, String> {
        let open = self.selected_agent_id().ok_or("No task is open.")?;
        let detail = self
            .chat
            .detail
            .as_ref()
            .filter(|detail| detail.agent == open)
            .ok_or("The task is still being read.")?;
        if !self
            .nav
            .projects
            .iter()
            .any(|project| project.id == detail.project)
        {
            return Err("Merge and check needs a registered project.".to_owned());
        }
        let branch = detail
            .branch
            .clone()
            .ok_or("Merge and check needs the task's own branch.")?;
        let base = detail
            .base_ref
            .clone()
            .ok_or("Merge and check needs the branch the task started from.")?;
        Ok(eliot_core::api::GitIntegrateDefinitionArgs {
            repository: detail.project.clone(),
            base_branch: base,
            branches: vec![branch],
            fast_forward_to: None,
            timeout: None,
        })
    }

    /// Submit `git.integrate` and wait for its retained result (bounded,
    /// cancellable; the daemon keeps the outcome either way).
    fn merge_and_check(&mut self, context: &ComponentContext<Self>) {
        let Merge::Confirm(agent) = self.pane.changes.merge.clone() else {
            return;
        };
        let (Ok(definition), Ok(data_dir)) = (self.merge_request(), self.data_dir.clone()) else {
            self.pane.changes.merge = Merge::Idle;
            return;
        };
        self.pane.changes.merge = Merge::Running(agent.clone());
        let mutation_id = client::new_mutation_id();
        let _task = context.spawn_background(move |cancel| {
            let result = client::block_on(client::submit_integrate(
                &data_dir,
                mutation_id,
                &definition,
            ))
            .map_err(|error| error.user_message())
            .and_then(|reply| {
                super::settle_command(&data_dir, &reply.command_id, 15 * 60, &|| {
                    cancel.is_cancelled()
                })
            });
            Message::Pane(PaneMsg::MergeSettled(agent, Box::new(result)))
        });
    }

    /// `goal.pause`, `goal.resume` or `goal.clear`, one at a time.
    fn goal_control(&mut self, control: client::GoalControl, context: &ComponentContext<Self>) {
        let (Some(agent), Ok(data_dir)) = (self.selected_agent_id(), self.data_dir.clone()) else {
            return;
        };
        if self.pane.goal_busy {
            return;
        }
        self.pane.goal_busy = true;
        self.pane.goal_note = None;
        let mutation_id = client::new_mutation_id();
        let _task = context.spawn_background(move |_| {
            let result = client::block_on(client::goal_control(
                &data_dir,
                mutation_id,
                &agent,
                control,
            ));
            Message::Pane(PaneMsg::GoalDone(agent, control, Box::new(result)))
        });
    }

    /// The pane: its tab strip over the open tab.
    pub(super) fn pane_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let body: View = match self.panel {
            Panel::Changes => self.changes_view(context, palette),
            Panel::Agents => self.tree_view(context, palette),
            Panel::Details => self.details_view(context, palette),
            Panel::Terminal => self.terminal_view(context, palette),
            Panel::Closed => View::empty(),
        };
        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((
                self.tab_strip(context, palette),
                Border::new().grid_row(1).content(body),
            ))
    }

    /// The strip (`.tabs`): the four tabs, then Wide and Full view, over an
    /// `outline-variant` hairline.
    fn tab_strip(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let mut cells: Vec<KeyedView> = Vec::new();
        let mut columns = Vec::new();
        for panel in Panel::TABS {
            let selected = self.panel == panel;
            let column = cells.len() as i32;
            columns.push(GridLength::Auto);
            let indicator: View = if selected {
                Border::new()
                    .height(tokens::layout::TAB_INDICATOR)
                    .margin(Thickness::xy(tokens::spacing::SM, 0.0))
                    .vertical_alignment(VerticalAlignment::Bottom)
                    .corner_radius(CornerRadius::new(
                        tokens::layout::TAB_INDICATOR,
                        tokens::layout::TAB_INDICATOR,
                        0.0,
                        0.0,
                    ))
                    .background(palette.primary())
                    .into()
            } else {
                View::empty()
            };
            cells.push(KeyedView::new(
                format!("tab:{}:{selected}", panel.label()),
                Grid::new().grid_column(column).children((
                    Button::new()
                        .resource_overrides(palette.tab(selected))
                        .height(tokens::height::CONTROL)
                        .min_height(tokens::height::CONTROL)
                        .automation_name(if selected {
                            format!("{} (shown)", panel.label())
                        } else {
                            panel.label().to_owned()
                        })
                        .on_click(context.message(Message::Pane(PaneMsg::Tab(panel))))
                        .content(
                            m3::text(panel.label(), tokens::text::BUTTON)
                                .margin(Thickness::xy(
                                    tokens::spacing::TEXT_INSET - tokens::spacing::ROW,
                                    0.0,
                                ))
                                .vertical_alignment(VerticalAlignment::Center),
                        ),
                    indicator,
                )),
            ));
        }
        columns.push(GridLength::STAR);
        columns.push(GridLength::Auto);
        columns.push(GridLength::Auto);
        let wide = self.pane.wide;
        let full = self.pane.full;
        let at = cells.len() as i32 + 1;
        cells.push(KeyedView::new(
            format!("wide:{wide}"),
            m3::icon_button(palette)
                .grid_column(at)
                .resource_overrides(palette.toggle(wide))
                .automation_name(if wide { "Wide pane (on)" } else { "Wide pane" })
                .on_click(context.message(Message::Pane(PaneMsg::Wide)))
                .content(m3::icon(m3::glyph::WIDTH))
                .tooltip("Wide pane"),
        ));
        cells.push(KeyedView::new(
            format!("full:{full}"),
            m3::icon_button(palette)
                .grid_column(at + 1)
                .resource_overrides(palette.toggle(full))
                .automation_name(if full { "Full view (on)" } else { "Full view" })
                .on_click(context.message(Message::Pane(PaneMsg::Full)))
                .content(m3::icon(m3::glyph::FULL_VIEW))
                .tooltip(if full { "Leave Full view" } else { "Full view" }),
        ));
        Border::new()
            .border_brush(palette.outline())
            .border_thickness(Thickness::new(0.0, 0.0, 0.0, tokens::layout::HAIRLINE))
            .padding(Thickness::xy(tokens::spacing::SM, 0.0))
            .content(
                Grid::new()
                    .columns(columns)
                    .column_spacing(tokens::spacing::ROW_GAP)
                    .keyed_children(cells),
            )
    }

    /// A tab's body (`.pane-r .body`): 12 DIPs in, its parts 12 apart, in
    /// a scroll view.
    pub(super) fn pane_body(&self, name: &str, items: Vec<KeyedView>) -> View {
        ScrollViewer::new()
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name(name.to_owned())
            .content(
                StackPanel::new()
                    .spacing(tokens::spacing::INSET)
                    .margin(Thickness::uniform(tokens::spacing::INSET))
                    .keyed_children(items),
            )
    }

    /// Changes (skeleton 5.4, `Decision.dc.html`): Compare and Refresh;
    /// the diff view and Filter files; the summary; the review digest; the
    /// files; the selected file's actions; Merge and check.
    fn changes_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let changes = &self.pane.changes;
        let open = self.selected_agent_id();
        let read = changes.read.as_deref().filter(|_| changes.agent == open);
        let mut items: Vec<KeyedView> = Vec::new();

        let compare: Vec<(&str, bool, Callback<()>)> = Source::ALL
            .iter()
            .map(|source| {
                (
                    source.label(),
                    changes.source == *source,
                    context.message(Message::Pane(PaneMsg::Source(*source))),
                )
            })
            .collect();
        let read_at = read
            .and_then(|read| monitor::last_read_label(read.at_ms))
            .map_or_else(|| "Refresh".to_owned(), |at| format!("Refresh ({at})"));
        items.push(KeyedView::new(
            "compare",
            toolbar(
                vec![KeyedView::new(
                    "group",
                    super::page_ui::choice(palette, &compare),
                )],
                vec![KeyedView::new(
                    "refresh",
                    m3::icon_button(palette)
                        .is_enabled(!changes.reading)
                        .automation_name("Refresh changes")
                        .on_click(context.message(Message::Pane(PaneMsg::Refresh)))
                        .content(m3::icon(m3::glyph::REFRESH))
                        .tooltip(read_at),
                )],
            ),
        ));
        // The diff controls govern the diff, which the daemon cannot give
        // yet: shown, unavailable, with the reason.
        let no_diff = "Swarm cannot show diffs yet: the daemon has no diff-text read";
        let view_group: Vec<KeyedView> = [("Unified", true), ("Split", false)]
            .into_iter()
            .map(|(label, selected)| {
                KeyedView::new(
                    label,
                    m3::segment(palette, selected)
                        .is_enabled(false)
                        .automation_name(format!("{label}: {no_diff}"))
                        .content(m3::segment_label(label)),
                )
            })
            .collect();
        let filter = TextBox::new()
            .text(changes.filter.clone())
            .placeholder_text("Filter files")
            .automation_name("Filter files")
            .on_text_changed(context.callback(|text| Message::Pane(PaneMsg::Filter(text))));
        items.push(KeyedView::new(
            "view",
            toolbar(
                vec![
                    KeyedView::new(
                        "group",
                        Border::new()
                            .content(m3::segmented(palette, view_group))
                            .tooltip(no_diff),
                    ),
                    KeyedView::new(
                        "wrap",
                        Border::new()
                            .content(
                                m3::icon_button(palette)
                                    .is_enabled(false)
                                    .automation_name(format!("Word wrap: {no_diff}"))
                                    .content(m3::icon(m3::glyph::WRAP)),
                            )
                            .tooltip(no_diff),
                    ),
                ],
                vec![KeyedView::new(
                    "filter",
                    Border::new()
                        .width(tokens::layout::FILTER_WIDTH)
                        .height(tokens::height::CONTROL)
                        .corner_radius(tokens::radius::FIELD)
                        .border_brush(palette.border())
                        .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
                        .padding(Thickness::new(tokens::spacing::ROW, 0.0, 0.0, 0.0))
                        .content(
                            Grid::new()
                                .columns([GridLength::Auto, GridLength::STAR])
                                .children((
                                    Border::new()
                                        .vertical_alignment(VerticalAlignment::Center)
                                        .content(m3::small_icon(m3::glyph::FILTER)),
                                    filter
                                        .grid_column(1)
                                        .background(Brush::Solid(Color::transparent()))
                                        .border_thickness(Thickness::uniform(0.0))
                                        .vertical_alignment(VerticalAlignment::Center),
                                )),
                        ),
                )],
            ),
        ));

        let Some(read) = read else {
            items.push(KeyedView::new(
                "state",
                super::page_ui::quiet(if changes.reading {
                    "Changes are not read yet."
                } else {
                    "Changes are not read yet. Refresh reads them."
                }),
            ));
            return self.pane_body("Changes", items);
        };
        let git = read.git.as_ref().ok().and_then(|reply| match &reply.git {
            GitView::Data(data) => Some(data.as_ref()),
            _ => None,
        });
        let git_error = match &read.git {
            Err(error) => Some(error.clone()),
            Ok(reply) => match &reply.git {
                GitView::Data(_) => None,
                GitView::Unavailable { error_code, .. } => {
                    Some(eliot_core::reason_words(error_code))
                }
                GitView::Error { error, .. } => Some(error.clone()),
            },
        };
        let digest = read.digest.as_ref().ok();
        if git.is_none() && digest.is_none() {
            let why = [git_error.clone(), read.digest.as_ref().err().cloned()]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join(" · ");
            items.push(KeyedView::new(
                "failed",
                super::page_ui::read_failed(
                    palette,
                    format!("Couldn't read changes: {why}"),
                    "Retry",
                    !changes.reading,
                    context.message(Message::Pane(PaneMsg::Refresh)),
                ),
            ));
            return self.pane_body("Changes", items);
        }

        // The files of the chosen source, with their state words.
        let files = self.source_files(changes.source, git, digest);
        let partial = match changes.source {
            Source::Uncommitted => git.is_none_or(|git| git.complete != Some(true)),
            Source::Branch => digest.is_none_or(|digest| !digest.complete || digest.truncated),
            Source::LastTurn => self.last_turn_edits().is_some_and(|(_, unnamed)| unnamed),
        };
        let work = self.open_work();
        let ahead = work.and_then(|work| work.commits_ahead);
        let base = self
            .chat
            .detail
            .as_ref()
            .filter(|detail| Some(&detail.agent) == open.as_ref())
            .and_then(|detail| detail.base_ref.clone());
        items.push(KeyedView::new(
            format!("summary:{:?}", changes.source),
            summary_line(
                palette,
                changes.source,
                files.as_ref().ok().map(Vec::len),
                git.and_then(|git| git.line_counts.as_ref())
                    .map(|counts| (counts.insertions, counts.deletions))
                    .or_else(|| work.and_then(|work| work.insertions.zip(work.deletions))),
                ahead,
                base.as_deref(),
                partial,
            ),
        ));
        if let Some(error) = &git_error
            && changes.source == Source::Uncommitted
        {
            items.push(KeyedView::new(
                "git-error",
                super::page_ui::error_line(palette, format!("Git status not read: {error}")),
            ));
        }
        if let Err(error) = &read.digest {
            items.push(KeyedView::new(
                "digest-error",
                super::page_ui::error_line(palette, format!("Review digest not read: {error}")),
            ));
        }
        if let Some(digest) = digest {
            items.push(KeyedView::new(
                "digest",
                self.digest_card(digest, context, palette),
            ));
        }

        match files {
            Err(why) => items.push(KeyedView::new("files-why", super::page_ui::quiet(why))),
            Ok(files) if files.is_empty() => items.push(KeyedView::new(
                "empty",
                super::page_ui::quiet(match changes.source {
                    Source::LastTurn if partial => {
                        "The last turn's edits did not name their files: Branch and Uncommitted list them."
                    }
                    Source::LastTurn => "The last turn reported no file edits.",
                    _ => "No changes yet.",
                }),
            )),
            Ok(files) => {
                let needle = changes.filter.trim().to_lowercase();
                let shown: Vec<&(String, &'static str)> = files
                    .iter()
                    .filter(|(path, _)| needle.is_empty() || path.to_lowercase().contains(&needle))
                    .collect();
                let mut rows: Vec<KeyedView> = Vec::new();
                for (path, state) in &shown {
                    let selected = changes.selected.as_deref() == Some(path.as_str());
                    rows.push(KeyedView::new(
                        format!("file:{path}:{selected}"),
                        file_row(palette, path, state, selected, context),
                    ));
                }
                if shown.is_empty() {
                    rows.push(KeyedView::new(
                        "none",
                        super::page_ui::quiet(format!("No file matches \u{201c}{needle}\u{201d}.")),
                    ));
                }
                items.push(KeyedView::new(
                    "files",
                    StackPanel::new()
                        .spacing(tokens::spacing::ROW_GAP)
                        .automation_name("Changed files")
                        .keyed_children(rows),
                ));
                if let Some(path) = changes
                    .selected
                    .as_ref()
                    .filter(|path| files.iter().any(|(file, _)| file == *path))
                {
                    items.push(KeyedView::new(
                        format!("selected:{path}"),
                        selected_file(palette, path, context),
                    ));
                }
            }
        }
        items.push(KeyedView::new("merge", self.merge_view(context, palette)));
        self.pane_body("Changes", items)
    }

    /// The open Agent's Git work summary (its sidebar row, else the
    /// Monitor's read of it).
    fn open_work(&self) -> Option<&eliot_core::api::WorkSummaryView> {
        let open = self.selected_agent_id()?;
        self.find_agent(&open)
            .and_then(|row| row.work.as_deref())
            .or_else(|| {
                self.monitor
                    .data
                    .detail
                    .as_ref()
                    .filter(|detail| detail.read.agent.agent.id == open)
                    .and_then(|detail| detail.read.agent.work.as_ref())
            })
    }

    /// The files `source` lists with their state words, or why it lists
    /// none.
    fn source_files(
        &self,
        source: Source,
        git: Option<&eliot_core::api::GitObservation>,
        digest: Option<&AgentPatchDigestResponse>,
    ) -> Result<Vec<(String, &'static str)>, String> {
        match source {
            Source::Uncommitted => {
                let git = git.ok_or("Git status was not read.")?;
                let dirty = git
                    .dirty
                    .as_ref()
                    .ok_or("Git status did not list the worktree's files.")?;
                Ok(dirty
                    .iter()
                    .map(|entry| (entry.path.clone(), porcelain_word(&entry.status)))
                    .collect())
            }
            Source::Branch => {
                let digest = digest.ok_or("The review digest was not read.")?;
                Ok(digest
                    .files
                    .iter()
                    .map(|file| (file.path.clone(), change_word(&file.change)))
                    .collect())
            }
            Source::LastTurn => {
                let (paths, _) = self
                    .last_turn_edits()
                    .ok_or("No turn has finished in this conversation yet.")?;
                let root = git
                    .map(|git| git.workspace.clone())
                    .or_else(|| self.open_workspace())
                    .unwrap_or_default();
                Ok(paths
                    .into_iter()
                    .map(|path| {
                        let relative = relative_path(&root, &path);
                        let word = digest
                            .and_then(|digest| {
                                digest.files.iter().find(|file| file.path == relative)
                            })
                            .map_or("Edited", |file| change_word(&file.change));
                        (relative, word)
                    })
                    .collect())
            }
        }
    }

    /// The files the last finished turn's steps reported editing (the
    /// transcript's Edited files), or `None` before any turn finished.
    /// Whether some of them did not name their files: the work log then
    /// says "made N edits" instead of "edited N files" (only some
    /// harnesses report changed paths).
    fn last_turn_edits(&self) -> Option<(Vec<String>, bool)> {
        use crate::chat::PartKind;
        self.chat.items.iter().rev().find_map(|item| {
            let finished = item
                .parts
                .iter()
                .any(|part| matches!(part.kind, PartKind::Final | PartKind::TurnFailed));
            finished.then(|| {
                let paths = item
                    .parts
                    .iter()
                    .find(|part| matches!(part.kind, PartKind::Edited))
                    .map(|part| part.detail.clone())
                    .unwrap_or_default();
                let unnamed = item.parts.iter().any(|part| {
                    matches!(part.kind, PartKind::Log)
                        && part.label.contains("made ")
                        && part.label.contains(" edit")
                });
                (paths, unnamed)
            })
        })
    }

    /// Review digest (component 63): one line of counts; a count opens its
    /// paths. "What to look at first", not a quality score.
    fn digest_card(
        &self,
        digest: &AgentPatchDigestResponse,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let open = self.pane.changes.open;
        let mut chips: Vec<KeyedView> = Vec::new();
        for group in Digest::ALL {
            let count = group.entries(digest).len();
            if count == 0 {
                continue;
            }
            let selected = open == Some(group);
            let words = group.words(count);
            chips.push(KeyedView::new(
                format!("{group:?}:{selected}"),
                m3::chip(palette)
                    .resource_overrides(super::page_ui::selected_chip(palette, selected))
                    .automation_name(if selected {
                        format!("{words} (paths shown)")
                    } else {
                        format!("{words}: show the paths")
                    })
                    .on_click(context.message(Message::Pane(PaneMsg::DigestGroup(group))))
                    .content(m3::chip_content(
                        selected.then_some(m3::glyph::CHECK),
                        &words,
                        false,
                        tokens::layout::PROJECT_CHIP_WIDTH,
                    )),
            ));
        }
        let mut parts: Vec<KeyedView> = vec![KeyedView::new(
            "head",
            m3::text("Review digest", tokens::text::SMALL_TITLE)
                .automation_heading_level(AutomationHeadingLevel::Level3),
        )];
        if chips.is_empty() {
            parts.push(KeyedView::new(
                "none",
                m3::secondary("Nothing stands out in this patch."),
            ));
        } else {
            let per_row = if self.pane.wide || self.pane.full {
                4
            } else {
                2
            };
            parts.push(KeyedView::new(
                "chips",
                super::page_ui::chip_rows(chips, per_row),
            ));
        }
        if let Some(group) = open {
            let entries = group.entries(digest);
            if !entries.is_empty() {
                parts.push(KeyedView::new(
                    format!("paths:{group:?}"),
                    m3::body(entries.join("\n"))
                        .text_wrapping(TextWrapping::Wrap)
                        .is_text_selection_enabled(true)
                        .automation_name(format!(
                            "{}: {}",
                            group.words(entries.len()),
                            entries.join(", ")
                        )),
                ));
            }
        }
        let mut caption = "What to look at first. Not a quality score.".to_owned();
        if !digest.complete || digest.truncated {
            caption.push_str(" Partial: the patch was larger than one read.");
        }
        parts.push(KeyedView::new(
            "caption",
            m3::secondary(caption).text_wrapping(TextWrapping::Wrap),
        ));
        m3::card_high(palette)
            .padding(Thickness::xy(tokens::spacing::MD, tokens::spacing::INSET))
            .content(
                StackPanel::new()
                    .spacing(tokens::spacing::CHIP_GAP)
                    .keyed_children(parts),
            )
    }

    /// Merge and check (`git.integrate`): the button, its question, the
    /// running state and the result card.
    fn merge_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let open = self.selected_agent_id().unwrap_or_default();
        let request = self.merge_request();
        match &self.pane.changes.merge {
            Merge::Confirm(agent) if *agent == open => {
                let what = request.as_ref().map_or_else(
                    |error| error.clone(),
                    |request| {
                        format!(
                            "Merge {} into {} in Swarm's own checkout and run the project check? Your worktree and the remote stay as they are.",
                            request.branches.join(", "),
                            request.base_branch
                        )
                    },
                );
                m3::card_high(palette).content(
                    StackPanel::new().spacing(tokens::spacing::SM).children((
                        m3::body(what).text_wrapping(TextWrapping::Wrap),
                        toolbar(
                            vec![],
                            vec![
                                KeyedView::new(
                                    "cancel",
                                    m3::text_button(palette)
                                        .automation_name("Cancel merge and check")
                                        .on_click(
                                            context.message(Message::Pane(PaneMsg::MergeAnswered(
                                                false,
                                            ))),
                                        )
                                        .content(m3::label("Cancel")),
                                ),
                                KeyedView::new(
                                    "go",
                                    m3::filled(palette)
                                        .is_enabled(request.is_ok())
                                        .automation_name("Merge and check")
                                        .on_click(
                                            context.message(Message::Pane(PaneMsg::MergeAnswered(
                                                true,
                                            ))),
                                        )
                                        .content(m3::label("Merge and check")),
                                ),
                            ],
                        ),
                    )),
                )
            }
            Merge::Running(agent) if *agent == open => m3::card_high(palette).content(
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .children((
                        ProgressRing::new()
                            .is_indeterminate(true)
                            .width(tokens::layout::LOADER_SMALL)
                            .height(tokens::layout::LOADER_SMALL)
                            .vertical_alignment(VerticalAlignment::Center),
                        m3::body("Merging and running the project check…"),
                    )),
            ),
            Merge::Done(agent, result) if *agent == open => merge_result(palette, result, context),
            _ => {
                let button = m3::tonal(palette)
                    .is_enabled(request.is_ok())
                    .automation_name(match &request {
                        Ok(_) => "Merge and check…".to_owned(),
                        Err(why) => format!("Merge and check: {why}"),
                    })
                    .on_click(context.message(Message::Pane(PaneMsg::MergeAsked)))
                    .content(
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .margin(Thickness::xy(
                                tokens::spacing::INSET - tokens::spacing::ROW,
                                0.0,
                            ))
                            .children((
                                m3::small_icon(m3::glyph::MERGE),
                                m3::text("Merge and check…", tokens::text::BUTTON)
                                    .vertical_alignment(VerticalAlignment::Center),
                            )),
                    );
                let button: View = match request {
                    Ok(_) => button,
                    Err(why) => Border::new().content(button).tooltip(why),
                };
                toolbar(vec![], vec![KeyedView::new("merge", button)])
            }
        }
    }

    /// Agents (skeleton 5.5, component 49): this task's manager and its
    /// workers nested, each with its state, name, model, activity and
    /// +/-; Open, Stop Agent and Hand off. Native subagents are observed.
    fn tree_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let tree = &self.monitor.data.tree;
        let root = self.tree_root();
        let mut items: Vec<KeyedView> = Vec::new();
        let current = tree.root == root;
        let workers = if current {
            tree.rows
                .iter()
                .filter(|row| Some(&row.id) != root.as_ref())
                .count()
        } else {
            0
        };
        items.push(KeyedView::new(
            "head",
            toolbar(
                vec![KeyedView::new(
                    "count",
                    m3::secondary(if current && tree.read {
                        eliot_core::count_words(workers as u64, "worker", "workers")
                    } else {
                        String::new()
                    })
                    .vertical_alignment(VerticalAlignment::Center),
                )],
                vec![KeyedView::new(
                    "refresh",
                    m3::icon_button(palette)
                        .automation_name("Refresh Agents")
                        .on_click(context.message(Message::Pane(PaneMsg::Refresh)))
                        .content(m3::icon(m3::glyph::REFRESH))
                        .tooltip(
                            tree.read_at_ms
                                .filter(|_| current)
                                .and_then(monitor::last_read_label)
                                .map_or_else(
                                    || "Refresh".to_owned(),
                                    |at| format!("Refresh ({at})"),
                                ),
                        ),
                )],
            ),
        ));
        if !current || (!tree.read && tree.rows.is_empty()) {
            items.push(KeyedView::new("reading", super::page_ui::quiet("Reading…")));
            return self.pane_body("Agents of this task", items);
        }
        if let Some(error) = &tree.error {
            items.push(KeyedView::new(
                "failed",
                super::page_ui::read_failed(
                    palette,
                    format!("Couldn't read this task's Agents: {error}"),
                    "Retry",
                    true,
                    context.message(Message::Pane(PaneMsg::Refresh)),
                ),
            ));
        }
        let now = tree.read_at_ms.unwrap_or_else(crate::handoff::now_ms);
        let selected = self.selected_agent_id();
        let one_line = self.pane.wide || self.pane.full;
        let mut rows: Vec<KeyedView> = Vec::new();
        if let Some(top) = root
            .as_ref()
            .and_then(|root| tree.rows.iter().find(|row| &row.id == root))
        {
            self.tree_rows(
                top,
                0,
                now,
                selected.as_deref(),
                one_line,
                context,
                palette,
                &mut rows,
            );
        } else {
            // The root is outside the read (archived away or past the
            // bound): its children still show, top level.
            for row in tree.rows.iter().filter(|row| {
                row.parent
                    .as_ref()
                    .is_none_or(|parent| !tree.rows.iter().any(|other| &other.id == parent))
            }) {
                self.tree_rows(
                    row,
                    0,
                    now,
                    selected.as_deref(),
                    one_line,
                    context,
                    palette,
                    &mut rows,
                );
            }
        }
        items.push(KeyedView::new(
            "tree",
            StackPanel::new()
                .spacing(tokens::spacing::ROW_GAP)
                .automation_name("Agents of this task")
                .keyed_children(rows),
        ));
        if workers == 0 {
            items.push(KeyedView::new("none", super::page_ui::quiet("No workers.")));
        }
        if tree.truncated {
            items.push(KeyedView::new(
                "partial",
                super::page_ui::quiet(format!(
                    "Partial: the first {} Agents of this task are shown.",
                    monitor::TREE_ROWS
                )),
            ));
        }
        self.pane_body("Agents of this task", items)
    }

    /// One tree row and, unless folded, its children one step in.
    #[allow(
        clippy::too_many_arguments,
        reason = "The row's distinct inputs stay explicit at its one recursive call site."
    )]
    fn tree_rows(
        &self,
        row: &MonitorRow,
        depth: usize,
        now: i64,
        selected: Option<&str>,
        one_line: bool,
        context: &mut ViewContext<Self>,
        palette: Palette,
        rows: &mut Vec<KeyedView>,
    ) {
        let tree = &self.monitor.data.tree;
        let children = tree.children(&row.id);
        let folded = self.pane.folded.contains(&row.id);
        let mut entries = Vec::new();
        self.monitor_entries(row, depth, None, selected, String::new(), now, &mut entries);
        let Some(monitor_view::MonitorEntry::Row(entry)) = entries.pop() else {
            return;
        };
        rows.push(KeyedView::new(
            format!("row:{}:{}:{folded}", row.id, entry.selected),
            tree_row(
                palette,
                &entry,
                depth,
                (!children.is_empty()).then_some(!folded),
                one_line,
                context,
            ),
        ));
        if !folded && depth < 6 {
            for child in children {
                self.tree_rows(
                    child,
                    depth + 1,
                    now,
                    selected,
                    one_line,
                    context,
                    palette,
                    rows,
                );
            }
        }
    }

    /// Terminal (skeleton 5.6): the worktree with Open in Windows Terminal,
    /// and the Agent's processes, read-only. No embedded terminal until
    /// that dependency is decided (skeleton 9.3).
    fn terminal_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let terminal = &self.pane.terminal;
        let open = self.selected_agent_id();
        let mut items: Vec<KeyedView> = Vec::new();
        match self.open_workspace() {
            None => items.push(KeyedView::new(
                "not-ready",
                super::page_ui::quiet("Available when the worktree is ready."),
            )),
            Some(folder) => {
                let shown = nav::display_path(&folder);
                items.push(KeyedView::new(
                    "folder",
                    Grid::new()
                        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::SM)
                        .children((
                            Border::new()
                                .vertical_alignment(VerticalAlignment::Center)
                                .content(m3::muted_icon(m3::glyph::FOLDER)),
                            m3::body(shown.clone())
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis)
                                .is_text_selection_enabled(true),
                            m3::tonal(palette)
                                .grid_column(2)
                                .automation_name(format!("Open {shown} in Windows Terminal"))
                                .on_click(context.message(Message::Pane(PaneMsg::OpenTerminal)))
                                .content(
                                    StackPanel::new()
                                        .orientation(Orientation::Horizontal)
                                        .spacing(tokens::spacing::SM)
                                        .margin(Thickness::xy(
                                            tokens::spacing::INSET - tokens::spacing::ROW,
                                            0.0,
                                        ))
                                        .children((
                                            m3::small_icon(m3::glyph::TERMINAL),
                                            m3::text(
                                                "Open in Windows Terminal",
                                                tokens::text::BUTTON,
                                            )
                                            .vertical_alignment(VerticalAlignment::Center),
                                        )),
                                ),
                        )),
                ));
            }
        }
        if let Some(note) = &terminal.note {
            items.push(KeyedView::new(
                "note",
                super::page_ui::error_line(palette, note.clone()),
            ));
        }
        items.push(KeyedView::new(
            "processes-head",
            toolbar(
                vec![KeyedView::new(
                    "title",
                    m3::text("Agent processes", tokens::text::SMALL_TITLE)
                        .automation_heading_level(AutomationHeadingLevel::Level3)
                        .vertical_alignment(VerticalAlignment::Center),
                )],
                vec![KeyedView::new(
                    "refresh",
                    m3::icon_button(palette)
                        .is_enabled(!terminal.reading)
                        .automation_name("Refresh Agent processes")
                        .on_click(context.message(Message::Pane(PaneMsg::Refresh)))
                        .content(m3::icon(m3::glyph::REFRESH))
                        .tooltip("Refresh"),
                )],
            ),
        ));
        let read = terminal.read.as_ref().filter(|_| terminal.agent == open);
        match read {
            None => items.push(KeyedView::new("reading", super::page_ui::quiet("Reading…"))),
            Some(Err(error)) => items.push(KeyedView::new(
                "failed",
                super::page_ui::read_failed(
                    palette,
                    format!("Couldn't read the processes: {error}"),
                    "Retry",
                    !terminal.reading,
                    context.message(Message::Pane(PaneMsg::Refresh)),
                ),
            )),
            Some(Ok(reply)) if reply.processes.is_empty() => items.push(KeyedView::new(
                "none",
                super::page_ui::quiet("No processes recorded for this Agent."),
            )),
            Some(Ok(reply)) => {
                let now = crate::handoff::now_ms();
                let rows: Vec<KeyedView> = reply
                    .processes
                    .iter()
                    .map(|process| {
                        let state = match process.state {
                            eliot_core::ProcessState::Running => "Running".to_owned(),
                            eliot_core::ProcessState::Exited => match process.exit_code {
                                Some(code) => format!("Exited · code {code}"),
                                None => "Exited".to_owned(),
                            },
                            eliot_core::ProcessState::Unknown => "State unknown".to_owned(),
                        };
                        let age = monitor::elapsed_label(now.saturating_sub(process.updated_at));
                        KeyedView::new(
                            format!("{}:{}", process.identity.pid, process.identity.start_time),
                            super::page_ui::kv_text(
                                &format!("{} · {}", process.role, process.identity.pid),
                                format!("{state} · {age} ago"),
                            ),
                        )
                    })
                    .collect();
                items.push(KeyedView::new(
                    "processes",
                    m3::card_high(palette).content(
                        StackPanel::new()
                            .spacing(tokens::spacing::XS)
                            .keyed_children(rows),
                    ),
                ));
                if reply.next.is_some() {
                    items.push(KeyedView::new(
                        "more",
                        super::page_ui::quiet("Partial: the first page of processes is shown."),
                    ));
                }
            }
        }
        self.pane_body("Terminal", items)
    }

    /// A collapsible Details section (`.sec` with `.sec-h`): its head
    /// toggles it; closed, only the head shows.
    pub(super) fn details_section(
        &self,
        key: &'static str,
        title: &str,
        body: Vec<KeyedView>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let open = self.pane.section_open(key);
        let head = m3::row(palette, false)
            .automation_name(format!(
                "{title} ({})",
                if open { "expanded" } else { "collapsed" }
            ))
            .on_click(context.message(Message::Pane(PaneMsg::Section(key))))
            .content(
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .children((
                        m3::text(title.to_owned(), tokens::text::SMALL_TITLE)
                            .automation_heading_level(AutomationHeadingLevel::Level3)
                            .vertical_alignment(VerticalAlignment::Center),
                        Border::new()
                            .grid_column(1)
                            .vertical_alignment(VerticalAlignment::Center)
                            .content(m3::muted_icon(if open {
                                m3::glyph::CHEVRON_UP
                            } else {
                                m3::glyph::CHEVRON_DOWN
                            })),
                    )),
            );
        let mut parts = vec![KeyedView::new(format!("head:{open}"), head)];
        if open {
            parts.extend(body);
        }
        m3::card_high(palette)
            .padding(Thickness::new(
                tokens::spacing::SM,
                tokens::spacing::XS,
                tokens::spacing::SM,
                if open {
                    tokens::spacing::INSET
                } else {
                    tokens::spacing::XS
                },
            ))
            .content(
                StackPanel::new()
                    .spacing(tokens::spacing::CHIP_GAP)
                    .keyed_children(parts),
            )
    }

    /// Details › Goal's controls where the daemon has the call: Pause or
    /// Resume and Clear; a round is the daemon's (Run a round now and Edit
    /// say why they are not here).
    pub(super) fn goal_actions(
        &self,
        goal: &eliot_core::api::GoalView,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> Vec<KeyedView> {
        use eliot_core::api::GoalStatus;
        let busy = self.pane.goal_busy;
        let control =
            |control: client::GoalControl, context: &mut ViewContext<Self>| -> KeyedView {
                KeyedView::new(
                    control.label(),
                    m3::text_button(palette)
                        .is_enabled(!busy)
                        .automation_name(format!("{} goal", control.label()))
                        .on_click(context.message(Message::Pane(PaneMsg::Goal(control))))
                        .content(m3::label(control.label())),
                )
            };
        let unavailable = |label: &str, why: &str| -> KeyedView {
            KeyedView::new(
                label.to_owned(),
                Border::new()
                    .content(
                        m3::text_button(palette)
                            .is_enabled(false)
                            .automation_name(format!("{label}: {why}"))
                            .content(m3::label(label)),
                    )
                    .tooltip(why.to_owned()),
            )
        };
        let mut actions = vec![unavailable(
            "Run a round now",
            "A round needs the daemon's idle or cadence key; a stuck goal offers one in the Inbox",
        )];
        match goal.status {
            GoalStatus::Active | GoalStatus::Blocked => {
                actions.push(control(client::GoalControl::Pause, context));
            }
            GoalStatus::Paused => actions.push(control(client::GoalControl::Resume, context)),
            GoalStatus::Completed | GoalStatus::Cleared => {}
        }
        actions.push(unavailable(
            "Edit",
            "Editing a goal is not in the desktop yet: swarm goal set changes it",
        ));
        if goal.status != GoalStatus::Cleared {
            actions.push(control(client::GoalControl::Clear, context));
        }
        let mut rows = vec![KeyedView::new("actions", toolbar(actions, Vec::new()))];
        if let Some(note) = &self.pane.goal_note {
            rows.push(KeyedView::new(
                "note",
                super::page_ui::error_line(palette, note.clone()),
            ));
        }
        rows
    }
}

/// Wait while the window is minimized or hidden (locally, no daemon
/// request); false when cancelled meanwhile.
fn wait_shown(cancelled: &dyn Fn() -> bool) -> bool {
    while crate::window_place::hidden() {
        if !feed::backoff(monitor::HIDDEN_CHECK_MS, cancelled) {
            return false;
        }
    }
    !cancelled()
}

/// A row of controls (`.toolbar-row`), 8 DIPs apart: `lead` at the start,
/// `trail` pushed to the end.
fn toolbar(lead: Vec<KeyedView>, trail: Vec<KeyedView>) -> View {
    Grid::new()
        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
        .column_spacing(tokens::spacing::SM)
        .children((
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .vertical_alignment(VerticalAlignment::Center)
                .keyed_children(lead),
            StackPanel::new()
                .grid_column(2)
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .vertical_alignment(VerticalAlignment::Center)
                .keyed_children(trail),
        ))
}

/// A porcelain status (`agent.git` dirty entry) in the tab's words.
fn porcelain_word(status: &str) -> &'static str {
    if status.contains('?') {
        "Untracked"
    } else if status.contains('U') {
        "Conflicted"
    } else if status.contains('R') {
        "Renamed"
    } else if status.contains('A') {
        "Added"
    } else if status.contains('D') {
        "Deleted"
    } else {
        "Modified"
    }
}

/// A digest file's change (`added`, `modified`, `deleted`, `untracked`).
fn change_word(change: &str) -> &'static str {
    match change {
        "added" => "Added",
        "deleted" => "Deleted",
        "untracked" => "Untracked",
        _ => "Modified",
    }
}

/// `path` relative to the worktree `root`, with forward slashes (as Git
/// and the digest name it); a path outside it stays as given.
fn relative_path(root: &str, path: &str) -> String {
    let normal = |text: &str| text.replace('\\', "/").trim_end_matches('/').to_owned();
    let (root, path) = (normal(root), normal(path));
    if !root.is_empty()
        && path.len() > root.len()
        && path[..root.len()].eq_ignore_ascii_case(&root)
        && path.as_bytes()[root.len()] == b'/'
    {
        path[root.len() + 1..].to_owned()
    } else {
        path
    }
}

/// The summary line: "{N} files changed +A -D · {commits} commits ahead of
/// {base}", with Partial when the read was cut. Counts the daemon does not
/// give for a source are left out, never shown as zero.
fn summary_line(
    palette: Palette,
    source: Source,
    files: Option<usize>,
    lines: Option<(u64, u64)>,
    ahead: Option<u64>,
    base: Option<&str>,
    partial: bool,
) -> View {
    let mut parts: Vec<KeyedView> = Vec::new();
    let files_words = match (source, files) {
        (_, None) => "Files not read".to_owned(),
        (Source::LastTurn, Some(count)) => {
            format!(
                "{} edited in the last turn",
                eliot_core::count_words(count as u64, "file", "files")
            )
        }
        (_, Some(count)) => format!(
            "{} changed",
            eliot_core::count_words(count as u64, "file", "files")
        ),
    };
    parts.push(KeyedView::new("files", m3::secondary(files_words)));
    if source == Source::Branch
        && let Some((add, del)) = lines
    {
        parts.push(KeyedView::new("lines", m3::diff_counts(add, del, false)));
    }
    if let Some(ahead) = ahead {
        let base = base.map_or_else(String::new, |base| format!(" of {base}"));
        parts.push(KeyedView::new(
            "ahead",
            m3::secondary(format!(
                "\u{b7} {} ahead{base}",
                eliot_core::count_words(ahead, "commit", "commits")
            )),
        ));
    }
    if partial {
        parts.push(KeyedView::new("partial", m3::badge(palette, "Partial")));
    }
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::XS)
        .keyed_children(parts)
}

/// A changed file (component 48, `.filelist .f`): its icon, the name
/// bright and the folder quiet, its state word; selected on the
/// selection role.
fn file_row(
    palette: Palette,
    path: &str,
    state: &str,
    selected: bool,
    context: &mut ViewContext<DesktopApp>,
) -> View {
    let (folder, name) = match path.rsplit_once('/') {
        Some((folder, name)) => (folder, name),
        None => ("", path),
    };
    let glyph = match state {
        "Added" | "Untracked" => m3::glyph::NEW_FILE,
        "Deleted" => m3::glyph::DELETE,
        _ => m3::glyph::EDIT,
    };
    let quiet = if selected {
        palette.on_selected()
    } else {
        palette.muted()
    };
    m3::row(palette, selected)
        .automation_name(format!(
            "{path}, {state}{}",
            if selected { ", selected" } else { "" }
        ))
        .on_click(context.message(Message::Pane(PaneMsg::File(path.to_owned()))))
        .content(
            Grid::new()
                .columns([
                    GridLength::Auto,
                    GridLength::Auto,
                    GridLength::STAR,
                    GridLength::Auto,
                ])
                .column_spacing(tokens::spacing::SM)
                .children((
                    Border::new()
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(m3::small_icon(glyph)),
                    m3::text(name.to_owned(), tokens::text::BUTTON)
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                    m3::text(folder.to_owned(), tokens::text::BODY)
                        .grid_column(2)
                        .foreground(quiet)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                    m3::text(state.to_owned(), tokens::text::CAPTION)
                        .grid_column(3)
                        .foreground(quiet)
                        .vertical_alignment(VerticalAlignment::Center),
                )),
        )
        .tooltip(path.to_owned())
}

/// The selected file: Open in editor and Copy path, and why its diff is
/// not shown.
fn selected_file(palette: Palette, path: &str, context: &mut ViewContext<DesktopApp>) -> View {
    m3::card_high(palette)
        .content(
            StackPanel::new().spacing(tokens::spacing::SM).children((
                m3::body(path.to_owned())
                    .text_trimming(TextTrimming::CharacterEllipsis)
                    .is_text_selection_enabled(true),
                m3::secondary(
                    "Swarm cannot show this file's diff yet: the daemon gives the changed files and line totals, not the diff text.",
                )
                .text_wrapping(TextWrapping::Wrap),
                toolbar(vec![
                    KeyedView::new(
                        "open",
                        m3::outlined(palette)
                            .automation_name(format!("Open {path} in its editor"))
                            .on_click(context.message(Message::Pane(PaneMsg::OpenFile(path.to_owned()))))
                            .content(m3::icon_label(m3::glyph::EXTERNAL, "Open in editor"))
                            .tooltip("Open with its default app"),
                    ),
                    KeyedView::new(
                        "copy",
                        m3::text_button(palette)
                            .automation_name(format!("Copy the path {path}"))
                            .on_click(context.message(Message::Pane(PaneMsg::CopyPath(path.to_owned()))))
                            .content(m3::icon_label(m3::glyph::COPY, "Copy path")),
                    ),
                    ], vec![
                ]),
            )),
        )
}

/// Merge and check's result card: merged and checked, conflicting paths,
/// a failed check, or why it did not run.
fn merge_result(
    palette: Palette,
    result: &Result<eliot_core::Command, String>,
    context: &mut ViewContext<DesktopApp>,
) -> View {
    let (title, lines, failed): (String, Vec<String>, bool) = match result {
        Err(error) => (
            "Merge and check did not finish".to_owned(),
            vec![error.clone()],
            true,
        ),
        Ok(command) => match command
            .outcome
            .as_ref()
            .filter(|body| body.get("base_oid").is_some())
            .and_then(|body| serde_json::from_value::<GitIntegrateOutcome>(body.clone()).ok())
        {
            None => (
                format!(
                    "Merge and check {}",
                    format!("{:?}", command.state).to_lowercase()
                ),
                vec![format!("Command {}", command.id)],
                true,
            ),
            Some(outcome) => match outcome.status {
                GitIntegrateStatus::Merged => (
                    "Merged and checked".to_owned(),
                    outcome
                        .integration_oid
                        .iter()
                        .map(|oid| format!("Merge commit {oid}"))
                        .collect(),
                    false,
                ),
                GitIntegrateStatus::Conflict => {
                    let mut lines = outcome.conflict_paths.clone();
                    if outcome.conflict_paths_truncated {
                        lines.push("More conflicting paths are not listed.".to_owned());
                    }
                    ("Merge conflict".to_owned(), lines, true)
                }
                GitIntegrateStatus::CheckFailed => (
                    "The project check failed".to_owned(),
                    outcome
                        .check_failure
                        .iter()
                        .flat_map(|check| {
                            let mut lines = Vec::new();
                            if !check.failed_branches.is_empty() {
                                lines.push(format!(
                                    "Failing on its own: {}",
                                    check.failed_branches.join(", ")
                                ));
                            }
                            if !check.tail.is_empty() {
                                lines.push(check.tail.clone());
                            }
                            lines
                        })
                        .collect(),
                    true,
                ),
                GitIntegrateStatus::Unknown => (
                    "The merge's outcome is unknown".to_owned(),
                    outcome
                        .failure_code
                        .iter()
                        .map(|code| eliot_core::reason_words(code))
                        .collect(),
                    true,
                ),
            },
        },
    };
    let title_view = m3::text(title.clone(), tokens::text::SMALL_TITLE);
    let title_view = if failed {
        title_view.foreground(palette.attention())
    } else {
        title_view
    };
    m3::card_high(palette).content(
        StackPanel::new().spacing(tokens::spacing::XS).children((
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .children((
                    title_view.vertical_alignment(VerticalAlignment::Center),
                    m3::icon_button(palette)
                        .grid_column(1)
                        .automation_name("Dismiss the merge result")
                        .on_click(context.message(Message::Pane(PaneMsg::MergeDismissed)))
                        .content(m3::icon(m3::glyph::CLOSE)),
                )),
            m3::body(lines.join("\n"))
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true)
                .automation_name(format!("{title}: {}", lines.join(" "))),
        )),
    )
}

/// One Agents tab row (component 49): its fold chevron, then the row (its
/// state mark, name, "observed" for a native subagent, model, activity and
/// +/-), which opens its conversation, then Stop Agent and Hand off. In
/// the Normal width the model and activity take a second line.
fn tree_row(
    palette: Palette,
    entry: &monitor_view::RowEntry,
    depth: usize,
    children: Option<bool>,
    one_line: bool,
    context: &mut ViewContext<DesktopApp>,
) -> View {
    let row = &entry.row;
    let counts: View = match row
        .work
        .as_ref()
        .and_then(|work| work.insertions.zip(work.deletions))
    {
        Some((add, del)) => m3::diff_counts(add, del, entry.selected),
        None => View::empty(),
    };
    let mut name: Vec<KeyedView> = vec![
        KeyedView::new(
            "mark",
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(super::page_ui::agent_mark(
                    palette,
                    row.state,
                    row.status_line.as_deref(),
                )),
        ),
        KeyedView::new(
            "title",
            m3::text(entry.title.clone(), tokens::text::BODY)
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
        ),
    ];
    if entry.observed {
        name.push(KeyedView::new(
            "observed",
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .content(m3::badge(palette, "observed")),
        ));
    }
    let model = m3::text(entry.model.clone(), tokens::text::CAPTION)
        .foreground(palette.muted())
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis);
    let activity = m3::text(entry.activity.clone(), tokens::text::CAPTION)
        .foreground(if entry.attention {
            palette.waiting()
        } else {
            palette.muted()
        })
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis);
    let name_grid = Grid::new()
        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
        .column_spacing(tokens::spacing::SM)
        .keyed_children(name);
    let content: View = if one_line {
        Grid::new()
            .columns([
                GridLength::Star(2.0),
                GridLength::STAR,
                GridLength::STAR,
                GridLength::Auto,
            ])
            .column_spacing(tokens::spacing::SM)
            .children((
                name_grid,
                model.grid_column(1),
                activity.grid_column(2),
                Border::new()
                    .grid_column(3)
                    .vertical_alignment(VerticalAlignment::Center)
                    .content(counts),
            ))
    } else {
        StackPanel::new()
            .vertical_alignment(VerticalAlignment::Center)
            .children((
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .column_spacing(tokens::spacing::SM)
                    .children((
                        name_grid,
                        Border::new()
                            .grid_column(1)
                            .vertical_alignment(VerticalAlignment::Center)
                            .content(counts),
                    )),
                Grid::new()
                    .margin(Thickness::new(
                        tokens::layout::MARK + tokens::spacing::SM,
                        0.0,
                        0.0,
                        0.0,
                    ))
                    .columns([GridLength::Auto, GridLength::STAR])
                    .column_spacing(tokens::spacing::SM)
                    .children((model, activity.grid_column(1))),
            ))
    };
    let words = format!(
        "{}, {}, {}, {}{}",
        entry.title,
        row.status_words(),
        entry.model,
        entry.activity,
        if entry.observed { ", observed" } else { "" }
    );
    let height = if one_line {
        tokens::height::CONTROL
    } else {
        tokens::height::TWO_LINE_ROW
    };
    let button = m3::row(palette, entry.selected)
        .grid_column(1)
        .height(height)
        .min_height(height)
        .automation_name(format!("Open {words}"))
        .on_click(context.message(Message::Pane(PaneMsg::TreeOpen(row.id.clone()))))
        .content(content)
        .tooltip(words.clone());
    let chevron: View = match children {
        Some(open) => m3::icon_button(palette)
            .automation_name(format!(
                "{} the workers of {}",
                if open { "Hide" } else { "Show" },
                entry.title
            ))
            .on_click(context.message(Message::Pane(PaneMsg::TreeFold(row.id.clone()))))
            .content(m3::small_icon(if open {
                m3::glyph::CHEVRON_DOWN
            } else {
                m3::glyph::CHEVRON_RIGHT
            })),
        None => View::empty(),
    };
    let stop: View = match entry.stop {
        Some(state) => m3::icon_button(palette)
            .is_enabled(state == monitor_view::StopButton::Ready)
            .automation_name(match state {
                monitor_view::StopButton::Stopping => format!("Stopping {}", entry.title),
                _ => format!("Stop Agent {} (run {})", entry.title, row.generation),
            })
            .on_click(context.message(Message::Pane(PaneMsg::TreeStop(
                row.id.clone(),
                row.generation,
            ))))
            .content(m3::icon(m3::glyph::STOP_CIRCLE))
            .tooltip("Stop Agent"),
        None => View::empty(),
    };
    let handoff: View = if entry.observed {
        View::empty()
    } else {
        m3::icon_button(palette)
            .automation_name(format!("Hand off {}", entry.title))
            .on_click(context.message(Message::Pane(PaneMsg::TreeHandoff(
                row.id.clone(),
                row.generation,
            ))))
            .content(m3::icon(m3::glyph::FORWARD))
            .tooltip("Hand off or switch model")
    };
    Grid::new()
        .margin(Thickness::new(
            depth as f64 * tokens::layout::TREE_INDENT,
            0.0,
            0.0,
            0.0,
        ))
        .columns([
            GridLength::Pixel(tokens::height::CONTROL),
            GridLength::STAR,
            GridLength::Pixel(tokens::height::CONTROL),
            GridLength::Pixel(tokens::height::CONTROL),
        ])
        .children((
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(chevron),
            button,
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .content(stop),
            Border::new()
                .grid_column(3)
                .vertical_alignment(VerticalAlignment::Center)
                .content(handoff),
        ))
}
