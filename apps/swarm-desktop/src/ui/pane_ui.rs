//! The right pane (skeleton 3 and 5.4-5.6; `Decision.dc.html` for Changes,
//! `Finished.dc.html` for Details): the open panel (Changes, Agents,
//! Details, Terminal) under its bar of the title, the Wide and Full view
//! toggles and Close, inside the main card.
//!
//! Reads (the Owner watches daemon load): a tab reads only while the pane
//! shows it, and nothing reads on render. Changes reads `agent.git`,
//! `agent.patch_digest` and the chosen source's bounded `agent.diff` when
//! it opens, when the open task or the source changes, when the open
//! Agent's run or Git work changes (and, on Last turn, its last finished
//! turn) and on Refresh. A selected file whose patch that read cut is read
//! alone (`agent.diff` with its path), once. The Terminal tab reads
//! `agent.processes` on open, task change and Refresh. Agents and Details
//! ride the Monitor's refresh chain, which follows `events.wait` on their
//! Agents' fleet events as the Agents page does. Every read waits while the
//! window is minimized.

use std::collections::HashSet;
use std::sync::Arc;

use eliot_core::api::{
    AgentDiffRequest, AgentDiffResponse, AgentGitResponse, AgentPatchDigestResponse,
    AgentProcessesResponse, GitDiffFile, GitDiffSource, GitIntegrateOutcome, GitIntegrateStatus,
    GitView, MAX_DIFF_TEXT_BYTES, MAX_DIFF_TEXT_FILES, MAX_DIFF_TEXT_HUNKS,
};

use super::*;
use crate::monitor::MonitorRow;

/// What Changes compares (brand book §11.1, `agent.diff` source).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum Source {
    /// HEAD to the worktree, staged and unstaged.
    Uncommitted,
    /// The recorded base to the current workspace.
    #[default]
    Branch,
    /// The current patch of the files the last completed turn reported:
    /// not a historical patch of that turn alone.
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

    /// Its meaning under the current API (§11.1's table), as its tooltip.
    fn meaning(self) -> &'static str {
        match self {
            Self::Uncommitted => "HEAD to the worktree, staged and unstaged changes",
            Self::Branch => "The recorded base to the current workspace",
            Self::LastTurn => "Current changes in files reported by the last completed turn",
        }
    }

    fn wire(self) -> GitDiffSource {
        match self {
            Self::Uncommitted => GitDiffSource::Uncommitted,
            Self::Branch => GitDiffSource::Branch,
            Self::LastTurn => GitDiffSource::LastTurn,
        }
    }
}

/// One `agent.diff` reply, each file's patch parsed off the UI thread
/// (`parsed` in `reply.files` order).
#[derive(Clone, Debug)]
pub(super) struct Diff {
    reply: AgentDiffResponse,
    parsed: Vec<Arc<diff_ui::Parsed>>,
}

impl Diff {
    /// The read of `source` of `agent`: one literal `path` in it when
    /// given; with `generation`, that run only.
    fn request(
        agent: &str,
        generation: Option<i64>,
        source: Source,
        path: Option<String>,
    ) -> AgentDiffRequest {
        AgentDiffRequest {
            id: agent.to_owned(),
            generation,
            source: source.wire(),
            path,
        }
    }

    fn of(reply: AgentDiffResponse) -> Self {
        let parsed = reply
            .files
            .iter()
            .map(|file| Arc::new(diff_ui::parse(file)))
            .collect();
        Self { reply, parsed }
    }

    fn file(&self, path: &str) -> Option<(&GitDiffFile, &Arc<diff_ui::Parsed>)> {
        self.reply
            .files
            .iter()
            .zip(&self.parsed)
            .find(|(file, _)| file.path == path)
    }
}

/// What reads Changes again when it changes: the open Agent's run and its
/// Git work, and on Last turn its last finished turn.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct WorkStamp {
    generation: Option<i64>,
    /// The row's work summary without its current-turn tool and message
    /// counts, which move with every step and say nothing of the files.
    work: Option<eliot_core::api::WorkSummaryView>,
    turn: Option<String>,
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

/// One Changes read: the Git snapshot, the review digest and the source's
/// patch, each with its own failure.
#[derive(Clone, Debug)]
pub(super) struct ChangesRead {
    git: Result<AgentGitResponse, String>,
    digest: Result<AgentPatchDigestResponse, String>,
    /// The source this read's patch is of.
    source: Source,
    diff: Result<Diff, eliot_core::Error>,
    at_ms: i64,
}

/// A selected file's own `agent.diff` read, when the source's read cut its
/// patch: asked once per list read.
#[derive(Default)]
pub(super) struct FileState {
    /// The path asked for (in flight or landed).
    path: Option<String>,
    epoch: u64,
    task: Option<ComponentTask>,
    read: Option<Result<Diff, eliot_core::Error>>,
}

impl FileState {
    fn drop_read(&mut self) {
        if let Some(task) = self.task.take() {
            task.cancel();
        }
        self.epoch += 1;
        self.path = None;
        self.read = None;
    }
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
    /// The open Agent's work when the shown read started.
    stamp: WorkStamp,
    filter: String,
    selected: Option<String>,
    file: FileState,
    /// Split chosen (it shows only in the wide pane; Unified otherwise).
    split: bool,
    /// Long patch lines scroll sideways instead of wrapping.
    nowrap: bool,
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

/// Full view, the panels' reads and the owner's choices in them.
#[derive(Default)]
pub(super) struct PaneState {
    /// Full view: the open panel over the whole main card.
    pub(super) full: bool,
    /// Wide: the pane shown at 720 while pressed. The saved width stays
    /// the handle's, so unpressing returns to the width before Wide, after
    /// a restart too.
    pub(super) wide: bool,
    changes: ChangesState,
    terminal: TerminalState,
    /// Agents tab rows whose workers are folded away.
    folded: HashSet<String>,
    /// Details sections whose open state the owner flipped.
    flipped: HashSet<&'static str>,
}

impl PaneState {
    pub(super) fn section_open(&self, key: &'static str) -> bool {
        CLOSED_AT_FIRST.contains(&key) == self.flipped.contains(key)
    }
}

#[derive(Clone, Debug)]
pub(super) enum PaneMsg {
    /// The Full view toggle: the panel over the whole main card, or back
    /// beside the conversation.
    Full(bool),
    /// The Wide toggle: the pane shown at its wide width (720), or back to
    /// its saved width, the one it had before. Wide itself saves nothing.
    Wide(bool),
    /// The open tab's Refresh (and Retry).
    Refresh,
    /// A Changes read landed (its epoch), or was dropped (`None`).
    ChangesLoaded(u64, Option<Box<ChangesRead>>),
    /// A selected file's own read landed (its epoch), or was dropped.
    FileLoaded(u64, Option<Box<Result<Diff, eliot_core::Error>>>),
    Source(Source),
    /// Unified (`false`) or Split (`true`).
    Split(bool),
    /// The Wrap toggle.
    Wrap(bool),
    Filter(String),
    File(String),
    DigestGroup(Digest),
    OpenFile(String),
    /// Show a changed file in File Explorer (its folder when it is gone).
    OpenFolder(String),
    /// Opening a changed file or its folder settled off the UI thread:
    /// why it failed, if it did.
    Opened(Option<String>),
    CopyPath(String),
    /// Copy a changed file's whole patch as shown.
    CopyDiff(String),
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
    /// Copy text (what it is, the text).
    Copy(String, String),
}

impl DesktopApp {
    pub(super) fn pane_update(&mut self, msg: PaneMsg, context: &ComponentContext<Self>) {
        match msg {
            PaneMsg::Full(on) => self.pane.full = on,
            PaneMsg::Wide(on) => {
                let saved = split_ui::PanelWidths::of(self.prefs.panels);
                if on || self.pane.wide {
                    self.pane.wide = on;
                } else if saved.pane >= tokens::layout::PANEL_WIDE_WIDTH {
                    // The handle or Settings saved 720 itself: there is no
                    // earlier width, so the pane takes its default.
                    self.shell_update(
                        ShellMsg::PanelsResized(
                            saved.with(split_ui::Edge::Pane, tokens::layout::PANEL_WIDTH),
                        ),
                        context,
                    );
                }
            }
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
                    // A new list: a file read of the old one is stale.
                    changes.file.drop_read();
                    self.read_file(context);
                }
            }
            PaneMsg::FileLoaded(epoch, read) => {
                let file = &mut self.pane.changes.file;
                if epoch != file.epoch {
                    return;
                }
                file.task = None;
                if let Some(read) = read {
                    file.read = Some(*read);
                }
            }
            PaneMsg::Source(source) => {
                if self.pane.changes.source != source {
                    self.pane.changes.source = source;
                    self.read_changes(context);
                }
            }
            PaneMsg::Split(on) => self.pane.changes.split = on,
            PaneMsg::Wrap(on) => self.pane.changes.nowrap = !on,
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
                self.read_file(context);
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
                // The file system checks and the open run off the UI thread.
                Ok((worktree, relative)) => {
                    let _task = context.spawn_background(move |_| {
                        match super::reveal::inside_worktree(&worktree, &relative)
                            .and_then(|full| super::reveal::open_for_editing(&full))
                        {
                            Ok(()) => Message::Pane(PaneMsg::Opened(None)),
                            Err(error) => Message::Pane(PaneMsg::Opened(Some(error))),
                        }
                    });
                }
                Err(error) => self.alert(error),
            },
            PaneMsg::OpenFolder(path) => match self.changes_file(&path) {
                Ok((worktree, relative)) => {
                    let _task = context.spawn_background(move |_| {
                        // A deleted file's folder still shows where it was.
                        let shown =
                            super::reveal::inside_worktree(&worktree, &relative).or_else(|_| {
                                match relative.parent() {
                                    Some(parent) if !parent.as_os_str().is_empty() => {
                                        super::reveal::inside_worktree(&worktree, parent)
                                    }
                                    _ => super::reveal::inside_worktree(&worktree, Path::new("")),
                                }
                            });
                        match shown.and_then(|path| super::reveal::reveal(&path.to_string_lossy()))
                        {
                            Ok(()) => Message::Pane(PaneMsg::Opened(None)),
                            Err(error) => Message::Pane(PaneMsg::Opened(Some(error))),
                        }
                    });
                }
                Err(error) => self.alert(error),
            },
            PaneMsg::Opened(failed) => {
                if let Some(error) = failed {
                    self.alert(error);
                }
            }
            PaneMsg::CopyDiff(path) => match self.whole_patch(&path) {
                Some(text) => match crate::clipboard::set_text(&text) {
                    Ok(()) => self.set_status(format!("Copied the diff of {path}.")),
                    Err(error) => self.alert(format!("Copy failed: {error}")),
                },
                None => self.alert(format!("{path} has no whole patch to copy.")),
            },
            PaneMsg::CopyPath(path) => match crate::clipboard::set_text(&path) {
                Ok(()) => self.set_status(format!("Copied {path}.")),
                Err(error) => self.alert(format!("Copy failed: {error}")),
            },
            PaneMsg::ViewChanges => {
                let changed = self.pane.changes.source != Source::LastTurn;
                self.pane.changes.source = Source::LastTurn;
                if self.panel == Panel::Changes && self.panel_shown() {
                    if changed {
                        self.read_changes(context);
                    }
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

    /// After every update, the one path that keeps the pane on the open
    /// Agent, however it was opened (a sidebar row, the list refresh, the
    /// remembered task at start, Back): a tab that no longer shows stops its
    /// read; the shown tab, when its read is of another Agent (or none),
    /// drops that read and reads the open Agent. A worker's Agents tab
    /// follows its task's root once the readback names it.
    pub(super) fn sync_pane(&mut self, context: &ComponentContext<Self>) {
        let shown = self.panel_shown();
        if !(shown && self.panel == Panel::Changes) {
            if let Some(task) = self.pane.changes.task.take() {
                task.cancel();
                self.pane.changes.epoch += 1;
                self.pane.changes.reading = false;
            }
            if self.pane.changes.file.task.is_some() {
                self.pane.changes.file.drop_read();
            }
        }
        if !(shown && self.panel == Panel::Terminal)
            && let Some(task) = self.pane.terminal.task.take()
        {
            task.cancel();
            self.pane.terminal.epoch += 1;
            self.pane.terminal.reading = false;
        }
        let Some(open) = self.selected_agent_id().filter(|_| shown) else {
            return;
        };
        match self.panel {
            Panel::Changes if self.pane.changes.agent.as_deref() != Some(open.as_str()) => {
                self.read_changes(context);
            }
            // The Agent's work changed since the shown read started.
            Panel::Changes
                if !self.pane.changes.reading && self.pane.changes.stamp != self.work_stamp() =>
            {
                self.read_changes(context);
            }
            Panel::Terminal if self.pane.terminal.agent.as_deref() != Some(open.as_str()) => {
                self.read_processes(context);
            }
            Panel::Details if self.monitor.selected.as_deref() != Some(open.as_str()) => {
                self.show_details(&open, context);
            }
            Panel::Agents if self.monitor.tree_requested != self.tree_root() => {
                self.read_monitor(0, context);
            }
            _ => {}
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

    /// A changed file as the read's worktree and its repository-relative
    /// path. The daemon's path is data: one with a root, a drive, `..` or
    /// `.` is refused here, and [`super::reveal::inside_worktree`] checks
    /// the canonical result stays inside the worktree before any open.
    fn changes_file(&self, path: &str) -> Result<(PathBuf, PathBuf), String> {
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
            .or_else(|| self.open_workspace())
            .ok_or_else(|| "This task has no worktree to open the file from.".to_owned())?;
        let relative = PathBuf::from(path.replace('/', "\\"));
        let plain = !relative.as_os_str().is_empty()
            && relative
                .components()
                .all(|part| matches!(part, std::path::Component::Normal(_)));
        if !plain {
            return Err(format!("{path} is not a path inside the worktree."));
        }
        Ok((PathBuf::from(root), relative))
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

    /// A changed file's whole patch text as Changes shows it: the source's
    /// read, else the file's own read when that one cut it.
    fn whole_patch(&self, path: &str) -> Option<String> {
        let changes = &self.pane.changes;
        let read = changes.read.as_deref()?;
        let (listed, _) = read.diff.as_ref().ok()?.file(path)?;
        let file = if listed.truncated && !listed.binary {
            changes
                .file
                .read
                .as_ref()
                .filter(|_| changes.file.path.as_deref() == Some(path))?
                .as_ref()
                .ok()?
                .file(path)?
                .0
        } else {
            listed
        };
        (!file.truncated).then(|| file.text.clone())
    }

    /// The open Agent's [`WorkStamp`] now.
    fn work_stamp(&self) -> WorkStamp {
        let open = self.selected_agent_id();
        let work = self
            .open_work()
            .map(|work| eliot_core::api::WorkSummaryView {
                tool_calls: None,
                messages: None,
                ..work.clone()
            });
        let turn = (self.pane.changes.source == Source::LastTurn)
            .then(|| self.last_finished_turn().map(|item| item.key.clone()))
            .flatten();
        WorkStamp {
            generation: open
                .as_deref()
                .and_then(|id| self.find_agent(id))
                .map(|row| row.generation),
            work,
            turn,
        }
    }

    /// Read the open task's Git snapshot, review digest and the chosen
    /// source's patch (together, on a worker; each file's patch parsed
    /// there), keeping the last read shown until it lands.
    fn read_changes(&mut self, context: &ComponentContext<Self>) {
        let (Some(agent), Ok(data_dir)) = (self.selected_agent_id(), self.data_dir.clone()) else {
            return;
        };
        if self.daemon_down() {
            return;
        }
        let stamp = self.work_stamp();
        let changes = &mut self.pane.changes;
        if let Some(task) = changes.task.take() {
            task.cancel();
        }
        if changes.agent.as_deref() != Some(agent.as_str()) {
            changes.agent = Some(agent.clone());
            changes.read = None;
            changes.selected = None;
            changes.file.drop_read();
            changes.open = None;
            changes.merge = Merge::Idle;
        }
        changes.stamp = stamp;
        changes.epoch += 1;
        changes.reading = true;
        let (epoch, source) = (changes.epoch, changes.source);
        changes.task = Some(context.spawn_background(move |cancel| {
            if cancel.is_cancelled() {
                return Message::Pane(PaneMsg::ChangesLoaded(epoch, None));
            }
            let request = Diff::request(&agent, None, source, None);
            let (git, digest, diff) = client::block_on(futures_util::future::join3(
                client::agent_git(&data_dir, &agent),
                client::patch_digest(&data_dir, &agent),
                client::agent_diff(&data_dir, &request),
            ));
            Message::Pane(PaneMsg::ChangesLoaded(
                epoch,
                Some(Box::new(ChangesRead {
                    git: git.map_err(|error| error.user_message()),
                    digest: digest.map_err(|error| error.user_message()),
                    source,
                    diff: diff.map(Diff::of),
                    at_ms: crate::handoff::now_ms(),
                })),
            ))
        }));
    }

    /// Read the selected file alone when the shown read cut its patch (the
    /// read's run only), once per list read; nothing otherwise.
    fn read_file(&mut self, context: &ComponentContext<Self>) {
        let (Some(agent), Ok(data_dir)) = (self.selected_agent_id(), self.data_dir.clone()) else {
            return;
        };
        let changes = &mut self.pane.changes;
        let Some(path) = changes.selected.clone() else {
            return;
        };
        let Some(read) = changes
            .read
            .as_deref()
            .filter(|read| read.source == changes.source && changes.agent.as_ref() == Some(&agent))
        else {
            return;
        };
        let Ok(diff) = &read.diff else {
            return;
        };
        let cut = diff
            .file(&path)
            .is_some_and(|(file, _)| file.truncated && !file.binary);
        if !cut || changes.file.path.as_deref() == Some(path.as_str()) {
            return;
        }
        let (source, generation) = (read.source, diff.reply.generation);
        changes.file.drop_read();
        changes.file.path = Some(path.clone());
        let epoch = changes.file.epoch;
        changes.file.task = Some(context.spawn_background(move |cancel| {
            if cancel.is_cancelled() {
                return Message::Pane(PaneMsg::FileLoaded(epoch, None));
            }
            let request = Diff::request(&agent, Some(generation), source, Some(path));
            let read = client::block_on(client::agent_diff(&data_dir, &request)).map(Diff::of);
            Message::Pane(PaneMsg::FileLoaded(epoch, Some(Box::new(read))))
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
            if cancel.is_cancelled() {
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

    /// The pane: its bar over the open panel. One panel at a time; the
    /// header's toggles choose it.
    pub(super) fn pane_view(&self, context: &mut ViewContext<Self>) -> View {
        let body: Option<View> = match self.panel {
            Panel::Changes => Some(self.changes_view(context)),
            Panel::Agents => Some(self.tree_view(context)),
            Panel::Details => self.details_view(context),
            Panel::Terminal => Some(self.terminal_view(context)),
            Panel::Closed => None,
        };
        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((
                self.pane_bar(context),
                Border::new().grid_row(1).maybe_content(body),
            ))
            .into()
    }

    /// The panel widths shown: `[desktop] panels`, the pane at 720 while
    /// Wide is pressed.
    pub(super) fn panel_widths(&self) -> split_ui::PanelWidths {
        let saved = split_ui::PanelWidths::of(self.prefs.panels);
        if self.pane.wide {
            saved.with(split_ui::Edge::Pane, tokens::layout::PANEL_WIDE_WIDTH)
        } else {
            saved
        }
    }

    /// Widths a handle or Settings reports, as `[desktop] panels` keeps
    /// them: Wide's 720 is shown, never saved, so a report of it keeps the
    /// saved pane width; any other pane width ends Wide.
    pub(super) fn saved_widths(
        &mut self,
        reported: split_ui::PanelWidths,
    ) -> split_ui::PanelWidths {
        if !self.pane.wide {
            return reported;
        }
        if reported.pane == tokens::layout::PANEL_WIDE_WIDTH {
            let saved = split_ui::PanelWidths::of(self.prefs.panels);
            reported.with(split_ui::Edge::Pane, saved.pane)
        } else {
            self.pane.wide = false;
            reported
        }
    }

    /// The pane's content takes its wide layout: Full view, or the pane at
    /// its widest (Wide, or 720 in `[desktop] panels`).
    fn pane_wide(&self) -> bool {
        self.pane.full || self.panel_widths().pane >= tokens::layout::PANEL_WIDE_WIDTH
    }

    /// The pane's bar, over a hairline: the open panel's title, then Wide
    /// (the pane at 720 and back, in one step; only where the wide pane
    /// docks beside the main area), Full view (expand over the
    /// conversation, or back beside it; only where the pane can dock, since
    /// otherwise it already fills the main area), both toggles, and Close,
    /// always at the bar's end, all stock bar commands of one size. Close
    /// is Back to conversation when the pane lies over it.
    fn pane_bar(&self, context: &mut ViewContext<Self>) -> View {
        let panel = self.panel;
        let title = panel.label();
        let full = self.pane.full;
        // Wide and Full view are toggles: UIA reads their pressed state.
        let wide = self.panel_widths().pane >= tokens::layout::PANEL_WIDE_WIDTH;
        let widen: Option<View> = (self.layout.wide_docks && !full).then(|| {
            controls::bar_toggle(wide)
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .automation_name("Wide pane")
                .on_is_checked_changed(crate::views::check_changes(
                    context.callback(|on: bool| Message::Pane(PaneMsg::Wide(on))),
                ))
                .content(controls::icon(controls::glyph::WIDE_PANE))
                .tooltip(if wide {
                    "Put the pane back to its earlier width"
                } else {
                    "Widen the pane to 720"
                })
        });
        let expand: Option<View> = self.layout.docks.then(|| {
            controls::bar_toggle(full)
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .automation_name("Full view")
                .on_is_checked_changed(crate::views::check_changes(
                    context.callback(|on: bool| Message::Pane(PaneMsg::Full(on))),
                ))
                .content(controls::icon(if full {
                    controls::glyph::EXIT_FULL_VIEW
                } else {
                    controls::glyph::FULL_VIEW
                }))
                .tooltip(if full {
                    format!("Put {title} back beside the conversation")
                } else {
                    format!("Show {title} over the whole conversation")
                })
        });
        let close: View = controls::bar_button(false)
            .grid_column(3)
            .vertical_alignment(VerticalAlignment::Center)
            .automation_name(format!("Close {title}"))
            .on_click(context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Closed))))
            .content(controls::icon(controls::glyph::CLOSE))
            .tooltip(if self.pane_docked() {
                panel.with_shortcut(&format!("Close {title}"))
            } else {
                panel.with_shortcut(&format!("Close {title} and go back to the conversation"))
            });
        Border::new()
            .min_height(tokens::height::HEADER)
            .border_brush(ThemeBrush::CardStroke)
            .border_thickness(Thickness::new(0.0, 0.0, 0.0, tokens::layout::HAIRLINE))
            .padding(Thickness::xy(tokens::spacing::MD, tokens::spacing::XS))
            .content(
                Grid::new()
                    .columns([
                        GridLength::STAR,
                        GridLength::Auto,
                        GridLength::Auto,
                        GridLength::Auto,
                    ])
                    .column_spacing(tokens::spacing::XS)
                    .keyed_children(crate::views::slots([
                        Some(
                            controls::text(title, tokens::text::HEADER)
                                .automation_heading_level(AutomationHeadingLevel::Level2)
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis)
                                .into(),
                        ),
                        widen,
                        expand,
                        Some(close),
                    ])),
            )
            .into()
    }

    /// A tab's body (`.pane-r .body`): 12 DIPs in, its parts 12 apart, in
    /// a scroll view.
    pub(super) fn pane_body(&self, name: &str, items: Vec<KeyedView>) -> View {
        ScrollViewer::new()
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name(name)
            .content(
                StackPanel::new()
                    .spacing(tokens::spacing::INSET)
                    .margin(Thickness::uniform(tokens::spacing::INSET))
                    .keyed_children(items),
            )
            .into()
    }

    /// Changes (brand book §11.1): the source, Unified/Split, Wrap and
    /// Refresh; Filter files; the summary and what it compares; the review
    /// digest; the source's files from `agent.diff`; the selected file's
    /// actions and patch; Merge and check.
    fn changes_view(&self, context: &mut ViewContext<Self>) -> View {
        let changes = &self.pane.changes;
        let open = self.selected_agent_id();
        let read = changes.read.as_deref().filter(|_| changes.agent == open);
        let mut items: Vec<KeyedView> = Vec::new();

        let compare: Vec<KeyedView> = Source::ALL
            .iter()
            .map(|source| {
                let selected = changes.source == *source;
                let label = source.label();
                KeyedView::new(
                    format!("{label}:{selected}"),
                    controls::segment(selected)
                        .automation_name(if selected {
                            format!("{label} (chosen): {}", source.meaning())
                        } else {
                            format!("{label}: {}", source.meaning())
                        })
                        .on_click(context.message(Message::Pane(PaneMsg::Source(*source))))
                        .content(controls::segment_label(label))
                        .tooltip(source.meaning()),
                )
            })
            .collect();
        let read_at = read
            .and_then(|read| monitor::last_read_label(read.at_ms))
            .map_or_else(|| "Refresh".to_owned(), |at| format!("Refresh ({at})"));
        items.push(KeyedView::new(
            "compare",
            toolbar(
                vec![KeyedView::new("group", controls::segmented(compare))],
                vec![KeyedView::new(
                    "refresh",
                    controls::icon_button()
                        .is_enabled(!changes.reading)
                        .automation_name("Refresh changes")
                        .on_click(context.message(Message::Pane(PaneMsg::Refresh)))
                        .content(controls::icon(controls::glyph::REFRESH))
                        .tooltip(read_at),
                )],
            ),
        ));
        // Split needs the wide pane; elsewhere Unified shows and says why,
        // the choice kept for when the pane is wide again.
        let wide = self.pane_wide();
        let view_group: Vec<KeyedView> = [("Unified", false), ("Split", true)]
            .into_iter()
            .map(|(label, split)| {
                let selected = changes.split == split;
                let segment = controls::segment(selected)
                    .is_enabled(wide || !split)
                    .automation_name(match (selected, wide || !split) {
                        (_, false) => format!("{label}: {NO_SPLIT}"),
                        (true, true) => format!("{label} (chosen)"),
                        (false, true) => label.to_owned(),
                    })
                    .on_click(context.message(Message::Pane(PaneMsg::Split(split))))
                    .content(controls::segment_label(label));
                let view: View = if wide || !split {
                    segment.into()
                } else {
                    Border::new().content(segment).tooltip(NO_SPLIT)
                };
                KeyedView::new(format!("{label}:{selected}:{wide}"), view)
            })
            .collect();
        let filter = TextBox::new()
            .text(changes.filter.clone())
            .placeholder_text("Filter files")
            .automation_name("Filter files")
            .on_text_changed(crate::views::text_edits(
                context.callback(|text| Message::Pane(PaneMsg::Filter(text))),
            ));
        items.push(KeyedView::new(
            "view",
            toolbar(
                vec![
                    KeyedView::new("group", controls::segmented(view_group)),
                    KeyedView::new(
                        "wrap",
                        controls::bar_toggle(!changes.nowrap)
                            .automation_name("Wrap long lines")
                            .on_is_checked_changed(crate::views::check_changes(
                                context.callback(|on: bool| Message::Pane(PaneMsg::Wrap(on))),
                            ))
                            .content(controls::icon(controls::glyph::WRAP))
                            .tooltip(if changes.nowrap {
                                "Wrap long lines"
                            } else {
                                "Scroll long lines sideways"
                            }),
                    ),
                ],
                vec![KeyedView::new(
                    "filter",
                    Border::new()
                        .width(tokens::layout::FILTER_WIDTH)
                        .min_height(tokens::height::CONTROL)
                        .corner_radius(tokens::radius::CONTROL)
                        .border_brush(ThemeBrush::CardStroke)
                        .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
                        .padding(Thickness::new(tokens::spacing::ROW, 0.0, 0.0, 0.0))
                        .content(
                            Grid::new()
                                .columns([GridLength::Auto, GridLength::STAR])
                                .children((
                                    Border::new()
                                        .vertical_alignment(VerticalAlignment::Center)
                                        .content(controls::small_icon(controls::glyph::FILTER)),
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
        // The shown read is of another source until the chosen one lands.
        let diff = (read.source == changes.source).then_some(&read.diff);
        if git.is_none()
            && digest.is_none()
            && let Some(Err(error)) = diff
        {
            let why = [
                git_error.clone(),
                read.digest.as_ref().err().cloned(),
                Some(diff_error_words(changes.source, error)),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" · ");
            items.push(KeyedView::new(
                "failed",
                super::page_ui::read_failed(
                    format!("Couldn't read changes: {why}"),
                    "Retry",
                    !changes.reading,
                    context.message(Message::Pane(PaneMsg::Refresh)),
                ),
            ));
            return self.pane_body("Changes", items);
        }

        let unnamed = changes.source == Source::LastTurn && self.last_turn_unnamed();
        let shown = diff.and_then(|diff| diff.as_ref().ok());
        let work = self.open_work();
        let base = self
            .chat
            .detail
            .as_ref()
            .filter(|detail| Some(&detail.agent) == open.as_ref())
            .and_then(|detail| detail.base_ref.clone());
        // Line totals: the patch's own when every file's are known, else
        // the daemon's Branch totals; never a guessed zero.
        let lines = shown
            .filter(|diff| !diff.reply.truncated)
            .and_then(|diff| {
                diff.parsed.iter().try_fold((0, 0), |(add, del), parsed| {
                    parsed.counts.map(|(a, d)| (add + a, del + d))
                })
            })
            .or_else(|| {
                (changes.source == Source::Branch)
                    .then(|| {
                        git.and_then(|git| git.line_counts.as_ref())
                            .map(|counts| (counts.insertions, counts.deletions))
                            .or_else(|| work.and_then(|work| work.insertions.zip(work.deletions)))
                    })
                    .flatten()
            });
        items.push(KeyedView::new(
            format!("summary:{:?}", changes.source),
            summary_line(
                changes.source,
                shown.map(|diff| diff.reply.files.len()),
                lines,
                work.and_then(|work| work.commits_ahead),
                base.as_deref(),
                shown.is_some_and(|diff| diff.reply.truncated) || unnamed,
            ),
        ));
        if let Some(diff) = shown {
            items.push(KeyedView::new(
                "compared",
                controls::secondary(compared_words(changes.source, &diff.reply))
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ));
        }
        if let Some(error) = &git_error
            && changes.source == Source::Uncommitted
        {
            items.push(KeyedView::new(
                "git-error",
                super::page_ui::error_line(format!("Git status not read: {error}")),
            ));
        }
        if let Err(error) = &read.digest
            && changes.source != Source::Uncommitted
        {
            items.push(KeyedView::new(
                "digest-error",
                super::page_ui::error_line(format!("Review digest not read: {error}")),
            ));
        }
        // The digest reads the branch's patch against its base: it goes
        // with Branch, not with the other sources' narrower scopes.
        if let Some(digest) = digest.filter(|_| changes.source == Source::Branch) {
            items.push(KeyedView::new("digest", self.digest_card(digest, context)));
        }

        match diff {
            None => items.push(KeyedView::new(
                "files-reading",
                super::page_ui::quiet(format!("Reading {}\u{2026}", changes.source.label())),
            )),
            Some(Err(error)) => items.push(KeyedView::new(
                "files-error",
                super::page_ui::error_line(diff_error_words(changes.source, error)),
            )),
            Some(Ok(diff)) if diff.reply.files.is_empty() => items.push(KeyedView::new(
                "empty",
                super::page_ui::quiet(match changes.source {
                    _ if diff.reply.truncated => {
                        "Partial: this read lists no files. Refresh reads it again."
                    }
                    Source::LastTurn if unnamed => {
                        "The last turn's edits did not name their files: Branch and Uncommitted list them."
                    }
                    Source::LastTurn => {
                        "No current changes in the files the last completed turn reported. Files its commands changed show under Uncommitted and Branch."
                    }
                    _ => "No changes.",
                }),
            )),
            Some(Ok(diff)) => {
                let needle = changes.filter.trim().to_lowercase();
                let mut rows: Vec<KeyedView> = Vec::new();
                for (file, parsed) in diff.reply.files.iter().zip(&diff.parsed) {
                    if !needle.is_empty() && !file.path.to_lowercase().contains(&needle) {
                        continue;
                    }
                    let selected = changes.selected.as_deref() == Some(file.path.as_str());
                    let state = file_state(file, git, digest);
                    rows.push(KeyedView::new(
                        format!("file:{}:{selected}", file.path),
                        file_row(file, state, parsed.counts, selected, context),
                    ));
                }
                if rows.is_empty() {
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
                if let Some(path) = &changes.selected {
                    // A selected file that left the list is explained,
                    // never shown with its old patch.
                    items.push(match diff.file(path) {
                        Some((file, parsed)) => KeyedView::new(
                            format!("selected:{path}"),
                            self.selected_file(file, parsed, diff, git, digest, context),
                        ),
                        None => KeyedView::new(
                            "selected-gone",
                            super::page_ui::quiet(format!(
                                "{path} no longer differs under {}.",
                                changes.source.label()
                            )),
                        ),
                    });
                }
            }
        }
        items.push(KeyedView::new("merge", self.merge_view(context)));
        self.pane_body("Changes", items)
    }

    /// The selected file (§11.1): its path, state, counts and rename or
    /// mode; Open in editor, Open folder, Copy path and Copy diff; then its
    /// patch, or the short state that stands for it (binary, larger than
    /// one read, rename or mode only, empty, being read, gone, failed).
    fn selected_file(
        &self,
        listed: &GitDiffFile,
        listed_parsed: &Arc<diff_ui::Parsed>,
        diff: &Diff,
        git: Option<&eliot_core::api::GitObservation>,
        digest: Option<&AgentPatchDigestResponse>,
        context: &mut ViewContext<Self>,
    ) -> View {
        let changes = &self.pane.changes;
        let path = listed.path.as_str();
        // The source's read cut this file's patch: its own read stands in.
        let own = (listed.truncated && !listed.binary).then(|| {
            changes
                .file
                .read
                .as_ref()
                .filter(|_| changes.file.path.as_deref() == Some(path))
        });
        let shown: Result<(&GitDiffFile, &Arc<diff_ui::Parsed>), String> = match own {
            None => Ok((listed, listed_parsed)),
            Some(None) => Err("Reading this file's patch\u{2026}".to_owned()),
            Some(Some(Err(error))) => Err(diff_error_words(changes.source, error)),
            Some(Some(Ok(own))) => own.file(path).ok_or_else(|| {
                format!(
                    "{path} no longer differs under {}. Refresh reads the list again.",
                    changes.source.label()
                )
            }),
        };
        let (file, parsed) = shown
            .as_ref()
            .map_or((listed, listed_parsed), |shown| *shown);
        let state = file_state(file, git, digest);

        let mut facts = vec![state.to_owned()];
        if let Some((add, del)) = parsed.counts {
            facts.push(format!("+{add} \u{2212}{del}"));
        }
        if file.hunks > 0 {
            facts.push(eliot_core::count_words(file.hunks as u64, "hunk", "hunks"));
        }
        if let Some(original) = &file.original_path {
            facts.push(format!("from {original}"));
        }
        if file.old_mode != file.new_mode && file.old_mode != NO_FILE && file.new_mode != NO_FILE {
            facts.push(format!("mode {} to {}", file.old_mode, file.new_mode));
        }
        let facts = facts.join(" \u{b7} ");

        let whole = shown.is_ok() && !file.truncated;
        let actions = toolbar(
            vec![
                KeyedView::new(
                    "open",
                    controls::outlined()
                        .is_enabled(state != "Deleted")
                        .automation_name(format!("Open {path} in its editor"))
                        .on_click(
                            context.message(Message::Pane(PaneMsg::OpenFile(path.to_owned()))),
                        )
                        .content(controls::icon_label(
                            controls::glyph::EXTERNAL,
                            "Open in editor",
                        ))
                        .tooltip(if state == "Deleted" {
                            "The file is deleted"
                        } else {
                            "Open with its default app"
                        }),
                ),
                KeyedView::new(
                    "folder",
                    controls::text_button()
                        .automation_name(format!("Show {path} in File Explorer"))
                        .on_click(
                            context.message(Message::Pane(PaneMsg::OpenFolder(path.to_owned()))),
                        )
                        .content(controls::icon_label(
                            controls::glyph::OPEN_FOLDER,
                            "Open folder",
                        )),
                ),
                KeyedView::new(
                    "copy-path",
                    controls::text_button()
                        .automation_name(format!("Copy the path {path}"))
                        .on_click(
                            context.message(Message::Pane(PaneMsg::CopyPath(path.to_owned()))),
                        )
                        .content(controls::icon_label(controls::glyph::COPY, "Copy path")),
                ),
                KeyedView::new(
                    "copy-diff",
                    controls::text_button()
                        .is_enabled(whole)
                        .automation_name(if whole {
                            format!("Copy the diff of {path}")
                        } else {
                            format!("Copy diff: {path} has no whole patch here")
                        })
                        .on_click(
                            context.message(Message::Pane(PaneMsg::CopyDiff(path.to_owned()))),
                        )
                        .content(controls::icon_label(controls::glyph::COPY, "Copy diff")),
                ),
            ],
            vec![],
        );

        let mut parts: Vec<KeyedView> = vec![
            KeyedView::new(
                "path",
                controls::body(path.to_owned())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ),
            KeyedView::new(
                "facts",
                controls::secondary(facts).text_wrapping(TextWrapping::Wrap),
            ),
            KeyedView::new("actions", actions),
        ];
        let split = changes.split && self.pane_wide();
        let body: View = match &shown {
            Err(words) if matches!(own, Some(None)) => StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .children((controls::spinner(), controls::body(words.clone())))
                .into(),
            Err(words) => super::page_ui::error_line(words.clone()),
            Ok(_) if file.binary => super::page_ui::quiet(
                "Binary file: Swarm names it here and does not show its content.",
            ),
            Ok(_) if file.truncated => super::page_ui::quiet(format!(
                "This file's patch is larger than one read ({} KiB, {} files, {} hunks), so Swarm shows none of it here. Open in editor shows the whole file.",
                MAX_DIFF_TEXT_BYTES / 1024,
                MAX_DIFF_TEXT_FILES,
                MAX_DIFF_TEXT_HUNKS
            )),
            Ok(_) if file.hunks == 0 => super::page_ui::quiet(match state {
                "Renamed" => "Renamed with the same content.",
                "Mode changed" => "Only the file mode changed.",
                "Added" | "Untracked" => "A new empty file.",
                "Deleted" => "An empty file was deleted.",
                _ => "No text changes.",
            }),
            Ok((_, parsed)) => {
                if changes.split && !split {
                    parts.push(KeyedView::new(
                        "unified-why",
                        controls::secondary(format!("{NO_SPLIT}; Unified here."))
                            .text_wrapping(TextWrapping::Wrap),
                    ));
                }
                diff_ui::patch_view(
                    parsed,
                    split,
                    !changes.nowrap,
                    (
                        changes.epoch,
                        changes.file.epoch,
                        diff.reply.generation,
                        path,
                    ),
                )
            }
        };
        parts.push(KeyedView::new(
            format!("body:{split}:{}", changes.nowrap),
            body,
        ));
        controls::card()
            .content(
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .keyed_children(parts),
            )
            .into()
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

    /// The open conversation's last finished turn.
    fn last_finished_turn(&self) -> Option<&crate::views::RenderedItem> {
        use crate::chat::PartKind;
        self.chat
            .items
            .iter()
            .rev()
            .find(|item| {
                item.parts
                    .iter()
                    .any(|part| matches!(part.kind, PartKind::Final | PartKind::TurnFailed))
            })
            .map(Rc::as_ref)
    }

    /// Whether some of the last finished turn's edits did not name their
    /// files: its work log then says "made N edits" instead of "edited N
    /// files" (only some harnesses report changed paths), and Last turn,
    /// which takes the reported paths, cannot list them.
    fn last_turn_unnamed(&self) -> bool {
        use crate::chat::PartKind;
        self.last_finished_turn().is_some_and(|item| {
            item.parts.iter().any(|part| {
                matches!(part.kind, PartKind::Log)
                    && part.label.contains("made ")
                    && part.label.contains(" edit")
            })
        })
    }

    /// Review digest (component 63): one line of counts; a count opens its
    /// paths. "What to look at first", not a quality score.
    fn digest_card(
        &self,
        digest: &AgentPatchDigestResponse,
        context: &mut ViewContext<Self>,
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
                controls::segment(selected)
                    .automation_name(if selected {
                        format!("{words} (paths shown)")
                    } else {
                        format!("{words}: show the paths")
                    })
                    .on_click(context.message(Message::Pane(PaneMsg::DigestGroup(group))))
                    .content(controls::chip_content(
                        selected.then_some(controls::glyph::CHECK),
                        &words,
                        false,
                    )),
            ));
        }
        let mut parts: Vec<KeyedView> = vec![KeyedView::new(
            "head",
            controls::text("Review digest", tokens::text::SMALL_TITLE)
                .automation_heading_level(AutomationHeadingLevel::Level3),
        )];
        if chips.is_empty() {
            parts.push(KeyedView::new(
                "none",
                controls::secondary("Nothing stands out in this patch."),
            ));
        } else {
            let per_row = if self.pane_wide() { 4 } else { 2 };
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
                    controls::body(
                        entries
                            .iter()
                            .map(|entry| {
                                if entry == "." {
                                    "Repository root"
                                } else {
                                    entry.as_str()
                                }
                            })
                            .collect::<Vec<_>>()
                            .join("\n"),
                    )
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
            controls::secondary(caption).text_wrapping(TextWrapping::Wrap),
        ));
        controls::card()
            .padding(Thickness::xy(tokens::spacing::MD, tokens::spacing::INSET))
            .content(
                StackPanel::new()
                    .spacing(tokens::spacing::CHIP_GAP)
                    .keyed_children(parts),
            )
            .into()
    }

    /// Merge and check (`git.integrate`): the button, its question, the
    /// running state and the result card.
    fn merge_view(&self, context: &mut ViewContext<Self>) -> View {
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
                controls::card()
                    .content(StackPanel::new().spacing(tokens::spacing::SM).children((
                        controls::body(what).text_wrapping(TextWrapping::Wrap),
                        toolbar(
                            vec![],
                            vec![
                                KeyedView::new(
                                    "cancel",
                                    controls::text_button()
                                        .automation_name("Cancel merge and check")
                                        .on_click(
                                            context.message(Message::Pane(PaneMsg::MergeAnswered(
                                                false,
                                            ))),
                                        )
                                        .content(controls::label("Cancel")),
                                ),
                                KeyedView::new(
                                    "go",
                                    controls::filled()
                                        .is_enabled(request.is_ok())
                                        .automation_name("Merge and check")
                                        .on_click(
                                            context.message(Message::Pane(PaneMsg::MergeAnswered(
                                                true,
                                            ))),
                                        )
                                        .content(controls::label("Merge and check")),
                                ),
                            ],
                        ),
                    )))
                    .into()
            }
            Merge::Running(agent) if *agent == open => controls::card()
                .content(
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .children((
                            controls::spinner(),
                            controls::body("Merging and running the project check…"),
                        )),
                )
                .into(),
            Merge::Done(agent, result) if *agent == open => merge_result(result, context),
            _ => {
                let button = controls::tonal()
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
                                controls::small_icon(controls::glyph::MERGE),
                                controls::text("Merge and check…", tokens::text::BUTTON)
                                    .vertical_alignment(VerticalAlignment::Center),
                            )),
                    );
                let button: View = match request {
                    Ok(_) => button.into(),
                    Err(why) => Border::new().content(button).tooltip(why),
                };
                toolbar(vec![], vec![KeyedView::new("merge", button)])
            }
        }
    }

    /// Agents (skeleton 5.5, component 49): this task's manager and its
    /// workers nested, each with its state, name, model, activity and
    /// +/-; Open, Stop Agent and Hand off. Native subagents are observed.
    fn tree_view(&self, context: &mut ViewContext<Self>) -> View {
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
                    controls::secondary(if current && tree.read {
                        eliot_core::count_words(workers as u64, "worker", "workers")
                    } else {
                        String::new()
                    })
                    .vertical_alignment(VerticalAlignment::Center),
                )],
                vec![KeyedView::new(
                    "refresh",
                    controls::icon_button()
                        .automation_name("Refresh Agents")
                        .on_click(context.message(Message::Pane(PaneMsg::Refresh)))
                        .content(controls::icon(controls::glyph::REFRESH))
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
                    format!("Couldn't read this task's Agents: {error}"),
                    "Retry",
                    true,
                    context.message(Message::Pane(PaneMsg::Refresh)),
                ),
            ));
        }
        let now = tree.read_at_ms.unwrap_or_else(crate::handoff::now_ms);
        let selected = self.selected_agent_id();
        let one_line = self.pane_wide();
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
                &entry,
                depth,
                (!children.is_empty()).then_some(!folded),
                one_line,
                context,
            ),
        ));
        if !folded && depth < 6 {
            for child in children {
                self.tree_rows(child, depth + 1, now, selected, one_line, context, rows);
            }
        }
    }

    /// Terminal (skeleton 5.6): the worktree with Open in Windows Terminal,
    /// and the Agent's processes, read-only. No embedded terminal until
    /// that dependency is decided (skeleton 9.3).
    fn terminal_view(&self, context: &mut ViewContext<Self>) -> View {
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
                                .content(controls::muted_icon(controls::glyph::FOLDER)),
                            controls::body(shown.clone())
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis)
                                .is_text_selection_enabled(true),
                            controls::tonal()
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
                                            controls::small_icon(controls::glyph::TERMINAL),
                                            controls::text(
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
                super::page_ui::error_line(note.clone()),
            ));
        }
        items.push(KeyedView::new(
            "processes-head",
            toolbar(
                vec![KeyedView::new(
                    "title",
                    controls::text("Agent processes", tokens::text::SMALL_TITLE)
                        .automation_heading_level(AutomationHeadingLevel::Level3)
                        .vertical_alignment(VerticalAlignment::Center),
                )],
                vec![KeyedView::new(
                    "refresh",
                    controls::icon_button()
                        .is_enabled(!terminal.reading)
                        .automation_name("Refresh Agent processes")
                        .on_click(context.message(Message::Pane(PaneMsg::Refresh)))
                        .content(controls::icon(controls::glyph::REFRESH))
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
                            eliot_core::ProcessState::Running => "Running · seen".to_owned(),
                            eliot_core::ProcessState::Exited => match process.exit_code {
                                Some(code) => format!("Exited · code {code} ·"),
                                None => "Exited ·".to_owned(),
                            },
                            eliot_core::ProcessState::Unknown => "State unknown ·".to_owned(),
                        };
                        let age = monitor::elapsed_label(now.saturating_sub(process.updated_at));
                        KeyedView::new(
                            format!("{}:{}", process.identity.pid, process.identity.start_time),
                            super::page_ui::kv_text(
                                &format!("{} · {}", process.role, process.identity.pid),
                                format!("{state} {age} ago"),
                            ),
                        )
                    })
                    .collect();
                items.push(KeyedView::new(
                    "processes",
                    controls::card().content(
                        StackPanel::new()
                            .spacing(tokens::spacing::XS)
                            .keyed_children(rows),
                    ),
                ));
                // `next` names the last row even on the last page: a full
                // page is what says more may follow.
                if reply.processes.len() >= eliot_core::api::AgentProcessesRequest::new("").limit {
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
    ) -> View {
        let open = self.pane.section_open(key);
        let head = controls::row(false)
            .automation_name(format!(
                "{title} ({})",
                if open { "expanded" } else { "collapsed" }
            ))
            .on_click(context.message(Message::Pane(PaneMsg::Section(key))))
            .content(
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .children((
                        controls::text(title.to_owned(), tokens::text::SMALL_TITLE)
                            .automation_heading_level(AutomationHeadingLevel::Level3)
                            .vertical_alignment(VerticalAlignment::Center),
                        Border::new()
                            .grid_column(1)
                            .vertical_alignment(VerticalAlignment::Center)
                            .content(controls::muted_icon(if open {
                                controls::glyph::CHEVRON_DOWN
                            } else {
                                controls::glyph::CHEVRON_RIGHT
                            })),
                    )),
            );
        let mut parts = vec![KeyedView::new(format!("head:{open}"), head)];
        if open {
            parts.extend(body);
        }
        controls::card()
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
            .into()
    }

    /// Details › Goal's one action, Goal… (the name the header and the
    /// task menu use), which opens the Goal dialog ([`super::goal_ui`])
    /// with Save, Pause, Resume and Clear as the daemon allows them.
    pub(super) fn goal_action(
        &self,
        agent: &str,
        has_goal: bool,
        context: &mut ViewContext<Self>,
    ) -> KeyedView {
        KeyedView::new(
            "goal-action",
            Border::new()
                .horizontal_alignment(HorizontalAlignment::Left)
                .content(
                    controls::outlined()
                        .automation_name("Goal")
                        .on_click(context.message(Message::Goal(super::goal_ui::GoalMsg::Open(
                            agent.to_owned(),
                        ))))
                        .content(controls::label("Goal…"))
                        .tooltip(if has_goal {
                            "Edit, pause or clear this task's goal"
                        } else {
                            "Set a goal this task keeps working toward"
                        }),
                ),
        )
    }
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
        .into()
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

/// Why Split is not offered in a narrow pane.
const NO_SPLIT: &str = "Split needs the wide pane: Wide or Full view shows it";

/// The Git mode of a side that has no file (an added file's old side, a
/// deleted file's new side).
const NO_FILE: &str = "000000";

/// A file's state word from its `agent.diff` record: Renamed, Added,
/// Deleted, Mode changed or Modified, with Git's Untracked or Conflicted
/// where the same read's status or digest names the path so.
fn file_state(
    file: &GitDiffFile,
    git: Option<&eliot_core::api::GitObservation>,
    digest: Option<&AgentPatchDigestResponse>,
) -> &'static str {
    let porcelain = git
        .and_then(|git| git.dirty.as_ref())
        .and_then(|dirty| dirty.iter().find(|entry| entry.path == file.path))
        .map(|entry| porcelain_word(&entry.status));
    let change = digest
        .and_then(|digest| digest.files.iter().find(|entry| entry.path == file.path))
        .map(|entry| change_word(&entry.change));
    if file.original_path.is_some() {
        "Renamed"
    } else if porcelain == Some("Conflicted") {
        "Conflicted"
    } else if file.old_mode == NO_FILE {
        if porcelain == Some("Untracked") || change == Some("Untracked") {
            "Untracked"
        } else {
            "Added"
        }
    } else if file.new_mode == NO_FILE {
        "Deleted"
    } else if file.old_mode != file.new_mode && file.hunks == 0 && !file.binary {
        "Mode changed"
    } else {
        "Modified"
    }
}

/// What the read compared, from the reply's own base and HEAD (§11.1's
/// table): Branch without a recorded base says it compared with HEAD.
fn compared_words(source: Source, reply: &AgentDiffResponse) -> String {
    let short = |oid: &str| oid.chars().take(7).collect::<String>();
    let (base, head) = (short(&reply.base), short(&reply.head));
    match source {
        Source::Uncommitted => {
            format!("HEAD {head} to the worktree, staged and unstaged changes.")
        }
        Source::Branch if reply.base == reply.head => {
            format!("No recorded base: HEAD {head} to the worktree.")
        }
        Source::Branch => format!("Base {base} to the worktree (HEAD {head})."),
        Source::LastTurn => format!(
            "Current changes in files reported by the last completed turn, from base {base}. Not that turn's own historical patch."
        ),
    }
}

/// A failed `agent.diff` in words: the states §11.1 names get their own
/// (no finished turn, a newer run, a checkout that moved during the read).
fn diff_error_words(source: Source, error: &eliot_core::Error) -> String {
    match error.code.as_str() {
        "not_found" if source == Source::LastTurn => {
            "No turn has finished in this run yet, so Last turn has no files.".to_owned()
        }
        "stale_generation" => {
            "The task started a new run since this list was read. Refresh reads the new run."
                .to_owned()
        }
        "stale_workspace" => {
            "The worktree changed while it was being read. Refresh reads it again.".to_owned()
        }
        _ => format!("Couldn't read the diff: {}", error.user_message()),
    }
}

/// The summary line: "{N} files changed +A -D · {commits} commits ahead of
/// {base}", with Partial when the read was cut. Counts not known for a
/// source are left out (`None`), never shown as zero.
fn summary_line(
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
        (Source::LastTurn, Some(0)) => "No current changes in the last turn's files".to_owned(),
        (Source::LastTurn, Some(count)) => {
            format!(
                "{} of the last turn changed",
                eliot_core::count_words(count as u64, "file", "files")
            )
        }
        (_, Some(count)) => format!(
            "{} changed",
            eliot_core::count_words(count as u64, "file", "files")
        ),
    };
    parts.push(KeyedView::new("files", controls::secondary(files_words)));
    if let Some((add, del)) = lines {
        parts.push(KeyedView::new("lines", controls::diff_counts(add, del)));
    }
    if let Some(ahead) = ahead {
        let base = base.map_or_else(String::new, |base| format!(" of {base}"));
        parts.push(KeyedView::new(
            "ahead",
            controls::secondary(format!(
                "\u{b7} {} ahead{base}",
                eliot_core::count_words(ahead, "commit", "commits")
            )),
        ));
    }
    if partial {
        parts.push(KeyedView::new("partial", controls::badge("Partial")));
    }
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::XS)
        .keyed_children(parts)
        .into()
}

/// A changed file (component 48, `.filelist .f`): its icon, the name
/// bright and the folder quiet, its `+A −D` when known and its state word;
/// selected on the selection role.
fn file_row(
    file: &GitDiffFile,
    state: &str,
    counts: Option<(u64, u64)>,
    selected: bool,
    context: &mut ViewContext<DesktopApp>,
) -> View {
    let path = file.path.as_str();
    let state = if file.binary {
        format!("{state}, binary")
    } else {
        state.to_owned()
    };
    let (folder, name) = match path.rsplit_once('/') {
        Some((folder, name)) => (folder, name),
        None => ("", path),
    };
    let glyph = match state.split(',').next() {
        Some("Added" | "Untracked") => controls::glyph::NEW_FILE,
        Some("Deleted") => controls::glyph::DELETE,
        _ => controls::glyph::EDIT,
    };
    let counted = counts.map_or_else(String::new, |(add, del)| {
        format!(", {add} added, {del} removed")
    });
    let tooltip = match &file.original_path {
        Some(original) => format!("{path} (from {original})"),
        None => path.to_owned(),
    };
    controls::row(selected)
        .automation_name(format!(
            "{tooltip}, {state}{counted}{}",
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
                    GridLength::Auto,
                ])
                .column_spacing(tokens::spacing::SM)
                .children((
                    Border::new()
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(controls::small_icon(glyph)),
                    controls::text(name.to_owned(), tokens::text::BUTTON)
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                    controls::text(folder.to_owned(), tokens::text::BODY)
                        .grid_column(2)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                    Border::new()
                        .grid_column(3)
                        .vertical_alignment(VerticalAlignment::Center)
                        .maybe_content(counts.map(|(add, del)| controls::diff_counts(add, del))),
                    controls::text(state, tokens::text::CAPTION)
                        .grid_column(4)
                        .vertical_alignment(VerticalAlignment::Center),
                )),
        )
        .tooltip(tooltip)
}

/// Merge and check's result card: merged and checked, conflicting paths,
/// a failed check, or why it did not run.
fn merge_result(
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
    let title_view = controls::text(title.clone(), tokens::text::SMALL_TITLE);
    let title_view = if failed {
        title_view.foreground(ThemeBrush::SystemCritical)
    } else {
        title_view
    };
    controls::card()
        .content(
            StackPanel::new().spacing(tokens::spacing::XS).children((
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .children((
                        title_view.vertical_alignment(VerticalAlignment::Center),
                        controls::icon_button()
                            .grid_column(1)
                            .automation_name("Dismiss the merge result")
                            .on_click(context.message(Message::Pane(PaneMsg::MergeDismissed)))
                            .content(controls::icon(controls::glyph::CLOSE)),
                    )),
                controls::body(lines.join("\n"))
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .automation_name(format!("{title}: {}", lines.join(" "))),
            )),
        )
        .into()
}

/// One Agents tab row (component 49): its fold chevron, then the row (its
/// state mark, name, "observed" for a native subagent, model, activity and
/// +/-), which opens its conversation, then Stop Agent and Hand off. In
/// the Normal width the model and activity take a second line.
fn tree_row(
    entry: &monitor_view::RowEntry,
    depth: usize,
    children: Option<bool>,
    one_line: bool,
    context: &mut ViewContext<DesktopApp>,
) -> View {
    let row = &entry.row;
    let counts: Option<View> = row
        .work
        .as_ref()
        .and_then(|work| work.insertions.zip(work.deletions))
        .map(|(add, del)| controls::diff_counts(add, del));
    let mut name: Vec<KeyedView> = vec![
        KeyedView::new(
            "mark",
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .maybe_content(super::page_ui::agent_mark(
                    row.state,
                    row.status_line.as_deref(),
                )),
        ),
        KeyedView::new(
            "title",
            controls::text(entry.title.clone(), tokens::text::BODY)
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
                .content(controls::badge("observed")),
        ));
    }
    let model = controls::text(entry.model.clone(), tokens::text::CAPTION)
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis);
    let activity = controls::text(entry.activity.clone(), tokens::text::CAPTION)
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis);
    let activity = if entry.attention {
        activity.foreground(ThemeBrush::AccentText)
    } else {
        activity
    };
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
                    .maybe_content(counts),
            ))
            .into()
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
                            .maybe_content(counts),
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
            .into()
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
    let button = controls::row(entry.selected)
        .grid_column(1)
        .height(height)
        .min_height(height)
        .automation_name(format!("Open {words}"))
        .on_click(context.message(Message::Pane(PaneMsg::TreeOpen(row.id.clone()))))
        .content(content)
        .tooltip(words.clone());
    let chevron: Option<View> = children.map(|open| {
        controls::icon_button()
            .automation_name(format!(
                "{} the workers of {}",
                if open { "Hide" } else { "Show" },
                entry.title
            ))
            .on_click(context.message(Message::Pane(PaneMsg::TreeFold(row.id.clone()))))
            .content(controls::small_icon(if open {
                controls::glyph::CHEVRON_DOWN
            } else {
                controls::glyph::CHEVRON_RIGHT
            }))
            .into()
    });
    let stop: Option<View> = entry.stop.map(|state| {
        controls::icon_button()
            .is_enabled(state == monitor_view::StopButton::Ready)
            .automation_name(match state {
                monitor_view::StopButton::Stopping => format!("Stopping {}", entry.title),
                _ => format!("Stop Agent {} (run {})", entry.title, row.generation),
            })
            .on_click(context.message(Message::Pane(PaneMsg::TreeStop(
                row.id.clone(),
                row.generation,
            ))))
            .content(controls::icon(controls::glyph::STOP_CIRCLE))
            .tooltip("Stop Agent")
    });
    let handoff: Option<View> = if entry.observed {
        None
    } else {
        Some(
            controls::icon_button()
                .automation_name(format!("Hand off {}", entry.title))
                .on_click(context.message(Message::Pane(PaneMsg::TreeHandoff(
                    row.id.clone(),
                    row.generation,
                ))))
                .content(controls::icon(controls::glyph::FORWARD))
                .tooltip("Hand off or switch model"),
        )
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
                .maybe_content(chevron),
            button,
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .maybe_content(stop),
            Border::new()
                .grid_column(3)
                .vertical_alignment(VerticalAlignment::Center)
                .maybe_content(handoff),
        ))
        .into()
}
