//! The one shell (`screens.css`): on the window's base, the sidebar (New
//! task, the places Search, Inbox, Agents and Automations, the projects
//! with their tasks and workers, Chats, then Show archived, Usage and
//! Settings) and one card holding the main area (a conversation, a project
//! page or Settings) and the optional right pane (the fleet view, the Inbox
//! or the open Agent's details).

use std::sync::mpsc::{Receiver, RecvTimeoutError};

use super::sidebar_ui::{
    AgentsEntry, InboxEntry, PlaceEntry, Renaming, SearchField, SidebarInput, TaskEntry,
};
use super::*;
use crate::controls::StatusTone;
use crate::nav::{Branch, Checkout, Fleet, Row, Shown};
use crate::settings::{Startup, ThemeChoice};
use crate::shortcuts::{Input, Shortcut};

/// What the main area shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Main {
    Conversation,
    /// A project's page: its tasks, worktrees, check and automations.
    Project,
    /// The Owner inbox: what waits for the owner's decision.
    Inbox,
    /// The fleet: every Agent as a tree, with the summary and autonomy.
    Agents,
    /// Rules, timers and brief queues, and the scripts.
    Automations,
    Settings,
}

/// A place Back and Forward return to (Codex's history): the new-task
/// view in a project, a conversation, a project page, a Settings page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Location {
    /// The new-task view: its project's checkout, or a chat with no project.
    Home {
        checkout: String,
        chat: bool,
    },
    Agent(String),
    /// A project page, by its sidebar key.
    Project(String),
    /// Inbox, Agents or Automations.
    Place(page_ui::Place),
    Settings(settings_ui::Page),
}

/// A step to an Agent, waiting for its read: the step is taken only when
/// the daemon has answered and the owner is still where they pressed it.
#[derive(Clone, Debug)]
pub(super) struct PlaceRead {
    epoch: u64,
    from: Option<Location>,
    forward: bool,
    /// Further presses of the same direction while this read runs.
    more: u32,
}

/// The right pane's open tab (skeleton 5.4-5.6), or no pane. The pane
/// belongs to the open conversation; the header's toggles open a tab, and
/// the pressed toggle closes the pane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Panel {
    Closed,
    /// The task's changes: Uncommitted, Branch or Last turn, the review
    /// digest and the files.
    Changes,
    /// The task's tree: its manager and workers.
    Agents,
    /// The open conversation's Agent, in collapsible sections.
    Details,
    /// The worktree in Windows Terminal and the Agent's processes.
    Terminal,
}

impl Panel {
    /// The tabs in the pane's order.
    pub(super) const TABS: [Self; 4] = [Self::Changes, Self::Agents, Self::Details, Self::Terminal];

    pub(super) fn label(self) -> &'static str {
        match self {
            Self::Closed => "",
            Self::Changes => "Changes",
            Self::Agents => "Agents",
            Self::Details => "Details",
            Self::Terminal => "Terminal",
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum ShellMsg {
    AutoSettings,
    RepairSettings,
    /// Start the daemon again on the same data directory.
    RetryStartup,
    /// A daemon its owner stopped: ask now whether it runs again (the
    /// list's cadence asks too), and reconnect if it does.
    RecheckLink,
    /// Open this data directory's editable daemon configuration.
    OpenStartupConfig,
    /// Open the daemon's log (`logs\daemon.log`).
    OpenStartupLog,
    /// Show the data directory in File Explorer.
    OpenDataFolder,
    /// The daemon runs (started when needed), the opened checkout was
    /// discovered, and the settings were read.
    DaemonReady(MutationResult<(Option<String>, eliot_core::api::SettingsReadResponse, bool)>),
    /// What `daemon.status` found after a failed read (or while stopped).
    LinkProbed(super::link_ui::Probe),
    /// The owner chose another existing Swarm data directory.
    ChooseDataDirectory,
    /// The owner asked for a new, empty private data directory (the last
    /// choice): confirm first.
    UseFreshDirectory,
    /// The new empty data directory was confirmed (or cancelled).
    FreshConfirmed(bool),
    /// A chosen data directory, checked on a worker, to start on (or why
    /// not).
    CandidateChecked(Result<PathBuf, String>),
    /// The new data directory runs: whether it was remembered.
    RootRemembered(PathBuf, Result<(), String>),
    ProjectsLoaded(Fallible<Vec<UiProject>>),
    /// The sidebar refresh's `project.list` ([`DesktopApp::poll_projects`]):
    /// each listed project's `policy_usage` and Git state are taken from
    /// it; the list, its order and the selection stay.
    ProjectsPolled(Fallible<Vec<UiProject>>),
    /// The sidebar's grouping readback (`chat.organization` groups with
    /// their headers and totals, and the `organization.changed` rows since
    /// the cursor). A failed read keeps the last server grouping with a
    /// stale marker.
    OrganizationLoaded(Box<nav::Organization>),
    /// The sidebar's queue state (`launch.queue` waits and this window's
    /// watched intents), polled independently of the grouping read.
    QueueLoaded(Box<super::QueueRead>),
    AgentsLoaded(u64, bool, Fallible<AgentPage>),
    /// A first-recent-page refresh: merge the rows into the loaded history
    /// (a launch while the desktop is open arrives here, without dropping
    /// appended older pages or needing a manual refresh).
    HeadLoaded(u64, Fallible<AgentPage>),
    /// The live and attention rows, fetched independently of the history
    /// page: merge them into the loaded Agents.
    AttentionLoaded(u64, Fallible<AgentPage>),
    /// The archived counts a refresh read (its list epoch).
    ArchivedLoaded(u64, Fallible<nav::Archived>),
    /// The Monitor's attention reports, read on the sidebar's cadence
    /// while the panel is closed (the cursor the read started from).
    AttentionReports(Option<i64>, Fallible<monitor::AttentionRead>),
    /// The replies of one sidebar refresh, together: one window render.
    Refreshed(Vec<ShellMsg>),
    /// An inbox read that replied with the sidebar's refresh.
    Inbox(super::inbox_ui::InboxMsg),
    /// A sidebar-search page (`agent.list` with `query`): its read epoch,
    /// the query it answers, whether it appends, and the page.
    SearchLoaded(u64, String, bool, Fallible<AgentPage>),
    /// A sidebar row: open this Agent's conversation.
    AgentOpened(UiAgent),
    /// A project's new-task button: a new task in this checkout.
    ProjectPicked(String),
    /// A project's name: open its page, its tasks unfolded in the sidebar.
    /// Carries the checkout and the section key.
    ProjectOpened(String, String),
    /// Back or Forward's read of one Agent by id: the step's epoch, its
    /// direction, the Agent and what the daemon said.
    PlaceRead(u64, bool, String, Box<MutationResult<UiAgent>>),
    /// A project's chevron: collapse or expand the project (its section key).
    ProjectToggled(String),
    /// A project's "Show N older" row: unfold or fold its older tasks.
    OlderToggled(String),
    /// The attention row: list only the Agents that need attention, or all.
    /// The search box's text (typed, or a suggestion WinUI wrote into it).
    SearchChanged(String),
    /// Open the Search row into its field (true), or close and clear it.
    SearchToggled(bool),
    /// A search suggestion chosen (clicked, or reached with the arrows).
    SearchChosen(String),
    /// A chord, a popover key or Enter from the keyboard hook.
    Shortcut(Shortcut),
    /// Try focusing the search box again (tries left): WinUI refused it,
    /// as it does right after the window is activated, or the box did not
    /// exist yet (Settings were open).
    FocusSearch(u8),
    /// Wait, then send [`ShellMsg::FocusSearch`] with these tries.
    FocusSearchLater(u8),
    /// Focus what a quiet start held back, now that the window is the
    /// foreground window (tries left; WinUI refuses focus right after an
    /// activation).
    FocusHeld(FocusTarget, u8),
    /// Wait, then send [`ShellMsg::FocusHeld`] with these tries.
    FocusHeldLater(FocusTarget, u8),
    /// The shortcut watcher stopped (window closing): nothing to do.
    ShortcutsEnded,
    RefreshRequested,
    /// The new-task view (the app title, New task, Ctrl+N).
    NewTask,
    /// One step back or forward through the places visited.
    Back,
    Forward,
    MoreAgentsRequested,
    /// The search's Show more row: the next page of the server's matches.
    MoreSearchRequested,
    SettingsOpened,
    /// Open Settings at one page (the sidebar's Automations and Usage).
    SettingsPage(settings_ui::Page),
    /// Open Inbox, Agents or Automations in the main card (the sidebar's
    /// places).
    PlaceOpened(page_ui::Place),
    /// The Projects label's +: Settings › Projects, and its folder picker
    /// once the settings document is read.
    AddProject,
    /// The Usage popover's Refresh: read the limits and holds again.
    UsageRefresh,
    /// `provider.holds` for the Usage popover.
    HoldsLoaded(Box<Fallible<eliot_core::api::ProviderHoldsResponse>>),
    /// The Settings navigation's search field.
    SettingsSearch(String),
    /// The Settings navigation's Back row: the conversation again.
    SettingsClosed,
    PanelToggled(Panel),
    /// The size class or the docking result changed (`split_ui`).
    Layout(split_ui::Layout),
    /// A panel handle was released, or a Settings › Panel widths slider
    /// moved, at these widths.
    PanelsResized(split_ui::PanelWidths),
    /// Settings › Appearance: the default panel widths again.
    PanelsReset,
    /// The compact window's navigation pane closed (light dismiss).
    OverlayClosed,
    ThemeChosen(ThemeChoice),
    StartupChosen(Startup),
    /// The re-created editors of this re-prime exist: give them their text.
    FieldsPrimed(u64),
    /// Expand (true) or collapse an Agent's children.
    ExpandToggled(String, bool),
    /// Tree epoch, manager id and its direct-children page.
    BranchLoaded(u64, String, Fallible<AgentPage>),
    /// The preferences written to `[desktop]` (or not).
    PreferencesSaved(
        crate::settings::Preferences,
        MutationResult<eliot_core::api::SettingsSaveResponse>,
    ),
    AlertDismissed,
    /// Re-read the listed Agents (states, Recents order).
    ListTick,
    /// Hide the sidebar to a rail of icons, or show it again.
    SidebarToggled,
}

impl ShellMsg {
    /// A sidebar refresh's reply that failed to read (its next refresh
    /// waits longer).
    fn read_failed(&self) -> bool {
        use super::inbox_ui::InboxMsg;
        match self {
            Self::HeadLoaded(_, page)
            | Self::AttentionLoaded(_, page)
            | Self::SearchLoaded(_, _, _, page) => page.is_err(),
            Self::ArchivedLoaded(_, read) => read.is_err(),
            Self::AttentionReports(_, read) => read.is_err(),
            Self::OrganizationLoaded(read) => read.stale,
            Self::QueueLoaded(read) => !read.errors.is_empty(),
            Self::ProjectsPolled(read) => read.is_err(),
            Self::Inbox(InboxMsg::Status(read)) => read.is_err(),
            Self::Inbox(InboxMsg::Loaded(_, read)) => read.is_err(),
            _ => false,
        }
    }
}

/// Re-check for the empty editor render one frame later.
const PRIME_RETRY_MS: u64 = 16;
/// The sidebar's padding around its rows.
const SIDEBAR_PADDING: f64 = tokens::spacing::SM;
/// Focus attempts for Ctrl+K and for focus held back until the window is
/// the foreground window, and the wait between them: right after a cold
/// start or an activation WinUI refuses focus until the content is ready.
const FOCUS_TRIES: u8 = 20;
const FOCUS_RETRY_MS: u64 = 100;
/// A suggestion WinUI reports as chosen this soon after ↑/↓ was reached
/// with the arrows: it is only highlighted.
const ARROW_CHOICE_MS: u64 = 500;
/// How long the shortcut watcher waits before checking for cancellation.
const SHORTCUT_POLL_MS: u64 = 500;
/// The sidebar's states and Recents follow the daemon at this cadence.
const LIST_REFRESH_MS: u64 = 10_000;
/// One attention fetch carries at most this many live and attention rows.
const ATTENTION_PAGE: usize = 1000;
/// The attention fetch states: the running ones plus every state that needs
/// the owner (waiting, failed, lost, unknown), read independently of the
/// history page so an old waiting Agent is never cut off by newer history.
const ATTENTION_STATES: [eliot_core::AgentState; 6] = [
    eliot_core::AgentState::Busy,
    eliot_core::AgentState::Starting,
    eliot_core::AgentState::Waiting,
    eliot_core::AgentState::Failed,
    eliot_core::AgentState::Lost,
    eliot_core::AgentState::Unknown,
];

/// The window's theme: WinUI's own controls, theme resources and the
/// system title bar follow it; under High Contrast Windows' colors apply.
fn window_theme(choice: ThemeChoice) -> WindowTheme {
    match choice {
        ThemeChoice::System => WindowTheme::System,
        ThemeChoice::Light => WindowTheme::Light,
        ThemeChoice::Dark => WindowTheme::Dark,
    }
}

/// Projects, the listed Agents, expanded branches, titles and the selection.
/// One sidebar read, run on a worker ([`DesktopApp::spawn_read`]).
pub(super) type SidebarRead = Box<dyn FnOnce() -> ShellMsg + Send>;

pub(super) struct ShellState {
    /// The reads of the sidebar refresh being collected
    /// ([`DesktopApp::spawn_read`]).
    pub(super) batch: Option<Vec<SidebarRead>>,
    /// `project.list` reads in flight ([`DesktopApp::load_projects`] and
    /// [`DesktopApp::poll_projects`]): a poll waits while any is.
    pub(super) project_reads: u32,
    /// The alert of the failing `project.list` streak: a poll repeats no
    /// alert while the error stays the same; a new error alerts, and a read
    /// that succeeds clears it.
    pub(super) projects_error: Option<String>,
    pub(super) selection: Selection,
    pub(super) projects: Vec<UiProject>,
    /// The sidebar's projects as the daemon groups and counts them.
    pub(super) organization: nav::Organization,
    /// An organization read is in flight (one at a time).
    pub(super) organization_loading: bool,
    /// A queue-state read is in flight (one at a time, independent of the
    /// grouping read).
    pub(super) queue_loading: bool,
    /// Listed pages, in daemon order.
    pub(super) agents: Vec<UiAgent>,
    /// Direct-children reads of expanded managers.
    pub(super) branches: HashMap<String, Branch>,
    pub(super) expanded: HashSet<String>,
    /// Tags branch reads; a reply from before a refresh is dropped.
    pub(super) tree_epoch: u64,
    pub(super) agents_next: Option<String>,
    pub(super) agents_total: usize,
    /// Archived Agents the list leaves out (the fleet line counts the
    /// live ones); empty while Show archived lists them.
    pub(super) archived: nav::Archived,
    pub(super) agents_loading: bool,
    /// A list read is in flight (a reload or Show more's next page): Show
    /// more is disabled meanwhile. The background refresh of the head
    /// leaves it as it is, so an unchanged list does not redraw the
    /// sidebar.
    pub(super) paging: bool,
    /// Tags agent-list loads; a reply for an older request is dropped.
    pub(super) list_epoch: u64,
    /// The state totals the last refresh read (`state_totals` over the
    /// sidebar's scope): a change says what the next refreshes read.
    pub(super) list_totals: Option<Vec<eliot_core::api::AgentStateTotal>>,
    /// The next refresh reads the attention rows (an attention state's
    /// total moved, and not all of them are on the first page).
    pub(super) attention_due: bool,
    /// The next refresh reads the archived counts (the list's total moved).
    pub(super) archived_due: bool,
    /// The Agents page's fleet read could not account for a listed row (it
    /// left it out, or the row left a state the fleet read whole): the next
    /// poll reads the sidebar's own first page even while that page shows.
    pub(super) head_due: bool,
    pub(super) pending_select: Option<UiAgent>,
    /// Section keys of collapsed projects (`[desktop] collapsed_projects`,
    /// written from [`DesktopApp::collapsed_projects_changed`]).
    pub(super) collapsed: HashSet<String>,
    /// Section keys whose older stopped tasks are unfolded.
    pub(super) unfolded: HashSet<String>,
    /// List archived Agents too (`agent.list` `archived: include`, k92).
    pub(super) show_archived: bool,
    pub(super) search: Search,
    pub(super) search_ref: ElementRef<AutoSuggestBox>,
    /// The search's server rows: every matching Agent, beyond the loaded
    /// pages.
    pub(super) found: Found,
    /// Ctrl+N and Ctrl+K from the window's keyboard hook.
    pub(super) shortcuts: Option<Arc<Mutex<Receiver<Shortcut>>>>,
    /// Sidebar refreshes in a row with a failed read (or with no daemon
    /// answering): each waits longer, a clean one starts over.
    pub(super) pace: live::Pace,
    /// The sidebar is hidden to its rail (this session).
    pub(super) sidebar_collapsed: bool,
    /// A compact window's sidebar is open in the navigation pane.
    pub(super) overlay_open: bool,
    /// The places visited, for Back and Forward.
    pub(super) history: crate::history::History<Location>,
    /// The Back or Forward step waiting for its Agent's read.
    pub(super) place_read: Option<PlaceRead>,
    /// Tags those reads: a reply for an older step is dropped.
    pub(super) place_epoch: u64,
}

/// The sidebar search: what the box shows, what was typed and the
/// suggestions it offers.
#[derive(Clone, Debug, Default)]
pub(super) struct Search {
    /// The box's text as WinUI last reported it (typed, or a suggestion it
    /// wrote in while the arrows move). The box is controlled: a render
    /// that left its text unset cleared it (observed live).
    pub(super) text: String,
    /// The typed query the list and the suggestions follow.
    pub(super) query: String,
    /// `(label, Agent id)` per suggestion ([`nav::search`]).
    pub(super) results: Vec<(String, String)>,
    /// The suggestion reached with ↑/↓: its label, Agent id and the input
    /// epoch then. Enter opens it while no other input came between.
    pub(super) highlighted: Option<(String, String, u64)>,
    /// The Search row opened into its field (the row, Ctrl+K).
    pub(super) open: bool,
}

/// The server side of the sidebar search: the rows `agent.list` returned
/// for `query` (its word search over the id, title, first input, project
/// and folder of every Agent), the cursor and the matching total. The
/// loaded history pages stay as they are underneath, so closing the search
/// or toggling Show archived never drops them.
#[derive(Clone, Debug, Default)]
pub(super) struct Found {
    /// The trimmed query these rows answer.
    pub(super) query: String,
    pub(super) rows: Vec<UiAgent>,
    pub(super) next: Option<String>,
    pub(super) total: usize,
    /// A search read is in flight.
    pub(super) loading: bool,
    /// That read is Show more's next page.
    pub(super) appending: bool,
    /// Tags search reads: a reply to an older read or a closed search is
    /// dropped. Never reset, so a late reply cannot match a new read.
    pub(super) epoch: u64,
}

impl DesktopApp {
    pub(super) fn shell_update(&mut self, msg: ShellMsg, context: &ComponentContext<Self>) {
        match msg {
            ShellMsg::AutoSettings => {
                self.close_popover();
                self.open_settings(Some(Page::Routing), context);
            }
            ShellMsg::RepairSettings => {
                self.open_settings(Some(Page::Advanced), context);
            }
            ShellMsg::RetryStartup => self.retry_startup(context),
            ShellMsg::RecheckLink => self.link_watch(context),
            ShellMsg::OpenStartupLog => self.open_in_data_dir(&["logs", "daemon.log"]),
            ShellMsg::OpenDataFolder => self.open_in_data_dir(&[]),
            ShellMsg::LinkProbed(probed) => self.link_probed(probed, context),
            ShellMsg::ChooseDataDirectory => self.choose_data_dir(context),
            ShellMsg::UseFreshDirectory => self.link.confirm_fresh = true,
            ShellMsg::FreshConfirmed(confirmed) => self.fresh_confirmed(confirmed, context),
            ShellMsg::CandidateChecked(checked) => self.candidate_checked(checked, context),
            ShellMsg::RootRemembered(root, remembered) => self.root_remembered(root, remembered),
            ShellMsg::OpenStartupConfig => {
                if let Ok(data_dir) = &self.data_dir
                    && let Err(error) =
                        super::reveal::open_with_default_app(&data_dir.join("config.toml"))
                {
                    self.alert(error);
                }
            }
            ShellMsg::DaemonReady(result) => match result {
                Ok((discovered, read, acl_repaired)) => {
                    self.startup_error = None;
                    let restarted = self.link_ready(context);
                    self.prefs = crate::settings::Preferences::from_reply(&read);
                    self.nav.collapsed = self.prefs.collapsed.iter().cloned().collect();
                    self.launch_preferences_loaded();
                    self.seed_settings(read, context);
                    let first_connection = !self.preferences_loaded;
                    // The opened checkout, registered by discovery, is where
                    // a new task starts (a restart keeps the folder chosen).
                    if let Some(checkout) = discovered.filter(|_| !restarted) {
                        self.launch.workspace = checkout;
                    }
                    self.preferences_loaded = true;
                    self.load_agents(context, None);
                    self.load_organization(context);
                    self.load_profile_list(context);
                    self.load_routing_preview(context);
                    // The picker opens on a read catalogue (the daemon caches it).
                    self.load_catalog(context);
                    self.load_projects(context);
                    if first_connection {
                        self.watch_list(LIST_REFRESH_MS, context);
                    } else {
                        // Reconnected: the open conversation and the panel
                        // (the Monitor or the details, one refresh chain)
                        // read again from the daemon.
                        self.set_status("Swarm is running again.");
                        self.load_queue(context);
                        if self.chat.convo.is_some() {
                            self.start_snapshot(context);
                        }
                        if self.monitor_panel() {
                            self.read_monitor(0, context);
                        }
                        self.pane_reread(context);
                    }
                    if acl_repaired {
                        self.alert("Swarm repaired the data directory ACL.");
                    }
                }
                Err(error) => self.link_failed(error, context),
            },
            ShellMsg::OrganizationLoaded(read) => {
                self.nav.organization_loading = false;
                let read = *read;
                let organization = &mut self.nav.organization;
                // A failed read keeps the last server grouping (marked
                // stale); nothing is synthesized in its place.
                if !read.stale {
                    organization.groups = read.groups;
                }
                organization.stale = read.stale;
                organization.errors = read.errors;
                organization.changes_after = read.changes_after;
                // Organization changes (retag, attach, detach) refresh the
                // affected loaded rows by id, so an older loaded row never
                // keeps a stale placement or tags.
                let changed =
                    nav::apply_changes(&mut self.nav.agents, &mut self.nav.branches, &read.changes);
                if changed > 0 {
                    self.refresh_search();
                }
                if read.stale {
                    self.link_suspect(context);
                }
                // Changes were lost: one whole read of the projects (none
                // when one is in flight, its reply is as new) and the rows.
                if read.history_gap {
                    if self.nav.project_reads == 0 {
                        self.load_projects(context);
                    }
                    self.load_agents(context, None);
                }
            }
            ShellMsg::QueueLoaded(read) => {
                self.nav.queue_loading = false;
                let read = *read;
                let organization = &mut self.nav.organization;
                // A failed queue read keeps the last waits beside its error.
                if read.queue.is_some() {
                    organization.queue = read.queue;
                }
                organization.intents = read.intents;
                organization.missing_intents = read.missing;
                let failed = !read.errors.is_empty();
                organization.queue_errors = read.errors;
                self.settle_queued_launches(context);
                if failed {
                    self.link_suspect(context);
                }
            }
            ShellMsg::ProjectsLoaded(projects) => {
                self.nav.project_reads = self.nav.project_reads.saturating_sub(1);
                match projects {
                    Ok(mut projects) => {
                        self.nav.projects_error = None;
                        keep_last_known(&mut projects, &self.nav.projects);
                        self.nav.projects = projects;
                        let workspace = self.launch.workspace.clone();
                        if let Some(project) = self
                            .nav
                            .projects
                            .iter()
                            .find(|project| project.workspace == workspace)
                            .cloned()
                        {
                            // The opened checkout is a registered project.
                            self.pick_project(&project.workspace);
                        } else if !self.launch.custom_workspace
                            && let Some(first) = self.nav.projects.first().cloned()
                        {
                            // A new task starts in the first project until one is picked.
                            self.pick_project(&first.workspace);
                        }
                        self.refresh_search();
                    }
                    Err(error) => self.projects_failed(&error),
                }
            }
            ShellMsg::ProjectsPolled(read) => {
                // A failed read keeps what is shown and says why once per
                // failing streak; the next poll reads again. A row still
                // refreshing keeps its last known Git data; one whose
                // marker cleared takes the new metadata.
                self.nav.project_reads = self.nav.project_reads.saturating_sub(1);
                let mut read = match read {
                    Ok(read) => read,
                    Err(error) => {
                        self.projects_failed(&error);
                        return;
                    }
                };
                self.nav.projects_error = None;
                keep_last_known(&mut read, &self.nav.projects);
                let mut git_changed = false;
                for fresh in read {
                    let Some(project) = self
                        .nav
                        .projects
                        .iter_mut()
                        .find(|project| project.id == fresh.id)
                    else {
                        continue;
                    };
                    if project.policy_usage != fresh.policy_usage {
                        project.policy_usage = fresh.policy_usage;
                    }
                    if project.git_refreshing != fresh.git_refreshing
                        || project.origin != fresh.origin
                        || project.git_checkouts != fresh.git_checkouts
                    {
                        project.git_refreshing = fresh.git_refreshing;
                        project.origin = fresh.origin;
                        project.git_checkouts = fresh.git_checkouts;
                        git_changed = true;
                    }
                }
                if git_changed {
                    self.refresh_search();
                }
            }
            ShellMsg::AgentsLoaded(list_epoch, append, page) => {
                // No quota read here: a harness's quota read can start the
                // vendor's own process, so the limits are read only while
                // their screen is open (the Usage popover, Settings › Usage)
                // and on Refresh (skeleton 2.4).
                if list_epoch != self.nav.list_epoch {
                    return;
                }
                self.nav.agents_loading = false;
                self.nav.paging = false;
                match page {
                    Ok(mut page) => {
                        if append {
                            let seen: std::collections::HashSet<&str> = self
                                .nav
                                .agents
                                .iter()
                                .map(|agent| agent.id.as_str())
                                .collect();
                            page.rows.retain(|agent| !seen.contains(agent.id.as_str()));
                            self.nav.agents.extend(page.rows);
                        } else {
                            // The open Agent renders explicitly above the list, so
                            // keep its last known row when it aged out of the
                            // newest page until a later read returns it fresh.
                            // Busy, waiting, starting and needs-attention rows
                            // arrive on the attention fetch; only the selected
                            // row needs pinning.
                            if let Some(selected) = self.nav.selection.agent.clone()
                                && !page.rows.iter().any(|agent| agent.id == selected)
                                && let Some(known) = self
                                    .nav
                                    .agents
                                    .iter()
                                    .find(|agent| agent.id == selected)
                                    .cloned()
                            {
                                page.rows.insert(0, known);
                            }
                            self.nav.agents = page.rows;
                        }
                        self.nav.agents_next = page.next;
                        self.nav.agents_total = page.total;
                        if !append {
                            self.nav.archived = page.archived;
                        }
                        // A just-launched Agent may not be on this page yet;
                        // its id is already the selection.
                        if let Some(wanted) = self.nav.pending_select.take()
                            && !self.nav.agents.iter().any(|agent| agent.id == wanted.id)
                        {
                            self.nav.agents.push(wanted);
                        }
                        let selected = self.nav.selection.agent.clone();
                        if let Some(agent) = selected
                            .and_then(|id| self.nav.agents.iter().find(|agent| agent.id == id))
                            .cloned()
                            && self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id)
                        {
                            self.open_conversation(&agent, context);
                        }
                        self.open_startup_conversation(context);
                        self.refresh_search();
                    }
                    Err(error) => {
                        // A daemon without the archived filter: the toggle
                        // goes back off, so the refresh does not repeat it.
                        if self.nav.show_archived && error.contains("(k92)") {
                            self.nav.show_archived = false;
                        }
                        self.read_failed(format!("Agents not read: {error}"), context);
                    }
                }
            }
            ShellMsg::HeadLoaded(list_epoch, page) => {
                if list_epoch != self.nav.list_epoch {
                    return;
                }
                self.nav.agents_loading = false;
                match page {
                    Ok(page) => {
                        if let Some(totals) = page.totals.clone() {
                            self.note_totals(&page.rows, totals);
                        }
                        merge_head_rows(&mut self.nav.agents, page.rows);
                        self.nav.agents_total = page.total;
                        if self.nav.agents.len() <= AGENT_PAGE {
                            self.nav.agents_next = page.next;
                        }
                        self.refresh_search();
                    }
                    Err(error) => {
                        self.read_failed(format!("Agents not read: {error}"), context);
                    }
                }
            }
            ShellMsg::AttentionLoaded(list_epoch, page) => {
                if list_epoch != self.nav.list_epoch {
                    return;
                }
                self.nav.agents_loading = false;
                match page {
                    Ok(page) => {
                        merge_head_rows(&mut self.nav.agents, page.rows);
                        self.refresh_search();
                    }
                    Err(error) => {
                        self.read_failed(format!("Agents not read: {error}"), context);
                    }
                }
            }
            ShellMsg::ArchivedLoaded(list_epoch, read) => {
                if list_epoch != self.nav.list_epoch {
                    return;
                }
                self.nav.agents_loading = false;
                match read {
                    Ok(archived) => self.nav.archived = archived,
                    Err(error) => self.read_failed(format!("Agents not read: {error}"), context),
                }
            }
            ShellMsg::Refreshed(replies) => {
                // One sidebar refresh replied: the next one comes a cadence
                // later, or later still after rounds with a failed read.
                let failed = replies.iter().any(ShellMsg::read_failed);
                for reply in replies {
                    self.shell_update(reply, context);
                }
                let delay = self.nav.pace.next(!failed, LIST_REFRESH_MS);
                if !failed {
                    self.link_answered();
                }
                self.watch_list(delay, context);
            }
            ShellMsg::Inbox(msg) => self.inbox_update(msg, context),
            ShellMsg::AttentionReports(cursor, read) => {
                self.monitor.sidebar_reading = false;
                // An open panel's refresh owns the cursor meanwhile.
                if !self.monitor_panel()
                    && self.monitor.data.attention_cursor == cursor
                    && let Ok(read) = read
                {
                    self.monitor.data.apply_attention(read);
                }
            }
            ShellMsg::SearchLoaded(epoch, query, append, page) => {
                if epoch != self.nav.found.epoch {
                    return;
                }
                self.nav.found.loading = false;
                self.nav.found.appending = false;
                match page {
                    Ok(page) => {
                        let found = &mut self.nav.found;
                        if found.query != query {
                            found.rows = page.rows;
                            found.next = page.next;
                        } else if append {
                            let seen: HashSet<&str> =
                                found.rows.iter().map(|agent| agent.id.as_str()).collect();
                            let rows: Vec<UiAgent> = page
                                .rows
                                .into_iter()
                                .filter(|agent| !seen.contains(agent.id.as_str()))
                                .collect();
                            found.rows.extend(rows);
                            found.next = page.next;
                        } else {
                            // A poll's first page refreshes the matches in
                            // place and keeps the pages Show more added.
                            merge_head_rows(&mut found.rows, page.rows);
                            if found.rows.len() <= AGENT_PAGE {
                                found.next = page.next;
                            }
                        }
                        found.query = query;
                        found.total = page.total;
                        self.refresh_search();
                    }
                    Err(error) => self.alert(format!("Search not read: {error}")),
                }
            }
            ShellMsg::SidebarToggled => self.toggle_sidebar(),
            ShellMsg::AgentOpened(agent) => {
                // A match beyond the loaded pages joins them, so it stays in
                // its project once the search closes.
                if !self.nav.agents.iter().any(|listed| listed.id == agent.id)
                    && self.nav.found.rows.iter().any(|found| found.id == agent.id)
                {
                    self.nav.agents.push(agent.clone());
                }
                self.close_popover();
                self.chat.opened_from_search = false;
                if self.main != Main::Conversation {
                    self.main = Main::Conversation;
                    self.reprime_fields(context);
                }
                if self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id) {
                    self.open_conversation(&agent, context);
                }
            }
            ShellMsg::ProjectPicked(checkout) => {
                if self.can_launch_here(&checkout) {
                    self.pick_project(&checkout);
                    self.new_task(context);
                }
            }
            ShellMsg::ProjectOpened(checkout, key) => {
                if self.nav.collapsed.remove(&key) {
                    self.collapsed_projects_changed(context);
                }
                // A checkout whose group the daemon has not returned yet
                // has no page to show: it opens a new task there.
                if !self.open_project(&key, context) && self.can_launch_here(&checkout) {
                    self.pick_project(&checkout);
                    self.new_task(context);
                }
            }
            ShellMsg::PlaceRead(epoch, forward, id, read) => {
                // Dropped when a newer step started or the owner went
                // somewhere else while the daemon answered.
                let current = self.location();
                let Some(step) = self.nav.place_read.take_if(|step| step.epoch == epoch) else {
                    return;
                };
                let place = Location::Agent(id);
                if step.from != current || self.nav.history.peek(forward) != Some(&place) {
                    return;
                }
                match *read {
                    Ok(agent) => {
                        let _ = if forward {
                            self.nav.history.forward()
                        } else {
                            self.nav.history.back()
                        };
                        match self.nav.agents.iter_mut().find(|row| row.id == agent.id) {
                            Some(row) => {
                                row.generation = agent.generation;
                                row.state = agent.state;
                            }
                            None => self.nav.agents.push(agent.clone()),
                        }
                        self.shell_update(ShellMsg::AgentOpened(agent), context);
                        if step.more > 0 {
                            self.walk(forward, step.more - 1, context);
                        }
                    }
                    // Only a deleted Agent leaves the history.
                    Err(error) if error.code == "not_found" => {
                        self.nav.history.forget(&place);
                        self.walk(forward, step.more, context);
                    }
                    Err(error) => {
                        self.alert(format!("Task not opened: {}", error.user_message()));
                    }
                }
            }
            ShellMsg::ExpandToggled(id, expand) => {
                if expand {
                    self.nav.expanded.insert(id.clone());
                    if !matches!(self.nav.branches.get(&id), Some(Branch::Loaded { .. })) {
                        self.load_branch(&id, context);
                    }
                } else {
                    self.nav.expanded.remove(&id);
                }
            }
            ShellMsg::BranchLoaded(epoch, id, page) => {
                if epoch != self.nav.tree_epoch {
                    return;
                }
                let branch = match page {
                    Ok(page) => Branch::Loaded {
                        more: page.next.is_some() && page.rows.len() < page.total,
                        children: page.rows,
                    },
                    Err(error) => Branch::Failed(error),
                };
                self.nav.branches.insert(id, branch);
            }
            ShellMsg::RefreshRequested => {
                self.load_projects(context);
                self.load_agents(context, None);
                self.load_organization(context);
                // Expanded branches are read again; their rows stay until then.
                let expanded: Vec<String> = self.nav.expanded.iter().cloned().collect();
                for id in expanded {
                    self.load_branch(&id, context);
                }
                self.load_profile_list(context);
                // A failed Agent catalogue re-reads with the refresh; a
                // cached success stays (#681).
                self.configure.failed = None;
                self.configure.failed_attempts = 0;
                if self.popover_agent().is_some()
                    && matches!(
                        self.launch.open,
                        Some(launch_ui::Pop::Model) | Some(launch_ui::Pop::Effort)
                    )
                {
                    self.ensure_catalog(context);
                }
                if self.chat.convo.is_some() {
                    self.chat.pace.reset();
                    self.start_snapshot(context);
                }
            }
            ShellMsg::NewTask => self.new_task(context),
            ShellMsg::Back => self.step(false, context),
            ShellMsg::Forward => self.step(true, context),
            ShellMsg::MoreAgentsRequested => {
                if let Some(after) = self.nav.agents_next.clone() {
                    self.load_agents(context, Some(after));
                }
            }
            ShellMsg::MoreSearchRequested => {
                if let Some(after) = self.nav.found.next.clone() {
                    self.load_search(context, Some(after));
                }
            }
            ShellMsg::SettingsOpened => self.open_settings(None, context),
            ShellMsg::SettingsPage(page) => self.open_settings(Some(page), context),
            ShellMsg::PlaceOpened(place) => self.open_place(place, context),
            ShellMsg::UsageRefresh => self.read_usage(context),
            ShellMsg::HoldsLoaded(reply) => self.holds_loaded(*reply),
            ShellMsg::AddProject => {
                self.open_settings(Some(settings_ui::Page::Projects), context);
                if self.settings.is_read() {
                    self.settings_update(SettingsMsg::ProjectBrowse, context);
                }
            }
            ShellMsg::SettingsSearch(text) => self.settings.query = text,
            ShellMsg::SettingsClosed => {
                // A preset's picker does not follow the owner out.
                self.close_popover();
                if self.main != Main::Conversation {
                    self.main = Main::Conversation;
                    self.reprime_fields(context);
                }
            }
            // The pane's X closes the pane.
            ShellMsg::PanelToggled(panel) => match panel {
                Panel::Closed => self.close_panel(),
                panel => self.choose_panel(panel, context),
            },
            ShellMsg::Layout(layout) => {
                if layout.class != split_ui::SizeClass::Compact {
                    self.nav.overlay_open = false;
                }
                self.layout = layout;
            }
            // A Settings slider reports the width it shows too: only a
            // change is saved.
            ShellMsg::PanelsResized(widths)
                if widths != split_ui::PanelWidths::of(self.prefs.panels) =>
            {
                self.prefs.panels = Some(widths.saved());
                self.prefs_rev += 1;
                self.save_preferences(context);
            }
            ShellMsg::PanelsReset if self.prefs.panels.is_some() => {
                self.prefs.panels = None;
                self.prefs_rev += 1;
                self.save_preferences(context);
            }
            ShellMsg::PanelsResized(_) | ShellMsg::PanelsReset => {}
            ShellMsg::OverlayClosed => self.nav.overlay_open = false,
            // The list reports the value it shows when it is built: only a
            // change saves. WinUI redraws in the new theme by itself.
            ShellMsg::ThemeChosen(theme) if theme != self.prefs.theme => {
                self.prefs.theme = theme;
                self.prefs_rev += 1;
                self.save_preferences(context);
            }
            ShellMsg::ThemeChosen(_) => {}
            // The list reports the value it shows when it is built: only a
            // change saves.
            ShellMsg::StartupChosen(startup) if startup != self.prefs.startup => {
                self.prefs.startup = startup;
                self.prefs_rev += 1;
                self.save_preferences(context);
            }
            ShellMsg::StartupChosen(_) => {}
            ShellMsg::PreferencesSaved(saved, result) => {
                self.prefs_saving = false;
                if let Err(error) = &result {
                    self.alert(format!("Preferences not saved: {error}"));
                }
                // A choice made on the page reports there; composer
                // writes stay quiet.
                if self.main == Main::Settings || result.is_err() {
                    self.settings
                        .notes
                        .insert(self.settings_page, settings_ui::Note::of(false, &result));
                }
                // A newer change arrived while this one was written.
                if saved != self.prefs {
                    self.save_preferences(context);
                } else if self.settings.loaded && result.is_ok() {
                    // The revision moved: the page's drafts save against it.
                    self.load_settings(context);
                } else {
                    // Page saves that waited for this write go now.
                    self.drain_saves(context);
                }
            }
            ShellMsg::ProjectToggled(key) => {
                if !self.nav.collapsed.remove(&key) {
                    self.nav.collapsed.insert(key);
                }
                self.collapsed_projects_changed(context);
            }
            ShellMsg::OlderToggled(key) => {
                if !self.nav.unfolded.remove(&key) {
                    self.nav.unfolded.insert(key);
                }
            }
            ShellMsg::SearchToggled(open) => {
                if open {
                    self.nav.search.open = true;
                    self.focus_search(FOCUS_TRIES, context);
                } else {
                    self.nav.search = Search::default();
                    self.clear_found();
                    self.refresh_search();
                }
            }
            ShellMsg::SearchChanged(text) => {
                self.nav.search.text = text.clone();
                // WinUI writes the suggestion reached with the arrows into
                // the box: that is not a new query, nor is the query itself
                // written back after an open (`open_found`).
                if text != self.nav.search.query
                    && !self
                        .nav
                        .search
                        .results
                        .iter()
                        .any(|(label, _)| *label == text)
                {
                    self.nav.search.query = text;
                    // The server searches every Agent, not just the loaded
                    // pages; the loaded matches show until it answers.
                    if self.nav.search.query.trim().is_empty() {
                        self.clear_found();
                    } else {
                        self.load_search(context, None);
                    }
                    self.refresh_search();
                }
            }
            ShellMsg::SearchChosen(label) => {
                let chosen = self
                    .nav
                    .search
                    .results
                    .iter()
                    .find(|(shown, _)| *shown == label)
                    .map(|(_, id)| id.clone());
                // WinUI reports every suggestion reached with the arrows as
                // chosen (Reactor 0.100.0 exposes no QuerySubmitted): that
                // only highlights it. Enter (`Shortcut::Enter`) or a click
                // opens it.
                let (input, age, epoch) = crate::shortcuts::last_input();
                if input == Input::Arrow && age <= ARROW_CHOICE_MS {
                    self.nav.search.highlighted = chosen.map(|id| (label, id, epoch));
                    return;
                }
                self.nav.search.highlighted = None;
                if let Some(id) = chosen {
                    self.open_found(&id, context);
                }
            }
            ShellMsg::Shortcut(shortcut) => {
                // Lines a hook or WinUI handler queued (a caught panic) are
                // written here, on the UI thread, outside the boundary.
                crate::startup::drain_deferred();
                match shortcut {
                    // Before the dialog arm: the hold ends either way (a
                    // closing dialog puts focus back itself).
                    Shortcut::Foreground => {
                        crate::window_place::hold_focus(false);
                        if let Some(target) = self.focus_held.take()
                            && self.dialog_kind() == 0
                        {
                            self.focus_held_now(target, FOCUS_TRIES, context);
                        }
                    }
                    // A flyout closed: the open popover or row menu follows
                    // whether or not a dialog is up.
                    Shortcut::FlyoutClosed(epoch) => {
                        crate::flyout::release();
                        self.launch_update(LaunchMsg::Closed(epoch), context);
                        self.menu_update(MenuMsg::Closed(epoch), context);
                    }
                    // Before the dialog arm: the notice shows either way.
                    Shortcut::Panicked => {
                        self.alert(
                            "Something went wrong inside the window and was stopped there. \
                             Details are in desktop.log; restart Eliot Swarm if it misbehaves.",
                        );
                    }
                    Shortcut::Popover(key) if self.dialog_kind() != 0 => {
                        self.dialog_key(key, context)
                    }
                    _ if self.dialog_kind() != 0 => {}
                    Shortcut::NewTask => self.new_task(context),
                    Shortcut::Back => self.step(false, context),
                    Shortcut::Forward => self.step(true, context),
                    Shortcut::Search => {
                        self.nav.search.open = true;
                        self.focus_search(FOCUS_TRIES, context)
                    }
                    Shortcut::Settings => self.open_settings(None, context),
                    Shortcut::Sidebar
                        if self.main != Main::Settings
                            || self.layout.class == split_ui::SizeClass::Compact =>
                    {
                        self.toggle_sidebar()
                    }
                    Shortcut::Sidebar => {}
                    Shortcut::Terminal => {
                        let panel = if self.panel == Panel::Terminal && self.panel_shown() {
                            Panel::Closed
                        } else {
                            Panel::Terminal
                        };
                        self.shell_update(ShellMsg::PanelToggled(panel), context);
                    }
                    Shortcut::ModelPicker => {
                        if self.launch.open != Some(launch_ui::Pop::Model) {
                            self.launch_update(LaunchMsg::Toggle(launch_ui::Pop::Model), context);
                        }
                    }
                    Shortcut::Popover(key) => self.launch_update(LaunchMsg::Key(key), context),
                    Shortcut::FocusMoved => {
                        let row = crate::elements::resolve_focused_row();
                        if self.menu.focused != row {
                            self.menu.focused = row;
                        }
                    }
                    Shortcut::RowMenu => {
                        if let Some((row, x, y)) = crate::elements::focused_row_anchor()
                            && let Some(target) = self.row_target(&row)
                        {
                            self.menu_update(MenuMsg::Requested(target, x, y, true), context);
                        }
                    }
                    Shortcut::Enter(_, press) if self.menu.renaming.is_some() => {
                        // The Enter that chose Rename or Tags in the native
                        // menu reaches the hook before the menu's click; it
                        // must not commit the field it opened.
                        if press > self.menu.rename_enter {
                            self.menu_update(MenuMsg::RenameCommitted, context);
                        }
                    }
                    Shortcut::Enter(epoch, _) => {
                        // Enter on a suggestion reached with the arrows, with
                        // no other input since and the box still showing it.
                        if let Some((label, id, at)) = self.nav.search.highlighted.take()
                            && at == epoch
                            && self.nav.search.text == label
                        {
                            self.open_found(&id, context);
                        }
                    }
                }
                self.watch_shortcuts(context);
            }
            ShellMsg::FocusSearch(tries) => self.focus_search(tries, context),
            ShellMsg::FocusSearchLater(tries) => {
                self.focus_later(ShellMsg::FocusSearch(tries), context)
            }
            ShellMsg::FocusHeld(target, tries) => self.focus_held_now(target, tries, context),
            ShellMsg::FocusHeldLater(target, tries) => {
                self.focus_later(ShellMsg::FocusHeld(target, tries), context)
            }
            ShellMsg::ShortcutsEnded => {}
            ShellMsg::AlertDismissed => {
                // Dismissing the queue notice releases its draft only once
                // no intent can exist for it (see `dismiss_queue_offer`).
                self.alert = None;
                self.dismiss_queue_notice(context);
            }
            ShellMsg::ListTick => {
                // The head (the first recent page plus the live and
                // attention rows) is re-read on every poll and merged into
                // the loaded history, even with older pages appended: a
                // launch or a state change while the desktop is open
                // arrives without a manual refresh. The queue state polls on
                // its own worker, so the grouping reads never delay an
                // arbiter's admission or refusal.
                // Every message renders the whole window (k209), so the
                // refresh's reads reply together, as one message, and the
                // next refresh is planned when they did. While no daemon
                // answers, the restart owns the connection attempts; the
                // daemon screen only asks whether one runs again, each
                // round waiting longer (one ask per 15 s at most).
                if self.daemon_down() {
                    self.link_watch(context);
                    let delay = self.nav.pace.fail(LIST_REFRESH_MS);
                    self.watch_list(delay, context);
                    return;
                }
                if self.main == Main::Project {
                    self.read_project(context);
                }
                self.nav.batch = Some(Vec::new());
                // While the Agents page shows, its fleet read (the Monitor's
                // chain) feeds the sidebar's rows instead (`take_fleet_rows`),
                // unless it could not account for a listed row.
                if self.preferences_loaded
                    && !self.nav.agents_loading
                    && (self.main != Main::Agents || self.nav.head_due)
                {
                    self.refresh_head(context);
                }
                // An open search re-reads its first page too, so a match
                // changing state while it is shown is not stale.
                if self.preferences_loaded
                    && !self.nav.found.loading
                    && !self.nav.search.query.trim().is_empty()
                {
                    self.load_search(context, None);
                }
                if self.preferences_loaded {
                    self.load_organization(context);
                    self.load_queue(context);
                }
                // Refreshing Git metadata and an open launch policy's
                // counter follow the daemon.
                self.poll_projects(context);
                // The navigation's inbox counts on every poll; the items
                // only while the Inbox shows them.
                self.read_inbox_counts(context);
                if self.main == Main::Inbox {
                    self.read_inbox(context);
                }
                let reads = self.nav.batch.take().unwrap_or_default();
                if reads.is_empty() {
                    self.watch_list(LIST_REFRESH_MS, context);
                } else {
                    let _task = context.spawn_background(move |_| {
                        let replies = std::thread::scope(|scope| {
                            let running: Vec<_> =
                                reads.into_iter().map(|read| scope.spawn(read)).collect();
                            running
                                .into_iter()
                                .filter_map(|read| read.join().ok())
                                .collect()
                        });
                        Message::Shell(ShellMsg::Refreshed(replies))
                    });
                }
            }
            ShellMsg::FieldsPrimed(epoch) => {
                if epoch != self.prime_epoch {
                    return;
                }
                // Reactor drains queued messages before it composes, so this
                // can arrive before the empty render: wait for one.
                if self.renders.get() > self.prime_after {
                    self.fields_primed = true;
                } else {
                    self.post_fields_primed(epoch, PRIME_RETRY_MS, context);
                }
            }
        }
    }

    /// Open Settings, at `page` when given.
    fn open_settings(&mut self, page: Option<settings_ui::Page>, context: &ComponentContext<Self>) {
        self.close_popover();
        let reopened = self.main != Main::Settings;
        let page_changed = page.is_some_and(|page| page != self.settings_page);
        if let Some(page) = page {
            self.settings_page = page;
        }
        if reopened || page_changed {
            self.main = Main::Settings;
            self.reprime_fields(context);
            self.settings_entered(reopened, context);
        }
    }

    /// No daemon runs: Settings reads and writes the file under the
    /// daemon lock with its own reader and writer (offline repair).
    pub(super) fn repairing_settings(&self) -> bool {
        self.startup_error.is_some()
    }

    /// Where the owner is now, for Back and Forward (nothing while the
    /// daemon is not reached).
    pub(super) fn location(&self) -> Option<Location> {
        if self.link_view_shown() && self.main != Main::Settings {
            return None;
        }
        Some(match self.main {
            Main::Settings => Location::Settings(self.settings_page),
            Main::Project => Location::Project(self.project.key.clone()),
            Main::Inbox => Location::Place(page_ui::Place::Inbox),
            Main::Agents => Location::Place(page_ui::Place::Agents),
            Main::Automations => Location::Place(page_ui::Place::Automations),
            Main::Conversation => match self.selected_agent_id() {
                Some(agent) => Location::Agent(agent),
                // A folder being typed is no place yet.
                None => Location::Home {
                    checkout: if self.launch.custom_workspace {
                        String::new()
                    } else {
                        self.launch.workspace.clone()
                    },
                    chat: self.launch.chat,
                },
            },
        })
    }

    /// Keep the place shown now in the history (after every update, so
    /// every way of getting somewhere counts: a row, a search, a menu, a
    /// shortcut, archiving the open chat).
    /// Show or hide the sidebar: a compact window opens it in the
    /// navigation pane, a wider one hides it to its rail.
    fn toggle_sidebar(&mut self) {
        if self.layout.class == split_ui::SizeClass::Compact {
            self.nav.overlay_open = !self.nav.overlay_open;
        } else {
            self.nav.sidebar_collapsed = !self.nav.sidebar_collapsed;
        }
    }

    pub(super) fn record_location(&mut self) {
        if let Some(place) = self.location() {
            self.nav.history.visit(place);
        }
    }

    /// One step back (or forward) through the places visited. A task is
    /// read by id from the daemon first, whatever the sidebar has loaded
    /// ([`ShellMsg::PlaceRead`] takes the step); a removed project is
    /// skipped.
    fn step(&mut self, forward: bool, context: &ComponentContext<Self>) {
        let here = self.location();
        if let Some(read) = self
            .nav
            .place_read
            .as_mut()
            .filter(|read| read.forward == forward && read.from == here)
        {
            read.more += 1;
            return;
        }
        self.walk(forward, 0, context);
    }

    /// Take one step and `more` after it, reading each task first.
    fn walk(&mut self, forward: bool, mut more: u32, context: &ComponentContext<Self>) {
        while let Some(place) = self.nav.history.peek(forward).cloned() {
            match &place {
                Location::Agent(id) => {
                    self.read_place(id.clone(), forward, more, context);
                    return;
                }
                Location::Project(key) if !self.project_exists(key) => {
                    self.nav.history.forget(&place);
                }
                _ => {
                    let _ = if forward {
                        self.nav.history.forward()
                    } else {
                        self.nav.history.back()
                    };
                    self.go_to(&place, context);
                    if more == 0 {
                        return;
                    }
                    more -= 1;
                }
            }
        }
    }

    /// Read the Agent of a Back or Forward step, by id.
    fn read_place(
        &mut self,
        id: String,
        forward: bool,
        more: u32,
        context: &ComponentContext<Self>,
    ) {
        let data_dir = match self.data_dir.clone() {
            Ok(data_dir) => data_dir,
            Err(error) => {
                self.alert(format!("Task not opened: {error}"));
                return;
            }
        };
        self.nav.place_epoch += 1;
        let epoch = self.nav.place_epoch;
        self.nav.place_read = Some(PlaceRead {
            epoch,
            from: self.location(),
            forward,
            more,
        });
        let _task = context.spawn_background(move |_| {
            let read = read_agent_row(&data_dir, &id);
            Message::Shell(ShellMsg::PlaceRead(epoch, forward, id, Box::new(read)))
        });
    }

    /// Show a place that needs no read.
    fn go_to(&mut self, place: &Location, context: &ComponentContext<Self>) {
        match place {
            Location::Home { checkout, chat } => {
                self.new_task(context);
                if *chat {
                    self.launch_update(
                        LaunchMsg::ProjectChosen(launch_ui::CHAT_PROJECT.to_owned()),
                        context,
                    );
                } else if self.can_launch_here(checkout) {
                    self.pick_project(checkout);
                }
            }
            Location::Agent(_) => {}
            Location::Project(key) => {
                self.open_project(key, context);
            }
            Location::Place(place) => self.open_place(*place, context),
            Location::Settings(page) => self.open_settings(Some(*page), context),
        }
    }

    /// Show Inbox, Agents or Automations in the main card. Agents runs the
    /// fleet's refresh chain while it shows (with the open Agent's
    /// details); the Inbox reads with the sidebar's poll; Automations
    /// reads its rules and the settings it edits.
    pub(super) fn open_place(&mut self, place: page_ui::Place, context: &ComponentContext<Self>) {
        self.close_popover();
        let main = match place {
            page_ui::Place::Inbox => Main::Inbox,
            page_ui::Place::Agents => Main::Agents,
            page_ui::Place::Automations => Main::Automations,
        };
        if self.panel != Panel::Closed {
            self.close_panel();
        }
        if self.main != main {
            self.main = main;
            self.reprime_fields(context);
        }
        match place {
            page_ui::Place::Inbox => {
                self.read_inbox(context);
                self.read_inbox_counts(context);
            }
            page_ui::Place::Agents => {
                self.start_monitor(context);
                self.read_liveness(context);
                if self.monitor.autonomy.report.is_none() {
                    self.read_autonomy(0, context);
                }
            }
            page_ui::Place::Automations => {
                if !self.settings.loaded {
                    self.load_settings(context);
                }
                self.load_rules(context);
            }
        }
    }

    /// Open the Agent a search found.
    fn open_found(&mut self, id: &str, context: &ComponentContext<Self>) {
        // The box shows the query again, not the suggestion WinUI wrote
        // into it when the arrows reached it.
        self.nav.search.text = self.nav.search.query.clone();
        if let Some(agent) = self.find_agent(id).cloned() {
            self.nav.selection.select_agent(Some(agent.id.clone()));
            self.shell_update(ShellMsg::AgentOpened(agent), context);
            self.chat.opened_from_search = true;
        }
    }

    /// Leave the open conversation locally (the Agent keeps running) and
    /// show the empty new-task view.
    pub(super) fn new_task(&mut self, context: &ComponentContext<Self>) {
        self.close_popover();
        if self.main != Main::Conversation {
            self.main = Main::Conversation;
            self.reprime_fields(context);
        }
        // Keyboard focus goes back to the composer (no-op while it is unbound).
        self.take_focus(FocusTarget::Composer);
        self.chat.epoch += 1;
        self.chat.inputs_checking = false;
        if let Some(task) = self.chat.live_task.take() {
            task.cancel();
        }
        self.nav.selection.select_agent(None);
        self.chat.convo = None;
        self.chat.detail = None;
        self.chat.live = LiveStatus::Idle;
        self.chat.items = Rc::new(Vec::new());
        if self.panel != Panel::Closed {
            self.close_panel();
        }
        self.load_routing_preview(context);
        self.set_status("New task.");
    }

    /// The project a new task starts in: its checkout, and its profile
    /// allowlist when the checkout is a registered project's own
    /// workspace. Anything else stays unregistered: the daemon resolves
    /// linked worktrees, subfolders and typed folders, so the pending row
    /// stays unknown until its Agent row arrives.
    pub(super) fn pick_project(&mut self, checkout: &str) {
        let key = nav::path_key(checkout);
        let registered = self
            .nav
            .projects
            .iter()
            .find(|project| nav::path_key(&project.workspace) == key)
            .cloned();
        self.launch.workspace = checkout.to_owned();
        self.launch.custom_workspace = false;
        self.launch.chat = false;
        match registered {
            Some(project) => {
                self.launch.profiles = project.profiles.clone();
                self.nav.selection.project = Some(project.id.clone());
            }
            None => {
                self.launch.profiles.clear();
                self.nav.selection.project = None;
            }
        }
        self.launch.reconcile_choice();
    }

    /// What the owner reads for a registered project ([`Self::place_title`]
    /// of its checkout): never its registration ID.
    pub(super) fn project_label(&self, project: &UiProject) -> String {
        self.place_title(&project.workspace)
    }

    /// What the owner reads for a task folder: the project's name (the
    /// display name they gave it, `[desktop] display_names`, else its
    /// logical name, [`nav::place_of`]) and, in a linked worktree, its
    /// subproject's ("atlas-web › search filters").
    pub(super) fn place_title(&self, label: &str) -> String {
        self.place_title_in(label, &self.checkouts())
    }

    /// [`Self::place_title`] over checkouts already built.
    pub(super) fn place_title_in(&self, label: &str, checkouts: &[Checkout]) -> String {
        // A chat outside every project names that, never a blank folder.
        if label.trim().is_empty() {
            return nav::NO_PROJECT_NAME.to_owned();
        }
        let (_, subproject) = nav::place_of(label, checkouts);
        let project = self.project_title_in(label, checkouts);
        match subproject {
            Some(subproject) => {
                let renamed = nav::subproject_key(label, checkouts)
                    .and_then(|key| self.prefs.display_name(&format!("project:{key}")));
                format!("{project} › {}", renamed.unwrap_or(&subproject))
            }
            None => project,
        }
    }

    /// The project's name alone (the owner's display name, else its logical
    /// name): what the sidebar's project row and the Monitor show.
    pub(super) fn project_title_in(&self, label: &str, checkouts: &[Checkout]) -> String {
        let key = format!("project:{}", nav::project_key(label, checkouts));
        self.prefs
            .display_name(&key)
            .map_or_else(|| nav::project_of(label, checkouts), str::to_owned)
    }

    /// Open the most recently active Agent's conversation once at start,
    /// when the preference asks.
    fn open_startup_conversation(&mut self, context: &ComponentContext<Self>) {
        if self.startup_done {
            return;
        }
        self.startup_done = true;
        if self.prefs.startup != Startup::LastConversation || self.chat.convo.is_some() {
            return;
        }
        if let Some(agent) = nav::recents(&self.nav.agents, 1, &|_| true)
            .first()
            .cloned()
            .cloned()
        {
            self.nav.selection.select_agent(Some(agent.id.clone()));
            self.open_conversation(&agent, context);
        }
    }

    /// Registered checkouts and daemon-verified worktrees with listed tasks.
    pub(super) fn checkouts(&self) -> Vec<Checkout> {
        let visible: HashSet<_> = self
            .nav
            .projects
            .iter()
            .map(|project| nav::path_key(&project.workspace))
            .chain(self.nav.agents.iter().map(|agent| {
                nav::path_key(agent.workspace.as_deref().unwrap_or(&agent.project_id))
            }))
            .collect();
        project_checkouts(&self.nav.projects)
            .into_iter()
            .filter(|checkout| {
                checkout.main == Some(true) || visible.contains(&nav::path_key(&checkout.path))
            })
            .collect()
    }

    /// The checkout at `path` belongs to a project whose Git metadata is
    /// refreshing.
    pub(super) fn git_refreshing_at(&self, path: &str) -> bool {
        let key = nav::path_key(path);
        !path.trim().is_empty()
            && self
                .checkouts()
                .iter()
                .any(|checkout| checkout.refreshing && nav::path_key(&checkout.path) == key)
    }

    /// The checkout at `path` is paused while its project's Git metadata
    /// refreshes ([`nav::Checkout::paused`]).
    pub(super) fn checkout_paused(&self, path: &str) -> bool {
        let key = nav::path_key(path);
        self.checkouts()
            .iter()
            .any(|checkout| checkout.paused() && nav::path_key(&checkout.path) == key)
    }

    /// A task can start in `path`: a listed checkout that is not missing
    /// and not paused while its Git metadata refreshes.
    pub(super) fn can_launch_here(&self, path: &str) -> bool {
        let key = nav::path_key(path);
        self.checkouts().iter().any(|checkout| {
            !checkout.missing && !checkout.paused() && nav::path_key(&checkout.path) == key
        })
    }

    /// The listed, fetched or found Agent with this id.
    pub(super) fn find_agent(&self, id: &str) -> Option<&UiAgent> {
        let branch_children = self.nav.branches.values().flat_map(|branch| match branch {
            Branch::Loaded { children, .. } => children.as_slice(),
            _ => &[],
        });
        self.nav
            .agents
            .iter()
            .chain(branch_children)
            .chain(&self.nav.found.rows)
            .find(|agent| agent.id == id)
    }

    /// The title of `agent` as its summary gave it, else the monitor's,
    /// else its id (an Agent on no page read yet).
    pub(super) fn agent_title(&self, agent: &str) -> String {
        if let Some(name) = self.prefs.display_name(&format!("agent:{agent}")) {
            return name.to_owned();
        }
        if let Some(found) = self.find_agent(agent) {
            return found.title.clone();
        }
        self.monitor
            .data
            .rows
            .iter()
            .find(|row| row.id == agent)
            .map_or_else(|| agent.to_owned(), |row| row.title.clone())
    }

    /// Recompute the search suggestions from what the search found.
    fn refresh_search(&mut self) {
        let checkouts = self.checkouts();
        let found = self.search_found(&checkouts);
        self.nav.search.results = nav::search(&found, &checkouts);
    }

    /// What the search shows: the server's matches for the typed query
    /// (every Agent, beyond the loaded pages) and the loaded rows matching
    /// it locally, which show at once while the server reads; the server's
    /// row first, one per id. Server rows answering an older query stay
    /// only where they match locally. Empty without a query.
    pub(super) fn search_found(&self, checkouts: &[Checkout]) -> Vec<UiAgent> {
        let query = self.nav.search.query.trim();
        if query.is_empty() {
            return Vec::new();
        }
        let answered = self.nav.found.query == query;
        let local =
            |agent: &UiAgent| nav::matches(agent, &nav::agent_place_label(agent, checkouts), query);
        let mut seen = HashSet::new();
        self.nav
            .found
            .rows
            .iter()
            .filter(|agent| answered || local(agent))
            .chain(self.nav.agents.iter().filter(|agent| local(agent)))
            .filter(|agent| seen.insert(agent.id.as_str()))
            .cloned()
            .collect()
    }

    /// Forget the server's search rows; a read still in flight is dropped.
    fn clear_found(&mut self) {
        self.nav.found = Found {
            epoch: self.nav.found.epoch + 1,
            ..Found::default()
        };
    }

    /// One page of the server's sidebar search (`agent.list` with `query`,
    /// newest first, the archived filter as the list shows it): a first
    /// page refreshes the matches, `after` appends the next. The loaded
    /// history is never touched.
    pub(super) fn load_search(&mut self, context: &ComponentContext<Self>, after: Option<String>) {
        let query = self.nav.search.query.trim().to_owned();
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if query.is_empty() {
            return;
        }
        let append = after.is_some();
        self.nav.found.epoch += 1;
        self.nav.found.loading = true;
        self.nav.found.appending = append;
        let epoch = self.nav.found.epoch;
        let archived = self.nav.show_archived.then_some("include");
        let request = AgentListRequest {
            after,
            limit: AGENT_PAGE,
            order: AgentListOrder::Recent,
            query: Some(query.clone()),
            ..Default::default()
        };
        self.spawn_read(context, move || {
            ShellMsg::SearchLoaded(
                epoch,
                query,
                append,
                load_agents_blocking(&data_dir, &request, archived),
            )
        });
    }

    /// Put the keyboard in the search box; from Settings, close them first.
    /// A refused or unbound focus is tried again, `tries` times in all.
    fn focus_search(&mut self, tries: u8, context: &ComponentContext<Self>) {
        if self.main != Main::Conversation {
            self.main = Main::Conversation;
            self.reprime_fields(context);
        }
        // The search box lives in the full sidebar.
        self.nav.sidebar_collapsed = false;
        self.nav.overlay_open = self.layout.class == split_ui::SizeClass::Compact;
        let Some(left) = tries.checked_sub(1) else {
            return;
        };
        let later = context.sender();
        let accepted = self.nav.search_ref.request_focus_result(move |result| {
            if !matches!(result, Ok(true)) && left > 0 {
                let _ = later.send(Message::Shell(ShellMsg::FocusSearchLater(left)));
            }
        });
        if !accepted && left > 0 {
            self.focus_later(ShellMsg::FocusSearch(left), context);
        }
    }

    /// Focus the control a quiet start held back (`take_focus`); a refused
    /// or unbound focus is tried again, `tries` times in all.
    fn focus_held_now(&self, target: FocusTarget, tries: u8, context: &ComponentContext<Self>) {
        let Some(left) = tries.checked_sub(1) else {
            return;
        };
        let later = context.sender();
        let retry = move |result: Result<bool, _>| {
            if !matches!(result, Ok(true)) && left > 0 {
                let _ = later.send(Message::Shell(ShellMsg::FocusHeldLater(target, left)));
            }
        };
        let accepted = match target {
            FocusTarget::Composer => self.chat.composer_ref.request_focus_result(retry),
            FocusTarget::Search => self.nav.search_ref.request_focus_result(retry),
        };
        if !accepted && left > 0 {
            self.focus_later(ShellMsg::FocusHeld(target, left), context);
        }
    }

    /// Send a focus retry after [`FOCUS_RETRY_MS`].
    fn focus_later(&self, retry: ShellMsg, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(FOCUS_RETRY_MS, &cancelled);
            Message::Shell(retry)
        });
    }

    /// Wait for the next Ctrl+N or Ctrl+K on a worker.
    pub(super) fn watch_shortcuts(&self, context: &ComponentContext<Self>) {
        let Some(chords) = self.nav.shortcuts.clone() else {
            return;
        };
        let _task = context.spawn_background(move |cancel| {
            let Ok(chords) = chords.lock() else {
                return Message::Shell(ShellMsg::ShortcutsEnded);
            };
            loop {
                // While keyboard focus is held, look for the window becoming
                // the foreground window at the focus retry pace.
                let wait = if crate::window_place::focus_held() {
                    FOCUS_RETRY_MS
                } else {
                    SHORTCUT_POLL_MS
                };
                match chords.recv_timeout(std::time::Duration::from_millis(wait)) {
                    Ok(chord) => return Message::Shell(ShellMsg::Shortcut(chord)),
                    Err(RecvTimeoutError::Timeout) if crate::window_place::held_focus_ready() => {
                        return Message::Shell(ShellMsg::Shortcut(Shortcut::Foreground));
                    }
                    Err(RecvTimeoutError::Timeout) if !cancel.is_cancelled() => {}
                    // Window closing or hook gone: nothing more to deliver.
                    Err(_) => return Message::Shell(ShellMsg::ShortcutsEnded),
                }
            }
        });
    }

    /// The collapsed projects changed: write `[desktop] collapsed_projects`
    /// through the settings writer, so the state survives a restart.
    fn collapsed_projects_changed(&mut self, context: &ComponentContext<Self>) {
        let mut collapsed: Vec<String> = self.nav.collapsed.iter().cloned().collect();
        collapsed.sort();
        if collapsed != self.prefs.collapsed {
            self.prefs.collapsed = collapsed;
            self.prefs_rev += 1;
            self.save_preferences(context);
        }
    }

    /// Reactor 0.100.0 creates a TextBox with `Text` before `AcceptsReturn`,
    /// and a TextBox that is still single-line keeps only the first line of
    /// the text it is given. Whenever the main tree is re-created the
    /// multi-line editors (composer, profile and script sources) therefore
    /// render one turn empty and get their text on the next message, once
    /// they already accept returns: the same path as a first load.
    pub(super) fn reprime_fields(&mut self, context: &ComponentContext<Self>) {
        self.fields_primed = false;
        self.prime_epoch += 1;
        self.prime_after = self.renders.get();
        self.post_fields_primed(self.prime_epoch, 0, context);
    }

    fn post_fields_primed(&self, epoch: u64, delay_ms: u64, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            if delay_ms > 0 {
                let _ = feed::backoff(delay_ms, &cancelled);
            }
            Message::Shell(ShellMsg::FieldsPrimed(epoch))
        });
    }

    /// The next sidebar refresh, `delay_ms` from now. Nothing is read while
    /// the window is minimized or hidden (checked locally, as the Monitor
    /// does); the first refresh after it shows again is immediate.
    pub(super) fn watch_list(&self, delay_ms: u64, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(delay_ms, &cancelled);
            while crate::window_place::hidden() {
                if !feed::backoff(monitor::HIDDEN_CHECK_MS, &cancelled) {
                    break;
                }
            }
            Message::Shell(ShellMsg::ListTick)
        });
    }

    /// The open Agent's row takes the state its detail just read.
    pub(super) fn sync_open_agent(&mut self) {
        let (Some(id), Some(detail)) = (self.selected_agent_id(), self.chat.detail.as_ref()) else {
            return;
        };
        let state = detail.state;
        for agent in self.nav.agents.iter_mut().filter(|agent| agent.id == id) {
            agent.state = state;
        }
        for branch in self.nav.branches.values_mut() {
            if let Branch::Loaded { children, .. } = branch {
                for agent in children.iter_mut().filter(|agent| agent.id == id) {
                    agent.state = state;
                }
            }
        }
    }

    /// A compact window shows the rail and opens the sidebar (or the
    /// Settings navigation in its place) in the navigation pane (§6.2);
    /// a wider one hides the sidebar to its rail by choice, never the
    /// Settings navigation.
    fn rail_shown(&self) -> bool {
        !self.link_view_shown()
            && (self.layout.class == split_ui::SizeClass::Compact
                || (self.main != Main::Settings && self.nav.sidebar_collapsed))
    }

    /// The right panel belongs to the conversation: Settings shows two
    /// columns (its navigation and the page), as in the Codex app.
    pub(super) fn panel_shown(&self) -> bool {
        self.panel != Panel::Closed
            && self.main == Main::Conversation
            && self.selected_agent_id().is_some()
            && !self.link_view_shown()
    }

    /// The main area is narrow (§6.2): a compact or medium window, or a Wide
    /// one whose pane docks beside the main area at its minimum width. The
    /// composer's parameters then take their own line.
    pub(super) fn narrow_main(&self) -> bool {
        match self.layout.class {
            split_ui::SizeClass::Compact | split_ui::SizeClass::Medium => true,
            split_ui::SizeClass::Wide => self.pane_docked(),
            split_ui::SizeClass::Wider => false,
        }
    }

    /// The open pane docks beside the main area (§6.2's docking condition,
    /// not Full view); otherwise it takes the main area, over the
    /// conversation, which keeps its scroll, selection and draft.
    pub(super) fn pane_docked(&self) -> bool {
        self.panel_shown() && !self.pane.full && self.layout.docks
    }

    /// Write an explicit theme or startup choice into `[desktop]` at once,
    /// carrying the current window size; one write at a time (a newer state
    /// is written when the running write reports back). A bare resize never
    /// writes: the size persists once, at close (see
    /// [`crate::settings::save_window_on_close`]).
    pub(super) fn save_preferences(&mut self, context: &ComponentContext<Self>) {
        if self.prefs_saving {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.prefs_saving = true;
        let preferences = self.prefs.clone();
        let _task = context.spawn_background(move |_| {
            let saved = client::block_on(config_client::save_preferences(
                &data_dir,
                preferences.clone(),
            ));
            Message::Shell(ShellMsg::PreferencesSaved(preferences, saved))
        });
    }

    /// Name the data directory the `[desktop]` write after the window
    /// closes goes to.
    pub(super) fn note_data_dir(&self) {
        if let Ok(data_dir) = self.data_dir.clone() {
            self.close_note.set_data_dir(data_dir);
        }
    }

    /// After `delay_ms`, start the daemon when needed (Owner only, the
    /// sibling `swarm.exe`), register the checkout `--workspace` named (only
    /// that: registering a project is the owner's explicit act), then read
    /// the settings.
    /// A cancelled task (superseded, or the window closing) starts
    /// nothing: its pause and its readiness wait end at the cancellation.
    pub(super) fn start_connection(
        &self,
        data_dir: PathBuf,
        delay_ms: u64,
        context: &ComponentContext<Self>,
    ) -> ComponentTask {
        let workspace = self.opened_workspace.clone();
        context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let stopped = || {
                Err(eliot_core::Error::new(
                    "cancelled",
                    "the start was cancelled before Swarm answered",
                ))
            };
            if !feed::backoff(delay_ms, &cancelled) || cancelled() {
                return Message::Shell(ShellMsg::DaemonReady(stopped()));
            }
            let started = client::block_on(feed::cancellable(
                async {
                    let executable = std::env::current_exe()
                        .map_err(|_| {
                            eliot_core::Error::new(
                                "platform_io",
                                "cannot locate desktop executable",
                            )
                        })?
                        .with_file_name("swarm.exe");
                    let acl_repaired =
                        eliot_daemon::startup::ensure_running(&data_dir, &executable).await?;
                    let mut discovered = None;
                    if let Some(workspace) = workspace {
                        let request = eliot_core::api::ProjectDiscoverRequest {
                            workspace: workspace.to_string_lossy().into_owned(),
                        };
                        let reply = client::call(&data_dir, &request).await?;
                        discovered = reply.project.map(|project| project.checkout);
                    }
                    let read = config_client::read_settings(&data_dir).await?;
                    Ok((discovered, read, acl_repaired))
                },
                &cancelled,
            ));
            Message::Shell(ShellMsg::DaemonReady(started.unwrap_or_else(stopped)))
        })
    }

    /// `text` once the editors are primed, empty during the priming render.
    pub(super) fn primed<'a>(&self, text: &'a str) -> &'a str {
        if self.fields_primed { text } else { "" }
    }

    /// The registered project a new task starts in, if any.
    pub(super) fn selected_project(&self) -> Option<&UiProject> {
        self.nav
            .projects
            .iter()
            .find(|project| project.workspace == self.launch.workspace)
    }

    /// Read one manager's direct children (`agent.list` with `parent`).
    fn load_branch(&mut self, id: &str, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if !matches!(self.nav.branches.get(id), Some(Branch::Loaded { .. })) {
            self.nav.branches.insert(id.to_owned(), Branch::Loading);
        }
        let request = AgentListRequest {
            limit: AGENT_PAGE,
            parent: Some(id.to_owned()),
            order: AgentListOrder::Recent,
            ..Default::default()
        };
        let (epoch, id) = (self.nav.tree_epoch, id.to_owned());
        let _task = context.spawn_background(move |_| {
            let page = load_agents_blocking(&data_dir, &request, None);
            Message::Shell(ShellMsg::BranchLoaded(epoch, id, page))
        });
    }

    pub(super) fn selected_agent_id(&self) -> Option<String> {
        self.nav.selection.agent.clone()
    }

    pub(super) fn load_agents(&mut self, context: &ComponentContext<Self>, after: Option<String>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let append = after.is_some();
        self.nav.list_epoch += 1;
        let list_epoch = self.nav.list_epoch;
        let request = AgentListRequest {
            after,
            limit: AGENT_PAGE,
            order: AgentListOrder::Recent,
            ..Default::default()
        };
        self.nav.agents_loading = true;
        self.nav.paging = true;
        let archived = self.nav.show_archived.then_some("include");
        let _task = context.spawn_background(move |_| {
            Message::Shell(ShellMsg::AgentsLoaded(
                list_epoch,
                append,
                load_agents_blocking(&data_dir, &request, archived),
            ))
        });
        if !append {
            // A head load also fetches the live and attention rows, so an
            // old waiting Agent beyond the first page lists immediately.
            self.load_attention(context);
        }
    }
    /// Re-read the organization readbacks the sidebar's projects show (one
    /// read at a time; the list poll and every refresh ask again).
    pub(super) fn load_organization(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.nav.organization_loading {
            return;
        }
        self.nav.organization_loading = true;
        let archived = if self.nav.show_archived {
            eliot_core::api::ArchivedFilter::Include
        } else {
            eliot_core::api::ArchivedFilter::Exclude
        };
        let changes_after = self.nav.organization.changes_after;
        self.spawn_read(context, move || {
            ShellMsg::OrganizationLoaded(Box::new(load_organization_blocking(
                &data_dir,
                archived,
                changes_after,
            )))
        });
    }

    /// Re-read the queue state (`launch.queue` waits and this window's
    /// queued intents) on a worker of its own, so the grouping reads never
    /// delay an arbiter's admission or refusal.
    pub(super) fn load_queue(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.nav.queue_loading {
            return;
        }
        self.nav.queue_loading = true;
        let watched: Vec<String> = self
            .launch
            .watched
            .iter()
            .map(|(id, _)| id.clone())
            .collect();
        self.spawn_read(context, move || {
            ShellMsg::QueueLoaded(Box::new(load_queue_blocking(&data_dir, watched)))
        });
    }

    /// The live and attention rows, fetched independently of the history
    /// page so an old waiting Agent is never cut off by newer history.
    fn load_attention(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let list_epoch = self.nav.list_epoch;
        let request = AgentListRequest {
            limit: ATTENTION_PAGE,
            states: Some(ATTENTION_STATES.to_vec()),
            order: AgentListOrder::Recent,
            ..Default::default()
        };
        self.spawn_read(context, move || {
            ShellMsg::AttentionLoaded(list_epoch, load_agents_blocking(&data_dir, &request, None))
        });
    }

    /// One `agent.list` per refresh (#826): normally the first recent page
    /// with every state's total, merged into the loaded history without
    /// dropping appended older pages, so a launch or a state change while
    /// the desktop is open arrives on the next poll, even after Show more.
    /// When the last totals moved, that refresh reads instead what they
    /// said is stale: the attention rows beyond the first page (an old
    /// waiting Agent is never cut off by newer history), or the archived
    /// counts.
    pub(super) fn refresh_head(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.load_attention_reports(context);
        if std::mem::take(&mut self.nav.attention_due) {
            self.nav.list_epoch += 1;
            self.nav.agents_loading = true;
            self.load_attention(context);
            return;
        }
        self.nav.list_epoch += 1;
        let list_epoch = self.nav.list_epoch;
        self.nav.agents_loading = true;
        if std::mem::take(&mut self.nav.archived_due) && !self.nav.show_archived {
            self.spawn_read(context, move || {
                ShellMsg::ArchivedLoaded(
                    list_epoch,
                    client::block_on(read_archived(&data_dir, AGENT_PAGE)),
                )
            });
            return;
        }
        self.nav.head_due = false;
        let archived = self.nav.show_archived.then_some("include");
        let head = AgentListRequest {
            limit: AGENT_PAGE,
            order: AgentListOrder::Recent,
            states: Some(eliot_core::api::AGENT_LIST_STATES.to_vec()),
            ..Default::default()
        };
        self.spawn_read(context, move || {
            ShellMsg::HeadLoaded(list_epoch, load_agents_blocking(&data_dir, &head, archived))
        });
    }

    /// A refresh read these totals with these first-page rows: the next
    /// refresh reads the attention rows when an attention state's total
    /// moved and not all of them are on the page, and the archived counts
    /// when the list's total moved. The first totals only set the mark (a
    /// full load read both).
    fn note_totals(&mut self, rows: &[UiAgent], totals: Vec<eliot_core::api::AgentStateTotal>) {
        let attention = |totals: &[eliot_core::api::AgentStateTotal]| -> Vec<usize> {
            ATTENTION_STATES
                .iter()
                .map(|state| {
                    totals
                        .iter()
                        .find(|entry| entry.state == *state)
                        .map_or(0, |entry| entry.total)
                })
                .collect()
        };
        let sum = |totals: &[eliot_core::api::AgentStateTotal]| -> usize {
            totals.iter().map(|entry| entry.total).sum()
        };
        if let Some(last) = self.nav.list_totals.as_deref() {
            let shown = rows
                .iter()
                .filter(|row| ATTENTION_STATES.contains(&row.state))
                .count();
            let now = attention(&totals);
            if now != attention(last) && now.iter().sum::<usize>() > shown {
                self.nav.attention_due = true;
            }
            if sum(&totals) != sum(last) {
                self.nav.archived_due = true;
            }
        }
        self.nav.list_totals = Some(totals);
    }

    /// The Agents page's fleet read (live rows) and its stopped rows feed
    /// the sidebar while that page shows, instead of a second list read:
    /// attention rows join the list as the attention read's would, and any
    /// other row updates a row already listed. Archived rows stay out
    /// unless Show archived lists them. A listed row the fleet left out, or
    /// one whose live state the fleet read whole without it, is not left
    /// with its old state: the next poll reads the sidebar's own first page,
    /// which says what it is now.
    pub(super) fn take_fleet_rows(&mut self, fleet: monitor::SidebarRows) {
        let monitor::SidebarRows {
            rows,
            left_out,
            whole,
        } = fleet;
        let read: HashSet<&str> = rows.iter().map(|row| row.id.as_str()).collect();
        if self.nav.agents.iter().any(|agent| {
            !read.contains(agent.id.as_str())
                && (left_out.contains(&agent.id) || whole.contains(&agent.state))
        }) {
            self.nav.head_due = true;
        }
        let mut changed = false;
        for row in rows {
            if row.archived && !self.nav.show_archived {
                continue;
            }
            match self.nav.agents.iter_mut().find(|agent| agent.id == row.id) {
                Some(known) => {
                    if *known != row {
                        *known = row;
                        changed = true;
                    }
                }
                None if ATTENTION_STATES.contains(&row.state) => {
                    self.nav.agents.push(row);
                    changed = true;
                }
                None => {}
            }
        }
        if changed {
            self.refresh_search();
        }
    }

    /// The Monitor's attention reports for the sidebar's rows: while the
    /// panel is open its refresh reads them; while it is closed the
    /// sidebar's refresh runs the same read into the same state, so one
    /// read is ever in flight.
    fn load_attention_reports(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.monitor_panel() || self.monitor.sidebar_reading {
            return;
        }
        self.monitor.sidebar_reading = true;
        let cursor = self.monitor.data.attention_cursor;
        self.spawn_read(context, move || {
            let read = client::block_on(monitor::read_attention(
                &data_dir,
                cursor,
                crate::handoff::now_ms(),
                &|| false,
            ));
            ShellMsg::AttentionReports(cursor, read)
        });
    }

    /// Read the projects again while a row's Git metadata refreshes (until
    /// its marker clears) or a launch policy editor shows the use counter
    /// (Settings › Projects with a project open). In the sidebar refresh
    /// it is that refresh's one `project.list`, and the next refresh waits
    /// for its reply. It skips while another project read is in flight, so
    /// a slow reply never overlaps the next tick.
    pub(super) fn poll_projects(&mut self, context: &ComponentContext<Self>) {
        let refreshing = self
            .nav
            .projects
            .iter()
            .any(|project| project.git_refreshing);
        let policy = self.main == Main::Settings
            && self.settings_page == Page::Projects
            && self.settings.policy.is_some();
        if !refreshing && !policy {
            return;
        }
        if self.nav.project_reads > 0 {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.nav.project_reads += 1;
        self.spawn_read(context, move || {
            ShellMsg::ProjectsPolled(load_projects_blocking(&data_dir))
        });
    }

    /// A `project.list` read failed: it alerts unless the same error is
    /// already the streak's alert ([`ShellState::projects_error`]).
    fn projects_failed(&mut self, error: &str) {
        let text = format!("Projects not read: {error}");
        if self.nav.projects_error.as_deref() != Some(text.as_str()) {
            self.nav.projects_error = Some(text.clone());
            self.alert(text);
        }
    }

    /// Read the projects again (start-up, a refresh, a settings save): it
    /// always runs, and a poll waits until it replies. Its task handle is
    /// dropped on purpose: Reactor's `ComponentTask` has no `Drop`, so the
    /// read runs on detached and replies as a message.
    pub(super) fn load_projects(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.nav.project_reads += 1;
        let _task = context.spawn_background(move |_| {
            Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(&data_dir)))
        });
    }

    /// Run one sidebar read on a worker: on its own, or in the refresh
    /// being collected, whose reads reply together (`ShellMsg::Refreshed`).
    pub(super) fn spawn_read(
        &mut self,
        context: &ComponentContext<Self>,
        read: impl FnOnce() -> ShellMsg + Send + 'static,
    ) {
        match self.nav.batch.as_mut() {
            Some(batch) => batch.push(Box::new(read)),
            None => {
                let _task = context.spawn_background(move |_| Message::Shell(read()));
            }
        }
    }

    /// The open attention report of a sidebar row's Agent (the Monitor's).
    fn reported(&self, agent: &UiAgent) -> Option<&monitor::Incident> {
        self.monitor
            .data
            .incident_of(&agent.id, agent.generation)
            .filter(|_| !agent.native_child && agent.state != eliot_core::AgentState::Stopped)
    }

    /// The window: sidebar, main area and the optional right panel.
    pub(super) fn shell_view(&self, context: &mut ViewContext<Self>) -> View {
        context.window_title("Eliot Swarm");
        // A bounded first window instead of WinUI's share of the monitor
        // (3826 px wide on a 5120 px display), resizable down to the
        // supported minimum client size (`[desktop] window`'s lower bound,
        // §6.2) and no further: Windows enforces it in every resize, drag or
        // programmatic. Reactor applies only visuals that differ from the
        // default, so this declaration also carries the title-bar theme.
        let (min_width, min_height) = eliot_daemon::DESKTOP_WINDOW_MIN;
        let visuals = WindowVisuals::new()
            .client_size(
                self.preferred_window
                    .map_or(tokens::layout::WINDOW_WIDTH, |w| f64::from(w.0)),
                self.preferred_window
                    .map_or(tokens::layout::WINDOW_HEIGHT, |w| f64::from(w.1)),
            )
            .constraints(WindowConstraints {
                min_width: Some(f64::from(min_width)),
                min_height: Some(f64::from(min_height)),
                ..WindowConstraints::default()
            })
            .theme(window_theme(self.prefs.theme));
        // The saved place and size, restored by Reactor before the first
        // show (`window_place`).
        context.window_visuals(match self.initial_bounds {
            Some(bounds) => visuals.initial_placement(bounds),
            None => visuals,
        });
        let main: View = match self.main {
            main if main != Main::Settings && self.link_view_shown() => {
                self.startup_error_view(context)
            }
            // Settings and the pages are drawn in the conversation's
            // place, which keeps its transcript (see `chat_view`).
            _ => self.chat_view(context),
        };
        // Settings always shows its navigation; tasks may hide the sidebar
        // to its rail.
        let rail = self.rail_shown();
        // The pane docks beside the main area, or lies over it (Full view,
        // or a window too narrow to dock it; see `split_ui`).
        let pane: Option<View> = self.panel_shown().then(|| self.panel_view(context));
        // Settings replaces the sidebar with its own navigation. While the
        // daemon screen shows, the sidebar keeps the last list it had,
        // dimmed, and takes no pointer input. A compact window opens the
        // sidebar over the main area, its rail staying in place.
        let compact = self.layout.class == split_ui::SizeClass::Compact;
        let overlay: Option<View> = (rail && compact && self.nav.overlay_open).then(|| {
            if self.main == Main::Settings {
                self.settings_nav(context)
            } else {
                View::component::<sidebar_ui::Sidebar>(self.sidebar_input(context))
            }
        });
        let side: View = match self.main {
            _ if rail => self.rail_view(context),
            Main::Settings => self.settings_nav(context),
            _ => View::component::<sidebar_ui::Sidebar>(self.sidebar_input(context)),
        };
        let side: View = if self.link_view_shown() && self.main != Main::Settings {
            Grid::new()
                .automation_name("Tasks and places (the last list, while Swarm is not reachable)")
                .children((
                    Border::new()
                        .opacity(tokens::state::DISABLED_CONTENT)
                        .content(side),
                    Border::new().background(Brush::Solid(Color::transparent())),
                ))
                .into()
        } else {
            side
        };
        let shell = View::component::<split_ui::SplitShell>(split_ui::SplitInput {
            side: split_ui::Built::new(side),
            mode: if rail {
                split_ui::Side::Rail
            } else {
                split_ui::Side::Panel
            },
            overlay: overlay.map(split_ui::Built::new),
            main: split_ui::Built::new(main),
            pane: pane.map(split_ui::Built::new),
            over: !self.pane_docked(),
            wide: self.pane.wide,
            widths: split_ui::PanelWidths::of(self.prefs.panels),
            on_layout: context.callback(|layout| Message::Shell(ShellMsg::Layout(layout))),
            on_widths: context.callback(|widths| Message::Shell(ShellMsg::PanelsResized(widths))),
            on_overlay_closed: context.callback(|_: bool| Message::Shell(ShellMsg::OverlayClosed)),
        });
        // One cell: the window, the row-menu button that carries the
        // sidebar's menu flyout and the attach chooser's button; the
        // confirmation dialog is attached to it (a Reactor 0.101 dialog is
        // an attachment, not a view).
        let overlay: Option<ContentDialog> = if self.handoff.open {
            self.handoff_dialog(context)
        } else if self.settings.cleanup_confirm {
            self.cleanup_confirmation(context)
        } else if let Some(dialog) = &self.bulk {
            Some(self.bulk_dialog(dialog, context))
        } else if self.link.confirm_fresh {
            Some(self.fresh_confirmation(context))
        } else if self.inbox.confirm.is_some() {
            self.inbox_confirmation(context)
        } else if self.settings.daemon_confirm.is_some() {
            Some(self.daemon_confirmation(context))
        } else if let Some(dialog) = self.goal_dialog(context) {
            Some(dialog)
        } else {
            Some(self.task_dialog(context))
        };
        let window = Grid::new().children((
            shell,
            self.menu_anchor(context),
            self.attach_anchor(context),
        ));
        match overlay {
            Some(dialog) => window.content_dialog(dialog),
            None => window.into(),
        }
    }

    /// The sidebar's sections now: a search's matches flat in their
    /// projects, else every loaded row nested in its project.
    fn sections_now(&self, checkouts: &[Checkout]) -> Vec<nav::Section> {
        let filtered = !self.nav.search.query.trim().is_empty();
        // A search lists what it found (the server's matches across every
        // Agent, plus the loaded rows matching while it reads).
        let found = self.search_found(checkouts);
        let listed: &[UiAgent] = if filtered { &found } else { &self.nav.agents };
        let keep = |_: &UiAgent| true;
        let open = self.selected_agent_id();
        // The no-project bucket is the Chats section: a label, never a
        // folded project.
        let mut collapsed = self.nav.collapsed.clone();
        collapsed.remove(nav::OTHER_LOCATIONS);
        let shown = Shown {
            branches: &self.nav.branches,
            expanded: &self.nav.expanded,
            collapsed: &collapsed,
            unfolded: &self.nav.unfolded,
            today_start: today_start_ms(),
            only: filtered.then_some(&keep as &dyn Fn(&UiAgent) -> bool),
            open: open.as_deref(),
        };
        nav::sections(listed, &self.nav.organization, checkouts, &shown)
    }

    /// The menu target of a sidebar row key (`agent:<id>`, `project:<key>`),
    /// as its "⋯" builds it.
    pub(super) fn row_target(&self, row: &str) -> Option<menu_ui::Target> {
        if let Some(id) = row.strip_prefix("agent:") {
            return self.find_agent(id).cloned().map(menu_ui::Target::Agent);
        }
        let key = row.strip_prefix("project:")?;
        let section = self
            .sections_now(&self.checkouts())
            .into_iter()
            .find(|section| section.key == key && section.key != nav::OTHER_LOCATIONS)?;
        Some(menu_ui::Target::Project {
            key: section.key.clone(),
            name: self
                .prefs
                .display_name(&format!("project:{}", section.key))
                .map_or_else(|| section.name.clone(), str::to_owned),
            checkout: section.checkout,
            checkouts: section.checkouts,
            collapsed: section.collapsed,
        })
    }

    /// The sidebar's rows as the window reads them ([`sidebar_ui::Sidebar`]
    /// draws them): the projects with their tasks nested inside them, a
    /// search's matches flat in their projects, the owner's names, the
    /// Monitor's reports and each row's age and work line, read now.
    fn sidebar_input(&self, context: &mut ViewContext<Self>) -> SidebarInput {
        let fleet = Fleet::of_totals(self.nav.list_totals.as_deref());
        let query = self.nav.search.query.trim().to_owned();
        let checkouts = self.checkouts();
        let filtered = !query.is_empty();
        // A search lists what it found (the server's matches across every
        // Agent, plus the loaded rows matching while it reads), flat in
        // their projects.
        let sections = self.sections_now(&checkouts);
        let empty = if !sections.is_empty() {
            None
        } else if self.nav.agents_loading || (filtered && self.nav.found.loading) {
            Some("Loading…")
        } else if filtered {
            Some("No matching tasks. Clear the search to see all.")
        } else {
            Some("No projects yet. Start a task with New task, or add one in Settings › Projects.")
        };
        let now = crate::handoff::now_ms();
        let places = sections
            .into_iter()
            .map(|mut section| {
                let name = self
                    .prefs
                    .display_name(&format!("project:{}", section.key))
                    .map_or_else(|| section.name.clone(), str::to_owned);
                // A renamed worktree shows the owner's name for it.
                let subproject = section.subproject.as_deref().map(|subproject| {
                    nav::subproject_key(&section.checkout, &checkouts)
                        .and_then(|key| self.prefs.display_name(&format!("project:{key}")))
                        .unwrap_or(subproject)
                        .to_owned()
                });
                let key = nav::path_key(&section.checkout);
                let opens = section.key != nav::OTHER_LOCATIONS
                    && checkouts
                        .iter()
                        .any(|checkout| !checkout.missing && nav::path_key(&checkout.path) == key);
                let registered = section
                    .missing
                    .then(|| {
                        self.nav
                            .projects
                            .iter()
                            .find(|project| nav::path_key(&project.workspace) == key)
                            .map(|project| project.id.clone())
                    })
                    .flatten();
                let tasks = std::mem::take(&mut section.rows)
                    .into_iter()
                    .map(|row| self.task_entry(row, now))
                    .collect();
                PlaceEntry {
                    selected: self.main == Main::Project && self.project.key == section.key,
                    section,
                    name,
                    subproject,
                    opens,
                    registered,
                    tasks,
                }
            })
            .collect();
        // Show more pages the list shown: the server's matches while
        // searching, the recent history otherwise.
        let (loaded, total, more, reading) = if filtered {
            (
                self.nav.found.rows.len(),
                self.nav.found.total,
                self.nav.found.query == query && self.nav.found.next.is_some(),
                self.nav.found.appending,
            )
        } else {
            (
                self.nav.agents.len(),
                self.nav.agents_total,
                self.nav.agents_next.is_some(),
                self.nav.paging,
            )
        };
        SidebarInput {
            can_back: self.nav.history.can_back(),
            can_forward: self.nav.history.can_forward(),
            page: (self.main == Main::Settings).then_some(self.settings_page),
            place: match self.main {
                Main::Inbox => Some(page_ui::Place::Inbox),
                Main::Agents => Some(page_ui::Place::Agents),
                Main::Automations => Some(page_ui::Place::Automations),
                _ => None,
            },
            search: self.nav.search.open.then(|| SearchField {
                text: self.nav.search.text.clone(),
                results: self
                    .nav
                    .search
                    .results
                    .iter()
                    .map(|(label, _)| label.clone())
                    .collect(),
                field: self.nav.search_ref.clone(),
            }),
            filtered,
            hovered: self.menu.hovered.clone(),
            focused: self.menu.focused.clone(),
            // While the daemon is checked or restarted, its notice speaks for
            // the reads it failed.
            errors: if self.link_unsure() {
                Vec::new()
            } else {
                self.nav
                    .organization
                    .errors
                    .iter()
                    .chain(&self.nav.organization.queue_errors)
                    .cloned()
                    .collect()
            },
            empty,
            places,
            more: (more && loaded < total)
                .then(|| (format!("Show more ({loaded} of {total})"), !reading)),
            show_archived: self.nav.show_archived,
            agents: AgentsEntry {
                parts: fleet.map_or_else(Vec::new, |fleet| {
                    fleet.compact(self.nav.agents_total, self.nav.archived)
                }),
                summary: fleet.map_or_else(
                    || "counts not read yet".to_owned(),
                    |fleet| fleet.summary(self.nav.agents_total, self.nav.archived),
                ),
                open: self.main == Main::Agents,
            },
            inbox: InboxEntry {
                counts: super::inbox_ui::counts_line(&self.inbox.counts),
                total: self.inbox.total(),
                open: self.main == Main::Inbox,
            },
            usage: self.usage_row(context),
            renaming: self.menu.renaming.as_ref().map(|(target, typed)| Renaming {
                target: target.clone(),
                typed: typed.clone(),
                tags: self.menu.tag_fields.clone(),
                field: self.menu.rename_ref.clone(),
            }),
            bulk_idle: self.bulk.is_none(),
            send: context.forward(),
        }
    }

    /// One task row as the sidebar shows it now.
    fn task_entry(&self, row: Row, now: i64) -> TaskEntry {
        let agent = &row.agent;
        let reported = self.reported(agent).map(|incident| {
            (
                if incident.no_code() {
                    "No code"
                } else {
                    "Stuck"
                },
                incident.words(now),
            )
        });
        let work = agent
            .work
            .as_ref()
            .filter(|_| reported.is_none() && agent.running() && !agent.native_child)
            .map(|work| {
                (
                    crate::monitor::work_line(work, now),
                    crate::monitor::work_words(work, now),
                )
            });
        TaskEntry {
            selected: self.main == Main::Conversation
                && self.nav.selection.agent.as_deref() == Some(agent.id.as_str()),
            label: self.agent_label(agent),
            reported,
            work,
            age: nav::short_age(now, agent.updated_at),
            row,
        }
    }

    /// The hidden sidebar's rail: show the sidebar, New task, then at the
    /// bottom Agents (the fleet view, with the fleet's most urgent dot) and
    /// Settings.
    fn rail_view(&self, context: &mut ViewContext<Self>) -> View {
        let pad = SIDEBAR_PADDING;
        let fleet = Fleet::of_totals(self.nav.list_totals.as_deref());
        let summary = fleet.map_or_else(
            || "counts not read yet".to_owned(),
            |fleet| fleet.summary(self.nav.agents_total, self.nav.archived),
        );
        let fleet = fleet.unwrap_or_default();
        let tone = if fleet.attention > 0 {
            Some(StatusTone::Attention)
        } else if fleet.running > 0 {
            Some(StatusTone::Active)
        } else {
            None
        };
        // The dot sits on the button's corner, clear of the icon.
        let dot: Option<View> = tone.map(|tone| {
            Border::new()
                .horizontal_alignment(HorizontalAlignment::Right)
                .vertical_alignment(VerticalAlignment::Top)
                .margin(Thickness::new(
                    0.0,
                    tokens::spacing::ROW_GAP,
                    tokens::spacing::ROW_GAP,
                    0.0,
                ))
                .content(controls::status_mark(tone, tokens::layout::DOT))
                .into()
        });
        let button = |glyph: &str, name: String, message: Message| {
            controls::icon_button()
                .automation_name(name)
                .on_click(context.message(message))
                .content(controls::icon(glyph))
        };
        let top = StackPanel::new()
            .margin(Thickness::new(
                pad,
                tokens::layout::SIDEBAR_PADDING,
                pad,
                0.0,
            ))
            .spacing(tokens::spacing::ROW_GAP)
            .children((
                button(
                    controls::glyph::SHOW_SIDEBAR,
                    "Show sidebar".to_owned(),
                    Message::Shell(ShellMsg::SidebarToggled),
                ),
                button(
                    controls::glyph::NEW_TASK,
                    "New task (Ctrl+N)".to_owned(),
                    Message::Shell(ShellMsg::NewTask),
                ),
            ));
        let open = self.main == Main::Agents;
        let bottom = StackPanel::new()
            .grid_row(1)
            .vertical_alignment(VerticalAlignment::Bottom)
            .margin(Thickness::new(
                pad,
                0.0,
                pad,
                tokens::layout::SIDEBAR_PADDING,
            ))
            .spacing(tokens::spacing::ROW_GAP)
            .children((
                Grid::new().keyed_children(crate::views::slots([
                    Some(
                        button(
                            controls::glyph::HUB,
                            format!(
                                "Agents: {summary} ({})",
                                if open { "open" } else { "closed" }
                            ),
                            Message::Shell(ShellMsg::PlaceOpened(page_ui::Place::Agents)),
                        )
                        .into(),
                    ),
                    dot,
                ])),
                button(
                    controls::glyph::SETTINGS,
                    "Settings".to_owned(),
                    Message::Shell(ShellMsg::SettingsOpened),
                ),
            ));
        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((top, bottom))
            .into()
    }

    /// Settings in the sidebar's place (the Codex settings navigation), on
    /// the sidebar's rows: a Back row, the pill search field that finds
    /// settings across the pages, then the pages under section labels.
    fn settings_nav(&self, context: &mut ViewContext<Self>) -> View {
        let pad = SIDEBAR_PADDING;
        let top = StackPanel::new()
            .margin(Thickness::new(pad, tokens::spacing::SM, pad, 0.0))
            .spacing(tokens::spacing::SM)
            .children((
                controls::row(false)
                    .automation_name("Back to tasks")
                    .on_click(context.message(Message::Shell(ShellMsg::SettingsClosed)))
                    .content(controls::nav_content(
                        controls::glyph::BACK,
                        "Back to tasks",
                        &[],
                    )),
                controls::search_field(
                    TextBox::new()
                        .text(self.settings.query.clone())
                        .placeholder_text("Search settings")
                        .automation_name("Search settings")
                        .on_text_changed(crate::views::text_edits(
                            context.callback(|text| Message::Shell(ShellMsg::SettingsSearch(text))),
                        )),
                ),
            ));
        let rows = self.settings_rows(context);
        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((
                top,
                ScrollViewer::new()
                    .grid_row(1)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .content(
                        StackPanel::new()
                            .margin(Thickness::new(pad, 0.0, pad, pad))
                            .spacing(tokens::spacing::ROW_GAP)
                            .keyed_children(rows),
                    ),
            ))
            .into()
    }

    /// Show a short notice above the composer (refusals, uncertain
    /// outcomes, failed reads); it stays until dismissed or replaced. An
    /// open queue offer keeps its notice, with Queue it (the same-id
    /// retry), on screen: other messages go to the status line meanwhile.
    pub(super) fn alert(&mut self, text: impl Into<String>) {
        let text = text.into();
        self.set_status(text.clone());
        if self
            .launch
            .queue_offer
            .as_ref()
            .is_some_and(|offer| offer.alert != text)
        {
            return;
        }
        self.alert = Some(text);
    }

    /// The right pane: its tabs over the open tab.
    fn panel_view(&self, context: &mut ViewContext<Self>) -> View {
        self.pane_view(context)
    }
}

/// Merge head or attention rows into the loaded list: replace known ids,
/// append the rest. Appended history stays deduplicated by id.
fn merge_head_rows(loaded: &mut Vec<UiAgent>, rows: Vec<UiAgent>) {
    for row in rows {
        if let Some(known) = loaded.iter_mut().find(|agent| agent.id == row.id) {
            *known = row;
        } else {
            loaded.push(row);
        }
    }
}

/// The start of the local day, in ms since the epoch.
fn today_start_ms() -> i64 {
    use chrono::{Local, TimeZone};
    Local::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .and_then(|midnight| Local.from_local_datetime(&midnight).earliest())
        .map_or(0, |start| start.timestamp_millis())
}
