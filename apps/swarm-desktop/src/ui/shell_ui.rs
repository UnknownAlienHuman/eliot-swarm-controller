//! The one shell: the sidebar (Search tasks, New task, collapsible
//! projects with their Agents nested, Recents, the fleet status and
//! Settings), the main area (one conversation or the Settings page) and the
//! optional right panel (the monitor or the open Agent's details).

use std::sync::mpsc::{Receiver, RecvTimeoutError};

use super::sidebar_ui::{FleetEntry, PlaceEntry, Renaming, SearchField, SidebarInput, TaskEntry};
use super::*;
use crate::nav::{Branch, Checkout, Fleet, Row, Shown};
use crate::settings::{Startup, ThemeChoice};
use crate::shortcuts::{Input, Shortcut};
use crate::theme::StatusTone;
use crate::theme::winui::Palette;

/// What the main area shows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Main {
    Conversation,
    Settings,
}

/// A place Back and Forward return to (Codex's history): the new-task
/// view in a project, a conversation, a Settings page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Location {
    /// The new-task view: its project's checkout, or a chat with no project.
    Home {
        checkout: String,
        chat: bool,
    },
    Agent(String),
    Settings(settings_ui::Page),
}

/// The optional right panel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Panel {
    Closed,
    /// Every Agent as a simple card.
    Monitor,
    /// The open conversation's Agent: settings, coverage, resources, usage
    /// and Change manager.
    Details,
}

#[derive(Clone, Debug)]
pub(super) enum ShellMsg {
    AutoSettings,
    RepairSettings,
    RetryStartup,
    /// Open this data directory's editable daemon configuration.
    OpenStartupConfig,
    /// The daemon runs (started when needed), the opened checkout was
    /// discovered, and the settings were read.
    DaemonReady(MutationResult<(Option<String>, eliot_core::api::SettingsReadResponse)>),
    /// The owner chose a fresh private data directory (unsafe default).
    UseFreshDirectory,
    ProjectsLoaded(Fallible<Vec<UiProject>>),
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
    /// The Monitor's attention reports, read on the sidebar's cadence
    /// while the panel is closed (the cursor the read started from).
    AttentionReports(Option<i64>, Fallible<monitor::AttentionRead>),
    /// The replies of one sidebar refresh, together: one window render.
    Refreshed(Vec<ShellMsg>),
    /// A sidebar-search page (`agent.list` with `query`): its read epoch,
    /// the query it answers, whether it appends, and the page.
    SearchLoaded(u64, String, bool, Fallible<AgentPage>),
    /// A sidebar row: open this Agent's conversation.
    AgentOpened(UiAgent),
    /// A project's new-task button: a new task in this checkout.
    ProjectPicked(String),
    /// A project's name: open the project (a new task in its checkout,
    /// its tasks unfolded). Carries the checkout and the section key.
    ProjectOpened(String, String),
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
    /// The Settings navigation's search field.
    SettingsSearch(String),
    /// The Settings navigation's Back row: the conversation again.
    SettingsClosed,
    PanelToggled(Panel),
    /// The window's actual Light/Dark scheme (Reactor `on_color_scheme`).
    SchemeObserved(ColorScheme),
    /// The window's client size (Reactor `on_window_size`).
    WindowResized(WindowSize),
    ThemeChosen(ThemeChoice),
    StartupChosen(Startup),
    /// The re-created editors of this re-prime exist: give them their text.
    FieldsPrimed(u64),
    /// The periodic High Contrast reading.
    HighContrastRead(bool),
    /// Expand (true) or collapse an Agent's children.
    ExpandToggled(String, bool),
    /// Tree epoch, manager id and its direct-children page.
    BranchLoaded(u64, String, Fallible<AgentPage>),
    /// The preferences and window size written to `[desktop]` (or not).
    PreferencesSaved(
        crate::settings::Preferences,
        Option<(u32, u32)>,
        MutationResult<eliot_core::api::SettingsSaveResponse>,
    ),
    AlertDismissed,
    /// Re-read the listed Agents (states, Recents order).
    ListTick,
    /// Hide the sidebar to a rail of icons, or show it again.
    SidebarToggled,
}

/// Reactor 0.100.0 reports the window's Light/Dark scheme only, and a High
/// Contrast theme can keep the same base scheme, so High Contrast is read
/// at the monitor's cadence.
const HIGH_CONTRAST_POLL_MS: u64 = 2_000;
/// Re-check for the empty editor render one frame later.
const PRIME_RETRY_MS: u64 = 16;
/// The sidebar's padding around its rows.
const SIDEBAR_PADDING: f64 = tokens::spacing::SM;
/// The hidden sidebar: a rail of icon buttons, one control wide.
const RAIL_WIDTH: f64 = tokens::height::CONTROL + 2.0 * SIDEBAR_PADDING;
/// Focus attempts for Ctrl+K, and the wait between them: right after a
/// cold start WinUI refuses focus until the window's content is ready.
const SEARCH_FOCUS_TRIES: u8 = 20;
const SEARCH_FOCUS_RETRY_MS: u64 = 100;
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

/// WinUI's own templates and the title bar take the theme's polarity.
fn window_theme(choice: ThemeChoice) -> WindowTheme {
    match choice {
        ThemeChoice::System => WindowTheme::System,
        other if other.theme(false).dark() => WindowTheme::Dark,
        _ => WindowTheme::Light,
    }
}

/// The scheme the window actually shows, and High Contrast.
pub(super) struct Appearance {
    pub(super) dark: bool,
    pub(super) high_contrast: bool,
}

impl Appearance {
    pub(super) fn new() -> Self {
        Self {
            dark: false,
            high_contrast: crate::accessibility::high_contrast_on(),
        }
    }

    /// The palette of `choice` on this window (System follows its
    /// observed Light/Dark scheme).
    pub(super) fn palette(&self, choice: ThemeChoice) -> theme::winui::Palette {
        theme::winui::Palette::new(choice.theme(self.dark), self.high_contrast)
    }
}

/// Projects, the listed Agents, expanded branches, titles and the selection.
/// One sidebar read, run on a worker ([`DesktopApp::spawn_read`]).
pub(super) type SidebarRead = Box<dyn FnOnce() -> ShellMsg + Send>;

pub(super) struct ShellState {
    /// The reads of the sidebar refresh being collected
    /// ([`DesktopApp::spawn_read`]).
    pub(super) batch: Option<Vec<SidebarRead>>,
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
    /// The sidebar is hidden to its rail (this session).
    pub(super) sidebar_collapsed: bool,
    /// The places visited, for Back and Forward.
    pub(super) history: crate::history::History<Location>,
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
            ShellMsg::RetryStartup => {
                if let Ok(data_dir) = self.data_dir.clone() {
                    self.startup_error = None;
                    self.start_connection(data_dir, context);
                }
            }
            ShellMsg::OpenStartupConfig => {
                if let Ok(data_dir) = &self.data_dir
                    && let Err(error) =
                        super::reveal::open_with_default_app(&data_dir.join("config.toml"))
                {
                    self.alert(error);
                }
            }
            ShellMsg::DaemonReady(result) => match result {
                Ok((discovered, read)) => {
                    self.startup_error = None;
                    let before = self.palette();
                    self.prefs = crate::settings::Preferences::from_reply(&read);
                    self.palette_changed(before, context);
                    self.nav.collapsed = self.prefs.collapsed.iter().cloned().collect();
                    self.launch_preferences_loaded();
                    self.seed_settings(read, context);
                    // The opened checkout, registered by discovery, is where
                    // a new task starts.
                    if let Some(checkout) = discovered {
                        self.launch.workspace = checkout;
                    }
                    let first_connection = !self.preferences_loaded;
                    self.preferences_loaded = true;
                    self.load_agents(context, None);
                    self.load_organization(context);
                    self.load_profile_list(context);
                    self.load_routing_preview(context);
                    // The picker opens on a read catalogue (the daemon caches it).
                    self.load_catalog(context);
                    if let Ok(data_dir) = self.data_dir.clone() {
                        let _task = context.spawn_background(move |_| {
                            Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(
                                &data_dir,
                            )))
                        });
                    }
                    if first_connection {
                        self.watch_list(context);
                    }
                }
                Err(error) => {
                    self.startup_error = Some(error);
                }
            },
            ShellMsg::UseFreshDirectory => {
                if let Ok(original) = client::data_dir() {
                    let fresh = original
                        .with_file_name(format!("Eliot-private-{}", client::new_mutation_id()));
                    match eliot_daemon::bootstrap::ensure_private_data_dir(&fresh).and_then(
                        |selected| {
                            eliot_daemon::persist_selected_root(&selected).map(|()| selected)
                        },
                    ) {
                        Ok(selected) => {
                            self.data_dir = Ok(selected.clone());
                            self.startup_error = None;
                            self.note_window_size();
                            self.alert = None;
                            self.set_status(format!(
                                "Starting in fresh private directory {}",
                                selected.display()
                            ));
                            self.start_connection(selected, context);
                        }
                        Err(error) => {
                            self.alert(format!("Cannot select fresh private directory: {error}"))
                        }
                    }
                }
            }
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
                organization.queue_errors = read.errors;
                self.settle_queued_launches(context);
            }
            ShellMsg::ProjectsLoaded(projects) => match projects {
                Ok(projects) => {
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
                Err(error) => self.alert(format!("Projects not read: {error}")),
            },
            ShellMsg::AgentsLoaded(list_epoch, append, page) => {
                // The composer's usage indicator reads every harness's quota
                // once the daemon answers (Usage's Refresh reads again).
                if self.settings.quotas.is_empty() {
                    self.load_quotas(context);
                }
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
                        self.alert(format!("Agents not read: {error}"));
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
                        merge_head_rows(&mut self.nav.agents, page.rows);
                        self.nav.agents_total = page.total;
                        if self.nav.agents.len() <= AGENT_PAGE {
                            self.nav.agents_next = page.next;
                            self.nav.archived = page.archived;
                        }
                        self.refresh_search();
                    }
                    Err(error) => {
                        self.alert(format!("Agents not read: {error}"));
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
                        self.alert(format!("Agents not read: {error}"));
                    }
                }
            }
            ShellMsg::Refreshed(replies) => {
                for reply in replies {
                    self.shell_update(reply, context);
                }
            }
            ShellMsg::AttentionReports(cursor, read) => {
                self.monitor.sidebar_reading = false;
                // An open panel's refresh owns the cursor meanwhile.
                if self.panel == Panel::Closed
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
            ShellMsg::SidebarToggled => self.nav.sidebar_collapsed = !self.nav.sidebar_collapsed,
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
                if self.panel == Panel::Details {
                    self.show_details(&agent.id, context);
                }
            }
            ShellMsg::ProjectPicked(checkout) => {
                if self.can_launch_here(&checkout) {
                    self.pick_project(&checkout);
                    self.new_task(context);
                }
            }
            ShellMsg::ProjectOpened(checkout, key) => {
                if self.can_launch_here(&checkout) {
                    self.pick_project(&checkout);
                    self.new_task(context);
                }
                if self.nav.collapsed.remove(&key) {
                    self.collapsed_projects_changed(context);
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
                if let Ok(data_dir) = self.data_dir.clone() {
                    let _task = context.spawn_background(move |_| {
                        Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(&data_dir)))
                    });
                }
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
                    self.chat.failures = 0;
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
            ShellMsg::SettingsSearch(text) => self.settings.query = text,
            ShellMsg::SettingsClosed => {
                // A preset's picker does not follow the owner out.
                self.close_popover();
                if self.main != Main::Conversation {
                    self.main = Main::Conversation;
                    self.reprime_fields(context);
                }
            }
            // The sidebar's Monitor entry opens and closes the Monitor;
            // the pane's X closes the pane.
            ShellMsg::PanelToggled(panel) => match panel {
                Panel::Monitor if self.panel == Panel::Monitor => self.close_panel(),
                Panel::Closed => self.close_panel(),
                panel => self.choose_panel(panel, context),
            },
            ShellMsg::SchemeObserved(scheme) => {
                let before = self.palette();
                self.appearance.dark = scheme == ColorScheme::Dark;
                self.appearance.high_contrast = crate::accessibility::high_contrast_on();
                self.palette_changed(before, context);
                // The first observation colors the title bar even when the
                // palette is the starting one.
                self.sync_title_bar();
            }
            ShellMsg::WindowResized(size) => {
                self.window_width = Some(size.width);
                self.window_height = Some(size.height);
                self.sync_title_bar();
                // A maximized size is not the size the window restores to.
                if size.width >= 640.0 && size.height >= 480.0 && !crate::window_place::maximized()
                {
                    let window = (size.width as u32, size.height as u32);
                    if self.window_size != Some(window) {
                        self.window_size = Some(window);
                        // No write here: the size persists once, at close.
                        self.note_window_size();
                    }
                }
            }
            ShellMsg::HighContrastRead(on) => {
                if on != self.appearance.high_contrast {
                    self.appearance.high_contrast = on;
                    self.apply_scheme();
                    self.reprime_fields(context);
                }
                self.watch_high_contrast(context);
            }
            ShellMsg::ThemeChosen(theme) => {
                let before = self.palette();
                self.prefs.theme = theme;
                self.prefs_rev += 1;
                self.save_preferences(context);
                // A theme of the same polarity raises no scheme change.
                self.palette_changed(before, context);
            }
            ShellMsg::StartupChosen(startup) => {
                self.prefs.startup = startup;
                self.prefs_rev += 1;
                self.save_preferences(context);
            }
            ShellMsg::PreferencesSaved(saved, window, result) => {
                self.prefs_saving = false;
                if let Err(error) = &result {
                    self.alert(format!("Preferences not saved: {error}"));
                }
                // A choice made on the page reports there; window-size
                // and composer writes stay quiet.
                if self.main == Main::Settings || result.is_err() {
                    self.settings
                        .notes
                        .insert(self.settings_page, settings_ui::Note::of(false, &result));
                }
                // A newer change arrived while this one was written.
                if saved != self.prefs || window != self.window_size {
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
                    self.focus_search(SEARCH_FOCUS_TRIES, context);
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
                match shortcut {
                    Shortcut::Popover(key) if self.dialog_kind() != 0 => {
                        self.dialog_key(key, context)
                    }
                    _ if self.dialog_kind() != 0 => {}
                    Shortcut::NewTask => self.new_task(context),
                    Shortcut::Back => self.step(false, context),
                    Shortcut::Forward => self.step(true, context),
                    Shortcut::Search => {
                        self.nav.search.open = true;
                        self.focus_search(SEARCH_FOCUS_TRIES, context)
                    }
                    Shortcut::Settings => self.open_settings(None, context),
                    Shortcut::ModelPicker => {
                        if self.launch.open != Some(launch_ui::Pop::Model) {
                            self.launch_update(LaunchMsg::Toggle(launch_ui::Pop::Model), context);
                        }
                    }
                    Shortcut::Popover(key) if self.menu.open.is_some() => {
                        use crate::shortcuts::PopoverKey;
                        match key {
                            PopoverKey::Escape | PopoverKey::Tab => self.close_menu(),
                            // Arrows reach clipped actions from the
                            // keyboard; Enter chooses the highlighted row.
                            PopoverKey::Up => self.menu_update(MenuMsg::HighlightMove(-1), context),
                            PopoverKey::Down => {
                                self.menu_update(MenuMsg::HighlightMove(1), context)
                            }
                            PopoverKey::Enter => {
                                self.menu_update(MenuMsg::HighlightActivate, context)
                            }
                            _ => {}
                        }
                    }
                    Shortcut::Popover(key) => self.launch_update(LaunchMsg::Key(key), context),
                    Shortcut::ComposerPressed => self.composer_pressed(),
                    Shortcut::FocusMoved => {
                        let row = crate::motion::resolve_focused_row();
                        if self.menu.focused != row {
                            self.menu.focused = row;
                        }
                    }
                    Shortcut::RowMenu => {
                        if let Some((row, x, y)) = crate::motion::focused_row_anchor()
                            && let Some(target) = self.row_target(&row)
                        {
                            self.menu_update(MenuMsg::Requested(target, x, y), context);
                        }
                    }
                    Shortcut::Enter(_) if self.menu.renaming.is_some() => {
                        self.menu_update(MenuMsg::RenameCommitted, context);
                    }
                    Shortcut::Enter(epoch) => {
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
            ShellMsg::FocusSearchLater(tries) => self.focus_search_later(tries, context),
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
                // refresh's reads reply together, as one message.
                self.nav.batch = Some(Vec::new());
                if self.preferences_loaded && !self.nav.agents_loading {
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
                let reads = self.nav.batch.take().unwrap_or_default();
                if !reads.is_empty() {
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
                self.watch_list(context);
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

    /// Write the shown scheme's M3 roles at the application root (removed
    /// under High Contrast), before the palette-keyed tree is rebuilt.
    /// Rebuild with the new palette when it differs from `before`.
    pub(super) fn palette_changed(
        &mut self,
        before: theme::winui::Palette,
        context: &ComponentContext<Self>,
    ) {
        if self.palette() != before {
            // The palette-keyed tree is rebuilt with new controls (an empty
            // search box among them).
            self.nav.search = Search::default();
            self.apply_scheme();
            self.reprime_fields(context);
        }
    }

    fn apply_scheme(&mut self) {
        if let Err(error) = crate::app_resources::apply_scheme(self.palette().roles()) {
            self.set_status(format!("WinUI M3 resources not applied: {error}"));
        }
        self.sync_title_bar();
    }

    /// Color the title bar in the shown palette once the window exists and
    /// whenever the palette changes (a runtime theme switch, High Contrast).
    /// Until the window is shown the colors stay pending and the next scheme
    /// or size observation tries again.
    fn sync_title_bar(&mut self) {
        let palette = self.palette();
        if self.title_bar == Some(palette) {
            return;
        }
        match crate::app_resources::apply_title_bar(palette.roles()) {
            Ok(()) => self.title_bar = Some(palette),
            Err(error) => self.set_status(format!("Title bar colors not applied: {error}")),
        }
    }

    /// Open Settings, at `page` when given.
    fn open_settings(&mut self, page: Option<settings_ui::Page>, context: &ComponentContext<Self>) {
        if self.startup_error.is_some() && !self.repairing_settings() {
            return;
        }
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

    pub(super) fn repairing_settings(&self) -> bool {
        self.startup_error.as_ref().is_some_and(|error| {
            error.failure_where.as_deref() == Some(eliot_daemon::startup::CONFIG_STARTUP_LOCATION)
        })
    }

    /// Where the owner is now, for Back and Forward (nothing while the
    /// daemon is not reached).
    pub(super) fn location(&self) -> Option<Location> {
        if self.startup_error.is_some() && self.main != Main::Settings {
            return None;
        }
        Some(match self.main {
            Main::Settings => Location::Settings(self.settings_page),
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
    pub(super) fn record_location(&mut self) {
        if let Some(place) = self.location() {
            self.nav.history.visit(place);
        }
    }

    /// One step back (or forward) through the places visited. A task that
    /// is gone from the lists is skipped.
    fn step(&mut self, forward: bool, context: &ComponentContext<Self>) {
        loop {
            let place = if forward {
                self.nav.history.forward()
            } else {
                self.nav.history.back()
            };
            let Some(place) = place else {
                return;
            };
            if self.go_to(&place, context) {
                return;
            }
            self.nav.history.forget(&place);
        }
    }

    /// Show `place`; false when it no longer exists.
    fn go_to(&mut self, place: &Location, context: &ComponentContext<Self>) -> bool {
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
                true
            }
            Location::Agent(id) => match self.find_agent(id).cloned() {
                Some(agent) => {
                    self.shell_update(ShellMsg::AgentOpened(agent), context);
                    true
                }
                None => false,
            },
            Location::Settings(page) => {
                self.open_settings(Some(*page), context);
                true
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
        let _ = self.chat.composer_ref.request_focus();
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
        if self.panel == Panel::Details {
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

    pub(super) fn can_launch_here(&self, path: &str) -> bool {
        let key = nav::path_key(path);
        self.checkouts()
            .iter()
            .any(|checkout| !checkout.missing && nav::path_key(&checkout.path) == key)
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
            self.focus_search_later(left, context);
        }
    }

    fn focus_search_later(&self, tries: u8, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(SEARCH_FOCUS_RETRY_MS, &cancelled);
            Message::Shell(ShellMsg::FocusSearch(tries))
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
                match chords.recv_timeout(std::time::Duration::from_millis(SHORTCUT_POLL_MS)) {
                    Ok(chord) => return Message::Shell(ShellMsg::Shortcut(chord)),
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

    /// The next sidebar refresh, one cadence from now.
    pub(super) fn watch_list(&self, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(|cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(LIST_REFRESH_MS, &cancelled);
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

    /// Read High Contrast at its cadence and report only a change. Every
    /// message re-renders the whole window (k209 measured 30 ms per render,
    /// 70 ms with a 588-item chat open), so an unchanged reading posts
    /// nothing.
    pub(super) fn watch_high_contrast(&self, context: &ComponentContext<Self>) {
        let shown = self.appearance.high_contrast;
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            loop {
                let ran = feed::backoff(HIGH_CONTRAST_POLL_MS, &cancelled);
                let on = crate::accessibility::high_contrast_on();
                if on != shown || !ran {
                    return Message::Shell(ShellMsg::HighContrastRead(on));
                }
            }
        });
    }

    /// The main area's width from the window width, the sidebar (its rail
    /// when hidden, the Settings navigation in its place) and the open
    /// panel, or None until the first size observation.
    pub(super) fn main_width(&self) -> Option<f64> {
        let panel = if self.panel_shown() {
            tokens::layout::PANEL_WIDTH
        } else {
            0.0
        };
        let side = if self.rail_shown() {
            RAIL_WIDTH
        } else {
            tokens::layout::SIDEBAR_WIDTH
        };
        self.window_width.map(|width| width - side - panel)
    }

    /// Tasks hide the sidebar to its rail, by choice or when the window is
    /// too narrow to keep the task area at its minimum beside the sidebar
    /// (and the open panel); Settings always shows its navigation.
    fn rail_shown(&self) -> bool {
        if self.main == Main::Settings || self.startup_error.is_some() {
            return false;
        }
        let panel = if self.panel_shown() {
            tokens::layout::PANEL_WIDTH
        } else {
            0.0
        };
        self.nav.sidebar_collapsed
            || self.window_width.is_some_and(|width| {
                width - tokens::layout::SIDEBAR_WIDTH - panel < tokens::layout::MAIN_MIN_WIDTH
            })
    }

    /// The right panel belongs to the conversation: Settings shows two
    /// columns (its navigation and the page), as in the Codex app.
    fn panel_shown(&self) -> bool {
        self.panel != Panel::Closed
            && self.main == Main::Conversation
            && self.startup_error.is_none()
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
        let (preferences, window) = (self.prefs.clone(), self.window_size);
        let _task = context.spawn_background(move |_| {
            let saved = client::block_on(config_client::save_preferences(
                &data_dir,
                preferences.clone(),
                window,
            ));
            Message::Shell(ShellMsg::PreferencesSaved(preferences, window, saved))
        });
    }

    /// Remember the last observed size (and its data directory) for the one
    /// `[desktop]` write after the window closes.
    pub(super) fn note_window_size(&mut self) {
        if let (Ok(data_dir), Some(window)) = (self.data_dir.clone(), self.window_size) {
            self.close_window.store(data_dir, window);
        }
    }

    /// Start the daemon when needed (Owner only, the sibling `swarm.exe`),
    /// register the checkout `--workspace` named (only that: registering a
    /// project is the owner's explicit act), then read the settings.
    pub(super) fn start_connection(&self, data_dir: PathBuf, context: &ComponentContext<Self>) {
        let workspace = self.opened_workspace.clone();
        let _task = context.spawn_background(move |_| {
            let result = client::block_on(async {
                let executable = std::env::current_exe()
                    .map_err(|_| {
                        eliot_core::Error::new("platform_io", "cannot locate desktop executable")
                    })?
                    .with_file_name("swarm.exe");
                eliot_daemon::startup::ensure_running(&data_dir, &executable).await?;
                let mut discovered = None;
                if let Some(workspace) = workspace {
                    let request = eliot_core::api::ProjectDiscoverRequest {
                        workspace: workspace.to_string_lossy().into_owned(),
                    };
                    request.validate()?;
                    let reply: eliot_core::api::ProjectDiscoverResponse =
                        eliot_daemon::client_call_typed(&data_dir, "project.discover", &request)
                            .await?;
                    request.validate_response(&reply)?;
                    discovered = reply.project.map(|project| project.checkout);
                }
                let read = config_client::read_settings(&data_dir).await?;
                Ok((discovered, read))
            });
            Message::Shell(ShellMsg::DaemonReady(result))
        });
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

    /// Re-read the first recent page and the live and attention rows, then
    /// merge them into the loaded history without dropping appended older
    /// pages: a launch or a state change while the desktop is open arrives
    /// on the next poll, even after Show more.
    fn refresh_head(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.nav.list_epoch += 1;
        let list_epoch = self.nav.list_epoch;
        self.nav.agents_loading = true;
        let archived = self.nav.show_archived.then_some("include");
        let head = AgentListRequest {
            limit: AGENT_PAGE,
            order: AgentListOrder::Recent,
            ..Default::default()
        };
        self.spawn_read(context, move || {
            ShellMsg::HeadLoaded(list_epoch, load_agents_blocking(&data_dir, &head, archived))
        });
        self.load_attention(context);
        self.load_attention_reports(context);
    }

    /// The Monitor's attention reports for the sidebar's rows: while the
    /// panel is open its refresh reads them; while it is closed the
    /// sidebar's refresh runs the same read into the same state, so one
    /// read is ever in flight.
    fn load_attention_reports(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.panel != Panel::Closed || self.monitor.sidebar_reading {
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

    /// Run one sidebar read on a worker: on its own, or in the refresh
    /// being collected, whose reads reply together (`ShellMsg::Refreshed`).
    fn spawn_read(
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
        // (3826 px wide on a 5120 px display). Reactor applies only visuals
        // that differ from the default, so this declaration also carries the
        // title-bar theme.
        context.window_visuals(
            WindowVisuals::new()
                .client_size(
                    self.preferred_window
                        .map_or(tokens::layout::WINDOW_WIDTH, |w| f64::from(w.0)),
                    self.preferred_window
                        .map_or(tokens::layout::WINDOW_HEIGHT, |w| f64::from(w.1)),
                )
                .theme(window_theme(self.prefs.theme)),
        );
        context.on_color_scheme(
            context.callback(|scheme| Message::Shell(ShellMsg::SchemeObserved(scheme))),
        );
        context
            .on_window_size(context.callback(|size| Message::Shell(ShellMsg::WindowResized(size))));
        let palette = self.palette();
        m3::set_palette(palette);

        let main: View = match self.main {
            Main::Conversation if self.startup_error.is_some() => {
                self.startup_error_view(context, palette)
            }
            // Settings is drawn in the conversation's place, which keeps
            // its transcript (see `chat_view`).
            Main::Conversation | Main::Settings => self.chat_view(context, palette),
        };
        // Settings always shows its navigation; tasks may hide the sidebar
        // to its rail.
        let rail = self.rail_shown();
        let mut columns = vec![
            GridLength::Pixel(if rail {
                RAIL_WIDTH
            } else {
                tokens::layout::SIDEBAR_WIDTH
            }),
            GridLength::STAR,
        ];
        let panel: View = if !self.panel_shown() {
            View::empty()
        } else {
            columns.push(GridLength::Pixel(tokens::layout::PANEL_WIDTH));
            Border::new()
                .grid_column(2)
                .background(palette.surface())
                .border_brush(palette.outline())
                .border_thickness(Thickness::new(tokens::layout::HAIRLINE, 0.0, 0.0, 0.0))
                .content(self.panel_view(context, palette))
        };
        // Settings replaces the sidebar with its own navigation.
        let side: View = match self.main {
            Main::Settings => self.settings_nav(context, palette),
            _ if rail => self.rail_view(context, palette),
            _ => View::component::<sidebar_ui::Sidebar>(self.sidebar_input(context, palette)),
        };
        let columns_grid = Grid::new()
            .background(palette.surface())
            .columns(columns)
            .children((
                Border::new()
                    .grid_column(0)
                    .background(palette.sidebar())
                    .content(side),
                Border::new().grid_column(1).content(main),
                panel,
            ));
        // One cell: the window, and over it the right-click menu or the
        // confirmation dialog.
        let overlay = if self.handoff.open {
            self.handoff_dialog(context)
        } else if self.settings.cleanup_confirm {
            self.cleanup_confirmation(context, palette)
        } else if let Some(dialog) = &self.bulk {
            self.bulk_dialog(dialog, context, palette)
        } else {
            self.task_dialog(context, palette)
        };
        let window =
            Grid::new().children((columns_grid, self.menu_overlay(context, palette), overlay));
        // Reactor updates a Button's resource overrides in place, but WinUI
        // resolves a template's ThemeResource values only when the template
        // is applied or the theme changes, and the theme change arrives before
        // the new palette. Keying the tree by palette rebuilds the native
        // controls on a scheme switch (rare) so every button takes the new
        // overrides; all view state is model-bound and is redrawn as it was.
        let palette_key = palette.roles().map_or_else(
            || "high-contrast".to_owned(),
            |roles| format!("{:?}", roles.theme),
        );
        Grid::new().keyed_children(vec![KeyedView::new(palette_key, window)])
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
        let shown = Shown {
            branches: &self.nav.branches,
            expanded: &self.nav.expanded,
            collapsed: &self.nav.collapsed,
            unfolded: &self.nav.unfolded,
            today_start: today_start_ms(),
            only: filtered.then_some(&keep as &dyn Fn(&UiAgent) -> bool),
            open: open.as_deref(),
        };
        nav::sections(listed, &self.nav.organization, checkouts, &shown)
    }

    /// The menu target of a sidebar row key (`agent:<id>`, `project:<key>`),
    /// as its "⋯" builds it.
    fn row_target(&self, row: &str) -> Option<menu_ui::Target> {
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
    fn sidebar_input(&self, context: &mut ViewContext<Self>, palette: Palette) -> SidebarInput {
        let fleet = Fleet::of(&self.nav.agents, |agent| self.reported(agent).is_some());
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
        let tone = if fleet.attention > 0 {
            StatusTone::Attention
        } else if fleet.running > 0 {
            StatusTone::Active
        } else {
            StatusTone::Ready
        };
        SidebarInput {
            palette,
            can_back: self.nav.history.can_back(),
            can_forward: self.nav.history.can_forward(),
            home: self.main == Main::Conversation && self.chat.convo.is_none(),
            page: (self.main == Main::Settings).then_some(self.settings_page),
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
            errors: self
                .nav
                .organization
                .errors
                .iter()
                .chain(&self.nav.organization.queue_errors)
                .cloned()
                .collect(),
            empty,
            places,
            more: (more && loaded < total)
                .then(|| (format!("Show more ({loaded} of {total})"), !reading)),
            show_archived: self.nav.show_archived,
            fleet: FleetEntry {
                line: fleet.compact(self.nav.agents_total, self.nav.archived),
                summary: fleet.summary(self.nav.agents_total, self.nav.archived),
                tone,
                open: self.panel == Panel::Monitor,
            },
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
        let reported = self.reported(agent).map(|incident| incident.words(now));
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
    /// bottom the Monitor (with the fleet's most urgent dot) and Settings.
    fn rail_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let pad = SIDEBAR_PADDING;
        let fleet = Fleet::of(&self.nav.agents, |agent| self.reported(agent).is_some());
        let summary = fleet.summary(self.nav.agents_total, self.nav.archived);
        let tone = if fleet.attention > 0 {
            Some(StatusTone::Attention)
        } else if fleet.running > 0 {
            Some(StatusTone::Active)
        } else {
            None
        };
        // The dot sits on the button's corner, clear of the icon.
        let dot: View = tone.map_or_else(View::empty, |tone| {
            Border::new()
                .horizontal_alignment(HorizontalAlignment::Right)
                .vertical_alignment(VerticalAlignment::Top)
                .margin(Thickness::new(
                    0.0,
                    tokens::spacing::ROW_GAP,
                    tokens::spacing::ROW_GAP,
                    0.0,
                ))
                .content(m3::status_mark(palette, tone, tokens::layout::DOT))
        });
        let button = |glyph: &str, name: String, message: Message| {
            m3::icon_button(palette)
                .height(tokens::height::CONTROL)
                .automation_name(name)
                .on_click(context.message(message))
                .content(m3::icon(glyph))
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
                    m3::glyph::SHOW_SIDEBAR,
                    "Show sidebar".to_owned(),
                    Message::Shell(ShellMsg::SidebarToggled),
                ),
                button(
                    m3::glyph::NEW_TASK,
                    "New task (Ctrl+N)".to_owned(),
                    Message::Shell(ShellMsg::NewTask),
                ),
            ));
        let open = self.panel == Panel::Monitor;
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
                Grid::new().children((
                    button(
                        m3::glyph::PULSE,
                        format!(
                            "Monitor: {summary} ({})",
                            if open { "open" } else { "closed" }
                        ),
                        Message::Shell(ShellMsg::PanelToggled(Panel::Monitor)),
                    ),
                    dot,
                )),
                button(
                    m3::glyph::SETTINGS,
                    "Settings".to_owned(),
                    Message::Shell(ShellMsg::SettingsOpened),
                ),
            ));
        Grid::new()
            .rows([GridLength::Auto, GridLength::STAR])
            .children((top, bottom))
    }

    /// Settings in the sidebar's place (the Codex settings navigation), on
    /// the sidebar's rows: a Back row, the pill search field that finds
    /// settings across the pages, then the pages under section labels.
    fn settings_nav(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let pad = SIDEBAR_PADDING;
        let top = StackPanel::new()
            .margin(Thickness::new(pad, tokens::spacing::SM, pad, 0.0))
            .spacing(tokens::spacing::SM)
            .children((
                m3::row(palette, false)
                    .automation_name("Back to tasks")
                    .on_click(context.message(Message::Shell(ShellMsg::SettingsClosed)))
                    .content(m3::nav_content(m3::glyph::BACK, "Back to tasks", "")),
                // On the sidebar the field takes the container tone (the
                // tonal one would vanish into it).
                m3::search_field(
                    palette.container(),
                    TextBox::new()
                        .text(self.settings.query.clone())
                        .placeholder_text("Search settings")
                        .automation_name("Search settings")
                        .on_text_changed(
                            context.callback(|text| Message::Shell(ShellMsg::SettingsSearch(text))),
                        ),
                ),
            ));
        let rows = self.settings_rows(context, palette);
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

    /// The data directory could not be opened (for example, the default
    /// root is owned by another principal). Nothing is moved; the owner may
    /// select a fresh private directory, which is remembered.
    fn startup_error_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let error = self
            .startup_error
            .as_ref()
            .map_or_else(String::new, ToString::to_string);
        StackPanel::new()
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .vertical_alignment(VerticalAlignment::Center)
            .spacing(tokens::spacing::MD)
            .margin(Thickness::uniform(tokens::spacing::LG))
            .children((
                m3::page_title("Swarm daemon could not start"),
                m3::secondary(error)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
                if self.repairing_settings() {
                    StackPanel::new().spacing(tokens::spacing::SM).children((
                        m3::filled(palette)
                            .automation_name("Repair settings")
                            .on_click(context.message(Message::Shell(ShellMsg::RepairSettings)))
                            .content(m3::label("Repair settings")),
                        m3::text_button(palette)
                            .automation_name("Open config.toml")
                            .on_click(context.message(Message::Shell(ShellMsg::OpenStartupConfig)))
                            .content(m3::link_content("Open config.toml")),
                    ))
                } else {
                    View::empty()
                },
                m3::text_button(palette)
                    .automation_name("Retry startup")
                    .on_click(context.message(Message::Shell(ShellMsg::RetryStartup)))
                    .content(m3::label("Retry startup")),
                // One filled action per page: Repair settings when the
                // config is what failed, else the fresh directory.
                if self.repairing_settings() {
                    m3::outlined(palette)
                } else {
                    m3::filled(palette)
                }
                .automation_name("Use fresh private directory")
                .on_click(context.message(Message::Shell(ShellMsg::UseFreshDirectory)))
                .content(m3::label("Use a fresh private directory")),
            ))
    }

    /// The right panel: the monitor or the open Agent's details.
    fn panel_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        match self.panel {
            Panel::Monitor => self.monitor_view(context, palette),
            Panel::Details => self.details_view(context, palette),
            Panel::Closed => View::empty(),
        }
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
