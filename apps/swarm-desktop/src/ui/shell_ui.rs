//! The one shell: the sidebar (Search tasks, New task, collapsible
//! projects with their Agents nested, Recents, the fleet status and
//! Settings), the main area (one conversation or the Settings page) and the
//! optional right panel (the monitor or the open Agent's details).

use std::sync::mpsc::{Receiver, RecvTimeoutError};

use super::menu_ui::{Target, with_menu};
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
    /// The daemon runs (started when needed), the opened checkout was
    /// discovered, and the settings were read.
    DaemonReady(Fallible<(Option<String>, eliot_core::api::SettingsReadResponse)>),
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
    /// A sidebar row: open this Agent's conversation.
    AgentOpened(UiAgent),
    /// A project's new-task button: a new task in this checkout.
    ProjectPicked(String),
    /// A project row: collapse or expand the project (its section key).
    ProjectToggled(String),
    /// A project's "Show N older" row: unfold or fold its older tasks.
    OlderToggled(String),
    /// The attention row: list only the Agents that need attention, or all.
    /// The search box's text (typed, or a suggestion WinUI wrote into it).
    SearchChanged(String),
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
    NewTask,
    MoreAgentsRequested,
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
/// Segoe Fluent Icons ClosePane and OpenPane: hide and show the sidebar.
const GLYPH_HIDE_SIDEBAR: &str = "\u{E89F}";
const GLYPH_SHOW_SIDEBAR: &str = "\u{E8A0}";
/// Rows in Recents.
const RECENTS: usize = 8;
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

fn window_theme(choice: ThemeChoice) -> WindowTheme {
    match choice {
        ThemeChoice::System => WindowTheme::System,
        ThemeChoice::Light => WindowTheme::Light,
        ThemeChoice::Dark => WindowTheme::Dark,
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

    pub(super) fn palette(&self) -> theme::winui::Palette {
        theme::winui::Palette::new(self.dark, self.high_contrast)
    }
}

/// Projects, the listed Agents, expanded branches, titles and the selection.
pub(super) struct ShellState {
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
    /// Ctrl+N and Ctrl+K from the window's keyboard hook.
    pub(super) shortcuts: Option<Arc<Mutex<Receiver<Shortcut>>>>,
    /// The sidebar is hidden to its rail (this session).
    pub(super) sidebar_collapsed: bool,
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
}

impl DesktopApp {
    pub(super) fn shell_update(&mut self, msg: ShellMsg, context: &ComponentContext<Self>) {
        match msg {
            ShellMsg::AutoSettings => {
                self.close_popover();
                self.open_settings(Some(Page::Routing), context);
            }
            ShellMsg::DaemonReady(result) => match result {
                Ok((discovered, read)) => {
                    self.startup_error = false;
                    self.prefs = crate::settings::Preferences::from_reply(&read);
                    self.nav.collapsed = self.prefs.collapsed.iter().cloned().collect();
                    self.launch_preferences_loaded();
                    self.preferred_window = crate::settings::window(&read);
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
                    self.startup_error = true;
                    self.alert(format!("Cannot start Swarm: {error}"));
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
                            self.startup_error = false;
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
            ShellMsg::SidebarToggled => self.nav.sidebar_collapsed = !self.nav.sidebar_collapsed,
            ShellMsg::AgentOpened(agent) => {
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
            ShellMsg::MoreAgentsRequested => {
                if let Some(after) = self.nav.agents_next.clone() {
                    self.load_agents(context, Some(after));
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
            ShellMsg::PanelToggled(panel) => match panel {
                Panel::Details => match self.selected_agent_id() {
                    Some(agent) if self.panel != Panel::Details => {
                        self.show_details(&agent, context);
                    }
                    _ => self.close_panel(),
                },
                Panel::Monitor if self.panel != Panel::Monitor => {
                    self.panel = Panel::Monitor;
                    self.start_monitor(context);
                }
                _ => self.close_panel(),
            },
            ShellMsg::SchemeObserved(scheme) => {
                let before = self.palette();
                self.appearance.dark = scheme == ColorScheme::Dark;
                self.appearance.high_contrast = crate::accessibility::high_contrast_on();
                if self.palette() != before {
                    // The palette-keyed tree is rebuilt with new controls
                    // (an empty search box among them).
                    self.nav.search = Search::default();
                    self.apply_scheme();
                    self.reprime_fields(context);
                }
                // The first observation colors the title bar even when the
                // palette is the starting one.
                self.sync_title_bar();
            }
            ShellMsg::WindowResized(size) => {
                self.window_width = Some(size.width);
                self.window_height = Some(size.height);
                self.sync_title_bar();
                if size.width >= 640.0 && size.height >= 480.0 {
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
                self.prefs.theme = theme;
                self.prefs_rev += 1;
                self.save_preferences(context);
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
            ShellMsg::SearchChanged(text) => {
                self.nav.search.text = text.clone();
                // WinUI writes the suggestion reached with the arrows into
                // the box: that is not a new query.
                if !self
                    .nav
                    .search
                    .results
                    .iter()
                    .any(|(label, _)| *label == text)
                {
                    self.nav.search.query = text;
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
                    Shortcut::Search => self.focus_search(SEARCH_FOCUS_TRIES, context),
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
                if self.preferences_loaded && !self.nav.agents_loading {
                    self.refresh_head(context);
                }
                if self.preferences_loaded {
                    self.load_organization(context);
                    self.load_queue(context);
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
    fn apply_scheme(&mut self) {
        if let Err(error) = crate::app_resources::apply_scheme(self.palette().scheme()) {
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
        match crate::app_resources::apply_title_bar(palette.scheme()) {
            Ok(()) => self.title_bar = Some(palette),
            Err(error) => self.set_status(format!("Title bar colors not applied: {error}")),
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

    /// Open the Agent a search found.
    fn open_found(&mut self, id: &str, context: &ComponentContext<Self>) {
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
        self.chat.items_revision = 0;
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

    /// The listed or fetched Agent with this id.
    pub(super) fn find_agent(&self, id: &str) -> Option<&UiAgent> {
        let branch_children = self.nav.branches.values().flat_map(|branch| match branch {
            Branch::Loaded { children, .. } => children.as_slice(),
            _ => &[],
        });
        self.nav
            .agents
            .iter()
            .chain(branch_children)
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

    /// Recompute the search suggestions from the listed Agents.
    fn refresh_search(&mut self) {
        self.nav.search.results =
            nav::search(&self.nav.agents, &self.checkouts(), &self.nav.search.query);
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

    /// The next High Contrast reading, one cadence from now.
    pub(super) fn watch_high_contrast(&self, context: &ComponentContext<Self>) {
        let _task = context.spawn_background(|cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(HIGH_CONTRAST_POLL_MS, &cancelled);
            Message::Shell(ShellMsg::HighContrastRead(
                crate::accessibility::high_contrast_on(),
            ))
        });
    }

    /// The main area's width from the window width, the sidebar (or the
    /// Settings navigation in its place) and the open panel, or None until
    /// the first size observation.
    pub(super) fn main_width(&self) -> Option<f64> {
        let panel = if self.panel_shown() {
            tokens::layout::PANEL_WIDTH
        } else {
            0.0
        };
        self.window_width
            .map(|width| width - tokens::layout::SIDEBAR_WIDTH - panel)
    }

    /// The right panel belongs to the conversation: Settings shows two
    /// columns (its navigation and the page), as in the Codex app.
    fn panel_shown(&self) -> bool {
        self.panel != Panel::Closed && self.main == Main::Conversation && !self.startup_error
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
    /// register the opened checkout, then read the settings.
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
                let request = eliot_core::api::ProjectDiscoverRequest {
                    workspace: workspace.to_string_lossy().into_owned(),
                };
                request.validate()?;
                let reply: eliot_core::api::ProjectDiscoverResponse =
                    eliot_daemon::client_call_typed(&data_dir, "project.discover", &request)
                        .await?;
                request.validate_response(&reply)?;
                let read = config_client::read_settings(&data_dir).await?;
                Ok((reply.project.map(|project| project.checkout), read))
            })
            .map_err(|error: eliot_core::Error| error.to_string());
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
        let _task = context.spawn_background(move |_| {
            Message::Shell(ShellMsg::OrganizationLoaded(Box::new(
                load_organization_blocking(&data_dir, archived, changes_after),
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
        let _task = context.spawn_background(move |_| {
            Message::Shell(ShellMsg::QueueLoaded(Box::new(load_queue_blocking(
                &data_dir, watched,
            ))))
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
        let _task = context.spawn_background(move |_| {
            Message::Shell(ShellMsg::AttentionLoaded(
                list_epoch,
                load_agents_blocking(&data_dir, &request, None),
            ))
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
        let _task = context.spawn_background(move |_| {
            Message::Shell(ShellMsg::HeadLoaded(
                list_epoch,
                load_agents_blocking(&data_dir, &head, archived),
            ))
        });
        self.load_attention(context);
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

        let main: View = match self.main {
            _ if self.startup_error => self.startup_error_view(context, palette),
            Main::Conversation => self.chat_view(context, palette),
            Main::Settings => self.settings_view(context, palette),
        };
        // Settings always shows its navigation; tasks may hide the sidebar
        // to its rail.
        let rail = self.nav.sidebar_collapsed && self.main != Main::Settings && !self.startup_error;
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
                .background(palette.sidebar())
                .content(self.panel_view(context, palette))
        };
        // Settings replaces the sidebar with its own navigation.
        let side: View = match self.main {
            Main::Settings if !self.startup_error => self.settings_nav(context, palette),
            _ if rail => self.rail_view(context, palette),
            _ => self.sidebar_view(context, palette),
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
        let palette_key = match palette.scheme() {
            None => "high-contrast",
            Some(scheme) if scheme.dark => "dark",
            Some(_) => "light",
        };
        Grid::new().keyed_children(vec![KeyedView::new(palette_key.to_owned(), window)])
    }

    /// The sidebar at Codex density (owner review 2026-11-16, the Codex
    /// and OpenCode sidebars as references): the app name, Search tasks
    /// (Ctrl+K), the one tonal New task row (Ctrl+N), the projects
    /// (collapsible 32-DIP rows) with their tasks nested (28-DIP rows
    /// indented 24 DIPs), Recents, then the fleet line, the attention row
    /// and Settings at the bottom. Section labels are secondary text.
    fn sidebar_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let pad = SIDEBAR_PADDING;
        // The app name, and the button that hides the sidebar to its rail.
        let header = Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .margin(Thickness::new(pad, tokens::spacing::SM, pad, 0.0))
            .children((
                m3::section_title("Eliot Swarm")
                    .height(tokens::height::CONTROL)
                    .margin(Thickness::new(tokens::spacing::INSET, 0.0, 0.0, 0.0))
                    .vertical_alignment(VerticalAlignment::Center),
                m3::icon_button(palette)
                    .grid_column(1)
                    .height(tokens::height::CONTROL)
                    .automation_name("Hide sidebar")
                    .on_click(context.message(Message::Shell(ShellMsg::SidebarToggled)))
                    .content(m3::icon(GLYPH_HIDE_SIDEBAR)),
            ));
        let search = AutoSuggestBox::new()
            .element_ref(&self.nav.search_ref)
            .placeholder_text("Search tasks (Ctrl+K)")
            .text(self.nav.search.text.clone())
            .items_source(
                self.nav
                    .search
                    .results
                    .iter()
                    .map(|(label, _)| label.clone()),
            )
            .on_text_changed(context.callback(|text| Message::Shell(ShellMsg::SearchChanged(text))))
            .on_suggestion_chosen(
                context.callback(|label| Message::Shell(ShellMsg::SearchChosen(label))),
            )
            .automation_name("Search tasks")
            .height(tokens::height::CONTROL)
            .grid_row(2)
            .margin(Thickness::new(pad, tokens::spacing::XS, pad, 0.0));
        let new_task_active = self.main == Main::Conversation && self.chat.convo.is_none();
        // WinUI's template caches its resource brushes: changing selection
        // recreates this one row, as with the keyed task rows below.
        let new_task = StackPanel::new()
            .grid_row(1)
            .margin(Thickness::new(pad, tokens::spacing::XS, pad, 0.0))
            .keyed_children(vec![KeyedView::new(
                format!("new-task:{new_task_active}"),
                m3::row(palette, new_task_active)
                    .automation_name("New task (Ctrl+N)")
                    .on_click(context.message(Message::Shell(ShellMsg::NewTask)))
                    .content(m3::row_content(
                        Some(m3::glyph::NEW_TASK),
                        "New task",
                        "",
                        m3::secondary("Ctrl+N")
                            .vertical_alignment(VerticalAlignment::Center)
                            .into(),
                    )),
            )]);
        // Then the two destinations, as in Antigravity's sidebar.
        let destination = |page: settings_ui::Page, glyph: &str, label: &str| {
            let shown = self.main == Main::Settings && self.settings_page == page;
            KeyedView::new(
                format!("{label}:{shown}"),
                m3::row(palette, shown)
                    .automation_name(label.to_owned())
                    .on_click(context.message(Message::Shell(ShellMsg::SettingsPage(page))))
                    .content(m3::icon_label(glyph, label)),
            )
        };
        let destinations = StackPanel::new()
            .grid_row(3)
            .margin(Thickness::new(pad, tokens::spacing::XS, pad, 0.0))
            .keyed_children(vec![
                destination(
                    settings_ui::Page::Automations,
                    m3::glyph::AUTOMATION,
                    "Automations",
                ),
                destination(settings_ui::Page::Usage, m3::glyph::USAGE, "Usage"),
            ]);

        let fleet = Fleet::of(&self.nav.agents);
        let query = self.nav.search.query.trim().to_owned();
        let checkouts = self.checkouts();
        let keep = |agent: &UiAgent| {
            query.is_empty()
                || nav::matches(agent, &nav::agent_place_label(agent, &checkouts), &query)
        };
        let filtered = !query.is_empty();
        // The open Agent renders explicitly above the list (outside folding
        // and page counts), so the sections skip it. Only while unfiltered:
        // a search shows its own matches.
        let pinned: Option<String> = if filtered {
            None
        } else {
            self.selected_agent_id()
                .filter(|id| self.nav.agents.iter().any(|agent| &agent.id == id))
        };
        let shown = Shown {
            branches: &self.nav.branches,
            expanded: &self.nav.expanded,
            collapsed: &self.nav.collapsed,
            unfolded: &self.nav.unfolded,
            today_start: today_start_ms(),
            only: filtered.then_some(&keep as &dyn Fn(&UiAgent) -> bool),
            pinned: pinned.as_deref(),
        };
        let sections = nav::sections(&self.nav.agents, &self.nav.organization, &checkouts, &shown);

        let caption = if filtered {
            "Matching tasks"
        } else {
            "Projects"
        };
        let caption_row: View = if filtered {
            section_label(caption).into()
        } else {
            // Bulk actions over every project (k142): archive, delete,
            // remove the unused ones, each after one confirmation.
            let items: Vec<MenuItem> = super::bulk_ui::ALL_MENU
                .iter()
                .map(|(id, label)| MenuItem::item(*id, *label))
                .collect();
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .children((
                    section_label(caption),
                    m3::icon_button(palette)
                        .grid_column(1)
                        .width(tokens::height::SUB_ROW)
                        .height(tokens::height::SUB_ROW)
                        .vertical_alignment(VerticalAlignment::Bottom)
                        .automation_name("Actions for all projects")
                        .content(m3::icon(m3::glyph::MORE))
                        .menu(Menu::new(
                            items,
                            // Capture-free: one identity across renders.
                            context.callback(|id: String| Message::Bulk(BulkMsg::Menu(id))),
                        )),
                ))
        };
        let mut items: Vec<KeyedView> =
            vec![KeyedView::new(format!("caption:{caption}"), caption_row)];
        // A failed organization or queue read says so; the rows keep the
        // group the daemon placed them in.
        for error in self
            .nav
            .organization
            .errors
            .iter()
            .chain(&self.nav.organization.queue_errors)
        {
            items.push(KeyedView::new(
                format!("organization-error:{error}"),
                detail_row(error, Some(palette.error())),
            ));
        }
        // The open Agent, explicitly above the list: visible even when it
        // aged out of the newest page or would fold under Show older.
        if let Some(open) = pinned.as_ref().and_then(|id| self.find_agent(id)).cloned() {
            let expanded = self.nav.expanded.contains(&open.id);
            let kids: Vec<&UiAgent> =
                nav::children_of(&open.id, &self.nav.agents, &self.nav.branches)
                    .into_iter()
                    .filter(|child| pinned.as_deref() != Some(child.id.as_str()))
                    .collect();
            items.push(self.task_item(
                &Row {
                    agent: open,
                    depth: 0,
                    children: kids.len(),
                    expanded,
                    note: None,
                },
                context,
                palette,
            ));
            if expanded {
                for child in kids {
                    items.push(self.task_item(
                        &Row {
                            agent: child.clone(),
                            depth: 1,
                            children: 0,
                            expanded: false,
                            note: None,
                        },
                        context,
                        palette,
                    ));
                }
            }
        }
        if sections.is_empty() {
            let empty = if self.nav.agents_loading {
                "Loading…"
            } else if filtered {
                "No matching tasks. Clear the search to see all."
            } else {
                "No projects yet. Start a task with New task, or add one in Settings › Projects."
            };
            items.push(KeyedView::new(
                "no-projects",
                m3::secondary(empty)
                    .text_wrapping(TextWrapping::Wrap)
                    .margin(Thickness::new(
                        tokens::spacing::INSET,
                        tokens::spacing::XS,
                        tokens::spacing::SM,
                        0.0,
                    )),
            ));
        }
        let new_task_selected =
            self.main == Main::Conversation && self.chat.convo.is_none() && !self.launch.chat;
        let launch_key = nav::path_key(&self.launch.workspace);
        for section in &sections {
            let selected = new_task_selected && nav::path_key(&section.checkout) == launch_key;
            let name = self
                .prefs
                .display_name(&format!("project:{}", section.key))
                .map_or_else(|| section.name.clone(), str::to_owned);
            let place = Place {
                key: &section.key,
                name: &name,
                checkout: &section.checkout,
                checkouts: &section.checkouts,
                collapsed: section.collapsed,
                running: section.running,
                missing: section.missing,
            };
            items.push(self.place_item(&place, selected, filtered, context, palette));
            if section.collapsed {
                continue;
            }
            // The server's lines: the team (manager, branch, assignment or
            // "Needs assignment"), competing roots, exact totals, then the
            // queued launches with their wait reasons.
            for (index, line) in section.detail.iter().chain(&section.waits).enumerate() {
                items.push(KeyedView::new(
                    format!("detail:{}:{index}:{line}", section.key),
                    detail_row(line, None),
                ));
            }
            // Older tasks fold below; "No tasks yet" only when no server
            // line already says what is there.
            if section.rows.is_empty()
                && (section.older > 0 || (section.detail.is_empty() && section.waits.is_empty()))
            {
                items.push(KeyedView::new(
                    format!("empty:{}", section.key),
                    quiet_row(
                        if section.older > 0 {
                            "No recent tasks"
                        } else {
                            "No tasks yet: start one with New task"
                        },
                        tokens::layout::TREE_INDENT,
                    ),
                ));
            }
            for row in &section.rows {
                items.push(self.task_item(row, context, palette));
            }
            if section.older > 0 {
                let label = if section.older_shown {
                    "Hide older".to_owned()
                } else {
                    format!("Show {} older", section.older)
                };
                items.push(KeyedView::new(
                    format!("older:{}:{}", section.key, section.older_shown),
                    m3::sub_row(palette, false)
                        .margin(Thickness::new(tokens::layout::TREE_INDENT, 0.0, 0.0, 0.0))
                        .automation_name(format!("{label} in {}", section.name))
                        .on_click(
                            context.message(Message::Shell(ShellMsg::OlderToggled(
                                section.key.clone(),
                            ))),
                        )
                        .content(m3::secondary(label)),
                ));
            }
        }
        if !filtered
            && self.nav.agents.len() < self.nav.agents_total
            && self.nav.agents_next.is_some()
        {
            items.push(KeyedView::new(
                "more",
                m3::sub_row(palette, false)
                    .is_enabled(!self.nav.agents_loading)
                    .margin(Thickness::new(tokens::layout::TREE_INDENT, 0.0, 0.0, 0.0))
                    .on_click(context.message(Message::Shell(ShellMsg::MoreAgentsRequested)))
                    .content(m3::secondary(format!(
                        "Show more ({} of {})",
                        self.nav.agents.len(),
                        self.nav.agents_total
                    ))),
            ));
        }
        // Recents: the newest tasks of collapsed projects, one per title;
        // never what the list shows or folds under "Show N older".
        let collapsed: HashSet<&str> = sections
            .iter()
            .filter(|section| section.collapsed)
            .map(|section| section.key.as_str())
            .collect();
        let recents: Vec<&UiAgent> = nav::sidebar_recents(&self.nav.agents, &collapsed, RECENTS)
            .into_iter()
            .filter(|agent| pinned.as_deref() != Some(agent.id.as_str()))
            .collect();
        if !filtered && !recents.is_empty() {
            items.push(KeyedView::new("recents-caption", section_label("Recents")));
            for agent in recents {
                items.push(KeyedView::new(
                    format!("recent:{}:{}", agent.id, self.agent_label(agent)),
                    with_menu(
                        self.recent_row(agent, context, palette),
                        Target::Agent(agent.clone()),
                        context,
                    ),
                ));
            }
        }
        items.push(KeyedView::new(
            format!("archived:{}", self.nav.show_archived),
            m3::sub_row(palette, self.nav.show_archived)
                .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                .automation_name(if self.nav.show_archived {
                    "Hide archived (showing archived)"
                } else {
                    "Show archived"
                })
                .on_click(context.message(Message::Menu(MenuMsg::ArchivedToggled)))
                .content(m3::row_content(
                    Some(m3::glyph::ARCHIVE),
                    if self.nav.show_archived {
                        "Hide archived"
                    } else {
                        "Show archived"
                    },
                    "",
                    View::empty(),
                )),
        ));
        let list = ScrollViewer::new()
            .grid_row(4)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Projects and Agents")
            .content(
                StackPanel::new()
                    .margin(Thickness::new(pad, 0.0, pad, pad))
                    .keyed_children(items),
            );

        let monitor_open = self.panel == Panel::Monitor;
        let mut bottom = vec![
            KeyedView::new(
                "divider",
                m3::divider(palette).margin(Thickness::new(
                    tokens::spacing::INSET,
                    0.0,
                    tokens::spacing::INSET,
                    tokens::spacing::SM,
                )),
            ),
            KeyedView::new(
                format!("fleet:{monitor_open}"),
                self.fleet_row(fleet, context, palette),
            ),
        ];
        bottom.push(KeyedView::new(
            "settings",
            m3::row(palette, false)
                .automation_name("Settings")
                .on_click(context.message(Message::Shell(ShellMsg::SettingsOpened)))
                .content(m3::icon_label(m3::glyph::SETTINGS, "Settings")),
        ));
        let bottom = StackPanel::new()
            .grid_row(5)
            .margin(Thickness::new(pad, 0.0, pad, pad))
            .keyed_children(bottom);

        Grid::new()
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
            ])
            .children((header, new_task, search, destinations, list, bottom))
    }

    /// A project or subproject row with its right-click menu, or the
    /// rename editor in its place.
    fn place_item(
        &self,
        place: &Place,
        selected: bool,
        filtered: bool,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> KeyedView {
        let renaming = self
            .menu
            .renaming
            .as_ref()
            .filter(|(target, _)| matches!(target, Target::Project { key, .. } if key == place.key))
            .map(|(_, typed)| typed.clone());
        let row = match renaming {
            _ if place.key == nav::OTHER_LOCATIONS => {
                self.place_row(place, selected, filtered, context, palette)
            }
            Some(typed) => self.rename_row(&typed, 0.0, context, palette),
            None => with_menu(
                self.place_row(place, selected, filtered, context, palette),
                Target::Project {
                    key: place.key.to_owned(),
                    name: place.name.to_owned(),
                    checkout: place.checkout.to_owned(),
                    checkouts: place.checkouts.to_vec(),
                    collapsed: place.collapsed,
                },
                context,
            ),
        };
        KeyedView::new(
            format!(
                "project:{}:{selected}:{}:{}:{}:{}:{}",
                place.key,
                place.collapsed,
                place.name,
                place.running,
                place.missing,
                self.menu.renaming.is_some()
            ),
            row,
        )
    }

    /// A project row (folder icon): its chevron and name collapse or expand
    /// it; tasks working now (the server's team totals) show as a count on
    /// the right, collapsed or not; a registered folder that is gone says
    /// "missing" with a one-click Remove. The trailing button starts a new
    /// task in it.
    fn place_row(
        &self,
        place: &Place,
        selected: bool,
        filtered: bool,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let name = place.name;
        let badge: View = if place.running > 0 {
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::XS)
                .vertical_alignment(VerticalAlignment::Center)
                .children((
                    palette
                        .dot(StatusTone::Active)
                        .map_or_else(View::empty, m3::dot),
                    m3::secondary(place.running.to_string())
                        .vertical_alignment(VerticalAlignment::Center),
                ))
        } else if place.missing {
            m3::secondary("missing")
                .foreground(palette.error())
                .vertical_alignment(VerticalAlignment::Center)
                .into()
        } else {
            View::empty()
        };
        let (chevron, state) = if place.collapsed {
            (m3::glyph::CHEVRON_RIGHT, "collapsed")
        } else {
            (m3::glyph::CHEVRON_DOWN, "expanded")
        };
        let kind = "project";
        let mut label = format!("{name}, {kind}, {state}");
        if place.running > 0 {
            label.push_str(&format!(", {} working", place.running));
        }
        if place.missing {
            label.push_str(", folder missing");
        }
        let row = m3::row(palette, selected);
        let height = tokens::height::CONTROL;
        let toggle = row
            .automation_name(label)
            .is_enabled(!filtered)
            .on_click(context.message(Message::Shell(ShellMsg::ProjectToggled(
                place.key.to_owned(),
            ))))
            .content(
                Grid::new()
                    .columns([GridLength::Auto, GridLength::STAR])
                    .column_spacing(tokens::spacing::SM)
                    .children((
                        m3::icon(chevron),
                        Border::new().grid_column(1).content(m3::row_content(
                            Some(m3::glyph::FOLDER),
                            name,
                            "",
                            badge,
                        )),
                    )),
            )
            // A worktree registration's name trims in the sidebar width.
            .tooltip(name.to_owned());
        let action = if place.key == nav::OTHER_LOCATIONS {
            View::empty()
        } else if place.missing {
            // The folder is gone: one click forgets the registration
            // (kept, with its reason, while a task still uses it).
            let registered = self
                .nav
                .projects
                .iter()
                .find(|project| nav::path_key(&project.workspace) == nav::path_key(place.checkout))
                .map(|project| project.id.clone());
            m3::icon_button(palette)
                .grid_column(1)
                .width(height)
                .height(height)
                .is_enabled(registered.is_some() && self.bulk.is_none())
                .automation_name(format!("Remove missing {kind} {name}"))
                .on_click(context.message(Message::Bulk(BulkMsg::Start(
                    super::bulk_ui::BulkOp::RemoveProjects,
                    super::bulk_ui::Pick::Missing {
                        project: registered.unwrap_or_default(),
                        checkout: place.checkout.to_owned(),
                    },
                    name.to_owned(),
                ))))
                .content(m3::icon(m3::glyph::DELETE))
        } else {
            m3::icon_button(palette)
                .grid_column(1)
                .width(height)
                .height(height)
                .is_enabled(self.can_launch_here(place.checkout))
                .automation_name(format!("New task in {name}"))
                .on_click(context.message(Message::Shell(ShellMsg::ProjectPicked(
                    place.checkout.to_owned(),
                ))))
                .content(m3::icon(m3::glyph::NEW_TASK))
        };
        Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .children((toggle, action))
    }

    /// A task row under its project: indented 24 DIPs per level, so its
    /// title lines up with the project's label. One line: the title (at
    /// least 60% of the row), its short model, the worktree where the
    /// project spans several, and a state dot only for a task that runs or
    /// needs attention. A manager with workers carries its expand chevron
    /// in that indent. Its accessible name says "Harness · model" in full.
    fn agent_row(
        &self,
        row: &Row,
        selected: bool,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let agent = &row.agent;
        let label = self.agent_label(agent);
        let title = label.as_str();
        let indent = tokens::layout::TREE_INDENT;
        let chevron: View = if row.expandable() {
            let (glyph, name) = if row.expanded {
                (
                    m3::glyph::CHEVRON_DOWN,
                    format!("Collapse workers of {title}"),
                )
            } else {
                (
                    m3::glyph::CHEVRON_RIGHT,
                    format!(
                        "Show {} of {title}",
                        eliot_core::count_words(row.children as u64, "worker", "workers")
                    ),
                )
            };
            m3::icon_button(palette)
                .width(indent)
                .height(tokens::height::SUB_ROW)
                .automation_name(name)
                .on_click(context.message(Message::Shell(ShellMsg::ExpandToggled(
                    agent.id.clone(),
                    !row.expanded,
                ))))
                .content(m3::icon(glyph))
        } else {
            View::empty()
        };
        let runs = crate::picker::harness_model_label(&agent.harness, agent.model.as_deref());
        let mut name = format!(
            "{title}, {runs}, {}, Agent {}",
            eliot_core::api::agent_state_name(agent.state),
            agent.id
        );
        if row.depth > 0 {
            name.push_str(&format!(", worker level {}", row.depth));
        }
        let note: View = match &row.note {
            Some(note) => m3::secondary(note.clone())
                .text_trimming(TextTrimming::CharacterEllipsis)
                .margin(Thickness::new(
                    tokens::spacing::INSET,
                    0.0,
                    0.0,
                    tokens::spacing::XS,
                ))
                .grid_row(1)
                .grid_column(1)
                .into(),
            None => View::empty(),
        };
        let quick = if agent.native_child {
            None
        } else if agent.live() {
            Some((super::menu_ui::Action::Stop, "Stop", m3::glyph::STOP))
        } else if agent.state == eliot_core::AgentState::Lost {
            Some((super::menu_ui::Action::Resume, "Resume", m3::glyph::RESUME))
        } else if !agent.archived {
            Some((
                super::menu_ui::Action::Archive(true),
                "Archive",
                m3::glyph::ARCHIVE,
            ))
        } else {
            None
        };
        let over = self.menu.hovered.as_deref() == Some(agent.id.as_str()) || selected;
        // The model gives way to the quick action, as a Codex sidebar
        // row's time does: the title keeps its width. The open task's
        // header names it.
        let shows_quick = over && quick.is_some();
        let shown_model = if shows_quick { "" } else { row_model(agent) };
        let text_width = row_text_width(
            (row.depth as f64 + 1.0) * indent,
            1 + usize::from(shows_quick),
        );
        let quick: View = if over {
            quick
                .map(|(action, label, glyph)| {
                    m3::icon_button(palette)
                        .width(tokens::height::SUB_ROW)
                        .height(tokens::height::SUB_ROW)
                        .automation_name(format!("{label} {title}"))
                        .on_click(
                            context.message(Message::Menu(MenuMsg::Quick(agent.clone(), action))),
                        )
                        .content(m3::icon(glyph))
                        .tooltip(label)
                })
                .unwrap_or_else(View::empty)
        } else {
            View::empty()
        };
        let actions = StackPanel::new()
            .orientation(Orientation::Horizontal)
            .children((
                quick,
                m3::icon_button(palette)
                    .width(tokens::height::SUB_ROW)
                    .height(tokens::height::SUB_ROW)
                    .automation_name(format!("Actions for {title}"))
                    .on_click(context.message(Message::Menu(MenuMsg::TaskOpened(agent.clone()))))
                    .content(m3::icon(m3::glyph::MORE))
                    .tooltip("Task actions"),
            ));
        Border::new()
            .on_pointer_entered(context.callback({
                let id = agent.id.clone();
                move |_: PointerEventInfo| Message::Menu(MenuMsg::Hovered(Some(id.clone())))
            }))
            .on_pointer_exited(
                context.callback(|_: PointerEventInfo| Message::Menu(MenuMsg::Hovered(None))),
            )
            .content(
                Grid::new()
                    .margin(Thickness::new(row.depth as f64 * indent, 0.0, 0.0, 0.0))
                    .columns([
                        GridLength::Pixel(indent),
                        GridLength::STAR,
                        GridLength::Auto,
                    ])
                    .rows([GridLength::Auto, GridLength::Auto])
                    .children((
                        Border::new()
                            .vertical_alignment(VerticalAlignment::Center)
                            .content(chevron),
                        m3::sub_row(palette, selected)
                            .grid_column(1)
                            .automation_name(name)
                            .on_click(
                                context
                                    .message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))),
                            )
                            .content(m3::row_content(
                                None,
                                title,
                                "",
                                row_tag(
                                    shown_model,
                                    &row_tag_text(agent),
                                    task_dot(palette, agent),
                                    text_width,
                                ),
                            )),
                        note,
                        Border::new().grid_column(2).content(actions),
                    )),
            )
    }

    /// One task or chat row: its right-click menu, or the rename editor.
    fn task_item(&self, row: &Row, context: &mut ViewContext<Self>, palette: Palette) -> KeyedView {
        let selected = self.main == Main::Conversation
            && self.nav.selection.agent.as_deref() == Some(row.agent.id.as_str());
        let renaming = self
            .menu
            .renaming
            .as_ref()
            .filter(
                |(target, _)| matches!(target, Target::Agent(agent) | Target::Tags(agent) if agent.id == row.agent.id),
            )
            .map(|(_, typed)| typed.clone());
        let label = self.agent_label(&row.agent);
        let view = match renaming {
            Some(typed) => self.rename_row(
                &typed,
                row.depth as f64 * tokens::layout::TREE_INDENT + tokens::layout::TREE_INDENT,
                context,
                palette,
            ),
            None => with_menu(
                self.agent_row(row, selected, context, palette),
                Target::Agent(row.agent.clone()),
                context,
            ),
        };
        KeyedView::new(
            format!(
                "agent:{}:{selected}:{}:{label}:{}:{}",
                row.agent.id,
                row.expanded,
                row.agent.archived,
                self.menu.renaming.is_some()
            ),
            view,
        )
    }

    /// A Recents row: the title, its short model and its dot, no tree.
    fn recent_row(
        &self,
        agent: &UiAgent,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let label = self.agent_label(agent);
        let runs = crate::picker::harness_model_label(&agent.harness, agent.model.as_deref());
        m3::sub_row(palette, false)
            .automation_name(format!(
                "Recent: {label}, {runs}, {}",
                eliot_core::api::agent_state_name(agent.state)
            ))
            .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))))
            .content(m3::row_content(
                None,
                &label,
                "",
                row_tag(
                    row_model(agent),
                    "",
                    task_dot(palette, agent),
                    row_text_width(0.0, 0),
                ),
            ))
    }

    /// The fleet in one calm row that is never cut off ("2 working · 1
    /// needs attention", "All quiet"); it opens the monitor, and its name
    /// carries every count.
    fn fleet_row(&self, fleet: Fleet, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let summary = fleet.summary(self.nav.agents_total, self.nav.archived);
        let line = fleet.compact(self.nav.agents_total, self.nav.archived);
        // One row: it opens the Monitor, whose filter lists only the
        // Agents that need attention; the dot says the most urgent tone.
        let tone = if fleet.attention > 0 {
            Some(StatusTone::Attention)
        } else if fleet.running > 0 {
            Some(StatusTone::Active)
        } else {
            None
        };
        let dot = tone
            .and_then(|tone| palette.dot(tone))
            .map_or_else(View::empty, m3::dot);
        let open = self.panel == Panel::Monitor;
        m3::row(palette, open)
            .automation_name(format!(
                "Monitor: {summary} ({})",
                if open { "open" } else { "closed" }
            ))
            .on_click(context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Monitor))))
            .content(m3::row_content(Some(m3::glyph::PULSE), &line, "", dot))
    }

    /// The hidden sidebar's rail: show the sidebar, New task, then at the
    /// bottom the Monitor (with the fleet's most urgent dot) and Settings.
    fn rail_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let pad = SIDEBAR_PADDING;
        let fleet = Fleet::of(&self.nav.agents);
        let summary = fleet.summary(self.nav.agents_total, self.nav.archived);
        let tone = if fleet.attention > 0 {
            Some(StatusTone::Attention)
        } else if fleet.running > 0 {
            Some(StatusTone::Active)
        } else {
            None
        };
        // The dot sits on the button's corner, clear of the icon.
        let dot: View = tone
            .and_then(|tone| palette.dot(tone))
            .map_or_else(View::empty, |fill| {
                Border::new()
                    .horizontal_alignment(HorizontalAlignment::Right)
                    .vertical_alignment(VerticalAlignment::Top)
                    .margin(Thickness::new(0.0, 2.0, 2.0, 0.0))
                    .content(m3::dot(fill))
            });
        let button = |glyph: &str, name: String, message: Message| {
            m3::icon_button(palette)
                .height(tokens::height::CONTROL)
                .automation_name(name)
                .on_click(context.message(message))
                .content(m3::icon(glyph))
        };
        let top = StackPanel::new()
            .margin(Thickness::new(pad, tokens::spacing::SM, pad, 0.0))
            .spacing(tokens::spacing::XS)
            .children((
                button(
                    GLYPH_SHOW_SIDEBAR,
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
            .margin(Thickness::new(pad, 0.0, pad, pad))
            .spacing(tokens::spacing::XS)
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

    /// Settings in the sidebar's place (the Codex settings navigation): a
    /// Back row, a Search field that finds settings across the pages, then
    /// the pages in their groups.
    fn settings_nav(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let pad = SIDEBAR_PADDING;
        let top = StackPanel::new()
            .margin(Thickness::new(pad, tokens::spacing::SM, pad, 0.0))
            .spacing(tokens::spacing::XS)
            .children((
                m3::row(palette, false)
                    .automation_name("Back to tasks")
                    .on_click(context.message(Message::Shell(ShellMsg::SettingsClosed)))
                    .content(m3::icon_label(m3::glyph::BACK, "Back to tasks")),
                m3::field()
                    .text(self.settings.query.clone())
                    .placeholder_text("Search settings")
                    .automation_name("Search settings")
                    .on_text_changed(
                        context.callback(|text| Message::Shell(ShellMsg::SettingsSearch(text))),
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
        let error = self.alert.clone().unwrap_or_default();
        StackPanel::new()
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .vertical_alignment(VerticalAlignment::Center)
            .spacing(tokens::spacing::MD)
            .margin(Thickness::uniform(tokens::spacing::LG))
            .children((
                m3::page_title("Swarm could not start here"),
                m3::text(
                    "Swarm could not open this data directory. Existing settings have not been moved.",
                    tokens::text::BODY,
                )
                .text_wrapping(TextWrapping::Wrap),
                m3::secondary(error)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
                m3::filled(palette)
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

/// A project or subproject row's content: what it is called and holds.
struct Place<'a> {
    /// `org:<project>`, or the no-project bucket's key.
    key: &'a str,
    name: &'a str,
    /// Where a new task in it starts.
    checkout: &'a str,
    checkouts: &'a [String],
    collapsed: bool,
    running: usize,
    missing: bool,
}

/// A sidebar section label: secondary text at the rows' content inset, 16
/// DIPs below the group above it.
fn section_label(text: &str) -> TextBlock {
    m3::section_label(text).margin(Thickness::new(
        tokens::spacing::INSET,
        tokens::spacing::MD,
        0.0,
        tokens::spacing::XS,
    ))
}

/// A muted sub-row that is not a control ("No tasks"), its text where an
/// indented task row's title starts.
fn quiet_row(text: &str, indent: f64) -> View {
    m3::secondary(text.to_owned())
        .height(tokens::height::SUB_ROW)
        .text_trimming(TextTrimming::CharacterEllipsis)
        .margin(Thickness::new(
            indent + tokens::spacing::INSET,
            0.0,
            0.0,
            0.0,
        ))
        .into()
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

/// A task row's quiet tag: "archived", else the execution folder of a
/// task the daemon reads in the no-project bucket (a legacy `wt-*` label is
/// shown as retained, never moved under a repository), else its tags.
fn row_tag_text(agent: &UiAgent) -> String {
    if agent.archived {
        "archived".to_owned()
    } else if agent.organization.is_none() && !agent.chat {
        nav::folder_name(agent.workspace.as_deref().unwrap_or(&agent.project_id)).to_owned()
    } else {
        agent.tags.join(", ")
    }
}

/// One server line under a project row (team, totals, a wait reason, a
/// failed read): muted and wrapped, so who can change a limit is never cut
/// off; its full text is the tooltip too.
fn detail_row(text: &str, error: Option<Brush>) -> View {
    let line = m3::secondary(text.to_owned());
    let line = match error {
        Some(brush) => line.foreground(brush),
        None => line,
    };
    line.text_wrapping(TextWrapping::Wrap)
        .margin(Thickness::new(
            tokens::layout::TREE_INDENT + tokens::spacing::INSET,
            0.0,
            tokens::spacing::SM,
            tokens::spacing::XS,
        ))
        .tooltip(text.to_owned())
}

/// The share of a sidebar row's text width its trailing marks take at
/// most: the title keeps at least 60% of it.
const TRAILING_SHARE: f64 = 0.4;

/// The text width of a sidebar row (inside its inset) that starts `indent`
/// DIPs in, with `actions` buttons beside it: the sidebar less its padding,
/// the indent, the actions and the row's inset on both sides.
fn row_text_width(indent: f64, actions: usize) -> f64 {
    tokens::layout::SIDEBAR_WIDTH
        - 2.0 * SIDEBAR_PADDING
        - indent
        - actions as f64 * tokens::height::SUB_ROW
        - 2.0 * tokens::spacing::INSET
}

/// The widths of a sidebar row's trailing model and tag (0: not shown) in a
/// row of `text_width`, with or without its dot. The trailing cell (the gap
/// before it included) never takes more than [`TRAILING_SHARE`] of the
/// row, so the title keeps the rest; the model trims past
/// [`tokens::layout::SIDEBAR_MODEL_WIDTH`], and a model and a tag share
/// what is left.
fn trailing_widths(model: bool, tag: bool, dot: bool, text_width: f64) -> (f64, f64) {
    let gap = tokens::spacing::SM;
    let mut budget = text_width * TRAILING_SHARE - gap;
    if dot {
        budget -= tokens::layout::DOT + gap;
    }
    let budget = budget.max(0.0);
    let model_cap = tokens::layout::SIDEBAR_MODEL_WIDTH;
    match (model, tag) {
        (true, true) => {
            let shared = ((budget - gap) / 2.0).max(0.0);
            let model_width = shared.min(model_cap);
            (model_width, (budget - gap - model_width).max(0.0))
        }
        (true, false) => (budget.min(model_cap), 0.0),
        (false, true) => (0.0, budget),
        (false, false) => (0.0, 0.0),
    }
}

/// A sidebar row's trailing marks: the Agent's short model
/// ([`crate::picker::model_short`], no harness word; empty hides it), its
/// quiet tag (its worktree folder, "archived") and its dot, sized by
/// [`trailing_widths`]. They sit in the row's trailing cell so the title
/// keeps the free width and trims with an ellipsis; a note column after an
/// auto-sized title got no width at all and the tag never showed.
fn row_tag(model: &str, tag: &str, dot: Option<View>, text_width: f64) -> View {
    let (model_width, tag_width) = trailing_widths(
        !model.is_empty(),
        !tag.is_empty(),
        dot.is_some(),
        text_width,
    );
    let mut parts = Vec::new();
    for (key, text, width) in [("model", model, model_width), ("tag", tag, tag_width)] {
        if !text.is_empty() && width > 0.0 {
            parts.push(KeyedView::new(
                key,
                m3::secondary(text.to_owned())
                    .max_width(width)
                    .text_trimming(TextTrimming::CharacterEllipsis)
                    .vertical_alignment(VerticalAlignment::Center),
            ));
        }
    }
    if let Some(dot) = dot {
        parts.push(KeyedView::new("dot", dot));
    }
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::SM)
        .vertical_alignment(VerticalAlignment::Center)
        .keyed_children(parts)
}

/// The short model a sidebar row shows ("" until the harness reports one).
fn row_model(agent: &UiAgent) -> &str {
    agent
        .model
        .as_deref()
        .map_or("", crate::picker::model_short)
}

/// A task's state dot, only while it runs or needs attention: a calm
/// list carries no marks.
fn task_dot(palette: Palette, agent: &UiAgent) -> Option<View> {
    (agent.running() || agent.needs_attention()).then(|| m3::list_dot(palette, agent.state))
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
