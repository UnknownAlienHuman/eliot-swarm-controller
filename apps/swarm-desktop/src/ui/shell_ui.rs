//! The one shell: the sidebar (New task, projects with their Agents nested,
//! Recents, the fleet status and Settings), the main area (one
//! conversation or the Settings page) and the optional right panel (the
//! monitor or the open Agent's details).

use super::*;
use crate::nav::{Branch, Row, Section};
use crate::settings::{Startup, ThemeChoice};
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
    /// The daemon runs (started when needed), the opened checkout was
    /// discovered, and the settings were read.
    DaemonReady(Fallible<(Option<String>, eliot_core::api::SettingsReadResponse)>),
    /// The owner chose a fresh private data directory (unsafe default).
    UseFreshDirectory,
    ProjectsLoaded(Fallible<Vec<UiProject>>),
    AgentsLoaded(u64, bool, Fallible<AgentPage>),
    /// A sidebar row: open this Agent's conversation.
    AgentOpened(UiAgent),
    /// A project row: a new task in this checkout.
    ProjectPicked(String),
    RefreshRequested,
    NewTask,
    MoreAgentsRequested,
    SettingsOpened,
    PanelToggled(Panel),
    /// The window's actual Light/Dark scheme (Reactor `on_color_scheme`).
    SchemeObserved(ColorScheme),
    /// The window's client size (Reactor `on_window_size`).
    WindowResized(WindowSize),
    /// The debounced window-size save of this epoch is due.
    SaveWindow(u64),
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
        Fallible<eliot_core::api::SettingsSaveResponse>,
    ),
    /// Titles read from first messages (`None`: no retained input).
    TitlesLoaded(Vec<(String, Option<String>)>),
    AlertDismissed,
    /// Re-read the listed Agents (states, Recents order).
    ListTick,
}

/// Reactor 0.100.0 reports the window's Light/Dark scheme only, and a High
/// Contrast theme can keep the same base scheme, so High Contrast is read
/// at the monitor's cadence.
const HIGH_CONTRAST_POLL_MS: u64 = 2_000;
/// Re-check for the empty editor render one frame later.
const PRIME_RETRY_MS: u64 = 16;
/// Indent per tree level in the sidebar.
const TREE_INDENT: f64 = 16.0;
/// The expand chevron column left of an Agent item (a 32 x 32 icon button).
const CHEVRON: f64 = 32.0;
/// The M3 navigation drawer's padding around its items.
const DRAWER_PADDING: f64 = 12.0;
/// The start inset of a navigation item's content (and of the headers).
const ITEM_INSET: f64 = 16.0;
/// The left edge of an Agent item: its title lines up with its project's
/// label (16 inset + 20 icon + 12 gap = 48 = 32 + 16).
const CHILD_INSET: f64 = 32.0;
/// Rows in Recents.
const RECENTS: usize = 8;
/// Titles read per background batch.
const TITLE_BATCH: usize = 24;
/// The sidebar's states and Recents follow the daemon at this cadence.
const LIST_REFRESH_MS: u64 = 10_000;

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
    /// Listed pages, in daemon order.
    pub(super) agents: Vec<UiAgent>,
    /// Direct-children reads of expanded managers.
    pub(super) branches: HashMap<String, Branch>,
    pub(super) expanded: HashSet<String>,
    /// Tags branch reads; a reply from before a refresh is dropped.
    pub(super) tree_epoch: u64,
    pub(super) agents_next: Option<String>,
    pub(super) agents_total: usize,
    pub(super) agents_loading: bool,
    /// Tags agent-list loads; a reply for an older request is dropped.
    pub(super) list_epoch: u64,
    pub(super) pending_select: Option<UiAgent>,
    /// Titles from first messages; `None` once read without one.
    pub(super) titles: HashMap<String, Option<String>>,
    pub(super) titles_inflight: bool,
}

impl DesktopApp {
    pub(super) fn shell_update(&mut self, msg: ShellMsg, context: &ComponentContext<Self>) {
        match msg {
            ShellMsg::DaemonReady(result) => match result {
                Ok((discovered, read)) => {
                    self.startup_error = false;
                    self.prefs = crate::settings::Preferences::from_reply(&read);
                    self.preferred_window = crate::settings::window(&read);
                    // The opened checkout, registered by discovery, is where
                    // a new task starts.
                    if let Some(checkout) = discovered {
                        self.launch.workspace = checkout;
                    }
                    let first_connection = !self.preferences_loaded;
                    self.preferences_loaded = true;
                    self.load_agents(context, None);
                    self.load_profile_list(context);
                    self.load_routing_preview(context);
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
                }
                Err(error) => self.alert(format!("Projects not read: {error}")),
            },
            ShellMsg::AgentsLoaded(list_epoch, append, page) => {
                if list_epoch != self.nav.list_epoch {
                    return;
                }
                self.nav.agents_loading = false;
                match page {
                    Ok(page) => {
                        if append {
                            self.nav.agents.extend(page.rows);
                        } else {
                            self.nav.agents = page.rows;
                        }
                        self.nav.agents_next = page.next;
                        self.nav.agents_total = page.total;
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
                        self.load_titles(context);
                    }
                    Err(error) => self.alert(format!("Agents not read: {error}")),
                }
            }
            ShellMsg::AgentOpened(agent) => {
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
                self.pick_project(&checkout);
                self.new_task(context);
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
                self.load_titles(context);
            }
            ShellMsg::RefreshRequested => {
                if let Ok(data_dir) = self.data_dir.clone() {
                    let _task = context.spawn_background(move |_| {
                        Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(&data_dir)))
                    });
                }
                self.load_agents(context, None);
                // Expanded branches are read again; their rows stay until then.
                let expanded: Vec<String> = self.nav.expanded.iter().cloned().collect();
                for id in expanded {
                    self.load_branch(&id, context);
                }
                self.load_profile_list(context);
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
            ShellMsg::SettingsOpened => {
                if self.main != Main::Settings {
                    self.main = Main::Settings;
                    self.reprime_fields(context);
                    self.settings_entered(context);
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
                    // The palette-keyed tree is rebuilt with new controls.
                    self.apply_scheme();
                    self.reprime_fields(context);
                }
            }
            ShellMsg::WindowResized(size) => {
                self.window_width = Some(size.width);
                if size.width >= 640.0 && size.height >= 480.0 {
                    let window = (size.width as u32, size.height as u32);
                    if self.window_size != Some(window) {
                        self.window_size = Some(window);
                        if self.preferences_loaded {
                            self.window_epoch += 1;
                            let epoch = self.window_epoch;
                            let _task = context.spawn_background(move |cancel| {
                                let _ = feed::backoff(500, &|| cancel.is_cancelled());
                                Message::Shell(ShellMsg::SaveWindow(epoch))
                            });
                        }
                    }
                }
            }
            ShellMsg::SaveWindow(epoch) => {
                if epoch == self.window_epoch {
                    self.save_preferences(context);
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
                self.save_preferences(context);
            }
            ShellMsg::StartupChosen(startup) => {
                self.prefs.startup = startup;
                self.save_preferences(context);
            }
            ShellMsg::PreferencesSaved(saved, window, result) => {
                self.prefs_saving = false;
                if let Err(error) = &result {
                    self.alert(format!("Preferences not saved: {error}"));
                }
                // A choice made on the page reports there; window-size
                // writes stay quiet.
                if self.main == Main::Settings || result.is_err() {
                    self.settings.notes.insert(
                        settings_ui::Page::General,
                        crate::settings_model::save_note(false, &result),
                    );
                }
                // A newer change arrived while this one was written.
                if saved != self.prefs || window != self.window_size {
                    self.save_preferences(context);
                } else if self.settings.loaded && result.is_ok() {
                    // The revision moved: the page's drafts save against it.
                    self.load_settings(context);
                }
            }
            ShellMsg::TitlesLoaded(titles) => {
                self.nav.titles_inflight = false;
                self.nav.titles.extend(titles);
                self.load_titles(context);
            }
            ShellMsg::AlertDismissed => self.alert = None,
            ShellMsg::ListTick => {
                // Only the first page is re-read; pages added with "Show
                // more" stay until a manual refresh.
                if self.preferences_loaded
                    && !self.nav.agents_loading
                    && self.nav.agents.len() <= AGENT_PAGE
                {
                    self.load_agents(context, None);
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
    }

    /// Leave the open conversation locally (the Agent keeps running) and
    /// show the empty new-task view.
    pub(super) fn new_task(&mut self, context: &ComponentContext<Self>) {
        if self.main != Main::Conversation {
            self.main = Main::Conversation;
            self.reprime_fields(context);
        }
        // Keyboard focus goes back to the composer (no-op while it is unbound).
        let _ = self.chat.composer_ref.request_focus();
        self.chat.epoch += 1;
        if let Some(task) = self.chat.live_task.take() {
            task.cancel();
        }
        self.nav.selection.select_agent(None);
        self.chat.convo = None;
        self.chat.detail = None;
        self.chat.live = LiveStatus::Idle;
        self.chat.items = Rc::new(Vec::new());
        self.chat.items_revision = 0;
        self.chat.unseen = 0;
        if self.panel == Panel::Details {
            self.close_panel();
        }
        self.load_routing_preview(context);
        self.set_status("New task.");
    }

    /// The project a new task starts in: its checkout, and its profile
    /// allowlist when it is registered.
    fn pick_project(&mut self, checkout: &str) {
        let registered = self
            .nav
            .projects
            .iter()
            .find(|project| project.workspace == checkout)
            .cloned();
        self.launch.workspace = checkout.to_owned();
        self.launch.custom_workspace = false;
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
        if let Some(agent) = nav::recents(&self.nav.agents, 1).first().cloned().cloned() {
            self.nav.selection.select_agent(Some(agent.id.clone()));
            self.open_conversation(&agent, context);
        }
    }

    /// Read titles of shown Agents that have none yet, one batch at a time.
    fn load_titles(&mut self, context: &ComponentContext<Self>) {
        if self.nav.titles_inflight {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let branch_children = self.nav.branches.values().flat_map(|branch| match branch {
            Branch::Loaded { children, .. } => children.as_slice(),
            _ => &[],
        });
        let mut missing: Vec<String> = Vec::new();
        for agent in self.nav.agents.iter().chain(branch_children) {
            if !self.nav.titles.contains_key(&agent.id) && !missing.contains(&agent.id) {
                missing.push(agent.id.clone());
            }
            if missing.len() == TITLE_BATCH {
                break;
            }
        }
        if missing.is_empty() {
            return;
        }
        self.nav.titles_inflight = true;
        let _task = context.spawn_background(move |_| {
            let titles = client::block_on(async {
                futures_util::stream::iter(missing)
                    .map(|id| {
                        let data_dir = data_dir.clone();
                        async move {
                            // A failed read leaves the id as the title; it
                            // is not retried in a loop.
                            let title = client::first_input(&data_dir, &id)
                                .await
                                .ok()
                                .flatten()
                                .and_then(|text| nav::title(&text));
                            (id, title)
                        }
                    })
                    .buffer_unordered(client::MAX_CONCURRENT_READS)
                    .collect::<Vec<_>>()
                    .await
            });
            Message::Shell(ShellMsg::TitlesLoaded(titles))
        });
    }

    /// The conversation title of `agent`: its first message, else its id.
    pub(super) fn agent_title(&self, agent: &str) -> String {
        self.nav
            .titles
            .get(agent)
            .cloned()
            .flatten()
            .unwrap_or_else(|| agent.to_owned())
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
        let state = detail.state.clone();
        for agent in self.nav.agents.iter_mut().filter(|agent| agent.id == id) {
            agent.state = state.clone();
        }
        for branch in self.nav.branches.values_mut() {
            if let Branch::Loaded { children, .. } = branch {
                for agent in children.iter_mut().filter(|agent| agent.id == id) {
                    agent.state = state.clone();
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

    /// The main area's width from the window width, the sidebar and the
    /// open panel, or None until the first size observation.
    pub(super) fn main_width(&self) -> Option<f64> {
        let panel = if self.panel == Panel::Closed {
            0.0
        } else {
            theme::layout::PANEL_WIDTH
        };
        self.window_width
            .map(|width| width - theme::layout::SIDEBAR_WIDTH - panel)
    }

    /// Write the theme and window size into `[desktop]`, one write at a
    /// time (a newer state is written when the running write reports back).
    pub(super) fn save_preferences(&mut self, context: &ComponentContext<Self>) {
        if self.prefs_saving {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.prefs_saving = true;
        let (preferences, window) = (self.prefs, self.window_size);
        let _task = context.spawn_background(move |_| {
            let saved = client::block_on(config_client::save_preferences(
                &data_dir,
                preferences,
                window,
            ))
            .map_err(|error| error.to_string());
            Message::Shell(ShellMsg::PreferencesSaved(preferences, window, saved))
        });
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
            ..Default::default()
        };
        let (epoch, id) = (self.nav.tree_epoch, id.to_owned());
        let _task = context.spawn_background(move |_| {
            let page = load_agents_blocking(&data_dir, &request);
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
            ..Default::default()
        };
        self.nav.agents_loading = true;
        let _task = context.spawn_background(move |_| {
            Message::Shell(ShellMsg::AgentsLoaded(
                list_epoch,
                append,
                load_agents_blocking(&data_dir, &request),
            ))
        });
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
                        .map_or(theme::layout::WINDOW_WIDTH, |w| f64::from(w.0)),
                    self.preferred_window
                        .map_or(theme::layout::WINDOW_HEIGHT, |w| f64::from(w.1)),
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
        let mut columns = vec![
            GridLength::Pixel(theme::layout::SIDEBAR_WIDTH),
            GridLength::STAR,
        ];
        let panel: View = if self.panel == Panel::Closed {
            View::empty()
        } else {
            columns.push(GridLength::Pixel(theme::layout::PANEL_WIDTH));
            Border::new()
                .grid_column(2)
                .background(palette.sidebar())
                .content(self.panel_view(context, palette))
        };
        let window = Grid::new()
            .background(palette.surface())
            .columns(columns)
            .children((
                Border::new()
                    .grid_column(0)
                    .background(palette.sidebar())
                    .content(self.sidebar_view(context, palette)),
                Border::new().grid_column(1).content(main),
                panel,
            ));
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

    /// The navigation drawer: the app name, the New task FAB, the projects
    /// with their Agents, Recents, then the fleet status and Settings at the
    /// bottom. M3 navigation-drawer metrics: 12-DIP container padding,
    /// 56-DIP items, title-small section headers at the items' 16-DIP inset.
    fn sidebar_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let gutter = DRAWER_PADDING;
        let header = Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .margin(Thickness::new(
                gutter + theme::spacing::MD,
                theme::spacing::MD,
                gutter,
                theme::spacing::SM,
            ))
            .children((
                m3::title("Eliot Swarm", theme::type_scale::TITLE_MEDIUM)
                    .vertical_alignment(VerticalAlignment::Center),
                m3::icon_button(palette)
                    .grid_column(1)
                    .automation_name("Refresh")
                    .on_click(context.message(Message::Shell(ShellMsg::RefreshRequested)))
                    .content(m3::icon(m3::glyph::REFRESH)),
            ));
        let new_task = m3::fab(palette)
            .grid_row(1)
            .margin(Thickness::new(
                gutter,
                theme::spacing::XS,
                gutter,
                theme::spacing::SM,
            ))
            .automation_name("New task")
            .on_click(context.message(Message::Shell(ShellMsg::NewTask)))
            .content(m3::icon_label(m3::glyph::NEW_TASK, "New task"));
        let new_task_selected = self.main == Main::Conversation && self.chat.convo.is_none();

        let mut items: Vec<KeyedView> = vec![KeyedView::new(
            "projects-caption",
            section_header("Projects"),
        )];
        let registered: Vec<(String, String)> = self
            .nav
            .projects
            .iter()
            .map(|project| (project.workspace.clone(), project.id.clone()))
            .collect();
        let rows = nav::rows(
            &self.nav.agents,
            &self.nav.branches,
            &self.nav.expanded,
            true,
            &|label| nav::project_name(label, &registered),
        );
        let sections = nav::sections(rows, &registered);
        if sections.is_empty() {
            items.push(KeyedView::new(
                "no-projects",
                quiet_line(if self.nav.agents_loading {
                    "Loading…"
                } else {
                    "No projects yet"
                })
                .margin(Thickness::new(ITEM_INSET, 0.0, 0.0, theme::spacing::SM)),
            ));
        }
        for section in &sections {
            let selected = new_task_selected && self.launch.workspace == section.checkout;
            items.push(KeyedView::new(
                format!("project:{}:{selected}", section.checkout),
                self.project_row(section, selected, context, palette),
            ));
            if section.rows.is_empty() {
                items.push(KeyedView::new(
                    format!("empty:{}", section.checkout),
                    quiet_line("No tasks").margin(Thickness::new(
                        CHILD_INSET + ITEM_INSET,
                        0.0,
                        0.0,
                        theme::spacing::SM,
                    )),
                ));
            }
            for row in &section.rows {
                let selected = self.main == Main::Conversation
                    && self.nav.selection.agent.as_deref() == Some(row.agent.id.as_str());
                items.push(KeyedView::new(
                    format!("agent:{}:{selected}:{}", row.agent.id, row.expanded),
                    self.agent_row(row, selected, context, palette),
                ));
            }
        }
        if self.nav.agents.len() < self.nav.agents_total && self.nav.agents_next.is_some() {
            items.push(KeyedView::new(
                "more",
                m3::text_button(palette)
                    .is_enabled(!self.nav.agents_loading)
                    .margin(Thickness::new(
                        CHILD_INSET + ITEM_INSET - 12.0,
                        0.0,
                        0.0,
                        0.0,
                    ))
                    .on_click(context.message(Message::Shell(ShellMsg::MoreAgentsRequested)))
                    .content(m3::label(&format!(
                        "Show more ({} of {})",
                        self.nav.agents.len(),
                        self.nav.agents_total
                    ))),
            ));
        }
        let recents = nav::recents(&self.nav.agents, RECENTS);
        if !recents.is_empty() {
            items.push(KeyedView::new("recents-caption", section_header("Recents")));
            for agent in recents {
                items.push(KeyedView::new(
                    format!("recent:{}", agent.id),
                    self.recent_row(agent, context, palette),
                ));
            }
        }
        let list = ScrollViewer::new()
            .grid_row(2)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Projects and Agents")
            .content(
                StackPanel::new()
                    .margin(Thickness::new(gutter, 0.0, gutter, gutter))
                    .keyed_children(items),
            );

        let settings_open = self.main == Main::Settings;
        let monitor_open = self.panel == Panel::Monitor;
        let bottom = StackPanel::new()
            .grid_row(3)
            .margin(Thickness::new(gutter, 0.0, gutter, gutter))
            .keyed_children(vec![
                KeyedView::new(
                    "divider",
                    Border::new()
                        .height(1.0)
                        .margin(Thickness::new(
                            theme::spacing::MD,
                            0.0,
                            theme::spacing::MD,
                            theme::spacing::SM,
                        ))
                        .background(palette.outline_variant()),
                ),
                KeyedView::new(
                    format!("fleet:{monitor_open}"),
                    self.fleet_row(context, palette),
                ),
                KeyedView::new(
                    format!("settings:{settings_open}"),
                    m3::nav_item(palette, settings_open)
                        .automation_name("Settings")
                        .on_click(context.message(Message::Shell(ShellMsg::SettingsOpened)))
                        .content(m3::nav_content(m3::icon_label(
                            m3::glyph::SETTINGS,
                            "Settings",
                        ))),
                ),
            ]);

        Grid::new()
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
            ])
            .children((header, new_task, list, bottom))
    }

    /// A project item: a new task in that project.
    fn project_row(
        &self,
        section: &Section,
        selected: bool,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        m3::nav_item(palette, selected)
            .automation_name(format!("New task in {}", section.name))
            .on_click(context.message(Message::Shell(ShellMsg::ProjectPicked(
                section.checkout.clone(),
            ))))
            .content(m3::nav_content(m3::icon_label(
                m3::glyph::FOLDER,
                &section.name,
            )))
    }

    /// An Agent item under its project: indented to the project's label,
    /// the expand chevron when it has workers, its title and a status dot
    /// when its state needs a look.
    fn agent_row(
        &self,
        row: &Row,
        selected: bool,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let agent = &row.agent;
        let title = self.agent_title(&agent.id);
        let chevron: View = if row.expandable() {
            let (glyph, name) = if row.expanded {
                (
                    m3::glyph::CHEVRON_DOWN,
                    format!("Collapse workers of {title}"),
                )
            } else {
                (
                    m3::glyph::CHEVRON_RIGHT,
                    format!("Show {} worker(s) of {title}", row.children),
                )
            };
            m3::icon_button(palette)
                .width(CHEVRON)
                .height(CHEVRON)
                .vertical_alignment(VerticalAlignment::Center)
                .automation_name(name)
                .on_click(context.message(Message::Shell(ShellMsg::ExpandToggled(
                    agent.id.clone(),
                    !row.expanded,
                ))))
                .content(m3::icon_sized(glyph, 12.0))
        } else {
            View::empty()
        };
        let mut name = format!("{title}, {}, Agent {}", agent.state, agent.id);
        if row.depth > 0 {
            name.push_str(&format!(", worker level {}", row.depth));
        }
        let note: View = match &row.note {
            Some(note) => quiet_line(note)
                .margin(Thickness::new(
                    theme::spacing::MD,
                    0.0,
                    0.0,
                    theme::spacing::XS,
                ))
                .grid_row(1)
                .grid_column(1)
                .into(),
            None => View::empty(),
        };
        Grid::new()
            .margin(Thickness::new(
                CHILD_INSET - CHEVRON + row.depth as f64 * TREE_INDENT,
                0.0,
                0.0,
                0.0,
            ))
            .columns([GridLength::Pixel(CHEVRON), GridLength::STAR])
            .rows([GridLength::Auto, GridLength::Auto])
            .children((
                Border::new()
                    .vertical_alignment(VerticalAlignment::Center)
                    .content(chevron),
                m3::nav_item(palette, selected)
                    .grid_column(1)
                    .automation_name(name)
                    .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))))
                    .content(m3::nav_content(row_content(
                        &title,
                        m3::list_dot(palette, &agent.state),
                    ))),
                note,
            ))
    }

    /// A Recents item: the title and its dot, no tree.
    fn recent_row(
        &self,
        agent: &UiAgent,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let title = self.agent_title(&agent.id);
        m3::nav_item(palette, false)
            .automation_name(format!("Recent: {title}, {}", agent.state))
            .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent.clone()))))
            .content(m3::nav_content(row_content(
                &title,
                m3::list_dot(palette, &agent.state),
            )))
    }

    /// The fleet in one line ("2 running · 1 waiting"); it opens the
    /// monitor.
    fn fleet_row(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let (summary, tone) = fleet_summary(&self.nav.agents, self.nav.agents_total);
        let dot = palette.dot(tone).map_or_else(View::empty, m3::dot);
        let open = self.panel == Panel::Monitor;
        m3::nav_item(palette, open)
            .automation_name(format!(
                "Monitor: {summary} ({})",
                if open { "open" } else { "closed" }
            ))
            .on_click(context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Monitor))))
            .content(m3::nav_content(
                Grid::new()
                    .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                    .column_spacing(12.0)
                    .children((
                        m3::icon(m3::glyph::PULSE),
                        TextBlock::new()
                            .grid_column(1)
                            .text(summary)
                            .vertical_alignment(VerticalAlignment::Center)
                            .text_trimming(TextTrimming::CharacterEllipsis),
                        Border::new().grid_column(2).content(dot),
                    )),
            ))
    }

    /// Show a short notice above the composer (refusals, uncertain
    /// outcomes, failed reads); it stays until dismissed or replaced.
    pub(super) fn alert(&mut self, text: impl Into<String>) {
        let text = text.into();
        self.set_status(text.clone());
        self.alert = Some(text);
    }

    /// The data directory could not be opened (for example, the default
    /// root is owned by another principal). Nothing is moved; the owner may
    /// select a fresh private directory, which is remembered.
    fn startup_error_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let error = self.alert.clone().unwrap_or_default();
        StackPanel::new()
            .max_width(theme::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .vertical_alignment(VerticalAlignment::Center)
            .spacing(theme::spacing::MD)
            .margin(Thickness::uniform(theme::spacing::LG))
            .children((
                m3::title("Swarm could not start here", theme::type_scale::HEADLINE_SMALL),
                m3::text(
                    "Swarm could not open this data directory. Existing settings have not been moved.",
                    theme::type_scale::BODY_LARGE,
                )
                .text_wrapping(TextWrapping::Wrap),
                m3::muted(error, theme::type_scale::BODY_MEDIUM)
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

/// A navigation-drawer section header: title-small at the items' inset.
fn section_header(text: &str) -> TextBlock {
    m3::caption(text).margin(Thickness::new(
        ITEM_INSET,
        theme::spacing::MD,
        0.0,
        theme::spacing::SM,
    ))
}

/// A muted one-line note in a list.
fn quiet_line(text: &str) -> TextBlock {
    m3::muted(text, theme::type_scale::BODY_SMALL).text_trimming(TextTrimming::CharacterEllipsis)
}

/// A row's content: the title, trimmed, and an optional trailing mark.
fn row_content(title: &str, trailing: View) -> View {
    Grid::new()
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(theme::spacing::SM)
        .children((
            TextBlock::new()
                .text(title.to_owned())
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .content(trailing),
        ))
}

// The fleet line and its most urgent tone, from the listed Agents.
pub(super) fn fleet_summary(agents: &[UiAgent], total: usize) -> (String, StatusTone) {
    let count = |tone: StatusTone| {
        agents
            .iter()
            .filter(|agent| StatusTone::of_state(&agent.state) == tone)
            .count()
    };
    let (running, waiting, failed, unknown) = (
        count(StatusTone::Active) + count(StatusTone::Pending),
        count(StatusTone::Attention),
        count(StatusTone::Failed),
        count(StatusTone::Unknown),
    );
    let mut parts = Vec::new();
    if running > 0 {
        parts.push(format!("{running} running"));
    }
    if waiting > 0 {
        parts.push(format!("{waiting} waiting"));
    }
    if failed > 0 {
        parts.push(format!("{failed} failed"));
    }
    if unknown > 0 {
        parts.push(format!("{unknown} unknown"));
    }
    let tone = if failed > 0 {
        StatusTone::Failed
    } else if waiting > 0 {
        StatusTone::Attention
    } else if unknown > 0 {
        StatusTone::Unknown
    } else if running > 0 {
        StatusTone::Active
    } else {
        StatusTone::Ready
    };
    let summary = if !parts.is_empty() {
        parts.join(" · ")
    } else if total == 0 {
        "No Agents yet".to_owned()
    } else {
        format!("All quiet · {total} Agents")
    };
    (summary, tone)
}
