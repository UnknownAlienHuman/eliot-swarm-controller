//! Navigation shell: projects, the Agent list, screen selection and the root layout.

use super::*;
use crate::nav::{Branch, Row};
use crate::settings::ThemeChoice;
use crate::theme::StatusTone;
use crate::theme::winui::Palette;

#[derive(Clone, Debug)]
pub(super) enum ShellMsg {
    DaemonReady(Fallible<eliot_core::api::SettingsReadResponse>),
    UseFreshDirectory,
    ProjectsLoaded(Fallible<Vec<UiProject>>),
    AgentsLoaded(u64, bool, Fallible<AgentPage>),
    ProjectSelected(Option<usize>),
    AgentSelected(Option<usize>),
    RefreshRequested,
    NewConversation,
    MoreAgentsRequested,
    HighlightAgent(String),
    ScreenSelected(Screen),
    /// The window's actual Light/Dark scheme (Reactor `on_color_scheme`).
    SchemeObserved(ColorScheme),
    /// The window's client size (Reactor `on_window_size`).
    WindowResized(WindowSize),
    SaveWindow(u64),
    /// System → Light → Dark → System.
    ThemeCycled,
    /// The re-created editors of this re-prime exist: give them their text.
    FieldsPrimed(u64),
    /// The periodic High Contrast reading.
    HighContrastRead(bool),
    /// Expand (true) or collapse an Agent's children.
    ExpandToggled(String, bool),
    /// Tree epoch, manager id and its direct-children page.
    BranchLoaded(u64, String, Fallible<AgentPage>),
    /// The theme choice written to the settings file (or not).
    SettingsSaved(ThemeChoice, Option<(u32, u32)>, Result<(), String>),
}

/// Reactor 0.100.0 reports the window's Light/Dark scheme only, and a High
/// Contrast theme can keep the same base scheme, so High Contrast is read
/// at the monitor's cadence.
const HIGH_CONTRAST_POLL_MS: u64 = 2_000;
/// Re-check for the empty editor render one frame later.
const PRIME_RETRY_MS: u64 = 16;
/// Indent per tree level in the Agent list.
const TREE_INDENT: f64 = 16.0;
/// The expand chevron button: FontIcon's 20-DIP glyph inside WinUI's fixed
/// button padding (11, 5, 11, 6) shows whole at 44 x 32.
const CHEVRON_BUTTON: (f64, f64) = (44.0, 32.0);
/// WinUI draws a Button's focus visual 3 DIP outside its bounds
/// (`FocusVisualMargin` -3), and Reactor 0.100.0 cannot change that, so the
/// chevron column holds the button plus that margin and 1 DIP on each side.
const CHEVRON_COLUMN: f64 = CHEVRON_BUTTON.0 + 2.0 * 4.0;

fn window_theme(choice: ThemeChoice) -> WindowTheme {
    match choice {
        ThemeChoice::System => WindowTheme::System,
        ThemeChoice::Light => WindowTheme::Light,
        ThemeChoice::Dark => WindowTheme::Dark,
    }
}

fn theme_glyph(choice: ThemeChoice) -> &'static str {
    match choice {
        ThemeChoice::System => m3::glyph::THEME_SYSTEM,
        ThemeChoice::Light => m3::glyph::THEME_LIGHT,
        ThemeChoice::Dark => m3::glyph::THEME_DARK,
    }
}

/// Theme choice (persisted per user), the scheme the window actually shows,
/// and High Contrast.
pub(super) struct Appearance {
    pub(super) pref: ThemeChoice,
    dark: bool,
    high_contrast: bool,
}

impl Appearance {
    pub(super) fn new() -> Self {
        Self {
            pref: ThemeChoice::System,
            dark: false,
            high_contrast: crate::accessibility::high_contrast_on(),
        }
    }

    pub(super) fn palette(&self) -> theme::winui::Palette {
        theme::winui::Palette::new(self.dark, self.high_contrast)
    }
}

/// Projects, the Agent list pages, expanded branches and the selection.
pub(super) struct ShellState {
    pub(super) selection: Selection,
    pub(super) projects: Vec<UiProject>,
    pub(super) project_idx: Option<usize>,
    /// Listed pages, in daemon order.
    pub(super) agents: Vec<UiAgent>,
    /// Direct-children reads of expanded managers.
    pub(super) branches: HashMap<String, Branch>,
    pub(super) expanded: HashSet<String>,
    /// Tags branch reads; a reply from before a project switch is dropped.
    pub(super) tree_epoch: u64,
    /// The rows on screen (see [`nav::rows`]); `agent_idx` indexes these.
    pub(super) rows: Vec<Row>,
    /// Row ids as last rendered, and the render count when `rows` was last
    /// replaced (equal to the current count: not rendered yet).
    pub(super) published: Vec<String>,
    pub(super) rows_render: u64,
    pub(super) agent_idx: Option<usize>,
    pub(super) agents_next: Option<String>,
    pub(super) agents_total: usize,
    pub(super) agents_loading: bool,
    /// Tags agent-list loads; a reply for an older request is dropped.
    pub(super) list_epoch: u64,
    pub(super) pending_select: Option<UiAgent>,
}

impl DesktopApp {
    pub(super) fn shell_update(&mut self, msg: ShellMsg, context: &ComponentContext<Self>) {
        match msg {
            ShellMsg::DaemonReady(result) => match result {
                Ok(read) => {
                    self.startup_error = false;
                    self.appearance.pref = crate::settings::theme(&read);
                    self.preferred_window = crate::settings::window(&read);
                    self.preferences_loaded = true;
                    self.load_agents(context, None);
                    self.load_profile_list(context);
                    self.watch_high_contrast(context);
                    if let Ok(data_dir) = self.data_dir.clone() {
                        let _task = context.spawn_background(move |_| {
                            Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(
                                &data_dir,
                            )))
                        });
                    }
                }
                Err(error) => {
                    self.startup_error = true;
                    self.set_status(format!("Cannot start Swarm: {error}"));
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
                            self.set_status(format!(
                                "Starting in fresh private directory {}",
                                selected.display()
                            ));
                            self.start_connection(selected, context);
                        }
                        Err(error) => self
                            .set_status(format!("Cannot select fresh private directory: {error}")),
                    }
                }
            }
            ShellMsg::ProjectsLoaded(projects) => match projects {
                Ok(projects) => {
                    let count = projects.len();
                    self.nav.projects = projects;
                    // Group captions name registered projects by id.
                    self.refresh_rows(context);
                    self.set_status(format!(
                        "{count} registered project(s); listing every Agent in scope."
                    ));
                }
                Err(error) => self.set_status(format!("project.list failed: {error}")),
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
                        self.refresh_rows(context);
                    }
                    Err(error) => self.set_status(format!("agent.list failed: {error}")),
                }
            }
            ShellMsg::ProjectSelected(index) => {
                // The owner cannot clear a ComboBox: None only reports
                // replaced items, and the same index is no change.
                if index.is_none() || index == self.nav.project_idx {
                    return;
                }
                self.nav.project_idx = index;
                self.nav.agents.clear();
                self.nav.branches.clear();
                self.nav.expanded.clear();
                self.nav.tree_epoch += 1;
                self.nav.rows.clear();
                self.nav.agent_idx = None;
                self.nav.agents_next = None;
                self.nav.agents_total = 0;
                match self.selected_project().cloned() {
                    Some(project) => {
                        self.launch.profiles = project.profiles.clone();
                        self.launch.workspace = project.workspace.clone();
                        self.nav.selection.project = Some(project.id.clone());
                    }
                    None => {
                        self.launch.profiles.clear();
                        self.nav.selection.project = None;
                    }
                }
                self.launch.profile_idx = None;
                self.nav.selection.profile = None;
                self.load_agents(context, None);
            }
            ShellMsg::AgentSelected(index) => {
                self.nav.agent_idx = index;
                // A list reload clears the native selection (None); the open
                // conversation and its feed stay as they are.
                if let Some(agent) = index
                    .and_then(|index| self.nav.rows.get(index))
                    .map(|row| row.agent.clone())
                    && self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id)
                {
                    self.open_conversation(&agent, context);
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
                self.refresh_rows(context);
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
                self.refresh_rows(context);
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
                if self.chat.convo.is_some() {
                    self.chat.failures = 0;
                    self.start_snapshot(context);
                }
            }
            ShellMsg::NewConversation => {
                // Keyboard focus goes back to the composer (no-op while it is unbound).
                let _ = self.chat.composer_ref.request_focus();
                // Leave the conversation locally; the Agent keeps running.
                self.chat.epoch += 1;
                if let Some(task) = self.chat.live_task.take() {
                    task.cancel();
                }
                self.nav.selection.select_agent(None);
                self.nav.agent_idx = None;
                self.chat.convo = None;
                self.chat.detail = None;
                self.chat.live = LiveStatus::Idle;
                self.chat.items = Rc::new(Vec::new());
                self.chat.items_revision = 0;
                self.chat.unseen = 0;
                self.set_status("New conversation: launch an Agent with its first message.");
            }
            ShellMsg::MoreAgentsRequested => {
                if let Some(after) = self.nav.agents_next.clone() {
                    self.load_agents(context, Some(after));
                }
            }
            ShellMsg::HighlightAgent(id) => {
                if self.nav.selection.agent.as_deref() == Some(id.as_str()) {
                    self.highlight_published(&id, context);
                }
            }
            ShellMsg::ScreenSelected(screen) => {
                if self.screen != screen {
                    self.reprime_fields(context);
                }
                self.screen = screen;
                match screen {
                    Screen::Profiles if !self.editor.loaded && !self.editor.busy => {
                        self.load_profile_source(context);
                        self.load_profile_list(context);
                    }
                    Screen::Settings if !self.unified.loaded && !self.unified.busy => {
                        self.load_settings(context)
                    }
                    Screen::Automation => self.load_rules(context),
                    Screen::Scripts => {
                        if !self.scripts.listed {
                            self.load_scripts(context);
                        }
                        self.load_rules(context);
                    }
                    _ => {}
                }
            }
            ShellMsg::SchemeObserved(scheme) => {
                let before = self.palette();
                self.appearance.dark = scheme == ColorScheme::Dark;
                self.appearance.high_contrast = crate::accessibility::high_contrast_on();
                if self.palette() != before {
                    // The palette-keyed tree is rebuilt with new controls.
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
                    self.save_settings(context);
                }
            }
            ShellMsg::HighContrastRead(on) => {
                if on != self.appearance.high_contrast {
                    self.appearance.high_contrast = on;
                    self.reprime_fields(context);
                }
                self.watch_high_contrast(context);
            }
            ShellMsg::ThemeCycled => {
                self.appearance.pref = self.appearance.pref.next();
                self.save_settings(context);
            }
            ShellMsg::SettingsSaved(saved, window, result) => {
                self.settings_saving = false;
                if let Err(error) = result {
                    self.set_status(format!("Theme choice not saved: {error}"));
                }
                // A newer choice arrived while this one was written.
                if saved != self.appearance.pref || window != self.window_size {
                    self.save_settings(context);
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

    /// Reactor 0.100.0 creates a TextBox with `Text` before `AcceptsReturn`,
    /// and a TextBox that is still single-line keeps only the first line of
    /// the text it is given. Whenever the screen tree is re-created the
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

    /// The chat pane's width from the window width and the shell layout
    /// below (rail, Agent list, optional monitor, spacing), or None until
    /// the first size observation.
    pub(super) fn chat_pane_width(&self) -> Option<f64> {
        let monitor = if self.monitor.open {
            theme::layout::MONITOR_WIDTH + theme::spacing::SM
        } else {
            0.0
        };
        self.window_width.map(|width| {
            width
                - theme::layout::RAIL_WIDTH
                - theme::layout::LIST_WIDTH
                - 2.0 * theme::spacing::SM
                - monitor
        })
    }

    /// Write the current theme choice, one write at a time (a newer choice
    /// is written when the running write reports back).
    fn save_settings(&mut self, context: &ComponentContext<Self>) {
        if self.settings_saving {
            return;
        }
        self.settings_saving = true;
        let theme = self.appearance.pref;
        let window = self.window_size;
        let Ok(data_dir) = self.data_dir.clone() else {
            self.settings_saving = false;
            return;
        };
        let _task = context.spawn_background(move |_| {
            let saved =
                client::block_on(crate::settings::save_preferences(&data_dir, theme, window))
                    .map_err(|e| e.to_string());
            Message::Shell(ShellMsg::SettingsSaved(theme, window, saved))
        });
    }

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
                crate::settings::read(&data_dir).await
            })
            .map_err(|e| e.to_string());
            Message::Shell(ShellMsg::DaemonReady(result))
        });
    }

    /// `text` once the editors are primed, empty during the priming render.
    pub(super) fn primed<'a>(&self, text: &'a str) -> &'a str {
        if self.fields_primed { text } else { "" }
    }

    pub(super) fn selected_project(&self) -> Option<&UiProject> {
        self.nav
            .project_idx
            .and_then(|index| index.checked_sub(1))
            .and_then(|index| self.nav.projects.get(index))
    }

    /// Recompute the rows on screen and the native selection index.
    ///
    /// Never point the native SelectedIndex at a row this same render adds
    /// or moves: Reactor 0.100.0 applies the SelectedIndex before the new
    /// items reach the ListView; an index past them fails with E_INVALIDARG
    /// (0x80070057), which Reactor treats as a fatal native command failure
    /// (the process aborts with 0xC0000409; standalone repro 2026-11-14), and
    /// an index shifted by an insertion would select another row. An index
    /// is kept only when the same Agent was already on it; otherwise the
    /// selected row is highlighted on the next tick, once published.
    pub(super) fn refresh_rows(&mut self, context: &ComponentContext<Self>) {
        let registered: Vec<(String, String)> = self
            .nav
            .projects
            .iter()
            .map(|project| (project.workspace.clone(), project.id.clone()))
            .collect();
        let group = self.selected_project().is_none();
        let rows = nav::rows(
            &self.nav.agents,
            &self.nav.branches,
            &self.nav.expanded,
            group,
            &|label| nav::project_name(label, &registered),
        );
        // Rows replaced earlier in this same message batch never reached
        // the native list; the published ids are the last rendered ones.
        if self.renders.get() != self.nav.rows_render {
            self.nav.published = self
                .nav
                .rows
                .iter()
                .map(|row| row.agent.id.clone())
                .collect();
        }
        self.nav.rows = rows;
        self.nav.rows_render = self.renders.get();
        let Some(id) = self.nav.selection.agent.clone() else {
            self.nav.agent_idx = None;
            return;
        };
        self.highlight_published(&id, context);
    }

    /// Point the native selection at `id` only where the native list already
    /// shows it at the same index; otherwise clear it and try again on the
    /// next tick, after the rows are published.
    pub(super) fn highlight_published(&mut self, id: &str, context: &ComponentContext<Self>) {
        let index = self.nav.rows.iter().position(|row| row.agent.id == id);
        let rendered: Vec<&str> = if self.renders.get() == self.nav.rows_render {
            self.nav.published.iter().map(String::as_str).collect()
        } else {
            // A render happened since the rows changed: they are on screen.
            self.nav
                .rows
                .iter()
                .map(|row| row.agent.id.as_str())
                .collect()
        };
        let stable = index.is_some_and(|index| rendered.get(index) == Some(&id));
        if stable {
            self.nav.agent_idx = index;
        } else {
            self.nav.agent_idx = None;
            if index.is_some() {
                let id = id.to_owned();
                let _task =
                    context.spawn_background(move |_| Message::Shell(ShellMsg::HighlightAgent(id)));
            }
        }
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
        // An Agent's project label is its registered checkout path, not the
        // registration id, so the filter is the checkout.
        let request = AgentListRequest {
            after,
            limit: AGENT_PAGE,
            project: self
                .selected_project()
                .map(|project| project.workspace.clone()),
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

    /// The window: navigation rail, the selected screen on rounded panes,
    /// the openable monitor and one status line (issue #407 §1).
    pub(super) fn shell_view(&self, context: &mut ViewContext<Self>) -> View {
        context.window_title("Swarm Desktop");
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
                .theme(window_theme(self.appearance.pref)),
        );
        context.on_color_scheme(
            context.callback(|scheme| Message::Shell(ShellMsg::SchemeObserved(scheme))),
        );
        context
            .on_window_size(context.callback(|size| Message::Shell(ShellMsg::WindowResized(size))));
        let palette = self.palette();

        let screen: View = match self.screen {
            Screen::Chat => Grid::new()
                .columns([
                    GridLength::Pixel(theme::layout::LIST_WIDTH),
                    GridLength::STAR,
                ])
                .column_spacing(theme::spacing::SM)
                .children((
                    m3::pane(palette)
                        .grid_column(0)
                        .content(self.nav_view(context, palette)),
                    m3::pane(palette)
                        .padding(Thickness::uniform(0.0))
                        .grid_column(1)
                        .content(self.chat_view(context, palette)),
                )),
            Screen::Profiles => m3::pane(palette).content(self.profiles_view(context, palette)),
            Screen::Settings => m3::pane(palette).content(self.settings_view(context, palette)),
            Screen::Automation => m3::pane(palette).content(config_view::automation_screen(
                &self.automation.rules,
                &self.automation.status,
                context.message(Message::Automation(
                    AutomationMsg::AutomationRefreshRequested,
                )),
                palette,
            )),
            Screen::Scripts => m3::pane(palette).content(self.scripts_view(context, palette)),
        };
        let mut columns = vec![GridLength::STAR];
        let monitor: View = if self.monitor.open {
            columns.push(GridLength::Pixel(theme::layout::MONITOR_WIDTH));
            m3::pane(palette)
                .grid_column(1)
                .content(self.monitor_view(context, palette))
        } else {
            View::empty()
        };
        let screen = if self.startup_error {
            StackPanel::new().spacing(theme::spacing::MD).children((
                TextBlock::new().text("Swarm could not open this data directory. Existing settings have not been moved.").text_wrapping(TextWrapping::Wrap),
                Button::new().automation_name("Use fresh private directory")
                    .on_click(context.message(Message::Shell(ShellMsg::UseFreshDirectory)))
                    .content(TextBlock::new().text("Use a fresh private directory")),
            ))
        } else {
            screen
        };
        let content = Grid::new()
            .grid_column(1)
            .rows([GridLength::STAR, GridLength::Auto])
            .margin(Thickness::new(
                0.0,
                theme::spacing::SM,
                theme::spacing::SM,
                0.0,
            ))
            .children((
                Grid::new()
                    .columns(columns)
                    .column_spacing(theme::spacing::SM)
                    .children((screen, monitor)),
                Border::new().grid_row(1).content(self.status_line(palette)),
            ));

        let window = Grid::new()
            .background(palette.background())
            .columns([
                GridLength::Pixel(theme::layout::RAIL_WIDTH),
                GridLength::STAR,
            ])
            .children((self.rail_view(context, palette), content));
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

    /// The M3 navigation rail: the new-conversation FAB, the destinations
    /// with their active indicator, and the monitor and theme switches.
    fn rail_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let chat = self.rail_item(
            palette,
            self.screen == Screen::Chat,
            m3::glyph::CHAT,
            "Chat",
            "Chat screen",
            context.message(Message::Shell(ShellMsg::ScreenSelected(Screen::Chat))),
        );
        let profiles = self.rail_item(
            palette,
            self.screen == Screen::Profiles,
            m3::glyph::PROFILES,
            "Profiles",
            "Profiles screen",
            context.message(Message::Shell(ShellMsg::ScreenSelected(Screen::Profiles))),
        );
        let settings = self.rail_item(
            palette,
            self.screen == Screen::Settings,
            m3::glyph::PROFILES,
            "Settings",
            "Settings screen",
            context.message(Message::Shell(ShellMsg::ScreenSelected(Screen::Settings))),
        );
        let automation = self.rail_item(
            palette,
            self.screen == Screen::Automation,
            m3::glyph::AUTOMATION,
            "Automation",
            "Automation screen",
            context.message(Message::Shell(ShellMsg::ScreenSelected(Screen::Automation))),
        );
        let scripts = self.rail_item(
            palette,
            self.screen == Screen::Scripts,
            m3::glyph::SCRIPTS,
            "Scripts",
            "Scripts screen",
            context.message(Message::Shell(ShellMsg::ScreenSelected(Screen::Scripts))),
        );
        let monitor = self.rail_item(
            palette,
            self.monitor.open,
            m3::glyph::MONITOR,
            "Monitor",
            "Monitor",
            context.message(Message::Monitor(MonitorMsg::MonitorToggled(
                !self.monitor.open,
            ))),
        );
        let fab = Button::new()
            .grid_row(0)
            .automation_name("New conversation")
            .resource_overrides(palette.fab())
            .width(56.0)
            .height(56.0)
            .horizontal_alignment(HorizontalAlignment::Center)
            .on_click(context.message(Message::Shell(ShellMsg::NewConversation)))
            .content(m3::icon(m3::glyph::ADD));
        let pref = self.appearance.pref;
        let theme_switch = Button::new()
            .automation_name(format!(
                "Theme: {} (switch to {})",
                pref.label(),
                pref.next().label()
            ))
            .resource_overrides(palette.rail_item(false))
            .width(theme::layout::RAIL_ITEM_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .on_click(context.message(Message::Shell(ShellMsg::ThemeCycled)))
            .content(rail_label(theme_glyph(pref), pref.label(), false, None));
        Grid::new()
            .grid_column(0)
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
            ])
            .row_spacing(theme::spacing::XL)
            .margin(Thickness::new(
                0.0,
                theme::spacing::MD,
                0.0,
                theme::spacing::MD,
            ))
            .children((
                fab,
                StackPanel::new()
                    .grid_row(1)
                    .spacing(theme::spacing::MD)
                    .children((chat, settings, profiles, automation, scripts)),
                StackPanel::new()
                    .grid_row(3)
                    .spacing(theme::spacing::SM)
                    .children((monitor, theme_switch)),
            ))
    }

    /// One rail destination: a real Button with the M3 indicator pill behind
    /// its icon when selected. The accessible name carries the selection.
    fn rail_item(
        &self,
        palette: Palette,
        selected: bool,
        glyph: &str,
        label: &str,
        name: &str,
        on_click: Callback<()>,
    ) -> View {
        Button::new()
            .automation_name(if selected {
                format!("{name} (current)")
            } else {
                name.to_owned()
            })
            .resource_overrides(palette.rail_item(selected))
            .width(theme::layout::RAIL_ITEM_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .on_click(on_click)
            .content(rail_label(
                glyph,
                label,
                selected,
                selected.then(|| palette.indicator()),
            ))
    }

    /// Connection state as a chip, then the latest status message.
    fn status_line(&self, palette: Palette) -> View {
        let (tone, short) = match &self.chat.live {
            LiveStatus::Idle => (StatusTone::Neutral, "No conversation"),
            LiveStatus::Loading => (StatusTone::Pending, "Loading"),
            LiveStatus::Live => (StatusTone::Ready, "Live"),
            LiveStatus::Stale(_) => (StatusTone::Unknown, "Stale"),
            LiveStatus::Reconnecting { .. } => (StatusTone::Unknown, "Reconnecting"),
            LiveStatus::Stopped(_) => (StatusTone::Failed, "Stopped"),
        };
        let detail = match &self.chat.live {
            LiveStatus::Stale(_) | LiveStatus::Reconnecting { .. } | LiveStatus::Stopped(_) => {
                format!("{} · {}", self.chat.live.label(), self.status)
            }
            _ => self.status.clone(),
        };
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(theme::spacing::SM)
            .margin(Thickness::new(
                theme::spacing::XS,
                theme::spacing::XS,
                theme::spacing::SM,
                theme::spacing::SM,
            ))
            .children((
                m3::chip(palette, tone, short),
                m3::muted(detail, theme::type_scale::BODY_SMALL)
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_trimming(TextTrimming::CharacterEllipsis),
            ))
    }

    /// Projects and Agents: the list beside the conversation.
    pub(super) fn nav_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let mut project_names = vec![ALL_PROJECTS.to_owned()];
        project_names.extend(self.nav.projects.iter().map(|project| project.id.clone()));
        // WinUI drops a ComboBox's selection when its items are replaced, so
        // a new project list gets a new control (as in `configure::panel`).
        let picker_key = format!("projects:{}", project_names.join("\u{1f}"));
        let project_picker = Grid::new().grid_row(1).keyed_children(vec![KeyedView::new(
            picker_key,
            ComboBox::new()
                .items_source(project_names)
                .selected_index(self.nav.project_idx)
                .placeholder_text("Project")
                .automation_name("Project")
                .horizontal_alignment(HorizontalAlignment::Stretch)
                .on_selection_changed(
                    context.callback(|value| Message::Shell(ShellMsg::ProjectSelected(value))),
                ),
        )]);

        let agent_items: Vec<KeyedView> = self
            .nav
            .rows
            .iter()
            .map(|row| KeyedView::new(row.agent.id.clone(), nav_row(row, context, palette)))
            .collect();
        let agent_list = ListView::new()
            .grid_row(2)
            .selected_index(self.nav.agent_idx)
            .automation_name("Agents")
            .on_selection_changed(
                context.callback(|value| Message::Shell(ShellMsg::AgentSelected(value))),
            )
            .collection_slot(ListViewSlot::Items, agent_items);

        let counter = m3::muted(
            if self.nav.agents_loading {
                "Loading Agents…".to_owned()
            } else {
                format!(
                    "Showing {} of {} Agent(s){}",
                    self.nav.agents.len(),
                    self.nav.agents_total,
                    if self.nav.agents_next.is_some()
                        && self.nav.agents.len() < self.nav.agents_total
                    {
                        "; more exist"
                    } else {
                        ""
                    }
                )
            },
            theme::type_scale::BODY_SMALL,
        )
        .vertical_alignment(VerticalAlignment::Center)
        .text_wrapping(TextWrapping::Wrap);
        let more = m3::text_button(palette)
            .grid_column(1)
            .is_enabled(!self.nav.agents_loading && self.nav.agents.len() < self.nav.agents_total)
            .on_click(context.message(Message::Shell(ShellMsg::MoreAgentsRequested)))
            .content(TextBlock::new().text("More Agents"));
        let refresh = m3::text_button(palette)
            .grid_column(1)
            .automation_name("Refresh")
            .on_click(context.message(Message::Shell(ShellMsg::RefreshRequested)))
            .content(m3::icon(m3::glyph::REFRESH));

        Grid::new()
            .rows([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
            ])
            .row_spacing(theme::spacing::MD)
            .children((
                Grid::new()
                    .grid_row(0)
                    .columns([GridLength::STAR, GridLength::Auto])
                    .children((
                        m3::title("Agents", theme::type_scale::TITLE_LARGE)
                            .vertical_alignment(VerticalAlignment::Center),
                        refresh,
                    )),
                project_picker,
                agent_list,
                Grid::new()
                    .grid_row(3)
                    .columns([GridLength::STAR, GridLength::Auto])
                    .children((counter, more)),
            ))
    }
}

/// One Agent row: its project caption when it opens a group, the indent of
/// its level, an expand chevron when it has children, identity and state.
fn nav_row(row: &Row, context: &mut ViewContext<DesktopApp>, palette: Palette) -> View {
    let agent = &row.agent;
    // ListViewItem exposes no content alignment in Reactor 0.100.0, so the
    // row takes the list's fixed inner width to right-align the chip.
    let row_width = theme::layout::LIST_WIDTH - 2.0 * theme::spacing::LG - 40.0;
    let indent = row.depth as f64 * TREE_INDENT;
    let chevron: View = if row.expandable() {
        let (glyph, name) = if row.expanded {
            (
                m3::glyph::CHEVRON_DOWN,
                format!("Collapse children of {}", agent.id),
            )
        } else {
            (
                m3::glyph::CHEVRON_RIGHT,
                format!("Expand {} child(ren) of {}", row.children, agent.id),
            )
        };
        m3::icon_button(palette)
            .width(CHEVRON_BUTTON.0)
            .height(CHEVRON_BUTTON.1)
            .horizontal_alignment(HorizontalAlignment::Center)
            .vertical_alignment(VerticalAlignment::Center)
            .automation_name(name)
            .on_click(context.message(Message::Shell(ShellMsg::ExpandToggled(
                agent.id.clone(),
                !row.expanded,
            ))))
            .content(m3::icon(glyph))
    } else {
        View::empty()
    };
    let mut detail = format!("{} · gen {}", agent.harness, agent.generation);
    match (&row.note, row.expanded, row.children) {
        (Some(note), _, _) => detail.push_str(&format!(" · {note}")),
        (None, false, children) if children > 0 => {
            detail.push_str(&format!(" · {children} child(ren)"));
        }
        _ => {}
    }
    // Every row reserves the chevron column, leaf or not, and its level
    // indent sits before it, so names of one level align and each level
    // starts one indent further in; the name never shares the chevron's
    // column or its focus visual.
    let main = Grid::new()
        .grid_row(1)
        .margin(Thickness::new(indent, 0.0, 0.0, 0.0))
        .columns([
            GridLength::Pixel(CHEVRON_COLUMN),
            GridLength::STAR,
            GridLength::Auto,
        ])
        .column_spacing(theme::spacing::SM)
        .children((
            chevron,
            StackPanel::new()
                .grid_column(1)
                .spacing(2.0)
                .vertical_alignment(VerticalAlignment::Center)
                .children((
                    m3::text(agent.id.clone(), theme::type_scale::TITLE_SMALL),
                    m3::muted(detail, theme::type_scale::BODY_SMALL)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                )),
            Border::new()
                .grid_column(2)
                .content(m3::status_chip(palette, &agent.state)),
        ));
    let caption: View = match &row.group {
        Some(project) => m3::text(project.clone(), theme::type_scale::LABEL_MEDIUM)
            .foreground(palette.primary())
            .automation_heading_level(AutomationHeadingLevel::Level3)
            .margin(Thickness::new(
                0.0,
                theme::spacing::XS,
                0.0,
                theme::spacing::XS,
            ))
            .into(),
        None => View::empty(),
    };
    let mut name = format!(
        "Agent {}, {}, {}, generation {}",
        agent.id, agent.state, agent.harness, agent.generation
    );
    if let Some(project) = &row.group {
        name = format!("{project}: {name}");
    }
    if row.depth > 0 {
        name.push_str(&format!(", level {}", row.depth + 1));
    }
    ListViewItem::new().automation_name(name).content(
        Grid::new()
            .width(row_width)
            .rows([GridLength::Auto, GridLength::Auto])
            .margin(Thickness::xy(0.0, theme::spacing::SM))
            .children((caption, main)),
    )
}

/// Icon over label, with the M3 indicator pill behind the icon when given.
fn rail_label(glyph: &str, label: &str, selected: bool, indicator: Option<Brush>) -> View {
    StackPanel::new()
        .spacing(theme::spacing::XS)
        .horizontal_alignment(HorizontalAlignment::Center)
        .children((
            Border::new()
                .width(56.0)
                .height(32.0)
                .corner_radius(16.0)
                .background(indicator.unwrap_or(Brush::Solid(Color::transparent())))
                .content(
                    m3::icon(glyph)
                        .horizontal_alignment(HorizontalAlignment::Center)
                        .vertical_alignment(VerticalAlignment::Center),
                ),
            m3::text(
                label,
                if selected {
                    theme::type_scale::LABEL_MEDIUM
                } else {
                    theme::type_scale::BODY_SMALL
                },
            )
            .horizontal_alignment(HorizontalAlignment::Center),
        ))
}
