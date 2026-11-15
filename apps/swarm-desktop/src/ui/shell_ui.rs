//! Navigation shell: projects, the Agent list, screen selection and the root layout.

use super::*;
use crate::theme::StatusTone;
use crate::theme::winui::Palette;

#[derive(Clone, Debug)]
pub(super) enum ShellMsg {
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
    /// System → Light → Dark → System.
    ThemeCycled,
    /// The re-created editors of this re-prime exist: give them their text.
    FieldsPrimed(u64),
    /// The periodic High Contrast reading.
    HighContrastRead(bool),
}

/// Reactor 0.100.0 reports the window's Light/Dark scheme only, and a High
/// Contrast theme can keep the same base scheme, so High Contrast is read
/// at the monitor's cadence.
const HIGH_CONTRAST_POLL_MS: u64 = 2_000;
/// Re-check for the empty editor render one frame later.
const PRIME_RETRY_MS: u64 = 16;

/// The owner's theme choice for this window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ThemePref {
    System,
    Light,
    Dark,
}

impl ThemePref {
    fn next(self) -> Self {
        match self {
            Self::System => Self::Light,
            Self::Light => Self::Dark,
            Self::Dark => Self::System,
        }
    }

    fn window_theme(self) -> WindowTheme {
        match self {
            Self::System => WindowTheme::System,
            Self::Light => WindowTheme::Light,
            Self::Dark => WindowTheme::Dark,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::System => "System",
            Self::Light => "Light",
            Self::Dark => "Dark",
        }
    }

    fn glyph(self) -> &'static str {
        match self {
            Self::System => m3::glyph::THEME_SYSTEM,
            Self::Light => m3::glyph::THEME_LIGHT,
            Self::Dark => m3::glyph::THEME_DARK,
        }
    }
}

/// Theme choice, the scheme the window actually shows, and High Contrast.
pub(super) struct Appearance {
    pref: ThemePref,
    dark: bool,
    high_contrast: bool,
}

impl Appearance {
    pub(super) fn new() -> Self {
        Self {
            pref: ThemePref::System,
            dark: false,
            high_contrast: crate::accessibility::high_contrast_on(),
        }
    }

    pub(super) fn palette(&self) -> theme::winui::Palette {
        theme::winui::Palette::new(self.dark, self.high_contrast)
    }
}

/// Projects, the Agent list page and the current selection.
pub(super) struct ShellState {
    pub(super) selection: Selection,
    pub(super) projects: Vec<UiProject>,
    pub(super) project_idx: Option<usize>,
    pub(super) agents: Vec<UiAgent>,
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
            ShellMsg::ProjectsLoaded(projects) => match projects {
                Ok(projects) => {
                    let count = projects.len();
                    self.nav.projects = projects;
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
                        let published = self.nav.agents.len();
                        if append {
                            self.nav.agents.extend(page.rows);
                        } else {
                            self.nav.agents = page.rows;
                        }
                        self.nav.agents_next = page.next;
                        self.nav.agents_total = page.total;
                        if let Some(wanted) = self.nav.pending_select.take()
                            && !self.nav.agents.iter().any(|agent| agent.id == wanted.id)
                        {
                            self.nav.agents.push(wanted.clone());
                            self.nav.pending_select = Some(wanted);
                        }
                        let selected = self
                            .nav
                            .pending_select
                            .take()
                            .map(|agent| agent.id)
                            .or_else(|| self.nav.selection.agent.clone());
                        let index = selected
                            .and_then(|id| self.nav.agents.iter().position(|agent| agent.id == id));
                        if let Some(agent) =
                            index.and_then(|index| self.nav.agents.get(index)).cloned()
                            && self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id)
                        {
                            self.open_conversation(&agent, context);
                        }
                        // Never point the native SelectedIndex at a row this same
                        // render adds: highlight a new row on the next tick.
                        // Reactor 0.100.0 applies the SelectedIndex before the
                        // new item reaches the ListView; WinUI rejects it with
                        // E_INVALIDARG (0x80070057), which Reactor treats as a
                        // fatal native command failure and the process aborts
                        // with 0xC0000409. A standalone repro on 2026-11-14
                        // aborted on the first same-update select and stayed up
                        // through repeated next-tick selects.
                        match index {
                            Some(index) if index >= published => {
                                self.nav.agent_idx = None;
                                let id = self.nav.agents[index].id.clone();
                                let _task = context.spawn_background(move |_| {
                                    Message::Shell(ShellMsg::HighlightAgent(id))
                                });
                            }
                            other => self.nav.agent_idx = other,
                        }
                    }
                    Err(error) => self.set_status(format!("agent.list failed: {error}")),
                }
            }
            ShellMsg::ProjectSelected(index) => {
                self.nav.project_idx = index;
                self.nav.agents.clear();
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
                if let Some(agent) = self.selected_agent().cloned()
                    && self.chat.convo.as_ref().map(Conversation::agent) != Some(&agent.id)
                {
                    self.open_conversation(&agent, context);
                }
            }
            ShellMsg::RefreshRequested => {
                if let Ok(data_dir) = self.data_dir.clone() {
                    let _task = context.spawn_background(move |_| {
                        Message::Shell(ShellMsg::ProjectsLoaded(load_projects_blocking(&data_dir)))
                    });
                }
                self.load_agents(context, None);
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
                    self.nav.agent_idx = self.nav.agents.iter().position(|agent| agent.id == id);
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
            ShellMsg::HighContrastRead(on) => {
                if on != self.appearance.high_contrast {
                    self.appearance.high_contrast = on;
                    self.reprime_fields(context);
                }
                self.watch_high_contrast(context);
            }
            ShellMsg::ThemeCycled => self.appearance.pref = self.appearance.pref.next(),
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

    pub(super) fn selected_agent(&self) -> Option<&UiAgent> {
        self.nav
            .agent_idx
            .and_then(|index| self.nav.agents.get(index))
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
            project: self.selected_project().map(|project| project.id.clone()),
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
                .client_size(theme::layout::WINDOW_WIDTH, theme::layout::WINDOW_HEIGHT)
                .theme(self.appearance.pref.window_theme()),
        );
        context.on_color_scheme(
            context.callback(|scheme| Message::Shell(ShellMsg::SchemeObserved(scheme))),
        );
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
            .content(rail_label(pref.glyph(), pref.label(), false, None));
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
                    .children((chat, profiles, automation, scripts)),
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
        let project_picker = ComboBox::new()
            .grid_row(1)
            .items_source(project_names)
            .selected_index(self.nav.project_idx)
            .placeholder_text("Project")
            .automation_name("Project")
            .horizontal_alignment(HorizontalAlignment::Stretch)
            .on_selection_changed(
                context.callback(|value| Message::Shell(ShellMsg::ProjectSelected(value))),
            );

        // ListViewItem exposes no content alignment in Reactor 0.100.0, so
        // the row takes the list's fixed inner width to right-align the chip.
        let row_width = theme::layout::LIST_WIDTH - 2.0 * theme::spacing::LG - 40.0;
        let agent_items: Vec<KeyedView> = self
            .nav
            .agents
            .iter()
            .map(|agent| {
                KeyedView::new(
                    agent.id.clone(),
                    ListViewItem::new()
                        .automation_name(format!(
                            "Agent {}, {}, {}, generation {}",
                            agent.id, agent.state, agent.harness, agent.generation
                        ))
                        .content(
                            Grid::new()
                                .width(row_width)
                                .columns([GridLength::STAR, GridLength::Auto])
                                .column_spacing(theme::spacing::SM)
                                .margin(Thickness::xy(0.0, theme::spacing::SM))
                                .children((
                                    StackPanel::new()
                                        .spacing(2.0)
                                        .vertical_alignment(VerticalAlignment::Center)
                                        .children((
                                            m3::text(
                                                agent.id.clone(),
                                                theme::type_scale::TITLE_SMALL,
                                            ),
                                            m3::muted(
                                                format!(
                                                    "{} · gen {}",
                                                    agent.harness, agent.generation
                                                ),
                                                theme::type_scale::BODY_SMALL,
                                            ),
                                        )),
                                    Border::new()
                                        .grid_column(1)
                                        .content(m3::status_chip(palette, &agent.state)),
                                )),
                        ),
                )
            })
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
