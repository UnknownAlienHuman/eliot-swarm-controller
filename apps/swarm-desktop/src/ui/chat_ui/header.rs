use super::*;
use menu_ui::Action;

impl DesktopApp {
    /// One "…" menu entry, bound to the Agent and generation it was built
    /// for: the row menu's action when the row menu offers it, else the
    /// header's own.
    pub(super) fn agent_menu(
        &mut self,
        action: Action,
        binding: MenuBinding,
        context: &ComponentContext<Self>,
    ) {
        let bound = BoundAction::agent_control(&binding.agent, binding.generation);
        if let Some(mut agent) = self.find_agent(&binding.agent).cloned() {
            agent.generation = binding.generation;
            let target = menu_ui::Target::Agent(agent);
            if menu_ui::offers(&self.menu_entries(&target), action) {
                self.menu_action(target, action, context);
                return;
            }
        }

        match action {
            Action::Details => self.show_details(&binding.agent, context),
            // No sidebar row to stop through: the same question, bound to
            // the generation the header shows.
            Action::Stop => {
                if let Some(agent) = self.header_agent(&binding) {
                    self.menu.confirm = Some((menu_ui::Target::Agent(agent), Action::Stop));
                }
            }
            Action::MoveUnder => {
                if let Some(agent) = self.header_agent(&binding) {
                    self.move_update(super::move_ui::MoveMsg::Open(agent), context);
                }
            }
            Action::Handoff => self.handoff_update(
                handoff_ui::HandoffMsg::Open {
                    agent: binding.agent,
                    generation: binding.generation,
                },
                context,
            ),
            Action::Resume => {
                let key = format!("resume:{}:{}", bound.agent, bound.generation);
                self.submit_control(context, "Resume", key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_resume(
                        data_dir,
                        mutation_id,
                        &bound.agent,
                        bound.generation,
                        None,
                    ))
                    .map(|reply| submit_note(&reply))
                });
            }
            Action::DetachObserver => {
                let key = format!("detach:{}:{}", bound.agent, bound.generation);
                self.submit_control(context, "Detach", key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_detach(
                        data_dir,
                        mutation_id,
                        &bound.agent,
                        bound.generation,
                    ))
                    .map(|reply| submit_note(&reply))
                });
            }
            Action::ForceStop => {
                let title = self.agent_title(&binding.agent);
                self.chat.confirm_force = Some((bound, title));
            }
            Action::CopyBranch => {
                let branch = self
                    .chat
                    .detail
                    .as_ref()
                    .filter(|detail| {
                        detail.agent == binding.agent && detail.generation == binding.generation
                    })
                    .and_then(|detail| detail.branch.clone());
                match branch.map(|branch| (crate::clipboard::set_text(&branch), branch)) {
                    Some((Ok(()), branch)) => self.set_status(format!("Copied {branch}.")),
                    Some((Err(error), _)) => self.alert(format!("Copy failed: {error}")),
                    None => self.alert("This task has no branch."),
                }
            }
            _ => {}
        }
    }

    /// The open Agent as a row bound to the generation the header shows:
    /// its sidebar row, else built from its readback of that generation.
    fn header_agent(&self, binding: &MenuBinding) -> Option<UiAgent> {
        let mut agent = self.find_agent(&binding.agent).cloned().or_else(|| {
            self.chat
                .detail
                .as_ref()
                .filter(|detail| {
                    detail.agent == binding.agent && detail.generation == binding.generation
                })
                .map(|detail| UiAgent {
                    id: binding.agent.clone(),
                    generation: binding.generation,
                    harness: detail.harness.clone(),
                    state: detail.state,
                    status_line: None,
                    project_id: detail.project.clone(),
                    project_scope: detail.project_scope.clone(),
                    repo_common_dir: None,
                    workspace: detail.workspace.clone(),
                    parent: detail.parent.clone(),
                    updated_at: 0,
                    title: self.agent_title(&binding.agent),
                    native_child: detail.observe_only,
                    tokens: None,
                    chat: false,
                    archived: false,
                    pending: false,
                    organization: None,
                    tags: Vec::new(),
                    organization_revision: None,
                    model: detail.settings.effective_model.clone(),
                    work: None,
                })
        })?;
        agent.generation = binding.generation;
        Some(agent)
    }

    /// The header's "…" entries, in the skeleton's order (5.2): Rename,
    /// Tags…, Attach to project… or Detach from project, Hand off or
    /// switch model…, Move under another task…, Resume, Stop Agent, Force
    /// stop…, Archive or Unarchive, Delete…, Copy Agent ID, Open folder,
    /// Copy branch name. Each is offered when its action applies; while an
    /// action settles, the ones that submit are left out.
    fn header_menu(&self, agent: &str, detail: &UiDetail) -> Vec<MenuItem> {
        let ended = matches!(
            detail.state,
            eliot_core::AgentState::Stopped
                | eliot_core::AgentState::Failed
                | eliot_core::AgentState::Lost
        );
        let row = self.find_agent(agent).map(|agent| {
            let target = menu_ui::Target::Agent(agent.clone());
            let mut actions = Vec::new();
            for entry in self.menu_entries(&target) {
                match entry {
                    menu_ui::Entry::Item { action, .. } => actions.push(action),
                    menu_ui::Entry::Submenu { items, .. } => {
                        actions.extend(items.into_iter().map(|item| item.action()));
                    }
                }
            }
            actions
        });
        let row = row.unwrap_or_default();
        let busy = self.submitting();
        let mut entries = Vec::new();
        for action in [
            Action::Rename,
            Action::EditTags,
            Action::AttachMenu,
            Action::Detach,
        ] {
            if row.contains(&action) {
                entries.push(action);
            }
        }
        if !busy {
            entries.push(Action::Handoff);
        }
        if row.contains(&Action::MoveUnder) {
            entries.push(Action::MoveUnder);
        }
        if !busy && row.contains(&Action::Resume) {
            entries.push(Action::Resume);
        }
        let controllable = !detail.observe_only && !ended;
        if !busy && controllable && self.stop_button(agent, detail.generation).is_some() {
            entries.push(Action::Stop);
            entries.push(Action::ForceStop);
        }
        if !busy && detail.observe_only {
            entries.push(Action::DetachObserver);
        }
        for action in [Action::Archive, Action::Unarchive, Action::Delete] {
            if row.contains(&action) {
                entries.push(action);
            }
        }
        entries.push(Action::CopyId);
        if row.contains(&Action::OpenFolder) {
            entries.push(Action::OpenFolder);
        }
        if detail.branch.is_some() {
            entries.push(Action::CopyBranch);
        }
        entries
            .into_iter()
            .map(|action| MenuItem::item(action.label(), action.label()))
            .collect()
    }

    /// The conversation's header ([`Header`]): the window reads what it
    /// says (the title, project and branch, the state, the goal, the pane
    /// toggles, the "…" entries); the header draws it, and a window render
    /// that changes none of it leaves the header as it is.
    pub(super) fn header_view(&self, agent: &str, context: &mut ViewContext<Self>) -> View {
        let title = self.agent_title(agent);
        let detail = self.bound_detail().ok();
        // While its detail is read, the sidebar row's state stands in, so
        // opening a task never flashes "Unknown".
        let row = self.find_agent(agent);
        let state = detail
            .map(|detail| detail.state)
            .or(row.map(|row| row.state))
            .unwrap_or(eliot_core::AgentState::Unknown);
        let held = detail
            .and_then(|detail| detail.provider_hold.as_ref())
            .is_some_and(|hold| hold.state == eliot_core::ProviderHoldState::Held);
        let pill = match state {
            // A hold shows on a task that is waiting for its provider; an
            // ended one says it ended.
            eliot_core::AgentState::Idle
            | eliot_core::AgentState::Waiting
            | eliot_core::AgentState::Created
            | eliot_core::AgentState::Starting
                if held =>
            {
                Pill::Hold
            }
            eliot_core::AgentState::Busy => Pill::Working("Working"),
            eliot_core::AgentState::Created | eliot_core::AgentState::Starting => {
                Pill::Working("Starting")
            }
            eliot_core::AgentState::Waiting => Pill::Waiting,
            eliot_core::AgentState::Idle => Pill::Idle,
            eliot_core::AgentState::Stopped => Pill::Stopped,
            eliot_core::AgentState::Failed => Pill::Failed("Failed"),
            eliot_core::AgentState::Lost => Pill::Failed("Lost"),
            eliot_core::AgentState::Unknown => Pill::Failed("Unknown"),
        };
        // Project · branch under the title.
        let workspace = detail
            .and_then(|detail| detail.workspace.clone())
            .or_else(|| row.and_then(|row| row.workspace.clone()))
            .unwrap_or_default();
        // The project's name (its registration), else the folder's.
        let project = detail
            .map(|detail| detail.project.as_str())
            .or(row.map(|row| row.project_id.as_str()))
            .and_then(|id| self.nav.projects.iter().find(|project| project.id == id));
        let place = match project {
            Some(project) => self.project_label(project),
            None => self.place_title(&workspace),
        };
        let subtitle = match detail.and_then(|detail| detail.branch.as_deref()) {
            Some(branch) => format!("{place} \u{b7} {branch}"),
            None => place,
        };
        let goal = self
            .chat
            .goal
            .as_ref()
            .filter(|(read, _)| read == agent)
            .and_then(|(_, goal)| goal.as_ref())
            .and_then(goal_words);
        let changes = row
            .and_then(|row| row.work.as_deref())
            .and_then(|work| work.insertions.zip(work.deletions));
        let workers = self
            .nav
            .agents
            .iter()
            .filter(|kid| kid.parent.as_deref() == Some(agent) && !kid.archived)
            .count();
        let menu = detail.map(|detail| self.header_menu(agent, detail));
        let running = detail
            .is_some_and(|detail| detail.active_turn.is_some() && !detail.observe_only)
            || (detail.is_none() && state == eliot_core::AgentState::Busy);
        View::component::<Header>(HeaderInput {
            title,
            subtitle,
            pill,
            goal,
            changes,
            workers,
            open: if self.panel_shown() {
                self.panel
            } else {
                Panel::Closed
            },
            menu,
            running,
            send: context.forward(),
        })
    }

    /// The Force stop question above the composer, bound to the Agent and
    /// generation of its menu click. In the window (not a ContentDialog),
    /// so it always takes the window's theme.
    pub(super) fn force_stop_banner(
        &self,
        bound: &BoundAction,
        title: &str,
        context: &mut ViewContext<Self>,
    ) -> View {
        Border::new()
            .grid_row(4)
            .column(tokens::layout::READING_WIDTH, tokens::spacing::SM, 0.0)
            .background(ThemeBrush::SystemCriticalBackground)
            .corner_radius(tokens::radius::CONTAINER)
            .padding(Thickness::uniform(tokens::spacing::MD))
            .content(
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .column_spacing(tokens::spacing::MD)
                    .children((
                        StackPanel::new()
                            .spacing(tokens::spacing::XS)
                            .vertical_alignment(VerticalAlignment::Center)
                            .children((
                                controls::section_title(format!("Force stop {title}?"))
                                    .foreground(ThemeBrush::SystemCritical)
                                    .text_trimming(TextTrimming::CharacterEllipsis),
                                controls::text(
                                    format!(
                                        "Ends Agent {} (run {}) at once instead of a clean stop; an active turn is cut off.",
                                        bound.agent, bound.generation
                                    ),
                                    tokens::text::CAPTION,
                                )
                                .foreground(ThemeBrush::SystemCritical)
                                .text_wrapping(TextWrapping::Wrap),
                            )),
                        StackPanel::new()
                            .grid_column(1)
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .vertical_alignment(VerticalAlignment::Center)
                            .children((
                                controls::destructive()
                                    .automation_name("Cancel force stop")
                                    .on_click(context.message(Message::Chat(
                                        ChatMsg::ForceStopAnswered(false),
                                    )))
                                    .content(controls::label("Cancel")),
                                controls::destructive()
                                    .is_enabled(!self.submitting())
                                    .automation_name(format!(
                                        "Force stop {} run {}",
                                        bound.agent, bound.generation
                                    ))
                                    .on_click(context.message(Message::Chat(
                                        ChatMsg::ForceStopAnswered(true),
                                    )))
                                    .content(controls::label("Force stop")),
                            )),
                    )),
            ).into()
    }
}

/// The goal mark's words ("Goal · round 2 of 3", "Goal blocked"); none for
/// a cleared goal.
fn goal_words(goal: &eliot_core::api::GoalView) -> Option<String> {
    use eliot_core::api::GoalStatus;
    let rounds = goal.counters.rounds;
    Some(match goal.status {
        GoalStatus::Cleared => return None,
        GoalStatus::Completed => "Goal met".to_owned(),
        GoalStatus::Blocked => "Goal blocked".to_owned(),
        GoalStatus::Paused => "Goal paused".to_owned(),
        GoalStatus::Active => match goal.definition.max_rounds {
            _ if rounds <= 0 => "Goal \u{b7} active".to_owned(),
            Some(max) => format!("Goal \u{b7} round {rounds} of {max}"),
            None => format!("Goal \u{b7} round {rounds}"),
        },
    })
}

/// The header's state pill: its mark (the loader while working, a dot while
/// waiting or idle, the hold or error icon) and its word, in a hairline
/// outline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Pill {
    Working(&'static str),
    Waiting,
    Hold,
    Idle,
    Stopped,
    Failed(&'static str),
}

impl Pill {
    fn word(self) -> &'static str {
        match self {
            Self::Working(word) | Self::Failed(word) => word,
            Self::Waiting => "Waiting for you",
            Self::Hold => "Provider hold",
            Self::Idle => "Idle",
            Self::Stopped => "Stopped",
        }
    }

    /// The pill: its mark and word, a hairline around them.
    fn view(self) -> View {
        let mark: Option<View> = match self {
            Self::Working(word) => Some(controls::row_mark(controls::RowMark::Working, word)),
            Self::Waiting => Some(controls::row_mark(
                controls::RowMark::Waiting,
                "Waiting for you",
            )),
            Self::Idle => Some(controls::row_mark(controls::RowMark::Idle, "Idle")),
            Self::Hold => Some(controls::xs_icon(controls::glyph::HOLD)),
            Self::Failed(_) => Some(controls::critical_icon(
                controls::glyph::ERROR,
                tokens::icon::CHEVRON,
            )),
            Self::Stopped => None,
        };
        Border::new()
            .min_height(tokens::height::BADGE)
            .corner_radius(tokens::radius::CONTROL)
            .border_brush(ThemeBrush::CardStroke)
            .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
            .padding(Thickness::xy(tokens::spacing::ROW, 0.0))
            .vertical_alignment(VerticalAlignment::Center)
            .automation_name(self.word())
            .content(
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::CHIP_GAP)
                    .keyed_children(crate::views::slots([
                        mark,
                        Some(
                            controls::text(self.word(), tokens::text::LABEL)
                                .vertical_alignment(VerticalAlignment::Center)
                                .into(),
                        ),
                    ])),
            )
            .into()
    }
}

/// What the conversation's header shows, read by the window
/// ([`DesktopApp::header_view`]).
#[derive(Clone, PartialEq)]
pub(super) struct HeaderInput {
    title: String,
    /// "project · branch".
    subtitle: String,
    pill: Pill,
    /// The goal mark's words, when the Agent has a goal.
    goal: Option<String>,
    /// The worktree's `+N −M`, when read.
    changes: Option<(u64, u64)>,
    /// The task's workers.
    workers: usize,
    /// The right pane's open tab (`Closed`: no pane).
    open: Panel,
    /// The "…" entries; `None` while the detail is read (the button is
    /// shown, not active).
    menu: Option<Vec<MenuItem>>,
    /// A turn runs: the progress bar under the header.
    running: bool,
    send: Callback<Message>,
}

/// The header (`ConversationHeader`), as its own component: the title over
/// project · branch, the state pill, the goal mark, the pane toggles
/// (Changes, Agents, Details, Terminal) and "…", 52 DIPs on a hairline;
/// under it the progress bar while a turn runs. Its controls go to the window.
pub(super) struct Header {
    send: Callback<Message>,
}

impl Component for Header {
    type Input = HeaderInput;
    type Message = Message;

    fn create(input: &HeaderInput, _context: &ComponentContext<Self>) -> Self {
        Self {
            send: input.send.clone(),
        }
    }

    fn input_changed(&mut self, input: &HeaderInput, _context: &ComponentContext<Self>) {
        self.send = input.send.clone();
    }

    fn update(&mut self, message: Message, _context: &ComponentContext<Self>) {
        self.send.call(message);
    }

    fn view(&self, input: &HeaderInput, context: &mut ViewContext<Self>) -> View {
        let more: View = match &input.menu {
            Some(items) => controls::icon_button()
                .automation_name("More task actions")
                .content(controls::icon(controls::glyph::MORE))
                .menu(Menu::new(
                    items.clone(),
                    // Capture-free, so the callback keeps one identity
                    // across renders: Reactor 0.100.0 re-sends a menu
                    // whose callback changed, replacing the native
                    // MenuFlyout and dropping a click on the open one.
                    // Reactor reports the chosen item's label only.
                    crate::views::menu_keys(context.callback(|label: String| {
                        Action::from_label(&label)
                            .map_or(Message::Menu(menu_ui::MenuMsg::Ignored), |action| {
                                Message::Chat(ChatMsg::AgentMenu(action))
                            })
                    })),
                )),
            None => controls::icon_button()
                .is_enabled(false)
                .automation_name("More task actions")
                .content(controls::icon(controls::glyph::MORE))
                .into(),
        };
        let goal: Option<View> = input.goal.as_ref().map(|words| {
            controls::status_chip_button()
                .automation_name(format!("{words}: open Details"))
                .on_click(
                    context.message(Message::Monitor(MonitorMsg::PanelChosen(Panel::Details))),
                )
                .content(controls::chip_content(
                    Some(controls::glyph::GOAL),
                    words,
                    false,
                ))
                .into()
        });
        // The pane toggles (component 3): pressed = that tab of the right
        // pane is open; the pressed one closes the pane. Changes and
        // "Agents N" are labelled buttons (accent when pressed), Agents
        // without workers, Details and Terminal icon buttons.
        let toggle = |panel: Panel| -> Callback<()> {
            if input.open == panel {
                context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Closed)))
            } else {
                context.message(Message::Shell(ShellMsg::PanelToggled(panel)))
            }
        };
        let named = |panel: Panel, words: &str| -> String {
            if input.open == panel {
                format!("{words} (open)")
            } else {
                words.to_owned()
            }
        };
        // An open pane's toggle is the accent button.
        let labelled = controls::segment;
        let changes: View = {
            let pressed = input.open == Panel::Changes;
            let counts: Option<View> = input
                .changes
                .map(|(add, del)| controls::diff_counts(add, del));
            labelled(pressed)
                .automation_name(named(Panel::Changes, "Changes"))
                .on_click(toggle(Panel::Changes))
                .content(
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .margin(Thickness::xy(
                            tokens::spacing::INSET - tokens::spacing::ROW,
                            0.0,
                        ))
                        .keyed_children(crate::views::slots([
                            Some(
                                controls::text("Changes", tokens::text::BUTTON)
                                    .vertical_alignment(VerticalAlignment::Center)
                                    .into(),
                            ),
                            counts,
                        ])),
                )
                .tooltip("Changes")
        };
        let icon_toggle = |panel: Panel, glyph: &str, words: &str| -> View {
            let pressed = input.open == panel;
            controls::toggle_icon_button(pressed)
                .automation_name(named(panel, words))
                .on_click(toggle(panel))
                .content(controls::icon(glyph))
                .tooltip(words)
        };
        let agents: View = if input.workers > 0 {
            let pressed = input.open == Panel::Agents;
            let words = format!("Agents {}", input.workers);
            labelled(pressed)
                .automation_name(named(Panel::Agents, &words))
                .on_click(toggle(Panel::Agents))
                .content(
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .margin(Thickness::xy(
                            tokens::spacing::INSET - tokens::spacing::ROW,
                            0.0,
                        ))
                        .children((
                            controls::small_icon(controls::glyph::HUB),
                            controls::text(words.clone(), tokens::text::BUTTON)
                                .vertical_alignment(VerticalAlignment::Center),
                        )),
                )
                .tooltip("Agents")
        } else {
            icon_toggle(Panel::Agents, controls::glyph::HUB, "Agents")
        };
        let details = icon_toggle(Panel::Details, controls::glyph::INFO, "Details");
        let terminal = icon_toggle(
            Panel::Terminal,
            controls::glyph::TERMINAL,
            "Terminal (Ctrl+J)",
        );
        let bar = Border::new()
            .min_height(tokens::height::HEADER)
            .border_brush(ThemeBrush::CardStroke)
            .border_thickness(Thickness::new(0.0, 0.0, 0.0, tokens::layout::HAIRLINE))
            .padding(Thickness::new(
                tokens::spacing::MD,
                0.0,
                tokens::spacing::SM,
                0.0,
            ))
            .content(
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .column_spacing(tokens::spacing::SM)
                    .children((
                        StackPanel::new()
                            .vertical_alignment(VerticalAlignment::Center)
                            .children((
                                controls::text(input.title.clone(), tokens::text::TITLE)
                                    .automation_heading_level(AutomationHeadingLevel::Level2)
                                    .text_trimming(TextTrimming::CharacterEllipsis)
                                    .is_text_selection_enabled(true),
                                controls::secondary(input.subtitle.clone())
                                    .text_trimming(TextTrimming::CharacterEllipsis),
                            )),
                        StackPanel::new()
                            .grid_column(1)
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .vertical_alignment(VerticalAlignment::Center)
                            // Keyed by the open tab: a toggle whose pressed
                            // state changes is a new control, so its style
                            // (accent or default, on or off) is applied.
                            .keyed_children(
                                [
                                    Some(KeyedView::new("pill", input.pill.view())),
                                    goal.map(|goal| KeyedView::new("goal", goal)),
                                    Some(KeyedView::new(
                                        format!("changes:{}", input.open == Panel::Changes),
                                        changes,
                                    )),
                                    Some(KeyedView::new(
                                        format!("agents:{}", input.open == Panel::Agents),
                                        agents,
                                    )),
                                    Some(KeyedView::new(
                                        format!("details:{}", input.open == Panel::Details),
                                        details,
                                    )),
                                    Some(KeyedView::new(
                                        format!("terminal:{}", input.open == Panel::Terminal),
                                        terminal,
                                    )),
                                    Some(KeyedView::new("more", more)),
                                ]
                                .into_iter()
                                .flatten()
                                .collect::<Vec<_>>(),
                            ),
                    )),
            );
        // WinUI's indeterminate progress bar under the header while a turn
        // runs.
        let wave: Option<View> = if input.running {
            Some(
                Border::new()
                    .padding(Thickness::xy(tokens::spacing::MD, 0.0))
                    .content(
                        ProgressBar::new()
                            .is_indeterminate(true)
                            .automation_name("Turn running")
                            .horizontal_alignment(HorizontalAlignment::Stretch),
                    )
                    .into(),
            )
        } else {
            None
        };
        StackPanel::new()
            .grid_row(0)
            .keyed_children(crate::views::slots([Some(bar.into()), wave]))
            .into()
    }
}
