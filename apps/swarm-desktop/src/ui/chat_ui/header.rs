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
            Action::Goal => {
                self.goal_update(super::goal_ui::GoalMsg::Open(binding.agent), context);
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

    /// The header's "…" entries: the task menu of the open Agent
    /// ([`DesktopApp::task_entries`], brand book §7.4), bound to the
    /// generation the header shows. Without a sidebar row only what the
    /// readback proves is offered: no placement, tags, archive or delete.
    fn header_menu(&self, agent: &str, detail: &UiDetail) -> Vec<MenuItem> {
        let binding = MenuBinding {
            agent: agent.to_owned(),
            generation: detail.generation,
        };
        let entries = match self.find_agent(agent) {
            Some(row) => {
                let mut row = row.clone();
                row.generation = detail.generation;
                self.task_entries(&row, Some(detail))
            }
            None => self
                .header_agent(&binding)
                .map(|agent| {
                    self.task_entries(&agent, Some(detail))
                        .into_iter()
                        .filter(|entry| {
                            !matches!(
                                entry,
                                menu_ui::Entry::Item {
                                    action: Action::EditTags
                                        | Action::AttachMenu
                                        | Action::Detach
                                        | Action::Archive
                                        | Action::Unarchive
                                        | Action::Delete,
                                    ..
                                }
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
        };
        menu_ui::menu_items(&entries)
    }

    /// The conversation's header ([`Header`]): the window reads what it
    /// says (the title, project and branch, the state, the goal, the pane
    /// commands, the "…" entries); the header draws it, and a window render
    /// that changes none of it leaves the header as it is. Path, PID,
    /// generation and session id stay in Details.
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
        let shown = match state {
            // A hold shows on a task that is waiting for its provider; an
            // ended one says it ended.
            eliot_core::AgentState::Idle
            | eliot_core::AgentState::Waiting
            | eliot_core::AgentState::Created
            | eliot_core::AgentState::Starting
                if held =>
            {
                State::Hold
            }
            eliot_core::AgentState::Busy => State::Working("Working"),
            eliot_core::AgentState::Created | eliot_core::AgentState::Starting => {
                State::Working("Starting")
            }
            eliot_core::AgentState::Waiting => State::Waiting,
            eliot_core::AgentState::Idle => State::Idle,
            eliot_core::AgentState::Stopped => State::Stopped,
            eliot_core::AgentState::Failed => State::Failed("Failed"),
            eliot_core::AgentState::Lost => State::Failed("Lost"),
            eliot_core::AgentState::Unknown => State::Failed("Outcome unknown"),
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
            state: shown,
            goal,
            changes,
            workers,
            open: if self.panel_shown() {
                self.panel
            } else {
                Panel::Closed
            },
            menu,
            compact: self.narrow_main(),
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
                                // Wraps as a dialog's title does.
                                controls::section_title(format!("Force stop {title}?"))
                                    .foreground(ThemeBrush::SystemCritical)
                                    .text_wrapping(TextWrapping::Wrap),
                                controls::text(
                                    format!(
                                        "Ends Agent {} run {} at once; a running turn is cut off.",
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
                                    .on_click(
                                        context.message(Message::Chat(ChatMsg::ForceStopAnswered(
                                            false,
                                        ))),
                                    )
                                    .content(controls::label("Cancel")),
                                controls::destructive()
                                    .is_enabled(!self.submitting())
                                    .automation_name(format!(
                                        "Force stop {} run {}",
                                        bound.agent, bound.generation
                                    ))
                                    .on_click(
                                        context.message(Message::Chat(ChatMsg::ForceStopAnswered(
                                            true,
                                        ))),
                                    )
                                    .content(controls::label("Force stop")),
                            )),
                    )),
            )
            .into()
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

/// The header's readable state (§4.2): a static mark and its words.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum State {
    Working(&'static str),
    Waiting,
    Hold,
    Idle,
    Stopped,
    Failed(&'static str),
}

impl State {
    fn word(self) -> &'static str {
        match self {
            Self::Working(word) | Self::Failed(word) => word,
            Self::Waiting => "Waiting for you",
            Self::Hold => "Provider hold",
            Self::Idle => "Idle",
            Self::Stopped => "Stopped",
        }
    }

    /// The mark: the sidebar row's static mark in its slot (the hold icon
    /// for a hold); none for a stopped task, whose word says it.
    fn mark(self) -> Option<View> {
        use controls::RowMark;
        let word = self.word();
        match self {
            Self::Working(_) => Some(controls::row_mark(RowMark::Working, word)),
            Self::Waiting => Some(controls::row_mark(RowMark::Waiting, word)),
            Self::Idle => Some(controls::row_mark(RowMark::Idle, word)),
            Self::Failed(_) => Some(controls::row_mark(RowMark::Failed, word)),
            Self::Hold => Some(controls::icon(controls::glyph::HOLD)),
            Self::Stopped => None,
        }
    }
}

/// What the conversation's header shows, read by the window
/// ([`DesktopApp::header_view`]).
#[derive(Clone, PartialEq)]
pub(super) struct HeaderInput {
    title: String,
    /// "project · branch".
    subtitle: String,
    state: State,
    /// The goal's words, when the Agent has a goal.
    goal: Option<String>,
    /// The worktree's `+N −M`, when read.
    changes: Option<(u64, u64)>,
    /// The task's workers.
    workers: usize,
    /// The right pane's open tab (`Closed`: no pane).
    open: Panel,
    /// The task's "…" entries; `None` while the detail is read.
    menu: Option<Vec<MenuItem>>,
    /// The main area is at its narrowest: Agents and Details move into
    /// More, so the title keeps its room.
    compact: bool,
    /// A turn runs: the progress bar under the header.
    running: bool,
    send: Callback<Message>,
}

/// The header (`ConversationHeader`, §9.1), as its own component. Left, in
/// the star cell: the title (trimmed with "…") over one line of project ·
/// branch, the state and the goal. Right: Changes, Agents, Details and
/// More, stock buttons of one height (the pane commands are toggle buttons,
/// checked while their tab is open); when the main area is at its
/// narrowest, Agents and Details are More's first entries unless their tab
/// is open, and Terminal is in More unless its tab is open. Under it the
/// progress bar while a turn runs: the header's one activity indicator.
pub(super) struct Header {
    send: Callback<Message>,
}

/// More's entries that open a pane tab not in the bar, before the task's
/// own actions.
const OPEN_AGENTS: &str = "Agents";
const OPEN_DETAILS: &str = "Details";
const OPEN_TERMINAL: &str = "Terminal (Ctrl+J)";

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
        // A pane command: checked while its tab is open; unchecking it
        // closes the pane, checking it opens that tab.
        let toggle = |panel: Panel, words: String, context: &mut ViewContext<Self>| -> View {
            let checked = input.open == panel;
            controls::bar_toggle(checked)
                .automation_name(if checked {
                    format!("{words} (open)")
                } else {
                    words.clone()
                })
                .on_is_checked_changed(crate::views::check_changes(context.callback(
                    move |on: bool| {
                        Message::Shell(ShellMsg::PanelToggled(if on {
                            panel
                        } else {
                            Panel::Closed
                        }))
                    },
                )))
                .content(controls::label(&words))
                .into()
        };
        let changes = toggle(
            Panel::Changes,
            match input.changes {
                Some((add, del)) => format!("Changes +{add} \u{2212}{del}"),
                None => "Changes".to_owned(),
            },
            context,
        );
        let agents_words = if input.workers > 0 {
            format!("Agents {}", input.workers)
        } else {
            "Agents".to_owned()
        };
        // A secondary pane command sits in the bar while its tab is open
        // (checked, so unchecking it closes the pane) and in More otherwise:
        // Agents and Details when the bar is compact, Terminal always.
        let in_bar = |panel: Panel| -> bool {
            input.open == panel || (!input.compact && panel != Panel::Terminal)
        };
        let mut items: Vec<MenuItem> = Vec::new();
        if !in_bar(Panel::Agents) {
            items.push(MenuItem::item(OPEN_AGENTS, agents_words.clone()));
        }
        if !in_bar(Panel::Details) {
            items.push(MenuItem::item(OPEN_DETAILS, OPEN_DETAILS));
        }
        if !in_bar(Panel::Terminal) {
            items.push(MenuItem::item(OPEN_TERMINAL, OPEN_TERMINAL));
        }
        if let Some(menu) = &input.menu {
            items.push(MenuItem::separator("actions"));
            items.extend(menu.iter().cloned());
        }
        let more: View = controls::bar_button(false)
            .automation_name("More task actions")
            .content(controls::icon(controls::glyph::MORE))
            .menu(Menu::new(
                items,
                // Capture-free, so the callback keeps one identity across
                // renders: Reactor re-sends a menu whose callback changed,
                // replacing the native MenuFlyout and dropping a click on
                // the open one.
                crate::views::menu_keys(context.callback(|key: String| {
                    match key.as_str() {
                        OPEN_AGENTS => Message::Shell(ShellMsg::PanelToggled(Panel::Agents)),
                        OPEN_DETAILS => Message::Shell(ShellMsg::PanelToggled(Panel::Details)),
                        OPEN_TERMINAL => Message::Shell(ShellMsg::PanelToggled(Panel::Terminal)),
                        key => Action::from_key(key)
                            .map_or(Message::Menu(menu_ui::MenuMsg::Ignored), |action| {
                                Message::Chat(ChatMsg::AgentMenu(action))
                            }),
                    }
                })),
            ));
        let mut commands = vec![KeyedView::new("changes", changes)];
        if in_bar(Panel::Agents) {
            commands.push(KeyedView::new(
                "agents",
                toggle(Panel::Agents, agents_words, context),
            ));
        }
        if in_bar(Panel::Details) {
            commands.push(KeyedView::new(
                "details",
                toggle(Panel::Details, OPEN_DETAILS.to_owned(), context),
            ));
        }
        if in_bar(Panel::Terminal) {
            commands.push(KeyedView::new(
                "terminal",
                toggle(Panel::Terminal, "Terminal".to_owned(), context),
            ));
        }
        commands.push(KeyedView::new("more", more));
        // The line under the title: project · branch (it trims first), the
        // state and the goal, in one type size on one baseline.
        let state: View = StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(tokens::spacing::XS)
            .vertical_alignment(VerticalAlignment::Center)
            .automation_name(input.state.word())
            .keyed_children(crate::views::slots([
                input.state.mark(),
                Some(
                    controls::secondary(input.state.word())
                        .vertical_alignment(VerticalAlignment::Center)
                        .into(),
                ),
            ]))
            .into();
        let goal: Option<View> = input.goal.as_ref().map(|words| {
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::XS)
                .vertical_alignment(VerticalAlignment::Center)
                .automation_name(words.clone())
                .children((
                    controls::xs_icon(controls::glyph::GOAL),
                    controls::secondary(words.clone())
                        .vertical_alignment(VerticalAlignment::Center),
                ))
                .into()
        });
        let mut columns = vec![GridLength::STAR, GridLength::Auto];
        let mut cells = vec![
            KeyedView::new(
                "place",
                controls::secondary(input.subtitle.clone())
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_trimming(TextTrimming::CharacterEllipsis),
            ),
            KeyedView::new("state", Border::new().grid_column(1).content(state)),
        ];
        if let Some(goal) = goal {
            columns.push(GridLength::Auto);
            cells.push(KeyedView::new(
                "goal",
                Border::new().grid_column(2).content(goal),
            ));
        }
        let line = Grid::new()
            .columns(columns)
            .column_spacing(tokens::spacing::MD)
            .horizontal_alignment(HorizontalAlignment::Left)
            .keyed_children(cells);
        let bar = Border::new()
            .min_height(tokens::height::HEADER)
            .border_brush(ThemeBrush::CardStroke)
            .border_thickness(Thickness::new(0.0, 0.0, 0.0, tokens::layout::HAIRLINE))
            .padding(Thickness::xy(tokens::spacing::MD, tokens::spacing::XS))
            .content(
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .column_spacing(tokens::spacing::MD)
                    .children((
                        StackPanel::new()
                            .vertical_alignment(VerticalAlignment::Center)
                            .children((
                                controls::text(input.title.clone(), tokens::text::HEADER)
                                    .automation_heading_level(AutomationHeadingLevel::Level2)
                                    .text_trimming(TextTrimming::CharacterEllipsis)
                                    .is_text_selection_enabled(true),
                                line,
                            )),
                        StackPanel::new()
                            .grid_column(1)
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::XS)
                            .vertical_alignment(VerticalAlignment::Center)
                            .keyed_children(commands),
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
