use super::*;

impl DesktopApp {
    /// One "…" menu entry, bound to the Agent and generation it was built for.
    pub(super) fn agent_menu(
        &mut self,
        label: &str,
        binding: MenuBinding,
        context: &ComponentContext<Self>,
    ) {
        let bound = BoundAction::agent_control(&binding.agent, binding.generation);
        if let Some(mut agent) = self.find_agent(&binding.agent).cloned() {
            agent.generation = binding.generation;
            let target = menu_ui::Target::Agent(agent);
            if let Some((action, _, _, _)) = self
                .menu_entries(&target)
                .into_iter()
                .chain(self.developer_entries(&target))
                .find(|(_, name, _, _)| name == label)
            {
                self.menu_action(target, action, context);
                return;
            }
        }

        match label {
            MENU_DETAILS => self.show_details(&binding.agent, context),
            MENU_MOVE => {
                // Bound to the generation the header shows.
                let agent = self.find_agent(&binding.agent).cloned().or_else(|| {
                    self.chat
                        .detail
                        .as_ref()
                        .filter(|detail| detail.generation == binding.generation)
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
                });
                if let Some(mut agent) = agent {
                    agent.generation = binding.generation;
                    self.move_update(super::move_ui::MoveMsg::Open(agent), context);
                }
            }
            MENU_HANDOFF => self.handoff_update(
                handoff_ui::HandoffMsg::Open {
                    agent: binding.agent,
                    generation: binding.generation,
                },
                context,
            ),
            MENU_RESUME => {
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
            MENU_DETACH => {
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
            MENU_STOP => self.stop_agent(&bound.agent, bound.generation, context),
            MENU_FORCE => {
                let title = self.agent_title(&binding.agent);
                self.chat.confirm_force = Some((bound, title));
            }
            _ => {}
        }
    }

    /// The conversation's header ([`Header`]): the window reads what it
    /// says (the title, the state with what runs the Agent and the live
    /// feed's note, the "…" entries); the header draws it, and a window
    /// render that changes none of it leaves the header as it is.
    pub(super) fn header_view(
        &self,
        agent: &str,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let title = self.agent_title(agent);
        let detail = self.chat.detail.as_ref();
        // While its detail is read, the sidebar row's state and model stand
        // in, so opening a task never flashes "Unknown".
        let row = self.find_agent(agent);
        let known = detail
            .map(|detail| detail.state)
            .or(row.map(|row| row.state));
        let tone = known.map_or(StatusTone::Neutral, StatusTone::of_state);
        let state = known.unwrap_or(eliot_core::AgentState::Unknown);
        let note = match &self.chat.live {
            LiveStatus::Stale(_) => "Catching up…",
            LiveStatus::Reconnecting { .. } => "Reconnecting…",
            LiveStatus::Stopped(_) => "Live updates stopped",
            _ => "",
        };
        // The harness and the model the daemon read back (`effective.model`).
        let runs = detail
            .map(|detail| {
                crate::picker::harness_model_label(
                    &detail.harness,
                    detail.settings.effective_model.as_deref(),
                )
            })
            .or_else(|| {
                row.map(|row| {
                    crate::picker::harness_model_label(&row.harness, row.model.as_deref())
                })
            });
        let menu = detail.map(|detail| {
            let ended = matches!(
                detail.state,
                eliot_core::AgentState::Stopped
                    | eliot_core::AgentState::Failed
                    | eliot_core::AgentState::Lost
            );
            let controllable = !detail.observe_only;
            let task = self
                .find_agent(agent)
                .map(|agent| menu_ui::Target::Agent(agent.clone()));
            // The row menu's entries, its destructive one last (after More).
            let (regular, destructive): (Vec<_>, Vec<_>) = task
                .as_ref()
                .map(|target| self.menu_entries(target))
                .unwrap_or_default()
                .into_iter()
                .filter(|(action, _, _, _)| *action != menu_ui::Action::More)
                .partition(|(_, _, _, destructive)| !destructive);
            let item = |(_, label, _, _): (menu_ui::Action, String, &str, bool)| {
                MenuItem::item(label.clone(), label)
            };
            let mut items: Vec<MenuItem> = regular.into_iter().map(item).collect();
            let mut more = vec![MenuItem::item("details", MENU_DETAILS)];
            if !self.submitting() {
                more.push(MenuItem::item("handoff", MENU_HANDOFF));
                if detail.observe_only {
                    more.push(MenuItem::item("detach", MENU_DETACH));
                }
                if controllable && !ended {
                    more.push(MenuItem::item("force", MENU_FORCE));
                }
            }
            if let Some(target) = &task {
                more.extend(
                    self.developer_entries(target)
                        .into_iter()
                        .map(|(_, label, _, _)| MenuItem::item(label.clone(), label)),
                );
            }
            items.push(MenuItem::submenu("more", "More", more));
            items.extend(destructive.into_iter().map(item));
            items
        });
        // The status pill: the state in words, then what runs it, and the
        // live feed's note when it is not following.
        let mut pill = match state {
            eliot_core::AgentState::Busy => "Working",
            eliot_core::AgentState::Idle => "Idle",
            eliot_core::AgentState::Waiting => "Waiting for you",
            eliot_core::AgentState::Created | eliot_core::AgentState::Starting => "Starting",
            eliot_core::AgentState::Stopped => "Stopped",
            eliot_core::AgentState::Failed => "Failed",
            eliot_core::AgentState::Lost => "Lost",
            eliot_core::AgentState::Unknown => "Unknown",
        }
        .to_owned();
        if let Some(runs) = &runs {
            pill.push_str(&format!(" · {runs}"));
        }
        if !note.is_empty() {
            pill.push_str(&format!(" · {note}"));
        }
        // Stop Agent stays in view while the Agent can still be stopped,
        // idle included; it reads Stopping… until its command settles.
        let stop = detail
            .filter(|detail| {
                !detail.observe_only
                    && !matches!(
                        detail.state,
                        eliot_core::AgentState::Stopped
                            | eliot_core::AgentState::Failed
                            | eliot_core::AgentState::Lost
                    )
            })
            .and_then(|detail| self.stop_button(agent, detail.generation));
        let actions = if stop.is_some() { STOP_WIDTH } else { 0.0 };
        View::component::<Header>(HeaderInput {
            palette,
            title,
            tone,
            pill,
            menu,
            stop,
            title_width: self
                .main_width()
                .map_or(tokens::layout::READING_WIDTH, |main| {
                    title_width(main - actions)
                }),
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
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        Border::new()
            .grid_row(4)
            .width(column)
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
            .background(palette.attention_container())
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
                                m3::section_title(format!("Force stop {title}?"))
                                    .foreground(palette.attention())
                                    .text_trimming(TextTrimming::CharacterEllipsis),
                                m3::text(
                                    format!(
                                        "Ends Agent {} (run {}) at once instead of a clean stop; an active turn is cut off.",
                                        bound.agent, bound.generation
                                    ),
                                    tokens::text::CAPTION,
                                )
                                .foreground(palette.attention())
                                .text_wrapping(TextWrapping::Wrap),
                            )),
                        StackPanel::new()
                            .grid_column(1)
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .vertical_alignment(VerticalAlignment::Center)
                            .children((
                                m3::destructive(palette)
                                    .automation_name("Cancel force stop")
                                    .on_click(context.message(Message::Chat(
                                        ChatMsg::ForceStopAnswered(false),
                                    )))
                                    .content(m3::label("Cancel")),
                                m3::destructive(palette)
                                    .is_enabled(!self.submitting())
                                    .automation_name(format!(
                                        "Force stop {} run {}",
                                        bound.agent, bound.generation
                                    ))
                                    .on_click(context.message(Message::Chat(
                                        ChatMsg::ForceStopAnswered(true),
                                    )))
                                    .content(m3::label("Force stop")),
                            )),
                    )),
            )
    }
}

/// What the conversation's header shows, read by the window
/// ([`DesktopApp::header_view`]).
#[derive(Clone, PartialEq)]
pub(super) struct HeaderInput {
    palette: Palette,
    title: String,
    tone: StatusTone,
    /// The state in words, what runs the Agent, the live feed's note.
    pill: String,
    /// The "…" entries; `None` while the detail is read (the button is
    /// shown, not active).
    menu: Option<Vec<MenuItem>>,
    /// Stop Agent while the Agent can be stopped, as it shows now.
    stop: Option<crate::monitor_view::StopButton>,
    /// The widest the title takes ([`title_width`]).
    title_width: f64,
    send: Callback<Message>,
}

/// The room the header's Stop Agent button takes beside the other actions.
const STOP_WIDTH: f64 = 104.0;

/// The widest the header's title takes in a main area `main` DIPs wide:
/// everything but the actions and the status pill at its shortest (its
/// dot and the start of its label). The title is what the header is for,
/// so the pill's label trims first (in the star column the title leaves),
/// then the title.
fn title_width(main: f64) -> f64 {
    let chrome = tokens::spacing::LG + tokens::spacing::MD + 2.0 * tokens::spacing::ROW;
    let pill_chrome =
        tokens::layout::BADGE_DOT + tokens::spacing::CHIP_GAP + 2.0 * tokens::spacing::ROW;
    let actions = 2.0 * tokens::height::CONTROL + tokens::spacing::ROW_GAP;
    (main - chrome - pill_chrome - actions - tokens::layout::HEADER_STATUS_MIN_WIDTH).clamp(
        tokens::layout::HEADER_TITLE_MIN_WIDTH,
        tokens::layout::READING_WIDTH,
    )
}

/// One line: the title, the status pill, then the details toggle and the
/// "…" menu, as its own component. Its controls go to the window.
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
        let _ = self.send.call(message);
    }

    fn view(&self, input: &HeaderInput, context: &mut ViewContext<Self>) -> View {
        let palette = input.palette;
        let menu: View = match &input.menu {
            Some(items) => m3::icon_button(palette)
                .automation_name("More actions")
                .content(m3::icon(m3::glyph::MORE))
                .menu(Menu::new(
                    items.clone(),
                    // Capture-free, so the callback keeps one identity
                    // across renders: Reactor 0.100.0 re-sends a menu
                    // whose callback changed, replacing the native
                    // MenuFlyout and dropping a click on the open one.
                    context.callback(|label: String| Message::Chat(ChatMsg::AgentMenu(label))),
                )),
            None => m3::icon_button(palette)
                .is_enabled(false)
                .automation_name("More actions")
                .content(m3::icon(m3::glyph::MORE)),
        };
        let stop: View = match input.stop {
            Some(state) => {
                use crate::monitor_view::StopButton;
                let label = if state == StopButton::Stopping {
                    "Stopping…"
                } else {
                    "Stop Agent"
                };
                m3::destructive(palette)
                    .is_enabled(state == StopButton::Ready)
                    .automation_name(label)
                    .on_click(
                        context.message(Message::Chat(ChatMsg::AgentMenu(MENU_STOP.to_owned()))),
                    )
                    .content(m3::label(label))
            }
            None => View::empty(),
        };
        // The design's header: a full-width 52-DIP bar on a hairline, the
        // title at 15/500, the status pill right after it, then Details and
        // More. The actions take their own column. In the left-aligned
        // grid the title keeps its width (up to `title_width`) and the
        // pill's star column hugs a short label and trims a long one to the
        // room the title leaves: a narrow window trims the pill's label
        // first, then the title, never the actions.
        Border::new()
            .grid_row(0)
            .height(tokens::height::HEADER)
            .border_brush(palette.outline())
            .border_thickness(Thickness::new(0.0, 0.0, 0.0, tokens::layout::HAIRLINE))
            .padding(Thickness::new(
                tokens::spacing::LG,
                0.0,
                tokens::spacing::MD,
                0.0,
            ))
            .content(
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .column_spacing(tokens::spacing::ROW)
                    .children((
                        Grid::new()
                            .columns([GridLength::Auto, GridLength::STAR])
                            .column_spacing(tokens::spacing::ROW)
                            .horizontal_alignment(HorizontalAlignment::Left)
                            .children((
                                m3::section_title(input.title.clone())
                                    .max_width(input.title_width)
                                    .vertical_alignment(VerticalAlignment::Center)
                                    .text_trimming(TextTrimming::CharacterEllipsis)
                                    .is_text_selection_enabled(true),
                                Border::new()
                                    .grid_column(1)
                                    .vertical_alignment(VerticalAlignment::Center)
                                    .content(m3::status_pill(
                                        palette,
                                        input.tone,
                                        &input.pill,
                                        tokens::layout::READING_WIDTH,
                                    )),
                            )),
                        // Details and More, 2 DIPs apart at the right end.
                        StackPanel::new()
                            .grid_column(1)
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::ROW_GAP)
                            .vertical_alignment(VerticalAlignment::Center)
                            .children((
                                stop,
                                // Opens (or keeps) the Details tab; only the
                                // pane's X closes the pane.
                                m3::icon_button(palette)
                                    .automation_name("Open details")
                                    .on_click(context.message(Message::Monitor(
                                        MonitorMsg::PanelChosen(Panel::Details),
                                    )))
                                    .content(m3::icon(m3::glyph::INFO)),
                                menu,
                            )),
                    )),
            )
    }
}
