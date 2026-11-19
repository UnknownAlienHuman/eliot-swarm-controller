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
                    self.chat.detail.as_ref().map(|detail| UiAgent {
                        id: binding.agent.clone(),
                        generation: binding.generation,
                        harness: detail.harness.clone(),
                        state: detail.state,
                        project_id: detail.project.clone(),
                        project_scope: None,
                        repo_common_dir: None,
                        workspace: None,
                        parent: None,
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
            MENU_STOP => {
                let key = format!("stop:{}:{}:false", bound.agent, bound.generation);
                self.submit_control(context, "Stop", key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_stop(
                        data_dir,
                        mutation_id,
                        &bound.agent,
                        bound.generation,
                        false,
                    ))
                    .map(|reply| submit_note(&reply))
                });
            }
            MENU_FORCE => {
                let title = self.agent_title(&binding.agent);
                self.chat.confirm_force = Some((bound, title));
            }
            _ => {}
        }
    }

    /// One line: the title, the status dot, what runs the Agent (harness and
    /// model) with a connection note when not live, then the details toggle
    /// and the "…" menu.
    pub(super) fn header_view(
        &self,
        agent: &str,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let title = self.agent_title(agent);
        let detail = self.chat.detail.as_ref();
        let tone = detail.map_or(StatusTone::Neutral, |detail| {
            StatusTone::of_state(detail.state)
        });
        let note = match &self.chat.live {
            LiveStatus::Stale(_) => "Catching up…",
            LiveStatus::Reconnecting { .. } => "Reconnecting…",
            LiveStatus::Stopped(_) => "Live updates stopped",
            _ => "",
        };
        // The harness and the model the daemon read back (`effective.model`).
        let runs = detail.map(|detail| {
            crate::picker::harness_model_label(
                &detail.harness,
                detail.settings.effective_model.as_deref(),
            )
        });
        let runs_width = runs.as_ref().map_or(0.0, |_| {
            tokens::layout::RUNS_LABEL_WIDTH + tokens::spacing::SM
        });
        let menu: View = match detail {
            Some(detail) => {
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
                let mut items: Vec<MenuItem> = task
                    .as_ref()
                    .map(|target| {
                        self.menu_entries(target)
                            .into_iter()
                            .filter(|(action, _, _, _)| *action != menu_ui::Action::More)
                            .map(|(_, label, _, _)| MenuItem::item(label.clone(), label))
                            .collect()
                    })
                    .unwrap_or_default();
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
                m3::icon_button(palette)
                    .automation_name("More actions")
                    .content(m3::icon(m3::glyph::MORE))
                    .menu(Menu::new(
                        items,
                        // Capture-free, so the callback keeps one identity
                        // across renders: Reactor 0.100.0 re-sends a menu
                        // whose callback changed, replacing the native
                        // MenuFlyout and dropping a click on the open one.
                        context.callback(|label: String| Message::Chat(ChatMsg::AgentMenu(label))),
                    ))
            }
            None => m3::icon_button(palette)
                .is_enabled(false)
                .automation_name("More actions")
                .content(m3::icon(m3::glyph::MORE)),
        };
        let details_open = self.panel == Panel::Details;
        // The status pill: the state in words, then what runs it, and the
        // live feed's note when it is not following.
        let state = detail.map_or(eliot_core::AgentState::Unknown, |detail| detail.state);
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
        let _ = (column, runs_width);
        // The design's header: a full-width 52-DIP bar on a hairline, the
        // title at 15/500, the status pill, then Details and More.
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
                    .columns([
                        GridLength::Auto,
                        GridLength::Auto,
                        GridLength::STAR,
                        GridLength::Auto,
                        GridLength::Auto,
                    ])
                    .column_spacing(tokens::spacing::ROW)
                    .children((
                        m3::section_title(title.clone())
                            .max_width(tokens::layout::READING_WIDTH)
                            .vertical_alignment(VerticalAlignment::Center)
                            .text_trimming(TextTrimming::CharacterEllipsis)
                            .is_text_selection_enabled(true),
                        Border::new()
                            .grid_column(1)
                            .vertical_alignment(VerticalAlignment::Center)
                            .content(m3::status_pill(palette, tone, &pill)),
                        m3::icon_button(palette)
                            .grid_column(3)
                            .automation_name(if details_open {
                                "Hide details"
                            } else {
                                "Show details"
                            })
                            .on_click(
                                context.message(Message::Shell(ShellMsg::PanelToggled(
                                    Panel::Details,
                                ))),
                            )
                            .content(m3::icon(m3::glyph::INFO)),
                        Border::new().grid_column(4).content(menu),
                    )),
            )
            .into()
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
                                    tokens::text::SECONDARY,
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
