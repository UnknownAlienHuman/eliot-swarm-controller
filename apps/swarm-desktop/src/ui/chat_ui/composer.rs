use super::*;

impl DesktopApp {
    /// What Send (the button, or Enter) does now: start the task, resume a
    /// stopped Agent, steer the running turn, or send the next message.
    pub(in crate::ui) fn primary_action(&self) -> Option<Message> {
        let Some(agent) = self.selected_agent_id() else {
            return Some(Message::Launch(LaunchMsg::LaunchRequested));
        };
        let detail = self.chat.detail.as_ref()?;
        let resumable = matches!(
            detail.state,
            eliot_core::AgentState::Stopped
                | eliot_core::AgentState::Failed
                | eliot_core::AgentState::Lost
        ) && detail.active_turn.is_none()
            && !detail.observe_only;
        let action = match (&detail.active_turn, resumable) {
            (_, true) => DraftAction::Resume {
                agent,
                generation: detail.generation,
            },
            (Some(turn), false) if !detail.observe_only => {
                DraftAction::Steer(BoundAction::turn_control(&agent, detail.generation, turn))
            }
            _ => DraftAction::Send { agent },
        };
        Some(Message::Chat(ChatMsg::DraftRequested(action)))
    }

    pub(in crate::ui) fn composer_text(&self) -> String {
        match self.selected_agent_id() {
            Some(agent) => self.chat.drafts.get(&agent).to_owned(),
            None => self.launch.launch_text.clone(),
        }
    }

    /// The one composer: the context row (project and profile), the field,
    /// then the model-and-effort and access chips with the actions on the
    /// right (Interrupt only while a turn runs; Send steers it).
    pub(super) fn composer_view(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let agent = self.selected_agent_id();
        let detail = self.chat.detail.as_ref();
        let (context_row, chips): (View, View) = match (agent.as_deref(), detail) {
            (None, _) => (
                self.composer_top_row(context, palette),
                self.composer_chips(context, palette),
            ),
            (Some(_), Some(detail)) => (
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .children((
                        m3::chip_static(
                            palette,
                            m3::glyph::FOLDER,
                            &nav::project_name(&detail.project, &self.registered_projects()),
                        ),
                        m3::chip_static(
                            palette,
                            m3::glyph::PROFILE,
                            &launch_ui::launched_as(&detail.settings.profile, &detail.harness),
                        ),
                    )),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .children((
                        self.model_chip(detail, context, palette),
                        m3::chip_static(
                            palette,
                            m3::glyph::SHIELD,
                            &launch_ui::agent_access_label(&detail.settings),
                        ),
                    )),
            ),
            (Some(_), None) => (View::empty(), View::empty()),
        };
        let placeholder = if agent.is_some() {
            "Send a follow-up"
        } else {
            "Describe a task…"
        };
        let field = TextBox::new()
            .grid_row(1)
            .element_ref(&self.chat.composer_ref)
            .text(self.primed(&self.composer_text()).to_owned())
            .placeholder_text(placeholder)
            .automation_name("Message")
            // Its text starts where the chips' icons do: the chips' content
            // inset less WinUI's own left text padding.
            .margin(Thickness::new(
                tokens::spacing::INSET - WINUI_TEXT_PADDING,
                0.0,
                0.0,
                0.0,
            ))
            .background(Brush::Solid(Color::transparent()))
            .border_thickness(Thickness::uniform(0.0))
            .accepts_return(true)
            .text_wrapping(TextWrapping::Wrap)
            .min_height(tokens::height::CONTROL)
            .max_height(tokens::layout::COMPOSER_MAX_HEIGHT)
            .on_text_changed(
                context.callback(|value| Message::Chat(ChatMsg::ComposerChanged(value))),
            );
        let actions = self.composer_actions(agent, context, palette);
        Border::new()
            .grid_row(6)
            .width(column)
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .margin(Thickness::new(
                0.0,
                tokens::spacing::SM,
                0.0,
                tokens::spacing::LG,
            ))
            // The composer: surface-container-high, its own 16-DIP corner.
            .background(palette.raised())
            .corner_radius(tokens::radius::COMPOSER)
            .padding(Thickness::uniform(tokens::spacing::SM))
            .content(
                Grid::new()
                    .rows([GridLength::Auto, GridLength::Auto, GridLength::Auto])
                    .row_spacing(tokens::spacing::SM)
                    .children((
                        context_row,
                        field,
                        Grid::new()
                            .grid_row(2)
                            .columns([GridLength::STAR, GridLength::Auto])
                            .column_spacing(tokens::spacing::SM)
                            .children((
                                Border::new()
                                    .vertical_alignment(VerticalAlignment::Center)
                                    .content(chips),
                                Border::new().grid_column(1).content(actions),
                            )),
                    )),
            )
    }

    /// Registered (checkout, id) pairs for project names.
    pub(in crate::ui) fn registered_projects(&self) -> Vec<(String, String)> {
        self.nav
            .projects
            .iter()
            .map(|project| (project.workspace.clone(), self.project_label(project)))
            .collect()
    }

    /// The model-and-effort chip of a running Agent: the next turn's values;
    /// its menu configures either one for the next turn.
    fn model_chip(
        &self,
        detail: &UiDetail,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let label = format!(
            "{} · {}",
            detail.settings.next_model().unwrap_or("default model"),
            detail.settings.next_effort().unwrap_or("default effort")
        );
        let agent = self.selected_agent_id().unwrap_or_default();
        let catalog = self
            .configure
            .catalog
            .as_ref()
            .filter(|catalog| catalog.is_for(&agent, detail.generation));
        let items = match catalog.map(|catalog| &catalog.reply) {
            Some(Ok(reply)) => {
                let (models, efforts) = configure::options(reply, detail.settings.next_model());
                configure::menu_items(
                    &models,
                    &efforts,
                    false,
                    reply.configure.model_refusal.as_deref(),
                    reply.configure.effort_refusal.as_deref(),
                )
            }
            Some(Err(error)) => vec![MenuItem::disabled(
                "error",
                format!("Catalogue unavailable: {error}"),
            )],
            None => vec![MenuItem::disabled("loading", "Reading the catalogue…")],
        };
        if self.submitting() {
            return m3::chip_static(palette, m3::glyph::MODEL, &label);
        }
        m3::chip(palette)
            .automation_name(format!("Model and effort: {label}"))
            .content(m3::icon_label(m3::glyph::MODEL, &label))
            .menu(Menu::new(
                items,
                // Capture-free for a stable menu identity (see the "…" menu).
                context.callback(|label: String| {
                    Message::Configure(configure_ui::ConfigureMsg::Chosen(label))
                }),
            ))
    }

    /// Send (steering the running turn, or Resume, Launch), with Interrupt while a
    /// turn runs. Each is bound now, from the detail on screen.
    fn composer_actions(
        &self,
        agent: Option<String>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let detail = self.chat.detail.as_ref();
        let active_turn = detail.and_then(|detail| detail.active_turn.clone());
        let resumable = detail.is_some_and(|detail| {
            matches!(
                detail.state,
                eliot_core::AgentState::Stopped
                    | eliot_core::AgentState::Failed
                    | eliot_core::AgentState::Lost
            ) && detail.active_turn.is_none()
                && !detail.observe_only
        });
        let draft_blank = self.composer_text().trim().is_empty();
        let enabled = !self.submitting();
        let primary: View = match (agent.clone(), detail) {
            (Some(agent), Some(detail)) if resumable => m3::filled(palette)
                .is_enabled(enabled)
                .automation_name(format!(
                    "{} (run {})",
                    if draft_blank {
                        "Resume"
                    } else {
                        "Resume with message"
                    },
                    detail.generation
                ))
                .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                    DraftAction::Resume {
                        agent,
                        generation: detail.generation,
                    },
                ))))
                .content(m3::label("Resume")),
            (Some(agent), detail) => {
                let steer = detail
                    .filter(|detail| !detail.observe_only)
                    .and_then(|detail| {
                        detail
                            .active_turn
                            .as_deref()
                            .map(|turn| BoundAction::turn_control(&agent, detail.generation, turn))
                    });
                let (tip, action) = match steer {
                    Some(bound) => ("Steer the running turn", DraftAction::Steer(bound)),
                    None => ("Send", DraftAction::Send { agent }),
                };
                m3::send_button(palette)
                    .is_enabled(enabled && !draft_blank)
                    .automation_name(format!("{tip} (Enter)"))
                    .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(action))))
                    .content(m3::icon(m3::glyph::SEND))
                    .tooltip(tip)
            }
            (None, _) => m3::send_button(palette)
                .is_enabled(enabled && !draft_blank)
                .automation_name("Start task (Enter)")
                .on_click(context.message(Message::Launch(LaunchMsg::LaunchRequested)))
                .content(m3::icon(m3::glyph::SEND))
                .tooltip("Send"),
        };
        let turn_actions: View = match (agent.as_deref(), detail, active_turn.as_deref()) {
            (Some(agent), Some(detail), Some(turn)) if !detail.observe_only => {
                let bound = BoundAction::turn_control(agent, detail.generation, turn);
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::XS)
                    .children((m3::text_button(palette)
                        .is_enabled(enabled)
                        .automation_name(format!("Interrupt turn {turn}"))
                        .on_click(
                            context.message(Message::Chat(ChatMsg::InterruptRequested(bound))),
                        )
                        .content(m3::icon_label(m3::glyph::STOP, "Interrupt")),))
            }
            _ => View::empty(),
        };
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(tokens::spacing::SM)
            .children((
                self.usage_indicator(context, palette),
                turn_actions,
                primary,
            ))
    }

    /// The harness the composer talks to: the open Agent's, else the new
    /// task's choice (a preset's or Auto's pick included).
    fn composer_harness(&self) -> Option<String> {
        if self.selected_agent_id().is_some() {
            return self
                .chat
                .detail
                .as_ref()
                .map(|detail| detail.harness.clone());
        }
        match &self.launch.choice {
            crate::picker::Choice::Model(pick) => Some(pick.harness.clone()),
            _ => self.launch.profile().map(|profile| profile.harness.clone()),
        }
    }

    /// Codex's usage indicator beside Send: the tightest quota window of
    /// the composer's harness as a small ring and the share left; it opens
    /// Usage. Nothing when the harness reports no quota.
    fn usage_indicator(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let Some(harness) = self.composer_harness() else {
            return View::empty();
        };
        let Some(Ok(reply)) = self.settings.quotas.get(&harness) else {
            return View::empty();
        };
        let Some(window) = crate::quota::tightest(reply) else {
            return View::empty();
        };
        let label = crate::picker::harness_label(&harness);
        let tip = format!("{label}: {}", window.line(crate::handoff::now_ms()));
        m3::text_button(palette)
            .height(tokens::height::CONTROL)
            .automation_name(format!("Usage: {tip}"))
            .on_click(context.message(Message::Shell(ShellMsg::SettingsPage(
                settings_ui::Page::Usage,
            ))))
            .content(
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::XS)
                    .children((
                        ProgressRing::new()
                            .is_indeterminate(false)
                            .minimum(0.0)
                            .maximum(100.0)
                            .value(window.remaining as f64)
                            .width(16.0)
                            .height(16.0)
                            .vertical_alignment(VerticalAlignment::Center),
                        m3::secondary(format!("{}%", window.remaining))
                            .vertical_alignment(VerticalAlignment::Center),
                    )),
            )
            .tooltip(tip)
    }
}
