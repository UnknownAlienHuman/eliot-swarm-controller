use super::*;
use crate::configure::locked_words;
use crate::picker::{effort_label, effort_name};
use eliot_core::api::{HarnessModelsResponse, ModelsCatalogResponse};
use launch_ui::Pop;

/// The usage popover's width (Codex's usage popover).
const USAGE_WIDTH: f64 = 320.0;
/// Limits the usage popover lists (the tightest first).
const USAGE_ROWS: usize = 3;
/// The effort popover's least width (its steps set the rest).
const EFFORT_MIN_WIDTH: f64 = 300.0;

impl DesktopApp {
    /// What Send (the button, or Enter) does now: start the task, resume a
    /// stopped Agent, steer the running turn, or send the next message.
    pub(in crate::ui) fn primary_action(&self) -> Option<Message> {
        let Some(agent) = self.selected_agent_id() else {
            return Some(Message::Launch(LaunchMsg::LaunchRequested));
        };
        let detail = self.chat.detail.as_ref()?;
        let action = match (&detail.active_turn, resumable(detail)) {
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

    /// The one composer, one rounded card. A new task: the project chip as
    /// the card's first row, then the field and the model and access
    /// chips. An open Agent: its project, harness and access as plain
    /// read-only text, the field, then the model-and-effort chip. Usage,
    /// Interrupt (only while a turn runs) and Send or Resume sit at the
    /// right.
    pub(super) fn composer_view(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let agent = self.selected_agent_id();
        let detail = self.chat.detail.as_ref();
        // The first row: a new task's project chip (its own inset lines its
        // icon up with the chips below), or the open Agent's read-only
        // context (inset to where the chips' icons start).
        let (context_row, chips): (Option<View>, View) = match (agent.as_deref(), detail) {
            (None, _) => (
                Some(self.composer_project_row(context, palette)),
                self.composer_chips(context, palette),
            ),
            (Some(_), Some(detail)) => (
                Some(
                    Border::new()
                        .margin(Thickness::new(
                            tokens::spacing::INSET,
                            tokens::spacing::XS,
                            0.0,
                            0.0,
                        ))
                        .content(self.agent_context(detail, palette)),
                ),
                self.model_chip(detail, context, palette),
            ),
            (Some(_), None) => (None, View::empty()),
        };
        let placeholder = match detail.filter(|_| agent.is_some()) {
            None if agent.is_none() => "Describe a task…",
            Some(detail) if resumable(detail) => "Type a message to resume and continue…",
            _ => "Send a follow-up",
        };
        let first = usize::from(context_row.is_some());
        let field = TextBox::new()
            .element_ref(&self.chat.composer_ref)
            .text(self.primed(&self.composer_text()).to_owned())
            .placeholder_text(placeholder)
            .automation_name("Message")
            .background(Brush::Solid(Color::transparent()))
            .border_thickness(Thickness::uniform(0.0))
            .accepts_return(true)
            .text_wrapping(TextWrapping::Wrap)
            .min_height(tokens::height::CONTROL)
            .max_height(tokens::layout::COMPOSER_MAX_HEIGHT)
            .on_text_changed(
                context.callback(|value| Message::Chat(ChatMsg::ComposerChanged(value))),
            );
        // WinUI's TextBox handles its own presses and Reactor 0.100.0 has no
        // focus events: around it, only where the pointer is reaches the
        // window. The pointer hook reports a press while it is over the
        // field, which closes an open popover (`shortcuts`).
        let field = Border::new()
            .grid_row(first as i32)
            // Its text starts where the chips' icons do: the chips' content
            // inset less WinUI's own left text padding.
            .margin(Thickness::new(
                tokens::spacing::INSET - WINUI_TEXT_PADDING,
                0.0,
                0.0,
                0.0,
            ))
            .on_pointer_entered(context.callback(|info: PointerEventInfo| {
                Message::Launch(LaunchMsg::ComposerPointer {
                    over: true,
                    pressed: info.is_left_button_pressed
                        || info.is_right_button_pressed
                        || info.is_middle_button_pressed,
                })
            }))
            .on_pointer_exited(context.callback(|_: PointerEventInfo| {
                Message::Launch(LaunchMsg::ComposerPointer {
                    over: false,
                    pressed: false,
                })
            }))
            .content(field);
        let actions = self.composer_actions(agent, context, palette);
        let bottom = Grid::new()
            .grid_row(first as i32 + 1)
            .columns([GridLength::STAR, GridLength::Auto])
            .column_spacing(tokens::spacing::SM)
            .children((
                Border::new()
                    .vertical_alignment(VerticalAlignment::Center)
                    .content(chips),
                Border::new().grid_column(1).content(actions),
            ));
        // Rows only for what is shown: an empty row would still take the gap.
        let rows = vec![GridLength::Auto; first + 2];
        let card = Border::new()
            // The composer: surface-container-high, its own 16-DIP corner.
            .background(palette.raised())
            .corner_radius(tokens::radius::COMPOSER)
            .padding(Thickness::uniform(tokens::spacing::SM))
            .content(
                Grid::new()
                    .rows(rows)
                    .row_spacing(tokens::spacing::SM)
                    .children((context_row.unwrap_or_else(View::empty), field, bottom)),
            );
        Border::new()
            .grid_row(5)
            .width(column)
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .margin(Thickness::new(
                0.0,
                tokens::spacing::SM,
                0.0,
                tokens::spacing::LG,
            ))
            .content(card)
    }

    /// The open Agent's project, harness and access: read-only, so plain
    /// icon and text (no chip outline); Full access in the warning color.
    fn agent_context(&self, detail: &UiDetail, palette: Palette) -> View {
        let access = launch_ui::agent_access_label(&detail.settings);
        let access: View = if access == crate::settings::Access::Full.label() {
            m3::meta_tinted(m3::glyph::WARNING, &access, palette.warning())
        } else {
            m3::meta(m3::glyph::SHIELD, &access)
        };
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(tokens::spacing::MD)
            .children((
                m3::meta(m3::glyph::FOLDER, &self.place_title(&detail.project)),
                m3::meta(
                    m3::glyph::PROFILE,
                    &launch_ui::launched_as(&detail.settings.profile, &detail.harness),
                ),
                access,
            ))
    }

    /// The open Agent's model-and-effort chip: the next turn's values; it
    /// opens the effort popover.
    fn model_chip(
        &self,
        detail: &UiDetail,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let model = detail.settings.next_model().unwrap_or("Default model");
        let effort = detail.settings.next_effort();
        let label = format!(
            "{model} · {}",
            effort.map_or("default effort", effort_label)
        );
        let name = format!(
            "{model} · {}",
            effort.map_or_else(|| "default effort".to_owned(), effort_name)
        );
        let open = self.launch.open == Some(Pop::Effort);
        m3::text_chip(palette)
            .automation_name(format!(
                "Model and effort: {name} ({})",
                if open { "open" } else { "change" }
            ))
            .on_click(context.message(Message::Launch(LaunchMsg::Toggle(Pop::Effort))))
            .content(
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .children((
                        m3::icon(m3::glyph::MODEL),
                        TextBlock::new()
                            .text(label)
                            .vertical_alignment(VerticalAlignment::Center)
                            .text_trimming(TextTrimming::CharacterEllipsis),
                        m3::icon(m3::glyph::CHEVRON_DOWN),
                    )),
            )
    }

    /// The open Agent and its detail when the popovers act on it (not a
    /// preset's picker in Settings).
    pub(in crate::ui) fn popover_agent(&self) -> Option<&UiDetail> {
        if self.launch.preset.is_some() || self.main != Main::Conversation {
            return None;
        }
        self.selected_agent_id()?;
        self.chat.detail.as_ref()
    }

    /// The open Agent's route catalogue, read for its exact generation.
    pub(in crate::ui) fn agent_catalog(&self) -> Option<&Result<HarnessModelsResponse, String>> {
        let agent = self.selected_agent_id()?;
        let generation = self.chat.detail.as_ref()?.generation;
        self.configure
            .catalog
            .as_ref()
            .filter(|catalog| catalog.is_for(&agent, generation))
            .map(|catalog| &catalog.reply)
    }

    /// Whether the open Agent's harness switches models mid-task (known
    /// only once its catalogue is read).
    pub(in crate::ui) fn agent_model_allowed(&self) -> bool {
        matches!(self.agent_catalog(), Some(Ok(reply)) if reply.configure.model_refusal.is_none())
    }

    /// Catalogue choices to preview while the exact route is being read.
    pub(in crate::ui) fn agent_launch_catalog(
        &self,
        harness: &str,
    ) -> Option<&Fallible<ModelsCatalogResponse>> {
        if self.agent_catalog().is_some() {
            return None;
        }
        self.launch.catalog.as_ref().filter(|read| {
            read.as_ref()
                .is_ok_and(|catalog| catalog.models.iter().any(|model| model.harness == harness))
        })
    }

    /// Confirmed effort steps of the open Agent's next model.
    pub(in crate::ui) fn agent_efforts(&self) -> Vec<String> {
        match (self.agent_catalog(), self.chat.detail.as_ref()) {
            (Some(Ok(reply)), Some(detail)) if reply.configure.effort_refusal.is_none() => {
                configure::options(reply, detail.settings.next_model()).1
            }
            _ => Vec::new(),
        }
    }

    /// Codex's model-and-effort popover for the open Agent: the effort in
    /// words and the model, the effort steps, then Change model… into the
    /// searchable picker. What the harness keeps is hidden and said in one
    /// plain line.
    pub(in crate::ui) fn effort_popover(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let Some(detail) = self.chat.detail.as_ref() else {
            return View::empty();
        };
        let harness = crate::picker::harness_label(&detail.harness);
        let read = self.agent_catalog();
        let (model_locked, effort_locked) = match read {
            Some(Ok(reply)) => (
                reply.configure.model_refusal.is_some(),
                reply.configure.effort_refusal.is_some(),
            ),
            _ => (false, false),
        };
        let current = detail.settings.next_effort();
        let shown = self.launch.effort_highlight.as_deref().or(current);
        let header = StackPanel::new().children((
            m3::section_title(shown.map_or("default effort", effort_label))
                .automation_name(shown.map_or_else(|| "default effort".to_owned(), effort_name)),
            m3::secondary(detail.settings.next_model_words()),
        ));
        let preview = self.agent_launch_catalog(&detail.harness);
        let efforts = match preview {
            Some(Ok(catalog)) => {
                configure::launch_model(catalog, &detail.harness, detail.settings.next_model())
                    .and_then(|model| model.efforts.clone())
                    .unwrap_or_default()
            }
            _ => self.agent_efforts(),
        };
        let ready = matches!(read, Some(Ok(_)));
        let enabled = ready && !self.submitting();
        let steps: View = match read {
            Some(Err(error)) => m3::secondary(format!(
                "Effort levels not read: {}",
                crate::state::plain_error(error)
            ))
            .foreground(palette.error())
            .text_wrapping(TextWrapping::Wrap)
            .into(),
            _ if effort_locked || (!ready && efforts.is_empty()) => View::empty(),
            _ => {
                if efforts.is_empty() {
                    m3::secondary("This model has no effort levels to choose.").into()
                } else {
                    let segments = efforts
                        .iter()
                        .map(|effort| {
                            let selected = shown == Some(effort.as_str());
                            KeyedView::new(
                                format!("effort:{effort}:{selected}"),
                                m3::segment(palette, selected)
                                    .is_enabled(enabled)
                                    .automation_name(format!(
                                        "Effort {}{}",
                                        effort_name(effort),
                                        if current == Some(effort.as_str()) {
                                            " (current)"
                                        } else {
                                            ""
                                        }
                                    ))
                                    .on_click(context.message(Message::Launch(
                                        LaunchMsg::EffortChosen(effort.clone()),
                                    )))
                                    .content(m3::segment_label(effort_label(effort))),
                            )
                        })
                        .collect();
                    m3::segmented(palette, segments)
                }
            }
        };
        let note = match read {
            Some(Ok(_)) => locked_words(harness, model_locked, effort_locked)
                .unwrap_or_else(|| "Applies from the next turn.".to_owned()),
            None => "Checking what this task can change…".to_owned(),
            Some(Err(_)) => String::new(),
        };
        let change_model: View = if (ready && !model_locked) || preview.is_some() {
            StackPanel::new().spacing(tokens::spacing::XS).children((
                m3::divider(palette),
                m3::row(palette, false)
                    .is_enabled(!self.submitting())
                    .automation_name("Change model")
                    .on_click(context.message(Message::Launch(LaunchMsg::Toggle(Pop::Model))))
                    .content(m3::row_content(
                        Some(m3::glyph::MODEL),
                        "Change model…",
                        "",
                        m3::icon(m3::glyph::CHEVRON_RIGHT),
                    )),
            ))
        } else {
            View::empty()
        };
        let hints: View = if effort_locked || !ready || efforts.is_empty() {
            View::empty()
        } else {
            m3::secondary("←→ effort   Enter apply   Esc close").into()
        };
        m3::popover(palette)
            .min_width(EFFORT_MIN_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Left)
            .automation_name("Model and effort")
            .content(
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .margin(Thickness::uniform(tokens::spacing::SM))
                    .children((
                        header,
                        steps,
                        if note.is_empty() {
                            View::empty()
                        } else {
                            m3::secondary(note).text_wrapping(TextWrapping::Wrap).into()
                        },
                        hints,
                        change_model,
                    )),
            )
    }

    /// Codex's usage popover over the composer: where the usage comes from,
    /// the tightest limits with when each refreshes, and Manage usage.
    pub(in crate::ui) fn usage_popover(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let Some(harness) = self.composer_harness() else {
            return View::empty();
        };
        let Some(Ok(reply)) = self.settings.quotas.get(&harness) else {
            return View::empty();
        };
        let label = crate::picker::harness_label(&harness);
        let mut source = match crate::quota::plan(reply) {
            Some(plan) => format!("Usage source: {label} · {plan} plan"),
            None => format!("Usage source: {label}"),
        };
        if let Some(observed) = crate::quota::observed_note(reply) {
            source = format!("{source} · {observed}");
        }
        let now = crate::handoff::now_ms();
        let mut windows = crate::quota::windows(reply);
        windows.sort_by_key(|window| window.remaining);
        let rows: Vec<KeyedView> = windows
            .iter()
            .take(USAGE_ROWS)
            .enumerate()
            .map(|(index, window)| {
                let name = window.name();
                let left = format!("{}% left", window.remaining);
                let resets = crate::quota::sentence(&window.resets_in(now));
                KeyedView::new(
                    format!("{index}:{name}:{left}:{resets}"),
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::MD)
                        .margin(Thickness::xy(0.0, tokens::spacing::XS))
                        .automation_name(format!("{name}: {left}, {resets}"))
                        .children((
                            StackPanel::new().children((
                                m3::body(name).text_trimming(TextTrimming::CharacterEllipsis),
                                m3::secondary(resets),
                            )),
                            m3::body(left)
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Top),
                        )),
                )
            })
            .collect();
        m3::popover(palette)
            .width(USAGE_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Right)
            .automation_name("Usage")
            .content(
                StackPanel::new().spacing(tokens::spacing::XS).children((
                    StackPanel::new()
                        .margin(Thickness::new(
                            tokens::spacing::SM,
                            tokens::spacing::SM,
                            tokens::spacing::SM,
                            0.0,
                        ))
                        .children((
                            m3::secondary(source).text_wrapping(TextWrapping::Wrap),
                            StackPanel::new().keyed_children(rows),
                        )),
                    m3::divider(palette),
                    m3::row(palette, false)
                        .automation_name("Manage usage")
                        .on_click(context.message(Message::Shell(ShellMsg::SettingsPage(
                            settings_ui::Page::Usage,
                        ))))
                        .content(m3::row_content(
                            None,
                            "Manage usage",
                            "",
                            m3::icon(m3::glyph::CHEVRON_RIGHT),
                        )),
                )),
            )
    }

    /// Send (steering the running turn), or Resume for a stopped Agent, with
    /// Interrupt while a turn runs. Each is bound now, from the detail on
    /// screen. A stopped Agent has one Resume: the filled button while the
    /// field is empty, the Send arrow (resume with the message) once it has
    /// text.
    fn composer_actions(
        &self,
        agent: Option<String>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let detail = self.chat.detail.as_ref();
        let active_turn = detail.and_then(|detail| detail.active_turn.clone());
        let draft_blank = self.composer_text().trim().is_empty();
        let enabled = !self.submitting();
        let primary: View = match (agent.clone(), detail) {
            (Some(agent), Some(detail)) if resumable(detail) => {
                let resume = context.message(Message::Chat(ChatMsg::DraftRequested(
                    DraftAction::Resume {
                        agent,
                        generation: detail.generation,
                    },
                )));
                if draft_blank {
                    m3::filled(palette)
                        .is_enabled(enabled)
                        .automation_name("Resume")
                        .on_click(resume)
                        .content(m3::label("Resume"))
                } else {
                    m3::send_button(palette)
                        .is_enabled(enabled)
                        .automation_name("Resume with this message (Enter)")
                        .on_click(resume)
                        .content(m3::icon(m3::glyph::SEND))
                        .tooltip("Resume with this message")
                }
            }
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
                m3::text_button(palette)
                    .is_enabled(enabled)
                    .automation_name("Interrupt the running turn")
                    .on_click(context.message(Message::Chat(ChatMsg::InterruptRequested(bound))))
                    .content(m3::icon_label(m3::glyph::STOP, "Interrupt"))
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
    /// the usage popover over the composer. Nothing when the harness
    /// reports no quota.
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
        let open = self.launch.open == Some(Pop::Usage);
        m3::text_button(palette)
            .height(tokens::height::CONTROL)
            .automation_name(format!(
                "Usage: {label}, {} {}% left ({})",
                window.name(),
                window.remaining,
                if open { "open" } else { "show limits" }
            ))
            .on_click(context.message(Message::Launch(LaunchMsg::Toggle(Pop::Usage))))
            .content(
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::XS)
                    .children((
                        // A scoped native track stays visible at this small size.
                        // Reactor 0.100.0 exposes the ring value, but no brush setters.
                        Border::new()
                            .width(20.0)
                            .height(20.0)
                            .corner_radius(10.0)
                            .border_thickness(Thickness::uniform(1.5))
                            .border_brush(palette.quota_track(window.remaining as f64))
                            .vertical_alignment(VerticalAlignment::Center)
                            .content(
                                ProgressRing::new()
                                    .is_indeterminate(false)
                                    .minimum(0.0)
                                    .maximum(100.0)
                                    .value(window.remaining as f64)
                                    .width(16.0)
                                    .height(16.0)
                                    .horizontal_alignment(HorizontalAlignment::Center)
                                    .vertical_alignment(VerticalAlignment::Center),
                            ),
                        m3::secondary(format!("{}%", window.remaining))
                            .vertical_alignment(VerticalAlignment::Center),
                    )),
            )
    }
}

/// A stopped, failed or lost Agent that Resume starts again (not while a
/// turn is recorded as running, never an observe-only Agent).
fn resumable(detail: &UiDetail) -> bool {
    matches!(
        detail.state,
        eliot_core::AgentState::Stopped
            | eliot_core::AgentState::Failed
            | eliot_core::AgentState::Lost
    ) && detail.active_turn.is_none()
        && !detail.observe_only
}
