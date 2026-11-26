use std::cell::RefCell;

use super::*;
use crate::configure::locked_words;
use crate::picker::{effort_label, effort_name};
use crate::state::Drafts;
use eliot_core::api::{HarnessModelsResponse, ModelsCatalogResponse};
use launch_ui::Pop;

/// Limits the usage popover lists (the tightest first).
const USAGE_ROWS: usize = 3;

impl DesktopApp {
    /// What Send (the button, or Enter) does now: retry the unresolved
    /// submission with its saved identity, or start the task, resume a
    /// stopped Agent, steer the running turn, or send the next message.
    pub(in crate::ui) fn primary_action(&self) -> Option<Message> {
        let Some(agent) = self.selected_agent_id() else {
            return Some(Message::Launch(LaunchMsg::LaunchRequested));
        };
        // An unresolved submission retries with its saved action, body,
        // ledger key and mutation id, whatever the Agent state has become:
        // reselecting now could Steer a running turn or Resume a stopped
        // Agent under a fresh id and admit a second effect.
        if self.chat.pending_retry.contains_key(&agent) {
            return Some(Message::Chat(ChatMsg::RetryPending(agent)));
        }
        let detail = match self.bound_detail() {
            Ok(detail) => detail,
            Err(reason) => {
                return Some(Message::Chat(ChatMsg::Refused(reason)));
            }
        };
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
        self.chat
            .drafts
            .borrow()
            .text(self.selected_agent_id().as_deref())
            .to_owned()
    }

    /// The one composer ([`Composer`]): the window binds what the card
    /// shows (its trailing button, chips, usage indicator and attachments);
    /// the composer draws it and owns its text, so a keystroke redraws
    /// the card alone.
    pub(super) fn composer_view(&self, context: &mut ViewContext<Self>) -> View {
        let agent = self.selected_agent_id();
        // Only a readback of the open Agent binds its controls; until then
        // the card keeps the shape its sidebar row implies.
        let detail = self.bound_detail().ok();
        let home = agent.is_none();
        let running =
            detail.is_some_and(|detail| detail.active_turn.is_some() && !detail.observe_only);
        let reading = self.reading_row();
        let placeholder = match detail.filter(|_| !home) {
            None if home => "Ask Eliot Swarm to do anything",
            Some(detail) if resumable(detail) => "Type a message to resume and continue…",
            None if reading == Some(RowMode::Resume) => "Type a message to resume and continue…",
            _ if running || reading == Some(RowMode::Running) => "Type to steer the running turn",
            _ => "Send a follow-up",
        };
        let chips: Vec<View> = match (agent.as_deref(), detail) {
            (None, _) => self.composer_chips(context),
            // The model and access chips keep one line with the action while
            // the card has its full reading width, beside a docked pane or
            // not; narrower, they wrap as Home's do.
            (Some(_), Some(detail)) => controls::chip_lines(
                [
                    Some(self.model_chip(detail, running, context)),
                    access_chip(detail),
                ]
                .into_iter()
                .flatten()
                .collect(),
                if self.pane_docked() {
                    self.layout.full_reading_docked
                } else {
                    self.layout.full_reading
                },
            ),
            (Some(_), None) => Vec::new(),
        };
        let retry = agent
            .as_ref()
            .is_some_and(|agent| self.chat.pending_retry.contains_key(agent));
        let primary =
            match (agent.clone(), detail) {
                (Some(agent), _) if retry => Primary::Retry(agent),
                (Some(agent), Some(detail)) if resumable(detail) => {
                    Primary::Resume(agent, detail.generation)
                }
                (Some(_), None) if reading == Some(RowMode::Resume) => Primary::ReadingResume,
                (Some(_), None) if reading == Some(RowMode::Running) => Primary::ReadingRunning,
                (Some(agent), detail) => Primary::Turn {
                    running: detail
                        .filter(|detail| !detail.observe_only)
                        .and_then(|detail| {
                            detail.active_turn.as_deref().map(|turn| {
                                BoundAction::turn_control(&agent, detail.generation, turn)
                            })
                        }),
                    held: detail.is_some_and(|detail| {
                        detail.active_turn.is_none()
                            && detail.provider_hold.as_ref().is_some_and(|hold| {
                                hold.state == eliot_core::ProviderHoldState::Held
                            })
                    }),
                    agent,
                },
                (None, _) => Primary::Start,
            };
        // The command this composer sent is still being confirmed: its
        // action shows the pending ring (§9.4) until the reply settles it.
        // A retry reuses the original action's key (`retry_pending`), and a
        // retried input is sent again as a launch or a send, so both are
        // covered here.
        let pending = self.in_flight_key.as_deref().is_some_and(|key| {
            let mut fields = key.split(':');
            match (fields.next(), agent.as_deref()) {
                (Some("launch" | "enqueue"), None) => true,
                (Some("send" | "steer" | "interrupt" | "resume"), Some(agent)) => {
                    fields.next() == Some(agent)
                }
                _ => false,
            }
        });
        View::component::<Composer>(ComposerInput {
            drafts: views::Shared(Rc::clone(&self.chat.drafts)),
            version: self.chat.drafts.borrow().version(),
            primed: self.fields_primed,
            placeholder,
            field: self.chat.composer_ref.clone(),
            retry,
            attached: self.attached_to(&self.composer_target()).to_vec(),
            chips,
            // Home's bar is the design's (+, the chips, Start task): the
            // limits are the sidebar's Usage row there.
            // The task's usage indicator (its harness's tightest window,
            // with its popover) and the context label.
            usage: if home {
                None
            } else {
                Some(
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::XS)
                        .vertical_alignment(VerticalAlignment::Center)
                        .keyed_children(crate::views::slots([
                            self.usage_indicator(context),
                            self.context_label(context),
                        ]))
                        .into(),
                )
            },
            // Add as a note: a live Agent the daemon can take a note for.
            note: detail
                .filter(|detail| !home && !detail.observe_only && !resumable(detail))
                .map(|detail| detail.generation),
            primary,
            enabled: !self.submitting() && !(home && self.launch_folder_missing()),
            pending,
            agent,
            send: context.forward(),
        })
    }

    /// The open Agent's model-and-effort chip: the next turn's values; it
    /// opens the effort popover, and the model picker from there.
    fn model_chip(
        &self,
        detail: &UiDetail,
        running: bool,
        context: &mut ViewContext<Self>,
    ) -> View {
        let model = detail.settings.next_model();
        let effort = detail.settings.next_effort();
        // Home's short words ("qwen4-max · high", the model alone at its
        // default effort), so a narrow main area never cuts the chip; its
        // accessible name and tooltip keep the full id and effort.
        let label = crate::picker::model_effort_label(model, effort);
        let name = format!(
            "{} · {}",
            model.unwrap_or("Default model"),
            effort.map_or_else(|| "default effort".to_owned(), effort_name)
        );
        // The model and effort change between turns only: while one runs
        // the chip rests disabled and says so (a flyout's chip takes no
        // tooltip, so this one opens nothing).
        if running {
            return Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(
                    controls::chip()
                        .is_enabled(false)
                        .automation_name(format!(
                            "Model and effort: {name} (change the model between turns)"
                        ))
                        .content(controls::chip_content(
                            Some(controls::glyph::MODEL),
                            &label,
                            false,
                        )),
                )
                .tooltip(format!("{name}. Change the model between turns"));
        }
        let open = self.launch.open == Some(Pop::Effort);
        let chip = self.popover_chip(
            controls::chip()
                .automation_name(format!(
                    "Model and effort: {name} ({})",
                    if open { "open" } else { "change" }
                ))
                .on_click(context.callback(|()| Message::Launch(LaunchMsg::Toggle(Pop::Effort)))),
            controls::chip_content(Some(controls::glyph::MODEL), &label, true),
            &[Pop::Effort, Pop::Model],
            FlyoutPlacement::TopEdgeAlignedLeft,
            context,
        );
        // The full words in the tooltip, as Home's chip (a chip carrying
        // its open flyout takes none).
        let carries_flyout = self.launch.preset.is_none()
            && matches!(self.launch.open, Some(Pop::Effort | Pop::Model));
        if carries_flyout {
            chip
        } else {
            chip.tooltip(name)
        }
    }

    /// The open Agent and its detail when the popovers act on it (not a
    /// preset's picker in Settings).
    pub(in crate::ui) fn popover_agent(&self) -> Option<&UiDetail> {
        if self.launch.preset.is_some() || self.main != Main::Conversation {
            return None;
        }
        self.selected_agent_id()?;
        self.bound_detail().ok()
    }

    /// The open Agent's route catalogue, read for its exact generation:
    /// the cached success, else the cached failure (which retries boundedly,
    /// or at once from its Retry).
    pub(in crate::ui) fn agent_catalog(&self) -> Option<&Result<HarnessModelsResponse, String>> {
        let agent = self.selected_agent_id()?;
        let generation = self.bound_detail().ok()?.generation;
        self.configure
            .catalog
            .as_ref()
            .filter(|catalog| catalog.is_for(&agent, generation))
            .or(self
                .configure
                .failed
                .as_ref()
                .filter(|catalog| catalog.is_for(&agent, generation)))
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
        if harness == "opencode" || self.agent_catalog().is_some() {
            return None;
        }
        self.launch.catalog.as_ref().filter(|read| {
            read.as_ref()
                .is_ok_and(|catalog| catalog.models.iter().any(|model| model.harness == harness))
        })
    }

    /// Confirmed effort steps of the open Agent's next model.
    pub(in crate::ui) fn agent_efforts(&self) -> Vec<String> {
        match (self.agent_catalog(), self.bound_detail().ok()) {
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
    pub(in crate::ui) fn effort_popover(&self, context: &mut ViewContext<Self>) -> Option<View> {
        let detail = self.bound_detail().ok()?;
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
            controls::section_title(shown.map_or("default effort", effort_label))
                .automation_name(shown.map_or_else(|| "default effort".to_owned(), effort_name)),
            controls::secondary(detail.settings.next_model_words()),
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
        let steps: Option<View> = match read {
            Some(Err(error)) => Some(
                StackPanel::new()
                    .spacing(tokens::spacing::XS)
                    .children((
                        controls::secondary(format!(
                            "Effort levels not read: {}",
                            crate::state::plain_error(error)
                        ))
                        .foreground(ThemeBrush::SystemCritical)
                        .text_wrapping(TextWrapping::Wrap),
                        controls::text_button()
                            .automation_name("Retry reading effort levels")
                            .on_click(
                                context.message(Message::Configure(ConfigureMsg::CatalogRetry)),
                            )
                            .content(controls::label("Retry")),
                    ))
                    .into(),
            ),
            _ if effort_locked || (!ready && efforts.is_empty()) => None,
            _ => {
                if efforts.is_empty() {
                    Some(controls::secondary("This model has no effort levels to choose.").into())
                } else {
                    let segments = efforts
                        .iter()
                        .map(|effort| {
                            let selected = shown == Some(effort.as_str());
                            KeyedView::new(
                                format!("effort:{effort}:{selected}"),
                                controls::segment(selected)
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
                                    .content(controls::segment_label(effort_label(effort))),
                            )
                        })
                        .collect();
                    Some(controls::segmented(segments))
                }
            }
        };
        // The refusal is the API's admission text (with its code); the
        // popover says what the harness keeps in plain words (D7).
        let note = match read {
            Some(Ok(_)) => locked_words(harness, model_locked, effort_locked)
                .unwrap_or_else(|| "Applies from the next turn.".to_owned()),
            None => "Checking what this task can change…".to_owned(),
            Some(Err(_)) => String::new(),
        };
        let change_model: Option<View> =
            if (ready && !model_locked) || preview.is_some() || detail.harness == "opencode" {
                Some(
                    StackPanel::new()
                        .spacing(tokens::spacing::XS)
                        .children((
                            controls::divider(),
                            controls::row(false)
                                .is_enabled(!self.submitting())
                                .automation_name("Change model")
                                .on_click(
                                    context.message(Message::Launch(LaunchMsg::Toggle(Pop::Model))),
                                )
                                .content(controls::row_content(
                                    Some(controls::glyph::MODEL),
                                    "Change model…",
                                    "",
                                    controls::icon(controls::glyph::CHEVRON_RIGHT),
                                )),
                        ))
                        .into(),
                )
            } else {
                None
            };
        let hints: Option<View> = if effort_locked || !ready || efforts.is_empty() {
            None
        } else {
            Some(controls::secondary("←→ effort   Enter apply   Esc close").into())
        };
        Some(
            StackPanel::new()
                .min_width(tokens::layout::EFFORT_MENU_MIN_WIDTH)
                .spacing(tokens::spacing::SM)
                .margin(Thickness::uniform(tokens::spacing::SM))
                .automation_name("Model and effort")
                .keyed_children(crate::views::slots([
                    Some(header.into()),
                    steps,
                    if note.is_empty() {
                        None
                    } else {
                        Some(
                            controls::secondary(note)
                                .text_wrapping(TextWrapping::Wrap)
                                .into(),
                        )
                    },
                    hints,
                    change_model,
                ]))
                .into(),
        )
    }

    /// Codex's usage popover over the composer: where the usage comes from,
    /// the tightest limits with when each refreshes, and Manage usage.
    pub(in crate::ui) fn usage_popover(&self, context: &mut ViewContext<Self>) -> Option<View> {
        let harness = self.composer_harness()?;
        let Some(Ok(reply)) = self.settings.quotas.get(&harness) else {
            return None;
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
                                controls::body(name).text_trimming(TextTrimming::CharacterEllipsis),
                                controls::secondary(resets),
                            )),
                            controls::body(left)
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Top),
                        )),
                )
            })
            .collect();
        Some(
            self.usage_menu(
                tokens::layout::USAGE_MENU_WIDTH,
                StackPanel::new()
                    .children((
                        controls::secondary(source).text_wrapping(TextWrapping::Wrap),
                        StackPanel::new().keyed_children(rows),
                    ))
                    .into(),
                context,
            ),
        )
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

    /// Whether the harness account's limits govern the composer's target:
    /// `Some(false)` when its preset runs through a proxy (another
    /// provider's limits apply), `None` when the preset is not known.
    fn own_account(&self) -> Option<(bool, Option<String>)> {
        let summary = match self.selected_agent_id() {
            Some(_) => {
                let name = &self.chat.detail.as_ref()?.settings.profile;
                self.launch
                    .profile_summaries
                    .iter()
                    .find(|profile| &profile.name == name)?
            }
            None => match &self.launch.choice {
                // An ad-hoc harness runs on its own account.
                crate::picker::Choice::Model(_) => return Some((true, None)),
                _ => self.launch.profile()?,
            },
        };
        Some((summary.proxy.is_none(), summary.proxy.clone()))
    }

    /// Codex's usage indicator beside Send: the tightest quota window of
    /// the composer's harness, the share left in words; it opens
    /// the usage popover as its flyout (without its tooltip meanwhile: a
    /// flyout's chip takes none). The words only for a real reading on the
    /// account the task runs on; the icon alone, its tooltip saying why,
    /// when that is not known or another provider's limits apply. Nothing
    /// when the harness reports no quota.
    fn usage_indicator(&self, context: &mut ViewContext<Self>) -> Option<View> {
        let harness = self.composer_harness()?;
        let Some(Ok(reply)) = self.settings.quotas.get(&harness) else {
            return None;
        };
        let window = crate::quota::tightest(reply)?;
        let label = crate::picker::harness_label(&harness);
        let open = self.launch.open == Some(Pop::Usage);
        let remaining = window.remaining as f64;
        let (applies, note) = match self.own_account() {
            Some((true, _)) => (
                true,
                format!("{} {}% left", window.name(), window.remaining),
            ),
            Some((false, proxy)) => (
                false,
                format!(
                    "{label} limits don't apply here: this preset runs through {}",
                    proxy.as_deref().unwrap_or("a proxy")
                ),
            ),
            None => (
                false,
                format!("Not known whether {label} limits apply here"),
            ),
        };
        // The share left in words beside the Usage icon; the icon alone
        // when no reading applies, its tooltip says why.
        let content: View = if applies {
            controls::command_content(
                controls::glyph::USAGE,
                &format!("{}% left", window.remaining),
                false,
            )
        } else {
            controls::icon(controls::glyph::USAGE)
        };
        let button = controls::bar_button(false)
            .style(ButtonStyle::Subtle)
            .automation_name(format!(
                "Usage: {label}, {note} ({})",
                if open { "open" } else { "show limits" }
            ))
            .on_click(context.callback(|()| Message::Launch(LaunchMsg::Toggle(Pop::Usage))));
        let open = open && self.launch.preset.is_none();
        let button = if open {
            self.anchored_popover(
                button,
                content,
                Some(Pop::Usage),
                FlyoutPlacement::TopEdgeAlignedRight,
                context,
            )
        } else {
            button.content(content).tooltip(note)
        };
        // Keyed by whether the reading applies and by whether its flyout is
        // open (a tooltip and a flyout do not share a button).
        Some(
            Grid::new()
                .keyed_children(vec![KeyedView::new(
                    format!(
                        "usage:{applies}:{}:{open}",
                        remaining < crate::controls::QUOTA_LOW
                    ),
                    button,
                )])
                .into(),
        )
    }

    /// The context beside the main action (brand book §9.5), small text as
    /// the harness reports it: "Context 34%" against a known window, else
    /// the tokens used ("Context 54k tokens"), never 0%; just "Context"
    /// while the harness reports no tokens, so the way to Details stays.
    /// Its tooltip says how fresh; a click opens Details. Provider quota is the
    /// usage indicator's, not this.
    fn context_label(&self, context: &mut ViewContext<Self>) -> Option<View> {
        let detail = self.bound_detail().ok()?;
        let fill = detail.context.as_ref();
        let age = fill
            .and_then(|fill| fill.observed_at)
            .map_or_else(String::new, |at| {
                format!(
                    ", {} ago",
                    monitor::elapsed_label(crate::handoff::now_ms().saturating_sub(at))
                )
            });
        let (label, words) = match fill.map(|fill| (fill.used, fill.window, fill.percent())) {
            Some((Some(used), Some(window), Some(share))) => (
                format!("Context {}%", share.round()),
                format!(
                    "{} of {} tokens{age}",
                    monitor::group(used),
                    monitor::group(window)
                ),
            ),
            Some((Some(used), _, _)) => (
                format!("Context {}k tokens", used / 1_000),
                format!("{} tokens{age}", monitor::group(used)),
            ),
            _ => ("Context".to_owned(), format!("Not reported{age}")),
        };
        Some(
            controls::bar_button(false)
                .style(ButtonStyle::Subtle)
                .automation_name(format!("{label}. {words} (open Details)"))
                .on_click(
                    context.message(Message::Monitor(MonitorMsg::PanelChosen(Panel::Details))),
                )
                .content(controls::secondary(label))
                .tooltip(words),
        )
    }
}

/// The open Agent's access, read only (`.chip.plain`): fixed for the
/// Agent; changing it is a hand-off. None when the launch named no level.
fn access_chip(detail: &UiDetail) -> Option<View> {
    let access = detail.settings.access.as_deref().and_then(|wire| {
        crate::settings::Access::ALL
            .into_iter()
            .find(|access| access.wire() == wire)
    })?;
    Some(
        controls::fixed_chip(
            access == crate::settings::Access::Full,
            Some(controls::glyph::SHIELD),
            access.label(),
            &format!("Access: {} (fixed for this Agent)", access.label()),
        )
        .tooltip("Access is fixed for this Agent; hand off to change it"),
    )
}

/// A stopped, failed or lost Agent that Resume starts again (not while a
/// turn is recorded as running, never an observe-only Agent).
/// What an open task's composer offers while its detail is still read,
/// taken from its sidebar row ([`DesktopApp::reading_row`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RowMode {
    Resume,
    Running,
}

impl DesktopApp {
    /// While the open task's detail is read, the mode its sidebar row
    /// implies: the card keeps the shape it will have (Resume for an ended
    /// task, Stop for a working one), so an open never flashes another.
    fn reading_row(&self) -> Option<RowMode> {
        if self.bound_detail().is_ok() {
            return None;
        }
        let row = self.find_agent(&self.selected_agent_id()?)?;
        match row.state {
            _ if row.native_child => None,
            eliot_core::AgentState::Stopped
            | eliot_core::AgentState::Failed
            | eliot_core::AgentState::Lost => Some(RowMode::Resume),
            eliot_core::AgentState::Busy => Some(RowMode::Running),
            _ => None,
        }
    }
}

fn resumable(detail: &UiDetail) -> bool {
    matches!(
        detail.state,
        eliot_core::AgentState::Stopped
            | eliot_core::AgentState::Failed
            | eliot_core::AgentState::Lost
    ) && detail.active_turn.is_none()
        && !detail.observe_only
}

/// The composer's trailing button, bound by the window from the detail on
/// screen ([`DesktopApp::composer_view`]); the draft decides between its
/// forms (Stop or Steer, Resume or Resume with the message).
#[derive(Clone, Debug, PartialEq)]
pub(in crate::ui) enum Primary {
    /// The unresolved submission's Retry (its Agent): it resubmits the
    /// saved action, body, ledger key and mutation id, whatever the Agent
    /// state has become since, and needs no new text.
    Retry(String),
    /// Resume the stopped Agent at this generation.
    Resume(String, i64),
    /// While the detail is read, the row's button in its final place, not
    /// yet active.
    ReadingResume,
    ReadingRunning,
    /// An open Agent: Stop with an empty field and Steer with text while
    /// this turn runs; Send otherwise (closed during a provider hold).
    Turn {
        agent: String,
        running: Option<BoundAction>,
        held: bool,
    },
    /// The new task's Start.
    Start,
}

/// The composer's input. The text is not in it: the composer reads the
/// shared drafts, and `version` counts the window's changes to them. The
/// views the window builds here (`project`, `chips`, `usage`) take only
/// captureless callbacks, so they compare equal from one window render to
/// the next and a render for anything else leaves the composer as it is.
#[derive(Clone, PartialEq)]
pub(in crate::ui) struct ComposerInput {
    /// The open Agent (`None`: the new task).
    agent: Option<String>,
    drafts: views::Shared<RefCell<Drafts>>,
    version: u64,
    /// False for the render that re-creates the editors empty
    /// ([`DesktopApp::reprime_fields`]).
    primed: bool,
    placeholder: &'static str,
    field: ElementRef<TextBox>,
    /// An unresolved submission: an edit abandons it, in the window.
    retry: bool,
    attached: Vec<String>,
    /// The chip lines, Home's (`composer_chips`) or a conversation's: one
    /// at the card's full width, else two ([`controls::chip_lines`]).
    chips: Vec<View>,
    usage: Option<View>,
    /// Add as a note is offered: the Agent's generation.
    note: Option<i64>,
    primary: Primary,
    /// No submission is in flight.
    enabled: bool,
    /// This composer's own command is being confirmed: its action carries
    /// the pending ring.
    pending: bool,
    send: Callback<Message>,
}

/// The one composer, one rounded card, as its own component. A new task:
/// the field, then + and the project, place, model and access chips with
/// Start task at the end of their last line. An open Agent: the field, then
/// the model-and-effort chip. Usage and the one trailing button (Send, Stop, Steer or Resume)
/// sit at the right. A keystroke is stored here and redraws this card
/// only; an edit that also concerns the window (Enter's line break, an
/// unresolved submission) and every control go to the window.
pub(in crate::ui) struct Composer {
    input: ComposerInput,
}

impl Composer {
    /// Store an edit that concerns nothing but the text; `false` when the
    /// window handles it (`ChatMsg::ComposerChanged`).
    fn store(&self, text: &str) -> bool {
        let input = &self.input;
        let agent = input.agent.as_deref();
        let mut drafts = input.drafts.0.borrow_mut();
        let previous = drafts.text(agent);
        // The echo of a text the window set (a clear after Send, a
        // restore) is no edit: the draft revision stays.
        if text == previous {
            return true;
        }
        if input.retry || enter_send_text(previous, text).is_some() {
            return false;
        }
        // Every edit spends an earlier plain Enter, as in the window.
        let _ = crate::shortcuts::take_plain_enter();
        // Kept exactly as the TextBox reports it: a normalized copy would
        // differ, be written back, and move the caret (or end an IME
        // composition). Line endings are normalized at submission.
        drafts.typed(agent, text.to_owned());
        true
    }
}

impl Component for Composer {
    type Input = ComposerInput;
    type Message = Message;

    fn create(input: &ComposerInput, _context: &ComponentContext<Self>) -> Self {
        Self {
            input: input.clone(),
        }
    }

    fn input_changed(&mut self, input: &ComposerInput, _context: &ComponentContext<Self>) {
        self.input = input.clone();
    }

    fn update(&mut self, message: Message, _context: &ComponentContext<Self>) {
        if let Message::Chat(ChatMsg::ComposerChanged(text)) = &message
            && self.store(text)
        {
            return;
        }
        self.input.send.call(message);
    }

    fn view(&self, input: &ComposerInput, context: &mut ViewContext<Self>) -> View {
        let text = input
            .drafts
            .0
            .borrow()
            .text(input.agent.as_deref())
            .to_owned();
        let blank = text.trim().is_empty();
        let field = TextBox::new()
            .element_ref(&input.field)
            .text(if input.primed { text } else { String::new() })
            .placeholder_text(input.placeholder)
            .automation_name("Message")
            .background(Brush::Solid(Color::transparent()))
            .border_thickness(Thickness::uniform(0.0))
            .accepts_return(true)
            .text_wrapping(TextWrapping::Wrap)
            .min_height(tokens::layout::COMPOSER_MIN_HEIGHT)
            .max_height(tokens::layout::COMPOSER_MAX_HEIGHT)
            .on_text_changed(crate::views::text_edits(
                context.callback(|value| Message::Chat(ChatMsg::ComposerChanged(value))),
            ));
        // WinUI's text box insets its text: the margin starts the text at
        // the card's 16-DIP inset, where the chips start.
        let field = Border::new()
            .margin(Thickness::new(
                -tokens::spacing::FIELD_LEADING_INSET,
                0.0,
                0.0,
                0.0,
            ))
            .content(field);
        // The design: +, then the chips, 6 DIPs apart; each line of chips
        // trims its first label to the room left. Home's + attaches files
        // (its Goal is a chip); an open chat's + is a menu of Attach files
        // and Goal (brand book §9.1: a goal by hand from the composer).
        let home = input.agent.is_none();
        let plus = controls::bar_button(false)
            .style(ButtonStyle::Subtle)
            .vertical_alignment(VerticalAlignment::Center);
        let attach: View = if home {
            plus.automation_name("Attach files")
                .on_click(context.message(Message::Launch(LaunchMsg::AttachRequested)))
                .content(controls::icon(controls::glyph::NEW_TASK))
                .tooltip("Attach files")
        } else {
            // A button carrying its menu flyout takes no tooltip.
            plus.automation_name("Attach files or set the goal")
                .content(controls::icon(controls::glyph::NEW_TASK))
                .menu(Menu::new(
                    vec![
                        MenuItem::item(ATTACH_LABEL, ATTACH_LABEL),
                        MenuItem::item(GOAL_LABEL, GOAL_LABEL),
                    ],
                    // Capture-free: one identity across renders.
                    crate::views::menu_keys(context.callback(
                        |label: String| match label.as_str() {
                            ATTACH_LABEL => Message::Launch(LaunchMsg::AttachRequested),
                            GOAL_LABEL => Message::Goal(goal_ui::GoalMsg::Composer),
                            _ => Message::Menu(MenuMsg::Ignored),
                        },
                    )),
                ))
        };
        // The hint before the action, while Enter steers: the
        // lowest-priority cell, it trims away first.
        let hint: Option<View> = match &input.primary {
            Primary::Turn {
                running: Some(_), ..
            } if !blank => Some(
                controls::secondary("Enter to steer")
                    .horizontal_alignment(HorizontalAlignment::Right)
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_trimming(TextTrimming::CharacterEllipsis)
                    .into(),
            ),
            _ => None,
        };
        // Without the hint its cell is empty and takes no room.
        let hint_cell = if hint.is_some() {
            GridLength::STAR
        } else {
            GridLength::Auto
        };
        let actions = composer_actions(input, blank, context);
        // One layout for Home and a conversation (§8.1, §9.4): +, the chip
        // lines (one, or two below the card's full width) and the action at
        // the end of the last line, right-aligned, so no line holds the
        // action alone; the steer hint before it trims away first.
        let last = input.chips.len().saturating_sub(1);
        let mut cells = vec![KeyedView::new("attach", attach)];
        cells.extend(input.chips.iter().enumerate().map(|(index, line)| {
            KeyedView::new(
                format!("chips:{index}"),
                Border::new()
                    .grid_row(index as i32)
                    .grid_column(1)
                    // A line above the last also takes the hint's and the
                    // action's columns.
                    .grid_column_span(if index == last { 1 } else { 3 })
                    .vertical_alignment(VerticalAlignment::Center)
                    .content(line.clone()),
            )
        }));
        if let Some(hint) = hint {
            cells.push(KeyedView::new(
                "hint",
                Border::new()
                    .grid_row(last as i32)
                    .grid_column(2)
                    .content(hint),
            ));
        }
        cells.push(KeyedView::new(
            "actions",
            Border::new()
                .grid_row(last as i32)
                .grid_column(3)
                .margin(Thickness::new(
                    tokens::spacing::SM - tokens::spacing::CHIP_GAP,
                    0.0,
                    0.0,
                    0.0,
                ))
                .vertical_alignment(VerticalAlignment::Center)
                .content(actions),
        ));
        let bottom: View = Grid::new()
            .columns([
                GridLength::Auto,
                GridLength::Star(3.0),
                hint_cell,
                GridLength::Auto,
            ])
            .rows(vec![GridLength::Auto; last + 1])
            .column_spacing(tokens::spacing::CHIP_GAP)
            .row_spacing(tokens::spacing::CHIP_GAP)
            .keyed_children(cells)
            .into();
        // Home (`Main.dc.html`): the field first, then + and the project,
        // place, model, access and Goal chips, then Start task. Attached
        // files as chips over the field.
        let mut rows: Vec<KeyedView> = Vec::new();
        if !input.attached.is_empty() {
            rows.push(KeyedView::new(
                format!("attached:{}", input.attached.len()),
                attachment_chips(&input.attached, context),
            ));
        }
        rows.push(KeyedView::new("field", field));
        rows.push(KeyedView::new(
            if home { "bottom:home" } else { "bottom" },
            bottom,
        ));
        // Home's composer stretches across the home column; a
        // conversation's takes the reading column.
        let card = Border::new().grid_row(5);
        if home {
            card
        } else {
            card.column(
                tokens::layout::READING_WIDTH,
                tokens::spacing::INSET,
                tokens::spacing::LG,
            )
        }
        .content(controls::composer_card(
            Grid::new(),
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .keyed_children(rows),
        ))
        .into()
    }
}

/// The composer's main action (`ComposerAction`, §9.4), named in words:
/// Send; while a turn runs, Stop turn (it interrupts that turn) with an
/// empty field and Steer with text; Resume for a stopped Agent, Resume with
/// message when there is text. An unresolved submission shows Retry
/// instead, resubmitting its saved identity. Each is bound by the window
/// from the detail on screen. Beside it the ▾ with Add as a note; usage
/// and context before them. Every command is one bar height.
fn composer_actions(
    input: &ComposerInput,
    draft_blank: bool,
    context: &mut ViewContext<Composer>,
) -> View {
    let enabled = input.enabled;
    let pending = input.pending;
    // Send, Steer, Stop turn and Resume say what they do in words (§9.4),
    // at the bar's height; the one being confirmed holds the pending ring.
    let primary: View = match &input.primary {
        Primary::Retry(agent) => controls::bar_button(true)
            .is_enabled(enabled)
            .automation_name("Retry (Enter)")
            .on_click(context.message(Message::Chat(ChatMsg::RetryPending(agent.clone()))))
            .content(controls::command_content(
                controls::glyph::REFRESH,
                "Retry",
                pending,
            ))
            .tooltip("Retry with the same id"),
        Primary::Resume(agent, generation) => {
            // §9.4 Resume, or Resume with message with text: the button
            // says Resume either way and names the message in its
            // accessible name and tooltip, so typing never widens it into
            // the chips' room (the one-row rule cuts no chip).
            let words = if draft_blank {
                "Resume"
            } else {
                "Resume with message"
            };
            controls::bar_button(false)
                .is_enabled(enabled)
                .automation_name(format!("{words} (Enter)"))
                .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                    DraftAction::Resume {
                        agent: agent.clone(),
                        generation: *generation,
                    },
                ))))
                .content(controls::command_content(
                    controls::glyph::RESUME,
                    "Resume",
                    pending,
                ))
                .tooltip(words)
        }
        Primary::ReadingResume => controls::bar_button(false)
            .is_enabled(false)
            .automation_name("Resume")
            .content(controls::command_content(
                controls::glyph::RESUME,
                "Resume",
                false,
            ))
            .into(),
        Primary::ReadingRunning => controls::bar_button(false)
            .is_enabled(false)
            .automation_name("Stop turn")
            .content(controls::command_content(
                controls::glyph::STOP,
                "Stop turn",
                false,
            ))
            .into(),
        Primary::Turn {
            agent,
            running,
            held,
        } => match running {
            Some(bound) if draft_blank => controls::bar_button(false)
                .is_enabled(enabled)
                .automation_name("Stop turn")
                .on_click(
                    context.message(Message::Chat(ChatMsg::InterruptRequested(bound.clone()))),
                )
                .content(controls::command_content(
                    controls::glyph::STOP,
                    "Stop turn",
                    pending,
                ))
                .into(),
            Some(bound) => controls::bar_button(true)
                .is_enabled(enabled)
                .automation_name("Steer the running turn (Enter)")
                .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                    DraftAction::Steer(bound.clone()),
                ))))
                .content(controls::command_content(
                    controls::glyph::STEER,
                    "Steer",
                    pending,
                ))
                .into(),
            None => controls::bar_button(true)
                .is_enabled(enabled && !draft_blank && !held)
                .automation_name(if *held {
                    "Send: closed during the provider hold"
                } else {
                    "Send (Enter)"
                })
                .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                    DraftAction::Send {
                        agent: agent.clone(),
                    },
                ))))
                .content(controls::command_content(
                    controls::glyph::ARROW_UP,
                    "Send",
                    pending,
                ))
                .tooltip(if *held {
                    "Closed during the provider hold"
                } else {
                    "Send"
                }),
        },
        Primary::Start => start_split(enabled, draft_blank, pending, context),
    };
    // The ▾ beside the action (an open Agent): Add as a note, the text
    // into the Agent's context at its next safe point without a new turn.
    let note: Option<View> = match (&input.primary, input.note) {
        (Primary::Start, _) => None,
        (primary, generation) => {
            let ready = enabled && !draft_blank && generation.is_some();
            let item = if ready {
                MenuItem::item(NOTE_LABEL, NOTE_LABEL)
            } else {
                MenuItem::disabled(NOTE_LABEL, NOTE_LABEL)
            };
            // The ▾ takes its action's style: accent beside Send, Steer and
            // Retry.
            let accent = matches!(
                primary,
                Primary::Turn { running: None, .. } | Primary::Retry(_)
            ) || (matches!(
                primary,
                Primary::Turn {
                    running: Some(_),
                    ..
                }
            ) && !draft_blank);
            Some(
                controls::bar_button(accent)
                    .is_enabled(ready)
                    .automation_name("More send options: Add as a note")
                    .content(controls::icon(controls::glyph::CHEVRON_DOWN))
                    .menu(Menu::new(
                        vec![item],
                        // Capture-free: one identity across renders.
                        crate::views::menu_keys(context.callback(|label: String| {
                            if label == NOTE_LABEL {
                                Message::Chat(ChatMsg::NoteRequested)
                            } else {
                                Message::Menu(MenuMsg::Ignored)
                            }
                        })),
                    )),
            )
        }
    };
    // The action is keyed by its kind: a Send turned Stop is a new button
    // with its own style and name.
    let kind = match &input.primary {
        Primary::Retry(_) => "retry",
        Primary::Resume(..) | Primary::ReadingResume => "resume",
        Primary::ReadingRunning => "stop",
        Primary::Turn {
            running: Some(_), ..
        } if draft_blank => "stop",
        Primary::Turn {
            running: Some(_), ..
        } => "steer",
        Primary::Turn { .. } | Primary::Start => "send",
    };
    let action = vec![KeyedView::new(format!("action:{kind}"), primary)];
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::SM)
        .keyed_children(crate::views::slots([
            input.usage.clone(),
            Some(
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::XS)
                    .children((
                        Grid::new().keyed_children(action),
                        Grid::new().keyed_children(
                            note.map(|note| KeyedView::new(format!("note:{kind}"), note))
                                .into_iter()
                                .collect::<Vec<_>>(),
                        ),
                    ))
                    .into(),
            ),
        ]))
        .into()
}

/// The composer menu's one item.
const NOTE_LABEL: &str = "Add as a note";
/// An open chat's + menu: attach files, or open the Goal dialog.
const ATTACH_LABEL: &str = "Attach files…";
const GOAL_LABEL: &str = "Goal…";

/// Files attached in the composer: one outlined chip each (file glyph,
/// name, the full path in its tooltip); a click removes it.
fn attachment_chips(files: &[String], context: &mut ViewContext<Composer>) -> View {
    let chips: Vec<KeyedView> = files
        .iter()
        .map(|path| {
            let name = crate::attachments::file_name(path);
            KeyedView::new(
                path.clone(),
                controls::chip()
                    .automation_name(format!("Remove attached file {name}"))
                    .on_click(
                        context
                            .message(Message::Launch(LaunchMsg::AttachmentRemoved(path.clone()))),
                    )
                    .content(
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::CHIP_GAP)
                            .children((
                                controls::chip_content(Some(controls::glyph::FILE), name, false),
                                controls::small_icon(controls::glyph::CLOSE),
                            )),
                    )
                    .tooltip(path.clone()),
            )
        })
        .collect();
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::CHIP_GAP)
        .keyed_children(chips)
        .into()
}

/// Home's Start task: the accent action (the up arrow and "Start task", the
/// pending ring while the launch is confirmed) and beside it its ▾ menu:
/// Queue it (the task as a queued intent) and Attach an existing session…,
/// which the desktop does not have yet (disabled, the reason in its label).
fn start_split(
    enabled: bool,
    draft_blank: bool,
    pending: bool,
    context: &mut ViewContext<Composer>,
) -> View {
    use super::menu_ui::{Action, Entry};
    let ready = enabled && !draft_blank;
    // Queue it while the draft can start; Attach an existing session… is
    // not in the desktop yet (disabled, the reason in its label).
    let items = super::menu_ui::menu_items(&[
        if ready {
            Entry::item(Action::QueueLaunch)
        } else {
            Entry::unavailable(Action::QueueLaunch, "write the task first")
        },
        Entry::unavailable(
            Action::AttachSession,
            "not in the desktop yet: swarm attach",
        ),
    ]);
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::XS)
        .children((
            controls::bar_button(true)
                .is_enabled(ready)
                .automation_name("Start task (Enter)")
                .on_click(context.message(Message::Launch(LaunchMsg::LaunchRequested)))
                .content(controls::command_content(
                    controls::glyph::ARROW_UP,
                    "Start task",
                    pending,
                )),
            controls::bar_button(true)
                .is_enabled(ready)
                .automation_name("More start options")
                .content(controls::icon(controls::glyph::CHEVRON_DOWN))
                .menu(Menu::new(
                    items,
                    // Capture-free: one identity across renders.
                    crate::views::menu_keys(context.callback(|key: String| {
                        match super::menu_ui::Action::from_key(&key) {
                            Some(super::menu_ui::Action::QueueLaunch) => {
                                Message::Launch(LaunchMsg::QueueDraft)
                            }
                            _ => Message::Menu(MenuMsg::Ignored),
                        }
                    })),
                )),
        ))
        .into()
}
