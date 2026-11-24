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
        self.chat
            .drafts
            .borrow()
            .text(self.selected_agent_id().as_deref())
            .to_owned()
    }

    /// The one composer ([`Composer`]): the window binds what the card
    /// shows (its trailing button, chips, usage ring and attachments);
    /// the composer draws it and owns its text, so a keystroke redraws
    /// the card alone.
    pub(super) fn composer_view(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let agent = self.selected_agent_id();
        let detail = self.chat.detail.as_ref();
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
        let widest = if home {
            tokens::layout::HOME_WIDTH
        } else {
            tokens::layout::READING_WIDTH
        };
        // Home's composer takes the home column (760), a conversation's its
        // reading column (720).
        let width = if home {
            self.column_width(widest)
        } else {
            column
        };
        // A narrow composer drops its hint and trims its chips' labels to
        // the room left, so the buttons and the chevrons stay whole.
        let hint = width.is_none_or(|width| width >= tokens::layout::COMPOSER_HINT_MIN_WIDTH);
        let label = |others: f64| {
            width.map_or(tokens::layout::PROJECT_CHIP_WIDTH, |width| {
                (width - tokens::layout::COMPOSER_ACTIONS_WIDTH - others).clamp(
                    tokens::layout::COMPOSER_LABEL_MIN_WIDTH,
                    tokens::layout::PROJECT_CHIP_WIDTH,
                )
            })
        };
        let chips: View = match (agent.as_deref(), detail) {
            (None, _) => self.composer_chips(
                label(tokens::layout::COMPOSER_ACCESS_WIDTH),
                context,
                palette,
            ),
            (Some(_), Some(detail)) => self.model_chip(detail, label(0.0), context, palette),
            (Some(_), None) => View::empty(),
        };
        let retry = agent
            .as_ref()
            .is_some_and(|agent| self.chat.pending_retry.contains_key(agent));
        let primary = match (agent.clone(), detail) {
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
                        detail
                            .active_turn
                            .as_deref()
                            .map(|turn| BoundAction::turn_control(&agent, detail.generation, turn))
                    }),
                agent,
            },
            (None, _) => Primary::Start,
        };
        let popover = self.launch.open.is_some();
        View::component::<Composer>(ComposerInput {
            palette,
            drafts: views::Shared(Rc::clone(&self.chat.drafts)),
            version: self.chat.drafts.borrow().version(),
            primed: self.fields_primed,
            placeholder,
            field: self.chat.composer_ref.clone(),
            popover,
            dismiss: popover && self.launch.preset.is_none(),
            retry,
            project: if home {
                self.composer_project_row(context, palette)
            } else {
                View::empty()
            },
            attached: self.attached_to(&self.composer_target()).to_vec(),
            chips,
            usage: self.usage_indicator(context, palette),
            primary,
            hint,
            enabled: !self.submitting(),
            width,
            widest,
            agent,
            send: context.forward(),
        })
    }

    /// The open Agent's model-and-effort chip: the next turn's values; it
    /// opens the effort popover.
    fn model_chip(
        &self,
        detail: &UiDetail,
        label_width: f64,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let full = detail.settings.next_model().unwrap_or("Default model");
        // The chip names the model as the header does ("qwen4-max · high");
        // its accessible name keeps the full id.
        let model = full;
        let effort = detail.settings.next_effort();
        let label = format!(
            "{} · {}",
            crate::picker::model_short(full),
            effort.map_or("default effort", effort_label)
        );
        let name = format!(
            "{model} · {}",
            effort.map_or_else(|| "default effort".to_owned(), effort_name)
        );
        let open = self.launch.open == Some(Pop::Effort);
        m3::chip(palette)
            .automation_name(format!(
                "Model and effort: {name} ({})",
                if open { "open" } else { "change" }
            ))
            .on_click(context.callback(|()| Message::Launch(LaunchMsg::Toggle(Pop::Effort))))
            .content(m3::chip_content(None, &label, true, label_width))
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

    /// The open Agent's route catalogue, read for its exact generation:
    /// the cached success, else the cached failure (which retries boundedly,
    /// or at once from its Retry).
    pub(in crate::ui) fn agent_catalog(&self) -> Option<&Result<HarnessModelsResponse, String>> {
        let agent = self.selected_agent_id()?;
        let generation = self.chat.detail.as_ref()?.generation;
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
            Some(Err(error)) => StackPanel::new().spacing(tokens::spacing::XS).children((
                m3::secondary(format!(
                    "Effort levels not read: {}",
                    crate::state::plain_error(error)
                ))
                .foreground(palette.attention())
                .text_wrapping(TextWrapping::Wrap),
                m3::text_button(palette)
                    .automation_name("Retry reading effort levels")
                    .on_click(context.message(Message::Configure(ConfigureMsg::CatalogRetry)))
                    .content(m3::label("Retry")),
            )),
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
        // The refusal is the API's admission text (with its code); the
        // popover says what the harness keeps in plain words (D7).
        let note = match read {
            Some(Ok(_)) => locked_words(harness, model_locked, effort_locked)
                .unwrap_or_else(|| "Applies from the next turn.".to_owned()),
            None => "Checking what this task can change…".to_owned(),
            Some(Err(_)) => String::new(),
        };
        let change_model: View =
            if (ready && !model_locked) || preview.is_some() || detail.harness == "opencode" {
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
        m3::lifted(
            palette,
            crate::theme::elevation::Level::Two,
            tokens::radius::CONTAINER,
            Grid::new()
                .min_width(tokens::layout::EFFORT_MENU_MIN_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Left),
            m3::popover(palette)
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
                ),
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
        m3::lifted(
            palette,
            crate::theme::elevation::Level::Two,
            tokens::radius::CONTAINER,
            Grid::new()
                .width(tokens::layout::USAGE_MENU_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Right),
            m3::popover(palette).automation_name("Usage").content(
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
    /// the composer's harness as a small ring and the share left; it opens
    /// the usage popover over the composer. The attention color only for a
    /// real reading under the threshold on the account the task runs on; a
    /// neutral outline ring when that is not known or another provider's
    /// limits apply. Nothing when the harness reports no quota.
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
        let ring: View = if applies {
            // Reactor 0.100.0 has no brush setters on the ring: its arc and
            // track come from this button's resources (`usage_ring`).
            ProgressRing::new()
                .is_indeterminate(false)
                .minimum(0.0)
                .maximum(100.0)
                .value(remaining)
                .width(tokens::layout::USAGE_RING)
                .height(tokens::layout::USAGE_RING)
                .horizontal_alignment(HorizontalAlignment::Center)
                .vertical_alignment(VerticalAlignment::Center)
                .into()
        } else {
            Border::new()
                .width(tokens::layout::USAGE_RING)
                .height(tokens::layout::USAGE_RING)
                .corner_radius(tokens::layout::USAGE_RING / 2.0)
                .border_brush(palette.outline())
                .border_thickness(Thickness::uniform(tokens::layout::USAGE_RING_STROKE))
                .horizontal_alignment(HorizontalAlignment::Center)
                .vertical_alignment(VerticalAlignment::Center)
                .into()
        };
        // Keyed by whether the reading applies: the ring's colors come from
        // the button's resources, read when its template is applied.
        Grid::new().keyed_children(vec![KeyedView::new(
            format!("usage:{applies}:{}", remaining < crate::theme::QUOTA_LOW),
            Button::new()
                .resource_overrides(palette.usage_ring(if applies { remaining } else { 100.0 }))
                .style(ButtonStyle::Subtle)
                .width(tokens::height::CONTROL)
                .height(tokens::height::CONTROL)
                .automation_name(format!(
                    "Usage: {label}, {note} ({})",
                    if open { "open" } else { "show limits" }
                ))
                .on_click(context.callback(|()| Message::Launch(LaunchMsg::Toggle(Pop::Usage))))
                .content(ring)
                .tooltip(note),
        )])
    }
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
        if self.chat.detail.is_some() {
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
    /// this turn runs; Send otherwise.
    Turn {
        agent: String,
        running: Option<BoundAction>,
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
    palette: Palette,
    /// The open Agent (`None`: the new task).
    agent: Option<String>,
    drafts: views::Shared<RefCell<Drafts>>,
    version: u64,
    /// False for the render that re-creates the editors empty
    /// ([`DesktopApp::reprime_fields`]).
    primed: bool,
    placeholder: &'static str,
    field: ElementRef<TextBox>,
    /// A popover is open: typing closes it, in the window.
    popover: bool,
    /// The clear layer over the field that takes a press and closes the
    /// popover (not for a preset's picker in Settings).
    dismiss: bool,
    /// An unresolved submission: an edit abandons it, in the window.
    retry: bool,
    /// Home's project and worktree chips above the field.
    project: View,
    attached: Vec<String>,
    chips: View,
    usage: View,
    primary: Primary,
    /// The hint before the trailing buttons ("Enter to send"), left out
    /// of a narrow composer.
    hint: bool,
    /// No submission is in flight.
    enabled: bool,
    width: Option<f64>,
    widest: f64,
    send: Callback<Message>,
}

/// The one composer, one rounded card, as its own component. A new task:
/// the project chip as the card's first row, then the field and the model
/// and access chips. An open Agent: the field, then the model-and-effort
/// chip. Usage and the one trailing button (Send, Stop, Steer or Resume)
/// sit at the right. A keystroke is stored here and redraws this card
/// only; an edit that also concerns the window (Enter's line break, an
/// open popover, an unresolved submission) and every control go to the
/// window.
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
        if input.popover || input.retry || enter_send_text(previous, text).is_some() {
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
        let _ = self.input.send.call(message);
    }

    fn view(&self, input: &ComposerInput, context: &mut ViewContext<Self>) -> View {
        let palette = input.palette;
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
            .on_text_changed(
                context.callback(|value| Message::Chat(ChatMsg::ComposerChanged(value))),
            );
        // WinUI's text box insets its text: the margin starts the text at
        // the card's 16-DIP inset, where the chips start.
        let mut field_cells = vec![KeyedView::new(
            "field",
            Border::new()
                .margin(Thickness::new(
                    -tokens::spacing::FIELD_LEADING_INSET,
                    0.0,
                    0.0,
                    0.0,
                ))
                .content(field),
        )];
        // A click into the field closes an open popover. WinUI's text box
        // keeps its presses and its mouse input never reaches the window's
        // hooks (observed live), so while a popover is open a clear layer
        // over the field takes the press, closes it and focuses the field.
        if input.dismiss {
            field_cells.push(KeyedView::new(
                "dismiss",
                Border::new()
                    .background(Brush::Solid(Color::transparent()))
                    .on_pointer_pressed(context.callback(|_: PointerEventInfo| {
                        Message::Launch(LaunchMsg::ComposerPointer {
                            over: true,
                            pressed: true,
                        })
                    })),
            ));
        }
        let field = Grid::new().keyed_children(field_cells);
        // The design: + (attach files), then the chips, 6 DIPs apart.
        let chips = StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(tokens::spacing::CHIP_GAP)
            .children((
                m3::icon_button(palette)
                    .automation_name("Attach files")
                    .on_click(context.message(Message::Launch(LaunchMsg::AttachRequested)))
                    .content(m3::icon(m3::glyph::NEW_TASK))
                    .tooltip("Attach files"),
                input.chips.clone(),
            ));
        let bottom = Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .column_spacing(tokens::spacing::SM)
            .children((
                Border::new()
                    .vertical_alignment(VerticalAlignment::Center)
                    .content(chips),
                Border::new()
                    .grid_column(1)
                    .content(composer_actions(input, blank, context)),
            ));
        // Home: the project and worktree chips above the field; attached
        // files as chips over it.
        let home = input.agent.is_none();
        let mut rows: Vec<KeyedView> = Vec::new();
        if home {
            rows.push(KeyedView::new("project", input.project.clone()));
        }
        if !input.attached.is_empty() {
            rows.push(KeyedView::new(
                format!("attached:{}", input.attached.len()),
                attachment_chips(&input.attached, context, palette),
            ));
        }
        rows.push(KeyedView::new("field", field));
        rows.push(KeyedView::new("bottom", bottom));
        Border::new()
            .grid_row(5)
            .width(input.width)
            .max_width(input.widest)
            .horizontal_alignment(HorizontalAlignment::Center)
            .margin(Thickness::new(
                0.0,
                tokens::spacing::INSET,
                0.0,
                if home {
                    tokens::spacing::HOME_GROUP_GAP
                } else {
                    tokens::spacing::LG
                },
            ))
            .content(m3::composer_card(
                palette,
                Grid::new(),
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .keyed_children(rows),
            ))
    }
}

/// The one trailing button, as in Codex: Send; while a turn runs, Stop
/// (it interrupts that turn) with an empty field and Steer with text;
/// Resume for a stopped Agent. An unresolved submission shows Retry
/// instead, resubmitting its saved identity. Each is bound by the window
/// from the detail on screen. A stopped Agent has one Resume: the filled
/// button while the field is empty, the Send arrow (resume with the
/// message) once it has text.
fn composer_actions(
    input: &ComposerInput,
    draft_blank: bool,
    context: &mut ViewContext<Composer>,
) -> View {
    let (palette, enabled) = (input.palette, input.enabled);
    let primary: View = match &input.primary {
        Primary::Retry(agent) => m3::filled(palette)
            .is_enabled(enabled)
            .automation_name("Retry (Enter)")
            .on_click(context.message(Message::Chat(ChatMsg::RetryPending(agent.clone()))))
            .content(m3::label("Retry"))
            .tooltip("Retry with the same id"),
        Primary::Resume(agent, generation) => {
            let resume = context.message(Message::Chat(ChatMsg::DraftRequested(
                DraftAction::Resume {
                    agent: agent.clone(),
                    generation: *generation,
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
                    .content(m3::send_icon())
                    .tooltip("Resume with this message")
            }
        }
        Primary::ReadingResume => m3::filled(palette)
            .is_enabled(false)
            .automation_name("Resume")
            .content(m3::label("Resume")),
        Primary::ReadingRunning => m3::stop_button(palette)
            .is_enabled(false)
            .automation_name("Stop the running turn")
            .content(m3::stop_mark(palette)),
        // Codex's one trailing button: while a turn runs, Stop with an
        // empty field (it interrupts that turn) and Steer once there is
        // text; otherwise Send.
        Primary::Turn { agent, running } => match running {
            Some(bound) if draft_blank => m3::stop_button(palette)
                .is_enabled(enabled)
                .automation_name("Stop the running turn")
                .on_click(
                    context.message(Message::Chat(ChatMsg::InterruptRequested(bound.clone()))),
                )
                .content(m3::stop_mark(palette))
                .tooltip("Stop"),
            Some(bound) => m3::steer_button(palette)
                .is_enabled(enabled)
                .automation_name("Steer the running turn (Enter)")
                .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                    DraftAction::Steer(bound.clone()),
                ))))
                .content(m3::send_icon())
                .tooltip("Steer"),
            None => m3::send_button(palette)
                .is_enabled(enabled && !draft_blank)
                .automation_name("Send (Enter)")
                .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                    DraftAction::Send {
                        agent: agent.clone(),
                    },
                ))))
                .content(m3::send_icon())
                .tooltip("Send"),
        },
        Primary::Start => m3::send_button(palette)
            .is_enabled(enabled && !draft_blank)
            .automation_name("Start task (Enter)")
            .on_click(context.message(Message::Launch(LaunchMsg::LaunchRequested)))
            .content(m3::send_icon())
            .tooltip("Send"),
    };
    // The design's hint before the ring and the FAB.
    let hint = match &input.primary {
        Primary::Retry(_) => "",
        Primary::Resume(..) | Primary::ReadingResume => "Enter resumes",
        Primary::ReadingRunning => "Stop the turn",
        Primary::Turn {
            running: Some(_), ..
        } => {
            if draft_blank {
                "Stop the turn"
            } else {
                "Enter steers the running turn"
            }
        }
        _ => "Enter to send",
    };
    let hint: View = if input.hint && !hint.is_empty() {
        m3::secondary(hint)
            .vertical_alignment(VerticalAlignment::Center)
            .into()
    } else {
        View::empty()
    };
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::ROW)
        .children((hint, input.usage.clone(), primary))
}

/// Files attached in the composer: one outlined chip each (file glyph,
/// name, the full path in its tooltip); a click removes it.
fn attachment_chips(
    files: &[String],
    context: &mut ViewContext<Composer>,
    palette: Palette,
) -> View {
    let chips: Vec<KeyedView> = files
        .iter()
        .map(|path| {
            let name = crate::attachments::file_name(path);
            KeyedView::new(
                path.clone(),
                m3::chip(palette)
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
                                m3::chip_content(
                                    Some(m3::glyph::FILE),
                                    name,
                                    false,
                                    tokens::layout::PROJECT_CHIP_WIDTH,
                                ),
                                m3::small_icon(m3::glyph::CLOSE),
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
}
