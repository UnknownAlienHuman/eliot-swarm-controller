//! The conversation: a one-line Agent header with its "…" menu, the
//! retained history and live delivery, pending requests, and the one
//! composer (context chips, the field, model/access chips and Send, with
//! Interrupt while a turn runs).
//!
//! Enter sends and Shift+Enter keeps the line break: the keyboard hook
//! ([`crate::shortcuts::take_plain_enter`]) tells the composer's text change
//! that came from Enter without Shift, and that change submits instead.
//! While a turn runs, Send steers exactly that turn (the Codex composer's
//! behaviour); a steer refused because the turn just ended (or a harness
//! without steering) goes as the next message, said in one line. The
//! message shows in the conversation at once, with what it waits for, until
//! its turn starts.

mod composer;
mod drafts;
mod header;
mod live_feed;
mod transcript;

use super::move_ui;
use super::*;
use crate::chat::InputFailure;
use crate::theme::StatusTone;
use crate::theme::winui::Palette;
use transcript::starting_view;

#[derive(Clone, Debug)]
pub(super) enum ChatMsg {
    ComposerChanged(String),
    AnswerChanged(String, String, Vec<String>),
    AnswerUnchanged,
    SnapshotLoaded(u64, eliot_core::Result<Box<Snapshot>>),
    Delivered(u64, eliot_core::Result<Option<Box<Delivery>>>),
    BackoffDone(u64, bool),
    RereadLoaded(u64, Result<Box<Reread>, (eliot_core::Error, Invalidation)>),
    RereadRetry(u64),
    LoadOlderRequested,
    /// Bring the latest turn into view once it is realized (epoch, tries).
    FollowLatest(u64, u32),
    ToolsToggled(String, bool),
    CopyPart(String),
    CopyCode(String, usize),
    /// Send, Steer or Resume with the draft, bound when it was clicked.
    DraftRequested(DraftAction),
    SendSubmitted(Result<SendOutcome, SendFailure>),
    /// Interrupt the turn shown on the button (Agent, generation, turn).
    InterruptRequested(BoundAction),
    ApprovalRequested(UiPending, bool),
    AnswerRequested(UiPending),
    ControlSubmitted(String, eliot_core::Result<String>),
    /// A "…" menu entry (its label).
    AgentMenu(String),
    /// The Force stop banner's answer: true confirms the bound action.
    ForceStopAnswered(bool),
    /// Outcomes of the inputs still shown as waiting (epoch, per command).
    InputsChecked(u64, Vec<(String, Fallible<Option<InputFailure>>)>),
    /// Read the waiting inputs again (epoch).
    InputsRecheck(u64),
    /// Retry a local input that did not start (its part key).
    RetryInput(String),
    /// The `why` summary of the open lost Agent (Agent, generation).
    LossRead(String, i64, Fallible<eliot_core::api::DiagnoseResponse>),
}

/// Between two reads of inputs that still wait while the Agent is idle or
/// has ended (bounded: only while one is shown as waiting).
const INPUT_RECHECK_MS: u64 = 2_000;

fn can_answer(
    request: &UiPending,
    answers: &HashMap<String, eliot_core::requests::Answers>,
) -> bool {
    request
        .binding
        .request
        .as_ref()
        .and_then(|id| answers.get(id))
        .is_some_and(|answer| !answer.is_empty())
        && request.questions.iter().any(|question| {
            !question.is_secret && question.kind != eliot_core::requests::QuestionKind::Resource
        })
}

/// The Agent and generation a "…" entry acts on: the header shown when the
/// click is handled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct MenuBinding {
    agent: String,
    generation: i64,
}

/// "…" menu entries.
const MENU_DETAILS: &str = "Agent details";
const MENU_HANDOFF: &str = "Hand off or switch…";
const MENU_MOVE: &str = "Move under another task…";
const MENU_RESUME: &str = "Resume";
const MENU_DETACH: &str = "Detach";
const MENU_STOP: &str = "Stop";
const MENU_FORCE: &str = "Force stop…";

/// WinUI's own left text padding inside a TextBox
/// (`TextControlThemePadding`, 10,5,6,6).
const WINUI_TEXT_PADDING: f64 = 10.0;
/// Follow-latest retries while the newest turn is not realized yet.
const FOLLOW_TRIES: u32 = 20;
const FOLLOW_DELAY_MS: u64 = 50;

/// Admission refusals of a steer after which the text goes as the next
/// message: the turn ended or changed (`bad_state`, `stale_generation`), or
/// the harness has no native steering (`unsupported`). Each is definite; an
/// uncertain steer is never followed by a send.
const STEER_FALLBACK: [&str; 3] = ["bad_state", "stale_generation", "unsupported"];

/// The composer's text change that Enter (no Shift) made: the text before
/// it when the change only inserted one line break; `None` otherwise.
pub(super) fn enter_send_text(previous: &str, text: &str) -> Option<String> {
    let prefix = previous
        .chars()
        .zip(text.chars())
        .take_while(|(a, b)| a == b)
        .map(|(a, _)| a.len_utf8())
        .sum::<usize>();
    let suffix = previous[prefix..]
        .chars()
        .rev()
        .zip(text[prefix..].chars().rev())
        .take_while(|(a, b)| a == b)
        .map(|(a, _)| a.len_utf8())
        .sum::<usize>();
    let inserted = &text[prefix..text.len() - suffix];
    let removed = &previous[prefix..previous.len() - suffix];
    (removed.is_empty() && matches!(inserted, "\r" | "\n" | "\r\n")).then(|| previous.to_owned())
}

/// A draft submission that did not go through: the error, its code, and
/// what was submitted (to give the text back or send it after a steer).
#[derive(Clone, Debug)]
pub(super) struct SendFailure {
    pub(super) error: eliot_core::Error,
    pub(super) mutation_id: String,
    pub(super) text: String,
    pub(super) revision: u64,
    pub(super) action: DraftAction,
}

impl SendFailure {
    fn new(
        error: &eliot_core::Error,
        mutation_id: &str,
        text: &str,
        revision: u64,
        action: &DraftAction,
    ) -> Self {
        Self {
            error: error.clone(),
            mutation_id: mutation_id.to_owned(),
            text: text.to_owned(),
            revision,
            action: action.clone(),
        }
    }
}

fn settles<T>(outcome: &Result<T, SendFailure>) -> bool {
    outcome
        .as_ref()
        .err()
        .is_none_or(|failure| !crate::state::uncertain(&failure.error))
}

/// What the composer's draft is submitted as, with the binding captured at
/// click time: the Agent, and for Steer its generation and active turn, for
/// Resume its retained generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum DraftAction {
    Send { agent: String },
    Steer(BoundAction),
    Resume { agent: String, generation: i64 },
}

impl DraftAction {
    fn agent(&self) -> &str {
        match self {
            Self::Send { agent } | Self::Resume { agent, .. } => agent,
            Self::Steer(bound) => &bound.agent,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Send { .. } => "Send",
            Self::Steer(_) => "Steer",
            Self::Resume { .. } => "Resume",
        }
    }

    /// One ledger key per distinct action and draft revision.
    fn key(&self, revision: u64) -> String {
        match self {
            Self::Send { agent } => format!("send:{agent}:{revision}"),
            Self::Steer(bound) => format!(
                "steer:{}:{}:{}:{revision}",
                bound.agent,
                bound.generation,
                bound.turn.as_deref().unwrap_or_default()
            ),
            Self::Resume { agent, generation } => {
                format!("resume:{agent}:{generation}:{revision}")
            }
        }
    }
}

/// The selected Agent's conversation, its delivery chain and per-Agent drafts.
pub(super) struct ChatState {
    // Conversation of the selected Agent.
    pub(super) convo: Option<Conversation>,
    pub(super) detail: Option<UiDetail>,
    pub(super) epoch: u64,
    pub(super) depth: Depth,
    pub(super) live: LiveStatus,
    pub(super) live_task: Option<ComponentTask>,
    pub(super) failures: u32,
    pub(super) reread_inflight: bool,
    pub(super) invalidated: Invalidation,
    pub(super) docs: HashMap<u64, Doc>,
    pub(super) render_cache: RenderCache,
    pub(super) items: Rc<Vec<Rc<RenderedItem>>>,
    pub(super) items_revision: u64,
    pub(super) expanded: HashSet<String>,
    /// A button of the latest turn, rendered outside the virtualized
    /// repeater so it always exists: opening a conversation focuses it, and
    /// WinUI brings a focused control into view (Reactor 0.100.0 exposes no
    /// ScrollViewer offset or ChangeView).
    pub(super) end_anchor: ElementRef<Button>,
    /// The composer, focused again after input is admitted, a launch and New.
    pub(super) composer_ref: ElementRef<TextBox>,
    pub(super) pending_local: Option<(String, String, String)>,
    /// What the new Agent's first message waits for ("Starting Codex…"),
    /// carried from the launch to its conversation.
    pub(super) waiting_label: Option<String>,
    /// A read of the waiting inputs' outcomes is running.
    pub(super) inputs_checking: bool,
    /// Launches from this window: the Agent, its first input and what was
    /// submitted, so Retry of a start that did not happen starts it again.
    pub(super) launches: HashMap<String, (String, Submitted)>,
    /// The open conversation came from a search suggestion: the keyboard
    /// stays in the search box (its arrows, Enter and Esc), so the latest
    /// turn is not followed by moving focus.
    pub(super) opened_from_search: bool,
    // Mutations.
    pub(super) drafts: DraftStore,
    pub(super) answers: HashMap<String, eliot_core::requests::Answers>,
    /// The Force stop awaiting confirmation, bound at its menu click, with
    /// the title shown in the question.
    pub(super) confirm_force: Option<(BoundAction, String)>,
    /// The open lost Agent's loss in words, read once per generation from
    /// `diagnose` (`None` inside: nothing to say beyond Lost).
    pub(super) loss: Option<(String, i64, Option<String>)>,
}

impl DesktopApp {
    pub(super) fn chat_update(&mut self, msg: ChatMsg, context: &ComponentContext<Self>) {
        match msg {
            ChatMsg::ComposerChanged(text) => {
                let previous = self.composer_text();
                // The echo of a text the window set (a clear after Send, a
                // restore) is no edit: the draft revision stays.
                if text == previous {
                    return;
                }
                // Typing in the composer dismisses an open popover.
                self.close_popover();
                // Enter without Shift typed this line break: send instead.
                if crate::shortcuts::take_plain_enter()
                    && let Some(sent) = enter_send_text(&previous, &text)
                {
                    if !sent.trim().is_empty()
                        && let Some(primary) = self.primary_action()
                    {
                        self.update(primary, context);
                    }
                    return;
                }
                // Keep the text exactly as the TextBox reports it: a normalized
                // copy would differ, be written back, and move the caret (or end
                // an IME composition). Line endings are normalized at submission.
                if let Some(agent) = self.selected_agent_id() {
                    self.chat.drafts.set(&agent, text);
                } else {
                    self.launch.launch_text = text;
                    self.launch.launch_rev = self.launch.launch_rev.wrapping_add(1);
                }
            }
            ChatMsg::AnswerUnchanged => {}
            ChatMsg::AnswerChanged(request, question, values) => {
                let answers = self.chat.answers.entry(request).or_default();
                if values.is_empty() {
                    answers.remove(&question);
                } else if values != [""] || answers.contains_key(&question) {
                    answers.insert(question, eliot_core::requests::Answer { answers: values });
                }
            }
            ChatMsg::SnapshotLoaded(epoch, result) => {
                if epoch != self.chat.epoch {
                    return;
                }
                match result {
                    Ok(snapshot) => {
                        let snapshot = *snapshot;
                        let first_load = self.chat.items.is_empty();
                        self.chat.failures = 0;
                        self.chat.docs.extend(snapshot.docs);
                        self.chat.detail = Some(detail_from(&snapshot.detail));
                        self.check_loss(context);
                        self.sync_open_agent();
                        self.ensure_catalog(context);
                        let cursor = snapshot.coverage.cursor;
                        if let Some(convo) = self.chat.convo.as_mut() {
                            let missing =
                                convo.reset(snapshot.coverage, &snapshot.turns, &snapshot.events);
                            convo.apply_inputs(snapshot.inputs);
                            convo.set_stale(None);
                            self.chat.invalidated.merge(Invalidation {
                                inputs: missing
                                    .inputs
                                    .into_iter()
                                    .filter(|command| convo.missing_inputs().contains(command))
                                    .collect(),
                                ..Invalidation::default()
                            });
                        }
                        self.rebuild_items();
                        self.check_waiting_inputs(context);
                        if first_load {
                            // Opened at the newest turn, like a chat.
                            self.follow_latest(0, context);
                        }
                        self.set_status("Retained history loaded; following live events.");
                        self.follow_next(Next::Wake { after: cursor }, context);
                        self.queue_reread(Invalidation::default(), context);
                    }
                    Err(error) => {
                        let next = live::after_error(&error, self.chat.failures);
                        self.set_status(format!("History read failed: {}", error.user_message()));
                        match next {
                            // A snapshot that cannot read its own window retries later.
                            Next::Resnapshot { reason } => self.follow_next(
                                Next::Backoff {
                                    delay_ms: live::backoff_ms(self.chat.failures),
                                    attempt: self.chat.failures + 1,
                                    reason,
                                },
                                context,
                            ),
                            other => self.follow_next(other, context),
                        }
                    }
                }
            }
            ChatMsg::Delivered(epoch, result) => {
                if epoch != self.chat.epoch {
                    return;
                }
                match result {
                    Ok(None) => {}
                    Ok(Some(delivery)) => {
                        let delivery = *delivery;
                        self.chat.failures = 0;
                        self.chat.docs.extend(delivery.docs);
                        let mut resnapshot = false;
                        if let Some(convo) = self.chat.convo.as_mut() {
                            let invalidation = convo.apply_events(&delivery.events);
                            convo.advance_cursor(delivery.next);
                            resnapshot = invalidation.generation_changed;
                            if !delivery.events.is_empty() {
                                self.rebuild_items();
                                self.queue_reread(invalidation, context);
                            }
                        }
                        let next = if resnapshot {
                            Next::Resnapshot {
                                reason: "a new run started".to_owned(),
                            }
                        } else {
                            live::after_page(
                                delivery.after,
                                delivery.next,
                                delivery.truncated,
                                delivery.lagged,
                            )
                        };
                        self.follow_next(next, context);
                    }
                    Err(error) => {
                        let next = live::after_error(&error, self.chat.failures);
                        self.follow_next(next, context);
                    }
                }
            }
            ChatMsg::BackoffDone(epoch, proceed) => {
                if epoch == self.chat.epoch && proceed {
                    // Reconnect: capture a new cursor and reload the reads.
                    self.start_snapshot(context);
                }
            }
            ChatMsg::RereadLoaded(epoch, result) => {
                if epoch != self.chat.epoch {
                    return;
                }
                self.chat.reread_inflight = false;
                match result {
                    Ok(reread) => {
                        let reread = *reread;
                        self.chat.docs.extend(reread.docs);
                        if let Some(detail) = reread.detail.as_ref() {
                            self.chat.detail = Some(detail_from(detail));
                            self.check_loss(context);
                            self.sync_open_agent();
                            self.ensure_catalog(context);
                        }
                        if let Some(convo) = self.chat.convo.as_mut() {
                            if let Some(turns) = reread.turns.as_deref() {
                                convo.apply_turns(turns);
                            }
                            convo.apply_inputs(reread.inputs);
                        }
                        self.rebuild_items();
                        self.check_waiting_inputs(context);
                        self.queue_reread(Invalidation::default(), context);
                    }
                    Err((error, work)) => {
                        self.set_status(format!(
                            "Readback failed, retrying: {}",
                            error.user_message()
                        ));
                        self.chat.invalidated.merge(work);
                        let delay = live::backoff_ms(self.chat.failures.min(4));
                        let _task = context.spawn_background(move |cancel| {
                            let cancelled = || cancel.is_cancelled();
                            let _ = feed::backoff(delay, &cancelled);
                            Message::Chat(ChatMsg::RereadRetry(epoch))
                        });
                    }
                }
            }
            ChatMsg::RereadRetry(epoch) => {
                if epoch == self.chat.epoch {
                    self.queue_reread(Invalidation::default(), context);
                }
            }
            ChatMsg::LoadOlderRequested => {
                self.chat.depth = self.chat.depth.older();
                self.set_status("Loading older retained history…");
                self.start_snapshot(context);
            }
            ChatMsg::FollowLatest(epoch, tries) => {
                if epoch != self.chat.epoch {
                    return;
                }
                if self.chat.opened_from_search {
                    // Focus would leave the search box and close its list.
                } else if self.chat.end_anchor.request_focus() {
                    // The latest turn is in view; typing goes on in the composer.
                    let _ = self.chat.composer_ref.request_focus();
                } else if tries < FOLLOW_TRIES && !self.chat.items.is_empty() {
                    self.follow_latest(tries + 1, context);
                }
            }
            ChatMsg::ToolsToggled(key, expanded) => {
                if expanded {
                    self.chat.expanded.insert(key);
                } else {
                    self.chat.expanded.remove(&key);
                }
            }
            ChatMsg::InputsChecked(epoch, results) => {
                if epoch != self.chat.epoch {
                    return;
                }
                self.chat.inputs_checking = false;
                let mut pending = false;
                if let Some(convo) = self.chat.convo.as_mut() {
                    for (command, result) in results {
                        match result {
                            Ok(Some(failure)) => convo.set_local_failure(&command, failure),
                            Ok(None) | Err(_) => pending = true,
                        }
                    }
                }
                self.rebuild_items();
                if pending {
                    let _task = context.spawn_background(move |cancel| {
                        let cancelled = || cancel.is_cancelled();
                        let _ = feed::backoff(INPUT_RECHECK_MS, &cancelled);
                        Message::Chat(ChatMsg::InputsRecheck(epoch))
                    });
                }
            }
            ChatMsg::InputsRecheck(epoch) => {
                if epoch == self.chat.epoch {
                    self.check_waiting_inputs(context);
                }
            }
            ChatMsg::RetryInput(key) => {
                let command = key.strip_prefix("fail:").unwrap_or(&key).to_owned();
                self.retry_input(&command, context);
            }
            ChatMsg::LossRead(agent, generation, summary) => {
                if self
                    .chat
                    .loss
                    .as_ref()
                    .is_some_and(|(read, at, _)| *read == agent && *at == generation)
                {
                    // Only a loss Swarm caused itself gets its own words; any
                    // other cause stays in Agent details ("What went wrong").
                    let words = summary
                        .ok()
                        .filter(monitor::restart_loss)
                        .map(|_| monitor::RESTART_LOSS_WORDS.to_owned());
                    self.chat.loss = Some((agent, generation, words));
                }
            }
            ChatMsg::CopyPart(key) => {
                let text = self
                    .chat
                    .convo
                    .as_ref()
                    .and_then(|convo| convo.part_text(&key));
                match text.map(|text| (text.chars().count(), crate::clipboard::set_text(&text))) {
                    Some((count, Ok(()))) => self.set_status(format!("Copied {count} characters.")),
                    Some((_, Err(error))) => self.alert(format!("Copy failed: {error}")),
                    None => self.alert("That message is no longer loaded."),
                }
            }
            ChatMsg::CopyCode(key, index) => {
                let code = self
                    .chat
                    .convo
                    .as_ref()
                    .and_then(|convo| convo.part_text(&key))
                    .map(|text| crate::markdown::parse(&text))
                    .and_then(|doc| doc.code_blocks().get(index).map(|code| (*code).to_owned()));
                match code.map(|code| crate::clipboard::set_text(&code)) {
                    Some(Ok(())) => self.set_status("Copied code block."),
                    Some(Err(error)) => self.alert(format!("Copy failed: {error}")),
                    None => self.alert("That code block is no longer loaded."),
                }
            }
            ChatMsg::DraftRequested(action) => {
                if self.submitting() {
                    return;
                }
                if self.data_dir.is_err() {
                    return;
                }
                let agent_id = action.agent().to_owned();
                let text = profiles::normalize_newlines(self.chat.drafts.get(&agent_id));
                // A plain Resume needs no text; Send and Steer do.
                let blank = text.trim().is_empty();
                if blank && !matches!(action, DraftAction::Resume { .. }) {
                    self.alert(format!(
                        "Type a message before {}.",
                        if matches!(action, DraftAction::Steer(_)) {
                            "steering"
                        } else {
                            "sending"
                        }
                    ));
                    return;
                }
                let revision = self.chat.drafts.revision(&agent_id);
                let key = action.key(revision);
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                let what = action.label();
                self.alert = None;
                self.set_status(format!("{what} ({mutation_id})…"));
                // At once: the composer empties (same revision, so a retry
                // keeps this id) and the message shows with what it waits for.
                self.chat.drafts.ack(&agent_id, revision);
                if !blank
                    && let Some(convo) = self.chat.convo.as_mut()
                    && convo.agent() == agent_id
                {
                    convo.note_local_input(&mutation_id, &text);
                    convo.set_local_note(
                        &mutation_id,
                        Some(
                            if matches!(action, DraftAction::Steer(_)) {
                                "Steering the running turn…"
                            } else {
                                "Sending…"
                            }
                            .to_owned(),
                        ),
                    );
                }
                self.rebuild_items();
                self.follow_latest(0, context);
                self.spawn_draft(action, mutation_id, text, revision, blank, false, context);
            }
            ChatMsg::SendSubmitted(outcome) => self.send_submitted(outcome, context),
            ChatMsg::InterruptRequested(bound) => {
                let turn = bound.turn.clone().unwrap_or_default();
                let key = format!("interrupt:{}:{}:{turn}", bound.agent, bound.generation);
                self.submit_control(context, "Interrupt", key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_interrupt(
                        data_dir,
                        mutation_id,
                        &bound.agent,
                        bound.generation,
                        turn,
                    ))
                    .map(|reply| submit_note(&reply))
                });
            }
            ChatMsg::ApprovalRequested(request, approve) => {
                let verb = if approve { "Approve" } else { "Deny" };
                let key = format!(
                    "{verb}:{}:{}",
                    request.binding.agent,
                    request.binding.request.clone().unwrap_or_default()
                );
                self.submit_control(context, verb, key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_approval(
                        data_dir,
                        mutation_id,
                        client::ApprovalInput {
                            agent: request.binding.agent.clone(),
                            generation: request.binding.generation,
                            request: request.binding.request.clone().unwrap_or_default(),
                            approve,
                            choice: None,
                            feedback: None,
                        },
                    ))
                    .map(|reply| submit_note(&reply))
                });
            }
            ChatMsg::AnswerRequested(request) => {
                if !can_answer(&request, &self.chat.answers) {
                    return;
                }
                let answer = self
                    .chat
                    .answers
                    .get(request.binding.request.as_deref().unwrap_or_default())
                    .cloned()
                    .unwrap_or_default();
                let answers = serde_json::to_value(&answer).expect("answer map encodes");
                let answer = answers.to_string();
                // The payload is part of the action: edited answers are a new
                // action with a new id, never a same-id retry with other data.
                let key = format!(
                    "answer:{}:{}:{:x}",
                    request.binding.agent,
                    request.binding.request.clone().unwrap_or_default(),
                    crate::chat::text_hash(&answer)
                );
                self.submit_control(context, "Answer", key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_answer(
                        data_dir,
                        mutation_id,
                        &request.binding.agent,
                        request.binding.generation,
                        request.binding.request.as_deref().unwrap_or_default(),
                        answers,
                    ))
                    .map(|reply| submit_note(&reply))
                });
            }
            ChatMsg::ControlSubmitted(what, outcome) => {
                self.settle_mutation(
                    outcome
                        .as_ref()
                        .err()
                        .is_none_or(|error| !crate::state::uncertain(error)),
                );
                match outcome {
                    Ok(state) => self.set_status(format!("{what} admitted: {state}.")),
                    Err(error) if crate::state::uncertain(&error) => self.alert(format!(
                        "{what}: not confirmed (trying again reuses its id): {}",
                        error.user_message()
                    )),
                    Err(error) => self.alert(format!("{what} refused: {}", error.user_message())),
                }
                if self.chat.convo.is_some() {
                    self.queue_reread(
                        Invalidation {
                            detail: true,
                            ..Invalidation::default()
                        },
                        context,
                    );
                }
            }
            ChatMsg::AgentMenu(label) => {
                // Bound now, to the Agent and generation the header shows.
                let binding = self.selected_agent_id().zip(self.chat.detail.as_ref()).map(
                    |(agent, detail)| MenuBinding {
                        agent,
                        generation: detail.generation,
                    },
                );
                if let Some(binding) = binding {
                    self.agent_menu(&label, binding, context);
                }
            }
            ChatMsg::ForceStopAnswered(confirmed) => {
                let Some((bound, _)) = self.chat.confirm_force.take() else {
                    return;
                };
                if !confirmed {
                    return;
                }
                let key = format!("stop:{}:{}:true", bound.agent, bound.generation);
                self.submit_control(context, "Force stop", key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_stop(
                        data_dir,
                        mutation_id,
                        &bound.agent,
                        bound.generation,
                        true,
                    ))
                    .map(|reply| submit_note(&reply))
                });
            }
        }
    }

    pub(super) fn submit_control(
        &mut self,
        context: &ComponentContext<Self>,
        what: &str,
        key: String,
        work: impl FnOnce(&Path, String) -> eliot_core::Result<String> + Send + 'static,
    ) {
        if self.submitting() {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let mutation_id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        self.alert = None;
        self.set_status(format!("{what} ({mutation_id})…"));
        let what = what.to_owned();
        let _task = context.spawn_background(move |_| {
            Message::Chat(ChatMsg::ControlSubmitted(
                what,
                work(&data_dir, mutation_id),
            ))
        });
    }

    /// The main area for a conversation: the header, the transcript (or the
    /// empty state of a new task), pending requests and the composer.
    pub(super) fn chat_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let agent = self.selected_agent_id();
        let column = self.reading_width();
        let header: View = match agent.as_deref() {
            Some(agent) => self.header_view(agent, column, context, palette),
            None => View::empty(),
        };
        let body: View = match agent {
            Some(_) => self.transcript_view(column, context, palette),
            // The start is being submitted: the message and what happens
            // now, at once (no empty pause after Send).
            None if self.launch.starting.is_some() => {
                let (text, label) = self.launch.starting.clone().unwrap_or_default();
                starting_view(&text, &label, column, palette)
            }
            None => {
                let project = (!self.launch.workspace.is_empty())
                    .then(|| self.place_title(&self.launch.workspace));
                let line = match project {
                    _ if self.launch.chat => "What would you like to chat about?".to_owned(),
                    Some(project) => format!("What should we build in {project}?"),
                    None => "What should we build?".to_owned(),
                };
                m3::page_title(line)
                    .grid_row(1)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .vertical_alignment(VerticalAlignment::Center)
                    .into()
            }
        };
        let pending = self.pending_view(column, context, palette);
        let alert: View = match (&self.chat.confirm_force, &self.alert) {
            (Some((bound, title)), _) => {
                self.force_stop_banner(bound, title, column, context, palette)
            }
            (None, Some(text)) => Border::new()
                .grid_row(4)
                .width(column)
                .max_width(tokens::layout::READING_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                .background(palette.card())
                .corner_radius(tokens::radius::CONTAINER)
                .padding(Thickness::new(
                    tokens::spacing::MD,
                    tokens::spacing::XS,
                    tokens::spacing::XS,
                    tokens::spacing::XS,
                ))
                .content(
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::SM)
                        .children((
                            m3::body(text.clone())
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_wrapping(TextWrapping::Wrap)
                                .is_text_selection_enabled(true),
                            m3::icon_button(palette)
                                .grid_column(1)
                                .automation_name("Dismiss")
                                .on_click(context.message(Message::Shell(ShellMsg::AlertDismissed)))
                                .content(m3::icon(m3::glyph::CLOSE)),
                        )),
                ),
            (None, None) => self
                .archived_notice(column, context, palette)
                .or_else(|| self.loss_notice(column))
                .unwrap_or_else(View::empty),
        };
        let composer = self.composer_view(column, context, palette);
        // An open popover overlays the rows above the composer (drawn after
        // them), so nothing moves when it opens; a click anywhere on those
        // rows outside it closes it.
        let popover = self.popover_view(column, context, palette);
        let dismiss: View = if self.launch.open.is_some() {
            Border::new()
                .grid_row(1)
                .grid_row_span(super::picker_ui::POPOVER_ROWS)
                .background(Brush::Solid(Color::transparent()))
                .on_pointer_pressed(
                    context.callback(|_: PointerEventInfo| Message::Launch(LaunchMsg::Close)),
                )
                .into()
        } else {
            View::empty()
        };

        // Rows: the header, the transcript, (spare), pending requests, the
        // notice, the composer; the popover spans the transcript to the
        // notice.
        Grid::new()
            .rows([
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
            ])
            .children((header, body, pending, alert, composer, dismiss, popover))
    }

    /// The reading column's width: the reading width, or the main area less
    /// its gutters when narrower; content-sized until the window size is
    /// observed. Inside the ScrollViewer a stretched MaxWidth column was
    /// arranged in a slot wider than the viewport (off center), so the
    /// column gets an explicit width.
    fn reading_width(&self) -> Option<f64> {
        self.main_width().map(|width| {
            (width - 2.0 * tokens::spacing::LG - tokens::spacing::MD)
                .clamp(tokens::layout::MIN_COLUMN, tokens::layout::READING_WIDTH)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::enter_send_text;

    #[test]
    fn answer_requires_edits_to_that_request() {
        let request = super::UiPending {
            binding: crate::state::BoundAction::request_control("a2", 1, "r1"),
            kind: "question".to_owned(),
            summary: "Choose".to_owned(),
            questions: vec![
                serde_json::from_value(serde_json::json!({"id":"q1", "kind":"text"})).unwrap(),
            ],
        };
        let mut answers = std::collections::HashMap::new();
        assert!(!super::can_answer(&request, &answers));
        answers.insert("r1".to_owned(), eliot_core::requests::Answers::new());
        assert!(!super::can_answer(&request, &answers));
        let edited = eliot_core::requests::Answers::from([(
            "q1".to_owned(),
            eliot_core::requests::Answer {
                answers: vec!["yes".to_owned()],
            },
        )]);
        answers.insert("r2".to_owned(), edited.clone());
        assert!(!super::can_answer(&request, &answers));
        answers.insert("r1".to_owned(), edited);
        assert!(super::can_answer(&request, &answers));
        answers.get_mut("r1").unwrap().clear();
        assert!(!super::can_answer(&request, &answers));
    }

    #[test]
    fn translated_send_failure_retains_the_same_mutation_id() {
        assert!(super::settles(&Ok::<_, super::SendFailure>(())));
        for code in ["unknown", "unavailable"] {
            let mut ledger = crate::state::MutationLedger::default();
            let action = super::DraftAction::Send {
                agent: "a2".to_owned(),
            };
            let key = action.key(7);
            let id = ledger.id_for(&key);
            let failure = super::SendFailure::new(
                &eliot_core::Error::new(code, "transport: reply missing"),
                &id,
                "hello",
                7,
                &action,
            );
            let result: Result<(), _> = Err(failure);
            if super::settles(&result) {
                ledger.complete(&key);
            }
            assert_eq!(ledger.id_for(&key), id);
        }
        let refused = super::SendFailure::new(
            &eliot_core::Error::new("invalid", "unknown: quoted model prose"),
            "id",
            "hello",
            7,
            &super::DraftAction::Send {
                agent: "a2".to_owned(),
            },
        );
        assert!(super::settles(&Err::<(), _>(refused)));
    }

    #[test]
    fn only_a_single_inserted_line_break_reads_as_enter() {
        assert_eq!(
            enter_send_text("hello", "hello\r").as_deref(),
            Some("hello")
        );
        assert_eq!(
            enter_send_text("hello", "hel\rlo").as_deref(),
            Some("hello")
        );
        assert_eq!(enter_send_text("", "\r").as_deref(), Some(""));
        assert_eq!(
            enter_send_text("héllo", "hé\r\nllo").as_deref(),
            Some("héllo")
        );
        assert_eq!(enter_send_text("hello", "hello!"), None);
        assert_eq!(enter_send_text("hello", "he\r"), None);
        assert_eq!(enter_send_text("a\rb", "a\r\rb").as_deref(), Some("a\rb"));
        assert_eq!(enter_send_text("hello", "hello\r\r"), None);
    }
}
