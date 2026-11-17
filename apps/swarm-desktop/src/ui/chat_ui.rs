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

use super::*;
use crate::chat::InputFailure;
use crate::configure::Pick;
use crate::theme::StatusTone;
use crate::theme::winui::Palette;

#[derive(Clone, Debug)]
pub(super) enum ChatMsg {
    ComposerChanged(String),
    AnswerChanged(String, String),
    SnapshotLoaded(u64, Fallible<Box<Snapshot>>),
    Delivered(u64, Fallible<Option<Box<Delivery>>>),
    BackoffDone(u64, bool),
    RereadLoaded(u64, Result<Box<Reread>, (String, Invalidation)>),
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
    ControlSubmitted(String, Fallible<String>),
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
    LossRead(String, i64, Fallible<String>),
}

/// Between two reads of inputs that still wait while the Agent is idle or
/// has ended (bounded: only while one is shown as waiting).
const INPUT_RECHECK_MS: u64 = 2_000;

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
const MENU_MANAGER: &str = "Change manager…";
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
    pub(super) error: String,
    pub(super) code: String,
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
            error: error.to_string(),
            code: error.code.clone(),
            mutation_id: mutation_id.to_owned(),
            text: text.to_owned(),
            revision,
            action: action.clone(),
        }
    }
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
    pub(super) answers: HashMap<String, String>,
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
            ChatMsg::AnswerChanged(request, text) => {
                self.chat.answers.insert(request, text);
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
                        let (code, text) = live::split_error(&error);
                        let next = live::after_error(code, text, self.chat.failures);
                        self.set_status(format!("History read failed: {error}"));
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
                                reason: "a newer generation started".to_owned(),
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
                        let (code, text) = live::split_error(&error);
                        let next = live::after_error(code, text, self.chat.failures);
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
                        self.set_status(format!("Readback failed, retrying: {error}"));
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
                        .filter(|summary| monitor::restart_loss(summary))
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
                    .map_err(|error| error.to_string())
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
                    .map_err(|error| error.to_string())
                });
            }
            ChatMsg::AnswerRequested(request) => {
                let answer = self
                    .chat
                    .answers
                    .get(request.binding.request.as_deref().unwrap_or_default())
                    .cloned()
                    .unwrap_or_default();
                let answers = match serde_json::from_str::<serde_json::Value>(&answer) {
                    Ok(answers) if answers.is_object() => answers,
                    _ => {
                        self.alert("Answers must be a JSON object keyed by question id.");
                        return;
                    }
                };
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
                    .map_err(|error| error.to_string())
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
                        "{what}: not confirmed (trying again reuses its id): {error}"
                    )),
                    Err(error) => self.alert(format!("{what} refused: {error}")),
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
                    .map_err(|error| error.to_string())
                });
            }
        }
    }

    /// Submit one draft action on a worker. `fallback` marks the send that
    /// follows a steer refused because the turn just ended.
    #[allow(clippy::too_many_arguments)]
    fn spawn_draft(
        &self,
        action: DraftAction,
        mutation_id: String,
        text: String,
        revision: u64,
        blank: bool,
        fallback: bool,
        context: &ComponentContext<Self>,
    ) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let agent_id = action.agent().to_owned();
        let what = action.label();
        let _task = context.spawn_background(move |_| {
            Message::Chat(ChatMsg::SendSubmitted(client::block_on(async {
                let id = mutation_id.clone();
                let submitted = match &action {
                    DraftAction::Send { agent } => {
                        client::submit_send(&data_dir, id, agent, &text).await
                    }
                    DraftAction::Steer(bound) => {
                        client::submit_steer(
                            &data_dir,
                            id,
                            &bound.agent,
                            bound.generation,
                            bound.turn.clone().unwrap_or_default(),
                            &text,
                        )
                        .await
                    }
                    DraftAction::Resume { agent, generation } => {
                        let text = (!blank).then_some(text.as_str());
                        client::submit_resume(&data_dir, id, agent, *generation, text).await
                    }
                };
                let (command_id, state, linked, recovered) = match submitted {
                    Ok(reply) => (
                        reply.command_id.clone(),
                        submit_note(&reply),
                        reply.linked_command.clone(),
                        false,
                    ),
                    Err(error) if matches!(error.code.as_str(), "unknown" | "unavailable") => {
                        // Uncertain reply: read the retained command
                        // instead of submitting again under a new id.
                        let command = client::read_command(&data_dir, &mutation_id)
                            .await
                            .map_err(|_| {
                                SendFailure::new(&error, &mutation_id, &text, revision, &action)
                            })?;
                        (
                            command.id,
                            format!("{:?}", command.state).to_lowercase(),
                            None,
                            true,
                        )
                    }
                    Err(error) => {
                        return Err(SendFailure::new(
                            &error,
                            &mutation_id,
                            &text,
                            revision,
                            &action,
                        ));
                    }
                };
                Ok(SendOutcome {
                    what,
                    agent: agent_id,
                    command_id,
                    linked,
                    draft_rev: revision,
                    text: if blank { String::new() } else { text },
                    state,
                    recovered,
                    fallback,
                })
            })))
        });
    }

    fn send_submitted(
        &mut self,
        outcome: Result<SendOutcome, SendFailure>,
        context: &ComponentContext<Self>,
    ) {
        // Keyboard focus goes back to the composer (no-op while it is unbound).
        let _ = self.chat.composer_ref.request_focus();
        let definite = outcome
            .as_ref()
            .err()
            .is_none_or(|failure| !crate::state::uncertain(&failure.error));
        self.settle_mutation(definite);
        match outcome {
            Ok(outcome) => {
                // Bound to the submitting Agent and draft revision, not
                // to whatever conversation is open now.
                self.chat.drafts.ack(&outcome.agent, outcome.draft_rev);
                // A SEND, or a resume's linked SEND, is new input of
                // this conversation; a steer joins the active turn.
                let input = match outcome.what {
                    "Send" | "Steer" => Some(outcome.command_id.as_str()),
                    "Resume" => outcome.linked.as_deref(),
                    _ => None,
                };
                let harness = self
                    .chat
                    .detail
                    .as_ref()
                    .map(|detail| crate::picker::harness_label(&detail.harness).to_owned())
                    .unwrap_or_else(|| "the Agent".to_owned());
                if let Some(command) = input
                    && !outcome.text.is_empty()
                    && let Some(convo) = self.chat.convo.as_mut()
                    && convo.agent() == outcome.agent
                {
                    convo.note_local_input(command, &outcome.text);
                    let note = match outcome.what {
                        "Steer" => "Sent to the running turn".to_owned(),
                        "Resume" => format!("Starting {harness}…"),
                        _ => format!("Waiting for {harness}…"),
                    };
                    convo.set_local_note(command, Some(note));
                }
                self.rebuild_items();
                if outcome.fallback {
                    self.alert("The turn had just ended, so the message went as the next one.");
                }
                self.set_status(format!(
                    "{} {} ({}){}{}.",
                    outcome.what,
                    outcome.state,
                    outcome.command_id,
                    outcome
                        .linked
                        .as_deref()
                        .map(|linked| format!("; its message is {linked}"))
                        .unwrap_or_default(),
                    if outcome.recovered {
                        ", confirmed by reading the retained command"
                    } else {
                        ""
                    }
                ));
            }
            Err(failure) => {
                if let Some(convo) = self.chat.convo.as_mut() {
                    convo.forget_local(&failure.mutation_id);
                }
                // A steer the turn no longer takes goes as the next message:
                // a new action (its own ledger key and id), after a definite
                // refusal only.
                if let DraftAction::Steer(bound) = &failure.action
                    && STEER_FALLBACK.contains(&failure.code.as_str())
                {
                    let action = DraftAction::Send {
                        agent: bound.agent.clone(),
                    };
                    let key = format!("{}:after-steer", action.key(failure.revision));
                    let mutation_id = self.ledger.id_for(&key);
                    self.in_flight_key = Some(key);
                    if let Some(convo) = self.chat.convo.as_mut()
                        && convo.agent() == bound.agent
                    {
                        convo.note_local_input(&mutation_id, &failure.text);
                        convo.set_local_note(&mutation_id, Some("Sending…".to_owned()));
                    }
                    self.rebuild_items();
                    self.spawn_draft(
                        action,
                        mutation_id,
                        failure.text,
                        failure.revision,
                        false,
                        true,
                        context,
                    );
                    return;
                }
                let agent = failure.action.agent().to_owned();
                self.chat
                    .drafts
                    .restore(&agent, failure.text.clone(), failure.revision);
                self.rebuild_items();
                if crate::state::uncertain(&failure.error) {
                    self.alert(format!(
                        "Not confirmed (sending again reuses the same id): {}",
                        failure.error
                    ));
                } else {
                    self.alert(format!("Not sent: {}", failure.error));
                }
            }
        }
    }

    /// What Send (the button, or Enter) does now: start the task, resume a
    /// stopped Agent, steer the running turn, or send the next message.
    pub(super) fn primary_action(&self) -> Option<Message> {
        let Some(agent) = self.selected_agent_id() else {
            return Some(Message::Launch(LaunchMsg::LaunchRequested));
        };
        let detail = self.chat.detail.as_ref()?;
        let resumable = matches!(detail.state.as_str(), "stopped" | "failed" | "lost")
            && detail.active_turn.is_none()
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

    /// One "…" menu entry, bound to the Agent and generation it was built for.
    fn agent_menu(&mut self, label: &str, binding: MenuBinding, context: &ComponentContext<Self>) {
        let bound = BoundAction::agent_control(&binding.agent, binding.generation);
        match label {
            MENU_DETAILS | MENU_MANAGER => self.show_details(&binding.agent, context),
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
                    .map_err(|error| error.to_string())
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
                    .map_err(|error| error.to_string())
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
                    .map_err(|error| error.to_string())
                });
            }
            MENU_FORCE => {
                let title = self.agent_title(&binding.agent);
                self.chat.confirm_force = Some((bound, title));
            }
            _ => {}
        }
    }

    /// Bring the newest turn into view after the next render.
    fn follow_latest(&self, tries: u32, context: &ComponentContext<Self>) {
        let epoch = self.chat.epoch;
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(FOLLOW_DELAY_MS, &cancelled);
            Message::Chat(ChatMsg::FollowLatest(epoch, tries))
        });
    }

    pub(super) fn composer_text(&self) -> String {
        match self.selected_agent_id() {
            Some(agent) => self.chat.drafts.get(&agent).to_owned(),
            None => self.launch.launch_text.clone(),
        }
    }

    pub(super) fn rebuild_items(&mut self) {
        let items = self
            .chat
            .convo
            .as_ref()
            .map(Conversation::items)
            .unwrap_or_default();
        let rendered = self.chat.render_cache.render(&items, &self.chat.docs);
        self.chat.items_revision = views::key_revision(&rendered);
        self.chat.items = Rc::new(rendered);
        let live: HashSet<u64> = items
            .iter()
            .flat_map(|item| item.parts.iter())
            .map(|part| crate::chat::text_hash(&part.text))
            .collect();
        self.chat.docs.retain(|hash, _| live.contains(hash));
    }

    /// Start (or restart) the conversation feed: new epoch, cancel the old
    /// live task, capture a cursor and reload the current reads.
    pub(super) fn start_snapshot(&mut self, context: &ComponentContext<Self>) {
        let Some(agent) = self
            .chat
            .convo
            .as_ref()
            .map(|convo| convo.agent().to_owned())
        else {
            return;
        };
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        self.chat.epoch += 1;
        // A failed-input check in flight belongs to the old epoch: its reply
        // is discarded, so the flag must not keep blocking new checks.
        self.chat.inputs_checking = false;
        if let Some(task) = self.chat.live_task.take() {
            task.cancel();
        }
        self.chat.reread_inflight = false;
        self.chat.invalidated = Invalidation::default();
        if !matches!(self.chat.live, LiveStatus::Stale(_)) {
            self.chat.live = LiveStatus::Loading;
        }
        let epoch = self.chat.epoch;
        let depth = self.chat.depth;
        let _task = context.spawn_background(move |_| {
            Message::Chat(ChatMsg::SnapshotLoaded(
                epoch,
                feed::snapshot(&data_dir, &agent, depth).map(Box::new),
            ))
        });
    }

    pub(super) fn spawn_delivery(
        &mut self,
        context: &ComponentContext<Self>,
        after: i64,
        wake: bool,
    ) {
        let Some(agent) = self
            .chat
            .convo
            .as_ref()
            .map(|convo| convo.agent().to_owned())
        else {
            return;
        };
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let epoch = self.chat.epoch;
        self.chat.live_task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            Message::Chat(ChatMsg::Delivered(
                epoch,
                feed::deliver(&data_dir, &agent, after, wake, &cancelled)
                    .map(|delivery| delivery.map(Box::new)),
            ))
        }));
    }

    pub(super) fn spawn_backoff(&mut self, context: &ComponentContext<Self>, delay_ms: u64) {
        let epoch = self.chat.epoch;
        self.chat.live_task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            Message::Chat(ChatMsg::BackoffDone(
                epoch,
                feed::backoff(delay_ms, &cancelled),
            ))
        }));
    }

    /// Coalesce authoritative rereads: at most one in flight; invalidations
    /// that arrive meanwhile merge into the next one.
    pub(super) fn queue_reread(
        &mut self,
        invalidation: Invalidation,
        context: &ComponentContext<Self>,
    ) {
        self.chat.invalidated.merge(invalidation);
        if self.chat.reread_inflight || !self.chat.invalidated.any() {
            return;
        }
        let (Some(agent), Ok(data_dir)) = (
            self.chat
                .convo
                .as_ref()
                .map(|convo| convo.agent().to_owned()),
            self.data_dir.clone(),
        ) else {
            return;
        };
        let work = std::mem::take(&mut self.chat.invalidated);
        let limit = self.chat.depth.result_limit;
        let epoch = self.chat.epoch;
        self.chat.reread_inflight = true;
        let _task = context.spawn_background(move |_| {
            let result = feed::reread(
                &data_dir,
                &agent,
                limit,
                work.turns,
                work.detail,
                work.inputs.clone(),
            );
            // A failed reread hands its work back so it is retried, not lost.
            Message::Chat(ChatMsg::RereadLoaded(
                epoch,
                result.map(Box::new).map_err(|error| (error, work)),
            ))
        });
    }

    pub(super) fn follow_next(&mut self, next: Next, context: &ComponentContext<Self>) {
        match next {
            Next::Wake { after } => {
                self.chat.live = LiveStatus::Live;
                self.spawn_delivery(context, after, true);
            }
            Next::Drain { after } => self.spawn_delivery(context, after, false),
            Next::Resnapshot { reason } => {
                if let Some(convo) = self.chat.convo.as_mut() {
                    convo.set_stale(Some(reason.clone()));
                }
                self.chat.live = LiveStatus::Stale(reason);
                self.start_snapshot(context);
            }
            Next::Backoff {
                delay_ms,
                attempt,
                reason,
            } => {
                self.chat.failures = attempt;
                self.chat.live = LiveStatus::Reconnecting { attempt, reason };
                self.spawn_backoff(context, delay_ms);
            }
            Next::Stop { reason } => {
                self.chat.live = LiveStatus::Stopped(reason);
                self.chat.live_task = None;
            }
        }
    }

    /// Inputs shown as waiting while the Agent is idle or has ended may
    /// never start: read their commands, and show why (`swarm why`, its
    /// origin and Retry) instead of an indefinite spinner.
    fn check_waiting_inputs(&mut self, context: &ComponentContext<Self>) {
        if self.chat.inputs_checking {
            return;
        }
        let (Some(detail), Some(convo), Ok(data_dir)) = (
            self.chat.detail.as_ref(),
            self.chat.convo.as_ref(),
            self.data_dir.clone(),
        ) else {
            return;
        };
        let ended = matches!(detail.state.as_str(), "failed" | "stopped" | "lost");
        if detail.active_turn.is_some() || !(ended || detail.state == "idle") {
            return;
        }
        let waiting = convo.waiting_locals();
        if waiting.is_empty() {
            return;
        }
        let agent = convo.agent().to_owned();
        let epoch = self.chat.epoch;
        self.chat.inputs_checking = true;
        let _task = context.spawn_background(move |_| {
            let results = waiting
                .into_iter()
                .take(4)
                .map(|command| {
                    let read =
                        client::block_on(client::input_failure(&data_dir, &agent, &command, ended))
                            .map_err(|error| error.to_string());
                    (command, read)
                })
                .collect();
            Message::Chat(ChatMsg::InputsChecked(epoch, results))
        });
    }

    /// Read why the open Agent is lost, once per generation: a loss to a
    /// Swarm restart reads "Stopped by a Swarm restart" with Resume.
    fn check_loss(&mut self, context: &ComponentContext<Self>) {
        let (Some(detail), Some(agent), Ok(data_dir)) = (
            self.chat.detail.as_ref(),
            self.selected_agent_id(),
            self.data_dir.clone(),
        ) else {
            return;
        };
        if detail.state != "lost" || detail.observe_only {
            self.chat.loss = None;
            return;
        }
        let generation = detail.generation;
        if self
            .chat
            .loss
            .as_ref()
            .is_some_and(|(read, at, _)| *read == agent && *at == generation)
        {
            return;
        }
        self.chat.loss = Some((agent.clone(), generation, None));
        let _task = context.spawn_background(move |_| {
            let summary = client::block_on(client::diagnose(&data_dir, &agent))
                .map(|reply| reply.summary)
                .map_err(|error| error.to_string());
            Message::Chat(ChatMsg::LossRead(agent, generation, summary))
        });
    }

    /// "Stopped by a Swarm restart · Resume" above the composer, bound to
    /// the generation shown.
    fn loss_notice(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> Option<View> {
        let detail = self.chat.detail.as_ref()?;
        let agent = self.selected_agent_id()?;
        let (read, generation, words) = self.chat.loss.as_ref()?;
        let words = words.as_deref()?;
        if *read != agent || *generation != detail.generation || detail.state != "lost" {
            return None;
        }
        Some(
            Border::new()
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
                .automation_name(format!("{words} · Resume"))
                .content(
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::SM)
                        .children((
                            m3::body(words.to_owned())
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                            m3::text_button(palette)
                                .grid_column(1)
                                .is_enabled(!self.submitting())
                                .automation_name(format!(
                                    "Resume after the Swarm restart (generation {generation})"
                                ))
                                .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                                    DraftAction::Resume {
                                        agent,
                                        generation: *generation,
                                    },
                                ))))
                                .content(m3::label("Resume")),
                        )),
                ),
        )
    }

    /// Retry a local input that definitely did not start: a launch that
    /// never ran starts again as submitted (a new task, new mutation id);
    /// any other input goes back through Send (or Resume) with its text.
    fn retry_input(&mut self, command: &str, context: &ComponentContext<Self>) {
        let Some(convo) = self.chat.convo.as_mut() else {
            return;
        };
        let Some(text) = convo.local_text(command) else {
            return;
        };
        let agent = convo.agent().to_owned();
        convo.forget_local(command);
        self.rebuild_items();
        match self.chat.launches.get(&agent).cloned() {
            Some((first, submitted)) if first == command => {
                self.new_task(context);
                self.launch.choice = submitted.choice;
                self.launch.access = Some(submitted.access);
                self.launch.chat = submitted.chat;
                self.launch.workspace = submitted.workspace;
                self.launch.launch_text = text;
                self.launch.launch_rev = self.launch.launch_rev.wrapping_add(1);
                self.update(Message::Launch(LaunchMsg::LaunchRequested), context);
            }
            _ => {
                self.chat.drafts.set(&agent, text);
                if let Some(primary) = self.primary_action() {
                    self.update(primary, context);
                }
            }
        }
    }

    pub(super) fn open_conversation(&mut self, agent: &UiAgent, context: &ComponentContext<Self>) {
        self.nav.selection.select_agent(Some(agent.id.clone()));
        // Configure catalogue and picks for one Agent never carry over.
        self.configure = configure_ui::ConfigureState::default();
        self.chat.convo = Some(Conversation::new(&agent.id));
        self.chat.detail = None;
        self.chat.depth = Depth::default();
        self.chat.failures = 0;
        self.chat.items = Rc::new(Vec::new());
        self.chat.live = LiveStatus::Loading;
        self.chat.confirm_force = None;
        self.alert = None;
        if let Some((target, command, text)) = self.chat.pending_local.take() {
            if target == agent.id
                && let Some(convo) = self.chat.convo.as_mut()
            {
                convo.note_local_input(&command, &text);
                convo.set_local_note(&command, self.chat.waiting_label.take());
            } else {
                self.chat.pending_local = Some((target, command, text));
            }
        }
        self.rebuild_items();
        self.set_status(format!("Reading retained history of {}…", agent.id));
        self.start_snapshot(context);
    }

    pub(super) fn submit_control(
        &mut self,
        context: &ComponentContext<Self>,
        what: &str,
        key: String,
        work: impl FnOnce(&Path, String) -> Fallible<String> + Send + 'static,
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
                let project = self
                    .selected_project()
                    .map(|project| self.project_label(project))
                    .or_else(|| {
                        (!self.launch.workspace.is_empty())
                            .then(|| nav::project_name(&self.launch.workspace, &[]))
                    });
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
                .or_else(|| self.loss_notice(column, context, palette))
                .unwrap_or_else(View::empty),
        };
        let composer = self.composer_view(column, context, palette);
        // An open popover sits in its own row right above the composer (the
        // empty area above gives way); a click on that area closes it.
        let popover = self.popover_view(column, context, palette);
        let dismiss: View = if self.launch.open.is_some() {
            Border::new()
                .grid_row(1)
                .background(Brush::Solid(Color::transparent()))
                .on_pointer_pressed(
                    context.callback(|_: PointerEventInfo| Message::Launch(LaunchMsg::Close)),
                )
                .into()
        } else {
            View::empty()
        };

        Grid::new()
            .rows([
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
            ])
            .children((
                header,
                body,
                dismiss,
                pending,
                alert,
                popover,
                composer,
                self.handoff_dialog(context),
            ))
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

    /// One line: the title, the status dot, a connection note when not live,
    /// then the details toggle and the "…" menu.
    fn header_view(
        &self,
        agent: &str,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let title = self.agent_title(agent);
        let detail = self.chat.detail.as_ref();
        let state = detail.map_or("reading", |detail| detail.state.as_str());
        let tone = StatusTone::of_state(state);
        let note = match &self.chat.live {
            LiveStatus::Stale(_) => "Catching up…",
            LiveStatus::Reconnecting { .. } => "Reconnecting…",
            LiveStatus::Stopped(_) => "Live updates stopped",
            _ => "",
        };
        let menu: View = match detail {
            Some(detail) => {
                let ended = matches!(detail.state.as_str(), "stopped" | "failed" | "lost");
                let controllable = !detail.observe_only;
                let mut items = vec![
                    MenuItem::item("details", MENU_DETAILS),
                    MenuItem::separator("s1"),
                ];
                if !self.submitting() {
                    items.push(MenuItem::item("handoff", MENU_HANDOFF));
                    items.push(MenuItem::item("manager", MENU_MANAGER));
                    if controllable && ended && detail.active_turn.is_none() {
                        items.push(MenuItem::item("resume", MENU_RESUME));
                    }
                    if detail.observe_only {
                        items.push(MenuItem::item("detach", MENU_DETACH));
                    }
                    if controllable && !ended {
                        items.push(MenuItem::separator("s2"));
                        items.push(MenuItem::item("stop", MENU_STOP));
                        items.push(MenuItem::item("force", MENU_FORCE));
                    }
                } else {
                    items.push(MenuItem::disabled("busy", "Waiting for the last action…"));
                }
                m3::icon_button(palette)
                    .automation_name("More actions")
                    .content(m3::toolbar_icon(m3::glyph::MORE))
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
                .content(m3::toolbar_icon(m3::glyph::MORE)),
        };
        let details_open = self.panel == Panel::Details;
        Grid::new()
            .grid_row(0)
            .columns([
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
                GridLength::Auto,
            ])
            .column_spacing(tokens::spacing::SM)
            // On the reading column, so the title starts and the toolbar
            // ends where the messages and the composer do.
            .width(column)
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .margin(Thickness::xy(0.0, tokens::spacing::SM))
            .children((
                m3::section_title(title.clone())
                    .max_width(
                        column.unwrap_or(tokens::layout::READING_WIDTH)
                            - 2.0 * tokens::height::CONTROL
                            - 4.0 * tokens::spacing::SM
                            - tokens::layout::DOT,
                    )
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_trimming(TextTrimming::CharacterEllipsis)
                    .is_text_selection_enabled(true),
                Border::new()
                    .grid_column(1)
                    .vertical_alignment(VerticalAlignment::Center)
                    .automation_name(format!("State: {state}"))
                    .content(m3::dot(palette.header_dot(tone))),
                m3::secondary(note)
                    .grid_column(2)
                    .vertical_alignment(VerticalAlignment::Center),
                m3::icon_button(palette)
                    .grid_column(4)
                    .automation_name(if details_open {
                        "Hide details"
                    } else {
                        "Show details"
                    })
                    .on_click(
                        context.message(Message::Shell(ShellMsg::PanelToggled(Panel::Details))),
                    )
                    .content(m3::toolbar_icon(m3::glyph::INFO)),
                Border::new().grid_column(5).content(menu),
            ))
    }

    /// The transcript: "Load older" when more history exists, then every
    /// turn in the reading column; the newest turn carries the end anchor.
    fn transcript_view(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let load_older: View = match self.chat.convo.as_ref() {
            Some(convo) if convo.coverage().can_load_older() => m3::text_button(palette)
                .is_enabled(!matches!(self.chat.live, LiveStatus::Loading))
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::MD))
                .on_click(context.message(Message::Chat(ChatMsg::LoadOlderRequested)))
                .content(m3::label("Load older messages")),
            _ => View::empty(),
        };
        let empty: View = if self.chat.items.is_empty() {
            m3::secondary(match self.chat.live {
                LiveStatus::Loading => "Loading…",
                _ => "No messages yet.",
            })
            .horizontal_alignment(HorizontalAlignment::Center)
            .into()
        } else {
            View::empty()
        };
        let callbacks = RowCallbacks {
            copy_part: context.callback(|value| Message::Chat(ChatMsg::CopyPart(value))),
            copy_code: context.callback(|(key, index): (String, usize)| {
                Message::Chat(ChatMsg::CopyCode(key, index))
            }),
            retry: context.callback(|key| Message::Chat(ChatMsg::RetryInput(key))),
            toggle_tools: context.callback(|(key, expanded): (String, bool)| {
                Message::Chat(ChatMsg::ToolsToggled(key, expanded))
            }),
        };
        // Every turn but the latest is virtualized; the latest is a plain
        // child that always exists and carries the end anchor.
        let earlier = self.chat.items.len().saturating_sub(1);
        let expanded = Rc::new(self.chat.expanded.clone());
        let latest: View = match self.chat.items.last() {
            Some(item) => StackPanel::new().keyed_children(vec![KeyedView::new(
                item.key.clone(),
                views::item_view(
                    item,
                    &expanded,
                    &callbacks,
                    palette,
                    Some(&self.chat.end_anchor),
                    column,
                ),
            )]),
            None => View::empty(),
        };
        let items = Rc::clone(&self.chat.items);
        let keys = Rc::clone(&self.chat.items);
        // A palette or width change re-realises the rows in the new colors
        // and at the new column width.
        let palette_code = palette
            .scheme()
            .map_or(0, |scheme| 1 + u64::from(scheme.dark));
        let width_code = column.map_or(0, |width| width.round() as u64);
        let source = VirtualSource::new(
            self.chat
                .items_revision
                .wrapping_mul(3)
                .wrapping_add(palette_code)
                .wrapping_add(width_code.wrapping_mul(0x9E37_79B9)),
            earlier,
            move |index| keys[index].key.clone(),
            move |index| {
                views::item_view(&items[index], &expanded, &callbacks, palette, None, column)
            },
        );
        ScrollViewer::new()
            .grid_row(1)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Conversation")
            .content(
                StackPanel::new()
                    .width(column)
                    .max_width(tokens::layout::READING_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .margin(Thickness::xy(0.0, tokens::spacing::MD))
                    .children((
                        load_older,
                        empty,
                        ItemsRepeater::new().virtual_source(source),
                        latest,
                    )),
            )
    }

    /// Pending requests of the open Agent: what it asks, and the answers.
    fn pending_view(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let pending_rows: Vec<KeyedView> = self
            .chat
            .detail
            .as_ref()
            .map(|detail| detail.pending.as_slice())
            .unwrap_or_default()
            .iter()
            .map(|request| {
                let key = request.binding.request.clone().unwrap_or_default();
                let answer_box = m3::field()
                    .text(self.chat.answers.get(&key).cloned().unwrap_or_default())
                    .placeholder_text("{\"question\": {\"answers\": [\"…\"]}}")
                    .automation_name("Answer JSON")
                    .on_text_changed({
                        let key = key.clone();
                        context.callback(move |text: String| {
                            Message::Chat(ChatMsg::AnswerChanged(key.clone(), text))
                        })
                    });
                let (fill, on) = palette.attention();
                KeyedView::new(
                    key,
                    Border::new()
                        .background(fill)
                        .corner_radius(tokens::radius::CONTAINER)
                        .padding(Thickness::uniform(tokens::spacing::MD))
                        .content(
                            StackPanel::new().spacing(tokens::spacing::SM).children((
                                m3::section_title(format!("The Agent asks: {}", request.kind))
                                    .foreground(on),
                                TextBlock::new()
                                    .text(request.summary.clone())
                                    .foreground(on)
                                    .text_wrapping(TextWrapping::Wrap)
                                    .is_text_selection_enabled(true),
                                answer_box,
                                StackPanel::new()
                                    .orientation(Orientation::Horizontal)
                                    .spacing(tokens::spacing::SM)
                                    .children((
                                        m3::filled(palette)
                                            .is_enabled(!self.submitting())
                                            .on_click(context.message(Message::Chat(
                                                ChatMsg::ApprovalRequested(request.clone(), true),
                                            )))
                                            .content(m3::label("Approve")),
                                        m3::outlined(palette)
                                            .is_enabled(!self.submitting())
                                            .on_click(context.message(Message::Chat(
                                                ChatMsg::ApprovalRequested(request.clone(), false),
                                            )))
                                            .content(m3::label("Deny")),
                                        m3::text_button(palette)
                                            .is_enabled(!self.submitting())
                                            .on_click(context.message(Message::Chat(
                                                ChatMsg::AnswerRequested(request.clone()),
                                            )))
                                            .content(m3::label("Answer")),
                                    )),
                            )),
                        ),
                )
            })
            .collect();
        StackPanel::new()
            .grid_row(3)
            .spacing(tokens::spacing::SM)
            .width(column)
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .keyed_children(pending_rows)
    }

    /// The one composer: the context row (project and profile), the field,
    /// then the model-and-effort and access chips with the actions on the
    /// right (Interrupt only while a turn runs; Send steers it).
    fn composer_view(
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
    pub(super) fn registered_projects(&self) -> Vec<(String, String)> {
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
            matches!(detail.state.as_str(), "stopped" | "failed" | "lost")
                && detail.active_turn.is_none()
                && !detail.observe_only
        });
        let draft_blank = self.composer_text().trim().is_empty();
        let enabled = !self.submitting();
        let primary: View = match (agent.clone(), detail) {
            (Some(agent), Some(detail)) if resumable => m3::filled(palette)
                .is_enabled(enabled)
                .automation_name(format!(
                    "{} (generation {})",
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

    /// The Force stop question above the composer, bound to the Agent and
    /// generation of its menu click. In the window (not a ContentDialog),
    /// so it always takes the window's theme.
    fn force_stop_banner(
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
            .background(palette.error_container())
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
                                    .foreground(palette.on_error_container())
                                    .text_trimming(TextTrimming::CharacterEllipsis),
                                m3::text(
                                    format!(
                                        "Ends Agent {} (generation {}) at once instead of a clean stop; an active turn is cut off.",
                                        bound.agent, bound.generation
                                    ),
                                    tokens::text::SECONDARY,
                                )
                                .foreground(palette.on_error_container())
                                .text_wrapping(TextWrapping::Wrap),
                            )),
                        StackPanel::new()
                            .grid_column(1)
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .vertical_alignment(VerticalAlignment::Center)
                            .children((
                                m3::error_text_button(palette)
                                    .automation_name("Cancel force stop")
                                    .on_click(context.message(Message::Chat(
                                        ChatMsg::ForceStopAnswered(false),
                                    )))
                                    .content(m3::label("Cancel")),
                                m3::destructive(palette)
                                    .is_enabled(!self.submitting())
                                    .automation_name(format!(
                                        "Force stop {} generation {}",
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

    /// The configure pick a model-chip menu label names, for `agent`.
    pub(super) fn configure_pick(&self, agent: &str, generation: i64, label: &str) -> Option<Pick> {
        let detail = self.chat.detail.as_ref()?;
        let catalog = self
            .configure
            .catalog
            .as_ref()
            .filter(|catalog| catalog.is_for(agent, generation))?;
        let reply = catalog.reply.as_ref().ok()?;
        let (mut models, mut efforts) = configure::options(reply, detail.settings.next_model());
        // A field the daemon refuses never resolves.
        if reply.configure.model_refusal.is_some() {
            models.clear();
        }
        if reply.configure.effort_refusal.is_some() {
            efforts.clear();
        }
        configure::resolve(label, &models, &efforts)
    }
}

/// A new task's first message and its "Starting Codex…" line while the
/// launch is submitted, in the transcript's place and shape.
fn starting_view(text: &str, label: &str, column: Option<f64>, palette: Palette) -> View {
    StackPanel::new()
        .grid_row(1)
        .width(column)
        .max_width(tokens::layout::READING_WIDTH)
        .horizontal_alignment(HorizontalAlignment::Center)
        .vertical_alignment(VerticalAlignment::Top)
        .margin(Thickness::xy(0.0, tokens::spacing::MD))
        .spacing(tokens::spacing::SM)
        .children((
            Border::new()
                .horizontal_alignment(HorizontalAlignment::Right)
                .max_width(tokens::layout::READING_WIDTH * tokens::layout::BUBBLE_SHARE)
                .background(palette.bubble())
                .corner_radius(tokens::radius::CONTAINER)
                .padding(Thickness::xy(tokens::spacing::INSET, tokens::spacing::SM))
                .automation_name(format!("You: {text}"))
                .content(
                    TextBlock::new()
                        .text(text.to_owned())
                        .foreground(palette.on_bubble())
                        .text_wrapping(TextWrapping::Wrap),
                ),
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .automation_name(label.to_owned())
                .children((
                    m3::secondary(label.to_owned()),
                    ProgressBar::new()
                        .is_indeterminate(true)
                        .width(tokens::layout::PROGRESS_WIDTH)
                        .horizontal_alignment(HorizontalAlignment::Left),
                )),
        ))
}

#[cfg(test)]
mod tests {
    use super::enter_send_text;

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
