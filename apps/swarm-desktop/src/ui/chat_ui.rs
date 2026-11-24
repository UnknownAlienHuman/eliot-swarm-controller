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
mod decision;
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
    /// The newest turn was scrolled into view, or why not (epoch).
    FollowLatest(u64, Result<(), String>),
    ToolsToggled(String, bool),
    CopyPart(String),
    CopyCode(String, usize),
    /// Send, Steer or Resume with the draft, bound when it was clicked.
    DraftRequested(DraftAction),
    /// Retry the unresolved submission with its saved action, body, ledger
    /// key and mutation id, whatever the Agent state has become since.
    RetryPending(String),
    SendSubmitted(Result<SendOutcome, SendFailure>),
    /// Interrupt the turn shown on the button (Agent, generation, turn).
    InterruptRequested(BoundAction),
    /// A Stop or interrupt was submitted; its final state is watched.
    ControlWatched(String, Box<MutationResult<CommandSubmitResponse>>),
    /// The final state of a watched control, or why it was not read.
    ControlSettled(String, Box<Fallible<Command>>),
    /// Approve or deny the request, with the offered choice when it has
    /// choices.
    ApprovalRequested(UiPending, bool, Option<String>),
    /// An approve choice from the split Approve's menu, by its label: the
    /// request shown when the click is handled.
    ApproveChoice(String),
    /// The open Agent's goal (`goal.get`), for the Agent it was read for.
    GoalRead(String, Fallible<Option<Box<eliot_core::api::GoalView>>>),
    /// The open Agent's inbox items (`owner.inbox`), for the decision panel.
    DecisionsRead(String, Fallible<Vec<eliot_core::api::OwnerInboxItem>>),
    /// The provider holds with their scopes, for the hold strip.
    HoldsRead(String, Fallible<Vec<eliot_core::ProviderHold>>),
    /// Decide an inbox item from the decision panel: its reference, the
    /// choice and its words.
    OwnerDecide(eliot_core::api::OwnerInboxItemRef, String, String),
    OwnerDecided(
        String,
        Box<MutationResult<eliot_core::api::OwnerDecideResponse>>,
    ),
    /// The reason typed for an unconfirmed outcome.
    DecideReason(String),
    /// Previous (-1) or next (1) item of the decision panel.
    DecisionStep(i32),
    /// Wait out the hold: the strip hides until the hold changes.
    HoldWait,
    /// Clear the hold with the owner's evidence.
    HoldClear(eliot_core::api::ProviderCorrectiveEvidence),
    /// Sign in to the held harness (`auth.login`).
    SignIn,
    /// Read the conversation again after live updates stopped.
    LiveRetry,
    /// Add the draft as a note (`inject`).
    NoteRequested,
    /// A note's admission: the Agent, the draft revision it cleared.
    NoteSubmitted(String, u64, eliot_core::Result<String>),
    AnswerRequested(UiPending),
    ControlSubmitted(String, eliot_core::Result<String>),
    /// A "…" menu entry, or Stop Agent.
    AgentMenu(menu_ui::Action),
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

/// How long the desktop waits for a Stop's final state before saying so.
const CONTROL_SETTLE_SECONDS: u64 = 10;

/// The outcome reason of an interrupt the daemon replaced with a proven stop.
const INTERRUPT_FELL_BACK: &str = "interrupt_unconfirmed_agent_stopped";

/// A retained failure's `code: reason: message` in [`command_reason`]'s
/// words: its message when it has one, else its last code in words.
fn failure_reason(reason: &str) -> String {
    let mut rest = reason.trim();
    while let Some((head, tail)) = rest.split_once(": ") {
        if head.contains(char::is_whitespace) {
            break;
        }
        rest = tail;
    }
    if rest.contains(char::is_whitespace) {
        rest.to_owned()
    } else {
        eliot_core::reason_words(rest)
    }
}

/// Why a command did not apply, in the daemon's words.
fn command_reason(outcome: Option<&serde_json::Value>) -> String {
    let field = |name: &str| {
        outcome
            .and_then(|outcome| outcome.get(name))
            .and_then(serde_json::Value::as_str)
            .filter(|text| !text.trim().is_empty())
    };
    field("message")
        .or_else(|| field("cause"))
        .map(str::to_owned)
        .or_else(|| {
            field("reason")
                .or_else(|| field("code"))
                .map(eliot_core::reason_words)
        })
        .unwrap_or_else(|| "No reason was reported.".to_owned())
}

/// Delay before the composer takes focus after an open or a send, and
/// before a send scrolls the transcript to its end: the next render has
/// realized the newest turn by then (ms).
const FOLLOW_DELAY_MS: u64 = 60;
/// The transcript viewer's automation id, which `crate::scroll_end` finds.
const TRANSCRIPT_ID: &str = "ConversationTranscript";

/// Admission refusals of a steer after which the text goes as the next
/// message: the turn ended or changed (`bad_state`, `stale_generation`), or
/// the harness has no native steering (`unsupported`). Each is definite; an
/// uncertain steer is never followed by a send.
const STEER_FALLBACK: [&str; 3] = ["bad_state", "stale_generation", "unsupported"];

/// Bound on retained unresolved-submission snapshots (one per Agent): beyond
/// this, another Agent's entry is evicted to make room for the live one.
const MAX_PENDING_RETRIES: usize = 16;

/// The unresolved submission to retry for this Agent, if any: the saved
/// action, body, ledger key and mutation id verbatim. It deliberately takes
/// no fresh action: whatever Send would select from the current Agent state
/// (Steer while a turn runs, Resume while stopped) must not replace the
/// saved identity, or an already-admitted effect would run twice.
pub(super) fn retry_submission(
    pending: &HashMap<String, PendingRetry>,
    agent: &str,
) -> Option<PendingRetry> {
    pending.get(agent).cloned()
}

/// Release an abandoned snapshot and its ledger key. Same-ID recovery is
/// already impossible (any new submission mints fresh identity), so holding
/// the key only leaks it.
pub(super) fn abandon_retry(
    pending: &mut HashMap<String, PendingRetry>,
    ledger: &mut crate::state::MutationLedger,
    agent: &str,
) {
    if let Some(snapshot) = pending.remove(agent) {
        ledger.complete(&snapshot.ledger_key);
    }
}

/// Store a retained unresolved submission, pruning what can never be
/// retried and capping the map. Snapshots whose draft revision moved on are
/// dead (their revision cannot recur): drop them and release their keys.
/// When still over the cap, evict another Agent's entry in deterministic
/// order to make room for this live one. Replacing this Agent's own entry
/// releases a superseded key.
pub(super) fn retain_retry(
    pending: &mut HashMap<String, PendingRetry>,
    ledger: &mut crate::state::MutationLedger,
    drafts: &Drafts,
    agent: &str,
    snapshot: PendingRetry,
) {
    let dead: Vec<String> = pending
        .iter()
        .filter(|(id, kept)| drafts.revision(id) != kept.revision)
        .map(|(id, _)| id.clone())
        .collect();
    for id in dead {
        abandon_retry(pending, ledger, &id);
    }
    while pending.len() >= MAX_PENDING_RETRIES {
        let Some(victim) = pending
            .keys()
            .filter(|id| id.as_str() != agent)
            .min()
            .cloned()
        else {
            break;
        };
        abandon_retry(pending, ledger, &victim);
    }
    if let Some(old) = pending.insert(agent.to_owned(), snapshot)
        && old.ledger_key != pending[agent].ledger_key
    {
        ledger.complete(&old.ledger_key);
    }
}

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
    pub(super) ledger_key: String,
    pub(super) text: String,
    pub(super) revision: u64,
    pub(super) action: DraftAction,
    pub(super) fallback: bool,
}

impl SendFailure {
    fn new(
        error: &eliot_core::Error,
        mutation_id: &str,
        ledger_key: &str,
        text: &str,
        revision: u64,
        action: &DraftAction,
        fallback: bool,
    ) -> Self {
        Self {
            error: error.clone(),
            mutation_id: mutation_id.to_owned(),
            ledger_key: ledger_key.to_owned(),
            text: text.to_owned(),
            revision,
            action: action.clone(),
            fallback,
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

/// An unconfirmed submission kept with the draft: the exact action, body,
/// mutation id and ledger key to retry. The next identical Send reuses this
/// snapshot instead of rederiving identity, which could admit a second
/// effect after a fallback `send:...:after-steer` submission whose response
/// was lost.
#[derive(Clone, Debug)]
pub(super) struct PendingRetry {
    pub(super) action: DraftAction,
    pub(super) text: String,
    pub(super) revision: u64,
    pub(super) mutation_id: String,
    pub(super) ledger_key: String,
    pub(super) fallback: bool,
}

/// Reuse the unresolved snapshot for this Agent when the draft revision
/// still matches, whatever action is requested now. Retrying the saved
/// action, body, ledger key and mutation id can only repeat the same
/// effect; minting a fresh id for a reselected action (Resume after an
/// unconfirmed Send) could admit a second effect. A moved-on revision or
/// no snapshot means a genuinely new action with fresh identity (current
/// generation/turn conflict handling is unchanged).
pub(super) fn draft_retry(pending: Option<&PendingRetry>, revision: u64) -> Option<PendingRetry> {
    pending
        .filter(|snapshot| snapshot.revision == revision)
        .cloned()
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
    pub(super) expanded: HashSet<String>,
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
    /// The composer's text, shared with the composer component.
    pub(super) drafts: Rc<std::cell::RefCell<Drafts>>,
    /// Unconfirmed submission identity per Agent, retried verbatim by
    /// Retry (or the next identical Send) instead of rederiving a fresh
    /// mutation id. Bounded, and abandoned on draft edits and Agent removal.
    pub(super) pending_retry: HashMap<String, PendingRetry>,
    pub(super) answers: HashMap<String, eliot_core::requests::Answers>,
    /// The Force stop awaiting confirmation, bound at its menu click, with
    /// the title shown in the question.
    pub(super) confirm_force: Option<(BoundAction, String)>,
    /// Refused Stop commands already told to the owner in this window.
    pub(super) control_failures: HashSet<String>,
    /// The Agent and generation whose Stop was sent and has not settled.
    pub(super) stopping: Option<(String, i64)>,
    /// The last generation of each Agent whose Stop applied (its row can
    /// lag behind until the next read).
    pub(super) stopped: HashMap<String, i64>,
    /// The open lost Agent's loss in words, read once per generation from
    /// `diagnose` (`None` inside: nothing to say beyond Lost).
    pub(super) loss: Option<(String, i64, Option<String>)>,
    /// The open Agent's Eliot Goal as last read (`goal.get`): the Agent
    /// it was read for and the goal, `None` inside when it has none.
    pub(super) goal: Option<(String, Option<eliot_core::api::GoalView>)>,
    /// The open Agent's `owner.inbox` items (requests, unconfirmed
    /// outcomes), for the decision panel.
    pub(super) decisions: Option<(String, Vec<eliot_core::api::OwnerInboxItem>)>,
    /// The provider holds with their scopes, read while the open Agent is
    /// held.
    pub(super) holds: Option<(String, Fallible<Vec<eliot_core::ProviderHold>>)>,
    /// The hold the owner chose to wait out (Agent, hold start).
    pub(super) hold_waited: Option<(String, i64)>,
    /// The decision panel's shown item.
    pub(super) decision_at: usize,
    /// The reason typed for an unconfirmed outcome.
    pub(super) decide_reason: String,
}

impl DesktopApp {
    /// Replace one Agent's draft (bumping its revision) and abandon its
    /// unresolved snapshot, if any: that revision can never recur, so the
    /// saved identity is unretryable and its ledger key must be released.
    /// Every draft edit goes through here so no stale snapshot survives it.
    fn store_draft_text(&mut self, agent: &str, text: String) {
        self.chat.drafts.borrow_mut().set(Some(agent), text);
        abandon_retry(&mut self.chat.pending_retry, &mut self.ledger, agent);
    }

    /// Submit the unresolved snapshot verbatim: its saved action, body,
    /// ledger key and mutation id, whatever the Agent state has become
    /// since. A fresh Send could now select Steer or Resume and admit a
    /// second effect under a fresh id; this path never reselects.
    fn retry_pending(&mut self, agent: &str, context: &ComponentContext<Self>) {
        if self.submitting() {
            return;
        }
        if self.data_dir.is_err() {
            return;
        }
        let Some(snapshot) = retry_submission(&self.chat.pending_retry, agent) else {
            self.set_status("Nothing to retry.");
            return;
        };
        self.in_flight_key = Some(snapshot.ledger_key.clone());
        self.alert = None;
        self.set_status(format!("Retry ({}).", snapshot.mutation_id));
        // The saved revision, so the composer empties and a later Send of
        // new text mints fresh identity.
        let agent_id = agent.to_owned();
        self.chat
            .drafts
            .borrow_mut()
            .ack(&agent_id, snapshot.revision);
        if !snapshot.text.trim().is_empty()
            && let Some(convo) = self.chat.convo.as_mut()
            && convo.agent() == agent_id
        {
            convo.note_local_input(&snapshot.mutation_id, &snapshot.text);
            convo.set_local_note(
                &snapshot.mutation_id,
                Some(
                    if matches!(snapshot.action, DraftAction::Steer(_)) {
                        "Steering the running turn…"
                    } else {
                        "Sending…"
                    }
                    .to_owned(),
                ),
            );
        }
        self.rebuild_items();
        self.follow_latest(true, context);
        let blank = snapshot.text.trim().is_empty();
        self.spawn_draft(
            snapshot.action,
            snapshot.mutation_id,
            snapshot.ledger_key,
            snapshot.text,
            snapshot.revision,
            blank,
            snapshot.fallback,
            context,
        );
    }

    pub(super) fn chat_update(&mut self, msg: ChatMsg, context: &ComponentContext<Self>) {
        match msg {
            ChatMsg::ComposerChanged(text) => {
                let previous = self.composer_text();
                // The echo of a text the window set (a clear after Send, a
                // restore) is no edit: the draft revision stays.
                if text == previous {
                    return;
                }
                // Enter without Shift typed this line break: send instead.
                // A pending retry needs no text (promptless Resume), so it
                // satisfies the Enter guard on its own.
                if crate::shortcuts::take_plain_enter()
                    && let Some(sent) = enter_send_text(&previous, &text)
                {
                    let retry_ready = self
                        .selected_agent_id()
                        .is_some_and(|agent| self.chat.pending_retry.contains_key(&agent));
                    if (!sent.trim().is_empty() || retry_ready)
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
                    self.store_draft_text(&agent, text);
                } else {
                    self.chat.drafts.borrow_mut().set(None, text);
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
                        self.show_control_failure();
                        self.check_loss(context);
                        self.sync_open_agent();
                        self.ensure_catalog(context);
                        self.read_conversation_extras(context);
                        let cursor = snapshot.coverage.cursor;
                        let mut generation_changed = false;
                        if let Some(convo) = self.chat.convo.as_mut() {
                            let missing =
                                convo.reset(snapshot.coverage, &snapshot.turns, &snapshot.events);
                            convo.apply_boundaries(&snapshot.boundaries);
                            generation_changed = missing.generation_changed;
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
                        if generation_changed {
                            self.follow_next(
                                Next::Resnapshot {
                                    reason: "a new run started".to_owned(),
                                },
                                context,
                            );
                            return;
                        }
                        self.check_waiting_inputs(context);
                        if first_load {
                            // The transcript opens at its newest turn: its
                            // far-edge anchor keeps the end only once the
                            // view is there, and a short history arrives in
                            // one frame at the top, so it is scrolled there
                            // once (observed live); typing goes to the
                            // composer.
                            self.follow_latest(true, context);
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
                        self.chat.failures = 0;
                        self.chat.docs.extend(reread.docs);
                        if let Some(detail) = reread.detail.as_ref() {
                            self.chat.detail = Some(detail_from(detail));
                            self.show_control_failure();
                            self.check_loss(context);
                            self.sync_open_agent();
                            self.ensure_catalog(context);
                            self.read_conversation_extras(context);
                        }
                        if let Some(convo) = self.chat.convo.as_mut() {
                            if let Some(results) = reread.results.as_ref() {
                                convo.update_turn_coverage(results.generation, results.has_older);
                                convo.apply_turns(&results.turns);
                                convo.apply_boundaries(&results.boundaries);
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
                        // Each failure in a row waits longer (#826).
                        let delay = live::backoff_ms(self.chat.failures);
                        self.chat.failures = self.chat.failures.saturating_add(1);
                        self.link_suspect(context);
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
            ChatMsg::FollowLatest(epoch, outcome) => {
                if epoch != self.chat.epoch {
                    return;
                }
                if let Err(error) = outcome {
                    self.set_status(format!(
                        "The latest turn was not scrolled into view: {error}"
                    ));
                }
                // Typing goes on in the composer; focus would leave the
                // search box and close its list.
                if !self.chat.opened_from_search {
                    self.take_focus(FocusTarget::Composer);
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
                    // A failed Agent's diagnosis in one line ("What went
                    // wrong"); a lost one gets words only when Swarm's own
                    // restart caused it, any other cause stays in Details.
                    let failed = self
                        .chat
                        .detail
                        .as_ref()
                        .is_some_and(|detail| detail.state == eliot_core::AgentState::Failed);
                    let words = match summary {
                        Ok(summary) if failed => summary
                            .summary
                            .lines()
                            .map(str::trim)
                            .find(|line| !line.is_empty())
                            .map(str::to_owned),
                        Ok(summary) if monitor::restart_loss(&summary) => {
                            Some("Stopped by a Swarm restart".to_owned())
                        }
                        _ => None,
                    };
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
                let text =
                    profiles::normalize_newlines(self.chat.drafts.borrow().text(Some(&agent_id)));
                // Attached files ride the message as its closing block.
                let text = crate::attachments::compose(&text, self.attached_to(&agent_id));
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
                let revision = self.chat.drafts.borrow().revision(&agent_id);
                // An unconfirmed submission at this revision is retried
                // with its saved action, body, ledger key and mutation id,
                // whatever is requested now: a reselected action (Resume
                // from the sidebar or Monitor after an unconfirmed Send)
                // under a fresh id could admit a second effect. Anything
                // else is a genuinely new action: drop only a dead snapshot
                // (its revision can never recur, releasing its key) and
                // mint fresh identity. Edits already abandon theirs, so
                // this only catches a revision that moved on another way.
                if self
                    .chat
                    .pending_retry
                    .get(&agent_id)
                    .is_some_and(|pending| pending.revision != revision)
                {
                    abandon_retry(&mut self.chat.pending_retry, &mut self.ledger, &agent_id);
                }
                let retry = draft_retry(self.chat.pending_retry.get(&agent_id), revision);
                let (action, text, key, mutation_id, fallback) = match retry {
                    Some(snapshot) => {
                        // The ledger still holds this key (an uncertain
                        // outcome never releases it), so the snapshot id is
                        // the key's id: no new mutation id is minted. The
                        // saved action and body win over the request: the
                        // same effect retried can only repeat itself.
                        (
                            snapshot.action,
                            snapshot.text,
                            snapshot.ledger_key,
                            snapshot.mutation_id,
                            snapshot.fallback,
                        )
                    }
                    None => {
                        let key = action.key(revision);
                        let mutation_id = self.ledger.id_for(&key);
                        (action, text, key, mutation_id, false)
                    }
                };
                let blank = text.trim().is_empty();
                self.in_flight_key = Some(key.clone());
                let what = action.label();
                self.alert = None;
                self.set_status(format!("{what} ({mutation_id})…"));
                // At once: the composer empties (same revision, so a retry
                // keeps this id) and the message shows with what it waits for.
                self.chat.drafts.borrow_mut().ack(&agent_id, revision);
                self.clear_attached(&agent_id);
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
                self.follow_latest(true, context);
                self.spawn_draft(
                    action,
                    mutation_id,
                    key,
                    text,
                    revision,
                    blank,
                    fallback,
                    context,
                );
            }
            ChatMsg::RetryPending(agent) => {
                self.retry_pending(&agent, context);
            }
            ChatMsg::SendSubmitted(outcome) => self.send_submitted(outcome, context),
            ChatMsg::InterruptRequested(bound) => {
                let turn = bound.turn.clone().unwrap_or_default();
                let key = format!("interrupt:{}:{}:{turn}", bound.agent, bound.generation);
                self.submit_watched(context, "Stop", key, move |data_dir, mutation_id| {
                    client::block_on(client::submit_interrupt(
                        data_dir,
                        mutation_id,
                        &bound.agent,
                        bound.generation,
                        turn,
                    ))
                });
            }
            ChatMsg::ApproveChoice(label) => {
                let choice = self.chat.detail.as_ref().and_then(|detail| {
                    detail.pending.iter().find_map(|request| {
                        request
                            .choices
                            .iter()
                            .find(|choice| {
                                choice.label == label
                                    && choice.decision
                                        == eliot_core::requests::ApprovalDecision::Approve
                            })
                            .map(|choice| (request.clone(), choice.id.clone()))
                    })
                });
                if let Some((request, choice)) = choice {
                    self.chat_update(
                        ChatMsg::ApprovalRequested(request, true, Some(choice)),
                        context,
                    );
                }
            }
            ChatMsg::GoalRead(agent, read) => {
                if self.selected_agent_id().as_deref() == Some(agent.as_str()) {
                    // A failed read shows no mark rather than a stale one.
                    self.chat.goal = Some((agent, read.ok().flatten().map(|goal| *goal)));
                }
            }
            ChatMsg::DecisionsRead(agent, read) => {
                if self.selected_agent_id().as_deref() == Some(agent.as_str()) {
                    match read {
                        Ok(items) => self.chat.decisions = Some((agent, items)),
                        Err(error) => {
                            self.set_status(format!("The inbox was not read: {error}"));
                        }
                    }
                }
            }
            ChatMsg::HoldsRead(agent, read) => {
                if self.selected_agent_id().as_deref() == Some(agent.as_str()) {
                    self.chat.holds = Some((agent, read));
                }
            }
            ChatMsg::OwnerDecide(item, choice, words) => {
                self.owner_decide(item, choice, words, context);
            }
            ChatMsg::OwnerDecided(words, result) => self.owner_decided(&words, *result, context),
            ChatMsg::DecideReason(text) => self.chat.decide_reason = text,
            ChatMsg::DecisionStep(step) => {
                self.chat.decision_at = self.chat.decision_at.saturating_add_signed(step as isize);
            }
            ChatMsg::HoldWait => {
                if let (Some(agent), Some(hold)) = (
                    self.selected_agent_id(),
                    self.chat
                        .detail
                        .as_ref()
                        .and_then(|detail| detail.provider_hold.as_ref()),
                ) {
                    self.chat.hold_waited = Some((agent, hold.held_since));
                }
            }
            ChatMsg::HoldClear(evidence) => self.clear_hold(evidence, context),
            ChatMsg::SignIn => {
                let (Some(harness), Ok(data_dir)) = (
                    self.chat
                        .detail
                        .as_ref()
                        .map(|detail| detail.harness.clone()),
                    self.data_dir.clone(),
                ) else {
                    return;
                };
                self.set_status(format!("Requesting a {harness} sign-in…"));
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(client::auth_login(&data_dir, &harness, None))
                        .map(|reply| format!("{:?}", reply.status).to_lowercase());
                    Message::Chat(ChatMsg::ControlSubmitted("Sign in".to_owned(), result))
                });
            }
            ChatMsg::LiveRetry => self.start_snapshot(context),
            ChatMsg::NoteRequested => self.add_note(context),
            ChatMsg::NoteSubmitted(agent, revision, outcome) => {
                self.settle_mutation(
                    outcome
                        .as_ref()
                        .err()
                        .is_none_or(|error| !crate::state::uncertain(error)),
                );
                match outcome {
                    Ok(state) => {
                        // The note is in: the composer empties.
                        self.chat.drafts.borrow_mut().ack(&agent, revision);
                        self.clear_attached(&agent);
                        self.set_status(format!("Note added: {state}."));
                    }
                    Err(error) if crate::state::uncertain(&error) => self.alert(format!(
                        "Add as a note: not confirmed (trying again reuses its id): {}",
                        error.user_message()
                    )),
                    Err(error) => {
                        self.alert(format!("Add as a note refused: {}", error.user_message()))
                    }
                }
                self.queue_reread(Invalidation::default(), context);
            }
            ChatMsg::ApprovalRequested(request, approve, choice) => {
                let verb = if approve { "Approve" } else { "Deny" };
                let key = format!(
                    "{verb}:{}:{}:{}",
                    request.binding.agent,
                    request.binding.request.clone().unwrap_or_default(),
                    choice.as_deref().unwrap_or_default()
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
                            choice,
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
            ChatMsg::ControlWatched(what, outcome) => {
                // The action stays in flight until its command settles, so a
                // second click cannot send a second Stop.
                match *outcome {
                    Ok(reply) if scripts::is_terminal(reply.state) => {
                        self.finish_control(reply.state);
                        self.report_control(
                            &what,
                            &reply.command_id,
                            reply.state,
                            reply.outcome.as_ref(),
                            context,
                        );
                    }
                    Ok(reply) => {
                        self.set_status(format!("{what} sent; waiting for the Agent…"));
                        let Ok(data_dir) = self.data_dir.clone() else {
                            self.finish_control(eliot_core::CommandState::Unknown);
                            return;
                        };
                        let _task = context.spawn_background(move |cancel| {
                            let cancelled = || cancel.is_cancelled();
                            let settled = settle_command(
                                &data_dir,
                                &reply.command_id,
                                CONTROL_SETTLE_SECONDS,
                                &cancelled,
                            );
                            Message::Chat(ChatMsg::ControlSettled(what, Box::new(settled)))
                        });
                    }
                    Err(error) if crate::state::uncertain(&error) => {
                        self.finish_control(eliot_core::CommandState::Unknown);
                        self.alert(format!(
                            "{what}: not confirmed (trying again reuses its id): {}",
                            error.user_message()
                        ));
                    }
                    Err(error) => {
                        self.finish_control(eliot_core::CommandState::Rejected);
                        self.alert(format!("{what} refused: {}", error.user_message()));
                    }
                }
            }
            ChatMsg::ControlSettled(what, settled) => match *settled {
                Ok(command) => {
                    self.finish_control(command.state);
                    self.report_control(
                        &what,
                        &command.id,
                        command.state,
                        command.outcome.as_ref(),
                        context,
                    );
                }
                // Not settled in time: the same click later reads this
                // command back instead of sending another.
                Err(error) => {
                    self.finish_control(eliot_core::CommandState::Unknown);
                    self.alert(format!("{what}: {error}"));
                }
            },
            ChatMsg::AgentMenu(action) => {
                // Bound now, to the Agent and generation the header shows.
                let binding = self.selected_agent_id().zip(self.chat.detail.as_ref()).map(
                    |(agent, detail)| MenuBinding {
                        agent,
                        generation: detail.generation,
                    },
                );
                if let Some(binding) = binding {
                    self.agent_menu(action, binding, context);
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
                let target = (bound.agent.clone(), bound.generation);
                let sent = self.submit_watched(
                    context,
                    "Force stop",
                    key,
                    move |data_dir, mutation_id| {
                        client::block_on(client::submit_stop(
                            data_dir,
                            mutation_id,
                            &bound.agent,
                            bound.generation,
                            true,
                        ))
                    },
                );
                if sent {
                    self.chat.stopping = Some(target);
                }
            }
        }
    }

    /// Add the draft as a note to the open Agent (`inject` at `soonest`):
    /// into its context at the next safe point of a running turn, else with
    /// the next input, without starting a turn. The draft stays until the
    /// daemon admits the note.
    fn add_note(&mut self, context: &ComponentContext<Self>) {
        if self.submitting() {
            return;
        }
        let (Some(agent), Some(generation), Ok(data_dir)) = (
            self.selected_agent_id(),
            self.chat.detail.as_ref().map(|detail| detail.generation),
            self.data_dir.clone(),
        ) else {
            return;
        };
        let text = profiles::normalize_newlines(self.chat.drafts.borrow().text(Some(&agent)));
        let text = crate::attachments::compose(&text, self.attached_to(&agent));
        if text.trim().is_empty() {
            self.alert("Type a note first.");
            return;
        }
        let revision = self.chat.drafts.borrow().revision(&agent);
        let key = format!("note:{agent}:{generation}:{revision}");
        let mutation_id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        self.alert = None;
        self.set_status(format!("Add as a note ({mutation_id})…"));
        let _task = context.spawn_background(move |_| {
            let outcome = client::block_on(client::submit_inject(
                &data_dir,
                mutation_id,
                &agent,
                generation,
                &text,
            ))
            .map(|reply| submit_note(&reply));
            Message::Chat(ChatMsg::NoteSubmitted(agent, revision, outcome))
        });
    }

    /// Stop this Agent generation (its process and session), from the
    /// header, the Monitor or a menu; the outcome is reported when it settles.
    pub(super) fn stop_agent(
        &mut self,
        agent: &str,
        generation: i64,
        context: &ComponentContext<Self>,
    ) {
        let bound = BoundAction::agent_control(agent, generation);
        let key = format!("stop:{}:{}:false", bound.agent, bound.generation);
        let sent = self.submit_watched(context, "Stop Agent", key, move |data_dir, mutation_id| {
            client::block_on(client::submit_stop(
                data_dir,
                mutation_id,
                &bound.agent,
                bound.generation,
                false,
            ))
        });
        if sent {
            self.chat.stopping = Some((agent.to_owned(), generation));
        }
    }

    /// How this Agent generation's Stop button shows; `None` once its Stop
    /// applied, before the next read says so.
    pub(super) fn stop_button(
        &self,
        agent: &str,
        generation: i64,
    ) -> Option<monitor_view::StopButton> {
        use monitor_view::StopButton;
        if self.chat.stopped.get(agent) == Some(&generation) {
            return None;
        }
        let stopping = self
            .chat
            .stopping
            .as_ref()
            .is_some_and(|(id, run)| id == agent && *run == generation);
        Some(if stopping {
            StopButton::Stopping
        } else if self.submitting() {
            StopButton::Waiting
        } else {
            StopButton::Ready
        })
    }

    /// A watched control reached its end, so the next action may start. An
    /// unknown result keeps its mutation id: trying again reads it back.
    fn finish_control(&mut self, state: eliot_core::CommandState) {
        use eliot_core::CommandState;
        self.settle_mutation(state != CommandState::Unknown);
        if let Some((agent, generation)) = self.chat.stopping.take()
            && state == CommandState::Applied
        {
            self.chat.stopped.insert(agent, generation);
        }
    }

    /// A control whose refusal the owner must see: a command can be admitted
    /// and refused by the harness afterwards, so its final state is watched
    /// ([`ChatMsg::ControlWatched`]).
    fn submit_watched(
        &mut self,
        context: &ComponentContext<Self>,
        what: &str,
        key: String,
        work: impl FnOnce(&Path, String) -> eliot_core::Result<CommandSubmitResponse> + Send + 'static,
    ) -> bool {
        if self.submitting() {
            self.alert("Waiting for the last action to settle.");
            return false;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return false;
        };
        let mutation_id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        self.alert = None;
        self.set_status(format!("{what} ({mutation_id})…"));
        let what = what.to_owned();
        let _task = context.spawn_background(move |_| {
            Message::Chat(ChatMsg::ControlWatched(
                what,
                Box::new(work(&data_dir, mutation_id)),
            ))
        });
        true
    }

    /// A Stop the daemon retained as refused, told once while its Agent is
    /// still live (also after a restart of this window).
    fn show_control_failure(&mut self) {
        let Some(failure) = self.chat.detail.as_ref().and_then(|detail| {
            let ended = matches!(
                detail.state,
                eliot_core::AgentState::Stopped
                    | eliot_core::AgentState::Failed
                    | eliot_core::AgentState::Lost
            );
            detail.control_failure.clone().filter(|_| !ended)
        }) else {
            return;
        };
        if self.chat.control_failures.insert(failure.command_id) {
            let what = if failure.verb == "stop" {
                "Stop Agent"
            } else {
                "Stop"
            };
            self.alert(format!(
                "{what} refused: {}",
                failure_reason(&failure.reason)
            ));
        }
    }

    /// The final state of a watched control, in words.
    fn report_control(
        &mut self,
        what: &str,
        command: &str,
        state: eliot_core::CommandState,
        outcome: Option<&serde_json::Value>,
        context: &ComponentContext<Self>,
    ) {
        use eliot_core::CommandState;
        self.chat.control_failures.insert(command.to_owned());
        let fell_back = outcome
            .and_then(|outcome| outcome.get("reason"))
            .and_then(serde_json::Value::as_str)
            == Some(INTERRUPT_FELL_BACK);
        match state {
            _ if fell_back => {
                self.alert("The turn did not confirm that it stopped, so Swarm stopped the Agent.")
            }
            CommandState::Applied => self.set_status(format!("{what}: done.")),
            CommandState::Unknown => self.alert(format!(
                "{what}: the result is not confirmed. {}",
                command_reason(outcome)
            )),
            _ => self.alert(format!("{what} refused: {}", command_reason(outcome))),
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
        // The sidebar, Monitor and project page show the new state now, not
        // at their next poll.
        if !self.nav.agents_loading {
            self.refresh_head(context);
        }
        if self.main == Main::Project {
            self.read_project(context);
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

    /// The Home screen (`Main.dc.html`): one centred column at the Home
    /// width, 168 DIPs below the card's top, its parts 28 DIPs apart: the
    /// question in `title-large` ("What should we build in {project ▾}?",
    /// the project name a menu button), a refusal notice when there is one,
    /// the composer, then three suggestion cards: Review the open PRs
    /// (fills the composer), Schedule an automation, and Continue {the last
    /// task} when there is one.
    fn home_view(
        &self,
        alert: View,
        composer: View,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        const REVIEW: &str =
            "Review the open pull requests in this repository and summarize what needs attention.";
        let last = self
            .nav
            .agents
            .iter()
            .filter(|agent| !agent.native_child && !agent.archived)
            .max_by_key(|agent| agent.updated_at)
            .cloned();
        let card = |glyph: &str, label: &str| {
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::ROW)
                .margin(Thickness::xy(
                    tokens::spacing::TEXT_INSET - tokens::spacing::INSET,
                    0.0,
                ))
                .children((
                    m3::icon(glyph),
                    m3::body(label.to_owned())
                        .vertical_alignment(VerticalAlignment::Center)
                        .max_width(tokens::layout::PROJECT_CHIP_WIDTH)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                ))
        };
        let mut cards: Vec<KeyedView> =
            vec![
                KeyedView::new(
                    "review",
                    m3::suggestion_card(
                        palette,
                        m3::suggestion(palette)
                            .automation_name("Review the open PRs")
                            .on_click(context.message(Message::Chat(ChatMsg::ComposerChanged(
                                REVIEW.to_owned(),
                            ))))
                            .content(card(m3::glyph::DIFF, "Review the open PRs")),
                    ),
                ),
                KeyedView::new(
                    "automation",
                    m3::suggestion_card(
                        palette,
                        m3::suggestion(palette)
                            .automation_name("Schedule an automation")
                            .on_click(context.message(Message::Shell(ShellMsg::PlaceOpened(
                                page_ui::Place::Automations,
                            ))))
                            .content(card(m3::glyph::SCHEDULE, "Schedule an automation")),
                    ),
                ),
            ];
        if let Some(agent) = last {
            let label = format!("Continue {}", self.agent_label(&agent));
            cards.push(KeyedView::new(
                format!("continue:{}", agent.id),
                m3::suggestion_card(
                    palette,
                    m3::suggestion(palette)
                        .automation_name(label.clone())
                        .on_click(context.message(Message::Shell(ShellMsg::AgentOpened(agent))))
                        .content(card(m3::glyph::HISTORY, &label)),
                ),
            ));
        }
        let title: View = if self.launch.chat {
            m3::page_title("What would you like to chat about?")
                .horizontal_alignment(HorizontalAlignment::Center)
                .into()
        } else if self.launch.workspace.is_empty() {
            m3::page_title("What should we build?")
                .horizontal_alignment(HorizontalAlignment::Center)
                .into()
        } else {
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .horizontal_alignment(HorizontalAlignment::Center)
                .automation_name(format!(
                    "What should we build in {}?",
                    self.launch_project_label()
                ))
                .automation_heading_level(AutomationHeadingLevel::Level2)
                .children((
                    m3::text("What should we build in", tokens::text::HERO)
                        .vertical_alignment(VerticalAlignment::Center),
                    self.title_project(context, palette),
                    m3::text("?", tokens::text::HERO).vertical_alignment(VerticalAlignment::Center),
                ))
        };
        let width = self.column_width(tokens::layout::HOME_WIDTH);
        ScrollViewer::new()
            .grid_row(1)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .content(
                StackPanel::new()
                    .width(width)
                    .max_width(tokens::layout::HOME_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .margin(Thickness::new(
                        0.0,
                        tokens::layout::HOME_TOP,
                        0.0,
                        tokens::spacing::XL,
                    ))
                    .spacing(tokens::spacing::HOME_GROUP_GAP)
                    .children((
                        title,
                        alert,
                        composer,
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .horizontal_alignment(HorizontalAlignment::Center)
                            .spacing(tokens::spacing::SM)
                            .keyed_children(cards),
                    )),
            )
    }

    /// The Settings page is drawn here too, over the transcript component,
    /// which stays mounted: Settings leaves the conversation, and the
    /// transcript removes its parts over later frames, as when another
    /// conversation opens (`views::Transcript`), instead of tearing them all
    /// down in one frame. Back to tasks builds it again from its end.
    pub(super) fn chat_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let column = self.reading_width();
        let transcript = self.transcript_view(column, context, palette);
        if self.main != Main::Conversation {
            let content = match self.main {
                Main::Project => self.project_view(context, palette),
                Main::Inbox => self.inbox_view(context, palette),
                Main::Agents => self.monitor_view(context, palette),
                Main::Automations => self.automations_page(context, palette),
                Main::Conversation | Main::Settings => self.settings_view(context, palette),
            };
            // Opaque over the transcript beneath; its corners follow the
            // main card's (a Border does not clip its child).
            let radius = tokens::layout::CARD_RADIUS;
            let page = Border::new()
                .grid_row(1)
                .background(palette.surface())
                .corner_radius(
                    if self.panel != Panel::Closed && self.main == Main::Project {
                        CornerRadius::new(radius, 0.0, 0.0, radius)
                    } else {
                        CornerRadius::uniform(radius)
                    },
                )
                .content(content);
            return Grid::new()
                .rows([
                    GridLength::Auto,
                    GridLength::STAR,
                    GridLength::Auto,
                    GridLength::Auto,
                    GridLength::Auto,
                    GridLength::Auto,
                ])
                .children((
                    View::empty(),
                    View::fragment((transcript, page)),
                    View::empty(),
                    View::empty(),
                    View::empty(),
                    View::empty(),
                    View::empty(),
                ));
        }
        let agent = self.selected_agent_id();
        let header: View = match agent.as_deref() {
            Some(agent) => self.header_view(agent, context, palette),
            None => View::empty(),
        };
        // The transcript stays in place without an open Agent too, so the
        // conversation just left is removed over later frames, not at once
        // (`views::Transcript`); the start or Home screen covers it.
        let home = agent.is_none() && self.launch.starting.is_none();
        let cover: View = match agent {
            // The start is being submitted: the message and what happens
            // now, at once (no empty pause after Send).
            None if self.launch.starting.is_some() => {
                let (text, label) = self.launch.starting.clone().unwrap_or_default();
                starting_view(&text, &label, column, palette)
            }
            // Home is built below, around the notice and the composer.
            _ => View::empty(),
        };
        let reconnect = self.reconnect_notice(column, palette);
        let alert: View = match (&self.chat.confirm_force, &self.alert) {
            (Some((bound, title)), _) => {
                self.force_stop_banner(bound, title, column, context, palette)
            }
            // A restart in progress says so once, over the reads it stalls.
            (None, _) if reconnect.is_some() => reconnect.unwrap_or_else(View::empty),
            (None, Some(text)) => {
                let queue: View = if self
                    .launch
                    .queue_offer
                    .as_ref()
                    .is_some_and(|offer| &offer.alert == text)
                {
                    m3::text_button(palette)
                        .grid_column(2)
                        .is_enabled(!self.submitting())
                        .automation_name("Queue it")
                        .on_click(context.message(Message::Launch(LaunchMsg::QueueRequested)))
                        .content("Queue it")
                } else {
                    View::empty()
                };
                Border::new()
                    .grid_row(4)
                    .width(column)
                    .max_width(tokens::layout::READING_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                    // Alerts are refusals, uncertain outcomes and failed
                    // reads: the attention surface and mark, the text in
                    // the body color so it stays easy to read.
                    .background(palette.attention_container())
                    .border_brush(palette.attention_outline())
                    .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
                    .corner_radius(tokens::radius::CONTAINER)
                    .padding(Thickness::new(
                        tokens::spacing::MD,
                        tokens::spacing::XS,
                        tokens::spacing::XS,
                        tokens::spacing::XS,
                    ))
                    .content(
                        Grid::new()
                            .columns([
                                GridLength::Auto,
                                GridLength::STAR,
                                GridLength::Auto,
                                GridLength::Auto,
                            ])
                            .column_spacing(tokens::spacing::SM)
                            .children((
                                Border::new()
                                    .vertical_alignment(VerticalAlignment::Center)
                                    .content(m3::status_mark(
                                        palette,
                                        crate::theme::StatusTone::Attention,
                                        tokens::layout::DOT,
                                    )),
                                m3::body(text.clone())
                                    .grid_column(1)
                                    .foreground(palette.text())
                                    .vertical_alignment(VerticalAlignment::Center)
                                    .text_wrapping(TextWrapping::Wrap)
                                    .is_text_selection_enabled(true),
                                queue,
                                m3::icon_button(palette)
                                    .grid_column(3)
                                    .automation_name("Dismiss")
                                    .on_click(
                                        context.message(Message::Shell(ShellMsg::AlertDismissed)),
                                    )
                                    .content(m3::icon(m3::glyph::CLOSE)),
                            )),
                    )
            }
            (None, None) => self
                .archived_notice(column, context, palette)
                .or_else(|| self.loss_notice(column, context, palette))
                .unwrap_or_else(View::empty),
        };
        // Waiting for the owner: the decision panel takes the composer's
        // place; otherwise the hold strip and the workers line sit above
        // the composer.
        let composer = match self.decision_panel(column, context, palette) {
            Some(panel) if !home => panel,
            _ => {
                let above: Vec<KeyedView> = [
                    self.hold_strip(context, palette)
                        .map(|strip| ("strip", strip)),
                    self.workers_line(palette).map(|line| ("workers", line)),
                ]
                .into_iter()
                .flatten()
                .map(|(key, view)| KeyedView::new(key, view))
                .collect();
                let composer = self.composer_view(column, context, palette);
                if home || above.is_empty() {
                    composer
                } else {
                    StackPanel::new().grid_row(5).children((
                        StackPanel::new()
                            .width(column)
                            .max_width(tokens::layout::READING_WIDTH)
                            .horizontal_alignment(HorizontalAlignment::Center)
                            .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                            .spacing(tokens::spacing::SM)
                            .keyed_children(above),
                        composer,
                    ))
                }
            }
        };
        // Home: the question, the notice, the composer and the suggestions
        // in one column over the (empty) transcript.
        if home {
            let home = self.home_view(alert, composer, context, palette);
            return Grid::new()
                .rows([
                    GridLength::Auto,
                    GridLength::STAR,
                    GridLength::Auto,
                    GridLength::Auto,
                    GridLength::Auto,
                    GridLength::Auto,
                ])
                .children((
                    header,
                    View::fragment((transcript, home)),
                    View::empty(),
                    View::empty(),
                    View::empty(),
                ));
        }

        let body = View::fragment((transcript, cover));
        // The live line under the header (Reconnecting, Live updates
        // stopped), in the transcript's row over its top.
        let live = Border::new()
            .grid_row(1)
            .vertical_alignment(VerticalAlignment::Top)
            .content(self.live_line(context, palette));
        // Rows: the header, the transcript, (spare), (spare), the notice,
        // the composer or the decision panel (its popovers are flyouts).
        Grid::new()
            .rows([
                GridLength::Auto,
                GridLength::STAR,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
                GridLength::Auto,
            ])
            .children((header, View::fragment((body, live)), alert, composer))
    }

    /// The reading column's width: the reading width, or the main area less
    /// its gutters when narrower; content-sized until the window size is
    /// observed. Inside the ScrollViewer a stretched MaxWidth column was
    /// arranged in a slot wider than the viewport (off center), so the
    /// column gets an explicit width.
    fn reading_width(&self) -> Option<f64> {
        self.column_width(tokens::layout::READING_WIDTH)
    }

    /// The column the main area gives: its width less the design's 32-DIP
    /// side padding, at most `widest` (720 in a conversation, 760 at home).
    pub(super) fn column_width(&self, widest: f64) -> Option<f64> {
        self.main_width().map(|width| {
            (width - 2.0 * tokens::spacing::XL).clamp(tokens::layout::MIN_COLUMN, widest)
        })
    }
}
