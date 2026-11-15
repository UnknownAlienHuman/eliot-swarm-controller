//! The conversation: a one-line Agent header with its "…" menu, the
//! retained history and live delivery, pending requests, and the one
//! composer (context chips, the field, model/access chips and Send, with
//! Interrupt and Steer while a turn runs).

use super::*;
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
    JumpToLatest,
    /// Bring the latest turn into view once it is realized (epoch, tries).
    FollowLatest(u64, u32),
    ToolsToggled(String, bool),
    CopyPart(String),
    CopyCode(String, usize),
    /// Send, Steer or Resume with the draft, bound when it was clicked.
    DraftRequested(DraftAction),
    SendSubmitted(Fallible<SendOutcome>),
    /// Interrupt the turn shown on the button (Agent, generation, turn).
    InterruptRequested(BoundAction),
    ApprovalRequested(UiPending, bool),
    AnswerRequested(UiPending),
    ControlSubmitted(String, Fallible<String>),
    /// A "…" menu entry, with the Agent as the header showed it.
    AgentMenu(String, MenuBinding),
    /// The Force stop confirmation closed (Primary confirms).
    ForceStopClosed(ContentDialogResult),
}

/// The Agent a "…" menu was built for: what is clicked is what was shown.
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

/// Follow-latest retries while the newest turn is not realized yet.
const FOLLOW_TRIES: u32 = 20;
const FOLLOW_DELAY_MS: u64 = 50;

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
    pub(super) unseen: usize,
    /// A button of the latest turn, rendered outside the virtualized
    /// repeater so it always exists: "Jump to latest" focuses it, and WinUI
    /// brings a focused control into view (Reactor 0.100.0 exposes no
    /// ScrollViewer offset or ChangeView).
    pub(super) end_anchor: ElementRef<Button>,
    /// The composer, focused again after input is admitted, a launch and New.
    pub(super) composer_ref: ElementRef<TextBox>,
    pub(super) pending_local: Option<(String, String, String)>,
    // Mutations.
    pub(super) drafts: DraftStore,
    pub(super) answers: HashMap<String, String>,
    /// The Force stop awaiting confirmation, bound at its menu click, with
    /// the title shown in the question.
    pub(super) confirm_force: Option<(BoundAction, String)>,
}

impl DesktopApp {
    pub(super) fn chat_update(&mut self, msg: ChatMsg, context: &ComponentContext<Self>) {
        match msg {
            ChatMsg::ComposerChanged(text) => {
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
                        if first_load {
                            // Opened at the newest turn, like a chat.
                            self.chat.unseen = 0;
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
                            self.ensure_catalog(context);
                        }
                        if let Some(convo) = self.chat.convo.as_mut() {
                            if let Some(turns) = reread.turns.as_deref() {
                                convo.apply_turns(turns);
                            }
                            convo.apply_inputs(reread.inputs);
                        }
                        self.rebuild_items();
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
            ChatMsg::JumpToLatest => {
                self.chat.unseen = 0;
                if !self.chat.end_anchor.request_focus() {
                    self.set_status(
                        "Jump to latest: the latest turn has no control to move to yet.",
                    );
                }
            }
            ChatMsg::FollowLatest(epoch, tries) => {
                if epoch != self.chat.epoch {
                    return;
                }
                if self.chat.end_anchor.request_focus() {
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
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
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
                            Err(error)
                                if matches!(error.code.as_str(), "unknown" | "unavailable") =>
                            {
                                // Uncertain reply: read the retained command
                                // instead of submitting again under a new id.
                                let command = client::read_command(&data_dir, &mutation_id)
                                    .await
                                    .map_err(|_| error.to_string())?;
                                (
                                    command.id,
                                    format!("{:?}", command.state).to_lowercase(),
                                    None,
                                    true,
                                )
                            }
                            Err(error) => return Err(error.to_string()),
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
                        })
                    })))
                });
            }
            ChatMsg::SendSubmitted(outcome) => {
                // Keyboard focus goes back to the composer (no-op while it is unbound).
                let _ = self.chat.composer_ref.request_focus();
                self.settle_mutation(
                    outcome
                        .as_ref()
                        .err()
                        .is_none_or(|error| !crate::state::uncertain(error)),
                );
                match outcome {
                    Ok(outcome) => {
                        // Bound to the submitting Agent and draft revision, not
                        // to whatever conversation is open now.
                        self.chat.drafts.ack(&outcome.agent, outcome.draft_rev);
                        // A SEND, or a resume's linked SEND, is new input of
                        // this conversation; steering joins the active turn.
                        let input = match outcome.what {
                            "Send" => Some(outcome.command_id.as_str()),
                            "Resume" => outcome.linked.as_deref(),
                            _ => None,
                        };
                        if let Some(command) = input
                            && !outcome.text.is_empty()
                            && let Some(convo) = self.chat.convo.as_mut()
                            && convo.agent() == outcome.agent
                        {
                            convo.note_local_input(command, &outcome.text);
                        }
                        self.rebuild_items();
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
                    Err(error) if crate::state::uncertain(&error) => {
                        self.alert(format!(
                            "Not confirmed (sending again reuses the same id): {error}"
                        ));
                    }
                    Err(error) => self.alert(format!("Not sent: {error}")),
                }
            }
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
            ChatMsg::AgentMenu(label, binding) => self.agent_menu(&label, binding, context),
            ChatMsg::ForceStopClosed(result) => {
                let Some((bound, _)) = self.chat.confirm_force.take() else {
                    return;
                };
                if result != ContentDialogResult::Primary {
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
        let previous: HashSet<&str> = self
            .chat
            .items
            .iter()
            .map(|item| item.key.as_str())
            .collect();
        let added = rendered
            .iter()
            .filter(|item| !previous.contains(item.key.as_str()))
            .count();
        if !self.chat.items.is_empty() {
            self.chat.unseen += added;
        }
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

    pub(super) fn open_conversation(&mut self, agent: &UiAgent, context: &ComponentContext<Self>) {
        self.nav.selection.select_agent(Some(agent.id.clone()));
        // Configure catalogue and picks for one Agent never carry over.
        self.configure = configure_ui::ConfigureState::default();
        self.chat.convo = Some(Conversation::new(&agent.id));
        self.chat.detail = None;
        self.chat.depth = Depth::default();
        self.chat.failures = 0;
        self.chat.unseen = 0;
        self.chat.items = Rc::new(Vec::new());
        self.chat.live = LiveStatus::Loading;
        self.chat.confirm_force = None;
        self.alert = None;
        if let Some((target, command, text)) = self.chat.pending_local.take() {
            if target == agent.id
                && let Some(convo) = self.chat.convo.as_mut()
            {
                convo.note_local_input(&command, &text);
            } else {
                self.chat.pending_local = Some((target, command, text));
            }
        }
        self.rebuild_items();
        self.set_status(format!("Reading retained history of {}…", agent.id));
        self.start_snapshot(context);
        if self.prefs.last_agent.as_deref() != Some(agent.id.as_str()) {
            self.prefs.last_agent = Some(agent.id.clone());
            self.save_preferences(context);
        }
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
        let header: View = match agent.as_deref() {
            Some(agent) => self.header_view(agent, context, palette),
            None => View::empty(),
        };
        let column = self.reading_width();
        let body: View = match agent {
            Some(_) => self.transcript_view(column, context, palette),
            None => {
                let project = self
                    .selected_project()
                    .map(|project| project.id.clone())
                    .or_else(|| {
                        (!self.launch.workspace.is_empty())
                            .then(|| nav::project_name(&self.launch.workspace, &[]))
                    });
                let line = match project {
                    Some(project) => format!("What should we build in {project}?"),
                    None => "What should we build?".to_owned(),
                };
                m3::title(line, theme::type_scale::TITLE_LARGE)
                    .grid_row(1)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .vertical_alignment(VerticalAlignment::Center)
                    .into()
            }
        };
        let jump: View = if self.chat.unseen > 0 && !self.chat.items.is_empty() {
            let label = format!("{} new", self.chat.unseen);
            m3::tonal(palette)
                .grid_row(2)
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(0.0, theme::spacing::SM, 0.0, 0.0))
                .automation_name(format!("Jump to latest, {label}"))
                .on_click(context.message(Message::Chat(ChatMsg::JumpToLatest)))
                .content(m3::icon_label(m3::glyph::ARROW_DOWN, &label))
        } else {
            View::empty()
        };
        let pending = self.pending_view(column, context, palette);
        let alert: View = match &self.alert {
            Some(text) => Border::new()
                .grid_row(4)
                .width(column)
                .max_width(theme::layout::READING_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(0.0, theme::spacing::SM, 0.0, 0.0))
                .background(palette.card())
                .corner_radius(theme::shape::MD)
                .padding(Thickness::new(
                    theme::spacing::MD,
                    theme::spacing::XS,
                    theme::spacing::XS,
                    theme::spacing::XS,
                ))
                .content(
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(theme::spacing::SM)
                        .children((
                            m3::text(text.clone(), theme::type_scale::BODY_SMALL)
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
            None => View::empty(),
        };
        let composer = self.composer_view(column, context, palette);

        Grid::new()
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
                body,
                jump,
                pending,
                alert,
                composer,
                self.handoff_dialog(context),
                self.force_stop_dialog(context),
            ))
    }

    /// The reading column's width: the reading width, or the main area less
    /// its gutters when narrower; content-sized until the window size is
    /// observed. Inside the ScrollViewer a stretched MaxWidth column was
    /// arranged in a slot wider than the viewport (off center), so the
    /// column gets an explicit width.
    fn reading_width(&self) -> Option<f64> {
        self.main_width().map(|width| {
            (width - 2.0 * theme::spacing::LG - theme::spacing::MD)
                .clamp(240.0, theme::layout::READING_WIDTH)
        })
    }

    /// One line: the title, the status dot, a connection note when not live,
    /// then the details toggle and the "…" menu.
    fn header_view(&self, agent: &str, context: &mut ViewContext<Self>, palette: Palette) -> View {
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
                let binding = MenuBinding {
                    agent: agent.to_owned(),
                    generation: detail.generation,
                };
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
                    .content(m3::icon(m3::glyph::MORE))
                    .menu(Menu::new(
                        items,
                        context.callback(move |label: String| {
                            Message::Chat(ChatMsg::AgentMenu(label, binding.clone()))
                        }),
                    ))
            }
            None => m3::icon_button(palette)
                .is_enabled(false)
                .automation_name("More actions")
                .content(m3::icon(m3::glyph::MORE)),
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
            .column_spacing(theme::spacing::SM)
            .margin(Thickness::new(
                theme::spacing::LG,
                theme::spacing::SM,
                theme::spacing::SM,
                theme::spacing::SM,
            ))
            .children((
                m3::title(title.clone(), theme::type_scale::TITLE_MEDIUM)
                    .max_width(560.0)
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_trimming(TextTrimming::CharacterEllipsis)
                    .is_text_selection_enabled(true),
                Border::new()
                    .grid_column(1)
                    .vertical_alignment(VerticalAlignment::Center)
                    .automation_name(format!("State: {state}"))
                    .content(m3::dot(palette.header_dot(tone))),
                m3::muted(note, theme::type_scale::BODY_SMALL)
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
                    .content(m3::icon(m3::glyph::INFO)),
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
                .margin(Thickness::new(0.0, 0.0, 0.0, theme::spacing::MD))
                .on_click(context.message(Message::Chat(ChatMsg::LoadOlderRequested)))
                .content(TextBlock::new().text("Load older messages")),
            _ => View::empty(),
        };
        let empty: View = if self.chat.items.is_empty() {
            m3::muted(
                match self.chat.live {
                    LiveStatus::Loading => "Loading…",
                    _ => "No messages yet.",
                },
                theme::type_scale::BODY_MEDIUM,
            )
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
                ),
            )]),
            None => View::empty(),
        };
        let items = Rc::clone(&self.chat.items);
        let keys = Rc::clone(&self.chat.items);
        // A palette change re-realises the rows in the new colors.
        let palette_code = palette
            .scheme()
            .map_or(0, |scheme| 1 + u64::from(scheme.dark));
        let source = VirtualSource::new(
            self.chat
                .items_revision
                .wrapping_mul(3)
                .wrapping_add(palette_code),
            earlier,
            move |index| keys[index].key.clone(),
            move |index| views::item_view(&items[index], &expanded, &callbacks, palette, None),
        );
        ScrollViewer::new()
            .grid_row(1)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Conversation")
            .content(
                StackPanel::new()
                    .width(column)
                    .max_width(theme::layout::READING_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .margin(Thickness::xy(0.0, theme::spacing::MD))
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
                let answer_box = m3::field(palette)
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
                        .corner_radius(theme::shape::MD)
                        .padding(Thickness::uniform(theme::spacing::MD))
                        .content(
                            StackPanel::new().spacing(theme::spacing::SM).children((
                                m3::text(
                                    format!("The Agent asks: {}", request.kind),
                                    theme::type_scale::TITLE_SMALL,
                                )
                                .foreground(on),
                                TextBlock::new()
                                    .text(request.summary.clone())
                                    .foreground(on)
                                    .text_wrapping(TextWrapping::Wrap)
                                    .is_text_selection_enabled(true),
                                answer_box,
                                StackPanel::new()
                                    .orientation(Orientation::Horizontal)
                                    .spacing(theme::spacing::SM)
                                    .children((
                                        m3::filled(palette)
                                            .is_enabled(!self.submitting())
                                            .on_click(context.message(Message::Chat(
                                                ChatMsg::ApprovalRequested(request.clone(), true),
                                            )))
                                            .content(TextBlock::new().text("Approve")),
                                        m3::tonal(palette)
                                            .is_enabled(!self.submitting())
                                            .on_click(context.message(Message::Chat(
                                                ChatMsg::ApprovalRequested(request.clone(), false),
                                            )))
                                            .content(TextBlock::new().text("Deny")),
                                        m3::text_button(palette)
                                            .is_enabled(!self.submitting())
                                            .on_click(context.message(Message::Chat(
                                                ChatMsg::AnswerRequested(request.clone()),
                                            )))
                                            .content(TextBlock::new().text("Answer")),
                                    )),
                            )),
                        ),
                )
            })
            .collect();
        StackPanel::new()
            .grid_row(3)
            .spacing(theme::spacing::SM)
            .width(column)
            .max_width(theme::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .keyed_children(pending_rows)
    }

    /// The one composer: the context row (project and profile), the field,
    /// then the model-and-effort and access chips with the actions on the
    /// right (Interrupt and Steer only while a turn runs).
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
                self.launch_context(context, palette),
                self.launch_chips(context, palette),
            ),
            (Some(_), Some(detail)) => (
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::XS)
                    .children((
                        m3::chip_static(
                            m3::glyph::FOLDER,
                            &nav::project_name(&detail.project, &self.registered_projects()),
                        ),
                        m3::chip_static(
                            m3::glyph::PROFILE,
                            &if detail.settings.profile == detail.harness {
                                detail.harness.clone()
                            } else {
                                format!("{} · {}", detail.settings.profile, detail.harness)
                            },
                        ),
                    )),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::XS)
                    .children((
                        self.model_chip(detail, context, palette),
                        m3::chip_static(
                            m3::glyph::SHIELD,
                            &launch_ui::access_label(detail.settings.permissions.as_deref()),
                        ),
                    )),
            ),
            (Some(_), None) => (View::empty(), View::empty()),
        };
        let placeholder = match agent.as_deref() {
            Some(agent) => format!("Message {}", self.agent_title(agent)),
            None => "Do anything".to_owned(),
        };
        let field = TextBox::new()
            .grid_row(1)
            .element_ref(&self.chat.composer_ref)
            .text(self.primed(&self.composer_text()).to_owned())
            .placeholder_text(placeholder)
            .automation_name("Message")
            .background(Brush::Solid(Color::transparent()))
            .border_thickness(Thickness::uniform(0.0))
            .accepts_return(true)
            .text_wrapping(TextWrapping::Wrap)
            .height(theme::layout::COMPOSER_FIELD_HEIGHT)
            .on_text_changed(
                context.callback(|value| Message::Chat(ChatMsg::ComposerChanged(value))),
            );
        let actions = self.composer_actions(agent, context, palette);
        Border::new()
            .grid_row(5)
            .width(column)
            .max_width(theme::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .margin(Thickness::new(
                0.0,
                theme::spacing::SM,
                0.0,
                theme::spacing::LG,
            ))
            .background(palette.raised())
            .corner_radius(theme::shape::LG)
            .padding(Thickness::uniform(theme::spacing::SM))
            .content(
                Grid::new()
                    .rows([GridLength::Auto, GridLength::Auto, GridLength::Auto])
                    .row_spacing(theme::spacing::XS)
                    .children((
                        context_row,
                        field,
                        Grid::new()
                            .grid_row(2)
                            .columns([GridLength::STAR, GridLength::Auto])
                            .column_spacing(theme::spacing::SM)
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
            .map(|project| (project.workspace.clone(), project.id.clone()))
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
        let generation = detail.generation;
        if self.submitting() {
            return m3::chip_static(m3::glyph::MODEL, &label);
        }
        m3::chip(palette)
            .automation_name(format!("Model and effort: {label}"))
            .content(m3::icon_label(m3::glyph::MODEL, &label))
            .menu(Menu::new(
                items,
                context.callback(move |label: String| {
                    Message::Configure(configure_ui::ConfigureMsg::Chosen {
                        agent: agent.clone(),
                        generation,
                        label,
                    })
                }),
            ))
    }

    /// Send (or Queue, Resume, Launch), with Interrupt and Steer while a
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
                .content(TextBlock::new().text("Resume")),
            (Some(agent), _) => m3::filled_icon(palette)
                .is_enabled(enabled && !draft_blank)
                .automation_name(if active_turn.is_some() {
                    "Queue message"
                } else {
                    "Send"
                })
                .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                    DraftAction::Send { agent },
                ))))
                .content(m3::icon(m3::glyph::SEND)),
            (None, _) => m3::filled_icon(palette)
                .is_enabled(enabled && !draft_blank)
                .automation_name("Start task")
                .on_click(context.message(Message::Launch(LaunchMsg::LaunchRequested)))
                .content(m3::icon(m3::glyph::SEND)),
        };
        let turn_actions: View =
            match (agent.as_deref(), detail, active_turn.as_deref()) {
                (Some(agent), Some(detail), Some(turn)) if !detail.observe_only => {
                    let bound = BoundAction::turn_control(agent, detail.generation, turn);
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(theme::spacing::XS)
                        .children((
                            m3::text_button(palette)
                                .is_enabled(enabled)
                                .automation_name(format!("Interrupt turn {turn}"))
                                .on_click(context.message(Message::Chat(
                                    ChatMsg::InterruptRequested(bound.clone()),
                                )))
                                .content(m3::icon_label(m3::glyph::STOP, "Interrupt")),
                            m3::tonal(palette)
                                .is_enabled(enabled && !draft_blank)
                                .automation_name(format!("Steer turn {turn} with the message"))
                                .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                                    DraftAction::Steer(bound),
                                ))))
                                .content(m3::icon_label(m3::glyph::STEER, "Steer")),
                        ))
                }
                _ => View::empty(),
            };
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(theme::spacing::SM)
            .children((turn_actions, primary))
    }

    /// The Force stop question, bound to the Agent and generation of its
    /// menu click.
    fn force_stop_dialog(&self, context: &mut ViewContext<Self>) -> View {
        let Some((bound, title)) = &self.chat.confirm_force else {
            // Reactor hides an open dialog only through is_open(false).
            return ContentDialog::new().is_open(false).content(View::empty());
        };
        ContentDialog::new()
            .is_open(true)
            .title(format!("Force stop {title}?"))
            .primary_button_text("Force stop")
            .close_button_text("Cancel")
            .on_closed(context.callback(|result| Message::Chat(ChatMsg::ForceStopClosed(result))))
            .content(
                TextBlock::new()
                    .text(format!(
                        "This ends Agent {} (generation {}) at once instead of a clean stop; an active turn is cut off.",
                        bound.agent, bound.generation
                    ))
                    .text_wrapping(TextWrapping::Wrap),
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
        let (models, efforts) = configure::options(reply, detail.settings.next_model());
        configure::resolve(label, &models, &efforts)
    }
}
