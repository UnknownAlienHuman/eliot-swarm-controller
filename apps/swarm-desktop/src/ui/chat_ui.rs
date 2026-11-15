//! The conversation: retained history and live delivery, the composer, Send/Interrupt and pending requests.

use super::*;
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
                    Some((_, Err(error))) => self.set_status(format!("Copy failed: {error}")),
                    None => self.set_status("That message is no longer resident."),
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
                    Some(Err(error)) => self.set_status(format!("Copy failed: {error}")),
                    None => self.set_status("That code block is no longer resident."),
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
                    self.set_status(format!(
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
                        self.set_status(format!(
                            "Outcome uncertain (the same button reuses its id): {error}"
                        ));
                    }
                    Err(error) => self.set_status(format!("Not admitted: {error}")),
                }
            }
            ChatMsg::InterruptRequested(bound) => {
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let turn = bound.turn.clone().unwrap_or_default();
                let key = format!("interrupt:{}:{}:{turn}", bound.agent, bound.generation);
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.set_status(format!("Interrupting turn {turn} ({mutation_id})…"));
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(async {
                        client::submit_interrupt(
                            &data_dir,
                            mutation_id,
                            &bound.agent,
                            bound.generation,
                            bound.turn.clone().unwrap_or_default(),
                        )
                        .await
                        .map(|reply| submit_note(&reply))
                        .map_err(|error| error.to_string())
                    });
                    Message::Chat(ChatMsg::ControlSubmitted("Interrupt".to_owned(), result))
                });
            }
            ChatMsg::ApprovalRequested(request, approve) => {
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let verb = if approve { "approve" } else { "deny" };
                let key = format!(
                    "{verb}:{}:{}",
                    request.binding.agent,
                    request.binding.request.clone().unwrap_or_default()
                );
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.set_status("Answering that exact request…");
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(async {
                        client::submit_approval(
                            &data_dir,
                            mutation_id,
                            client::ApprovalInput {
                                agent: request.binding.agent.clone(),
                                generation: request.binding.generation,
                                request: request.binding.request.clone().unwrap_or_default(),
                                approve,
                                choice: None,
                                feedback: None,
                            },
                        )
                        .await
                        .map(|reply| submit_note(&reply))
                        .map_err(|error| error.to_string())
                    });
                    Message::Chat(ChatMsg::ControlSubmitted(verb.to_owned(), result))
                });
            }
            ChatMsg::AnswerRequested(request) => {
                if self.submitting() {
                    return;
                }
                let Ok(data_dir) = self.data_dir.clone() else {
                    return;
                };
                let answer = self
                    .chat
                    .answers
                    .get(request.binding.request.as_deref().unwrap_or_default())
                    .cloned()
                    .unwrap_or_default();
                let answers = match serde_json::from_str::<serde_json::Value>(&answer) {
                    Ok(answers) if answers.is_object() => answers,
                    _ => {
                        self.set_status("Answers must be a JSON object keyed by question id.");
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
                let mutation_id = self.ledger.id_for(&key);
                self.in_flight_key = Some(key);
                self.set_status("Answering that exact request…");
                let _task = context.spawn_background(move |_| {
                    let result = client::block_on(async {
                        client::submit_answer(
                            &data_dir,
                            mutation_id,
                            &request.binding.agent,
                            request.binding.generation,
                            request.binding.request.as_deref().unwrap_or_default(),
                            answers,
                        )
                        .await
                        .map(|reply| submit_note(&reply))
                        .map_err(|error| error.to_string())
                    });
                    Message::Chat(ChatMsg::ControlSubmitted("Answer".to_owned(), result))
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
                    Err(error) if crate::state::uncertain(&error) => self.set_status(format!(
                        "{what}: outcome uncertain (retry reuses its id): {error}"
                    )),
                    Err(error) => self.set_status(format!("{what} not admitted: {error}")),
                }
            }
        }
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
        // Configure picks and catalogue for one Agent never carry over.
        self.configure = configure_ui::ConfigureState {
            expanded: self.configure.expanded,
            ..configure_ui::ConfigureState::default()
        };
        self.chat.convo = Some(Conversation::new(&agent.id));
        self.chat.detail = None;
        self.chat.depth = Depth::default();
        self.chat.failures = 0;
        self.chat.unseen = 0;
        self.chat.items = Rc::new(Vec::new());
        self.chat.live = LiveStatus::Loading;
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
        self.set_status(format!("{what} ({mutation_id})…"));
        let what = what.to_owned();
        let _task = context.spawn_background(move |_| {
            Message::Chat(ChatMsg::ControlSubmitted(
                what,
                work(&data_dir, mutation_id),
            ))
        });
    }

    /// The conversation pane: Agent header, transcript in a bounded reading
    /// column, pending requests, the launch picker for a new conversation
    /// and the composer with its primary action.
    pub(super) fn chat_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let gutter = theme::spacing::XL;
        let agent = self.selected_agent_id();
        // State, harness and model as chips; generation, profile, turn and
        // configuration details live in the monitor and the configure panel.
        let header: View = match (&self.chat.detail, agent.as_deref()) {
            (Some(detail), Some(agent)) => {
                let model = detail
                    .settings
                    .effective_model
                    .as_deref()
                    .or(detail.settings.profile_model.as_deref())
                    .unwrap_or("default model")
                    .to_owned();
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .children((
                        m3::title(agent.to_owned(), theme::type_scale::TITLE_LARGE)
                            .vertical_alignment(VerticalAlignment::Center)
                            .margin(Thickness::new(0.0, 0.0, theme::spacing::XS, 0.0))
                            .is_text_selection_enabled(true),
                        m3::status_chip(palette, &detail.state),
                        m3::chip(palette, StatusTone::Neutral, &detail.harness),
                        m3::chip(palette, StatusTone::Neutral, &model),
                    ))
            }
            (None, Some(agent)) => StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(theme::spacing::MD)
                .children((
                    m3::title(agent.to_owned(), theme::type_scale::TITLE_LARGE),
                    m3::muted("reading…", theme::type_scale::BODY_MEDIUM)
                        .vertical_alignment(VerticalAlignment::Center),
                )),
            _ => StackPanel::new().spacing(2.0).children((
                m3::title("New conversation", theme::type_scale::TITLE_LARGE),
                m3::muted(
                    "Pick a profile and a workspace, then write the first message.",
                    theme::type_scale::BODY_MEDIUM,
                ),
            )),
        };
        // A is bound when the dialog opens: the generation shown now.
        let open_handoff = agent
            .clone()
            .zip(self.chat.detail.as_ref())
            .map(|(agent, detail)| {
                context.message(Message::Handoff(handoff_ui::HandoffMsg::Open {
                    agent,
                    generation: detail.generation,
                }))
            });
        let hand_off = match open_handoff {
            Some(open) => m3::tonal(palette)
                .is_enabled(!self.submitting())
                .on_click(open),
            None => m3::tonal(palette).is_enabled(false),
        }
        .grid_column(1)
        .automation_name("Hand off or switch")
        .content(m3::icon_label(m3::glyph::HAND_OFF, "Hand off or switch…"));

        let top = StackPanel::new()
            .grid_row(0)
            .spacing(theme::spacing::SM)
            .margin(Thickness::new(
                gutter,
                theme::spacing::LG,
                gutter,
                theme::spacing::SM,
            ))
            .children((
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .column_spacing(theme::spacing::MD)
                    .children((header, hand_off)),
                self.configure_view(context, palette),
            ));

        // The start of the shown history: "Load older" centered above the
        // first message, with the coverage it extends.
        let history_start: View = match self.chat.convo.as_ref() {
            Some(convo) => {
                let coverage = convo.coverage();
                let load_older: View = if coverage.can_load_older() {
                    m3::text_button(palette)
                        .is_enabled(!matches!(self.chat.live, LiveStatus::Loading))
                        .horizontal_alignment(HorizontalAlignment::Center)
                        .on_click(context.message(Message::Chat(ChatMsg::LoadOlderRequested)))
                        .content(TextBlock::new().text("Load older"))
                } else {
                    View::empty()
                };
                let mut lines = coverage.lines(convo.inputs_unavailable());
                if self.chat.items.is_empty() {
                    lines.push("No retained turns in coverage yet.".to_owned());
                }
                // One centered TextBlock per line (Reactor 0.100.0 has no
                // TextAlignment).
                let lines: Vec<KeyedView> = lines
                    .into_iter()
                    .enumerate()
                    .map(|(index, line)| {
                        KeyedView::new(
                            index,
                            m3::muted(line, theme::type_scale::BODY_SMALL)
                                .horizontal_alignment(HorizontalAlignment::Center)
                                .text_wrapping(TextWrapping::Wrap),
                        )
                    })
                    .collect();
                StackPanel::new()
                    .spacing(theme::spacing::XS)
                    .margin(Thickness::new(0.0, 0.0, 0.0, theme::spacing::LG))
                    .children((load_older, StackPanel::new().keyed_children(lines)))
            }
            None => View::empty(),
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
        // The reading column has the composer's width: the reading width,
        // or the pane less its gutters and scroll bar when narrower. Inside
        // the ScrollViewer a stretched MaxWidth column was arranged in a
        // slot wider than the viewport (off center), so it is centered at an
        // explicit width; content-sized until the window size is observed.
        let column = self.chat_pane_width().map(|pane| {
            (pane - 2.0 * gutter - theme::spacing::LG).clamp(240.0, theme::layout::READING_WIDTH)
        });
        let transcript = ScrollViewer::new()
            .grid_row(1)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Conversation")
            .content(
                StackPanel::new()
                    .width(column)
                    .max_width(theme::layout::READING_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .margin(Thickness::xy(0.0, theme::spacing::SM))
                    .children((
                        history_start,
                        ItemsRepeater::new().virtual_source(source),
                        latest,
                    )),
            );
        // Reactor 0.100.0 cannot tell whether the transcript is scrolled to
        // the bottom (no offset, extent or ViewChanged), so the chip shows
        // whenever there is history. It has its own row between the
        // transcript and the composer, right-aligned with the composer, so
        // it never covers a message and the transcript's last item stays
        // fully visible.
        let jump: View = if self.chat.items.is_empty() {
            View::empty()
        } else {
            let label = if self.chat.unseen > 0 {
                format!("Jump to latest · {} new", self.chat.unseen)
            } else {
                "Jump to latest".to_owned()
            };
            Border::new()
                .grid_row(2)
                .max_width(theme::layout::READING_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Stretch)
                .margin(Thickness::new(gutter, theme::spacing::SM, gutter, 0.0))
                .content(
                    m3::tonal(palette)
                        .horizontal_alignment(HorizontalAlignment::Right)
                        .automation_name(label.clone())
                        .on_click(context.message(Message::Chat(ChatMsg::JumpToLatest)))
                        .content(m3::icon_label(m3::glyph::ARROW_DOWN, &label)),
                )
        };

        let pending_rows: Vec<KeyedView> = self
            .chat
            .detail
            .as_ref()
            .map(|detail| detail.pending.as_slice())
            .unwrap_or_default()
            .iter()
            .map(|request| {
                let key = request.binding.request.clone().unwrap_or_default();
                let answer_box = TextBox::new()
                    .text(self.chat.answers.get(&key).cloned().unwrap_or_default())
                    .placeholder_text("{\"question\": {\"answers\": [\"…\"]}}")
                    .automation_name("Answer JSON")
                    .on_text_changed({
                        let key = key.clone();
                        context.callback(move |text: String| {
                            Message::Chat(ChatMsg::AnswerChanged(key.clone(), text))
                        })
                    });
                let (fill, on) = palette.chip(StatusTone::Attention);
                KeyedView::new(
                    key,
                    Border::new()
                        .background(fill)
                        .corner_radius(theme::shape::LG)
                        .padding(Thickness::uniform(theme::spacing::LG))
                        .content(
                            StackPanel::new().spacing(theme::spacing::SM).children((
                                m3::text(
                                    format!("Pending {} · turn {}", request.kind, request.turn_id),
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
        let pending = StackPanel::new()
            .grid_row(3)
            .spacing(theme::spacing::SM)
            .max_width(theme::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Stretch)
            .margin(Thickness::xy(gutter, 0.0))
            .keyed_children(pending_rows);

        let has_agent = agent.is_some();
        let launch: View = if has_agent {
            View::empty()
        } else {
            Border::new()
                .grid_row(4)
                .max_width(theme::layout::READING_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Stretch)
                .margin(Thickness::new(gutter, theme::spacing::SM, gutter, 0.0))
                .content(self.launch_card(context, palette))
        };

        let composer = TextBox::new()
            .element_ref(&self.chat.composer_ref)
            .text(self.primed(&self.composer_text()).to_owned())
            .placeholder_text(if has_agent {
                "Message the selected Agent…"
            } else {
                "First message for a new Agent…"
            })
            .automation_name("Message")
            .background(palette.surface_container_high())
            .border_thickness(Thickness::uniform(0.0))
            .accepts_return(true)
            .text_wrapping(TextWrapping::Wrap)
            .height(theme::layout::COMPOSER_HEIGHT - 24.0)
            .on_text_changed(
                context.callback(|value| Message::Chat(ChatMsg::ComposerChanged(value))),
            );
        // Turn controls are bound now, from the detail on screen: the Agent,
        // its generation and its active turn (Steer, Interrupt) or its
        // retained generation (Resume).
        let detail = self.chat.detail.as_ref();
        let active_turn = detail.and_then(|detail| detail.active_turn.clone());
        let resumable = detail.is_some_and(|detail| {
            matches!(detail.state.as_str(), "stopped" | "failed" | "lost")
                && detail.active_turn.is_none()
        });
        let draft_blank = self.composer_text().trim().is_empty();
        let primary = match (agent.clone(), detail) {
            (Some(agent), Some(detail)) if resumable => {
                let label = if draft_blank {
                    "Resume"
                } else {
                    "Resume with message"
                };
                m3::filled(palette)
                    .is_enabled(!self.submitting())
                    .automation_name(format!("{label} (generation {})", detail.generation))
                    .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                        DraftAction::Resume {
                            agent,
                            generation: detail.generation,
                        },
                    ))))
                    .content(m3::icon_label(m3::glyph::SEND, label))
            }
            (Some(agent), _) => {
                let label = if active_turn.is_some() {
                    "Queue"
                } else {
                    "Send"
                };
                m3::filled(palette)
                    .is_enabled(!self.submitting())
                    .automation_name(label)
                    .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                        DraftAction::Send { agent },
                    ))))
                    .content(m3::icon_label(m3::glyph::SEND, label))
            }
            (None, _) => m3::filled(palette)
                .is_enabled(!self.submitting())
                .automation_name("Launch Agent")
                .on_click(context.message(Message::Launch(LaunchMsg::LaunchRequested)))
                .content(m3::icon_label(m3::glyph::SEND, "Launch Agent")),
        };
        // Steer and Interrupt exist only while a turn is active.
        let turn_actions: View =
            match (agent.as_deref(), detail, active_turn.as_deref()) {
                (Some(agent), Some(detail), Some(turn)) => {
                    let bound = BoundAction::turn_control(agent, detail.generation, turn);
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(theme::spacing::SM)
                        .children((
                            m3::tonal(palette)
                                .is_enabled(!self.submitting())
                                .automation_name(format!("Interrupt turn {turn}"))
                                .on_click(context.message(Message::Chat(
                                    ChatMsg::InterruptRequested(bound.clone()),
                                )))
                                .content(m3::icon_label(m3::glyph::INTERRUPT, "Interrupt")),
                            m3::tonal(palette)
                                .is_enabled(!self.submitting() && !draft_blank)
                                .automation_name(format!("Steer turn {turn} with the message"))
                                .on_click(context.message(Message::Chat(ChatMsg::DraftRequested(
                                    DraftAction::Steer(bound),
                                ))))
                                .content(m3::icon_label(m3::glyph::STEER, "Steer")),
                        ))
                }
                _ => View::empty(),
            };
        let composer_card = Border::new()
            .grid_row(5)
            .background(palette.surface_container_high())
            .corner_radius(theme::shape::XL)
            .padding(Thickness::uniform(theme::spacing::SM))
            .max_width(theme::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Stretch)
            .margin(Thickness::new(
                gutter,
                theme::spacing::MD,
                gutter,
                theme::spacing::LG,
            ))
            .content(
                Grid::new()
                    .rows([GridLength::Auto, GridLength::Auto])
                    .row_spacing(theme::spacing::XS)
                    .children((
                        composer,
                        StackPanel::new()
                            .grid_row(1)
                            .orientation(Orientation::Horizontal)
                            .horizontal_alignment(HorizontalAlignment::Right)
                            .spacing(theme::spacing::SM)
                            .children((turn_actions, primary)),
                    )),
            );
        let dialog = self.handoff_dialog(context);

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
                top,
                transcript,
                jump,
                pending,
                launch,
                composer_card,
                dialog,
            ))
    }
}
