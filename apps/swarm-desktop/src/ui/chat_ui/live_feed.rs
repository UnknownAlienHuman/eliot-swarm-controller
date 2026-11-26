use super::*;

impl DesktopApp {
    /// Start (or restart) the conversation feed: new epoch, cancel the old
    /// live task, capture a cursor and reload the current reads.
    pub(in crate::ui) fn start_snapshot(&mut self, context: &ComponentContext<Self>) {
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

    pub(in crate::ui) fn spawn_delivery(
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

    pub(in crate::ui) fn spawn_backoff(&mut self, context: &ComponentContext<Self>, delay_ms: u64) {
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
    pub(in crate::ui) fn queue_reread(
        &mut self,
        invalidation: Invalidation,
        context: &ComponentContext<Self>,
    ) {
        self.chat.invalidated.merge(invalidation);
        // While no daemon answers, the reconnect's snapshot reads it all.
        if self.chat.reread_inflight || !self.chat.invalidated.any() || self.daemon_down() {
            return;
        }
        let (Some((agent, deliveries)), Ok(data_dir)) = (
            self.chat
                .convo
                .as_ref()
                .map(|convo| (convo.agent().to_owned(), convo.deliveries())),
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
                deliveries,
            );
            // A failed reread hands its work back so it is retried, not lost.
            Message::Chat(ChatMsg::RereadLoaded(
                epoch,
                result.map(Box::new).map_err(|error| (error, work)),
            ))
        });
    }

    pub(in crate::ui) fn follow_next(&mut self, next: Next, context: &ComponentContext<Self>) {
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
            Next::Backoff { reason } => {
                let delay_ms = self.chat.pace.fail(0);
                let attempt = self.chat.pace.failures();
                // The daemon is being started again: its reply resumes the
                // feed, and the notice counts its attempts.
                if self.daemon_down() {
                    self.chat.live_task = None;
                    return;
                }
                self.chat.live = LiveStatus::Reconnecting { attempt, reason };
                self.spawn_backoff(context, delay_ms);
                self.link_suspect(context);
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
    pub(super) fn check_waiting_inputs(&mut self, context: &ComponentContext<Self>) {
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
        let ended = matches!(
            detail.state,
            eliot_core::AgentState::Failed
                | eliot_core::AgentState::Stopped
                | eliot_core::AgentState::Lost
        );
        if detail.active_turn.is_some() || !(ended || detail.state == eliot_core::AgentState::Idle)
        {
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
    pub(super) fn check_loss(&mut self, context: &ComponentContext<Self>) {
        let (Some(detail), Some(agent), Ok(data_dir)) = (
            self.chat.detail.as_ref(),
            self.selected_agent_id(),
            self.data_dir.clone(),
        ) else {
            return;
        };
        if !matches!(
            detail.state,
            eliot_core::AgentState::Lost | eliot_core::AgentState::Failed
        ) || detail.observe_only
        {
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
                .map_err(|error| error.to_string());
            Message::Chat(ChatMsg::LossRead(agent, generation, summary))
        });
    }

    /// Why the open Agent ended, above the composer: a lost one's "Stopped
    /// by a Swarm restart" as a service line; a failed one's diagnosis as
    /// the "What went wrong" card with Details. Resume itself is the
    /// composer's one action, never repeated here.
    pub(super) fn loss_notice(&self, context: &mut ViewContext<Self>) -> Option<View> {
        let detail = self.chat.detail.as_ref()?;
        let agent = self.selected_agent_id()?;
        let (read, generation, words) = self.chat.loss.as_ref()?;
        let words = words.as_deref()?;
        if *read != agent || *generation != detail.generation {
            return None;
        }
        let place = Border::new()
            .grid_row(4)
            .column(tokens::layout::READING_WIDTH, tokens::spacing::SM, 0.0)
            .automation_name(words);
        match detail.state {
            eliot_core::AgentState::Lost => Some(
                place
                    .content(controls::row_grid(
                        Some(controls::muted_icon(controls::glyph::RESTART)),
                        controls::secondary(words.to_owned())
                            .text_trimming(TextTrimming::CharacterEllipsis)
                            .into(),
                        None,
                        tokens::spacing::SM,
                    ))
                    .into(),
            ),
            eliot_core::AgentState::Failed => Some(
                place
                    .background(ThemeBrush::SystemCriticalBackground)
                    .corner_radius(tokens::radius::CONTAINER)
                    .padding(Thickness::new(
                        tokens::spacing::INSET,
                        tokens::layout::STRIP_INSET,
                        tokens::layout::STRIP_INSET,
                        tokens::layout::STRIP_INSET,
                    ))
                    .content(
                        Grid::new()
                            .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                            .column_spacing(tokens::spacing::ROW)
                            .children((
                                controls::critical_icon(controls::glyph::ERROR, tokens::icon::ROW),
                                Grid::new()
                                    .grid_column(1)
                                    .columns([GridLength::Auto, GridLength::STAR])
                                    .column_spacing(tokens::spacing::XS)
                                    .vertical_alignment(VerticalAlignment::Center)
                                    .children((
                                        controls::text("What went wrong", tokens::text::BUTTON),
                                        controls::body(words.to_owned())
                                            .grid_column(1)
                                            .text_trimming(TextTrimming::CharacterEllipsis),
                                    )),
                                controls::text_button()
                                    .grid_column(2)
                                    .automation_name("Details")
                                    .on_click(context.message(Message::Monitor(
                                        MonitorMsg::PanelChosen(Panel::Details),
                                    )))
                                    .content(controls::label("Details")),
                            )),
                    )
                    .into(),
            ),
            _ => None,
        }
    }

    /// Retry a local input that definitely did not start: a launch that
    /// never ran starts again as submitted (a new task, new mutation id);
    /// any other input goes back through Send (or Resume) with its text.
    pub(super) fn retry_input(&mut self, command: &str, context: &ComponentContext<Self>) {
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
                self.chat.drafts.borrow_mut().set(None, text);
                self.update(Message::Launch(LaunchMsg::LaunchRequested), context);
            }
            _ => {
                // A fresh edit after a definite failure: any unresolved
                // snapshot for this Agent is abandoned (its key released).
                self.store_draft_text(&agent, text);
                if let Some(primary) = self.primary_action() {
                    self.update(primary, context);
                }
            }
        }
    }

    pub(in crate::ui) fn open_conversation(
        &mut self,
        agent: &UiAgent,
        context: &ComponentContext<Self>,
    ) {
        self.nav.selection.select_agent(Some(agent.id.clone()));
        // Configure catalogue and picks for one Agent never carry over.
        self.configure = configure_ui::ConfigureState::default();
        self.chat.convo = Some(Conversation::new(&agent.id));
        self.chat.detail = None;
        self.chat.depth = Depth::default();
        self.chat.pace.reset();
        self.chat.items = Rc::new(Vec::new());
        self.chat.live = LiveStatus::Loading;
        self.chat.confirm_force = None;
        self.chat.goal = None;
        self.chat.goal_error = None;
        self.chat.decisions = None;
        self.chat.decisions_facts = None;
        self.chat.decisions_error = None;
        self.chat.holds = None;
        self.chat.decision_at = 0;
        self.chat.decide_reason.clear();
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
}

/// The exact live turn a draft belongs to: the open Agent's generation and
/// active turn, as its readback has them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::ui) struct PreviewTurn {
    agent: String,
    generation: i64,
    turn_id: String,
}

/// One `turn.preview.watch` reply, with its preview's Markdown.
pub(in crate::ui) type PreviewReply =
    eliot_core::Result<(eliot_core::api::TurnPreviewWatchResponse, Option<Doc>)>;

/// The open running turn's live draft (`turn.preview.watch`: best effort,
/// memory only) and its one watch. The draft is never in the conversation,
/// its items or any store.
#[derive(Default)]
pub(in crate::ui) struct PreviewState {
    /// The turn watched: the visible running turn.
    turn: Option<PreviewTurn>,
    /// The chat epoch the watch belongs to: a newer one drops it.
    epoch: u64,
    /// Counts the calls: only the reply of the latest one is taken.
    call: u64,
    /// The last non-null revision returned.
    revision: Option<u64>,
    /// The call in flight. Cancelling it drops its request, and with it
    /// the request's slot among the daemon's watches.
    task: Option<ComponentTask>,
    /// Failed calls in a row (`busy` included): each waits longer.
    pace: live::Pace,
    /// The daemon said the turn ended: nothing more is asked for it.
    ended: bool,
    draft: Option<ShownDraft>,
}

/// The draft as drawn, with its message's id and text.
struct ShownDraft {
    message_id: String,
    part: crate::chat::Part,
    view: views::Draft,
}

impl DesktopApp {
    /// The open Agent's running turn while its conversation is shown and
    /// the transcript does not hold the turn's end yet.
    fn visible_turn(&self) -> Option<(&str, i64, &str)> {
        let convo = self.chat.convo.as_ref()?;
        let detail = self.chat.detail.as_ref()?;
        let turn_id = detail.active_turn.as_deref()?;
        (self.main == Main::Conversation
            && self.nav.selection.agent.as_deref() == Some(convo.agent())
            && detail.agent == convo.agent()
            && !convo.turn_ended(detail.generation, turn_id))
        .then_some((detail.agent.as_str(), detail.generation, turn_id))
    }

    /// Keep exactly one watch, for the visible running turn (run after
    /// every message). Another turn, a new chat epoch or a closed chat
    /// cancels the call in flight, so its request is dropped, not just its
    /// reply; the draft goes with its turn. The committed message with the
    /// draft's id, or its turn's committed end, replaces the draft.
    pub(in crate::ui) fn sync_preview(&mut self, context: &ComponentContext<Self>) {
        let visible = self.visible_turn();
        let same = match (&self.chat.preview.turn, visible) {
            (Some(turn), Some((agent, generation, turn_id))) => {
                turn.agent == agent && turn.generation == generation && turn.turn_id == turn_id
            }
            (None, None) => true,
            _ => false,
        };
        let turn = (!same).then(|| {
            visible.map(|(agent, generation, turn_id)| PreviewTurn {
                agent: agent.to_owned(),
                generation,
                turn_id: turn_id.to_owned(),
            })
        });
        let preview = &mut self.chat.preview;
        if turn.is_some() || preview.epoch != self.chat.epoch {
            if let Some(task) = preview.task.take() {
                task.cancel();
            }
            if let Some(turn) = turn {
                preview.turn = turn;
                preview.draft = None;
                preview.ended = false;
                preview.pace.reset();
            }
            // A new epoch can follow a daemon restart: ask for the current
            // state, keeping the draft until it answers.
            preview.revision = None;
            preview.epoch = self.chat.epoch;
        }
        if let (Some(turn), Some(draft), Some(convo)) =
            (&preview.turn, &preview.draft, &self.chat.convo)
            && convo.holds_message(turn.generation, &turn.turn_id, &draft.message_id)
        {
            preview.draft = None;
        }
        if preview.turn.is_some() && preview.task.is_none() && !preview.ended {
            self.watch_preview(context);
        }
    }

    /// One call for the watched turn: the current state first, then a long
    /// poll from the last revision. A failed call waits out its backoff
    /// before its reply returns; leaving the turn cancels it at once.
    fn watch_preview(&mut self, context: &ComponentContext<Self>) {
        let (Some(turn), Ok(data_dir)) = (self.chat.preview.turn.clone(), self.data_dir.clone())
        else {
            return;
        };
        let preview = &mut self.chat.preview;
        preview.call += 1;
        let (epoch, call, revision) = (self.chat.epoch, preview.call, preview.revision);
        let failed_wait = {
            let mut pace = preview.pace;
            pace.fail(0)
        };
        preview.task = Some(context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let reply = client::block_on(feed::cancellable(
                client::turn_preview_watch(
                    &data_dir,
                    &turn.agent,
                    turn.generation,
                    &turn.turn_id,
                    revision,
                ),
                &cancelled,
            ))
            .map(|reply| {
                if reply.is_err() {
                    let _ = feed::backoff(failed_wait, &cancelled);
                }
                Box::new(reply.map(|reply| {
                    let doc = reply
                        .preview
                        .as_ref()
                        .map(|preview| crate::markdown::parse(&preview.text));
                    (reply, doc)
                }))
            });
            Message::Chat(ChatMsg::PreviewLoaded(epoch, call, turn, reply))
        }));
    }

    /// Apply the latest call's reply: a new revision replaces the draft (or
    /// clears it), a timeout keeps it, an end clears it for good. A failure
    /// (`busy` included) keeps the draft and says nothing: the preview is
    /// best effort, and the next call waits longer.
    pub(super) fn preview_loaded(
        &mut self,
        epoch: u64,
        call: u64,
        turn: PreviewTurn,
        reply: Option<Box<PreviewReply>>,
    ) {
        let preview = &mut self.chat.preview;
        if epoch != self.chat.epoch || call != preview.call || preview.turn.as_ref() != Some(&turn)
        {
            return;
        }
        let Some(reply) = reply else {
            return;
        };
        preview.task = None;
        match *reply {
            Ok((reply, doc)) => {
                preview.pace.reset();
                if reply.ended {
                    preview.ended = true;
                    preview.draft = None;
                } else if reply.revision != preview.revision {
                    preview.revision = reply.revision;
                    preview.draft = reply.preview.zip(doc).map(|(shown, doc)| {
                        let part = crate::chat::Part::draft(&shown.message_id, shown.text);
                        let view = views::Draft {
                            item: crate::chat::turn_item_key(turn.generation, &turn.turn_id),
                            part: views::Shared(Rc::new(views::draft_part(&part, doc))),
                            truncated: shown.truncated,
                        };
                        ShownDraft {
                            message_id: shown.message_id,
                            part,
                            view,
                        }
                    });
                }
            }
            Err(_) => {
                let _ = preview.pace.fail(0);
            }
        }
    }

    /// The open turn's draft as the transcript draws it, for the Agent
    /// whose conversation is shown.
    pub(super) fn shown_draft(&self, conversation: &str) -> Option<views::Draft> {
        let preview = &self.chat.preview;
        preview
            .turn
            .as_ref()
            .filter(|turn| turn.agent == conversation)
            .and(preview.draft.as_ref())
            .map(|draft| draft.view.clone())
    }

    /// A shown part's text for Copy: a transcript part, or the draft.
    pub(super) fn shown_part_text(&self, key: &str) -> Option<String> {
        self.chat
            .convo
            .as_ref()
            .and_then(|convo| convo.part_text(key))
            .or_else(|| {
                self.chat
                    .preview
                    .draft
                    .as_ref()
                    .filter(|draft| draft.part.key == key)
                    .map(|draft| draft.part.text.clone())
            })
    }
}
