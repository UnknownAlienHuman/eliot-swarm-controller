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
        if detail.state != eliot_core::AgentState::Lost || detail.observe_only {
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

    /// Why the open Agent stopped ("Stopped by a Swarm restart"), one line
    /// above the composer. Resume itself is the composer's one action (the
    /// filled button, or Send with a message), never repeated here.
    pub(super) fn loss_notice(&self, column: Option<f64>) -> Option<View> {
        let detail = self.chat.detail.as_ref()?;
        let agent = self.selected_agent_id()?;
        let (read, generation, words) = self.chat.loss.as_ref()?;
        let words = words.as_deref()?;
        if *read != agent
            || *generation != detail.generation
            || detail.state != eliot_core::AgentState::Lost
        {
            return None;
        }
        Some(
            Border::new()
                .grid_row(4)
                .width(column)
                .max_width(tokens::layout::READING_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                .padding(Thickness::xy(tokens::spacing::MD, 0.0))
                .automation_name(words.to_owned())
                .content(
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .children((
                            m3::icon(m3::glyph::INFO),
                            m3::secondary(words.to_owned())
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        )),
                ),
        )
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
}
