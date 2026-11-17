use super::*;

impl DesktopApp {
    /// Submit one draft action on a worker. `fallback` marks the send that
    /// follows a steer refused because the turn just ended.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn_draft(
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

    pub(super) fn send_submitted(
        &mut self,
        outcome: Result<SendOutcome, SendFailure>,
        context: &ComponentContext<Self>,
    ) {
        // Keyboard focus goes back to the composer (no-op while it is unbound).
        let _ = self.chat.composer_ref.request_focus();
        self.settle_mutation(settles(&outcome));
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
                    && STEER_FALLBACK.contains(&failure.error.code.as_str())
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
                        failure.error.user_message()
                    ));
                } else {
                    self.alert(format!("Not sent: {}", failure.error.user_message()));
                }
            }
        }
    }
}
