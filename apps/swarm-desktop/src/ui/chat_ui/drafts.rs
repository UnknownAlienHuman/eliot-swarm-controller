use super::*;

impl DesktopApp {
    /// Submit one draft action on a worker.
    #[allow(
        clippy::too_many_arguments,
        reason = "The operation's distinct inputs remain explicit at its established call boundary."
    )]
    pub(super) fn spawn_draft(
        &self,
        action: DraftAction,
        mutation_id: String,
        ledger_key: String,
        text: String,
        revision: u64,
        blank: bool,
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
                    Err(error) if crate::state::uncertain(&error) => {
                        // Uncertain reply: read back what this mutation id
                        // admitted instead of submitting again under a new id.
                        let (command_id, state, linked) =
                            recover_retained(&action, &mutation_id, blank, async |id: &str| {
                                client::read_command(&data_dir, id).await
                            })
                            .await
                            .map_err(|_| {
                                SendFailure::new(
                                    &error,
                                    &mutation_id,
                                    &ledger_key,
                                    &text,
                                    revision,
                                    &action,
                                )
                            })?;
                        (command_id, state, linked, true)
                    }
                    Err(error) => {
                        return Err(SendFailure::new(
                            &error,
                            &mutation_id,
                            &ledger_key,
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
                })
            })))
        });
    }

    pub(super) fn send_submitted(&mut self, outcome: Result<SendOutcome, SendFailure>) {
        // Keyboard focus goes back to the composer (no-op while it is unbound).
        self.take_focus(FocusTarget::Composer);
        self.settle_mutation(settles(&outcome));
        match outcome {
            Ok(outcome) => {
                // The submission settled: its snapshot (if any) is resolved.
                abandon_retry(
                    &mut self.chat.pending_retry,
                    &mut self.ledger,
                    &outcome.agent,
                );
                // Bound to the submitting Agent and draft revision, not
                // to whatever conversation is open now.
                self.chat
                    .drafts
                    .borrow_mut()
                    .ack(&outcome.agent, outcome.draft_rev);
                let harness = self
                    .chat
                    .detail
                    .as_ref()
                    .map(|detail| crate::picker::harness_label(&detail.harness).to_owned())
                    .unwrap_or_else(|| "the Agent".to_owned());
                if let Some(convo) = self.chat.convo.as_mut()
                    && convo.agent() == outcome.agent
                {
                    keep_admitted_input(convo, &outcome, &harness);
                }
                // A refusal the feed delivered before this reply.
                self.report_refused_steers();
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
            Err(failure) => {
                if let Some(convo) = self.chat.convo.as_mut() {
                    convo.forget_local(&failure.mutation_id);
                }
                // A refused steer is never sent on by itself (§9.4): its
                // text goes back to the composer, where Send sends it as
                // the next message once the turn has ended.
                let agent = failure.action.agent().to_owned();
                let restored = self.chat.drafts.borrow_mut().restore(
                    &agent,
                    failure.text.clone(),
                    failure.revision,
                );
                self.rebuild_items();
                if crate::state::uncertain(&failure.error) {
                    // Keep the exact unresolved identity with the draft so
                    // Retry resubmits this snapshot (same action, body,
                    // mutation id and ledger key) instead of reselecting
                    // from the Agent state, which could admit a duplicate
                    // effect under a fresh id. Dead snapshots and
                    // over-cap entries are pruned (keys released) first.
                    if restored {
                        retain_retry(
                            &mut self.chat.pending_retry,
                            &mut self.ledger,
                            &self.chat.drafts.borrow(),
                            &agent,
                            PendingRetry {
                                action: failure.action.clone(),
                                text: failure.text.clone(),
                                revision: failure.revision,
                                mutation_id: failure.mutation_id.clone(),
                                ledger_key: failure.ledger_key.clone(),
                            },
                        );
                    }
                    self.alert(format!(
                        "Not confirmed (Retry resubmits with the same id): {}",
                        failure.error.user_message()
                    ));
                } else {
                    // A definite refusal settles the action: drop its
                    // snapshot (releasing its key) so a later Send mints
                    // fresh identity.
                    if self
                        .chat
                        .pending_retry
                        .get(&agent)
                        .is_some_and(|pending| pending.revision == failure.revision)
                    {
                        abandon_retry(&mut self.chat.pending_retry, &mut self.ledger, &agent);
                    }
                    self.alert(match failure.action {
                        DraftAction::Steer(_) => {
                            steer_refused(&failure.error.user_message(), restored)
                        }
                        _ => format!("Not sent: {}", failure.error.user_message()),
                    });
                }
            }
        }
    }

    /// Steers the turn refused after admitting them (it ended before a
    /// safe point): each is said in words, and its text goes back to an
    /// empty composer for Send, never sent on by itself (§9.4).
    pub(super) fn report_refused_steers(&mut self) {
        let Some(convo) = self.chat.convo.as_mut() else {
            return;
        };
        let agent = convo.agent().to_owned();
        // A steer to this Agent still waits for its reply, which brings
        // its command and text.
        let sending = self.in_flight_key.as_deref().is_some_and(|key| {
            let mut fields = key.split(':');
            fields.next() == Some("steer") && fields.next() == Some(agent.as_str())
        });
        for steer in convo.take_refused_steers(sending) {
            // Only a steer this window sent has its exact text; another's
            // refusal is said with its reason alone.
            let Some(text) = steer.text else {
                self.alert(format!(
                    "Steer not applied: {}.",
                    steer.reason.trim_end_matches('.')
                ));
                continue;
            };
            let restored = {
                let mut drafts = self.chat.drafts.borrow_mut();
                let empty = drafts.text(Some(&agent)).trim().is_empty();
                if empty {
                    drafts.set(Some(&agent), text);
                }
                empty
            };
            self.alert(steer_refused(&steer.reason, restored));
        }
    }
}

/// A refused steer in words, and where its text is.
fn steer_refused(reason: &str, restored: bool) -> String {
    let reason = reason.trim_end_matches('.');
    if restored {
        format!(
            "Steer not applied: {reason}. Its text is back in the composer: Send sends it as the next message."
        )
    } else {
        format!(
            "Steer not applied: {reason}. The composer has newer text, so the steer's text stays in the turn's work log."
        )
    }
}

/// What a lost submit reply is read back from, under the same mutation id
/// (never a new submission): the retained command and its state, and for a
/// Resume with a message the exact linked `<id>/send` its admission created
/// in the same transaction (api.md: launch/resume with text). Either read
/// failing leaves the outcome uncertain.
async fn recover_retained(
    action: &DraftAction,
    mutation_id: &str,
    blank: bool,
    read: impl AsyncFn(&str) -> eliot_core::Result<Command>,
) -> eliot_core::Result<(String, String, Option<String>)> {
    let command = read(mutation_id).await?;
    let linked = match action {
        DraftAction::Resume { .. } if !blank => {
            Some(read(&format!("{mutation_id}/send")).await?.id)
        }
        _ => None,
    };
    Ok((
        command.id,
        format!("{:?}", command.state).to_lowercase(),
        linked,
    ))
}

/// The submitted message's local copy once its command is admitted, with
/// what it waits for. A SEND or a steer keeps it under its own command. A
/// resume's own command never starts a turn, so its message moves to the
/// linked SEND, whose failure (with Retry) it then shows; until that SEND
/// is known the copy stays under the resume rather than vanish.
fn keep_admitted_input(convo: &mut Conversation, outcome: &SendOutcome, harness: &str) {
    if outcome.text.is_empty() {
        return;
    }
    let (input, note) = match (outcome.what, outcome.linked.as_deref()) {
        ("Send", _) => (
            outcome.command_id.as_str(),
            format!("Waiting for {harness}…"),
        ),
        ("Steer", _) => {
            convo.note_steer_text(&outcome.command_id, &outcome.text);
            (
                outcome.command_id.as_str(),
                "Sent to the running turn".to_owned(),
            )
        }
        ("Resume", Some(linked)) => (linked, format!("Starting {harness}…")),
        _ => return,
    };
    if input != outcome.command_id {
        convo.forget_local(&outcome.command_id);
    }
    convo.note_local_input(input, &outcome.text);
    convo.set_local_note(input, Some(note));
}
