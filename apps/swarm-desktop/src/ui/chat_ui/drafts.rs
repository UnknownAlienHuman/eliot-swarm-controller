use super::*;

impl DesktopApp {
    /// Submit one draft action on a worker. `fallback` marks the send that
    /// follows a steer refused because the turn just ended.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn spawn_draft(
        &self,
        action: DraftAction,
        mutation_id: String,
        ledger_key: String,
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
                                    fallback,
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
                            fallback,
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
                // The submission settled: its snapshot (if any) is resolved.
                self.chat.pending_retry.remove(&outcome.agent);
                // Bound to the submitting Agent and draft revision, not
                // to whatever conversation is open now.
                self.chat.drafts.ack(&outcome.agent, outcome.draft_rev);
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
                    self.in_flight_key = Some(key.clone());
                    // The steer is definitely settled; its snapshot (if any)
                    // is superseded by the fallback submission below.
                    self.chat.pending_retry.remove(&bound.agent);
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
                        key,
                        failure.text,
                        failure.revision,
                        false,
                        true,
                        context,
                    );
                    return;
                }
                let agent = failure.action.agent().to_owned();
                let restored =
                    self.chat
                        .drafts
                        .restore(&agent, failure.text.clone(), failure.revision);
                self.rebuild_items();
                if crate::state::uncertain(&failure.error) {
                    // Keep the exact unresolved identity with the draft so
                    // the next identical Send retries this snapshot (same
                    // mutation id and ledger key) instead of rederiving a
                    // fresh id that could admit a duplicate effect.
                    if restored {
                        self.chat.pending_retry.insert(
                            agent.clone(),
                            PendingRetry {
                                action: failure.action.clone(),
                                text: failure.text.clone(),
                                revision: failure.revision,
                                mutation_id: failure.mutation_id.clone(),
                                ledger_key: failure.ledger_key.clone(),
                                fallback: failure.fallback,
                            },
                        );
                    }
                    self.alert(format!(
                        "Not confirmed (sending again reuses the same id): {}",
                        failure.error.user_message()
                    ));
                } else {
                    // A definite refusal settles the action: drop its
                    // snapshot so a later Send mints fresh identity.
                    if self
                        .chat
                        .pending_retry
                        .get(&agent)
                        .is_some_and(|pending| pending.revision == failure.revision)
                    {
                        self.chat.pending_retry.remove(&agent);
                    }
                    self.alert(format!("Not sent: {}", failure.error.user_message()));
                }
            }
        }
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
        ("Steer", _) => (
            outcome.command_id.as_str(),
            "Sent to the running turn".to_owned(),
        ),
        ("Resume", Some(linked)) => (linked, format!("Starting {harness}…")),
        _ => return,
    };
    if input != outcome.command_id {
        convo.forget_local(&outcome.command_id);
    }
    convo.note_local_input(input, &outcome.text);
    convo.set_local_note(input, Some(note));
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::CommandState;

    fn retained(id: &str, verb: &str) -> Command {
        Command {
            id: id.to_owned(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: verb.to_owned(),
            target: Some("a2".to_owned()),
            selector: None,
            expected_generation: Some(1),
            generation: Some(2),
            args: serde_json::json!({ "text": "hello" }),
            state: CommandState::Accepted,
            turn_id: None,
            request_id: None,
            outcome: None,
            created_at: 1,
            updated_at: 1,
            probe: false,
            config_revision: 0,
            config: None,
        }
    }

    fn resume() -> DraftAction {
        DraftAction::Resume {
            agent: "a2".to_owned(),
            generation: 1,
        }
    }

    fn resume_outcome(command_id: String, linked: Option<String>, state: String) -> SendOutcome {
        SendOutcome {
            what: "Resume",
            agent: "a2".to_owned(),
            command_id,
            linked,
            draft_rev: 3,
            text: "hello".to_owned(),
            state,
            recovered: true,
            fallback: false,
        }
    }

    /// The window's copy of a Resume with a message, as Send showed it.
    fn sending(id: &str) -> Conversation {
        let mut convo = Conversation::new("a2");
        convo.note_local_input(id, "hello");
        convo.set_local_note(id, Some("Sending…".to_owned()));
        convo
    }

    /// Reads of the retained commands; any other id is not found.
    fn read_from(
        commands: &HashMap<String, Command>,
    ) -> impl AsyncFn(&str) -> eliot_core::Result<Command> {
        async |id: &str| {
            commands
                .get(id)
                .cloned()
                .ok_or_else(|| eliot_core::Error::new("not_found", id))
        }
    }

    #[test]
    fn a_lost_resume_reply_keeps_its_message_on_the_linked_send() {
        // The resume and its linked SEND, admitted in one transaction.
        let commands = HashMap::from([
            ("r1".to_owned(), retained("r1", "resume")),
            ("r1/send".to_owned(), retained("r1/send", "send")),
        ]);
        let (command_id, state, linked) = client::block_on(recover_retained(
            &resume(),
            "r1",
            false,
            read_from(&commands),
        ))
        .unwrap();
        assert_eq!(command_id, "r1");
        assert_eq!(linked.as_deref(), Some("r1/send"));
        let mut convo = sending("r1");
        keep_admitted_input(
            &mut convo,
            &resume_outcome(command_id, linked, state),
            "Codex",
        );
        // The message waits on the linked SEND, whose outcome is read...
        assert_eq!(convo.waiting_locals(), vec!["r1/send".to_owned()]);
        // ...so a resume that fails before a turn starts shows it with Retry.
        convo.set_local_failure(
            "r1/send",
            InputFailure {
                code: "cancelled".to_owned(),
                message: String::new(),
                origin: "eliot".to_owned(),
                definite: true,
            },
        );
        assert_eq!(convo.local_text("r1/send").as_deref(), Some("hello"));
    }

    #[test]
    fn a_resume_whose_linked_send_is_not_read_stays_uncertain_and_visible() {
        // Only the resume reads back: no outcome is made up. The submission
        // then fails with the lost reply's own uncertain error, so the draft
        // comes back and a retry reuses the same mutation id.
        let commands = HashMap::from([("r1".to_owned(), retained("r1", "resume"))]);
        let read = client::block_on(recover_retained(
            &resume(),
            "r1",
            false,
            read_from(&commands),
        ));
        assert_eq!(read.unwrap_err().code, "not_found");
        let lost = eliot_core::Error::new("unknown", "transport: reply missing");
        let failure = SendFailure::new(&lost, "r1", "resume:a2:1:3", "hello", 3, &resume(), false);
        assert!(!settles(&Err::<(), _>(failure)));
        // A resume admitted without a known linked SEND keeps its copy.
        let mut convo = sending("r1");
        keep_admitted_input(
            &mut convo,
            &resume_outcome("r1".to_owned(), None, "accepted".to_owned()),
            "Codex",
        );
        assert_eq!(convo.waiting_locals(), vec!["r1".to_owned()]);
        assert_eq!(convo.local_text("r1").as_deref(), Some("hello"));
    }

    #[test]
    fn a_blank_resume_and_a_send_read_back_only_their_own_command() {
        let commands = HashMap::from([("r1".to_owned(), retained("r1", "resume"))]);
        let read = client::block_on(recover_retained(
            &resume(),
            "r1",
            true,
            read_from(&commands),
        ));
        assert_eq!(read.unwrap().2, None);
        let send = DraftAction::Send {
            agent: "a2".to_owned(),
        };
        let read = client::block_on(recover_retained(&send, "r1", false, read_from(&commands)));
        assert_eq!(read.unwrap().2, None);
    }
}
