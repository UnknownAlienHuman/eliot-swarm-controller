//! What waits for the owner in the open conversation (`Decision.dc.html`,
//! `Hold.dc.html`): the decision panel in the composer's place (an
//! approval, a question, a confidential question, an unconfirmed outcome),
//! the provider-hold strip above the composer, the workers line, the live
//! line under the header and the end-of-task notices. Every choice is the
//! daemon's: a request's offered choices, an `owner.inbox` item's choices,
//! a hold's corrective evidence.

use super::*;
use eliot_core::api::{
    OwnerDecideRequest, OwnerDecideResponse, OwnerInboxItem, OwnerInboxItemRef, OwnerInboxKind,
    ProviderCorrectiveEvidence,
};
use eliot_core::requests::{ApprovalDecision, QuestionKind};

/// One item of the decision panel.
enum Decision<'a> {
    /// A harness request on the Agent (approval or question), and its
    /// `owner.inbox` item when read (for Snooze).
    Request(&'a UiPending, Option<&'a OwnerInboxItem>),
    /// An unconfirmed command or turn of the Agent (`owner.inbox`).
    Unknown(&'a OwnerInboxItem),
    /// The Agent's Goal is blocked (`owner.inbox` `blocked_goal`).
    Goal(&'a OwnerInboxItem),
}

/// The `owner.inbox` kinds the decision panel shows besides the open
/// Agent's own requests: their daemon counts decide whether it reads them.
const PANEL_KINDS: [OwnerInboxKind; 3] = [
    OwnerInboxKind::UnknownTurn,
    OwnerInboxKind::UnknownCommand,
    OwnerInboxKind::BlockedGoal,
];

/// A Goal's budget in words: rounds and turns used of their limits, and
/// its cost when tracked ("2 of 5 rounds · 7 of 20 turns").
fn goal_budget(goal: &eliot_core::api::GoalView) -> Option<String> {
    let mut words = Vec::new();
    if let Some(max) = goal.definition.max_rounds {
        words.push(format!("{} of {max} rounds", goal.counters.rounds));
    }
    if let Some(max) = goal.definition.max_turns {
        words.push(format!("{} of {max} turns", goal.counters.turns_used));
    }
    if goal.cost_status != "untracked" && !goal.cost_status.is_empty() {
        words.push(goal.cost_status.clone());
    }
    (!words.is_empty()).then(|| words.join(" \u{b7} "))
}

/// The hold strip's corrective choices: the menu label, the daemon's
/// evidence.
const HOLD_CLEARS: [(&str, ProviderCorrectiveEvidence); 3] = [
    (
        "I've signed in",
        ProviderCorrectiveEvidence::CredentialsUpdated,
    ),
    (
        "I've fixed the settings",
        ProviderCorrectiveEvidence::ConfigurationUpdated,
    ),
    ("Clear anyway…", ProviderCorrectiveEvidence::OwnerVerified),
];

/// The Agent an inbox subject names (`a12`, `a12:g1:…`).
fn subject_agent(subject: &str) -> Option<&str> {
    let first = subject.split(':').next()?;
    let digits = first.strip_prefix('a')?;
    (!digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit())).then_some(first)
}

/// A choice's words ("interrupted" → "Interrupted").
fn choice_words(choice: &str) -> String {
    let words = choice.replace(['_', '.'], " ");
    let mut chars = words.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => words,
    }
}

impl DesktopApp {
    /// The decision panel's items: the open Agent's pending requests, then
    /// its unconfirmed outcomes from `owner.inbox`.
    fn decisions(&self) -> Vec<Decision<'_>> {
        let (Some(agent), Some(detail)) = (self.selected_agent_id(), self.bound_detail().ok())
        else {
            return Vec::new();
        };
        let inbox: &[OwnerInboxItem] = self
            .chat
            .decisions
            .as_ref()
            .filter(|(read, _, _)| *read == agent)
            .map(|(_, items, _)| items.as_slice())
            .unwrap_or_default();
        let mut items: Vec<Decision<'_>> = detail
            .pending
            .iter()
            .map(|request| {
                let subject = format!(
                    "{agent}:g{}:{}",
                    request.binding.generation,
                    request.binding.request.as_deref().unwrap_or_default()
                );
                let item = inbox.iter().find(|item| {
                    item.kind == OwnerInboxKind::HarnessRequest && item.subject == subject
                });
                Decision::Request(request, item)
            })
            .collect();
        let convo = self.chat.convo.as_ref();
        items.extend(
            inbox
                .iter()
                .filter(|item| match item.kind {
                    OwnerInboxKind::UnknownTurn => {
                        subject_agent(&item.subject) == Some(agent.as_str())
                    }
                    OwnerInboxKind::UnknownCommand => {
                        convo.is_some_and(|convo| convo.knows(&item.subject))
                    }
                    _ => false,
                })
                .map(Decision::Unknown),
        );
        items.extend(
            inbox
                .iter()
                .filter(|item| {
                    item.kind == OwnerInboxKind::BlockedGoal
                        && subject_agent(&item.subject) == Some(agent.as_str())
                })
                .map(Decision::Goal),
        );
        items
    }

    /// The daemon's counts of the inbox kinds the panel shows, as last
    /// read with the sidebar (`daemon.sidebar` `owner_inbox`).
    pub(in crate::ui) fn panel_counts(&self) -> [usize; 3] {
        PANEL_KINDS.map(|kind| self.inbox.counts.get(&kind).copied().unwrap_or(0))
    }

    /// Read the open Agent's inbox items for the decision panel when one
    /// may wait: a pending request, an unknown state or outcome in its
    /// history, or a panel kind the daemon counts. The read is decided by
    /// these facts as they stand, never by the order the reads arrive in,
    /// so a restart rebuilds the panel from the inbox. One read is in
    /// flight at a time. With nothing shown it reads only for facts not
    /// already read: a detail refresh with the same facts reads nothing,
    /// and facts that change during a read are read on its reply. While an
    /// item shows, each detail read checks it again.
    pub(in crate::ui) fn read_decisions(&mut self, context: &ComponentContext<Self>) {
        let (Some(agent), Some(detail), Ok(data_dir)) = (
            self.selected_agent_id(),
            self.chat.detail.as_ref(),
            self.data_dir.clone(),
        ) else {
            return;
        };
        let facts = DecisionFacts {
            agent: agent.clone(),
            pending: detail.pending.len(),
            unknown: detail.state == eliot_core::AgentState::Unknown,
            unconfirmed: self
                .chat
                .convo
                .as_ref()
                .is_some_and(Conversation::has_unconfirmed),
            counts: self.panel_counts(),
        };
        let waiting = facts.pending > 0
            || facts.unknown
            || facts.unconfirmed
            || facts.counts.iter().any(|count| *count > 0);
        if !waiting {
            self.chat.decisions = None;
            self.chat.decisions_facts = None;
            self.chat.decisions_error = None;
            // A read still in flight would show its items again: its
            // reply looks once more and clears them.
            if self.chat.decisions_reading {
                self.chat.decisions_again = true;
            }
            return;
        }
        // While the panel shows an item, every detail read checks it again:
        // its revision changes with events the facts do not count (a new
        // attention report), and a choice on a stale revision is refused.
        let showing = self
            .chat
            .decisions
            .as_ref()
            .is_some_and(|(shown, items, _)| *shown == agent && !items.is_empty());
        if !showing && self.chat.decisions_facts.as_ref() == Some(&facts) {
            return;
        }
        if self.chat.decisions_reading {
            self.chat.decisions_again = true;
            return;
        }
        self.chat.decisions_facts = Some(facts);
        self.chat.decisions_reading = true;
        let _task = context.spawn_background(move |_| {
            let read = client::block_on(client::owner_inbox(&data_dir, None))
                .map(|reply| reply.items)
                .map_err(|error| error.user_message());
            Message::Chat(ChatMsg::DecisionsRead(agent, read))
        });
    }

    /// Read the open Agent's goal (`goal.get`) into `chat.goal`, which the
    /// header, Details and the Goal dialog show: with every detail read,
    /// when the Goal dialog opens, and when the inbox's counts change (a
    /// goal that blocks after the turn's last event sends no other one),
    /// and after every goal action. One read is in flight at a time; a
    /// request during it reads once more after its reply.
    pub(in crate::ui) fn read_goal(&mut self, context: &ComponentContext<Self>) {
        if self.chat.goal_reading {
            self.chat.goal_again = true;
            return;
        }
        let (Some(agent), Ok(data_dir)) = (self.selected_agent_id(), self.data_dir.clone()) else {
            return;
        };
        self.chat.goal_reading = true;
        let _task = context.spawn_background(move |_| {
            let read = client::block_on(client::goal_get(&data_dir, &agent))
                .map(|goal| goal.map(Box::new))
                .map_err(|error| error.user_message());
            Message::Chat(ChatMsg::GoalRead(agent, read))
        });
    }

    /// Read what the decision panel and the hold strip need for the open
    /// Agent, once per detail read: its goal (`goal.get`); its inbox items
    /// while a request or an unconfirmed outcome may wait (`owner.inbox`);
    /// the hold's scope while it is held (`provider.holds`, owner-only),
    /// which the strip's corrective choices clear.
    pub(super) fn read_conversation_extras(&mut self, context: &ComponentContext<Self>) {
        let (Some(agent), Some(detail), Ok(data_dir)) = (
            self.selected_agent_id(),
            self.chat.detail.clone(),
            self.data_dir.clone(),
        ) else {
            return;
        };
        self.read_goal(context);
        self.read_decisions(context);
        if detail.provider_hold.is_some() {
            let _task = context.spawn_background(move |_| {
                let read = client::block_on(crate::config_client::provider_holds(&data_dir))
                    .map(|reply| reply.holds)
                    .map_err(|error| error.user_message());
                Message::Chat(ChatMsg::HoldsRead(agent, read))
            });
        } else {
            self.chat.holds = None;
        }
    }

    /// Decide an inbox item from the conversation (`owner.decide`), bound
    /// to the revision shown; an unconfirmed outcome carries its reason.
    pub(super) fn owner_decide(
        &mut self,
        item: OwnerInboxItemRef,
        choice: String,
        words: String,
        context: &ComponentContext<Self>,
    ) {
        if self.submitting() {
            return;
        }
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        let unknown = matches!(
            item.kind,
            OwnerInboxKind::UnknownCommand | OwnerInboxKind::UnknownTurn
        );
        let reason = self.chat.decide_reason.trim().to_owned();
        if unknown && choice != "snooze" && reason.is_empty() {
            self.alert("Say why before choosing an outcome.");
            return;
        }
        let key = format!(
            "decide:{}/{}@{}:{choice}:{:x}",
            item.kind.as_str(),
            item.subject,
            item.revision,
            crate::chat::text_hash(&reason)
        );
        let id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        self.alert = None;
        self.set_status(format!("{words} ({id})…"));
        let args = (unknown && choice != "snooze").then(|| serde_json::json!({ "reason": reason }));
        let _task = context.spawn_background(move |_| {
            let request = OwnerDecideRequest {
                id,
                item,
                choice,
                args,
            };
            let result = client::block_on(client::owner_decide(&data_dir, &request));
            Message::Chat(ChatMsg::OwnerDecided(words, Box::new(result)))
        });
    }

    /// The outcome of a decision from the conversation.
    pub(super) fn owner_decided(
        &mut self,
        words: &str,
        result: MutationResult<OwnerDecideResponse>,
        context: &ComponentContext<Self>,
    ) {
        self.settle_mutation(
            result
                .as_ref()
                .err()
                .is_none_or(|error| !crate::state::uncertain(error)),
        );
        match result {
            Ok(reply) if reply.snoozed => {
                self.set_status("Snoozed until something new happens on it.");
            }
            Ok(_) => {
                self.chat.decide_reason.clear();
                self.set_status(format!("{words}: done."));
            }
            Err(error) if crate::state::uncertain(&error) => self.alert(format!(
                "{words}: not confirmed (choosing it again retries the same decision): {}",
                error.user_message()
            )),
            Err(error) => self.alert(format!("{words} refused: {}", error.user_message())),
        }
        // The panel reads the inbox again after any choice: a resolved item
        // leaves, and a refused one (a stale revision) shows its current one.
        self.chat.decisions_facts = None;
        self.queue_reread(
            Invalidation {
                detail: true,
                ..Invalidation::default()
            },
            context,
        );
    }

    /// Clear the open Agent's hold with the owner's evidence.
    pub(super) fn clear_hold(
        &mut self,
        evidence: ProviderCorrectiveEvidence,
        context: &ComponentContext<Self>,
    ) {
        if self.submitting() {
            return;
        }
        let (Some(scope), Ok(data_dir)) = (self.hold_scope(), self.data_dir.clone()) else {
            self.alert("The hold's scope is not read yet.");
            return;
        };
        let key = format!("clear_hold:{scope}:{}", evidence.as_str());
        let id = self.ledger.id_for(&key);
        self.in_flight_key = Some(key);
        self.alert = None;
        self.set_status(format!("Clearing the hold ({id})…"));
        let _task = context.spawn_background(move |_| {
            let result = client::block_on(crate::config_client::provider_clear_hold(
                &data_dir, id, &scope, evidence,
            ))
            .map(|reply| {
                if reply.cleared {
                    "cleared".to_owned()
                } else {
                    "already clear".to_owned()
                }
            });
            Message::Chat(ChatMsg::ControlSubmitted("Clear hold".to_owned(), result))
        });
    }

    /// The scope of the open Agent's hold: the one `provider.holds` entry
    /// with the same kind, start and update (the Agent row carries no
    /// scope); `None` unless exactly one matches.
    fn hold_scope(&self) -> Option<String> {
        let agent = self.selected_agent_id()?;
        let hold = self.bound_detail().ok()?.provider_hold.as_ref()?;
        let (read, holds) = self.chat.holds.as_ref()?;
        if *read != agent {
            return None;
        }
        let mut matching = holds.as_ref().ok()?.iter().filter(|candidate| {
            candidate.kind == hold.kind
                && candidate.held_since == hold.held_since
                && candidate.updated_at == hold.updated_at
        });
        let scope = matching.next()?.scope.clone();
        matching.next().is_none().then_some(scope)
    }

    /// The decision panel (`DecisionPanel`) in the composer's place: the
    /// kind line with "1 of N", the agent's words, the subject in the code
    /// ground, the actions. `None` when nothing waits.
    pub(super) fn decision_panel(&self, context: &mut ViewContext<Self>) -> Option<View> {
        let items = self.decisions();
        if items.is_empty() {
            return None;
        }
        let detail = self.bound_detail().ok()?;
        let at = self.chat.decision_at.min(items.len() - 1);
        let harness = crate::picker::harness_label(&detail.harness).to_owned();
        let enabled = !self.submitting();
        let count = if items.len() > 1 {
            format!(" \u{b7} {} of {}", at + 1, items.len())
        } else {
            String::new()
        };
        // Stop turn stays: the daemon takes a stop while a request is open.
        let stop: Option<View> = match detail.active_turn.as_deref() {
            Some(turn) if !detail.observe_only => {
                let agent = self.selected_agent_id().unwrap_or_default();
                Some(
                    controls::text_button()
                        .is_enabled(enabled)
                        .automation_name("Stop turn")
                        .on_click(context.message(Message::Chat(ChatMsg::InterruptRequested(
                            BoundAction::turn_control(&agent, detail.generation, turn),
                        ))))
                        .content(controls::label("Stop turn"))
                        .into(),
                )
            }
            _ => None,
        };
        let snooze =
            |item: Option<&OwnerInboxItem>, context: &mut ViewContext<Self>| -> Option<View> {
                item.map(|item| {
                    controls::icon_button()
                        .is_enabled(enabled)
                        .automation_name("Snooze")
                        .on_click(context.message(Message::Chat(ChatMsg::OwnerDecide(
                            OwnerInboxItemRef {
                                kind: item.kind,
                                subject: item.subject.clone(),
                                revision: item.revision.clone(),
                            },
                            "snooze".to_owned(),
                            "Snooze".to_owned(),
                        ))))
                        .content(controls::icon(controls::glyph::SNOOZE))
                        .tooltip("Snooze")
                })
            };
        let (glyph, kind, question, subject, body, actions): (
            &str,
            String,
            String,
            String,
            Option<View>,
            Vec<KeyedView>,
        ) = match &items[at] {
            Decision::Request(request, item) if request.kind == "approval" => {
                // The harness's own words: its summary's first line asks,
                // the rest is what it would run or change.
                let mut lines = request.summary.lines();
                let question = lines
                    .next()
                    .unwrap_or("The agent asks for approval.")
                    .to_owned();
                let subject = lines.collect::<Vec<_>>().join("\n");
                let approvals: Vec<_> = request
                    .choices
                    .iter()
                    .filter(|choice| choice.decision == ApprovalDecision::Approve)
                    .collect();
                let deny = request
                    .choices
                    .iter()
                    .find(|choice| choice.decision == ApprovalDecision::Deny);
                let deny_label = deny.map_or("Deny".to_owned(), |choice| choice.label.clone());
                let approve_label = approvals
                    .first()
                    .map_or("Approve".to_owned(), |choice| choice.label.clone());
                let approve = Message::Chat(ChatMsg::ApprovalRequested(
                    (*request).clone(),
                    true,
                    approvals.first().map(|choice| choice.id.clone()),
                ));
                let deny_message = Message::Chat(ChatMsg::ApprovalRequested(
                    (*request).clone(),
                    false,
                    deny.map(|choice| choice.id.clone()),
                ));
                // A broader grant is its own button, never folded into
                // Allow once (§10.1).
                let broader: Vec<KeyedView> = approvals
                    .iter()
                    .skip(1)
                    .map(|choice| {
                        KeyedView::new(
                            choice.id.clone(),
                            controls::outlined_small()
                                .is_enabled(enabled)
                                .automation_name(choice.label.clone())
                                .on_click(context.message(Message::Chat(
                                    ChatMsg::ApprovalRequested(
                                        (*request).clone(),
                                        true,
                                        Some(choice.id.clone()),
                                    ),
                                )))
                                .content(controls::label(&choice.label)),
                        )
                    })
                    .collect();
                let mut actions: Vec<KeyedView> = stop
                    .map(|stop| KeyedView::new("stop", stop))
                    .into_iter()
                    .collect();
                actions.extend([
                    KeyedView::new("sp", Border::new().grid_column(1)),
                    KeyedView::new(
                        "snooze",
                        Border::new()
                            .grid_column(2)
                            .maybe_content(snooze(*item, context)),
                    ),
                    KeyedView::new(
                        "choices",
                        StackPanel::new()
                            .grid_column(3)
                            .grid_column_span(2)
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .keyed_children(
                                std::iter::once(KeyedView::new(
                                    "deny",
                                    controls::outlined_small()
                                        .is_enabled(enabled)
                                        .automation_name(deny_label.clone())
                                        .on_click(context.message(deny_message))
                                        .content(controls::label(&deny_label)),
                                ))
                                .chain(broader)
                                .chain(std::iter::once(KeyedView::new(
                                    "approve",
                                    controls::filled()
                                        .is_enabled(enabled)
                                        .automation_name(approve_label.clone())
                                        .on_click(context.message(approve))
                                        .content(controls::label(&approve_label)),
                                )))
                                .collect::<Vec<_>>(),
                            ),
                    ),
                ]);
                (
                    controls::glyph::TERMINAL,
                    format!("Approval \u{b7} {harness}{count}"),
                    question,
                    subject,
                    None,
                    actions,
                )
            }
            Decision::Request(request, item) => {
                let key = request.binding.request.clone().unwrap_or_default();
                let secret = request.questions.iter().any(|question| question.is_secret);
                let total = request.questions.len();
                let questions: Vec<KeyedView> = request
                    .questions
                    .iter()
                    .enumerate()
                    .map(|(index, question)| {
                        let view = self.question_view(&key, question, context);
                        // Several questions: an explicit sequence.
                        let view: View = if total > 1 {
                            StackPanel::new()
                                .spacing(tokens::spacing::XS)
                                .children((
                                    controls::secondary(format!(
                                        "Question {} of {total}",
                                        index + 1
                                    )),
                                    view,
                                ))
                                .into()
                        } else {
                            view
                        };
                        KeyedView::new(question.id.clone(), view)
                    })
                    .collect();
                let body: Option<View> = if secret {
                    Some(
                        controls::body("Answer this question in the agent's own application.")
                            .text_wrapping(TextWrapping::Wrap)
                            .into(),
                    )
                } else {
                    Some(
                        StackPanel::new()
                            .spacing(tokens::spacing::SM)
                            .keyed_children(questions)
                            .into(),
                    )
                };
                let answer: Option<View> = if secret {
                    None
                } else {
                    Some(
                        controls::filled()
                            .grid_column(4)
                            .is_enabled(enabled && can_answer(request, &self.chat.answers))
                            .automation_name("Answer")
                            .on_click(context.message(Message::Chat(ChatMsg::AnswerRequested(
                                (*request).clone(),
                            ))))
                            .content(controls::label("Answer"))
                            .into(),
                    )
                };
                let mut actions = vec![
                    KeyedView::new(
                        "left",
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .keyed_children(crate::views::slots([
                                stop,
                                Some(
                                    controls::text_button()
                                        .is_enabled(enabled)
                                        .automation_name("Cancel request")
                                        .on_click(context.message(Message::Chat(
                                            ChatMsg::ApprovalRequested(
                                                (*request).clone(),
                                                false,
                                                None,
                                            ),
                                        )))
                                        .content(controls::label("Cancel request"))
                                        .into(),
                                ),
                            ])),
                    ),
                    KeyedView::new("sp", Border::new().grid_column(1)),
                    KeyedView::new(
                        "snooze",
                        Border::new()
                            .grid_column(2)
                            .maybe_content(snooze(*item, context)),
                    ),
                ];
                actions.extend(answer.map(|answer| KeyedView::new("answer", answer)));
                (
                    controls::glyph::INFO,
                    format!(
                        "{} \u{b7} {harness}{count}",
                        if secret {
                            "Confidential question"
                        } else {
                            "Question"
                        }
                    ),
                    request.summary.clone(),
                    String::new(),
                    body,
                    actions,
                )
            }
            Decision::Unknown(item) => {
                let reference = OwnerInboxItemRef {
                    kind: item.kind,
                    subject: item.subject.clone(),
                    revision: item.revision.clone(),
                };
                let reason_ready = !self.chat.decide_reason.trim().is_empty();
                let mut buttons: Vec<KeyedView> = Vec::new();
                let recommended = item
                    .recommended
                    .as_ref()
                    .map(|choice| choice.choice.clone());
                let mut choices: Vec<&str> = recommended.iter().map(String::as_str).collect();
                choices.extend(
                    item.choices
                        .iter()
                        .map(|choice| choice.choice.as_str())
                        .filter(|choice| Some(*choice) != recommended.as_deref()),
                );
                for choice in choices {
                    let words = choice_words(choice);
                    let decide = context.message(Message::Chat(ChatMsg::OwnerDecide(
                        reference.clone(),
                        if Some(choice) == recommended.as_deref() {
                            "recommended".to_owned()
                        } else {
                            choice.to_owned()
                        },
                        words.clone(),
                    )));
                    let button = if Some(choice) == recommended.as_deref() {
                        controls::filled()
                    } else {
                        controls::outlined_small()
                    };
                    buttons.push(KeyedView::new(
                        choice.to_owned(),
                        button
                            .is_enabled(enabled && reason_ready)
                            .automation_name(format!("Resolve as {words}"))
                            .on_click(decide)
                            .content(controls::label(&words)),
                    ));
                }
                let reason = controls::field()
                    .text(self.chat.decide_reason.clone())
                    .placeholder_text("Why (required)")
                    .automation_name("Reason for the outcome")
                    .on_text_changed(crate::views::text_edits(
                        context.callback(|text| Message::Chat(ChatMsg::DecideReason(text))),
                    ));
                // What is known and when it was read; no choice repeats
                // the request (§10.1).
                let read_at = self
                    .chat
                    .decisions
                    .as_ref()
                    .map(|(_, _, at)| crate::monitor::clock(*at))
                    .map(|at| match &self.chat.decisions_error {
                        // A failed re-read leaves the last answer, and says so.
                        Some(error) => format!("{at} · not read again: {error}"),
                        None => at,
                    });
                // The technical identity stays folded until asked for.
                let details_key = format!("details:{}", item.subject);
                let body = StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .keyed_children(crate::views::slots([
                        read_at.map(|at| controls::secondary(format!("Checked {at}")).into()),
                        Some(reason.into()),
                        Some(details_view(
                            &item.subject,
                            item.detail.as_deref(),
                            self.chat.expanded.contains(&details_key),
                            context.message(Message::Chat(ChatMsg::ToolsToggled(
                                details_key.clone(),
                                !self.chat.expanded.contains(&details_key),
                            ))),
                        )),
                    ]));
                let check = controls::text_button()
                    .is_enabled(enabled)
                    .automation_name("Check again")
                    .on_click(context.message(Message::Chat(ChatMsg::DecisionsRecheck)))
                    .content(controls::label("Check again"));
                let mut actions: Vec<KeyedView> = vec![KeyedView::new(
                    "left",
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .keyed_children(crate::views::slots([stop, Some(check.into())])),
                )];
                actions.extend([
                    KeyedView::new("sp", Border::new().grid_column(1)),
                    KeyedView::new(
                        "snooze",
                        Border::new()
                            .grid_column(2)
                            .maybe_content(snooze(Some(item), context)),
                    ),
                    KeyedView::new(
                        "choices",
                        StackPanel::new()
                            .grid_column(3)
                            .grid_column_span(2)
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .keyed_children(buttons),
                    ),
                ]);
                let what = if item.kind == OwnerInboxKind::UnknownTurn {
                    "This turn's outcome is unknown"
                } else {
                    "This command's outcome is unknown"
                };
                (
                    controls::glyph::ERROR,
                    format!(
                        "{} \u{b7} {harness}{count}",
                        if item.kind == OwnerInboxKind::UnknownTurn {
                            "Unconfirmed turn"
                        } else {
                            "Unconfirmed command"
                        }
                    ),
                    format!(
                        "{what}: {}.",
                        eliot_core::reason_words(&item.reason).to_lowercase()
                    ),
                    String::new(),
                    Some(body.into()),
                    actions,
                )
            }
            // Goal blocked: why, the budget left, the daemon's actions.
            Decision::Goal(item) => {
                let reference = OwnerInboxItemRef {
                    kind: item.kind,
                    subject: item.subject.clone(),
                    revision: item.revision.clone(),
                };
                let agent = self.selected_agent_id().unwrap_or_default();
                let goal = self
                    .chat
                    .goal
                    .as_ref()
                    .filter(|(read, _)| *read == agent)
                    .and_then(|(_, goal)| goal.as_ref());
                let recommended = item
                    .recommended
                    .as_ref()
                    .map(|choice| choice.choice.clone());
                let mut choices: Vec<&str> = recommended.iter().map(String::as_str).collect();
                choices.extend(
                    item.choices
                        .iter()
                        .map(|choice| choice.choice.as_str())
                        .filter(|choice| Some(*choice) != recommended.as_deref()),
                );
                let buttons: Vec<KeyedView> = choices
                    .into_iter()
                    .map(|choice| {
                        let words = format!("{} goal", choice_words(choice));
                        let primary = Some(choice) == recommended.as_deref();
                        let button = if primary {
                            controls::filled()
                        } else {
                            controls::outlined_small()
                        };
                        KeyedView::new(
                            choice.to_owned(),
                            button
                                .is_enabled(enabled)
                                .automation_name(words.clone())
                                .on_click(context.message(Message::Chat(ChatMsg::OwnerDecide(
                                    reference.clone(),
                                    if primary {
                                        "recommended".to_owned()
                                    } else {
                                        choice.to_owned()
                                    },
                                    words.clone(),
                                ))))
                                .content(controls::label(&words)),
                        )
                    })
                    .collect();
                let mut actions: Vec<KeyedView> = stop
                    .map(|stop| KeyedView::new("stop", stop))
                    .into_iter()
                    .collect();
                actions.extend([
                    KeyedView::new("sp", Border::new().grid_column(1)),
                    KeyedView::new(
                        "snooze",
                        Border::new()
                            .grid_column(2)
                            .maybe_content(snooze(Some(item), context)),
                    ),
                    KeyedView::new(
                        "choices",
                        StackPanel::new()
                            .grid_column(3)
                            .grid_column_span(2)
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .keyed_children(buttons),
                    ),
                ]);
                let reason = goal
                    .and_then(|goal| goal.blocked_reason.clone())
                    .unwrap_or_else(|| eliot_core::reason_words(&item.reason));
                let budget: Option<View> = goal.and_then(goal_budget).map(|budget| {
                    controls::secondary(format!("Budget used: {budget}"))
                        .text_wrapping(TextWrapping::Wrap)
                        .is_text_selection_enabled(true)
                        .into()
                });
                // The audit cycles that ran before it blocked (A10).
                let audits = self.goal_audits(item);
                let body: Option<View> = (budget.is_some() || audits.is_some()).then(|| {
                    StackPanel::new()
                        .spacing(tokens::spacing::SM)
                        .keyed_children(crate::views::slots([budget, audits]))
                        .into()
                });
                (
                    controls::glyph::GOAL,
                    format!("Goal blocked \u{b7} {harness}{count}"),
                    reason,
                    String::new(),
                    body,
                    actions,
                )
            }
        };
        let steps: Option<View> = if items.len() > 1 {
            Some(
                StackPanel::new()
                    .grid_column(2)
                    .orientation(Orientation::Horizontal)
                    .children((
                        controls::icon_button()
                            .is_enabled(at > 0)
                            .automation_name("Previous request")
                            .on_click(context.message(Message::Chat(ChatMsg::DecisionStep(-1))))
                            .content(controls::icon(controls::glyph::CHEVRON_LEFT)),
                        controls::icon_button()
                            .is_enabled(at + 1 < items.len())
                            .automation_name("Next request")
                            .on_click(context.message(Message::Chat(ChatMsg::DecisionStep(1))))
                            .content(controls::icon(controls::glyph::CHEVRON_RIGHT)),
                    ))
                    .into(),
            )
        } else {
            None
        };
        let kind_line = Grid::new()
            .min_height(tokens::height::CONTROL)
            .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
            .column_spacing(tokens::spacing::CHIP_GAP)
            .keyed_children(crate::views::slots([
                Some(controls::xs_icon(glyph)),
                Some(
                    controls::text(kind.clone(), tokens::text::LABEL)
                        .grid_column(1)
                        .foreground(ThemeBrush::AccentText)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis)
                        .into(),
                ),
                steps,
            ]));
        let subject: Option<View> = if subject.trim().is_empty() {
            None
        } else {
            Some(
                Border::new()
                    .background(ThemeBrush::CardBackground)
                    .border_brush(ThemeBrush::CardStroke)
                    .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
                    .corner_radius(tokens::radius::CONTAINER)
                    .padding(Thickness::xy(tokens::spacing::INSET, tokens::spacing::SM))
                    .content(
                        controls::body(subject)
                            .font_size(tokens::text::KID_SIZE)
                            .text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(true),
                    )
                    .into(),
            )
        };
        let card = Border::new()
            .background(ThemeBrush::CardBackground)
            .border_brush(ThemeBrush::Accent)
            .border_thickness(Thickness::uniform(tokens::layout::DECISION_OUTLINE))
            .corner_radius(tokens::radius::CONTAINER)
            .padding(Thickness::new(
                tokens::layout::DECISION_LEADING,
                tokens::spacing::SM,
                tokens::spacing::MD,
                tokens::spacing::INSET,
            ))
            .automation_name(kind)
            .content(
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .keyed_children(crate::views::slots([
                        Some(kind_line.into()),
                        Some(
                            controls::body(question)
                                .text_wrapping(TextWrapping::Wrap)
                                .is_text_selection_enabled(true)
                                .into(),
                        ),
                        subject,
                        body,
                        Some(
                            Grid::new()
                                .columns([
                                    GridLength::Auto,
                                    GridLength::STAR,
                                    GridLength::Auto,
                                    GridLength::Auto,
                                    GridLength::Auto,
                                ])
                                .column_spacing(tokens::spacing::SM)
                                .keyed_children(actions)
                                .into(),
                        ),
                    ])),
            );
        Some(
            Border::new()
                .grid_row(5)
                .column(
                    tokens::layout::READING_WIDTH,
                    tokens::spacing::SM,
                    tokens::spacing::LG,
                )
                .content(card)
                .into(),
        )
    }

    /// One question of a question request: its title and prompt, then the
    /// control its kind takes: options as chips (one answer) or check
    /// boxes (several), "Other answer" when free entry is offered, a field
    /// for a number or text; "Leave unanswered" clears it. The plugin
    /// validates the answer against the native offer before writing.
    fn question_view(
        &self,
        request: &str,
        question: &eliot_core::requests::Question,
        context: &mut ViewContext<Self>,
    ) -> View {
        let values = self
            .chat
            .answers
            .get(request)
            .and_then(|answers| answers.get(&question.id))
            .map(|answer| answer.answers.clone())
            .unwrap_or_default();
        let request = request.to_owned();
        let id = question.id.clone();
        let changed = {
            let request = request.clone();
            let id = id.clone();
            move |values| Message::Chat(ChatMsg::AnswerChanged(request.clone(), id.clone(), values))
        };
        let omit: Option<View> = if values.is_empty() {
            None
        } else {
            Some(
                controls::text_button()
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .on_click(context.message(Message::Chat(ChatMsg::AnswerChanged(
                        request.clone(),
                        id.clone(),
                        Vec::new(),
                    ))))
                    .content(controls::label("Leave unanswered"))
                    .into(),
            )
        };
        let control: View = match question.kind {
            _ if question.is_secret => {
                controls::body("Answer this question in the agent's own application.").into()
            }
            QuestionKind::Resource => controls::body(
                "This step needs action in the agent's own application. You can cancel this request here.",
            )
            .text_wrapping(TextWrapping::Wrap)
            .into(),
            QuestionKind::Number | QuestionKind::Text => {
                let before = values.first().cloned().unwrap_or_default();
                controls::field()
                    .text(before.clone())
                    .placeholder_text(if question.kind == QuestionKind::Number {
                        "Enter a number"
                    } else {
                        "Your answer"
                    })
                    .automation_name(if question.kind == QuestionKind::Number {
                        "Number answer"
                    } else {
                        "Text answer"
                    })
                    .on_text_changed(crate::views::text_edits(context.callback(move |text| {
                        if text == before {
                            Message::Chat(ChatMsg::AnswerUnchanged)
                        } else {
                            changed(vec![text])
                        }
                    })))
                    .into()
            }
            QuestionKind::Boolean | QuestionKind::Single => {
                let options: Vec<(String, String)> = if question.kind == QuestionKind::Boolean {
                    vec![
                        ("true".to_owned(), "Yes".to_owned()),
                        ("false".to_owned(), "No".to_owned()),
                    ]
                } else {
                    question
                        .options
                        .iter()
                        .map(|option| (option.id.clone(), option.label.clone()))
                        .collect()
                };
                let selected = values.first().cloned();
                // One answer: native radio buttons (§10.1).
                let picked = context.callback({
                    let (request, id, options) = (request.clone(), id.clone(), options.clone());
                    move |index: Option<usize>| match index.and_then(|index| options.get(index)) {
                        Some((option, _)) => Message::Chat(ChatMsg::AnswerChanged(
                            request.clone(),
                            id.clone(),
                            vec![option.clone()],
                        )),
                        None => Message::Chat(ChatMsg::AnswerUnchanged),
                    }
                });
                let chips = RadioButtons::new()
                    .automation_name(
                        question
                            .title
                            .clone()
                            .or_else(|| question.prompt.clone())
                            .unwrap_or_else(|| "Answer".to_owned()),
                    )
                    .items_source(options.iter().map(|(_, label)| label.as_str()))
                    .selected_index(
                        options
                            .iter()
                            .position(|(option, _)| selected.as_deref() == Some(option.as_str())),
                    )
                    .on_selection_changed(move |index: Option<usize>| picked.call(index));
                if question.free_entry {
                    let request = request.clone();
                    let id = id.clone();
                    let before = selected
                        .filter(|value| !options.iter().any(|(option, _)| option == value))
                        .unwrap_or_default();
                    StackPanel::new()
                        .spacing(tokens::spacing::SM)
                        .children((
                            chips,
                            controls::field()
                                .text(before.clone())
                                .placeholder_text("Other answer")
                                .automation_name("Other answer")
                                .on_text_changed(crate::views::text_edits(context.callback(move |text| {
                                    if text == before {
                                        Message::Chat(ChatMsg::AnswerUnchanged)
                                    } else {
                                        Message::Chat(ChatMsg::AnswerChanged(
                                            request.clone(),
                                            id.clone(),
                                            vec![text],
                                        ))
                                    }
                                }))),
                        )).into()
                } else {
                    chips.into()
                }
            }
            QuestionKind::Multiple => {
                // One answer per question: the wire carries either the
                // selected labels or one free text, never a mix.
                let selected: Vec<String> = values
                    .iter()
                    .filter(|value| question.options.iter().any(|option| &option.id == *value))
                    .cloned()
                    .collect();
                let mut controls: Vec<KeyedView> = question
                    .options
                    .iter()
                    .map(|option| {
                        let request = request.clone();
                        let id = id.clone();
                        let option_id = option.id.clone();
                        let selected = selected.clone();
                        KeyedView::new(
                            option.id.clone(),
                            CheckBox::new()
                                .is_checked(values.contains(&option.id))
                                .automation_name(option.label.clone())
                                .on_is_checked_changed(crate::views::check_changes(context.callback(move |on| {
                                    Message::Chat(ChatMsg::AnswerChanged(
                                        request.clone(),
                                        id.clone(),
                                        transcript::multiple_toggle(selected.clone(), &option_id, on),
                                    ))
                                })))
                                .content(controls::label(&option.label)),
                        )
                    })
                    .collect();
                if question.free_entry {
                    let other = values
                        .iter()
                        .filter(|value| !selected.contains(value))
                        .cloned()
                        .collect::<Vec<_>>()
                        .join("\n");
                    let option_ids: Vec<String> =
                        question.options.iter().map(|option| option.id.clone()).collect();
                    controls.push(KeyedView::new(
                        "free",
                        controls::field()
                            .text(other.clone())
                            .accepts_return(true)
                            .placeholder_text("Other answer")
                            .automation_name("Other answer")
                            .on_text_changed(crate::views::text_edits(context.callback(move |text: String| {
                                if text == other {
                                    return Message::Chat(ChatMsg::AnswerUnchanged);
                                }
                                changed(transcript::multiple_submission(
                                    selected.clone(),
                                    &option_ids,
                                    &text,
                                ))
                            }))),
                    ));
                }
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .keyed_children(controls).into()
            }
        };
        let title: Option<View> = match question.title.as_deref().zip(question.prompt.as_deref()) {
            Some((title, _)) if !title.trim().is_empty() => Some(
                controls::text(title.to_owned(), tokens::text::BUTTON)
                    .text_wrapping(TextWrapping::Wrap)
                    .into(),
            ),
            _ => None,
        };
        StackPanel::new()
            .spacing(tokens::spacing::SM)
            .keyed_children(crate::views::slots([
                title,
                match question.prompt.as_deref() {
                    Some(prompt) if !prompt.trim().is_empty() => Some(
                        controls::body(prompt.to_owned())
                            .text_wrapping(TextWrapping::Wrap)
                            .into(),
                    ),
                    _ => None,
                },
                Some(control),
                omit,
            ]))
            .into()
    }

    /// The provider-hold strip above the composer (`AttentionStrip`): the
    /// hold in one line, then by its kind Wait
    /// and Switch model (a limit), Sign in (a sign-in), Pick another model
    /// (a removed or mismatched model), and the corrective choices in a
    /// menu, each with the daemon's evidence code.
    pub(super) fn hold_strip(&self, context: &mut ViewContext<Self>) -> Option<View> {
        use eliot_core::ProviderRestrictionKind as Kind;
        let agent = self.selected_agent_id()?;
        let detail = self.bound_detail().ok()?;
        let hold = detail.provider_hold.as_ref()?;
        if self
            .chat
            .hold_waited
            .as_ref()
            .is_some_and(|(waited, since)| *waited == agent && *since == hold.held_since)
        {
            return None;
        }
        let harness = crate::picker::harness_label(&detail.harness).to_owned();
        let now = crate::handoff::now_ms();
        let when = match (hold.retry_after, hold.next_probe_at) {
            _ if hold.state == eliot_core::ProviderHoldState::Probing => {
                format!("Checking {harness} again now.")
            }
            (Some(at), _) => format!("Resets at {}.", usage_ui::clock(at, now)),
            (None, Some(at)) => format!("Next check at {}.", usage_ui::clock(at, now)),
            (None, None) => "No reset time reported.".to_owned(),
        };
        let model = detail
            .settings
            .effective_model
            .clone()
            .or_else(|| detail.settings.profile_model.clone())
            .unwrap_or_else(|| "The model".to_owned());
        let (lead, rest) = match hold.kind {
            Kind::RateLimit | Kind::Quota => (format!("{harness} limit reached."), when),
            Kind::Auth => (format!("Sign in to {harness} needed."), String::new()),
            Kind::ModelRemoved => (format!("{model} is no longer available."), String::new()),
            Kind::ModelMismatch => (
                format!("{harness} served another model than {model}."),
                String::new(),
            ),
        };
        let enabled = !self.submitting();
        let scope = self.hold_scope().is_some();
        let primary: View = match hold.kind {
            Kind::RateLimit | Kind::Quota => {
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .children((
                        controls::text_button()
                            .automation_name("Wait")
                            .on_click(context.message(Message::Chat(ChatMsg::HoldWait)))
                            .content(controls::label("Wait")),
                        controls::tonal()
                            .is_enabled(enabled)
                            .automation_name("Switch model")
                            .on_click(context.message(Message::Chat(ChatMsg::AgentMenu(
                                menu_ui::Action::Handoff,
                            ))))
                            .content(controls::label("Switch model")),
                    ))
                    .into()
            }
            Kind::Auth => controls::tonal()
                .is_enabled(enabled)
                .automation_name(format!("Sign in to {harness}"))
                .on_click(context.message(Message::Chat(ChatMsg::SignIn)))
                .content(controls::label("Sign in"))
                .into(),
            Kind::ModelRemoved | Kind::ModelMismatch => controls::tonal()
                .is_enabled(enabled)
                .automation_name("Pick another model")
                .on_click(
                    context.message(Message::Chat(ChatMsg::AgentMenu(menu_ui::Action::Handoff))),
                )
                .content(controls::label("Pick another model"))
                .into(),
        };
        let items: Vec<MenuItem> = HOLD_CLEARS
            .iter()
            .map(|(label, _)| {
                if scope && enabled {
                    MenuItem::item(*label, *label)
                } else {
                    MenuItem::disabled(*label, *label)
                }
            })
            .collect();
        Some(
            Border::new()
                .background(ThemeBrush::CardBackground)
                .border_brush(ThemeBrush::CardStroke)
                .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
                .corner_radius(tokens::radius::CONTAINER)
                .padding(Thickness::new(
                    tokens::spacing::INSET,
                    tokens::layout::STRIP_INSET,
                    tokens::layout::STRIP_INSET,
                    tokens::layout::STRIP_INSET,
                ))
                .automation_name(format!("{lead} {rest}"))
                .content(
                    Grid::new()
                        .columns([
                            GridLength::Auto,
                            GridLength::STAR,
                            GridLength::Auto,
                            GridLength::Auto,
                        ])
                        .column_spacing(tokens::spacing::ROW)
                        .children((
                            controls::icon(controls::glyph::HOLD),
                            Grid::new()
                                .grid_column(1)
                                .columns([GridLength::Auto, GridLength::STAR])
                                .column_spacing(tokens::spacing::XS)
                                .vertical_alignment(VerticalAlignment::Center)
                                .children((
                                    controls::text(lead, tokens::text::BUTTON),
                                    controls::body(rest)
                                        .grid_column(1)
                                        .text_trimming(TextTrimming::CharacterEllipsis),
                                )),
                            Border::new()
                                .grid_column(2)
                                .vertical_alignment(VerticalAlignment::Center)
                                .content(primary),
                            controls::icon_button()
                                .grid_column(3)
                                .automation_name(if scope {
                                    "Other options: I've signed in, I've fixed the settings, Clear anyway"
                                } else {
                                    "Other options (the hold's scope is not read yet)"
                                })
                                .content(controls::icon(controls::glyph::MORE_VERTICAL))
                                .menu(Menu::new(
                                    items,
                                    crate::views::menu_keys(context.callback(|label: String| {
                                        HOLD_CLEARS
                                            .iter()
                                            .find(|(known, _)| *known == label)
                                            .map_or(Message::Menu(MenuMsg::Ignored), |(_, evidence)| {
                                                Message::Chat(ChatMsg::HoldClear(*evidence))
                                            })
                                    })),
                                )),
                        )),
                ).into(),
        )
    }

    /// The workers line above a manager's composer: "2 workers running ·
    /// 1 waiting for you" (its `nav` rows).
    pub(super) fn workers_line(&self) -> Option<View> {
        let agent = self.selected_agent_id()?;
        let kids: Vec<&UiAgent> = self
            .nav
            .agents
            .iter()
            .filter(|kid| kid.parent.as_deref() == Some(agent.as_str()) && !kid.archived)
            .collect();
        let running = kids
            .iter()
            .filter(|kid| {
                matches!(
                    kid.state,
                    eliot_core::AgentState::Busy
                        | eliot_core::AgentState::Starting
                        | eliot_core::AgentState::Created
                )
            })
            .count();
        let waiting = kids
            .iter()
            .filter(|kid| kid.state == eliot_core::AgentState::Waiting)
            .count();
        if running == 0 && waiting == 0 {
            return None;
        }
        let mut words = Vec::new();
        if running > 0 {
            words.push(format!(
                "{} running",
                eliot_core::count_words(running as u64, "worker", "workers")
            ));
        }
        if waiting > 0 {
            words.push(format!("{waiting} waiting for you"));
        }
        let words = words.join(" \u{b7} ");
        Some(
            Border::new()
                .min_height(tokens::height::CONTROL)
                .padding(Thickness::xy(tokens::spacing::INSET, 0.0))
                .automation_name(words.clone())
                .content(
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .vertical_alignment(VerticalAlignment::Center)
                        .children((
                            controls::muted_icon(controls::glyph::HUB),
                            controls::text(words, tokens::text::LABEL)
                                .vertical_alignment(VerticalAlignment::Center),
                        )),
                )
                .into(),
        )
    }

    /// The live line under the header (5.2.1): "Reconnecting…", "Catching
    /// up…" or "Live updates stopped" with Retry. After a few failed
    /// reconnects in a row it says Swarm is not answering and offers Start
    /// Swarm (the startup path on this data directory, which starts a
    /// daemon only when none answers); the feed keeps retrying meanwhile.
    pub(super) fn live_line(&self, context: &mut ViewContext<Self>) -> Option<View> {
        let start = self.chat.live.offers_start();
        let (words, retry) = match &self.chat.live {
            LiveStatus::Stale(_) => ("Catching up…".to_owned(), false),
            LiveStatus::Reconnecting { attempt, .. } if start => {
                (format!("Swarm is not answering (attempt {attempt})"), false)
            }
            LiveStatus::Reconnecting { attempt, .. } => (format!("Reconnecting {attempt}…"), false),
            LiveStatus::Stopped(reason) => (format!("Live updates stopped: {reason}"), true),
            _ => return None,
        };
        Some(
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::SM)
                .margin(Thickness::xy(0.0, tokens::spacing::XS))
                .automation_name(words.clone())
                .keyed_children(crate::views::slots([
                    Some(
                        controls::secondary(words)
                            .vertical_alignment(VerticalAlignment::Center)
                            .text_trimming(TextTrimming::CharacterEllipsis)
                            .into(),
                    ),
                    if retry {
                        Some(
                            controls::text_button()
                                .grid_column(1)
                                .automation_name("Retry live updates")
                                .on_click(context.message(Message::Chat(ChatMsg::LiveRetry)))
                                .content(controls::label("Retry"))
                                .into(),
                        )
                    } else if start {
                        Some(
                            controls::text_button()
                                .grid_column(1)
                                .automation_name("Start Swarm")
                                .on_click(context.message(Message::Shell(ShellMsg::RetryStartup)))
                                .content(controls::label("Start Swarm"))
                                .into(),
                        )
                    } else {
                        None
                    },
                ]))
                .into(),
        )
    }
}

/// The request's technical identity, folded under Details (§10.1).
fn details_view(subject: &str, detail: Option<&str>, open: bool, toggle: Callback<()>) -> View {
    let text: Option<View> = open.then(|| {
        controls::body(match detail {
            Some(detail) => format!("{subject}\n{detail}"),
            None => subject.to_owned(),
        })
        .font_size(tokens::text::KID_SIZE)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
        .into()
    });
    StackPanel::new()
        .spacing(tokens::spacing::XS)
        .keyed_children(crate::views::slots([
            Some(
                controls::text_button()
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .automation_name(if open { "Hide details" } else { "Details" })
                    .on_click(toggle)
                    .content(controls::icon_label(
                        if open {
                            controls::glyph::CHEVRON_UP
                        } else {
                            controls::glyph::CHEVRON_DOWN
                        },
                        "Details",
                    ))
                    .into(),
            ),
            text,
        ]))
        .into()
}
