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
            .filter(|(read, _)| *read == agent)
            .map(|(_, items)| items.as_slice())
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
        items
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
        {
            let (agent, data_dir) = (agent.clone(), data_dir.clone());
            let _task = context.spawn_background(move |_| {
                let read = client::block_on(client::goal_get(&data_dir, &agent))
                    .map(|goal| goal.map(Box::new))
                    .map_err(|error| error.user_message());
                Message::Chat(ChatMsg::GoalRead(agent, read))
            });
        }
        let unknown = detail.state == eliot_core::AgentState::Unknown
            || self
                .chat
                .convo
                .as_ref()
                .is_some_and(Conversation::has_unconfirmed);
        if !detail.pending.is_empty() || unknown {
            let (agent, data_dir) = (agent.clone(), data_dir.clone());
            let _task = context.spawn_background(move |_| {
                let read = client::block_on(client::owner_inbox(&data_dir, None))
                    .map(|reply| reply.items)
                    .map_err(|error| error.user_message());
                Message::Chat(ChatMsg::DecisionsRead(agent, read))
            });
        } else {
            self.chat.decisions = None;
        }
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
    pub(super) fn decision_panel(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> Option<View> {
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
        let stop: View = match detail.active_turn.as_deref() {
            Some(turn) if !detail.observe_only => {
                let agent = self.selected_agent_id().unwrap_or_default();
                m3::text_button(palette)
                    .is_enabled(enabled)
                    .automation_name("Stop turn")
                    .on_click(context.message(Message::Chat(ChatMsg::InterruptRequested(
                        BoundAction::turn_control(&agent, detail.generation, turn),
                    ))))
                    .content(m3::label("Stop turn"))
            }
            _ => View::empty(),
        };
        let snooze = |item: Option<&OwnerInboxItem>, context: &mut ViewContext<Self>| -> View {
            match item {
                Some(item) => m3::icon_button(palette)
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
                    .content(m3::icon(m3::glyph::SNOOZE))
                    .tooltip("Snooze"),
                None => View::empty(),
            }
        };
        let (glyph, kind, question, subject, body, actions): (
            &str,
            String,
            String,
            String,
            View,
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
                let more: Vec<MenuItem> = approvals
                    .iter()
                    .skip(1)
                    .map(|choice| MenuItem::item(choice.label.clone(), choice.label.clone()))
                    .collect();
                let mut actions = vec![
                    KeyedView::new("stop", stop),
                    KeyedView::new("sp", Border::new().grid_column(1)),
                    KeyedView::new(
                        "snooze",
                        Border::new().grid_column(2).content(snooze(*item, context)),
                    ),
                    KeyedView::new(
                        "deny",
                        m3::outlined_small(palette)
                            .grid_column(3)
                            .is_enabled(enabled)
                            .automation_name(deny_label.clone())
                            .on_click(context.message(deny_message))
                            .content(m3::label(&deny_label)),
                    ),
                ];
                let split = split_action(
                    palette,
                    enabled,
                    &approve_label,
                    context.message(approve),
                    more.clone(),
                    context,
                );
                actions.push(KeyedView::new(
                    format!("approve:{}", more.len()),
                    Border::new().grid_column(4).content(split),
                ));
                (
                    m3::glyph::TERMINAL,
                    format!("Approval \u{b7} {harness}{count}"),
                    question,
                    subject,
                    View::empty(),
                    actions,
                )
            }
            Decision::Request(request, item) => {
                let key = request.binding.request.clone().unwrap_or_default();
                let secret = request.questions.iter().any(|question| question.is_secret);
                let questions: Vec<KeyedView> = request
                    .questions
                    .iter()
                    .map(|question| {
                        KeyedView::new(
                            question.id.clone(),
                            self.question_view(&key, question, context, palette),
                        )
                    })
                    .collect();
                let body: View = if secret {
                    m3::body("Answer this question in the agent's own application.")
                        .text_wrapping(TextWrapping::Wrap)
                        .into()
                } else {
                    StackPanel::new()
                        .spacing(tokens::spacing::SM)
                        .keyed_children(questions)
                };
                let answer: View =
                    if secret {
                        View::empty()
                    } else {
                        m3::filled(palette)
                            .grid_column(4)
                            .is_enabled(enabled && can_answer(request, &self.chat.answers))
                            .automation_name("Answer")
                            .on_click(context.message(Message::Chat(ChatMsg::AnswerRequested(
                                (*request).clone(),
                            ))))
                            .content(m3::label("Answer"))
                    };
                let actions = vec![
                    KeyedView::new(
                        "left",
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::SM)
                            .children((
                                stop,
                                m3::text_button(palette)
                                    .is_enabled(enabled)
                                    .automation_name("Cancel request")
                                    .on_click(context.message(Message::Chat(
                                        ChatMsg::ApprovalRequested((*request).clone(), false, None),
                                    )))
                                    .content(m3::label("Cancel request")),
                            )),
                    ),
                    KeyedView::new("sp", Border::new().grid_column(1)),
                    KeyedView::new(
                        "snooze",
                        Border::new().grid_column(2).content(snooze(*item, context)),
                    ),
                    KeyedView::new("answer", answer),
                ];
                (
                    m3::glyph::INFO,
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
                        m3::filled(palette)
                    } else {
                        m3::outlined_small(palette)
                    };
                    buttons.push(KeyedView::new(
                        choice.to_owned(),
                        button
                            .is_enabled(enabled && reason_ready)
                            .automation_name(format!("Resolve as {words}"))
                            .on_click(decide)
                            .content(m3::label(&words)),
                    ));
                }
                let body = m3::field()
                    .text(self.chat.decide_reason.clone())
                    .placeholder_text("Why (required)")
                    .automation_name("Reason for the outcome")
                    .on_text_changed(
                        context.callback(|text| Message::Chat(ChatMsg::DecideReason(text))),
                    );
                let actions = vec![
                    KeyedView::new("stop", stop),
                    KeyedView::new("sp", Border::new().grid_column(1)),
                    KeyedView::new(
                        "snooze",
                        Border::new()
                            .grid_column(2)
                            .content(snooze(Some(item), context)),
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
                ];
                let what = if item.kind == OwnerInboxKind::UnknownTurn {
                    "This turn's outcome is unknown"
                } else {
                    "This command's outcome is unknown"
                };
                (
                    m3::glyph::ERROR,
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
                    item.subject.clone(),
                    body.into(),
                    actions,
                )
            }
        };
        let steps: View = if items.len() > 1 {
            StackPanel::new()
                .grid_column(2)
                .orientation(Orientation::Horizontal)
                .children((
                    m3::icon_button(palette)
                        .is_enabled(at > 0)
                        .automation_name("Previous request")
                        .on_click(context.message(Message::Chat(ChatMsg::DecisionStep(-1))))
                        .content(m3::icon(m3::glyph::CHEVRON_LEFT)),
                    m3::icon_button(palette)
                        .is_enabled(at + 1 < items.len())
                        .automation_name("Next request")
                        .on_click(context.message(Message::Chat(ChatMsg::DecisionStep(1))))
                        .content(m3::icon(m3::glyph::CHEVRON_RIGHT)),
                ))
        } else {
            View::empty()
        };
        let kind_line = Grid::new()
            .min_height(tokens::height::CONTROL)
            .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
            .column_spacing(tokens::spacing::CHIP_GAP)
            .children((
                m3::carrier(
                    palette,
                    |s| s.tertiary,
                    "",
                    tokens::icon::CHEVRON,
                    m3::xs_icon(glyph),
                ),
                m3::text(kind.clone(), tokens::text::LABEL)
                    .grid_column(1)
                    .foreground(palette.tertiary())
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_trimming(TextTrimming::CharacterEllipsis),
                steps,
            ));
        let subject: View = if subject.trim().is_empty() {
            View::empty()
        } else {
            Border::new()
                .background(palette.code())
                .border_brush(palette.outline())
                .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
                .corner_radius(tokens::radius::CONTAINER)
                .padding(Thickness::xy(tokens::spacing::INSET, tokens::spacing::SM))
                .content(
                    m3::body(subject)
                        .font_size(tokens::text::KID_SIZE)
                        .text_wrapping(TextWrapping::Wrap)
                        .is_text_selection_enabled(true),
                )
        };
        let card = Border::new()
            .background(palette.composer())
            .border_brush(palette.tertiary())
            .border_thickness(Thickness::uniform(tokens::layout::DECISION_OUTLINE))
            .corner_radius(tokens::radius::COMPOSER)
            .padding(Thickness::new(
                tokens::layout::DECISION_LEADING,
                tokens::spacing::SM,
                tokens::spacing::MD,
                tokens::spacing::INSET,
            ))
            .automation_name(kind)
            .content(
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    kind_line,
                    m3::body(question)
                        .text_wrapping(TextWrapping::Wrap)
                        .is_text_selection_enabled(true),
                    subject,
                    body,
                    Grid::new()
                        .columns([
                            GridLength::Auto,
                            GridLength::STAR,
                            GridLength::Auto,
                            GridLength::Auto,
                            GridLength::Auto,
                        ])
                        .column_spacing(tokens::spacing::SM)
                        .keyed_children(actions),
                )),
            );
        Some(
            Border::new()
                .grid_row(5)
                .width(column)
                .max_width(tokens::layout::READING_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(
                    0.0,
                    tokens::spacing::SM,
                    0.0,
                    tokens::spacing::LG,
                ))
                .content(card),
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
        palette: Palette,
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
        let omit: View = if values.is_empty() {
            View::empty()
        } else {
            m3::text_button(palette)
                .horizontal_alignment(HorizontalAlignment::Left)
                .on_click(context.message(Message::Chat(ChatMsg::AnswerChanged(
                    request.clone(),
                    id.clone(),
                    Vec::new(),
                ))))
                .content(m3::label("Leave unanswered"))
        };
        let control: View = match question.kind {
            _ if question.is_secret => {
                m3::body("Answer this question in the agent's own application.").into()
            }
            QuestionKind::Resource => m3::body(
                "This step needs action in the agent's own application. You can cancel this request here.",
            )
            .text_wrapping(TextWrapping::Wrap)
            .into(),
            QuestionKind::Number | QuestionKind::Text => {
                let before = values.first().cloned().unwrap_or_default();
                m3::field()
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
                    .on_text_changed(context.callback(move |text| {
                        if text == before {
                            Message::Chat(ChatMsg::AnswerUnchanged)
                        } else {
                            changed(vec![text])
                        }
                    }))
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
                let chips: Vec<KeyedView> = options
                    .iter()
                    .map(|(option, label)| {
                        let on = selected.as_deref() == Some(option.as_str());
                        KeyedView::new(
                            format!("{option}:{on}"),
                            m3::segment(palette, on)
                                .automation_name(label.clone())
                                .on_click(context.message(Message::Chat(ChatMsg::AnswerChanged(
                                    request.clone(),
                                    id.clone(),
                                    vec![option.clone()],
                                ))))
                                .content(m3::segment_label(label)),
                        )
                    })
                    .collect();
                let chips = StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .keyed_children(chips);
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
                            m3::field()
                                .text(before.clone())
                                .placeholder_text("Other answer")
                                .automation_name("Other answer")
                                .on_text_changed(context.callback(move |text| {
                                    if text == before {
                                        Message::Chat(ChatMsg::AnswerUnchanged)
                                    } else {
                                        Message::Chat(ChatMsg::AnswerChanged(
                                            request.clone(),
                                            id.clone(),
                                            vec![text],
                                        ))
                                    }
                                })),
                        ))
                } else {
                    chips
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
                                .on_is_checked_changed(context.callback(move |on| {
                                    Message::Chat(ChatMsg::AnswerChanged(
                                        request.clone(),
                                        id.clone(),
                                        transcript::multiple_toggle(selected.clone(), &option_id, on),
                                    ))
                                }))
                                .content(m3::label(&option.label)),
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
                        m3::field()
                            .text(other.clone())
                            .accepts_return(true)
                            .placeholder_text("Other answer")
                            .automation_name("Other answer")
                            .on_text_changed(context.callback(move |text: String| {
                                if text == other {
                                    return Message::Chat(ChatMsg::AnswerUnchanged);
                                }
                                changed(transcript::multiple_submission(
                                    selected.clone(),
                                    &option_ids,
                                    &text,
                                ))
                            })),
                    ));
                }
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .keyed_children(controls)
            }
        };
        let title: View = match question.title.as_deref().zip(question.prompt.as_deref()) {
            Some((title, _)) if !title.trim().is_empty() => {
                m3::text(title.to_owned(), tokens::text::BUTTON)
                    .text_wrapping(TextWrapping::Wrap)
                    .into()
            }
            _ => View::empty(),
        };
        StackPanel::new().spacing(tokens::spacing::SM).children((
            title,
            match question.prompt.as_deref() {
                Some(prompt) if !prompt.trim().is_empty() => m3::body(prompt.to_owned())
                    .text_wrapping(TextWrapping::Wrap)
                    .into(),
                _ => View::empty(),
            },
            control,
            omit,
        ))
    }

    /// The provider-hold strip above the composer (`AttentionStrip`,
    /// `tertiary-container`): the hold in one line, then by its kind Wait
    /// and Switch model (a limit), Sign in (a sign-in), Pick another model
    /// (a removed or mismatched model), and the corrective choices in a
    /// menu, each with the daemon's evidence code.
    pub(super) fn hold_strip(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> Option<View> {
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
                        m3::text_button(palette)
                            .resource_overrides(palette.strip_button())
                            .automation_name("Wait")
                            .on_click(context.message(Message::Chat(ChatMsg::HoldWait)))
                            .content(m3::label("Wait")),
                        m3::tonal(palette)
                            .is_enabled(enabled)
                            .automation_name("Switch model")
                            .on_click(context.message(Message::Chat(ChatMsg::AgentMenu(
                                menu_ui::Action::Handoff,
                            ))))
                            .content(m3::label("Switch model")),
                    ))
            }
            Kind::Auth => m3::tonal(palette)
                .is_enabled(enabled)
                .automation_name(format!("Sign in to {harness}"))
                .on_click(context.message(Message::Chat(ChatMsg::SignIn)))
                .content(m3::label("Sign in")),
            Kind::ModelRemoved | Kind::ModelMismatch => m3::tonal(palette)
                .is_enabled(enabled)
                .automation_name("Pick another model")
                .on_click(
                    context.message(Message::Chat(ChatMsg::AgentMenu(menu_ui::Action::Handoff))),
                )
                .content(m3::label("Pick another model")),
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
        let (container, on) = palette.strip();
        Some(
            Border::new()
                .background(container)
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
                            m3::carrier(
                                palette,
                                |s| s.on_tertiary_container,
                                "",
                                tokens::icon::ROW,
                                m3::icon(m3::glyph::HOLD),
                            ),
                            StackPanel::new()
                                .grid_column(1)
                                .orientation(Orientation::Horizontal)
                                .spacing(tokens::spacing::XS)
                                .vertical_alignment(VerticalAlignment::Center)
                                .children((
                                    m3::text(lead, tokens::text::BUTTON).foreground(on),
                                    m3::body(rest)
                                        .foreground(on)
                                        .text_trimming(TextTrimming::CharacterEllipsis),
                                )),
                            Border::new()
                                .grid_column(2)
                                .vertical_alignment(VerticalAlignment::Center)
                                .content(primary),
                            m3::icon_button(palette)
                                .resource_overrides(palette.strip_button())
                                .grid_column(3)
                                .automation_name(if scope {
                                    "Other options: I've signed in, I've fixed the settings, Clear anyway"
                                } else {
                                    "Other options (the hold's scope is not read yet)"
                                })
                                .content(m3::icon(m3::glyph::MORE_VERTICAL))
                                .menu(Menu::new(
                                    items,
                                    context.callback(|label: String| {
                                        HOLD_CLEARS
                                            .iter()
                                            .find(|(known, _)| *known == label)
                                            .map_or(Message::Menu(MenuMsg::Ignored), |(_, evidence)| {
                                                Message::Chat(ChatMsg::HoldClear(*evidence))
                                            })
                                    }),
                                )),
                        )),
                ),
        )
    }

    /// The workers line above a manager's composer: "2 workers running ·
    /// 1 waiting for you" (its `nav` rows).
    pub(super) fn workers_line(&self, palette: Palette) -> Option<View> {
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
                .height(tokens::height::CONTROL)
                .padding(Thickness::xy(tokens::spacing::INSET, 0.0))
                .automation_name(words.clone())
                .content(
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .vertical_alignment(VerticalAlignment::Center)
                        .children((
                            m3::muted_icon(m3::glyph::HUB),
                            m3::text(words, tokens::text::LABEL)
                                .foreground(palette.muted())
                                .vertical_alignment(VerticalAlignment::Center),
                        )),
                ),
        )
    }

    /// The live line under the header (5.2.1): "Reconnecting…", "Catching
    /// up…" or "Live updates stopped" with Retry.
    pub(super) fn live_line(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let (words, retry) = match &self.chat.live {
            LiveStatus::Stale(_) => ("Catching up…".to_owned(), false),
            LiveStatus::Reconnecting { attempt, .. } => (format!("Reconnecting {attempt}…"), false),
            LiveStatus::Stopped(reason) => (format!("Live updates stopped: {reason}"), true),
            _ => return View::empty(),
        };
        StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(tokens::spacing::SM)
            .horizontal_alignment(HorizontalAlignment::Center)
            .margin(Thickness::xy(0.0, tokens::spacing::XS))
            .automation_name(words.clone())
            .children((
                m3::secondary(words)
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_trimming(TextTrimming::CharacterEllipsis),
                if retry {
                    m3::text_button(palette)
                        .automation_name("Retry live updates")
                        .on_click(context.message(Message::Chat(ChatMsg::LiveRetry)))
                        .content(m3::label("Retry"))
                } else {
                    View::empty()
                },
            ))
    }
}

/// A filled split action (`SplitButton`): the main choice and, 2 DIPs after
/// it, the menu of its variants (none: the main choice alone).
fn split_action(
    palette: Palette,
    enabled: bool,
    label: &str,
    click: Callback<()>,
    more: Vec<MenuItem>,
    context: &mut ViewContext<DesktopApp>,
) -> View {
    let main = m3::filled(palette)
        .is_enabled(enabled)
        .automation_name(label.to_owned())
        .on_click(click)
        .content(
            m3::text(label.to_owned(), tokens::text::EMPHASIZED).margin(Thickness::xy(
                tokens::spacing::MD - tokens::spacing::ROW,
                0.0,
            )),
        );
    let menu: View = if more.is_empty() {
        View::empty()
    } else {
        m3::icon_button(palette)
            .resource_overrides(palette.filled())
            .width(tokens::layout::SPLIT_MENU)
            .is_enabled(enabled)
            .automation_name("Approval options")
            .content(m3::icon(m3::glyph::CHEVRON_DOWN))
            .menu(Menu::new(
                more,
                // Capture-free: one identity across renders; the label
                // names the request's choice when the click is handled.
                context.callback(|label: String| Message::Chat(ChatMsg::ApproveChoice(label))),
            ))
    };
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::ROW_GAP)
        .vertical_alignment(VerticalAlignment::Center)
        .children((main, menu))
}
