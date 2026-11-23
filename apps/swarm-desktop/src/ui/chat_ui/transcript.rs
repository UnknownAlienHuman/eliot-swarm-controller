use super::*;

impl DesktopApp {
    /// Hand typing to the composer once the conversation is drawn; after a
    /// send (`scroll`), first scroll the transcript to its end
    /// (`crate::scroll_end`) in case it was read further up. An opened
    /// conversation needs no scroll: the transcript is anchored at its end.
    pub(super) fn follow_latest(&self, scroll: bool, context: &ComponentContext<Self>) {
        let epoch = self.chat.epoch;
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            // False: the conversation was left while waiting.
            if !feed::backoff(FOLLOW_DELAY_MS, &cancelled) || !scroll {
                return Message::Chat(ChatMsg::FollowLatest(epoch, Ok(())));
            }
            let outcome = match crate::app_resources::own_window() {
                None => Err("the window was not found".to_owned()),
                Some(window) => match crate::scroll_end::scroll_to_end(window, TRANSCRIPT_ID) {
                    Ok(Some(_)) => Ok(()),
                    Ok(None) => Err("the transcript was not realized".to_owned()),
                    Err(error) => Err(error),
                },
            };
            Message::Chat(ChatMsg::FollowLatest(epoch, outcome))
        });
    }

    pub(in crate::ui) fn rebuild_items(&mut self) {
        let items = self
            .chat
            .convo
            .as_ref()
            .map(Conversation::items)
            .unwrap_or_default();
        let rendered = self.chat.render_cache.render(&items, &self.chat.docs);
        // An unchanged conversation keeps the list it had, so the
        // transcript's input compares equal and nothing is redrawn.
        let unchanged = rendered.len() == self.chat.items.len()
            && rendered
                .iter()
                .zip(self.chat.items.iter())
                .all(|(new, old)| {
                    new.key == old.key
                        && new.parts.len() == old.parts.len()
                        && new
                            .parts
                            .iter()
                            .zip(&old.parts)
                            .all(|(new, old)| Rc::ptr_eq(new, old))
                });
        if !unchanged {
            self.chat.items = Rc::new(rendered);
        }
        let live: HashSet<u64> = items
            .iter()
            .flat_map(|item| item.parts.iter())
            .map(|part| crate::chat::text_hash(&part.text))
            .collect();
        self.chat.docs.retain(|hash, _| live.contains(hash));
    }

    /// The transcript ([`views::Transcript`]): "Load older" when more
    /// history exists, then every turn in the reading column. It is its own
    /// component: a window render compares its input and redraws it only
    /// when the conversation, its expanded rows, the column or the palette
    /// changed.
    pub(super) fn transcript_view(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        // Settings and a project page leave the conversation as New task does.
        let conversation = match self.main {
            Main::Conversation => self.selected_agent_id().unwrap_or_default(),
            Main::Project | Main::Settings => String::new(),
        };
        let open = !conversation.is_empty();
        // Not while the first read is in flight: its coverage is not known.
        let loading = matches!(self.chat.live, LiveStatus::Loading);
        let load_older = self
            .chat
            .convo
            .as_ref()
            .filter(|convo| {
                open && convo.coverage().can_load_older() && !self.chat.items.is_empty()
            })
            .map(|_| {
                (
                    !loading,
                    context.callback(|()| Message::Chat(ChatMsg::LoadOlderRequested)),
                )
            });
        let empty = (open && self.chat.items.is_empty()).then_some(match self.chat.live {
            LiveStatus::Loading => "Loading…",
            _ => "No messages yet.",
        });
        let workspace = self
            .chat
            .detail
            .as_ref()
            .and_then(|detail| detail.workspace.clone())
            .unwrap_or_default();
        let path_scope = format!("chat:{}:", self.chat.epoch);
        let path_failure = self
            .revealed
            .clone()
            .filter(|(item, _)| item.starts_with(&path_scope));
        // Every callback comes from a captureless mapper, so it compares
        // equal from one window render to the next.
        let callbacks = RowCallbacks {
            open_path: context.callback(|(key, path)| Message::Reveal(key, path)),
            path_scope,
            path_failure,
            workspace,
            copy_part: context.callback(|value| Message::Chat(ChatMsg::CopyPart(value))),
            copy_code: context.callback(|(key, index): (String, usize)| {
                Message::Chat(ChatMsg::CopyCode(key, index))
            }),
            retry: context.callback(|key| Message::Chat(ChatMsg::RetryInput(key))),
            pick_model: self
                .chat
                .detail
                .as_ref()
                .filter(|detail| {
                    eliot_core::api::ConfigureSupport::for_harness(&detail.harness)
                        .model_refusal
                        .is_none()
                })
                .map(|_| context.callback(|()| Message::Launch(LaunchMsg::OpenModel))),
            toggle_tools: context.callback(|(key, expanded): (String, bool)| {
                Message::Chat(ChatMsg::ToolsToggled(key, expanded))
            }),
        };
        View::component::<views::Transcript>(views::TranscriptInput {
            conversation,
            items: if open {
                views::Shared(Rc::clone(&self.chat.items))
            } else {
                views::no_items()
            },
            expanded: Rc::new(self.chat.expanded.clone()),
            callbacks: Rc::new(callbacks),
            palette,
            column,
            load_older,
            empty,
            automation_id: TRANSCRIPT_ID,
        })
    }

    /// Pending requests of the open Agent: what it asks, and the answers.
    pub(super) fn pending_view(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let pending_rows: Vec<KeyedView> =
            self.chat
                .detail
                .as_ref()
                .map(|detail| detail.pending.as_slice())
                .unwrap_or_default()
                .iter()
                .map(|request| {
                    let key = request.binding.request.clone().unwrap_or_default();
                    let questions = request.questions.iter().map(|question| {
                        KeyedView::new(
                            question.id.clone(),
                            self.question_view(&key, question, context),
                        )
                    });
                    let answer_box: View = StackPanel::new()
                        .spacing(tokens::spacing::SM)
                        .keyed_children(questions.collect::<Vec<_>>());
                    let mut actions = vec![KeyedView::new(
                        "cancel",
                        m3::outlined(palette)
                            .is_enabled(!self.submitting())
                            .on_click(context.message(Message::Chat(ChatMsg::ApprovalRequested(
                                request.clone(),
                                false,
                            ))))
                            .content(m3::label(if request.kind == "approval" {
                                "Deny"
                            } else {
                                "Cancel request"
                            })),
                    )];
                    if request.kind == "approval" {
                        actions.insert(
                            0,
                            KeyedView::new(
                                "approve",
                                m3::filled(palette)
                                    .is_enabled(!self.submitting())
                                    .on_click(context.message(Message::Chat(
                                        ChatMsg::ApprovalRequested(request.clone(), true),
                                    )))
                                    .content(m3::label("Approve")),
                            ),
                        );
                    } else if request.kind == "question" {
                        actions.push(KeyedView::new(
                            "answer",
                            m3::text_button(palette)
                                .is_enabled(
                                    !self.submitting() && can_answer(request, &self.chat.answers),
                                )
                                .on_click(context.message(Message::Chat(ChatMsg::AnswerRequested(
                                    request.clone(),
                                ))))
                                .content(m3::label("Answer")),
                        ));
                    }
                    let (fill, on) = palette.status_chip(crate::theme::StatusTone::Attention);
                    KeyedView::new(
                        key,
                        Border::new()
                            .background(fill)
                            .corner_radius(tokens::radius::CONTAINER)
                            .padding(Thickness::uniform(tokens::spacing::MD))
                            .content(
                                StackPanel::new().spacing(tokens::spacing::SM).children((
                                    m3::section_title(if request.kind == "approval" {
                                        "Permission requested"
                                    } else {
                                        "Your answer is needed"
                                    })
                                    .foreground(on),
                                    // The title carries the attention tone;
                                    // the request itself (often a long
                                    // command or edit) reads in the body color.
                                    TextBlock::new()
                                        .text(request.summary.clone())
                                        .foreground(palette.text())
                                        .text_wrapping(TextWrapping::Wrap)
                                        .is_text_selection_enabled(true),
                                    answer_box,
                                    StackPanel::new()
                                        .orientation(Orientation::Horizontal)
                                        .spacing(tokens::spacing::SM)
                                        .keyed_children(actions),
                                )),
                            ),
                    )
                })
                .collect();
        StackPanel::new()
            .grid_row(3)
            .spacing(tokens::spacing::SM)
            .width(column)
            .max_width(tokens::layout::READING_WIDTH)
            .horizontal_alignment(HorizontalAlignment::Center)
            .keyed_children(pending_rows)
    }

    /// Controls keep the universal string-list answer shape. The plugin still
    /// validates against the retained native offer immediately before writing.
    fn question_view(
        &self,
        request: &str,
        question: &eliot_core::requests::Question,
        context: &mut ViewContext<Self>,
    ) -> View {
        use eliot_core::requests::QuestionKind;
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
            Button::new()
                .on_click(context.message(Message::Chat(ChatMsg::AnswerChanged(
                    request.clone(),
                    id.clone(),
                    Vec::new(),
                ))))
                .content(m3::label("Leave unanswered"))
        };
        let control: View = if question.is_secret {
            m3::label("Answer this confidential question in the Agent's native application.").into()
        } else {
            match question.kind {
            QuestionKind::Resource => m3::label("This step needs action in the native application. You can cancel this request here.").into(),
            QuestionKind::Number | QuestionKind::Text => {
                let before = values.first().cloned().unwrap_or_default();
                m3::field().text(before.clone())
                    .placeholder_text(if question.kind == QuestionKind::Number { "Enter a number" } else { "Your answer" })
                    .automation_name(if question.kind == QuestionKind::Number { "Number answer" } else { "Text answer" })
                    .on_text_changed(context.callback(move |text| if text == before {
                        Message::Chat(ChatMsg::AnswerUnchanged)
                    } else { changed(vec![text]) })).into()
            },
            QuestionKind::Boolean | QuestionKind::Single => {
                let options: Vec<(String, String)> = if question.kind == QuestionKind::Boolean {
                    vec![("true".to_owned(), "Yes".to_owned()), ("false".to_owned(), "No".to_owned())]
                } else { question.options.iter().map(|option| (option.id.clone(), option.label.clone())).collect() };
                let ids: Vec<String> = options.iter().map(|(id, _)| id.clone()).collect();
                let selected = values.first().and_then(|value| ids.iter().position(|id| id == value));
                let pick = ComboBox::new().items_source(options.into_iter().map(|(_, label)| label).collect::<Vec<_>>())
                    .selected_index(selected).placeholder_text("Choose an answer")
                    .automation_name("Choose an answer")
                    .on_selection_changed(context.callback(move |index: Option<usize>|
                        index.and_then(|index| ids.get(index).cloned()).map_or(
                            Message::Chat(ChatMsg::AnswerUnchanged), |id| changed(vec![id]))));
                if question.free_entry {
                    let request = request.clone(); let id = id.clone();
                    let before = if selected.is_none() { values.first().cloned().unwrap_or_default() } else { String::new() };
                    StackPanel::new().spacing(tokens::spacing::SM).children((pick,
                        m3::field().text(before.clone())
                            .placeholder_text("Or enter another answer")
                            .automation_name("Other answer")
                            .on_text_changed(context.callback(move |text| if text == before {
                                Message::Chat(ChatMsg::AnswerUnchanged)
                            } else { Message::Chat(ChatMsg::AnswerChanged(request.clone(), id.clone(), vec![text])) }))))
                } else { pick.into() }
            }
            QuestionKind::Multiple => {
                // One answer per question: the wire carries either the
                // selected labels or one free text, never a mix the adapter
                // must reject. Toggling an option keeps selections only;
                // typing an offered label checks that option (it joins the
                // selections, still subject to the prompt bounds), while
                // typing anything else replaces the selections with the one
                // free-text answer.
                let selected: Vec<String> = values
                    .iter()
                    .filter(|value| {
                        question.options.iter().any(|option| &option.id == *value)
                    })
                    .cloned()
                    .collect();
                let mut controls: Vec<KeyedView> = question.options.iter().map(|option| {
                    let request = request.clone(); let id = id.clone(); let option_id = option.id.clone(); let selected = selected.clone();
                    KeyedView::new(option.id.clone(), CheckBox::new()
                        .is_checked(values.contains(&option.id)).automation_name(option.label.clone())
                        .on_is_checked_changed(context.callback(move |on| {
                            Message::Chat(ChatMsg::AnswerChanged(request.clone(), id.clone(), multiple_toggle(selected.clone(), &option_id, on)))
                        })).content(m3::label(&option.label)))
                }).collect();
                if question.free_entry {
                    let other = values.iter().filter(|value| !selected.contains(value)).cloned().collect::<Vec<_>>().join("\n");
                    let option_ids: Vec<String> = question.options.iter().map(|option| option.id.clone()).collect();
                    controls.push(KeyedView::new("free", m3::field().text(other.clone()).accepts_return(true)
                        .placeholder_text("Or enter another answer").automation_name("Other answer")
                        .on_text_changed(context.callback(move |text: String| {
                            if text == other { return Message::Chat(ChatMsg::AnswerUnchanged); }
                            changed(multiple_submission(selected.clone(), &option_ids, &text))
                        }))));
                }
                StackPanel::new().spacing(tokens::spacing::SM).keyed_children(controls)
            }

        }
        };
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::label(question.title.as_deref().unwrap_or("Your answer")),
            TextBlock::new()
                .text(question.prompt.clone().unwrap_or_default())
                .text_wrapping(TextWrapping::Wrap),
            control,
            omit,
        ))
    }
}

/// A new task's first message and its "Starting Codex…" line while the
/// launch is submitted, in the transcript's place and shape.
pub(super) fn starting_view(
    text: &str,
    label: &str,
    column: Option<f64>,
    palette: Palette,
) -> View {
    let (shown, files) = crate::attachments::split(text);
    StackPanel::new()
        .grid_row(1)
        .width(column)
        .max_width(tokens::layout::READING_WIDTH)
        .horizontal_alignment(HorizontalAlignment::Center)
        .vertical_alignment(VerticalAlignment::Top)
        .margin(Thickness::xy(0.0, tokens::spacing::MD))
        .spacing(tokens::spacing::SM)
        .children((
            // The owner's message as the transcript will show it: the
            // bubble, and attached files as chips under it.
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .horizontal_alignment(HorizontalAlignment::Right)
                .max_width(tokens::layout::READING_WIDTH * tokens::layout::BUBBLE_SHARE)
                .automation_name(format!("You: {shown}"))
                .children((
                    m3::bubble(
                        palette,
                        m3::body(shown.to_owned()).text_wrapping(TextWrapping::Wrap),
                        None,
                    ),
                    if files.is_empty() {
                        View::empty()
                    } else {
                        views::attached_chips(&files, palette)
                    },
                )),
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .automation_name(label.to_owned())
                .children((
                    m3::secondary(label.to_owned()),
                    ProgressBar::new()
                        .is_indeterminate(true)
                        .width(tokens::layout::PROGRESS_WIDTH)
                        .horizontal_alignment(HorizontalAlignment::Left),
                )),
        ))
}

/// Multiple-choice submission rule: the MSP wire carries exactly one of
/// `selectedLabels` or `freeText` per question, so the UI submits either
/// the current selections or one free-text answer, never a mix. Typing an
/// offered label checks that option: it joins the selections (still subject
/// to the prompt bounds) instead of submitting label-identical free text, so
/// a checked option and typed text never share a payload and the adapter can
/// map selections apart from free text. Typing anything else replaces the
/// selections with the one free-text answer.
fn multiple_submission(selected: Vec<String>, option_ids: &[String], text: &str) -> Vec<String> {
    if text.is_empty() {
        selected
    } else if option_ids.iter().any(|id| id == text) {
        let mut picked = selected;
        if !picked.iter().any(|value| value == text) {
            picked.push(text.to_owned());
        }
        picked
    } else {
        vec![text.to_owned()]
    }
}

/// Multiple-choice toggle rule: (un)checking an option keeps selections
/// only, dropping any free text so a later submit cannot mix.
fn multiple_toggle(mut selected: Vec<String>, option: &str, on: bool) -> Vec<String> {
    selected.retain(|value| value != option);
    if on {
        selected.push(option.to_owned());
    }
    selected
}
