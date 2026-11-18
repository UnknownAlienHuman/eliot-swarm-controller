use super::*;

impl DesktopApp {
    /// Bring the newest turn into view after the next render.
    pub(super) fn follow_latest(&self, tries: u32, context: &ComponentContext<Self>) {
        let epoch = self.chat.epoch;
        let _task = context.spawn_background(move |cancel| {
            let cancelled = || cancel.is_cancelled();
            let _ = feed::backoff(FOLLOW_DELAY_MS, &cancelled);
            Message::Chat(ChatMsg::FollowLatest(epoch, tries))
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
        self.chat.items_revision = views::key_revision(&rendered);
        self.chat.items = Rc::new(rendered);
        let live: HashSet<u64> = items
            .iter()
            .flat_map(|item| item.parts.iter())
            .map(|part| crate::chat::text_hash(&part.text))
            .collect();
        self.chat.docs.retain(|hash, _| live.contains(hash));
    }

    /// The transcript: "Load older" when more history exists, then every
    /// turn in the reading column; the newest turn carries the end anchor.
    pub(super) fn transcript_view(
        &self,
        column: Option<f64>,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let load_older: View = match self.chat.convo.as_ref() {
            Some(convo) if convo.coverage().can_load_older() => m3::text_button(palette)
                .is_enabled(!matches!(self.chat.live, LiveStatus::Loading))
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::MD))
                .on_click(context.message(Message::Chat(ChatMsg::LoadOlderRequested)))
                .content(m3::label("Load older messages")),
            _ => View::empty(),
        };
        let empty: View = if self.chat.items.is_empty() {
            m3::secondary(match self.chat.live {
                LiveStatus::Loading => "Loading…",
                _ => "No messages yet.",
            })
            .horizontal_alignment(HorizontalAlignment::Center)
            .into()
        } else {
            View::empty()
        };
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
        let path_code = path_failure.as_ref().map_or(0, |(item, note)| {
            crate::chat::text_hash(item) ^ crate::chat::text_hash(note)
        });
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
                    column,
                ),
            )]),
            None => View::empty(),
        };
        let items = Rc::clone(&self.chat.items);
        let keys = Rc::clone(&self.chat.items);
        // A palette or width change re-realises the rows in the new colors
        // and at the new column width.
        let palette_code = palette
            .scheme()
            .map_or(0, |scheme| 1 + u64::from(scheme.dark));
        let width_code = column.map_or(0, |width| width.round() as u64);
        // A toggle in an older virtualized turn must re-realize its card too.
        let expansion_code = self
            .chat
            .expanded
            .iter()
            .fold(0, |code, key| code ^ crate::chat::text_hash(key));
        let source = VirtualSource::new(
            self.chat
                .items_revision
                .wrapping_mul(3)
                .wrapping_add(palette_code)
                .wrapping_add(expansion_code)
                .wrapping_add(path_code)
                .wrapping_add(width_code.wrapping_mul(0x9E37_79B9)),
            earlier,
            move |index| keys[index].key.clone(),
            move |index| {
                views::item_view(&items[index], &expanded, &callbacks, palette, None, column)
            },
        );
        ScrollViewer::new()
            .grid_row(1)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .automation_name("Conversation")
            .content(
                StackPanel::new()
                    .width(column)
                    .max_width(tokens::layout::READING_WIDTH)
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .margin(Thickness::xy(0.0, tokens::spacing::MD))
                    .children((
                        load_older,
                        empty,
                        ItemsRepeater::new().virtual_source(source),
                        latest,
                    )),
            )
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
                    let (fill, on) = palette.attention();
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
                                    TextBlock::new()
                                        .text(request.summary.clone())
                                        .foreground(on)
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
                // typing replaces the selections with one text answer.
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
                    controls.push(KeyedView::new("free", m3::field().text(other.clone()).accepts_return(true)
                        .placeholder_text("Or enter another answer").automation_name("Other answer")
                        .on_text_changed(context.callback(move |text: String| {
                            if text == other { return Message::Chat(ChatMsg::AnswerUnchanged); }
                            changed(multiple_submission(selected.clone(), &text))
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
    StackPanel::new()
        .grid_row(1)
        .width(column)
        .max_width(tokens::layout::READING_WIDTH)
        .horizontal_alignment(HorizontalAlignment::Center)
        .vertical_alignment(VerticalAlignment::Top)
        .margin(Thickness::xy(0.0, tokens::spacing::MD))
        .spacing(tokens::spacing::SM)
        .children((
            Border::new()
                .horizontal_alignment(HorizontalAlignment::Right)
                .max_width(tokens::layout::READING_WIDTH * tokens::layout::BUBBLE_SHARE)
                .background(palette.bubble())
                .corner_radius(tokens::radius::CONTAINER)
                .padding(Thickness::xy(tokens::spacing::INSET, tokens::spacing::SM))
                .automation_name(format!("You: {text}"))
                .content(
                    TextBlock::new()
                        .text(text.to_owned())
                        .foreground(palette.on_bubble())
                        .text_wrapping(TextWrapping::Wrap),
                ),
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
/// the current selections or one free-text answer, never a mix.
fn multiple_submission(selected: Vec<String>, text: &str) -> Vec<String> {
    if text.is_empty() {
        selected
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

#[cfg(test)]
mod tests {
    use super::{multiple_submission, multiple_toggle};

    #[test]
    fn multiple_choice_submits_either_selections_or_one_text() {
        // Audit #639: typing after checking an option must not mix the
        // selections with free text; the wire accepts exactly one shape.
        assert_eq!(
            multiple_submission(vec!["a".to_owned(), "b".to_owned()], ""),
            vec!["a".to_owned(), "b".to_owned()]
        );
        assert_eq!(
            multiple_submission(vec!["a".to_owned()], "my own wording"),
            vec!["my own wording".to_owned()]
        );
        // Multi-line typing stays one text answer, not one answer per line.
        assert_eq!(
            multiple_submission(vec!["a".to_owned()], "first\nsecond"),
            vec!["first\nsecond".to_owned()]
        );
    }

    #[test]
    fn multiple_choice_toggle_keeps_selections_only() {
        assert_eq!(
            multiple_toggle(vec!["a".to_owned()], "b", true),
            vec!["a".to_owned(), "b".to_owned()]
        );
        assert_eq!(
            multiple_toggle(vec!["a".to_owned(), "b".to_owned()], "a", false),
            vec!["b".to_owned()]
        );
    }
}
