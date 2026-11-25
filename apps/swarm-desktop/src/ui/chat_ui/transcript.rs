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
            Main::Project | Main::Inbox | Main::Agents | Main::Automations | Main::Settings => {
                String::new()
            }
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
                .filter(|detail| eliot_core::api::is_supported_harness(&detail.harness))
                .map(|_| context.callback(|()| Message::Launch(LaunchMsg::OpenModel))),
            toggle_tools: context.callback(|(key, expanded): (String, bool)| {
                Message::Chat(ChatMsg::ToolsToggled(key, expanded))
            }),
            view_changes: context
                .callback(|()| Message::Pane(crate::ui::pane_ui::PaneMsg::ViewChanges)),
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
        .maybe_width(column)
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
                .keyed_children(crate::views::slots([
                    Some(m3::bubble(
                        palette,
                        m3::body(shown.to_owned()).text_wrapping(TextWrapping::Wrap),
                        None,
                    )),
                    (!files.is_empty()).then(|| views::attached_chips(&files, palette)),
                ])),
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .automation_name(label)
                .children((
                    m3::secondary(label.to_owned()),
                    ProgressBar::new()
                        .is_indeterminate(true)
                        .width(tokens::layout::PROGRESS_WIDTH)
                        .horizontal_alignment(HorizontalAlignment::Left),
                )),
        ))
        .into()
}

/// Multiple-choice submission rule: the MSP wire carries exactly one of
/// `selectedLabels` or `freeText` per question, so the UI submits either
/// the current selections or one free-text answer, never a mix. Typing an
/// offered label checks that option: it joins the selections (still subject
/// to the prompt bounds) instead of submitting label-identical free text, so
/// a checked option and typed text never share a payload and the adapter can
/// map selections apart from free text. Typing anything else replaces the
/// selections with the one free-text answer.
pub(super) fn multiple_submission(
    selected: Vec<String>,
    option_ids: &[String],
    text: &str,
) -> Vec<String> {
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
pub(super) fn multiple_toggle(mut selected: Vec<String>, option: &str, on: bool) -> Vec<String> {
    selected.retain(|value| value != option);
    if on {
        selected.push(option.to_owned());
    }
    selected
}
