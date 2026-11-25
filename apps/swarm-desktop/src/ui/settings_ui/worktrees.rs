use super::*;
use eliot_core::api::ArtifactKind;

impl DesktopApp {
    /// The cleanup review under its row: the plan by kind (a checkbox per
    /// group that has something to remove, each item with its size and its
    /// keep or remove reason), Remove with one inline confirmation, then the
    /// result: removed, kept with why, and unknown.
    fn cleanup_review(&self, context: &mut ViewContext<Self>) -> View {
        let state = &self.settings;
        // A task title when Swarm knows one (else the item keeps its id).
        let title = |agent: &str| {
            let title = self.agent_title(agent);
            self.prefs
                .display_name(&format!("agent:{agent}"))
                .map(str::to_owned)
                .or_else(|| state.cleanup_titles.get(agent).cloned())
                .or_else(|| (title != agent).then_some(title))
        };
        let mut parts: Vec<KeyedView> = Vec::new();
        if state.cleanup_busy {
            parts.push(KeyedView::new(
                "busy",
                controls::secondary(if state.cleanup_plan.is_some() {
                    "Removing the selected items…"
                } else {
                    "Reviewing what can be removed…"
                }),
            ));
        }
        if let Some(error) = &state.cleanup_error {
            parts.push(KeyedView::new(
                format!("error:{error}"),
                controls::body(error.clone())
                    .foreground(ThemeBrush::SystemCritical)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ));
        }
        if state.cleanup_plan.is_some() || state.cleanup_result.is_some() {
            let usage = state
                .cleanup_disk_usage
                .as_ref()
                .map(crate::worktrees::disk_usage_line)
                .unwrap_or_else(|| {
                    "Disk totals unavailable; review cleanup again to refresh.".to_owned()
                });
            parts.push(KeyedView::new(
                "disk-usage",
                controls::secondary(usage)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            ));
        }
        if let Some((plan, reply)) = &state.cleanup_result {
            let lines = crate::worktrees::result_lines(plan, reply, &title);
            let rows: Vec<KeyedView> = lines
                .iter()
                .enumerate()
                .map(|(index, line)| {
                    let text = if index == 0 {
                        controls::text(line.clone(), tokens::text::HEADER)
                    } else {
                        controls::body(line.clone())
                    };
                    KeyedView::new(
                        format!("result:{index}"),
                        text.text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(true),
                    )
                })
                .collect();
            parts.push(KeyedView::new(
                "result",
                controls::card().automation_name("Cleanup result").content(
                    StackPanel::new()
                        .spacing(tokens::spacing::XS)
                        .keyed_children(rows),
                ),
            ));
        }
        if let Some(plan) = &state.cleanup_plan {
            let groups = crate::worktrees::cleanup_groups(plan);
            if groups.is_empty() {
                parts.push(KeyedView::new(
                    "empty",
                    controls::secondary("No cleanup items on this page.")
                        .text_wrapping(TextWrapping::Wrap),
                ));
            }
            for view in &groups {
                let group = view.group;
                let checked = !state.cleanup_unchecked.contains(&group);
                let kept = view.items.len() - view.removable;
                let size = if crate::worktrees::sized(group) {
                    format!(" · {}", crate::worktrees::size_words(view.bytes))
                } else {
                    String::new()
                };
                let summary = match (view.removable, kept) {
                    (0, kept) => format!("{kept} kept"),
                    (removable, 0) => format!("{removable} to remove{size}"),
                    (removable, kept) => format!("{removable} to remove{size} · {kept} kept"),
                };
                let header: View = if view.removable > 0 {
                    CheckBox::new()
                        .is_checked(checked)
                        .is_enabled(!state.cleanup_busy)
                        .automation_name(format!("Remove {}: {summary}", group.title()))
                        .on_is_checked_changed(crate::views::check_changes(context.callback(
                            move |on: bool| {
                                Message::Settings(SettingsMsg::CleanupGroupToggled(group, on))
                            },
                        )))
                        .content(controls::text(
                            format!("{} · {summary}", group.title()),
                            tokens::text::HEADER,
                        ))
                        .into()
                } else {
                    controls::text(
                        format!("{} · {summary}", group.title()),
                        tokens::text::HEADER,
                    )
                    .into()
                };
                let mut rows: Vec<KeyedView> =
                    vec![KeyedView::new(format!("header:{checked}"), header)];
                let expanded = state.cleanup_open.contains(&group);
                rows.push(KeyedView::new(
                    format!("expand:{expanded}"),
                    controls::sub_row(false)
                        .automation_name(format!(
                            "{} items ({})",
                            group.title(),
                            if expanded { "shown" } else { "hidden" }
                        ))
                        .on_click(
                            context.message(Message::Settings(SettingsMsg::CleanupExpanded(group))),
                        )
                        .content(controls::icon_label(
                            if expanded {
                                controls::glyph::CHEVRON_DOWN
                            } else {
                                controls::glyph::CHEVRON_RIGHT
                            },
                            "Review items",
                        )),
                ));
                let mut details = Vec::new();
                // File, folder and checkout items open in Explorer.
                let item_row = |item: &eliot_core::api::CleanupCandidate,
                                context: &mut ViewContext<Self>| {
                    let (name, line) =
                        crate::worktrees::item_line(item, title(&item.artifact.agent).as_deref());
                    let on_disk = matches!(
                        item.artifact.kind,
                        ArtifactKind::Build
                            | ArtifactKind::DataDir
                            | ArtifactKind::Temp
                            | ArtifactKind::Worktree
                            | ArtifactKind::ProjectRegistration
                    );
                    let lines = StackPanel::new().children((
                        controls::body(name.clone()).text_trimming(TextTrimming::CharacterEllipsis),
                        controls::secondary(line)
                            .text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(!on_disk),
                    ));
                    let path = &item.artifact.path;
                    let view = if on_disk {
                        let reveal_item = format!("cleanup:{}", item.artifact.id);
                        StackPanel::new().keyed_children(crate::views::slots([
                            Some(super::super::reveal::path_item(
                                &name,
                                lines.into(),
                                self.reveal_item_message(context, &reveal_item, path),
                            )),
                            self.reveal_note(&reveal_item),
                        ]))
                    } else {
                        lines
                    };
                    KeyedView::new(item.artifact.id.clone(), view)
                };
                // What Remove takes comes first; the kept items fold under
                // one "N kept" row.
                let (removable, kept_items) = view.split();
                details.extend(removable.iter().map(|item| item_row(item, context)));
                if !kept_items.is_empty() {
                    let open = state.cleanup_kept_open.contains(&group);
                    let label = format!("{} kept", kept_items.len());
                    details.push(KeyedView::new(
                        format!("kept:{open}"),
                        controls::sub_row(false)
                            .automation_name(format!(
                                "{label} in {} ({})",
                                group.title(),
                                if open { "shown" } else { "hidden" }
                            ))
                            .on_click(
                                context.message(Message::Settings(
                                    SettingsMsg::CleanupKeptToggled(group),
                                )),
                            )
                            .content(controls::row_content(
                                Some(if open {
                                    controls::glyph::CHEVRON_DOWN
                                } else {
                                    controls::glyph::CHEVRON_RIGHT
                                }),
                                &label,
                                "",
                                None,
                            )),
                    ));
                    if open {
                        details.extend(kept_items.iter().map(|item| item_row(item, context)));
                    }
                }
                if expanded {
                    rows.push(KeyedView::new(
                        "items",
                        ScrollViewer::new()
                            .max_height(tokens::layout::DIALOG_LIST_MAX_HEIGHT)
                            .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                            .content(
                                StackPanel::new()
                                    .spacing(tokens::spacing::SM)
                                    .keyed_children(details),
                            ),
                    ));
                }
                parts.push(KeyedView::new(
                    format!("group:{group:?}"),
                    controls::card().content(
                        StackPanel::new()
                            .spacing(tokens::spacing::SM)
                            .keyed_children(rows),
                    ),
                ));
            }
            if plan.truncated && plan.continuation.is_none() {
                parts.push(KeyedView::new(
                    "truncated",
                    controls::secondary(
                        "The review stopped at its bound; review again after this cleanup for the rest.",
                    )
                    .text_wrapping(TextWrapping::Wrap),
                ));
            }
        }
        let page = state
            .cleanup_plan
            .as_ref()
            .or_else(|| state.cleanup_result.as_ref().map(|(plan, _)| plan));
        if let Some(continuation) = page.and_then(|plan| plan.continuation.clone()) {
            parts.push(KeyedView::new(
                "next-page",
                controls::outlined()
                    .automation_name("Next cleanup page")
                    .is_enabled(!state.cleanup_busy)
                    .on_click(
                        context.message(Message::Settings(SettingsMsg::CleanupPlan(Some(
                            continuation,
                        )))),
                    )
                    .content(controls::label("Next page")),
            ));
        }
        StackPanel::new()
            .spacing(tokens::spacing::SM)
            .keyed_children(parts)
            .into()
    }

    /// Kept outside the page scroller: selection and Remove never scroll away.
    pub(super) fn cleanup_actions(&self, context: &mut ViewContext<Self>) -> Option<View> {
        let state = &self.settings;
        let Some(plan) = &state.cleanup_plan else {
            return None;
        };
        let groups = crate::worktrees::cleanup_groups(plan);
        let mut parts = Vec::new();
        let selected = crate::worktrees::selected_artifacts(plan, &state.cleanup_unchecked);
        let bytes: u64 = plan
            .items
            .iter()
            .filter(|item| selected.contains(&item.artifact.id))
            .filter_map(|item| item.bytes)
            .sum();
        let what = format!(
            "{}, {}",
            eliot_core::count_words(selected.len() as u64, "item", "items"),
            crate::worktrees::size_words(bytes)
        );
        // Every group with something safe to remove, opt-in groups (native
        // session archives, project registrations) included; each item
        // keeps its own keep or remove reason.
        let all_safe = groups
            .iter()
            .filter(|view| view.removable > 0)
            .all(|view| !state.cleanup_unchecked.contains(&view.group));
        if groups.iter().any(|view| view.removable > 0) {
            parts.push(KeyedView::new(
                format!("all-safe:{all_safe}"),
                controls::outlined()
                    .automation_name("Select all safe items")
                    .is_enabled(!state.cleanup_busy && !all_safe)
                    .on_click(context.message(Message::Settings(SettingsMsg::CleanupSelectAll)))
                    .content(controls::label("Select all safe")),
            ));
        }
        if !groups.is_empty() {
            parts.push(KeyedView::new(
                "apply",
                controls::destructive()
                    .automation_name(format!("Remove selected: {what}"))
                    .is_enabled(!state.cleanup_busy && !selected.is_empty())
                    .on_click(context.message(Message::Settings(SettingsMsg::CleanupApply)))
                    .content(controls::label(&if selected.is_empty() {
                        "Nothing selected".to_owned()
                    } else {
                        format!("Remove {what}")
                    })),
            ));
        }
        Some(
            controls::card()
                .min_width(tokens::layout::MIN_COLUMN)
                .max_width(tokens::page::SETTINGS_WIDTH)
                .margin(Thickness::uniform(tokens::spacing::SM))
                .content(
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .horizontal_alignment(HorizontalAlignment::Right)
                        .spacing(tokens::spacing::SM)
                        .keyed_children(parts),
                )
                .into(),
        )
    }

    pub(in crate::ui) fn cleanup_confirmation(
        &self,
        context: &mut ViewContext<Self>,
    ) -> Option<ContentDialog> {
        let Some(plan) = &self.settings.cleanup_plan else {
            return None;
        };
        let selected = crate::worktrees::selected_artifacts(plan, &self.settings.cleanup_unchecked);
        Some(super::super::dialog_ui::surface(context, self.dialog.epoch, "Remove selected items?".to_owned(),
            controls::body(format!("Remove {}? This cannot be undone. Kept items stay; Swarm checks each item again before removing it.",
                eliot_core::count_words(selected.len() as u64, "item", "items"))).text_wrapping(TextWrapping::Wrap).into(),
            super::super::dialog_ui::actions(
                controls::text_button().element_ref(&self.dialog.close).automation_name("Cancel cleanup")
                    .on_click(context.message(Message::Settings(SettingsMsg::CleanupConfirmed(false)))).content(controls::label("Cancel")).into(),
                View::from(controls::destructive().element_ref(&self.dialog.primary).is_enabled(!self.settings.cleanup_busy && !selected.is_empty()).automation_name("Confirm Remove")
                    .on_click(context.message(Message::Settings(SettingsMsg::CleanupConfirmed(true)))).content(controls::label("Remove")))), tokens::layout::DIALOG_WIDTH))
    }

    /// Worktrees & cleanup (Codex's Worktrees page as the pattern): where
    /// Swarm creates worktrees, Clean up finished work (`cleanup.plan`
    /// reviewed by kind, then `cleanup.apply` of the checked groups after
    /// one confirmation), and the worktrees it manages, by project (the
    /// daemon's `workspace.list`, every page).
    pub(super) fn worktrees_page(&self, context: &mut ViewContext<Self>) -> View {
        let root = self
            .data_dir
            .as_ref()
            .map(|dir| dir.join("worktrees").to_string_lossy().into_owned())
            .unwrap_or_default();
        let settings = controls::settings_card(vec![
            KeyedView::new("root", self.path_row(context, "Worktree root", "", &root)),
            KeyedView::new(
                "cleanup",
                controls::setting_row(
                    "Clean up finished work",
                    "Review what Swarm can remove for finished tasks, by kind, with sizes and reasons. Nothing is removed until you confirm.",
                    View::from(
                        controls::outlined()
                            .automation_name("Review cleanup")
                            .is_enabled(!self.settings.cleanup_busy)
                            .on_click(
                                context.message(Message::Settings(SettingsMsg::CleanupPlan(None))),
                            )
                            .content(controls::label(if self.settings.cleanup_plan.is_some() {
                                "Review again"
                            } else {
                                "Review…"
                            })),
                    ),
                ),
            ),
        ]);
        let cleanup = self.cleanup_review(context);
        let mut groups: Vec<KeyedView> = Vec::new();
        let empty: &[eliot_core::api::ManagedWorkspaceView] = &[];
        let (rows, total, complete) = match &self.settings.worktrees {
            Some(Ok((rows, total, complete))) => (&rows[..], *total, *complete),
            Some(Err(error)) => {
                groups.push(KeyedView::new(
                    format!("error:{error}"),
                    controls::body(format!(
                        "Managed worktrees could not be read: {}",
                        crate::state::plain_error(error)
                    ))
                    .foreground(ThemeBrush::SystemCritical)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
                ));
                (empty, 0, true)
            }
            None => (empty, 0, true),
        };
        let managed = crate::worktrees::grouped(rows, &self.checkouts());
        let heading = match &self.settings.worktrees {
            None => "Reading…".to_owned(),
            Some(Err(_)) => String::new(),
            Some(Ok(_)) if !complete => format!(
                "{} of {total} shown (the read stopped at its page bound)",
                rows.len()
            ),
            Some(Ok(_)) => {
                eliot_core::count_words(total as u64, "managed worktree", "managed worktrees")
            }
        };
        if matches!(self.settings.worktrees, Some(Ok(_))) && managed.is_empty() {
            groups.push(KeyedView::new(
                "none",
                controls::secondary(
                    "No managed worktrees yet. A task started with swarm launch --worktree BRANCH gets one here.",
                )
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        for (project, worktrees) in managed {
            let rows: Vec<KeyedView> = worktrees
                .iter()
                .map(|worktree| {
                    let name = std::path::Path::new(&worktree.path)
                        .file_name()
                        .map_or_else(
                            || worktree.path.clone(),
                            |name| name.to_string_lossy().into_owned(),
                        );
                    let detail = format!(
                        "{} · {} · {} · {}",
                        worktree.branch, worktree.agent, worktree.phase, worktree.path
                    );
                    KeyedView::new(
                        format!("{}:{detail}", worktree.path),
                        self.path_row(context, &name, &detail, &worktree.path),
                    )
                })
                .collect();
            groups.push(KeyedView::new(
                format!("project:{project}"),
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    controls::section_heading(
                        &project,
                        &eliot_core::count_words(worktrees.len() as u64, "worktree", "worktrees"),
                        None,
                    ),
                    controls::settings_card(rows),
                )),
            ));
        }
        StackPanel::new()
            .spacing(tokens::spacing::MD)
            .children((
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    controls::card_title("Creation and cleanup"),
                    settings,
                    cleanup,
                )),
                controls::section_heading(
                    "Managed worktrees",
                    &heading,
                    Some(
                        controls::text_button()
                            .is_enabled(!self.settings.worktrees_loading)
                            .automation_name("Read managed worktrees again")
                            .on_click(
                                context.message(Message::Settings(SettingsMsg::WorktreesRefresh)),
                            )
                            .content(controls::label("Refresh"))
                            .into(),
                    ),
                ),
                StackPanel::new()
                    .spacing(tokens::spacing::MD)
                    .keyed_children(groups),
            ))
            .into()
    }
}
