use super::*;
use eliot_core::api::ArtifactKind;

impl DesktopApp {
    /// The cleanup review under its row: the plan by kind (a checkbox per
    /// group that has something to remove, each item with its size and its
    /// keep or remove reason), Remove with one inline confirmation, then the
    /// result: removed, kept with why, and unknown.
    fn cleanup_review(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
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
                m3::secondary(if state.cleanup_plan.is_some() {
                    "Removing the selected items…"
                } else {
                    "Reviewing what can be removed…"
                }),
            ));
        }
        if let Some(error) = &state.cleanup_error {
            parts.push(KeyedView::new(
                format!("error:{error}"),
                m3::body(error.clone())
                    .foreground(palette.error())
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
                        m3::text(line.clone(), tokens::text::SECTION_TITLE)
                    } else {
                        m3::body(line.clone())
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
                m3::card_high(palette)
                    .automation_name("Cleanup result")
                    .content(
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
                    m3::secondary("Nothing to clean up: Swarm holds no finished work.")
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
                        .on_is_checked_changed(context.callback(move |on: bool| {
                            Message::Settings(SettingsMsg::CleanupGroupToggled(group, on))
                        }))
                        .content(m3::text(
                            format!("{} · {summary}", group.title()),
                            tokens::text::SECTION_TITLE,
                        ))
                } else {
                    m3::text(
                        format!("{} · {summary}", group.title()),
                        tokens::text::SECTION_TITLE,
                    )
                    .into()
                };
                let mut rows: Vec<KeyedView> =
                    vec![KeyedView::new(format!("header:{checked}"), header)];
                let expanded = state.cleanup_open.contains(&group);
                rows.push(KeyedView::new(
                    format!("expand:{expanded}"),
                    m3::sub_row(palette, false)
                        .automation_name(format!(
                            "{} items ({})",
                            group.title(),
                            if expanded { "shown" } else { "hidden" }
                        ))
                        .on_click(
                            context.message(Message::Settings(SettingsMsg::CleanupExpanded(group))),
                        )
                        .content(m3::icon_label(
                            if expanded {
                                m3::glyph::CHEVRON_DOWN
                            } else {
                                m3::glyph::CHEVRON_RIGHT
                            },
                            "Review items",
                        )),
                ));
                let mut details = Vec::new();
                // A folder or file item opens in Explorer; a branch,
                // session or registration has no path to show.
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
                    );
                    let lines = StackPanel::new().children((
                        m3::body(name.clone()).text_trimming(TextTrimming::CharacterEllipsis),
                        m3::secondary(line)
                            .text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(!on_disk),
                    ));
                    let path = &item.artifact.path;
                    let view = if on_disk {
                        StackPanel::new().children((
                            super::super::reveal::path_item(
                                palette,
                                &name,
                                lines,
                                self.reveal_message(context, path),
                            ),
                            self.reveal_note(path, palette),
                        ))
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
                        m3::sub_row(palette, false)
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
                            .content(m3::row_content(
                                Some(if open {
                                    m3::glyph::CHEVRON_DOWN
                                } else {
                                    m3::glyph::CHEVRON_RIGHT
                                }),
                                &label,
                                "",
                                View::empty(),
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
                            .max_height(240.0)
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
                    m3::card_high(palette).content(
                        StackPanel::new()
                            .spacing(tokens::spacing::SM)
                            .keyed_children(rows),
                    ),
                ));
            }
            if plan.truncated {
                parts.push(KeyedView::new(
                    "truncated",
                    m3::secondary(
                        "The review stopped at its bound; review again after this cleanup for the rest.",
                    )
                    .text_wrapping(TextWrapping::Wrap),
                ));
            }
        }
        StackPanel::new()
            .spacing(tokens::spacing::SM)
            .keyed_children(parts)
    }

    /// Kept outside the page scroller: selection and Remove never scroll away.
    pub(super) fn cleanup_actions(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let state = &self.settings;
        let Some(plan) = &state.cleanup_plan else {
            return View::empty();
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
                m3::outlined(palette)
                    .automation_name("Select all safe items")
                    .is_enabled(!state.cleanup_busy && !all_safe)
                    .on_click(context.message(Message::Settings(SettingsMsg::CleanupSelectAll)))
                    .content(m3::label("Select all safe")),
            ));
        }
        if !groups.is_empty() {
            parts.push(KeyedView::new(
                "apply",
                m3::destructive(palette)
                    .automation_name(format!("Remove selected: {what}"))
                    .is_enabled(!state.cleanup_busy && !selected.is_empty())
                    .on_click(context.message(Message::Settings(SettingsMsg::CleanupApply)))
                    .content(m3::label(&if selected.is_empty() {
                        "Nothing selected".to_owned()
                    } else {
                        format!("Remove {what}")
                    })),
            ));
        }
        m3::card_high(palette)
            .width(self.settings_column_width())
            .horizontal_alignment(HorizontalAlignment::Center)
            .margin(Thickness::uniform(tokens::spacing::SM))
            .content(
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .horizontal_alignment(HorizontalAlignment::Right)
                    .spacing(tokens::spacing::SM)
                    .keyed_children(parts),
            )
    }

    pub(in crate::ui) fn cleanup_confirmation(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let Some(plan) = &self.settings.cleanup_plan else {
            return View::empty();
        };
        let selected = crate::worktrees::selected_artifacts(plan, &self.settings.cleanup_unchecked);
        super::super::dialog_ui::surface(context, self.dialog.epoch, palette, "Remove selected items?".to_owned(),
            m3::body(format!("Remove {}? This cannot be undone. Kept items stay; Swarm checks each item again before removing it.",
                eliot_core::count_words(selected.len() as u64, "item", "items"))).text_wrapping(TextWrapping::Wrap).into(),
            super::super::dialog_ui::actions(
                m3::text_button(palette).element_ref(&self.dialog.close).automation_name("Cancel cleanup")
                    .on_click(context.message(Message::Settings(SettingsMsg::CleanupConfirmed(false)))).content(m3::label("Cancel")),
                m3::destructive(palette).element_ref(&self.dialog.primary).is_enabled(!self.settings.cleanup_busy && !selected.is_empty()).automation_name("Confirm Remove")
                    .on_click(context.message(Message::Settings(SettingsMsg::CleanupConfirmed(true)))).content(m3::label("Remove"))), 420.0)
    }

    /// Worktrees & cleanup (Codex's Worktrees page as the pattern): where
    /// Swarm creates worktrees, Clean up finished work (`cleanup.plan`
    /// reviewed by kind, then `cleanup.apply` of the checked groups after
    /// one confirmation), and the worktrees it manages, by project (the
    /// daemon's `workspace.list`, every page).
    pub(super) fn worktrees_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let root = self
            .data_dir
            .as_ref()
            .map(|dir| dir.join("worktrees").to_string_lossy().into_owned())
            .unwrap_or_default();
        let settings = m3::settings_card(
            palette,
            vec![
                KeyedView::new(
                    "root",
                    self.path_row(context, palette, "Worktree root", "", &root),
                ),
                KeyedView::new(
                    "cleanup",
                    m3::setting_row(
                        "Clean up finished work",
                        "Review what Swarm can remove for finished tasks, by kind, with sizes and reasons. Nothing is removed until you confirm.",
                        m3::outlined(palette)
                            .automation_name("Review cleanup")
                            .is_enabled(!self.settings.cleanup_busy)
                            .on_click(context.message(Message::Settings(SettingsMsg::CleanupPlan)))
                            .content(m3::label(if self.settings.cleanup_plan.is_some() {
                                "Review again"
                            } else {
                                "Review…"
                            })),
                    ),
                ),
            ],
        );
        let cleanup = self.cleanup_review(context, palette);
        let mut groups: Vec<KeyedView> = Vec::new();
        let empty: &[eliot_core::api::ManagedWorkspaceView] = &[];
        let (rows, total, complete) = match &self.settings.worktrees {
            Some(Ok((rows, total, complete))) => (&rows[..], *total, *complete),
            Some(Err(error)) => {
                groups.push(KeyedView::new(
                    format!("error:{error}"),
                    m3::body(format!(
                        "Managed worktrees could not be read: {}",
                        crate::state::plain_error(error)
                    ))
                    .foreground(palette.error())
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
                m3::secondary(
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
                        self.path_row(context, palette, &name, &detail, &worktree.path),
                    )
                })
                .collect();
            groups.push(KeyedView::new(
                format!("project:{project}"),
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    m3::section_heading(
                        &project,
                        &eliot_core::count_words(worktrees.len() as u64, "worktree", "worktrees"),
                        None,
                    ),
                    m3::settings_card(palette, rows),
                )),
            ));
        }
        StackPanel::new().spacing(tokens::spacing::MD).children((
            StackPanel::new().spacing(tokens::spacing::SM).children((
                m3::card_title("Creation and cleanup"),
                settings,
                cleanup,
            )),
            m3::section_heading(
                "Managed worktrees",
                &heading,
                Some(
                    m3::text_button(palette)
                        .is_enabled(!self.settings.worktrees_loading)
                        .automation_name("Read managed worktrees again")
                        .on_click(context.message(Message::Settings(SettingsMsg::WorktreesRefresh)))
                        .content(m3::label("Refresh")),
                ),
            ),
            StackPanel::new()
                .spacing(tokens::spacing::MD)
                .keyed_children(groups),
        ))
    }
}
