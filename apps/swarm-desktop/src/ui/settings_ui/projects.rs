use super::*;
use crate::ui::bulk_ui::{BulkMsg, BulkOp, Pick};

/// A policy field's message: every control reports through one mapper.
fn edit(edit: PolicyEdit) -> Message {
    Message::Settings(SettingsMsg::PolicyEdited(edit))
}

/// One policy list as the tag editor shows tags: a field per entry with its
/// Remove button, then a blank field that adds one. Nothing is split, so
/// an argument with spaces stays one argument (and a multi-line box, whose
/// first frame drops every line after the first, is not needed).
fn policy_list(
    fields: &model::PolicyFields,
    list: model::PolicyList,
    (entry, add): (&str, &str),
    name: &str,
    context: &mut ViewContext<DesktopApp>,
    palette: Palette,
) -> View {
    let entries = fields.list(list);
    let blank = String::new();
    let rows: Vec<KeyedView> = entries
        .iter()
        .chain(std::iter::once(&blank))
        .enumerate()
        .map(|(index, text)| {
            let new = index == entries.len();
            let field = m3::field()
                .text(text.clone())
                .placeholder_text(if new { add } else { entry })
                .automation_name(if new {
                    format!("{name}: {add}")
                } else {
                    format!("{name}: {entry} {}", index + 1)
                })
                .on_text_changed(
                    context.callback(move |text| edit(PolicyEdit::Entry(list, index, text))),
                );
            let remove: View = if new {
                View::empty()
            } else {
                m3::icon_button(palette)
                    .grid_column(1)
                    .width(tokens::height::CONTROL)
                    .height(tokens::height::CONTROL)
                    .automation_name(format!("{name}: remove {entry} {}", index + 1))
                    .on_click(context.message(edit(PolicyEdit::Remove(list, index))))
                    .content(m3::icon(m3::glyph::CLOSE))
            };
            KeyedView::new(
                format!("{list:?}:{index}"),
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Pixel(tokens::height::CONTROL)])
                    .column_spacing(tokens::spacing::XS)
                    .children((field, remove)),
            )
        })
        .collect();
    StackPanel::new()
        .spacing(tokens::spacing::XS)
        .margin(Thickness::xy(0.0, tokens::spacing::XS))
        .keyed_children(rows)
}

impl DesktopApp {
    /// Settings › Projects: every registration by its logical name (the
    /// project, and "project › subproject" for a linked worktree, never
    /// its ID), with a check box for the bulk actions above the list
    /// (Select all, Remove selected, Remove unused); a row opens its
    /// editor (folder, rule profiles and actions, Remove project).
    pub(super) fn projects_section(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let drafts = &self.settings.projects;
        let profiles = self.known_profiles();
        let checkouts = self.checkouts();
        let mut ids: Vec<(String, String)> = model::project_ids(drafts)
            .into_iter()
            .map(|id| {
                let path = model::project_path(drafts, &id);
                (self.place_title_in(&path, &checkouts), id)
            })
            .collect();
        // Each repository's subprojects follow it ("app" < "app › x").
        ids.sort();
        let selected: Vec<String> = ids
            .iter()
            .filter(|(_, id)| self.settings.project_selected.contains(id))
            .map(|(_, id)| id.clone())
            .collect();
        let all = !ids.is_empty() && selected.len() == ids.len();
        let mut rows: Vec<KeyedView> = Vec::new();
        for (name, id) in &ids {
            let (name, id) = (name.clone(), id.clone());
            let open = self.settings.project_open.as_deref() == Some(id.as_str());
            let checked = self.settings.project_selected.contains(&id);
            let path = model::project_path(drafts, &id);
            let allowed = model::project_profiles(drafts, &id);
            let missing = self
                .nav
                .projects
                .iter()
                .any(|project| project.id == id && project.missing);
            let summary = if allowed.is_empty() {
                "no profiles allowed".to_owned()
            } else {
                allowed.join(", ")
            };
            let note = if missing {
                format!("Folder missing · {}", nav::display_path(&path))
            } else {
                format!("{} · {summary}", nav::display_path(&path))
            };
            let select_id = id.clone();
            let checkout_item = format!("settings-project:{id}:{path}");
            rows.push(KeyedView::new(
                format!("project:{id}:{open}:{checked}:{name}"),
                StackPanel::new().children((
                    Grid::new()
                        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::XS)
                        .children((
                            CheckBox::new()
                                .is_checked(checked)
                                .min_width(0.0)
                                .vertical_alignment(VerticalAlignment::Center)
                                .margin(Thickness::new(tokens::spacing::SM, 0.0, 0.0, 0.0))
                                .automation_name(format!("Select {name}"))
                                .on_is_checked_changed(context.callback(move |on: bool| {
                                    Message::Settings(SettingsMsg::ProjectSelected(
                                        select_id.clone(),
                                        on,
                                    ))
                                })),
                            m3::row(palette, open)
                                .grid_column(1)
                                .automation_name(format!("Project {name}: {note}"))
                                .on_click(context.message(Message::Settings(
                                    SettingsMsg::ProjectOpened(id.clone()),
                                )))
                                .content(m3::row_content(
                                    Some(if name.contains(" › ") {
                                        m3::glyph::WORKTREE
                                    } else {
                                        m3::glyph::FOLDER
                                    }),
                                    &name,
                                    &note,
                                    if missing {
                                        m3::secondary("missing")
                                            .foreground(palette.attention())
                                            .vertical_alignment(VerticalAlignment::Center)
                                            .into()
                                    } else {
                                        View::empty()
                                    },
                                )),
                            Border::new()
                                .grid_column(2)
                                .vertical_alignment(VerticalAlignment::Center)
                                .content(super::super::reveal::path_button(
                                    palette,
                                    &name,
                                    self.reveal_item_message(context, &checkout_item, &path),
                                )),
                        )),
                    self.reveal_note(&checkout_item, palette),
                )),
            ));
            if !open {
                continue;
            }
            let path_id = id.clone();
            let actions_id = id.clone();
            let editor_item = format!("settings-project-editor:{id}:{path}");
            let checks: Vec<KeyedView> = profiles
                .iter()
                .map(|profile| {
                    let on = allowed.contains(profile);
                    let (check_id, check_profile) = (id.clone(), profile.clone());
                    KeyedView::new(
                        format!("{profile}:{on}"),
                        CheckBox::new()
                            .is_checked(on)
                            .automation_name(format!("Allow {profile} for rules of {name}"))
                            .on_is_checked_changed(context.callback(move |on: bool| {
                                Message::Settings(SettingsMsg::ProjectProfile(
                                    check_id.clone(),
                                    check_profile.clone(),
                                    on,
                                ))
                            }))
                            .content(m3::body(profile.clone())),
                    )
                })
                .collect();
            // The list's 8-DIP padding plus these 8 put the labels at the
            // 16-DIP inset of every other group.
            let editor = StackPanel::new()
                .margin(Thickness::uniform(tokens::spacing::SM))
                .children((
                    m3::form_row(
                        "Checkout folder",
                        m3::field()
                            .text(nav::display_path(&path).to_owned())
                            .automation_name(format!("Checkout folder of {name}"))
                            .on_text_changed(context.callback(move |text| {
                                Message::Settings(SettingsMsg::ProjectPath(path_id.clone(), text))
                            }))
                            .into(),
                        Some(super::super::reveal::path_button(
                            palette,
                            &name,
                            self.reveal_item_message(context, &editor_item, &path),
                        )),
                    ),
                    self.reveal_note(&editor_item, palette),
                    m3::form_row(
                        "Rule profiles",
                        StackPanel::new()
                            .orientation(Orientation::Horizontal)
                            .spacing(tokens::spacing::MD)
                            .keyed_children(checks),
                        None,
                    ),
                    m3::form_row(
                        "Rule actions",
                        m3::field()
                            .text(model::project_actions(drafts, &id))
                            .placeholder_text("For example send, stop, git.fetch")
                            .automation_name(format!("Rule actions of {name}"))
                            .on_text_changed(context.callback(move |text| {
                                Message::Settings(SettingsMsg::ProjectActions(
                                    actions_id.clone(),
                                    text,
                                ))
                            }))
                            .into(),
                        None,
                    ),
                    self.policy_editor(&id, &name, context, palette),
                    m3::destructive(palette)
                        .margin(Thickness::new(
                            tokens::layout::FIELD_INDENT,
                            tokens::spacing::XS,
                            0.0,
                            0.0,
                        ))
                        .automation_name(format!("Remove project {name}"))
                        .on_click(
                            context.message(Message::Settings(SettingsMsg::ProjectRemoved(
                                id.clone(),
                            ))),
                        )
                        .content(m3::label("Remove project")),
                ));
            rows.push(KeyedView::new(format!("editor:{id}"), editor));
        }
        if rows.is_empty() {
            rows.push(KeyedView::new(
                "none",
                m3::secondary(
                    "No projects yet. Opening Swarm in a Git checkout registers it here.",
                )
                .text_wrapping(TextWrapping::Wrap)
                .margin(Thickness::uniform(tokens::spacing::SM)),
            ));
        }
        let idle = !self.settings.busy() && self.bulk.is_none();
        let remove_label = if selected.is_empty() {
            "Remove selected".to_owned()
        } else {
            format!("Remove {} selected…", selected.len())
        };
        // Bulk actions: one confirmation names the count and what stays;
        // a project with a live task is kept, never forced.
        let bulk = StackPanel::new()
            .orientation(Orientation::Horizontal)
            .spacing(tokens::spacing::SM)
            .children((
                m3::text_button(palette)
                    .is_enabled(!ids.is_empty())
                    .automation_name(if all {
                        "Clear the selection"
                    } else {
                        "Select all projects"
                    })
                    .on_click(
                        context.message(Message::Settings(SettingsMsg::ProjectsSelectAll(!all))),
                    )
                    .content(m3::label(if all {
                        "Clear selection"
                    } else {
                        "Select all"
                    })),
                m3::outlined(palette)
                    .is_enabled(idle && !ids.is_empty())
                    .automation_name("Remove unused projects")
                    .on_click(context.message(Message::Bulk(BulkMsg::Start(
                        BulkOp::RemoveProjects,
                        Pick::Unused,
                        "all projects".to_owned(),
                    ))))
                    .content(m3::label("Remove unused…")),
                m3::destructive(palette)
                    .is_enabled(idle && !selected.is_empty())
                    .automation_name(remove_label.clone())
                    .on_click(context.message(Message::Bulk(BulkMsg::Start(
                        BulkOp::RemoveProjects,
                        Pick::Selected(selected.clone()),
                        "the selection".to_owned(),
                    ))))
                    .content(m3::label(&remove_label)),
            ));
        let add = m3::settings_card(
            palette,
            vec![KeyedView::new(
                "browse",
                m3::setting_row(
                    "Checkout folder",
                    "Choose a folder to add and save the project.",
                    m3::filled(palette)
                        .automation_name("Browse to add a project")
                        .is_enabled(!self.settings.busy())
                        .on_click(context.message(Message::Settings(SettingsMsg::ProjectBrowse)))
                        .content(m3::icon_label(m3::glyph::OPEN_FOLDER, "Browse…")),
                ),
            )],
        );
        StackPanel::new().spacing(tokens::spacing::MD).children((
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .children((bulk, m3::list(palette, rows))),
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .children((m3::section_heading("Add a project", "", None), add)),
        ))
    }

    /// The open project's launch policy (`[projects.ID.policy]`): the
    /// check and Goal defaults Swarm applies to every Agent launched here,
    /// saved together with Save on the read revision (Validate asks the
    /// store without saving), the store's message when it refuses, and how
    /// many of the last day's launches took the policy, with the newest
    /// opt-outs and their reasons (`project.list` `policy_usage`).
    fn policy_editor(
        &self,
        id: &str,
        name: &str,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let Some(policy) = self
            .settings
            .policy
            .as_ref()
            .filter(|policy| policy.project == id)
        else {
            return View::empty();
        };
        let fields = &policy.fields;
        let busy = self.settings.busy() || self.prefs_saving;
        let dirty = policy.dirty();
        let saving = policy.saving && busy;
        let number = |text: &str| {
            m3::field()
                .text(text.to_owned())
                .width(tokens::layout::NUMBER_WIDTH)
                .horizontal_alignment(HorizontalAlignment::Left)
        };
        let note: View = match &policy.note {
            Some(note) => {
                let text = m3::secondary(note.text.clone())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .automation_name(format!("Policy status: {}", note.text));
                if note.error {
                    text.foreground(palette.attention()).into()
                } else {
                    text.into()
                }
            }
            None if dirty => m3::secondary("Unsaved changes.").into(),
            None => View::empty(),
        };
        let now = crate::handoff::now_ms();
        let usage = self
            .nav
            .projects
            .iter()
            .find(|project| project.id == id)
            .map(|project| &project.policy_usage);
        let counter = match usage {
            None => "Launch counts are read with the project list.".to_owned(),
            Some(usage) if usage.total == 0 => {
                "No Agents launched in this project in the last 24 h.".to_owned()
            }
            Some(usage) => format!(
                "Applied to {} of {} Agents launched in the last 24 h · {} opted out",
                usage.applied, usage.total, usage.opted_out
            ),
        };
        let opt_outs: Vec<KeyedView> = usage
            .map(|usage| usage.opt_outs.as_slice())
            .unwrap_or_default()
            .iter()
            .map(|opt_out| {
                let line = format!(
                    "{} · {} · {}",
                    self.agent_title(&opt_out.agent),
                    opt_out.reason,
                    crate::nav::short_age(now, opt_out.created_at)
                );
                KeyedView::new(
                    opt_out.agent.clone(),
                    m3::secondary(line.clone())
                        .text_wrapping(TextWrapping::Wrap)
                        .is_text_selection_enabled(true)
                        .automation_name(format!("Opted out: {line}")),
                )
            })
            .collect();
        let field_row = |label: &str, control: View| m3::form_row(label, control, None);
        StackPanel::new()
            .spacing(tokens::spacing::XS)
            .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
            .children((
                m3::section_heading(
                    "Launch policy",
                    "The check and limits Swarm applies to every Agent launched in this project.",
                    None,
                ),
                field_row(
                    "Check command",
                    policy_list(
                        fields,
                        model::PolicyList::Argv,
                        ("Argument", "Add an argument"),
                        &format!("Policy check command of {name}"),
                        context,
                        palette,
                    ),
                ),
                field_row(
                    "Check deadline (s)",
                    number(&fields.deadline_s)
                        .placeholder_text("Default")
                        .automation_name(format!("Policy check deadline of {name}"))
                        .on_text_changed(context.callback(|text| edit(PolicyEdit::Deadline(text))))
                        .into(),
                ),
                field_row(
                    "When",
                    CheckBox::new()
                        .is_checked(fields.at_turn_end)
                        .automation_name(format!("Run the policy check at turn end in {name}"))
                        .on_is_checked_changed(
                            context.callback(|on: bool| edit(PolicyEdit::AtTurnEnd(on))),
                        )
                        .content(m3::body("Run the check when a turn ends")),
                ),
                field_row(
                    "Timeout retry",
                    CheckBox::new()
                        .is_checked(fields.retry_safe)
                        .automation_name(format!(
                            "The policy check of {name} is safe to repeat after a timeout"
                        ))
                        .on_is_checked_changed(
                            context.callback(|on: bool| edit(PolicyEdit::RetrySafe(on))),
                        )
                        .content(m3::body("Safe to repeat after a timeout")),
                ),
                field_row(
                    "Rounds",
                    number(&fields.max_rounds)
                        .automation_name(format!("Policy rounds of {name}"))
                        .on_text_changed(context.callback(|text| edit(PolicyEdit::Rounds(text))))
                        .into(),
                ),
                field_row(
                    "Cost limit",
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .children((
                            number(&fields.cost_amount)
                                .placeholder_text("Amount")
                                .automation_name(format!("Policy cost limit of {name}"))
                                .on_text_changed(
                                    context.callback(|text| edit(PolicyEdit::CostAmount(text))),
                                ),
                            number(&fields.cost_unit)
                                .placeholder_text("Unit")
                                .automation_name(format!("Policy cost unit of {name}"))
                                .on_text_changed(
                                    context.callback(|text| edit(PolicyEdit::CostUnit(text))),
                                ),
                        )),
                ),
                field_row(
                    "Scope",
                    policy_list(
                        fields,
                        model::PolicyList::Scope,
                        ("Glob", "Add a project-relative glob"),
                        &format!("Policy scope of {name}"),
                        context,
                        palette,
                    ),
                ),
                field_row(
                    "Check inputs",
                    policy_list(
                        fields,
                        model::PolicyList::CheckInputs,
                        ("Glob", "Add a protected glob"),
                        &format!("Policy check inputs of {name}"),
                        context,
                        palette,
                    ),
                ),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::SM)
                    .margin(Thickness::new(
                        tokens::layout::FIELD_INDENT,
                        tokens::spacing::XS,
                        0.0,
                        0.0,
                    ))
                    .children((
                        m3::filled(palette)
                            .is_enabled(dirty && !busy)
                            .automation_name(format!("Save the launch policy of {name}"))
                            .on_click(
                                context.message(Message::Settings(SettingsMsg::PolicySave(false))),
                            )
                            .content(m3::label(if saving { "Saving…" } else { "Save policy" })),
                        m3::outlined(palette)
                            .is_enabled(!busy)
                            .automation_name(format!("Validate the launch policy of {name}"))
                            .on_click(
                                context.message(Message::Settings(SettingsMsg::PolicySave(true))),
                            )
                            .content(m3::label("Validate")),
                        m3::text_button(palette)
                            .is_enabled(dirty && !busy)
                            .automation_name(format!("Discard policy changes of {name}"))
                            .on_click(
                                context.message(Message::Settings(SettingsMsg::PolicyDiscard)),
                            )
                            .content(m3::label("Discard")),
                    )),
                Border::new()
                    .margin(Thickness::new(tokens::layout::FIELD_INDENT, 0.0, 0.0, 0.0))
                    .content(note),
                Border::new()
                    .margin(Thickness::new(
                        tokens::layout::FIELD_INDENT,
                        tokens::spacing::SM,
                        0.0,
                        0.0,
                    ))
                    .content(
                        StackPanel::new().spacing(tokens::spacing::XS).children((
                            m3::body(counter.clone())
                                .text_wrapping(TextWrapping::Wrap)
                                .automation_name(format!("Policy use: {counter}")),
                            StackPanel::new()
                                .spacing(tokens::spacing::XS)
                                .keyed_children(opt_outs),
                        )),
                    ),
            ))
    }
}
