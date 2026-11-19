use super::*;
use crate::ui::bulk_ui::{BulkMsg, BulkOp, Pick};

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
}
