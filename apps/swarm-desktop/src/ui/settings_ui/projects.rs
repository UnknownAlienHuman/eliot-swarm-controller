use super::*;

impl DesktopApp {
    pub(super) fn projects_section(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let drafts = &self.settings.projects;
        let profiles = self.known_profiles();
        let mut rows: Vec<KeyedView> = Vec::new();
        for id in model::project_ids(drafts) {
            let open = self.settings.project_open.as_deref() == Some(id.as_str());
            let path = model::project_path(drafts, &id);
            let allowed = model::project_profiles(drafts, &id);
            let summary = if allowed.is_empty() {
                "no profiles allowed".to_owned()
            } else {
                allowed.join(", ")
            };
            let name = nav::project_display(&id, &path);
            rows.push(KeyedView::new(
                format!("project:{id}:{open}"),
                m3::row(palette, open)
                    .automation_name(format!("Project {name}: {path}"))
                    .on_click(
                        context.message(Message::Settings(SettingsMsg::ProjectOpened(id.clone()))),
                    )
                    .content(m3::row_content(
                        Some(m3::glyph::FOLDER),
                        &name,
                        &format!("{} · {summary}", nav::display_path(&path)),
                        View::empty(),
                    )),
            ));
            if !open {
                continue;
            }
            let path_id = id.clone();
            let actions_id = id.clone();
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
                            .content(TextBlock::new().text(profile.clone())),
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
                        None,
                    ),
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
                            tokens::layout::LABEL_WIDTH + tokens::spacing::SM,
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
        let add = m3::group(
            palette,
            vec![
                KeyedView::new(
                    "id",
                    m3::form_row(
                        "Project id",
                        m3::field()
                            .text(self.settings.new_project_id.clone())
                            .placeholder_text("Letters, digits, - and _")
                            .automation_name("New project id")
                            .on_text_changed(context.callback(|text| {
                                Message::Settings(SettingsMsg::NewProjectId(text))
                            }))
                            .into(),
                        None,
                    ),
                ),
                KeyedView::new(
                    "path",
                    m3::form_row(
                        "Checkout folder",
                        m3::field()
                            .text(self.settings.new_project_path.clone())
                            .placeholder_text("C:\\path\\to\\checkout")
                            .automation_name("New project folder")
                            .on_text_changed(context.callback(|text| {
                                Message::Settings(SettingsMsg::NewProjectPath(text))
                            }))
                            .into(),
                        Some(
                            m3::outlined(palette)
                                .is_enabled(!self.settings.new_project_id.trim().is_empty())
                                .automation_name("Add project")
                                .on_click(
                                    context.message(Message::Settings(SettingsMsg::ProjectAdded)),
                                )
                                .content(m3::label("Add")),
                        ),
                    ),
                ),
            ],
        );
        StackPanel::new().spacing(tokens::spacing::MD).children((
            m3::list(palette, rows),
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .children((m3::section_heading("Add a project", "", None), add)),
        ))
    }
}
