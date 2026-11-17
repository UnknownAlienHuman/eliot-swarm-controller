use super::*;

impl DesktopApp {
    pub(super) fn automation_section(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        StackPanel::new().spacing(tokens::spacing::MD).children((
            self.assignments_view(context, palette),
            config_view::automation_screen(
                &self.automation.rules,
                context.message(Message::Automation(
                    AutomationMsg::AutomationRefreshRequested,
                )),
                palette,
            ),
            m3::card_title("Scripts"),
            self.scripts_view(context, palette),
        ))
    }

    /// "Keep N busy from folder X": the `[automation.assignments.*]` entries.
    fn assignments_view(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let automation = &self.settings.automation;
        let roles = model::routing_roles(&self.settings.routing);
        let mut groups: Vec<KeyedView> = vec![KeyedView::new(
            "title",
            m3::section_heading(
                "Assignments",
                "Keep a number of Agents busy with the briefs (.md files) of a folder, in filename order. Each brief starts one Agent through routing; a finished Agent is retired and the next brief starts.",
                None,
            ),
        )];
        for key in model::assignment_keys(automation) {
            let busy_count = model::assignment_busy(automation, &key);
            let role = model::assignment_role(automation, &key);
            let mut role_items = roles.clone();
            if !role_items.contains(&role) {
                role_items.push(role.clone());
            }
            let role_index = role_items.iter().position(|item| *item == role);
            let text_field = |field: AssignmentField, label: &str, hint: &str| {
                let callback_key = key.clone();
                m3::form_row(
                    label,
                    m3::field()
                        .text(
                            nav::display_path(&model::assignment_text(automation, &key, field))
                                .to_owned(),
                        )
                        .placeholder_text(hint)
                        .automation_name(format!("{label} of {key}"))
                        .on_text_changed(context.callback(move |text| {
                            Message::Settings(SettingsMsg::AssignmentText(
                                callback_key.clone(),
                                field,
                                text,
                            ))
                        }))
                        .into(),
                    None,
                )
            };
            let folder = text_field(
                AssignmentField::Folder,
                "Briefs folder",
                "C:\\briefs\\backlog",
            );
            let workspace = text_field(
                AssignmentField::Workspace,
                "Work in",
                "A project's checkout folder",
            );
            let interval = text_field(AssignmentField::Interval, "Check every", "30s");
            let (busy_key, role_key) = (key.clone(), key.clone());
            let rows = vec![
                KeyedView::new(
                    "title",
                    m3::section_title(format!(
                        "{key} · keep {} busy",
                        busy_count.map_or("?".to_owned(), |count| count.to_string())
                    ))
                    .height(tokens::height::FORM_ROW)
                    .vertical_alignment(VerticalAlignment::Center),
                ),
                KeyedView::new(
                    "busy",
                    m3::form_row(
                        "Agents kept busy",
                        NumberBox::new()
                            .width(tokens::layout::NUMBER_WIDTH)
                            .horizontal_alignment(HorizontalAlignment::Left)
                            .minimum(1.0)
                            .maximum(1000.0)
                            .value(busy_count.map(|count| count as f64))
                            .automation_name(format!("Agents kept busy by {key}"))
                            .on_value_changed(context.callback(move |value| {
                                Message::Settings(SettingsMsg::AssignmentBusy(
                                    busy_key.clone(),
                                    value,
                                ))
                            }))
                            .into(),
                        None,
                    ),
                ),
                KeyedView::new("folder", folder),
                KeyedView::new("workspace", workspace),
                KeyedView::new(
                    "role",
                    m3::form_row(
                        "Routing role",
                        Grid::new().keyed_children(vec![KeyedView::new(
                            format!("{}:{role}", role_items.join("\u{1f}")),
                            m3::enum_width(ComboBox::new())
                                .items_source(role_items)
                                .selected_index(role_index)
                                .automation_name(format!("Routing role of {key}"))
                                .on_selection_changed(context.callback(move |index| {
                                    Message::Settings(SettingsMsg::AssignmentRole(
                                        role_key.clone(),
                                        index,
                                    ))
                                })),
                        )]),
                        None,
                    ),
                ),
                KeyedView::new("interval", interval),
                KeyedView::new(
                    "remove",
                    m3::destructive(palette)
                        .margin(Thickness::new(
                            tokens::layout::LABEL_WIDTH + tokens::spacing::SM,
                            tokens::spacing::XS,
                            0.0,
                            0.0,
                        ))
                        .automation_name(format!("Remove assignment {key}"))
                        .on_click(context.message(Message::Settings(
                            SettingsMsg::AssignmentRemoved(key.clone()),
                        )))
                        .content(m3::label("Remove assignment")),
                ),
            ];
            groups.push(KeyedView::new(
                format!("assignment:{key}"),
                m3::group(palette, rows),
            ));
        }
        groups.push(KeyedView::new(
            "add",
            m3::group(
                palette,
                vec![KeyedView::new(
                    "add",
                    m3::form_row(
                        "New assignment",
                        m3::field()
                            .text(self.settings.new_assignment.clone())
                            .placeholder_text("Name, for example backlog")
                            .automation_name("New assignment name")
                            .on_text_changed(context.callback(|text| {
                                Message::Settings(SettingsMsg::NewAssignment(text))
                            }))
                            .into(),
                        Some(
                            m3::outlined(palette)
                                .is_enabled(!self.settings.new_assignment.trim().is_empty())
                                .automation_name("Add assignment")
                                .on_click(
                                    context
                                        .message(Message::Settings(SettingsMsg::AssignmentAdded)),
                                )
                                .content(m3::label("Add")),
                        ),
                    ),
                )],
            ),
        ));
        StackPanel::new()
            .spacing(tokens::spacing::MD)
            .keyed_children(groups)
    }
}
