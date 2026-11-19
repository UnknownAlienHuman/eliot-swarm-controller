use super::*;

impl DesktopApp {
    pub(super) fn routing_section(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let mut groups: Vec<KeyedView> = vec![KeyedView::new(
            "intro",
            m3::secondary(
                "When a task starts on Auto, Swarm tries the role's profiles in order and runs the first one that is installed, not on hold, not out of quota and within capacity. The choice and the skipped profiles are recorded on the task.",
            )
            .text_wrapping(TextWrapping::Wrap),
        )];
        // What Auto picks right now, from the saved policy.
        if let Some(Ok(decision)) = &self.launch.preview {
            let (headline, skipped) = model::decision_lines(decision);
            let now = std::iter::once(format!(
                "Right now Auto {}.",
                headline.replacen("Routing picks", "picks", 1)
            ))
            .chain(skipped)
            .collect::<Vec<_>>()
            .join("\n");
            groups.push(KeyedView::new(
                format!("now:{now}"),
                m3::body(now)
                    .automation_name("What Auto picks now")
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
        for role in model::routing_roles(&self.settings.routing) {
            let list = model::candidates(&self.settings.routing, &role);
            let headline = match role.as_str() {
                "default" => "Default",
                "manager" => "Manager",
                "worker" => "Worker",
                "audit" => "Audit",
                other => other,
            };
            let supporting = match (role.as_str(), list.is_empty()) {
                ("default", true) => "No profiles: tasks on Auto cannot start.",
                ("default", false) => "New tasks on Auto.",
                (_, true) => "Not set: launches that ask for this role are refused.",
                (_, false) => "Launches that ask for this role.",
            };
            let mut rows: Vec<KeyedView> = Vec::new();
            let count = list.len();
            for (index, name) in list.iter().enumerate() {
                let first = index == 0;
                let last = index + 1 == count;
                let buttons = StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .children((
                        m3::icon_button(palette)
                            .is_enabled(!first)
                            .automation_name(format!("Move {name} earlier in {role}"))
                            .on_click(context.message(Message::Settings(
                                SettingsMsg::RoutingMoved(role.clone(), index, true),
                            )))
                            .content(m3::icon(m3::glyph::ARROW_UP)),
                        m3::icon_button(palette)
                            .is_enabled(!last)
                            .automation_name(format!("Move {name} later in {role}"))
                            .on_click(context.message(Message::Settings(
                                SettingsMsg::RoutingMoved(role.clone(), index, false),
                            )))
                            .content(m3::icon(m3::glyph::ARROW_DOWN)),
                        m3::icon_button(palette)
                            .automation_name(format!("Remove {name} from {role}"))
                            .on_click(context.message(Message::Settings(
                                SettingsMsg::RoutingRemoved(role.clone(), index),
                            )))
                            .content(m3::icon(m3::glyph::CLOSE)),
                    ));
                rows.push(KeyedView::new(
                    format!("{role}:{index}:{name}:{first}:{last}"),
                    candidate_row(index + 1, name, buttons),
                ));
            }
            let addable = self.addable_profiles(&role);
            if !addable.is_empty() {
                let key = format!("{role}:add:{}", addable.join("\u{1f}"));
                let callback_role = role.clone();
                rows.push(KeyedView::new(
                    key.clone(),
                    Grid::new()
                        .min_height(tokens::height::FORM_ROW)
                        .keyed_children(vec![KeyedView::new(
                            key,
                            m3::enum_width(ComboBox::new())
                                .items_source(addable)
                                .selected_index(None)
                                .placeholder_text("Add a profile")
                                .automation_name(format!("Add a profile to {role}"))
                                .vertical_alignment(VerticalAlignment::Center)
                                .on_selection_changed(context.callback(move |index| {
                                    Message::Settings(SettingsMsg::RoutingAdded(
                                        callback_role.clone(),
                                        index,
                                    ))
                                })),
                        )]),
                ));
            }
            groups.push(KeyedView::new(
                format!("role:{role}"),
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    m3::section_heading(headline, supporting, None),
                    m3::settings_card(palette, rows),
                )),
            ));
        }
        groups.push(KeyedView::new(
            "new-role",
            m3::settings_card(
                palette,
                vec![KeyedView::new(
                    "new-role",
                    m3::form_row(
                        "Another role",
                        m3::field()
                            .text(self.settings.new_role.clone())
                            .placeholder_text("Role name, for example review")
                            .automation_name("New role name")
                            .on_text_changed(context.callback(|text| {
                                Message::Settings(SettingsMsg::NewRoleChanged(text))
                            }))
                            .into(),
                        Some(
                            m3::outlined(palette)
                                .is_enabled(!self.settings.new_role.trim().is_empty())
                                .automation_name("Add role")
                                .on_click(
                                    context.message(Message::Settings(SettingsMsg::RoleAdded)),
                                )
                                .content(m3::label("Add role")),
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

/// One routing candidate row: its place in the order, its name and the
/// reorder / remove buttons.
fn candidate_row(place: usize, name: &str, buttons: View) -> View {
    Grid::new()
        .height(tokens::height::CONTROL)
        .columns([
            GridLength::Pixel(tokens::layout::TREE_INDENT),
            GridLength::STAR,
            GridLength::Auto,
        ])
        .children((
            m3::secondary(format!("{place}")).vertical_alignment(VerticalAlignment::Center),
            m3::body(name.to_owned())
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
            Border::new().grid_column(2).content(buttons),
        ))
}
