use super::*;

impl DesktopApp {
    pub(super) fn cloud_section(&self, context: &mut ViewContext<Self>) -> View {
        let enabled = model::cloud_enabled(&self.settings.cloud);
        // An on/off setting is a switch (as General's), keyed by state.
        let switch = Grid::new().keyed_children(vec![KeyedView::new(
            format!("cloud:{enabled}"),
            ToggleSwitch::new()
                .is_on(enabled)
                .automation_name("Cloud MCP configured")
                .on_toggled(
                    context.callback(|on: bool| Message::Settings(SettingsMsg::CloudEnabled(on))),
                ),
        )]);
        let mut rows: Vec<KeyedView> = vec![KeyedView::new(
            "enabled",
            controls::setting_row(
                "Cloud MCP",
                "Swarm's MCP endpoint through a Cloudflare tunnel behind Cloudflare Access",
                View::from(switch),
            ),
        )];
        if enabled {
            rows.extend(
                model::CLOUD_FIELDS
                    .iter()
                    .enumerate()
                    .map(|(index, (key, label))| {
                        KeyedView::new(
                            *key,
                            controls::form_row(
                                label,
                                controls::field()
                                    .text(model::cloud_field(&self.settings.cloud, key))
                                    .automation_name(*label)
                                    .on_text_changed(crate::views::text_edits(context.callback(
                                        move |text| {
                                            Message::Settings(SettingsMsg::CloudField(index, text))
                                        },
                                    )))
                                    .into(),
                                None,
                            ),
                        )
                    }),
            );
        }
        controls::settings_card(rows)
    }
}
