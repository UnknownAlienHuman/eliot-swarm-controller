use super::*;

impl DesktopApp {
    pub(super) fn cloud_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
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
            m3::setting_row(
                "Cloud MCP",
                "Serve Swarm's MCP endpoint through a Cloudflare tunnel behind Cloudflare Access. The daemon applies a change at its next start.",
                switch,
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
                            m3::form_row(
                                label,
                                m3::field()
                                    .text(model::cloud_field(&self.settings.cloud, key))
                                    .automation_name(*label)
                                    .on_text_changed(context.callback(move |text| {
                                        Message::Settings(SettingsMsg::CloudField(index, text))
                                    }))
                                    .into(),
                                None,
                            ),
                        )
                    }),
            );
        }
        m3::settings_card(palette, rows)
    }
}
