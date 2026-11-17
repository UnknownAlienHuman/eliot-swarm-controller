use super::*;

impl DesktopApp {
    pub(super) fn cloud_section(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let enabled = model::cloud_enabled(&self.settings.cloud);
        let switch = Grid::new().keyed_children(vec![KeyedView::new(
            format!("cloud:{enabled}"),
            CheckBox::new()
                .is_checked(enabled)
                .automation_name("Cloud MCP configured")
                .on_is_checked_changed(
                    context.callback(|on: bool| Message::Settings(SettingsMsg::CloudEnabled(on))),
                )
                .content(m3::label("Configured")),
        )]);
        let mut rows: Vec<KeyedView> = vec![
            KeyedView::new("enabled", m3::form_row("Cloud MCP", switch, None)),
            KeyedView::new(
                "help",
                m3::help(
                    "Serve Swarm's MCP endpoint through a Cloudflare tunnel behind Cloudflare Access. The daemon applies a change at its next start.",
                ),
            ),
        ];
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
        m3::group(palette, rows)
    }
}
