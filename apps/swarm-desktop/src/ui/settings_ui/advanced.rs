use super::*;

impl DesktopApp {
    pub(super) fn advanced_section(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let data_dir = match &self.data_dir {
            Ok(path) => nav::display_path(&path.display().to_string()).to_owned(),
            Err(error) => format!("unavailable: {error}"),
        };
        let open = self.settings.history_open;
        let history: Vec<KeyedView> = self
            .settings
            .history
            .iter()
            .map(|entry| {
                let (headline, supporting) = model::history_lines(entry);
                KeyedView::new(
                    format!("history:{}", entry.revision),
                    Border::new()
                        .height(tokens::height::CONTROL)
                        .automation_name(format!("{headline}, {supporting}"))
                        .content(m3::row_content(None, &headline, &supporting, View::empty())),
                )
            })
            .collect();
        let history_view: View = if !open {
            View::empty()
        } else if history.is_empty() {
            m3::secondary("No settings changes recorded yet.").into()
        } else {
            m3::group(palette, history)
        };
        let restart = if self.settings.restart_required.is_empty() {
            "Every section applies without a restart.".to_owned()
        } else {
            format!(
                "Changes to {} apply at the daemon's next start.",
                self.settings
                    .restart_required
                    .iter()
                    .map(|section| model::section_label(*section))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let dirty = self.settings.dirty(Page::Advanced);
        StackPanel::new()
            .spacing(tokens::spacing::MD)
            .children((
                m3::group(
                    palette,
                    vec![
                        KeyedView::new(
                            "data",
                            m3::form_row(
                                "Daemon data",
                                m3::body(data_dir)
                                    .text_trimming(TextTrimming::CharacterEllipsis)
                                    .is_text_selection_enabled(true)
                                    .into(),
                                Some(
                                    m3::outlined(palette)
                                        .automation_name("Reload everything")
                                        .on_click(context.message(Message::Shell(
                                            ShellMsg::RefreshRequested,
                                        )))
                                        .content(m3::label("Reload")),
                                ),
                            ),
                        ),
                        KeyedView::new(
                            "data-help",
                            m3::help(
                                "Settings live in its config.toml; every change goes through the daemon's checks.",
                            ),
                        ),
                        KeyedView::new(
                            "history",
                            m3::form_row(
                                "History",
                                m3::body(format!(
                                    "{} recent change(s): who changed which section, and when.",
                                    self.settings.history.len()
                                ))
                                .text_trimming(TextTrimming::CharacterEllipsis)
                                .into(),
                                Some(
                                    m3::text_button(palette)
                                        .automation_name(if open {
                                            "Hide history"
                                        } else {
                                            "Show history"
                                        })
                                        .on_click(context.message(Message::Settings(
                                            SettingsMsg::HistoryToggled,
                                        )))
                                        .content(m3::label(if open { "Hide" } else { "Show" })),
                                ),
                            ),
                        ),
                    ],
                ),
                history_view,
                m3::settings_card(palette, vec![KeyedView::new("storage", self.storage_row())]),
                m3::section_heading(
                    "All settings",
                    &format!(
                        "Every section of config.toml as JSON, including daemon, mail, accounts and proxies. {restart}"
                    ),
                    None,
                ),
                m3::field()
                    .accepts_return(true)
                    .text_wrapping(TextWrapping::NoWrap)
                    .min_height(tokens::layout::SOURCE_MIN_HEIGHT)
                    .max_height(tokens::layout::SOURCE_MAX_HEIGHT)
                    .automation_name("Settings sections JSON")
                    .text(self.primed(&self.settings.all))
                    .is_enabled(!self.settings.busy())
                    .on_text_changed(
                        context.callback(|text| Message::Settings(SettingsMsg::AllDraft(text))),
                    ),
                self.save_bar(Page::Advanced, dirty, context, palette),
            ))
    }
}
