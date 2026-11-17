use super::*;

impl DesktopApp {
    pub(super) fn appearance_page(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let segments: Vec<KeyedView> = [ThemeChoice::System, ThemeChoice::Light, ThemeChoice::Dark]
            .into_iter()
            .map(|choice| {
                let selected = self.prefs.theme == choice;
                KeyedView::new(
                    format!("{}:{selected}", choice.label()),
                    m3::segment(palette, selected)
                        .automation_name(format!(
                            "Theme {}{}",
                            choice.label(),
                            if selected { " (current)" } else { "" }
                        ))
                        .on_click(context.message(Message::Shell(ShellMsg::ThemeChosen(choice))))
                        .content(m3::segment_label(choice.label())),
                )
            })
            .collect();
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::settings_card(
                palette,
                vec![KeyedView::new(
                    "theme",
                    m3::setting_row(
                        "Theme",
                        "System follows the Windows setting.",
                        m3::segmented(palette, segments),
                    ),
                )],
            ),
            self.page_note(Page::Appearance, "Changes save as you choose them."),
        ))
    }
}
