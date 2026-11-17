use super::*;

impl DesktopApp {
    /// General: how Swarm looks (the theme) and what it opens with, each
    /// saved the moment it is chosen (`[desktop]` through the preferences
    /// writer), as the Codex General page does.
    pub(super) fn general_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
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
        let reopen = self.prefs.startup == Startup::LastConversation;
        // A check box rather than a switch: its shape stays on the scale (a
        // switch is a pill with a round thumb).
        let check = Grid::new().keyed_children(vec![KeyedView::new(
            format!("reopen:{reopen}"),
            CheckBox::new()
                .is_checked(reopen)
                .automation_name("Reopen the last task at start")
                .on_is_checked_changed(context.callback(|on: bool| {
                    Message::Shell(ShellMsg::StartupChosen(if on {
                        Startup::LastConversation
                    } else {
                        Startup::NewTask
                    }))
                })),
        )]);
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::card_title("Appearance"),
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
            m3::card_title("Startup"),
            m3::settings_card(
                palette,
                vec![KeyedView::new(
                    "reopen",
                    m3::setting_row(
                        "Reopen the last task",
                        "At start, open the most recently active task instead of a new one.",
                        check,
                    ),
                )],
            ),
        ))
    }
}
