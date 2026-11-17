use super::*;

impl DesktopApp {
    pub(super) fn startup_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
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
            self.page_note(Page::Startup, "Changes save as you choose them."),
        ))
    }
}
