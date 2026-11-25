use super::*;

impl DesktopApp {
    /// General: Appearance (System, Light or Dark; High Contrast follows
    /// Windows), then Start and work (what the window opens with, the
    /// default access of new tasks), each saved the moment it is chosen
    /// (`[desktop]` through the preferences writer).
    pub(super) fn general_page(&self, context: &mut ViewContext<Self>) -> View {
        let theme = ThemeChoice::ALL
            .iter()
            .position(|choice| *choice == self.prefs.theme);
        let theme_labels: Vec<String> = ThemeChoice::ALL
            .iter()
            .map(|choice| choice.label().to_owned())
            .collect();
        let shown_theme = self.prefs.theme;
        let theme_picker = Grid::new().keyed_children(vec![KeyedView::new(
            format!("theme:{theme:?}"),
            ComboBox::new()
                .width(tokens::page::SETTINGS_CONTROL)
                .items_source(theme_labels)
                .selected_index(theme)
                .automation_name("Theme")
                // The list reports the value it shows when it is built: the
                // window saves only a change.
                .on_selection_changed(context.callback(move |index: Option<usize>| {
                    Message::Shell(ShellMsg::ThemeChosen(
                        index
                            .and_then(|index| ThemeChoice::ALL.get(index).copied())
                            .unwrap_or(shown_theme),
                    ))
                })),
        )]);
        let startup = Startup::ALL
            .iter()
            .position(|startup| *startup == self.prefs.startup);
        let startup_labels: Vec<String> = Startup::ALL
            .iter()
            .map(|startup| startup.label().to_owned())
            .collect();
        let shown_startup = self.prefs.startup;
        let startup_picker = Grid::new().keyed_children(vec![KeyedView::new(
            format!("startup:{startup:?}"),
            ComboBox::new()
                .width(tokens::page::SETTINGS_CONTROL)
                .items_source(startup_labels)
                .selected_index(startup)
                .automation_name("On startup")
                // No selection is the value shown, never New task.
                .on_selection_changed(context.callback(move |index: Option<usize>| {
                    Message::Shell(ShellMsg::StartupChosen(
                        index
                            .and_then(|index| Startup::ALL.get(index).copied())
                            .unwrap_or(shown_startup),
                    ))
                })),
        )]);
        let shown = self.prefs.last_access.unwrap_or(Access::Ask);
        let access = Access::ALL.iter().position(|access| *access == shown);
        let access_labels: Vec<String> = Access::ALL
            .iter()
            .map(|access| access.label().to_owned())
            .collect();
        let access_picker = Grid::new().keyed_children(vec![KeyedView::new(
            format!("access:{}", shown.wire()),
            ComboBox::new()
                .width(tokens::page::SETTINGS_CONTROL)
                .items_source(access_labels)
                .selected_index(access)
                .automation_name("Default access for new tasks")
                .on_selection_changed(
                    context.callback(|index| Message::Settings(SettingsMsg::AccessChosen(index))),
                ),
        )]);
        let levels: Vec<KeyedView> = Access::ALL
            .iter()
            .map(|access| {
                KeyedView::new(
                    access.wire(),
                    controls::setting_row(access.label(), access.description(), None),
                )
            })
            .collect();
        StackPanel::new().spacing(tokens::spacing::SM).children((
            controls::card_title("Appearance"),
            controls::settings_card(vec![KeyedView::new(
                "theme",
                controls::setting_row(
                    "Theme",
                    "System follows Windows; High Contrast always does",
                    View::from(theme_picker),
                ),
            )]),
            controls::card_title("Start and work"),
            controls::settings_card(
                vec![
                    KeyedView::new(
                        "startup",
                        controls::setting_row("On startup", "What the window opens with", View::from(startup_picker)),
                    ),
                    KeyedView::new(
                        "access",
                        controls::setting_row(
                            "Default access",
                            "For new tasks. You can change it in the composer",
                            View::from(access_picker),
                        ),
                    ),
                ],
            ),
            controls::card_title("What each access level allows"),
            controls::settings_card(levels),
            controls::secondary(
                "Presets and Auto keep their own permissions. OpenCode has no Full access mode.",
            )
            .text_wrapping(TextWrapping::Wrap),
        )).into()
    }
}
