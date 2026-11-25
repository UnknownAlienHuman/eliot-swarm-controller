use super::*;

use crate::theme::winui::color;

/// What each theme is, as the Themes board says.
fn theme_note(choice: ThemeChoice) -> &'static str {
    match choice {
        ThemeChoice::System => "Light or Dark, as Windows is set",
        ThemeChoice::Light => "Default",
        ThemeChoice::Dark => "Same roles, dark tones",
        ThemeChoice::Amoled => "True black for OLED",
        ThemeChoice::Purple => "Material baseline",
        ThemeChoice::Neon => "Cyan on night, magenta alerts",
        ThemeChoice::Matrix => "Phosphor green on black",
    }
}

/// A theme's swatch: its sidebar, surface and selection side by side on
/// the card corner, its primary as a dot (System shows Light and Dark).
fn swatch(choice: ThemeChoice, palette: Palette) -> View {
    let half = |theme: crate::theme::Theme, column: i32| {
        let roles = theme.roles();
        Grid::new()
            .grid_column(column)
            .columns([GridLength::STAR, GridLength::STAR])
            .children((
                Border::new().background(Brush::Solid(color(roles.sidebar))),
                Border::new()
                    .grid_column(1)
                    .background(Brush::Solid(color(roles.surface)))
                    .content(
                        Ellipse::new()
                            .fill(Brush::Solid(color(roles.primary)))
                            .width(tokens::layout::DOT)
                            .height(tokens::layout::DOT)
                            .horizontal_alignment(HorizontalAlignment::Center)
                            .vertical_alignment(VerticalAlignment::Center),
                    ),
            ))
    };
    let body: View = match choice {
        ThemeChoice::System => Grid::new()
            .columns([GridLength::STAR, GridLength::STAR])
            .children((
                half(crate::theme::Theme::Light, 0),
                half(crate::theme::Theme::Dark, 1),
            )),
        other => half(other.theme(false), 0),
    };
    Border::new()
        .width(tokens::page::THEME_SWATCH_WIDTH)
        .height(tokens::page::THEME_SWATCH_HEIGHT)
        .corner_radius(tokens::radius::SWATCH)
        .border_brush(palette.outline())
        .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
        .vertical_alignment(VerticalAlignment::Center)
        .content(body)
}

impl DesktopApp {
    /// General (`Settings.dc.html`): Appearance (System and the six themes
    /// of the Themes board as swatch cards), then Start and work (what the
    /// window opens with, the default access of new tasks), each saved the
    /// moment it is chosen (`[desktop]` through the preferences writer).
    pub(super) fn general_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let themes: Vec<KeyedView> = ThemeChoice::ALL
            .into_iter()
            .map(|choice| {
                let selected = self.prefs.theme == choice;
                let label: View = if selected {
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::XS)
                        .horizontal_alignment(HorizontalAlignment::Center)
                        .children((
                            m3::small_icon(m3::glyph::CHECK),
                            m3::text(choice.label(), tokens::text::LABEL),
                        ))
                } else {
                    m3::text(choice.label(), tokens::text::LABEL)
                        .horizontal_alignment(HorizontalAlignment::Center)
                        .into()
                };
                // Keyed by its selection: WinUI reads a button's colors only
                // when its template is applied.
                KeyedView::new(
                    format!("{}:{selected}", choice.label()),
                    m3::segment(palette, selected)
                        .height(
                            tokens::page::THEME_SWATCH_HEIGHT
                                + 2.0 * tokens::spacing::SM
                                + tokens::text::LABEL.line_height
                                + tokens::spacing::CHIP_GAP,
                        )
                        .automation_name(format!(
                            "Theme {}: {}{}",
                            choice.label(),
                            theme_note(choice),
                            if selected { " (current)" } else { "" }
                        ))
                        .on_click(context.message(Message::Shell(ShellMsg::ThemeChosen(choice))))
                        .content(
                            StackPanel::new()
                                .spacing(tokens::spacing::CHIP_GAP)
                                .margin(Thickness::uniform(
                                    tokens::spacing::SM - tokens::spacing::ROW,
                                ))
                                .children((swatch(choice, palette), label)),
                        )
                        .tooltip(theme_note(choice)),
                )
            })
            .collect();
        let startup = Startup::ALL
            .iter()
            .position(|startup| *startup == self.prefs.startup);
        let startup_labels: Vec<String> = Startup::ALL
            .iter()
            .map(|startup| startup.label().to_owned())
            .collect();
        let startup_picker = Grid::new().keyed_children(vec![KeyedView::new(
            format!("startup:{startup:?}"),
            ComboBox::new()
                .width(tokens::page::SETTINGS_CONTROL)
                .items_source(startup_labels)
                .selected_index(startup)
                .automation_name("On startup")
                .on_selection_changed(context.callback(|index: Option<usize>| {
                    Message::Shell(ShellMsg::StartupChosen(
                        index
                            .and_then(|index| Startup::ALL.get(index).copied())
                            .unwrap_or_default(),
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
                    m3::setting_row(access.label(), access.description(), View::empty()),
                )
            })
            .collect();
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::card_title("Appearance"),
            m3::settings_card(
                palette,
                vec![KeyedView::new(
                    "theme",
                    StackPanel::new()
                        .margin(Thickness::xy(0.0, tokens::spacing::INSET))
                        .spacing(tokens::spacing::SM)
                        .children((
                            StackPanel::new().children((
                                m3::body("Theme"),
                                m3::secondary("Applies to every window right away"),
                            )),
                            StackPanel::new()
                                .orientation(Orientation::Horizontal)
                                .spacing(tokens::spacing::SM)
                                .keyed_children(themes),
                        )),
                )],
            ),
            m3::card_title("Start and work"),
            m3::settings_card(
                palette,
                vec![
                    KeyedView::new(
                        "startup",
                        m3::setting_row("On startup", "What the window opens with", startup_picker),
                    ),
                    KeyedView::new(
                        "access",
                        m3::setting_row(
                            "Default access",
                            "For new tasks. You can change it in the composer",
                            access_picker,
                        ),
                    ),
                ],
            ),
            m3::card_title("What each access level allows"),
            m3::settings_card(palette, levels),
            m3::secondary(
                "Presets and Auto keep their own permissions. OpenCode has no Full access mode.",
            )
            .text_wrapping(TextWrapping::Wrap),
        ))
    }
}
