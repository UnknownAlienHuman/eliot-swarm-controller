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
fn swatch(choice: ThemeChoice) -> View {
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
        .width(48.0)
        .height(24.0)
        .corner_radius(6.0)
        .vertical_alignment(VerticalAlignment::Center)
        .content(body)
}

impl DesktopApp {
    /// General: Appearance (System and the six themes of the design's
    /// Themes board, each a row with its swatch) and what the window opens
    /// with, each saved the moment it is chosen (`[desktop]` through the
    /// preferences writer).
    pub(super) fn general_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let themes: Vec<KeyedView> = ThemeChoice::ALL
            .into_iter()
            .map(|choice| {
                let selected = self.prefs.theme == choice;
                // Keyed by its selection: WinUI reads a button's colors only
                // when its template is applied.
                KeyedView::new(
                    format!("{}:{selected}", choice.label()),
                    m3::row(palette, selected)
                        .automation_name(format!(
                            "Theme {}{}",
                            choice.label(),
                            if selected { " (current)" } else { "" }
                        ))
                        .on_click(context.message(Message::Shell(ShellMsg::ThemeChosen(choice))))
                        .content(
                            Grid::new()
                                .columns([
                                    GridLength::Auto,
                                    GridLength::Auto,
                                    GridLength::STAR,
                                    GridLength::Auto,
                                ])
                                .column_spacing(tokens::spacing::INSET)
                                .children((
                                    swatch(choice),
                                    m3::text(choice.label(), tokens::text::BUTTON)
                                        .grid_column(1)
                                        .vertical_alignment(VerticalAlignment::Center),
                                    m3::secondary(theme_note(choice))
                                        .grid_column(2)
                                        .vertical_alignment(VerticalAlignment::Center)
                                        .text_trimming(TextTrimming::CharacterEllipsis),
                                    if selected {
                                        Border::new()
                                            .grid_column(3)
                                            .vertical_alignment(VerticalAlignment::Center)
                                            .content(m3::icon(m3::glyph::CHECK))
                                    } else {
                                        View::empty()
                                    },
                                )),
                        ),
                )
            })
            .collect();
        let reopen = self.prefs.startup == Startup::LastConversation;
        let switch = Grid::new().keyed_children(vec![KeyedView::new(
            format!("reopen:{reopen}"),
            ToggleSwitch::new()
                .is_on(reopen)
                .width(tokens::layout::SWITCH_WIDTH)
                .automation_name("Reopen the last task at start")
                .on_toggled(context.callback(|on: bool| {
                    Message::Shell(ShellMsg::StartupChosen(if on {
                        Startup::LastConversation
                    } else {
                        Startup::NewTask
                    }))
                })),
        )]);
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::card_title("Appearance"),
            m3::list(palette, themes),
            m3::card_title("Startup"),
            m3::settings_card(
                palette,
                vec![KeyedView::new(
                    "reopen",
                    m3::setting_row(
                        "Reopen the last task",
                        "At start, open the most recently active task instead of a new one.",
                        switch,
                    ),
                )],
            ),
        ))
    }
}
