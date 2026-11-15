//! Shared M3 view functions used by more than one screen: type roles,
//! surfaces, status chips, icons, and the M3 button and field treatments.
//! Colors come from the active [`Palette`]; buttons are real WinUI
//! `Button`s themed through Reactor resource overrides (issue #407 §6), so
//! these are plain compositions, not a wrapper family. Every variant keeps
//! the default button style: Reactor 0.100.0 sets a named style
//! (`ButtonStyle::Accent`/`Subtle`) from the application dictionary, and a
//! named style together with resource overrides on the same button failed
//! with a stowed XAML `E_FAIL` (0xC000027B) in this window.

use windows_reactor::*;

use crate::theme::type_scale::TypeStyle;
use crate::theme::winui::Palette;
use crate::theme::{self, StatusTone, shape, spacing};

/// Segoe Fluent Icons code points (the system icon font; no icon pack).
pub mod glyph {
    pub const ADD: &str = "\u{E710}";
    pub const CHAT: &str = "\u{E8BD}";
    pub const PROFILES: &str = "\u{E77B}";
    pub const AUTOMATION: &str = "\u{E823}";
    pub const SCRIPTS: &str = "\u{E943}";
    pub const MONITOR: &str = "\u{E9D9}";
    pub const THEME_SYSTEM: &str = "\u{E790}";
    pub const THEME_LIGHT: &str = "\u{E706}";
    pub const THEME_DARK: &str = "\u{E708}";
    pub const REFRESH: &str = "\u{E72C}";
    pub const SEND: &str = "\u{E724}";
    pub const INTERRUPT: &str = "\u{E711}";
    pub const HAND_OFF: &str = "\u{E8AB}";
}

pub fn weight(style: TypeStyle) -> FontWeight {
    FontWeight::new(style.weight).unwrap_or(FontWeight::NORMAL)
}

/// Text in an M3 type role.
pub fn text(value: impl Into<String>, style: TypeStyle) -> TextBlock {
    TextBlock::new()
        .text(value)
        .font_size(style.size)
        .font_weight(weight(style))
}

/// Supporting text: the role at lowered emphasis.
pub fn muted(value: impl Into<String>, style: TypeStyle) -> TextBlock {
    text(value, style).opacity(theme::MUTED_OPACITY)
}

/// A screen or pane title with its accessibility heading level.
pub fn title(value: impl Into<String>, style: TypeStyle) -> TextBlock {
    text(value, style).automation_heading_level(AutomationHeadingLevel::Level2)
}

/// A rounded pane on the surface role (call `.content(..)` after placing).
pub fn pane(palette: Palette) -> Border {
    Border::new()
        .background(palette.surface())
        .corner_radius(shape::PANE)
        .padding(Thickness::uniform(spacing::LG))
}

/// A card inside a pane (surface-container).
pub fn card(palette: Palette) -> Border {
    Border::new()
        .background(palette.surface_container())
        .corner_radius(shape::LG)
        .padding(Thickness::xy(spacing::LG, spacing::MD))
}

/// A compact status chip in the tone's container colors.
pub fn status_chip(palette: Palette, state: &str) -> View {
    chip(palette, StatusTone::of_state(state), state)
}

/// A compact chip with a tone and a short label.
pub fn chip(palette: Palette, tone: StatusTone, label: &str) -> View {
    let (fill, on) = palette.chip(tone);
    Border::new()
        .background(fill)
        .corner_radius(shape::SM)
        .padding(Thickness::xy(spacing::SM, 2.0))
        .vertical_alignment(VerticalAlignment::Center)
        .content(text(label, theme::type_scale::LABEL_MEDIUM).foreground(on))
}

/// A system icon; it takes the foreground of the control around it.
pub fn icon(glyph: &str) -> FontIcon {
    FontIcon::new().glyph(glyph.to_owned())
}

/// Icon and label side by side: the content of a labelled M3 button.
pub fn icon_label(glyph: &str, label: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::SM)
        .children((
            icon(glyph).vertical_alignment(VerticalAlignment::Center),
            TextBlock::new()
                .text(label.to_owned())
                .vertical_alignment(VerticalAlignment::Center),
        ))
}

/// The filled button: the one prominent action of a surface.
pub fn filled(palette: Palette) -> Button {
    Button::new().resource_overrides(palette.filled())
}

/// The tonal button: secondary actions.
pub fn tonal(palette: Palette) -> Button {
    Button::new().resource_overrides(palette.tonal())
}

/// The outlined button: an unselected segment or an optional action.
pub fn outlined(palette: Palette) -> Button {
    Button::new().resource_overrides(palette.outlined())
}

/// The text button: low-emphasis actions.
pub fn text_button(palette: Palette) -> Button {
    Button::new().resource_overrides(palette.text_button())
}

/// A segmented choice: tonal when selected, outlined otherwise.
pub fn segment(palette: Palette, selected: bool) -> Button {
    if selected {
        tonal(palette)
    } else {
        outlined(palette)
    }
}

/// A filled M3 text field (surface-container-highest with an outline).
pub fn field(palette: Palette) -> TextBox {
    TextBox::new()
        .background(palette.surface_container_highest())
        .border_brush(palette.outline_variant())
}
