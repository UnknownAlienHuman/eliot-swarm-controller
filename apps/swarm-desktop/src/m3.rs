//! The one component system of the desktop: type roles, one row, one chip,
//! three buttons (filled, tonal, text), the status dot, cards and fields,
//! all on the 8-point grid with the three radii of [`theme::shape`].
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
use crate::theme::{self, StatusTone, layout, shape, spacing};

/// Segoe Fluent Icons code points (the system icon font; no icon pack).
pub mod glyph {
    pub const NEW_TASK: &str = "\u{E70F}";
    pub const FOLDER: &str = "\u{E8B7}";
    pub const SETTINGS: &str = "\u{E713}";
    pub const SEND: &str = "\u{E724}";
    pub const STOP: &str = "\u{E71A}";
    pub const STEER: &str = "\u{E72A}";
    pub const MORE: &str = "\u{E712}";
    pub const INFO: &str = "\u{E946}";
    pub const CLOSE: &str = "\u{E711}";
    pub const BACK: &str = "\u{E72B}";
    pub const REFRESH: &str = "\u{E72C}";
    pub const CHEVRON_RIGHT: &str = "\u{E76C}";
    pub const CHEVRON_DOWN: &str = "\u{E70D}";
    pub const ARROW_DOWN: &str = "\u{E74B}";
    pub const COPY: &str = "\u{E8C8}";
    pub const MODEL: &str = "\u{E945}";
    pub const SHIELD: &str = "\u{EA18}";
    pub const PROFILE: &str = "\u{E77B}";
    pub const PULSE: &str = "\u{E9D9}";
}

pub fn weight(style: TypeStyle) -> FontWeight {
    FontWeight::new(style.weight).unwrap_or(FontWeight::NORMAL)
}

/// Text in a type role.
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

/// A page or pane title with its accessibility heading level.
pub fn title(value: impl Into<String>, style: TypeStyle) -> TextBlock {
    text(value, style).automation_heading_level(AutomationHeadingLevel::Level2)
}

/// A section caption ("Projects", "Recents", a settings group).
pub fn caption(value: impl Into<String>) -> TextBlock {
    muted(value, theme::type_scale::LABEL_MEDIUM)
        .automation_heading_level(AutomationHeadingLevel::Level3)
}

/// A card: the one container of grouped content (call `.content(..)`).
pub fn card(palette: Palette) -> Border {
    Border::new()
        .background(palette.card())
        .corner_radius(shape::MD)
        .padding(Thickness::uniform(spacing::MD))
}

/// The status dot of an Agent state in a list; empty for states that need
/// no mark.
pub fn list_dot(palette: Palette, state: &str) -> View {
    match palette.dot(StatusTone::of_state(state)) {
        Some(fill) => dot(fill),
        None => View::empty(),
    }
}

/// An 8-DIP status dot.
pub fn dot(fill: Brush) -> View {
    Ellipse::new()
        .fill(fill)
        .width(8.0)
        .height(8.0)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// A 16-DIP system icon; it takes the foreground of the control around it.
/// Reactor 0.100.0's `FontIcon` has no font size (WinUI draws 20 DIPs), so
/// a `Viewbox` scales it to the 16-DIP icon size of rows and chips.
pub fn icon(glyph: &str) -> View {
    Viewbox::new()
        .width(16.0)
        .height(16.0)
        .vertical_alignment(VerticalAlignment::Center)
        .slot(ViewboxSlot::Child, FontIcon::new().glyph(glyph.to_owned()))
}

/// Icon and label side by side: the content of a labelled button, row or
/// chip.
pub fn icon_label(glyph: &str, label: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::SM)
        .children((
            icon(glyph),
            TextBlock::new()
                .text(label.to_owned())
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
        ))
}

/// Filled: the one main action of a surface.
pub fn filled(palette: Palette) -> Button {
    Button::new()
        .resource_overrides(palette.filled())
        .height(layout::CONTROL_HEIGHT)
}

/// Tonal: secondary actions that still read as buttons.
pub fn tonal(palette: Palette) -> Button {
    Button::new()
        .resource_overrides(palette.tonal())
        .height(layout::CONTROL_HEIGHT)
}

/// Text: low-emphasis actions.
pub fn text_button(palette: Palette) -> Button {
    Button::new()
        .resource_overrides(palette.text_button())
        .height(layout::CONTROL_HEIGHT)
}

/// WinUI's default Button padding (11, 5, 11, 6), which Reactor 0.100.0
/// cannot change: a glyph button is this wide so the 16-DIP glyph is whole.
pub const GLYPH_BUTTON: f64 = 40.0;

/// A text button for one glyph (`.content(icon(..))` last). Give it an
/// accessible name.
pub fn icon_button(palette: Palette) -> Button {
    text_button(palette).width(GLYPH_BUTTON)
}

/// The filled icon button of the composer (Send).
pub fn filled_icon(palette: Palette) -> Button {
    filled(palette).width(GLYPH_BUTTON)
}

/// The one row: full width, content on the left, the selection in
/// secondary-container. Used by the sidebar and the settings sections.
pub fn row(palette: Palette, selected: bool) -> Button {
    Button::new()
        .resource_overrides(palette.row(selected))
        .height(layout::CONTROL_HEIGHT)
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Stretch)
}

/// The one chip: a compact neutral button (`.content(icon_label(..))`
/// last) that opens a menu (`.menu(..)`).
pub fn chip(palette: Palette) -> Button {
    Button::new()
        .resource_overrides(palette.chip())
        .height(32.0)
}

/// The read-only form of [`chip`]: the same look, no button.
pub fn chip_static(glyph: &str, label: &str) -> View {
    Border::new()
        .height(32.0)
        .padding(Thickness::xy(11.0, 0.0))
        .opacity(theme::MUTED_OPACITY)
        .content(
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(spacing::SM)
                .vertical_alignment(VerticalAlignment::Center)
                .children((
                    icon(glyph),
                    TextBlock::new()
                        .text(label.to_owned())
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                )),
        )
}

/// A segmented choice: tonal when selected, text otherwise.
pub fn segment(palette: Palette, selected: bool) -> Button {
    if selected {
        tonal(palette)
    } else {
        text_button(palette)
    }
}

/// A text field on the field role, no outline.
pub fn field(palette: Palette) -> TextBox {
    TextBox::new()
        .background(palette.field())
        .border_thickness(Thickness::uniform(0.0))
}
