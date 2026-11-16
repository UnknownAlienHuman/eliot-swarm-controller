//! The one component system of the desktop, in Material 3 components: the
//! type roles, the navigation-drawer item and section header, the extended
//! FAB, common buttons (filled, tonal, outlined, text), icon buttons,
//! assist/filter chips, the segmented button, status dots and chips, cards,
//! list items and the outlined text field. Colors come from the active
//! [`Palette`]; buttons are real WinUI `Button`s whose colors come from
//! their family's resource overrides, so these are
//! plain compositions, not a wrapper family. The M3 shape of a button is
//! its style: common buttons and navigation items use the implicit style,
//! icon buttons `ButtonStyle::Subtle`, chips `ButtonStyle::TextLink` and the
//! FAB and card buttons `ButtonStyle::Accent`, the keys `app_resources`
//! redefines.

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
    pub const CHECK: &str = "\u{E73E}";
    pub const TOOLS: &str = "\u{E90F}";
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

/// A navigation-drawer section header ("Projects", "Recents"): M3
/// title-small in on-surface-variant.
pub fn caption(value: impl Into<String>) -> TextBlock {
    muted(value, theme::type_scale::TITLE_SMALL)
        .automation_heading_level(AutomationHeadingLevel::Level3)
}

/// A card on the surface: surface-container-low, medium corner (call
/// `.content(..)`).
pub fn card(palette: Palette) -> Border {
    Border::new()
        .background(palette.card())
        .corner_radius(shape::MD)
        .padding(Thickness::uniform(spacing::MD))
}

/// A card on the drawer-toned panel: surface-container-high.
pub fn card_high(palette: Palette) -> Border {
    card(palette).background(palette.card_high())
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

/// A status chip: the state's container with its on-container label
/// (label-medium), small corner.
pub fn status_chip(palette: Palette, state: &str) -> View {
    let (fill, on) = palette.status_chip(StatusTone::of_state(state));
    Border::new()
        .background(fill)
        .corner_radius(shape::SM)
        .height(24.0)
        .padding(Thickness::xy(spacing::SM, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .content(
            text(state.to_owned(), theme::type_scale::LABEL_MEDIUM)
                .foreground(on)
                .vertical_alignment(VerticalAlignment::Center),
        )
}

/// A system icon of `size` DIPs; it takes the foreground of the control
/// around it. Reactor 0.100.0's `FontIcon` has no font size (WinUI draws 20
/// DIPs), so a `Viewbox` scales it.
pub fn icon_sized(glyph: &str, size: f64) -> View {
    Viewbox::new()
        .width(size)
        .height(size)
        .vertical_alignment(VerticalAlignment::Center)
        .slot(ViewboxSlot::Child, FontIcon::new().glyph(glyph.to_owned()))
}

/// The 20-DIP icon of icon buttons and navigation items.
pub fn icon(glyph: &str) -> View {
    icon_sized(glyph, 20.0)
}

/// Icon and label side by side: the content of a labelled button or
/// navigation item (20-DIP icon, 12-DIP gap).
pub fn icon_label(glyph: &str, label: &str) -> View {
    leading(icon(glyph), label, 12.0)
}

/// The 18-DIP icon and label of a chip (8-DIP gap).
pub fn chip_label(glyph: &str, label: &str) -> View {
    leading(icon_sized(glyph, 18.0), label, spacing::SM)
}

fn leading(icon: View, label: &str, gap: f64) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(gap)
        .children((
            icon,
            TextBlock::new()
                .text(label.to_owned())
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
        ))
}

fn button(overrides: ResourceOverrides, height: f64) -> Button {
    Button::new().resource_overrides(overrides).height(height)
}

/// Filled: the one main action of a surface.
pub fn filled(palette: Palette) -> Button {
    button(palette.filled(), layout::CONTROL_HEIGHT)
}

/// Danger: the confirmed destructive action (Force stop).
pub fn danger(palette: Palette) -> Button {
    button(palette.danger(), layout::CONTROL_HEIGHT)
}

/// Filled tonal: secondary actions that still read as buttons.
pub fn tonal(palette: Palette) -> Button {
    button(palette.tonal(), layout::CONTROL_HEIGHT)
}

/// Outlined: a medium-emphasis action beside a filled one.
pub fn outlined(palette: Palette) -> Button {
    button(palette.outlined(), layout::CONTROL_HEIGHT)
}

/// Text: low-emphasis actions.
pub fn text_button(palette: Palette) -> Button {
    button(palette.text_button(), layout::CONTROL_HEIGHT)
}

/// A standard icon button (`.content(icon(..))` last). Give it an
/// accessible name.
pub fn icon_button(palette: Palette) -> Button {
    button(palette.icon_button(), layout::ICON_BUTTON)
        .width(layout::ICON_BUTTON)
        .style(ButtonStyle::Subtle)
}

/// The filled icon button of the composer (Send).
pub fn filled_icon(palette: Palette) -> Button {
    button(palette.filled(), layout::ICON_BUTTON)
        .width(layout::ICON_BUTTON)
        .style(ButtonStyle::Subtle)
}

/// The M3 extended FAB (New task): primary-container, large corner, icon
/// and label.
pub fn fab(palette: Palette) -> Button {
    button(palette.fab(), layout::NAV_ITEM_HEIGHT).style(ButtonStyle::Accent)
}

/// A navigation-drawer item: 56 DIPs, full width, the active indicator
/// pill in secondary-container when selected. Give it [`nav_content`].
pub fn nav_item(palette: Palette, selected: bool) -> Button {
    button(palette.nav_item(selected), layout::NAV_ITEM_HEIGHT)
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Stretch)
}

/// The content of a navigation item, at the M3 16-DIP start inset (the
/// implicit button style pads 24 on both sides).
pub fn nav_content(content: impl Into<View>) -> View {
    Border::new()
        .margin(Thickness::new(-8.0, 0.0, 0.0, 0.0))
        .content(content)
}

/// The icon and label of a text button, at the M3 12/16-DIP sides.
pub fn text_icon_label(glyph: &str, label: &str) -> View {
    Border::new()
        .margin(Thickness::new(-12.0, 0.0, -8.0, 0.0))
        .content(leading(icon_sized(glyph, 18.0), label, spacing::SM))
}

/// The label of a text button or segment, at the M3 12-DIP sides.
pub fn label(value: &str) -> TextBlock {
    TextBlock::new()
        .text(value.to_owned())
        .margin(Thickness::xy(-12.0, 0.0))
}

/// A card that is one button (monitor cards).
pub fn card_button(palette: Palette) -> Button {
    Button::new()
        .resource_overrides(palette.card_button())
        .style(ButtonStyle::Accent)
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Stretch)
}

/// An assist chip that opens a menu (`.content(chip_label(..))` last,
/// then `.menu(..)`): outlined, small corner.
pub fn chip(palette: Palette) -> Button {
    button(palette.chip(false), layout::CHIP_HEIGHT).style(ButtonStyle::TextLink)
}

/// The read-only form of [`chip`]: the same outline and label, no button.
pub fn chip_static(palette: Palette, glyph: &str, label: &str) -> View {
    Border::new()
        .height(layout::CHIP_HEIGHT)
        .corner_radius(shape::SM)
        .border_thickness(Thickness::uniform(1.0))
        .border_brush(palette.outline_variant())
        .padding(Thickness::new(spacing::SM, 0.0, 12.0, 0.0))
        .content(
            Border::new()
                .opacity(theme::MUTED_OPACITY)
                .vertical_alignment(VerticalAlignment::Center)
                .content(chip_label(glyph, label)),
        )
}

/// One segment of a segmented button (see [`segmented`]).
pub fn segment(palette: Palette, selected: bool) -> Button {
    button(palette.segment(selected), layout::CONTROL_HEIGHT - 2.0)
}

/// The content of a segment: a check before the selected label.
pub fn segment_label(label: &str, selected: bool) -> View {
    if selected {
        leading(icon_sized(glyph::CHECK, 16.0), label, spacing::SM)
    } else {
        TextBlock::new().text(label.to_owned()).into()
    }
}

/// The M3 segmented button: the segments inside one outlined pill.
pub fn segmented(palette: Palette, segments: Vec<KeyedView>) -> View {
    Border::new()
        .height(layout::CONTROL_HEIGHT)
        .corner_radius(layout::CONTROL_HEIGHT / 2.0)
        .border_thickness(Thickness::uniform(1.0))
        .border_brush(palette.outline())
        .content(
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .keyed_children(segments),
        )
}

/// An outlined text field (the application's text-box style).
pub fn field() -> TextBox {
    TextBox::new()
}

/// An M3 list item: headline (body-large) and supporting text (body-medium,
/// on-surface-variant) on the left, an optional trailing control.
pub fn list_item(headline: &str, supporting: &str, trailing: View) -> View {
    let text_column = StackPanel::new()
        .spacing(2.0)
        .vertical_alignment(VerticalAlignment::Center)
        .children((
            text(headline.to_owned(), theme::type_scale::BODY_LARGE)
                .text_wrapping(TextWrapping::Wrap),
            if supporting.is_empty() {
                View::empty()
            } else {
                muted(supporting.to_owned(), theme::type_scale::BODY_MEDIUM)
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true)
                    .into()
            },
        ));
    Border::new()
        .min_height(layout::NAV_ITEM_HEIGHT)
        .padding(Thickness::xy(spacing::MD, spacing::SM))
        .content(
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .column_spacing(spacing::MD)
                .children((
                    text_column,
                    Border::new()
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(trailing),
                )),
        )
}

/// A list of items in one container (surface-container-low, medium corner),
/// separated by outline-variant dividers.
pub fn list_group(palette: Palette, items: Vec<(String, View)>) -> View {
    let count = items.len();
    let mut children: Vec<KeyedView> = Vec::with_capacity(count * 2);
    for (index, (key, item)) in items.into_iter().enumerate() {
        children.push(KeyedView::new(key.clone(), item));
        if index + 1 < count {
            children.push(KeyedView::new(
                format!("{key}:divider"),
                Border::new()
                    .height(1.0)
                    .margin(Thickness::xy(spacing::MD, 0.0))
                    .background(palette.outline_variant()),
            ));
        }
    }
    Border::new()
        .background(palette.card())
        .corner_radius(shape::MD)
        .padding(Thickness::xy(0.0, spacing::XS))
        .vertical_alignment(VerticalAlignment::Top)
        .content(StackPanel::new().keyed_children(children))
}
