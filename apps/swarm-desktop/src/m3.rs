//! The one component system of the desktop: Material 3 color roles on the
//! one size scale of [`crate::ui::tokens`] (compact, Codex density). Type
//! in four styles; rows, buttons, chips and fields 32 DIPs tall with the
//! 8-DIP control corner; cards and the Details sections on the 12-DIP
//! container corner; one action pattern (primary filled, secondary
//! outlined, tertiary text, destructive text in the error color).
//!
//! Buttons are real WinUI `Button`s whose colors come from their family's
//! resource overrides, so these are plain compositions, not a wrapper
//! family. A button's shape is its style: labelled buttons, rows, chips and
//! segments use the implicit style, icon buttons `ButtonStyle::Subtle` and
//! the Send circle `ButtonStyle::TextLink`, the keys `app_resources`
//! redefines from the same tokens.

use windows_reactor::*;

use crate::theme::winui::Palette;
use crate::theme::{self, StatusTone};
use crate::ui::tokens::text::{self as style, TypeStyle};
use crate::ui::tokens::{height, icon as icon_size, layout, radius, spacing};

/// Segoe Fluent Icons code points (the system icon font, one stroke family;
/// no icon pack).
pub mod glyph {
    pub const NEW_TASK: &str = "\u{E70F}";
    pub const FOLDER: &str = "\u{E8B7}";
    pub const SETTINGS: &str = "\u{E713}";
    pub const SEND: &str = "\u{E724}";
    pub const STOP: &str = "\u{E71A}";
    pub const MORE: &str = "\u{E712}";
    pub const INFO: &str = "\u{E946}";
    pub const CLOSE: &str = "\u{E711}";
    pub const BACK: &str = "\u{E72B}";
    pub const CHEVRON_RIGHT: &str = "\u{E76C}";
    pub const CHEVRON_DOWN: &str = "\u{E70D}";
    pub const ARROW_DOWN: &str = "\u{E74B}";
    pub const ARROW_UP: &str = "\u{E74A}";
    pub const COPY: &str = "\u{E8C8}";
    pub const MODEL: &str = "\u{E945}";
    pub const SHIELD: &str = "\u{EA18}";
    pub const PROFILE: &str = "\u{E77B}";
    pub const PULSE: &str = "\u{E9D9}";
    pub const TOOLS: &str = "\u{E90F}";
    pub const FLAG: &str = "\u{E7C1}";
    pub const CHECK: &str = "\u{E73E}";
    pub const STAR: &str = "\u{E734}";
    pub const STAR_FILLED: &str = "\u{E735}";
    pub const LOCK: &str = "\u{E72E}";
    pub const WARNING: &str = "\u{E7BA}";
    pub const AUTO: &str = "\u{E99A}";
    pub const AUTOMATION: &str = "\u{E916}";
    pub const USAGE: &str = "\u{E9D2}";
    pub const EXTERNAL: &str = "\u{E8A7}";
    pub const OPEN_FOLDER: &str = "\u{E838}";
    pub const KEYBOARD: &str = "\u{E765}";
    pub const APPEARANCE: &str = "\u{E790}";
    pub const STARTUP: &str = "\u{E7E8}";
    pub const ROUTING: &str = "\u{E8AB}";
    pub const WORKTREE: &str = "\u{E8F1}";
    pub const CLOUD: &str = "\u{E753}";
    pub const CODE: &str = "\u{E943}";
    /// Manage models (OpenCode's sliders button): Segoe Fluent Equalizer.
    pub const SLIDERS: &str = "\u{E9E9}";
    pub const CHAT: &str = "\u{E8BD}";
    pub const ARCHIVE: &str = "\u{E7B8}";
    pub const DELETE: &str = "\u{E74D}";
    pub const RENAME: &str = "\u{E8AC}";
}

pub fn weight(style: TypeStyle) -> FontWeight {
    FontWeight::new(style.weight).unwrap_or(FontWeight::NORMAL)
}

/// Text in one of the four styles.
pub fn text(value: impl Into<String>, style: TypeStyle) -> TextBlock {
    TextBlock::new()
        .text(value)
        .font_size(style.size)
        .font_weight(weight(style))
}

/// Text at lowered emphasis.
pub fn muted(value: impl Into<String>, style: TypeStyle) -> TextBlock {
    text(value, style).opacity(theme::MUTED_OPACITY)
}

/// Body text: messages, labels, values.
pub fn body(value: impl Into<String>) -> TextBlock {
    text(value, style::BODY)
}

/// Secondary text: notes and supporting lines.
pub fn secondary(value: impl Into<String>) -> TextBlock {
    muted(value, style::SECONDARY)
}

/// A page title (the Settings page, the empty-state greeting).
pub fn page_title(value: impl Into<String>) -> TextBlock {
    text(value, style::PAGE_TITLE).automation_heading_level(AutomationHeadingLevel::Level2)
}

/// A section, card, panel or conversation title.
pub fn section_title(value: impl Into<String>) -> TextBlock {
    text(value, style::SECTION_TITLE).automation_heading_level(AutomationHeadingLevel::Level3)
}

/// A sidebar section label ("Projects", "Recents"): secondary text.
pub fn section_label(value: impl Into<String>) -> TextBlock {
    secondary(value).automation_heading_level(AutomationHeadingLevel::Level3)
}

/// A card on the surface: surface-container-low, the container corner
/// (call `.content(..)`).
pub fn card(palette: Palette) -> Border {
    Border::new()
        .background(palette.card())
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::uniform(spacing::MD))
}

/// A card on the sidebar-toned panel: surface-container-high.
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

/// A status dot.
pub fn dot(fill: Brush) -> View {
    Ellipse::new()
        .fill(fill)
        .width(layout::DOT)
        .height(layout::DOT)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// An Agent state (its code picks the dot's tone) with its label in
/// secondary text (no container, so a card holds no extra shape).
pub fn status(palette: Palette, state: &str, label: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::XS)
        .vertical_alignment(VerticalAlignment::Center)
        .children((
            dot(palette.header_dot(StatusTone::of_state(state))),
            secondary(label.to_owned()).vertical_alignment(VerticalAlignment::Center),
        ))
}

/// A system icon of `size` DIPs; it takes the foreground of the control
/// around it. Reactor 0.100.0's `FontIcon` has no font size (WinUI draws 20
/// DIPs), so a `Viewbox` scales it.
fn icon_sized(glyph: &str, size: f64) -> View {
    Viewbox::new()
        .width(size)
        .height(size)
        .vertical_alignment(VerticalAlignment::Center)
        .slot(ViewboxSlot::Child, FontIcon::new().glyph(glyph.to_owned()))
}

/// The icon of rows, chips and labelled buttons.
pub fn icon(glyph: &str) -> View {
    icon_sized(glyph, icon_size::ROW)
}

/// The icon of toolbar buttons (the conversation and panel headers).
pub fn toolbar_icon(glyph: &str) -> View {
    icon_sized(glyph, icon_size::TOOLBAR)
}

/// Icon and label side by side: a labelled button, chip or row.
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

/// A button's label.
pub fn label(value: &str) -> TextBlock {
    TextBlock::new().text(value.to_owned())
}

fn button(overrides: ResourceOverrides) -> Button {
    Button::new()
        .resource_overrides(overrides)
        .height(height::CONTROL)
}

/// Primary: filled. One per page (Save, Start).
pub fn filled(palette: Palette) -> Button {
    button(palette.filled())
}

/// Secondary: outlined (Validate, Reload, Duplicate, Add).
pub fn outlined(palette: Palette) -> Button {
    button(palette.outlined())
}

/// Tertiary: text (Discard, Edit as TOML, Refresh, Hide).
pub fn text_button(palette: Palette) -> Button {
    button(palette.text_button())
}

/// Destructive: a text button in the error color (Remove, Force stop).
pub fn destructive(palette: Palette) -> Button {
    button(palette.destructive())
}

/// A text button on an error container (on-error-container).
pub fn error_text_button(palette: Palette) -> Button {
    button(palette.error_text_button())
}

/// A square icon button (`.content(icon(..))` or `toolbar_icon(..)` last).
/// Give it an accessible name.
pub fn icon_button(palette: Palette) -> Button {
    button(palette.icon_button())
        .width(height::CONTROL)
        .style(ButtonStyle::Subtle)
}

/// The composer's icon-only Send: the one circle, filled.
pub fn send_button(palette: Palette) -> Button {
    button(palette.filled())
        .width(height::CONTROL)
        .style(ButtonStyle::TextLink)
}

/// A list or navigation row: full width, the selection in
/// secondary-container. Give it [`row_content`] or [`icon_label`].
pub fn row(palette: Palette, selected: bool) -> Button {
    button(palette.row(selected))
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Stretch)
}

/// The one tonal row: the sidebar's New task, the main action of the
/// shell (primary-container, as M3's tonal emphasis).
pub fn tonal_row(palette: Palette) -> Button {
    button(palette.tonal_row())
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Stretch)
}

/// A sidebar sub-row: a task under its project, a Recents entry (its own
/// minimum height: the button style's is the control height).
pub fn sub_row(palette: Palette, selected: bool) -> Button {
    row(palette, selected)
        .height(height::SUB_ROW)
        .min_height(height::SUB_ROW)
}

/// A row's content: an optional icon, the title (trimmed), an optional
/// secondary note and a trailing mark. Absent parts take no column, so no
/// grid gap shifts the title.
pub fn row_content(glyph: Option<&str>, title: &str, note: &str, trailing: View) -> View {
    let mut columns = Vec::new();
    let mut cells: Vec<KeyedView> = Vec::new();
    if let Some(glyph) = glyph {
        columns.push(GridLength::Auto);
        cells.push(KeyedView::new("icon", Border::new().content(icon(glyph))));
    }
    let title_cell = TextBlock::new()
        .text(title.to_owned())
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis);
    if note.is_empty() {
        columns.push(GridLength::STAR);
        cells.push(KeyedView::new(
            "title",
            title_cell.grid_column(cells.len() as i32),
        ));
    } else {
        columns.extend([GridLength::Auto, GridLength::STAR]);
        cells.push(KeyedView::new(
            "title",
            title_cell.grid_column(cells.len() as i32),
        ));
        cells.push(KeyedView::new(
            "note",
            secondary(note.to_owned())
                .grid_column(cells.len() as i32)
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
        ));
    }
    columns.push(GridLength::Auto);
    cells.push(KeyedView::new(
        "trailing",
        Border::new()
            .grid_column(cells.len() as i32)
            .vertical_alignment(VerticalAlignment::Center)
            .content(trailing),
    ));
    Grid::new()
        .columns(columns)
        .column_spacing(spacing::SM)
        .keyed_children(cells)
}

/// A card that is one button (monitor cards): the card's container clips a
/// full-size button, so its state layers take the container corner.
pub fn card_button(
    palette: Palette,
    name: String,
    open: Callback<()>,
    content: impl Into<View>,
) -> View {
    Border::new()
        .background(palette.card_high())
        .corner_radius(radius::CONTAINER)
        .content(
            Button::new()
                .resource_overrides(palette.card_button())
                .horizontal_alignment(HorizontalAlignment::Stretch)
                .horizontal_content_alignment(HorizontalAlignment::Stretch)
                .automation_name(name)
                .on_click(open)
                .content(content),
        )
}

/// An assist chip that opens a menu (`.content(icon_label(..))` last, then
/// `.menu(..)`): outlined, the control corner.
pub fn chip(palette: Palette) -> Button {
    button(palette.chip())
}

/// The read-only form of [`chip`]: the same outline and label, no button.
pub fn chip_static(palette: Palette, glyph: &str, label: &str) -> View {
    Border::new()
        .height(height::CONTROL)
        .corner_radius(radius::CONTROL)
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .border_brush(palette.outline_variant())
        .padding(Thickness::xy(spacing::INSET, 0.0))
        .content(
            Border::new()
                .opacity(theme::MUTED_OPACITY)
                .vertical_alignment(VerticalAlignment::Center)
                .content(icon_label(glyph, label)),
        )
}

/// One segment of a segmented button (see [`segmented`]).
pub fn segment(palette: Palette, selected: bool) -> Button {
    button(palette.segment(selected))
}

/// The content of a segment: its label (the fill marks the selection, so
/// the segments keep their widths when it moves).
pub fn segment_label(label: &str) -> View {
    TextBlock::new().text(label.to_owned()).into()
}

/// A segmented button: the segments on one filled track of the control
/// height and corner.
pub fn segmented(palette: Palette, segments: Vec<KeyedView>) -> View {
    Border::new()
        .height(height::CONTROL)
        .corner_radius(radius::CONTROL)
        .background(palette.well())
        .horizontal_alignment(HorizontalAlignment::Left)
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

/// A labelled form row: the label column, the control (sized by its
/// caller: enum dropdowns [`layout::ENUM_WIDTH`], numbers
/// [`layout::NUMBER_WIDTH`], paths and text the full width), an optional
/// action.
pub fn form_row(label: &str, control: View, action: Option<View>) -> View {
    // No action, no column: an empty column would still take the grid gap
    // and end the control short of the group's edge.
    let mut columns = vec![GridLength::Pixel(layout::LABEL_WIDTH), GridLength::STAR];
    let mut cells = vec![
        KeyedView::new(
            "label",
            body(label.to_owned())
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
        ),
        KeyedView::new(
            "control",
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .content(control),
        ),
    ];
    if let Some(action) = action {
        columns.push(GridLength::Auto);
        cells.push(KeyedView::new(
            "action",
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .content(action),
        ));
    }
    Grid::new()
        .min_height(height::FORM_ROW)
        .columns(columns)
        .column_spacing(spacing::SM)
        .keyed_children(cells)
}

/// A form field that opens a list as a popover (the preset Model field):
/// the dropdowns' width, outline and corner; [`field_button_content`]
/// puts the value at the left and their chevron at the right.
pub fn field_button(palette: Palette) -> Button {
    button(palette.field_button())
        .width(layout::ENUM_WIDTH)
        .horizontal_alignment(HorizontalAlignment::Left)
        .horizontal_content_alignment(HorizontalAlignment::Stretch)
}

/// A [`field_button`]'s value and chevron.
pub fn field_button_content(value: &str) -> View {
    Grid::new()
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::SM)
        .children((
            TextBlock::new()
                .text(value.to_owned())
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .content(icon_sized(glyph::CHEVRON_DOWN, icon_size::CHEVRON)),
        ))
}

/// A dropdown sized for an enum value.
pub fn enum_width(combo: ComboBox) -> ComboBox {
    combo
        .width(layout::ENUM_WIDTH)
        .horizontal_alignment(HorizontalAlignment::Left)
}

/// Help text under a form row's control.
pub fn help(text: impl Into<String>) -> TextBlock {
    secondary(text)
        .text_wrapping(TextWrapping::Wrap)
        .margin(Thickness::new(
            layout::LABEL_WIDTH + spacing::SM,
            0.0,
            0.0,
            spacing::SM,
        ))
}

/// Rows in one card (surface-container-low, the container corner).
pub fn group(palette: Palette, rows: Vec<KeyedView>) -> View {
    Border::new()
        .background(palette.card())
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::xy(spacing::MD, spacing::SM))
        .vertical_alignment(VerticalAlignment::Top)
        .content(StackPanel::new().keyed_children(rows))
}

/// A list of selectable rows in one card: the rows reach 8 DIPs from the
/// card's edges, so their highlight keeps an even inset.
pub fn list(palette: Palette, rows: Vec<KeyedView>) -> View {
    Border::new()
        .background(palette.card())
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::uniform(spacing::SM))
        .vertical_alignment(VerticalAlignment::Top)
        .content(StackPanel::new().keyed_children(rows))
}

/// A section title with its supporting line and an optional action on the
/// right, 16 DIPs above the group it heads.
pub fn section_heading(title: &str, supporting: &str, action: Option<View>) -> View {
    Grid::new()
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::SM)
        .children((
            StackPanel::new()
                .spacing(spacing::XS)
                .vertical_alignment(VerticalAlignment::Center)
                .children((
                    section_title(title.to_owned()),
                    if supporting.is_empty() {
                        View::empty()
                    } else {
                        secondary(supporting.to_owned())
                            .text_wrapping(TextWrapping::Wrap)
                            .into()
                    },
                )),
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .content(action.unwrap_or_else(View::empty)),
        ))
}

/// A divider.
pub fn divider(palette: Palette) -> Border {
    Border::new()
        .height(layout::HAIRLINE)
        .background(palette.outline_variant())
}

/// A settings page's header (the Codex settings pages): the title, its
/// one-line subtitle and an optional action on the right (a link).
pub fn page_header(title: &str, subtitle: &str, action: Option<View>) -> View {
    Grid::new()
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::SM)
        .children((
            StackPanel::new().spacing(spacing::XS).children((
                page_title(title.to_owned()),
                secondary(subtitle.to_owned()).text_wrapping(TextWrapping::Wrap),
            )),
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Bottom)
                .content(action.unwrap_or_else(View::empty)),
        ))
}

/// The title above a settings card ("Permissions"), 16 DIPs below the
/// card above it.
pub fn card_title(title: &str) -> View {
    section_title(title.to_owned())
        .margin(Thickness::new(0.0, spacing::SM, 0.0, 0.0))
        .into()
}

/// One settings row (the Codex cards): the name and its one-line
/// description on the left, the control on the right.
pub fn setting_row(title: &str, description: &str, control: View) -> View {
    Grid::new()
        .min_height(height::FORM_ROW)
        .margin(Thickness::xy(0.0, spacing::SM))
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::MD)
        .children((
            StackPanel::new()
                .vertical_alignment(VerticalAlignment::Center)
                .children((
                    body(title.to_owned()).text_trimming(TextTrimming::CharacterEllipsis),
                    if description.is_empty() {
                        View::empty()
                    } else {
                        secondary(description.to_owned())
                            .text_wrapping(TextWrapping::Wrap)
                            .into()
                    },
                )),
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .content(control),
        ))
}

/// Settings rows in one card, a hairline between rows.
pub fn settings_card(palette: Palette, rows: Vec<KeyedView>) -> View {
    let mut children = Vec::with_capacity(rows.len() * 2);
    for (index, row) in rows.into_iter().enumerate() {
        if index > 0 {
            children.push(KeyedView::new(format!("divider:{index}"), divider(palette)));
        }
        children.push(row);
    }
    Border::new()
        .background(palette.card())
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::xy(spacing::MD, spacing::XS))
        .vertical_alignment(VerticalAlignment::Top)
        .content(StackPanel::new().keyed_children(children))
}

/// The content of a link-style text button ("Open config.toml ↗"): the
/// label and its trailing glyph.
pub fn link_content(label: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::XS)
        .children((
            TextBlock::new()
                .text(label.to_owned())
                .vertical_alignment(VerticalAlignment::Center),
            icon(glyph::EXTERNAL),
        ))
}

/// The container of a popover above the composer (the M3 menu: a
/// surface-container card with a hairline outline and the container
/// corner).
pub fn popover(palette: Palette) -> Border {
    Border::new()
        .background(palette.popover())
        .border_brush(palette.outline_variant())
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::uniform(spacing::XS))
}

/// A small outlined badge ("default").
pub fn badge(palette: Palette, text: &str) -> View {
    Border::new()
        .border_brush(palette.outline_variant())
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .corner_radius(radius::CONTROL)
        .padding(Thickness::xy(spacing::XS, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .content(secondary(text.to_owned()))
}

/// A quota ring: the share remaining (0–100) as a determinate ring.
pub fn ring(remaining: f64) -> View {
    ProgressRing::new()
        .is_indeterminate(false)
        .minimum(0.0)
        .maximum(100.0)
        .value(remaining.clamp(0.0, 100.0))
        .width(layout::RING)
        .height(layout::RING)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}
