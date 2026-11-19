//! The desktop's components, built once from the approved design
//! (`System.dc.html`, `Main.dc.html`, `Conversation.dc.html`): nav row,
//! project row, nested task row, section label, search field, chip,
//! segmented button, icon button, FAB, tool row, message bubble, composer
//! card, pane tabs and list row, and the form and settings pieces on the
//! same tokens. Every size comes from [`crate::ui::tokens`], every color
//! from the theme's roles.
//!
//! Buttons are real WinUI `Button`s whose colors come from their family's
//! resource overrides (`Palette`), and whose shape, states, ripple, lift
//! and motion come from their style's template (`app_resources`): the
//! implicit style for rows, chips, segments and labelled buttons (flat
//! pills, a 10-DIP inset), `ButtonStyle::Subtle` for icon buttons,
//! `ButtonStyle::TextLink` for the FAB and `ButtonStyle::Accent` for card
//! pressables (suggestion cards, tool rows).
//!
//! The window shows one palette at a time, so text that only needs a role
//! (secondary text, hints, section labels) reads it from [`set_palette`],
//! which the shell sets before every render: Reactor has no context, and
//! threading the palette through every caller of a caption would change
//! nothing but signatures.

use std::cell::Cell;

use windows_reactor::*;

use crate::theme::winui::Palette;
use crate::theme::{self, StatusTone};
use crate::ui::tokens::text::{self as style, TypeStyle};
use crate::ui::tokens::{height, icon as icon_size, layout, radius, spacing};

thread_local! {
    static PALETTE: Cell<Option<Palette>> = const { Cell::new(None) };
}

/// The palette of the frame being built (the shell sets it first).
pub fn set_palette(palette: Palette) {
    PALETTE.with(|slot| slot.set(Some(palette)));
}

fn palette() -> Palette {
    PALETTE
        .with(Cell::get)
        .unwrap_or_else(|| Palette::new(theme::Theme::Light, false))
}

/// Segoe Fluent Icons code points (the system icon font, one stroke family;
/// no icon pack).
pub mod glyph {
    /// New task: the design's plus.
    pub const NEW_TASK: &str = "\u{E710}";
    pub const SEARCH: &str = "\u{E721}";
    pub const FOLDER: &str = "\u{E8B7}";
    pub const SETTINGS: &str = "\u{E713}";
    /// A filled square (Segoe Fluent E978): the outline Stop glyph read as
    /// a check box (owner review 2026-11-19).
    pub const STOP: &str = "\u{E978}";
    pub const MORE: &str = "\u{E712}";
    pub const INFO: &str = "\u{E946}";
    pub const CLOSE: &str = "\u{E711}";
    pub const BACK: &str = "\u{E76B}";
    pub const FORWARD: &str = "\u{E76C}";
    /// Segoe Fluent ClosePane and OpenPane: hide and show the sidebar.
    pub const HIDE_SIDEBAR: &str = "\u{E89F}";
    pub const SHOW_SIDEBAR: &str = "\u{E8A0}";
    pub const CHEVRON_RIGHT: &str = "\u{E76C}";
    pub const CHEVRON_DOWN: &str = "\u{E70D}";
    pub const ARROW_DOWN: &str = "\u{E74B}";
    pub const ARROW_UP: &str = "\u{E74A}";
    pub const COPY: &str = "\u{E8C8}";
    pub const MODEL: &str = "\u{E945}";
    pub const SHIELD: &str = "\u{EA18}";
    pub const PULSE: &str = "\u{E9D9}";
    pub const TOOLS: &str = "\u{E90F}";
    /// A command run: Segoe Fluent CommandPrompt.
    pub const TERMINAL: &str = "\u{E756}";
    /// Edited files: Segoe Fluent Edit.
    pub const EDIT: &str = "\u{E70F}";
    pub const CHECK: &str = "\u{E73E}";
    pub const STAR: &str = "\u{E734}";
    pub const STAR_FILLED: &str = "\u{E735}";
    pub const LOCK: &str = "\u{E72E}";
    pub const WARNING: &str = "\u{E7BA}";
    pub const AUTO: &str = "\u{E99A}";
    pub const AUTOMATION: &str = "\u{E823}";
    pub const USAGE: &str = "\u{E9D2}";
    pub const EXTERNAL: &str = "\u{E8A7}";
    pub const OPEN_FOLDER: &str = "\u{E838}";
    pub const KEYBOARD: &str = "\u{E765}";
    pub const ROUTING: &str = "\u{E8AB}";
    pub const WORKTREE: &str = "\u{E8F1}";
    pub const CLOUD: &str = "\u{E753}";
    pub const CODE: &str = "\u{E943}";
    /// Manage models (OpenCode's sliders button): Segoe Fluent Equalizer.
    pub const SLIDERS: &str = "\u{E9E9}";
    pub const CHAT: &str = "\u{E8BD}";
    pub const ARCHIVE: &str = "\u{E7B8}";
    pub const DELETE: &str = "\u{E74D}";
    /// Move under another task: Segoe Fluent MoveToFolder.
    pub const MOVE: &str = "\u{E8DE}";
    /// Resume a lost task: Segoe Fluent Play.
    pub const RESUME: &str = "\u{E768}";
    pub const RENAME: &str = "\u{E8AC}";
    /// Tags: Segoe Fluent Tag.
    pub const TAG: &str = "\u{E8EC}";
    /// Review: Segoe Fluent BulletedList.
    pub const LIST: &str = "\u{E8FD}";
}

pub fn weight(style: TypeStyle) -> FontWeight {
    FontWeight::new(style.weight).unwrap_or(FontWeight::NORMAL)
}

/// Text in one of the design's type roles.
pub fn text(value: impl Into<String>, style: TypeStyle) -> TextBlock {
    TextBlock::new()
        .text(value)
        .font_size(style.size)
        .font_weight(weight(style))
}

/// Text in the muted role.
pub fn muted(value: impl Into<String>, style: TypeStyle) -> TextBlock {
    text(value, style).foreground(palette().muted())
}

/// Body text: messages, labels, values.
pub fn body(value: impl Into<String>) -> TextBlock {
    text(value, style::BODY)
}

/// Secondary text: notes, hints, counts, supporting lines (caption, muted).
pub fn secondary(value: impl Into<String>) -> TextBlock {
    muted(value, style::CAPTION)
}

/// A page title (a Settings page, the home greeting).
pub fn page_title(value: impl Into<String>) -> TextBlock {
    text(value, style::HERO).automation_heading_level(AutomationHeadingLevel::Level2)
}

/// A section, card, panel or conversation title.
pub fn section_title(value: impl Into<String>) -> TextBlock {
    text(value, style::HEADER).automation_heading_level(AutomationHeadingLevel::Level3)
}

/// A section label ("Projects", "Chats"): 12/500 muted, on a 32-DIP row at
/// the rows' inset, 12 DIPs below the group above.
pub fn section_label(value: impl Into<String>) -> TextBlock {
    muted(value, style::LABEL).automation_heading_level(AutomationHeadingLevel::Level3)
}

/// The sidebar's section label row: the label at the rows' content edge.
pub fn section_row(value: &str) -> View {
    Border::new()
        .height(height::CONTROL)
        .padding(Thickness::xy(spacing::ROW, 0.0))
        .margin(Thickness::new(0.0, spacing::INSET, 0.0, 0.0))
        .content(
            muted(value.to_owned(), style::LABEL)
                .automation_heading_level(AutomationHeadingLevel::Level3)
                .vertical_alignment(VerticalAlignment::Center),
        )
}

/// A card on the surface: the tonal tone, the container corner (call
/// `.content(..)`).
pub fn card(palette: Palette) -> Border {
    Border::new()
        .background(palette.tonal())
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::uniform(spacing::MD))
}

/// A card on a tonal panel: the container role.
pub fn card_high(palette: Palette) -> Border {
    card(palette).background(palette.container())
}

/// The status dot of an Agent state in a list; empty for states that need
/// no mark.
pub fn list_dot(palette: Palette, state: eliot_core::AgentState) -> View {
    match palette.dot(StatusTone::of_state(state)) {
        Some(fill) => dot(fill),
        None => View::empty(),
    }
}

/// An 8-DIP status dot.
pub fn dot(fill: Brush) -> View {
    Ellipse::new()
        .fill(fill)
        .width(layout::DOT)
        .height(layout::DOT)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// A system icon of `size` DIPs; it takes the foreground of the control
/// around it. Reactor 0.100.0's `FontIcon` has no font size (WinUI draws 20
/// DIPs), so a `Viewbox` scales it.
fn icon_sized(glyph: &str, size: f64) -> View {
    // Stop is the composer's rounded square at the icons' optical weight
    // (the font's solid square outweighs the stroke icons beside it).
    if glyph == glyph::STOP {
        return Border::new()
            .width(size)
            .height(size)
            .vertical_alignment(VerticalAlignment::Center)
            .content(
                Border::new()
                    .width(icon_size::STOP - 2.0)
                    .height(icon_size::STOP - 2.0)
                    .corner_radius(radius::STOP)
                    .background(palette().text())
                    .horizontal_alignment(HorizontalAlignment::Center)
                    .vertical_alignment(VerticalAlignment::Center),
            );
    }
    Viewbox::new()
        .width(size)
        .height(size)
        .vertical_alignment(VerticalAlignment::Center)
        .slot(ViewboxSlot::Child, FontIcon::new().glyph(glyph.to_owned()))
}

/// The 18-DIP icon of rows and icon buttons.
pub fn icon(glyph: &str) -> View {
    icon_sized(glyph, icon_size::ROW)
}

/// The 16-DIP icon of chips, tool rows and fields.
pub fn small_icon(glyph: &str) -> View {
    icon_sized(glyph, icon_size::SMALL)
}

/// A chip's chevron (a chip or field that opens a list).
pub fn dropdown_chevron() -> View {
    icon_sized(glyph::CHEVRON_DOWN, icon_size::CHEVRON)
}

/// Icon and label side by side, 10 DIPs apart: a row or menu entry.
pub fn icon_label(glyph: &str, label: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::ROW)
        .children((
            icon(glyph),
            TextBlock::new()
                .text(label.to_owned())
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
        ))
}

/// A labelled button's label (14/500), 16 DIPs from the pill's ends.
pub fn label(value: &str) -> TextBlock {
    text(value.to_owned(), style::BUTTON).margin(Thickness::xy(spacing::MD - spacing::ROW, 0.0))
}

/// `view` lifted off its surface at `level`: the theme's lift (Material's
/// key and ambient shadows, or the true-black themes' ring) drawn under it
/// on a corner of `radius` (WinUI's `ThemeShadow` draws nothing outside a
/// popup without receivers, which Reactor 0.100.0 cannot add). `place` is
/// the grid that sizes and places it, so the lift follows the view.
pub fn lifted(
    palette: Palette,
    level: crate::theme::elevation::Level,
    radius: f64,
    place: Grid,
    view: impl Into<View>,
) -> View {
    let Some(roles) = palette.roles() else {
        return place.keyed_children(vec![KeyedView::new("content", view.into())]);
    };
    let tint = match roles.lift {
        crate::theme::Lift::Shadow { .. } => crate::theme::Rgb8::new(0, 0, 0),
        crate::theme::Lift::Ring(color) => color,
    };
    let mut cells: Vec<KeyedView> = crate::theme::elevation::layers(level, roles.lift)
        .into_iter()
        .enumerate()
        .map(|(index, layer)| {
            KeyedView::new(
                format!("lift:{index}"),
                Border::new()
                    .corner_radius(radius + layer.spread)
                    .margin(Thickness::new(
                        -layer.spread,
                        layer.offset - layer.spread,
                        -layer.spread,
                        -layer.offset - layer.spread,
                    ))
                    .background(Brush::Solid(Color::argb(
                        layer.alpha,
                        tint.r,
                        tint.g,
                        tint.b,
                    ))),
            )
        })
        .collect();
    cells.push(KeyedView::new("content", view.into()));
    place.keyed_children(cells)
}

fn button(overrides: ResourceOverrides) -> Button {
    Button::new()
        .resource_overrides(overrides)
        .height(height::CONTROL)
}

/// Filled: the one main action of a page.
pub fn filled(palette: Palette) -> Button {
    button(palette.filled())
}

/// Outlined (Validate, Reload, Duplicate, Add).
pub fn outlined(palette: Palette) -> Button {
    button(palette.outlined())
}

/// Text (Discard, Edit as TOML, Refresh, Hide).
pub fn text_button(palette: Palette) -> Button {
    button(palette.text_button())
}

/// Destructive: a text button in the attention color (Remove, Force stop).
pub fn destructive(palette: Palette) -> Button {
    button(palette.destructive())
}

/// An icon button: a 32-DIP circle with an 18-DIP icon
/// (`.content(icon(..))` last). Give it an accessible name.
pub fn icon_button(palette: Palette) -> Button {
    button(palette.icon_button())
        .width(height::CONTROL)
        .min_height(height::CONTROL)
        .style(ButtonStyle::Subtle)
}

/// The composer's FAB (Send, Stop, Steer): a raised 36-DIP circle.
pub fn send_button(palette: Palette) -> Button {
    Button::new()
        .resource_overrides(palette.filled())
        .width(height::FAB)
        .height(height::FAB)
        .style(ButtonStyle::TextLink)
}

/// Send's glyph: the design's up arrow, centred in the circle.
pub fn send_icon() -> View {
    icon(glyph::ARROW_UP)
}

/// Stop's mark: a filled 12-DIP square in the on-primary role.
pub fn stop_mark(palette: Palette) -> View {
    Border::new()
        .width(icon_size::STOP)
        .height(icon_size::STOP)
        .corner_radius(radius::STOP)
        .background(palette.on_primary())
        .horizontal_alignment(HorizontalAlignment::Center)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// A nav, project, task or list row: a full-width 32-DIP pill, the
/// selection in the selected role (hover 8 %, press 12 %). Give it
/// [`row_content`], [`nav_content`], [`project_content`] or
/// [`task_content`].
pub fn row(palette: Palette, selected: bool) -> Button {
    button(palette.row(selected))
        .min_height(height::CONTROL)
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Stretch)
}

/// A row under its parent (same height as every row).
pub fn sub_row(palette: Palette, selected: bool) -> Button {
    row(palette, selected)
}

/// A row's content: an optional 18-DIP icon, the title (trimmed), an
/// optional caption note and a trailing mark. Absent parts take no
/// column, so no gap shifts the title.
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
    columns.push(if note.is_empty() {
        GridLength::STAR
    } else {
        GridLength::Auto
    });
    cells.push(KeyedView::new(
        "title",
        title_cell.grid_column(cells.len() as i32),
    ));
    if !note.is_empty() {
        columns.push(GridLength::STAR);
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
        .column_spacing(spacing::ROW)
        .keyed_children(cells)
}

/// A nav row's content: its icon, its label and a trailing caption
/// ("Ctrl+N").
pub fn nav_content(glyph: &str, label: &str, hint: &str) -> View {
    row_content(
        Some(glyph),
        label,
        "",
        if hint.is_empty() {
            View::empty()
        } else {
            secondary(hint.to_owned()).into()
        },
    )
}

/// A project row's content: the folder, the name at 14/500 and a trailing
/// view (its count).
pub fn project_content(name: &str, trailing: View) -> View {
    Grid::new()
        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::ROW)
        .children((
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(icon(glyph::FOLDER)),
            text(name.to_owned(), style::BUTTON)
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .content(trailing),
        ))
}

/// A nested row's content: indented `depth` steps of 28 DIPs, so its title
/// starts under its parent's label; ended tasks in the variant tone; a
/// trailing view (a dot, an age).
pub fn task_content(depth: usize, title: &str, ended: bool, trailing: View) -> View {
    let title = TextBlock::new()
        .text(title.to_owned())
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis);
    let title = if ended {
        title.foreground(palette().variant())
    } else {
        title
    };
    Grid::new()
        .margin(Thickness::new(
            layout::TREE_INDENT * depth as f64,
            0.0,
            0.0,
            0.0,
        ))
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::ROW)
        .children((
            title,
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .content(trailing),
        ))
}

/// A chip: 28 DIPs, outlined in the border tone (`.content(chip_content(..))`
/// last, then a menu or click).
pub fn chip(palette: Palette) -> Button {
    Button::new()
        .resource_overrides(palette.chip())
        .height(height::CHIP)
        .min_height(height::CHIP)
}

/// A chip in the attention color (Full access).
pub fn attention_chip(palette: Palette) -> Button {
    chip(palette).resource_overrides(palette.attention_chip())
}

/// A chip's content: an optional 16-DIP icon, the 13-DIP label (trimmed at
/// `max`), an optional chevron, 6 DIPs apart.
pub fn chip_content(glyph: Option<&str>, label: &str, chevron: bool, max: f64) -> View {
    let mut parts: Vec<KeyedView> = Vec::new();
    if let Some(glyph) = glyph {
        parts.push(KeyedView::new("icon", small_icon(glyph)));
    }
    parts.push(KeyedView::new(
        "label",
        text(label.to_owned(), style::CHIP)
            .vertical_alignment(VerticalAlignment::Center)
            .max_width(max)
            .text_trimming(TextTrimming::CharacterEllipsis),
    ));
    if chevron {
        parts.push(KeyedView::new("chevron", dropdown_chevron()));
    }
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(6.0)
        .keyed_children(parts)
}

/// A borderless chip (the composer's project selector, a menu entry): the
/// text color, a state layer on hover, no outline.
pub fn text_chip(palette: Palette) -> Button {
    button(palette.row(false))
}

/// Read-only context: a 16-DIP icon and a caption in the muted role. No
/// outline and no button, so it never reads as a control.
pub fn meta(glyph: &str, label: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(6.0)
        .vertical_alignment(VerticalAlignment::Center)
        .automation_name(label.to_owned())
        .children((
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(small_icon(glyph)),
            muted(label.to_owned(), style::CHIP)
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
        ))
}

/// One segment of a segmented button (see [`segmented`]): 28 DIPs, the
/// selection filled in the selected role.
pub fn segment(palette: Palette, selected: bool) -> Button {
    Button::new()
        .resource_overrides(palette.segment(selected))
        .height(height::CHIP)
        .min_height(height::CHIP)
}

/// The content of a segment: its 13-DIP label (the fill marks the
/// selection, so the segments keep their widths when it moves).
pub fn segment_label(label: &str) -> View {
    text(label.to_owned(), style::CHIP)
        .margin(Thickness::xy(spacing::INSET - spacing::ROW, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// A segmented button: the segments inside one outlined 28-DIP pill.
pub fn segmented(palette: Palette, segments: Vec<KeyedView>) -> View {
    Border::new()
        .height(height::CHIP + 2.0 * layout::HAIRLINE)
        .corner_radius(radius::CHIP + layout::HAIRLINE)
        .border_brush(palette.border())
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .horizontal_alignment(HorizontalAlignment::Left)
        .content(
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .keyed_children(segments),
        )
}

/// A pane tab (Monitor, Details): a 32-DIP pill, the selection in the
/// selected role (`.content(pane_tab_label(..))` last).
pub fn pane_tab(palette: Palette, selected: bool) -> Button {
    button(palette.segment(selected))
}

/// A pane tab's label: 13/500, 14 DIPs from the pill's ends.
pub fn pane_tab_label(label: &str) -> View {
    text(label.to_owned(), style::LABEL)
        .font_size(style::CHIP.size)
        .margin(Thickness::xy(14.0 - spacing::ROW, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// The search field of a pane: a 32-DIP pill in the tonal tone, a 16-DIP
/// icon and a borderless field (13 DIPs) 8 DIPs after it.
pub fn search_field(fill: Brush, field: TextBox) -> View {
    // WinUI's text box insets its text by 10 DIPs; the margin puts the text
    // 8 DIPs after the icon, as the design does.
    const TEXT_BOX_INSET: f64 = 10.0;
    Border::new()
        .height(height::CONTROL)
        .corner_radius(radius::PILL)
        .background(fill)
        .padding(Thickness::new(spacing::INSET, 0.0, spacing::XS, 0.0))
        .content(
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR])
                .column_spacing(spacing::SM - TEXT_BOX_INSET)
                .children((
                    Border::new()
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(small_icon(glyph::SEARCH)),
                    field
                        .grid_column(1)
                        .background(Brush::Solid(Color::transparent()))
                        .border_thickness(Thickness::uniform(0.0))
                        .vertical_alignment(VerticalAlignment::Center),
                )),
        )
}

/// A tool row: 32 DIPs, the card corner, tonal (running: the running
/// tint), lifting under the pointer. Give it [`tool_content`].
pub fn tool_row(palette: Palette, running: bool) -> Button {
    Button::new()
        .resource_overrides(palette.tool_row(running))
        .style(ButtonStyle::Accent)
        .height(height::CONTROL)
        .min_height(height::CONTROL)
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Stretch)
}

/// A tool row's content: a 16-DIP icon, the 13-DIP label, a trailing
/// caption (a duration; the status in `tone` while it runs) and a chevron.
pub fn tool_content(glyph: &str, label: &str, trailing: &str, tone: Option<Brush>) -> View {
    let trailing = text(trailing.to_owned(), style::CAPTION)
        .grid_column(2)
        .vertical_alignment(VerticalAlignment::Center);
    let trailing = trailing.foreground(tone.unwrap_or_else(|| palette().muted()));
    Grid::new()
        .columns([
            GridLength::Auto,
            GridLength::STAR,
            GridLength::Auto,
            GridLength::Auto,
        ])
        .column_spacing(spacing::ROW)
        .children((
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(small_icon(glyph)),
            text(label.to_owned(), style::CHIP)
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
            trailing,
            Border::new()
                .grid_column(3)
                .vertical_alignment(VerticalAlignment::Center)
                .content(icon_sized(glyph::CHEVRON_RIGHT, icon_size::CHEVRON)),
        ))
}

/// A suggestion card (home): the card corner, tonal, lifting under the
/// pointer (`.content(suggestion_content(..))` last). Its content color is
/// the primary role, which its icon takes.
pub fn suggestion(palette: Palette) -> Button {
    Button::new()
        .resource_overrides(palette.suggestion())
        .style(ButtonStyle::Accent)
        .min_height(44.0)
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Left)
}

/// A suggestion card's content: the 18-DIP icon (in the card's primary
/// content color) and its 14-DIP label in the text role, 12/14 inset.
pub fn suggestion_content(palette: Palette, glyph: &str, label: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::ROW)
        .margin(Thickness::xy(14.0 - spacing::INSET, spacing::INSET))
        .children((
            icon(glyph),
            body(label.to_owned())
                .foreground(palette.text())
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
        ))
}

/// A message bubble: the user's in the container role, right-aligned with
/// the tail at the bottom right (18, 18, 4, 18), 10/14 padding, 14/20.
pub fn bubble(palette: Palette, content: impl Into<View>, max_width: Option<f64>) -> View {
    Border::new()
        .background(palette.container())
        .corner_radius(CornerRadius::new(
            radius::BUBBLE,
            radius::BUBBLE,
            radius::BUBBLE_TAIL,
            radius::BUBBLE,
        ))
        .padding(Thickness::xy(14.0, spacing::ROW))
        .horizontal_alignment(HorizontalAlignment::Right)
        .max_width(max_width)
        .content(content)
}

/// A steered message: outlined in the placeholder tone instead of filled,
/// "Steer:" before the text, in the variant tone.
pub fn steer_bubble(palette: Palette, text_value: &str, max_width: Option<f64>) -> View {
    Border::new()
        .border_brush(palette.muted())
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .corner_radius(CornerRadius::new(
            radius::BUBBLE,
            radius::BUBBLE,
            radius::BUBBLE_TAIL,
            radius::BUBBLE,
        ))
        .padding(Thickness::xy(14.0, spacing::ROW))
        .horizontal_alignment(HorizontalAlignment::Right)
        .max_width(max_width)
        .content(
            body(format!("Steer: {text_value}"))
                .foreground(palette.variant())
                .text_wrapping(TextWrapping::Wrap)
                .is_text_selection_enabled(true),
        )
}

/// The composer card: the composer role, the 20-DIP corner, lifted to
/// level 3, padded 12/12/10/16. `place` sizes it.
pub fn composer_card(palette: Palette, place: Grid, content: impl Into<View>) -> View {
    lifted(
        palette,
        crate::theme::elevation::Level::Three,
        radius::COMPOSER,
        place,
        Border::new()
            .background(palette.composer())
            .corner_radius(radius::COMPOSER)
            .padding(Thickness::new(
                spacing::MD,
                spacing::INSET,
                spacing::INSET,
                spacing::ROW,
            ))
            .content(content),
    )
}

/// The header's status pill: 24 DIPs, the tone's container, a 6-DIP dot
/// and a caption.
pub fn status_pill(palette: Palette, tone: StatusTone, label: &str) -> View {
    let (container, on) = palette.status_chip(tone);
    Border::new()
        .height(height::BADGE)
        .corner_radius(radius::BADGE)
        .background(container)
        .padding(Thickness::xy(spacing::ROW, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .automation_name(label.to_owned())
        .content(
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(6.0)
                .children((
                    Ellipse::new()
                        .fill(palette.header_dot(tone))
                        .width(layout::BADGE_DOT)
                        .height(layout::BADGE_DOT)
                        .vertical_alignment(VerticalAlignment::Center),
                    text(label.to_owned(), style::CAPTION)
                        .foreground(on)
                        .vertical_alignment(VerticalAlignment::Center)
                        .max_width(layout::RUNS_LABEL_WIDTH)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                )),
        )
}

/// A text field (the application's text-box style).
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
/// the dropdowns' width and outline; [`field_button_content`] puts the
/// value at the left and the chevron at the right.
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
                .content(dropdown_chevron()),
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

/// Rows in one card (tonal, the container corner).
pub fn group(palette: Palette, rows: Vec<KeyedView>) -> View {
    Border::new()
        .background(palette.tonal())
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::xy(spacing::MD, spacing::SM))
        .vertical_alignment(VerticalAlignment::Top)
        .content(StackPanel::new().keyed_children(rows))
}

/// A list of selectable rows in one card: the rows reach 8 DIPs from the
/// card's edges, so their pills keep an even inset.
pub fn list(palette: Palette, rows: Vec<KeyedView>) -> View {
    Border::new()
        .background(palette.tonal())
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::uniform(spacing::SM))
        .vertical_alignment(VerticalAlignment::Top)
        .content(
            StackPanel::new()
                .spacing(spacing::ROW_GAP)
                .keyed_children(rows),
        )
}

/// A section title with its supporting line and an optional action on the
/// right.
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

/// A hairline divider in the outline role.
pub fn divider(palette: Palette) -> Border {
    Border::new()
        .height(layout::HAIRLINE)
        .background(palette.outline())
}

/// A settings page's header: the title, its one-line subtitle and an
/// optional action on the right (a link).
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

/// The title above a settings card ("Permissions").
pub fn card_title(title: &str) -> View {
    section_title(title.to_owned())
        .margin(Thickness::new(0.0, spacing::SM, 0.0, 0.0))
        .into()
}

/// One settings row: the name and its one-line description on the left,
/// the control on the right.
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
        .background(palette.tonal())
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
        .margin(Thickness::xy(spacing::MD - spacing::ROW, 0.0))
        .children((
            text(label.to_owned(), style::BUTTON).vertical_alignment(VerticalAlignment::Center),
            small_icon(glyph::EXTERNAL),
        ))
}

/// A popover or in-window menu: the composer role, the container corner,
/// a 4-DIP inset (lift it with [`lifted`] at level 2).
pub fn popover(palette: Palette) -> Border {
    Border::new()
        .background(palette.composer())
        .border_brush(palette.outline())
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::uniform(spacing::XS))
}

/// A small outlined badge ("default").
pub fn badge(palette: Palette, text: &str) -> View {
    Border::new()
        .border_brush(palette.border())
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .corner_radius(radius::BADGE)
        .padding(Thickness::xy(spacing::SM, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .content(secondary(text.to_owned()))
}

/// A colored outline marks the quota band, including at zero remaining.
pub fn status_ring(status: Brush) -> View {
    Border::new()
        .width(layout::RING)
        .height(layout::RING)
        .corner_radius(layout::RING / 2.0)
        .border_thickness(Thickness::uniform(2.0))
        .border_brush(status)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}
