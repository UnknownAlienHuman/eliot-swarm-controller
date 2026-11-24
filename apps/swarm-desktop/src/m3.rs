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

/// Material Symbols Rounded code points in the shipped Eliot Symbols font
/// (`crate::fonts`): the outline glyph at its own code point; [`filled`]
/// gives the filled one (a selected or toggled icon).
#[allow(
    dead_code,
    reason = "the brand book's icon set; the screens adopt them one by one"
)]
pub mod glyph {
    /// The filled form of an outline glyph: `0xF0000 + code - 0xE000`.
    pub fn filled(outline: &str) -> String {
        outline
            .chars()
            .map(|c| char::from_u32(0xF0000 + u32::from(c) - 0xE000).unwrap_or(c))
            .collect()
    }

    /// New task: add.
    pub const NEW_TASK: &str = "\u{E145}";
    pub const SEARCH: &str = "\u{EF7A}";
    pub const FOLDER: &str = "\u{E2C7}";
    /// A file (an attachment, a file the answer names): description.
    pub const FILE: &str = "\u{E873}";
    /// A worktree (New worktree, Settings › Worktrees): alt_route.
    pub const WORKTREE: &str = "\u{F184}";
    pub const SETTINGS: &str = "\u{E8B8}";
    /// Stop: the filled square.
    pub const STOP: &str = "\u{F0047}";
    pub const MORE: &str = "\u{E5D3}";
    pub const MORE_VERTICAL: &str = "\u{E5D4}";
    pub const INFO: &str = "\u{E88E}";
    /// The Owner inbox.
    pub const INBOX: &str = "\u{E156}";
    pub const CLOSE: &str = "\u{E5CD}";
    pub const BACK: &str = "\u{E5C4}";
    pub const FORWARD: &str = "\u{E5C8}";
    /// Hide and show the sidebar: left_panel_close and left_panel_open.
    pub const HIDE_SIDEBAR: &str = "\u{F717}";
    pub const SHOW_SIDEBAR: &str = "\u{F716}";
    pub const CHEVRON_RIGHT: &str = "\u{E5CC}";
    pub const CHEVRON_DOWN: &str = "\u{E5CF}";
    pub const CHEVRON_UP: &str = "\u{E5CE}";
    pub const ARROW_DOWN: &str = "\u{E5DB}";
    pub const ARROW_UP: &str = "\u{E5D8}";
    pub const COPY: &str = "\u{E14D}";
    /// A model: memory.
    pub const MODEL: &str = "\u{E322}";
    pub const SHIELD: &str = "\u{E9E0}";
    /// The fleet monitor: monitoring.
    pub const PULSE: &str = "\u{F190}";
    pub const TOOLS: &str = "\u{F8CD}";
    /// A command run.
    pub const TERMINAL: &str = "\u{EB8E}";
    /// Edited files: edit_document.
    pub const EDIT: &str = "\u{F88C}";
    pub const CHECK: &str = "\u{E668}";
    pub const STAR: &str = "\u{F09A}";
    pub const STAR_FILLED: &str = "\u{F109A}";
    pub const LOCK: &str = "\u{E899}";
    pub const WARNING: &str = "\u{F083}";
    /// Auto routing: auto_awesome.
    pub const AUTO: &str = "\u{E65F}";
    /// Automations: bolt.
    pub const AUTOMATION: &str = "\u{EA0B}";
    pub const USAGE: &str = "\u{EFF2}";
    pub const EXTERNAL: &str = "\u{E89E}";
    pub const OPEN_FOLDER: &str = "\u{E2C8}";
    pub const KEYBOARD: &str = "\u{E312}";
    pub const ROUTING: &str = "\u{EACD}";
    pub const CLOUD: &str = "\u{F15C}";
    pub const CODE: &str = "\u{E86F}";
    /// Manage models: tune.
    pub const SLIDERS: &str = "\u{E429}";
    pub const CHAT: &str = "\u{E0C9}";
    pub const ARCHIVE: &str = "\u{E149}";
    pub const DELETE: &str = "\u{E92E}";
    /// Move under another task: drive_file_move.
    pub const MOVE: &str = "\u{E9A1}";
    /// Resume a lost task: play_arrow.
    pub const RESUME: &str = "\u{E037}";
    pub const RENAME: &str = "\u{F097}";
    /// Tags: sell.
    pub const TAG: &str = "\u{F05B}";
    /// Review: list.
    pub const LIST: &str = "\u{E896}";
    /// Workers and the fleet: hub.
    pub const HUB: &str = "\u{E9F4}";
    /// Queued: schedule.
    pub const SCHEDULE: &str = "\u{EFD6}";
    /// Failed, lost, unknown: error.
    pub const ERROR: &str = "\u{F8B6}";
    pub const CHECK_CIRCLE: &str = "\u{F0BE}";
    pub const PENDING: &str = "\u{EF64}";
    /// Steer: subdirectory_arrow_right.
    pub const STEER: &str = "\u{E5DA}";
    /// A provider hold: hourglass_top.
    pub const HOLD: &str = "\u{EA5B}";
    /// A goal: flag.
    pub const GOAL: &str = "\u{F0C6}";
    pub const MERGE: &str = "\u{EB98}";
    pub const REFRESH: &str = "\u{E5D5}";
    pub const SNOOZE: &str = "\u{E046}";
    pub const FILTER: &str = "\u{E152}";
    pub const WRAP: &str = "\u{E25B}";
    pub const FULL_VIEW: &str = "\u{F1CE}";
    pub const WIDTH: &str = "\u{F730}";
    pub const NEW_FILE: &str = "\u{E89C}";
    /// Archive and checkpoints: inventory_2.
    pub const INVENTORY: &str = "\u{E1A1}";
    pub const RESTART: &str = "\u{F053}";
    pub const PAUSE: &str = "\u{E1A2}";
    pub const ATTACH: &str = "\u{E226}";
    /// Review the changes: difference.
    pub const DIFF: &str = "\u{EB7D}";
    pub const HISTORY: &str = "\u{E8B3}";
    pub const STOP_CIRCLE: &str = "\u{EF71}";
    pub const GROUP: &str = "\u{EA21}";
    pub const PERSON_SEARCH: &str = "\u{F106}";
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

/// The status mark of an Agent state in a list; empty for states that need
/// no mark (ready, ended), so a calm list shows none.
pub fn list_dot(palette: Palette, state: eliot_core::AgentState) -> View {
    let tone = StatusTone::of_state(state);
    match palette.dot(tone) {
        Some(_) => status_mark(palette, tone, layout::DOT),
        None => View::empty(),
    }
}

/// A status mark of `size` DIPs whose shape tells states apart within the
/// theme's two status colors: a filled circle for working (accent), needs
/// you (attention) and ready (outline grey); a ring for starting (accent),
/// ended (grey) and an unconfirmed outcome (attention); a rounded square
/// for failed or lost (attention), so a failure never reads as a question.
pub fn status_mark(palette: Palette, tone: StatusTone, size: f64) -> View {
    let circle = |fill: Brush| -> View {
        Ellipse::new()
            .fill(fill)
            .width(size)
            .height(size)
            .vertical_alignment(VerticalAlignment::Center)
            .into()
    };
    let ring = |stroke: Brush| -> View {
        Ellipse::new()
            .stroke(stroke)
            .stroke_thickness(layout::STATUS_STROKE)
            .width(size)
            .height(size)
            .vertical_alignment(VerticalAlignment::Center)
            .into()
    };
    match tone {
        StatusTone::Active => circle(palette.primary()),
        StatusTone::Attention => circle(palette.attention()),
        StatusTone::Ready => circle(palette.border()),
        StatusTone::Pending => ring(palette.primary()),
        StatusTone::Neutral => ring(palette.border()),
        StatusTone::Unknown => ring(palette.attention()),
        StatusTone::Failed => Border::new()
            .background(palette.attention())
            .corner_radius(radius::STATUS_MARK)
            .width(size)
            .height(size)
            .vertical_alignment(VerticalAlignment::Center)
            .into(),
    }
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
    use crate::theme::elevation::{self, Band};
    let Some(roles) = palette.roles() else {
        return place.keyed_children(vec![KeyedView::new("content", view.into())]);
    };
    let paint = |band: Band, tint: crate::theme::Rgb8, opacity: f64| {
        let brush = Brush::Solid(Color::argb(
            (opacity * band.share * 255.0).round() as u8,
            tint.r,
            tint.g,
            tint.b,
        ));
        let border = Border::new()
            .corner_radius((radius + band.reach).max(0.0))
            .margin(Thickness::new(
                -band.reach,
                band.offset - band.reach,
                -band.reach,
                -band.offset - band.reach,
            ));
        match band.width {
            None => border.background(brush),
            Some(width) => border
                .border_brush(brush)
                .border_thickness(Thickness::uniform(width)),
        }
    };
    let bands: Vec<Border> = match roles.lift {
        crate::theme::Lift::Shadow { key, ambient } => {
            let [key_bands, ambient_bands] = elevation::level_bands(level);
            let shadow = |bands: &'static [Band], opacity| {
                bands
                    .iter()
                    .map(move |band| paint(*band, crate::theme::SHADOW_COLOR, opacity))
            };
            shadow(ambient_bands, ambient)
                .chain(shadow(key_bands, key))
                .collect()
        }
        // A ring theme: one hairline around the element, no blur.
        crate::theme::Lift::Ring(color) => vec![paint(
            Band {
                reach: layout::HAIRLINE,
                width: None,
                offset: 0.0,
                share: 1.0,
            },
            color,
            1.0,
        )],
    };
    let mut cells: Vec<KeyedView> = bands
        .into_iter()
        .enumerate()
        .map(|(index, band)| KeyedView::new(format!("lift:{index}"), band))
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

/// The composer's main action (Send; Stop and Steer recolor it): a raised
/// 40-DIP square at the medium corner.
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

/// The composer's Stop turn: the 40-DIP action in `error`.
pub fn stop_button(palette: Palette) -> Button {
    send_button(palette).resource_overrides(palette.stop_fab())
}

/// The composer's Steer: the 40-DIP action in `tertiary`.
pub fn steer_button(palette: Palette) -> Button {
    send_button(palette).resource_overrides(palette.steer_fab())
}

/// The New task button: `primary-container`, 40 DIPs.
pub fn new_task_button(palette: Palette) -> Button {
    row(palette, false)
        .resource_overrides(palette.new_task())
        .min_height(height::NEW_TASK)
}

/// Stop's mark: a filled 12-DIP square in `on-error`.
pub fn stop_mark(palette: Palette) -> View {
    Border::new()
        .width(icon_size::STOP)
        .height(icon_size::STOP)
        .corner_radius(radius::STOP)
        .background(palette.on_error())
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

/// A menu row with the attention role carried by its content and state layer.
pub fn destructive_row(palette: Palette, selected: bool) -> Button {
    row(palette, selected).resource_overrides(palette.destructive_row(selected))
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
/// view (its count). A linked worktree's subproject follows the name in
/// the muted caption ("eliot-swarm… › a786 sidebar fixes"): the name trims
/// first, at most [`layout::PROJECT_NAME_WIDTH`], so sibling worktrees of
/// one repository never trim to the same text.
pub fn project_content(name: &str, subproject: Option<&str>, trailing: View) -> View {
    let title = text(name.to_owned(), style::BUTTON)
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis);
    let label: View = match subproject {
        Some(subproject) => Grid::new()
            .grid_column(1)
            .columns([GridLength::Auto, GridLength::STAR])
            .column_spacing(spacing::XS)
            .children((
                title.max_width(layout::PROJECT_NAME_WIDTH),
                secondary(format!("› {subproject}"))
                    .grid_column(1)
                    .vertical_alignment(VerticalAlignment::Center)
                    .text_trimming(TextTrimming::CharacterEllipsis),
            )),
        None => title.grid_column(1).into(),
    };
    Grid::new()
        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::ROW)
        .children((
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(icon(glyph::FOLDER)),
            label,
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .content(trailing),
        ))
}

/// A nested row's content: indented `depth` steps of 28 DIPs, so its title
/// starts under its parent's label; ended tasks in the variant tone; a
/// trailing view (a dot, an age). A running task's work line runs under
/// the title and the trailing mark in the muted caption, one line, trimmed
/// (the row is then [`height::TWO_LINE_ROW`] tall).
pub fn task_content(
    depth: usize,
    title: &str,
    ended: bool,
    trailing: View,
    line: Option<(String, bool)>,
) -> View {
    let title = TextBlock::new()
        .text(title.to_owned())
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis);
    let title = if ended {
        title.foreground(palette().variant())
    } else {
        title
    };
    let indent = Thickness::new(layout::TREE_INDENT * depth as f64, 0.0, 0.0, 0.0);
    let top = Grid::new()
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::ROW);
    let top = |title: TextBlock, trailing: View| {
        top.children((
            title,
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .content(trailing),
        ))
    };
    match line {
        // The second line: the work line, or what needs attention in the
        // attention tone (as the Monitor's row says it).
        Some((line, attention)) => {
            let line = secondary(line).text_trimming(TextTrimming::CharacterEllipsis);
            let line = if attention {
                line.foreground(palette().attention())
            } else {
                line
            };
            StackPanel::new()
                .margin(indent)
                .vertical_alignment(VerticalAlignment::Center)
                .children((top(title, trailing), line))
        }
        None => Border::new().margin(indent).content(top(title, trailing)),
    }
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

/// A fact shown as a chip but not a control (a preset's access, a project
/// that always takes a worktree): the chip's outline and label, no state
/// layer. Full access is its label alone in the attention tone, as the
/// design draws it (Reactor 0.100.0 sets no icon color outside a button).
pub fn fixed_chip(
    palette: Palette,
    attention: bool,
    glyph: Option<&str>,
    label: &str,
    name: &str,
) -> View {
    let outline = if attention {
        palette.attention_outline()
    } else {
        palette.border()
    };
    let mut label_view = text(label.to_owned(), style::CHIP)
        .vertical_alignment(VerticalAlignment::Center)
        .max_width(layout::PROJECT_CHIP_WIDTH)
        .text_trimming(TextTrimming::CharacterEllipsis);
    if attention {
        label_view = label_view.foreground(palette.attention());
    }
    let mut parts: Vec<KeyedView> = Vec::new();
    if let Some(glyph) = glyph.filter(|_| !attention) {
        parts.push(KeyedView::new("icon", small_icon(glyph)));
    }
    parts.push(KeyedView::new("label", label_view));
    Border::new()
        .automation_name(name.to_owned())
        .height(height::CHIP)
        .corner_radius(radius::CHIP)
        .border_brush(outline)
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .padding(Thickness::xy(spacing::ROW, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .content(
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(spacing::CHIP_GAP)
                .vertical_alignment(VerticalAlignment::Center)
                .keyed_children(parts),
        )
}

/// A transcript action (Copy) that shows only while its message is under
/// the pointer: there it is an icon button; elsewhere this keyboard stop
/// keeps its place and shows its glyph only while it has keyboard focus
/// (the implicit `HyperlinkButton` style in `app_resources`).
pub fn reveal_button() -> HyperlinkButton {
    HyperlinkButton::new()
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
        .spacing(spacing::CHIP_GAP)
        .keyed_children(parts)
}

/// A borderless chip (the composer's project selector, a menu entry): the
/// text color, a state layer on hover, no outline.
pub fn text_chip(palette: Palette) -> Button {
    button(palette.row(false))
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
        .margin(Thickness::xy(spacing::TEXT_INSET - spacing::ROW, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// The search field of a pane: a 32-DIP pill in the tonal tone, a 16-DIP
/// icon and a borderless field (13 DIPs) 8 DIPs after it.
pub fn search_field(fill: Brush, field: TextBox) -> View {
    // WinUI's text box insets its text by 10 DIPs; the margin puts the text
    // 8 DIPs after the icon, as the design does.
    Border::new()
        .height(height::CONTROL)
        .corner_radius(radius::PILL)
        .background(fill)
        .padding(Thickness::new(spacing::INSET, 0.0, spacing::XS, 0.0))
        .content(
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR])
                .column_spacing(spacing::SM - spacing::FIELD_LEADING_INSET)
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

/// A tool row's trailing caption: a duration, or the status in `tone`
/// while it runs.
pub fn tool_caption(trailing: String, tone: Option<Brush>) -> TextBlock {
    text(trailing, style::CAPTION).foreground(tone.unwrap_or_else(|| palette().muted()))
}

/// A tool row's content: a 16-DIP icon, the 13-DIP label, the trailing
/// caption ([`tool_caption`], or a component that keeps its own running
/// seconds) and a chevron.
pub fn tool_content(glyph: &str, label: &str, trailing: View) -> View {
    let trailing = Border::new()
        .grid_column(2)
        .vertical_alignment(VerticalAlignment::Center)
        .content(trailing);
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
        .min_height(height::SUGGESTION)
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Left)
}

/// A suggestion card's content: the 18-DIP icon (in the card's primary
/// content color) and its 14-DIP label in the text role, 12/14 inset.
pub fn suggestion_content(palette: Palette, glyph: &str, label: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::ROW)
        .margin(Thickness::xy(
            spacing::TEXT_INSET - spacing::INSET,
            spacing::INSET,
        ))
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
        .padding(Thickness::xy(spacing::TEXT_INSET, spacing::ROW))
        .horizontal_alignment(HorizontalAlignment::Right)
        .max_width(max_width)
        .content(content)
}

/// A steered message: the user's bubble shape filled with the selected
/// role at half strength, its "Steer" label over the text (the design's
/// dashed outline has no Reactor stroke; a solid one reads as a field).
pub fn steer_bubble(palette: Palette, text_value: &str, max_width: Option<f64>) -> View {
    Border::new()
        .background(palette.steer())
        .corner_radius(CornerRadius::new(
            radius::BUBBLE,
            radius::BUBBLE,
            radius::BUBBLE_TAIL,
            radius::BUBBLE,
        ))
        .padding(Thickness::xy(spacing::TEXT_INSET, spacing::ROW))
        .horizontal_alignment(HorizontalAlignment::Right)
        .max_width(max_width)
        .content(
            StackPanel::new().spacing(spacing::ROW_GAP).children((
                muted("Steer", style::LABEL),
                body(text_value.to_owned())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            )),
        )
}

/// The composer card: the composer role, the 20-DIP corner, lifted to
/// level 3, padded 12/12/10/16. `place` sizes it.
pub fn composer_card(palette: Palette, place: Grid, content: impl Into<View>) -> View {
    lifted(
        palette,
        crate::theme::elevation::Level::One,
        radius::COMPOSER,
        place,
        Border::new()
            .background(palette.composer())
            .border_brush(palette.outline())
            .border_thickness(Thickness::uniform(layout::HAIRLINE))
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
/// and a caption of at most `label_width` that also trims to the room the
/// pill is given (a grid, as a horizontal stack never trims).
pub fn status_pill(palette: Palette, tone: StatusTone, label: &str, label_width: f64) -> View {
    let (container, on) = palette.status_chip(tone);
    Border::new()
        .height(height::BADGE)
        .corner_radius(radius::BADGE)
        .background(container)
        .padding(Thickness::xy(spacing::ROW, 0.0))
        .horizontal_alignment(HorizontalAlignment::Left)
        .vertical_alignment(VerticalAlignment::Center)
        .automation_name(label.to_owned())
        .content(
            Grid::new()
                .columns([GridLength::Auto, GridLength::STAR])
                .column_spacing(spacing::CHIP_GAP)
                .children((
                    Border::new()
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(status_mark(palette, tone, layout::BADGE_DOT)),
                    text(label.to_owned(), style::CAPTION)
                        .grid_column(1)
                        .foreground(on)
                        .vertical_alignment(VerticalAlignment::Center)
                        .max_width(label_width)
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
            body(value)
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

/// A popover or in-window menu: the menu role (`surface-container`), the
/// container corner, the dark themes' overlay border, a 4-DIP inset (lift
/// it with [`lifted`] at level 2).
pub fn popover(palette: Palette) -> Border {
    Border::new()
        .background(palette.menu())
        .border_brush(palette.overlay_border())
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
        .border_thickness(Thickness::uniform(layout::USAGE_RING_STROKE))
        .border_brush(status)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}
