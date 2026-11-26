//! The desktop's building blocks as stock WinUI controls (brand book
//! `docs/design/fluent-design-system.md` §5.1): rows, chips, segments, icon
//! buttons, the composer's actions, cards, form and settings rows, status
//! marks and counters. Every builder is a standard control or a plain
//! composition of standard controls with their own templates, visual states
//! and theme resources; sizes come from [`crate::ui::tokens`].
//!
//! Buttons take one of WinUI's own styles (Reactor's [`ButtonStyle`]):
//! Accent for the one main action, Default for ordinary actions, Subtle for
//! quiet and icon actions. The app paints no color of its own: where a mark
//! or text needs the accent or the critical color it takes a Reactor
//! [`ThemeBrush`], which follows System, Light, Dark and High Contrast.

use windows_reactor::*;

use crate::ui::tokens::text::{self as style, TypeStyle};
use crate::ui::tokens::{height, icon as icon_size, layout, radius, spacing};
use crate::views::{MaybeContent, MaybeSize};

/// The app's icons: Segoe Fluent Icons code points (the system symbol font
/// WinUI's `FontIcon` uses), checked against Microsoft's catalogue
/// (learn.microsoft.com, "Segoe Fluent Icons font"). One glyph per action.
pub mod glyph {
    /// Add.
    pub const NEW_TASK: &str = "\u{E710}";
    /// Search.
    pub const SEARCH: &str = "\u{E721}";
    /// Folder.
    pub const FOLDER: &str = "\u{E8B7}";
    /// Page.
    pub const FILE: &str = "\u{E7C3}";
    /// TreeFolderFolder.
    pub const WORKTREE: &str = "\u{ED41}";
    /// Settings.
    pub const SETTINGS: &str = "\u{E713}";
    /// StopSolid.
    pub const STOP: &str = "\u{EE95}";
    /// Stop.
    pub const STOP_CIRCLE: &str = "\u{E71A}";
    /// More.
    pub const MORE: &str = "\u{E712}";
    /// More (the catalogue has no vertical form).
    pub const MORE_VERTICAL: &str = MORE;
    /// Info.
    pub const INFO: &str = "\u{E946}";
    /// Mail.
    pub const INBOX: &str = "\u{E715}";
    /// Cancel.
    pub const CLOSE: &str = "\u{E711}";
    /// Back.
    pub const BACK: &str = "\u{E72B}";
    /// Forward.
    pub const FORWARD: &str = "\u{E72A}";
    /// ClosePane.
    pub const HIDE_SIDEBAR: &str = "\u{E89F}";
    /// OpenPane.
    pub const SHOW_SIDEBAR: &str = "\u{E8A0}";
    /// ChevronRight.
    pub const CHEVRON_RIGHT: &str = "\u{E76C}";
    /// ChevronLeft.
    pub const CHEVRON_LEFT: &str = "\u{E76B}";
    /// ChevronDown.
    pub const CHEVRON_DOWN: &str = "\u{E70D}";
    /// ChevronUp.
    pub const CHEVRON_UP: &str = "\u{E70E}";
    /// Down.
    pub const ARROW_DOWN: &str = "\u{E74B}";
    /// Up.
    pub const ARROW_UP: &str = "\u{E74A}";
    /// Copy.
    pub const COPY: &str = "\u{E8C8}";
    /// Robot.
    pub const MODEL: &str = "\u{E99A}";
    /// Shield.
    pub const SHIELD: &str = "\u{EA18}";
    /// Repair.
    pub const TOOLS: &str = "\u{E90F}";
    /// CommandPrompt.
    pub const TERMINAL: &str = "\u{E756}";
    /// Edit.
    pub const EDIT: &str = "\u{E70F}";
    /// CheckMark.
    pub const CHECK: &str = "\u{E73E}";
    /// FavoriteStar.
    pub const STAR: &str = "\u{E734}";
    /// FavoriteStarFill.
    pub const STAR_FILLED: &str = "\u{E735}";
    /// Lock.
    pub const LOCK: &str = "\u{E72E}";
    /// Warning.
    pub const WARNING: &str = "\u{E7BA}";
    /// LightningBolt.
    pub const AUTOMATION: &str = "\u{E945}";
    /// SpeedHigh.
    pub const USAGE: &str = "\u{EC4A}";
    /// OpenInNewWindow.
    pub const EXTERNAL: &str = "\u{E8A7}";
    /// OpenFolderHorizontal.
    pub const OPEN_FOLDER: &str = "\u{ED25}";
    /// KeyboardClassic.
    pub const KEYBOARD: &str = "\u{E765}";
    /// Directions.
    pub const ROUTING: &str = "\u{E8F0}";
    /// Cloud.
    pub const CLOUD: &str = "\u{E753}";
    /// Equalizer.
    pub const SLIDERS: &str = "\u{E9E9}";
    /// Message.
    pub const CHAT: &str = "\u{E8BD}";
    /// Package.
    pub const ARCHIVE: &str = "\u{E7B8}";
    /// Delete.
    pub const DELETE: &str = "\u{E74D}";
    /// MoveToFolder.
    pub const MOVE: &str = "\u{E8DE}";
    /// Play.
    pub const RESUME: &str = "\u{E768}";
    /// Rename.
    pub const RENAME: &str = "\u{E8AC}";
    /// Tag.
    pub const TAG: &str = "\u{E8EC}";
    /// BulletedList.
    pub const LIST: &str = "\u{E8FD}";
    /// Network.
    pub const HUB: &str = "\u{E968}";
    /// Clock.
    pub const SCHEDULE: &str = "\u{E917}";
    /// ErrorBadge.
    pub const ERROR: &str = "\u{EA39}";
    /// Send.
    pub const STEER: &str = "\u{E724}";
    /// Pause.
    pub const HOLD: &str = "\u{E769}";
    /// Flag.
    pub const GOAL: &str = "\u{E7C1}";
    /// Sync.
    pub const MERGE: &str = "\u{E895}";
    /// Refresh.
    pub const REFRESH: &str = "\u{E72C}";
    /// Snooze.
    pub const SNOOZE: &str = "\u{F4BD}";
    /// Filter.
    pub const FILTER: &str = "\u{E71C}";
    /// AlignLeft.
    pub const WRAP: &str = "\u{E8E4}";
    /// FullScreen.
    pub const FULL_VIEW: &str = "\u{E740}";
    /// BackToWindow.
    pub const EXIT_FULL_VIEW: &str = "\u{E73F}";
    /// PageAdd.
    pub const NEW_FILE: &str = "\u{EA1A}";
    /// Library.
    pub const INVENTORY: &str = "\u{E8F1}";
    /// Replay.
    pub const RESTART: &str = "\u{EF3B}";
    /// Code.
    pub const DIFF: &str = "\u{E943}";
    /// History.
    pub const HISTORY: &str = "\u{E81C}";
    /// CheckList.
    pub const CHECKLIST: &str = "\u{E9D5}";
    /// Admin.
    pub const GAVEL: &str = "\u{E7EF}";
    /// Broom.
    pub const CLEANUP: &str = "\u{EA99}";
    /// Important.
    pub const ATTENTION: &str = "\u{E8C9}";
    /// Permissions.
    pub const KEY: &str = "\u{E8D7}";
    /// Process.
    pub const DAEMON: &str = "\u{E9F3}";
    /// People.
    pub const GROUP: &str = "\u{E716}";
}

/// Semantic meaning of an Agent or command state, for status marks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusTone {
    /// Working now (`busy`).
    Active,
    /// Needs the owner (`waiting`: a pending request).
    Attention,
    /// `failed` or `lost`.
    Failed,
    /// On its way (`created`, `starting`).
    Pending,
    /// Ended or anything else (`stopped`).
    Neutral,
}

/// A quota window with less than this share left (percent) reads as low.
pub const QUOTA_LOW: f64 = 20.0;

pub fn weight(style: TypeStyle) -> FontWeight {
    FontWeight::new(style.weight).unwrap_or(FontWeight::NORMAL)
}

/// Text in one of the type ramp's roles, in the theme's text color.
pub fn text(value: impl Into<String>, style: TypeStyle) -> TextBlock {
    TextBlock::new()
        .text(value.into())
        .font_size(style.size)
        .font_weight(weight(style))
}

/// Body text: messages, labels, values.
pub fn body(value: impl Into<String>) -> TextBlock {
    text(value, style::BODY)
}

/// Secondary text: notes, hints, counts, supporting lines (Caption).
pub fn secondary(value: impl Into<String>) -> TextBlock {
    text(value, style::CAPTION)
}

/// A page title (a Settings page, the home greeting).
pub fn page_title(value: impl Into<String>) -> TextBlock {
    text(value, style::HERO).automation_heading_level(AutomationHeadingLevel::Level2)
}

/// A section, card, panel or conversation title.
pub fn section_title(value: impl Into<String>) -> TextBlock {
    text(value, style::HEADER).automation_heading_level(AutomationHeadingLevel::Level3)
}

/// A section label ("Projects", "Chats").
pub fn section_label(value: impl Into<String>) -> TextBlock {
    text(value, style::LABEL).automation_heading_level(AutomationHeadingLevel::Level3)
}

/// The sidebar's section label row: the label at the rows' content edge.
pub fn section_row(value: &str) -> View {
    Border::new()
        .min_height(height::CONTROL)
        .padding(Thickness::xy(spacing::ROW, 0.0))
        .margin(Thickness::new(0.0, spacing::INSET, 0.0, 0.0))
        .content(section_label(value.to_owned()).vertical_alignment(VerticalAlignment::Center))
        .into()
}

/// A card (§4.1): the theme's card background and stroke on the container
/// corner (call `.content(..)`).
pub fn card() -> Border {
    Border::new()
        .background(ThemeBrush::CardBackground)
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::uniform(spacing::MD))
}

/// A status mark of `size` DIPs whose shape tells states apart: a filled
/// circle for working (accent) and needs you (critical); a ring for
/// starting (accent) and ended (text); a rounded square for failed
/// or lost (critical), so a failure never reads as a question.
pub fn status_mark(tone: StatusTone, size: f64) -> View {
    let circle = |fill: ThemeBrush| -> View {
        Ellipse::new()
            .fill(fill)
            .width(size)
            .height(size)
            .vertical_alignment(VerticalAlignment::Center)
            .into()
    };
    let ring = |stroke: ThemeBrush| -> View {
        Ellipse::new()
            .stroke(stroke)
            .stroke_thickness(layout::STATUS_STROKE)
            .width(size)
            .height(size)
            .vertical_alignment(VerticalAlignment::Center)
            .into()
    };
    match tone {
        StatusTone::Active => circle(ThemeBrush::Accent),
        StatusTone::Attention => circle(ThemeBrush::SystemCritical),
        StatusTone::Pending => ring(ThemeBrush::Accent),
        StatusTone::Neutral => ring(ThemeBrush::PrimaryText),
        StatusTone::Failed => Border::new()
            .background(ThemeBrush::SystemCritical)
            .corner_radius(radius::STATUS_MARK)
            .width(size)
            .height(size)
            .vertical_alignment(VerticalAlignment::Center)
            .into(),
    }
}

/// A Segoe Fluent Icons glyph of `size` DIPs; it takes the foreground of
/// the control around it.
fn glyph_icon(glyph: &str, size: f64) -> FontIcon {
    FontIcon::new().glyph(glyph).font_size(size)
}

fn icon_sized(glyph: &str, size: f64) -> View {
    glyph_icon(glyph, size)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// The icon of rows and icon buttons.
pub fn icon(glyph: &str) -> View {
    icon_sized(glyph, icon_size::ROW)
}

/// The icon of chips, tool rows and fields.
pub fn small_icon(glyph: &str) -> View {
    icon_sized(glyph, icon_size::SMALL)
}

/// An icon in the theme's critical color (a failed row, a danger entry).
pub fn critical_icon(glyph: &str, size: f64) -> View {
    glyph_icon(glyph, size)
        .foreground(ThemeBrush::SystemCritical)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// A chip's chevron (a chip or field that opens a list).
pub fn dropdown_chevron() -> View {
    icon_sized(glyph::CHEVRON_DOWN, icon_size::CHEVRON)
}

/// The one row layout (the row rule): `Grid [Auto lead, STAR text, Auto
/// trailing]`. The star cell holds the one trimmable text; a second text
/// takes its own star cell (see [`row_content`]); absent parts take no
/// column, so no gap shifts the text.
pub fn row_grid(lead: Option<View>, text: View, trailing: Option<View>, gap: f64) -> View {
    let mut columns = Vec::new();
    let mut cells: Vec<KeyedView> = Vec::new();
    if let Some(lead) = lead {
        columns.push(GridLength::Auto);
        cells.push(KeyedView::new(
            "lead",
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(lead),
        ));
    }
    columns.push(GridLength::STAR);
    cells.push(KeyedView::new(
        "text",
        Border::new()
            .grid_column(cells.len() as i32)
            .vertical_alignment(VerticalAlignment::Center)
            .content(text),
    ));
    if let Some(trailing) = trailing {
        columns.push(GridLength::Auto);
        cells.push(KeyedView::new(
            "trailing",
            Border::new()
                .grid_column(cells.len() as i32)
                .vertical_alignment(VerticalAlignment::Center)
                .content(trailing),
        ));
    }
    Grid::new()
        .columns(columns)
        .column_spacing(gap)
        .keyed_children(cells)
        .into()
}

/// A trimmable label: one line, an ellipsis where it ends.
fn trimmed(label: &str, style: TypeStyle) -> TextBlock {
    text(label.to_owned(), style)
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis)
}

/// Icon and label side by side: a row or menu entry.
pub fn icon_label(glyph: &str, label: &str) -> View {
    row_grid(
        Some(icon(glyph)),
        trimmed(label, style::BODY).into(),
        None,
        spacing::ROW,
    )
}

/// A labelled button's label.
pub fn label(value: &str) -> TextBlock {
    text(value.to_owned(), style::BUTTON)
}

/// The one main action of a page: WinUI's accent button.
pub fn filled() -> Button {
    Button::new().style(ButtonStyle::Accent)
}

/// An ordinary action (Validate, Reload, Duplicate, Add): the default
/// button.
pub fn outlined() -> Button {
    Button::new()
}

/// A quiet action (Discard, Edit as TOML, Refresh, Hide): the subtle
/// button.
pub fn text_button() -> Button {
    Button::new().style(ButtonStyle::Subtle)
}

/// A destructive action (Remove, Force stop): the default button; its
/// label says what it does.
pub fn destructive() -> Button {
    Button::new()
}

/// The stroke every WinUI button template draws inside its bounds
/// (`ButtonBorderThemeThickness`), transparent on the subtle style.
const BUTTON_BORDER: f64 = 1.0;

/// An icon action: a subtle button around one icon, 32 DIPs square like
/// every labelled button beside it (`.content(icon(..))` last): its padding
/// leaves room for the template's own stroke. Give it an accessible name
/// and a tooltip.
pub fn icon_button() -> Button {
    Button::new()
        .style(ButtonStyle::Subtle)
        .padding(Thickness::uniform(
            (height::CONTROL - icon_size::ROW) / 2.0 - BUTTON_BORDER,
        ))
        .min_width(height::CONTROL)
        .min_height(height::CONTROL)
}

/// A text action inside a column of text (an Inbox item's title, a
/// project's Open folder): a subtle button with no inset and no minimum
/// height, so its label stands where a plain line of text would and lines
/// up with the lines around it.
pub fn inline_button() -> Button {
    Button::new()
        .style(ButtonStyle::Subtle)
        .padding(Thickness::uniform(0.0))
        .min_width(0.0)
        .min_height(0.0)
        .horizontal_alignment(HorizontalAlignment::Left)
}

/// A command in a bar (the conversation header, the composer row): a
/// stock button at the control height with its own padding, so every
/// command in one bar has the same height whatever its content (a label,
/// an icon and a label, or the "…" icon). `accent` for the one main action,
/// and for the ▾ beside it (the composer's Add as a note, Home's Start task
/// options).
pub fn bar_button(accent: bool) -> Button {
    let button = Button::new()
        .min_height(height::CONTROL)
        .vertical_alignment(VerticalAlignment::Stretch);
    if accent {
        button.style(ButtonStyle::Accent)
    } else {
        button
    }
}

/// A pane command in the conversation header (Changes, Agents, Details):
/// WinUI's toggle button at the bar's height, checked while its tab of the
/// right pane is open; its checked look is the template's own state.
pub fn bar_toggle(checked: bool) -> ToggleButton {
    ToggleButton::new()
        .is_checked(checked)
        .min_height(height::CONTROL)
        .vertical_alignment(VerticalAlignment::Stretch)
}

/// A command's content: its icon, then its label (Body) on the same line,
/// both centred on it; `pending` puts WinUI's small indeterminate ring in
/// the icon's place while the command is confirmed (§9.4), so the busy
/// state stays on the action itself.
pub fn command_content(glyph: &str, label: &str, pending: bool) -> View {
    let lead: View = if pending {
        ProgressRing::new()
            .is_indeterminate(true)
            .width(icon_size::ROW)
            .height(icon_size::ROW)
            .min_width(icon_size::ROW)
            .min_height(icon_size::ROW)
            .vertical_alignment(VerticalAlignment::Center)
            .into()
    } else {
        icon(glyph)
    };
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::SM)
        .vertical_alignment(VerticalAlignment::Center)
        .children((
            lead,
            text(label.to_owned(), style::BUTTON).vertical_alignment(VerticalAlignment::Center),
        ))
        .into()
}

/// The New task button: the sidebar's main action, an accent button.
pub fn new_task_button() -> Button {
    Button::new()
        .style(ButtonStyle::Accent)
        .min_height(height::NEW_TASK)
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Stretch)
}

/// A nav, project, task or list row: a full-width subtle button; the
/// selection takes the card background under the button's own states.
/// Give it [`row_content`], [`nav_content`] or [`sidebar_row`].
pub fn row(selected: bool) -> Button {
    let row = Button::new()
        .style(ButtonStyle::Subtle)
        .min_height(height::CONTROL)
        .horizontal_alignment(HorizontalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Stretch);
    if selected {
        row.background(ThemeBrush::CardBackground)
    } else {
        row
    }
}

/// A row under its parent (same height as every row).
pub fn sub_row(selected: bool) -> Button {
    row(selected)
}

/// A row's content: an optional icon, the title (trimmed), an optional
/// caption note and a trailing mark. Absent parts take no column, so no
/// gap shifts the title.
pub fn row_content(
    glyph: Option<&str>,
    title: &str,
    note: &str,
    trailing: impl Into<Option<View>>,
) -> View {
    let title = trimmed(title, style::BODY);
    // A note stands under the title, as the model picker's provider does
    // under a model name: each line takes the row's whole width, so a short
    // name ("repo") leaves its room to a long note (its path), and either
    // trims only when the row is too narrow for it.
    let text: View = if note.is_empty() {
        title.into()
    } else {
        StackPanel::new()
            .vertical_alignment(VerticalAlignment::Center)
            .children((
                title,
                secondary(note.to_owned()).text_trimming(TextTrimming::CharacterEllipsis),
            ))
            .into()
    };
    row_grid(glyph.map(icon), text, trailing.into(), spacing::ROW)
}

/// A nav row's content: its icon, its label and its trailing status, one
/// part per line ("1 working" over "1 needs attention"), as the Codex
/// sidebar shows one status per row rather than a sentence. The label is
/// the row's name and keeps its width; the parts take the star cell at the
/// right and trim only when the sidebar is too narrow for one of them.
pub fn nav_content(glyph: &str, label: &str, parts: &[String]) -> View {
    if parts.is_empty() {
        return row_content(Some(glyph), label, "", None);
    }
    let lines: Vec<KeyedView> = parts
        .iter()
        .map(|part| {
            KeyedView::new(
                part.clone(),
                secondary(part.clone())
                    .horizontal_alignment(HorizontalAlignment::Right)
                    .text_trimming(TextTrimming::CharacterEllipsis),
            )
        })
        .collect();
    Grid::new()
        .columns([GridLength::Auto, GridLength::Auto, GridLength::STAR])
        .column_spacing(spacing::ROW)
        .children((
            icon(glyph),
            text(label.to_owned(), style::BODY)
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center),
            StackPanel::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .keyed_children(lines),
        ))
        .into()
}

/// A chip: a default button (`.content(chip_content(..))` last, then a
/// menu or click).
pub fn chip() -> Button {
    Button::new().min_height(height::CHIP)
}

/// A fact shown as a chip but not a control (a preset's access, a project
/// that always takes a worktree): a stroked shape with the chip's content,
/// taking no input; `attention` draws its icon in the critical color. Its
/// tooltip goes on the cell around it.
pub fn fixed_chip(attention: bool, glyph: Option<&str>, label: &str, name: &str) -> View {
    let lead = glyph.map(|glyph| {
        if attention {
            critical_icon(glyph, icon_size::SMALL)
        } else {
            small_icon(glyph)
        }
    });
    Border::new()
        .min_height(height::CHIP)
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .corner_radius(radius::CONTROL)
        .padding(Thickness::xy(spacing::INSET, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .horizontal_alignment(HorizontalAlignment::Left)
        .automation_name(name)
        .content(row_grid(
            lead,
            trimmed(label, style::CHIP)
                .max_width(layout::PROJECT_CHIP_WIDTH)
                .into(),
            None,
            spacing::CHIP_GAP,
        ))
        .into()
}

/// A transcript action (Copy) that shows only while its message is under
/// the pointer; elsewhere this keyboard stop keeps its place.
pub fn reveal_button() -> HyperlinkButton {
    HyperlinkButton::new()
}

/// A chip's content (the row rule): an optional icon, the label (trimmed
/// to the room the chip gets, at most a project chip's width), an optional
/// chevron.
pub fn chip_content(glyph: Option<&str>, label: &str, chevron: bool) -> View {
    row_grid(
        glyph.map(small_icon),
        trimmed(label, style::CHIP)
            .max_width(layout::PROJECT_CHIP_WIDTH)
            .into(),
        chevron.then(dropdown_chevron),
        spacing::CHIP_GAP,
    )
}

/// A row of chips, 6 DIPs apart: the first chip takes the star cell and
/// trims its label when the row runs out of room; the others keep their
/// size.
pub fn chip_row(chips: Vec<View>) -> View {
    let columns: Vec<GridLength> = (0..chips.len())
        .map(|index| {
            if index == 0 {
                GridLength::STAR
            } else {
                GridLength::Auto
            }
        })
        .collect();
    // Left-aligned, so the star cell is as wide as its chip while the row
    // has room (no gap after the first chip) and trims it only when not.
    Grid::new()
        .columns(columns)
        .column_spacing(spacing::CHIP_GAP)
        .horizontal_alignment(HorizontalAlignment::Left)
        .keyed_children(chips.into_iter().enumerate().map(|(index, chip)| {
            KeyedView::new(
                index,
                Border::new()
                    .grid_column(index as i32)
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .vertical_alignment(VerticalAlignment::Center)
                    .content(chip),
            )
        }))
        .into()
}

/// One segment of a segmented choice (see [`segmented`]): the selected
/// segment is the accent button, the others default buttons.
pub fn segment(selected: bool) -> Button {
    if selected {
        Button::new().style(ButtonStyle::Accent)
    } else {
        Button::new()
    }
}

/// The content of a segment: its label.
pub fn segment_label(label: &str) -> View {
    text(label.to_owned(), style::CHIP)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// A segmented choice: the segments side by side.
pub fn segmented(segments: Vec<KeyedView>) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::XS)
        .horizontal_alignment(HorizontalAlignment::Left)
        .keyed_children(segments)
        .into()
}

/// A pane tab (Monitor, Details): a subtle button; the selected tab is a
/// default button (`.content(pane_tab_label(..))` last).
pub fn pane_tab(selected: bool) -> Button {
    if selected {
        Button::new()
    } else {
        Button::new().style(ButtonStyle::Subtle)
    }
}

/// A pane tab's label.
pub fn pane_tab_label(label: &str) -> View {
    text(label.to_owned(), style::CHIP)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// The search field of a pane or page: the native text box (its
/// placeholder names what it searches).
pub fn search_field(field: TextBox) -> View {
    field.vertical_alignment(VerticalAlignment::Center).into()
}

/// A small icon: inline beside caption text, in a status pill.
pub fn xs_icon(glyph: &str) -> View {
    icon_sized(glyph, icon_size::CHEVRON)
}

/// A small ordinary action (Deny).
pub fn outlined_small() -> Button {
    Button::new()
}

/// A secondary action (Switch model, Resume): the default button.
pub fn tonal() -> Button {
    Button::new()
}

/// A step's trailing time (Caption, or `tone`).
pub fn tool_caption(trailing: String, tone: Option<ThemeBrush>) -> TextBlock {
    let caption = text(trailing, style::CAPTION);
    match tone {
        Some(tone) => caption.foreground(tone),
        None => caption,
    }
}

/// A work-log step or head: a full-width subtle button (running: the card
/// background). Give it [`step_content`].
pub fn step_row(running: bool) -> Button {
    row(running)
}

/// A step's content: the leading icon or mark, verb and object (trimmed),
/// the time on the right.
pub fn step_content(lead: View, name: &str, trailing: impl Into<Option<View>>) -> View {
    Grid::new()
        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::ROW)
        .children((
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(lead),
            text(name.to_owned(), style::BUTTON)
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .maybe_content(trailing.into()),
        ))
        .into()
}

/// A small icon beside quiet text (a service line, an edited file).
pub fn muted_icon(glyph: &str) -> View {
    icon_sized(glyph, icon_size::CHEVRON)
}

/// A file chip in an answer: a subtle button; [`file_chip_content`] inside.
pub fn file_chip() -> Button {
    Button::new().style(ButtonStyle::Subtle)
}

/// A file chip's icon and its name.
pub fn file_chip_content(glyph: &str, name: &str) -> View {
    row_grid(
        Some(icon_sized(glyph, icon_size::CHEVRON)),
        trimmed(name, style::BODY)
            .max_width(layout::PROJECT_CHIP_WIDTH)
            .into(),
        None,
        spacing::XS,
    )
}

/// A suggestion card on Home: a default button with its icon and label at
/// the left; it fills its row's height, so a card whose label takes two
/// lines does not stand taller than the cards beside it.
pub fn suggestion() -> Button {
    Button::new()
        .min_height(height::SUGGESTION)
        .vertical_alignment(VerticalAlignment::Stretch)
        .horizontal_content_alignment(HorizontalAlignment::Left)
}

/// The owner's message (§3.3): a neutral card surface at the right.
pub fn bubble(content: impl Into<View>, max_width: Option<f64>) -> View {
    Border::new()
        .background(ThemeBrush::CardBackground)
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .corner_radius(radius::CONTAINER)
        .padding(Thickness::xy(spacing::TEXT_INSET, spacing::ROW))
        .horizontal_alignment(HorizontalAlignment::Right)
        .maybe_max_width(max_width)
        .content(content)
        .into()
}

/// The composer card: a card surface; `place` sizes it.
pub fn composer_card(place: Grid, content: impl Into<View>) -> View {
    place
        .children((Border::new()
            .background(ThemeBrush::CardBackground)
            .border_brush(ThemeBrush::CardStroke)
            .border_thickness(Thickness::uniform(layout::HAIRLINE))
            .corner_radius(radius::CONTAINER)
            .padding(Thickness::new(
                spacing::MD,
                spacing::INSET,
                spacing::INSET,
                spacing::ROW,
            ))
            .content(content),))
        .into()
}

/// The header's status pill: the tone's mark and a caption of at most
/// `label_width` that also trims to the room the pill is given (a grid, as
/// a horizontal stack never trims).
pub fn status_pill(tone: StatusTone, label: &str) -> View {
    Border::new()
        .min_height(height::BADGE)
        .corner_radius(radius::CONTROL)
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .padding(Thickness::xy(spacing::SM, 0.0))
        .horizontal_alignment(HorizontalAlignment::Left)
        .vertical_alignment(VerticalAlignment::Center)
        .automation_name(label)
        .content(row_grid(
            Some(status_mark(tone, layout::BADGE_DOT)),
            trimmed(label, style::CAPTION).into(),
            None,
            spacing::CHIP_GAP,
        ))
        .into()
}

/// A text field.
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
        .into()
}

/// A form field that opens a list as a popover (the preset Model field):
/// a default button at the dropdowns' width; [`field_button_content`] puts
/// the value at the left and the chevron at the right.
pub fn field_button() -> Button {
    Button::new()
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
        .into()
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

/// A list of selectable rows in one card.
pub fn list(rows: Vec<KeyedView>) -> View {
    card()
        .padding(Thickness::uniform(spacing::SM))
        .vertical_alignment(VerticalAlignment::Top)
        .content(
            StackPanel::new()
                .spacing(spacing::ROW_GAP)
                .keyed_children(rows),
        )
        .into()
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
                .keyed_children(crate::views::slots([
                    Some(section_title(title.to_owned()).into()),
                    (!supporting.is_empty()).then(|| {
                        secondary(supporting.to_owned())
                            .text_wrapping(TextWrapping::Wrap)
                            .into()
                    }),
                ])),
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .maybe_content(action),
        ))
        .into()
}

/// A hairline divider in the theme's card stroke.
pub fn divider() -> Border {
    Border::new()
        .height(layout::HAIRLINE)
        .background(ThemeBrush::CardStroke)
}

/// The heading above a settings card ("Appearance").
pub fn card_title(title: &str) -> View {
    text(title.to_owned(), style::SMALL_TITLE)
        .automation_heading_level(AutomationHeadingLevel::Level3)
        .margin(Thickness::new(0.0, spacing::SM, 0.0, 0.0))
        .into()
}

/// One settings row: the name and its one-line description on the left,
/// the control on the right.
pub fn setting_row(title: &str, description: &str, control: impl Into<Option<View>>) -> View {
    Grid::new()
        .min_height(height::FORM_ROW)
        .margin(Thickness::xy(0.0, spacing::SM))
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::MD)
        .children((
            StackPanel::new()
                .vertical_alignment(VerticalAlignment::Center)
                .keyed_children(crate::views::slots([
                    Some(
                        body(title.to_owned())
                            .text_trimming(TextTrimming::CharacterEllipsis)
                            .into(),
                    ),
                    (!description.is_empty()).then(|| {
                        secondary(description.to_owned())
                            .text_wrapping(TextWrapping::Wrap)
                            .into()
                    }),
                ])),
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .maybe_content(control.into()),
        ))
        .into()
}

/// Settings rows in one card, a divider between rows.
pub fn settings_card(rows: Vec<KeyedView>) -> View {
    let mut children = Vec::with_capacity(rows.len() * 2);
    for (index, row) in rows.into_iter().enumerate() {
        if index > 0 {
            children.push(KeyedView::new(format!("divider:{index}"), divider()));
        }
        children.push(row);
    }
    card()
        .padding(Thickness::xy(spacing::MD, spacing::XS))
        .vertical_alignment(VerticalAlignment::Top)
        .content(StackPanel::new().keyed_children(children))
        .into()
}

/// The content of a link-style button ("Open config.toml ↗"): the label
/// and its trailing glyph.
pub fn link_content(label: &str) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::XS)
        .children((
            text(label.to_owned(), style::BUTTON).vertical_alignment(VerticalAlignment::Center),
            small_icon(glyph::EXTERNAL),
        ))
        .into()
}

/// A small stroked badge ("default").
pub fn badge(text: &str) -> View {
    Border::new()
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .corner_radius(radius::CONTROL)
        .padding(Thickness::xy(spacing::SM, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .content(secondary(text.to_owned()))
        .into()
}

/// A colored outline marks the quota band, including at zero remaining.
pub fn status_ring(status: ThemeBrush) -> View {
    Border::new()
        .width(layout::RING)
        .height(layout::RING)
        .corner_radius(layout::RING / 2.0)
        .border_thickness(Thickness::uniform(layout::USAGE_RING_STROKE))
        .border_brush(status)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// A counter (the Inbox count): WinUI's `InfoBadge`.
pub fn count_badge(count: usize) -> View {
    InfoBadge::new()
        .value(i32::try_from(count).unwrap_or(i32::MAX))
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}

/// A sidebar row's trailing word, age or shortcut (Caption).
pub fn trailing(value: impl Into<String>) -> TextBlock {
    text(value, style::TRAILING).vertical_alignment(VerticalAlignment::Center)
}

/// A task row's `+N −M` (Caption); the signs carry the meaning.
pub fn diff_counts(insertions: u64, deletions: u64) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::XS)
        .vertical_alignment(VerticalAlignment::Center)
        .children((
            text(format!("+{}", group_digits(insertions)), style::TRAILING),
            text(
                format!("\u{2212}{}", group_digits(deletions)),
                style::TRAILING,
            ),
        ))
        .into()
}

/// `1234567` as `1,234,567`.
fn group_digits(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// A task row's state mark (§4.2): a static shape in a fixed slot; the row
/// or label beside it carries the words. No row draws a progress ring, so
/// a list of working tasks never shows a row of spinning fragments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowMark {
    /// Working or starting: a filled accent dot.
    Working,
    /// Waiting for you: the attention icon in the accent text color.
    Waiting,
    /// Idle or ready: a small ring.
    Idle,
    /// Queued: the clock icon.
    Queued,
    /// Failed, lost or unknown: the error icon in the critical color.
    Failed,
}

impl RowMark {
    /// The mark's own name for a screen reader, when the row gives no more
    /// precise word (`Lost`, `Provider hold`).
    pub fn word(self) -> &'static str {
        match self {
            Self::Working => "Working",
            Self::Waiting => "Waiting",
            Self::Idle => "Idle",
            Self::Queued => "Queued",
            Self::Failed => "Failed",
        }
    }
}

/// The view of a row's state mark in its slot: `word` names it for a
/// screen reader; the row's text or tooltip says it on screen. A stopped
/// row has no mark (`None` where a mark is optional).
pub fn row_mark(mark: RowMark, word: &str) -> View {
    let slot = |content: View| -> Border {
        Border::new()
            .width(layout::MARK_SLOT)
            .height(layout::MARK_SLOT)
            .vertical_alignment(VerticalAlignment::Center)
            .content(content)
    };
    let dot = |size: f64| -> Ellipse {
        Ellipse::new()
            .width(size)
            .height(size)
            .horizontal_alignment(HorizontalAlignment::Center)
            .vertical_alignment(VerticalAlignment::Center)
    };
    let slot = match mark {
        RowMark::Working => slot(dot(layout::DOT_WAIT).fill(ThemeBrush::Accent).into()),
        RowMark::Waiting => slot(
            glyph_icon(glyph::ATTENTION, icon_size::ROW)
                .foreground(ThemeBrush::AccentText)
                .vertical_alignment(VerticalAlignment::Center)
                .into(),
        ),
        RowMark::Idle => slot(
            dot(layout::DOT_IDLE)
                .stroke(ThemeBrush::PrimaryText)
                .stroke_thickness(layout::HAIRLINE)
                .into(),
        ),
        RowMark::Queued => slot(icon(glyph::SCHEDULE)),
        RowMark::Failed => slot(critical_icon(glyph::ERROR, icon_size::ROW)),
    };
    slot.automation_name(word).into()
}

/// A sidebar row's content: the leading mark or icon, the label (Body;
/// Body Strong when selected) and the trailing view; a worker `indent`s
/// past its manager's mark, its label at the kid size. `tone` recolors the
/// label alone.
pub fn sidebar_row(
    leading: impl Into<Option<View>>,
    label: &str,
    selected: bool,
    indent: f64,
    kid: bool,
    tone: Option<ThemeBrush>,
    trailing: impl Into<Option<View>>,
) -> View {
    let mut title = text(
        label.to_owned(),
        if selected {
            style::EMPHASIZED
        } else {
            style::BUTTON
        },
    )
    .vertical_alignment(VerticalAlignment::Center)
    .text_trimming(TextTrimming::CharacterEllipsis);
    if kid {
        title = title.font_size(style::KID_SIZE);
    }
    if let Some(tone) = tone {
        title = title.foreground(tone);
    }
    Grid::new()
        .margin(Thickness::new(indent, 0.0, 0.0, 0.0))
        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::SM)
        .children((
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .maybe_content(leading.into()),
            title.grid_column(1),
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .maybe_content(trailing.into()),
        ))
        .into()
}

/// A section label row: the label, its actions at the end.
pub fn section_label_row(value: &str, actions: impl Into<Option<View>>) -> View {
    Border::new()
        .min_height(height::CONTROL)
        .padding(Thickness::new(spacing::ROW, 0.0, spacing::XS, 0.0))
        .content(
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .children((
                    section_label(value.to_owned()).vertical_alignment(VerticalAlignment::Center),
                    Border::new()
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .maybe_content(actions.into()),
                )),
        )
        .into()
}

/// A quota meter: WinUI's determinate progress bar at the share `value`
/// (0 to 100), in its error state when `low`.
pub fn meter(value: f64, low: bool) -> View {
    ProgressBar::new()
        .minimum(0.0)
        .maximum(100.0)
        .value(value.clamp(0.0, 100.0))
        .show_error(low)
        .vertical_alignment(VerticalAlignment::Center)
        .into()
}
