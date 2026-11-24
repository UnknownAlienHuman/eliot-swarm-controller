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

use crate::shadow::ShadowHost;
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

pub use crate::brand::glyph;

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

/// `view` lifted off its surface at `elevation-1`, on a corner of
/// `radius`: a real shadow under it ([`ShadowHost`], whose layers the
/// owner keeps on the theme's, [`elevation_layers`]) or, in the true-black
/// themes, the 1-DIP ring. `place` is the grid that sizes and places both,
/// so the lift follows the view; under High Contrast nothing lifts.
pub fn lifted(
    palette: Palette,
    shadow: &ShadowHost,
    radius: f64,
    place: Grid,
    view: impl Into<View>,
) -> View {
    let lift: View = match palette.roles().map(|roles| roles.lift) {
        Some(crate::theme::Lift::Shadow(_)) => shadow.view().into(),
        Some(crate::theme::Lift::Ring(color)) => Border::new()
            .corner_radius(radius + layout::HAIRLINE)
            .margin(Thickness::uniform(-layout::HAIRLINE))
            .background(Brush::Solid(crate::theme::winui::color(color)))
            .into(),
        None => View::empty(),
    };
    place.keyed_children(vec![
        KeyedView::new("lift", lift),
        KeyedView::new("content", view.into()),
    ])
}

/// The brand book's `elevation-1` layers in `palette`'s theme: none in the
/// true-black themes (their ring is drawn by [`lifted`]) and under High
/// Contrast.
pub fn elevation_layers(palette: Palette) -> &'static [crate::brand::Shadow] {
    match palette.roles().map(|roles| roles.lift) {
        Some(crate::theme::Lift::Shadow(layers)) => layers,
        _ => &[],
    }
}

/// The night themes' glow behind the composer's main action
/// (`glow-shadow`); none elsewhere and under High Contrast.
pub fn glow_layers(palette: Palette) -> &'static [crate::brand::Shadow] {
    palette.roles().map_or(&[], |roles| roles.glow)
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
/// that always takes a worktree): a chip that takes no input, drawn as the
/// chip at rest (its outline, its icon and label in `on-surface-variant`;
/// Full access in the attention tone). It is a disabled chip carrying the
/// rest colors, so its icon takes its color too (Reactor 0.100.0 colors an
/// icon only through the control around it); its tooltip goes on the
/// cell around it.
pub fn fixed_chip(
    palette: Palette,
    attention: bool,
    glyph: Option<&str>,
    label: &str,
    name: &str,
) -> View {
    Border::new()
        .vertical_alignment(VerticalAlignment::Center)
        .content(
            Button::new()
                .resource_overrides(palette.fixed_chip(attention))
                .is_enabled(false)
                .height(height::CHIP)
                .min_height(height::CHIP)
                .automation_name(name.to_owned())
                .content(chip_content(
                    glyph,
                    label,
                    false,
                    layout::PROJECT_CHIP_WIDTH,
                )),
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

/// A suggestion card (Home, `.suggest`): the card corner, tonal, lifting
/// under the pointer; its icon and label in `on-surface`. Its hairline is
/// [`suggestion_card`]'s (the card style draws no border).
pub fn suggestion(palette: Palette) -> Button {
    Button::new()
        .resource_overrides(palette.suggestion())
        .style(ButtonStyle::Accent)
        .height(height::SUGGESTION - 2.0 * layout::HAIRLINE)
        .min_height(height::SUGGESTION - 2.0 * layout::HAIRLINE)
        .horizontal_content_alignment(HorizontalAlignment::Left)
}

/// A suggestion card's `outline-variant` hairline around its button.
pub fn suggestion_card(palette: Palette, card: impl Into<View>) -> View {
    Border::new()
        .border_brush(palette.outline())
        .border_thickness(Thickness::uniform(layout::HAIRLINE))
        .corner_radius(radius::CONTAINER + layout::HAIRLINE)
        .content(card)
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

/// The composer card: the composer role, the extra-large corner, lifted
/// to `elevation-1` (`shadow`, its owner's), padded 12/12/10/16. `place`
/// sizes it.
pub fn composer_card(
    palette: Palette,
    shadow: &ShadowHost,
    place: Grid,
    content: impl Into<View>,
) -> View {
    lifted(
        palette,
        shadow,
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

/// An Expressive shape (`brand::SHAPE_*` path data in a 100-unit box) at
/// `size` DIPs: a `PathIcon` scaled by a `Viewbox`. It takes the
/// foreground of the control around it.
pub fn shape(data: &str, size: f64) -> View {
    Viewbox::new()
        .width(size)
        .height(size)
        .vertical_alignment(VerticalAlignment::Center)
        .slot(ViewboxSlot::Child, PathIcon::new().data(data.to_owned()))
}

/// `content` in the color `pick` names where it is not a pressable (the
/// daemon screen's shape, a failed row's error icon): a disabled carrier
/// button, which takes no focus and no input (a press reaches the row
/// around it). Reactor 0.100.0 colors an icon or shape only through the
/// control around it. `name` is what a screen reader says.
pub fn carrier(
    palette: Palette,
    pick: fn(&crate::brand::Scheme) -> u32,
    name: &str,
    size: f64,
    content: impl Into<View>,
) -> View {
    Button::new()
        .resource_overrides(palette.carrier(pick))
        .style(ButtonStyle::Subtle)
        .is_enabled(false)
        .width(size)
        .height(size)
        .min_height(size)
        .vertical_alignment(VerticalAlignment::Center)
        .automation_name(name.to_owned())
        .content(content)
}

/// A counter (the Inbox count): `tertiary-container`, a 16-DIP pill,
/// `label-small-emphasized`; more than 99 reads "99+".
pub fn count_badge(palette: Palette, count: usize) -> View {
    let (container, on) = palette.badge();
    let label = if count > 99 {
        "99+".to_owned()
    } else {
        count.to_string()
    };
    Border::new()
        .min_width(layout::BADGE)
        .height(layout::BADGE)
        .corner_radius(layout::BADGE / 2.0)
        .background(container)
        .padding(Thickness::xy(spacing::XS, 0.0))
        .vertical_alignment(VerticalAlignment::Center)
        .content(
            text(label, style::BADGE)
                .foreground(on)
                .horizontal_alignment(HorizontalAlignment::Center)
                .vertical_alignment(VerticalAlignment::Center),
        )
}

/// A sidebar row's trailing word, age or shortcut (`label-small`, the
/// variant tone; on the selection, its content color).
pub fn trailing(value: impl Into<String>, selected: bool) -> TextBlock {
    let block = text(value, style::TRAILING).vertical_alignment(VerticalAlignment::Center);
    if selected {
        block.foreground(palette().on_selected())
    } else {
        block.foreground(palette().variant())
    }
}

/// A task row's `+N −M` (`label-small`: `add` and `error`; on the
/// selection, its content color).
pub fn diff_counts(insertions: u64, deletions: u64, selected: bool) -> View {
    let palette = palette();
    let (add, del) = if selected {
        (palette.on_selected(), palette.on_selected())
    } else {
        (palette.add(), palette.attention())
    };
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(spacing::XS)
        .vertical_alignment(VerticalAlignment::Center)
        .children((
            text(format!("+{}", group_digits(insertions)), style::TRAILING).foreground(add),
            text(
                format!("\u{2212}{}", group_digits(deletions)),
                style::TRAILING,
            )
            .foreground(del),
        ))
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

/// A task row's state mark (brand book, Color, status in rows).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowMark {
    /// Working or starting: the loader in `primary`.
    Working,
    /// Waiting for you: an 8-DIP `tertiary` dot.
    Waiting,
    /// Idle or ready: a 6-DIP `outline` dot.
    Idle,
    /// Queued: the `schedule` icon (the row's variant content).
    Queued,
    /// Failed, lost or unknown: the `error` icon in `error`.
    Failed,
    /// Stopped: no mark.
    None,
}

/// The view of a row's state mark: dots and the loader in a 16-DIP slot,
/// the icons in the 20-DIP icon slot (as the design draws them). `word`
/// names a carried icon for a screen reader.
pub fn row_mark(palette: Palette, mark: RowMark, word: &str) -> View {
    let slot = |content: View| -> View {
        Border::new()
            .width(layout::MARK_SLOT)
            .height(layout::MARK_SLOT)
            .vertical_alignment(VerticalAlignment::Center)
            .content(content)
    };
    let dot = |fill: Brush, size: f64| -> View {
        Ellipse::new()
            .fill(fill)
            .width(size)
            .height(size)
            .horizontal_alignment(HorizontalAlignment::Center)
            .vertical_alignment(VerticalAlignment::Center)
            .into()
    };
    match mark {
        // WinUI's indeterminate ring in `primary` (the app's ring
        // resources): the brand's shape loader needs a re-templated
        // control (the motion increment).
        RowMark::Working => slot(
            ProgressRing::new()
                .is_indeterminate(true)
                .width(layout::LOADER_SMALL)
                .height(layout::LOADER_SMALL)
                .min_width(layout::LOADER_SMALL)
                .min_height(layout::LOADER_SMALL)
                .horizontal_alignment(HorizontalAlignment::Center)
                .vertical_alignment(VerticalAlignment::Center)
                .into(),
        ),
        RowMark::Waiting => slot(dot(palette.waiting(), layout::DOT_WAIT)),
        RowMark::Idle => slot(dot(palette.idle_dot(), layout::DOT_IDLE)),
        RowMark::Queued => icon(glyph::SCHEDULE),
        RowMark::Failed => carrier(
            palette,
            |s| s.error,
            word,
            icon_size::ROW,
            icon(glyph::ERROR),
        ),
        RowMark::None => View::empty(),
    }
}

/// A sidebar row's content (`Sidebar.dc.html` `.row`): the leading mark
/// or icon, the label (`label-large`; emphasized when selected) and the
/// trailing view, 8 DIPs apart; the row's inset is 10 at the start and 8
/// at the end; a worker `indent`s past its manager's mark, its label at
/// the kid size. `tone` recolors the label alone (a stopped task's).
pub fn sidebar_row(
    leading: View,
    label: &str,
    selected: bool,
    indent: f64,
    kid: bool,
    tone: Option<Brush>,
    trailing: View,
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
        .margin(Thickness::new(indent, 0.0, spacing::SM - spacing::ROW, 0.0))
        .columns([GridLength::Auto, GridLength::STAR, GridLength::Auto])
        .column_spacing(spacing::SM)
        .children((
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(leading),
            title.grid_column(1),
            Border::new()
                .grid_column(2)
                .vertical_alignment(VerticalAlignment::Center)
                .content(trailing),
        ))
}

/// A nav row's icon: outlined, filled when its row is selected.
pub fn nav_icon(glyph: &str, selected: bool) -> View {
    if selected {
        icon(&glyph::filled(glyph))
    } else {
        icon(glyph)
    }
}

/// A section label row (`.lab`): the label in `label-medium`, the variant
/// tone, 10 DIPs in, its actions at the end (4 DIPs in).
pub fn section_label_row(value: &str, actions: View) -> View {
    Border::new()
        .height(height::CONTROL)
        .padding(Thickness::new(spacing::ROW, 0.0, spacing::XS, 0.0))
        .content(
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .children((
                    muted(value.to_owned(), style::LABEL)
                        .automation_heading_level(AutomationHeadingLevel::Level3)
                        .vertical_alignment(VerticalAlignment::Center),
                    Border::new()
                        .grid_column(1)
                        .vertical_alignment(VerticalAlignment::Center)
                        .content(actions),
                )),
        )
}

/// A thin quota meter: the share `value` (0 to 100) in `primary` (`error`
/// when `low`) on a `secondary-container` track, fully rounded.
pub fn meter(palette: Palette, value: f64, low: bool) -> View {
    let value = value.clamp(0.0, 100.0);
    let fill = if low {
        palette.attention()
    } else {
        palette.primary()
    };
    Border::new()
        .height(layout::METER)
        .corner_radius(layout::METER / 2.0)
        .background(palette.meter_track())
        .content(
            Grid::new()
                .columns([GridLength::Star(value), GridLength::Star(100.0 - value)])
                .children((Border::new()
                    .corner_radius(layout::METER / 2.0)
                    .background(fill),)),
        )
}
