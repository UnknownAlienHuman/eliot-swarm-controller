//! The pages in the main card (`screens.css` `.page`, the canvas boards
//! Project, Inbox, Agents, Automations and Settings): one centred column
//! with the page padding, a `headline-medium` title row, `title-small`
//! section headings, panels on `surface-container-low` with an
//! `outline-variant` hairline, key-value lines, stats and filter chips.
//! Sizes and type come from [`tokens::page`]; colors from the theme's
//! roles. The pages share these pieces, so a board's part is drawn once.

use super::*;
use crate::theme::winui::Palette;
use eliot_core::AgentState;

/// Where a page leads, for the sidebar's places and Back/Forward.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Place {
    Inbox,
    Agents,
    Automations,
}

impl DesktopApp {
    /// A page column's width: the card's width less the page's side
    /// padding, at most `max` (content-sized until the window size is
    /// observed). An explicit width, as Settings has: a column sized by an
    /// auto cell would follow its widest text past the card.
    pub(super) fn page_width(&self, max: f64) -> Option<f64> {
        self.main_width()
            .map(|width| (width - 2.0 * tokens::spacing::XL).clamp(tokens::layout::MIN_COLUMN, max))
    }
}

/// A scrolling page: its parts in one column `width` wide
/// ([`DesktopApp::page_width`]), centred, 24 DIPs from the card's top and
/// bottom, 16 DIPs apart.
pub(super) fn page(name: &str, width: Option<f64>, items: Vec<KeyedView>) -> View {
    ScrollViewer::new()
        .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
        .horizontal_scroll_bar_visibility(ScrollBarVisibility::Disabled)
        .automation_name(name.to_owned())
        .content(
            StackPanel::new()
                .width(width)
                .horizontal_alignment(HorizontalAlignment::Center)
                .margin(Thickness::new(
                    0.0,
                    tokens::spacing::LG,
                    0.0,
                    tokens::spacing::LG,
                ))
                .spacing(tokens::spacing::MD)
                .keyed_children(items),
        )
}

/// A page's title (`headline-medium`).
pub(super) fn title(value: impl Into<String>) -> TextBlock {
    m3::text(value, tokens::page::TITLE)
        .automation_heading_level(AutomationHeadingLevel::Level1)
        .vertical_alignment(VerticalAlignment::Center)
        .text_trimming(TextTrimming::CharacterEllipsis)
}

/// A toolbar row (`.toolbar-row`): the leading part, then the actions at
/// the end, 8 DIPs apart.
pub(super) fn toolbar(lead: impl Into<View>, actions: Vec<KeyedView>) -> View {
    Grid::new()
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(tokens::spacing::SM)
        .children((
            Border::new()
                .vertical_alignment(VerticalAlignment::Center)
                .content(lead),
            StackPanel::new()
                .grid_column(1)
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .vertical_alignment(VerticalAlignment::Center)
                .keyed_children(actions),
        ))
}

/// A section heading on a page or in a panel (`title-small`, the variant
/// tone).
pub(super) fn heading(value: impl Into<String>) -> TextBlock {
    m3::muted(value, tokens::page::HEADING)
        .automation_heading_level(AutomationHeadingLevel::Level2)
        .vertical_alignment(VerticalAlignment::Center)
}

/// A heading between a page's groups: 8 DIPs more above it.
pub(super) fn group_heading(value: impl Into<String>) -> View {
    heading(value)
        .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
        .into()
}

/// A panel (`.panel`): `surface-container-low`, an `outline-variant`
/// hairline, the medium corner, 12/16 inside, its parts 8 DIPs apart
/// (`gap` overrides it).
pub(super) fn panel(palette: Palette, gap: f64, children: Vec<KeyedView>) -> View {
    Border::new()
        .background(palette.tonal())
        .border_brush(palette.outline())
        .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
        .corner_radius(tokens::radius::CONTAINER)
        .padding(Thickness::xy(
            tokens::spacing::MD - tokens::layout::HAIRLINE,
            tokens::spacing::INSET - tokens::layout::HAIRLINE,
        ))
        .vertical_alignment(VerticalAlignment::Top)
        .content(StackPanel::new().spacing(gap).keyed_children(children))
}

/// A panel's head: its heading and an optional action at the end.
pub(super) fn panel_head(value: &str, action: Option<View>) -> View {
    Grid::new()
        .min_height(tokens::height::CONTROL)
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(tokens::spacing::SM)
        .children((
            heading(value.to_owned()),
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center)
                .content(action.unwrap_or_else(View::empty)),
        ))
}

/// A key-value line (`.kv`): the key in the variant tone in its column,
/// the value after it.
pub(super) fn kv(key: &str, value: impl Into<View>) -> View {
    Grid::new()
        .columns([
            GridLength::Pixel(tokens::page::KEY_COLUMN),
            GridLength::STAR,
        ])
        .column_spacing(tokens::spacing::SM)
        .children((
            m3::muted(key.to_owned(), tokens::text::BODY),
            Border::new().grid_column(1).content(value),
        ))
}

/// A key-value line whose value is text (wrapped, selectable).
pub(super) fn kv_text(key: &str, value: impl Into<String>) -> View {
    kv(
        key,
        m3::body(value)
            .text_wrapping(TextWrapping::Wrap)
            .is_text_selection_enabled(true),
    )
}

/// A stat (`.stat`): the number in `title-large`, its label under it.
/// `tip` says where the number comes from (or why it is partial).
pub(super) fn stat(value: impl Into<String>, label: &str, tip: Option<String>) -> View {
    let value = value.into();
    let body = StackPanel::new()
        .automation_name(format!("{label}: {value}"))
        .children((
            m3::text(value, tokens::page::STAT),
            m3::secondary(label.to_owned()).text_wrapping(TextWrapping::Wrap),
        ));
    match tip {
        Some(tip) => body.tooltip(tip),
        None => body,
    }
}

/// A stat in its own panel (the Agents summary).
pub(super) fn stat_panel(palette: Palette, stat: View) -> View {
    panel(
        palette,
        tokens::spacing::SM,
        vec![KeyedView::new("stat", stat)],
    )
}

/// Equal columns of `views`, 16 DIPs apart (`.grid3`, `.grid4`).
pub(super) fn grid(views: Vec<View>) -> View {
    let columns: Vec<GridLength> = views.iter().map(|_| GridLength::STAR).collect();
    let cells: Vec<KeyedView> = views
        .into_iter()
        .enumerate()
        .map(|(index, view)| {
            KeyedView::new(
                format!("cell:{index}"),
                Border::new().grid_column(index as i32).content(view),
            )
        })
        .collect();
    Grid::new()
        .columns(columns)
        .column_spacing(tokens::spacing::MD)
        .keyed_children(cells)
}

/// The two columns of the project page and Automations (`.cols`): three
/// fifths and two fifths, 16 DIPs apart, each a stack 16 DIPs apart (`gap`
/// for the left one).
pub(super) fn columns(left: Vec<KeyedView>, left_gap: f64, right: Vec<KeyedView>) -> View {
    Grid::new()
        .columns([GridLength::Star(3.0), GridLength::Star(2.0)])
        .column_spacing(tokens::spacing::MD)
        .children((
            StackPanel::new()
                .spacing(left_gap)
                .vertical_alignment(VerticalAlignment::Top)
                .keyed_children(left),
            StackPanel::new()
                .grid_column(1)
                .spacing(tokens::spacing::MD)
                .vertical_alignment(VerticalAlignment::Top)
                .keyed_children(right),
        ))
}

/// A filter chip with its count (`.chip`, "Working 3"): selected, it
/// fills with `secondary-container` and shows the check.
pub(super) fn filter_chip(
    palette: Palette,
    label: &str,
    count: Option<usize>,
    selected: bool,
    on_click: Callback<()>,
) -> View {
    let text = match count {
        Some(count) => format!("{label} {count}"),
        None => label.to_owned(),
    };
    let glyph = selected.then_some(m3::glyph::CHECK);
    m3::chip(palette)
        .resource_overrides(selected_chip(palette, selected))
        .automation_name(if selected {
            format!("{text} (shown)")
        } else {
            format!("Show {text}")
        })
        .on_click(on_click)
        .content(m3::chip_content(
            glyph,
            &text,
            false,
            tokens::layout::PROJECT_CHIP_WIDTH,
        ))
}

/// A wrapping row of chips 6 DIPs apart (`.filters`). Reactor 0.100.0 has
/// no wrap panel, so the chips sit in rows of at most `per_row`.
pub(super) fn chip_rows(chips: Vec<KeyedView>, per_row: usize) -> View {
    let mut rows: Vec<KeyedView> = Vec::new();
    let mut current: Vec<KeyedView> = Vec::new();
    for chip in chips {
        current.push(chip);
        if current.len() == per_row.max(1) {
            rows.push(KeyedView::new(
                format!("chips:{}", rows.len()),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(tokens::spacing::CHIP_GAP)
                    .keyed_children(std::mem::take(&mut current)),
            ));
        }
    }
    if !current.is_empty() {
        rows.push(KeyedView::new(
            format!("chips:{}", rows.len()),
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::CHIP_GAP)
                .keyed_children(current),
        ));
    }
    StackPanel::new()
        .spacing(tokens::spacing::CHIP_GAP)
        .keyed_children(rows)
}

/// An age or count at a row's end (`.age`, `label-small`, the variant
/// tone).
pub(super) fn age(value: impl Into<String>) -> TextBlock {
    m3::muted(value, tokens::page::AGE).vertical_alignment(VerticalAlignment::Center)
}

/// A chip's colors: selected, `secondary-container` without the outline;
/// at rest, the outlined chip.
pub(super) fn selected_chip(palette: Palette, selected: bool) -> ResourceOverrides {
    if selected {
        palette.segment(true)
    } else {
        palette.chip()
    }
}

/// Whether an Agent's status line says its provider holds it (the
/// sidebar's test): it then waits, whatever its state.
pub(crate) fn held(status_line: Option<&str>) -> bool {
    status_line.is_some_and(|line| {
        line.starts_with("Provider hold") || line.starts_with("Waiting for the provider")
    })
}

/// An Agent's mark in a page row: a provider hold waits (the waiting dot),
/// else its state's mark ([`state_mark`]).
pub(crate) fn agent_mark(palette: Palette, state: AgentState, status_line: Option<&str>) -> View {
    if held(status_line) {
        m3::row_mark(palette, m3::RowMark::Waiting, "Provider hold")
    } else {
        state_mark(palette, state)
    }
}

/// An Agent's state mark in a page row (brand book, Color, status in
/// rows), the sidebar's marks: the loader while it works, the waiting and
/// idle dots, the error icon, none when stopped.
pub(crate) fn state_mark(palette: Palette, state: AgentState) -> View {
    let (mark, word) = match state {
        AgentState::Created | AgentState::Starting | AgentState::Busy => {
            (m3::RowMark::Working, "Working")
        }
        AgentState::Waiting => (m3::RowMark::Waiting, "Waiting for you"),
        AgentState::Idle => (m3::RowMark::Idle, "Ready"),
        AgentState::Failed => (m3::RowMark::Failed, "Failed"),
        AgentState::Lost => (m3::RowMark::Failed, "Lost"),
        AgentState::Unknown => (m3::RowMark::Failed, "Unknown"),
        AgentState::Stopped => (m3::RowMark::None, "Stopped"),
    };
    m3::row_mark(palette, mark, word)
}

/// A row's text (`.item .tx`): the title in `title-small` and one
/// supporting line in `body-small`, both trimmed to the row.
pub(super) fn two_lines(title: &str, line: &str) -> View {
    let mut parts: Vec<KeyedView> = vec![KeyedView::new(
        "title",
        m3::text(title.to_owned(), tokens::page::HEADING)
            .text_trimming(TextTrimming::CharacterEllipsis),
    )];
    if !line.is_empty() {
        parts.push(KeyedView::new(
            "line",
            m3::secondary(line.to_owned()).text_trimming(TextTrimming::CharacterEllipsis),
        ));
    }
    StackPanel::new()
        .vertical_alignment(VerticalAlignment::Center)
        .keyed_children(parts)
}

/// A line of text in the error color (a failed read, a refusal).
pub(super) fn error_line(palette: Palette, value: impl Into<String>) -> View {
    m3::text(value, tokens::text::CAPTION)
        .foreground(palette.attention())
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
        .into()
}

/// A failed read with its retry (`Retry`, `Read again`).
pub(super) fn read_failed(
    palette: Palette,
    text: impl Into<String>,
    retry: &str,
    enabled: bool,
    on_click: Callback<()>,
) -> View {
    let text = text.into();
    StackPanel::new().spacing(tokens::spacing::XS).children((
        error_line(palette, text),
        m3::outlined(palette)
            .horizontal_alignment(HorizontalAlignment::Left)
            .is_enabled(enabled)
            .automation_name(retry.to_owned())
            .on_click(on_click)
            .content(m3::label(retry)),
    ))
}

/// A page's empty state or reading line: one sentence in the variant
/// tone.
pub(super) fn quiet(value: impl Into<String>) -> View {
    m3::secondary(value)
        .text_wrapping(TextWrapping::Wrap)
        .into()
}

/// A segmented choice (`.group`: By state / By project, 24 hours /
/// 7 days): one segment per label, the selected one filled.
pub(super) fn choice(palette: Palette, labels: &[(&str, bool, Callback<()>)]) -> View {
    let segments: Vec<KeyedView> = labels
        .iter()
        .map(|(label, selected, click)| {
            KeyedView::new(
                format!("{label}:{selected}"),
                m3::segment(palette, *selected)
                    .automation_name(if *selected {
                        format!("{label} (chosen)")
                    } else {
                        (*label).to_owned()
                    })
                    .on_click(click.clone())
                    .content(m3::segment_label(label)),
            )
        })
        .collect();
    m3::segmented(palette, segments)
}
