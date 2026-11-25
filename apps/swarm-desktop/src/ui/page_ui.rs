//! The pages in the main card (`screens.css` `.page`, the canvas boards
//! Project, Inbox, Agents, Automations and Settings): one centred column
//! with the page padding, a title row, section headings, panels with a
//! hairline, key-value lines, stats and filter chips.
//! Sizes and type come from [`tokens::page`]; colors from WinUI's control
//! styles. The pages share these pieces, so a board's part is drawn once.

use super::*;
use eliot_core::AgentState;

/// Where a page leads, for the sidebar's places and Back/Forward.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) enum Place {
    Inbox,
    Agents,
    Automations,
}

/// A scrolling page: its parts in one centred column at most `widest`
/// wide (see [`crate::views::Column`]), 24 DIPs from the card's top and
/// bottom, 16 DIPs apart.
pub(super) fn page(name: &str, widest: f64, items: Vec<KeyedView>) -> View {
    ScrollViewer::new()
        .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
        .horizontal_scroll_bar_visibility(ScrollBarVisibility::Disabled)
        .automation_name(name)
        .content(
            Border::new().content(
                StackPanel::new()
                    .column(widest, tokens::spacing::LG, tokens::spacing::LG)
                    .spacing(tokens::spacing::MD)
                    .keyed_children(items),
            ),
        )
        .into()
}

/// A page's title.
pub(super) fn title(value: impl Into<String>) -> TextBlock {
    controls::text(value, tokens::page::TITLE)
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
        .into()
}

/// A section heading on a page or in a panel.
pub(super) fn heading(value: impl Into<String>) -> TextBlock {
    controls::text(value, tokens::page::HEADING)
        .automation_heading_level(AutomationHeadingLevel::Level2)
        .vertical_alignment(VerticalAlignment::Center)
}

/// A heading between a page's groups: 8 DIPs more above it.
pub(super) fn group_heading(value: impl Into<String>) -> View {
    heading(value)
        .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
        .into()
}

/// A panel (`.panel`): a hairline, the medium corner, 12/16 inside, its parts 8 DIPs apart
/// (`gap` overrides it).
pub(super) fn panel(gap: f64, children: Vec<KeyedView>) -> View {
    Border::new()
        .background(ThemeBrush::CardBackground)
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
        .corner_radius(tokens::radius::CONTAINER)
        .padding(Thickness::xy(
            tokens::spacing::MD - tokens::layout::HAIRLINE,
            tokens::spacing::INSET - tokens::layout::HAIRLINE,
        ))
        .vertical_alignment(VerticalAlignment::Top)
        .content(StackPanel::new().spacing(gap).keyed_children(children))
        .into()
}

/// A panel's head: its heading and an optional action at the end.
pub(super) fn panel_head(value: &str, action: Option<View>) -> View {
    Grid::new()
        .min_height(tokens::height::CONTROL)
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(tokens::spacing::SM)
        .children((
            heading(value.to_owned()),
            crate::views::MaybeContent::maybe_content(
                Border::new()
                    .grid_column(1)
                    .vertical_alignment(VerticalAlignment::Center),
                action,
            ),
        ))
        .into()
}

/// A key-value line (`.kv`): the key muted in its column,
/// the value after it.
pub(super) fn kv(key: &str, value: impl Into<View>) -> View {
    Grid::new()
        .columns([
            GridLength::Pixel(tokens::page::KEY_COLUMN),
            GridLength::STAR,
        ])
        .column_spacing(tokens::spacing::SM)
        .children((
            controls::text(key.to_owned(), tokens::text::BODY),
            Border::new().grid_column(1).content(value),
        ))
        .into()
}

/// A key-value line whose value is text (wrapped, selectable).
pub(super) fn kv_text(key: &str, value: impl Into<String>) -> View {
    kv(
        key,
        controls::body(value)
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
            controls::text(value, tokens::page::STAT),
            controls::secondary(label.to_owned()).text_wrapping(TextWrapping::Wrap),
        ));
    match tip {
        Some(tip) => body.tooltip(tip),
        None => body.into(),
    }
}

/// A stat in its own panel (the Agents summary).
pub(super) fn stat_panel(stat: View) -> View {
    panel(tokens::spacing::SM, vec![KeyedView::new("stat", stat)])
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
        .into()
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
        .into()
}

/// A filter chip with its count (`.chip`, "Working 3"): selected, it
/// fills with the accent and shows the check.
pub(super) fn filter_chip(
    label: &str,
    count: Option<usize>,
    selected: bool,
    on_click: Callback<()>,
) -> View {
    let text = match count {
        Some(count) => format!("{label} {count}"),
        None => label.to_owned(),
    };
    let glyph = selected.then_some(controls::glyph::CHECK);
    controls::segment(selected)
        .automation_name(if selected {
            format!("{text} (shown)")
        } else {
            format!("Show {text}")
        })
        .on_click(on_click)
        .content(controls::chip_content(glyph, &text, false))
        .into()
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
        .into()
}

/// An age or count at a row's end (`.age`, muted).
pub(super) fn age(value: impl Into<String>) -> TextBlock {
    controls::text(value, tokens::page::AGE).vertical_alignment(VerticalAlignment::Center)
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
pub(crate) fn agent_mark(state: AgentState, status_line: Option<&str>) -> Option<View> {
    if held(status_line) {
        Some(controls::row_mark(
            controls::RowMark::Waiting,
            "Provider hold",
        ))
    } else {
        state_mark(state)
    }
}

/// An Agent's state mark in a page row, the sidebar's marks: the loader
/// while it works, the waiting and idle dots, the error icon, none when
/// stopped.
pub(crate) fn state_mark(state: AgentState) -> Option<View> {
    let (mark, word) = match state {
        AgentState::Created | AgentState::Starting | AgentState::Busy => {
            (Some(controls::RowMark::Working), "Working")
        }
        AgentState::Waiting => (Some(controls::RowMark::Waiting), "Waiting for you"),
        AgentState::Idle => (Some(controls::RowMark::Idle), "Ready"),
        AgentState::Failed => (Some(controls::RowMark::Failed), "Failed"),
        AgentState::Lost => (Some(controls::RowMark::Failed), "Lost"),
        AgentState::Unknown => (Some(controls::RowMark::Failed), "Unknown"),
        AgentState::Stopped => (None, "Stopped"),
    };
    mark.map(|mark| controls::row_mark(mark, word))
}

/// A row's text (`.item .tx`): the title and one supporting line, both
/// trimmed to the row.
pub(super) fn two_lines(title: &str, line: &str) -> View {
    let mut parts: Vec<KeyedView> = vec![KeyedView::new(
        "title",
        controls::text(title.to_owned(), tokens::page::HEADING)
            .text_trimming(TextTrimming::CharacterEllipsis),
    )];
    if !line.is_empty() {
        parts.push(KeyedView::new(
            "line",
            controls::secondary(line.to_owned()).text_trimming(TextTrimming::CharacterEllipsis),
        ));
    }
    StackPanel::new()
        .vertical_alignment(VerticalAlignment::Center)
        .keyed_children(parts)
        .into()
}

/// A line of text in the error color (a failed read, a refusal).
pub(super) fn error_line(value: impl Into<String>) -> View {
    controls::text(value, tokens::text::CAPTION)
        .foreground(ThemeBrush::SystemCritical)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
        .into()
}

/// A failed read with its retry (`Retry`, `Read again`).
pub(super) fn read_failed(
    text: impl Into<String>,
    retry: &str,
    enabled: bool,
    on_click: Callback<()>,
) -> View {
    let text = text.into();
    StackPanel::new()
        .spacing(tokens::spacing::XS)
        .children((
            error_line(text),
            controls::outlined()
                .horizontal_alignment(HorizontalAlignment::Left)
                .is_enabled(enabled)
                .automation_name(retry)
                .on_click(on_click)
                .content(controls::label(retry)),
        ))
        .into()
}

/// A page's empty state or reading line: one muted sentence.
pub(super) fn quiet(value: impl Into<String>) -> View {
    controls::secondary(value)
        .text_wrapping(TextWrapping::Wrap)
        .into()
}

/// A segmented choice (`.group`: By state / By project, 24 hours /
/// 7 days): one segment per label, the selected one filled.
pub(super) fn choice(labels: &[(&str, bool, Callback<()>)]) -> View {
    let segments: Vec<KeyedView> = labels
        .iter()
        .map(|(label, selected, click)| {
            KeyedView::new(
                format!("{label}:{selected}"),
                controls::segment(*selected)
                    .automation_name(if *selected {
                        format!("{label} (chosen)")
                    } else {
                        (*label).to_owned()
                    })
                    .on_click(click.clone())
                    .content(controls::segment_label(label)),
            )
        })
        .collect();
    controls::segmented(segments)
}
