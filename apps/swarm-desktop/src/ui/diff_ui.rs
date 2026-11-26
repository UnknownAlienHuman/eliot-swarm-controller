//! One file's patch in Changes (brand book §11.1): an `agent.diff` file
//! record parsed once, off the UI thread, into numbered lines, then shown
//! Unified or Split through a virtualizing `ItemsRepeater`, so a long file
//! builds only the rows in view rather than thousands before the first.
//!
//! Line numbers, the `+`/`−` signs, the Added/Removed words in each row's
//! name and the Split columns carry the meaning; the signs' color only
//! supplements them, so the patch reads the same in High Contrast.
//! Monospace is limited to the patch text.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use eliot_core::api::GitDiffFile;

use super::*;

/// What one shown patch line is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// A hunk header (`@@ -12,5 +12,7 @@ fn name`).
    Hunk,
    Context,
    Added,
    Removed,
    /// Git's note on the line before (`\ No newline at end of file`).
    Note,
}

#[derive(Clone, Debug)]
struct Line {
    kind: Kind,
    old: Option<u32>,
    new: Option<u32>,
    text: String,
}

/// A Split row: a line on both sides (context), the old side's and the new
/// side's lines of one change, or a hunk header or note across both.
#[derive(Clone, Copy, Debug)]
enum SplitRow {
    Across(usize),
    Sides(Option<usize>, Option<usize>),
}

/// One file's patch, ready for the UI thread.
#[derive(Debug)]
pub(super) struct Parsed {
    lines: Vec<Line>,
    split: Vec<SplitRow>,
    /// Digits of the largest line number, so the number columns line up
    /// in the monospace face.
    digits: usize,
    /// Added and removed lines, known only for a whole text patch.
    pub(super) counts: Option<(u64, u64)>,
}

/// Parse a file record's bounded patch text. File header lines (`diff
/// --git`, `index`, modes, `---`/`+++`) are left out: the file's card
/// states the rename, modes and binary from the record's own fields.
pub(super) fn parse(file: &GitDiffFile) -> Parsed {
    let mut lines = Vec::new();
    let (mut old, mut new) = (0_u32, 0_u32);
    let (mut added, mut removed) = (0_u64, 0_u64);
    // Before the first hunk, and inside a combined (`@@@`) hunk whose two
    // sign columns this view does not number.
    let mut in_hunk = false;
    let mut combined = false;
    for raw in file.text.split('\n') {
        let line = raw.strip_suffix('\r').unwrap_or(raw);
        if line.starts_with("@@") {
            in_hunk = true;
            combined = line.starts_with("@@@");
            if let Some((first_old, first_new)) = hunk_start(line) {
                (old, new) = (first_old, first_new);
            }
            lines.push(Line {
                kind: Kind::Hunk,
                old: None,
                new: None,
                text: line.to_owned(),
            });
            continue;
        }
        if !in_hunk || line.is_empty() {
            continue;
        }
        if combined {
            lines.push(Line {
                kind: Kind::Context,
                old: None,
                new: None,
                text: line.to_owned(),
            });
            continue;
        }
        let mut chars = line.chars();
        let sign = chars.next();
        let text = chars.as_str().to_owned();
        match sign {
            Some('+') => {
                lines.push(Line {
                    kind: Kind::Added,
                    old: None,
                    new: Some(new),
                    text,
                });
                new = new.saturating_add(1);
                added += 1;
            }
            Some('-') => {
                lines.push(Line {
                    kind: Kind::Removed,
                    old: Some(old),
                    new: None,
                    text,
                });
                old = old.saturating_add(1);
                removed += 1;
            }
            Some(' ') => {
                lines.push(Line {
                    kind: Kind::Context,
                    old: Some(old),
                    new: Some(new),
                    text,
                });
                old = old.saturating_add(1);
                new = new.saturating_add(1);
            }
            Some('\\') => lines.push(Line {
                kind: Kind::Note,
                old: None,
                new: None,
                text: text.trim().to_owned(),
            }),
            _ => {}
        }
    }
    let largest = old.max(new).max(1);
    Parsed {
        split: split_rows(&lines),
        lines,
        digits: largest.to_string().len(),
        counts: (!file.binary && !file.truncated).then_some((added, removed)),
    }
}

/// The first old and new line numbers of `@@ -a[,b] +c[,d] @@`.
fn hunk_start(header: &str) -> Option<(u32, u32)> {
    let mut fields = header.trim_start_matches('@').split_whitespace();
    let number = |field: Option<&str>, sign: char| -> Option<u32> {
        let field = field?.strip_prefix(sign)?;
        field.split(',').next()?.parse().ok()
    };
    let old = number(fields.next(), '-')?;
    let new = number(fields.next(), '+')?;
    Some((old, new))
}

/// Pair each change's removed lines with its added ones, row by row.
fn split_rows(lines: &[Line]) -> Vec<SplitRow> {
    let mut rows = Vec::new();
    let (mut removed, mut added): (Vec<usize>, Vec<usize>) = (Vec::new(), Vec::new());
    let flush = |rows: &mut Vec<SplitRow>, removed: &mut Vec<usize>, added: &mut Vec<usize>| {
        for index in 0..removed.len().max(added.len()) {
            rows.push(SplitRow::Sides(
                removed.get(index).copied(),
                added.get(index).copied(),
            ));
        }
        removed.clear();
        added.clear();
    };
    for (index, line) in lines.iter().enumerate() {
        match line.kind {
            Kind::Removed => {
                if !added.is_empty() {
                    flush(&mut rows, &mut removed, &mut added);
                }
                removed.push(index);
            }
            Kind::Added => added.push(index),
            Kind::Context => {
                flush(&mut rows, &mut removed, &mut added);
                rows.push(SplitRow::Sides(Some(index), Some(index)));
            }
            Kind::Hunk | Kind::Note => {
                flush(&mut rows, &mut removed, &mut added);
                rows.push(SplitRow::Across(index));
            }
        }
    }
    flush(&mut rows, &mut removed, &mut added);
    rows
}

/// The patch on the code ground: Unified, or Split (old and new side by
/// side); long lines wrap, or scroll sideways when `wrap` is off. `key`
/// names what is shown (the read, the file, the layout): rows are rebuilt
/// for a new one and kept otherwise.
pub(super) fn patch_view(parsed: &Arc<Parsed>, split: bool, wrap: bool, key: impl Hash) -> View {
    let mut hasher = DefaultHasher::new();
    (key, split, wrap).hash(&mut hasher);
    let len = if split {
        parsed.split.len()
    } else {
        parsed.lines.len()
    };
    let rows = Arc::clone(parsed);
    let repeater = ItemsRepeater::new()
        .automation_name(if split {
            "Patch, old and new side by side"
        } else {
            "Patch"
        })
        .virtual_source(VirtualSource::new(
            hasher.finish(),
            len,
            Key::from,
            move |index| -> View {
                if split {
                    split_row(&rows, index, wrap)
                } else {
                    unified_row(&rows, index, wrap)
                }
            },
        ));
    let body: View = if wrap {
        repeater.into()
    } else {
        ScrollViewer::new()
            .horizontal_scroll_bar_visibility(ScrollBarVisibility::Auto)
            .vertical_scroll_bar_visibility(ScrollBarVisibility::Disabled)
            .content(repeater)
            .into()
    };
    Border::new()
        .border_brush(ThemeBrush::CardStroke)
        .border_thickness(Thickness::uniform(tokens::layout::HAIRLINE))
        .corner_radius(tokens::radius::CONTAINER)
        .padding(Thickness::xy(tokens::spacing::SM, tokens::spacing::XS))
        .content(body)
        .into()
}

/// Monospace patch text in the body size, selectable.
fn code(value: String, wrap: bool) -> TextBlock {
    controls::body(value)
        .font_family(crate::views::CODE_FONT)
        .text_wrapping(if wrap {
            TextWrapping::Wrap
        } else {
            TextWrapping::NoWrap
        })
        .is_text_selection_enabled(true)
}

/// A line number right-aligned in `digits` columns, blank when absent.
fn number(value: Option<u32>, digits: usize) -> String {
    value.map_or_else(|| " ".repeat(digits), |value| format!("{value:>digits$}"))
}

/// The sign column: `+` or `−`, colored as a supplement only.
fn sign(kind: Kind) -> View {
    let (mark, brush) = match kind {
        Kind::Added => ("+", Some(ThemeBrush::AccentText)),
        Kind::Removed => ("\u{2212}", Some(ThemeBrush::SystemCritical)),
        _ => (" ", None),
    };
    let text = controls::body(mark).font_family(crate::views::CODE_FONT);
    match brush {
        Some(brush) => text.foreground(brush).into(),
        None => text.into(),
    }
}

/// A line's words for a screen reader: "Added line 13: text".
fn line_words(line: &Line) -> String {
    let word = match line.kind {
        Kind::Added => "Added line",
        Kind::Removed => "Removed line",
        _ => "Line",
    };
    match line.new.or(line.old) {
        Some(number) => format!("{word} {number}: {}", line.text),
        None => line.text.clone(),
    }
}

/// A hunk header or Git's note, across the row.
fn across(line: &Line, wrap: bool) -> View {
    match line.kind {
        Kind::Note => controls::secondary(line.text.clone())
            .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::XS))
            .into(),
        _ => code(line.text.clone(), wrap)
            .foreground(ThemeBrush::AccentText)
            .margin(Thickness::new(
                0.0,
                tokens::spacing::XS,
                0.0,
                tokens::spacing::XS,
            ))
            .into(),
    }
}

/// One side of a line: its number columns, its sign and its text.
fn cells(numbers: String, line: &Line, wrap: bool) -> Grid {
    Grid::new()
        .columns([GridLength::Auto, GridLength::Auto, GridLength::STAR])
        .column_spacing(tokens::spacing::SM)
        .children((
            controls::body(numbers)
                .font_family(crate::views::CODE_FONT)
                .vertical_alignment(VerticalAlignment::Top),
            Border::new()
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Top)
                .content(sign(line.kind)),
            code(line.text.clone(), wrap).grid_column(2),
        ))
}

fn unified_row(parsed: &Parsed, index: usize, wrap: bool) -> View {
    let Some(line) = parsed.lines.get(index) else {
        return Border::new().into();
    };
    if matches!(line.kind, Kind::Hunk | Kind::Note) {
        return across(line, wrap);
    }
    let numbers = format!(
        "{} {}",
        number(line.old, parsed.digits),
        number(line.new, parsed.digits)
    );
    cells(numbers, line, wrap)
        .automation_name(line_words(line))
        .into()
}

fn split_row(parsed: &Parsed, index: usize, wrap: bool) -> View {
    let side = |line: Option<usize>, old: bool| -> (View, Option<String>) {
        match line.and_then(|line| parsed.lines.get(line)) {
            Some(line) => {
                // A context line shows on both sides, each with its own number.
                let shown = if old {
                    Line {
                        new: None,
                        ..line.clone()
                    }
                } else {
                    Line {
                        old: None,
                        ..line.clone()
                    }
                };
                (
                    cells(number(shown.old.or(shown.new), parsed.digits), &shown, wrap).into(),
                    Some(line_words(&shown)),
                )
            }
            None => (Border::new().into(), None),
        }
    };
    match parsed.split.get(index) {
        Some(SplitRow::Across(line)) => parsed
            .lines
            .get(*line)
            .map_or_else(|| Border::new().into(), |line| across(line, wrap)),
        Some(SplitRow::Sides(left, right)) => {
            let (old, old_words) = side(*left, true);
            let (new, new_words) = side(*right, false);
            let words = old_words
                .into_iter()
                .chain(new_words)
                .collect::<Vec<_>>()
                .join("; ");
            Grid::new()
                .columns([
                    GridLength::STAR,
                    GridLength::Pixel(tokens::layout::HAIRLINE),
                    GridLength::STAR,
                ])
                .column_spacing(tokens::spacing::SM)
                .automation_name(words)
                .children((
                    old,
                    Border::new()
                        .grid_column(1)
                        .background(ThemeBrush::CardStroke),
                    Border::new().grid_column(2).content(new),
                ))
                .into()
        }
        None => Border::new().into(),
    }
}
