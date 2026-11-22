//! Swarm autonomy in the Monitor: `autonomy.get` (Owner) as one compact
//! row per Store source, with its numbers, its source and its coverage
//! (complete, partial or unavailable with the daemon's reason), for the
//! last 24 hours or 7 days. Numbers only: no thresholds and no judgement
//! (`docs/spec/monitoring.md`, Swarm autonomy). The rows are built once per
//! read ([`lines`]) and drawn by their own component ([`Autonomy`]), so a
//! window render redraws them only when a read or a choice changes them.

use eliot_core::api::{AUTONOMY_DAY_MS, AutonomyCoverage, AutonomyResponse, AutonomyRow};
use windows_reactor::*;

use crate::monitor::group;

/// A count with digit groups.
fn n(value: u64) -> String {
    group(i64::try_from(value).unwrap_or(i64::MAX))
}
use crate::theme::winui::Palette;
use crate::ui::tokens;
use crate::views::Shared;

/// The 7-day window.
pub const WEEK_MS: i64 = 7 * AUTONOMY_DAY_MS;
/// A shown report is read again this often while it stays open.
pub const REFRESH_MS: u64 = 60_000;

/// One source row as drawn.
#[derive(Clone, Debug, PartialEq)]
pub struct Line {
    pub title: &'static str,
    /// The numbers, one line each (none when unavailable).
    pub values: Vec<String>,
    pub source: String,
    pub coverage: AutonomyCoverage,
    pub reason: Option<String>,
}

fn line<T>(
    title: &'static str,
    row: &AutonomyRow<T>,
    values: impl FnOnce(&T) -> Vec<String>,
) -> Line {
    Line {
        title,
        values: row.counts.as_ref().map(values).unwrap_or_default(),
        source: row.source.clone(),
        coverage: row.coverage,
        reason: row.reason.clone(),
    }
}

fn words(code: &str) -> String {
    code.replace(['_', '.'], " ")
}

/// Every row of one report. `title` names an Agent by its title and
/// `project` a registration by its name (the sidebar's); `now_ms` ages the
/// oldest waiting items.
pub fn lines(
    read: &AutonomyResponse,
    title: impl Fn(&str) -> String,
    project_name: impl Fn(&str) -> String,
    now_ms: i64,
) -> Vec<Line> {
    vec![
        line("Project policy", &read.project_policy, |counts| {
            let mut values = vec![format!(
                "{} of {} launches with policy · {} opted out",
                n(counts.applied),
                n(counts.created),
                n(counts.opted_out)
            )];
            values.extend(counts.projects.iter().map(|project| {
                format!(
                    "{}: {} of {} · {} opted out",
                    project_name(&project.project),
                    n(project.applied),
                    n(project.created),
                    n(project.opted_out)
                )
            }));
            if counts.projects_truncated {
                values.push("More projects than listed".to_owned());
            }
            values.extend(counts.opt_outs.iter().map(|opt_out| {
                format!(
                    "Opted out: {} in {}: {}",
                    title(&opt_out.agent),
                    project_name(&opt_out.project),
                    opt_out.reason
                )
            }));
            if counts.opt_outs_truncated {
                values.push("More opt-outs than listed".to_owned());
            }
            values
        }),
        line("Steers", &read.steer, |counts| {
            vec![format!(
                "{} manual · {} by Swarm automation",
                n(counts.manual),
                n(counts.swarm)
            )]
        }),
        line("Checks", &read.checks, |counts| {
            vec![format!(
                "{} in Agent shells · {} run by Swarm",
                n(counts.manual),
                n(counts.swarm)
            )]
        }),
        line("Waits", &read.waits, |counts| {
            vec![format!("{} wait calls", n(counts.calls))]
        }),
        line("Swarm fact notes", &read.notes, |counts| {
            let mut values = vec![format!(
                "{} delivered · {} moved · {} cancelled",
                n(counts.delivered),
                n(counts.moved),
                n(counts.cancelled)
            )];
            values.extend(counts.by_kind.iter().map(|kind| {
                format!(
                    "{}: {} delivered · {} moved · {} cancelled · {} resolved",
                    words(&kind.kind),
                    n(kind.delivered),
                    n(kind.moved),
                    n(kind.cancelled),
                    n(kind.resolved)
                )
            }));
            values
        }),
        line("Owner backlog", &read.owner_waiting, |counts| {
            let mut values = vec![
                format!("{} waiting for you", n(counts.total)),
                format!(
                    "{} unknown commands · {} failed or lost Agents · {} provider holds",
                    n(counts.unknown_commands),
                    n(counts.failed_or_lost_agents),
                    n(counts.provider_holds)
                ),
            ];
            values.extend(
                counts
                    .by_kind
                    .iter()
                    .map(|(kind, count)| format!("{}: {}", words(kind.as_str()), n(*count))),
            );
            values.extend(counts.oldest.iter().map(|item| {
                let subject = if item.kind == "agent" {
                    title(&item.id)
                } else {
                    item.id.clone()
                };
                format!(
                    "Oldest: {} {subject} · {}",
                    words(&item.kind),
                    crate::nav::short_age(now_ms, item.since_ms)
                )
            }));
            if counts.oldest_truncated {
                values.push("More waiting items than listed".to_owned());
            }
            values
        }),
        line("Usage", &read.usage, |counts| {
            let mut values = vec![
                format!(
                    "Input tokens: {} managers · {} executors",
                    n(counts.manager_input_tokens),
                    n(counts.executor_input_tokens)
                ),
                format!("{} observed calls", n(counts.observed_calls)),
            ];
            if let Some(share) = counts.manager_input_share {
                values.push(format!("Manager share {:.1}%", share * 100.0));
            }
            if let Some(average) = counts.average_context_tokens {
                values.push(format!(
                    "Average first-call context {} tokens",
                    n(average.round() as u64)
                ));
            }
            values
        }),
        line("Executors calling Swarm", &read.executor_swarm, |counts| {
            let mut values: Vec<String> = counts
                .profiles
                .iter()
                .map(|profile| {
                    format!(
                        "{}: {} of {} called Swarm",
                        profile.profile,
                        n(profile.called_swarm),
                        n(profile.executors)
                    )
                })
                .collect();
            if values.is_empty() {
                values.push("No executors in the window".to_owned());
            }
            if counts.profiles_truncated {
                values.push("More profiles than listed".to_owned());
            }
            values
        }),
    ]
}

/// What the section shows.
#[derive(Clone, PartialEq)]
pub enum Report {
    /// Not read yet (or reading for the first time).
    Reading,
    Read(Shared<Vec<Line>>),
    Failed(String),
}

#[derive(Clone, PartialEq)]
pub struct AutonomyInput {
    pub open: bool,
    /// The 7-day window (else 24 hours).
    pub week: bool,
    pub report: Report,
    /// A read is running (the shown rows stay until it lands).
    pub reading: bool,
    pub palette: Palette,
    /// Show or hide the section.
    pub toggle: Callback<()>,
    /// Choose the window: 7 days (true) or 24 hours.
    pub window: Callback<bool>,
    /// Read again.
    pub refresh: Callback<()>,
}

/// The Monitor's Swarm autonomy section: a toggle row, then (open) the
/// window choice and one row per source.
pub struct Autonomy;

impl Component for Autonomy {
    type Input = AutonomyInput;
    type Message = ();

    fn create(_input: &AutonomyInput, _context: &ComponentContext<Self>) -> Self {
        Self
    }

    fn view(&self, input: &AutonomyInput, _context: &mut ViewContext<Self>) -> View {
        let palette = input.palette;
        let window = if input.week {
            "last 7 days"
        } else {
            "last 24 h"
        };
        let label = format!("Swarm autonomy · {window}");
        let toggle = input.toggle.clone();
        let header = crate::m3::sub_row(palette, false)
            .automation_name(format!(
                "{label} ({})",
                if input.open { "shown" } else { "hidden" }
            ))
            .on_click(move || {
                let _ = toggle.call(());
            })
            .content(crate::m3::row_content(
                Some(if input.open {
                    crate::m3::glyph::CHEVRON_DOWN
                } else {
                    crate::m3::glyph::CHEVRON_RIGHT
                }),
                &label,
                "",
                View::empty(),
            ));
        if !input.open {
            return header;
        }
        let segments: Vec<KeyedView> = [(false, "24 h"), (true, "7 days")]
            .into_iter()
            .map(|(week, text)| {
                let selected = input.week == week;
                let choose = input.window.clone();
                // Keyed by its selection: WinUI reads a button's colors
                // only when its template is applied.
                KeyedView::new(
                    format!("{text}:{selected}"),
                    crate::m3::segment(palette, selected)
                        .automation_name(format!("Swarm autonomy for the {text}"))
                        .on_click(move || {
                            let _ = choose.call(week);
                        })
                        .content(crate::m3::segment_label(text)),
                )
            })
            .collect();
        let refresh = input.refresh.clone();
        let controls = Grid::new()
            .columns([GridLength::STAR, GridLength::Auto])
            .column_spacing(tokens::spacing::SM)
            .children((
                crate::m3::segmented(palette, segments),
                crate::m3::text_button(palette)
                    .grid_column(1)
                    .is_enabled(!input.reading)
                    .automation_name("Read Swarm autonomy again")
                    .on_click(move || {
                        let _ = refresh.call(());
                    })
                    .content(crate::m3::label(if input.reading {
                        "Reading…"
                    } else {
                        "Read again"
                    })),
            ));
        let body: Vec<KeyedView> = match &input.report {
            Report::Reading => vec![KeyedView::new("reading", crate::m3::secondary("Reading…"))],
            Report::Failed(error) => vec![KeyedView::new(
                "failed",
                crate::m3::secondary(format!("Swarm autonomy could not be read: {error}"))
                    .foreground(palette.attention())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            )],
            Report::Read(lines) => lines
                .0
                .iter()
                .map(|line| KeyedView::new(line.title, line_view(line, palette)))
                .collect(),
        };
        StackPanel::new().spacing(tokens::spacing::ROW).children((
            header,
            controls,
            crate::m3::card(palette).content(
                StackPanel::new()
                    .spacing(tokens::spacing::MD)
                    .keyed_children(body),
            ),
        ))
    }
}

/// One source: its title and coverage, its numbers, then its source (and
/// the coverage reason when it is not complete).
fn line_view(line: &Line, palette: Palette) -> View {
    let mut said = vec![line.title.to_owned(), line.coverage.as_str().to_owned()];
    said.extend(line.values.iter().cloned());
    let values: Vec<KeyedView> = line
        .values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            KeyedView::new(
                index,
                crate::m3::body(value.clone())
                    .text_wrapping(TextWrapping::Wrap)
                    .is_text_selection_enabled(true),
            )
        })
        .collect();
    let reason: View = match &line.reason {
        Some(reason) => crate::m3::secondary(format!("{}: {reason}", line.coverage.as_str()))
            .text_wrapping(TextWrapping::Wrap)
            .is_text_selection_enabled(true)
            .into(),
        None => View::empty(),
    };
    StackPanel::new()
        .spacing(tokens::spacing::XS)
        .automation_name(said.join(", "))
        .children((
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .column_spacing(tokens::spacing::SM)
                .children((
                    crate::m3::text(line.title, tokens::text::LABEL)
                        .vertical_alignment(VerticalAlignment::Center)
                        .text_trimming(TextTrimming::CharacterEllipsis),
                    Border::new()
                        .grid_column(1)
                        .content(crate::m3::badge(palette, line.coverage.as_str())),
                )),
            StackPanel::new()
                .spacing(tokens::spacing::XS)
                .keyed_children(values),
            reason,
            crate::m3::secondary(format!("Source: {}", line.source))
                .text_wrapping(TextWrapping::Wrap),
        ))
}
