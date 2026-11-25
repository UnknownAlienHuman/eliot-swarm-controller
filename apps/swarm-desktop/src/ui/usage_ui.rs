//! The sidebar's Usage row and its popover (`Sidebar.dc.html` footer,
//! `Hold.dc.html`; skeleton 5.12): the row shows a ring of the tightest
//! harness window it knows and a short word ("Codex 62% · 1 hold"); it
//! opens a real WinUI `Flyout` beside the sidebar
//! ([`DesktopApp::anchored_popover`], `Pop::Quotas`) that lists each
//! harness's tightest window (its share left, when it resets, a meter that
//! turns `error` under 20 %), "Quota unknown" where none is reported (never
//! 0 %), the provider holds (`provider.holds`) and Open usage settings.
//!
//! Quota reads start the vendors' own processes, so they run only while
//! their screen is open (skeleton 2.4): once when the popover (or Settings
//! › Usage) opens, and on Refresh. The row keeps the last reading and says
//! nothing before the first one.

use super::launch_ui::Pop;
use super::*;
use eliot_core::api::ProviderHoldsResponse;

/// The Usage popover's reads: the provider holds and when the limits were
/// last read in full.
#[derive(Default)]
pub(super) struct UsageState {
    pub(super) holds: Option<Fallible<ProviderHoldsResponse>>,
    holds_loading: bool,
    /// When the last quota read of every harness settled (ms since the
    /// epoch).
    pub(super) read_at: Option<i64>,
}

/// One harness's tightest window as the row and the popover say it.
struct Tightest {
    harness: &'static str,
    window: crate::quota::Window,
}

impl DesktopApp {
    /// Read the limits of every harness and the provider holds once (the
    /// popover opened, or its Refresh).
    pub(super) fn read_usage(&mut self, context: &ComponentContext<Self>) {
        self.load_quotas(context);
        self.load_holds(context);
    }

    fn load_holds(&mut self, context: &ComponentContext<Self>) {
        let Ok(data_dir) = self.data_dir.clone() else {
            return;
        };
        if self.usage.holds_loading {
            return;
        }
        self.usage.holds_loading = true;
        let _task = context.spawn_background(move |_| {
            let reply = client::block_on(config_client::provider_holds(&data_dir))
                .map_err(|error| error.user_message());
            Message::Shell(ShellMsg::HoldsLoaded(Box::new(reply)))
        });
    }

    pub(super) fn holds_loaded(&mut self, reply: Fallible<ProviderHoldsResponse>) {
        self.usage.holds_loading = false;
        self.usage.holds = Some(reply);
    }

    /// The tightest window of every harness whose limits were read.
    fn tightest_windows(&self) -> Vec<Tightest> {
        crate::picker::HARNESSES
            .into_iter()
            .filter_map(|harness| {
                let reply = self.settings.quotas.get(harness)?.as_ref().ok()?;
                Some(Tightest {
                    harness,
                    window: crate::quota::tightest(reply)?,
                })
            })
            .collect()
    }

    /// The holds read, held or probing (a probe still holds new work).
    fn holds(&self) -> &[eliot_core::ProviderHold] {
        match &self.usage.holds {
            Some(Ok(reply)) => &reply.holds,
            _ => &[],
        }
    }

    /// The footer's Usage row (`Sidebar.dc.html`): the ring (the share
    /// used of the tightest window read; `error` under 20 % left), "Usage"
    /// and its word ("Codex 62% · 1 hold"); before any reading, a neutral
    /// ring and no word. It carries the Usage popover while that is open.
    pub(super) fn usage_row(&self, context: &mut ViewContext<Self>) -> View {
        let tightest = self
            .tightest_windows()
            .into_iter()
            .min_by_key(|tightest| tightest.window.remaining);
        let holds = self.holds().len();
        let mut word = tightest.as_ref().map_or_else(String::new, |tightest| {
            format!(
                "{} {}%",
                crate::picker::harness_label(tightest.harness),
                tightest.window.remaining
            )
        });
        if holds > 0 {
            if !word.is_empty() {
                word.push_str(" · ");
            }
            word.push_str(&eliot_core::count_words(holds as u64, "hold", "holds"));
        }
        let low = tightest.as_ref().is_some_and(|tightest| {
            (tightest.window.remaining as f64) < crate::controls::QUOTA_LOW
        });
        let open = self.launch.open == Some(Pop::Quotas);
        let ring: View = match &tightest {
            // WinUI's own determinate ring: its accent arc and track.
            Some(tightest) => ProgressRing::new()
                .is_indeterminate(false)
                .minimum(0.0)
                .maximum(100.0)
                .value(100.0 - tightest.window.remaining as f64)
                .width(tokens::layout::ROW_RING)
                .height(tokens::layout::ROW_RING)
                .min_width(tokens::layout::ROW_RING)
                .min_height(tokens::layout::ROW_RING)
                .vertical_alignment(VerticalAlignment::Center)
                .into(),
            // No limit read yet: the Usage icon in the ring's place.
            None => controls::icon(controls::glyph::USAGE),
        };
        let spoken = if word.is_empty() {
            "Usage: limits not read yet".to_owned()
        } else {
            format!("Usage: {word}")
        };
        let row = controls::row(open)
            .height(tokens::height::CONTROL)
            .min_height(tokens::height::CONTROL)
            .horizontal_alignment(HorizontalAlignment::Stretch)
            .horizontal_content_alignment(HorizontalAlignment::Stretch)
            .automation_name(format!(
                "{spoken} ({})",
                if open { "open" } else { "show limits" }
            ))
            .on_click(context.callback(|()| Message::Launch(LaunchMsg::Toggle(Pop::Quotas))));
        let content = controls::sidebar_row(
            View::from(Border::new().width(tokens::icon::ROW).content(ring)),
            "Usage",
            open,
            0.0,
            false,
            None,
            if word.is_empty() {
                None
            } else {
                Some(controls::trailing(word).into())
            },
        );
        let row = self.anchored_popover(
            row,
            content,
            open.then_some(Pop::Quotas),
            FlyoutPlacement::RightEdgeAlignedBottom,
            context,
        );
        // Keyed by its colors (a button's resources apply when its
        // template does) and by whether its flyout is attached; while it
        // is open the key holds, so a reading that lands meanwhile never
        // re-creates the button under its open flyout (which would close
        // it).
        let key = if open {
            "usage:open".to_owned()
        } else {
            format!("usage:{low}:{}", tightest.is_some())
        };
        Grid::new()
            .keyed_children(vec![KeyedView::new(key, row)])
            .into()
    }

    /// The Usage popover (`Hold.dc.html`): "Usage" with when the limits
    /// were read and Refresh; per harness its tightest window ("Codex ·
    /// weekly", "62% left · resets Fri 09:00", a meter), "Quota unknown"
    /// where none is reported, and its hold ("On hold until 19:27"); holds
    /// of no harness listed; then Open usage settings.
    pub(super) fn quotas_popover(&self, context: &mut ViewContext<Self>) -> View {
        let now = crate::handoff::now_ms();
        let reading = self.settings.quotas_reading();
        let catalog = self
            .launch
            .catalog
            .as_ref()
            .and_then(|reply| reply.as_ref().ok());
        let holds = self.holds();
        let hold_line =
            |hold: &eliot_core::ProviderHold| match hold.retry_after.or(hold.next_probe_at) {
                Some(at) => format!("On hold until {}", clock(at, now)),
                None => "On hold".to_owned(),
            };
        let mut rows: Vec<KeyedView> = Vec::new();
        for harness in crate::picker::HARNESSES {
            let label = crate::picker::harness_label(harness);
            let installed = catalog
                .and_then(|reply| {
                    reply
                        .harnesses
                        .iter()
                        .find(|entry| entry.harness == harness)
                })
                .is_none_or(|entry| entry.installed);
            let read = self.settings.quotas.get(harness);
            let window = read
                .and_then(|read| read.as_ref().ok())
                .and_then(crate::quota::tightest);
            let (title, right) = match (&window, read) {
                _ if !installed => (label.to_owned(), "Not installed".to_owned()),
                (Some(window), _) => (
                    format!("{label} · {}", window.label.to_lowercase()),
                    format!(
                        "{}% left · {}",
                        window.remaining,
                        match window.resets_at_ms {
                            Some(at) => format!("resets {}", clock(at, now)),
                            None => "reset time not reported".to_owned(),
                        }
                    ),
                ),
                (None, None) if reading => (label.to_owned(), "Reading…".to_owned()),
                (None, _) => (label.to_owned(), "Quota unknown".to_owned()),
            };
            let held = holds
                .iter()
                .filter(|hold| {
                    hold.source
                        .as_ref()
                        .is_some_and(|source| source.harness == harness)
                })
                .map(hold_line)
                .next();
            let mut parts: Vec<KeyedView> = vec![KeyedView::new(
                "line",
                Grid::new()
                    .columns([GridLength::STAR, GridLength::Auto])
                    .column_spacing(tokens::spacing::SM)
                    .children((
                        controls::body(title.clone())
                            .vertical_alignment(VerticalAlignment::Center)
                            .text_trimming(TextTrimming::CharacterEllipsis),
                        controls::text(right.clone(), tokens::text::LABEL)
                            .grid_column(1)
                            .vertical_alignment(VerticalAlignment::Center),
                    )),
            )];
            if let Some(window) = &window {
                let remaining = window.remaining as f64;
                parts.push(KeyedView::new(
                    format!("meter:{}", window.remaining),
                    controls::meter(remaining, remaining < crate::controls::QUOTA_LOW),
                ));
            }
            if let Some(held) = &held {
                parts.push(KeyedView::new(
                    format!("hold:{held}"),
                    controls::secondary(held.clone()).foreground(ThemeBrush::SystemCritical),
                ));
            }
            rows.push(KeyedView::new(
                format!("harness:{harness}:{title}:{right}:{}", held.is_some()),
                StackPanel::new()
                    .spacing(tokens::spacing::XS)
                    .automation_name(match &held {
                        Some(held) => format!("{title}: {right}. {held}"),
                        None => format!("{title}: {right}"),
                    })
                    .keyed_children(parts),
            ));
        }
        // Holds no harness row names (an exact-profile or model hold).
        for hold in holds.iter().filter(|hold| {
            hold.source
                .as_ref()
                .is_none_or(|source| !crate::picker::HARNESSES.contains(&source.harness.as_str()))
        }) {
            let line = format!("{}: {}", hold.scope, hold_line(hold).to_lowercase());
            rows.push(KeyedView::new(
                format!("hold:{}", hold.scope),
                controls::secondary(line)
                    .foreground(ThemeBrush::SystemCritical)
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
        if let Some(Err(error)) = &self.usage.holds {
            rows.push(KeyedView::new(
                format!("holds-error:{error}"),
                controls::secondary(format!("Holds not read: {error}"))
                    .foreground(ThemeBrush::SystemCritical)
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
        let updated = match self.usage.read_at {
            Some(at) if !reading => format!("Updated {}", clock(at, now)),
            _ if reading => "Reading…".to_owned(),
            _ => String::new(),
        };
        let header = Grid::new()
            .columns([GridLength::STAR, GridLength::Auto, GridLength::Auto])
            .column_spacing(tokens::spacing::SM)
            .children((
                controls::text("Usage", tokens::text::SMALL_TITLE)
                    .automation_heading_level(AutomationHeadingLevel::Level2)
                    .vertical_alignment(VerticalAlignment::Center),
                controls::text(updated, tokens::text::LABEL)
                    .grid_column(1)
                    .vertical_alignment(VerticalAlignment::Center),
                controls::icon_button()
                    .grid_column(2)
                    .is_enabled(!reading && !self.usage.holds_loading)
                    .automation_name("Refresh the limits")
                    .on_click(context.message(Message::Shell(ShellMsg::UsageRefresh)))
                    .content(controls::icon(controls::glyph::REFRESH)),
            ));
        let footer = Border::new()
            .border_brush(ThemeBrush::CardStroke)
            .border_thickness(Thickness::new(0.0, tokens::layout::HAIRLINE, 0.0, 0.0))
            .padding(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
            .content(
                controls::text_button()
                    .horizontal_alignment(HorizontalAlignment::Right)
                    .automation_name("Open usage settings")
                    .on_click(context.message(Message::Shell(ShellMsg::SettingsPage(
                        settings_ui::Page::Usage,
                    ))))
                    .content(controls::label("Open usage settings")),
            );
        Border::new()
            .width(tokens::layout::USAGE_POPOVER_WIDTH)
            .padding(Thickness::uniform(tokens::spacing::INSET))
            .automation_name("Usage")
            .content(
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    header,
                    StackPanel::new()
                        .spacing(tokens::spacing::SM)
                        .keyed_children(rows),
                    footer,
                )),
            )
            .into()
    }
}

/// A time on the local clock: "14:20" today, "Fri 09:00" within the
/// week, "24 Nov 09:00" beyond.
pub(in crate::ui) fn clock(at_ms: i64, now_ms: i64) -> String {
    use chrono::{Local, TimeZone};
    let (Some(at), Some(now)) = (
        Local.timestamp_millis_opt(at_ms).single(),
        Local.timestamp_millis_opt(now_ms).single(),
    ) else {
        return String::new();
    };
    let days = (at.date_naive() - now.date_naive()).num_days();
    if days == 0 {
        at.format("%H:%M").to_string()
    } else if (-6..=6).contains(&days) {
        at.format("%a %H:%M").to_string()
    } else {
        at.format("%-d %b %H:%M").to_string()
    }
}
