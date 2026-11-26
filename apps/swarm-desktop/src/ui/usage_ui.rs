//! The sidebar's Usage row and its popover (`Sidebar.dc.html` footer,
//! `Hold.dc.html`; skeleton 5.12), shaped as Codex shows usage (an ambient
//! "{share} left" line, a Usage menu, Manage usage) with Antigravity's
//! per-harness windows:
//!
//! * the row shows the static Usage mark (§4.2) and one short line for the
//!   harness whose limit is active where the owner is ([`usage_harness`]):
//!   "Claude Code 62% left", "Codex on hold" or "Codex quota unknown";
//! * the popover, a real WinUI `Flyout` beside the sidebar
//!   ([`DesktopApp::anchored_popover`], `Pop::Quotas`), lists each harness
//!   once with every window it reports (its share left, when it resets, a
//!   meter that turns `error` under 20 %), "Quota unknown" where it reports
//!   none (never 0 %), its hold in a few words and Manage usage;
//! * Settings › Usage holds the detail and the controls.
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
    pub(super) holds_loading: bool,
    /// When the last quota read of every harness settled (ms since the
    /// epoch).
    pub(super) read_at: Option<i64>,
}

/// Where the owner is, for [`usage_harness`].
pub(super) enum UsagePlace<'a> {
    /// A chat is open: its Agent's harness.
    Agent(&'a str),
    /// A project page, by its sidebar key.
    Project(&'a str),
    /// Home or any other page.
    Fleet,
}

/// The harness whose limit the Usage row shows: the limit active where the
/// owner is, not the tightest one (owner, 2026-11-26). An open chat shows
/// its Agent's harness; a project page the harness with the most running
/// Agents there, else (a tie, or none running) its most recently active
/// Agent's; any other page the harness with the most running Agents in
/// the fleet, else `next`, the harness the composer launches next.
pub(super) fn usage_harness<'a>(
    place: UsagePlace<'a>,
    agents: &'a [nav::UiAgent],
    next: Option<&'a str>,
) -> Option<&'a str> {
    match place {
        UsagePlace::Agent(harness) => Some(harness),
        UsagePlace::Project(key) => {
            let mine = agents
                .iter()
                .filter(|agent| nav::group_key(agent.organization.as_deref()) == key);
            busiest(mine.clone()).or_else(|| {
                mine.max_by_key(|agent| agent.updated_at)
                    .map(|agent| agent.harness.as_str())
            })
        }
        UsagePlace::Fleet => busiest(agents.iter()).or(next),
    }
}

/// The harness with the most running Agents; `None` when none runs or two
/// harnesses tie.
fn busiest<'a>(agents: impl Iterator<Item = &'a nav::UiAgent>) -> Option<&'a str> {
    let mut counts: std::collections::BTreeMap<&str, usize> = Default::default();
    for agent in agents.filter(|agent| agent.running()) {
        *counts.entry(agent.harness.as_str()).or_default() += 1;
    }
    let top = counts.values().copied().max()?;
    let mut leaders = counts.into_iter().filter(|(_, count)| *count == top);
    let (harness, _) = leaders.next()?;
    leaders.next().is_none().then_some(harness)
}

impl DesktopApp {
    /// Read the limits of every harness and the provider holds once (the
    /// popover or Settings › Usage opened, or their Refresh).
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

    /// The holds read, held or probing (a probe still holds new work).
    pub(super) fn holds(&self) -> &[eliot_core::ProviderHold] {
        match &self.usage.holds {
            Some(Ok(reply)) => &reply.holds,
            _ => &[],
        }
    }

    /// A harness's hold, when it has one.
    pub(super) fn harness_hold(&self, harness: &str) -> Option<&eliot_core::ProviderHold> {
        self.holds().iter().find(|hold| {
            hold.source
                .as_ref()
                .is_some_and(|source| source.harness == harness)
        })
    }

    /// The harness the Usage row speaks for, where the owner is now.
    fn row_harness(&self) -> Option<String> {
        let open = self.selected_agent_id().and_then(|id| {
            self.nav
                .agents
                .iter()
                .find(|agent| agent.id == id)
                .map(|agent| agent.harness.clone())
                .or_else(|| {
                    self.chat
                        .detail
                        .as_ref()
                        .filter(|detail| detail.agent == id)
                        .map(|detail| detail.harness.clone())
                })
        });
        let place = match (self.main, &open) {
            (Main::Conversation, Some(harness)) => UsagePlace::Agent(harness),
            // An open chat speaks only for its own Agent: until its harness
            // is known, no number (never the fleet's).
            (Main::Conversation, None) if self.selected_agent_id().is_some() => return None,
            (Main::Project, _) => UsagePlace::Project(&self.project.key),
            _ => UsagePlace::Fleet,
        };
        let next = self.starting_harness();
        usage_harness(place, &self.nav.agents, next.as_deref()).map(str::to_owned)
    }

    /// The footer's Usage row (`Sidebar.dc.html`): the static Usage mark
    /// (§4.2: no row draws a progress ring; critical under 20 % left or on
    /// hold), "Usage" and one line for [`Self::row_harness`]: "Claude Code
    /// 62% left" from its tightest window, "Claude Code on hold", or
    /// "Claude Code quota unknown" when its read reports no window (never
    /// another harness's number); before the first reading, no line. It
    /// carries the Usage popover while that is open.
    pub(super) fn usage_row(&self, context: &mut ViewContext<Self>) -> View {
        let harness = self.row_harness();
        let window = harness
            .as_deref()
            .and_then(|harness| self.settings.quotas.get(harness))
            .and_then(|read| read.as_ref().ok())
            .and_then(crate::quota::tightest);
        let held = harness
            .as_deref()
            .is_some_and(|harness| self.harness_hold(harness).is_some());
        let word = match harness.as_deref() {
            None => String::new(),
            Some(harness) => {
                let label = crate::picker::harness_label(harness);
                match &window {
                    _ if held => format!("{label} on hold"),
                    Some(window) => format!("{label} {}% left", window.remaining),
                    None if self.usage.read_at.is_some() => format!("{label} quota unknown"),
                    None => String::new(),
                }
            }
        };
        let low = held
            || window
                .as_ref()
                .is_some_and(|window| (window.remaining as f64) < crate::controls::QUOTA_LOW);
        let open = self.launch.open == Some(Pop::Quotas);
        // A static mark; the line beside it carries the share left.
        let mark: View = if low {
            controls::critical_icon(controls::glyph::USAGE, tokens::icon::ROW)
        } else {
            controls::icon(controls::glyph::USAGE)
        };
        // No line once read: no harness speaks here (nothing runs, and
        // Auto has no previewed pick).
        let spoken = match self.usage.read_at {
            _ if !word.is_empty() => format!("Usage: {word}"),
            None => "Usage: limits not read yet".to_owned(),
            Some(_) => "Usage".to_owned(),
        };
        let row = controls::row(open)
            .horizontal_alignment(HorizontalAlignment::Stretch)
            .horizontal_content_alignment(HorizontalAlignment::Stretch)
            .automation_name(format!(
                "{spoken} ({})",
                if open { "open" } else { "show limits" }
            ))
            .on_click(context.callback(|()| Message::Launch(LaunchMsg::Toggle(Pop::Quotas))));
        let content = controls::sidebar_row(
            View::from(Border::new().width(tokens::icon::ROW).content(mark)),
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
            format!("usage:{low}")
        };
        Grid::new()
            .keyed_children(vec![KeyedView::new(key, row)])
            .into()
    }

    /// The Usage popover (`Hold.dc.html`): "Usage" with when the limits
    /// were read and Refresh; each harness once, its name and status
    /// ("Quota unknown", "Not installed", "Reading…"), every window it
    /// reports ("Weekly limit", "62% left · resets Fri 09:00", a meter) and
    /// its hold ("On hold until 19:27"); holds of no harness; then Manage
    /// usage.
    pub(super) fn quotas_popover(&self, context: &mut ViewContext<Self>) -> View {
        let now = crate::handoff::now_ms();
        let reading = self.settings.quotas_reading();
        let catalog = self
            .launch
            .catalog
            .as_ref()
            .and_then(|reply| reply.as_ref().ok());
        let mut blocks: Vec<KeyedView> = Vec::new();
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
            let windows = read
                .and_then(|read| read.as_ref().ok())
                .map(crate::quota::windows)
                .unwrap_or_default();
            let status = match read {
                _ if !installed => "Not installed",
                _ if !windows.is_empty() => "",
                None if reading => "Reading…",
                _ => "Quota unknown",
            };
            let held = self.harness_hold(harness).map(|hold| hold_line(hold, now));
            let mut parts: Vec<KeyedView> = vec![KeyedView::new(
                format!("name:{status}"),
                line(
                    controls::text(label, tokens::text::SMALL_TITLE),
                    controls::secondary(status),
                ),
            )];
            let mut spoken = vec![if status.is_empty() {
                label.to_owned()
            } else {
                format!("{label}: {status}")
            }];
            for (index, window) in windows.iter().enumerate() {
                let name = window.name();
                let left = match window.resets_at_ms {
                    Some(at) => format!("{}% left · resets {}", window.remaining, clock(at, now)),
                    None => format!("{}% left", window.remaining),
                };
                let remaining = window.remaining as f64;
                parts.push(KeyedView::new(
                    format!("window:{index}:{name}:{left}"),
                    StackPanel::new()
                        .spacing(tokens::spacing::XS)
                        .automation_name(format!("{name}: {left}"))
                        .children((
                            line(
                                controls::body(name.clone()),
                                controls::secondary(left.clone()),
                            ),
                            controls::meter(remaining, remaining < crate::controls::QUOTA_LOW),
                        )),
                ));
                spoken.push(format!("{name} {left}"));
            }
            if let Some(held) = &held {
                parts.push(KeyedView::new(
                    format!("hold:{held}"),
                    controls::secondary(held.clone()).foreground(ThemeBrush::SystemCritical),
                ));
                spoken.push(held.clone());
            }
            blocks.push(KeyedView::new(
                format!("harness:{harness}"),
                StackPanel::new()
                    .spacing(tokens::spacing::XS)
                    .automation_name(spoken.join("; "))
                    .keyed_children(parts),
            ));
        }
        // Holds no harness row names (an exact-profile or model hold).
        for hold in self.holds().iter().filter(|hold| {
            hold.source
                .as_ref()
                .is_none_or(|source| !crate::picker::HARNESSES.contains(&source.harness.as_str()))
        }) {
            let text = format!("{}: {}", hold.scope, hold_line(hold, now).to_lowercase());
            blocks.push(KeyedView::new(
                format!("hold:{}", hold.scope),
                controls::secondary(text)
                    .foreground(ThemeBrush::SystemCritical)
                    .text_wrapping(TextWrapping::Wrap),
            ));
        }
        if let Some(Err(error)) = &self.usage.holds {
            blocks.push(KeyedView::new(
                format!("holds-error:{error}"),
                controls::secondary(format!(
                    "Holds not read: {}",
                    crate::state::plain_reason(error)
                ))
                .foreground(ThemeBrush::SystemCritical)
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        let header = Grid::new()
            .columns([GridLength::STAR, GridLength::Auto, GridLength::Auto])
            .column_spacing(tokens::spacing::SM)
            .children((
                controls::text("Usage", tokens::text::SMALL_TITLE)
                    .automation_heading_level(AutomationHeadingLevel::Level2)
                    .vertical_alignment(VerticalAlignment::Center),
                controls::secondary(self.usage_updated(now))
                    .grid_column(1)
                    .vertical_alignment(VerticalAlignment::Center),
                controls::icon_button()
                    .grid_column(2)
                    .is_enabled(!reading && !self.usage.holds_loading)
                    .automation_name("Refresh the limits")
                    .on_click(context.message(Message::Shell(ShellMsg::UsageRefresh)))
                    .content(controls::icon(controls::glyph::REFRESH)),
            ));
        self.usage_menu(
            tokens::layout::USAGE_POPOVER_WIDTH,
            StackPanel::new()
                .spacing(tokens::spacing::MD)
                .children((
                    header,
                    StackPanel::new()
                        .spacing(tokens::spacing::MD)
                        .keyed_children(blocks),
                ))
                .into(),
            context,
        )
    }

    /// "Updated 14:05" once every harness was read, "Reading…" meanwhile.
    pub(super) fn usage_updated(&self, now: i64) -> String {
        match self.usage.read_at {
            _ if self.settings.quotas_reading() => "Reading…".to_owned(),
            Some(at) => format!("Updated {}", clock(at, now)),
            None => String::new(),
        }
    }

    /// A usage flyout (the sidebar's and the composer's): its content, then
    /// Codex's Manage usage, which opens Settings › Usage.
    pub(super) fn usage_menu(
        &self,
        width: f64,
        content: View,
        context: &mut ViewContext<Self>,
    ) -> View {
        StackPanel::new()
            .width(width)
            .spacing(tokens::spacing::XS)
            .automation_name("Usage")
            .children((
                Border::new()
                    .margin(Thickness::new(
                        tokens::spacing::SM,
                        tokens::spacing::SM,
                        tokens::spacing::SM,
                        0.0,
                    ))
                    .content(content),
                controls::divider(),
                controls::row(false)
                    .automation_name("Manage usage")
                    .on_click(context.message(Message::Shell(ShellMsg::SettingsPage(
                        settings_ui::Page::Usage,
                    ))))
                    .content(controls::row_content(
                        None,
                        "Manage usage",
                        "",
                        controls::icon(controls::glyph::CHEVRON_RIGHT),
                    )),
            ))
            .into()
    }
}

/// A popover line: its name on the left (trimmed where narrow), its value
/// at the end.
fn line(name: TextBlock, value: TextBlock) -> View {
    Grid::new()
        .columns([GridLength::STAR, GridLength::Auto])
        .column_spacing(tokens::spacing::SM)
        .children((
            name.vertical_alignment(VerticalAlignment::Center)
                .text_trimming(TextTrimming::CharacterEllipsis),
            value
                .grid_column(1)
                .vertical_alignment(VerticalAlignment::Center),
        ))
        .into()
}

/// A hold in a few words: "On hold until 19:27", or "On hold".
pub(super) fn hold_line(hold: &eliot_core::ProviderHold, now: i64) -> String {
    match hold.retry_after.or(hold.next_probe_at) {
        Some(at) => format!("On hold until {}", clock(at, now)),
        None => "On hold".to_owned(),
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
