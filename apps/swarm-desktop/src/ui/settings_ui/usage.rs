use super::*;

impl DesktopApp {
    /// The tokens of the loaded Agents active today and how many of them
    /// report a total (Swarm has no per-day read: each reports its whole
    /// total).
    pub(in crate::ui) fn tokens_today(&self) -> (i64, usize) {
        let today = today_start_ms();
        self.nav
            .agents
            .iter()
            .filter(|agent| agent.updated_at >= today)
            .fold((0i64, 0usize), |(sum, count), agent| match agent.tokens {
                Some(tokens) => (sum + tokens, count + 1),
                None => (sum, count),
            })
    }

    /// Settings › Usage, the detail behind the sidebar's Usage popover
    /// (Antigravity's Models & Usage, Codex's usage settings): Limits with
    /// when they were read and Refresh; one card per harness (its provider,
    /// version and plan; each window with its exact reset and the time to
    /// it; why a quota is unknown; its hold and what clears it); holds of
    /// no harness; then what Swarm runs now. Storage and retention live in
    /// Advanced.
    pub(super) fn usage_page(&self, context: &mut ViewContext<Self>) -> View {
        let now = crate::handoff::now_ms();
        let catalog = self
            .launch
            .catalog
            .as_ref()
            .and_then(|reply| reply.as_ref().ok());
        let refresh = controls::text_button()
            .is_enabled(
                self.settings.quotas_loading == 0
                    && !self.settings.fleet_loading
                    && !self.usage.holds_loading,
            )
            .automation_name("Refresh the limits")
            .on_click(context.message(Message::Settings(SettingsMsg::UsageRefresh)))
            .content(controls::label("Refresh"));
        let mut limits = vec![KeyedView::new(
            "heading",
            controls::section_heading("Limits", &self.usage_updated(now), Some(refresh.into())),
        )];
        for harness in crate::picker::HARNESSES {
            let entry = catalog.and_then(|reply| {
                reply
                    .harnesses
                    .iter()
                    .find(|entry| entry.harness == harness)
            });
            let label = crate::picker::harness_label(harness);
            let provider =
                crate::picker::provider_label(&eliot_core::api::catalog_provider(harness, ""));
            let title = if provider.eq_ignore_ascii_case(label) || provider.is_empty() {
                label.to_owned()
            } else {
                format!("{label} · {provider}")
            };
            let read = self.settings.quotas.get(harness);
            let quota = read.and_then(|result| result.as_ref().ok());
            let observed = quota.and_then(crate::quota::observed_note);
            let tier = quota
                .filter(|_| harness == "muse")
                .and_then(crate::quota::plan);
            let metadata = [
                entry.and_then(|entry| entry.version.as_deref()),
                tier.as_deref(),
                observed.as_deref(),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" · ");
            let mut rows = match (entry, read) {
                (Some(entry), _) if !entry.installed => vec![KeyedView::new(
                    "not-installed",
                    controls::setting_row("Not installed", "", None),
                )],
                (_, None) if self.settings.quotas_loading > 0 => vec![KeyedView::new(
                    "reading",
                    controls::setting_row("Reading limits…", "", None),
                )],
                (_, None) => unknown_limits(""),
                (_, Some(Err(error))) => unknown_limits(&crate::state::plain_reason(error)),
                (_, Some(Ok(reply))) => {
                    let windows = crate::quota::windows(reply);
                    if windows.is_empty() {
                        let status = crate::quota::status_line(label, reply);
                        let note = status
                            .strip_prefix(label)
                            .and_then(|status| status.strip_prefix(": "))
                            .map_or_else(|| status.clone(), crate::quota::sentence);
                        unknown_limits(&note)
                    } else {
                        windows
                            .iter()
                            .enumerate()
                            .map(|(index, window)| {
                                let name = window.name();
                                let note = resets_detail(window, now);
                                KeyedView::new(
                                    format!("{index}:{name}:{note}:{}", window.remaining),
                                    controls::setting_row(
                                        &name,
                                        &note,
                                        limit_value(window.remaining),
                                    ),
                                )
                            })
                            .collect()
                    }
                }
            };
            if let Some(hold) = self.harness_hold(harness) {
                rows.push(hold_row(hold, &usage_ui::hold_line(hold, now), now));
            }
            limits.push(KeyedView::new(
                format!("harness:{harness}:{metadata}"),
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    Grid::new()
                        .columns([GridLength::Auto, GridLength::STAR])
                        .column_spacing(tokens::spacing::SM)
                        .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                        .children((
                            controls::text(title, tokens::text::SMALL_TITLE)
                                .automation_heading_level(AutomationHeadingLevel::Level3)
                                .vertical_alignment(VerticalAlignment::Center),
                            controls::secondary(metadata)
                                .grid_column(1)
                                .vertical_alignment(VerticalAlignment::Center)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        )),
                    controls::settings_card(rows),
                )),
            ));
        }
        // Holds no harness card names (an exact-profile or model hold).
        let others: Vec<KeyedView> = self
            .holds()
            .iter()
            .filter(|hold| {
                hold.source.as_ref().is_none_or(|source| {
                    !crate::picker::HARNESSES.contains(&source.harness.as_str())
                })
            })
            .map(|hold| hold_row(hold, &hold.scope, now))
            .collect();
        if !others.is_empty() {
            limits.push(KeyedView::new(
                "other-holds-title",
                controls::card_title("Other holds"),
            ));
            limits.push(KeyedView::new(
                "other-holds",
                controls::settings_card(others),
            ));
        }
        if let Some(Err(error)) = &self.usage.holds {
            limits.push(KeyedView::new(
                format!("holds-error:{error}"),
                controls::secondary(format!(
                    "Holds not read: {}",
                    crate::state::plain_reason(error)
                ))
                .foreground(ThemeBrush::SystemCritical)
                .text_wrapping(TextWrapping::Wrap),
            ));
        }
        let loaded = self.nav.agents.len();
        let (tokens_today, reporting) = self.tokens_today();
        let fleet = self.settings.fleet.as_ref();
        let total = fleet.and_then(|fleet| fleet.as_ref().ok().map(|fleet| fleet.agents));
        let (running_value, running_note) = match fleet {
            None => ("…".to_owned(), "Counting…".to_owned()),
            Some(Ok(fleet)) => (
                format!("{} of {}", fleet.running, fleet.agents),
                "Working or starting".to_owned(),
            ),
            Some(Err(error)) => (
                "—".to_owned(),
                format!("Not counted: {}", crate::state::plain_error(error)),
            ),
        };
        // Each Agent reports its whole total; Swarm has no per-day read.
        let coverage = match total {
            Some(total) if total > loaded => format!(
                "Totals of {} active today, among the {loaded} most recent of {total}",
                agents(reporting)
            ),
            _ => format!("Totals of {} active today", agents(reporting)),
        };
        let totals = controls::settings_card(vec![
            KeyedView::new(
                format!("running:{running_value}:{running_note}"),
                controls::setting_row(
                    "Running Agents",
                    &running_note,
                    View::from(controls::body(running_value)),
                ),
            ),
            KeyedView::new(
                format!("tokens:{tokens_today}:{coverage}"),
                controls::setting_row(
                    "Tokens today",
                    &coverage,
                    View::from(controls::body(group_digits(tokens_today))),
                ),
            ),
        ]);
        StackPanel::new()
            .spacing(tokens::spacing::MD)
            .children((
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .keyed_children(limits),
                StackPanel::new()
                    .spacing(tokens::spacing::SM)
                    .children((controls::section_heading("Swarm", "", None), totals)),
            ))
            .into()
    }

    /// Advanced's storage row: the store's size and what retention prunes,
    /// from `daemon.status` (read when Advanced opens).
    pub(super) fn storage_row(&self) -> View {
        let (value, note) = match self.settings.fleet.as_ref() {
            None => ("…".to_owned(), "Reading…".to_owned()),
            Some(Err(error)) => (
                "—".to_owned(),
                format!("Not read: {}", crate::state::plain_error(error)),
            ),
            Some(Ok(fleet)) => retention_lines(fleet.retention.as_ref()),
        };
        controls::setting_row(
            "Storage and history",
            &note,
            View::from(controls::body(value)),
        )
    }
}

/// "1 Agent", "40 Agents".
fn agents(count: usize) -> String {
    format!("{count} {}", if count == 1 { "Agent" } else { "Agents" })
}

fn unknown_limits(note: &str) -> Vec<KeyedView> {
    vec![KeyedView::new(
        format!("unknown:{note}"),
        controls::setting_row("Quota unknown", note, None),
    )]
}

/// When a window resets, exactly and from now: "Resets Fri 09:00 · in 3
/// days"; or that the time is not reported.
fn resets_detail(window: &crate::quota::Window, now: i64) -> String {
    match (window.resets_at_ms, window.until_reset(now)) {
        (Some(at), Some(until)) => format!("Resets {} · {until}", usage_ui::clock(at, now)),
        _ => crate::quota::sentence(&window.resets_in(now)),
    }
}

/// A hold's row: its few words ("On hold until 19:27") or the scope it
/// holds, why it holds and when Swarm checks again. The words say it is
/// held; no mark beside them.
fn hold_row(hold: &eliot_core::ProviderHold, title: &str, now: i64) -> KeyedView {
    use eliot_core::{ProviderHoldState, ProviderRestrictionKind as Kind};
    let why = match hold.kind {
        Kind::RateLimit => "Rate limited",
        Kind::Quota => "Usage limit reached",
        Kind::Auth => "Sign-in needed",
        Kind::ModelRemoved => "Model no longer offered",
        Kind::ModelMismatch => "Provider served another model",
    };
    let note = match (hold.state, hold.next_probe_at) {
        (ProviderHoldState::Probing, _) => format!("{why} · checking now"),
        (_, Some(at)) => format!("{why} · next check {}", usage_ui::clock(at, now)),
        _ => why.to_owned(),
    };
    KeyedView::new(
        format!("hold:{}:{title}:{note}", hold.scope),
        controls::setting_row(title, &note, None),
    )
}

/// A limit's share left in words ("21% left"), in the critical color
/// under [`controls::QUOTA_LOW`]; the words say the share, so no mark
/// beside them.
fn limit_value(remaining: i64) -> View {
    let value = controls::body(format!("{remaining}% left"));
    if (remaining as f64) < controls::QUOTA_LOW {
        value.foreground(ThemeBrush::SystemCritical).into()
    } else {
        value.into()
    }
}

/// `1234567` as `1,234,567`.
pub(super) fn group_digits(value: i64) -> String {
    let digits = value.unsigned_abs().to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    if value < 0 { format!("-{out}") } else { out }
}

/// The storage row: the store's size as the value; what retention keeps,
/// or why that could not be read, as the note. An unreadable
/// configuration is never shown as "kept forever".
pub(super) fn retention_lines(view: Option<&eliot_core::api::RetentionView>) -> (String, String) {
    let Some(view) = view else {
        return (
            "—".to_owned(),
            "This Swarm reports no storage view.".to_owned(),
        );
    };
    let size = crate::monitor::bytes_label(Some(view.db_bytes.saturating_add(view.wal_bytes)));
    if let Some(error) = &view.config_error {
        return (
            size,
            format!(
                "What history is kept could not be read: config.toml failed to load ({error})."
            ),
        );
    }
    if view.unbounded {
        // The daemon's size advisory says the unbounded store has grown
        // large; the size itself is shown beside this line.
        let advisory = if view.size_advisory.is_some() {
            " Set retention windows to keep the store smaller (configuration.md)."
        } else {
            ""
        };
        return (
            size,
            format!("Retention off. All history is kept.{advisory}"),
        );
    }
    let window = |days: Option<i64>| {
        days.map_or_else(
            || "kept".to_owned(),
            |days| format!("{days} {}", if days == 1 { "day" } else { "days" }),
        )
    };
    (
        size,
        format!(
            "History kept: events {}, child processes {}, commands {}, mail {}.",
            view.events_days.map_or_else(
                || view
                    .events_seconds
                    .map_or_else(|| "kept".to_owned(), |seconds| format!("{seconds}s")),
                |days| window(Some(days)),
            ),
            view.processes_seconds
                .map_or_else(|| "kept".to_owned(), |seconds| format!("{seconds}s")),
            window(view.commands_days),
            window(view.mail_days)
        ),
    )
}

/// The start of the local day, in ms since the epoch.
fn today_start_ms() -> i64 {
    use chrono::{Local, TimeZone};
    Local::now()
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .and_then(|midnight| Local.from_local_datetime(&midnight).earliest())
        .map_or(0, |start| start.timestamp_millis())
}
