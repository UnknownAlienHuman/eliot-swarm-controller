use super::*;

impl DesktopApp {
    /// Models & Usage (Antigravity's page as the pattern): what Swarm runs
    /// now, then each installed harness's limits as two-line rows (the
    /// limit and when it refreshes) with the share left and its ring.
    /// Storage and retention live in Advanced.
    pub(super) fn usage_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let today = today_start_ms();
        let loaded = self.nav.agents.len();
        let (tokens_today, reporting) = self
            .nav
            .agents
            .iter()
            .filter(|agent| agent.updated_at >= today)
            .fold((0i64, 0usize), |(sum, count), agent| match agent.tokens {
                Some(tokens) => (sum + tokens, count + 1),
                None => (sum, count),
            });
        let fleet = self.settings.fleet.as_ref();
        let total = fleet.and_then(|fleet| fleet.as_ref().ok().map(|fleet| fleet.agents));
        let (running_value, running_note) = match fleet {
            None => ("…".to_owned(), "Counting…".to_owned()),
            Some(Ok(fleet)) => (
                fleet.running.to_string(),
                format!("Working or starting, of {} in Swarm", agents(fleet.agents)),
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
        let totals = m3::settings_card(
            palette,
            vec![
                KeyedView::new(
                    format!("running:{running_value}:{running_note}"),
                    m3::setting_row(
                        "Running Agents",
                        &running_note,
                        m3::body(running_value).into(),
                    ),
                ),
                KeyedView::new(
                    format!("tokens:{tokens_today}:{coverage}"),
                    m3::setting_row(
                        "Tokens today",
                        &coverage,
                        m3::body(group_digits(tokens_today)).into(),
                    ),
                ),
            ],
        );
        let installed: Vec<(String, Option<String>)> = match &self.launch.catalog {
            Some(Ok(reply)) => reply
                .harnesses
                .iter()
                .filter(|entry| entry.installed)
                .map(|entry| (entry.harness.clone(), entry.version.clone()))
                .collect(),
            _ => crate::picker::HARNESSES
                .iter()
                .map(|harness| ((*harness).to_owned(), None))
                .collect(),
        };
        let now = crate::handoff::now_ms();
        let mut cards: Vec<KeyedView> = Vec::new();
        for (harness, version) in installed {
            let label = crate::picker::harness_label(&harness);
            let provider =
                crate::picker::provider_label(&eliot_core::api::catalog_provider(&harness, ""));
            let title = if provider.eq_ignore_ascii_case(label) || provider.is_empty() {
                label.to_owned()
            } else {
                format!("{label} · {provider}")
            };
            let rows: Vec<KeyedView> = match self.settings.quotas.get(&harness) {
                None => vec![KeyedView::new(
                    "reading",
                    m3::setting_row("Reading the limits…", "", View::empty()),
                )],
                Some(Err(error)) => vec![KeyedView::new(
                    "error",
                    m3::setting_row(
                        "Limits not read",
                        &crate::state::plain_error(error),
                        View::empty(),
                    ),
                )],
                Some(Ok(reply)) => {
                    let windows = crate::quota::windows(reply);
                    if windows.is_empty() {
                        vec![KeyedView::new(
                            "status",
                            m3::setting_row(
                                &crate::quota::status_line(label, reply),
                                "",
                                View::empty(),
                            ),
                        )]
                    } else {
                        let observed = crate::quota::observed_note(reply);
                        windows
                            .iter()
                            .enumerate()
                            .map(|(index, window)| {
                                let title = format!("{} remaining", window.name());
                                let mut note = crate::quota::sentence(&window.resets_in(now));
                                if let Some(observed) = &observed {
                                    note = format!("{note} · {observed}");
                                }
                                KeyedView::new(
                                    format!("{index}:{title}:{note}:{}", window.remaining),
                                    m3::setting_row(&title, &note, limit_value(window.remaining)),
                                )
                            })
                            .collect()
                    }
                }
            };
            cards.push(KeyedView::new(
                format!("harness:{harness}"),
                StackPanel::new().spacing(tokens::spacing::SM).children((
                    StackPanel::new()
                        .orientation(Orientation::Horizontal)
                        .spacing(tokens::spacing::SM)
                        .margin(Thickness::new(0.0, tokens::spacing::SM, 0.0, 0.0))
                        .children((
                            m3::section_title(title),
                            m3::secondary(version.unwrap_or_default())
                                .vertical_alignment(VerticalAlignment::Center),
                        )),
                    m3::settings_card(palette, rows),
                )),
            ));
        }
        StackPanel::new().spacing(tokens::spacing::MD).children((
            StackPanel::new().spacing(tokens::spacing::SM).children((
                m3::section_heading(
                    "Swarm",
                    "",
                    Some(
                        m3::text_button(palette)
                            .is_enabled(
                                self.settings.quotas_loading == 0 && !self.settings.fleet_loading,
                            )
                            .automation_name("Read the limits again")
                            .on_click(context.message(Message::Settings(SettingsMsg::UsageRefresh)))
                            .content(m3::label("Refresh")),
                    ),
                ),
                totals,
            )),
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .keyed_children(cards),
        ))
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
        m3::setting_row("Storage and history", &note, m3::body(value).into())
    }
}

/// "1 Agent", "40 Agents".
fn agents(count: usize) -> String {
    format!("{count} {}", if count == 1 { "Agent" } else { "Agents" })
}

/// A limit's share left and its ring (Antigravity's rows).
fn limit_value(remaining: i64) -> View {
    StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(tokens::spacing::SM)
        .children((
            m3::body(format!("{remaining}%")).vertical_alignment(VerticalAlignment::Center),
            m3::ring(remaining as f64),
        ))
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
        let advisory = view
            .size_advisory
            .as_deref()
            .map(|advisory| format!(" {advisory}."))
            .unwrap_or_default();
        return (
            size,
            format!(
                "All history is kept: no retention window is set (daemon.retention).{advisory}"
            ),
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
            "History kept: events {}, commands {}, mail {}.",
            window(view.events_days),
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
