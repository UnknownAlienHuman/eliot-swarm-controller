use super::*;

impl DesktopApp {
    /// Models & Usage (Antigravity's page as the pattern): each installed
    /// harness's remaining provider quota as rings (weekly and 5-hour where
    /// the provider reports them), and what Swarm is running.
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
                format!(
                    "Working now or starting, of {} Agent(s) in Swarm (exact counts).",
                    fleet.agents
                ),
            ),
            Some(Err(error)) => (
                "not counted".to_owned(),
                format!("Could not count: {}", crate::state::plain_error(error)),
            ),
        };
        let coverage = match total {
            Some(total) if total > loaded => format!(
                "Partial: from the {loaded} most recent Agents loaded here, of {total} in Swarm; {reporting} active today report tokens (their totals; Swarm has no per-day usage read)."
            ),
            _ => format!(
                "From all {loaded} Agent(s); {reporting} active today report tokens (their totals; Swarm has no per-day usage read)."
            ),
        };
        let (retention_value, retention_note) = match fleet {
            None => ("…".to_owned(), "Reading…".to_owned()),
            Some(Err(_)) => (
                "not read".to_owned(),
                "daemon.status could not be read.".to_owned(),
            ),
            Some(Ok(fleet)) => retention_lines(fleet.retention.as_ref()),
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
                KeyedView::new(
                    format!("retention:{retention_value}:{retention_note}"),
                    m3::setting_row(
                        "History retention",
                        &retention_note,
                        m3::body(retention_value).into(),
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
                    m3::setting_row("Quota", "Reading…", View::empty()),
                )],
                Some(Err(error)) => vec![KeyedView::new(
                    "error",
                    m3::setting_row(
                        &format!("{label}: quota not read"),
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
                        let now = crate::handoff::now_ms();
                        let observed = crate::quota::observed_note(reply);
                        windows
                            .iter()
                            .enumerate()
                            .map(|(index, window)| {
                                let mut line = window.line(now);
                                if let Some(observed) = &observed {
                                    line = format!("{line} · {observed}");
                                }
                                KeyedView::new(
                                    format!("{index}:{line}"),
                                    m3::setting_row(&line, "", m3::ring(window.remaining as f64)),
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
                            .automation_name("Read quota again")
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

/// The retention row: the configured windows, or why they are unknown.
/// An unreadable configuration is never shown as "unbounded".
pub(super) fn retention_lines(view: Option<&eliot_core::api::RetentionView>) -> (String, String) {
    let Some(view) = view else {
        return (
            "not reported".to_owned(),
            "This daemon reports no retention view.".to_owned(),
        );
    };
    let store = format!(
        "Store {} (database and log).",
        crate::monitor::bytes_label(Some(view.db_bytes.saturating_add(view.wal_bytes)))
    );
    if let Some(error) = &view.config_error {
        return (
            "unknown".to_owned(),
            format!("config.toml could not be read ({error}); the windows are unknown. {store}"),
        );
    }
    if view.unbounded {
        let advisory = view
            .size_advisory
            .as_deref()
            .map(|advisory| format!(" {advisory}."))
            .unwrap_or_default();
        return (
            "unbounded".to_owned(),
            format!("No daemon.retention window is set: nothing is pruned. {store}{advisory}"),
        );
    }
    let window =
        |days: Option<i64>| days.map_or_else(|| "kept".to_owned(), |days| format!("{days} d"));
    (
        "bounded".to_owned(),
        format!(
            "Events {}, commands {}, mail {}. {store}",
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
