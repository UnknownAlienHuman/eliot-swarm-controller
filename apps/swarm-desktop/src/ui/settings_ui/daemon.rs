//! The Settings pages over the daemon's own sections (skeleton 5.11):
//! Owner rules (`[owner_rules]`), Attention (`[daemon.attention.*]`),
//! Accounts & proxies (`[accounts]`, `[proxies]`, read here and edited in
//! the settings file), Daemon (`on_restart`, `log_level`, `chats_root`)
//! and Retention (`[daemon.retention]`). Each row is a title, one sentence
//! and its control; switches and dropdowns save at once, numbers and text
//! when typing rests, the owner rules text with Save.

use super::*;
use model::DaemonEdit;

/// A row's switch over a `[daemon]` key, keyed by its state.
fn daemon_switch(
    context: &mut ViewContext<DesktopApp>,
    daemon: &Value,
    pointer: &'static str,
    default: bool,
    name: &str,
) -> View {
    let on = model::daemon_bool(daemon, pointer, default);
    Grid::new().keyed_children(vec![KeyedView::new(
        format!("{pointer}:{on}"),
        ToggleSwitch::new()
            .is_on(on)
            .min_width(0.0)
            .automation_name(name.to_owned())
            .on_toggled(context.callback(move |on: bool| {
                Message::Settings(SettingsMsg::DaemonEdit(DaemonEdit::Switch(pointer, on)))
            })),
    )])
}

/// A row's number box over a `[daemon]` key; empty removes the key.
fn daemon_number(
    context: &mut ViewContext<DesktopApp>,
    daemon: &Value,
    pointer: &'static str,
    minimum: f64,
    name: &str,
) -> View {
    NumberBox::new()
        .width(tokens::layout::NUMBER_WIDTH)
        .minimum(minimum)
        .value(model::daemon_number(daemon, pointer))
        .automation_name(name.to_owned())
        .on_value_changed(context.callback(move |value| {
            Message::Settings(SettingsMsg::DaemonEdit(DaemonEdit::Number(pointer, value)))
        }))
        .into()
}

/// A row's dropdown over a `[daemon]` key: `(value, label)` choices.
fn daemon_choice(
    context: &mut ViewContext<DesktopApp>,
    daemon: &Value,
    pointer: &'static str,
    choices: &'static [(&'static str, &'static str)],
    name: &str,
) -> View {
    let current = model::daemon_text(daemon, pointer);
    let index = choices
        .iter()
        .position(|(value, _)| *value == current)
        .unwrap_or(0);
    let labels: Vec<String> = choices
        .iter()
        .map(|(_, label)| (*label).to_owned())
        .collect();
    Grid::new().keyed_children(vec![KeyedView::new(
        format!("{pointer}:{index}"),
        ComboBox::new()
            .width(tokens::page::SETTINGS_CONTROL)
            .items_source(labels)
            .selected_index(Some(index))
            .automation_name(name.to_owned())
            .on_selection_changed(context.callback(move |index: Option<usize>| {
                match index.and_then(|index| choices.get(index)) {
                    Some((value, _)) => Message::Settings(SettingsMsg::DaemonEdit(
                        DaemonEdit::Choice(pointer, value),
                    )),
                    None => Message::Settings(SettingsMsg::Reload),
                }
            })),
    )])
}

/// One attention kind: its title and sentence, its `enabled` key, its wait
/// key (when it takes one) and its one opt-in step (key, title, sentence).
struct Kind {
    title: &'static str,
    sentence: &'static str,
    enabled: &'static str,
    threshold: Option<&'static str>,
    step: Option<(&'static str, &'static str, &'static str)>,
}

/// The kinds of `docs/spec/configuration.md` (automatic attention).
const ATTENTION: [Kind; 8] = [
    Kind {
        title: "No progress",
        sentence: "Ask a task that stops reporting progress what blocks it, and tell its manager.",
        enabled: "/attention/no_progress/enabled",
        threshold: Some("/attention/no_progress/threshold_secs"),
        step: Some((
            "/attention/no_progress/interrupt",
            "Interrupt it",
            "Also stop the turn when it stays without progress.",
        )),
    },
    Kind {
        title: "No code progress",
        sentence: "Report a task that works without writing code.",
        enabled: "/attention/no_code_progress/enabled",
        threshold: Some("/attention/no_code_progress/threshold_secs"),
        step: None,
    },
    Kind {
        title: "Workers unattended",
        sentence: "Report a manager whose workers wait for it.",
        enabled: "/attention/children_unattended/enabled",
        threshold: Some("/attention/children_unattended/threshold_secs"),
        step: None,
    },
    Kind {
        title: "Agent lost",
        sentence: "Report an Agent whose process ended unexpectedly.",
        enabled: "/attention/agent_lost/enabled",
        threshold: None,
        step: None,
    },
    Kind {
        title: "Source unhealthy",
        sentence: "Report an agent app that stops answering Swarm.",
        enabled: "/attention/source_unhealthy/enabled",
        threshold: None,
        step: None,
    },
    Kind {
        title: "Provider refusal",
        sentence: "Report a provider that refuses a turn (limits, sign-in).",
        enabled: "/attention/provider_refusal/enabled",
        threshold: None,
        step: None,
    },
    Kind {
        title: "Session growth",
        sentence: "Report a session that grows near its context window.",
        enabled: "/attention/session_growth/enabled",
        threshold: None,
        step: Some((
            "/attention/session_growth/switch",
            "Switch the model",
            "Also hand the work to a new session when it grows too large.",
        )),
    },
    Kind {
        title: "Repeated command failure",
        sentence: "Report a command that fails three times in one turn.",
        enabled: "/attention/repeated_command_failure/enabled",
        threshold: None,
        step: None,
    },
];

impl DesktopApp {
    /// Owner rules: the text every agent receives at its start.
    pub(super) fn owner_rules_page(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let text = model::owner_rules_text(&self.settings.owner_rules);
        let bytes = text.len();
        let dirty = self.settings.dirty(Page::OwnerRules);
        StackPanel::new()
            .spacing(tokens::spacing::SM)
            .children((
                m3::card_title("Rules for every agent"),
                m3::settings_card(
                    palette,
                    vec![KeyedView::new(
                        "text",
                        StackPanel::new()
                            .spacing(tokens::spacing::SM)
                            .margin(Thickness::xy(0.0, tokens::spacing::INSET))
                            .children((
                                m3::secondary(
                                    "Each new task receives this text at its start; a running task keeps the text it started with.",
                                )
                                .text_wrapping(TextWrapping::Wrap),
                                m3::field()
                                    .text(text)
                                    .accepts_return(true)
                                    .text_wrapping(TextWrapping::Wrap)
                                    .min_height(tokens::layout::SOURCE_MIN_HEIGHT)
                                    .max_height(tokens::layout::SOURCE_MAX_HEIGHT)
                                    .is_enabled(!self.settings.busy())
                                    .automation_name("Owner rules")
                                    .on_text_changed(context.callback(|text| {
                                        Message::Settings(SettingsMsg::OwnerRulesText(text))
                                    })),
                                m3::secondary(if bytes > 8 * 1024 {
                                    format!("{bytes} bytes: longer than 8 KiB, which agents read slowly.")
                                } else {
                                    format!("{bytes} bytes")
                                }),
                            )),
                    )],
                ),
                self.save_bar(Page::OwnerRules, dirty, context, palette),
            ))
    }

    /// Attention: the daemon's reactions by kind, each on by default; the
    /// wait before reporting and the opt-in steps.
    pub(super) fn attention_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let daemon = &self.settings.daemon;
        let mut rows: Vec<KeyedView> = Vec::new();
        for kind in &ATTENTION {
            rows.push(KeyedView::new(
                kind.enabled,
                m3::setting_row(
                    kind.title,
                    kind.sentence,
                    daemon_switch(context, daemon, kind.enabled, true, kind.title),
                ),
            ));
            if let Some(threshold) = kind.threshold {
                rows.push(KeyedView::new(
                    threshold,
                    m3::setting_row(
                        &format!("{}: wait", kind.title),
                        "Seconds without it before Swarm reacts; at least 900. Empty uses the default.",
                        daemon_number(
                            context,
                            daemon,
                            threshold,
                            900.0,
                            &format!("{} wait in seconds", kind.title),
                        ),
                    ),
                ));
            }
            if let Some((pointer, title, sentence)) = kind.step {
                rows.push(KeyedView::new(
                    pointer,
                    m3::setting_row(
                        title,
                        sentence,
                        daemon_switch(context, daemon, pointer, false, title),
                    ),
                ));
            }
        }
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::card_title("Reactions"),
            m3::settings_card(palette, rows),
            m3::secondary("The daemon applies a change at its next start.")
                .text_wrapping(TextWrapping::Wrap),
        ))
    }

    /// Accounts & proxies: what the settings hold, by name; they are
    /// edited in the settings file.
    pub(super) fn accounts_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let list = |section: Section, what: &str| -> Vec<KeyedView> {
            let value = self.settings.base(section);
            let entries: Vec<(String, String)> = value
                .as_object()
                .map(|object| {
                    object
                        .iter()
                        .map(|(name, entry)| {
                            let words = entry
                                .as_object()
                                .map(|fields| {
                                    fields
                                        .iter()
                                        .filter(|(_, value)| {
                                            value.is_string()
                                                || value.is_number()
                                                || value.is_boolean()
                                        })
                                        .filter(|(key, _)| {
                                            !["secret", "token", "key", "password"]
                                                .iter()
                                                .any(|hidden| key.contains(hidden))
                                        })
                                        .map(|(key, value)| {
                                            format!(
                                                "{key} = {}",
                                                value.to_string().trim_matches('"')
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                        .join(" · ")
                                })
                                .unwrap_or_default();
                            (name.clone(), words)
                        })
                        .collect()
                })
                .unwrap_or_default();
            if entries.is_empty() {
                return vec![KeyedView::new(
                    "none",
                    m3::setting_row(&format!("No {what}"), "None is configured.", View::empty()),
                )];
            }
            entries
                .into_iter()
                .map(|(name, words)| {
                    KeyedView::new(name.clone(), m3::setting_row(&name, &words, View::empty()))
                })
                .collect()
        };
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::card_title("Accounts"),
            m3::settings_card(palette, list(Section::Accounts, "accounts")),
            m3::card_title("Proxies"),
            m3::settings_card(palette, list(Section::Proxies, "proxies")),
            m3::text_button(palette)
                .horizontal_alignment(HorizontalAlignment::Left)
                .automation_name("Edit in Settings file & history")
                .on_click(
                    context.message(Message::Settings(SettingsMsg::PageSelected(Page::Advanced))),
                )
                .content(m3::label("Edit in Settings file & history")),
        ))
    }

    /// Daemon: what a restart does, how much the log says, where chats
    /// work.
    pub(super) fn daemon_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        const RESTART: &[(&str, &str)] = &[("continue", "Continue"), ("hold", "Hold")];
        const LOG: &[(&str, &str)] = &[("standard", "Standard"), ("debug", "Debug")];
        let daemon = &self.settings.daemon;
        let chats = model::daemon_text(daemon, "/chats_root");
        StackPanel::new()
            .spacing(tokens::spacing::SM)
            .children((
                m3::card_title("Swarm"),
                m3::settings_card(
                    palette,
                    vec![
                        KeyedView::new(
                            "restart",
                            m3::setting_row(
                                "On restart",
                                "Continue resumes interrupted work after a restart; Hold leaves it for you.",
                                daemon_choice(context, daemon, "/on_restart", RESTART, "On restart"),
                            ),
                        ),
                        KeyedView::new(
                            "log",
                            m3::setting_row(
                                "Log level",
                                "Debug writes everything to daemon.log; it applies at once.",
                                daemon_choice(context, daemon, "/log_level", LOG, "Log level"),
                            ),
                        ),
                        KeyedView::new(
                            "chats",
                            m3::setting_row(
                                "Chats folder",
                                "Where chats without a project work; empty uses the data folder.",
                                m3::field()
                                    .width(tokens::page::SETTINGS_CONTROL)
                                    .text(chats)
                                    .placeholder_text("Default")
                                    .automation_name("Chats folder")
                                    .on_text_changed(context.callback(|text| {
                                        Message::Settings(SettingsMsg::DaemonEdit(
                                            DaemonEdit::Text("/chats_root", text),
                                        ))
                                    }))
                                    .into(),
                            ),
                        ),
                    ],
                ),
                m3::secondary("On restart and the chats folder apply at the next start.")
                    .text_wrapping(TextWrapping::Wrap),
            ))
    }

    /// Retention: how long the store keeps events, commands and mail.
    pub(super) fn retention_page(&self, context: &mut ViewContext<Self>, palette: Palette) -> View {
        let daemon = &self.settings.daemon;
        let windows: [(&'static str, &str, &str); 5] = [
            (
                "/retention/events_days",
                "Events",
                "Days of events to keep; empty keeps every event.",
            ),
            (
                "/retention/commands_days",
                "Commands",
                "Days of finished commands to keep; empty keeps them all.",
            ),
            (
                "/retention/mail_days",
                "Mail",
                "Days of read mail to keep; empty keeps it all.",
            ),
            (
                "/retention/processes_seconds",
                "Exited processes",
                "Seconds to keep exited child processes; empty keeps them.",
            ),
            (
                "/retention/interval_seconds",
                "Check every",
                "Seconds between retention passes, 1 to 86400; empty is 300.",
            ),
        ];
        let mut rows: Vec<KeyedView> = windows
            .into_iter()
            .map(|(pointer, title, sentence)| {
                KeyedView::new(
                    pointer,
                    m3::setting_row(
                        title,
                        sentence,
                        daemon_number(context, daemon, pointer, 1.0, title),
                    ),
                )
            })
            .collect();
        rows.push(KeyedView::new(
            "compact",
            m3::setting_row(
                "Compact at start",
                "Reclaim the store's free space when Swarm starts (needs about twice its size free).",
                daemon_switch(
                    context,
                    daemon,
                    "/retention/compact_on_start",
                    false,
                    "Compact at start",
                ),
            ),
        ));
        StackPanel::new().spacing(tokens::spacing::SM).children((
            m3::card_title("History"),
            m3::settings_card(palette, rows),
            m3::secondary("Retention windows apply at once.").text_wrapping(TextWrapping::Wrap),
        ))
    }
}
