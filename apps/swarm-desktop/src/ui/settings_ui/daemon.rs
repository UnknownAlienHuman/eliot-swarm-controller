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
            // A selection WinUI reports for the list it was given is the
            // value shown: the edit then changes nothing.
            .on_selection_changed(context.callback(move |index: Option<usize>| {
                Message::Settings(SettingsMsg::DaemonEdit(
                    match index.and_then(|index| choices.get(index)) {
                        Some((value, _)) => DaemonEdit::Choice(pointer, value),
                        None => DaemonEdit::Unchanged,
                    },
                ))
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

/// How a `[daemon]` value reads in the confirmation.
#[derive(Clone, Copy)]
enum Unit {
    /// A switch, on by default or not.
    Switch(bool),
    Seconds,
    /// A retention window: unset keeps everything.
    Days,
    WindowSeconds,
    Choice(&'static [(&'static str, &'static str)]),
    Text,
}

/// What a change does, as the confirmation says it.
#[derive(Clone, Copy)]
enum Effect {
    NextStart,
    AtOnce,
    /// A retention window: shortening it deletes history.
    Deletes,
}

const RESTART_CHOICES: &[(&str, &str)] = &[("continue", "Continue"), ("hold", "Hold")];
const LOG_CHOICES: &[(&str, &str)] = &[("standard", "Standard"), ("debug", "Debug")];

/// Every `[daemon]` key the three pages edit, for the confirmation.
fn daemon_fields() -> Vec<(&'static str, String, Unit, Effect)> {
    let mut fields = Vec::new();
    for kind in &ATTENTION {
        fields.push((
            kind.enabled,
            kind.title.to_owned(),
            Unit::Switch(true),
            Effect::NextStart,
        ));
        if let Some(threshold) = kind.threshold {
            fields.push((
                threshold,
                format!("{}: wait", kind.title),
                Unit::Seconds,
                Effect::NextStart,
            ));
        }
        if let Some((pointer, title, _)) = kind.step {
            fields.push((
                pointer,
                title.to_owned(),
                Unit::Switch(false),
                Effect::NextStart,
            ));
        }
    }
    fields.extend([
        (
            "/on_restart",
            "On restart".to_owned(),
            Unit::Choice(RESTART_CHOICES),
            Effect::NextStart,
        ),
        (
            "/log_level",
            "Log level".to_owned(),
            Unit::Choice(LOG_CHOICES),
            Effect::AtOnce,
        ),
        (
            "/chats_root",
            "Chats folder".to_owned(),
            Unit::Text,
            Effect::NextStart,
        ),
        (
            "/retention/events_days",
            "Events kept".to_owned(),
            Unit::Days,
            Effect::Deletes,
        ),
        (
            "/retention/commands_days",
            "Commands kept".to_owned(),
            Unit::Days,
            Effect::Deletes,
        ),
        (
            "/retention/mail_days",
            "Mail kept".to_owned(),
            Unit::Days,
            Effect::Deletes,
        ),
        (
            "/retention/processes_seconds",
            "Exited processes kept".to_owned(),
            Unit::WindowSeconds,
            Effect::Deletes,
        ),
        (
            "/retention/interval_seconds",
            "Retention check every".to_owned(),
            Unit::Seconds,
            Effect::AtOnce,
        ),
        (
            "/retention/compact_on_start",
            "Compact at start".to_owned(),
            Unit::Switch(false),
            Effect::NextStart,
        ),
    ]);
    fields
}

/// A value in words.
fn value_words(value: Option<&Value>, unit: Unit) -> String {
    match (unit, value) {
        (Unit::Switch(default), value) => {
            if value.and_then(Value::as_bool).unwrap_or(default) {
                "on".to_owned()
            } else {
                "off".to_owned()
            }
        }
        (Unit::Days, Some(value)) => format!("{value} days"),
        (Unit::WindowSeconds | Unit::Seconds, Some(value)) => format!("{value} s"),
        (Unit::Days | Unit::WindowSeconds, None) => "forever".to_owned(),
        (Unit::Seconds, None) => "the default".to_owned(),
        (Unit::Choice(choices), value) => {
            let value = value.and_then(Value::as_str).unwrap_or(choices[0].0);
            choices
                .iter()
                .find(|(key, _)| *key == value)
                .map_or_else(|| value.to_owned(), |(_, label)| (*label).to_owned())
        }
        (Unit::Text, Some(value)) => value.as_str().unwrap_or_default().to_owned(),
        (Unit::Text, None) => "the default".to_owned(),
    }
}

/// The changes between the saved `[daemon]` section and the draft, one
/// sentence each, and whether one of them deletes history.
pub(super) fn daemon_changes(base: &Value, draft: &Value) -> (Vec<String>, bool) {
    let mut lines = Vec::new();
    let mut deletes = false;
    for (pointer, label, unit, effect) in daemon_fields() {
        let (before, after) = (base.pointer(pointer), draft.pointer(pointer));
        let (was, now) = (value_words(before, unit), value_words(after, unit));
        if was == now {
            continue;
        }
        let effect = match effect {
            Effect::NextStart => "Applies at the daemon's next start.".to_owned(),
            Effect::AtOnce => "Applies at once.".to_owned(),
            Effect::Deletes => {
                let shorter = match (
                    before.and_then(Value::as_i64),
                    after.and_then(Value::as_i64),
                ) {
                    (Some(before), Some(after)) => after < before,
                    (None, Some(_)) => true,
                    _ => false,
                };
                if shorter {
                    deletes = true;
                    "Older history is deleted at the next retention pass and cannot be read back."
                        .to_string()
                } else {
                    "Nothing more is deleted.".to_owned()
                }
            }
        };
        lines.push(format!("{label}: {was} → {now}. {effect}"));
    }
    (lines, deletes)
}

impl DesktopApp {
    /// The confirmation before the daemon's settings save: each change and
    /// what it does (Retention deletes history; the rest apply at the
    /// daemon's next start or at once).
    pub(in crate::ui) fn daemon_confirmation(
        &self,
        context: &mut ViewContext<Self>,
        palette: Palette,
    ) -> View {
        let (lines, deletes) =
            daemon_changes(&self.settings.base(Section::Daemon), &self.settings.daemon);
        let mut rows: Vec<KeyedView> = lines
            .iter()
            .enumerate()
            .map(|(index, line)| {
                KeyedView::new(
                    index,
                    m3::body(format!("• {line}")).text_wrapping(TextWrapping::Wrap),
                )
            })
            .collect();
        if rows.is_empty() {
            rows.push(KeyedView::new(
                "none",
                m3::body("Nothing changed on this page.").text_wrapping(TextWrapping::Wrap),
            ));
        }
        let save = if deletes {
            m3::destructive(palette)
        } else {
            m3::filled(palette)
        };
        super::super::dialog_ui::surface(
            context,
            self.dialog.epoch,
            palette,
            if deletes {
                "Save and delete older history?".to_owned()
            } else {
                "Save these daemon settings?".to_owned()
            },
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .keyed_children(rows),
            super::super::dialog_ui::actions(
                m3::text_button(palette)
                    .element_ref(&self.dialog.close)
                    .automation_name("Cancel")
                    .on_click(
                        context.message(Message::Settings(SettingsMsg::DaemonConfirmed(false))),
                    )
                    .content(m3::label("Cancel")),
                save.element_ref(&self.dialog.primary)
                    .is_enabled(!lines.is_empty())
                    .automation_name("Save the daemon settings")
                    .on_click(
                        context.message(Message::Settings(SettingsMsg::DaemonConfirmed(true))),
                    )
                    .content(m3::label("Save")),
            ),
            tokens::layout::DIALOG_WIDTH,
        )
    }

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
            m3::secondary("The daemon applies a change at its next start. Save asks first.")
                .text_wrapping(TextWrapping::Wrap),
            self.save_bar(
                Page::Attention,
                self.settings.dirty(Page::Attention),
                context,
                palette,
            ),
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
        const RESTART: &[(&str, &str)] = RESTART_CHOICES;
        const LOG: &[(&str, &str)] = LOG_CHOICES;
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
                m3::secondary(
                    "On restart and the chats folder apply at the next start. Save asks first.",
                )
                .text_wrapping(TextWrapping::Wrap),
                self.save_bar(Page::Daemon, self.settings.dirty(Page::Daemon), context, palette),
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
            m3::secondary(
                "Retention windows apply at once; a shorter one deletes older history. Save asks first.",
            )
            .text_wrapping(TextWrapping::Wrap),
            self.save_bar(
                Page::Retention,
                self.settings.dirty(Page::Retention),
                context,
                palette,
            ),
        ))
    }
}
