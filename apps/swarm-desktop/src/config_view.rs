//! WinUI views for the Settings pages Models and profiles, Automation (its
//! rules) and Scripts.
//!
//! Profiles: one document draft (the `profiles` settings section as
//! `[profiles.*]` TOML) shown both as a few common form fields for the
//! selected profile and, on request, as the full source text; both edit the
//! same text. The Settings save bar validates and saves it through the
//! daemon; an invalid draft stays here. Automation is read-only: no daemon
//! operation pauses or resumes a rule or timer.

use windows_reactor::*;

use crate::profiles::{self, FORM_FIELDS};
use crate::theme::winui::Palette;
use crate::ui::tokens;
use eliot_core::api::{ProfileSummary, RuleListEntry, TimerSchedule};

/// A note: secondary text, wrapped and selectable.
fn small(text: impl Into<String>) -> TextBlock {
    crate::m3::secondary(text)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

/// A failure note: the secondary size at full emphasis in the error color.
fn error_note(text: impl Into<String>, palette: Palette) -> TextBlock {
    crate::m3::text(text, tokens::text::SECONDARY)
        .foreground(palette.error())
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

/// State the Models and profiles section renders.
pub struct ProfilesData<'a> {
    pub file_valid: bool,
    pub file_error: Option<&'a str>,
    pub editable: bool,
    pub reason: Option<&'a str>,
    pub draft: &'a str,
    /// The source TextBox's own text, bound back unchanged.
    pub view: &'a str,
    pub selected: Option<&'a str>,
    pub duplicate_name: &'a str,
    pub status: &'a str,
    pub busy: bool,
    pub summaries: &'a [ProfileSummary],
    /// The TOML source editor is shown.
    pub source_open: bool,
}

/// Callbacks the Models and profiles section emits.
pub struct ProfilesActions {
    pub draft_changed: Callback<String>,
    pub select: Callback<Option<usize>>,
    pub field_changed: Vec<Callback<String>>,
    pub duplicate_name_changed: Callback<String>,
    pub duplicate: Callback<()>,
    pub toggle_source: Callback<()>,
    /// One per [`FORM_FIELDS`] entry: a dropdown index picked.
    pub choice_picked: Vec<Callback<Option<usize>>>,
}

/// Form labels of [`FORM_FIELDS`].
fn field_label(key: &str) -> &str {
    match key {
        "model" => "Model",
        "effort" => "Effort",
        "permissions" => "Permissions",
        "approval_policy" => "Approval policy",
        other => other,
    }
}

/// Models and profiles: one 32-DIP row per profile, the selected profile's
/// form (a 200-DIP label column; dropdowns sized for their values, text the
/// full width), and the full TOML source on request. Form and source edit
/// one draft.
pub fn profiles_screen(data: ProfilesData<'_>, actions: ProfilesActions, palette: Palette) -> View {
    let names = profiles::profile_names(data.draft);
    let mut rows: Vec<KeyedView> = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let selected = data.selected == Some(name.as_str());
        let summary = data.summaries.iter().find(|profile| &profile.name == name);
        let detail = match summary {
            Some(profile) => {
                let mut line = format!(
                    "{} · {} · {}",
                    profile.harness,
                    profile.model.as_deref().unwrap_or("default model"),
                    profile.effort.as_deref().unwrap_or("default effort")
                );
                if !profile.launchable {
                    line.push_str(" · cannot launch now");
                }
                line
            }
            None => "not saved yet".to_owned(),
        };
        let select = actions.select.clone();
        rows.push(KeyedView::new(
            format!("{name}:{selected}"),
            crate::m3::row(palette, selected)
                .automation_name(format!("Profile {name}: {detail}"))
                .on_click(move || {
                    let _ = select.call(Some(index));
                })
                .content(crate::m3::row_content(None, name, &detail, View::empty())),
        ));
    }
    if rows.is_empty() {
        rows.push(KeyedView::new(
            "none",
            small(if data.editable {
                "No profiles yet: add one with Edit as TOML."
            } else {
                "Profiles cannot be shown."
            }),
        ));
    }

    let mut form: Vec<KeyedView> = Vec::new();
    if let Some(name) = data.selected.filter(|_| data.editable) {
        form.push(KeyedView::new(
            "name",
            crate::m3::section_title(name.to_owned())
                .height(tokens::height::FORM_ROW)
                .vertical_alignment(VerticalAlignment::Center),
        ));
        if let Some(error) = data
            .summaries
            .iter()
            .find(|profile| profile.name == name)
            .and_then(|profile| profile.error.as_deref())
        {
            form.push(KeyedView::new(
                "error",
                error_note(format!("Cannot launch now: {error}"), palette),
            ));
        }
        for (index, key) in FORM_FIELDS.iter().enumerate() {
            // Only plain strings round-trip through the form; anything else
            // is edited in the source so its value cannot change.
            let (value, editable, hint) = match profiles::field(data.draft, name, key) {
                profiles::Field::Absent => (String::new(), true, "Harness default".to_owned()),
                profiles::Field::Simple(value) => (value, true, String::new()),
                profiles::Field::Complex => (
                    String::new(),
                    false,
                    "Complex value: edit it as TOML".to_owned(),
                ),
            };
            let choices = editable
                .then(|| profiles::choices(key, Some(value.as_str()).filter(|v| !v.is_empty())))
                .flatten();
            let field: View = match (choices, actions.choice_picked.get(index).cloned()) {
                // An enumerated field: a dropdown sized for its values.
                // WinUI drops a ComboBox's selection when its items change,
                // so the choices key the control.
                (Some(choices), Some(picked)) => {
                    let selected = choices
                        .iter()
                        .position(|choice| *choice == value)
                        .unwrap_or(0);
                    Grid::new().keyed_children(vec![KeyedView::new(
                        choices.join("\u{1f}"),
                        crate::m3::enum_width(ComboBox::new())
                            .items_source(choices)
                            .selected_index(selected)
                            .is_enabled(!data.busy)
                            .automation_name(format!("Profile {}", field_label(key)))
                            .on_selection_changed(picked),
                    )])
                }
                _ => {
                    let callback = actions.field_changed.get(index).cloned();
                    let mut field = crate::m3::field()
                        .text(value)
                        .placeholder_text(hint)
                        .automation_name(format!("Profile {}", field_label(key)))
                        .is_enabled(!data.busy && editable);
                    if let Some(callback) = callback.filter(|_| editable) {
                        field = field.on_text_changed(callback);
                    }
                    field.into()
                }
            };
            form.push(KeyedView::new(
                format!("field-{key}"),
                crate::m3::form_row(field_label(key), field, None),
            ));
        }
        form.push(KeyedView::new(
            "duplicate",
            crate::m3::form_row(
                "Duplicate as",
                crate::m3::field()
                    .text(data.duplicate_name.to_owned())
                    .placeholder_text("New profile name")
                    .automation_name("Duplicate as")
                    .on_text_changed(actions.duplicate_name_changed)
                    .into(),
                Some(
                    crate::m3::outlined(palette)
                        .is_enabled(!data.busy)
                        .automation_name("Duplicate profile")
                        .on_click(actions.duplicate)
                        .content(crate::m3::label("Duplicate")),
                ),
            ),
        ));
    }
    let state = if !data.file_valid {
        format!(
            "The saved settings do not parse: {}. New tasks cannot start until it is fixed.",
            data.file_error.unwrap_or("unparseable")
        )
    } else if !data.editable {
        format!("Not editable: {}", data.reason.unwrap_or("not editable"))
    } else {
        "Running Agents keep the settings they started with; the next task uses the saved ones."
            .to_owned()
    };
    let toggle = crate::m3::text_button(palette)
        .horizontal_alignment(HorizontalAlignment::Left)
        .automation_name(if data.source_open {
            "Hide the TOML source"
        } else {
            "Edit as TOML"
        })
        .on_click(actions.toggle_source)
        .content(crate::m3::label(if data.source_open {
            "Hide TOML"
        } else {
            "Edit as TOML"
        }));
    let source: View = if data.source_open {
        crate::m3::field()
            .text(data.view.to_owned())
            .accepts_return(true)
            .text_wrapping(TextWrapping::NoWrap)
            .is_enabled(data.editable && !data.busy)
            .automation_name("Profile source")
            .min_height(tokens::layout::SOURCE_MIN_HEIGHT)
            .max_height(tokens::layout::SOURCE_MAX_HEIGHT)
            .on_text_changed(actions.draft_changed)
            .into()
    } else {
        View::empty()
    };
    let form: View = if form.is_empty() {
        View::empty()
    } else {
        crate::m3::group(palette, form)
    };
    // The Settings page scrolls, so the save bar follows.
    StackPanel::new().spacing(tokens::spacing::MD).children((
        crate::m3::list(palette, rows),
        form,
        small(state),
        toggle,
        source,
        small(data.status.to_owned()),
    ))
}

fn utc(ms: i64) -> String {
    let seconds = ms.div_euclid(1000).rem_euclid(86_400);
    format!(
        "{:02}:{:02}:{:02} UTC",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60
    )
}

fn compact(value: &serde_json::Value) -> String {
    let text = value.to_string();
    if text.chars().count() > 200 {
        format!("{}…", text.chars().take(200).collect::<String>())
    } else {
        text
    }
}

/// Text lines for one rule, all from the stored state and definition.
pub fn rule_lines(entry: &RuleListEntry) -> Vec<String> {
    let state = &entry.state;
    let mut lines = vec![format!(
        "{} · {} · revision {}{}{}",
        state.id,
        if state.enabled { "enabled" } else { "disabled" },
        state.revision,
        state
            .paused_reason
            .as_deref()
            .map(|reason| format!(" · paused: {reason}"))
            .unwrap_or_default(),
        state
            .retiring_revision
            .map(|revision| format!(" · retiring revision {revision}"))
            .unwrap_or_default(),
    )];
    match &entry.timer {
        Some(timer) => {
            let schedule = match &timer.schedule {
                TimerSchedule::Interval { period_ms, .. } => {
                    format!("every {} s", period_ms / 1000)
                }
                TimerSchedule::Cron { expression, tz } => format!("cron {expression} ({tz})"),
                TimerSchedule::Once { at } => format!("once at {}", utc(*at)),
            };
            lines.push(format!(
                "timer: {schedule} · next due {}",
                timer.next_due_at.map_or_else(|| "none".to_owned(), utc)
            ));
        }
        None => {
            let trigger = entry
                .definition
                .get("on")
                .map_or_else(|| "unknown trigger".to_owned(), compact);
            lines.push(format!("on: {trigger}"));
        }
    }
    if let Some(actions) = entry
        .definition
        .get("do")
        .and_then(|value| value.as_array())
    {
        for action in actions.iter().take(6) {
            lines.push(format!("do: {}", compact(action)));
        }
        if actions.len() > 6 {
            lines.push(format!("… {} more action(s)", actions.len() - 6));
        }
    }
    lines.push(match entry.definition.get("limit") {
        Some(limit) => format!("limit: {}", compact(limit)),
        None => "limit: none".to_owned(),
    });
    lines
}

/// One namespace read for the Automation screen.
pub struct RuleNamespace {
    /// Registered project id; absent for the global namespace.
    pub project: Option<String>,
    pub label: String,
    pub rules: Result<Vec<RuleListEntry>, String>,
    pub more: bool,
}

/// Rules and timers of every namespace, read-only.
pub fn automation_screen(
    namespaces: &[RuleNamespace],
    status: &str,
    refresh: Callback<()>,
    palette: Palette,
) -> View {
    let mut rows: Vec<KeyedView> = Vec::new();
    for namespace in namespaces {
        match &namespace.rules {
            Ok(rules) if rules.is_empty() => {}
            Ok(rules) => {
                rows.push(KeyedView::new(
                    format!("ns-{}", namespace.label),
                    crate::m3::section_title(namespace.label.clone()),
                ));
                for entry in rules {
                    rows.push(KeyedView::new(
                        format!("{}/{}", namespace.label, entry.state.id),
                        crate::m3::body(rule_lines(entry).join("\n"))
                            .text_wrapping(TextWrapping::Wrap)
                            .is_text_selection_enabled(true),
                    ));
                }
                if namespace.more {
                    rows.push(KeyedView::new(
                        format!("more-{}", namespace.label),
                        small("More rules exist beyond this page."),
                    ));
                }
            }
            Err(error) => rows.push(KeyedView::new(
                format!("error-{}", namespace.label),
                error_note(format!("{}: not read ({error})", namespace.label), palette),
            )),
        }
    }
    if rows.is_empty() {
        rows.push(KeyedView::new(
            "none",
            small(if status.is_empty() || status.starts_with("Reading") {
                "Reading…"
            } else {
                "No rules or timers."
            }),
        ));
    }
    StackPanel::new()
        .spacing(tokens::spacing::SM)
        .children((
            crate::m3::section_heading(
                "Rules and timers",
                "Read-only here. Rules live in <data_dir>/rules/*.toml and reload when the file changes.",
                Some(
                    crate::m3::text_button(palette)
                        .automation_name("Refresh rules")
                        .on_click(refresh)
                        .content(crate::m3::label("Refresh")),
                ),
            ),
            crate::m3::card(palette).content(
                ScrollViewer::new()
                    .max_height(tokens::layout::RULES_MAX_HEIGHT)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .automation_name("Rules and timers")
                    .content(
                        StackPanel::new()
                            .spacing(tokens::spacing::SM)
                            .keyed_children(rows),
                    ),
            ),
        ))
}

/// State the Scripts card renders.
pub struct ScriptsData<'a> {
    pub labels: Vec<String>,
    pub unavailable: &'a [String],
    pub selected: Option<usize>,
    pub display_path: Option<&'a str>,
    pub revision: Option<&'a str>,
    pub editable: bool,
    pub reason: Option<&'a str>,
    /// The editor TextBox's own text, bound back unchanged.
    pub view: &'a str,
    pub dirty: bool,
    pub timers: &'a [String],
    pub editor_exe: &'a str,
    pub status: &'a str,
    pub run_status: &'a str,
    pub busy: bool,
    pub run_busy: bool,
}

/// Callbacks the Scripts card emits; `run` has one entry per timer.
pub struct ScriptsActions {
    pub select: Callback<Option<usize>>,
    pub draft_changed: Callback<String>,
    pub validate: Callback<()>,
    pub save: Callback<()>,
    pub reload: Callback<()>,
    pub refresh: Callback<()>,
    pub editor_changed: Callback<String>,
    pub open_editor: Callback<()>,
    pub run: Vec<Callback<()>>,
}

/// Scripts: pick an entry, edit it (Save replaces the file only while its
/// revision matches; saving never runs it), open it in an editor, or run
/// it through a timer that names it. Save is the page's one filled action.
pub fn scripts_screen(data: ScriptsData<'_>, actions: ScriptsActions, palette: Palette) -> View {
    let selected = data.selected.is_some();
    let refresh = crate::m3::text_button(palette)
        .automation_name("Refresh script list")
        .on_click(actions.refresh)
        .content(crate::m3::label("Refresh"));
    let mut parts: Vec<KeyedView> = Vec::new();
    let no_entries = data.labels.is_empty();
    if no_entries {
        parts.push(KeyedView::new(
            "none",
            crate::m3::form_row(
                "Script",
                small("No scripts. Add files under <data_dir>/scripts/ or a project's .eliot/scripts/.")
                    .into(),
                Some(refresh),
            ),
        ));
    } else {
        parts.push(KeyedView::new(
            format!("entries:{}", data.labels.join("\u{1f}")),
            crate::m3::form_row(
                "Script",
                ComboBox::new()
                    .items_source(data.labels)
                    .selected_index(data.selected)
                    .placeholder_text("Choose a script")
                    .automation_name("Script entry")
                    .horizontal_alignment(HorizontalAlignment::Stretch)
                    .on_selection_changed(actions.select)
                    .into(),
                Some(refresh),
            ),
        ));
    }
    if !data.unavailable.is_empty() {
        parts.push(KeyedView::new(
            "unavailable",
            crate::m3::help(format!("Not readable: {}", data.unavailable.join("; "))),
        ));
    }
    if selected {
        let state = match (data.display_path, data.revision) {
            _ if !data.editable => format!(
                "Not editable here: {}",
                data.reason.unwrap_or("withheld by the daemon")
            ),
            (Some(path), _) if data.dirty => format!("{path} · unsaved changes"),
            (Some(path), Some(revision)) => format!(
                "{path} · saved (revision {})",
                revision.chars().take(12).collect::<String>()
            ),
            _ => String::new(),
        };
        parts.push(KeyedView::new("state", crate::m3::help(state)));
        parts.push(KeyedView::new(
            "editor",
            crate::m3::form_row(
                "Editor",
                crate::m3::field()
                    .text(data.editor_exe.to_owned())
                    .placeholder_text("Editor .exe (absolute path)")
                    .automation_name("Editor executable")
                    .on_text_changed(actions.editor_changed)
                    .into(),
                Some(
                    crate::m3::outlined(palette)
                        .is_enabled(data.display_path.is_some())
                        .automation_name("Open in editor")
                        .on_click(actions.open_editor)
                        .content(crate::m3::label("Open in editor")),
                ),
            ),
        ));
    }
    let picker = crate::m3::group(palette, parts);
    let mut below: Vec<KeyedView> = Vec::new();
    if selected {
        below.push(KeyedView::new(
            "source",
            crate::m3::field()
                .text(data.view.to_owned())
                .accepts_return(true)
                .text_wrapping(TextWrapping::NoWrap)
                .is_enabled(data.editable && !data.busy)
                .automation_name("Script source")
                .min_height(tokens::layout::SOURCE_MIN_HEIGHT)
                .max_height(tokens::layout::SOURCE_MAX_HEIGHT)
                .on_text_changed(actions.draft_changed),
        ));
        below.push(KeyedView::new(
            "actions",
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(tokens::spacing::SM)
                .children((
                    crate::m3::filled(palette)
                        .is_enabled(data.editable && data.dirty && !data.busy)
                        .automation_name("Save script")
                        .on_click(actions.save)
                        .content(crate::m3::label("Save")),
                    crate::m3::outlined(palette)
                        .is_enabled(data.editable && !data.busy)
                        .automation_name("Validate script")
                        .on_click(actions.validate)
                        .content(crate::m3::label("Validate")),
                    crate::m3::text_button(palette)
                        .is_enabled(data.dirty && !data.busy)
                        .automation_name("Discard script changes")
                        .on_click(actions.reload)
                        .content(crate::m3::label("Discard")),
                )),
        ));
        if data.timers.is_empty() {
            below.push(KeyedView::new(
                "no-timer",
                small("To run it, add a timer whose run action names it (rules/*.toml)."),
            ));
        }
        for (index, (timer, callback)) in data.timers.iter().zip(actions.run).enumerate() {
            below.push(KeyedView::new(
                format!("run-{index}"),
                crate::m3::outlined(palette)
                    .horizontal_alignment(HorizontalAlignment::Left)
                    .is_enabled(!data.run_busy && !data.dirty)
                    .automation_name(format!("Run via timer {timer}"))
                    .on_click(callback)
                    .content(crate::m3::label(&format!("Run via timer {timer}"))),
            ));
        }
    }
    // With no entries the list status repeats the empty state above.
    let status = if no_entries { "" } else { data.status };
    for (key, line) in [("run-status", data.run_status), ("status", status)] {
        if !line.is_empty() {
            below.push(KeyedView::new(key, small(line.to_owned())));
        }
    }
    StackPanel::new().spacing(tokens::spacing::MD).children((
        picker,
        StackPanel::new()
            .spacing(tokens::spacing::SM)
            .keyed_children(below),
    ))
}
