//! WinUI views for the Settings pages Models and profiles and Scripts, and
//! the plain words the Automations cards use for rules and timers.
//!
//! Profiles: one document draft (the `profiles` settings section as
//! `[profiles.*]` TOML) shown both as a few common form fields for the
//! selected profile and, on request, as the full source text; both edit the
//! same text. A form change saves at once through the daemon; typed source
//! keeps the Settings save bar; an invalid draft stays here.

use windows_reactor::*;

use crate::profiles::{self, FormField};
use crate::theme::winui::Palette;
use crate::ui::tokens;
use eliot_core::api::{ProfileCommandIssue, ProfileSummary, RuleListEntry, TimerSchedule};

/// A note: secondary text, wrapped and selectable.
fn small(text: impl Into<String>) -> TextBlock {
    crate::m3::secondary(text)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

/// A failure note: the secondary size at full emphasis in the error color.
fn error_note(text: impl Into<String>, palette: Palette) -> TextBlock {
    crate::m3::text(text, tokens::text::CAPTION)
        .foreground(palette.attention())
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
    pub command_issue: Option<&'a ProfileCommandIssue>,
    /// A saved preset's line: its app, model name and effort.
    pub detail: &'a dyn Fn(&ProfileSummary) -> String,
    /// The TOML source editor is shown.
    pub source_open: bool,
    /// The selected profile's enumerated fields, with its harness's values.
    pub fields: Vec<FormField>,
    /// The selected profile's Model control: a field that opens the
    /// catalogue picker.
    pub model: Option<View>,
    /// The open picker: a layer over the form rows from the Model row down
    /// (the rows stay where they are).
    pub model_popover: Option<View>,
    /// The selected profile's harness in the owner's words ("Claude Code").
    pub harness: Option<&'a str>,
    /// The selected profile's Subagents section.
    pub subagents: Option<SubagentsForm>,
    /// Why the last change was refused: the field key it names (a profile
    /// key, `new` or `duplicate`) and the reason in plain words.
    pub field_error: Option<(&'a str, &'a str)>,
    pub new_name: &'a str,
    /// The new preset's harness: an index in [`crate::picker::HARNESSES`].
    pub new_harness: usize,
}

/// The selected profile's subagent settings.
pub struct SubagentsForm {
    /// The other presets and whether this one may start each
    /// (`child_profiles`); `None` when the value is source-only.
    pub children: Option<Vec<(String, bool)>>,
    /// The harness's own subagents are allowed (`native_subagents`).
    pub native_allowed: bool,
    /// Why the harness's own subagents cannot be turned off, if so.
    pub native_fixed: Option<&'static str>,
    /// The `control` scope as written (`self` when absent); `None` when the
    /// value is source-only.
    pub control: Option<String>,
}

/// Callbacks the Models and profiles section emits.
pub struct ProfilesActions {
    pub draft_changed: Callback<String>,
    pub select: Callback<Option<usize>>,
    pub duplicate_name_changed: Callback<String>,
    pub duplicate: Callback<()>,
    pub toggle_source: Callback<()>,
    /// One per [`ProfilesData::fields`] entry: a dropdown index picked.
    pub choice_picked: Vec<Callback<Option<usize>>>,
    /// One per [`SubagentsForm::children`] entry: checked or unchecked.
    pub child_toggled: Vec<Callback<bool>>,
    pub native_toggled: Callback<bool>,
    pub control_toggled: Callback<bool>,
    pub new_name_changed: Callback<String>,
    pub new_harness_picked: Callback<Option<usize>>,
    pub create: Callback<()>,
}

/// A switch keyed by its state (WinUI keeps a toggled switch's own state
/// otherwise), as General's and Cloud MCP's.
fn switch(key: &str, on: bool, enabled: bool, name: String, toggled: Callback<bool>) -> View {
    Grid::new().keyed_children(vec![KeyedView::new(
        format!("{key}:{on}:{enabled}"),
        ToggleSwitch::new()
            .is_on(on)
            .is_enabled(enabled)
            .automation_name(name)
            .on_toggled(toggled),
    )])
}

/// The Subagents card of the selected profile: the presets it may start
/// (`child_profiles`), its app's own subagents (`native_subagents`) and
/// whether it manages its subagents through Swarm (`control = "tree"`).
fn subagents_card(
    form: SubagentsForm,
    harness: &str,
    error: Option<(&str, &str)>,
    busy: bool,
    actions: &ProfilesActions,
    palette: Palette,
) -> View {
    let error_for = |key: &str| {
        error
            .filter(|(field, _)| *field == key)
            .map(|(_, words)| KeyedView::new(format!("error-{key}"), error_note(words, palette)))
    };
    let mut rows: Vec<KeyedView> = Vec::new();

    let mut start: Vec<KeyedView> = vec![KeyedView::new(
        "title",
        crate::m3::setting_row(
            "Can start",
            "Presets this one may start as Swarm subagents.",
            View::empty(),
        ),
    )];
    match form.children {
        None => start.push(KeyedView::new(
            "complex",
            small("Complex value: edit it as TOML"),
        )),
        Some(children) if children.is_empty() => {
            start.push(KeyedView::new("none", small("No other presets yet.")))
        }
        Some(children) => {
            for ((child, checked), toggled) in children.into_iter().zip(&actions.child_toggled) {
                start.push(KeyedView::new(
                    format!("child:{child}:{checked}"),
                    CheckBox::new()
                        .is_checked(checked)
                        .is_enabled(!busy)
                        .min_width(0.0)
                        .automation_name(format!("Can start {child}"))
                        .on_is_checked_changed(toggled.clone())
                        .content(crate::m3::label(&child)),
                ));
            }
        }
    }
    start.extend(error_for("child_profiles"));
    rows.push(KeyedView::new(
        "children",
        StackPanel::new()
            .margin(Thickness::new(0.0, 0.0, 0.0, tokens::spacing::SM))
            .keyed_children(start),
    ));

    let native_note = match form.native_fixed {
        Some(reason) => format!("Allowed: {reason}."),
        None if form.native_allowed => {
            format!("Allowed: {harness} may start its own subagents too.")
        }
        None => "Off: it delegates only through Swarm subagents.".to_owned(),
    };
    let mut native = vec![KeyedView::new(
        "row",
        crate::m3::setting_row(
            "The agent app's own subagents",
            &native_note,
            switch(
                "native",
                form.native_allowed || form.native_fixed.is_some(),
                !busy && form.native_fixed.is_none(),
                "The agent app's own subagents".to_owned(),
                actions.native_toggled.clone(),
            ),
        ),
    )];
    native.extend(error_for("native_subagents"));
    rows.push(KeyedView::new(
        "native",
        StackPanel::new().keyed_children(native),
    ));

    let control_row: View = match form.control.as_deref() {
        None => crate::m3::setting_row(
            "Manages its subagents through Swarm",
            "Complex value: edit it as TOML",
            View::empty(),
        ),
        Some(control) => crate::m3::setting_row(
            "Manages its subagents through Swarm",
            match control {
                "self" => "Off: its Agents act on themselves only.",
                "project" => {
                    "On, for the whole project (set as TOML): it can steer every Agent of its project."
                }
                _ => "On: its Agents can steer, wait for and stop the subagents they start.",
            },
            switch(
                "control",
                control != "self",
                !busy,
                "Manages its subagents through Swarm".to_owned(),
                actions.control_toggled.clone(),
            ),
        ),
    };
    let mut control = vec![KeyedView::new("row", control_row)];
    control.extend(error_for("control"));
    rows.push(KeyedView::new(
        "control",
        StackPanel::new().keyed_children(control),
    ));
    crate::m3::settings_card(palette, rows)
}

/// Models and profiles: one 32-DIP row per profile, the selected profile's
/// form (a 200-DIP label column; dropdowns sized for their values, text the
/// full width), and the full TOML source on request. Form and source edit
/// one draft.
pub fn profiles_screen(data: ProfilesData<'_>, actions: ProfilesActions, palette: Palette) -> View {
    let names = profiles::profile_names(data.draft);
    // A form row with the reason its last change was refused under it,
    // in one card row (no divider between them).
    let with_error = |key: &str, row: View| -> View {
        match data.field_error.filter(|(field, _)| *field == key) {
            Some((_, words)) => StackPanel::new().children((
                row,
                crate::m3::help(words.to_owned()).foreground(palette.attention()),
            )),
            None => row,
        }
    };
    let subagents = data
        .selected
        .filter(|_| data.editable)
        .zip(data.subagents)
        .map(|(_, form)| {
            subagents_card(
                form,
                data.harness.unwrap_or("The agent app"),
                data.field_error,
                data.busy,
                &actions,
                palette,
            )
        });
    let new_preset = data.editable.then(|| {
        let harnesses: Vec<String> = crate::picker::HARNESSES
            .iter()
            .map(|code| crate::picker::harness_label(code).to_owned())
            .collect();
        let rows = vec![KeyedView::new(
            "new",
            with_error(
                profiles::NEW_PRESET_ROW,
                crate::m3::form_row(
                    "New preset",
                    Grid::new()
                        .columns([GridLength::STAR, GridLength::Auto])
                        .column_spacing(tokens::spacing::SM)
                        .children((
                            crate::m3::field()
                                .text(data.new_name.to_owned())
                                .placeholder_text("New preset name")
                                .automation_name("New preset name")
                                .on_text_changed(actions.new_name_changed.clone()),
                            Grid::new()
                                .grid_column(1)
                                .keyed_children(vec![KeyedView::new(
                                    format!("harness:{}", data.new_harness),
                                    crate::m3::enum_width(ComboBox::new())
                                        .items_source(harnesses)
                                        .selected_index(data.new_harness)
                                        .is_enabled(!data.busy)
                                        .automation_name("New preset agent app")
                                        .on_selection_changed(actions.new_harness_picked.clone()),
                                )]),
                        )),
                    Some(
                        crate::m3::outlined(palette)
                            .is_enabled(!data.busy)
                            .automation_name("Create preset")
                            .on_click(actions.create.clone())
                            .content(crate::m3::label("Create")),
                    ),
                ),
            ),
        )];
        crate::m3::settings_card(palette, rows)
    });
    let mut rows: Vec<KeyedView> = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let selected = data.selected == Some(name.as_str());
        let summary = data.summaries.iter().find(|profile| &profile.name == name);
        let detail = summary.map_or_else(|| "not saved yet".to_owned(), data.detail);
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
                "No presets yet: create one below."
            } else {
                "Presets cannot be shown."
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
        } else if let Some(issue) = data.command_issue {
            form.push(KeyedView::new(
                "error",
                error_note(
                    format!(
                        "Executable is missing at {}. Omit cmd in the profile source to follow the installed version.",
                        issue.configured_cmd
                    ),
                    palette,
                ),
            ));
        }
        if let Some(harness) = data.harness {
            form.push(KeyedView::new(
                "field-harness",
                with_error(
                    "harness",
                    crate::m3::form_row(
                        "Agent app",
                        crate::m3::body(harness.to_owned())
                            .automation_name(format!("Profile agent app: {harness}"))
                            .into(),
                        None,
                    ),
                ),
            ));
        }
        // The rows from Model down: the open picker lies over them.
        let above = form.len();
        if let Some(model) = data.model {
            form.push(KeyedView::new(
                "field-model",
                with_error("model", crate::m3::form_row("Model", model, None)),
            ));
        }
        for (index, field) in data.fields.iter().enumerate() {
            let key = field.key;
            // Only plain strings round-trip through the form; anything else
            // is edited in the source so its value cannot change.
            let control: View = match (
                profiles::field(data.draft, name, key),
                actions.choice_picked.get(index).cloned(),
            ) {
                (profiles::Field::Complex, _) | (_, None) => {
                    small("Complex value: edit it as TOML").into()
                }
                (current, Some(picked)) => {
                    let value = match current {
                        profiles::Field::Simple(value) => Some(value),
                        _ => None,
                    };
                    let choices = profiles::choices(&field.values, value.as_deref());
                    let selected = value
                        .as_deref()
                        .and_then(|value| choices.iter().position(|choice| choice == value))
                        .unwrap_or(0);
                    // A dropdown sized for its values. WinUI drops a
                    // ComboBox's selection when its items change, so the
                    // choices key the control.
                    Grid::new().keyed_children(vec![KeyedView::new(
                        format!("{}\u{1f}{selected}", choices.join("\u{1f}")),
                        crate::m3::enum_width(ComboBox::new())
                            .items_source(choices)
                            .selected_index(selected)
                            .is_enabled(!data.busy)
                            .automation_name(format!("Profile {}", field.label))
                            .on_selection_changed(picked),
                    )])
                }
            };
            form.push(KeyedView::new(
                format!("field-{key}"),
                with_error(key, crate::m3::form_row(field.label, control, None)),
            ));
        }
        form.push(KeyedView::new(
            "duplicate",
            with_error(
                profiles::DUPLICATE_ROW,
                crate::m3::form_row(
                    "Duplicate as",
                    crate::m3::field()
                        .text(data.duplicate_name.to_owned())
                        .placeholder_text("New preset name")
                        .automation_name("Duplicate as")
                        .on_text_changed(actions.duplicate_name_changed)
                        .into(),
                    Some(
                        crate::m3::outlined(palette)
                            .is_enabled(!data.busy)
                            .automation_name("Duplicate preset")
                            .on_click(actions.duplicate)
                            .content(crate::m3::label("Duplicate")),
                    ),
                ),
            ),
        ));
        if let Some(popover) = data.model_popover {
            let below = form.split_off(above);
            form.push(KeyedView::new(
                "fields-with-picker",
                Grid::new().children((StackPanel::new().keyed_children(below), popover)),
            ));
        }
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
    let source: Option<View> = data.source_open.then(|| {
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
    });
    // Only shown parts take a place, so no empty child adds a gap.
    let mut parts = vec![KeyedView::new("profiles", crate::m3::list(palette, rows))];
    if !form.is_empty() {
        parts.push(KeyedView::new(
            "form",
            crate::m3::settings_card(palette, form),
        ));
    }
    if let Some(subagents) = subagents {
        parts.push(KeyedView::new(
            "subagents-title",
            crate::m3::card_title("Subagents"),
        ));
        parts.push(KeyedView::new("subagents", subagents));
    }
    if let Some(new_preset) = new_preset {
        parts.push(KeyedView::new("new-preset", new_preset));
    }
    parts.push(KeyedView::new("state", small(state)));
    parts.push(KeyedView::new("toggle", toggle));
    if let Some(source) = source {
        parts.push(KeyedView::new("source", source));
    }
    if !data.status.is_empty() {
        parts.push(KeyedView::new("status", small(data.status.to_owned())));
    }
    StackPanel::new()
        .spacing(tokens::spacing::MD)
        .keyed_children(parts)
}

/// A daemon time (epoch ms) on this computer's clock: "00:06:45" for a
/// time today, "2026-11-18 02:00" for another day.
pub fn local_time(ms: i64) -> String {
    local_time_on(ms, chrono::Local::now().date_naive())
}

fn local_time_on(ms: i64, today: chrono::NaiveDate) -> String {
    use chrono::TimeZone;
    match chrono::Local.timestamp_millis_opt(ms).single() {
        Some(at) if at.date_naive() == today => at.format("%H:%M:%S").to_string(),
        Some(at) => at.format("%Y-%m-%d %H:%M").to_string(),
        None => "an unreadable time".to_owned(),
    }
}

fn compact(value: &serde_json::Value) -> String {
    let text = value.to_string();
    if text.chars().count() > 200 {
        format!("{}…", text.chars().take(200).collect::<String>())
    } else {
        text
    }
}

/// An interval in words: "10 min", "30 s", "2 h".
pub fn duration_words(ms: i64) -> String {
    let seconds = ms / 1000;
    if seconds >= 3600 && seconds % 3600 == 0 {
        format!("{} h", seconds / 3600)
    } else if seconds >= 60 && seconds % 60 == 0 {
        format!("{} min", seconds / 60)
    } else {
        format!("{seconds} s")
    }
}

fn text_field<'a>(object: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    object.get(key).and_then(serde_json::Value::as_str)
}

/// A value in words: strings as written (paths without the `\\?\` prefix),
/// anything else compact.
fn value_words(value: &serde_json::Value) -> String {
    match value.as_str() {
        Some(text) => crate::nav::display_path(text).to_owned(),
        None => compact(value),
    }
}

/// An action's target: the Agent of the triggering event, or a named one.
fn target_words(action: &serde_json::Value) -> String {
    match action.get("target").map(value_words) {
        Some(target) if target == "${event.agent}" => "that Agent".to_owned(),
        Some(target) => format!("Agent {target}"),
        None => "its Agent".to_owned(),
    }
}

/// Literal text an action sends, shortened.
fn quoted(action: &serde_json::Value) -> String {
    match text_field(action, "text") {
        Some(text) if text.chars().count() > 60 => {
            format!(" “{}…”", text.chars().take(60).collect::<String>())
        }
        Some(text) => format!(" “{text}”"),
        None => String::new(),
    }
}

/// One rule action in words.
fn action_words(action: &serde_json::Value) -> String {
    let kind = text_field(action, "action").unwrap_or("an action without a name");
    let workspace = action
        .get("workspace")
        .map(|value| format!(" in {}", value_words(value)))
        .unwrap_or_default();
    match kind {
        "launch" => match action.get("assignment") {
            Some(assignment) => format!(
                "launch a {} with the next brief from {}{workspace}, keeping {} busy",
                text_field(action, "role").unwrap_or("default-role Agent"),
                assignment
                    .get("folder")
                    .map_or_else(|| "its folder".to_owned(), value_words),
                assignment
                    .get("max_busy")
                    .map_or_else(|| "?".to_owned(), value_words),
            ),
            None => {
                let how = match (text_field(action, "profile"), text_field(action, "role")) {
                    (Some(profile), _) => format!(" with profile {profile}"),
                    (None, Some(role)) => format!(" on the {role} role"),
                    (None, None) => " through routing".to_owned(),
                };
                format!("launch an Agent{how}{workspace}{}", quoted(action))
            }
        },
        "stop" => format!("stop {}", target_words(action)),
        "send" => format!("send{} to {}", quoted(action), target_words(action)),
        "resume" => format!("resume {}{}", target_words(action), quoted(action)),
        "interrupt" => format!("interrupt {}", target_words(action)),
        "steer" => format!("steer {}{}", target_words(action), quoted(action)),
        "answer" => format!("answer the pending request of {}", target_words(action)),
        "run" => {
            let command = action
                .get("cmd")
                .and_then(serde_json::Value::as_array)
                .map(|parts| parts.iter().map(value_words).collect::<Vec<_>>().join(" "))
                .unwrap_or_else(|| "a script".to_owned());
            let cwd = action
                .get("cwd")
                .map(|value| format!(" in {}", value_words(value)))
                .unwrap_or_default();
            let timeout = text_field(action, "timeout")
                .map(|timeout| format!(" (timeout {timeout})"))
                .unwrap_or_default();
            format!("run {command}{cwd}{timeout}")
        }
        "var.set" => format!(
            "set the {} variable {} to {}",
            text_field(action, "scope").unwrap_or("project"),
            text_field(action, "key").unwrap_or("?"),
            action
                .get("value")
                .map(|value| match value.as_str() {
                    Some(text) => format!("“{text}”"),
                    None => compact(value),
                })
                .unwrap_or_default(),
        ),
        "mail.send" | "msg.send" => format!(
            "send mail{} to {}",
            quoted(action),
            action
                .get("to")
                .and_then(serde_json::Value::as_array)
                .map(|to| to.iter().map(value_words).collect::<Vec<_>>().join(", "))
                .unwrap_or_else(|| "its recipients".to_owned()),
        ),
        "git.fetch" => format!(
            "fetch {} from {} of {}",
            text_field(action, "source_ref").unwrap_or("the default ref"),
            text_field(action, "remote").unwrap_or("its remote"),
            text_field(action, "repository").unwrap_or("its repository"),
        ),
        other => {
            let mut rest = action.clone();
            if let Some(object) = rest.as_object_mut() {
                object.remove("action");
            }
            format!("{other} {}", compact(&rest))
        }
    }
}

/// A persisted event kind in words.
fn event_words(kind: &str) -> String {
    match kind {
        "turn.completed" => "a turn completes".to_owned(),
        "turn.failed" => "a turn fails".to_owned(),
        "turn.started" => "a turn starts".to_owned(),
        "agent.failed" => "an Agent fails".to_owned(),
        "agent.lost" => "an Agent is lost".to_owned(),
        "agent.stopped" => "an Agent stops".to_owned(),
        "request.opened" => "an Agent asks for input".to_owned(),
        other => format!("the event {other} happens"),
    }
}

/// One match condition in words.
fn condition_words(path: &str, condition: &serde_json::Value) -> String {
    if path == "event.cause.rule"
        && let Some(rule) = condition.as_str()
    {
        return format!("for an Agent started by rule {rule}");
    }
    let test = match condition {
        serde_json::Value::String(text) if text.starts_with("glob:") => {
            format!("matches {}", &text["glob:".len()..])
        }
        serde_json::Value::String(text) if text.starts_with("re:") => {
            format!("matches the pattern {}", &text["re:".len()..])
        }
        serde_json::Value::Object(object) if object.contains_key("one_of") => format!(
            "is one of {}",
            object["one_of"]
                .as_array()
                .map(|choices| choices
                    .iter()
                    .map(value_words)
                    .collect::<Vec<_>>()
                    .join(", "))
                .unwrap_or_default()
        ),
        serde_json::Value::Object(object) if object.contains_key("exists") => {
            if object["exists"] == serde_json::Value::Bool(true) {
                "is set".to_owned()
            } else {
                "is not set".to_owned()
            }
        }
        other => format!("is {}", value_words(other)),
    };
    format!("where {path} {test}")
}

/// When a rule fires, in words, from its timer state or its definition.
fn trigger_words(entry: &RuleListEntry) -> String {
    let on = entry.definition.get("on");
    if let Some(timer) = &entry.timer {
        let schedule = match &timer.schedule {
            TimerSchedule::Interval { period_ms, .. } => {
                format!("every {}", duration_words(*period_ms))
            }
            TimerSchedule::Cron { expression, tz } => {
                format!("on the schedule {expression} ({tz})")
            }
            TimerSchedule::Once { at } => format!("once at {}", local_time(*at)),
        };
        return schedule;
    }
    match on {
        Some(on) if on.get("event").is_some() => {
            let mut words = format!(
                "when {}",
                event_words(text_field(on, "event").unwrap_or("an event"))
            );
            if let Some(conditions) = on.get("match").and_then(serde_json::Value::as_object) {
                for (path, condition) in conditions {
                    words.push(' ');
                    words.push_str(&condition_words(path, condition));
                }
            }
            words
        }
        Some(on) if on.get("every").is_some() => {
            format!(
                "every {}",
                on.get("every").map(value_words).unwrap_or_default()
            )
        }
        Some(on) if on.get("cron").is_some() => format!(
            "on the schedule {}",
            on.get("cron").map(value_words).unwrap_or_default()
        ),
        Some(on) if on.get("at").is_some() => {
            format!(
                "once at {}",
                on.get("at").map(value_words).unwrap_or_default()
            )
        }
        Some(on) => format!("on {}", compact(on)),
        None => "with no trigger set".to_owned(),
    }
}

/// A rule's firing limit in words.
fn limit_words(limit: &serde_json::Value) -> String {
    match (
        limit.get("max").and_then(serde_json::Value::as_i64),
        text_field(limit, "per"),
        text_field(limit, "window"),
    ) {
        (Some(max), per, window) => {
            let per = match per {
                Some("agent") => " per Agent".to_owned(),
                Some(other) => format!(" per {other}"),
                None => String::new(),
            };
            // The daemon's canonical definition normalizes `window` to
            // `window_ms` (`automation::definition`).
            let window = window
                .map(str::to_owned)
                .or_else(|| {
                    limit
                        .get("window_ms")
                        .and_then(serde_json::Value::as_i64)
                        .map(duration_words)
                })
                .map(|window| format!(" within {window}"))
                .unwrap_or_default();
            format!(
                "At most {}{per}{window}.",
                eliot_core::count_words(max.max(0) as u64, "firing", "firings")
            )
        }
        _ => format!("Limit {}.", compact(limit)),
    }
}

fn capitalized(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// What one rule does when, in words (its state and times are the card's).
#[derive(Clone, Debug, PartialEq)]
pub struct RuleText {
    /// "Every 10 min: launch …."
    pub sentence: String,
    pub limit: Option<String>,
}

pub fn rule_text(entry: &RuleListEntry) -> RuleText {
    let actions: Vec<String> = entry
        .definition
        .get("do")
        .and_then(serde_json::Value::as_array)
        .map(|actions| actions.iter().map(action_words).collect())
        .unwrap_or_default();
    let mut sentence = format!(
        "{}: {}",
        capitalized(&trigger_words(entry)),
        if actions.is_empty() {
            "nothing".to_owned()
        } else {
            actions.join("; then ")
        }
    );
    sentence.push('.');
    RuleText {
        sentence,
        limit: entry.definition.get("limit").map(limit_words),
    }
}

/// One rule namespace read for the Automations page.
#[derive(Clone)]
pub struct RuleNamespace {
    /// Registered project id; absent for the global namespace.
    pub project: Option<String>,
    pub label: String,
    pub rules: Result<Vec<RuleListEntry>, String>,
    pub more: bool,
}

// State the Scripts card renders.
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
    /// Why `timers` may be incomplete (unreadable or partial rule reads).
    pub rule_issues: &'a [String],
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
                small("No scripts yet. Put them in the scripts folder of Swarm's data folder, or in a project's .eliot\\scripts folder.")
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
    let mut page = vec![KeyedView::new(
        "picker",
        crate::m3::settings_card(palette, parts),
    )];
    let mut below: Vec<KeyedView> = Vec::new();
    if selected {
        // The source is a group of its own, 16 DIPs from the picker and
        // from the actions under it.
        page.push(KeyedView::new(
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
        for (index, issue) in data.rule_issues.iter().enumerate() {
            below.push(KeyedView::new(
                format!("rule-issue-{index}"),
                error_note(issue.clone(), palette),
            ));
        }
        if data.timers.is_empty() && data.rule_issues.is_empty() {
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
    if !below.is_empty() {
        page.push(KeyedView::new(
            "below",
            StackPanel::new()
                .spacing(tokens::spacing::SM)
                .keyed_children(below),
        ));
    }
    StackPanel::new()
        .spacing(tokens::spacing::MD)
        .keyed_children(page)
}
