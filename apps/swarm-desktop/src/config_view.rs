//! WinUI views for the Settings sections Models and profiles, and
//! Automation and scripts.
//!
//! Profiles: one document draft (the `profiles` settings section as
//! `[profiles.*]` TOML) shown both as a few common form fields for the
//! selected profile and, on request, as the full source text; both edit the
//! same text. The Settings save bar validates and saves it through the
//! daemon; an invalid draft stays here. Automation is read-only: no daemon
//! operation pauses or resumes a rule or timer.

use windows_reactor::*;

use crate::profiles::{self, FORM_FIELDS};
use crate::theme;
use crate::theme::winui::Palette;
use eliot_core::api::{ProfileSummary, RuleListEntry, TimerSchedule};

fn small(text: impl Into<String>) -> TextBlock {
    TextBlock::new()
        .text(text.into())
        .font_size(theme::type_scale::BODY_SMALL.size)
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

/// The label column of the profile form.
const FORM_LABEL_WIDTH: f64 = 160.0;

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

/// Models and profiles: one row per profile, the selected profile's form,
/// and the full TOML source on request. Form and source edit one draft.
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
            crate::m3::nav_item(palette, selected)
                .automation_name(format!("Profile {name}: {detail}"))
                .on_click(move || {
                    let _ = select.call(Some(index));
                })
                .content(crate::m3::nav_content(
                    StackPanel::new()
                        .spacing(0.0)
                        .vertical_alignment(VerticalAlignment::Center)
                        .children((
                            crate::m3::text(name.clone(), theme::type_scale::BODY_LARGE),
                            crate::m3::muted(detail, theme::type_scale::BODY_SMALL)
                                .text_trimming(TextTrimming::CharacterEllipsis),
                        )),
                )),
        ));
    }
    if rows.is_empty() {
        rows.push(KeyedView::new(
            "none",
            small(if data.editable {
                "No profiles yet: add one with Edit as TOML."
            } else {
                "Profiles cannot be shown."
            })
            .opacity(theme::MUTED_OPACITY),
        ));
    }

    let mut form: Vec<KeyedView> = Vec::new();
    if let Some(name) = data.selected.filter(|_| data.editable) {
        form.push(KeyedView::new(
            "name",
            crate::m3::text(name.to_owned(), theme::type_scale::TITLE_MEDIUM),
        ));
        if let Some(error) = data
            .summaries
            .iter()
            .find(|profile| profile.name == name)
            .and_then(|profile| profile.error.as_deref())
        {
            form.push(KeyedView::new(
                "error",
                small(format!("Cannot launch now: {error}")).foreground(palette.error()),
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
                // An enumerated field: an exposed dropdown menu. WinUI drops
                // a ComboBox's selection when its items change, so the
                // choices key the control.
                (Some(choices), Some(picked)) => {
                    let selected = choices
                        .iter()
                        .position(|choice| *choice == value)
                        .unwrap_or(0);
                    Grid::new().keyed_children(vec![KeyedView::new(
                        choices.join("\u{1f}"),
                        ComboBox::new()
                            .items_source(choices)
                            .selected_index(selected)
                            .is_enabled(!data.busy)
                            .automation_name(format!("Profile {}", field_label(key)))
                            .horizontal_alignment(HorizontalAlignment::Stretch)
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
                Grid::new()
                    .columns([GridLength::Pixel(FORM_LABEL_WIDTH), GridLength::STAR])
                    .children((
                        crate::m3::text(field_label(key), theme::type_scale::BODY_LARGE)
                            .vertical_alignment(VerticalAlignment::Center),
                        Border::new().grid_column(1).content(field),
                    )),
            ));
        }
        form.push(KeyedView::new(
            "duplicate",
            Grid::new()
                .columns([
                    GridLength::Pixel(FORM_LABEL_WIDTH),
                    GridLength::STAR,
                    GridLength::Auto,
                ])
                .column_spacing(theme::spacing::SM)
                .children((
                    crate::m3::text("Duplicate as", theme::type_scale::BODY_LARGE)
                        .vertical_alignment(VerticalAlignment::Center),
                    crate::m3::field()
                        .grid_column(1)
                        .text(data.duplicate_name.to_owned())
                        .placeholder_text("New profile name")
                        .automation_name("Duplicate as")
                        .on_text_changed(actions.duplicate_name_changed),
                    crate::m3::outlined(palette)
                        .grid_column(2)
                        .is_enabled(!data.busy)
                        .automation_name("Duplicate profile")
                        .on_click(actions.duplicate)
                        .content(TextBlock::new().text("Duplicate")),
                )),
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
    let actions_row = StackPanel::new()
        .orientation(Orientation::Horizontal)
        .spacing(theme::spacing::SM)
        .children((crate::m3::text_button(palette)
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
            })),));
    let source: View = if data.source_open {
        crate::m3::field()
            .text(data.view.to_owned())
            .accepts_return(true)
            .text_wrapping(TextWrapping::NoWrap)
            .is_enabled(data.editable && !data.busy)
            .automation_name("Profile source")
            .min_height(240.0)
            .on_text_changed(actions.draft_changed)
            .into()
    } else {
        View::empty()
    };
    ScrollViewer::new()
        .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
        .content(
            StackPanel::new().spacing(theme::spacing::MD).children((
                crate::m3::card(palette)
                    .padding(Thickness::uniform(theme::spacing::SM))
                    .content(StackPanel::new().keyed_children(rows)),
                crate::m3::card(palette)
                    .content(StackPanel::new().spacing(12.0).keyed_children(form)),
                small(state).opacity(theme::MUTED_OPACITY),
                actions_row,
                source,
                small(data.status.to_owned()),
            )),
        )
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

/// A card's heading row: the title and an optional action on the right.
fn card_heading(title: &str, action: Option<View>) -> View {
    Grid::new()
        .columns([GridLength::STAR, GridLength::Auto])
        .children((
            crate::m3::text(title, theme::type_scale::TITLE_SMALL)
                .automation_heading_level(AutomationHeadingLevel::Level3)
                .vertical_alignment(VerticalAlignment::Center),
            Border::new()
                .grid_column(1)
                .content(action.unwrap_or_else(View::empty)),
        ))
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
                    crate::m3::muted(namespace.label.clone(), theme::type_scale::LABEL_MEDIUM),
                ));
                for entry in rules {
                    rows.push(KeyedView::new(
                        format!("{}/{}", namespace.label, entry.state.id),
                        Border::new()
                            .background(palette.raised())
                            .corner_radius(theme::shape::SM)
                            .padding(Thickness::uniform(theme::spacing::SM))
                            .content(small(rule_lines(entry).join("\n"))),
                    ));
                }
                if namespace.more {
                    rows.push(KeyedView::new(
                        format!("more-{}", namespace.label),
                        small("More rules exist beyond this page.").opacity(theme::MUTED_OPACITY),
                    ));
                }
            }
            Err(error) => rows.push(KeyedView::new(
                format!("error-{}", namespace.label),
                small(format!("{}: not read ({error})", namespace.label))
                    .foreground(palette.error()),
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
            })
            .opacity(theme::MUTED_OPACITY),
        ));
    }
    crate::m3::card(palette)
        .content(
            StackPanel::new().spacing(theme::spacing::SM).children((
                card_heading(
                    "Rules and timers",
                    Some(
                        crate::m3::text_button(palette)
                            .automation_name("Refresh rules")
                            .on_click(refresh)
                            .content(TextBlock::new().text("Refresh")),
                    ),
                ),
                small("Read-only here. Rules live in <data_dir>/rules/*.toml and reload when the file changes.")
                    .opacity(theme::MUTED_OPACITY),
                ScrollViewer::new()
                    .max_height(180.0)
                    .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                    .automation_name("Rules and timers")
                    .content(
                        StackPanel::new()
                            .spacing(theme::spacing::SM)
                            .keyed_children(rows),
                    ),
            )),
        )
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
/// it through a timer that names it.
pub fn scripts_screen(data: ScriptsData<'_>, actions: ScriptsActions, palette: Palette) -> View {
    let selected = data.selected.is_some();
    let refresh = crate::m3::text_button(palette)
        .automation_name("Refresh script list")
        .on_click(actions.refresh)
        .content(TextBlock::new().text("Refresh"));
    let mut parts: Vec<KeyedView> = vec![KeyedView::new(
        "heading",
        card_heading("Scripts", Some(refresh)),
    )];
    let no_entries = data.labels.is_empty();
    if no_entries {
        parts.push(KeyedView::new(
            "none",
            small(
                "No scripts. Add files under <data_dir>/scripts/ or a project's .eliot/scripts/.",
            )
            .opacity(theme::MUTED_OPACITY),
        ));
    } else {
        parts.push(KeyedView::new(
            format!("entries:{}", data.labels.join("\u{1f}")),
            ComboBox::new()
                .items_source(data.labels)
                .selected_index(data.selected)
                .placeholder_text("Choose a script")
                .automation_name("Script entry")
                .min_width(320.0)
                .on_selection_changed(actions.select),
        ));
    }
    if !data.unavailable.is_empty() {
        parts.push(KeyedView::new(
            "unavailable",
            small(format!("Not readable: {}", data.unavailable.join("; ")))
                .opacity(theme::MUTED_OPACITY),
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
        parts.push(KeyedView::new(
            "state",
            small(state).opacity(theme::MUTED_OPACITY),
        ));
        parts.push(KeyedView::new(
            "actions",
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(theme::spacing::SM)
                .children((
                    crate::m3::filled(palette)
                        .is_enabled(data.editable && data.dirty && !data.busy)
                        .automation_name("Save script")
                        .on_click(actions.save)
                        .content(TextBlock::new().text("Save")),
                    crate::m3::text_button(palette)
                        .is_enabled(data.editable && !data.busy)
                        .automation_name("Validate script")
                        .on_click(actions.validate)
                        .content(TextBlock::new().text("Validate")),
                    crate::m3::text_button(palette)
                        .is_enabled(data.dirty && !data.busy)
                        .automation_name("Discard script changes")
                        .on_click(actions.reload)
                        .content(TextBlock::new().text("Discard changes")),
                )),
        ));
        parts.push(KeyedView::new(
            "source",
            crate::m3::field()
                .text(data.view.to_owned())
                .accepts_return(true)
                .text_wrapping(TextWrapping::NoWrap)
                .is_enabled(data.editable && !data.busy)
                .automation_name("Script source")
                .min_height(200.0)
                .on_text_changed(actions.draft_changed),
        ));
        parts.push(KeyedView::new(
            "editor",
            Grid::new()
                .columns([GridLength::STAR, GridLength::Auto])
                .column_spacing(theme::spacing::SM)
                .children((
                    crate::m3::field()
                        .text(data.editor_exe.to_owned())
                        .placeholder_text("Editor .exe (absolute path)")
                        .automation_name("Editor executable")
                        .on_text_changed(actions.editor_changed),
                    crate::m3::text_button(palette)
                        .grid_column(1)
                        .is_enabled(data.display_path.is_some())
                        .automation_name("Open in editor")
                        .on_click(actions.open_editor)
                        .content(TextBlock::new().text("Open in editor")),
                )),
        ));
        if data.timers.is_empty() {
            parts.push(KeyedView::new(
                "no-timer",
                small("To run it, add a timer whose run action names it (rules/*.toml).")
                    .opacity(theme::MUTED_OPACITY),
            ));
        }
        for (index, (timer, callback)) in data.timers.iter().zip(actions.run).enumerate() {
            parts.push(KeyedView::new(
                format!("run-{index}"),
                crate::m3::tonal(palette)
                    .is_enabled(!data.run_busy && !data.dirty)
                    .automation_name(format!("Run via timer {timer}"))
                    .on_click(callback)
                    .content(TextBlock::new().text(format!("Run via timer {timer}"))),
            ));
        }
    }
    // With no entries the list status repeats the empty state above.
    let status = if no_entries { "" } else { data.status };
    for (key, line) in [("run-status", data.run_status), ("status", status)] {
        if !line.is_empty() {
            parts.push(KeyedView::new(key, small(line.to_owned())));
        }
    }
    crate::m3::card(palette).content(
        StackPanel::new()
            .spacing(theme::spacing::SM)
            .keyed_children(parts),
    )
}
