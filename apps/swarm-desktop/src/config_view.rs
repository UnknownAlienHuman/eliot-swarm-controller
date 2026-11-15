//! WinUI views for the Profiles and Automation screens.
//!
//! Profiles: one document draft (the `[profiles.*]` source the daemon
//! returned) shown both as a few common form fields for the selected profile
//! and as the full source text; both edit the same text. Validate and Save
//! go to the daemon, which runs its own parser and launch resolution; an
//! invalid draft stays here. Automation is read-only: no daemon operation
//! pauses or resumes a rule or timer.

use windows_reactor::*;

use crate::profiles::{self, FORM_FIELDS};
use crate::theme;
use crate::theme::winui::Palette;
use eliot_core::api::{ProfileSummary, RuleListEntry, TimerSchedule};

fn small(text: impl Into<String>) -> TextBlock {
    TextBlock::new()
        .text(text.into())
        .font_size(theme::type_scale::LABEL_MEDIUM.size)
        .text_wrapping(TextWrapping::Wrap)
        .is_text_selection_enabled(true)
}

fn short(revision: Option<&str>) -> String {
    revision.map_or_else(
        || "none (no config.toml)".to_owned(),
        |revision| revision.chars().take(12).collect(),
    )
}

/// One-line summary of a profile for the launch picker: requested values,
/// with native defaults named when a field is unset.
pub fn summary_line(profile: &ProfileSummary) -> String {
    let mut line = format!(
        "{} · model {} · effort {} · {} · approval {} · control {}",
        profile.harness,
        profile.model.as_deref().unwrap_or("native default"),
        profile.effort.as_deref().unwrap_or("native default"),
        profile
            .permissions
            .as_deref()
            .unwrap_or("default permissions"),
        profile.approval_policy.as_deref().unwrap_or("default"),
        profile.control.as_deref().unwrap_or("self"),
    );
    line.push_str(&match (&profile.proxy, &profile.account) {
        (Some(proxy), Some(account)) => format!(" · route proxy {proxy}, account {account}"),
        (Some(proxy), None) => format!(" · route proxy {proxy}"),
        (None, Some(account)) => format!(" · route direct, account {account}"),
        (None, None) => " · route direct".to_owned(),
    });
    if let Some(error) = &profile.error {
        line.push_str(&format!("\nNot launchable now: {error}"));
    }
    line
}

/// State the Profiles screen renders.
pub struct ProfilesData<'a> {
    pub revision: Option<&'a str>,
    pub file_valid: bool,
    pub file_error: Option<&'a str>,
    pub editable: bool,
    pub reason: Option<&'a str>,
    pub draft: &'a str,
    /// The source TextBox's own text, bound back unchanged.
    pub view: &'a str,
    pub dirty: bool,
    pub selected: Option<&'a str>,
    pub duplicate_name: &'a str,
    pub status: &'a str,
    pub busy: bool,
    pub summaries: &'a [ProfileSummary],
}

/// Callbacks the Profiles screen emits.
pub struct ProfilesActions {
    pub draft_changed: Callback<String>,
    pub select: Callback<Option<usize>>,
    pub field_changed: Vec<Callback<String>>,
    pub duplicate_name_changed: Callback<String>,
    pub duplicate: Callback<()>,
    pub validate: Callback<()>,
    pub save: Callback<()>,
    pub reload: Callback<()>,
}

pub fn profiles_screen(data: ProfilesData<'_>, actions: ProfilesActions, palette: Palette) -> View {
    let names = profiles::profile_names(data.draft);
    let selected_index = data
        .selected
        .and_then(|selected| names.iter().position(|name| name == selected));
    let saved = format!(
        "Saved file: revision {}{}",
        short(data.revision),
        if data.file_valid {
            " · valid".to_owned()
        } else {
            format!(
                " · INVALID: {}. New launches fail until it is fixed.",
                data.file_error.unwrap_or("unparseable")
            )
        }
    );
    let active = "Active: each launch resolves its profile from the saved file at admission, so a saved edit applies to the next launch. Running Agents keep the settings frozen at their launch unless a configure succeeds.";
    let draft_state = if !data.editable {
        format!(
            "Source view unavailable: {}",
            data.reason.unwrap_or("not editable")
        )
    } else if data.dirty {
        "Draft: modified, not saved.".to_owned()
    } else {
        "Draft: same as the saved file.".to_owned()
    };

    let mut fields: Vec<KeyedView> = Vec::new();
    if let Some(name) = data.selected.filter(|_| data.editable) {
        let summary = data.summaries.iter().find(|profile| profile.name == name);
        if let Some(summary) = summary {
            fields.push(KeyedView::new(
                "saved-summary",
                small(format!("Saved: {}", summary_line(summary))).opacity(theme::MUTED_OPACITY),
            ));
        }
        for (index, key) in FORM_FIELDS.iter().enumerate() {
            // Only plain strings round-trip through the form; anything else
            // is edited in the source view so its value cannot change.
            let (value, editable, hint) = match profiles::field(data.draft, name, key) {
                profiles::Field::Absent => (
                    String::new(),
                    true,
                    format!("{key} (unset: native default)"),
                ),
                profiles::Field::Simple(value) => (value, true, String::new()),
                profiles::Field::Complex => (
                    String::new(),
                    false,
                    "complex value: edit it in the source view".to_owned(),
                ),
            };
            let callback = actions.field_changed.get(index).cloned();
            let mut field = crate::m3::field(palette)
                .text(value)
                .placeholder_text(hint)
                .automation_name(format!("Profile {key}"))
                .is_enabled(!data.busy && editable);
            if let Some(callback) = callback.filter(|_| editable) {
                field = field.on_text_changed(callback);
            }
            fields.push(KeyedView::new(
                format!("field-{key}"),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .children((
                        small(*key)
                            .width(120.0)
                            .vertical_alignment(VerticalAlignment::Center),
                        field.width(320.0),
                    )),
            ));
        }
        fields.push(KeyedView::new(
            "duplicate",
            StackPanel::new()
                .orientation(Orientation::Horizontal)
                .spacing(theme::spacing::SM)
                .children((
                    crate::m3::field(palette)
                        .text(data.duplicate_name.to_owned())
                        .placeholder_text("new profile name")
                        .automation_name("Duplicate as")
                        .width(220.0)
                        .on_text_changed(actions.duplicate_name_changed),
                    crate::m3::tonal(palette)
                        .is_enabled(!data.busy)
                        .automation_name("Duplicate profile")
                        .on_click(actions.duplicate)
                        .content(TextBlock::new().text("Duplicate")),
                )),
        ));
    }

    let source = crate::m3::field(palette)
        .grid_row(5)
        .text(data.view.to_owned())
        .accepts_return(true)
        .text_wrapping(TextWrapping::NoWrap)
        .is_enabled(data.editable && !data.busy)
        .automation_name("Profile source")
        .on_text_changed(actions.draft_changed);

    Grid::new()
        .grid_column(1)
        .rows([
            GridLength::Auto,
            GridLength::Auto,
            GridLength::Auto,
            GridLength::Auto,
            GridLength::Auto,
            GridLength::STAR,
            GridLength::Auto,
        ])
        .row_spacing(theme::spacing::SM)
        .children((
            StackPanel::new().grid_row(0).spacing(2.0).children((
                crate::m3::title("Profiles", theme::type_scale::TITLE_LARGE),
                small(saved),
                small(active).opacity(theme::MUTED_OPACITY),
            )),
            StackPanel::new()
                .grid_row(1)
                .orientation(Orientation::Horizontal)
                .spacing(theme::spacing::SM)
                .children((
                    small("Profile")
                        .width(60.0)
                        .vertical_alignment(VerticalAlignment::Center),
                    ComboBox::new()
                        .items_source(names)
                        .selected_index(selected_index)
                        .placeholder_text("Select a profile")
                        .automation_name("Edited profile")
                        .on_selection_changed(actions.select),
                )),
            StackPanel::new()
                .grid_row(2)
                .spacing(theme::spacing::XS)
                .keyed_children(fields),
            small(draft_state).grid_row(3),
            StackPanel::new()
                .grid_row(4)
                .orientation(Orientation::Horizontal)
                .spacing(theme::spacing::SM)
                .children((
                    crate::m3::tonal(palette)
                        .is_enabled(data.editable && !data.busy)
                        .on_click(actions.validate)
                        .content(TextBlock::new().text("Validate")),
                    crate::m3::filled(palette)
                        .is_enabled(data.editable && data.dirty && !data.busy)
                        .on_click(actions.save)
                        .content(TextBlock::new().text("Save")),
                    crate::m3::tonal(palette)
                        .is_enabled(!data.busy)
                        .automation_name("Reload saved profiles")
                        .on_click(actions.reload)
                        .content(TextBlock::new().text("Reload (discard draft)")),
                )),
            source,
            small(data.status.to_owned()).grid_row(6),
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

pub fn automation_screen(
    namespaces: &[RuleNamespace],
    status: &str,
    refresh: Callback<()>,
    palette: Palette,
) -> View {
    let mut rows: Vec<KeyedView> = Vec::new();
    for namespace in namespaces {
        rows.push(KeyedView::new(
            format!("ns-{}", namespace.label),
            TextBlock::new()
                .text(namespace.label.clone())
                .font_weight(FontWeight::SEMI_BOLD),
        ));
        match &namespace.rules {
            Ok(rules) if rules.is_empty() => rows.push(KeyedView::new(
                format!("empty-{}", namespace.label),
                small("No rules or timers in this namespace.").opacity(theme::MUTED_OPACITY),
            )),
            Ok(rules) => {
                for entry in rules {
                    let lines = rule_lines(entry);
                    rows.push(KeyedView::new(
                        format!("{}/{}", namespace.label, entry.state.id),
                        Border::new()
                            .background(palette.surface_container())
                            .border_brush(palette.outline_variant())
                            .border_thickness(1.0)
                            .corner_radius(theme::shape::SM)
                            .padding(Thickness::uniform(theme::spacing::SM))
                            .content(small(lines.join("\n"))),
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
                small(format!("rule.list failed: {error}")).foreground(palette.error()),
            )),
        }
    }
    Grid::new()
        .grid_column(1)
        .rows([GridLength::Auto, GridLength::STAR])
        .row_spacing(theme::spacing::SM)
        .children((
            StackPanel::new().grid_row(0).spacing(2.0).children((
                crate::m3::title("Automation", theme::type_scale::TITLE_LARGE),
                small("Read-only. No daemon operation pauses or resumes a rule or timer: `enabled` comes from the rule file, and `paused` is set by the daemon (backlog). Edit rules in <data_dir>/rules/*.toml; the watcher keeps the last valid definition on a bad edit.").opacity(theme::MUTED_OPACITY),
                StackPanel::new()
                    .orientation(Orientation::Horizontal)
                    .spacing(theme::spacing::SM)
                    .children((
                        crate::m3::tonal(palette)
                            .on_click(refresh)
                            .content(TextBlock::new().text("Refresh")),
                        small(status.to_owned()),
                    )),
            )),
            ScrollViewer::new()
                .grid_row(1)
                .vertical_scroll_bar_visibility(ScrollBarVisibility::Auto)
                .automation_name("Rules and timers")
                .content(
                    StackPanel::new()
                        .spacing(theme::spacing::SM)
                        .max_width(theme::layout::READING_WIDTH)
                        .horizontal_alignment(HorizontalAlignment::Left)
                        .keyed_children(rows),
                ),
        ))
}

/// State the Scripts screen renders.
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

/// Callbacks the Scripts screen emits; `run` has one entry per timer.
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

pub fn scripts_screen(data: ScriptsData<'_>, actions: ScriptsActions, palette: Palette) -> View {
    let selected = data.selected.is_some();
    let meta = match (data.display_path, data.revision) {
        (Some(path), Some(revision)) => format!(
            "{path} · revision {}",
            revision.chars().take(12).collect::<String>()
        ),
        _ => "Select a script entry.".to_owned(),
    };
    let draft_state = if !selected {
        String::new()
    } else if !data.editable {
        format!(
            "Not editable here: {}",
            data.reason.unwrap_or("withheld by the daemon")
        )
    } else if data.dirty {
        "Draft: modified, not saved. Saving never runs the script.".to_owned()
    } else {
        "Draft: same as the saved file.".to_owned()
    };
    let mut runs: Vec<KeyedView> = Vec::new();
    if selected && data.timers.is_empty() {
        runs.push(KeyedView::new(
            "no-timer",
            small("Run: no timer's run action names this script. Add one to <data_dir>/rules/*.toml (for example a once timer) to run it through the daemon's Run path.")
                .opacity(theme::MUTED_OPACITY),
        ));
    }
    for (index, (timer, callback)) in data.timers.iter().zip(actions.run).enumerate() {
        runs.push(KeyedView::new(
            format!("run-{index}"),
            crate::m3::tonal(palette)
                .is_enabled(!data.run_busy && !data.dirty)
                .automation_name(format!("Run via timer {timer}"))
                .on_click(callback)
                .content(TextBlock::new().text(format!("Run via timer {timer}"))),
        ));
    }
    if !data.run_status.is_empty() {
        runs.push(KeyedView::new(
            "run-status",
            small(data.run_status.to_owned()),
        ));
    }
    let unavailable = if data.unavailable.is_empty() {
        String::new()
    } else {
        format!("Unavailable roots: {}", data.unavailable.join("; "))
    };
    Grid::new()
        .grid_column(1)
        .rows([
            GridLength::Auto,
            GridLength::Auto,
            GridLength::Auto,
            GridLength::Auto,
            GridLength::Auto,
            GridLength::STAR,
            GridLength::Auto,
        ])
        .row_spacing(theme::spacing::SM)
        .children((
            StackPanel::new().grid_row(0).spacing(2.0).children((
                crate::m3::title("Scripts", theme::type_scale::TITLE_LARGE),
                small("Existing entries under <data_dir>/scripts/ and registered .eliot/scripts/. Save replaces the file only when its revision still matches; Run goes through a timer's run action, with the daemon's custody, timeout and unknown-outcome rules.").opacity(theme::MUTED_OPACITY),
                small(unavailable).opacity(theme::MUTED_OPACITY),
            )),
            StackPanel::new()
                .grid_row(1)
                .orientation(Orientation::Horizontal)
                .spacing(theme::spacing::SM)
                .children((
                    ComboBox::new()
                        .items_source(data.labels)
                        .selected_index(data.selected)
                        .placeholder_text("Script entry")
                        .automation_name("Script entry")
                        .on_selection_changed(actions.select),
                    crate::m3::tonal(palette)
                        .automation_name("Refresh script list")
                        .on_click(actions.refresh)
                        .content(TextBlock::new().text("Refresh")),
                )),
            small(format!("{meta}\n{draft_state}")).grid_row(2),
            StackPanel::new()
                .grid_row(3)
                .orientation(Orientation::Horizontal)
                .spacing(theme::spacing::SM)
                .children((
                    crate::m3::tonal(palette)
                        .is_enabled(data.editable && !data.busy)
                        .automation_name("Validate script")
                        .on_click(actions.validate)
                        .content(TextBlock::new().text("Validate")),
                    crate::m3::filled(palette)
                        .is_enabled(data.editable && data.dirty && !data.busy)
                        .automation_name("Save script")
                        .on_click(actions.save)
                        .content(TextBlock::new().text("Save")),
                    crate::m3::tonal(palette)
                        .is_enabled(selected && !data.busy)
                        .automation_name("Reload script")
                        .on_click(actions.reload)
                        .content(TextBlock::new().text("Reload (discard draft)")),
                    crate::m3::field(palette)
                        .text(data.editor_exe.to_owned())
                        .placeholder_text("Editor .exe (absolute path)")
                        .automation_name("Editor executable")
                        .width(260.0)
                        .on_text_changed(actions.editor_changed),
                    crate::m3::tonal(palette)
                        .is_enabled(selected && data.display_path.is_some())
                        .automation_name("Open in editor")
                        .on_click(actions.open_editor)
                        .content(TextBlock::new().text("Open in editor")),
                )),
            StackPanel::new()
                .grid_row(4)
                .spacing(theme::spacing::XS)
                .keyed_children(runs),
            crate::m3::field(palette)
                .grid_row(5)
                .text(data.view.to_owned())
                .accepts_return(true)
                .text_wrapping(TextWrapping::NoWrap)
                .is_enabled(data.editable && !data.busy)
                .automation_name("Script source")
                .on_text_changed(actions.draft_changed),
            small(data.status.to_owned()).grid_row(6),
        ))
}
