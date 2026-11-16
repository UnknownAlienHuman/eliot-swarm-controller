//! Pure edits of the unified settings sections the Settings page drafts.
//!
//! Each draft is the JSON value `settings.read` returned for one section
//! (`routing`, `projects`, `automation`, `cloud_mcp`), changed in place and
//! sent back whole by `settings.save`, which is a complete replacement of the
//! named section. Fields the page does not show (a project's Git remotes or
//! interpreters, the automation backlog limit and Run grant) ride along
//! unchanged. Nothing here decides validity: the daemon validates every save,
//! dry run first when asked. Wire shapes: [`eliot_core::api::config_views`].

use eliot_core::api::{SettingsHistoryEntry, SettingsHistoryStatus, SettingsSaveResponse};
use serde_json::{Map, Value, json};

/// The routing roles every launch path knows, shown even when unset.
pub const ROLES: [&str; 4] = ["default", "manager", "worker", "audit"];

/// The Cloud MCP fields, in form order, with their labels.
pub const CLOUD_FIELDS: [(&str, &str); 6] = [
    ("cloudflared_executable", "cloudflared executable"),
    ("tunnel_id", "Tunnel id"),
    ("credentials_file", "Tunnel credentials file"),
    ("hostname", "Public hostname"),
    ("access_team_name", "Access team name"),
    ("access_audience_tag", "Access audience tag"),
];

/// The editable fields of one assignment, besides its role and capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssignmentField {
    Folder,
    Workspace,
    Interval,
}

impl AssignmentField {
    fn key(self) -> &'static str {
        match self {
            Self::Folder => "folder",
            Self::Workspace => "workspace",
            Self::Interval => "interval",
        }
    }
}

fn object(value: &mut Value) -> &mut Map<String, Value> {
    if !value.is_object() {
        *value = Value::Object(Map::new());
    }
    value.as_object_mut().expect("just made an object")
}

fn strings(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// A name the daemon accepts for a role, project or assignment key.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

/// The roles to show: the four standard roles, then any others the draft has.
pub fn routing_roles(draft: &Value) -> Vec<String> {
    let mut roles: Vec<String> = ROLES.iter().map(|role| (*role).to_owned()).collect();
    if let Some(map) = draft.as_object() {
        roles.extend(
            map.keys()
                .filter(|key| !ROLES.contains(&key.as_str()))
                .cloned(),
        );
    }
    roles
}

/// The ordered candidate profiles of `role`.
pub fn candidates(draft: &Value, role: &str) -> Vec<String> {
    strings(draft.get(role))
}

fn set_candidates(draft: &mut Value, role: &str, list: Vec<String>) {
    let map = object(draft);
    if list.is_empty() && role != "default" {
        // An unset role is a routing miss, like an empty one; leave it out.
        map.remove(role);
    } else {
        map.insert(role.to_owned(), json!(list));
    }
}

/// Append `profile` to `role` (no duplicates).
pub fn routing_add(draft: &mut Value, role: &str, profile: &str) {
    let mut list = candidates(draft, role);
    if !list.iter().any(|name| name == profile) {
        list.push(profile.to_owned());
        set_candidates(draft, role, list);
    }
}

/// Remove the candidate at `index` of `role`.
pub fn routing_remove(draft: &mut Value, role: &str, index: usize) {
    let mut list = candidates(draft, role);
    if index < list.len() {
        list.remove(index);
        set_candidates(draft, role, list);
    }
}

/// Move the candidate at `index` one place earlier (`up`) or later.
pub fn routing_move(draft: &mut Value, role: &str, index: usize, up: bool) {
    let mut list = candidates(draft, role);
    let other = if up {
        index.checked_sub(1)
    } else {
        Some(index + 1)
    };
    if let Some(other) = other.filter(|other| *other < list.len() && index < list.len()) {
        list.swap(index, other);
        set_candidates(draft, role, list);
    }
}

/// Add an empty extra role; it is saved once it has a candidate.
pub fn routing_add_role(draft: &mut Value, role: &str) -> Result<(), String> {
    if !valid_name(role) {
        return Err("Role names use letters, digits, - and _.".to_owned());
    }
    if routing_roles(draft).iter().any(|known| known == role) {
        return Err(format!("{role} is already listed."));
    }
    object(draft).insert(role.to_owned(), json!([]));
    Ok(())
}

/// Roles with no candidates are left out of a save (except `default`).
pub fn routing_for_save(draft: &Value) -> Value {
    let mut clean = draft.clone();
    let map = object(&mut clean);
    map.retain(|role, list| role == "default" || list.as_array().is_none_or(|l| !l.is_empty()));
    map.entry("default").or_insert_with(|| json!([]));
    clean
}

/// Project ids in draft order.
pub fn project_ids(draft: &Value) -> Vec<String> {
    draft
        .as_object()
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default()
}

/// The checkout path of a project.
pub fn project_path(draft: &Value, id: &str) -> String {
    draft
        .get(id)
        .and_then(|project| project.get("path"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// The profile allowlist of a project.
pub fn project_profiles(draft: &Value, id: &str) -> Vec<String> {
    strings(draft.get(id).and_then(|project| project.get("profiles")))
}

/// The approved actions of a project, comma separated.
pub fn project_actions(draft: &Value, id: &str) -> String {
    strings(draft.get(id).and_then(|project| project.get("actions"))).join(", ")
}

fn project_mut<'a>(draft: &'a mut Value, id: &str) -> Option<&'a mut Map<String, Value>> {
    draft.get_mut(id).and_then(Value::as_object_mut)
}

pub fn set_project_path(draft: &mut Value, id: &str, path: &str) {
    if let Some(project) = project_mut(draft, id) {
        project.insert("path".to_owned(), json!(path.trim()));
    }
}

/// Allow (or stop allowing) `profile` in a project; the order of the
/// remaining entries is kept.
pub fn set_project_profile(draft: &mut Value, id: &str, profile: &str, allowed: bool) {
    let mut list = project_profiles(draft, id);
    list.retain(|name| name != profile);
    if allowed {
        list.push(profile.to_owned());
    }
    if let Some(project) = project_mut(draft, id) {
        project.insert("profiles".to_owned(), json!(list));
    }
}

/// Set the actions from comma- or space-separated text.
pub fn set_project_actions(draft: &mut Value, id: &str, text: &str) {
    let actions: Vec<&str> = text
        .split([',', ' ', '\n'])
        .map(str::trim)
        .filter(|action| !action.is_empty())
        .collect();
    if let Some(project) = project_mut(draft, id) {
        project.insert("actions".to_owned(), json!(actions));
    }
}

/// Add a project allowing every listed profile; the daemon resolves its Git
/// identity when the draft is validated or saved.
pub fn add_project(
    draft: &mut Value,
    id: &str,
    path: &str,
    profiles: &[String],
) -> Result<(), String> {
    if !valid_name(id) {
        return Err("Project ids use letters, digits, - and _.".to_owned());
    }
    if path.trim().is_empty() {
        return Err("Give the project's checkout folder.".to_owned());
    }
    if draft.get(id).is_some() {
        return Err(format!("{id} already exists."));
    }
    object(draft).insert(
        id.to_owned(),
        json!({"path": path.trim(), "profiles": profiles, "actions": []}),
    );
    Ok(())
}

pub fn remove_entry(draft: &mut Value, key: &str) {
    object(draft).remove(key);
}

/// Assignment keys of the automation draft, in order.
pub fn assignment_keys(automation: &Value) -> Vec<String> {
    automation
        .get("assignments")
        .and_then(Value::as_object)
        .map(|map| map.keys().cloned().collect())
        .unwrap_or_default()
}

fn assignment<'a>(automation: &'a Value, key: &str) -> Option<&'a Value> {
    automation.get("assignments")?.get(key)
}

fn assignment_mut<'a>(automation: &'a mut Value, key: &str) -> Option<&'a mut Map<String, Value>> {
    automation
        .get_mut("assignments")?
        .get_mut(key)?
        .as_object_mut()
}

/// A text field of an assignment.
pub fn assignment_text(automation: &Value, key: &str, field: AssignmentField) -> String {
    assignment(automation, key)
        .and_then(|entry| entry.get(field.key()))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// The role an assignment launches with (the daemon's default is `worker`).
pub fn assignment_role(automation: &Value, key: &str) -> String {
    assignment(automation, key)
        .and_then(|entry| entry.get("role"))
        .and_then(Value::as_str)
        .unwrap_or("worker")
        .to_owned()
}

/// How many Agents an assignment keeps busy.
pub fn assignment_busy(automation: &Value, key: &str) -> Option<u64> {
    assignment(automation, key)?.get("max_busy")?.as_u64()
}

pub fn set_assignment_text(automation: &mut Value, key: &str, field: AssignmentField, text: &str) {
    if let Some(entry) = assignment_mut(automation, key) {
        entry.insert(field.key().to_owned(), json!(text.trim()));
    }
}

pub fn set_assignment_role(automation: &mut Value, key: &str, role: &str) {
    if let Some(entry) = assignment_mut(automation, key) {
        entry.insert("role".to_owned(), json!(role));
    }
}

pub fn set_assignment_busy(automation: &mut Value, key: &str, busy: u64) {
    if let Some(entry) = assignment_mut(automation, key) {
        entry.insert("max_busy".to_owned(), json!(busy));
    }
}

/// A new "keep 1 busy" assignment launching `worker` Agents every 30 s.
pub fn add_assignment(automation: &mut Value, key: &str, workspace: &str) -> Result<(), String> {
    if !valid_name(key) {
        return Err("Assignment names use letters, digits, - and _.".to_owned());
    }
    if assignment(automation, key).is_some() {
        return Err(format!("{key} already exists."));
    }
    let assignments = object(automation)
        .entry("assignments")
        .or_insert_with(|| Value::Object(Map::new()));
    object(assignments).insert(
        key.to_owned(),
        json!({"folder": "", "workspace": workspace, "role": "worker", "max_busy": 1, "interval": "30s"}),
    );
    Ok(())
}

pub fn remove_assignment(automation: &mut Value, key: &str) {
    if let Some(assignments) = automation
        .get_mut("assignments")
        .and_then(Value::as_object_mut)
    {
        assignments.remove(key);
    }
}

/// A Cloud MCP field's text.
pub fn cloud_field(draft: &Value, key: &str) -> String {
    draft
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

pub fn set_cloud_field(draft: &mut Value, key: &str, text: &str) {
    object(draft).insert(key.to_owned(), json!(text.trim()));
}

/// Cloud MCP is configured (an empty section removes it).
pub fn cloud_enabled(draft: &Value) -> bool {
    draft.as_object().is_some_and(|map| !map.is_empty())
}

/// An empty form for every Cloud MCP field.
pub fn cloud_template() -> Value {
    Value::Object(
        CLOUD_FIELDS
            .iter()
            .map(|(key, _)| ((*key).to_owned(), json!("")))
            .collect(),
    )
}

/// The `[desktop]` section with the given preferences; other fields kept.
pub fn desktop_section(
    current: Option<&Value>,
    theme: &str,
    startup: &str,
    window: Option<(u32, u32)>,
) -> Value {
    let mut desktop = current.cloned().unwrap_or_else(|| json!({}));
    let map = object(&mut desktop);
    map.insert("theme".to_owned(), json!(theme));
    map.insert("startup".to_owned(), json!(startup));
    if let Some((width, height)) = window {
        map.insert(
            "window".to_owned(),
            json!({"width": width, "height": height}),
        );
    }
    desktop
}

/// The `[profiles.*]` tables of the `profiles` section as TOML text, for the
/// form and source editor.
pub fn profiles_toml(section: &Value) -> Result<String, String> {
    if section.as_object().is_none_or(Map::is_empty) {
        return Ok(String::new());
    }
    let table = toml::Value::try_from(json!({ "profiles": section }))
        .map_err(|_| "the profiles section cannot be shown as TOML".to_owned())?;
    toml::to_string(&table).map_err(|_| "the profiles section cannot be shown as TOML".to_owned())
}

/// The `profiles` section from the editor's TOML text: only `[profiles.*]`
/// tables belong there.
pub fn profiles_from_toml(text: &str) -> Result<Value, String> {
    let table: toml::Table =
        toml::from_str(text).map_err(|error| format!("Not valid TOML: {}", error.message()))?;
    if let Some(other) = table.keys().find(|key| key.as_str() != "profiles") {
        return Err(format!(
            "Only [profiles.*] tables belong here; [{other}] has its own section."
        ));
    }
    match table.get("profiles") {
        None => Ok(json!({})),
        Some(profiles) if profiles.is_table() => serde_json::to_value(profiles)
            .map_err(|_| "the profiles cannot be read as settings".to_owned()),
        Some(_) => Err("profiles must be tables ([profiles.NAME]).".to_owned()),
    }
}

fn short(revision: &str) -> &str {
    &revision[..revision.len().min(12)]
}

/// What one Validate or Save reported, in one line.
pub fn save_note(dry_run: bool, result: &Result<SettingsSaveResponse, String>) -> String {
    let restart = |sections: &[eliot_core::api::SettingsSection]| {
        if sections.is_empty() {
            String::new()
        } else {
            format!(
                " Restart the daemon to apply {}.",
                sections
                    .iter()
                    .map(|section| section_label(*section))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    };
    match result {
        Ok(reply) if dry_run => format!(
            "Valid; nothing saved.{}",
            restart(&reply.restart_required)
        ),
        Ok(reply) if reply.saved => format!(
            "Saved: revision {}{}.{}",
            reply.revision.as_deref().map_or("?", short),
            reply
                .history_revision
                .map_or_else(String::new, |entry| format!(", history #{entry}")),
            restart(&reply.restart_required)
        ),
        Ok(reply) => format!(
            "No change; revision {} stays.",
            reply.revision.as_deref().map_or("?", short)
        ),
        Err(error) if error.starts_with("conflict") => {
            "Not saved: the settings changed elsewhere. Your draft is kept; Reload to see the current values.".to_owned()
        }
        Err(error) if crate::state::uncertain(error) => format!(
            "Outcome unknown; Reload and check the revision before saving again. ({error})"
        ),
        Err(error) if dry_run => format!("Invalid: {error}"),
        Err(error) => format!("Not saved: {error}"),
    }
}

/// A settings section's name in the page's words.
pub fn section_label(section: eliot_core::api::SettingsSection) -> &'static str {
    use eliot_core::api::SettingsSection as S;
    match section {
        S::Profiles => "profiles",
        S::Routing => "routing",
        S::Projects => "projects",
        S::Daemon => "daemon",
        S::Automation => "automation",
        S::Mail => "mail",
        S::CloudMcp => "Cloud MCP",
        S::Desktop => "desktop",
        S::Accounts => "accounts",
        S::Proxies => "proxies",
    }
}

/// Who made a settings change, in words.
pub fn author_label(author: &str) -> String {
    match author {
        "owner:desktop" => "You, in the desktop".to_owned(),
        "owner:cli" => "You, with the CLI".to_owned(),
        "bootstrap" => "Swarm, at first start".to_owned(),
        "project_discovery" => "Swarm, registering an opened project".to_owned(),
        other => other.to_owned(),
    }
}

/// One history entry as a headline and a supporting line.
pub fn history_lines(entry: &SettingsHistoryEntry) -> (String, String) {
    let sections = entry
        .changed_sections
        .iter()
        .map(|section| section_label(*section))
        .collect::<Vec<_>>()
        .join(", ");
    let status = match entry.status {
        SettingsHistoryStatus::Applied => "applied",
        SettingsHistoryStatus::Pending => "pending",
        SettingsHistoryStatus::NotApplied => "not applied",
        SettingsHistoryStatus::Unknown => "outcome unknown",
    };
    (
        format!("{} · {sections}", author_label(&entry.author)),
        format!(
            "{} · #{} · revision {} · {status}",
            utc_minute(entry.created_at),
            entry.revision,
            short(&entry.current_revision)
        ),
    )
}

/// `YYYY-MM-DD HH:MM UTC` of epoch milliseconds.
pub fn utc_minute(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let minutes = ms.rem_euclid(86_400_000) / 60_000;
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02} UTC",
        minutes / 60,
        minutes % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_edits_keep_order_and_drop_empty_roles() {
        let mut draft = json!({"default": ["a"]});
        routing_add(&mut draft, "worker", "a");
        routing_add(&mut draft, "worker", "b");
        routing_add(&mut draft, "worker", "a");
        assert_eq!(candidates(&draft, "worker"), ["a", "b"]);
        routing_move(&mut draft, "worker", 1, true);
        assert_eq!(candidates(&draft, "worker"), ["b", "a"]);
        routing_move(&mut draft, "worker", 1, false);
        assert_eq!(candidates(&draft, "worker"), ["b", "a"]);
        routing_remove(&mut draft, "worker", 0);
        routing_remove(&mut draft, "worker", 0);
        assert!(draft.get("worker").is_none());
        routing_remove(&mut draft, "default", 0);
        assert_eq!(draft["default"], json!([]));
        assert!(routing_add_role(&mut draft, "review").is_ok());
        assert!(routing_add_role(&mut draft, "worker").is_err());
        assert!(routing_add_role(&mut draft, "bad role").is_err());
        assert_eq!(
            routing_roles(&draft),
            ["default", "manager", "worker", "audit", "review"]
        );
        assert_eq!(routing_for_save(&draft), json!({"default": []}));
    }

    #[test]
    fn project_edits_keep_unshown_fields() {
        let mut draft = json!({"app": {"path": "C:/a", "profiles": ["x"], "git": {"credential_helper": "manager"}}});
        set_project_profile(&mut draft, "app", "y", true);
        set_project_profile(&mut draft, "app", "x", false);
        set_project_actions(&mut draft, "app", "send, stop  git.fetch");
        set_project_path(&mut draft, "app", " C:/b ");
        assert_eq!(
            draft["app"],
            json!({"path": "C:/b", "profiles": ["y"], "actions": ["send", "stop", "git.fetch"], "git": {"credential_helper": "manager"}})
        );
        assert_eq!(project_actions(&draft, "app"), "send, stop, git.fetch");
        assert!(add_project(&mut draft, "app", "C:/c", &[]).is_err());
        assert!(add_project(&mut draft, "web", "", &[]).is_err());
        add_project(&mut draft, "web", "C:/w", &["y".to_owned()]).unwrap();
        assert_eq!(project_profiles(&draft, "web"), ["y"]);
        remove_entry(&mut draft, "app");
        assert_eq!(project_ids(&draft), ["web"]);
    }

    #[test]
    fn assignments_live_inside_the_automation_section() {
        let mut automation = json!({"max_event_backlog": 100000, "run": {"run_env": []}});
        add_assignment(&mut automation, "backlog", "C:/w").unwrap();
        assert!(add_assignment(&mut automation, "backlog", "C:/w").is_err());
        set_assignment_text(
            &mut automation,
            "backlog",
            AssignmentField::Folder,
            "C:/briefs",
        );
        set_assignment_busy(&mut automation, "backlog", 2);
        set_assignment_role(&mut automation, "backlog", "audit");
        assert_eq!(assignment_keys(&automation), ["backlog"]);
        assert_eq!(
            assignment_text(&automation, "backlog", AssignmentField::Folder),
            "C:/briefs"
        );
        assert_eq!(assignment_busy(&automation, "backlog"), Some(2));
        assert_eq!(assignment_role(&automation, "backlog"), "audit");
        assert_eq!(automation["max_event_backlog"], 100000);
        remove_assignment(&mut automation, "backlog");
        assert!(assignment_keys(&automation).is_empty());
    }

    #[test]
    fn profiles_round_trip_through_toml() {
        let section = json!({"codex": {"harness": "codex", "model": "m", "env": {"A": "1"}}, "muse": {"harness": "muse"}});
        let text = profiles_toml(&section).unwrap();
        assert!(text.contains("[profiles.codex]"));
        assert_eq!(profiles_from_toml(&text).unwrap(), section);
        assert_eq!(profiles_toml(&json!({})).unwrap(), "");
        assert_eq!(profiles_from_toml("").unwrap(), json!({}));
        assert!(profiles_from_toml("[routing]\ndefault = []\n").is_err());
        assert!(profiles_from_toml("[profiles.a\n").is_err());
    }

    #[test]
    fn desktop_section_keeps_other_fields() {
        let current = json!({"theme": "system", "window": {"width": 900, "height": 700}});
        assert_eq!(
            desktop_section(Some(&current), "dark", "last_conversation", None),
            json!({"theme": "dark", "startup": "last_conversation", "window": {"width": 900, "height": 700}})
        );
    }

    #[test]
    fn notes_say_what_happened() {
        let saved = SettingsSaveResponse {
            saved: true,
            revision: Some("ab".repeat(32)),
            changed_sections: vec![eliot_core::api::SettingsSection::CloudMcp],
            restart_required: vec![eliot_core::api::SettingsSection::CloudMcp],
            history_revision: Some(7),
        };
        assert_eq!(
            save_note(false, &Ok(saved)),
            "Saved: revision abababababab, history #7. Restart the daemon to apply Cloud MCP."
        );
        assert!(save_note(false, &Err("conflict: stale".to_owned())).contains("draft is kept"));
        assert!(save_note(true, &Err("profile_error: bad".to_owned())).starts_with("Invalid"));
        assert_eq!(utc_minute(0), "1970-01-01 00:00 UTC");
        assert_eq!(utc_minute(1_763_215_380_000), "2025-11-15 14:03 UTC");
    }
}
