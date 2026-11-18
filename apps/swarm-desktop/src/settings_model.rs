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
use eliot_core::{RouteQuotaStatus, RouteSkipReason, RoutingDecision};
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

/// Derive a bounded valid ID from a checkout name, avoiding saved IDs.
pub fn project_id_for_folder(draft: &Value, path: &str) -> String {
    let leaf = path
        .trim_end_matches(['/', '\\'])
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or("");
    let slug: String = leaf
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();
    let slug = slug.trim_matches('-');
    let slug = &slug[..slug.len().min(112)];
    let base = if slug.is_empty() { "project" } else { slug };
    let mut id = base.to_owned();
    let mut suffix = 2;
    while draft.get(&id).is_some() {
        id = format!("{base}-{suffix}");
        suffix += 1;
    }
    id
}

/// Add a project allowing the listed profiles; the daemon resolves its Git
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

/// Whether an assignment starts briefs (`enabled`, default on); off is
/// paused.
pub fn assignment_enabled(automation: &Value, key: &str) -> bool {
    assignment(automation, key)
        .and_then(|entry| entry.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(true)
}

/// Pause (false) or resume an assignment. On is the default, so resuming
/// drops the key instead of writing it.
pub fn set_assignment_enabled(automation: &mut Value, key: &str, on: bool) {
    if let Some(entry) = assignment_mut(automation, key) {
        if on {
            entry.remove("enabled");
        } else {
            entry.insert("enabled".to_owned(), json!(false));
        }
    }
}

/// What the Automations form writes for one assignment.
#[derive(Clone, Debug, PartialEq)]
pub struct AssignmentDraft {
    pub folder: String,
    pub workspace: String,
    pub role: String,
    pub max_busy: u64,
    pub interval: String,
}

/// Create an assignment or replace its form fields; its other keys
/// (`enabled`, `cleanup_on_retire`) stay. A new name must be valid and
/// unused.
pub fn put_assignment(
    automation: &mut Value,
    key: &str,
    draft: &AssignmentDraft,
    new: bool,
) -> Result<(), String> {
    if new {
        if !valid_name(key) {
            return Err("A name uses only letters, digits, - and _.".to_owned());
        }
        if assignment(automation, key).is_some() {
            return Err(format!("An automation named {key} already exists."));
        }
    }
    let assignments = object(automation)
        .entry("assignments")
        .or_insert_with(|| Value::Object(Map::new()));
    let entry = object(
        object(assignments)
            .entry(key.to_owned())
            .or_insert(json!({})),
    );
    entry.insert("folder".to_owned(), json!(draft.folder.trim()));
    entry.insert("workspace".to_owned(), json!(draft.workspace.trim()));
    entry.insert("role".to_owned(), json!(draft.role));
    entry.insert("max_busy".to_owned(), json!(draft.max_busy));
    entry.insert("interval".to_owned(), json!(draft.interval.trim()));
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

/// Required Cloud MCP fields still blank, in form order: `None` when the
/// draft is complete. An empty section (Cloud MCP off) is complete by
/// definition; the caller decides whether an enabled-but-incomplete draft
/// may be sent (#678).
pub fn cloud_missing(cloud: &Value) -> Option<String> {
    if !cloud_enabled(cloud) {
        return None;
    }
    let missing: Vec<&str> = CLOUD_FIELDS
        .iter()
        .filter(|(key, _)| {
            cloud
                .get(*key)
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
        })
        .map(|(_, label)| *label)
        .collect();
    (!missing.is_empty()).then(|| missing.join(", "))
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
    preferences: &crate::settings::Preferences,
    window: Option<(u32, u32)>,
) -> Value {
    use crate::settings::{MAX_COLLAPSED, MAX_COLLAPSED_KEY, MAX_FAVORITES, MAX_RECENTS};
    let mut desktop = current.cloned().unwrap_or_else(|| json!({}));
    let map = object(&mut desktop);
    map.insert("theme".to_owned(), json!(preferences.theme));
    map.insert("startup".to_owned(), json!(preferences.startup));
    let bounded = |models: &[crate::settings::ModelRef], limit: usize| {
        json!(models.iter().take(limit).collect::<Vec<_>>())
    };
    map.insert(
        "favorite_models".to_owned(),
        bounded(&preferences.favorites, MAX_FAVORITES),
    );
    map.insert(
        "recent_models".to_owned(),
        bounded(&preferences.recents, MAX_RECENTS),
    );
    // A key `[desktop]` would refuse (too long) is not remembered.
    map.insert(
        "collapsed_projects".to_owned(),
        json!(
            preferences
                .collapsed
                .iter()
                .filter(|key| !key.trim().is_empty() && key.len() <= MAX_COLLAPSED_KEY)
                .take(MAX_COLLAPSED)
                .collect::<Vec<_>>()
        ),
    );
    map.insert("last_access".to_owned(), json!(preferences.last_access));
    map.insert(
        "model_visibility".to_owned(),
        json!(
            preferences
                .visibility
                .iter()
                .take(crate::settings::MAX_VISIBILITY)
                .collect::<Vec<_>>()
        ),
    );
    map.insert(
        "collapsed_model_groups".to_owned(),
        json!(
            preferences
                .collapsed_groups
                .iter()
                .filter(|key| !key.trim().is_empty() && key.len() <= MAX_COLLAPSED_KEY)
                .take(MAX_COLLAPSED)
                .collect::<Vec<_>>()
        ),
    );
    map.insert(
        "display_names".to_owned(),
        json!(
            preferences
                .names
                .iter()
                .take(crate::settings::MAX_DISPLAY_NAMES)
                .collect::<Vec<_>>()
        ),
    );
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
pub fn save_note(
    dry_run: bool,
    result: &Result<SettingsSaveResponse, eliot_core::Error>,
) -> String {
    let restart = |sections: &[eliot_core::api::SettingsSection]| {
        if sections.is_empty() {
            String::new()
        } else {
            format!(
                " Restart Swarm to apply {}.",
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
        // Revisions and history numbers stay in Advanced (no hashes in the
        // main pages).
        Ok(reply) if reply.saved => format!("Saved.{}", restart(&reply.restart_required)),
        Ok(_) => "No change to save.".to_owned(),
        // The next read decides: the change goes again on the new revision
        // only if the sections it edits are unchanged there (see
        // `SettingsState::take`).
        Err(error) if error.code == "conflict" => {
            "Not saved yet: the settings changed elsewhere. Your change is kept and checked against them.".to_owned()
        }
        // Never "Saved": the read that follows shows whether it landed.
        Err(error) if crate::state::uncertain(error) => format!(
            "The save couldn't be confirmed ({}). The page shows the saved values once they are read again.",
            error.user_message()
        ),
        Err(error) if dry_run => format!("Invalid: {}", error.user_message()),
        Err(error) => format!("Not saved: {}", error.user_message()),
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
        "project_cleanup" => "You, removing stale project registrations".to_owned(),
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

/// Why routing passed over a candidate, in words.
pub fn skip_reason(reason: RouteSkipReason) -> &'static str {
    match reason {
        RouteSkipReason::NotInstalled => "not installed",
        RouteSkipReason::ProviderHold => "provider on hold",
        RouteSkipReason::QuotaExhausted => "quota used up",
        RouteSkipReason::CapacityFull => "at capacity",
        RouteSkipReason::Unauthorized => "not allowed here",
        RouteSkipReason::InvalidProfile => "profile does not resolve",
    }
}

/// A routing decision as a headline and one line per skipped candidate.
pub fn decision_lines(decision: &RoutingDecision) -> (String, Vec<String>) {
    let role = if decision.role == "default" {
        String::new()
    } else {
        format!(" for the {} role", decision.role)
    };
    let headline = match &decision.profile {
        Some(profile) => {
            let quota = match decision.quota_status {
                RouteQuotaStatus::Supported => "",
                RouteQuotaStatus::Unsupported => "; no quota readback",
                RouteQuotaStatus::Unknown => "; quota unknown",
            };
            format!("Routing picks {profile}{role}: {}{quota}", decision.reason)
        }
        None if decision.skipped.is_empty() => {
            format!("No profile is set{role}; add one in Settings, Routing")
        }
        None => format!("No profile is available{role}"),
    };
    let skipped = decision
        .skipped
        .iter()
        .map(|skip| format!("Skipped {}: {}", skip.profile, skip_reason(skip.reason)))
        .collect();
    (headline, skipped)
}

/// A refused launch in words: a routing miss lists its skipped candidates.
pub fn launch_error(error: &eliot_core::Error) -> String {
    let decision = error
        .routing
        .as_deref()
        .cloned()
        .or_else(|| serde_json::from_str::<RoutingDecision>(&error.message).ok());
    match decision {
        Some(decision) if error.code == "routing_unavailable" => {
            let (headline, skipped) = decision_lines(&decision);
            let mut text = format!("routing_unavailable: {headline}");
            if !skipped.is_empty() {
                text.push_str(&format!(" ({})", skipped.join("; ")));
            }
            text
        }
        _ => error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn folder_project_ids_accept_windows_paths_and_resolve_slug_collisions() {
        let draft = json!({"my-project": {}, "my-project-2": {}});
        assert_eq!(
            project_id_for_folder(&draft, r"C:\work\My Project\"),
            "my-project-3"
        );
        assert_eq!(
            project_id_for_folder(&draft, "/work/Other_Repo/"),
            "other_repo"
        );
        assert_eq!(project_id_for_folder(&draft, "/work/日本語"), "project");
        let long = format!("C:/work/{}", "x".repeat(200));
        let id = project_id_for_folder(&json!({}), &long);
        assert!(valid_name(&id));
        assert!(valid_name(&project_id_for_folder(&json!({id: {}}), &long)));
    }

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
        let draft = AssignmentDraft {
            folder: " C:/briefs ".to_owned(),
            workspace: "C:/w".to_owned(),
            role: "audit".to_owned(),
            max_busy: 2,
            interval: "30s".to_owned(),
        };
        put_assignment(&mut automation, "backlog", &draft, true).unwrap();
        assert!(put_assignment(&mut automation, "backlog", &draft, true).is_err());
        assert!(put_assignment(&mut automation, "bad name", &draft, true).is_err());
        assert_eq!(assignment_keys(&automation), ["backlog"]);
        assert_eq!(
            assignment_text(&automation, "backlog", AssignmentField::Folder),
            "C:/briefs"
        );
        assert_eq!(assignment_busy(&automation, "backlog"), Some(2));
        assert_eq!(assignment_role(&automation, "backlog"), "audit");
        assert_eq!(automation["max_event_backlog"], 100000);

        // Paused, then edited: the edit keeps the pause and other keys.
        assert!(assignment_enabled(&automation, "backlog"));
        set_assignment_enabled(&mut automation, "backlog", false);
        automation["assignments"]["backlog"]["cleanup_on_retire"] = json!(true);
        let edited = AssignmentDraft {
            max_busy: 3,
            ..draft
        };
        put_assignment(&mut automation, "backlog", &edited, false).unwrap();
        assert!(!assignment_enabled(&automation, "backlog"));
        assert_eq!(
            automation["assignments"]["backlog"]["cleanup_on_retire"],
            true
        );
        assert_eq!(assignment_busy(&automation, "backlog"), Some(3));
        // Resuming drops the key (on is the daemon's default).
        set_assignment_enabled(&mut automation, "backlog", true);
        assert!(
            automation["assignments"]["backlog"]
                .get("enabled")
                .is_none()
        );
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
        use crate::settings::{Access, ModelRef, Preferences, Startup, ThemeChoice};
        let current = json!({"theme": "system", "window": {"width": 900, "height": 700}});
        let preferences = Preferences {
            theme: ThemeChoice::Dark,
            startup: Startup::LastConversation,
            favorites: vec![ModelRef {
                harness: "claude".to_owned(),
                model: "claude-opus-5-5".to_owned(),
                effort: Some("high".to_owned()),
            }],
            recents: Vec::new(),
            collapsed: vec!["repo:c:/a/.git".to_owned(), "x".repeat(300)],
            last_access: Some(Access::Full),
            visibility: vec![crate::settings::Visibility {
                harness: "opencode".to_owned(),
                model: None,
                provider: Some("openrouter".to_owned()),
                visible: false,
            }],
            collapsed_groups: vec!["opencode/openrouter".to_owned()],
            names: vec![crate::settings::DisplayName {
                key: "agent:a3".to_owned(),
                name: "Fix login".to_owned(),
            }],
        };
        assert_eq!(
            desktop_section(Some(&current), &preferences, None),
            json!({
                "theme": "dark",
                "startup": "last_conversation",
                "window": {"width": 900, "height": 700},
                "favorite_models": [{"harness": "claude", "model": "claude-opus-5-5", "effort": "high"}],
                "recent_models": [],
                // A key longer than `[desktop]` accepts is not written.
                "collapsed_projects": ["repo:c:/a/.git"],
                "last_access": "full",
                "model_visibility": [{"harness": "opencode", "provider": "openrouter", "visible": false}],
                "collapsed_model_groups": ["opencode/openrouter"],
                "display_names": [{"key": "agent:a3", "name": "Fix login"}]
            })
        );
    }

    #[test]
    fn decisions_read_as_words() {
        let decision = RoutingDecision {
            role: "worker".to_owned(),
            profile: Some("muse".to_owned()),
            source: "settings.routing.worker".to_owned(),
            reason: "first available candidate".to_owned(),
            skipped: vec![eliot_core::RouteSkip {
                profile: "codex".to_owned(),
                reason: RouteSkipReason::CapacityFull,
                quota_status: RouteQuotaStatus::Unknown,
            }],
            quota_status: RouteQuotaStatus::Unknown,
        };
        let (headline, skipped) = decision_lines(&decision);
        assert_eq!(
            headline,
            "Routing picks muse for the worker role: first available candidate; quota unknown"
        );
        assert_eq!(skipped, ["Skipped codex: at capacity"]);
        let mut miss = decision.clone();
        miss.profile = None;
        miss.role = "default".to_owned();
        let mut error =
            eliot_core::Error::new("routing_unavailable", serde_json::to_string(&miss).unwrap());
        assert_eq!(
            launch_error(&error),
            "routing_unavailable: No profile is available (Skipped codex: at capacity)"
        );
        error.code = "not_allowed".to_owned();
        assert!(launch_error(&error).starts_with("not_allowed: "));
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
            "Saved. Restart Swarm to apply Cloud MCP."
        );
        assert!(
            save_note(false, &Err(eliot_core::Error::new("conflict", "stale")))
                .contains("change is kept")
        );
        let unknown = save_note(false, &Err(eliot_core::Error::new("unknown", "timed out")));
        assert!(unknown.contains("couldn't be confirmed"), "{unknown}");
        assert!(!unknown.contains("Saved"), "{unknown}");
        assert!(
            save_note(true, &Err(eliot_core::Error::new("profile_error", "bad")))
                .starts_with("Invalid")
        );
        assert_eq!(utc_minute(0), "1970-01-01 00:00 UTC");
        assert_eq!(utc_minute(1_763_215_380_000), "2025-11-15 14:03 UTC");
    }
    #[test]
    fn cloud_missing_names_blank_fields_in_form_order() {
        // #678: the send gate for a Cloud MCP draft.
        let mut template = cloud_template();
        let missing = cloud_missing(&template).expect("empty template is incomplete");
        assert_eq!(missing, "cloudflared executable, Tunnel id, Tunnel credentials file, Public hostname, Access team name, Access audience tag");
        set_cloud_field(&mut template, "hostname", "example.com");
        let missing = cloud_missing(&template).expect("still incomplete");
        assert!(!missing.contains("Public hostname"));
        assert!(missing.contains("Tunnel id"));
        assert!(cloud_missing(&json!({})).is_none());
    }
}
