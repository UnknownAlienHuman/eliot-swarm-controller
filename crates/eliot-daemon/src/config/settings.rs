//! Unified typed settings over the one bounded `config.toml` source.

use super::*;
use eliot_core::SecretPolicy;
use eliot_core::api::{
    SettingsClientSource, SettingsHistoryStatus, SettingsReadResponse, SettingsSaveRequest,
    SettingsSaveResponse, SettingsSection,
};
use eliot_store::Store;
use serde_json::{Map as JsonMap, Value as JsonValue};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

const HISTORY_LIMIT: usize = 100;
pub(super) static SETTINGS_SAVE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SettingsAuthor {
    Owner(SettingsClientSource),
    Bootstrap,
    ProjectDiscovery,
    ProjectCleanup,
}

impl SettingsAuthor {
    fn as_str(self) -> &'static str {
        match self {
            Self::Owner(SettingsClientSource::Cli) => "owner:cli",
            Self::Owner(SettingsClientSource::Desktop) => "owner:desktop",
            Self::Bootstrap => "bootstrap",
            Self::ProjectDiscovery => "project_discovery",
            Self::ProjectCleanup => "project_cleanup",
        }
    }
}

fn section_name(section: SettingsSection) -> &'static str {
    match section {
        SettingsSection::Profiles => "profiles",
        SettingsSection::Routing => "routing",
        SettingsSection::Projects => "projects",
        SettingsSection::Daemon => "daemon",
        SettingsSection::Automation => "automation",
        SettingsSection::Mail => "mail",
        SettingsSection::CloudMcp => "cloud_mcp",
        SettingsSection::Desktop => "desktop",
        SettingsSection::Accounts => "accounts",
        SettingsSection::Proxies => "proxies",
    }
}

fn all_sections() -> [SettingsSection; 10] {
    [
        SettingsSection::Profiles,
        SettingsSection::Routing,
        SettingsSection::Projects,
        SettingsSection::Daemon,
        SettingsSection::Automation,
        SettingsSection::Mail,
        SettingsSection::CloudMcp,
        SettingsSection::Desktop,
        SettingsSection::Accounts,
        SettingsSection::Proxies,
    ]
}

fn restart_required(sections: impl IntoIterator<Item = SettingsSection>) -> Vec<SettingsSection> {
    let mut result: Vec<_> = sections
        .into_iter()
        .filter(|section| {
            matches!(
                section,
                SettingsSection::Daemon | SettingsSection::Mail | SettingsSection::CloudMcp
            )
        })
        .collect();
    result.sort();
    result.dedup();
    result
}

fn empty_object() -> JsonValue {
    JsonValue::Object(JsonMap::new())
}

fn carries_redaction(value: &JsonValue) -> bool {
    match value {
        JsonValue::String(text) => text.contains(eliot_core::api::REDACTION_MARKER),
        JsonValue::Array(items) => items.iter().any(carries_redaction),
        JsonValue::Object(items) => items.iter().any(|(key, value)| {
            key.contains(eliot_core::api::REDACTION_MARKER) || carries_redaction(value)
        }),
        _ => false,
    }
}

fn section_json(source: &str, name: &str) -> core::Result<JsonValue> {
    section_value(&parse_generic(source)?, name)
}

fn parse_generic(source: &str) -> core::Result<toml::Table> {
    toml::from_str(source)
        .map_err(|_| core::Error::new("profile_error", "config.toml is not valid TOML"))
}

/// One section of an already-parsed file as the settings reader shows it.
fn section_value(parsed: &toml::Table, name: &str) -> core::Result<JsonValue> {
    let Some(value) = parsed.get(name) else {
        return Ok(match name {
            "routing" => serde_json::json!({"default": []}),
            "desktop" => desktop_default(),
            _ => empty_object(),
        });
    };
    let mut value = serde_json::to_value(value)
        .map_err(|_| core::Error::new("internal", "settings section could not be encoded"))?;
    if name == "desktop" {
        let object = value.as_object_mut().ok_or_else(|| {
            core::Error::new("source_integrity", "desktop settings are not an object")
        })?;
        // Every field reads with its default, except `theme`, which an
        // older file may omit and the reader keeps verbatim.
        if let JsonValue::Object(defaults) = desktop_default() {
            for (key, default) in defaults {
                if key != "theme" {
                    object.entry(key).or_insert(default);
                }
            }
        }
    }
    if name == "automation" {
        value
            .as_object_mut()
            .ok_or_else(|| {
                core::Error::new("source_integrity", "automation settings are not an object")
            })?
            .entry("auto_cleanup")
            .or_insert(JsonValue::Bool(false));
    }
    Ok(value)
}

/// The `[desktop]` section a file without one reads as.
fn desktop_default() -> JsonValue {
    serde_json::json!({
        "theme": "system",
        "window": null,
        "startup": "new_task",
        "favorite_models": [],
        "recent_models": [],
        "collapsed_projects": [],
        "last_access": null,
        "model_visibility": [],
        "collapsed_model_groups": [],
        "display_names": [],
    })
}

/// One-time daemon-startup settlement of crash-left pending settings history
/// rows against the current file revision. Reads never reconcile, so this
/// plus the save path settle every pending row exactly once.
pub(crate) fn reconcile_at_startup(data_root: &Path, store: &mut Store) -> core::Result<()> {
    let current = read_config_source(data_root)?;
    let current_revision = current.as_deref().map(profiles_edit::revision_of);
    store.reconcile_settings_history(current_revision.as_deref())
}

/// The file half of `settings.read`: one read of `config.toml` and one
/// parse of each kind (the generic table for the section values, the strict
/// config for validation), so the gate, the checks and the returned values
/// all describe the same bytes under one revision. It needs no Store, so a
/// caller holding the Store lock can run it first.
pub(crate) struct SettingsSnapshot {
    revision: Option<String>,
    sections: Vec<SettingsSection>,
    values: BTreeMap<SettingsSection, JsonValue>,
}

impl SettingsSnapshot {
    /// The full `settings.read` reply: these values plus the Store's
    /// bounded author history.
    pub(crate) fn into_response(self, store: &Store) -> core::Result<SettingsReadResponse> {
        Ok(SettingsReadResponse {
            revision: self.revision,
            restart_required: restart_required(self.sections),
            sections: self.values,
            history: store.list_settings_history(HISTORY_LIMIT)?,
        })
    }
}

pub(crate) fn read_internal(
    data_root: &Path,
    store: &Store,
    requested: Option<&[SettingsSection]>,
    policy: &SecretPolicy,
) -> core::Result<SettingsReadResponse> {
    read_snapshot(data_root, requested, policy)?.into_response(store)
}

pub(crate) fn read_snapshot(
    data_root: &Path,
    requested: Option<&[SettingsSection]>,
    policy: &SecretPolicy,
) -> core::Result<SettingsSnapshot> {
    let original = read_config_source(data_root)?;
    let source = original.as_deref().unwrap_or_default();
    let revision = original.as_deref().map(profiles_edit::revision_of);
    let table = parse_generic(source).ok();
    let config = parse_config(source);
    if profiles_edit::editability(source, table.as_ref(), config.as_ref().ok(), policy).is_some() {
        return Err(core::Error::new(
            "not_allowed",
            "settings include a profile section that is not safe to expose",
        ));
    }
    validate_parsed(&config?, source)?;
    let table =
        table.ok_or_else(|| core::Error::new("profile_error", "config.toml is not valid TOML"))?;
    let sections = requested.map_or_else(|| all_sections().to_vec(), ToOwned::to_owned);
    let mut values = BTreeMap::new();
    for section in &sections {
        let mut value = section_value(&table, section_name(*section))?;
        if *section == SettingsSection::Daemon {
            // Explicit and default roots resolve through the same
            // launch-time validator, so a stored root inside the owner
            // chat tree is refused at readback too.
            let configured = match value.get("chats_root") {
                None => None,
                Some(JsonValue::String(root)) => Some(root.as_str()),
                Some(_) => {
                    return Err(core::Error::new(
                        "profile_error",
                        "daemon.chats_root must be a string",
                    ));
                }
            };
            value["chats_root"] = serde_json::json!(
                super::validate_chats_location(data_root, configured)?.to_string_lossy()
            );
        }
        if policy.contains_value(&value) {
            return Err(core::Error::new(
                "not_allowed",
                "a settings section contains a value withheld by the current secret policy",
            ));
        }
        values.insert(*section, value);
    }
    Ok(SettingsSnapshot {
        revision,
        sections,
        values,
    })
}

pub(crate) fn save(
    data_root: &Path,
    store: &mut Store,
    request: &SettingsSaveRequest,
    policy: &SecretPolicy,
    projects: Option<&ResolvedProjects>,
) -> core::Result<(SettingsSaveResponse, Vec<core::Event>)> {
    let _guard = SETTINGS_SAVE_LOCK
        .lock()
        .map_err(|_| core::Error::new("internal", "settings save lock poisoned"))?;
    if request.sections.contains_key(&SettingsSection::Projects)
        && projects.is_none_or(|projects| projects.source_revision != request.expected_revision)
    {
        return Err(core::Error::new(
            "conflict",
            "resolved projects do not match the settings source revision",
        ));
    }
    if !request.dry_run
        && let Some(projects) = projects
    {
        let (revision, configured) = project_configs_snapshot(data_root)?;
        if revision != request.expected_revision {
            return Err(core::Error::new(
                "conflict",
                "config.toml changed since settings were read; reload and reapply the edit",
            ));
        }
        let retained: BTreeSet<_> = projects
            .inputs
            .iter()
            .map(|input| input.id.as_str())
            .collect();
        let mut previous: BTreeMap<_, _> = configured
            .into_iter()
            .map(|row| (row.id, row.path))
            .collect();
        for row in store.list_project_registrations("owner", None, 1_000)? {
            previous.insert(row.id, row.checkout);
        }
        for (id, path) in previous {
            if !retained.contains(id.as_str()) {
                require_project_unused(store, &id, &path)?;
            }
        }
    }
    let reply = save_internal_locked(
        data_root,
        store,
        request.expected_revision.as_deref(),
        &request.sections,
        SettingsAuthor::Owner(request.client),
        request.dry_run,
        policy,
    )?;
    let mut events = Vec::new();
    if reply.saved && reply.changed_sections.contains(&SettingsSection::Projects) {
        let projects = projects.expect("project settings were resolved before publication");
        if reply.revision.as_deref() != Some(projects.candidate_revision.as_str())
            || current_revision(data_root).map_err(|_| core::Error::new("unknown", "project settings were published but their current revision could not be verified; read settings before retrying"))? != reply.revision
        {
            return Err(core::Error::new(
                "unknown",
                "project settings source changed before registration reconciliation; read settings before retrying",
            ));
        }
        let change = store.reconcile_project_registrations("owner", &projects.inputs, crate::common::now_ms())
            .map_err(|_| core::Error::new("unknown", "project settings were saved but Store registration reconciliation is uncertain; read settings and projects before retrying"))?;
        events = change.events;
    }
    Ok((reply, events))
}

/// Reuse the complete Store usage fence for both settings removal and cleanup.
/// Callers hold the Store lock through settings publication.
pub(crate) fn require_project_unused(store: &Store, id: &str, path: &str) -> core::Result<()> {
    let registration = store.get_project_registration("owner", id)?;
    let checkout = registration
        .as_ref()
        .map_or(path, |row| row.checkout.as_str());
    let common = registration
        .as_ref()
        .map(|row| row.repo_common_dir.as_str());
    let linked = common.map_or_else(Vec::new, crate::workspace_git::registration_linked_paths);
    if store.project_in_use(id, checkout, common, &linked)? {
        return Err(core::Error::new(
            "project_in_use",
            "an Agent or unsettled command still uses the registration",
        ));
    }
    Ok(())
}

pub(crate) struct ResolvedProjects {
    pub source_revision: Option<String>,
    pub candidate_revision: String,
    pub inputs: Vec<eliot_store::ProjectRegistrationInput>,
}

/// Internal writer used by authenticated Owner RPCs and machine bootstrap.
/// The author and dry-run state are not caller-controlled on this path.
pub(crate) fn save_internal(
    data_root: &Path,
    store: &mut Store,
    expected_revision: Option<&str>,
    sections: &BTreeMap<SettingsSection, JsonValue>,
    author: SettingsAuthor,
    dry_run: bool,
    policy: &SecretPolicy,
) -> core::Result<SettingsSaveResponse> {
    let _guard = SETTINGS_SAVE_LOCK
        .lock()
        .map_err(|_| core::Error::new("internal", "settings save lock poisoned"))?;
    save_internal_locked(
        data_root,
        store,
        expected_revision,
        sections,
        author,
        dry_run,
        policy,
    )
}

/// Publish an Owner cleanup through the same settings writer and revoke
/// exactly the removed registrations. Unselected paths may also be missing.
pub(crate) fn remove_stale_projects(
    data_root: &Path,
    store: &mut Store,
    revision: &str,
    projects: JsonMap<String, JsonValue>,
    removed: &[String],
    policy: &SecretPolicy,
) -> core::Result<()> {
    let _guard = SETTINGS_SAVE_LOCK
        .lock()
        .map_err(|_| core::Error::new("internal", "settings save lock poisoned"))?;
    let reply = save_internal_locked(
        data_root,
        store,
        Some(revision),
        &BTreeMap::from([(SettingsSection::Projects, JsonValue::Object(projects))]),
        SettingsAuthor::ProjectCleanup,
        false,
        policy,
    )?;
    if !reply.saved
        || current_revision(data_root).map_err(|_| {
            core::Error::new("unknown", "project cleanup revision could not be verified")
        })? != reply.revision
    {
        return Err(core::Error::new(
            "unknown",
            "project cleanup publication could not be confirmed",
        ));
    }
    store.remove_project_registrations(removed).map_err(|_| {
        core::Error::new(
            "unknown",
            "project settings were saved but Store reconciliation is uncertain",
        )
    })
}

fn save_internal_locked(
    data_root: &Path,
    store: &mut Store,
    expected_revision: Option<&str>,
    sections: &BTreeMap<SettingsSection, JsonValue>,
    author: SettingsAuthor,
    dry_run: bool,
    policy: &SecretPolicy,
) -> core::Result<SettingsSaveResponse> {
    let (original_file, candidate) =
        prepare_candidate(data_root, expected_revision, sections, policy)?;
    let original = original_file.as_deref().unwrap_or_default();
    let current_revision = original_file.as_deref().map(profiles_edit::revision_of);
    let before = parse_generic(original).ok();
    let after = parse_generic(&candidate).ok();
    let shown = |table: &Option<toml::Table>, section: SettingsSection| {
        table
            .as_ref()
            .and_then(|table| section_value(table, section_name(section)).ok())
    };
    let changed_sections = sections
        .keys()
        .copied()
        .filter(|section| {
            shown(&before, *section) != shown(&after, *section)
                || (*section == SettingsSection::Desktop
                    && has_desktop_section(original) != has_desktop_section(&candidate))
        })
        .collect::<Vec<_>>();
    let restart = restart_required_for_change(original, &candidate, &changed_sections);
    if dry_run || candidate == original || changed_sections.is_empty() {
        return Ok(SettingsSaveResponse {
            saved: false,
            revision: current_revision,
            changed_sections,
            restart_required: restart,
            history_revision: None,
        });
    }
    // Reconcile crash-left pending history only on the mutating path: a
    // validation-only dry run must leave prior save outcomes untouched.
    store.reconcile_settings_history(current_revision.as_deref())?;
    commit_candidate_locked(
        data_root,
        store,
        original_file.as_deref(),
        &candidate,
        changed_sections,
        restart,
        author,
    )
}

/// Read-only preparation also used before asynchronous Git identity resolution.
/// The writer repeats this validation and source CAS before publication.
pub(crate) fn prepare_candidate(
    data_root: &Path,
    expected_revision: Option<&str>,
    sections: &BTreeMap<SettingsSection, JsonValue>,
    policy: &SecretPolicy,
) -> core::Result<(Option<String>, String)> {
    let original_file = read_config_source(data_root)?;
    let current_revision = original_file.as_deref().map(profiles_edit::revision_of);
    if expected_revision != current_revision.as_deref() {
        return Err(core::Error::new(
            "conflict",
            "config.toml changed since settings were read; reload and reapply the edit",
        ));
    }
    if sections.is_empty() || sections.len() > 16 {
        return Err(core::Error::new(
            "invalid",
            "settings save section count is invalid",
        ));
    }
    let original = original_file.as_deref().unwrap_or_default();
    let candidate = splice_sections(
        original,
        &replacements(sections, policy, has_desktop_section(original))?,
    );
    if candidate.len() > MAX_CONFIG_BYTES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: file would exceed the 128 KiB bound",
        ));
    }
    validate_unselected_sections(original, &candidate, sections.keys().copied())?;
    validate_candidate(&candidate)?;
    super::validate_chats_location(
        data_root,
        parse_config(&candidate)?.daemon.chats_root.as_deref(),
    )?;
    Ok((original_file, candidate))
}

pub(crate) fn project_configs(source: &str) -> core::Result<Vec<ProjectRegistrationConfig>> {
    project_registrations_in(&parse_config(source)?, source)
}

/// Read a single settings source for Git resolution and its later CAS.
pub(crate) fn project_configs_snapshot(
    data_root: &Path,
) -> core::Result<(Option<String>, Vec<ProjectRegistrationConfig>)> {
    match read_config_source(data_root)? {
        Some(source) => Ok((
            Some(profiles_edit::revision_of(&source)),
            project_configs(&source)?,
        )),
        None => Ok((None, Vec::new())),
    }
}

pub(crate) fn current_revision(data_root: &Path) -> core::Result<Option<String>> {
    Ok(read_config_source(data_root)?
        .as_deref()
        .map(profiles_edit::revision_of))
}

/// Shared file publication and history path. Caller holds `SETTINGS_SAVE_LOCK`.
pub(super) fn commit_candidate_locked(
    data_root: &Path,
    store: &mut Store,
    original: Option<&str>,
    candidate: &str,
    changed_sections: Vec<SettingsSection>,
    restart_required: Vec<SettingsSection>,
    author: SettingsAuthor,
) -> core::Result<SettingsSaveResponse> {
    let previous_revision = original.map(profiles_edit::revision_of);
    if read_config_source(data_root)?.as_deref() != original {
        return Err(core::Error::new(
            "conflict",
            "config.toml changed while settings were validated; reload and reapply the edit",
        ));
    }
    let new_revision = profiles_edit::revision_of(candidate);
    let history_revision = store.prepare_settings_history(
        previous_revision.as_deref(),
        &new_revision,
        &changed_sections,
        author.as_str(),
        crate::common::now_ms(),
    )?;
    if let Err(error) = crate::platform::data_dir::replace_private_file(
        data_root,
        "config.toml",
        candidate.as_bytes(),
    ) {
        let observed = read_config_source(data_root)
            .map(|source| source.as_deref().map(profiles_edit::revision_of));
        let status = match observed {
            Ok(revision) if revision.as_deref() == Some(new_revision.as_str()) => {
                SettingsHistoryStatus::Applied
            }
            Ok(revision) if revision == previous_revision => SettingsHistoryStatus::NotApplied,
            _ => SettingsHistoryStatus::Unknown,
        };
        if store
            .settle_settings_history(history_revision, status)
            .is_err()
            || status != SettingsHistoryStatus::NotApplied
        {
            return Err(core::Error::new(
                "unknown",
                "settings replacement outcome is uncertain; read settings and history before retrying",
            ));
        }
        return Err(error);
    }
    if store
        .settle_settings_history(history_revision, SettingsHistoryStatus::Applied)
        .is_err()
    {
        return Err(core::Error::new(
            "unknown",
            "settings were replaced but history settlement is uncertain; read settings and history before retrying",
        ));
    }
    Ok(SettingsSaveResponse {
        saved: true,
        revision: Some(new_revision),
        changed_sections,
        restart_required,
        history_revision: Some(history_revision),
    })
}

fn replacements(
    sections: &BTreeMap<SettingsSection, JsonValue>,
    policy: &SecretPolicy,
    desktop_present: bool,
) -> core::Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for (section, value) in sections {
        if policy.contains_value(value) {
            return Err(core::Error::new(
                "not_allowed",
                "settings draft contains a value withheld by the current secret policy",
            ));
        }
        if carries_redaction(value) {
            return Err(core::Error::new(
                "not_allowed",
                "settings draft contains a redaction marker",
            ));
        }
        result.insert(
            section_name(*section).to_owned(),
            section_source(*section, value, desktop_present)?,
        );
    }
    Ok(result)
}

/// One section's replacement TOML; empty when the section is removed (an
/// empty or default section reads as its default).
fn section_source(
    section: SettingsSection,
    value: &JsonValue,
    desktop_present: bool,
) -> core::Result<String> {
    if value.as_object().is_none() {
        return Err(core::Error::new(
            "invalid",
            "settings sections must be objects",
        ));
    }
    if (is_default_section(section, value)
        && (section != SettingsSection::Desktop || !desktop_present))
        || value.as_object().is_some_and(JsonMap::is_empty)
    {
        return Ok(String::new());
    }
    let mut normalized = value.clone();
    if section == SettingsSection::Desktop
        && let Some(object) = normalized.as_object_mut()
    {
        // TOML has no null: omit null window/last_access like the
        // pre-existing window handling, so a JSON null round-trips.
        if object.get("window").is_some_and(JsonValue::is_null) {
            object.remove("window");
        }
        if object.get("last_access").is_some_and(JsonValue::is_null) {
            object.remove("last_access");
        }
    }
    let section_value = toml::Value::try_from(normalized).map_err(|_| {
        core::Error::new(
            "profile_error",
            "settings section contains a non-TOML value",
        )
    })?;
    let name = section_name(section).to_owned();
    let mut root = toml::map::Map::new();
    root.insert(name, section_value);
    toml::to_string(&toml::Value::Table(root)).map_err(|_| {
        core::Error::new(
            "profile_error",
            "settings section could not be encoded as TOML",
        )
    })
}

/// A section as the settings reader shows it once a save stored `value`
/// (TOML has no null; an empty or default section reads as its default).
/// An editor compares a read with this to tell its own save from another
/// writer's (the stale-revision rule in `docs/spec/configuration.md`).
/// `None` when `value` cannot be stored. The read also fills the daemon
/// section's `chats_root`; this does not.
pub fn stored_section(section: SettingsSection, value: &JsonValue) -> Option<JsonValue> {
    let source = section_source(section, value, true).ok()?;
    section_json(&source, section_name(section)).ok()
}

fn validate_unselected_sections(
    before: &str,
    after: &str,
    selected: impl IntoIterator<Item = SettingsSection>,
) -> core::Result<()> {
    let selected: BTreeSet<&str> = selected.into_iter().map(section_name).collect();
    let mut before: toml::Table = toml::from_str(before)
        .map_err(|_| core::Error::new("profile_error", "config.toml is not valid TOML"))?;
    let mut after: toml::Table = toml::from_str(after)
        .map_err(|_| core::Error::new("profile_error", "settings draft is not valid TOML"))?;
    before.retain(|key, _| !selected.contains(key));
    after.retain(|key, _| !selected.contains(key));
    if before != after {
        return Err(core::Error::new(
            "profile_error",
            "settings splice changed an unselected section",
        ));
    }
    Ok(())
}

fn is_default_section(section: SettingsSection, value: &JsonValue) -> bool {
    match section {
        SettingsSection::Routing => value == &serde_json::json!({"default": []}),
        SettingsSection::Desktop => {
            value == &desktop_default()
                || value == &serde_json::json!({"theme": "system", "window": null})
        }
        _ => false,
    }
}

fn has_desktop_section(source: &str) -> bool {
    toml::from_str::<toml::Table>(source).is_ok_and(|table| table.contains_key("desktop"))
}

fn restart_required_for_change(
    previous: &str,
    current: &str,
    changed: &[SettingsSection],
) -> Vec<SettingsSection> {
    let mut needs_restart = Vec::new();
    for section in changed {
        match section {
            SettingsSection::Daemon => {
                let (Some(mut before), Some(mut after)) = (
                    section_json(previous, "daemon").ok(),
                    section_json(current, "daemon").ok(),
                ) else {
                    needs_restart.push(*section);
                    continue;
                };
                // Retention windows and the log level apply live: they are
                // never a restart gate on their own.
                if let Some(object) = before.as_object_mut() {
                    object.remove("retention");
                    object.remove("log_level");
                }
                if let Some(object) = after.as_object_mut() {
                    object.remove("retention");
                    object.remove("log_level");
                }
                if before != after {
                    needs_restart.push(*section);
                }
            }
            SettingsSection::Mail | SettingsSection::CloudMcp => needs_restart.push(*section),
            _ => {}
        }
    }
    needs_restart.sort();
    needs_restart.dedup();
    needs_restart
}

/// Replace selected top-level tables while keeping every other source block
/// verbatim. `split_blocks` keeps nested tables under their top-level owner.
fn splice_sections(source: &str, replacements: &BTreeMap<String, String>) -> String {
    let mut out = String::with_capacity(source.len());
    let mut inserted = BTreeSet::new();
    for block in profiles_edit::split_blocks(source) {
        match block
            .section
            .as_deref()
            .and_then(|name| replacements.get(name).map(|_| name))
        {
            Some(name) => {
                if inserted.insert(name.to_owned())
                    && let Some(replacement) = replacements.get(name)
                    && !replacement.is_empty()
                {
                    if !out.is_empty() && !out.ends_with('\n') {
                        out.push('\n');
                    }
                    out.push_str(replacement);
                }
            }
            None => out.push_str(block.text),
        }
    }
    for (name, replacement) in replacements {
        if inserted.contains(name) || replacement.is_empty() {
            continue;
        }
        if !out.is_empty() {
            if !out.ends_with('\n') {
                out.push('\n');
            }
            if !out.ends_with("\n\n") {
                out.push('\n');
            }
        }
        out.push_str(replacement);
    }
    out
}

fn validate_candidate(source: &str) -> core::Result<()> {
    validate_parsed(&parse_config(source)?, source)
}

/// [`validate_candidate`] over the strict parse of that same `source`.
fn validate_parsed(config: &FileConfig, source: &str) -> core::Result<()> {
    for (name, proxy) in &config.proxies {
        let proxy = proxy.get_ref();
        if is_builtin_codex_provider(name) {
            return Err(core::Error::new(
                "profile_error",
                "proxy name collides with a builtin Codex provider",
            ));
        }
        let route = core::RouteBinding {
            name: name.clone(),
            endpoint: proxy.attach.clone(),
            readiness_path: proxy.health.clone(),
            provider_base_url: proxy.base_url_for.codex.clone(),
            client_key_env_name: proxy.api_key_env.clone(),
        };
        route
            .validate()
            .map_err(|_| core::Error::new("profile_error", "invalid adopted proxy route"))?;
        core::CapacityScope {
            scope: format!("proxy:{name}"),
            max_live: proxy.max_live,
        }
        .validate()
        .map_err(|_| core::Error::new("profile_error", "invalid proxy capacity"))?;
        if proxy
            .api_key_env
            .as_deref()
            .is_some_and(|name| native_store_selector(name).is_some())
        {
            return Err(core::Error::new(
                "profile_error",
                "proxy client key cannot use a native store selector",
            ));
        }
    }
    for name in config.profiles.keys() {
        resolve_profile_in(config, source, name)?;
    }
    project_registrations_in(config, source)?;
    let route_credentials = configured_route_credential_env_names(&config.proxies);
    config
        .automation
        .clone()
        .validate(source, &route_credentials)?;
    config.daemon.clone().validate()?;
    config.mail.clone().validate()?;
    if let Some(cloud) = &config.cloud_mcp {
        let executable = std::env::current_exe()
            .map_err(|_| core::Error::new("platform_io", "cannot locate the daemon executable"))?;
        let settings = crate::cloud_mcp::CloudMcpConfig {
            http_mcp_executable: executable,
            cloudflared_executable: cloud.cloudflared_executable.clone(),
            tunnel_id: cloud.tunnel_id.clone(),
            credentials_file: cloud.credentials_file.clone(),
            hostname: cloud.hostname.clone(),
            access_team_name: cloud.access_team_name.clone(),
            access_audience_tag: cloud.access_audience_tag.clone(),
        };
        crate::cloud_mcp::validate_config(&settings)?;
    }
    Ok(())
}
