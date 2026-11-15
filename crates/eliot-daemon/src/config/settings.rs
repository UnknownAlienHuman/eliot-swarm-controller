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
}

impl SettingsAuthor {
    fn as_str(self) -> &'static str {
        match self {
            Self::Owner(SettingsClientSource::Cli) => "owner:cli",
            Self::Owner(SettingsClientSource::Desktop) => "owner:desktop",
            Self::Bootstrap => "bootstrap",
            Self::ProjectDiscovery => "project_discovery",
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
                SettingsSection::Daemon
                    | SettingsSection::Automation
                    | SettingsSection::Mail
                    | SettingsSection::CloudMcp
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
    let parsed: toml::Table = toml::from_str(source)
        .map_err(|_| core::Error::new("profile_error", "config.toml is not valid TOML"))?;
    let Some(value) = parsed.get(name) else {
        return Ok(match name {
            "routing" => serde_json::json!({"default": []}),
            "desktop" => serde_json::json!({"theme": "system", "window": null}),
            _ => empty_object(),
        });
    };
    let mut value = serde_json::to_value(value)
        .map_err(|_| core::Error::new("internal", "settings section could not be encoded"))?;
    if name == "desktop" {
        let object = value.as_object_mut().ok_or_else(|| {
            core::Error::new("source_integrity", "desktop settings are not an object")
        })?;
        object.entry("window").or_insert(JsonValue::Null);
    }
    Ok(value)
}

pub(crate) fn read_internal(
    data_root: &Path,
    store: &mut Store,
    requested: Option<&[SettingsSection]>,
    policy: &SecretPolicy,
) -> core::Result<SettingsReadResponse> {
    let original = read_config_source(data_root)?;
    let source = original.clone().unwrap_or_default();
    let revision = original.as_deref().map(profiles_edit::revision_of);
    let safe_profiles = profiles_edit::read(data_root, policy)?;
    if !safe_profiles.editable {
        return Err(core::Error::new(
            "not_allowed",
            "settings include a profile section that is not safe to expose",
        ));
    }
    validate_candidate(&source)?;
    store.reconcile_settings_history(revision.as_deref())?;
    let sections = requested.map_or_else(|| all_sections().to_vec(), ToOwned::to_owned);
    let mut values = BTreeMap::new();
    for section in &sections {
        let value = section_json(&source, section_name(*section))?;
        if policy.contains_value(&value) {
            return Err(core::Error::new(
                "not_allowed",
                "a settings section contains a value withheld by the current secret policy",
            ));
        }
        values.insert(*section, value);
    }
    let history = store.list_settings_history(HISTORY_LIMIT)?;
    Ok(SettingsReadResponse {
        revision,
        sections: values,
        restart_required: restart_required(sections),
        history,
    })
}

pub(crate) fn save(
    data_root: &Path,
    store: &mut Store,
    request: &SettingsSaveRequest,
    policy: &SecretPolicy,
) -> core::Result<SettingsSaveResponse> {
    save_internal(
        data_root,
        store,
        request.expected_revision.as_deref(),
        &request.sections,
        SettingsAuthor::Owner(request.client),
        request.dry_run,
        policy,
    )
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
    let original_file = read_config_source(data_root)?;
    let original = original_file.clone().unwrap_or_default();
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
    store.reconcile_settings_history(current_revision.as_deref())?;
    let replacements = replacements(sections, policy)?;
    let candidate = splice_sections(&original, &replacements);
    if candidate.len() > MAX_CONFIG_BYTES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: file would exceed the 128 KiB bound",
        ));
    }
    validate_unselected_sections(&original, &candidate, sections.keys().copied())?;
    validate_candidate(&candidate)?;
    let changed_sections = sections
        .keys()
        .copied()
        .filter(|section| {
            section_json(&original, section_name(*section)).ok()
                != section_json(&candidate, section_name(*section)).ok()
        })
        .collect::<Vec<_>>();
    let restart = restart_required_for_change(&original, &candidate, &changed_sections);
    if dry_run || candidate == original || changed_sections.is_empty() {
        return Ok(SettingsSaveResponse {
            saved: false,
            revision: current_revision,
            changed_sections,
            restart_required: restart,
            history_revision: None,
        });
    }
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
        if value.as_object().is_none() {
            return Err(core::Error::new(
                "invalid",
                "settings sections must be objects",
            ));
        }
        let name = section_name(*section).to_owned();
        if is_default_section(*section, value) || value.as_object().is_some_and(JsonMap::is_empty) {
            result.insert(name, String::new());
            continue;
        }
        let mut normalized = value.clone();
        if *section == SettingsSection::Desktop
            && let Some(window) = normalized.get("window")
            && window.is_null()
        {
            normalized
                .as_object_mut()
                .expect("validated settings object")
                .remove("window");
        }
        let section_value = toml::Value::try_from(normalized).map_err(|_| {
            core::Error::new(
                "profile_error",
                "settings section contains a non-TOML value",
            )
        })?;
        let mut root = toml::map::Map::new();
        root.insert(name.clone(), section_value);
        let source = toml::to_string(&toml::Value::Table(root)).map_err(|_| {
            core::Error::new(
                "profile_error",
                "settings section could not be encoded as TOML",
            )
        })?;
        result.insert(name, source);
    }
    Ok(result)
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
            value == &serde_json::json!({"theme": "system", "window": null})
        }
        _ => false,
    }
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
                if let Some(object) = before.as_object_mut() {
                    object.remove("retention");
                }
                if let Some(object) = after.as_object_mut() {
                    object.remove("retention");
                }
                if before != after {
                    needs_restart.push(*section);
                }
            }
            SettingsSection::Automation | SettingsSection::Mail | SettingsSection::CloudMcp => {
                needs_restart.push(*section)
            }
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
    let config = parse_config(source)?;
    for name in config.profiles.keys() {
        resolve_profile_in(&config, source, name)?;
    }
    project_registrations_in(&config, source)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::SettingsClientSource;
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let id = NEXT.fetch_add(1, Ordering::SeqCst);
            let path =
                std::env::temp_dir().join(format!("eliot-settings-{}-{id}", std::process::id()));
            std::fs::create_dir_all(&path).expect("create settings fixture root");
            Self(path)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn profile_section(model: &str) -> JsonValue {
        serde_json::json!({"codex": {"harness": "codex", "model": model}})
    }

    #[test]
    fn selected_section_splice_preserves_other_bytes() {
        let original = "# preserve\n[daemon]\nattention_suspicion_secs = 900\n\n[profiles.old]\nharness = \"codex\"\nmodel = \"m1\"\n";
        let replacements = BTreeMap::from([(
            "profiles".to_owned(),
            "[profiles.new]\nharness = \"codex\"\nmodel = \"m2\"\n".to_owned(),
        )]);
        let spliced = splice_sections(original, &replacements);
        assert!(spliced.starts_with("# preserve\n[daemon]\nattention_suspicion_secs = 900\n\n"));
        assert!(spliced.contains("[profiles.new]"));
        assert!(!spliced.contains("[profiles.old]"));
    }

    #[test]
    fn dry_run_and_stale_revision_do_not_write() {
        let root = TestRoot::new();
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let policy = SecretPolicy::new(Vec::<String>::new());
        let sections = BTreeMap::from([(SettingsSection::Profiles, profile_section("gpt-6-luna"))]);
        let dry = save_internal(
            &root.0,
            &mut store,
            None,
            &sections,
            SettingsAuthor::Owner(SettingsClientSource::Cli),
            true,
            &policy,
        )
        .expect("dry run validates");
        assert!(!dry.saved);
        assert!(!root.0.join("config.toml").exists());
        assert!(
            store
                .list_settings_history(100)
                .expect("history")
                .is_empty()
        );

        let saved = save_internal(
            &root.0,
            &mut store,
            None,
            &sections,
            SettingsAuthor::Owner(SettingsClientSource::Cli),
            false,
            &policy,
        )
        .expect("save settings");
        assert!(saved.saved);
        let stale = save_internal(
            &root.0,
            &mut store,
            None,
            &BTreeMap::from([(SettingsSection::Profiles, profile_section("different"))]),
            SettingsAuthor::Owner(SettingsClientSource::Cli),
            false,
            &policy,
        )
        .expect_err("stale revision is rejected");
        assert_eq!(stale.code, "conflict");
    }

    #[test]
    fn existing_empty_config_keeps_its_revision_and_default_roundtrip_is_noop() {
        let root = TestRoot::new();
        std::fs::write(root.0.join("config.toml"), "").expect("write empty config");
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let policy = SecretPolicy::new(Vec::<String>::new());
        let read = read_internal(&root.0, &mut store, None, &policy).expect("read empty config");
        let revision = profiles_edit::revision_of("");
        assert_eq!(read.revision.as_deref(), Some(revision.as_str()));
        let result = save_internal(
            &root.0,
            &mut store,
            Some(&revision),
            &BTreeMap::from([
                (SettingsSection::Routing, serde_json::json!({"default": []})),
                (
                    SettingsSection::Desktop,
                    serde_json::json!({"theme": "system", "window": null}),
                ),
            ]),
            SettingsAuthor::Owner(SettingsClientSource::Desktop),
            false,
            &policy,
        )
        .expect("round-trip defaults");
        assert!(!result.saved);
        assert_eq!(result.revision.as_deref(), Some(revision.as_str()));
        assert_eq!(
            std::fs::read_to_string(root.0.join("config.toml")).unwrap(),
            ""
        );
    }

    #[test]
    fn known_secrets_are_rejected_before_replacement() {
        let root = TestRoot::new();
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let policy = SecretPolicy::new(["settings-secret-literal"]);
        let result = save_internal(
            &root.0,
            &mut store,
            None,
            &BTreeMap::from([(
                SettingsSection::Profiles,
                profile_section("settings-secret-literal"),
            )]),
            SettingsAuthor::Owner(SettingsClientSource::Cli),
            false,
            &policy,
        );
        assert_eq!(
            result.expect_err("known secret rejected").code,
            "not_allowed"
        );
        assert!(!root.0.join("config.toml").exists());
    }
}
