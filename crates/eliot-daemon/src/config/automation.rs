//! Automation bounds, assignments and shared project/global Run grants.

use super::{
    MAX_PASS_ENV_ENTRIES, MAX_PROFILE_NAME_LEN, config_error_at,
    configured_route_credential_env_names, native_store_selector, read_config,
    valid_environment_name, validate_profile_name, validate_routing_role,
};
use eliot_core as core;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashSet},
    path::Path,
};

/// Owner-approved interpreter and action-environment grant shared by project
/// registrations and owner-global automation Run.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunGrantConfig {
    #[serde(default)]
    pub(crate) interpreters: BTreeMap<String, core::api::ScriptInterpreter>,
    #[serde(default)]
    pub(crate) run_env: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationConfig {
    #[serde(default = "default_max_event_backlog")]
    pub(crate) max_event_backlog: i64,
    /// An omitted setting is plan-only. Explicit `true` permits age-eligible cleanup.
    #[serde(default)]
    pub(crate) auto_cleanup: Option<bool>,
    /// Minimum age before automatic cleanup, measured from the later of the
    /// artifact creation and the exact terminal generation end.
    #[serde(default = "default_auto_cleanup_after")]
    pub(crate) auto_cleanup_after: String,
    #[serde(default)]
    pub(crate) run: RunGrantConfig,
    #[serde(default)]
    pub(crate) assignments: BTreeMap<String, AssignmentConfig>,
}

/// Settings-owned identity and bounds for a routing-aware backlog assignment.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AssignmentConfig {
    pub(crate) folder: String,
    pub(crate) workspace: String,
    #[serde(default = "default_assignment_role")]
    pub(crate) role: String,
    pub(crate) max_busy: u32,
    pub(crate) interval: String,
    /// Clean up build/temp directories and managed worktrees after an
    /// applied stop retires an assignment Agent, through the existing
    /// completion-rule stop path. Off by default; not a second executor.
    #[serde(default)]
    pub(crate) cleanup_on_retire: bool,
    /// Off pauses the assignment: its launch timer starts no brief, while
    /// the completion rule still retires the Agents already running.
    #[serde(default = "default_assignment_enabled")]
    pub(crate) enabled: bool,
    /// Goal-owned work: each brief's Agent keeps working under this check
    /// and retires when its Goal completes or blocks, not on its first
    /// completed turn. Absent keeps one-turn work.
    #[serde(default)]
    pub(crate) goal: Option<core::api::AssignmentGoal>,
}

impl Default for AutomationConfig {
    fn default() -> Self {
        Self {
            max_event_backlog: default_max_event_backlog(),
            auto_cleanup: None,
            auto_cleanup_after: default_auto_cleanup_after(),
            run: RunGrantConfig::default(),
            assignments: BTreeMap::new(),
        }
    }
}

impl AutomationConfig {
    pub(super) fn validate(
        mut self,
        source: &str,
        route_credential_env_names: &[String],
    ) -> core::Result<Self> {
        if self.max_event_backlog <= 0 {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: automation.max_event_backlog must be a positive integer",
            ));
        }
        if self.auto_cleanup_after.trim() != self.auto_cleanup_after
            || self.auto_cleanup_after.is_empty()
            || self.auto_cleanup_after.len() > 64
            || crate::automation::parse_positive_duration_ms(&self.auto_cleanup_after).is_err()
        {
            return Err(core::Error::new(
                "profile_error",
                "config.toml: automation.auto_cleanup_after must be a positive duration",
            ));
        }
        for (key, assignment) in &self.assignments {
            if key.is_empty()
                || key.len() > MAX_PROFILE_NAME_LEN
                || !key.chars().all(|character| {
                    character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
                })
                || assignment.folder.trim().is_empty()
                || !Path::new(&assignment.folder).is_absolute()
                || !Path::new(&assignment.workspace).is_absolute()
                || assignment.workspace.trim().is_empty()
                || assignment.folder.chars().any(char::is_control)
                || assignment.workspace.chars().any(char::is_control)
                || assignment.max_busy == 0
                || assignment.max_busy > 1000
                || assignment.interval.trim() != assignment.interval
                || assignment.interval.is_empty()
                || crate::automation::parse_positive_duration_ms(&assignment.interval).is_err()
            {
                return Err(core::Error::new(
                    "profile_error",
                    "config.toml: automation assignment has invalid identity, paths, capacity or interval",
                ));
            }
            validate_routing_role(&assignment.role)?;
            if let Some(goal) = &assignment.goal {
                goal.validate().map_err(|error| {
                    core::Error::new(
                        "profile_error",
                        format!("config.toml: automation assignment goal: {}", error.message),
                    )
                })?;
            }
        }
        self.run = normalize_run_grant(
            &self.run.interpreters,
            &self.run.run_env,
            route_credential_env_names,
            source,
            0,
            "automation.run",
        )?;
        Ok(self)
    }
}

fn default_max_event_backlog() -> i64 {
    100_000
}

fn default_auto_cleanup_after() -> String {
    "7d".to_owned()
}

fn default_assignment_enabled() -> bool {
    true
}

fn default_assignment_role() -> String {
    "worker".to_owned()
}

pub(crate) fn load_automation_config(data_root: &Path) -> core::Result<AutomationConfig> {
    read_automation_config(data_root).map(|(config, _)| config)
}

pub(crate) fn load_automation_config_snapshot(
    data_root: &Path,
) -> core::Result<(Option<String>, AutomationConfig)> {
    let (config, source) = read_automation_config(data_root)?;
    let revision = source.as_deref().map(super::profiles_edit::revision_of);
    Ok((revision, config))
}

fn read_automation_config(data_root: &Path) -> core::Result<(AutomationConfig, Option<String>)> {
    let Some((config, source)) = read_config(data_root)? else {
        return Ok((AutomationConfig::default(), None));
    };
    let route_credential_env_names = configured_route_credential_env_names(&config.proxies);
    let config = config
        .automation
        .validate(&source, &route_credential_env_names)?;
    Ok((config, Some(source)))
}

pub(super) fn normalize_run_grant(
    interpreters: &BTreeMap<String, core::api::ScriptInterpreter>,
    run_env: &[String],
    route_credential_env_names: &[String],
    source: &str,
    offset: usize,
    grant_path: &str,
) -> core::Result<RunGrantConfig> {
    let mut normalized = BTreeMap::new();
    for (reference, interpreter) in interpreters {
        let field = format!("{grant_path}.interpreters.{reference}");
        if validate_profile_name(reference).is_err() {
            return Err(config_error_at(
                source,
                offset,
                &field,
                "interpreter reference must use letters, digits, hyphen and underscore",
                "profile_error",
            ));
        }
        interpreter.validate().map_err(|_| {
            config_error_at(
                source,
                offset,
                &field,
                "must specify an absolute program path and a NUL-free argv_prefix",
                "profile_error",
            )
        })?;
        normalized.insert(reference.clone(), interpreter.clone());
    }
    let run_env = normalize_run_env(
        run_env,
        route_credential_env_names,
        source,
        offset,
        &format!("{grant_path}.run_env"),
    )?;
    Ok(RunGrantConfig {
        interpreters: normalized,
        run_env,
    })
}

fn normalize_run_env(
    names: &[String],
    route_credential_env_names: &[String],
    source: &str,
    offset: usize,
    field: &str,
) -> core::Result<Vec<String>> {
    if names.len() > MAX_PASS_ENV_ENTRIES {
        return Err(config_error_at(
            source,
            offset,
            field,
            "exceeds 64 entries",
            "profile_error",
        ));
    }

    let mut normalized = Vec::with_capacity(names.len());
    let mut seen = HashSet::with_capacity(names.len());
    for raw in names {
        let name = raw.trim();
        if !valid_environment_name(name) {
            return Err(config_error_at(
                source,
                offset,
                field,
                "names must use 1-256 letters, digits and underscores, starting with a letter or underscore",
                "profile_error",
            ));
        }
        if forbidden_run_env_name(name, route_credential_env_names) {
            return Err(config_error_at(
                source,
                offset,
                field,
                "cannot include identity, credential, route, native-store, Git or reserved environment names",
                "profile_error",
            ));
        }
        let key = if cfg!(windows) {
            name.to_ascii_uppercase()
        } else {
            name.to_owned()
        };
        if !seen.insert(key) {
            return Err(config_error_at(
                source,
                offset,
                field,
                "contains a duplicate environment name",
                "profile_error",
            ));
        }
        normalized.push(name.to_owned());
    }
    normalized.sort();
    Ok(normalized)
}

pub(super) fn forbidden_run_env_name(name: &str, route_credential_env_names: &[String]) -> bool {
    let uppercase = name.to_ascii_uppercase();
    uppercase.starts_with("ELIOT_")
        || uppercase.starts_with("GIT_")
        || env_names_equal(name, "PATH")
        || native_store_selector(name).is_some()
        || route_credential_env_names
            .iter()
            .any(|credential| env_names_equal(name, credential))
        || uppercase.split('_').any(|part| {
            matches!(
                part,
                "ROLE" | "TOKEN" | "CREDENTIAL" | "CREDENTIALS" | "PASSWORD" | "SECRET" | "AUTH"
            )
        })
        || [
            "_API_KEY",
            "_ACCESS_KEY",
            "_PRIVATE_KEY",
            "_BASE_URL",
            "_ENDPOINT",
            "_ROUTE",
            "_ACCOUNT",
        ]
        .iter()
        .any(|suffix| uppercase.ends_with(suffix))
}

fn env_names_equal(left: &str, right: &str) -> bool {
    if cfg!(windows) {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}
