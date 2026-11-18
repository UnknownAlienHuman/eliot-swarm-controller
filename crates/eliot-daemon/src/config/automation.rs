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
}

impl Default for AutomationConfig {
    fn default() -> Self {
        Self {
            max_event_backlog: default_max_event_backlog(),
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

#[cfg(test)]
mod tests {
    use super::super::{
        ProjectRegistrationConfig, load_project_registration_config, tests::AutomationConfigFixture,
    };
    use super::*;

    #[test]
    fn automation_backlog_defaults_and_coexists_with_project_registrations() {
        let fixture = AutomationConfigFixture::new();
        assert_eq!(
            load_automation_config(&fixture.root).unwrap(),
            AutomationConfig::default()
        );

        fixture.write("[automation]\n");
        assert_eq!(
            load_automation_config(&fixture.root).unwrap(),
            AutomationConfig::default()
        );

        let project_path = format!("{:?}", fixture.root.to_string_lossy());
        fixture.write(&format!(
            "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\n\
             [projects.sample]\npath = {project_path}\nprofiles = ['fixture']\nactions = ['stop']\n\
             [automation]\nmax_event_backlog = 4096\n"
        ));
        assert_eq!(
            load_automation_config(&fixture.root).unwrap(),
            AutomationConfig {
                max_event_backlog: 4096,
                run: RunGrantConfig::default(),
                assignments: BTreeMap::new(),
            }
        );
        assert_eq!(
            load_project_registration_config(&fixture.root).unwrap(),
            vec![ProjectRegistrationConfig {
                id: "sample".to_owned(),
                path: fixture.root.to_string_lossy().into_owned(),
                profiles: vec!["fixture".to_owned()],
                actions: vec!["stop".to_owned()],
                git: None,
                interpreters: BTreeMap::new(),
                run_env: Vec::new(),
            }]
        );
    }

    #[test]
    fn automation_assignments_validate_and_load_from_settings_shape() {
        let fixture = AutomationConfigFixture::new();
        fixture.write(
            "[automation.assignments.review-queue]\n\
             folder='C:/work/briefs'\nworkspace='C:/work/eliot'\n\
             role='worker'\nmax_busy=2\ninterval='30s'\n",
        );
        let config = load_automation_config(&fixture.root).unwrap();
        let assignment = config.assignments.get("review-queue").unwrap();
        assert_eq!(assignment.folder, "C:/work/briefs");
        assert_eq!(assignment.workspace, "C:/work/eliot");
        assert_eq!(assignment.role, "worker");
        assert_eq!(assignment.max_busy, 2);
        assert_eq!(assignment.interval, "30s");
        assert!(
            !assignment.cleanup_on_retire,
            "retirement cleanup stays off unless the assignment opts in"
        );

        fixture.write(
            "[automation.assignments.review-queue]\n\
             folder='C:/work/briefs'\nworkspace='C:/work/eliot'\n\
             role='worker'\nmax_busy=2\ninterval='30s'\ncleanup_on_retire=true\n",
        );
        let opted = load_automation_config(&fixture.root)
            .unwrap()
            .assignments
            .remove("review-queue")
            .unwrap();
        assert!(opted.cleanup_on_retire);

        fixture.write(
            "[automation.assignments.review-queue]\n\
             folder='C:/work/briefs'\nworkspace='C:/work/eliot'\n\
             role='worker'\nmax_busy=0\ninterval='30s'\n",
        );
        assert!(load_automation_config(&fixture.root).is_err());
    }

    #[test]
    fn global_run_configuration_defaults_to_denial_and_keeps_literal_grants() {
        let fixture = AutomationConfigFixture::new();
        let denied = load_automation_config(&fixture.root).unwrap();
        assert!(denied.run.interpreters.is_empty());
        assert!(denied.run.run_env.is_empty());

        fixture.write("[automation.run]\nrun_env = [\"CI\"]\n");
        let no_interpreter = load_automation_config(&fixture.root).unwrap();
        assert!(no_interpreter.run.interpreters.is_empty());
        assert_eq!(no_interpreter.run.run_env, vec!["CI"]);

        let program = std::env::current_exe()
            .expect("current executable path")
            .to_string_lossy()
            .into_owned();
        fixture.write(
            "[automation.run]\n\
             [automation.run.interpreters.fixture]\n\
             program = \"relative-program\"\nargv_prefix = [\"--literal\"]\n",
        );
        assert!(load_automation_config(&fixture.root).is_err());

        fixture.write(&format!(
            "[automation.run]\nrun_env = [\"PATH\"]\n\
             [automation.run.interpreters.fixture]\nprogram = {program:?}\n\
             argv_prefix = [\"--literal\", \"fixed-value\"]\n"
        ));
        assert!(load_automation_config(&fixture.root).is_err());

        fixture.write(&format!(
            "[automation.run]\nrun_env = [\"CI\"]\n\
             [automation.run.interpreters.fixture]\nprogram = {program:?}\n\
             argv_prefix = [\"--literal\", \"fixed-value\"]\n"
        ));
        let configured = load_automation_config(&fixture.root).unwrap();
        assert_eq!(
            configured.run,
            RunGrantConfig {
                interpreters: BTreeMap::from([(
                    "fixture".to_owned(),
                    core::api::ScriptInterpreter {
                        program: program.clone(),
                        argv_prefix: vec!["--literal".to_owned(), "fixed-value".to_owned()],
                    },
                )]),
                run_env: vec!["CI".to_owned()],
            }
        );

        fixture.write(&format!(
            "[automation.run.interpreters.fixture]\nprogram = {program:?}\n\
             argv_prefix = [\"--literal\"]\n"
        ));
        let no_action_env = load_automation_config(&fixture.root).unwrap();
        assert!(no_action_env.run.run_env.is_empty());

        fixture.write("[automation.run]\nrun_env = [\"GITHUB_TOKEN\"]\n");
        assert!(load_automation_config(&fixture.root).is_err());
    }

    #[test]
    fn automation_backlog_rejects_nonpositive_wrong_type_and_unknown_fields() {
        let fixture = AutomationConfigFixture::new();
        for source in [
            "[automation]\nmax_event_backlog = 0\n",
            "[automation]\nmax_event_backlog = -1\n",
            "[automation]\nmax_event_backlog = '1000'\n",
            "[automation]\nfuture_setting = 1\n",
        ] {
            fixture.write(source);
            let error = load_automation_config(&fixture.root).unwrap_err();
            assert_eq!(error.code, "profile_error", "accepted {source:?}");
        }
    }
}
