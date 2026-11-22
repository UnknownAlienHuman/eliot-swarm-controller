//! Project registrations, approved actions and chat workspace configuration.

use super::automation::normalize_run_grant;
use super::{
    FileConfig, MAX_PROFILE_NAME_LEN, MAX_PROFILES, config_error_at,
    configured_route_credential_env_names, read_config, validate_profile_name,
};
use crate::command_guard::ProjectCommandGuard;
use eliot_core as core;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashSet},
    path::{Path, PathBuf},
};

const MAX_PROJECT_PATH_LEN: usize = 4096;
const MAX_PROJECT_CHECK_ROUNDS: i64 = 16;
const DEFAULT_PROJECT_CHECK_ROUNDS: i64 = 3;

/// Managed-worktree policy for top-level launches in a registered project.
/// `worktree` (the default) prepares one standard worktree per top-level
/// launch without any flag; `shared` keeps sharing the registered checkout.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProjectWorkspaceMode {
    #[default]
    Worktree,
    Shared,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectRegistrationConfig {
    pub id: String,
    pub path: String,
    pub profiles: Vec<String>,
    pub actions: Vec<String>,
    pub git: Option<core::api::ProjectGitView>,
    pub interpreters: BTreeMap<String, core::api::ScriptInterpreter>,
    pub run_env: Vec<String>,
    pub format: Vec<String>,
    pub policy: Option<ProjectPolicyConfig>,
    pub command_guards: Vec<ProjectCommandGuard>,
    pub workspace: ProjectWorkspaceMode,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ProjectPolicyConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub check: Option<ProjectPolicyCheck>,
    pub budget: ProjectPolicyBudget,
    pub scope: Vec<String>,
    pub check_inputs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectPolicyCheck {
    pub argv: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "project_policy_false")]
    pub check_timeout_retry_safe: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at_turn_end: Option<bool>,
}

impl ProjectPolicyCheck {
    /// Project checks outside a Goal round use the fixed default.
    pub(crate) fn fixed_deadline_ms(&self) -> i64 {
        self.deadline_ms
            .unwrap_or(core::api::DEFAULT_GOAL_CHECK_TIMEOUT_MS)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProjectPolicyBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rounds: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_cost: Option<core::api::GoalCostLimit>,
}

/// The settings boundary also accepts the former top-level #879 fields so it
/// can migrate them into the canonical policy before saving the section.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
struct ProjectPolicySettingsInput {
    policy: Option<ProjectPolicyConfig>,
    check: Vec<String>,
    check_at_turn_end: bool,
    max_rounds: Option<i64>,
}

fn project_policy_false(value: &bool) -> bool {
    !value
}

/// Default chat root for one data dir. The Owner's default data dir keeps
/// the historical `~/Eliot/chats`; every other data dir (including a
/// private `--data-dir` daemon) gets a root specific to that dir that is
/// never under the owner's canonical `~/Eliot/chats` tree: a
/// `<data-dir>-chats` sibling when the protected-path checks accept it,
/// otherwise a deterministic `~/Eliot/chats-<name>-<hash>` fallback.
/// An explicit `daemon.chats_root` always wins; when no default passes,
/// the error asks for one.
pub(crate) fn default_chats_root(data_root: &Path) -> core::Result<std::path::PathBuf> {
    if is_owner_default_data_dir(data_root) {
        return owner_chats_root();
    }
    let protected = protected_paths(data_root)?;
    if let Some(sibling) = sibling_chats_root(data_root)
        && validate_chats_root(&sibling, &protected).is_ok()
    {
        return Ok(sibling);
    }
    let fallback = home_namespaced_chats_root(data_root)?;
    validate_chats_root(&fallback, &protected)?;
    Ok(fallback)
}

fn owner_chats_root() -> core::Result<std::path::PathBuf> {
    let home =
        std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).ok_or_else(|| {
            core::Error::new("profile_error", "user home is unavailable for chats_root")
        })?;
    Ok(std::path::PathBuf::from(home).join("Eliot").join("chats"))
}

/// Deterministic per-data-dir fallback beside (never under) the owner's
/// chats tree, for data dirs whose sibling would overlap a protected root.
fn home_namespaced_chats_root(data_root: &Path) -> core::Result<std::path::PathBuf> {
    let home =
        std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" }).ok_or_else(|| {
            core::Error::new(
                "profile_error",
                "data dir needs an explicit disjoint daemon.chats_root",
            )
        })?;
    let absolute = std::path::absolute(data_root).unwrap_or_else(|_| data_root.to_owned());
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in absolute.to_string_lossy().bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let raw = data_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("data");
    let mut sanitized: String = raw
        .chars()
        .map(|cell| {
            if cell.is_ascii_alphanumeric() {
                cell
            } else {
                '_'
            }
        })
        .take(32)
        .collect();
    if sanitized.is_empty() {
        sanitized.push_str("data");
    }
    Ok(std::path::PathBuf::from(home)
        .join("Eliot")
        .join(format!("chats-{sanitized}-{hash:016x}")))
}

fn sibling_chats_root(data_root: &Path) -> Option<std::path::PathBuf> {
    let name = data_root.file_name()?.to_str()?;
    if name.is_empty() {
        return None;
    }
    let parent = data_root
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())?;
    Some(parent.join(format!("{name}-chats")))
}

fn is_owner_default_data_dir(data_root: &Path) -> bool {
    let Ok(default) = crate::platform::data_dir::default_data_dir() else {
        return false;
    };
    std::path::absolute(data_root).ok() == std::path::absolute(&default).ok()
}

pub(crate) fn validate_chats_location(
    data_root: &Path,
    chats_root: Option<&str>,
) -> core::Result<std::path::PathBuf> {
    let explicit = chats_root.map(std::path::PathBuf::from);
    let root = match &explicit {
        Some(root) => root.clone(),
        None => default_chats_root(data_root)?,
    };
    let mut protected = protected_paths(data_root)?;
    // An Owner may explicitly choose the canonical chat tree itself for a
    // non-default data dir (a production controller dir, for example).
    // Derived defaults and every other explicit root stay outside it.
    if explicit.is_some() && !is_owner_default_data_dir(data_root) {
        let owner = owner_chats_root()?;
        if same_absolute_path(&root, &owner) {
            protected.retain(|path| !same_absolute_path(path, &owner));
        }
    }
    validate_chats_root(&root, &protected)
}

fn same_absolute_path(left: &Path, right: &Path) -> bool {
    match (std::path::absolute(left), std::path::absolute(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => false,
    }
}

/// Every location a chat root must stay disjoint from: the daemon data
/// dir, the canonical owner chat tree for non-default data dirs,
/// credential and configuration homes. Shared by the explicit path and
/// every automatic default. When the owner home cannot be resolved for a
/// non-default data dir, this fails closed instead of omitting the owner
/// tree from the protected set.
fn protected_paths(data_root: &Path) -> core::Result<Vec<PathBuf>> {
    let mut protected = vec![data_root.to_owned()];
    if !is_owner_default_data_dir(data_root) {
        protected.push(owner_chats_root()?);
    }
    for name in [
        "CODEX_HOME",
        "CODEX_SQLITE_HOME",
        "CLAUDE_CONFIG_DIR",
        "MUSE_CONFIG_DIR",
        "OPENCODE_CONFIG_DIR",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "APPDATA",
        "LOCALAPPDATA",
    ] {
        if let Some(path) = std::env::var_os(name).filter(|path| !path.is_empty()) {
            protected.push(PathBuf::from(path));
        }
    }
    for name in ["USERPROFILE", "HOME"] {
        if let Some(home) = std::env::var_os(name).filter(|path| !path.is_empty()) {
            let home = PathBuf::from(home);
            for relative in [
                ".codex",
                ".claude",
                ".config",
                ".local/share",
                ".gemini",
                ".antigravity",
                ".agy",
                ".muse",
            ] {
                protected.push(home.join(relative));
            }
            #[cfg(windows)]
            protected.push(home.join("AppData"));
        }
    }
    Ok(protected)
}

fn validate_chats_root(root: &Path, protected: &[PathBuf]) -> core::Result<PathBuf> {
    let refused = || {
        core::Error::new(
            "profile_error",
            "chats_root overlaps protected data/configuration or traverses a link",
        )
    };
    if !root.is_absolute() {
        return Err(refused());
    }
    // Inspect the supplied spelling before canonicalization can erase a link
    // or junction. Nonexistent suffixes are allowed for exclusive creation.
    let mut prefix = PathBuf::new();
    for component in root.components() {
        match component {
            std::path::Component::ParentDir | std::path::Component::CurDir => return Err(refused()),
            #[cfg(windows)]
            std::path::Component::Normal(name)
                if name
                    .to_str()
                    .is_none_or(|name| name.ends_with(['.', ' ']) || name.contains(':')) =>
            {
                return Err(refused());
            }
            #[cfg(windows)]
            std::path::Component::Prefix(p)
                if !matches!(
                    p.kind(),
                    std::path::Prefix::Disk(_) | std::path::Prefix::VerbatimDisk(_)
                ) =>
            {
                return Err(refused());
            }
            _ => prefix.push(component),
        }
        match std::fs::symlink_metadata(&prefix) {
            Ok(metadata)
                if metadata.file_type().is_symlink()
                    || crate::platform::is_reparse_point(&metadata)
                    || !metadata.is_dir() =>
            {
                return Err(refused());
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(refused()),
        }
    }
    let resolve = |path: &Path| -> core::Result<PathBuf> {
        let mut existing = std::path::absolute(path).map_err(|_| refused())?;
        let mut suffix = Vec::new();
        loop {
            match std::fs::canonicalize(&existing) {
                Ok(mut resolved) => {
                    for name in suffix.into_iter().rev() {
                        resolved.push(name);
                    }
                    return Ok(resolved);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    suffix.push(existing.file_name().ok_or_else(refused)?.to_owned());
                    if !existing.pop() {
                        return Err(refused());
                    }
                }
                Err(_) => return Err(refused()),
            }
        }
    };
    let root = resolve(root)?;
    let normalized = |path: &Path| {
        let text = path.to_string_lossy().replace('\\', "/");
        let text = text.strip_prefix("//?/").unwrap_or(&text);
        std::path::PathBuf::from(if cfg!(windows) {
            text.to_lowercase()
        } else {
            text.to_owned()
        })
    };
    let normalized_root = normalized(&root);
    for path in protected {
        let protected = normalized(&resolve(path)?);
        if normalized_root.starts_with(&protected) || protected.starts_with(&normalized_root) {
            return Err(refused());
        }
    }
    Ok(root)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProjectEntry {
    path: String,
    #[serde(default)]
    profiles: Vec<String>,
    #[serde(default)]
    actions: Vec<String>,
    #[serde(default)]
    git: Option<core::api::ProjectGitView>,
    #[serde(default)]
    interpreters: BTreeMap<String, core::api::ScriptInterpreter>,
    #[serde(default)]
    run_env: Vec<String>,
    /// Explicit argv run after a debounced file edit to format the Agent worktree.
    #[serde(default)]
    format: Vec<String>,
    /// Legacy #879 input. Settings read/save migrate this into `policy.check.argv`.
    #[serde(default)]
    check: Vec<String>,
    /// Legacy #879 input. Settings read/save migrate this into `policy.check`.
    #[serde(default)]
    check_at_turn_end: bool,
    /// Legacy #879 input. Settings read/save migrate this into `policy.budget`.
    #[serde(default)]
    max_rounds: Option<i64>,
    #[serde(default)]
    policy: Option<ProjectPolicyConfig>,
    #[serde(default)]
    command_guards: Vec<ProjectCommandGuard>,
    /// `workspace = "worktree" | "shared"`, default `worktree`.
    #[serde(default)]
    workspace: ProjectWorkspaceMode,
}

/// Read only owner-configured project registrations. Paths and Git identity are
/// canonicalized by `automation::projects` outside Store transactions.
pub(crate) fn load_project_registration_config(
    data_root: &Path,
) -> core::Result<Vec<ProjectRegistrationConfig>> {
    let Some((config, source)) = read_config(data_root)? else {
        return Ok(Vec::new());
    };
    project_registrations_in(&config, &source)
}

/// Owner display name for a project launch: the first `[desktop]
/// display_names` entry matching `candidate_keys` in order
/// (`project:repo:<common-dir>`, `project:<registration-id>`,
/// `project:folder:<workspace>`). Absent config or no match reads `None`.
pub(crate) fn load_project_display_name(
    data_root: &Path,
    candidate_keys: &[String],
) -> Option<String> {
    let (config, _) = read_config(data_root).ok()??;
    config.desktop_project_name(candidate_keys)
}

/// Project registration validation over already parsed source text (also
/// run on an edited profile draft: a project may not keep naming a profile
/// the draft removed).
pub(super) fn project_registrations_in(
    config: &FileConfig,
    source: &str,
) -> core::Result<Vec<ProjectRegistrationConfig>> {
    if config.projects.is_empty() {
        return Ok(Vec::new());
    }
    if config.profiles.len() > MAX_PROFILES {
        return Err(core::Error::new(
            "profile_error",
            "config.toml: profiles: profile count exceeds 256 entries",
        ));
    }

    let global_profiles: HashSet<&str> = config.profiles.keys().map(String::as_str).collect();
    let route_credential_env_names = configured_route_credential_env_names(&config.proxies);
    let mut projects = Vec::with_capacity(config.projects.len());
    for (id, spanned) in &config.projects {
        let entry = spanned.get_ref();
        let offset = spanned.span().start;
        if validate_profile_name(id).is_err() {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{id}"),
                "invalid project id",
                "profile_error",
            ));
        }

        let path = entry.path.trim();
        if path.is_empty()
            || path.len() > MAX_PROJECT_PATH_LEN
            || path.chars().any(char::is_control)
            || !Path::new(path).is_absolute()
        {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{id}.path"),
                "must be a nonempty absolute checkout path of at most 4096 characters",
                "profile_error",
            ));
        }

        let profiles = normalize_project_allowlist(
            &entry.profiles,
            source,
            offset,
            id,
            "profiles",
            |profile| global_profiles.contains(profile) && validate_profile_name(profile).is_ok(),
        )?;
        let actions =
            normalize_project_allowlist(&entry.actions, source, offset, id, "actions", |action| {
                action == eliot_core::api::PROJECT_ASSIGNMENT_CLAIM_ACTION
                    || matches!(
                        action,
                        "launch"
                            | "resume"
                            | "stop"
                            | "send"
                            | "git.fetch"
                            | "git.integrate"
                            | "git.publish"
                            | "steer"
                            | "interrupt"
                            | "approve"
                            | "deny"
                            | "answer"
                            | "run"
                            | "notify"
                            | "emit"
                            | "mail.send"
                            | "var.set"
                    )
            })?;
        let format = normalize_project_command_argv(&entry.format, source, offset, id, "format")?;
        let policy = normalize_project_policy(
            id,
            entry.policy.clone(),
            &entry.check,
            entry.check_at_turn_end,
            entry.max_rounds,
            source,
            offset,
        )?;
        let git = entry.git.clone();
        if let Some(git) = &git {
            validate_project_git_config(git, source, offset, id)?;
        }
        let run_grant = normalize_run_grant(
            &entry.interpreters,
            &entry.run_env,
            &route_credential_env_names,
            source,
            offset,
            &format!("projects.{id}"),
        )?;
        validate_project_command_guards(id, &entry.command_guards, source, offset)?;

        projects.push(ProjectRegistrationConfig {
            id: id.clone(),
            path: path.to_owned(),
            profiles,
            actions,
            git,
            interpreters: run_grant.interpreters,
            run_env: run_grant.run_env,
            format,
            policy,
            command_guards: entry.command_guards.clone(),
            workspace: entry.workspace,
        });
    }
    projects.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(projects)
}

/// Normalize the canonical policy and the legacy #879 project fields into
/// one typed value. When both forms are present, canonical policy fields win.
fn normalize_project_policy(
    id: &str,
    policy: Option<ProjectPolicyConfig>,
    legacy_check: &[String],
    legacy_at_turn_end: bool,
    legacy_max_rounds: Option<i64>,
    source: &str,
    offset: usize,
) -> core::Result<Option<ProjectPolicyConfig>> {
    let has_legacy = !legacy_check.is_empty() || legacy_at_turn_end || legacy_max_rounds.is_some();
    if policy.is_none() && !has_legacy {
        return Ok(None);
    }

    let policy = policy.unwrap_or_default();
    let legacy_check_used = policy.check.is_none() && !legacy_check.is_empty();
    let mut check = match policy.check {
        Some(mut check) => {
            if check.at_turn_end.is_none() && legacy_at_turn_end {
                check.at_turn_end = Some(true);
            }
            Some(check)
        }
        None if !legacy_check.is_empty() => Some(ProjectPolicyCheck {
            argv: legacy_check.to_vec(),
            deadline_ms: None,
            check_timeout_retry_safe: false,
            at_turn_end: legacy_at_turn_end.then_some(true),
        }),
        None => None,
    };

    if let Some(configured) = &mut check {
        let field = if legacy_check_used {
            "check"
        } else {
            "policy.check.argv"
        };
        if configured.argv.is_empty() {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{id}.{field}"),
                "must contain 1 to 32 nonempty arguments",
                "profile_error",
            ));
        }
        configured.argv =
            normalize_project_command_argv(&configured.argv, source, offset, id, field)?;
        if configured.deadline_ms.is_some_and(|deadline_ms| {
            !(core::api::MIN_GOAL_CHECK_TIMEOUT_MS..=core::api::MAX_GOAL_CHECK_TIMEOUT_MS)
                .contains(&deadline_ms)
        }) {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{id}.policy.check.deadline_ms"),
                "must be between 1000 and 600000 milliseconds",
                "profile_error",
            ));
        }
    }

    let legacy_rounds_used = policy.budget.max_rounds.is_none() && legacy_max_rounds.is_some();
    let mut max_rounds = policy.budget.max_rounds.or(legacy_max_rounds);
    if check.is_none() {
        let check_required_by = if legacy_rounds_used {
            Some("max_rounds")
        } else if policy.budget.max_rounds.is_some() {
            Some("policy.budget.max_rounds")
        } else if policy.budget.max_cost.is_some() {
            Some("policy.budget.max_cost")
        } else if !policy.scope.is_empty() {
            Some("policy.scope")
        } else if !policy.check_inputs.is_empty() {
            Some("policy.check_inputs")
        } else {
            None
        };
        if let Some(field) = check_required_by {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{id}.{field}"),
                "requires a project check command",
                "profile_error",
            ));
        }
    }
    if check.is_some() && max_rounds.is_none() {
        max_rounds = Some(DEFAULT_PROJECT_CHECK_ROUNDS);
    }
    if max_rounds.is_some_and(|rounds| !(1..=MAX_PROJECT_CHECK_ROUNDS).contains(&rounds)) {
        let field = if legacy_rounds_used {
            "max_rounds"
        } else {
            "policy.budget.max_rounds"
        };
        return Err(config_error_at(
            source,
            offset,
            &format!("projects.{id}.{field}"),
            "must be between 1 and 16",
            "profile_error",
        ));
    }

    if let Some(limit) = &policy.budget.max_cost {
        limit.validate().map_err(|error| {
            config_error_at(
                source,
                offset,
                &format!("projects.{id}.policy.budget.max_cost"),
                &error.message,
                "profile_error",
            )
        })?;
    }

    for (name, globs) in [
        ("scope", &policy.scope),
        ("check_inputs", &policy.check_inputs),
    ] {
        if globs.len() > core::api::MAX_GOAL_SCOPE_GLOBS
            || globs
                .iter()
                .any(|glob| core::api::normalize_claim_glob(glob).is_err())
        {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{id}.policy.{name}"),
                "must contain at most 32 valid project-relative globs",
                "profile_error",
            ));
        }
    }

    Ok(Some(ProjectPolicyConfig {
        check,
        budget: ProjectPolicyBudget {
            max_rounds,
            max_cost: policy.budget.max_cost,
        },
        scope: policy.scope,
        check_inputs: policy.check_inputs,
    }))
}

/// Convert a projects settings value to its one canonical policy shape.
/// Invalid data remains readable for repair, but cannot be saved in legacy
/// form or silently lose fields.
pub(super) fn canonicalize_project_settings(
    value: &mut serde_json::Value,
    strict: bool,
) -> core::Result<()> {
    let Some(projects) = value.as_object_mut() else {
        return Err(core::Error::new(
            "invalid",
            "projects settings must be an object",
        ));
    };

    for (id, project_value) in projects {
        let input = serde_json::from_value::<ProjectPolicySettingsInput>(project_value.clone());
        let Some(project) = project_value.as_object_mut() else {
            if strict {
                return Err(core::Error::new(
                    "profile_error",
                    format!("projects.{id} must be a table"),
                ));
            }
            continue;
        };
        let normalized = input
            .map_err(|_| {
                core::Error::new(
                    "profile_error",
                    format!("projects.{id} policy fields are invalid"),
                )
            })
            .and_then(|input| {
                normalize_project_policy(
                    id,
                    input.policy,
                    &input.check,
                    input.check_at_turn_end,
                    input.max_rounds,
                    "",
                    0,
                )
            });
        let normalized = match normalized {
            Ok(normalized) => normalized,
            Err(_) if !strict => continue,
            Err(error) => return Err(error),
        };

        project.remove("check");
        project.remove("check_at_turn_end");
        project.remove("max_rounds");
        match normalized {
            Some(policy) => {
                let policy = serde_json::to_value(policy).map_err(|_| {
                    core::Error::new("internal", "project policy could not be encoded")
                })?;
                project.insert("policy".to_owned(), policy);
            }
            None => {
                project.remove("policy");
            }
        }
    }
    Ok(())
}

fn normalize_project_command_argv(
    argv: &[String],
    source: &str,
    offset: usize,
    project_id: &str,
    field: &str,
) -> core::Result<Vec<String>> {
    if argv.is_empty() {
        return Ok(Vec::new());
    }
    if argv.len() > core::api::MAX_GOAL_CHECK_ARGS
        || argv.iter().any(|arg| {
            arg.is_empty()
                || arg.len() > core::api::MAX_GOAL_CHECK_ARG_BYTES
                || arg.chars().any(char::is_control)
        })
    {
        return Err(config_error_at(
            source,
            offset,
            &format!("projects.{project_id}.{field}"),
            "must be explicit argv with 1 to 32 nonempty arguments of at most 4 KiB and no control characters",
            "profile_error",
        ));
    }
    Ok(argv.to_vec())
}

fn validate_project_command_guards(
    project_id: &str,
    entries: &[ProjectCommandGuard],
    source: &str,
    offset: usize,
) -> core::Result<()> {
    let field = format!("projects.{project_id}.command_guards");
    if entries.len() > crate::command_guard::MAX_PROJECT_GUARDS {
        return Err(config_error_at(
            source,
            offset,
            &field,
            "exceeds 64 entries",
            "profile_error",
        ));
    }
    for entry in entries {
        if entry.pattern.is_empty()
            || entry.pattern.len() > crate::command_guard::MAX_PATTERN_BYTES
            || entry.pattern.chars().any(char::is_control)
            || globset::Glob::new(&entry.pattern).is_err()
        {
            return Err(config_error_at(
                source,
                offset,
                &field,
                "patterns must be valid nonempty globs of at most 1024 bytes",
                "profile_error",
            ));
        }
        if entry.refusal.trim().is_empty()
            || entry.refusal.len() > crate::command_guard::MAX_REFUSAL_BYTES
            || entry.refusal.chars().any(char::is_control)
        {
            return Err(config_error_at(
                source,
                offset,
                &field,
                "refusal text must be a nonempty single line of at most 512 bytes",
                "profile_error",
            ));
        }
    }
    Ok(())
}

fn normalize_project_allowlist(
    values: &[String],
    source: &str,
    offset: usize,
    project_id: &str,
    field: &str,
    allowed: impl Fn(&str) -> bool,
) -> core::Result<Vec<String>> {
    let mut normalized = Vec::with_capacity(values.len());
    let mut seen = HashSet::with_capacity(values.len());
    for raw in values {
        let value = raw.trim();
        if value.is_empty() || value.len() > MAX_PROFILE_NAME_LEN || !allowed(value) {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{project_id}.{field}"),
                if field == "actions" {
                    "only implemented command and project assignment actions may be approved"
                } else {
                    "must name an existing global profile"
                },
                "profile_error",
            ));
        }
        if !seen.insert(value.to_owned()) {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{project_id}.{field}"),
                "contains a duplicate entry",
                "profile_error",
            ));
        }
        normalized.push(value.to_owned());
    }
    normalized.sort();
    Ok(normalized)
}

fn validate_project_git_config(
    git: &core::api::ProjectGitView,
    source: &str,
    offset: usize,
    project_id: &str,
) -> core::Result<()> {
    git.validate().map_err(|_| {
        config_error_at(
            source,
            offset,
            &format!("projects.{project_id}.git"),
            "contains an invalid Git fetch approval",
            "profile_error",
        )
    })?;
    for (remote, endpoint) in &git.fetch_remotes {
        if !is_direct_project_fetch_endpoint(endpoint) {
            return Err(config_error_at(
                source,
                offset,
                &format!("projects.{project_id}.git.fetch_remotes.{remote}"),
                "must be an absolute local path or direct HTTP, HTTPS, Git, or file URL without credentials or helper schemes; SSH is unsupported",
                "profile_error",
            ));
        }
    }
    if let Some(publication) = &git.publication {
        for (remote, endpoint) in &publication.remotes {
            if !is_direct_project_fetch_endpoint(endpoint) {
                return Err(config_error_at(
                    source,
                    offset,
                    &format!("projects.{project_id}.git.publication.remotes.{remote}"),
                    "must be an absolute local path or direct HTTP, HTTPS, Git, or file URL without credentials or helper schemes; SSH is unsupported",
                    "profile_error",
                ));
            }
        }
    }
    Ok(())
}

fn is_direct_project_fetch_endpoint(endpoint: &str) -> bool {
    if endpoint.trim() != endpoint
        || endpoint.starts_with('-')
        || endpoint.len() > MAX_PROJECT_PATH_LEN
        || endpoint.chars().any(char::is_control)
        || has_git_remote_helper_delimiter(endpoint)
    {
        return false;
    }
    if Path::new(endpoint).is_absolute() {
        return true;
    }
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return false;
    };
    matches!(url.scheme(), "file" | "git" | "http" | "https")
        && url.username().is_empty()
        && url.password().is_none()
        && !project_git_endpoint_has_userinfo(endpoint)
        && url.query().is_none()
        && url.fragment().is_none()
        && (url.scheme() == "file" || url.host_str().is_some_and(|host| !host.is_empty()))
}

fn project_git_endpoint_has_userinfo(endpoint: &str) -> bool {
    let Some((_, rest)) = endpoint.split_once("://") else {
        return false;
    };
    rest.split(['/', '?', '#'])
        .next()
        .is_some_and(|authority| authority.contains('@'))
}

fn has_git_remote_helper_delimiter(endpoint: &str) -> bool {
    endpoint
        .find("::")
        .is_some_and(|delimiter| endpoint.find("://").is_none_or(|scheme| delimiter < scheme))
}
