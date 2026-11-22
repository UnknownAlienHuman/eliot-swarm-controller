//! Project event-rule run freezing and pre-spawn verification.
//!
//! This module resolves owner-approved inputs and builds an explicit child command.
//! The caller owns Router/Store admission, process custody, Run identity injection,
//! stdout validation and action-child admission.

use eliot_core::{
    Error, FrozenScriptContext, FrozenScriptInterpreter, FrozenScriptRun, FrozenSetupFile, Result,
    RunDefinitionArgs, SecretPolicy,
    api::{ProjectRegistrationView, ScriptInterpreter},
};
use std::{
    collections::{BTreeMap, HashSet},
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::process::Command;

#[derive(Clone, Copy)]
enum CwdPolicy {
    Project,
    Owner,
}

struct RunScope<'a> {
    interpreters: &'a BTreeMap<String, ScriptInterpreter>,
    run_env: &'a [String],
    entry_base: &'a Path,
    entry_root: &'a Path,
    cwd_policy: CwdPolicy,
}

impl<'a> RunScope<'a> {
    fn project(
        registration: &'a ProjectRegistrationView,
        checkout: &'a Path,
        scripts_root: &'a Path,
    ) -> Self {
        Self {
            interpreters: &registration.interpreters,
            run_env: &registration.run_env,
            entry_base: checkout,
            entry_root: scripts_root,
            cwd_policy: CwdPolicy::Project,
        }
    }

    fn owner(grant: &'a crate::config::RunGrantConfig, scripts_root: &'a Path) -> Self {
        Self {
            interpreters: &grant.interpreters,
            run_env: &grant.run_env,
            entry_base: scripts_root,
            entry_root: scripts_root,
            cwd_policy: CwdPolicy::Owner,
        }
    }
}

/// Resolve and freeze one project event-rule run before ordinary command admission.
pub(crate) fn freeze_project_run(
    definition: RunDefinitionArgs,
    registration: &ProjectRegistrationView,
    context: FrozenScriptContext,
    policy: &SecretPolicy,
) -> Result<FrozenScriptRun> {
    registration.validate()?;
    authorize_run(registration)?;
    let (checkout, scripts_root) = project_script_roots(registration)?;
    freeze_run_in_scope(
        definition,
        RunScope::project(registration, &checkout, &scripts_root),
        context,
        policy,
    )
}

/// Resolve and freeze one owner-global event-rule run before ordinary command admission.
pub(crate) fn freeze_global_run(
    definition: RunDefinitionArgs,
    grant: &crate::config::RunGrantConfig,
    data_root: &Path,
    context: FrozenScriptContext,
    policy: &SecretPolicy,
) -> Result<FrozenScriptRun> {
    let scripts_root = global_script_root(data_root)?;
    freeze_run_in_scope(
        definition,
        RunScope::owner(grant, &scripts_root),
        context,
        policy,
    )
}

fn freeze_run_in_scope(
    definition: RunDefinitionArgs,
    scope: RunScope<'_>,
    context: FrozenScriptContext,
    policy: &SecretPolicy,
) -> Result<FrozenScriptRun> {
    definition.validate()?;
    context.validate()?;
    validate_run_environment(&definition, scope.run_env, scope.cwd_policy)?;
    reject_known_secrets(&definition, policy)?;
    reject_known_secrets(&context, policy)?;

    let reference = definition.cmd[0].clone();
    let configured = scope
        .interpreters
        .get(&reference)
        .ok_or_else(|| match scope.cwd_policy {
            CwdPolicy::Project => not_allowed("project interpreter is no longer approved"),
            CwdPolicy::Owner => not_allowed("global interpreter is not approved"),
        })?;
    configured.validate()?;
    crate::setup::check_program(Path::new(&configured.program)).map_err(|_| {
        match scope.cwd_policy {
            CwdPolicy::Project => not_allowed("project interpreter path is invalid"),
            CwdPolicy::Owner => not_allowed("global interpreter path is invalid"),
        }
    })?;

    let entry_path = script_entry_path(
        scope.entry_base,
        scope.entry_root,
        &definition,
        scope.cwd_policy,
    )?;
    let entry = capture_file(&entry_path, Some(scope.entry_root))?;
    let executable = capture_file(Path::new(&configured.program), None)?;

    let frozen = FrozenScriptRun {
        definition,
        interpreter: FrozenScriptInterpreter {
            reference,
            executable,
            argv_prefix: configured.argv_prefix.clone(),
        },
        entry,
        context,
    };
    frozen.validate()?;
    project_run_timeout(&frozen)?;
    reject_known_secrets(&frozen, policy)?;
    resolve_script_cwd(&scope, &frozen)?;
    Ok(frozen)
}

/// Verify the current owner grant and frozen file receipts, then build an explicit child command.
pub(crate) fn verify_project_run(
    frozen: &FrozenScriptRun,
    registration: &ProjectRegistrationView,
    policy: &SecretPolicy,
) -> Result<Command> {
    registration.validate()?;
    authorize_run(registration)?;
    let (checkout, scripts_root) = project_script_roots(registration)?;
    verify_run_in_scope(
        frozen,
        RunScope::project(registration, &checkout, &scripts_root),
        policy,
    )
}

/// Verify the current owner grant and frozen file receipts for one global run.
pub(crate) fn verify_global_run(
    frozen: &FrozenScriptRun,
    grant: &crate::config::RunGrantConfig,
    data_root: &Path,
    policy: &SecretPolicy,
) -> Result<Command> {
    let scripts_root = global_script_root(data_root)
        .map_err(|_| source_changed("global script root is unavailable or unsafe"))?;
    verify_run_in_scope(frozen, RunScope::owner(grant, &scripts_root), policy)
}

fn verify_run_in_scope(
    frozen: &FrozenScriptRun,
    scope: RunScope<'_>,
    policy: &SecretPolicy,
) -> Result<Command> {
    frozen.validate()?;
    validate_run_environment(&frozen.definition, scope.run_env, scope.cwd_policy)?;
    reject_known_secrets(frozen, policy)?;

    let configured = scope
        .interpreters
        .get(&frozen.interpreter.reference)
        .ok_or_else(|| match scope.cwd_policy {
            CwdPolicy::Project => not_allowed("frozen project interpreter is no longer approved"),
            CwdPolicy::Owner => not_allowed("frozen global interpreter is no longer approved"),
        })?;
    configured.validate()?;
    if configured.argv_prefix != frozen.interpreter.argv_prefix {
        return Err(source_changed("approved interpreter argv_prefix changed"));
    }
    crate::setup::check_program(Path::new(&configured.program))
        .map_err(|_| source_changed("approved interpreter executable path changed"))?;

    let current_executable = capture_file(Path::new(&configured.program), None)
        .map_err(|_| source_changed("approved interpreter executable is unavailable"))?;
    if current_executable != frozen.interpreter.executable {
        return Err(source_changed("approved interpreter executable changed"));
    }

    let current_entry_path = script_entry_path(
        scope.entry_base,
        scope.entry_root,
        &frozen.definition,
        scope.cwd_policy,
    )
    .map_err(|_| match scope.cwd_policy {
        CwdPolicy::Project => source_changed("registered script entry path changed"),
        CwdPolicy::Owner => source_changed("global script entry path changed"),
    })?;
    let current_entry =
        capture_file(&current_entry_path, Some(scope.entry_root)).map_err(|_| {
            match scope.cwd_policy {
                CwdPolicy::Project => source_changed("registered script entry is unavailable"),
                CwdPolicy::Owner => source_changed("global script entry is unavailable"),
            }
        })?;
    if current_entry != frozen.entry {
        let message = match scope.cwd_policy {
            CwdPolicy::Project => "registered script entry changed",
            CwdPolicy::Owner => "global script entry changed",
        };
        return Err(source_changed(message));
    }

    let cwd = resolve_script_cwd(&scope, frozen)?;
    #[cfg(windows)]
    let cwd = {
        let changed = || source_changed("display Run cwd differs from the admitted directory");
        let rendered = eliot_core::display_path(cwd.to_str().ok_or_else(changed)?);
        let path = PathBuf::from(rendered);
        let verified = crate::setup::canonical_directory(&path).map_err(|_| changed())?;
        if verified != cwd {
            return Err(changed());
        }
        path
    };
    project_run_timeout(frozen)?;

    let entry_argument =
        crate::setup::file_argument(&frozen.entry).map_err(|_| match scope.cwd_policy {
            CwdPolicy::Project => source_changed("registered script entry argument path changed"),
            CwdPolicy::Owner => source_changed("global script entry argument path changed"),
        })?;
    let mut command =
        super::process::local_process_command(&frozen.interpreter.executable.canonical_path)?;
    command
        .args(&frozen.interpreter.argv_prefix)
        .arg(entry_argument)
        .args(frozen.definition.cmd.iter().skip(2))
        .current_dir(cwd);

    for (name, value) in &frozen.definition.env {
        command.env(name, value);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    Ok(command)
}

/// Parse the shared humantime grammar and prove a checked monotonic deadline.
pub(crate) fn project_run_timeout(frozen: &FrozenScriptRun) -> Result<Duration> {
    let raw = frozen.definition.timeout.as_deref().unwrap_or("10m");
    let duration = humantime::parse_duration(raw)
        .map_err(|_| invalid("run timeout must be a positive humantime duration"))?;
    let nanos = duration.as_nanos();
    const NANOS_PER_MILLISECOND: u128 = 1_000_000;
    if nanos == 0 || nanos % NANOS_PER_MILLISECOND != 0 {
        return Err(invalid(
            "run timeout must be a positive whole number of milliseconds",
        ));
    }
    let milliseconds = i64::try_from(nanos / NANOS_PER_MILLISECOND)
        .map_err(|_| invalid("run timeout is outside the timer millisecond range"))?;
    let duration = Duration::from_millis(milliseconds as u64);
    if Instant::now().checked_add(duration).is_none() {
        return Err(invalid("run timeout has no representable checked deadline"));
    }
    Ok(duration)
}

fn authorize_run(registration: &ProjectRegistrationView) -> Result<()> {
    if !registration.actions.iter().any(|action| action == "run") {
        return Err(not_allowed("project run action is not approved"));
    }
    Ok(())
}

fn validate_run_environment(
    definition: &RunDefinitionArgs,
    approved_names: &[String],
    cwd_policy: CwdPolicy,
) -> Result<()> {
    let mut seen = HashSet::new();
    for (name, value) in &definition.env {
        if !approved_names
            .iter()
            .any(|approved| environment_names_equal(name, approved))
        {
            let message = match cwd_policy {
                CwdPolicy::Project => "project run environment name is not approved",
                CwdPolicy::Owner => "global run environment name is not approved",
            };
            return Err(not_allowed(message));
        }
        #[cfg(windows)]
        if environment_names_equal(name, "SystemRoot") {
            let message = match cwd_policy {
                CwdPolicy::Project => "project run environment cannot override SystemRoot",
                CwdPolicy::Owner => "global run environment cannot override SystemRoot",
            };
            return Err(not_allowed(message));
        }
        let key = if cfg!(windows) {
            name.to_ascii_uppercase()
        } else {
            name.clone()
        };
        if !seen.insert(key) {
            return Err(invalid("project run environment contains duplicate names"));
        }
        if value.contains('\0') {
            return Err(invalid("project run environment values cannot contain NUL"));
        }
    }
    Ok(())
}

fn environment_names_equal(left: &str, right: &str) -> bool {
    if cfg!(windows) {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

pub(crate) fn project_script_roots(
    registration: &ProjectRegistrationView,
) -> Result<(PathBuf, PathBuf)> {
    let checkout = crate::setup::canonical_directory(Path::new(&registration.checkout))
        .map_err(|_| not_allowed("registered project checkout is unavailable or unsafe"))?;
    let scripts_root = crate::setup::canonical_directory(&checkout.join(".eliot").join("scripts"))
        .map_err(|_| not_allowed("registered .eliot/scripts directory is unavailable or unsafe"))?;
    if !crate::setup::inside(&checkout, &scripts_root) {
        return Err(not_allowed(
            "registered .eliot/scripts directory is outside the project checkout",
        ));
    }
    Ok((checkout, scripts_root))
}

pub(crate) fn global_script_root(data_root: &Path) -> Result<PathBuf> {
    let data_root = crate::setup::canonical_directory(data_root)
        .map_err(|_| not_allowed("global data root is unavailable or unsafe"))?;
    let scripts_root = crate::setup::canonical_directory(&data_root.join("scripts"))
        .map_err(|_| not_allowed("global scripts directory is unavailable or unsafe"))?;
    if !crate::setup::inside(&data_root, &scripts_root) {
        return Err(not_allowed(
            "global scripts directory is outside the data root",
        ));
    }
    Ok(scripts_root)
}

fn script_entry_path(
    entry_base: &Path,
    entry_root: &Path,
    definition: &RunDefinitionArgs,
    cwd_policy: CwdPolicy,
) -> Result<PathBuf> {
    let raw = definition
        .cmd
        .get(1)
        .ok_or_else(|| invalid("run cmd requires a script entry"))?;
    let relative = Path::new(raw);
    if relative.is_absolute() || relative.has_root() {
        let message = match cwd_policy {
            CwdPolicy::Project => {
                "project script entry must be relative to the registered checkout"
            }
            CwdPolicy::Owner => "global script entry must be relative to the scripts directory",
        };
        return Err(not_allowed(message));
    }
    let candidate = entry_base.join(relative);
    let canonical = std::fs::canonicalize(&candidate).map_err(|_| match cwd_policy {
        CwdPolicy::Project => not_allowed("project script entry is unavailable"),
        CwdPolicy::Owner => not_allowed("global script entry is unavailable"),
    })?;
    if !crate::setup::inside(entry_root, &canonical) {
        let message = match cwd_policy {
            CwdPolicy::Project => "project script entry is outside registered .eliot/scripts",
            CwdPolicy::Owner => "global script entry is outside the scripts directory",
        };
        return Err(not_allowed(message));
    }
    Ok(candidate)
}

fn resolve_run_cwd(scope: &RunScope<'_>, cwd: Option<&str>) -> Result<PathBuf> {
    match scope.cwd_policy {
        CwdPolicy::Project => resolve_project_cwd(scope.entry_base, cwd),
        CwdPolicy::Owner => resolve_owner_cwd(scope.entry_root, cwd),
    }
}

fn resolve_script_cwd(scope: &RunScope<'_>, frozen: &FrozenScriptRun) -> Result<PathBuf> {
    let Some(workspace) = event_workspace(&frozen.context) else {
        return resolve_run_cwd(scope, frozen.definition.cwd.as_deref());
    };
    let workspace = crate::setup::canonical_directory(Path::new(workspace))
        .map_err(|_| not_allowed("event-time Agent worktree is unavailable or unsafe"))?;
    let Some(cwd) = frozen.definition.cwd.as_deref() else {
        return Ok(workspace);
    };
    if matches!(scope.cwd_policy, CwdPolicy::Owner) && Path::new(cwd).is_absolute() {
        return resolve_owner_cwd(scope.entry_root, Some(cwd));
    }

    let relative = Path::new(cwd);
    if cwd.trim().is_empty()
        || relative.is_absolute()
        || relative.has_root()
        || relative
            .components()
            .any(|component| matches!(component, Component::Prefix(_)))
    {
        return Err(not_allowed(
            "event-time run cwd must be a nonempty relative path",
        ));
    }
    let canonical = crate::setup::canonical_directory(&workspace.join(relative))
        .map_err(|_| not_allowed("event-time run cwd is unavailable or unsafe"))?;
    if !same_path(&workspace, &canonical) && !crate::setup::inside(&workspace, &canonical) {
        return Err(not_allowed(
            "event-time run cwd is outside the Agent worktree",
        ));
    }
    Ok(canonical)
}

pub(crate) fn event_workspace(context: &FrozenScriptContext) -> Option<&str> {
    context
        .subject()
        .and_then(|subject| subject.workspace.as_deref())
        .or_else(|| {
            context
                .event()
                .and_then(|event| event.subject.as_ref())
                .and_then(|subject| subject.workspace.as_deref())
        })
}

fn resolve_project_cwd(checkout: &Path, cwd: Option<&str>) -> Result<PathBuf> {
    let candidate = match cwd {
        Some(value) => {
            let relative = Path::new(value);
            if value.trim().is_empty() || relative.is_absolute() || relative.has_root() {
                return Err(not_allowed(
                    "project run cwd must be a nonempty relative path",
                ));
            }
            checkout.join(relative)
        }
        None => checkout.to_path_buf(),
    };
    let canonical = crate::setup::canonical_directory(&candidate)
        .map_err(|_| not_allowed("project run cwd is unavailable or unsafe"))?;
    if !same_path(checkout, &canonical) && !crate::setup::inside(checkout, &canonical) {
        return Err(not_allowed(
            "project run cwd is outside the registered checkout",
        ));
    }
    Ok(canonical)
}

fn resolve_owner_cwd(scripts_root: &Path, cwd: Option<&str>) -> Result<PathBuf> {
    let Some(value) = cwd else {
        return Ok(scripts_root.to_path_buf());
    };
    if value.trim().is_empty() {
        return Err(not_allowed("global run cwd must be a nonempty path"));
    }

    let requested = Path::new(value);
    if requested.is_absolute() {
        return crate::setup::canonical_directory(requested)
            .map_err(|_| not_allowed("global run cwd is unavailable or unsafe"));
    }
    if requested.has_root()
        || requested
            .components()
            .any(|component| matches!(component, Component::Prefix(_)))
    {
        return Err(not_allowed(
            "global relative run cwd must not contain a path root",
        ));
    }

    let canonical = crate::setup::canonical_directory(&scripts_root.join(requested))
        .map_err(|_| not_allowed("global run cwd is unavailable or unsafe"))?;
    if !same_path(scripts_root, &canonical) && !crate::setup::inside(scripts_root, &canonical) {
        return Err(not_allowed(
            "global relative run cwd is outside the scripts directory",
        ));
    }
    Ok(canonical)
}

fn same_path(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.as_os_str()
            .to_string_lossy()
            .eq_ignore_ascii_case(&right.as_os_str().to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn capture_file(path: &Path, root: Option<&Path>) -> Result<FrozenSetupFile> {
    let mut remaining = u64::MAX;
    crate::setup::capture_file(path, root, &mut remaining)
        .map_err(|_| invalid("script file is not a bounded canonical regular file"))
}

fn reject_known_secrets<T: serde::Serialize>(value: &T, policy: &SecretPolicy) -> Result<()> {
    let value = serde_json::to_value(value)
        .map_err(|_| invalid("script run semantic data cannot be encoded"))?;
    if policy.contains_value(&value) {
        return Err(not_allowed(
            "script run semantic data contains a known credential",
        ));
    }
    Ok(())
}

fn invalid(message: &'static str) -> Error {
    Error::new("invalid", message)
}

fn not_allowed(message: &'static str) -> Error {
    Error::new("not_allowed", message)
}

fn source_changed(message: &'static str) -> Error {
    Error::new("source_integrity", message)
}
