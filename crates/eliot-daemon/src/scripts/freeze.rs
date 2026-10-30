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
    resolve_run_cwd(&scope, frozen.definition.cwd.as_deref())?;
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

    let cwd = resolve_run_cwd(&scope, frozen.definition.cwd.as_deref())?;
    project_run_timeout(frozen)?;

    let entry_argument =
        crate::setup::file_argument(&frozen.entry).map_err(|_| match scope.cwd_policy {
            CwdPolicy::Project => source_changed("registered script entry argument path changed"),
            CwdPolicy::Owner => source_changed("global script entry argument path changed"),
        })?;
    let mut command = Command::new(&frozen.interpreter.executable.canonical_path);
    command
        .args(&frozen.interpreter.argv_prefix)
        .arg(entry_argument)
        .args(frozen.definition.cmd.iter().skip(2))
        .current_dir(cwd)
        .env_clear();

    add_minimal_platform_environment(&mut command)?;
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

fn project_script_roots(registration: &ProjectRegistrationView) -> Result<(PathBuf, PathBuf)> {
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

fn global_script_root(data_root: &Path) -> Result<PathBuf> {
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

fn add_minimal_platform_environment(command: &mut Command) -> Result<()> {
    #[cfg(windows)]
    {
        let system_root = std::env::var_os("SystemRoot")
            .ok_or_else(|| Error::new("unavailable", "Windows SystemRoot is unavailable"))?;
        command.env("SystemRoot", system_root);
        let pathext = std::env::var_os("PATHEXT")
            .ok_or_else(|| Error::new("unavailable", "Windows PATHEXT is unavailable"))?;
        command.env("PATHEXT", pathext);
    }
    #[cfg(not(windows))]
    {
        let _ = command;
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{
        AgentState, Event, EventSubject, ScriptRuleContext,
        api::{ScriptTimerContext, ScriptTimerInvocation, TimerSchedule},
    };
    use serde_json::json;
    use std::{fs, path::PathBuf};

    struct ProjectFixture {
        root: PathBuf,
        registration: ProjectRegistrationView,

        entry: PathBuf,
    }

    impl ProjectFixture {
        fn new() -> Self {
            let root =
                std::env::temp_dir().join(format!("eliot-script-freeze-{}", ulid::Ulid::new()));
            fs::create_dir(&root).expect("create unique project fixture");
            let scripts = root.join(".eliot").join("scripts");
            fs::create_dir_all(&scripts).expect("create registered script directory");
            fs::create_dir(root.join("work")).expect("create cwd");
            let tools = root.join("tools");
            fs::create_dir(&tools).expect("create tool directory");
            let executable = tools.join(if cfg!(windows) { "pwsh.exe" } else { "pwsh" });
            fs::write(&executable, b"fixture interpreter").expect("write interpreter fixture");
            let entry = scripts.join("check.ps1");
            fs::write(&entry, b"Write-Output 'ok'").expect("write script entry");
            let checkout = fs::canonicalize(&root).expect("canonical project root");
            let checkout_text = checkout.to_string_lossy().into_owned();
            let registration = ProjectRegistrationView {
                id: "sample".to_owned(),
                checkout: checkout_text.clone(),
                repo_common_dir: checkout_text,
                profiles: vec!["fake".to_owned()],
                actions: vec!["run".to_owned(), "send".to_owned()],
                interpreters: std::collections::BTreeMap::from([(
                    "pwsh".to_owned(),
                    eliot_core::ScriptInterpreter {
                        program: executable.to_string_lossy().into_owned(),
                        argv_prefix: vec![
                            "-NoProfile".to_owned(),
                            "-NonInteractive".to_owned(),
                            "-File".to_owned(),
                        ],
                    },
                )]),
                run_env: vec!["CI".to_owned()],
                revision: 1,
            };
            Self {
                root: checkout,
                registration,
                entry,
            }
        }

        fn definition(&self) -> RunDefinitionArgs {
            RunDefinitionArgs {
                cmd: vec![
                    "pwsh".to_owned(),
                    ".eliot/scripts/check.ps1".to_owned(),
                    "-Changed".to_owned(),
                ],
                cwd: Some("work".to_owned()),
                timeout: Some("10m".to_owned()),
                env: std::collections::BTreeMap::from([("CI".to_owned(), "true".to_owned())]),
            }
        }

        fn global_data_root(&self) -> PathBuf {
            let data_root = self.root.join("owner-data");
            let scripts_root = data_root.join("scripts");
            fs::create_dir_all(&scripts_root).expect("create global scripts root");
            fs::copy(&self.entry, scripts_root.join("check.ps1"))
                .expect("copy global script entry");
            data_root
        }

        fn global_grant(&self) -> crate::config::RunGrantConfig {
            crate::config::RunGrantConfig {
                interpreters: self.registration.interpreters.clone(),
                run_env: self.registration.run_env.clone(),
            }
        }

        fn global_definition(&self) -> RunDefinitionArgs {
            let mut definition = self.definition();
            definition.cmd[1] = "check.ps1".to_owned();
            definition.cwd = None;
            definition
        }

        fn context(&self) -> FrozenScriptContext {
            let subject = EventSubject {
                project: "sample".to_owned(),
                harness: "fake".to_owned(),
                profile: "fake".to_owned(),
                state: AgentState::Busy,
                workspace: Some(self.root.to_string_lossy().into_owned()),
            };
            FrozenScriptContext::Event {
                event: Box::new(Event {
                    seq: 1,
                    ts: 2,
                    kind: "turn.completed".to_owned(),
                    agent: Some("a1".to_owned()),
                    subject: Some(subject.clone()),
                    generation: Some(1),
                    source: "stream".to_owned(),
                    native_id: Some("fake:turn.completed".to_owned()),
                    data: json!({"status":"complete"}),
                    cause: None,
                }),
                subject: Some(subject),
                rule: ScriptRuleContext {
                    id: "check-completed".to_owned(),
                    revision: 1,
                },
            }
        }

        fn timer_context(&self) -> FrozenScriptContext {
            FrozenScriptContext::Timer {
                timer: ScriptTimerContext {
                    schedule_revision: 2,
                    schedule: TimerSchedule::Interval {
                        period_ms: 60_000,
                        anchor: Some(30_000),
                    },
                    anchor: 30_000,
                    invocation: ScriptTimerInvocation::Scheduled {
                        scheduled_at_ms: 90_000,
                    },
                },
                rule: ScriptRuleContext {
                    id: "periodic-check".to_owned(),
                    revision: 2,
                },
            }
        }
    }

    impl Drop for ProjectFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn policy() -> SecretPolicy {
        SecretPolicy::default()
    }

    #[test]
    fn project_run_freezes_and_reverifies_entry_interpreter_and_explicit_argv() {
        let fixture = ProjectFixture::new();
        let frozen = freeze_project_run(
            fixture.definition(),
            &fixture.registration,
            fixture.context(),
            &policy(),
        )
        .expect("freeze project run");
        assert_eq!(
            frozen.context.event().map(|event| event.kind.as_str()),
            Some("turn.completed")
        );
        assert!(frozen.context.subject().is_some());
        assert!(frozen.context.timer().is_none());
        let context_json = serde_json::to_value(&frozen.context).expect("event context JSON");
        assert_eq!(context_json.as_object().map(|object| object.len()), Some(3));
        assert!(context_json.get("event").is_some());
        assert!(context_json.get("subject").is_some());
        assert!(context_json.get("rule").is_some());
        assert!(context_json.get("timer").is_none());
        let command = verify_project_run(&frozen, &fixture.registration, &policy())
            .expect("verify project run");
        let process = command.as_std();
        assert_eq!(
            process.get_program().to_string_lossy(),
            frozen.interpreter.executable.canonical_path
        );
        let expected_entry_argument = crate::setup::file_argument(&frozen.entry)
            .expect("validated frozen entry argument")
            .to_string_lossy()
            .into_owned();
        let args: Vec<String> = process
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec![
                "-NoProfile",
                "-NonInteractive",
                "-File",
                expected_entry_argument.as_str(),
                "-Changed"
            ]
        );
        let expected_cwd = fs::canonicalize(fixture.root.join("work")).expect("canonical cwd");
        assert_eq!(process.get_current_dir(), Some(expected_cwd.as_path()));
        let environment: Vec<String> = process
            .get_envs()
            .map(|(name, _)| name.to_string_lossy().into_owned())
            .collect();
        assert!(environment.iter().any(|name| name == "CI"));
        assert!(
            !environment
                .iter()
                .any(|name| name.eq_ignore_ascii_case("PATH"))
        );
        assert!(
            !environment
                .iter()
                .any(|name| name.eq_ignore_ascii_case("ELIOT_RUN_TOKEN"))
        );
        #[cfg(windows)]
        {
            let system_root = std::env::var_os("SystemRoot").expect("Windows SystemRoot");
            let pathext = std::env::var_os("PATHEXT").expect("Windows PATHEXT");
            let baseline = process
                .get_envs()
                .map(|(name, value)| {
                    (
                        name.to_string_lossy().into_owned(),
                        value.map(|value| value.to_os_string()),
                    )
                })
                .collect::<Vec<_>>();
            let mut names = baseline
                .iter()
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            names.sort();
            assert_eq!(
                names,
                vec![
                    "CI".to_owned(),
                    "PATHEXT".to_owned(),
                    "SystemRoot".to_owned(),
                ]
            );
            let actual_pathext = baseline
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("PATHEXT"))
                .and_then(|(_, value)| value.as_deref());
            assert_eq!(actual_pathext, Some(pathext.as_os_str()));
            let actual_system_root = baseline
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("SystemRoot"))
                .and_then(|(_, value)| value.as_deref());
            assert_eq!(actual_system_root, Some(system_root.as_os_str()));
            let literal_ci = baseline
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("CI"))
                .and_then(|(_, value)| value.as_deref());
            assert_eq!(literal_ci, Some(std::ffi::OsStr::new("true")));
        }
    }

    #[cfg(windows)]
    #[test]
    fn project_run_normalizes_windows_entry_argv_without_changing_receipt() {
        let fixture = ProjectFixture::new();
        let frozen = freeze_project_run(
            fixture.definition(),
            &fixture.registration,
            fixture.context(),
            &policy(),
        )
        .expect("freeze project run");
        let frozen_entry = frozen.entry.clone();
        assert!(
            frozen_entry.canonical_path.starts_with(r"\\?\"),
            "Windows canonical entry should use its verbatim receipt spelling"
        );

        let command = verify_project_run(&frozen, &fixture.registration, &policy())
            .expect("verify project run");
        let entry_argument = command
            .as_std()
            .get_args()
            .nth(3)
            .expect("entry after PowerShell argv prefix");
        assert!(
            !entry_argument.to_string_lossy().starts_with(r"\\?\"),
            "PowerShell argv uses a normalized disk or UNC spelling"
        );
        assert_eq!(
            fs::canonicalize(Path::new(entry_argument)).expect("normal entry path"),
            PathBuf::from(frozen_entry.canonical_path.as_str())
        );
        assert_eq!(frozen.entry, frozen_entry);
    }

    #[test]
    fn project_run_rejects_unapproved_environment_and_changed_files_or_grants() {
        let fixture = ProjectFixture::new();
        let mut definition = fixture.definition();
        definition
            .env
            .insert("PATH".to_owned(), "unapproved".to_owned());
        assert!(
            freeze_project_run(
                definition,
                &fixture.registration,
                fixture.context(),
                &policy()
            )
            .is_err()
        );

        let frozen = freeze_project_run(
            fixture.definition(),
            &fixture.registration,
            fixture.context(),
            &policy(),
        )
        .expect("freeze project run");
        fs::write(&fixture.entry, b"changed script bytes").expect("change script entry");
        assert!(verify_project_run(&frozen, &fixture.registration, &policy()).is_err());

        let mut changed_registration = fixture.registration.clone();
        changed_registration.actions = vec!["send".to_owned()];
        assert!(verify_project_run(&frozen, &changed_registration, &policy()).is_err());
    }

    #[test]
    fn project_run_rejects_path_escape_secret_values_and_timeout_ambiguity() {
        let fixture = ProjectFixture::new();
        let mut definition = fixture.definition();
        definition.cmd[1] = ".eliot/scripts/../outside.ps1".to_owned();
        assert!(
            freeze_project_run(
                definition,
                &fixture.registration,
                fixture.context(),
                &policy()
            )
            .is_err()
        );

        let mut definition = fixture.definition();
        definition
            .env
            .insert("CI".to_owned(), "known-secret".to_owned());
        let secret_policy = SecretPolicy::new(["known-secret"]);
        assert!(
            freeze_project_run(
                definition,
                &fixture.registration,
                fixture.context(),
                &secret_policy
            )
            .is_err()
        );

        let frozen = freeze_project_run(
            fixture.definition(),
            &fixture.registration,
            fixture.context(),
            &policy(),
        )
        .expect("freeze project run");
        assert_eq!(
            project_run_timeout(&frozen).expect("default timeout"),
            Duration::from_secs(600)
        );
        let mut two_hours = frozen.clone();
        two_hours.definition.timeout = Some("2h".to_owned());
        assert_eq!(
            project_run_timeout(&two_hours).expect("no setup-only one-hour cap"),
            Duration::from_secs(7_200)
        );
        let mut submillisecond = frozen.clone();
        submillisecond.definition.timeout = Some("1ns".to_owned());
        assert!(project_run_timeout(&submillisecond).is_err());
        let mut zero = frozen;
        zero.definition.timeout = Some("0s".to_owned());
        assert!(project_run_timeout(&zero).is_err());
    }

    #[test]
    fn global_run_empty_grant_denies_execution() {
        let fixture = ProjectFixture::new();
        let data_root = fixture.global_data_root();
        let mut definition = fixture.global_definition();
        definition.env.clear();

        assert!(
            freeze_global_run(
                definition,
                &crate::config::RunGrantConfig::default(),
                &data_root,
                fixture.context(),
                &policy(),
            )
            .is_err()
        );
    }

    #[test]
    fn global_run_allows_safe_absolute_cwd_outside_scripts_root() {
        let fixture = ProjectFixture::new();
        let data_root = fixture.global_data_root();
        let scripts_root =
            fs::canonicalize(data_root.join("scripts")).expect("canonical global scripts root");
        let grant = fixture.global_grant();

        let frozen = freeze_global_run(
            fixture.global_definition(),
            &grant,
            &data_root,
            fixture.context(),
            &policy(),
        )
        .expect("freeze global run with default cwd");
        let command = verify_global_run(&frozen, &grant, &data_root, &policy())
            .expect("verify global run with default cwd");
        assert_eq!(
            command.as_std().get_current_dir(),
            Some(scripts_root.as_path())
        );

        let absolute_cwd = fixture.root.join("owner-cwd");
        fs::create_dir(&absolute_cwd).expect("create owner-approved cwd");
        let relative_escape = data_root.join("owner-cwd");
        fs::create_dir(&relative_escape).expect("create cwd outside scripts root");
        let canonical_cwd = fs::canonicalize(&absolute_cwd).expect("canonical owner cwd");

        let mut definition = fixture.global_definition();
        definition.cwd = Some("../owner-cwd".to_owned());
        assert!(
            freeze_global_run(definition, &grant, &data_root, fixture.context(), &policy(),)
                .is_err()
        );

        let mut definition = fixture.global_definition();
        definition.cwd = Some(absolute_cwd.to_string_lossy().into_owned());
        let frozen =
            freeze_global_run(definition, &grant, &data_root, fixture.context(), &policy())
                .expect("freeze global run with absolute owner cwd");
        let command = verify_global_run(&frozen, &grant, &data_root, &policy())
            .expect("verify global run with absolute owner cwd");
        assert_eq!(
            command.as_std().get_current_dir(),
            Some(canonical_cwd.as_path())
        );
    }

    #[test]
    fn global_run_rejects_entry_escape_from_scripts_root() {
        let fixture = ProjectFixture::new();
        let data_root = fixture.global_data_root();
        fs::write(data_root.join("outside.ps1"), b"outside scripts root")
            .expect("create escaping entry target");
        let mut definition = fixture.global_definition();
        definition.cmd[1] = "../outside.ps1".to_owned();

        assert!(
            freeze_global_run(
                definition,
                &fixture.global_grant(),
                &data_root,
                fixture.context(),
                &policy(),
            )
            .is_err()
        );
    }

    #[test]
    fn global_run_rechecks_interpreter_and_environment_grants_before_spawn() {
        let fixture = ProjectFixture::new();
        let data_root = fixture.global_data_root();
        let grant = fixture.global_grant();
        let frozen = freeze_global_run(
            fixture.global_definition(),
            &grant,
            &data_root,
            fixture.context(),
            &policy(),
        )
        .expect("freeze global run");

        let mut removed_interpreter = grant.clone();
        removed_interpreter.interpreters.clear();
        assert!(verify_global_run(&frozen, &removed_interpreter, &data_root, &policy()).is_err());

        let mut changed_prefix = grant.clone();
        changed_prefix
            .interpreters
            .get_mut("pwsh")
            .expect("approved interpreter")
            .argv_prefix
            .push("-NoLogo".to_owned());
        assert!(verify_global_run(&frozen, &changed_prefix, &data_root, &policy()).is_err());

        let mut removed_environment = grant;
        removed_environment.run_env.clear();
        assert!(verify_global_run(&frozen, &removed_environment, &data_root, &policy()).is_err());
    }

    #[test]
    fn project_and_global_freezers_accept_the_real_timer_context() {
        let fixture = ProjectFixture::new();
        let timer_context = fixture.timer_context();

        let project = freeze_project_run(
            fixture.definition(),
            &fixture.registration,
            timer_context.clone(),
            &policy(),
        )
        .expect("freeze timer Run under project grants");
        assert!(project.context.event().is_none());
        assert!(project.context.subject().is_none());
        assert_eq!(
            project.context.timer().map(|timer| timer.schedule_revision),
            Some(2)
        );
        assert_eq!(project.context.rule().id, "periodic-check");
        verify_project_run(&project, &fixture.registration, &policy())
            .expect("verify timer Run under project grants");

        let data_root = fixture.global_data_root();
        let grant = fixture.global_grant();
        let global = freeze_global_run(
            fixture.global_definition(),
            &grant,
            &data_root,
            timer_context,
            &policy(),
        )
        .expect("freeze timer Run under owner grants");
        assert!(global.context.event().is_none());
        assert!(global.context.subject().is_none());
        assert_eq!(
            global.context.timer().map(|timer| timer.invocation.clone()),
            Some(ScriptTimerInvocation::Scheduled {
                scheduled_at_ms: 90_000
            })
        );
        assert_eq!(global.context.rule().revision, 2);
        verify_global_run(&global, &grant, &data_root, &policy())
            .expect("verify timer Run under owner grants");
    }
}
