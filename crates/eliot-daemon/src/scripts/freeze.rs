//! Project event-rule run freezing and pre-spawn verification.
//!
//! This module resolves owner-approved inputs and builds an explicit child command.
//! The caller owns Router/Store admission, process custody, Run identity injection,
//! stdout validation and action-child admission.

use eliot_core::{
    Error, FrozenScriptContext, FrozenScriptInterpreter, FrozenScriptRun, FrozenSetupFile, Result,
    RunDefinitionArgs, SecretPolicy, api::ProjectRegistrationView,
};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::process::Command;

/// Resolve and freeze one project event-rule run before ordinary command admission.
pub(crate) fn freeze_project_run(
    definition: RunDefinitionArgs,
    registration: &ProjectRegistrationView,
    context: FrozenScriptContext,
    policy: &SecretPolicy,
) -> Result<FrozenScriptRun> {
    registration.validate()?;
    authorize_run(registration)?;
    definition.validate()?;
    context.validate()?;
    validate_run_environment(&definition, registration)?;
    reject_known_secrets(&definition, policy)?;
    reject_known_secrets(&context, policy)?;

    let reference = definition.cmd[0].clone();
    let configured = registration
        .interpreters
        .get(&reference)
        .ok_or_else(|| not_allowed("project interpreter is no longer approved"))?;
    configured.validate()?;
    crate::setup::check_program(Path::new(&configured.program))
        .map_err(|_| not_allowed("project interpreter path is invalid"))?;

    let (checkout, scripts_root) = project_script_roots(registration)?;
    let entry_path = project_script_path(&checkout, &scripts_root, &definition)?;
    let entry = capture_file(&entry_path, Some(&scripts_root))?;
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
    resolve_project_cwd(&checkout, frozen.definition.cwd.as_deref())?;
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
    frozen.validate()?;
    validate_run_environment(&frozen.definition, registration)?;
    reject_known_secrets(frozen, policy)?;

    let configured = registration
        .interpreters
        .get(&frozen.interpreter.reference)
        .ok_or_else(|| not_allowed("frozen project interpreter is no longer approved"))?;
    configured.validate()?;
    if configured.argv_prefix != frozen.interpreter.argv_prefix {
        return Err(source_changed("approved interpreter argv_prefix changed"));
    }
    crate::setup::check_program(Path::new(&configured.program))
        .map_err(|_| source_changed("approved interpreter executable path changed"))?;

    let (checkout, scripts_root) = project_script_roots(registration)?;
    let current_executable = capture_file(Path::new(&configured.program), None)
        .map_err(|_| source_changed("approved interpreter executable is unavailable"))?;
    if current_executable != frozen.interpreter.executable {
        return Err(source_changed("approved interpreter executable changed"));
    }

    let current_entry_path = project_script_path(&checkout, &scripts_root, &frozen.definition)
        .map_err(|_| source_changed("registered script entry path changed"))?;
    let current_entry = capture_file(&current_entry_path, Some(&scripts_root))
        .map_err(|_| source_changed("registered script entry is unavailable"))?;
    if current_entry != frozen.entry {
        return Err(source_changed("registered script entry changed"));
    }

    let cwd = resolve_project_cwd(&checkout, frozen.definition.cwd.as_deref())?;
    project_run_timeout(frozen)?;

    let mut command = Command::new(&frozen.interpreter.executable.canonical_path);
    command
        .args(&frozen.interpreter.argv_prefix)
        .arg(&frozen.entry.canonical_path)
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
    registration: &ProjectRegistrationView,
) -> Result<()> {
    let mut seen = HashSet::new();
    for (name, value) in &definition.env {
        if !registration
            .run_env
            .iter()
            .any(|approved| environment_names_equal(name, approved))
        {
            return Err(not_allowed("project run environment name is not approved"));
        }
        #[cfg(windows)]
        if environment_names_equal(name, "SystemRoot") {
            return Err(not_allowed(
                "project run environment cannot override SystemRoot",
            ));
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

fn project_script_path(
    checkout: &Path,
    scripts_root: &Path,
    definition: &RunDefinitionArgs,
) -> Result<PathBuf> {
    let raw = definition
        .cmd
        .get(1)
        .ok_or_else(|| invalid("run cmd requires a script entry"))?;
    let relative = Path::new(raw);
    if relative.is_absolute() || relative.has_root() {
        return Err(not_allowed(
            "project script entry must be relative to the registered checkout",
        ));
    }
    let candidate = checkout.join(relative);
    let canonical = std::fs::canonicalize(&candidate)
        .map_err(|_| not_allowed("project script entry is unavailable"))?;
    if !crate::setup::inside(scripts_root, &canonical) {
        return Err(not_allowed(
            "project script entry is outside registered .eliot/scripts",
        ));
    }
    Ok(candidate)
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
    use eliot_core::{AgentState, Event, EventSubject, ScriptRuleContext};
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

        fn context(&self) -> FrozenScriptContext {
            let subject = EventSubject {
                project: "sample".to_owned(),
                harness: "fake".to_owned(),
                profile: "fake".to_owned(),
                state: AgentState::Busy,
                workspace: Some(self.root.to_string_lossy().into_owned()),
            };
            FrozenScriptContext {
                event: Event {
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
                },
                subject: Some(subject),
                rule: ScriptRuleContext {
                    id: "check-completed".to_owned(),
                    revision: 1,
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
        let command = verify_project_run(&frozen, &fixture.registration, &policy())
            .expect("verify project run");
        let process = command.as_std();
        assert_eq!(
            process.get_program().to_string_lossy(),
            frozen.interpreter.executable.canonical_path
        );
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
                frozen.entry.canonical_path.as_str(),
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
}
