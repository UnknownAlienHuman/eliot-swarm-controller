//! Scripts screen logic, pure: which timers run a script, the external
//! editor check, and how a Run's command outcome reads.
//!
//! The daemon owns every effect. A script is saved with `script.save` and
//! run only through the existing Owner Run path, `timer.run` of a timer
//! whose `run` action names the entry: the daemon freezes the entry, owns
//! the process tree, applies the timeout and reports `unknown` when it
//! cannot prove the outcome. Global `cmd[1]` is relative to the scripts
//! directory; project `cmd[1]` is relative to the checkout
//! (`.eliot/scripts/...`).

use std::path::{Path, PathBuf};

use eliot_core::api::RuleListEntry;
use eliot_core::{Command, CommandState};

fn normalize(path: &str) -> String {
    let path = path.replace('\\', "/");
    path.strip_prefix("./").unwrap_or(&path).to_owned()
}

/// Timer ids in one namespace whose `run` action names this entry.
pub fn running_timers(rules: &[RuleListEntry], project: Option<&str>, path: &str) -> Vec<String> {
    let wanted = match project {
        None => normalize(path),
        Some(_) => format!(".eliot/scripts/{}", normalize(path)),
    };
    rules
        .iter()
        .filter(|entry| entry.timer.is_some())
        .filter(|entry| {
            entry
                .definition
                .get("do")
                .and_then(|actions| actions.as_array())
                .is_some_and(|actions| {
                    actions.iter().any(|action| {
                        action.get("action").and_then(|value| value.as_str()) == Some("run")
                            && action
                                .get("cmd")
                                .and_then(|cmd| cmd.get(1))
                                .and_then(|entry| entry.as_str())
                                .is_some_and(|entry| normalize(entry) == wanted)
                    })
                })
        })
        .map(|entry| entry.state.id.clone())
        .collect()
}

/// Timers of the script's namespace whose `run` action names it, and one
/// line per reason that list may be incomplete: the namespace could not be
/// read (its error), or only its first page was read. A read failure never
/// looks like "no timer runs this script".
#[cfg(windows)]
pub fn timers_in(
    namespaces: &[crate::config_view::RuleNamespace],
    project: Option<&str>,
    path: &str,
) -> (Vec<String>, Vec<String>) {
    let mut timers = Vec::new();
    let mut issues = Vec::new();
    for namespace in namespaces
        .iter()
        .filter(|namespace| namespace.project.as_deref() == project)
    {
        match &namespace.rules {
            Ok(rules) => {
                timers.extend(running_timers(rules, project, path));
                if namespace.more {
                    issues.push(format!(
                        "Only the first page of {} was read; a timer on a later page may also run this script.",
                        namespace.label
                    ));
                }
            }
            Err(error) => issues.push(format!(
                "{} could not be read ({error}); timers there that run this script are not shown.",
                namespace.label
            )),
        }
    }
    (timers, issues)
}

/// Supported GUI editor executable names. A positive list prevents ordinary
/// interpreter aliases from turning Open in editor into script execution.
/// This is user intent validation, not binary authentication or a sandbox.
const EDITORS: &[&str] = &[
    "notepad",
    "notepad++",
    "code",
    "code - insiders",
    "codium",
    "sublime_text",
    "gvim",
    "zed",
    "notepad4",
];

/// Only the GUI environment needed by these editors. Provider credentials and
/// Eliot identity are absent because the command starts from env_clear().
const GUI_ENV: &[&str] = &[
    "SystemRoot",
    "WINDIR",
    "PATH",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "APPDATA",
    "LOCALAPPDATA",
    "HOMEDRIVE",
    "HOMEPATH",
];

pub fn editor_command(editor: &Path, script: &str) -> std::process::Command {
    let environment = GUI_ENV
        .iter()
        .filter_map(|name| std::env::var_os(name).map(|value| (*name, value)));
    editor_command_with_env(editor, script, environment)
}

fn editor_command_with_env(
    editor: &Path,
    script: &str,
    environment: impl IntoIterator<Item = (impl AsRef<std::ffi::OsStr>, impl AsRef<std::ffi::OsStr>)>,
) -> std::process::Command {
    let mut command = std::process::Command::new(editor);
    command.env_clear().arg(script);
    for (name, value) in environment {
        if GUI_ENV.iter().any(|allowed| {
            name.as_ref()
                .to_string_lossy()
                .eq_ignore_ascii_case(allowed)
        }) {
            command.env(name, value);
        }
    }
    command
}

/// A user-selected editor must be an absolute path to an existing `.exe`.
/// Script hosts and shell-associated files (`.cmd`, `.bat`, `.ps1`, `.lnk`,
/// ...) are refused: the editor is started with literal argv, never through
/// a shell association.
pub fn editor_executable(raw: &str) -> Result<PathBuf, String> {
    let path = Path::new(raw.trim());
    if raw.trim().is_empty() || !path.is_absolute() {
        return Err("choose the editor as an absolute path to its .exe".to_owned());
    }
    let is_exe = path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("exe"));
    if !is_exe {
        return Err("the editor must be an .exe, not a script or shell association".to_owned());
    }
    // Hosts that would execute the entry instead of editing it, and the shell
    // that would follow its file association, are refused by name.
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !EDITORS.contains(&stem.as_str()) {
        return Err(format!(
            "{stem}.exe is not a supported GUI editor; choose Notepad, Notepad++, VS Code, VSCodium, Sublime Text, gVim, Zed or Notepad4"
        ));
    }
    if !path.is_file() {
        return Err(format!("{} does not exist", path.display()));
    }
    Ok(path.to_path_buf())
}

/// True once a command can no longer change.
pub fn is_terminal(state: CommandState) -> bool {
    matches!(
        state,
        CommandState::Applied
            | CommandState::Rejected
            | CommandState::Failed
            | CommandState::Unknown
            | CommandState::Cancelled
    )
}

/// One line for a Run command: state plus the outcome code and cause.
pub fn outcome_line(command: &Command) -> String {
    let state = format!("{:?}", command.state).to_lowercase();
    let outcome = command.outcome.as_ref();
    let code = outcome
        .and_then(|value| value.get("code"))
        .and_then(|value| value.as_str());
    let cause = outcome
        .and_then(|value| value.get("cause"))
        .and_then(|value| value.as_str());
    let exit = outcome
        .and_then(|value| value.pointer("/process/direct_exit/exit_code"))
        .and_then(|value| value.as_i64());
    let mut line = format!("{} {} {state}", command.verb, command.id);
    if let Some(code) = code {
        line.push_str(&format!(" · {code}"));
    }
    if let Some(cause) = cause {
        line.push_str(&format!(" ({cause})"));
    }
    if let Some(exit) = exit {
        line.push_str(&format!(" · exit {exit}"));
    }
    if command.state == CommandState::Unknown {
        line.push_str(" · outcome unknown: the daemon could not prove what the script did; it is never rerun automatically");
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::{RuleStateView, TimerSchedule, TimerStateView};
    use serde_json::json;

    #[test]
    fn editor_command_clears_ambient_and_only_restores_gui_environment() {
        let command = editor_command_with_env(
            Path::new("editor.exe"),
            "literal path.ps1",
            [
                ("SystemRoot", "C:/Windows"),
                ("PATH", "C:/Windows"),
                ("OPENAI_API_KEY", "fixture"),
                ("ANTHROPIC_API_KEY", "fixture"),
                ("ELIOT_AGENT_TOKEN", "fixture"),
                ("COMSPEC", "cmd.exe"),
            ],
        );
        let environment: Vec<_> = command.get_envs().collect();
        assert_eq!(environment.len(), 2);
        assert!(environment.iter().all(|(name, value)| {
            value.is_some()
                && GUI_ENV
                    .iter()
                    .any(|allowed| name.to_string_lossy().eq_ignore_ascii_case(allowed))
        }));
        assert_eq!(
            command.get_args().collect::<Vec<_>>(),
            vec![std::ffi::OsStr::new("literal path.ps1")]
        );
        #[cfg(windows)]
        {
            // Inspect a real child's environment without mutating global test
            // environment or printing any credentials.
            let system_root = std::env::var_os("SystemRoot").unwrap();
            let powershell =
                Path::new(&system_root).join("System32/WindowsPowerShell/v1.0/powershell.exe");
            let mut child =
                editor_command_with_env(&powershell, "-NoProfile", [("SystemRoot", system_root)]);
            let output = child.args(["-NonInteractive", "-Command", "exit ([int][bool]($env:OPENAI_API_KEY -or $env:ANTHROPIC_API_KEY -or $env:ELIOT_AGENT_TOKEN -or $env:COMPUTERNAME))"]).output().unwrap();
            assert!(output.status.success());
        }
    }

    #[test]
    fn interpreter_aliases_are_refused_before_file_existence_checks() {
        for host in [
            "python3",
            "pythonw",
            "py",
            "ruby",
            "perl",
            "node",
            "deno",
            "bun",
            "php",
            "lua",
            "Rscript",
            "osascript",
            "wscript",
            "cscript",
            "mshta",
            "bash",
            "sh",
            "zsh",
            "fish",
            "nu",
            "python3.14",
            "unknown",
        ] {
            let path = std::env::temp_dir().join(format!("{host}.exe"));
            assert!(
                editor_executable(path.to_str().unwrap())
                    .unwrap_err()
                    .contains("not a supported GUI editor"),
                "{host}"
            );
        }
    }

    fn timer(id: &str, cmd: serde_json::Value) -> RuleListEntry {
        RuleListEntry {
            state: RuleStateView {
                id: id.to_owned(),
                project: None,
                revision: 1,
                current_cursor: 0,
                retiring_revision: None,
                retiring_cursor: None,
                retire_through: None,
                enabled: true,
                paused_reason: None,
            },
            timer: Some(TimerStateView {
                id: id.to_owned(),
                project: None,
                schedule_revision: 1,
                schedule: TimerSchedule::Once { at: 0 },
                anchor: 0,
                last_key: None,
                next_due_at: None,
                enabled: true,
            }),
            definition: json!({"on": {"at": "2099-01-01T00:00:00Z"}, "do": [{"action": "run", "cmd": cmd}]}),
        }
    }

    #[cfg(windows)]
    #[test]
    fn unreadable_or_partial_namespaces_are_reported_not_hidden() {
        use crate::config_view::RuleNamespace;
        let namespaces = vec![
            RuleNamespace {
                project: None,
                label: "Global rules".to_owned(),
                rules: Ok(vec![timer("g", json!(["pwsh", "check.ps1"]))]),
                more: true,
            },
            RuleNamespace {
                project: Some("app".to_owned()),
                label: "Project app".to_owned(),
                rules: Err("unavailable: rule store busy".to_owned()),
                more: false,
            },
        ];
        let (timers, issues) = timers_in(&namespaces, None, "check.ps1");
        assert_eq!(timers, vec!["g".to_owned()]);
        assert_eq!(issues.len(), 1);
        assert!(issues[0].contains("Only the first page of Global rules"));
        // The project namespace failed: no Run action, but the error shows.
        let (timers, issues) = timers_in(&namespaces, Some("app"), "check.ps1");
        assert!(timers.is_empty());
        assert_eq!(
            issues,
            vec![
                "Project app could not be read (unavailable: rule store busy); timers there that run this script are not shown."
                    .to_owned()
            ]
        );
    }

    #[test]
    fn timers_match_global_and_project_entries() {
        let rules = vec![
            timer("g", json!(["pwsh", "deploy\\check.ps1"])),
            timer("p", json!(["pwsh", "./.eliot/scripts/deploy/check.ps1"])),
            timer("other", json!(["pwsh", "x.ps1"])),
        ];
        assert_eq!(
            running_timers(&rules, None, "deploy/check.ps1"),
            vec!["g".to_owned()]
        );
        assert_eq!(
            running_timers(&rules, Some("app"), "deploy/check.ps1"),
            vec!["p".to_owned()]
        );
        let mut event_rule = timer("e", json!(["pwsh", "x.ps1"]));
        event_rule.timer = None;
        assert!(running_timers(&[event_rule], None, "x.ps1").is_empty());
    }

    #[test]
    fn editors_must_be_absolute_exe_files() {
        assert!(editor_executable("notepad.exe").is_err());
        assert!(editor_executable("C:/Windows/System32/cmd.cmd").is_err());
        assert!(editor_executable("C:/nope/editor.exe").is_err());
        if Path::new("C:/Windows/System32/notepad.exe").is_file() {
            assert!(editor_executable("C:/Windows/System32/notepad.exe").is_ok());
        }
        for host in ["C:/Windows/System32/cmd.exe", "C:/Windows/explorer.exe"] {
            if Path::new(host).is_file() {
                assert!(editor_executable(host).is_err(), "{host}");
            }
        }
        let pwsh = "C:/Program Files/PowerShell/7/pwsh.exe";
        if Path::new(pwsh).is_file() {
            assert!(
                editor_executable(pwsh)
                    .unwrap_err()
                    .contains("not a supported GUI editor")
            );
        }
    }
}
