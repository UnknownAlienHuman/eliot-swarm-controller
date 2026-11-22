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

#[expect(
    clippy::disallowed_types,
    reason = "This public helper returns the editor child command for a script."
)]
pub fn editor_command(editor: &Path, script: &str) -> std::process::Command {
    let environment = GUI_ENV
        .iter()
        .filter_map(|name| std::env::var_os(name).map(|value| (*name, value)));
    editor_command_with_env(editor, script, environment)
}

#[expect(
    clippy::disallowed_types,
    reason = "This helper constructs an editor child command with the selected environment."
)]
fn editor_command_with_env(
    editor: &Path,
    script: &str,
    environment: impl IntoIterator<Item = (impl AsRef<std::ffi::OsStr>, impl AsRef<std::ffi::OsStr>)>,
) -> std::process::Command {
    #[expect(
        clippy::disallowed_methods,
        reason = "This statement constructs a process owned by this module."
    )]
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
