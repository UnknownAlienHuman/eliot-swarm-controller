//! Explicit owner setup entry-file references. This module never spawns or writes Store state.

use crate::platform::is_reparse_point;
use eliot_core::{Error, FrozenSetupFile, Result, WorkspaceSetup, WorkspaceSetupArg};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    ffi::OsString,
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Component, Path, PathBuf},
    process::Stdio,
};
use tokio::process::Command;

const MAX_STEPS: usize = 16;
const MAX_ARGS: usize = 64;
const MAX_TEXT_BYTES: usize = 4096;
const MAX_METADATA_BYTES: usize = 128 * 1024;
const MAX_FILE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_PLAN_FILE_BYTES: u64 = 256 * 1024 * 1024;
const MAX_TIMEOUT_MS: u64 = 3_600_000;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SetupEntry {
    program: String,
    args: Vec<SetupArg>,
    timeout_ms: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum SetupArg {
    Literal(String),
    File(FileArg),
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FileArg {
    file: String,
}

pub(crate) fn validate_entries(entries: &[SetupEntry]) -> Result<()> {
    if entries.len() > MAX_STEPS {
        return Err(invalid("setup exceeds 16 entries"));
    }
    check_metadata(entries)?;
    for entry in entries {
        check_timeout(entry.timeout_ms)?;
        check_text(&entry.program)?;
        check_program(Path::new(&entry.program))?;
        if entry.args.len() > MAX_ARGS {
            return Err(invalid("setup entry exceeds 64 arguments"));
        }
        for arg in &entry.args {
            match arg {
                SetupArg::Literal(value) => check_text(value)?,
                SetupArg::File(file) => {
                    check_text(&file.file)?;
                    if file.file.is_empty() {
                        return Err(invalid("setup file reference must not be empty"));
                    }
                    check_path(Path::new(&file.file))?;
                }
            }
        }
    }
    Ok(())
}

/// Freeze only the selected executable and explicit file arguments, not an import graph.
pub(crate) fn freeze(profile: &Value, repo_root: &Path) -> Result<Vec<WorkspaceSetup>> {
    if !profile.is_object() {
        return Err(invalid("setup requires an admitted profile object"));
    }
    let Some(raw) = profile.get("setup") else {
        return Ok(Vec::new());
    };
    let entries: Vec<SetupEntry> = serde_json::from_value(raw.clone())
        .map_err(|_| invalid("setup requires explicit program, args and timeout_ms entries"))?;
    validate_entries(&entries)?;
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    let root = canonical_directory(repo_root)?;
    let mut remaining = MAX_PLAN_FILE_BYTES;
    let mut frozen = Vec::with_capacity(entries.len());
    for entry in entries {
        // Selecting an executable does not extend the allowed file-reference root.
        let program = capture_file(Path::new(&entry.program), None, &mut remaining)?;
        let mut args = Vec::with_capacity(entry.args.len());
        for arg in entry.args {
            args.push(match arg {
                SetupArg::Literal(value) => WorkspaceSetupArg::Literal(value),
                SetupArg::File(file) => {
                    let reference = Path::new(&file.file);
                    let path = if reference.is_absolute() {
                        reference.to_path_buf()
                    } else {
                        root.join(reference)
                    };
                    WorkspaceSetupArg::File(capture_file(&path, Some(&root), &mut remaining)?)
                }
            });
        }
        frozen.push(WorkspaceSetup {
            program,
            args,
            timeout_ms: entry.timeout_ms,
        });
    }
    check_metadata(&frozen)?;
    Ok(frozen)
}

/// Construct a command only after checking the same admitted references and contents.
pub(crate) fn verify_command(
    entry: &WorkspaceSetup,
    workspace: &Path,
    environment: &[(OsString, OsString)],
) -> Result<Command> {
    check_timeout(entry.timeout_ms)?;
    check_metadata(entry)?;
    if entry.args.len() > MAX_ARGS {
        return Err(invalid("setup entry exceeds 64 arguments"));
    }
    let workspace = canonical_directory(workspace)?;
    let mut remaining = MAX_PLAN_FILE_BYTES;
    check_program(Path::new(&entry.program.canonical_path))?;
    verify_file(&entry.program, &mut remaining)?;
    let mut args = Vec::with_capacity(entry.args.len());
    for arg in &entry.args {
        args.push(match arg {
            WorkspaceSetupArg::Literal(value) => {
                check_text(value)?;
                OsString::from(value)
            }
            WorkspaceSetupArg::File(file) => {
                verify_file(file, &mut remaining)?;
                file_argument(file)?
            }
        });
    }
    check_environment(environment)?;
    let mut command = Command::new(&entry.program.canonical_path);
    command
        .args(args)
        .current_dir(workspace)
        .env_clear()
        .envs(environment.iter().map(|(name, value)| (name, value)))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    Ok(command)
}

/// Execution-time values stay in memory; incomplete Agent identity prevents owner fallback.
pub(crate) fn build_environment(
    profile: &Value,
    agent: &str,
    target_generation: i64,
) -> Result<Vec<(OsString, OsString)>> {
    if !valid_agent_id(agent) || target_generation <= 0 {
        return Err(invalid(
            "setup requires an exact Agent and positive target generation",
        ));
    }
    let mut environment = crate::plugin_host::build_process_environment(profile)?;
    environment.insert("ELIOT_AGENT_ID".to_owned(), Value::String(agent.to_owned()));
    environment.insert(
        "ELIOT_AGENT_GENERATION".to_owned(),
        Value::String(target_generation.to_string()),
    );
    environment
        .into_iter()
        .map(|(name, value)| {
            let value = value
                .as_str()
                .ok_or_else(|| invalid("setup environment values must be strings"))?;
            Ok((OsString::from(name), OsString::from(value)))
        })
        .collect()
}

fn check_environment(environment: &[(OsString, OsString)]) -> Result<()> {
    let mut agent = false;
    let mut generation = false;
    let mut seen = std::collections::HashSet::new();
    for (name, value) in environment {
        if value.to_string_lossy().contains('\0') {
            return Err(invalid("setup environment values cannot contain NUL"));
        }
        let name = name
            .to_str()
            .ok_or_else(|| invalid("setup environment names must be Unicode"))?;
        let mut chars = name.chars();
        if !matches!(chars.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
            || !chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
        {
            return Err(invalid("setup environment contains an invalid name"));
        }
        #[cfg(windows)]
        let key = name.to_ascii_uppercase();
        #[cfg(not(windows))]
        let key = name.to_owned();
        if !seen.insert(key) {
            return Err(invalid("setup environment contains a repeated name"));
        }
        if name == "ELIOT_AGENT_ID" {
            agent = value.to_str().is_some_and(valid_agent_id);
        } else if name == "ELIOT_AGENT_GENERATION" {
            generation = value
                .to_str()
                .and_then(|value| value.parse::<i64>().ok())
                .is_some_and(|value| value > 0);
        } else if name.to_ascii_uppercase().starts_with("ELIOT_AGENT_") {
            return Err(invalid(
                "setup environment cannot carry a token or identity override",
            ));
        }
    }
    if !agent || !generation {
        return Err(invalid(
            "setup environment requires Agent identity and target generation",
        ));
    }
    Ok(())
}

fn valid_agent_id(value: &str) -> bool {
    value
        .strip_prefix('a')
        .and_then(|value| value.parse::<i64>().ok())
        .is_some_and(|id| id > 0 && format!("a{id}") == value)
}

pub(crate) fn canonical_directory(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid("setup root must be an absolute admitted directory"));
    }
    check_path(path)?;
    reject_links(path)?;
    let canonical = fs::canonicalize(path).map_err(|_| invalid("setup root is unavailable"))?;
    reject_links(&canonical)?;
    if !canonical.is_dir() {
        return Err(invalid("setup root must be a directory"));
    }
    Ok(canonical)
}

pub(crate) fn capture_file(
    path: &Path,
    root: Option<&Path>,
    remaining: &mut u64,
) -> Result<FrozenSetupFile> {
    check_path(path)?;
    reject_links(path)?;
    let canonical = fs::canonicalize(path).map_err(|_| invalid("setup file is unavailable"))?;
    reject_links(&canonical)?;
    if root.is_some_and(|root| !inside(root, &canonical)) {
        return Err(invalid(
            "setup file reference escapes the admitted owner repository",
        ));
    }
    let metadata = fs::symlink_metadata(&canonical)
        .map_err(|_| invalid("setup file metadata is unavailable"))?;
    if !metadata.is_file() || is_reparse_point(&metadata) {
        return Err(invalid("setup reference must be a regular file"));
    }
    let mut file = open_reference(&canonical)?;
    let before = file
        .metadata()
        .map_err(|_| invalid("setup file metadata is unavailable"))?;
    if !before.is_file()
        || is_reparse_point(&before)
        || before.len() > MAX_FILE_BYTES.min(*remaining)
    {
        return Err(invalid("setup reference must be a bounded regular file"));
    }
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let mut buffer = [0u8; 16 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| invalid("setup file cannot be read"))?;
        if count == 0 {
            break;
        }
        size = size.saturating_add(count as u64);
        if size > MAX_FILE_BYTES.min(*remaining) {
            return Err(invalid("setup file exceeds its bounded read allowance"));
        }
        hasher.update(&buffer[..count]);
    }
    let after = file
        .metadata()
        .map_err(|_| invalid("setup file metadata is unavailable"))?;
    reject_links(path)?;
    reject_links(&canonical)?;
    let current = fs::symlink_metadata(&canonical)
        .map_err(|_| invalid("setup file changed while it was read"))?;
    if size != before.len()
        || after.len() != before.len()
        || after.modified().ok() != before.modified().ok()
        || current.len() != before.len()
        || current.modified().ok() != before.modified().ok()
        || !current.is_file()
        || is_reparse_point(&current)
        || fs::canonicalize(path).ok().as_ref() != Some(&canonical)
    {
        return Err(invalid("setup file changed while it was read"));
    }
    *remaining -= size;
    let canonical_path = canonical
        .to_str()
        .filter(|path| path.len() <= MAX_TEXT_BYTES)
        .ok_or_else(|| invalid("setup canonical path must be bounded Unicode"))?
        .to_owned();
    let sha256 = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(FrozenSetupFile {
        canonical_path,
        size,
        sha256,
    })
}

fn verify_file(expected: &FrozenSetupFile, remaining: &mut u64) -> Result<()> {
    let path = Path::new(&expected.canonical_path);
    if !path.is_absolute() || expected.sha256.len() != 64 {
        return Err(invalid("frozen setup reference is invalid"));
    }
    let current = capture_file(path, None, remaining).map_err(|_| {
        Error::new(
            "setup_input_changed",
            "admitted setup reference is unavailable or changed",
        )
    })?;
    if current != *expected {
        return Err(Error::new(
            "setup_input_changed",
            "admitted setup reference no longer matches its path, size and SHA-256",
        ));
    }
    Ok(())
}

pub(crate) fn file_argument(file: &FrozenSetupFile) -> Result<OsString> {
    #[cfg(windows)]
    {
        use std::path::Prefix;

        // PowerShell 7.6.9.500 -File rejects verbatim spelling accepted by CreateProcess.
        // Change only argv spelling, then prove it still names the admitted reference.
        let canonical = &file.canonical_path;
        let rendered = if let Some(rest) = canonical.strip_prefix(r"\\?\UNC\") {
            format!(r"\\{rest}")
        } else if let Some(rest) = canonical.strip_prefix(r"\\?\") {
            rest.to_owned()
        } else {
            canonical.clone()
        };
        let path = Path::new(&rendered);
        let changed = || {
            Error::new(
                "setup_input_changed",
                "normal Windows setup argument does not resolve to the admitted canonical file",
            )
        };
        if !path.is_absolute()
            || !matches!(
                path.components().next(),
                Some(Component::Prefix(prefix))
                    if matches!(prefix.kind(), Prefix::Disk(_) | Prefix::UNC(_, _))
            )
        {
            return Err(changed());
        }
        check_path(path).map_err(|_| changed())?;
        reject_links(path).map_err(|_| changed())?;
        let round_trip = fs::canonicalize(path).map_err(|_| changed())?;
        if round_trip.as_path() != Path::new(canonical) {
            return Err(changed());
        }
        reject_links(&round_trip).map_err(|_| changed())?;
        Ok(OsString::from(rendered))
    }
    #[cfg(not(windows))]
    {
        Ok(OsString::from(&file.canonical_path))
    }
}

fn open_reference(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options
        .open(path)
        .map_err(|_| invalid("setup file cannot be opened without following links"))
}

pub(crate) fn check_program(path: &Path) -> Result<()> {
    if !path.is_absolute() {
        return Err(invalid("setup program must be an absolute executable path"));
    }
    check_path(path)?;
    #[cfg(windows)]
    if path
        .extension()
        .and_then(|value| value.to_str())
        .is_some_and(|extension| {
            extension.eq_ignore_ascii_case("bat") || extension.eq_ignore_ascii_case("cmd")
        })
    {
        return Err(invalid(
            "setup batch files require an explicitly selected interpreter",
        ));
    }
    Ok(())
}

fn check_path(path: &Path) -> Result<()> {
    let text = path
        .to_str()
        .ok_or_else(|| invalid("setup paths must be Unicode"))?;
    check_text(text)?;
    if path.has_root() && !path.is_absolute() {
        return Err(invalid("setup paths cannot have an ambiguous root"));
    }
    if text.split(['/', '\\']).any(|part| part == "..") {
        return Err(invalid("setup paths cannot contain parent traversal"));
    }
    for component in path.components() {
        match component {
            Component::ParentDir => {
                return Err(invalid("setup paths cannot contain parent traversal"));
            }
            Component::Prefix(prefix) => {
                use std::path::Prefix;
                if !path.is_absolute()
                    || !matches!(
                        prefix.kind(),
                        Prefix::Disk(_)
                            | Prefix::VerbatimDisk(_)
                            | Prefix::UNC(_, _)
                            | Prefix::VerbatimUNC(_, _)
                    )
                {
                    return Err(invalid("setup device paths are not allowed"));
                }
            }
            Component::Normal(name) => {
                let name = name
                    .to_str()
                    .ok_or_else(|| invalid("setup paths must be Unicode"))?;
                if name.contains(':') {
                    return Err(invalid("setup alternate streams are not allowed"));
                }
                #[cfg(windows)]
                {
                    let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();
                    if name.ends_with([' ', '.'])
                        || matches!(
                            stem.as_str(),
                            "CON" | "PRN" | "AUX" | "NUL" | "CLOCK$" | "CONIN$" | "CONOUT$"
                        )
                        || ["COM", "LPT"].iter().any(|prefix| {
                            stem.strip_prefix(*prefix).is_some_and(|suffix| {
                                matches!(
                                    suffix,
                                    "1" | "2"
                                        | "3"
                                        | "4"
                                        | "5"
                                        | "6"
                                        | "7"
                                        | "8"
                                        | "9"
                                        | "¹"
                                        | "²"
                                        | "³"
                                )
                            })
                        })
                    {
                        return Err(invalid(
                            "setup device or ambiguous path components are not allowed",
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn reject_links(path: &Path) -> Result<()> {
    for ancestor in path.ancestors() {
        let metadata = fs::symlink_metadata(ancestor)
            .map_err(|_| invalid("setup path cannot be inspected"))?;
        if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
            return Err(invalid("setup symlink or reparse paths are not allowed"));
        }
    }
    Ok(())
}

pub(crate) fn inside(root: &Path, candidate: &Path) -> bool {
    let base: Vec<_> = root.components().collect();
    let path: Vec<_> = candidate.components().collect();
    path.len() > base.len()
        && base.iter().zip(&path).all(|(left, right)| {
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
        })
}

fn check_text(value: &str) -> Result<()> {
    if value.len() > MAX_TEXT_BYTES || value.contains('\0') {
        return Err(invalid("setup text must be at most 4096 bytes without NUL"));
    }
    Ok(())
}

fn check_timeout(timeout_ms: u64) -> Result<()> {
    if timeout_ms == 0 || timeout_ms > MAX_TIMEOUT_MS {
        return Err(invalid("setup timeout_ms must be between 1 and 3600000"));
    }
    Ok(())
}

fn check_metadata<T: Serialize + ?Sized>(value: &T) -> Result<()> {
    if serde_json::to_vec(value)
        .map_err(|_| invalid("setup metadata could not be encoded"))?
        .len()
        > MAX_METADATA_BYTES
    {
        return Err(invalid("setup metadata exceeds 128 KiB"));
    }
    Ok(())
}

fn invalid(message: &str) -> Error {
    Error::new("profile_error", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reparse_checks_allow_regular_file_and_directory_metadata() {
        let root = fs::canonicalize(env!("CARGO_MANIFEST_DIR")).unwrap();
        let file = root.join("Cargo.toml");
        let metadata = fs::symlink_metadata(&file).unwrap();
        assert!(metadata.is_file());
        assert!(!is_reparse_point(&metadata));
        let metadata = fs::symlink_metadata(&root).unwrap();
        assert!(metadata.is_dir());
        assert!(!is_reparse_point(&metadata));
        reject_links(&file).unwrap();
        crate::git_monitor::reject_links(&file).unwrap();
    }

    #[test]
    fn link_checks_preserve_missing_setup_and_git_policies() {
        let missing = fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .join(format!("eliot missing reparse {}", ulid::Ulid::new()))
            .join("deleted.txt");
        assert_eq!(
            fs::symlink_metadata(&missing).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
        let error = reject_links(&missing).unwrap_err();
        assert_eq!(error.code, "profile_error");
        assert_eq!(error.message, "setup path cannot be inspected");
        crate::git_monitor::reject_links(&missing).unwrap();

        let frozen = FrozenSetupFile {
            canonical_path: missing.to_str().unwrap().to_owned(),
            size: 0,
            sha256: "0".repeat(64),
        };
        let mut remaining = MAX_PLAN_FILE_BYTES;
        assert_eq!(
            verify_file(&frozen, &mut remaining).unwrap_err().code,
            "setup_input_changed"
        );
    }
}
