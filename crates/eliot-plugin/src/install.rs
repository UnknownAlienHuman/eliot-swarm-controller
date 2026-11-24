//! Shared native install discovery infrastructure. Each harness adapter owns
//! its declaration and registers it with this process-local registry from the
//! `eliot` binary composition root before daemon dispatch.
//!
//! The filesystem helpers below are pure and process-free: candidate
//! enumeration never launches an executable. Version probing stays with the
//! caller (the daemon uses its bounded native reader; see
//! `crates/eliot-daemon/src/adhoc.rs`).

use eliot_core as core;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::OnceLock,
};

/// Stage and atomically publish one exclusively owned file without replacing
/// an existing destination. The staged contents are synced before publication.
pub fn write_new_file(
    path: &Path,
    contents: &[u8],
    create_error: impl FnOnce() -> core::Error,
    write_error: impl FnOnce() -> core::Error,
) -> core::Result<()> {
    let Some(filename) = path.file_name() else {
        return Err(create_error());
    };
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let Ok(parent) = fs::canonicalize(parent) else {
        return Err(create_error());
    };
    let target = parent.join(filename);
    let temporary = parent.join(format!(".eliot-install.{}.tmp", ulid::Ulid::new()));
    let Ok(mut file) = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
    else {
        return Err(create_error());
    };
    let write_result = file.write_all(contents).and_then(|()| file.sync_all());
    if write_result.is_err() {
        drop(file);
        let _ = fs::remove_file(&temporary);
        return Err(write_error());
    }
    drop(file);
    if publish_new_file(&temporary, &target).is_err() {
        let _ = fs::remove_file(&temporary);
        return Err(create_error());
    }
    Ok(())
}

#[cfg(not(windows))]
fn publish_new_file(temporary: &Path, target: &Path) -> std::io::Result<()> {
    match fs::hard_link(temporary, target) {
        Ok(()) => {
            let _ = fs::remove_file(temporary);
            Ok(())
        }
        // A dangling symlink also owns the destination name.
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Err(error),
        Err(_) => publish_no_replace_rename(temporary, target),
    }
}

#[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
#[allow(
    unsafe_code,
    reason = "Unix no-replace rename requires the native filesystem API."
)]
fn publish_no_replace_rename(temporary: &Path, target: &Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::io::{Error, ErrorKind};
    use std::os::unix::ffi::OsStrExt;

    let source =
        CString::new(temporary.as_os_str().as_bytes()).map_err(|_| ErrorKind::InvalidInput)?;
    let destination =
        CString::new(target.as_os_str().as_bytes()).map_err(|_| ErrorKind::InvalidInput)?;
    // SAFETY: Both buffers are NUL-terminated and live for the call. The flags
    // forbid replacing any destination entry, even a dangling symlink.
    let result = unsafe {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            libc::syscall(
                libc::SYS_renameat2,
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            ) == 0
        }
        #[cfg(target_os = "macos")]
        {
            libc::renameatx_np(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_EXCL,
            ) == 0
        }
    };
    if result {
        Ok(())
    } else {
        Err(Error::last_os_error())
    }
}

#[cfg(not(any(
    windows,
    target_os = "linux",
    target_os = "android",
    target_os = "macos"
)))]
fn publish_no_replace_rename(_temporary: &Path, _target: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic no-replace rename is unavailable on this platform",
    ))
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "Atomic no-replace rename uses the Windows filesystem API."
)]
fn publish_new_file(temporary: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MOVEFILE_WRITE_THROUGH, MoveFileExW};

    let mut source: Vec<u16> = temporary.as_os_str().encode_wide().collect();
    let mut destination: Vec<u16> = target.as_os_str().encode_wide().collect();
    if source.contains(&0) || destination.contains(&0) {
        return Err(std::io::ErrorKind::InvalidInput.into());
    }
    source.push(0);
    destination.push(0);
    // SAFETY: Both buffers are NUL-terminated and live for the whole call.
    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

static INSTALL_DECLARATIONS: OnceLock<HashMap<&'static str, InstallDiscovery>> = OnceLock::new();

/// Vendor-owned version check. The daemon owns its short-lived launcher
/// process, outside the Agent's process custody.
#[cfg(windows)]
pub struct LauncherUpdate {
    pub program: PathBuf,
    pub args: &'static [&'static str],
    pub environment: Vec<(&'static str, String)>,
}

pub type ModelHoldRevision = fn(&serde_json::Map<String, serde_json::Value>) -> Option<String>;

/// How shared install code turns a plugin's enumerated candidates into one
/// executable. `ByName` resolves the stem on PATH with no discovery;
/// `FirstExisting` takes the first enumerated candidate that exists;
/// `NewestVerified` probes candidates with `--version` and takes the newest
/// parseable result.
#[derive(Clone, Copy, Debug)]
pub enum InstallStrategy {
    ByName,
    FirstExisting,
    /// Resolve the first payload, then read its version from install metadata.
    MetadataVersion(fn(&Path) -> Option<String>),
    NewestVerified,
}

/// Adapter-owned readback that verifies the declared executable is a CLI.
#[derive(Clone, Copy)]
pub struct InstallProbe {
    pub args: &'static [&'static str],
    /// Adapter-owned, non-executing identity check before invoking the probe.
    pub verify_executable: fn(&Path, std::time::Instant) -> bool,
    pub refusal: &'static str,
}

/// One adapter-owned install declaration. `candidates` and `searched`
/// enumerate (never probe); `not_found` and the optional error constructors
/// own the adapter's exact diagnostic text. These function pointers stay in
/// the process-local registry and never cross the plugin protocol.
#[derive(Clone, Copy)]
pub struct InstallDiscovery {
    /// The plugin's own diagnostic name, used in errors, never for branching.
    pub name: &'static str,
    /// Process-free fingerprint of the native install and settings for a model hold.
    pub model_hold_revision: Option<ModelHoldRevision>,
    /// Returns an update invocation only when the vendor's check is due.
    #[cfg(windows)]
    pub launcher_update: Option<fn() -> Option<LauncherUpdate>>,
    /// Executable stems to look for (without extension or directory).
    pub exe_stems: &'static [&'static str],
    pub strategy: InstallStrategy,
    /// Required CLI file stem (without extension): a cmd naming any other
    /// executable is refused before any probe. `None` disables the gate.
    pub cli_identity: Option<&'static str>,
    /// Native capability readback required before the executable is selected.
    pub capability_probe: Option<InstallProbe>,
    /// Whether an explicit executable pin may name a script wrapper. Only
    /// the PATH-resolved harness keeps this; every discovered native
    /// executable must be a real payload.
    pub allow_script_wrapper: bool,
    /// Local-roots-relative IDE subpath named only for guidance when no CLI
    /// is found (never launched or probed). `None` disables the hint.
    pub ide_hint_subpath: Option<&'static [&'static str]>,
    /// Extract a timed-out candidate's version label from its path (used by
    /// `NewestVerified` to refuse when a newer install timed out). `None`
    /// disables the rule.
    pub timeout_label: Option<fn(cmd: &str) -> Option<String>>,
    pub candidates: fn() -> core::Result<Vec<PathBuf>>,
    pub searched: fn() -> Vec<String>,
    /// Exact typed refusal when no candidate resolved. The flag reports
    /// whether any candidate path existed at all.
    pub not_found: fn(any_candidates: bool, searched: &[String]) -> core::Error,
    pub ide_missing: Option<fn(ide: &str, searched: &[String]) -> core::Error>,
    pub ide_unsupported: Option<fn(cmd: &str) -> core::Error>,
    pub timeout_newer: Option<fn() -> core::Error>,
}

/// Register the built-in adapter declarations once, before daemon dispatch.
/// Duplicate names and repeated initialization are composition errors.
pub fn register_declarations(
    declarations: impl IntoIterator<Item = InstallDiscovery>,
) -> core::Result<()> {
    let mut registered = HashMap::new();
    for declaration in declarations {
        if registered.insert(declaration.name, declaration).is_some() {
            return Err(core::Error::new(
                "internal",
                "duplicate native install declaration",
            ));
        }
    }
    if registered.is_empty() {
        return Err(core::Error::new(
            "internal",
            "native install declaration registry cannot be empty",
        ));
    }
    INSTALL_DECLARATIONS.set(registered).map_err(|_| {
        core::Error::new(
            "internal",
            "native install declarations were already registered",
        )
    })
}

/// Read one adapter-owned declaration by its protocol harness name.
pub fn discovery_for(harness: &str) -> core::Result<InstallDiscovery> {
    let declarations = INSTALL_DECLARATIONS.get().ok_or_else(|| {
        core::Error::new(
            "internal",
            "native install declarations were not registered by the binary",
        )
    })?;
    declarations
        .get(harness)
        .copied()
        .ok_or_else(|| core::Error::new("unsupported", "unsupported native discovery harness"))
}

/// Whether `cmd` names the required CLI stem (without extension), before any
/// probe. An IDE-named path (or any other name) never qualifies.
pub fn cli_identity_matches(cmd: &str, stem: &str) -> bool {
    let file = cmd
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(cmd)
        .to_ascii_lowercase();
    file.strip_suffix(".exe").unwrap_or(&file) == stem
}

pub fn path_search_dirs() -> Vec<PathBuf> {
    std::env::var_os("PATH").map_or_else(Vec::new, |paths| std::env::split_paths(&paths).collect())
}

/// Native executable file name for `stem` on this platform: `stem.exe` on
/// Windows, the bare stem on Unix.
pub fn native_exe_name(stem: &str) -> String {
    if cfg!(windows) {
        format!("{stem}.exe")
    } else {
        stem.to_owned()
    }
}

pub fn find_on_path(file: &str) -> Option<PathBuf> {
    let target = file.to_ascii_lowercase();
    for dir in path_search_dirs() {
        let candidate = dir.join(file);
        if is_native_executable(&candidate)
            && candidate
                .file_name()
                .is_some_and(|name| name.to_string_lossy().to_ascii_lowercase() == target)
        {
            return Some(candidate);
        }
    }
    None
}

/// Returns true when the path names a script wrapper that must never be
/// frozen as a native executable.
pub fn is_script_wrapper(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".cmd")
        || lower.ends_with(".bat")
        || lower.ends_with(".ps1")
        || lower.ends_with(".js")
}

/// Unix discovery accepts ELF or Mach-O payloads, including resolved
/// explicit commands. Windows keeps its native executable naming checks.
pub fn is_native_executable(path: &std::path::Path) -> bool {
    if !path.is_file() {
        return false;
    }
    if cfg!(windows) {
        return true;
    }
    if is_script_wrapper(&path.to_string_lossy()) {
        return false;
    }
    has_native_executable_magic(path)
}

fn has_native_executable_magic(path: &std::path::Path) -> bool {
    use std::io::Read;
    let mut header = [0u8; 4];
    if std::fs::File::open(path)
        .and_then(|mut file| file.read_exact(&mut header))
        .is_err()
    {
        return false;
    }
    matches!(
        header,
        [0x7f, b'E', b'L', b'F'] // ELF
            | [0xfe, 0xed, 0xfa, 0xce] // Mach-O 32-bit
            | [0xfe, 0xed, 0xfa, 0xcf] // Mach-O 64-bit
            | [0xce, 0xfa, 0xed, 0xfe] // Mach-O 32-bit, byte-swapped
            | [0xcf, 0xfa, 0xed, 0xfe] // Mach-O 64-bit, byte-swapped
            | [0xca, 0xfe, 0xba, 0xbe] // Mach-O universal (fat)
            | [0xbe, 0xba, 0xfe, 0xca] // Mach-O universal, byte-swapped
    )
}

pub fn user_profile() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
}

/// Well-known data roots for one Windows shell folder, without probing the
/// filesystem: the owning environment variable first, then the
/// `USERPROFILE`-derived default.
pub fn data_roots(
    variable: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
    leaf: &str,
) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(dir) = variable {
        roots.push(PathBuf::from(dir));
    }
    if let Some(home) = home {
        let derived = PathBuf::from(home).join("AppData").join(leaf);
        if !roots.contains(&derived) {
            roots.push(derived);
        }
    }
    roots
}

pub fn local_roots() -> Vec<PathBuf> {
    data_roots(
        std::env::var_os("LOCALAPPDATA").or_else(|| std::env::var_os("APPDATA")),
        user_profile().map(|path| path.into_os_string()),
        "Local",
    )
}

pub fn roaming_roots() -> Vec<PathBuf> {
    data_roots(
        std::env::var_os("APPDATA"),
        user_profile().map(|path| path.into_os_string()),
        "Roaming",
    )
}

type CliVersion = (u64, u64, u64);

/// Newest-first ordering key for `<exe> --version` output: the first
/// `major.minor.patch` numeric triple on the bounded first line
/// (`2.1.325 (build)`, `1.4.20-R8619.1`).
pub fn parse_cli_version(text: &str) -> Option<CliVersion> {
    let first = text.lines().next().unwrap_or_default();
    for token in first.split(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '-')) {
        let base = token.split('-').next().unwrap_or(token);
        let mut parts = base.split('.');
        let triple = match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(major), Some(minor), Some(patch), None) => {
                match (major.parse(), minor.parse(), patch.parse()) {
                    (Ok(major), Ok(minor), Ok(patch)) => Some((major, minor, patch)),
                    _ => None,
                }
            }
            _ => None,
        };
        if triple.is_some() {
            return triple;
        }
    }
    None
}

/// Read a vendor launcher version file: the trimmed content of the first
/// present name, bounded and control-free.
pub fn launcher_manifest_version(dir: &Path, names: &[&str]) -> Option<String> {
    for name in names {
        let file = dir.join(name);
        if let Ok(text) = std::fs::read_to_string(&file) {
            let version = text.trim().to_owned();
            if !version.is_empty()
                && version.len() <= 64
                && !version.chars().any(char::is_control)
                && !version.contains(['/', '\\', ':', '*', '?', '"', '<', '>', '|'])
            {
                return Some(version);
            }
        }
    }
    None
}

/// Cap on version probes per candidate enumeration: filesystem hits are
/// cheap, process launches are not.
pub const MAX_DISCOVERY_CANDIDATE_PROBES: usize = 8;

/// Version candidate paths, newest first, probing at most
/// [`MAX_DISCOVERY_CANDIDATE_PROBES`] native executables: paths past the cap
/// stay listed without a version so inventory stays complete while launches
/// stay bounded. Automatic selection accepts only candidates with a
/// successful, parseable `--version`; inventory retains every path and any
/// raw first line. The stable sort keeps parsed versions newest-first and
/// other paths in discovery order at the end.
pub fn probe_candidate_paths_with(
    paths: Vec<PathBuf>,
    mut probe: impl FnMut(&str) -> Option<String>,
) -> Vec<core::api::HarnessCandidate> {
    /// One discovered install plus its parsed `--version` triple, if any.
    type VersionedCandidate = (core::api::HarnessCandidate, Option<(u64, u64, u64)>);
    // Absolute native payloads only, before any probe: a relative candidate
    // is never launched for `--version`, and on Unix an extensionless script
    // never counts as a native executable.
    let existing: Vec<PathBuf> = paths
        .into_iter()
        .filter(|path| path.is_absolute() && is_native_executable(path))
        .collect();
    let mut candidates: Vec<VersionedCandidate> = existing
        .iter()
        .take(MAX_DISCOVERY_CANDIDATE_PROBES)
        .map(|path| {
            let cmd = path.to_string_lossy().into_owned();
            let version = probe(&cmd);
            (
                core::api::HarnessCandidate {
                    cmd,
                    version: version.clone(),
                },
                version.as_deref().and_then(parse_cli_version),
            )
        })
        .collect();
    let mut unversioned: Vec<VersionedCandidate> = existing
        .iter()
        .skip(MAX_DISCOVERY_CANDIDATE_PROBES)
        .map(|path| {
            (
                core::api::HarnessCandidate {
                    cmd: path.to_string_lossy().into_owned(),
                    version: None,
                },
                None,
            )
        })
        .collect();
    candidates.append(&mut unversioned);
    candidates.retain(|(candidate, _)| {
        Path::new(&candidate.cmd).is_absolute() && !is_script_wrapper(&candidate.cmd)
    });
    // Stable sort: versioned newest-first, unversioned last in discovery
    // order. Ties keep the stable priority order above.
    candidates.sort_by(|left, right| match (&left.1, &right.1) {
        (Some(left), Some(right)) => right.cmp(left),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    candidates
        .into_iter()
        .map(|(candidate, _)| candidate)
        .collect()
}
