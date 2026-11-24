//! Muse install discovery: the native payload is pinned through the
//! vendor launcher directory, never resolved from PATH (a PATH `muse`
//! names the launcher wrapper whose PID would orphan the real host).

#[cfg(windows)]
mod update;
#[cfg(windows)]
pub(crate) use update::installed_channel;

use eliot_core as core;
use eliot_plugin::install::{
    InstallDiscovery, InstallStrategy, find_on_path, is_native_executable, is_script_wrapper,
    launcher_manifest_version,
};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};

/// Vendor launcher directory the native Muse payload installs under.
pub const LAUNCHER_DIR: &str = r"C:\Tools\MuseCode";

/// Version-file names read inside the launcher directory, in order.
pub const LAUNCHER_VERSION_FILES: &[&str] = &["muse-version", ".muse-version"];

/// The launcher directory is small; refuse fallback discovery if it has
/// grown beyond this bound instead of selecting from a partial enumeration.
const MAX_LAUNCHER_DIRECTORY_ENTRIES: usize = 128;

/// The Muse install declaration read by shared install code.
pub fn discovery() -> InstallDiscovery {
    InstallDiscovery {
        name: "muse",
        model_hold_revision: Some(model_hold_revision),
        #[cfg(windows)]
        launcher_update: Some(update::due_launcher_update),
        exe_stems: &["muse"],
        strategy: InstallStrategy::MetadataVersion(payload_version),
        cli_identity: None,
        capability_probe: None,
        allow_script_wrapper: false,
        ide_hint_subpath: None,
        timeout_label: None,
        candidates: candidate_paths,
        searched: searched_locations,
        not_found: not_found_error,
        ide_missing: None,
        ide_unsupported: None,
        timeout_newer: None,
    }
}

pub(crate) fn model_hold_revision(
    environment: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    model_hold_revision_for_path(&model_hold_settings_path(environment)?)
}

pub(crate) fn model_hold_settings_path(
    environment: &serde_json::Map<String, serde_json::Value>,
) -> Option<PathBuf> {
    let lookup = |name: &str| {
        environment
            .iter()
            .find(|(key, _)| {
                if cfg!(windows) {
                    key.eq_ignore_ascii_case(name)
                } else {
                    key.as_str() == name
                }
            })
            .and_then(|(_, value)| value.as_str())
            .filter(|value| !value.is_empty())
    };
    let directory = if let Some(path) = lookup("XDG_CONFIG_HOME") {
        PathBuf::from(path)
    } else {
        Path::new(lookup("HOME").or_else(|| lookup("USERPROFILE"))?).join(".config")
    };
    directory
        .is_absolute()
        .then(|| directory.join("muse").join("settings.json"))
}

pub(crate) fn model_hold_revision_for_path(path: &Path) -> Option<String> {
    const MAX_SETTINGS_BYTES: u64 = 1024 * 1024;
    let settings = match File::open(path) {
        Ok(file) => {
            let mut bytes = Vec::new();
            file.take(MAX_SETTINGS_BYTES + 1)
                .read_to_end(&mut bytes)
                .ok()?;
            if bytes.len() as u64 > MAX_SETTINGS_BYTES {
                return None;
            }
            Some(core::api::inference::sha256_hex(&bytes))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(_) => return None,
    };
    let binary = candidate_paths().ok()?.into_iter().next()?;
    let metadata = binary.metadata().ok()?;
    let installed = (binary, metadata.len(), metadata.modified().ok()?);
    let encoded = serde_json::to_vec(&(installed, path, settings)).ok()?;
    Some(core::api::inference::sha256_hex(&encoded))
}

fn payload_version(path: &Path) -> Option<String> {
    if !cfg!(windows) {
        return None;
    }
    let (expected, version) = manifest_payload(Path::new(LAUNCHER_DIR))?;
    path.to_str()?
        .eq_ignore_ascii_case(expected.to_str()?)
        .then_some(version)
}

fn manifest_payload(directory: &Path) -> Option<(PathBuf, String)> {
    let version = launcher_manifest_version(directory, LAUNCHER_VERSION_FILES)?;
    let path = directory.join(format!("muse-bin-{version}.exe"));
    fs::symlink_metadata(&path)
        .ok()?
        .is_file()
        .then_some((path, version))
}

pub(crate) fn candidate_paths() -> core::Result<Vec<PathBuf>> {
    if cfg!(windows) {
        candidate_paths_windows(Path::new(LAUNCHER_DIR))
    } else {
        candidate_paths_unix()
    }
}

/// Unix Muse: the native payload installs directly, so PATH plus the
/// documented absolute path win. No `--version` probe runs here; the binary
/// is validated on use.
fn candidate_paths_unix() -> core::Result<Vec<PathBuf>> {
    let mut searched = searched_locations();
    if let Some(found) = find_on_path("muse") {
        let path = found.to_string_lossy().into_owned();
        if !is_script_wrapper(&path) && Path::new(&path).is_absolute() {
            return Ok(vec![found]);
        }
        searched.push(format!("muse on PATH (wrapper or non-absolute: {path})"));
    }
    let fixed = Path::new("/usr/local/bin/muse");
    if is_native_executable(fixed) {
        return Ok(vec![fixed.to_path_buf()]);
    }
    Err(not_found_error(false, &searched))
}

fn candidate_paths_windows(launcher_dir: &Path) -> core::Result<Vec<PathBuf>> {
    if let Some((native, _)) = manifest_payload(launcher_dir) {
        return Ok(vec![native]);
    }

    // A sole regular payload can launch without a verified version.
    let refusal = |reason| {
        core::Error::new(
            "profile_error",
            format!(
                "muse native executable {reason}; searched {}",
                launcher_dir.display()
            ),
        )
    };
    let entries = fs::read_dir(launcher_dir).map_err(|_| refusal("directory is unavailable"))?;
    let mut selected = None;
    for (index, entry) in entries.enumerate() {
        if index == MAX_LAUNCHER_DIRECTORY_ENTRIES {
            return Err(refusal("inventory exceeds the discovery bound"));
        }
        let entry = entry.map_err(|_| refusal("inventory could not be read"))?;
        let name = entry.file_name().to_ascii_lowercase();
        let is_payload = name
            .to_str()
            .and_then(|name| name.strip_prefix("muse-bin-"))
            .and_then(|suffix| suffix.strip_suffix(".exe"))
            .is_some_and(|suffix| {
                !suffix.is_empty() && suffix.len() <= 64 && !suffix.chars().any(char::is_control)
            });
        if !is_payload {
            continue;
        }
        let kind = entry
            .file_type()
            .map_err(|_| refusal("identity could not be read"))?;
        if kind.is_symlink() || (kind.is_file() && selected.replace(entry.path()).is_some()) {
            return Err(refusal(
                "selection is ambiguous or redirects through a link",
            ));
        }
    }
    selected
        .map(|path| vec![path])
        .ok_or_else(|| refusal("was not found"))
}

fn searched_locations() -> Vec<String> {
    if cfg!(windows) {
        vec![
            Path::new(LAUNCHER_DIR)
                .join(".muse-version")
                .to_string_lossy()
                .into_owned(),
        ]
    } else {
        vec!["muse on PATH".to_owned(), "/usr/local/bin/muse".to_owned()]
    }
}

fn not_found_error(_any_candidates: bool, searched: &[String]) -> core::Error {
    core::Error::new(
        "profile_error",
        format!(
            "muse native executable was not found; searched {}",
            searched.join(", ")
        ),
    )
}
