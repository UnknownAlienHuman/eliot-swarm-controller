//! Muse install discovery: the native payload is pinned through the
//! vendor launcher directory, never resolved from PATH (a PATH `muse`
//! names the launcher wrapper whose PID would orphan the real host).

#[cfg(windows)]
mod update;

use eliot_core as core;
use eliot_plugin::install::{
    InstallDiscovery, InstallStrategy, find_on_path, is_native_executable, is_script_wrapper,
    launcher_manifest_version,
};
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

/// Vendor launcher directory the native Muse payload installs under.
pub const LAUNCHER_DIR: &str = r"C:\Tools\MuseCode";

/// Version-file names read inside the launcher directory, in order.
pub const LAUNCHER_VERSION_FILES: &[&str] = &["muse-version", ".muse-version"];

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
    // Windows discovery has already tied this payload to the launcher
    // manifest. Unix has no supported passive version source.
    path.file_stem()?
        .to_str()?
        .strip_prefix("muse-bin-")
        .map(str::to_owned)
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
    let version =
        launcher_manifest_version(launcher_dir, LAUNCHER_VERSION_FILES).ok_or_else(|| {
            core::Error::new(
                "profile_error",
                format!(
                    "muse launcher version is unknown; searched {}",
                    launcher_dir.join(".muse-version").to_string_lossy()
                ),
            )
        })?;
    let file_name = format!("muse-bin-{version}.exe");
    let native = launcher_dir.join(&file_name);
    if native.is_file() {
        return Ok(vec![native]);
    }
    Err(core::Error::new(
        "profile_error",
        format!(
            "muse native executable for launcher version {version} was not found; searched {}",
            native.to_string_lossy()
        ),
    ))
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
