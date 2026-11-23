//! OpenCode install discovery: an npm-installed OpenCode resolves through
//! its shim to the native exe inside the package. The shim itself
//! (`opencode.cmd`) must never be frozen.

use eliot_core as core;
use eliot_plugin::install::{
    InstallDiscovery, InstallStrategy, find_on_path, is_native_executable, is_script_wrapper,
    path_search_dirs, roaming_roots, user_profile,
};
use std::path::{Path, PathBuf};

/// The OpenCode install declaration read by shared install code.
pub fn discovery() -> InstallDiscovery {
    InstallDiscovery {
        name: "opencode",
        exe_stems: &["opencode"],
        strategy: InstallStrategy::FirstExisting,
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

pub(crate) fn candidate_paths() -> core::Result<Vec<PathBuf>> {
    if cfg!(windows) {
        candidate_paths_windows()
    } else {
        candidate_paths_unix()
    }
}

/// Candidate native executables for an npm-installed OpenCode, in priority
/// order: the shim target
/// (`<shim-dir>/node_modules/@opencode/cli/bin/opencode.exe`) for every
/// discovered `opencode.cmd` shim, then each known package-local path under
/// every roaming root (the shim itself must never be frozen).
fn package_candidates(shim_dirs: &[PathBuf], roaming: &[PathBuf]) -> Vec<PathBuf> {
    const PACKAGE: &[&str] = &["node_modules", "@opencode", "cli"];
    fn package_bin(root: &Path, extra: &[&str]) -> PathBuf {
        let mut path = root.to_owned();
        for segment in PACKAGE.iter().chain(extra.iter()) {
            path = path.join(segment);
        }
        path.join("bin").join("opencode.exe")
    }
    let mut candidates = Vec::new();
    let mut push = |path: PathBuf| {
        if !candidates.contains(&path) {
            candidates.push(path);
        }
    };
    for dir in shim_dirs {
        push(package_bin(dir, &[]));
    }
    for root in roaming {
        let npm = root.join("npm");
        push(package_bin(&npm, &[]));
        for arch in ["cli-windows-x64", "cli-windows-x64-baseline"] {
            push(package_bin(&npm, &["node_modules", "@opencode", arch]));
        }
    }
    candidates
}

fn shim_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let mut push = |dir: PathBuf| {
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    };
    for dir in path_search_dirs() {
        if dir.join("opencode.cmd").is_file() {
            push(dir);
        }
    }
    for root in roaming_roots() {
        push(root.join("npm"));
    }
    dirs
}

fn candidate_paths_windows() -> core::Result<Vec<PathBuf>> {
    // Follow the npm shim to the native exe inside the package: the shim
    // itself (opencode.cmd) must never be frozen.
    let roaming = roaming_roots();
    let candidates = package_candidates(&shim_dirs(), &roaming);
    let mut searched: Vec<String> = candidates
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
    for candidate in &candidates {
        if candidate.is_file() {
            let path = candidate.to_string_lossy().into_owned();
            if !is_script_wrapper(&path) {
                return Ok(vec![candidate.clone()]);
            }
        }
    }
    if let Some(found) = find_on_path("opencode.exe") {
        let path = found.to_string_lossy().into_owned();
        if !is_script_wrapper(&path) && Path::new(&path).is_absolute() {
            return Ok(vec![found]);
        }
        searched.push("opencode.exe on PATH (wrapper or non-absolute)".to_owned());
    } else {
        searched.push("opencode.exe on PATH".to_owned());
    }
    Err(not_found_error(!candidates.is_empty(), &searched))
}

/// Unix OpenCode: the bare `opencode` binary. The Windows npm `.cmd` shim
/// layout does not exist here and is never consulted.
fn candidate_paths_unix() -> core::Result<Vec<PathBuf>> {
    let mut searched = vec!["opencode on PATH".to_owned()];
    let mut candidates = Vec::new();
    for dir in path_search_dirs() {
        let exe = dir.join("opencode");
        if is_native_executable(&exe) {
            candidates.push(exe);
        }
    }
    for fixed in [
        Path::new("/usr/local/bin/opencode").to_path_buf(),
        Path::new("/opt/opencode/.opencode/bin/opencode").to_path_buf(),
    ] {
        searched.push(fixed.to_string_lossy().into_owned());
        if is_native_executable(&fixed) && !candidates.contains(&fixed) {
            candidates.push(fixed);
        }
    }
    if let Some(home) = user_profile() {
        let exe = home.join(".opencode").join("bin").join("opencode");
        searched.push(exe.to_string_lossy().into_owned());
        if is_native_executable(&exe) && !candidates.contains(&exe) {
            candidates.push(exe);
        }
    }
    for candidate in &candidates {
        if candidate.is_file() {
            let path = candidate.to_string_lossy().into_owned();
            if !is_script_wrapper(&path) && Path::new(&path).is_absolute() {
                return Ok(vec![candidate.clone()]);
            }
        }
    }
    Err(not_found_error(!candidates.is_empty(), &searched))
}

fn searched_locations() -> Vec<String> {
    if cfg!(windows) {
        let roaming = roaming_roots();
        let mut searched: Vec<String> = package_candidates(&shim_dirs(), &roaming)
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect();
        searched.push("opencode.exe on PATH".to_owned());
        searched
    } else {
        let mut searched = vec![
            "opencode on PATH".to_owned(),
            "/usr/local/bin/opencode".to_owned(),
            "/opt/opencode/.opencode/bin/opencode".to_owned(),
        ];
        if let Some(home) = user_profile() {
            searched.push(
                home.join(".opencode")
                    .join("bin")
                    .join("opencode")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        searched
    }
}

fn not_found_error(_any_candidates: bool, searched: &[String]) -> core::Error {
    core::Error::new(
        "unsupported",
        format!(
            "opencode native executable was not found; searched {}",
            searched.join(", ")
        ),
    )
}
