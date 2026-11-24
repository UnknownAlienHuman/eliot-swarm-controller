//! Claude install discovery: enumerate native CLI installations across
//! versioned roots and PATH. Shared install code probes the enumerated
//! candidates and selects the newest verified `--version`.

use eliot_core as core;
use eliot_plugin::install::{
    InstallDiscovery, InstallStrategy, is_native_executable, local_roots, native_exe_name,
    parse_cli_version, path_search_dirs, roaming_roots, user_profile,
};
use std::path::{Path, PathBuf};

/// The Claude install declaration read by shared install code.
pub fn discovery() -> InstallDiscovery {
    InstallDiscovery {
        name: "claude",
        model_hold_revision: None,
        exe_stems: &["claude"],
        strategy: InstallStrategy::NewestVerified,
        cli_identity: None,
        capability_probe: None,
        allow_script_wrapper: false,
        ide_hint_subpath: None,
        timeout_label: Some(timeout_version_label),
        candidates: candidate_paths,
        searched: searched_locations,
        not_found: not_found_error,
        ide_missing: None,
        ide_unsupported: None,
        timeout_newer: Some(timeout_newer_error),
    }
}

pub(crate) fn candidate_paths() -> core::Result<Vec<PathBuf>> {
    Ok(enumerate_candidate_paths())
}

type VersionedCliPath = (PathBuf, Option<(u64, u64, u64)>);

fn versioned_paths(root: &Path, executable: &str, nested_hash: bool) -> Vec<VersionedCliPath> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut versions = entries
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            (path.is_dir() || (!nested_hash && is_native_executable(&path))).then(|| {
                let version = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(parse_cli_version);
                (path, version)
            })
        })
        .collect::<Vec<_>>();
    versions.sort_by(|(left_path, left_version), (right_path, right_version)| {
        right_version
            .cmp(left_version)
            .then_with(|| left_path.cmp(right_path))
    });

    let mut candidates = Vec::new();
    for (version_path, version) in versions {
        if nested_hash {
            let Ok(hash_entries) = std::fs::read_dir(&version_path) else {
                continue;
            };
            let mut hash_dirs = hash_entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.is_dir())
                .collect::<Vec<_>>();
            hash_dirs.sort();
            for hash_dir in hash_dirs {
                let path = hash_dir.join(executable);
                if is_native_executable(&path) {
                    candidates.push((path, version));
                }
            }
        } else {
            let path = if version_path.is_file() {
                version_path
            } else {
                version_path.join(executable)
            };
            if is_native_executable(&path) {
                candidates.push((path, version));
            }
        }
    }
    candidates
}

/// Native versioned roots precede PATH so semver candidates are probed first.
/// Standard user locations follow. Unix layouts follow vendor installers.
fn enumerate_candidate_paths() -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut push = |path: PathBuf| {
        if !paths.contains(&path) {
            paths.push(path);
        }
    };
    let executable = native_exe_name("claude");
    let mut versioned = Vec::new();
    if cfg!(windows) {
        for roaming in roaming_roots() {
            versioned.extend(versioned_paths(
                &roaming.join("Claude").join("claude-code"),
                &executable,
                true,
            ));
        }
    }
    if let Some(home) = user_profile() {
        versioned.extend(versioned_paths(
            &home
                .join(".local")
                .join("share")
                .join("claude")
                .join("versions"),
            &executable,
            false,
        ));
    }
    versioned.sort_by(|(left_path, left_version), (right_path, right_version)| {
        right_version
            .cmp(left_version)
            .then_with(|| left_path.cmp(right_path))
    });
    for (path, _) in versioned {
        push(path);
    }
    if cfg!(windows) {
        for dir in path_search_dirs() {
            if !dir.is_absolute() {
                continue;
            }
            let exe = dir.join(native_exe_name("claude"));
            if exe.is_file() {
                push(exe);
            }
        }
        if let Some(home) = user_profile() {
            push(
                home.join(".local")
                    .join("bin")
                    .join(native_exe_name("claude")),
            );
        }
        for roaming in roaming_roots() {
            push(roaming.join("npm").join(native_exe_name("claude")));
        }
        for local in local_roots() {
            push(
                local
                    .join("Programs")
                    .join("claude")
                    .join(native_exe_name("claude")),
            );
        }
    } else {
        for dir in path_search_dirs() {
            // Absolute native payloads only: a relative PATH entry is never
            // probed, and an extensionless script never counts as native.
            if !dir.is_absolute() {
                continue;
            }
            let exe = dir.join("claude");
            if is_native_executable(&exe) {
                push(exe);
            }
        }
        let fixed = Path::new("/usr/local/bin/claude");
        if is_native_executable(fixed) {
            push(fixed.to_path_buf());
        }
        if let Some(home) = user_profile() {
            push(home.join(".local").join("bin").join("claude"));
        }
    }
    paths
}

fn searched_locations() -> Vec<String> {
    if cfg!(not(windows)) {
        let mut searched = vec![
            "claude on PATH".to_owned(),
            "/usr/local/bin/claude".to_owned(),
        ];
        if let Some(home) = user_profile() {
            searched.push(
                home.join(".local")
                    .join("bin")
                    .join("claude")
                    .to_string_lossy()
                    .into_owned(),
            );
            searched.push(
                home.join(".local")
                    .join("share")
                    .join("claude")
                    .join("versions")
                    .join("<version>")
                    .join("claude")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        searched.dedup();
        return searched;
    }
    let mut searched = vec!["claude.exe on PATH".to_owned()];
    if let Some(home) = user_profile() {
        searched.push(
            home.join(".local")
                .join("bin")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
        searched.push(
            home.join(".local")
                .join("share")
                .join("claude")
                .join("versions")
                .join("<version>")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    for roaming in roaming_roots() {
        searched.push(
            roaming
                .join("Claude")
                .join("claude-code")
                .join("<version>")
                .join("<hash>")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
        searched.push(
            roaming
                .join("npm")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    for local in local_roots() {
        searched.push(
            local
                .join("Programs")
                .join("claude")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    searched.dedup();
    searched
}

/// Version label for a timed-out candidate: the directory name under a
/// versioned root (`claude-code` or `versions`), so a newer install that
/// timed out during `--version` refuses instead of losing to an older probe.
fn timeout_version_label(cmd: &str) -> Option<String> {
    Path::new(cmd)
        .ancestors()
        .take(4)
        .find(|entry| {
            entry
                .parent()
                .and_then(Path::file_name)
                .is_some_and(|parent| {
                    parent.eq_ignore_ascii_case("claude-code") || parent == "versions"
                })
        })
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .map(str::to_owned)
}

fn not_found_error(any_candidates: bool, searched: &[String]) -> core::Error {
    let detail = if !any_candidates {
        "Standalone Claude CLI was not found"
    } else {
        "Claude CLI executables were found, but none had a successful parseable --version result"
    };
    core::Error::new(
        "profile_error",
        format!(
            "{detail}; searched {}. Install Claude Code or select a native executable explicitly with --cmd",
            searched.join(", ")
        ),
    )
}

fn timeout_newer_error() -> core::Error {
    core::Error::new(
        "timeout",
        "a newer installed Claude CLI timed out during --version",
    )
}
