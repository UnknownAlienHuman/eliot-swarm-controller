//! Antigravity install discovery: the `agy` CLI serves both `acp`
//! (launch) and `models` (catalogue). The Electron app path is named only
//! for guidance and is never launched, executed or version-probed.

use eliot_core as core;
use eliot_plugin::install::{
    InstallDiscovery, InstallProbe, InstallStrategy, find_on_path, is_script_wrapper, local_roots,
};
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Required CLI file stem: only `agy` implements the native ACP/model CLI.
pub const CLI_STEM: &str = "agy";

/// Local-roots-relative IDE subpath, named for guidance only when no CLI is
/// found. The IDE is never returned, executed or version-probed.
pub const IDE_HINT_SUBPATH: &[&str] = &["Programs", "Antigravity", "Antigravity.exe"];

/// The Antigravity install declaration read by shared install code.
pub fn discovery() -> InstallDiscovery {
    InstallDiscovery {
        name: "antigravity",
        model_hold_revision: None,
        #[cfg(windows)]
        launcher_update: None,
        exe_stems: &["agy"],
        strategy: InstallStrategy::FirstExisting,
        cli_identity: Some(CLI_STEM),
        capability_probe: Some(InstallProbe {
            args: &["acp", "--help"],
            verify_executable: has_cli_module,
            refusal: "executable did not establish Antigravity CLI ACP capability",
        }),
        allow_script_wrapper: false,
        ide_hint_subpath: Some(IDE_HINT_SUBPATH),
        timeout_label: None,
        candidates: candidate_paths,
        searched: searched_locations,
        not_found: not_found_error,
        ide_missing: Some(ide_missing_error),
        ide_unsupported: Some(ide_unsupported_error),
        timeout_newer: None,
    }
}

/// The CLI has no passive capability receipt. Its Go module distinguishes
/// the native payload from an IDE copied to a file named `agy` before execution.
fn has_cli_module(program: &Path, deadline: Instant) -> bool {
    use std::io::Read;
    const MODULE: &[u8] = b"google-antigravity/antigravity-cli";
    let Ok(mut file) = std::fs::File::open(program) else {
        return false;
    };
    if !file
        .metadata()
        .is_ok_and(|metadata| metadata.len() <= 512 * 1024 * 1024)
    {
        return false;
    }
    let mut bytes = vec![0; 64 * 1024];
    let mut carry = 0;
    while Instant::now() < deadline {
        let Ok(read) = file.read(&mut bytes[carry..]) else {
            return false;
        };
        if read == 0 {
            return false;
        }
        let end = carry + read;
        if bytes[..end]
            .windows(MODULE.len())
            .any(|window| window == MODULE)
        {
            return true;
        }
        carry = (MODULE.len() - 1).min(end);
        bytes.copy_within(end - carry..end, 0);
    }
    false
}

pub(crate) fn candidate_paths() -> core::Result<Vec<PathBuf>> {
    if let Some(found) = find_on_path("agy.exe").or_else(|| find_on_path("agy")) {
        let path = found.to_string_lossy().into_owned();
        if !is_script_wrapper(&path) && Path::new(&path).is_absolute() {
            return Ok(vec![found]);
        }
    }
    Ok(Vec::new())
}

fn searched_locations() -> Vec<String> {
    let mut searched: Vec<String> = vec!["agy.exe on PATH".to_owned()];
    if cfg!(not(windows)) {
        searched.push("agy on PATH".to_owned());
    }
    // An IDE-only install is reported with actionable guidance: name where
    // the IDE was seen so the message points at the fix without launching it.
    for local in local_roots() {
        let mut native = local;
        for segment in IDE_HINT_SUBPATH {
            native = native.join(segment);
        }
        searched.push(native.to_string_lossy().into_owned());
    }
    searched
}

fn not_found_error(_any_candidates: bool, searched: &[String]) -> core::Error {
    core::Error::new(
        "unsupported",
        format!(
            "antigravity native executable was not found; searched {}",
            searched.join(", ")
        ),
    )
}

/// Error for an `agy`-less install. The IDE path, when seen, is named for
/// guidance only: it is never returned, executed or version-probed.
fn ide_missing_error(ide: &str, searched: &[String]) -> core::Error {
    core::Error::new(
        "unsupported",
        format!(
            "antigravity native CLI `agy` was not found on PATH; the Antigravity IDE at {ide} does not implement the required native ACP/model CLI and is never launched for discovery. Install the `agy` CLI and put it on PATH; searched {}",
            searched.join(", ")
        ),
    )
}

/// Typed refusal for an IDE-named Antigravity cmd: named for guidance only,
/// never returned, executed or version-probed.
fn ide_unsupported_error(cmd: &str) -> core::Error {
    core::Error::new(
        "unsupported",
        format!(
            "antigravity cmd points at the Antigravity IDE ({cmd}), which does not implement the required native ACP/model CLI and is never launched for discovery; install the `agy` CLI and put it on PATH"
        ),
    )
}
