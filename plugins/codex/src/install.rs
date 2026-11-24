//! Codex install discovery: the harness launches `codex` by name, so
//! there is no install search. Readers pin the resolved executable later.

use eliot_core as core;
use eliot_plugin::install::{InstallDiscovery, InstallStrategy};
use std::path::PathBuf;

/// The Codex install declaration read by shared install code.
pub fn discovery() -> InstallDiscovery {
    InstallDiscovery {
        name: "codex",
        model_hold_revision: None,
        #[cfg(windows)]
        launcher_update: None,
        exe_stems: &["codex"],
        strategy: InstallStrategy::ByName,
        cli_identity: None,
        capability_probe: None,
        allow_script_wrapper: true,
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

fn candidate_paths() -> core::Result<Vec<PathBuf>> {
    Ok(Vec::new())
}

fn searched_locations() -> Vec<String> {
    vec!["codex on PATH".to_owned()]
}

fn not_found_error(_any_candidates: bool, searched: &[String]) -> core::Error {
    core::Error::new(
        "unsupported",
        format!(
            "codex native executable was not found; searched {}",
            searched.join(", ")
        ),
    )
}
