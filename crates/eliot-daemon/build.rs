#[path = "../../build/git_identity.rs"]
mod git_identity;

use std::{fs, path::Path};

const SHARED_CRATES: &[&str] = &["eliot-core", "eliot-daemon", "eliot-store"];
const HARNESS_NAMES: &[&str] = &["codex", "claude", "opencode", "muse", "antigravity"];
// Exact allowances for remaining configuration identity and display vocabulary.
const EXISTING: &[(&str, usize)] = &[
    ("eliot-core/src/api/harness.rs", 6),
    ("eliot-daemon/src/config.rs", 9),
    ("eliot-daemon/src/router/discovery.rs", 5),
];

fn main() {
    let crates = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("daemon crate belongs to the workspace");
    for name in SHARED_CRATES {
        let sources = crates.join(name).join("src");
        println!("cargo:rerun-if-changed={}", sources.display());
        check_shared_harness_names(crates, &sources).expect("shared harness-name boundary");
    }
    git_identity::emit();
}

fn check_shared_harness_names(crates: &Path, sources: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(sources)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            check_shared_harness_names(crates, &path)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let relative = path
                .strip_prefix(crates)
                .expect("source path is below crates")
                .to_string_lossy()
                .replace('\\', "/");
            let allowed = EXISTING
                .iter()
                .find_map(|(file, allowed)| (*file == relative).then_some(*allowed))
                .unwrap_or(0);
            let count = fs::read_to_string(&path)?
                .lines()
                .filter(|line| {
                    line.match_indices(['"', '\'']).any(|(offset, quote)| {
                        let tail = &line[offset + 1..];
                        HARNESS_NAMES.iter().any(|name| {
                            tail.strip_prefix(name)
                                .is_some_and(|rest| rest.starts_with(quote))
                        })
                    })
                })
                .count();
            assert!(
                count <= allowed,
                "{relative} has {count} harness-name lines, allowed {allowed}. Use an adapter capability."
            );
        }
    }
    Ok(())
}
