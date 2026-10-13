use super::*;
use crate::scripts::runner::durable_authority_tests::fixture_work;
use std::{fs, path::PathBuf};

struct FixtureDirectory(PathBuf);

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn store_observer_does_not_consume_visible_terminal_without_its_read_barrier() {
    let root = std::env::temp_dir().join(format!(
        "script-terminal-read-barrier-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&root).unwrap();
    let root = fs::canonicalize(&root).unwrap();
    let _cleanup = FixtureDirectory(root.clone());
    let work = fixture_work(&root, None);
    let dir = runner::directory(&root, &work.run_id).unwrap();
    fs::create_dir_all(&dir).unwrap();
    let terminal_path = dir.join("terminal.json");
    fs::write(&terminal_path, b"visible terminal bytes").unwrap();
    let files = ArtifactFiles::new(&root).unwrap();
    let blocked_for_scope = terminal_path.clone();
    let _barrier = swarm_process::test_support::scoped_durability_barrier(move |target| {
        if target == blocked_for_scope {
            Err(swarm_contracts::error::Error::new(
                "PRIVATE_FILE_DURABILITY_UNKNOWN",
                "fixture failed while reading a visible ScriptRun terminal",
            ))
        } else {
            swarm_process::sync_parent_directory(target)
        }
    });

    assert!(runner::terminal_receipt_exists(&root, &work.run_id).unwrap());
    match observe(&work, &files, None) {
        Err(error) => assert_eq!(error.code, "PRIVATE_FILE_DURABILITY_UNKNOWN"),
        Ok(_) => panic!("visible terminal bytes became a Store completion without a barrier"),
    }
    assert!(!dir.join("cleanup-release.json").exists());
}
