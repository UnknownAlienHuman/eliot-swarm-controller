use super::*;
use std::{cell::Cell, fs, path::PathBuf, rc::Rc};

struct FixtureDirectory(PathBuf);

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn fixture_work() -> (FixtureDirectory, Work, PathBuf) {
    // Mirrors the CheckRun worker durable-authority donor: a bare retained
    // Work identity with no live child, native send, or Store behind it. PIDs
    // and exit values below are opaque pass-through bytes, never interpreted.
    let root = std::env::temp_dir().join(format!(
        "check-child-departure-durable-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&root).unwrap();
    let root = fs::canonicalize(&root).unwrap();
    let cleanup = FixtureDirectory(root.clone());
    let work = Work {
        check_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        token: uuid::Uuid::new_v4().to_string(),
        data_dir: root.clone(),
        candidate: ArtifactRecord {
            kind: "source_snapshot".to_owned(),
            artifact_id: "candidate-1".to_owned(),
            relative_path: "artifacts/candidate-1".to_owned(),
            byte_length: 0,
            content_digest: "digest".to_owned(),
            metadata: json!({}),
        },
        profile: crate::checks::model::CheckProfile {
            profile_id: "profile".to_owned(),
            profile_revision: "1".to_owned(),
            executable: PathBuf::from("fixture-command-not-started"),
            args: Vec::new(),
            parser: crate::checks::model::Parser::ExitCode,
            resource: "target".to_owned(),
            environment: Default::default(),
            inherit_env: Vec::new(),
            expected_targets: Vec::new(),
            reproducible: false,
            fingerprint_env: Vec::new(),
            versioned_inputs: Default::default(),
        },
        executor: None,
        resolved_inputs: None,
        scope_plan: None,
        input_fingerprint: None,
        preflight_error: None,
        cancel_request: None,
        expected_worker: None,
        launch: None,
    };
    let dir = worker::directory(&root, &work.check_id).unwrap();
    fs::create_dir_all(&dir).unwrap();
    (cleanup, work, dir)
}

fn scoped_failure_for(
    path: PathBuf,
    failing: Rc<Cell<bool>>,
) -> swarm_process::test_support::ScopedDurabilityBarrier {
    swarm_process::test_support::scoped_durability_barrier(move |target| {
        if *target == path && failing.get() {
            Err(swarm_contracts::error::Error::new(
                "PRIVATE_FILE_DURABILITY_UNKNOWN",
                "fixture failed at the CheckRun launch durability boundary",
            ))
        } else {
            swarm_process::sync_parent_directory(target)
        }
    })
}

#[test]
fn child_departure_barrier_failure_stays_unknown_on_equal_retry() {
    let (_cleanup, work, dir) = fixture_work();
    let path = dir.join("launch-departed.json");
    let failing = Rc::new(Cell::new(true));
    let _barrier = scoped_failure_for(path.clone(), Rc::clone(&failing));
    let launch_error = Error::new(
        "FIXTURE_LAUNCH_AMBIGUOUS",
        "fixture retains an ambiguous launch effect",
    );

    // First write: the barrier fails, the error stays explicit, and the exact
    // bytes remain visible on disk.
    let first =
        retain_check_child_departure(&work, 4242, false, None, &launch_error).unwrap_err();
    assert_eq!(first.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
    let visible = fs::read(&path).unwrap();
    assert_eq!(
        visible,
        model::canonical(&check_child_departure_receipt(
            &work,
            4242,
            false,
            None,
            &launch_error
        ))
        .unwrap()
        .into_bytes()
    );

    // Equal-bytes retry under the still-failing barrier must remain an
    // explicit persistence Unknown instead of promoting visibility to success.
    let retry =
        retain_check_child_departure(&work, 4242, false, None, &launch_error).unwrap_err();
    assert_eq!(retry.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
    assert_eq!(fs::read(&path).unwrap(), visible);

    // A healed barrier converges on the identical bytes with no extra files.
    failing.set(false);
    retain_check_child_departure(&work, 4242, false, None, &launch_error).unwrap();
    assert_eq!(fs::read(&path).unwrap(), visible);
    let mut names: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec![std::ffi::OsString::from("launch-departed.json")]
    );
}
