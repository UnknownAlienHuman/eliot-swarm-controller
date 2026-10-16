use super::*;
use crate::checks::model::Parser;
use std::{
    cell::{Cell, RefCell},
    fs,
    path::PathBuf,
    rc::Rc,
};
use swarm_checks::CheckIdentity;

struct FixtureDirectory(PathBuf);

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn fixture_work() -> (FixtureDirectory, Work, PathBuf) {
    let root =
        std::env::temp_dir().join(format!("check-durable-authority-{}", uuid::Uuid::new_v4()));
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
        profile: CheckProfile {
            profile_id: "profile".to_owned(),
            profile_revision: "1".to_owned(),
            executable: PathBuf::from("fixture-command-not-started"),
            args: Vec::new(),
            parser: Parser::ExitCode,
            resource: "target".to_owned(),
            environment: Default::default(),
            inherit_env: Vec::new(),
            expected_targets: Vec::new(),
            reproducible: false,
            fingerprint_env: Vec::new(),
            versioned_inputs: BTreeMap::new(),
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
    let dir = directory(&root, &work.check_id).unwrap();
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
                "fixture failed at the CheckRun durability boundary",
            ))
        } else {
            swarm_process::sync_parent_directory(target)
        }
    })
}

#[test]
fn check_go_reader_does_not_return_start_until_its_read_barrier_succeeds() {
    // The executor enters a real process-owned Job. Keep it out of the
    // parallel parent harness so unrelated fixture children cannot join it.
    let module = module_path!()
        .split_once("::")
        .expect("fixture module has a crate prefix")
        .1;
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            &format!("{module}::check_go_read_barrier_isolated_child"),
            "--nocapture",
        ])
        .output()
        .expect("run the isolated CheckGo read-barrier fixture");
    assert!(
        output.status.success(),
        "isolated CheckGo fixture failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .any(|line| line == "CHECK_GO_READ_BARRIER_ASSERTIONS_COMPLETE"),
        "successful exit without completed CheckGo assertions is not a pass"
    );
}

#[test]
#[ignore = "spawned by the CheckGo read-barrier regression in an isolated process-owned Job"]
fn check_go_read_barrier_isolated_child() {
    let (_cleanup, work, dir) = fixture_work();
    let go_path = dir.join("go.json");
    let failing = Rc::new(Cell::new(true));
    let _barrier = scoped_failure_for(go_path.clone(), Rc::clone(&failing));

    let publication = allow(&work).expect_err("visible Go must remain an unknown publication");
    assert_eq!(publication.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
    assert!(go_path.is_file());

    let mut control = HostCheckControl::new(&work, &dir);
    let plan_built = Rc::new(Cell::new(false));
    let plan_built_in_closure = Rc::clone(&plan_built);
    let result = swarm_checks::execute_with_plan(
        CheckIdentity {
            check_id: work.check_id.clone(),
            operation_id: work.operation_id.clone(),
            token: work.token.clone(),
        },
        dir.clone(),
        &mut control,
        move || {
            plan_built_in_closure.set(true);
            Err(swarm_contracts::error::Error::new(
                "FIXTURE_PLAN_MUST_NOT_BE_BUILT",
                "unconfirmed Go reached plan materialization",
            ))
        },
        |_execution, _control| Ok(()),
    );
    match result {
        Err(error) => assert_eq!(error.code, "PRIVATE_FILE_DURABILITY_UNKNOWN"),
        Ok(_) => panic!("CheckGo reached command execution before its read barrier"),
    }
    assert!(!plan_built.get());
    assert!(control.started_at_ms.is_none());
    assert!(dir.join("worker.json").is_file());
    assert!(!dir.join("execution.json").exists());
    assert!(!dir.join("stdout").exists());
    assert!(!dir.join("stderr").exists());

    failing.set(false);
    assert_eq!(
        read_value(&go_path).unwrap(),
        json!({"token":work.token.clone()})
    );
    println!("CHECK_GO_READ_BARRIER_ASSERTIONS_COMPLETE");
}

#[test]
fn visible_terminal_completion_and_release_bytes_do_not_yield_a_release_proof() {
    let (_cleanup, work, dir) = fixture_work();
    let files = ArtifactFiles::new(&work.data_dir).unwrap();
    let terminal_path = dir.join("terminal.json");
    let completion_path = dir.join("completion.json");
    fs::write(&terminal_path, b"visible terminal bytes").unwrap();
    assert!(completion(&work, &files).unwrap().is_none());
    fs::write(&completion_path, b"visible completion bytes").unwrap();

    let blocked_path = Rc::new(RefCell::new(Some(completion_path.clone())));
    let blocked_for_scope = Rc::clone(&blocked_path);
    let _barrier = swarm_process::test_support::scoped_durability_barrier(move |target| {
        if blocked_for_scope
            .borrow()
            .as_ref()
            .is_some_and(|path| path == target)
        {
            Err(swarm_contracts::error::Error::new(
                "PRIVATE_FILE_DURABILITY_UNKNOWN",
                "fixture failed while reading a visible CheckRun receipt",
            ))
        } else {
            swarm_process::sync_parent_directory(target)
        }
    });

    assert!(completion(&work, &files).is_err());
    let release_path = dir.join("release-evidence.json");
    assert!(!release_path.exists());

    *blocked_path.borrow_mut() = Some(release_path.clone());
    let process = json!({"fixture":"departed-check-process"});
    assert!(release_evidence(&work, &dir, &process).is_err());
    assert!(
        release_path.is_file(),
        "failed publication leaves bytes visible"
    );
    assert!(release_evidence(&work, &dir, &process).is_err());

    *blocked_path.borrow_mut() = None;
    let proof = release_evidence(&work, &dir, &process).unwrap();
    assert_eq!(proof["method"], "departed_empty");
    assert_eq!(proof["process"], process);
    assert_eq!(proof["token"], work.token);
}
