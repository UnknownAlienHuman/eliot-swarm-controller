use std::{cell::Cell, fs, path::PathBuf, rc::Rc};

struct FixtureDirectory(PathBuf);

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn uncertain_allow_equal_retry_requires_durable_decision_identity() {
    let root = std::env::temp_dir().join(format!(
        "script-start-decision-retry-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&root).expect("create isolated ScriptRun directory");
    let _cleanup = FixtureDirectory(root.clone());
    let work = crate::scripts::runner::durable_authority_tests::fixture_work(&root, None);
    let run_dir = crate::scripts::runner::directory(&root, &work.run_id).unwrap();
    fs::create_dir_all(&run_dir).expect("create actual worker protocol directory");
    let decision_path = run_dir.join("start-decision.json");

    let fail_barrier = Rc::new(Cell::new(true));
    let fail_for_scope = Rc::clone(&fail_barrier);
    let barrier_decision_path = decision_path.clone();
    let scoped_barrier = swarm_process::test_support::scoped_durability_barrier(move |path| {
        if path == barrier_decision_path && fail_for_scope.get() {
            Err(swarm_contracts::error::Error::new(
                "PRIVATE_FILE_DURABILITY_UNKNOWN",
                "fixture failed after decision publication",
            ))
        } else {
            swarm_process::sync_parent_directory(path)
        }
    });

    let uncertain = crate::scripts::runner::allow(&work)
        .expect_err("visible Allow without its durability barrier cannot authorize start");
    assert_eq!(uncertain.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
    assert!(
        decision_path.is_file(),
        "the published file remains visible"
    );
    assert!(
        crate::scripts::runner::start_decision(&work).is_err(),
        "the worker decision reader must preserve uncertainty"
    );
    assert!(!run_dir.join("execution-plan.json").exists());
    assert!(!run_dir.join("started.json").exists());
    let visible_bytes = fs::read(&decision_path).expect("read visible uncertain bytes");
    let uncertain_retry = crate::scripts::runner::allow(&work)
        .expect_err("equal retry cannot treat a visible decision as durable");
    assert_eq!(uncertain_retry.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
    assert_eq!(
        fs::read(&decision_path).expect("read unchanged uncertain decision"),
        visible_bytes
    );
    assert!(crate::scripts::runner::start_decision(&work).is_err());
    assert!(!run_dir.join("execution-plan.json").exists());
    assert!(!run_dir.join("started.json").exists());

    fail_barrier.set(false);
    crate::scripts::runner::allow(&work)
        .expect("equal retry may proceed after exact decision identity is durable");
    let retained = crate::scripts::runner::start_decision(&work)
        .expect("worker reads the decision after the durability barrier recovers");
    assert!(matches!(
        &retained,
        swarm_script_worker::StartDecisionRead::Current(
            swarm_script_worker::ScriptStartDecision::Allow {
                run_id,
                operation_id,
                token,
                ..
            }
        ) if run_id == &work.run_id
            && operation_id == &work.operation_id
            && token == &work.token
    ));
    assert_eq!(
        crate::scripts::runner::start_decision_allows_start(&retained).unwrap(),
        Some(true)
    );
    assert_eq!(
        fs::read(&decision_path).expect("read retained decision bytes"),
        visible_bytes,
        "the equal retry retains the original immutable decision"
    );
    drop(scoped_barrier);
}
