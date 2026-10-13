use super::*;
use std::{cell::Cell, fs, path::PathBuf, rc::Rc};

fn fixture_work(
    root: &std::path::Path,
    executor: Option<swarm_script_worker::ExecutorPin>,
) -> Work {
    let run_id = uuid::Uuid::new_v4().to_string();
    let operation_id = uuid::Uuid::new_v4().to_string();
    let token = uuid::Uuid::new_v4().to_string();
    let interpreter = manifest::InterpreterIdentity {
        kind: manifest::InterpreterKind::Powershell,
        canonical_path: root.join("fixture-interpreter.exe"),
        sha256: "a".repeat(64),
        byte_length: 1,
    };
    let bundle = manifest::ScriptBundle {
        bundle_version: 1,
        script_id: "durability_fixture".to_owned(),
        interpreter: interpreter.clone(),
        entrypoint: "main.ps1".to_owned(),
        argv: Vec::new(),
        trust: manifest::ScriptTrust::TrustedLocal,
        inherit_environment: Vec::new(),
        controller_effects: Vec::new(),
        input_schema: manifest::ScriptValueSchema::Null,
        result_schema: manifest::ScriptValueSchema::Null,
        files: Vec::new(),
    };
    Work {
        run_id: run_id.clone(),
        operation_id: operation_id.clone(),
        token,
        data_dir: root.to_path_buf(),
        bundle_record: ArtifactRecord {
            kind: registry::BUNDLE_KIND.to_owned(),
            artifact_id: format!("script-{}", "b".repeat(64)),
            relative_path: format!("artifacts/script-{}", "b".repeat(64)),
            byte_length: 0,
            content_digest: "c".repeat(64),
            metadata: json!({}),
        },
        bundle,
        interpreter,
        environment: BTreeMap::new(),
        environment_sha256: "d".repeat(64),
        invocation: protocol::ScriptInvocation {
            protocol_version: 1,
            operation_id,
            run_id,
            script_id: "durability_fixture".to_owned(),
            script_revision: 1,
            task_id: None,
            task_revision: None,
            attempt_id: None,
            input: Value::Null,
            controller_effects: Vec::new(),
        },
        executor,
    }
}

struct FixtureDirectory(PathBuf);

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn store_allow_keeps_visible_decision_unknown_before_plan_materialization() {
    let root = std::env::temp_dir().join(format!(
        "script-allow-durable-authority-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&root).unwrap();
    let _cleanup = FixtureDirectory(root.clone());
    let pin = swarm_script_worker::ExecutorPin {
        executable: root.join("fixture-worker.exe"),
        sha256: "e".repeat(64),
        artifact_id: "fixture-worker".to_owned(),
        version: "fixture".to_owned(),
    };
    let work = fixture_work(&root, Some(pin));
    let dir = directory(&root, &work.run_id).unwrap();
    fs::create_dir_all(&dir).unwrap();
    let decision_path = dir.join("start-decision.json");
    let fail_barrier = Rc::new(Cell::new(true));
    let fail_for_scope = Rc::clone(&fail_barrier);
    let scoped_barrier = swarm_process::test_support::scoped_durability_barrier(move |path| {
        if path == decision_path && fail_for_scope.get() {
            Err(swarm_contracts::error::Error::new(
                "PRIVATE_FILE_DURABILITY_UNKNOWN",
                "fixture failed after publication",
            ))
        } else {
            swarm_process::sync_parent_directory(path)
        }
    });

    let publication = allow(&work).expect_err("visible Allow must not count before its barrier");
    assert_eq!(publication.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
    assert!(
        decision_path.is_file(),
        "atomic publication remains visible"
    );
    assert!(
        start_decision(&work).is_err(),
        "worker-side read must also remain unknown while its barrier fails"
    );
    assert!(!dir.join("execution-plan.json").exists());
    assert!(!dir.join("execution-plan-ready.json").exists());
    assert!(!dir.join("started.json").exists());

    fail_barrier.set(false);
    assert!(matches!(
        start_decision(&work).unwrap(),
        swarm_script_worker::StartDecisionRead::Current(
            swarm_script_worker::ScriptStartDecision::Allow { .. }
        )
    ));
    drop(scoped_barrier);
}
