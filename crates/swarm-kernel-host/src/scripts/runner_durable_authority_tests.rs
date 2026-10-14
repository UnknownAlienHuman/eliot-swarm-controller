use super::*;
use crate::scripts::protocol;
use std::{cell::Cell, fs, path::PathBuf, rc::Rc, sync::Barrier, thread};

pub(crate) fn fixture_work(
    root: &std::path::Path,
    executor: Option<swarm_script_worker::ExecutorPin>,
) -> Work {
    fixture_work_with_identity(
        root,
        &uuid::Uuid::new_v4().to_string(),
        &uuid::Uuid::new_v4().to_string(),
        &uuid::Uuid::new_v4().to_string(),
        executor,
    )
}

pub(crate) fn fixture_work_with_identity(
    root: &std::path::Path,
    run_id: &str,
    operation_id: &str,
    token: &str,
    executor: Option<swarm_script_worker::ExecutorPin>,
) -> Work {
    let run_id = run_id.to_owned();
    let operation_id = operation_id.to_owned();
    let token = token.to_owned();
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
    let barrier_decision_path = decision_path.clone();
    let scoped_barrier = swarm_process::test_support::scoped_durability_barrier(move |path| {
        if path == barrier_decision_path && fail_for_scope.get() {
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

#[test]
fn concurrent_worker_reader_cannot_bypass_inflight_allow_barrier() {
    use std::time::Duration;

    let root = std::env::temp_dir().join(format!(
        "script-allow-reader-barrier-race-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&root).unwrap();
    let _cleanup = FixtureDirectory(root.clone());
    let work = fixture_work(&root, None);
    let dir = directory(&root, &work.run_id).unwrap();
    fs::create_dir_all(&dir).unwrap();
    let decision_path = dir.join("start-decision.json");
    let publication_path = decision_path.clone();
    let (visible_tx, visible_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let publisher_work = work.clone();
    let publisher = thread::spawn(move || {
        let should_pause = Rc::new(Cell::new(true));
        let pause_once = Rc::clone(&should_pause);
        let _barrier = swarm_process::test_support::scoped_durability_barrier(move |path| {
            if path == publication_path && pause_once.replace(false) {
                visible_tx
                    .send(())
                    .expect("reader waits for the published decision");
                resume_rx
                    .recv_timeout(Duration::from_secs(10))
                    .expect("test resumes the publisher after checking the reader");
            }
            swarm_process::sync_parent_directory(path)
        });
        allow(&publisher_work)
    });

    visible_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("publisher exposes the decision before completing its barrier");
    let visible_bytes = fs::read(&decision_path).unwrap();
    let reader_path = decision_path.clone();
    let failed_barrier = swarm_process::test_support::scoped_durability_barrier(move |path| {
        if path == reader_path {
            Err(swarm_contracts::error::Error::new(
                "PRIVATE_FILE_DURABILITY_UNKNOWN",
                "fixture reader barrier remains unconfirmed",
            ))
        } else {
            swarm_process::sync_parent_directory(path)
        }
    });
    let read_while_unknown = start_decision(&work);
    let bytes_while_unknown = fs::read(&decision_path).unwrap();
    let plan_while_unknown = dir.join("execution-plan.json").exists();
    drop(failed_barrier);
    resume_tx
        .send(())
        .expect("release the publisher's durability barrier");
    publisher
        .join()
        .expect("publisher thread completes")
        .unwrap();

    let error =
        read_while_unknown.expect_err("reader cannot return Current while its barrier fails");
    assert_eq!(error.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
    assert_eq!(bytes_while_unknown, visible_bytes);
    assert!(
        !plan_while_unknown,
        "unknown Allow does not materialize a plan"
    );
    assert!(matches!(
        start_decision(&work).unwrap(),
        swarm_script_worker::StartDecisionRead::Current(
            swarm_script_worker::ScriptStartDecision::Allow { .. }
        )
    ));
    assert_eq!(fs::read(&decision_path).unwrap(), visible_bytes);
}

#[test]
fn store_deny_keeps_visible_decision_unknown_until_retry_barrier() {
    let root = std::env::temp_dir().join(format!(
        "script-deny-durable-authority-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&root).unwrap();
    let _cleanup = FixtureDirectory(root.clone());
    let work = fixture_work(&root, None);
    let dir = directory(&root, &work.run_id).unwrap();
    fs::create_dir_all(&dir).unwrap();
    let decision_path = dir.join("start-decision.json");
    let fail_barrier = Rc::new(Cell::new(true));
    let fail_for_scope = Rc::clone(&fail_barrier);
    let barrier_decision_path = decision_path.clone();
    let scoped_barrier = swarm_process::test_support::scoped_durability_barrier(move |path| {
        if path == barrier_decision_path && fail_for_scope.get() {
            Err(swarm_contracts::error::Error::new(
                "PRIVATE_FILE_DURABILITY_UNKNOWN",
                "fixture failed after Deny publication",
            ))
        } else {
            swarm_process::sync_parent_directory(path)
        }
    });

    let publication = deny(&work, "DENIED").expect_err("unconfirmed Deny stays unknown");
    assert_eq!(publication.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
    let visible_bytes = fs::read(&decision_path).expect("Deny bytes remain visible");
    let read_error = start_decision(&work).expect_err("Deny read remains durability-unknown");
    assert_eq!(read_error.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
    assert!(!dir.join("execution-plan.json").exists());
    assert!(!dir.join("started.json").exists());

    fail_barrier.set(false);
    deny(&work, "DENIED").expect("same Deny converges after its barrier succeeds");
    let retained = start_decision(&work).unwrap();
    assert_eq!(start_decision_allows_start(&retained).unwrap(), Some(false));
    assert_eq!(fs::read(&decision_path).unwrap(), visible_bytes);
    drop(scoped_barrier);
}

#[test]
fn store_allow_and_deny_race_retains_one_decision_across_reopen() {
    let root = std::env::temp_dir().join(format!("script-decision-race-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&root).unwrap();
    let _cleanup = FixtureDirectory(root.clone());
    let work = fixture_work(&root, None);
    let dir = directory(&root, &work.run_id).unwrap();
    fs::create_dir_all(&dir).unwrap();
    let gate = Arc::new(Barrier::new(2));
    let allow_work = work.clone();
    let allow_gate = Arc::clone(&gate);
    let allower = thread::spawn(move || {
        allow_gate.wait();
        allow(&allow_work)
    });

    gate.wait();
    let deny_result = deny(&work, "DENIED");
    let allow_result = allower.join().unwrap();
    let retained = start_decision(&work).unwrap();
    let allows_start = start_decision_allows_start(&retained).unwrap();
    assert!(allow_result.is_ok() ^ deny_result.is_ok());
    assert_eq!(allow_result.is_ok(), allows_start == Some(true));
    assert_eq!(deny_result.is_ok(), allows_start == Some(false));

    let reopened =
        fixture_work_with_identity(&root, &work.run_id, &work.operation_id, &work.token, None);
    assert_eq!(start_decision(&reopened).unwrap(), retained);
    let retained_bytes = fs::read(dir.join("start-decision.json")).unwrap();
    if allows_start == Some(true) {
        allow(&reopened).unwrap();
    } else {
        deny(&reopened, "DENIED").unwrap();
    }
    assert_eq!(
        fs::read(dir.join("start-decision.json")).unwrap(),
        retained_bytes
    );
    assert!(!dir.join("execution-plan.json").exists());
}

#[test]
fn store_start_decision_reads_legacy_receipts_and_rejects_ambiguity() {
    let legacy_allow_root =
        std::env::temp_dir().join(format!("script-legacy-allow-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&legacy_allow_root).unwrap();
    let _legacy_allow_cleanup = FixtureDirectory(legacy_allow_root.clone());
    let legacy_allow = fixture_work(&legacy_allow_root, None);
    let allow_dir = directory(&legacy_allow_root, &legacy_allow.run_id).unwrap();
    fs::create_dir_all(&allow_dir).unwrap();
    fs::write(
        allow_dir.join("go.json"),
        serde_json::to_vec(&json!({
            "run_id":legacy_allow.run_id.clone(),
            "operation_id":legacy_allow.operation_id.clone(),
            "token":legacy_allow.token.clone()
        }))
        .unwrap(),
    )
    .unwrap();
    let reopened_allow = fixture_work_with_identity(
        &legacy_allow_root,
        &legacy_allow.run_id,
        &legacy_allow.operation_id,
        &legacy_allow.token,
        None,
    );
    assert_eq!(
        start_decision(&reopened_allow).unwrap(),
        swarm_script_worker::StartDecisionRead::LegacyAllow
    );
    assert!(has_start_gate(&reopened_allow).unwrap());

    let legacy_deny_root =
        std::env::temp_dir().join(format!("script-legacy-deny-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&legacy_deny_root).unwrap();
    let _legacy_deny_cleanup = FixtureDirectory(legacy_deny_root.clone());
    let legacy_deny = fixture_work(&legacy_deny_root, None);
    let deny_dir = directory(&legacy_deny_root, &legacy_deny.run_id).unwrap();
    fs::create_dir_all(&deny_dir).unwrap();
    fs::write(
        deny_dir.join("deny.json"),
        serde_json::to_vec(&json!({
            "run_id":legacy_deny.run_id.clone(),
            "operation_id":legacy_deny.operation_id.clone(),
            "token":legacy_deny.token.clone(),
            "error_code":"DENIED"
        }))
        .unwrap(),
    )
    .unwrap();
    let reopened_deny = fixture_work_with_identity(
        &legacy_deny_root,
        &legacy_deny.run_id,
        &legacy_deny.operation_id,
        &legacy_deny.token,
        None,
    );
    assert_eq!(
        start_decision(&reopened_deny).unwrap(),
        swarm_script_worker::StartDecisionRead::LegacyDeny {
            error_code: "DENIED".to_owned()
        }
    );
    assert!(!has_start_gate(&reopened_deny).unwrap());

    let ambiguous_root =
        std::env::temp_dir().join(format!("script-legacy-ambiguous-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&ambiguous_root).unwrap();
    let _ambiguous_cleanup = FixtureDirectory(ambiguous_root.clone());
    let ambiguous = fixture_work(&ambiguous_root, None);
    let ambiguous_dir = directory(&ambiguous_root, &ambiguous.run_id).unwrap();
    fs::create_dir_all(&ambiguous_dir).unwrap();
    fs::write(
        ambiguous_dir.join("go.json"),
        serde_json::to_vec(&json!({
            "run_id":ambiguous.run_id.clone(),
            "operation_id":ambiguous.operation_id.clone(),
            "token":ambiguous.token.clone()
        }))
        .unwrap(),
    )
    .unwrap();
    fs::write(
        ambiguous_dir.join("deny.json"),
        serde_json::to_vec(&json!({
            "run_id":ambiguous.run_id.clone(),
            "operation_id":ambiguous.operation_id.clone(),
            "token":ambiguous.token.clone(),
            "error_code":"DENIED"
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        start_decision(&ambiguous).unwrap(),
        swarm_script_worker::StartDecisionRead::Ambiguous
    );
    assert!(start_decision_allows_start(&start_decision(&ambiguous).unwrap()).is_err());

    let mixed_root = std::env::temp_dir().join(format!(
        "script-current-legacy-ambiguous-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&mixed_root).unwrap();
    let _mixed_cleanup = FixtureDirectory(mixed_root.clone());
    let mixed = fixture_work(&mixed_root, None);
    let mixed_dir = directory(&mixed_root, &mixed.run_id).unwrap();
    fs::create_dir_all(&mixed_dir).unwrap();
    allow(&mixed).unwrap();
    fs::write(
        mixed_dir.join("go.json"),
        serde_json::to_vec(&json!({
            "run_id":mixed.run_id.clone(),
            "operation_id":mixed.operation_id.clone(),
            "token":mixed.token.clone()
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        start_decision(&mixed).unwrap(),
        swarm_script_worker::StartDecisionRead::Ambiguous
    );
    assert!(has_start_gate(&mixed).is_err());
}

#[test]
#[cfg(any(unix, windows))]
fn store_allow_rejects_a_reparse_decision_path_without_changing_its_target() {
    let fixture_root =
        std::env::temp_dir().join(format!("script-reparse-decision-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&fixture_root).unwrap();
    let root = fs::canonicalize(&fixture_root).unwrap();
    let _cleanup = FixtureDirectory(root.clone());
    let work = fixture_work(&root, None);
    let dir = directory(&root, &work.run_id).unwrap();
    #[cfg(windows)]
    let junction_dir = directory(&fixture_root, &work.run_id).unwrap();
    let target_dir = fixture_root.join("retained-target");
    fs::create_dir_all(&target_dir).unwrap();
    let target = target_dir.join("start-decision.json");
    let retained = b"owner bytes";
    fs::write(&target, retained).unwrap();

    #[cfg(unix)]
    {
        fs::create_dir_all(&dir).unwrap();
        let decision = dir.join("start-decision.json");
        create_reparse_link(&target, &decision).unwrap();
    }
    #[cfg(windows)]
    {
        assert!(
            !junction_dir.exists(),
            "run directory must not exist before junction creation"
        );
        let junction_parent = junction_dir
            .parent()
            .expect("script run directory has a parent");
        fs::create_dir_all(junction_parent).unwrap();
        let canonical_parent = fs::canonicalize(junction_parent).unwrap();
        assert!(
            canonical_parent.starts_with(&root),
            "junction parent must remain inside the canonical temp root"
        );
        let canonical_target_dir = fs::canonicalize(&target_dir).unwrap();
        assert!(
            canonical_target_dir.starts_with(&root),
            "junction target must remain inside the canonical temp root"
        );
        create_directory_junction(&target_dir, &junction_dir).unwrap();

        use std::os::windows::fs::MetadataExt;
        let metadata = fs::symlink_metadata(&junction_dir).unwrap();
        assert_ne!(
            metadata.file_attributes() & 0x400,
            0,
            "PowerShell created a reparse point at the run directory"
        );
        let resolved_junction = fs::canonicalize(&junction_dir).unwrap_or_else(|error| {
            panic!("created junction does not resolve to its retained target: {error}")
        });
        assert_eq!(
            resolved_junction, canonical_target_dir,
            "junction resolves to the exact retained target directory"
        );
    }

    let error = allow(&work).unwrap_err();
    #[cfg(unix)]
    assert_eq!(error.code, "PRIVATE_FILE_INVALID", "allow error: {error:?}");
    #[cfg(windows)]
    assert_eq!(error.code, "PRIVATE_PATH_INVALID", "allow error: {error:?}");
    assert_eq!(fs::read(&target).unwrap(), retained);

    #[cfg(unix)]
    assert!(
        fs::symlink_metadata(dir.join("start-decision.json"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        let metadata = fs::symlink_metadata(&dir).unwrap();
        assert_ne!(
            metadata.file_attributes() & 0x400,
            0,
            "run directory remains a reparse point after rejected Allow"
        );
    }
}

#[cfg(unix)]
fn create_reparse_link(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_directory_junction(target: &Path, junction: &Path) -> std::io::Result<()> {
    use std::os::windows::process::CommandExt;

    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    const SCRIPT: &str = r#"
$ErrorActionPreference = 'Stop'
try {
    New-Item -ItemType Junction -Path $env:ELIOT_SWARM_TEST_JUNCTION_LINK -Target $env:ELIOT_SWARM_TEST_JUNCTION_TARGET | Out-Null
    exit 0
} catch {
    [Console]::Error.WriteLine($_.Exception.Message)
    exit 1
}
"#;

    let output = std::process::Command::new("powershell.exe")
        .args([
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            SCRIPT,
        ])
        .env("ELIOT_SWARM_TEST_JUNCTION_LINK", junction)
        .env("ELIOT_SWARM_TEST_JUNCTION_TARGET", target)
        .creation_flags(CREATE_NO_WINDOW)
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "PowerShell New-Item -ItemType Junction failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

#[cfg(not(any(unix, windows)))]
fn create_reparse_link(_target: &Path, _link: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "reparse links are unsupported on this target",
    ))
}
