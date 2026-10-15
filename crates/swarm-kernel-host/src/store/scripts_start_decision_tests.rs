use std::{cell::Cell, fs, path::PathBuf, rc::Rc};

#[cfg(any(windows, target_os = "linux"))]
use std::{
    thread,
    time::{Duration, Instant},
};

#[cfg(any(windows, target_os = "linux"))]
use base64::Engine as _;
use serde_json::Value;
#[cfg(any(windows, target_os = "linux"))]
use serde_json::json;

struct FixtureDirectory(PathBuf);

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(any(windows, target_os = "linux"))]
struct SourceWorkerCleanup {
    custody: crate::scripts::runner::ScriptChildCustody,
    work: crate::scripts::runner::Work,
    data_dir: PathBuf,
    run_dir: PathBuf,
    armed: bool,
}

#[cfg(any(windows, target_os = "linux"))]
impl SourceWorkerCleanup {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

#[cfg(any(windows, target_os = "linux"))]
impl Drop for SourceWorkerCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let _ = crate::scripts::runner::allow(&self.work);
        let completion_path = self.run_dir.join("completion.json");
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut released = false;
        loop {
            let exited = self
                .custody
                .exit_observed(&self.work.run_id, &self.work.operation_id)
                .ok()
                .flatten()
                == Some(true);
            if exited
                && completion_path.is_file()
                && self
                    .custody
                    .release_after_terminal(&self.work.run_id, &self.work.operation_id)
                    .unwrap_or(false)
            {
                released = true;
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
            thread::sleep(Duration::from_millis(25));
        }
        if released {
            let _ = fs::remove_dir_all(&self.data_dir);
        } else {
            eprintln!(
                "source-worker fixture preserves unconfirmed custody at {}",
                self.data_dir.display()
            );
        }
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

#[cfg(any(windows, target_os = "linux"))]
#[tokio::test]
async fn store_source_worker_waits_for_durable_allow_and_runs_once_after_recovery() {
    use crate::{
        config::Config,
        platform::{DataRoot, bootstrap_credential},
        store::StoreOwner,
    };

    let root = std::env::temp_dir().join(format!(
        "script-source-worker-barrier-{}",
        uuid::Uuid::new_v4()
    ));
    fs::create_dir_all(&root).expect("create isolated Store data root");
    // A failed native fixture must preserve its files until its exact family
    // has departed. SourceWorkerCleanup owns cleanup after the spawn seam.
    let data_root = DataRoot::acquire(&root).expect("acquire the fixture Store root");
    let data_dir = data_root.path.clone();
    let credential = bootstrap_credential(&data_dir).expect("bootstrap fixture Operator");
    let executor = crate::store::o6_taskless_path_fixture::store_only_script_executor_pin();
    let mut config = Config::default();
    config.storage.data_dir = data_dir.clone();
    config.scripts.executor = Some(executor.clone());
    let owner = StoreOwner::start(data_root, std::sync::Arc::new(config), credential)
        .await
        .expect("start the real Store file-I/O and custody owners");

    let mut work = crate::scripts::runner::durable_authority_tests::fixture_work(
        &data_dir,
        Some(executor.clone()),
    );
    let interpreter_path = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
        .map(|directory| directory.join(if cfg!(windows) { "pwsh.exe" } else { "pwsh" }))
        .find(|path| path.is_file())
        .expect("the existing pwsh executable is required for the real worker fixture");
    let interpreter = crate::scripts::manifest::capture_interpreter(
        &interpreter_path,
        crate::scripts::manifest::InterpreterKind::Powershell,
    )
    .expect("capture the exact test interpreter");
    work.interpreter = interpreter.clone();
    work.bundle.interpreter = interpreter.clone();
    // This applies only to the worker-launched PowerShell process, allowing
    // the unsigned fixture source under hosts whose user policy is RemoteSigned.
    work.environment.insert(
        "PSExecutionPolicyPreference".to_owned(),
        "Bypass".to_owned(),
    );
    work.environment_sha256 = crate::scripts::runner::environment_sha256(&work.environment)
        .expect("hash the exact worker environment");

    let marker_path = data_dir.join("source-worker-started.txt");
    let marker_for_script = marker_path.to_string_lossy().replace('\'', "''");
    let result_json = serde_json::to_string(&json!({
        "protocol_version":1,
        "operation_id":work.operation_id.clone(),
        "run_id":work.run_id.clone(),
        "result":Value::Null,
        "effects":[],
    }))
    .expect("serialize the exact ScriptResult protocol receipt");
    let script_bytes = format!(
        "[System.IO.File]::AppendAllText('{marker_for_script}', \"started`n\")\nStart-Sleep -Seconds 2\n[Console]::Out.WriteLine('{result_json}')\n"
    )
    .into_bytes();
    work.bundle.files = vec![crate::scripts::manifest::BundleFile {
        path: work.bundle.entrypoint.clone(),
        sha256: crate::model::digest(&script_bytes),
        byte_length: script_bytes.len() as u64,
        content_base64: base64::engine::general_purpose::STANDARD.encode(&script_bytes),
    }];
    let bundle_bytes = crate::model::canonical(&serde_json::to_value(&work.bundle).unwrap())
        .expect("canonicalize the exact worker bundle")
        .into_bytes();
    work.bundle_record.relative_path = format!("artifacts/{}.bin", work.bundle_record.artifact_id);
    work.bundle_record.byte_length = bundle_bytes.len() as u64;
    work.bundle_record.content_digest = crate::model::digest(&bundle_bytes);
    work.bundle_record.metadata = json!({
        "script_id":work.bundle.script_id.clone(),
        "revision":work.invocation.script_revision,
        "bundle_sha256":work.bundle_record.content_digest.clone(),
        "interpreter_sha256":work.interpreter.sha256.clone(),
    });
    let bundle_path = data_dir.join(&work.bundle_record.relative_path);
    fs::create_dir_all(bundle_path.parent().unwrap())
        .expect("create the retained bundle directory");
    swarm_process::write_private_new(&bundle_path, &bundle_bytes)
        .expect("publish the exact content-pinned worker bundle");

    let verified_worker = swarm_script_worker::verify_pinned_executor_file(&executor)
        .expect("resolve the source-built standalone worker, never the host test binary");
    assert_eq!(
        verified_worker,
        fs::canonicalize(&executor.executable).expect("canonicalize the pinned worker path")
    );
    assert_eq!(
        verified_worker.file_name().and_then(|name| name.to_str()),
        Some(if cfg!(windows) {
            "swarm-script-worker.exe"
        } else {
            "swarm-script-worker"
        }),
        "the Store pin names the source worker binary, not a libtest image"
    );
    assert_eq!(
        crate::model::digest(&fs::read(&verified_worker).expect("read source worker bytes")),
        executor.sha256,
        "the launched worker bytes match the reused source-worker pin"
    );
    let (receipt_path, work_digest) = crate::scripts::runner::write_work(&work)
        .expect("write the immutable Work and actual ScriptWorker receipt");
    let receipt: Value = serde_json::from_slice(&fs::read(&receipt_path).unwrap()).unwrap();
    assert_eq!(receipt["run_id"], work.run_id);
    assert_eq!(receipt["work_digest"], work_digest);
    assert_eq!(receipt["data_dir"], json!(data_dir));
    let run_dir = crate::scripts::runner::directory(&data_dir, &work.run_id).unwrap();
    let decision_path = run_dir.join("start-decision.json");
    let custody = owner.store.script_child_custody.clone();
    let mut cleanup = SourceWorkerCleanup {
        custody: custody.clone(),
        work: work.clone(),
        data_dir: data_dir.clone(),
        run_dir: run_dir.clone(),
        armed: true,
    };

    let spawn_work = work.clone();
    let spawn_digest = work_digest.clone();
    let spawn_custody = custody.clone();
    let launch = owner
        .store
        .file_io(move |_| spawn_custody.prepare_and_spawn(&spawn_work, &spawn_digest))
        .await
        .expect("Store file-I/O owner spawns its pinned ScriptWorker");
    assert_eq!(launch["run_id"], work.run_id);
    assert_eq!(launch["operation_id"], work.operation_id);
    assert_eq!(launch["token"], work.token);
    assert_eq!(launch["process"]["scope"], "launcher_spawned_process");
    assert_eq!(launch["process"]["purpose"], "check");

    let ready_deadline = Instant::now() + Duration::from_secs(10);
    let ready = loop {
        if let Some(ready) =
            crate::scripts::runner::ready(&work).expect("read the live worker-ready receipt")
        {
            break ready;
        }
        assert!(
            Instant::now() < ready_deadline,
            "source worker becomes ready"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(ready["run_id"], work.run_id);
    assert_eq!(ready["operation_id"], work.operation_id);
    assert_eq!(ready["token"], work.token);
    assert_eq!(ready["control_version"], 1);
    assert_eq!(ready["process"]["purpose"], "script");
    assert_eq!(launch["process"]["pid"], ready["process"]["pid"]);
    assert_eq!(
        ready["process"]["scope"],
        if cfg!(windows) {
            "windows_job"
        } else {
            "linux_process_group"
        }
    );
    let retained_worker =
        fs::read(run_dir.join("worker.json")).expect("read actual Worker ABI receipt");

    let barrier_path = decision_path.clone();
    let allow_work = work.clone();
    let failed_publication = owner
        .store
        .file_io(move |_| {
            let _barrier = swarm_process::test_support::scoped_durability_barrier(move |path| {
                if path == barrier_path {
                    Err(swarm_contracts::error::Error::new(
                        "PRIVATE_FILE_DURABILITY_UNKNOWN",
                        "injected Store start-decision persistence failure",
                    ))
                } else {
                    swarm_process::sync_parent_directory(path)
                }
            });
            crate::scripts::runner::allow(&allow_work)
        })
        .await
        .expect_err("unconfirmed Store Allow remains unknown");
    assert_eq!(failed_publication.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
    let visible_bytes = fs::read(&decision_path).expect("published decision bytes remain visible");
    assert!(!run_dir.join("execution-plan.json").exists());
    assert!(!run_dir.join("execution-plan-ready.json").exists());
    assert!(!run_dir.join("started.json").exists());
    assert!(
        !marker_path.exists(),
        "the real interpreter has not started"
    );

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!run_dir.join("execution-plan.json").exists());
    assert!(!run_dir.join("started.json").exists());
    assert!(!marker_path.exists());
    assert_eq!(fs::read(&decision_path).unwrap(), visible_bytes);
    assert_eq!(
        fs::read(run_dir.join("worker.json")).unwrap(),
        retained_worker
    );
    assert_eq!(
        custody
            .exit_observed(&work.run_id, &work.operation_id)
            .unwrap(),
        Some(false),
        "the exact Store-owned Child remains live behind the missing plan"
    );
    assert!(custody.poll_active_children(false));
    assert!(custody.poll_active_children(true));
    let replay_work = work.clone();
    let replay_digest = work_digest.clone();
    let replay_custody = custody.clone();
    let replay_launch = owner
        .store
        .file_io(move |_| replay_custody.prepare_and_spawn(&replay_work, &replay_digest))
        .await
        .expect("Store retries the same retained child identity");
    assert_eq!(replay_launch, launch, "retry never creates a second worker");

    for _ in 0..2 {
        let retry_work = work.clone();
        owner
            .store
            .file_io(move |_| crate::scripts::runner::allow(&retry_work))
            .await
            .expect("durable current Allow materializes the existing plan");
    }
    let completion_path = run_dir.join("completion.json");
    let start_deadline = Instant::now() + Duration::from_secs(10);
    while !marker_path.is_file() {
        if Instant::now() >= start_deadline {
            let completion = fs::read_to_string(&completion_path)
                .unwrap_or_else(|error| format!("<unavailable: {error}>"));
            let stderr = serde_json::from_str::<Value>(&completion)
                .ok()
                .and_then(|value| value["stderr"]["relative_path"].as_str().map(PathBuf::from))
                .and_then(|relative| {
                    (!relative.is_absolute()
                        && !relative
                            .components()
                            .any(|component| matches!(component, std::path::Component::ParentDir)))
                    .then(|| fs::read_to_string(data_dir.join(relative)).ok())
                    .flatten()
                })
                .unwrap_or_else(|| "<stderr artifact unavailable>".to_owned());
            panic!(
                "recovered worker did not start the interpreter: plan={}, plan_ready={}, started={}, completion={completion}, stderr={stderr}, custody_exit={:?}",
                run_dir.join("execution-plan.json").is_file(),
                run_dir.join("execution-plan-ready.json").is_file(),
                run_dir.join("started.json").is_file(),
                custody.exit_observed(&work.run_id, &work.operation_id),
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(run_dir.join("execution-plan.json").is_file());
    assert!(run_dir.join("started.json").is_file());
    assert!(
        custody.poll_active_children(false),
        "worker family remains custodied while its script runs"
    );

    let finish_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let exited = custody
            .exit_observed(&work.run_id, &work.operation_id)
            .expect("poll the exact retained Store Child")
            == Some(true);
        if exited && completion_path.is_file() {
            break;
        }
        assert!(
            Instant::now() < finish_deadline,
            "the recovered source worker completes"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let completion: Value = serde_json::from_slice(&fs::read(&completion_path).unwrap()).unwrap();
    assert_eq!(completion["run_id"], work.run_id);
    assert_eq!(completion["operation_id"], work.operation_id);
    assert_eq!(completion["token"], work.token);
    let stderr_path = completion["stderr"]["relative_path"]
        .as_str()
        .map(|relative| data_dir.join(relative))
        .expect("completion retains the interpreter stderr artifact");
    let stderr = fs::read_to_string(stderr_path).unwrap_or_default();
    assert_eq!(
        completion["state"], "completed",
        "real worker completion: {completion}; interpreter stderr: {stderr}"
    );
    assert_eq!(completion["exit_code"], 0);
    assert!(completion["error_code"].is_null());
    assert_eq!(
        fs::read_to_string(&marker_path)
            .expect("read interpreter invocation marker")
            .lines()
            .collect::<Vec<_>>(),
        ["started"],
        "two Store retries start the pinned interpreter at most once"
    );
    let mut released = false;
    let release_deadline = Instant::now() + Duration::from_secs(10);
    while !released {
        released = custody
            .release_after_terminal(&work.run_id, &work.operation_id)
            .expect("release only after the source worker has completed");
        if !released {
            assert!(
                Instant::now() < release_deadline,
                "the exact script family departs"
            );
            let _ = custody.poll_active_children(true);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }
    cleanup.disarm();
    owner
        .close()
        .await
        .expect("close Store after worker family departure");
    fs::remove_dir_all(data_dir).expect("remove fixture only after confirmed family departure");
}
