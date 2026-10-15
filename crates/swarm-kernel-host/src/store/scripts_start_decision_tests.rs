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

#[cfg(windows)]
struct ScopedPsExecutionPolicy(Option<std::ffi::OsString>);

#[cfg(windows)]
impl ScopedPsExecutionPolicy {
    fn bypass_for_work_capture() -> Self {
        let previous = std::env::var_os("PSExecutionPolicyPreference");
        // SAFETY: this guard is used around one awaited Store admission in the
        // single selected fixture test; its file-I/O capture completes before
        // Drop restores the value, with no concurrent environment mutation.
        unsafe { std::env::set_var("PSExecutionPolicyPreference", "Bypass") };
        Self(previous)
    }
}

#[cfg(windows)]
impl Drop for ScopedPsExecutionPolicy {
    fn drop(&mut self) {
        // SAFETY: the admission capture has completed and the same focused
        // single-test process still owns the temporary environment override.
        unsafe {
            match self.0.take() {
                Some(previous) => std::env::set_var("PSExecutionPolicyPreference", previous),
                None => std::env::remove_var("PSExecutionPolicyPreference"),
            }
        }
    }
}

#[cfg(any(windows, target_os = "linux"))]
struct SourceWorkerCleanup {
    custody: crate::scripts::runner::ScriptChildCustody,
    work: crate::scripts::runner::Work,
    data_dir: PathBuf,
    run_dir: PathBuf,
    terminal_settlement_confirmed: bool,
    armed: bool,
}

#[cfg(any(windows, target_os = "linux"))]
impl SourceWorkerCleanup {
    fn disarm(&mut self) {
        self.armed = false;
    }

    fn confirm_terminal_settlement(
        &mut self,
        run_id: &str,
        operation_id: &str,
        operation_state: &str,
        run_state: &str,
        diagnostic: Option<&str>,
    ) -> bool {
        if run_id != self.work.run_id
            || operation_id != self.work.operation_id
            || operation_state != "settled"
            || run_state != "completed"
            || diagnostic.is_some()
        {
            return false;
        }
        self.terminal_settlement_confirmed = true;
        true
    }

    fn release_after_terminal(&self) -> crate::error::Result<bool> {
        if !self.terminal_settlement_confirmed {
            return Ok(false);
        }
        self.custody
            .release_after_terminal(&self.work.run_id, &self.work.operation_id)
    }
}

#[cfg(any(windows, target_os = "linux"))]
impl Drop for SourceWorkerCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if !self.terminal_settlement_confirmed {
            eprintln!(
                "source-worker fixture preserves unconfirmed custody at {}",
                self.data_dir.display()
            );
            // Keep the shared owner map and its Child alive if this fixture exits
            // before observing the exact Store terminal projection.
            std::mem::forget(self.custody.clone());
            return;
        }
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
            if exited && completion_path.is_file() && self.release_after_terminal().unwrap_or(false)
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
            std::mem::forget(self.custody.clone());
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
        terminal_settlement_confirmed: false,
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

    let prepublication_path = decision_path.clone();
    let allow_with_prepublication_fault = work.clone();
    let prepublication_error = owner
        .store
        .file_io(move |_| {
            let _fault = swarm_process::test_support::scoped_prepublication_fault(move |path| {
                if path == prepublication_path {
                    Err(swarm_contracts::error::Error::new(
                        "FIXTURE_PREPUBLICATION_FAILURE",
                        "injected failure after temp sync and before decision publication",
                    ))
                } else {
                    Ok(())
                }
            });
            crate::scripts::runner::allow(&allow_with_prepublication_fault)
        })
        .await
        .expect_err("pre-publication fault leaves the decision pending");
    assert_eq!(prepublication_error.code, "FIXTURE_PREPUBLICATION_FAILURE");
    assert!(
        !decision_path.exists(),
        "pre-publication failure leaves no decision"
    );
    assert_eq!(
        crate::scripts::runner::start_decision(&work).unwrap(),
        swarm_script_worker::StartDecisionRead::Pending
    );
    assert!(!run_dir.join("execution-plan.json").exists());
    assert!(!run_dir.join("execution-plan-ready.json").exists());
    assert!(!run_dir.join("started.json").exists());
    assert!(!marker_path.exists());
    assert_eq!(
        fs::read(run_dir.join("worker.json")).unwrap(),
        retained_worker
    );
    assert_eq!(
        custody
            .exit_observed(&work.run_id, &work.operation_id)
            .unwrap(),
        Some(false),
        "the real source worker stays held across a pre-publication path failure"
    );

    let barrier_path = decision_path.clone();
    let allow_work = work.clone();
    let store = owner.store.clone();
    let (published_tx, published_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let failed_publication = tokio::spawn(async move {
        store
            .file_io(move |_| {
                let _barrier =
                    swarm_process::test_support::scoped_durability_barrier(move |path| {
                        if path == barrier_path {
                            published_tx
                                .send(())
                                .expect("signal the competing Deny after Allow is visible");
                            resume_rx
                                .recv_timeout(Duration::from_secs(10))
                                .expect("resume the uncertain Allow after the competing Deny");
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
    });
    tokio::task::spawn_blocking(move || published_rx.recv_timeout(Duration::from_secs(10)))
        .await
        .expect("wait for the published Store Allow without blocking the test runtime")
        .expect("the Store Allow publishes bytes before its durability barrier");
    let competing_deny = crate::scripts::runner::deny(&work, "DENIED")
        .expect_err("the concurrent Deny cannot replace retained Allow");
    assert_eq!(competing_deny.code, "CONFLICT");
    resume_tx
        .send(())
        .expect("release the publisher after the opposite intent conflicts");
    let failed_publication = failed_publication
        .await
        .expect("the Store publication task completes")
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

    let allow_work = work.clone();
    let deny_work = work.clone();
    let allow_retry = owner
        .store
        .file_io(move |_| crate::scripts::runner::allow(&allow_work));
    let deny_retry = owner
        .store
        .file_io(move |_| crate::scripts::runner::deny(&deny_work, "DENIED"));
    let (allow_retry, deny_retry) = tokio::join!(allow_retry, deny_retry);
    allow_retry.expect("durable Allow winner materializes the existing plan");
    assert_eq!(
        deny_retry
            .expect_err("the concurrent Store Deny conflicts with retained Allow")
            .code,
        "CONFLICT"
    );
    let replay_work = work.clone();
    owner
        .store
        .file_io(move |_| crate::scripts::runner::allow(&replay_work))
        .await
        .expect("same-intent Store retry retains Allow without a second execution");
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

    // The same pinned source worker must honor a retained Deny as well. The
    // competing Allow publisher is already live, then released after Deny has
    // won so this second race phase is deterministic without hand-written
    // decision bytes.
    let mut denied_work = work.clone();
    denied_work.run_id = uuid::Uuid::new_v4().to_string();
    denied_work.operation_id = uuid::Uuid::new_v4().to_string();
    denied_work.token = uuid::Uuid::new_v4().to_string();
    denied_work.invocation.run_id = denied_work.run_id.clone();
    denied_work.invocation.operation_id = denied_work.operation_id.clone();
    let (denied_receipt, denied_work_digest) = crate::scripts::runner::write_work(&denied_work)
        .expect("write the second immutable Work receipt for the Deny winner");
    assert!(denied_receipt.is_file());
    let denied_run_dir = crate::scripts::runner::directory(&data_dir, &denied_work.run_id).unwrap();
    let mut denied_cleanup = SourceWorkerCleanup {
        custody: custody.clone(),
        work: denied_work.clone(),
        data_dir: data_dir.clone(),
        run_dir: denied_run_dir.clone(),
        terminal_settlement_confirmed: false,
        armed: true,
    };
    let spawn_denied_work = denied_work.clone();
    let spawn_denied_digest = denied_work_digest.clone();
    let spawn_denied_custody = custody.clone();
    let denied_launch = owner
        .store
        .file_io(move |_| {
            spawn_denied_custody.prepare_and_spawn(&spawn_denied_work, &spawn_denied_digest)
        })
        .await
        .expect("Store file-I/O owner spawns the same pinned worker for the Deny phase");
    let denied_ready_deadline = Instant::now() + Duration::from_secs(10);
    let denied_ready = loop {
        if let Some(ready) = crate::scripts::runner::ready(&denied_work).unwrap() {
            break ready;
        }
        assert!(
            Instant::now() < denied_ready_deadline,
            "source worker becomes ready for the Deny phase"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(denied_ready["run_id"], denied_work.run_id);
    assert_eq!(denied_ready["operation_id"], denied_work.operation_id);
    assert_eq!(denied_ready["token"], denied_work.token);
    assert_eq!(denied_ready["control_version"], 1);
    assert_eq!(
        denied_launch["process"]["pid"],
        denied_ready["process"]["pid"]
    );

    let (allow_waiting_tx, allow_waiting_rx) = std::sync::mpsc::channel();
    let (release_allow_tx, release_allow_rx) = std::sync::mpsc::channel();
    let late_allow_work = denied_work.clone();
    let late_allow = thread::spawn(move || {
        allow_waiting_tx
            .send(())
            .expect("report the live competing Allow publisher");
        release_allow_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("release the Allow publisher after Deny wins");
        crate::scripts::runner::allow(&late_allow_work)
    });
    allow_waiting_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the opposite intent is ready to publish");
    let deny_work_for_store = denied_work.clone();
    owner
        .store
        .file_io(move |_| crate::scripts::runner::deny(&deny_work_for_store, "DENIED"))
        .await
        .expect("the real Store file-I/O publisher retains Deny first");
    release_allow_tx
        .send(())
        .expect("release the losing Allow publisher");
    assert_eq!(
        late_allow
            .join()
            .expect("the competing Allow publisher exits")
            .expect_err("Allow cannot replace the retained Deny")
            .code,
        "CONFLICT"
    );

    let denied_completion_path = denied_run_dir.join("completion.json");
    let denied_finish_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if custody
            .exit_observed(&denied_work.run_id, &denied_work.operation_id)
            .unwrap()
            == Some(true)
            && denied_completion_path.is_file()
        {
            break;
        }
        assert!(
            Instant::now() < denied_finish_deadline,
            "the real source worker finishes without passing the Deny gate"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let denied_completion: Value =
        serde_json::from_slice(&fs::read(&denied_completion_path).unwrap()).unwrap();
    assert_eq!(denied_completion["run_id"], denied_work.run_id);
    assert_eq!(denied_completion["operation_id"], denied_work.operation_id);
    assert_eq!(denied_completion["token"], denied_work.token);
    assert_eq!(denied_completion["state"], "failed");
    assert_eq!(denied_completion["error_code"], "DENIED");
    assert!(!denied_run_dir.join("execution-plan.json").exists());
    assert!(!denied_run_dir.join("execution-plan-ready.json").exists());
    assert!(!denied_run_dir.join("started.json").exists());
    assert_eq!(
        fs::read_to_string(&marker_path)
            .expect("read the unchanged interpreter marker")
            .lines()
            .collect::<Vec<_>>(),
        ["started"],
        "the Deny-winning real worker never starts the interpreter"
    );
    assert!(
        custody
            .release_after_terminal(&denied_work.run_id, &denied_work.operation_id)
            .expect("release the denied worker after its direct child exits")
    );
    denied_cleanup.disarm();
    assert_eq!(
        custody
            .exit_observed(&denied_work.run_id, &denied_work.operation_id)
            .unwrap(),
        None
    );

    owner
        .close()
        .await
        .expect("close Store after worker family departure");
    fs::remove_dir_all(data_dir).expect("remove fixture only after confirmed family departure");
}

#[cfg(any(windows, target_os = "linux"))]
#[test]
fn reconcile_scripts_retains_operation_and_source_worker_across_allow_durability_unknown() {
    use crate::{
        artifacts::ArtifactFiles,
        config::Config,
        model::{self, Credential},
        platform::{DataRoot, bootstrap_credential},
        store::{Store, StoreOwner},
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    async fn stored_state(
        store: &Store,
        run_id: &str,
        operation_id: &str,
    ) -> (String, String, Option<String>, Option<String>) {
        let run_id = run_id.to_owned();
        let operation_id = operation_id.to_owned();
        store
            .run(move |db| {
                Ok(db.query_row(
                    "SELECT o.state,r.state,json_extract(o.result_json,'$.diagnostic_code'),r.process_identity_json \
                       FROM operations o JOIN script_runs r ON r.operation_id=o.operation_id \
                      WHERE o.operation_id=?1 AND r.run_id=?2 AND o.method='script.run'",
                    rusqlite::params![operation_id, run_id],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                        ))
                    },
                )?)
            })
            .await
            .expect("read the exact retained Operation and ScriptRun rows")
    }

    async fn clear_single_subject_cursor(store: &Store) {
        store
            .run(|db| {
                db.execute(
                    "DELETE FROM meta WHERE key='coordination:script:v1:pending_run_cursor'",
                    [],
                )?;
                Ok(())
            })
            .await
            .expect("reselect the single pending subject for this focused pass");
    }

    fn copy_completion_fixture(
        source_data_dir: &std::path::Path,
        source_run_dir: &std::path::Path,
        work: &crate::scripts::runner::Work,
        completion: &Value,
        label: &str,
    ) -> (PathBuf, crate::scripts::runner::Work, ArtifactFiles) {
        let copied_data_dir = std::env::temp_dir().join(format!(
            "script-completion-wire-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&copied_data_dir).expect("create completion copy root");
        let copied_data_dir =
            fs::canonicalize(&copied_data_dir).expect("canonicalize completion copy root");
        let copied_run_dir = copied_data_dir.join(
            source_run_dir
                .strip_prefix(source_data_dir)
                .expect("ScriptRun belongs to the fixture data root"),
        );
        fs::create_dir_all(&copied_run_dir).expect("create isolated completion wire fixture");
        for file_name in ["worker.json", "completion.json"] {
            fs::copy(
                source_run_dir.join(file_name),
                copied_run_dir.join(file_name),
            )
            .expect("copy actual source-worker protocol receipt");
        }
        for artifact_name in ["result", "stdout", "stderr"] {
            let relative = completion[artifact_name]["relative_path"]
                .as_str()
                .expect("completion retains an artifact path");
            assert!(!std::path::Path::new(relative).is_absolute());
            let source = source_data_dir.join(relative);
            let destination = copied_data_dir.join(relative);
            fs::create_dir_all(destination.parent().expect("artifact has a parent"))
                .expect("create copied artifact directory");
            fs::copy(source, destination).expect("copy actual content-addressed artifact");
        }
        let mut copied_work = work.clone();
        copied_work.data_dir = copied_data_dir.clone();
        let files = ArtifactFiles::new(&copied_data_dir)
            .expect("open isolated ArtifactFiles over copied actual receipts");
        (copied_data_dir, copied_work, files)
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .expect("create an isolated one-thread Store file-I/O runtime");
    runtime.block_on(async {
        let root = std::env::temp_dir().join(format!(
            "script-reconcile-source-worker-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&root).expect("create isolated Store data root");
        let data_root = DataRoot::acquire(&root).expect("acquire the fixture Store root");
        let data_dir = data_root.path.clone();
        let credential = bootstrap_credential(&data_dir).expect("bootstrap fixture Operator");
        let executor = crate::store::o6_taskless_path_fixture::store_only_script_executor_pin();
        let verified_worker = swarm_script_worker::verify_pinned_executor_file(&executor)
            .expect("resolve the source-built standalone ScriptWorker");
        assert_eq!(
            verified_worker.file_name().and_then(|name| name.to_str()),
            Some(if cfg!(windows) {
                "swarm-script-worker.exe"
            } else {
                "swarm-script-worker"
            })
        );
        assert_eq!(
            crate::model::digest(&fs::read(&verified_worker).expect("read pinned worker bytes")),
            executor.sha256,
            "the Store launches the exact staged source worker, not the test image"
        );

        let mut config = Config::default();
        config.storage.data_dir = data_dir.clone();
        config.scripts.executor = Some(executor);
        let owner = StoreOwner::start(data_root, Arc::new(config), credential.clone())
            .await
            .expect("start real Store, database and file-I/O owner");
        let operator = owner
            .store
            .authenticate(credential)
            .await
            .expect("authenticate fixture Operator");

        let manager_id = format!("pr67reconcile{}", uuid::Uuid::new_v4().simple());
        let manager_token = format!("pr67-reconcile-{}", uuid::Uuid::new_v4());
        owner
            .store
            .call(
                operator,
                "client.register".into(),
                json!({
                    "client_request_id":format!("register-{manager_id}"),
                    "client_id":manager_id,
                    "role":"manager",
                    "token_hash":model::digest(manager_token.as_bytes()),
                }),
            )
            .await
            .expect("register the script owner through Store");
        let manager = owner
            .store
            .authenticate(Credential {
                client_id: manager_id.clone(),
                token: manager_token,
            })
            .await
            .expect("authenticate script owner");

        let script_id = format!("pr67_reconcile_{}", uuid::Uuid::new_v4().simple());
        let marker_path = data_dir.join("reconcile-interpreter-started.txt");
        let marker_literal = marker_path.to_string_lossy().replace('\'', "''");
        // Exercise the valid null ScriptResult through terminal receipt readback.
        let script_bytes = r#"
$invocation = [Console]::In.ReadToEnd() | ConvertFrom-Json
[System.IO.File]::AppendAllText('__MARKER__', "started`n")
Start-Sleep -Seconds 10
$result = [ordered]@{
    protocol_version = 1
    operation_id = $invocation.operation_id
    run_id = $invocation.run_id
    result = $null
}
[Console]::Out.WriteLine(($result | ConvertTo-Json -Compress -Depth 8))
"#
        .replace("__MARKER__", &marker_literal)
        .into_bytes();
        let interpreter_path = std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .map(|directory| directory.join(if cfg!(windows) { "pwsh.exe" } else { "pwsh" }))
            .find(|path| path.is_file())
            .expect("the existing pwsh executable is required for the source worker fixture");
        let inherited_environment = if cfg!(windows) {
            vec!["PSExecutionPolicyPreference"]
        } else {
            Vec::new()
        };
        let bundle = json!({
            "script_id":script_id,
            "interpreter_kind":"powershell",
            "interpreter_path":interpreter_path,
            "entrypoint":"main.ps1",
            "argv":[],
            "trust":"trusted_local",
            "inherit_environment":inherited_environment,
            "controller_effects":[],
            "input_schema":{"type":"object","properties":{},"required":[],"additional_properties":true},
            "result_schema":{"type":"null"},
            "files":[{
                "path":"main.ps1",
                "content_base64":base64::engine::general_purpose::STANDARD.encode(&script_bytes),
            }],
        });
        owner
            .store
            .call(
                manager.clone(),
                "script.register".into(),
                json!({
                    "client_request_id":format!("register-{script_id}"),
                    "bundle":bundle,
                }),
            )
            .await
            .expect("register the content-pinned script bundle");
        owner
            .store
            .call(
                manager.clone(),
                "script.activate".into(),
                json!({
                    "client_request_id":format!("activate-{script_id}"),
                    "script_id":script_id,
                    "revision":1,
                }),
            )
            .await
            .expect("activate immutable script revision");

        let task = owner
            .store
            .call(
                manager.clone(),
                "task.create".into(),
                json!({
                    "client_request_id":format!("task-create-{}", uuid::Uuid::new_v4()),
                    "project_id":format!("pr67-reconcile-project-{}", uuid::Uuid::new_v4()),
                    "spec":{
                        "objective":"Exercise ScriptRun reconciliation across uncertain Allow durability",
                        "phase":"implementation",
                        "requirements":[{"id":"R1","statement":"Retain the exact Operation and worker custody"}],
                        "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
                    },
                }),
            )
            .await
            .expect("create the exact Task through Store");
        let task_id = model::text(&task, "task_id").unwrap().to_owned();
        let claim = owner
            .store
            .call(
                manager.clone(),
                "task.claim".into(),
                json!({
                    "client_request_id":format!("task-claim-{}", uuid::Uuid::new_v4()),
                    "task_id":task_id,
                    "expected_revision":1,
                    "owner_id":manager.client_id,
                }),
            )
            .await
            .expect("claim the exact Task and create its Attempt");
        let attempt_id = model::text(&claim, "attempt_id").unwrap().to_owned();
        #[cfg(windows)]
        let execution_policy = ScopedPsExecutionPolicy::bypass_for_work_capture();
        let admission_result = owner
            .store
            .call(
                manager,
                "script.run".into(),
                json!({
                    "client_request_id":format!("script-run-{}", uuid::Uuid::new_v4()),
                    "script_id":script_id,
                    "expected_script_revision":1,
                    "attempt_id":attempt_id,
                    "expected_task_revision":1,
                    "input":{},
                }),
            )
            .await;
        #[cfg(windows)]
        drop(execution_policy);
        let admission = admission_result.expect("admit the real ScriptRun Operation through Store");
        let run_id = model::text(&admission, "run_id").unwrap().to_owned();
        let operation_id = model::text(&admission, "operation_id").unwrap().to_owned();
        assert_eq!(admission["state"], "queued");

        let run_dir = crate::scripts::runner::directory(&data_dir, &run_id).unwrap();
        let receipt_path = run_dir.join("receipt.json");
        let (work, _) = crate::scripts::runner::read_work_record(&receipt_path)
            .expect("read the exact Store-admitted Work");
        assert_eq!(work.run_id, run_id);
        assert_eq!(work.operation_id, operation_id);
        let worker_pin = work.executor.as_ref().expect("new ScriptRun has a worker pin");
        assert_eq!(
            fs::canonicalize(&worker_pin.executable).unwrap(),
            verified_worker,
            "the immutable Work invokes the verified standalone worker path"
        );
        assert_eq!(worker_pin.sha256, crate::model::digest(&fs::read(&verified_worker).unwrap()));

        let custody = owner.store.script_child_custody.clone();
        let mut cleanup = SourceWorkerCleanup {
            custody: custody.clone(),
            work: work.clone(),
            data_dir: data_dir.clone(),
            run_dir: run_dir.clone(),
            terminal_settlement_confirmed: false,
            armed: true,
        };
        assert_eq!(
            stored_state(&owner.store, &run_id, &operation_id).await.0,
            "queued"
        );

        owner
            .store
            .reconcile_scripts_once()
            .await
            .expect("background reconciliation begins and spawns the retained Operation");
        assert_eq!(
            stored_state(&owner.store, &run_id, &operation_id).await.0,
            "sending"
        );
        let ready_deadline = Instant::now() + Duration::from_secs(10);
        let ready = loop {
            if let Some(ready) = crate::scripts::runner::ready(&work).expect("read worker ABI") {
                break ready;
            }
            assert!(Instant::now() < ready_deadline, "source worker publishes readiness");
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        assert_eq!(ready["run_id"], run_id);
        assert_eq!(ready["operation_id"], operation_id);
        assert_eq!(ready["token"], work.token);
        assert_eq!(ready["control_version"], 1);
        assert_eq!(ready["process"]["purpose"], "script");
        let retained_worker = fs::read(run_dir.join("worker.json"))
            .expect("read actual source-worker protocol receipt");
        let launch: Value =
            serde_json::from_slice(&fs::read(run_dir.join("launch.json")).unwrap()).unwrap();
        assert_eq!(launch["run_id"], run_id);
        assert_eq!(launch["operation_id"], operation_id);
        assert_eq!(launch["process"]["pid"], ready["process"]["pid"]);

        let fail_barrier = Arc::new(AtomicBool::new(true));
        let barrier_fired = Arc::new(AtomicBool::new(false));
        let fail_for_hook = Arc::clone(&fail_barrier);
        let fired_for_hook = Arc::clone(&barrier_fired);
        let barrier_path = run_dir.join("start-decision.json");
        owner
            .store
            .file_io(move |_| {
                let hook = swarm_process::test_support::scoped_durability_barrier(move |path| {
                    if path == barrier_path && fail_for_hook.load(Ordering::SeqCst) {
                        fired_for_hook.store(true, Ordering::SeqCst);
                        Err(swarm_contracts::error::Error::new(
                            "PRIVATE_FILE_DURABILITY_UNKNOWN",
                            "injected background Store start-decision durability failure",
                        ))
                    } else {
                        swarm_process::sync_parent_directory(path)
                    }
                });
                // Store file_io uses spawn_blocking. Keep the hook on the
                // runtime's single blocking thread so reconcile's later file
                // jobs observe the injected exact-path failure.
                std::mem::forget(hook);
                Ok(())
            })
            .await
            .expect("install exact-path failure on the isolated Store file-I/O thread");

        clear_single_subject_cursor(&owner.store).await;
        owner
            .store
            .reconcile_scripts_once()
            .await
            .expect("reconciliation records the durability uncertainty as unknown");
        assert!(barrier_fired.load(Ordering::SeqCst), "Allow reached the injected barrier");
        let (operation_state, run_state, diagnostic, identity_json) =
            stored_state(&owner.store, &run_id, &operation_id).await;
        assert_eq!(operation_state, "outcome_unknown");
        assert_eq!(run_state, "outcome_unknown");
        assert_eq!(diagnostic.as_deref(), Some("PRIVATE_FILE_DURABILITY_UNKNOWN"));
        assert_eq!(
            serde_json::from_str::<Value>(identity_json.as_deref().unwrap()).unwrap(),
            ready,
            "unknown transition retains the exact acknowledged worker identity"
        );
        let retained_allow_bytes = fs::read(run_dir.join("start-decision.json"))
            .expect("visible Allow bytes remain available for exact recovery");
        assert!(!run_dir.join("execution-plan.json").exists());
        assert!(!run_dir.join("execution-plan-ready.json").exists());
        assert!(!run_dir.join("started.json").exists());
        assert!(!marker_path.exists(), "interpreter has not started before durability recovery");
        assert_eq!(
            custody.exit_observed(&run_id, &operation_id).unwrap(),
            Some(false),
            "the exact process family remains in Store custody while the Operation is unknown"
        );
        assert!(custody.poll_active_children(false));

        fail_barrier.store(false, Ordering::SeqCst);
        clear_single_subject_cursor(&owner.store).await;
        owner
            .store
            .reconcile_scripts_once()
            .await
            .expect("reconciliation retries the acknowledged Allow after the barrier recovers");
        let (operation_state, run_state, _, recovered_identity) =
            stored_state(&owner.store, &run_id, &operation_id).await;
        assert_eq!(operation_state, "native_accepted");
        assert_eq!(run_state, "running");
        assert_eq!(recovered_identity, identity_json);
        assert_eq!(fs::read(run_dir.join("worker.json")).unwrap(), retained_worker);
        assert_eq!(
            fs::read(run_dir.join("start-decision.json")).unwrap(),
            retained_allow_bytes,
            "recovery retains the exact visible decision bytes"
        );
        assert!(run_dir.join("execution-plan.json").is_file());
        let completion_path = run_dir.join("completion.json");
        let start_deadline = Instant::now() + Duration::from_secs(10);
        while !run_dir.join("started.json").is_file() || !marker_path.is_file() {
            if Instant::now() >= start_deadline {
                let completion_text = fs::read_to_string(&completion_path)
                    .unwrap_or_else(|error| format!("<unavailable: {error}>"));
                let completion = serde_json::from_str::<Value>(&completion_text).ok();
                let stderr = completion
                    .as_ref()
                    .and_then(|value| value["stderr"]["relative_path"].as_str())
                    .map(PathBuf::from)
                    .filter(|relative| {
                        !relative.is_absolute()
                            && !relative.components().any(|component| {
                                matches!(component, std::path::Component::ParentDir)
                            })
                    })
                    .and_then(|relative| fs::read_to_string(data_dir.join(relative)).ok())
                    .unwrap_or_else(|| "<stderr artifact unavailable>".to_owned());
                panic!(
                    "recovered source worker did not start: plan_ready={}, started={}, marker={}, completion={completion_text}, stderr={stderr}",
                    run_dir.join("execution-plan-ready.json").is_file(),
                    run_dir.join("started.json").is_file(),
                    marker_path.is_file(),
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        clear_single_subject_cursor(&owner.store).await;
        owner
            .store
            .reconcile_scripts_once()
            .await
            .expect("a repeated background pass does not replace the acknowledged worker");
        assert_eq!(
            stored_state(&owner.store, &run_id, &operation_id).await.3,
            identity_json,
            "the same worker identity remains attached after recovery"
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&fs::read(run_dir.join("launch.json")).unwrap())
                .unwrap(),
            launch,
            "reconciliation does not spawn a second source worker"
        );
        let (operation_state, run_state, _, recovered_identity) =
            stored_state(&owner.store, &run_id, &operation_id).await;
        assert_eq!(operation_state, "native_accepted");
        assert_eq!(run_state, "running");
        assert_eq!(recovered_identity, identity_json);
        assert_eq!(
            custody.exit_observed(&run_id, &operation_id).unwrap(),
            Some(false),
            "the same Store-owned source worker remains live during interpreter execution"
        );

        let completion_deadline = Instant::now() + Duration::from_secs(15);
        while !completion_path.is_file() {
            assert!(Instant::now() < completion_deadline, "source worker completes");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let completion: Value =
            serde_json::from_slice(&fs::read(&completion_path).unwrap()).unwrap();
        assert_eq!(completion["run_id"], run_id);
        assert_eq!(completion["operation_id"], operation_id);
        assert_eq!(completion["token"], work.token);
        assert_eq!(completion["state"], "completed");
        assert_eq!(completion["exit_code"], 0);
        assert_eq!(completion["result_value"], Value::Null);
        assert_eq!(
            fs::read_to_string(&marker_path)
                .expect("read actual interpreter invocation marker")
                .lines()
                .collect::<Vec<_>>(),
            ["started"],
            "the real interpreter starts once across failed and recovered reconciliation"
        );

        let worker_exit_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match custody.exit_observed(&run_id, &operation_id).unwrap() {
                Some(true) => break,
                Some(false) => {
                    assert!(
                        Instant::now() < worker_exit_deadline,
                        "completed source worker departs while Store custody remains"
                    );
                    let _ = custody.poll_active_children(true);
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                None => panic!("Store released the source worker before terminal settlement"),
            }
        }

        let source_artifacts = ArtifactFiles::new(&data_dir).unwrap();
        assert!(
            crate::scripts::runner::completion(&work, &source_artifacts)
                .unwrap()
                .is_some(),
            "the exact completed-null source-worker receipt is valid"
        );

        let (missing_value_dir, missing_value_work, missing_value_files) =
            copy_completion_fixture(&data_dir, &run_dir, &work, &completion, "missing-value");
        let _missing_value_cleanup = FixtureDirectory(missing_value_dir.clone());
        let missing_value_completion = crate::scripts::runner::directory(
            &missing_value_dir,
            &run_id,
        )
        .unwrap()
        .join("completion.json");
        let mut missing_value_receipt: Value =
            serde_json::from_slice(&fs::read(&missing_value_completion).unwrap()).unwrap();
        assert!(
            missing_value_receipt
                .as_object_mut()
                .unwrap()
                .remove("result_value")
                .is_some(),
            "the positive receipt carries an explicit result_value key"
        );
        fs::write(
            &missing_value_completion,
            serde_json::to_vec(&missing_value_receipt).unwrap(),
        )
        .unwrap();
        assert_eq!(
            crate::scripts::runner::completion(&missing_value_work, &missing_value_files)
                .unwrap_err()
                .code,
            "CONFLICT",
            "a missing completed result_value is rejected even for Null schema"
        );

        let (nonnull_schema_dir, nonnull_schema_work, nonnull_schema_files) =
            copy_completion_fixture(&data_dir, &run_dir, &work, &completion, "nonnull-schema");
        let _nonnull_schema_cleanup = FixtureDirectory(nonnull_schema_dir);
        let mut nonnull_schema_work = nonnull_schema_work;
        nonnull_schema_work.bundle.result_schema =
            crate::scripts::manifest::ScriptValueSchema::String { max_bytes: 64 };
        assert_eq!(
            crate::scripts::runner::completion(&nonnull_schema_work, &nonnull_schema_files)
                .unwrap_err()
                .code,
            "CONFLICT",
            "explicit null is invalid under a non-null immutable result schema"
        );

        let (missing_member_dir, missing_member_work, missing_member_files) =
            copy_completion_fixture(&data_dir, &run_dir, &work, &completion, "missing-member");
        let _missing_member_cleanup = FixtureDirectory(missing_member_dir.clone());
        let missing_member_run_dir =
            crate::scripts::runner::directory(&missing_member_dir, &run_id).unwrap();
        let missing_member_completion = missing_member_run_dir.join("completion.json");
        let mut missing_member_receipt: Value =
            serde_json::from_slice(&fs::read(&missing_member_completion).unwrap()).unwrap();
        let result_relative = missing_member_receipt["result"]["relative_path"]
            .as_str()
            .unwrap()
            .to_owned();
        let result_path = missing_member_dir.join(result_relative);
        let mut result_document: Value =
            serde_json::from_slice(&fs::read(&result_path).unwrap()).unwrap();
        assert!(
            result_document
                .as_object_mut()
                .unwrap()
                .remove("result")
                .is_some(),
            "the positive result artifact carries the result member"
        );
        let result_bytes = crate::model::canonical(&result_document)
            .unwrap()
            .into_bytes();
        missing_member_receipt["result"]["byte_length"] = json!(result_bytes.len() as u64);
        missing_member_receipt["result"]["content_digest"] =
            json!(crate::model::digest(&result_bytes));
        fs::write(&result_path, &result_bytes).unwrap();
        fs::write(
            &missing_member_completion,
            serde_json::to_vec(&missing_member_receipt).unwrap(),
        )
        .unwrap();
        assert_eq!(
            crate::scripts::runner::completion(&missing_member_work, &missing_member_files)
                .unwrap_err()
                .code,
            "CONFLICT",
            "a correctly hashed result artifact without its result member is rejected"
        );

        let (operation_state, run_state, diagnostic, retained_identity) =
            stored_state(&owner.store, &run_id, &operation_id).await;
        assert_eq!(operation_state, "native_accepted");
        assert_eq!(run_state, "running");
        assert_eq!(retained_identity, identity_json);
        assert!(
            !cleanup.confirm_terminal_settlement(
                &run_id,
                &operation_id,
                &operation_state,
                &run_state,
                diagnostic.as_deref(),
            ),
            "worker exit and completion bytes do not prove Store terminal settlement"
        );
        assert!(
            !cleanup
                .release_after_terminal()
                .expect("unsettled ScriptRun keeps Store-owned custody"),
            "cleanup cannot release a completed worker before reconciliation settles its rows"
        );
        assert_eq!(
            custody.exit_observed(&run_id, &operation_id).unwrap(),
            Some(true),
            "the exact exited worker remains in custody until terminal projection"
        );
        assert!(data_dir.exists(), "unsettled evidence remains on disk");

        let settlement_deadline = Instant::now() + Duration::from_secs(10);
        loop {
            clear_single_subject_cursor(&owner.store).await;
            owner
                .store
                .reconcile_scripts_once()
                .await
                .expect("reconciliation settles the exact successful completion");
            let (operation_state, run_state, diagnostic, _) =
                stored_state(&owner.store, &run_id, &operation_id).await;
            if operation_state == "settled" && run_state == "completed" {
                assert!(
                    cleanup.confirm_terminal_settlement(
                        &run_id,
                        &operation_id,
                        &operation_state,
                        &run_state,
                        diagnostic.as_deref(),
                    ),
                    "only the exact clean terminal Operation/ScriptRun projection permits release"
                );
                break;
            }
            assert!(
                Instant::now() < settlement_deadline,
                "completed source worker reaches terminal projection: operation={operation_state}, run={run_state}, diagnostic={diagnostic:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        // Reconciliation may have already released the exact child after
        // terminal projection; an absent custody entry is confirmed release.
        match custody.exit_observed(&run_id, &operation_id).unwrap() {
            None => cleanup.disarm(),
            Some(true) => assert!(
                cleanup
                    .release_after_terminal()
                    .expect("release terminal worker after confirmed family departure")
            ),
            Some(false) => panic!("terminal ScriptRun still has a live Store-owned worker"),
        }
        cleanup.disarm();
        drop(cleanup);
        assert_eq!(
            custody.exit_observed(&run_id, &operation_id).unwrap(),
            None,
            "fixture cleanup confirms release after the exact worker family departs"
        );
        owner
            .close()
            .await
            .expect("close Store after exact worker-family release");
        if data_dir.exists() {
            fs::remove_dir_all(data_dir).expect("remove fixture after confirmed worker departure");
        }
    });
}
