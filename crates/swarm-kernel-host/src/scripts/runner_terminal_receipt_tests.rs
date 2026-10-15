use super::*;
use base64::Engine as _;
use std::{
    fs,
    path::PathBuf,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const CHILD_MODE_ENV: &str = "ELIOT_SCRIPT_TERMINAL_FIXTURE_MODE";
const CHILD_RECEIPT_ENV: &str = "ELIOT_SCRIPT_TERMINAL_FIXTURE_RECEIPT";
const CHILD_READY_ENV: &str = "ELIOT_SCRIPT_TERMINAL_FIXTURE_READY";
const CHILD_RELEASE_ENV: &str = "ELIOT_SCRIPT_TERMINAL_FIXTURE_RELEASE";
const CHILD_TEST: &str = "scripts::runner::terminal_receipt_tests::owned_child_receipt_fixture";

struct OwnedFixture {
    work: Work,
    work_digest: String,
    custody: ScriptChildCustody,
    root: PathBuf,
    ready_path: PathBuf,
    release_path: PathBuf,
}

impl OwnedFixture {
    fn spawn(mode: &str) -> Self {
        let root =
            std::env::temp_dir().join(format!("script-terminal-receipt-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&root).expect("create isolated terminal receipt fixture");
        let root = fs::canonicalize(&root).expect("canonicalize isolated terminal receipt fixture");
        let work = fixture_work(&root);
        let (receipt_path, work_digest) = publish_fixture_work(&work);
        let ready_path = root.join("fixture.ready");
        let release_path = root.join("fixture.release");
        let child = Command::new(std::env::current_exe().expect("test executable path"))
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env(CHILD_MODE_ENV, mode)
            .env(CHILD_RECEIPT_ENV, &receipt_path)
            .env(CHILD_READY_ENV, &ready_path)
            .env(CHILD_RELEASE_ENV, &release_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn a real owned process for the receipt consumer");
        let pid = child.id();
        let custody = ScriptChildCustody::default();
        custody.owners.lock().expect("lock child custody").insert(
            work.run_id.clone(),
            ScriptChildOwner {
                data_dir: work.data_dir.clone(),
                work_digest: work_digest.clone(),
                operation_id: work.operation_id.clone(),
                token: work.token.clone(),
                pid,
                child: Some(child),
                launch: None,
                launch_persistence_pending: false,
                exit: None,
                release_when_exited: false,
            },
        );
        Self {
            work,
            work_digest,
            custody,
            root,
            ready_path,
            release_path,
        }
    }

    fn observe(&self) -> ScriptChildObservation {
        self.custody
            .observe(&self.work, &self.work_digest)
            .expect("observe the retained real Child")
            .expect("fixture remains in Store-owned custody")
    }

    fn wait_for_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.ready_path.is_file() {
            let observation = self.observe();
            if self.ready_path.is_file() {
                break;
            }
            assert!(
                observation.exit.is_none(),
                "receipt fixture exited before publishing readiness"
            );
            assert!(
                Instant::now() < deadline,
                "receipt fixture did not become ready"
            );
            thread::sleep(Duration::from_millis(10));
        }
        self.observe();
    }

    fn wait_for_exit(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(exit) = self.observe().exit {
                assert_eq!(exit.code, Some(0), "receipt fixture exits successfully");
                return;
            }
            assert!(Instant::now() < deadline, "receipt fixture did not exit");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn launch(&self) -> Value {
        self.custody.owners.lock().expect("lock child custody")[&self.work.run_id]
            .launch
            .clone()
            .expect("retained Child has a captured launch birth identity")
    }

    fn owner_confirms_terminal_family_departure(&self) -> bool {
        let owners = self.custody.owners.lock().expect("lock child custody");
        let owner = owners.get(&self.work.run_id).expect("retained child owner");
        let launch = owner.launch.as_ref().expect("captured launch identity");
        terminal_completion_confirms_child_family_departure(owner, launch)
    }
}

impl Drop for OwnedFixture {
    fn drop(&mut self) {
        let _ = fs::write(&self.release_path, b"release");
        if let Ok(mut owners) = self.custody.owners.lock() {
            for owner in owners.values_mut() {
                if let Some(child) = owner.child.as_mut() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
            }
            owners.clear();
        }
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn fixture_work(root: &std::path::Path) -> Work {
    let mut work = durable_authority_tests::fixture_work(root, None);
    let script_source = b"Write-Output 'terminal receipt fixture'\n";
    work.bundle.files = vec![manifest::BundleFile {
        path: work.bundle.entrypoint.clone(),
        sha256: model::digest(script_source),
        byte_length: script_source.len() as u64,
        content_base64: base64::engine::general_purpose::STANDARD.encode(script_source),
    }];
    let bundle_bytes = model::canonical(&json!(work.bundle)).expect("canonical fixture bundle");
    work.bundle_record.relative_path = format!("artifacts/{}.bin", work.bundle_record.artifact_id);
    work.bundle_record.byte_length = bundle_bytes.len() as u64;
    work.bundle_record.content_digest = model::digest(bundle_bytes.as_bytes());
    work.bundle_record.metadata = json!({
        "script_id":work.bundle.script_id,
        "revision":work.invocation.script_revision,
        "bundle_sha256":work.bundle_record.content_digest,
        "interpreter_sha256":work.interpreter.sha256,
    });
    work.environment_sha256 =
        environment_sha256(&work.environment).expect("hash terminal receipt fixture environment");
    validate_work(&work).expect("fixture Work matches the retained completion contract");
    work
}

fn publish_fixture_work(work: &Work) -> (PathBuf, String) {
    let dir = directory(&work.data_dir, &work.run_id).expect("build fixture run directory");
    fs::create_dir_all(&dir).expect("create fixture run directory");
    ensure_private_directory(&work.data_dir, &dir).expect("secure fixture run directory");
    let work_bytes = model::canonical(&json!(work))
        .expect("canonical fixture Work")
        .into_bytes();
    let work_digest = model::digest(&work_bytes);
    write_once(&dir.join("work.json"), &work_bytes).expect("publish pinned fixture Work");
    let receipt = WorkerReceipt {
        run_id: work.run_id.clone(),
        work_digest: work_digest.clone(),
        data_dir: work.data_dir.clone(),
    };
    write_once(
        &dir.join("receipt.json"),
        &model::canonical(&json!(receipt))
            .expect("canonical fixture receipt")
            .into_bytes(),
    )
    .expect("publish pinned Work receipt");
    (dir.join("receipt.json"), work_digest)
}

fn write_terminal_fixture(work: &Work, process: &Value, mode: &str) {
    let released = mode != "unknown";
    let direct_state = match mode {
        "direct_unknown" => "observation_unknown",
        "direct_not_started" => "not_started",
        _ => "observed",
    };
    let facts = json!({
        "schema_version":1,
        "direct_exit":{"state":direct_state,"exit_code":if mode == "direct_wrong_code" {7} else {0}},
        "family_departure":{
            "state":if released {"confirmed"} else {"observation_unknown"},
            "process":process,
        },
        "stdout_capture":empty_capture_fact(),
        "stderr_capture":empty_capture_fact(),
        "resource_released":released,
        "cleanup_pending":!released,
        "timed_out":false,
        "termination_attempts":0,
        "termination_request_unconfirmed":false,
        "input_write_complete":true,
        "primary_error_code":null,
    });
    let started_at_ms = model::now_ms().expect("read fixture timestamp");
    let result_value = Value::Null;
    let stdout = empty_output_record(work, "stdout");
    let stderr = empty_output_record(work, "stderr");
    let result_id = format!(
        "scriptresult-{}",
        model::digest(work.operation_id.as_bytes())
    );
    let result_document = json!({
        "protocol_version":1,
        "run_id":work.run_id,
        "operation_id":work.operation_id,
        "script_id":work.bundle.script_id,
        "script_revision":work.invocation.script_revision,
        "state":"completed",
        "started_at_ms":started_at_ms,
        "exit_code":0,
        "error_code":null,
        "result":result_value,
        "stdout_ref":stdout.artifact_id,
        "stderr_ref":stderr.artifact_id,
        "controller_effects":[],
        "process_facts":facts,
    });
    let (result, result_bytes) = ArtifactFiles::document(
        "script_result",
        &result_id,
        &result_document,
        json!({
            "run_id":work.run_id,
            "operation_id":work.operation_id,
            "script_id":work.bundle.script_id,
            "state":"completed",
        }),
    )
    .expect("build validated result artifact");
    let files = ArtifactFiles::new(&work.data_dir).expect("open fixture artifacts");
    files
        .publish(&result, &result_bytes)
        .expect("publish result artifact");
    files
        .publish(&stdout, &[])
        .expect("publish empty stdout artifact");
    files
        .publish(&stderr, &[])
        .expect("publish empty stderr artifact");

    let completion = Completion {
        run_id: work.run_id.clone(),
        operation_id: work.operation_id.clone(),
        token: work.token.clone(),
        state: "completed".to_owned(),
        started_at_ms: Some(started_at_ms),
        exit_code: Some(0),
        process: process.clone(),
        result,
        stdout,
        stderr,
        result_value: Some(result_value),
        controller_effects: Vec::new(),
        error_code: None,
        process_facts: Some(facts),
    };
    let terminal_path = directory(&work.data_dir, &work.run_id)
        .expect("build fixture run directory")
        .join("terminal.json");
    write_once(
        &terminal_path,
        &model::canonical(&json!(completion))
            .expect("canonical terminal completion")
            .into_bytes(),
    )
    .expect("publish terminal completion");
}

fn empty_capture_fact() -> Value {
    json!({
        "state":"complete",
        "bytes_retained":0,
        "bytes_observed":0,
        "capture_complete":true,
        "truncated":false,
        "capture_error":null,
    })
}

fn empty_output_record(work: &Work, stream: &str) -> ArtifactRecord {
    let id = format!(
        "scriptlog-{}",
        model::digest(format!("{}:{stream}", work.run_id).as_bytes())
    );
    ArtifactRecord {
        kind: "script_output".to_owned(),
        artifact_id: id.clone(),
        relative_path: format!("artifacts/{id}.bin"),
        byte_length: 0,
        content_digest: model::digest(&[]),
        metadata: json!({
            "run_id":work.run_id,
            "operation_id":work.operation_id,
            "stream":stream,
        }),
    }
}

#[test]
fn terminal_receipt_fallback_requires_valid_proof_and_child_exit() {
    let valid = OwnedFixture::spawn("valid_hold");
    valid.wait_for_ready();
    let files = ArtifactFiles::new(&valid.work.data_dir).expect("open valid receipt artifacts");
    let live_completion = completion(&valid.work, &files)
        .expect("validate live worker terminal")
        .expect("terminal receipt exists");
    assert!(
        !completion_family_departed(&valid.work, &live_completion)
            .expect("query family while the actual Child is live"),
        "a positive receipt cannot replace the live Child/family observation"
    );
    assert!(
        !valid
            .custody
            .release_after_terminal(&valid.work.run_id, &valid.work.operation_id)
            .expect("retain live Child custody"),
        "terminal custody waits for the actual Child to exit"
    );
    assert_eq!(valid.custody.owners.lock().unwrap().len(), 1);

    fs::write(&valid.release_path, b"release").expect("allow fixture worker to exit");
    valid.wait_for_exit();
    let launch = valid.launch();
    #[cfg(windows)]
    assert!(matches!(
        spawned_departed(&launch["process"], &valid.work.token),
        Err(ref error) if error.code == "PROCESS_FAMILY_UNKNOWN"
    ));
    let exited_completion = completion(&valid.work, &files)
        .expect("revalidate terminal after Child exit")
        .expect("terminal receipt remains available");
    assert!(
        completion_family_departed(&valid.work, &exited_completion)
            .expect("consume exact terminal family proof after the name disappears")
    );
    assert!(
        valid
            .custody
            .release_after_terminal(&valid.work.run_id, &valid.work.operation_id)
            .expect("release after actual family departure")
    );
    assert!(valid.custody.owners.lock().unwrap().is_empty());
    assert!(
        valid
            .custody
            .release_after_terminal(&valid.work.run_id, &valid.work.operation_id)
            .expect("terminal release is idempotent")
    );
    drop(valid);

    for_negative_receipt_fixture("missing", |result| {
        assert!(
            result
                .expect("missing receipt is not an I/O failure")
                .is_none()
        );
    });
    for_negative_receipt_fixture("invalid", |result| {
        assert!(result.is_err(), "malformed completion cannot be validated");
    });
    for_negative_receipt_fixture("unknown", |result| {
        assert!(result.is_err(), "unknown family facts cannot be validated");
    });
    for mode in ["direct_unknown", "direct_not_started", "direct_wrong_code"] {
        for_negative_receipt_fixture(mode, |result| {
            assert!(
                result.is_err(),
                "{mode} cannot validate a completed result or release custody"
            );
        });
    }
}

fn for_negative_receipt_fixture(
    mode: &str,
    assert_completion: impl FnOnce(Result<Option<Completion>>),
) {
    let fixture = OwnedFixture::spawn(mode);
    fixture.wait_for_ready();
    fixture.wait_for_exit();
    let files =
        ArtifactFiles::new(&fixture.work.data_dir).expect("open negative fixture artifacts");
    assert_completion(completion(&fixture.work, &files));
    assert!(
        !fixture.owner_confirms_terminal_family_departure(),
        "{mode} receipt cannot authorize the missing-Job fallback"
    );
}

#[test]
fn owned_child_receipt_fixture() {
    let Ok(mode) = std::env::var(CHILD_MODE_ENV) else {
        return;
    };
    let receipt_path = PathBuf::from(
        std::env::var_os(CHILD_RECEIPT_ENV).expect("parent supplies Work receipt path"),
    );
    let ready_path = PathBuf::from(
        std::env::var_os(CHILD_READY_ENV).expect("parent supplies fixture readiness path"),
    );
    let release_path = PathBuf::from(
        std::env::var_os(CHILD_RELEASE_ENV).expect("parent supplies fixture release path"),
    );
    let (work, _) = read_work_record(&receipt_path).expect("read exact fixture Work receipt");
    let group = platform::process_group::Group::enter_script(&work.token)
        .expect("enter the token-owned script family");
    let process = group.identity.clone();
    let dir = directory(&work.data_dir, &work.run_id).expect("build fixture run directory");
    let ready = json!({
        "run_id":work.run_id,
        "operation_id":work.operation_id,
        "token":work.token,
        "control_version":1,
        "ready_at_ms":model::now_ms().expect("read ready timestamp"),
        "process":process,
    });
    write_once(
        &dir.join("worker.json"),
        &model::canonical(&ready)
            .expect("canonical worker identity")
            .into_bytes(),
    )
    .expect("publish worker identity");
    group
        .disarm()
        .expect("prove no active descendants before release");
    drop(group);

    match mode.as_str() {
        "missing" => {}
        "invalid" => fs::write(dir.join("terminal.json"), b"{invalid")
            .expect("write malformed terminal receipt"),
        "valid_hold" | "unknown" | "direct_unknown" | "direct_not_started"
        | "direct_wrong_code" => {
            write_terminal_fixture(&work, &ready["process"], &mode);
        }
        _ => panic!("unknown terminal receipt fixture mode"),
    }
    fs::write(&ready_path, b"ready").expect("signal receipt fixture readiness");
    if mode == "valid_hold" {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !release_path.is_file() {
            assert!(
                Instant::now() < deadline,
                "parent did not release fixture worker"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}
