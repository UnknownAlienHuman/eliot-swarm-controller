#![cfg(any(target_os = "windows", target_os = "linux"))]

use serde_json::{Value, json};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const FIXTURE_MODE: &str = "ELIOT_MODULE_CUSTODY_FIXTURE_MODE";
const FIXTURE_EVENTS: &str = "ELIOT_MODULE_CUSTODY_FIXTURE_EVENTS";
const FIXTURE_CHILD: &str = "ELIOT_MODULE_CUSTODY_FIXTURE_CHILD";
const FIXTURE_EXIT: &str = "ELIOT_MODULE_CUSTODY_FIXTURE_EXIT";
const CHILD_STARTED: &str = "MODULE_CUSTODY_GRANDCHILD_STARTED";
const CHILD_EXITED: &str = "MODULE_CUSTODY_GRANDCHILD_EXITED";
const FIXTURE_LIFETIME: Duration = Duration::from_secs(50);
const PIPE_CLOSE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Debug)]
struct CapturedCli {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    elapsed: Duration,
}

#[derive(Debug, serde::Deserialize)]
struct FixtureProcess {
    pid: u32,
    birth: Value,
}

#[derive(Debug)]
enum PipeCapture {
    Stdout(Vec<u8>),
    Stderr(Vec<u8>),
}

fn new_test_directory() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "eliot module run custody-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&path).expect("create isolated module custody test directory");
    path
}

fn fixture_path(name: &str) -> PathBuf {
    std::env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("fixture environment variable {name} is required"))
}

fn append_fixture_event(mode: &str) {
    let path = fixture_path(FIXTURE_EVENTS);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open fixture event log");
    writeln!(file, "{mode}").expect("record fixture bridge start");
    file.sync_all().expect("flush fixture bridge start");
}

/// The real host launches this test executable as its native bridge. The
/// bridge starts one exact child and exits, leaving that child in the host's
/// existing process family. A later short invocation proves replacement.
#[test]
fn custody_fixture_bridge_process() {
    let Ok(mode) = std::env::var(FIXTURE_MODE) else {
        return;
    };
    assert!(matches!(mode.as_str(), "long" | "short"));
    append_fixture_event(&mode);
    if mode == "short" {
        return;
    }

    let child = Command::new(std::env::current_exe().expect("fixture executable path"))
        .args([
            "--exact",
            "custody_fixture_grandchild_process",
            "--nocapture",
        ])
        .env(FIXTURE_MODE, "grandchild")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn finite native grandchild fixture");
    let pid = child.id();
    let birth = swarm_process::process_birth_identity(pid)
        .expect("query exact grandchild birth identity")
        .expect("grandchild remains live after spawn");
    fs::write(
        fixture_path(FIXTURE_CHILD),
        serde_json::to_vec(&json!({"pid":pid,"birth":birth}))
            .expect("serialize grandchild birth identity"),
    )
    .expect("publish exact grandchild identity for the parent test");
    // Dropping Child does not signal it. Its own finite fixture lifetime ends
    // the process naturally; no broad process lookup or kill is used.
    drop(child);
}

/// Invoked only by custody_fixture_bridge_process as a native descendant.
#[test]
fn custody_fixture_grandchild_process() {
    if std::env::var(FIXTURE_MODE).as_deref() != Ok("grandchild") {
        return;
    }
    println!("{CHILD_STARTED}");
    std::io::stdout()
        .flush()
        .expect("flush grandchild output to the owner's private log");
    thread::sleep(FIXTURE_LIFETIME);
    fs::write(fixture_path(FIXTURE_EXIT), CHILD_EXITED).expect("record natural grandchild exit");
    println!("{CHILD_EXITED}");
}

fn run_cli(
    state_dir: &Path,
    fixture_mode: &str,
    events: &Path,
    child_identity: &Path,
    child_exit: &Path,
    timeout: Duration,
) -> CapturedCli {
    let started = Instant::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_swarm-kernel-host"))
        .arg("module-run")
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--command")
        .arg(std::env::current_exe().expect("test executable path"))
        .arg("--")
        .args(["--exact", "custody_fixture_bridge_process", "--nocapture"])
        .env(FIXTURE_MODE, fixture_mode)
        .env(FIXTURE_EVENTS, events)
        .env(FIXTURE_CHILD, child_identity)
        .env(FIXTURE_EXIT, child_exit)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the public swarm-kernel-host CLI");

    let stdout = child.stdout.take().expect("captured CLI stdout");
    let stderr = child.stderr.take().expect("captured CLI stderr");
    let (sender, receiver) = mpsc::channel();
    let stdout_sender = sender.clone();
    let stdout_reader = thread::spawn(move || send_pipe_capture(stdout, stdout_sender, true));
    let stderr_reader = thread::spawn(move || send_pipe_capture(stderr, sender, false));

    let deadline = started + timeout;
    let status = loop {
        match child.try_wait().expect("observe exact CLI child") {
            Some(status) => break status,
            None if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
            None => {
                let pid = child.id();
                // Only the exact CLI child created above is stopped on timeout.
                // Its owner worker and native family retain their own custody.
                let _ = child.kill();
                let _ = child.wait();
                panic!("public CLI PID {pid} did not return within {timeout:?}");
            }
        }
    };
    let elapsed = started.elapsed();
    drop(child);

    let pipe_deadline = Instant::now() + PIPE_CLOSE_TIMEOUT;
    let mut captured_stdout = None;
    let mut captured_stderr = None;
    while captured_stdout.is_none() || captured_stderr.is_none() {
        let remaining = pipe_deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "CLI exited but a descendant kept its foreground stdout or stderr pipe open"
        );
        match receiver.recv_timeout(remaining) {
            Ok(PipeCapture::Stdout(bytes)) => captured_stdout = Some(bytes),
            Ok(PipeCapture::Stderr(bytes)) => captured_stderr = Some(bytes),
            Err(error) => panic!("CLI output pipes did not close after exit: {error}"),
        }
    }
    stdout_reader.join().expect("stdout reader exits after EOF");
    stderr_reader.join().expect("stderr reader exits after EOF");

    CapturedCli {
        status,
        stdout: captured_stdout.expect("stdout capture completed"),
        stderr: captured_stderr.expect("stderr capture completed"),
        elapsed,
    }
}

fn send_pipe_capture<R: Read + Send + 'static>(
    mut pipe: R,
    sender: mpsc::Sender<PipeCapture>,
    stdout: bool,
) {
    let mut bytes = Vec::new();
    pipe.read_to_end(&mut bytes)
        .expect("read bounded integration CLI output");
    let capture = if stdout {
        PipeCapture::Stdout(bytes)
    } else {
        PipeCapture::Stderr(bytes)
    };
    let _ = sender.send(capture);
}

fn error_code(output: &CapturedCli) -> String {
    String::from_utf8_lossy(&output.stderr)
        .lines()
        .rev()
        .find_map(|line| {
            serde_json::from_str::<Value>(line)
                .ok()?
                .get("error")?
                .get("code")?
                .as_str()
                .map(str::to_owned)
        })
        .unwrap_or_else(|| {
            panic!(
                "CLI returned no structured error code; stderr was: {}",
                String::from_utf8_lossy(&output.stderr)
            )
        })
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(
        &fs::read(path)
            .unwrap_or_else(|error| panic!("read JSON evidence {}: {error}", path.display())),
    )
    .unwrap_or_else(|error| panic!("parse JSON evidence {}: {error}", path.display()))
}

fn event_lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn same_birth(expected: &Value, observed: &Value) -> bool {
    if expected["pid"] != observed["pid"] {
        return false;
    }
    #[cfg(windows)]
    {
        let expected_birth = expected["creation_filetime"]
            .as_u64()
            .or_else(|| expected["creation_filetime"].as_str()?.parse().ok());
        let observed_birth = observed["creation_filetime"]
            .as_u64()
            .or_else(|| observed["creation_filetime"].as_str()?.parse().ok());
        expected_birth.is_some() && expected_birth == observed_birth
    }
    #[cfg(target_os = "linux")]
    {
        expected["start_ticks"] == observed["start_ticks"]
            && !expected["start_ticks"].is_null()
            && expected["boot_id"] == observed["boot_id"]
            && !expected["boot_id"].is_null()
    }
}

fn assert_process_birth_is_live(pid: u32, expected: &Value) {
    let observed = swarm_process::process_birth_identity(pid)
        .unwrap_or_else(|error| panic!("query process {pid} birth identity: {error}"))
        .unwrap_or_else(|| panic!("process {pid} departed before expected"));
    assert!(
        same_birth(expected, &observed),
        "process {pid} identity changed: expected {expected}, observed {observed}"
    );
}

fn wait_for_process_departure(pid: u32, expected: &Value, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match swarm_process::process_birth_identity(pid) {
            Ok(None) => return,
            Ok(Some(observed)) if !same_birth(expected, &observed) => return,
            Ok(Some(_)) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
            Ok(Some(observed)) => {
                panic!("exact grandchild {pid} remained live past {timeout:?}: {observed}")
            }
            Err(error) => panic!("grandchild {pid} departure is unknown: {error}"),
        }
    }
}

fn wait_for_departed_receipt(state_dir: &Path, owner: &Value, timeout: Duration) -> Value {
    let receipt_path = state_dir.join("cleanup-status.json");
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(bytes) = fs::read(&receipt_path)
            && let Ok(receipt) = serde_json::from_slice::<Value>(&bytes)
            && receipt["disposition"] == "Departed"
            && receipt["family_departure"] == "verified_empty"
            && receipt["owner"] == *owner
        {
            return receipt;
        }
        assert!(
            Instant::now() < deadline,
            "owner did not publish exact-family departure in {timeout:?}"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn create_corrupt_owner_state(state_dir: &Path) {
    fs::create_dir_all(state_dir).expect("create corrupt-state fixture directory");
    fs::write(state_dir.join("module.lock"), b"ELIOT_SWARM_MODULE_V1\n")
        .expect("create exact module marker");
    fs::write(state_dir.join("owner.json"), b"{not valid JSON")
        .expect("create corrupt prior owner evidence");
}

fn wait_for_marker_release(state_dir: &Path, timeout: Duration) {
    let canonical = fs::canonicalize(state_dir).expect("canonicalize module state directory");
    let deadline = Instant::now() + timeout;
    loop {
        match swarm_process::acquire_state_marker(
            &canonical,
            "module.lock",
            b"ELIOT_SWARM_MODULE_V1\n",
        ) {
            Ok(marker) => {
                drop(marker);
                return;
            }
            Err(swarm_process::StateMarkerError::Busy) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(swarm_process::StateMarkerError::Busy) => {
                panic!("module owner did not release its exact lock within {timeout:?}");
            }
            Err(_) => panic!("module lock became invalid after exact family departure"),
        }
    }
}

#[test]
fn public_module_run_returns_pending_and_retains_exact_family_custody() {
    let root = new_test_directory();
    let state_dir = root.join("state");
    let corrupt_state = root.join("corrupt-state");
    let events = root.join("bridge-events.txt");
    let corrupt_events = root.join("corrupt-bridge-events.txt");
    let child_identity_path = root.join("grandchild-identity.json");
    let child_exit_path = root.join("grandchild-exited.txt");

    // Corrupt retained owner state is rejected before a native bridge effect.
    create_corrupt_owner_state(&corrupt_state);
    let corrupt = run_cli(
        &corrupt_state,
        "short",
        &corrupt_events,
        &child_identity_path,
        &child_exit_path,
        Duration::from_secs(10),
    );
    assert!(!corrupt.status.success());
    assert_eq!(error_code(&corrupt), "MODULE_OWNER_IDENTITY_INVALID");
    assert!(event_lines(&corrupt_events).is_empty());
    assert!(corrupt_state.join("recovery-gap.json").is_file());

    // The public CLI waits through the configured grace, then reports Pending
    // while the finite native grandchild remains in the exact process family.
    let first = run_cli(
        &state_dir,
        "long",
        &events,
        &child_identity_path,
        &child_exit_path,
        Duration::from_secs(55),
    );
    assert!(!first.status.success());
    assert_eq!(error_code(&first), "MODULE_CLEANUP_PENDING");
    assert!(first.elapsed >= Duration::from_secs(29));
    assert!(
        first.elapsed < Duration::from_secs(42),
        "foreground waited for native family departure: {:?}",
        first.elapsed
    );
    assert!(String::from_utf8_lossy(&first.stdout).contains(CHILD_STARTED));
    assert_eq!(event_lines(&events), vec!["long"]);

    let owner = read_json(&state_dir.join("owner.json"));
    let owner_token = owner["token"]
        .as_str()
        .expect("owner record has a UUID token");
    assert!(uuid::Uuid::parse_str(owner_token).is_ok());
    let owner_process = &owner["process"];
    assert_eq!(owner_process["purpose"], "module");
    let owner_pid = owner_process["pid"]
        .as_u64()
        .and_then(|pid| u32::try_from(pid).ok())
        .expect("owner record has an exact process PID");
    assert_process_birth_is_live(owner_pid, owner_process);

    let pending = read_json(&state_dir.join("cleanup-status.json"));
    assert_eq!(pending["disposition"], "CleanupPending");
    assert_eq!(pending["family_departure"], "not_proven");
    assert_eq!(pending["custody"], "retained_by_current_owner_process");
    assert_eq!(pending["owner"], owner);
    let stdout_log = owner["stdout_log"]
        .as_str()
        .map(PathBuf::from)
        .expect("owner records private stdout log path");
    assert!(stdout_log.is_file());
    assert!(
        fs::read_to_string(&stdout_log)
            .expect("read private native output log while grandchild is live")
            .contains(CHILD_STARTED)
    );

    let fixture_process: FixtureProcess = serde_json::from_slice(
        &fs::read(&child_identity_path).expect("read exact grandchild identity"),
    )
    .expect("parse exact grandchild identity");
    assert_process_birth_is_live(fixture_process.pid, &fixture_process.birth);

    // A concurrent public launch sees the held OS marker and cannot start a
    // second bridge while the old grandchild remains alive.
    let refused = run_cli(
        &state_dir,
        "short",
        &events,
        &child_identity_path,
        &child_exit_path,
        Duration::from_secs(10),
    );
    assert!(!refused.status.success());
    assert_eq!(error_code(&refused), "MODULE_OWNER_ACTIVE");
    assert_eq!(event_lines(&events), vec!["long"]);
    assert_process_birth_is_live(fixture_process.pid, &fixture_process.birth);

    // Wait for the child's own finite lifetime and the owner's exact
    // Group::children_empty receipt. PID reuse or an unreadable birth proof is
    // never treated as departure by this test.
    let exit_deadline = Instant::now() + Duration::from_secs(30);
    while !child_exit_path.is_file() {
        assert!(
            Instant::now() < exit_deadline,
            "grandchild fixture did not exit"
        );
        thread::sleep(Duration::from_millis(50));
    }
    wait_for_process_departure(
        fixture_process.pid,
        &fixture_process.birth,
        Duration::from_secs(5),
    );
    let departed = wait_for_departed_receipt(&state_dir, &owner, Duration::from_secs(10));
    assert_eq!(departed["owner"], owner);

    // The receipt is written immediately before lock release. Observe release
    // through the same OS marker primitive so the final public launch has no
    // timing race and remains a single native invocation.
    wait_for_marker_release(&state_dir, Duration::from_secs(5));
    let replacement = run_cli(
        &state_dir,
        "short",
        &events,
        &child_identity_path,
        &child_exit_path,
        Duration::from_secs(10),
    );
    assert!(replacement.status.success());
    assert_eq!(event_lines(&events), vec!["long", "short"]);

    fs::remove_dir_all(&root).expect("remove isolated test data after all owners departed");
}
