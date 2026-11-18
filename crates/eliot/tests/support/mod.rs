use serde_json::Value;
use std::{
    ffi::{OsStr, OsString},
    fs,
    io::{self, Read, Seek},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Receiver},
    },
    thread,
    time::{Duration, Instant},
};

pub const CLI: &str = env!("CARGO_BIN_EXE_swarm");
const WAIT: Duration = Duration::from_secs(20);
// Bounded first-line CLI stderr snippets in failure diagnostics. Full outcome
// text stays out; the daemon's timestamped log in the retained fixture has it.
const MAX_ERROR_SNIPPET_CHARS: usize = 240;

static CURRENTLY_STARTING_DAEMONS_PER_PROCESS: AtomicUsize = AtomicUsize::new(0);
static PEAK_STARTING_DAEMONS_PER_PROCESS: AtomicUsize = AtomicUsize::new(0);

pub const AMBIENT_IDENTITY_ENV_KEYS: &[&str] = &[
    "ELIOT_AGENT_ID",
    "ELIOT_AGENT_GENERATION",
    "ELIOT_AGENT_TOKEN",
    "ELIOT_OWNER_ID",
    "ELIOT_OWNER_TOKEN",
    "ELIOT_OWNER_CREDENTIAL",
    "ELIOT_RUN_COMMAND_ID",
    "ELIOT_RUN_TOKEN",
];

pub fn scrub_ambient_identity(command: &mut Command) -> &mut Command {
    for &key in AMBIENT_IDENTITY_ENV_KEYS {
        command.env_remove(key);
    }
    command
}

struct DaemonStartGuard;

impl DaemonStartGuard {
    fn enter() -> Self {
        let current = CURRENTLY_STARTING_DAEMONS_PER_PROCESS.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = PEAK_STARTING_DAEMONS_PER_PROCESS.fetch_max(current, Ordering::SeqCst);
        Self
    }
}

impl Drop for DaemonStartGuard {
    fn drop(&mut self) {
        CURRENTLY_STARTING_DAEMONS_PER_PROCESS.fetch_sub(1, Ordering::SeqCst);
    }
}

fn error_snippet(stderr: &str) -> String {
    let line = stderr.lines().next().unwrap_or("").trim();
    if line.chars().count() <= MAX_ERROR_SNIPPET_CHARS {
        return line.to_owned();
    }
    let mut end: usize = line
        .char_indices()
        .take(MAX_ERROR_SNIPPET_CHARS + 1)
        .map(|(index, _)| index)
        .last()
        .unwrap_or(0);
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}[truncated]", &line[..end])
}

/// Bounded readiness fact: whether the daemon published its endpoint record,
/// without IPC or credentials. PIDs and endpoint lengths only.
/// The last startup log codes with their timestamps (codes only, never record
/// payloads), so a readiness timeout names the phase that stalled.
fn daemon_log_phases(data: &Path) -> String {
    let Ok(log) = fs::read_to_string(data.join("logs").join("daemon.log")) else {
        return "absent".to_owned();
    };
    let records: Vec<(String, i64)> = log
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|record| {
            Some((
                record.get("code")?.as_str()?.to_owned(),
                record.get("ts")?.as_i64()?,
            ))
        })
        .collect();
    let Some(&(_, first)) = records.first() else {
        return "empty".to_owned();
    };
    let start = records.len().saturating_sub(12);
    records[start..]
        .iter()
        .map(|(code, ts)| format!("{code}@+{}ms", ts - first))
        .collect::<Vec<_>>()
        .join(",")
}

fn daemon_info_snippet(data: &Path) -> String {
    let bytes = match fs::read(data.join("daemon.json")) {
        Ok(bytes) => bytes,
        Err(_) => return "absent".to_owned(),
    };
    match serde_json::from_slice::<Value>(&bytes) {
        Ok(info) => format!(
            "present(pid={},endpoint_chars={})",
            info["pid"].as_u64().unwrap_or(0),
            info["endpoint"].as_str().map_or(0, str::len),
        ),
        Err(_) => "unparseable".to_owned(),
    }
}

pub struct Captured {
    pub child: Child,
    stdout: Option<Receiver<io::Result<String>>>,
    stderr: Receiver<io::Result<String>>,
}

fn capture(pipe: impl Read + Send + 'static) -> Receiver<io::Result<String>> {
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = pipe
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .and_then(|_| {
                if bytes.len() > 1024 * 1024 {
                    return Err(io::Error::other("test child output exceeded 1 MiB"));
                }
                String::from_utf8(bytes).map_err(io::Error::other)
            });
        let _ = sender.send(result);
    });
    receiver
}

impl Captured {
    pub fn spawn(mut command: Command) -> Self {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start test-owned process");
        let stdout = child.stdout.take().map(capture);
        let stderr = capture(child.stderr.take().expect("stderr pipe"));
        Self {
            child,
            stdout,
            stderr,
        }
    }

    pub fn wait(&mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "test child did not exit",
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn output(&self, timeout: Duration) -> (String, String) {
        let deadline = Instant::now() + timeout;
        let stdout = self.stdout.as_ref().map_or_else(String::new, |stdout| {
            stdout
                .recv_timeout(timeout)
                .expect("stdout EOF deadline")
                .expect("read stdout")
        });
        let stderr = self
            .stderr
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .expect("stderr EOF deadline")
            .expect("read stderr");
        (stdout, stderr)
    }
}

impl Drop for Captured {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let _ = self.wait(Duration::from_secs(5));
        }
    }
}

/// Bounded tail of a fixture-owned daemon stderr file, oldest bytes dropped.
/// An early data-directory or lock failure happens before logging starts, so
/// the CLI's stderr is the only evidence; keeping it in a fixture-owned file
/// (never a pipe the daemon could hold) preserves the cause without leaking.
pub fn file_stderr_tail(path: &Path, max_bytes: u64) -> String {
    const TAIL_CONTEXT: u64 = 256;
    let mut file = match fs::File::open(path) {
        Ok(file) => file,
        Err(_) => return "stderr file absent".to_owned(),
    };
    let size = file.metadata().map(|meta| meta.len()).unwrap_or(0);
    let offset = size.saturating_sub(max_bytes.saturating_add(TAIL_CONTEXT));
    if file.seek(io::SeekFrom::Start(offset)).is_err() {
        return "stderr file unreadable".to_owned();
    }
    let mut bytes = Vec::new();
    if file
        .take(max_bytes.saturating_add(TAIL_CONTEXT))
        .read_to_end(&mut bytes)
        .is_err()
    {
        return "stderr file unreadable".to_owned();
    }
    let text = String::from_utf8_lossy(&bytes);
    let start = if offset > 0 {
        text.find('\n').map(|index| index + 1).unwrap_or(text.len())
    } else {
        0
    };
    const MAX_TAIL_CHARS: usize = 2048;
    let tail = &text[start..];
    if tail.chars().count() <= MAX_TAIL_CHARS {
        return tail.to_owned();
    }
    let mut chars: Vec<char> = tail.chars().collect();
    let skip = chars.len() - MAX_TAIL_CHARS;
    chars.drain(..skip);
    let mut trimmed: String = chars.into_iter().collect();
    while !trimmed.is_char_boundary(0) {
        trimmed.remove(0);
    }
    trimmed
}

/// A test-owned daemon child. Its stdio is never inherited and never piped
/// back to the test: stdin and stdout are null, and stderr goes to a
/// fixture-owned file, so even a leaked daemon can never hold the harness
/// pipe. Readiness and failure evidence comes from the daemon's files
/// (`daemon.json`, `logs/daemon.log`), the stderr file and IPC, never from a
/// pipe.
///
/// Drop kills the exact child handle first and reaps it within a bound, on
/// every path including a panic mid-startup. Kill targets this exact process
/// object, so PID reuse cannot redirect it; no IPC to a possibly wedged
/// daemon is ever required.
pub struct OwnedDaemon {
    child: Child,
    stderr_path: PathBuf,
}

impl OwnedDaemon {
    pub fn spawn(mut command: Command, stderr_path: &Path) -> Self {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let stderr_file =
            fs::File::create(stderr_path).expect("create fixture-owned daemon stderr file");
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_file))
            .spawn()
            .expect("start test-owned daemon");
        Self {
            child,
            stderr_path: stderr_path.to_owned(),
        }
    }

    /// Bounded tail of this daemon's fixture-owned stderr file.
    pub fn stderr_tail(&self) -> String {
        file_stderr_tail(&self.stderr_path, 8192)
    }

    pub fn id(&self) -> u32 {
        self.child.id()
    }

    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    /// Reap an already-stopping daemon without killing it: used after an
    /// acknowledged shutdown. A daemon still live past the deadline is a test
    /// failure the caller reports; the guard's Drop still kills it.
    pub fn reap(&mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "owned daemon did not exit",
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for OwnedDaemon {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            if self.reap(Duration::from_secs(10)).is_err() {
                eprintln!(
                    "test-owned daemon {} survived kill; leaked",
                    self.child.id()
                );
            }
        }
    }
}

pub struct Reply {
    pub status: ExitStatus,
    pub stdout: String,
    pub stderr: String,
    context: String,
}

impl Reply {
    pub fn json(self) -> Value {
        assert!(
            self.status.success(),
            "CLI {} exit {:?}; stderr omitted because a redaction regression may contain credentials",
            self.context,
            self.status.code()
        );
        serde_json::from_str(&self.stdout).expect("CLI JSON response")
    }
}

pub fn run(command: Command) -> Reply {
    // Bounded CLI argv for failure context, without inherited environment values.
    let args: Vec<_> = command
        .get_args()
        .take(32)
        .map(|argument| {
            argument
                .to_string_lossy()
                .chars()
                .take(256)
                .collect::<String>()
        })
        .collect();
    let context = format!("{:?} {args:?}", command.get_program());
    let mut process = Captured::spawn(command);
    let status = process
        .wait(Duration::from_secs(40))
        .expect("CLI process deadline");
    let (stdout, stderr) = process.output(Duration::from_secs(2));
    Reply {
        status,
        stdout,
        stderr,
        context,
    }
}

pub fn cli(data: &Path, args: &[&str]) -> Reply {
    let mut command = Command::new(CLI);
    scrub_ambient_identity(&mut command);
    command.args([
        OsStr::new("--data-dir"),
        data.as_os_str(),
        OsStr::new("--json"),
    ]);
    command.args(args);
    run(command)
}

/// Bounded panic check without pipes: the crash hook records a
/// `daemon.panicked-<pid>-<ts>.json` marker under `<data>/crashes`, and
/// daemon startup consumes markers from earlier runs. A marker present after
/// shutdown proves this run's daemon panicked.
fn assert_no_daemon_panic(data: &Path) {
    let mut panicked = false;
    if let Ok(entries) = fs::read_dir(data.join("crashes")) {
        for entry in entries.filter_map(|entry| entry.ok()) {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("daemon.panicked-"))
            {
                panicked = true;
                break;
            }
        }
    }
    assert!(
        !panicked,
        "daemon panicked; see the retained fixture crashes dir"
    );
}

pub struct Fixture {
    pub root: PathBuf,
    pub data: PathBuf,
    pub workspace: PathBuf,
    pub retain_data: bool,
    daemon: Option<OwnedDaemon>,
    daemon_env: Vec<(OsString, OsString)>,
    daemon_stderr: PathBuf,
    starts: u32,
}

impl Fixture {
    pub fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!("eliot {label} Проба {}", ulid::Ulid::new()));
        fs::create_dir(&root).expect("create unique test root");
        let workspace = root.join("repo space");
        fs::create_dir(&workspace).expect("create fixture workspace");
        let daemon_stderr = root.join("daemon.stderr.log");
        Self {
            data: root.join("data space"),
            workspace,
            daemon_stderr,
            root,
            daemon: None,
            daemon_env: Vec::new(),
            retain_data: false,
            starts: 0,
        }
    }

    /// Extra environment for the owned daemon child (for example selector
    /// credentials a regression test injects before startup).
    pub fn set_daemon_env(&mut self, key: &str, value: &str) {
        self.daemon_env
            .push((OsString::from(key), OsString::from(value)));
    }

    /// Bounded tail of the owned daemon's fixture-owned stderr file.
    pub fn daemon_stderr_tail(&self) -> String {
        file_stderr_tail(&self.daemon_stderr, 8192)
    }

    /// Exact PID of the owned daemon child (the guard's handle, never a
    /// re-resolved PID, so the proof test cannot observe a recycled process).
    pub fn daemon_pid(&self) -> u32 {
        self.daemon.as_ref().expect("owned daemon").id()
    }

    pub fn start(&mut self) {
        assert!(self.daemon.is_none());
        let _start_guard = DaemonStartGuard::enter();
        self.starts += 1;
        let mut data_argument = self.data.as_os_str().to_os_string();
        data_argument.push(std::path::MAIN_SEPARATOR_STR);
        let mut command = Command::new(CLI);
        scrub_ambient_identity(&mut command);
        for (key, val) in &self.daemon_env {
            command.env(key, val);
        }
        command
            .arg("--data-dir")
            .arg(data_argument)
            .args(["daemon", "--foreground"]);
        self.daemon = Some(OwnedDaemon::spawn(command, &self.daemon_stderr));
        let child_id = self.daemon.as_ref().unwrap().id();
        let started = Instant::now();
        let deadline = started + WAIT;
        let mut polls = 0u32;
        let mut first_error: Option<String> = None;
        loop {
            if let Some(status) = self
                .daemon
                .as_mut()
                .unwrap()
                .try_wait()
                .expect("daemon status")
            {
                // The owned daemon's stdout is null (never a pipe), so its
                // last words come from its files: the published endpoint
                // record, the timestamped phase codes in its log, and the
                // bounded stderr tail (the only evidence when startup fails
                // before logging starts).
                panic!(
                    "foreground daemon exited during startup (status={status:?}, per_process currently_starting={}, per_process peak_starting={}); daemon.json={}; log={}; stderr={}",
                    CURRENTLY_STARTING_DAEMONS_PER_PROCESS.load(Ordering::SeqCst),
                    PEAK_STARTING_DAEMONS_PER_PROCESS.load(Ordering::SeqCst),
                    daemon_info_snippet(&self.data),
                    daemon_log_phases(&self.data),
                    self.daemon.as_ref().unwrap().stderr_tail(),
                );
            }
            let response = cli(&self.data, &["status"]);
            polls += 1;
            if first_error.is_none() {
                first_error = Some(error_snippet(&response.stderr));
            }
            if response.status.success() {
                assert_eq!(response.json()["state"], "running");
                let info: Value = serde_json::from_slice(
                    &fs::read(self.data.join("daemon.json")).expect("published daemon info"),
                )
                .expect("daemon info JSON");
                assert_eq!(info["pid"], child_id);
                return;
            }
            assert!(
                Instant::now() < deadline,
                "daemon readiness deadline (start #{}, {polls} status polls in {:?}, per_process currently_starting={}, per_process peak_starting={}): child={}; daemon.json={}; first_error={}; last_error={}; log={}; stderr={}",
                self.starts,
                started.elapsed(),
                CURRENTLY_STARTING_DAEMONS_PER_PROCESS.load(Ordering::SeqCst),
                PEAK_STARTING_DAEMONS_PER_PROCESS.load(Ordering::SeqCst),
                match self.daemon.as_mut().unwrap().try_wait() {
                    Ok(None) => "alive".to_owned(),
                    Ok(Some(status)) => format!("exited({status:?})"),
                    Err(_) => "wait_error".to_owned(),
                },
                daemon_info_snippet(&self.data),
                first_error.as_deref().unwrap_or("none"),
                error_snippet(&response.stderr),
                daemon_log_phases(&self.data),
                self.daemon.as_ref().unwrap().stderr_tail(),
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn json(&self, args: &[&str]) -> Value {
        cli(&self.data, args).json()
    }

    pub fn events(&self) -> Vec<Value> {
        let mut after = 0;
        let mut events = Vec::new();
        let mut end = None;
        for _ in 0..8 {
            let page = self.json(&["events", "--since", &after.to_string(), "--limit", "1000"]);
            let end =
                *end.get_or_insert_with(|| page["high_water"].as_i64().expect("event high-water"));
            events.extend(
                page["events"]
                    .as_array()
                    .expect("events array")
                    .iter()
                    .filter(|event| event["seq"].as_i64().expect("event seq") <= end)
                    .cloned(),
            );
            let next = page["next"].as_i64().expect("event cursor");
            if next >= end {
                return events;
            }
            assert!(next > after, "nonadvancing event cursor");
            after = next;
        }
        panic!("fixture events exceeded eight bounded pages");
    }

    pub fn stop(&mut self, force: bool) {
        assert!(self.daemon.is_some());
        let stop = cli(
            &self.data,
            &["daemon", "stop", if force { "--force" } else { "--drain" }],
        );
        assert!(
            matches!(stop.status.code(), Some(0 | 6)),
            "shutdown reply: {}",
            stop.stderr
        );
        self.finish_shutdown();
    }

    /// A shutdown acknowledgement is not exit evidence. Wait on this fixture's
    /// exact owned process without admitting a second shutdown mutation.
    pub fn finish_shutdown(&mut self) {
        let daemon = self.daemon.as_mut().unwrap();
        assert!(
            daemon
                .reap(WAIT)
                .expect("owned daemon shutdown deadline")
                .success(),
            "owned daemon returned nonzero"
        );
        assert_no_daemon_panic(&self.data);
        self.daemon = None;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // The owned foreground daemon dies with the fixture on every path,
        // including a panic mid-startup such as the readiness deadline:
        // dropping the guard kills the exact child handle first and reaps it,
        // without depending on IPC to a daemon that may be wedged under load.
        if let Some(daemon) = self.daemon.take() {
            drop(daemon);
        }
        if self.retain_data {
            // No owned foreground child remains (killed above through its
            // exact handle). No IPC by data directory either: the daemon may
            // have exited and a replacement may since have bound the
            // directory. Detached-background cleanup belongs to the
            // birth-verified handle claimed at creation, never to this drop.
            eprintln!(
                "unconfirmed background fixture retained at {}",
                self.root.display()
            );
            return;
        }
        if thread::panicking() {
            eprintln!("failed fixture retained at {}", self.root.display());
            return;
        }
        // Only this create-new fixture path, after its exact owned child exited.
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn error_snippet_keeps_first_line_within_a_char_bound() {
        use super::{MAX_ERROR_SNIPPET_CHARS, error_snippet};
        assert_eq!(error_snippet(""), "");
        assert_eq!(
            error_snippet("error: unavailable: daemon is not running\nsecond line"),
            "error: unavailable: daemon is not running"
        );
        let cyrillic = "Проба ".repeat(100);
        let snippet = error_snippet(&cyrillic);
        assert!(snippet.ends_with("[truncated]"));
        assert!(snippet.chars().count() <= MAX_ERROR_SNIPPET_CHARS + "[truncated]".len());
        assert_eq!(
            snippet[..snippet.len() - "[truncated]".len()]
                .chars()
                .count(),
            240
        );
        let ascii = "x".repeat(300);
        assert_eq!(
            error_snippet(&ascii),
            format!("{}[truncated]", "x".repeat(240))
        );
    }
}
