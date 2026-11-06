use serde_json::Value;
use std::{
    ffi::{OsStr, OsString},
    fs,
    io::{self, Read},
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

pub struct Fixture {
    pub root: PathBuf,
    pub data: PathBuf,
    pub workspace: PathBuf,
    pub retain_data: bool,
    daemon: Option<Captured>,
    daemon_env: Vec<(OsString, OsString)>,
    starts: u32,
}

impl Fixture {
    pub fn new(label: &str) -> Self {
        let root = std::env::temp_dir().join(format!("eliot {label} Проба {}", ulid::Ulid::new()));
        fs::create_dir(&root).expect("create unique test root");
        let workspace = root.join("repo space");
        fs::create_dir(&workspace).expect("create fixture workspace");
        Self {
            data: root.join("data space"),
            workspace,
            root,
            daemon: None,
            daemon_env: Vec::new(),
            retain_data: false,
            starts: 0,
        }
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
        self.daemon = Some(Captured::spawn(command));
        let child_id = self.daemon.as_ref().unwrap().child.id();
        let started = Instant::now();
        let deadline = started + WAIT;
        let mut polls = 0u32;
        let mut first_error: Option<String> = None;
        loop {
            if let Some(status) = self
                .daemon
                .as_mut()
                .unwrap()
                .child
                .try_wait()
                .expect("daemon status")
            {
                let (_, stderr) = self.daemon.as_ref().unwrap().output(Duration::from_secs(2));
                let mut end = stderr.len().min(16 * 1024);
                while !stderr.is_char_boundary(end) {
                    end -= 1;
                }
                let truncated = if end < stderr.len() {
                    "\n[stderr truncated at 16384 bytes]"
                } else {
                    ""
                };
                panic!(
                    "foreground daemon exited during startup (status={status:?}, per_process currently_starting={}, per_process peak_starting={}); stderr:\n{}{}",
                    CURRENTLY_STARTING_DAEMONS_PER_PROCESS.load(Ordering::SeqCst),
                    PEAK_STARTING_DAEMONS_PER_PROCESS.load(Ordering::SeqCst),
                    &stderr[..end],
                    truncated,
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
                "daemon readiness deadline (start #{}, {polls} status polls in {:?}, per_process currently_starting={}, per_process peak_starting={}): child={}; daemon.json={}; first_error={}; last_error={}; log={}",
                self.starts,
                started.elapsed(),
                CURRENTLY_STARTING_DAEMONS_PER_PROCESS.load(Ordering::SeqCst),
                PEAK_STARTING_DAEMONS_PER_PROCESS.load(Ordering::SeqCst),
                match self.daemon.as_mut().unwrap().child.try_wait() {
                    Ok(None) => "alive".to_owned(),
                    Ok(Some(status)) => format!("exited({status:?})"),
                    Err(_) => "wait_error".to_owned(),
                },
                daemon_info_snippet(&self.data),
                first_error.as_deref().unwrap_or("none"),
                error_snippet(&response.stderr),
                daemon_log_phases(&self.data),
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
                .wait(WAIT)
                .expect("owned daemon shutdown deadline")
                .success(),
            "owned daemon returned nonzero"
        );
        let (_, stderr) = daemon.output(Duration::from_secs(2));
        assert!(!stderr.contains("panicked"), "daemon panic: {stderr}");
        self.daemon = None;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if self.retain_data {
            // A background starter can fail before metadata is published. Only
            // attempt authenticated IPC; do not adopt a PID or claim OS exit.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                cli(&self.data, &["daemon", "stop", "--force"])
            }));
            eprintln!(
                "unconfirmed background fixture retained at {}",
                self.root.display()
            );
            return;
        }
        if let Some(mut daemon) = self.daemon.take() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                cli(&self.data, &["daemon", "stop", "--force"])
            }));
            if daemon.wait(WAIT).is_err() {
                let _ = daemon.child.kill();
                if daemon.wait(Duration::from_secs(5)).is_err() {
                    return;
                }
            }
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
