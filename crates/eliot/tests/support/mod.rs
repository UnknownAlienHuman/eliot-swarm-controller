use serde_json::Value;
use std::{
    ffi::OsStr,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};

pub const CLI: &str = env!("CARGO_BIN_EXE_eliot");
const WAIT: Duration = Duration::from_secs(20);

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
    pub fn spawn(command: Command) -> Self {
        Self::spawn_with_stdout(command, None)
    }

    #[allow(dead_code)] // Used by another integration target; support is compiled per target.
    pub fn spawn_stdout_file(command: Command, path: &Path) -> Self {
        Self::spawn_with_stdout(command, Some(path))
    }

    fn spawn_with_stdout(mut command: Command, path: Option<&Path>) -> Self {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        let mut child = command
            .stdin(Stdio::null())
            .stdout(match path {
                Some(path) => Stdio::from(fs::File::create(path).expect("create test stdout file")),
                None => Stdio::piped(),
            })
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
    // Fake-only fixture argv, bounded and without inherited environment values.
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
            retain_data: false,
        }
    }

    pub fn start(&mut self) {
        assert!(self.daemon.is_none());
        let mut data_argument = self.data.as_os_str().to_os_string();
        data_argument.push(std::path::MAIN_SEPARATOR_STR);
        let mut command = Command::new(CLI);
        command
            .arg("--data-dir")
            .arg(data_argument)
            .args(["daemon", "--foreground"]);
        self.daemon = Some(Captured::spawn(command));
        let deadline = Instant::now() + WAIT;
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
                    "foreground daemon exited during startup (status={status:?}); stderr:\n{}{}",
                    &stderr[..end],
                    truncated,
                );
            }
            let response = cli(&self.data, &["status"]);
            if response.status.success() {
                assert_eq!(response.json()["state"], "running");
                let info: Value = serde_json::from_slice(
                    &fs::read(self.data.join("daemon.json")).expect("published daemon info"),
                )
                .expect("daemon info JSON");
                assert_eq!(info["pid"], self.daemon.as_ref().unwrap().child.id());
                return;
            }
            assert!(
                Instant::now() < deadline,
                "daemon readiness deadline: {}",
                response.stderr
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn json(&self, args: &[&str]) -> Value {
        cli(&self.data, args).json()
    }

    #[track_caller]
    pub fn wait_command(&self, id: &str, expected: &str) -> Value {
        let deadline = Instant::now() + WAIT;
        loop {
            let command = self.json(&["cmd", "show", id]);
            let state = command["state"].as_str();
            // Run tests use "terminal" to inspect any retained outcome, including unknown.
            let any_outcome = expected == "terminal"
                && matches!(
                    state,
                    Some("applied" | "rejected" | "failed" | "unknown" | "cancelled")
                );
            if state == Some(expected) || any_outcome {
                return command;
            }
            if !matches!(state, Some("accepted" | "queued" | "dispatched")) {
                let outcome = command.get("outcome").and_then(Value::as_object);
                let workspace_pre_dispatch = outcome.is_some_and(|outcome| {
                    matches!(
                        outcome.get("stage").and_then(Value::as_str),
                        Some("workspace" | "workspace_resume_rejected")
                    ) && outcome.get("native_dispatched").and_then(Value::as_bool) == Some(false)
                });
                let error_code_class = match outcome
                    .and_then(|outcome| outcome.get("code"))
                    .and_then(Value::as_str)
                {
                    Some("bad_state") => "bad_state",
                    Some("native_not_dispatched") => "native_not_dispatched",
                    Some("not_allowed") => "not_allowed",
                    Some("process_custody_unknown") => "process_custody_unknown",
                    Some("profile_error") => "profile_error",
                    Some("source_generation_mismatch") => "source_generation_mismatch",
                    Some("source_integrity") => "source_integrity",
                    Some("stale_generation") => "stale_generation",
                    Some("store_lock_deadline") => "store_lock_deadline",
                    Some("timeout") => "timeout",
                    Some("unknown") => "unknown",
                    Some("unavailable") => "unavailable",
                    Some("unsupported") => "unsupported",
                    Some(_) => "other",
                    None => "none",
                };
                let state_class = match state {
                    Some("accepted") => "accepted",
                    Some("queued") => "queued",
                    Some("dispatched") => "dispatched",
                    Some("applied") => "applied",
                    Some("rejected") => "rejected",
                    Some("failed") => "failed",
                    Some("unknown") => "unknown",
                    Some("cancelled") => "cancelled",
                    Some(_) => "other",
                    None => "missing",
                };
                let generation = command["generation"].as_i64();
                let (event_snapshot_available, dispatch_seen, ready_seen_at_command_generation) =
                    match self.events_if_available() {
                        Some(events) => {
                            let dispatch_seen = events.iter().any(|event| {
                                event["kind"] == "command.dispatched"
                                    && event["data"]["command_id"] == id
                            });
                            let ready_seen = command["target"]
                                .as_str()
                                .zip(generation)
                                .is_some_and(|(agent, generation)| {
                                    events.iter().any(|event| {
                                        event["kind"] == "session.ready"
                                            && event["agent"] == agent
                                            && event["generation"] == generation
                                    })
                                });
                            (true, dispatch_seen, ready_seen)
                        }
                        None => (false, false, false),
                    };
                panic!(
                    "unexpected command state {state_class}; generation={generation:?}; workspace_pre_dispatch={workspace_pre_dispatch}; error_code_class={error_code_class}; event_snapshot_available={event_snapshot_available}; dispatch_seen={dispatch_seen}; ready_seen_at_command_generation={ready_seen_at_command_generation}; private outcome deliberately omitted"
                );
            }
            assert!(
                Instant::now() < deadline,
                "command readback deadline; private outcome deliberately omitted"
            );
            thread::sleep(Duration::from_millis(25));
        }
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

    fn events_if_available(&self) -> Option<Vec<Value>> {
        let mut after = 0;
        let mut events = Vec::new();
        let mut end = None;
        for _ in 0..8 {
            let after_arg = after.to_string();
            let reply = cli(
                &self.data,
                &["events", "--since", &after_arg, "--limit", "1000"],
            );
            if !reply.status.success() {
                return None;
            }
            let page: Value = serde_json::from_str(&reply.stdout).ok()?;
            let page_high_water = page["high_water"].as_i64()?;
            let high_water = *end.get_or_insert(page_high_water);
            for event in page["events"].as_array()? {
                if event["seq"].as_i64()? <= high_water {
                    events.push(event.clone());
                }
            }
            let next = page["next"].as_i64()?;
            if next >= high_water {
                return Some(events);
            }
            if next <= after {
                return None;
            }
            after = next;
        }
        None
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

    pub fn git(&self, args: &[&str]) -> Reply {
        let mut command = Command::new("git");
        command.current_dir(&self.workspace);
        for (name, _) in std::env::vars_os() {
            if name
                .to_string_lossy()
                .to_ascii_uppercase()
                .starts_with("GIT_")
            {
                command.env_remove(name);
            }
        }
        let hooks = self.root.join("hooks");
        fs::create_dir_all(&hooks).expect("empty fixture hooks directory");
        command
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env(
                "GIT_CONFIG_GLOBAL",
                if cfg!(windows) { "NUL" } else { "/dev/null" },
            )
            .env("GIT_TERMINAL_PROMPT", "0");
        command.args([
            "-c",
            &format!("core.hooksPath={}", hooks.display()),
            "-c",
            "core.fsmonitor=false",
            "-c",
            "user.name=EliotTest",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgSign=false",
        ]);
        command.args(args);
        run(command)
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
