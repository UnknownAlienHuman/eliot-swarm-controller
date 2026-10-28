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
    stdout: Receiver<io::Result<String>>,
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
        let stdout = capture(child.stdout.take().expect("stdout pipe"));
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
        let stdout = self
            .stdout
            .recv_timeout(timeout)
            .expect("stdout EOF deadline")
            .expect("read stdout");
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
            assert!(
                self.daemon
                    .as_mut()
                    .unwrap()
                    .child
                    .try_wait()
                    .expect("daemon status")
                    .is_none(),
                "foreground daemon exited during startup"
            );
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

    pub fn wait_command(&self, id: &str, expected: &str) -> Value {
        let deadline = Instant::now() + WAIT;
        loop {
            let command = self.json(&["cmd", "show", id]);
            if command["state"] == expected {
                return command;
            }
            assert!(
                matches!(
                    command["state"].as_str(),
                    Some("accepted" | "queued" | "dispatched")
                ),
                "unexpected command state {}; private outcome deliberately omitted",
                command["state"]
            );
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
