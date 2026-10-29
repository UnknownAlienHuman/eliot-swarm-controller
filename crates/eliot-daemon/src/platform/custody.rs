use eliot_core::{Error, Result};
use eliot_plugin::sync::LockExt;
use std::process::ExitStatus;
#[cfg(windows)]
use std::time::Duration;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};

/// How strongly the operating system contains this child family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CustodyGuarantee {
    /// Windows Job Object assignment occurs before the suspended child resumes.
    #[cfg(windows)]
    Exact,
    /// Unix process groups contain ordinary descendants but can be escaped.
    #[cfg(unix)]
    Degraded,
    /// No exact process identity is available for a safe group signal.
    #[cfg(not(windows))]
    Unknown,
}

/// A process started inside the platform's scoped child custody.
pub struct OwnedChild {
    // Drop the owning Job first: terminate the family and invalidate its shared
    // query handle before Tokio releases the process handle.
    #[cfg(windows)]
    job: super::windows::Job,
    child: Child,
    #[cfg(unix)]
    identity: Option<super::unix::ProcessGroupIdentity>,
    observation: std::sync::Mutex<Option<super::CustodyObserver>>,
}

impl OwnedChild {
    /// Capture observation authority while the exact leader identity is owned.
    pub fn observer(&self) -> Result<super::CustodyObserver> {
        let mut retained = self.observation.lock_recover();
        if let Some(observer) = retained.as_ref() {
            return Ok(observer.clone());
        }
        #[cfg(windows)]
        {
            let pid = self.child.id().ok_or_else(|| {
                Error::new("process_identity", "owned child has no live process id")
            })?;
            let observer = super::CustodyObserver::new(self.job.observer(pid)?);
            *retained = Some(observer.clone());
            Ok(observer)
        }
        #[cfg(unix)]
        {
            let identity = self.identity.ok_or_else(|| {
                #[cfg(target_os = "linux")]
                {
                    Error::new(
                        "process_custody_unknown",
                        "owned process-group identity is unavailable",
                    )
                }
                #[cfg(not(target_os = "linux"))]
                {
                    Error::new(
                        "unsupported",
                        "process observation requires Linux boot and birth identity",
                    )
                }
            })?;
            let observer =
                super::CustodyObserver::new(super::unix::ProcessObserver::new(identity)?);
            *retained = Some(observer.clone());
            Ok(observer)
        }
        #[cfg(not(any(windows, unix)))]
        {
            Err(Error::new(
                "unsupported",
                "process observation is unsupported",
            ))
        }
    }

    /// Separate from leader wait(): only Windows can prove exact family exit.
    /// `false` means the bounded wait expired, never that the family was killed.
    #[cfg(windows)]
    pub async fn wait_family_empty(&mut self, timeout: Duration) -> Result<bool> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.job.is_empty()? {
                return Ok(true);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            tokio::time::sleep((deadline - now).min(Duration::from_millis(10))).await;
        }
    }

    /// Start the configured command under a Windows Job or a new Unix group.
    /// Configure Tokio's stdin/stdout/stderr as piped before passing the command.
    pub fn spawn(command: Command) -> Result<Self> {
        Self::spawn_with_policy(command, false)
    }

    /// Start a read-only Git leaf with child-process creation fenced before exec.
    /// The caller must select the actual executable, not a launcher requiring children.
    pub fn spawn_leaf(command: Command) -> Result<Self> {
        #[cfg(any(windows, unix))]
        {
            #[cfg(unix)]
            let command = super::unix::prepare_leaf_command(command)?;
            Self::spawn_with_policy(command, true).map_err(|_| {
                Error::new(
                    "git_leaf_fence_unavailable",
                    "cannot start process with verified leaf restrictions",
                )
            })
        }
        #[cfg(not(any(windows, unix)))]
        {
            let _ = command;
            Err(Error::new(
                "git_leaf_fence_unavailable",
                "leaf process restrictions are not verified on this platform",
            ))
        }
    }

    fn spawn_with_policy(mut command: Command, leaf: bool) -> Result<Self> {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};

            let job = if leaf {
                super::windows::Job::new_leaf()?
            } else {
                super::windows::Job::new_kill_on_close()?
            };
            command
                .as_std_mut()
                .creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW);
            let mut child = command
                .spawn()
                .map_err(|error| io_error("spawn suspended child", error))?;
            let process = match child.raw_handle() {
                Some(process) => process,
                None => {
                    let _ = child.start_kill();
                    return Err(Error::new(
                        "process_handle_unavailable",
                        "Tokio did not expose the suspended child process handle",
                    ));
                }
            };
            job.assign_then_resume(process)?;
            Ok(Self {
                job,
                child,
                observation: std::sync::Mutex::new(None),
            })
        }

        #[cfg(unix)]
        {
            let _ = leaf;
            command.process_group(0);
            let child = command
                .spawn()
                .map_err(|error| io_error("spawn process-group child", error))?;
            let identity = child
                .id()
                .and_then(|pid| super::unix::capture_process_group_identity(pid).ok());
            Ok(Self {
                child,
                identity,
                observation: std::sync::Mutex::new(None),
            })
        }

        #[cfg(not(any(windows, unix)))]
        {
            let _ = (command, leaf);
            Err(Error::new(
                "platform_unsupported",
                "owned child custody is implemented for Windows and Unix",
            ))
        }
    }

    pub fn guarantee(&self) -> CustodyGuarantee {
        #[cfg(windows)]
        {
            CustodyGuarantee::Exact
        }
        #[cfg(unix)]
        {
            if self.identity.is_some() {
                CustodyGuarantee::Degraded
            } else {
                CustodyGuarantee::Unknown
            }
        }
        #[cfg(not(any(windows, unix)))]
        {
            CustodyGuarantee::Unknown
        }
    }

    /// Terminate the exact Windows Job or best-effort Unix process group.
    pub fn terminate(&self) -> Result<()> {
        #[cfg(windows)]
        {
            self.job.terminate()
        }
        #[cfg(unix)]
        {
            match self.identity.as_ref() {
                Some(identity) => super::unix::terminate_process_group(identity),
                None => Err(Error::new(
                    "process_custody_unknown",
                    "spawned child birth identity is unavailable; no signal was sent",
                )),
            }
        }
        #[cfg(not(any(windows, unix)))]
        {
            Err(Error::new(
                "platform_unsupported",
                "owned child termination is unsupported on this platform",
            ))
        }
    }

    pub async fn wait(&mut self) -> Result<ExitStatus> {
        let status = self
            .child
            .wait()
            .await
            .map_err(|error| io_error("wait for owned child", error))?;
        if let Some(observer) = self.observation.lock_recover().as_ref() {
            observer.record_root_exit(status.code());
        }
        #[cfg(unix)]
        {
            // wait reaps the leader, after which its numeric PID/PGID may be
            // reused. Descendant custody remains degraded/unknown from here.
            self.identity = None;
        }
        Ok(status)
    }

    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }
}

#[cfg(unix)]
impl Drop for OwnedChild {
    fn drop(&mut self) {
        // Best effort only: a child can escape its process group with setsid,
        // and Unix does not kill the group automatically when this owner exits.
        // Missing or stale identity is deliberately not converted to a PGID-only
        // kill; Drop cannot report its error, so guarantee() remains non-clean.
        if let Some(identity) = self.identity.as_ref() {
            let _ = super::unix::terminate_process_group(identity);
        }
    }
}

#[cfg(all(test, windows))]
mod leaf_tests {
    use super::OwnedChild;
    use std::{fs, path::PathBuf, process::ExitStatus, process::Stdio, time::Duration};
    use tokio::{process::Command, time::timeout};

    struct FreshTemp(PathBuf);

    impl Drop for FreshTemp {
        fn drop(&mut self) {
            // Only the fixed files created by this test; never recurse.
            let _ = fs::remove_file(self.0.join("attempt.cmd"));
            let _ = fs::remove_file(self.0.join("child.out"));
            let _ = fs::remove_dir(&self.0);
        }
    }

    async fn finish(mut child: OwnedChild) -> ExitStatus {
        let status = timeout(Duration::from_secs(5), child.wait())
            .await
            .expect("owned process deadline")
            .expect("owned process exit");
        assert!(
            child
                .wait_family_empty(Duration::from_secs(5))
                .await
                .expect("owned family query"),
            "owned family did not exit"
        );
        status
    }

    #[tokio::test]
    async fn leaf_process_runs_but_cannot_start_existing_absolute_child() {
        let system_root =
            PathBuf::from(std::env::var_os("SystemRoot").expect("Windows SystemRoot"));
        assert!(system_root.is_absolute(), "SystemRoot must be absolute");
        let parent = system_root.join("System32").join("cmd.exe");
        let child_program = system_root.join("System32").join("whoami.exe");
        assert!(
            parent.is_file(),
            "known absolute parent executable must exist"
        );
        assert!(
            child_program.is_file(),
            "known absolute child executable must exist"
        );

        let temp =
            FreshTemp(std::env::temp_dir().join(format!("eliot-leaf-fence-{}", ulid::Ulid::new())));
        fs::create_dir(&temp.0).expect("create fresh test directory");
        let command = || {
            let mut command = Command::new(&parent);
            command
                .current_dir(&temp.0)
                .env_clear()
                .env("SystemRoot", &system_root)
                .kill_on_drop(true)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            command.arg("/d").arg("/c");
            command
        };

        let mut basic = command();
        basic.arg("exit /b 0");
        assert!(
            finish(OwnedChild::spawn_leaf(basic).expect("spawn leaf parent"))
                .await
                .success(),
            "the leaf parent itself must run"
        );

        fs::write(
            temp.0.join("attempt.cmd"),
            format!(
                "@echo off\n\"{}\" > child.out 2>nul\nif errorlevel 1 exit /b 0\nexit /b 23\n",
                child_program.display(),
            ),
        )
        .expect("write child-attempt script");

        let mut control = command();
        control.arg("attempt.cmd");
        assert_eq!(
            finish(OwnedChild::spawn(control).expect("spawn normal control"))
                .await
                .code(),
            Some(23)
        );
        assert!(
            !fs::read(temp.0.join("child.out"))
                .expect("read control output")
                .is_empty(),
            "the existing child must actually execute without the leaf restriction"
        );
        fs::remove_file(temp.0.join("child.out")).expect("remove control output");

        let mut leaf = command();
        leaf.arg("attempt.cmd");
        assert_eq!(
            finish(OwnedChild::spawn_leaf(leaf).expect("spawn fenced parent"))
                .await
                .code(),
            Some(0)
        );
        assert!(
            fs::read(temp.0.join("child.out"))
                .expect("read fenced output")
                .is_empty(),
            "the known absolute child must not execute in the leaf Job"
        );
    }
}

fn io_error(action: &str, error: std::io::Error) -> Error {
    Error::new("platform_io", format!("{action}: {error}"))
}
