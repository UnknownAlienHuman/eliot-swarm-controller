use eliot_core::{Error, FailureOrigin, Result};
use eliot_plugin::sync::LockExt;
use std::process::ExitStatus;
#[cfg(any(windows, target_os = "linux"))]
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

/// Whether an unsuccessful spawn ever returned an OS child. A suspended
/// child's cleanup failure must not be confused with refusal before spawn.
#[derive(Debug)]
pub enum AgentSpawnError {
    NotSpawned(Error),
    Uncertain(Error),
}

impl AgentSpawnError {
    pub fn into_error(self) -> Error {
        match self {
            Self::NotSpawned(error) | Self::Uncertain(error) => error,
        }
    }
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

    /// Observe the Linux direct child exit without reaping its birth-checked
    /// group leader. Call `wait()` only after the retained group is settled.
    #[cfg(target_os = "linux")]
    pub async fn wait_exit_unreaped(&mut self) -> Result<()> {
        loop {
            if self.try_wait_exit_unreaped()? {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Nonblocking, non-reaping Linux exit observation for timeout arbitration.
    #[cfg(target_os = "linux")]
    pub fn try_wait_exit_unreaped(&self) -> Result<bool> {
        let identity = self.identity.ok_or_else(|| {
            Error::new(
                "process_custody_unknown",
                "owned process-group identity is unavailable",
            )
        })?;
        super::unix::process_group_leader_exited_unreaped(&identity)
    }

    /// Wait until the retained exited leader is the only process in its group.
    /// Reaping it is deliberately left to the existing `wait()` method.
    #[cfg(target_os = "linux")]
    pub async fn wait_group_empty_except_leader(&self, timeout: Duration) -> Result<bool> {
        let identity = self.identity.ok_or_else(|| {
            Error::new(
                "process_custody_unknown",
                "owned process-group identity is unavailable",
            )
        })?;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if super::unix::process_group_empty_except_leader(&identity)? {
                return Ok(true);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            tokio::time::sleep((deadline - now).min(Duration::from_millis(50))).await;
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
        #[cfg(windows)]
        {
            // Build the leaf fence first: only its failure means the verified
            // restrictions are unavailable. A later spawn failure keeps the
            // true OS error (for example the working-directory limit) instead
            // of blaming the fence.
            let job = super::windows::Job::new_leaf().map_err(|_| {
                Error::new(
                    "git_leaf_fence_unavailable",
                    "cannot start process with verified leaf restrictions",
                )
            })?;
            Self::spawn_in_job(command, job).map_err(AgentSpawnError::into_error)
        }
        #[cfg(unix)]
        {
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

    /// Start one Agent generation's plugin in custody that a later daemon can
    /// check after replacement: a Job named from the Store custody namespace on
    /// Windows, or a Unix group whose recorded leader identifies it.
    pub fn spawn_agent(
        command: Command,
        namespace: &str,
        agent: &str,
        generation: i64,
    ) -> std::result::Result<Self, AgentSpawnError> {
        #[cfg(windows)]
        {
            let name = agent_custody_name(namespace, agent, generation);
            Self::spawn_in_job(
                command,
                super::windows::Job::new_named_kill_on_close(&name)
                    .map_err(AgentSpawnError::NotSpawned)?,
            )
        }
        #[cfg(not(windows))]
        {
            let _ = (namespace, agent, generation);
            Self::spawn(command).map_err(AgentSpawnError::NotSpawned)
        }
    }

    #[cfg(windows)]
    fn spawn_in_job(
        mut command: Command,
        job: super::windows::Job,
    ) -> std::result::Result<Self, AgentSpawnError> {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};

        command
            .as_std_mut()
            .creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW);
        let mut child = command.spawn().map_err(|error| {
            AgentSpawnError::NotSpawned(io_error("spawn suspended child", error))
        })?;
        let process = match child.raw_handle() {
            Some(process) => process,
            None => {
                let _ = child.start_kill();
                return Err(AgentSpawnError::Uncertain(Error::new(
                    "process_handle_unavailable",
                    "Tokio did not expose the suspended child process handle",
                )));
            }
        };
        job.assign_then_resume(process)
            .map_err(AgentSpawnError::Uncertain)?;
        Ok(Self {
            job,
            child,
            observation: std::sync::Mutex::new(None),
        })
    }

    fn spawn_with_policy(command: Command, leaf: bool) -> Result<Self> {
        #[cfg(windows)]
        {
            let job = if leaf {
                super::windows::Job::new_leaf()?
            } else {
                super::windows::Job::new_kill_on_close()?
            };
            Self::spawn_in_job(command, job).map_err(AgentSpawnError::into_error)
        }

        #[cfg(unix)]
        {
            let _ = leaf;
            let mut command = command;
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

/// What a replacement daemon can establish about an earlier daemon's Agent
/// custody, read from the OS without holding the Store lock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestartCustody {
    /// No custody member can still run; Unix cannot exclude escaped descendants.
    Closed {
        evidence: &'static str,
        orphan_possible: bool,
    },
    /// Closure is not proven, so the reservation stays; names the missing proof.
    Retained { missing: &'static str },
}

#[cfg(windows)]
pub(crate) fn agent_custody_name(namespace: &str, agent: &str, generation: i64) -> String {
    // Global, not session-local: a daemon restarted in another logon session
    // must not mistake a still-live Job for a missing one.
    format!("Global\\eliot-{namespace}-{agent}-{generation}")
}

/// Check the custody of a generation that an earlier daemon started.
pub fn restart_custody(generation: &eliot_store::RetainedGeneration) -> Result<RestartCustody> {
    if generation.processes_truncated {
        return Ok(RestartCustody::Retained {
            missing: "process_readback_truncated",
        });
    }
    #[cfg(windows)]
    {
        let Some(namespace) = generation.custody_namespace.as_deref() else {
            return Ok(RestartCustody::Retained {
                missing: "named_custody_unrecorded",
            });
        };
        let name = agent_custody_name(namespace, &generation.agent, generation.generation);
        if !super::windows::named_job_closed(&name)? {
            return Ok(RestartCustody::Retained {
                missing: "windows_job_live",
            });
        }
        // Closing the Job only started member termination; the boot marker is an
        // estimate, so match recorded identities by PID and exact creation time.
        for process in generation
            .processes
            .iter()
            .filter(|process| !process.exited)
        {
            if super::windows::recorded_process_alive(
                process.identity.pid,
                process.identity.start_time,
            )? {
                return Ok(RestartCustody::Retained {
                    missing: "windows_process_live",
                });
            }
        }
        Ok(RestartCustody::Closed {
            evidence: "windows_job_closed",
            orphan_possible: false,
        })
    }
    #[cfg(target_os = "linux")]
    {
        let boot = super::unix::host_boot_id()?;
        let mut leader = None;
        let mut current_boot = false;
        for process in &generation.processes {
            if process.identity.host_boot_id != boot {
                continue;
            }
            current_boot = true;
            if !process.exited
                && super::unix::recorded_process_alive(
                    process.identity.pid,
                    process.identity.start_time,
                )?
            {
                return Ok(RestartCustody::Retained {
                    missing: "unix_process_live",
                });
            }
            if process.role == "plugin" {
                leader = Some(process.identity.pid);
            }
        }
        if generation.processes.is_empty() {
            return Ok(RestartCustody::Retained {
                missing: "unix_process_identity_unrecorded",
            });
        }
        if !current_boot {
            return Ok(RestartCustody::Closed {
                evidence: "host_rebooted",
                orphan_possible: false,
            });
        }
        let Some(leader) = leader else {
            return Ok(RestartCustody::Retained {
                missing: "unix_group_leader_unrecorded",
            });
        };
        Ok(if super::unix::recorded_process_group_empty(leader)? {
            RestartCustody::Closed {
                evidence: "unix_group_empty",
                orphan_possible: true,
            }
        } else {
            RestartCustody::Retained {
                missing: "unix_group_live",
            }
        })
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = generation;
        Ok(RestartCustody::Retained {
            missing: "custody_unsupported",
        })
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

    #[tokio::test]
    async fn leaf_spawn_preserves_unspawnable_cwd_error() {
        let system_root =
            PathBuf::from(std::env::var_os("SystemRoot").expect("Windows SystemRoot"));
        let parent = system_root.join("System32").join("cmd.exe");
        let base = std::env::temp_dir().join(format!("eliot-leaf-long-{}", ulid::Ulid::new()));
        let _temp_1 = crate::test_support::TempDir::own(base.clone());
        let long = base.join("Q".repeat(240));
        assert!(
            long.as_os_str().len() > 258,
            "fixture must exceed the spawnable cwd"
        );
        std::fs::create_dir_all(&base).expect("create fixture base");
        std::fs::create_dir_all(&long).expect("create fixture cwd");
        let mut command = Command::new(&parent);
        command
            .current_dir(&long)
            .env_clear()
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command.arg("/d").arg("/c").arg("exit /b 0");
        let error = match OwnedChild::spawn_leaf(command) {
            Ok(_) => panic!("over-long cwd cannot spawn"),
            Err(error) => error,
        };
        assert_ne!(
            error.code, "git_leaf_fence_unavailable",
            "the true spawn error must survive, not the fence code: {}",
            error.message
        );
    }
}

fn io_error(action: &str, error: std::io::Error) -> Error {
    // Process-start failures are the OS/machine refusing: a missing binary
    // or denied execution is `environment`, never a secret, parameter or
    // prompt. The action names the gate, never a working directory.
    Error::new("platform_io", format!("{action}: {error}"))
        .with_where("platform.process.spawn")
        .with_origin(
            FailureOrigin::Environment,
            "OS refused the process start (missing binary, denied execution, or machine limit)",
        )
}

#[cfg(all(test, windows))]
mod named_job_tests {
    use super::super::windows;
    use super::{OwnedChild, agent_custody_name};
    use std::{path::PathBuf, process::Stdio, time::Duration};
    use tokio::{process::Command, time::timeout};

    fn sleeper() -> Command {
        let system_root =
            PathBuf::from(std::env::var_os("SystemRoot").expect("Windows SystemRoot"));
        let shell = system_root
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe");
        assert!(shell.is_file(), "fixture shell must exist");
        let mut command = Command::new(&shell);
        command
            .arg("-NoProfile")
            .arg("-Command")
            .arg("Start-Sleep 30")
            .env_clear()
            .env("SystemRoot", &system_root)
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }

    // The missing-entry force-stop path terminates the generation family by
    // its recorded name without holding the OwnedChild handle: a fenced or
    // stuck actor can keep the named Job open after its routing entry is
    // gone, and an absent name must report nothing to terminate.
    #[tokio::test]
    async fn missing_entry_force_stop_terminates_named_job_family() {
        let namespace = format!("test-{}", ulid::Ulid::new());
        let name = agent_custody_name(&namespace, "a9", 7);
        assert!(
            windows::named_job_closed(&name).expect("probe absent job"),
            "fresh test custody name must not exist"
        );
        assert!(
            !windows::terminate_named_generation_job(&name).expect("terminate absent job"),
            "absent custody name terminates nothing"
        );
        let mut child =
            OwnedChild::spawn_agent(sleeper(), &namespace, "a9", 7).expect("spawn named family");
        assert!(
            !windows::named_job_closed(&name).expect("probe live job"),
            "spawned family must hold its named Job open"
        );
        assert!(
            windows::terminate_named_generation_job(&name).expect("terminate named family"),
            "live named Job must terminate and drain"
        );
        assert!(
            windows::named_job_closed(&name).expect("probe terminated job"),
            "terminated family must leave no named Job"
        );
        assert!(
            windows::terminate_named_generation_job(&name).expect("repeat named force"),
            "an empty Job still held by its actor is an idempotent force"
        );
        timeout(Duration::from_secs(10), child.wait())
            .await
            .expect("family deadline")
            .expect("family exit");
        drop(child);
    }
}

#[cfg(all(test, any(windows, target_os = "linux")))]
mod restart_custody_tests {
    use super::{OwnedChild, RestartCustody, restart_custody};
    use eliot_store::RetainedGeneration;
    use std::{process::Stdio, time::Duration};
    use tokio::process::Command;

    fn long_running() -> Command {
        #[cfg(windows)]
        let mut command = {
            let mut command = Command::new("cmd.exe");
            command.args(["/d", "/c", "ping -n 60 127.0.0.1 >NUL"]);
            command
        };
        #[cfg(unix)]
        let mut command = {
            let mut command = Command::new("sleep");
            command.arg("60");
            command
        };
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        command
    }

    async fn eventually_closed(generation: &RetainedGeneration) -> RestartCustody {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let custody = restart_custody(generation).expect("probe custody");
            if matches!(custody, RestartCustody::Closed { .. })
                || tokio::time::Instant::now() >= deadline
            {
                return custody;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn named_agent_job_is_retained_while_live_and_closed_after_its_owner_drops_it() {
        use eliot_core::ProcessIdentity;
        use eliot_store::RecordedProcess;

        let recorded = |pid: u32| RecordedProcess {
            identity: ProcessIdentity {
                host_boot_id: "windows-boot-filetime-estimate:earlier-daemon".to_owned(),
                pid,
                start_time: super::super::windows::process_start_time(pid),
            },
            role: "plugin".to_owned(),
            exited: false,
        };
        let namespace = ulid::Ulid::new().to_string();
        let child = OwnedChild::spawn_agent(long_running(), &namespace, "a1", 3)
            .expect("spawn named Agent custody");
        let generation = RetainedGeneration {
            agent: "a1".to_owned(),
            generation: 3,
            custody_namespace: Some(namespace.clone()),
            processes: vec![recorded(child.child.id().expect("live plugin pid"))],
            processes_truncated: false,
        };
        assert_eq!(
            restart_custody(&generation).expect("probe live custody"),
            RestartCustody::Retained {
                missing: "windows_job_live"
            }
        );
        let duplicate = OwnedChild::spawn_agent(long_running(), &namespace, "a1", 3)
            .err()
            .expect("an existing custody name is never adopted");
        assert_eq!(duplicate.into_error().code, "process_custody_conflict");

        drop(child);
        assert_eq!(
            eventually_closed(&generation).await,
            RestartCustody::Closed {
                evidence: "windows_job_closed",
                orphan_possible: false
            }
        );
        let unnamed = RetainedGeneration {
            custody_namespace: None,
            ..generation.clone()
        };
        assert_eq!(
            restart_custody(&unnamed).expect("probe unnamed custody"),
            RestartCustody::Retained {
                missing: "named_custody_unrecorded"
            }
        );

        // A missing Job name alone is not enough: a recorded process that is
        // still running keeps the reservation.
        let mut outside = long_running();
        let mut outside = outside.spawn().expect("spawn recorded process");
        let still_running = RetainedGeneration {
            processes: vec![recorded(outside.id().expect("live recorded pid"))],
            ..generation
        };
        assert_eq!(
            restart_custody(&still_running).expect("probe recorded process"),
            RestartCustody::Retained {
                missing: "windows_process_live"
            }
        );
        outside.kill().await.expect("stop recorded process");
        assert_eq!(
            restart_custody(&still_running).expect("probe exited process"),
            RestartCustody::Closed {
                evidence: "windows_job_closed",
                orphan_possible: false
            }
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn recorded_unix_leader_is_retained_while_live_and_closed_after_its_group_empties() {
        use eliot_core::ProcessIdentity;
        use eliot_store::RecordedProcess;

        let mut child = OwnedChild::spawn_agent(long_running(), "unused", "a1", 3)
            .expect("spawn Agent process group");
        let pid = child.child.id().expect("live leader pid");
        let leader = RecordedProcess {
            identity: ProcessIdentity {
                host_boot_id: super::super::unix::host_boot_id().expect("boot id"),
                pid,
                start_time: super::super::unix::linux_start_ticks(pid),
            },
            role: "plugin".to_owned(),
            exited: false,
        };
        let generation = RetainedGeneration {
            agent: "a1".to_owned(),
            generation: 3,
            custody_namespace: Some("unused".to_owned()),
            processes: vec![leader.clone()],
            processes_truncated: false,
        };
        assert_eq!(
            restart_custody(&generation).expect("probe live group"),
            RestartCustody::Retained {
                missing: "unix_process_live"
            }
        );

        child.terminate().expect("terminate group");
        child.wait().await.expect("reap leader");
        assert_eq!(
            eventually_closed(&generation).await,
            RestartCustody::Closed {
                evidence: "unix_group_empty",
                orphan_possible: true
            }
        );
        let mut earlier_boot = leader;
        earlier_boot.identity.host_boot_id = "earlier-boot".to_owned();
        assert_eq!(
            restart_custody(&RetainedGeneration {
                processes: vec![earlier_boot],
                ..generation
            })
            .expect("probe earlier boot"),
            RestartCustody::Closed {
                evidence: "host_rebooted",
                orphan_possible: false
            }
        );
    }
}
