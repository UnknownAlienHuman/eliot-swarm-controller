use eliot_core::{Error, FailureOrigin, Result};
use eliot_plugin::sync::LockExt;
use std::process::ExitStatus;
use std::sync::{Arc, Mutex, Weak};
#[cfg(any(windows, target_os = "linux"))]
use std::time::Duration;
#[expect(
    clippy::disallowed_types,
    reason = "The custody module accepts Tokio commands and owns their platform process families."
)]
use tokio::process::Command;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::Notify;

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
    // Keep membership discoverable until the owning Job or group is released.
    local_custody: Option<Arc<LiveAgentCustody>>,
}

#[cfg(target_os = "linux")]
pub(crate) use super::unix::LocalPeer;
#[cfg(target_os = "linux")]
use super::unix::ProcessGroupIdentity as Membership;
#[cfg(windows)]
use super::windows::JobMembership as Membership;
#[cfg(windows)]
pub(crate) use super::windows::LocalPeer;
#[cfg(not(any(windows, target_os = "linux")))]
pub(crate) struct LocalPeer;
#[cfg(not(any(windows, target_os = "linux")))]
type Membership = ();

struct LiveAgentCustody {
    agent: String,
    membership: Membership,
}

/// A daemon-local view of owned Agent generations, independent of sampled
/// process records, control credentials, and other daemons' Jobs or groups.
#[derive(Default)]
pub(crate) struct AgentCustodies {
    state: Mutex<CustodyState>,
    started: Notify,
}

#[derive(Default)]
struct CustodyState {
    starting: usize,
    agents: Vec<Weak<LiveAgentCustody>>,
}

struct CustodyStart<'a>(&'a AgentCustodies);

impl Drop for CustodyStart<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock_recover();
        state.starting -= 1;
        if state.starting == 0 {
            self.0.started.notify_waiters();
        }
    }
}

impl AgentCustodies {
    #[cfg(target_os = "linux")]
    pub(crate) fn restart_groups(&self) -> Vec<super::unix::ProcessGroupIdentity> {
        self.state
            .lock_recover()
            .agents
            .iter()
            .filter_map(Weak::upgrade)
            .map(|agent| agent.membership)
            .collect()
    }

    pub(crate) fn spawn(
        &self,
        agent: &str,
        spawn: impl FnOnce() -> std::result::Result<OwnedChild, AgentSpawnError>,
    ) -> std::result::Result<OwnedChild, AgentSpawnError> {
        // A peer accepted while spawn is in flight must wait for registration.
        // Keep only that publication fence while creating the process.
        self.state.lock_recover().starting += 1;
        let _start = CustodyStart(self);
        let mut child = spawn()?;
        #[cfg(windows)]
        let membership = child.job.membership();
        #[cfg(target_os = "linux")]
        let membership = child.identity.ok_or_else(|| {
            Error::new(
                "process_custody_unknown",
                "cannot read the Agent plugin's process-group identity",
            )
        });
        #[cfg(not(any(windows, target_os = "linux")))]
        let membership = Ok(());
        let membership = match membership {
            Ok(membership) => membership,
            Err(error) => {
                // The plugin has not been initialized. Stop the direct child
                // and drop its custody, while retaining startup uncertainty.
                let _ = child.child.start_kill();
                return Err(AgentSpawnError::Uncertain(
                    error.with_where("swarm.start.agent_custody"),
                ));
            }
        };
        let custody = Arc::new(LiveAgentCustody {
            agent: agent.to_owned(),
            membership,
        });
        {
            let mut state = self.state.lock_recover();
            state.agents.retain(|agent| agent.strong_count() != 0);
            state.agents.push(Arc::downgrade(&custody));
        }
        child.local_custody = Some(custody);
        Ok(child)
    }

    pub(crate) async fn authorize_owner(
        &self,
        peer: Option<LocalPeer>,
    ) -> Result<Option<LocalPeer>> {
        let agents = loop {
            let registered = self.started.notified();
            let snapshot = {
                let mut state = self.state.lock_recover();
                (state.starting == 0).then(|| {
                    state.agents.retain(|agent| agent.strong_count() != 0);
                    state
                        .agents
                        .iter()
                        .filter_map(Weak::upgrade)
                        .collect::<Vec<_>>()
                })
            };
            if let Some(snapshot) = snapshot {
                break snapshot;
            }
            registered.await;
        };

        #[cfg(windows)]
        if agents.is_empty() {
            return Ok(peer);
        }
        let peer = peer.ok_or_else(owner_peer_unknown)?;

        #[cfg(any(windows, target_os = "linux"))]
        {
            let process = peer
                .inspect(!agents.is_empty())
                .map_err(|_| owner_peer_unknown())?;
            let mut unknown = false;
            for agent in agents {
                match agent.membership.contains(&process) {
                    Ok(true) => {
                        return Err(Error::new(
                            "not_allowed",
                            format!(
                                "This client runs inside Agent {}; use the Agent-scoped Swarm tool.",
                                agent.agent
                            ),
                        ));
                    }
                    Ok(false) => {}
                    Err(_) => unknown = true,
                }
            }
            if unknown || process.verify().is_err() {
                return Err(owner_peer_unknown());
            }
            #[cfg(target_os = "linux")]
            let process = process.into_peer();
            Ok(Some(process))
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        {
            let _ = (agents, peer);
            Err(owner_peer_unknown())
        }
    }
}

fn owner_peer_unknown() -> Error {
    Error::new(
        "not_allowed",
        "The daemon could not verify this local client process; Owner access was refused.",
    )
}

/// Whether an unsuccessful spawn ever returned an OS child. A suspended
/// child's cleanup failure must not be confused with refusal before spawn.
#[derive(Debug)]
pub enum AgentSpawnError {
    NotSpawned(Error),
    ProgramUnrunnable(Error),
    Uncertain(Error),
}

impl AgentSpawnError {
    pub fn into_error(self) -> Error {
        match self {
            Self::NotSpawned(error) | Self::ProgramUnrunnable(error) | Self::Uncertain(error) => {
                error
            }
        }
    }
}

impl OwnedChild {
    /// The owned root process ID while it is unreaped.
    pub(crate) fn pid(&self) -> Option<u32> {
        self.child.id()
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn integration_root_identity(&self) -> Option<eliot_core::ProcessIdentity> {
        let group = self.identity?;
        Some(eliot_core::ProcessIdentity {
            pid: group.pid,
            host_boot_id: super::unix::host_boot_id().ok()?,
            start_time: group.start_ticks,
        })
    }

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

    /// Nonblocking root-exit observation for select arbitration: true only
    /// when the leader exit was already observed. Linux never reaps here so
    /// the birth-checked group drain still runs before the reap; elsewhere
    /// the reaped status is cached by the child handle.
    pub fn try_observe_root_exit(&mut self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.try_wait_exit_unreaped().unwrap_or(false)
        }
        #[cfg(not(target_os = "linux"))]
        {
            self.child
                .try_wait()
                .map(|status| status.is_some())
                .unwrap_or(false)
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) fn try_root_exit_if_finished(&mut self) -> Option<eliot_core::ProcessExit> {
        let status = self.child.try_wait().ok()??;
        if let Some(observer) = self.observation.lock_recover().as_ref() {
            observer.record_root_exit(status.code());
        }
        Some(eliot_core::ProcessExit::from_code(status.code()))
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
            #[expect(
                clippy::disallowed_methods,
                reason = "This bounded wait is owned by the surrounding operation."
            )]
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
            #[expect(
                clippy::disallowed_methods,
                reason = "This bounded wait is owned by the surrounding operation."
            )]
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
            #[expect(
                clippy::disallowed_methods,
                reason = "This bounded wait is owned by the surrounding operation."
            )]
            tokio::time::sleep((deadline - now).min(Duration::from_millis(50))).await;
        }
    }

    /// Start the configured command under a Windows Job or a new Unix group.
    /// Configure Tokio's stdin/stdout/stderr as piped before passing the command.
    #[expect(
        clippy::disallowed_types,
        reason = "This entry point moves a configured child command into platform custody."
    )]
    pub fn spawn(command: Command) -> Result<Self> {
        Self::spawn_report(command).map_err(AgentSpawnError::into_error)
    }

    /// Preserve whether the OS refused to start the process or setup failed
    /// after a child had already been created.
    #[expect(
        clippy::disallowed_types,
        reason = "This entry point preserves spawn versus post-creation custody outcomes."
    )]
    pub(crate) fn spawn_report(command: Command) -> std::result::Result<Self, AgentSpawnError> {
        Self::spawn_with_policy(command, false)
    }

    /// Start a read-only Git leaf with child-process creation fenced before exec.
    /// The caller must select the actual executable, not a launcher requiring children.
    #[expect(
        clippy::disallowed_types,
        reason = "This entry point applies leaf restrictions before admitting a Git child."
    )]
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

    #[cfg(windows)]
    #[expect(
        clippy::disallowed_types,
        reason = "Restore records its exact suspended Git leaf before it can write."
    )]
    pub(crate) fn spawn_checkpoint_restore_writer(
        command: Command,
        id: &str,
        boot: String,
        record: impl FnOnce(&eliot_core::ProcessIdentity) -> Result<()>,
    ) -> std::result::Result<Self, AgentSpawnError> {
        let job = super::windows::Job::new_named_leaf(&checkpoint_restore_job_name(id))
            .map_err(AgentSpawnError::NotSpawned)?;
        Self::spawn_in_job_checked(command, job, |child| {
            let pid = child.id().ok_or_else(|| {
                Error::new(
                    "process_identity",
                    "suspended restore writer has no process id",
                )
            })?;
            record(&eliot_core::ProcessIdentity {
                pid,
                host_boot_id: boot,
                start_time: super::windows::process_start_time(pid)?,
            })
        })
    }

    /// Start one Agent generation's plugin in custody that a later daemon can
    /// check after replacement: a Job named from the Store custody namespace on
    /// Windows, or a Unix group whose recorded leader identifies it.
    #[expect(
        clippy::disallowed_types,
        reason = "This entry point binds the plugin child to the Agent generation's custody."
    )]
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
    #[expect(
        clippy::disallowed_types,
        reason = "This Windows helper starts a child inside the selected Job object."
    )]
    fn spawn_in_job(
        command: Command,
        job: super::windows::Job,
    ) -> std::result::Result<Self, AgentSpawnError> {
        Self::spawn_in_job_checked(command, job, |_| Ok(()))
    }

    #[cfg(windows)]
    #[expect(
        clippy::disallowed_types,
        reason = "This Windows entry point records identity for a named integration child Job."
    )]
    pub(crate) fn spawn_integration(
        command: Command,
        scratch_id: &str,
        record: impl FnOnce(&eliot_core::ProcessIdentity) -> Result<()>,
    ) -> std::result::Result<Self, AgentSpawnError> {
        let name = agent_custody_name("integration", scratch_id, 1);
        let job = super::windows::Job::new_named_kill_on_close(&name)
            .map_err(AgentSpawnError::NotSpawned)?;
        Self::spawn_in_job_checked(command, job, |child| {
            let pid = child.id().ok_or_else(|| {
                Error::new(
                    "process_identity",
                    "suspended integration child has no process id",
                )
            })?;
            record(&eliot_core::ProcessIdentity {
                pid,
                host_boot_id: super::windows::host_boot_id()?,
                start_time: super::windows::process_start_time(pid)?,
            })
        })
    }

    #[cfg(windows)]
    #[expect(
        clippy::disallowed_types,
        reason = "This Windows helper assigns the suspended child before resuming it."
    )]
    fn spawn_in_job_checked(
        mut command: Command,
        job: super::windows::Job,
        before_resume: impl FnOnce(&Child) -> Result<()>,
    ) -> std::result::Result<Self, AgentSpawnError> {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};

        command
            .as_std_mut()
            .creation_flags(CREATE_SUSPENDED | CREATE_NO_WINDOW);
        let mut child = command
            .spawn()
            .map_err(|error| spawn_io_error("spawn suspended child", error))?;
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
        job.assign_then_resume(process, || before_resume(&child))
            .map_err(AgentSpawnError::Uncertain)?;
        Ok(Self {
            job,
            child,
            observation: std::sync::Mutex::new(None),
            local_custody: None,
        })
    }

    #[expect(
        clippy::disallowed_types,
        reason = "This platform helper selects the custody policy for the configured child."
    )]
    fn spawn_with_policy(
        command: Command,
        leaf: bool,
    ) -> std::result::Result<Self, AgentSpawnError> {
        #[cfg(windows)]
        {
            let job = if leaf {
                super::windows::Job::new_leaf()
            } else {
                super::windows::Job::new_kill_on_close()
            }
            .map_err(AgentSpawnError::NotSpawned)?;
            Self::spawn_in_job(command, job)
        }

        #[cfg(unix)]
        {
            let _ = leaf;
            let mut command = command;
            command.process_group(0);
            let child = command
                .spawn()
                .map_err(|error| spawn_io_error("spawn process-group child", error))?;
            let identity = child
                .id()
                .and_then(|pid| super::unix::capture_process_group_identity(pid).ok());
            Ok(Self {
                child,
                identity,
                observation: std::sync::Mutex::new(None),
                local_custody: None,
            })
        }

        #[cfg(not(any(windows, unix)))]
        {
            let _ = (command, leaf);
            Err(AgentSpawnError::NotSpawned(Error::new(
                "platform_unsupported",
                "owned child custody is implemented for Windows and Unix",
            )))
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

    /// Bounded reap for the spontaneous-exit path. A failed exit observation
    /// can leave the child live (`/proc` loss also defeats birth-checked
    /// termination), so an unbounded `wait()` would hang the actor before
    /// source settlement. A timeout reports `unknown` so the caller settles
    /// through the unknown-custody path instead of waiting indefinitely.
    #[cfg(target_os = "linux")]
    pub async fn wait_bounded(&mut self, timeout: Duration) -> Result<ExitStatus> {
        match tokio::time::timeout(timeout, self.wait()).await {
            Ok(status) => status,
            Err(_) => Err(Error::new("unknown", "bounded owned-child reap timed out")),
        }
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

#[cfg(windows)]
pub(crate) fn checkpoint_restore_job_name(id: &str) -> String {
    agent_custody_name("checkpoint-restore", id, 1)
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

#[cfg(any(windows, unix))]
fn spawn_io_error(action: &str, error: std::io::Error) -> AgentSpawnError {
    #[cfg(windows)]
    let invalid_image = {
        use windows_sys::Win32::Foundation::{
            ERROR_BAD_EXE_FORMAT, ERROR_BAD_FORMAT, ERROR_EXE_MACHINE_TYPE_MISMATCH,
        };
        matches!(
            error.raw_os_error().map(|code| code as u32),
            Some(ERROR_BAD_EXE_FORMAT | ERROR_BAD_FORMAT | ERROR_EXE_MACHINE_TYPE_MISMATCH)
        )
    };
    #[cfg(unix)]
    let invalid_image = error.raw_os_error() == Some(libc::ENOEXEC);

    let unrunnable = error.kind() == std::io::ErrorKind::NotFound || invalid_image;
    let error = io_error(action, error);
    if unrunnable {
        AgentSpawnError::ProgramUnrunnable(error)
    } else {
        AgentSpawnError::NotSpawned(error)
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
