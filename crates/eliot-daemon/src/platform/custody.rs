use eliot_core::{Error, Result};
use std::process::ExitStatus;
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
        let mut retained = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    pub async fn wait_family_empty(&mut self, timeout: Duration) -> Result<bool> {
        #[cfg(windows)]
        {
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
        #[cfg(not(windows))]
        {
            let _ = timeout;
            Err(Error::new(
                "unsupported",
                "Unix process-group custody cannot prove exact family exit",
            ))
        }
    }

    /// Start the configured command under a Windows Job or a new Unix group.
    /// Configure Tokio's stdin/stdout/stderr as piped before passing the command.
    pub fn spawn(mut command: Command) -> Result<Self> {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};

            let job = super::windows::Job::new_kill_on_close()?;
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
            let _ = command;
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
        if let Some(observer) = self
            .observation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
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

fn io_error(action: &str, error: std::io::Error) -> Error {
    Error::new("platform_io", format!("{action}: {error}"))
}
