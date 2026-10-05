use std::{env, path::PathBuf, process::Stdio};

use serde_json::Value;
use swarm_contracts::error::{Error, Result};
use swarm_process::{
    module_child_belongs_to_owner, module_owner::verify_current_adapter, process_image_identity,
};
use tokio::process::{Child, Command};

use crate::launch::NativeLaunchSpec;

/// Owner data read from the exact private record published by the manager's
/// module launcher. It is not launch authority until the authenticated
/// `module.hello` succeeds.
pub struct CandidateManagedOwner {
    record: Value,
    token: String,
    adapter_image: Value,
}

impl CandidateManagedOwner {
    pub fn from_environment() -> Result<Self> {
        let state_dir = env::var_os("ELIOT_SWARM_MODULE_STATE")
            .map(PathBuf::from)
            .ok_or_else(|| Error::invalid("MANAGED_OWNER_REQUIRED"))?;
        let owner_file = env::var_os("ELIOT_SWARM_MODULE_OWNER")
            .map(PathBuf::from)
            .ok_or_else(|| Error::invalid("MANAGED_OWNER_REQUIRED"))?;
        if !state_dir.is_absolute() || !owner_file.is_absolute() {
            return Err(Error::invalid("INVALID_MODULE_OWNER_PATH"));
        }
        let canonical_state = state_dir
            .canonicalize()
            .map_err(|_| Error::invalid("INVALID_MODULE_OWNER_PATH"))?;
        let canonical_owner = owner_file
            .canonicalize()
            .map_err(|_| Error::invalid("INVALID_MODULE_OWNER_PATH"))?;
        if canonical_owner != canonical_state.join("owner.json") {
            return Err(Error::invalid("INVALID_MODULE_OWNER_PATH"));
        }

        // Validate the current adapter as a distinct live member of this exact
        // published owner group. The helper captures the current process image
        // and checks Windows Job/Linux process-group membership; it does not
        // treat the owner PID as the adapter or prove family departure.
        let proof = verify_current_adapter(&canonical_owner)?;
        let token = validate_owner_record(&proof.owner_record)?.to_owned();
        if token != proof.boot_id {
            return Err(Error::invalid("MANAGED_MODULE_OWNER_IDENTITY_INVALID"));
        }
        Ok(Self {
            record: proof.owner_record,
            token,
            adapter_image: proof.process_image_identity,
        })
    }

    pub fn clone_for_reconnect(&self) -> Self {
        Self {
            record: self.record.clone(),
            token: self.token.clone(),
            adapter_image: self.adapter_image.clone(),
        }
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn record(&self) -> &Value {
        &self.record
    }

    pub fn adapter_image(&self) -> &Value {
        &self.adapter_image
    }
}

/// Evidence that `module.hello` accepted the same live non-killing module-owner
/// record which launched this adapter. The record is retained so a spawner
/// cannot be constructed from an unverified PID or check-purpose identity.
/// `Group::children_empty` remains the manager supervisor's family-level
/// departure proof after this adapter exits.
pub struct VerifiedManagedOwner {
    _record: Value,
    _adapter_image: Value,
    token: String,
    native_launch_allowed: bool,
}

impl VerifiedManagedOwner {
    /// Called only after the authenticated manager accepted `module.hello`.
    /// This checks the same owner envelope the manager verifies against its
    /// live process group before it records a module boot.
    pub(crate) fn accepted_module_hello(
        boot_id: &str,
        record: Value,
        adapter_image: Value,
        recovery_required: bool,
    ) -> Result<Self> {
        let token = validate_owner_record(&record)?.to_owned();
        if token != boot_id {
            return Err(Error::invalid("MANAGED_MODULE_OWNER_IDENTITY_INVALID"));
        }
        Ok(Self {
            _record: record,
            _adapter_image: adapter_image,
            token,
            native_launch_allowed: !recovery_required,
        })
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn native_launch_allowed(&self) -> bool {
        self.native_launch_allowed
    }
}

fn validate_owner_record(record: &Value) -> Result<&str> {
    let encoded_len = serde_json::to_vec(record)
        .map_err(|_| Error::invalid("MANAGED_MODULE_OWNER_IDENTITY_INVALID"))?
        .len();
    let token = record
        .get("token")
        .and_then(Value::as_str)
        .filter(|value| valid_owner_token(value))
        .ok_or_else(|| Error::invalid("MANAGED_OWNER_TOKEN_REQUIRED"))?;
    let process = record
        .get("process")
        .filter(|value| value.is_object())
        .ok_or_else(|| Error::invalid("MANAGED_OWNER_PROCESS_REQUIRED"))?;
    let valid_scope = matches!(
        process.get("scope").and_then(Value::as_str),
        Some("windows_job" | "linux_process_group")
    );
    let has_process_identity = process
        .get("pid")
        .and_then(Value::as_u64)
        .is_some_and(|pid| pid > 0);
    if record.get("version").and_then(Value::as_u64) != Some(1)
        || encoded_len > 65_536
        || process.get("purpose").and_then(Value::as_str) != Some("module")
        || !valid_scope
        || !has_process_identity
    {
        return Err(Error::invalid("MANAGED_MODULE_OWNER_IDENTITY_INVALID"));
    }
    Ok(token)
}

fn valid_owner_token(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
}

/// Launches the fixed Antigravity CLI directly after authenticated module
/// ownership has been accepted. Because the module runner starts this adapter
/// in its non-killing group/job and this child is spawned directly without
/// breakaway flags, it inherits that same owner. `Child::wait` below proves
/// only this direct child's exit; family-level departure belongs to the
/// manager-owned `Group::children_empty` check after adapter exit.
pub struct OwnedNativeSpawner {
    _owner: VerifiedManagedOwner,
}

impl OwnedNativeSpawner {
    pub fn new(owner: VerifiedManagedOwner) -> Self {
        Self { _owner: owner }
    }

    pub fn spawn_owned(&self, spec: NativeLaunchSpec) -> Result<OwnedNativeChild> {
        if !self._owner.native_launch_allowed {
            return Err(Error::new(
                "RECOVERY_REQUIRED",
                "manager requires reconciliation before a native process may start",
            ));
        }
        if spec.shell
            || spec.kill_on_drop
            || !spec.executable.is_absolute()
            || !spec.cwd.is_absolute()
        {
            return Err(Error::invalid(
                "native launch spec violates the owned-process boundary",
            ));
        }
        if !spec.stdin_piped || !spec.stdout_piped || !spec.stderr_piped {
            return Err(Error::invalid(
                "native launch requires piped standard streams",
            ));
        }

        let mut command = Command::new(&spec.executable);
        command
            .args(&spec.args)
            .current_dir(&spec.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(false);
        let child = command.spawn().map_err(|_| {
            Error::new(
                "NATIVE_SPAWN_FAILED",
                "native executable could not be started",
            )
        })?;
        let owner_record = self._owner._record.clone();
        let expected_executable = spec.executable.clone();
        let (pid, process_instance, membership_error_code) = match child.id() {
            Some(pid) => match process_image_identity(pid) {
                Ok(identity) => {
                    let guard = NativeMembershipGuard {
                        owner_record: owner_record.clone(),
                        pid,
                        expected_executable: expected_executable.clone(),
                        verified_at_spawn: true,
                    };
                    match guard.verify_image(&identity) {
                        Ok(()) => (Some(pid), Some(identity), None),
                        Err(error) => (Some(pid), Some(identity), Some(error.code)),
                    }
                }
                Err(error) => (Some(pid), None, Some(error.code)),
            },
            None => (None, None, Some("NATIVE_CHILD_ID_UNAVAILABLE".to_owned())),
        };
        let membership_guard = NativeMembershipGuard {
            owner_record: self._owner._record.clone(),
            pid: pid.unwrap_or_default(),
            expected_executable: spec.executable.clone(),
            verified_at_spawn: membership_error_code.is_none(),
        };
        Ok(OwnedNativeChild {
            child,
            membership_guard,
            membership_error_code,
            process_instance,
        })
    }
}

/// Rechecks the exact live native child before each admitted prompt write.
/// The helper accepts only a distinct child PID and the current image receipt;
/// it does not adopt or launch a process and does not prove family departure.
#[derive(Clone)]
pub struct NativeMembershipGuard {
    owner_record: Value,
    pid: u32,
    expected_executable: PathBuf,
    verified_at_spawn: bool,
}

impl NativeMembershipGuard {
    pub fn verify(&self) -> Result<()> {
        if !self.verified_at_spawn || self.pid == 0 {
            return Err(Error::new(
                "NATIVE_OWNER_MEMBERSHIP_UNVERIFIED",
                "native child membership was not established at spawn",
            ));
        }
        let identity = process_image_identity(self.pid).map_err(|_| {
            Error::new(
                "NATIVE_OWNER_MEMBERSHIP_UNVERIFIED",
                "native child image identity is no longer observable",
            )
        })?;
        self.verify_image(&identity)
    }

    fn verify_image(&self, identity: &Value) -> Result<()> {
        if !same_executable_path(
            identity.get("image_path").and_then(Value::as_str),
            &self.expected_executable,
        ) {
            return Err(Error::new(
                "NATIVE_IMAGE_MISMATCH",
                "native child image differs from the configured executable",
            ));
        }
        let belongs =
            module_child_belongs_to_owner(&self.owner_record, identity).map_err(|_| {
                Error::new(
                    "NATIVE_OWNER_MEMBERSHIP_UNVERIFIED",
                    "native child membership could not be verified",
                )
            })?;
        if !belongs {
            return Err(Error::new(
                "NATIVE_OWNER_MEMBERSHIP_UNVERIFIED",
                "native child is not a live member of the manager-owned module process group",
            ));
        }
        Ok(())
    }
}

pub struct OwnedNativeChild {
    pub(crate) child: Child,
    membership_guard: NativeMembershipGuard,
    membership_error_code: Option<String>,
    process_instance: Option<Value>,
}

impl OwnedNativeChild {
    pub fn take_stdin(&mut self) -> Option<tokio::process::ChildStdin> {
        self.child.stdin.take()
    }

    pub fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        self.child.stdout.take()
    }

    pub fn take_stderr(&mut self) -> Option<tokio::process::ChildStderr> {
        self.child.stderr.take()
    }

    pub fn membership_guard(&self) -> NativeMembershipGuard {
        self.membership_guard.clone()
    }

    pub fn membership_error_code(&self) -> Option<&str> {
        self.membership_error_code.as_deref()
    }

    /// Wait for this exact child handle. The manager-owned non-killing process
    /// group still owns descendant cleanup; this direct-child observation says
    /// nothing about group emptiness or remote native work completion.
    pub async fn wait(&mut self) -> Result<NativeExit> {
        let status = self
            .child
            .wait()
            .await
            .map_err(|_| Error::new("NATIVE_WAIT_FAILED", "native process wait failed"))?;
        Ok(NativeExit {
            exit_code: status.code(),
            success: status.success(),
            process_instance: self.process_instance.clone(),
            direct_child_exited: true,
        })
    }
}

#[derive(Debug, Clone)]
pub struct NativeExit {
    pub exit_code: Option<i32>,
    pub success: bool,
    /// Optional exact process birth identity captured after Tokio spawn. It is
    /// diagnostic evidence for this direct child only, never family departure.
    pub process_instance: Option<Value>,
    pub direct_child_exited: bool,
}

fn same_executable_path(observed: Option<&str>, expected: &std::path::Path) -> bool {
    let Some(observed) = observed else {
        return false;
    };
    let Some(expected) = expected.to_str() else {
        return false;
    };
    #[cfg(windows)]
    {
        observed.eq_ignore_ascii_case(expected)
    }
    #[cfg(not(windows))]
    {
        observed == expected
    }
}
