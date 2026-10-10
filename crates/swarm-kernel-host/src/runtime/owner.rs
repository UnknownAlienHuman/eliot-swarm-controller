//! Optional local module launcher. Holds a real OS lock and non-killing process
//! group until both the bridge and its native children end. It is not a scheduler.
use crate::{
    error::{Error, Result},
    model,
    platform::{
        private_permissions,
        process_group::{Group, departed_empty},
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
    process::{Command, ExitStatus},
    sync::mpsc,
    time::{Duration, Instant},
};
use swarm_process::{StateMarkerError, acquire_state_marker};

#[cfg(windows)]
mod windows;
#[cfg(not(windows))]
use std::process::{Child, Stdio};
#[cfg(windows)]
use windows::Child;

const FAMILY_CLEANUP_GRACE: Duration = Duration::from_secs(30);
const FAMILY_CLEANUP_POLL: Duration = Duration::from_millis(500);
const MAX_REPLY_BYTES: usize = 65_536;
const OUTPUT_PAGE_BYTES: usize = 65_536;
const OWNER_EXIT_GRACE: Duration = Duration::from_secs(2);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerReply {
    version: u8,
    invocation: String,
    pending: bool,
    owner: Option<Value>,
    error_code: Option<String>,
    message: Option<String>,
}

fn emit_reply(reply: &OwnerReply) {
    // A disconnected foreground reader never changes process custody.
    if let Ok(bytes) = serde_json::to_vec(reply)
        && bytes.len() <= MAX_REPLY_BYTES
    {
        let mut stdout = std::io::stdout().lock();
        let _ = stdout
            .write_all(&bytes)
            .and_then(|()| stdout.write_all(b"\n"));
        let _ = stdout.flush();
    }
}

fn detach_child(mut child: Child) {
    // This waiter only reaps a child while the caller process remains alive.
    // Custody belongs to the independent owner process, never this thread.
    std::thread::spawn(move || {
        let _ = child.wait();
    });
}

fn forward_output(path: &Path, offset: &mut u64, output: &mut impl Write) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() < *offset {
        return Err(Error::new(
            "MODULE_OUTPUT_INVALID",
            "module output capture changed",
        ));
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(*offset))?;
    let mut page = [0; OUTPUT_PAGE_BYTES];
    let count = file.read(&mut page)?;
    output.write_all(&page[..count])?;
    output.flush()?;
    *offset += count as u64;
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FamilyCleanupDisposition {
    CleanupPending,
    Unknown,
    Departed,
}

impl FamilyCleanupDisposition {
    fn as_str(self) -> &'static str {
        match self {
            Self::CleanupPending => "CleanupPending",
            Self::Unknown => "Unknown",
            Self::Departed => "Departed",
        }
    }
}

pub fn verify_departed(owner: &Value) -> Result<()> {
    let token = model::text(owner, "token")?;
    if uuid::Uuid::parse_str(token).is_err() || owner["process"]["purpose"] != "module" {
        return Err(Error::invalid(
            "a recorded module-owner identity is required",
        ));
    }
    if !departed_empty(&owner["process"], token)? {
        return Err(Error::new(
            "MODULE_OWNER_ACTIVE",
            "previous bridge or native descendants remain; no replacement started",
        ));
    }
    Ok(())
}

pub fn read_record(path: &Path) -> Result<Value> {
    let mut body = Vec::new();
    File::open(path)?.take(65_537).read_to_end(&mut body)?;
    if body.len() > 65_536 {
        return Err(Error::invalid("module ownership record exceeds envelope"));
    }
    Ok(serde_json::from_slice(&body)?)
}

fn publish(path: &Path, value: &Value) -> Result<()> {
    let temp = path.with_file_name(format!(".{}.tmp", model::new_id()));
    let result = (|| -> Result<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        f.write_all(model::canonical(value)?.as_bytes())?;
        f.sync_all()?;
        drop(f);
        fs::rename(&temp, path)?;
        #[cfg(unix)]
        File::open(
            path.parent()
                .ok_or_else(|| Error::invalid("no owner directory"))?,
        )?
        .sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Persist an explicit recovery-gap classification in the module state
/// directory. A missing or corrupt owner identity is a terminal gap for this
/// state directory — not a prompt to invent an owner, adopt the checkpoint or
/// start fresh. Best-effort: the classified error is returned either way, and
/// an existing record of the same gap is kept; a different classification
/// replaces it (the latest classification is the operative one).
fn record_gap(dir: &Path, kind: &str, detail: &str) {
    let path = dir.join("recovery-gap.json");
    if let Ok(existing) = read_record(&path)
        && existing["kind"] == kind
    {
        return;
    }
    let _ = publish(
        &path,
        &json!({"version":1,"kind":kind,"detail":detail,"disposition":"recovery_not_authorized","recorded_at_ms":model::now_ms().unwrap_or(0)}),
    );
}

/// Publish an observation without changing custody. Failure to write or emit
/// the receipt must never unwind the owner and drop its lock or process group.
fn report_family_cleanup(
    dir: &Path,
    owner: &Value,
    bridge: &std::io::Result<ExitStatus>,
    disposition: FamilyCleanupDisposition,
    detail: Option<&str>,
) {
    let bridge = match bridge {
        Ok(status) => json!({
            "state":"exited",
            "success":status.success(),
            "code":status.code(),
        }),
        Err(error) => json!({
            "state":"wait_unknown",
            "error":error.to_string(),
        }),
    };
    let receipt = json!({
        "version":1,
        "disposition":disposition.as_str(),
        "owner":owner,
        "bridge":bridge,
        "family_departure":if disposition == FamilyCleanupDisposition::Departed {
            "verified_empty"
        } else {
            "not_proven"
        },
        "custody":"retained_by_current_owner_process",
        "detail":detail,
        "observed_at_ms":model::now_ms().ok(),
    });
    let path = dir.join("cleanup-status.json");
    let observation = match publish(&path, &receipt) {
        Ok(()) => json!({
            "module_owner_disposition":disposition.as_str(),
            "receipt":path.display().to_string(),
        }),
        Err(error) => json!({
            "module_owner_disposition":disposition.as_str(),
            "receipt_write_error":error.to_string(),
        }),
    };
    let _ = writeln!(std::io::stderr().lock(), "{observation}");
    if disposition != FamilyCleanupDisposition::Departed {
        emit_reply(&OwnerReply {
            version: 1,
            invocation: owner["token"].as_str().unwrap_or_default().to_owned(),
            pending: true,
            owner: Some(owner.clone()),
            error_code: Some(match disposition {
                FamilyCleanupDisposition::CleanupPending => "MODULE_CLEANUP_PENDING",
                _ => "MODULE_OWNER_CLEANUP_UNKNOWN",
            }.to_owned()),
            message: Some("module owner retains its lock and native process family; see cleanup-status.json and private output logs".to_owned()),
        });
    }
}

/// Run one explicit bridge command. No auto restart, executable lookup, shell
/// interpolation or native prompt. A second invocation never adopts live work.
pub fn run(state_dir: &Path, executable: &Path, args: &[String]) -> Result<()> {
    validate_executable(executable)?;
    let invocation = model::new_id();
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("module-owner-worker")
        .arg("--state-dir")
        .arg(state_dir)
        .arg("--command")
        .arg(executable)
        .arg("--invocation")
        .arg(&invocation)
        .arg("--")
        .args(args);
    #[cfg(windows)]
    let mut child = windows::spawn_owner(&command)?;
    #[cfg(not(windows))]
    let mut child = {
        command
            .stdin(Stdio::inherit())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        command.spawn()?
    };
    let pid = child.id();
    let birth = swarm_process::spawned_identity(pid).ok();
    let Some(stdout) = child.stdout.take() else {
        detach_child(child);
        return Err(Error::new(
            "MODULE_OWNER_STATUS_UNKNOWN",
            "owner response channel is unavailable",
        ));
    };
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let result = BufReader::new(stdout)
            .take((MAX_REPLY_BYTES + 1) as u64)
            .read_until(b'\n', &mut bytes)
            .map(|_| bytes);
        let _ = sender.send(result);
    });
    let mut stdout_offset = 0;
    let mut stderr_offset = 0;
    let stdout_path = state_dir.join(format!("stdout-{invocation}.log"));
    let stderr_path = state_dir.join(format!("stderr-{invocation}.log"));
    let received = loop {
        let result = receiver.recv_timeout(Duration::from_millis(100));
        let forwarding = forward_output(&stdout_path, &mut stdout_offset, &mut std::io::stdout())
            .and_then(|()| {
                forward_output(&stderr_path, &mut stderr_offset, &mut std::io::stderr())
            });
        if let Err(error) = forwarding {
            detach_child(child);
            return Err(error);
        }
        match result {
            Ok(result) => break result,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                detach_child(child);
                return Err(Error::new(
                    "MODULE_OWNER_STATUS_UNKNOWN",
                    "owner response channel ended without a disposition",
                ));
            }
        }
    };
    let reply = received
        .ok()
        .filter(|bytes| bytes.len() <= MAX_REPLY_BYTES && bytes.last() == Some(&b'\n'))
        .and_then(|bytes| serde_json::from_slice::<OwnerReply>(&bytes).ok())
        .filter(|reply| reply.version == 1 && reply.invocation == invocation);
    let Some(reply) = reply else {
        detach_child(child);
        return Err(Error::new(
            "MODULE_OWNER_STATUS_UNKNOWN",
            "owner returned no exact bounded disposition",
        ));
    };
    if reply.pending {
        let matches_birth =
            reply
                .owner
                .as_ref()
                .zip(birth.as_ref())
                .is_some_and(|(owner, birth)| {
                    let process = &owner["process"];
                    owner["token"] == invocation
                        && process["pid"].as_u64() == Some(pid as u64)
                        && process["purpose"] == "module"
                        && if cfg!(windows) {
                            process["creation_filetime"] == birth["creation_filetime"]
                                && !process["creation_filetime"].is_null()
                        } else {
                            process["start_ticks"] == birth["start_ticks"]
                                && !process["start_ticks"].is_null()
                                && process["boot_id"] == birth["boot_id"]
                                && !process["boot_id"].is_null()
                        }
                });
        detach_child(child);
        if !matches_birth
            || !matches!(
                reply.error_code.as_deref(),
                Some("MODULE_CLEANUP_PENDING" | "MODULE_OWNER_CLEANUP_UNKNOWN")
            )
        {
            return Err(Error::new(
                "MODULE_OWNER_STATUS_UNKNOWN",
                "pending disposition has no exact owner birth identity",
            ));
        }
        return Err(Error::new(
            reply.error_code.unwrap_or_default(),
            reply.message.unwrap_or_default(),
        ));
    }
    let deadline = Instant::now() + OWNER_EXIT_GRACE;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if let Some(code) = reply.error_code {
                    return Err(Error::new(code, reply.message.unwrap_or_default()));
                }
                return if status.success() {
                    Ok(())
                } else {
                    Err(Error::new(
                        "MODULE_OWNER_STATUS_UNKNOWN",
                        "owner exited without confirming its result",
                    ))
                };
            }
            Ok(None) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            _ => {
                detach_child(child);
                return Err(Error::new(
                    "MODULE_OWNER_STATUS_UNKNOWN",
                    "owner terminal exit is unconfirmed",
                ));
            }
        }
    }
}

fn validate_executable(executable: &Path) -> Result<()> {
    if !executable.is_absolute() {
        return Err(Error::invalid(
            "module executable must be an absolute native executable path",
        ));
    }
    #[cfg(windows)]
    if executable
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("cmd") || e.eq_ignore_ascii_case("bat"))
    {
        return Err(Error::invalid(
            "select a native executable, not a shell wrapper",
        ));
    }
    Ok(())
}

/// Hidden worker entrypoint. Its stdout is a disposition channel, never native
/// output. The worker remains the independent custodian after the CLI returns.
pub fn run_worker(state_dir: &Path, executable: &Path, args: &[String], invocation: &str) {
    let result = run_owned(state_dir, executable, args, invocation);
    emit_reply(&OwnerReply {
        version: 1,
        invocation: invocation.to_owned(),
        pending: false,
        owner: None,
        error_code: result.as_ref().err().map(|error| error.code.clone()),
        message: result.as_ref().err().map(|error| error.message.clone()),
    });
}

fn run_owned(state_dir: &Path, executable: &Path, args: &[String], invocation: &str) -> Result<()> {
    validate_executable(executable)?;
    if uuid::Uuid::parse_str(invocation).is_err() {
        return Err(Error::invalid("module owner invocation must be a UUID"));
    }
    fs::create_dir_all(state_dir)?;
    let dir = fs::canonicalize(state_dir)?;
    const MARKER: &[u8] = b"ELIOT_SWARM_MODULE_V1\n";
    let lock = match acquire_state_marker(&dir, "module.lock", MARKER) {
        Ok(lock) => lock,
        Err(StateMarkerError::Busy) => {
            return Err(Error::new(
                "MODULE_OWNER_ACTIVE",
                "module marker lock is already held",
            ));
        }
        Err(StateMarkerError::ForeignDirectory | StateMarkerError::InvalidMarker) => {
            return Err(Error::new(
                "FOREIGN_STATE_DIRECTORY",
                "use a dedicated empty module state directory",
            ));
        }
        Err(StateMarkerError::System(error)) => return Err(error.into()),
    };
    private_permissions(&dir, true)?;
    let record = dir.join("owner.json");
    let cleanup_status = dir.join("cleanup-status.json");
    let mut previous_owner_departed = false;
    if record.try_exists()? {
        let parsed = read_record(&record);
        match parsed {
            Ok(owner) => {
                if let Err(e) = verify_departed(&owner) {
                    // A structurally incomplete identity is a recorded gap;
                    // an active owner is a live boundary, not a gap.
                    if e.code == "INVALID_PARAMS" {
                        record_gap(&dir, "incomplete_owner_identity", &e.to_string());
                    }
                    return Err(e);
                }
                previous_owner_departed = true;
            }
            Err(e) => {
                record_gap(&dir, "corrupt_owner_identity", &e.to_string());
                return Err(Error::new(
                    "MODULE_OWNER_IDENTITY_INVALID",
                    format!(
                        "recorded module-owner identity is unreadable; recovery gap recorded, checkpoint is not adopted: {e}"
                    ),
                ));
            }
        }
    } else if cleanup_status.try_exists()? {
        record_gap(
            &dir,
            "missing_owner_identity",
            "cleanup status exists without its exact owner identity",
        );
        return Err(Error::new(
            "MODULE_OWNER_IDENTITY_MISSING",
            "cleanup status without owner identity is not safe to replace; recovery gap recorded",
        ));
    } else if dir.join("checkpoint.json").try_exists()? {
        record_gap(
            &dir,
            "missing_owner_identity",
            "checkpoint exists without an ownership record",
        );
        return Err(Error::new(
            "MODULE_OWNER_IDENTITY_MISSING",
            "checkpoint without ownership evidence is not safe to resume; recovery gap recorded",
        ));
    }
    if previous_owner_departed && cleanup_status.try_exists()? {
        fs::remove_file(&cleanup_status)?;
    }
    let token = invocation;
    let group = Group::enter_module(token)?;
    let owner = json!({
        "version":1,
        "token":token,
        "process":group.identity.clone(),
        "stdout_log":dir.join(format!("stdout-{token}.log")),
        "stderr_log":dir.join(format!("stderr-{token}.log")),
    });
    publish(&record, &owner)?;
    let open_output = |name: &str| -> Result<File> {
        let path = dir.join(format!("{name}-{token}.log"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        private_permissions(&path, false)?;
        Ok(file)
    };
    let mut command = Command::new(executable);
    command
        .args(args)
        .env("ELIOT_SWARM_MODULE_OWNER", &record)
        .env("ELIOT_SWARM_MODULE_STATE", &dir);
    let stdout = open_output("stdout")?;
    let stderr = open_output("stderr")?;
    #[cfg(windows)]
    let status = windows::spawn_bridge(&command, stdout, stderr)?.wait();
    #[cfg(not(windows))]
    let status = {
        command.stdout(stdout).stderr(stderr);
        command.spawn()?.wait()
    };
    // The bridge can die before its native process drains. Neither host loss,
    // bridge exit nor dropping this non-killing Job authorizes killing agents.
    let cleanup_started = Instant::now();
    let mut pending_reported = false;
    let mut unknown_reported = false;
    if let Err(error) = &status {
        report_family_cleanup(
            &dir,
            &owner,
            &status,
            FamilyCleanupDisposition::Unknown,
            Some(&error.to_string()),
        );
        unknown_reported = true;
    }
    loop {
        match group.children_empty() {
            Ok(true) => {
                if pending_reported || unknown_reported {
                    report_family_cleanup(
                        &dir,
                        &owner,
                        &status,
                        FamilyCleanupDisposition::Departed,
                        None,
                    );
                }
                break;
            }
            Ok(false) => {
                if cleanup_started.elapsed() >= FAMILY_CLEANUP_GRACE
                    && !pending_reported
                    && !unknown_reported
                {
                    report_family_cleanup(
                        &dir,
                        &owner,
                        &status,
                        FamilyCleanupDisposition::CleanupPending,
                        None,
                    );
                    pending_reported = true;
                }
            }
            Err(error) => {
                if !unknown_reported {
                    report_family_cleanup(
                        &dir,
                        &owner,
                        &status,
                        FamilyCleanupDisposition::Unknown,
                        Some(&error.to_string()),
                    );
                    unknown_reported = true;
                }
            }
        }
        std::thread::sleep(FAMILY_CLEANUP_POLL);
    }
    drop(group);
    drop(lock);
    let status = status.map_err(|error| {
        Error::new(
            "MODULE_OWNER_WAIT_UNKNOWN",
            format!("bridge wait failed after exact family departure: {error}"),
        )
    })?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::new(
            "MODULE_EXITED",
            format!("bridge ended: {status}; native group is empty"),
        ))
    }
}
