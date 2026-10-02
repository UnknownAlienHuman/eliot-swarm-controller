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
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::Path,
    process::Command,
    time::Duration,
};

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
/// an existing record of a different gap is never overwritten.
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

/// Run one explicit bridge command. No auto restart, executable lookup, shell
/// interpolation or native prompt. A second invocation never adopts live work.
pub fn run(state_dir: &Path, executable: &Path, args: &[String]) -> Result<()> {
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
    fs::create_dir_all(state_dir)?;
    let dir = fs::canonicalize(state_dir)?;
    let marker = dir.join("module.lock");
    let empty = fs::read_dir(&dir)?.next().transpose()?.is_none();
    if !empty && !marker.is_file() {
        return Err(Error::new(
            "FOREIGN_STATE_DIRECTORY",
            "use a dedicated empty module state directory",
        ));
    }
    let mut lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(empty)
        .truncate(false)
        .open(&marker)?;
    lock.try_lock()
        .map_err(|e| Error::new("MODULE_OWNER_ACTIVE", e.to_string()))?;
    let mut text = String::new();
    (&mut lock).take(128).read_to_string(&mut text)?;
    const MARKER: &str = "ELIOT_SWARM_MODULE_V1\n";
    if text.is_empty() && empty {
        lock.write_all(MARKER.as_bytes())?;
        lock.sync_all()?;
    } else if text != MARKER {
        return Err(Error::new(
            "FOREIGN_STATE_DIRECTORY",
            "invalid module ownership marker",
        ));
    }
    private_permissions(&dir, true)?;
    let record = dir.join("owner.json");
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
    let token = model::new_id();
    let group = Group::enter_module(&token)?;
    publish(
        &record,
        &json!({"version":1,"token":token,"process":group.identity}),
    )?;
    let mut command = Command::new(executable);
    command
        .args(args)
        .env("ELIOT_SWARM_MODULE_OWNER", &record)
        .env("ELIOT_SWARM_MODULE_STATE", &dir);
    let status = command.spawn()?.wait()?;
    // The bridge can die before its native process drains. Neither host loss,
    // bridge exit nor dropping this non-killing Job authorizes killing agents.
    while !group.children_empty()? {
        std::thread::sleep(Duration::from_millis(500));
    }
    drop(group);
    drop(lock);
    if status.success() {
        Ok(())
    } else {
        Err(Error::new(
            "MODULE_EXITED",
            format!("bridge ended: {status}; native group is empty"),
        ))
    }
}
