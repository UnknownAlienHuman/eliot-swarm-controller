//! Shared stand registration, expiry, external-repo Git proof, and offline cleanup.

use crate::common::now_ms;
use crate::git_monitor::{GitProbe, canonical_root, reject_links};
use crate::operator::{
    DaemonLock, create_stand_root, daemon_alive, daemon_info, lock_offline, lock_transition,
    validate_stand_directory, write_stand_record,
};
use crate::platform::script_file::directory_identity;
use crate::platform::{OwnedChild, is_reparse_point, worktree_guard::WorktreeWriteGuard};
use crate::workspace_git::{WorkspaceGit, containment::branch_rev_parse};
use eliot_core::api::{CleanupGitBinding, MAX_ARTIFACT_TTL_MS};
use eliot_core::{
    Error, Result, WorkspacePreparation, WorkspaceRemovalOutcome, workspace_path_key,
};
use eliot_store::{StandWorkspaceRecord, Store};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
#[expect(
    clippy::disallowed_types,
    reason = "Stand cleanup executes prepared non-force Git worktree removal commands under OwnedChild custody."
)]
use tokio::process::Command;
use tokio::time::{Instant, timeout_at};

pub const STAND_MARKER: &str = ".eliot-stand.json";
const MAX_RECORD_BYTES: u64 = 32 * 1024;
const MAX_STAND_ENTRIES: usize = 200_000;
const MAX_STAND_DEPTH: usize = 64;
const STAND_OUTPUT_BYTES: usize = 128 * 1024;
const STAND_GIT_TIME: Duration = Duration::from_secs(60);
const STAND_CUSTODY_CLEANUP_TIME: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandMarker {
    pub format: u32,
    pub root: PathBuf,
    pub token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandWriteGuardJournal {
    pub worktree_path: String,
    pub journal: serde_json::Value,
}

/// One stand-created managed worktree, recorded in the external registration
/// by the stand daemon before Git creates it. Cleanup trusts repository,
/// branch and creation OID only when the stand Store agrees with this record.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandWorktreeRecord {
    pub repo_common_dir: String,
    pub branch: String,
    pub base_oid: String,
    pub worktree_path: String,
}

impl StandWorktreeRecord {
    fn of(preparation: &WorkspacePreparation) -> Self {
        Self {
            repo_common_dir: workspace_path_key(&preparation.repo_common_dir).into_owned(),
            branch: preparation.requested_branch.clone(),
            base_oid: preparation.base_oid.clone(),
            worktree_path: workspace_path_key(&preparation.worktree_path).into_owned(),
        }
    }

    fn file_name(&self) -> String {
        format!(
            "{}.json",
            eliot_core::api::inference::sha256_hex(self.worktree_path.as_bytes())
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandRegistration {
    pub marker: StandMarker,
    pub removing_pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_data_dir: Option<PathBuf>,
    /// SHA-256 of the owner token the stand's own daemon minted at creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_token_sha256: Option<String>,
    /// Handle identity (volume serial and file index) of the directory
    /// `stand up` created as this stand's root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_write_guard: Option<StandWriteGuardJournal>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub settled_external_worktrees: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub settled_external_branches: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandWorktreeOutcome {
    pub worktree_path: String,
    pub repo_path: String,
    pub branch: String,
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The refusal text when a record or location check kept this item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandBranchOutcome {
    pub repo_common_dir: String,
    pub branch: String,
    pub outcome: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The refusal text when a record check kept this item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StandExternalCleanupReport {
    pub worktrees: Vec<StandWorktreeOutcome>,
    pub branches: Vec<StandBranchOutcome>,
    /// Unusable creation records not already reported on a worktree or branch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unusable_records: Vec<String>,
}

impl StandExternalCleanupReport {
    pub fn has_retained_or_unknown(&self) -> bool {
        self.worktrees.iter().any(|item| item.outcome != "removed")
            || self.branches.iter().any(|item| item.outcome != "removed")
            || !self.unusable_records.is_empty()
    }

    pub fn has_unknown(&self) -> bool {
        self.worktrees.iter().any(|item| item.outcome == "unknown")
            || self.branches.iter().any(|item| item.outcome == "unknown")
    }

    pub fn first_blocker_reason(&self) -> (&'static str, &str) {
        if let Some(reason) = self
            .worktrees
            .iter()
            .filter(|item| item.outcome == "unknown")
            .filter_map(|item| item.reason.as_deref())
            .chain(
                self.branches
                    .iter()
                    .filter(|item| item.outcome == "unknown")
                    .filter_map(|item| item.reason.as_deref()),
            )
            .next()
        {
            return ("unknown", reason);
        }
        for item in &self.worktrees {
            if item.outcome != "removed" {
                let code = leak_static_known_reason(item.reason.as_deref());
                let detail = item
                    .detail
                    .as_deref()
                    .unwrap_or("stand external worktree could not be safely removed");
                return (code, detail);
            }
        }
        for item in &self.branches {
            if item.outcome != "removed" {
                let code = leak_static_known_reason(item.reason.as_deref());
                let detail = item
                    .detail
                    .as_deref()
                    .unwrap_or("stand external branch could not be safely removed");
                return (code, detail);
            }
        }
        if let Some(message) = self.unusable_records.first() {
            return ("not_allowed", message);
        }
        (
            "stand_external_retained",
            "stand external Git resource retained",
        )
    }
}

fn leak_static_known_reason(reason: Option<&str>) -> &'static str {
    match reason {
        Some("dirty") => "dirty",
        Some("unmerged") => "unmerged",
        Some("ahead") => "ahead",
        Some("content_not_at_tip") => "content_not_at_tip",
        Some("worktree_attached") => "worktree_attached",
        Some("unregistered_worktree") => "unregistered_worktree",
        Some("branch_checked_out") => "branch_checked_out",
        Some("no_upstream_remote") => "no_upstream_remote",
        Some("no_default_branch") => "no_default_branch",
        Some("fetch_failed") => "fetch_failed",
        Some("remote_check_failed") => "remote_check_failed",
        Some("remote_transport_unsupported") => "remote_transport_unsupported",
        Some("invalid_remote") => "invalid_remote",
        Some("merge_check_failed") => "merge_check_failed",
        Some("merge_scan_limit") => "merge_scan_limit",
        Some("merge_scan_deadline") => "merge_scan_deadline",
        Some("pending_write_guard") => "pending_write_guard",
        Some("source_integrity") => "source_integrity",
        Some("identity_changed") => "identity_changed",
        Some("not_allowed") => "not_allowed",
        Some("unavailable") => "unavailable",
        Some("busy") => "busy",
        Some("unsupported") => "unsupported",
        Some("unknown") => "unknown",
        _ => "stand_external_retained",
    }
}

#[derive(Clone, Debug)]
pub struct StandEntry {
    pub registry_file: String,
    pub registration: StandRegistration,
    /// The record's path while it is still at the former AppData location.
    pub former_path: Option<String>,
}

/// A registry file that exists but cannot be used as a stand registration.
#[derive(Clone, Debug, Serialize)]
pub struct UnusableStandRegistration {
    pub registry_path: String,
    pub detail: String,
}

/// Every registry file, either usable or reported with its failure.
#[derive(Clone, Debug, Default)]
pub struct StandRegistryScan {
    pub entries: Vec<StandEntry>,
    pub unusable: Vec<UnusableStandRegistration>,
}

impl StandRegistryScan {
    fn sort(&mut self) {
        self.unusable
            .sort_by(|left, right| left.registry_path.cmp(&right.registry_path));
        self.entries.sort_by(|left, right| {
            left.registration
                .marker
                .created_at
                .cmp(&right.registration.marker.created_at)
                .then_with(|| {
                    left.registration
                        .marker
                        .root
                        .cmp(&right.registration.marker.root)
                })
        });
    }
}

/// One stand's registration and the directory that holds it now.
#[derive(Clone, Debug)]
pub struct LocatedRegistration {
    pub registry: PathBuf,
    pub record: StandRegistration,
    /// The record is still at the former AppData location.
    pub former: bool,
}

/// A stand's registry after any former-location record was handled.
#[derive(Clone, Debug)]
pub struct SettledRegistration {
    pub registry: PathBuf,
    /// What happened at the former location and its path, when reportable.
    pub former: Option<(String, String)>,
}

pub fn parse_stand_ttl_ms(value: &str) -> Result<i64> {
    let ms = crate::automation::parse_positive_duration_ms(value).map_err(|_| {
        Error::new(
            "invalid",
            "TTL must be a positive duration (e.g. 30m, 24h, 7d)",
        )
    })?;
    if !(1_000..=MAX_ARTIFACT_TTL_MS).contains(&ms) {
        return Err(Error::new(
            "invalid",
            format!("TTL must be between 1s and 30d ({MAX_ARTIFACT_TTL_MS} ms)"),
        ));
    }
    Ok(ms)
}

/// The per-user stand home from the account profile (Known Folder on Windows,
/// the password database on Unix). The process environment cannot move it. On
/// Windows it stays outside AppData, so a CLI inside an MSIX package and the
/// out-of-package stand daemon see the same files.
#[cfg(windows)]
fn fixed_stand_home() -> Result<PathBuf> {
    crate::platform::windows::account_profile().map(|path| path.join(".eliot-stands"))
}

#[cfg(unix)]
fn fixed_stand_home() -> Result<PathBuf> {
    crate::platform::unix::account_home()
        .map(|path| path.join(".local").join("state").join("eliot-stands"))
}

#[cfg(not(any(windows, unix)))]
fn fixed_stand_home() -> Result<PathBuf> {
    Err(Error::new(
        "platform_unsupported",
        "private stands are unsupported on this platform",
    ))
}

/// Designated parent for new stands without `--root`: `SWARM_STANDS_ROOT`,
/// or the fixed stand home. It never selects the registry.
pub fn stand_base(create: bool) -> Result<PathBuf> {
    let base = match std::env::var_os("SWARM_STANDS_ROOT") {
        Some(path) if !path.is_empty() => PathBuf::from(path),
        _ => fixed_stand_home()?,
    };
    checked_stand_dir(base, create)
}

/// The stand registry, always under the fixed stand home and never selected
/// by the environment.
pub fn stand_registry(create: bool) -> Result<PathBuf> {
    let base = checked_stand_dir(fixed_stand_home()?, create)?;
    let registry = base.join(".registry");
    if create
        && !registry.exists()
        && let Err(error) = create_stand_root(&registry)
        && !registry.exists()
    {
        return Err(error);
    }
    if !registry.exists() {
        return Err(Error::new(
            "not_found",
            "stand registry directory does not exist",
        ));
    }
    check_directory_chain(&registry)?;
    validate_stand_directory(&registry)?;
    Ok(registry)
}

fn checked_stand_dir(base: PathBuf, create: bool) -> Result<PathBuf> {
    let base = std::path::absolute(base).map_err(|error| {
        Error::new(
            "platform_io",
            format!("resolve designated stand root: {error}"),
        )
    })?;
    if create {
        crate::platform::data_dir::refuse_unverified_write(&base)?;
    } else {
        crate::platform::data_dir::refuse_package_redirected(&base)?;
    }
    if base.components().any(|part| part == Component::ParentDir) {
        return Err(Error::new(
            "invalid",
            "stand root must not contain parent-directory traversal",
        ));
    }
    if base.starts_with(std::env::temp_dir()) {
        return Err(Error::new(
            "invalid",
            "the designated stand root must be outside the temporary directory",
        ));
    }
    if base
        .ancestors()
        .any(|path| path.join("daemon.lock").exists() || path.join(STAND_MARKER).exists())
    {
        return Err(Error::new(
            "invalid",
            "the designated stand root cannot be inside daemon data",
        ));
    }
    if create {
        if let Some(existing) = base
            .ancestors()
            .filter(|path| !path.as_os_str().is_empty())
            .find(|path| fs::symlink_metadata(path).is_ok())
        {
            check_directory_chain(existing)?;
        }
        fs::create_dir_all(&base).map_err(|error| {
            Error::new(
                "platform_io",
                format!("create designated stand root: {error}"),
            )
        })?;
    } else if fs::symlink_metadata(&base).is_err() {
        return Err(Error::new(
            "not_found",
            "designated stand root does not exist",
        ));
    }
    check_directory_chain(&base)?;
    let base = base.canonicalize().map_err(|error| {
        Error::new(
            "platform_io",
            format!("resolve designated stand root: {error}"),
        )
    })?;
    if std::env::temp_dir()
        .canonicalize()
        .is_ok_and(|temp| base.starts_with(temp))
    {
        return Err(Error::new(
            "invalid",
            "the designated stand root must be outside the temporary directory",
        ));
    }
    Ok(base)
}

pub fn ensure_registry_is_external(root: &Path, registry: &Path) -> Result<()> {
    if registry.starts_with(root) || root.starts_with(registry) {
        return Err(Error::new(
            "invalid",
            "a stand and its registry must be in separate directories",
        ));
    }
    Ok(())
}

pub fn registry_name(root: &Path) -> String {
    let identity = root.to_string_lossy();
    let identity = if cfg!(windows) {
        identity.to_lowercase()
    } else {
        identity.into_owned()
    };
    format!(
        "{}.json",
        eliot_core::api::inference::sha256_hex(identity.as_bytes())
    )
}

pub fn write_stand_registration(
    registry: &Path,
    name: &str,
    registration: &StandRegistration,
) -> Result<()> {
    let bytes = serde_json::to_vec(registration)
        .map_err(|_| Error::new("internal", "encode stand registration"))?;
    write_stand_record(registry, name, &bytes)
}

pub fn read_stand_registration(
    registry: &Path,
    name: &str,
    root: &Path,
) -> Result<StandRegistration> {
    let path = registry.join(name);
    let record: StandRegistration = read_record(&path).map_err(|failure| match failure {
        RecordFailure::Missing => Error::new(
            "not_found",
            format!(
                "directory is not registered by swarm stand up: no registration at {}",
                path.display()
            ),
        ),
        RecordFailure::Unusable(detail) => unusable_record("stand registration", &path, &detail),
    })?;
    if record.marker.format != 2 || registry_name(&record.marker.root) != registry_name(root) {
        return Err(Error::new(
            "not_allowed",
            "stand registration does not bind this directory",
        ));
    }
    Ok(record)
}

pub fn list_stand_registrations(registry: &Path) -> Result<StandRegistryScan> {
    let mut scan = StandRegistryScan::default();
    for item in fs::read_dir(registry)
        .map_err(|error| Error::new("platform_io", format!("list stand registry: {error}")))?
    {
        let item = item.map_err(|error| {
            Error::new("platform_io", format!("read stand registry entry: {error}"))
        })?;
        let file_name = item.file_name();
        let Some(name) = file_name.to_str() else {
            scan.unusable.push(UnusableStandRegistration {
                registry_path: item.path().display().to_string(),
                detail: "file name is not valid UTF-8".to_owned(),
            });
            continue;
        };
        if !name.ends_with(".json") || name.starts_with('.') {
            continue;
        }
        let path = item.path();
        let detail = match read_record::<StandRegistration>(&path) {
            Ok(reg) if reg.marker.format == 2 && registry_name(&reg.marker.root) == name => {
                scan.entries.push(StandEntry {
                    registry_file: name.to_owned(),
                    registration: reg,
                    former_path: None,
                });
                continue;
            }
            Ok(_) => "binding check failed: marker format or root does not match this file name"
                .to_owned(),
            // Removed while the registry was being listed.
            Err(RecordFailure::Missing) => continue,
            Err(RecordFailure::Unusable(detail)) => detail,
        };
        scan.unusable.push(UnusableStandRegistration {
            registry_path: path.display().to_string(),
            detail,
        });
    }
    scan.sort();
    Ok(scan)
}

/// How output names the former registry when its path cannot be resolved.
const FORMER_REGISTRY_LABEL: &str = r"%LOCALAPPDATA%\EliotStands\.registry";

/// The AppData registry used before the stand home moved out of MSIX
/// redirection. `Ok(None)` on platforms that never had it.
fn former_registry() -> Result<Option<PathBuf>> {
    #[cfg(windows)]
    {
        crate::platform::windows::account_local_app_data()
            .map(|local| Some(local.join("EliotStands").join(".registry")))
    }
    #[cfg(not(windows))]
    {
        Ok(None)
    }
}

/// One wording for a former registry whose location cannot be resolved.
fn former_unavailable(error: &Error) -> String {
    format!("cannot be located: {}", error.message)
}

/// Add the stands an earlier build registered at the former location, each
/// with that path, and report every former file that cannot be used.
pub fn add_former_registrations(scan: &mut StandRegistryScan) {
    let dir = match former_registry() {
        Ok(Some(dir)) => dir,
        Ok(None) => return,
        Err(error) => {
            scan.unusable.push(UnusableStandRegistration {
                registry_path: FORMER_REGISTRY_LABEL.to_owned(),
                detail: format!("former stand registry {}", former_unavailable(&error)),
            });
            return;
        }
    };
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            scan.unusable.push(UnusableStandRegistration {
                registry_path: dir.display().to_string(),
                detail: format!("former stand registry cannot be listed: {error}"),
            });
            return;
        }
    };
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                scan.unusable.push(UnusableStandRegistration {
                    registry_path: dir.display().to_string(),
                    detail: format!("former stand registry entry cannot be read: {error}"),
                });
                continue;
            }
        };
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            scan.unusable.push(UnusableStandRegistration {
                registry_path: entry.path().display().to_string(),
                detail: "former registry location: file name is not valid UTF-8".to_owned(),
            });
            continue;
        };
        if !name.ends_with(".json") || name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        let detail = match read_record::<StandRegistration>(&path) {
            Ok(reg) if reg.marker.format == 2 && registry_name(&reg.marker.root) == name => {
                match scan
                    .entries
                    .iter()
                    .find(|current| current.registry_file == name)
                {
                    None => {
                        scan.entries.push(StandEntry {
                            registry_file: name.to_owned(),
                            registration: reg,
                            former_path: Some(path.display().to_string()),
                        });
                        continue;
                    }
                    // A move interrupted before the former file was removed.
                    Some(current) if current.registration.marker.token == reg.marker.token => {
                        continue;
                    }
                    Some(_) => "this directory now has a different current registration".to_owned(),
                }
            }
            Ok(_) => "binding check failed: marker format or root does not match this file name"
                .to_owned(),
            Err(RecordFailure::Missing) => continue,
            Err(RecordFailure::Unusable(detail)) => detail,
        };
        scan.unusable.push(UnusableStandRegistration {
            registry_path: path.display().to_string(),
            detail: format!("former registry location: {detail}"),
        });
    }
    scan.sort();
}

/// Find one stand's registration: the current registry first, then the former
/// location. Every caller applies the same marker, identity and owner checks.
pub fn locate_stand_registration(root: &Path) -> Result<LocatedRegistration> {
    let name = registry_name(root);
    match stand_registry(false) {
        Ok(registry) => match read_stand_registration(&registry, &name, root) {
            Ok(record) => {
                return Ok(LocatedRegistration {
                    registry,
                    record,
                    former: false,
                });
            }
            Err(error) if error.code == "not_found" => {}
            Err(error) => return Err(error),
        },
        Err(error) if error.code == "not_found" => {}
        Err(error) => return Err(error),
    }
    let current = fixed_stand_home()?.join(".registry").join(&name);
    let Some(former) = former_registry()? else {
        return Err(Error::new(
            "not_found",
            format!(
                "directory is not registered by swarm stand up: no registration at {}",
                current.display()
            ),
        ));
    };
    if fs::symlink_metadata(former.join(&name))
        .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Err(Error::new(
            "not_found",
            format!(
                "directory is not registered by swarm stand up: no registration at {} or the former location {}",
                current.display(),
                former.join(&name).display()
            ),
        ));
    }
    check_directory_chain(&former)?;
    crate::platform::data_dir::validate_former_registry(&former)?;
    let record = read_stand_registration(&former, &name, root)?;
    Ok(LocatedRegistration {
        registry: former,
        record,
        former: true,
    })
}

/// Move a former-location registration and its worktree records into the
/// current registry. Call it only after the stand's daemon exit is proven and
/// under its offline lock, so the old daemon cannot add a record. Safe to
/// repeat after a crash. A problem at the former location never blocks a
/// stand already in the current registry; it is returned for the output.
pub fn settle_former_registration(
    root: &Path,
    located: &LocatedRegistration,
    record: &StandRegistration,
) -> Result<SettledRegistration> {
    let name = registry_name(root);
    let keep = |former: Option<(String, String)>| {
        Ok(SettledRegistration {
            registry: located.registry.clone(),
            former,
        })
    };
    let former = match former_registry() {
        Ok(Some(former)) => former,
        Ok(None) => return keep(None),
        Err(error) if located.former => return Err(error),
        Err(error) => {
            return keep(Some((
                former_unavailable(&error),
                FORMER_REGISTRY_LABEL.to_owned(),
            )));
        }
    };
    let former_file = former.join(&name);
    let found = read_record::<StandRegistration>(&former_file);
    let same_stand = matches!(
        &found,
        Ok(found) if found.marker.token == record.marker.token
            && registry_name(&found.marker.root) == name
    );
    if !same_stand {
        if located.former {
            return Err(Error::new(
                "identity_changed",
                format!(
                    "former stand registration at {} changed before it could be moved",
                    former_file.display()
                ),
            ));
        }
        let note = match found {
            Err(RecordFailure::Missing) => None,
            Err(RecordFailure::Unusable(detail)) => {
                Some(format!("unusable, left in place: {detail}"))
            }
            Ok(_) => Some("belongs to a different creation, left in place".to_owned()),
        };
        return keep(note.map(|action| (action, former_file.display().to_string())));
    }
    match move_former_registration(&former, &name, record, located.former) {
        Ok(registry) => Ok(SettledRegistration {
            registry,
            former: Some((
                "migrated to the current registry".to_owned(),
                former_file.display().to_string(),
            )),
        }),
        Err(error) if located.former => Err(error),
        Err(error) => keep(Some((
            format!("could not be retired, left in place: {}", error.message),
            former_file.display().to_string(),
        ))),
    }
}

/// Copy the former worktree records and, when the stand is not yet in the
/// current registry, its registration, then remove the former files. Every
/// record is checked before the first write.
fn move_former_registration(
    former: &Path,
    name: &str,
    record: &StandRegistration,
    write_registration: bool,
) -> Result<PathBuf> {
    let former_records = read_former_worktree_records(&worktree_records_dir(former, name))?;
    let registry = stand_registry(true)?;
    let dir = worktree_records_dir(&registry, name);
    let mut to_write = Vec::new();
    for (file, moved) in &former_records {
        let path = dir.join(file);
        match read_record::<StandWorktreeRecord>(&path) {
            Ok(existing) if existing == *moved => {}
            Ok(_) => {
                return Err(Error::new(
                    "identity_changed",
                    format!(
                        "stand worktree record at {} differs from the former record",
                        path.display()
                    ),
                ));
            }
            Err(RecordFailure::Unusable(detail)) => {
                return Err(unusable_record("stand worktree record", &path, &detail));
            }
            Err(RecordFailure::Missing) => to_write.push((file, moved)),
        }
    }
    if !to_write.is_empty() {
        if !dir.exists()
            && let Err(error) = create_stand_root(&dir)
            && !dir.exists()
        {
            return Err(error);
        }
        check_directory_chain(&dir)?;
        validate_stand_directory(&dir)?;
        for (file, moved) in to_write {
            let bytes = serde_json::to_vec(moved)
                .map_err(|_| Error::new("internal", "encode stand worktree record"))?;
            write_stand_record(&dir, file, &bytes)?;
        }
    }
    if write_registration {
        write_stand_registration(&registry, name, record)?;
    }
    remove_stand_registration(former, name)?;
    Ok(registry)
}

/// Every former worktree record, or a refusal naming the first unusable one,
/// before anything is moved.
fn read_former_worktree_records(dir: &Path) -> Result<Vec<(String, StandWorktreeRecord)>> {
    let list_failed = |error: std::io::Error| {
        Error::new(
            "platform_io",
            format!(
                "list former stand worktree records at {}: {error}",
                dir.display()
            ),
        )
    };
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(list_failed(error)),
    };
    let mut records = Vec::new();
    for entry in entries {
        let entry = entry.map_err(list_failed)?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        match read_record::<StandWorktreeRecord>(&path) {
            Ok(record) if record.file_name() == name => records.push((name, record)),
            Ok(_) => {
                return Err(unusable_record(
                    "former stand worktree record",
                    &path,
                    "binding check failed: record does not match this file name",
                ));
            }
            Err(RecordFailure::Missing) => {}
            Err(RecordFailure::Unusable(detail)) => {
                return Err(unusable_record(
                    "former stand worktree record",
                    &path,
                    &detail,
                ));
            }
        }
    }
    Ok(records)
}

/// A refusal before any effect, so a caller on a write path never reports it
/// as an uncertain outcome. Names the exact path, including any verbatim prefix.
fn unusable_record(kind: &str, path: &Path, detail: &str) -> Error {
    Error::new(
        "not_allowed",
        format!("{kind} at {} is unusable: {detail}", path.display()),
    )
}

/// Why a bounded stand record could not be used.
enum RecordFailure {
    /// The path has no entry.
    Missing,
    /// An entry exists but cannot be used. The detail never quotes file
    /// content, because a record carries the stand's creation token.
    Unusable(String),
}

fn read_record<T: serde::de::DeserializeOwned>(
    path: &Path,
) -> std::result::Result<T, RecordFailure> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            RecordFailure::Missing
        } else {
            RecordFailure::Unusable(format!("inspect failed: {error}"))
        }
    })?;
    if !metadata.is_file() || is_reparse_point(&metadata) || metadata.file_type().is_symlink() {
        return Err(RecordFailure::Unusable("not a plain file".to_owned()));
    }
    if metadata.len() > MAX_RECORD_BYTES {
        return Err(RecordFailure::Unusable(format!(
            "{} bytes exceeds the {MAX_RECORD_BYTES}-byte limit",
            metadata.len()
        )));
    }
    let file = File::open(path)
        .map_err(|error| RecordFailure::Unusable(format!("open failed: {error}")))?;
    let mut bytes = Vec::new();
    file.take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| RecordFailure::Unusable(format!("read failed: {error}")))?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(RecordFailure::Unusable(format!(
            "content exceeds the {MAX_RECORD_BYTES}-byte limit"
        )));
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        RecordFailure::Unusable(format!(
            "parse failed ({:?}) at line {}, column {}",
            error.classify(),
            error.line(),
            error.column()
        ))
    })
}

pub fn check_directory_chain(path: &Path) -> Result<()> {
    if path.components().any(|part| part == Component::ParentDir) {
        return Err(Error::new(
            "invalid",
            "stand path must not contain a parent-directory component",
        ));
    }
    for ancestor in path
        .ancestors()
        .filter(|ancestor| !ancestor.as_os_str().is_empty())
    {
        let metadata = fs::symlink_metadata(ancestor).map_err(|error| {
            Error::new(
                if error.kind() == std::io::ErrorKind::NotFound {
                    "not_found"
                } else {
                    "platform_io"
                },
                format!("inspect stand directory: {error}"),
            )
        })?;
        if !metadata.is_dir() || is_reparse_point(&metadata) || metadata.file_type().is_symlink() {
            return Err(Error::new(
                "not_allowed",
                "stand path must name a directory without symbolic links or reparse points",
            ));
        }
    }
    Ok(())
}

pub fn verify_stand_marker(root: &Path, registration: &StandRegistration) -> Result<()> {
    let path = root.join(STAND_MARKER);
    match read_record::<StandMarker>(&path) {
        Ok(marker)
            if marker.format == registration.marker.format
                && marker.root == registration.marker.root
                && marker.token == registration.marker.token =>
        {
            return Ok(());
        }
        Err(RecordFailure::Unusable(detail)) => {
            return Err(unusable_record("stand marker", &path, &detail));
        }
        Err(RecordFailure::Missing)
            if registration.removing_pid.is_some()
                && fs::read_dir(root).is_ok_and(|mut entries| entries.next().is_none()) =>
        {
            return Ok(());
        }
        _ => {}
    }
    Err(Error::new(
        "identity_changed",
        "stand ownership marker does not match the external registration",
    ))
}

fn owner_token_digest(token: &str) -> String {
    eliot_core::api::inference::sha256_hex(token.as_bytes())
}

/// The registry spelling of a stand root: the canonical parent joined with the
/// root's own name. The root component itself is never resolved, so a link
/// planted at the root cannot redirect the lookup.
pub fn stand_key_root(path: &Path) -> Result<PathBuf> {
    let path = std::path::absolute(path)
        .map_err(|error| Error::new("unavailable", format!("resolve stand root: {error}")))?;
    let name = path
        .file_name()
        .ok_or_else(|| Error::new("invalid", "stand path has no directory name"))?;
    let parent = path
        .parent()
        .ok_or_else(|| Error::new("invalid", "stand path has no parent directory"))?
        .canonicalize()
        .map_err(|error| Error::new("unavailable", format!("resolve stand parent: {error}")))?;
    Ok(parent.join(name))
}

/// A stand root opened by handle. Every ancestor stays held without delete
/// sharing, so no component can be renamed or swapped for a junction while
/// held, no component may be a reparse point, and files are read relative to
/// the held root. `stand up` holds one from creation through the owner bind.
pub struct PinnedStandRoot {
    root: PathBuf,
    dir: crate::platform::script_file::Directory,
    identity: String,
}

impl PinnedStandRoot {
    pub fn open(root: &Path) -> Result<Self> {
        let dir = crate::platform::script_file::Directory::open(root).map_err(|_| {
            Error::new(
                "not_allowed",
                "stand root must be a plain directory chain without links or reparse points",
            )
        })?;
        let identity = dir.identity()?;
        Ok(Self {
            root: root.to_owned(),
            dir,
            identity,
        })
    }

    /// Handle identity (volume serial and file index) of the held root.
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Read `name` relative to the held root; `None` when it does not exist.
    fn read(&self, name: &str) -> Result<Option<Vec<u8>>> {
        if fs::symlink_metadata(self.root.join(name))
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        {
            return Ok(None);
        }
        let mut file = self.dir.file(name).map_err(|_| {
            Error::new(
                "not_allowed",
                "stand file must be a plain single-link file inside the held root",
            )
        })?;
        crate::platform::script_file::read(&mut file).map(Some)
    }
}

/// Open the stand root by handle and prove it is the exact directory `stand
/// up` created and marked for `registration`. Returns the held root.
fn pinned_stand_root(root: &Path, registration: &StandRegistration) -> Result<PinnedStandRoot> {
    let pinned = PinnedStandRoot::open(root)?;
    let expected = registration.root_identity.as_deref().ok_or_else(|| {
        Error::new(
            "not_allowed",
            "stand registration carries no creation identity from stand up",
        )
    })?;
    if pinned.identity != expected {
        return Err(Error::new(
            "identity_changed",
            "stand root is not the directory stand up created",
        ));
    }
    let marker = pinned
        .read(STAND_MARKER)?
        .and_then(|bytes| serde_json::from_slice::<StandMarker>(&bytes).ok())
        .filter(|marker| {
            marker.format == registration.marker.format
                && marker.root == registration.marker.root
                && marker.token == registration.marker.token
        });
    if marker.is_none() {
        return Err(Error::new(
            "identity_changed",
            "stand ownership marker does not match the external registration",
        ));
    }
    Ok(pinned)
}

impl PinnedStandRoot {
    /// The owner token read from the held root, if minted.
    fn owner_token(&self) -> Result<Option<String>> {
        self.read("owner.token")?
            .map(|bytes| {
                String::from_utf8(bytes)
                    .map(|token| token.trim().to_owned())
                    .map_err(|_| Error::new("not_allowed", "stand owner token is not UTF-8"))
            })
            .transpose()
    }
}

/// Bind the registration to the owner token the stand's own daemon minted at
/// creation. Only `stand up` calls this while starting the daemon it just
/// created; `Ok(false)` means the daemon has not minted the token yet. The
/// protection check runs first, and the token is read from the same held
/// root whose identity and marker were verified.
pub fn bind_stand_owner_token(root: &Path) -> Result<bool> {
    if crate::operator::target_is_protected_from_stand_owner_auth(root) {
        return Err(Error::new(
            "not_allowed",
            "a stand operation cannot target a daemon root this process resolves as its own",
        ));
    }
    let root = stand_key_root(root)?;
    let registry = stand_registry(false)?;
    let name = registry_name(&root);
    let mut registration = read_stand_registration(&registry, &name, &root)?;
    crate::operator::refuse_issuing_root(&root, &registration)?;
    let pinned = pinned_stand_root(&root, &registration)?;
    // A token still being written (or unreadable) is "not minted yet": retry,
    // never bind anything but a complete token from the held root.
    let Ok(Some(token)) = pinned.owner_token() else {
        return Ok(false);
    };
    let digest = owner_token_digest(&token);
    let bound = match registration.owner_token_sha256.as_deref() {
        Some(bound) if bound == digest => true,
        Some(_) => {
            return Err(Error::new(
                "identity_changed",
                "stand owner token differs from its creation binding",
            ));
        }
        None => {
            registration.owner_token_sha256 = Some(digest);
            write_stand_registration(&registry, &name, &registration)?;
            true
        }
    };
    drop(pinned);
    Ok(bound)
}

/// Require the held root to be the directory `stand up` created, its marker
/// to match, and its owner token to be the one its daemon minted at stand
/// creation, so a copied or forged marker cannot qualify without the target's
/// own secret. Returns that token.
pub fn verify_stand_owner_binding(root: &Path, registration: &StandRegistration) -> Result<String> {
    let bound = registration.owner_token_sha256.as_deref().ok_or_else(|| {
        Error::new(
            "not_allowed",
            "stand registration carries no owner binding from stand up",
        )
    })?;
    let pinned = pinned_stand_root(root, registration)?;
    let token = pinned
        .owner_token()?
        .ok_or_else(|| Error::new("not_allowed", "stand has no owner token"))?;
    if owner_token_digest(&token) != bound {
        return Err(Error::new(
            "not_allowed",
            "stand owner token does not match its creation binding",
        ));
    }
    Ok(token)
}

fn worktree_records_dir(registry: &Path, registry_file: &str) -> PathBuf {
    registry.join(format!(
        "{}.worktrees",
        registry_file.trim_end_matches(".json")
    ))
}

/// Record a stand-created worktree in the external registration before Git
/// creates it. A non-stand daemon records nothing; a stand whose registration
/// cannot be verified refuses the creation.
pub(crate) fn record_stand_worktree(
    data_root: &Path,
    preparation: &WorkspacePreparation,
) -> Result<()> {
    match fs::symlink_metadata(data_root.join(STAND_MARKER)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(Error::new(
                "platform_io",
                format!("inspect stand marker: {error}"),
            ));
        }
        Ok(_) => {}
    }
    let root = stand_key_root(data_root)?;
    let name = registry_name(&root);
    // A stand an earlier build registered keeps its records beside that
    // registration until `stand down` moves them together.
    let located = locate_stand_registration(&root)?;
    let registration = located.record;
    verify_stand_marker(&root, &registration)?;
    let dir = worktree_records_dir(&located.registry, &name);
    if !dir.exists()
        && let Err(error) = create_stand_root(&dir)
        && !dir.exists()
    {
        return Err(error);
    }
    check_directory_chain(&dir)?;
    validate_stand_directory(&dir)?;
    let record = StandWorktreeRecord::of(preparation);
    let file = record.file_name();
    let path = dir.join(&file);
    match read_record::<StandWorktreeRecord>(&path) {
        Ok(existing) if existing == record => return Ok(()),
        Ok(_) => {
            return Err(Error::new(
                "identity_changed",
                "stand worktree record differs from this creation",
            ));
        }
        Err(RecordFailure::Unusable(detail)) => {
            return Err(unusable_record("stand worktree record", &path, &detail));
        }
        Err(RecordFailure::Missing) => {}
    }
    let bytes = serde_json::to_vec(&record)
        .map_err(|_| Error::new("internal", "encode stand worktree record"))?;
    write_stand_record(&dir, &file, &bytes)
}

/// Require this Store worktree's own external creation record. A missing or
/// different record is `unregistered_worktree`; one that exists but cannot be
/// used is refused with its path and failed step.
fn check_worktree_record(
    registry: &Path,
    registry_file: &str,
    preparation: &WorkspacePreparation,
) -> Result<()> {
    let expected = StandWorktreeRecord::of(preparation);
    let path = worktree_records_dir(registry, registry_file).join(expected.file_name());
    match read_record::<StandWorktreeRecord>(&path) {
        Ok(existing) if existing == expected => Ok(()),
        Ok(_) | Err(RecordFailure::Missing) => Err(unregistered_worktree()),
        Err(RecordFailure::Unusable(detail)) => {
            Err(unusable_record("stand worktree record", &path, &detail))
        }
    }
}

/// Every creation record that exists but cannot be used, including one no
/// Store worktree refers to, because teardown would otherwise delete it
/// unseen. Interrupted-write temporaries (dot-prefixed) are not records.
fn unusable_worktree_records(registry: &Path, registry_file: &str) -> Result<Vec<String>> {
    let dir = worktree_records_dir(registry, registry_file);
    let list_failed = |error: std::io::Error| {
        Error::new(
            "platform_io",
            format!("list stand worktree records at {}: {error}", dir.display()),
        )
    };
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(list_failed(error)),
    };
    let mut unusable = Vec::new();
    for entry in entries {
        let entry = entry.map_err(list_failed)?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        let detail = match read_record::<StandWorktreeRecord>(&path) {
            Ok(record) if record.file_name() == name => continue,
            Ok(_) => "binding check failed: record does not match this file name".to_owned(),
            // Removed while the sidecar was being listed.
            Err(RecordFailure::Missing) => continue,
            Err(RecordFailure::Unusable(detail)) => detail,
        };
        unusable.push(*unusable_record("stand worktree record", &path, &detail).message);
    }
    unusable.sort();
    Ok(unusable)
}

/// Remove the external registration and its worktree records after the
/// stand root is gone.
pub fn remove_stand_registration(registry: &Path, registry_file: &str) -> Result<()> {
    let io_fail = |error: std::io::Error| {
        Error::new("platform_io", format!("remove stand registration: {error}"))
    };
    let dir = worktree_records_dir(registry, registry_file);
    if let Ok(entries) = fs::read_dir(&dir) {
        for entry in entries {
            let path = entry.map_err(io_fail)?.path();
            if !fs::symlink_metadata(&path).map_err(io_fail)?.is_file() {
                return Err(Error::new(
                    "not_allowed",
                    "stand worktree records contain an unexpected entry",
                ));
            }
            fs::remove_file(path).map_err(io_fail)?;
        }
        fs::remove_dir(&dir).map_err(io_fail)?;
    }
    match fs::remove_file(registry.join(registry_file)) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(io_fail(error)),
        _ => Ok(()),
    }
}

pub fn check_nested_roots(root: &Path) -> Result<()> {
    let mut dirs = vec![root.to_owned()];
    while let Some(dir) = dirs.pop() {
        if dir != root
            && (dir.join(STAND_MARKER).exists()
                || (dir.join("owner.token").exists() && dir.join("daemon.lock").exists()))
        {
            return Err(Error::new(
                "nested_daemon_lock",
                format!(
                    "stand contains another daemon data directory at {}",
                    eliot_core::display_path(&dir.to_string_lossy())
                ),
            ));
        }
        for contents in fs::read_dir(&dir)
            .map_err(|error| Error::new("platform_io", format!("inspect stand files: {error}")))?
        {
            let entry = contents.map_err(|error| {
                Error::new("platform_io", format!("inspect stand file: {error}"))
            })?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|error| {
                Error::new("platform_io", format!("inspect stand file type: {error}"))
            })?;
            if is_reparse_point(&metadata) || metadata.file_type().is_symlink() {
                return Err(Error::new(
                    "not_allowed",
                    "stand directory contains a symbolic link or reparse point",
                ));
            }
            if metadata.is_dir() {
                dirs.push(path);
            }
        }
    }
    Ok(())
}

pub fn remove_offline_stand_root(root: &Path, offline: DaemonLock) -> Result<()> {
    let io_fail = |error| {
        Error::new(
            "platform_io",
            format!(
                "remove stand {}; retry stand down: {error}",
                eliot_core::display_path(&root.to_string_lossy())
            ),
        )
    };
    for entry in fs::read_dir(root).map_err(io_fail)? {
        let entry = entry.map_err(io_fail)?;
        if [STAND_MARKER, "daemon.lock", "operator.lock"]
            .iter()
            .any(|name| entry.file_name() == *name)
        {
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(io_fail)?;
        if metadata.is_dir() && (is_reparse_point(&metadata) || metadata.file_type().is_symlink()) {
            fs::remove_dir(path).map_err(io_fail)?;
        } else if metadata.is_dir() {
            crate::router::remove_dir_all(&path).map_err(io_fail)?;
        } else {
            fs::remove_file(path).map_err(io_fail)?;
        }
    }
    drop(offline);
    for name in ["daemon.lock", "operator.lock", STAND_MARKER] {
        match fs::remove_file(root.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_fail(error)),
        }
    }
    fs::remove_dir(root).map_err(io_fail)
}

/// Bounded offline tree walk for stand size calculation and link/nested-root
/// verification. Allows the stand's own `<root>/repo/.git` scratch directory.
pub(crate) fn stand_tree_bytes(root: &Path) -> Result<u64> {
    let mut stack = vec![(root.to_owned(), 0_usize)];
    let mut visited = 0_usize;
    let mut bytes = 0_u64;
    while let Some((dir, depth)) = stack.pop() {
        if depth >= MAX_STAND_DEPTH {
            return Err(Error::new(
                "unavailable",
                "stand directory exceeds depth bound",
            ));
        }
        for entry in fs::read_dir(&dir)
            .map_err(|error| Error::new("unavailable", format!("read stand directory: {error}")))?
        {
            visited += 1;
            if visited > MAX_STAND_ENTRIES {
                return Err(Error::new(
                    "unavailable",
                    "stand directory exceeds entry bound",
                ));
            }
            let entry = entry
                .map_err(|error| Error::new("unavailable", format!("read stand entry: {error}")))?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|error| {
                Error::new("unavailable", format!("inspect stand entry: {error}"))
            })?;
            if is_reparse_point(&metadata) || metadata.file_type().is_symlink() {
                return Err(Error::new(
                    "not_allowed",
                    "stand contains a symbolic link or reparse point",
                ));
            }
            if metadata.is_dir() {
                if path != root
                    && (path.join(STAND_MARKER).exists()
                        || (path.join("owner.token").exists() && path.join("daemon.lock").exists()))
                {
                    return Err(Error::new(
                        "nested_daemon_lock",
                        "stand contains another daemon data directory",
                    ));
                }
                stack.push((path, depth + 1));
            } else if metadata.is_file() {
                bytes = bytes
                    .checked_add(metadata.len())
                    .ok_or_else(|| Error::new("unavailable", "stand size exceeds byte bound"))?;
            }
        }
    }
    Ok(bytes)
}

fn is_external_repo(stand_root: &Path, repo_common_dir: &str) -> Result<bool> {
    let stand_canonical = fs::canonicalize(stand_root)
        .map_err(|error| Error::new("unavailable", format!("resolve stand root: {error}")))?;
    let stand_key = workspace_path_key(&stand_canonical.to_string_lossy()).into_owned();
    let repo_resolved = match fs::canonicalize(repo_common_dir) {
        Ok(path) => path,
        Err(_) => std::path::absolute(repo_common_dir).map_err(|error| {
            Error::new("unavailable", format!("resolve repository path: {error}"))
        })?,
    };
    let repo_key = workspace_path_key(&repo_resolved.to_string_lossy()).into_owned();
    Ok(!Path::new(&repo_key).starts_with(Path::new(&stand_key)))
}

fn load_external_workspaces(stand_root: &Path) -> Result<Vec<StandWorkspaceRecord>> {
    let db_path = stand_root.join("eliot.db");
    match fs::symlink_metadata(&db_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(Error::new(
                "unavailable",
                format!("inspect stand database: {error}"),
            ));
        }
        Ok(metadata) => {
            if !metadata.is_file()
                || is_reparse_point(&metadata)
                || metadata.file_type().is_symlink()
            {
                return Err(Error::new(
                    "not_allowed",
                    "stand database must be a regular file",
                ));
            }
        }
    }
    let all = Store::list_stand_workspaces_readonly(&db_path)?;
    let mut external = Vec::new();
    for record in all {
        if is_external_repo(stand_root, &record.preparation.repo_common_dir)? {
            external.push(record);
        }
    }
    Ok(external)
}

fn external_worktree_key(path: &str) -> String {
    workspace_path_key(path).into_owned()
}

fn external_branch_key(common_dir: &str, branch: &str) -> String {
    format!("{}::refs/heads/{branch}", workspace_path_key(common_dir))
}

fn check_stand_worktree_location(stand_root: &Path, worktree_path: &str) -> Result<()> {
    let path = Path::new(worktree_path);
    reject_links(path)?;
    let canonical_stand_worktrees =
        fs::canonicalize(stand_root.join("worktrees")).map_err(|_| {
            Error::new(
                "source_integrity",
                "stand worktree directory is unavailable",
            )
        })?;
    let canonical_worktree = match fs::canonicalize(path) {
        Ok(resolved) => resolved,
        Err(_) => std::path::absolute(path).map_err(|_| {
            Error::new(
                "source_integrity",
                "stand worktree path could not be resolved",
            )
        })?,
    };
    let root_key = workspace_path_key(&canonical_stand_worktrees.to_string_lossy()).into_owned();
    let wt_key = workspace_path_key(&canonical_worktree.to_string_lossy()).into_owned();
    let root_path = Path::new(&root_key);
    let wt_path = Path::new(&wt_key);
    if wt_path == root_path || !wt_path.starts_with(root_path) {
        return Err(Error::new(
            "not_allowed",
            "recorded stand worktree escapes stand worktrees directory",
        ));
    }
    Ok(())
}

async fn branch_exists(
    probe: &GitProbe,
    common: &Path,
    branch: &str,
    deadline: Instant,
) -> Result<bool> {
    let branch_ref = format!("refs/heads/{branch}");
    match branch_rev_parse(probe, common, &branch_ref, "branch_absent", deadline).await {
        Ok(_) => Ok(true),
        Err(error) if error.code == "branch_absent" => Ok(false),
        Err(error) => Err(error),
    }
}

async fn worktree_registry_entries(
    probe: &GitProbe,
    common: &Path,
    deadline: Instant,
) -> Result<(BTreeSet<String>, Vec<(String, Option<String>)>)> {
    let output = probe
        .workspace_command(common, &["worktree", "list", "--porcelain", "-z"], deadline)
        .await
        .map_err(|_| Error::new("unavailable", "external worktree registry is unreadable"))?;
    if output.code != 0 {
        return Err(Error::new(
            "unavailable",
            "external worktree registry could not be listed",
        ));
    }
    let text = std::str::from_utf8(&output.stdout)
        .map_err(|_| Error::new("unavailable", "external worktree registry is not UTF-8"))?;
    let mut keys = BTreeSet::new();
    let mut attachments = Vec::new();
    for record in text.split("\0\0").filter(|record| !record.is_empty()) {
        let mut fields = record.split('\0');
        let Some(wt_path) = fields
            .next()
            .and_then(|field| field.strip_prefix("worktree "))
        else {
            return Err(Error::new(
                "source_integrity",
                "external worktree registry entry omitted its path",
            ));
        };
        let key = external_worktree_key(wt_path);
        let mut branch = None;
        for field in fields {
            if let Some(name) = field.strip_prefix("branch ") {
                branch = Some(name.to_owned());
            }
        }
        keys.insert(key.clone());
        attachments.push((key, branch));
    }
    Ok((keys, attachments))
}

async fn worktree_present_or_registered(
    probe: &GitProbe,
    rec: &StandWorkspaceRecord,
    deadline: Instant,
) -> Result<bool> {
    let on_disk = match fs::symlink_metadata(&rec.preparation.worktree_path) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(Error::new(
                "unavailable",
                format!("inspect stand worktree: {error}"),
            ));
        }
    };
    if on_disk {
        return Ok(true);
    }
    let common = canonical_root(&rec.preparation.repo_common_dir)?;
    let (keys, _) = worktree_registry_entries(probe, &common, deadline).await?;
    Ok(keys.contains(&external_worktree_key(&rec.preparation.worktree_path)))
}

fn checked_stand_scratch_file(
    stand_root: &Path,
    preparation: &WorkspacePreparation,
    scratch: &[String],
    candidate: &Path,
) -> Result<PathBuf> {
    let managed = fs::canonicalize(stand_root.join("worktrees"))
        .map_err(|_| Error::new("not_allowed", "stand worktree root unavailable"))?;
    let workspace = canonical_root(&preparation.worktree_path)?;
    let source = canonical_root(&preparation.repo_path)?;
    if workspace == managed
        || !workspace.starts_with(&managed)
        || workspace == source
        || !candidate.is_absolute()
        || candidate
            .components()
            .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
    {
        return Err(Error::new(
            "not_allowed",
            "scratch file is outside its stand worktree",
        ));
    }
    reject_links(candidate)?;
    let metadata = fs::symlink_metadata(candidate)
        .map_err(|_| Error::new("unknown", "scratch file cannot be inspected"))?;
    if !metadata.file_type().is_file() || is_reparse_point(&metadata) {
        return Err(Error::new(
            "invalid",
            "scratch candidate is not a regular file",
        ));
    }
    let file = fs::canonicalize(candidate)
        .map_err(|_| Error::new("unknown", "scratch file cannot be resolved"))?;
    if !file.starts_with(&workspace) || file == workspace {
        return Err(Error::new(
            "not_allowed",
            "scratch file escaped its worktree",
        ));
    }
    for directory in scratch {
        let path = workspace.join(directory);
        reject_links(&path)?;
        if let Ok(root) = fs::canonicalize(&path)
            && root != workspace
            && root.starts_with(&workspace)
            && file != root
            && file.starts_with(&root)
        {
            return Ok(file);
        }
    }
    Err(Error::new(
        "not_allowed",
        "file is outside admitted scratch directories",
    ))
}

#[expect(
    clippy::disallowed_types,
    reason = "This helper runs the prepared non-force Git worktree removal command under OwnedChild custody."
)]
async fn run_prepared_stand_command(mut command: Command, deadline: Instant) -> Result<()> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = OwnedChild::spawn(command).map_err(|error| {
        Error::new(
            "unknown",
            format!("stand worktree removal command could not start: {error}"),
        )
    })?;
    let stdout = child.take_stdout();
    let stderr = child.take_stderr();
    let quiescent = if let (Some(stdout), Some(stderr)) = (stdout, stderr) {
        let capture = async {
            let (_, _, status) = tokio::try_join!(
                drain_stand_output(stdout),
                drain_stand_output(stderr),
                child.wait(),
            )?;
            Ok::<_, Error>(status)
        };
        match timeout_at(deadline, capture).await {
            Ok(Ok(_status)) => {
                #[cfg(windows)]
                {
                    matches!(
                        child.wait_family_empty(STAND_CUSTODY_CLEANUP_TIME).await,
                        Ok(true)
                    )
                }
                #[cfg(not(windows))]
                {
                    true
                }
            }
            _ => terminate_and_wait_empty(&mut child).await,
        }
    } else {
        terminate_and_wait_empty(&mut child).await
    };
    if !quiescent {
        return Err(Error::new(
            "unknown",
            "stand worktree removal child custody did not settle",
        ));
    }
    Ok(())
}

async fn terminate_and_wait_empty(child: &mut OwnedChild) -> bool {
    let signalled = child.terminate().is_ok();
    let deadline = Instant::now() + STAND_CUSTODY_CLEANUP_TIME;
    let reaped = matches!(timeout_at(deadline, child.wait()).await, Ok(Ok(_)));
    #[cfg(windows)]
    {
        signalled
            && reaped
            && matches!(
                child.wait_family_empty(STAND_CUSTODY_CLEANUP_TIME).await,
                Ok(true)
            )
    }
    #[cfg(not(windows))]
    {
        let _ = signalled;
        reaped
    }
}

async fn drain_stand_output<R: AsyncRead + Unpin>(mut stream: R) -> Result<()> {
    let mut buffer = vec![0_u8; 4096];
    let mut total = 0_usize;
    loop {
        let count = stream
            .read(&mut buffer)
            .await
            .map_err(|_| Error::new("unknown", "stand removal output read failed"))?;
        if count == 0 {
            return Ok(());
        }
        total = total.saturating_add(count);
        if total > STAND_OUTPUT_BYTES {
            return Err(Error::new(
                "unknown",
                "stand removal output exceeds capture bound",
            ));
        }
    }
}

pub(crate) async fn plan_external_git(
    stand_root: &Path,
    registry: &Path,
    registry_file: &str,
    registration: &StandRegistration,
) -> Result<()> {
    if registration.pending_write_guard.is_some() {
        return Err(Error::new(
            "pending_write_guard",
            "prior stand worktree removal requires stand recovery",
        ));
    }
    if let Some(message) = unusable_worktree_records(registry, registry_file)?
        .into_iter()
        .next()
    {
        return Err(Error::new("not_allowed", message));
    }
    let workspaces = load_external_workspaces(stand_root)?;
    if workspaces.is_empty() {
        return Ok(());
    }
    let git = WorkspaceGit::new()?;
    let probe = GitProbe::new()?;
    let deadline = Instant::now() + STAND_GIT_TIME;
    let mut planned_worktrees = BTreeSet::new();

    for rec in &workspaces {
        let wt_key = external_worktree_key(&rec.preparation.worktree_path);
        if registration.settled_external_worktrees.contains(&wt_key) {
            continue;
        }
        if !worktree_present_or_registered(&probe, rec, deadline).await? {
            continue;
        }
        check_worktree_record(registry, registry_file, &rec.preparation)?;
        check_stand_worktree_location(stand_root, &rec.preparation.worktree_path)?;
        Box::pin(git.removal_candidates(&rec.preparation, &rec.scratch, None, deadline)).await?;
        planned_worktrees.insert(wt_key);
    }

    let mut seen_branches = BTreeSet::new();
    for rec in &workspaces {
        let branch_key = external_branch_key(
            &rec.preparation.repo_common_dir,
            &rec.preparation.requested_branch,
        );
        if !seen_branches.insert(branch_key.clone())
            || registration.settled_external_branches.contains(&branch_key)
        {
            continue;
        }
        let common = canonical_root(&rec.preparation.repo_common_dir)?;
        if !branch_exists(&probe, &common, &rec.preparation.requested_branch, deadline).await? {
            continue;
        }
        check_worktree_record(registry, registry_file, &rec.preparation)?;
        let branch_ref = format!("refs/heads/{}", rec.preparation.requested_branch);
        let (_, attachments) = worktree_registry_entries(&probe, &common, deadline).await?;
        if attachments.iter().any(|(wt_key, attached_branch)| {
            attached_branch.as_deref() == Some(branch_ref.as_str())
                && !planned_worktrees.contains(wt_key)
        }) {
            return Err(Error::new(
                "branch_checked_out",
                "stand branch is checked out in another worktree",
            ));
        }
        crate::router::Router::pin_local_branch_tip(
            &rec.preparation.repo_path,
            &common,
            &rec.preparation.requested_branch,
            deadline,
        )
        .await?;
    }
    Ok(())
}

pub async fn cleanup_stand_external_git(
    stand_root: &Path,
    registry: &Path,
    registry_file: &str,
    registration: &mut StandRegistration,
) -> Result<StandExternalCleanupReport> {
    if let Some(pending) = registration.pending_write_guard.clone() {
        WorktreeWriteGuard::recover(Path::new(&pending.worktree_path), &pending.journal)?;
        registration.pending_write_guard = None;
        write_stand_registration(registry, registry_file, registration)?;
    }
    let unusable = unusable_worktree_records(registry, registry_file)?;
    let workspaces = load_external_workspaces(stand_root)?;
    let mut report = StandExternalCleanupReport::default();
    if workspaces.is_empty() {
        report.unusable_records = unusable;
        return Ok(report);
    }
    let git = WorkspaceGit::new()?;
    let probe = GitProbe::new()?;
    let deadline = Instant::now() + STAND_GIT_TIME;
    let mut blocked_branches = BTreeSet::new();

    // Pass 1: clean external worktrees.
    for rec in &workspaces {
        let wt_key = external_worktree_key(&rec.preparation.worktree_path);
        let branch_key = external_branch_key(
            &rec.preparation.repo_common_dir,
            &rec.preparation.requested_branch,
        );
        if registration.settled_external_worktrees.contains(&wt_key) {
            continue;
        }
        match worktree_present_or_registered(&probe, rec, deadline).await {
            Ok(false) => {
                registration.settled_external_worktrees.push(wt_key);
                write_stand_registration(registry, registry_file, registration)?;
                continue;
            }
            Err(error) => {
                blocked_branches.insert(branch_key);
                report.worktrees.push(StandWorktreeOutcome {
                    worktree_path: eliot_core::display_path(&rec.preparation.worktree_path),
                    repo_path: rec.preparation.repo_path.clone(),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: "kept".to_owned(),
                    reason: Some(error.code),
                    detail: None,
                });
                continue;
            }
            Ok(true) => {}
        }

        if let Err(error) = check_worktree_record(registry, registry_file, &rec.preparation)
            .and_then(|()| {
                check_stand_worktree_location(stand_root, &rec.preparation.worktree_path)
            })
        {
            blocked_branches.insert(branch_key);
            report.worktrees.push(StandWorktreeOutcome {
                worktree_path: eliot_core::display_path(&rec.preparation.worktree_path),
                repo_path: rec.preparation.repo_path.clone(),
                branch: rec.preparation.requested_branch.clone(),
                outcome: "kept".to_owned(),
                reason: Some(error.code),
                detail: Some(*error.message),
            });
            continue;
        }

        let mut guard = match WorktreeWriteGuard::prepare(Path::new(&rec.preparation.worktree_path))
        {
            Ok(guard) => guard,
            Err(error) => {
                blocked_branches.insert(branch_key);
                report.worktrees.push(StandWorktreeOutcome {
                    worktree_path: eliot_core::display_path(&rec.preparation.worktree_path),
                    repo_path: rec.preparation.repo_path.clone(),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: "kept".to_owned(),
                    reason: Some(error.code),
                    detail: None,
                });
                continue;
            }
        };
        let journal = guard.journal()?;
        registration.pending_write_guard = Some(StandWriteGuardJournal {
            worktree_path: rec.preparation.worktree_path.clone(),
            journal,
        });
        write_stand_registration(registry, registry_file, registration)?;

        if let Err(error) = guard.activate() {
            blocked_branches.insert(branch_key);
            if guard.restore().is_ok() {
                registration.pending_write_guard = None;
                write_stand_registration(registry, registry_file, registration)?;
                report.worktrees.push(StandWorktreeOutcome {
                    worktree_path: eliot_core::display_path(&rec.preparation.worktree_path),
                    repo_path: rec.preparation.repo_path.clone(),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: if error.code == "unknown" {
                        "unknown"
                    } else {
                        "kept"
                    }
                    .to_owned(),
                    reason: Some(error.code),
                    detail: None,
                });
            } else {
                report.worktrees.push(StandWorktreeOutcome {
                    worktree_path: eliot_core::display_path(&rec.preparation.worktree_path),
                    repo_path: rec.preparation.repo_path.clone(),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: "unknown".to_owned(),
                    reason: Some("workspace_write_guard_readback".to_owned()),
                    detail: None,
                });
            }
            continue;
        }

        let outcome = Box::pin(remove_guarded_stand_worktree(
            stand_root, &git, rec, &mut guard, deadline,
        ))
        .await;
        match outcome {
            GuardedOutcome::Removed => {
                if guard.restore().is_err() {
                    blocked_branches.insert(branch_key);
                    report.worktrees.push(StandWorktreeOutcome {
                        worktree_path: eliot_core::display_path(&rec.preparation.worktree_path),
                        repo_path: rec.preparation.repo_path.clone(),
                        branch: rec.preparation.requested_branch.clone(),
                        outcome: "unknown".to_owned(),
                        reason: Some("workspace_write_guard_readback".to_owned()),
                        detail: None,
                    });
                    continue;
                }
                registration.pending_write_guard = None;
                registration.settled_external_worktrees.push(wt_key);
                write_stand_registration(registry, registry_file, registration)?;
                report.worktrees.push(StandWorktreeOutcome {
                    worktree_path: eliot_core::display_path(&rec.preparation.worktree_path),
                    repo_path: rec.preparation.repo_path.clone(),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: "removed".to_owned(),
                    reason: None,
                    detail: None,
                });
            }
            GuardedOutcome::Kept(code) => {
                blocked_branches.insert(branch_key);
                if guard.restore().is_ok() {
                    registration.pending_write_guard = None;
                    write_stand_registration(registry, registry_file, registration)?;
                    report.worktrees.push(StandWorktreeOutcome {
                        worktree_path: eliot_core::display_path(&rec.preparation.worktree_path),
                        repo_path: rec.preparation.repo_path.clone(),
                        branch: rec.preparation.requested_branch.clone(),
                        outcome: "kept".to_owned(),
                        reason: Some(code),
                        detail: None,
                    });
                } else {
                    report.worktrees.push(StandWorktreeOutcome {
                        worktree_path: eliot_core::display_path(&rec.preparation.worktree_path),
                        repo_path: rec.preparation.repo_path.clone(),
                        branch: rec.preparation.requested_branch.clone(),
                        outcome: "unknown".to_owned(),
                        reason: Some("workspace_write_guard_readback".to_owned()),
                        detail: None,
                    });
                }
            }
            GuardedOutcome::Unknown {
                code,
                leave_guard_active,
            } => {
                if leave_guard_active {
                    guard.leave_active();
                } else if guard.restore().is_ok() {
                    registration.pending_write_guard = None;
                    write_stand_registration(registry, registry_file, registration)?;
                }
                blocked_branches.insert(branch_key);
                report.worktrees.push(StandWorktreeOutcome {
                    worktree_path: eliot_core::display_path(&rec.preparation.worktree_path),
                    repo_path: rec.preparation.repo_path.clone(),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: "unknown".to_owned(),
                    reason: Some(code),
                    detail: None,
                });
            }
        }
    }

    // Pass 2: detached, current-tip-contained external branches.
    let mut seen_branches = BTreeSet::new();
    for rec in &workspaces {
        let branch_key = external_branch_key(
            &rec.preparation.repo_common_dir,
            &rec.preparation.requested_branch,
        );
        if !seen_branches.insert(branch_key.clone())
            || registration.settled_external_branches.contains(&branch_key)
        {
            continue;
        }
        if blocked_branches.contains(&branch_key) {
            report.branches.push(StandBranchOutcome {
                repo_common_dir: eliot_core::display_path(&rec.preparation.repo_common_dir),
                branch: rec.preparation.requested_branch.clone(),
                outcome: "kept".to_owned(),
                reason: Some("worktree_attached".to_owned()),
                detail: None,
            });
            continue;
        }
        let common = match canonical_root(&rec.preparation.repo_common_dir) {
            Ok(common) => common,
            Err(error) => {
                report.branches.push(StandBranchOutcome {
                    repo_common_dir: eliot_core::display_path(&rec.preparation.repo_common_dir),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: "kept".to_owned(),
                    reason: Some(error.code),
                    detail: None,
                });
                continue;
            }
        };
        match branch_exists(&probe, &common, &rec.preparation.requested_branch, deadline).await {
            Ok(false) => {
                registration.settled_external_branches.push(branch_key);
                write_stand_registration(registry, registry_file, registration)?;
                continue;
            }
            Err(error) => {
                report.branches.push(StandBranchOutcome {
                    repo_common_dir: eliot_core::display_path(&rec.preparation.repo_common_dir),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: "kept".to_owned(),
                    reason: Some(error.code),
                    detail: None,
                });
                continue;
            }
            Ok(true) => {}
        }
        if let Err(error) = check_worktree_record(registry, registry_file, &rec.preparation) {
            report.branches.push(StandBranchOutcome {
                repo_common_dir: eliot_core::display_path(&rec.preparation.repo_common_dir),
                branch: rec.preparation.requested_branch.clone(),
                outcome: "kept".to_owned(),
                reason: Some(error.code),
                detail: Some(*error.message),
            });
            continue;
        }
        let tip = match crate::router::Router::pin_local_branch_tip(
            &rec.preparation.repo_path,
            &common,
            &rec.preparation.requested_branch,
            deadline,
        )
        .await
        {
            Ok(tip) => tip,
            Err(error) => {
                report.branches.push(StandBranchOutcome {
                    repo_common_dir: eliot_core::display_path(&rec.preparation.repo_common_dir),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: "kept".to_owned(),
                    reason: Some(error.code),
                    detail: None,
                });
                continue;
            }
        };
        let binding = CleanupGitBinding {
            repo: rec.preparation.repo_path.clone(),
            common: common.to_string_lossy().into_owned(),
            branch: rec.preparation.requested_branch.clone(),
            worktree: rec.preparation.worktree_path.clone(),
            admin: String::new(),
            admin_identity: String::new(),
            planned_tip: Some(tip.clone()),
        };
        match Box::pin(crate::router::Router::delete_local_branch(&binding, &tip)).await {
            Ok(()) => {
                registration.settled_external_branches.push(branch_key);
                write_stand_registration(registry, registry_file, registration)?;
                report.branches.push(StandBranchOutcome {
                    repo_common_dir: eliot_core::display_path(&rec.preparation.repo_common_dir),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: "removed".to_owned(),
                    reason: None,
                    detail: None,
                });
            }
            Err(error) => {
                report.branches.push(StandBranchOutcome {
                    repo_common_dir: eliot_core::display_path(&rec.preparation.repo_common_dir),
                    branch: rec.preparation.requested_branch.clone(),
                    outcome: if error.code == "unknown" {
                        "unknown"
                    } else {
                        "kept"
                    }
                    .to_owned(),
                    reason: Some(error.code),
                    detail: None,
                });
            }
        }
    }

    report.unusable_records = unusable
        .into_iter()
        .filter(|message| {
            !report
                .worktrees
                .iter()
                .any(|item| item.detail.as_ref() == Some(message))
                && !report
                    .branches
                    .iter()
                    .any(|item| item.detail.as_ref() == Some(message))
        })
        .collect();
    Ok(report)
}

enum GuardedOutcome {
    Removed,
    Kept(String),
    Unknown {
        code: String,
        leave_guard_active: bool,
    },
}

async fn remove_guarded_stand_worktree(
    stand_root: &Path,
    git: &WorkspaceGit,
    rec: &StandWorkspaceRecord,
    _guard: &mut WorktreeWriteGuard,
    deadline: Instant,
) -> GuardedOutcome {
    let (tip, files) = match timeout_at(
        deadline,
        Box::pin(git.removal_candidates(&rec.preparation, &rec.scratch, None, deadline)),
    )
    .await
    {
        Ok(Ok(pair)) => pair,
        Ok(Err(error)) => return GuardedOutcome::Kept(error.code),
        Err(_) => return GuardedOutcome::Kept("unavailable".to_owned()),
    };

    let mut mutated = false;
    for candidate in files {
        let verified = match checked_stand_scratch_file(
            stand_root,
            &rec.preparation,
            &rec.scratch,
            &candidate,
        ) {
            Ok(file) => file,
            Err(error) => {
                return if mutated {
                    GuardedOutcome::Unknown {
                        code: error.code,
                        leave_guard_active: false,
                    }
                } else {
                    GuardedOutcome::Kept(error.code)
                };
            }
        };
        mutated = true;
        if fs::remove_file(&verified).is_err() {
            return GuardedOutcome::Unknown {
                code: "workspace_scratch_delete_unknown".to_owned(),
                leave_guard_active: false,
            };
        }
    }

    let command = match timeout_at(
        deadline,
        Box::pin(git.prepare_stand_worktree_remove(&rec.preparation, &tip, deadline)),
    )
    .await
    {
        Ok(Ok(command)) => command,
        Ok(Err(error)) => {
            return if mutated {
                GuardedOutcome::Unknown {
                    code: error.code,
                    leave_guard_active: false,
                }
            } else {
                GuardedOutcome::Kept(error.code)
            };
        }
        Err(_) => {
            return if mutated {
                GuardedOutcome::Unknown {
                    code: "unavailable".to_owned(),
                    leave_guard_active: false,
                }
            } else {
                GuardedOutcome::Kept("unavailable".to_owned())
            };
        }
    };

    if let Err(error) = Box::pin(run_prepared_stand_command(command, deadline)).await {
        return GuardedOutcome::Unknown {
            code: error.code,
            leave_guard_active: true,
        };
    }

    match timeout_at(deadline, Box::pin(git.inspect_removal(&rec.preparation))).await {
        Ok(WorkspaceRemovalOutcome::Removed { .. }) => GuardedOutcome::Removed,
        Ok(WorkspaceRemovalOutcome::Unknown { code, .. }) => GuardedOutcome::Unknown {
            code,
            leave_guard_active: false,
        },
        Ok(WorkspaceRemovalOutcome::Failed { code, .. }) => GuardedOutcome::Unknown {
            code,
            leave_guard_active: false,
        },
        Err(_) => GuardedOutcome::Unknown {
            code: "workspace_removal_unconfirmed".to_owned(),
            leave_guard_active: false,
        },
    }
}

fn unregistered_worktree() -> Error {
    Error::new(
        "unregistered_worktree",
        "stand Store worktree has no matching external creation record",
    )
}

fn verify_stand_stopped(
    stand_root: &Path,
    registration: &StandRegistration,
) -> Result<Option<u32>> {
    let daemon = daemon_info(stand_root)?;
    if let Some(info) = &daemon
        && daemon_alive(info)?
    {
        return Err(Error::new(
            "daemon_lock_held",
            "a live stand daemon holds this directory",
        ));
    }
    if registration.removing_pid.is_some() {
        return Ok(registration.removing_pid.or(daemon.map(|info| info.pid)));
    }
    let Some(daemon) = daemon else {
        return Err(Error::new(
            "stand_unproven_exit",
            "stand lacks a clean exit record",
        ));
    };
    if crate::daemon::last_daemon_phase(stand_root).as_deref() != Some("daemon.stopped") {
        return Err(Error::new(
            "stand_unproven_exit",
            "stand exited without a clean shutdown record",
        ));
    }
    Ok(Some(daemon.pid))
}

pub(crate) async fn plan_stand_artifact(
    daemon_data_root: &Path,
    stand_root: &Path,
    expected_identity: &str,
) -> Result<u64> {
    if directory_identity(stand_root)? != expected_identity {
        return Err(Error::new(
            "identity_changed",
            "stand directory identity changed",
        ));
    }
    check_directory_chain(stand_root)?;
    check_nested_roots(stand_root)?;
    let registry = stand_registry(false)?;
    ensure_registry_is_external(stand_root, &registry)?;
    let reg_name = registry_name(stand_root);
    let registration = read_stand_registration(&registry, &reg_name, stand_root)?;
    verify_stand_marker(stand_root, &registration)?;
    refuse_parent_root(daemon_data_root, stand_root)?;
    crate::operator::verify_stand_target(stand_root, &registration)?;
    let _ = verify_stand_stopped(stand_root, &registration)?;
    Box::pin(plan_external_git(
        stand_root,
        &registry,
        &reg_name,
        &registration,
    ))
    .await?;
    stand_tree_bytes(stand_root)
}

pub(crate) async fn apply_stand_artifact(
    daemon_data_root: &Path,
    stand_root: &Path,
    expected_identity: &str,
) -> Result<u64> {
    if directory_identity(stand_root)? != expected_identity {
        return Err(Error::new(
            "identity_changed",
            "stand directory identity changed",
        ));
    }
    check_directory_chain(stand_root)?;
    check_nested_roots(stand_root)?;
    let registry = stand_registry(false)?;
    ensure_registry_is_external(stand_root, &registry)?;
    let reg_name = registry_name(stand_root);
    let _transition = lock_transition(stand_root)?;
    let mut registration = read_stand_registration(&registry, &reg_name, stand_root)?;
    verify_stand_marker(stand_root, &registration)?;
    refuse_parent_root(daemon_data_root, stand_root)?;
    crate::operator::verify_stand_target(stand_root, &registration)?;
    let pid = verify_stand_stopped(stand_root, &registration)?.unwrap_or_default();
    let offline = lock_offline(stand_root)?;
    check_directory_chain(stand_root)?;
    if directory_identity(stand_root)? != expected_identity {
        return Err(Error::new(
            "identity_changed",
            "stand directory identity changed",
        ));
    }
    registration = read_stand_registration(&registry, &reg_name, stand_root)?;
    verify_stand_marker(stand_root, &registration)?;
    crate::operator::verify_stand_target(stand_root, &registration)?;
    check_nested_roots(stand_root)?;
    if let Some(info) = daemon_info(stand_root)?
        && ((registration.removing_pid.is_none() && info.pid != pid) || daemon_alive(&info)?)
    {
        return Err(Error::new(
            "daemon_lock_held",
            "stand daemon state changed before removal",
        ));
    }
    let bytes = stand_tree_bytes(stand_root)?;
    let report = Box::pin(cleanup_stand_external_git(
        stand_root,
        &registry,
        &reg_name,
        &mut registration,
    ))
    .await?;
    if report.has_retained_or_unknown() {
        let (code, detail) = report.first_blocker_reason();
        return Err(Error::new(code, detail));
    }
    registration.removing_pid = Some(pid);
    write_stand_registration(&registry, &reg_name, &registration)?;
    remove_offline_stand_root(stand_root, offline)?;
    remove_stand_registration(&registry, &reg_name)?;
    Ok(bytes)
}

/// Parent cleanup never treats its own data root as a stand.
fn refuse_parent_root(daemon_data_root: &Path, stand_root: &Path) -> Result<()> {
    if crate::operator::data_dirs_match(daemon_data_root, stand_root)
        || fs::canonicalize(daemon_data_root)
            .ok()
            .zip(fs::canonicalize(stand_root).ok())
            .is_some_and(|(left, right)| crate::operator::data_dirs_match(&left, &right))
    {
        return Err(Error::new(
            "not_allowed",
            "parent cleanup cannot treat its own data root as a stand",
        ));
    }
    Ok(())
}

pub fn stand_now_ms() -> i64 {
    now_ms()
}
