//! Local process and directory custody used by the operator CLI.

use crate::platform::data_dir::{DataDir, sync_directory, validate_private_file};
use eliot_core::api::{AgentListRequest, AgentListResponse, AgentListRow};
use eliot_core::{Error, Result};
use eliot_store::{AgentListFilter, Store};
use std::fs;
use std::path::{Path, PathBuf};

pub use crate::config::profiles_edit::profile_draft_blocks;
pub use crate::daemon::run_for_update;
pub use crate::harness_capabilities::native_subagents_policy;
pub use crate::platform::data_dir::{DaemonInfo, DaemonLock, OperatorLock};
pub use crate::platform::data_dir::{READ_ONLY_STAND_STORE, read_only_stand_exists};
pub use crate::stand_cleanup::{
    LocatedRegistration, PinnedStandRoot, STAND_MARKER, StandBranchOutcome, StandEntry,
    StandExternalCleanupReport, StandMarker, StandRegistration, StandRegistryScan,
    StandWorktreeOutcome, UnusableStandRegistration, add_former_registrations,
    bind_stand_owner_token, check_directory_chain, check_nested_roots, cleanup_stand_external_git,
    ensure_registry_is_external, list_stand_registrations, locate_stand_registration,
    parse_stand_ttl_ms, read_stand_registration, registry_name, remove_offline_stand_root,
    remove_stand_registration, settle_former_registration, stand_base, stand_key_root,
    stand_now_ms, stand_registry, verify_stand_marker, verify_stand_owner_binding,
    write_stand_registration,
};
pub use crate::update_backup::{
    check_update_store_copy, copy_store_for_update_check, discard_update_backup,
    lock_update_publication, restore_update_backup, update_backup_path,
};
pub const STORE_SCHEMA_VERSION: i64 = eliot_store::Store::SCHEMA_VERSION;

/// Start a daemon process that is independent of the caller's Unix
/// session, with standard streams disconnected.
#[cfg(not(windows))]
#[allow(
    unsafe_code,
    reason = "The child calls async-signal-safe setsid before exec to leave the caller's session."
)]
pub fn start_detached_daemon(executable: &Path, data_dir: &Path) -> std::io::Result<()> {
    #[expect(
        clippy::disallowed_methods,
        clippy::disallowed_types,
        reason = "This statement constructs the Unix daemon process owned by this helper."
    )]
    let mut command = std::process::Command::new(executable);
    command
        .arg("--data-dir")
        .arg(data_dir)
        .arg("daemon")
        .arg("--foreground")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        // SAFETY: this pre-exec hook only invokes setsid, which is async-signal-safe.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
    }
    command.spawn().map(drop)
}

pub fn continue_after_restart(path: &Path) -> Result<bool> {
    Ok(crate::config::load_daemon_config(path)?.on_restart == crate::config::OnRestart::Continue)
}

/// Returns the issuing parent daemon data directory when running inside a
/// managed Agent session with complete inherited credentials.
pub fn inherited_agent_parent_data_dir() -> Option<PathBuf> {
    let has_agent_creds = [
        "ELIOT_AGENT_ID",
        "ELIOT_AGENT_GENERATION",
        "ELIOT_AGENT_TOKEN",
    ]
    .into_iter()
    .all(|name| std::env::var_os(name).is_some_and(|val| !val.is_empty()));
    if !has_agent_creds {
        return None;
    }
    let resolved = if let Some(dir) = std::env::var_os(crate::daemon::AGENT_DATA_DIR_ENV)
        .filter(|val| !val.is_empty())
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
    {
        dir
    } else {
        crate::platform::data_dir::resolve_data_dir(None).ok()?
    };
    if let Some(scope) = std::env::var_os(crate::daemon::AGENT_DATA_SCOPE_ENV)
        && scope != std::ffi::OsStr::new(&crate::daemon::identity_data_scope(&resolved))
    {
        return None;
    }
    if let Some(issuing) = std::env::var_os(crate::daemon::AGENT_DATA_DIR_ENV)
        && !crate::daemon::identity_data_dirs_equal(&resolved, Path::new(&issuing))
    {
        return None;
    }
    Some(resolved)
}

/// Checks whether `candidate` matches `expected` data directory identity.
pub fn data_dirs_match(left: &Path, right: &Path) -> bool {
    crate::daemon::identity_data_dirs_equal(left, right)
}

fn same_root(target: &Path, candidate: &Path) -> bool {
    let canonical = std::fs::canonicalize(target).unwrap_or_else(|_| target.to_path_buf());
    data_dirs_match(target, candidate)
        || data_dirs_match(&canonical, candidate)
        || std::fs::canonicalize(candidate)
            .is_ok_and(|canon_candidate| data_dirs_match(&canonical, &canon_candidate))
}

/// Returns whether `target` is a daemon root this process could resolve as
/// its own (`--data-dir`-less resolution, `ELIOT_HOME`, the durable selection
/// and the default, from the environment and the account profile alike).
/// The issuing parent comes only from the stand registration
/// ([`refuse_issuing_root`]); live daemons need a full stand binding
/// ([`verify_stand_target`]).
pub fn target_is_protected_from_stand_owner_auth(target: &Path) -> bool {
    crate::platform::data_dir::protected_daemon_roots()
        .iter()
        .any(|dir| same_root(target, dir))
}

/// Refuse a target that is the daemon `stand up` recorded as the stand's
/// issuing parent. The persisted root is used, never inherited environment.
pub fn refuse_issuing_root(
    target: &Path,
    registration: &crate::stand_cleanup::StandRegistration,
) -> Result<()> {
    if registration
        .parent_data_dir
        .as_deref()
        .is_some_and(|parent| same_root(target, parent))
    {
        return Err(Error::new(
            "not_allowed",
            "a stand operation cannot target the stand's issuing daemon",
        ));
    }
    Ok(())
}

/// Refuse any target that is not a stand bound by `stand up`: a protected
/// daemon root, the recorded issuing daemon, or a root that is not the
/// handle-identified directory `stand up` created with its marker and owner
/// token. A root with a live (or undeterminable) daemon never qualifies
/// without that full binding. Returns the owner token. During an interrupted
/// removal (`removing_pid` recorded, no live daemon, no owner token left)
/// only the root protections apply and no token is returned.
pub fn verify_stand_target(
    root: &Path,
    registration: &crate::stand_cleanup::StandRegistration,
) -> Result<Option<String>> {
    if target_is_protected_from_stand_owner_auth(root) {
        return Err(Error::new(
            "not_allowed",
            "a stand operation cannot target a daemon root this process resolves as its own",
        ));
    }
    refuse_issuing_root(root, registration)?;
    let live = match daemon_info(root) {
        Ok(None) => false,
        Ok(Some(info)) => daemon_alive(&info).unwrap_or(true),
        Err(_) => true,
    };
    if !live
        && registration.removing_pid.is_some()
        && std::fs::symlink_metadata(root.join("owner.token"))
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
    {
        return Ok(None);
    }
    match crate::stand_cleanup::verify_stand_owner_binding(root, registration) {
        Ok(token) => Ok(Some(token)),
        Err(error) if live => Err(Error::new(
            "not_allowed",
            format!(
                "target runs a live daemon that is not a stand created by this registry: {}",
                error.message
            ),
        )),
        Err(error) => Err(error),
    }
}

pub fn daemon_info(path: &Path) -> Result<Option<DaemonInfo>> {
    DataDir::open_existing(path)?.read_daemon_info()
}

pub fn lock_offline(path: &Path) -> Result<DaemonLock> {
    DataDir::open_existing(path)?.acquire_daemon_lock()
}

pub fn lock_transition(path: &Path) -> Result<OperatorLock> {
    DataDir::open_existing(path)?.acquire_operator_lock(true)
}

pub fn create_stand_root(path: &Path) -> Result<()> {
    crate::platform::data_dir::refuse_unverified_write(path)?;
    #[cfg(windows)]
    {
        if !crate::platform::windows::create_private_directory(path)? {
            return Err(Error::new(
                "stand_exists",
                "stand root must be a fresh directory",
            ));
        }
        Ok(())
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(path)
            .map_err(|error| Error::new("platform_io", format!("create private stand: {error}")))
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "platform_unsupported",
            "private stands are unsupported on this platform",
        ))
    }
}

pub fn validate_stand_profiles(source: &[u8]) -> Result<()> {
    let format_error = || {
        Error::new(
            "invalid",
            "stand profile file must contain a TOML [profiles] table and at most a [routing] table",
        )
    };
    let text = std::str::from_utf8(source).map_err(|_| format_error())?;
    let table = text.parse::<toml::Table>().map_err(|_| format_error())?;
    let only_known = table
        .iter()
        .all(|(name, value)| matches!(name.as_str(), "profiles" | "routing") && value.is_table());
    if !only_known || !table.contains_key("profiles") {
        return Err(format_error());
    }
    Ok(())
}

pub fn select_stand_profiles(source: &[u8], names: &[String]) -> Result<Vec<u8>> {
    let invalid = || {
        Error::new(
            "invalid",
            "stand settings must be a UTF-8 TOML file with a [profiles] table",
        )
    };
    let text = std::str::from_utf8(source).map_err(|_| invalid())?;
    let mut settings: toml::Table = text.parse().map_err(|_| invalid())?;
    let Some(toml::Value::Table(mut profiles)) = settings.remove("profiles") else {
        return Err(invalid());
    };
    let mut selected = toml::Table::new();
    for name in names {
        if selected.contains_key(name) {
            return Err(Error::new(
                "invalid",
                format!("duplicate stand profile {name:?}"),
            ));
        }
        let profile = profiles.remove(name).ok_or_else(|| {
            Error::new(
                "not_found",
                format!("settings profile {name:?} was not found"),
            )
        })?;
        selected.insert(name.clone(), profile);
    }
    let output = toml::Table::from_iter([("profiles".to_owned(), toml::Value::Table(selected))]);
    toml::to_string(&output)
        .map(String::into_bytes)
        .map_err(|_| invalid())
}

pub fn copy_stand_read_only(source: &Path, stand: &Path) -> Result<()> {
    let target = stand.join(READ_ONLY_STAND_STORE);
    if read_only_stand_exists(stand)? {
        return Err(Error::new("invalid", "read-only stand copy already exists"));
    }
    // Keep the same external registry and owner binding as a live stand.
    DataDir::open(stand)?;
    let id = ulid::Ulid::new().to_string();
    let copy = crate::update_backup::copy_store_to_private_root(source, stand, &id)?;
    fs::rename(copy, target).map_err(|error| {
        Error::new(
            "platform_io",
            format!("install read-only Store copy: {error}"),
        )
    })?;
    sync_directory(stand)
}

fn open_stand_copy(stand: &Path) -> Result<Store> {
    let data = DataDir::open_existing(stand)?;
    let path = data.path().join(READ_ONLY_STAND_STORE);
    validate_private_file(&path)?;
    Store::open_read_only(&path)
}

pub fn stand_copy_event_high_water(stand: &Path) -> Result<i64> {
    open_stand_copy(stand)?.high_water()
}

pub fn stand_copy_agents(stand: &Path, request: &AgentListRequest) -> Result<AgentListResponse> {
    request.validate()?;
    let filter = AgentListFilter {
        archived: request.archived,
        order: request.order,
        state: request.state,
        states: request.states.clone(),
        project: request.project.clone(),
        project_scope: request.project_scope.clone(),
        harness: request.harness.clone(),
        parent: request.parent.clone(),
        root: request.root.clone(),
        query: request.query.clone(),
    };
    let page = open_stand_copy(stand)?.list_agent_summaries(
        &filter,
        request.after.as_deref(),
        request.limit,
        crate::common::now_ms(),
        &Default::default(),
    )?;
    let result = AgentListResponse {
        state_totals: page.state_totals,
        agents: page
            .summaries
            .into_iter()
            .map(crate::router::display_agent_summary)
            .map(|summary| AgentListRow {
                summary,
                observation: None,
            })
            .collect(),
        next: page.next,
        total: page.total,
        truncated: page.truncated,
    };
    request.validate_response(&result)?;
    Ok(result)
}

pub fn validate_stand_directory(path: &Path) -> Result<()> {
    DataDir::open_existing(path)?;
    Ok(())
}

pub fn write_stand_record(path: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    DataDir::open_existing(path)?;
    crate::platform::data_dir::replace_file_atomically(path, name, bytes, true)
}

pub fn write_stand_config(root: &Path, bytes: &[u8]) -> Result<()> {
    let config = [
        bytes,
        b"\n",
        crate::config::NEW_STORE_RETENTION_CONFIG.as_bytes(),
    ]
    .concat();
    crate::platform::data_dir::replace_private_file(root, "config.toml", &config)
}

pub fn daemon_alive(info: &DaemonInfo) -> Result<bool> {
    #[cfg(windows)]
    return crate::platform::windows::recorded_process_alive(
        info.pid,
        birth_time(info, "windows-creation-filetime:")?,
    );

    #[cfg(target_os = "linux")]
    return crate::platform::unix::recorded_process_alive(
        info.pid,
        birth_time(info, "linux-start-ticks:")?,
    );

    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = info;
        Err(Error::new(
            "platform_unsupported",
            "cannot prove daemon process identity",
        ))
    }
}

pub fn running_binary(info: &DaemonInfo) -> Result<PathBuf> {
    #[cfg(windows)]
    return crate::platform::windows::recorded_process_image(
        info.pid,
        birth_time(info, "windows-creation-filetime:")?,
    )?
    .canonicalize()
    .map_err(|error| Error::new("platform_io", format!("resolve previous release: {error}")));
    #[cfg(target_os = "linux")]
    {
        let path = std::fs::read_link(format!("/proc/{}/exe", info.pid)).map_err(|error| {
            Error::new("platform_io", format!("resolve previous release: {error}"))
        })?;
        if !daemon_alive(info)? || !path.is_file() {
            return Err(Error::new(
                "process_identity",
                "previous release is unavailable",
            ));
        }
        Ok(path)
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = info;
        Err(Error::new(
            "platform_unsupported",
            "cannot identify the previous release",
        ))
    }
}

#[cfg(windows)]
pub fn candidate_birth(pid: u32) -> Result<u64> {
    crate::platform::windows::process_start_time(pid)
}

#[cfg(windows)]
pub fn stop_candidate(pid: u32, birth: u64) -> Result<()> {
    crate::platform::windows::terminate_recorded_process(pid, birth)
}

#[cfg(windows)]
pub fn stop_published_candidate(info: &DaemonInfo) -> Result<()> {
    stop_candidate(info.pid, birth_time(info, "windows-creation-filetime:")?)
}

/// Preserve the directory spelling given to the old process. On Windows a
/// verbatim path can be the same directory while retaining a different native
/// session scope in a legacy adapter.
pub fn replacement_data_dir(path: &Path, info: &DaemonInfo) -> Result<PathBuf> {
    #[cfg(windows)]
    {
        let argv = crate::platform::windows::recorded_process_argv(
            info.pid,
            birth_time(info, "windows-creation-filetime:")?,
        )?;
        let mut args = argv.iter().skip(1);
        while let Some(arg) = args.next() {
            let value = if arg == "--data-dir" {
                args.next().map(String::as_str)
            } else {
                arg.strip_prefix("--data-dir=")
            };
            let Some(reported) = value.map(Path::new).filter(|path| path.is_absolute()) else {
                continue;
            };
            let canonical = |path: &Path| {
                path.canonicalize().map_err(|error| {
                    Error::new(
                        "platform_io",
                        format!("resolve daemon data directory: {error}"),
                    )
                })
            };
            if !crate::daemon::identity_data_dirs_equal(&canonical(path)?, &canonical(reported)?) {
                return Err(Error::new(
                    "process_identity",
                    "running daemon names another data directory",
                ));
            }
            return Ok(reported.to_owned());
        }
    }
    #[cfg(not(windows))]
    let _ = info;
    Ok(path.to_owned())
}

#[cfg(any(windows, target_os = "linux"))]
fn birth_time(info: &DaemonInfo, prefix: &str) -> Result<u64> {
    info.process_birth
        .strip_prefix(prefix)
        .and_then(|value| value.parse().ok())
        .filter(|value| *value != 0)
        .ok_or_else(|| {
            Error::new(
                "daemon_metadata",
                "daemon process birth identity is invalid",
            )
        })
}

pub fn binary_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    use std::io::{BufRead, BufReader};
    let file = std::fs::File::open(path)
        .map_err(|error| Error::new("platform_io", format!("open release binary: {error}")))?;
    let mut reader = BufReader::new(file);
    let mut hash = Sha256::new();
    loop {
        let bytes = reader
            .fill_buf()
            .map_err(|error| Error::new("platform_io", format!("hash release binary: {error}")))?;
        if bytes.is_empty() {
            break;
        }
        hash.update(bytes);
        let len = bytes.len();
        reader.consume(len);
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

#[cfg(windows)]
const DESKTOP_RECORD: &str = "operator-desktop.json";

#[cfg(windows)]
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum DesktopRecord {
    Starting,
    Started(DesktopProcess),
}

#[cfg(windows)]
#[derive(serde::Deserialize, serde::Serialize)]
struct DesktopProcess {
    pid: u32,
    start_time: u64,
    sha256: String,
}

#[cfg(windows)]
fn desktop_record(path: &Path) -> Result<Option<DesktopRecord>> {
    let root = DataDir::open_existing(path)?;
    let file = root.path().join(DESKTOP_RECORD);
    match std::fs::symlink_metadata(&file) {
        Ok(_) => crate::platform::data_dir::validate_private_file(&file)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(Error::new(
                "platform_io",
                format!("inspect desktop ownership: {error}"),
            ));
        }
    }
    let bytes = std::fs::read(file)
        .map_err(|error| Error::new("platform_io", format!("read desktop ownership: {error}")))?;
    let record = serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("process_identity", "desktop ownership record is invalid"))?;
    Ok(Some(record))
}

/// Stop only a desktop previously started by this operator for this data root.
#[cfg(windows)]
pub fn stop_desktop(path: &Path) -> Result<()> {
    match desktop_record(path)? {
        Some(DesktopRecord::Starting) => Err(Error::new(
            "process_identity",
            "previous desktop startup has unresolved process custody",
        )),
        Some(DesktopRecord::Started(process)) => close_desktop(&process),
        None => Ok(()),
    }
}

#[cfg(windows)]
fn close_desktop(record: &DesktopProcess) -> Result<()> {
    if !crate::platform::windows::recorded_process_alive(record.pid, record.start_time)? {
        return Ok(());
    }
    const CLOSE: &str = r#"
$ErrorActionPreference = 'Stop'
$request = [Console]::In.ReadToEnd() | ConvertFrom-Json
$process = Get-Process -Id ([int]$request.pid) -ErrorAction SilentlyContinue
if ($null -eq $process) { exit 0 }
$null = $process.Handle
if ($process.StartTime.ToFileTimeUtc() -ne [long]$request.start_time) { exit 0 }
if ($process.HasExited) { exit 0 }
if (!$process.CloseMainWindow() -or !$process.WaitForExit(5000)) { exit 1 }
"#;
    powershell(CLOSE, record)?;
    if crate::platform::windows::recorded_process_alive(record.pid, record.start_time)? {
        return Err(Error::new(
            "desktop_busy",
            format!(
                "desktop PID {} did not close; finish its pending interaction and retry",
                record.pid
            ),
        ));
    }
    Ok(())
}

/// Start the release companion after the daemon is ready.
#[cfg(windows)]
pub fn start_desktop(path: &Path, executable: &Path, sha256: String) -> Result<u32> {
    if let Some(DesktopRecord::Started(record)) = desktop_record(path)?
        && record.sha256 == sha256
        && crate::platform::windows::recorded_process_alive(record.pid, record.start_time)?
    {
        return Ok(record.pid);
    }
    stop_desktop(path)?;
    // If launch or the later write is interrupted, teardown must retain the
    // directory until the companion's exit can be established.
    write_desktop_record(path, &DesktopRecord::Starting)?;
    let pid = start_detached(executable, &[], false, Some(path))?;
    let start_time = crate::platform::windows::process_start_time(pid)?;
    let record = DesktopRecord::Started(DesktopProcess {
        pid,
        start_time,
        sha256,
    });
    if let Err(error) = write_desktop_record(path, &record) {
        if let DesktopRecord::Started(process) = record {
            close_desktop(&process)?;
        }
        std::fs::remove_file(path.join(DESKTOP_RECORD)).map_err(|error| {
            Error::new(
                "platform_io",
                format!("clear desktop startup marker: {error}"),
            )
        })?;
        return Err(error);
    }
    Ok(pid)
}

#[cfg(windows)]
fn write_desktop_record(path: &Path, record: &DesktopRecord) -> Result<()> {
    let bytes = serde_json::to_vec(record).map_err(|_| {
        Error::new(
            "process_identity",
            "desktop ownership record cannot be encoded",
        )
    })?;
    crate::platform::data_dir::replace_file_atomically(path, DESKTOP_RECORD, &bytes, true)
}

/// Start outside the caller's Windows Job through local WMI. Send argv and
/// environment over stdin, keeping their values out of the helper command line.
#[cfg(windows)]
pub fn start_detached(
    executable: &Path,
    args: &[&std::ffi::OsStr],
    hidden: bool,
    home: Option<&Path>,
) -> Result<u32> {
    const WMI_START: &str = r#"
$ErrorActionPreference = 'Stop'
[Console]::InputEncoding = [System.Text.Encoding]::UTF8
$request = [Console]::In.ReadToEnd() | ConvertFrom-Json
$startup = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{
    ShowWindow = [uint16]$request.show_window
    EnvironmentVariables = [string[]]$request.environment
}
$created = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -OperationTimeoutSec 15 -Arguments @{
    CommandLine = [string]$request.command_line
    CurrentDirectory = [string]$request.directory
    ProcessStartupInformation = $startup
}
if ($created.ReturnValue -ne 0) {
    [Console]::Error.WriteLine([string]$created.ReturnValue)
    exit 1
}
[Console]::Out.WriteLine([string]$created.ProcessId)
"#;
    let executable =
        eliot_core::display_path(executable.to_str().ok_or_else(|| {
            Error::new("invalid", "release binary path contains invalid Unicode")
        })?);
    let executable = Path::new(&executable);
    let mut argv = vec![executable.as_os_str()];
    argv.extend_from_slice(args);
    let mut line = crate::platform::windows::windows_command_line(&argv)?;
    line.pop();
    let line = String::from_utf16(&line)
        .map_err(|_| Error::new("invalid", "release command contains invalid Unicode"))?;
    let directory = executable
        .parent()
        .ok_or_else(|| Error::new("invalid", "release binary must have a parent directory"))?;
    let mut environment: Vec<_> = std::env::vars_os()
        .filter_map(|(name, value)| {
            let name = name.into_string().ok()?;
            // A managed Agent's Swarm environment belongs to its issuing daemon.
            (!name.to_ascii_uppercase().starts_with("ELIOT_"))
                .then(|| format!("{name}={}", value.to_string_lossy()))
        })
        .collect();
    if let Some(home) = home {
        environment.push(format!("ELIOT_HOME={}", home.display()));
    }
    let request = serde_json::json!({
        "command_line": line,
        "directory": directory,
        "show_window": if hidden { 0 } else { 7 },
        "environment": environment,
    });
    let output = powershell(WMI_START, &request)?;
    if !output.status.success() {
        let code = String::from_utf8_lossy(&output.stderr)
            .trim()
            .parse::<u32>();
        let detail = code.map_or_else(|_| String::new(), |code| format!(" (WMI result {code})"));
        return Err(Error::new(
            "platform_io",
            format!("WMI could not start the release process{detail}"),
        ));
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u32>()
        .ok()
        .filter(|pid| *pid != 0)
        .ok_or_else(|| Error::new("process_identity", "WMI did not return a process ID"))
}

#[cfg(windows)]
#[expect(
    clippy::disallowed_types,
    reason = "This Windows operator helper owns the PowerShell child command."
)]
fn powershell(script: &str, request: &impl serde::Serialize) -> Result<std::process::Output> {
    use std::io::Write;
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};

    let request = serde_json::to_vec(request)
        .map_err(|_| Error::new("invalid", "operator process request could not be encoded"))?;
    let system_root = std::env::var_os("SystemRoot")
        .ok_or_else(|| Error::new("platform_io", "Windows system directory is unavailable"))?;
    let powershell =
        Path::new(&system_root).join(r"System32\WindowsPowerShell\v1.0\powershell.exe");
    #[expect(
        clippy::disallowed_methods,
        reason = "This statement constructs a process owned by this module."
    )]
    let mut helper = Command::new(powershell)
        .args(["-NoProfile", "-NonInteractive", "-Command", script])
        .creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| Error::new("platform_io", format!("start WMI helper: {error}")))?;
    let sent = helper
        .stdin
        .take()
        .expect("WMI helper has piped stdin")
        .write_all(&request);
    let output = helper
        .wait_with_output()
        .map_err(|error| Error::new("platform_io", format!("wait for WMI launch: {error}")))?;
    sent.map_err(|error| Error::new("platform_io", format!("write WMI launch request: {error}")))?;
    Ok(output)
}
