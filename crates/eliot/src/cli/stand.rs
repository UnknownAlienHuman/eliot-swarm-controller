//! Own the directories and processes of a fresh private proof stand.

use super::operator::{start_owned, stop_and_drain};
use super::support::{Failure, print_value};
use eliot_daemon::operator::{
    create_stand_root, daemon_alive, daemon_info, lock_offline, lock_transition,
    validate_stand_directory, write_stand_record,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

const MARKER: &str = ".eliot-stand.json";
const MAX_PROFILE_BYTES: u64 = 1024 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StandMarker {
    format: u32,
    root: PathBuf,
    token: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Registration {
    marker: StandMarker,
    removing_pid: Option<u32>,
}

pub(crate) async fn up(
    root: Option<PathBuf>,
    profile: Option<PathBuf>,
    binary: Option<PathBuf>,
    json_output: bool,
) -> Result<u8, Failure> {
    let (base, registry) = stand_registry(true)?;
    let root = root.unwrap_or_else(|| base.join(format!("swarm-stand-{}", ulid::Ulid::new())));
    let root =
        std::path::absolute(root).map_err(|error| io_failure("resolve stand path", error))?;
    let parent = root
        .parent()
        .ok_or_else(|| Failure::new(2, "stand path has no parent directory"))?;
    let name = root
        .file_name()
        .ok_or_else(|| Failure::new(2, "stand path has no directory name"))?;
    check_directory_chain(parent)?;
    let parent = parent
        .canonicalize()
        .map_err(|error| io_failure("resolve stand parent", error))?;
    for ancestor in parent.ancestors() {
        if ancestor.join("daemon.lock").exists() || ancestor.join(MARKER).exists() {
            return Err(Failure::new(
                2,
                "a stand must be outside existing daemon data directories",
            ));
        }
    }
    let root = parent.join(name);
    ensure_registry_is_external(&root, &registry)?;
    let name = registry_name(&root);
    if registry.join(&name).exists() {
        return Err(Failure::new(2, "this path has a prior stand registration"));
    }
    let binary = match binary {
        Some(binary) => binary,
        None => std::env::current_exe().map_err(|error| io_failure("locate Swarm", error))?,
    }
    .canonicalize()
    .map_err(|error| io_failure("resolve stand binary", error))?;
    if !binary.is_file() {
        return Err(Failure::new(2, "stand binary must be a file"));
    }
    let profile = profile.as_deref().map(read_profile).transpose()?;

    create_stand_root(&root).map_err(Failure::for_read)?;
    let root = root
        .canonicalize()
        .map_err(|error| io_failure("resolve created stand", error))?;
    let workspace = root.join("repo");
    if let Err(failure) = prepare(&root, &workspace, profile).await {
        fs::remove_dir_all(&root).map_err(|error| {
            io_failure(
                &format!(
                    "{}; remove incomplete stand {}",
                    failure.message,
                    root.display()
                ),
                error,
            )
        })?;
        return Err(failure);
    }
    let _transition = lock_transition(&root).map_err(Failure::for_read)?;
    let registration = Registration {
        marker: StandMarker {
            format: 2,
            root: root.clone(),
            token: format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new()),
        },
        removing_pid: None,
    };
    let mut marker = File::create_new(root.join(MARKER))
        .map_err(|error| io_failure("create stand marker", error))?;
    let bytes = serde_json::to_vec(&registration.marker)
        .map_err(|_| Failure::new(1, "encode stand marker"))?;
    marker
        .write_all(&bytes)
        .and_then(|()| marker.sync_all())
        .map_err(|error| io_failure("write stand marker", error))?;
    write_record(&registry, &name, &registration)?;
    let pid = start_owned(&root, &binary).await.map_err(|mut failure| {
        failure.message = format!("{}; stand retained at {}", failure.message, root.display());
        failure
    })?;
    let data_dir = display(&root);
    let workspace = display(&workspace);
    if json_output {
        print_value(
            &json!({"state": "ready", "data_dir": data_dir, "workspace": workspace, "pid": pid}),
            true,
        );
    } else {
        println!("Stand ready at {data_dir} (daemon PID {pid})");
        println!("Scratch Git repository at {workspace}");
        println!("swarm --data-dir \"{data_dir}\" status");
        println!("swarm stand down \"{data_dir}\"");
    }
    Ok(0)
}

pub(crate) async fn down(path: PathBuf, json_output: bool) -> Result<u8, Failure> {
    let path =
        std::path::absolute(path).map_err(|error| io_failure("resolve stand path", error))?;
    let (_, registry) = stand_registry(false)?;
    let parent = path
        .parent()
        .ok_or_else(|| Failure::new(2, "stand path has no parent"))?;
    check_directory_chain(parent)?;
    let name = path
        .file_name()
        .ok_or_else(|| Failure::new(2, "stand path has no name"))?;
    let root = parent
        .canonicalize()
        .map_err(|error| io_failure("resolve stand parent", error))?
        .join(name);
    let name = registry_name(&root);
    ensure_registry_is_external(&root, &registry)?;
    let mut record = registered_stand(&registry, &name, &root)?;
    match fs::symlink_metadata(&root) {
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound && record.removing_pid.is_some() =>
        {
            fs::remove_file(registry.join(name))
                .map_err(|error| io_failure("remove stand registration", error))?;
            return removed(&root, record.removing_pid.unwrap_or_default(), json_output);
        }
        Err(error) => return Err(io_failure("inspect stand root", error)),
        Ok(_) => {}
    }
    check_directory_chain(&root)?;
    verify_marker(&root, &record)?;
    let _transition = lock_transition(&root).map_err(Failure::for_read)?;
    record = registered_stand(&registry, &name, &root)?;
    verify_marker(&root, &record)?;
    check_nested_roots(&root)?;
    let pid = match record.removing_pid {
        Some(pid) => pid,
        None => prove_stand_exit(&root).await?,
    };
    let offline = lock_offline(&root).map_err(Failure::for_read)?;
    if let Some(info) = daemon_info(&root).map_err(Failure::for_read)?
        && (info.pid != pid || daemon_alive(&info).map_err(Failure::for_read)?)
    {
        return Err(Failure::new(
            4,
            "stand daemon identity changed; files retained",
        ));
    }
    record.removing_pid = Some(pid);
    write_record(&registry, &name, &record)?;
    remove_offline_root(&root, offline)?;
    fs::remove_file(registry.join(name))
        .map_err(|error| io_failure("remove stand registration", error))?;
    removed(&root, pid, json_output)
}

async fn prove_stand_exit(root: &Path) -> Result<u32, Failure> {
    let daemon = daemon_info(root)
        .map_err(Failure::for_read)?
        .ok_or_else(|| unproven_exit(root))?;
    #[cfg(windows)]
    eliot_daemon::operator::stop_desktop(root).map_err(Failure::for_read)?;
    if daemon_alive(&daemon).map_err(Failure::for_read)? {
        stop_and_drain(root).await?;
    }
    let current = daemon_info(root).map_err(Failure::for_read)?;
    if current.as_ref() != Some(&daemon)
        || daemon_alive(&daemon).map_err(Failure::for_read)?
        || eliot_daemon::last_daemon_phase(root).as_deref() != Some("daemon.stopped")
    {
        return Err(unproven_exit(root));
    }
    Ok(daemon.pid)
}

fn unproven_exit(root: &Path) -> Failure {
    Failure::new(
        4,
        format!(
            "stand lacks a clean exit record; files retained. Start this registered stand with swarm --data-dir \"{}\" daemon to reconcile custody, then retry stand down",
            display(root)
        ),
    )
}

fn removed(root: &Path, pid: u32, json_output: bool) -> Result<u8, Failure> {
    let data_dir = display(root);
    if json_output {
        print_value(
            &json!({"state":"removed","data_dir":data_dir,"daemon_pid":pid}),
            true,
        );
    } else {
        println!("Removed {data_dir} after daemon PID {pid} and its owned processes exited");
    }
    Ok(0)
}

fn remove_offline_root(
    root: &Path,
    offline: eliot_daemon::operator::DaemonLock,
) -> Result<(), Failure> {
    let failure = |error| {
        io_failure(
            &format!("remove stand {}; retry stand down", display(root)),
            error,
        )
    };
    for entry in fs::read_dir(root).map_err(failure)? {
        let entry = entry.map_err(failure)?;
        if [MARKER, "daemon.lock", "operator.lock"]
            .iter()
            .any(|name| entry.file_name() == *name)
        {
            continue;
        }
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path).map_err(failure)?;
        if metadata.is_dir() && is_link(&metadata) {
            fs::remove_dir(path).map_err(failure)?;
        } else if metadata.is_dir() {
            fs::remove_dir_all(path).map_err(failure)?;
        } else {
            fs::remove_file(path).map_err(failure)?;
        }
    }
    drop(offline);
    for name in ["daemon.lock", "operator.lock", MARKER] {
        match fs::remove_file(root.join(name)) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(failure(error)),
        }
    }
    fs::remove_dir(root).map_err(failure)
}

fn stand_registry(create: bool) -> Result<(PathBuf, PathBuf), Failure> {
    let base = match std::env::var_os("SWARM_STANDS_ROOT") {
        Some(path) => PathBuf::from(path),
        None => {
            #[cfg(windows)]
            let base = std::env::var_os("LOCALAPPDATA")
                .map(PathBuf::from)
                .map(|p| p.join("EliotStands"));
            #[cfg(not(windows))]
            let base = std::env::var_os("XDG_STATE_HOME")
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var_os("HOME")
                        .map(PathBuf::from)
                        .map(|p| p.join(".local/state"))
                })
                .map(|p| p.join("eliot-stands"));
            base.ok_or_else(|| {
                Failure::new(2, "set SWARM_STANDS_ROOT to a designated stand directory")
            })?
        }
    };
    let base = std::path::absolute(base)
        .map_err(|error| io_failure("resolve designated stand root", error))?;
    if base.components().any(|part| part == Component::ParentDir) {
        return Err(Failure::new(
            2,
            "stand root must not contain parent-directory traversal",
        ));
    }
    if base.starts_with(std::env::temp_dir()) {
        return Err(Failure::new(
            2,
            "the designated stand root must be outside the temporary directory",
        ));
    }
    if base
        .ancestors()
        .any(|path| path.join("daemon.lock").exists() || path.join(MARKER).exists())
    {
        return Err(Failure::new(
            2,
            "the designated stand root cannot be inside daemon data",
        ));
    }
    if create {
        fs::create_dir_all(&base)
            .map_err(|error| io_failure("create designated stand root", error))?;
    }
    check_directory_chain(&base)?;
    let base = base
        .canonicalize()
        .map_err(|error| io_failure("resolve designated stand root", error))?;
    if std::env::temp_dir()
        .canonicalize()
        .is_ok_and(|temp| base.starts_with(temp))
    {
        return Err(Failure::new(
            2,
            "the designated stand root must be outside the temporary directory",
        ));
    }
    let registry = base.join(".registry");
    if create
        && !registry.exists()
        && let Err(error) = create_stand_root(&registry)
        && !registry.exists()
    {
        return Err(Failure::for_read(error));
    }
    check_directory_chain(&registry)?;
    validate_stand_directory(&registry).map_err(Failure::for_read)?;
    Ok((base, registry))
}

fn ensure_registry_is_external(root: &Path, registry: &Path) -> Result<(), Failure> {
    if registry.starts_with(root) || root.starts_with(registry) {
        return Err(Failure::new(
            2,
            "a stand and its registry must be in separate directories",
        ));
    }
    Ok(())
}

fn registry_name(root: &Path) -> String {
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

fn write_record(dir: &Path, name: &str, value: &impl Serialize) -> Result<(), Failure> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| Failure::new(1, "encode stand registration"))?;
    write_stand_record(dir, name, &bytes).map_err(Failure::for_read)
}

fn registered_stand(registry: &Path, name: &str, root: &Path) -> Result<Registration, Failure> {
    let record: Registration = read_record(&registry.join(name))
        .ok_or_else(|| Failure::new(2, "directory is not registered by swarm stand up"))?;
    if record.marker.format != 2 || registry_name(&record.marker.root) != registry_name(root) {
        return Err(Failure::new(
            2,
            "stand registration does not bind this directory",
        ));
    }
    Ok(record)
}

fn read_record<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    let metadata = fs::symlink_metadata(path).ok()?;
    if !metadata.is_file() || is_link(&metadata) || metadata.len() > 32 * 1024 {
        return None;
    }
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

async fn prepare(root: &Path, workspace: &Path, profile: Option<Vec<u8>>) -> Result<(), Failure> {
    fs::create_dir(workspace).map_err(|error| io_failure("create scratch repository", error))?;
    git(workspace, &["init", "--quiet", "--template="]).await?;
    git(
        workspace,
        &[
            "-c",
            "user.name=Swarm Stand",
            "-c",
            "user.email=stand@eliot.invalid",
            "-c",
            "commit.gpgSign=false",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "Initialize scratch repository",
        ],
    )
    .await?;
    if let Some(profile) = profile {
        fs::write(root.join("config.toml"), profile)
            .map_err(|error| io_failure("write private profile configuration", error))?;
    }
    Ok(())
}

async fn git(workspace: &Path, args: &[&str]) -> Result<(), Failure> {
    let mut command = tokio::process::Command::new("git");
    for (name, _) in std::env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("GIT_")
        {
            command.env_remove(name);
        }
    }
    #[cfg(windows)]
    const NULL_DEVICE: &str = "NUL";
    #[cfg(not(windows))]
    const NULL_DEVICE: &str = "/dev/null";
    command
        .current_dir(eliot_core::display_path(&workspace.to_string_lossy()))
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", NULL_DEVICE)
        .args(args)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(10), command.output())
        .await
        .map_err(|_| Failure::new(5, "scratch Git setup timed out"))?
        .map_err(|error| io_failure("run scratch Git setup", error))?;
    if !output.status.success() {
        return Err(Failure::new(
            1,
            format!(
                "scratch Git setup failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ));
    }
    Ok(())
}

fn check_directory_chain(path: &Path) -> Result<(), Failure> {
    if path.components().any(|part| part == Component::ParentDir) {
        return Err(Failure::new(
            2,
            "stand path must not contain a parent-directory component",
        ));
    }
    for ancestor in path.ancestors() {
        let metadata = fs::symlink_metadata(ancestor)
            .map_err(|error| io_failure("inspect stand directory", error))?;
        if !metadata.is_dir() || is_link(&metadata) {
            return Err(Failure::new(
                2,
                "stand path must name a directory without symbolic links or reparse points",
            ));
        }
    }
    Ok(())
}

fn verify_marker(root: &Path, registration: &Registration) -> Result<(), Failure> {
    let path = root.join(MARKER);
    if let Some(marker) = read_record::<StandMarker>(&path) {
        if marker.format == registration.marker.format
            && marker.root == registration.marker.root
            && marker.token == registration.marker.token
        {
            return Ok(());
        }
    } else if registration.removing_pid.is_some()
        && fs::symlink_metadata(&path)
            .is_err_and(|error| error.kind() == std::io::ErrorKind::NotFound)
        && fs::read_dir(root).is_ok_and(|mut entries| entries.next().is_none())
    {
        return Ok(());
    }
    Err(Failure::new(
        2,
        "stand ownership marker does not match the external registration",
    ))
}

fn check_nested_roots(root: &Path) -> Result<(), Failure> {
    let mut dirs = vec![root.to_owned()];
    while let Some(dir) = dirs.pop() {
        if dir != root
            && (dir.join(MARKER).exists()
                || (dir.join("owner.token").exists() && dir.join("daemon.lock").exists()))
        {
            return Err(Failure::new(
                2,
                format!(
                    "stand contains another daemon data directory at {}",
                    display(&dir)
                ),
            ));
        }
        for contents in
            fs::read_dir(dir).map_err(|error| io_failure("inspect stand files", error))?
        {
            let entry = contents.map_err(|error| io_failure("inspect stand file", error))?;
            let metadata = fs::symlink_metadata(entry.path())
                .map_err(|error| io_failure("inspect stand file type", error))?;
            if metadata.is_dir() && !is_link(&metadata) {
                dirs.push(entry.path());
            }
        }
    }
    Ok(())
}

fn is_link(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    metadata.file_type().is_symlink()
}

fn read_profile(path: &Path) -> Result<Vec<u8>, Failure> {
    let file = File::open(path)
        .map_err(|error| io_failure("open private profile configuration", error))?;
    if !file
        .metadata()
        .map_err(|error| io_failure("inspect private profile configuration", error))?
        .is_file()
    {
        return Err(Failure::new(
            2,
            "stand profile configuration must be a regular file",
        ));
    }
    let mut profile = Vec::new();
    file.take(MAX_PROFILE_BYTES + 1)
        .read_to_end(&mut profile)
        .map_err(|error| io_failure("read private profile configuration", error))?;
    if profile.len() as u64 > MAX_PROFILE_BYTES {
        return Err(Failure::new(2, "stand profile configuration exceeds 1 MiB"));
    }
    eliot_daemon::operator::validate_stand_profiles(&profile).map_err(Failure::for_read)?;
    Ok(profile)
}

fn display(path: &Path) -> String {
    eliot_core::display_path(&path.to_string_lossy())
}

fn io_failure(context: &str, error: std::io::Error) -> Failure {
    Failure::new(1, format!("{context}: {error}"))
}
