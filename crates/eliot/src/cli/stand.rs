//! Own the directories and processes of a fresh private proof stand.

use super::operator::{start_owned, stop_and_drain};
use super::support::{Failure, print_value};
use eliot_daemon::operator::{
    create_stand_root, daemon_alive, daemon_info, lock_offline, lock_transition,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs::{self, File};
use std::io::Read;
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
}

pub(crate) async fn up(
    root: Option<PathBuf>,
    profile: Option<PathBuf>,
    binary: Option<PathBuf>,
    json_output: bool,
) -> Result<u8, Failure> {
    let root = root
        .unwrap_or_else(|| std::env::temp_dir().join(format!("swarm-stand-{}", ulid::Ulid::new())));
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
    check_directory_chain(&path)?;
    let root = path
        .canonicalize()
        .map_err(|error| io_failure("resolve stand path", error))?;
    verify_marker(&root)?;
    let _transition = lock_transition(&root).map_err(Failure::for_read)?;
    verify_marker(&root)?;
    let daemon = daemon_info(&root)
        .map_err(Failure::for_read)?
        .ok_or_else(|| Failure::new(4, "stand has no daemon process identity; files retained"))?;
    #[cfg(windows)]
    eliot_daemon::operator::stop_desktop(&root).map_err(Failure::for_read)?;
    if daemon_alive(&daemon).map_err(Failure::for_read)? {
        stop_and_drain(&root).await?;
    }
    let _offline = lock_offline(&root).map_err(Failure::for_read)?;
    let current = daemon_info(&root).map_err(Failure::for_read)?;
    if current.as_ref() != Some(&daemon)
        || daemon_alive(&daemon).map_err(Failure::for_read)?
        || eliot_daemon::last_daemon_phase(&root).as_deref() != Some("daemon.stopped")
    {
        return Err(Failure::new(
            4,
            "stand lacks a clean daemon exit record; files retained",
        ));
    }
    verify_marker(&root)?;
    check_nested_roots(&root)?;
    remove_offline_root(&root)?;
    let data_dir = display(&root);
    if json_output {
        print_value(
            &json!({"state": "removed", "data_dir": data_dir, "daemon_pid": daemon.pid}),
            true,
        );
    } else {
        println!(
            "Removed {data_dir} after daemon PID {} and its owned processes exited",
            daemon.pid
        );
    }
    Ok(0)
}

fn remove_offline_root(root: &Path) -> Result<(), Failure> {
    #[cfg(windows)]
    let retired = root.to_owned();
    #[cfg(not(windows))]
    let retired = {
        let retired = root.with_file_name(format!(".retired-swarm-stand-{}", ulid::Ulid::new()));
        fs::rename(root, &retired)
            .map_err(|error| io_failure("retire offline stand directory", error))?;
        retired
    };
    fs::remove_dir_all(&retired).map_err(|error| {
        io_failure(
            &format!("remove offline stand {}", display(&retired)),
            error,
        )
    })
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
    let marker = serde_json::to_vec(&StandMarker {
        format: 1,
        root: root.to_owned(),
    })
    .map_err(|_| Failure::new(1, "cannot encode stand ownership marker"))?;
    fs::write(root.join(MARKER), marker)
        .map_err(|error| io_failure("write stand ownership marker", error))
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

fn verify_marker(root: &Path) -> Result<(), Failure> {
    let file = root.join(MARKER);
    let metadata = fs::symlink_metadata(&file)
        .map_err(|_| Failure::new(2, "directory was not created by swarm stand up"))?;
    if !metadata.is_file() || is_link(&metadata) || metadata.len() > 32 * 1024 {
        return Err(Failure::new(2, "stand ownership marker is invalid"));
    }
    let marker: StandMarker = serde_json::from_slice(
        &fs::read(file).map_err(|error| io_failure("read stand ownership marker", error))?,
    )
    .map_err(|_| Failure::new(2, "stand ownership marker is invalid"))?;
    if marker.format != 1 || marker.root != root {
        return Err(Failure::new(
            2,
            "stand ownership marker does not match this directory",
        ));
    }
    Ok(())
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
