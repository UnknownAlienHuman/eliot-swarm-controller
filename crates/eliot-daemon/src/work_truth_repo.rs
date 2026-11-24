//! Private Git storage for daemon-owned work-truth snapshots and task records.

use crate::git_monitor::{GitProbe, canonical_root};
use eliot_core::{Error, Result};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tokio::time::Instant;

mod retention;
pub(crate) use retention::maintain;

const REPOSITORY_NAME: &str = "work-truth.git";
static INITIALIZATION: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static WRITE_LANE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Serialize every write to the daemon-owned bare repository, including GC.
pub(crate) async fn write_lane() -> tokio::sync::MutexGuard<'static, ()> {
    WRITE_LANE.lock().await
}

fn repository_path(data_root: &Path) -> Result<PathBuf> {
    std::fs::canonicalize(data_root)
        .map(|root| root.join(REPOSITORY_NAME))
        .map_err(|_| {
            Error::new(
                "unavailable",
                "private daemon data directory is unavailable",
            )
        })
}

fn require_plain_path(path: &Path) -> Result<()> {
    if !std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir())
        || !std::fs::canonicalize(path).is_ok_and(|actual| actual == path)
    {
        return Err(Error::new(
            "source_integrity",
            "private work-truth repository path changed",
        ));
    }
    Ok(())
}

async fn is_bare(git: &GitProbe, repository: &Path) -> Result<bool> {
    let result = git
        .workspace_command(
            repository,
            &["rev-parse", "--is-bare-repository"],
            Instant::now() + std::time::Duration::from_secs(30),
        )
        .await?;
    Ok(result.code == 0
        && std::str::from_utf8(&result.stdout).is_ok_and(|value| value.trim() == "true"))
}

/// Resolve a previously created repository even when its source worktree
/// disappeared. Recovery must never initialize a missing repository.
pub(crate) async fn existing(data_root: &Path) -> Result<PathBuf> {
    let repository = repository_path(data_root)?;
    require_plain_path(&repository)?;
    if !is_bare(&GitProbe::new()?, &repository).await? {
        return Err(Error::new(
            "source_integrity",
            "private checkpoint repository is missing",
        ));
    }
    Ok(repository)
}

/// Resolve the one daemon-owned bare repository and ensure it is outside both
/// the project checkout and its common Git directory.
pub(crate) async fn open(data_root: &Path, workspace: &str) -> Result<PathBuf> {
    let git = GitProbe::new()?;
    let project_root = canonical_root(workspace)?;
    let deadline = Instant::now() + std::time::Duration::from_secs(30);
    let common = git
        .workspace_command(
            &project_root,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
            deadline,
        )
        .await?;
    if common.code != 0 {
        return Err(Error::new(
            "unavailable",
            "project Git common directory is unavailable",
        ));
    }
    let common_path = std::str::from_utf8(&common.stdout)
        .ok()
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| Error::new("unavailable", "project Git common directory is unreadable"))?;
    let common_root = canonical_root(common_path)?;
    let repository = repository_path(data_root)?;
    let _initialization = INITIALIZATION.lock().await;

    match fs::symlink_metadata(&repository) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(Error::new(
                "unavailable",
                "private work-truth repository path is not a directory",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if fs::create_dir(&repository).is_err()
                && !fs::symlink_metadata(&repository)
                    .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
            {
                return Err(Error::new(
                    "unavailable",
                    "private work-truth repository could not be created",
                ));
            }
        }
        Err(_) => {
            return Err(Error::new(
                "unavailable",
                "private work-truth repository could not be inspected",
            ));
        }
    }

    require_plain_path(&repository)?;
    if repository.starts_with(&project_root) || repository.starts_with(&common_root) {
        return Err(Error::new(
            "not_allowed",
            "private work-truth repository must be outside the project Git repository",
        ));
    }

    if is_bare(&git, &repository).await? {
        return Ok(repository);
    }
    if fs::read_dir(&repository)
        .map_err(|_| Error::new("unavailable", "private work-truth repository is unreadable"))?
        .next()
        .is_some()
    {
        return Err(Error::new(
            "unavailable",
            "private work-truth repository directory is not an empty bare repository",
        ));
    }
    let initialized = git
        .workspace_command(
            &repository,
            &["init", "--bare", "--quiet", "--template="],
            Instant::now() + std::time::Duration::from_secs(30),
        )
        .await?;
    if initialized.code != 0 {
        return Err(Error::new(
            "unavailable",
            "private work-truth repository initialization failed",
        ));
    }
    if !is_bare(&git, &repository).await? {
        return Err(Error::new(
            "unavailable",
            "private work-truth repository verification failed",
        ));
    }
    Ok(repository)
}
