use super::{GitProbe, WorkspaceGit, path_text, unavailable, worktree_registered};
use crate::platform::{OwnedChild, custody::AgentSpawnError};
use eliot_core::{Error, Result};
use eliot_store::{IntegrationCustody, Store};
use std::{
    fs,
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub(crate) struct IntegrationLease {
    store: Arc<Mutex<Store>>,
    command_id: String,
    #[cfg(windows)]
    scratch_id: String,
}

impl IntegrationLease {
    pub(super) fn for_record(store: &Arc<Mutex<Store>>, record: &IntegrationCustody) -> Self {
        Self {
            store: Arc::clone(store),
            command_id: record.command_id.clone(),
            #[cfg(windows)]
            scratch_id: record.scratch_id.clone(),
        }
    }

    pub(crate) async fn spawn(
        &self,
        command: tokio::process::Command,
    ) -> std::result::Result<OwnedChild, AgentSpawnError> {
        let mut store = self.store.lock().await;
        // An existing arm marker may describe an earlier, unproven spawn.
        store
            .arm_integration_process(&self.command_id)
            .map_err(AgentSpawnError::Uncertain)?;
        #[cfg(windows)]
        {
            OwnedChild::spawn_integration(command, &self.scratch_id, |identity| {
                store.record_integration_process(&self.command_id, identity)
            })
        }
        #[cfg(not(windows))]
        {
            OwnedChild::spawn_report(command)
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) async fn record_process(&self, process: &eliot_core::ProcessIdentity) -> Result<()> {
        self.store
            .lock()
            .await
            .record_integration_process(&self.command_id, process)
    }

    pub(crate) async fn process_closed(&self) -> Result<()> {
        self.store
            .lock()
            .await
            .clear_integration_process(&self.command_id)
    }

    pub(super) async fn ref_pending(&self, pending: bool) -> Result<()> {
        self.store
            .lock()
            .await
            .mark_integration_ref_pending(&self.command_id, pending)
    }
}

pub(super) struct Scratch {
    pub(super) root: PathBuf,
    pub(super) worktree: PathBuf,
    pub(super) index: PathBuf,
    registration: PathBuf,
}

impl Scratch {
    fn paths(data_root: &Path, record: &IntegrationCustody) -> Result<Self> {
        let root = fs::canonicalize(data_root)
            .map_err(|_| unavailable("integration data root is unavailable"))?;
        if path_text(&root)? != record.data_root || Path::new(&record.common_dir).is_relative() {
            return Err(Error::new(
                "not_allowed",
                "integration custody belongs to another data root",
            ));
        }
        if !ulid::Ulid::from_string(&record.scratch_id)
            .is_ok_and(|id| id.to_string() == record.scratch_id)
        {
            return Err(unavailable("integration scratch identity is invalid"));
        }
        let root = root.join(format!("git-integrate-{}", record.scratch_id));
        let name = format!("integration-{}", record.scratch_id);
        Ok(Self {
            worktree: root.join(&name),
            index: root.join("index"),
            root,
            registration: Path::new(&record.common_dir).join("worktrees").join(name),
        })
    }

    pub(super) fn allocate(data_root: &Path, record: &IntegrationCustody) -> Result<Self> {
        let scratch = Self::paths(data_root, record)?;
        if directory_present(&scratch.registration)? {
            return Err(unavailable(
                "integration worktree registration already exists",
            ));
        }
        #[cfg(unix)]
        let create = {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new().mode(0o700).create(&scratch.root)
        };
        #[cfg(not(unix))]
        let create = fs::create_dir(&scratch.root);
        create.map_err(|_| unavailable("integration scratch directory cannot be created"))?;
        Ok(scratch)
    }
}

pub(super) fn boot_id() -> Result<String> {
    #[cfg(windows)]
    {
        crate::platform::windows::host_boot_id()
    }
    #[cfg(unix)]
    {
        crate::platform::unix::host_boot_id()
    }
    #[cfg(not(any(windows, unix)))]
    {
        Err(unavailable(
            "integration process custody is unavailable on this platform",
        ))
    }
}

fn process_closed(record: &IntegrationCustody) -> Result<bool> {
    if !record.armed {
        return Ok(true);
    }
    #[cfg(windows)]
    {
        let name =
            crate::platform::custody::agent_custody_name("integration", &record.scratch_id, 1);
        if !crate::platform::windows::named_job_closed(&name)? {
            return Ok(false);
        }
        if let Some(process) = &record.process {
            return crate::platform::windows::recorded_process_alive(
                process.pid,
                process.start_time,
            )
            .map(|live| !live);
        }
        Ok(true)
    }
    #[cfg(target_os = "linux")]
    {
        if boot_id()? != record.boot_id {
            return Ok(true);
        }
        let Some(process) = &record.process else {
            return Ok(false);
        };
        if crate::platform::unix::recorded_process_alive(process.pid, process.start_time)? {
            return Ok(false);
        }
        crate::platform::unix::recorded_process_group_empty(process.pid)
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        Ok(false)
    }
}

fn directory_present(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.is_dir()
                && !metadata.file_type().is_symlink()
                && !crate::platform::is_reparse_point(&metadata) =>
        {
            Ok(true)
        }
        Ok(_) => Err(unavailable(
            "integration cleanup path is not an owned directory",
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(_) => Err(unavailable(
            "integration cleanup path could not be inspected",
        )),
    }
}

fn verify_registration_link(scratch: &Scratch) -> Result<()> {
    let pointer = scratch.registration.join("gitdir");
    match fs::symlink_metadata(&pointer) {
        // A crash can interrupt worktree add before the backlink is written.
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Ok(metadata) if metadata.is_file() && !crate::platform::is_reparse_point(&metadata) => {}
        _ => return Err(unavailable("integration worktree backlink is unsafe")),
    }
    let mut text = String::new();
    fs::File::open(&pointer)
        .and_then(|file| file.take(8193).read_to_string(&mut text))
        .map_err(|_| unavailable("integration worktree backlink is unreadable"))?;
    let expected = scratch.worktree.join(".git");
    let observed = text.trim_end_matches(['\r', '\n']);
    #[cfg(windows)]
    let matches = eliot_core::display_path(observed)
        .replace('\\', "/")
        .eq_ignore_ascii_case(&eliot_core::display_path(path_text(&expected)?).replace('\\', "/"));
    #[cfg(not(windows))]
    let matches = observed == path_text(&expected)?;
    if !matches {
        return Err(unavailable(
            "integration worktree backlink names a different checkout",
        ));
    }
    Ok(())
}

/// Remove only this checkout and its unique Git registration. A repository-wide
/// prune could discard another checkout whose disk is temporarily unavailable.
pub(crate) async fn reconcile(
    store: &Arc<Mutex<Store>>,
    data_root: &Path,
    record: &IntegrationCustody,
) -> Result<()> {
    let scratch = Scratch::paths(data_root, record)?;
    if !process_closed(record)? {
        return Err(Error::new(
            "busy",
            "integration process custody is still open",
        ));
    }
    let (checkout, common_dir) = WorkspaceGit::new()?
        .repository_identity(&record.checkout)
        .await?;
    if checkout != record.checkout || common_dir != record.common_dir {
        return Err(Error::new(
            "not_allowed",
            "integration repository changed before cleanup",
        ));
    }
    let git = GitProbe::new()?;
    let registry = scratch
        .registration
        .parent()
        .ok_or_else(|| unavailable("integration worktree registry has no parent"))?;
    let registered = directory_present(registry)? && directory_present(&scratch.registration)?;
    let listed = worktree_registered(&git, Path::new(&checkout), &scratch.worktree).await?;
    if listed && !registered {
        return Err(unavailable(
            "integration worktree has an unexpected Git registration",
        ));
    }
    if registered {
        verify_registration_link(&scratch)?;
    }
    if directory_present(&scratch.root)? {
        fs::remove_dir_all(&scratch.root).map_err(|_| {
            unavailable("integration checkout or private index removal is incomplete")
        })?;
    }
    if registered {
        fs::remove_dir_all(&scratch.registration)
            .map_err(|_| unavailable("integration worktree registration removal is incomplete"))?;
    }
    if worktree_registered(&git, Path::new(&checkout), &scratch.worktree).await? {
        return Err(unavailable("integration worktree registration remains"));
    }
    let mut store = store.lock().await;
    store.clear_integration_process(&record.command_id)?;
    store.complete_integration_cleanup(&record.command_id)
}
