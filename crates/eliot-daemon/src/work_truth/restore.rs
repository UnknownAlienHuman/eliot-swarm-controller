use super::*;
use crate::platform::{OwnedChild, data_dir::replace_file};
use eliot_core::api::CheckpointRestoreRequest;
use eliot_store::{CheckpointRestoreIntent, Store};
use std::io::Write;
use tokio::sync::Mutex;

pub(crate) struct CheckpointRestoreOutcome {
    pub(crate) head: String,
    pub(crate) paths: Vec<String>,
}

struct RestoredFile {
    snapshot: SnapshotFile,
    bytes: Option<Vec<u8>>,
    project_oid: Option<String>,
}

pub(crate) struct CheckpointRestoreWriter<'a> {
    pub(crate) store: &'a Mutex<Store>,
    pub(crate) id: &'a str,
}

impl CheckpointRestoreWriter<'_> {
    #[expect(
        clippy::disallowed_types,
        reason = "This binds the prepared Git leaf to the restore receipt before it writes."
    )]
    pub(crate) async fn spawn(&self, command: tokio::process::Command) -> Result<OwnedChild> {
        #[cfg(windows)]
        {
            use crate::platform::custody::AgentSpawnError;
            let boot = crate::platform::windows::host_boot_id()?;
            let mut store = self.store.lock().await;
            let key = format!("{}-{}", store.custody_namespace()?, self.id);
            store.arm_checkpoint_restore_writer(self.id, &boot)?;
            match OwnedChild::spawn_checkpoint_restore_writer(command, &key, boot, |identity| {
                store.record_checkpoint_restore_writer(self.id, identity)
            }) {
                Ok(child) => Ok(child),
                Err(
                    AgentSpawnError::NotSpawned(error) | AgentSpawnError::ProgramUnrunnable(error),
                ) => {
                    store.close_checkpoint_restore_writer(self.id)?;
                    Err(error)
                }
                Err(AgentSpawnError::Uncertain(_)) => Err(Error::new(
                    "unknown",
                    "checkpoint restore writer startup did not settle",
                )),
            }
        }
        #[cfg(target_os = "linux")]
        {
            use crate::platform::unix;
            let boot = unix::host_boot_id()?;
            let mut store = self.store.lock().await;
            let key = format!("{}-{}", store.custody_namespace()?, self.id);
            let custody = unix::restore_writer_custody(&key)
                .map_err(|_| Error::new("unavailable", "cannot bind restore writer custody"))?;
            store.arm_checkpoint_restore_writer(self.id, &boot)?;
            let mut command = command;
            unix::inherit_restore_writer_custody(&mut command, &custody);
            let child = OwnedChild::spawn_leaf(command);
            drop(custody);
            match child {
                Ok(child) => Ok(child),
                Err(error) => {
                    store.close_checkpoint_restore_writer(self.id)?;
                    Err(error)
                }
            }
        }
        #[cfg(not(any(windows, target_os = "linux")))]
        {
            let _ = command;
            Err(Error::new(
                "unsupported",
                "checkpoint restore Git custody is unavailable",
            ))
        }
    }

    pub(crate) async fn close(&self) -> Result<()> {
        self.store
            .lock()
            .await
            .close_checkpoint_restore_writer(self.id)
    }
}

pub(crate) fn restore_writer_closed(intent: &CheckpointRestoreIntent) -> Result<bool> {
    let Some(_boot) = &intent.writer_boot else {
        return Ok(true);
    };
    if !intent.writer_custody_bound {
        return Err(Error::new(
            "custody_unproven",
            "the restore writer has no custody binding to this Store",
        ));
    }
    #[cfg(windows)]
    {
        use crate::platform::{custody::checkpoint_restore_job_name, windows};
        if !windows::named_job_closed(&checkpoint_restore_job_name(&intent.writer_key))? {
            return Ok(false);
        }
        match &intent.writer_identity {
            Some(identity) => {
                windows::recorded_process_alive(identity.pid, identity.start_time).map(|live| !live)
            }
            None => Ok(true),
        }
    }
    #[cfg(target_os = "linux")]
    {
        use crate::platform::unix;
        if unix::host_boot_id()? != *_boot {
            return Ok(true);
        }
        match unix::restore_writer_custody(&intent.writer_key) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => Ok(false),
            Err(_) => Err(Error::new(
                "unavailable",
                "restore writer custody could not be checked",
            )),
        }
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        Err(Error::new(
            "unsupported",
            "checkpoint restore Git custody is unavailable on this platform",
        ))
    }
}

pub(crate) fn stop_restore_writer(intent: &CheckpointRestoreIntent) -> Result<()> {
    if restore_writer_closed(intent)? {
        return Ok(());
    }
    #[cfg(windows)]
    {
        use crate::platform::{custody::checkpoint_restore_job_name, windows};
        let identity = intent.writer_identity.as_ref().ok_or_else(|| {
            Error::new(
                "custody_unproven",
                "restore writer identity was not recorded",
            )
        })?;
        windows::terminate_named_restore_job(
            &checkpoint_restore_job_name(&intent.writer_key),
            identity,
        )?;
        // A Job can report an empty process list before the writer's process
        // handle is signalled. Keep the reservation until both observations agree.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !restore_writer_closed(intent)? {
            if std::time::Instant::now() >= deadline {
                return Err(Error::new(
                    "unknown",
                    "restore writer exit could not be confirmed",
                ));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        Err(Error::new(
            "custody_unproven",
            "there is no verified restore writer signal target",
        ))
    }
}

/// Restore both index and worktree layers from the daemon-owned repository.
/// The target must be clean at the captured HEAD; all paths and bytes are
/// validated before importing any object into the project repository.
pub(crate) async fn restore_checkpoint(
    data_root: &Path,
    request: &CheckpointRestoreRequest,
    expected_oid: &str,
    expected_common_dir: &str,
    policy: &SecretPolicy,
    limits: CheckpointLimits,
    writer: &CheckpointRestoreWriter<'_>,
) -> Result<CheckpointRestoreOutcome> {
    let CheckpointRestoreRequest {
        workspace,
        agent,
        generation,
        checkpoint_ref: reference,
        ..
    } = request;
    let expected_prefix = format!("refs/eliot/checkpoints/{agent}/g{generation}/");
    let Some(label) = reference.strip_prefix(&expected_prefix) else {
        return Err(Error::new(
            "invalid",
            "checkpoint reference is outside the Agent generation",
        ));
    };
    if label.len() < 17
        || !label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(Error::new("invalid", "checkpoint reference is malformed"));
    }
    let git = GitProbe::new()?;
    let repository = crate::work_truth_repo::open(data_root, workspace).await?;
    if checkpoint_reference(&repository, reference)
        .await?
        .as_deref()
        != Some(expected_oid)
    {
        return Err(Error::new(
            "source_integrity",
            "checkpoint ref differs from Store",
        ));
    }
    let manifest_path = format!("{expected_oid}:manifest.json");
    let stored = git
        .workspace_command(
            &repository,
            &["cat-file", "blob", &manifest_path],
            Instant::now() + GIT_TIME,
        )
        .await?;
    if stored.code != 0 {
        return Err(Error::new("not_found", "checkpoint is no longer retained"));
    }
    let manifest: CheckpointManifest = serde_json::from_slice(&stored.stdout)
        .map_err(|_| Error::new("unavailable", "checkpoint manifest is unreadable"))?;
    if manifest.version != 1
        || manifest.agent != *agent
        || manifest.generation != *generation
        || manifest.staged.len().max(manifest.worktree.len()) > MAX_CHECKPOINT_FILES
    {
        return Err(Error::new(
            "unavailable",
            "checkpoint manifest binding is invalid",
        ));
    }

    let private_overrides = git
        .batch_overrides(&repository, Instant::now() + GIT_TIME)
        .await?;
    let mut total_bytes = 0_u64;
    let mut staged = load_restore_layer(
        &git,
        &repository,
        &private_overrides,
        expected_oid,
        manifest.staged,
        (policy, limits),
        &mut total_bytes,
    )
    .await?;
    let worktree = load_restore_layer(
        &git,
        &repository,
        &private_overrides,
        expected_oid,
        manifest.worktree,
        (policy, limits),
        &mut total_bytes,
    )
    .await?;
    let mut paths: Vec<String> = staged
        .iter()
        .chain(&worktree)
        .map(|file| file.snapshot.path.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    paths.sort();
    if paths.is_empty() {
        return Err(Error::new(
            "unavailable",
            "checkpoint contains no restorable paths",
        ));
    }

    let root = canonical_root(workspace)?;
    let common_dir = read_line(
        &git,
        &root,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        Instant::now() + GIT_TIME,
    )
    .await?;
    let common_dir = canonical_root(&common_dir)?;
    if common_dir.to_string_lossy().as_ref() != manifest.repository_common_dir.as_str()
        || workspace_path_key(&common_dir.to_string_lossy())
            != workspace_path_key(expected_common_dir)
        || workspace_path_key(&root.to_string_lossy()) != workspace_path_key(workspace)
    {
        return Err(Error::new(
            "conflict",
            "restore target belongs to a different Git repository",
        ));
    }
    let current_head = read_line(
        &git,
        &root,
        &["rev-parse", "--verify", "HEAD"],
        Instant::now() + GIT_TIME,
    )
    .await?;
    if current_head != manifest.head {
        return Err(Error::new(
            "conflict",
            "restore target HEAD differs from the checkpoint",
        ));
    }
    let status = git
        .workspace_command(
            &root,
            &["status", "--porcelain=v2", "-z", "--untracked-files=all"],
            Instant::now() + GIT_TIME,
        )
        .await?;
    if status.code != 0 || !status.stdout.is_empty() {
        return Err(Error::new(
            "conflict",
            "restore target worktree must be clean",
        ));
    }
    let tracked = tracked_paths(&git, &root, &paths).await?;
    for path in &paths {
        let target = validate_restore_target(&root, path)?;
        match std::fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {
                return Err(Error::new("conflict", "restore target path is a directory"));
            }
            Ok(_) if !tracked.contains(path) => {
                return Err(Error::new(
                    "conflict",
                    "restore target has an untracked path collision",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(_) => {}
            Err(_) => {
                return Err(Error::new(
                    "unavailable",
                    "restore target cannot be inspected",
                ));
            }
        }
    }

    let project_overrides = git
        .batch_overrides(&root, Instant::now() + GIT_TIME)
        .await?;
    for file in &mut staged {
        let Some(bytes) = file.bytes.as_ref() else {
            continue;
        };
        let project_oid = hash_blob(&git, &root, &project_overrides, bytes).await?;
        file.project_oid = Some(project_oid.clone());
    }
    let mut imported = HashSet::new();
    for file in &staged {
        let (Some(bytes), Some(project_oid)) = (file.bytes.as_ref(), file.project_oid.as_deref())
        else {
            continue;
        };
        if imported.insert(project_oid.to_owned()) {
            let written = git
                .checkpoint_restore_write(
                    &root,
                    &["hash-object", "-w", "--stdin", "--no-filters"],
                    &project_overrides,
                    bytes,
                    Instant::now() + GIT_TIME,
                    writer,
                )
                .await?;
            if written.code != 0 || first_line(&written.stdout)? != project_oid {
                return Err(Error::new(
                    "unknown",
                    "checkpoint restore object import did not settle",
                ));
            }
        }
    }

    let zero = "0".repeat(current_head.len());
    let mut staged_info = Vec::new();
    for file in &staged {
        append_restore_index_info(&mut staged_info, file, &zero)?;
    }
    if !staged_info.is_empty() && !cfg!(any(windows, target_os = "linux")) {
        return Err(Error::new(
            "unsupported",
            "checkpoint restore Git custody is unavailable on this platform",
        ));
    }
    writer
        .store
        .lock()
        .await
        .mark_checkpoint_restore_applying(writer.id)?;
    for file in &worktree {
        apply_disk_file(&root, file)?;
    }
    if !staged_info.is_empty() {
        let staged_index = git
            .checkpoint_restore_write(
                &root,
                &["update-index", "-z", "--index-info"],
                &project_overrides,
                &staged_info,
                Instant::now() + GIT_TIME,
                writer,
            )
            .await?;
        if staged_index.code != 0 {
            return Err(Error::new(
                "unknown",
                "checkpoint restore staged layer did not settle",
            ));
        }
    }
    Ok(CheckpointRestoreOutcome {
        head: manifest.head,
        paths,
    })
}

/// Disk blobs already contain the original checkout representation.
fn apply_disk_file(root: &Path, file: &RestoredFile) -> Result<()> {
    let target = validate_restore_target(root, &file.snapshot.path)?;
    let unknown = || Error::new("unknown", "checkpoint restore file write did not settle");
    let Some(bytes) = file.bytes.as_deref() else {
        return match std::fs::remove_file(&target) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err(unknown()),
        };
    };
    let parent = target.parent().ok_or_else(unknown)?;
    std::fs::create_dir_all(parent).map_err(|_| unknown())?;
    validate_restore_target(root, &file.snapshot.path)?;
    let temporary = parent.join(format!(".eliot-restore-{}.tmp", ulid::Ulid::new()));
    if file.snapshot.mode.as_deref() == Some("120000") {
        let link = std::str::from_utf8(bytes).map_err(|_| unknown())?;
        #[cfg(windows)]
        std::os::windows::fs::symlink_file(link, &temporary).map_err(|_| unknown())?;
        #[cfg(unix)]
        std::os::unix::fs::symlink(link, &temporary).map_err(|_| unknown())?;
    } else {
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| unknown())?;
        let written = (|| {
            output.write_all(bytes)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = if file.snapshot.mode.as_deref() == Some("100755") {
                    0o755
                } else {
                    0o644
                };
                output.set_permissions(std::fs::Permissions::from_mode(mode))?;
            }
            output.sync_all()
        })();
        drop(output);
        if written.is_err() {
            let _ = std::fs::remove_file(&temporary);
            return Err(unknown());
        }
    }
    if replace_file(&temporary, &target).is_err() {
        let _ = std::fs::remove_file(&temporary);
        return Err(unknown());
    }
    Ok(())
}

async fn load_restore_layer(
    git: &GitProbe,
    repository: &Path,
    overrides: &[(String, String)],
    reference: &str,
    files: Vec<SnapshotFile>,
    (policy, limits): (&SecretPolicy, CheckpointLimits),
    total_bytes: &mut u64,
) -> Result<Vec<RestoredFile>> {
    let mut seen = HashSet::new();
    let mut restored = Vec::with_capacity(files.len());
    for snapshot in files {
        if !valid_relative_git_path(&snapshot.path)
            || is_credential_path(&snapshot.path)
            || !seen.insert(snapshot.path.clone())
        {
            return Err(Error::new(
                "unavailable",
                "checkpoint restore path validation failed",
            ));
        }
        let (bytes, project_oid) = match (&snapshot.mode, &snapshot.oid) {
            (None, None) => (None, None),
            (Some(mode), Some(oid))
                if (oid.len() == 40 || oid.len() == 64)
                    && matches!(mode.as_str(), "100644" | "100755" | "120000")
                    && oid.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
            {
                let object_path = format!("{reference}:blobs/{oid}");
                let deadline = Instant::now() + GIT_TIME;
                let tree_oid = read_line(
                    git,
                    repository,
                    &["rev-parse", "--verify", &object_path],
                    deadline,
                )
                .await?;
                let size = read_line(git, repository, &["cat-file", "-s", &tree_oid], deadline)
                    .await?
                    .parse::<u64>()
                    .map_err(|_| {
                        Error::new("source_integrity", "checkpoint blob size is invalid")
                    })?;
                if tree_oid != *oid
                    || size > limits.file_bytes
                    || (*total_bytes).saturating_add(size) > limits.total_bytes
                {
                    return Err(Error::new(
                        "unavailable",
                        "checkpoint restore content validation failed",
                    ));
                }
                let size_bound = usize::try_from(size).map_err(|_| {
                    Error::new("unavailable", "checkpoint blob size is out of range")
                })?;
                let bytes = git
                    .workspace_blob(repository, oid, size_bound, deadline)
                    .await?;
                if hash_blob(git, repository, overrides, &bytes).await? != *oid
                    || contains_checkpoint_credential(&bytes, policy)
                {
                    return Err(Error::new(
                        "unavailable",
                        "checkpoint restore content validation failed",
                    ));
                }
                *total_bytes = total_bytes.saturating_add(size);
                (Some(bytes), None)
            }
            _ => {
                return Err(Error::new(
                    "unavailable",
                    "checkpoint restore layer is malformed",
                ));
            }
        };
        restored.push(RestoredFile {
            snapshot,
            bytes,
            project_oid,
        });
    }
    Ok(restored)
}

fn append_restore_index_info(output: &mut Vec<u8>, file: &RestoredFile, zero: &str) -> Result<()> {
    match (&file.snapshot.mode, &file.project_oid) {
        (Some(mode), Some(oid)) => {
            output.extend_from_slice(format!("{mode} {oid}\t{}\0", file.snapshot.path).as_bytes())
        }
        (None, None) => {
            output.extend_from_slice(format!("0 {zero}\t{}\0", file.snapshot.path).as_bytes())
        }
        _ => {
            return Err(Error::new(
                "unavailable",
                "checkpoint restore index entry is malformed",
            ));
        }
    }
    Ok(())
}

async fn tracked_paths(git: &GitProbe, root: &Path, paths: &[String]) -> Result<HashSet<String>> {
    let mut args = vec!["ls-files", "--stage", "-z", "--"];
    // GitProbe already treats these arguments as literal pathnames.
    args.extend(paths.iter().map(String::as_str));
    let output = git
        .workspace_command(root, &args, Instant::now() + GIT_TIME)
        .await?;
    if output.code != 0 {
        return Err(Error::new(
            "unavailable",
            "restore target index could not be read",
        ));
    }
    let mut tracked = HashSet::new();
    for record in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        let Some(separator) = record.iter().position(|byte| *byte == b'\t') else {
            return Err(Error::new(
                "unavailable",
                "restore target index is unreadable",
            ));
        };
        let path = &record[separator + 1..];
        let path = std::str::from_utf8(path)
            .map_err(|_| Error::new("unavailable", "restore target index path is unreadable"))?;
        tracked.insert(path.to_owned());
    }
    Ok(tracked)
}

fn validate_restore_target(root: &Path, path: &str) -> Result<PathBuf> {
    if !valid_relative_git_path(path) {
        return Err(Error::new("invalid", "checkpoint restore path is invalid"));
    }
    let target = root.join(path);
    let mut parent = root.to_owned();
    let mut components = Path::new(path).components().peekable();
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(Error::new("invalid", "checkpoint restore path is invalid"));
        };
        parent.push(name);
        if components.peek().is_some() {
            match std::fs::symlink_metadata(&parent) {
                Ok(metadata) if metadata.is_dir() && !is_reparse_point(&metadata) => {}
                Ok(_) => {
                    return Err(Error::new(
                        "conflict",
                        "restore path crosses a non-directory",
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(_) => {
                    return Err(Error::new(
                        "unavailable",
                        "restore parent cannot be inspected",
                    ));
                }
            }
        }
    }
    Ok(target)
}
