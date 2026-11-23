use super::*;
use crate::publication::TemporaryGitIndex;

pub(crate) struct CheckpointRestoreOutcome {
    pub(crate) head: String,
    pub(crate) paths: Vec<String>,
}

struct RestoredFile {
    snapshot: SnapshotFile,
    bytes: Option<Vec<u8>>,
    project_oid: Option<String>,
}

/// Restore both index and worktree layers from the daemon-owned repository.
/// The target must be clean at the captured HEAD; all paths and bytes are
/// validated before importing any object into the project repository.
pub(crate) async fn restore_checkpoint(
    data_root: &Path,
    workspace: &str,
    agent: &str,
    generation: i64,
    reference: &str,
    policy: &SecretPolicy,
    limits: CheckpointLimits,
) -> Result<CheckpointRestoreOutcome> {
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
    let manifest_path = format!("{reference}:manifest.json");
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
        || manifest.agent != agent
        || manifest.generation != generation
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
        reference,
        manifest.staged,
        policy,
        limits,
        &mut total_bytes,
    )
    .await?;
    let mut worktree = load_restore_layer(
        &git,
        &repository,
        &private_overrides,
        reference,
        manifest.worktree,
        policy,
        limits,
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
    if common_dir.to_string_lossy().as_ref() != manifest.repository_common_dir.as_str() {
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
    for file in staged.iter_mut().chain(worktree.iter_mut()) {
        let Some(bytes) = file.bytes.as_ref() else {
            continue;
        };
        let project_oid = hash_blob(&git, &root, &project_overrides, bytes).await?;
        file.project_oid = Some(project_oid.clone());
    }
    let mut imported = HashSet::new();
    for file in staged.iter().chain(&worktree) {
        let (Some(bytes), Some(project_oid)) = (file.bytes.as_ref(), file.project_oid.as_deref())
        else {
            continue;
        };
        if imported.insert(project_oid.to_owned()) {
            let written = git
                .workspace_input_with(
                    &root,
                    &["hash-object", "-w", "--stdin", "--no-filters"],
                    &project_overrides,
                    None,
                    bytes,
                    Instant::now() + GIT_TIME,
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
    let mut worktree_info = Vec::new();
    let mut checkout_paths = Vec::new();
    let mut deletions = Vec::new();
    for file in &worktree {
        append_restore_index_info(&mut worktree_info, file, &zero)?;
        if file.bytes.is_some() {
            checkout_paths.extend_from_slice(file.snapshot.path.as_bytes());
            checkout_paths.push(0);
        } else {
            deletions.push(file.snapshot.path.as_str());
        }
    }

    let index = TemporaryGitIndex::create_with_prefix(data_root, "checkpoint-restore-index")?;
    let base = git
        .workspace_command_with_index(
            &root,
            &["read-tree", manifest.head.as_str()],
            index.path(),
            Instant::now() + GIT_TIME,
        )
        .await?;
    if base.code != 0 {
        return Err(Error::new(
            "unavailable",
            "restore base index could not be prepared",
        ));
    }
    let disk_index = git
        .workspace_input_with(
            &root,
            &["update-index", "-z", "--index-info"],
            &project_overrides,
            Some(index.path()),
            &worktree_info,
            Instant::now() + GIT_TIME,
        )
        .await?;
    if disk_index.code != 0 {
        return Err(Error::new(
            "unavailable",
            "restore worktree layer could not be prepared",
        ));
    }
    if !checkout_paths.is_empty() {
        let checked_out = git
            .workspace_input_with(
                &root,
                &["checkout-index", "--force", "--stdin", "-z"],
                &project_overrides,
                Some(index.path()),
                &checkout_paths,
                Instant::now() + GIT_TIME,
            )
            .await?;
        if checked_out.code != 0 {
            return Err(Error::new(
                "unknown",
                "checkpoint restore file write did not settle",
            ));
        }
    }
    for path in deletions {
        let target = validate_restore_target(&root, path)?;
        match std::fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.is_file() || metadata.file_type().is_symlink() => {
                std::fs::remove_file(&target).map_err(|_| {
                    Error::new("unknown", "checkpoint restore deletion did not settle")
                })?;
            }
            Ok(_) => return Err(Error::new("conflict", "restore target is not a file")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => {
                return Err(Error::new(
                    "unknown",
                    "checkpoint restore path readback failed",
                ));
            }
        }
    }
    if !staged_info.is_empty() {
        let staged_index = git
            .workspace_input_command(
                &root,
                &["update-index", "-z", "--index-info"],
                &staged_info,
                Instant::now() + GIT_TIME,
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

async fn load_restore_layer(
    git: &GitProbe,
    repository: &Path,
    overrides: &[(String, String)],
    reference: &str,
    files: Vec<SnapshotFile>,
    policy: &SecretPolicy,
    limits: CheckpointLimits,
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
                let output = git
                    .workspace_command(
                        repository,
                        &["cat-file", "blob", &object_path],
                        Instant::now() + GIT_TIME,
                    )
                    .await?;
                if output.code != 0
                    || output.stdout.len() as u64 > limits.file_bytes
                    || (*total_bytes).saturating_add(output.stdout.len() as u64)
                        > limits.total_bytes
                    || hash_blob(git, repository, overrides, &output.stdout).await? != *oid
                    || policy.contains_text(&String::from_utf8_lossy(&output.stdout))
                {
                    return Err(Error::new(
                        "unavailable",
                        "checkpoint restore content validation failed",
                    ));
                }
                *total_bytes = total_bytes.saturating_add(output.stdout.len() as u64);
                (Some(output.stdout), None)
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
    let specs: Vec<String> = paths
        .iter()
        .map(|path| format!(":(literal){path}"))
        .collect();
    let mut args = vec!["ls-files", "--stage", "-z", "--"];
    args.extend(specs.iter().map(String::as_str));
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
                Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
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
