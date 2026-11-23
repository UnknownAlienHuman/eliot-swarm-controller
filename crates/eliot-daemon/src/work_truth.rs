//! The daemon's own record of work in Git. A checkpoint captures changed and
//! new files under a private ref through a separate index, so the Agent's
//! branch, index and working tree are never touched. The task record keeps the
//! full task, its card and a journal under `refs/eliot/tasks`. Ordinary branch
//! pushes do not carry these refs.

use crate::{
    config::{CheckpointLimits, settings::redact_credential_shapes},
    git_monitor::{GitProbe, PathContent, canonical_root, read_path_content},
    publication::TemporaryGitIndex,
    scripts::process::ProcessOutcome,
};
use eliot_core::{
    Error, Result, SecretPolicy,
    api::{GitCheckpointState, GitCheckpointView, GitCheckpointWithheld},
    bound_chars,
};
use eliot_store::GitCheckpointInput;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

const GIT_TIME: Duration = Duration::from_secs(30);
/// Owner decision on #922: the newest 20 checkpoints per generation.
const KEEP_CHECKPOINTS: usize = 20;
const MAX_REPORTED_PATHS: usize = 64;
const MAX_JOURNAL_BYTES: usize = 128 * 1024;
const MAX_FACT_BYTES: usize = 6 * 1024;
const MAX_PATCH_BYTES: usize = 8 * 1024;
/// One blob write per changed file bounds the work of a single checkpoint.
const MAX_CHECKPOINT_FILES: usize = 512;
const IDENTITY: [&str; 6] = [
    "-c",
    "user.name=Eliot Swarm",
    "-c",
    "user.email=swarm@eliot.invalid",
    "-c",
    "commit.gpgSign=false",
];

pub(crate) struct CheckpointRequest<'a> {
    pub(crate) data_root: &'a Path,
    pub(crate) workspace: &'a str,
    pub(crate) agent: &'a str,
    pub(crate) generation: i64,
    pub(crate) label: i64,
    pub(crate) trigger: &'static str,
    pub(crate) turn_id: Option<String>,
    /// The content identity of the previous successful checkpoint.
    pub(crate) previous: Option<String>,
    pub(crate) previous_snapshot: Option<String>,
    /// New object bytes this generation has already written.
    pub(crate) generation_used: u64,
    pub(crate) limits: CheckpointLimits,
    pub(crate) policy: Arc<SecretPolicy>,
}

pub(crate) struct TaskRecord {
    pub(crate) task_ref: String,
    pub(crate) text: String,
    pub(crate) card: Value,
    pub(crate) sources: Vec<TaskSource>,
}

/// A task file saved with the record, already redacted.
pub(crate) struct TaskSource {
    pub(crate) name: String,
    pub(crate) text: String,
}

/// Task text and report text as stored in Git: known secrets and anything
/// shaped like a credential are replaced.
pub(crate) fn redact_record(text: &str, policy: &SecretPolicy) -> String {
    redact_credential_shapes(&policy.redact_text(text))
}

pub(crate) fn redact_record_value(value: &mut Value, policy: &SecretPolicy) {
    match value {
        Value::String(text) => *text = redact_record(text, policy),
        Value::Array(values) => values
            .iter_mut()
            .for_each(|value| redact_record_value(value, policy)),
        Value::Object(values) => values
            .values_mut()
            .for_each(|value| redact_record_value(value, policy)),
        _ => {}
    }
}

pub(crate) fn task_ref(command: &str) -> String {
    let digest = eliot_core::api::inference::sha256_hex(command.as_bytes());
    format!("refs/eliot/tasks/{digest}")
}

/// Capture the worktree. A failure is a reported fact, never a blocked turn.
pub(crate) async fn checkpoint(request: CheckpointRequest<'_>) -> GitCheckpointInput {
    let trigger = request.trigger;
    let turn_id = request.turn_id.clone();
    let policy = Arc::clone(&request.policy);
    match capture(request).await {
        Ok(input) => input,
        Err(error) => GitCheckpointInput::failed(
            trigger,
            turn_id,
            bound_chars(
                &policy.redact_text(&format!("{}: {}", error.code, error.message)),
                240,
            ),
        ),
    }
}

async fn capture(request: CheckpointRequest<'_>) -> Result<GitCheckpointInput> {
    let git = GitProbe::new()?;
    let root = canonical_root(request.workspace)?;
    let deadline = Instant::now() + GIT_TIME;
    let head = read_line(&git, &root, &["rev-parse", "--verify", "HEAD"], deadline).await?;
    let status = git
        .workspace_command(
            &root,
            &[
                "status",
                "--porcelain=v2",
                "-z",
                "--untracked-files=all",
                "--no-renames",
            ],
            deadline,
        )
        .await?;
    if status.code != 0 {
        return Err(Error::new("unavailable", "checkpoint status failed"));
    }
    let dirty = dirty_paths(&status.stdout)?;
    let scan = {
        let (scan_root, policy, limits, scan_head) = (
            root.clone(),
            Arc::clone(&request.policy),
            request.limits,
            head.clone(),
        );
        tokio::task::spawn_blocking(move || {
            scan_worktree(&scan_root, &scan_head, dirty, &policy, limits)
        })
        .await
        .map_err(|_| Error::new("internal", "checkpoint scan failed"))?
    };
    let WorktreeScan {
        files,
        mut withheld,
        fingerprint,
    } = scan;
    let mut input = GitCheckpointInput {
        state: GitCheckpointState::Unchanged,
        trigger: request.trigger.to_owned(),
        turn_id: request.turn_id.clone(),
        head: None,
        checkpoint_ref: None,
        oid: None,
        fingerprint: Some(fingerprint.clone()),
        paths: Vec::new(),
        paths_truncated: false,
        withheld: Vec::new(),
        withheld_truncated: false,
        captured_bytes: 0,
        error: None,
        journaled_through: None,
    };
    if request.previous.as_deref() == Some(fingerprint.as_str()) {
        set_withheld(&mut input, withheld);
        return Ok(input);
    }
    input.head = Some(head.clone());
    if files.is_empty() && withheld.is_empty() {
        input.state = GitCheckpointState::Clean;
        return Ok(input);
    }
    let overrides = git
        .batch_overrides(&root, Instant::now() + GIT_TIME)
        .await?;
    let known = previous_blobs(
        &git,
        &root,
        &overrides,
        request.previous_snapshot.as_deref(),
        &files,
    )
    .await;
    let zero = "0".repeat(head.len());
    let mut index_info = Vec::new();
    let mut captured = Vec::with_capacity(files.len());
    let mut new_bytes = 0_u64;
    let mut write_failed = false;
    for file in files {
        let ScannedFile { path, content } = file;
        let Some(FileContent { bytes, mode, link }) = content else {
            index_info.extend_from_slice(format!("0 {zero}\t{path}").as_bytes());
            index_info.push(0);
            captured.push(path);
            continue;
        };
        let size = bytes.len() as u64;
        // A blob the last snapshot already holds costs no new storage, so
        // an exhausted budget still keeps unchanged files in the checkpoint.
        let room = request
            .generation_used
            .saturating_add(new_bytes)
            .saturating_add(size)
            <= request.limits.generation_bytes;
        let path_arg = format!("--path={path}");
        let mut args = vec!["hash-object", "--stdin"];
        if room {
            args.push("-w");
        }
        args.push(if link {
            "--no-filters"
        } else {
            path_arg.as_str()
        });
        // The bytes hashed here are the bytes the secret scan read.
        let written = git
            .workspace_input_with(
                &root,
                &args,
                &overrides,
                None,
                &bytes,
                Instant::now() + GIT_TIME,
            )
            .await;
        let written = match written {
            Ok(written) if written.code == 0 => written,
            _ => {
                write_failed = true;
                break;
            }
        };
        let Ok(oid) = first_line(&written.stdout) else {
            write_failed = true;
            break;
        };
        let unchanged = known.get(&path) == Some(&oid);
        if !room && !unchanged {
            withheld.push(GitCheckpointWithheld {
                path,
                bytes: size,
                reason: "generation_budget".to_owned(),
            });
            continue;
        }
        if !unchanged {
            new_bytes = new_bytes.saturating_add(size);
        }
        index_info.extend_from_slice(format!("{mode} {oid}\t{path}").as_bytes());
        index_info.push(0);
        captured.push(path);
    }
    input.captured_bytes = new_bytes;
    set_withheld(&mut input, withheld);
    // Blobs already written count against the budget even if publishing fails.
    let failure = if write_failed {
        Some("checkpoint step hash-object failed".to_owned())
    } else if index_info.is_empty() {
        Some("no changed file could be captured".to_owned())
    } else {
        match prepare_snapshot(&request, &git, &root, &overrides, &head, &index_info).await {
            Ok((reference, oid)) => {
                input.state = GitCheckpointState::Snapshot;
                input.checkpoint_ref = Some(reference);
                input.oid = Some(oid);
                input.paths_truncated = captured.len() > MAX_REPORTED_PATHS;
                input.paths = captured.into_iter().take(MAX_REPORTED_PATHS).collect();
                None
            }
            Err(error) => Some(bound_chars(
                &request
                    .policy
                    .redact_text(&format!("{}: {}", error.code, error.message)),
                240,
            )),
        }
    };
    if let Some(error) = failure {
        input.state = GitCheckpointState::Failed;
        input.fingerprint = None;
        input.error = Some(error);
    }
    Ok(input)
}

/// Build the snapshot commit. The caller must retain its intent before publication.
async fn prepare_snapshot(
    request: &CheckpointRequest<'_>,
    git: &GitProbe,
    root: &Path,
    overrides: &[(String, String)],
    head: &str,
    index_info: &[u8],
) -> Result<(String, String)> {
    let index = TemporaryGitIndex::create_with_prefix(request.data_root, "checkpoint-index")?;
    let (_cancel, cancellation) = watch::channel(false);
    let run = Effect {
        git,
        root,
        index: index.path(),
        policy: &request.policy,
        cancellation: &cancellation,
    };
    run.output(&["read-tree", head]).await?;
    let staged = git
        .workspace_input_with(
            root,
            &["update-index", "-z", "--index-info"],
            overrides,
            Some(index.path()),
            index_info,
            Instant::now() + GIT_TIME,
        )
        .await?;
    if staged.code != 0 {
        return Err(Error::new(
            "unavailable",
            "checkpoint step update-index failed",
        ));
    }
    let tree = run.line(&["write-tree"]).await?;
    let message = format!(
        "eliot checkpoint {} g{} {}",
        request.agent, request.generation, request.trigger
    );
    let mut commit_args = IDENTITY.to_vec();
    commit_args.extend([
        "commit-tree",
        tree.as_str(),
        "-p",
        head,
        "--no-gpg-sign",
        "-m",
        message.as_str(),
    ]);
    let oid = run.line(&commit_args).await?;
    let prefix = format!(
        "refs/eliot/checkpoints/{}/g{}/",
        request.agent, request.generation
    );
    // The time prefix orders retention; the suffix keeps equal times distinct.
    let reference = format!("{prefix}{:016}-{}", request.label, ulid::Ulid::new());
    Ok((reference, oid))
}

pub(crate) async fn publish_checkpoint(root: &Path, reference: &str, oid: &str) -> Result<()> {
    let git = GitProbe::new()?;
    let zero = "0".repeat(oid.len());
    let result = git
        .workspace_input_command(
            root,
            &["update-ref", reference, oid, &zero],
            &[],
            Instant::now() + GIT_TIME,
        )
        .await?;
    if result.code != 0 {
        return Err(Error::new(
            "unavailable",
            "checkpoint ref publication failed",
        ));
    }
    Ok(())
}

pub(crate) async fn checkpoint_reference(root: &Path, reference: &str) -> Result<Option<String>> {
    let git = GitProbe::new()?;
    let deadline = Instant::now() + GIT_TIME;
    let result = git
        .workspace_command(root, &["show-ref", "--exists", reference], deadline)
        .await?;
    match result.code {
        0 => read_line(
            &git,
            root,
            &["show-ref", "--verify", "--hash", reference],
            deadline,
        )
        .await
        .map(Some),
        2 => Ok(None),
        _ => Err(Error::new("unavailable", "checkpoint ref readback failed")),
    }
}

fn set_withheld(input: &mut GitCheckpointInput, mut withheld: Vec<GitCheckpointWithheld>) {
    input.withheld_truncated = withheld.len() > MAX_REPORTED_PATHS;
    withheld.truncate(MAX_REPORTED_PATHS);
    input.withheld = withheld;
}

/// The blob each path holds in the previous snapshot, read in one process.
async fn previous_blobs(
    git: &GitProbe,
    root: &Path,
    overrides: &[(String, String)],
    snapshot: Option<&str>,
    files: &[ScannedFile],
) -> HashMap<String, String> {
    let Some(snapshot) = snapshot else {
        return HashMap::new();
    };
    let paths: Vec<&str> = files
        .iter()
        .filter(|file| file.content.is_some() && !file.path.contains('\n'))
        .map(|file| file.path.as_str())
        .collect();
    let mut input = String::new();
    for path in &paths {
        input.push_str(&format!("{snapshot}:{path}\n"));
    }
    let Ok(output) = git
        .workspace_input_with(
            root,
            &["cat-file", "--batch-check=%(objectname)"],
            overrides,
            None,
            input.as_bytes(),
            Instant::now() + GIT_TIME,
        )
        .await
    else {
        return HashMap::new();
    };
    if output.code != 0 {
        return HashMap::new();
    }
    let text = String::from_utf8_lossy(&output.stdout);
    paths
        .iter()
        .zip(text.lines())
        .filter(|(_, line)| !line.ends_with(" missing"))
        .map(|(path, line)| ((*path).to_owned(), line.trim().to_owned()))
        .collect()
}

struct Effect<'a> {
    git: &'a GitProbe,
    root: &'a Path,
    index: &'a Path,
    policy: &'a Arc<SecretPolicy>,
    cancellation: &'a watch::Receiver<bool>,
}

impl Effect<'_> {
    async fn output(&self, args: &[&str]) -> Result<Vec<u8>> {
        let outcome = self
            .git
            .workspace_effect_command_with_index(
                self.root,
                args,
                self.index,
                GIT_TIME,
                Arc::clone(self.policy),
                self.cancellation.clone(),
            )
            .await?;
        match outcome {
            ProcessOutcome::Completed(capture)
                if capture.stdout_complete && capture.status.success() =>
            {
                Ok(capture.stdout)
            }
            _ => Err(Error::new(
                "unavailable",
                format!(
                    "checkpoint step {} failed",
                    args.iter()
                        .find(|arg| !arg.starts_with('-') && !arg.contains('='))
                        .unwrap_or(&"git")
                ),
            )),
        }
    }

    async fn line(&self, args: &[&str]) -> Result<String> {
        let output = self.output(args).await?;
        first_line(&output)
    }
}

pub(crate) async fn prune_checkpoints(root: &Path, current: &str) -> Result<()> {
    let git = GitProbe::new()?;
    let Some((prefix, _)) = current.rsplit_once('/') else {
        return Err(Error::new(
            "source_integrity",
            "checkpoint ref has no parent",
        ));
    };
    let deadline = Instant::now() + GIT_TIME;
    let listed = git
        .workspace_command(
            root,
            &["for-each-ref", "--format=%(refname)", prefix],
            deadline,
        )
        .await?;
    if listed.code != 0 {
        return Ok(());
    }
    let text = String::from_utf8_lossy(&listed.stdout);
    let mut references: Vec<&str> = text
        .lines()
        .filter(|line| !line.is_empty() && *line != current)
        .collect();
    references.sort_unstable();
    let excess = references.len().saturating_sub(KEEP_CHECKPOINTS - 1);
    for reference in &references[..excess] {
        let result = git
            .workspace_input_command(root, &["update-ref", "-d", reference], &[], deadline)
            .await?;
        if result.code != 0 {
            return Err(Error::new("unavailable", "checkpoint ref pruning failed"));
        }
    }
    Ok(())
}

struct DirtyPath {
    path: String,
    /// Worktree mode from Git; absent for an untracked path.
    mode: Option<String>,
    nested: bool,
}

struct FileContent {
    bytes: Vec<u8>,
    mode: String,
    link: bool,
}

struct ScannedFile {
    path: String,
    /// `None` removes the path from the checkpoint tree.
    content: Option<FileContent>,
}

struct WorktreeScan {
    files: Vec<ScannedFile>,
    withheld: Vec<GitCheckpointWithheld>,
    fingerprint: String,
}

/// Read each changed path once, inside the size bounds. Whatever passes the
/// secret scan is held in memory and is what the checkpoint stores, so a
/// file rewritten after the scan cannot enter unscanned.
fn scan_worktree(
    root: &Path,
    head: &str,
    dirty: Vec<DirtyPath>,
    policy: &SecretPolicy,
    limits: CheckpointLimits,
) -> WorktreeScan {
    let mut hasher = Sha256::new();
    hasher.update(head.as_bytes());
    let mut files = Vec::new();
    let mut withheld = Vec::new();
    let mut read_bytes = 0_u64;
    for (index, entry) in dirty.into_iter().enumerate() {
        let DirtyPath { path, mode, nested } = entry;
        hasher.update([0]);
        hasher.update(path.as_bytes());
        let meta = std::fs::symlink_metadata(root.join(&path)).ok();
        let size = meta
            .as_ref()
            .filter(|meta| meta.is_file())
            .map_or(0, std::fs::Metadata::len);
        let refusal = if nested {
            Some("nested_repository")
        } else if index >= MAX_CHECKPOINT_FILES {
            Some("too_many_files")
        } else if size > limits.file_bytes {
            Some("file_too_large")
        } else if read_bytes.saturating_add(size) > limits.total_bytes {
            Some("checkpoint_total_limit")
        } else {
            None
        };
        let outcome = match refusal {
            Some(reason) => Err(reason),
            None => match read_path_content(
                root,
                &path,
                limits.file_bytes,
                limits.total_bytes,
                &mut read_bytes,
            ) {
                Ok(PathContent::Missing) => Ok(None),
                Ok(PathContent::Link(target)) => Ok(Some(FileContent {
                    bytes: target.to_string_lossy().replace('\\', "/").into_bytes(),
                    mode: "120000".to_owned(),
                    link: true,
                })),
                Ok(PathContent::File(bytes)) => Ok(Some(FileContent {
                    mode: file_mode(mode.as_deref(), meta.as_ref()),
                    bytes,
                    link: false,
                })),
                Ok(PathContent::Other) => Err("not_a_file"),
                Err("cap_exceeded") => Err("file_too_large"),
                Err(_) => Err("unreadable"),
            },
        };
        let outcome = outcome.and_then(|content| match content {
            Some(file) if policy.contains_text(&String::from_utf8_lossy(&file.bytes)) => {
                hasher.update(Sha256::digest(&file.bytes));
                Err("known_secret")
            }
            other => Ok(other),
        });
        match outcome {
            Ok(content) => {
                match &content {
                    Some(file) => {
                        hasher.update(file.mode.as_bytes());
                        hasher.update(Sha256::digest(&file.bytes));
                    }
                    None => hasher.update(b"missing"),
                }
                files.push(ScannedFile { path, content });
            }
            Err(reason) => {
                // An oversized file is identified by metadata, never read.
                hasher.update(reason.as_bytes());
                hasher.update(size.to_le_bytes());
                if let Some(modified) = meta
                    .as_ref()
                    .and_then(|meta| meta.modified().ok())
                    .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                {
                    hasher.update(modified.as_nanos().to_le_bytes());
                }
                withheld.push(GitCheckpointWithheld {
                    path,
                    bytes: size,
                    reason: reason.to_owned(),
                });
            }
        }
    }
    let fingerprint = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    WorktreeScan {
        files,
        withheld,
        fingerprint,
    }
}

fn file_mode(git_mode: Option<&str>, _meta: Option<&std::fs::Metadata>) -> String {
    if let Some(mode @ ("100644" | "100755" | "120000")) = git_mode {
        return mode.to_owned();
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if _meta.is_some_and(|meta| meta.permissions().mode() & 0o111 != 0) {
            return "100755".to_owned();
        }
    }
    "100644".to_owned()
}

/// Parse `git status --porcelain=v2 -z --no-renames`.
fn dirty_paths(status: &[u8]) -> Result<Vec<DirtyPath>> {
    let unreadable = || Error::new("unavailable", "checkpoint status is unreadable");
    let mut paths = Vec::new();
    for record in status.split(|byte| *byte == 0) {
        if record.is_empty() {
            continue;
        }
        let text = std::str::from_utf8(record).map_err(|_| unreadable())?;
        let (fields, mode_at, path_at) = match text.as_bytes()[0] {
            b'1' => (9, 5, 8),
            b'u' => (11, 6, 10),
            b'?' => {
                let path = text.get(2..).filter(|path| !path.is_empty());
                let path = path.ok_or_else(unreadable)?;
                paths.push(DirtyPath {
                    path: path.trim_end_matches('/').to_owned(),
                    mode: None,
                    nested: path.ends_with('/'),
                });
                continue;
            }
            b'!' | b'#' => continue,
            _ => return Err(unreadable()),
        };
        let parts: Vec<&str> = text.splitn(fields, ' ').collect();
        if parts.len() != fields {
            return Err(unreadable());
        }
        paths.push(DirtyPath {
            path: parts[path_at].to_owned(),
            mode: Some(parts[mode_at].to_owned()),
            nested: parts[2].starts_with('S'),
        });
    }
    Ok(paths)
}

fn first_line(output: &[u8]) -> Result<String> {
    std::str::from_utf8(output)
        .ok()
        .and_then(|text| text.lines().next())
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Error::new("unavailable", "Git returned no value"))
}

async fn read_line(
    git: &GitProbe,
    root: &Path,
    args: &[&str],
    deadline: Instant,
) -> Result<String> {
    let output = git.workspace_command(root, args, deadline).await?;
    if output.code != 0 {
        return Err(Error::new("unavailable", "Git readback failed"));
    }
    first_line(&output.stdout)
}

/// The current branch, or `None` for a directory outside Git or a detached HEAD.
pub(crate) async fn current_branch(workspace: &str) -> Option<String> {
    let git = GitProbe::new().ok()?;
    let root = canonical_root(workspace).ok()?;
    let deadline = Instant::now() + GIT_TIME;
    read_line(
        &git,
        &root,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        deadline,
    )
    .await
    .ok()
}

async fn input_line(
    git: &GitProbe,
    root: &Path,
    args: &[&str],
    input: &[u8],
    deadline: Instant,
) -> Result<String> {
    let output = git
        .workspace_input_command(root, args, input, deadline)
        .await?;
    if output.code != 0 {
        return Err(Error::new("unavailable", "Git task record step failed"));
    }
    first_line(&output.stdout)
}

async fn write_blob(
    git: &GitProbe,
    root: &Path,
    bytes: &[u8],
    deadline: Instant,
) -> Result<String> {
    input_line(
        git,
        root,
        &["hash-object", "-w", "--stdin", "--no-filters"],
        bytes,
        deadline,
    )
    .await
}

async fn write_tree(
    git: &GitProbe,
    root: &Path,
    entries: &[(String, String, String, String)],
    deadline: Instant,
) -> Result<String> {
    let mut input = Vec::new();
    for (mode, kind, oid, name) in entries {
        input.extend_from_slice(format!("{mode} {kind} {oid}\t{name}").as_bytes());
        input.push(0);
    }
    input_line(git, root, &["mktree", "-z"], &input, deadline).await
}

async fn publish_task(
    git: &GitProbe,
    root: &Path,
    task_ref: &str,
    tree: &str,
    old: &str,
    deadline: Instant,
) -> Result<()> {
    let mut args = IDENTITY.to_vec();
    args.extend([
        "commit-tree",
        tree,
        "--no-gpg-sign",
        "-m",
        "eliot task record",
    ]);
    let commit = input_line(git, root, &args, &[], deadline).await?;
    let updated = git
        .workspace_input_command(root, &["update-ref", task_ref, &commit, old], &[], deadline)
        .await?;
    if updated.code != 0 {
        return Err(Error::new("unavailable", "task record ref moved"));
    }
    Ok(())
}

async fn task_commit(
    git: &GitProbe,
    root: &Path,
    task_ref: &str,
    deadline: Instant,
) -> Option<String> {
    let target = format!("{task_ref}^{{commit}}");
    read_line(
        git,
        root,
        &["rev-parse", "--verify", "--quiet", &target],
        deadline,
    )
    .await
    .ok()
}

/// Write the task record once. A successor on the same branch reuses it.
pub(crate) async fn ensure_task(workspace: &str, record: &TaskRecord) -> Result<()> {
    let git = GitProbe::new()?;
    let root = canonical_root(workspace)?;
    let deadline = Instant::now() + GIT_TIME;
    if task_commit(&git, &root, &record.task_ref, deadline)
        .await
        .is_some()
    {
        return Ok(());
    }
    let blob = |mode: &str, oid: String, name: &str| {
        (mode.to_owned(), "blob".to_owned(), oid, name.to_owned())
    };
    let card = serde_json::to_vec_pretty(&record.card)
        .map_err(|_| Error::new("internal", "task card cannot be encoded"))?;
    let mut entries = vec![
        blob(
            "100644",
            write_blob(&git, &root, &card, deadline).await?,
            "card.json",
        ),
        blob(
            "100644",
            write_blob(&git, &root, b"", deadline).await?,
            "journal.md",
        ),
        blob(
            "100644",
            write_blob(&git, &root, record.text.as_bytes(), deadline).await?,
            "task.md",
        ),
    ];
    if !record.sources.is_empty() {
        let mut files = Vec::with_capacity(record.sources.len());
        for source in &record.sources {
            let oid = write_blob(&git, &root, source.text.as_bytes(), deadline).await?;
            files.push(blob("100644", oid, &source.name));
        }
        let sources = write_tree(&git, &root, &files, deadline).await?;
        entries.push((
            "040000".to_owned(),
            "tree".to_owned(),
            sources,
            "sources".to_owned(),
        ));
    }
    let tree = write_tree(&git, &root, &entries, deadline).await?;
    publish_task(&git, &root, &record.task_ref, &tree, "", deadline).await
}

/// Whether this profile runs in a sandbox that needs Git roots to commit.
pub(crate) fn needs_commit_roots(harness: &str, profile: &Value) -> bool {
    harness == "codex"
        && profile
            .get("permissions")
            .and_then(Value::as_str)
            .unwrap_or("workspace-write")
            == "workspace-write"
}

/// The narrowest directories that let a sandboxed harness commit on a linked
/// worktree's own branch: its admin directory, the object store, and the
/// branch's ref and log directories. Hooks and repository config stay out. A
/// top-level branch gets none, because its ref directory also holds `main`.
pub(crate) async fn commit_roots(workspace: &str, branch: &str) -> Option<Vec<String>> {
    let (parent, _) = branch.rsplit_once('/')?;
    let git = GitProbe::new().ok()?;
    let root = canonical_root(workspace).ok()?;
    let deadline = Instant::now() + GIT_TIME;
    let head = git
        .workspace_command(&root, &["symbolic-ref", "-q", "HEAD"], deadline)
        .await
        .ok()?;
    if head.code != 0
        || String::from_utf8_lossy(&head.stdout).trim() != format!("refs/heads/{branch}")
    {
        return None;
    }
    let paths = git
        .workspace_command(
            &root,
            &[
                "rev-parse",
                "--path-format=absolute",
                "--git-dir",
                "--git-common-dir",
            ],
            deadline,
        )
        .await
        .ok()?;
    if paths.code != 0 {
        return None;
    }
    let text = String::from_utf8(paths.stdout).ok()?;
    let mut lines = text.lines();
    let admin = PathBuf::from(lines.next()?);
    let common = PathBuf::from(lines.next()?);
    if admin == common {
        return None;
    }
    let refs = common.join("refs").join("heads").join(parent);
    let logs = common.join("logs").join("refs").join("heads").join(parent);
    std::fs::create_dir_all(&refs).ok()?;
    std::fs::create_dir_all(&logs).ok()?;
    Some(
        [admin, common.join("objects"), refs, logs]
            .iter()
            .map(|path| {
                path.components()
                    .collect::<PathBuf>()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect(),
    )
}

/// The task text kept in the record, for when the Store no longer has it.
pub(crate) async fn task_record_text(workspace: &str, task_ref: &str) -> Option<String> {
    let git = GitProbe::new().ok()?;
    let root = canonical_root(workspace).ok()?;
    let path = format!("{task_ref}:task.md");
    let output = git
        .workspace_command(
            &root,
            &["cat-file", "blob", &path],
            Instant::now() + GIT_TIME,
        )
        .await
        .ok()?;
    if output.code != 0 {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!text.is_empty()).then_some(text)
}

/// How a successor reads the task files saved with the record.
pub(crate) async fn task_source_note(workspace: &str, task_ref: &str) -> Option<String> {
    let git = GitProbe::new().ok()?;
    let root = canonical_root(workspace).ok()?;
    let tree = format!("{task_ref}:sources");
    let listed = git
        .workspace_command(
            &root,
            &["ls-tree", "-z", "--name-only", &tree],
            Instant::now() + GIT_TIME,
        )
        .await
        .ok()?;
    if listed.code != 0 {
        return None;
    }
    let commands: Vec<String> = listed
        .stdout
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
        .filter_map(|name| std::str::from_utf8(name).ok())
        .map(|name| format!("git show \"{task_ref}:sources/{name}\""))
        .collect();
    (!commands.is_empty()).then(|| {
        format!(
            "[The task file named above is saved in Git, even if its path is gone: {}]",
            commands.join("; ")
        )
    })
}

/// Append to the journal, keeping its newest bytes. The ref holds one commit,
/// so replaced journals become unreachable instead of growing history.
pub(crate) async fn append_journal(workspace: &str, task_ref: &str, entries: &str) -> Result<()> {
    let git = GitProbe::new()?;
    let root = canonical_root(workspace)?;
    let deadline = Instant::now() + GIT_TIME;
    let Some(parent) = task_commit(&git, &root, task_ref, deadline).await else {
        return Ok(());
    };
    let listed = git
        .workspace_command(&root, &["ls-tree", "-z", &parent], deadline)
        .await?;
    if listed.code != 0 {
        return Err(Error::new("unavailable", "task record tree is unreadable"));
    }
    let journal_path = format!("{parent}:journal.md");
    let current = git
        .workspace_command(&root, &["cat-file", "blob", &journal_path], deadline)
        .await?;
    let mut journal = if current.code == 0 {
        String::from_utf8_lossy(&current.stdout).into_owned()
    } else {
        String::new()
    };
    journal.push_str(entries);
    if journal.len() > MAX_JOURNAL_BYTES {
        let start = journal.ceil_char_boundary(journal.len() - MAX_JOURNAL_BYTES);
        journal = format!("[older journal entries removed]\n{}", &journal[start..]);
    }
    let blob = write_blob(&git, &root, journal.as_bytes(), deadline).await?;
    let mut tree_entries = Vec::new();
    for entry in listed.stdout.split(|byte| *byte == 0) {
        if entry.is_empty() {
            continue;
        }
        let text = std::str::from_utf8(entry)
            .map_err(|_| Error::new("unavailable", "task record tree is unreadable"))?;
        let (meta, name) = text
            .split_once('\t')
            .ok_or_else(|| Error::new("unavailable", "task record tree is unreadable"))?;
        let mut fields = meta.split(' ');
        let (Some(mode), Some(kind), Some(oid)) = (fields.next(), fields.next(), fields.next())
        else {
            return Err(Error::new("unavailable", "task record tree is unreadable"));
        };
        let oid = if name == "journal.md" { &blob } else { oid };
        tree_entries.push((
            mode.to_owned(),
            kind.to_owned(),
            oid.to_owned(),
            name.to_owned(),
        ));
    }
    let tree = write_tree(&git, &root, &tree_entries, deadline).await?;
    publish_task(&git, &root, task_ref, &tree, &parent, deadline).await
}

/// Redact before bounding, so a cut cannot leave part of a known secret.
fn bounded_text(bytes: &[u8], limit: usize, policy: &SecretPolicy) -> String {
    let text = redact_record(&String::from_utf8_lossy(bytes), policy);
    let text = text.trim_end();
    if text.len() <= limit {
        return text.to_owned();
    }
    format!(
        "{}\n[truncated: {} bytes omitted]",
        &text[..text.floor_char_boundary(limit)],
        text.len() - limit
    )
}

async fn fact(
    git: &GitProbe,
    root: &Path,
    args: &[&str],
    limit: usize,
    policy: &SecretPolicy,
) -> String {
    let deadline = Instant::now() + GIT_TIME;
    match git.workspace_command(root, args, deadline).await {
        Ok(output) if output.code == 0 => {
            let text = bounded_text(&output.stdout, limit, policy);
            if text.is_empty() {
                "(none)".to_owned()
            } else {
                text
            }
        }
        _ => "(unavailable)".to_owned(),
    }
}

/// What a successor needs to know, read from Git and the file system now.
pub(crate) async fn handoff_facts(
    workspace: &str,
    base: Option<&str>,
    task: Option<&str>,
    checkpoint: Option<&GitCheckpointView>,
    policy: &SecretPolicy,
) -> Result<String> {
    let git = GitProbe::new()?;
    let root = canonical_root(workspace)?;
    let status = fact(
        &git,
        &root,
        &[
            "status",
            "--branch",
            "--porcelain=v1",
            "--untracked-files=all",
        ],
        MAX_FACT_BYTES,
        policy,
    )
    .await;
    let head = fact(&git, &root, &["rev-parse", "HEAD"], 128, policy).await;
    let range = base.map_or_else(|| "HEAD".to_owned(), |base| format!("{base}..HEAD"));
    let log = fact(
        &git,
        &root,
        &["log", "--format=%h %s", "-n", "20", &range],
        MAX_FACT_BYTES,
        policy,
    )
    .await;
    let from = base.unwrap_or("HEAD");
    let stat = fact(
        &git,
        &root,
        &[
            "-c",
            "core.safecrlf=false",
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--stat=120",
            from,
        ],
        MAX_FACT_BYTES,
        policy,
    )
    .await;
    // Tracked changes only. The brief is persisted, so new files, which may
    // be anything, are named in the status and stay in the worktree.
    let patch = fact(
        &git,
        &root,
        &[
            "-c",
            "core.safecrlf=false",
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "HEAD",
        ],
        MAX_PATCH_BYTES,
        policy,
    )
    .await;
    let task = task.map_or_else(
        || "Task record: none".to_owned(),
        |reference| {
            format!(
                "Task record: git show {reference}:task.md (card.json holds the card and the paths and hashes of files the task names; sources/ holds a task file when the task was a pointer to it; journal.md holds earlier turn reports)"
            )
        },
    );
    let snapshot = match checkpoint {
        Some(GitCheckpointView {
            state: GitCheckpointState::Snapshot,
            checkpoint_ref: Some(reference),
            paths,
            ..
        }) => format!(
            "Latest checkpoint: {reference} ({} uncommitted paths captured, including new files; the same files are in this worktree)",
            paths.len()
        ),
        Some(GitCheckpointView {
            state: GitCheckpointState::Failed,
            error,
            checkpoint_ref,
            ..
        }) => format!(
            "Latest checkpoint: FAILED ({}); newer uncommitted files exist only in this worktree. Last good checkpoint: {}",
            error.as_deref().unwrap_or("unavailable"),
            checkpoint_ref.as_deref().unwrap_or("none")
        ),
        _ => "Latest checkpoint: none needed since the last commit".to_owned(),
    };
    let not_captured = checkpoint
        .filter(|view| !view.withheld.is_empty())
        .map_or_else(String::new, |view| {
            let mut lines = String::from(
                "\nNOT CAPTURED in Git (these changed files exist only in this worktree):",
            );
            for file in &view.withheld {
                lines.push_str(&format!(
                    "\n- {} ({} bytes, {})",
                    redact_record(&file.path, policy),
                    file.bytes,
                    file.reason
                ));
            }
            if view.withheld_truncated {
                lines.push_str("\n- more files omitted");
            }
            lines
        });
    Ok(format!(
        "Worktree: {workspace}\nHEAD: {head}\nBase: {}\n{task}\n{snapshot}{not_captured}\n\nStatus (uncommitted files are listed here):\n{status}\n\nCommits since base (newest first):\n{log}\n\nDiff from base, including uncommitted tracked files:\n{stat}\n\nUncommitted patch of tracked files (new files are listed in the status):\n{patch}",
        base.unwrap_or("not recorded"),
    ))
}
