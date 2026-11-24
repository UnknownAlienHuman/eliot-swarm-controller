//! The daemon's own record of work in Git. Snapshots and task records live in
//! a daemon-owned bare repository outside the project's Git common directory.
//! Checkpoint capture records staged and worktree layers without changing the
//! Agent's branch, index or files. Project pushes cannot carry these refs.

use crate::{
    config::{
        CheckpointLimits,
        settings::{contains_credential_shape, redact_credential_shapes},
    },
    git_monitor::{GitProbe, PathContent, canonical_root, read_path_content},
};
use eliot_core::{
    Error, Result, SecretPolicy,
    api::{GitCheckpointState, GitCheckpointView, GitCheckpointWithheld},
    bound_chars, workspace_path_key,
};
use eliot_store::{GitCheckpointInput, TaskInputs};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;

mod restore;
pub(crate) use restore::{
    CheckpointRestoreWriter, restore_checkpoint, restore_writer_closed, stop_restore_writer,
};
mod task_inputs;
pub(crate) use task_inputs::{read_handoff_task, record_task_inputs};

const GIT_TIME: Duration = Duration::from_secs(30);
const CHECKPOINT_CAPTURE_TIME: Duration = Duration::from_secs(90);
/// Owner decision on #922: the newest 20 checkpoints per generation.
pub(crate) const KEEP_CHECKPOINTS: usize = 20;
const _: usize = KEEP_CHECKPOINTS;
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
    pub(crate) inputs: TaskInputs,
}

/// A task file saved with the record, already redacted.
pub(crate) struct TaskSource {
    pub(crate) name: String,
    pub(crate) text: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SnapshotFile {
    path: String,
    mode: Option<String>,
    oid: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CheckpointManifest {
    version: u8,
    agent: String,
    generation: i64,
    head: String,
    repository_common_dir: String,
    staged: Vec<SnapshotFile>,
    worktree: Vec<SnapshotFile>,
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

pub(crate) fn task_ref(command: &eliot_core::Command) -> String {
    let identity = format!(
        "{}\0{}\0{}\0{}",
        command.target.as_deref().unwrap_or_default(),
        command.generation.unwrap_or_default(),
        command.id,
        command.created_at
    );
    let digest = eliot_core::api::inference::sha256_hex(identity.as_bytes());
    format!("refs/eliot/tasks/{digest}")
}

/// Capture the worktree. A failure is a reported fact, never a blocked turn.
pub(crate) async fn checkpoint(request: CheckpointRequest<'_>) -> GitCheckpointInput {
    let trigger = request.trigger;
    let turn_id = request.turn_id.clone();
    let policy = Arc::clone(&request.policy);
    let charge = request
        .limits
        .generation_bytes
        .saturating_sub(request.generation_used)
        .min(request.limits.total_bytes);
    match tokio::time::timeout(CHECKPOINT_CAPTURE_TIME, async {
        let _write_lane = crate::work_truth_repo::write_lane().await;
        capture(request).await
    })
    .await
    {
        Ok(Ok(input)) => input,
        Ok(Err(error)) => GitCheckpointInput::failed(
            trigger,
            turn_id,
            bound_chars(
                &policy.redact_text(&format!("{}: {}", error.code, error.message)),
                240,
            ),
        ),
        Err(_) => {
            let mut input = GitCheckpointInput::failed(
                trigger,
                turn_id,
                "checkpoint capture timed out".to_owned(),
            );
            input.captured_bytes = charge;
            input
        }
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
    let common_dir = read_line(
        &git,
        &root,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        Instant::now() + GIT_TIME,
    )
    .await?;
    let common_dir = canonical_root(&common_dir)?;
    let repository_common_dir = common_dir.to_string_lossy().into_owned();
    let worktree = {
        let (scan_root, policy, limits, scan_head, dirty) = (
            root.clone(),
            Arc::clone(&request.policy),
            request.limits,
            head.clone(),
            dirty.clone(),
        );
        tokio::task::spawn_blocking(move || {
            scan_worktree(&scan_root, &scan_head, dirty, &policy, limits)
        })
        .await
        .map_err(|_| Error::new("internal", "checkpoint scan failed"))?
    };
    let staged = scan_index(&git, &root, &head, &dirty, &request.policy, request.limits).await?;
    let withheld = merge_withheld(worktree.withheld, staged.withheld);
    let withheld_paths: HashSet<String> = withheld.iter().map(|file| file.path.clone()).collect();
    let mut staged_files: Vec<_> = staged
        .files
        .into_iter()
        .filter(|file| !withheld_paths.contains(&file.path))
        .collect();
    let mut worktree_files: Vec<_> = worktree
        .files
        .into_iter()
        .filter(|file| !withheld_paths.contains(&file.path))
        .collect();
    staged_files.sort_by(|left, right| left.path.cmp(&right.path));
    worktree_files.sort_by(|left, right| left.path.cmp(&right.path));
    let mut fingerprint = Sha256::new();
    fingerprint.update(worktree.fingerprint.as_bytes());
    fingerprint.update(staged.fingerprint.as_bytes());
    let fingerprint: String = fingerprint
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let mut input = GitCheckpointInput {
        state: GitCheckpointState::Unchanged,
        trigger: request.trigger.to_owned(),
        turn_id: request.turn_id.clone(),
        head: None,
        checkpoint_ref: None,
        oid: None,
        task_ref: None,
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
    if staged_files.is_empty() && worktree_files.is_empty() && withheld.is_empty() {
        input.state = GitCheckpointState::Clean;
        return Ok(input);
    }
    // No private repository object or ref is written until both disk and
    // index paths and the exact bytes have passed the bounded secret scan.
    let repository = crate::work_truth_repo::open(request.data_root, request.workspace).await?;
    let overrides = git
        .batch_overrides(&repository, Instant::now() + GIT_TIME)
        .await?;
    let previous_oids =
        previous_checkpoint_oids(&git, &repository, request.previous_snapshot.as_deref()).await;
    let mut prepared = prepare_layers(
        &git,
        &repository,
        &overrides,
        [staged_files, worktree_files],
        &request,
        previous_oids,
        withheld,
    )
    .await?;
    input.captured_bytes = prepared.captured_bytes;
    set_withheld(&mut input, std::mem::take(&mut prepared.withheld));
    if prepared.staged.is_empty() && prepared.worktree.is_empty() {
        input.state = GitCheckpointState::Failed;
        input.fingerprint = None;
        input.error = Some("no changed path could be captured".to_owned());
        return Ok(input);
    }
    let mut captured: Vec<String> = prepared
        .staged
        .iter()
        .chain(&prepared.worktree)
        .map(|file| file.path.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    captured.sort();
    match write_checkpoint(
        &git,
        &repository,
        &overrides,
        &request,
        &head,
        &repository_common_dir,
        prepared,
    )
    .await
    {
        Ok((reference, oid)) => {
            input.state = GitCheckpointState::Snapshot;
            input.checkpoint_ref = Some(reference);
            input.oid = Some(oid);
            input.paths_truncated = captured.len() > MAX_REPORTED_PATHS;
            input.paths = captured.into_iter().take(MAX_REPORTED_PATHS).collect();
        }
        Err(error) => {
            input.state = GitCheckpointState::Failed;
            input.fingerprint = None;
            input.error = Some(bound_chars(
                &request
                    .policy
                    .redact_text(&format!("{}: {}", error.code, error.message)),
                240,
            ));
        }
    }
    Ok(input)
}

#[derive(Clone)]
struct StagedPath {
    path: String,
    mode: Option<String>,
    oid: Option<String>,
}

struct IndexScan {
    files: Vec<ScannedFile>,
    withheld: Vec<GitCheckpointWithheld>,
    fingerprint: String,
}

async fn scan_index(
    git: &GitProbe,
    root: &Path,
    head: &str,
    dirty: &[DirtyPath],
    policy: &SecretPolicy,
    limits: CheckpointLimits,
) -> Result<IndexScan> {
    let raw = git
        .workspace_command(
            root,
            &[
                "diff",
                "--cached",
                "--raw",
                "--no-abbrev",
                "-z",
                "--no-renames",
                head,
            ],
            Instant::now() + GIT_TIME,
        )
        .await?;
    if raw.code != 0 {
        return Err(Error::new("unavailable", "checkpoint index scan failed"));
    }
    let staged = parse_staged_paths(&raw.stdout)?;
    let dirty_by_path: HashMap<_, _> = dirty
        .iter()
        .map(|entry| (entry.path.as_str(), entry))
        .collect();
    let mut files = Vec::new();
    let mut withheld = Vec::new();
    let mut read_bytes = 0_u64;
    let mut hasher = Sha256::new();
    hasher.update(head.as_bytes());
    for (index, path) in staged.into_iter().enumerate() {
        hasher.update([0]);
        hasher.update(path.path.as_bytes());
        let dirty_path = dirty_by_path.get(path.path.as_str()).copied();
        let size = if let Some(oid) = path.oid.as_deref() {
            read_line(
                git,
                root,
                &["cat-file", "-s", oid],
                Instant::now() + GIT_TIME,
            )
            .await?
            .parse::<u64>()
            .map_err(|_| Error::new("unavailable", "staged checkpoint size is unreadable"))?
        } else {
            0
        };
        let refusal = if is_credential_path(&path.path) {
            Some("credential_path")
        } else if dirty_path.is_some_and(|entry| entry.unmerged) {
            Some("unmerged_index")
        } else if dirty_path.is_some_and(|entry| entry.nested)
            || path.mode.as_deref() == Some("160000")
        {
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
        let content = match (refusal, path.oid.as_deref()) {
            (Some(reason), _) => {
                withheld.push(GitCheckpointWithheld {
                    path: path.path.clone(),
                    bytes: size,
                    reason: reason.to_owned(),
                });
                hasher.update(reason.as_bytes());
                hasher.update(size.to_le_bytes());
                continue;
            }
            (None, None) => None,
            (None, Some(oid)) => {
                let blob_size = usize::try_from(size).map_err(|_| {
                    Error::new("unavailable", "staged checkpoint blob size is out of range")
                })?;
                let bytes = git
                    .workspace_blob(root, oid, blob_size, Instant::now() + GIT_TIME)
                    .await?;
                read_bytes = read_bytes.saturating_add(size);
                if contains_checkpoint_credential(&bytes, policy) {
                    withheld.push(GitCheckpointWithheld {
                        path: path.path.clone(),
                        bytes: size,
                        reason: "known_secret".to_owned(),
                    });
                    hasher.update(b"known_secret");
                    hasher.update(size.to_le_bytes());
                    continue;
                }
                hasher.update(Sha256::digest(&bytes));
                Some(FileContent {
                    bytes,
                    mode: path.mode.clone().unwrap_or_else(|| "100644".to_owned()),
                })
            }
        };
        hasher.update(path.mode.as_deref().unwrap_or("deleted").as_bytes());
        if content.is_none() {
            hasher.update(b"missing");
        }
        files.push(ScannedFile {
            path: path.path,
            content,
        });
    }
    let fingerprint = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(IndexScan {
        files,
        withheld,
        fingerprint,
    })
}

fn parse_staged_paths(raw: &[u8]) -> Result<Vec<StagedPath>> {
    let mut fields = raw.split(|byte| *byte == 0);
    let mut staged = Vec::new();
    while let Some(metadata) = fields.next().filter(|field| !field.is_empty()) {
        let metadata = std::str::from_utf8(metadata)
            .map_err(|_| Error::new("unavailable", "checkpoint index metadata is unreadable"))?;
        let mut parts = metadata
            .strip_prefix(':')
            .ok_or_else(|| Error::new("unavailable", "checkpoint index metadata is unreadable"))?
            .split_ascii_whitespace();
        let (Some(_old_mode), Some(new_mode), Some(_old_oid), Some(new_oid), Some(_status)) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(Error::new(
                "unavailable",
                "checkpoint index metadata is unreadable",
            ));
        };
        let path = fields
            .next()
            .ok_or_else(|| Error::new("unavailable", "checkpoint index path is missing"))?;
        let path = std::str::from_utf8(path)
            .map_err(|_| Error::new("unavailable", "checkpoint index path is unreadable"))?
            .to_owned();
        if !valid_relative_git_path(&path) {
            return Err(Error::new(
                "unavailable",
                "checkpoint index path is invalid",
            ));
        }
        let deleted = new_oid.bytes().all(|byte| byte == b'0') || new_mode == "000000";
        staged.push(StagedPath {
            path,
            mode: (!deleted).then(|| new_mode.to_owned()),
            oid: (!deleted).then(|| new_oid.to_owned()),
        });
    }
    Ok(staged)
}

fn valid_relative_git_path(path: &str) -> bool {
    !path.is_empty()
        && !Path::new(path).is_absolute()
        && Path::new(path)
            .components()
            .all(|part| matches!(part, std::path::Component::Normal(_)))
        && !path.contains('\\')
}

pub(crate) fn is_credential_path(path: &str) -> bool {
    let normalized = path.replace('\\', "/").to_ascii_lowercase();
    let components: Vec<_> = normalized.split('/').collect();
    let name = components.last().copied().unwrap_or_default();
    let public_env = [".env.example", ".env.sample", ".env.template", ".env.dist"];
    let env_file = name == ".env" || (name.starts_with(".env.") && !public_env.contains(&name));
    let credential_token = name
        .split(|character: char| !character.is_ascii_alphanumeric())
        .any(|token| {
            matches!(
                token,
                "secret"
                    | "secrets"
                    | "credential"
                    | "credentials"
                    | "token"
                    | "tokens"
                    | "key"
                    | "keys"
                    | "apikey"
                    | "privatekey"
            )
        });
    env_file
        || credential_token
        || components.iter().any(|part| {
            matches!(
                *part,
                ".ssh" | ".aws" | ".gnupg" | "secrets" | "credentials" | "tokens"
            )
        })
        || matches!(
            name,
            ".netrc" | ".npmrc" | ".pypirc" | "credentials" | "credentials.json"
        )
        || [".pem", ".p12", ".pfx", ".key", ".jks", ".keystore"]
            .iter()
            .any(|extension| name.ends_with(extension))
        || [
            "id_rsa",
            "id_ed25519",
            "id_ecdsa",
            "id_dsa",
            "private_key",
            "privatekey",
        ]
        .iter()
        .any(|needle| name.contains(needle))
}

pub(crate) fn contains_checkpoint_credential(bytes: &[u8], policy: &SecretPolicy) -> bool {
    let has_credential = |text: &str| policy.contains_text(text) || contains_credential_shape(text);
    if has_credential(&String::from_utf8_lossy(bytes)) {
        return true;
    }
    let (little_endian, payload) = match bytes {
        [0xff, 0xfe, payload @ ..] => (true, payload),
        [0xfe, 0xff, payload @ ..] => (false, payload),
        _ => return false,
    };
    let (units, _) = payload.as_chunks::<2>();
    let text = char::decode_utf16(units.iter().map(|&pair| {
        if little_endian {
            u16::from_le_bytes(pair)
        } else {
            u16::from_be_bytes(pair)
        }
    }))
    .map(|character| character.unwrap_or(char::REPLACEMENT_CHARACTER))
    .collect::<String>();
    has_credential(&text)
}

fn merge_withheld(
    first: Vec<GitCheckpointWithheld>,
    second: Vec<GitCheckpointWithheld>,
) -> Vec<GitCheckpointWithheld> {
    let mut entries = BTreeMap::new();
    for file in first.into_iter().chain(second) {
        entries
            .entry(file.path.clone())
            .and_modify(|known: &mut GitCheckpointWithheld| {
                known.bytes = known.bytes.max(file.bytes);
                if known.reason != "credential_path" && file.reason == "credential_path" {
                    known.reason = file.reason.clone();
                }
            })
            .or_insert(file);
    }
    entries.into_values().collect()
}

async fn previous_checkpoint_oids(
    git: &GitProbe,
    root: &Path,
    snapshot: Option<&str>,
) -> HashSet<String> {
    let Some(snapshot) = snapshot else {
        return HashSet::new();
    };
    let path = format!("{snapshot}:blobs");
    let output = git
        .workspace_command(
            root,
            &["ls-tree", "-z", "--name-only", &path],
            Instant::now() + GIT_TIME,
        )
        .await;
    let Ok(output) = output else {
        return HashSet::new();
    };
    if output.code != 0 {
        return HashSet::new();
    }
    output
        .stdout
        .split(|byte| *byte == 0)
        .filter_map(|entry| std::str::from_utf8(entry).ok())
        .filter(|entry| eliot_core::api::is_git_oid(entry))
        .map(str::to_owned)
        .collect()
}

async fn hash_blob(
    git: &GitProbe,
    root: &Path,
    overrides: &[(String, String)],
    bytes: &[u8],
) -> Result<String> {
    let result = git
        .workspace_input_with(
            root,
            &["hash-object", "--stdin", "--no-filters"],
            overrides,
            None,
            bytes,
            Instant::now() + GIT_TIME,
        )
        .await?;
    if result.code != 0 {
        return Err(Error::new("unavailable", "checkpoint blob scan failed"));
    }
    first_line(&result.stdout)
}

struct PreparedLayers {
    staged: Vec<SnapshotFile>,
    worktree: Vec<SnapshotFile>,
    blobs: BTreeMap<String, Vec<u8>>,
    captured_bytes: u64,
    withheld: Vec<GitCheckpointWithheld>,
}

async fn prepare_layers(
    git: &GitProbe,
    repository: &Path,
    overrides: &[(String, String)],
    [staged_files, worktree_files]: [Vec<ScannedFile>; 2],
    request: &CheckpointRequest<'_>,
    previous_oids: HashSet<String>,
    mut withheld: Vec<GitCheckpointWithheld>,
) -> Result<PreparedLayers> {
    let mut layers: BTreeMap<String, (Option<ScannedFile>, Option<ScannedFile>)> = BTreeMap::new();
    for file in staged_files {
        let path = file.path.clone();
        layers.entry(path).or_default().0 = Some(file);
    }
    for file in worktree_files {
        let path = file.path.clone();
        layers.entry(path).or_default().1 = Some(file);
    }
    let mut staged = Vec::new();
    let mut worktree = Vec::new();
    let mut blobs = BTreeMap::new();
    let mut known = previous_oids;
    let mut checkpoint_bytes = 0_u64;
    let mut captured_bytes = 0_u64;
    for (path, (staged_file, worktree_file)) in layers {
        let contents: Vec<_> = staged_file
            .iter()
            .chain(worktree_file.iter())
            .filter_map(|file| file.content.as_ref())
            .collect();
        let path_size: u64 = contents
            .iter()
            .map(|content| content.bytes.len() as u64)
            .sum();
        if checkpoint_bytes.saturating_add(path_size) > request.limits.total_bytes {
            withheld.push(GitCheckpointWithheld {
                path,
                bytes: path_size,
                reason: "checkpoint_total_limit".to_owned(),
            });
            continue;
        }
        let mut staged_oid = None;
        let mut worktree_oid = None;
        let mut path_blobs: Vec<(String, Vec<u8>)> = Vec::new();
        let mut path_oids = HashSet::new();
        let mut path_known = HashSet::new();
        let mut path_new_bytes = 0_u64;
        for (file, is_staged) in [
            (staged_file.as_ref(), true),
            (worktree_file.as_ref(), false),
        ] {
            let Some(content) = file.and_then(|file| file.content.as_ref()) else {
                continue;
            };
            let oid = hash_blob(git, repository, overrides, &content.bytes).await?;
            if !known.contains(&oid) && path_oids.insert(oid.clone()) {
                path_new_bytes = path_new_bytes.saturating_add(content.bytes.len() as u64);
                path_blobs.push((oid.clone(), content.bytes.clone()));
            }
            path_known.insert(oid.clone());
            if is_staged {
                staged_oid = Some(oid);
            } else {
                worktree_oid = Some(oid);
            }
        }
        if request
            .generation_used
            .saturating_add(captured_bytes)
            .saturating_add(path_new_bytes)
            > request.limits.generation_bytes
        {
            withheld.push(GitCheckpointWithheld {
                path,
                bytes: path_size,
                reason: "generation_budget".to_owned(),
            });
            continue;
        }
        captured_bytes = captured_bytes.saturating_add(path_new_bytes);
        known.extend(path_known);
        checkpoint_bytes = checkpoint_bytes.saturating_add(path_size);
        for (oid, bytes) in path_blobs {
            blobs.entry(oid).or_insert(bytes);
        }
        if let Some(file) = staged_file {
            staged.push(SnapshotFile {
                path: path.clone(),
                mode: file.content.as_ref().map(|content| content.mode.clone()),
                oid: staged_oid,
            });
        }
        if let Some(file) = worktree_file {
            worktree.push(SnapshotFile {
                path: path.clone(),
                mode: file.content.as_ref().map(|content| content.mode.clone()),
                oid: worktree_oid,
            });
        }
    }
    Ok(PreparedLayers {
        staged,
        worktree,
        blobs,
        captured_bytes,
        withheld,
    })
}

async fn write_checkpoint(
    git: &GitProbe,
    repository: &Path,
    overrides: &[(String, String)],
    request: &CheckpointRequest<'_>,
    head: &str,
    repository_common_dir: &str,
    prepared: PreparedLayers,
) -> Result<(String, String)> {
    let PreparedLayers {
        staged,
        worktree,
        blobs,
        ..
    } = prepared;
    for (oid, bytes) in blobs {
        let written = git
            .workspace_input_with(
                repository,
                &["hash-object", "-w", "--stdin", "--no-filters"],
                overrides,
                None,
                &bytes,
                Instant::now() + GIT_TIME,
            )
            .await?;
        if written.code != 0 || first_line(&written.stdout)? != oid {
            return Err(Error::new("unavailable", "checkpoint blob readback failed"));
        }
    }
    // A checkpoint owns every object referenced by its manifest, including
    // deduplicated bytes written by an earlier, potentially prunable ref.
    let blob_entries: Vec<_> = staged
        .iter()
        .chain(&worktree)
        .filter_map(|file| file.oid.as_deref())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|oid| {
            (
                "100644".to_owned(),
                "blob".to_owned(),
                oid.to_owned(),
                oid.to_owned(),
            )
        })
        .collect();
    let blob_tree = write_tree(git, repository, &blob_entries, Instant::now() + GIT_TIME).await?;
    let manifest = CheckpointManifest {
        version: 1,
        agent: request.agent.to_owned(),
        generation: request.generation,
        head: head.to_owned(),
        repository_common_dir: repository_common_dir.to_owned(),
        staged,
        worktree,
    };
    let manifest_bytes = serde_json::to_vec(&manifest)
        .map_err(|_| Error::new("internal", "checkpoint manifest cannot be encoded"))?;
    let manifest_oid =
        write_blob(git, repository, &manifest_bytes, Instant::now() + GIT_TIME).await?;
    let tree = write_tree(
        git,
        repository,
        &[
            (
                "100644".to_owned(),
                "blob".to_owned(),
                manifest_oid,
                "manifest.json".to_owned(),
            ),
            (
                "040000".to_owned(),
                "tree".to_owned(),
                blob_tree,
                "blobs".to_owned(),
            ),
        ],
        Instant::now() + GIT_TIME,
    )
    .await?;
    let message = format!(
        "eliot checkpoint {} g{} {}",
        request.agent, request.generation, request.trigger
    );
    let mut commit_args = IDENTITY.to_vec();
    commit_args.extend([
        "commit-tree",
        tree.as_str(),
        "--no-gpg-sign",
        "-m",
        message.as_str(),
    ]);
    let oid = input_line(
        git,
        repository,
        &commit_args,
        &[],
        Instant::now() + GIT_TIME,
    )
    .await?;
    let reference = format!(
        "refs/eliot/checkpoints/{}/g{}/{:016}-{}",
        request.agent,
        request.generation,
        request.label,
        ulid::Ulid::new()
    );
    Ok((reference, oid))
}

pub(crate) async fn publish_checkpoint(root: &Path, reference: &str, oid: &str) -> Result<()> {
    let _write_lane = crate::work_truth_repo::write_lane().await;
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
        .workspace_command(
            root,
            &["show-ref", "--verify", "--quiet", reference],
            deadline,
        )
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
        1 => Ok(None),
        _ => Err(Error::new("unavailable", "checkpoint ref readback failed")),
    }
}

fn set_withheld(input: &mut GitCheckpointInput, mut withheld: Vec<GitCheckpointWithheld>) {
    input.withheld_truncated = withheld.len() > MAX_REPORTED_PATHS;
    withheld.truncate(MAX_REPORTED_PATHS);
    input.withheld = withheld;
}

pub(crate) async fn prune_checkpoints(
    root: &Path,
    current: &str,
    protected: &HashSet<String>,
) -> Result<()> {
    let _write_lane = crate::work_truth_repo::write_lane().await;
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
        return Err(Error::new("unavailable", "checkpoint ref listing failed"));
    }
    let text = String::from_utf8_lossy(&listed.stdout);
    let mut references: Vec<&str> = text
        .lines()
        .filter(|line| !line.is_empty() && *line != current)
        .collect();
    references.sort_unstable();
    let excess = references.len().saturating_sub(KEEP_CHECKPOINTS - 1);
    for reference in references
        .into_iter()
        .take(excess)
        .filter(|reference| !protected.contains(*reference))
    {
        let result = git
            .workspace_input_command(root, &["update-ref", "-d", reference], &[], deadline)
            .await?;
        if result.code != 0 {
            return Err(Error::new("unavailable", "checkpoint ref pruning failed"));
        }
    }
    Ok(())
}

#[derive(Clone)]
struct DirtyPath {
    path: String,
    /// Worktree mode from Git; absent for an untracked path.
    mode: Option<String>,
    nested: bool,
    unmerged: bool,
}

#[derive(Clone)]
struct FileContent {
    bytes: Vec<u8>,
    mode: String,
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
        let DirtyPath {
            path,
            mode,
            nested,
            unmerged,
        } = entry;
        hasher.update([0]);
        hasher.update(path.as_bytes());
        let meta = std::fs::symlink_metadata(root.join(&path)).ok();
        let size = meta
            .as_ref()
            .filter(|meta| meta.is_file())
            .map_or(0, std::fs::Metadata::len);
        let refusal = if !valid_relative_git_path(&path) {
            Some("invalid_path")
        } else if is_credential_path(&path) {
            Some("credential_path")
        } else if unmerged {
            Some("unmerged_index")
        } else if nested {
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
                })),
                Ok(PathContent::File(bytes)) => Ok(Some(FileContent {
                    mode: file_mode(mode.as_deref(), meta.as_ref()),
                    bytes,
                })),
                Ok(PathContent::Other) => Err("not_a_file"),
                Err("cap_exceeded") => Err("file_too_large"),
                Err(_) => Err("unreadable"),
            },
        };
        let outcome = outcome.and_then(|content| match content {
            Some(file) if contains_checkpoint_credential(&file.bytes, policy) => {
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
                    unmerged: false,
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
            unmerged: text.as_bytes()[0] == b'u',
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
    // Task refs point to one parentless snapshot. 'old' is exclusively
    // the compare-and-swap guard, so updates do not retain prior revisions.
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
pub(crate) async fn ensure_task(
    data_root: &Path,
    workspace: &str,
    record: &TaskRecord,
) -> Result<()> {
    let git = GitProbe::new()?;
    let root = crate::work_truth_repo::open(data_root, workspace).await?;
    let _write_lane = crate::work_truth_repo::write_lane().await;
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
    let inputs = encode_task_inputs(&record.inputs)?;
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
        blob(
            "100644",
            write_blob(&git, &root, &inputs, deadline).await?,
            "instructions.json",
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

fn encode_task_inputs(inputs: &TaskInputs) -> Result<Vec<u8>> {
    serde_json::to_vec(inputs)
        .map_err(|_| Error::new("internal", "task instructions cannot be encoded"))
}

/// Whether this profile runs in a sandbox that needs Git roots to commit.
/// This profile gate is separate from the worktree and branch checks below.
pub(crate) fn needs_commit_roots(harness: &str, profile: &Value) -> bool {
    eliot_core::api::HarnessCapabilities::of(harness).workspace_write_commit_roots
        && profile
            .get("permissions")
            .and_then(Value::as_str)
            .unwrap_or("workspace-write")
            == "workspace-write"
}

/// Git metadata roots needed by a sandboxed Codex Agent in a managed worktree.
/// The worktree admin directory and shared objects remain writable. Branches
/// get their actual ref and reflog parents after the path and ref checks.
/// A shared parent makes sibling loose refs and logs writable.
/// Successors keep the same grant when they inherit the worktree. Hooks,
/// repository config and packed-refs stay out.
pub(crate) async fn commit_roots(workspace: &str, branch: &str) -> Option<Vec<String>> {
    let git = GitProbe::new().ok()?;
    let root = canonical_root(workspace).ok()?;
    let deadline = Instant::now() + GIT_TIME;
    let full_ref = format!("refs/heads/{branch}");
    let valid = git
        .workspace_command(&root, &["check-ref-format", &full_ref], deadline)
        .await
        .ok()?;
    if valid.code != 0 {
        return None;
    }
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
    let canonical_admin = fs::canonicalize(&admin).ok()?;
    let canonical_common = fs::canonicalize(&common).ok()?;
    if lines.next().is_some()
        || !same_workspace_path(&canonical_admin, &admin)
        || !same_workspace_path(&canonical_common, &common)
        || same_workspace_path(&canonical_admin, &canonical_common)
        || !plain_directory(&admin)
        || !plain_directory(&common)
    {
        return None;
    }
    let objects = common.join("objects");
    if !plain_directory(&objects) {
        return None;
    }

    // The common object store is required for staging and object creation. It
    // remains a shared integrity boundary; a broker would be needed to make
    // these writes per-Agent.
    let mut paths = vec![admin, objects];
    let (parent, leaf) = branch
        .rsplit_once('/')
        .map_or((None, branch), |(parent, leaf)| (Some(parent), leaf));
    let mut refs = common.join("refs").join("heads");
    let mut logs = common.join("logs").join("refs").join("heads");
    if let Some(parent) = parent {
        refs.push(parent);
        logs.push(parent);
    }
    if ensure_plain_directory_tree(&common, &refs) && ensure_plain_directory_tree(&common, &logs) {
        let exclusive = if let Some((parent, _)) = exclusive_agent_ref_parent(branch) {
            contains_only_optional_file(&refs, leaf)
                && contains_only_optional_file(&logs, leaf)
                && branch_ref_parent_is_exclusive(&git, &root, parent, &full_ref, deadline).await
        } else {
            false
        };
        if exclusive
            || (contains_only_plain_entries(&refs, Some(leaf))
                && contains_only_plain_entries(&logs, Some(leaf))
                && branch_ref_is_unambiguous(&git, &root, parent, &full_ref, deadline).await)
        {
            paths.push(refs);
            paths.push(logs);
        }
    }

    Some(
        paths
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

fn exclusive_agent_ref_parent(branch: &str) -> Option<(&str, &str)> {
    let mut components = branch.split('/');
    components.next()?;
    if !is_agent_id_component(components.next()?) {
        return None;
    }
    let leaf = components.next()?;
    if components.next().is_some() {
        return None;
    }
    let (parent, _) = branch.rsplit_once('/')?;
    Some((parent, leaf))
}

fn is_agent_id_component(agent: &str) -> bool {
    agent.strip_prefix('a').is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    })
}

async fn branch_ref_parent_is_exclusive(
    git: &GitProbe,
    root: &Path,
    parent: &str,
    full_ref: &str,
    deadline: Instant,
) -> bool {
    let Ok(refs) = git
        .workspace_command(root, &["for-each-ref", "--format=%(refname)"], deadline)
        .await
    else {
        return false;
    };
    if refs.code != 0 {
        return false;
    }
    let Ok(refs) = String::from_utf8(refs.stdout) else {
        return false;
    };
    let parent_ref = format!("refs/heads/{parent}");
    let nested_prefix = format!("{parent_ref}/");
    let full_ref_key = workspace_path_key(full_ref);
    let parent_ref_key = workspace_path_key(&parent_ref);
    let nested_prefix_key = workspace_path_key(&nested_prefix);
    let mut current_found = false;
    for name in refs.lines() {
        if name == full_ref {
            current_found = true;
            continue;
        }
        let name_key = workspace_path_key(name);
        if name_key == full_ref_key
            || name_key == parent_ref_key
            || name_key.starts_with(nested_prefix_key.as_ref())
        {
            // This also catches packed sibling refs, which are absent from
            // the loose refs directory but would share the granted parent.
            // Windows path comparison also catches case-only ref collisions.
            return false;
        }
    }
    current_found
}

async fn branch_ref_is_unambiguous(
    git: &GitProbe,
    root: &Path,
    parent: Option<&str>,
    full_ref: &str,
    deadline: Instant,
) -> bool {
    let Ok(refs) = git
        .workspace_command(root, &["for-each-ref", "--format=%(refname)"], deadline)
        .await
    else {
        return false;
    };
    if refs.code != 0 {
        return false;
    }
    let Ok(refs) = String::from_utf8(refs.stdout) else {
        return false;
    };
    let full_ref_key = workspace_path_key(full_ref);
    let parent_ref = parent.map(|parent| format!("refs/heads/{parent}"));
    let parent_ref_key = parent_ref.as_deref().map(workspace_path_key);
    let nested_prefix = format!("{full_ref}/");
    let nested_prefix_key = workspace_path_key(&nested_prefix);
    let mut current_found = false;
    for name in refs.lines() {
        if name == full_ref {
            current_found = true;
            continue;
        }
        let name_key = workspace_path_key(name);
        if name_key.as_ref() == full_ref_key.as_ref()
            || parent_ref_key
                .as_ref()
                .is_some_and(|parent_key| name_key.as_ref() == parent_key.as_ref())
            || name_key.starts_with(nested_prefix_key.as_ref())
        {
            // Shared parents permit sibling branches, but the selected ref
            // must remain distinct from case-only, parent, or nested refs.
            return false;
        }
    }
    current_found
}

fn ensure_plain_directory_tree(root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let _ = fs::create_dir(&current);
                let Ok(metadata) = fs::symlink_metadata(&current) else {
                    return false;
                };
                metadata
            }
            Err(_) => return false,
        };
        if !plain_directory_metadata(&metadata) {
            return false;
        }
    }
    fs::canonicalize(path)
        .ok()
        .is_some_and(|canonical| same_workspace_path(&canonical, path))
}

fn contains_only_optional_file(directory: &Path, expected: &str) -> bool {
    let Ok(entries) = fs::read_dir(directory) else {
        return false;
    };
    let mut found = false;
    for entry in entries {
        let Ok(entry) = entry else {
            return false;
        };
        let name = entry.file_name();
        if name.to_str() != Some(expected) || found {
            return false;
        }
        let Ok(metadata) = fs::symlink_metadata(entry.path()) else {
            return false;
        };
        if !metadata.is_file() || is_reparse_point(&metadata) {
            return false;
        }
        found = true;
    }
    true
}

fn contains_only_plain_entries(directory: &Path, expected: Option<&str>) -> bool {
    let expected_key = expected.map(workspace_path_key);
    let mut directories = vec![(directory.to_path_buf(), true)];
    while let Some((current, is_root)) = directories.pop() {
        let Ok(entries) = fs::read_dir(&current) else {
            return false;
        };
        for entry in entries {
            let Ok(entry) = entry else {
                return false;
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return false;
            };
            let is_expected = is_root && expected == Some(name);
            if is_root
                && expected_key.as_ref().is_some_and(|expected_key| {
                    workspace_path_key(name).as_ref() == expected_key.as_ref() && !is_expected
                })
            {
                return false;
            }
            let Ok(metadata) = fs::symlink_metadata(entry.path()) else {
                return false;
            };
            if plain_directory_metadata(&metadata) {
                if is_expected {
                    return false;
                }
                directories.push((entry.path(), false));
            } else if !metadata.is_file() || is_reparse_point(&metadata) {
                return false;
            }
        }
    }
    true
}

fn plain_directory(path: &Path) -> bool {
    fs::symlink_metadata(path).is_ok_and(|metadata| plain_directory_metadata(&metadata))
}

fn plain_directory_metadata(metadata: &fs::Metadata) -> bool {
    metadata.is_dir() && !is_reparse_point(metadata)
}

#[cfg(windows)]
fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
}

#[cfg(not(windows))]
fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

fn same_workspace_path(left: &Path, right: &Path) -> bool {
    left.to_str()
        .zip(right.to_str())
        .is_some_and(|(left, right)| workspace_path_key(left) == workspace_path_key(right))
}

/// The task text kept in the record, for when the Store no longer has it.
pub(crate) async fn task_record_text(
    data_root: &Path,
    workspace: &str,
    task_ref: &str,
) -> Option<String> {
    let git = GitProbe::new().ok()?;
    let root = crate::work_truth_repo::open(data_root, workspace)
        .await
        .ok()?;
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
    let text = String::from_utf8(output.stdout).ok()?;
    (!text.trim().is_empty()).then_some(text)
}

/// Append to the journal, keeping its newest bytes. The ref holds one commit,
/// so replaced journals become unreachable instead of growing history.
pub(crate) async fn append_journal(
    data_root: &Path,
    workspace: &str,
    task_ref: &str,
    entries: &str,
) -> Result<()> {
    let git = GitProbe::new()?;
    let root = crate::work_truth_repo::open(data_root, workspace).await?;
    let _write_lane = crate::work_truth_repo::write_lane().await;
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
    let task = if task.is_some() {
        "The private work-truth repository holds the task record. This brief includes retained task text and saved source excerpts. The card records source hashes and the journal keeps earlier turn reports."
    } else {
        "Task record: none"
    };
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
