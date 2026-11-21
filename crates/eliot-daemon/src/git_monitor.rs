//! Local, bounded Git readback. Wire formats follow the official Git 2.55
//! git-status (porcelain v2/-z), git-ls-files, git-log and git-check-ignore docs.
//! No donor application code, shell, index/ref/object writes, or network reads.
//! Reads require leaf custody. Workspace writes are only prepared here; their
//! manager owns ordinary scoped execution and cancellation serialization.

use crate::platform::{OwnedChild, is_reparse_point};
use eliot_core::api::is_git_oid;
use eliot_core::{Error, GitBaseline, GitCommit, GitPathStatus, GitSnapshot, GitWorkProbe, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::OsString,
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{ChildStdout, Command},
    time::{Instant, timeout_at},
};

const OUTPUT_BYTES: usize = 1024 * 1024;
const STDERR_BYTES: usize = 32 * 1024;
const INPUT_BYTES: usize = 32 * 1024;
const CONFIG_PAIRS: usize = 256;
const CONFIG_BYTES: usize = 16 * 1024;
const HISTORY_COMMITS: usize = 64;
const WORK_DIFF_MAX_FILES: usize = 1_000;
const WORK_UNTRACKED_MAX_BYTES_PER_FILE: u64 = 512 * 1024;
const WORK_UNTRACKED_MAX_TOTAL_BYTES: u64 = 4 * 1024 * 1024;
const WORK_MAX_COMMITS_AHEAD: u64 = 10_000;
const PROBE_TIME: Duration = Duration::from_secs(15);
const CLEANUP_TIME: Duration = Duration::from_secs(2);
const STATUS_ARGS: &[&str] = &[
    "status",
    "--porcelain=v2",
    "--branch",
    "-z",
    "--untracked-files=all",
    "--ignore-submodules=all",
    "--no-ahead-behind",
    "--renames",
];
const INDEX_ARGS: &[&str] = &["ls-files", "--stage", "-v", "-z"];
const INDEX_RECORDS: usize = 100_000;
const INDEX_RECORD_BYTES: usize = 32 * 1024;
const INDEX_TOTAL_BYTES: usize = 16 * 1024 * 1024;
const ATTRIBUTE_BATCH_BYTES: usize = OUTPUT_BYTES / 4;
/// A Goal's change scope lists a rename as both of its paths. A changed
/// submodule is reported, so its record makes the change set unverified
/// instead of hiding edits inside it.
const SCOPE_STATUS_ARGS: &[&str] = &[
    "status",
    "--porcelain=v2",
    "--branch",
    "-z",
    "--untracked-files=all",
    "--ignore-submodules=none",
    "--no-ahead-behind",
    "--no-renames",
];
/// Most bytes read to digest the paths one scope comparison names.
const SCOPE_READ_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct GitProbe {
    program: PathBuf,
}

pub(crate) struct Captured {
    pub code: i32,
    pub stdout: Vec<u8>,
}

#[derive(Clone, Copy)]
pub(crate) struct GitBinding<'a> {
    pub git_dir: &'a Path,
    pub common_dir: &'a Path,
}

/// The change an independent review looks at: a zero-context diff from the
/// base (or HEAD) to the worktree, plus untracked paths.
pub(crate) struct PatchRead {
    pub root: PathBuf,
    pub base: Option<String>,
    pub head: Option<String>,
    pub diff: String,
    pub untracked: Vec<String>,
    /// False when the diff exceeded the read bound and was left out.
    pub complete: bool,
}

/// HEAD and the paths that differ from it (index, worktree and untracked
/// files; ignored files excluded), for a Goal's change scope.
pub(crate) struct ScopeState {
    pub root: PathBuf,
    pub head: Option<String>,
    pub changed: BTreeSet<String>,
}

struct Status {
    head: Option<String>,
    branch: Option<String>,
    dirty: Vec<GitPathStatus>,
    renamed_from: BTreeSet<String>,
}

impl GitProbe {
    /// Resolve only absolute PATH entries, never the implicit Windows/CWD search.
    pub(crate) fn new() -> Result<Self> {
        let search = env::var_os("PATH").ok_or_else(|| unavailable("Git PATH is unavailable"))?;
        for directory in env::split_paths(&search).filter(|path| path.is_absolute()) {
            let candidate = directory.join(if cfg!(windows) { "git.exe" } else { "git" });
            let Ok(metadata) = fs::metadata(&candidate) else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if metadata.permissions().mode() & 0o111 == 0 {
                    continue;
                }
            }
            let program = fs::canonicalize(candidate)
                .map_err(|_| unavailable("Git executable cannot be resolved"))?;
            #[cfg(windows)]
            let program = resolve_windows_git(program)?;
            return Ok(Self { program });
        }
        Err(unavailable(
            "Git executable was not found on absolute PATH entries",
        ))
    }

    pub(crate) async fn probe(
        &self,
        workspace: &str,
        baseline: Option<&GitBaseline>,
        base_oid: Option<&str>,
        edited_paths: &[String],
        now: i64,
    ) -> Result<GitSnapshot> {
        let root = canonical_root(workspace)?;
        let root_text = root
            .to_str()
            .ok_or_else(|| unavailable("workspace is not UTF-8"))?;
        let deadline = Instant::now() + PROBE_TIME;
        let overrides = self.safe_overrides(&root, deadline).await?;
        self.require_exact_root(&root, &overrides, deadline).await?;

        let (index_digest, index) = self.index_snapshot(&root, &overrides, deadline).await?;
        let status_bytes = self
            .checked(&root, STATUS_ARGS, &overrides, &[], deadline)
            .await?;
        let status = parse_status(&status_bytes)?;

        let mut edited = BTreeSet::new();
        let mut input = Vec::new();
        for path in edited_paths {
            if Instant::now() >= deadline {
                return Err(unavailable("Git observation deadline elapsed"));
            }
            let path = normalize_hint(root_text, path)?;
            if edited.insert(path.clone()) {
                input.extend_from_slice(path.as_bytes());
                input.push(0);
                if input.len() > INPUT_BYTES {
                    return Err(unavailable(
                        "edited-path input exceeds the observation bound",
                    ));
                }
            }
        }
        let ignored = if input.is_empty() {
            BTreeSet::new()
        } else {
            let output = self
                .run(
                    &root,
                    &["check-ignore", "--stdin", "-z"],
                    &overrides,
                    &input,
                    deadline,
                )
                .await?;
            if !matches!(output.code, 0 | 1) {
                return Err(unavailable("Git ignore readback failed"));
            }
            parse_paths(&output.stdout)?.into_iter().collect()
        };
        let mut attribute_paths: BTreeSet<String> = index.keys().cloned().collect();
        attribute_paths.extend(edited.iter().cloned());
        for dirty in &status.dirty {
            attribute_paths.insert(dirty.path.clone());
            attribute_paths.extend(dirty.original_path.iter().cloned());
        }
        let filter_affected = self
            .filter_attributes(&root, &overrides, attribute_paths, deadline)
            .await?;
        // Disabling executable conversions can differ from the owner's normal
        // comparison. Publish bounded raw status as partial, never normal-Git
        // clean coverage, when an applicable filter attribute is present.
        let complete = filter_affected.is_empty() && index.values().all(|covered| *covered);
        let mut clean_paths = Vec::new();
        for path in edited {
            if ignored.contains(&path)
                || status
                    .dirty
                    .iter()
                    .any(|dirty| related_path(&dirty.path, &path))
                || status
                    .renamed_from
                    .iter()
                    .any(|original| related_path(original, &path))
            {
                continue;
            }
            match index.get(&path) {
                Some(true) if !filter_affected.contains(&path) => clean_paths.push(path),
                Some(true) => {}
                Some(false) => {} // assume-unchanged/skip-worktree/unmerged is not proof.
                None => match fs::symlink_metadata(root.join(&path)) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        clean_paths.push(path)
                    }
                    Ok(_) => {}
                    Err(_) => return Err(unavailable("edited-path existence is unavailable")),
                },
            }
        }

        let (commits, history_complete, history_gap) = self
            .history(
                &root,
                baseline,
                status.head.as_deref(),
                &overrides,
                deadline,
            )
            .await?;
        let (committed_paths, base_complete) = self
            .committed_paths(
                &root,
                base_oid,
                status.head.as_deref(),
                &overrides,
                deadline,
            )
            .await?;
        let work = self
            .work_probe(
                &root,
                base_oid,
                status.head.as_deref(),
                &status.dirty,
                complete,
                &overrides,
                deadline,
            )
            .await?;
        // Do not clear attribution from a poll that crossed a checkout/index/status
        // change. The history commands themselves use immutable object IDs.
        let (final_index_digest, _) = self.index_snapshot(&root, &overrides, deadline).await?;
        let final_status = self
            .checked(&root, STATUS_ARGS, &overrides, &[], deadline)
            .await?;
        if index_digest != final_index_digest || status_bytes != final_status {
            return Err(unavailable("Git worktree changed during observation"));
        }
        if !complete || !base_complete {
            clean_paths.clear();
        } else {
            clean_paths.retain(|path| {
                !committed_paths
                    .iter()
                    .any(|changed| related_path(changed, path))
            });
        }
        Ok(GitSnapshot {
            workspace: root_text.to_owned(),
            head: status.head,
            branch: status.branch,
            dirty: status.dirty,
            commits,
            clean_paths,
            observed_at: now,
            complete,
            history_complete,
            history_gap,
            base_oid: base_oid.map(str::to_owned),
            committed_paths,
            base_complete,
            work: Some(work),
        })
    }

    async fn index_snapshot(
        &self,
        root: &Path,
        overrides: &[(String, String)],
        deadline: Instant,
    ) -> Result<([u8; 32], BTreeMap<String, bool>)> {
        let command = self.build_command(root, INDEX_ARGS, overrides, None)?;
        let (code, index) = Self::capture_git(command, &[], deadline, read_index).await?;
        if code != 0 {
            return Err(unavailable("Git index readback failed"));
        }
        Ok(index)
    }

    pub(crate) async fn cached_index_paths(
        &self,
        root: &Path,
        deadline: Instant,
    ) -> Result<BTreeSet<String>> {
        let (_, index) = self.index_snapshot(root, &[], deadline).await?;
        Ok(index.into_keys().collect())
    }

    async fn filter_attributes(
        &self,
        root: &Path,
        overrides: &[(String, String)],
        paths: BTreeSet<String>,
        deadline: Instant,
    ) -> Result<BTreeSet<String>> {
        let mut affected = BTreeSet::new();
        let mut input = Vec::new();
        let mut batch = BTreeSet::new();
        let args = ["check-attr", "--stdin", "-z", "filter"];
        for path in paths {
            if !input.is_empty() && input.len() + path.len() + 1 > ATTRIBUTE_BATCH_BYTES {
                let output = self
                    .checked(root, &args, overrides, &input, deadline)
                    .await?;
                affected.extend(parse_filter_attributes(&output, &batch)?);
                batch.clear();
                input.clear();
            }
            input.extend_from_slice(path.as_bytes());
            input.push(0);
            batch.insert(path);
        }
        if !input.is_empty() {
            let output = self
                .checked(root, &args, overrides, &input, deadline)
                .await?;
            affected.extend(parse_filter_attributes(&output, &batch)?);
        }
        Ok(affected)
    }

    async fn safe_overrides(
        &self,
        root: &Path,
        deadline: Instant,
    ) -> Result<Vec<(String, String)>> {
        self.safe_overrides_for(root, deadline, None).await
    }

    async fn safe_overrides_for(
        &self,
        root: &Path,
        deadline: Instant,
        binding: Option<GitBinding<'_>>,
    ) -> Result<Vec<(String, String)>> {
        // Read keys only: filter/alias command values may contain credentials.
        let output = self
            .run_scoped(
                root,
                &[
                    "config",
                    "--null",
                    "--name-only",
                    "--get-regexp",
                    "^(filter\\..*|alias\\..*)$",
                ],
                &[],
                &[],
                deadline,
                binding,
            )
            .await?;
        if !matches!(output.code, 0 | 1) {
            return Err(unavailable("Git configuration is unavailable"));
        }
        let mut filters = BTreeSet::new();
        let mut overrides = BTreeMap::new();
        for key in nul_records(&output.stdout)? {
            if key.starts_with("alias.") {
                overrides.insert(key.to_owned(), String::new());
            } else if let Some((prefix, _)) = key.rsplit_once('.')
                && prefix.starts_with("filter.")
            {
                filters.insert(prefix.to_owned());
            }
        }
        // Git 2.55 convert.c apply_filter explicitly requires a nonempty command
        // before starting either a single-file or long-running filter. All three
        // command fields must be blank; process takes precedence over clean/smudge.
        for filter in filters {
            // check-attr uses these strings for absent/disabled attributes too.
            // Do not mistake a literal driver with the same name for no filter.
            if matches!(filter.as_str(), "filter.unset" | "filter.unspecified") {
                return Err(unavailable(
                    "Git filter name is ambiguous in attribute readback",
                ));
            }
            for command in ["clean", "smudge", "process"] {
                overrides.insert(format!("{filter}.{command}"), String::new());
            }
            overrides.insert(format!("{filter}.required"), "false".to_owned());
        }
        if overrides.len() > CONFIG_PAIRS
            || overrides
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>()
                > CONFIG_BYTES
        {
            return Err(unavailable(
                "Git executable configuration exceeds the observation bound",
            ));
        }
        Ok(overrides.into_iter().collect())
    }

    async fn history(
        &self,
        root: &Path,
        baseline: Option<&GitBaseline>,
        head: Option<&str>,
        overrides: &[(String, String)],
        deadline: Instant,
    ) -> Result<(Vec<GitCommit>, bool, bool)> {
        let Some(baseline) = baseline else {
            return Ok((Vec::new(), false, true));
        };
        if baseline.head.as_deref() == head {
            return Ok((Vec::new(), true, false));
        }
        let Some(head) = head else {
            return Ok((Vec::new(), false, baseline.head.is_some()));
        };
        if !is_git_oid(head) {
            return Err(unavailable("Git HEAD is malformed"));
        }
        let mut gap = false;
        let range = if let Some(previous) = baseline.head.as_deref() {
            if !is_git_oid(previous) {
                return Err(unavailable("stored Git baseline is malformed"));
            }
            let ancestor = self
                .run(
                    root,
                    &["merge-base", "--is-ancestor", previous, head],
                    overrides,
                    &[],
                    deadline,
                )
                .await?;
            match ancestor.code {
                0 => format!("{previous}..{head}"),
                1 => {
                    gap = true;
                    format!("{previous}..{head}")
                }
                _ => {
                    gap = true;
                    head.to_owned()
                }
            }
        } else {
            head.to_owned()
        };
        let limit = format!("--max-count={}", HISTORY_COMMITS + 1);
        let output = self
            .checked(
                root,
                &[
                    "log",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--no-decorate",
                    "--no-notes",
                    "--no-show-signature",
                    "--format=%H",
                    "-z",
                    &limit,
                    &range,
                    "--",
                ],
                overrides,
                &[],
                deadline,
            )
            .await?;
        let ids = nul_records(&output)?;
        if ids.iter().any(|oid| !is_git_oid(oid)) {
            return Err(unavailable("Git history contains a malformed object id"));
        }
        let mut complete = ids.len() <= HISTORY_COMMITS;
        let mut commits = Vec::new();
        let history_deadline = deadline
            .checked_sub(Duration::from_secs(2))
            .unwrap_or(deadline);
        for oid in ids.into_iter().take(HISTORY_COMMITS) {
            if Instant::now() >= history_deadline {
                complete = false;
                break;
            }
            let paths = match self
                .checked(
                    root,
                    &[
                        "diff-tree",
                        "--no-ext-diff",
                        "--no-textconv",
                        "--no-commit-id",
                        "--name-only",
                        "-z",
                        "-r",
                        "-m",
                        "--root",
                        "--no-renames",
                        oid,
                        "--",
                    ],
                    overrides,
                    &[],
                    history_deadline,
                )
                .await
            {
                Ok(paths) => parse_paths(&paths)?,
                Err(error) if error.code == "git_leaf_fence_unavailable" => return Err(error),
                Err(_) => {
                    complete = false;
                    break;
                }
            };
            commits.push(GitCommit {
                oid: oid.to_owned(),
                paths: paths
                    .into_iter()
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            });
        }
        Ok((commits, complete && !gap, gap))
    }

    async fn committed_paths(
        &self,
        root: &Path,
        base: Option<&str>,
        head: Option<&str>,
        overrides: &[(String, String)],
        deadline: Instant,
    ) -> Result<(Vec<String>, bool)> {
        let (Some(base), Some(head)) = (base, head) else {
            return Ok((Vec::new(), false));
        };
        if !is_git_oid(base) || !is_git_oid(head) {
            return Err(unavailable(
                "committed-path comparison requires exact object ids",
            ));
        }
        let read_deadline = deadline
            .checked_sub(Duration::from_secs(2))
            .unwrap_or(deadline);
        let output = match self
            .run(
                root,
                &[
                    "diff",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--name-only",
                    "--no-renames",
                    "-z",
                    base,
                    head,
                    "--",
                ],
                overrides,
                &[],
                read_deadline,
            )
            .await
        {
            Ok(output) if output.code == 0 => output,
            Err(error) if error.code == "git_leaf_fence_unavailable" => return Err(error),
            _ => return Ok((Vec::new(), false)),
        };
        let paths = parse_paths(&output.stdout)?;
        Ok((
            paths
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
            true,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    async fn work_probe(
        &self,
        root: &Path,
        base: Option<&str>,
        head: Option<&str>,
        dirty: &[GitPathStatus],
        complete: bool,
        overrides: &[(String, String)],
        deadline: Instant,
    ) -> Result<GitWorkProbe> {
        let unavailable_work =
            |error: &str, dirty_digest: Option<String>, has_changes: bool| GitWorkProbe {
                available: false,
                error: Some(error.to_owned()),
                commits_ahead: None,
                files_changed: None,
                insertions: None,
                deletions: None,
                diff_digest: None,
                dirty_digest,
                has_changes,
            };
        if !complete {
            return Ok(unavailable_work("partial_status", None, !dirty.is_empty()));
        }
        let dirty_result = dirty_worktree_digest(root, dirty);
        let dirty_digest = dirty_result.as_ref().ok().map(|(digest, _)| digest.clone());
        let Some(base) = base else {
            return Ok(unavailable_work(
                "no_base_oid",
                dirty_digest,
                !dirty.is_empty(),
            ));
        };
        let Some(head) = head else {
            return Ok(unavailable_work(
                "unborn_head",
                dirty_digest,
                !dirty.is_empty(),
            ));
        };
        if !is_git_oid(base) || !is_git_oid(head) {
            return Ok(unavailable_work(
                "invalid_oid",
                dirty_digest,
                !dirty.is_empty(),
            ));
        }
        let (dirty_digest, untracked_lines) = match dirty_result {
            Ok(value) => value,
            Err(reason) => {
                return Ok(unavailable_work(reason, None, !dirty.is_empty()));
            }
        };
        let read_deadline = deadline
            .checked_sub(Duration::from_secs(2))
            .unwrap_or(deadline);
        let commits_ahead = if head == base {
            0_u64
        } else {
            let ancestor = match self
                .run(
                    root,
                    &["merge-base", "--is-ancestor", base, head],
                    overrides,
                    &[],
                    read_deadline,
                )
                .await
            {
                Ok(output) => output,
                Err(error) if error.code == "git_leaf_fence_unavailable" => return Err(error),
                Err(_) => {
                    return Ok(unavailable_work(
                        "git_unavailable",
                        Some(dirty_digest),
                        !dirty.is_empty(),
                    ));
                }
            };
            if ancestor.code != 0 {
                return Ok(unavailable_work(
                    "diverged_from_base",
                    Some(dirty_digest),
                    !dirty.is_empty(),
                ));
            }
            let max_arg = format!("--max-count={}", WORK_MAX_COMMITS_AHEAD + 1);
            let range = format!("{base}..{head}");
            let count_out = match self
                .run(
                    root,
                    &["rev-list", "--count", &max_arg, &range, "--"],
                    overrides,
                    &[],
                    read_deadline,
                )
                .await
            {
                Ok(output) if output.code == 0 => output,
                Err(error) if error.code == "git_leaf_fence_unavailable" => return Err(error),
                _ => {
                    return Ok(unavailable_work(
                        "git_unavailable",
                        Some(dirty_digest),
                        !dirty.is_empty(),
                    ));
                }
            };
            let count_text = match std::str::from_utf8(&count_out.stdout) {
                Ok(text) => text.trim(),
                Err(_) => {
                    return Ok(unavailable_work(
                        "git_unavailable",
                        Some(dirty_digest),
                        !dirty.is_empty(),
                    ));
                }
            };
            let Ok(count) = count_text.parse::<u64>() else {
                return Ok(unavailable_work(
                    "git_unavailable",
                    Some(dirty_digest),
                    !dirty.is_empty(),
                ));
            };
            if count > WORK_MAX_COMMITS_AHEAD {
                return Ok(unavailable_work(
                    "cap_exceeded",
                    Some(dirty_digest),
                    !dirty.is_empty(),
                ));
            }
            count
        };
        let diff_out = match self
            .run(
                root,
                &[
                    "diff",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--numstat",
                    "--no-renames",
                    "-z",
                    base,
                    "--",
                ],
                overrides,
                &[],
                read_deadline,
            )
            .await
        {
            Ok(output) if output.code == 0 => output,
            Err(error) if error.code == "git_leaf_fence_unavailable" => return Err(error),
            _ => {
                return Ok(unavailable_work(
                    "git_unavailable",
                    Some(dirty_digest),
                    !dirty.is_empty() || commits_ahead > 0,
                ));
            }
        };
        let mut changed_paths = BTreeSet::new();
        let mut insertions = 0_u64;
        let mut deletions = 0_u64;
        let mut diff_hasher = Sha256::new();
        let records = match nul_records(&diff_out.stdout) {
            Ok(records) => records,
            Err(_) => {
                return Ok(unavailable_work(
                    "git_unavailable",
                    Some(dirty_digest),
                    !dirty.is_empty() || commits_ahead > 0,
                ));
            }
        };
        for record in records {
            let mut parts = record.splitn(3, '\t');
            let (Some(added), Some(removed), Some(raw_path)) =
                (parts.next(), parts.next(), parts.next())
            else {
                return Ok(unavailable_work(
                    "git_unavailable",
                    Some(dirty_digest),
                    !dirty.is_empty() || commits_ahead > 0,
                ));
            };
            let Ok(path) = git_path(raw_path) else {
                return Ok(unavailable_work(
                    "git_unavailable",
                    Some(dirty_digest),
                    !dirty.is_empty() || commits_ahead > 0,
                ));
            };
            if changed_paths.insert(path.clone()) && changed_paths.len() > WORK_DIFF_MAX_FILES {
                return Ok(unavailable_work("cap_exceeded", Some(dirty_digest), true));
            }
            if added != "-" {
                let Ok(value) = added.parse::<u64>() else {
                    return Ok(unavailable_work(
                        "git_unavailable",
                        Some(dirty_digest),
                        true,
                    ));
                };
                insertions = insertions.saturating_add(value);
            }
            if removed != "-" {
                let Ok(value) = removed.parse::<u64>() else {
                    return Ok(unavailable_work(
                        "git_unavailable",
                        Some(dirty_digest),
                        true,
                    ));
                };
                deletions = deletions.saturating_add(value);
            }
            hash_digest_part(&mut diff_hasher, b"diff");
            hash_digest_part(&mut diff_hasher, path.as_bytes());
            hash_digest_part(&mut diff_hasher, added.as_bytes());
            hash_digest_part(&mut diff_hasher, removed.as_bytes());
        }
        for (path, lines) in untracked_lines {
            if !changed_paths.contains(&path) {
                changed_paths.insert(path.clone());
                if changed_paths.len() > WORK_DIFF_MAX_FILES {
                    return Ok(unavailable_work("cap_exceeded", Some(dirty_digest), true));
                }
                insertions = insertions.saturating_add(lines);
                hash_digest_part(&mut diff_hasher, b"untracked");
                hash_digest_part(&mut diff_hasher, path.as_bytes());
                hash_digest_part(&mut diff_hasher, &lines.to_le_bytes());
            }
        }
        let files_changed = changed_paths.len() as u64;
        let diff_digest = hex_digest(diff_hasher.finalize().as_slice());
        Ok(GitWorkProbe {
            available: true,
            error: None,
            commits_ahead: Some(commits_ahead),
            files_changed: Some(files_changed),
            insertions: Some(insertions),
            deletions: Some(deletions),
            diff_digest: Some(diff_digest),
            dirty_digest: Some(dirty_digest),
            has_changes: files_changed > 0 || !dirty.is_empty(),
        })
    }

    async fn require_exact_root(
        &self,
        root: &Path,
        overrides: &[(String, String)],
        deadline: Instant,
    ) -> Result<()> {
        let top = self
            .checked(
                root,
                &["rev-parse", "--path-format=absolute", "--show-toplevel"],
                overrides,
                &[],
                deadline,
            )
            .await?;
        let top = std::str::from_utf8(&top).map_err(|_| unavailable("Git root is not UTF-8"))?;
        // rev-parse adds exactly one record terminator; do not trim valid spaces.
        let top = top
            .strip_suffix('\n')
            .ok_or_else(|| unavailable("Git root omitted its terminator"))?;
        if canonical_root(top)? != root {
            return Err(unavailable("workspace must be the exact Git worktree root"));
        }
        Ok(())
    }

    /// A Goal's no-progress fingerprint, read now: HEAD and the monitor's
    /// content `dirty_digest` over the dirty set. A read past the monitor's
    /// caps or of a workspace that is not an exact Git root fails.
    pub(crate) async fn work_fingerprint(&self, workspace: &str) -> Result<String> {
        let root = canonical_root(workspace)?;
        let deadline = Instant::now() + PROBE_TIME;
        let overrides = self.safe_overrides(&root, deadline).await?;
        self.require_exact_root(&root, &overrides, deadline).await?;
        let status = parse_status(
            &self
                .checked(&root, STATUS_ARGS, &overrides, &[], deadline)
                .await?,
        )?;
        let (dirty_digest, _) = dirty_worktree_digest(&root, &status.dirty)
            .map_err(|reason| unavailable(&format!("workspace content digest: {reason}")))?;
        let mut hasher = Sha256::new();
        hash_digest_part(&mut hasher, status.head.as_deref().unwrap_or("").as_bytes());
        hash_digest_part(&mut hasher, dirty_digest.as_bytes());
        Ok(hex_digest(hasher.finalize().as_slice()))
    }

    /// HEAD and the changed paths of a Goal's workspace.
    pub(crate) async fn scope_state(&self, workspace: &str) -> Result<ScopeState> {
        let root = canonical_root(workspace)?;
        let deadline = Instant::now() + PROBE_TIME;
        let overrides = self.safe_overrides(&root, deadline).await?;
        self.require_exact_root(&root, &overrides, deadline).await?;
        let status = parse_status(
            &self
                .checked(&root, SCOPE_STATUS_ARGS, &overrides, &[], deadline)
                .await?,
        )?;
        let mut changed = BTreeSet::new();
        for entry in status.dirty {
            changed.extend(entry.original_path);
            changed.insert(entry.path);
        }
        Ok(ScopeState {
            root,
            head: status.head,
            changed,
        })
    }

    /// The patch of a workspace against `base`, or against HEAD without one.
    pub(crate) async fn patch_read(
        &self,
        workspace: &str,
        base: Option<&str>,
    ) -> Result<PatchRead> {
        let root = canonical_root(workspace)?;
        let deadline = Instant::now() + PROBE_TIME;
        let overrides = self.safe_overrides(&root, deadline).await?;
        self.require_exact_root(&root, &overrides, deadline).await?;
        let status = parse_status(
            &self
                .checked(&root, SCOPE_STATUS_ARGS, &overrides, &[], deadline)
                .await?,
        )?;
        let untracked = status
            .dirty
            .iter()
            .filter(|entry| entry.status == "??")
            .map(|entry| entry.path.clone())
            .collect();
        let base = base.filter(|oid| is_git_oid(oid)).map(ToOwned::to_owned);
        let from = base
            .clone()
            .or_else(|| status.head.clone())
            .ok_or_else(|| unavailable("the patch needs a base commit or a HEAD"))?;
        // Non-ASCII paths stay readable instead of octal-quoted.
        let mut overrides = overrides;
        overrides.push(("core.quotepath".to_owned(), "false".to_owned()));
        let (diff, complete) = match self
            .run(
                &root,
                &[
                    "diff",
                    "--no-ext-diff",
                    "--no-textconv",
                    "--no-color",
                    "--no-renames",
                    "-U0",
                    &from,
                    "--",
                ],
                &overrides,
                &[],
                deadline,
            )
            .await
        {
            Ok(output) if output.code == 0 => {
                (String::from_utf8_lossy(&output.stdout).into_owned(), true)
            }
            Ok(_) => return Err(unavailable("Git could not diff the patch")),
            Err(error) if error.code == "git_leaf_fence_unavailable" => return Err(error),
            Err(_) => (String::new(), false),
        };
        let mut diff = diff;
        if !complete {
            // Over the bound: keep the changed paths so the digest still
            // names every file and package.
            // Even the name list may exceed the bound: then no paths.
            if let Ok(names) = self
                .checked(
                    &root,
                    &["diff", "--name-only", "--no-renames", "-z", &from, "--"],
                    &overrides,
                    &[],
                    deadline,
                )
                .await
            {
                for path in parse_paths(&names)? {
                    diff.push_str(&format!("diff --git a/{path} b/{path}\n"));
                }
            }
        }
        Ok(PatchRead {
            root,
            base,
            head: status.head,
            diff,
            untracked,
            complete,
        })
    }

    /// Paths whose content differs between two commits of a Goal's workspace.
    pub(crate) async fn scope_committed(
        &self,
        root: &Path,
        base: &str,
        head: &str,
    ) -> Result<Vec<String>> {
        let deadline = Instant::now() + PROBE_TIME;
        let overrides = self.safe_overrides(root, deadline).await?;
        match self
            .committed_paths(root, Some(base), Some(head), &overrides, deadline)
            .await?
        {
            (paths, true) => Ok(paths),
            (_, false) => Err(unavailable(
                "Git could not compare the starting commit with HEAD",
            )),
        }
    }

    /// Read actual workspace identity without masking it with frozen bindings.
    pub(crate) async fn workspace_command(
        &self,
        root: &Path,
        args: &[&str],
        deadline: Instant,
    ) -> Result<Captured> {
        let overrides = self.safe_overrides(root, deadline).await?;
        self.run(root, args, &overrides, &[], deadline).await
    }

    /// Read from the captured Git repository binding, even if the checkout's
    /// `.git` pointer changes after identity was established.
    pub(crate) async fn workspace_command_bound(
        &self,
        root: &Path,
        args: &[&str],
        binding: GitBinding<'_>,
        deadline: Instant,
    ) -> Result<Captured> {
        let overrides = self
            .safe_overrides_for(root, deadline, Some(binding))
            .await?;
        if Instant::now() >= deadline {
            return Err(unavailable(
                "bound workspace command preparation deadline elapsed",
            ));
        }
        self.run_scoped(root, args, &overrides, &[], deadline, Some(binding))
            .await
    }

    /// Build, but never execute, an explicitly authorized workspace mutation.
    pub(crate) async fn prepare_workspace_command(
        &self,
        root: &Path,
        args: &[&str],
        binding: GitBinding<'_>,
        deadline: Instant,
    ) -> Result<Command> {
        let overrides = self
            .safe_overrides_for(root, deadline, Some(binding))
            .await?;
        if Instant::now() >= deadline {
            return Err(unavailable(
                "workspace command preparation deadline elapsed",
            ));
        }
        self.build_command(root, args, &overrides, Some(binding))
    }

    /// Prepare a workspace mutation with executable Git configuration disabled
    /// for both the command checkout and every checkout whose configuration Git
    /// may consult while performing the mutation.
    pub(crate) async fn prepare_workspace_command_with_config_roots(
        &self,
        root: &Path,
        args: &[&str],
        binding: GitBinding<'_>,
        additional_roots: &[&Path],
        deadline: Instant,
    ) -> Result<Command> {
        let overrides = self
            .workspace_overrides(root, Some(binding), additional_roots, deadline)
            .await?;
        if Instant::now() >= deadline {
            return Err(unavailable(
                "workspace command preparation deadline elapsed",
            ));
        }
        self.build_command(root, args, &overrides, Some(binding))
    }

    /// Run a bounded read using the union of safe overrides from every relevant
    /// checkout's effective configuration.
    pub(crate) async fn workspace_command_bound_with_config_roots(
        &self,
        root: &Path,
        args: &[&str],
        binding: GitBinding<'_>,
        additional_roots: &[&Path],
        deadline: Instant,
    ) -> Result<Captured> {
        let overrides = self
            .workspace_overrides(root, Some(binding), additional_roots, deadline)
            .await?;
        if Instant::now() >= deadline {
            return Err(unavailable("bound workspace command deadline elapsed"));
        }
        self.run_scoped(root, args, &overrides, &[], deadline, Some(binding))
            .await
    }

    async fn workspace_overrides(
        &self,
        root: &Path,
        binding: Option<GitBinding<'_>>,
        additional_roots: &[&Path],
        deadline: Instant,
    ) -> Result<Vec<(String, String)>> {
        let mut merged = BTreeMap::new();
        for (config_root, config_binding) in std::iter::once((root, binding))
            .chain(additional_roots.iter().map(|path| (*path, None)))
        {
            for (key, value) in self
                .safe_overrides_for(config_root, deadline, config_binding)
                .await?
            {
                merged.insert(key, value);
            }
        }
        if merged.len() > CONFIG_PAIRS
            || merged
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>()
                > CONFIG_BYTES
        {
            return Err(unavailable(
                "Git executable configuration exceeds the observation bound",
            ));
        }
        Ok(merged.into_iter().collect())
    }

    async fn checked(
        &self,
        root: &Path,
        args: &[&str],
        overrides: &[(String, String)],
        input: &[u8],
        deadline: Instant,
    ) -> Result<Vec<u8>> {
        let output = self.run(root, args, overrides, input, deadline).await?;
        if output.code != 0 {
            return Err(unavailable("Git local readback failed"));
        }
        Ok(output.stdout)
    }

    async fn run(
        &self,
        root: &Path,
        args: &[&str],
        overrides: &[(String, String)],
        input: &[u8],
        deadline: Instant,
    ) -> Result<Captured> {
        self.run_scoped(root, args, overrides, input, deadline, None)
            .await
    }

    fn build_command(
        &self,
        root: &Path,
        args: &[&str],
        overrides: &[(String, String)],
        binding: Option<GitBinding<'_>>,
    ) -> Result<Command> {
        let mut command = Command::new(&self.program);
        command.current_dir(root).env_clear();
        if let Some(binding) = binding {
            command
                .env("GIT_DIR", git_path_argument(binding.git_dir)?)
                .env("GIT_COMMON_DIR", git_path_argument(binding.common_dir)?)
                .env("GIT_WORK_TREE", git_path_argument(root)?);
        }
        // Only the executable's trusted directory is searched by Git's own helpers.
        let executable_dir = self
            .program
            .parent()
            .ok_or_else(|| unavailable("Git executable has no absolute parent"))?;
        command.env("PATH", git_path_argument(executable_dir)?);
        // Preserve normal ignore/normalization configuration, but never inherit
        // GIT_* routing, execution, trace, index, or configuration overrides.
        for name in [
            "SystemRoot",
            "WINDIR",
            "TEMP",
            "TMP",
            "HOME",
            "USERPROFILE",
            "HOMEDRIVE",
            "HOMEPATH",
            "XDG_CONFIG_HOME",
        ] {
            if let Some(value) = env::var_os(name) {
                command.env(name, value);
            }
        }
        for (name, value) in [
            ("GIT_OPTIONAL_LOCKS", "0"),
            ("GIT_NO_LAZY_FETCH", "1"),
            ("GIT_NO_REPLACE_OBJECTS", "1"),
            ("GIT_TERMINAL_PROMPT", "0"),
            ("GIT_ASKPASS", ""),
            ("SSH_ASKPASS", ""),
            ("GIT_EDITOR", ""),
            ("GIT_SEQUENCE_EDITOR", ""),
            ("GIT_PAGER", ""),
            ("PAGER", ""),
            ("GIT_EXTERNAL_DIFF", ""),
            ("GIT_ALLOW_PROTOCOL", ""),
            ("GIT_PROTOCOL_FROM_USER", "0"),
            ("LC_ALL", "C"),
        ] {
            command.env(name, value);
        }
        // Environment key/value pairs avoid -c's ambiguous '=' split in driver names.
        command.env("GIT_CONFIG_COUNT", overrides.len().to_string());
        for (index, (key, value)) in overrides.iter().enumerate() {
            command.env(format!("GIT_CONFIG_KEY_{index}"), key);
            command.env(format!("GIT_CONFIG_VALUE_{index}"), value);
        }
        command.args(["--no-pager", "--no-optional-locks"]);
        // check-ignore --stdin -z consumes pathnames and rejects literal
        // pathspec magic. env_clear above also removes inherited pathspec flags.
        if args.first() != Some(&"check-ignore") {
            command
                .arg("--literal-pathspecs")
                .env("GIT_LITERAL_PATHSPECS", "1");
        }
        for (key, value) in [
            ("core.fsmonitor", "false"),
            ("core.preloadIndex", "false"),
            ("index.threads", "1"),
            ("checkout.workers", "1"),
            ("core.hooksPath", null_device()),
            ("core.untrackedCache", "false"),
            ("submodule.recurse", "false"),
            ("protocol.allow", "never"),
            ("log.showSignature", "false"),
            ("core.alternateRefsCommand", ""),
            ("gc.auto", "0"),
            ("maintenance.auto", "false"),
            ("color.ui", "false"),
            // Under core.autocrlf a dirty LF file makes Git warn on stderr,
            // which the leaf fence reads as a failure. safecrlf only checks
            // index check-ins, which no command built here performs.
            ("core.safecrlf", "false"),
        ] {
            command.arg("-c").arg(format!("{key}={value}"));
        }
        command
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        Ok(command)
    }

    async fn run_scoped(
        &self,
        root: &Path,
        args: &[&str],
        overrides: &[(String, String)],
        input: &[u8],
        deadline: Instant,
        binding: Option<GitBinding<'_>>,
    ) -> Result<Captured> {
        let command = self.build_command(root, args, overrides, binding)?;
        let (code, stdout) = Self::capture_git(command, input, deadline, |stdout| {
            bounded_read(stdout, OUTPUT_BYTES)
        })
        .await?;
        Ok(Captured { code, stdout })
    }
    async fn capture_git<T, F: Future<Output = Result<T>>>(
        command: Command,
        input: &[u8],
        deadline: Instant,
        read_stdout: impl FnOnce(ChildStdout) -> F,
    ) -> Result<(i32, T)> {
        if Instant::now() >= deadline {
            return Err(unavailable("Git observation deadline elapsed"));
        }
        let mut child = OwnedChild::spawn_leaf(command).map_err(|error| {
            if error.code == "unavailable" {
                Error::new("git_leaf_fence_unavailable", *error.message)
            } else {
                error
            }
        })?;
        let stdout = child
            .take_stdout()
            .ok_or_else(|| unavailable("Git stdout is unavailable"))?;
        let stderr = child
            .take_stderr()
            .ok_or_else(|| unavailable("Git stderr is unavailable"))?;
        let mut stdin = child
            .take_stdin()
            .ok_or_else(|| unavailable("Git stdin is unavailable"))?;
        let result = {
            let capture = async {
                let write = async move {
                    if !input.is_empty() {
                        stdin
                            .write_all(input)
                            .await
                            .map_err(|_| unavailable("Git stdin failed"))?;
                    }
                    stdin
                        .shutdown()
                        .await
                        .map_err(|_| unavailable("Git stdin close failed"))?;
                    drop(stdin);
                    Ok::<_, Error>(())
                };
                let (stdout, stderr, (), status) = tokio::try_join!(
                    read_stdout(stdout),
                    bounded_read(stderr, STDERR_BYTES),
                    write,
                    child.wait(),
                )?;
                let code = status
                    .code()
                    .ok_or_else(|| unavailable("Git exit code is unknown"))?;
                // Optional filters may swallow a denied child spawn and return
                // success. Never turn their diagnostics into a successful poll.
                // Retain neither the diagnostic text nor possible command secrets.
                if matches!(code, 0 | 1) && !stderr.is_empty() {
                    return Err(unavailable(
                        "Git emitted diagnostics under the leaf process fence",
                    ));
                }
                Ok::<_, Error>((code, stdout))
            };
            timeout_at(deadline, capture).await
        };
        // On timeout/cap/error, stop only this owned family and bound cleanup.
        // wait() remains leader-only; Windows separately confirms its Job empty.
        let _ = child.terminate();
        let cleanup = Instant::now() + CLEANUP_TIME;
        if !matches!(timeout_at(cleanup, child.wait()).await, Ok(Ok(_))) {
            return Err(unavailable("Git process cleanup is unconfirmed"));
        }
        #[cfg(windows)]
        if !matches!(
            child
                .wait_family_empty(cleanup.saturating_duration_since(Instant::now()))
                .await,
            Ok(true)
        ) {
            return Err(unavailable("Git family cleanup is unconfirmed"));
        }
        result.map_err(|_| unavailable("Git observation timed out"))?
    }
}

#[cfg(windows)]
fn resolve_windows_git(program: PathBuf) -> Result<PathBuf> {
    let parent = program
        .parent()
        .ok_or_else(|| unavailable("Git executable has no parent"))?;
    let directory = parent
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let launcher = directory.eq_ignore_ascii_case("cmd");
    let mut selected = program.clone();
    if (launcher || directory.eq_ignore_ascii_case("bin"))
        && let Some(root) = parent.parent()
    {
        // Git for Windows cmd/bin entry points may be launchers. Resolve a real
        // installed executable once, before any fenced read; never retry a failed
        // command with a launcher, different executable, or weaker custody.
        for architecture in ["ucrt64", "mingw64", "mingw32", "clangarm64"] {
            let native = root.join(architecture).join("bin").join("git.exe");
            if fs::metadata(&native).is_ok_and(|metadata| metadata.is_file()) {
                selected = fs::canonicalize(native)
                    .map_err(|_| unavailable("installed native Git cannot be resolved"))?;
                break;
            }
        }
    }
    if launcher && selected == program {
        return Err(unavailable(
            "Git launcher has no installed native sibling for leaf execution",
        ));
    }
    if !fs::metadata(&selected).is_ok_and(|metadata| metadata.is_file()) {
        return Err(unavailable("resolved Git executable is not a regular file"));
    }
    Ok(selected)
}

async fn bounded_read<R: AsyncRead + Unpin>(mut reader: R, cap: usize) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    let mut buffer = vec![0u8; 8192];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|_| unavailable("Git capture failed"))?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > cap {
            return Err(unavailable("Git capture exceeds the observation bound"));
        }
        output.extend_from_slice(&buffer[..read]);
    }
}

fn parse_status(bytes: &[u8]) -> Result<Status> {
    let records = nul_records(bytes)?;
    let mut records = records.into_iter();
    let mut head = None;
    let mut branch = None;
    let mut seen_head = false;
    let mut seen_branch = false;
    let mut dirty = Vec::new();
    let mut renamed_from = BTreeSet::new();
    while let Some(record) = records.next() {
        if let Some(oid) = record.strip_prefix("# branch.oid ") {
            if seen_head || (oid != "(initial)" && !is_git_oid(oid)) {
                return Err(unavailable("Git branch header is malformed"));
            }
            seen_head = true;
            head = (oid != "(initial)").then(|| oid.to_owned());
        } else if let Some(name) = record.strip_prefix("# branch.head ") {
            if seen_branch || name.is_empty() {
                return Err(unavailable("Git branch name is malformed"));
            }
            seen_branch = true;
            branch = (name != "(detached)").then(|| name.to_owned());
        } else if record.starts_with("# ") {
            continue;
        } else if let Some(path) = record.strip_prefix("? ") {
            dirty.push(GitPathStatus {
                path: git_path(path)?,
                status: "??".to_owned(),
                original_path: None,
            });
        } else if let Some(path) = record.strip_prefix("! ") {
            git_path(path)?;
        } else {
            let fields = match record.as_bytes().first() {
                Some(b'1') => 9,
                Some(b'2') => 10,
                Some(b'u') => 11,
                _ => return Err(unavailable("Git status contains an unsupported record")),
            };
            let parts: Vec<_> = record.splitn(fields, ' ').collect();
            if parts.len() != fields
                || !matches!(parts[0], "1" | "2" | "u")
                || parts[1].len() != 2
                || !parts[1].is_ascii()
            {
                return Err(unavailable("Git tracked status is malformed"));
            }
            if parts[2].starts_with('S') || parts[3..fields - 1].contains(&"160000") {
                return Err(unavailable(
                    "Git submodule status is not covered by this local probe",
                ));
            }
            let original_path = if parts[0] == "2" {
                parts[8]
                    .strip_prefix('R')
                    .or_else(|| parts[8].strip_prefix('C'))
                    .and_then(|score| score.parse::<u8>().ok())
                    .filter(|score| *score <= 100)
                    .ok_or_else(|| unavailable("Git rename/copy score is malformed"))?;
                let original = git_path(
                    records
                        .next()
                        .ok_or_else(|| unavailable("Git rename omitted its original path"))?,
                )?;
                if parts[8].starts_with('R') {
                    renamed_from.insert(original.clone());
                }
                Some(original)
            } else {
                None
            };
            dirty.push(GitPathStatus {
                path: git_path(parts[fields - 1])?,
                status: parts[1].to_owned(),
                original_path,
            });
        }
    }
    if !seen_head || !seen_branch {
        return Err(unavailable("Git status omitted its exact HEAD/branch"));
    }
    dirty.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(Status {
        head,
        branch,
        dirty,
        renamed_from,
    })
}

async fn read_index(mut stdout: ChildStdout) -> Result<([u8; 32], BTreeMap<String, bool>)> {
    let mut index = BTreeMap::<String, bool>::new();
    let mut digest = Sha256::new();
    let mut buffer = [0u8; 8192];
    let mut record = Vec::new();
    let mut records = 0;
    let mut bytes = 0;
    loop {
        let read = stdout
            .read(&mut buffer)
            .await
            .map_err(|_| unavailable("Git index capture failed"))?;
        if read == 0 {
            break;
        }
        bytes += read;
        if bytes > INDEX_TOTAL_BYTES {
            return Err(unavailable(&format!(
                "Git index scan read {bytes} bytes and exceeded the {INDEX_TOTAL_BYTES}-byte bound"
            )));
        }
        digest.update(&buffer[..read]);
        for chunk in buffer[..read].split_inclusive(|byte| *byte == 0) {
            if record.len() + chunk.len() > INDEX_RECORD_BYTES {
                return Err(unavailable(
                    "Git index record exceeds the observation bound",
                ));
            }
            if let Some(record_end) = chunk.strip_suffix(&[0]) {
                record.extend_from_slice(record_end);
                records += 1;
                if records > INDEX_RECORDS {
                    return Err(unavailable(&format!(
                        "Git index scan read {records} records and exceeded the {INDEX_RECORDS}-record bound"
                    )));
                }
                add_index_record(&mut index, &record)?;
                record.clear();
            } else {
                record.extend_from_slice(chunk);
            }
        }
    }
    if !record.is_empty() {
        return Err(unavailable("Git index record is truncated"));
    }
    Ok((digest.finalize().into(), index))
}

fn add_index_record(index: &mut BTreeMap<String, bool>, record: &[u8]) -> Result<()> {
    let record = std::str::from_utf8(record)
        .map_err(|_| unavailable("Git index record contains non-UTF-8 data"))?;
    let (metadata, path) = record
        .split_once('\t')
        .ok_or_else(|| unavailable("Git index record is malformed"))?;
    let fields: Vec<_> = metadata.split(' ').collect();
    if fields.len() != 4 || fields[0].len() != 1 || !is_git_oid(fields[2]) {
        return Err(unavailable("Git index metadata is malformed"));
    }
    if fields[1] == "160000" {
        return Err(unavailable(
            "Git submodule worktrees are not covered by this local probe",
        ));
    }
    let path = git_path(path)?;
    let clean_eligible = fields[0] == "H" && fields[3] == "0";
    index
        .entry(path)
        .and_modify(|eligible| *eligible &= clean_eligible)
        .or_insert(clean_eligible);
    Ok(())
}

fn nul_records(bytes: &[u8]) -> Result<Vec<&str>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| unavailable("Git output contains non-UTF-8 data"))?;
    let text = text
        .strip_suffix('\0')
        .ok_or_else(|| unavailable("Git NUL record is truncated"))?;
    let records: Vec<_> = text.split('\0').collect();
    if records.iter().any(|record| record.is_empty()) {
        return Err(unavailable("Git output contains an empty record"));
    }
    Ok(records)
}

fn parse_paths(bytes: &[u8]) -> Result<Vec<String>> {
    nul_records(bytes)?.into_iter().map(git_path).collect()
}

fn parse_filter_attributes(bytes: &[u8], expected: &BTreeSet<String>) -> Result<BTreeSet<String>> {
    let records = nul_records(bytes)?;
    if records.len() != expected.len() * 3 {
        return Err(unavailable("Git filter attribute coverage is incomplete"));
    }
    let mut seen = BTreeSet::new();
    let mut affected = BTreeSet::new();
    for entry in records.as_chunks::<3>().0 {
        let path = git_path(entry[0])?;
        if entry[1] != "filter" || !expected.contains(&path) || !seen.insert(path.clone()) {
            return Err(unavailable("Git filter attribute identity is malformed"));
        }
        if !matches!(entry[2], "unspecified" | "unset") {
            affected.insert(path);
        }
    }
    Ok(affected)
}

fn related_path(left: &str, right: &str) -> bool {
    left == right
        || left
            .strip_prefix(right)
            .is_some_and(|tail| tail.starts_with('/'))
        || right
            .strip_prefix(left)
            .is_some_and(|tail| tail.starts_with('/'))
}

fn git_path(path: &str) -> Result<String> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\0')
        || path
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | ".."))
    {
        return Err(unavailable("Git path is not a canonical relative path"));
    }
    Ok(path.to_owned())
}

/// Normalize verified native file-change hints, including deleted files, without
/// following a symlink/reparse point outside (or within) the selected worktree.
pub(crate) fn normalize_hint(workspace: &str, path: &str) -> Result<String> {
    let root = canonical_root(workspace)?;
    if path.is_empty() || path.contains('\0') {
        return Err(unavailable("file-change hint is empty or malformed"));
    }
    let input = Path::new(path);
    if input
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(unavailable("file-change hint contains parent traversal"));
    }
    if !input.is_absolute()
        && input
            .components()
            .any(|part| matches!(part, Component::RootDir | Component::Prefix(_)))
    {
        return Err(unavailable("file-change hint has an ambiguous root"));
    }
    let target = if input.is_absolute() {
        input.to_path_buf()
    } else {
        root.join(input)
    };
    reject_links(&target)?;
    let mut existing = target.clone();
    let mut missing: Vec<OsString> = Vec::new();
    loop {
        match fs::symlink_metadata(&existing) {
            Ok(metadata) => {
                if !missing.is_empty() && !metadata.is_dir() {
                    return Err(unavailable("hint parent is not a directory"));
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(
                    existing
                        .file_name()
                        .ok_or_else(|| unavailable("hint has no existing root"))?
                        .to_owned(),
                );
                if !existing.pop() {
                    return Err(unavailable("hint has no existing parent"));
                }
            }
            Err(_) => return Err(unavailable("hint path cannot be inspected")),
        }
    }
    let mut canonical =
        fs::canonicalize(existing).map_err(|_| unavailable("hint parent cannot be resolved"))?;
    for component in missing.into_iter().rev() {
        canonical.push(component);
    }
    let relative = canonical
        .strip_prefix(&root)
        .map_err(|_| unavailable("file-change hint escapes its worktree"))?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(name) => {
                let name = name
                    .to_str()
                    .ok_or_else(|| unavailable("hint path is not UTF-8"))?;
                #[cfg(windows)]
                if name.contains(':') {
                    return Err(unavailable("hint path contains a Windows alternate stream"));
                }
                parts.push(name);
            }
            Component::CurDir => {}
            _ => return Err(unavailable("hint path has an invalid component")),
        }
    }
    git_path(&parts.join("/"))
}

/// Git's path parser does not accept Windows verbatim prefixes in its own
/// routing values. Keep OS paths opaque and convert only at the Git boundary.
pub(crate) fn git_path_argument(path: &Path) -> Result<String> {
    let text = path
        .to_str()
        .ok_or_else(|| unavailable("Git-facing path is not UTF-8"))?;
    #[cfg(windows)]
    {
        let text = text.replace('\\', "/");
        if let Some(share) = text.strip_prefix("//?/UNC/") {
            return Ok(format!("//{share}"));
        }
        Ok(text.strip_prefix("//?/").unwrap_or(&text).to_owned())
    }
    #[cfg(not(windows))]
    {
        Ok(text.to_owned())
    }
}

pub(crate) fn canonical_root(workspace: &str) -> Result<PathBuf> {
    let path = Path::new(workspace);
    if !path.is_absolute() {
        return Err(unavailable("workspace must be absolute"));
    }
    reject_links(path)?;
    let root = fs::canonicalize(path).map_err(|_| unavailable("workspace cannot be resolved"))?;
    if !root.is_dir() {
        return Err(unavailable("workspace is not a directory"));
    }
    Ok(root)
}

pub(crate) fn reject_links(path: &Path) -> Result<()> {
    let ancestors: Vec<_> = path.ancestors().collect();
    for prefix in ancestors.into_iter().rev() {
        match fs::symlink_metadata(prefix) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
                    return Err(unavailable(
                        "workspace path contains a symlink or reparse point",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(unavailable("workspace path cannot be inspected")),
        }
    }
    Ok(())
}

fn null_device() -> &'static str {
    if cfg!(windows) {
        "//./NUL"
    } else {
        "/dev/null"
    }
}

fn unavailable(message: &str) -> Error {
    Error::new("unavailable", message)
}

fn dirty_worktree_digest(
    root: &Path,
    dirty: &[GitPathStatus],
) -> std::result::Result<(String, BTreeMap<String, u64>), &'static str> {
    if dirty.len() > WORK_DIFF_MAX_FILES {
        return Err("cap_exceeded");
    }
    let mut hasher = Sha256::new();
    let mut untracked_lines = BTreeMap::new();
    let mut total_bytes = 0_u64;
    for entry in dirty {
        hash_digest_part(&mut hasher, b"entry");
        hash_digest_part(&mut hasher, entry.path.as_bytes());
        hash_digest_part(&mut hasher, entry.status.as_bytes());
        if let Some(orig) = &entry.original_path {
            hash_digest_part(&mut hasher, orig.as_bytes());
        }
        match read_path_content(
            root,
            &entry.path,
            WORK_UNTRACKED_MAX_BYTES_PER_FILE,
            WORK_UNTRACKED_MAX_TOTAL_BYTES,
            &mut total_bytes,
        )? {
            PathContent::Link(target) => {
                hash_digest_part(&mut hasher, b"symlink");
                hash_digest_part(&mut hasher, target.to_string_lossy().as_bytes());
                if entry.status == "??" {
                    untracked_lines.insert(entry.path.clone(), 0);
                }
            }
            PathContent::File(bytes) => {
                let content_hash = Sha256::digest(&bytes);
                hash_digest_part(&mut hasher, b"file");
                hash_digest_part(&mut hasher, &(bytes.len() as u64).to_le_bytes());
                hash_digest_part(&mut hasher, content_hash.as_slice());
                if entry.status == "??" {
                    let lines = if bytes.contains(&0) {
                        0
                    } else {
                        count_text_lines(&bytes)
                    };
                    untracked_lines.insert(entry.path.clone(), lines);
                }
            }
            PathContent::Other => {
                hash_digest_part(&mut hasher, b"other");
                if entry.status == "??" {
                    untracked_lines.insert(entry.path.clone(), 0);
                }
            }
            PathContent::Missing => hash_digest_part(&mut hasher, b"missing"),
        }
    }
    Ok((hex_digest(hasher.finalize().as_slice()), untracked_lines))
}

/// Content of one Git path under `root`, read without following a link or
/// non-directory among its parents.
enum PathContent {
    Missing,
    Link(PathBuf),
    File(Vec<u8>),
    Other,
}

fn read_path_content(
    root: &Path,
    path: &str,
    max_file_bytes: u64,
    max_total_bytes: u64,
    total_bytes: &mut u64,
) -> std::result::Result<PathContent, &'static str> {
    let mut current = root.to_path_buf();
    let parts: Vec<&str> = path.split('/').collect();
    for (index, part) in parts.iter().enumerate() {
        current.push(part);
        let meta = match fs::symlink_metadata(&current) {
            Ok(meta) => meta,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PathContent::Missing);
            }
            Err(_) => return Err("unavailable"),
        };
        let link = meta.file_type().is_symlink() || is_reparse_point(&meta);
        if index + 1 < parts.len() {
            if link || !meta.is_dir() {
                return Err("unavailable");
            }
            continue;
        }
        if link {
            return fs::read_link(&current)
                .map(PathContent::Link)
                .map_err(|_| "unavailable");
        }
        if !meta.is_file() {
            return Ok(PathContent::Other);
        }
        let len = meta.len();
        if len > max_file_bytes || total_bytes.saturating_add(len) > max_total_bytes {
            return Err("cap_exceeded");
        }
        let file = fs::File::open(&current).map_err(|_| "unavailable")?;
        let max_read = max_file_bytes.min(max_total_bytes - *total_bytes);
        let mut bytes = Vec::with_capacity(len.min(max_read) as usize);
        file.take(max_read.saturating_add(1))
            .read_to_end(&mut bytes)
            .map_err(|_| "unavailable")?;
        if bytes.len() as u64 > max_read {
            return Err("cap_exceeded");
        }
        *total_bytes = total_bytes.saturating_add(bytes.len() as u64);
        return Ok(PathContent::File(bytes));
    }
    Err("unavailable")
}

/// Content digest of each path for a Goal's change scope: the file bytes,
/// the link target, `other` or `missing`. Equal digests mean equal content.
pub(crate) fn scope_digests<'a>(
    root: &Path,
    paths: impl IntoIterator<Item = &'a str>,
) -> Result<BTreeMap<String, String>> {
    let mut total_bytes = 0_u64;
    let mut digests = BTreeMap::new();
    for path in paths {
        let path = git_path(path)?;
        let digest = match read_path_content(
            root,
            &path,
            SCOPE_READ_MAX_BYTES,
            SCOPE_READ_MAX_BYTES,
            &mut total_bytes,
        )
        .map_err(|reason| unavailable(&format!("scope content read failed: {reason}")))?
        {
            PathContent::Missing => "missing".to_owned(),
            PathContent::Other => "other".to_owned(),
            PathContent::Link(target) => format!(
                "link:{}",
                &hex_digest(&Sha256::digest(target.to_string_lossy().as_bytes()))[..32]
            ),
            PathContent::File(bytes) => {
                format!("file:{}", &hex_digest(&Sha256::digest(&bytes))[..32])
            }
        };
        digests.insert(path, digest);
    }
    Ok(digests)
}

fn count_text_lines(bytes: &[u8]) -> u64 {
    if bytes.is_empty() {
        return 0;
    }
    let newlines = bytes.iter().filter(|byte| **byte == b'\n').count() as u64;
    if bytes.ends_with(b"\n") {
        newlines
    } else {
        newlines.saturating_add(1)
    }
}

fn hash_digest_part(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
