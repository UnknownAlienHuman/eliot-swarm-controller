//! Workspace Git identity and command preparation. Only the manager executes
//! mutations, under its durable cancellation/spawn gate and ordinary custody.

use crate::git_monitor::{
    GitBinding, GitProbe, canonical_root, git_path_argument, is_oid, reject_links,
};
use eliot_core::{
    Error, Result, WorkspaceCreationOutcome, WorkspacePhase, WorkspacePreparation,
    WorkspaceRemovalOutcome,
};
use std::{
    collections::BTreeSet,
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    process::Command,
    time::{Instant, timeout_at},
};

const READ_TIME: Duration = Duration::from_secs(15);
const METADATA_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct FrozenWorkspaceGit {
    pub repo_path: String,
    pub repo_common_dir: String,
    pub requested_branch: String,
    pub base_oid: String,
    pub worktree_path: String,
}

#[derive(Clone)]
pub(crate) struct WorkspaceGit {
    git: GitProbe,
}

struct Repository {
    root: PathBuf,
    git_dir: PathBuf,
    common_dir: PathBuf,
}

struct WorktreeEntry {
    path: PathBuf,
    branch: Option<String>,
    head: Option<String>,
    incomplete: bool,
}

struct Subject {
    source: Repository,
    target: Repository,
    branch: String,
    head: String,
}

impl WorkspaceGit {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            git: GitProbe::new()?,
        })
    }

    /// Read the exact canonical checkout and repository common-directory identity.
    /// This is a bounded, read-only Git preflight and performs no Store work.
    pub(crate) async fn repository_identity(&self, path: &str) -> Result<(String, String)> {
        let repository = self.repository(path, Instant::now() + READ_TIME).await?;
        Ok((
            path_text(&repository.root)?,
            path_text(&repository.common_dir)?,
        ))
    }

    pub(crate) async fn freeze(
        &self,
        repo_path: &str,
        branch: &str,
        base_ref: Option<&str>,
        target: &str,
    ) -> Result<FrozenWorkspaceGit> {
        let deadline = Instant::now() + READ_TIME;
        let source = self.repository(repo_path, deadline).await?;
        self.validate_branch(&source.root, branch, deadline).await?;
        let destination = destination_path(target)?;
        separate_target(&source, &destination)?;
        require_absent(&destination)?;
        if self
            .branch_oid(&source.root, branch, deadline)
            .await?
            .is_some()
        {
            return Err(problem("already_exists", "workspace branch must be unused"));
        }
        let entries = self.entries(&source.root, deadline).await?;
        let full_ref = branch_ref(branch);
        if entries.iter().any(|entry| {
            entry.path == destination || entry.branch.as_deref() == Some(full_ref.as_str())
        }) {
            return Err(problem(
                "already_exists",
                "workspace path or branch is already registered",
            ));
        }
        let reference = base_ref.unwrap_or("HEAD");
        if reference.is_empty() || reference.contains('\0') {
            return Err(problem(
                "invalid",
                "workspace base reference is empty or malformed",
            ));
        }
        let commit = format!("{reference}^{{commit}}");
        let base_oid = self
            .read_line(
                &source.root,
                &["rev-parse", "--verify", "--end-of-options", &commit],
                deadline,
            )
            .await?;
        if !is_oid(&base_oid) {
            return Err(problem(
                "source_integrity",
                "resolved workspace base is not a full commit id",
            ));
        }
        let verified = self.repository(repo_path, deadline).await?;
        if source.root != verified.root
            || source.git_dir != verified.git_dir
            || source.common_dir != verified.common_dir
        {
            return Err(problem(
                "source_integrity",
                "source repository identity changed during freeze",
            ));
        }
        Ok(FrozenWorkspaceGit {
            repo_path: path_text(&source.root)?,
            repo_common_dir: path_text(&source.common_dir)?,
            requested_branch: branch.to_owned(),
            base_oid,
            worktree_path: path_text(&destination)?,
        })
    }

    /// Requires the manager's durable `creating` intent. This never spawns the
    /// returned mutator and cannot repair/replay a partial previous creation.
    pub(crate) async fn prepare_create(
        &self,
        preparation: &WorkspacePreparation,
    ) -> Result<Command> {
        if preparation.phase != WorkspacePhase::Creating {
            return Err(problem(
                "bad_state",
                "workspace create requires its creating marker",
            ));
        }
        let deadline = Instant::now() + READ_TIME;
        let source = self.source(preparation, deadline).await?;
        self.validate_branch(&source.root, &preparation.requested_branch, deadline)
            .await?;
        let target = destination_path(&preparation.worktree_path)?;
        separate_target(&source, &target)?;
        require_absent(&target)?;
        if self
            .branch_oid(&source.root, &preparation.requested_branch, deadline)
            .await?
            .is_some()
        {
            return Err(problem(
                "already_exists",
                "workspace branch already exists; inspect instead of replaying create",
            ));
        }
        let full_ref = branch_ref(&preparation.requested_branch);
        if self
            .entries(&source.root, deadline)
            .await?
            .iter()
            .any(|entry| entry.path == target || entry.branch.as_deref() == Some(full_ref.as_str()))
        {
            return Err(problem(
                "already_exists",
                "workspace registration already exists; inspect instead of replaying create",
            ));
        }
        let commit = format!("{}^{{commit}}", preparation.base_oid);
        let actual = self
            .read_line(
                &source.root,
                &["rev-parse", "--verify", "--end-of-options", &commit],
                deadline,
            )
            .await?;
        if actual != preparation.base_oid {
            return Err(problem(
                "source_integrity",
                "frozen base commit cannot be read exactly",
            ));
        }
        let target_text = git_path_argument(&target)?;
        self.git
            .prepare_workspace_command(
                &source.root,
                &[
                    "worktree",
                    "add",
                    "--no-guess-remote",
                    "--no-relative-paths",
                    "-b",
                    &preparation.requested_branch,
                    "--",
                    &target_text,
                    &preparation.base_oid,
                ],
                GitBinding {
                    git_dir: &source.git_dir,
                    common_dir: &source.common_dir,
                },
                deadline,
            )
            .await
    }

    /// Call only after the manager confirms the previous mutator is quiescent.
    pub(crate) async fn inspect(
        &self,
        preparation: &WorkspacePreparation,
    ) -> WorkspaceCreationOutcome {
        match self.inspect_created(preparation).await {
            Ok(outcome) => outcome,
            Err(error) => WorkspaceCreationOutcome::Unknown {
                code: error.code,
                message: error.message,
            },
        }
    }

    /// Identity-only readback for an already accepted workspace. Dirty files and
    /// legitimate branch advancement are allowed; this cannot prove initial
    /// creation/population or authorize setup replay. Prior mutators must be quiescent.
    pub(crate) async fn inspect_retained(
        &self,
        preparation: &WorkspacePreparation,
    ) -> WorkspaceCreationOutcome {
        let readback = async {
            let deadline = Instant::now() + READ_TIME;
            let subject = self.subject(preparation, deadline).await?;
            self.confirm_subject(preparation, &subject, deadline)
                .await?;
            Ok::<_, Error>(WorkspaceCreationOutcome::Verified {
                repo_path: path_text(&subject.source.root)?,
                repo_common_dir: path_text(&subject.source.common_dir)?,
                worktree_path: path_text(&subject.target.root)?,
                branch: preparation.requested_branch.clone(),
                head: subject.head,
            })
        }
        .await;
        match readback {
            Ok(outcome) => outcome,
            Err(error) => WorkspaceCreationOutcome::Unknown {
                code: error.code,
                message: error.message,
            },
        }
    }

    async fn inspect_created(
        &self,
        preparation: &WorkspacePreparation,
    ) -> Result<WorkspaceCreationOutcome> {
        let deadline = Instant::now() + READ_TIME;
        let source = self.source(preparation, deadline).await?;
        let target = destination_path(&preparation.worktree_path)?;
        separate_target(&source, &target)?;
        let entries = self.entries(&source.root, deadline).await?;
        let registered = entries.iter().any(|entry| entry.path == target);
        let branch = self
            .branch_oid(&source.root, &preparation.requested_branch, deadline)
            .await?;
        if !registered && absent(&target)? && branch.is_none() {
            return Ok(WorkspaceCreationOutcome::Failed {
                code: "not_created".to_owned(),
                message: "no workspace path, branch, or registration exists".to_owned(),
            });
        }
        let subject = self.subject(preparation, deadline).await?;
        if subject.head != preparation.base_oid {
            return Err(problem(
                "source_integrity",
                "created workspace HEAD differs from the frozen base",
            ));
        }
        self.require_clean(&subject, true, deadline).await?;
        self.confirm_subject(preparation, &subject, deadline)
            .await?;
        Ok(WorkspaceCreationOutcome::Verified {
            repo_path: path_text(&subject.source.root)?,
            repo_common_dir: path_text(&subject.source.common_dir)?,
            worktree_path: path_text(&subject.target.root)?,
            branch: preparation.requested_branch.clone(),
            head: subject.head,
        })
    }

    /// Read-only full preflight. The manager holds its removing/cancellation gate
    /// before deleting returned regular files individually; never delete a scratch
    /// directory recursively or use this list as authority for tracked files.
    pub(crate) async fn scratch_candidates(
        &self,
        preparation: &WorkspacePreparation,
        scratch: &[String],
    ) -> Result<Vec<PathBuf>> {
        let deadline = Instant::now() + READ_TIME;
        let subject = self.subject(preparation, deadline).await?;
        self.require_tracked_clean(&subject, deadline).await?;
        let root = &subject.target.root;
        let mut allowed = BTreeSet::new();
        for prefix in scratch {
            if Instant::now() >= deadline {
                return Err(problem("unavailable", "scratch preflight deadline elapsed"));
            }
            let relative = scratch_relative(prefix)?;
            let prefix = destination_path(&path_text(&root.join(relative))?)?;
            if prefix == *root || !prefix.starts_with(root) {
                return Err(problem(
                    "not_allowed",
                    "scratch prefix escapes the exact worktree",
                ));
            }
            allowed.insert(prefix);
        }

        let cached_args = ["ls-files", "--cached", "-z"];
        let other_args = ["ls-files", "--others", "--exclude-standard", "-z"];
        let ignored_args = [
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "-z",
        ];
        let cached = scratch_paths(&self.read(root, &cached_args, deadline).await?)?;
        let other = scratch_paths(&self.read(root, &other_args, deadline).await?)?;
        let ignored = scratch_paths(&self.read(root, &ignored_args, deadline).await?)?;
        let mut tracked = BTreeSet::new();
        for relative in &cached {
            if Instant::now() >= deadline {
                return Err(problem("unavailable", "scratch preflight deadline elapsed"));
            }
            let path = root.join(relative);
            let metadata = fs::symlink_metadata(&path).map_err(|_| {
                problem(
                    "unavailable",
                    "tracked scratch preflight metadata is unavailable",
                )
            })?;
            if metadata.file_type().is_file() {
                // Canonical file names also prevent case aliases of clean tracked
                // files on Windows from becoming scratch deletion candidates.
                tracked.insert(destination_path(&path_text(&path)?)?);
            }
        }
        let mut candidates = BTreeSet::new();
        for relative in other.union(&ignored) {
            if Instant::now() >= deadline {
                return Err(problem("unavailable", "scratch preflight deadline elapsed"));
            }
            let path = destination_path(&path_text(&root.join(relative))?)?;
            if path == *root || !path.starts_with(root) {
                return Err(problem(
                    "not_allowed",
                    "scratch candidate escapes the exact worktree",
                ));
            }
            let metadata = fs::symlink_metadata(&path)
                .map_err(|_| problem("unavailable", "scratch candidate metadata is unavailable"))?;
            if !metadata.file_type().is_file() {
                return Err(problem(
                    "unavailable",
                    "scratch coverage includes a directory, symlink, or nonregular file",
                ));
            }
            if tracked.contains(&path) {
                continue;
            }
            if !allowed.iter().any(|prefix| path.starts_with(prefix)) {
                return Err(problem(
                    "not_allowed",
                    "untracked or ignored file lies outside allowed scratch",
                ));
            }
            candidates.insert(path);
        }

        self.require_tracked_clean(&subject, deadline).await?;
        if cached != scratch_paths(&self.read(root, &cached_args, deadline).await?)?
            || other != scratch_paths(&self.read(root, &other_args, deadline).await?)?
            || ignored != scratch_paths(&self.read(root, &ignored_args, deadline).await?)?
        {
            return Err(problem(
                "unavailable",
                "scratch/index coverage changed during preflight",
            ));
        }
        self.confirm_subject(preparation, &subject, deadline)
            .await?;
        Ok(candidates.into_iter().collect())
    }

    async fn require_tracked_clean(&self, subject: &Subject, deadline: Instant) -> Result<()> {
        let path = path_text(&subject.target.root)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|value| i64::try_from(value.as_millis()).ok())
            .ok_or_else(|| problem("unavailable", "scratch observation clock is unavailable"))?;
        let snapshot = timeout_at(
            deadline,
            self.git.probe(&path, None, Some(&subject.head), &[], now),
        )
        .await
        .map_err(|_| problem("unavailable", "scratch Git coverage timed out"))??;
        if !snapshot.complete
            || snapshot.head.as_deref() != Some(subject.head.as_str())
            || snapshot.branch.as_deref() != subject.branch.strip_prefix("refs/heads/")
        {
            return Err(problem(
                "unavailable",
                "scratch Git coverage is incomplete or changed identity",
            ));
        }
        if snapshot.dirty.iter().any(|entry| entry.status != "??") {
            return Err(problem(
                "dirty",
                "tracked changes forbid all scratch cleanup",
            ));
        }
        Ok(())
    }

    /// Only prepares ordinary, non-force removal. The manager removes explicitly
    /// allowed scratch after committing `removing`, before calling this method.
    pub(crate) async fn prepare_remove(
        &self,
        preparation: &WorkspacePreparation,
    ) -> Result<Command> {
        if preparation.phase != WorkspacePhase::Removing {
            return Err(problem(
                "bad_state",
                "workspace remove requires its removing marker",
            ));
        }
        let deadline = Instant::now() + READ_TIME;
        let subject = self.subject(preparation, deadline).await?;
        self.require_clean(&subject, true, deadline).await?;
        self.confirm_subject(preparation, &subject, deadline)
            .await?;
        let target = git_path_argument(&subject.target.root)?;
        self.git
            .prepare_workspace_command(
                &subject.source.root,
                &["worktree", "remove", "--", &target],
                GitBinding {
                    git_dir: &subject.source.git_dir,
                    common_dir: &subject.source.common_dir,
                },
                deadline,
            )
            .await
    }

    /// Call only after confirmed mutator quiescence. Present/partial/conflicting
    /// state remains Unknown; an exit status alone is never a removal proof.
    pub(crate) async fn inspect_removal(
        &self,
        preparation: &WorkspacePreparation,
    ) -> WorkspaceRemovalOutcome {
        match self.inspect_removed(preparation).await {
            Ok(outcome) => outcome,
            Err(error) => WorkspaceRemovalOutcome::Unknown {
                code: error.code,
                message: error.message,
            },
        }
    }

    async fn inspect_removed(
        &self,
        preparation: &WorkspacePreparation,
    ) -> Result<WorkspaceRemovalOutcome> {
        let deadline = Instant::now() + READ_TIME;
        let source = self.source(preparation, deadline).await?;
        let target = destination_path(&preparation.worktree_path)?;
        separate_target(&source, &target)?;
        if !absent(&target)?
            || self
                .entries(&source.root, deadline)
                .await?
                .iter()
                .any(|entry| entry.path == target)
        {
            return Err(problem(
                "unknown",
                "workspace removal is incomplete; no repair or replay was attempted",
            ));
        }
        if self
            .branch_oid(&source.root, &preparation.requested_branch, deadline)
            .await?
            .is_none()
        {
            return Err(problem(
                "source_integrity",
                "removed workspace branch was not retained",
            ));
        }
        Ok(WorkspaceRemovalOutcome::Removed {
            repo_common_dir: path_text(&source.common_dir)?,
            worktree_path: path_text(&target)?,
            branch: preparation.requested_branch.clone(),
            branch_retained: true,
        })
    }

    async fn source(
        &self,
        preparation: &WorkspacePreparation,
        deadline: Instant,
    ) -> Result<Repository> {
        if !is_oid(&preparation.base_oid) {
            return Err(problem(
                "invalid",
                "workspace preparation omitted its exact frozen base",
            ));
        }
        let source = self.repository(&preparation.repo_path, deadline).await?;
        if source.common_dir != canonical_root(&preparation.repo_common_dir)? {
            return Err(problem(
                "source_integrity",
                "source repository common directory differs from the frozen identity",
            ));
        }
        Ok(source)
    }

    async fn repository(&self, path: &str, deadline: Instant) -> Result<Repository> {
        let root = canonical_root(path)?;
        let actual_root = self
            .read_line(
                &root,
                &["rev-parse", "--path-format=absolute", "--show-toplevel"],
                deadline,
            )
            .await?;
        if canonical_root(&actual_root)? != root {
            return Err(problem(
                "source_integrity",
                "workspace path is not the exact repository root",
            ));
        }
        let git_dir = self
            .read_line(&root, &["rev-parse", "--absolute-git-dir"], deadline)
            .await?;
        let common_dir = self
            .read_line(
                &root,
                &["rev-parse", "--path-format=absolute", "--git-common-dir"],
                deadline,
            )
            .await?;
        Ok(Repository {
            root,
            git_dir: canonical_root(&git_dir)?,
            common_dir: canonical_root(&common_dir)?,
        })
    }

    async fn subject(
        &self,
        preparation: &WorkspacePreparation,
        deadline: Instant,
    ) -> Result<Subject> {
        let source = self.source(preparation, deadline).await?;
        let target_path = destination_path(&preparation.worktree_path)?;
        separate_target(&source, &target_path)?;
        let entries = self.entries(&source.root, deadline).await?;
        let mut matches = entries.iter().filter(|entry| entry.path == target_path);
        let entry = matches
            .next()
            .ok_or_else(|| problem("unknown", "workspace registration is absent or partial"))?;
        if matches.next().is_some() || entry.incomplete {
            return Err(problem(
                "source_integrity",
                "workspace registration is duplicate, locked, prunable, or incomplete",
            ));
        }
        let full_ref = branch_ref(&preparation.requested_branch);
        if entry.branch.as_deref() != Some(full_ref.as_str()) {
            return Err(problem(
                "source_integrity",
                "registered workspace branch differs from the requested branch",
            ));
        }
        let target = self.repository(&path_text(&target_path)?, deadline).await?;
        let registry_root = canonical_root(&path_text(&source.common_dir.join("worktrees"))?)?;
        if target.common_dir != source.common_dir
            || target.git_dir.parent() != Some(registry_root.as_path())
        {
            return Err(problem(
                "source_integrity",
                "workspace is not linked to the frozen source repository",
            ));
        }
        let back_pointer = metadata_line(&target.git_dir.join("gitdir"))?;
        if destination_path(&back_pointer)? != target.root.join(".git") {
            return Err(problem(
                "source_integrity",
                "workspace Git registration is not reciprocal",
            ));
        }
        let branch = self
            .read_line(&target.root, &["symbolic-ref", "--quiet", "HEAD"], deadline)
            .await?;
        let head = self
            .read_line(
                &target.root,
                &["rev-parse", "--verify", "--end-of-options", "HEAD^{commit}"],
                deadline,
            )
            .await?;
        if branch != full_ref || !is_oid(&head) || entry.head.as_deref() != Some(head.as_str()) {
            return Err(problem(
                "source_integrity",
                "workspace HEAD, branch, or registration disagree",
            ));
        }
        Ok(Subject {
            source,
            target,
            branch,
            head,
        })
    }

    async fn require_clean(
        &self,
        subject: &Subject,
        include_ignored: bool,
        deadline: Instant,
    ) -> Result<()> {
        let path = path_text(&subject.target.root)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|value| i64::try_from(value.as_millis()).ok())
            .ok_or_else(|| problem("unavailable", "workspace observation clock is unavailable"))?;
        let snapshot = timeout_at(
            deadline,
            self.git.probe(&path, None, Some(&subject.head), &[], now),
        )
        .await
        .map_err(|_| problem("unavailable", "workspace clean readback timed out"))??;
        if !snapshot.complete
            || snapshot.head.as_deref() != Some(subject.head.as_str())
            || snapshot.branch.as_deref() != subject.branch.strip_prefix("refs/heads/")
            || !snapshot.dirty.is_empty()
        {
            return Err(problem(
                "dirty",
                "workspace population/cleanliness is incomplete, filtered, hidden, or dirty",
            ));
        }
        if include_ignored {
            let ignored = self
                .read(
                    &subject.target.root,
                    &[
                        "ls-files",
                        "--others",
                        "--ignored",
                        "--exclude-standard",
                        "-z",
                    ],
                    deadline,
                )
                .await?;
            if !ignored.is_empty() {
                return Err(problem(
                    "dirty",
                    "workspace contains ignored files; only the manager may delete explicitly allowed scratch",
                ));
            }
        }
        Ok(())
    }

    async fn confirm_subject(
        &self,
        preparation: &WorkspacePreparation,
        before: &Subject,
        deadline: Instant,
    ) -> Result<()> {
        let after = self.subject(preparation, deadline).await?;
        if before.source.root != after.source.root
            || before.source.git_dir != after.source.git_dir
            || before.source.common_dir != after.source.common_dir
            || before.target.git_dir != after.target.git_dir
            || before.target.root != after.target.root
            || before.head != after.head
        {
            return Err(problem(
                "source_integrity",
                "workspace identity changed during readback",
            ));
        }
        Ok(())
    }

    async fn validate_branch(&self, root: &Path, branch: &str, deadline: Instant) -> Result<()> {
        if branch.is_empty() || branch == "HEAD" || branch.starts_with('-') || branch.contains('\0')
        {
            return Err(problem(
                "invalid",
                "workspace branch is empty, reserved, or malformed",
            ));
        }
        let full_ref = branch_ref(branch);
        let output = self
            .git
            .workspace_command(root, &["check-ref-format", &full_ref], deadline)
            .await?;
        if output.code != 0 {
            return Err(problem(
                "invalid",
                "workspace branch is not a valid local branch name",
            ));
        }
        Ok(())
    }

    async fn branch_oid(
        &self,
        root: &Path,
        branch: &str,
        deadline: Instant,
    ) -> Result<Option<String>> {
        let full_ref = branch_ref(branch);
        let output = self
            .git
            .workspace_command(
                root,
                &["show-ref", "--verify", "--quiet", &full_ref],
                deadline,
            )
            .await?;
        match output.code {
            1 => Ok(None),
            0 => {
                let oid = self
                    .read_line(
                        root,
                        &["show-ref", "--verify", "--hash", &full_ref],
                        deadline,
                    )
                    .await?;
                if !is_oid(&oid) {
                    return Err(problem(
                        "source_integrity",
                        "workspace branch object id is malformed",
                    ));
                }
                Ok(Some(oid))
            }
            _ => Err(problem(
                "unavailable",
                "workspace branch presence cannot be read",
            )),
        }
    }

    async fn entries(&self, root: &Path, deadline: Instant) -> Result<Vec<WorktreeEntry>> {
        let bytes = self
            .read(root, &["worktree", "list", "--porcelain", "-z"], deadline)
            .await?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| problem("unavailable", "worktree registry is not UTF-8"))?;
        if !text.ends_with("\0\0") {
            return Err(problem(
                "source_integrity",
                "worktree registry is truncated",
            ));
        }
        let mut entries = Vec::new();
        for record in text.split("\0\0").filter(|record| !record.is_empty()) {
            let mut fields = record.split('\0');
            let path = fields
                .next()
                .and_then(|field| field.strip_prefix("worktree "))
                .ok_or_else(|| problem("source_integrity", "worktree registry omitted its path"))?;
            let mut entry = WorktreeEntry {
                path: destination_path(path)?,
                branch: None,
                head: None,
                incomplete: false,
            };
            for field in fields {
                if let Some(branch) = field.strip_prefix("branch ") {
                    if entry.branch.replace(branch.to_owned()).is_some() {
                        return Err(problem(
                            "source_integrity",
                            "worktree registry repeats its branch",
                        ));
                    }
                } else if let Some(head) = field.strip_prefix("HEAD ") {
                    if !is_oid(head) || entry.head.replace(head.to_owned()).is_some() {
                        return Err(problem(
                            "source_integrity",
                            "worktree registry HEAD is malformed",
                        ));
                    }
                } else if matches!(field, "bare" | "detached" | "locked" | "prunable")
                    || field.starts_with("locked ")
                    || field.starts_with("prunable ")
                {
                    entry.incomplete = true;
                }
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    async fn read(&self, root: &Path, args: &[&str], deadline: Instant) -> Result<Vec<u8>> {
        let output = self.git.workspace_command(root, args, deadline).await?;
        if output.code != 0 {
            return Err(problem("unavailable", "workspace Git readback failed"));
        }
        Ok(output.stdout)
    }

    async fn read_line(&self, root: &Path, args: &[&str], deadline: Instant) -> Result<String> {
        let bytes = self.read(root, args, deadline).await?;
        line(&bytes)
    }
}

fn branch_ref(branch: &str) -> String {
    format!("refs/heads/{branch}")
}

fn scratch_relative(path: &str) -> Result<PathBuf> {
    let path = Path::new(path);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || !path
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
    {
        return Err(problem(
            "invalid",
            "scratch path must be a nonempty relative component path without traversal",
        ));
    }
    Ok(path.to_path_buf())
}

fn scratch_paths(bytes: &[u8]) -> Result<BTreeSet<PathBuf>> {
    if bytes.is_empty() {
        return Ok(BTreeSet::new());
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| problem("unavailable", "scratch Git listing is not UTF-8"))?;
    let text = text
        .strip_suffix('\0')
        .ok_or_else(|| problem("unavailable", "scratch Git listing is truncated"))?;
    text.split('\0')
        .map(|path| {
            scratch_relative(path).map_err(|_| {
                problem(
                    "unavailable",
                    "scratch Git listing contains an invalid relative path",
                )
            })
        })
        .collect()
}

fn require_absent(path: &Path) -> Result<()> {
    if absent(path)? {
        Ok(())
    } else {
        Err(problem(
            "already_exists",
            "workspace target must not already exist",
        ))
    }
}

fn absent(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(false),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(_) => Err(problem(
            "unavailable",
            "workspace path presence cannot be read",
        )),
    }
}

fn separate_target(source: &Repository, target: &Path) -> Result<()> {
    if target == source.root
        || source.root.starts_with(target)
        || target.starts_with(&source.common_dir)
        || source.common_dir.starts_with(target)
    {
        return Err(problem(
            "invalid",
            "workspace target overlaps protected repository identity",
        ));
    }
    Ok(())
}

fn destination_path(path: &str) -> Result<PathBuf> {
    let path = Path::new(path);
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(problem(
            "invalid",
            "workspace path must be absolute without parent traversal",
        ));
    }
    #[cfg(windows)]
    for part in path.components() {
        if let Component::Prefix(prefix) = part
            && matches!(
                prefix.kind(),
                std::path::Prefix::DeviceNS(_) | std::path::Prefix::Verbatim(_)
            )
        {
            return Err(problem("invalid", "workspace path uses a device namespace"));
        }
        if let Component::Normal(name) = part {
            let name = name
                .to_str()
                .ok_or_else(|| problem("invalid", "workspace path is not UTF-8"))?;
            let stem = name.split('.').next().unwrap_or("").to_ascii_uppercase();
            if name.contains(':')
                || matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
                || (stem.len() == 4
                    && (stem.starts_with("COM") || stem.starts_with("LPT"))
                    && stem.as_bytes()[3].is_ascii_digit())
            {
                return Err(problem(
                    "invalid",
                    "workspace path contains an alternate stream or device",
                ));
            }
        }
    }
    reject_links(path)?;
    let mut parent = path.to_path_buf();
    let mut missing = Vec::new();
    loop {
        match fs::symlink_metadata(&parent) {
            Ok(metadata) => {
                if !missing.is_empty() && !metadata.is_dir() {
                    return Err(problem("invalid", "workspace ancestor is not a directory"));
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(
                    parent
                        .file_name()
                        .ok_or_else(|| problem("invalid", "workspace has no existing ancestor"))?
                        .to_owned(),
                );
                if !parent.pop() {
                    return Err(problem("invalid", "workspace ancestor cannot be resolved"));
                }
            }
            Err(_) => {
                return Err(problem(
                    "unavailable",
                    "workspace ancestor cannot be inspected",
                ));
            }
        }
    }
    let mut canonical = fs::canonicalize(parent)
        .map_err(|_| problem("unavailable", "workspace path cannot be canonicalized"))?;
    for component in missing.into_iter().rev() {
        canonical.push(component);
    }
    Ok(canonical)
}

fn metadata_line(path: &Path) -> Result<String> {
    reject_links(path)?;
    let mut bytes = Vec::new();
    fs::File::open(path)
        .and_then(|file| file.take(METADATA_BYTES + 1).read_to_end(&mut bytes))
        .map_err(|_| problem("unavailable", "workspace Git backlink cannot be read"))?;
    if bytes.len() as u64 > METADATA_BYTES {
        return Err(problem(
            "unavailable",
            "workspace Git backlink exceeds its bound",
        ));
    }
    line(&bytes)
}

fn line(bytes: &[u8]) -> Result<String> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| problem("unavailable", "workspace Git readback is not UTF-8"))?;
    let text = text
        .strip_suffix('\n')
        .filter(|text| !text.is_empty() && !text.contains('\0'))
        .ok_or_else(|| {
            problem(
                "source_integrity",
                "workspace Git readback omitted its exact record",
            )
        })?;
    Ok(text.to_owned())
}

fn path_text(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| problem("unavailable", "workspace path is not UTF-8"))
}

fn problem(code: &str, message: &str) -> Error {
    Error::new(code, message)
}
