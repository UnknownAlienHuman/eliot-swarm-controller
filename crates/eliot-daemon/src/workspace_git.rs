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

mod readback;
pub(crate) use readback::registration_linked_paths;

const READ_TIME: Duration = Duration::from_secs(15);
/// Shared Git command budget for a project readback.
const ORIGIN_READ_TIME: Duration = Duration::from_secs(3);
const METADATA_BYTES: u64 = 64 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct FrozenWorkspaceGit {
    pub repo_path: String,
    pub repo_common_dir: String,
    pub requested_branch: String,
    /// The resolved base ref retained at admission. Cleanup compares the
    /// branch against this ref, never against a freshly resolved default.
    pub base_ref: String,
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

    /// The repository name of `common_dir`'s `origin` remote
    /// ([`eliot_core::api::repository_name_from_url`]): one bounded
    /// `git config --get remote.origin.url` run in the common directory with
    /// the probe's sanitized environment (no inherited `GIT_*`). `None`
    /// without an origin, for a missing directory, or when Git cannot read
    /// it. The URL itself, which may embed credentials, never leaves here.
    async fn origin_name(&self, common_dir: &str, deadline: Instant) -> Option<String> {
        let root = canonical_root(common_dir).ok()?;
        let output = self
            .git
            .workspace_command_bound(
                &root,
                &[
                    "config",
                    "--local",
                    "--no-includes",
                    "--get",
                    "remote.origin.url",
                ],
                GitBinding {
                    git_dir: &root,
                    common_dir: &root,
                },
                deadline,
            )
            .await
            .ok()?;
        if output.code != 0 {
            return None;
        }
        eliot_core::api::repository_name_from_url(std::str::from_utf8(&output.stdout).ok()?)
    }

    /// Discover the canonical checkout containing an Owner-opened directory.
    /// Unlike `repository_identity`, this accepts a path below the Git root.
    pub(crate) async fn discover_repository_identity(
        &self,
        path: &str,
    ) -> Result<(String, String, bool)> {
        let requested = canonical_root(path)?;
        let deadline = Instant::now() + READ_TIME;
        let bytes = self
            .read(
                &requested,
                &[
                    "rev-parse",
                    "--path-format=absolute",
                    "--show-toplevel",
                    "--absolute-git-dir",
                    "--git-common-dir",
                ],
                deadline,
            )
            .await?;
        let (top, git_dir, common_dir) = identity_lines(&bytes)?;
        let checkout = canonical_root(&top)?;
        if !requested.starts_with(&checkout) {
            return Err(problem(
                "source_integrity",
                "Git checkout does not contain the requested workspace",
            ));
        }
        let common_dir = canonical_root(&common_dir)?;
        let linked = canonical_root(&git_dir)? != common_dir;
        Ok((path_text(&checkout)?, path_text(&common_dir)?, linked))
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
        self.require_branch_namespace_free(&source.root, branch, deadline)
            .await?;
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
        let reference = match base_ref {
            Some(reference) if !reference.is_empty() && !reference.contains('\0') => {
                reference.to_owned()
            }
            Some(_) => {
                return Err(problem(
                    "invalid",
                    "workspace base reference is empty or malformed",
                ));
            }
            // No explicit base: the local default branch when one exists.
            // Without a readable default the launch-time branch is retained
            // instead of a moving HEAD, so cleanup compares against the
            // checkout the worktree was actually cut from.
            None => {
                let resolved = self.default_base_ref(&source.root, deadline).await?;
                if resolved != "HEAD" {
                    resolved
                } else {
                    self.current_branch_name(&source.root, deadline)
                        .await?
                        .unwrap_or(resolved)
                }
            }
        };
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
            base_ref: reference,
            base_oid,
            worktree_path: path_text(&destination)?,
        })
    }

    /// A requested branch must not collide with the branch namespace: an
    /// existing branch `atlas` blocks `atlas/a1-task` (Git cannot create a
    /// ref under an existing ref), and an existing `atlas/a1-task` blocks a
    /// new `atlas`. The conflict is refused before admission.
    async fn require_branch_namespace_free(
        &self,
        root: &Path,
        branch: &str,
        deadline: Instant,
    ) -> Result<()> {
        let mut prefix = String::new();
        for component in branch.split('/') {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(component);
            if prefix.len() == branch.len() {
                break;
            }
            if self.branch_oid(root, &prefix, deadline).await?.is_some() {
                return Err(problem(
                    "already_exists",
                    "workspace branch conflicts with an existing branch",
                ));
            }
        }
        let pattern = format!("refs/heads/{branch}/");
        let output = self
            .git
            .workspace_command(
                root,
                &["for-each-ref", "--format=%(refname)", &pattern],
                deadline,
            )
            .await?;
        if output.code != 0 {
            return Err(problem(
                "unknown",
                "branch namespace check did not complete",
            ));
        }
        if !output.stdout.is_empty() {
            return Err(problem(
                "already_exists",
                "workspace branch conflicts with an existing branch namespace",
            ));
        }
        Ok(())
    }

    /// Final removal for a retained, clean worktree. The only mutation is a
    /// plain `git worktree remove` (never `--force`): Git itself performs
    /// the final clean check, so a file dirtied after the last probe refuses
    /// instead of being deleted. Only handles held for identity proof are
    /// read; tracked files are never deleted through them.
    pub(crate) async fn remove_managed_worktree(
        &self,
        repo: &str,
        worktree_path: &str,
        branch: &str,
    ) -> Result<()> {
        if branch.is_empty() || branch.starts_with('-') || branch.contains('\0') {
            return Err(problem("invalid", "managed branch is malformed"));
        }
        let deadline = Instant::now() + READ_TIME;
        let source = self.repository(repo, deadline).await?;
        let target = destination_path(worktree_path)?;
        separate_target(&source, &target)?;
        let target_text = git_path_argument(&target)?;
        // An authorized mutation runs under ordinary custody, not the
        // single-process leaf fence: `worktree remove` spawns helpers.
        let output = self
            .git
            .workspace_mutation(
                &source.root,
                &["worktree", "remove", "--", &target_text],
                GitBinding {
                    git_dir: &source.git_dir,
                    common_dir: &source.common_dir,
                },
                deadline,
            )
            .await?;
        if output.code != 0 {
            // Git refuses a dirty checkout itself, but a nonzero exit can
            // also be a fatal (bad path, lock, I/O). Re-probe strict
            // cleanliness so only real dirt reads `dirty_worktree`; anything
            // else stays unknown instead of being mislabeled.
            let probe = self
                .git
                .workspace_command(
                    &target,
                    &[
                        "status",
                        "--porcelain=v1",
                        "-z",
                        "--untracked-files=all",
                        "--ignored=matching",
                    ],
                    deadline,
                )
                .await?;
            if probe.code == 0 && !probe.stdout.is_empty() {
                return Err(problem(
                    "dirty_worktree",
                    "worktree removal refused: the checkout is not clean",
                ));
            }
            return Err(problem(
                "unknown",
                "worktree removal did not complete and was not retried",
            ));
        }
        if absent(&target)? {
            Ok(())
        } else {
            Err(problem(
                "unknown",
                "worktree removal is incomplete and was not retried",
            ))
        }
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
                code: error.code.clone(),
                message: (*error.message).clone(),
                failure_where: Some(
                    error
                        .failure_where
                        .clone()
                        .map(String::from)
                        .unwrap_or_else(|| "store.dispatch.workspace_gate".to_owned()),
                ),
                origin: Some(error.origin_or_unknown()),
                origin_evidence: error.origin_evidence.clone().map(String::from),
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
                code: error.code.clone(),
                message: (*error.message).clone(),
                failure_where: Some(
                    error
                        .failure_where
                        .clone()
                        .map(String::from)
                        .unwrap_or_else(|| "store.dispatch.workspace_gate".to_owned()),
                ),
                origin: Some(error.origin_or_unknown()),
                origin_evidence: error.origin_evidence.clone().map(String::from),
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
                failure_where: Some("store.dispatch.workspace_gate".to_owned()),
                origin: Some(eliot_core::FailureOrigin::Swarm),
                origin_evidence: Some(
                    "managed workspace preparation gate found no subject".to_owned(),
                ),
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

    /// The shared merge and strict-clean gate before the first removal
    /// mutation. An unmerged, dirty or unreadable worktree is refused here,
    /// before any scratch file is touched; unknown Git state stays unknown.
    /// `worktree.remove` and cleanup apply run this same gate.
    pub(crate) async fn preflight_remove(&self, preparation: &WorkspacePreparation) -> Result<()> {
        let deadline = Instant::now() + READ_TIME;
        let subject = self.subject(preparation, deadline).await?;
        self.require_clean(&subject, true, deadline).await?;
        if !self
            .branch_merged_into_base(
                &subject.source.root,
                &preparation.requested_branch,
                preparation.base_ref.as_deref(),
                deadline,
            )
            .await?
        {
            return Err(problem(
                "unmerged",
                "worktree branch is not merged into its base",
            ));
        }
        self.confirm_subject(preparation, &subject, deadline)
            .await?;
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
                message: *error.message,
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
        // One rev-parse reports toplevel, git dir and common dir together. Each
        // read costs two Git spawns (safe-overrides preflight plus the read),
        // so three separate reads serialize six spawns on every repository
        // identity check, including each project registration at daemon startup.
        let bytes = self
            .read(
                &root,
                &[
                    "rev-parse",
                    "--path-format=absolute",
                    "--show-toplevel",
                    "--absolute-git-dir",
                    "--git-common-dir",
                ],
                deadline,
            )
            .await?;
        let (actual_root, git_dir, common_dir) = identity_lines(&bytes)?;
        if canonical_root(&actual_root)? != root {
            return Err(problem(
                "source_integrity",
                "workspace path is not the exact repository root",
            ));
        }
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

    /// The project's default branch as it is locally. A local default
    /// branch wins when one exists: the retained base must not track a
    /// moving remote HEAD or the current checkout. Else the short form of
    /// `refs/remotes/origin/HEAD` when it resolves to a commit, else HEAD.
    /// Read-only; a missing or unreadable default is not an error.
    pub(crate) async fn default_base_ref(&self, root: &Path, deadline: Instant) -> Result<String> {
        if let Some(local) = self.local_default_branch(root, deadline).await? {
            return Ok(local);
        }
        if let Some(remote) = self.origin_head_short(root, deadline).await? {
            return Ok(remote);
        }
        Ok("HEAD".to_owned())
    }

    /// The local counterpart of the `origin/HEAD` target when it exists
    /// locally, else a conventional local default (`main`, then `master`).
    async fn local_default_branch(&self, root: &Path, deadline: Instant) -> Result<Option<String>> {
        let mut candidates = Vec::new();
        if let Some(short) = self.origin_head_short(root, deadline).await?
            && let Some(local) = short.strip_prefix("origin/")
            && !local.is_empty()
            && !candidates.iter().any(|name| name == local)
        {
            candidates.push(local.to_owned());
        }
        for name in ["main", "master"] {
            if !candidates.iter().any(|candidate| candidate == name) {
                candidates.push(name.to_owned());
            }
        }
        for candidate in candidates {
            if self.branch_oid(root, &candidate, deadline).await?.is_some() {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    /// The current checkout branch when HEAD is attached, else `None`.
    async fn current_branch_name(&self, root: &Path, deadline: Instant) -> Result<Option<String>> {
        let output = self
            .git
            .workspace_command(
                root,
                &["symbolic-ref", "--short", "--quiet", "HEAD"],
                deadline,
            )
            .await?;
        if output.code == 0
            && let Ok(name) = std::str::from_utf8(&output.stdout)
            && let name = name.trim_end_matches(['\r', '\n'])
            && !name.is_empty()
            && name != "HEAD"
            && !name.contains('\0')
            && !name.starts_with('-')
        {
            return Ok(Some(name.to_owned()));
        }
        Ok(None)
    }

    /// The short form of `refs/remotes/origin/HEAD` when it resolves to a
    /// commit, else `None`.
    async fn origin_head_short(&self, root: &Path, deadline: Instant) -> Result<Option<String>> {
        let output = self
            .git
            .workspace_command(
                root,
                &[
                    "symbolic-ref",
                    "--short",
                    "--quiet",
                    "refs/remotes/origin/HEAD",
                ],
                deadline,
            )
            .await?;
        if output.code == 0
            && let Ok(name) = std::str::from_utf8(&output.stdout)
            && let name = name.trim_end_matches(['\r', '\n'])
            && !name.is_empty()
            && !name.contains('\0')
        {
            let commit = format!("{name}^{{commit}}");
            if let Ok(oid) = self
                .read_line(root, &["rev-parse", "--verify", &commit], deadline)
                .await
                && is_oid(&oid)
            {
                return Ok(Some(name.to_owned()));
            }
        }
        Ok(None)
    }

    /// Whether `branch` is merged into its base: the branch tip is an
    /// ancestor of the base tip. The base is the ref retained at admission;
    /// legacy rows without one resolve the local default, and rows whose
    /// base cannot be established are an error (kept, never removed).
    /// Any unreadable identity is an error, never a silent unmerged verdict.
    pub(crate) async fn branch_merged_into_base(
        &self,
        repo_root: &Path,
        branch: &str,
        base_ref: Option<&str>,
        deadline: Instant,
    ) -> Result<bool> {
        let tip = self
            .read_line(
                repo_root,
                &[
                    "rev-parse",
                    "--verify",
                    "--end-of-options",
                    &branch_ref(branch),
                ],
                deadline,
            )
            .await?;
        if !is_oid(&tip) {
            return Err(problem(
                "source_integrity",
                "managed branch tip is not a commit id",
            ));
        }
        let base = match base_ref.filter(|base| !base.is_empty()) {
            Some(base) => base.to_owned(),
            None => {
                let resolved = self.default_base_ref(repo_root, deadline).await?;
                if resolved == "HEAD" {
                    return Err(problem(
                        "unknown",
                        "managed base cannot be established; the worktree is kept",
                    ));
                }
                resolved
            }
        };
        if base.contains('\0') {
            return Err(problem("invalid", "managed base reference is malformed"));
        }
        let base_tip = self
            .read_line(
                repo_root,
                &[
                    "rev-parse",
                    "--verify",
                    "--end-of-options",
                    &format!("{base}^{{commit}}"),
                ],
                deadline,
            )
            .await?;
        if !is_oid(&base_tip) {
            return Err(problem(
                "source_integrity",
                "managed base tip is not a commit id",
            ));
        }
        let ancestor = self
            .git
            .workspace_command(
                repo_root,
                &["merge-base", "--is-ancestor", &tip, &base_tip],
                deadline,
            )
            .await?;
        match ancestor.code {
            0 => Ok(true),
            1 => Ok(false),
            _ => Err(problem("unknown", "branch merge check did not complete")),
        }
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

/// Windows paths (including the trailing NUL) stay under 260 characters;
/// the complete destination is budgeted before admission so generated
/// names are always launchable.
const MAX_DESTINATION_LEN: usize = 260;

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
    if path
        .to_str()
        .is_none_or(|text| text.len() >= MAX_DESTINATION_LEN)
    {
        return Err(problem(
            "invalid",
            "workspace path exceeds the Windows length limit",
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
    if !fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_file()) {
        return Err(problem(
            "unavailable",
            "workspace Git metadata is not a regular file",
        ));
    }
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

fn identity_lines(bytes: &[u8]) -> Result<(String, String, String)> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| problem("unavailable", "workspace Git readback is not UTF-8"))?;
    let text = text.strip_suffix('\n').ok_or_else(|| {
        problem(
            "source_integrity",
            "workspace Git readback omitted its exact record",
        )
    })?;
    let mut lines = text.split('\n');
    match (lines.next(), lines.next(), lines.next(), lines.next()) {
        (Some(toplevel), Some(git_dir), Some(common_dir), None)
            if [toplevel, git_dir, common_dir]
                .iter()
                .all(|line| !line.is_empty() && !line.contains('\0')) =>
        {
            Ok((
                toplevel.to_owned(),
                git_dir.to_owned(),
                common_dir.to_owned(),
            ))
        }
        _ => Err(problem(
            "source_integrity",
            "workspace Git readback omitted its exact record",
        )),
    }
}

fn path_text(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| problem("unavailable", "workspace path is not UTF-8"))
}

fn problem(code: &str, message: &str) -> Error {
    Error::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as GitCommand;

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "eliot-workspace-git-identity-{label}-{}",
                ulid::Ulid::new().to_string().to_ascii_lowercase()
            ));
            fs::create_dir_all(&path).expect("create Git identity fixture root");
            Self(path)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn git(checkout: &Path, args: &[&str]) {
        let output = GitCommand::new("git")
            .arg("-C")
            .arg(checkout)
            .args(args)
            .output()
            .expect("run local Git fixture command");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn init_repo(root: &Path) {
        git(root, &["-c", "init.defaultBranch=main", "init"]);
        git(root, &["config", "user.name", "EliotTest"]);
        git(root, &["config", "user.email", "test@example.invalid"]);
        git(root, &["commit", "--allow-empty", "-m", "root"]);
    }

    #[test]
    fn identity_lines_accepts_exactly_three_nonempty_records() {
        assert_eq!(
            identity_lines(b"top\ngit\ncommon\n").expect("three records"),
            ("top".to_owned(), "git".to_owned(), "common".to_owned())
        );
        for bytes in [
            &b""[..],
            &b"top\n"[..],
            &b"top\ngit\n"[..],
            &b"top\ngit\ncommon"[..],
            &b"top\ngit\ncommon\nextra\n"[..],
            &b"\ngit\ncommon\n"[..],
            &b"top\n\0\ncommon\n"[..],
        ] {
            assert_eq!(
                identity_lines(bytes)
                    .expect_err("ragged identity must fail")
                    .code,
                "source_integrity"
            );
        }
        assert_eq!(
            identity_lines(&[0xff, b'\n', b'a', b'\n', b'b', b'\n'])
                .expect_err("non-UTF-8 identity must fail")
                .code,
            "unavailable"
        );
    }

    #[tokio::test]
    async fn repository_identity_matches_repo_and_linked_worktree() {
        let identity = WorkspaceGit::new().expect("resolve installed Git");
        let root = TestRoot::new("repo");
        init_repo(&root.0);
        let deadline = Instant::now() + Duration::from_secs(30);
        let repository = identity
            .repository(
                &path_text(&root.0).expect("fixture path is UTF-8"),
                deadline,
            )
            .await
            .expect("repository identity");
        assert_eq!(
            repository.root,
            canonical_root(&path_text(&root.0).expect("fixture path is UTF-8"))
                .expect("canonical fixture root")
        );
        assert_eq!(
            repository.common_dir,
            repository.root.join(".git"),
            "plain repository common dir"
        );
        let worktree = root.0.join("linked");
        git(
            &root.0,
            &[
                "worktree",
                "add",
                worktree.to_str().expect("worktree path is UTF-8"),
                "-b",
                "linked-branch",
            ],
        );
        let linked = identity
            .repository(
                &path_text(&worktree).expect("worktree path is UTF-8"),
                deadline,
            )
            .await
            .expect("linked worktree identity");
        assert_eq!(
            linked.root,
            fs::canonicalize(&worktree).expect("canonical worktree")
        );
        assert_eq!(
            linked.common_dir, repository.common_dir,
            "linked worktree shares the main common dir"
        );
        assert!(
            linked.git_dir.starts_with(&repository.common_dir),
            "linked worktree git dir lives under the main repository"
        );
        let plain = TestRoot::new("plain");
        let error = match identity
            .repository(&path_text(&plain.0).expect("plain path is UTF-8"), deadline)
            .await
        {
            Ok(_) => panic!("non-repository must fail"),
            Err(error) => error,
        };
        assert_eq!(error.code, "unavailable");
    }

    #[test]
    fn destination_path_budgets_the_windows_length_limit() {
        let root = std::env::temp_dir();
        let root_text = root.to_str().expect("temp dir is UTF-8");
        let spare = MAX_DESTINATION_LEN
            .saturating_sub(root_text.len())
            .saturating_sub(2);
        let fits = format!("{root_text}/{}", "d".repeat(spare));
        assert!(fits.len() < MAX_DESTINATION_LEN);
        assert!(destination_path(&fits).is_ok());
        let overflow = format!("{root_text}/{}", "d".repeat(spare + 2));
        assert!(overflow.len() >= MAX_DESTINATION_LEN);
        assert_eq!(destination_path(&overflow).unwrap_err().code, "invalid");
    }

    #[tokio::test]
    async fn freeze_resolves_namespace_conflicts_and_retains_the_local_base() {
        let fixture = WorkspaceGit::new().expect("resolve installed Git");
        let root = TestRoot::new("naming-gate");
        init_repo(&root.0);
        git(&root.0, &["branch", "atlas"]);
        let repo = path_text(&root.0).expect("fixture path is UTF-8");
        let target = |leaf: &str| {
            root.0
                .join("worktrees")
                .join(leaf)
                .to_str()
                .expect("target is UTF-8")
                .to_owned()
        };
        // An existing branch `atlas` blocks `atlas/a1-task` before admission.
        assert_eq!(
            fixture
                .freeze(&repo, "atlas/a1-task", None, &target("blocked"))
                .await
                .unwrap_err()
                .code,
            "already_exists"
        );
        git(&root.0, &["branch", "-D", "atlas"]);
        git(&root.0, &["branch", "topic/sub"]);
        // And an existing `topic/sub` blocks a new `topic`.
        assert_eq!(
            fixture
                .freeze(&repo, "topic", None, &target("blocked-topic"))
                .await
                .unwrap_err()
                .code,
            "already_exists"
        );
        git(&root.0, &["branch", "-D", "topic/sub"]);
        // A free branch freezes with the local default retained as the base.
        let frozen = fixture
            .freeze(&repo, "atlas/a9-task", None, &target("pending-free"))
            .await
            .expect("free branch freezes");
        assert_eq!(frozen.base_ref, "main");
        assert_eq!(frozen.requested_branch, "atlas/a9-task");
    }

    #[tokio::test]
    async fn origin_names_come_from_the_common_directory_without_credentials() {
        let root = TestRoot::new("origin");
        init_repo(&root.0);
        let common = path_text(&root.0.join(".git")).expect("fixture path is UTF-8");
        let registration = |id: &str| eliot_core::api::ProjectRegistrationView {
            id: id.to_owned(),
            checkout: path_text(&root.0).expect("fixture path is UTF-8"),
            repo_common_dir: common.clone(),
            profiles: Vec::new(),
            actions: Vec::new(),
            interpreters: Default::default(),
            run_env: Vec::new(),
            git: None,
            revision: 1,
            origin: Some("stale".to_owned()),
            checkouts: Vec::new(),
        };
        let missing = {
            let mut gone = registration("gone");
            gone.repo_common_dir =
                path_text(&root.0.join("missing").join(".git")).expect("fixture path is UTF-8");
            gone
        };
        // No origin remote: nothing to name it by.
        let mut registrations = vec![registration("a"), missing.clone()];
        WorkspaceGit::fill_project_readbacks(&mut registrations).await;
        assert_eq!(registrations[0].origin, None);
        assert_eq!(registrations[1].origin, None);

        git(
            &root.0,
            &[
                "remote",
                "add",
                "origin",
                "https://user:secret@example.com/acme/atlas-web.git",
            ],
        );
        let mut registrations = vec![registration("a"), registration("b"), missing];
        WorkspaceGit::fill_project_readbacks(&mut registrations).await;
        let origins: Vec<Option<&str>> = registrations
            .iter()
            .map(|registration| registration.origin.as_deref())
            .collect();
        assert_eq!(origins, [Some("atlas-web"), Some("atlas-web"), None]);
        for registration in &registrations {
            registration.validate().expect("filled view stays valid");
        }
    }

    #[tokio::test]
    async fn project_checkout_readback_names_reftable_packed_and_separate_git_dir() {
        for layout in ["reftable", "packed", "separate"] {
            let root = TestRoot::new(layout);
            let main = root.0.join("checkout");
            fs::create_dir(&main).unwrap();
            let common = if layout == "separate" {
                let admin = root.0.join("metadata");
                git(
                    &main,
                    &[
                        "init",
                        "--initial-branch=main",
                        "--separate-git-dir",
                        admin.to_str().unwrap(),
                    ],
                );
                admin
            } else {
                let format = if layout == "reftable" {
                    "--ref-format=reftable"
                } else {
                    "--ref-format=files"
                };
                git(&main, &["init", "--initial-branch=main", format]);
                main.join(".git")
            };
            git(
                &main,
                &[
                    "-c",
                    "user.name=EliotTest",
                    "-c",
                    "user.email=test@example.invalid",
                    "commit",
                    "--allow-empty",
                    "-m",
                    "root",
                ],
            );
            let linked = root.0.join("linked");
            git(
                &main,
                &[
                    "worktree",
                    "add",
                    "-b",
                    "feat/readback-name",
                    linked.to_str().unwrap(),
                ],
            );
            if layout == "packed" {
                git(&main, &["pack-refs", "--all"]);
            }
            let mut registrations = [eliot_core::api::ProjectRegistrationView {
                id: "project".to_owned(),
                checkout: path_text(&fs::canonicalize(&main).unwrap()).unwrap(),
                repo_common_dir: path_text(&fs::canonicalize(&common).unwrap()).unwrap(),
                profiles: Vec::new(),
                actions: Vec::new(),
                interpreters: Default::default(),
                run_env: Vec::new(),
                git: None,
                revision: 1,
                origin: None,
                checkouts: Vec::new(),
            }];
            WorkspaceGit::fill_project_readbacks(&mut registrations).await;
            let registration = &registrations[0];
            assert_eq!(
                registration.checkouts.len(),
                2,
                "{layout}: {registration:?}"
            );
            let main = registration.checkouts.iter().find(|row| row.main).unwrap();
            assert_eq!(main.path, registration.checkout);
            assert_eq!(main.branch.as_deref(), Some("main"));
            let worktree = registration.checkouts.iter().find(|row| !row.main).unwrap();
            assert_eq!(worktree.branch.as_deref(), Some("feat/readback-name"));

            // A one-way .git pointer cannot make an unrelated checkout a member.
            let unrelated = root.0.join("unrelated");
            fs::create_dir(&unrelated).unwrap();
            fs::copy(linked.join(".git"), unrelated.join(".git")).unwrap();
            assert!(
                !registration_linked_paths(&registration.repo_common_dir)
                    .contains(&path_text(&unrelated).unwrap())
            );
        }
    }
}
