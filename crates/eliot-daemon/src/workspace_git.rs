//! Workspace Git identity and command preparation. Only the manager executes
//! mutations, under its durable cancellation/spawn gate and ordinary custody.

use crate::git_monitor::{GitBinding, GitProbe, canonical_root, git_path_argument, reject_links};
use crate::platform::script_file::Directory;
use eliot_core::api::{is_git_oid, validate_integration_branch};
use eliot_core::{
    Error, Result, WorkspaceCreationOutcome, WorkspacePhase, WorkspacePreparation,
    WorkspaceRemovalOutcome, workspace_path_key,
};
use std::{
    collections::BTreeSet,
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[expect(
    clippy::disallowed_types,
    reason = "Workspace Git prepares native commands for authorized worktree effects."
)]
use tokio::process::Command;
use tokio::time::{Instant, sleep_until, timeout_at};

pub(crate) mod containment;
mod readback;

const READ_TIME: Duration = Duration::from_secs(15);
const UNTRACKED_ARGS: &[&str] = &["ls-files", "--others", "--exclude-standard", "-z"];
const IGNORED_ARGS: &[&str] = &[
    "ls-files",
    "--others",
    "--ignored",
    "--exclude-standard",
    "-z",
];
/// Shared Git command budget for a project readback.
const ORIGIN_READ_TIME: Duration = Duration::from_secs(3);
const METADATA_BYTES: u64 = 64 * 1024;
const ABSENCE_GRACE: Duration = Duration::from_secs(15);

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

impl Repository {
    fn same_identity(&self, other: &Self) -> bool {
        git_path_matches(&self.root, &other.root)
            && git_path_matches(&self.git_dir, &other.git_dir)
            && git_path_matches(&self.common_dir, &other.common_dir)
    }
}

fn git_path_matches(left: &Path, right: &Path) -> bool {
    left == right
        || left
            .to_str()
            .zip(right.to_str())
            .is_some_and(|(left, right)| workspace_path_key(left) == workspace_path_key(right))
}

struct WorktreeEntry {
    path: PathBuf,
    branch: Option<String>,
    head: Option<String>,
    incomplete: bool,
    prunable: bool,
    locked: bool,
}

pub(crate) struct MissingWorktreeRemoval {
    pub(crate) parent: Directory,
    pub(crate) branch_tip: Option<String>,
    #[expect(
        clippy::disallowed_types,
        reason = "The worktree owner dispatches the prepared exact Git removal command."
    )]
    pub(crate) command: Option<Command>,
}

pub(crate) enum RemovalBranch<'a> {
    Retained,
    At(&'a str),
    Absent,
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
    pub(crate) async fn origin_name(&self, common_dir: &str, deadline: Instant) -> Option<String> {
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
        base_repo_path: Option<&str>,
        target: &str,
        require_parent_ancestry: bool,
    ) -> Result<FrozenWorkspaceGit> {
        let deadline = Instant::now() + READ_TIME;
        let source_is_parent_workspace = require_parent_ancestry && base_repo_path.is_none();
        let source = self
            .workspace_repository(repo_path, !source_is_parent_workspace, deadline)
            .await?;
        let base_source = match base_repo_path {
            Some(path) => {
                let base = self
                    .workspace_repository(path, !require_parent_ancestry, deadline)
                    .await?;
                if !git_path_matches(&base.common_dir, &source.common_dir) {
                    return Err(problem(
                        "source_integrity",
                        "workspace base repository differs from project checkout",
                    ));
                }
                Some(base)
            }
            None => None,
        };
        if require_parent_ancestry {
            validate_integration_branch(branch)?;
        }
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
        let base_root = base_source.as_ref().map_or(&source.root, |repo| &repo.root);
        let base_oid = if require_parent_ancestry {
            self.child_base_oid(base_root, base_ref, deadline).await?
        } else {
            self.worktree_commit(base_root, base_ref.unwrap_or("HEAD"), deadline)
                .await?
        };
        let verified = self
            .workspace_repository(repo_path, !source_is_parent_workspace, deadline)
            .await?;
        if !source.same_identity(&verified) {
            return Err(problem(
                "source_integrity",
                "source repository identity changed during freeze",
            ));
        }
        if let (Some(base), Some(base_path)) = (base_source.as_ref(), base_repo_path) {
            let verified_base = self
                .workspace_repository(base_path, !require_parent_ancestry, deadline)
                .await?;
            if !base.same_identity(&verified_base) {
                return Err(problem(
                    "source_integrity",
                    "base workspace identity changed during freeze",
                ));
            }
        }
        Ok(FrozenWorkspaceGit {
            repo_path: path_text(&source.root)?,
            repo_common_dir: path_text(&source.common_dir)?,
            requested_branch: branch.to_owned(),
            base_oid,
            worktree_path: path_text(&destination)?,
        })
    }

    async fn worktree_commit(
        &self,
        root: &Path,
        reference: &str,
        deadline: Instant,
    ) -> Result<String> {
        if reference.is_empty() || reference.contains('\0') {
            return Err(problem(
                "invalid",
                "workspace base reference is empty or malformed",
            ));
        }
        let commit = format!("{reference}^{{commit}}");
        let base_oid = self
            .read_line(
                root,
                &["rev-parse", "--verify", "--end-of-options", &commit],
                deadline,
            )
            .await?;
        if !is_git_oid(&base_oid) {
            return Err(problem(
                "source_integrity",
                "resolved workspace base is not a full commit id",
            ));
        }
        Ok(base_oid)
    }

    async fn child_base_oid(
        &self,
        root: &Path,
        base_ref: Option<&str>,
        deadline: Instant,
    ) -> Result<String> {
        let head = self
            .git
            .workspace_command(root, &["symbolic-ref", "--quiet", "HEAD"], deadline)
            .await?;
        if head.code == 1 {
            return Err(problem(
                "not_allowed",
                "parent workspace has a detached HEAD",
            ));
        }
        if head.code != 0 {
            return Err(problem(
                "unavailable",
                "parent workspace branch cannot be read",
            ));
        }
        let branch = line(&head.stdout)?;
        if !branch.starts_with("refs/heads/") {
            return Err(problem(
                "not_allowed",
                "parent workspace HEAD must name a local branch",
            ));
        }
        let tip = self.worktree_commit(root, &branch, deadline).await?;
        let Some(reference) = base_ref else {
            return Ok(tip);
        };
        let base = self.worktree_commit(root, reference, deadline).await?;
        if base == tip {
            return Ok(base);
        }
        let ancestry = self
            .git
            .workspace_command(
                root,
                &["merge-base", "--is-ancestor", &base, &tip],
                deadline,
            )
            .await?;
        match ancestry.code {
            0 => Ok(base),
            1 => Err(problem(
                "not_allowed",
                "child worktree base is outside the parent's current branch history",
            )),
            _ => Err(problem(
                "unavailable",
                "child worktree base ancestry could not be verified",
            )),
        }
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

    /// Requires the manager's durable `creating` intent. This never spawns the
    /// returned mutator and cannot repair/replay a partial previous creation.
    #[expect(
        clippy::disallowed_types,
        reason = "This prepares the Git command for an admitted worktree creation."
    )]
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
        let mut command = self
            .git
            .prepare_workspace_command(
                &source.root,
                &[
                    "-c",
                    "core.sparseCheckout=false",
                    "-c",
                    "index.sparse=false",
                    "-c",
                    "core.logAllRefUpdates=always",
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
            .await?;
        command.env("GIT_COMMITTER_NAME", creation_committer(preparation));
        command.env("GIT_COMMITTER_EMAIL", "worktree@eliot.invalid");
        Ok(command)
    }

    /// Git can leave its new branch after a failed checkout has removed its
    /// registration and directory. Authorize deletion only for our own ref.
    #[expect(
        clippy::disallowed_types,
        reason = "This prepares rollback of the branch left by a failed checkout."
    )]
    pub(crate) async fn prepare_failed_checkout_rollback(
        &self,
        preparation: &WorkspacePreparation,
    ) -> Result<Option<Command>> {
        let deadline = Instant::now() + READ_TIME;
        let source = self.source(preparation, deadline).await?;
        let target = destination_path(&preparation.worktree_path)?;
        separate_target(&source, &target)?;
        let branch = branch_ref(&preparation.requested_branch);
        if !absent(&target)?
            || self
                .entries(&source.root, deadline)
                .await?
                .iter()
                .any(|entry| {
                    entry.path == target || entry.branch.as_deref() == Some(branch.as_str())
                })
        {
            return Err(problem(
                "source_integrity",
                "workspace path or Git registration remains",
            ));
        }
        let Some(branch_tip) = self
            .branch_oid(&source.root, &preparation.requested_branch, deadline)
            .await?
        else {
            return Ok(None);
        };
        if branch_tip != preparation.base_oid {
            return Err(problem(
                "workspace_commits",
                "remaining branch tip differs from the frozen base; preserving the branch",
            ));
        }
        if !self
            .creation_branch_is_owned(preparation, &source, deadline)
            .await?
        {
            return Err(problem(
                "workspace_branch_preserved",
                "remaining branch does not match the launch's creation marker; preserving it",
            ));
        }
        self.git
            .prepare_workspace_command(
                &source.root,
                &[
                    "update-ref",
                    "--no-deref",
                    "-d",
                    &branch,
                    &preparation.base_oid,
                ],
                GitBinding {
                    git_dir: &source.git_dir,
                    common_dir: &source.common_dir,
                },
                deadline,
            )
            .await
            .map(Some)
    }

    #[expect(
        clippy::disallowed_types,
        reason = "This prepares removal of a worktree created by the failed launch."
    )]
    pub(crate) async fn prepare_created_worktree_rollback(
        &self,
        preparation: &WorkspacePreparation,
    ) -> Result<Option<Command>> {
        if absent(&destination_path(&preparation.worktree_path)?)? {
            return Ok(None);
        }
        let deadline = Instant::now() + READ_TIME;
        let subject = self.subject(preparation, deadline).await?;
        if subject.head != preparation.base_oid {
            return Err(problem(
                "workspace_commits",
                "created worktree contains commits beyond the frozen base; preserving it",
            ));
        }
        if !self
            .creation_branch_is_owned(preparation, &subject.source, deadline)
            .await?
        {
            return Err(problem(
                "workspace_branch_preserved",
                "worktree branch does not match the launch's creation marker; preserving it",
            ));
        }
        self.require_created_clean(&subject, deadline).await?;
        self.confirm_subject(preparation, &subject, deadline)
            .await?;
        let target = git_path_argument(&subject.target.root)?;
        self.git
            .prepare_workspace_command_with_config_roots(
                &subject.source.root,
                &["worktree", "remove", "--", &target],
                GitBinding {
                    git_dir: &subject.source.git_dir,
                    common_dir: &subject.source.common_dir,
                },
                &[&subject.target.root],
                deadline,
            )
            .await
            .map(Some)
    }

    async fn creation_branch_is_owned(
        &self,
        preparation: &WorkspacePreparation,
        source: &Repository,
        deadline: Instant,
    ) -> Result<bool> {
        if self
            .branch_oid(&source.root, &preparation.requested_branch, deadline)
            .await?
            .as_deref()
            != Some(preparation.base_oid.as_str())
        {
            return Ok(false);
        }
        let branch = branch_ref(&preparation.requested_branch);
        let provenance = self
            .read_line(
                &source.root,
                &["reflog", "show", "-n", "1", "--format=%gn;%ge;%H", &branch],
                deadline,
            )
            .await?;
        let expected = format!(
            "{};worktree@eliot.invalid;{}",
            creation_committer(preparation),
            preparation.base_oid,
        );
        Ok(provenance == expected)
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

    pub(crate) async fn creation_is_absent(
        &self,
        preparation: &WorkspacePreparation,
    ) -> Result<bool> {
        let deadline = Instant::now() + READ_TIME;
        let source = self.source(preparation, deadline).await?;
        let target = destination_path(&preparation.worktree_path)?;
        separate_target(&source, &target)?;
        let branch = branch_ref(&preparation.requested_branch);
        let entries = self.entries(&source.root, deadline).await?;
        Ok(!entries
            .iter()
            .any(|entry| entry.path == target || entry.branch.as_deref() == Some(branch.as_str()))
            && absent(&target)?
            && self
                .branch_oid(&source.root, &preparation.requested_branch, deadline)
                .await?
                .is_none())
    }

    async fn inspect_created(
        &self,
        preparation: &WorkspacePreparation,
    ) -> Result<WorkspaceCreationOutcome> {
        let deadline = Instant::now() + READ_TIME;
        if self.creation_is_absent(preparation).await? {
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
        self.require_created_clean(&subject, deadline).await?;
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

    async fn require_created_clean(&self, subject: &Subject, deadline: Instant) -> Result<()> {
        let status = self
            .read(
                &subject.target.root,
                &[
                    "status",
                    "--porcelain=v2",
                    "--untracked-files=all",
                    "--ignored=matching",
                    "--ignore-submodules=none",
                    "-z",
                ],
                deadline,
            )
            .await?;
        if !status.is_empty() {
            return Err(problem(
                "dirty",
                "created workspace has tracked, untracked, or ignored changes",
            ));
        }
        Ok(())
    }

    /// Read-only full preflight. The manager holds its removing/cancellation gate
    /// before deleting returned regular files individually; never delete a scratch
    /// directory recursively or use this list as authority for tracked files.
    pub(crate) async fn removal_candidates(
        &self,
        preparation: &WorkspacePreparation,
        scratch: &[String],
        expected_tip: Option<&str>,
        deadline: Instant,
    ) -> Result<(String, Vec<PathBuf>)> {
        let subject = self.subject(preparation, deadline).await?;
        if expected_tip.is_some_and(|tip| tip != subject.head) {
            return Err(problem(
                "identity_changed",
                "worktree tip moved since the plan",
            ));
        }
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

        let other = self.scratch_listing(root, UNTRACKED_ARGS, deadline).await?;
        let ignored = self.scratch_listing(root, IGNORED_ARGS, deadline).await?;
        if other.is_empty() && ignored.is_empty() {
            self.confirm_subject(preparation, &subject, deadline)
                .await?;
            return Ok((subject.head, Vec::new()));
        }
        let cached = scratch_paths(self.git.cached_index_paths(root, deadline).await?)?;
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
        let cached_readback = scratch_paths(self.git.cached_index_paths(root, deadline).await?)?;
        if cached != cached_readback
            || other != self.scratch_listing(root, UNTRACKED_ARGS, deadline).await?
            || ignored != self.scratch_listing(root, IGNORED_ARGS, deadline).await?
        {
            return Err(problem(
                "unavailable",
                "scratch/index coverage changed during preflight",
            ));
        }
        self.confirm_subject(preparation, &subject, deadline)
            .await?;
        Ok((subject.head, candidates.into_iter().collect()))
    }

    async fn scratch_listing(
        &self,
        root: &Path,
        args: &[&str],
        deadline: Instant,
    ) -> Result<BTreeSet<PathBuf>> {
        scratch_paths(
            self.git
                .workspace_path_listing(root, args, deadline)
                .await?,
        )
    }

    async fn require_tracked_clean(&self, subject: &Subject, deadline: Instant) -> Result<()> {
        let path = path_text(&subject.target.root)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|value| i64::try_from(value.as_millis()).ok())
            .ok_or_else(|| problem("unavailable", "scratch observation clock is unavailable"))?;
        let snapshot = timeout_at(deadline, self.git.probe_cleanup(&path, &subject.head, now))
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
        if !snapshot.dirty.is_empty() {
            return Err(problem(
                "dirty",
                "worktree contains staged or unstaged tracked changes",
            ));
        }
        Ok(())
    }

    /// Only prepares ordinary, non-force removal. The manager removes explicitly
    /// allowed scratch after committing `removing`, before calling this method.
    #[expect(
        clippy::disallowed_types,
        reason = "This prepares the authorized non-force worktree removal command."
    )]
    pub(crate) async fn prepare_remove(
        &self,
        preparation: &WorkspacePreparation,
        verified_tip: &str,
        deadline: Instant,
    ) -> Result<Command> {
        if preparation.phase != WorkspacePhase::Removing {
            return Err(problem(
                "bad_state",
                "workspace remove requires its removing marker",
            ));
        }
        self.prepare_remove_checked(preparation, verified_tip, deadline)
            .await
    }

    /// Prove against an independent branch ref in the frozen source, without
    /// fetching or changing any refs. Recheck both object IDs after ancestry.
    pub(crate) async fn require_automatic_removal_merge(
        &self,
        preparation: &WorkspacePreparation,
        expected_tip: &str,
        deadline: Instant,
    ) -> Result<()> {
        let source = self.source(preparation, deadline).await?;
        let candidate = branch_ref(&preparation.requested_branch);
        let unproven = || {
            problem(
                "merge_proof_unavailable",
                "local merge proof is unavailable",
            )
        };
        if !is_git_oid(expected_tip)
            || source
                .common_dir
                .join("info/grafts")
                .try_exists()
                .map_err(|_| unproven())?
        {
            return Err(unproven());
        }

        let base_name = if let Some(base) = preparation
            .base_ref
            .as_deref()
            .filter(|base| *base != "HEAD" && !is_git_oid(base))
        {
            self.read_line(
                &source.root,
                &[
                    "rev-parse",
                    "--symbolic-full-name",
                    "--verify",
                    "--end-of-options",
                    base,
                ],
                deadline,
            )
            .await
        } else {
            self.read_line(&source.root, &["symbolic-ref", "--quiet", "HEAD"], deadline)
                .await
        }
        .map_err(|_| unproven())?;
        if base_name == candidate
            || !["refs/heads/", "refs/remotes/"]
                .iter()
                .any(|prefix| base_name.starts_with(prefix))
        {
            return Err(unproven());
        }
        let read_oid = async |reference: &str| {
            self.read_line(
                &source.root,
                &["rev-parse", "--verify", "--end-of-options", reference],
                deadline,
            )
            .await
        };
        let base = read_oid(&base_name).await?;
        if !is_git_oid(&base) {
            return Err(unproven());
        }
        let result = self
            .git
            .workspace_command(
                &source.root,
                &["merge-base", "--is-ancestor", expected_tip, &base],
                deadline,
            )
            .await?;
        if read_oid(&candidate).await? != expected_tip || read_oid(&base_name).await? != base {
            return Err(problem(
                "identity_changed",
                "worktree or base ref moved during merge proof",
            ));
        }
        match result.code {
            0 => Ok(()),
            1 => Err(problem(
                "unmerged",
                "managed branch has commits outside its local base",
            )),
            _ => Err(unproven()),
        }
    }

    /// A hand-deleted directory has no contents left to guard. Only a unique,
    /// unlocked, prunable registration of the frozen path and branch may be
    /// passed to Git's non-force removal. Never prune unrelated worktrees.
    pub(crate) async fn prepare_missing_remove(
        &self,
        preparation: &WorkspacePreparation,
        expected_tip: Option<&str>,
        deadline: Instant,
    ) -> Result<Option<MissingWorktreeRemoval>> {
        let path = Path::new(&preparation.worktree_path);
        let Some(parent) = observe_worktree_absence(path)? else {
            return Ok(None);
        };
        let target = destination_path(&preparation.worktree_path)?;
        if !git_path_matches(path, &target) {
            return Err(problem(
                "identity_changed",
                "missing worktree path changed during observation",
            ));
        }
        let source = self.source(preparation, deadline).await?;
        separate_target(&source, &target)?;
        let branch_tip = self
            .branch_oid(&source.root, &preparation.requested_branch, deadline)
            .await?;
        if expected_tip.is_some_and(|tip| branch_tip.as_deref() != Some(tip)) {
            return Err(problem(
                "identity_changed",
                "missing worktree branch tip moved since the plan",
            ));
        }
        confirm_worktree_absence_after_grace(path, &parent, deadline).await?;
        let entries = self.entries(&source.root, deadline).await?;
        let mut matches = entries.iter().filter(|entry| entry.path == target);
        let Some(entry) = matches.next() else {
            return Ok(Some(MissingWorktreeRemoval {
                parent,
                branch_tip,
                command: None,
            }));
        };
        if entry.locked {
            return Err(problem(
                "worktree_locked",
                "missing worktree still has a locked Git registration",
            ));
        }
        let branch = branch_ref(&preparation.requested_branch);
        if matches.next().is_some()
            || !entry.prunable
            || entry.branch.as_deref() != Some(branch.as_str())
            || entry.head.is_none()
        {
            return Err(problem(
                "source_integrity",
                "missing worktree's Git registration does not match its managed path and branch",
            ));
        }
        if branch_tip != entry.head {
            return Err(problem(
                "source_integrity",
                "missing worktree registration and retained branch tip disagree",
            ));
        }
        let target_arg = git_path_argument(&target)?;
        let command = self
            .git
            .prepare_workspace_command_with_config_roots(
                &source.root,
                &["worktree", "remove", "--", &target_arg],
                GitBinding {
                    git_dir: &source.git_dir,
                    common_dir: &source.common_dir,
                },
                &[],
                deadline,
            )
            .await?;
        confirm_worktree_absence(path, &parent)?;
        Ok(Some(MissingWorktreeRemoval {
            parent,
            branch_tip,
            command: Some(command),
        }))
    }

    /// Non-force worktree removal command for an offline private stand whose
    /// daemon lock is held exclusively by cleanup.
    #[expect(
        clippy::disallowed_types,
        reason = "This prepares the authorized non-force worktree removal command for stand cleanup."
    )]
    pub(crate) async fn prepare_stand_worktree_remove(
        &self,
        preparation: &WorkspacePreparation,
        verified_tip: &str,
        deadline: Instant,
    ) -> Result<Command> {
        self.prepare_remove_checked(preparation, verified_tip, deadline)
            .await
    }

    #[expect(
        clippy::disallowed_types,
        reason = "This prepares the authorized non-force worktree removal command."
    )]
    async fn prepare_remove_checked(
        &self,
        preparation: &WorkspacePreparation,
        verified_tip: &str,
        deadline: Instant,
    ) -> Result<Command> {
        let subject = self.subject(preparation, deadline).await?;
        if subject.head != verified_tip {
            return Err(problem(
                "identity_changed",
                "worktree tip moved before removal",
            ));
        }
        self.require_clean(&subject, deadline).await?;
        self.confirm_subject(preparation, &subject, deadline)
            .await?;
        let target = git_path_argument(&subject.target.root)?;
        self.git
            .prepare_workspace_command_with_config_roots(
                &subject.source.root,
                &["worktree", "remove", "--", &target],
                GitBinding {
                    git_dir: &subject.source.git_dir,
                    common_dir: &subject.source.common_dir,
                },
                &[&subject.target.root],
                deadline,
            )
            .await
    }

    /// Call only after confirmed mutator quiescence. Present/partial/conflicting
    /// state remains Unknown; an exit status alone is never a removal proof.
    pub(crate) async fn inspect_removal(
        &self,
        preparation: &WorkspacePreparation,
        branch: RemovalBranch<'_>,
    ) -> WorkspaceRemovalOutcome {
        match self.inspect_removed(preparation, branch).await {
            Ok(outcome) => outcome,
            Err(error) => WorkspaceRemovalOutcome::Unknown {
                code: error.code,
                message: *error.message,
            },
        }
    }

    /// Owner readback of a legacy removal that never dispatched Git.
    pub(crate) async fn verify_absent_removal(
        &self,
        preparation: &WorkspacePreparation,
    ) -> Result<WorkspaceRemovalOutcome> {
        let path = Path::new(&preparation.worktree_path);
        let Some(parent) = observe_worktree_absence(path)? else {
            return Err(problem(
                "bad_state",
                "the managed worktree directory exists",
            ));
        };
        confirm_worktree_absence_after_grace(
            path,
            &parent,
            Instant::now() + READ_TIME + ABSENCE_GRACE,
        )
        .await?;
        let outcome = self
            .inspect_removed(preparation, RemovalBranch::Absent)
            .await?;
        confirm_worktree_absence(path, &parent)?;
        Ok(outcome)
    }

    async fn inspect_removed(
        &self,
        preparation: &WorkspacePreparation,
        branch: RemovalBranch<'_>,
    ) -> Result<WorkspaceRemovalOutcome> {
        let deadline = Instant::now() + READ_TIME;
        let source = self.source(preparation, deadline).await?;
        let target = destination_path(&preparation.worktree_path)?;
        separate_target(&source, &target)?;
        let Some(parent) = observe_worktree_absence(&target)? else {
            return Err(problem(
                "unknown",
                "worktree path still exists after removal",
            ));
        };
        if self
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
        let branch_tip = self
            .branch_oid(&source.root, &preparation.requested_branch, deadline)
            .await?;
        let branch_matches = match branch {
            RemovalBranch::Retained => branch_tip.is_some(),
            RemovalBranch::At(tip) => branch_tip.as_deref() == Some(tip),
            RemovalBranch::Absent => branch_tip.is_none(),
        };
        if !branch_matches {
            return Err(problem(
                "identity_changed",
                "removed workspace branch does not match its expected state",
            ));
        }
        confirm_worktree_absence(&target, &parent)?;
        Ok(WorkspaceRemovalOutcome::Removed {
            repo_common_dir: path_text(&source.common_dir)?,
            worktree_path: path_text(&target)?,
            branch: preparation.requested_branch.clone(),
            branch_retained: branch_tip.is_some(),
            directory_absent_before_remove: matches!(branch, RemovalBranch::Absent),
            branch_absent_before_remove: matches!(branch, RemovalBranch::Absent),
        })
    }

    async fn source(
        &self,
        preparation: &WorkspacePreparation,
        deadline: Instant,
    ) -> Result<Repository> {
        if !is_git_oid(&preparation.base_oid) {
            return Err(problem(
                "invalid",
                "workspace preparation omitted its exact frozen base",
            ));
        }
        let source = self.repository(&preparation.repo_path, deadline).await?;
        if !git_path_matches(
            &source.common_dir,
            &canonical_root(&preparation.repo_common_dir)?,
        ) {
            return Err(problem(
                "source_integrity",
                "source repository common directory differs from the frozen identity",
            ));
        }
        Ok(source)
    }

    async fn repository(&self, path: &str, deadline: Instant) -> Result<Repository> {
        self.workspace_repository(path, true, deadline).await
    }

    async fn workspace_repository(
        &self,
        path: &str,
        require_root: bool,
        deadline: Instant,
    ) -> Result<Repository> {
        let workspace = canonical_root(path)?;
        // One rev-parse reports toplevel, git dir and common dir together. Each
        // read costs two Git spawns (safe-overrides preflight plus the read),
        // so three separate reads serialize six spawns on every repository
        // identity check, including each project registration at daemon startup.
        let bytes = self
            .read(
                &workspace,
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
        let root = canonical_root(&actual_root)?;
        let root_text = path_text(&root)?;
        let workspace_text = path_text(&workspace)?;
        let root_key = workspace_path_key(&root_text);
        let workspace_key = workspace_path_key(&workspace_text);
        if (require_root && workspace_key != root_key)
            || !Path::new(workspace_key.as_ref()).starts_with(Path::new(root_key.as_ref()))
        {
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
        if branch != full_ref || !is_git_oid(&head) || entry.head.as_deref() != Some(head.as_str())
        {
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

    async fn require_clean(&self, subject: &Subject, deadline: Instant) -> Result<()> {
        self.require_tracked_clean(subject, deadline).await?;
        let root = &subject.target.root;
        if !self
            .scratch_listing(root, UNTRACKED_ARGS, deadline)
            .await?
            .is_empty()
            || !self
                .scratch_listing(root, IGNORED_ARGS, deadline)
                .await?
                .is_empty()
        {
            return Err(problem(
                "dirty",
                "workspace contains untracked or ignored files after scratch cleanup",
            ));
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
                if !is_git_oid(&oid) {
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
                prunable: false,
                locked: false,
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
                    if !is_git_oid(head) || entry.head.replace(head.to_owned()).is_some() {
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
                    entry.locked |= field == "locked" || field.starts_with("locked ");
                    entry.prunable |= field == "prunable" || field.starts_with("prunable ");
                }
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    async fn read(&self, root: &Path, args: &[&str], deadline: Instant) -> Result<Vec<u8>> {
        let output = self.git.workspace_command(root, args, deadline).await?;
        if output.code != 0 {
            return Err(Error::new(
                "unavailable",
                format!("Git {} exited with code {}", args[0], output.code),
            ));
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

fn creation_committer(preparation: &WorkspacePreparation) -> String {
    format!("Eliot workspace {}", preparation.start_command_id)
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

fn scratch_paths(paths: BTreeSet<String>) -> Result<BTreeSet<PathBuf>> {
    paths
        .into_iter()
        .map(|path| {
            scratch_relative(&path).map_err(|_| {
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

/// Observe a missing leaf from a readable parent. Windows keeps that parent
/// and its ancestors pinned while the caller retains the returned guard.
async fn confirm_worktree_absence_after_grace(
    path: &Path,
    parent: &Directory,
    deadline: Instant,
) -> Result<()> {
    timeout_at(deadline, sleep_until(Instant::now() + ABSENCE_GRACE))
        .await
        .map_err(|_| {
            problem(
                "unavailable",
                "worktree absence observation deadline elapsed",
            )
        })?;
    confirm_worktree_absence(path, parent)
}

pub(crate) fn observe_worktree_absence(path: &Path) -> Result<Option<Directory>> {
    if !absent(path)? {
        return Ok(None);
    }
    let parent = path
        .parent()
        .ok_or_else(|| problem("invalid", "worktree has no parent directory"))?;
    let unreadable = |_| {
        problem(
            "unavailable",
            "worktree parent directory is unavailable or unreadable",
        )
    };
    let guard = Directory::open(parent).map_err(unreadable)?;
    let list_error = |_| problem("unavailable", "worktree parent directory cannot be listed");
    for entry in fs::read_dir(parent).map_err(list_error)? {
        if git_path_matches(&entry.map_err(list_error)?.path(), path) {
            return Ok(None);
        }
    }
    Ok(absent(path)?.then_some(guard))
}

fn confirm_worktree_absence(path: &Path, previous: &Directory) -> Result<()> {
    let Some(current) = observe_worktree_absence(path)? else {
        return Err(problem(
            "worktree_present",
            "worktree reappeared during absence verification",
        ));
    };
    if current.identity()? != previous.identity()? {
        return Err(problem(
            "identity_changed",
            "worktree parent changed during absence verification",
        ));
    }
    Ok(())
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
