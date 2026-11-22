//! Local branch integration on an owned temporary worktree.
//!
//! The Router freezes registration, branch names and OIDs before command
//! admission. This module uses fixed Git argv, a private index, scoped process
//! custody and the registered project policy check. It never pushes.

use crate::{
    git_monitor::{GitProbe, git_path_argument},
    publication::TemporaryGitIndex,
    workspace_git::WorkspaceGit,
};
use eliot_core::{
    CommandState, Error, Result, SecretPolicy,
    api::{
        FrozenGitIntegrate, GitIntegrateBranchPin, GitIntegrateCaller,
        GitIntegrateCheckClassification, GitIntegrateCheckFailure, GitIntegrateDefinitionArgs,
        GitIntegrateOutcome, GitIntegrateRefUpdate, GitIntegrateStatus,
        MAX_GIT_INTEGRATE_CHECK_TAIL_BYTES, MAX_GIT_INTEGRATE_CONFLICT_PATHS,
        ProjectRegistrationView, is_git_oid,
    },
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

const REF_READ_BUDGET: Duration = Duration::from_secs(10);
const MAX_CONFLICT_PATH_BYTES: usize = 4 * 1024;

pub(crate) struct GitIntegrateExecution {
    pub state: CommandState,
    pub outcome: Value,
}

pub(crate) async fn freeze(
    registration: &ProjectRegistrationView,
    definition: &GitIntegrateDefinitionArgs,
    caller: Option<GitIntegrateCaller>,
    timeout_ms: i64,
) -> Result<FrozenGitIntegrate> {
    registration.validate()?;
    definition.validate()?;
    if timeout_ms < 1_000 {
        return Err(Error::new("invalid", "git.integrate timeout is too small"));
    }
    let (checkout, common_dir) = WorkspaceGit::new()?
        .repository_identity(&registration.checkout)
        .await?;
    if checkout != registration.checkout || common_dir != registration.repo_common_dir {
        return Err(Error::new(
            "not_allowed",
            "git.integrate repository identity differs from its saved registration",
        ));
    }
    if let Some(caller) = &caller {
        let (workspace, caller_common_dir) = WorkspaceGit::new()?
            .repository_identity(&caller.workspace)
            .await?;
        if workspace != caller.workspace || caller_common_dir != registration.repo_common_dir {
            return Err(Error::new(
                "not_allowed",
                "git.integrate caller workspace differs from its registered repository",
            ));
        }
    }
    let git = GitProbe::new()?;
    let root = Path::new(&registration.checkout);
    let deadline = Instant::now() + REF_READ_BUDGET;
    let base_oid = current_branch_oid(&git, root, &definition.base_branch, deadline).await?;
    let mut branches = Vec::with_capacity(definition.branches.len());
    for name in &definition.branches {
        branches.push(GitIntegrateBranchPin {
            branch: name.clone(),
            oid: current_branch_oid(&git, root, name, deadline).await?,
        });
    }
    let fast_forward = definition.fast_forward_to.as_deref().map(|name| async {
        Ok::<_, Error>(eliot_core::api::GitIntegrateTargetPin {
            branch: name.to_owned(),
            expected_oid: current_branch_oid(&git, root, name, deadline).await?,
        })
    });
    let fast_forward = match fast_forward {
        Some(future) => Some(future.await?),
        None => None,
    };
    let frozen = FrozenGitIntegrate {
        repository: registration.id.clone(),
        registration_revision: registration.revision,
        canonical_checkout: registration.checkout.clone(),
        common_dir: registration.repo_common_dir.clone(),
        base_branch: definition.base_branch.clone(),
        base_oid,
        branches,
        caller,
        fast_forward,
        timeout_ms,
    };
    frozen.validate()?;
    Ok(frozen)
}

async fn read_git_line(
    git: &GitProbe,
    root: &Path,
    args: &[&str],
    deadline: Instant,
) -> Result<String> {
    let output = git.workspace_command(root, args, deadline).await?;
    if output.code != 0 {
        return Err(unavailable(
            "Git could not read the requested repository fact",
        ));
    }
    let bytes = output
        .stdout
        .strip_suffix(b"\n")
        .ok_or_else(|| unavailable("Git readback is truncated"))?;
    let value = std::str::from_utf8(bytes).map_err(|_| unavailable("Git readback is not UTF-8"))?;
    if value.is_empty() || value.contains(['\r', '\n']) {
        return Err(unavailable("Git readback is malformed"));
    }
    Ok(value.to_owned())
}

fn unavailable(message: impl Into<String>) -> Error {
    Error::new("unavailable", message)
}

fn outcome(frozen: &FrozenGitIntegrate, status: GitIntegrateStatus) -> GitIntegrateOutcome {
    GitIntegrateOutcome {
        status,
        base_oid: frozen.base_oid.clone(),
        branches: frozen.branches.clone(),
        integration_oid: None,
        conflict_paths: Vec::new(),
        conflict_paths_truncated: false,
        check_failure: None,
        fast_forward: None,
        failure_code: None,
    }
}

fn execution(
    frozen: &FrozenGitIntegrate,
    mut state: CommandState,
    mut result: GitIntegrateOutcome,
    failure_code: Option<&str>,
) -> GitIntegrateExecution {
    if let Some(code) = failure_code {
        result.failure_code = Some(code.to_owned());
    }
    if result.validate_for(frozen).is_err() {
        result = outcome(frozen, GitIntegrateStatus::Unknown);
        result.failure_code = Some("git_integrate_result_invalid".to_owned());
        state = CommandState::Unknown;
    }
    let value = serde_json::to_value(result)
        .unwrap_or_else(|_| json!({"code":"git_integrate_result_encode_failed"}));
    GitIntegrateExecution {
        state,
        outcome: value,
    }
}

struct TemporaryWorktree {
    root: PathBuf,
    path: PathBuf,
}

impl TemporaryWorktree {
    fn create(data_root: &Path) -> Result<Self> {
        let data_root = fs::canonicalize(data_root)
            .map_err(|_| unavailable("private Git integration data root cannot be resolved"))?;
        let root = data_root.join(format!("git-integrate-{}", ulid::Ulid::new()));
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new()
                .mode(0o700)
                .create(&root)
                .map_err(|_| unavailable("private Git integration directory cannot be created"))?;
        }
        #[cfg(not(unix))]
        fs::create_dir(&root)
            .map_err(|_| unavailable("private Git integration directory cannot be created"))?;
        let canonical = fs::canonicalize(&root)
            .map_err(|_| unavailable("private Git integration directory cannot be resolved"))?;
        if canonical.parent() != Some(data_root.as_path())
            || fs::symlink_metadata(&canonical)
                .map(|metadata| crate::platform::is_reparse_point(&metadata))
                .unwrap_or(true)
        {
            let _ = fs::remove_dir(&root);
            return Err(unavailable("private Git integration directory is unsafe"));
        }
        Ok(Self {
            path: canonical.join("worktree"),
            root: canonical,
        })
    }

    fn cleanup_root(&self) -> Result<()> {
        let metadata = fs::symlink_metadata(&self.root)
            .map_err(|_| unavailable("private Git integration directory disappeared"))?;
        if !metadata.is_dir() || crate::platform::is_reparse_point(&metadata) {
            return Err(unavailable("private Git integration directory changed"));
        }
        fs::remove_dir(&self.root)
            .map_err(|_| unavailable("private Git integration artifacts remain"))?;
        Ok(())
    }
}

fn remaining(deadline: Instant) -> Result<Duration> {
    let value = deadline.saturating_duration_since(Instant::now());
    if value.is_zero() {
        Err(unavailable("git.integrate deadline elapsed"))
    } else {
        Ok(value)
    }
}

fn path_text(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| unavailable("Git integration path is not UTF-8"))
}

async fn read_git_line_with_index(
    git: &GitProbe,
    root: &Path,
    args: &[&str],
    index: &Path,
    deadline: Instant,
) -> Result<String> {
    let output = git
        .workspace_command_with_index(root, args, index, deadline)
        .await?;
    if output.code != 0 {
        return Err(unavailable(
            "Git could not read the private integration index",
        ));
    }
    let bytes = output
        .stdout
        .strip_suffix(b"\n")
        .ok_or_else(|| unavailable("Git integration readback is truncated"))?;
    let value = std::str::from_utf8(bytes)
        .map_err(|_| unavailable("Git integration readback is not UTF-8"))?;
    if value.is_empty() || value.contains('\r') || value.contains('\n') {
        return Err(unavailable("Git integration readback is malformed"));
    }
    Ok(value.to_owned())
}

async fn read_git_bytes_with_index(
    git: &GitProbe,
    root: &Path,
    args: &[&str],
    index: &Path,
    deadline: Instant,
) -> Result<Vec<u8>> {
    let output = git
        .workspace_command_with_index(root, args, index, deadline)
        .await?;
    if output.code != 0 {
        return Err(unavailable(
            "Git could not read the private integration worktree",
        ));
    }
    Ok(output.stdout)
}

async fn ref_is_checked_out(git: &GitProbe, root: &Path, branch: &str) -> Result<bool> {
    let output = git
        .workspace_command(
            root,
            &["worktree", "list", "--porcelain", "-z"],
            Instant::now() + REF_READ_BUDGET,
        )
        .await?;
    if output.code != 0 {
        return Err(unavailable("Git could not enumerate checked-out branches"));
    }
    let wanted = format!("branch refs/heads/{branch}");
    Ok(output.stdout.split(|byte| *byte == 0).any(|record| {
        if cfg!(windows) {
            record.eq_ignore_ascii_case(wanted.as_bytes())
        } else {
            record == wanted.as_bytes()
        }
    }))
}

async fn worktree_registered(git: &GitProbe, root: &Path, path: &Path) -> Result<bool> {
    let output = git
        .workspace_command(
            root,
            &["worktree", "list", "--porcelain", "-z"],
            Instant::now() + REF_READ_BUDGET,
        )
        .await?;
    if output.code != 0 {
        return Err(unavailable(
            "Git could not verify temporary worktree ownership",
        ));
    }
    let wanted = path_text(path)?;
    Ok(output.stdout.split(|byte| *byte == 0).any(|record| {
        record
            .strip_prefix(b"worktree ")
            .and_then(|found| std::str::from_utf8(found).ok())
            .is_some_and(|found| {
                #[cfg(windows)]
                {
                    eliot_core::display_path(found)
                        .replace('\\', "/")
                        .eq_ignore_ascii_case(&eliot_core::display_path(wanted).replace('\\', "/"))
                }
                #[cfg(not(windows))]
                {
                    found == wanted
                }
            })
    }))
}

fn process_exit(outcome: &crate::scripts::process::ProcessOutcome) -> Option<i32> {
    use crate::scripts::process::{ProcessFailure, ProcessOutcome};
    match outcome {
        ProcessOutcome::Completed(capture) => capture.status.code(),
        ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture: Some(capture),
        } => capture.status.code(),
        _ => None,
    }
}

fn process_cannot_have_effect(outcome: &crate::scripts::process::ProcessOutcome) -> bool {
    use crate::scripts::process::{ProcessFailure, ProcessOutcome};
    matches!(
        outcome,
        ProcessOutcome::NotSpawned(_)
            | ProcessOutcome::ProgramUnrunnable(_)
            | ProcessOutcome::Failed {
                reason: ProcessFailure::Exit,
                capture: Some(_),
            }
    )
}

fn process_quiescent(outcome: &crate::scripts::process::ProcessOutcome) -> bool {
    use crate::scripts::process::{ProcessFailure, ProcessOutcome};
    match outcome {
        ProcessOutcome::Completed(_)
        | ProcessOutcome::NotSpawned(_)
        | ProcessOutcome::ProgramUnrunnable(_)
        | ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture: Some(_),
        } => true,
        ProcessOutcome::Unknown { quiescent, .. } => *quiescent,
        ProcessOutcome::Failed { .. } => false,
    }
}

async fn run_git_effect(
    git: &GitProbe,
    root: &Path,
    args: &[&str],
    index: &Path,
    deadline: Instant,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
) -> Result<crate::scripts::process::ProcessOutcome> {
    git.workspace_effect_command_with_index(
        root,
        args,
        index,
        remaining(deadline)?,
        policy,
        cancellation,
    )
    .await
}

async fn current_head(
    git: &GitProbe,
    root: &Path,
    index: &Path,
    deadline: Instant,
) -> Result<String> {
    let oid = read_git_line_with_index(
        git,
        root,
        &["rev-parse", "--verify", "--end-of-options", "HEAD^{commit}"],
        index,
        deadline,
    )
    .await?;
    if is_git_oid(&oid) {
        Ok(oid)
    } else {
        Err(unavailable("temporary Git HEAD is not an exact commit OID"))
    }
}

async fn verify_private_tree(
    git: &GitProbe,
    root: &Path,
    index: &Path,
    commit: &str,
    deadline: Instant,
) -> Result<()> {
    let expected_tree = read_git_line_with_index(
        git,
        root,
        &[
            "rev-parse",
            "--verify",
            "--end-of-options",
            &format!("{commit}^{{tree}}"),
        ],
        index,
        deadline,
    )
    .await?;
    let index_tree = read_git_line_with_index(git, root, &["write-tree"], index, deadline).await?;
    if !is_git_oid(&expected_tree) || index_tree != expected_tree {
        return Err(unavailable(
            "private index differs from the exact integration commit",
        ));
    }
    let status = read_git_bytes_with_index(
        git,
        root,
        &["status", "--porcelain=v1", "--untracked-files=no", "-z"],
        index,
        deadline,
    )
    .await?;
    if !status.is_empty() {
        return Err(unavailable(
            "temporary worktree differs from the checked commit",
        ));
    }
    Ok(())
}

async fn conflict_paths(
    git: &GitProbe,
    root: &Path,
    index: &Path,
    deadline: Instant,
) -> Result<(Vec<String>, bool)> {
    let bytes = read_git_bytes_with_index(
        git,
        root,
        &["diff", "--name-only", "--diff-filter=U", "-z"],
        index,
        deadline,
    )
    .await?;
    let mut paths = Vec::new();
    let mut truncated = false;
    let mut bytes_used = 0;
    for record in bytes
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        if paths.len() >= MAX_GIT_INTEGRATE_CONFLICT_PATHS {
            truncated = true;
            break;
        }
        let Ok(path) = std::str::from_utf8(record) else {
            truncated = true;
            continue;
        };
        if bytes_used + path.len() > MAX_CONFLICT_PATH_BYTES {
            truncated = true;
            continue;
        }
        bytes_used += path.len();
        paths.push(path.to_owned());
    }
    Ok((paths, truncated))
}

fn diagnostic_tail(value: &str) -> String {
    let mut start = value
        .len()
        .saturating_sub(MAX_GIT_INTEGRATE_CHECK_TAIL_BYTES);
    while !value.is_char_boundary(start) {
        start += 1;
    }
    value[start..].to_owned()
}

enum MergeResult {
    Merged(String),
    Conflict(Vec<String>, bool),
    Failed(&'static str),
    Unknown(&'static str),
}

async fn merge_branch(
    git: &GitProbe,
    root: &Path,
    index: &Path,
    pin: &GitIntegrateBranchPin,
    deadline: Instant,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
) -> MergeResult {
    let message = format!("Integrate {}", pin.branch);
    let args = [
        "-c",
        "user.name=Eliot Swarm",
        "-c",
        "user.email=eliot@localhost",
        "merge",
        "--no-edit",
        "--no-ff",
        "--no-verify",
        "--no-gpg-sign",
        "--message",
        message.as_str(),
        pin.oid.as_str(),
    ];
    let result = match run_git_effect(git, root, &args, index, deadline, policy, cancellation).await
    {
        Ok(result) => result,
        Err(_) => return MergeResult::Failed("git_integrate_merge_not_started"),
    };
    match process_exit(&result) {
        Some(0) => match current_head(git, root, index, deadline).await {
            Ok(head) => MergeResult::Merged(head),
            Err(_) => MergeResult::Unknown("git_integrate_merge_readback_unknown"),
        },
        Some(_) => match conflict_paths(git, root, index, deadline).await {
            Ok((paths, truncated)) if !paths.is_empty() || truncated => {
                MergeResult::Conflict(paths, truncated)
            }
            Ok(_) => MergeResult::Failed("git_integrate_merge_failed"),
            Err(_) => MergeResult::Unknown("git_integrate_conflict_readback_unknown"),
        },
        None if process_cannot_have_effect(&result) => {
            MergeResult::Failed("git_integrate_merge_not_started")
        }
        None => MergeResult::Unknown("git_integrate_merge_outcome_unknown"),
    }
}

async fn reset_to_base(
    git: &GitProbe,
    root: &Path,
    index: &Path,
    base_oid: &str,
    deadline: Instant,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
) -> Result<()> {
    let clean_args = ["clean", "-fdx"];
    let clean = run_git_effect(
        git,
        root,
        &clean_args,
        index,
        deadline,
        Arc::clone(&policy),
        cancellation.clone(),
    )
    .await?;
    if process_exit(&clean) != Some(0) {
        return Err(unavailable(
            "temporary integration worktree could not be cleaned",
        ));
    }
    let reset_args = ["reset", "--hard", base_oid];
    let reset = run_git_effect(
        git,
        root,
        &reset_args,
        index,
        deadline,
        policy,
        cancellation,
    )
    .await?;
    if process_exit(&reset) != Some(0) {
        return Err(unavailable(
            "temporary integration worktree could not return to base",
        ));
    }
    verify_private_tree(git, root, index, base_oid, deadline).await
}

async fn run_policy_check(
    argv: &[String],
    configured_timeout_ms: i64,
    root: &Path,
    index: &Path,
    deadline: Instant,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
) -> Result<crate::router::project_checks::CheckOutcome> {
    let remaining_ms = deadline
        .saturating_duration_since(Instant::now())
        .as_millis()
        .min(i64::MAX as u128) as i64;
    if remaining_ms < 1_000 {
        return Ok(crate::router::project_checks::CheckOutcome::Unknown(
            "git.integrate total deadline elapsed".to_owned(),
        ));
    }
    crate::router::project_checks::run_argv_with_index(
        argv,
        path_text(root)?,
        configured_timeout_ms.min(remaining_ms),
        policy,
        cancellation,
        Some(index),
    )
    .await
}

struct IsolationResult {
    failed: Vec<String>,
    unknown: Vec<String>,
}

#[derive(Clone, Copy)]
struct GitRun<'a> {
    deadline: Instant,
    policy: &'a Arc<SecretPolicy>,
    cancellation: &'a watch::Receiver<bool>,
}

async fn verify_private_worktree(
    git: &GitProbe,
    root: &Path,
    expected_common_dir: &str,
    deadline: Instant,
) -> Result<()> {
    let root_text = path_text(root)?;
    let (workspace, common_dir) = WorkspaceGit::new()?.repository_identity(root_text).await?;
    let head = git
        .workspace_command(root, &["symbolic-ref", "-q", "HEAD"], deadline)
        .await?;
    if workspace != root_text || common_dir != expected_common_dir || head.code != 1 {
        return Err(unavailable("private integration worktree identity changed"));
    }
    Ok(())
}

async fn isolate_failed_branches(
    git: &GitProbe,
    root: &Path,
    index: &Path,
    frozen: &FrozenGitIntegrate,
    check_argv: &[String],
    check_timeout_ms: i64,
    run: GitRun<'_>,
) -> std::result::Result<IsolationResult, &'static str> {
    let GitRun {
        deadline,
        policy,
        cancellation,
    } = run;
    let mut result = IsolationResult {
        failed: Vec::new(),
        unknown: Vec::new(),
    };
    for (position, pin) in frozen.branches.iter().enumerate() {
        if remaining(deadline).is_err() {
            result.unknown.extend(
                frozen.branches[position..]
                    .iter()
                    .map(|item| item.branch.clone()),
            );
            return Ok(result);
        }
        verify_private_worktree(git, root, &frozen.common_dir, deadline)
            .await
            .map_err(|_| "git_integrate_isolation_worktree_changed")?;
        reset_to_base(
            git,
            root,
            index,
            &frozen.base_oid,
            deadline,
            Arc::clone(policy),
            cancellation.clone(),
        )
        .await
        .map_err(|_| "git_integrate_isolation_reset_unknown")?;
        match merge_branch(
            git,
            root,
            index,
            pin,
            deadline,
            Arc::clone(policy),
            cancellation.clone(),
        )
        .await
        {
            MergeResult::Merged(oid) => {
                verify_private_tree(git, root, index, &oid, deadline)
                    .await
                    .map_err(|_| "git_integrate_isolation_tree_unknown")?;
                match run_policy_check(
                    check_argv,
                    check_timeout_ms,
                    root,
                    index,
                    deadline,
                    Arc::clone(policy),
                    cancellation.clone(),
                )
                .await
                {
                    Ok(crate::router::project_checks::CheckOutcome::Passed) => {}
                    Ok(crate::router::project_checks::CheckOutcome::Failed { .. }) => {
                        result.failed.push(pin.branch.clone());
                    }
                    Ok(crate::router::project_checks::CheckOutcome::Unknown(_)) => {
                        return Err("git_integrate_isolation_check_unknown");
                    }
                    Err(_) => result.unknown.push(pin.branch.clone()),
                }
            }
            MergeResult::Conflict(_, _) | MergeResult::Failed(_) => {
                result.failed.push(pin.branch.clone());
            }
            MergeResult::Unknown(_) => return Err("git_integrate_isolation_merge_unknown"),
        }
    }
    Ok(result)
}

async fn cleanup_temporary_worktree(
    git: &GitProbe,
    repository_root: &Path,
    temporary: &TemporaryWorktree,
    index: &Path,
    deadline: Instant,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
) -> Result<()> {
    let registered = worktree_registered(git, repository_root, &temporary.path).await?;
    let exists = fs::symlink_metadata(&temporary.path).is_ok();
    if registered || exists {
        let path = git_path_argument(&temporary.path)?;
        let args = ["worktree", "remove", "--force", path.as_str()];
        let outcome = run_git_effect(
            git,
            repository_root,
            &args,
            index,
            deadline,
            Arc::clone(&policy),
            cancellation,
        )
        .await?;
        if process_exit(&outcome) != Some(0) {
            return Err(unavailable("temporary Git worktree removal is unconfirmed"));
        }
        if worktree_registered(git, repository_root, &temporary.path).await?
            || fs::symlink_metadata(&temporary.path).is_ok()
        {
            return Err(unavailable(
                "temporary Git worktree still exists after removal",
            ));
        }
    }
    temporary.cleanup_root()
}

async fn current_branch_oid(
    git: &GitProbe,
    root: &Path,
    branch: &str,
    deadline: Instant,
) -> Result<String> {
    let reference = format!("refs/heads/{branch}^{{commit}}");
    let oid = read_git_line(
        git,
        root,
        &["rev-parse", "--verify", "--end-of-options", &reference],
        deadline,
    )
    .await?;
    if is_git_oid(&oid) {
        Ok(oid)
    } else {
        Err(unavailable("Git branch ref is not an exact OID"))
    }
}
async fn branch_is_ancestor(
    git: &GitProbe,
    root: &Path,
    ancestor: &str,
    descendant: &str,
    deadline: Instant,
) -> Result<bool> {
    let output = git
        .workspace_command(
            root,
            &["merge-base", "--is-ancestor", ancestor, descendant],
            deadline,
        )
        .await?;
    match output.code {
        0 => Ok(true),
        1 => Ok(false),
        _ => Err(unavailable("Git could not verify fast-forward ancestry")),
    }
}

struct TargetUpdateFailure {
    code: &'static str,
    unknown: bool,
}

async fn update_fast_forward(
    git: &GitProbe,
    root: &Path,
    index: &Path,
    frozen: &FrozenGitIntegrate,
    integration_oid: &str,
    run: GitRun<'_>,
) -> std::result::Result<GitIntegrateRefUpdate, TargetUpdateFailure> {
    let GitRun {
        deadline,
        policy,
        cancellation,
    } = run;
    let Some(target) = &frozen.fast_forward else {
        return Err(TargetUpdateFailure {
            code: "git_integrate_target_missing",
            unknown: false,
        });
    };
    match ref_is_checked_out(git, root, &target.branch).await {
        Ok(false) => {}
        Ok(true) => {
            return Err(TargetUpdateFailure {
                code: "git_integrate_target_checked_out",
                unknown: false,
            });
        }
        Err(_) => {
            return Err(TargetUpdateFailure {
                code: "git_integrate_target_checkout_unknown",
                unknown: true,
            });
        }
    }
    match branch_is_ancestor(git, root, &target.expected_oid, integration_oid, deadline).await {
        Ok(true) => {}
        Ok(false) => {
            return Err(TargetUpdateFailure {
                code: "git_integrate_target_not_fast_forwardable",
                unknown: false,
            });
        }
        Err(_) => {
            return Err(TargetUpdateFailure {
                code: "git_integrate_target_ancestry_unknown",
                unknown: true,
            });
        }
    }
    match current_branch_oid(git, root, &target.branch, deadline).await {
        Ok(oid) if oid == target.expected_oid => {}
        Ok(_) => {
            return Err(TargetUpdateFailure {
                code: "git_integrate_target_moved",
                unknown: false,
            });
        }
        Err(_) => {
            return Err(TargetUpdateFailure {
                code: "git_integrate_target_readback_unknown",
                unknown: true,
            });
        }
    }
    match ref_is_checked_out(git, root, &target.branch).await {
        Ok(false) => {}
        Ok(true) => {
            return Err(TargetUpdateFailure {
                code: "git_integrate_target_checked_out",
                unknown: false,
            });
        }
        Err(_) => {
            return Err(TargetUpdateFailure {
                code: "git_integrate_target_checkout_unknown",
                unknown: true,
            });
        }
    }
    let reference = format!("refs/heads/{}", target.branch);
    let args = [
        "update-ref",
        "--no-deref",
        reference.as_str(),
        integration_oid,
        target.expected_oid.as_str(),
    ];
    let effect = match run_git_effect(
        git,
        root,
        &args,
        index,
        deadline,
        Arc::clone(policy),
        cancellation.clone(),
    )
    .await
    {
        Ok(effect) => effect,
        Err(_) => {
            return Err(TargetUpdateFailure {
                code: "git_integrate_target_update_not_started",
                unknown: false,
            });
        }
    };
    let exit = process_exit(&effect);
    if exit.is_none() && !process_cannot_have_effect(&effect) && !process_quiescent(&effect) {
        return Err(TargetUpdateFailure {
            code: "git_integrate_target_update_unknown",
            unknown: true,
        });
    }
    let current = current_branch_oid(git, root, &target.branch, deadline).await;
    match current {
        Ok(oid) if oid == integration_oid => {
            match ref_is_checked_out(git, root, &target.branch).await {
                Ok(false) => Ok(GitIntegrateRefUpdate {
                    branch: target.branch.clone(),
                    oid,
                }),
                Ok(true) => Err(TargetUpdateFailure {
                    code: "git_integrate_target_checkout_race_unknown",
                    unknown: true,
                }),
                Err(_) => Err(TargetUpdateFailure {
                    code: "git_integrate_target_checkout_unknown",
                    unknown: true,
                }),
            }
        }
        Ok(oid) if oid == target.expected_oid && exit != Some(0) => Err(TargetUpdateFailure {
            code: "git_integrate_target_update_failed",
            unknown: false,
        }),
        Ok(_) => Err(TargetUpdateFailure {
            code: "git_integrate_target_update_readback_mismatch",
            unknown: true,
        }),
        Err(_) => Err(TargetUpdateFailure {
            code: "git_integrate_target_update_readback_unknown",
            unknown: true,
        }),
    }
}
pub(crate) async fn execute(
    data_root: &Path,
    frozen: &FrozenGitIntegrate,
    registration: &ProjectRegistrationView,
    cancellation: watch::Receiver<bool>,
    secret_policy: Arc<SecretPolicy>,
) -> GitIntegrateExecution {
    let mut result = outcome(frozen, GitIntegrateStatus::Unknown);
    if frozen.validate().is_err()
        || registration.validate().is_err()
        || frozen.repository != registration.id
        || frozen.registration_revision != registration.revision
        || frozen.canonical_checkout != registration.checkout
        || frozen.common_dir != registration.repo_common_dir
    {
        result.failure_code = Some("git_integrate_binding_stale".to_owned());
        return execution(frozen, CommandState::Rejected, result, None);
    }
    if crate::automation::verify_project_registration(data_root, registration)
        .await
        .is_err()
    {
        result.failure_code = Some("git_integrate_registration_unavailable".to_owned());
        return execution(frozen, CommandState::Rejected, result, None);
    }

    let workspace_git = match WorkspaceGit::new() {
        Ok(git) => git,
        Err(_) => {
            result.failure_code = Some("git_integrate_git_unavailable".to_owned());
            return execution(frozen, CommandState::Rejected, result, None);
        }
    };
    match workspace_git
        .repository_identity(&registration.checkout)
        .await
    {
        Ok((checkout, common_dir))
            if checkout == frozen.canonical_checkout && common_dir == frozen.common_dir => {}
        _ => {
            result.failure_code = Some("git_integrate_repository_identity_changed".to_owned());
            return execution(frozen, CommandState::Rejected, result, None);
        }
    }
    if let Some(caller) = &frozen.caller {
        match workspace_git.repository_identity(&caller.workspace).await {
            Ok((workspace, common_dir))
                if workspace == caller.workspace && common_dir == frozen.common_dir => {}
            _ => {
                result.failure_code = Some("git_integrate_manager_workspace_changed".to_owned());
                return execution(frozen, CommandState::Rejected, result, None);
            }
        }
    }
    let git = match GitProbe::new() {
        Ok(git) => git,
        Err(_) => {
            result.failure_code = Some("git_integrate_git_unavailable".to_owned());
            return execution(frozen, CommandState::Rejected, result, None);
        }
    };
    let repo = Path::new(&frozen.canonical_checkout);
    let preflight_deadline = Instant::now() + REF_READ_BUDGET;
    match current_branch_oid(&git, repo, &frozen.base_branch, preflight_deadline).await {
        Ok(oid) if oid == frozen.base_oid => {}
        Ok(_) => {
            result.failure_code = Some("git_integrate_base_ref_moved".to_owned());
            return execution(frozen, CommandState::Rejected, result, None);
        }
        Err(_) => {
            result.failure_code = Some("git_integrate_base_ref_unavailable".to_owned());
            return execution(frozen, CommandState::Rejected, result, None);
        }
    }
    for pin in &frozen.branches {
        match current_branch_oid(&git, repo, &pin.branch, preflight_deadline).await {
            Ok(oid) if oid == pin.oid => {}
            Ok(_) => {
                result.failure_code = Some("git_integrate_source_ref_moved".to_owned());
                return execution(frozen, CommandState::Rejected, result, None);
            }
            Err(_) => {
                result.failure_code = Some("git_integrate_source_ref_unavailable".to_owned());
                return execution(frozen, CommandState::Rejected, result, None);
            }
        }
    }
    if let Some(target) = &frozen.fast_forward {
        match current_branch_oid(&git, repo, &target.branch, preflight_deadline).await {
            Ok(oid) if oid == target.expected_oid => {}
            Ok(_) => {
                result.failure_code = Some("git_integrate_target_ref_moved".to_owned());
                return execution(frozen, CommandState::Rejected, result, None);
            }
            Err(_) => {
                result.failure_code = Some("git_integrate_target_ref_unavailable".to_owned());
                return execution(frozen, CommandState::Rejected, result, None);
            }
        }
        match ref_is_checked_out(&git, repo, &target.branch).await {
            Ok(false) => {}
            Ok(true) => {
                result.failure_code = Some("git_integrate_target_checked_out".to_owned());
                return execution(frozen, CommandState::Rejected, result, None);
            }
            Err(_) => {
                result.failure_code = Some("git_integrate_target_checkout_unknown".to_owned());
                return execution(frozen, CommandState::Unknown, result, None);
            }
        }
    }

    let check = match crate::config::load_project_registration_config(data_root) {
        Ok(configs) => configs
            .into_iter()
            .find(|project| project.id == registration.id)
            .and_then(|project| project.policy)
            .and_then(|policy| policy.check),
        Err(_) => {
            result.failure_code = Some("git_integrate_project_check_config_unavailable".to_owned());
            return execution(frozen, CommandState::Rejected, result, None);
        }
    };
    let Some(check) = check else {
        result.failure_code = Some("git_integrate_project_check_not_configured".to_owned());
        return execution(frozen, CommandState::Rejected, result, None);
    };
    if secret_policy.contains_value(&json!(check.argv)) {
        result.failure_code = Some("git_integrate_project_check_secret".to_owned());
        return execution(frozen, CommandState::Rejected, result, None);
    }

    let deadline = Instant::now()
        .checked_add(Duration::from_millis(frozen.timeout_ms as u64))
        .unwrap_or_else(Instant::now);
    let run = GitRun {
        deadline,
        policy: &secret_policy,
        cancellation: &cancellation,
    };
    let temporary = match TemporaryWorktree::create(data_root) {
        Ok(temporary) => temporary,
        Err(_) => {
            result.failure_code = Some("git_integrate_private_worktree_unavailable".to_owned());
            return execution(frozen, CommandState::Rejected, result, None);
        }
    };
    let index = match TemporaryGitIndex::create_with_prefix(data_root, "git-integrate-index") {
        Ok(index) => index,
        Err(_) => {
            let _ = temporary.cleanup_root();
            result.failure_code = Some("git_integrate_private_index_unavailable".to_owned());
            return execution(frozen, CommandState::Rejected, result, None);
        }
    };
    let repo_root = Path::new(&frozen.canonical_checkout);
    let temporary_root = temporary.path.as_path();
    let temporary_path = match git_path_argument(temporary_root) {
        Ok(path) => path,
        Err(_) => {
            let _ = temporary.cleanup_root();
            result.failure_code = Some("git_integrate_private_worktree_path_invalid".to_owned());
            return execution(frozen, CommandState::Rejected, result, None);
        }
    };
    let add_args = [
        "worktree",
        "add",
        "--detach",
        "--no-checkout",
        temporary_path.as_str(),
        frozen.base_oid.as_str(),
    ];
    let added = match run_git_effect(
        &git,
        repo_root,
        &add_args,
        index.path(),
        deadline,
        Arc::clone(&secret_policy),
        cancellation.clone(),
    )
    .await
    {
        Ok(effect) => effect,
        Err(_) => {
            let _ = temporary.cleanup_root();
            result.failure_code = Some("git_integrate_worktree_add_not_started".to_owned());
            return execution(frozen, CommandState::Failed, result, None);
        }
    };
    let add_exit = process_exit(&added);
    if add_exit != Some(0) {
        if process_cannot_have_effect(&added) {
            if cleanup_temporary_worktree(
                &git,
                repo_root,
                &temporary,
                index.path(),
                deadline,
                Arc::clone(&secret_policy),
                cancellation.clone(),
            )
            .await
            .is_err()
            {
                std::mem::forget(index);
                result.failure_code = Some("git_integrate_worktree_add_cleanup_unknown".to_owned());
                return execution(frozen, CommandState::Unknown, result, None);
            }
            result.failure_code = Some("git_integrate_worktree_add_failed".to_owned());
            return execution(frozen, CommandState::Failed, result, None);
        }
        std::mem::forget(index);
        result.failure_code = Some("git_integrate_worktree_add_unknown".to_owned());
        return execution(frozen, CommandState::Unknown, result, None);
    }

    if verify_private_worktree(&git, temporary_root, &frozen.common_dir, deadline)
        .await
        .is_err()
    {
        std::mem::forget(index);
        result.failure_code = Some("git_integrate_private_worktree_unverified".to_owned());
        return execution(frozen, CommandState::Unknown, result, None);
    }
    let reset_args = ["reset", "--hard", frozen.base_oid.as_str()];
    let reset = match run_git_effect(
        &git,
        temporary_root,
        &reset_args,
        index.path(),
        deadline,
        Arc::clone(&secret_policy),
        cancellation.clone(),
    )
    .await
    {
        Ok(effect) => effect,
        Err(_) => {
            result.failure_code = Some("git_integrate_base_checkout_not_started".to_owned());
            if cleanup_temporary_worktree(
                &git,
                repo_root,
                &temporary,
                index.path(),
                deadline,
                Arc::clone(&secret_policy),
                cancellation.clone(),
            )
            .await
            .is_err()
            {
                std::mem::forget(index);
                result.failure_code = Some("git_integrate_base_cleanup_unknown".to_owned());
                return execution(frozen, CommandState::Unknown, result, None);
            }
            return execution(frozen, CommandState::Failed, result, None);
        }
    };
    let reset_exit = process_exit(&reset);
    let base_tree = if reset_exit == Some(0) {
        verify_private_tree(
            &git,
            temporary_root,
            index.path(),
            &frozen.base_oid,
            deadline,
        )
        .await
        .is_ok()
    } else {
        false
    };
    if !base_tree {
        if reset_exit.is_none() && !process_cannot_have_effect(&reset) {
            std::mem::forget(index);
            result.failure_code = Some("git_integrate_base_checkout_unknown".to_owned());
            return execution(frozen, CommandState::Unknown, result, None);
        }
        if cleanup_temporary_worktree(
            &git,
            repo_root,
            &temporary,
            index.path(),
            deadline,
            Arc::clone(&secret_policy),
            cancellation.clone(),
        )
        .await
        .is_err()
        {
            std::mem::forget(index);
            result.failure_code = Some("git_integrate_base_cleanup_unknown".to_owned());
            return execution(frozen, CommandState::Unknown, result, None);
        }
        result.failure_code = Some("git_integrate_base_checkout_failed".to_owned());
        return execution(frozen, CommandState::Failed, result, None);
    }

    let mut integration_oid = frozen.base_oid.clone();
    for pin in &frozen.branches {
        match merge_branch(
            &git,
            temporary_root,
            index.path(),
            pin,
            deadline,
            Arc::clone(&secret_policy),
            cancellation.clone(),
        )
        .await
        {
            MergeResult::Merged(oid) => integration_oid = oid,
            MergeResult::Conflict(paths, truncated) => {
                result.status = GitIntegrateStatus::Conflict;
                result.conflict_paths = paths;
                result.conflict_paths_truncated = truncated;
                if cleanup_temporary_worktree(
                    &git,
                    repo_root,
                    &temporary,
                    index.path(),
                    deadline,
                    Arc::clone(&secret_policy),
                    cancellation.clone(),
                )
                .await
                .is_err()
                {
                    std::mem::forget(index);
                    result.status = GitIntegrateStatus::Unknown;
                    result.failure_code = Some("git_integrate_conflict_cleanup_unknown".to_owned());
                    return execution(frozen, CommandState::Unknown, result, None);
                }
                return execution(
                    frozen,
                    CommandState::Failed,
                    result,
                    Some("git_integrate_conflict"),
                );
            }
            MergeResult::Failed(code) => {
                if cleanup_temporary_worktree(
                    &git,
                    repo_root,
                    &temporary,
                    index.path(),
                    deadline,
                    Arc::clone(&secret_policy),
                    cancellation.clone(),
                )
                .await
                .is_err()
                {
                    std::mem::forget(index);
                    result.failure_code = Some("git_integrate_merge_cleanup_unknown".to_owned());
                    return execution(frozen, CommandState::Unknown, result, None);
                }
                result.failure_code = Some(code.to_owned());
                return execution(frozen, CommandState::Failed, result, None);
            }
            MergeResult::Unknown(code) => {
                std::mem::forget(index);
                result.failure_code = Some(code.to_owned());
                return execution(frozen, CommandState::Unknown, result, None);
            }
        }
    }
    if verify_private_tree(
        &git,
        temporary_root,
        index.path(),
        &integration_oid,
        deadline,
    )
    .await
    .is_err()
    {
        std::mem::forget(index);
        result.failure_code = Some("git_integrate_combined_tree_unknown".to_owned());
        return execution(frozen, CommandState::Unknown, result, None);
    }
    match run_policy_check(
        &check.argv,
        check.deadline_ms,
        temporary_root,
        index.path(),
        deadline,
        Arc::clone(&secret_policy),
        cancellation.clone(),
    )
    .await
    {
        Ok(crate::router::project_checks::CheckOutcome::Passed) => {}
        Ok(crate::router::project_checks::CheckOutcome::Failed { exit_code, output }) => {
            let isolation = isolate_failed_branches(
                &git,
                temporary_root,
                index.path(),
                frozen,
                &check.argv,
                check.deadline_ms,
                run,
            )
            .await;
            let isolation = match isolation {
                Ok(isolation) => isolation,
                Err(code) => {
                    std::mem::forget(index);
                    result.status = GitIntegrateStatus::CheckFailed;
                    result.integration_oid = Some(integration_oid.clone());
                    result.check_failure = Some(GitIntegrateCheckFailure {
                        exit_code,
                        tail: diagnostic_tail(&output),
                        classification: GitIntegrateCheckClassification::IsolationUnknown,
                        failed_branches: Vec::new(),
                        unknown_branches: frozen
                            .branches
                            .iter()
                            .map(|pin| pin.branch.clone())
                            .collect(),
                    });
                    result.failure_code = Some(code.to_owned());
                    return execution(frozen, CommandState::Unknown, result, None);
                }
            };
            let classification = if !isolation.unknown.is_empty() {
                GitIntegrateCheckClassification::IsolationUnknown
            } else if !isolation.failed.is_empty() {
                GitIntegrateCheckClassification::BranchFailures
            } else {
                GitIntegrateCheckClassification::CombinationOnly
            };
            result.status = GitIntegrateStatus::CheckFailed;
            result.integration_oid = Some(integration_oid.clone());
            result.check_failure = Some(GitIntegrateCheckFailure {
                exit_code,
                tail: diagnostic_tail(&output),
                classification,
                failed_branches: isolation.failed,
                unknown_branches: isolation.unknown,
            });
            if cleanup_temporary_worktree(
                &git,
                repo_root,
                &temporary,
                index.path(),
                deadline,
                Arc::clone(&secret_policy),
                cancellation.clone(),
            )
            .await
            .is_err()
            {
                std::mem::forget(index);
                result.status = GitIntegrateStatus::Unknown;
                result.failure_code = Some("git_integrate_check_cleanup_unknown".to_owned());
                return execution(frozen, CommandState::Unknown, result, None);
            }
            return execution(
                frozen,
                CommandState::Failed,
                result,
                Some("git_integrate_check_failed"),
            );
        }
        Ok(crate::router::project_checks::CheckOutcome::Unknown(_)) | Err(_) => {
            std::mem::forget(index);
            result.failure_code = Some("git_integrate_check_unknown".to_owned());
            return execution(frozen, CommandState::Unknown, result, None);
        }
    }

    let checked_tree_intact =
        verify_private_worktree(&git, temporary_root, &frozen.common_dir, deadline)
            .await
            .is_ok()
            && verify_private_tree(
                &git,
                temporary_root,
                index.path(),
                &integration_oid,
                deadline,
            )
            .await
            .is_ok();
    if cleanup_temporary_worktree(
        &git,
        repo_root,
        &temporary,
        index.path(),
        deadline,
        Arc::clone(&secret_policy),
        cancellation.clone(),
    )
    .await
    .is_err()
    {
        std::mem::forget(index);
        result.status = GitIntegrateStatus::Unknown;
        result.failure_code = Some("git_integrate_cleanup_unknown".to_owned());
        return execution(frozen, CommandState::Unknown, result, None);
    }
    if !checked_tree_intact {
        result.failure_code = Some("git_integrate_checked_tree_changed".to_owned());
        return execution(frozen, CommandState::Failed, result, None);
    }
    result.status = GitIntegrateStatus::Merged;
    result.integration_oid = Some(integration_oid.clone());

    if frozen.fast_forward.is_some() {
        match update_fast_forward(&git, repo_root, index.path(), frozen, &integration_oid, run)
            .await
        {
            Ok(update) => result.fast_forward = Some(update),
            Err(failure) => {
                result.failure_code = Some(failure.code.to_owned());
                let state = if failure.unknown {
                    result.status = GitIntegrateStatus::Unknown;
                    CommandState::Unknown
                } else {
                    CommandState::Failed
                };
                return execution(frozen, state, result, None);
            }
        }
    }
    execution(frozen, CommandState::Applied, result, None)
}
