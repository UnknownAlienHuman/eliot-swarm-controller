//! Durable project-check publication effects.
//!
//! Rule admission and Store dispatch own idempotency. This module consumes only
//! the dedicated passing-check receipt, confirms its managed worktree snapshot,
//! commits locally once, then performs explicitly configured push/PR effects.

use crate::{
    automation,
    common::now_ms,
    git_actions,
    git_monitor::{GitProbe, scope_digests, work_fingerprint_from_snapshot},
    workspace_git::WorkspaceGit,
};
use eliot_core::{
    Agent, CommandState, Error, GitSnapshot, Result, SecretPolicy, WorkspacePreparation,
    api::{
        FrozenGitPublish, GitPublishOutcome, GitPublishTrigger, ProjectGitHubCliView,
        ProjectRegistrationView, is_git_oid,
    },
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{process::Command, sync::watch, time::Instant};

use crate::scripts::process::{
    ProcessFailure, ProcessOutcome, local_process_command, run_owned_process,
};

pub(crate) struct GitPublishExecution {
    pub state: CommandState,
    pub outcome: Value,
    pub cleanup_view: Option<PathBuf>,
}

pub(crate) struct GitPublishExecutionContext<'a> {
    pub router: &'a crate::router::Router,
    pub data_root: &'a Path,
    pub command_id: &'a str,
    pub frozen: &'a FrozenGitPublish,
    pub registration: &'a ProjectRegistrationView,
    pub agent: &'a Agent,
    pub workspace: &'a WorkspacePreparation,
    pub cancellation: watch::Receiver<bool>,
    pub secret_policy: Arc<SecretPolicy>,
}

struct TemporaryGitIndex {
    directory: PathBuf,
    index_file: PathBuf,
}

impl TemporaryGitIndex {
    fn create(data_root: &Path) -> Result<Self> {
        let data_root = fs::canonicalize(data_root).map_err(|_| {
            Error::new(
                "unavailable",
                "private Git publication data root cannot be resolved",
            )
        })?;
        let directory = data_root.join(format!("publish-index-{}", ulid::Ulid::new()));
        #[cfg(unix)]
        let create = {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new().mode(0o700).create(&directory)
        };
        #[cfg(not(unix))]
        let create = fs::create_dir(&directory);
        create.map_err(|_| {
            Error::new(
                "unavailable",
                "private Git publication index cannot be created",
            )
        })?;
        Ok(Self {
            index_file: directory.join("index"),
            directory,
        })
    }

    fn path(&self) -> &Path {
        &self.index_file
    }
}

impl Drop for TemporaryGitIndex {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.index_file.with_extension("lock"));
        let _ = fs::remove_file(&self.index_file);
        let _ = fs::remove_dir(&self.directory);
    }
}

struct PublishCommitContext<'a> {
    router: &'a crate::router::Router,
    data_root: &'a Path,
    command_id: &'a str,
    frozen: &'a FrozenGitPublish,
    registration: &'a ProjectRegistrationView,
    git: &'a GitProbe,
    root: &'a Path,
    commit_oid: String,
    commit_subject: String,
    committed: bool,
    index_file: Option<&'a Path>,
    cancellation: watch::Receiver<bool>,
    secret_policy: Arc<SecretPolicy>,
}

/// Freeze one qualified check event against the current owner registration and
/// exact Store-managed worktree. No mutation occurs during this preparation.
pub(crate) async fn freeze(
    registration: &ProjectRegistrationView,
    agent: &Agent,
    workspace: &WorkspacePreparation,
    trigger: GitPublishTrigger,
) -> Result<FrozenGitPublish> {
    registration.validate()?;
    trigger.validate()?;
    verify_managed_workspace(registration, agent, workspace, &trigger)?;

    let (canonical_workspace, common_dir) = WorkspaceGit::new()?
        .repository_identity(&trigger.workspace)
        .await?;
    if canonical_workspace != trigger.workspace || common_dir != registration.repo_common_dir {
        return Err(Error::new(
            "not_allowed",
            "project check workspace Git identity changed",
        ));
    }
    let snapshot = capture_workspace_snapshot(&trigger.workspace).await?;
    let fingerprint = work_fingerprint_from_snapshot(&snapshot)?;
    if fingerprint != trigger.check.fingerprint {
        return Err(Error::new(
            "stale_generation",
            "project check fingerprint no longer matches its worktree",
        ));
    }
    let head_oid = snapshot
        .head
        .clone()
        .filter(|head| is_git_oid(head))
        .ok_or_else(|| Error::new("unavailable", "project check worktree has no exact HEAD"))?;
    let branch = snapshot
        .branch
        .clone()
        .filter(|branch| !branch.is_empty())
        .ok_or_else(|| Error::new("not_allowed", "project check worktree is detached"))?;
    if !is_git_oid(&workspace.base_oid) {
        return Err(Error::new(
            "not_allowed",
            "managed project worktree has no exact base OID",
        ));
    }
    verify_base_ancestor(&trigger.workspace, &workspace.base_oid, &head_oid).await?;
    if branch != workspace.requested_branch {
        return Err(Error::new(
            "not_allowed",
            "project check branch differs from its managed worktree",
        ));
    }
    reject_unmerged_paths(&snapshot)?;

    let mut endpoint = None;
    let mut github_cli = None;
    let mut credential_helper = None;
    if let Some(remote) = trigger.remote.as_deref() {
        let git = registration
            .git
            .as_ref()
            .ok_or_else(|| Error::new("not_allowed", "project has no publication approval"))?;
        let publication = git
            .publication
            .as_ref()
            .ok_or_else(|| Error::new("not_allowed", "project has no publication approval"))?;
        let approved_endpoint = publication
            .remotes
            .get(remote)
            .ok_or_else(|| Error::new("not_allowed", "publication remote is not approved"))?;
        git_actions::validate_approved_endpoint(approved_endpoint)?;
        endpoint = Some(approved_endpoint.clone());
        if trigger.create_pr {
            let gh = publication.github_cli.as_ref().ok_or_else(|| {
                Error::new("not_allowed", "GitHub CLI integration is not configured")
            })?;
            verify_github_integration(gh, approved_endpoint)?;
            github_cli = Some(gh.clone());
        }
        if git.credential_helper == Some(eliot_core::api::GitCredentialHelper::Manager) {
            credential_helper = Some(
                git_actions::resolve_publication_helper(
                    &registration.checkout,
                    &registration.repo_common_dir,
                )
                .await?,
            );
        }
    }

    let frozen = FrozenGitPublish {
        repository: registration.id.clone(),
        registration_revision: registration.revision,
        canonical_checkout: registration.checkout.clone(),
        common_dir: registration.repo_common_dir.clone(),
        workspace: trigger.workspace,
        agent: agent.id.clone(),
        generation: agent.generation,
        source_event_seq: trigger.source_event_seq,
        check: trigger.check,
        head_oid,
        base_oid: workspace.base_oid.clone(),
        branch,
        remote: trigger.remote,
        endpoint,
        create_pr: trigger.create_pr,
        github_cli,
        credential_helper,
        timeout_ms: trigger.timeout_ms,
    };
    frozen.validate()?;
    Ok(frozen)
}

/// Execute local commit and optional owner-approved publication. The Router
/// settles this result durably before acknowledging or cleaning a transport view.
pub(crate) async fn execute(context: GitPublishExecutionContext<'_>) -> GitPublishExecution {
    let GitPublishExecutionContext {
        router,
        data_root,
        command_id,
        frozen,
        registration,
        agent,
        workspace,
        cancellation,
        secret_policy,
    } = context;
    if frozen.validate().is_err()
        || verify_frozen_registration(frozen, registration).is_err()
        || verify_managed_workspace(registration, agent, workspace, &trigger_from_frozen(frozen))
            .is_err()
        || workspace.base_oid != frozen.base_oid
        || router.verify_git_publish_state(frozen).await.is_err()
        || automation::verify_project_registration(data_root, registration)
            .await
            .is_err()
    {
        return execution(
            CommandState::Rejected,
            "native_not_dispatched",
            json!({}),
            None,
        );
    }
    if verify_github_snapshot(frozen).is_err() {
        return execution(
            CommandState::Rejected,
            "native_not_dispatched",
            json!({}),
            None,
        );
    }
    let snapshot = match capture_workspace_snapshot(&frozen.workspace).await {
        Ok(snapshot) => snapshot,
        Err(_) => {
            return execution(
                CommandState::Rejected,
                "git_publish_snapshot_unavailable",
                json!({}),
                None,
            );
        }
    };
    if snapshot_matches_frozen(&snapshot, frozen).is_err() {
        return execution(
            CommandState::Rejected,
            "git_publish_snapshot_changed",
            json!({}),
            None,
        );
    }
    if verify_base_ancestor(&frozen.workspace, &frozen.base_oid, &frozen.head_oid)
        .await
        .is_err()
    {
        return execution(
            CommandState::Rejected,
            "git_publish_branch_not_based_on_managed_base",
            json!({"code":"git_publish_branch_not_based_on_managed_base"}),
            None,
        );
    }
    if snapshot.dirty.is_empty() && (frozen.remote.is_none() || frozen.head_oid == frozen.base_oid)
    {
        return execution(
            CommandState::Applied,
            "git_publish_no_changes",
            json!({"code":"git_publish_no_changes"}),
            None,
        );
    }

    let git = match GitProbe::new() {
        Ok(git) => git,
        Err(_) => {
            return execution(
                CommandState::Rejected,
                "native_not_dispatched",
                json!({}),
                None,
            );
        }
    };
    let root = Path::new(&frozen.workspace);
    let timeout = Duration::from_millis(frozen.timeout_ms as u64);
    let existing_commit = snapshot.dirty.is_empty();
    let existing_commit_subject = if existing_commit {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return execution(
                CommandState::Rejected,
                "git_publish_timeout_unrepresentable",
                json!({"code":"git_publish_timeout_unrepresentable"}),
                None,
            );
        };
        match read_git_line(&git, root, &["log", "-1", "--format=%s"], deadline).await {
            Ok(subject) if eliot_core::api::validate_git_publish_subject(&subject).is_ok() => {
                Some(subject)
            }
            Ok(_) => {
                return execution(
                    CommandState::Rejected,
                    "git_publish_existing_commit_subject_invalid",
                    json!({"code":"git_publish_existing_commit_subject_invalid"}),
                    None,
                );
            }
            Err(_) => {
                return execution(
                    CommandState::Rejected,
                    "git_publish_existing_commit_readback_failed",
                    json!({"code":"git_publish_existing_commit_readback_failed"}),
                    None,
                );
            }
        }
    } else {
        None
    };
    let checked_contents = if existing_commit {
        None
    } else {
        match changed_contents(root, &snapshot) {
            Ok(contents)
                if git
                    .work_fingerprint(&frozen.workspace)
                    .await
                    .ok()
                    .as_deref()
                    == Some(frozen.check.fingerprint.as_str()) =>
            {
                Some(contents)
            }
            _ => {
                return execution(
                    CommandState::Rejected,
                    "git_publish_snapshot_changed",
                    json!({}),
                    None,
                );
            }
        }
    };
    let private_index = if existing_commit {
        None
    } else {
        match TemporaryGitIndex::create(data_root) {
            Ok(index) => Some(index),
            Err(_) => {
                return execution(
                    CommandState::Rejected,
                    "native_not_dispatched",
                    json!({}),
                    None,
                );
            }
        }
    };
    let index_file = private_index.as_ref().map(TemporaryGitIndex::path);
    if let Some(index_file) = index_file {
        let read_tree_args = ["read-tree", frozen.head_oid.as_str()];
        let initialized = match git
            .workspace_effect_command_with_index(
                root,
                &read_tree_args,
                index_file,
                timeout,
                Arc::clone(&secret_policy),
                cancellation.clone(),
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(_) => {
                return execution(
                    CommandState::Rejected,
                    "native_not_dispatched",
                    json!({}),
                    None,
                );
            }
        };
        match effect_exit(&initialized) {
            Some(0) if effect_quiescent(&initialized) => {}
            _ if effect_failed_definitely(&initialized) => {
                return execution(
                    CommandState::Failed,
                    "git_publish_stage_failed",
                    json!({"code":"git_publish_stage_failed"}),
                    None,
                );
            }
            _ => {
                return execution(
                    CommandState::Unknown,
                    "git_publish_stage_outcome_unknown",
                    json!({"code":"git_publish_stage_outcome_unknown"}),
                    None,
                );
            }
        }
    }
    let stage_args = ["add", "--all"];
    let staged = if let Some(index_file) = index_file {
        Some(
            match git
                .workspace_effect_command_with_index(
                    root,
                    &stage_args,
                    index_file,
                    timeout,
                    Arc::clone(&secret_policy),
                    cancellation.clone(),
                )
                .await
            {
                Ok(outcome) => outcome,
                Err(_) => {
                    return execution(
                        CommandState::Rejected,
                        "native_not_dispatched",
                        json!({}),
                        None,
                    );
                }
            },
        )
    } else {
        None
    };
    if let Some(staged) = &staged {
        match effect_exit(staged) {
            Some(0) if effect_quiescent(staged) => {}
            _ if effect_failed_definitely(staged) => {
                return execution(
                    CommandState::Failed,
                    "git_publish_stage_failed",
                    json!({"code":"git_publish_stage_failed"}),
                    None,
                );
            }
            _ => {
                return execution(
                    CommandState::Unknown,
                    "git_publish_stage_outcome_unknown",
                    json!({"code":"git_publish_stage_outcome_unknown"}),
                    None,
                );
            }
        }
    }

    let diff_args = ["diff", "--cached", "--quiet"];
    let staged_diff = if let Some(index_file) = index_file {
        Some(
            match git
                .workspace_effect_command_with_index(
                    root,
                    &diff_args,
                    index_file,
                    timeout,
                    Arc::clone(&secret_policy),
                    cancellation.clone(),
                )
                .await
            {
                Ok(outcome) => outcome,
                Err(_) => {
                    return execution(
                        CommandState::Rejected,
                        "native_not_dispatched",
                        json!({}),
                        None,
                    );
                }
            },
        )
    } else {
        None
    };
    if let Some(staged_diff) = &staged_diff {
        match effect_exit(staged_diff) {
            Some(0) if effect_quiescent(staged_diff) => {
                return execution(
                    CommandState::Applied,
                    "git_publish_no_changes",
                    json!({"code":"git_publish_no_changes"}),
                    None,
                );
            }
            Some(1) if effect_quiescent(staged_diff) => {}
            _ if effect_failed_definitely(staged_diff) => {
                return execution(
                    CommandState::Failed,
                    "git_publish_index_read_failed",
                    json!({"code":"git_publish_index_read_failed"}),
                    None,
                );
            }
            _ => {
                return execution(
                    CommandState::Unknown,
                    "git_publish_index_read_unknown",
                    json!({"code":"git_publish_index_read_unknown"}),
                    None,
                );
            }
        }
    }

    if !matches!(
        read_branch_and_head(&git, root, timeout).await,
        Ok((branch, head)) if branch == frozen.branch && head == frozen.head_oid
    ) {
        return execution(
            CommandState::Failed,
            "git_publish_head_changed_before_commit",
            json!({"code":"git_publish_head_changed_before_commit"}),
            None,
        );
    }
    if router.verify_git_publish_state(frozen).await.is_err() {
        return execution(
            CommandState::Failed,
            "git_publish_state_changed_before_commit",
            json!({"code":"git_publish_state_changed_before_commit"}),
            None,
        );
    }
    if *cancellation.borrow() {
        return execution(
            CommandState::Failed,
            "git_publish_cancelled_before_commit",
            json!({"code":"git_publish_cancelled_before_commit"}),
            None,
        );
    }
    if router.verify_git_publish_state(frozen).await.is_err()
        || automation::verify_project_registration(data_root, registration)
            .await
            .is_err()
    {
        return execution(
            CommandState::Failed,
            "git_publish_authority_changed_before_commit",
            json!({"code":"git_publish_authority_changed_before_commit"}),
            None,
        );
    }
    if existing_commit {
        let commit_oid = frozen.head_oid.clone();
        let commit_subject = existing_commit_subject.unwrap_or_default();
        return publish_commit(PublishCommitContext {
            router,
            data_root,
            command_id,
            frozen,
            registration,
            git: &git,
            root,
            commit_oid,
            commit_subject,
            committed: false,
            index_file: None,
            cancellation,
            secret_policy,
        })
        .await;
    }

    let Some(contents) = checked_contents.as_ref() else {
        return execution(
            CommandState::Failed,
            "git_publish_contents_unavailable",
            json!({}),
            None,
        );
    };
    let Some(index_file) = index_file else {
        return execution(
            CommandState::Failed,
            "git_publish_temporary_index_missing",
            json!({}),
            None,
        );
    };
    let tree = match checked_commit_tree(&git, root, frozen, contents, index_file, timeout).await {
        Ok(tree) => tree,
        Err(_) => {
            return execution(
                CommandState::Failed,
                "git_publish_staged_tree_changed",
                json!({"code":"git_publish_staged_tree_changed"}),
                None,
            );
        }
    };
    let subject = frozen.check.subject.as_str();
    let commit_args = [
        "-c",
        "commit.gpgSign=false",
        "commit-tree",
        tree.as_str(),
        "-p",
        frozen.head_oid.as_str(),
        "--no-gpg-sign",
        "-m",
        subject,
    ];
    let committed = match git
        .workspace_effect_command_with_index(
            root,
            &commit_args,
            index_file,
            timeout,
            Arc::clone(&secret_policy),
            cancellation.clone(),
        )
        .await
    {
        Ok(outcome) => outcome,
        Err(_) => {
            return execution(
                CommandState::Rejected,
                "native_not_dispatched",
                json!({}),
                None,
            );
        }
    };
    let commit_object = match &committed {
        ProcessOutcome::Completed(capture) if capture.stdout_complete => {
            std::str::from_utf8(&capture.stdout).ok().map(str::trim)
        }
        _ => None,
    };
    let Some(commit_object) = commit_object.filter(|oid| is_git_oid(oid)) else {
        let state = if effect_failed_definitely(&committed) {
            CommandState::Failed
        } else {
            CommandState::Unknown
        };
        return execution(state, "git_publish_commit_object_unproven", json!({}), None);
    };
    if router.verify_git_publish_state(frozen).await.is_err()
        || automation::verify_project_registration(data_root, registration)
            .await
            .is_err()
        || *cancellation.borrow()
    {
        return execution(
            CommandState::Failed,
            "git_publish_authority_changed_before_ref",
            json!({}),
            None,
        );
    }
    let branch_ref = format!("refs/heads/{}", frozen.branch);
    let updated = match git
        .workspace_effect_command_with_index(
            root,
            &[
                "update-ref",
                "--no-deref",
                &branch_ref,
                commit_object,
                &frozen.head_oid,
            ],
            index_file,
            timeout,
            Arc::clone(&secret_policy),
            cancellation.clone(),
        )
        .await
    {
        Ok(outcome) if effect_quiescent(&outcome) => outcome,
        _ => {
            return execution(
                CommandState::Unknown,
                "git_publish_commit_ref_unknown",
                json!({}),
                None,
            );
        }
    };
    let commit_oid = match verify_commit_readback(&git, root, frozen, index_file, timeout).await {
        Ok(Some(oid)) if oid == commit_object => oid,
        Ok(None) if effect_failed_definitely(&updated) => {
            return execution(
                CommandState::Failed,
                "git_publish_commit_failed",
                json!({"code":"git_publish_commit_failed"}),
                None,
            );
        }
        _ => {
            return execution(
                CommandState::Unknown,
                "git_publish_commit_readback_unknown",
                json!({"code":"git_publish_commit_outcome_unknown"}),
                None,
            );
        }
    };

    publish_commit(PublishCommitContext {
        router,
        data_root,
        command_id,
        frozen,
        registration,
        git: &git,
        root,
        commit_oid,
        commit_subject: frozen.check.subject.clone(),
        committed: true,
        index_file: Some(index_file),
        cancellation,
        secret_policy,
    })
    .await
}

async fn publish_commit(context: PublishCommitContext<'_>) -> GitPublishExecution {
    let PublishCommitContext {
        router,
        data_root,
        command_id,
        frozen,
        registration,
        git,
        root,
        commit_oid,
        commit_subject,
        committed,
        index_file,
        cancellation,
        secret_policy,
    } = context;
    let timeout = Duration::from_millis(frozen.timeout_ms as u64);
    if router.verify_git_publish_state(frozen).await.is_err()
        || verify_committed_workspace(git, root, &frozen.branch, &commit_oid, timeout, index_file)
            .await
            .is_err()
        || automation::verify_project_registration(data_root, registration)
            .await
            .is_err()
    {
        return execution(
            CommandState::Failed,
            "git_publish_state_changed_after_commit",
            json!({"code":"git_publish_state_changed_after_commit","commit_oid":commit_oid}),
            None,
        );
    }

    let mut remote_ref = None;
    let mut cleanup_view = None;
    if frozen.remote.is_some() {
        let pushed = git_actions::push_project_publish(
            data_root,
            command_id,
            frozen,
            registration,
            &commit_oid,
            cancellation.clone(),
        )
        .await;
        if pushed.state != CommandState::Applied {
            let state = if pushed.state == CommandState::Unknown {
                CommandState::Unknown
            } else {
                CommandState::Failed
            };
            return execution(
                state,
                pushed.code,
                json!({"code":pushed.code,"commit_oid":commit_oid}),
                pushed.cleanup_view,
            );
        }
        remote_ref = pushed.remote_ref;
        cleanup_view = pushed.cleanup_view;
    }

    let mut pull_request_url = None;
    if frozen.create_pr {
        let Some(gh) = frozen.github_cli.as_ref() else {
            return execution(
                CommandState::Failed,
                "git_publish_pr_integration_missing",
                json!({"code":"git_publish_pr_integration_missing","commit_oid":commit_oid}),
                cleanup_view,
            );
        };
        if *cancellation.borrow() {
            return execution(
                CommandState::Failed,
                "git_publish_cancelled_before_pr",
                json!({"code":"git_publish_cancelled_before_pr","commit_oid":commit_oid,"remote_ref":remote_ref}),
                cleanup_view,
            );
        }
        if router.verify_git_publish_state(frozen).await.is_err()
            || verify_committed_workspace(
                git,
                root,
                &frozen.branch,
                &commit_oid,
                timeout,
                index_file,
            )
            .await
            .is_err()
            || automation::verify_project_registration(data_root, registration)
                .await
                .is_err()
            || verify_github_snapshot(frozen).is_err()
        {
            return execution(
                CommandState::Failed,
                "git_publish_pr_authority_changed_before_create",
                json!({"code":"git_publish_pr_authority_changed_before_create","commit_oid":commit_oid,"remote_ref":remote_ref}),
                cleanup_view,
            );
        }
        match lookup_pull_request(
            gh,
            &frozen.branch,
            &frozen.workspace,
            &secret_policy,
            timeout,
            cancellation.clone(),
        )
        .await
        {
            Ok(Some(url)) => pull_request_url = Some(url),
            Ok(None) => {
                let created = match create_pull_request(
                    gh,
                    &frozen.branch,
                    &frozen.workspace,
                    &frozen.check.subject,
                    &secret_policy,
                    timeout,
                    cancellation.clone(),
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        return execution(
                            CommandState::Failed,
                            "git_publish_pr_spawn_failed",
                            json!({"code":"git_publish_pr_spawn_failed","commit_oid":commit_oid,"remote_ref":remote_ref}),
                            cleanup_view,
                        );
                    }
                };
                if !effect_quiescent(&created) {
                    return execution(
                        CommandState::Unknown,
                        "git_publish_pr_quiescence_unknown",
                        json!({"code":"git_publish_pr_outcome_unknown","commit_oid":commit_oid,"remote_ref":remote_ref}),
                        None,
                    );
                }
                if automation::verify_project_registration(data_root, registration)
                    .await
                    .is_err()
                    || verify_github_snapshot(frozen).is_err()
                {
                    return execution(
                        CommandState::Unknown,
                        "git_publish_pr_authority_readback_unknown",
                        json!({"code":"git_publish_pr_outcome_unknown","commit_oid":commit_oid,"remote_ref":remote_ref}),
                        None,
                    );
                }
                match lookup_pull_request(
                    gh,
                    &frozen.branch,
                    &frozen.workspace,
                    &secret_policy,
                    timeout,
                    cancellation,
                )
                .await
                {
                    Ok(Some(url)) => pull_request_url = Some(url),
                    _ => {
                        return execution(
                            CommandState::Unknown,
                            "git_publish_pr_readback_unknown",
                            json!({"code":"git_publish_pr_outcome_unknown","commit_oid":commit_oid,"remote_ref":remote_ref}),
                            None,
                        );
                    }
                }
            }
            Err(_) => {
                return execution(
                    CommandState::Failed,
                    "git_publish_pr_lookup_failed",
                    json!({"code":"git_publish_pr_lookup_failed","commit_oid":commit_oid,"remote_ref":remote_ref}),
                    cleanup_view,
                );
            }
        }
    }

    let outcome = GitPublishOutcome {
        commit_oid,
        commit_subject,
        committed,
        remote_ref,
        pull_request_url,
    };
    if outcome.validate_for(frozen).is_err() {
        return execution(
            CommandState::Unknown,
            "git_publish_result_binding_invalid",
            json!({"code":"git_publish_result_binding_invalid"}),
            None,
        );
    }
    execution(
        CommandState::Applied,
        "git_publish_applied",
        serde_json::to_value(outcome)
            .unwrap_or_else(|_| json!({"code":"git_publish_result_encode_failed"})),
        cleanup_view,
    )
}

fn execution(
    state: CommandState,
    code: &str,
    mut outcome: Value,
    cleanup_view: Option<PathBuf>,
) -> GitPublishExecution {
    if !outcome.is_object() {
        outcome = json!({});
    }
    if let Some(object) = outcome.as_object_mut() {
        object
            .entry("code")
            .or_insert_with(|| Value::String(code.to_owned()));
    }
    GitPublishExecution {
        state,
        outcome,
        cleanup_view: if state == CommandState::Unknown {
            None
        } else {
            cleanup_view
        },
    }
}

fn verify_managed_workspace(
    registration: &ProjectRegistrationView,
    agent: &Agent,
    workspace: &WorkspacePreparation,
    trigger: &GitPublishTrigger,
) -> Result<()> {
    if agent.id != trigger.agent
        || agent.generation != trigger.generation
        || agent.active_turn.is_some()
        || agent.workspace.as_deref() != Some(trigger.workspace.as_str())
        || workspace.agent != agent.id
        || workspace.target_generation != agent.generation
        || workspace.phase != eliot_core::WorkspacePhase::Ready
        || workspace.worktree_path != trigger.workspace
        || workspace.worktree_path == registration.checkout
        || workspace.project != registration.checkout
        || workspace.repo_path != registration.checkout
        || workspace.repo_common_dir != registration.repo_common_dir
    {
        return Err(Error::new(
            "not_allowed",
            "git.publish requires the exact ready managed worktree generation",
        ));
    }
    Ok(())
}

fn verify_frozen_registration(
    frozen: &FrozenGitPublish,
    registration: &ProjectRegistrationView,
) -> Result<()> {
    registration.validate()?;
    if registration.id != frozen.repository
        || registration.revision != frozen.registration_revision
        || registration.checkout != frozen.canonical_checkout
        || registration.repo_common_dir != frozen.common_dir
    {
        return Err(Error::new(
            "not_allowed",
            "git.publish registration snapshot is stale",
        ));
    }
    let trigger = trigger_from_frozen(frozen);
    verify_git_approval(registration, &trigger, frozen)?;
    Ok(())
}

fn verify_git_approval(
    registration: &ProjectRegistrationView,
    trigger: &GitPublishTrigger,
    frozen: &FrozenGitPublish,
) -> Result<()> {
    let approved = registration
        .git
        .as_ref()
        .and_then(|git| git.publication.as_ref());
    match (&frozen.remote, &frozen.endpoint) {
        (Some(remote), Some(endpoint))
            if approved.and_then(|approved| approved.remotes.get(remote)) == Some(endpoint) => {}
        (None, None) if !frozen.create_pr => {}
        _ => {
            return Err(Error::new(
                "not_allowed",
                "git.publish remote is no longer approved",
            ));
        }
    }
    if frozen.create_pr
        && approved.and_then(|approved| approved.github_cli.as_ref()) != frozen.github_cli.as_ref()
    {
        return Err(Error::new(
            "not_allowed",
            "git.publish GitHub CLI integration is no longer approved",
        ));
    }
    let helper_approved = frozen.remote.is_some()
        && registration
            .git
            .as_ref()
            .and_then(|git| git.credential_helper)
            == Some(eliot_core::api::GitCredentialHelper::Manager);
    if helper_approved != frozen.credential_helper.is_some() {
        return Err(Error::new(
            "not_allowed",
            "git.publish credential helper approval changed",
        ));
    }
    if trigger.check != frozen.check
        || trigger.repository != frozen.repository
        || trigger.agent != frozen.agent
        || trigger.generation != frozen.generation
        || trigger.workspace != frozen.workspace
        || trigger.source_event_seq != frozen.source_event_seq
    {
        return Err(Error::new(
            "not_allowed",
            "git.publish check binding changed",
        ));
    }
    Ok(())
}

fn trigger_from_frozen(frozen: &FrozenGitPublish) -> GitPublishTrigger {
    GitPublishTrigger {
        repository: frozen.repository.clone(),
        remote: frozen.remote.clone(),
        create_pr: frozen.create_pr,
        timeout_ms: frozen.timeout_ms,
        source_event_seq: frozen.source_event_seq,
        check: frozen.check.clone(),
        agent: frozen.agent.clone(),
        generation: frozen.generation,
        workspace: frozen.workspace.clone(),
    }
}

async fn capture_workspace_snapshot(workspace: &str) -> Result<GitSnapshot> {
    let snapshot = GitProbe::new()?
        .probe(workspace, None, None, &[], now_ms())
        .await?;
    if !snapshot.complete {
        return Err(Error::new(
            "unavailable",
            "Git cannot provide a complete publication snapshot",
        ));
    }
    Ok(snapshot)
}

async fn verify_base_ancestor(workspace: &str, base_oid: &str, head_oid: &str) -> Result<()> {
    if !is_git_oid(base_oid) || !is_git_oid(head_oid) {
        return Err(Error::new(
            "invalid",
            "publication base or HEAD is not an exact OID",
        ));
    }
    let root = Path::new(workspace);
    let git = GitProbe::new()?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let result = git
        .workspace_command(
            root,
            &["merge-base", "--is-ancestor", base_oid, head_oid],
            deadline,
        )
        .await?;
    match result.code {
        0 => Ok(()),
        1 => Err(Error::new(
            "not_allowed",
            "managed publication branch no longer descends from its frozen base",
        )),
        _ => Err(Error::new(
            "unavailable",
            "Git could not verify the managed publication base",
        )),
    }
}

fn snapshot_matches_frozen(snapshot: &GitSnapshot, frozen: &FrozenGitPublish) -> Result<()> {
    let fingerprint = work_fingerprint_from_snapshot(snapshot)?;
    if fingerprint != frozen.check.fingerprint
        || snapshot.head.as_deref() != Some(frozen.head_oid.as_str())
        || snapshot.branch.as_deref() != Some(frozen.branch.as_str())
    {
        return Err(Error::new(
            "stale_generation",
            "managed worktree differs from the passed project check snapshot",
        ));
    }
    reject_unmerged_paths(snapshot)
}

fn reject_unmerged_paths(snapshot: &GitSnapshot) -> Result<()> {
    if snapshot
        .dirty
        .iter()
        .any(|path| path.status.chars().take(2).any(|state| state == 'U'))
    {
        return Err(Error::new(
            "bad_state",
            "git.publish does not commit an unmerged worktree",
        ));
    }
    Ok(())
}

fn verify_github_integration(gh: &ProjectGitHubCliView, endpoint: &str) -> Result<()> {
    gh.validate()?;
    let program = Path::new(&gh.program);
    let config_dir = Path::new(&gh.config_dir);
    for (path, is_dir) in [(program, false), (config_dir, true)] {
        let metadata = fs::symlink_metadata(path)
            .map_err(|_| Error::new("unavailable", "configured GitHub CLI path is unavailable"))?;
        if metadata.file_type().is_symlink() || crate::platform::is_reparse_point(&metadata) {
            return Err(Error::new(
                "not_allowed",
                "configured GitHub CLI path is a link or reparse point",
            ));
        }
        if metadata.is_dir() != is_dir || (!is_dir && !metadata.is_file()) {
            return Err(Error::new(
                "not_allowed",
                "configured GitHub CLI path has the wrong file type",
            ));
        }
    }
    let canonical_program = fs::canonicalize(program)
        .map_err(|_| Error::new("unavailable", "configured GitHub CLI is unavailable"))?;
    let canonical_config = fs::canonicalize(config_dir).map_err(|_| {
        Error::new(
            "unavailable",
            "configured GitHub auth directory is unavailable",
        )
    })?;
    if !canonical_program.is_absolute() || !canonical_config.is_absolute() {
        return Err(Error::new(
            "not_allowed",
            "configured GitHub CLI paths are not canonical absolute paths",
        ));
    }
    if !endpoint_matches_github(endpoint, gh)? {
        return Err(Error::new(
            "not_allowed",
            "publication remote does not match its Owner-selected GitHub repository",
        ));
    }
    Ok(())
}

fn verify_github_snapshot(frozen: &FrozenGitPublish) -> Result<()> {
    if let Some(gh) = &frozen.github_cli {
        let endpoint = frozen
            .endpoint
            .as_deref()
            .ok_or_else(|| Error::new("not_allowed", "GitHub publication has no remote"))?;
        verify_github_integration(gh, endpoint)?;
    }
    Ok(())
}

fn endpoint_matches_github(endpoint: &str, gh: &ProjectGitHubCliView) -> Result<bool> {
    let url = reqwest::Url::parse(endpoint).map_err(|_| {
        Error::new(
            "not_allowed",
            "GitHub publication remote must be a direct URL",
        )
    })?;
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url
            .host_str()
            .is_some_and(|host| host.eq_ignore_ascii_case(&gh.host))
    {
        return Ok(false);
    }
    let mut path = url.path().trim_matches('/').to_owned();
    if let Some(without_git) = path.strip_suffix(".git") {
        path = without_git.to_owned();
    }
    Ok(path == gh.repository)
}

fn changed_contents(root: &Path, snapshot: &GitSnapshot) -> Result<BTreeMap<String, String>> {
    let contents = scope_digests(
        root,
        snapshot.dirty.iter().flat_map(|entry| {
            std::iter::once(entry.path.as_str()).chain(entry.original_path.as_deref())
        }),
    )?;
    if contents.values().any(|digest| digest == "other") {
        return Err(Error::new(
            "unsupported",
            "publication contains a non-file change",
        ));
    }
    Ok(contents)
}

async fn checked_commit_tree(
    git: &GitProbe,
    root: &Path,
    frozen: &FrozenGitPublish,
    contents: &BTreeMap<String, String>,
    index_file: &Path,
    timeout: Duration,
) -> Result<String> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| Error::new("invalid", "git.publish timeout is out of range"))?;
    let tree = read_git_line_with_index(git, root, &["write-tree"], index_file, deadline).await?;
    if !is_git_oid(&tree) {
        return Err(Error::new("source_integrity", "Git staged tree is invalid"));
    }
    let snapshot = capture_workspace_snapshot(&frozen.workspace).await?;
    let diff = git
        .workspace_command_with_index(
            root,
            &["diff", "--quiet", &tree, "--"],
            index_file,
            deadline,
        )
        .await?;
    if diff.code != 0
        || snapshot.head.as_deref() != Some(frozen.head_oid.as_str())
        || snapshot.branch.as_deref() != Some(frozen.branch.as_str())
        || changed_contents(root, &snapshot)? != *contents
    {
        return Err(Error::new(
            "stale_generation",
            "Git staged tree differs from the checked worktree",
        ));
    }
    Ok(tree)
}

async fn read_branch_and_head(
    git: &GitProbe,
    root: &Path,
    timeout: Duration,
) -> Result<(String, String)> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| Error::new("invalid", "git.publish timeout is out of range"))?;
    let branch = read_git_line(
        git,
        root,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
        deadline,
    )
    .await?;
    let head = read_git_line(git, root, &["rev-parse", "--verify", "HEAD"], deadline).await?;
    if !is_git_oid(&head) {
        return Err(Error::new(
            "source_integrity",
            "git.publish HEAD readback is invalid",
        ));
    }
    Ok((branch, head))
}

async fn verify_commit_readback(
    git: &GitProbe,
    root: &Path,
    frozen: &FrozenGitPublish,
    index_file: &Path,
    timeout: Duration,
) -> Result<Option<String>> {
    let (branch, head) = read_branch_and_head(git, root, timeout).await?;
    if head == frozen.head_oid {
        return Ok(None);
    }
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| Error::new("invalid", "git.publish timeout is out of range"))?;
    let parent = read_git_line(git, root, &["rev-parse", "--verify", "HEAD^"], deadline).await?;
    let subject = read_git_line(git, root, &["log", "-1", "--format=%s"], deadline).await?;
    let worktree_diff = git
        .workspace_command_with_index(
            root,
            &["diff", "--quiet", head.as_str(), "--"],
            index_file,
            deadline,
        )
        .await?;
    let untracked = git
        .workspace_command_with_index(
            root,
            &["ls-files", "--others", "--exclude-standard", "-z"],
            index_file,
            deadline,
        )
        .await?;
    if branch != frozen.branch
        || parent != frozen.head_oid
        || subject != frozen.check.subject
        || worktree_diff.code != 0
        || untracked.code != 0
        || !untracked.stdout.is_empty()
    {
        return Err(Error::new(
            "unknown",
            "git.publish commit readback does not prove the exact checked tree",
        ));
    }
    Ok(Some(head))
}

async fn verify_committed_workspace(
    git: &GitProbe,
    root: &Path,
    branch: &str,
    commit_oid: &str,
    timeout: Duration,
    index_file: Option<&Path>,
) -> Result<()> {
    let (actual_branch, actual_head) = read_branch_and_head(git, root, timeout).await?;
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| Error::new("invalid", "git.publish timeout is out of range"))?;
    let diff_args = ["diff", "--quiet", commit_oid, "--"];
    let untracked_args = ["ls-files", "--others", "--exclude-standard", "-z"];
    let (worktree_diff, untracked) = match index_file {
        Some(index_file) => (
            git.workspace_command_with_index(root, &diff_args, index_file, deadline)
                .await?,
            git.workspace_command_with_index(root, &untracked_args, index_file, deadline)
                .await?,
        ),
        None => (
            git.workspace_command(root, &diff_args, deadline).await?,
            git.workspace_command(root, &untracked_args, deadline)
                .await?,
        ),
    };
    if actual_branch != branch
        || actual_head != commit_oid
        || worktree_diff.code != 0
        || untracked.code != 0
        || !untracked.stdout.is_empty()
    {
        return Err(Error::new(
            "stale_generation",
            "managed worktree changed after its publication commit",
        ));
    }
    Ok(())
}

async fn read_git_line(
    git: &GitProbe,
    root: &Path,
    args: &[&str],
    deadline: Instant,
) -> Result<String> {
    let output = git.workspace_command(root, args, deadline).await?;
    parse_git_line(output.code, &output.stdout)
}

async fn read_git_line_with_index(
    git: &GitProbe,
    root: &Path,
    args: &[&str],
    index_file: &Path,
    deadline: Instant,
) -> Result<String> {
    let output = git
        .workspace_command_with_index(root, args, index_file, deadline)
        .await?;
    parse_git_line(output.code, &output.stdout)
}

fn parse_git_line(code: i32, stdout: &[u8]) -> Result<String> {
    if code != 0 {
        return Err(Error::new("unavailable", "Git readback failed"));
    }
    let text = std::str::from_utf8(stdout)
        .map_err(|_| Error::new("source_integrity", "Git readback is not UTF-8"))?;
    let line = text.trim_end_matches(['\r', '\n']);
    if line.is_empty()
        || line
            .chars()
            .any(|character| matches!(character, '\r' | '\n'))
    {
        return Err(Error::new("source_integrity", "Git readback is malformed"));
    }
    Ok(line.to_owned())
}

fn effect_exit(outcome: &ProcessOutcome) -> Option<i32> {
    match outcome {
        ProcessOutcome::Completed(capture) => capture.status.code(),
        ProcessOutcome::Failed {
            capture: Some(capture),
            ..
        } => capture.status.code(),
        ProcessOutcome::Failed { capture: None, .. }
        | ProcessOutcome::NotSpawned(_)
        | ProcessOutcome::ProgramUnrunnable(_)
        | ProcessOutcome::Unknown { .. } => None,
    }
}

fn effect_quiescent(outcome: &ProcessOutcome) -> bool {
    match outcome {
        ProcessOutcome::Completed(_)
        | ProcessOutcome::NotSpawned(_)
        | ProcessOutcome::ProgramUnrunnable(_) => true,
        ProcessOutcome::Failed {
            capture: Some(_), ..
        } => true,
        ProcessOutcome::Failed { capture: None, .. } => false,
        ProcessOutcome::Unknown { quiescent, .. } => *quiescent,
    }
}

fn effect_failed_definitely(outcome: &ProcessOutcome) -> bool {
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

async fn lookup_pull_request(
    gh: &ProjectGitHubCliView,
    branch: &str,
    workspace: &str,
    policy: &Arc<SecretPolicy>,
    timeout: Duration,
    cancellation: watch::Receiver<bool>,
) -> Result<Option<String>> {
    let repo = format!("{}/{}", gh.host, gh.repository);
    let args = [
        "pr",
        "list",
        "--repo",
        repo.as_str(),
        "--head",
        branch,
        "--base",
        gh.base_ref.as_str(),
        "--state",
        "open",
        "--json",
        "url,headRefName,baseRefName",
        "--limit",
        "2",
    ];
    let outcome = run_gh(gh, &args, workspace, policy, timeout, cancellation).await?;
    let capture = match outcome {
        ProcessOutcome::Completed(capture)
            if capture.status.success() && capture.stdout_complete =>
        {
            capture
        }
        _ => return Err(Error::new("unavailable", "GitHub PR readback failed")),
    };
    let rows: Vec<Value> = serde_json::from_slice(&capture.stdout)
        .map_err(|_| Error::new("source_integrity", "GitHub PR readback is malformed"))?;
    if rows.len() > 1 {
        return Err(Error::new(
            "bad_state",
            "multiple open PRs match the publication branch",
        ));
    }
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    if row.get("headRefName").and_then(Value::as_str) != Some(branch)
        || row.get("baseRefName").and_then(Value::as_str) != Some(gh.base_ref.as_str())
    {
        return Err(Error::new(
            "source_integrity",
            "GitHub PR readback binding differs",
        ));
    }
    let url = row
        .get("url")
        .and_then(Value::as_str)
        .filter(|url| url.starts_with("https://"))
        .ok_or_else(|| Error::new("source_integrity", "GitHub PR URL is invalid"))?;
    Ok(Some(url.to_owned()))
}

async fn create_pull_request(
    gh: &ProjectGitHubCliView,
    branch: &str,
    workspace: &str,
    subject: &str,
    policy: &Arc<SecretPolicy>,
    timeout: Duration,
    cancellation: watch::Receiver<bool>,
) -> Result<ProcessOutcome> {
    let repo = format!("{}/{}", gh.host, gh.repository);
    let args = [
        "pr",
        "create",
        "--repo",
        repo.as_str(),
        "--head",
        branch,
        "--base",
        gh.base_ref.as_str(),
        "--title",
        subject,
        "--body",
        "",
    ];
    let command = build_gh_command(gh, &args, workspace)?;
    Ok(run_owned_process(
        command,
        Vec::new(),
        timeout,
        Arc::clone(policy),
        cancellation,
    )
    .await)
}

async fn run_gh(
    gh: &ProjectGitHubCliView,
    args: &[&str],
    workspace: &str,
    policy: &Arc<SecretPolicy>,
    timeout: Duration,
    cancellation: watch::Receiver<bool>,
) -> Result<ProcessOutcome> {
    let command = build_gh_command(gh, args, workspace)?;
    Ok(run_owned_process(
        command,
        Vec::new(),
        timeout,
        Arc::clone(policy),
        cancellation,
    )
    .await)
}

fn build_gh_command(gh: &ProjectGitHubCliView, args: &[&str], workspace: &str) -> Result<Command> {
    let mut command = local_process_command(&gh.program)?;
    command
        .current_dir(workspace)
        .env("GH_CONFIG_DIR", &gh.config_dir)
        .env("GH_HOST", &gh.host)
        .env("GH_PROMPT_DISABLED", "1")
        .env("GH_NO_UPDATE_NOTIFIER", "1")
        .env("GH_PAGER", "")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .args(args);
    Ok(command)
}
