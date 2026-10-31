//! One frozen, project-scoped native Git fetch action.
//!
//! Store admits and dispatches the ordinary `git.fetch` command. This module
//! prepares an isolated transport view, runs only the frozen fetch, CAS-updates
//! the tracking ref, and returns exact readback or an explicit uncertain result.

use crate::{
    automation,
    git_monitor::{GitBinding, GitProbe, canonical_root, git_path_argument, is_oid, reject_links},
    platform::OwnedChild,
    workspace_git::WorkspaceGit,
};
use eliot_core::{
    CommandState, Error, FrozenSetupFile, Result,
    api::{
        FrozenGitFetch, GitCredentialHelper, GitFetchDefinitionArgs, GitFetchOutcome,
        ProjectRegistrationView,
    },
};
use serde_json::{Value, json};
use std::{
    env,
    ffi::OsString,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::watch,
    time::{Instant, timeout_at},
};
use ulid::Ulid;

const GIT_READ_TIME: Duration = Duration::from_secs(15);
const GIT_CLEANUP_TIME: Duration = Duration::from_secs(2);
const PRIVATE_REMOTE: &str = "eliot-approved";

pub(crate) struct GitFetchExecution {
    pub state: CommandState,
    pub outcome: Value,
    pub cleanup_view: Option<PathBuf>,
}

pub(crate) fn validate_approved_endpoint(endpoint: &str) -> Result<()> {
    if Path::new(endpoint).is_absolute() {
        if endpoint.chars().any(char::is_control) || endpoint.contains("::") {
            return Err(Error::new(
                "invalid",
                "absolute local Git endpoint contains an unsupported delimiter",
            ));
        }
        return Ok(());
    }
    endpoint_protocol(endpoint).map(|_| ())
}

struct RepositoryBinding {
    checkout: PathBuf,
    common_dir: PathBuf,
    git_dir: PathBuf,
}

struct ProcessResult {
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr_len: usize,
    #[cfg(test)]
    stderr: Vec<u8>,
    quiescent: bool,
    interrupted: bool,
}

#[derive(Clone)]
struct TransportEnvironment {
    global_config: PathBuf,
    home: PathBuf,
    xdg_config: PathBuf,
    hooks: PathBuf,
    template: PathBuf,
}

struct GitToolchain {
    program: PathBuf,
    exec_path: PathBuf,
}

struct GitProcessContext {
    toolchain: GitToolchain,
    environment: TransportEnvironment,
    helper: Option<FrozenSetupFile>,
    protocol: String,
    object_directory: Option<PathBuf>,
}

/// Resolve and retain the exact repository, remote observation, and old ref.
/// All Git reads and credential acquisition occur without a Store lock.
pub(crate) async fn prepare_fetch(
    data_root: &Path,
    registration: &ProjectRegistrationView,
    definition: &GitFetchDefinitionArgs,
) -> Result<FrozenGitFetch> {
    definition.validate()?;
    registration.validate()?;
    if definition.repository != registration.id {
        return Err(Error::new(
            "not_allowed",
            "git.fetch repository must match its saved registration",
        ));
    }
    validate_refs(
        &definition.remote,
        &definition.source_ref,
        &definition.tracking_ref,
    )?;
    let project_git = registration
        .git
        .as_ref()
        .ok_or_else(|| Error::new("not_allowed", "repository has no Git fetch approval"))?;
    let endpoint = project_git
        .fetch_remotes
        .get(&definition.remote)
        .ok_or_else(|| Error::new("not_allowed", "remote is not approved for Git fetch"))?
        .clone();
    endpoint_protocol(&endpoint)?;
    let timeout_ms = automation::git_fetch_timeout_ms(definition.timeout.as_deref())?;

    automation::verify_project_registration(data_root, registration).await?;
    let binding = repository_binding(&registration.checkout, &registration.repo_common_dir).await?;
    let git = GitProbe::new()?;
    verify_remote_configuration(&git, &binding, &definition.remote, &endpoint).await?;
    let helper = match project_git.credential_helper {
        Some(GitCredentialHelper::Manager) => Some(resolve_manager_helper(&git, &binding).await?),
        None => None,
    };
    let advertised_oid = advertise_ref(
        data_root,
        &git,
        &binding,
        &endpoint,
        definition,
        helper.as_ref(),
        timeout_ms,
    )
    .await?;
    automation::verify_project_registration(data_root, registration).await?;
    let current_binding =
        repository_binding(&registration.checkout, &registration.repo_common_dir).await?;
    verify_remote_configuration(&git, &current_binding, &definition.remote, &endpoint).await?;
    verify_helper_receipt(&git, &current_binding, helper.as_ref()).await?;
    if current_binding.checkout != binding.checkout
        || current_binding.common_dir != binding.common_dir
        || current_binding.git_dir != binding.git_dir
    {
        return Err(Error::new(
            "source_integrity",
            "Git repository identity changed during source advertisement",
        ));
    }
    let expected_old_oid =
        read_tracking_ref(&git, &current_binding, &definition.tracking_ref).await?;
    let frozen = FrozenGitFetch {
        repository: definition.repository.clone(),
        registration_revision: registration.revision,
        canonical_checkout: registration.checkout.clone(),
        common_dir: registration.repo_common_dir.clone(),
        remote: definition.remote.clone(),
        endpoint,
        source_ref: definition.source_ref.clone(),
        advertised_oid,
        tracking_ref: definition.tracking_ref.clone(),
        expected_old_oid,
        timeout_ms,
        credential_helper: helper,
    };
    frozen.validate()?;
    validate_refs(&frozen.remote, &frozen.source_ref, &frozen.tracking_ref)?;
    Ok(frozen)
}

/// Execute one Store-dispatched fetch. This never retries Git and never writes
/// the local branch or worktree. Only a direct tracking-ref readback is applied.
pub(crate) async fn execute(
    data_root: &Path,
    command_id: &str,
    frozen: &FrozenGitFetch,
    registration: &ProjectRegistrationView,
    cancellation: watch::Receiver<bool>,
) -> GitFetchExecution {
    let view = match private_view_path(data_root, command_id) {
        Ok(view) => view,
        Err(_) => return execution(CommandState::Rejected, "native_not_dispatched", None),
    };
    if let Err(error) = validate_frozen_registration(registration, frozen) {
        return execution(CommandState::Rejected, &error.code, Some(view));
    }
    if automation::verify_project_registration(data_root, registration)
        .await
        .is_err()
    {
        return execution(CommandState::Rejected, "native_not_dispatched", Some(view));
    }
    if frozen.validate().is_err() {
        return execution(CommandState::Rejected, "native_not_dispatched", Some(view));
    }
    if validate_refs(&frozen.remote, &frozen.source_ref, &frozen.tracking_ref).is_err() {
        return execution(CommandState::Rejected, "native_not_dispatched", Some(view));
    }
    let protocol = match endpoint_protocol(&frozen.endpoint) {
        Ok(protocol) => protocol,
        Err(error) => return execution(CommandState::Rejected, &error.code, Some(view)),
    };
    let repo = match repository_binding(&frozen.canonical_checkout, &frozen.common_dir).await {
        Ok(repo) => repo,
        Err(_) => return execution(CommandState::Rejected, "native_not_dispatched", Some(view)),
    };
    let git = match GitProbe::new() {
        Ok(git) => git,
        Err(_) => return execution(CommandState::Rejected, "native_not_dispatched", Some(view)),
    };
    if verify_helper_receipt(&git, &repo, frozen.credential_helper.as_ref())
        .await
        .is_err()
    {
        return execution(CommandState::Rejected, "native_not_dispatched", Some(view));
    }
    if verify_remote_configuration(&git, &repo, &frozen.remote, &frozen.endpoint)
        .await
        .is_err()
    {
        return execution(CommandState::Rejected, "native_not_dispatched", Some(view));
    }
    if automation::verify_project_registration(data_root, registration)
        .await
        .is_err()
    {
        return execution(CommandState::Rejected, "native_not_dispatched", Some(view));
    }

    if verify_tracking_ref_before_effect(&git, &repo, frozen)
        .await
        .is_err()
    {
        return execution(CommandState::Rejected, "native_not_dispatched", Some(view));
    }
    if let Err(error) = ensure_transport_view(
        data_root,
        command_id,
        &git,
        &repo,
        frozen,
        cancellation.clone(),
    )
    .await
    {
        let state = if matches!(error.code.as_str(), "unknown" | "bad_state") {
            CommandState::Unknown
        } else {
            CommandState::Rejected
        };
        let cleanup = (state != CommandState::Unknown).then_some(view);
        return execution(state, "git_fetch_private_view_preparation_failed", cleanup);
    }
    if verify_transport_view(data_root, command_id, frozen).is_err() {
        return execution(CommandState::Rejected, "native_not_dispatched", Some(view));
    }

    let environment = transport_environment(&view);
    let object_directory = match shared_object_directory(&repo.common_dir) {
        Ok(path) => path,
        Err(_) => return execution(CommandState::Rejected, "native_not_dispatched", Some(view)),
    };
    let process = match git_process_context(
        &git,
        &repo,
        &environment,
        frozen.credential_helper.as_ref(),
        protocol,
        Some(object_directory),
    )
    .await
    {
        Ok(process) => process,
        Err(_) => return execution(CommandState::Rejected, "native_not_dispatched", Some(view)),
    };
    let private_ref = private_fetch_ref(command_id);
    let effect_deadline = operation_deadline(frozen.timeout_ms);
    let fetch_args = vec![
        OsString::from("fetch"),
        OsString::from("--quiet"),
        OsString::from("--no-tags"),
        OsString::from("--no-write-fetch-head"),
        OsString::from("--no-recurse-submodules"),
        OsString::from(PRIVATE_REMOTE),
        OsString::from(format!("{}:{private_ref}", frozen.source_ref)),
    ];
    let fetch = run_transport_command(
        &process,
        &view,
        &fetch_args,
        effect_deadline,
        cancellation.clone(),
    )
    .await;
    let fetch = match fetch {
        Ok(result) => result,
        Err(_) => {
            return execution(
                CommandState::Unknown,
                "git_fetch_spawn_or_wait_unknown",
                None,
            );
        }
    };
    if !fetch.quiescent {
        return execution(
            CommandState::Unknown,
            "git_fetch_family_quiescence_unknown",
            None,
        );
    }
    if fetch.interrupted {
        return match read_tracking_ref(&git, &repo, &frozen.tracking_ref).await {
            Ok(current) if current == frozen.expected_old_oid => execution(
                CommandState::Failed,
                "git_fetch_interrupted_before_cas",
                Some(view),
            ),
            _ => execution(
                CommandState::Unknown,
                "git_fetch_interrupted_readback_unknown",
                None,
            ),
        };
    }

    let fetched_oid = match read_private_ref(&view, &private_ref) {
        Ok(oid) => oid,
        Err(_) => {
            return execution(
                CommandState::Unknown,
                "git_fetch_private_ref_readback_unknown",
                Some(view),
            );
        }
    };
    if fetch.exit_code != Some(0) || fetched_oid != frozen.advertised_oid {
        let current = read_tracking_ref(&git, &repo, &frozen.tracking_ref).await;
        return match current {
            Ok(current) if current == frozen.expected_old_oid => execution(
                CommandState::Failed,
                "git_fetch_source_moved_or_failed",
                Some(view),
            ),
            _ => execution(
                CommandState::Unknown,
                "git_fetch_target_after_failed_transfer_unknown",
                Some(view),
            ),
        };
    }
    let object_check = match verify_registered_object_graph(
        &process,
        &repo,
        &frozen.advertised_oid,
        effect_deadline,
        Some(cancellation.clone()),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            return match read_tracking_ref(&git, &repo, &frozen.tracking_ref).await {
                Ok(current) if current == frozen.expected_old_oid => execution(
                    CommandState::Failed,
                    "git_fetch_registered_object_verification_unavailable",
                    Some(view),
                ),
                _ => execution(
                    CommandState::Unknown,
                    "git_fetch_registered_object_verification_unknown",
                    None,
                ),
            };
        }
    };
    if !object_check.quiescent {
        return execution(
            CommandState::Unknown,
            "git_fetch_registered_object_verifier_quiescence_unknown",
            None,
        );
    }
    if object_check.interrupted || object_check.exit_code != Some(0) {
        let mut failed = execution(
            CommandState::Failed,
            "git_fetch_registered_object_readback_failed",
            Some(view),
        );
        failed.outcome["git_exit_code"] = object_check.exit_code.map_or(Value::Null, Value::from);
        failed.outcome["stderr_bytes"] = Value::from(object_check.stderr_len);
        #[cfg(test)]
        if protocol == "file"
            && frozen.credential_helper.is_none()
            && !object_check.stderr.is_empty()
        {
            failed.outcome["diagnostic"] =
                Value::String(String::from_utf8_lossy(&object_check.stderr).into_owned());
        }
        return match read_tracking_ref(&git, &repo, &frozen.tracking_ref).await {
            Ok(current) if current == frozen.expected_old_oid => failed,
            _ => execution(
                CommandState::Unknown,
                "git_fetch_registered_object_readback_target_unknown",
                None,
            ),
        };
    }

    if verify_transport_view(data_root, command_id, frozen).is_err() {
        return execution(
            CommandState::Unknown,
            "git_fetch_transport_config_changed",
            Some(view),
        );
    }
    if verify_remote_configuration(&git, &repo, &frozen.remote, &frozen.endpoint)
        .await
        .is_err()
    {
        return execution(
            CommandState::Unknown,
            "git_fetch_remote_config_changed",
            Some(view),
        );
    }
    if automation::verify_project_registration(data_root, registration)
        .await
        .is_err()
    {
        return execution(
            CommandState::Unknown,
            "git_fetch_registration_changed_after_transfer",
            Some(view),
        );
    }
    if verify_helper_receipt(&git, &repo, frozen.credential_helper.as_ref())
        .await
        .is_err()
    {
        return execution(
            CommandState::Unknown,
            "git_fetch_helper_changed_after_transfer",
            Some(view),
        );
    }
    match read_tracking_ref(&git, &repo, &frozen.tracking_ref).await {
        Ok(current) if current == frozen.expected_old_oid => {}
        Ok(_) => {
            return execution(
                CommandState::Failed,
                "git_fetch_tracking_ref_changed_before_cas",
                Some(view),
            );
        }
        Err(_) => {
            return execution(
                CommandState::Failed,
                "git_fetch_tracking_ref_unreadable_before_cas",
                Some(view),
            );
        }
    }
    if Instant::now() >= effect_deadline || *cancellation.borrow() {
        return match read_tracking_ref(&git, &repo, &frozen.tracking_ref).await {
            Ok(current) if current == frozen.expected_old_oid => execution(
                CommandState::Failed,
                "git_fetch_cancelled_before_cas",
                Some(view),
            ),
            _ => execution(
                CommandState::Unknown,
                "git_fetch_cancelled_before_cas_unknown",
                None,
            ),
        };
    }

    let update_input = update_ref_input(frozen, &frozen.advertised_oid);
    let update_args = vec![
        OsString::from("update-ref"),
        OsString::from("--no-deref"),
        OsString::from("--stdin"),
    ];
    let update = run_repository_command(
        &process,
        &repo.checkout,
        &update_args,
        update_input.as_bytes(),
        effect_deadline,
        cancellation,
    )
    .await;
    let update = match update {
        Ok(result) => result,
        Err(_) => {
            return execution(
                CommandState::Unknown,
                "git_fetch_cas_spawn_or_wait_unknown",
                None,
            );
        }
    };
    if !update.quiescent {
        return execution(
            CommandState::Unknown,
            "git_fetch_cas_family_quiescence_unknown",
            None,
        );
    }

    if verify_transport_view(data_root, command_id, frozen).is_err()
        || verify_remote_configuration(&git, &repo, &frozen.remote, &frozen.endpoint)
            .await
            .is_err()
        || automation::verify_project_registration(data_root, registration)
            .await
            .is_err()
        || verify_helper_receipt(&git, &repo, frozen.credential_helper.as_ref())
            .await
            .is_err()
    {
        return execution(
            CommandState::Unknown,
            "git_fetch_readback_config_or_authority_changed",
            Some(view),
        );
    }
    match read_tracking_ref(&git, &repo, &frozen.tracking_ref).await {
        Ok(Some(oid)) if oid == frozen.advertised_oid => {
            let outcome = GitFetchOutcome {
                tracking_ref: frozen.tracking_ref.clone(),
                tracking_oid: oid,
            };
            if outcome.validate_for(frozen).is_err() {
                execution(
                    CommandState::Unknown,
                    "git_fetch_result_binding_invalid",
                    Some(view),
                )
            } else {
                GitFetchExecution {
                    state: CommandState::Applied,
                    outcome: serde_json::to_value(outcome).unwrap_or_else(|_| json!({})),
                    cleanup_view: Some(view),
                }
            }
        }
        Ok(current)
            if current == frozen.expected_old_oid
                && (update.interrupted || update.exit_code.is_some_and(|code| code != 0)) =>
        {
            execution(CommandState::Failed, "git_fetch_cas_refused", Some(view))
        }
        _ => execution(
            CommandState::Unknown,
            "git_fetch_tracking_readback_not_proven",
            Some(view),
        ),
    }
}

pub(crate) fn cleanup_after_settlement(data_root: &Path, view: &Path) {
    let Ok(root) = canonical_root(data_root.to_str().unwrap_or_default()) else {
        return;
    };
    let Ok(base) = canonical_root(&root.join("git-fetch").to_string_lossy()) else {
        return;
    };
    if view.parent() != Some(base.as_path())
        || fs::symlink_metadata(view)
            .map(|metadata| crate::platform::is_reparse_point(&metadata))
            .unwrap_or(true)
        || ensure_no_reparse_tree(view).is_err()
    {
        return;
    }
    let _ = fs::remove_dir_all(view);
}

fn execution(state: CommandState, code: &str, cleanup_view: Option<PathBuf>) -> GitFetchExecution {
    GitFetchExecution {
        state,
        outcome: json!({"code":code}),
        cleanup_view: if state == CommandState::Unknown {
            None
        } else {
            cleanup_view
        },
    }
}

fn validate_frozen_registration(
    registration: &ProjectRegistrationView,
    frozen: &FrozenGitFetch,
) -> Result<()> {
    registration.validate()?;
    if registration.id != frozen.repository
        || registration.revision != frozen.registration_revision
        || registration.checkout != frozen.canonical_checkout
        || registration.repo_common_dir != frozen.common_dir
    {
        return Err(not_allowed("git.fetch registration snapshot is stale"));
    }
    let git = registration
        .git
        .as_ref()
        .ok_or_else(|| not_allowed("repository has no Git fetch approval"))?;
    if git.fetch_remotes.get(&frozen.remote) != Some(&frozen.endpoint) {
        return Err(not_allowed(
            "git.fetch endpoint differs from owner approval",
        ));
    }
    if (git.credential_helper == Some(GitCredentialHelper::Manager))
        != frozen.credential_helper.is_some()
    {
        return Err(not_allowed("git.fetch helper policy changed"));
    }
    Ok(())
}

fn validate_refs(remote: &str, source_ref: &str, tracking_ref: &str) -> Result<()> {
    if remote.is_empty()
        || !remote
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character))
        || remote.starts_with('-')
        || remote.starts_with('.')
        || remote.ends_with('.')
        || remote.contains("..")
    {
        return Err(Error::new("invalid", "git.fetch remote name is invalid"));
    }
    if !source_ref.starts_with("refs/") {
        return Err(Error::new(
            "invalid",
            "git.fetch source ref must be a full ref",
        ));
    }
    if tracking_ref
        .strip_prefix("refs/remotes/")
        .is_none_or(str::is_empty)
    {
        return Err(Error::new(
            "invalid",
            "git.fetch tracking ref must be below refs/remotes/",
        ));
    }
    Ok(())
}

fn endpoint_protocol(endpoint: &str) -> Result<&'static str> {
    let path = Path::new(endpoint);
    if path.is_absolute() {
        reject_links(path)?;
        let canonical = canonical_root(endpoint)?;
        if !canonical.is_dir() {
            return Err(Error::new(
                "invalid",
                "absolute local Git endpoint must be an existing repository directory",
            ));
        }
        return Ok("file");
    }
    let url = reqwest::Url::parse(endpoint)
        .map_err(|_| Error::new("invalid", "git.fetch endpoint is not a supported URL"))?;
    if url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.as_str() != endpoint
    {
        return Err(Error::new(
            "invalid",
            "git.fetch endpoint must be a canonical direct URL without credentials or query data",
        ));
    }
    match url.scheme() {
        "https" if url.host_str().is_some() => Ok("https"),
        "http" if url.host_str().is_some() => Ok("http"),
        "git" if url.host_str().is_some() => Ok("git"),
        "file"
            if url
                .host_str()
                .is_none_or(|host| host.is_empty() || host == "localhost") =>
        {
            Ok("file")
        }
        _ => Err(Error::new(
            "unsupported",
            "git.fetch supports only approved HTTPS and local file endpoints",
        )),
    }
}

fn native_endpoint_argument(endpoint: &str) -> Result<String> {
    if Path::new(endpoint).is_absolute() {
        reject_links(Path::new(endpoint))?;
        let canonical = canonical_root(endpoint)?;
        reject_links(&canonical)?;
        return git_path_argument(&canonical);
    }
    endpoint_protocol(endpoint)?;
    Ok(endpoint.to_owned())
}

async fn repository_binding(checkout: &str, common_dir: &str) -> Result<RepositoryBinding> {
    let workspace_git = WorkspaceGit::new()?;
    let (canonical_checkout, canonical_common) =
        workspace_git.repository_identity(checkout).await?;
    if canonical_checkout != checkout || canonical_common != common_dir {
        return Err(Error::new(
            "source_integrity",
            "git.fetch repository identity differs from its registration",
        ));
    }
    let checkout_path = canonical_root(checkout)?;
    let common_path = canonical_root(common_dir)?;
    let git = GitProbe::new()?;
    let deadline = Instant::now() + GIT_READ_TIME;
    let git_dir = read_git_line(
        &git,
        &checkout_path,
        &["rev-parse", "--absolute-git-dir"],
        deadline,
    )
    .await?;
    let git_dir = canonical_root(&git_dir)?;
    let common_readback = read_git_line(
        &git,
        &checkout_path,
        &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        deadline,
    )
    .await?;
    if canonical_root(&common_readback)? != common_path {
        return Err(Error::new(
            "source_integrity",
            "git.fetch common directory changed during repository binding",
        ));
    }
    Ok(RepositoryBinding {
        checkout: checkout_path,
        common_dir: common_path,
        git_dir,
    })
}

async fn read_git_line(
    git: &GitProbe,
    root: &Path,
    args: &[&str],
    deadline: Instant,
) -> Result<String> {
    let output = git.workspace_command(root, args, deadline).await?;
    if output.code != 0 {
        return Err(Error::new(
            "unavailable",
            "local Git identity readback failed",
        ));
    }
    one_line(&output.stdout)
}

fn one_line(bytes: &[u8]) -> Result<String> {
    let value = std::str::from_utf8(bytes)
        .map_err(|_| Error::new("source_integrity", "Git readback is not UTF-8"))?;
    let value = value
        .strip_suffix('\n')
        .ok_or_else(|| Error::new("source_integrity", "Git readback omitted its terminator"))?;
    if value.contains('\n') || value.contains('\r') || value.contains('\0') {
        return Err(Error::new(
            "source_integrity",
            "Git readback is not one record",
        ));
    }
    Ok(value.to_owned())
}

async fn read_config_values(
    git: &GitProbe,
    root: &Path,
    key: &str,
    deadline: Instant,
) -> Result<Vec<String>> {
    let args = ["config", "--local", "--null", "--get-all", key];
    let output = git.workspace_command(root, &args, deadline).await?;
    if output.code == 1 && output.stdout.is_empty() {
        return Ok(Vec::new());
    }
    if output.code != 0 {
        return Err(Error::new(
            "unavailable",
            "local Git remote config is unavailable",
        ));
    }
    nul_values(&output.stdout)
}

fn nul_values(bytes: &[u8]) -> Result<Vec<String>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if bytes.last() != Some(&0) {
        return Err(Error::new(
            "source_integrity",
            "Git config readback is truncated",
        ));
    }
    bytes[..bytes.len() - 1]
        .split(|byte| *byte == 0)
        .map(|record| {
            let value = std::str::from_utf8(record)
                .map_err(|_| Error::new("source_integrity", "Git config value is not UTF-8"))?;
            if value.chars().any(char::is_control) {
                return Err(Error::new("invalid", "Git remote config contains controls"));
            }
            Ok(value.to_owned())
        })
        .collect()
}

async fn verify_remote_configuration(
    git: &GitProbe,
    repo: &RepositoryBinding,
    remote: &str,
    approved_endpoint: &str,
) -> Result<()> {
    let fetch_key = format!("remote.{remote}.url");
    let push_key = format!("remote.{remote}.pushurl");
    let deadline = Instant::now() + GIT_READ_TIME;
    let fetch_urls = read_config_values(git, &repo.checkout, &fetch_key, deadline).await?;
    // Read pushurls independently; a push endpoint is never a fetch fallback.
    let _push_urls = read_config_values(git, &repo.checkout, &push_key, deadline).await?;
    if fetch_urls.len() != 1 || fetch_urls[0] != approved_endpoint {
        return Err(Error::new(
            "not_allowed",
            "raw fetch URL differs from its owner-approved direct endpoint",
        ));
    }
    let effective = git
        .workspace_command(
            &repo.checkout,
            &["remote", "get-url", "--all", remote],
            deadline,
        )
        .await?;
    if effective.code != 0 {
        return Err(Error::new(
            "unavailable",
            "effective fetch URL cannot be read",
        ));
    }
    let effective_urls = line_records(&effective.stdout)?;
    if effective_urls.len() != 1 || effective_urls[0] != approved_endpoint {
        return Err(Error::new(
            "not_allowed",
            "fetch URL rewrite is not the approved direct endpoint",
        ));
    }
    endpoint_protocol(approved_endpoint)?;
    Ok(())
}

fn line_records(bytes: &[u8]) -> Result<Vec<String>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| Error::new("source_integrity", "Git URL readback is not UTF-8"))?;
    let text = text
        .strip_suffix('\n')
        .ok_or_else(|| Error::new("source_integrity", "Git URL readback is truncated"))?;
    text.split('\n')
        .map(|line| {
            if line.is_empty() || line.contains('\r') || line.contains('\0') {
                return Err(Error::new(
                    "source_integrity",
                    "Git URL readback is malformed",
                ));
            }
            Ok(line.to_owned())
        })
        .collect()
}

async fn read_tracking_ref(
    git: &GitProbe,
    repo: &RepositoryBinding,
    tracking_ref: &str,
) -> Result<Option<String>> {
    let deadline = Instant::now() + GIT_READ_TIME;
    let symbolic = git
        .workspace_command(
            &repo.checkout,
            &["symbolic-ref", "--quiet", tracking_ref],
            deadline,
        )
        .await?;
    if symbolic.code == 0 {
        return Err(Error::new(
            "bad_state",
            "git.fetch tracking ref is symbolic",
        ));
    }
    if symbolic.code != 1 {
        return Err(Error::new(
            "unavailable",
            "tracking ref symbolic readback failed",
        ));
    }
    let output = git
        .workspace_command(
            &repo.checkout,
            &["rev-parse", "--verify", "--quiet", tracking_ref],
            deadline,
        )
        .await?;
    match output.code {
        0 => {
            let oid = one_line(&output.stdout)?;
            if !is_oid(&oid) {
                return Err(Error::new(
                    "source_integrity",
                    "tracking ref OID is malformed",
                ));
            }
            Ok(Some(oid))
        }
        1 if output.stdout.is_empty() => Ok(None),
        _ => Err(Error::new("unavailable", "tracking ref readback failed")),
    }
}

async fn verify_tracking_ref_before_effect(
    git: &GitProbe,
    repo: &RepositoryBinding,
    frozen: &FrozenGitFetch,
) -> Result<()> {
    let current = read_tracking_ref(git, repo, &frozen.tracking_ref).await?;
    if current != frozen.expected_old_oid {
        return Err(Error::new(
            "stale_generation",
            "tracking ref changed after git.fetch intent was frozen",
        ));
    }
    Ok(())
}

async fn resolve_manager_helper(
    git: &GitProbe,
    repo: &RepositoryBinding,
) -> Result<FrozenSetupFile> {
    let toolchain = git_toolchain(git, repo).await?;
    let helper_name = if cfg!(windows) {
        "git-credential-manager.exe"
    } else {
        "git-credential-manager"
    };
    let program_dir = toolchain
        .program
        .parent()
        .ok_or_else(|| unavailable("selected Git executable has no parent"))?;
    let exec_candidate = toolchain.exec_path.join(helper_name);
    let program_candidate = program_dir.join(helper_name);
    let resolved = if exec_candidate.is_file() {
        exec_candidate
    } else if program_candidate.is_file() {
        program_candidate
    } else {
        let path = env::var_os("PATH")
            .ok_or_else(|| Error::new("unsupported", "approved manager helper is unavailable"))?;
        let mut candidates = env::split_paths(&path)
            .filter(|directory| directory.is_absolute())
            .map(|directory| directory.join(helper_name))
            .filter(|path| path.is_file())
            .collect::<Vec<_>>();
        candidates.sort();
        candidates.dedup();
        if candidates.len() != 1 {
            return Err(Error::new(
                "unsupported",
                "approved manager helper is absent or ambiguous in the selected Git installation",
            ));
        }
        candidates.remove(0)
    };
    freeze_helper_file(&resolved)
}

async fn verify_helper_receipt(
    git: &GitProbe,
    repo: &RepositoryBinding,
    receipt: Option<&FrozenSetupFile>,
) -> Result<()> {
    let Some(receipt) = receipt else {
        return Ok(());
    };
    let current = resolve_manager_helper(git, repo).await?;
    if &current != receipt {
        return Err(Error::new(
            "source_integrity",
            "approved Git credential helper identity changed",
        ));
    }
    Ok(())
}

fn freeze_helper_file(path: &Path) -> Result<FrozenSetupFile> {
    let mut remaining = u64::MAX;
    crate::setup::capture_file(path, None, &mut remaining)
}

fn private_view_path(data_root: &Path, command_id: &str) -> Result<PathBuf> {
    Ulid::from_string(command_id)
        .map_err(|_| Error::new("invalid", "git.fetch command id is not a canonical ULID"))?;
    let root = canonical_root(
        data_root
            .to_str()
            .ok_or_else(|| Error::new("invalid", "data directory path is not UTF-8"))?,
    )?;
    Ok(root.join("git-fetch").join(command_id))
}

fn private_fetch_ref(command_id: &str) -> String {
    format!("refs/eliot-fetch/{command_id}")
}

fn update_ref_input(frozen: &FrozenGitFetch, new_oid: &str) -> String {
    match frozen.expected_old_oid.as_deref() {
        Some(old_oid) => format!("update {} {new_oid} {old_oid}\n", frozen.tracking_ref),
        None => format!("create {} {new_oid}\n", frozen.tracking_ref),
    }
}

fn operation_deadline(timeout_ms: i64) -> Instant {
    let millis = u64::try_from(timeout_ms).unwrap_or(1);
    Instant::now() + Duration::from_millis(millis.max(1))
}

async fn advertise_ref(
    data_root: &Path,
    git: &GitProbe,
    repo: &RepositoryBinding,
    endpoint: &str,
    definition: &GitFetchDefinitionArgs,
    helper: Option<&FrozenSetupFile>,
    timeout_ms: i64,
) -> Result<String> {
    let id = Ulid::new().to_string();
    let root = private_view_path(data_root, &id)?;
    create_private_environment_root(&root)?;
    let environment = create_transport_environment(&root)?;
    let protocol = endpoint_protocol(endpoint)?;
    let process = git_process_context(git, repo, &environment, helper, protocol, None).await?;
    let args = [
        OsString::from("ls-remote"),
        OsString::from("--exit-code"),
        OsString::from("--refs"),
        OsString::from(native_endpoint_argument(endpoint)?),
        OsString::from(&definition.source_ref),
    ];
    let result = run_owned_git(
        &process,
        &root,
        &args,
        &[],
        operation_deadline(timeout_ms),
        None,
    )
    .await?;
    if !result.quiescent {
        return Err(Error::new(
            "unknown",
            "Git advertisement process family did not settle",
        ));
    }
    if result.interrupted || result.exit_code != Some(0) {
        cleanup_after_settlement(data_root, &root);
        let message = format!(
            "approved Git source ref could not be advertised (exit {:?}, {} bounded stderr bytes)",
            result.exit_code, result.stderr_len
        );
        #[cfg(test)]
        let message = if protocol == "file" && helper.is_none() && !result.stderr.is_empty() {
            format!("{message}: {}", String::from_utf8_lossy(&result.stderr))
        } else {
            message
        };
        return Err(unavailable(&message));
    }
    let advertised = parse_advertisement(&result.stdout, &definition.source_ref);
    cleanup_after_settlement(data_root, &root);
    advertised
}

fn parse_advertisement(stdout: &[u8], source_ref: &str) -> Result<String> {
    let text = std::str::from_utf8(stdout)
        .map_err(|_| Error::new("source_integrity", "Git advertisement is not UTF-8"))?;
    let text = text
        .strip_suffix('\n')
        .ok_or_else(|| Error::new("source_integrity", "Git advertisement is truncated"))?;
    let (oid, reference) = text
        .split_once('\t')
        .ok_or_else(|| Error::new("source_integrity", "Git advertisement record is malformed"))?;
    if reference != source_ref || !is_oid(oid) || oid.len() != 40 && oid.len() != 64 {
        return Err(Error::new(
            "source_integrity",
            "Git advertisement did not contain the exact requested ref and OID",
        ));
    }
    Ok(oid.to_owned())
}

fn create_private_environment_root(root: &Path) -> Result<()> {
    let base = root
        .parent()
        .ok_or_else(|| Error::new("invalid", "Git private view has no parent"))?;
    ensure_private_root(base)?;
    if fs::symlink_metadata(root).is_ok() {
        return Err(Error::new(
            "bad_state",
            "Git private view already exists and will not be reused",
        ));
    }
    fs::create_dir(root).map_err(|_| unavailable("Git private view could not be created"))?;
    ensure_private_root(root)
}

fn create_transport_environment(root: &Path) -> Result<TransportEnvironment> {
    let environment = TransportEnvironment {
        global_config: root.join(".eliot-empty-global"),
        home: root.join(".eliot-home"),
        xdg_config: root.join(".eliot-xdg"),
        hooks: root.join(".eliot-hooks"),
        template: root.join(".eliot-template"),
    };
    write_private_file_new(&environment.global_config, b"")?;
    for directory in [
        &environment.home,
        &environment.xdg_config,
        &environment.hooks,
        &environment.template,
    ] {
        fs::create_dir(directory)
            .map_err(|_| unavailable("Git private transport directory could not be created"))?;
        ensure_private_root(directory)?;
    }
    Ok(environment)
}

fn transport_environment(root: &Path) -> TransportEnvironment {
    TransportEnvironment {
        global_config: root.join(".eliot-empty-global"),
        home: root.join(".eliot-home"),
        xdg_config: root.join(".eliot-xdg"),
        hooks: root.join(".eliot-hooks"),
        template: root.join(".eliot-template"),
    }
}

async fn ensure_transport_view(
    data_root: &Path,
    command_id: &str,
    git: &GitProbe,
    repo: &RepositoryBinding,
    frozen: &FrozenGitFetch,
    cancellation: watch::Receiver<bool>,
) -> Result<()> {
    let view = private_view_path(data_root, command_id)?;
    create_private_environment_root(&view)?;
    let environment = create_transport_environment(&view)?;
    let process = git_process_context(
        git,
        repo,
        &environment,
        frozen.credential_helper.as_ref(),
        endpoint_protocol(&frozen.endpoint)?,
        None,
    )
    .await?;
    let mut args = vec![
        OsString::from("init"),
        OsString::from("--bare"),
        OsString::from("--quiet"),
        OsString::from(format!(
            "--template={}",
            git_path_argument(&environment.template)?
        )),
    ];
    if frozen.advertised_oid.len() == 64 {
        args.push(OsString::from("--object-format=sha256"));
    }
    args.push(view.as_os_str().to_owned());
    let initialized = run_owned_git(
        &process,
        &view,
        &args,
        &[],
        Instant::now() + GIT_READ_TIME,
        Some(cancellation),
    )
    .await?;
    if !initialized.quiescent {
        return Err(Error::new(
            "unknown",
            "Git private repository initialization did not settle",
        ));
    }
    if initialized.exit_code != Some(0) || initialized.interrupted {
        return Err(unavailable(
            "Git private repository could not be initialized",
        ));
    }

    let config = render_transport_config(&environment, frozen)?;
    write_private_file_replace(&view.join("config"), config.as_bytes())?;
    shared_object_directory(&repo.common_dir)?;
    verify_transport_view(data_root, command_id, frozen)
}

fn render_transport_config(
    environment: &TransportEnvironment,
    frozen: &FrozenGitFetch,
) -> Result<String> {
    let hooks = git_path_argument(&environment.hooks)?;
    let protocol = endpoint_protocol(&frozen.endpoint)?;
    let endpoint = config_quote(&native_endpoint_argument(&frozen.endpoint)?)?;
    let hooks = config_quote(&hooks)?;
    let mut config = format!(
        "[core]\n\trepositoryformatversion = {}\n\tbare = true\n\thooksPath = {hooks}\n\tfsmonitor = false\n\tuntrackedCache = false\n\tpreloadIndex = false\n\talternateRefsCommand = \"\"\n[remote \"{PRIVATE_REMOTE}\"]\n\turl = {endpoint}\n[credential]\n\thelper = \"\"\n\tinteractive = false\n[protocol]\n\tallow = never\n[gc]\n\tauto = 0\n[maintenance]\n\tauto = false\n[submodule]\n\trecurse = false\n[fetch]\n\twriteCommitGraph = false\n",
        if frozen.advertised_oid.len() == 64 {
            1
        } else {
            0
        }
    );
    if frozen.credential_helper.is_some() {
        config.push_str("[credential]\n\thelper = manager\n");
    }
    if frozen.advertised_oid.len() == 64 {
        config.push_str("[extensions]\n\tobjectformat = sha256\n");
    }
    for scheme in ["https", "http", "git", "file"] {
        config.push_str(&format!("[protocol \"{scheme}\"]\n\tallow = "));
        config.push_str(if protocol == scheme {
            "always\n"
        } else {
            "never\n"
        });
    }
    Ok(config)
}

fn config_quote(value: &str) -> Result<String> {
    if value.chars().any(char::is_control) {
        return Err(Error::new(
            "invalid",
            "Git private configuration contains control characters",
        ));
    }
    let mut quoted = String::from("\"");
    for character in value.chars() {
        match character {
            '\\' => quoted.push_str("\\\\"),
            '"' => quoted.push_str("\\\""),
            value => quoted.push(value),
        }
    }
    quoted.push('"');
    Ok(quoted)
}

fn verify_transport_view(
    data_root: &Path,
    command_id: &str,
    frozen: &FrozenGitFetch,
) -> Result<()> {
    let view = private_view_path(data_root, command_id)?;
    ensure_no_reparse_tree(&view)?;
    let environment = transport_environment(&view);
    let expected_config = render_transport_config(&environment, frozen)?;
    let config = read_private_file(&view.join("config"), eliot_core::limits::MAX_FRAME_BYTES)?;
    if config != expected_config.as_bytes() {
        return Err(Error::new(
            "source_integrity",
            "private Git transport config differs from the frozen safe builder",
        ));
    }
    if !read_private_file(&environment.global_config, 1)?.is_empty() {
        return Err(Error::new(
            "source_integrity",
            "private Git global config is not empty",
        ));
    }
    shared_object_directory(Path::new(&frozen.common_dir))?;
    ensure_empty_directory(&environment.hooks)?;
    ensure_empty_directory(&environment.template)?;
    Ok(())
}

fn ensure_empty_directory(path: &Path) -> Result<()> {
    ensure_private_directory(path)?;
    let mut entries =
        fs::read_dir(path).map_err(|_| unavailable("private Git directory cannot be read"))?;
    if entries.next().is_some() {
        return Err(Error::new(
            "source_integrity",
            "private Git hook or template directory is not empty",
        ));
    }
    Ok(())
}

fn ensure_no_reparse_tree(view: &Path) -> Result<()> {
    let bare_paths = [
        view.to_owned(),
        view.join("config"),
        view.join("HEAD"),
        view.join("objects"),
        view.join("objects").join("info"),
        view.join("objects").join("pack"),
        view.join("refs"),
        view.join(".eliot-empty-global"),
        view.join(".eliot-home"),
        view.join(".eliot-xdg"),
        view.join(".eliot-hooks"),
        view.join(".eliot-template"),
    ];
    for path in bare_paths {
        if fs::symlink_metadata(&path).is_ok() {
            ensure_no_reparse_path(&path)?;
        }
    }
    Ok(())
}

fn ensure_no_reparse_path(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| Error::new("source_integrity", "private Git path disappeared"))?;
    if metadata.file_type().is_symlink() || crate::platform::is_reparse_point(&metadata) {
        return Err(Error::new(
            "source_integrity",
            "private Git path is a symbolic link or reparse point",
        ));
    }
    Ok(())
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    ensure_no_reparse_path(path)?;
    let metadata = fs::metadata(path)
        .map_err(|_| Error::new("source_integrity", "private Git directory is unavailable"))?;
    if !metadata.is_dir() {
        return Err(Error::new(
            "source_integrity",
            "private Git path is not a directory",
        ));
    }
    validate_private_root(path)
}

fn write_private_file_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|_| unavailable("private Git file could not be created"))?;
    file.write_all(bytes)
        .map_err(|_| unavailable("private Git file could not be written"))?;
    file.sync_all()
        .map_err(|_| unavailable("private Git file could not be flushed"))?;
    ensure_private_file(path)
}

fn write_private_file_replace(path: &Path, bytes: &[u8]) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| unavailable("Git-generated private config is unavailable"))?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || crate::platform::is_reparse_point(&metadata)
    {
        return Err(Error::new(
            "source_integrity",
            "Git-generated private config is not a regular file",
        ));
    }
    fs::remove_file(path).map_err(|_| unavailable("Git-generated config could not be replaced"))?;
    write_private_file_new(path, bytes)
}

fn read_private_file(path: &Path, limit: usize) -> Result<Vec<u8>> {
    ensure_no_reparse_path(path)?;
    let metadata =
        fs::metadata(path).map_err(|_| unavailable("private Git file is unavailable"))?;
    if !metadata.is_file() || metadata.len() > limit as u64 {
        return Err(Error::new(
            "source_integrity",
            "private Git file is not a bounded regular file",
        ));
    }
    fs::read(path).map_err(|_| unavailable("private Git file could not be read"))
}

fn shared_object_directory(common_dir: &Path) -> Result<PathBuf> {
    let path = common_dir.join("objects");
    reject_links(&path)?;
    let canonical = canonical_root(
        path.to_str()
            .ok_or_else(|| Error::new("invalid", "repository object path is not UTF-8"))?,
    )?;
    if !canonical.is_dir() {
        return Err(Error::new(
            "source_integrity",
            "registered Git common directory has no object database",
        ));
    }
    reject_links(&canonical)?;
    Ok(canonical)
}

async fn git_toolchain(git: &GitProbe, repo: &RepositoryBinding) -> Result<GitToolchain> {
    let binding = GitBinding {
        git_dir: &repo.git_dir,
        common_dir: &repo.common_dir,
    };
    let prepared = git
        .prepare_workspace_command(&repo.checkout, &[], binding, Instant::now() + GIT_READ_TIME)
        .await?;
    let program = PathBuf::from(prepared.as_std().get_program());
    if !program.is_absolute() {
        return Err(unavailable("selected Git executable is not absolute"));
    }
    let program = fs::canonicalize(program)
        .map_err(|_| unavailable("selected Git executable cannot be resolved"))?;
    let exec_path = canonical_root(
        &read_git_line(
            git,
            &repo.checkout,
            &["--exec-path"],
            Instant::now() + GIT_READ_TIME,
        )
        .await?,
    )?;
    Ok(GitToolchain { program, exec_path })
}

async fn git_process_context(
    git: &GitProbe,
    repo: &RepositoryBinding,
    environment: &TransportEnvironment,
    helper: Option<&FrozenSetupFile>,
    protocol: &str,
    object_directory: Option<PathBuf>,
) -> Result<GitProcessContext> {
    Ok(GitProcessContext {
        toolchain: git_toolchain(git, repo).await?,
        environment: environment.clone(),
        helper: helper.cloned(),
        protocol: protocol.to_owned(),
        object_directory,
    })
}

async fn run_transport_command(
    process: &GitProcessContext,
    view: &Path,
    args: &[OsString],
    deadline: Instant,
    cancellation: watch::Receiver<bool>,
) -> Result<ProcessResult> {
    run_owned_git(process, view, args, &[], deadline, Some(cancellation)).await
}

async fn run_repository_command(
    process: &GitProcessContext,
    checkout: &Path,
    args: &[OsString],
    input: &[u8],
    deadline: Instant,
    cancellation: watch::Receiver<bool>,
) -> Result<ProcessResult> {
    run_owned_git(process, checkout, args, input, deadline, Some(cancellation)).await
}

async fn run_owned_git(
    context: &GitProcessContext,
    cwd: &Path,
    args: &[OsString],
    input: &[u8],
    deadline: Instant,
    mut cancellation: Option<watch::Receiver<bool>>,
) -> Result<ProcessResult> {
    let toolchain = &context.toolchain;
    let environment = &context.environment;
    let helper = context.helper.as_ref();
    let protocol = context.protocol.as_str();
    let object_directory = context.object_directory.as_deref();
    if Instant::now() >= deadline {
        return Err(unavailable("Git operation deadline elapsed before spawn"));
    }
    if !matches!(protocol, "https" | "http" | "git" | "file") {
        return Err(Error::new(
            "unsupported",
            "Git transport protocol is unsupported",
        ));
    }
    if cancellation
        .as_ref()
        .is_some_and(|receiver| *receiver.borrow())
    {
        return Ok(ProcessResult {
            exit_code: None,
            stdout: Vec::new(),
            stderr_len: 0,
            #[cfg(test)]
            stderr: Vec::new(),
            quiescent: true,
            interrupted: true,
        });
    }
    let mut command = Command::new(&toolchain.program);
    command
        .current_dir(cwd)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for name in [
        "SystemRoot",
        "WINDIR",
        "TEMP",
        "TMP",
        "USERPROFILE",
        "HOMEDRIVE",
        "HOMEPATH",
        "APPDATA",
        "LOCALAPPDATA",
        "ProgramData",
        "COMSPEC",
    ] {
        if let Some(value) = env::var_os(name) {
            command.env(name, value);
        }
    }
    let mut path_entries = Vec::<PathBuf>::new();
    if let Some(helper) = helper {
        let helper_path = PathBuf::from(&helper.canonical_path);
        if !helper_path.is_absolute() {
            return Err(Error::new(
                "source_integrity",
                "frozen helper path is not absolute",
            ));
        }
        path_entries.push(
            helper_path
                .parent()
                .ok_or_else(|| Error::new("source_integrity", "frozen helper has no parent"))?
                .to_owned(),
        );
    }
    path_entries.push(
        toolchain
            .program
            .parent()
            .ok_or_else(|| unavailable("selected Git executable has no parent"))?
            .to_owned(),
    );
    path_entries.push(toolchain.exec_path.clone());
    if let Some(system_root) = env::var_os("SystemRoot").or_else(|| env::var_os("WINDIR")) {
        let system32 = PathBuf::from(system_root).join("System32");
        if system32.is_dir() {
            path_entries.push(system32);
        }
    }
    let mut unique = Vec::<PathBuf>::new();
    let mut git_path_entries = Vec::<OsString>::new();
    for path in path_entries {
        let canonical = fs::canonicalize(&path)
            .map_err(|_| unavailable("scoped Git PATH directory is unavailable"))?;
        if !unique.contains(&canonical) {
            git_path_entries.push(OsString::from(git_path_argument(&canonical)?));
            unique.push(canonical);
        }
    }
    let path = env::join_paths(git_path_entries)
        .map_err(|_| unavailable("scoped Git PATH could not be constructed"))?;
    let git_exec_path = git_path_argument(&toolchain.exec_path)?;
    let home = git_path_argument(&environment.home)?;
    let xdg_config = git_path_argument(&environment.xdg_config)?;
    let global_config = git_path_argument(&environment.global_config)?;
    command
        .env("PATH", path)
        .env("GIT_EXEC_PATH", git_exec_path)
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", xdg_config)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", global_config)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .env("GIT_NO_LAZY_FETCH", "1")
        .env("GIT_NO_REPLACE_OBJECTS", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_ASKPASS", "")
        .env("SSH_ASKPASS", "")
        .env("GIT_EDITOR", "")
        .env("GIT_SEQUENCE_EDITOR", "")
        .env("GIT_PAGER", "")
        .env("PAGER", "")
        .env("GIT_EXTERNAL_DIFF", "")
        .env("GIT_PROTOCOL_FROM_USER", "0")
        .env("LC_ALL", "C")
        .env("GCM_INTERACTIVE", "Never");
    if let Some(object_directory) = object_directory {
        command.env("GIT_OBJECT_DIRECTORY", git_path_argument(object_directory)?);
    }
    let mut overrides = vec![
        ("core.hooksPath", git_path_argument(&environment.hooks)?),
        ("core.fsmonitor", "false".to_owned()),
        ("core.untrackedCache", "false".to_owned()),
        ("core.preloadIndex", "false".to_owned()),
        ("core.alternateRefsCommand", String::new()),
        ("gc.auto", "0".to_owned()),
        ("maintenance.auto", "false".to_owned()),
        ("submodule.recurse", "false".to_owned()),
        ("protocol.allow", "never".to_owned()),
        (
            "protocol.https.allow",
            if protocol == "https" {
                "always"
            } else {
                "never"
            }
            .to_owned(),
        ),
        (
            "protocol.http.allow",
            if protocol == "http" {
                "always"
            } else {
                "never"
            }
            .to_owned(),
        ),
        (
            "protocol.git.allow",
            if protocol == "git" { "always" } else { "never" }.to_owned(),
        ),
        (
            "protocol.file.allow",
            if protocol == "file" {
                "always"
            } else {
                "never"
            }
            .to_owned(),
        ),
        ("credential.interactive", "false".to_owned()),
        ("credential.helper", String::new()),
    ];
    if helper.is_some() {
        overrides.push(("credential.helper", "manager".to_owned()));
    }
    command.env("GIT_CONFIG_COUNT", overrides.len().to_string());
    for (index, (key, value)) in overrides.iter().enumerate() {
        command
            .env(format!("GIT_CONFIG_KEY_{index}"), key)
            .env(format!("GIT_CONFIG_VALUE_{index}"), value);
    }
    command.args(["--no-pager", "--no-optional-locks"]);
    command.args(args);

    if Instant::now() >= deadline
        || cancellation
            .as_ref()
            .is_some_and(|receiver| *receiver.borrow())
    {
        return Ok(ProcessResult {
            exit_code: None,
            stdout: Vec::new(),
            stderr_len: 0,
            #[cfg(test)]
            stderr: Vec::new(),
            quiescent: true,
            interrupted: true,
        });
    }
    let mut child = OwnedChild::spawn(command)?;
    let Some(stdout) = child.take_stdout() else {
        let (exit_code, quiescent) = stop_owned_child(&mut child).await;
        return Ok(ProcessResult {
            exit_code,
            stdout: Vec::new(),
            stderr_len: 0,
            #[cfg(test)]
            stderr: Vec::new(),
            quiescent,
            interrupted: true,
        });
    };
    let Some(stderr) = child.take_stderr() else {
        let (exit_code, quiescent) = stop_owned_child(&mut child).await;
        return Ok(ProcessResult {
            exit_code,
            stdout: Vec::new(),
            stderr_len: 0,
            #[cfg(test)]
            stderr: Vec::new(),
            quiescent,
            interrupted: true,
        });
    };
    let Some(mut stdin) = child.take_stdin() else {
        let (exit_code, quiescent) = stop_owned_child(&mut child).await;
        return Ok(ProcessResult {
            exit_code,
            stdout: Vec::new(),
            stderr_len: 0,
            #[cfg(test)]
            stderr: Vec::new(),
            quiescent,
            interrupted: true,
        });
    };
    let input = input.to_vec();
    let captured = async {
        let write = async move {
            if !input.is_empty() {
                stdin
                    .write_all(&input)
                    .await
                    .map_err(|_| unavailable("Git stdin write failed"))?;
            }
            stdin
                .shutdown()
                .await
                .map_err(|_| unavailable("Git stdin close failed"))?;
            Ok::<(), Error>(())
        };
        let (stdout, stderr, (), (exit_code, quiescent)) = tokio::try_join!(
            capture_bounded(stdout, eliot_core::limits::MAX_FRAME_BYTES),
            capture_bounded(stderr, eliot_core::MAX_STDERR_BYTES),
            write,
            wait_owned_child(&mut child),
        )?;
        Ok::<_, Error>((stdout, stderr, exit_code, quiescent))
    };
    let result = if let Some(receiver) = cancellation.as_mut() {
        tokio::select! {
            biased;
            result = timeout_at(deadline, captured) => Some(result),
            _ = receiver.changed() => None,
        }
    } else {
        Some(timeout_at(deadline, captured).await)
    };
    match result {
        Some(Ok(Ok((stdout, stderr, exit_code, quiescent)))) => Ok(ProcessResult {
            exit_code,
            stdout,
            stderr_len: stderr.len(),
            #[cfg(test)]
            stderr,
            quiescent,
            interrupted: false,
        }),
        Some(Ok(Err(_))) | Some(Err(_)) | None => {
            let (exit_code, quiescent) = stop_owned_child(&mut child).await;
            Ok(ProcessResult {
                exit_code,
                stdout: Vec::new(),
                stderr_len: 0,
                #[cfg(test)]
                stderr: Vec::new(),
                quiescent,
                interrupted: true,
            })
        }
    }
}

async fn capture_bounded<R: AsyncRead + Unpin>(mut reader: R, cap: usize) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(cap.min(64 * 1024));
    let mut buffer = [0_u8; 8192];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|_| unavailable("Git output capture failed"))?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > cap {
            return Err(unavailable("Git stdout exceeded the process capture bound"));
        }
        output.extend_from_slice(&buffer[..read]);
    }
}

async fn wait_owned_child(child: &mut OwnedChild) -> Result<(Option<i32>, bool)> {
    #[cfg(windows)]
    {
        let status = child
            .wait()
            .await
            .map_err(|_| unavailable("Git child wait failed"))?;
        let quiescent = child
            .wait_family_empty(GIT_CLEANUP_TIME)
            .await
            .unwrap_or(false);
        Ok((status.code(), quiescent))
    }
    #[cfg(target_os = "linux")]
    {
        child
            .wait_exit_unreaped()
            .await
            .map_err(|_| unavailable("Git leader exit observation failed"))?;
        let quiescent = child
            .wait_group_empty_except_leader(GIT_CLEANUP_TIME)
            .await
            .unwrap_or(false);
        let status = child
            .wait()
            .await
            .map_err(|_| unavailable("Git child wait failed"))?;
        Ok((status.code(), quiescent))
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let status = child
            .wait()
            .await
            .map_err(|_| unavailable("Git child wait failed"))?;
        Ok((status.code(), false))
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = child;
        Err(Error::new(
            "unsupported",
            "owned Git process custody is unavailable on this platform",
        ))
    }
}

async fn stop_owned_child(child: &mut OwnedChild) -> (Option<i32>, bool) {
    let _ = child.terminate();
    match timeout_at(Instant::now() + GIT_CLEANUP_TIME, wait_owned_child(child)).await {
        Ok(Ok(result)) => result,
        _ => (None, false),
    }
}

fn not_allowed(message: &str) -> Error {
    Error::new("not_allowed", message)
}

fn ensure_private_root(path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        crate::platform::windows::ensure_private_root(path)
    }
    #[cfg(unix)]
    {
        crate::platform::unix::ensure_private_root(path)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "unsupported",
            "private Git directories are unsupported on this platform",
        ))
    }
}

fn validate_private_root(path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        crate::platform::windows::validate_private_root(path)
    }
    #[cfg(unix)]
    {
        crate::platform::unix::validate_private_root(path)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "unsupported",
            "private Git directories are unsupported on this platform",
        ))
    }
}

fn ensure_private_file(path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        crate::platform::windows::ensure_private_file(path)
    }
    #[cfg(unix)]
    {
        crate::platform::unix::ensure_private_file(path)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "unsupported",
            "private Git files are unsupported on this platform",
        ))
    }
}

fn unavailable(message: &str) -> Error {
    Error::new("unavailable", message)
}

fn read_private_ref(view: &Path, reference: &str) -> Result<String> {
    let suffix = reference
        .strip_prefix("refs/")
        .ok_or_else(|| Error::new("invalid", "private fetch ref is not full"))?;
    let components = suffix.split('/').collect::<Vec<_>>();
    if components.is_empty()
        || components
            .iter()
            .any(|part| part.is_empty() || *part == "..")
    {
        return Err(Error::new("invalid", "private fetch ref path is malformed"));
    }
    let mut path = view.join("refs");
    ensure_no_reparse_path(&path)?;
    for component in components {
        path.push(component);
        ensure_no_reparse_path(&path)?;
    }
    let oid = one_line(&read_private_file(&path, eliot_core::MAX_STDERR_BYTES)?)?;
    if !is_oid(&oid) {
        return Err(Error::new(
            "source_integrity",
            "private fetched ref OID is malformed",
        ));
    }
    Ok(oid)
}

async fn verify_registered_object_graph(
    process: &GitProcessContext,
    repo: &RepositoryBinding,
    oid: &str,
    deadline: Instant,
    cancellation: Option<watch::Receiver<bool>>,
) -> Result<ProcessResult> {
    let args = [
        OsString::from("rev-list"),
        OsString::from("--objects"),
        OsString::from("--quiet"),
        OsString::from("--missing=error"),
        OsString::from(oid),
    ];
    run_owned_git(process, &repo.checkout, &args, &[], deadline, cancellation).await
}

#[cfg(all(test, any(windows, target_os = "linux")))]
mod tests {
    use super::*;
    use std::process::{Command as TestCommand, Output};

    const PROJECT: &str = "native-fetch-test";
    const TRACKING_REF: &str = "refs/remotes/origin/eliot-fetch";

    struct TestRoot {
        path: PathBuf,
        retain: bool,
    }

    impl TestRoot {
        fn new() -> Self {
            let path = env::temp_dir().join(format!("eliot-git-fetch-{}", Ulid::new()));
            fs::create_dir(&path).expect("create isolated Git test root");
            Self {
                path,
                retain: false,
            }
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            if !self.retain {
                let _ = fs::remove_dir_all(&self.path);
            }
        }
    }

    struct TestGit {
        program: PathBuf,
        path: OsString,
        global: PathBuf,
        home: PathBuf,
        hooks: PathBuf,
        temp: PathBuf,
    }

    impl TestGit {
        fn new(root: &Path) -> Self {
            let path = env::var_os("PATH").expect("test Git PATH");
            let name = if cfg!(windows) { "git.exe" } else { "git" };
            let program = env::split_paths(&path)
                .filter(|directory| directory.is_absolute())
                .map(|directory| directory.join(name))
                .find(|candidate| candidate.is_file())
                .and_then(|candidate| fs::canonicalize(candidate).ok())
                .expect("test Git executable");
            let global = root.join("empty-global.gitconfig");
            let home = root.join("home");
            let hooks = root.join("hooks");
            let temp = root.join("tmp");
            fs::write(&global, b"").expect("empty global Git config");
            for directory in [&home, &hooks, &temp] {
                fs::create_dir(directory).expect("isolated Git support directory");
            }
            Self {
                program,
                path,
                global,
                home,
                hooks,
                temp,
            }
        }

        fn run(&self, cwd: &Path, args: &[OsString]) -> Output {
            let mut command = TestCommand::new(&self.program);
            command
                .current_dir(cwd)
                .env_clear()
                .env("PATH", &self.path)
                .env("HOME", &self.home)
                .env("USERPROFILE", &self.home)
                .env("XDG_CONFIG_HOME", &self.home)
                .env("TEMP", &self.temp)
                .env("TMP", &self.temp)
                .env("GIT_CONFIG_NOSYSTEM", "1")
                .env("GIT_CONFIG_GLOBAL", &self.global)
                .env("GIT_CONFIG_COUNT", "1")
                .env("GIT_CONFIG_KEY_0", "core.hooksPath")
                .env("GIT_CONFIG_VALUE_0", self.hooks.to_string_lossy().as_ref())
                .env("GIT_TERMINAL_PROMPT", "0")
                .env("GIT_ASKPASS", "")
                .env("SSH_ASKPASS", "")
                .env("GCM_INTERACTIVE", "Never");
            for name in [
                "SystemRoot",
                "WINDIR",
                "HOMEDRIVE",
                "HOMEPATH",
                "APPDATA",
                "LOCALAPPDATA",
                "ProgramData",
                "COMSPEC",
            ] {
                if let Some(value) = env::var_os(name) {
                    command.env(name, value);
                }
            }
            command.args(args).output().expect("run local Git fixture")
        }

        fn success(&self, cwd: &Path, args: &[OsString]) -> Vec<u8> {
            let output = self.run(cwd, args);
            assert!(
                output.status.success(),
                "local Git fixture command failed: {:?}: {}",
                args,
                String::from_utf8_lossy(&output.stderr)
            );
            output.stdout
        }

        fn text(&self, cwd: &Path, args: &[OsString]) -> String {
            String::from_utf8(self.success(cwd, args))
                .expect("local Git output is UTF-8")
                .trim_end_matches(['\r', '\n'])
                .to_owned()
        }
    }

    struct LocalFetchFixture {
        root: TestRoot,
        git: TestGit,
        data_root: PathBuf,
        seed: PathBuf,
        checkout: PathBuf,
        remote: PathBuf,
        registration: ProjectRegistrationView,
    }

    impl LocalFetchFixture {
        async fn new() -> Self {
            let root = TestRoot::new();
            let git = TestGit::new(&root.path);
            let seed = root.path.join("seed");
            let checkout = root.path.join("checkout");
            let remote = root.path.join("origin.git");
            let data_root = root.path.join("data");
            fs::create_dir(&seed).expect("create source worktree");
            fs::create_dir(&checkout).expect("create registered checkout");
            ensure_private_root(&data_root).expect("create private data root");

            let mut args = git_args(&["init", "--bare", "--quiet", "--initial-branch=main"]);
            args.push(remote.as_os_str().to_owned());
            git.success(&root.path, &args);
            init_worktree(&git, &root.path, &seed);
            git.success(
                &seed,
                &git_args(&["config", "user.name", "ELIOT local test"]),
            );
            git.success(
                &seed,
                &git_args(&["config", "user.email", "git-fetch-test@example.invalid"]),
            );
            fs::write(seed.join("remote.txt"), b"remote source commit\n")
                .expect("write remote source");
            git.success(&seed, &git_args(&["add", "--", "remote.txt"]));
            git.success(
                &seed,
                &git_args(&["commit", "--quiet", "-m", "remote source"]),
            );
            let remote_text = remote.to_string_lossy().into_owned();
            git.success(&seed, &git_args(&["remote", "add", "origin", &remote_text]));
            git.success(&seed, &git_args(&["push", "origin", "refs/heads/main"]));

            init_worktree(&git, &root.path, &checkout);
            git.success(
                &checkout,
                &git_args(&["config", "user.name", "ELIOT local test"]),
            );
            git.success(
                &checkout,
                &git_args(&["config", "user.email", "git-fetch-test@example.invalid"]),
            );
            fs::write(checkout.join("local.txt"), b"registered checkout\n")
                .expect("write checkout commit");
            git.success(&checkout, &git_args(&["add", "--", "local.txt"]));
            git.success(
                &checkout,
                &git_args(&["commit", "--quiet", "-m", "checkout commit"]),
            );
            git.success(
                &checkout,
                &git_args(&["remote", "add", "origin", &remote_text]),
            );
            let endpoint = git.text(
                &checkout,
                &git_args(&["config", "--local", "--get-all", "remote.origin.url"]),
            );
            let checkout_text = fs::canonicalize(&checkout)
                .expect("canonical checkout")
                .to_str()
                .expect("checkout is UTF-8")
                .to_owned();
            let (canonical_checkout, repo_common_dir) = WorkspaceGit::new()
                .unwrap()
                .repository_identity(&checkout_text)
                .await
                .unwrap();
            let path = serde_json::to_string(&canonical_checkout).unwrap();
            let endpoint_value = serde_json::to_string(&endpoint).unwrap();
            fs::write(
                data_root.join("config.toml"),
                format!(
                    "[projects.{PROJECT}]\n\
                     path = {path}\n\
                     profiles = [\"fake\"]\n\
                     actions = [\"git.fetch\"]\n\
                     run_env = []\n\
                     [projects.{PROJECT}.git]\n\
                     fetch_remotes = {{ origin = {endpoint_value} }}\n"
                ),
            )
            .expect("write owner project Git approval");
            let registration = ProjectRegistrationView {
                id: PROJECT.to_owned(),
                checkout: canonical_checkout,
                repo_common_dir,
                profiles: vec!["fake".to_owned()],
                actions: vec!["git.fetch".to_owned()],
                interpreters: Default::default(),
                run_env: Vec::new(),
                git: Some(eliot_core::api::ProjectGitView {
                    fetch_remotes: std::collections::BTreeMap::from([(
                        "origin".to_owned(),
                        endpoint,
                    )]),
                    credential_helper: None,
                }),
                revision: 1,
            };
            registration.validate().unwrap();
            Self {
                root,
                git,
                data_root,
                seed,
                checkout,
                remote,
                registration,
            }
        }

        async fn freeze(&mut self) -> FrozenGitFetch {
            match prepare_fetch(
                &self.data_root,
                &self.registration,
                &GitFetchDefinitionArgs {
                    repository: PROJECT.to_owned(),
                    remote: "origin".to_owned(),
                    source_ref: "refs/heads/main".to_owned(),
                    tracking_ref: TRACKING_REF.to_owned(),
                    timeout: Some("30s".to_owned()),
                },
            )
            .await
            {
                Ok(frozen) => frozen,
                Err(error) => {
                    if error.code == "unknown" {
                        self.root.retain = true;
                    }
                    panic!("freeze local Git fetch: {error}");
                }
            }
        }

        fn remote_oid(&self) -> String {
            self.git
                .text(&self.remote, &git_args(&["rev-parse", "refs/heads/main"]))
        }

        fn checkout_head(&self) -> String {
            self.git
                .text(&self.checkout, &git_args(&["rev-parse", "HEAD"]))
        }

        fn tracking_oid(&self) -> Option<String> {
            let output = self.git.run(
                &self.checkout,
                &git_args(&["show-ref", "--verify", "--hash", TRACKING_REF]),
            );
            output.status.success().then(|| {
                String::from_utf8(output.stdout)
                    .expect("tracking ref output is UTF-8")
                    .trim_end_matches(['\r', '\n'])
                    .to_owned()
            })
        }

        fn push_later_source(&self) {
            fs::write(self.seed.join("later.txt"), b"later remote commit\n")
                .expect("write later remote source");
            self.git
                .success(&self.seed, &git_args(&["add", "--", "later.txt"]));
            self.git.success(
                &self.seed,
                &git_args(&["commit", "--quiet", "-m", "later source"]),
            );
            self.git.success(
                &self.seed,
                &git_args(&["push", "origin", "refs/heads/main"]),
            );
        }
    }

    fn init_worktree(git: &TestGit, cwd: &Path, path: &Path) {
        let mut args = git_args(&["init", "--quiet", "--initial-branch=main"]);
        args.push(OsString::from(format!(
            "--template={}",
            git.hooks.to_string_lossy()
        )));
        args.push(path.as_os_str().to_owned());
        git.success(cwd, &args);
    }

    fn git_args(args: &[&str]) -> Vec<OsString> {
        args.iter().map(OsString::from).collect()
    }

    #[tokio::test]
    async fn local_fetch_shares_new_objects_and_changes_only_tracking_ref() {
        let mut fixture = LocalFetchFixture::new().await;
        let head_before = fixture.checkout_head();
        let remote_oid = fixture.remote_oid();
        assert_ne!(head_before, remote_oid);
        assert!(
            !fixture
                .git
                .run(
                    &fixture.checkout,
                    &git_args(&["cat-file", "-e", &format!("{remote_oid}^{{commit}}")]),
                )
                .status
                .success()
        );
        let frozen = fixture.freeze().await;
        assert_eq!(frozen.advertised_oid, remote_oid);
        assert_eq!(frozen.expected_old_oid, None);
        let command_id = Ulid::new().to_string();
        let (_cancel, cancellation) = watch::channel(false);
        let mut execution = execute(
            &fixture.data_root,
            &command_id,
            &frozen,
            &fixture.registration,
            cancellation,
        )
        .await;
        if execution.state != CommandState::Applied {
            fixture.root.retain = true;
        }
        assert_eq!(
            execution.state,
            CommandState::Applied,
            "unexpected fetch outcome: {}",
            execution.outcome
        );
        let outcome: GitFetchOutcome =
            serde_json::from_value(execution.outcome.clone()).expect("typed fetch outcome");
        assert_eq!(outcome.tracking_ref, TRACKING_REF);
        assert_eq!(outcome.tracking_oid, remote_oid);
        assert_eq!(fixture.tracking_oid().as_deref(), Some(remote_oid.as_str()));
        assert_eq!(fixture.checkout_head(), head_before);
        assert!(
            fixture
                .git
                .text(&fixture.checkout, &git_args(&["status", "--porcelain"]))
                .is_empty()
        );
        let view = execution
            .cleanup_view
            .take()
            .expect("settled private fetch view");
        cleanup_after_settlement(&fixture.data_root, &view);
        assert!(!view.exists());
        fixture.git.success(
            &fixture.checkout,
            &git_args(&["cat-file", "-e", &format!("{remote_oid}^{{commit}}")]),
        );
    }

    #[tokio::test]
    async fn moved_local_source_does_not_write_tracking_ref() {
        let mut fixture = LocalFetchFixture::new().await;
        let frozen = fixture.freeze().await;
        let advertised = frozen.advertised_oid.clone();
        let head_before = fixture.checkout_head();
        fixture.push_later_source();
        assert_ne!(fixture.remote_oid(), advertised);

        let command_id = Ulid::new().to_string();
        let (_cancel, cancellation) = watch::channel(false);
        let mut execution = execute(
            &fixture.data_root,
            &command_id,
            &frozen,
            &fixture.registration,
            cancellation,
        )
        .await;
        if execution.state == CommandState::Unknown {
            fixture.root.retain = true;
        }
        assert_eq!(execution.state, CommandState::Failed);
        assert_eq!(fixture.tracking_oid(), None);
        assert_eq!(fixture.checkout_head(), head_before);
        let view = execution
            .cleanup_view
            .take()
            .expect("settled failed private fetch view");
        cleanup_after_settlement(&fixture.data_root, &view);
    }

    #[tokio::test]
    async fn changed_checkout_remote_is_rejected_before_tracking_write() {
        let mut fixture = LocalFetchFixture::new().await;
        let frozen = fixture.freeze().await;
        let head_before = fixture.checkout_head();
        fixture.git.success(
            &fixture.checkout,
            &git_args(&[
                "remote",
                "set-url",
                "origin",
                "https://changed.invalid/repository.git",
            ]),
        );

        let command_id = Ulid::new().to_string();
        let (_cancel, cancellation) = watch::channel(false);
        let mut execution = execute(
            &fixture.data_root,
            &command_id,
            &frozen,
            &fixture.registration,
            cancellation,
        )
        .await;
        if execution.state == CommandState::Unknown {
            fixture.root.retain = true;
        }
        assert_eq!(execution.state, CommandState::Rejected);
        assert_eq!(fixture.tracking_oid(), None);
        assert_eq!(fixture.checkout_head(), head_before);
        if let Some(view) = execution.cleanup_view.take() {
            cleanup_after_settlement(&fixture.data_root, &view);
        }
    }

    #[tokio::test]
    async fn pre_cancelled_fetch_never_starts_tracking_cas() {
        let mut fixture = LocalFetchFixture::new().await;
        let frozen = fixture.freeze().await;
        let head_before = fixture.checkout_head();
        let command_id = Ulid::new().to_string();
        let (_cancel, cancellation) = watch::channel(true);
        let mut execution = execute(
            &fixture.data_root,
            &command_id,
            &frozen,
            &fixture.registration,
            cancellation,
        )
        .await;
        if execution.state == CommandState::Unknown {
            fixture.root.retain = true;
        }
        assert_eq!(execution.state, CommandState::Rejected);
        assert_eq!(fixture.tracking_oid(), None);
        assert_eq!(fixture.checkout_head(), head_before);
        if let Some(view) = execution.cleanup_view.take() {
            cleanup_after_settlement(&fixture.data_root, &view);
        }
    }
}
