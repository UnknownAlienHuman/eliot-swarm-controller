//! Local Git post-commit wrapper installation.
//!
//! Installation is repository-local, read-back verified and reversible. The
//! callback is detached from Git and its failure is always ignored; the hook
//! is observational and cannot veto the commit that already happened.

use crate::{
    error::{Error, Result},
    hooks::contract::{EVENT_NAME, GitCommitSnapshot, validate_hook_credential},
    model::{self, Credential},
    platform,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    thread,
    time::{Duration, Instant},
};

const MAX_HOOK_FILE_BYTES: u64 = 1024 * 1024;
const MAX_GIT_OUTPUT_BYTES: usize = 4096;
const GIT_COMMAND_TIMEOUT: Duration = Duration::from_secs(4);
const SOURCE_DIRECTORY: &str = "eliot-hook-sources";
const INSTALL_SCHEMA_VERSION: u32 = 1;
const HOOK_NAME: &str = "post-commit";
const PENDING_SETUP_SCHEMA_VERSION: u32 = 1;
const SETUP_REQUEST_FILE: &str = "setup-request.json";

#[derive(Debug, Clone)]
pub struct HookInstallPlan {
    repository_path: PathBuf,
    git_executable: PathBuf,
    git_dir: PathBuf,
    hooks_dir: PathBuf,
    hook_path: PathBuf,
    prior_hook: Option<PreviousHook>,
    prior_hook_active: bool,
    prior_exe_active: bool,
    prior_exe_digest: Option<String>,
}

#[derive(Debug, Clone)]
struct PreviousHook {
    bytes: Vec<u8>,
    sha256: String,
    mode: Option<u32>,
}

impl HookInstallPlan {
    pub fn repository_path(&self) -> &Path {
        &self.repository_path
    }

    pub fn git_executable(&self) -> &Path {
        &self.git_executable
    }

    /// A nonsecret manager-facing preview. Source credentials are issued only
    /// after the user chooses the explicit apply command.
    pub fn public_value(&self) -> serde_json::Value {
        serde_json::json!({
            "valid":true,
            "event":EVENT_NAME,
            "phase":"post_commit",
            "veto":"none_after_commit",
            "repository_root":self.repository_path,
            "git_directory":self.git_dir,
            "hooks_directory":self.hooks_dir,
            "hook_path":self.hook_path,
            "existing_hook":match &self.prior_hook {
                Some(hook) => serde_json::json!({"present":true,"sha256":hook.sha256,"active_before_install":self.prior_hook_active}),
                None => serde_json::json!({"present":false,"active_before_install":self.prior_hook_active}),
            },
            "existing_post_commit_exe":self.prior_exe_digest.as_ref().map(|digest|serde_json::json!({"sha256":digest,"active_before_install":self.prior_exe_active})),
            "will_modify_global_git_config":false,
            "will_modify_path":false,
            "long_work_on_hook_path":false
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InstallPhase {
    Installing,
    #[default]
    Installed,
    Revoking,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallManifest {
    schema_version: u32,
    #[serde(default)]
    phase: InstallPhase,
    source_id: String,
    event: String,
    repository_path: String,
    git_executable: String,
    git_dir: String,
    hook_path: String,
    wrapper_sha256: String,
    previous_hook_path: Option<String>,
    previous_hook_sha256: Option<String>,
    previous_hook_mode: Option<u32>,
    previous_hook_was_active: bool,
    chained_hook_path: Option<String>,
    chained_hook_sha256: Option<String>,
    credential_path: String,
    installed_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookInstallReadback {
    pub source_id: String,
    pub event: String,
    pub state: String,
    pub repository_root: String,
    pub hook_path: String,
    pub wrapper_sha256: Option<String>,
    pub wrapper_matches: bool,
    pub backup_matches: Option<bool>,
    pub credential_file_present: bool,
    pub message: String,
}

/// Local setup state prepared before sending the closed Store request. This
/// type intentionally has no Debug implementation because it contains the
/// HookSource token.
pub struct PreparedHookSource {
    pub client_request_id: String,
    pub project_id: String,
    pub source_id: String,
    pub credential: Credential,
    pub credential_path: PathBuf,
    pending_descriptor_path: PathBuf,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingSetupDescriptor {
    schema_version: u32,
    client_request_id: String,
    project_id: String,
    source_id: String,
    credential_ready: bool,
}

/// Read the configured repository's native hook state without changing it.
/// Any explicit effective `core.hooksPath` is left untouched and unsupported;
/// no global setting or user hook directory is rewritten.
pub fn preview_post_commit(
    repository_path: &Path,
    git_executable: &Path,
) -> Result<HookInstallPlan> {
    validate_git_executable(git_executable)?;
    let repository_path = fs::canonicalize(repository_path).map_err(|_| {
        Error::new(
            "HOOK_REPOSITORY_UNAVAILABLE",
            "configured repository path could not be canonicalized",
        )
    })?;
    let top = git_stdout(
        git_executable,
        &repository_path,
        &["rev-parse", "--show-toplevel"],
    )?;
    let top = canonical_output_path(&top)?;
    if top != repository_path {
        return Err(Error::new(
            "HOOK_REPOSITORY_MISMATCH",
            "configured path is not the exact Git repository root",
        ));
    }
    let git_dir = canonical_output_path(&git_stdout(
        git_executable,
        &repository_path,
        &["rev-parse", "--absolute-git-dir"],
    )?)?;

    match run_git(
        git_executable,
        &repository_path,
        &["config", "--get", "core.hooksPath"],
    )? {
        output if output.status.success() => {
            return Err(Error::new(
                "HOOK_CUSTOM_HOOKS_PATH",
                "this repository inherits an explicit core.hooksPath; setup leaves that user-managed hook directory unchanged",
            ));
        }
        output if output.status.code() == Some(1) => {}
        _ => {
            return Err(Error::new(
                "HOOK_GIT_CONFIG_READBACK_FAILED",
                "Git could not safely determine the effective core.hooksPath setting",
            ));
        }
    }

    // A working HEAD is part of installation preflight. It prevents an empty
    // repository or a nonrepository directory from receiving a callback that
    // cannot ever produce a commit fact.
    let head = git_stdout(
        git_executable,
        &repository_path,
        &["rev-parse", "--verify", "HEAD^{commit}"],
    )?;
    if !crate::forge::valid_object_id(head.trim()) {
        return Err(Error::new(
            "HOOK_REPOSITORY_NO_COMMIT",
            "configured repository has no readable commit at HEAD",
        ));
    }

    let hooks_value = git_stdout(
        git_executable,
        &repository_path,
        &["rev-parse", "--git-path", "hooks"],
    )?;
    let hooks_candidate = PathBuf::from(hooks_value.trim());
    let hooks_candidate = if hooks_candidate.is_absolute() {
        hooks_candidate
    } else {
        repository_path.join(hooks_candidate)
    };
    let hooks_dir = if hooks_candidate.exists() {
        fs::canonicalize(&hooks_candidate).map_err(|_| {
            Error::new(
                "HOOK_DIRECTORY_INVALID",
                "Git hook directory is not readable",
            )
        })?
    } else {
        // Git's default for a repository without a hooks directory is the
        // configured git-dir/hooks path; create only during explicit apply.
        git_dir.join("hooks")
    };
    if !hooks_dir.starts_with(&git_dir) {
        return Err(Error::new(
            "HOOK_CUSTOM_HOOKS_PATH",
            "resolved Git hook directory is outside this repository's git directory",
        ));
    }
    if let Ok(metadata) = fs::symlink_metadata(&hooks_dir)
        && (metadata.file_type().is_symlink() || !metadata.is_dir())
    {
        return Err(Error::new(
            "HOOK_DIRECTORY_INVALID",
            "Git hook directory must be a regular repository-local directory",
        ));
    }

    let hook_path = hooks_dir.join(HOOK_NAME);
    let prior_hook = read_optional_regular_file(&hook_path)?;
    let prior_hook_active = prior_hook
        .as_ref()
        .is_some_and(|hook| is_executable_hook(&hook_path, &hook.bytes, hook.mode));
    let exe_path = hooks_dir.join("post-commit.exe");
    let prior_exe = read_optional_regular_file(&exe_path)?;
    let prior_exe_digest = prior_exe.as_ref().map(|hook| model::digest(&hook.bytes));
    let prior_exe_active = cfg!(windows) && !prior_hook_active && prior_exe.is_some();

    Ok(HookInstallPlan {
        repository_path,
        git_executable: git_executable.to_path_buf(),
        git_dir,
        hooks_dir,
        hook_path,
        prior_hook,
        prior_hook_active,
        prior_exe_active,
        prior_exe_digest,
    })
}

/// Create a stable source identity and private credential before any Store
/// request is sent. An unfinished request is resumed with its original source
/// identity and token; callers must never replace a ready pending credential.
pub fn prepare_source_credential(
    plan: &HookInstallPlan,
    project_id: &str,
    request_id: Option<&str>,
) -> Result<PreparedHookSource> {
    validate_setup_identity(project_id, "project ID")?;
    if let Some(request_id) = request_id {
        validate_setup_identity(request_id, "client request ID")?;
    }

    let sources_dir = plan.git_dir.join(SOURCE_DIRECTORY);
    ensure_private_directory(&sources_dir)?;
    let descriptor_path = pending_descriptor_path(&sources_dir, project_id);
    let mut descriptor = match read_pending_descriptor(&descriptor_path)? {
        Some(descriptor) => descriptor,
        None => {
            let descriptor = PendingSetupDescriptor {
                schema_version: PENDING_SETUP_SCHEMA_VERSION,
                client_request_id: request_id.map(str::to_owned).unwrap_or_else(model::new_id),
                project_id: project_id.to_owned(),
                source_id: model::new_id(),
                credential_ready: false,
            };
            validate_pending_descriptor(&descriptor)?;
            match write_private_new_json(&descriptor_path, &descriptor) {
                Ok(()) => descriptor,
                Err(error) => match read_pending_descriptor(&descriptor_path)? {
                    Some(existing) => existing,
                    None => return Err(error),
                },
            }
        }
    };
    validate_pending_descriptor(&descriptor)?;
    if descriptor.project_id != project_id
        || request_id.is_some_and(|request_id| request_id != descriptor.client_request_id)
    {
        return Err(Error::new(
            "HOOK_SETUP_PENDING_CONFLICT",
            "another hook setup request is pending for this project",
        ));
    }

    let source_dir = source_dir(&plan.git_dir, &descriptor.source_id);
    ensure_private_directory(&source_dir)?;
    let source_descriptor_path = source_dir.join(SETUP_REQUEST_FILE);
    let source_descriptor = PendingSetupDescriptor {
        credential_ready: descriptor.credential_ready,
        ..descriptor.clone()
    };
    match read_pending_descriptor(&source_descriptor_path)? {
        Some(existing) if same_pending_identity(&existing, &descriptor) => {}
        Some(_) => {
            return Err(Error::new(
                "HOOK_SETUP_PENDING_CONFLICT",
                "hook source directory belongs to a different setup request",
            ));
        }
        None => write_private_new_json(&source_descriptor_path, &source_descriptor)?,
    }

    let credential_path = source_dir.join("credential.json");
    let credential = if descriptor.credential_ready {
        let credential = read_hook_credential(&credential_path, &descriptor.source_id)?;
        validate_hook_credential(&descriptor.source_id, &credential)?;
        credential
    } else {
        // The not-ready state proves no caller could have sent this token.
        // Reuse a complete file left by an interrupted write; never replace it.
        let credential = match fs::symlink_metadata(&credential_path) {
            Ok(_) => read_hook_credential(&credential_path, &descriptor.source_id)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let generated = Credential {
                    client_id: format!("hook-source:{}", descriptor.source_id),
                    token: format!("{}{}", model::new_id(), model::new_id()),
                };
                validate_hook_credential(&descriptor.source_id, &generated)?;
                match write_private_secret_new(&credential_path, &serde_json::to_vec(&generated)?) {
                    Ok(()) => generated,
                    Err(write_error) => {
                        // Another local setup process may have won create_new.
                        // Accept only the exact canonical private credential.
                        read_hook_credential(&credential_path, &descriptor.source_id)
                            .map_err(|_| write_error)?
                    }
                }
            }
            Err(error) => return Err(error.into()),
        };
        validate_hook_credential(&descriptor.source_id, &credential)?;
        descriptor.credential_ready = true;
        private_replace_json(&descriptor_path, &descriptor)?;
        credential
    };

    Ok(PreparedHookSource {
        client_request_id: descriptor.client_request_id,
        project_id: descriptor.project_id,
        source_id: descriptor.source_id,
        credential,
        credential_path,
        pending_descriptor_path: descriptor_path,
    })
}

/// Finish local setup after Store registration and exact Git readback succeed.
/// The installed credential remains at the wrapper path; only retry metadata
/// is removed.
pub fn complete_source_setup(plan: &HookInstallPlan, prepared: &PreparedHookSource) -> Result<()> {
    validate_prepared_source(plan, prepared)?;
    let descriptor =
        read_pending_descriptor(&prepared.pending_descriptor_path)?.ok_or_else(|| {
            Error::new(
                "HOOK_SETUP_PENDING_MISSING",
                "pending hook setup identity is unavailable",
            )
        })?;
    if !descriptor.credential_ready || !same_prepared_identity(&descriptor, prepared) {
        return Err(Error::new(
            "HOOK_SETUP_PENDING_CONFLICT",
            "pending hook setup identity changed before completion",
        ));
    }
    let source_request = source_dir(&plan.git_dir, &prepared.source_id).join(SETUP_REQUEST_FILE);
    let source_descriptor = read_pending_descriptor(&source_request)?.ok_or_else(|| {
        Error::new(
            "HOOK_SETUP_PENDING_MISSING",
            "hook source setup descriptor is unavailable",
        )
    })?;
    if !same_prepared_identity(&source_descriptor, prepared) {
        return Err(Error::new(
            "HOOK_SETUP_PENDING_CONFLICT",
            "hook source setup descriptor changed before completion",
        ));
    }
    let saved = read_hook_credential(&prepared.credential_path, &prepared.source_id)?;
    if !same_credential(&saved, &prepared.credential) {
        return Err(Error::new(
            "HOOK_CREDENTIAL_FILE_INVALID",
            "private hook credential changed before setup completion",
        ));
    }
    remove_regular_file_if_exists(
        &prepared.pending_descriptor_path,
        "HOOK_SETUP_PENDING_INVALID",
    )?;
    remove_regular_file_if_exists(&source_request, "HOOK_SETUP_PENDING_INVALID")?;
    Ok(())
}

/// Remove only the matching pre-IPC credential and setup descriptors after a
/// definitive Store rejection or confirmed server revocation. This never
/// edits a Git hook or its preservation backup.
pub fn discard_source_setup(plan: &HookInstallPlan, prepared: &PreparedHookSource) -> Result<()> {
    validate_prepared_source(plan, prepared)?;
    match fs::symlink_metadata(&prepared.credential_path) {
        Ok(_) => {
            let saved = read_hook_credential(&prepared.credential_path, &prepared.source_id)?;
            if !same_credential(&saved, &prepared.credential) {
                return Err(Error::new(
                    "HOOK_CREDENTIAL_FILE_INVALID",
                    "private hook credential changed before setup discard",
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    remove_matching_setup_descriptors(plan, prepared)?;
    remove_credential_file(&prepared.credential_path)
}

/// Apply a previewed local wrapper. The operation re-runs preflight and refuses
/// hook/config drift; the exact previous extensionless hook is retained and
/// chained only if Git would have executed it before this installation.
pub fn apply_post_commit(
    plan: &HookInstallPlan,
    source_id: &str,
    credential: &Credential,
    swarm_executable: &Path,
) -> Result<HookInstallReadback> {
    validate_hook_credential(source_id, credential)?;
    if !swarm_executable.is_absolute() || !swarm_executable.is_file() {
        return Err(Error::new(
            "HOOK_EXECUTABLE_UNAVAILABLE",
            "the current Swarm executable is not an absolute existing file",
        ));
    }
    let current = preview_post_commit(&plan.repository_path, &plan.git_executable)?;
    if !same_preview(plan, &current) {
        return Err(Error::new(
            "HOOK_INSTALL_PLAN_STALE",
            "repository hook state changed after preview; preview again before apply",
        ));
    }

    let source_dir = source_dir(&plan.git_dir, source_id);
    ensure_existing_directory(&source_dir)?;
    let credential_path = source_dir.join("credential.json");
    let saved_credential = read_hook_credential(&credential_path, source_id)?;
    if saved_credential.client_id != credential.client_id
        || saved_credential.token != credential.token
    {
        return Err(Error::new(
            "HOOK_CREDENTIAL_SCOPE_INVALID",
            "prepared hook credential differs from the private source credential",
        ));
    }

    let existing = readback_post_commit(&plan.repository_path, &plan.git_executable, source_id)?;
    if existing.state == "installing" {
        return resume_install_post_commit(
            &current,
            source_id,
            swarm_executable,
            &source_dir,
            &credential_path,
        );
    }
    if existing.state != "absent" {
        if existing.wrapper_matches && existing.state == "installed" {
            return Ok(existing);
        }
        return Err(Error::new(
            "HOOK_INSTALL_STATE_CONFLICT",
            "source installation already has a different or inactive wrapper state",
        ));
    }

    let previous_path = plan
        .prior_hook
        .as_ref()
        .map(|_| source_dir.join("previous-post-commit"));
    let chained_path = if plan.prior_hook_active {
        previous_path.clone()
    } else if plan.prior_exe_active {
        Some(plan.hooks_dir.join("post-commit.exe"))
    } else {
        None
    };
    if let (Some(previous), Some(hook)) = (&previous_path, &plan.prior_hook) {
        match fs::symlink_metadata(previous) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(Error::new(
                    "HOOK_FILE_INVALID",
                    "preserved hook backup is not a regular file",
                ));
            }
            Ok(_) => {
                let saved = read_bounded(previous)?;
                if model::digest(&saved) != hook.sha256 {
                    return Err(Error::new(
                        "HOOK_INSTALL_STATE_CONFLICT",
                        "existing preserved hook backup differs from the preview",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                write_preserving_mode(previous, &hook.bytes, hook.mode)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    let wrapper = wrapper_script(
        source_id,
        &plan.repository_path,
        &plan.git_executable,
        &credential_path,
        swarm_executable,
        chained_path.as_deref(),
    )?;
    let wrapper_digest = model::digest(&wrapper);
    let manifest = InstallManifest {
        schema_version: INSTALL_SCHEMA_VERSION,
        phase: InstallPhase::Installing,
        source_id: source_id.to_owned(),
        event: EVENT_NAME.to_owned(),
        repository_path: path_string(&plan.repository_path)?,
        git_executable: path_string(&plan.git_executable)?,
        git_dir: path_string(&plan.git_dir)?,
        hook_path: path_string(&plan.hook_path)?,
        wrapper_sha256: wrapper_digest.clone(),
        previous_hook_path: previous_path.as_deref().map(path_string).transpose()?,
        previous_hook_sha256: plan.prior_hook.as_ref().map(|hook| hook.sha256.clone()),
        previous_hook_mode: plan.prior_hook.as_ref().and_then(|hook| hook.mode),
        previous_hook_was_active: plan.prior_hook_active,
        chained_hook_path: chained_path.as_deref().map(path_string).transpose()?,
        chained_hook_sha256: if plan.prior_hook_active {
            plan.prior_hook.as_ref().map(|hook| hook.sha256.clone())
        } else if plan.prior_exe_active {
            plan.prior_exe_digest.clone()
        } else {
            None
        },
        credential_path: path_string(&credential_path)?,
        installed_at_ms: model::now_ms()?,
    };
    let manifest_path = source_dir.join("install.json");
    write_new(
        &manifest_path,
        &serde_json::to_vec_pretty(&manifest)?,
        Some(0o600),
    )?;

    publish_wrapper(&plan.hooks_dir, &plan.hook_path, &wrapper)?;
    finalize_installation(
        &plan.repository_path,
        &plan.git_executable,
        source_id,
        &manifest_path,
        &wrapper_digest,
    )
}

fn resume_install_post_commit(
    plan: &HookInstallPlan,
    source_id: &str,
    swarm_executable: &Path,
    source_dir: &Path,
    credential_path: &Path,
) -> Result<HookInstallReadback> {
    let manifest_path = source_dir.join("install.json");
    let manifest = read_install_manifest(&manifest_path)?;
    validate_install_manifest(
        &manifest,
        source_id,
        &plan.repository_path,
        &plan.git_executable,
        &plan.git_dir,
        source_dir,
    )?;
    if manifest.phase != InstallPhase::Installing {
        return Err(Error::new(
            "HOOK_INSTALL_STATE_CONFLICT",
            "source manifest is not in an install-recovery phase",
        ));
    }
    if manifest.source_id != source_id
        || manifest.credential_path != path_string(credential_path)?
        || manifest.hook_path != path_string(&plan.hook_path)?
        || plan.hook_path != plan.hooks_dir.join(HOOK_NAME)
    {
        return Err(Error::new(
            "HOOK_INSTALL_STATE_CONFLICT",
            "install recovery does not match the retained source identity",
        ));
    }
    let readback = readback_post_commit(&plan.repository_path, &plan.git_executable, source_id)?;
    if readback.state != "installing"
        || (manifest.previous_hook_path.is_some() && readback.backup_matches != Some(true))
    {
        return Err(Error::new(
            "HOOK_INSTALL_STATE_CONFLICT",
            "install recovery no longer matches the retained wrapper and backup evidence",
        ));
    }

    let current = read_optional_regular_file(&plan.hook_path)?;
    if current
        .as_ref()
        .is_some_and(|hook| hook.sha256 == manifest.wrapper_sha256)
    {
        return finalize_installation(
            &plan.repository_path,
            &plan.git_executable,
            source_id,
            &manifest_path,
            &manifest.wrapper_sha256,
        );
    }

    let wrapper = wrapper_script(
        source_id,
        &plan.repository_path,
        &plan.git_executable,
        credential_path,
        swarm_executable,
        manifest.chained_hook_path.as_deref().map(Path::new),
    )?;
    if model::digest(&wrapper) != manifest.wrapper_sha256 {
        return Err(Error::new(
            "HOOK_INSTALL_STATE_CONFLICT",
            "current executable does not reproduce the retained wrapper identity",
        ));
    }

    let target_is_resumable = match manifest.previous_hook_sha256.as_deref() {
        Some(previous_digest) => current.as_ref().is_some_and(|hook| {
            (hook.sha256 == previous_digest && hook.mode == manifest.previous_hook_mode)
                || hook.sha256 == manifest.wrapper_sha256
        }),
        None => current
            .as_ref()
            .is_none_or(|hook| hook.sha256 == manifest.wrapper_sha256),
    };
    if !target_is_resumable {
        return Err(Error::new(
            "HOOK_INSTALL_STATE_CONFLICT",
            "hook path changed outside the exact retained install transition",
        ));
    }
    if !current
        .as_ref()
        .is_some_and(|hook| hook.sha256 == manifest.wrapper_sha256)
    {
        publish_wrapper(&plan.hooks_dir, &plan.hook_path, &wrapper)?;
    }

    finalize_installation(
        &plan.repository_path,
        &plan.git_executable,
        source_id,
        &manifest_path,
        &manifest.wrapper_sha256,
    )
}

fn publish_wrapper(hooks_dir: &Path, hook_path: &Path, wrapper: &[u8]) -> Result<()> {
    let wrapper_tmp = hooks_dir.join(format!("post-commit.eliot-tmp-{}", model::new_id()));
    ensure_hook_directory(hooks_dir)?;
    if let Err(error) = write_new(&wrapper_tmp, wrapper, Some(0o755))
        .and_then(|()| atomic_replace(&wrapper_tmp, hook_path))
    {
        let _ = fs::remove_file(&wrapper_tmp);
        return Err(error);
    }
    Ok(())
}

fn finalize_installation(
    repository_path: &Path,
    git_executable: &Path,
    source_id: &str,
    manifest_path: &Path,
    expected_wrapper_sha256: &str,
) -> Result<HookInstallReadback> {
    let readback = readback_post_commit(repository_path, git_executable, source_id)?;
    if readback.state != "installing"
        || !readback.wrapper_matches
        || readback.wrapper_sha256.as_deref() != Some(expected_wrapper_sha256)
    {
        return Err(Error::new(
            "HOOK_INSTALL_READBACK_FAILED",
            "Git hook wrapper did not match the retained installing manifest",
        ));
    }
    let mut manifest = read_install_manifest(manifest_path)?;
    if manifest.phase != InstallPhase::Installing
        || manifest.source_id != source_id
        || manifest.wrapper_sha256 != expected_wrapper_sha256
    {
        return Err(Error::new(
            "HOOK_INSTALL_STATE_CONFLICT",
            "install completion no longer matches the retained source manifest",
        ));
    }
    manifest.phase = InstallPhase::Installed;
    private_replace_json(manifest_path, &manifest)?;
    let readback = readback_post_commit(repository_path, git_executable, source_id)?;
    if readback.state != "installed" || !readback.wrapper_matches {
        return Err(Error::new(
            "HOOK_INSTALL_READBACK_FAILED",
            "Git hook wrapper did not match the completed install manifest",
        ));
    }
    Ok(readback)
}

/// Read the repository-local wrapper and its preservation manifest without
/// exposing the credential bytes.
pub fn readback_post_commit(
    repository_path: &Path,
    git_executable: &Path,
    source_id: &str,
) -> Result<HookInstallReadback> {
    validate_source_id(source_id)?;
    validate_git_executable(git_executable)?;
    let canonical = fs::canonicalize(repository_path).map_err(|_| {
        Error::new(
            "HOOK_REPOSITORY_UNAVAILABLE",
            "repository path is unavailable",
        )
    })?;
    let git_dir = canonical_output_path(&git_stdout(
        git_executable,
        &canonical,
        &["rev-parse", "--absolute-git-dir"],
    )?)?;
    let source_dir = source_dir(&git_dir, source_id);
    match fs::symlink_metadata(&source_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(Error::new(
                "HOOK_DIRECTORY_INVALID",
                "hook source storage path is not a regular directory",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return absent_readback(&canonical, &git_dir, source_id);
        }
        Err(error) => return Err(error.into()),
    }
    let manifest_path = source_dir.join("install.json");
    match fs::symlink_metadata(&manifest_path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(Error::new(
                "HOOK_INSTALL_MANIFEST_INVALID",
                "hook install manifest is not a regular file",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let hooks_dir = git_dir.join("hooks");
            return Ok(HookInstallReadback {
                source_id: source_id.to_owned(),
                event: EVENT_NAME.to_owned(),
                state: "absent".to_owned(),
                repository_root: path_string(&canonical)?,
                hook_path: path_string(&hooks_dir.join(HOOK_NAME))?,
                wrapper_sha256: None,
                wrapper_matches: false,
                backup_matches: None,
                credential_file_present: source_dir.join("credential.json").is_file(),
                message: "no source installation manifest is present".to_owned(),
            });
        }
        Err(error) => return Err(error.into()),
    }
    let manifest = read_install_manifest(&manifest_path)?;
    validate_install_manifest(
        &manifest,
        source_id,
        &canonical,
        git_executable,
        &git_dir,
        &source_dir,
    )?;
    let hook_path = PathBuf::from(&manifest.hook_path);
    let current_wrapper = read_optional_regular_file(&hook_path)?;
    let wrapper_matches = current_wrapper
        .as_ref()
        .is_some_and(|hook| hook.sha256 == manifest.wrapper_sha256);
    let backup_matches = match (&manifest.previous_hook_path, &manifest.previous_hook_sha256) {
        (Some(path), Some(_)) => read_optional_regular_file(Path::new(path))?
            .as_ref()
            .map(|backup| matches_saved_previous(backup, &manifest)),
        (None, None) => None,
        _ => {
            return Err(Error::new(
                "HOOK_INSTALL_MANIFEST_INVALID",
                "hook backup fields are incomplete",
            ));
        }
    };
    let original_hook_matches = current_wrapper
        .as_ref()
        .is_some_and(|hook| matches_saved_previous(hook, &manifest));
    let no_original_hook = manifest.previous_hook_path.is_none();
    let backup_is_exact = if no_original_hook {
        backup_matches.is_none()
    } else {
        backup_matches == Some(true)
    };
    let hooks_path_is_custom = match run_git(
        git_executable,
        &canonical,
        &["config", "--get", "core.hooksPath"],
    )? {
        output if output.status.success() => true,
        output if output.status.code() == Some(1) => false,
        _ => {
            return Err(Error::new(
                "HOOK_GIT_CONFIG_READBACK_FAILED",
                "Git could not safely determine the effective core.hooksPath setting",
            ));
        }
    };
    let state = match manifest.phase {
        InstallPhase::Installing => {
            let target_is_exact = if no_original_hook {
                current_wrapper.is_none() || wrapper_matches
            } else {
                original_hook_matches || wrapper_matches
            };
            if backup_is_exact && target_is_exact {
                "installing"
            } else {
                "modified"
            }
        }
        InstallPhase::Installed => {
            if wrapper_matches && backup_is_exact && !hooks_path_is_custom {
                "installed"
            } else if wrapper_matches && backup_is_exact {
                "inactive"
            } else {
                "modified"
            }
        }
        InstallPhase::Revoking => {
            let target_is_exact = if no_original_hook {
                current_wrapper.is_none() || wrapper_matches
            } else {
                let backup_can_restore = backup_matches == Some(true);
                let backup_already_consumed = backup_matches.is_none() && original_hook_matches;
                backup_already_consumed
                    || (backup_can_restore
                        && (current_wrapper.is_none() || wrapper_matches || original_hook_matches))
            };
            if target_is_exact && backup_is_exact_for_revocation(backup_matches, &manifest) {
                "revoking"
            } else {
                "modified"
            }
        }
    };
    Ok(HookInstallReadback {
        source_id: source_id.to_owned(),
        event: EVENT_NAME.to_owned(),
        state: state.to_owned(),
        repository_root: manifest.repository_path,
        hook_path: manifest.hook_path,
        wrapper_sha256: Some(manifest.wrapper_sha256),
        wrapper_matches,
        backup_matches,
        credential_file_present: Path::new(&manifest.credential_path).is_file(),
        message: match state {
            "installed" => {
                "installed wrapper and preserved hook match their readback digests".to_owned()
            }
            "inactive" => {
                "wrapper bytes match, but core.hooksPath redirects Git elsewhere".to_owned()
            }
            "installing" => {
                "installation is durably pending exact wrapper publication or finalization"
                    .to_owned()
            }
            "revoking" => "revocation is durably pending exact restoration or cleanup".to_owned(),
            _ => "hook or preserved user hook changed; automatic restoration is refused".to_owned(),
        },
    })
}

/// Restore the exact previous extensionless hook only while the installed
/// wrapper and backup still match their recorded digests. The caller should
/// revoke the Store credential first so a failed local restore leaves an inert
/// wrapper rather than an active source.
pub fn revoke_post_commit(
    repository_path: &Path,
    git_executable: &Path,
    source_id: &str,
) -> Result<HookInstallReadback> {
    validate_source_id(source_id)?;
    let before = match readback_post_commit(repository_path, git_executable, source_id) {
        Ok(readback) => readback,
        Err(error) => {
            remove_source_credential(repository_path, git_executable, source_id)?;
            return Err(error);
        }
    };
    let canonical = fs::canonicalize(repository_path)?;
    let git_dir = canonical_output_path(&git_stdout(
        git_executable,
        &canonical,
        &["rev-parse", "--absolute-git-dir"],
    )?)?;
    let source_dir = source_dir(&git_dir, source_id);
    if before.state == "absent" {
        remove_source_credential(repository_path, git_executable, source_id)?;
        remove_empty_source_dir_if_present(&source_dir)?;
        return readback_post_commit(repository_path, git_executable, source_id);
    }
    ensure_existing_directory(&source_dir)?;
    let manifest_path = source_dir.join("install.json");
    let mut manifest = read_install_manifest(&manifest_path)?;
    validate_install_manifest(
        &manifest,
        source_id,
        &canonical,
        git_executable,
        &git_dir,
        &source_dir,
    )?;
    if manifest.phase != InstallPhase::Revoking {
        if before.state == "modified" {
            remove_source_credential(repository_path, git_executable, source_id)?;
            return readback_post_commit(repository_path, git_executable, source_id);
        }
        let resumable_phase = (manifest.phase == InstallPhase::Installing
            && before.state == "installing")
            || (manifest.phase == InstallPhase::Installed
                && matches!(before.state.as_str(), "installed" | "inactive"));
        if !resumable_phase {
            return Err(Error::new(
                "HOOK_INSTALL_STATE_CONFLICT",
                "local hook state does not authorize revocation recovery",
            ));
        }
        manifest.phase = InstallPhase::Revoking;
        private_replace_json(&manifest_path, &manifest)?;
    }
    finish_revocation(
        repository_path,
        git_executable,
        source_id,
        &source_dir,
        &manifest,
        before,
    )
}

fn finish_revocation(
    repository_path: &Path,
    git_executable: &Path,
    source_id: &str,
    source_dir: &Path,
    manifest: &InstallManifest,
    before: HookInstallReadback,
) -> Result<HookInstallReadback> {
    if manifest.phase != InstallPhase::Revoking {
        return Err(Error::new(
            "HOOK_INSTALL_STATE_CONFLICT",
            "local hook manifest is not in a revocation-recovery phase",
        ));
    }
    let manifest_path = source_dir.join("install.json");
    let target = PathBuf::from(&manifest.hook_path);
    let current = read_optional_regular_file(&target)?;
    match (
        manifest.previous_hook_path.as_deref(),
        manifest.previous_hook_sha256.as_deref(),
    ) {
        (Some(previous), Some(_)) => {
            let previous_path = PathBuf::from(previous);
            let backup = read_optional_regular_file(&previous_path)?;
            if backup
                .as_ref()
                .is_some_and(|hook| !matches_saved_previous(hook, manifest))
            {
                return revocation_conflict(repository_path, git_executable, source_id);
            }
            let target_is_original = current
                .as_ref()
                .is_some_and(|hook| matches_saved_previous(hook, manifest));
            let target_is_wrapper = current
                .as_ref()
                .is_some_and(|hook| hook.sha256 == manifest.wrapper_sha256);
            match (backup.as_ref(), current.as_ref()) {
                (Some(_), Some(_)) if target_is_original => {}
                (Some(backup), Some(_)) if target_is_wrapper => {
                    atomic_replace_bytes(&target, &backup.bytes, manifest.previous_hook_mode)?;
                }
                (Some(backup), None) => {
                    atomic_replace_bytes(&target, &backup.bytes, manifest.previous_hook_mode)?;
                }
                (None, Some(_)) if target_is_original => {}
                _ => return revocation_conflict(repository_path, git_executable, source_id),
            }
            let restored = read_optional_regular_file(&target)?;
            if !restored
                .as_ref()
                .is_some_and(|hook| matches_saved_previous(hook, manifest))
            {
                return revocation_conflict(repository_path, git_executable, source_id);
            }
            if backup.is_some() {
                let retained = read_optional_regular_file(&previous_path)?;
                if !retained
                    .as_ref()
                    .is_some_and(|hook| matches_saved_previous(hook, manifest))
                {
                    return revocation_conflict(repository_path, git_executable, source_id);
                }
                fs::remove_file(previous_path)?;
            }
        }
        (None, None) => match current.as_ref() {
            Some(hook) if hook.sha256 == manifest.wrapper_sha256 => fs::remove_file(&target)?,
            None => {}
            Some(_) => return revocation_conflict(repository_path, git_executable, source_id),
        },
        _ => {
            return Err(Error::new(
                "HOOK_INSTALL_MANIFEST_INVALID",
                "hook backup fields are incomplete",
            ));
        }
    }

    remove_source_credential(repository_path, git_executable, source_id)?;
    ensure_source_dir_contains_only_manifest(source_dir, &manifest_path)?;
    fs::remove_file(&manifest_path)?;
    remove_empty_source_dir_if_present(source_dir)?;
    Ok(HookInstallReadback {
        state: "restored".to_owned(),
        wrapper_matches: false,
        credential_file_present: false,
        backup_matches: None,
        wrapper_sha256: before.wrapper_sha256,
        message: "the original hook was restored and the setup credential file removed".to_owned(),
        ..before
    })
}

fn revocation_conflict(
    repository_path: &Path,
    git_executable: &Path,
    source_id: &str,
) -> Result<HookInstallReadback> {
    remove_source_credential(repository_path, git_executable, source_id)?;
    readback_post_commit(repository_path, git_executable, source_id)
}

fn ensure_source_dir_contains_only_manifest(source_dir: &Path, manifest_path: &Path) -> Result<()> {
    for entry in fs::read_dir(source_dir)? {
        let entry = entry?;
        if entry.path().as_path() != manifest_path {
            return Err(Error::new(
                "HOOK_INSTALL_CLEANUP_PENDING",
                "unexpected source files remain; retained revocation state was preserved",
            ));
        }
    }
    Ok(())
}

fn remove_empty_source_dir_if_present(source_dir: &Path) -> Result<()> {
    match fs::symlink_metadata(source_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(Error::new(
            "HOOK_DIRECTORY_INVALID",
            "hook source storage path is not a regular directory",
        )),
        Ok(_) => match fs::remove_dir(source_dir) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// Verify the exact captured commit against the setup-bound checkout. This is
/// intentionally a local object lookup only: it never fetches, invokes Git
/// hooks, or accepts a repository path from the event request.
pub fn resolve_commit(
    scope: &crate::hooks::contract::EmitScope,
    commit_oid: &str,
) -> Result<GitCommitSnapshot> {
    validate_git_executable(&scope.git_executable)?;
    if !crate::forge::valid_object_id(commit_oid) {
        return Err(Error::invalid("commit_oid must be a full Git object ID"));
    }
    let repository_path = fs::canonicalize(&scope.repository_path).map_err(|_| {
        Error::new(
            "HOOK_REPOSITORY_UNAVAILABLE",
            "configured repository path is unavailable",
        )
    })?;
    if repository_path != scope.repository_path {
        return Err(Error::new(
            "HOOK_REPOSITORY_MISMATCH",
            "configured repository path changed after source setup",
        ));
    }
    let typed_commit = format!("{commit_oid}^{{commit}}");
    let exists = run_git(
        &scope.git_executable,
        &repository_path,
        &["cat-file", "-e", &typed_commit],
    )?;
    if !exists.status.success() {
        return Err(Error::new(
            "HOOK_COMMIT_NOT_FOUND",
            "captured commit is not a commit object in the configured repository",
        ));
    }
    let resolved = git_stdout(
        &scope.git_executable,
        &repository_path,
        &["rev-parse", "--verify", &typed_commit],
    )?;
    let resolved = resolved.trim();
    if !crate::forge::valid_object_id(resolved) || !resolved.eq_ignore_ascii_case(commit_oid) {
        return Err(Error::new(
            "HOOK_COMMIT_READBACK_MISMATCH",
            "Git did not resolve the captured object ID to the same commit",
        ));
    }
    Ok(GitCommitSnapshot {
        commit_oid: resolved.to_ascii_lowercase(),
    })
}

fn same_preview(before: &HookInstallPlan, after: &HookInstallPlan) -> bool {
    before.repository_path == after.repository_path
        && before.git_dir == after.git_dir
        && before.hooks_dir == after.hooks_dir
        && before.hook_path == after.hook_path
        && before
            .prior_hook
            .as_ref()
            .map(|hook| (&hook.sha256, hook.mode))
            == after
                .prior_hook
                .as_ref()
                .map(|hook| (&hook.sha256, hook.mode))
        && before.prior_hook_active == after.prior_hook_active
        && before.prior_exe_active == after.prior_exe_active
        && before.prior_exe_digest == after.prior_exe_digest
}

fn wrapper_script(
    source_id: &str,
    repository_path: &Path,
    git_executable: &Path,
    credential_path: &Path,
    swarm_executable: &Path,
    chained_hook: Option<&Path>,
) -> Result<Vec<u8>> {
    let mut script = String::from("#!/bin/sh\n");
    script.push_str("# ELIOT-HOOK-SOURCE:");
    script.push_str(source_id);
    script.push('\n');
    script.push_str("eliot_status=0\n");
    // Capture before chaining a user hook: it may move HEAD, while this
    // callback must identify the commit that invoked post-commit.
    script.push_str("eliot_commit_oid=$(");
    script.push_str("unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_COMMON_DIR GIT_NAMESPACE GIT_CONFIG GIT_CONFIG_PARAMETERS GIT_CONFIG_COUNT GIT_SSH GIT_SSH_COMMAND GIT_ASKPASS SSH_ASKPASS; ");
    script.push_str(&shell_quote(&path_for_shell(git_executable)));
    script.push_str(" --no-optional-locks --no-replace-objects -C ");
    script.push_str(&shell_quote(&path_for_shell(repository_path)));
    script.push_str(" rev-parse --verify 'HEAD^{commit}' 2>/dev/null) || eliot_commit_oid=''\n");
    if let Some(chained_hook) = chained_hook {
        script.push_str(&format!(
            "{} \"$@\" || eliot_status=$?\n",
            shell_quote(&path_for_shell(chained_hook))
        ));
    }
    script.push_str("if [ -n \"$eliot_commit_oid\" ]; then\n");
    script.push_str(&shell_quote(&path_for_shell(swarm_executable)));
    script.push_str(" --credential ");
    script.push_str(&shell_quote(&path_for_shell(credential_path)));
    script.push_str(" hook emit --source-id ");
    script.push_str(&shell_quote(source_id));
    script.push_str(" --commit-oid \"$eliot_commit_oid\" </dev/null >/dev/null 2>&1 &\nfi\n");
    script.push_str("exit \"$eliot_status\"\n");
    Ok(script.into_bytes())
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn path_for_shell(path: &Path) -> String {
    let value = path.to_string_lossy();
    #[cfg(windows)]
    {
        value.replace('\\', "/")
    }
    #[cfg(not(windows))]
    {
        value.into_owned()
    }
}

fn git_stdout(executable: &Path, repository: &Path, args: &[&str]) -> Result<String> {
    let output = run_git(executable, repository, args)?;
    if !output.status.success() {
        return Err(Error::new(
            "HOOK_GIT_READBACK_FAILED",
            "Git repository readback failed or exceeded its output bound",
        ));
    }
    String::from_utf8(output.stdout).map_err(|_| {
        Error::new(
            "HOOK_GIT_READBACK_FAILED",
            "Git returned non-UTF-8 repository data",
        )
    })
}

fn run_git(executable: &Path, repository: &Path, args: &[&str]) -> Result<Output> {
    let mut command = Command::new(executable);
    command
        .args(["--no-optional-locks", "--no-replace-objects", "-C"])
        .arg(repository)
        .args(args)
        .current_dir(repository)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_OPTIONAL_LOCKS", "0");
    for key in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_NAMESPACE",
        "GIT_CONFIG",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_COUNT",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
    ] {
        command.env_remove(key);
    }
    for (key, _) in std::env::vars_os() {
        if key.to_str().is_some_and(|key| {
            key.starts_with("GIT_CONFIG_KEY_")
                || key.starts_with("GIT_CONFIG_VALUE_")
                || key.starts_with("GIT_TRACE")
                || key == "GIT_CURL_VERBOSE"
        }) {
            command.env_remove(key);
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }

    let mut child = command.spawn().map_err(|_| {
        Error::new(
            "HOOK_GIT_START",
            "configured Git executable could not start",
        )
    })?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("HOOK_GIT_IO", "Git stdout pipe is unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| Error::new("HOOK_GIT_IO", "Git stderr pipe is unavailable"))?;
    let stdout_reader = thread::spawn(move || read_capped(stdout, MAX_GIT_OUTPUT_BYTES));
    let stderr_reader = thread::spawn(move || read_capped(stderr, MAX_GIT_OUTPUT_BYTES));
    let deadline = Instant::now() + GIT_COMMAND_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(15)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::new(
                    "HOOK_GIT_TIMEOUT",
                    "bounded Git readback timed out",
                ));
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::new(
                    "HOOK_GIT_WAIT",
                    "Git process status was unavailable",
                ));
            }
        }
    };
    let (stdout, stdout_truncated) = stdout_reader
        .join()
        .map_err(|_| Error::new("HOOK_GIT_OUTPUT", "Git stdout reader failed"))??;
    let (stderr, stderr_truncated) = stderr_reader
        .join()
        .map_err(|_| Error::new("HOOK_GIT_OUTPUT", "Git stderr reader failed"))??;
    let mut output = Output {
        status,
        stdout,
        stderr,
    };
    if stdout_truncated || stderr_truncated {
        // Callers need to distinguish a complete empty result from a cap hit.
        // Retain that fact out of band by refusing all oversized output here.
        return Err(Error::new(
            "HOOK_GIT_OUTPUT_LIMIT",
            "Git readback exceeded the fixed output bound",
        ));
    }
    output.stdout.shrink_to_fit();
    Ok(output)
}

fn read_capped<R: Read>(mut reader: R, cap: usize) -> std::io::Result<(Vec<u8>, bool)> {
    let mut output = Vec::with_capacity(cap.min(1024));
    let mut buffer = [0u8; 1024];
    let mut truncated = false;
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let remaining = cap.saturating_sub(output.len());
        let kept = remaining.min(count);
        output.extend_from_slice(&buffer[..kept]);
        truncated |= kept != count;
    }
    Ok((output, truncated))
}

fn canonical_output_path(value: &str) -> Result<PathBuf> {
    let output = value.trim();
    if output.is_empty() || output.len() > 4096 || output.chars().any(char::is_control) {
        return Err(Error::new(
            "HOOK_GIT_READBACK_FAILED",
            "Git path readback is invalid",
        ));
    }
    fs::canonicalize(output).map_err(|_| {
        Error::new(
            "HOOK_GIT_READBACK_FAILED",
            "Git path could not be canonicalized",
        )
    })
}

fn read_optional_regular_file(path: &Path) -> Result<Option<PreviousHook>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(Error::new(
                    "HOOK_FILE_INVALID",
                    "existing Git hook must be a regular nonsymlink file",
                ));
            }
            if metadata.len() > MAX_HOOK_FILE_BYTES {
                return Err(Error::new(
                    "HOOK_FILE_TOO_LARGE",
                    "existing Git hook exceeds the preservation bound",
                ));
            }
            let bytes = fs::read(path)?;
            Ok(Some(PreviousHook {
                sha256: model::digest(&bytes),
                bytes,
                mode: file_mode(&metadata),
            }))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn is_executable_hook(path: &Path, bytes: &[u8], mode: Option<u32>) -> bool {
    #[cfg(unix)]
    {
        let _ = (path, bytes);
        mode.is_some_and(|mode| mode & 0o111 != 0)
    }
    #[cfg(windows)]
    {
        let _ = mode;
        path.extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("exe"))
            || bytes.starts_with(b"#!")
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (path, bytes, mode);
        false
    }
}

#[cfg(unix)]
fn file_mode(metadata: &std::fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(metadata.permissions().mode() & 0o777)
}

#[cfg(not(unix))]
fn file_mode(_metadata: &std::fs::Metadata) -> Option<u32> {
    None
}

fn write_preserving_mode(path: &Path, bytes: &[u8], mode: Option<u32>) -> Result<()> {
    write_new(path, bytes, mode)
}

fn write_new(path: &Path, bytes: &[u8], mode: Option<u32>) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(mode.unwrap_or(0o600));
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    #[cfg(unix)]
    if let Some(mode) = mode {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    {
        let _ = mode;
    }
    Ok(())
}

fn atomic_replace(source: &Path, destination: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        use windows_sys::Win32::Storage::FileSystem::{
            MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
        };
        let source = wide_nul(source);
        let destination = wide_nul(destination);
        // SAFETY: both paths are nul-terminated UTF-16 strings kept alive for
        // the duration of this atomic same-volume replacement.
        let moved = unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if moved == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        fs::rename(source, destination)?;
        Ok(())
    }
}

fn atomic_replace_bytes(destination: &Path, bytes: &[u8], mode: Option<u32>) -> Result<()> {
    let parent = destination
        .parent()
        .ok_or_else(|| Error::invalid("hook destination has no parent directory"))?;
    let temporary = parent.join(format!(".eliot-restore-{}", model::new_id()));
    write_new(&temporary, bytes, mode)?;
    if let Err(error) = atomic_replace(&temporary, destination) {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(())
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > MAX_HOOK_FILE_BYTES
    {
        return Err(Error::new(
            "HOOK_FILE_INVALID",
            "hook metadata file is not a bounded regular file",
        ));
    }
    Ok(fs::read(path)?)
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(Error::new(
            "HOOK_DIRECTORY_INVALID",
            "hook source storage path is not a regular directory",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path)?;
            platform::private_permissions(path, true)
        }
        Err(error) => Err(error.into()),
    }
}

/// Require a directory that must already have been created by the setup path.
/// Readback and revoke use this non-mutating check rather than the setup helper.
fn ensure_existing_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(Error::new(
            "HOOK_DIRECTORY_INVALID",
            "hook source storage path is not a regular directory",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Err(Error::new(
            "HOOK_DIRECTORY_INVALID",
            "required hook source directory is missing",
        )),
        Err(error) => Err(error.into()),
    }
}

fn ensure_hook_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => Err(Error::new(
            "HOOK_DIRECTORY_INVALID",
            "resolved Git hook directory is not a regular directory",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn absent_readback(
    repository_path: &Path,
    git_dir: &Path,
    source_id: &str,
) -> Result<HookInstallReadback> {
    Ok(HookInstallReadback {
        source_id: source_id.to_owned(),
        event: EVENT_NAME.to_owned(),
        state: "absent".to_owned(),
        repository_root: path_string(repository_path)?,
        hook_path: path_string(&git_dir.join("hooks").join(HOOK_NAME))?,
        wrapper_sha256: None,
        wrapper_matches: false,
        backup_matches: None,
        credential_file_present: false,
        message: "no source installation manifest is present".to_owned(),
    })
}

fn remove_source_credential(
    repository_path: &Path,
    git_executable: &Path,
    source_id: &str,
) -> Result<()> {
    validate_git_executable(git_executable)?;
    let canonical = fs::canonicalize(repository_path).map_err(|_| {
        Error::new(
            "HOOK_REPOSITORY_UNAVAILABLE",
            "repository path is unavailable",
        )
    })?;
    let git_dir = canonical_output_path(&git_stdout(
        git_executable,
        &canonical,
        &["rev-parse", "--absolute-git-dir"],
    )?)?;
    let source_dir = source_dir(&git_dir, source_id);
    match fs::symlink_metadata(&source_dir) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
            return Err(Error::new(
                "HOOK_DIRECTORY_INVALID",
                "hook source storage path is not a regular directory",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    }
    let source_request = source_dir.join(SETUP_REQUEST_FILE);
    if let Some(descriptor) = read_pending_descriptor(&source_request)?
        && descriptor.source_id == source_id
    {
        let marker =
            pending_descriptor_path(&git_dir.join(SOURCE_DIRECTORY), &descriptor.project_id);
        if read_pending_descriptor(&marker)?
            .is_some_and(|pending| same_pending_identity(&pending, &descriptor))
        {
            remove_regular_file_if_exists(&marker, "HOOK_SETUP_PENDING_INVALID")?;
        }
        remove_regular_file_if_exists(&source_request, "HOOK_SETUP_PENDING_INVALID")?;
    }
    remove_credential_file(&source_dir.join("credential.json"))
}

fn remove_credential_file(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(Error::new(
                "HOOK_CREDENTIAL_FILE_INVALID",
                "hook credential path is not a regular file",
            ))
        }
        Ok(_) => {
            fs::remove_file(path)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn validate_manifest_paths(
    manifest: &InstallManifest,
    source_dir: &Path,
    git_dir: &Path,
) -> Result<()> {
    let expected_credential = path_string(&source_dir.join("credential.json"))?;
    let expected_previous = path_string(&source_dir.join("previous-post-commit"))?;
    let expected_chained_exe = path_string(&git_dir.join("hooks").join("post-commit.exe"))?;
    let backup_fields_match = match (
        manifest.previous_hook_path.as_deref(),
        manifest.previous_hook_sha256.as_deref(),
    ) {
        (Some(path), Some(digest)) => path == expected_previous && valid_sha256(digest),
        (None, None) => true,
        _ => false,
    };
    let chain_matches = match manifest.chained_hook_path.as_deref() {
        None => manifest.chained_hook_sha256.is_none(),
        Some(path) if Some(path) == manifest.previous_hook_path.as_deref() => {
            manifest.chained_hook_sha256 == manifest.previous_hook_sha256
        }
        Some(path) if path == expected_chained_exe => manifest
            .chained_hook_sha256
            .as_deref()
            .is_some_and(valid_sha256),
        _ => false,
    };
    if manifest.credential_path != expected_credential || !backup_fields_match || !chain_matches {
        return Err(Error::new(
            "HOOK_INSTALL_MANIFEST_INVALID",
            "hook manifest contains paths outside its source and Git hook directories",
        ));
    }
    Ok(())
}

fn read_install_manifest(path: &Path) -> Result<InstallManifest> {
    serde_json::from_slice(&read_bounded(path)?).map_err(|_| {
        Error::new(
            "HOOK_INSTALL_MANIFEST_INVALID",
            "hook install manifest is invalid",
        )
    })
}

fn validate_install_manifest(
    manifest: &InstallManifest,
    source_id: &str,
    repository_path: &Path,
    git_executable: &Path,
    git_dir: &Path,
    source_dir: &Path,
) -> Result<()> {
    if manifest.schema_version != INSTALL_SCHEMA_VERSION
        || manifest.source_id != source_id
        || manifest.event != EVENT_NAME
        || manifest.repository_path != path_string(repository_path)?
        || manifest.git_dir != path_string(git_dir)?
        || manifest.git_executable != path_string(git_executable)?
        || manifest.hook_path != path_string(&git_dir.join("hooks").join(HOOK_NAME))?
        || manifest.installed_at_ms < 0
        || !valid_sha256(&manifest.wrapper_sha256)
    {
        return Err(Error::new(
            "HOOK_INSTALL_MANIFEST_INVALID",
            "hook install manifest scope or digest is invalid",
        ));
    }
    validate_manifest_paths(manifest, source_dir, git_dir)
}

fn matches_saved_previous(hook: &PreviousHook, manifest: &InstallManifest) -> bool {
    manifest.previous_hook_sha256.as_deref() == Some(hook.sha256.as_str())
        && manifest.previous_hook_mode == hook.mode
}

fn backup_is_exact_for_revocation(
    backup_matches: Option<bool>,
    manifest: &InstallManifest,
) -> bool {
    if manifest.previous_hook_path.is_some() {
        backup_matches != Some(false)
    } else {
        backup_matches.is_none()
    }
}

fn source_dir(git_dir: &Path, source_id: &str) -> PathBuf {
    git_dir.join(SOURCE_DIRECTORY).join(source_id)
}

fn pending_descriptor_path(sources_dir: &Path, project_id: &str) -> PathBuf {
    sources_dir.join(format!(
        "setup-pending-{}.json",
        model::digest(project_id.as_bytes())
    ))
}

fn validate_setup_identity(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
        return Err(Error::invalid(format!("invalid hook {label}")));
    }
    Ok(())
}

fn validate_pending_descriptor(descriptor: &PendingSetupDescriptor) -> Result<()> {
    if descriptor.schema_version != PENDING_SETUP_SCHEMA_VERSION
        || descriptor.client_request_id.trim().is_empty()
        || descriptor.client_request_id.len() > 128
        || descriptor.client_request_id.chars().any(char::is_control)
        || descriptor.project_id.trim().is_empty()
        || descriptor.project_id.len() > 128
        || descriptor.project_id.chars().any(char::is_control)
        || !crate::hooks::contract::is_canonical_v4_uuid(&descriptor.source_id)
    {
        return Err(Error::new(
            "HOOK_SETUP_PENDING_INVALID",
            "pending hook setup identity is invalid",
        ));
    }
    Ok(())
}

fn read_pending_descriptor(path: &Path) -> Result<Option<PendingSetupDescriptor>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(Error::new(
                "HOOK_SETUP_PENDING_INVALID",
                "pending hook setup descriptor is not a regular file",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let descriptor: PendingSetupDescriptor =
        serde_json::from_slice(&read_bounded(path)?).map_err(|_| {
            Error::new(
                "HOOK_SETUP_PENDING_INVALID",
                "pending hook setup descriptor is invalid",
            )
        })?;
    validate_pending_descriptor(&descriptor)?;
    Ok(Some(descriptor))
}

fn write_private_new_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    platform::write_private_new(path, &serde_json::to_vec(value)?)
}

fn private_replace_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::invalid("pending hook setup path has no parent"))?;
    let temporary = parent.join(format!("setup-update-{}.tmp", model::new_id()));
    write_private_new_json(&temporary, value)?;
    if let Err(error) = atomic_replace(&temporary, path) {
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    Ok(())
}

fn same_pending_identity(left: &PendingSetupDescriptor, right: &PendingSetupDescriptor) -> bool {
    left.schema_version == right.schema_version
        && left.client_request_id == right.client_request_id
        && left.project_id == right.project_id
        && left.source_id == right.source_id
}

fn same_credential(left: &Credential, right: &Credential) -> bool {
    left.client_id == right.client_id && left.token == right.token
}

fn same_prepared_identity(
    descriptor: &PendingSetupDescriptor,
    prepared: &PreparedHookSource,
) -> bool {
    descriptor.client_request_id == prepared.client_request_id
        && descriptor.project_id == prepared.project_id
        && descriptor.source_id == prepared.source_id
}

fn validate_prepared_source(plan: &HookInstallPlan, prepared: &PreparedHookSource) -> Result<()> {
    validate_hook_credential(&prepared.source_id, &prepared.credential)?;
    validate_pending_descriptor(&PendingSetupDescriptor {
        schema_version: PENDING_SETUP_SCHEMA_VERSION,
        client_request_id: prepared.client_request_id.clone(),
        project_id: prepared.project_id.clone(),
        source_id: prepared.source_id.clone(),
        credential_ready: true,
    })?;
    let expected_source = source_dir(&plan.git_dir, &prepared.source_id);
    if prepared.credential_path != expected_source.join("credential.json")
        || prepared.pending_descriptor_path
            != pending_descriptor_path(&plan.git_dir.join(SOURCE_DIRECTORY), &prepared.project_id)
    {
        return Err(Error::new(
            "HOOK_SETUP_PENDING_CONFLICT",
            "prepared hook setup paths do not match this repository and source",
        ));
    }
    Ok(())
}

fn read_hook_credential(path: &Path, source_id: &str) -> Result<Credential> {
    let credential: Credential = serde_json::from_slice(&read_bounded(path)?).map_err(|_| {
        Error::new(
            "HOOK_CREDENTIAL_FILE_INVALID",
            "private hook credential file is invalid",
        )
    })?;
    validate_hook_credential(source_id, &credential)?;
    Ok(credential)
}

fn write_private_secret_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    let result = (|| {
        platform::private_permissions(path, false)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        drop(file);
        let _ = fs::remove_file(path);
    }
    result
}

fn remove_regular_file_if_exists(path: &Path, code: &'static str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(
            Error::new(code, "hook setup metadata path is not a regular file"),
        ),
        Ok(_) => {
            fs::remove_file(path)?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn remove_matching_setup_descriptors(
    plan: &HookInstallPlan,
    prepared: &PreparedHookSource,
) -> Result<()> {
    let source_request = source_dir(&plan.git_dir, &prepared.source_id).join(SETUP_REQUEST_FILE);
    for path in [&prepared.pending_descriptor_path, &source_request] {
        if let Some(descriptor) = read_pending_descriptor(path)?
            && !same_prepared_identity(&descriptor, prepared)
        {
            return Err(Error::new(
                "HOOK_SETUP_PENDING_CONFLICT",
                "hook setup descriptor belongs to another source identity",
            ));
        }
    }
    remove_regular_file_if_exists(
        &prepared.pending_descriptor_path,
        "HOOK_SETUP_PENDING_INVALID",
    )?;
    remove_regular_file_if_exists(&source_request, "HOOK_SETUP_PENDING_INVALID")?;
    Ok(())
}

fn validate_source_id(source_id: &str) -> Result<()> {
    if !crate::hooks::contract::is_canonical_v4_uuid(source_id) {
        return Err(Error::invalid("hook source ID is invalid"));
    }
    Ok(())
}

fn validate_git_executable(git_executable: &Path) -> Result<()> {
    if !git_executable.is_absolute() || !git_executable.is_file() {
        return Err(Error::new(
            "HOOK_GIT_EXECUTABLE_UNAVAILABLE",
            "the configured Git executable is not an absolute existing file",
        ));
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn path_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| Error::invalid("hook path must be valid UTF-8"))
}

#[cfg(windows)]
fn wide_nul(path: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    const ORIGINAL_HOOK: &[u8] = b"#!/bin/sh\n# retained user post-commit hook\nexit 0\n";
    const MODIFIED_HOOK: &[u8] = b"#!/bin/sh\n# user changed this hook\nexit 0\n";

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("swarm-hook-recovery-{}", model::new_id()));
            fs::create_dir_all(&path).expect("create isolated hook test root");
            Self(path)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct TestRepository {
        _root: TestRoot,
        repository_path: PathBuf,
        git_executable: PathBuf,
        swarm_executable: PathBuf,
    }

    impl TestRepository {
        fn new(previous_hook: Option<&[u8]>) -> Self {
            let root = TestRoot::new();
            let repository_path = root.0.join("repository");
            let empty_hooks = root.0.join("empty-hooks");
            fs::create_dir_all(&repository_path).expect("create test repository");
            fs::create_dir_all(&empty_hooks).expect("create isolated Git hooks directory");
            let git_executable = find_git_executable();
            run_fixture_git(
                &git_executable,
                &repository_path,
                &empty_hooks,
                &["init", "--quiet"],
            );
            run_fixture_git(
                &git_executable,
                &repository_path,
                &empty_hooks,
                &["config", "user.name", "Hook Recovery Fixture"],
            );
            run_fixture_git(
                &git_executable,
                &repository_path,
                &empty_hooks,
                &["config", "user.email", "hook-recovery@example.invalid"],
            );
            run_fixture_git(
                &git_executable,
                &repository_path,
                &empty_hooks,
                &["config", "commit.gpgsign", "false"],
            );
            fs::write(repository_path.join("seed.txt"), b"hook recovery fixture\n")
                .expect("write seed file");
            run_fixture_git(
                &git_executable,
                &repository_path,
                &empty_hooks,
                &["add", "seed.txt"],
            );
            run_fixture_git(
                &git_executable,
                &repository_path,
                &empty_hooks,
                &["commit", "--quiet", "-m", "hook recovery fixture"],
            );

            let swarm_executable = std::env::current_exe().expect("test executable path");
            let repository = Self {
                _root: root,
                repository_path,
                git_executable,
                swarm_executable,
            };
            if let Some(previous_hook) = previous_hook {
                let plan = repository.plan();
                write_new(&plan.hook_path, previous_hook, Some(0o755))
                    .expect("write previous hook fixture");
            }
            repository
        }

        fn plan(&self) -> HookInstallPlan {
            preview_post_commit(&self.repository_path, &self.git_executable)
                .expect("preview isolated test repository")
        }

        fn git_dir(&self) -> PathBuf {
            canonical_output_path(
                &git_stdout(
                    &self.git_executable,
                    &self.repository_path,
                    &["rev-parse", "--absolute-git-dir"],
                )
                .expect("read test repository Git directory"),
            )
            .expect("canonical test repository Git directory")
        }
    }

    struct HookFixture {
        repository: TestRepository,
        source_id: String,
        credential: Credential,
    }

    impl HookFixture {
        fn new(previous_hook: Option<&[u8]>) -> Self {
            let repository = TestRepository::new(previous_hook);
            let source_id = model::new_id();
            let credential = test_credential(&source_id);
            store_test_credential(&repository, &source_id, &credential);
            Self {
                repository,
                source_id,
                credential,
            }
        }

        fn from_prepared(repository: TestRepository, prepared: &PreparedHookSource) -> Self {
            Self {
                repository,
                source_id: prepared.source_id.clone(),
                credential: prepared.credential.clone(),
            }
        }

        fn source_dir(&self) -> PathBuf {
            source_dir(&self.repository.git_dir(), &self.source_id)
        }

        fn manifest_path(&self) -> PathBuf {
            self.source_dir().join("install.json")
        }

        fn credential_path(&self) -> PathBuf {
            self.source_dir().join("credential.json")
        }

        fn hook_path(&self) -> PathBuf {
            self.repository.plan().hook_path
        }

        fn manifest(&self) -> InstallManifest {
            read_install_manifest(&self.manifest_path()).expect("read retained install manifest")
        }

        fn set_phase(&self, phase: InstallPhase) -> InstallManifest {
            let mut manifest = self.manifest();
            manifest.phase = phase;
            private_replace_json(&self.manifest_path(), &manifest)
                .expect("persist simulated durable phase boundary");
            manifest
        }

        fn install(&self) -> HookInstallReadback {
            apply_post_commit(
                &self.repository.plan(),
                &self.source_id,
                &self.credential,
                &self.repository.swarm_executable,
            )
            .expect("install actual hook fixture")
        }

        fn readback(&self) -> HookInstallReadback {
            readback_post_commit(
                &self.repository.repository_path,
                &self.repository.git_executable,
                &self.source_id,
            )
            .expect("read hook fixture state")
        }

        fn revoke(&self) -> HookInstallReadback {
            revoke_post_commit(
                &self.repository.repository_path,
                &self.repository.git_executable,
                &self.source_id,
            )
            .expect("revoke hook fixture")
        }

        fn restore_from_retained_backup(&self, manifest: &InstallManifest) {
            match (
                manifest.previous_hook_path.as_deref(),
                manifest.previous_hook_sha256.as_deref(),
            ) {
                (Some(path), Some(_)) => {
                    let backup = read_optional_regular_file(Path::new(path))
                        .expect("read preserved hook backup")
                        .expect("preserved hook backup exists");
                    assert!(matches_saved_previous(&backup, manifest));
                    atomic_replace_bytes(
                        &PathBuf::from(&manifest.hook_path),
                        &backup.bytes,
                        manifest.previous_hook_mode,
                    )
                    .expect("apply exact hook restoration effect");
                }
                (None, None) => {
                    let target = PathBuf::from(&manifest.hook_path);
                    let current = read_optional_regular_file(&target)
                        .expect("read wrapper before removal")
                        .expect("installed wrapper exists");
                    assert_eq!(current.sha256, manifest.wrapper_sha256);
                    fs::remove_file(target).expect("apply exact hook removal effect");
                }
                _ => panic!("actual install manifest has complete backup identity"),
            }
        }

        fn finish_cleanup_tail_before_directory(&self, manifest: &InstallManifest) {
            self.restore_from_retained_backup(manifest);
            if let Some(path) = manifest.previous_hook_path.as_deref() {
                fs::remove_file(path).expect("apply backup deletion effect");
            }
            remove_source_credential(
                &self.repository.repository_path,
                &self.repository.git_executable,
                &self.source_id,
            )
            .expect("apply source credential and setup descriptor cleanup effect");
            fs::remove_file(self.manifest_path()).expect("apply manifest deletion effect");
        }
    }

    fn find_git_executable() -> PathBuf {
        let path_value = std::env::var_os("PATH").expect("test process PATH");
        #[cfg(windows)]
        let names = ["git.exe"];
        #[cfg(not(windows))]
        let names = ["git"];
        std::env::split_paths(&path_value)
            .find_map(|directory| {
                names
                    .iter()
                    .map(|name| directory.join(*name))
                    .find(|candidate| candidate.is_file())
            })
            .and_then(|path| fs::canonicalize(path).ok())
            .expect("Git executable is available for hook integration tests")
    }

    fn run_fixture_git(git: &Path, repository: &Path, empty_hooks: &Path, args: &[&str]) {
        let mut command = Command::new(git);
        command
            .arg("-c")
            .arg(format!("core.hooksPath={}", empty_hooks.display()))
            .args(args)
            .current_dir(repository)
            .stdin(std::process::Stdio::null());
        for key in [
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_COMMON_DIR",
            "GIT_NAMESPACE",
            "GIT_CONFIG",
            "GIT_CONFIG_PARAMETERS",
            "GIT_CONFIG_COUNT",
            "GIT_SSH",
            "GIT_SSH_COMMAND",
            "GIT_ASKPASS",
            "SSH_ASKPASS",
        ] {
            command.env_remove(key);
        }
        for (key, _) in std::env::vars_os() {
            if key.to_str().is_some_and(|key| {
                key.starts_with("GIT_CONFIG_KEY_")
                    || key.starts_with("GIT_CONFIG_VALUE_")
                    || key.starts_with("GIT_TRACE")
                    || key == "GIT_CURL_VERBOSE"
            }) {
                command.env_remove(key);
            }
        }
        let output = command.output().expect("start fixture Git command");
        assert!(
            output.status.success(),
            "fixture Git command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn test_credential(source_id: &str) -> Credential {
        Credential {
            client_id: format!("hook-source:{source_id}"),
            token: format!("{}{}", model::new_id(), model::new_id()),
        }
    }

    fn store_test_credential(
        repository: &TestRepository,
        source_id: &str,
        credential: &Credential,
    ) {
        let git_dir = repository.git_dir();
        let sources_dir = git_dir.join(SOURCE_DIRECTORY);
        ensure_private_directory(&sources_dir).expect("create hook source root");
        let source_path = source_dir(&git_dir, source_id);
        ensure_private_directory(&source_path).expect("create hook source directory");
        write_private_secret_new(
            &source_path.join("credential.json"),
            &serde_json::to_vec(credential).expect("serialize private hook credential"),
        )
        .expect("write private hook credential");
    }

    fn assert_restored(fixture: &HookFixture, manifest: &InstallManifest) {
        match manifest.previous_hook_path.as_deref() {
            Some(_) => {
                let hook = read_optional_regular_file(&PathBuf::from(&manifest.hook_path))
                    .expect("read restored hook")
                    .expect("original hook was restored");
                assert!(matches_saved_previous(&hook, manifest));
            }
            None => assert!(
                read_optional_regular_file(&PathBuf::from(&manifest.hook_path))
                    .expect("read removed wrapper")
                    .is_none()
            ),
        }
        assert!(!fixture.credential_path().exists());
        assert!(!fixture.manifest_path().exists());
        assert!(!fixture.source_dir().exists());
    }

    #[test]
    fn install_resumes_from_manifest_before_wrapper_publication() {
        let fixture = HookFixture::new(Some(ORIGINAL_HOOK));
        assert_eq!(fixture.install().state, "installed");
        let manifest = fixture.set_phase(InstallPhase::Installing);
        fixture.restore_from_retained_backup(&manifest);

        let interrupted = fixture.readback();
        assert_eq!(interrupted.state, "installing");
        assert!(!interrupted.wrapper_matches);
        assert_eq!(
            fs::read(fixture.hook_path()).unwrap().as_slice(),
            ORIGINAL_HOOK
        );

        let resumed = fixture.install();
        assert_eq!(resumed.state, "installed");
        assert!(resumed.wrapper_matches);
        assert_eq!(resumed.source_id, fixture.source_id);
        assert_eq!(resumed.backup_matches, Some(true));
        let installed = fixture.readback();
        assert_eq!(installed.state, "installed");
        assert_eq!(installed.wrapper_sha256, interrupted.wrapper_sha256);
        let backup = read_optional_regular_file(Path::new(
            fixture.manifest().previous_hook_path.as_deref().unwrap(),
        ))
        .unwrap()
        .unwrap();
        assert!(matches_saved_previous(&backup, &fixture.manifest()));
    }

    #[test]
    fn install_finalizes_after_wrapper_publication_without_rewriting_it() {
        let fixture = HookFixture::new(Some(ORIGINAL_HOOK));
        assert_eq!(fixture.install().state, "installed");
        let wrapper_before = fs::read(fixture.hook_path()).unwrap();
        let manifest = fixture.set_phase(InstallPhase::Installing);
        let interrupted = fixture.readback();
        assert_eq!(interrupted.state, "installing");
        assert!(interrupted.wrapper_matches);
        assert_eq!(manifest.wrapper_sha256, interrupted.wrapper_sha256.unwrap());

        let resumed = fixture.install();
        assert_eq!(resumed.state, "installed");
        assert_eq!(fs::read(fixture.hook_path()).unwrap(), wrapper_before);
        assert_eq!(fixture.readback().state, "installed");
    }

    #[test]
    fn revoke_resumes_after_exact_restore_and_backup_removal() {
        for remove_backup_before_retry in [false, true] {
            let fixture = HookFixture::new(Some(ORIGINAL_HOOK));
            assert_eq!(fixture.install().state, "installed");
            let manifest = fixture.set_phase(InstallPhase::Revoking);
            fixture.restore_from_retained_backup(&manifest);
            if remove_backup_before_retry {
                fs::remove_file(manifest.previous_hook_path.as_deref().unwrap())
                    .expect("apply backup deletion boundary");
            }

            assert_eq!(fixture.readback().state, "revoking");
            let resumed = fixture.revoke();
            assert_eq!(resumed.state, "restored");
            assert_restored(&fixture, &manifest);
        }
    }

    #[test]
    fn revoke_resumes_after_wrapper_removal_when_no_user_hook_existed() {
        let fixture = HookFixture::new(None);
        assert_eq!(fixture.install().state, "installed");
        let manifest = fixture.set_phase(InstallPhase::Revoking);
        fixture.restore_from_retained_backup(&manifest);
        assert_eq!(fixture.readback().state, "revoking");

        let resumed = fixture.revoke();
        assert_eq!(resumed.state, "restored");
        assert_restored(&fixture, &manifest);
    }

    #[test]
    fn revoke_cleans_empty_source_directory_after_credential_and_manifest_tail() {
        let fixture = HookFixture::new(Some(ORIGINAL_HOOK));
        assert_eq!(fixture.install().state, "installed");
        let manifest = fixture.set_phase(InstallPhase::Revoking);
        fixture.finish_cleanup_tail_before_directory(&manifest);
        assert!(fixture.source_dir().is_dir());
        assert!(!fixture.credential_path().exists());
        assert!(!fixture.manifest_path().exists());

        let retry = fixture.revoke();
        assert_eq!(retry.state, "absent");
        assert!(!fixture.source_dir().exists());
        assert_eq!(
            fs::read(&fixture.hook_path()).unwrap().as_slice(),
            ORIGINAL_HOOK
        );
    }

    #[test]
    fn revoke_preserves_user_modified_hook_and_original_backup() {
        let fixture = HookFixture::new(Some(ORIGINAL_HOOK));
        assert_eq!(fixture.install().state, "installed");
        let manifest = fixture.set_phase(InstallPhase::Revoking);
        fs::write(&manifest.hook_path, MODIFIED_HOOK).expect("simulate user hook edit");
        let backup_path = manifest.previous_hook_path.as_deref().unwrap();
        let backup_before = fs::read(backup_path).unwrap();

        let result = fixture.revoke();
        assert_eq!(result.state, "modified");
        assert_eq!(
            fs::read(&manifest.hook_path).unwrap().as_slice(),
            MODIFIED_HOOK
        );
        assert_eq!(fs::read(backup_path).unwrap(), backup_before);
        assert!(fixture.manifest_path().is_file());
        assert_eq!(fixture.manifest().phase, InstallPhase::Revoking);
    }

    #[test]
    fn revoke_preserves_mismatched_backup_and_installed_wrapper() {
        let fixture = HookFixture::new(Some(ORIGINAL_HOOK));
        assert_eq!(fixture.install().state, "installed");
        let manifest = fixture.set_phase(InstallPhase::Revoking);
        let wrapper_before = fs::read(&manifest.hook_path).unwrap();
        let backup_path = manifest.previous_hook_path.as_deref().unwrap();
        let mismatched_backup = b"user-modified preserved backup\n";
        fs::write(backup_path, mismatched_backup).expect("simulate changed backup");

        let result = fixture.revoke();
        assert_eq!(result.state, "modified");
        assert_eq!(fs::read(&manifest.hook_path).unwrap(), wrapper_before);
        assert_eq!(fs::read(backup_path).unwrap().as_slice(), mismatched_backup);
        assert!(fixture.manifest_path().is_file());
        assert_eq!(fixture.manifest().phase, InstallPhase::Revoking);
    }

    #[test]
    fn phase_less_legacy_manifest_remains_installed_and_revoke_compatible() {
        let fixture = HookFixture::new(Some(ORIGINAL_HOOK));
        assert_eq!(fixture.install().state, "installed");
        let manifest_path = fixture.manifest_path();
        let mut legacy: serde_json::Value =
            serde_json::from_slice(&read_bounded(&manifest_path).unwrap()).unwrap();
        assert_eq!(legacy["phase"], "installed");
        legacy
            .as_object_mut()
            .unwrap()
            .remove("phase")
            .expect("remove phase to represent a retained legacy manifest");
        private_replace_json(&manifest_path, &legacy).unwrap();

        let readback = fixture.readback();
        assert_eq!(readback.state, "installed");
        assert_eq!(readback.backup_matches, Some(true));
        let manifest = fixture.manifest();
        assert_eq!(manifest.phase, InstallPhase::Installed);
        assert_eq!(fixture.revoke().state, "restored");
        assert_restored(&fixture, &manifest);
    }

    #[test]
    fn setup_rollback_cleans_an_interrupted_install_and_matching_identity() {
        let repository = TestRepository::new(Some(ORIGINAL_HOOK));
        let plan = repository.plan();
        let prepared = prepare_source_credential(
            &plan,
            &format!("hook-test-project-{}", model::new_id()),
            Some(&model::new_id()),
        )
        .expect("prepare exact setup identity and private credential");
        let fixture = HookFixture::from_prepared(repository, &prepared);
        assert_eq!(fixture.install().state, "installed");
        let manifest = fixture.set_phase(InstallPhase::Installing);
        fixture.restore_from_retained_backup(&manifest);

        let rollback = fixture.revoke();
        assert_eq!(rollback.state, "restored");
        assert_restored(&fixture, &manifest);
        discard_source_setup(&plan, &prepared).expect("finish matching setup rollback cleanup");
        assert!(!prepared.pending_descriptor_path.exists());
        assert!(!fixture.source_dir().join(SETUP_REQUEST_FILE).exists());
        assert_eq!(fixture.readback().state, "absent");
    }
}
