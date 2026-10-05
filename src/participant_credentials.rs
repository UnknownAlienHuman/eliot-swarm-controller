//! Host-owned, assignment-scoped Participant credential issuance.
//!
//! This module prepares private local artifacts and then asks the launch-scoped
//! Store adapter to apply the ordinary registration inside the held-lease
//! admission transaction. It does not write Store metadata, change the global
//! MCP configuration, or claim that a native MCP client loaded the profile.

use crate::{
    config::{Config, Ipc, McpConfig, McpProfileConfig, McpToolProfile},
    error::{Error, Result},
    mcp,
    model::{self, Credential, Role},
    platform,
    store::Store,
    store::launcher::LaunchActor,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
};

const CREDENTIALS_DIRECTORY: &str = "participant-credentials";
const REFERENCES_DIRECTORY: &str = "references";
const CREDENTIAL_FILE: &str = "credential.json";
const PROFILE_FILE: &str = "mcp.toml";
const MANIFEST_FILE: &str = "issuance.json";
const REJECTION_FILE: &str = "registration-rejected.json";

/// The only participation basis supported by launch issuance. Producer-bound
/// Participants and sponsored reviewers have separate issuance contracts.
#[derive(Clone)]
pub(crate) enum ParticipationBasis {
    AttemptOwner,
}

impl ParticipationBasis {
    fn as_value(&self) -> Value {
        match self {
            Self::AttemptOwner => json!({
                "kind":"attempt_owner",
                "assignment_id":null,
                "review_scope":null,
            }),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum InboundPolicy {
    PullOnly,
}

impl InboundPolicy {
    fn as_str(self) -> &'static str {
        match self {
            Self::PullOnly => "pull_only",
        }
    }
}

/// Exact assignment input for creating one fresh Participant identity. The
/// caller owns the idempotency key; this module never invents one.
pub(crate) struct IssueRequest {
    /// Trusted host context from the retained parent launch Operation. This
    /// field is never parsed from registration params or serialized publicly.
    pub launch_operation_id: String,
    pub client_request_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub binding_id: String,
    pub binding_generation: i64,
    pub participation_basis: ParticipationBasis,
    pub mcp_profile: String,
    pub mcp_surface: String,
    pub display_alias: Option<String>,
    pub inbound_policy: Option<InboundPolicy>,
    pub native_session_id: Option<String>,
}

/// Public artifact handles are opaque; local paths stay private to resolution
/// and are never copied into a Store receipt or MCP response.
pub(crate) struct IssuedParticipant {
    pub credential_ref: String,
    pub profile_config_ref: String,
    pub registration: Value,
}

/// Local paths resolved from a matching opaque credential/profile reference
/// pair. Paths remain crate-local and are not serializable.
pub(crate) struct ResolvedParticipantArtifacts {
    credential_path: PathBuf,
    profile_config_path: PathBuf,
}

fn launch_actor_caller_id(actor: &LaunchActor) -> &str {
    match actor {
        LaunchActor::Direct(principal) => &principal.client_id,
        LaunchActor::OnBehalf(context) => context.technical_requester_id(),
    }
}

fn launch_identity(actor: &LaunchActor, request: &IssueRequest) -> Value {
    let mut identity = json!({
        "caller_id":launch_actor_caller_id(actor),
        "launch_operation_id":request.launch_operation_id,
        "client_request_id":request.client_request_id,
        "task_id":request.task_id,
        "task_revision":request.task_revision,
        "attempt_id":request.attempt_id,
        "binding_id":request.binding_id,
        "binding_generation":request.binding_generation,
        "participation_basis":request.participation_basis.as_value(),
        "mcp_profile":request.mcp_profile,
        "mcp_surface":request.mcp_surface,
        "display_alias":request.display_alias,
        "inbound_policy":request.inbound_policy.map(InboundPolicy::as_str).unwrap_or("pull_only"),
        "native_session_id":request.native_session_id,
    });
    if let LaunchActor::OnBehalf(context) = actor {
        identity["effective_manager_id"] = json!(context.effective_manager_id());
        identity["work_dispatch"] = context.linkage_value();
    }
    identity
}

/// Keep the Store-side registration validator aligned with the host's
/// deterministic participant identity without exposing credential material.
pub(crate) fn client_id_for_launch(actor: &LaunchActor, request: &IssueRequest) -> Result<String> {
    let digest = model::digest(model::canonical(&launch_identity(actor, request))?.as_bytes());
    Ok(format!("participant-{}", &digest[..48]))
}

impl ResolvedParticipantArtifacts {
    pub(crate) fn credential_path(&self) -> &Path {
        &self.credential_path
    }

    pub(crate) fn profile_config_path(&self) -> &Path {
        &self.profile_config_path
    }
}

#[derive(Serialize)]
struct ScopedStorage<'a> {
    data_dir: &'a Path,
    queue_capacity: usize,
}

#[derive(Serialize)]
struct ScopedConfig<'a> {
    schema_version: u32,
    storage: ScopedStorage<'a>,
    ipc: &'a Ipc,
    mcp: &'a McpConfig,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IssuanceManifest {
    schema_version: u32,
    identity_digest: String,
    request_slot_digest: String,
    registration_fingerprint: String,
    credential_fingerprint: String,
    profile_fingerprint: String,
    credential_ref: String,
    profile_config_ref: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RejectedIssuance {
    schema_version: u32,
    identity_digest: String,
    request_slot_digest: String,
    registration_fingerprint: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReferenceIndex {
    schema_version: u32,
    identity_digest: String,
    request_slot_digest: String,
    profile_fingerprint: String,
    credential_ref: String,
    profile_config_ref: String,
}

/// Issue a fresh credential for the supplied current Task/Attempt/binding.
/// Store remains authoritative for manager rights and live scope validation.
/// Local private artifacts are created before registration so a lost Store
/// response can be retried with the same token and caller-owned request ID.
pub(crate) async fn issue_for_launch(
    store: &Store,
    actor: LaunchActor,
    config: &Config,
    request: IssueRequest,
) -> Result<IssuedParticipant> {
    if matches!(&actor, LaunchActor::Direct(principal)
        if principal.role != Role::Manager && principal.role != Role::Operator)
    {
        return Err(Error::new(
            "FORBIDDEN",
            "manager or verified local operator authority required",
        ));
    }
    validate_request(&request)?;
    let source_profile = selected_participant_profile(config, &actor, &request)?;

    let identity = launch_identity(&actor, &request);
    let identity_digest = model::digest(model::canonical(&identity)?.as_bytes());
    let client_id = client_id_for_launch(&actor, &request)?;
    // The directory is stable for the caller-owned Store idempotency slot.
    // Reusing one request ID with different assignment data reaches the
    // existing private files and is rejected before another credential can be
    // minted for that slot.
    let request_slot = json!({
        "caller_id":launch_actor_caller_id(&actor),
        "client_request_id":request.client_request_id,
    });
    let request_slot_digest = model::digest(model::canonical(&request_slot)?.as_bytes());

    let data_root = fs::canonicalize(&config.storage.data_dir)?;
    if !data_root.is_dir() {
        return Err(Error::new(
            "CONFIG_ERROR",
            "configured Store data directory is not a directory",
        ));
    }
    let credentials_root = data_root.join(CREDENTIALS_DIRECTORY);
    ensure_private_directory(&credentials_root, &data_root)?;
    let credentials_root = fs::canonicalize(&credentials_root)?;
    let assignment_directory = credentials_root.join(&request_slot_digest);
    ensure_private_directory(&assignment_directory, &credentials_root)?;
    let assignment_directory = fs::canonicalize(&assignment_directory)?;

    let profile_config =
        scoped_profile_config(config, &request, &source_profile, &client_id, &data_root)?;
    let profile_bytes = profile_config.into_bytes()?;
    let credential_path = assignment_directory.join(CREDENTIAL_FILE);
    let profile_config_path = assignment_directory.join(PROFILE_FILE);
    let manifest_path = assignment_directory.join(MANIFEST_FILE);
    let rejection_path = assignment_directory.join(REJECTION_FILE);
    reject_terminal_issuance(&rejection_path, &identity_digest, &request_slot_digest)?;

    let credential = load_or_create_credential(&credential_path, &client_id)?;
    write_or_verify_exact(&profile_config_path, &profile_bytes)?;
    let profile_fingerprint = model::digest(&profile_bytes);
    let credential_fingerprint = model::digest(credential.token.as_bytes());

    let registration = registration_params(&request, &client_id, &credential);
    let registration_fingerprint = model::digest(model::canonical(&registration)?.as_bytes());
    let manifest = load_or_create_manifest(
        &manifest_path,
        &identity_digest,
        &request_slot_digest,
        &registration_fingerprint,
        &credential_fingerprint,
        &profile_fingerprint,
    )?;
    let references_directory = credentials_root.join(REFERENCES_DIRECTORY);
    ensure_private_directory(&references_directory, &credentials_root)?;
    let reference_index = ReferenceIndex {
        schema_version: 1,
        identity_digest: identity_digest.clone(),
        request_slot_digest: request_slot_digest.clone(),
        profile_fingerprint: profile_fingerprint.clone(),
        credential_ref: manifest.credential_ref.clone(),
        profile_config_ref: manifest.profile_config_ref.clone(),
    };
    let index_bytes = serde_json::to_vec_pretty(&reference_index)?;
    write_or_verify_exact(
        &reference_path(
            &references_directory,
            &manifest.credential_ref,
            "participant-credential",
        )?,
        &index_bytes,
    )?;
    write_or_verify_exact(
        &reference_path(
            &references_directory,
            &manifest.profile_config_ref,
            "participant-mcp-config",
        )?,
        &index_bytes,
    )?;

    let result = crate::store::participant_credentials::register_for_launch(
        store,
        actor,
        request.launch_operation_id.clone(),
        registration,
    )
    .await?;
    let result = match result {
        crate::store::participant_credentials::LaunchRegistrationOutcome::Registered(value) => {
            value
        }
        crate::store::participant_credentials::LaunchRegistrationOutcome::Rejected(error) => {
            retain_rejected_issuance(
                &rejection_path,
                &identity_digest,
                &request_slot_digest,
                &registration_fingerprint,
            )?;
            return Err(error);
        }
    };

    Ok(IssuedParticipant {
        credential_ref: manifest.credential_ref,
        profile_config_ref: manifest.profile_config_ref,
        registration: result,
    })
}

/// Resolve durable opaque references after a host restart. The paired refs
/// must lead to the same private manifest and the saved profile bytes must
/// still match the digest retained when the credential was issued.
pub(crate) fn resolve_refs(
    config: &Config,
    credential_ref: &str,
    profile_config_ref: &str,
) -> Result<ResolvedParticipantArtifacts> {
    let credential_uuid = opaque_ref_uuid(credential_ref, "participant-credential")?;
    let profile_uuid = opaque_ref_uuid(profile_config_ref, "participant-mcp-config")?;
    let data_root = fs::canonicalize(&config.storage.data_dir)?;
    let credentials_root =
        verify_private_directory(&data_root.join(CREDENTIALS_DIRECTORY), &data_root)?;
    let references_directory = verify_private_directory(
        &credentials_root.join(REFERENCES_DIRECTORY),
        &credentials_root,
    )?;
    let credential_index_path = references_directory.join(format!("{credential_uuid}.json"));
    let profile_index_path = references_directory.join(format!("{profile_uuid}.json"));
    let credential_index = read_reference_index(&credential_index_path)?;
    let profile_index = read_reference_index(&profile_index_path)?;
    if credential_index.schema_version != 1
        || credential_index.credential_ref != credential_ref
        || credential_index.profile_config_ref != profile_config_ref
        || !valid_opaque_ref(&credential_index.credential_ref, "participant-credential")
        || !valid_opaque_ref(
            &credential_index.profile_config_ref,
            "participant-mcp-config",
        )
        || serde_json::to_vec(&credential_index)? != serde_json::to_vec(&profile_index)?
    {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "opaque refs do not identify one matching assignment credential and MCP profile",
        ));
    }
    validate_digest(&credential_index.identity_digest)?;
    validate_digest(&credential_index.request_slot_digest)?;
    validate_digest(&credential_index.profile_fingerprint)?;

    let assignment_directory = verify_private_directory(
        &credentials_root.join(&credential_index.request_slot_digest),
        &credentials_root,
    )?;
    let manifest_path = assignment_directory.join(MANIFEST_FILE);
    let manifest_bytes = read_private_regular_file(&manifest_path)?.ok_or_else(|| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "assignment manifest is missing",
        )
    })?;
    let manifest: IssuanceManifest = serde_json::from_slice(&manifest_bytes)?;
    validate_digest(&manifest.registration_fingerprint)?;
    validate_digest(&manifest.credential_fingerprint)?;
    if manifest.schema_version != 2
        || manifest.identity_digest != credential_index.identity_digest
        || manifest.request_slot_digest != credential_index.request_slot_digest
        || manifest.profile_fingerprint != credential_index.profile_fingerprint
        || manifest.credential_ref != credential_ref
        || manifest.profile_config_ref != profile_config_ref
    {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "opaque refs do not match the retained assignment manifest",
        ));
    }
    if rejected_issuance_matches(
        &assignment_directory.join(REJECTION_FILE),
        &manifest.identity_digest,
        &manifest.request_slot_digest,
        &manifest.registration_fingerprint,
    )? {
        return Err(Error::new(
            "LAUNCH_ISSUANCE_REJECTED",
            "participant registration was durably rejected; retained artifacts cannot be resolved",
        ));
    }

    let credential_path = assignment_directory.join(CREDENTIAL_FILE);
    let credential_bytes = read_private_regular_file(&credential_path)?.ok_or_else(|| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "assignment credential is missing",
        )
    })?;
    let credential: Credential = serde_json::from_slice(&credential_bytes)?;
    let expected_client_id = format!("participant-{}", &credential_index.identity_digest[..48]);
    if credential.client_id != expected_client_id
        || credential.token.len() < 32
        || model::digest(credential.token.as_bytes()) != manifest.credential_fingerprint
    {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "retained credential does not match its issuance reference",
        ));
    }
    let profile_config_path = assignment_directory.join(PROFILE_FILE);
    let profile_bytes = read_private_regular_file(&profile_config_path)?.ok_or_else(|| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "assignment MCP profile is missing",
        )
    })?;
    if model::digest(&profile_bytes) != credential_index.profile_fingerprint {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "retained MCP profile does not match its issuance reference",
        ));
    }

    Ok(ResolvedParticipantArtifacts {
        credential_path,
        profile_config_path,
    })
}

fn validate_request(request: &IssueRequest) -> Result<()> {
    validate_text(
        &request.launch_operation_id,
        "launch_operation_id",
        128,
        false,
    )?;
    validate_text(&request.client_request_id, "client_request_id", 128, true)?;
    validate_text(&request.task_id, "task_id", 512, false)?;
    validate_text(&request.attempt_id, "attempt_id", 512, false)?;
    validate_text(&request.binding_id, "binding_id", 512, false)?;
    if request.task_revision <= 0 || request.binding_generation <= 0 {
        return Err(Error::invalid(
            "task_revision and binding_generation must be positive",
        ));
    }
    if let Some(alias) = request.display_alias.as_deref() {
        validate_text(alias, "display_alias", 128, true)?;
    }
    if let Some(session_id) = request.native_session_id.as_deref() {
        validate_text(session_id, "native_session_id", 512, false)?;
    }
    Ok(())
}

fn validate_text(value: &str, field: &str, max: usize, no_whitespace: bool) -> Result<()> {
    if value.is_empty()
        || value.len() > max
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || (no_whitespace && byte.is_ascii_whitespace()))
    {
        return Err(Error::invalid(format!(
            "{field} must be nonempty, at most {max} bytes, and contain no disallowed whitespace or controls"
        )));
    }
    Ok(())
}

fn selected_participant_profile(
    config: &Config,
    actor: &LaunchActor,
    request: &IssueRequest,
) -> Result<McpProfileConfig> {
    launch_participant_profile_template(
        config,
        actor,
        &request.mcp_profile,
        &request.mcp_surface,
    )?
    .ok_or_else(|| {
        Error::new(
            "FORBIDDEN",
            "launch profile is not an exact Participant template or the current Manager's profile",
        )
    })
}

/// Resolve the launch selector to a Participant-only template. An explicitly
/// configured Participant profile keeps its own declared Participant surface.
/// A Manager may instead select its own configured Manager profile as an
/// identity anchor; that profile's broader groups and manual methods are never
/// copied into the native Participant configuration.
pub(crate) fn launch_participant_profile_template(
    config: &Config,
    actor: &LaunchActor,
    profile_name: &str,
    surface_name: &str,
) -> Result<Option<McpProfileConfig>> {
    config.mcp.validate()?;
    let profile = config
        .mcp
        .profiles
        .get(profile_name)
        .ok_or_else(|| Error::new("CONFIG_ERROR", "MCP profile is not configured"))?;
    match profile.tool_profile {
        McpToolProfile::Participant => {
            if profile
                .surface
                .as_deref()
                .is_some_and(|surface| surface != surface_name)
            {
                return Err(Error::new(
                    "CONFIG_ERROR",
                    "requested MCP surface differs from the configured Participant profile",
                ));
            }
            mcp::launch_profile_surface(
                profile.tool_profile,
                surface_name,
                &profile.deferred_groups,
                &profile.manual_tools,
            )?;
            Ok(Some(profile.clone()))
        }
        McpToolProfile::Manager
            if actor.role() == Role::Manager
                && profile.expected_client_id == actor.effective_manager_id() =>
        {
            // The requested surface is validated against the Participant
            // hard role. Nothing from the Manager profile's groups or manual
            // methods crosses this translation boundary.
            mcp::launch_profile_surface(McpToolProfile::Participant, surface_name, &[], &[])?;
            Ok(Some(McpProfileConfig {
                tool_profile: McpToolProfile::Participant,
                expected_client_id: actor.effective_manager_id().to_owned(),
                surface: Some(surface_name.to_owned()),
                deferred_groups: Vec::new(),
                manual_tools: Vec::new(),
            }))
        }
        _ => Ok(None),
    }
}

fn scoped_profile_config(
    config: &Config,
    request: &IssueRequest,
    configured_profile: &McpProfileConfig,
    client_id: &str,
    data_root: &Path,
) -> Result<ScopedConfigOwned> {
    let mut profile = configured_profile.clone();
    profile.expected_client_id = client_id.to_owned();
    // A profile without an explicitly named surface receives only this
    // statically validated assignment surface in its private config copy.
    profile.surface = Some(request.mcp_surface.clone());
    let mcp = McpConfig {
        default_profile: request.mcp_profile.clone(),
        profiles: [(request.mcp_profile.clone(), profile)]
            .into_iter()
            .collect(),
    };
    mcp.validate()?;
    Ok(ScopedConfigOwned {
        data_dir: data_root.to_path_buf(),
        queue_capacity: config.storage.queue_capacity,
        ipc: config.ipc.clone(),
        mcp,
    })
}

struct ScopedConfigOwned {
    data_dir: PathBuf,
    queue_capacity: usize,
    ipc: Ipc,
    mcp: McpConfig,
}

impl ScopedConfigOwned {
    fn into_bytes(self) -> Result<Vec<u8>> {
        let view = ScopedConfig {
            schema_version: 1,
            storage: ScopedStorage {
                data_dir: &self.data_dir,
                queue_capacity: self.queue_capacity,
            },
            ipc: &self.ipc,
            mcp: &self.mcp,
        };
        toml::to_string_pretty(&view)
            .map(String::into_bytes)
            .map_err(|error| Error::new("CONFIG_ERROR", error.to_string()))
    }
}

fn registration_params(request: &IssueRequest, client_id: &str, credential: &Credential) -> Value {
    json!({
        "client_request_id":request.client_request_id,
        "client_id":client_id,
        "token_hash":model::digest(credential.token.as_bytes()),
        "task_id":request.task_id,
        "task_revision":request.task_revision,
        "attempt_id":request.attempt_id,
        "participation_basis":request.participation_basis.as_value(),
        "binding_id":request.binding_id,
        "binding_generation":request.binding_generation,
        "native_session_id":request.native_session_id,
        "display_alias":request.display_alias.as_deref().unwrap_or(client_id),
        "inbound_policy":request.inbound_policy.unwrap_or(InboundPolicy::PullOnly).as_str(),
    })
}

fn load_or_create_credential(path: &Path, client_id: &str) -> Result<Credential> {
    if let Some(bytes) = read_private_regular_file(path)? {
        let credential: Credential = serde_json::from_slice(&bytes)?;
        if credential.client_id != client_id || credential.token.len() < 32 {
            return Err(Error::conflict(
                "existing private credential does not match the exact assignment identity",
            ));
        }
        return Ok(credential);
    }

    let credential = Credential {
        client_id: client_id.to_owned(),
        token: format!("{}{}", model::new_id(), model::new_id()),
    };
    platform::write_private_new(path, &serde_json::to_vec_pretty(&credential)?)?;
    Ok(credential)
}

fn write_or_verify_exact(path: &Path, expected: &[u8]) -> Result<()> {
    if let Some(existing) = read_private_regular_file(path)? {
        if existing != expected {
            return Err(Error::conflict(
                "existing private MCP profile differs from the requested assignment profile",
            ));
        }
        return Ok(());
    }
    platform::write_private_new(path, expected)
}

fn load_or_create_manifest(
    path: &Path,
    identity_digest: &str,
    request_slot_digest: &str,
    registration_fingerprint: &str,
    credential_fingerprint: &str,
    profile_fingerprint: &str,
) -> Result<IssuanceManifest> {
    if let Some(bytes) = read_private_regular_file(path)? {
        let manifest: IssuanceManifest = serde_json::from_slice(&bytes)?;
        if manifest.schema_version != 2
            || manifest.identity_digest != identity_digest
            || manifest.request_slot_digest != request_slot_digest
            || manifest.registration_fingerprint != registration_fingerprint
            || manifest.credential_fingerprint != credential_fingerprint
            || manifest.profile_fingerprint != profile_fingerprint
            || !valid_opaque_ref(&manifest.credential_ref, "participant-credential")
            || !valid_opaque_ref(&manifest.profile_config_ref, "participant-mcp-config")
        {
            return Err(Error::conflict(
                "existing issuance manifest belongs to different request contents",
            ));
        }
        return Ok(manifest);
    }

    let manifest = IssuanceManifest {
        schema_version: 2,
        identity_digest: identity_digest.to_owned(),
        request_slot_digest: request_slot_digest.to_owned(),
        registration_fingerprint: registration_fingerprint.to_owned(),
        credential_fingerprint: credential_fingerprint.to_owned(),
        profile_fingerprint: profile_fingerprint.to_owned(),
        credential_ref: format!("participant-credential:{}", model::new_id()),
        profile_config_ref: format!("participant-mcp-config:{}", model::new_id()),
    };
    platform::write_private_new(path, &serde_json::to_vec_pretty(&manifest)?)?;
    Ok(manifest)
}

fn read_rejected_issuance(path: &Path) -> Result<Option<RejectedIssuance>> {
    let Some(bytes) = read_private_regular_file(path)? else {
        return Ok(None);
    };
    let rejected: RejectedIssuance = serde_json::from_slice(&bytes)?;
    validate_digest(&rejected.identity_digest)?;
    validate_digest(&rejected.request_slot_digest)?;
    validate_digest(&rejected.registration_fingerprint)?;
    if rejected.schema_version != 1 {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "participant issuance rejection marker has an unsupported version",
        ));
    }
    Ok(Some(rejected))
}

fn reject_terminal_issuance(
    path: &Path,
    identity_digest: &str,
    request_slot_digest: &str,
) -> Result<()> {
    let Some(rejected) = read_rejected_issuance(path)? else {
        return Ok(());
    };
    if rejected.identity_digest != identity_digest
        || rejected.request_slot_digest != request_slot_digest
    {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "participant issuance rejection marker belongs to different request contents",
        ));
    }
    Err(Error::new(
        "LAUNCH_ISSUANCE_REJECTED",
        "participant registration was durably rejected; its credential cannot be reused",
    ))
}

fn rejected_issuance_matches(
    path: &Path,
    identity_digest: &str,
    request_slot_digest: &str,
    registration_fingerprint: &str,
) -> Result<bool> {
    let Some(rejected) = read_rejected_issuance(path)? else {
        return Ok(false);
    };
    if rejected.identity_digest != identity_digest
        || rejected.request_slot_digest != request_slot_digest
        || rejected.registration_fingerprint != registration_fingerprint
    {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "participant issuance rejection marker does not match the retained manifest",
        ));
    }
    Ok(true)
}

fn retain_rejected_issuance(
    path: &Path,
    identity_digest: &str,
    request_slot_digest: &str,
    registration_fingerprint: &str,
) -> Result<()> {
    let rejected = RejectedIssuance {
        schema_version: 1,
        identity_digest: identity_digest.to_owned(),
        request_slot_digest: request_slot_digest.to_owned(),
        registration_fingerprint: registration_fingerprint.to_owned(),
    };
    if rejected_issuance_matches(
        path,
        identity_digest,
        request_slot_digest,
        registration_fingerprint,
    )? {
        return Ok(());
    }
    if let Err(error) = platform::write_private_new(path, &serde_json::to_vec_pretty(&rejected)?) {
        if rejected_issuance_matches(
            path,
            identity_digest,
            request_slot_digest,
            registration_fingerprint,
        )? {
            return Ok(());
        }
        return Err(error);
    }
    Ok(())
}

fn valid_opaque_ref(value: &str, prefix: &str) -> bool {
    opaque_ref_uuid(value, prefix).is_ok()
}

fn opaque_ref_uuid<'a>(value: &'a str, prefix: &str) -> Result<&'a str> {
    let expected_prefix = format!("{prefix}:");
    let suffix = value
        .strip_prefix(expected_prefix.as_str())
        .filter(|suffix| uuid::Uuid::parse_str(suffix).is_ok())
        .ok_or_else(|| Error::invalid("invalid opaque participant artifact reference"))?;
    Ok(suffix)
}

fn reference_path(directory: &Path, reference: &str, prefix: &str) -> Result<PathBuf> {
    Ok(directory.join(format!("{}.json", opaque_ref_uuid(reference, prefix)?)))
}

fn read_reference_index(path: &Path) -> Result<ReferenceIndex> {
    let bytes = read_private_regular_file(path)?.ok_or_else(|| {
        Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "opaque participant artifact ref is unknown",
        )
    })?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn validate_digest(value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_REFERENCE",
            "participant artifact index contains an invalid digest",
        ));
    }
    Ok(())
}

fn ensure_private_directory(path: &Path, allowed_parent: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir(path)?,
        Err(error) => return Err(error.into()),
    }
    verify_private_directory(path, allowed_parent).map(|_| ())
}

fn verify_private_directory(path: &Path, allowed_parent: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || is_link_or_reparse(&metadata) {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_PATH",
            "assignment credential path must be a regular directory, not a link",
        ));
    }
    let canonical_parent = fs::canonicalize(allowed_parent)?;
    let canonical_path = fs::canonicalize(path)?;
    if canonical_path == canonical_parent || !canonical_path.starts_with(&canonical_parent) {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_PATH",
            "assignment credential directory escaped its private Store root",
        ));
    }
    platform::private_permissions(&canonical_path, true)?;
    Ok(canonical_path)
}

fn read_private_regular_file(path: &Path) -> Result<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !metadata.is_file() || is_link_or_reparse(&metadata) {
        return Err(Error::new(
            "PRIVATE_ARTIFACT_PATH",
            "assignment credential artifact must be a regular file, not a link",
        ));
    }
    platform::private_permissions(path, false)?;
    Ok(Some(fs::read(path)?))
}

fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return true;
        }
    }
    false
}
