//! Shared per-Agent session helpers for the stream-owning harness plugins.
//!
//! Muse, OpenCode, Codex and Claude bind one Agent generation to one owned
//! native server through this module: the target-environment mapping,
//! identity/binding validation, unknown snapshots, contextual errors,
//! native process registration, the no-write code predicate, stream event
//! commits/gaps, the bounded native-error/final-text attachments, control
//! outcomes, request event ids, optional profile text, and resume subjects
//! are identical up to the harness name. This module holds the single copy;
//! each plugin passes its own display name (`"Muse"`, `"OpenCode"`,
//! `"Codex"`, `"Claude"`) and id prefix (`"muse"`, `"opencode"`, `"codex"`,
//! `"claude"`). No trait: the shared state is passed by reference through
//! [`StreamSource`].

use std::{ffi::OsString, path::Path, time::Duration};

use eliot_core::{
    ContextItem, ContextOutcome, Error, InjectPoint, NativeEvent, PermissionRefusal, ProcessRecord,
    ProcessState, ProviderRestriction, Result, SecretPolicy, SourceBatch, SourceGap,
};
use serde_json::{Map, Value, json};
use tokio::time::Instant;

use crate::protocol::{AgentCommandParams, InitializeParams, MAX_CREDENTIAL_BYTES};

/// Fill only missing origin/location from a machine-owned error code. A
/// no-effect verdict says nothing about the layer that caused the failure.
pub fn classify_start_failure(mut error: Error, harness: &str, phase: &str) -> Error {
    if error.failure_where.is_none() {
        error = error.with_where(format!("plugin.{harness}.start.{phase}"));
    }
    if error.origin.is_none() {
        use eliot_core::FailureOrigin;
        let (origin, evidence) = match error.code.as_str() {
            "native_spawn_refused" | "native_spawn" | "platform_io" | "io" | "auth_required" => (
                FailureOrigin::Environment,
                "OS, executable, filesystem or external credential operation failed",
            ),
            "native_rejected" | "native_model_unavailable" => (
                FailureOrigin::Harness,
                "native harness refused the request without structured upstream evidence",
            ),
            "invalid" | "profile_error" | "unsupported" | "internal" | "schema" | "busy" => (
                FailureOrigin::Swarm,
                "Swarm validation, schema or implementation gate refused the request",
            ),
            _ => (
                FailureOrigin::Unknown,
                "missing a classified native error or OS failure signal",
            ),
        };
        error = error.with_origin(origin, evidence);
    }
    error
}

/// Preserve typed diagnostics when a plugin proves that startup had no effect.
/// Redact the complete fields before Error's wire bounds are applied.
pub fn failed_start_value(params: &crate::AgentStartParams, error: Error) -> Value {
    let secrets: Vec<_> = params
        .target_environment
        .iter()
        .chain(params.mcp_environment.iter())
        .filter_map(Value::as_object)
        .flat_map(|env| env.iter())
        .filter(|(name, _)| {
            is_secret_name(name)
                || is_passed_env(
                    name,
                    params.profile.as_ref().and_then(|p| p.get("pass_env")),
                )
        })
        .filter_map(|(_, value)| value.as_str())
        .collect();
    let error = error.sanitized(&SecretPolicy::new(secrets));
    let mut value = serde_json::to_value(error).unwrap_or_else(|_| json!({"code": "internal"}));
    value["agent_id"] = json!(params.agent_id);
    value["generation"] = json!(params.generation);
    value["command_id"] = json!(params.command_id);
    value["state"] = json!("failed");
    value["reason"] = value["message"].clone();
    value
}

/// Classified diagnostic fields on an exactly associated native terminal.
/// These fields explain a failure; they do not decide effect settlement.
pub fn attach_failure_origin(
    data: &mut Value,
    harness: &str,
    origin: eliot_core::FailureOrigin,
    evidence: &str,
    policy: &SecretPolicy,
) {
    let error = Error::new("native_failure", "native turn failed")
        .with_where(format!("plugin.{harness}.turn"))
        .with_origin(origin, evidence)
        .sanitized(policy);
    for (key, value) in [
        ("where", json!(error.failure_where)),
        ("origin", json!(error.origin)),
        ("origin_evidence", json!(error.origin_evidence)),
    ] {
        if !policy.contains_text(key) && !policy.contains_value(&value) {
            data[key] = value;
        }
    }
}

use crate::{
    AgentSnapshotParams, CommitSink, MAX_EVENT_BYTES, MAX_TEXT_BYTES, PeerClient,
    protocol::MAX_COMMAND_ID_BYTES,
};

/// Deadline for one stream commit or gap round trip.
pub const EVENT_DEADLINE: Duration = Duration::from_secs(30);

/// Validated target environment: child variables plus the values that must
/// be redacted from diagnostics.
pub struct TargetEnvironment {
    pub variables: Vec<(OsString, OsString)>,
    pub secrets: Vec<String>,
}

/// Heuristic secret-name rule shared by every harness plugin.
pub fn is_secret_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    upper.contains("TOKEN")
        || upper.contains("SECRET")
        || upper.contains("PASSWORD")
        || upper.contains("API_KEY")
        || upper.contains("AUTH")
}

/// Whether `name` is explicitly passed through by the frozen profile's
/// `pass_env` list (case-insensitive on Windows, exact elsewhere).
pub fn is_passed_env(name: &str, pass_env: Option<&Value>) -> bool {
    pass_env.and_then(Value::as_array).is_some_and(|names| {
        names.iter().filter_map(Value::as_str).any(|passed| {
            if cfg!(windows) {
                name.eq_ignore_ascii_case(passed)
            } else {
                name == passed
            }
        })
    })
}

/// Map the frozen `target_environment` object to native child variables and
/// collect secret values for redaction. Data-root variables stay scoped to the
/// MCP integration; Owner variables are excluded from managed child environments.
/// `harness` names the caller in diagnostics.
pub fn target_environment(
    environment: Option<&Value>,
    pass_env: Option<&Value>,
    harness: &str,
) -> Result<TargetEnvironment> {
    target_environment_inner(environment, pass_env, harness, false)
}

fn target_environment_inner(
    environment: Option<&Value>,
    pass_env: Option<&Value>,
    harness: &str,
    allow_mcp_scope: bool,
) -> Result<TargetEnvironment> {
    if pass_env.and_then(Value::as_array).is_some_and(|names| {
        names
            .iter()
            .filter_map(Value::as_str)
            .any(is_reserved_eliot_environment_name)
    }) {
        return Err(Error::new(
            "profile_error",
            format!("{harness} pass_env cannot include reserved ELIOT_* names"),
        ));
    }
    let Some(environment) = environment else {
        return Ok(TargetEnvironment {
            variables: Vec::new(),
            secrets: Vec::new(),
        });
    };
    let object = environment.as_object().ok_or_else(|| {
        Error::new(
            "profile_error",
            format!("{harness} target_environment must be an object"),
        )
    })?;
    let mut vars = Vec::with_capacity(object.len());
    let mut secrets = Vec::new();
    for (name, value) in object {
        if !allow_mcp_scope && is_mcp_only_scope_name(name) {
            continue;
        }
        if name.is_empty() || name.contains('=') || name.contains('\0') {
            return Err(Error::new(
                "profile_error",
                format!("{harness} environment contains an invalid variable name"),
            ));
        }
        let value = value.as_str().ok_or_else(|| {
            Error::new(
                "profile_error",
                format!("{harness} environment values must be strings"),
            )
        })?;
        if (is_secret_name(name) || is_passed_env(name, pass_env)) && !value.is_empty() {
            secrets.push(value.to_owned());
        }
        vars.push((OsString::from(name), OsString::from(value)));
    }
    Ok(TargetEnvironment {
        variables: vars,
        secrets,
    })
}

fn is_reserved_eliot_environment_name(name: &str) -> bool {
    name.get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ELIOT_"))
}

fn is_mcp_only_scope_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("ELIOT_HOME")
        || name.eq_ignore_ascii_case("ELIOT_AGENT_DATA_DIR")
        || name
            .get(..12)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("ELIOT_OWNER_"))
}

/// Resolve the exact Agent identity for one private MCP server. The normal
/// path uses the daemon's separate MCP environment. The legacy fallback reads
/// scoped fields from old target environments; ordinary target mapping still
/// removes data-root and Owner variables before native child launch.
pub fn agent_mcp_environment(
    mcp_environment: Option<&Value>,
    target_environment_value: Option<&Value>,
    pass_agent_identity_to_shell: Option<bool>,
    agent_id: &str,
    generation: i64,
    harness: &str,
) -> Result<TargetEnvironment> {
    const IDENTITY_NAMES: &[&str] = &[
        "ELIOT_HOME",
        "ELIOT_AGENT_DATA_DIR",
        "ELIOT_AGENT_ID",
        "ELIOT_AGENT_GENERATION",
        "ELIOT_AGENT_TOKEN",
    ];
    let value = if let Some(value) = mcp_environment {
        value.clone()
    } else if pass_agent_identity_to_shell.unwrap_or(true) {
        let source = target_environment_value
            .and_then(Value::as_object)
            .ok_or_else(|| {
                Error::new(
                    "not_allowed",
                    format!("{harness} Agent MCP requires its explicit identity environment"),
                )
            })?;
        let mut filtered = Map::new();
        for &name in IDENTITY_NAMES {
            if let Some(value) = source.iter().find_map(|(key, value)| {
                (key == name || (cfg!(windows) && key.eq_ignore_ascii_case(name))).then_some(value)
            }) {
                filtered.insert(name.to_owned(), value.clone());
            }
        }
        Value::Object(filtered)
    } else {
        return Err(Error::new(
            "not_allowed",
            format!("{harness} shell identity is withheld but its MCP identity is missing"),
        ));
    };
    let object = value.as_object().ok_or_else(|| {
        Error::new(
            "profile_error",
            format!("{harness} MCP identity environment must be an object"),
        )
    })?;
    if object.len() != IDENTITY_NAMES.len()
        || object.keys().any(|name| {
            !IDENTITY_NAMES.iter().any(|allowed| {
                name == *allowed || (cfg!(windows) && name.eq_ignore_ascii_case(allowed))
            })
        })
    {
        return Err(Error::new(
            "profile_error",
            format!("{harness} MCP identity environment has an invalid field set"),
        ));
    }
    let mapped = target_environment_inner(Some(&value), None, harness, true)?;
    crate::native_cli::require_agent_mcp_root(
        &mapped.variables,
        agent_id,
        generation,
        "Agent MCP identity does not match its bound generation",
        "Agent MCP identity has no owned Eliot data root",
    )?;
    let data_dir =
        crate::native_cli::unique_environment_value(&mapped.variables, "ELIOT_AGENT_DATA_DIR")
            .and_then(std::ffi::OsStr::to_str)
            .filter(|path| {
                std::path::Path::new(path).is_absolute() && std::path::Path::new(path).is_dir()
            })
            .ok_or_else(|| Error::new("not_allowed", "Agent MCP data directory is unavailable"))?;
    if data_dir.trim().is_empty() {
        return Err(Error::new(
            "not_allowed",
            "Agent MCP data directory is empty",
        ));
    }
    Ok(mapped)
}

/// Refuse a request whose Agent/generation does not match the bound session.
pub fn validate_binding(
    bound_agent_id: &str,
    bound_generation: i64,
    agent_id: &str,
    generation: i64,
    harness: &str,
) -> Result<()> {
    if bound_agent_id != agent_id || bound_generation != generation {
        return Err(Error::new(
            "stale_generation",
            format!("{harness} request does not match the bound Agent generation"),
        ));
    }
    Ok(())
}

/// Refuse a request without a bounded Agent, generation, and command identity.
pub fn validate_identity(
    agent_id: &str,
    generation: i64,
    command_id: &str,
    harness: &str,
) -> Result<()> {
    if agent_id.trim().is_empty()
        || generation <= 0
        || command_id.trim().is_empty()
        || command_id.len() > MAX_COMMAND_ID_BYTES
    {
        return Err(Error::new(
            "invalid",
            format!("{harness} request requires a bounded Agent, generation, and command identity"),
        ));
    }
    Ok(())
}

/// Decode typed plugin params, mapping any shape mismatch to `invalid`.
pub fn decode<T: serde::de::DeserializeOwned>(raw: &Value, message: &str) -> Result<T> {
    serde_json::from_value(raw.clone()).map_err(|_| Error::new("invalid", message))
}

/// Local non-dispatch codes: the write provably never reached the native
/// side, so the host may report `rejected` (never sent, safe to supersede).
pub fn native_no_write(code: &str) -> bool {
    matches!(code, "native_not_dispatched" | "busy" | "frame_too_large")
}

/// Prepend FIFO-claimed context to the original send text for one native input
/// operation. Items stay in claim order, joined by blank lines, then a blank
/// line, then the exact original text. The Store has already framed each item
/// as quoted data. Retain the original input for idempotency and digests, and
/// send the returned string natively
/// in the same operation. The Store checks the unframed input budget. The
/// adapter checks the encoded native frame against `MAX_FRAME_BYTES` before I/O.
pub fn with_claimed_context(text: &str, context: &[ContextItem], harness: &str) -> Result<String> {
    if text.len() > MAX_TEXT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            format!("{harness} input exceeds 512 KiB"),
        ));
    }
    if context.is_empty() {
        return Ok(text.to_owned());
    }
    let mut combined = String::new();
    for item in context {
        if !combined.is_empty() {
            combined.push_str("\n\n");
        }
        combined.push_str(&item.text);
    }
    combined.push_str("\n\n");
    combined.push_str(text);
    if combined.len() > crate::MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            format!("{harness} input with context exceeds the frame limit"),
        ));
    }
    Ok(combined)
}

/// Build one [`ContextOutcome`] per FIFO-claimed `next_input` item, in claim
/// order, for the exact send (`consumer_id`) that applied them inside its
/// native input. Matches the Store `turn.started.data.context_outcomes`
/// receipt shape: Store zips these positionally against its FIFO claims and
/// checks the exact count and consumer binding. Empty in, empty out: the
/// caller omits the key when nothing was claimed. Pure: no I/O, no replay.
pub fn next_input_outcomes(items: &[ContextItem], consumer_id: &str) -> Vec<ContextOutcome> {
    items
        .iter()
        .map(|_| ContextOutcome {
            mechanism: "send".to_owned(),
            application_point: InjectPoint::NextInput,
            consumer_id: consumer_id.to_owned(),
        })
        .collect()
}

/// Project normalized supported context points into a `session.ready`
/// effective object. Admission reads `Agent.effective.context_points` as
/// `Vec<InjectPoint>`; each adapter declares only the points it actually
/// consumes. Non-objects pass through unchanged.
pub fn with_context_points(mut effective: Value, points: &[InjectPoint]) -> Value {
    if let Some(object) = effective.as_object_mut() {
        object.insert(
            "context_points".to_owned(),
            serde_json::to_value(points).unwrap_or(Value::Null),
        );
    }
    effective
}

/// Declare in a `session.ready` effective object that this session has no
/// native steer and takes `steer` at a turn safe point.
pub fn with_safe_point_steer(mut effective: Value) -> Value {
    if let Some(object) = effective.as_object_mut() {
        object.insert(
            eliot_core::STEER_DELIVERY_FIELD.to_owned(),
            serde_json::to_value(InjectPoint::TurnSafePoint).unwrap_or(Value::Null),
        );
    }
    effective
}

/// `bad_state` for a request that arrived before `initialize`.
pub fn not_initialized(harness: &str) -> Error {
    Error::new(
        "bad_state",
        format!("{harness} plugin has not been initialized"),
    )
}

/// Require the installed stream source plan for exactly `families`.
/// `harness` names the caller in diagnostics; extra families stay tolerated
/// (the host installs the connected plan, which may carry more).
pub fn validate_source_plan(
    harness: &str,
    families: &[&str],
    source_plan: Option<&Value>,
) -> Result<()> {
    let Some(plan) = source_plan.and_then(Value::as_object) else {
        return Err(Error::new(
            "unsupported",
            format!("{harness} start requires an installed stream source plan"),
        ));
    };
    if families
        .iter()
        .any(|family| plan.get(*family).and_then(Value::as_str) != Some("stream"))
    {
        return Err(Error::new(
            "unsupported",
            format!(
                "{harness} start requires stream ownership for {}",
                join_families(families)
            ),
        ));
    }
    Ok(())
}

/// Oxford-join a family list exactly as the per-harness messages spelled it
/// (`"session, turn, and message"`).
fn join_families(families: &[&str]) -> String {
    match families {
        [] => String::new(),
        [only] => (*only).to_owned(),
        [first, second] => format!("{first} and {second}"),
        _ => {
            let (last, rest) = families.split_last().expect("non-empty families");
            format!("{}, and {last}", rest.join(", "))
        }
    }
}

/// Read one optional frozen-profile text field: absent or null is unset, a
/// non-empty string is kept, anything else is a profile error.
pub fn optional_profile_text(
    profile: &Map<String, Value>,
    key: &str,
    harness: &str,
) -> Result<Option<String>> {
    match profile.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(Error::new(
            "profile_error",
            format!("{harness} profile field {key} must be a non-empty string"),
        )),
    }
}

/// Split the exact provider/model profile form at its first slash. OpenCode
/// v2.2.7 Model.Ref.parse keeps later slashes in the model id; its separate
/// `#variant` syntax is not mapped by the current Eliot profile path.
pub fn split_provider_model(model: &str) -> Option<(&str, &str)> {
    if model
        .chars()
        .any(|ch| ch.is_whitespace() || ch.is_control())
        || model.contains('\\')
        || model.contains('#')
    {
        return None;
    }
    let (provider, id) = model.split_once('/')?;
    if provider.is_empty() || id.split('/').any(str::is_empty) {
        return None;
    }
    Some((provider, id))
}

/// A validated retained resume subject: the exact native session to continue
/// plus the retained generation's last turn summary, when it had one. The
/// request family is new in the resuming generation, so only the
/// session/turn/message ownership is required of the retained source.
#[derive(Debug)]
pub struct ResumeSubject {
    pub native_scope: String,
    pub native_ref: String,
    pub retained_turn: Option<String>,
}

/// Validate the retained resume subject and its generation continuity. The
/// retained source must still bind this Agent's previous generation on the
/// stream, and the retained `current_turn` summary must be complete: a
/// malformed summary is `unknown`, never silently adopted.
pub fn parse_resume_subject(
    resume: &Value,
    source_state: Option<&Value>,
    agent_id: &str,
    generation: i64,
    harness: &str,
) -> Result<ResumeSubject> {
    let resume = resume.as_object().ok_or_else(|| {
        Error::new(
            "invalid",
            format!("{harness} resume_from must be an object"),
        )
    })?;
    if resume.len() != 2
        || !resume.contains_key("native_scope")
        || !resume.contains_key("native_ref")
    {
        return Err(Error::new(
            "invalid",
            format!("{harness} resume_from must contain only native_scope and native_ref"),
        ));
    }
    let native_scope = resume
        .get("native_scope")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "invalid",
                format!("{harness} resume_from requires native_scope"),
            )
        })?
        .to_owned();
    let native_ref = resume
        .get("native_ref")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "invalid",
                format!("{harness} resume_from requires native_ref"),
            )
        })?
        .to_owned();
    let source_state = source_state.and_then(Value::as_object).ok_or_else(|| {
        Error::new(
            "unknown",
            format!("{harness} resume requires retained native readback"),
        )
    })?;
    let retained_generation = source_state
        .get("retained_generation")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            Error::new(
                "unknown",
                format!("{harness} resume readback omitted its retained generation"),
            )
        })?;
    if retained_generation.checked_add(1) != Some(generation) {
        return Err(Error::new(
            "stale_generation",
            format!("{harness} resume generation does not follow the retained source generation"),
        ));
    }
    let retained_source = source_state
        .get("retained_source")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            Error::new(
                "unknown",
                format!("{harness} resume omitted retained source state"),
            )
        })?;
    if retained_source.get("agent_id").and_then(Value::as_str) != Some(agent_id)
        || retained_source.get("generation").and_then(Value::as_i64) != Some(retained_generation)
        || retained_source.get("source").and_then(Value::as_str) != Some("stream")
    {
        return Err(Error::new(
            "stale_generation",
            format!("{harness} resume source state does not match the retained Agent generation"),
        ));
    }
    let families = retained_source
        .get("families")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            Error::new(
                "unknown",
                format!("{harness} resume source state omitted its families"),
            )
        })?;
    if ["session", "turn", "message"]
        .iter()
        .any(|family| !families.iter().any(|value| value.as_str() == Some(family)))
    {
        return Err(Error::new(
            "unknown",
            format!("{harness} retained source did not own the required event families"),
        ));
    }
    let current_turn = source_state.get("current_turn").ok_or_else(|| {
        Error::new(
            "unknown",
            format!("{harness} resume readback omitted current_turn"),
        )
    })?;
    let retained_turn = match current_turn {
        Value::Null => None,
        Value::Object(turn) => {
            if !["state", "status", "started_at", "completed_at"]
                .iter()
                .all(|field| turn.contains_key(*field))
                || !matches!(
                    turn.get("state").and_then(Value::as_str),
                    Some("running" | "completed" | "unknown")
                )
                || !matches!(
                    turn.get("status"),
                    Some(value) if value.is_null() || value.as_str().is_some()
                )
                || !matches!(
                    turn.get("started_at"),
                    Some(value) if value.is_null() || value.as_i64().is_some()
                )
                || !matches!(
                    turn.get("completed_at"),
                    Some(value) if value.is_null() || value.as_i64().is_some()
                )
            {
                return Err(Error::new(
                    "unknown",
                    format!("{harness} resume current_turn summary is incomplete"),
                ));
            }
            Some(
                turn.get("turn_id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new(
                            "unknown",
                            format!("{harness} resume current_turn omitted its exact turn id"),
                        )
                    })?
                    .to_owned(),
            )
        }
        _ => {
            return Err(Error::new(
                "unknown",
                format!("{harness} resume current_turn readback is malformed"),
            ));
        }
    };
    Ok(ResumeSubject {
        native_scope,
        native_ref,
        retained_turn,
    })
}

/// Canonicalizing path comparison shared by the server readiness proofs:
/// same file on case-insensitive Windows, byte equality elsewhere.
pub fn paths_equal(left: &Path, right: &Path) -> bool {
    let left = std::fs::canonicalize(left).unwrap_or_else(|_| left.to_path_buf());
    let right = std::fs::canonicalize(right).unwrap_or_else(|_| right.to_path_buf());
    if cfg!(windows) {
        left.to_string_lossy()
            .eq_ignore_ascii_case(&right.to_string_lossy())
    } else {
        left == right
    }
}

/// Unknown agent snapshot: the read proved nothing about the requested turn.
pub fn unknown_snapshot(params: &AgentSnapshotParams, reason: &str) -> Result<Value> {
    Ok(json!({
        "agent_id": params.agent_id,
        "generation": params.generation,
        "command_id": params.command_id,
        "state": "unknown",
        "active_turn": null,
        "last_turn": null,
        "source_unknown": true,
        "result_known": false,
        "reason": reason,
    }))
}

/// Unknown outcomes stay verbatim; other errors get the caller code/message.
pub fn contextual_error(
    error: Error,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Error {
    if error.code == "unknown" {
        return error;
    }
    error.with_code(code).with_message(message)
}

/// A control reply the native side provably never applied.
pub fn rejected_control(reason: &str) -> Value {
    json!({"state": "rejected", "reason": reason})
}

/// A control reply whose native outcome is unresolved.
pub fn unknown_control(reason: &str) -> Value {
    json!({"state": "unknown", "reason": reason})
}

/// The shared `initialize` identity check: exact protocol, a non-empty
/// private instance directory, and a bounded non-empty credential. Each
/// adapter adds its own extra checks and error message.
pub fn initialize_fields_valid(params: &InitializeParams) -> bool {
    params.protocol == "eliot-plugin/1"
        && !params.instance_dir.trim().is_empty()
        && !params.credential.trim().is_empty()
        && params.credential.len() <= MAX_CREDENTIAL_BYTES
}

/// Stop latch for one Agent. `stopped` is the session phase, `latch` the
/// retained stop command id. Returns `Ok(true)` for the exact repeat of an
/// applied stop (answer `applied`), `Ok(false)` after latching this command
/// before any native I/O, and an error for a different or unresolved stop.
pub fn begin_stop(
    latch: &mut Option<String>,
    stopped: bool,
    command_id: &str,
    harness: &str,
) -> Result<bool> {
    if stopped && latch.as_deref() == Some(command_id) {
        return Ok(true);
    }
    if let Some(existing) = latch {
        if existing != command_id {
            return Err(Error::new(
                "source_integrity",
                format!("{harness} stop command id changed after dispatch"),
            ));
        }
        return Err(Error::new(
            "unknown",
            format!("{harness} stop outcome is unresolved and will not be repeated"),
        ));
    }
    *latch = Some(command_id.to_owned());
    Ok(false)
}

/// The captured `(turn_id, request_id)` a request reply must carry.
pub fn request_reply_ids<'a>(
    params: &'a AgentCommandParams,
    harness: &str,
) -> Result<(&'a str, &'a str)> {
    let turn_id = params
        .turn_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "stale_turn",
                format!("{harness} request reply needs its captured turn id"),
            )
        })?;
    let request_id = params
        .request_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "not_found",
                format!("{harness} request reply needs its captured request id"),
            )
        })?;
    Ok((turn_id, request_id))
}

/// The applied reply for one request decision.
pub fn request_reply_applied(command_id: &str, request_id: &str, turn_id: &str) -> Value {
    json!({
        "state": "applied",
        "command_id": command_id,
        "request_id": request_id,
        "turn_id": turn_id,
    })
}

/// Reply diagnostics: a failed call gets the native tail on its error. A
/// snapshot, or a reply whose state is `unknown`, `rejected` or `failed`,
/// gets the tail attached as `diagnostics`. `diagnose` maps an error through
/// the adapter's native client; `context` builds the error whose tail is
/// read for a successful reply.
pub fn attach_reply_diagnostics(
    result: Result<Value>,
    method: &str,
    harness: &str,
    diagnose: impl FnOnce(Error) -> Error,
    context: impl FnOnce(&Value) -> Error,
) -> Result<Value> {
    match result {
        Err(error) => Err(diagnose(error)),
        Ok(mut value) => {
            if method == "agent.snapshot"
                || value
                    .get("state")
                    .and_then(Value::as_str)
                    .is_some_and(|state| matches!(state, "unknown" | "rejected" | "failed"))
            {
                let diagnostics =
                    serde_json::to_value(diagnose(context(&value))).map_err(|_| {
                        Error::new(
                            "internal",
                            format!("{harness} diagnostics could not be encoded"),
                        )
                    })?;
                if let Some(tails) = diagnostics
                    .get("diagnostics")
                    .filter(|tails| !tails.is_null())
                {
                    value["diagnostics"] = tails.clone();
                }
            }
            Ok(value)
        }
    }
}

/// The frozen native command in a profile snapshot, or `profile_error` with
/// the adapter's own `message`.
pub fn required_native_cmd<'a>(snapshot: &'a Value, message: &str) -> Result<&'a str> {
    snapshot
        .get("cmd")
        .and_then(Value::as_str)
        .filter(|cmd| !cmd.trim().is_empty())
        .ok_or_else(|| Error::new("profile_error", message))
}

/// Register the owned native process as the `harness` role under this
/// Agent generation. `harness` is the display name (`"Muse"`,
/// `"OpenCode"`, `"Codex"`, `"Claude"`); `process_noun` is the owned
/// child (`"session host"`, `"server"`, `"app-server"`, `"CLI"`);
/// `scope_noun` is the
/// native scope no input was sent to (`"session"`, or `"thread"` for
/// Codex). The caller resolves the pid through its own child handle; a
/// missing or zero pid fails before any session or input is sent.
#[allow(
    clippy::too_many_arguments,
    reason = "The operation's distinct inputs remain explicit at its established call boundary."
)]
pub async fn register_native_process(
    peer: &PeerClient,
    credential: &str,
    agent_id: &str,
    generation: i64,
    pid: Option<u32>,
    harness: &str,
    process_noun: &str,
    scope_noun: &str,
    rpc_timeout: Duration,
) -> Result<()> {
    let pid = pid.filter(|pid| *pid > 0).ok_or_else(|| {
        Error::new(
            "process_registration",
            format!(
                "{harness} {process_noun} PID is unavailable; no {scope_noun} or input was sent"
            ),
        )
    })?;
    let value = peer
        .call(
            "process.put",
            json!({
                "credential": credential,
                "agent_id": agent_id,
                "generation": generation,
                "pid": pid,
                "role": "harness",
            }),
            Instant::now() + rpc_timeout,
        )
        .await
        .map_err(|error| {
            let message = format!(
                "{harness} {process_noun} process registration failed ({})",
                error.code
            );
            contextual_error(error, "process_registration", message)
        })?;
    let record: ProcessRecord = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "process_registration",
            format!("{harness} host returned an invalid process.put acknowledgement"),
        )
    })?;
    if record.identity.pid != pid
        || record.identity.host_boot_id.trim().is_empty()
        || record.identity.start_time == 0
        || record.owner != format!("agent:{agent_id}#{generation}")
        || record.role != "harness"
        || record.state != ProcessState::Running
        || record.exit_code.is_some()
    {
        return Err(Error::new(
            "process_registration",
            format!(
                "{harness} host did not verify the running {process_noun} under this Agent generation"
            ),
        ));
    }
    Ok(())
}

/// Native identity for one request event under the exact native `request_id`,
/// bounded like every command identity. `id_prefix` is the harness id
/// prefix (`"muse"`, `"claude"`); `harness` names it in diagnostics.
pub fn request_event_id(
    request_id: &str,
    kind: &str,
    id_prefix: &str,
    harness: &str,
) -> Result<String> {
    let short = match kind {
        "request.opened" => "o",
        "request.resolved" => "r",
        "request.closed" => "c",
        _ => {
            return Err(Error::new(
                "invalid",
                format!("{harness} request event kind is unsupported"),
            ));
        }
    };
    let native_id = format!("{id_prefix}-{short}:{request_id}");
    if native_id.len() > MAX_COMMAND_ID_BYTES || request_id.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            format!("{harness} native request event identity exceeded its bound"),
        ));
    }
    Ok(native_id)
}

/// Native event identity for one stream-owned fact.
pub fn event_id(
    session_id: &str,
    turn_id: &str,
    item_id: &str,
    kind: &str,
    id_prefix: &str,
    harness: &str,
) -> Result<String> {
    let native_id = format!("{id_prefix}:{session_id}:{turn_id}:{item_id}:{kind}");
    if native_id.len() > MAX_COMMAND_ID_BYTES
        || session_id.trim().is_empty()
        || item_id.trim().is_empty()
    {
        return Err(Error::new(
            "source_integrity",
            format!("{harness} native event identity exceeded its bound"),
        ));
    }
    Ok(native_id)
}

/// Attach a bounded redacted native error message to committed event data.
/// `None` (no native error) and data that cannot take the field are no-ops;
/// an over-budget message halves to char-boundary-safe redacted prefixes
/// until it fits, marking `truncated`.
pub fn attach_native_error(data: &mut Value, message: Option<&str>, policy: &SecretPolicy) {
    let Some(message) = message else { return };
    if ["native_error", "message", "truncated"]
        .into_iter()
        .any(|key| policy.contains_text(key))
    {
        return;
    }
    let Some(data) = data.as_object_mut() else {
        return;
    };
    let Ok(encoded) = serde_json::to_vec(&*data) else {
        return;
    };
    let budget = MAX_EVENT_BYTES
        .saturating_sub(encoded.len().saturating_add(r#","native_error":"#.len()))
        .min(4 * 1024);
    let mut message = policy.redact_text(message);
    let mut truncated = false;
    loop {
        let diagnostic = json!({"message": &message, "truncated": truncated});
        let Ok(encoded) = serde_json::to_vec(&diagnostic) else {
            return;
        };
        if encoded.len() <= budget {
            data.insert("native_error".to_owned(), diagnostic);
            return;
        }
        if message.is_empty() {
            return;
        }
        let mut end = message.len() / 2;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        // Preserve complete masks across the host's repeated sanitization.
        let (safe, _) = policy.redact_prefix(&message.as_bytes()[..end], false);
        message = String::from_utf8_lossy(&safe).into_owned();
        truncated = true;
    }
}

/// Translate an available native final reply into terminal turn data.
pub fn attach_final_text(data: &mut Value, final_text: &str) {
    let Some(fields) = data.as_object_mut() else {
        return;
    };
    fields.insert(
        "final_text".to_owned(),
        Value::String(final_text.to_owned()),
    );
    // Adapters can also retain the attached reply for a private snapshot.
    // Its smaller public event budget is applied when the Store writes it.
    eliot_core::limits::bound_terminal_text(data, eliot_core::limits::MAX_TERMINAL_WIRE_BYTES);
}

/// Attach a terminal provider-restriction fact to committed event data.
/// The key and its value define immutable wire semantics: refuse to write
/// when the redaction policy collides with the key or value, when the field
/// is already present, or when the encoded event would exceed its bound.
/// Returns whether the field was attached.
pub fn attach_provider_restriction(
    data: &mut Value,
    restriction: ProviderRestriction,
    policy: &SecretPolicy,
) -> bool {
    let Ok(value) = serde_json::to_value(restriction) else {
        return false;
    };
    // The key and actual value define immutable semantics and may not be redacted.
    if policy.contains_text("provider_restriction")
        || policy.contains_value(&value)
        || data
            .as_object()
            .is_none_or(|fields| fields.contains_key("provider_restriction"))
    {
        return false;
    }
    let mut candidate = data.clone();
    let Some(fields) = candidate.as_object_mut() else {
        return false;
    };
    fields.insert("provider_restriction".to_owned(), value.clone());
    if serde_json::to_vec(&candidate).map_or(true, |encoded| encoded.len() > MAX_EVENT_BYTES) {
        return false;
    }
    if let Some(fields) = data.as_object_mut() {
        fields.insert("provider_restriction".to_owned(), value);
        return true;
    }
    false
}

/// Whether a native reply carries no text after trimming. This is the rule
/// core applies to `message.completed` `text`, so a blank reply is never
/// offered to the store as a message fact.
pub fn is_blank_native_text(text: &str) -> bool {
    text.trim().is_empty()
}

/// The outcome of [`StreamSource::commit_message`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageCommit {
    /// The reply was blank after trimming: nothing was written, no gap opened.
    Blank,
    /// The message exceeded the persisted-event bound: one `message` gap was acknowledged.
    Gapped,
    /// The `message.completed` fact was acknowledged.
    Committed,
}

/// The stream-commit half of a harness event processor: the commit sink, the
/// host client, the bound identity, and the latches a failed commit sets.
/// Each plugin builds one per call from its own processor fields.
pub struct StreamSource<'a> {
    pub sink: &'a mut CommitSink,
    pub host: &'a PeerClient,
    pub credential: &'a str,
    pub agent_id: &'a str,
    pub generation: i64,
    /// Display name used in diagnostics, e.g. `"Muse"`.
    pub harness: &'a str,
    /// Native id prefix, e.g. `"muse"`.
    pub id_prefix: &'a str,
    /// Stream families owned by this source, for gap reports.
    pub families: &'a [&'a str],
    pub writes_blocked: &'a mut bool,
    pub gap_reported: &'a mut bool,
    pub gap_number: &'a mut u64,
    pub source_unknown: &'a mut bool,
    pub batch_unknown: &'a mut bool,
    pub failure: &'a mut Option<String>,
}

impl StreamSource<'_> {
    /// Send one native record and wait for the host's outcome.
    pub async fn commit_event(&mut self, kind: &str, native_id: String, data: Value) -> Result<()> {
        self.commit_event_with_refusal(kind, native_id, data)
            .await
            .map(|_| ())
    }

    /// Commit one exact native permission ask and return the daemon refusal,
    /// if its frozen project policy matched this request.
    pub async fn commit_request_event(
        &mut self,
        native_id: String,
        data: Value,
    ) -> Result<Option<PermissionRefusal>> {
        self.commit_event_with_refusal("request.opened", native_id, data)
            .await
    }

    async fn commit_event_with_refusal(
        &mut self,
        kind: &str,
        native_id: String,
        data: Value,
    ) -> Result<Option<PermissionRefusal>> {
        if *self.writes_blocked {
            return Err(Error::new(
                "unknown",
                format!("{} event acknowledgement is unresolved", self.harness),
            ));
        }
        let request_id = data
            .get("request_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let turn_id = data
            .get("turn_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let batch = SourceBatch {
            batch_id: format!("{native_id}:batch"),
            agent_id: self.agent_id.to_owned(),
            generation: self.generation,
            source: "stream".to_owned(),
            events: vec![NativeEvent {
                kind: kind.to_owned(),
                native_id,
                data,
                ts: None,
            }],
            cursor: None,
        };
        let ack = match self
            .sink
            .commit(batch, Instant::now() + EVENT_DEADLINE)
            .await
        {
            Ok(ack) => ack,
            Err(error) if crate::is_definite_batch_rejection(&error.code) => {
                return Err(error.with_where("plugin.events.put"));
            }
            Err(error) => {
                *self.writes_blocked = true;
                *self.source_unknown = true;
                *self.failure = Some(format!(
                    "{} event batch acknowledgement is unknown",
                    self.harness
                ));
                *self.batch_unknown = true;
                return Err(error);
            }
        };
        let Some(refusal) = ack.permission_refusal else {
            return Ok(None);
        };
        if kind != "request.opened"
            || refusal.agent_id != self.agent_id
            || refusal.generation != self.generation
            || request_id.as_deref() != Some(refusal.request_id.as_str())
            || turn_id.as_deref() != Some(refusal.turn_id.as_str())
        {
            *self.writes_blocked = true;
            *self.source_unknown = true;
            *self.failure = Some(format!(
                "{} permission refusal acknowledgement binding is unknown",
                self.harness
            ));
            *self.batch_unknown = true;
            return Err(Error::new(
                "unknown",
                format!(
                    "{} permission refusal acknowledgement binding is invalid",
                    self.harness
                ),
            ));
        }
        Ok(Some(refusal))
    }

    /// Commit one turn-bound `message.completed`. A blank native reply (its
    /// `text` is empty after trimming) is not a message fact: nothing is
    /// written and no gap opens. A valid native message whose encoded data
    /// exceeds the persisted-event bound is not persisted: it becomes one
    /// acknowledged `message` coverage gap, so the turn's native outcome, its
    /// exact-turn evidence and later facts survive.
    pub async fn commit_message(
        &mut self,
        native_id: String,
        data: Value,
    ) -> Result<MessageCommit> {
        if data
            .get("text")
            .and_then(Value::as_str)
            .is_some_and(is_blank_native_text)
        {
            return Ok(MessageCommit::Blank);
        }
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            let reason = format!(
                "{} completed message exceeded the persisted-event bound",
                self.harness
            );
            self.record_coverage_gap(&reason, &["message"]).await?;
            return Ok(MessageCommit::Gapped);
        }
        self.commit_event("message.completed", native_id, data)
            .await?;
        Ok(MessageCommit::Committed)
    }

    /// Record a native loss reported by the adapter.
    pub async fn record_gap(&mut self, reason: &str) -> Result<()> {
        self.record_gap_until(reason, Instant::now() + EVENT_DEADLINE)
            .await
    }

    /// Report missing observation coverage for `families` only, without
    /// declaring a native outcome unknown. The families stay local to this
    /// gap: the source keeps its configured families for later reports.
    pub async fn record_coverage_gap(&mut self, reason: &str, families: &[&str]) -> Result<()> {
        let deadline = Instant::now() + EVENT_DEADLINE;
        self.record_gap_inner_until(reason, families, (None, None), deadline, false)
            .await
    }

    pub async fn record_gap_until(&mut self, reason: &str, deadline: Instant) -> Result<()> {
        self.record_gap_range_until(reason, None, None, deadline)
            .await
    }

    /// Record the observed opaque cursor bounds without inferring positions inside them.
    pub async fn record_gap_range_until(
        &mut self,
        reason: &str,
        from: Option<String>,
        to: Option<String>,
        deadline: Instant,
    ) -> Result<()> {
        let families = self.families;
        self.record_gap_inner_until(reason, families, (from, to), deadline, true)
            .await
    }

    async fn record_gap_inner_until(
        &mut self,
        reason: &str,
        families: &[&str],
        bounds: (Option<String>, Option<String>),
        deadline: Instant,
        terminal_unknown: bool,
    ) -> Result<()> {
        if *self.writes_blocked {
            *self.source_unknown = true;
            return Err(Error::new(
                "unknown",
                format!(
                    "{} event acknowledgement is unresolved; no source gap was issued",
                    self.harness
                ),
            ));
        }
        if *self.gap_reported {
            return Err(Error::new(
                "unknown",
                format!("{} source already has an unresolved gap", self.harness),
            ));
        }
        if terminal_unknown {
            *self.source_unknown = true;
            *self.gap_reported = true;
        }
        let next = self.gap_number.saturating_add(1);
        *self.gap_number = next;
        let gap = SourceGap {
            gap_id: format!(
                "{}:{}:{}:gap-{}",
                self.id_prefix, self.agent_id, self.generation, next
            ),
            agent_id: self.agent_id.to_owned(),
            generation: self.generation,
            source: "stream".to_owned(),
            families: families.iter().map(|family| (*family).to_owned()).collect(),
            reason: reason.to_owned(),
            from: bounds.0,
            to: bounds.1,
        };
        let result = self.send_gap_until(&gap, deadline).await;
        match result {
            Ok(ack) if Self::is_canonical_gap_ack(&gap, &ack) => {
                if terminal_unknown {
                    *self.failure = Some(format!("{reason}; terminal outcome is unknown"));
                }
                Ok(())
            }
            Ok(_) | Err(_) => {
                *self.source_unknown = true;
                *self.gap_reported = true;
                *self.writes_blocked = true;
                *self.failure = Some(format!(
                    "{} source gap acknowledgement is unknown",
                    self.harness
                ));
                self.host.retire();
                Err(Error::new(
                    "unknown",
                    format!("{} source gap acknowledgement is unknown", self.harness),
                ))
            }
        }
    }

    async fn send_gap_until(&self, gap: &SourceGap, deadline: Instant) -> Result<Value> {
        self.host
            .call(
                "source.gap",
                json!({
                    "credential": self.credential,
                    "agent_id": gap.agent_id,
                    "generation": gap.generation,
                    "source": gap.source,
                    "gap_id": gap.gap_id,
                    "families": gap.families,
                    "reason": gap.reason,
                    "from": gap.from,
                    "to": gap.to,
                }),
                deadline,
            )
            .await
    }

    /// Whether `ack` canonically establishes `gap` (same gap_id, positive seq).
    fn is_canonical_gap_ack(gap: &SourceGap, ack: &Value) -> bool {
        ack.get("gap_id").and_then(Value::as_str) == Some(gap.gap_id.as_str())
            && ack
                .get("seq")
                .and_then(Value::as_i64)
                .is_some_and(|seq| seq > 0)
    }
}

/// Approval reply args: parsed with the shared `ApprovalArgs` syntax, then
/// re-encoded as an owned map so explicitly null fields are omitted
/// consistently. Present non-string or empty `choice_id` rejects at the
/// shared parse instead of degrading into an absent default downstream.
pub fn approval_reply_args(args: &Value) -> Result<serde_json::Map<String, Value>> {
    let parsed = eliot_core::requests::ApprovalArgs::parse(args)?;
    if parsed
        .choice_id
        .as_deref()
        .is_some_and(|choice_id| choice_id.trim().is_empty())
    {
        return Err(Error::new(
            "invalid",
            "approval choice_id must be a non-empty string",
        ));
    }
    let value = serde_json::to_value(&parsed)
        .map_err(|_| Error::new("internal", "approval reply args could not be encoded"))?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| Error::new("internal", "approval reply args could not be encoded"))
}

/// A native request field that must not be blank.
pub fn require_nonempty(value: &str, harness: &str, what: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            format!("{harness} native request has an empty {what}"),
        ));
    }
    Ok(())
}

/// Redact the free-text offer fields of a normalized public request view for
/// the committed `request.opened` event: the approval `summary`, `questions`
/// and approval `choices`. The in-memory offer stays raw so choice matching
/// and re-issue comparison keep working on exact native values; only the
/// persisted copy is redacted.
pub fn redact_request_details_for_commit(details: &Value, policy: &SecretPolicy) -> Value {
    let mut redacted = details.clone();
    if let Some(fields) = redacted.as_object_mut() {
        for key in ["summary", "questions", "choices"] {
            if let Some(field) = fields.get_mut(key) {
                policy.redact_values(field);
            }
        }
    }
    redacted
}

/// Extract the command supplied by a Bash or PowerShell permission callback.
pub fn shell_permission_command<'a>(tool: &str, input: Option<&'a Value>) -> Option<&'a str> {
    if tool.eq_ignore_ascii_case("bash") || tool.eq_ignore_ascii_case("powershell") {
        input?.get("command")?.as_str()
    } else {
        None
    }
}

/// Refuse a `model.list` or `quota.read` call on a session-bound plugin
/// instance. Catalogue and quota reads run on a dedicated reader instance
/// with no session, no retained start outcome and no observer; anything else
/// is caller error, never a native fact. The caller passes one
/// occupied-state predicate (`has_session || has_failed_start ||
/// has_observer`, including attach state where it exists).
pub fn ensure_dedicated_reader(occupied: bool, harness: &str, method: &str) -> Result<()> {
    if occupied {
        return Err(Error::new(
            "bad_state",
            format!("{harness} {method} requires a dedicated reader plugin instance"),
        ));
    }
    Ok(())
}

/// Refuse a `model.list` or `quota.read` call addressed to another harness.
/// The daemon routes each harness read to its own plugin; a mismatch is a
/// routing error, never a native fact about this harness. `harness_id` is the
/// snapshot id (`"codex"`) while `harness` is the display name (`"Codex"`)
/// used in diagnostics.
pub fn ensure_reader_harness(
    snapshot: &Value,
    harness_id: &str,
    harness: &str,
    method: &str,
) -> Result<()> {
    if snapshot.get("harness").and_then(Value::as_str) != Some(harness_id) {
        return Err(Error::new(
            "profile_error",
            format!("{harness} {method} received another harness"),
        ));
    }
    Ok(())
}

pub fn model_catalogue_profile<'a>(
    raw: &'a Value,
    occupied: bool,
    harness_id: &str,
    harness: &str,
    requires_wrapped_profile: bool,
) -> Result<&'a Value> {
    ensure_dedicated_reader(occupied, harness, "model.list")?;
    let profile = match raw.get("profile") {
        Some(profile) => profile,
        None if !requires_wrapped_profile => raw,
        None => {
            return Err(Error::new(
                "invalid",
                format!("{harness} model.list params are invalid"),
            ));
        }
    };
    ensure_reader_harness(profile, harness_id, harness, "model.list")?;
    Ok(profile)
}

pub enum LaunchModelRequest<'a> {
    Explicit(&'a str),
    Frozen(&'a str),
    CatalogueDefault,
    VendorDefault,
}

pub enum LaunchModelMissing {
    Explicit,
    Frozen,
    Default,
}

/// Select the exact explicit or frozen model. An omitted selection can
/// delegate to a vendor that reports its default at session creation.
pub fn choose_launch_model<'a, T>(
    entries: &'a [T],
    request: LaunchModelRequest<'_>,
    id: impl Fn(&T) -> &str,
    exact: impl Fn(&T, &str) -> bool,
    is_default: impl Fn(&T) -> bool,
) -> std::result::Result<Option<&'a T>, LaunchModelMissing> {
    let selected = match request {
        LaunchModelRequest::Explicit(requested) => entries
            .iter()
            .find(|entry| id(entry) == requested && exact(entry, requested))
            .ok_or(LaunchModelMissing::Explicit)?,
        LaunchModelRequest::Frozen(frozen) => entries
            .iter()
            .find(|entry| id(entry) == frozen)
            .ok_or(LaunchModelMissing::Frozen)?,
        LaunchModelRequest::CatalogueDefault => entries
            .iter()
            .find(|entry| is_default(entry))
            .ok_or(LaunchModelMissing::Default)?,
        LaunchModelRequest::VendorDefault => {
            return if entries.is_empty() {
                Err(LaunchModelMissing::Default)
            } else {
                Ok(None)
            };
        }
    };
    Ok(Some(selected))
}

/// Close the owned event task after stop. Teardown after a transport loss
/// still closes the task if its stop reply could not be proved.
pub async fn shutdown_session<S>(
    session: Option<&mut S>,
    best_effort: bool,
    stop: impl AsyncFnOnce(&mut S) -> Result<()>,
    close: impl AsyncFnOnce(&mut S),
) -> Result<Value> {
    if let Some(session) = session {
        let stopped = stop(session).await;
        if !best_effort {
            stopped?;
        }
        close(session).await;
    }
    Ok(json!({"state": "shutdown"}))
}

/// Stop a process-owned native session and verify output and source drain.
/// A failed begin, process close or drain must still allow the remaining
/// bounded cleanup stages. A caller with an end-to-end budget supplies the
/// native and settlement cutoffs from that budget.
pub async fn stop_owned_session<P>(
    harness: &str,
    deadlines: Option<(Instant, Instant)>,
    begin: impl std::future::Future<Output = Result<()>>,
    native: impl std::future::Future<Output = (Result<P>, Result<()>)>,
    end: impl std::future::Future<Output = Result<()>>,
) -> Result<()> {
    let began = if let Some((native_deadline, _)) = deadlines {
        let began = Instant::now() < native_deadline
            && matches!(
                tokio::time::timeout_at(native_deadline, begin).await,
                Ok(Ok(()))
            );
        if Instant::now() >= native_deadline {
            return Err(Error::new(
                "unknown",
                format!("{harness} stop deadline elapsed before native termination"),
            ));
        }
        began
    } else {
        begin.await.is_ok()
    };
    let (process_exit, drained) = native.await;
    let ended = if let Some((_, settlement_deadline)) = deadlines {
        Instant::now() < settlement_deadline
            && matches!(
                tokio::time::timeout_at(settlement_deadline, end).await,
                Ok(Ok(()))
            )
            && Instant::now() < settlement_deadline
    } else {
        end.await.is_ok()
    };
    if began && process_exit.is_ok() && drained.is_ok() && ended {
        Ok(())
    } else {
        Err(Error::new(
            "unknown",
            format!("{harness} process/output drain or session.ended outcome is unknown"),
        ))
    }
}

pub fn unconfirmed_control(acknowledged: bool, pending: &str, withdrawal: &str) -> Value {
    unknown_control(if acknowledged { pending } else { withdrawal })
}

/// A cancellation notification only proves dispatch. Applied requires an
/// observed cancellation terminal for the same bound turn.
pub fn interrupt_settlement_outcome(
    harness: &str,
    command_id: &str,
    turn_id: &str,
    cancelled: Option<bool>,
) -> Result<Value> {
    match cancelled {
        Some(true) => Ok(json!({
            "state": "applied",
            "command_id": command_id,
            "turn_id": turn_id,
        })),
        Some(false) => Err(Error::new(
            "unknown",
            format!("{harness} interrupt raced natural turn completion"),
        )),
        None => Err(Error::new(
            "unknown",
            format!("{harness} interrupt settlement is unknown"),
        )),
    }
}

#[derive(Clone, Copy)]
pub enum RetainedStopStep {
    Begin,
    VerifyRetention,
    Drain,
    Process,
    End,
    Diagnostics,
}

/// For a retained native session, graceful stop drains the source while
/// leaving process exit to the daemon. Forced cleanup can publish End only
/// after proving closure of its owned process.
pub async fn stop_retained_session(
    harness: &str,
    force: bool,
    mut step: impl AsyncFnMut(RetainedStopStep) -> Result<()>,
) -> Result<()> {
    use RetainedStopStep::{Begin, Diagnostics, Drain, End, Process, VerifyRetention};

    let began = step(Begin).await.is_ok();
    let retained = began && step(VerifyRetention).await.is_ok();
    let unknown = |stage| {
        Error::new(
            "unknown",
            format!("{harness} stop outcome is unknown at stage {stage}"),
        )
    };
    if !force {
        let drained = step(Drain).await.is_ok();
        return if retained && drained {
            Ok(())
        } else {
            Err(unknown(if began { "retention" } else { "begin" }))
        };
    }
    if step(Process).await.is_err() {
        return Err(unknown("end_ack"));
    }
    let ended = step(End).await.is_ok();
    let _ = step(Diagnostics).await;
    if retained && ended {
        Ok(())
    } else {
        Err(unknown("end_ack"))
    }
}
