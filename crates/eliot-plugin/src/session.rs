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

use std::{collections::HashMap, ffi::OsString, path::Path, time::Duration};

use eliot_core::{
    Error, NativeEvent, ProcessRecord, ProcessState, Result, SecretPolicy, SourceBatch,
};
use serde_json::{Map, Value, json};
use tokio::time::Instant;

use crate::{
    AgentSnapshotParams, CommitSink, MAX_EVENT_BYTES, PeerClient, protocol::MAX_COMMAND_ID_BYTES,
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

/// Map the frozen `target_environment` object to child variables, collecting
/// secret values (secret names plus explicit `pass_env` hits) for redaction.
/// `harness` names the caller in diagnostics.
pub fn target_environment(
    environment: Option<&Value>,
    pass_env: Option<&Value>,
    harness: &str,
) -> Result<TargetEnvironment> {
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

/// Whether `command_id` is a retained command of this generation: the start,
/// a dispatched input, or the stop. The session fields are passed by
/// reference because each plugin owns its own `AgentSession` type.
pub fn known_command(
    start_command_id: &str,
    command_inputs: &HashMap<String, String>,
    stop_command_id: &Option<String>,
    command_id: &str,
) -> bool {
    start_command_id == command_id
        || command_inputs.contains_key(command_id)
        || stop_command_id.as_deref() == Some(command_id)
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

/// Re-code an error for its propagation context, keeping its diagnostics.
pub fn contextual_error(
    error: Error,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Error {
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

/// Register the owned native process as the `harness` role under this
/// Agent generation. `harness` is the display name (`"Muse"`,
/// `"OpenCode"`, `"Codex"`); `process_noun` is the owned child
/// (`"session host"`, `"server"`, `"app-server"`); `scope_noun` is the
/// native scope no input was sent to (`"session"`, or `"thread"` for
/// Codex). The caller resolves the pid through its own child handle; a
/// missing or zero pid fails before any session or input is sent.
#[allow(clippy::too_many_arguments)]
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

/// Attach terminal `final_text` to committed turn data. A failed turn whose
/// text would exceed the event bound records `final_text_unavailable`
/// instead; anything else keeps the text as committed.
pub fn attach_final_text(data: &mut Value, final_text: &str, failed: bool, policy: &SecretPolicy) {
    let mut candidate = data.clone();
    let Some(fields) = candidate.as_object_mut() else {
        return;
    };
    fields.insert(
        "final_text".to_owned(),
        Value::String(final_text.to_owned()),
    );
    let exceeds_event_bound =
        serde_json::to_vec(&candidate).map_or(true, |encoded| encoded.len() > MAX_EVENT_BYTES);
    if failed && exceeds_event_bound {
        attach_final_text_unavailable(data, policy);
        return;
    }
    *data = candidate;
}

/// Record that terminal text exists but exceeded the event bound. Refuses to
/// write when the marker words collide with a redacted secret or the field
/// is already present, and when even the marker would exceed the bound.
pub fn attach_final_text_unavailable(data: &mut Value, policy: &SecretPolicy) {
    if ["final_text_unavailable", "event_budget"]
        .iter()
        .any(|literal| policy.contains_text(literal))
        || data
            .as_object()
            .is_none_or(|fields| fields.contains_key("final_text_unavailable"))
    {
        return;
    }
    let mut candidate = data.clone();
    let Some(fields) = candidate.as_object_mut() else {
        return;
    };
    fields.insert(
        "final_text_unavailable".to_owned(),
        Value::String("event_budget".to_owned()),
    );
    if serde_json::to_vec(&candidate).map_or(true, |encoded| encoded.len() > MAX_EVENT_BYTES) {
        return;
    }
    if let Some(fields) = data.as_object_mut() {
        fields.insert(
            "final_text_unavailable".to_owned(),
            Value::String("event_budget".to_owned()),
        );
    }
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
    /// Commit one native record. An unknown acknowledgement latches the
    /// source (no replay is ever allowed); any other commit failure records
    /// an explicit gap instead.
    pub async fn commit_event(&mut self, kind: &str, native_id: String, data: Value) -> Result<()> {
        if *self.writes_blocked {
            return Err(Error::new(
                "unknown",
                format!("{} event acknowledgement is unresolved", self.harness),
            ));
        }
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
        let error = match self
            .sink
            .commit(batch, Instant::now() + EVENT_DEADLINE)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };
        if self.sink.is_unknown() {
            *self.writes_blocked = true;
            *self.source_unknown = true;
            *self.failure = Some(format!(
                "{} event batch acknowledgement is unknown",
                self.harness
            ));
            *self.batch_unknown = true;
            return Err(Error::new(
                "unknown",
                format!("{} event batch acknowledgement is unknown", self.harness),
            ));
        }
        let reason = format!("{kind} could not be committed: {}", error.code);
        self.record_gap(&reason).await?;
        Err(Error::new(
            "unknown",
            format!(
                "{} native fact was not committed; no replay is allowed",
                self.harness
            ),
        ))
    }

    /// Record one explicit source gap. At most one gap is ever outstanding;
    /// a second loss while it stands, or an ambiguous gap acknowledgement,
    /// latches the source unknown.
    pub async fn record_gap(&mut self, reason: &str) -> Result<()> {
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
        *self.source_unknown = true;
        *self.gap_reported = true;
        let next = self.gap_number.saturating_add(1);
        *self.gap_number = next;
        let gap_id = format!(
            "{}:{}:{}:gap-{}",
            self.id_prefix, self.agent_id, self.generation, next
        );
        let ack_id = gap_id.clone();
        let result = self
            .host
            .call(
                "source.gap",
                json!({
                    "credential": self.credential,
                    "agent_id": self.agent_id,
                    "generation": self.generation,
                    "source": "stream",
                    "gap_id": gap_id,
                    "families": self.families,
                    "reason": reason,
                    "from": null,
                    "to": null,
                }),
                Instant::now() + EVENT_DEADLINE,
            )
            .await;
        match result {
            Ok(ack)
                if ack.get("gap_id").and_then(Value::as_str) == Some(ack_id.as_str())
                    && ack
                        .get("seq")
                        .and_then(Value::as_i64)
                        .is_some_and(|seq| seq > 0) =>
            {
                *self.failure = Some(format!("{reason}; terminal outcome is unknown"));
                Ok(())
            }
            Ok(_) | Err(_) => {
                *self.writes_blocked = true;
                *self.failure = Some(format!(
                    "{} source gap acknowledgement is unknown",
                    self.harness
                ));
                Err(Error::new(
                    "unknown",
                    format!("{} source gap acknowledgement is unknown", self.harness),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{optional_profile_text, request_event_id, validate_source_plan};
    use serde_json::json;

    /// The shared source-plan check spells every harness message exactly as
    /// the per-plugin copies did (pure move).
    #[test]
    fn source_plan_messages_match_the_per_harness_originals() {
        for (harness, families, joined) in [
            (
                "OpenCode",
                &["session", "turn", "message"][..],
                "session, turn, and message",
            ),
            (
                "Muse",
                &["session", "turn", "message", "request"][..],
                "session, turn, message, and request",
            ),
            (
                "Claude",
                &["session", "turn", "message", "request"][..],
                "session, turn, message, and request",
            ),
            (
                "Codex",
                &["session", "turn", "message", "tool", "request"][..],
                "session, turn, message, tool, and request",
            ),
        ] {
            let error = validate_source_plan(harness, families, None).expect_err("missing plan");
            assert_eq!(error.code, "unsupported");
            assert_eq!(
                error.message,
                format!("{harness} start requires an installed stream source plan")
            );
            let error = validate_source_plan(harness, families, Some(&json!({})))
                .expect_err("unowned families");
            assert_eq!(error.code, "unsupported");
            assert_eq!(
                error.message,
                format!("{harness} start requires stream ownership for {joined}")
            );
        }
    }

    /// The shared request ids and profile texts spell every harness exactly
    /// as the per-plugin copies did (pure move).
    #[test]
    fn request_ids_and_profile_text_match_the_per_harness_originals() {
        for (harness, id_prefix) in [("Muse", "muse"), ("Claude", "claude")] {
            for (kind, short) in [
                ("request.opened", "o"),
                ("request.resolved", "r"),
                ("request.closed", "c"),
            ] {
                assert_eq!(
                    request_event_id("req-1", kind, id_prefix, harness).expect("request id"),
                    format!("{id_prefix}-{short}:req-1")
                );
            }
            let error = request_event_id("req-1", "request.unknown", id_prefix, harness)
                .expect_err("unsupported kind");
            assert_eq!(error.code, "invalid");
            assert_eq!(
                error.message,
                format!("{harness} request event kind is unsupported")
            );
            let error = request_event_id("", "request.opened", id_prefix, harness)
                .expect_err("empty request id");
            assert_eq!(error.code, "source_integrity");
            let profile = json!({"mode": 7}).as_object().cloned().unwrap();
            let error = optional_profile_text(&profile, "mode", harness).expect_err("non-string");
            assert_eq!(error.code, "profile_error");
            assert_eq!(
                error.message,
                format!("{harness} profile field mode must be a non-empty string")
            );
            assert_eq!(
                optional_profile_text(&profile, "absent", harness).expect("absent"),
                None
            );
        }
    }
}
