//! Shared per-Agent session helpers for the stream-owning harness plugins.
//!
//! Codex, Muse and OpenCode bind one Agent generation to one owned native
//! server: the target-environment mapping, identity/binding validation,
//! unknown snapshots, contextual errors, stream event commits/gaps and the
//! bounded native-error attachment are identical up to the harness name.
//! This module holds the single copy; each plugin passes its own display
//! name (`"Codex"`, `"Muse"`, `"OpenCode"`) and id prefix (`"codex"`,
//! `"muse"`, `"opencode"`). No trait: the shared state is passed by
//! reference through [`StreamSource`].

use std::{ffi::OsString, time::Duration};

use eliot_core::{Error, NativeEvent, Result, SecretPolicy, SourceBatch};
use serde_json::{Value, json};
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
