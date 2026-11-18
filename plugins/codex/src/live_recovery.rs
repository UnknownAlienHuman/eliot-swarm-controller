//! Publication of normalized Codex rollout facts and verified file boundaries.

use eliot_core::{
    Error, NativeEvent, Result, SecretPolicy, SourceBatch, SourceGap, SourceGapAck, SourceHandoff,
    SourceHandoffAck, SourceHandoffGap, SourceHealthUpdate, api::MAX_OBSERVATION_CURSOR_BYTES,
    parse_agent_id,
};
use eliot_plugin::{CommitSink, ObserverAttachParams, PeerClient, protocol::MAX_CREDENTIAL_BYTES};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::time::Instant;
use ulid::Ulid;

const OBSERVER_SOURCE: &str = "observe";
const TRANSCRIPT_SOURCE: &str = "transcript";
const MECHANISM: &str = "rollout_tail";
const LIVE_FAMILIES: [&str; 3] = ["turn", "message", "tool"];
const BASELINE_REASON: &str = "codex_observer_live_baseline";

/// Owns one Codex observer's durable live-source operations.
pub(super) struct RecoveryPublisher {
    agent_id: String,
    generation: i64,
    families: Vec<String>,
    client: PeerClient,
    credential: String,
    policy: Arc<SecretPolicy>,
    sink: CommitSink,
    observer_ready: bool,
    last_checkpoint: Option<String>,
}

impl RecoveryPublisher {
    pub(super) fn new(
        attach: &ObserverAttachParams,
        client: PeerClient,
        credential: String,
        policy: Arc<SecretPolicy>,
    ) -> Result<Self> {
        parse_agent_id(&attach.agent_id)?;
        if attach.generation < 0 || attach.mode != "observe" {
            return Err(Error::new(
                "invalid",
                "Codex live publisher requires a bounded observe attachment",
            ));
        }
        if credential.is_empty() || credential.len() > MAX_CREDENTIAL_BYTES {
            return Err(Error::new(
                "invalid",
                "Codex live publisher credential is outside its size limit",
            ));
        }
        if policy.contains_text(&attach.agent_id) {
            return Err(secret_error());
        }

        let plan = attach
            .source_plan
            .as_ref()
            .and_then(Value::as_object)
            .ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "Codex observer attachment omitted its captured source plan",
                )
            })?;
        let mut families: Vec<_> = LIVE_FAMILIES
            .iter()
            .filter(|family| plan.get(**family).and_then(Value::as_str) == Some("native_read"))
            .map(|family| (*family).to_owned())
            .collect();
        families.sort_unstable();
        if families.is_empty() {
            return Err(Error::new(
                "not_allowed",
                "Codex observer attachment grants no normalized live families",
            ));
        }
        if families.iter().any(|family| policy.contains_text(family)) {
            return Err(secret_error());
        }

        Ok(Self {
            agent_id: attach.agent_id.clone(),
            generation: attach.generation,
            families,
            sink: CommitSink::new(client.clone(), credential.clone()),
            client,
            credential,
            policy,
            observer_ready: false,
            last_checkpoint: None,
        })
    }

    /// Only an attached observer with the Store-derived proof may publish.
    pub(super) async fn observer_ready(&mut self, deadline: Instant) -> Result<bool> {
        if self.observer_ready {
            return Ok(true);
        }
        let state = self
            .call_with_credential(
                "source.state",
                json!({
                    "agent_id": self.agent_id,
                    "generation": self.generation,
                    "source": OBSERVER_SOURCE,
                }),
                deadline,
            )
            .await?;
        self.require_state_scope(&state, OBSERVER_SOURCE)?;
        if state.get("state").and_then(Value::as_str) != Some("open") {
            return Err(Error::new(
                "stale_generation",
                "Codex observer source is no longer open",
            ));
        }
        self.observer_ready = state
            .get("observer_ready")
            .and_then(Value::as_bool)
            .ok_or_else(unknown_error)?;
        Ok(self.observer_ready)
    }

    /// Transfer the captured observer families at a verified live-file edge.
    pub(super) async fn begin(
        &mut self,
        edge: &str,
        reason: Option<&str>,
        deadline: Instant,
    ) -> Result<()> {
        self.ensure_ready()?;
        let mut request = self.new_handoff(edge, reason.unwrap_or(BASELINE_REASON))?;
        request
            .prepare(&self.policy)
            .map_err(|error| self.safe_error(error, "source handoff preparation"))?;
        request
            .validate()
            .map_err(|error| self.safe_error(error, "source handoff validation"))?;
        let value = serde_json::to_value(&request).map_err(|_| unknown_error())?;
        let value = self
            .call_with_credential("source.handoff", value, deadline)
            .await?;
        let ack: SourceHandoffAck = serde_json::from_value(value).map_err(|_| unknown_error())?;
        request.validate_ack(&ack).map_err(|_| unknown_error())?;
        self.last_checkpoint = Some(edge.to_owned());
        Ok(())
    }

    /// Commit a complete normalized native record with its exclusive cursor.
    pub(super) async fn commit(
        &mut self,
        mut events: Vec<NativeEvent>,
        cursor: String,
        deadline: Instant,
    ) -> Result<String> {
        self.ensure_ready()?;
        self.prepare_events(&mut events, &cursor)?;
        let batch = SourceBatch {
            batch_id: format!("codex-live-batch:{}", Ulid::new()),
            agent_id: self.agent_id.clone(),
            generation: self.generation,
            source: TRANSCRIPT_SOURCE.to_owned(),
            events,
            cursor: Some(cursor),
        };
        if self.policy.contains_text(&batch.batch_id) {
            return Err(secret_error());
        }
        let ack = self
            .sink
            .commit(batch, deadline)
            .await
            .map_err(|error| self.safe_error(error, "native event commit"))?;
        let checkpoint = ack.cursor.ok_or_else(unknown_error)?;
        self.last_checkpoint = Some(checkpoint.clone());
        Ok(checkpoint)
    }

    /// Report an uncovered native interval with its observed boundary, if any.
    pub(super) async fn gap(
        &mut self,
        reason: &str,
        to: Option<String>,
        deadline: Instant,
    ) -> Result<()> {
        self.ensure_ready()?;
        let gap = self.new_gap(reason, to)?;
        let value = serde_json::to_value(&gap).map_err(|_| unknown_error())?;
        let value = self
            .call_with_credential("source.gap", value, deadline)
            .await?;
        let ack: SourceGapAck = serde_json::from_value(value).map_err(|_| unknown_error())?;
        if ack.gap_id != gap.gap_id || ack.seq <= 0 {
            return Err(unknown_error());
        }
        if gap.to.is_some() {
            self.last_checkpoint = gap.to;
        }
        Ok(())
    }

    pub(super) async fn health(&self, source: &str, health: &str, deadline: Instant) -> Result<()> {
        self.ensure_ready()?;
        let update =
            SourceHealthUpdate::new(self.agent_id.clone(), self.generation, source, health);
        update
            .validate()
            .map_err(|error| self.safe_error(error, "source health validation"))?;
        if source != OBSERVER_SOURCE && source != TRANSCRIPT_SOURCE {
            return Err(Error::new(
                "not_allowed",
                "Codex live health source is outside the captured observer grant",
            ));
        }
        if [&update.agent_id, &update.source, &update.health]
            .iter()
            .any(|value| self.policy.contains_text(value))
        {
            return Err(secret_error());
        }
        let value = serde_json::to_value(&update).map_err(|_| unknown_error())?;
        let value = self
            .call_with_credential("source.health", value, deadline)
            .await?;
        let ack: SourceHealthUpdate = serde_json::from_value(value).map_err(|_| unknown_error())?;
        if ack != update {
            return Err(unknown_error());
        }
        Ok(())
    }

    fn new_handoff(&self, edge: &str, reason: &str) -> Result<SourceHandoff> {
        self.validate_edge(edge)?;
        let request = SourceHandoff {
            agent_id: self.agent_id.clone(),
            generation: self.generation,
            source: OBSERVER_SOURCE.to_owned(),
            families: self.families.clone(),
            successor: TRANSCRIPT_SOURCE.to_owned(),
            mechanism: MECHANISM.to_owned(),
            cursor: None,
            gap: Some(SourceHandoffGap {
                gap_id: self.new_gap_id(),
                reason: reason.to_owned(),
                from: None,
                to: Some(edge.to_owned()),
            }),
        };
        Ok(request)
    }

    fn new_gap(&self, reason: &str, to: Option<String>) -> Result<SourceGap> {
        if let Some(edge) = to.as_deref() {
            self.validate_edge(edge)?;
        }
        let gap = SourceGap {
            gap_id: self.new_gap_id(),
            agent_id: self.agent_id.clone(),
            generation: self.generation,
            source: TRANSCRIPT_SOURCE.to_owned(),
            families: self.families.clone(),
            reason: self.policy.redact_text(reason),
            from: self.last_checkpoint.clone(),
            to,
        };
        if gap.reason.trim().is_empty() {
            return Err(Error::new("invalid", "Codex live gap reason is empty"));
        }
        // Redaction is limited to the mutable reason. Immutable bindings are
        // rejected instead of being rewritten before digesting the operation.
        if self.policy.contains_text(&gap.gap_id)
            || self.policy.contains_text(&gap.agent_id)
            || self.policy.contains_text(&gap.source)
            || gap
                .families
                .iter()
                .any(|family| self.policy.contains_text(family))
            || [&gap.from, &gap.to]
                .into_iter()
                .flatten()
                .any(|cursor| self.policy.contains_text(cursor))
        {
            return Err(secret_error());
        }
        Ok(gap)
    }

    fn new_gap_id(&self) -> String {
        format!("codex-live-gap:{}", Ulid::new())
    }

    fn prepare_events(&self, events: &mut [NativeEvent], cursor: &str) -> Result<()> {
        self.validate_edge(cursor)?;
        if self.policy.contains_text(&self.agent_id) || self.policy.contains_text(TRANSCRIPT_SOURCE)
        {
            return Err(secret_error());
        }
        if events.is_empty() {
            return Err(Error::new("invalid", "Codex live batch contains no events"));
        }
        for event in events {
            if self.policy.contains_text(&event.kind) || self.policy.contains_text(&event.native_id)
            {
                return Err(secret_error());
            }
            let family = family_for_kind(&event.kind).ok_or_else(|| {
                Error::new(
                    "not_allowed",
                    "Codex live event family is outside its grant",
                )
            })?;
            if !self.families.iter().any(|allowed| allowed == family) {
                return Err(Error::new(
                    "not_allowed",
                    "Codex live event family is outside its captured grant",
                ));
            }
            if event.ts.is_none() || !event.data.is_object() {
                return Err(Error::new(
                    "source_integrity",
                    "Codex live event omitted its normalized timestamp or object data",
                ));
            }
            // The rollout normalizer already redacts values before deriving
            // native IDs. A change here means the immutable ID no longer binds
            // the normalized event, so fail closed rather than rewriting it.
            if self.policy.redact_values(&mut event.data) || self.policy.contains_value(&event.data)
            {
                return Err(secret_error());
            }
            if ["command_id", "cause", "request_id"]
                .iter()
                .any(|field| event.data.get(*field).is_some())
            {
                return Err(Error::new(
                    "not_allowed",
                    "Codex live facts cannot settle commands or requests",
                ));
            }
        }
        Ok(())
    }

    fn validate_edge(&self, edge: &str) -> Result<()> {
        if edge.trim().is_empty() || edge.len() > MAX_OBSERVATION_CURSOR_BYTES {
            return Err(Error::new("invalid", "Codex live file edge is invalid"));
        }
        if self.policy.contains_text(edge) {
            return Err(secret_error());
        }
        Ok(())
    }

    fn ensure_ready(&self) -> Result<()> {
        if !self.observer_ready {
            return Err(Error::new(
                "not_allowed",
                "Codex live publication requires settled observer readiness",
            ));
        }
        Ok(())
    }

    fn require_state_scope(&self, state: &Value, source: &str) -> Result<()> {
        if state.get("agent_id").and_then(Value::as_str) != Some(self.agent_id.as_str())
            || state.get("generation").and_then(Value::as_i64) != Some(self.generation)
            || state.get("source").and_then(Value::as_str) != Some(source)
        {
            return Err(Error::new(
                "stale_generation",
                "Codex source-state proof has a different captured binding",
            ));
        }
        Ok(())
    }

    async fn call_with_credential(
        &self,
        method: &str,
        mut params: Value,
        deadline: Instant,
    ) -> Result<Value> {
        if self.credential.len() > MAX_CREDENTIAL_BYTES {
            return Err(Error::new(
                "invalid",
                "Codex source credential is too large",
            ));
        }
        let Some(object) = params.as_object_mut() else {
            return Err(Error::new(
                "internal",
                "Codex source request was not an object",
            ));
        };
        object.insert(
            "credential".to_owned(),
            Value::String(self.credential.clone()),
        );
        self.client
            .call(method, params, deadline)
            .await
            .map_err(|error| self.safe_error(error, "live source request"))
    }

    fn safe_error(&self, error: Error, operation: &str) -> Error {
        let safe_code = !error.code.is_empty()
            && error.code.len() <= 128
            && error.code.is_ascii()
            && error
                .code
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-/.:".contains(&byte))
            && !self.policy.contains_text(&error.code);
        Error::new(
            if safe_code {
                error.code
            } else {
                "unknown".to_owned()
            },
            format!("Codex {operation} failed"),
        )
    }
}

fn family_for_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "turn.started" | "turn.completed" => Some("turn"),
        "message.completed" => Some("message"),
        "tool.started" | "tool.completed" => Some("tool"),
        _ => None,
    }
}

fn secret_error() -> Error {
    Error::new(
        "secret_in_payload",
        "Codex live source metadata contains a known secret",
    )
}

fn unknown_error() -> Error {
    Error::new("unknown", "Codex live source acknowledgement was invalid")
}
