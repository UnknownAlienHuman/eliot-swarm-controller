//! Observe-only publication of normalized Codex rollout facts.
//!
//! Private observation pages never enter this module. It publishes only the
//! captured observer grant and retains one frozen operation until its exact
//! acknowledgement is returned or proved by private source-state readback.

use eliot_core::{
    BatchAck, Error, NativeEvent, Result, SecretPolicy, SourceBatch, SourceGap, SourceGapAck,
    SourceHandoff, SourceHandoffAck, SourceHandoffGap, SourceHealthUpdate, canonical_json_sha256,
    parse_agent_id,
};
use eliot_plugin::{
    CommitSink, ObserverAttachParams, PeerClient,
    protocol::{MAX_CREDENTIAL_BYTES, MAX_OBSERVATION_CURSOR_BYTES},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, sleep_until};
use ulid::Ulid;

const OBSERVER_SOURCE: &str = "observe";
const TRANSCRIPT_SOURCE: &str = "transcript";
const SUCCESSOR: &str = "transcript";
const MECHANISM: &str = "rollout_tail";
const LIVE_FAMILIES: [&str; 3] = ["turn", "message", "tool"];
const BASELINE_REASON: &str = "codex_observer_live_baseline";
const RECOVERY_REASON: &str = "codex_observer_live_recovery";

#[derive(Clone, Copy, PartialEq, Eq)]
enum GapPurpose {
    Unavailable,
    Recovery,
}

#[derive(Clone)]
enum PendingOperation {
    Handoff(SourceHandoff),
    Batch(SourceBatch),
    Gap(GapPurpose, SourceGap),
    Health(SourceHealthUpdate),
    PreHandoffUnavailable {
        reason: String,
        update: SourceHealthUpdate,
    },
}

#[derive(Clone)]
enum SettledOperation {
    Gap(GapPurpose, SourceGap, SourceGapAck),
    Health(SourceHealthUpdate),
    PreHandoffUnavailable {
        reason: String,
        update: SourceHealthUpdate,
    },
}

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
    handoff_edge: Option<String>,
    pre_handoff_reason: Option<String>,
    pending: Option<PendingOperation>,
    settled: Option<SettledOperation>,
    last_checkpoint: Option<String>,
    last_health: Option<SourceHealthUpdate>,
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
            handoff_edge: None,
            pre_handoff_reason: None,
            pending: None,
            settled: None,
            last_checkpoint: None,
            last_health: None,
        })
    }

    /// Wait for the Store-derived attach proof before issuing any mutation.
    pub(super) async fn wait_observer_ready(&mut self, deadline: Instant) -> Result<bool> {
        loop {
            if Instant::now() >= deadline {
                return Ok(false);
            }
            let state = match self.read_state(OBSERVER_SOURCE, deadline).await {
                Ok(state) => state,
                Err(_) if Instant::now() >= deadline => return Ok(false),
                Err(error) => return Err(error),
            };
            self.require_state_scope(&state, OBSERVER_SOURCE)?;
            if state.get("state").and_then(Value::as_str) != Some("open") {
                return Err(Error::new(
                    "stale_generation",
                    "Codex observer source is no longer open",
                ));
            }
            match state.get("observer_ready").and_then(Value::as_bool) {
                Some(true) => {
                    self.observer_ready = true;
                    return Ok(true);
                }
                Some(false) => {
                    let wake = (Instant::now() + Duration::from_millis(10)).min(deadline);
                    sleep_until(wake).await;
                }
                None => return Err(unknown_error()),
            }
        }
    }

    /// Reconcile the retained operation before the caller reads another record.
    /// A non-batch proof is kept for the same caller method to consume.
    pub(super) async fn settle(&mut self, deadline: Instant) -> Result<Option<BatchAck>> {
        let Some(operation) = self.pending.clone() else {
            return Ok(None);
        };
        let source = operation.source();
        let state = self.read_state(source, deadline).await?;
        if self.require_state_scope(&state, source).is_err() {
            return Ok(None);
        }

        match operation {
            PendingOperation::Batch(batch) => {
                let ack = match self.sink.settle_from_state(&batch, &state) {
                    Ok(ack) => ack,
                    Err(_) => return Ok(None),
                };
                self.pending = None;
                self.last_checkpoint = ack.cursor.clone();
                Ok(Some(ack))
            }
            PendingOperation::Handoff(request) => {
                if request.ack_from_state(&state).is_err() {
                    return Ok(None);
                }
                self.last_checkpoint = request.gap.as_ref().and_then(|gap| gap.to.clone());
                self.handoff_edge = self.last_checkpoint.clone();
                self.pending = None;
                Ok(None)
            }
            PendingOperation::Gap(purpose, gap) => {
                let ack = match gap_ack_from_state(&gap, &state) {
                    Some(ack) => ack,
                    None => return Ok(None),
                };
                if self.settled.is_some() {
                    return Err(unknown_error());
                }
                if let Some(checkpoint) = &gap.to {
                    self.last_checkpoint = Some(checkpoint.clone());
                }
                self.pending = None;
                self.settled = Some(SettledOperation::Gap(purpose, gap, ack));
                Ok(None)
            }
            PendingOperation::Health(update) => {
                if !health_state_proves(&update, &state, &self.families) {
                    return Ok(None);
                }
                if self.settled.is_some() {
                    return Err(unknown_error());
                }
                self.last_health = Some(update.clone());
                self.pending = None;
                self.settled = Some(SettledOperation::Health(update));
                Ok(None)
            }
            PendingOperation::PreHandoffUnavailable { reason, update } => {
                if !health_state_proves(&update, &state, &self.families) {
                    return Ok(None);
                }
                if self.settled.is_some() {
                    return Err(unknown_error());
                }
                self.last_health = Some(update.clone());
                self.pending = None;
                self.settled = Some(SettledOperation::PreHandoffUnavailable { reason, update });
                Ok(None)
            }
        }
    }

    /// Transfer only the captured observer families at a verified live-file edge.
    pub(super) async fn begin(&mut self, edge: &str, deadline: Instant) -> Result<()> {
        self.ensure_ready()?;
        if self.settled.is_some() {
            return Err(unknown_error());
        }
        if self.handoff_edge.as_deref() == Some(edge) {
            return Ok(());
        }
        if self.handoff_edge.is_some() {
            return Err(Error::new(
                "source_integrity",
                "Codex observer handoff was already settled at another file edge",
            ));
        }
        if let Some(operation) = self.pending.as_ref() {
            if !pending_handoff_matches(
                operation,
                edge,
                &self.agent_id,
                self.generation,
                &self.families,
            ) {
                return Err(unknown_error());
            }
            self.settle(deadline).await?;
            return if self.handoff_edge.as_deref() == Some(edge) {
                Ok(())
            } else {
                Err(unknown_error())
            };
        }

        let reason = self
            .pre_handoff_reason
            .as_deref()
            .unwrap_or(BASELINE_REASON);
        let mut request = self.new_handoff(edge, reason)?;
        request
            .prepare(&self.policy)
            .map_err(|error| self.safe_error(error, "source handoff preparation"))?;
        request
            .validate()
            .map_err(|error| self.safe_error(error, "source handoff validation"))?;
        self.pre_handoff_reason = None;
        self.pending = Some(PendingOperation::Handoff(request.clone()));
        let value = serde_json::to_value(&request).map_err(|_| unknown_error())?;
        let value = match self
            .call_with_credential("source.handoff", value, deadline)
            .await
        {
            Ok(value) => value,
            Err(error) => return Err(error),
        };
        let ack: SourceHandoffAck = match serde_json::from_value(value) {
            Ok(ack) => ack,
            Err(_) => return Err(unknown_error()),
        };
        request.validate_ack(&ack).map_err(|_| unknown_error())?;
        self.pending = None;
        self.last_checkpoint = request.gap.and_then(|gap| gap.to);
        self.handoff_edge = self.last_checkpoint.clone();
        Ok(())
    }

    /// Commit a complete normalized native record with its exclusive cursor.
    pub(super) async fn commit(
        &mut self,
        mut events: Vec<NativeEvent>,
        cursor: String,
        deadline: Instant,
    ) -> Result<BatchAck> {
        self.ensure_ready()?;
        self.ensure_no_settled()?;
        self.prepare_events(&mut events, &cursor)?;

        if let Some(PendingOperation::Batch(batch)) = self.pending.as_ref() {
            if batch.events != events || batch.cursor.as_deref() != Some(cursor.as_str()) {
                return Err(unknown_error());
            }
            if let Some(ack) = self.settle(deadline).await? {
                return Ok(ack);
            }
            return Err(unknown_error());
        }
        if self.pending.is_some() {
            return Err(unknown_error());
        }

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
        self.pending = Some(PendingOperation::Batch(batch.clone()));
        match self.sink.commit(batch.clone(), deadline).await {
            Ok(ack) if batch_ack_matches(&batch, &ack) => {
                self.pending = None;
                self.last_checkpoint = ack.cursor.clone();
                Ok(ack)
            }
            Ok(_) => Err(unknown_error()),
            Err(error) => {
                if !self.sink.is_unknown() {
                    self.pending = None;
                }
                Err(self.safe_error(error, "native event commit"))
            }
        }
    }

    /// Commit an explicit uncovered interval; its ID remains frozen while pending.
    pub(super) async fn unavailable(
        &mut self,
        reason: &str,
        to_checkpoint: Option<String>,
        deadline: Instant,
    ) -> Result<Option<String>> {
        self.ensure_ready()?;
        let reason = self.policy.redact_text(reason);
        if reason.trim().is_empty() {
            return Err(Error::new("invalid", "Codex live gap reason is empty"));
        }
        if self.handoff_edge.is_none() {
            if to_checkpoint.is_some() {
                return Err(Error::new(
                    "not_allowed",
                    "Codex cannot advance a live checkpoint before its handoff",
                ));
            }
            if self.take_settled_pre_handoff_unavailable(&reason)? {
                return Ok(None);
            }
            if let Some(operation) = self.pending.as_ref() {
                if !matches!(operation,
                    PendingOperation::PreHandoffUnavailable { reason: pending, update }
                        if pending == &reason
                            && update.agent_id == self.agent_id
                            && update.generation == self.generation
                            && update.source == OBSERVER_SOURCE
                            && update.health == "stale")
                {
                    return Err(unknown_error());
                }
                self.settle(deadline).await?;
                return if self.take_settled_pre_handoff_unavailable(&reason)? {
                    Ok(None)
                } else {
                    Err(unknown_error())
                };
            }
            if self
                .pre_handoff_reason
                .as_ref()
                .is_some_and(|prior| prior != &reason)
            {
                return Err(unknown_error());
            }
            // There is no transcript grant before the edge-backed handoff.
            // Retain a sanitized reason and mark only the ready observe row
            // stale; the later handoff records this gap at its verified edge.
            self.pre_handoff_reason = Some(reason.clone());
            let update = SourceHealthUpdate::new(
                self.agent_id.clone(),
                self.generation,
                OBSERVER_SOURCE,
                "stale",
            );
            update
                .validate()
                .map_err(|error| self.safe_error(error, "source health validation"))?;
            if [&update.agent_id, &update.source, &update.health]
                .iter()
                .any(|value| self.policy.contains_text(value))
            {
                return Err(secret_error());
            }
            if self.last_health.as_ref() == Some(&update) {
                return Ok(None);
            }
            if self.settled.is_some() {
                return Err(unknown_error());
            }
            self.pending = Some(PendingOperation::PreHandoffUnavailable {
                reason,
                update: update.clone(),
            });
            let value = serde_json::to_value(&update).map_err(|_| unknown_error())?;
            let value = self
                .call_with_credential("source.health", value, deadline)
                .await?;
            if !health_ack_matches(&update, &value) {
                return Err(unknown_error());
            }
            self.last_health = Some(update);
            self.pending = None;
            return Ok(None);
        }
        if let Some(checkpoint) =
            self.take_settled_gap(GapPurpose::Unavailable, &reason, to_checkpoint.as_deref())?
        {
            return Ok(checkpoint);
        }
        if let Some(operation) = self.pending.as_ref() {
            if !self.pending_gap_matches(
                operation,
                GapPurpose::Unavailable,
                &reason,
                to_checkpoint.as_deref(),
            ) {
                return Err(unknown_error());
            }
            self.settle(deadline).await?;
            let checkpoint = self
                .take_settled_gap(GapPurpose::Unavailable, &reason, to_checkpoint.as_deref())?
                .ok_or_else(unknown_error)?;
            return Ok(checkpoint);
        }
        self.ensure_no_settled()?;

        let gap = self.new_gap(&reason, to_checkpoint)?;
        self.send_gap(GapPurpose::Unavailable, gap, deadline).await
    }

    /// Record the verified new file edge after a live-reader recovery.
    pub(super) async fn recover(&mut self, edge: &str, deadline: Instant) -> Result<()> {
        self.ensure_ready()?;
        if self.handoff_edge.is_none() {
            return self.begin(edge, deadline).await;
        }
        if self
            .take_settled_gap(GapPurpose::Recovery, RECOVERY_REASON, Some(edge))?
            .is_some()
        {
            return Ok(());
        }
        if let Some(operation) = self.pending.as_ref() {
            if !pending_recovery_matches(
                operation,
                edge,
                &self.agent_id,
                self.generation,
                &self.families,
            ) {
                return Err(unknown_error());
            }
            self.settle(deadline).await?;
            return if self
                .take_settled_gap(GapPurpose::Recovery, RECOVERY_REASON, Some(edge))?
                .is_some()
            {
                Ok(())
            } else {
                Err(unknown_error())
            };
        }
        self.ensure_no_settled()?;
        self.validate_edge(edge)?;
        if self.last_checkpoint.as_deref() == Some(edge) {
            return Ok(());
        }
        let gap = self.new_gap(RECOVERY_REASON, Some(edge.to_owned()))?;
        self.send_gap(GapPurpose::Recovery, gap, deadline)
            .await
            .map(|_| ())
    }

    /// Update current source health after the caller's observation/readback facts.
    pub(super) async fn health(
        &mut self,
        source: &str,
        health: &str,
        deadline: Instant,
    ) -> Result<()> {
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
        if self.take_settled_health(&update)? {
            return Ok(());
        }
        if let Some(operation) = self.pending.as_ref() {
            if !matches!(operation, PendingOperation::Health(pending) if pending == &update) {
                return Err(unknown_error());
            }
            self.settle(deadline).await?;
            if self.take_settled_health(&update)? {
                return Ok(());
            }
            return Err(unknown_error());
        }
        self.ensure_no_settled()?;
        let value = serde_json::to_value(&update).map_err(|_| unknown_error())?;
        self.pending = Some(PendingOperation::Health(update.clone()));
        let value = match self
            .call_with_credential("source.health", value, deadline)
            .await
        {
            Ok(value) => value,
            Err(error) => return Err(error),
        };
        if !health_ack_matches(&update, &value) {
            return Err(unknown_error());
        }
        self.last_health = Some(update);
        self.pending = None;
        Ok(())
    }

    async fn send_gap(
        &mut self,
        purpose: GapPurpose,
        gap: SourceGap,
        deadline: Instant,
    ) -> Result<Option<String>> {
        self.pending = Some(PendingOperation::Gap(purpose, gap.clone()));
        let value = serde_json::to_value(&gap).map_err(|_| unknown_error())?;
        let value = match self
            .call_with_credential("source.gap", value, deadline)
            .await
        {
            Ok(value) => value,
            Err(error) => return Err(error),
        };
        match serde_json::from_value::<SourceGapAck>(value) {
            Ok(ack) if ack.gap_id == gap.gap_id && ack.seq > 0 => {}
            _ => return Err(unknown_error()),
        }
        if let Some(checkpoint) = &gap.to {
            self.last_checkpoint = Some(checkpoint.clone());
        }
        self.pending = None;
        Ok(gap.to)
    }

    fn new_handoff(&self, edge: &str, reason: &str) -> Result<SourceHandoff> {
        self.validate_edge(edge)?;
        let request = SourceHandoff {
            agent_id: self.agent_id.clone(),
            generation: self.generation,
            source: OBSERVER_SOURCE.to_owned(),
            families: self.families.clone(),
            successor: SUCCESSOR.to_owned(),
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
        if self.policy.contains_text(&self.agent_id)
            || self.policy.contains_text(TRANSCRIPT_SOURCE)
            || self
                .families
                .iter()
                .any(|family| self.policy.contains_text(family))
            || to
                .as_deref()
                .is_some_and(|cursor| self.policy.contains_text(cursor))
        {
            return Err(secret_error());
        }
        if to.as_deref().is_some_and(|cursor| {
            cursor.trim().is_empty() || cursor.len() > MAX_OBSERVATION_CURSOR_BYTES
        }) {
            return Err(Error::new("invalid", "Codex live checkpoint is invalid"));
        }
        let gap_id = self.new_gap_id();
        if self.policy.contains_text(&gap_id) {
            return Err(secret_error());
        }
        let gap = SourceGap {
            gap_id,
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

    fn ensure_no_settled(&self) -> Result<()> {
        if self.settled.is_some() {
            Err(unknown_error())
        } else {
            Ok(())
        }
    }

    fn pending_gap_matches(
        &self,
        operation: &PendingOperation,
        purpose: GapPurpose,
        reason: &str,
        to: Option<&str>,
    ) -> bool {
        matches!(operation, PendingOperation::Gap(found_purpose, gap)
            if *found_purpose == purpose
                && gap.reason == reason
                && gap.to.as_deref() == to
                && gap.from.as_deref() == self.last_checkpoint.as_deref()
                && gap.agent_id == self.agent_id
                && gap.generation == self.generation
                && gap.source == TRANSCRIPT_SOURCE
                && gap.families == self.families)
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

    async fn read_state(&self, source: &str, deadline: Instant) -> Result<Value> {
        self.call_with_credential(
            "source.state",
            json!({
                "agent_id": self.agent_id,
                "generation": self.generation,
                "source": source,
            }),
            deadline,
        )
        .await
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

    fn take_settled_gap(
        &mut self,
        purpose: GapPurpose,
        reason: &str,
        to: Option<&str>,
    ) -> Result<Option<Option<String>>> {
        let Some(settled) = self.settled.take() else {
            return Ok(None);
        };
        match settled {
            SettledOperation::Gap(found_purpose, gap, ack)
                if found_purpose == purpose
                    && gap.reason == reason
                    && gap.to.as_deref() == to
                    && gap.agent_id == self.agent_id
                    && gap.generation == self.generation
                    && gap.source == TRANSCRIPT_SOURCE
                    && gap.families == self.families
                    && ack.gap_id == gap.gap_id
                    && ack.seq > 0 =>
            {
                Ok(Some(gap.to))
            }
            other => {
                self.settled = Some(other);
                Err(unknown_error())
            }
        }
    }

    fn take_settled_health(&mut self, expected: &SourceHealthUpdate) -> Result<bool> {
        let Some(settled) = self.settled.take() else {
            return Ok(false);
        };
        match settled {
            SettledOperation::Health(update) if &update == expected => Ok(true),
            other => {
                self.settled = Some(other);
                Err(unknown_error())
            }
        }
    }

    fn take_settled_pre_handoff_unavailable(&mut self, reason: &str) -> Result<bool> {
        let Some(settled) = self.settled.take() else {
            return Ok(false);
        };
        match settled {
            SettledOperation::PreHandoffUnavailable {
                reason: settled_reason,
                update,
            } if settled_reason == reason
                && update.agent_id == self.agent_id
                && update.generation == self.generation
                && update.source == OBSERVER_SOURCE
                && update.health == "stale" =>
            {
                Ok(true)
            }
            other => {
                self.settled = Some(other);
                Err(unknown_error())
            }
        }
    }
}

impl PendingOperation {
    fn source(&self) -> &str {
        match self {
            Self::Handoff(_) => OBSERVER_SOURCE,
            Self::Batch(_) | Self::Gap(_, _) => TRANSCRIPT_SOURCE,
            Self::Health(update) => &update.source,
            Self::PreHandoffUnavailable { update, .. } => &update.source,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HealthAck {
    agent_id: String,
    generation: i64,
    source: String,
    state: String,
    health: String,
}

fn health_ack_matches(update: &SourceHealthUpdate, value: &Value) -> bool {
    serde_json::from_value::<HealthAck>(value.clone()).is_ok_and(|ack| {
        ack.agent_id == update.agent_id
            && ack.generation == update.generation
            && ack.source == update.source
            && ack.state == "open"
            && ack.health == update.health
    })
}

fn health_state_proves(update: &SourceHealthUpdate, state: &Value, families: &[String]) -> bool {
    state.get("state").and_then(Value::as_str) == Some("open")
        && state.get("health").and_then(Value::as_str) == Some(update.health.as_str())
        && (update.source != TRANSCRIPT_SOURCE
            || state_owns_families(state, families, TRANSCRIPT_SOURCE, MECHANISM))
        && (update.source != OBSERVER_SOURCE
            || state.get("observer_ready").and_then(Value::as_bool) == Some(true))
}

fn gap_ack_from_state(gap: &SourceGap, state: &Value) -> Option<SourceGapAck> {
    if state.get("agent_id").and_then(Value::as_str) != Some(gap.agent_id.as_str())
        || state.get("generation").and_then(Value::as_i64) != Some(gap.generation)
        || state.get("source").and_then(Value::as_str) != Some(gap.source.as_str())
        || state.get("gap_id").and_then(Value::as_str) != Some(gap.gap_id.as_str())
        || state.get("gap_from") != Some(&json!(gap.from))
        || state.get("gap_to") != Some(&json!(gap.to))
        || !state_owns_families(state, &gap.families, TRANSCRIPT_SOURCE, MECHANISM)
    {
        return None;
    }
    let digest = canonical_json_sha256(&gap_semantic_value(gap)).ok()?;
    let digest_value = serde_json::to_value(digest).ok()?;
    if state.get("gap_digest") != Some(&digest_value) {
        return None;
    }
    let seq = state.get("gap_seq").and_then(Value::as_i64)?;
    if seq <= 0 {
        return None;
    }
    Some(SourceGapAck {
        gap_id: gap.gap_id.clone(),
        seq,
    })
}

fn gap_semantic_value(gap: &SourceGap) -> Value {
    let mut families = gap.families.clone();
    families.sort_unstable();
    json!({
        "gap_id": gap.gap_id,
        "agent_id": gap.agent_id,
        "generation": gap.generation,
        "source": gap.source,
        "families": families,
        "reason": gap.reason,
        "from": gap.from,
        "to": gap.to,
    })
}

fn state_owns_families(state: &Value, families: &[String], source: &str, mechanism: &str) -> bool {
    let Some(owners) = state.get("owners").and_then(Value::as_array) else {
        return false;
    };
    families.iter().all(|family| {
        let mut matches = owners
            .iter()
            .filter(|owner| owner.get("family").and_then(Value::as_str) == Some(family.as_str()));
        matches.next().is_some_and(|owner| {
            owner.get("source").and_then(Value::as_str) == Some(source)
                && owner.get("mechanism").and_then(Value::as_str) == Some(mechanism)
                && matches.next().is_none()
        })
    })
}

fn batch_ack_matches(batch: &SourceBatch, ack: &BatchAck) -> bool {
    ack.first_seq > 0
        && ack
            .last_seq
            .checked_sub(ack.first_seq)
            .and_then(|count| count.checked_add(1))
            == i64::try_from(batch.events.len()).ok()
        && ack.cursor == batch.cursor
}

fn family_for_kind(kind: &str) -> Option<&'static str> {
    match kind {
        "turn.started" | "turn.completed" => Some("turn"),
        "message.completed" => Some("message"),
        "tool.started" | "tool.completed" => Some("tool"),
        _ => None,
    }
}

fn handoff_matches_edge(
    request: &SourceHandoff,
    edge: &str,
    agent_id: &str,
    generation: i64,
    families: &[String],
) -> bool {
    request.agent_id == agent_id
        && request.generation == generation
        && request.source == OBSERVER_SOURCE
        && request.families == families
        && request.successor == SUCCESSOR
        && request.mechanism == MECHANISM
        && request.cursor.is_none()
        && request
            .gap
            .as_ref()
            .is_some_and(|gap| gap.to.as_deref() == Some(edge) && gap.from.is_none())
}

fn pending_handoff_matches(
    operation: &PendingOperation,
    edge: &str,
    agent_id: &str,
    generation: i64,
    families: &[String],
) -> bool {
    matches!(operation, PendingOperation::Handoff(request)
        if handoff_matches_edge(request, edge, agent_id, generation, families))
}

fn pending_recovery_matches(
    operation: &PendingOperation,
    edge: &str,
    agent_id: &str,
    generation: i64,
    families: &[String],
) -> bool {
    matches!(operation, PendingOperation::Gap(GapPurpose::Recovery, gap)
        if gap.reason == RECOVERY_REASON
            && gap.to.as_deref() == Some(edge)
            && gap.agent_id == agent_id
            && gap.generation == generation
            && gap.source == TRANSCRIPT_SOURCE
            && gap.families == families)
}

fn secret_error() -> Error {
    Error::new(
        "secret_in_payload",
        "Codex live source metadata contains a known secret",
    )
}

fn unknown_error() -> Error {
    Error::new(
        "unknown",
        "Codex live source operation is unresolved; settle it before reading more",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gap_readback_binds_prepared_reason_boundary_digest_and_current_owner() {
        let gap = SourceGap {
            gap_id: "codex-live-gap:test".to_owned(),
            agent_id: "a1".to_owned(),
            generation: 2,
            source: TRANSCRIPT_SOURCE.to_owned(),
            families: vec!["turn".to_owned(), "message".to_owned()],
            reason: "codex_rollout_unsupported".to_owned(),
            from: Some("verified-from".to_owned()),
            to: Some("verified-to".to_owned()),
        };
        let digest = canonical_json_sha256(&gap_semantic_value(&gap)).unwrap();
        let state = json!({
            "agent_id": gap.agent_id,
            "generation": gap.generation,
            "source": gap.source,
            "gap_id": gap.gap_id,
            "gap_from": gap.from,
            "gap_to": gap.to,
            "gap_digest": digest,
            "gap_seq": 17,
            "owners": [
                {"family":"turn", "source":"transcript", "mechanism":"rollout_tail"},
                {"family":"message", "source":"transcript", "mechanism":"rollout_tail"},
            ],
        });
        assert_eq!(
            gap_ack_from_state(&gap, &state),
            Some(SourceGapAck {
                gap_id: gap.gap_id.clone(),
                seq: 17,
            })
        );

        let mut changed_boundary = state.clone();
        changed_boundary["gap_to"] = json!("different-edge");
        assert!(gap_ack_from_state(&gap, &changed_boundary).is_none());

        let mut changed_owner = state;
        changed_owner["owners"][0]["mechanism"] = json!("native_read");
        assert!(gap_ack_from_state(&gap, &changed_owner).is_none());
    }

    #[test]
    fn health_readback_requires_typed_ack_and_current_source_state() {
        let update = SourceHealthUpdate::new("a1", 2, TRANSCRIPT_SOURCE, "ok");
        let ack = json!({
            "agent_id":"a1", "generation":2, "source":"transcript",
            "state":"open", "health":"ok"
        });
        assert!(health_ack_matches(&update, &ack));
        let mut malformed = ack.clone();
        malformed["generation"] = json!("2");
        assert!(!health_ack_matches(&update, &malformed));

        let state = json!({
            "state":"open", "health":"ok",
            "owners":[{"family":"turn", "source":"transcript", "mechanism":"rollout_tail"}]
        });
        assert!(health_state_proves(&update, &state, &["turn".to_owned()]));
        assert!(!health_state_proves(
            &update,
            &state,
            &["message".to_owned()]
        ));
        let mut stale = state;
        stale["state"] = json!("closed");
        assert!(!health_state_proves(&update, &stale, &["turn".to_owned()]));

        let observe_update = SourceHealthUpdate::new("a1", 2, OBSERVER_SOURCE, "stale");
        let observe_state = json!({
            "state":"open", "health":"stale", "observer_ready":true
        });
        assert!(health_state_proves(
            &observe_update,
            &observe_state,
            &["turn".to_owned()]
        ));
        let mut not_ready = observe_state;
        not_ready["observer_ready"] = json!(false);
        assert!(!health_state_proves(
            &observe_update,
            &not_ready,
            &["turn".to_owned()]
        ));
    }
}
