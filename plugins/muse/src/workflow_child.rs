//! Muse workflow-child observer (producer half): ONE connected observer over the
//! actual `workflow` item wire.
//!
//! Source: the wave3b recorded wire
//! (`muse-native-child-probe-wave3b-20261105/raw_frames.jsonl`, Muse Code 1.4.0,
//! bundle fingerprint `sha256:36466f63…0d3d7f`) plus the installed
//! `WorkflowChild` schema in the probe receipt. One native `host.agent` child
//! is one Eliot Agent per canonical key `K(parentSessionId, workflowRunId,
//! childId, attempt)` ([`WorkflowChildKey`], owned by this module — the shared
//! core carries only the harness-generic registration RPC, never
//! vendor-specific key encodings); the wire carries no session or turn id on
//! the child itself, so the ONE normalized Eliot turn key is synthesized and
//! is explicitly NOT an MSP turn id.
//!
//! Turn-only coverage ([`WORKFLOW_CHILD_FAMILIES`]): `scheduled`
//! registers the observe-only tokenless child (Agent `Unknown`); the first
//! live actual-wire state commits exactly one `turn.started`; `completed`
//! WITHOUT an explicit `terminal` never completes; an explicit terminal
//! commits exactly one `turn.completed` with the existing Muse status mapping;
//! a repeated terminal revision dedups. Never `session.ready`/`session.ended`
//! or `message.completed`: there is no child session to fake, and `resultRef`
//! / `message` history composites are unsupported — no fact is ever
//! synthesized from them.

use std::time::Duration;

use eliot_core::{ChildRegistration, ChildRegistrationAck, Error, Result};
use eliot_plugin::{
    CommitSink, PeerClient,
    protocol::{ChildParams, MAX_COMMAND_ID_BYTES},
    session::StreamSource,
};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::events::{native_terminal, turn_status};
use crate::native::types::NativeTurnTerminal;

/// Bound on DETAILED in-memory tracking per generation. An over-limit
/// proven child is still persisted through one idempotent registration plus
/// an observation-unavailable child gap, then marked unknown — a proven
/// tracked child is never evicted to make room, and a proven newcomer is
/// never merely parent-gapped.
pub(crate) const MAX_WORKFLOW_CHILDREN: usize = 256;

/// Turn-only observation grant the Muse child expects: the registration ACK
/// must plan exactly this source plan, and every child fact/gap publishes
/// under it. Must match the Store's `muse` grant (shared boundary); a drift
/// fails closed at ACK validation.
pub(crate) const WORKFLOW_CHILD_FAMILIES: [&str; 1] = ["turn"];

/// Bounded registration attempt: a lost ACK retains the identical
/// registration for the next revision; only deterministic rejections end it.
const CHILD_DEADLINE: Duration = Duration::from_secs(30);

/// Whether this notification frame is a workflow-item revision carrying child
/// lifecycle facts.
pub(crate) fn is_workflow_item_frame(method: &str, params: &Value) -> bool {
    matches!(method, "item/started" | "item/updated" | "item/completed")
        && params
            .get("item")
            .and_then(|item| item.get("kind"))
            .and_then(Value::as_str)
            == Some("workflow")
}

/// Whether a workflow-item frame routes immediately, ahead of
/// active-parent-turn buffering. Workflow children outlive the parent turn
/// (the actual wire schedules the child AFTER the parent `turn/completed`),
/// so routing is independent of any active parent turn by construction: there
/// is deliberately no turn parameter here.
pub(crate) fn should_bypass_parent_buffer(
    ready: bool,
    session_known: bool,
    method: &str,
    params: &Value,
) -> bool {
    ready && session_known && is_workflow_item_frame(method, params)
}

/// Actual-wire live states: the child demonstrably ran, so the ONE turn may
/// start. `scheduled` is registration-only; anything unrecognized is ignored
/// until a known-live state or an explicit terminal proves liveness.
pub(crate) fn is_live_child_status(status: &str) -> bool {
    matches!(status, "started" | "usage" | "completed" | "terminal")
}

/// Canonical observation key `K(parentSessionId, workflowRunId, childId,
/// attempt)` for one workflow child, owned by the Muse plugin.
///
/// Identity depends ONLY on these four parts — never on revision, status,
/// terminal, or any other per-revision value — so the key and every id built
/// from it are stable across workflow revisions.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct WorkflowChildKey {
    parent_session: String,
    workflow_run: String,
    child_id: String,
    attempt: i64,
}

impl WorkflowChildKey {
    /// Build K. Parts must be non-blank with no control characters;
    /// `attempt` is retained as the exact signed integer from the SDK wire.
    pub(crate) fn new(
        parent_session: impl Into<String>,
        workflow_run: impl Into<String>,
        child_id: impl Into<String>,
        attempt: i64,
    ) -> Result<Self> {
        let key = Self {
            parent_session: parent_session.into(),
            workflow_run: workflow_run.into(),
            child_id: child_id.into(),
            attempt,
        };
        key.validate()?;
        Ok(key)
    }

    fn validate(&self) -> Result<()> {
        for part in [&self.parent_session, &self.workflow_run, &self.child_id] {
            if part.trim().is_empty() || part.chars().any(char::is_control) {
                return Err(Error::new(
                    "native_protocol",
                    "Muse workflow child omitted its exact child identity",
                ));
            }
        }
        Ok(())
    }

    /// Escape the join separator and the escape itself so the joined encoding
    /// is injective (collision-free) over all valid parts.
    fn encode_part(part: &str) -> String {
        let mut encoded = String::with_capacity(part.len());
        for ch in part.chars() {
            match ch {
                '%' => encoded.push_str("%25"),
                ':' => encoded.push_str("%3A"),
                _ => encoded.push(ch),
            }
        }
        encoded
    }

    fn encoded(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            Self::encode_part(&self.parent_session),
            Self::encode_part(&self.workflow_run),
            Self::encode_part(&self.child_id),
            self.attempt
        )
    }

    /// Opaque native subject reference: namespaced, stable, revision-free.
    pub(crate) fn native_ref(&self) -> String {
        format!("muse:child:{}", self.encoded())
    }

    /// Canonical registration id: distinct namespace, same stability.
    pub(crate) fn registration_id(&self) -> String {
        format!("muse:spawn:{}", self.encoded())
    }

    /// The ONE normalized Eliot turn key for this child. Explicitly NOT an
    /// MSP turn id: the wire carries no turn id on a workflow child.
    pub(crate) fn turn_id(&self) -> String {
        format!("muse:turn:{}", self.encoded())
    }

    /// Build the shared registration: this Agent is the immediate parent and
    /// the child inherits our native scope.
    pub(crate) fn build_registration(
        &self,
        parent_id: &str,
        parent_generation: i64,
        native_scope: &str,
    ) -> Result<ChildRegistration> {
        let registration = ChildRegistration {
            registration_id: self.registration_id(),
            parent_id: parent_id.to_owned(),
            parent_generation,
            parent_native_ref: self.parent_session.clone(),
            native_scope: native_scope.to_owned(),
            native_ref: self.native_ref(),
        };
        registration.validate()?;
        Ok(registration)
    }
}

/// Build the canonical observation key K from one wire child entry.
pub(crate) fn muse_child_key(
    parent_session: &str,
    workflow_run_id: &str,
    child_id: &str,
    attempt: i64,
) -> Result<WorkflowChildKey> {
    WorkflowChildKey::new(parent_session, workflow_run_id, child_id, attempt)
}

/// Native fact identity within the child source: the canonical turn key plus
/// the turn fact kind. Turn facts only — anything else is rejected here.
pub(crate) fn child_event_id(key: &WorkflowChildKey, kind: &str) -> Result<String> {
    if !matches!(kind, "turn.started" | "turn.completed") {
        return Err(Error::new(
            "invalid",
            "Muse workflow child publishes turn facts only",
        ));
    }
    let native_id = format!("{}:{kind}", key.turn_id());
    if native_id.len() > MAX_COMMAND_ID_BYTES {
        return Err(Error::new(
            "source_integrity",
            "Muse workflow child event identity exceeded its bound",
        ));
    }
    Ok(native_id)
}

/// Observed `turn.started` data: the ONE normalized turn key only. Observed
/// facts must not carry command/request settlement (the Store rejects them).
pub(crate) fn start_event_data(key: &WorkflowChildKey) -> Value {
    json!({ "turn_id": key.turn_id() })
}

/// Observed `turn.completed` data: turn key plus mapped status, canonical
/// outcome and the raw wire terminal. Final text is unknown (`resultRef` is
/// never dereferenced), usage is never totaled, and no timestamp is invented
/// from `durationMs`. The raw wire terminal rides as `native_status` next to
/// the normalized `status`: wire `cancelled` normalizes to `interrupted`
/// while its raw `cancelled` is preserved for readback.
pub(crate) fn complete_event_data(key: &WorkflowChildKey, status: &str, native: &str) -> Value {
    let outcome = match status {
        "completed" => "success",
        "failed" => "failure",
        "interrupted" => "interrupted",
        _ => "unknown",
    };
    json!({
        "turn_id": key.turn_id(),
        "status": status,
        "outcome": outcome,
        "native_status": native,
    })
}

/// The single-parent binding the producer needs before publishing child facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChildBinding {
    pub registration: ChildRegistration,
    pub agent_id: String,
    pub agent_generation: i64,
    pub native_ref: String,
}

/// Register one workflow child through the host. Success commits nothing by
/// itself; the caller publishes child facts only after this ACK validates.
///
/// The host authenticates the producer from the private connection. No child
/// credential is sent and no child control verb is exposed.
pub(crate) async fn register_child(
    host: &PeerClient,
    credential: &str,
    registration: ChildRegistration,
) -> Result<ChildRegistrationAck> {
    registration.validate()?;
    let params = serde_json::to_value(ChildParams {
        credential: credential.to_owned(),
        registration: registration.clone(),
    })
    .map_err(|_| Error::new("internal", "Muse child params could not be encoded"))?;
    let value = host
        .call("child", params, Instant::now() + CHILD_DEADLINE)
        .await?;
    let ack: ChildRegistrationAck = serde_json::from_value(value)
        .map_err(|_| Error::new("source_integrity", "child ACK was not well formed"))?;
    registration.validate_ack(&ack, "muse", &WORKFLOW_CHILD_FAMILIES)?;
    Ok(ack)
}

/// Whether an RPC error proves the registration was never admitted. Only
/// deterministic rejections end tracking; anything else retains the identical
/// registration for the next revision.
pub(crate) fn is_admission_rejection(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "invalid" | "unsupported" | "stale_generation" | "not_allowed" | "id_conflict"
    )
}

/// Build the child binding from a validated ACK.
pub(crate) fn binding_from_ack(
    registration: ChildRegistration,
    ack: &ChildRegistrationAck,
) -> ChildBinding {
    ChildBinding {
        registration,
        agent_id: ack.agent.id.clone(),
        agent_generation: ack.agent.generation,
        native_ref: ack.agent.native_ref.clone().unwrap_or_default(),
    }
}

/// Pure plan for one child revision against its tracked state. The caller
/// executes registration/commits; NOTHING here touches the network or Store,
/// so the actual-wire revision walk is unit-testable without a host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlannedAction {
    /// No fact: dedup, scheduled-while-registered, uninterpretable status, or
    /// an unknown child covered by its gap.
    Ignore,
    /// No binding yet: register (or retry the retained registration), then
    /// re-plan the same revision.
    Register,
    /// Commit the ONE `turn.started`.
    Start,
    /// Commit the ONE `turn.completed`, starting first when the live states
    /// were never observed. `status` is the normalized turn status;
    /// `native` is the raw wire terminal preserved as `native_status`.
    Complete {
        status: &'static str,
        native: &'static str,
    },
    /// Record a child-scoped gap; optionally stop expecting more revisions.
    ChildGap {
        reason: &'static str,
        mark_unknown: bool,
    },
}

/// Per-K tracked state: the turn-only observation progress for one identity.
/// K itself lives in the caller map key (the parent-session, run, child,
/// and attempt parts are consumed there to build native_ref,
/// registration_id, and turn_id), not in a copied field. K's parent
/// session component is the ONE normalized Eliot turn binding: turn_id()
/// is synthesized from K and is explicitly NOT the parent MSP turn id, which
/// rides on the workflow item only as routing context and is never stored.
/// Item id is likewise item-level provenance never reused by the child plan.
/// Last revision, status, and source range track the latest actual wire
/// values; identity never depends on them.
#[derive(Clone, Debug)]
pub(crate) struct WorkflowChildState {
    pub last_revision: i64,
    /// Latest actual item status verbatim (`inProgress`, `completed`, ...):
    /// provenance only, never mapped — the child terminal drives completion.
    pub item_status: String,
    pub source_range: Option<Value>,
    pub binding: Option<ChildBinding>,
    pub pending_registration: Option<ChildRegistration>,
    pub started: bool,
    pub completed_status: Option<String>,
    pub completed_revision: Option<i64>,
    pub unknown: bool,
    pub writes_blocked: bool,
    pub gap_reported: bool,
    pub gap_number: u64,
    pub source_unknown: bool,
    pub batch_unknown: bool,
    pub failure: Option<String>,
}

impl WorkflowChildState {
    pub(crate) fn new() -> Self {
        Self {
            last_revision: 0,
            item_status: String::new(),
            source_range: None,
            binding: None,
            pending_registration: None,
            started: false,
            completed_status: None,
            completed_revision: None,
            unknown: false,
            writes_blocked: false,
            gap_reported: false,
            gap_number: 0,
            source_unknown: false,
            batch_unknown: false,
            failure: None,
        }
    }

    pub(crate) fn observe_rev(
        &mut self,
        revision: i64,
        item_status: &str,
        source_range: Option<Value>,
    ) {
        self.last_revision = self.last_revision.max(revision);
        self.item_status = item_status.to_owned();
        if source_range.is_some() {
            self.source_range = source_range;
        }
    }

    pub(crate) fn plan(&self, status: &str, terminal: Option<&str>) -> PlannedAction {
        if self.unknown {
            return PlannedAction::Ignore;
        }
        if let Some(done) = self.completed_status.as_deref() {
            // A repeated terminal revision dedups; only a MAPPED DIFFERENT
            // terminal contradicts the kept completion. Unmapped noise after
            // completion is ignored, never regressed.
            if let Some(term) = terminal {
                let mapped = turn_status(&NativeTurnTerminal::parse_workflow_terminal(term));
                if matches!(mapped, Some(text) if text != done) {
                    return PlannedAction::ChildGap {
                        reason: "Muse workflow child terminal changed after completion",
                        mark_unknown: false,
                    };
                }
            }
            return PlannedAction::Ignore;
        }
        if self.binding.is_none() {
            return PlannedAction::Register;
        }
        if let Some(term) = terminal {
            let parsed = NativeTurnTerminal::parse_workflow_terminal(term);
            match (turn_status(&parsed), native_terminal(&parsed)) {
                (Some(status_text), Some(native_text)) => {
                    return PlannedAction::Complete {
                        status: status_text,
                        native: native_text,
                    };
                }
                _ => {
                    return PlannedAction::ChildGap {
                        reason: "Muse workflow child terminal is unknown",
                        mark_unknown: true,
                    };
                }
            }
        }
        if status == "scheduled" || !is_live_child_status(status) {
            return PlannedAction::Ignore;
        }
        if self.started {
            PlannedAction::Ignore
        } else {
            PlannedAction::Start
        }
    }

    pub(crate) async fn commit_event(
        &mut self,
        sink: &mut CommitSink,
        host: &PeerClient,
        credential: &str,
        kind: &str,
        native_id: String,
        data: Value,
    ) -> Result<()> {
        let Some(binding) = self.binding.as_ref() else {
            return Err(Error::new(
                "bad_state",
                "Muse workflow child published before its registration",
            ));
        };
        let mut source = StreamSource {
            sink,
            host,
            credential,
            agent_id: &binding.agent_id,
            generation: binding.agent_generation,
            harness: "Muse",
            id_prefix: "muse",
            families: &WORKFLOW_CHILD_FAMILIES,
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.source_unknown,
            batch_unknown: &mut self.batch_unknown,
            failure: &mut self.failure,
        };
        source.commit_event(kind, native_id, data).await
    }

    pub(crate) async fn record_gap(
        &mut self,
        sink: &mut CommitSink,
        host: &PeerClient,
        credential: &str,
        reason: &str,
    ) -> Result<()> {
        let Some(binding) = self.binding.as_ref() else {
            // Never registered: nothing was promised under a child identity,
            // so there is no child source to gap — the caller unknowns the
            // key and the parent gap (if any) covers the loss.
            self.source_unknown = true;
            return Ok(());
        };
        let mut source = StreamSource {
            sink,
            host,
            credential,
            agent_id: &binding.agent_id,
            generation: binding.agent_generation,
            harness: "Muse",
            id_prefix: "muse",
            families: &WORKFLOW_CHILD_FAMILIES,
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.source_unknown,
            batch_unknown: &mut self.batch_unknown,
            failure: &mut self.failure,
        };
        source.record_gap(reason).await
    }
}
