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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::types::NativeWorkflowItemNotification;
    use eliot_core::{Agent, AgentControl, AgentOrigin, AgentState};
    use serde_json::json;
    use std::collections::BTreeMap;

    // Sanitized wave3b identities: shapes verbatim, ids redacted. No probe
    // ULIDs, marker text, or paths appear anywhere below.
    const PARENT: &str = "parent-session-sanitized";
    const RUN: &str = "workflow-run-sanitized";
    const CHILD: &str = "child-sanitized";
    const ITEM: &str = "workflow-item-sanitized";
    const PARENT_TURN: &str = "parent-turn-sanitized";

    fn workflow_child_wire(
        attempt: i64,
        revision: i64,
        item_status: &str,
        child_status: &str,
        terminal: Option<&str>,
    ) -> NativeWorkflowItemNotification {
        let mut child = json!({
            "childId": CHILD,
            "attempt": attempt,
            "status": child_status,
        });
        if let Some(terminal) = terminal {
            child["terminal"] = json!(terminal);
        }
        serde_json::from_value(json!({
            "sessionId": PARENT,
            "sourceRange": {
                "stream": {"kind": "session", "id": PARENT},
                "first": {"id": ITEM, "sequence": revision},
                "last": {"id": format!("revision-{revision}"), "sequence": revision},
            },
            "item": {
                "itemId": ITEM,
                "kind": "workflow",
                "revision": revision,
                "status": item_status,
                "workflowRunId": RUN,
                "children": [child],
            },
        }))
        .expect("sanitized SDK workflow child revision")
    }

    fn key() -> WorkflowChildKey {
        WorkflowChildKey::new(PARENT, RUN, CHILD, 1).expect("sanitized key")
    }

    fn registered() -> WorkflowChildState {
        let mut state = WorkflowChildState::new();
        let registration = state_key_registration();
        state.binding = Some(binding_from_ack(
            registration,
            &test_ack(&state_key_registration()),
        ));
        state
    }

    fn state_key_registration() -> ChildRegistration {
        key()
            .build_registration("a1", 1, "muse-home-sanitized")
            .expect("sanitized registration")
    }

    fn test_ack(reg: &ChildRegistration) -> ChildRegistrationAck {
        let mut agent = Agent::new(
            "a2",
            "proj".to_owned(),
            "muse".to_owned(),
            "prof".to_owned(),
            1,
        );
        agent.generation = 1;
        agent.origin = AgentOrigin::Native;
        agent.observe_only = true;
        agent.parent = Some(reg.parent_id.clone());
        agent.root = reg.parent_id.clone();
        agent.depth = 1;
        agent.control = AgentControl::Slf;
        agent.state = AgentState::Unknown;
        agent.native_scope = Some(reg.native_scope.clone());
        agent.native_ref = Some(reg.native_ref.clone());
        ChildRegistrationAck {
            registration_id: reg.registration_id.clone(),
            agent,
            source_plan: BTreeMap::from([("turn".to_owned(), "stream".to_owned())]),
        }
    }

    /// Drive the pure plan for one revision, applying registration and commit
    /// outcomes the way the event processor executes them.
    fn apply(
        state: &mut WorkflowChildState,
        status: &str,
        terminal: Option<&str>,
    ) -> PlannedAction {
        match state.plan(status, terminal) {
            PlannedAction::Register => {
                let registration = state_key_registration();
                state.binding = Some(binding_from_ack(
                    registration,
                    &test_ack(&state_key_registration()),
                ));
                PlannedAction::Register
            }
            PlannedAction::Start => {
                state.started = true;
                PlannedAction::Start
            }
            PlannedAction::Complete { status, native } => {
                state.started = true;
                state.completed_status = Some(status.to_owned());
                PlannedAction::Complete { status, native }
            }
            PlannedAction::ChildGap {
                reason,
                mark_unknown,
            } => {
                if mark_unknown {
                    state.unknown = true;
                }
                PlannedAction::ChildGap {
                    reason,
                    mark_unknown,
                }
            }
            PlannedAction::Ignore => PlannedAction::Ignore,
        }
    }

    #[test]
    fn key_builders_are_stable_namespaced_and_independent_of_revision() {
        let first = key();
        let again = key();
        assert_eq!(first.native_ref(), again.native_ref());
        assert_eq!(first.registration_id(), again.registration_id());
        assert_eq!(first.turn_id(), again.turn_id());
        assert_eq!(
            first.native_ref(),
            "muse:child:parent-session-sanitized:workflow-run-sanitized:child-sanitized:1"
        );
        assert_eq!(
            first.registration_id(),
            "muse:spawn:parent-session-sanitized:workflow-run-sanitized:child-sanitized:1"
        );
        assert_eq!(
            first.turn_id(),
            "muse:turn:parent-session-sanitized:workflow-run-sanitized:child-sanitized:1"
        );
        // The Eliot turn key is namespaced: it can never be mistaken for the
        // bare parent MSP turn id, and no revision/status feeds any builder.
        assert_ne!(first.turn_id(), PARENT_TURN);
        assert!(first.turn_id().starts_with("muse:turn:"));
        assert_ne!(first.native_ref(), first.registration_id());

        let retry = WorkflowChildKey::new(PARENT, RUN, CHILD, 2).expect("attempt 2");
        assert_ne!(retry.native_ref(), first.native_ref());
        assert_ne!(retry.turn_id(), first.turn_id());
        let other_run = WorkflowChildKey::new(PARENT, "other-run", CHILD, 1).expect("other run");
        assert_ne!(other_run.native_ref(), first.native_ref());
    }

    #[test]
    fn encoding_is_collision_free_over_separator_like_parts() {
        // A naive join would collide here (`a:b`+`c` vs `a`+`b:c`); the
        // escaped encoding keeps every id distinct.
        let left = WorkflowChildKey::new("a:b", "c", "d", 1).expect("left");
        let right = WorkflowChildKey::new("a", "b:c", "d", 1).expect("right");
        assert_ne!(left.native_ref(), right.native_ref());
        assert_ne!(left.registration_id(), right.registration_id());
        assert_ne!(left.turn_id(), right.turn_id());
        assert!(left.native_ref().contains("a%3Ab"));
        // The escape itself is escaped, so a literal `%3A` can never forge
        // another part's encoding.
        let forged = WorkflowChildKey::new("a%3Ab", "c", "d", 1).expect("forged");
        assert_ne!(forged.native_ref(), left.native_ref());
        assert!(forged.native_ref().contains("a%253Ab"));
    }

    #[test]
    fn key_parts_reject_blank_and_control_identity() {
        for (parent, run, child) in [
            ("", RUN, CHILD),
            ("   ", RUN, CHILD),
            (PARENT, "", CHILD),
            (PARENT, RUN, ""),
            ("bad\nid", RUN, CHILD),
        ] {
            assert!(
                muse_child_key(parent, run, child, 1).is_err(),
                "parent={parent:?} run={run:?} child={child:?}"
            );
        }
    }

    #[test]
    fn sdk_wire_attempts_reach_registration_turn_facts_without_collision() {
        let attempts = [1_i64, 0, -1, 4_294_967_296];
        let mut tracked = std::collections::HashMap::new();
        let mut native_refs = std::collections::HashSet::new();
        let mut registration_ids = std::collections::HashSet::new();
        let mut turn_ids = std::collections::HashSet::new();

        for attempt in attempts {
            let scheduled = workflow_child_wire(attempt, 2, "inProgress", "scheduled", None);
            let child = scheduled.item.children.first().expect("wire child");
            assert_eq!(child.attempt, attempt);
            let key = muse_child_key(
                &scheduled.session_id,
                &scheduled.item.workflow_run_id,
                &child.child_id,
                child.attempt,
            )
            .expect("exact wire attempt remains keyable");
            assert_eq!(
                key.native_ref(),
                format!("muse:child:{PARENT}:{RUN}:{CHILD}:{attempt}")
            );
            let native_ref = key.native_ref();
            let registration_id = key.registration_id();
            let turn_id = key.turn_id();
            assert!(native_refs.insert(native_ref.clone()), "attempt={attempt}");
            assert!(
                registration_ids.insert(registration_id.clone()),
                "attempt={attempt}"
            );
            assert!(turn_ids.insert(turn_id.clone()), "attempt={attempt}");

            let mut state = WorkflowChildState::new();
            state.observe_rev(
                scheduled.item.revision,
                &scheduled.item.status,
                scheduled.source_range.clone(),
            );
            assert_eq!(
                state.plan(&child.status, child.terminal.as_deref()),
                PlannedAction::Register,
                "attempt={attempt}"
            );

            let registration = key
                .build_registration("a1", 7, "muse-home-sanitized")
                .expect("wire identity builds registration");
            let ack = test_ack(&registration);
            registration
                .validate_ack(&ack, "muse", &WORKFLOW_CHILD_FAMILIES)
                .expect("matching observe-only ACK");
            let binding = binding_from_ack(registration.clone(), &ack);
            assert_eq!(binding.registration.native_ref, native_ref);
            assert_eq!(binding.registration.registration_id, registration_id);
            assert_eq!(binding.native_ref, native_ref);
            state.binding = Some(binding);

            let live = workflow_child_wire(attempt, 3, "inProgress", "started", None);
            let live_child = live.item.children.first().expect("live wire child");
            state.observe_rev(
                live.item.revision,
                &live.item.status,
                live.source_range.clone(),
            );
            assert_eq!(
                state.plan(&live_child.status, live_child.terminal.as_deref()),
                PlannedAction::Start,
                "attempt={attempt}"
            );
            assert_eq!(start_event_data(&key), json!({"turn_id": turn_id}));
            assert_eq!(
                child_event_id(&key, "turn.started").expect("start identity"),
                format!("{turn_id}:turn.started")
            );
            state.started = true;

            let terminal =
                workflow_child_wire(attempt, 4, "completed", "terminal", Some("completed"));
            let terminal_child = terminal.item.children.first().expect("terminal wire child");
            state.observe_rev(
                terminal.item.revision,
                &terminal.item.status,
                terminal.source_range.clone(),
            );
            assert_eq!(
                state.plan(&terminal_child.status, terminal_child.terminal.as_deref()),
                PlannedAction::Complete {
                    status: "completed",
                    native: "completed"
                },
                "attempt={attempt}"
            );
            assert_eq!(
                complete_event_data(&key, "completed", "completed"),
                json!({"turn_id": turn_id, "status": "completed", "outcome": "success", "native_status": "completed"})
            );
            assert_eq!(
                child_event_id(&key, "turn.completed").expect("complete identity"),
                format!("{turn_id}:turn.completed")
            );
            state.completed_status = Some("completed".to_owned());
            state.completed_revision = Some(terminal.item.revision);
            assert_eq!(
                state.plan(&terminal_child.status, terminal_child.terminal.as_deref()),
                PlannedAction::Ignore,
                "duplicate terminal stays deduplicated for attempt={attempt}"
            );
            assert!(tracked.insert(key, state).is_none(), "attempt={attempt}");
        }

        assert_eq!(tracked.len(), attempts.len());
        assert_eq!(native_refs.len(), attempts.len());
        assert_eq!(registration_ids.len(), attempts.len());
        assert_eq!(turn_ids.len(), attempts.len());
    }

    #[test]
    fn child_event_id_is_bounded_and_turn_only() {
        let started = child_event_id(&key(), "turn.started").expect("started id");
        let completed = child_event_id(&key(), "turn.completed").expect("completed id");
        assert_ne!(started, completed);
        assert!(started.len() <= MAX_COMMAND_ID_BYTES);
        assert!(completed.len() <= MAX_COMMAND_ID_BYTES);
        assert!(started.starts_with("muse:turn:"));
        for kind in ["", "session.ready", "session.ended", "message.completed"] {
            assert!(child_event_id(&key(), kind).is_err(), "kind={kind}");
        }
        let long_run = "r".repeat(MAX_COMMAND_ID_BYTES);
        let long = WorkflowChildKey::new(PARENT, long_run, CHILD, 1).expect("long key builds");
        assert!(child_event_id(&long, "turn.started").is_err());
    }

    #[test]
    fn bypass_routes_workflow_revisions_past_parent_completion() {
        // The actual wire schedules the child AFTER the parent turn/completed:
        // routing takes no turn parameter, so no active parent turn is needed.
        let updated = json!({
            "sessionId": PARENT,
            "item": {"itemId": ITEM, "kind": "workflow", "revision": 2},
        });
        assert!(should_bypass_parent_buffer(
            true,
            true,
            "item/updated",
            &updated
        ));
        assert!(should_bypass_parent_buffer(
            true,
            true,
            "item/started",
            &updated
        ));
        assert!(should_bypass_parent_buffer(
            true,
            true,
            "item/completed",
            &updated
        ));
        assert!(!should_bypass_parent_buffer(
            false,
            true,
            "item/updated",
            &updated
        ));
        assert!(!should_bypass_parent_buffer(
            true,
            false,
            "item/updated",
            &updated
        ));
        let agent_message = json!({
            "sessionId": PARENT,
            "item": {"itemId": ITEM, "kind": "agentMessage", "revision": 2},
        });
        assert!(!should_bypass_parent_buffer(
            true,
            true,
            "item/completed",
            &agent_message
        ));
        assert!(!should_bypass_parent_buffer(
            true,
            true,
            "turn/completed",
            &json!({"sessionId": PARENT})
        ));
        assert!(!is_workflow_item_frame("item/delta", &updated));
    }

    #[test]
    fn live_status_matches_the_actual_wire_only() {
        for live in ["started", "usage", "completed", "terminal"] {
            assert!(is_live_child_status(live), "status={live}");
        }
        assert!(!is_live_child_status("scheduled"));
        // `inProgress` is the workflow ITEM status on the wire, never a child
        // status; unknown future states must not start a turn.
        for other in ["", "inProgress", "failed", "paused", "SCHEDULED"] {
            assert!(!is_live_child_status(other), "status={other}");
        }
    }

    #[test]
    fn actual_wire_revision_walk_commits_exactly_one_turn() {
        // Sanitized wave3b revisions 2-7 (parent turn already completed):
        // scheduled => register; started => one turn.started; usage and
        // completed-without-terminal stay quiet; explicit terminal => one
        // turn.completed; the repeated terminal revision dedups.
        // (revision, actual item status, child status, child terminal).
        let revs: [(i64, &str, &str, Option<&str>); 6] = [
            (2, "inProgress", "scheduled", None),
            (3, "inProgress", "started", None),
            (4, "inProgress", "usage", None),
            (5, "inProgress", "completed", None),
            (6, "inProgress", "terminal", Some("completed")),
            (7, "completed", "terminal", Some("completed")),
        ];
        let mut state = WorkflowChildState::new();
        let mut trace = Vec::new();
        for (revision, item_status, status, terminal) in revs {
            state.observe_rev(revision, item_status, None);
            trace.push(apply(&mut state, status, terminal));
        }
        assert_eq!(
            trace,
            [
                PlannedAction::Register,
                PlannedAction::Start,
                PlannedAction::Ignore,
                PlannedAction::Ignore,
                PlannedAction::Complete {
                    status: "completed",
                    native: "completed"
                },
                PlannedAction::Ignore,
            ]
        );
        assert!(state.started);
        assert_eq!(state.completed_status.as_deref(), Some("completed"));
        assert_eq!(state.last_revision, 7);
        assert_eq!(state.item_status, "completed");
        // The Eliot turn binding survived the walk: it is the synthesized
        // K-derived turn_id() — never the parent wire turn id.
        assert_ne!(key().turn_id(), PARENT_TURN);
    }

    #[test]
    fn completed_without_terminal_never_completes() {
        let mut state = registered();
        state.started = true;
        assert_eq!(state.plan("completed", None), PlannedAction::Ignore);
        assert_eq!(state.completed_status, None);
        // ... even when the start was never observed: liveness starts, the
        // terminal alone completes.
        let unseen = registered();
        assert_eq!(unseen.plan("completed", None), PlannedAction::Start);
        assert_eq!(unseen.completed_status, None);
    }

    #[test]
    fn explicit_terminal_completes_with_the_existing_muse_mapping() {
        for (terminal, status, native) in [
            ("completed", "completed", "completed"),
            ("failed", "failed", "failed"),
            ("cancelled", "interrupted", "cancelled"),
        ] {
            let mut state = registered();
            state.started = true;
            assert_eq!(
                state.plan("terminal", Some(terminal)),
                PlannedAction::Complete { status, native },
                "terminal={terminal}"
            );
            // The raw wire terminal is preserved as `native_status` next to
            // the normalized status.
            let outcome = match status {
                "completed" => "success",
                "failed" => "failure",
                _ => "interrupted",
            };
            assert_eq!(
                complete_event_data(&key(), status, native),
                json!({"turn_id": key().turn_id(), "status": status, "outcome": outcome, "native_status": native}),
                "terminal={terminal}"
            );
        }
        // The terminal completes even when no live state was ever seen: the
        // executor starts first, so the ONE turn still brackets the terminal.
        let unseen = registered();
        assert_eq!(
            unseen.plan("terminal", Some("completed")),
            PlannedAction::Complete {
                status: "completed",
                native: "completed"
            }
        );
        // An unmapped terminal gaps the child and stops expectations.
        let unknown = registered();
        assert_eq!(
            unknown.plan("terminal", Some("expired")),
            PlannedAction::ChildGap {
                reason: "Muse workflow child terminal is unknown",
                mark_unknown: true,
            }
        );
    }

    #[test]
    fn repeated_terminal_dedups_and_changed_terminal_gaps_without_regressing() {
        let mut state = registered();
        state.started = true;
        state.completed_status = Some("completed".to_owned());
        state.completed_revision = Some(6);
        assert_eq!(
            state.plan("terminal", Some("completed")),
            PlannedAction::Ignore
        );
        assert_eq!(state.plan("terminal", None), PlannedAction::Ignore);
        // Unmapped noise after completion is ignored, never regressed.
        assert_eq!(state.plan("terminal", Some("bogus")), PlannedAction::Ignore);
        assert_eq!(
            state.plan("terminal", Some("failed")),
            PlannedAction::ChildGap {
                reason: "Muse workflow child terminal changed after completion",
                mark_unknown: false,
            }
        );
        assert_eq!(state.completed_status.as_deref(), Some("completed"));
    }

    #[test]
    fn scheduled_after_register_and_unknown_stay_silent() {
        let mut state = registered();
        assert_eq!(state.plan("scheduled", None), PlannedAction::Ignore);
        assert!(!state.started);
        state.unknown = true;
        assert_eq!(state.plan("started", None), PlannedAction::Ignore);
        assert_eq!(
            state.plan("terminal", Some("completed")),
            PlannedAction::Ignore
        );
    }

    #[test]
    fn turn_data_carry_no_settlement_totals_or_timestamps() {
        let started = start_event_data(&key());
        assert_eq!(started, json!({ "turn_id": key().turn_id() }));
        let completed = complete_event_data(&key(), "completed", "completed");
        assert_eq!(
            completed,
            json!({ "turn_id": key().turn_id(), "status": "completed", "outcome": "success", "native_status": "completed" })
        );
        for data in [&started, &completed] {
            let fields = data.as_object().expect("event data is an object");
            for forbidden in [
                "command_id",
                "cause",
                "request_id",
                "final_text",
                "final_text_unavailable",
                "usage",
                "duration_ms",
                "durationMs",
                "started_at",
                "completed_at",
                "native_error",
            ] {
                assert!(!fields.contains_key(forbidden), "field={forbidden}");
            }
        }
    }

    #[test]
    fn revision_walk_yields_turn_facts_only() {
        // Structural pin: `resultRef`/`message` composites never reach the
        // planner (unmodelled), so the walk below — terminal WITH the
        // composite payloads on the wire — can only yield turn facts.
        let revs: [(&str, Option<&str>); 6] = [
            ("scheduled", None),
            ("started", None),
            ("usage", None),
            ("completed", None),
            ("terminal", Some("completed")),
            ("terminal", Some("completed")),
        ];
        let mut state = WorkflowChildState::new();
        let mut kinds = Vec::new();
        for (status, terminal) in revs {
            match apply(&mut state, status, terminal) {
                PlannedAction::Start => kinds.push("turn.started"),
                PlannedAction::Complete { .. } => kinds.push("turn.completed"),
                PlannedAction::Register
                | PlannedAction::Ignore
                | PlannedAction::ChildGap { .. } => {}
            }
        }
        assert_eq!(kinds, ["turn.started", "turn.completed"]);
    }

    #[test]
    fn registration_and_ack_match_the_turn_only_grant() {
        let reg = state_key_registration();
        assert_eq!(reg.parent_id, "a1");
        assert_eq!(reg.parent_generation, 1);
        assert_eq!(reg.parent_native_ref, PARENT);
        assert_eq!(reg.native_scope, "muse-home-sanitized");
        assert_eq!(reg.native_ref, key().native_ref());
        assert_eq!(reg.registration_id, key().registration_id());
        reg.validate().expect("sanitized registration validates");

        reg.validate_ack(&test_ack(&reg), "muse", &WORKFLOW_CHILD_FAMILIES)
            .expect("exact turn-only plan");
        // Session/message coverage is outside the Muse grant.
        let mut widened = test_ack(&reg);
        widened
            .source_plan
            .insert("session".to_owned(), "stream".to_owned());
        assert!(
            reg.validate_ack(&widened, "muse", &WORKFLOW_CHILD_FAMILIES)
                .is_err()
        );
        let mut wrong_harness = test_ack(&reg);
        wrong_harness.agent.harness = "codex".to_owned();
        assert!(
            reg.validate_ack(&wrong_harness, "muse", &WORKFLOW_CHILD_FAMILIES)
                .is_err()
        );
        let mut wrong_ref = test_ack(&reg);
        wrong_ref.agent.native_ref = Some("other".to_owned());
        assert!(
            reg.validate_ack(&wrong_ref, "muse", &WORKFLOW_CHILD_FAMILIES)
                .is_err()
        );

        let binding = binding_from_ack(reg.clone(), &test_ack(&reg));
        assert_eq!(binding.agent_id, "a2");
        assert_eq!(binding.agent_generation, 1);
        assert_eq!(binding.native_ref, key().native_ref());
        assert_eq!(binding.registration, reg);
    }

    #[test]
    fn child_params_envelope_carries_credential_and_registration_only() {
        let reg = state_key_registration();
        let params = serde_json::to_value(ChildParams {
            credential: "producer-cred".to_owned(),
            registration: reg.clone(),
        })
        .expect("child params encode");
        let envelope = params.as_object().expect("params object");
        assert_eq!(envelope.len(), 2);
        assert!(envelope.contains_key("credential"));
        assert!(envelope.contains_key("registration"));
        assert_eq!(params["credential"], "producer-cred");
        let decoded: ChildRegistration =
            serde_json::from_value(params["registration"].clone()).expect("decode");
        assert_eq!(decoded, reg);
    }

    #[test]
    fn only_deterministic_rejections_end_tracking() {
        for code in [
            "invalid",
            "unsupported",
            "stale_generation",
            "not_allowed",
            "id_conflict",
        ] {
            assert!(is_admission_rejection(&Error::new(code, "x")), "{code}");
        }
        for code in ["unknown", "busy", "source_integrity", "transport_closed"] {
            assert!(!is_admission_rejection(&Error::new(code, "x")), "{code}");
        }
    }
}
