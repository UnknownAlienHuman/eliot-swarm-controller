use crate::module_catalog::{ArtifactIdentity, ModuleId, ProtocolVersion};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const MODULE_EVENT_METADATA_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleEventStatus {
    Applied,
    Completed,
    Failed,
    Incomplete,
    Cancelled,
    Rejected,
    Sent,
    Answered,
    Invalidated,
    Unknown,
}

/// Closed metadata-only projection supplied by an authenticated Module event
/// producer. It deliberately has no payload, credential, Task, or Operation
/// fields; Store binds those identities from the authenticated connection and
/// the immutable observation row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleEventMetadata {
    pub schema_id: String,
    pub schema_version: u16,
    pub event_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ModuleEventStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurrence_phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub occurrence_id: Option<String>,
}

impl ModuleEventMetadata {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_id != crate::module_contract::MODULE_EVENT_METADATA_SCHEMA_ID
            || self.schema_version != MODULE_EVENT_METADATA_SCHEMA_VERSION
            || !valid_bounded_event_atom(&self.event_kind, 256)
            || self.occurrence_phase.is_some() != self.occurrence_id.is_some()
            || self
                .occurrence_phase
                .as_deref()
                .is_some_and(|value| !valid_bounded_event_atom(value, 128))
            || self
                .occurrence_id
                .as_deref()
                .is_some_and(|value| !valid_bounded_event_atom(value, 256))
        {
            return Err("module event metadata is invalid");
        }
        Ok(())
    }
}

fn valid_bounded_event_atom(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-/@".contains(&byte))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeCommand {
    pub operation_id: String,
    pub method: String,
    pub created_at_ms: i64,
    pub binding_id: String,
    pub generation: i64,
    pub native_root_id: Option<String>,
    pub route: Value,
    pub input: Value,
    /// SHA-256 of the canonical, immutable stored request before host-side
    /// enrichment of `input`. Legacy routes may omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_sha256: Option<String>,
    /// For readback operations, the canonical digest of the exact target
    /// Operation's original request. This is distinct from the current
    /// command's own digest and is supplied by the Store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_input_sha256: Option<String>,
}

/// Generic immutable receipt identity for an independently built module.
/// The Store validates every field against the authenticated binding,
/// retained descriptor selector, and original Operation request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleReceiptIdentity {
    pub schema_version: u16,
    pub module_id: ModuleId,
    pub artifact: ArtifactIdentity,
    pub protocol: ProtocolVersion,
    pub binding_id: String,
    pub binding_generation: i64,
    pub operation_id: String,
    pub input_sha256: String,
}

impl ModuleReceiptIdentity {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1
            || self.protocol.major != 1
            || self.protocol.minor != 0
            || self.binding_id.trim().is_empty()
            || self.binding_generation <= 0
            || self.operation_id.trim().is_empty()
            || self.input_sha256.len() != 64
            || !self
                .input_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("module receipt identity is outside protocol 1.0");
        }
        Ok(())
    }
}

/// Store-computed immutable Task and source-text identity supplied with a
/// descriptor-backed `task.dispatch`. The adapter echoes this exact context
/// in its admission receipt; the Store recomputes it from the original
/// Operation and retained Attempt before recording a producer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskDispatchContext {
    pub schema_version: u16,
    pub operation_id: String,
    pub binding_id: String,
    pub binding_generation: i64,
    pub worker_boot_id: String,
    pub attempt_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub task_snapshot_sha256: String,
    pub source_text_sha256: String,
    pub source_text_bytes: u64,
}

impl TaskDispatchContext {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1
            || self.operation_id.trim().is_empty()
            || self.binding_id.trim().is_empty()
            || self.binding_generation <= 0
            || self.worker_boot_id.trim().is_empty()
            || self.attempt_id.trim().is_empty()
            || self.task_id.trim().is_empty()
            || self.task_revision <= 0
            || !is_lower_sha256(&self.task_snapshot_sha256)
            || !is_lower_sha256(&self.source_text_sha256)
        {
            return Err("task dispatch context is invalid");
        }
        Ok(())
    }
}

/// Normalized evidence that a module admitted the exact dispatch payload.
/// This records input admission only; it is never Task completion evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskDispatchAdmissionReceipt {
    pub schema_version: u16,
    pub module_receipt: ModuleReceiptIdentity,
    pub operation_id: String,
    pub binding_id: String,
    pub binding_generation: i64,
    pub worker_boot_id: String,
    pub attempt_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub task_snapshot_sha256: String,
    pub source_text_sha256: String,
    pub source_text_bytes: u64,
    /// Digest and byte length of the exact normalized payload submitted to
    /// the native interface. The Store can bind this claim to source/context,
    /// but native-specific payload bytes remain owned by the adapter.
    pub native_payload_sha256: String,
    pub native_payload_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_input_id: Option<String>,
}

impl TaskDispatchAdmissionReceipt {
    pub fn validate(&self) -> Result<(), &'static str> {
        let context = self.context();
        if self.schema_version != 1
            || context.validate().is_err()
            || self.module_receipt.validate().is_err()
            || !is_lower_sha256(&self.native_payload_sha256)
            || self.native_payload_bytes == 0
            || self
                .native_input_id
                .as_ref()
                .is_some_and(|id| id.trim().is_empty())
        {
            return Err("task dispatch admission receipt is invalid");
        }
        Ok(())
    }

    pub fn context(&self) -> TaskDispatchContext {
        TaskDispatchContext {
            schema_version: self.schema_version,
            operation_id: self.operation_id.clone(),
            binding_id: self.binding_id.clone(),
            binding_generation: self.binding_generation,
            worker_boot_id: self.worker_boot_id.clone(),
            attempt_id: self.attempt_id.clone(),
            task_id: self.task_id.clone(),
            task_revision: self.task_revision,
            task_snapshot_sha256: self.task_snapshot_sha256.clone(),
            source_text_sha256: self.source_text_sha256.clone(),
            source_text_bytes: self.source_text_bytes,
        }
    }
}

/// Immutable Attempt and dispatch origin sealed by Store when it admits an
/// opted-in `agent.result` request. This is provenance only: it does not
/// assert native execution or Task completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedResultOriginContext {
    pub schema_version: u16,
    pub binding_id: String,
    pub binding_generation: i64,
    pub task_id: String,
    pub task_revision: i64,
    pub task_snapshot_sha256: String,
    pub attempt_id: String,
    pub target_operation_id: String,
    pub target_input_sha256: String,
    pub selector_sha256: String,
    pub producer: NormalizedResultProducerOrigin,
}

/// Exact durable normalized `task.dispatch` producer retained on the Attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedResultProducerOrigin {
    pub assignment_id: String,
    pub dispatch_operation_id: String,
    pub attempt_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub task_snapshot_sha256: String,
    pub source_text_sha256: String,
    pub source_text_bytes: u64,
    pub native_payload_sha256: String,
    pub native_payload_bytes: u64,
    pub completion_condition: String,
    pub execution_complete: bool,
    pub task_completion: String,
    pub disposition: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_input_id: Option<String>,
    pub module_receipt: ModuleReceiptIdentity,
}

impl NormalizedResultOriginContext {
    pub fn validate(&self) -> Result<(), &'static str> {
        let producer = &self.producer;
        if self.schema_version != 1
            || self.binding_id.trim().is_empty()
            || self.binding_generation <= 0
            || self.task_id.trim().is_empty()
            || self.task_revision <= 0
            || self.attempt_id.trim().is_empty()
            || self.target_operation_id.trim().is_empty()
            || !is_lower_sha256(&self.task_snapshot_sha256)
            || !is_lower_sha256(&self.target_input_sha256)
            || !is_lower_sha256(&self.selector_sha256)
            || producer.assignment_id != self.target_operation_id
            || producer.dispatch_operation_id != self.target_operation_id
            || producer.attempt_id != self.attempt_id
            || producer.task_id != self.task_id
            || producer.task_revision != self.task_revision
            || producer.task_snapshot_sha256 != self.task_snapshot_sha256
            || !is_lower_sha256(&producer.source_text_sha256)
            || producer.source_text_bytes > i64::MAX as u64
            || !is_lower_sha256(&producer.native_payload_sha256)
            || producer.native_payload_bytes > i64::MAX as u64
            || producer.task_completion != "unknown"
            || !matches!(
                (
                    producer.completion_condition.as_str(),
                    producer.execution_complete,
                    producer.disposition.as_str()
                ),
                ("native_input_admitted", false, "admitted")
                    | ("native_turn_completed", true, "completed")
            )
            || producer.module_receipt.validate().is_err()
            || producer.module_receipt.operation_id != self.target_operation_id
            || producer.module_receipt.binding_id != self.binding_id
            || producer.module_receipt.binding_generation != self.binding_generation
            || producer.module_receipt.input_sha256 != self.target_input_sha256
            || producer
                .native_input_id
                .as_ref()
                .is_some_and(|id| id.trim().is_empty())
        {
            return Err("normalized result origin is invalid");
        }
        Ok(())
    }
}

/// Typed, bounded output page from any descriptor-admitted executor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NormalizedResultPageSource {
    pub schema_id: String,
    pub schema_version: u16,
    pub origin: NormalizedResultOriginContext,
    pub result_operation_id: String,
    pub result_input_sha256: String,
    pub result_module_receipt: ModuleReceiptIdentity,
    pub payload_sha256: String,
    pub payload_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_response_identity: Option<String>,
    pub execution_complete: bool,
    pub task_completion: String,
    pub native_replay: bool,
}

impl NormalizedResultPageSource {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_id != crate::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
            || self.schema_version != 1
            || self.origin.validate().is_err()
            || self.result_operation_id.trim().is_empty()
            || !is_lower_sha256(&self.result_input_sha256)
            || self.result_module_receipt.validate().is_err()
            || self.result_module_receipt.operation_id != self.result_operation_id
            || self.result_module_receipt.binding_id != self.origin.binding_id
            || self.result_module_receipt.binding_generation != self.origin.binding_generation
            || self.result_module_receipt.input_sha256 != self.result_input_sha256
            || !is_lower_sha256(&self.payload_sha256)
            || self.payload_bytes > i64::MAX as u64
            || self.native_response_identity.as_ref().is_some_and(|id| {
                id.trim().is_empty() || id.len() > 512 || id.chars().any(char::is_control)
            })
            || self.execution_complete
            || self.task_completion != "unknown"
            || self.native_replay
        {
            return Err("normalized result page source is invalid");
        }
        Ok(())
    }
}

/// Exact immutable event identity retained by a Goal terminal evidence
/// producer.  `id` may be an adapter-owned journal identity; it is not
/// required to be a provider-issued identifier.  Store binds this reference
/// to the outer immutable observation and the source Operation tuple.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalTerminalEventRef {
    pub id: String,
    pub seq: u64,
    pub sha256: String,
}

impl GoalTerminalEventRef {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !valid_bounded_identity(&self.id, 512) || self.seq == 0 || !is_lower_sha256(&self.sha256)
        {
            return Err("goal terminal EventRef is invalid");
        }
        Ok(())
    }
}

/// Versioned, adapter-neutral proof that one exact source Operation produced
/// one immutable completed terminal event.  It proves only the native terminal
/// fact; it never grants a Goal continuation and it never asserts Task
/// acceptance/completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalTerminalEvidence {
    pub schema_id: String,
    pub schema_version: u16,
    pub source: String,
    pub reader_revision: String,
    pub operation_id: String,
    pub binding_id: String,
    pub binding_generation: i64,
    pub task_id: String,
    pub task_revision: i64,
    pub task_snapshot_sha256: String,
    pub attempt_id: String,
    pub native_session_id: String,
    pub native_input_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_run_id: Option<String>,
    pub completion_condition: String,
    pub disposition: String,
    pub terminal_outcome: String,
    pub terminal_event: GoalTerminalEventRef,
}

impl GoalTerminalEvidence {
    pub const VERSION: u16 = 1;

    pub fn validate(&self) -> Result<(), &'static str> {
        let source_valid = matches!(self.source.as_str(), "codex" | "opencode");
        let reader_valid = matches!(
            (self.source.as_str(), self.reader_revision.as_str()),
            ("codex", "codex-turn-journal-v1") | ("opencode", "opencode-execution-log-v1")
        );
        let completion_valid = matches!(
            (self.source.as_str(), self.completion_condition.as_str()),
            ("codex", "native_turn_completed") | ("opencode", "native_execution_terminal")
        );
        if self.schema_id != crate::module_contract::GOAL_TERMINAL_EVIDENCE_SCHEMA_ID
            || self.schema_version != Self::VERSION
            || !source_valid
            || !reader_valid
            || !completion_valid
            || !valid_bounded_identity(&self.operation_id, 512)
            || !valid_bounded_identity(&self.binding_id, 512)
            || self.binding_generation <= 0
            || !valid_bounded_identity(&self.task_id, 512)
            || self.task_revision <= 0
            || !is_lower_sha256(&self.task_snapshot_sha256)
            || !valid_bounded_identity(&self.attempt_id, 512)
            || !valid_bounded_identity(&self.native_session_id, 512)
            || !valid_bounded_identity(&self.native_input_id, 512)
            || self
                .native_run_id
                .as_deref()
                .is_some_and(|value| !valid_bounded_identity(value, 512))
            || self.disposition != "completed"
            || self.terminal_outcome != "completed"
            || self.terminal_event.validate().is_err()
        {
            return Err("goal terminal evidence is invalid");
        }
        Ok(())
    }
}

/// Closed linkage used by the one manager-owned Goal continuation consumer.
/// The method is an admitted ordinary action: OpenCode keeps its controller
/// `agent.goal` path, while Codex uses the already supported `agent.send`
/// next-turn path.  The source EventRef and immutable Task/Attempt tuple are
/// retained in the same linkage so a retry cannot select a different turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalContinuationLink {
    pub schema_id: String,
    pub schema_version: u16,
    pub method: String,
    pub owner: String,
    pub source_operation_id: String,
    pub source_observation_id: i64,
    pub terminal_event: GoalTerminalEventRef,
    pub native_session_id: String,
    pub native_input_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_run_id: Option<String>,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub binding_id: String,
    pub binding_generation: i64,
    pub goal_id: String,
    pub goal_revision: i64,
    pub objective_sha256: String,
}

impl GoalContinuationLink {
    pub const VERSION: u16 = 1;
    pub const OWNER: &'static str = "manager_enabled_automation";

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_id != crate::module_contract::GOAL_CONTINUATION_SCHEMA_ID
            || self.schema_version != Self::VERSION
            || !matches!(self.method.as_str(), "agent.goal" | "agent.send")
            || self.owner != Self::OWNER
            || !valid_bounded_identity(&self.source_operation_id, 512)
            || self.source_observation_id <= 0
            || self.terminal_event.validate().is_err()
            || !valid_bounded_identity(&self.native_session_id, 512)
            || !valid_bounded_identity(&self.native_input_id, 512)
            || self
                .native_run_id
                .as_deref()
                .is_some_and(|value| !valid_bounded_identity(value, 512))
            || !valid_bounded_identity(&self.task_id, 512)
            || self.task_revision <= 0
            || !valid_bounded_identity(&self.attempt_id, 512)
            || !valid_bounded_identity(&self.binding_id, 512)
            || self.binding_generation <= 0
            || !valid_bounded_identity(&self.goal_id, 512)
            || self.goal_revision <= 0
            || !is_lower_sha256(&self.objective_sha256)
        {
            return Err("goal continuation linkage is invalid");
        }
        Ok(())
    }
}

/// Store-enriched context for one exact Codex Goal continuation `agent.send`.
/// This is carried in the authenticated RuntimeCommand envelope after the
/// ordinary request has been persisted. It is not a caller-provided
/// capability or a task-dispatch receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalContinuationAdmissionContext {
    pub schema_id: String,
    pub schema_version: u16,
    pub operation_id: String,
    pub binding_id: String,
    pub binding_generation: i64,
    pub worker_boot_id: String,
    pub continuation: GoalContinuationLink,
}

impl GoalContinuationAdmissionContext {
    pub const VERSION: u16 = 1;

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_id != crate::module_contract::GOAL_CONTINUATION_CONTEXT_SCHEMA_ID
            || self.schema_version != Self::VERSION
            || !valid_bounded_identity(&self.operation_id, 512)
            || !valid_bounded_identity(&self.binding_id, 512)
            || self.binding_generation <= 0
            || !valid_bounded_identity(&self.worker_boot_id, 512)
            || self.continuation.validate().is_err()
            || self.continuation.method != "agent.send"
            || self.continuation.binding_id != self.binding_id
            || self.continuation.binding_generation != self.binding_generation
            || self.continuation.source_operation_id == self.operation_id
        {
            return Err("goal continuation admission context is invalid");
        }
        Ok(())
    }
}

/// Adapter receipt that proves admission and exact native input identity for
/// one Store-authorized Goal continuation. It is deliberately distinct from
/// `TaskDispatchAdmissionReceipt`: an ordinary `agent.send` continuation must
/// never be presented as a `task.dispatch` producer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalContinuationAdmissionReceipt {
    pub schema_id: String,
    pub schema_version: u16,
    pub context: GoalContinuationAdmissionContext,
    pub module_receipt: ModuleReceiptIdentity,
    pub native_payload_sha256: String,
    pub native_payload_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_input_id: Option<String>,
}

impl GoalContinuationAdmissionReceipt {
    pub const VERSION: u16 = 1;

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_id != crate::module_contract::GOAL_CONTINUATION_ADMISSION_SCHEMA_ID
            || self.schema_version != Self::VERSION
            || self.context.validate().is_err()
            || self.module_receipt.validate().is_err()
            || self.module_receipt.operation_id != self.context.operation_id
            || self.module_receipt.binding_id != self.context.binding_id
            || self.module_receipt.binding_generation != self.context.binding_generation
            || !is_lower_sha256(&self.native_payload_sha256)
            || self.native_payload_bytes == 0
            || self
                .native_input_id
                .as_ref()
                .is_some_and(|id| !valid_bounded_identity(id, 512))
        {
            return Err("goal continuation admission receipt is invalid");
        }
        Ok(())
    }
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectOutcome {
    /// Native admission is known, but the required application boundary is pending.
    Accepted,
    Applied,
    Rejected,
    Unknown,
}

/// Bounded, non-secret proof that the adapter-owned native service published
/// its validated readiness records.  The adapter never projects the owner
/// JSON, endpoint, password, or stderr into this DTO.  Store adds its
/// retained route digest before persisting the existing owned-service proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedServiceProcessIdentity {
    pub pid: u32,
    pub birth_token: String,
    pub binary_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedServiceReadyReceipt {
    pub schema_version: u16,
    pub status: String,
    pub service_id: String,
    pub service_version: String,
    pub owner_nonce: String,
    pub process: OwnedServiceProcessIdentity,
    pub endpoint_digest: String,
    pub connection_digest: String,
    pub config_digest: String,
    pub plugin_module_sha256: String,
    pub plugin_entrypoint_sha256: String,
    pub server_program_sha256: String,
    pub bun_sha256: String,
    pub readiness_observed: bool,
    pub plugin_loaded: String,
    pub dispatch_permitted: bool,
}

impl OwnedServiceReadyReceipt {
    /// Validate only the adapter-owned shape.  Binding, nonce, route, and
    /// operation identity are checked by Store against its retained row.
    pub fn validate(&self) -> Result<(), &'static str> {
        let process = &self.process;
        if self.schema_version != 1
            || self.status != "ready"
            || !valid_bounded_identity(&self.service_id, 128)
            || !valid_bounded_identity(&self.service_version, 128)
            || !valid_bounded_identity(&self.owner_nonce, 128)
            || process.pid == 0
            || !is_lower_sha256(&process.birth_token)
            || !is_lower_sha256(&process.binary_sha256)
            || !is_lower_sha256(&self.endpoint_digest)
            || !is_lower_sha256(&self.connection_digest)
            || !is_lower_sha256(&self.config_digest)
            || !is_lower_sha256(&self.plugin_module_sha256)
            || !is_lower_sha256(&self.plugin_entrypoint_sha256)
            || !is_lower_sha256(&self.server_program_sha256)
            || !is_lower_sha256(&self.bun_sha256)
            || !self.readiness_observed
            || self.plugin_loaded != "unknown"
            || self.dispatch_permitted
        {
            return Err("owned service readiness receipt is invalid");
        }
        Ok(())
    }
}

fn valid_bounded_identity(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOutcome {
    pub operation_id: String,
    pub outcome: EffectOutcome,
    #[serde(default)]
    pub native_scope_key: Option<String>,
    #[serde(default)]
    pub native_root_id: Option<String>,
    #[serde(default)]
    pub turn_id: Option<String>,
    /// Durable native input admission is not a native turn or its completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_input_id: Option<String>,
    pub details: Value,
}
