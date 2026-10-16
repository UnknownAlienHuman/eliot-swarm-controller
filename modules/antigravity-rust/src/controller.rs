use std::collections::{BTreeMap, VecDeque};

use serde_json::json;
use sha2::{Digest, Sha256};
use swarm_contracts::{
    error::Result,
    runtime::{
        ANTIGRAVITY_DISPATCH_SESSION_PROOF_KIND, AntigravityDispatchSessionProof, EffectOutcome,
        RuntimeCommand, RuntimeOutcome, TaskDispatchAdmissionReceipt,
    },
    task_prompt::TASK_PROMPT_CONTRACT_REVISION,
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    launch::selected_model_id,
    module_receipt,
    stream::{Phase, StreamState, TerminalDisposition, Turn},
    wire::{OperationIdentity, encode_user_line, normalized_dispatch_admission, prompt_for},
};

const MAX_RECONCILE_RECEIPTS: usize = 128;
const MAX_PENDING_RECEIPTS: usize = 8;

#[derive(Debug)]
pub enum PromptWrite {
    Written,
    Unknown(RuntimeOutcome),
    MissingPendingOperation,
}

#[derive(Debug, Clone)]
pub struct PendingObservation {
    pub event_id: String,
    pub sequence: u64,
    pub state: serde_json::Value,
    pub observation_id: Option<i64>,
}

#[derive(Debug, Clone)]
struct PendingPrompt {
    identity: OperationIdentity,
    conversation_id: String,
    result_ordinal_before: u64,
    stream_generation: u64,
    dispatch_admission: Option<TaskDispatchAdmissionReceipt>,
}

/// Immutable record of one completed native turn, keyed by the exact prompt
/// Operation that settled it. Store ACKs and prunes the adapter's retry
/// outbox, so a body lookup cannot depend on pending_receipts surviving. This
/// mapping is a separate bounded same-process cache that keeps only what a
/// later normalized result page needs: the exact receipt, conversation,
/// ordinal, boot, and stream generation that prove which prompt owned which
/// response. It is never sufficient to recover after this process exits.
#[derive(Debug, Clone)]
struct CompletedTurn {
    receipt: swarm_contracts::runtime::ModuleReceiptIdentity,
    conversation_id: String,
    result_ordinal: u64,
    boot_id: String,
    stream_generation: u64,
}

/// Stateful native turn reducer. The manager remains the sole durable
/// operation/journal authority; its outbox and reconciliation summaries are
/// bounded same-process caches and are never sufficient to recover after this
/// process exits.
pub struct Controller {
    boot_id: String,
    native_scope_key: String,
    binding_id: String,
    generation: i64,
    native_root_id: Option<String>,
    requested_model_id: Option<String>,
    model_readback_state: Option<&'static str>,
    open_failure_code: Option<&'static str>,
    normalized_dispatch_enabled: bool,
    pending_open: Option<OperationIdentity>,
    pending: Option<PendingPrompt>,
    stream: StreamState,
    completed_turns: BTreeMap<String, CompletedTurn>,
    pending_receipts: VecDeque<serde_json::Value>,
    journal: VecDeque<serde_json::Value>,
    local_execution_results: VecDeque<serde_json::Value>,
    observation_sequence: u64,
    pending_observation: Option<PendingObservation>,
}

impl Controller {
    pub fn new(
        boot_id: String,
        native_scope_key: String,
        binding_id: String,
        generation: i64,
        native_root_id: Option<String>,
        normalized_dispatch_enabled: bool,
    ) -> Self {
        Self {
            boot_id,
            native_scope_key,
            binding_id,
            generation,
            native_root_id,
            requested_model_id: None,
            model_readback_state: None,
            open_failure_code: None,
            normalized_dispatch_enabled,
            pending_open: None,
            pending: None,
            stream: StreamState::new(0),
            completed_turns: BTreeMap::new(),
            pending_receipts: VecDeque::new(),
            journal: VecDeque::new(),
            local_execution_results: VecDeque::new(),
            observation_sequence: 0,
            pending_observation: None,
        }
    }

    pub fn stream(&self) -> &StreamState {
        &self.stream
    }

    pub fn stream_mut(&mut self) -> &mut StreamState {
        &mut self.stream
    }

    pub fn consume_line(&mut self, line: &[u8]) {
        self.stream.consume_line(line);
    }

    pub fn consume_frame_error(&mut self) {
        self.stream.note_malformed_frame();
    }

    /// The child stream ended or the reader failed. This only records the
    /// direct child's status; it says nothing about other manager-owned child
    /// processes or native remote work.
    pub fn child_exited(&mut self, exit_code: Option<i32>) -> Option<RuntimeOutcome> {
        self.stream.note_stream_end(exit_code);
        if self.pending_open.is_some() {
            self.finish_open_without_init()
        } else {
            self.pending_unknown("NATIVE_STREAM_ENDED_BEFORE_TERMINAL_RESULT")
        }
    }

    pub fn stdout_failed(&mut self) -> Option<RuntimeOutcome> {
        self.stream.note_stream_failure();
        if self.pending_open.is_some() {
            self.finish_open_without_init()
        } else {
            self.pending_unknown("NATIVE_STREAM_FAILED")
        }
    }

    pub fn native_root_id(&self) -> Option<&str> {
        self.native_root_id.as_deref()
    }

    pub fn may_request_next(&self) -> bool {
        // An agent.reconcile command may need one separate target receipt in
        // addition to its own outcome. Reserve both slots before module.next
        // can durably admit that command.
        self.pending_receipts.len().saturating_add(2) <= MAX_PENDING_RECEIPTS
    }

    pub fn begin_open(&mut self, command: &RuntimeCommand) -> Result<OperationIdentity> {
        let identity = self.identity_for(command)?;
        if identity.method != "agent.open" {
            return Err(swarm_contracts::error::Error::invalid(
                "EXPECTED_AGENT_OPEN",
            ));
        }
        if self.pending_open.is_some() || self.native_root_id.is_some() {
            return Err(swarm_contracts::error::Error::new(
                "SESSION_ALREADY_OPEN",
                "this adapter process already owns an open native session",
            ));
        }
        let requested_model = selected_model_id(&command.route)
            .map_err(|code| {
                swarm_contracts::error::Error::new(code, "native route model override is invalid")
            })?
            .map(str::to_owned);
        self.requested_model_id = requested_model;
        self.model_readback_state = None;
        self.open_failure_code = None;
        self.stream
            .set_resume_baseline_pending(command.input.get("resume_conversation_id").is_some());
        self.pending_open = Some(identity.clone());
        Ok(identity)
    }

    /// Open becomes Applied only after the native `init` event establishes its
    /// conversation identity. The module manager already durably marked this
    /// exact operation `sending` before it delivered the command.
    pub fn complete_open(&mut self, command: &RuntimeCommand) -> Result<RuntimeOutcome> {
        let identity = self.identity_for(command)?;
        if identity.method != "agent.open" {
            return Err(swarm_contracts::error::Error::invalid(
                "EXPECTED_AGENT_OPEN",
            ));
        }
        if self.pending_open.as_ref() != Some(&identity) {
            return Err(swarm_contracts::error::Error::invalid(
                "OPEN_OPERATION_IDENTITY_MISMATCH",
            ));
        }
        let init = self.stream.init.as_ref().ok_or_else(|| {
            swarm_contracts::error::Error::new(
                "NATIVE_INIT_NOT_OBSERVED",
                "native session initialization has not been observed",
            )
        })?;
        let requested_model = self.requested_model_id.clone();
        match requested_model.as_deref() {
            Some(requested_model) => {
                let Some(observed_model) = init.model.as_deref() else {
                    self.open_failure_code = Some("NATIVE_MODEL_READBACK_MISSING");
                    self.model_readback_state = Some("not_observed");
                    return Err(swarm_contracts::error::Error::new(
                        "NATIVE_MODEL_READBACK_MISSING",
                        "native init did not report the explicitly selected model",
                    ));
                };
                if observed_model != requested_model {
                    self.open_failure_code = Some("NATIVE_MODEL_READBACK_MISMATCH");
                    self.model_readback_state = Some("mismatch");
                    return Err(swarm_contracts::error::Error::new(
                        "NATIVE_MODEL_READBACK_MISMATCH",
                        "native init model differs from the current native route selection",
                    ));
                }
                self.model_readback_state = Some("verified");
            }
            None => {
                // No explicit route override was requested, so no --model flag
                // was passed and the installed harness default applies.
                // init.model is reported as observed when present and never
                // guessed, so the effective model stays unknown unless the
                // native stream observed it. A v2 dispatch proof can attest
                // session identity without claiming verified model selection.
                self.model_readback_state = Some("native_default");
            }
        }
        if command
            .native_root_id
            .as_deref()
            .is_some_and(|stored| stored != init.conversation_id.as_str())
        {
            self.open_failure_code = Some("NATIVE_RESUME_IDENTITY_MISMATCH");
            return Err(swarm_contracts::error::Error::new(
                "NATIVE_RESUME_IDENTITY_MISMATCH",
                "native init differs from the manager's stored conversation identity",
            ));
        }
        if let Some(resume_id) = command
            .input
            .get("resume_conversation_id")
            .and_then(serde_json::Value::as_str)
            && resume_id != init.conversation_id
        {
            self.open_failure_code = Some("NATIVE_RESUME_IDENTITY_MISMATCH");
            return Err(swarm_contracts::error::Error::new(
                "NATIVE_RESUME_IDENTITY_MISMATCH",
                "native init did not identify the explicitly resumed conversation",
            ));
        }
        self.native_root_id = Some(init.conversation_id.clone());
        self.pending_open = None;
        let outcome = RuntimeOutcome {
            operation_id: identity.operation_id.clone(),
            outcome: EffectOutcome::Applied,
            native_scope_key: self.native_scope_key_if_root_known(),
            native_root_id: Some(init.conversation_id.clone()),
            turn_id: None,
            native_input_id: None,
            details: json!({
                "completion_condition": "native_session_initialized",
                "native_conversation_id": init.conversation_id,
                "native_init": init,
                "bridge_boot_id": self.boot_id,
                "resumed_conversation": command.input.get("resume_conversation_id").is_some(),
                "describe": {
                    "session_id": init.conversation_id,
                    "model_requested": self.requested_model_id,
                    "model_observed": init.model,
                    "model_verification": self.model_readback_state,
                    "permission_mode_observed": init.permission_mode,
                    "agent_observed": init.agent,
                    "executor_version": serde_json::Value::Null,
                }
            }),
        };
        self.record_outcome(&outcome, &identity.method, &identity.module_receipt)?;
        Ok(outcome)
    }

    /// A spawned open ending before `init` is a known rejection only when the
    /// native CLI emitted its explicit pre-init result; otherwise it is
    /// unknown. Neither path relaunches the process.
    pub fn finish_open_without_init(&mut self) -> Option<RuntimeOutcome> {
        let identity = self.pending_open.take()?;
        let native_responded = self.stream.phase == crate::stream::Phase::InitFailed;
        let diagnostic_code = self.open_failure_code.unwrap_or(if native_responded {
            "NATIVE_INIT_REJECTED"
        } else {
            "NATIVE_INIT_OUTCOME_UNKNOWN"
        });
        let outcome = RuntimeOutcome {
            operation_id: identity.operation_id.clone(),
            outcome: if native_responded {
                EffectOutcome::Rejected
            } else {
                EffectOutcome::Unknown
            },
            native_scope_key: self.native_scope_key_if_root_known(),
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details: json!({
                "diagnostic_code": diagnostic_code,
                "native_status": self.stream.init_failure_status,
                "native_init": self.stream.init,
                "native_conversation_id_observed": self.stream.init.as_ref().map(|init| &init.conversation_id),
                "model_requested": self.requested_model_id,
                "model_observed": self.stream.init.as_ref().and_then(|init| init.model.as_deref()),
                "model_verification": self.model_readback_state,
            }),
        };
        let _ = self.record_outcome(&outcome, "agent.open", &identity.module_receipt);
        Some(outcome)
    }

    /// Safe rejection for configuration/spawn failure before a native process
    /// was created.
    pub fn reject_open_before_spawn(&mut self, code: &'static str) -> Option<RuntimeOutcome> {
        let identity = self.pending_open.take()?;
        let outcome = RuntimeOutcome {
            operation_id: identity.operation_id.clone(),
            outcome: EffectOutcome::Rejected,
            native_scope_key: self.native_scope_key_if_root_known(),
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details: json!({ "diagnostic_code": code }),
        };
        let _ = self.record_outcome(&outcome, "agent.open", &identity.module_receipt);
        Some(outcome)
    }

    /// Record the exact Operation before attempting the native write. The
    /// caller must have received it through authenticated `module.next`, which
    /// is the manager's durable write-before-effect marker.
    pub fn begin_prompt(&mut self, command: &RuntimeCommand) -> Result<Vec<u8>> {
        let identity = self.identity_for(command)?;
        if !matches!(identity.method.as_str(), "task.dispatch" | "agent.send") {
            return Err(swarm_contracts::error::Error::invalid(
                "EXPECTED_PROMPT_OPERATION",
            ));
        }
        if identity.method == "task.dispatch" && !self.normalized_dispatch_enabled {
            return Err(swarm_contracts::error::Error::new(
                "TASK_PROMPT_CONTRACT_REQUIRED",
                "artifact v5 requires the Store-produced TaskPrompt and dispatch admission schemas",
            ));
        }
        let conversation_id = self.native_root_id.as_deref().ok_or_else(|| {
            swarm_contracts::error::Error::new(
                "NATIVE_SESSION_NOT_READY",
                "native session identity is not known",
            )
        })?;
        if command.native_root_id.as_deref() != Some(conversation_id) {
            return Err(swarm_contracts::error::Error::invalid(
                "NATIVE_IDENTITY_MISMATCH",
            ));
        }
        if self.pending.is_some() {
            return Err(swarm_contracts::error::Error::new(
                "NATIVE_TURN_PENDING",
                "a prior native turn has no terminal readback",
            ));
        }
        let text = prompt_for(command).map_err(swarm_contracts::error::Error::invalid)?;
        let line = encode_user_line(&text).map_err(swarm_contracts::error::Error::invalid)?;
        let dispatch_admission =
            if self.normalized_dispatch_enabled && identity.method == "task.dispatch" {
                Some(
                    normalized_dispatch_admission(command, &identity, &self.boot_id, &text)
                        .map_err(swarm_contracts::error::Error::invalid)?,
                )
            } else {
                None
            };
        self.pending = Some(PendingPrompt {
            identity,
            conversation_id: conversation_id.to_owned(),
            result_ordinal_before: self.stream.result_ordinal,
            stream_generation: self.stream.stream_generation,
            dispatch_admission,
        });
        Ok(line)
    }

    /// A failed or partial write is ambiguous. It produces `Unknown` and the
    /// caller must not try `write_all` again or resend the prompt on reconnect.
    pub async fn write_prompt<W>(&mut self, writer: &mut W, line: &[u8]) -> PromptWrite
    where
        W: AsyncWrite + Unpin,
    {
        if let Err(_error) = writer.write_all(line).await {
            return self
                .pending_unknown("NATIVE_WRITE_OUTCOME_UNKNOWN")
                .map(PromptWrite::Unknown)
                .unwrap_or(PromptWrite::MissingPendingOperation);
        }
        if let Err(_error) = writer.flush().await {
            return self
                .pending_unknown("NATIVE_WRITE_OUTCOME_UNKNOWN")
                .map(PromptWrite::Unknown)
                .unwrap_or(PromptWrite::MissingPendingOperation);
        }
        PromptWrite::Written
    }

    /// A result settles only the single in-flight prompt, for the same native
    /// conversation and an explicitly terminal status with response digest.
    pub fn settle_terminal(&mut self) -> Option<RuntimeOutcome> {
        let (identity, conversation_id, result_ordinal_before, dispatch_admission) = {
            let pending = self.pending.as_ref()?;
            (
                pending.identity.clone(),
                pending.conversation_id.clone(),
                pending.result_ordinal_before,
                pending.dispatch_admission.clone(),
            )
        };
        let (turn, disposition) = self.stream.terminal_for(&conversation_id)?;
        let turn = turn.clone();
        // Headless emits one terminal result per prompt and the caller must
        // await it before the next prompt, so the settled turn must be the
        // exact next ordinal for this prompt. Accepting any greater ordinal
        // would let a later turn's result settle this Operation and publish
        // that turn's response text as this prompt's answer. A stale or absent
        // ordinal simply is not the terminal readback this prompt is waiting
        // for, so it stays pending rather than becoming an Unknown outcome.
        let expected_ordinal = result_ordinal_before.checked_add(1)?;
        if turn.result_ordinal != expected_ordinal {
            return None;
        }
        let stream_generation = self.pending.as_ref()?.stream_generation;
        if stream_generation != self.stream.stream_generation {
            return None;
        }
        if turn.response_sha256.is_none() {
            return self.pending_unknown("NATIVE_RESULT_FINGERPRINT_UNAVAILABLE");
        }
        let outcome = terminal_outcome(
            &identity,
            &self.boot_id,
            &self.native_scope_key,
            &conversation_id,
            &turn,
            disposition,
            dispatch_admission.as_ref(),
        );
        if let Some(receipt) = outcome.details.get("local_execution_ref") {
            if self.local_execution_results.len() == crate::stream::MAX_TURNS {
                self.local_execution_results.pop_front();
            }
            self.local_execution_results.push_back(receipt.clone());
        }
        self.pending = None;
        self.pending_observation = None;
        let _ = self.record_outcome(&outcome, &identity.method, &identity.module_receipt);
        self.remember_completed_turn(&identity, &conversation_id, &turn);
        Some(outcome)
    }

    pub fn stream_ended(&mut self) -> Option<RuntimeOutcome> {
        self.stream.note_stream_end(None);
        self.pending_unknown("NATIVE_STREAM_ENDED_BEFORE_TERMINAL_RESULT")
    }

    /// Read the exact response body this process retained for one sequential
    /// turn. The caller names the prompt's conversation, the exact next
    /// ordinal, and the stream generation that owned the write. This only
    /// reports what this boot actually observed; it never reconstructs a body
    /// from a digest and never attributes another turn's text to this prompt.
    /// A miss is reported as a closed diagnostic so the caller fails closed.
    pub fn retained_response_body(
        &self,
        conversation_id: &str,
        result_ordinal: u64,
        stream_generation: u64,
    ) -> std::result::Result<crate::stream::RetainedResponseBody, &'static str> {
        if self.native_root_id.as_deref() != Some(conversation_id) {
            return Err("NATIVE_RESULT_BODY_ORIGIN_MISMATCH");
        }
        if stream_generation != self.stream.stream_generation {
            return Err("NATIVE_RESULT_BODY_STALE_OWNER");
        }
        self.stream
            .retained_response_for(result_ordinal, conversation_id, stream_generation)
            .map_err(|miss| miss.diagnostic_code())
    }

    /// Read the response body this process retained for one exact sealed
    /// dispatch Operation. The completed-turn mapping is separate from the
    /// retry outbox, so a prior Store ACK that prunes pending_receipts cannot
    /// erase the causal lookup. Store's sealed origin names the Operation, and
    /// the caller supplies no ordinal: the sealed producer receipt, target
    /// digest, and this adapter's own recorded mapping must all agree before
    /// any body is read. Any disagreement, or a prompt this process never
    /// settled or settled under another boot, fails closed.
    pub fn retained_response_for_operation(
        &self,
        operation_id: &str,
        origin: &swarm_contracts::runtime::NormalizedResultOriginContext,
    ) -> std::result::Result<crate::stream::RetainedResponseBody, &'static str> {
        let completed = self
            .completed_turns
            .get(operation_id)
            .ok_or("NATIVE_RESULT_BODY_UNAVAILABLE")?;
        // The sealed origin must name this exact Operation, not merely an
        // Operation whose request hashes the same. Two different Operations can
        // carry an identical input digest, so digest equality alone would let a
        // prompt's body be published under another Operation's result. Require
        // the exact Operation id and the full typed receipt equality Store sealed.
        if operation_id != origin.target_operation_id
            || completed.receipt != origin.producer.module_receipt
        {
            return Err("NATIVE_RESULT_BODY_ORIGIN_MISMATCH");
        }
        if completed.boot_id != self.boot_id
            || completed.stream_generation != self.stream.stream_generation
        {
            return Err("NATIVE_RESULT_BODY_STALE_OWNER");
        }
        self.retained_response_body(
            &completed.conversation_id,
            completed.result_ordinal,
            completed.stream_generation,
        )
    }

    /// Record the immutable causal mapping for one settled turn. This runs
    /// after the outcome is recorded and never touches the retry outbox, so a
    /// later Store ACK that prunes pending_receipts still leaves this entry
    /// intact. It is bounded and evicted together with its retained body.
    fn remember_completed_turn(
        &mut self,
        identity: &OperationIdentity,
        conversation_id: &str,
        turn: &Turn,
    ) {
        self.completed_turns.insert(
            identity.operation_id.clone(),
            CompletedTurn {
                receipt: identity.module_receipt.clone(),
                conversation_id: conversation_id.to_owned(),
                result_ordinal: turn.result_ordinal,
                boot_id: self.boot_id.clone(),
                stream_generation: self.stream.stream_generation,
            },
        );
        while self.completed_turns.len() > crate::stream::MAX_RETAINED_RESPONSES {
            // Evict the lowest result ordinal, not the lexically first key.
            // Operation ids are opaque UUIDs whose ordering carries no time
            // meaning, so a lexical eviction could discard the newest
            // completed prompt and leave an older one unreadable.
            let Some(oldest) = self
                .completed_turns
                .iter()
                .min_by_key(|(_, turn)| turn.result_ordinal)
                .map(|(operation_id, _)| operation_id.clone())
            else {
                break;
            };
            self.completed_turns.remove(&oldest);
        }
    }

    /// Reads only facts observed by this process. Antigravity exposes no
    /// history GET/recovery API, so a retained Store identity after restart is
    /// not evidence that the old native conversation can be read.
    pub fn read_command(&mut self, command: &RuntimeCommand) -> Result<RuntimeOutcome> {
        let identity = self.identity_for(command)?;
        let details = match identity.method.as_str() {
            "agent.refresh" => {
                let target = command
                    .input
                    .get("session_id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_else(|| self.native_root_id.as_deref().unwrap_or(""));
                if target.is_empty()
                    || self.native_root_id.as_deref() != Some(target)
                    || self.stream.init.is_none()
                {
                    return Ok(self.read_unknown(
                        &identity,
                        "NATIVE_HISTORY_UNAVAILABLE",
                        "Antigravity exposes no exact native history readback for this root",
                    ));
                }
                let snapshot = self.stream.snapshot();
                let dispatch_session_proof = self.dispatch_session_proof(&identity);
                json!({
                    "completion_condition": "current_process_native_observation",
                    "target": target,
                    "snapshot": snapshot,
                    "antigravity_dispatch_session_proof": dispatch_session_proof,
                })
            }
            "agent.reconcile" => {
                let target = command
                    .input
                    .get("operation_id")
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(|| {
                        swarm_contracts::error::Error::invalid("RECONCILE_OPERATION_ID_REQUIRED")
                    })?;
                let target_receipt = module_receipt::for_reconcile_target(command)?;
                if let Some(saved) = self
                    .pending_receipts
                    .iter()
                    .find(|entry| entry["operation_id"].as_str() == Some(target))
                {
                    let saved_identity = saved["details"]["module_receipt"].clone();
                    if saved_identity != serde_json::to_value(&target_receipt)? {
                        return Err(swarm_contracts::error::Error::new(
                            "MODULE_RECEIPT_TARGET_CONFLICT",
                            "pending target receipt differs from the exact Store target identity",
                        ));
                    }
                } else {
                    let target_outcome = RuntimeOutcome {
                        operation_id: target.to_owned(),
                        outcome: EffectOutcome::Unknown,
                        native_scope_key: self.native_scope_key_if_root_known(),
                        native_root_id: self.native_root_id.clone(),
                        turn_id: None,
                        native_input_id: None,
                        details: json!({
                            "diagnostic_code": "NATIVE_HISTORY_UNAVAILABLE",
                            "completion_condition": "native_history_unavailable",
                            "target_operation_id": target,
                            "reconcile_operation_id": identity.operation_id,
                        }),
                    };
                    self.record_outcome(
                        &target_outcome,
                        "agent.reconcile_target",
                        &target_receipt,
                    )?;
                }
                let outcome = RuntimeOutcome {
                    operation_id: identity.operation_id.clone(),
                    outcome: EffectOutcome::Unknown,
                    native_scope_key: self.native_scope_key_if_root_known(),
                    native_root_id: self.native_root_id.clone(),
                    turn_id: None,
                    native_input_id: None,
                    details: json!({
                        "diagnostic_code": "NATIVE_HISTORY_UNAVAILABLE",
                        "completion_condition": "native_history_unavailable",
                        "target_operation_id": target,
                    }),
                };
                self.record_outcome(&outcome, &identity.method, &identity.module_receipt)?;
                return Ok(outcome);
            }
            _ => {
                return Err(swarm_contracts::error::Error::invalid(
                    "READ_METHOD_REQUIRED",
                ));
            }
        };
        let outcome = RuntimeOutcome {
            operation_id: identity.operation_id.clone(),
            outcome: EffectOutcome::Applied,
            native_scope_key: self.native_scope_key_if_root_known(),
            native_root_id: self.native_root_id.clone(),
            turn_id: None,
            native_input_id: None,
            details,
        };
        self.record_outcome(&outcome, &identity.method, &identity.module_receipt)?;
        Ok(outcome)
    }

    fn dispatch_session_proof(
        &self,
        identity: &OperationIdentity,
    ) -> Option<AntigravityDispatchSessionProof> {
        let init = self.stream.init.as_ref()?;
        let native_root_id = self.native_root_id.as_deref()?;
        // Model selection is independent of session/Task identity. A native
        // default has no requested model and may have no model observation;
        // v2 preserves those unknowns without claiming verified selection.
        let requested_model_id = self.requested_model_id.as_deref();
        let observed_model_id = init.model.as_deref();
        if requested_model_id.is_some() && observed_model_id.is_none() {
            return None;
        }
        let model_readback_state = self.model_readback_state?;
        let stream_phase = match self.stream.phase {
            Phase::AwaitingInit => "awaiting_init",
            Phase::Ready => "ready",
            Phase::InitFailed => "init_failed",
            Phase::StreamEnded => "stream_ended",
            Phase::StreamFailed => "stream_failed",
        };
        Some(AntigravityDispatchSessionProof {
            schema_version: if requested_model_id.is_some() { 1 } else { 2 },
            kind: if requested_model_id.is_some() {
                ANTIGRAVITY_DISPATCH_SESSION_PROOF_KIND
            } else {
                swarm_contracts::runtime::ANTIGRAVITY_NATIVE_DEFAULT_SESSION_PROOF_KIND
            }
            .to_owned(),
            refresh_operation_id: identity.operation_id.clone(),
            binding_id: self.binding_id.clone(),
            binding_generation: self.generation,
            worker_boot_id: self.boot_id.clone(),
            native_root_id: native_root_id.to_owned(),
            native_scope_key_sha256: format!(
                "{:x}",
                Sha256::digest(self.native_scope_key.as_bytes())
            ),
            conversation_id: init.conversation_id.clone(),
            requested_model_id: requested_model_id.map(str::to_owned),
            observed_model_id: observed_model_id.map(str::to_owned),
            model_readback_state: model_readback_state.to_owned(),
            stream_phase: stream_phase.to_owned(),
            native_events_seen: self.stream.native_events_seen,
            result_ordinal: self.stream.result_ordinal,
            gap_count: self.stream.gaps,
            turn_pending: self.pending.is_some() || self.pending_open.is_some(),
            stream_error_code: self.stream.stream_error_code.map(str::to_owned),
            stream_exit_code: self.stream.exit_code,
        })
    }

    fn read_unknown(
        &mut self,
        identity: &OperationIdentity,
        diagnostic_code: &'static str,
        message: &'static str,
    ) -> RuntimeOutcome {
        let outcome = RuntimeOutcome {
            operation_id: identity.operation_id.clone(),
            outcome: EffectOutcome::Unknown,
            native_scope_key: self.native_scope_key_if_root_known(),
            native_root_id: self.native_root_id.clone(),
            turn_id: None,
            native_input_id: None,
            details: json!({
                "diagnostic_code": diagnostic_code,
                "completion_condition": "native_history_unavailable",
                "message": message,
            }),
        };
        let _ = self.record_outcome(&outcome, &identity.method, &identity.module_receipt);
        outcome
    }

    fn identity_for(&self, command: &RuntimeCommand) -> Result<OperationIdentity> {
        let identity =
            OperationIdentity::try_from(command).map_err(swarm_contracts::error::Error::invalid)?;
        if identity.binding_id != self.binding_id || identity.generation != self.generation {
            return Err(swarm_contracts::error::Error::new(
                "BINDING_IDENTITY_MISMATCH",
                "command belongs to another binding generation",
            ));
        }
        Ok(identity)
    }

    pub fn cached_readback(&self, operation_id: &str) -> Option<serde_json::Value> {
        self.pending_receipts
            .iter()
            .find(|outcome| outcome["operation_id"].as_str() == Some(operation_id))
            .cloned()
    }

    pub fn pending_receipts(&self) -> Vec<serde_json::Value> {
        self.pending_receipts.iter().cloned().collect()
    }

    pub fn receipt_needs_observation(value: &serde_json::Value) -> bool {
        value["details"]["local_execution_ref"].is_object()
            && value["details"]["local_execution_ref"]["observation_id"].is_null()
    }

    /// Record a deterministic rejection for a manager-admitted but unsupported
    /// operation. Identity validation still binds it to this route and exact
    /// binding generation before it enters the local outbox.
    pub fn reject_command(
        &mut self,
        command: &RuntimeCommand,
        diagnostic_code: &'static str,
    ) -> Result<RuntimeOutcome> {
        let identity = self.identity_for(command)?;
        let details = if diagnostic_code == "RESULT_BODY_UNAVAILABLE" {
            json!({
                "diagnostic_code": diagnostic_code,
                "completion_condition": "result_body_unavailable",
                "native_response_identity": "unavailable",
                "execution_complete": false,
                "task_completion": "unknown",
                "native_replay": false,
            })
        } else {
            json!({ "diagnostic_code": diagnostic_code })
        };
        let outcome = RuntimeOutcome {
            operation_id: identity.operation_id.clone(),
            outcome: EffectOutcome::Rejected,
            native_scope_key: self.native_scope_key_if_root_known(),
            native_root_id: self.native_root_id.clone(),
            turn_id: None,
            native_input_id: None,
            details,
        };
        self.record_outcome(&outcome, &identity.method, &identity.module_receipt)?;
        Ok(outcome)
    }

    pub fn prepare_observation(&mut self) -> PendingObservation {
        if let Some(pending) = &self.pending_observation {
            return pending.clone();
        }
        self.observation_sequence = self.observation_sequence.saturating_add(1);
        let mut state = self.stream.snapshot();
        if let Some(object) = state.as_object_mut() {
            object.insert("native_root_id".to_owned(), json!(self.native_root_id));
            object.insert("native_scope_key".to_owned(), json!(self.native_scope_key));
            object.insert("boot_id".to_owned(), json!(self.boot_id));
            object.insert(
                "requested_model_id".to_owned(),
                json!(self.requested_model_id),
            );
            object.insert(
                "model_readback_state".to_owned(),
                json!(self.model_readback_state),
            );
            object.insert(
                "local_execution_results".to_owned(),
                json!(self.local_execution_results),
            );
        }
        let pending = PendingObservation {
            event_id: format!("{}:{}", self.boot_id, self.observation_sequence),
            sequence: self.observation_sequence,
            state,
            observation_id: None,
        };
        self.pending_observation = Some(pending.clone());
        pending
    }

    /// Invalidate a replayed/stale observation so the next call uses a fresh
    /// monotonic event id. No receipt can cite the stale response.
    pub fn retry_observation_after_stale(&mut self) {
        self.pending_observation = None;
    }

    pub fn acknowledge_observation(&mut self, response: &serde_json::Value) -> Option<i64> {
        let recorded = response
            .get("recorded")
            .and_then(serde_json::Value::as_bool)
            == Some(true);
        let stale = response.get("stale").and_then(serde_json::Value::as_bool) == Some(true);
        let observation_id = response
            .get("observation_id")
            .and_then(serde_json::Value::as_i64)
            .filter(|id| *id > 0);
        if !recorded || stale || observation_id.is_none() {
            if stale
                || response
                    .get("replayed")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
            {
                self.retry_observation_after_stale();
            }
            return None;
        }
        if let (Some(pending), Some(observation_id)) =
            (self.pending_observation.as_mut(), observation_id)
        {
            pending.observation_id = Some(observation_id);
            Some(observation_id)
        } else {
            None
        }
    }

    pub fn finish_observation(&mut self) {
        self.pending_observation = None;
    }

    pub fn bind_observation_id(
        &mut self,
        outcome: &mut RuntimeOutcome,
        observation_id: i64,
    ) -> Result<()> {
        if observation_id <= 0 {
            return Err(swarm_contracts::error::Error::invalid(
                "OBSERVATION_ID_MUST_BE_POSITIVE",
            ));
        }
        if self
            .pending_observation
            .as_ref()
            .and_then(|pending| pending.observation_id)
            != Some(observation_id)
        {
            return Err(swarm_contracts::error::Error::invalid(
                "OBSERVATION_ID_NOT_ACKNOWLEDGED",
            ));
        }
        let receipt = outcome
            .details
            .get_mut("local_execution_ref")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(|| {
                swarm_contracts::error::Error::invalid("LOCAL_EXECUTION_RECEIPT_REQUIRED")
            })?;
        receipt.insert("observation_id".to_owned(), json!(observation_id));
        self.replace_pending_outcome(outcome)?;
        Ok(())
    }

    pub fn remember_acknowledged_outcome(&mut self, outcome: &RuntimeOutcome) {
        self.pending_receipts
            .retain(|value| value["operation_id"].as_str() != Some(outcome.operation_id.as_str()));
        self.remove_local_execution_result(&outcome.operation_id);
        self.pending_observation = None;
    }

    fn replace_pending_outcome(&mut self, outcome: &RuntimeOutcome) -> Result<()> {
        let serialized = serde_json::to_value(outcome).map_err(|_| {
            swarm_contracts::error::Error::new(
                "MODULE_OUTCOME_INVALID",
                "adapter could not retain its bound operation receipt",
            )
        })?;
        let Some(entry) = self
            .pending_receipts
            .iter_mut()
            .find(|entry| entry["operation_id"].as_str() == Some(outcome.operation_id.as_str()))
        else {
            return Err(swarm_contracts::error::Error::new(
                "MODULE_OUTCOME_NOT_PENDING",
                "bound receipt is absent from the adapter outbox",
            ));
        };
        *entry = serialized;
        Ok(())
    }

    pub fn acknowledge_outcome(&mut self, operation_id: &str) {
        self.pending_receipts
            .retain(|value| value["operation_id"].as_str() != Some(operation_id));
        self.remove_local_execution_result(operation_id);
        self.pending_observation = None;
    }

    fn remove_local_execution_result(&mut self, operation_id: &str) {
        self.local_execution_results
            .retain(|receipt| receipt["input_operation_id"].as_str() != Some(operation_id));
    }

    pub fn snapshot(&self) -> serde_json::Value {
        self.stream.snapshot()
    }

    fn pending_unknown(&mut self, diagnostic_code: &'static str) -> Option<RuntimeOutcome> {
        let pending = self.pending.take()?;
        let outcome = RuntimeOutcome {
            operation_id: pending.identity.operation_id.clone(),
            outcome: EffectOutcome::Unknown,
            native_scope_key: Some(self.native_scope_key.clone()),
            native_root_id: Some(pending.conversation_id),
            turn_id: None,
            native_input_id: None,
            details: json!({ "diagnostic_code": diagnostic_code }),
        };
        let _ = self.record_outcome(
            &outcome,
            &pending.identity.method,
            &pending.identity.module_receipt,
        );
        Some(outcome)
    }

    fn record_outcome(
        &mut self,
        outcome: &RuntimeOutcome,
        method: &str,
        receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    ) -> Result<()> {
        let value = module_receipt::serialize_outcome(outcome, receipt).map_err(|_| {
            swarm_contracts::error::Error::new(
                "MODULE_OUTCOME_INVALID",
                "adapter could not retain its bounded operation receipt",
            )
        })?;
        self.pending_receipts
            .retain(|entry| entry["operation_id"].as_str() != Some(outcome.operation_id.as_str()));
        if self.pending_receipts.len() == MAX_PENDING_RECEIPTS {
            return Err(swarm_contracts::error::Error::new(
                "MODULE_OUTCOME_OUTBOX_FULL",
                "acknowledge pending manager receipts before admitting another operation",
            ));
        }
        self.pending_receipts.push_back(value);
        let summary = json!({
            "operation_id": outcome.operation_id,
            "method": method,
            "outcome": match outcome.outcome {
                EffectOutcome::Accepted => "accepted",
                EffectOutcome::Applied => "applied",
                EffectOutcome::Rejected => "rejected",
                EffectOutcome::Unknown => "unknown",
            },
            "completion_condition": outcome.details.get("completion_condition"),
            "diagnostic_code": outcome.details.get("diagnostic_code"),
        });
        self.journal
            .retain(|entry| entry["operation_id"].as_str() != Some(outcome.operation_id.as_str()));
        if self.journal.len() == MAX_RECONCILE_RECEIPTS {
            self.journal.pop_front();
        }
        self.journal.push_back(summary);
        Ok(())
    }

    fn native_scope_key_if_root_known(&self) -> Option<String> {
        self.native_root_id
            .as_ref()
            .map(|_| self.native_scope_key.clone())
    }
}

fn terminal_outcome(
    identity: &OperationIdentity,
    boot_id: &str,
    scope_key: &str,
    conversation_id: &str,
    turn: &Turn,
    disposition: TerminalDisposition,
    dispatch_admission: Option<&TaskDispatchAdmissionReceipt>,
) -> RuntimeOutcome {
    let status = turn.status.as_str();
    let reference = json!({
        "input_operation_id": identity.operation_id,
        "native_conversation_id": conversation_id,
        "bridge_boot_id": boot_id,
        "result_ordinal": turn.result_ordinal,
        "response_sha256": turn.response_sha256,
        "status": status,
    });
    let mut details = json!({
        "completion_condition": "native_terminal_result_observed",
        "turn_status": status,
        "num_turns": turn.num_turns,
        "duration_seconds": turn.duration_seconds,
        "duration_delta_seconds": turn.duration_delta_seconds,
        "usage": turn.usage,
        "local_execution_ref": reference,
    });
    if matches!(disposition, TerminalDisposition::Applied)
        && let Some(admission) = dispatch_admission
    {
        // The stream proves that this process observed a terminal result, but
        // exposes no native input-parent edge. Retain dispatch admission without
        // promoting the local ordinal into execution or Task completion proof.
        details["execution_complete"] = json!(false);
        details["task_completion"] = json!("unknown");
        details["dispatch_admission"] =
            serde_json::to_value(admission).unwrap_or(serde_json::Value::Null);
        details["prompt_contract_revision"] = json!(TASK_PROMPT_CONTRACT_REVISION);
    }
    RuntimeOutcome {
        operation_id: identity.operation_id.clone(),
        outcome: match disposition {
            TerminalDisposition::Applied => EffectOutcome::Applied,
            TerminalDisposition::Rejected => EffectOutcome::Rejected,
        },
        native_scope_key: Some(scope_key.to_owned()),
        native_root_id: Some(conversation_id.to_owned()),
        // The CLI result carries no native turn identifier. Ordinal is only a
        // local sequential association, so keep the native turn field empty.
        turn_id: None,
        native_input_id: None,
        details,
    }
}
