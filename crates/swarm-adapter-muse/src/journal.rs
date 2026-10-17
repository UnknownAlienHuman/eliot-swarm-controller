//! Muse-specific private checkpoint/outbox, not another Task store or SDK facade.
//!
//! Ports bridge.13 `submitNative`, `completeNative`, `reconcileNative`, `report`
//! and checkpoint.mjs: intent before I/O, ACK before application, exact host
//! delivery until ACK, and no native replay on reopen/reconnect. Native wire,
//! work/model fences, authorization and outcome evidence remain caller duties.
//!
//! Format v1 is exclusive to a NEW Rust binding state directory. The existing
//! module owner owns its lock/custody; this module never acquires another owner
//! or reads/adopts Node checkpoint.json. A native root in Scope is an identity
//! fence, not evidence that it exists. session/start requires the public explicit
//! sessionId field, so the intended identity is retained before possible I/O.
//! Child provenance, session MCP launch config, family/settings projections and
//! facade activation are outside this v1 slice; unsupported params fail closed.
//! V1 has no same-live-MSP-connection proof. A no-ACK reconciliation therefore
//! requires a fresh current-boot durable-host declaration; ephemeral same-link
//! replay stays unavailable instead of assuming persisted native idempotency.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{collections::BTreeMap, error::Error, fmt, fs, path::{Path, PathBuf}};
use swarm_contracts::{
    module_catalog::{ArtifactIdentity, ModuleId, Sha256Digest},
    runtime::{EffectOutcome, ModuleReceiptIdentity, RuntimeOutcome},
};
use swarm_process::permissions::{read_private_durable, replace_private_durable, write_private_new};

const FORMAT: &str = "eliot.muse.rust-private-state";
const VERSION: u16 = 1;
const MARKER: &str = "muse-rust-scope.v1.json";
const CHECKPOINT: &str = "muse-rust-checkpoint.v1.json";
const MAX_FILE: u64 = 16 * 1024 * 1024;
const MAX_ENTRY: usize = 1024 * 1024;
const MAX_OPERATIONS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    OwnerUnverified, ScopeMismatch, LegacyState, StateAlreadyExists, MissingState,
    InvalidState, Capacity, PersistenceUnknown, Poisoned, IntentConflict,
    ExplicitReconcileRequired, AckConflict, OutcomeConflict, DurabilityUnavailable,
}
impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.code()) }
}
impl Error for Fault {}
impl Fault {
    pub fn code(self) -> &'static str {
        match self {
            Self::OwnerUnverified => "MUSE_JOURNAL_OWNER_UNVERIFIED",
            Self::ScopeMismatch => "MUSE_JOURNAL_SCOPE_MISMATCH",
            Self::LegacyState => "MUSE_JOURNAL_LEGACY_STATE",
            Self::StateAlreadyExists => "MUSE_JOURNAL_STATE_ALREADY_EXISTS",
            Self::MissingState => "MUSE_JOURNAL_MISSING_STATE",
            Self::InvalidState => "MUSE_JOURNAL_INVALID_STATE",
            Self::Capacity => "MUSE_JOURNAL_CAPACITY",
            Self::PersistenceUnknown => "MUSE_JOURNAL_PERSISTENCE_UNKNOWN",
            Self::Poisoned => "MUSE_JOURNAL_POISONED",
            Self::IntentConflict => "MUSE_JOURNAL_INTENT_CONFLICT",
            Self::ExplicitReconcileRequired => "MUSE_JOURNAL_EXPLICIT_RECONCILE_REQUIRED",
            Self::AckConflict => "MUSE_JOURNAL_ACK_CONFLICT",
            Self::OutcomeConflict => "MUSE_JOURNAL_OUTCOME_CONFLICT",
            Self::DurabilityUnavailable => "MUSE_JOURNAL_DURABILITY_UNAVAILABLE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    pub module_client_id: String,
    pub module_id: ModuleId,
    pub artifact: ArtifactIdentity,
    pub binding_id: String,
    pub generation: i64,
    pub native_namespace: String,
    pub native_root_id: String,
}
impl Scope {
    fn validate(&self) -> Result<(), Fault> {
        if self.generation <= 0 || [&self.module_client_id, &self.binding_id,
            &self.native_namespace, &self.native_root_id].iter().any(|value| !atom(value))
        { return Err(Fault::InvalidState); }
        Ok(())
    }
    fn receipt(&self, receipt: &ModuleReceiptIdentity) -> Result<(), Fault> {
        receipt.validate().map_err(|_| Fault::InvalidState)?;
        if receipt.module_id != self.module_id || receipt.artifact != self.artifact
            || receipt.binding_id != self.binding_id || receipt.binding_generation != self.generation
        { return Err(Fault::ScopeMismatch); }
        Ok(())
    }
}

fn atom(value: &str) -> bool {
    !value.is_empty() && value.len() <= 8192 && !value.chars().any(char::is_control)
}
fn digest(value: &Value) -> Result<Sha256Digest, Fault> {
    Sha256Digest::of_canonical_json(value).map_err(|_| Fault::InvalidState)
}

/// Exact saved JSON plus SHA-256 of its canonical JSON value, matching the
/// public SDK's value-equivalent command signature contract. Byte equality is
/// additionally required for saved ACKs and delivery retirement; no text repair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedJson { utf8: String, sha256: Sha256Digest }
impl SavedJson {
    fn new(value: &Value) -> Result<Self, Fault> {
        let utf8 = serde_json::to_string(value).map_err(|_| Fault::InvalidState)?;
        if utf8.len() > MAX_ENTRY { return Err(Fault::Capacity); }
        Ok(Self { utf8, sha256: digest(value)? })
    }
    fn value(&self) -> Result<Value, Fault> {
        if self.utf8.len() > MAX_ENTRY { return Err(Fault::InvalidState); }
        let value = serde_json::from_str(&self.utf8).map_err(|_| Fault::InvalidState)?;
        if digest(&value)? != self.sha256 { return Err(Fault::InvalidState); }
        Ok(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeInvocation {
    method: String,
    command_id: String,
    params: SavedJson,
    payload_sha256: Sha256Digest,
}
impl NativeInvocation {
    pub fn new(scope: &Scope, method: &str, command_id: &str, params: Map<String, Value>) -> Result<Self, Fault> {
        let params = SavedJson::new(&Value::Object(params))?;
        let invocation = Self {
            method: method.into(), command_id: command_id.into(),
            payload_sha256: digest(&json!({"method":method,"params":params.value()?}))?, params,
        };
        invocation.validate(scope)?;
        Ok(invocation)
    }
    pub fn method(&self) -> &str { &self.method }
    pub fn command_id(&self) -> &str { &self.command_id }
    pub fn payload_sha256(&self) -> &str { self.payload_sha256.as_str() }
    pub fn params(&self) -> Result<Map<String, Value>, Fault> {
        self.params.value()?.as_object().cloned().ok_or(Fault::InvalidState)
    }
    fn validate(&self, scope: &Scope) -> Result<(), Fault> {
        let id = self.command_id.as_bytes();
        if id.len() != 36 || id[14] != b'7' || !b"89ab".contains(&id[19])
            || id.iter().enumerate().any(|(n, b)| {
                if [8,13,18,23].contains(&n) { *b != b'-' } else { !b.is_ascii_hexdigit() || b.is_ascii_uppercase() }
            })
        { return Err(Fault::InvalidState); }
        let value = self.params.value()?;
        let params = value.as_object().ok_or(Fault::InvalidState)?;
        let extra: &[&str] = match self.method.as_str() {
            "session/start" => &["workspaceRoot","modelId","providerId","approvalMode"],
            "session/resume" => &["excludeItems"],
            "turn/start" => &["input","ifBusy","reasoningEffort"],
            "turn/steer" => &["input","expectedTurnId","reasoningEffort"],
            "session/setModel" => &["modelId","providerId"],
            "session/setReasoningEffort" => &["reasoningEffort"],
            "session/setApprovalMode" => &["mode"],
            "goal/set" | "goal/edit" => &["objective"],
            "goal/pause" | "goal/resume" | "goal/clear" => &[],
            "approval/decide" => &["approvalId","choiceId","feedback","requirementId"],
            "userInput/answer" => &["userInputId","answers"],
            "userInput/cancel" => &["userInputId","reason"],
            "userInput/clarify" => &["userInputId","clarification"],
            _ => return Err(Fault::InvalidState),
        };
        if params.get("commandId").and_then(Value::as_str) != Some(self.command_id.as_str())
            || params.get("sessionId").and_then(Value::as_str) != Some(scope.native_root_id.as_str())
            || params.keys().any(|key| !["commandId","sessionId"].contains(&key.as_str()) && !extra.contains(&key.as_str()))
            || self.payload_sha256 != digest(&json!({"method":&self.method,"params":&value}))?
            || self.method == "session/resume" && params.get("excludeItems") != Some(&Value::Bool(true))
        { return Err(Fault::ScopeMismatch); }
        if matches!(self.method.as_str(), "turn/start" | "turn/steer") {
            let input = params.get("input").and_then(Value::as_array).ok_or(Fault::InvalidState)?;
            if input.is_empty() || input.iter().any(|block| {
                block.as_object().is_none_or(|block| block.len() != 2
                    || block.get("type").and_then(Value::as_str) != Some("text")
                    || block.get("text").and_then(Value::as_str).is_none())
            }) || self.method == "turn/start" && params.get("ifBusy").is_some_and(|value| value != "queue")
                || self.method == "turn/steer" && params.get("expectedTurnId").and_then(Value::as_str).is_none_or(str::is_empty)
            { return Err(Fault::InvalidState); }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Durability { Durable, Ephemeral, Unrecognized }
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurabilityFact { profile: Durability, observed_boot: String }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativePending {
    invocation: NativeInvocation,
    submission_boot: String,
    ack: Option<SavedJson>,
    reconcile_block: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum DeliveryMethod { Outcome, Result }
impl DeliveryMethod {
    fn method(&self) -> &'static str {
        match self { Self::Outcome => "module.outcome", Self::Result => "module.result" }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Delivery { method: DeliveryMethod, params: SavedJson }
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Operation {
    receipt: ModuleReceiptIdentity,
    native: Option<NativePending>,
    final_delivery: Option<Delivery>,
    final_host_acked: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Slot { revision: u64, delivery: Delivery }
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    revision: u64,
    durability: Option<DurabilityFact>,
    operations: BTreeMap<String, Operation>,
    outbox: BTreeMap<String, Slot>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScopeFile { format: String, version: u16, scope: Scope }
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint { format: String, version: u16, scope: Scope, state: State, sha256: Sha256Digest }

/// Private delivery token. A stale ACK can never retire a newer revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryTicket { operation_id: String, slot: Slot }
impl DeliveryTicket {
    pub fn operation_id(&self) -> &str { &self.operation_id }
    pub fn method(&self) -> &'static str { self.slot.delivery.method.method() }
    pub fn params_json(&self) -> &str { &self.slot.delivery.params.utf8 }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckDisposition { Retired, Superseded, AlreadyRetired }

/// Borrow excludes other checkpoint changes until the caller synchronously
/// enqueues this exact command. It grants no Store/native authorization. Drop
/// does not prove no write: the retained intent remains potentially sent.
pub struct PreparedNative<'a> { journal: &'a mut Journal, operation_id: String }
impl PreparedNative<'_> {
    pub fn invocation(&self) -> &NativeInvocation {
        &self.journal.state.operations[&self.operation_id].native.as_ref().expect("retained intent").invocation
    }
}
pub enum Reconciliation<'a> {
    Write(PreparedNative<'a>),
    AckReadback(Value),
    AwaitingHostReceipt,
    Finalized,
    Blocked(String),
}

/// One instance per owner/worker, serialized through mutable access. The caller
/// must not open concurrent writers; the existing owner holds the directory OS
/// lock. No second lock, scheduler, Task DB, or automatic retention GC is added.
/// Capacity is explicit until safe result/reference reclamation is implemented.
pub struct Journal { path: PathBuf, scope: Scope, boot: String, state: State, poisoned: bool }

impl Journal {
    /// Only for a new Rust binding. The caller must have selected a native root
    /// identity; this API supplies no native-existence or old-family-gone proof.
    pub fn create_owned(scope: Scope) -> Result<Self, Fault> {
        let (dir, boot) = owned_directory(&scope)?;
        Self::create_at(&dir, scope, boot)
    }
    /// Never falls back to fresh state. Existing owner/custody admission occurs
    /// in swarm-process before this method can access the private directory.
    pub fn open_owned(scope: Scope) -> Result<Self, Fault> {
        let (dir, boot) = owned_directory(&scope)?;
        Self::open_at(&dir, scope, boot)
    }
    fn create_at(dir: &Path, scope: Scope, boot: String) -> Result<Self, Fault> {
        scope.validate()?;
        reject_legacy(dir)?;
        if present(&dir.join(MARKER))? || present(&dir.join(CHECKPOINT))? { return Err(Fault::StateAlreadyExists); }
        let marker = ScopeFile { format: FORMAT.into(), version: VERSION, scope: scope.clone() };
        write_private_new(&dir.join(MARKER), &serde_json::to_vec(&marker).map_err(|_| Fault::InvalidState)?)
            .map_err(|_| Fault::PersistenceUnknown)?;
        let state = State { revision: 0, durability: None, operations: BTreeMap::new(), outbox: BTreeMap::new() };
        let path = dir.join(CHECKPOINT);
        write_private_new(&path, &encode(&scope, &state)?).map_err(|_| Fault::PersistenceUnknown)?;
        Ok(Self { path, scope, boot, state, poisoned: false })
    }
    fn open_at(dir: &Path, scope: Scope, boot: String) -> Result<Self, Fault> {
        scope.validate()?;
        reject_legacy(dir)?;
        if !present(&dir.join(MARKER))? || !present(&dir.join(CHECKPOINT))? { return Err(Fault::MissingState); }
        let marker: ScopeFile = serde_json::from_slice(&read_private_durable(&dir.join(MARKER), MAX_FILE)
            .map_err(|_| Fault::PersistenceUnknown)?).map_err(|_| Fault::InvalidState)?;
        if marker.format != FORMAT || marker.version != VERSION || marker.scope != scope { return Err(Fault::ScopeMismatch); }
        let path = dir.join(CHECKPOINT);
        let checkpoint: Checkpoint = serde_json::from_slice(&read_private_durable(&path, MAX_FILE)
            .map_err(|_| Fault::PersistenceUnknown)?).map_err(|_| Fault::InvalidState)?;
        if checkpoint.format != FORMAT || checkpoint.version != VERSION || checkpoint.scope != scope { return Err(Fault::ScopeMismatch); }
        if checkpoint.sha256 != state_digest(&scope, &checkpoint.state)? { return Err(Fault::InvalidState); }
        validate_state(&scope, &checkpoint.state)?;
        Ok(Self { path, scope, boot, state: checkpoint.state, poisoned: false })
    }
    fn healthy(&self, scope: &Scope) -> Result<(), Fault> {
        if self.poisoned { return Err(Fault::Poisoned); }
        if scope != &self.scope { return Err(Fault::ScopeMismatch); }
        Ok(())
    }
    fn commit(&mut self, mut next: State) -> Result<(), Fault> {
        if self.poisoned { return Err(Fault::Poisoned); }
        next.revision = self.state.revision.checked_add(1).ok_or(Fault::Capacity)?;
        validate_state(&self.scope, &next)?;
        let bytes = encode(&self.scope, &next)?;
        if replace_private_durable(&self.path, &bytes).is_err() {
            self.poisoned = true;
            return Err(Fault::PersistenceUnknown);
        }
        self.state = next;
        Ok(())
    }
    pub fn record_durability(&mut self, scope: &Scope, profile: Durability) -> Result<(), Fault> {
        self.healthy(scope)?;
        let mut next = self.state.clone();
        next.durability = Some(DurabilityFact { profile, observed_boot: self.boot.clone() });
        self.commit(next)
    }
    pub fn prepare_native(&mut self, scope: &Scope, receipt: ModuleReceiptIdentity, invocation: NativeInvocation) -> Result<PreparedNative<'_>, Fault> {
        self.healthy(scope)?;
        scope.receipt(&receipt)?;
        invocation.validate(scope)?;
        if let Some(old) = self.state.operations.get(&receipt.operation_id) {
            if old.receipt != receipt { return Err(Fault::IntentConflict); }
            if old.native.as_ref().is_some_and(|native| native.invocation != invocation) { return Err(Fault::IntentConflict); }
            // A prior unknown outcome without native intent is a recovery gap,
            // not permission to mint missing command identity and send afresh.
            return Err(Fault::ExplicitReconcileRequired);
        }
        if invocation.method == "session/resume" && self.state.durability.as_ref().is_none_or(|fact| {
            fact.profile != Durability::Durable || fact.observed_boot != self.boot
        }) { return Err(Fault::DurabilityUnavailable); }
        let operation_id = receipt.operation_id.clone();
        let mut next = self.state.clone();
        let operation = next.operations.entry(operation_id.clone()).or_insert_with(|| Operation {
            receipt, native: None, final_delivery: None, final_host_acked: false,
        });
        operation.native = Some(NativePending { invocation, submission_boot: self.boot.clone(), ack: None, reconcile_block: None });
        self.commit(next)?;
        Ok(PreparedNative { journal: self, operation_id })
    }
    /// Records the bounded native admission ACK only; application/readback is a
    /// distinct facade decision. Replay must return the same retained ACK.
    pub fn record_native_ack(&mut self, scope: &Scope, operation_id: &str, command_id: &str, ack: Value) -> Result<(), Fault> {
        self.healthy(scope)?;
        let mut next = self.state.clone();
        let native = next.operations.get_mut(operation_id).and_then(|operation| operation.native.as_mut()).ok_or(Fault::IntentConflict)?;
        validate_ack(scope, &native.invocation, &ack)?;
        if command_id != native.invocation.command_id { return Err(Fault::IntentConflict); }
        let ack = SavedJson::new(&ack)?;
        if let Some(old) = &native.ack {
            return if old == &ack { Ok(()) } else { Err(Fault::AckConflict) };
        }
        native.ack = Some(ack);
        self.commit(next)
    }
    /// Retains a failed work/model fence without claiming no earlier effect.
    pub fn block_reconciliation(&mut self, scope: &Scope, operation_id: &str, code: &str) -> Result<(), Fault> {
        self.healthy(scope)?;
        if code.is_empty() || code.len() > 128 || !code.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_') { return Err(Fault::InvalidState); }
        let mut next = self.state.clone();
        next.operations.get_mut(operation_id).and_then(|operation| operation.native.as_mut())
            .ok_or(Fault::IntentConflict)?.reconcile_block = Some(code.into());
        self.commit(next)
    }
    /// Explicit caller request only. Reopen, outbox reads and host ACKs never
    /// call this. Fresh authorization/work fences remain mandatory at enqueue.
    pub fn explicit_reconcile(&mut self, scope: &Scope, operation_id: &str, expected_payload_sha256: &str) -> Result<Reconciliation<'_>, Fault> {
        self.healthy(scope)?;
        let operation = self.state.operations.get(operation_id).ok_or(Fault::IntentConflict)?;
        let native = operation.native.as_ref().ok_or(Fault::IntentConflict)?;
        if native.invocation.payload_sha256() != expected_payload_sha256 { return Err(Fault::IntentConflict); }
        if operation.final_delivery.is_some() {
            return Ok(if operation.final_host_acked { Reconciliation::Finalized } else { Reconciliation::AwaitingHostReceipt });
        }
        if let Some(ack) = &native.ack { return Ok(Reconciliation::AckReadback(ack.value()?)); }
        if let Some(code) = &native.reconcile_block { return Ok(Reconciliation::Blocked(code.clone())); }
        if self.state.durability.as_ref().is_none_or(|fact| {
            fact.profile != Durability::Durable || fact.observed_boot != self.boot
        }) { return Err(Fault::DurabilityUnavailable); }
        self.commit(self.state.clone())?;
        Ok(Reconciliation::Write(PreparedNative { journal: self, operation_id: operation_id.into() }))
    }
    /// Metadata only: (Operation, original submission boot, ACK present).
    /// No ACK means potentially sent, never NotStarted; ACK is not application.
    pub fn unresolved_native(&self, scope: &Scope) -> Result<Vec<(&str, &str, bool)>, Fault> {
        self.healthy(scope)?;
        Ok(self.state.operations.iter().filter_map(|(id, operation)| {
            operation.native.as_ref().filter(|_| operation.final_delivery.is_none())
                .map(|native| (id.as_str(), native.submission_boot.as_str(), native.ack.is_some()))
        }).collect())
    }
    /// Reconciliation comparison token from the retained intent, not a newly
    /// rendered payload. No token exists for a gap with no native intent.
    pub fn native_payload_sha256(&self, scope: &Scope, operation_id: &str) -> Result<&str, Fault> {
        self.healthy(scope)?;
        self.state.operations.get(operation_id).and_then(|operation| operation.native.as_ref())
            .map(|native| native.invocation.payload_sha256()).ok_or(Fault::IntentConflict)
    }
    pub fn queue_outcome(&mut self, scope: &Scope, receipt: ModuleReceiptIdentity, outcome: RuntimeOutcome) -> Result<(), Fault> {
        self.healthy(scope)?;
        scope.receipt(&receipt)?;
        if outcome.operation_id != receipt.operation_id || outcome.native_root_id.as_ref().is_some_and(|root| root != &scope.native_root_id)
            || outcome.native_scope_key.as_ref().is_some_and(|namespace| namespace != &scope.native_namespace)
            || serde_json::from_value::<ModuleReceiptIdentity>(outcome.details["module_receipt"].clone()).map_err(|_| Fault::InvalidState)? != receipt
        { return Err(Fault::ScopeMismatch); }
        let final_delivery = matches!(outcome.outcome, EffectOutcome::Applied | EffectOutcome::Rejected);
        let value = serde_json::to_value(outcome).map_err(|_| Fault::InvalidState)?;
        self.queue(receipt, Delivery { method: DeliveryMethod::Outcome, params: SavedJson::new(&value)? }, final_delivery)
    }
    /// Stores exact caller-produced result page bytes; the facade and Store
    /// validate native provenance/content. This cannot settle the source send.
    pub fn queue_result(&mut self, scope: &Scope, receipt: ModuleReceiptIdentity, page: Value) -> Result<(), Fault> {
        self.healthy(scope)?;
        scope.receipt(&receipt)?;
        if !page.is_object() || self.state.operations.get(&receipt.operation_id).is_some_and(|operation| operation.native.is_some()) {
            return Err(Fault::IntentConflict);
        }
        if page["source"]["native_session_id"] != scope.native_root_id { return Err(Fault::ScopeMismatch); }
        let params = SavedJson::new(&json!({"operation_id":receipt.operation_id,"page":page}))?;
        self.queue(receipt, Delivery { method: DeliveryMethod::Result, params }, true)
    }
    fn queue(&mut self, receipt: ModuleReceiptIdentity, delivery: Delivery, final_delivery: bool) -> Result<(), Fault> {
        let id = receipt.operation_id.clone();
        if let Some(operation) = self.state.operations.get(&id) {
            if operation.receipt != receipt { return Err(Fault::IntentConflict); }
            if let Some(old) = &operation.final_delivery {
                return if old == &delivery || !final_delivery { Ok(()) } else { Err(Fault::OutcomeConflict) };
            }
        }
        if self.state.outbox.get(&id).is_some_and(|slot| slot.delivery == delivery) { return Ok(()); }
        let mut next = self.state.clone();
        let operation = next.operations.entry(id.clone()).or_insert_with(|| Operation {
            receipt, native: None, final_delivery: None, final_host_acked: false,
        });
        if final_delivery { operation.final_delivery = Some(delivery.clone()); }
        let revision = self.state.revision.checked_add(1).ok_or(Fault::Capacity)?;
        next.outbox.insert(id, Slot { revision, delivery });
        self.commit(next)
    }
    pub fn outbox(&self, scope: &Scope) -> Result<Vec<DeliveryTicket>, Fault> {
        self.healthy(scope)?;
        Ok(self.state.outbox.iter().map(|(id, slot)| DeliveryTicket { operation_id: id.clone(), slot: slot.clone() }).collect())
    }
    /// Call only after an authenticated matching host ACK. Lost ACK means this
    /// is not called. A newer delivery is untouched by an older ACK.
    pub fn acknowledge(&mut self, scope: &Scope, ticket: &DeliveryTicket) -> Result<AckDisposition, Fault> {
        self.healthy(scope)?;
        let Some(slot) = self.state.outbox.get(&ticket.operation_id) else { return Ok(AckDisposition::AlreadyRetired); };
        if slot != &ticket.slot { return Ok(AckDisposition::Superseded); }
        let mut next = self.state.clone();
        next.outbox.remove(&ticket.operation_id);
        let operation = next.operations.get_mut(&ticket.operation_id).ok_or(Fault::InvalidState)?;
        if operation.final_delivery.as_ref() == Some(&slot.delivery) { operation.final_host_acked = true; }
        self.commit(next)?;
        Ok(AckDisposition::Retired)
    }
}

fn state_digest(scope: &Scope, state: &State) -> Result<Sha256Digest, Fault> {
    digest(&json!({"format":FORMAT,"version":VERSION,"scope":scope,"state":state}))
}
fn encode(scope: &Scope, state: &State) -> Result<Vec<u8>, Fault> {
    let bytes = serde_json::to_vec(&Checkpoint {
        format: FORMAT.into(), version: VERSION, scope: scope.clone(), state: state.clone(), sha256: state_digest(scope, state)?,
    }).map_err(|_| Fault::InvalidState)?;
    if bytes.len() as u64 > MAX_FILE { return Err(Fault::Capacity); }
    Ok(bytes)
}
fn validate_ack(scope: &Scope, invocation: &NativeInvocation, ack: &Value) -> Result<(), Fault> {
    let object = ack.as_object().ok_or(Fault::InvalidState)?;
    let lifecycle = matches!(invocation.method.as_str(), "session/start" | "session/resume");
    if lifecycle {
        if ack["session"]["sessionId"] != scope.native_root_id
            || ack["viewCursor"].as_str().is_none_or(str::is_empty)
        { return Err(Fault::InvalidState); }
        if invocation.method == "session/resume" && (ack["history"]["mode"] != "none"
            || !ack["history"]["items"].is_null() || !ack["history"]["snapshot"].is_null()
            || !ack["pendingRequests"].is_array())
        { return Err(Fault::InvalidState); }
    } else if ack["commandId"] != invocation.command_id || ack["status"] != "accepted" {
        return Err(Fault::InvalidState);
    }
    if object.keys().any(|key| !["commandId","status","turnId","approvalId","userInputId","effectiveMode","goal","session","viewCursor","history","pendingRequests"].contains(&key.as_str()))
        || object.get("commandId").is_some_and(|id| id != &invocation.command_id)
        || object.get("status").is_some_and(|status| status != "accepted")
        || object.get("session").is_some_and(|session| session["sessionId"] != scope.native_root_id)
        || object.get("history").is_some_and(|history| history["mode"] != "none" || !history["items"].is_null() || !history["snapshot"].is_null())
    { return Err(Fault::InvalidState); }
    if let Some(session) = object.get("session") {
        let session = session.as_object().ok_or(Fault::InvalidState)?;
        if session.keys().any(|key| !["sessionId","modelId","providerId","workspaceRoot","path","status","activeTurnId","turnCount","createdAt","updatedAt","forkedFrom","approvalMode","attention","branch","firstUserPrompt","lastActivityAt","name","title"].contains(&key.as_str())) { return Err(Fault::InvalidState); }
    }
    Ok(())
}
fn validate_state(scope: &Scope, state: &State) -> Result<(), Fault> {
    if state.operations.len() > MAX_OPERATIONS || state.outbox.len() > MAX_OPERATIONS { return Err(Fault::Capacity); }
    if state.durability.as_ref().is_some_and(|fact| !atom(&fact.observed_boot)) { return Err(Fault::InvalidState); }
    let mut native_ids = std::collections::HashSet::new();
    for (id, operation) in &state.operations {
        scope.receipt(&operation.receipt)?;
        if id != &operation.receipt.operation_id { return Err(Fault::InvalidState); }
        if let Some(native) = &operation.native {
            native.invocation.validate(scope)?;
            if !atom(&native.submission_boot) || !native_ids.insert(native.invocation.command_id.clone()) { return Err(Fault::IntentConflict); }
            if let Some(ack) = &native.ack { validate_ack(scope, &native.invocation, &ack.value()?)?; }
            if native.reconcile_block.as_ref().is_some_and(|code| code.is_empty() || code.len() > 128
                || !code.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_'))
            { return Err(Fault::InvalidState); }
        }
        if let Some(delivery) = &operation.final_delivery {
            validate_delivery(scope, &operation.receipt, delivery)?;
            let params = delivery.params.value()?;
            if delivery.method == DeliveryMethod::Outcome && !matches!(params["outcome"].as_str(), Some("applied" | "rejected")) { return Err(Fault::InvalidState); }
            if operation.final_host_acked == state.outbox.contains_key(id) { return Err(Fault::InvalidState); }
        }
        if operation.final_host_acked && operation.final_delivery.is_none() { return Err(Fault::InvalidState); }
    }
    for (id, slot) in &state.outbox {
        let operation = state.operations.get(id).ok_or(Fault::InvalidState)?;
        validate_delivery(scope, &operation.receipt, &slot.delivery)?;
        if slot.revision == 0 || slot.revision > state.revision || slot.delivery.params.value()?["operation_id"] != *id
            || operation.final_delivery.as_ref().is_some_and(|delivery| delivery != &slot.delivery)
        { return Err(Fault::InvalidState); }
    }
    Ok(())
}
fn validate_delivery(scope: &Scope, receipt: &ModuleReceiptIdentity, delivery: &Delivery) -> Result<(), Fault> {
    let value = delivery.params.value()?;
    if value["operation_id"] != receipt.operation_id { return Err(Fault::ScopeMismatch); }
    match delivery.method {
        DeliveryMethod::Outcome => {
            let outcome: RuntimeOutcome = serde_json::from_value(value).map_err(|_| Fault::InvalidState)?;
            if serde_json::from_value::<ModuleReceiptIdentity>(outcome.details["module_receipt"].clone()).map_err(|_| Fault::InvalidState)? != *receipt
                || outcome.native_root_id.as_ref().is_some_and(|root| root != &scope.native_root_id)
                || outcome.native_scope_key.as_ref().is_some_and(|namespace| namespace != &scope.native_namespace)
            { return Err(Fault::ScopeMismatch); }
        }
        DeliveryMethod::Result => {
            let object = value.as_object().ok_or(Fault::InvalidState)?;
            if object.len() != 2 || !value["page"].is_object() { return Err(Fault::InvalidState); }
            if value["page"]["source"]["native_session_id"] != scope.native_root_id { return Err(Fault::ScopeMismatch); }
        }
    }
    Ok(())
}
fn present(path: &Path) -> Result<bool, Fault> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(Fault::PersistenceUnknown),
    }
}
fn reject_legacy(dir: &Path) -> Result<(), Fault> {
    if present(&dir.join("checkpoint.json"))? { return Err(Fault::LegacyState); }
    Ok(())
}
fn owned_directory(scope: &Scope) -> Result<(PathBuf, String), Fault> {
    let owner = swarm_process::module_owner::verify_current_adapter_from_env().map_err(|_| Fault::OwnerUnverified)?;
    for (name, expected) in [
        ("ELIOT_SWARM_MODULE_ID", scope.module_id.as_str().to_owned()),
        ("ELIOT_SWARM_MODULE_CLIENT_ID", scope.module_client_id.clone()),
        ("ELIOT_SWARM_MODULE_ARTIFACT_ID", scope.artifact.artifact_id.as_str().to_owned()),
        ("ELIOT_SWARM_MODULE_ARTIFACT_VERSION", scope.artifact.version.as_str().to_owned()),
        ("ELIOT_SWARM_MODULE_BINDING_ID", scope.binding_id.clone()),
        ("ELIOT_SWARM_MODULE_BINDING_GENERATION", scope.generation.to_string()),
        ("ELIOT_SWARM_MODULE_PROTOCOL", "1.0".into()),
    ] {
        if std::env::var(name).ok().as_deref() != Some(expected.as_str()) { return Err(Fault::ScopeMismatch); }
    }
    if std::env::var("ELIOT_SWARM_MODULE_BUILD_ID").ok().as_deref() != scope.artifact.build_id.as_deref() { return Err(Fault::ScopeMismatch); }
    let dir = std::env::var_os("ELIOT_SWARM_MODULE_STATE").map(PathBuf::from).ok_or(Fault::OwnerUnverified)?;
    if !dir.is_absolute() || std::env::var_os("ELIOT_SWARM_MODULE_OWNER").map(PathBuf::from) != Some(dir.join("owner.json")) { return Err(Fault::OwnerUnverified); }
    Ok((dir, owner.boot_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, sync::atomic::{AtomicU64, Ordering}, time::{SystemTime, UNIX_EPOCH}};
    use swarm_contracts::module_catalog::{ArtifactId, ArtifactVersion, ProtocolVersion};
    use swarm_process::permissions::test_support::scoped_durability_barrier;

    const BOOT_A: &str = "fixture-boot-a";
    const BOOT_B: &str = "fixture-boot-b";
    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct Fixture { directory: PathBuf, scope: Scope }
    impl Fixture {
        fn new() -> Self {
            let name = format!("swarm-muse-journal-{}-{}-{}", std::process::id(),
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(), NEXT.fetch_add(1, Ordering::Relaxed));
            let directory = std::env::temp_dir().join(name);
            fs::create_dir(&directory).unwrap();
            let directory = directory.canonicalize().unwrap();
            let scope = Scope {
                module_client_id: "fixture-module-client".into(), module_id: ModuleId::new("muse").unwrap(),
                artifact: ArtifactIdentity { artifact_id: ArtifactId::new("muse-rust-fixture").unwrap(), version: ArtifactVersion::new("0.1.0").unwrap(), build_id: None },
                binding_id: "fixture-binding".into(), generation: 1,
                native_namespace: "muse:fixture:observed-namespace".into(),
                native_root_id: "019a0000-0000-7000-8000-000000000099".into(),
            };
            Self { directory, scope }
        }
        fn create(&self) -> Journal { Journal::create_at(&self.directory, self.scope.clone(), BOOT_A.into()).unwrap() }
        fn reopen(&self) -> Journal { Journal::open_at(&self.directory, self.scope.clone(), BOOT_B.into()).unwrap() }
        fn receipt(&self, operation: &str) -> ModuleReceiptIdentity {
            ModuleReceiptIdentity {
                schema_version: 1, module_id: self.scope.module_id.clone(), artifact: self.scope.artifact.clone(),
                protocol: ProtocolVersion { major: 1, minor: 0 }, binding_id: self.scope.binding_id.clone(),
                binding_generation: self.scope.generation, operation_id: operation.into(),
                input_sha256: digest(&json!({"fixture_operation":operation})).unwrap().as_str().into(),
            }
        }
        fn invocation(&self, n: u64, method: &str) -> NativeInvocation {
            let id = format!("019a0000-0000-7000-8000-{n:012x}");
            let mut params = json!({"commandId":id,"sessionId":self.scope.native_root_id});
            if method == "turn/start" { params["input"] = json!([{"type":"text","text":"exact fixture input"}]); params["ifBusy"] = json!("queue"); }
            if method == "session/resume" { params["excludeItems"] = json!(true); }
            NativeInvocation::new(&self.scope, method, &id, params.as_object().unwrap().clone()).unwrap()
        }
        fn outcome(&self, operation: &str, outcome: EffectOutcome) -> RuntimeOutcome {
            RuntimeOutcome {
                operation_id: operation.into(), outcome, native_scope_key: Some(self.scope.native_namespace.clone()),
                native_root_id: Some(self.scope.native_root_id.clone()), turn_id: None, native_input_id: None,
                details: json!({"module_receipt":self.receipt(operation),"fixture":true}),
            }
        }
        fn ack(&self, invocation: &NativeInvocation) -> Value {
            json!({"commandId":invocation.command_id(),"status":"accepted","turnId":"fixture-turn"})
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let parent = std::env::temp_dir().canonicalize().unwrap();
            assert_eq!(self.directory.parent(), Some(parent.as_path()));
            assert!(self.directory.file_name().unwrap().to_string_lossy().starts_with("swarm-muse-journal-"));
            fs::remove_dir_all(&self.directory).unwrap();
        }
    }

    #[test]
    fn muse_journal_reopen_never_resends_and_requires_exact_explicit_reconciliation() {
        let fixture = Fixture::new();
        let invocation = fixture.invocation(1, "turn/start");
        let mut journal = fixture.create();
        drop(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()).unwrap());
        drop(journal);
        let mut journal = fixture.reopen();
        assert_eq!(journal.unresolved_native(&fixture.scope).unwrap(), vec![("send", BOOT_A, false)]);
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
        assert_eq!(journal.native_payload_sha256(&fixture.scope, "send").unwrap(), invocation.payload_sha256());
        assert!(matches!(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()), Err(Fault::ExplicitReconcileRequired)));
        assert!(matches!(journal.explicit_reconcile(&fixture.scope, "send", "wrong-digest"), Err(Fault::IntentConflict)));
        assert!(matches!(journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()), Err(Fault::DurabilityUnavailable)));
        journal.record_durability(&fixture.scope, Durability::Durable).unwrap();
        match journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()).unwrap() {
            Reconciliation::Write(prepared) => assert_eq!(prepared.invocation(), &invocation),
            _ => panic!("only explicit same-command reconciliation may expose stored native payload"),
        }
        assert_eq!(journal.unresolved_native(&fixture.scope).unwrap()[0].1, BOOT_A);
    }

    #[test]
    fn muse_journal_wrong_scope_or_receipt_cannot_prepare_or_adopt_state() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        let mut scopes = Vec::new();
        let mut scope = fixture.scope.clone(); scope.generation += 1; scopes.push(scope);
        let mut scope = fixture.scope.clone(); scope.native_namespace.push_str("-foreign"); scopes.push(scope);
        let mut scope = fixture.scope.clone(); scope.native_root_id.push_str("-foreign"); scopes.push(scope);
        let mut scope = fixture.scope.clone(); scope.artifact.version = ArtifactVersion::new("0.2.0").unwrap(); scopes.push(scope);
        let mut scope = fixture.scope.clone(); scope.module_client_id.push_str("-foreign"); scopes.push(scope);
        for scope in scopes {
            assert!(matches!(Journal::open_at(&fixture.directory, scope.clone(), BOOT_B.into()), Err(Fault::ScopeMismatch)));
            assert!(matches!(journal.prepare_native(&scope, fixture.receipt("send"), invocation.clone()), Err(Fault::ScopeMismatch)));
        }
        let mut receipt = fixture.receipt("send"); receipt.binding_generation += 1;
        assert!(matches!(journal.prepare_native(&fixture.scope, receipt, invocation), Err(Fault::ScopeMismatch)));
        assert!(journal.unresolved_native(&fixture.scope).unwrap().is_empty());
    }

    #[test]
    fn muse_journal_missing_or_legacy_state_never_becomes_fresh() {
        let fixture = Fixture::new();
        fs::write(fixture.directory.join("checkpoint.json"), b"legacy sentinel; not a Rust checkpoint").unwrap();
        assert!(matches!(Journal::create_at(&fixture.directory, fixture.scope.clone(), BOOT_A.into()), Err(Fault::LegacyState)));
        assert!(!fixture.directory.join(MARKER).exists());
        assert_eq!(fs::read(fixture.directory.join("checkpoint.json")).unwrap(), b"legacy sentinel; not a Rust checkpoint");
        let fixture = Fixture::new();
        drop(fixture.create());
        fs::remove_file(fixture.directory.join(CHECKPOINT)).unwrap();
        assert!(matches!(Journal::open_at(&fixture.directory, fixture.scope.clone(), BOOT_B.into()), Err(Fault::MissingState)));
        assert!(matches!(Journal::create_at(&fixture.directory, fixture.scope.clone(), BOOT_B.into()), Err(Fault::StateAlreadyExists)));
    }

    #[test]
    fn muse_journal_failure_before_publication_prevents_mutation_and_poison_is_sticky() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let path = fixture.directory.join(CHECKPOINT);
        let saved = fs::read(&path).unwrap();
        fs::remove_file(&path).unwrap(); fs::create_dir(&path).unwrap();
        let attempted_native_write = Cell::new(false);
        if let Ok(_prepared) = journal.prepare_native(&fixture.scope, fixture.receipt("send"), fixture.invocation(1, "turn/start")) {
            attempted_native_write.set(true);
        }
        assert!(!attempted_native_write.get());
        assert!(matches!(journal.prepare_native(&fixture.scope, fixture.receipt("another"), fixture.invocation(2, "turn/start")), Err(Fault::Poisoned)));
        fs::remove_dir(&path).unwrap(); write_private_new(&path, &saved).unwrap();
        assert!(matches!(journal.prepare_native(&fixture.scope, fixture.receipt("another"), fixture.invocation(2, "turn/start")), Err(Fault::Poisoned)));
        assert!(fixture.reopen().unresolved_native(&fixture.scope).unwrap().is_empty());
    }

    #[test]
    fn muse_journal_postpublication_uncertainty_retains_unknown_prior_write() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        let failed_path = fixture.directory.join(CHECKPOINT);
        let hook = scoped_durability_barrier(move |path| {
            if path == failed_path { Err(swarm_contracts::error::Error::new("FIXTURE_SYNC_FAILURE", "fixture")) } else { Ok(()) }
        });
        assert!(matches!(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()), Err(Fault::PersistenceUnknown)));
        assert!(matches!(journal.outbox(&fixture.scope), Err(Fault::Poisoned)));
        drop(hook); drop(journal);
        let mut journal = fixture.reopen();
        assert_eq!(journal.unresolved_native(&fixture.scope).unwrap(), vec![("send", BOOT_A, false)]);
        assert!(matches!(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()), Err(Fault::ExplicitReconcileRequired)));
        journal.record_durability(&fixture.scope, Durability::Durable).unwrap();
        match journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()).unwrap() {
            Reconciliation::Write(prepared) => assert_eq!(prepared.invocation(), &invocation),
            _ => panic!("retained intent is uncertain, never invented NotStarted or completed"),
        }
    }

    #[test]
    fn muse_journal_native_ack_is_retained_before_outcome_and_never_resubmitted() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()).unwrap());
        assert_eq!(journal.record_native_ack(&fixture.scope, "send", invocation.command_id(), json!({})), Err(Fault::InvalidState));
        let mut foreign_ack = fixture.ack(&invocation);
        foreign_ack["commandId"] = json!(fixture.invocation(2, "turn/start").command_id());
        assert_eq!(journal.record_native_ack(&fixture.scope, "send", invocation.command_id(), foreign_ack), Err(Fault::InvalidState));
        let ack = fixture.ack(&invocation);
        journal.record_native_ack(&fixture.scope, "send", invocation.command_id(), ack.clone()).unwrap();
        journal.record_native_ack(&fixture.scope, "send", invocation.command_id(), ack.clone()).unwrap();
        let mut changed = ack.clone(); changed["turnId"] = json!("different-turn");
        assert_eq!(journal.record_native_ack(&fixture.scope, "send", invocation.command_id(), changed), Err(Fault::AckConflict));
        drop(journal);
        let mut journal = fixture.reopen();
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty(), "native ACK is not an invented applied outcome");
        match journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()).unwrap() {
            Reconciliation::AckReadback(saved) => assert_eq!(saved, ack),
            _ => panic!("ACK-backed command must use readback, never a setter/input resend"),
        }
    }

    #[test]
    fn muse_journal_uncertain_ack_publication_reopens_as_readback_only() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()).unwrap());
        let failed_path = fixture.directory.join(CHECKPOINT);
        let hook = scoped_durability_barrier(move |path| {
            if path == failed_path { Err(swarm_contracts::error::Error::new("FIXTURE_SYNC_FAILURE", "fixture")) } else { Ok(()) }
        });
        let ack = fixture.ack(&invocation);
        assert_eq!(journal.record_native_ack(&fixture.scope, "send", invocation.command_id(), ack.clone()), Err(Fault::PersistenceUnknown));
        assert!(matches!(journal.prepare_native(&fixture.scope, fixture.receipt("other"), fixture.invocation(2, "turn/start")), Err(Fault::Poisoned)));
        drop(hook); drop(journal);
        let mut journal = fixture.reopen();
        match journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()).unwrap() {
            Reconciliation::AckReadback(saved) => assert_eq!(saved, ack),
            _ => panic!("durably revalidated ACK must not trigger native replay"),
        }
    }

    #[test]
    fn muse_journal_lost_host_ack_retains_exact_delivery_and_unknown_native_intent() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation).unwrap());
        journal.queue_outcome(&fixture.scope, fixture.receipt("send"), fixture.outcome("send", EffectOutcome::Unknown)).unwrap();
        let sent = journal.outbox(&fixture.scope).unwrap().remove(0);
        drop(journal); // Host may have committed, but its ACK was lost.
        let mut journal = fixture.reopen();
        assert_eq!(journal.outbox(&fixture.scope).unwrap(), vec![sent.clone()]);
        journal.queue_outcome(&fixture.scope, fixture.receipt("send"), fixture.outcome("send", EffectOutcome::Unknown)).unwrap();
        assert_eq!(journal.outbox(&fixture.scope).unwrap(), vec![sent.clone()], "unchanged delivery is stable");
        assert_eq!(journal.acknowledge(&fixture.scope, &sent).unwrap(), AckDisposition::Retired);
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
        assert_eq!(journal.unresolved_native(&fixture.scope).unwrap(), vec![("send", BOOT_A, false)]);
    }

    #[test]
    fn muse_journal_stale_ack_cannot_retire_new_outcome_or_downgrade_terminal_fact() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()).unwrap());
        journal.queue_outcome(&fixture.scope, fixture.receipt("send"), fixture.outcome("send", EffectOutcome::Unknown)).unwrap();
        let old = journal.outbox(&fixture.scope).unwrap().remove(0);
        journal.queue_outcome(&fixture.scope, fixture.receipt("send"), fixture.outcome("send", EffectOutcome::Applied)).unwrap();
        let current = journal.outbox(&fixture.scope).unwrap().remove(0);
        assert_ne!(old, current);
        assert_eq!(journal.acknowledge(&fixture.scope, &old).unwrap(), AckDisposition::Superseded);
        journal.queue_outcome(&fixture.scope, fixture.receipt("send"), fixture.outcome("send", EffectOutcome::Unknown)).unwrap();
        assert_eq!(journal.outbox(&fixture.scope).unwrap(), vec![current.clone()]);
        journal.acknowledge(&fixture.scope, &current).unwrap();
        assert!(matches!(journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()).unwrap(), Reconciliation::Finalized));
        assert_eq!(journal.acknowledge(&fixture.scope, &current).unwrap(), AckDisposition::AlreadyRetired);
    }

    #[test]
    fn muse_journal_result_ack_only_retires_read_operation_and_preserves_exact_page() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        drop(journal.prepare_native(&fixture.scope, fixture.receipt("send"), fixture.invocation(1, "turn/start")).unwrap());
        let page = json!({"source":{"native_session_id":fixture.scope.native_root_id,"item_id":"item","item_revision":1},"content_base64":"ZXhhY3Q=","offset_bytes":0,"eof":true});
        let mut foreign = page.clone(); foreign["source"]["native_session_id"] = json!("foreign-root");
        assert_eq!(journal.queue_result(&fixture.scope, fixture.receipt("read"), foreign), Err(Fault::ScopeMismatch));
        journal.queue_result(&fixture.scope, fixture.receipt("read"), page.clone()).unwrap();
        let ticket = journal.outbox(&fixture.scope).unwrap().remove(0);
        assert_eq!(serde_json::from_str::<Value>(ticket.params_json()).unwrap(), json!({"operation_id":"read","page":page}));
        assert_eq!(ticket.method(), "module.result");
        drop(journal);
        let mut journal = fixture.reopen();
        assert_eq!(journal.outbox(&fixture.scope).unwrap(), vec![ticket.clone()]);
        journal.acknowledge(&fixture.scope, &ticket).unwrap();
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
        assert_eq!(journal.unresolved_native(&fixture.scope).unwrap(), vec![("send", BOOT_A, false)]);
    }

    #[test]
    fn muse_journal_unknown_without_native_intent_is_not_permission_for_fresh_input() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        journal.queue_outcome(&fixture.scope, fixture.receipt("send"), fixture.outcome("send", EffectOutcome::Unknown)).unwrap();
        assert!(matches!(journal.prepare_native(&fixture.scope, fixture.receipt("send"), fixture.invocation(1, "turn/start")), Err(Fault::ExplicitReconcileRequired)));
        assert!(matches!(journal.explicit_reconcile(&fixture.scope, "send", "invented-digest"), Err(Fault::IntentConflict)));
    }

    #[test]
    fn muse_journal_changed_intent_duplicate_id_and_wrong_payload_digest_are_rejected() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()).unwrap());
        assert!(matches!(journal.prepare_native(&fixture.scope, fixture.receipt("send"), fixture.invocation(2, "turn/start")), Err(Fault::IntentConflict)));
        assert!(matches!(journal.prepare_native(&fixture.scope, fixture.receipt("other"), invocation), Err(Fault::IntentConflict)));
        drop(journal);
        let path = fixture.directory.join(CHECKPOINT);
        let mut checkpoint: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        checkpoint["state"]["operations"]["send"]["native"]["invocation"]["params"]["utf8"] = json!("{\"changed\":true}");
        replace_private_durable(&path, &serde_json::to_vec(&checkpoint).unwrap()).unwrap();
        assert!(matches!(Journal::open_at(&fixture.directory, fixture.scope.clone(), BOOT_B.into()), Err(Fault::InvalidState)));
    }

    #[test]
    fn muse_journal_resume_requires_current_boot_durable_observation_and_keeps_original_attempt() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "session/resume");
        assert!(matches!(journal.prepare_native(&fixture.scope, fixture.receipt("recover"), invocation.clone()), Err(Fault::DurabilityUnavailable)));
        journal.record_durability(&fixture.scope, Durability::Durable).unwrap();
        drop(journal.prepare_native(&fixture.scope, fixture.receipt("recover"), invocation.clone()).unwrap());
        drop(journal);
        let mut journal = fixture.reopen();
        assert!(matches!(journal.explicit_reconcile(&fixture.scope, "recover", invocation.payload_sha256()), Err(Fault::DurabilityUnavailable)));
        journal.record_durability(&fixture.scope, Durability::Ephemeral).unwrap();
        assert!(matches!(journal.explicit_reconcile(&fixture.scope, "recover", invocation.payload_sha256()), Err(Fault::DurabilityUnavailable)));
        journal.record_durability(&fixture.scope, Durability::Durable).unwrap();
        assert!(matches!(journal.explicit_reconcile(&fixture.scope, "recover", invocation.payload_sha256()).unwrap(), Reconciliation::Write(_)));
        assert_eq!(journal.unresolved_native(&fixture.scope).unwrap()[0].1, BOOT_A);
    }

    #[test]
    fn muse_journal_fence_rejection_is_retained_without_inventing_no_effect() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()).unwrap());
        journal.block_reconciliation(&fixture.scope, "send", "NATIVE_WORK_SNAPSHOT_CHANGED").unwrap();
        drop(journal);
        let mut journal = fixture.reopen();
        match journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()).unwrap() {
            Reconciliation::Blocked(code) => assert_eq!(code, "NATIVE_WORK_SNAPSHOT_CHANGED"),
            _ => panic!("retained work fence must not become a send"),
        }
        assert_eq!(journal.unresolved_native(&fixture.scope).unwrap(), vec![("send", BOOT_A, false)]);
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
    }

    #[test]
    fn muse_journal_ephemeral_unknown_input_never_assumes_cross_connection_idempotency() {
        let fixture = Fixture::new();
        let mut journal = fixture.create();
        let invocation = fixture.invocation(1, "turn/start");
        drop(journal.prepare_native(&fixture.scope, fixture.receipt("send"), invocation.clone()).unwrap());
        journal.record_durability(&fixture.scope, Durability::Ephemeral).unwrap();
        assert!(matches!(journal.explicit_reconcile(&fixture.scope, "send", invocation.payload_sha256()), Err(Fault::DurabilityUnavailable)));
        assert_eq!(journal.unresolved_native(&fixture.scope).unwrap(), vec![("send", BOOT_A, false)]);
        assert!(journal.outbox(&fixture.scope).unwrap().is_empty());
    }

    #[test]
    fn muse_journal_provider_configuration_and_full_transcript_are_not_checkpoint_inputs() {
        let fixture = Fixture::new();
        let invocation = fixture.invocation(1, "turn/start");
        let mut params = invocation.params().unwrap(); params.insert("providerApiKey".into(), json!("fixture-only"));
        assert!(NativeInvocation::new(&fixture.scope, invocation.method(), invocation.command_id(), params).is_err());
        let mut params = fixture.invocation(2, "session/start").params().unwrap();
        params.insert("config".into(), json!({"mcpServers":{}}));
        assert!(NativeInvocation::new(&fixture.scope, "session/start", "019a0000-0000-7000-8000-000000000002", params).is_err());
        let mut ack = fixture.ack(&invocation); ack["history"] = json!({"mode":"inline","items":[{"text":"transcript"}],"snapshot":null});
        assert!(validate_ack(&fixture.scope, &invocation, &ack).is_err());
    }
}
