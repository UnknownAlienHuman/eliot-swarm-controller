from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, content: str) -> None:
    (ROOT / path).write_text(content, encoding="utf-8", newline="\n")


def replace_once(path: str, old: str, new: str) -> None:
    content = read(path)
    count = content.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one exact match, found {count}")
    write(path, content.replace(old, new, 1))


def replace_all(path: str, old: str, new: str, minimum: int = 1) -> None:
    content = read(path)
    count = content.count(old)
    if count < minimum:
        raise RuntimeError(f"{path}: expected at least {minimum} matches, found {count}")
    write(path, content.replace(old, new))


def regex_once(path: str, pattern: str, replacement: str) -> None:
    content = read(path)
    updated, count = re.subn(pattern, replacement, content, count=1, flags=re.DOTALL)
    if count != 1:
        raise RuntimeError(f"{path}: expected one regex match, found {count}")
    write(path, updated)


STORE_DELIVERY = r'''use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use swarm_contracts::{
    error::{Error, Result},
    runtime::{ModuleReceiptIdentity, NormalizedResultOriginContext},
};
use swarm_process::{
    private_permissions, read_private_durable, remove_private_acknowledged,
    replace_private_durable, sync_parent_directory, write_private_exact,
};

use crate::stream::RetainedResponseBody;

pub const OUTCOME_METHOD: &str = "module.outcome";
pub const RESULT_METHOD: &str = "module.result";
const SCHEMA_VERSION: u32 = 1;
const MAX_DELIVERY_BYTES: u64 = 2 * 1024 * 1024;
const MAX_BODY_BYTES: u64 = 2 * 1024 * 1024;
const MAX_DELIVERIES: usize = 64;
const MAX_BODIES: usize = 32;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreDelivery {
    schema_version: u32,
    method: String,
    operation_id: String,
    binding_id: String,
    binding_generation: i64,
    origin_boot_id: String,
    params_sha256: String,
    params: Value,
}

impl StoreDelivery {
    pub fn method(&self) -> &str {
        &self.method
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub fn origin_boot_id(&self) -> &str {
        &self.origin_boot_id
    }

    pub fn params(&self) -> &Value {
        &self.params
    }

    pub fn reconcile_linked(&self) -> bool {
        self.params["details"]["reconcile_operation_id"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    }

    pub fn memory_outcome(
        params: Value,
        binding_id: &str,
        binding_generation: i64,
        origin_boot_id: &str,
    ) -> Result<Self> {
        Self::new(
            OUTCOME_METHOD,
            params,
            binding_id,
            binding_generation,
            origin_boot_id,
        )
    }

    fn new(
        method: &str,
        params: Value,
        binding_id: &str,
        binding_generation: i64,
        origin_boot_id: &str,
    ) -> Result<Self> {
        if !matches!(method, OUTCOME_METHOD | RESULT_METHOD) {
            return Err(Error::invalid("unsupported Store delivery method"));
        }
        let operation_id = params
            .get("operation_id")
            .and_then(Value::as_str)
            .filter(|value| bounded_identity(value, 512))
            .ok_or_else(|| Error::invalid("Store delivery Operation ID is missing"))?
            .to_owned();
        if !bounded_identity(binding_id, 512)
            || binding_generation <= 0
            || !bounded_identity(origin_boot_id, 256)
        {
            return Err(Error::invalid("Store delivery identity is invalid"));
        }
        let params_sha256 = digest(&serde_json::to_vec(&params)?);
        let delivery = Self {
            schema_version: SCHEMA_VERSION,
            method: method.to_owned(),
            operation_id,
            binding_id: binding_id.to_owned(),
            binding_generation,
            origin_boot_id: origin_boot_id.to_owned(),
            params_sha256,
            params,
        };
        delivery.validate(binding_id, binding_generation)?;
        Ok(delivery)
    }

    fn validate(&self, binding_id: &str, binding_generation: i64) -> Result<()> {
        if self.schema_version != SCHEMA_VERSION
            || !matches!(self.method.as_str(), OUTCOME_METHOD | RESULT_METHOD)
            || !bounded_identity(&self.operation_id, 512)
            || self.params["operation_id"].as_str() != Some(self.operation_id.as_str())
            || self.binding_id != binding_id
            || self.binding_generation != binding_generation
            || !bounded_identity(&self.origin_boot_id, 256)
            || !is_sha256(&self.params_sha256)
            || self.params_sha256 != digest(&serde_json::to_vec(&self.params)?)
        {
            return Err(Error::new(
                "STORE_DELIVERY_INVALID",
                "retained Store delivery identity or bytes are invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedBodyRecord {
    schema_version: u32,
    operation_id: String,
    binding_id: String,
    binding_generation: i64,
    origin_boot_id: String,
    module_receipt: ModuleReceiptIdentity,
    native_conversation_id: String,
    result_ordinal: u64,
    response_sha256: String,
    response_bytes: u64,
    response: String,
}

impl RetainedBodyRecord {
    fn validate(&self, binding_id: &str, binding_generation: i64) -> Result<()> {
        let response_bytes = u64::try_from(self.response.len())
            .map_err(|_| Error::invalid("retained response length is out of range"))?;
        if self.schema_version != SCHEMA_VERSION
            || !bounded_identity(&self.operation_id, 512)
            || self.binding_id != binding_id
            || self.binding_generation != binding_generation
            || !bounded_identity(&self.origin_boot_id, 256)
            || !bounded_identity(&self.native_conversation_id, 512)
            || self.result_ordinal == 0
            || !is_sha256(&self.response_sha256)
            || self.response_bytes != response_bytes
            || self.response_sha256 != digest(self.response.as_bytes())
            || self.module_receipt.operation_id != self.operation_id
            || self.module_receipt.binding_id != self.binding_id
            || self.module_receipt.binding_generation != self.binding_generation
        {
            return Err(Error::new(
                "RESULT_PROVENANCE_INVALID",
                "retained response body identity or bytes are invalid",
            ));
        }
        self.module_receipt.validate().map_err(|_| {
            Error::new(
                "RESULT_PROVENANCE_INVALID",
                "retained response module receipt is invalid",
            )
        })?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct StoreDeliverySpool {
    directory: PathBuf,
    binding_id: String,
    binding_generation: i64,
    current_boot_id: String,
}

impl StoreDeliverySpool {
    pub fn open(
        state_dir: &Path,
        binding_id: &str,
        binding_generation: i64,
        current_boot_id: &str,
    ) -> Result<Self> {
        if !state_dir.is_absolute()
            || !bounded_identity(binding_id, 512)
            || binding_generation <= 0
            || !bounded_identity(current_boot_id, 256)
        {
            return Err(Error::invalid("Store delivery spool identity is invalid"));
        }
        let root = state_dir.join("antigravity-store-delivery-v1");
        ensure_private_directory(&root)?;
        let scope = digest(format!("{binding_id}\0{binding_generation}").as_bytes());
        let directory = root.join(scope);
        ensure_private_directory(&directory)?;
        let spool = Self {
            directory,
            binding_id: binding_id.to_owned(),
            binding_generation,
            current_boot_id: current_boot_id.to_owned(),
        };
        let _ = spool.load_deliveries()?;
        Ok(spool)
    }

    pub fn current_boot_id(&self) -> &str {
        &self.current_boot_id
    }

    pub fn load_deliveries(&self) -> Result<Vec<StoreDelivery>> {
        let mut deliveries = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                return Err(Error::new(
                    "STORE_DELIVERY_INVALID",
                    "delivery spool contains a non-UTF-8 entry",
                ));
            };
            if name.starts_with('.') || name.starts_with("body-") {
                continue;
            }
            if !name.starts_with("delivery-") || !name.ends_with(".json") {
                return Err(Error::new(
                    "STORE_DELIVERY_INVALID",
                    "delivery spool contains an unknown entry",
                ));
            }
            if deliveries.len() == MAX_DELIVERIES {
                return Err(Error::new(
                    "MODULE_OUTCOME_OUTBOX_FULL",
                    "retained Store delivery count reached its bound",
                ));
            }
            let delivery = self.read_delivery_path(&entry.path())?;
            if entry.path() != self.delivery_path(delivery.method(), delivery.operation_id()) {
                return Err(Error::new(
                    "STORE_DELIVERY_INVALID",
                    "retained delivery file name differs from its identity",
                ));
            }
            deliveries.push(delivery);
        }
        deliveries.sort_by(|left, right| {
            let left_rank = usize::from(left.method() != OUTCOME_METHOD);
            let right_rank = usize::from(right.method() != OUTCOME_METHOD);
            (left_rank, left.operation_id()).cmp(&(right_rank, right.operation_id()))
        });
        Ok(deliveries)
    }

    pub fn load_delivery(&self, method: &str, operation_id: &str) -> Result<Option<StoreDelivery>> {
        let path = self.delivery_path(method, operation_id);
        if !path.try_exists()? {
            return Ok(None);
        }
        self.read_delivery_path(&path).map(Some)
    }

    pub fn save_delivery(
        &self,
        method: &str,
        params: Value,
        origin_boot_id: &str,
    ) -> Result<StoreDelivery> {
        let delivery = StoreDelivery::new(
            method,
            params,
            &self.binding_id,
            self.binding_generation,
            origin_boot_id,
        )?;
        let path = self.delivery_path(method, delivery.operation_id());
        let bytes = serde_json::to_vec(&delivery)?;
        if bytes.len() as u64 > MAX_DELIVERY_BYTES {
            return Err(Error::invalid("Store delivery exceeds its byte limit"));
        }
        if path.try_exists()? {
            let existing = self.read_delivery_path(&path)?;
            if existing == delivery {
                write_private_exact(&path, &bytes, MAX_DELIVERY_BYTES)?;
                return Ok(existing);
            }
            if existing.method != delivery.method
                || existing.operation_id != delivery.operation_id
                || existing.binding_id != delivery.binding_id
                || existing.binding_generation != delivery.binding_generation
                || delivery.method != OUTCOME_METHOD
            {
                return Err(Error::conflict(
                    "retained Store delivery identity already names different bytes",
                ));
            }
            replace_private_durable(&path, &bytes)?;
        } else {
            if self.load_deliveries()?.len() == MAX_DELIVERIES {
                return Err(Error::new(
                    "MODULE_OUTCOME_OUTBOX_FULL",
                    "retained Store delivery count reached its bound",
                ));
            }
            write_private_exact(&path, &bytes, MAX_DELIVERY_BYTES)?;
        }
        Ok(delivery)
    }

    pub fn acknowledge(&self, delivery: &StoreDelivery) -> Result<bool> {
        delivery.validate(&self.binding_id, self.binding_generation)?;
        let Some(current) = self.load_delivery(delivery.method(), delivery.operation_id())? else {
            return Ok(false);
        };
        if current.params_sha256 != delivery.params_sha256 {
            return Ok(false);
        }
        let _ = remove_private_acknowledged(
            &self.delivery_path(delivery.method(), delivery.operation_id()),
        )?;
        Ok(true)
    }

    pub fn save_body(
        &self,
        operation_id: &str,
        origin_boot_id: &str,
        receipt: &ModuleReceiptIdentity,
        native_conversation_id: &str,
        result_ordinal: u64,
        body: &RetainedResponseBody,
    ) -> Result<()> {
        let response_bytes = u64::try_from(body.response_bytes)
            .map_err(|_| Error::invalid("retained response length is out of range"))?;
        let record = RetainedBodyRecord {
            schema_version: SCHEMA_VERSION,
            operation_id: operation_id.to_owned(),
            binding_id: self.binding_id.clone(),
            binding_generation: self.binding_generation,
            origin_boot_id: origin_boot_id.to_owned(),
            module_receipt: receipt.clone(),
            native_conversation_id: native_conversation_id.to_owned(),
            result_ordinal,
            response_sha256: body.response_sha256.clone(),
            response_bytes,
            response: body.response.clone(),
        };
        record.validate(&self.binding_id, self.binding_generation)?;
        let path = self.body_path(operation_id);
        let bytes = serde_json::to_vec(&record)?;
        if bytes.len() as u64 > MAX_BODY_BYTES {
            return Err(Error::new(
                "RESULT_STORAGE_PRESSURE",
                "retained response body exceeds its durable byte bound",
            ));
        }
        if path.try_exists()? {
            let existing = self.read_body_record(&path)?;
            if existing != record {
                return Err(Error::conflict(
                    "retained response Operation already names different bytes",
                ));
            }
            write_private_exact(&path, &bytes, MAX_BODY_BYTES)?;
            return Ok(());
        }
        let body_count = fs::read_dir(&self.directory)?
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("body-") && name.ends_with(".json"))
            })
            .count();
        if body_count >= MAX_BODIES {
            return Err(Error::new(
                "RESULT_STORAGE_PRESSURE",
                "retained response body count reached its bound; new work is held",
            ));
        }
        write_private_exact(&path, &bytes, MAX_BODY_BYTES)
    }

    pub fn read_body(
        &self,
        operation_id: &str,
        origin: &NormalizedResultOriginContext,
    ) -> Result<Option<RetainedResponseBody>> {
        let path = self.body_path(operation_id);
        if !path.try_exists()? {
            return Ok(None);
        }
        let record = self.read_body_record(&path)?;
        if operation_id != origin.target_operation_id
            || record.module_receipt != origin.producer.module_receipt
        {
            return Err(Error::new(
                "RESULT_PROVENANCE_INVALID",
                "retained body differs from the sealed Store origin",
            ));
        }
        Ok(Some(RetainedResponseBody {
            response: record.response,
            response_sha256: record.response_sha256,
            response_bytes: usize::try_from(record.response_bytes)
                .map_err(|_| Error::invalid("retained response length is out of range"))?,
        }))
    }

    fn read_delivery_path(&self, path: &Path) -> Result<StoreDelivery> {
        let bytes = read_private_durable(path, MAX_DELIVERY_BYTES)?;
        let delivery: StoreDelivery = serde_json::from_slice(&bytes).map_err(|_| {
            Error::new(
                "STORE_DELIVERY_INVALID",
                "retained Store delivery JSON is invalid",
            )
        })?;
        delivery.validate(&self.binding_id, self.binding_generation)?;
        Ok(delivery)
    }

    fn read_body_record(&self, path: &Path) -> Result<RetainedBodyRecord> {
        let bytes = read_private_durable(path, MAX_BODY_BYTES)?;
        let record: RetainedBodyRecord = serde_json::from_slice(&bytes).map_err(|_| {
            Error::new(
                "RESULT_PROVENANCE_INVALID",
                "retained response body JSON is invalid",
            )
        })?;
        record.validate(&self.binding_id, self.binding_generation)?;
        Ok(record)
    }

    fn delivery_path(&self, method: &str, operation_id: &str) -> PathBuf {
        let kind = if method == OUTCOME_METHOD {
            "outcome"
        } else {
            "result"
        };
        self.directory.join(format!(
            "delivery-{kind}-{}.json",
            digest(format!("{method}\0{operation_id}").as_bytes())
        ))
    }

    fn body_path(&self, operation_id: &str) -> PathBuf {
        self.directory.join(format!(
            "body-{}.json",
            digest(format!("body\0{operation_id}").as_bytes())
        ))
    }
}

pub fn outcome_origin_boot(params: &Value, current_boot_id: &str) -> String {
    [
        &params["details"]["local_execution_ref"]["bridge_boot_id"],
        &params["details"]["dispatch_admission"]["worker_boot_id"],
        &params["details"]["bridge_boot_id"],
    ]
    .into_iter()
    .find_map(Value::as_str)
    .filter(|value| bounded_identity(value, 256))
    .unwrap_or(current_boot_id)
    .to_owned()
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    match fs::create_dir(path) {
        Ok(()) => {
            private_permissions(path, true)?;
            sync_parent_directory(&path.join(".directory-entry"))?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            private_permissions(path, true)
        }
        Err(error) => Err(error.into()),
    }
}

fn bounded_identity(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= maximum
        && !value.chars().any(char::is_control)
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
'''

write("modules/antigravity-rust/src/store_delivery.rs", STORE_DELIVERY)

replace_once(
    "modules/antigravity-rust/src/lib.rs",
    '''//! All durable admission and Operation state remain in the controller Store.
//! This crate keeps only bounded same-process stream state and retry payloads.
''',
    '''//! All durable admission and Operation state remain in the controller Store.
//! The adapter retains only exact already-formed Store deliveries and selected
//! response bodies needed to survive IPC/process failure without replaying input.
''',
)
replace_once(
    "modules/antigravity-rust/src/lib.rs",
    '''pub mod stderr;
pub mod stream;
''',
    '''pub mod stderr;
pub mod store_delivery;
pub mod stream;
''',
)

replace_once(
    "modules/antigravity-rust/src/process.rs",
    '''pub struct CandidateManagedOwner {
    record: Value,
    token: String,
    boot_id: String,
    adapter_image: Value,
}
''',
    '''pub struct CandidateManagedOwner {
    record: Value,
    token: String,
    boot_id: String,
    adapter_image: Value,
    state_dir: PathBuf,
}
''',
)
replace_once(
    "modules/antigravity-rust/src/process.rs",
    '''        let proof = verify_current_adapter(&canonical_owner)?;
        Self::from_verified_worker(proof)
    }

    fn from_verified_worker(proof: VerifiedModuleWorker) -> Result<Self> {
''',
    '''        let proof = verify_current_adapter(&canonical_owner)?;
        Self::from_verified_worker(proof, canonical_state)
    }

    fn from_verified_worker(proof: VerifiedModuleWorker, state_dir: PathBuf) -> Result<Self> {
''',
)
replace_once(
    "modules/antigravity-rust/src/process.rs",
    '''            boot_id: proof.boot_id,
            adapter_image: proof.process_image_identity,
        })
''',
    '''            boot_id: proof.boot_id,
            adapter_image: proof.process_image_identity,
            state_dir,
        })
''',
)
replace_once(
    "modules/antigravity-rust/src/process.rs",
    '''            boot_id: self.boot_id.clone(),
            adapter_image: self.adapter_image.clone(),
        }
''',
    '''            boot_id: self.boot_id.clone(),
            adapter_image: self.adapter_image.clone(),
            state_dir: self.state_dir.clone(),
        }
''',
)
replace_once(
    "modules/antigravity-rust/src/process.rs",
    '''    pub fn adapter_image(&self) -> &Value {
        &self.adapter_image
    }
''',
    '''    pub fn adapter_image(&self) -> &Value {
        &self.adapter_image
    }

    pub fn state_dir(&self) -> &std::path::Path {
        &self.state_dir
    }
''',
)

replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''use crate::{
    launch::selected_model_id,
    module_receipt,
    stream::{StreamState, TerminalDisposition, Turn},
''',
    '''use crate::{
    launch::selected_model_id,
    module_receipt,
    store_delivery::{
        OUTCOME_METHOD, RESULT_METHOD, StoreDelivery, StoreDeliverySpool,
        outcome_origin_boot,
    },
    stream::{StreamState, TerminalDisposition, Turn},
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''    pending_receipts: VecDeque<serde_json::Value>,
    journal: VecDeque<serde_json::Value>,
''',
    '''    pending_receipts: VecDeque<serde_json::Value>,
    delivery_spool: Option<StoreDeliverySpool>,
    retention_failure: Option<String>,
    journal: VecDeque<serde_json::Value>,
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''            completed_turns: BTreeMap::new(),
            pending_receipts: VecDeque::new(),
            journal: VecDeque::new(),
''',
    '''            completed_turns: BTreeMap::new(),
            pending_receipts: VecDeque::new(),
            delivery_spool: None,
            retention_failure: None,
            journal: VecDeque::new(),
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''    pub fn stream(&self) -> &StreamState {
''',
    '''    pub fn with_delivery(
        boot_id: String,
        native_scope_key: String,
        binding_id: String,
        generation: i64,
        native_root_id: Option<String>,
        normalized_dispatch_enabled: bool,
        state_dir: &std::path::Path,
    ) -> Result<Self> {
        let spool = StoreDeliverySpool::open(state_dir, &binding_id, generation, &boot_id)?;
        let retained = spool.load_deliveries()?;
        let outcomes = retained
            .iter()
            .filter(|delivery| delivery.method() == OUTCOME_METHOD)
            .map(|delivery| delivery.params().clone())
            .collect::<VecDeque<_>>();
        if outcomes.len() > MAX_PENDING_RECEIPTS {
            return Err(swarm_contracts::error::Error::new(
                "MODULE_OUTCOME_OUTBOX_FULL",
                "retained module outcome count exceeds its active bound",
            ));
        }
        let mut controller = Self::new(
            boot_id,
            native_scope_key,
            binding_id,
            generation,
            native_root_id,
            normalized_dispatch_enabled,
        );
        controller.pending_receipts = outcomes;
        controller.delivery_spool = Some(spool);
        Ok(controller)
    }

    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    pub fn stream(&self) -> &StreamState {
''',
)

replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''    /// A result settles only the single in-flight prompt, for the same native
    /// conversation and an explicitly terminal status with response digest.
    pub fn settle_terminal(&mut self) -> Option<RuntimeOutcome> {
''',
    '''    /// Compatibility helper for source tests without a durable delivery root.
    pub fn settle_terminal(&mut self) -> Option<RuntimeOutcome> {
        self.settle_terminal_inner().ok().flatten()
    }

    /// Production terminal settlement. Complete retained bytes and the exact
    /// Store outcome are made durable before the pending prompt is released.
    pub fn settle_terminal_durable(&mut self) -> Result<Option<RuntimeOutcome>> {
        self.settle_terminal_inner()
    }

    fn settle_terminal_inner(&mut self) -> Result<Option<RuntimeOutcome>> {
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''        let (turn, disposition) = self
            .stream
            .terminal_for_after_foreign(&conversation_id, result_ordinal_before)?;
''',
    '''        let Some((turn, disposition)) = self
            .stream
            .terminal_for_after_foreign(&conversation_id, result_ordinal_before)
        else {
            return Ok(None);
        };
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''        if stream_generation != self.stream.stream_generation {
            return None;
        }
        if turn.response_sha256.is_none() {
            return self.pending_unknown("NATIVE_RESULT_FINGERPRINT_UNAVAILABLE");
        }
''',
    '''        if stream_generation != self.stream.stream_generation {
            return Ok(None);
        }
        if turn.response_sha256.is_none() {
            return Ok(self.pending_unknown("NATIVE_RESULT_FINGERPRINT_UNAVAILABLE"));
        }
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''        self.pending = None;
        self.pending_observation = None;
        let _ = self.record_outcome(&outcome, &identity.method, &identity.module_receipt);
        self.remember_completed_turn(&identity, &conversation_id, &turn);
        Some(outcome)
''',
    '''        if let Some(spool) = self.delivery_spool.as_ref()
            && let Ok(body) = self.stream.retained_response_for(
                turn.result_ordinal,
                &conversation_id,
                stream_generation,
            )
        {
            spool.save_body(
                &identity.operation_id,
                &self.boot_id,
                &identity.module_receipt,
                &conversation_id,
                turn.result_ordinal,
                &body,
            )?;
        }
        self.record_outcome(&outcome, &identity.method, &identity.module_receipt)?;
        self.pending = None;
        self.pending_observation = None;
        self.remember_completed_turn(&identity, &conversation_id, &turn);
        Ok(Some(outcome))
''',
)

replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''    pub fn retained_response_for_operation(
        &self,
        operation_id: &str,
        origin: &swarm_contracts::runtime::NormalizedResultOriginContext,
    ) -> std::result::Result<crate::stream::RetainedResponseBody, &'static str> {
        let completed = self
''',
    '''    pub fn retained_response_for_operation(
        &self,
        operation_id: &str,
        origin: &swarm_contracts::runtime::NormalizedResultOriginContext,
    ) -> std::result::Result<crate::stream::RetainedResponseBody, &'static str> {
        if let Some(spool) = self.delivery_spool.as_ref() {
            match spool.read_body(operation_id, origin) {
                Ok(Some(body)) => return Ok(body),
                Ok(None) => {}
                Err(_) => return Err("NATIVE_RESULT_BODY_ORIGIN_MISMATCH"),
            }
        }
        let completed = self
''',
)

replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''                if let Some(saved) = self
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
''',
    '''                if let Some(saved) = self
                    .pending_receipts
                    .iter()
                    .find(|entry| entry["operation_id"].as_str() == Some(target))
                    .cloned()
                {
                    let saved_identity = saved["details"]["module_receipt"].clone();
                    if saved_identity != serde_json::to_value(&target_receipt)? {
                        return Err(swarm_contracts::error::Error::new(
                            "MODULE_RECEIPT_TARGET_CONFLICT",
                            "pending target receipt differs from the exact Store target identity",
                        ));
                    }
                    self.link_pending_outcome_for_reconcile(target, &identity.operation_id)?;
                } else {
''',
)

replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''    pub fn pending_receipts(&self) -> Vec<serde_json::Value> {
        self.pending_receipts.iter().cloned().collect()
    }
''',
    '''    pub fn pending_receipts(&self) -> Vec<serde_json::Value> {
        self.pending_receipts.iter().cloned().collect()
    }

    pub fn pending_store_deliveries(&self) -> Result<Vec<StoreDelivery>> {
        if let Some(spool) = self.delivery_spool.as_ref() {
            return spool.load_deliveries();
        }
        self.pending_receipts
            .iter()
            .cloned()
            .map(|params| {
                StoreDelivery::memory_outcome(
                    params,
                    &self.binding_id,
                    self.generation,
                    &self.boot_id,
                )
            })
            .collect()
    }

    pub fn retain_result_delivery(&self, params: Value) -> Result<StoreDelivery> {
        if let Some(spool) = self.delivery_spool.as_ref() {
            return spool.save_delivery(RESULT_METHOD, params, &self.boot_id);
        }
        StoreDelivery::new_for_current(
            RESULT_METHOD,
            params,
            &self.binding_id,
            self.generation,
            &self.boot_id,
        )
    }

    pub fn acknowledge_store_delivery(&mut self, delivery: &StoreDelivery) -> Result<()> {
        if let Some(spool) = self.delivery_spool.as_ref() {
            let _ = spool.acknowledge(delivery)?;
        }
        if delivery.method() == OUTCOME_METHOD {
            self.pending_receipts.retain(|value| {
                value["operation_id"].as_str() != Some(delivery.operation_id())
                    || value != delivery.params()
            });
        }
        Ok(())
    }

    pub fn discard_store_delivery(&mut self, delivery: &StoreDelivery) -> Result<()> {
        self.acknowledge_store_delivery(delivery)
    }

    pub fn ensure_delivery_healthy(&self) -> Result<()> {
        if let Some(code) = self.retention_failure.as_deref() {
            return Err(swarm_contracts::error::Error::new(
                "MODULE_DELIVERY_RETENTION_FAILED",
                format!("exact Store delivery retention failed: {code}"),
            ));
        }
        Ok(())
    }

    fn link_pending_outcome_for_reconcile(
        &mut self,
        target_operation_id: &str,
        reconcile_operation_id: &str,
    ) -> Result<()> {
        let Some(index) = self.pending_receipts.iter().position(|value| {
            value["operation_id"].as_str() == Some(target_operation_id)
        }) else {
            return Err(swarm_contracts::error::Error::new(
                "NATIVE_HISTORY_UNAVAILABLE",
                "retained target outcome is unavailable",
            ));
        };
        let mut params = self.pending_receipts[index].clone();
        params["details"]["reconcile_operation_id"] = json!(reconcile_operation_id);
        if let Some(spool) = self.delivery_spool.as_ref() {
            let existing = spool
                .load_delivery(OUTCOME_METHOD, target_operation_id)?
                .ok_or_else(|| {
                    swarm_contracts::error::Error::new(
                        "NATIVE_HISTORY_UNAVAILABLE",
                        "durable target outcome is unavailable",
                    )
                })?;
            spool.save_delivery(OUTCOME_METHOD, params.clone(), existing.origin_boot_id())?;
        }
        self.pending_receipts[index] = params;
        Ok(())
    }
''',
)

# Add public constructor needed by Controller for memory-only result delivery.
replace_once(
    "modules/antigravity-rust/src/store_delivery.rs",
    '''    pub fn memory_outcome(
''',
    '''    pub fn new_for_current(
        method: &str,
        params: Value,
        binding_id: &str,
        binding_generation: i64,
        origin_boot_id: &str,
    ) -> Result<Self> {
        Self::new(
            method,
            params,
            binding_id,
            binding_generation,
            origin_boot_id,
        )
    }

    pub fn memory_outcome(
''',
)

replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''    pub fn acknowledge_outcome(&mut self, operation_id: &str) {
        self.pending_receipts
            .retain(|value| value["operation_id"].as_str() != Some(operation_id));
    }
''',
    '''    pub fn acknowledge_outcome(&mut self, operation_id: &str) {
        self.pending_receipts
            .retain(|value| value["operation_id"].as_str() != Some(operation_id));
    }
''',
)

replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''        let value = module_receipt::serialize_outcome(outcome, receipt).map_err(|_| {
''',
    '''        let value = module_receipt::serialize_outcome(outcome, receipt).map_err(|_| {
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''        self.pending_receipts
            .retain(|entry| entry["operation_id"].as_str() != Some(outcome.operation_id.as_str()));
        if self.pending_receipts.len() == MAX_PENDING_RECEIPTS {
''',
    '''        let replacing = self
            .pending_receipts
            .iter()
            .any(|entry| entry["operation_id"].as_str() == Some(outcome.operation_id.as_str()));
        if !replacing && self.pending_receipts.len() == MAX_PENDING_RECEIPTS {
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''        self.pending_receipts.push_back(value);
        let summary = json!({
''',
    '''        if let Some(spool) = self.delivery_spool.as_ref() {
            let origin_boot = outcome_origin_boot(&value, &self.boot_id);
            spool.save_delivery(OUTCOME_METHOD, value.clone(), &origin_boot)?;
        }
        self.pending_receipts
            .retain(|entry| entry["operation_id"].as_str() != Some(outcome.operation_id.as_str()));
        self.pending_receipts.push_back(value);
        let summary = json!({
''',
)

# All formerly ignored retention errors become sticky admission failures.
replace_all(
    "modules/antigravity-rust/src/controller.rs",
    '''        let _ = self.record_outcome(&outcome, "agent.open", &identity.module_receipt);
''',
    '''        self.record_outcome_or_mark(&outcome, "agent.open", &identity.module_receipt);
''',
    minimum=2,
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''        let _ = self.record_outcome(&outcome, &identity.method, &identity.module_receipt);
        outcome
''',
    '''        self.record_outcome_or_mark(&outcome, &identity.method, &identity.module_receipt);
        outcome
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''        let _ = self.record_outcome(
            &outcome,
            &pending.identity.method,
            &pending.identity.module_receipt,
        );
''',
    '''        self.record_outcome_or_mark(
            &outcome,
            &pending.identity.method,
            &pending.identity.module_receipt,
        );
''',
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    '''    fn native_scope_key_if_root_known(&self) -> Option<String> {
''',
    '''    fn record_outcome_or_mark(
        &mut self,
        outcome: &RuntimeOutcome,
        method: &str,
        receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    ) {
        if let Err(error) = self.record_outcome(outcome, method, receipt) {
            self.retention_failure = Some(error.code);
        }
    }

    fn native_scope_key_if_root_known(&self) -> Option<String> {
''',
)

# Main production wiring.
replace_once(
    "modules/antigravity-rust/src/main.rs",
    '''    result_page,
    stderr::{self, StderrSummary},
''',
    '''    result_page,
    stderr::{self, StderrSummary},
    store_delivery::{OUTCOME_METHOD, RESULT_METHOD},
''',
)
replace_once(
    "modules/antigravity-rust/src/main.rs",
    '''    let boot_id = owner.boot_id().to_owned();
    let native_scope_key = native_scope_key()?;

    let mut host = HostSession::new(config, credential, owner, boot_id, native_scope_key);
    let initial_hello = host.connect_and_hello(None, false).await?;
    let mut controller = controller_from_hello(&host, &initial_hello)?;
''',
    '''    let boot_id = owner.boot_id().to_owned();
    let delivery_state_dir = owner.state_dir().to_path_buf();
    let native_scope_key = native_scope_key()?;

    let mut host = HostSession::new(config, credential, owner, boot_id, native_scope_key);
    let initial_hello = host.connect_and_hello(None, false).await?;
    let mut controller = controller_from_hello(&host, &initial_hello, &delivery_state_dir)?;
''',
)
replace_once(
    "modules/antigravity-rust/src/main.rs",
    '''fn controller_from_hello(host: &HostSession, hello: &VerifiedModuleHello) -> Result<Controller> {
''',
    '''fn controller_from_hello(
    host: &HostSession,
    hello: &VerifiedModuleHello,
    delivery_state_dir: &std::path::Path,
) -> Result<Controller> {
''',
)
replace_once(
    "modules/antigravity-rust/src/main.rs",
    '''    Ok(Controller::new(
        host.boot_id.clone(),
        host.native_scope_key.clone(),
        binding_id.to_owned(),
        generation,
        native_root_id,
        normalized_dispatch_enabled,
    ))
''',
    '''    Controller::with_delivery(
        host.boot_id.clone(),
        host.native_scope_key.clone(),
        binding_id.to_owned(),
        generation,
        native_root_id,
        normalized_dispatch_enabled,
        delivery_state_dir,
    )
''',
)

replace_once(
    "modules/antigravity-rust/src/main.rs",
    '''                    Ok(params) => {
                        let response = match host
                            .request_saved("module.result", params, &controller, native_live)
                            .await
                        {
''',
    '''                    Ok(params) => {
                        let delivery = controller.retain_result_delivery(params)?;
                        let response = match host
                            .request_saved(
                                delivery.method(),
                                delivery.params().clone(),
                                &controller,
                                native_live,
                            )
                            .await
                        {
''',
)
replace_once(
    "modules/antigravity-rust/src/main.rs",
    '''                                controller.reject_command(&command, diagnostic_code)?;
                                dirty = true;
''',
    '''                                controller.discard_store_delivery(&delivery)?;
                                controller.reject_command(&command, diagnostic_code)?;
                                dirty = true;
''',
)
replace_once(
    "modules/antigravity-rust/src/main.rs",
    '''                        if response["recorded"] != true {
                            controller.reject_command(&command, "MODULE_RESULT_ACK_INVALID")?;
''',
    '''                        if response["recorded"] == true {
                            controller.acknowledge_store_delivery(&delivery)?;
                        } else {
                            controller.reject_command(&command, "MODULE_RESULT_ACK_INVALID")?;
''',
)

# Use durable terminal settlement in production paths.
replace_all(
    "modules/antigravity-rust/src/main.rs",
    '''let _ = controller.settle_terminal();''',
    '''let _ = controller.settle_terminal_durable()?;''',
    minimum=2,
)
replace_once(
    "modules/antigravity-rust/src/main.rs",
    '''                    if controller.settle_terminal().is_some() {
''',
    '''                    if controller.settle_terminal_durable()?.is_some() {
''',
)

# Replace outcome-only flush with exact durable outcome/result draining.
regex_once(
    "modules/antigravity-rust/src/main.rs",
    r'''async fn flush_reports\(\n    host: &mut HostSession,\n    controller: &mut Controller,\n    dirty: &mut bool,\n    native_live: bool,\n\) -> Result<\(\)> \{\n    for value in controller\.pending_receipts\(\) \{.*?\n    \}\n\n    if \*dirty \{''',
    '''async fn flush_reports(
    host: &mut HostSession,
    controller: &mut Controller,
    dirty: &mut bool,
    native_live: bool,
) -> Result<()> {
    controller.ensure_delivery_healthy()?;
    let mut deferred_cross_boot_outcome = false;
    for delivery in controller.pending_store_deliveries()? {
        if delivery.method() == RESULT_METHOD && deferred_cross_boot_outcome {
            continue;
        }
        let response = match host
            .request_saved(
                delivery.method(),
                delivery.params().clone(),
                controller,
                native_live,
            )
            .await
        {
            Ok(response) => response,
            Err(error)
                if delivery.method() == OUTCOME_METHOD
                    && error.code == "NATIVE_IDENTITY_MISMATCH"
                    && delivery.origin_boot_id() != controller.boot_id()
                    && !delivery.reconcile_linked() =>
            {
                // The exact prior-boot bytes may already have committed; Store
                // replay ACK would have succeeded above. Otherwise retain them
                // until an admitted agent.reconcile links this original
                // Operation to the proven owner transition.
                deferred_cross_boot_outcome = true;
                continue;
            }
            Err(error) => return Err(error),
        };
        let acknowledged = match delivery.method() {
            OUTCOME_METHOD => response["recorded"] == true || response["superseded"] == true,
            RESULT_METHOD => response["recorded"] == true,
            _ => false,
        };
        if !acknowledged {
            return Err(Error::new(
                if delivery.method() == OUTCOME_METHOD {
                    "MODULE_OUTCOME_ACK_INVALID"
                } else {
                    "MODULE_RESULT_ACK_INVALID"
                },
                "manager did not acknowledge the exact saved Store delivery",
            ));
        }
        controller.acknowledge_store_delivery(&delivery)?;
        if delivery.method() == OUTCOME_METHOD {
            *dirty = true;
        }
    }

    if *dirty {''',
)

# Store permits a prior boot only after the exact reconcile link was validated.
replace_once(
    "crates/swarm-kernel-host/src/runtime/warm_stream.rs",
    '''pub struct OutcomeContext<'a> {
    pub binding: &'a Value,
    pub operation: &'a Value,
    pub outcome: &'a RuntimeOutcome,
}
''',
    '''pub struct OutcomeContext<'a> {
    pub binding: &'a Value,
    pub operation: &'a Value,
    pub outcome: &'a RuntimeOutcome,
    /// Supplied only after Store validates an exact admitted reconcile link and
    /// the binding's proven previous-owner departure transition.
    pub allowed_origin_boot_id: Option<&'a str>,
}
''',
)
replace_once(
    "crates/swarm-kernel-host/src/runtime/warm_stream.rs",
    '''    let OutcomeContext {
        binding,
        operation,
        outcome,
    } = context;
''',
    '''    let OutcomeContext {
        binding,
        operation,
        outcome,
        allowed_origin_boot_id,
    } = context;
''',
)
replace_once(
    "crates/swarm-kernel-host/src/runtime/warm_stream.rs",
    '''    if outcome.details["completion_condition"] != COMPLETION_CONDITION
        || receipt["input_operation_id"] != operation_id
        || receipt["native_conversation_id"] != native_conversation_id
        || receipt["bridge_boot_id"] != active_boot_id
''',
    '''    let origin_boot_id = required_text(&receipt["bridge_boot_id"], "origin bridge boot_id")?;
    let accepted_boot = origin_boot_id == active_boot_id
        || allowed_origin_boot_id.is_some_and(|allowed| allowed == origin_boot_id);
    if outcome.details["completion_condition"] != COMPLETION_CONDITION
        || receipt["input_operation_id"] != operation_id
        || receipt["native_conversation_id"] != native_conversation_id
        || !accepted_boot
''',
)
replace_once(
    "crates/swarm-kernel-host/src/runtime/warm_stream.rs",
    '''            "bridge_boot_id":active_boot_id,
''',
    '''            "bridge_boot_id":origin_boot_id,
''',
)
replace_all(
    "crates/swarm-kernel-host/src/runtime/warm_stream.rs",
    '''            outcome: &outcome,
        })''',
    '''            outcome: &outcome,
            allowed_origin_boot_id: None,
        })''',
    minimum=6,
)

# Runtime passes previous boot only after exact recovery-link validation.
replace_once(
    "crates/swarm-kernel-host/src/store/runtime.rs",
    '''    validate_registered_module_recovery_link(&tx, &id, generation, &b, &o, &r)?;

    if crate::runtime::batch::is_legacy_command_route(&b["route"]) {
''',
    '''    validate_registered_module_recovery_link(&tx, &id, generation, &b, &o, &r)?;
    let recovered_delivery_origin_boot = if r.details["reconcile_operation_id"]
        .as_str()
        .is_some_and(|value| !value.is_empty())
        && b["state"] == "reconciling"
        && b["observation"]["recovery_required"] == true
    {
        let previous = b["observation"]["previous_bridge_boot_id"].as_str();
        let current = b["observation"]["bridge_boot_id"].as_str();
        previous.filter(|previous| current.is_some_and(|current| current != *previous))
    } else {
        None
    };

    if crate::runtime::batch::is_legacy_command_route(&b["route"]) {
''',
)
replace_once(
    "crates/swarm-kernel-host/src/store/runtime.rs",
    '''                binding: &b,
                operation: &o,
                outcome: &r,
            },
''',
    '''                binding: &b,
                operation: &o,
                outcome: &r,
                allowed_origin_boot_id: recovered_delivery_origin_boot,
            },
''',
)

# Test one exact cross-boot allowance in the warm-stream validator.
replace_once(
    "crates/swarm-kernel-host/src/runtime/warm_stream.rs",
    '''    fn legacy_observation_field_is_ignored_during_finite_transition() {
''',
    '''    fn prior_boot_requires_store_validated_reconcile_origin() {
        let (mut binding, operation, mut outcome) =
            fixture("SUCCESS", EffectOutcome::Applied);
        binding["observation"]["bridge_boot_id"] = json!("boot-2");
        binding["observation"]["previous_bridge_boot_id"] = json!("boot-1");
        binding["observation"]["recovery_required"] = json!(true);
        binding["state"] = json!("reconciling");
        outcome.details["reconcile_operation_id"] = json!("reconcile-1");
        let producer = validate_outcome(OutcomeContext {
            binding: &binding,
            operation: &operation,
            outcome: &outcome,
            allowed_origin_boot_id: Some("boot-1"),
        })
        .unwrap()
        .unwrap();
        assert_eq!(producer["local_execution_ref"]["bridge_boot_id"], "boot-1");

        assert!(
            validate_outcome(OutcomeContext {
                binding: &binding,
                operation: &operation,
                outcome: &outcome,
                allowed_origin_boot_id: None,
            })
            .is_err()
        );
    }

    #[test]
    fn legacy_observation_field_is_ignored_during_finite_transition() {
''',
)

# Update active documentation and registry as an A2 partial slice.
replace_once(
    "docs/agent-operations/audit-current.md",
    '''- **Next exact chain — implementation A2:** adapter
  `Controller::{record_outcome,pending_receipts,completed_turns}` and normalized result
  pages → `HostSession::request_saved/flush_reports` → exact `module.outcome` /
  `module.result` ACK and replay → private atomic retained outcome/body/page files →
  verified prior-owner departure and successor delivery for the original Operation.
  Preserve origin boot and source identity; never put native prompt intent in the
  delivery spool or replay input.
- Runtime implementation order is now **A2–A3, then B–V**.
''',
    '''- **Current implementation slice — A2a:** exact `module.outcome`, selected response
  body and `module.result` params are retained in private atomic module-state files
  before IPC. Startup reloads deliveries; exact ACK removes only matching bytes. A
  prior-boot outcome may settle only after Store validates its exact admitted
  `agent.reconcile` link and proven previous-owner transition. Native prompt intent is
  never stored or replayed.
- **Next exact chain — A2b:** Store-authoritative aggregate page coverage/next-missing
  ACK, safe local body release, pressure/readback behavior and connected restart tests
  across crash-before-write, crash-after-write, ACK loss and several replacements.
- Runtime implementation order is now **A2b–A3, then B–V**.
''',
)
replace_once(
    "docs/agent-operations/implementation.md",
    '''**A2:** retain complete terminal bytes and final outcome/result parameters with
Operation, original binding/generation and origin boot in private atomic files before
acknowledging local retention. Same-boot IPC reconnect resends unchanged parameters;
delete only after Store ACK. Observations use one replaceable checkpoint per source.
''',
    '''**A2a — implemented in this branch, qualification pending:** retain exact
`module.outcome`, selected terminal body and `module.result` parameters with Operation,
original binding/generation and origin boot in private atomic module-state files before
IPC. Startup reloads the bounded delivery set. Exact ACK removes only matching bytes;
a newer outcome under the same Operation is not deleted by an older ACK. A prior-boot
outcome can settle only through an exact admitted `agent.reconcile` after Store verifies
`previous_bridge_boot_id -> bridge_boot_id` and prior-owner departure. Native prompt
intent is never persisted or replayed.

**A2b:** make Store ACK own aggregate result-page coverage/next missing range, release
local body only after complete coverage, and close pressure/crash/ACK-loss/replacement
cases with connected tests. Observations use one replaceable checkpoint per source.
''',
)
replace_once(
    "docs/implementation-status.md",
    '''A2   NEXT / NOT IMPLEMENTED   durable outcome/body/page delivery and successor completion
A3   NOT IMPLEMENTED          real typed actions and one active route
''',
    '''A2a  IMPLEMENTED / CHECKING    private outcome/body/page retention + reconciled old boot
A2b  NEXT / NOT IMPLEMENTED   Store coverage ACK, source release and crash matrix
A3   NOT IMPLEMENTED          real typed actions and one active route
''',
)
replace_once(
    "docs/implementation-status.md",
    '''**Runtime A2:** retain complete Antigravity outcome/body/page Store-delivery parameters
in private atomic files before treating them as locally retained. Same-boot reconnect
may resend identical bytes; after verified prior-owner departure, an authorized
successor may deliver the original Operation with immutable origin boot/source identity.
Never persist or replay native prompt intent. Missing/partial capture remains visible
unknown and must not complete a newer Attempt.
''',
    '''**Runtime A2b:** complete Store-authoritative aggregate result coverage and source
release, then close the crash/ACK-loss/replacement matrix around the A2a private
outcome/body/page retention path. Preserve immutable origin boot/source identity and
never persist or replay native prompt intent. Missing/partial capture remains visible
unknown and must not complete a newer Attempt.
''',
)
replace_once(
    "modules/antigravity-rust/README.md",
    '''Installed/native qualification remains pending.
''',
    '''A2a adds bounded private exact Store-delivery and selected-body retention under the
module state directory. It stores no prompt intent. A prior-boot outcome remains held
until exact Store reconciliation proves the owner transition; normal IPC reconnect
retries identical bytes. Store-authoritative aggregate page coverage and local source
release remain A2b. Installed/native qualification remains pending.
''',
)

# Temporary inspection material must not survive the code commit.
for relative in [
    "tools/ci/a2-inspect-output.txt",
]:
    path = ROOT / relative
    if path.exists():
        path.unlink()

# Guard the intended no-prompt persistence boundary.
for path in [
    "modules/antigravity-rust/src/store_delivery.rs",
    "modules/antigravity-rust/src/controller.rs",
]:
    content = read(path)
    if "native_payload" in content or "task_prompt" in read("modules/antigravity-rust/src/store_delivery.rs"):
        raise RuntimeError("A2 delivery spool must not persist native prompt intent")

print("Antigravity A2a patch applied")
