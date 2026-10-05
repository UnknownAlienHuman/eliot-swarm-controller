//! Small durable adapter receipt journal. It is scoped below the existing
//! module owner directory and never stores prompts, credentials, or SDK text.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
};
use swarm_contracts::{
    error::{Error, Result},
    runtime::{ModuleReceiptIdentity, RuntimeOutcome},
};
use swarm_process::{private_permissions, write_private_new};

const MAX_RECORD_BYTES: usize = 1_048_576;
const MAX_STATE_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct StateIdentity {
    version: u32,
    binding_id: String,
    generation: i64,
    native_scope_key: String,
    route_sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalRecord {
    version: u32,
    operation_id: String,
    kind: String,
    #[serde(default)]
    receipt: Option<ModuleReceiptIdentity>,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    intent: Option<Value>,
    #[serde(default)]
    outcome: Option<Value>,
    #[serde(default)]
    digest: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct OperationState {
    pub operation_id: String,
    pub receipt: Option<ModuleReceiptIdentity>,
    pub method: Option<String>,
    pub intent: Option<Value>,
    pub outcome: Option<Value>,
    pub outcome_sha256: Option<String>,
    pub acknowledged_sha256: Option<String>,
}

pub struct OperationJournal {
    root: PathBuf,
}

impl OperationJournal {
    pub fn open(
        root: &Path,
        binding_id: &str,
        generation: i64,
        native_scope_key: &str,
        route_sha256: &str,
    ) -> Result<Self> {
        if !root.is_absolute() {
            return Err(Error::new(
                "ADAPTER_STATE",
                "state directory must be absolute",
            ));
        }
        fs::create_dir_all(root)
            .map_err(|_| Error::new("ADAPTER_STATE", "state directory cannot be created"))?;
        ensure_directory(root)?;
        private_permissions(root, true)?;
        let expected = StateIdentity {
            version: 1,
            binding_id: binding_id.to_owned(),
            generation,
            native_scope_key: native_scope_key.to_owned(),
            route_sha256: route_sha256.to_owned(),
        };
        let identity_path = root.join("state-identity.json");
        if identity_path.exists() {
            ensure_regular(&identity_path)?;
            let bytes = read_limited(&identity_path, MAX_RECORD_BYTES)?;
            let saved: StateIdentity = serde_json::from_slice(&bytes)
                .map_err(|_| Error::new("ADAPTER_STATE", "state identity is invalid"))?;
            if saved != expected {
                return Err(Error::new(
                    "ADAPTER_STATE_IDENTITY_MISMATCH",
                    "state directory belongs to another binding generation or native scope",
                ));
            }
        } else {
            let bytes = serde_json::to_vec(&expected)?;
            write_private_new(&identity_path, &bytes)
                .map_err(|_| Error::new("ADAPTER_STATE", "state identity cannot be created"))?;
        }
        Ok(Self {
            root: root.to_owned(),
        })
    }

    pub fn get(&self, operation_id: &str) -> Result<Option<OperationState>> {
        let path = self.path(operation_id)?;
        if !path.exists() {
            return Ok(None);
        }
        ensure_regular(&path)?;
        let file = File::open(&path)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be read"))?;
        let mut reader = BufReader::new(file);
        let mut state = OperationState {
            operation_id: operation_id.to_owned(),
            ..OperationState::default()
        };
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = reader
                .read_until(b'\n', &mut line)
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record read failed"))?;
            if read == 0 {
                break;
            }
            if line.len() > MAX_RECORD_BYTES || line.last() != Some(&b'\n') {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation record exceeds its line boundary",
                ));
            }
            line.pop();
            let record: JournalRecord = serde_json::from_slice(&line)
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record is invalid"))?;
            if record.version != 1 || record.operation_id != operation_id {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation record identity changed",
                ));
            }
            match record.kind.as_str() {
                "intent" => {
                    let receipt = record.receipt.ok_or_else(|| {
                        Error::new("ADAPTER_JOURNAL", "intent receipt is missing")
                    })?;
                    let method = record
                        .method
                        .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "intent method is missing"))?;
                    let intent = record
                        .intent
                        .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "intent marker is missing"))?;
                    if receipt.operation_id != operation_id
                        || state.receipt.as_ref().is_some_and(|old| old != &receipt)
                        || state.method.as_ref().is_some_and(|old| old != &method)
                        || state.intent.as_ref().is_some_and(|old| old != &intent)
                    {
                        return Err(Error::new(
                            "ADAPTER_JOURNAL",
                            "operation intent conflicts with its saved identity",
                        ));
                    }
                    receipt
                        .validate()
                        .map_err(|_| Error::new("ADAPTER_JOURNAL", "intent receipt is invalid"))?;
                    state.receipt = Some(receipt);
                    state.method = Some(method);
                    state.intent = Some(intent);
                }
                "outcome" => {
                    let value = record
                        .outcome
                        .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "saved outcome is missing"))?;
                    if state.receipt.is_none() || value["operation_id"] != operation_id {
                        return Err(Error::new(
                            "ADAPTER_JOURNAL",
                            "outcome lacks its exact saved intent",
                        ));
                    }
                    let digest = digest_json(&value)?;
                    if record.digest.as_deref() != Some(digest.as_str())
                        || state.outcome.as_ref().is_some_and(|old| old != &value)
                    {
                        return Err(Error::new(
                            "ADAPTER_JOURNAL",
                            "saved outcome digest or identity changed",
                        ));
                    }
                    state.outcome = Some(value);
                    state.outcome_sha256 = Some(digest);
                }
                "acknowledged" => {
                    let digest = record.digest.ok_or_else(|| {
                        Error::new("ADAPTER_JOURNAL", "acknowledgement digest is missing")
                    })?;
                    if state.outcome_sha256.as_deref() != Some(digest.as_str()) {
                        return Err(Error::new(
                            "ADAPTER_JOURNAL",
                            "acknowledgement differs from the exact saved outcome",
                        ));
                    }
                    state.acknowledged_sha256 = Some(digest);
                }
                _ => {
                    return Err(Error::new(
                        "ADAPTER_JOURNAL",
                        "unknown operation record kind",
                    ));
                }
            }
        }
        if state.receipt.is_none() {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation record has no durable intent",
            ));
        }
        Ok(Some(state))
    }

    pub fn write_intent(
        &self,
        operation_id: &str,
        receipt: &ModuleReceiptIdentity,
        method: &str,
        intent: &Value,
    ) -> Result<()> {
        if self.get(operation_id)?.is_some() {
            return Err(Error::new(
                "ADAPTER_INTENT_EXISTS",
                "operation already has a durable effect marker",
            ));
        }
        let record = JournalRecord {
            version: 1,
            operation_id: operation_id.to_owned(),
            kind: "intent".to_owned(),
            receipt: Some(receipt.clone()),
            method: Some(method.to_owned()),
            intent: Some(intent.clone()),
            outcome: None,
            digest: None,
        };
        self.append(operation_id, &record)
    }

    pub fn save_outcome(&self, operation_id: &str, outcome: &RuntimeOutcome) -> Result<()> {
        self.save_outcome_value(operation_id, &serde_json::to_value(outcome)?)
    }

    pub fn save_outcome_value(&self, operation_id: &str, outcome: &Value) -> Result<()> {
        let current = self.get(operation_id)?.ok_or_else(|| {
            Error::new(
                "ADAPTER_INTENT_MISSING",
                "outcome has no durable effect marker",
            )
        })?;
        if outcome["operation_id"] != operation_id {
            return Err(Error::new(
                "ADAPTER_OUTCOME",
                "outcome names another operation",
            ));
        }
        if let Some(saved) = current.outcome {
            if saved == *outcome {
                return Ok(());
            }
            return Err(Error::new(
                "ADAPTER_OUTCOME_CONFLICT",
                "operation outcome cannot be replaced",
            ));
        }
        let digest = digest_json(outcome)?;
        let record = JournalRecord {
            version: 1,
            operation_id: operation_id.to_owned(),
            kind: "outcome".to_owned(),
            receipt: None,
            method: None,
            intent: None,
            outcome: Some(outcome.clone()),
            digest: Some(digest),
        };
        self.append(operation_id, &record)
    }

    pub fn acknowledge(&self, operation_id: &str, outcome: &Value) -> Result<()> {
        let current = self
            .get(operation_id)?
            .ok_or_else(|| Error::new("ADAPTER_OUTBOX", "acknowledged operation is missing"))?;
        let digest = digest_json(outcome)?;
        if current.outcome.as_ref() != Some(outcome)
            || current.outcome_sha256.as_deref() != Some(digest.as_str())
        {
            return Err(Error::new(
                "ADAPTER_OUTBOX",
                "host acknowledgement differs from the exact saved outcome",
            ));
        }
        if current.acknowledged_sha256.as_deref() == Some(digest.as_str()) {
            return Ok(());
        }
        let record = JournalRecord {
            version: 1,
            operation_id: operation_id.to_owned(),
            kind: "acknowledged".to_owned(),
            receipt: None,
            method: None,
            intent: None,
            outcome: None,
            digest: Some(digest),
        };
        self.append(operation_id, &record)
    }

    pub fn pending_outcomes(&self) -> Result<Vec<OperationState>> {
        let mut pending = Vec::new();
        for entry in fs::read_dir(&self.root)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be listed"))?
        {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
                continue;
            }
            ensure_regular(&path)?;
            let name = path
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "operation filename is invalid"))?;
            let state = self.read_path(&path)?;
            if hex_digest(state.operation_id.as_bytes()) != name {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation filename does not match its identity",
                ));
            }
            if state.outcome.is_some() && state.acknowledged_sha256 != state.outcome_sha256 {
                pending.push(state);
            }
        }
        pending.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
        Ok(pending)
    }

    pub fn unresolved(&self) -> Result<Vec<OperationState>> {
        let mut pending = Vec::new();
        for entry in fs::read_dir(&self.root)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be listed"))?
        {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
                continue;
            }
            ensure_regular(&path)?;
            let state = self.read_path(&path)?;
            if state.outcome.is_none() {
                pending.push(state);
            }
        }
        Ok(pending)
    }

    pub fn has_task_dispatch_for_boot(&self, boot_id: &str) -> Result<bool> {
        for entry in fs::read_dir(&self.root)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation directory cannot be listed"))?
        {
            let entry = entry
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation entry cannot be read"))?;
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
                continue;
            }
            ensure_regular(&path)?;
            let state = self.read_path(&path)?;
            if state.method.as_deref() == Some("task.dispatch")
                && state
                    .intent
                    .as_ref()
                    .and_then(|intent| intent["bridge_boot_id"].as_str())
                    == Some(boot_id)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn recover_uncertain(&self) -> Result<()> {
        self.recover_uncertain_with_code("ADAPTER_RESTART_AFTER_EFFECT_MARKER")
    }

    pub fn recover_uncertain_with_code(&self, diagnostic_code: &str) -> Result<()> {
        let diagnostic_code = safe_diagnostic_code(diagnostic_code);
        for state in self.unresolved()? {
            let receipt = state.receipt.as_ref().ok_or_else(|| {
                Error::new("ADAPTER_JOURNAL", "unresolved operation has no receipt")
            })?;
            let scope = state
                .intent
                .as_ref()
                .and_then(|intent| {
                    intent["native_scope_key"]
                        .as_str()
                        .or_else(|| intent["native"]["native_scope_key"].as_str())
                })
                .map(ToOwned::to_owned);
            let native = state.intent.as_ref().map(|intent| &intent["native"]);
            let outcome = RuntimeOutcome {
                operation_id: state.operation_id.clone(),
                outcome: swarm_contracts::runtime::EffectOutcome::Unknown,
                native_scope_key: scope,
                native_root_id: state
                    .intent
                    .as_ref()
                    .and_then(|intent| intent["native_root_id"].as_str())
                    .map(ToOwned::to_owned),
                turn_id: None,
                native_input_id: native
                    .and_then(|intent| intent["user_message_uuid"].as_str())
                    .map(ToOwned::to_owned),
                details: serde_json::json!({
                    "diagnostic_code":diagnostic_code,
                    "completion_condition":"unknown",
                    "replay_permitted":false,
                    "module_receipt":receipt
                }),
            };
            self.save_outcome(&state.operation_id, &outcome)?;
        }
        Ok(())
    }

    fn append(&self, operation_id: &str, record: &JournalRecord) -> Result<()> {
        let path = self.path(operation_id)?;
        if path.exists() {
            ensure_regular(&path)?;
        } else {
            write_private_new(&path, &[])
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be created"))?;
        }
        let bytes = serde_json::to_vec(record)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation record exceeds its size boundary",
            ));
        }
        if !path.exists() {
            let mut first = bytes;
            first.push(b'\n');
            return write_private_new(&path, &first).map_err(|_| {
                Error::new(
                    "ADAPTER_JOURNAL",
                    "operation intent cannot be durably created",
                )
            });
        }
        if path.exists()
            && fs::metadata(&path).is_ok_and(|metadata| {
                metadata.len().saturating_add(bytes.len() as u64 + 1) > MAX_STATE_BYTES
            })
        {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation history exceeds its total size boundary",
            ));
        }
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be appended"))?;
        private_permissions(&path, false)?;
        file.write_all(&bytes)
            .and_then(|_| file.write_all(b"\n"))
            .and_then(|_| file.sync_all())
            .map_err(|_| {
                Error::new(
                    "ADAPTER_JOURNAL",
                    "operation record could not be durably saved",
                )
            })
    }

    fn path(&self, operation_id: &str) -> Result<PathBuf> {
        if operation_id.trim().is_empty()
            || operation_id.len() > 256
            || operation_id.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation identity is invalid",
            ));
        }
        Ok(self
            .root
            .join(format!("{}.jsonl", hex_digest(operation_id.as_bytes()))))
    }

    fn read_path(&self, path: &Path) -> Result<OperationState> {
        let file = File::open(path)
            .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be read"))?;
        let mut reader = BufReader::new(file);
        let mut state = OperationState::default();
        let mut line = Vec::new();
        loop {
            line.clear();
            let read = reader
                .read_until(b'\n', &mut line)
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record read failed"))?;
            if read == 0 {
                break;
            }
            if line.len() > MAX_RECORD_BYTES || line.last() != Some(&b'\n') {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation record exceeds its line boundary",
                ));
            }
            line.pop();
            apply_record(
                &mut state,
                serde_json::from_slice(&line)
                    .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record is invalid"))?,
            )?;
        }
        if state.operation_id.is_empty() || state.receipt.is_none() {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "operation record has no durable intent",
            ));
        }
        Ok(state)
    }
}

fn apply_record(state: &mut OperationState, record: JournalRecord) -> Result<()> {
    if record.version != 1
        || (!state.operation_id.is_empty() && record.operation_id != state.operation_id)
    {
        return Err(Error::new(
            "ADAPTER_JOURNAL",
            "operation record identity changed",
        ));
    }
    state.operation_id = record.operation_id;
    match record.kind.as_str() {
        "intent" => {
            let receipt = record
                .receipt
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "intent receipt is missing"))?;
            let method = record
                .method
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "intent method is missing"))?;
            let intent = record
                .intent
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "intent marker is missing"))?;
            receipt
                .validate()
                .map_err(|_| Error::new("ADAPTER_JOURNAL", "intent receipt is invalid"))?;
            if receipt.operation_id != state.operation_id
                || state.receipt.as_ref().is_some_and(|old| old != &receipt)
                || state.method.as_ref().is_some_and(|old| old != &method)
                || state.intent.as_ref().is_some_and(|old| old != &intent)
            {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "operation intent conflicts with its saved identity",
                ));
            }
            state.receipt = Some(receipt);
            state.method = Some(method);
            state.intent = Some(intent);
        }
        "outcome" => {
            let outcome = record
                .outcome
                .ok_or_else(|| Error::new("ADAPTER_JOURNAL", "saved outcome is missing"))?;
            if state.receipt.is_none() || outcome["operation_id"] != state.operation_id {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "outcome lacks its exact saved intent",
                ));
            }
            let digest = digest_json(&outcome)?;
            if record.digest.as_deref() != Some(digest.as_str())
                || state.outcome.as_ref().is_some_and(|old| old != &outcome)
            {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "saved outcome digest or identity changed",
                ));
            }
            state.outcome = Some(outcome);
            state.outcome_sha256 = Some(digest);
        }
        "acknowledged" => {
            let digest = record.digest.ok_or_else(|| {
                Error::new("ADAPTER_JOURNAL", "acknowledgement digest is missing")
            })?;
            if state.outcome_sha256.as_deref() != Some(digest.as_str()) {
                return Err(Error::new(
                    "ADAPTER_JOURNAL",
                    "acknowledgement differs from the exact saved outcome",
                ));
            }
            state.acknowledged_sha256 = Some(digest);
        }
        _ => {
            return Err(Error::new(
                "ADAPTER_JOURNAL",
                "unknown operation record kind",
            ));
        }
    }
    Ok(())
}

fn ensure_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| Error::new("ADAPTER_STATE", "state directory cannot be inspected"))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new(
            "ADAPTER_STATE",
            "state directory must be a real directory",
        ));
    }
    Ok(())
}

fn ensure_regular(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| Error::new("ADAPTER_JOURNAL", "operation record cannot be inspected"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_STATE_BYTES
    {
        return Err(Error::new(
            "ADAPTER_JOURNAL",
            "operation record must be a bounded regular file",
        ));
    }
    Ok(())
}

fn read_limited(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let mut file = File::open(path)
        .map_err(|_| Error::new("ADAPTER_STATE", "state identity cannot be read"))?;
    let mut bytes = Vec::new();
    file.take((maximum + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::new("ADAPTER_STATE", "state identity read failed"))?;
    if bytes.len() > maximum {
        return Err(Error::new(
            "ADAPTER_STATE",
            "state identity exceeds its boundary",
        ));
    }
    Ok(bytes)
}

pub fn digest_json(value: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(value)?;
    Ok(hex_digest(&bytes))
}

pub fn digest_bytes(bytes: &[u8]) -> String {
    hex_digest(bytes)
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn safe_diagnostic_code(value: &str) -> &str {
    if !value.is_empty()
        && value.len() <= 64
        && value.as_bytes()[0].is_ascii_uppercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        value
    } else {
        "ADAPTER_UNCERTAIN_AFTER_EFFECT_MARKER"
    }
}
