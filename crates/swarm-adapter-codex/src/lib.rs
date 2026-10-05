//! Standalone, attach-only Codex adapter. This package deliberately depends
//! only on the shared host client/contracts and its native JSON-RPC transport.

mod module_contract;

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    env,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use swarm_client::{IpcConfig, ModuleLink};
use swarm_contracts::{
    module_contract::ModuleContractClaim,
    runtime::{EffectOutcome, ModuleReceiptIdentity, RuntimeCommand, RuntimeOutcome},
};
use swarm_process::module_owner::{VerifiedModuleWorker, verify_current_adapter_from_env};
use tokio::{net::TcpStream, time::timeout};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async_with_config,
    tungstenite::{Message, client::IntoClientRequest, http::header::AUTHORIZATION},
};
use url::Url;
use uuid::Uuid;

pub const ARTIFACT_ID: &str = "codex-rust-controller.1";
pub const ARTIFACT_VERSION: &str = "1";
pub const MODULE_ID: &str = "codex";
const MAX_FRAME_BYTES: usize = 1_048_576;
const MAX_HISTORY_PAGES: usize = 100;
const PAGE_SIZE: u64 = 100;
const MAX_CHECKPOINT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_LIVE_OPERATION_COUNT: usize = 256;
const MAX_LIVE_OPERATION_BYTES: usize = 2 * 1024 * 1024;
const MAX_LIVE_OPERATION_RECORD_BYTES: usize = 64 * 1024;
const MAX_OPERATION_ID_BYTES: usize = 512;
const MAX_LIVE_OPERATION_SLOT_BYTES: usize =
    MAX_LIVE_OPERATION_RECORD_BYTES + 2 * MAX_OPERATION_ID_BYTES;
const OPERATION_ACK_DIRECTORY: &str = "operation-acks";
const JOURNAL_MARKER_FILE: &str = "journal.active";
const JOURNAL_MARKER_BYTES: &[u8] = b"eliot-codex-journal-v1\n";
const MAX_OPERATION_ACK_BYTES: u64 = 64 * 1024;
const MAX_PENDING_OUTCOMES_PER_BATCH: usize = 32;
const NATIVE_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const HOST_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub enum AdapterError {
    Configuration,
    Owner,
    Checkpoint,
    Host,
    HostProtocol,
    NativeAttach,
}

impl AdapterError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Configuration => "CODEX_ADAPTER_CONFIGURATION_INVALID",
            Self::Owner => "CODEX_MODULE_OWNER_INVALID",
            Self::Checkpoint => "CODEX_CHECKPOINT_UNAVAILABLE",
            Self::Host => "CODEX_HOST_LINK_UNAVAILABLE",
            Self::HostProtocol => "CODEX_HOST_PROTOCOL_INVALID",
            Self::NativeAttach => "CODEX_NATIVE_ATTACH_UNAVAILABLE",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    pub host_data_dir: PathBuf,
    pub ipc: Value,
    pub endpoint: String,
    #[serde(default)]
    pub token_env: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    version: u32,
    module_artifact_id: String,
    boot_id: String,
    binding_id: Option<String>,
    generation: Option<i64>,
    native_root_id: Option<String>,
    native_scope_key: Option<String>,
    requested_model_provider: Option<String>,
    requested_model: Option<String>,
    workspace_root: Option<String>,
    effective_model_provider: Option<String>,
    effective_model: Option<String>,
    operations: BTreeMap<String, OperationRecord>,
    acknowledged_outcomes: BTreeSet<String>,
    observe_sequence: u64,
    pending_observation: Option<Value>,
}

impl Checkpoint {
    fn new(boot_id: String) -> Self {
        Self {
            version: 2,
            module_artifact_id: ARTIFACT_ID.into(),
            boot_id,
            binding_id: None,
            generation: None,
            native_root_id: None,
            native_scope_key: None,
            requested_model_provider: None,
            requested_model: None,
            workspace_root: None,
            effective_model_provider: None,
            effective_model: None,
            operations: BTreeMap::new(),
            acknowledged_outcomes: BTreeSet::new(),
            observe_sequence: 0,
            pending_observation: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperationRecord {
    method: String,
    kind: String,
    state: String,
    #[serde(default)]
    input_sha256: Option<String>,
    native_root_id: Option<String>,
    native_scope_key: Option<String>,
    requested_model_provider: Option<String>,
    requested_model: Option<String>,
    workspace_root: Option<String>,
    client_user_message_id: Option<String>,
    prompt_sha256: Option<String>,
    prompt_bytes: Option<u64>,
    delivery: Option<String>,
    expected_turn_id: Option<String>,
    returned_turn_id: Option<String>,
    returned_turn_status: Option<String>,
    native_input_id: Option<String>,
    outcome: Option<Value>,
}

impl OperationRecord {
    fn intent(method: &str, kind: &str) -> Self {
        Self {
            method: method.into(),
            kind: kind.into(),
            state: "native_effect_may_have_started".into(),
            input_sha256: None,
            native_root_id: None,
            native_scope_key: None,
            requested_model_provider: None,
            requested_model: None,
            workspace_root: None,
            client_user_message_id: None,
            prompt_sha256: None,
            prompt_bytes: None,
            delivery: None,
            expected_turn_id: None,
            returned_turn_id: None,
            returned_turn_status: None,
            native_input_id: None,
            outcome: None,
        }
    }

    fn compacted(method: String, kind: String, outcome: Value, input_sha256: String) -> Self {
        Self {
            method,
            kind,
            state: "terminal_acknowledged_compacted".into(),
            input_sha256: Some(input_sha256),
            native_root_id: None,
            native_scope_key: None,
            requested_model_provider: None,
            requested_model: None,
            workspace_root: None,
            client_user_message_id: None,
            prompt_sha256: None,
            prompt_bytes: None,
            delivery: None,
            expected_turn_id: None,
            returned_turn_id: None,
            returned_turn_status: None,
            native_input_id: None,
            outcome: Some(outcome),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationTombstone {
    version: u32,
    operation_id: String,
    method: String,
    kind: String,
    outcome: Value,
}

/// A write-ahead full-snapshot journal. A torn latest record makes startup
/// fail closed, so a possible native write cannot be replayed from an older
/// checkpoint. Older snapshots are pruned only after publishing the new one;
/// terminal host-acknowledged IDs remain in the keyed tombstone index.
struct Journal {
    directory: PathBuf,
    sequence: u64,
    state: Checkpoint,
}

impl Journal {
    fn open(directory: PathBuf, boot_id: String) -> Result<Self, AdapterError> {
        if !directory.is_absolute() {
            return Err(AdapterError::Owner);
        }
        fs::create_dir_all(&directory).map_err(|_| AdapterError::Checkpoint)?;
        let mut latest_sequence = 0u64;
        for entry in fs::read_dir(&directory).map_err(|_| AdapterError::Checkpoint)? {
            let entry = entry.map_err(|_| AdapterError::Checkpoint)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(number) = name
                .strip_prefix("checkpoint-")
                .and_then(|value| value.strip_suffix(".json"))
            else {
                if name.starts_with("checkpoint-") {
                    return Err(AdapterError::Checkpoint);
                }
                continue;
            };
            if !(16..=20).contains(&number.len())
                || !number.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(AdapterError::Checkpoint);
            }
            let file_type = entry.file_type().map_err(|_| AdapterError::Checkpoint)?;
            if !file_type.is_file() || file_type.is_symlink() {
                return Err(AdapterError::Checkpoint);
            }
            if entry
                .metadata()
                .map_err(|_| AdapterError::Checkpoint)?
                .len()
                > MAX_CHECKPOINT_BYTES
            {
                return Err(AdapterError::Checkpoint);
            }
            let sequence = number
                .parse::<u64>()
                .map_err(|_| AdapterError::Checkpoint)?;
            if sequence == 0 || format!("{sequence:016}") != number {
                return Err(AdapterError::Checkpoint);
            }
            latest_sequence = latest_sequence.max(sequence);
        }
        let marker_path = directory.join(JOURNAL_MARKER_FILE);
        let marker_present = match fs::symlink_metadata(&marker_path) {
            Ok(_) => {
                if read_bounded_file(&marker_path, JOURNAL_MARKER_BYTES.len() as u64)?
                    != JOURNAL_MARKER_BYTES
                {
                    return Err(AdapterError::Checkpoint);
                }
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(_) => return Err(AdapterError::Checkpoint),
        };
        if marker_present && latest_sequence == 0 {
            return Err(AdapterError::Checkpoint);
        }
        let (sequence, mut state) = if latest_sequence > 0 {
            let path = directory.join(format!("checkpoint-{latest_sequence:016}.json"));
            let bytes = read_bounded_file(&path, MAX_CHECKPOINT_BYTES)?;
            let state: Checkpoint =
                serde_json::from_slice(&bytes).map_err(|_| AdapterError::Checkpoint)?;
            (latest_sequence, state)
        } else {
            (0, Checkpoint::new(boot_id.clone()))
        };
        if state.version != 2 || state.module_artifact_id != ARTIFACT_ID {
            return Err(AdapterError::Checkpoint);
        }
        let prior_boot = state.boot_id.clone();
        state.boot_id = boot_id;
        if prior_boot != state.boot_id {
            state.pending_observation = None;
        }
        let mut journal = Self {
            directory,
            sequence,
            state,
        };
        journal.compact_acknowledged()?;
        journal.validate_live_bounds()?;
        // Persist the new launcher boot even when the native thread stays
        // attached; host recovery owns departure verification. This full
        // snapshot subsumes older snapshots, which are pruned after publish.
        journal.save()?;
        Ok(journal)
    }

    fn save(&mut self) -> Result<(), AdapterError> {
        self.validate_live_bounds()?;
        let bytes = serde_json::to_vec(&self.state).map_err(|_| AdapterError::Checkpoint)?;
        if bytes.len() as u64 > MAX_CHECKPOINT_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        // Mark this directory as initialized before the first snapshot can be
        // published. If a checkpoint is later lost, startup must not mistake
        // the initialized journal for a fresh one.
        self.ensure_journal_marker()?;
        let next = self
            .sequence
            .checked_add(1)
            .ok_or(AdapterError::Checkpoint)?;
        let path = self.directory.join(format!("checkpoint-{next:016}.json"));
        let temporary = self
            .directory
            .join(format!(".checkpoint-{}-{}.tmp", next, Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| AdapterError::Checkpoint)?;
        if file.write_all(&bytes).is_err() || file.sync_all().is_err() {
            drop(file);
            let _ = fs::remove_file(&temporary);
            return Err(AdapterError::Checkpoint);
        }
        drop(file);
        if path.exists() {
            let _ = fs::remove_file(&temporary);
            return Err(AdapterError::Checkpoint);
        }
        if fs::rename(&temporary, &path).is_err() {
            let _ = fs::remove_file(&temporary);
            return Err(AdapterError::Checkpoint);
        }
        self.sequence = next;
        self.prune_old_checkpoints()
    }

    fn ensure_journal_marker(&self) -> Result<(), AdapterError> {
        let path = self.directory.join(JOURNAL_MARKER_FILE);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                let bytes = read_bounded_file(&path, JOURNAL_MARKER_BYTES.len() as u64)?;
                return if bytes == JOURNAL_MARKER_BYTES {
                    Ok(())
                } else {
                    Err(AdapterError::Checkpoint)
                };
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(AdapterError::Checkpoint),
        }
        let temporary = self
            .directory
            .join(format!(".journal-active-{}.tmp", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| AdapterError::Checkpoint)?;
        if file.write_all(JOURNAL_MARKER_BYTES).is_err() || file.sync_all().is_err() {
            drop(file);
            let _ = fs::remove_file(&temporary);
            return Err(AdapterError::Checkpoint);
        }
        drop(file);
        if path.exists() || fs::rename(&temporary, &path).is_err() {
            let _ = fs::remove_file(&temporary);
            let bytes = read_bounded_file(&path, JOURNAL_MARKER_BYTES.len() as u64)?;
            return if bytes == JOURNAL_MARKER_BYTES {
                Ok(())
            } else {
                Err(AdapterError::Checkpoint)
            };
        }
        Ok(())
    }

    fn live_usage(&self) -> Result<(usize, usize), AdapterError> {
        let mut bytes = self
            .state
            .acknowledged_outcomes
            .iter()
            .try_fold(0usize, |total, operation_id| {
                if operation_id.len() <= MAX_OPERATION_ID_BYTES {
                    total.checked_add(operation_id.len())
                } else {
                    None
                }
            })
            .ok_or(AdapterError::Checkpoint)?;
        for (operation_id, record) in &self.state.operations {
            if operation_id.len() > MAX_OPERATION_ID_BYTES {
                return Err(AdapterError::Checkpoint);
            }
            let encoded = serde_json::to_vec(record).map_err(|_| AdapterError::Checkpoint)?;
            if encoded.len() > MAX_LIVE_OPERATION_RECORD_BYTES {
                return Err(AdapterError::Checkpoint);
            }
            bytes = bytes
                .checked_add(operation_id.len())
                .and_then(|total| total.checked_add(encoded.len()))
                .ok_or(AdapterError::Checkpoint)?;
        }
        Ok((self.state.operations.len(), bytes))
    }

    fn validate_live_bounds(&self) -> Result<(), AdapterError> {
        let (count, bytes) = self.live_usage()?;
        if count > MAX_LIVE_OPERATION_COUNT || bytes > MAX_LIVE_OPERATION_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        Ok(())
    }

    fn can_start_operation(&self, reconciliation: bool) -> Result<bool, AdapterError> {
        let (count, bytes) = self.live_usage()?;
        // Each command result is persisted before it is reported and
        // acknowledged before the host can give us another command. Ordinary
        // work reserves a later reconcile receipt and target-result growth;
        // reconciliation reserves its receipt and possible target growth.
        let reserved_slots = if reconciliation { 1 } else { 2 };
        let reserved_bytes = MAX_LIVE_OPERATION_SLOT_BYTES
            .checked_mul(if reconciliation { 2 } else { 3 })
            .ok_or(AdapterError::Checkpoint)?;
        Ok(count
            .checked_add(reserved_slots)
            .is_some_and(|needed| needed <= MAX_LIVE_OPERATION_COUNT)
            && bytes
                .checked_add(reserved_bytes)
                .is_some_and(|needed| needed <= MAX_LIVE_OPERATION_BYTES))
    }

    fn operation_record(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationRecord>, AdapterError> {
        if operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        if let Some(record) = self.state.operations.get(operation_id) {
            return Ok(Some(record.clone()));
        }
        let Some(tombstone) = self.read_operation_tombstone(operation_id)? else {
            return Ok(None);
        };
        let input_sha256 = receipt_identity_from_value(&tombstone.outcome)?.input_sha256;
        Ok(Some(OperationRecord::compacted(
            tombstone.method,
            tombstone.kind,
            tombstone.outcome,
            input_sha256,
        )))
    }

    fn operation_ack_path(
        &self,
        operation_id: &str,
        create: bool,
    ) -> Result<Option<PathBuf>, AdapterError> {
        let digest = digest_hex(operation_id.as_bytes());
        let root = self.directory.join(OPERATION_ACK_DIRECTORY);
        if create {
            ensure_directory(&root)?;
        } else if !checked_directory_exists(&root)? {
            return Ok(None);
        }
        let shard = root.join(&digest[..2]);
        if create {
            ensure_directory(&shard)?;
        } else if !checked_directory_exists(&shard)? {
            return Ok(None);
        }
        Ok(Some(shard.join(format!("{}.json", &digest[2..]))))
    }

    fn read_operation_tombstone(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationTombstone>, AdapterError> {
        if operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        let Some(path) = self.operation_ack_path(operation_id, false)? else {
            return Ok(None);
        };
        match fs::symlink_metadata(&path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(AdapterError::Checkpoint),
        };
        let bytes = read_bounded_file(&path, MAX_OPERATION_ACK_BYTES)?;
        let tombstone: OperationTombstone =
            serde_json::from_slice(&bytes).map_err(|_| AdapterError::Checkpoint)?;
        if tombstone.version != 1
            || tombstone.operation_id != operation_id
            || tombstone.method.is_empty()
            || tombstone.kind.is_empty()
        {
            return Err(AdapterError::Checkpoint);
        }
        let outcome: RuntimeOutcome = serde_json::from_value(tombstone.outcome.clone())
            .map_err(|_| AdapterError::Checkpoint)?;
        let receipt = receipt_identity_from_outcome(&outcome)?;
        if operation_id.len() > MAX_OPERATION_ID_BYTES
            || outcome.operation_id != operation_id
            || receipt.operation_id != operation_id
            || !matches!(
                outcome.outcome,
                EffectOutcome::Applied | EffectOutcome::Rejected
            )
        {
            return Err(AdapterError::Checkpoint);
        }
        Ok(Some(tombstone))
    }

    fn write_operation_tombstone(
        &self,
        operation_id: &str,
        record: &OperationRecord,
    ) -> Result<(), AdapterError> {
        if operation_id.len() > MAX_OPERATION_ID_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        let outcome = record.outcome.clone().ok_or(AdapterError::Checkpoint)?;
        let parsed: RuntimeOutcome =
            serde_json::from_value(outcome.clone()).map_err(|_| AdapterError::Checkpoint)?;
        if parsed.operation_id != operation_id
            || !matches!(
                parsed.outcome,
                EffectOutcome::Applied | EffectOutcome::Rejected
            )
        {
            return Err(AdapterError::Checkpoint);
        }
        let receipt = receipt_identity_from_outcome(&parsed)?;
        if record.input_sha256.as_deref() != Some(receipt.input_sha256.as_str()) {
            return Err(AdapterError::Checkpoint);
        }
        if let Some(existing) = self.read_operation_tombstone(operation_id)? {
            if existing.method == record.method
                && existing.kind == record.kind
                && existing.outcome == outcome
            {
                return Ok(());
            }
            return Err(AdapterError::Checkpoint);
        }
        let tombstone = OperationTombstone {
            version: 1,
            operation_id: operation_id.to_owned(),
            method: record.method.clone(),
            kind: record.kind.clone(),
            outcome,
        };
        let bytes = serde_json::to_vec(&tombstone).map_err(|_| AdapterError::Checkpoint)?;
        if bytes.len() as u64 > MAX_OPERATION_ACK_BYTES {
            return Err(AdapterError::Checkpoint);
        }
        let Some(path) = self.operation_ack_path(operation_id, true)? else {
            return Err(AdapterError::Checkpoint);
        };
        let parent = path.parent().ok_or(AdapterError::Checkpoint)?;
        let temporary = parent.join(format!(".operation-ack-{}.tmp", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| AdapterError::Checkpoint)?;
        if file.write_all(&bytes).is_err() || file.sync_all().is_err() {
            drop(file);
            let _ = fs::remove_file(&temporary);
            return Err(AdapterError::Checkpoint);
        }
        drop(file);
        if path.exists() || fs::rename(&temporary, &path).is_err() {
            let _ = fs::remove_file(&temporary);
            if let Some(existing) = self.read_operation_tombstone(operation_id)? {
                if existing.method == record.method
                    && existing.kind == record.kind
                    && existing.outcome == tombstone.outcome
                {
                    return Ok(());
                }
            }
            return Err(AdapterError::Checkpoint);
        }
        Ok(())
    }

    fn compact_acknowledged(&mut self) -> Result<(), AdapterError> {
        let orphan_acknowledgments = self
            .state
            .acknowledged_outcomes
            .iter()
            .filter(|operation_id| !self.state.operations.contains_key(*operation_id))
            .cloned()
            .collect::<Vec<_>>();
        for operation_id in &orphan_acknowledgments {
            if self.read_operation_tombstone(operation_id)?.is_none() {
                return Err(AdapterError::Checkpoint);
            }
        }
        let operation_ids = self.state.operations.keys().cloned().collect::<Vec<_>>();
        let mut compact = Vec::new();
        for operation_id in operation_ids {
            let record = self
                .state
                .operations
                .get(&operation_id)
                .ok_or(AdapterError::Checkpoint)?
                .clone();
            if let Some(tombstone) = self.read_operation_tombstone(&operation_id)? {
                if tombstone.method != record.method || tombstone.kind != record.kind {
                    return Err(AdapterError::Checkpoint);
                }
                if record.outcome.as_ref() != Some(&tombstone.outcome) {
                    return Err(AdapterError::Checkpoint);
                }
                compact.push(operation_id);
            } else if self.state.acknowledged_outcomes.contains(&operation_id)
                && is_terminal_record(&record, &operation_id)?
            {
                self.write_operation_tombstone(&operation_id, &record)?;
                compact.push(operation_id);
            }
        }
        for operation_id in compact {
            self.state.operations.remove(&operation_id);
            self.state.acknowledged_outcomes.remove(&operation_id);
        }
        for operation_id in orphan_acknowledgments {
            self.state.acknowledged_outcomes.remove(&operation_id);
        }
        Ok(())
    }

    fn prune_old_checkpoints(&self) -> Result<(), AdapterError> {
        for entry in fs::read_dir(&self.directory).map_err(|_| AdapterError::Checkpoint)? {
            let entry = entry.map_err(|_| AdapterError::Checkpoint)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(number) = name
                .strip_prefix("checkpoint-")
                .and_then(|value| value.strip_suffix(".json"))
            else {
                if name.starts_with("checkpoint-") {
                    return Err(AdapterError::Checkpoint);
                }
                continue;
            };
            if !(16..=20).contains(&number.len())
                || !number.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(AdapterError::Checkpoint);
            }
            let sequence = number
                .parse::<u64>()
                .map_err(|_| AdapterError::Checkpoint)?;
            if sequence == 0 || format!("{sequence:016}") != number {
                return Err(AdapterError::Checkpoint);
            }
            let file_type = entry.file_type().map_err(|_| AdapterError::Checkpoint)?;
            if !file_type.is_file() || file_type.is_symlink() || sequence > self.sequence {
                return Err(AdapterError::Checkpoint);
            }
            if sequence < self.sequence {
                fs::remove_file(entry.path()).map_err(|_| AdapterError::Checkpoint)?;
            }
        }
        Ok(())
    }

    fn bind(&mut self, binding_id: &str, generation: i64) -> Result<(), AdapterError> {
        if generation <= 0
            || self
                .state
                .binding_id
                .as_deref()
                .is_some_and(|old| old != binding_id)
            || self.state.generation.is_some_and(|old| old != generation)
        {
            return Err(AdapterError::Checkpoint);
        }
        if self.state.binding_id.as_deref() == Some(binding_id)
            && self.state.generation == Some(generation)
        {
            return Ok(());
        }
        self.state.binding_id = Some(binding_id.to_owned());
        self.state.generation = Some(generation);
        self.save()
    }

    fn store_outcome(
        &mut self,
        outcome: &RuntimeOutcome,
        method: &str,
        kind: &str,
    ) -> Result<(), AdapterError> {
        let operation_id = outcome.operation_id.clone();
        let receipt = receipt_identity_from_outcome(outcome)?;
        if receipt.operation_id != operation_id
            || self.state.binding_id.as_deref() != Some(receipt.binding_id.as_str())
            || self.state.generation != Some(receipt.binding_generation)
        {
            return Err(AdapterError::Checkpoint);
        }
        let input_sha256 = receipt.input_sha256;
        let encoded = serde_json::to_value(outcome).map_err(|_| AdapterError::Checkpoint)?;
        if let Some(tombstone) = self.read_operation_tombstone(&operation_id)? {
            return if tombstone.method == method
                && tombstone.kind == kind
                && tombstone.outcome == encoded
            {
                Ok(())
            } else {
                Err(AdapterError::Checkpoint)
            };
        }
        if self
            .state
            .operations
            .get(&operation_id)
            .and_then(|record| record.outcome.as_ref())
            == Some(&encoded)
        {
            return Ok(());
        }
        // Reconciliation may upgrade a previously acknowledged Unknown result
        // after later native history evidence. The new bytes need a fresh host
        // acknowledgment even though the operation ID stays stable.
        self.state.acknowledged_outcomes.remove(&operation_id);
        if let Some(record) = self.state.operations.get_mut(&operation_id) {
            if record.method != method
                || record.kind != kind
                || record
                    .input_sha256
                    .as_deref()
                    .is_some_and(|saved| saved != input_sha256)
            {
                return Err(AdapterError::Checkpoint);
            }
            record.input_sha256 = Some(input_sha256);
            record.state = "reported_pending".into();
            record.outcome = Some(encoded);
        } else {
            let mut record = OperationRecord::intent(method, kind);
            record.input_sha256 = Some(input_sha256);
            record.state = "reported_pending".into();
            record.outcome = Some(encoded);
            self.state.operations.insert(operation_id.clone(), record);
        }
        self.save()
    }

    fn pending_outcomes(&self) -> Result<Vec<RuntimeOutcome>, AdapterError> {
        self.state
            .operations
            .iter()
            .filter(|(id, record)| {
                record.outcome.is_some() && !self.state.acknowledged_outcomes.contains(*id)
            })
            .filter_map(|(_, record)| record.outcome.as_ref())
            .take(MAX_PENDING_OUTCOMES_PER_BATCH)
            .map(|value| {
                serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)
            })
            .collect()
    }

    fn acknowledge_outcome(&mut self, operation_id: &str) -> Result<(), AdapterError> {
        let Some(record) = self.state.operations.get(operation_id).cloned() else {
            return if self.read_operation_tombstone(operation_id)?.is_some() {
                Ok(())
            } else {
                Err(AdapterError::Checkpoint)
            };
        };
        if is_terminal_record(&record, operation_id)? {
            // `module.outcome` has returned only after the Store accepted the
            // result. Publish the durable dedupe marker before removing the
            // record from the current full snapshot.
            self.write_operation_tombstone(operation_id, &record)?;
            self.state.operations.remove(operation_id);
            self.state.acknowledged_outcomes.remove(operation_id);
        } else {
            if self.state.acknowledged_outcomes.contains(operation_id) {
                return Ok(());
            }
            self.state
                .acknowledged_outcomes
                .insert(operation_id.to_owned());
        }
        self.save()
    }

    fn next_observation(&mut self, ready: bool) -> Result<Value, AdapterError> {
        if let Some(pending) = &self.state.pending_observation {
            return Ok(pending.clone());
        }
        self.state.observe_sequence = self
            .state
            .observe_sequence
            .checked_add(1)
            .ok_or(AdapterError::Checkpoint)?;
        let state = json!({
            "module_artifact_id": ARTIFACT_ID,
            "boot_id": self.state.boot_id.as_str(),
            "native_root_id": self.state.native_root_id.as_deref(),
            "native": {
                "root_id": self.state.native_root_id.as_deref(),
                "scope_key": self.state.native_scope_key.as_deref(),
                "ready": ready,
            },
            "describe": {
                "module_artifact_id": ARTIFACT_ID,
                "requested_model_provider": self.state.requested_model_provider.as_deref(),
                "requested_model": self.state.requested_model.as_deref(),
                "effective_model_provider": self.state.effective_model_provider.as_deref(),
                "effective_model": self.state.effective_model.as_deref(),
                "served_model_status": "unknown",
                "billing_status": "unknown",
                "capabilities": {
                    "supported": ["agent.open", "task.dispatch", "agent.send", "agent.reconcile"],
                    "unsupported": ["agent.recover", "agent.refresh", "agent.result", "agent.reply", "agent.configure", "agent.goal", "agent.background", "tools", "family_enumeration", "task_completion"]
                }
            }
        });
        let pending = json!({
            "event_id": format!("{}:{}", self.state.boot_id, self.state.observe_sequence),
            "sequence": self.state.observe_sequence,
            "state": state,
        });
        self.state.pending_observation = Some(pending.clone());
        self.save()?;
        Ok(pending)
    }

    fn acknowledge_observation(&mut self) -> Result<(), AdapterError> {
        self.state.pending_observation = None;
        self.save()
    }
}

fn is_terminal_record(record: &OperationRecord, operation_id: &str) -> Result<bool, AdapterError> {
    let Some(value) = record.outcome.as_ref() else {
        return Ok(false);
    };
    let outcome: RuntimeOutcome =
        serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
    if outcome.operation_id != operation_id {
        return Err(AdapterError::Checkpoint);
    }
    Ok(matches!(
        outcome.outcome,
        EffectOutcome::Applied | EffectOutcome::Rejected
    ))
}

fn checked_directory_exists(path: &Path) -> Result<bool, AdapterError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => Ok(true),
        Ok(_) => Err(AdapterError::Checkpoint),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(AdapterError::Checkpoint),
    }
}

fn ensure_directory(path: &Path) -> Result<(), AdapterError> {
    if !checked_directory_exists(path)? {
        match fs::create_dir(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(AdapterError::Checkpoint),
        }
    }
    if !checked_directory_exists(path)? {
        return Err(AdapterError::Checkpoint);
    }
    Ok(())
}

fn read_bounded_file(path: &Path, maximum_bytes: u64) -> Result<Vec<u8>, AdapterError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| AdapterError::Checkpoint)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > maximum_bytes {
        return Err(AdapterError::Checkpoint);
    }
    let file = File::open(path).map_err(|_| AdapterError::Checkpoint)?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| AdapterError::Checkpoint)?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(AdapterError::Checkpoint);
    }
    Ok(bytes)
}

#[derive(Debug)]
enum NativeError {
    Attach,
    Transport,
    Protocol,
    Rejected,
    UnsupportedServerRequest,
}

struct NativeClient {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    endpoint_identity: String,
    server_name: String,
    server_version: String,
}

impl NativeClient {
    async fn attach(endpoint: &str, bearer: Option<&str>) -> Result<Self, NativeError> {
        let parsed = Url::parse(endpoint).map_err(|_| NativeError::Attach)?;
        if !matches!(parsed.scheme(), "ws" | "wss") || parsed.host_str().is_none() {
            return Err(NativeError::Attach);
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(NativeError::Attach);
        }
        let mut safe = parsed.clone();
        let _ = safe.set_username("");
        let _ = safe.set_password(None);
        safe.set_query(None);
        safe.set_fragment(None);
        let endpoint_identity = digest_hex(safe.as_str().as_bytes());
        let mut request = endpoint
            .into_client_request()
            .map_err(|_| NativeError::Attach)?;
        if let Some(token) = bearer {
            let value = format!("Bearer {token}")
                .parse()
                .map_err(|_| NativeError::Attach)?;
            request.headers_mut().insert(AUTHORIZATION, value);
        }
        let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
            max_message_size: Some(MAX_FRAME_BYTES),
            max_frame_size: Some(MAX_FRAME_BYTES),
            ..Default::default()
        };
        let (socket, _) = timeout(
            CONNECT_TIMEOUT,
            connect_async_with_config(request, Some(config), false),
        )
        .await
        .map_err(|_| NativeError::Attach)?
        .map_err(|_| NativeError::Attach)?;
        let mut client = Self {
            socket,
            endpoint_identity,
            server_name: "unknown-server".into(),
            server_version: "unknown-version".into(),
        };
        let initialized = client
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": "eliot-swarm-codex-controller",
                        "title": "ELIOT Swarm Codex Controller",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {"experimentalApi": false}
                }),
            )
            .await?;
        client.server_name = initialized["serverInfo"]["name"]
            .as_str()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or("unknown-server")
            .to_owned();
        client.server_version = initialized["serverInfo"]["version"]
            .as_str()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or("unknown-version")
            .to_owned();
        client.notify("initialized", None).await?;
        Ok(client)
    }

    fn scope_key(&self) -> String {
        format!(
            "codex-appserver:{}:{}:{}",
            self.endpoint_identity, self.server_name, self.server_version
        )
    }

    fn identity_is_known(&self) -> bool {
        self.server_name != "unknown-server" && self.server_version != "unknown-version"
    }

    async fn notify(&mut self, method: &str, params: Option<Value>) -> Result<(), NativeError> {
        let mut packet = json!({"method":method});
        if let Some(params) = params {
            packet["params"] = params;
        }
        let frame = serde_json::to_string(&packet).map_err(|_| NativeError::Protocol)?;
        if frame.len() > MAX_FRAME_BYTES {
            return Err(NativeError::Protocol);
        }
        timeout(
            NATIVE_TIMEOUT,
            self.socket.send(Message::Text(frame.into())),
        )
        .await
        .map_err(|_| NativeError::Transport)?
        .map_err(|_| NativeError::Transport)
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value, NativeError> {
        if !matches!(
            method,
            "initialize"
                | "thread/start"
                | "thread/read"
                | "thread/items/list"
                | "thread/turns/list"
                | "turn/start"
                | "turn/steer"
        ) {
            return Err(NativeError::Protocol);
        }
        let id = Uuid::new_v4().to_string();
        let frame = serde_json::to_string(&json!({
            "id":id,
            "method":method,
            "params":params,
        }))
        .map_err(|_| NativeError::Protocol)?;
        if frame.len() > MAX_FRAME_BYTES {
            return Err(NativeError::Protocol);
        }
        timeout(
            NATIVE_TIMEOUT,
            self.socket.send(Message::Text(frame.into())),
        )
        .await
        .map_err(|_| NativeError::Transport)?
        .map_err(|_| NativeError::Transport)?;
        timeout(NATIVE_TIMEOUT, self.receive_response(&id))
            .await
            .map_err(|_| NativeError::Transport)?
    }

    async fn receive_response(&mut self, request_id: &str) -> Result<Value, NativeError> {
        loop {
            let message = self
                .socket
                .next()
                .await
                .ok_or(NativeError::Transport)?
                .map_err(|_| NativeError::Transport)?;
            let text = match message {
                Message::Text(text) => text,
                Message::Ping(_) | Message::Pong(_) => continue,
                _ => return Err(NativeError::Protocol),
            };
            if text.len() > MAX_FRAME_BYTES {
                return Err(NativeError::Protocol);
            }
            let packet: Value =
                serde_json::from_str(text.as_str()).map_err(|_| NativeError::Protocol)?;
            if packet.get("id").and_then(Value::as_str) == Some(request_id) {
                if let Some(error) = packet.get("error") {
                    let _ = error;
                    return Err(NativeError::Rejected);
                }
                return packet.get("result").cloned().ok_or(NativeError::Protocol);
            }
            if packet.get("method").and_then(Value::as_str).is_some() && packet.get("id").is_some()
            {
                let id = packet.get("id").cloned().ok_or(NativeError::Protocol)?;
                let method = packet["method"].as_str().ok_or(NativeError::Protocol)?;
                let params = packet.get("params").cloned().unwrap_or(Value::Null);
                let reply = match decline_server_request(method, &params) {
                    Some(result) => json!({"id":id,"result":result}),
                    None => {
                        let failure = json!({"id":id,"error":{"code":-32601,"message":"unsupported server request"}});
                        let wire =
                            serde_json::to_string(&failure).map_err(|_| NativeError::Protocol)?;
                        timeout(NATIVE_TIMEOUT, self.socket.send(Message::Text(wire.into())))
                            .await
                            .map_err(|_| NativeError::Transport)?
                            .map_err(|_| NativeError::Transport)?;
                        return Err(NativeError::UnsupportedServerRequest);
                    }
                };
                let wire = serde_json::to_string(&reply).map_err(|_| NativeError::Protocol)?;
                if wire.len() > MAX_FRAME_BYTES {
                    return Err(NativeError::Protocol);
                }
                timeout(NATIVE_TIMEOUT, self.socket.send(Message::Text(wire.into())))
                    .await
                    .map_err(|_| NativeError::Transport)?
                    .map_err(|_| NativeError::Transport)?;
            }
        }
    }
}

fn decline_server_request(method: &str, params: &Value) -> Option<Value> {
    match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            Some(json!({"decision":"decline"}))
        }
        "applyPatchApproval" | "execCommandApproval" => Some(json!({"decision":"denied"})),
        "item/permissions/requestApproval" => Some(json!({"permissions":{},"scope":"turn"})),
        "item/tool/call" => Some(json!({"contentItems":[],"success":false})),
        "item/tool/requestUserInput" => {
            let mut answers = serde_json::Map::new();
            if let Some(questions) = params.get("questions").and_then(Value::as_array) {
                for question in questions {
                    if let Some(id) = question.get("id").and_then(Value::as_str) {
                        answers.insert(id.to_owned(), json!({"answers":[]}));
                    }
                }
            }
            Some(json!({"answers":answers}))
        }
        "mcpServer/elicitation/request" => Some(json!({"action":"decline"})),
        _ => None,
    }
}

#[derive(Clone)]
struct HistoryMatch {
    item_id: String,
    turn_id: String,
    text: String,
}

enum HistoryRead {
    Complete(Vec<HistoryMatch>),
    Truncated,
    Failed,
}

impl NativeClient {
    async fn read_thread(&mut self, thread_id: &str) -> Result<Value, NativeError> {
        self.request(
            "thread/read",
            json!({"threadId":thread_id,"includeTurns":false}),
        )
        .await
    }

    async fn read_history(&mut self, thread_id: &str, client_id: &str) -> HistoryRead {
        let mut cursor: Option<String> = None;
        let mut seen = HashSet::new();
        let mut matches = Vec::new();
        for _ in 0..MAX_HISTORY_PAGES {
            let mut params = json!({
                "threadId":thread_id,
                "limit":PAGE_SIZE,
                "sortDirection":"desc",
            });
            if let Some(value) = cursor.as_ref() {
                params["cursor"] = json!(value);
            }
            let response = match self.request("thread/items/list", params).await {
                Ok(response) => response,
                Err(_) => return HistoryRead::Failed,
            };
            let Some(data) = response.get("data").and_then(Value::as_array) else {
                return HistoryRead::Failed;
            };
            for entry in data {
                let item = &entry["item"];
                if item["type"] != "userMessage" || item["clientId"] != client_id {
                    continue;
                }
                let Some(item_id) = item["id"].as_str().filter(|s| !s.is_empty()) else {
                    return HistoryRead::Failed;
                };
                let Some(turn_id) = entry["turnId"].as_str().filter(|s| !s.is_empty()) else {
                    return HistoryRead::Failed;
                };
                let Some(parts) = item["content"].as_array() else {
                    return HistoryRead::Failed;
                };
                let mut text = String::new();
                for part in parts {
                    if part["type"] != "text" {
                        return HistoryRead::Failed;
                    }
                    let Some(part_text) = part["text"].as_str() else {
                        return HistoryRead::Failed;
                    };
                    text.push_str(part_text);
                }
                matches.push(HistoryMatch {
                    item_id: item_id.to_owned(),
                    turn_id: turn_id.to_owned(),
                    text,
                });
            }
            match response.get("nextCursor") {
                None | Some(Value::Null) => return HistoryRead::Complete(matches),
                Some(Value::String(next)) if next.is_empty() => {
                    return HistoryRead::Complete(matches);
                }
                Some(Value::String(next)) if !seen.insert(next.clone()) => {
                    return HistoryRead::Truncated;
                }
                Some(Value::String(next)) => cursor = Some(next.clone()),
                Some(_) => return HistoryRead::Failed,
            }
        }
        HistoryRead::Truncated
    }

    async fn active_turns(
        &mut self,
        thread_id: &str,
    ) -> Result<(Vec<(String, String)>, bool), NativeError> {
        let response = self
            .request(
                "thread/turns/list",
                json!({"threadId":thread_id,"limit":20,"sortDirection":"desc"}),
            )
            .await?;
        let data = response["data"].as_array().ok_or(NativeError::Protocol)?;
        let mut active = Vec::new();
        for turn in data {
            let status = turn["status"].as_str().ok_or(NativeError::Protocol)?;
            if status == "inProgress" {
                let id = turn["id"].as_str().ok_or(NativeError::Protocol)?;
                active.push((id.to_owned(), status.to_owned()));
            }
        }
        let page_limited = match response.get("nextCursor") {
            None | Some(Value::Null) => false,
            Some(Value::String(cursor)) => !cursor.is_empty(),
            Some(_) => return Err(NativeError::Protocol),
        };
        Ok((active, page_limited))
    }
}

fn digest_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn normalize_path(value: &str) -> Option<String> {
    let path = Path::new(value);
    if !path.is_absolute() {
        return None;
    }
    Some(
        path.to_string_lossy()
            .replace('/', "\\")
            .trim_end_matches('\\')
            .to_lowercase(),
    )
}

fn validate_route(command: &RuntimeCommand) -> Result<(String, String, String), &'static str> {
    if command.route["runtime"] != "codex" || command.route["module_artifact_id"] != ARTIFACT_ID {
        return Err("ROUTE_ARTIFACT_MISMATCH");
    }
    let options = &command.route["native_options"];
    let provider = options["modelProvider"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or("EXPLICIT_MODEL_PROVIDER_REQUIRED")?;
    let model = options["model"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or("EXPLICIT_MODEL_REQUIRED")?;
    let workspace = options["workspaceRoot"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or("ABSOLUTE_WORKSPACE_REQUIRED")?;
    if normalize_path(workspace).is_none() {
        return Err("ABSOLUTE_WORKSPACE_REQUIRED");
    }
    Ok((provider.to_owned(), model.to_owned(), workspace.to_owned()))
}

fn prompt_for(command: &RuntimeCommand) -> Result<String, &'static str> {
    let input = &command.input;
    let prompt = if command.method == "task.dispatch" {
        let canonical = input["task_snapshot_canonical"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or("TASK_SNAPSHOT_CANONICAL_REQUIRED")?;
        let text = input["text"]
            .as_str()
            .filter(|s| !s.trim().is_empty())
            .ok_or("PROMPT_REQUIRED")?;
        format!("Task specification: {canonical}\n\n{text}")
    } else {
        input["text"]
            .as_str()
            .filter(|s| !s.trim().is_empty())
            .ok_or("PROMPT_REQUIRED")?
            .to_owned()
    };
    if prompt.trim().is_empty() {
        return Err("PROMPT_REQUIRED");
    }
    Ok(prompt)
}

fn outcome(
    operation_id: &str,
    disposition: EffectOutcome,
    root: Option<&str>,
    scope: Option<&str>,
    turn: Option<&str>,
    input: Option<&str>,
    details: Value,
) -> RuntimeOutcome {
    RuntimeOutcome {
        operation_id: operation_id.to_owned(),
        outcome: disposition,
        native_scope_key: scope.map(str::to_owned),
        native_root_id: root.map(str::to_owned),
        turn_id: turn.map(str::to_owned),
        native_input_id: input.map(str::to_owned),
        details,
    }
}

fn decode_outcome(value: &Value, operation_id: &str) -> RuntimeOutcome {
    serde_json::from_value(value.clone()).unwrap_or_else(|_| {
        outcome(
            operation_id,
            EffectOutcome::Unknown,
            None,
            None,
            None,
            None,
            json!({"diagnostic_code":"CHECKPOINT_OUTCOME_INVALID","native_replay":false}),
        )
    })
}

fn receipt_identity_from_outcome(
    outcome: &RuntimeOutcome,
) -> Result<ModuleReceiptIdentity, AdapterError> {
    let receipt: ModuleReceiptIdentity =
        serde_json::from_value(outcome.details["module_receipt"].clone())
            .map_err(|_| AdapterError::Checkpoint)?;
    receipt.validate().map_err(|_| AdapterError::Checkpoint)?;
    if receipt.operation_id != outcome.operation_id {
        return Err(AdapterError::Checkpoint);
    }
    Ok(receipt)
}

fn receipt_identity_from_value(value: &Value) -> Result<ModuleReceiptIdentity, AdapterError> {
    let outcome: RuntimeOutcome =
        serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
    receipt_identity_from_outcome(&outcome)
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn attach_module_receipt(
    outcome: &mut RuntimeOutcome,
    claim: &ModuleContractClaim,
    binding_id: &str,
    generation: i64,
    operation_id: &str,
    input_sha256: &str,
) -> Result<(), AdapterError> {
    if outcome.operation_id != operation_id || !valid_sha256(input_sha256) {
        return Err(AdapterError::HostProtocol);
    }
    let expected = module_contract::receipt_identity(
        claim,
        binding_id,
        generation,
        operation_id,
        input_sha256,
    )?;
    let details = outcome
        .details
        .as_object_mut()
        .ok_or(AdapterError::HostProtocol)?;
    if let Some(existing) = details.get("module_receipt") {
        let saved: ModuleReceiptIdentity =
            serde_json::from_value(existing.clone()).map_err(|_| AdapterError::HostProtocol)?;
        if saved != expected {
            return Err(AdapterError::HostProtocol);
        }
    } else {
        details.insert(
            "module_receipt".to_owned(),
            serde_json::to_value(expected).map_err(|_| AdapterError::HostProtocol)?,
        );
    }
    Ok(())
}

fn attach_command_receipt(
    outcome: &mut RuntimeOutcome,
    command: &RuntimeCommand,
    claim: &ModuleContractClaim,
) -> Result<(), AdapterError> {
    let input_sha256 = command
        .input_sha256
        .as_deref()
        .filter(|digest| valid_sha256(digest))
        .ok_or(AdapterError::HostProtocol)?;
    attach_module_receipt(
        outcome,
        claim,
        &command.binding_id,
        command.generation,
        &command.operation_id,
        input_sha256,
    )
}

fn validate_saved_receipt(
    outcome: &RuntimeOutcome,
    claim: &ModuleContractClaim,
    binding_id: &str,
    generation: i64,
    operation_id: &str,
    input_sha256: &str,
) -> Result<(), AdapterError> {
    let saved = receipt_identity_from_outcome(outcome)?;
    let expected = module_contract::receipt_identity(
        claim,
        binding_id,
        generation,
        operation_id,
        input_sha256,
    )?;
    if saved != expected {
        return Err(AdapterError::Checkpoint);
    }
    Ok(())
}

fn base_details(state: &Checkpoint) -> Value {
    json!({
        "module_artifact_id": ARTIFACT_ID,
        "requested_model_provider": state.requested_model_provider.as_deref(),
        "requested_model": state.requested_model.as_deref(),
        "effective_model_provider": state.effective_model_provider.as_deref(),
        "effective_model": state.effective_model.as_deref(),
        "served_model": null,
        "served_model_status": "unknown",
        "billing_status": "unknown",
        "fallback_used": false,
    })
}

fn unknown_send(
    record: &OperationRecord,
    operation_id: &str,
    code: &'static str,
) -> RuntimeOutcome {
    let mut details = json!({
        "diagnostic_code": code,
        "native_replay": false,
        "native_input_readback": "unverified",
        "client_user_message_id": record.client_user_message_id,
        "prompt_sha256": record.prompt_sha256,
        "prompt_bytes": record.prompt_bytes,
    });
    details["completion_condition"] = Value::Null;
    outcome(
        operation_id,
        EffectOutcome::Unknown,
        record.native_root_id.as_deref(),
        record.native_scope_key.as_deref(),
        None,
        None,
        details,
    )
}

async fn reconcile_send(
    native: &mut NativeClient,
    record: &OperationRecord,
    operation_id: &str,
) -> RuntimeOutcome {
    let (Some(root), Some(client_id), Some(expected_digest), Some(expected_bytes)) = (
        record.native_root_id.as_deref(),
        record.client_user_message_id.as_deref(),
        record.prompt_sha256.as_deref(),
        record.prompt_bytes,
    ) else {
        return unknown_send(record, operation_id, "NATIVE_IDENTITY_UNAVAILABLE");
    };
    let matches = match native.read_history(root, client_id).await {
        HistoryRead::Complete(matches) => matches,
        HistoryRead::Truncated => {
            return unknown_send(record, operation_id, "NATIVE_HISTORY_PAGE_LIMIT");
        }
        HistoryRead::Failed => {
            return unknown_send(record, operation_id, "NATIVE_HISTORY_READ_FAILED");
        }
    };
    if matches.is_empty() {
        return unknown_send(record, operation_id, "NATIVE_ITEM_NOT_OBSERVED");
    }
    if matches.len() != 1 {
        return unknown_send(record, operation_id, "NATIVE_ITEM_CORRELATION_NOT_UNIQUE");
    }
    let matched = &matches[0];
    if digest_hex(matched.text.as_bytes()) != expected_digest
        || matched.text.len() as u64 != expected_bytes
        || record
            .returned_turn_id
            .as_deref()
            .is_some_and(|id| id != matched.turn_id)
        || record.delivery.as_deref() == Some("steer")
            && record.expected_turn_id.as_deref() != Some(matched.turn_id.as_str())
    {
        return unknown_send(
            record,
            operation_id,
            "NATIVE_ITEM_CONTENT_OR_IDENTITY_MISMATCH",
        );
    }
    let mut details = json!({
        "module_artifact_id": ARTIFACT_ID,
        "completion_condition": "native_input_admitted",
        "native_input_readback": "verified",
        "client_user_message_id": client_id,
        "prompt_sha256": expected_digest,
        "prompt_bytes": expected_bytes,
        "native_replay": false,
        "requested_model_provider": record.requested_model_provider.as_deref(),
        "requested_model": record.requested_model.as_deref(),
        "effective_model_provider": record.requested_model_provider.as_deref(),
        "effective_model": record.requested_model.as_deref(),
        "effective_model_status": "thread_configuration_verified",
        "served_model": null,
        "served_model_status": "unknown",
        "billing_status": "unknown",
        "fallback_used": false,
    });
    outcome(
        operation_id,
        EffectOutcome::Applied,
        Some(root),
        record.native_scope_key.as_deref(),
        Some(&matched.turn_id),
        Some(&matched.item_id),
        details,
    )
}

async fn reconcile_open(
    native: &mut NativeClient,
    record: &OperationRecord,
    operation_id: &str,
) -> RuntimeOutcome {
    let (Some(root), Some(scope), Some(provider), Some(model), Some(workspace)) = (
        record.native_root_id.as_deref(),
        record.native_scope_key.as_deref(),
        record.requested_model_provider.as_deref(),
        record.requested_model.as_deref(),
        record.workspace_root.as_deref(),
    ) else {
        let mut details =
            json!({"diagnostic_code":"THREAD_START_OUTCOME_UNKNOWN","native_replay":false});
        details["completion_condition"] = Value::Null;
        return outcome(
            operation_id,
            EffectOutcome::Unknown,
            None,
            record.native_scope_key.as_deref(),
            None,
            None,
            details,
        );
    };
    if native.scope_key() != scope || !native.identity_is_known() {
        return outcome(
            operation_id,
            EffectOutcome::Unknown,
            Some(root),
            Some(scope),
            None,
            None,
            json!({"diagnostic_code":"NATIVE_SCOPE_CHANGED","native_replay":false}),
        );
    }
    let thread = match native.read_thread(root).await {
        Ok(response) => response["thread"].clone(),
        Err(_) => {
            return outcome(
                operation_id,
                EffectOutcome::Unknown,
                Some(root),
                Some(scope),
                None,
                None,
                json!({"diagnostic_code":"THREAD_READ_FAILED","native_replay":false}),
            );
        }
    };
    let observed_cwd = thread["cwd"].as_str();
    let exact = thread["id"].as_str() == Some(root)
        && thread["modelProvider"].as_str() == Some(provider)
        && thread["model"].as_str() == Some(model)
        && observed_cwd.and_then(normalize_path) == normalize_path(workspace);
    let mut details = json!({
        "module_artifact_id": ARTIFACT_ID,
        "native_thread_readback": if exact { "verified" } else { "mismatch" },
        "native_replay": false,
        "requested_model_provider": provider,
        "requested_model": model,
        "effective_model_provider": thread["modelProvider"],
        "effective_model": thread["model"],
        "effective_model_status": if exact { "thread_configuration_verified" } else { "unknown" },
        "served_model": null,
        "served_model_status": "unknown",
        "billing_status": "unknown",
        "fallback_used": false,
        "thread": {
            "id": thread["id"],
            "model_provider": thread["modelProvider"],
            "model": thread["model"],
            "cwd": observed_cwd,
            "workspace_status": if observed_cwd.and_then(normalize_path) == normalize_path(workspace) { "workspace_exact" } else { "workspace_mismatch" },
        }
    });
    if !exact {
        details["diagnostic_code"] = json!("THREAD_CONFIGURATION_MISMATCH");
        return outcome(
            operation_id,
            EffectOutcome::Unknown,
            Some(root),
            Some(scope),
            None,
            None,
            details,
        );
    }
    details["completion_condition"] = json!("native_thread_opened");
    outcome(
        operation_id,
        EffectOutcome::Applied,
        Some(root),
        Some(scope),
        None,
        None,
        details,
    )
}

fn rejected(command: &RuntimeCommand, code: &'static str, state: &Checkpoint) -> RuntimeOutcome {
    let mut details = base_details(state);
    details["diagnostic_code"] = json!(code);
    details["native_replay"] = json!(false);
    outcome(
        &command.operation_id,
        EffectOutcome::Rejected,
        state.native_root_id.as_deref(),
        state.native_scope_key.as_deref(),
        None,
        None,
        details,
    )
}

async fn open_operation(
    command: &RuntimeCommand,
    journal: &mut Journal,
    endpoint: &str,
    token: Option<&str>,
) -> RuntimeOutcome {
    if command.native_root_id.is_some() {
        return rejected(command, "OPEN_ROOT_ALREADY_ASSIGNED", &journal.state);
    }
    if let Some(previous) = journal.state.operations.get(&command.operation_id) {
        if let Some(result) = &previous.outcome {
            return serde_json::from_value(result.clone()).unwrap_or_else(|_| {
                outcome(
                    &command.operation_id,
                    EffectOutcome::Unknown,
                    None,
                    None,
                    None,
                    None,
                    json!({"diagnostic_code":"CHECKPOINT_OUTCOME_INVALID","native_replay":false}),
                )
            });
        }
        let mut details = base_details(&journal.state);
        details["diagnostic_code"] = json!("THREAD_START_OUTCOME_UNKNOWN");
        details["native_replay"] = json!(false);
        return outcome(
            &command.operation_id,
            EffectOutcome::Unknown,
            None,
            None,
            None,
            None,
            details,
        );
    }
    if journal.state.native_root_id.is_some() {
        return rejected(command, "THREAD_ALREADY_OPEN", &journal.state);
    }
    let (provider, model, workspace) = match validate_route(command) {
        Ok(route) => route,
        Err(code) => return rejected(command, code, &journal.state),
    };
    let mut native = match NativeClient::attach(endpoint, token).await {
        Ok(native) => native,
        Err(_) => return rejected(command, "NATIVE_ATTACH_FAILED", &journal.state),
    };
    if !native.identity_is_known() {
        return rejected(command, "NATIVE_SERVER_IDENTITY_UNVERIFIED", &journal.state);
    }
    let scope = native.scope_key();
    let mut record = OperationRecord::intent("agent.open", "open");
    record.input_sha256 = command.input_sha256.clone();
    record.native_scope_key = Some(scope.clone());
    record.requested_model_provider = Some(provider.clone());
    record.requested_model = Some(model.clone());
    record.workspace_root = Some(workspace.clone());
    journal.state.requested_model_provider = Some(provider.clone());
    journal.state.requested_model = Some(model.clone());
    journal.state.workspace_root = Some(workspace.clone());
    journal.state.native_scope_key = Some(scope.clone());
    journal
        .state
        .operations
        .insert(command.operation_id.clone(), record);
    if journal.save().is_err() {
        return outcome(
            &command.operation_id,
            EffectOutcome::Rejected,
            None,
            None,
            None,
            None,
            json!({"diagnostic_code":"CHECKPOINT_WRITE_FAILED","native_replay":false}),
        );
    }
    let response = native
        .request(
            "thread/start",
            json!({"cwd":workspace,"modelProvider":provider,"model":model}),
        )
        .await;
    let Ok(response) = response else {
        let mut details = base_details(&journal.state);
        details["diagnostic_code"] = json!("THREAD_START_OUTCOME_UNKNOWN");
        details["native_replay"] = json!(false);
        return outcome(
            &command.operation_id,
            EffectOutcome::Unknown,
            None,
            Some(&scope),
            None,
            None,
            details,
        );
    };
    let thread = &response["thread"];
    let thread_id = thread["id"].as_str().filter(|s| !s.is_empty());
    let observed_cwd = thread["cwd"].as_str();
    let effective_provider = thread["modelProvider"].as_str();
    let effective_model = thread["model"].as_str();
    let exact_workspace = observed_cwd
        .and_then(normalize_path)
        .zip(normalize_path(&workspace))
        .is_some_and(|(observed, requested)| observed == requested);
    let Some(thread_id) = thread_id else {
        let mut details = base_details(&journal.state);
        details["diagnostic_code"] = json!("THREAD_START_IDENTITY_MISSING");
        details["native_replay"] = json!(false);
        return outcome(
            &command.operation_id,
            EffectOutcome::Unknown,
            None,
            Some(&scope),
            None,
            None,
            details,
        );
    };
    journal.state.native_root_id = Some(thread_id.to_owned());
    journal.state.effective_model_provider = effective_provider.map(str::to_owned);
    journal.state.effective_model = effective_model.map(str::to_owned);
    if let Some(record) = journal.state.operations.get_mut(&command.operation_id) {
        record.native_root_id = Some(thread_id.to_owned());
    }
    let config_exact = effective_provider == Some(provider.as_str())
        && effective_model == Some(model.as_str())
        && exact_workspace;
    if journal.save().is_err() {
        return outcome(
            &command.operation_id,
            EffectOutcome::Unknown,
            Some(thread_id),
            Some(&scope),
            None,
            None,
            json!({"diagnostic_code":"CHECKPOINT_WRITE_FAILED","native_replay":false}),
        );
    }
    let mut details = base_details(&journal.state);
    details["native_replay"] = json!(false);
    details["thread"] = json!({
        "id": thread_id,
        "model_provider": effective_provider,
        "model": effective_model,
        "cwd": observed_cwd,
        "workspace_status": if exact_workspace { "workspace_exact" } else { "workspace_mismatch" },
    });
    if !config_exact {
        details["diagnostic_code"] = json!("THREAD_CONFIGURATION_MISMATCH");
        let result = outcome(
            &command.operation_id,
            EffectOutcome::Unknown,
            Some(thread_id),
            Some(&scope),
            None,
            None,
            details,
        );
        return result;
    }
    details["completion_condition"] = json!("native_thread_opened");
    let result = outcome(
        &command.operation_id,
        EffectOutcome::Applied,
        Some(thread_id),
        Some(&scope),
        None,
        None,
        details,
    );
    result
}

async fn send_operation(
    command: &RuntimeCommand,
    journal: &mut Journal,
    endpoint: &str,
    token: Option<&str>,
) -> RuntimeOutcome {
    if let Some(previous) = journal.state.operations.get(&command.operation_id) {
        if let Some(result) = &previous.outcome {
            return decode_outcome(result, &command.operation_id);
        }
        if previous.native_root_id.as_deref() != journal.state.native_root_id.as_deref()
            || previous.native_scope_key.as_deref() != journal.state.native_scope_key.as_deref()
        {
            return unknown_send(
                previous,
                &command.operation_id,
                "NATIVE_OPERATION_CONTEXT_CHANGED",
            );
        }
        let Ok(mut native) = NativeClient::attach(endpoint, token).await else {
            return unknown_send(previous, &command.operation_id, "NATIVE_CLIENT_UNAVAILABLE");
        };
        if journal.state.native_scope_key.as_deref() != Some(native.scope_key().as_str()) {
            return unknown_send(previous, &command.operation_id, "NATIVE_SCOPE_CHANGED");
        }
        return reconcile_send(&mut native, previous, &command.operation_id).await;
    }
    let root = match (
        journal.state.native_root_id.as_deref(),
        command.native_root_id.as_deref(),
    ) {
        (Some(expected), Some(supplied)) if expected == supplied => expected.to_owned(),
        _ => return rejected(command, "NATIVE_IDENTITY_MISMATCH", &journal.state),
    };
    let (provider, model, workspace) = match validate_route(command) {
        Ok(route) => route,
        Err(code) => return rejected(command, code, &journal.state),
    };
    if journal.state.requested_model_provider.as_deref() != Some(provider.as_str())
        || journal.state.requested_model.as_deref() != Some(model.as_str())
        || journal.state.workspace_root.as_deref() != Some(workspace.as_str())
    {
        return rejected(command, "ROUTE_CONFIGURATION_MISMATCH", &journal.state);
    }
    let prompt = match prompt_for(command) {
        Ok(prompt) => prompt,
        Err(code) => return rejected(command, code, &journal.state),
    };
    let digest = digest_hex(prompt.as_bytes());
    let byte_count = prompt.len() as u64;
    let delivery = command.input["delivery"].as_str();
    if command.method == "agent.send" && !matches!(delivery, Some("next_turn" | "steer")) {
        return rejected(command, "DELIVERY_MODE_UNSUPPORTED", &journal.state);
    }
    let steer = command.method == "agent.send" && delivery == Some("steer");
    let expected_turn = if steer {
        match command.input["expected_turn_id"]
            .as_str()
            .filter(|s| !s.is_empty())
        {
            Some(id) => Some(id.to_owned()),
            None => return rejected(command, "EXPECTED_TURN_ID_REQUIRED", &journal.state),
        }
    } else {
        None
    };
    let mut native = match NativeClient::attach(endpoint, token).await {
        Ok(native) => native,
        Err(_) => return rejected(command, "NATIVE_ATTACH_FAILED", &journal.state),
    };
    let scope = native.scope_key();
    if journal.state.native_scope_key.as_deref() != Some(scope.as_str())
        || !native.identity_is_known()
    {
        return rejected(command, "NATIVE_SCOPE_CHANGED", &journal.state);
    }
    let thread = match native.read_thread(&root).await {
        Ok(value) => value["thread"].clone(),
        Err(_) => return rejected(command, "THREAD_READ_FAILED", &journal.state),
    };
    if thread["id"].as_str() != Some(root.as_str())
        || thread["modelProvider"].as_str() != Some(provider.as_str())
        || thread["model"].as_str() != Some(model.as_str())
        || thread["cwd"].as_str().and_then(normalize_path) != normalize_path(&workspace)
    {
        return rejected(command, "THREAD_CONFIGURATION_MISMATCH", &journal.state);
    }
    if steer {
        let (active, limited) = match native.active_turns(&root).await {
            Ok(result) => result,
            Err(_) => return rejected(command, "ACTIVE_TURN_READ_FAILED", &journal.state),
        };
        if limited || active.len() != 1 || Some(active[0].0.as_str()) != expected_turn.as_deref() {
            return rejected(command, "EXPECTED_TURN_NOT_ACTIVE", &journal.state);
        }
    } else if thread["status"]["type"] != "idle" {
        return rejected(command, "THREAD_NOT_IDLE", &journal.state);
    }
    let mut record = OperationRecord::intent(&command.method, "send");
    record.input_sha256 = command.input_sha256.clone();
    record.native_root_id = Some(root.clone());
    record.native_scope_key = Some(scope.clone());
    record.requested_model_provider = Some(provider.clone());
    record.requested_model = Some(model.clone());
    record.workspace_root = Some(workspace);
    record.client_user_message_id = Some(command.operation_id.clone());
    record.prompt_sha256 = Some(digest);
    record.prompt_bytes = Some(byte_count);
    record.delivery = Some(if steer { "steer" } else { "next_turn" }.into());
    record.expected_turn_id = expected_turn.clone();
    journal
        .state
        .operations
        .insert(command.operation_id.clone(), record);
    if journal.save().is_err() {
        return outcome(
            &command.operation_id,
            EffectOutcome::Rejected,
            Some(&root),
            Some(&scope),
            None,
            None,
            json!({"diagnostic_code":"CHECKPOINT_WRITE_FAILED","native_replay":false}),
        );
    }
    let input = json!([{"type":"text","text":prompt}]);
    let request = if steer {
        native
            .request(
                "turn/steer",
                json!({
                    "threadId":root,
                    "expectedTurnId":expected_turn,
                    "input":input,
                    "clientUserMessageId":command.operation_id,
                }),
            )
            .await
            .map(|response| {
                (
                    response["turnId"].as_str().map(str::to_owned),
                    Some("inProgress".to_owned()),
                )
            })
    } else {
        native
            .request(
                "turn/start",
                json!({
                    "threadId":root,
                    "input":input,
                    "model":model,
                    "clientUserMessageId":command.operation_id,
                }),
            )
            .await
            .map(|response| {
                (
                    response["turn"]["id"].as_str().map(str::to_owned),
                    response["turn"]["status"].as_str().map(str::to_owned),
                )
            })
    };
    if let Ok((Some(turn_id), turn_status)) = request {
        if let Some(record) = journal.state.operations.get_mut(&command.operation_id) {
            record.returned_turn_id = Some(turn_id);
            record.returned_turn_status = turn_status;
        }
        if journal.save().is_err() {
            let record = journal
                .state
                .operations
                .get(&command.operation_id)
                .expect("saved send marker remains");
            return unknown_send(record, &command.operation_id, "CHECKPOINT_WRITE_FAILED");
        }
    }
    let result = {
        let record = journal
            .state
            .operations
            .get(&command.operation_id)
            .expect("send marker persisted");
        reconcile_send(&mut native, record, &command.operation_id).await
    };
    result
}

async fn reconcile_operation(
    command: &RuntimeCommand,
    journal: &mut Journal,
    target_record: Option<OperationRecord>,
    claim: &ModuleContractClaim,
    endpoint: &str,
    token: Option<&str>,
) -> Result<Vec<RuntimeOutcome>, AdapterError> {
    let Some(input_sha256) = command
        .input_sha256
        .as_deref()
        .filter(|digest| valid_sha256(digest))
    else {
        return Err(AdapterError::HostProtocol);
    };
    let target_id = command.input["operation_id"]
        .as_str()
        .filter(|s| !s.is_empty());
    let Some(target_id) = target_id else {
        let mut result = rejected(command, "RECONCILIATION_TARGET_REQUIRED", &journal.state);
        attach_command_receipt(&mut result, command, claim)?;
        return Ok(vec![result]);
    };
    let Some(target_input_sha256) = command.target_input_sha256.as_deref() else {
        let mut result = rejected(
            command,
            "RECONCILIATION_TARGET_DIGEST_REQUIRED",
            &journal.state,
        );
        attach_command_receipt(&mut result, command, claim)?;
        return Ok(vec![result]);
    };
    if !valid_sha256(target_input_sha256) {
        let mut result = rejected(
            command,
            "RECONCILIATION_TARGET_DIGEST_MISMATCH",
            &journal.state,
        );
        attach_command_receipt(&mut result, command, claim)?;
        return Ok(vec![result]);
    }
    let Some(target) = target_record else {
        let mut receipt = outcome(
            &command.operation_id,
            EffectOutcome::Applied,
            journal.state.native_root_id.as_deref(),
            journal.state.native_scope_key.as_deref(),
            None,
            None,
            json!({"completion_condition":"native_readback_completed","target_operation_id":target_id,"resolved":false,"disposition":"reconciliation_context_unavailable","native_replay":false}),
        );
        receipt.details["diagnostic_code"] = json!("RECONCILIATION_CONTEXT_UNAVAILABLE");
        attach_module_receipt(
            &mut receipt,
            claim,
            &command.binding_id,
            command.generation,
            &command.operation_id,
            input_sha256,
        )?;
        return Ok(vec![receipt]);
    };
    if target.input_sha256.as_deref() != Some(target_input_sha256) {
        let mut result = rejected(
            command,
            "RECONCILIATION_TARGET_DIGEST_MISMATCH",
            &journal.state,
        );
        attach_command_receipt(&mut result, command, claim)?;
        return Ok(vec![result]);
    }
    let method_kind_matches = matches!(
        (target.method.as_str(), target.kind.as_str()),
        ("agent.open", "open") | ("agent.send", "send") | ("task.dispatch", "send")
    );
    if !method_kind_matches {
        let mut result = rejected(command, "RECONCILIATION_TARGET_UNSUPPORTED", &journal.state);
        attach_command_receipt(&mut result, command, claim)?;
        return Ok(vec![result]);
    }
    let mut target_result = if let Some(value) = target.outcome.as_ref() {
        let result: RuntimeOutcome =
            serde_json::from_value(value.clone()).map_err(|_| AdapterError::Checkpoint)?;
        validate_saved_receipt(
            &result,
            claim,
            &command.binding_id,
            command.generation,
            target_id,
            target_input_sha256,
        )?;
        Some(result)
    } else {
        None
    };
    let mut disposition = String::from("recorded_outcome");
    let prior_resolved = target_result.as_ref().is_some_and(|value| {
        matches!(
            value.outcome,
            EffectOutcome::Applied | EffectOutcome::Rejected
        )
    });
    let target_context_matches = target.native_root_id.as_deref()
        == journal.state.native_root_id.as_deref()
        && target.native_scope_key.as_deref() == journal.state.native_scope_key.as_deref();
    if target.kind == "send" && !prior_resolved {
        let mut reconciled = None;
        if target_context_matches {
            if let Ok(mut native) = NativeClient::attach(endpoint, token).await {
                if journal.state.native_scope_key.as_deref() == Some(native.scope_key().as_str()) {
                    reconciled = Some(reconcile_send(&mut native, &target, target_id).await);
                }
            }
        }
        let mut result = reconciled.unwrap_or_else(|| {
            unknown_send(
                &target,
                target_id,
                if target_context_matches {
                    "NATIVE_CLIENT_UNAVAILABLE"
                } else {
                    "NATIVE_OPERATION_CONTEXT_CHANGED"
                },
            )
        });
        result.details["reconcile_operation_id"] = json!(command.operation_id.as_str());
        disposition = if matches!(result.outcome, EffectOutcome::Applied) {
            String::from("native_item_readback_verified")
        } else {
            result.details["diagnostic_code"]
                .as_str()
                .unwrap_or("native_readback_unresolved")
                .to_owned()
        };
        attach_module_receipt(
            &mut result,
            claim,
            &command.binding_id,
            command.generation,
            target_id,
            target_input_sha256,
        )?;
        journal.store_outcome(&result, &target.method, &target.kind)?;
        target_result = Some(result);
    }
    if target.kind == "open" && !prior_resolved {
        if target.native_root_id.is_some() && target_context_matches {
            let mut result = None;
            if let Ok(mut native) = NativeClient::attach(endpoint, token).await {
                result = Some(reconcile_open(&mut native, &target, target_id).await);
            }
            let mut value = result.unwrap_or_else(|| {
                outcome(
                    target_id,
                    EffectOutcome::Unknown,
                    target.native_root_id.as_deref(),
                    target.native_scope_key.as_deref(),
                    None,
                    None,
                    json!({"diagnostic_code":"NATIVE_CLIENT_UNAVAILABLE","native_replay":false}),
                )
            });
            value.details["reconcile_operation_id"] = json!(command.operation_id.as_str());
            disposition = if matches!(value.outcome, EffectOutcome::Applied) {
                String::from("native_thread_readback_verified")
            } else {
                value.details["diagnostic_code"]
                    .as_str()
                    .unwrap_or("thread_start_uncorrelated")
                    .to_owned()
            };
            attach_module_receipt(
                &mut value,
                claim,
                &command.binding_id,
                command.generation,
                target_id,
                target_input_sha256,
            )?;
            journal.store_outcome(&value, &target.method, &target.kind)?;
            target_result = Some(value);
        } else {
            let mut unresolved = outcome(
                target_id,
                EffectOutcome::Unknown,
                target.native_root_id.as_deref(),
                target.native_scope_key.as_deref(),
                None,
                None,
                json!({
                    "diagnostic_code": if target_context_matches { "THREAD_START_OUTCOME_UNKNOWN" } else { "NATIVE_OPERATION_CONTEXT_CHANGED" },
                    "native_replay":false
                }),
            );
            unresolved.details["reconcile_operation_id"] = json!(command.operation_id.as_str());
            disposition = if target_context_matches {
                String::from("thread_start_uncorrelated")
            } else {
                String::from("native_operation_context_changed")
            };
            attach_module_receipt(
                &mut unresolved,
                claim,
                &command.binding_id,
                command.generation,
                target_id,
                target_input_sha256,
            )?;
            journal.store_outcome(&unresolved, &target.method, &target.kind)?;
            target_result = Some(unresolved);
        }
    }
    let resolved = target_result.as_ref().is_some_and(|value| {
        matches!(
            value.outcome,
            EffectOutcome::Applied | EffectOutcome::Rejected
        )
    });
    let mut reconcile = outcome(
        &command.operation_id,
        EffectOutcome::Applied,
        journal.state.native_root_id.as_deref(),
        journal.state.native_scope_key.as_deref(),
        None,
        None,
        json!({
            "completion_condition":"native_readback_completed",
            "target_operation_id":target_id,
            "resolved":resolved,
            "disposition":disposition,
            "native_replay":false,
        }),
    );
    attach_module_receipt(
        &mut reconcile,
        claim,
        &command.binding_id,
        command.generation,
        &command.operation_id,
        input_sha256,
    )?;
    let mut results = Vec::new();
    if let Some(result) = target_result {
        results.push(result);
    }
    results.push(reconcile);
    Ok(results)
}

async fn handle_command(
    command: RuntimeCommand,
    journal: &mut Journal,
    claim: &ModuleContractClaim,
    endpoint: &str,
    token: Option<&str>,
) -> Result<Vec<RuntimeOutcome>, AdapterError> {
    let Some(input_sha256) = command
        .input_sha256
        .as_deref()
        .filter(|digest| valid_sha256(digest))
    else {
        return Err(AdapterError::HostProtocol);
    };
    if command.operation_id.is_empty() || command.operation_id.len() > MAX_OPERATION_ID_BYTES {
        return Err(AdapterError::HostProtocol);
    }
    if journal.state.binding_id.as_deref() != Some(command.binding_id.as_str())
        || journal.state.generation != Some(command.generation)
    {
        return Err(AdapterError::HostProtocol);
    }
    let previous = journal.operation_record(&command.operation_id)?;
    if previous.as_ref().is_some_and(|record| {
        record.method != command.method
            || record
                .input_sha256
                .as_deref()
                .is_some_and(|saved| saved != input_sha256)
    }) {
        return Err(AdapterError::HostProtocol);
    }
    if let Some(record) = &previous {
        if record.input_sha256.as_deref() != Some(input_sha256) {
            return Err(AdapterError::Checkpoint);
        }
        if let Some(saved) = &record.outcome {
            let result: RuntimeOutcome =
                serde_json::from_value(saved.clone()).map_err(|_| AdapterError::Checkpoint)?;
            validate_saved_receipt(
                &result,
                claim,
                &command.binding_id,
                command.generation,
                &command.operation_id,
                input_sha256,
            )?;
            return Ok(vec![result]);
        }
    }
    let result = if previous.is_none()
        && !journal.can_start_operation(command.method == "agent.reconcile")?
    {
        let mut failure = rejected(&command, "JOURNAL_LIVE_CAPACITY_REACHED", &journal.state);
        attach_command_receipt(&mut failure, &command, claim)?;
        vec![failure]
    } else {
        let reconciliation_target = if command.method == "agent.reconcile" {
            command.input["operation_id"]
                .as_str()
                .map(|operation_id| journal.operation_record(operation_id))
                .transpose()?
                .flatten()
        } else {
            None
        };
        match command.method.as_str() {
            "agent.open" => {
                let mut result = open_operation(&command, journal, endpoint, token).await;
                attach_command_receipt(&mut result, &command, claim)?;
                vec![result]
            }
            "task.dispatch" | "agent.send" => {
                let mut result = send_operation(&command, journal, endpoint, token).await;
                attach_command_receipt(&mut result, &command, claim)?;
                vec![result]
            }
            "agent.reconcile" => {
                reconcile_operation(
                    &command,
                    journal,
                    reconciliation_target,
                    claim,
                    endpoint,
                    token,
                )
                .await?
            }
            _ => {
                let mut result = rejected(&command, "CAPABILITY_UNAVAILABLE", &journal.state);
                result.details["capability"] = json!(command.method);
                attach_command_receipt(&mut result, &command, claim)?;
                vec![result]
            }
        }
    };
    for outcome in &result {
        let (method, kind) = if outcome.operation_id == command.operation_id {
            let kind = match command.method.as_str() {
                "agent.open" => String::from("open"),
                "agent.send" | "task.dispatch" => String::from("send"),
                "agent.reconcile" => String::from("reconcile"),
                _ => String::from("receipt"),
            };
            (command.method.clone(), kind)
        } else {
            let record = journal
                .operation_record(&outcome.operation_id)?
                .ok_or(AdapterError::Checkpoint)?;
            (record.method, record.kind)
        };
        journal.store_outcome(outcome, &method, &kind)?;
    }
    Ok(result)
}

async fn connect_host(
    config: &AdapterConfig,
    context: &module_contract::ModuleRuntimeContext,
    ipc: &IpcConfig,
) -> Result<ModuleLink, AdapterError> {
    ModuleLink::connect(&config.host_data_dir, &context.credential, ipc)
        .await
        .map_err(|_| AdapterError::Host)
}

async fn report_pending(host: &mut ModuleLink, journal: &mut Journal) -> Result<(), AdapterError> {
    loop {
        let batch = journal.pending_outcomes()?;
        if batch.is_empty() {
            break;
        }
        for result in batch {
            let operation_id = result.operation_id.clone();
            let value = serde_json::to_value(&result).map_err(|_| AdapterError::Checkpoint)?;
            host.outcome(value).await.map_err(|_| AdapterError::Host)?;
            journal.acknowledge_outcome(&operation_id)?;
        }
    }
    let pending = journal.next_observation(false)?;
    host.observe(pending.clone())
        .await
        .map_err(|_| AdapterError::Host)?;
    journal.acknowledge_observation()?;
    Ok(())
}

/// Run the module protocol. The app-server is only contacted with WebSocket
/// client operations; this adapter contains no process start/stop code.
pub async fn run(config: AdapterConfig) -> Result<(), AdapterError> {
    if !config.host_data_dir.is_absolute() {
        return Err(AdapterError::Configuration);
    }
    let parsed_endpoint = Url::parse(&config.endpoint).map_err(|_| AdapterError::Configuration)?;
    if !matches!(parsed_endpoint.scheme(), "ws" | "wss")
        || parsed_endpoint.host_str().is_none()
        || !parsed_endpoint.username().is_empty()
        || parsed_endpoint.password().is_some()
    {
        return Err(AdapterError::Configuration);
    }
    let context = module_contract::load_runtime_context()?;
    let mut journal = Journal::open(context.state_dir.clone(), context.worker.boot_id.clone())?;
    let ipc: IpcConfig =
        serde_json::from_value(config.ipc.clone()).map_err(|_| AdapterError::Configuration)?;
    let token = match config.token_env.as_deref() {
        Some(name) if !name.trim().is_empty() => {
            let value = env::var(name).map_err(|_| AdapterError::Configuration)?;
            if value.trim().is_empty() {
                return Err(AdapterError::Configuration);
            }
            Some(value)
        }
        Some(_) => return Err(AdapterError::Configuration),
        None => None,
    };
    let endpoint = config.endpoint.clone();
    loop {
        let mut host = match connect_host(&config, &context, &ipc).await {
            Ok(host) => host,
            Err(_) => {
                tokio::time::sleep(HOST_RETRY_DELAY).await;
                continue;
            }
        };
        let hello = json!({
            "boot_id": journal.state.boot_id,
            "module_artifact_id": ARTIFACT_ID,
            "native_root_id": journal.state.native_root_id,
            "native_scope_key": journal.state.native_scope_key,
            "native_ready": false,
            "managed_owner": context.worker.owner_record.clone(),
        });
        let hello_result = match host.hello(hello, Some(&context.claim)).await {
            Ok(value) => value,
            Err(_) => {
                tokio::time::sleep(HOST_RETRY_DELAY).await;
                continue;
            }
        };
        module_contract::validate_negotiated_hello(&hello_result, &context)?;
        journal.bind(&context.binding_id, context.generation)?;
        match report_pending(&mut host, &mut journal).await {
            Ok(()) => {}
            Err(AdapterError::Host) => {
                tokio::time::sleep(HOST_RETRY_DELAY).await;
                continue;
            }
            Err(error) => return Err(error),
        }
        loop {
            let response = match host.next().await {
                Ok(value) => value,
                Err(_) => break,
            };
            if let Some(raw) = response.get("command").filter(|value| !value.is_null()) {
                let command: RuntimeCommand = match serde_json::from_value(raw.clone()) {
                    Ok(command) => command,
                    Err(_) => return Err(AdapterError::HostProtocol),
                };
                let outcomes = handle_command(
                    command,
                    &mut journal,
                    &context.claim,
                    &endpoint,
                    token.as_deref(),
                )
                .await?;
                for result in outcomes {
                    let operation_id = result.operation_id.clone();
                    let value =
                        serde_json::to_value(&result).map_err(|_| AdapterError::Checkpoint)?;
                    if host.outcome(value).await.is_err() {
                        break;
                    }
                    journal.acknowledge_outcome(&operation_id)?;
                }
                match report_pending(&mut host, &mut journal).await {
                    Ok(()) => {}
                    Err(AdapterError::Host) => break,
                    Err(error) => return Err(error),
                }
            } else {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        tokio::time::sleep(HOST_RETRY_DELAY).await;
    }
}
