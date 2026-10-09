//! Durable, root-scoped Claude SDK permission and question callbacks.
//!
//! The files here are a bounded durable index of exact SDK callback requests
//! and reply transitions. The live callback itself remains owned by the SDK
//! bridge; this journal never authorizes replay after an uncertain delivery.

use crate::journal::{OperationJournal, digest_bytes, digest_json};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};
use swarm_contracts::error::{Error, Result};
use swarm_process::{
    private_permissions, remove_private_durable, replace_private_durable, write_private_new,
};

const MAX_INTERACTION_FILES: usize = 256;
const MAX_PENDING_INTERACTIONS: usize = 8;
const MAX_REQUEST_BYTES: usize = 32 * 1024;
const MAX_REPLY_BYTES: usize = 16 * 1024;
const MAX_RECORD_BYTES: usize = 96 * 1024;
const MAX_RETENTION_BYTES: usize = 16 * 1024;
const MAX_RETENTION_STEPS: usize = 16;
const INTERACTION_RETENTION_DIRECTORY: &str = "retention";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    Permission,
    Question,
}

impl InteractionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Permission => "permission",
            Self::Question => "question",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct InteractionRequest {
    pub schema_version: u32,
    pub binding_id: String,
    pub generation: i64,
    pub native_scope_key: String,
    pub bridge_boot_id: String,
    pub native_root_id: String,
    pub request_id: String,
    pub kind: InteractionKind,
    pub tool_name: String,
    pub input: Value,
    pub request_sha256: String,
}

pub struct InteractionRequestEvidence<'a> {
    pub bridge_boot_id: &'a str,
    pub native_root_id: &'a str,
    pub request_id: &'a str,
    pub kind: InteractionKind,
    pub tool_name: &'a str,
    pub input: &'a Value,
    pub request_sha256: &'a str,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum InteractionStatus {
    Pending,
    ReplyIntent,
    Acknowledged,
    Rejected,
    Unknown,
    Retired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct InteractionRecord {
    schema_version: u32,
    request: InteractionRequest,
    #[serde(default)]
    sequence: Option<u64>,
    status: InteractionStatus,
    reply_operation_id: Option<String>,
    reply_sha256: Option<String>,
    reply: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct InteractionRetentionState {
    version: u32,
    next_sequence: u64,
    cursor_sequence: u64,
    migration_complete: bool,
    examined: u64,
    compacted: u64,
    deferred_unknown: u64,
    deferred_unacknowledged: u64,
    deferred_reference: u64,
    deferred_damage: u64,
    degraded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct InteractionRetentionItem {
    version: u32,
    sequence: u64,
    request_id: String,
    request_key: String,
    request_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PendingInteractionIndex {
    version: u32,
    bridge_boot_id: String,
    native_root_id: String,
    entries: Vec<PendingInteractionIndexEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PendingInteractionIndexEntry {
    request_id: String,
    request_key: String,
    request_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct InteractionTombstone {
    version: u32,
    sequence: u64,
    request_id: String,
    request_key: String,
    binding_id: String,
    generation: i64,
    native_scope_key: String,
    bridge_boot_id: String,
    native_root_id: String,
    kind: InteractionKind,
    tool_name: String,
    request_sha256: String,
    status: InteractionStatus,
    reply_operation_id: String,
    reply_sha256: String,
    outcome_sha256: String,
    acknowledged_sha256: String,
}

#[derive(Clone, Copy)]
struct InteractionTerminalIdentity<'a> {
    request_id: &'a str,
    request_sha256: &'a str,
    binding_id: &'a str,
    generation: i64,
    native_scope_key: &'a str,
    bridge_boot_id: &'a str,
    native_root_id: &'a str,
    kind: InteractionKind,
    tool_name: &'a str,
    status: InteractionStatus,
    reply_operation_id: Option<&'a str>,
    reply_sha256: Option<&'a str>,
}

enum InteractionRetentionDecision {
    Ready(String),
    Unknown,
    Unacknowledged,
    Referenced,
    Damage,
}

pub struct InteractionJournal {
    directory: PathBuf,
    retention_directory: PathBuf,
    binding_id: String,
    generation: i64,
    native_scope_key: String,
}

impl InteractionJournal {
    pub fn open(
        state_directory: &Path,
        binding_id: &str,
        generation: i64,
        native_scope_key: &str,
    ) -> Result<Self> {
        if !state_directory.is_absolute()
            || binding_id.trim().is_empty()
            || generation < 1
            || native_scope_key.trim().is_empty()
        {
            return Err(Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "interactive state identity is invalid",
            ));
        }
        let directory = state_directory.join("interactions");
        match fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(Error::new(
                    "ADAPTER_INTERACTION_JOURNAL",
                    "interactive state path is not a regular directory",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&directory).map_err(|_| {
                    Error::new(
                        "ADAPTER_INTERACTION_JOURNAL",
                        "interactive state directory cannot be created",
                    )
                })?;
            }
            Err(_) => {
                return Err(Error::new(
                    "ADAPTER_INTERACTION_JOURNAL",
                    "interactive state directory cannot be inspected",
                ));
            }
        }
        private_permissions(&directory, true)?;
        let retention_directory = directory.join(INTERACTION_RETENTION_DIRECTORY);
        ensure_interaction_directory(&retention_directory)?;
        ensure_interaction_directory(&retention_directory.join("items"))?;
        ensure_interaction_directory(&retention_directory.join("tombstones"))?;
        ensure_interaction_directory(&retention_directory.join("pending"))?;
        let journal = Self {
            directory,
            retention_directory,
            binding_id: binding_id.to_owned(),
            generation,
            native_scope_key: native_scope_key.to_owned(),
        };
        journal.ensure_bounded_history()?;
        Ok(journal)
    }

    pub fn record_request(
        &self,
        evidence: InteractionRequestEvidence<'_>,
    ) -> Result<InteractionRequest> {
        let InteractionRequestEvidence {
            bridge_boot_id,
            native_root_id,
            request_id,
            kind,
            tool_name,
            input,
            request_sha256,
        } = evidence;
        let mut request = InteractionRequest {
            schema_version: 1,
            binding_id: self.binding_id.clone(),
            generation: self.generation,
            native_scope_key: self.native_scope_key.clone(),
            bridge_boot_id: bridge_boot_id.to_owned(),
            native_root_id: native_root_id.to_owned(),
            request_id: request_id.to_owned(),
            kind,
            tool_name: tool_name.to_owned(),
            input: input.clone(),
            request_sha256: String::new(),
        };
        request.request_sha256 = digest_json(&request_payload(&request)?)?;
        if request.request_sha256 != request_sha256 {
            return Err(Error::new(
                "SDK_INTERACTION_DIGEST_MISMATCH",
                "Claude callback digest differs from its exact typed request",
            ));
        }
        validate_request(&request)?;
        let path = self.path(request_id)?;
        let tombstone_path = self.tombstone_path(request_id)?;
        if path.exists() {
            let previous = self.read_path(&path)?;
            if previous.request == request {
                if previous.status == InteractionStatus::Pending {
                    self.add_pending_entry(&previous.request)?;
                }
                return Ok(request);
            }
            return Err(Error::new(
                "ADAPTER_INTERACTION_CONFLICT",
                "callback request identity was reused with different content",
            ));
        }
        if tombstone_path.exists() {
            let tombstone = self.read_tombstone(&tombstone_path, request_id)?;
            if tombstone.matches_request(&request) {
                return Ok(request);
            }
            return Err(Error::new(
                "ADAPTER_INTERACTION_CONFLICT",
                "callback request identity conflicts with its compact terminal marker",
            ));
        }
        if self
            .pending_records_for_root(bridge_boot_id, native_root_id)?
            .len()
            >= MAX_PENDING_INTERACTIONS
        {
            return Err(Error::new(
                "ADAPTER_INTERACTION_CAPACITY",
                "too many Claude SDK callbacks are waiting for a reply",
            ));
        }
        let sequence = self.allocate_sequence()?;
        let record = InteractionRecord {
            schema_version: 1,
            request: request.clone(),
            sequence: Some(sequence),
            status: InteractionStatus::Pending,
            reply_operation_id: None,
            reply_sha256: None,
            reply: None,
        };
        let item = InteractionRetentionItem {
            version: 1,
            sequence,
            request_id: request.request_id.clone(),
            request_key: request_key(&request.request_id),
            request_sha256: request.request_sha256.clone(),
        };
        self.write_retention_item(&item, true)?;
        let bytes = encode_record(&record)?;
        write_private_new(&path, &bytes).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "callback request cannot be durably recorded",
            )
        })?;
        let saved = self.read_path(&path)?;
        if saved.request != request
            || saved.sequence != Some(sequence)
            || saved.status != InteractionStatus::Pending
        {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "callback request failed exact durable readback",
            ));
        }
        self.add_pending_entry(&saved.request)?;
        Ok(request)
    }

    pub fn get(&self, request_id: &str) -> Result<Option<InteractionRequest>> {
        let path = self.path(request_id)?;
        if !path.exists() {
            let tombstone_path = self.tombstone_path(request_id)?;
            if tombstone_path.exists() {
                self.read_tombstone(&tombstone_path, request_id)?;
            }
            return Ok(None);
        }
        let record = self.read_path(&path)?;
        Ok(Some(record.request))
    }

    pub fn pending_snapshot(&self, bridge_boot_id: &str, native_root_id: &str) -> Result<Value> {
        let mut pending = self
            .pending_records_for_root(bridge_boot_id, native_root_id)?
            .into_iter()
            .map(|record| {
                json!({
                    "request":record.request,
                    "status":"pending"
                })
            })
            .collect::<Vec<_>>();
        pending.sort_by(|left, right| {
            left["request"]["request_id"]
                .as_str()
                .cmp(&right["request"]["request_id"].as_str())
        });
        Ok(Value::Array(pending))
    }

    pub fn cancel_request(
        &self,
        bridge_boot_id: &str,
        native_root_id: &str,
        request_id: &str,
        request_sha256: &str,
    ) -> Result<Option<String>> {
        let path = self.path(request_id)?;
        if !path.exists() {
            let tombstone_path = self.tombstone_path(request_id)?;
            if !tombstone_path.exists() {
                return Err(Error::new(
                    "ADAPTER_INTERACTION_JOURNAL",
                    "callback state is missing",
                ));
            }
            let tombstone = self.read_tombstone(&tombstone_path, request_id)?;
            if tombstone.bridge_boot_id != bridge_boot_id
                || tombstone.native_root_id != native_root_id
                || tombstone.request_sha256 != request_sha256
            {
                return Err(Error::new(
                    "INTERACTION_REQUEST_IDENTITY",
                    "callback cancellation differs from its compact terminal marker",
                ));
            }
            return Ok(None);
        }
        let mut record = self.read_path(&path)?;
        if record.request.bridge_boot_id != bridge_boot_id
            || record.request.native_root_id != native_root_id
            || record.request.request_sha256 != request_sha256
        {
            return Err(Error::new(
                "INTERACTION_REQUEST_IDENTITY",
                "callback cancellation differs from the exact pending request",
            ));
        }
        match record.status {
            InteractionStatus::Pending => {
                record.status = InteractionStatus::Retired;
                self.replace(&path, &record)?;
                Ok(None)
            }
            InteractionStatus::ReplyIntent => {
                let operation_id = record.reply_operation_id.clone().ok_or_else(|| {
                    Error::new(
                        "ADAPTER_INTERACTION_JOURNAL",
                        "reply intent has no operation identity",
                    )
                })?;
                record.status = InteractionStatus::Unknown;
                self.replace(&path, &record)?;
                Ok(Some(operation_id))
            }
            InteractionStatus::Unknown => Ok(record.reply_operation_id.clone()),
            InteractionStatus::Retired
            | InteractionStatus::Acknowledged
            | InteractionStatus::Rejected => Ok(None),
        }
    }

    pub fn begin_reply(
        &self,
        request_id: &str,
        operation_id: &str,
        reply: &Value,
        reply_sha256: &str,
    ) -> Result<InteractionRequest> {
        let path = self.path(request_id)?;
        let mut record = self.read_path(&path)?;
        if record.status != InteractionStatus::Pending {
            return Err(Error::new(
                "INTERACTION_NOT_PENDING",
                "callback has already been answered, retired, or made uncertain",
            ));
        }
        if operation_id.trim().is_empty()
            || operation_id.len() > 256
            || !valid_sha256(reply_sha256)
            || digest_json(reply)? != reply_sha256
        {
            return Err(Error::new(
                "INTERACTION_REPLY_IDENTITY",
                "callback reply intent identity or digest is invalid",
            ));
        }
        record.status = InteractionStatus::ReplyIntent;
        record.reply_operation_id = Some(operation_id.to_owned());
        record.reply_sha256 = Some(reply_sha256.to_owned());
        record.reply = Some(reply.clone());
        self.replace(&path, &record)?;
        Ok(record.request)
    }

    pub fn acknowledge_reply(
        &self,
        request_id: &str,
        operation_id: &str,
        reply_sha256: &str,
    ) -> Result<()> {
        let path = self.path(request_id)?;
        if !path.exists() {
            let tombstone_path = self.tombstone_path(request_id)?;
            if !tombstone_path.exists() {
                return Err(Error::new(
                    "ADAPTER_INTERACTION_JOURNAL",
                    "callback state is missing",
                ));
            }
            let tombstone = self.read_tombstone(&tombstone_path, request_id)?;
            if tombstone.status == InteractionStatus::Acknowledged
                && tombstone.reply_operation_id == operation_id
                && tombstone.reply_sha256 == reply_sha256
            {
                return Ok(());
            }
            return Err(Error::new(
                "INTERACTION_ACK_MISMATCH",
                "callback acknowledgement differs from its exact compact marker",
            ));
        }
        let mut record = self.read_path(&path)?;
        if record.status == InteractionStatus::Acknowledged
            && record.reply_operation_id.as_deref() == Some(operation_id)
            && record.reply_sha256.as_deref() == Some(reply_sha256)
        {
            return Ok(());
        }
        if record.status != InteractionStatus::ReplyIntent
            || record.reply_operation_id.as_deref() != Some(operation_id)
            || record.reply_sha256.as_deref() != Some(reply_sha256)
        {
            return Err(Error::new(
                "INTERACTION_ACK_MISMATCH",
                "SDK callback acknowledgement differs from the exact durable reply intent",
            ));
        }
        record.status = InteractionStatus::Acknowledged;
        self.replace(&path, &record)
    }

    pub fn mark_unknown(&self, request_id: &str, operation_id: &str) -> Result<()> {
        let path = self.path(request_id)?;
        let mut record = self.read_path(&path)?;
        if record.status == InteractionStatus::Unknown
            && record.reply_operation_id.as_deref() == Some(operation_id)
        {
            return Ok(());
        }
        if record.status != InteractionStatus::ReplyIntent
            || record.reply_operation_id.as_deref() != Some(operation_id)
        {
            return Err(Error::new(
                "INTERACTION_REPLY_IDENTITY",
                "uncertain reply does not match its durable callback intent",
            ));
        }
        record.status = InteractionStatus::Unknown;
        self.replace(&path, &record)
    }

    pub fn mark_uncertain_reply(
        &self,
        request_id: &str,
        operation_id: &str,
        reply: &Value,
        reply_sha256: &str,
    ) -> Result<()> {
        let path = self.path(request_id)?;
        let mut record = self.read_path(&path)?;
        if record.status == InteractionStatus::Unknown
            && record.reply_operation_id.as_deref() == Some(operation_id)
            && record.reply_sha256.as_deref() == Some(reply_sha256)
        {
            return Ok(());
        }
        if !matches!(
            record.status,
            InteractionStatus::Pending | InteractionStatus::ReplyIntent
        ) || (record.status == InteractionStatus::ReplyIntent
            && (record.reply_operation_id.as_deref() != Some(operation_id)
                || record.reply_sha256.as_deref() != Some(reply_sha256)))
            || !valid_sha256(reply_sha256)
            || digest_json(reply)? != reply_sha256
        {
            return Err(Error::new(
                "INTERACTION_REPLY_IDENTITY",
                "uncertain callback delivery differs from its durable operation intent",
            ));
        }
        validate_reply(&record.request, reply)?;
        record.status = InteractionStatus::Unknown;
        record.reply_operation_id = Some(operation_id.to_owned());
        record.reply_sha256 = Some(reply_sha256.to_owned());
        record.reply = Some(reply.clone());
        self.replace(&path, &record)
    }

    pub fn mark_rejected(&self, request_id: &str, operation_id: &str) -> Result<()> {
        let path = self.path(request_id)?;
        let mut record = self.read_path(&path)?;
        if record.status != InteractionStatus::ReplyIntent
            || record.reply_operation_id.as_deref() != Some(operation_id)
        {
            return Err(Error::new(
                "INTERACTION_REPLY_IDENTITY",
                "rejected reply does not match its durable callback intent",
            ));
        }
        record.status = InteractionStatus::Rejected;
        self.replace(&path, &record)
    }

    pub fn retire_bridge(&self, bridge_boot_id: &str, operations: &OperationJournal) -> Result<()> {
        for mut record in self.read_all()? {
            if record.request.bridge_boot_id != bridge_boot_id {
                continue;
            }
            match record.status {
                InteractionStatus::Pending => {
                    record.status = InteractionStatus::Retired;
                    let path = self.path(&record.request.request_id)?;
                    self.replace(&path, &record)?;
                }
                InteractionStatus::ReplyIntent => {
                    let operation_id = record.reply_operation_id.as_deref().ok_or_else(|| {
                        Error::new(
                            "ADAPTER_INTERACTION_JOURNAL",
                            "reply intent has no operation identity",
                        )
                    })?;
                    if let Some(operation) = operations.get(operation_id)?
                        && let Some(outcome) = operation.outcome.as_ref()
                        && outcome["outcome"] == "applied"
                    {
                        record.status = InteractionStatus::Acknowledged;
                    } else {
                        record.status = InteractionStatus::Unknown;
                    }
                    let path = self.path(&record.request.request_id)?;
                    self.replace(&path, &record)?;
                }
                InteractionStatus::Acknowledged
                | InteractionStatus::Rejected
                | InteractionStatus::Unknown
                | InteractionStatus::Retired => {}
            }
        }
        Ok(())
    }

    pub fn recover_stale(&self, operations: &OperationJournal) -> Result<()> {
        for mut record in self.read_all()? {
            match record.status {
                InteractionStatus::Pending => record.status = InteractionStatus::Retired,
                InteractionStatus::ReplyIntent => {
                    let operation_id = record.reply_operation_id.as_deref().ok_or_else(|| {
                        Error::new(
                            "ADAPTER_INTERACTION_JOURNAL",
                            "reply intent has no operation identity",
                        )
                    })?;
                    let operation = operations.get(operation_id)?;
                    record.status = match operation.and_then(|state| state.outcome) {
                        Some(outcome) if outcome["outcome"] == "applied" => {
                            InteractionStatus::Acknowledged
                        }
                        Some(outcome) if outcome["outcome"] == "rejected" => {
                            InteractionStatus::Rejected
                        }
                        _ => InteractionStatus::Unknown,
                    };
                }
                InteractionStatus::Acknowledged | InteractionStatus::Rejected => {
                    if let Some(operation_id) = record.reply_operation_id.as_deref() {
                        self.reclaim_acknowledged_operation(operation_id, operations)?;
                    }
                    continue;
                }
                InteractionStatus::Unknown | InteractionStatus::Retired => continue,
            }
            let path = self.path(&record.request.request_id)?;
            self.replace(&path, &record)?;
        }
        Ok(())
    }

    pub fn process_retention_batch(&self, operations: &OperationJournal) -> Result<()> {
        for _ in 0..MAX_RETENTION_STEPS {
            let state = self.read_retention_state()?;
            if state.cursor_sequence >= state.next_sequence {
                break;
            }
            let sequence = state.cursor_sequence;
            self.increment_examined()?;
            self.process_retention_item(sequence, operations)?;
            self.advance_cursor(sequence)?;
        }
        Ok(())
    }

    pub fn reclaim_acknowledged_operation(
        &self,
        operation_id: &str,
        operations: &OperationJournal,
    ) -> Result<()> {
        let operation = match operations.get(operation_id)? {
            Some(operation) if operation.method.as_deref() == Some("agent.reply") => operation,
            _ => return Ok(()),
        };
        let Some(request_id) = operation
            .intent
            .as_ref()
            .and_then(|intent| intent.get("native"))
            .and_then(|native| native.get("interaction_request_id"))
            .and_then(Value::as_str)
        else {
            return Ok(());
        };
        let record_path = self.path(request_id)?;
        if !record_path.exists() {
            let tombstone_path = self.tombstone_path(request_id)?;
            if tombstone_path.exists() {
                let tombstone = self.read_tombstone(&tombstone_path, request_id)?;
                if tombstone.reply_operation_id != operation_id {
                    self.note_deferred_damage()?;
                } else {
                    match self.operation_acknowledgement(
                        terminal_identity_from_tombstone(&tombstone),
                        operations,
                    ) {
                        InteractionRetentionDecision::Ready(digest)
                            if digest == tombstone.acknowledged_sha256 => {}
                        decision => self.note_retention_decision(decision)?,
                    }
                }
            } else {
                self.note_deferred_damage()?;
            }
            return Ok(());
        }
        let record = match self.read_path(&record_path) {
            Ok(record) => record,
            Err(_) => {
                self.note_deferred_damage()?;
                return Ok(());
            }
        };
        if record.reply_operation_id.as_deref() != Some(operation_id) {
            self.note_deferred_damage()?;
            return Ok(());
        }
        let Some(sequence) = record.sequence else {
            self.note_deferred_damage()?;
            return Ok(());
        };
        let item_path = self.retention_item_path(sequence);
        let item = match self.read_retention_item(&item_path) {
            Ok(item)
                if item.sequence == sequence
                    && item.request_id == request_id
                    && item.request_sha256 == record.request.request_sha256 =>
            {
                item
            }
            _ => {
                self.note_deferred_damage()?;
                return Ok(());
            }
        };
        self.process_retention_item(item.sequence, operations)
    }

    pub fn retention_status(&self) -> Result<Value> {
        let state = self.read_retention_state()?;
        Ok(json!({
            "schema_version":state.version,
            "degraded":state.degraded,
            "cursor":state.cursor_sequence,
            "counters":{
                "examined":state.examined,
                "compacted":state.compacted,
                "deferred_unknown":state.deferred_unknown,
                "deferred_unacknowledged":state.deferred_unacknowledged,
                "deferred_reference":state.deferred_reference,
                "deferred_damage":state.deferred_damage
            }
        }))
    }

    fn process_retention_item(&self, sequence: u64, operations: &OperationJournal) -> Result<()> {
        let item_path = self.retention_item_path(sequence);
        if !item_path.exists() {
            return Ok(());
        }
        let item = match self.read_retention_item(&item_path) {
            Ok(item) if item.sequence == sequence => item,
            Ok(_) | Err(_) => {
                self.note_deferred_damage()?;
                return Ok(());
            }
        };
        let record_path = self.path(&item.request_id)?;
        let tombstone_path = self.tombstone_path(&item.request_id)?;
        if record_path.exists() {
            let record = match self.read_path(&record_path) {
                Ok(record) => record,
                Err(_) => {
                    self.note_deferred_damage()?;
                    return Ok(());
                }
            };
            if record.sequence != Some(sequence)
                || record.request.request_id != item.request_id
                || request_key(&record.request.request_id) != item.request_key
                || record.request.request_sha256 != item.request_sha256
            {
                self.note_deferred_damage()?;
                return Ok(());
            }
            let acknowledged_sha256 =
                match self.operation_acknowledgement(terminal_identity(&record), operations) {
                    InteractionRetentionDecision::Ready(digest) => digest,
                    decision => {
                        self.note_retention_decision(decision)?;
                        return Ok(());
                    }
                };
            let tombstone = interaction_tombstone(&record, &acknowledged_sha256)?;
            let newly_written = self.write_tombstone(&tombstone)?;
            let readback = self.read_tombstone(&tombstone_path, &item.request_id)?;
            if readback != tombstone {
                self.note_deferred_damage()?;
                return Ok(());
            }
            let saved = self.read_path(&record_path)?;
            if saved != record || !readback.matches_record(&saved) {
                self.note_deferred_damage()?;
                return Ok(());
            }
            remove_interaction_file(&record_path)?;
            self.remove_retention_item_after_marker(&item, &readback)?;
            if newly_written {
                self.increment_compacted()?;
            }
            return Ok(());
        }
        if !tombstone_path.exists() {
            self.note_deferred_damage()?;
            return Ok(());
        }
        let tombstone = match self.read_tombstone(&tombstone_path, &item.request_id) {
            Ok(tombstone)
                if tombstone.sequence == item.sequence
                    && tombstone.request_key == item.request_key
                    && tombstone.request_sha256 == item.request_sha256 =>
            {
                tombstone
            }
            _ => {
                self.note_deferred_damage()?;
                return Ok(());
            }
        };
        match self
            .operation_acknowledgement(terminal_identity_from_tombstone(&tombstone), operations)
        {
            InteractionRetentionDecision::Ready(acknowledged_sha256)
                if acknowledged_sha256 == tombstone.acknowledged_sha256 =>
            {
                self.remove_retention_item_after_marker(&item, &tombstone)?;
            }
            decision => self.note_retention_decision(decision)?,
        }
        Ok(())
    }

    fn operation_acknowledgement(
        &self,
        identity: InteractionTerminalIdentity<'_>,
        operations: &OperationJournal,
    ) -> InteractionRetentionDecision {
        if !matches!(
            identity.status,
            InteractionStatus::Acknowledged | InteractionStatus::Rejected
        ) {
            return match identity.status {
                InteractionStatus::Unknown => InteractionRetentionDecision::Unknown,
                _ => InteractionRetentionDecision::Unacknowledged,
            };
        }
        let (Some(operation_id), Some(reply_sha256)) =
            (identity.reply_operation_id, identity.reply_sha256)
        else {
            return InteractionRetentionDecision::Damage;
        };
        let operation = match operations.get(operation_id) {
            Ok(Some(operation)) => operation,
            Ok(None) | Err(_) => return InteractionRetentionDecision::Damage,
        };
        let Some(outcome) = operation.outcome.as_ref() else {
            return InteractionRetentionDecision::Unknown;
        };
        let Some(outcome_sha256) = operation.outcome_sha256.as_deref() else {
            return InteractionRetentionDecision::Damage;
        };
        if operation.acknowledged_sha256.as_deref() != Some(outcome_sha256) {
            return InteractionRetentionDecision::Unacknowledged;
        }
        let Some(receipt) = operation.receipt.as_ref() else {
            return InteractionRetentionDecision::Damage;
        };
        let Some(intent) = operation.intent.as_ref() else {
            return InteractionRetentionDecision::Damage;
        };
        let native = &intent["native"];
        if operation.operation_id != operation_id
            || operation.method.as_deref() != Some("agent.reply")
            || receipt.operation_id != operation_id
            || receipt.binding_id != identity.binding_id
            || receipt.binding_generation != identity.generation
            || intent["bridge_boot_id"] != identity.bridge_boot_id
            || native["native_root_id"] != identity.native_root_id
            || native["native_scope_key"] != identity.native_scope_key
            || native["interaction_request_id"] != identity.request_id
            || native["interaction_kind"] != identity.kind.as_str()
            || native["tool_name"] != identity.tool_name
            || native["request_sha256"] != identity.request_sha256
            || native["reply_sha256"] != reply_sha256
            || outcome["operation_id"] != operation_id
        {
            return InteractionRetentionDecision::Damage;
        }
        let expected_outcome = match identity.status {
            InteractionStatus::Acknowledged => "applied",
            InteractionStatus::Rejected => "rejected",
            _ => return InteractionRetentionDecision::Damage,
        };
        if outcome["outcome"] == "unknown" || outcome["outcome"] == "accepted" {
            return InteractionRetentionDecision::Unknown;
        }
        if outcome["outcome"] != expected_outcome
            || (identity.status == InteractionStatus::Acknowledged
                && (outcome["details"]["completion_condition"] != "callback_reply_acknowledged"
                    || outcome["details"]["callback_reply_acknowledged"] != true
                    || outcome["details"]["interaction_request_id"] != identity.request_id
                    || outcome["details"]["reply_sha256"] != reply_sha256))
            || (identity.status == InteractionStatus::Rejected
                && outcome["details"]["completion_condition"] != "rejected_before_native_admission")
        {
            return InteractionRetentionDecision::Damage;
        }
        if !operation.compacted {
            return InteractionRetentionDecision::Referenced;
        }
        if !valid_sha256(outcome_sha256) {
            return InteractionRetentionDecision::Damage;
        }
        InteractionRetentionDecision::Ready(outcome_sha256.to_owned())
    }

    fn note_retention_decision(&self, decision: InteractionRetentionDecision) -> Result<()> {
        match decision {
            InteractionRetentionDecision::Ready(_) => Ok(()),
            InteractionRetentionDecision::Unknown => self.note_deferred_unknown(),
            InteractionRetentionDecision::Unacknowledged => self.note_deferred_unacknowledged(),
            InteractionRetentionDecision::Referenced => self.note_deferred_reference(),
            InteractionRetentionDecision::Damage => self.note_deferred_damage(),
        }
    }

    fn ensure_retention_state(&self) -> Result<()> {
        let path = self.retention_state_path();
        if path.exists() {
            self.read_retention_state()?;
            return Ok(());
        }
        let state = InteractionRetentionState {
            version: 1,
            next_sequence: 0,
            cursor_sequence: 0,
            migration_complete: false,
            examined: 0,
            compacted: 0,
            deferred_unknown: 0,
            deferred_unacknowledged: 0,
            deferred_reference: 0,
            deferred_damage: 0,
            degraded: false,
        };
        write_private_new(&path, &serde_json::to_vec(&state)?).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention cursor cannot be initialized",
            )
        })?;
        if self.read_retention_state()? != state {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention cursor failed exact initialization readback",
            ));
        }
        Ok(())
    }

    fn migrate_existing_records(&self) -> Result<()> {
        if self.read_retention_state()?.migration_complete {
            return Ok(());
        }
        for mut record in self.read_all()? {
            let sequence = match record.sequence {
                Some(sequence) => {
                    self.raise_sequence_high_water(sequence)?;
                    sequence
                }
                None => {
                    let sequence = self.allocate_sequence()?;
                    record.sequence = Some(sequence);
                    let path = self.path(&record.request.request_id)?;
                    self.replace(&path, &record)?;
                    sequence
                }
            };
            let item = InteractionRetentionItem {
                version: 1,
                sequence,
                request_id: record.request.request_id.clone(),
                request_key: request_key(&record.request.request_id),
                request_sha256: record.request.request_sha256.clone(),
            };
            self.write_retention_item(&item, true)?;
            self.increment_examined()?;
        }
        let mut state = self.read_retention_state()?;
        state.migration_complete = true;
        self.write_retention_state(&state)
    }

    fn raise_sequence_high_water(&self, sequence: u64) -> Result<()> {
        let next_sequence = sequence.checked_add(1).ok_or_else(|| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction sequence is exhausted",
            )
        })?;
        let mut state = self.read_retention_state()?;
        if state.next_sequence < next_sequence {
            state.next_sequence = next_sequence;
            self.write_retention_state(&state)?;
        }
        Ok(())
    }

    fn allocate_sequence(&self) -> Result<u64> {
        let mut state = self.read_retention_state()?;
        let sequence = state.next_sequence;
        state.next_sequence = sequence.checked_add(1).ok_or_else(|| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction sequence is exhausted",
            )
        })?;
        self.write_retention_state(&state)?;
        Ok(sequence)
    }

    fn read_retention_state(&self) -> Result<InteractionRetentionState> {
        let path = self.retention_state_path();
        let bytes = read_interaction_limited(&path, MAX_RETENTION_BYTES)?;
        let state: InteractionRetentionState = serde_json::from_slice(&bytes).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention cursor is malformed",
            )
        })?;
        if state.version != 1 || state.cursor_sequence > state.next_sequence {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention cursor identity or sequence is invalid",
            ));
        }
        Ok(state)
    }

    fn write_retention_state(&self, state: &InteractionRetentionState) -> Result<()> {
        if state.version != 1 || state.cursor_sequence > state.next_sequence {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention cursor cannot move outside its sequence",
            ));
        }
        let bytes = serde_json::to_vec(state)?;
        if bytes.len() > MAX_RETENTION_BYTES {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention counters exceed their size boundary",
            ));
        }
        replace_private_durable(&self.retention_state_path(), &bytes).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention cursor cannot be durably updated",
            )
        })?;
        if self.read_retention_state()? != *state {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention cursor failed exact durable readback",
            ));
        }
        Ok(())
    }

    fn advance_cursor(&self, sequence: u64) -> Result<()> {
        let mut state = self.read_retention_state()?;
        if state.cursor_sequence == sequence {
            state.cursor_sequence = sequence.saturating_add(1);
            self.write_retention_state(&state)?;
        }
        Ok(())
    }

    fn update_retention_counters(
        &self,
        update: impl FnOnce(&mut InteractionRetentionState),
    ) -> Result<()> {
        let mut state = self.read_retention_state()?;
        update(&mut state);
        if state.deferred_unknown > 0
            || state.deferred_unacknowledged > 0
            || state.deferred_reference > 0
            || state.deferred_damage > 0
        {
            state.degraded = true;
        }
        self.write_retention_state(&state)
    }

    fn increment_examined(&self) -> Result<()> {
        self.update_retention_counters(|state| state.examined = state.examined.saturating_add(1))
    }

    fn increment_compacted(&self) -> Result<()> {
        self.update_retention_counters(|state| state.compacted = state.compacted.saturating_add(1))
    }

    fn note_deferred_unknown(&self) -> Result<()> {
        self.update_retention_counters(|state| {
            state.deferred_unknown = state.deferred_unknown.saturating_add(1)
        })
    }

    fn note_deferred_unacknowledged(&self) -> Result<()> {
        self.update_retention_counters(|state| {
            state.deferred_unacknowledged = state.deferred_unacknowledged.saturating_add(1)
        })
    }

    fn note_deferred_reference(&self) -> Result<()> {
        self.update_retention_counters(|state| {
            state.deferred_reference = state.deferred_reference.saturating_add(1)
        })
    }

    fn note_deferred_damage(&self) -> Result<()> {
        self.update_retention_counters(|state| {
            state.deferred_damage = state.deferred_damage.saturating_add(1)
        })
    }

    fn write_retention_item(&self, item: &InteractionRetentionItem, create: bool) -> Result<()> {
        let path = self.retention_item_path(item.sequence);
        if create && path.exists() {
            if self.read_retention_item(&path)? == *item {
                return Ok(());
            }
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction cursor item conflicts with its exact request identity",
            ));
        }
        let bytes = serde_json::to_vec(item)?;
        let result = if create {
            write_private_new(&path, &bytes)
        } else {
            replace_private_durable(&path, &bytes)
        };
        result.map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction cursor item cannot be durably saved",
            )
        })?;
        if self.read_retention_item(&path)? != *item {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction cursor item failed exact durable readback",
            ));
        }
        Ok(())
    }

    fn read_retention_item(&self, path: &Path) -> Result<InteractionRetentionItem> {
        let bytes = read_interaction_limited(path, MAX_RETENTION_BYTES)?;
        let item: InteractionRetentionItem = serde_json::from_slice(&bytes).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction cursor item is malformed",
            )
        })?;
        if item.version != 1
            || item.sequence == u64::MAX
            || !valid_identity(&item.request_id, 128)
            || item.request_key != request_key(&item.request_id)
            || !valid_sha256(&item.request_sha256)
        {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction cursor item identity is invalid",
            ));
        }
        Ok(item)
    }

    fn write_tombstone(&self, tombstone: &InteractionTombstone) -> Result<bool> {
        let path = self.tombstone_path(&tombstone.request_id)?;
        if path.exists() {
            let saved = self.read_tombstone(&path, &tombstone.request_id)?;
            if saved != *tombstone {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_INTERACTION_RETENTION",
                    "callback compact marker conflicts with exact terminal evidence",
                ));
            }
            return Ok(false);
        }
        let bytes = serde_json::to_vec(tombstone)?;
        if bytes.len() > MAX_RETENTION_BYTES {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "callback compact marker exceeds its size boundary",
            ));
        }
        write_private_new(&path, &bytes).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "callback compact marker cannot be durably written",
            )
        })?;
        if self.read_tombstone(&path, &tombstone.request_id)? != *tombstone {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "callback compact marker failed exact durable readback",
            ));
        }
        Ok(true)
    }

    fn read_tombstone(
        &self,
        path: &Path,
        expected_request_id: &str,
    ) -> Result<InteractionTombstone> {
        let bytes = read_interaction_limited(path, MAX_RETENTION_BYTES)?;
        let tombstone: InteractionTombstone = serde_json::from_slice(&bytes).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "callback compact marker is malformed",
            )
        })?;
        if tombstone.version != 1
            || tombstone.sequence == u64::MAX
            || tombstone.request_id != expected_request_id
            || tombstone.request_key != request_key(expected_request_id)
            || path.file_stem().and_then(|value| value.to_str())
                != Some(tombstone.request_key.as_str())
            || !valid_identity(&tombstone.binding_id, 256)
            || tombstone.generation < 1
            || !valid_identity(&tombstone.native_scope_key, 512)
            || !valid_identity(&tombstone.bridge_boot_id, 256)
            || !valid_identity(&tombstone.native_root_id, 512)
            || !valid_identity(&tombstone.tool_name, 256)
            || !valid_sha256(&tombstone.request_sha256)
            || !valid_identity(&tombstone.reply_operation_id, 256)
            || !valid_sha256(&tombstone.reply_sha256)
            || !valid_sha256(&tombstone.outcome_sha256)
            || tombstone.acknowledged_sha256 != tombstone.outcome_sha256
            || !matches!(
                tombstone.status,
                InteractionStatus::Acknowledged | InteractionStatus::Rejected
            )
        {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "callback compact marker identity or acknowledgement is invalid",
            ));
        }
        Ok(tombstone)
    }

    fn remove_retention_item_after_marker(
        &self,
        item: &InteractionRetentionItem,
        tombstone: &InteractionTombstone,
    ) -> Result<()> {
        let marker_path = self.tombstone_path(&tombstone.request_id)?;
        if self.read_tombstone(&marker_path, &tombstone.request_id)? != *tombstone {
            self.note_deferred_damage()?;
            return Ok(());
        }
        if tombstone.sequence != item.sequence
            || tombstone.request_id != item.request_id
            || tombstone.request_key != item.request_key
            || tombstone.request_sha256 != item.request_sha256
        {
            self.note_deferred_damage()?;
            return Ok(());
        }
        self.remove_pending_entry_for(
            &tombstone.bridge_boot_id,
            &tombstone.native_root_id,
            &tombstone.request_id,
            &tombstone.request_sha256,
        )?;
        let path = self.retention_item_path(item.sequence);
        if !path.exists() {
            return Ok(());
        }
        if self.read_retention_item(&path)? != *item {
            self.note_deferred_damage()?;
            return Ok(());
        }
        remove_interaction_file(&path)
    }

    fn retention_state_path(&self) -> PathBuf {
        self.retention_directory.join("state.json")
    }

    fn retention_item_path(&self, sequence: u64) -> PathBuf {
        self.retention_directory
            .join("items")
            .join(format!("{sequence:020}.json"))
    }

    fn tombstone_path(&self, request_id: &str) -> Result<PathBuf> {
        if !valid_identity(request_id, 128) {
            return Err(Error::new(
                "ADAPTER_INTERACTION_IDENTITY",
                "callback request identity is invalid",
            ));
        }
        Ok(self
            .retention_directory
            .join("tombstones")
            .join(format!("{}.json", request_key(request_id))))
    }

    fn pending_index_path(&self, bridge_boot_id: &str, native_root_id: &str) -> Result<PathBuf> {
        if !valid_identity(bridge_boot_id, 256) || !valid_identity(native_root_id, 512) {
            return Err(Error::new(
                "ADAPTER_INTERACTION_IDENTITY",
                "pending callback index identity is invalid",
            ));
        }
        Ok(self.retention_directory.join("pending").join(format!(
            "{}-{}.json",
            request_key(bridge_boot_id),
            request_key(native_root_id)
        )))
    }

    fn pending_records_for_root(
        &self,
        bridge_boot_id: &str,
        native_root_id: &str,
    ) -> Result<Vec<InteractionRecord>> {
        let mut index = self.read_pending_index(bridge_boot_id, native_root_id)?;
        let original_entries = index.entries.clone();
        let entries = std::mem::take(&mut index.entries);
        let mut retained = Vec::with_capacity(entries.len());
        let mut records = Vec::with_capacity(entries.len());
        for entry in entries {
            let record_path = self.path(&entry.request_id)?;
            if record_path.exists() {
                let record = match self.read_path(&record_path) {
                    Ok(record) => record,
                    Err(error) => {
                        self.note_deferred_damage()?;
                        return Err(error);
                    }
                };
                if record.request.request_id != entry.request_id
                    || record.request.request_sha256 != entry.request_sha256
                    || record.request.bridge_boot_id != bridge_boot_id
                    || record.request.native_root_id != native_root_id
                {
                    self.note_deferred_damage()?;
                    return Err(Error::new(
                        "ADAPTER_INTERACTION_RETENTION",
                        "pending callback index differs from its exact durable request",
                    ));
                }
                if record.status == InteractionStatus::Pending {
                    retained.push(entry);
                    records.push(record);
                }
                continue;
            }

            let marker_path = self.tombstone_path(&entry.request_id)?;
            if !marker_path.exists() {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_INTERACTION_RETENTION",
                    "pending callback index points to missing request evidence",
                ));
            }
            let tombstone = match self.read_tombstone(&marker_path, &entry.request_id) {
                Ok(tombstone) => tombstone,
                Err(error) => {
                    self.note_deferred_damage()?;
                    return Err(error);
                }
            };
            if tombstone.request_sha256 != entry.request_sha256
                || tombstone.bridge_boot_id != bridge_boot_id
                || tombstone.native_root_id != native_root_id
            {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_INTERACTION_RETENTION",
                    "pending callback index differs from its compact terminal marker",
                ));
            }
        }
        if original_entries != retained {
            index.entries = retained;
            self.write_pending_index(&index)?;
        }
        records.sort_by(|left, right| left.request.request_id.cmp(&right.request.request_id));
        Ok(records)
    }

    fn read_pending_index(
        &self,
        bridge_boot_id: &str,
        native_root_id: &str,
    ) -> Result<PendingInteractionIndex> {
        let path = self.pending_index_path(bridge_boot_id, native_root_id)?;
        if !path.exists() {
            return Ok(PendingInteractionIndex {
                version: 1,
                bridge_boot_id: bridge_boot_id.to_owned(),
                native_root_id: native_root_id.to_owned(),
                entries: Vec::new(),
            });
        }
        let bytes = match read_interaction_limited(&path, MAX_RETENTION_BYTES) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.note_deferred_damage()?;
                return Err(error);
            }
        };
        let index: PendingInteractionIndex = match serde_json::from_slice(&bytes) {
            Ok(index) => index,
            Err(_) => {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_INTERACTION_RETENTION",
                    "pending callback index is malformed",
                ));
            }
        };
        let expected_file_name = format!(
            "{}-{}.json",
            request_key(bridge_boot_id),
            request_key(native_root_id)
        );
        let mut ids = std::collections::BTreeSet::new();
        if index.version != 1
            || index.bridge_boot_id != bridge_boot_id
            || index.native_root_id != native_root_id
            || index.entries.len() > MAX_PENDING_INTERACTIONS
            || index.entries.iter().any(|entry| {
                !valid_identity(&entry.request_id, 128)
                    || entry.request_key != request_key(&entry.request_id)
                    || !valid_sha256(&entry.request_sha256)
                    || !ids.insert(entry.request_id.as_str())
            })
            || path.file_name().and_then(|name| name.to_str()) != Some(expected_file_name.as_str())
        {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "pending callback index identity or bounds are invalid",
            ));
        }
        Ok(index)
    }

    fn write_pending_index(&self, index: &PendingInteractionIndex) -> Result<()> {
        let path = self.pending_index_path(&index.bridge_boot_id, &index.native_root_id)?;
        if index.entries.len() > MAX_PENDING_INTERACTIONS {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_CAPACITY",
                "pending callback index exceeds its item boundary",
            ));
        }
        if index.entries.is_empty() {
            if path.exists() {
                remove_interaction_file(&path)?;
            }
            if path.exists() {
                self.note_deferred_damage()?;
                return Err(Error::new(
                    "ADAPTER_INTERACTION_RETENTION",
                    "empty pending callback index remains after durable removal",
                ));
            }
            return Ok(());
        }
        let bytes = serde_json::to_vec(index)?;
        if bytes.len() > MAX_RETENTION_BYTES {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "pending callback index exceeds its byte boundary",
            ));
        }
        let result = if path.exists() {
            replace_private_durable(&path, &bytes)
        } else {
            write_private_new(&path, &bytes)
        };
        result.map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "pending callback index cannot be durably saved",
            )
        })?;
        if self.read_pending_index(&index.bridge_boot_id, &index.native_root_id)? != *index {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "pending callback index failed exact durable readback",
            ));
        }
        Ok(())
    }

    fn add_pending_entry(&self, request: &InteractionRequest) -> Result<()> {
        let records =
            self.pending_records_for_root(&request.bridge_boot_id, &request.native_root_id)?;
        if let Some(existing) = records
            .iter()
            .find(|record| record.request.request_id == request.request_id)
        {
            if existing.request.request_sha256 == request.request_sha256 {
                return Ok(());
            }
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "pending callback request identity conflicts with its index",
            ));
        }
        if records.len() >= MAX_PENDING_INTERACTIONS {
            return Err(Error::new(
                "ADAPTER_INTERACTION_CAPACITY",
                "too many Claude SDK callbacks are waiting for a reply",
            ));
        }
        let mut index =
            self.read_pending_index(&request.bridge_boot_id, &request.native_root_id)?;
        if index
            .entries
            .iter()
            .any(|entry| entry.request_id == request.request_id)
        {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "pending callback index contains an unresolved duplicate identity",
            ));
        }
        index.entries.push(PendingInteractionIndexEntry {
            request_id: request.request_id.clone(),
            request_key: request_key(&request.request_id),
            request_sha256: request.request_sha256.clone(),
        });
        index
            .entries
            .sort_by(|left, right| left.request_id.cmp(&right.request_id));
        self.write_pending_index(&index)
    }

    fn remove_pending_entry_for(
        &self,
        bridge_boot_id: &str,
        native_root_id: &str,
        request_id: &str,
        request_sha256: &str,
    ) -> Result<()> {
        let mut index = self.read_pending_index(bridge_boot_id, native_root_id)?;
        let Some(position) = index
            .entries
            .iter()
            .position(|entry| entry.request_id == request_id)
        else {
            return Ok(());
        };
        if index.entries[position].request_sha256 != request_sha256 {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "pending callback removal differs from its exact request digest",
            ));
        }
        index.entries.remove(position);
        self.write_pending_index(&index)
    }

    fn replace(&self, path: &Path, record: &InteractionRecord) -> Result<()> {
        replace_private_durable(path, &encode_record(record)?).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "callback state transition cannot be durably saved",
            )
        })?;
        if self.read_path(path)? != *record {
            self.note_deferred_damage()?;
            return Err(Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "callback state transition failed exact durable readback",
            ));
        }
        if record.status != InteractionStatus::Pending {
            self.remove_pending_entry_for(
                &record.request.bridge_boot_id,
                &record.request.native_root_id,
                &record.request.request_id,
                &record.request.request_sha256,
            )?;
        }
        Ok(())
    }

    fn read_all(&self) -> Result<Vec<InteractionRecord>> {
        let entries = fs::read_dir(&self.directory).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "interactive state directory cannot be listed",
            )
        })?;
        let mut records = Vec::new();
        let mut entry_count = 0usize;
        for entry in entries {
            let entry = entry.map_err(|_| {
                Error::new(
                    "ADAPTER_INTERACTION_JOURNAL",
                    "interactive state entry cannot be read",
                )
            })?;
            entry_count = entry_count.saturating_add(1);
            if entry_count > MAX_INTERACTION_FILES * 2 {
                return Err(Error::new(
                    "ADAPTER_INTERACTION_CAPACITY",
                    "interactive state directory exceeds its bounded entry count",
                ));
            }
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            records.push(self.read_path(&path)?);
            if records.len() > MAX_INTERACTION_FILES {
                return Err(Error::new(
                    "ADAPTER_INTERACTION_CAPACITY",
                    "interactive history exceeds its bounded record count",
                ));
            }
        }
        Ok(records)
    }

    fn ensure_bounded_history(&self) -> Result<()> {
        self.read_all()?;
        self.ensure_retention_state()?;
        self.migrate_existing_records()
    }

    fn read_path(&self, path: &Path) -> Result<InteractionRecord> {
        let metadata = fs::symlink_metadata(path).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "callback state file cannot be inspected",
            )
        })?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() > MAX_RECORD_BYTES as u64
        {
            return Err(Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "callback state file violates its type or size boundary",
            ));
        }
        let mut file = File::open(path).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "callback state file cannot be read",
            )
        })?;
        let mut bytes = Vec::with_capacity(metadata.len() as usize);
        file.by_ref()
            .take(MAX_RECORD_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| {
                Error::new(
                    "ADAPTER_INTERACTION_JOURNAL",
                    "callback state file cannot be read",
                )
            })?;
        let record: InteractionRecord = serde_json::from_slice(&bytes).map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "callback state record is malformed",
            )
        })?;
        let file_id = path.file_stem().and_then(|value| value.to_str());
        if record.schema_version != 1
            || record.request.schema_version != 1
            || file_id != Some(digest_bytes(record.request.request_id.as_bytes()).as_str())
            || record.request.binding_id != self.binding_id
            || record.request.generation != self.generation
            || record.request.native_scope_key != self.native_scope_key
        {
            return Err(Error::new(
                "ADAPTER_INTERACTION_IDENTITY",
                "callback state belongs to another binding or native scope",
            ));
        }
        validate_request(&record.request)?;
        if record.reply.is_some() != record.reply_sha256.is_some()
            || record.reply.is_some() != record.reply_operation_id.is_some()
            || record.reply.as_ref().is_some_and(|reply| {
                digest_json(reply).ok().as_deref() != record.reply_sha256.as_deref()
            })
            || (record.status != InteractionStatus::Pending
                && record.status != InteractionStatus::Retired
                && record.reply_operation_id.is_none())
        {
            return Err(Error::new(
                "ADAPTER_INTERACTION_JOURNAL",
                "callback state has inconsistent reply evidence",
            ));
        }
        Ok(record)
    }

    fn path(&self, request_id: &str) -> Result<PathBuf> {
        if !valid_identity(request_id, 128) {
            return Err(Error::new(
                "ADAPTER_INTERACTION_IDENTITY",
                "callback request identity is invalid",
            ));
        }
        Ok(self
            .directory
            .join(format!("{}.json", digest_bytes(request_id.as_bytes()))))
    }
}

impl InteractionTombstone {
    fn matches_request(&self, request: &InteractionRequest) -> bool {
        self.request_id == request.request_id
            && self.request_key == request_key(&request.request_id)
            && self.binding_id == request.binding_id
            && self.generation == request.generation
            && self.native_scope_key == request.native_scope_key
            && self.bridge_boot_id == request.bridge_boot_id
            && self.native_root_id == request.native_root_id
            && self.kind == request.kind
            && self.tool_name == request.tool_name
            && self.request_sha256 == request.request_sha256
    }

    fn matches_record(&self, record: &InteractionRecord) -> bool {
        Some(self.sequence) == record.sequence
            && self.request_id == record.request.request_id
            && self.request_key == request_key(&record.request.request_id)
            && self.binding_id == record.request.binding_id
            && self.generation == record.request.generation
            && self.native_scope_key == record.request.native_scope_key
            && self.bridge_boot_id == record.request.bridge_boot_id
            && self.native_root_id == record.request.native_root_id
            && self.kind == record.request.kind
            && self.tool_name == record.request.tool_name
            && self.request_sha256 == record.request.request_sha256
            && self.status == record.status
            && Some(self.reply_operation_id.as_str()) == record.reply_operation_id.as_deref()
            && Some(self.reply_sha256.as_str()) == record.reply_sha256.as_deref()
    }
}

fn terminal_identity(record: &InteractionRecord) -> InteractionTerminalIdentity<'_> {
    InteractionTerminalIdentity {
        request_id: &record.request.request_id,
        request_sha256: &record.request.request_sha256,
        binding_id: &record.request.binding_id,
        generation: record.request.generation,
        native_scope_key: &record.request.native_scope_key,
        bridge_boot_id: &record.request.bridge_boot_id,
        native_root_id: &record.request.native_root_id,
        kind: record.request.kind,
        tool_name: &record.request.tool_name,
        status: record.status,
        reply_operation_id: record.reply_operation_id.as_deref(),
        reply_sha256: record.reply_sha256.as_deref(),
    }
}

fn terminal_identity_from_tombstone(
    tombstone: &InteractionTombstone,
) -> InteractionTerminalIdentity<'_> {
    InteractionTerminalIdentity {
        request_id: &tombstone.request_id,
        request_sha256: &tombstone.request_sha256,
        binding_id: &tombstone.binding_id,
        generation: tombstone.generation,
        native_scope_key: &tombstone.native_scope_key,
        bridge_boot_id: &tombstone.bridge_boot_id,
        native_root_id: &tombstone.native_root_id,
        kind: tombstone.kind,
        tool_name: &tombstone.tool_name,
        status: tombstone.status,
        reply_operation_id: Some(&tombstone.reply_operation_id),
        reply_sha256: Some(&tombstone.reply_sha256),
    }
}

fn interaction_tombstone(
    record: &InteractionRecord,
    acknowledged_sha256: &str,
) -> Result<InteractionTombstone> {
    let sequence = record.sequence.ok_or_else(|| {
        Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "callback record has no stable retention sequence",
        )
    })?;
    let reply_operation_id = record.reply_operation_id.as_deref().ok_or_else(|| {
        Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "terminal callback has no exact reply operation identity",
        )
    })?;
    let reply_sha256 = record.reply_sha256.as_deref().ok_or_else(|| {
        Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "terminal callback has no exact reply digest",
        )
    })?;
    if !matches!(
        record.status,
        InteractionStatus::Acknowledged | InteractionStatus::Rejected
    ) || !valid_sha256(acknowledged_sha256)
    {
        return Err(Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "callback compaction lacks exact terminal acknowledgement evidence",
        ));
    }
    Ok(InteractionTombstone {
        version: 1,
        sequence,
        request_id: record.request.request_id.clone(),
        request_key: request_key(&record.request.request_id),
        binding_id: record.request.binding_id.clone(),
        generation: record.request.generation,
        native_scope_key: record.request.native_scope_key.clone(),
        bridge_boot_id: record.request.bridge_boot_id.clone(),
        native_root_id: record.request.native_root_id.clone(),
        kind: record.request.kind,
        tool_name: record.request.tool_name.clone(),
        request_sha256: record.request.request_sha256.clone(),
        status: record.status,
        reply_operation_id: reply_operation_id.to_owned(),
        reply_sha256: reply_sha256.to_owned(),
        outcome_sha256: acknowledged_sha256.to_owned(),
        acknowledged_sha256: acknowledged_sha256.to_owned(),
    })
}

fn request_key(request_id: &str) -> String {
    digest_bytes(request_id.as_bytes())
}

fn ensure_interaction_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention path is not a real directory",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).map_err(|_| {
                Error::new(
                    "ADAPTER_INTERACTION_RETENTION",
                    "interaction retention directory cannot be created",
                )
            })?;
        }
        Err(_) => {
            return Err(Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention directory cannot be inspected",
            ));
        }
    }
    private_permissions(path, true)
}

fn read_interaction_limited(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "interaction retention file cannot be inspected",
        )
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > maximum as u64 {
        return Err(Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "interaction retention file violates its type or size boundary",
        ));
    }
    let mut file = File::open(path).map_err(|_| {
        Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "interaction retention file cannot be read",
        )
    })?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| {
            Error::new(
                "ADAPTER_INTERACTION_RETENTION",
                "interaction retention file read failed",
            )
        })?;
    if bytes.len() > maximum {
        return Err(Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "interaction retention file exceeds its size boundary",
        ));
    }
    Ok(bytes)
}

fn remove_interaction_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "interaction cleanup target cannot be inspected",
        )
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "interaction cleanup target is not a regular file",
        ));
    }
    remove_private_durable(path).map(|_| ()).map_err(|_| {
        Error::new(
            "ADAPTER_INTERACTION_RETENTION",
            "interaction evidence cannot be removed",
        )
    })
}

fn request_payload(request: &InteractionRequest) -> Result<Value> {
    Ok(json!({
        "schema_version":request.schema_version,
        "request_id":request.request_id,
        "kind":request.kind,
        "tool_name":request.tool_name,
        "input":request.input
    }))
}

fn validate_request(request: &InteractionRequest) -> Result<()> {
    let input_bytes = serde_json::to_vec(&request.input)?.len();
    let digest = digest_json(&request_payload(request)?)?;
    if request.schema_version != 1
        || !valid_identity(&request.binding_id, 256)
        || request.generation < 1
        || !valid_identity(&request.native_scope_key, 512)
        || !valid_identity(&request.bridge_boot_id, 256)
        || !valid_identity(&request.native_root_id, 512)
        || !valid_identity(&request.request_id, 128)
        || !valid_identity(&request.tool_name, 256)
        || !request.input.is_object()
        || input_bytes > MAX_REQUEST_BYTES
        || (request.kind == InteractionKind::Question && request.tool_name != "AskUserQuestion")
        || (request.kind == InteractionKind::Permission && request.tool_name == "AskUserQuestion")
        || (request.kind == InteractionKind::Question && !valid_question_input(&request.input))
        || !valid_sha256(&request.request_sha256)
        || request.request_sha256 != digest
    {
        return Err(Error::new(
            "SDK_INTERACTION_SCHEMA",
            "Claude SDK callback request violates its typed boundary",
        ));
    }
    Ok(())
}

fn valid_question_input(input: &Value) -> bool {
    let Some(questions) = input.get("questions").and_then(Value::as_array) else {
        return false;
    };
    !questions.is_empty()
        && questions.len() <= 16
        && questions.iter().all(|question| {
            let Some(question_text) = question.get("question").and_then(Value::as_str) else {
                return false;
            };
            let Some(header) = question.get("header").and_then(Value::as_str) else {
                return false;
            };
            let Some(options) = question.get("options").and_then(Value::as_array) else {
                return false;
            };
            question["multiSelect"].is_boolean()
                && valid_identity(question_text, 4_096)
                && valid_identity(header, 12)
                && (2..=4).contains(&options.len())
                && options.iter().all(|option| {
                    option
                        .get("label")
                        .and_then(Value::as_str)
                        .is_some_and(|label| valid_identity(label, 256))
                        && option
                            .get("description")
                            .and_then(Value::as_str)
                            .is_some_and(|description| valid_identity(description, 2_048))
                        && option.get("preview").is_none_or(|preview| {
                            preview.as_str().is_some_and(|value| value.len() <= 4_096)
                        })
                })
        })
}

fn valid_answer_text(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 4_096 && !value.bytes().any(|byte| byte == 0)
}

fn encode_record(record: &InteractionRecord) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(record)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::new(
            "ADAPTER_INTERACTION_CAPACITY",
            "callback state record exceeds its byte boundary",
        ));
    }
    Ok(bytes)
}

fn valid_identity(value: &str, maximum: usize) -> bool {
    !value.trim().is_empty()
        && value.len() <= maximum
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub fn validate_reply(request: &InteractionRequest, reply: &Value) -> Result<()> {
    let reply_bytes = serde_json::to_vec(reply)?.len();
    let Some(object) = reply.as_object() else {
        return Err(Error::new(
            "INTERACTION_REPLY_SCHEMA",
            "callback reply must be a typed object",
        ));
    };
    let reply_type = reply["type"].as_str();
    let valid = match request.kind {
        InteractionKind::Permission => match reply_type {
            Some("permission") if reply["decision"] == "allow" => {
                object
                    .keys()
                    .all(|key| matches!(key.as_str(), "type" | "decision" | "updated_input"))
                    && (reply.get("updated_input").is_none_or(Value::is_object))
            }
            Some("permission") if reply["decision"] == "deny" => {
                object
                    .keys()
                    .all(|key| matches!(key.as_str(), "type" | "decision" | "message"))
                    && reply["message"]
                        .as_str()
                        .is_some_and(|message| valid_identity(message, 1_024))
            }
            _ => false,
        },
        InteractionKind::Question => {
            let input_questions = request.input.get("questions").and_then(Value::as_array);
            let reply_questions = reply["updated_input"]["questions"].as_array();
            let answers = reply["updated_input"]["answers"].as_object();
            let questions_valid = input_questions.is_some_and(|questions| {
                !questions.is_empty()
                    && questions.len() <= 16
                    && reply_questions == Some(questions)
                    && answers.is_some_and(|answers| answers.len() == questions.len())
                    && questions.iter().all(|question| {
                        let Some(text) = question.get("question").and_then(Value::as_str) else {
                            return false;
                        };
                        let Some(answer) = answers.and_then(|answers| answers.get(text)) else {
                            return false;
                        };
                        let multi_select = question["multiSelect"] == true;
                        match answer {
                            Value::String(value) => valid_answer_text(value),
                            Value::Array(values) if multi_select => {
                                !values.is_empty()
                                    && values.len() <= 16
                                    && values
                                        .iter()
                                        .all(|value| value.as_str().is_some_and(valid_answer_text))
                            }
                            _ => false,
                        }
                    })
            });
            request.tool_name == "AskUserQuestion"
                && reply_type == Some("question")
                && object
                    .keys()
                    .all(|key| matches!(key.as_str(), "type" | "updated_input"))
                && reply["updated_input"].as_object().is_some_and(|updated| {
                    updated.len() == 2
                        && updated.contains_key("questions")
                        && updated.contains_key("answers")
                })
                && questions_valid
        }
    };
    if !valid || reply_bytes > MAX_REPLY_BYTES {
        return Err(Error::new(
            "INTERACTION_REPLY_SCHEMA",
            "callback reply does not match the exact pending permission or question",
        ));
    }
    Ok(())
}
