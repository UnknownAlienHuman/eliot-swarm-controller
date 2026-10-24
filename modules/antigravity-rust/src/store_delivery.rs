use std::{
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

    pub fn new_for_current(
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
    status: String,
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
            || !bounded_identity(&self.status, 64)
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
        let response_bytes = u64::try_from(body.body.len())
            .map_err(|_| Error::invalid("retained response length is out of range"))?;
        let record = RetainedBodyRecord {
            schema_version: SCHEMA_VERSION,
            operation_id: operation_id.to_owned(),
            binding_id: self.binding_id.clone(),
            binding_generation: self.binding_generation,
            origin_boot_id: origin_boot_id.to_owned(),
            status: body.status.clone(),
            module_receipt: receipt.clone(),
            native_conversation_id: native_conversation_id.to_owned(),
            result_ordinal,
            response_sha256: body.response_sha256.clone(),
            response_bytes,
            response: body.body.clone(),
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
            status: record.status,
            response_sha256: record.response_sha256,
            body: record.response,
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
    !value.trim().is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
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
