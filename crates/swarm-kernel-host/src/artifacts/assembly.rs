//! Assemble existing pages only. This path cannot ask a vendor for more work.
use super::{ArtifactFiles, ArtifactRecord, MAX_PAGE_BYTES};
use crate::{
    error::{Error, Result},
    model,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
    sync::{Arc, Mutex, OnceLock},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssemblyRequest {
    pub client_request_id: String,
    pub page_refs: Vec<String>,
    #[serde(default)]
    pub expected_sha256: Option<String>,
}
impl AssemblyRequest {
    pub fn validate(&self) -> Result<()> {
        let mut ids = BTreeSet::new();
        if self.client_request_id.trim().is_empty() || self.page_refs.is_empty() {
            return Err(Error::invalid(
                "request ID and a nonempty ordered page_refs list are required",
            ));
        }
        for id in &self.page_refs {
            if id.trim().is_empty() || !ids.insert(id) {
                return Err(Error::invalid(
                    "page_refs must contain distinct nonempty artifact IDs",
                ));
            }
        }
        if self
            .expected_sha256
            .as_ref()
            .is_some_and(|h| h.len() != 64 || !h.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(Error::invalid(
                "expected_sha256 must be a 64-character SHA-256 digest",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultPart {
    pub artifact_ref: String,
    pub offset_bytes: u64,
    pub byte_length: u64,
    pub sha256: String,
}

const MAX_ASSEMBLY_PART_CACHE_ENTRIES: usize = 32;
const MAX_ASSEMBLY_PART_CACHE_BYTES: usize = 8 * 1024 * 1024;
const ASSEMBLY_PART_CHECKPOINT_INTERVAL: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CachedResultPart {
    offset_bytes: u64,
    byte_length: u64,
    sha256: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AssemblyPartCheckpoint {
    part_index: usize,
    offset_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// The Store-provided epoch binds these range checkpoints to one assembled row,
/// database connection and validated proof generation.
struct AssemblyPartCacheKey {
    cache_epoch: String,
    artifact_id: String,
    relative_path: String,
    byte_length: u64,
    content_digest: String,
    part_count: u64,
}

struct AssemblyPartCacheEntry {
    key: AssemblyPartCacheKey,
    checkpoints: Arc<Vec<AssemblyPartCheckpoint>>,
    bytes: usize,
}

static ASSEMBLY_PART_CACHE: OnceLock<Mutex<VecDeque<AssemblyPartCacheEntry>>> = OnceLock::new();

fn assembly_part_cache() -> &'static Mutex<VecDeque<AssemblyPartCacheEntry>> {
    ASSEMBLY_PART_CACHE.get_or_init(|| Mutex::new(VecDeque::new()))
}

fn assembly_part_cache_entry_bytes(entry: &AssemblyPartCacheEntry) -> usize {
    size_of::<AssemblyPartCacheEntry>()
        .saturating_add(size_of::<Vec<AssemblyPartCheckpoint>>())
        .saturating_add(
            entry
                .checkpoints
                .capacity()
                .saturating_mul(size_of::<AssemblyPartCheckpoint>()),
        )
        .saturating_add(entry.key.cache_epoch.capacity())
        .saturating_add(entry.key.artifact_id.capacity())
        .saturating_add(entry.key.relative_path.capacity())
        .saturating_add(entry.key.content_digest.capacity())
        .saturating_add(2 * size_of::<usize>())
}

fn insert_assembly_part_cache_entry(
    cache: &mut VecDeque<AssemblyPartCacheEntry>,
    entry: AssemblyPartCacheEntry,
) -> bool {
    if entry.bytes > MAX_ASSEMBLY_PART_CACHE_BYTES {
        return false;
    }
    let mut total_bytes = cache.iter().fold(0usize, |total, resident| {
        total.saturating_add(resident.bytes)
    });
    while !cache.is_empty()
        && (cache.len() >= MAX_ASSEMBLY_PART_CACHE_ENTRIES
            || total_bytes.saturating_add(entry.bytes) > MAX_ASSEMBLY_PART_CACHE_BYTES)
    {
        let Some(removed) = cache.pop_front() else {
            return false;
        };
        total_bytes = total_bytes.saturating_sub(removed.bytes);
    }
    cache.push_back(entry);
    true
}

fn assembly_part_cache_key(
    record: &ArtifactRecord,
    cache_epoch: &str,
) -> Result<AssemblyPartCacheKey> {
    let part_count = record.metadata["part_count"]
        .as_u64()
        .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "assembly part count is missing"))?;
    Ok(AssemblyPartCacheKey {
        cache_epoch: cache_epoch.to_owned(),
        artifact_id: record.artifact_id.clone(),
        relative_path: record.relative_path.clone(),
        byte_length: record.byte_length,
        content_digest: record.content_digest.clone(),
        part_count,
    })
}

fn cached_assembly_parts_for_range(
    record: &ArtifactRecord,
    cache_epoch: Option<&str>,
    offset: u64,
    requested_end: u64,
) -> Result<Vec<CachedResultPart>> {
    let expected_part_count = record.metadata["part_count"]
        .as_u64()
        .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "assembly part count is missing"))?;
    let part_values = record.metadata["parts"]
        .as_array()
        .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "assembly part manifest is missing"))?;
    if part_values.is_empty()
        || expected_part_count != part_values.len() as u64
        || record.metadata["sha256"] != record.content_digest
    {
        return Err(Error::new(
            "ARTIFACT_DAMAGED",
            "assembly manifest does not cover stored content",
        ));
    }
    let cache_key = cache_epoch
        .map(|epoch| assembly_part_cache_key(record, epoch))
        .transpose()?;
    if let Some(cache_key) = cache_key.as_ref()
        && let Ok(mut cache) = assembly_part_cache().lock()
        && let Some(index) = cache.iter().position(|entry| entry.key == *cache_key)
    {
        let entry = cache.remove(index).expect("located cache entry");
        let checkpoints = Arc::clone(&entry.checkpoints);
        cache.push_back(entry);
        drop(cache);
        return parts_from_checkpoints(part_values, &checkpoints, offset, requested_end);
    }

    #[cfg(test)]
    crate::store::object_scope::count_assembly_part_manifest_decode(&record.artifact_id, 0);
    let checkpoint_count = part_values
        .len()
        .div_ceil(ASSEMBLY_PART_CHECKPOINT_INTERVAL);
    let can_cache = cache_key.as_ref().is_some_and(|key| {
        assembly_part_cache_entry_estimate(key, checkpoint_count) <= MAX_ASSEMBLY_PART_CACHE_BYTES
    });
    let mut checkpoints = if can_cache {
        Vec::with_capacity(checkpoint_count)
    } else {
        Vec::new()
    };
    let mut selected = Vec::new();
    let mut end = 0u64;
    for (part_index, value) in part_values.iter().enumerate() {
        let part = decode_cached_result_part(value)?;
        if part.offset_bytes != end
            || part.byte_length > MAX_PAGE_BYTES as u64
            || (part.byte_length == 0 && !(part_values.len() == 1 && record.byte_length == 0))
        {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "invalid assembly segment manifest",
            ));
        }
        if can_cache && part_index % ASSEMBLY_PART_CHECKPOINT_INTERVAL == 0 {
            checkpoints.push(AssemblyPartCheckpoint {
                part_index,
                offset_bytes: part.offset_bytes,
            });
        }
        let part_end = end
            .checked_add(part.byte_length)
            .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "segment overflow"))?;
        if part.offset_bytes < requested_end && part_end > offset {
            selected.push(part);
        }
        end = part_end;
    }
    if end != record.byte_length {
        return Err(Error::new(
            "ARTIFACT_DAMAGED",
            "assembly manifest does not cover stored content",
        ));
    }
    if can_cache && let Some(key) = cache_key {
        let mut entry = AssemblyPartCacheEntry {
            key,
            checkpoints: Arc::new(checkpoints),
            bytes: 0,
        };
        entry.bytes = assembly_part_cache_entry_bytes(&entry);
        if entry.bytes <= MAX_ASSEMBLY_PART_CACHE_BYTES
            && let Ok(mut cache) = assembly_part_cache().lock()
        {
            cache.retain(|resident| {
                resident.key.artifact_id != entry.key.artifact_id
                    || resident.key.cache_epoch != entry.key.cache_epoch
            });
            insert_assembly_part_cache_entry(&mut cache, entry);
        }
    }
    Ok(selected)
}

fn assembly_part_cache_entry_estimate(key: &AssemblyPartCacheKey, capacity: usize) -> usize {
    size_of::<AssemblyPartCacheEntry>()
        .saturating_add(size_of::<Vec<AssemblyPartCheckpoint>>())
        .saturating_add(capacity.saturating_mul(size_of::<AssemblyPartCheckpoint>()))
        .saturating_add(key.cache_epoch.capacity())
        .saturating_add(key.artifact_id.capacity())
        .saturating_add(key.relative_path.capacity())
        .saturating_add(key.content_digest.capacity())
        .saturating_add(2 * size_of::<usize>())
}

fn decode_cached_result_part(value: &Value) -> Result<CachedResultPart> {
    if let Some(fields) = value.as_array() {
        if fields.len() != 4 {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "compact part entry has unknown or missing fields",
            ));
        }
        fields[0]
            .as_str()
            .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "part reference is malformed"))?;
        let offset_bytes = fields[1]
            .as_u64()
            .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "part offset is malformed"))?;
        let byte_length = fields[2]
            .as_u64()
            .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "part length is malformed"))?;
        let sha256 = fields[3]
            .as_str()
            .and_then(parse_sha256)
            .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "part digest is malformed"))?;
        return Ok(CachedResultPart {
            offset_bytes,
            byte_length,
            sha256,
        });
    }
    let fields = value
        .as_object()
        .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "part entry is malformed"))?;
    if fields.len() != 4
        || fields.keys().any(|key| {
            !matches!(
                key.as_str(),
                "artifact_ref" | "offset_bytes" | "byte_length" | "sha256"
            )
        })
    {
        return Err(Error::new(
            "ARTIFACT_DAMAGED",
            "part entry contains unknown or missing fields",
        ));
    }
    value["artifact_ref"]
        .as_str()
        .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "part reference is malformed"))?;
    let offset_bytes = value["offset_bytes"]
        .as_u64()
        .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "part offset is malformed"))?;
    let byte_length = value["byte_length"]
        .as_u64()
        .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "part length is malformed"))?;
    let sha256 = value["sha256"]
        .as_str()
        .and_then(parse_sha256)
        .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "part digest is malformed"))?;
    Ok(CachedResultPart {
        offset_bytes,
        byte_length,
        sha256,
    })
}

fn parse_sha256(value: &str) -> Option<[u8; 32]> {
    fn nibble(value: u8) -> Option<u8> {
        match value {
            b'0'..=b'9' => Some(value - b'0'),
            b'a'..=b'f' => Some(value - b'a' + 10),
            _ => None,
        }
    }

    let encoded = value.as_bytes();
    if encoded.len() != 64 {
        return None;
    }
    let mut digest = [0; 32];
    for (index, pair) in encoded.as_chunks::<2>().0.iter().enumerate() {
        digest[index] = nibble(pair[0])?.checked_mul(16)? + nibble(pair[1])?;
    }
    Some(digest)
}

fn parts_from_checkpoints(
    part_values: &[Value],
    checkpoints: &[AssemblyPartCheckpoint],
    offset: u64,
    requested_end: u64,
) -> Result<Vec<CachedResultPart>> {
    if offset >= requested_end {
        return Ok(Vec::new());
    }
    let checkpoint_index = checkpoints
        .partition_point(|checkpoint| checkpoint.offset_bytes <= offset)
        .checked_sub(1)
        .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "assembly part index is missing"))?;
    let checkpoint = checkpoints[checkpoint_index];
    if checkpoint.part_index >= part_values.len() {
        return Err(Error::new(
            "ARTIFACT_DAMAGED",
            "assembly part index exceeds the manifest",
        ));
    }
    let mut expected_offset = checkpoint.offset_bytes;
    let mut selected = Vec::new();
    for value in &part_values[checkpoint.part_index..] {
        let part = decode_cached_result_part(value)?;
        if part.offset_bytes != expected_offset || part.byte_length > MAX_PAGE_BYTES as u64 {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "invalid indexed assembly segment",
            ));
        }
        if part.offset_bytes >= requested_end {
            break;
        }
        let part_end = expected_offset
            .checked_add(part.byte_length)
            .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "segment overflow"))?;
        if part_end > offset {
            selected.push(part);
        }
        expected_offset = part_end;
        if expected_offset >= requested_end {
            break;
        }
    }
    Ok(selected)
}

fn number(value: &Value, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .ok_or_else(|| Error::new("RESULT_METADATA", format!("missing integer {key}")))
}
fn identity(page: &ArtifactRecord) -> Result<Value> {
    let m = &page.metadata;
    if page.kind != "native_result_page" || !m["source"].is_object() || !m["selector"].is_object() {
        return Err(Error::new(
            "RESULT_METADATA",
            "only registered native result pages can be assembled",
        ));
    }
    let command_output = m["source"]["kind"] == "command_output";
    let claude_assistant_result = m["source"]["kind"] == "claude_assistant_result";
    let normalized_result_page = m["source"]["schema_id"]
        == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID;
    model::text(m, "binding_id")?;
    model::positive(m, "generation")?;
    if command_output {
        if m["selector"]["kind"] != "command_output" {
            return Err(Error::new(
                "RESULT_METADATA",
                "Command output selector differs from its page provenance",
            ));
        }
    } else if !normalized_result_page {
        for key in ["native_scope_key", "native_root_id"] {
            model::text(m, key)?;
        }
    }
    let mut source = m["source"].clone();
    // This flag describes individual page coverage, not source identity.
    source
        .as_object_mut()
        .expect("checked object")
        .remove("whole_digest_verified");
    if command_output || claude_assistant_result || normalized_result_page {
        let source = source.as_object_mut().expect("checked object");
        source.remove("result_operation_id");
        source.remove("result_input_sha256");
        source.remove("result_module_receipt");
        Ok(json!({
            "binding_id":m["binding_id"],
            "generation":m["generation"],
            "binding_generation":m["generation"],
            "selector":m["selector"],
            "source":source,
            "media_type":m["media_type"],
            "total_bytes":m["total_bytes"]
        }))
    } else {
        Ok(json!({
            "binding_id":m["binding_id"],
            "generation":m["generation"],
            "native_scope_key":m["native_scope_key"],
            "native_root_id":m["native_root_id"],
            "selector":m["selector"],
            "source":source,
            "media_type":m["media_type"],
            "total_bytes":m["total_bytes"]
        }))
    }
}

fn identity_comparison_key(identity: &Value) -> Value {
    let mut key = identity.clone();
    if key["source"]["schema_id"]
        == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
        && let Some(producer) = key
            .get_mut("source")
            .and_then(|source| source.get_mut("origin"))
            .and_then(|origin| origin.get_mut("producer"))
            .and_then(Value::as_object_mut)
    {
        // Assembly compares a stable origin while retaining the first
        // page's exact sealed lifecycle facts in the published identity.
        producer.insert(
            "completion_condition".to_owned(),
            json!("native_input_admitted"),
        );
        producer.insert("execution_complete".to_owned(), json!(false));
        producer.insert("task_completion".to_owned(), json!("unknown"));
        producer.insert("disposition".to_owned(), json!("admitted"));
    }
    key
}

fn plan(pages: &[ArtifactRecord]) -> Result<(Value, Vec<ResultPart>, u64)> {
    let first = pages
        .first()
        .ok_or_else(|| Error::invalid("no result pages"))?;
    let source = identity(first)?;
    let source_identity_key = identity_comparison_key(&source);
    let total = number(&first.metadata, "total_bytes")?;
    let mut offset = 0u64;
    let mut parts = Vec::with_capacity(pages.len());
    for (index, page) in pages.iter().enumerate() {
        let end = offset
            .checked_add(page.byte_length)
            .ok_or_else(|| Error::invalid("result length overflow"))?;
        if identity_comparison_key(&identity(page)?) != source_identity_key
            || number(&page.metadata, "offset_bytes")? != offset
            || number(&page.metadata, "byte_length")? != page.byte_length
            || page.metadata["page_sha256"] != page.content_digest
            || page.byte_length > MAX_PAGE_BYTES as u64
            || end > total
            || page.metadata["eof"].as_bool() != Some(end == total)
            || (page.byte_length == 0 && !(pages.len() == 1 && total == 0))
            || (end == total && index + 1 != pages.len())
        {
            return Err(Error::new(
                "RESULT_COVERAGE",
                "pages differ in source identity, overlap, are reordered or leave a gap",
            ));
        }
        parts.push(ResultPart {
            artifact_ref: page.artifact_id.clone(),
            offset_bytes: offset,
            byte_length: page.byte_length,
            sha256: page.content_digest.clone(),
        });
        offset = end;
    }
    if offset != total {
        return Err(Error::new(
            "RESULT_INCOMPLETE",
            "pages do not cover the complete source body",
        ));
    }
    Ok((source, parts, total))
}

impl ArtifactFiles {
    pub fn assemble(
        &self,
        operation_id: &str,
        pages: &[ArtifactRecord],
        expected: Option<&str>,
    ) -> Result<ArtifactRecord> {
        let (identity, parts, total) = plan(pages)?;
        let id = format!("assembled-{}", model::digest(operation_id.as_bytes()));
        let temp = self.root.join(format!(".{}.tmp", model::new_id()));
        let result = (|| -> Result<ArtifactRecord> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut output = options.open(&temp)?;
            let mut hash = Sha256::new();
            for page in pages {
                let bytes = self.verified_bytes(page)?;
                output.write_all(&bytes)?;
                hash.update(&bytes);
            }
            let digest = format!("{:x}", hash.finalize());
            if expected.is_some_and(|h| !h.eq_ignore_ascii_case(&digest)) {
                return Err(Error::new(
                    "RESULT_DIGEST_MISMATCH",
                    "assembled bytes differ from expected_sha256",
                ));
            }
            let command_output = identity["source"]["kind"] == "command_output";
            let normalized_result_page = identity["source"]["schema_id"]
                == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID;
            let capture = &identity["source"]["target_command_output"];
            let stored_sha = command_output
                .then(|| capture["stored_sha256"].as_str())
                .flatten();
            if stored_sha.is_some_and(|expected| !expected.eq_ignore_ascii_case(&digest)) {
                return Err(Error::new(
                    "RESULT_DIGEST_MISMATCH",
                    "assembled bytes differ from the captured stored-stream digest",
                ));
            }
            let native_sha = if command_output {
                if capture["truncated"] == false && capture["read_error"] == false {
                    capture["stream_sha256"].as_str()
                } else {
                    None
                }
            } else {
                identity["source"]["content_digest"]
                    .as_str()
                    .and_then(|h| h.strip_prefix("sha256:"))
            };
            let normalized_payload_sha = normalized_result_page
                .then(|| identity["source"]["payload_sha256"].as_str())
                .flatten();
            if normalized_payload_sha
                .is_some_and(|expected| !expected.eq_ignore_ascii_case(&digest))
            {
                return Err(Error::new(
                    "RESULT_DIGEST_MISMATCH",
                    "assembled bytes differ from the normalized source payload digest",
                ));
            }
            if native_sha.is_some_and(|h| !h.eq_ignore_ascii_case(&digest)) {
                return Err(Error::new(
                    "RESULT_DIGEST_MISMATCH",
                    "assembled bytes differ from the native source digest",
                ));
            }
            output.sync_all()?;
            drop(output);
            let record = ArtifactRecord {
                kind: "native_result".into(),
                artifact_id: id.clone(),
                relative_path: format!("artifacts/{id}.bin"),
                byte_length: total,
                content_digest: digest.clone(),
                metadata: json!({"assembly_operation_id":operation_id,"identity":identity,"parts":parts,
                    "part_count":pages.len(),"coverage":"complete","byte_length":total,
                    "sha256":digest,"expected_sha256":expected,"expected_digest_verified":expected.is_some(),
                    "native_digest_verified":native_sha.is_some(),
                    "normalized_payload_digest_verified":normalized_payload_sha.is_some(),
                    "capture_prefix_verified":stored_sha.is_some(),
                    "capture_truncated":if command_output {capture["truncated"].clone()} else {Value::Null},
                    "capture_read_error":if command_output {capture["read_error"].clone()} else {Value::Null},
                    "task_accepted":false}),
            };
            let destination = self.path(&record)?;
            match fs::hard_link(&temp, &destination) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Recovery after publication but before DB registration: compare
                    // existing bytes; never replace a possibly different artifact.
                    let mut file = self.open_regular(&record)?;
                    let mut hash = Sha256::new();
                    let mut buffer = [0u8; MAX_PAGE_BYTES];
                    let mut left = total;
                    while left > 0 {
                        let n = left.min(MAX_PAGE_BYTES as u64) as usize;
                        file.read_exact(&mut buffer[..n])?;
                        hash.update(&buffer[..n]);
                        left -= n as u64;
                    }
                    if format!("{:x}", hash.finalize()) != digest {
                        return Err(Error::new(
                            "ARTIFACT_DAMAGED",
                            "existing assembly has different bytes",
                        ));
                    }
                }
                Err(e) => return Err(e.into()),
            }
            swarm_process::sync_parent_directory(&destination)?;
            Ok(record)
        })();
        let cleanup: Result<bool> =
            swarm_process::remove_private_durable(&temp).map_err(Into::into);
        match (result, cleanup) {
            (Ok(record), Ok(_)) => Ok(record),
            (Err(error), Ok(_)) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(cleanup_error)) => Err(error.with_secondary_error(cleanup_error)),
        }
    }
    pub(super) fn open_regular(&self, record: &ArtifactRecord) -> Result<File> {
        self.open_regular_existing(record)?
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound).into())
    }
    pub(super) fn open_regular_existing(&self, record: &ArtifactRecord) -> Result<Option<File>> {
        let path = self.path(record)?;
        let m = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if !m.is_file() || m.file_type().is_symlink() || m.len() != record.byte_length {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "assembled artifact file or length changed",
            ));
        }
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if file.metadata()?.len() != record.byte_length {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "artifact changed during open",
            ));
        }
        Ok(Some(file))
    }
    pub(super) fn read_assembled(
        &self,
        record: &ArtifactRecord,
        offset: u64,
        length: usize,
    ) -> Result<Value> {
        self.read_assembled_at_epoch(record, offset, length, None)
    }

    pub(crate) fn read_assembled_at_epoch(
        &self,
        record: &ArtifactRecord,
        offset: u64,
        length: usize,
        cache_epoch: Option<&str>,
    ) -> Result<Value> {
        if length == 0 || length > MAX_PAGE_BYTES || offset > record.byte_length {
            return Err(Error::invalid(
                "artifact range is outside the retained content",
            ));
        }
        let requested_end = record.byte_length.min(offset.saturating_add(length as u64));
        let parts = cached_assembly_parts_for_range(record, cache_epoch, offset, requested_end)?;
        if record.metadata["sha256"] != record.content_digest {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "assembly manifest does not cover stored content",
            ));
        }
        let mut file = self.open_regular(record)?;
        let mut bytes = Vec::with_capacity(length);
        // Verify only touched segments against their committed hashes. Sequential
        // export rehashes each content segment once, not the whole file per range.
        for part in &parts {
            let end = part
                .offset_bytes
                .checked_add(part.byte_length)
                .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "segment overflow"))?;
            file.seek(SeekFrom::Start(part.offset_bytes))?;
            let mut page = vec![0u8; part.byte_length as usize];
            file.read_exact(&mut page)?;
            let actual_digest: [u8; 32] = Sha256::digest(&page).into();
            if actual_digest != part.sha256 {
                return Err(Error::new(
                    "ARTIFACT_DAMAGED",
                    "assembled segment digest changed",
                ));
            }
            let start = offset.saturating_sub(part.offset_bytes) as usize;
            let stop = (requested_end.min(end) - part.offset_bytes) as usize;
            bytes.extend_from_slice(&page[start..stop]);
        }
        if bytes.len() as u64 != requested_end - offset {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "indexed assembly segments do not cover the requested range",
            ));
        }
        let (encoding, content) = match std::str::from_utf8(&bytes) {
            Ok(s) => ("utf8", s.to_owned()),
            Err(_) => ("base64", STANDARD.encode(&bytes)),
        };
        Ok(
            json!({"artifact_id":record.artifact_id,"offset_bytes":offset,"byte_length":bytes.len(),
            "eof":requested_end==record.byte_length,"encoding":encoding,"content":content,
            "artifact_sha256":record.content_digest,"metadata":record.public_metadata()}),
        )
    }
}

#[cfg(test)]
mod assembly_part_cache_budget_tests {
    use super::*;

    fn entry(
        id: &str,
        epoch: &str,
        checkpoints: Vec<AssemblyPartCheckpoint>,
        bytes: usize,
    ) -> AssemblyPartCacheEntry {
        AssemblyPartCacheEntry {
            key: AssemblyPartCacheKey {
                cache_epoch: epoch.to_owned(),
                artifact_id: id.to_owned(),
                relative_path: format!("artifacts/{id}.bin"),
                byte_length: checkpoints.len() as u64,
                content_digest: "digest".to_owned(),
                part_count: checkpoints.len() as u64,
            },
            checkpoints: Arc::new(checkpoints),
            bytes,
        }
    }

    fn resident_bytes(cache: &VecDeque<AssemblyPartCacheEntry>) -> usize {
        cache
            .iter()
            .fold(0usize, |total, entry| total.saturating_add(entry.bytes))
    }

    #[test]
    fn oversized_checkpoint_fixture_is_skipped_and_small_hot_entry_gets_fair_space() {
        let mut cache = VecDeque::new();
        let half_budget = MAX_ASSEMBLY_PART_CACHE_BYTES / 2;
        let cold = entry("cold", "epoch-cold", Vec::new(), half_budget);
        let hot = entry("hot", "epoch-hot", Vec::new(), half_budget);
        assert!(insert_assembly_part_cache_entry(&mut cache, cold));
        assert!(insert_assembly_part_cache_entry(&mut cache, hot));

        let checkpoint_count = MAX_ASSEMBLY_PART_CACHE_BYTES
            .checked_div(size_of::<AssemblyPartCheckpoint>())
            .unwrap_or(0)
            .saturating_add(1);
        let checkpoints = (0..checkpoint_count)
            .map(|index| AssemblyPartCheckpoint {
                part_index: index.saturating_mul(ASSEMBLY_PART_CHECKPOINT_INTERVAL),
                offset_bytes: index as u64,
            })
            .collect::<Vec<_>>();
        let mut oversized = entry("large", "epoch-large", checkpoints, 0);
        oversized.bytes = assembly_part_cache_entry_bytes(&oversized);
        assert!(oversized.bytes > MAX_ASSEMBLY_PART_CACHE_BYTES);
        assert!(
            assembly_part_cache_entry_estimate(&oversized.key, checkpoint_count)
                > MAX_ASSEMBLY_PART_CACHE_BYTES
        );
        assert!(!insert_assembly_part_cache_entry(&mut cache, oversized));
        assert_eq!(cache.len(), 2);
        assert_eq!(resident_bytes(&cache), MAX_ASSEMBLY_PART_CACHE_BYTES);
        assert!(cache.iter().any(|entry| entry.key.artifact_id == "hot"));

        let incoming = entry("incoming", "epoch-incoming", Vec::new(), half_budget);
        assert!(insert_assembly_part_cache_entry(&mut cache, incoming));
        assert_eq!(cache.len(), 2);
        assert_eq!(resident_bytes(&cache), MAX_ASSEMBLY_PART_CACHE_BYTES);
        assert!(cache.iter().any(|entry| entry.key.artifact_id == "hot"));
        assert!(
            cache
                .iter()
                .any(|entry| entry.key.artifact_id == "incoming")
        );
        assert!(!cache.iter().any(|entry| entry.key.artifact_id == "cold"));
    }

    #[test]
    fn large_byte_range_allocates_selected_parts_by_segment_count() {
        let digest = "0".repeat(64);
        let record = ArtifactRecord {
            kind: "native_result".to_owned(),
            artifact_id: "large-range".to_owned(),
            relative_path: "artifacts/large-range.bin".to_owned(),
            byte_length: MAX_PAGE_BYTES as u64,
            content_digest: digest.clone(),
            metadata: json!({
                "sha256": digest,
                "part_count": 1,
                "parts": [{
                    "artifact_ref": "artifacts/part.bin",
                    "offset_bytes": 0,
                    "byte_length": MAX_PAGE_BYTES,
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                }]
            }),
        };

        let selected = cached_assembly_parts_for_range(&record, None, 0, record.byte_length)
            .expect("select the segment covering the requested range");

        assert_eq!(selected.len(), 1);
        assert!(selected.capacity() < MAX_PAGE_BYTES / size_of::<CachedResultPart>());
    }
}
