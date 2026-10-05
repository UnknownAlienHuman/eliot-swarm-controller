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
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
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
    model::text(m, "binding_id")?;
    model::positive(m, "generation")?;
    if command_output {
        if m["selector"]["kind"] != "command_output" {
            return Err(Error::new(
                "RESULT_METADATA",
                "Command output selector differs from its page provenance",
            ));
        }
    } else {
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
    if command_output {
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
fn plan(pages: &[ArtifactRecord]) -> Result<(Value, Vec<ResultPart>, u64)> {
    let first = pages
        .first()
        .ok_or_else(|| Error::invalid("no result pages"))?;
    let source = identity(first)?;
    let total = number(&first.metadata, "total_bytes")?;
    let mut offset = 0u64;
    let mut parts = Vec::with_capacity(pages.len());
    for (index, page) in pages.iter().enumerate() {
        let end = offset
            .checked_add(page.byte_length)
            .ok_or_else(|| Error::invalid("result length overflow"))?;
        if identity(page)? != source
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
                    "capture_prefix_verified":stored_sha.is_some(),
                    "capture_truncated":if command_output {capture["truncated"].clone()} else {Value::Null},
                    "capture_read_error":if command_output {capture["read_error"].clone()} else {Value::Null},
                    "task_accepted":false}),
            };
            match fs::hard_link(&temp, self.path(&record)?) {
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
            Ok(record)
        })();
        let _ = fs::remove_file(&temp);
        result
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
        let parts: Vec<ResultPart> = serde_json::from_value(record.metadata["parts"].clone())?;
        let mut end = 0u64;
        for part in &parts {
            if part.offset_bytes != end
                || part.byte_length > MAX_PAGE_BYTES as u64
                || (part.byte_length == 0 && !(parts.len() == 1 && record.byte_length == 0))
            {
                return Err(Error::new(
                    "ARTIFACT_DAMAGED",
                    "invalid assembly segment manifest",
                ));
            }
            end = end
                .checked_add(part.byte_length)
                .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "segment overflow"))?;
        }
        if parts.is_empty()
            || end != record.byte_length
            || record.metadata["sha256"] != record.content_digest
        {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "assembly manifest does not cover stored content",
            ));
        }
        let requested_end = record.byte_length.min(offset.saturating_add(length as u64));
        let mut file = self.open_regular(record)?;
        let mut bytes = Vec::with_capacity(length);
        // Verify only touched segments against their committed hashes. Sequential
        // export rehashes each content segment once, not the whole file per range.
        for part in &parts {
            let end = part.offset_bytes + part.byte_length;
            if end <= offset || part.offset_bytes >= requested_end {
                continue;
            }
            file.seek(SeekFrom::Start(part.offset_bytes))?;
            let mut page = vec![0u8; part.byte_length as usize];
            file.read_exact(&mut page)?;
            if model::digest(&page) != part.sha256 {
                return Err(Error::new(
                    "ARTIFACT_DAMAGED",
                    "assembled segment digest changed",
                ));
            }
            let start = offset.saturating_sub(part.offset_bytes) as usize;
            let stop = (requested_end.min(end) - part.offset_bytes) as usize;
            bytes.extend_from_slice(&page[start..stop]);
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
