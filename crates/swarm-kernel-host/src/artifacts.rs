//! Immutable result pages and whole results. File I/O stays outside the SQLite owner thread.
mod assembly;
use crate::{
    error::{Error, Result},
    model, platform,
};
pub use assembly::{AssemblyRequest, ResultPart};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

pub const MAX_PAGE_BYTES: usize = 65_536;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResultPage {
    pub source: Value,
    pub offset_bytes: u64,
    pub byte_length: u64,
    pub total_bytes: u64,
    pub eof: bool,
    pub media_type: String,
    pub content_base64: String,
    pub page_sha256: String,
}
impl ResultPage {
    pub fn decode(&self) -> Result<Vec<u8>> {
        if !self.source.is_object()
            || model::canonical(&self.source)?.len() > 8192
            || self.media_type.is_empty()
            || self.media_type.len() > 256
            || self.content_base64.len() > MAX_PAGE_BYTES.div_ceil(3) * 4
            || self.total_bytes > i64::MAX as u64
        {
            return Err(Error::invalid(
                "invalid result source, media type or page size",
            ));
        }
        let bytes = STANDARD
            .decode(&self.content_base64)
            .map_err(|_| Error::invalid("result content is not canonical base64"))?;
        let end = self
            .offset_bytes
            .checked_add(self.byte_length)
            .ok_or_else(|| Error::invalid("result byte range overflow"))?;
        if bytes.len() > MAX_PAGE_BYTES
            || self.byte_length != bytes.len() as u64
            || end > self.total_bytes
            || self.eof != (end == self.total_bytes)
            || (bytes.is_empty() && !self.eof)
            || model::digest(&bytes) != self.page_sha256
        {
            return Err(Error::invalid(
                "result bytes, digest, range or EOF do not match",
            ));
        }
        Ok(bytes)
    }
    pub fn metadata(&self) -> Value {
        json!({"source":self.source,"offset_bytes":self.offset_bytes,
            "byte_length":self.byte_length,"total_bytes":self.total_bytes,
            "eof":self.eof,"media_type":self.media_type,"page_sha256":self.page_sha256})
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub kind: String,
    pub artifact_id: String,
    pub relative_path: String,
    pub byte_length: u64,
    pub content_digest: String,
    pub metadata: Value,
}

#[derive(Clone)]
pub struct ArtifactFiles {
    root: PathBuf,
}
impl ArtifactFiles {
    pub fn new(data_dir: &Path) -> Result<Self> {
        let root = data_dir.join("artifacts");
        fs::create_dir_all(&root)?;
        if fs::symlink_metadata(&root)?.file_type().is_symlink()
            || !fs::canonicalize(&root)?.starts_with(data_dir)
        {
            return Err(Error::new(
                "ARTIFACT_PATH",
                "artifact directory must belong to the state directory",
            ));
        }
        platform::private_permissions(&root, true)?;
        Ok(Self { root })
    }
    /// Sealed controller document; metadata is only its compact address/provenance.
    pub fn document(
        kind: &str,
        id: &str,
        document: &Value,
        metadata: Value,
    ) -> Result<(ArtifactRecord, Vec<u8>)> {
        let bytes = model::canonical(document)?.into_bytes();
        Ok((
            ArtifactRecord {
                kind: kind.into(),
                artifact_id: id.into(),
                relative_path: format!("artifacts/{id}.bin"),
                byte_length: bytes.len() as u64,
                content_digest: model::digest(&bytes),
                metadata,
            },
            bytes,
        ))
    }
    pub fn document_bytes(&self, record: &ArtifactRecord) -> Result<Vec<u8>> {
        let size = usize::try_from(record.byte_length)
            .map_err(|_| Error::invalid("document is too large"))?;
        // These are metadata documents, never arbitrary stdout or source blobs.
        if size > 64 * 1024 * 1024 {
            return Err(Error::invalid("document exceeds the metadata envelope"));
        }
        self.verified_range(record, 0, size)
    }
    /// Only closed, completed check outputs may be sealed. The complete body stays
    /// on disk; its compact hash is what gets registered in SQLite.
    pub fn seal_file(
        &self,
        identity: &str,
        source: &Path,
        mut metadata: Value,
    ) -> Result<ArtifactRecord> {
        let mut input = OpenOptions::new().read(true).write(true).open(source)?;
        let mut hash = Sha256::new();
        let mut length = 0u64;
        let mut segments = Vec::new();
        let mut buffer = [0; MAX_PAGE_BYTES];
        loop {
            let remaining = input.metadata()?.len().saturating_sub(length);
            if remaining == 0 {
                break;
            }
            let n = remaining.min(MAX_PAGE_BYTES as u64) as usize;
            input.read_exact(&mut buffer[..n])?;
            hash.update(&buffer[..n]);
            segments.push(model::digest(&buffer[..n]));
            length += n as u64;
        }
        input.sync_all()?;
        drop(input);
        let id = format!("checklog-{}", model::digest(identity.as_bytes()));
        metadata["segment_sha256"] = json!(segments);
        let record = ArtifactRecord {
            kind: "check_output".into(),
            artifact_id: id.clone(),
            relative_path: format!("artifacts/{id}.bin"),
            byte_length: length,
            content_digest: format!("{:x}", hash.finalize()),
            metadata,
        };
        match fs::hard_link(source, self.path(&record)?) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => self.verify(&record)?,
            Err(e) => return Err(e.into()),
        }
        Ok(record)
    }
    pub fn record(operation_id: &str, bytes: &[u8], metadata: Value) -> ArtifactRecord {
        let id = format!("result-{}", model::digest(operation_id.as_bytes()));
        ArtifactRecord {
            kind: "native_result_page".into(),
            relative_path: format!("artifacts/{id}.bin"),
            artifact_id: id,
            byte_length: bytes.len() as u64,
            content_digest: model::digest(bytes),
            metadata,
        }
    }
    fn path(&self, record: &ArtifactRecord) -> Result<PathBuf> {
        let prefix = match record.kind.as_str() {
            "native_result_page" => "result-",
            "native_result" => "assembled-",
            "task_submission" => "submission-",
            "source_snapshot" => "source-",
            "check_result" => "check-",
            "check_output" => "checklog-",
            "script_bundle" => "script-",
            "script_result" => "scriptresult-",
            "script_output" => "scriptlog-",
            _ => return Err(Error::new("ARTIFACT_KIND", "unsupported artifact kind")),
        };
        let hex = record.artifact_id.strip_prefix(prefix).unwrap_or("");
        if hex.len() != 64
            || !hex.bytes().all(|c| c.is_ascii_hexdigit())
            || record.relative_path != format!("artifacts/{}.bin", record.artifact_id)
        {
            return Err(Error::new(
                "ARTIFACT_PATH",
                "invalid generated artifact identity",
            ));
        }
        Ok(self.root.join(format!("{}.bin", record.artifact_id)))
    }
    /// Publish atomically without overwriting. A crash before DB commit can leave
    /// an orphan file; it cannot turn a partial file into a completed artifact.
    pub fn publish(&self, record: &ArtifactRecord, bytes: &[u8]) -> Result<()> {
        if (record.kind == "native_result_page" && bytes.len() > MAX_PAGE_BYTES)
            || record.byte_length != bytes.len() as u64
            || record.content_digest != model::digest(bytes)
        {
            return Err(Error::invalid("artifact bytes differ from their identity"));
        }
        let destination = self.path(record)?;
        let temp = self.root.join(format!(".{}.tmp", model::new_id()));
        let result = (|| -> Result<()> {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temp)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            match fs::hard_link(&temp, &destination) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    self.verify(record)?;
                }
                Err(e) => return Err(e.into()),
            }
            Ok(())
        })();
        let _ = fs::remove_file(&temp); // Only our uniquely named temporary file.
        result
    }
    pub(super) fn verified_bytes(&self, record: &ArtifactRecord) -> Result<Vec<u8>> {
        let path = self.path(record)?;
        let m = fs::symlink_metadata(&path)?;
        if !m.is_file()
            || m.file_type().is_symlink()
            || m.len() != record.byte_length
            || record.byte_length > MAX_PAGE_BYTES as u64
        {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "artifact size or file type changed",
            ));
        }
        // Bounded even if a full-access external process races the metadata read.
        let mut bytes = Vec::with_capacity(record.byte_length as usize);
        File::open(path)?
            .take((MAX_PAGE_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() as u64 != record.byte_length
            || model::digest(&bytes) != record.content_digest
        {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "artifact bytes differ from the committed digest",
            ));
        }
        Ok(bytes)
    }
    pub fn submission(operation_id: &str, document: &Value) -> Result<(ArtifactRecord, Vec<u8>)> {
        let bytes = model::canonical(document)?.into_bytes();
        let id = format!("submission-{}", model::digest(operation_id.as_bytes()));
        let mut metadata = document.clone();
        if let Some(fields) = metadata.as_object_mut() {
            fields.remove("claims");
            fields.remove("summary");
        }
        Ok((
            ArtifactRecord {
                kind: "task_submission".into(),
                artifact_id: id.clone(),
                relative_path: format!("artifacts/{id}.bin"),
                byte_length: bytes.len() as u64,
                content_digest: model::digest(&bytes),
                metadata,
            },
            bytes,
        ))
    }
    /// Stream the immutable body's hash once; no file-sized buffer or DB work.
    pub fn verify(&self, record: &ArtifactRecord) -> Result<()> {
        self.verified_range(record, 0, 0).map(|_| ())
    }
    /// Read back an existing immutable artifact. Absence is distinct from a
    /// damaged body or any other I/O failure; this method never creates files.
    pub fn verify_existing(&self, record: &ArtifactRecord) -> Result<bool> {
        let Some(file) = self.open_regular_existing(record)? else {
            return Ok(false);
        };
        self.verified_range_from_file(record, file, 0, 0)?;
        Ok(true)
    }
    fn verified_range(
        &self,
        record: &ArtifactRecord,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        let file = self.open_regular(record)?;
        self.verified_range_from_file(record, file, offset, length)
    }
    fn verified_range_from_file(
        &self,
        record: &ArtifactRecord,
        mut file: File,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>> {
        let mut hash = Sha256::new();
        let mut buffer = [0u8; MAX_PAGE_BYTES];
        let mut position = 0u64;
        let requested_end = record.byte_length.min(offset.saturating_add(length as u64));
        let mut selected = Vec::with_capacity(length);
        while position < record.byte_length {
            let n = (record.byte_length - position).min(MAX_PAGE_BYTES as u64) as usize;
            file.read_exact(&mut buffer[..n])?;
            hash.update(&buffer[..n]);
            let end = position + n as u64;
            if end > offset && position < requested_end {
                let from = offset.saturating_sub(position) as usize;
                let to = (requested_end.min(end) - position) as usize;
                selected.extend_from_slice(&buffer[from..to]);
            }
            position = end;
        }
        let mut extra = [0u8; 1];
        if file.read(&mut extra)? != 0 || format!("{:x}", hash.finalize()) != record.content_digest
        {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "retained body differs from the committed digest",
            ));
        }
        Ok(selected)
    }
    fn read_check_output(
        &self,
        record: &ArtifactRecord,
        offset: u64,
        length: usize,
    ) -> Result<Value> {
        let mut file = self.open_regular(record)?;
        let hashes = record.metadata["segment_sha256"]
            .as_array()
            .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "output segment index is absent"))?;
        if hashes.len() as u64 != record.byte_length.div_ceil(MAX_PAGE_BYTES as u64) {
            return Err(Error::new(
                "ARTIFACT_DAMAGED",
                "output segment index length differs",
            ));
        }
        let end = record.byte_length.min(offset.saturating_add(length as u64));
        let mut selected = Vec::new();
        if end > offset {
            for index in offset / MAX_PAGE_BYTES as u64..=(end - 1) / MAX_PAGE_BYTES as u64 {
                let begin = index * MAX_PAGE_BYTES as u64;
                let n = (record.byte_length - begin).min(MAX_PAGE_BYTES as u64) as usize;
                let mut buf = vec![0; n];
                file.seek(SeekFrom::Start(begin))?;
                file.read_exact(&mut buf)?;
                if hashes[index as usize].as_str() != Some(model::digest(&buf).as_str()) {
                    return Err(Error::new(
                        "ARTIFACT_DAMAGED",
                        "check output segment changed",
                    ));
                }
                selected.extend_from_slice(
                    &buf[(offset.saturating_sub(begin)) as usize
                        ..(end.min(begin + n as u64) - begin) as usize],
                );
            }
        }
        let (encoding, content) = match std::str::from_utf8(&selected) {
            Ok(s) => ("utf8", s.to_owned()),
            Err(_) => ("base64", STANDARD.encode(&selected)),
        };
        Ok(
            json!({"artifact_id":record.artifact_id,"offset_bytes":offset,"byte_length":selected.len(),"eof":end==record.byte_length,"encoding":encoding,"content":content,"artifact_sha256":record.content_digest,"metadata":record.public_metadata()}),
        )
    }
    pub fn read(&self, record: &ArtifactRecord, offset: u64, length: usize) -> Result<Value> {
        if length == 0 || length > MAX_PAGE_BYTES || offset > record.byte_length {
            return Err(Error::invalid(
                "artifact range is outside the retained content",
            ));
        }
        if record.kind == "check_output" {
            return self.read_check_output(record, offset, length);
        }
        if record.kind == "native_result" {
            return self.read_assembled(record, offset, length);
        }
        if matches!(
            record.kind.as_str(),
            "task_submission"
                | "source_snapshot"
                | "check_result"
                | "check_output"
                | "script_bundle"
                | "script_result"
                | "script_output"
        ) {
            let bytes = self.verified_range(record, offset, length)?;
            let (encoding, content) = match std::str::from_utf8(&bytes) {
                Ok(s) => ("utf8", s.to_owned()),
                Err(_) => ("base64", STANDARD.encode(&bytes)),
            };
            return Ok(
                json!({"artifact_id":record.artifact_id,"offset_bytes":offset,
                "byte_length":bytes.len(),"eof":offset + bytes.len() as u64 == record.byte_length,
                "encoding":encoding,"content":content,"artifact_sha256":record.content_digest,
                "metadata":record.public_metadata()}),
            );
        }
        let bytes = self.verified_bytes(record)?;
        let start = offset as usize;
        let end = bytes.len().min(start.saturating_add(length));
        let data = &bytes[start..end];
        let (encoding, content) = match std::str::from_utf8(data) {
            Ok(s) => ("utf8", s.to_owned()),
            Err(_) => ("base64", STANDARD.encode(data)),
        };
        Ok(
            json!({"artifact_id":record.artifact_id,"offset_bytes":offset,
            "byte_length":data.len(),"eof":end==bytes.len(),"encoding":encoding,"content":content,
            "artifact_sha256":record.content_digest,"metadata":record.metadata}),
        )
    }
}

impl ArtifactRecord {
    /// Large provenance lists are paginated, never copied into every byte-range reply.
    pub fn public_metadata(&self) -> Value {
        match self.metadata.as_object() {
            Some(map) => Value::Object(
                map.iter()
                    .filter(|(key, _)| !matches!(key.as_str(), "parts" | "segment_sha256"))
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            ),
            None => self.metadata.clone(),
        }
    }
}
