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
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
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

#[derive(Debug, Clone)]
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
        if bytes.len() > MAX_PAGE_BYTES
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
                    self.verified_bytes(record)?;
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
    pub fn read(&self, record: &ArtifactRecord, offset: u64, length: usize) -> Result<Value> {
        if length == 0 || length > MAX_PAGE_BYTES || offset > record.byte_length {
            return Err(Error::invalid(
                "artifact range is outside the retained content",
            ));
        }
        if record.kind == "native_result" {
            return self.read_assembled(record, offset, length);
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
                    .filter(|(key, _)| key.as_str() != "parts")
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            ),
            None => self.metadata.clone(),
        }
    }
}
