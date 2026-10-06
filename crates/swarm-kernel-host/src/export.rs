//! Client-side streaming export. No filesystem paths are sent to the host.
use crate::{
    artifacts::MAX_PAGE_BYTES,
    config::Ipc,
    error::{Error, Result},
    ipc::Client,
    model::{self, Credential},
    platform,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

pub async fn artifact(
    root: &Path,
    credential: &Credential,
    config: &Ipc,
    artifact_id: &str,
    out: &Path,
) -> Result<Value> {
    match fs::symlink_metadata(out) {
        Ok(_) => {
            return Err(Error::new(
                "DESTINATION_EXISTS",
                "export never overwrites an existing path",
            ));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut client = Client::connect(root, credential, config).await?;
    let record = client
        .request("artifact.get", json!({"artifact_id":artifact_id}))
        .await?;
    let total = record["byte_length"]
        .as_u64()
        .ok_or_else(|| Error::new("PROTOCOL_ERROR", "artifact length missing"))?;
    let expected = model::text(&record, "content_digest")?;
    let parent = out
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let path = parent.join(format!(".swarm-export-{}.tmp", model::new_id()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path)?;
    let temp = Temporary(path);
    platform::private_permissions(&temp.0, false)?;
    let mut offset = 0u64;
    let mut hash = Sha256::new();
    // Read even an empty artifact once: metadata alone does not prove that its
    // backing file is still present and valid.
    loop {
        let length = (total - offset).clamp(1, MAX_PAGE_BYTES as u64);
        let page = client
            .request(
                "artifact.read",
                json!({"artifact_id":artifact_id,"offset_bytes":offset,"length_bytes":length}),
            )
            .await?;
        let content = page["content"]
            .as_str()
            .ok_or_else(|| Error::new("PROTOCOL_ERROR", "export content missing"))?;
        let bytes = match page["encoding"].as_str() {
            Some("utf8") => content.as_bytes().to_vec(),
            Some("base64") => STANDARD
                .decode(content)
                .map_err(|_| Error::new("PROTOCOL_ERROR", "invalid export base64"))?,
            _ => return Err(Error::new("PROTOCOL_ERROR", "unknown export encoding")),
        };
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::new("PROTOCOL_ERROR", "export length overflow"))?;
        if page["artifact_id"] != artifact_id
            || page["artifact_sha256"] != expected
            || page["offset_bytes"].as_u64() != Some(offset)
            || page["byte_length"].as_u64() != Some(bytes.len() as u64)
            || (bytes.is_empty() && total != 0)
            || bytes.len() as u64 > length
            || end > total
            || page["eof"].as_bool() != Some(end == total)
        {
            return Err(Error::new(
                "PROTOCOL_ERROR",
                "export range or artifact identity changed",
            ));
        }
        file.write_all(&bytes)?;
        hash.update(&bytes);
        offset = end;
        if offset == total {
            break;
        }
    }
    let digest = format!("{:x}", hash.finalize());
    if digest != expected {
        return Err(Error::new(
            "ARTIFACT_DAMAGED",
            "exported whole-file SHA-256 differs from the stored artifact",
        ));
    }
    file.sync_all()?;
    drop(file);
    fs::hard_link(&temp.0, out)?; // Atomic publication, no overwrite; temp is on the same volume.
    Ok(
        json!({"artifact_id":artifact_id,"path":out,"byte_length":total,"sha256":digest,"verified":true}),
    )
}
