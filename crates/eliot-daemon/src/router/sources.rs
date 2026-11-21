//! Required sources: files a launch or send names in `sources` are read by the
//! daemon and delivered inside the input text with path, content digest and
//! completeness. The Agent does not have to reopen them, and a missing,
//! binary or out-of-workspace file is stated instead of silently skipped.
//! Delivery proves the text reached the input, not that it was understood.

use super::Router;
use eliot_core::{
    Error, Result, SecretPolicy, SubmitCommand,
    api::{inference::sha256_hex, truncate_bytes},
    limits::MAX_EVENT_BYTES,
};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// Most files one input may name.
pub(crate) const MAX_SOURCES: usize = 16;
const MAX_SOURCE_PATH_BYTES: usize = 1024;
/// Room kept for the block's own header, and for each section's header line
/// beyond its path (digest, sizes and the completeness phrase).
const BLOCK_HEADER_BYTES: usize = 256;
const SECTION_HEADER_BYTES: usize = 160;
const BLOCK_HEADER: &str = "Required sources (delivered by Swarm with path, content digest and completeness; read them here instead of reopening the files):";
/// Larger files are refused rather than read just to be cut.
const MAX_SOURCE_FILE_BYTES: u64 = 8 * 1024 * 1024;

impl Router {
    /// Replace `args.sources` of a launch or send with the delivered block
    /// appended to its text. Files are read from the launch workspace or the
    /// target Agent's workspace.
    pub(super) async fn deliver_sources(&self, request: &mut SubmitCommand) -> Result<()> {
        let Some(sources) = request
            .args
            .as_object_mut()
            .and_then(|args| args.remove("sources"))
        else {
            return Ok(());
        };
        let paths = parse_sources(&sources)?;
        let root = match request.verb.as_str() {
            "launch" => request
                .args
                .get("workspace")
                .and_then(Value::as_str)
                .map(PathBuf::from),
            "send" => {
                let target = request
                    .target
                    .as_deref()
                    .ok_or_else(|| Error::new("invalid", "send sources need a target Agent"))?;
                self.inner
                    .store
                    .lock()
                    .await
                    .get_agent(target)?
                    .and_then(|agent| agent.workspace)
                    .map(PathBuf::from)
            }
            _ => {
                return Err(Error::new(
                    "invalid",
                    "sources are only valid for launch and send",
                ));
            }
        }
        .ok_or_else(|| Error::new("invalid", "sources need a workspace to read from"))?;
        let root = std::fs::canonicalize(&root)
            .map_err(|_| Error::new("unavailable", "source workspace is unavailable"))?;
        let text = request
            .args
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        // The whole input text is persisted with the command, so the
        // delivered content shares the 64 KiB persisted-text bound.
        let headers = paths
            .iter()
            .map(|path| path.len() + SECTION_HEADER_BYTES)
            .sum::<usize>();
        let budget = MAX_EVENT_BYTES.saturating_sub(text.len() + BLOCK_HEADER_BYTES + headers);
        let policy = self.secret_policy_snapshot();
        // File reads stay off the async router threads.
        let block =
            tokio::task::spawn_blocking(move || delivered_block(&root, &paths, budget, &policy))
                .await
                .map_err(|_| Error::new("internal", "source delivery task failed"))?;
        request.args["text"] = Value::String(if text.trim().is_empty() {
            block
        } else {
            format!("{text}\n\n{block}")
        });
        Ok(())
    }
}

fn parse_sources(value: &Value) -> Result<Vec<String>> {
    let invalid = || {
        Error::new(
            "invalid",
            format!(
                "sources must be 1..={MAX_SOURCES} distinct non-empty paths of at most {MAX_SOURCE_PATH_BYTES} bytes without control characters"
            ),
        )
    };
    let items = value.as_array().ok_or_else(invalid)?;
    if items.is_empty() || items.len() > MAX_SOURCES {
        return Err(invalid());
    }
    let mut paths: Vec<String> = Vec::with_capacity(items.len());
    for item in items {
        let path = item.as_str().ok_or_else(invalid)?;
        if path.trim().is_empty()
            || path.len() > MAX_SOURCE_PATH_BYTES
            || path.chars().any(char::is_control)
            || paths.iter().any(|known| known == path)
        {
            return Err(invalid());
        }
        paths.push(path.to_owned());
    }
    Ok(paths)
}

/// The delivered block: one section per named path, in the given order.
/// Deliverable texts share `budget` bytes smallest first, so a short file is
/// never cut to make room and unused shares pass to larger files; a file
/// over its share keeps its beginning and says how much is missing.
fn delivered_block(root: &Path, paths: &[String], budget: usize, policy: &SecretPolicy) -> String {
    enum Read {
        Text {
            content: String,
            size: usize,
            digest: String,
        },
        Skipped(String),
    }
    let reads: Vec<Read> = paths
        .iter()
        .map(|path| match read_source(root, path) {
            Err(reason) => Read::Skipped(reason.to_owned()),
            Ok(bytes) => {
                let digest = sha256_hex(&bytes)[..16].to_owned();
                match std::str::from_utf8(&bytes) {
                    Ok(text) if !text.contains('\0') => Read::Text {
                        content: policy.redact_text(text),
                        size: bytes.len(),
                        digest,
                    },
                    _ => Read::Skipped(format!(
                        "binary content, sha256 {digest}, {} bytes",
                        bytes.len()
                    )),
                }
            }
        })
        .collect();
    let mut order: Vec<usize> = (0..reads.len())
        .filter(|index| matches!(reads[*index], Read::Text { .. }))
        .collect();
    order.sort_by_key(|index| match &reads[*index] {
        Read::Text { content, .. } => content.len(),
        Read::Skipped(_) => 0,
    });
    let mut shares = vec![0; reads.len()];
    let mut remaining = budget;
    for (position, index) in order.iter().enumerate() {
        if let Read::Text { content, .. } = &reads[*index] {
            let share = (remaining / (order.len() - position)).min(content.len());
            shares[*index] = share;
            remaining -= share;
        }
    }
    let mut out = String::from(BLOCK_HEADER);
    for ((path, read), share) in paths.iter().zip(&reads).zip(shares) {
        match read {
            Read::Skipped(reason) => {
                out.push_str(&format!("\n\n=== {path}: not delivered ({reason}) ==="));
            }
            Read::Text {
                content,
                size,
                digest,
            } => {
                let (kept, cut) = truncate_bytes(content, share);
                let completeness = if cut {
                    format!(
                        "first {} of {} bytes; read the rest from the file",
                        kept.len(),
                        content.len()
                    )
                } else {
                    "complete".to_owned()
                };
                out.push_str(&format!(
                    "\n\n=== {path} (sha256 {digest}, {size} bytes, {completeness}) ===\n{kept}"
                ));
            }
        }
    }
    out
}

/// Read one named file inside `root`. The reason is a short fixed phrase.
fn read_source(root: &Path, path: &str) -> std::result::Result<Vec<u8>, &'static str> {
    let candidate = Path::new(path);
    let joined = if candidate.is_absolute() {
        candidate.to_path_buf()
    } else {
        root.join(candidate)
    };
    let canonical = std::fs::canonicalize(&joined).map_err(|_| "not found")?;
    if !canonical.starts_with(root) {
        return Err("outside the workspace");
    }
    let metadata = std::fs::metadata(&canonical).map_err(|_| "unreadable")?;
    if !metadata.is_file() {
        return Err("not a regular file");
    }
    if metadata.len() > MAX_SOURCE_FILE_BYTES {
        return Err("larger than 8 MiB");
    }
    std::fs::read(&canonical).map_err(|_| "unreadable")
}

/// Whether `stored` is `text` with a delivered sources block appended, so a
/// same-ID retry of that request replays the admitted text instead of
/// re-reading files that may have changed since.
pub(super) fn is_delivery_of(stored: &str, text: &str) -> bool {
    if text.trim().is_empty() {
        return stored.starts_with(BLOCK_HEADER);
    }
    stored
        .strip_prefix(text)
        .and_then(|rest| rest.strip_prefix("\n\n"))
        .is_some_and(|rest| rest.starts_with(BLOCK_HEADER))
}
