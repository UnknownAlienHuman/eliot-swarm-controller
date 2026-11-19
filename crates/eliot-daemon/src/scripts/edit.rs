//! Owner script editing over the approved Run roots.
//!
//! Owning contract: docs/spec/automation.md "Script editing". The roots are
//! exactly the ones Run resolves: the global `<data_dir>/scripts/` directory
//! (`freeze::global_script_root`) and each registered project's canonical
//! `.eliot/scripts/` (`freeze::project_script_roots`). An entry is checked
//! with Run's path restrictions, using retained directory/file handles (no `..`,
//! links, reparse points, devices or alternate streams; a canonical regular
//! file inside the root). Only existing entries are read or replaced;
//! saving never executes a script. A save is guarded by the
//! SHA-256 revision the editor read and writes a temporary file in the same
//! directory, then renames it over the entry. A script admitted before the
//! save fails its digest check at dispatch instead of running new bytes.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use eliot_core::api::{
    MAX_SCRIPT_EDIT_BYTES, MAX_SCRIPT_ENTRIES, MAX_SCRIPT_PAGE_BYTES, ProjectRegistrationView,
    REDACTION_MARKER, ScriptEntryView, ScriptListRequest, ScriptListResponse, ScriptReadRequest,
    ScriptReadResponse, ScriptRootView, ScriptSaveRequest, ScriptSaveResponse,
};
use eliot_core::{Error, Result, SecretPolicy};
use serde_json::Value;

use crate::platform::script_file::{self, Directory};

/// Directory depth listed below a root.
const MAX_DEPTH: usize = 6;
const BOM: &str = "\u{feff}";

static SAVE_LOCK: Mutex<()> = Mutex::new(());

fn root_for(
    data_root: &Path,
    registrations: &[ProjectRegistrationView],
    project: Option<&str>,
) -> Result<PathBuf> {
    let root = match project {
        None => data_root.join("scripts"),
        Some(id) => {
            let registration = registrations
                .iter()
                .find(|r| r.id == id)
                .ok_or_else(|| Error::new("not_found", "project is not registered"))?;
            PathBuf::from(&registration.checkout)
                .join(".eliot")
                .join("scripts")
        }
    };
    // Do not canonicalize then reopen: the component walk itself verifies all
    // ancestors from the volume/filesystem root without following links.
    Ok(root)
}

/// Canonical path without the Windows verbatim prefix, for editors.
fn display_path(path: &Path) -> String {
    let text = path.to_string_lossy();
    match text.strip_prefix(r"\\?\") {
        Some(rest) if rest.starts_with("UNC\\") => format!(r"\\{}", &rest[4..]),
        Some(rest) => rest.to_owned(),
        None => text.into_owned(),
    }
}

fn walk(
    root: &Path,
    dir: &Path,
    depth: usize,
    out: &mut Vec<ScriptEntryView>,
    scanned: &mut usize,
) {
    if *scanned >= MAX_SCRIPT_ENTRIES {
        return;
    }
    let Ok(_pinned) = Directory::open(dir) else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        // Budget counts all directory entries, including directories and refused
        // entries. Never collect a whole directory before enforcing the bound.
        if *scanned >= MAX_SCRIPT_ENTRIES {
            return;
        }
        *scanned += 1;
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.file_type().is_symlink() || crate::platform::is_reparse_point(&metadata) {
            continue;
        }
        if metadata.is_dir() {
            if depth < MAX_DEPTH {
                walk(root, &path, depth + 1, out, scanned);
            }
        } else if metadata.is_file() {
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if name.starts_with('.') && name.ends_with(".tmp") {
                continue;
            }
            let Ok(relative) = path.strip_prefix(root) else {
                continue;
            };
            let Some(relative) = relative.to_str() else {
                continue;
            };
            let relative = relative.replace('\\', "/");
            let request = ScriptReadRequest {
                project: None,
                path: relative.clone(),
            };
            if request.validate().is_err() {
                continue;
            }
            // Pin and verify the handle, including link count, before listing.
            let Ok((parent, name)) = Directory::open(root).and_then(|d| d.descend(&relative))
            else {
                continue;
            };
            let Ok(file) = parent.file(&name) else {
                continue;
            };
            let Ok(metadata) = file.metadata() else {
                continue;
            };
            out.push(ScriptEntryView {
                path: relative,
                size: metadata.len(),
            });
        }
    }
}

fn unsafe_value<T: serde::Serialize>(policy: &SecretPolicy, value: &T) -> bool {
    serde_json::to_value(value).map_or(true, |value| {
        policy.contains_value(&value) || value.to_string().contains(REDACTION_MARKER)
    })
}

/// One bounded root scan per RPC, split into encoded-byte-bounded pages.
/// The router supplies just the indexed root, not the entire root inventory.
pub(crate) fn list(
    data_root: &Path,
    registration: Option<&ProjectRegistrationView>,
    request: &ScriptListRequest,
    more_roots: bool,
    policy: &SecretPolicy,
) -> Result<ScriptListResponse> {
    let (root_index, offset) = request.position()?;
    let project = registration.map(|r| r.id.clone());
    let registrations = registration.map(std::slice::from_ref).unwrap_or_default();
    let mut root = match root_for(data_root, registrations, project.as_deref())
        .and_then(|root| Directory::open(&root))
    {
        Ok(pinned) => {
            let mut entries = Vec::new();
            let mut scanned = 0;
            walk(&pinned.path, &pinned.path, 0, &mut entries, &mut scanned);
            entries.retain(|entry| {
                !unsafe_value(
                    policy,
                    &(
                        project.as_ref(),
                        entry,
                        display_path(&pinned.path.join(&entry.path)),
                    ),
                )
            });
            entries.sort_by(|a, b| a.path.cmp(&b.path));
            ScriptRootView {
                project,
                available: true,
                reason: None,
                entries,
                truncated: scanned >= MAX_SCRIPT_ENTRIES,
            }
        }
        Err(_) => ScriptRootView {
            project,
            available: false,
            reason: Some("script root unavailable or unsafe".to_owned()),
            entries: Vec::new(),
            truncated: false,
        },
    };
    let next_root = || more_roots.then(|| format!("{}:0", root_index + 1));
    if unsafe_value(policy, &root) {
        return Ok(ScriptListResponse {
            roots: Vec::new(),
            next_cursor: next_root(),
        });
    }
    let entries = std::mem::take(&mut root.entries);
    let mut reply = ScriptListResponse {
        roots: vec![root],
        next_cursor: next_root(),
    };
    for (index, entry) in entries.into_iter().enumerate().skip(offset) {
        reply.roots[0].entries.push(entry);
        // Include the continuation in the measured bytes.
        reply.next_cursor = Some(format!("{root_index}:{}", index + 1));
        if serde_json::to_vec(&reply)
            .map_err(|_| Error::new("internal", "script page encoding failed"))?
            .len()
            > MAX_SCRIPT_PAGE_BYTES
        {
            reply.roots[0].entries.pop();
            reply.next_cursor = Some(format!("{root_index}:{index}"));
            return Ok(reply);
        }
    }
    reply.next_cursor = next_root();
    Ok(reply)
}

/// One captured entry: canonical path, revision, size, and the bytes when
/// they fit the editor bound.
struct Loaded {
    canonical: PathBuf,
    parent: Directory,
    name: String,
    file: std::fs::File,
    revision: String,
    size: u64,
    bytes: Option<Vec<u8>>,
}

fn load(root: &Path, relative: &str) -> Result<Loaded> {
    let (parent, name) = Directory::open(root)?.descend(relative)?;
    let mut file = parent.file(&name)?;
    let bytes = script_file::read(&mut file)?;
    let revision = eliot_core::api::inference::sha256_hex(&bytes);
    Ok(Loaded {
        canonical: parent.path.join(&name),
        parent,
        name,
        file,
        revision,
        size: bytes.len() as u64,
        bytes: Some(bytes),
    })
}

fn known_secret(policy: &SecretPolicy, text: &str) -> bool {
    let source = text.strip_prefix(BOM).unwrap_or(text);
    text.contains(REDACTION_MARKER)
        || policy.contains_value(&Value::String(text.to_owned()))
        || toml::from_str::<toml::Value>(source)
            .ok()
            .is_some_and(|parsed| unsafe_value(policy, &parsed))
        || serde_json::from_str::<Value>(source)
            .ok()
            .is_some_and(|parsed| unsafe_value(policy, &parsed))
}

/// The editable text of an entry, or why the editor may not touch it. Read
/// and save share this gate, so a withheld entry is never overwritten.
fn editable_text(
    loaded: &Loaded,
    policy: &SecretPolicy,
) -> std::result::Result<String, &'static str> {
    let Some(bytes) = loaded.bytes.as_ref() else {
        return Err("larger than the 256 KiB editor bound; use an external editor");
    };
    let text = std::str::from_utf8(bytes).map_err(|_| "not UTF-8 text")?;
    if text.contains('\0') {
        return Err("contains NUL bytes");
    }
    if known_secret(policy, text) {
        return Err("contains a known secret value; edit it outside Swarm");
    }
    Ok(text.to_owned())
}

/// `script.read`: the entry's text and revision when it is editable.
pub(crate) fn read(
    data_root: &Path,
    registrations: &[ProjectRegistrationView],
    request: &ScriptReadRequest,
    policy: &SecretPolicy,
) -> Result<ScriptReadResponse> {
    request.validate()?;
    if unsafe_value(policy, &(request.project.as_ref(), &request.path)) {
        return Err(Error::new(
            "not_allowed",
            "script identity contains a known secret",
        ));
    }
    let root = root_for(data_root, registrations, request.project.as_deref())?;
    let loaded = load(&root, &request.path)?;
    let (content, reason) = match editable_text(&loaded, policy) {
        Ok(text) => (
            Some(text.strip_prefix(BOM).unwrap_or(&text).to_owned()),
            None,
        ),
        Err(reason) => (None, Some(reason.to_owned())),
    };
    let reply = ScriptReadResponse {
        project: request.project.clone(),
        path: request.path.clone(),
        revision: loaded.revision.clone(),
        size: loaded.size,
        display_path: display_path(&loaded.canonical),
        editable: content.is_some(),
        content,
        reason,
    };
    if unsafe_value(policy, &reply) {
        return Err(Error::new(
            "not_allowed",
            "script response contains a known secret",
        ));
    }
    Ok(reply)
}

/// Encode edited text the way the file was: keep its BOM and CRLF endings.
fn encode_like(original: &[u8], content: &str) -> Vec<u8> {
    let original = String::from_utf8_lossy(original);
    let normalized = content.replace("\r\n", "\n");
    let mut text = if original.contains("\r\n") {
        normalized.replace('\n', "\r\n")
    } else {
        normalized
    };
    if original.starts_with(BOM) && !text.starts_with(BOM) {
        text.insert_str(0, BOM);
    }
    text.into_bytes()
}

/// `script.save`: replace an existing entry when its revision still matches.
pub(crate) fn save(
    data_root: &Path,
    registrations: &[ProjectRegistrationView],
    request: &ScriptSaveRequest,
    policy: &SecretPolicy,
) -> Result<ScriptSaveResponse> {
    let _guard = SAVE_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    request.validate()?;
    if unsafe_value(policy, &(request.project.as_ref(), &request.path)) {
        return Err(Error::new(
            "not_allowed",
            "script identity contains a known secret",
        ));
    }
    let root = root_for(data_root, registrations, request.project.as_deref())?;
    let loaded = load(&root, &request.path)?;
    if loaded.revision != request.expected_revision {
        return Err(Error::new(
            "conflict",
            "the script changed since it was read; read it again and reapply the edit",
        ));
    }
    // The same gate as read: an entry the editor may not show is never replaced.
    if let Err(reason) = editable_text(&loaded, policy) {
        return Err(Error::new(
            "not_allowed",
            format!("script is not editable here: {reason}"),
        ));
    }
    if known_secret(policy, &request.content) {
        return Err(Error::new(
            "not_allowed",
            "the script contains a known secret value or redaction marker",
        ));
    }
    let original = loaded.bytes.clone().unwrap_or_default();
    let bytes = encode_like(&original, &request.content);
    if known_secret(policy, &display_path(&loaded.canonical))
        || known_secret(policy, &String::from_utf8_lossy(&bytes))
    {
        return Err(Error::new("not_allowed", "script contains a known secret"));
    }
    if bytes.len() > MAX_SCRIPT_EDIT_BYTES {
        return Err(Error::new("invalid", "script content exceeds 256 KiB"));
    }
    if request.dry_run {
        return Ok(ScriptSaveResponse {
            saved: false,
            revision: loaded.revision,
        });
    }
    if bytes == original {
        return Ok(ScriptSaveResponse {
            saved: true,
            revision: loaded.revision,
        });
    }
    loaded
        .parent
        .replace(&loaded.name, &loaded.file, &original, &bytes)?;
    Ok(ScriptSaveResponse {
        saved: true,
        revision: eliot_core::api::inference::sha256_hex(&bytes),
    })
}
