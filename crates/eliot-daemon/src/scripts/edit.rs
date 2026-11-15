//! Owner script editing over the approved Run roots.
//!
//! Owning contract: docs/spec/automation.md "Script editing". The roots are
//! exactly the ones Run resolves: the global `<data_dir>/scripts/` directory
//! (`freeze::global_script_root`) and each registered project's canonical
//! `.eliot/scripts/` (`freeze::project_script_roots`). An entry is checked
//! with the same `setup::capture_file` rules Run freezes with (no `..`,
//! links, reparse points, devices or alternate streams; a canonical regular
//! file inside the root). Only existing files are read or replaced; nothing
//! is created, renamed, deleted or executed here. A save is guarded by the
//! SHA-256 revision the editor read and writes a temporary file in the same
//! directory, then renames it over the entry. A script admitted before the
//! save fails its digest check at dispatch instead of running new bytes.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use eliot_core::api::{
    MAX_SCRIPT_EDIT_BYTES, MAX_SCRIPT_ENTRIES, ProjectRegistrationView, REDACTION_MARKER,
    ScriptEntryView, ScriptListResponse, ScriptReadRequest, ScriptReadResponse, ScriptRootView,
    ScriptSaveRequest, ScriptSaveResponse,
};
use eliot_core::{Error, Result, SecretPolicy};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::freeze::{global_script_root, project_script_roots};

/// Directory depth listed below a root.
const MAX_DEPTH: usize = 6;
const BOM: &str = "\u{feff}";

static SAVE_LOCK: Mutex<()> = Mutex::new(());

fn revision_of(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn root_for(
    data_root: &Path,
    registrations: &[ProjectRegistrationView],
    project: Option<&str>,
) -> Result<PathBuf> {
    match project {
        None => global_script_root(data_root),
        Some(id) => {
            let registration = registrations
                .iter()
                .find(|registration| registration.id == id)
                .ok_or_else(|| Error::new("not_found", "project is not registered"))?;
            project_script_roots(registration).map(|(_, root)| root)
        }
    }
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
    truncated: &mut bool,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let Ok(metadata) = std::fs::symlink_metadata(entry.path()) else {
            continue;
        };
        // Links, junctions and other reparse points are never followed or listed.
        if metadata.file_type().is_symlink() || crate::platform::is_reparse_point(&metadata) {
            continue;
        }
        let path = entry.path();
        if metadata.is_dir() {
            if depth < MAX_DEPTH {
                walk(root, &path, depth + 1, out, truncated);
            }
        } else if metadata.is_file() {
            let name = entry.file_name().to_string_lossy().into_owned();
            // Editor temporaries left by an interrupted save are not entries.
            if name.starts_with('.') && name.ends_with(".tmp") {
                continue;
            }
            if out.len() >= MAX_SCRIPT_ENTRIES {
                *truncated = true;
                return;
            }
            let Ok(relative) = path.strip_prefix(root) else {
                continue;
            };
            let relative: Vec<String> = relative
                .components()
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect();
            out.push(ScriptEntryView {
                path: relative.join("/"),
                size: metadata.len(),
            });
        }
    }
}

/// `script.list`: every approved root and its regular files.
pub(crate) fn list(
    data_root: &Path,
    registrations: &[ProjectRegistrationView],
) -> ScriptListResponse {
    let mut roots = Vec::new();
    let mut scopes: Vec<Option<String>> = vec![None];
    scopes.extend(
        registrations
            .iter()
            .map(|registration| Some(registration.id.clone())),
    );
    for project in scopes {
        match root_for(data_root, registrations, project.as_deref()) {
            Ok(root) => {
                let mut entries = Vec::new();
                let mut truncated = false;
                walk(&root, &root, 0, &mut entries, &mut truncated);
                roots.push(ScriptRootView {
                    project,
                    available: true,
                    reason: None,
                    entries,
                    truncated,
                });
            }
            Err(error) => roots.push(ScriptRootView {
                project,
                available: false,
                reason: Some(error.message),
                entries: Vec::new(),
                truncated: false,
            }),
        }
    }
    ScriptListResponse { roots }
}

/// One captured entry: canonical path, revision, size, and the bytes when
/// they fit the editor bound.
struct Loaded {
    canonical: PathBuf,
    revision: String,
    size: u64,
    bytes: Option<Vec<u8>>,
}

/// Resolve and capture one existing entry with Run's own capture rules.
fn load(root: &Path, relative: &str) -> Result<Loaded> {
    let candidate = relative
        .split('/')
        .fold(root.to_path_buf(), |path, part| path.join(part));
    // The capture bound is setup's own file limit; the editor bound applies after.
    let mut allowance = u64::MAX;
    let frozen = crate::setup::capture_file(&candidate, Some(root), &mut allowance)
        .map_err(|error| Error::new("not_allowed", format!("script entry: {}", error.message)))?;
    let canonical = PathBuf::from(&frozen.canonical_path);
    let bytes = if frozen.size as usize > MAX_SCRIPT_EDIT_BYTES {
        None
    } else {
        let bytes = std::fs::read(&canonical)
            .map_err(|_| Error::new("unavailable", "script entry cannot be read"))?;
        if revision_of(&bytes) != frozen.sha256 {
            return Err(Error::new(
                "conflict",
                "script entry changed while it was read; read it again",
            ));
        }
        Some(bytes)
    };
    Ok(Loaded {
        canonical,
        revision: frozen.sha256,
        size: frozen.size,
        bytes,
    })
}

fn known_secret(policy: &SecretPolicy, text: &str) -> bool {
    text.contains(REDACTION_MARKER) || policy.contains_value(&Value::String(text.to_owned()))
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
    let root = root_for(data_root, registrations, request.project.as_deref())?;
    let loaded = load(&root, &request.path)?;
    let (content, reason) = match editable_text(&loaded, policy) {
        Ok(text) => (
            Some(text.strip_prefix(BOM).unwrap_or(&text).to_owned()),
            None,
        ),
        Err(reason) => (None, Some(reason.to_owned())),
    };
    Ok(ScriptReadResponse {
        project: request.project.clone(),
        path: request.path.clone(),
        revision: loaded.revision.clone(),
        size: loaded.size,
        display_path: display_path(&loaded.canonical),
        editable: content.is_some(),
        content,
        reason,
    })
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
    // Recheck right before the rename; an external edit since is a conflict.
    if load(&root, &request.path)?.revision != loaded.revision {
        return Err(Error::new(
            "conflict",
            "the script changed while the save was validated; read it again",
        ));
    }
    let (Some(dir), Some(name)) = (loaded.canonical.parent(), loaded.canonical.file_name()) else {
        return Err(Error::new(
            "not_allowed",
            "script entry has no parent directory",
        ));
    };
    let name = name.to_string_lossy();
    let private = request.project.is_none();
    crate::platform::data_dir::replace_file_atomically(dir, &name, &bytes, private)?;
    Ok(ScriptSaveResponse {
        saved: true,
        revision: revision_of(&bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edits_keep_bom_and_crlf() {
        let original = "\u{feff}a\r\nb\r\n".as_bytes();
        assert_eq!(
            encode_like(original, "a\nc\n"),
            "\u{feff}a\r\nc\r\n".as_bytes()
        );
        assert_eq!(encode_like(b"a\nb\n", "a\r\nc\n"), b"a\nc\n");
    }

    #[test]
    fn display_path_drops_the_verbatim_prefix() {
        assert_eq!(
            display_path(Path::new(r"\\?\C:\d\scripts\x.ps1")),
            r"C:\d\scripts\x.ps1"
        );
        assert_eq!(
            display_path(Path::new(r"\\?\UNC\host\share\x.ps1")),
            r"\\host\share\x.ps1"
        );
    }

    #[test]
    fn withheld_entries_are_never_replaced_and_editable_ones_are() {
        let data_root =
            std::env::temp_dir().join(format!("eliot-script-save-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(data_root.join("scripts")).unwrap();
        std::fs::write(data_root.join("scripts").join("bin.ps1"), b"a\0b").unwrap();
        std::fs::write(
            data_root.join("scripts").join("ok.ps1"),
            b"Write-Output 1\r\n",
        )
        .unwrap();
        let policy = SecretPolicy::new(Vec::<String>::new());
        let request = |path: &str, revision: String, content: &str| ScriptSaveRequest {
            project: None,
            path: path.to_owned(),
            expected_revision: revision,
            content: content.to_owned(),
            dry_run: false,
        };
        let binary = read(
            &data_root,
            &[],
            &ScriptReadRequest {
                project: None,
                path: "bin.ps1".to_owned(),
            },
            &policy,
        )
        .unwrap();
        assert!(!binary.editable);
        let refused = save(
            &data_root,
            &[],
            &request("bin.ps1", binary.revision, "x"),
            &policy,
        );
        assert_eq!(refused.unwrap_err().code, "not_allowed");
        assert_eq!(
            std::fs::read(data_root.join("scripts").join("bin.ps1")).unwrap(),
            b"a\0b"
        );

        let ok = read(
            &data_root,
            &[],
            &ScriptReadRequest {
                project: None,
                path: "ok.ps1".to_owned(),
            },
            &policy,
        )
        .unwrap();
        assert_eq!(ok.content.as_deref(), Some("Write-Output 1\r\n"));
        let stale = save(
            &data_root,
            &[],
            &request("ok.ps1", "0".repeat(64), "x\n"),
            &policy,
        );
        assert_eq!(stale.unwrap_err().code, "conflict");
        let saved = save(
            &data_root,
            &[],
            &request("ok.ps1", ok.revision, "Write-Output 2\n"),
            &policy,
        )
        .unwrap();
        assert!(saved.saved);
        assert_eq!(
            std::fs::read(data_root.join("scripts").join("ok.ps1")).unwrap(),
            b"Write-Output 2\r\n"
        );
        let _ = std::fs::remove_dir_all(data_root);
    }

    #[test]
    fn listing_skips_temporaries_and_stays_relative() {
        let base = std::env::temp_dir().join(format!("eliot-script-list-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(base.join("sub")).unwrap();
        std::fs::write(base.join("a.ps1"), "1").unwrap();
        std::fs::write(base.join("sub").join("b.py"), "22").unwrap();
        std::fs::write(base.join(".a.ps1.01X.tmp"), "x").unwrap();
        let mut entries = Vec::new();
        let mut truncated = false;
        walk(&base, &base, 0, &mut entries, &mut truncated);
        let paths: Vec<_> = entries.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(paths, vec!["a.ps1", "sub/b.py"]);
        assert!(!truncated);
        let _ = std::fs::remove_dir_all(base);
    }
}
