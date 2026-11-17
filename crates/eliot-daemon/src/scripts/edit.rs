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
use sha2::{Digest, Sha256};

use crate::platform::script_file::{self, Directory};

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
    let revision = revision_of(&bytes);
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
        revision: revision_of(&bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Files(PathBuf);
    impl Files {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("eliot-k31-{}", ulid::Ulid::new()));
            std::fs::create_dir_all(path.join("scripts")).unwrap();
            Self(path)
        }
        fn put(&self, name: &str, bytes: impl AsRef<[u8]>) {
            std::fs::write(self.0.join("scripts").join(name), bytes).unwrap();
        }
        fn read(&self, name: &str, policy: &SecretPolicy) -> Result<ScriptReadResponse> {
            read(
                &self.0,
                &[],
                &ScriptReadRequest {
                    project: None,
                    path: name.to_owned(),
                },
                policy,
            )
        }
        fn save(
            &self,
            name: &str,
            revision: String,
            content: &str,
            policy: &SecretPolicy,
        ) -> Result<ScriptSaveResponse> {
            save(
                &self.0,
                &[],
                &ScriptSaveRequest {
                    project: None,
                    path: name.to_owned(),
                    expected_revision: revision,
                    content: content.to_owned(),
                    dry_run: false,
                },
                policy,
            )
        }
    }
    impl Drop for Files {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn secret_names_projects_and_display_paths_are_withheld() {
        let files = Files::new();
        files.put("audit-secret.ps1", "1");
        files.put("safe.ps1", "1");
        let policy = SecretPolicy::new(["audit-secret"]);
        let reply = list(
            &files.0,
            None,
            &ScriptListRequest::default(),
            false,
            &policy,
        )
        .unwrap();
        assert!(
            !serde_json::to_string(&reply)
                .unwrap()
                .contains("audit-secret")
        );
        assert!(files.read("audit-secret.ps1", &policy).is_err());
        let policy = SecretPolicy::new([files.0.file_name().unwrap().to_str().unwrap()]);
        assert!(files.read("safe.ps1", &policy).is_err());
        assert!(
            list(
                &files.0,
                None,
                &ScriptListRequest::default(),
                false,
                &policy
            )
            .unwrap()
            .roots[0]
                .entries
                .is_empty()
        );
        let registration = ProjectRegistrationView {
            id: "audit-secret".to_owned(),
            checkout: files.0.to_string_lossy().into_owned(),
            repo_common_dir: "unused".into(),
            profiles: Vec::new(),
            actions: Vec::new(),
            interpreters: Default::default(),
            run_env: Vec::new(),
            git: None,
            revision: 1,
            origin: None,
        };
        let policy = SecretPolicy::new(["audit-secret"]);
        let reply = list(
            &files.0,
            Some(&registration),
            &ScriptListRequest {
                cursor: Some("1:0".into()),
            },
            false,
            &policy,
        )
        .unwrap();
        assert!(reply.roots.is_empty());
    }

    #[test]
    fn escaped_toml_and_json_keys_and_values_are_rejected_on_read_and_save() {
        let files = Files::new();
        let policy = SecretPolicy::new(["audit-secret"]);
        for text in [
            r#"token = "audit-secr\u0065t""#,
            r#""audit-secr\u0065t" = "ok""#,
            r#"{"token":"audit-secr\u0065t"}"#,
            r#"{"audit-secr\u0065t":"ok"}"#,
        ] {
            files.put("entry.toml", text);
            let read = files.read("entry.toml", &policy).unwrap();
            assert!(!read.editable, "{text}");
            assert_eq!(
                files
                    .save("entry.toml", read.revision, "safe = 1", &policy)
                    .unwrap_err()
                    .code,
                "not_allowed"
            );
            files.put("entry.toml", "safe = 1");
            let revision = files.read("entry.toml", &policy).unwrap().revision;
            assert_eq!(
                files
                    .save("entry.toml", revision, text, &policy)
                    .unwrap_err()
                    .code,
                "not_allowed"
            );
        }
    }

    #[test]
    fn hardlinked_entries_are_neither_listed_nor_read_nor_saved() {
        let files = Files::new();
        let policy = SecretPolicy::default();
        let outside = files.0.join("outside.txt");
        std::fs::write(&outside, "outside").unwrap();
        std::fs::hard_link(&outside, files.0.join("scripts/linked.txt")).unwrap();
        assert!(files.read("linked.txt", &policy).is_err());
        assert!(
            files
                .save("linked.txt", revision_of(b"outside"), "changed", &policy)
                .is_err()
        );
        assert!(
            list(
                &files.0,
                None,
                &ScriptListRequest::default(),
                false,
                &policy
            )
            .unwrap()
            .roots[0]
                .entries
                .is_empty()
        );
        assert_eq!(std::fs::read_to_string(outside).unwrap(), "outside");
    }

    #[test]
    fn read_stays_on_the_verified_handle_and_rejects_growth() {
        let files = Files::new();
        files.put("entry.txt", "small");
        let (parent, name) = Directory::open(&files.0.join("scripts"))
            .unwrap()
            .descend("entry.txt")
            .unwrap();
        let mut handle = parent.file(&name).unwrap();
        std::fs::rename(
            files.0.join("scripts/entry.txt"),
            files.0.join("scripts/old.txt"),
        )
        .unwrap();
        files.put("entry.txt", vec![b'x'; MAX_SCRIPT_EDIT_BYTES + 100]);
        assert_eq!(script_file::read(&mut handle).unwrap(), b"small");
        assert!(files.read("entry.txt", &SecretPolicy::default()).is_err());
    }

    #[test]
    fn final_handle_recheck_rejects_replacement_and_removal() {
        let files = Files::new();
        files.put("entry.txt", "old");
        let root = files.0.join("scripts");
        let loaded = load(&root, "entry.txt").unwrap();
        std::fs::rename(root.join("entry.txt"), root.join("previous.txt")).unwrap();
        files.put("entry.txt", "external");
        assert_eq!(
            loaded
                .parent
                .replace(&loaded.name, &loaded.file, b"old", b"new")
                .unwrap_err()
                .code,
            "conflict"
        );
        assert_eq!(std::fs::read(root.join("entry.txt")).unwrap(), b"external");
        std::fs::remove_file(root.join("entry.txt")).unwrap();
        assert_eq!(
            loaded
                .parent
                .replace(&loaded.name, &loaded.file, b"old", b"new")
                .unwrap_err()
                .code,
            "conflict"
        );
        assert!(!root.join("entry.txt").exists());
        assert!(
            !std::fs::read_dir(&root).unwrap().any(|e| e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp"))
        );
    }

    #[test]
    fn concurrent_daemon_saves_of_one_revision_have_one_winner() {
        let files = Files::new();
        files.put("entry.txt", "old");
        let revision = files
            .read("entry.txt", &SecretPolicy::default())
            .unwrap()
            .revision;
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let run = |text| {
                barrier.wait();
                files.save(
                    "entry.txt",
                    revision.clone(),
                    text,
                    &SecretPolicy::default(),
                )
            };
            let a = scope.spawn(move || run("first"));
            let b = scope.spawn(move || run("second"));
            let results = [a.join().unwrap(), b.join().unwrap()];
            assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
            assert_eq!(
                results
                    .iter()
                    .filter(|r| r.as_ref().is_err_and(|e| e.code == "conflict"))
                    .count(),
                1
            );
        });
    }

    #[test]
    fn scan_budget_counts_directories_and_refused_entries() {
        let files = Files::new();
        for index in 0..510 {
            std::fs::create_dir(files.0.join("scripts").join(format!("d{index}"))).unwrap();
        }
        let mut scanned = 0;
        let mut entries = Vec::new();
        walk(
            &files.0.join("scripts"),
            &files.0.join("scripts"),
            0,
            &mut entries,
            &mut scanned,
        );
        assert_eq!(scanned, MAX_SCRIPT_ENTRIES);
        assert!(entries.is_empty());
    }

    #[test]
    fn pages_resume_inside_a_root_then_advance_to_the_next_root() {
        let files = Files::new();
        let policy = SecretPolicy::default();
        for index in 0..400 {
            files.put(&format!("{index:03}-{}.txt", "a".repeat(200)), "1");
        }
        let mut request = ScriptListRequest::default();
        let mut paths = std::collections::HashSet::new();
        let mut pages = 0;
        loop {
            let reply = list(&files.0, None, &request, true, &policy).unwrap();
            request.validate_response(&reply).unwrap();
            assert!(serde_json::to_vec(&reply).unwrap().len() <= MAX_SCRIPT_PAGE_BYTES);
            for entry in &reply.roots[0].entries {
                assert!(paths.insert(entry.path.clone()));
            }
            pages += 1;
            let next = reply.next_cursor.unwrap();
            if next == "1:0" {
                break;
            }
            request.cursor = Some(next);
            assert!(pages < 10);
        }
        assert!(pages > 1);
        assert_eq!(paths.len(), 400);
        std::fs::create_dir_all(files.0.join(".eliot/scripts")).unwrap();
        std::fs::write(files.0.join(".eliot/scripts/project.ps1"), "1").unwrap();
        let project = ProjectRegistrationView {
            id: "second-root".into(),
            checkout: files.0.to_string_lossy().into_owned(),
            repo_common_dir: "unused".into(),
            profiles: Vec::new(),
            actions: Vec::new(),
            interpreters: Default::default(),
            run_env: Vec::new(),
            git: None,
            revision: 1,
            origin: None,
        };
        let request = ScriptListRequest {
            cursor: Some("1:0".into()),
        };
        let reply = list(&files.0, Some(&project), &request, false, &policy).unwrap();
        request.validate_response(&reply).unwrap();
        assert_eq!(reply.roots[0].project.as_deref(), Some("second-root"));
        assert_eq!(reply.roots[0].entries[0].path, "project.ps1");
        assert!(reply.next_cursor.is_none());
    }

    #[test]
    fn existing_dotfiles_can_be_saved() {
        let files = Files::new();
        files.put(".settings", "old");
        let policy = SecretPolicy::default();
        let revision = files.read(".settings", &policy).unwrap().revision;
        files.save(".settings", revision, "new", &policy).unwrap();
        assert_eq!(
            files.read(".settings", &policy).unwrap().content.as_deref(),
            Some("new")
        );
    }

    #[cfg(windows)]
    #[test]
    fn real_junctions_are_refused_and_pinned_ancestors_cannot_be_swapped() {
        assert!(Directory::open(Path::new(r"\\server\share\scripts")).is_err());
        assert!(Directory::open(Path::new(r"\\?\UNC\server\share\scripts")).is_err());
        let files = Files::new();
        let root = files.0.join("scripts");
        let outside = files.0.join("outside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("entry.txt"), "outside").unwrap();
        let junction = root.join("link");
        // New-Item creates a real NTFS junction without developer-mode symlink privileges.
        let output = std::process::Command::new("powershell.exe").args(["-NoProfile", "-NonInteractive", "-Command", "New-Item -ItemType Junction -Path $env:K31_LINK -Target $env:K31_OUTSIDE | Out-Null"])
            .env("K31_LINK", &junction).env("K31_OUTSIDE", &outside).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            files
                .read("link/entry.txt", &SecretPolicy::default())
                .is_err()
        );
        std::fs::remove_dir(&junction).unwrap();
        std::fs::create_dir(root.join("sub")).unwrap();
        files.put("sub/entry.txt", "inside");
        let loaded = load(&root, "sub/entry.txt").unwrap();
        assert!(std::fs::rename(root.join("sub"), root.join("moved")).is_err());
        assert!(std::fs::rename(&root, files.0.join("moved-root")).is_err());
        loaded
            .parent
            .replace(&loaded.name, &loaded.file, b"inside", b"saved")
            .unwrap();
        assert_eq!(
            std::fs::read(outside.join("entry.txt")).unwrap(),
            b"outside"
        );
    }

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
        let mut scanned = 0;
        walk(&base, &base, 0, &mut entries, &mut scanned);
        entries.sort_by(|a, b| a.path.cmp(&b.path));
        let paths: Vec<_> = entries.iter().map(|entry| entry.path.as_str()).collect();
        assert_eq!(paths, vec!["a.ps1", "sub/b.py"]);
        assert!(scanned < MAX_SCRIPT_ENTRIES);
        let _ = std::fs::remove_dir_all(base);
    }
}
