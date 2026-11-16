//! Settings-backed folder assignment uses the normal timer launch path.
//! The selected brief and cursor are frozen into that ordinary launch; Store
//! commits the cursor with the launch admission.

use super::Router;
use eliot_core::{Error, SubmitCommand, limits::MAX_EVENT_BYTES};
use serde_json::Value;
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};

const MAX_BRIEF_ENTRIES: usize = 500;

pub(crate) async fn prepare(
    router: &Router,
    mut request: SubmitCommand,
) -> eliot_core::Result<SubmitCommand> {
    let Some(assignment) = request.args.get("assignment").cloned() else {
        return Ok(request);
    };
    if request.verb != "launch" {
        return Err(Error::new(
            "invalid",
            "assignment metadata is only valid for launch",
        ));
    }
    let key = assignment
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("invalid", "assignment key is missing"))?;
    let folder = assignment
        .get("folder")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("invalid", "assignment folder is missing"))?;
    let max_busy = assignment
        .get("max_busy")
        .and_then(Value::as_u64)
        .filter(|limit| *limit > 0)
        .ok_or_else(|| Error::new("invalid", "assignment max_busy must be positive"))?;

    let status = router.assignment_status(key, max_busy).await?;
    if status.busy >= usize::try_from(max_busy).unwrap_or(usize::MAX) {
        return Err(Error::new("bad_state", "assignment has reached max_busy"));
    }
    let root = canonical_directory(Path::new(folder))?;
    let (brief, text) = next_brief(&root, status.cursor.as_deref())?;
    let previous = status.cursor;

    request.args["text"] = Value::String(text);
    request.args["assignment"] = serde_json::json!({
        "key": key,
        "brief": brief,
        "previous": previous,
        "max_busy": max_busy,
    });
    request.validate()?;
    Ok(request)
}

fn canonical_directory(path: &Path) -> eliot_core::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(Error::new(
            "invalid",
            "assignment folder must be an absolute path",
        ));
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| Error::new("unavailable", "assignment folder is unavailable"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            "not_allowed",
            "assignment folder must be a regular directory",
        ));
    }
    fs::canonicalize(path)
        .map_err(|_| Error::new("unavailable", "assignment folder is unavailable"))
}

fn next_brief(root: &Path, cursor: Option<&str>) -> eliot_core::Result<(String, String)> {
    let entries = fs::read_dir(root)
        .map_err(|_| Error::new("unavailable", "cannot enumerate assignment folder"))?;
    let entries = entries
        .take(MAX_BRIEF_ENTRIES + 1)
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|_| Error::new("unavailable", "cannot read assignment folder entry"))?;
    if entries.len() > MAX_BRIEF_ENTRIES {
        return Err(Error::new(
            "frame_too_large",
            "assignment folder exceeds the 500-entry scan bound",
        ));
    }
    let mut candidates = Vec::new();
    for entry in entries {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if cursor.is_some_and(|cursor| name.as_str() <= cursor) {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(|_| Error::new("unavailable", "cannot inspect assignment brief"))?;
        if !file_type.is_file() || !name.to_ascii_lowercase().ends_with(".md") {
            continue;
        }
        let path = entry.path();
        let canonical = fs::canonicalize(&path)
            .map_err(|_| Error::new("unavailable", "assignment brief is unavailable"))?;
        if !canonical.starts_with(root) {
            continue;
        }
        candidates.push((name, canonical));
    }
    candidates.sort_by(|left, right| left.0.cmp(&right.0));
    let (name, path) = candidates
        .into_iter()
        .next()
        .ok_or_else(|| Error::new("not_found", "assignment folder has no next brief"))?;
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|file| {
            file.take((MAX_EVENT_BYTES + 1) as u64)
                .read_to_end(&mut bytes)
        })
        .map_err(|_| Error::new("unavailable", "cannot read assignment brief"))?;
    if bytes.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "assignment brief exceeds the persisted launch text bound",
        ));
    }
    let text = String::from_utf8(bytes)
        .map_err(|_| Error::new("invalid", "assignment brief is not UTF-8"))?;
    if text.trim().is_empty() {
        return Err(Error::new("invalid", "assignment brief is empty"));
    }
    Ok((name, text))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignment_selects_first_markdown_filename_after_cursor() {
        let root = std::env::temp_dir().join(format!("eliot-assignment-{}", ulid::Ulid::new()));
        fs::create_dir(&root).unwrap();
        fs::write(root.join("01-first.md"), "first").unwrap();
        fs::write(root.join("02-second.md"), "second task").unwrap();
        fs::write(root.join("03-ignore.txt"), "not a brief").unwrap();
        let canonical_root = fs::canonicalize(&root).unwrap();

        assert_eq!(
            next_brief(&canonical_root, Some("01-first.md")).unwrap(),
            ("02-second.md".to_owned(), "second task".to_owned())
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn assignment_folder_overflow_fails_closed() {
        let root = std::env::temp_dir().join(format!("eliot-assignment-{}", ulid::Ulid::new()));
        fs::create_dir(&root).unwrap();
        for index in 0..=MAX_BRIEF_ENTRIES {
            fs::write(root.join(format!("{index:03}.md")), "brief").unwrap();
        }
        let canonical_root = fs::canonicalize(&root).unwrap();
        assert_eq!(
            next_brief(&canonical_root, None).unwrap_err().code,
            "frame_too_large"
        );
        fs::remove_dir_all(root).unwrap();
    }
}
