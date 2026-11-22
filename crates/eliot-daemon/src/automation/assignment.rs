//! Settings-backed folder assignment uses the normal timer launch path.
//! A producer publishes a closed brief with an empty marker. Store
//! records the selected name in the launch admission transaction.

use super::Router;
use eliot_core::{Error, SubmitCommand, limits::MAX_EVENT_BYTES};
use serde_json::Value;
use std::{
    fs::{self, File, Metadata, OpenOptions},
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
    time::SystemTime,
};

const MAX_BRIEF_ENTRIES: usize = 500;
const READY_DIRECTORY: &str = ".ready";

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

    router.require_assignment_slot(key, max_busy).await?;
    let root = canonical_directory(Path::new(folder))?;
    let ready_root = root.join(READY_DIRECTORY);
    match fs::symlink_metadata(&ready_root) {
        Ok(metadata) if metadata.is_dir() && !is_link(&metadata) => {}
        Ok(_) => {
            return Err(Error::new(
                "not_allowed",
                "assignment .ready must be a regular directory",
            ));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(Error::new(
                "not_found",
                "assignment folder has no ready markers",
            ));
        }
        Err(_) => {
            return Err(Error::new(
                "unavailable",
                "cannot inspect assignment .ready directory",
            ));
        }
    }
    let candidates = router.unadmitted_briefs(key, brief_names(&root)?).await?;
    let mut selected = None;
    for name in candidates {
        if let Some(text) = read_ready_brief(&root, &ready_root, &name)? {
            selected = Some((name, text));
            break;
        }
    }
    let (brief, text) = selected.ok_or_else(|| {
        Error::new(
            "not_found",
            "assignment folder has no ready, unadmitted brief",
        )
    })?;

    // Goal-owned work freezes the brief's Goal into the launch, so Store
    // installs it with the Agent before any native dispatch.
    let goal = assignment
        .get("goal")
        .map(|goal| {
            serde_json::from_value::<eliot_core::api::AssignmentGoal>(goal.clone())
                .map_err(|_| Error::new("invalid", "assignment goal is invalid"))
                .map(|goal| goal.definition(&brief))
        })
        .transpose()?;
    request.args["text"] = Value::String(text);
    request.args["assignment"] = serde_json::json!({
        "key": key,
        "brief": brief,
        "max_busy": max_busy,
    });
    if let Some(goal) = goal {
        goal.validate()?;
        request.args["assignment"]["goal"] = serde_json::to_value(goal)
            .map_err(|_| Error::new("invalid", "assignment goal cannot be encoded"))?;
    }
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
    if !metadata.is_dir() || is_link(&metadata) {
        return Err(Error::new(
            "not_allowed",
            "assignment folder must be a regular directory",
        ));
    }
    fs::canonicalize(path)
        .map_err(|_| Error::new("unavailable", "assignment folder is unavailable"))
}

fn brief_names(root: &Path) -> eliot_core::Result<Vec<String>> {
    let entries = fs::read_dir(root)
        .map_err(|_| Error::new("unavailable", "cannot enumerate assignment folder"))?;
    let mut candidates = Vec::new();
    let mut count = 0;
    for entry in entries {
        let entry =
            entry.map_err(|_| Error::new("unavailable", "cannot read assignment folder entry"))?;
        let name = entry.file_name();
        if name == READY_DIRECTORY {
            continue;
        }
        count += 1;
        if count > MAX_BRIEF_ENTRIES {
            return Err(Error::new(
                "frame_too_large",
                "assignment folder exceeds the 500-entry scan bound",
            ));
        }
        let Ok(name) = name.into_string() else {
            continue;
        };
        if !name.to_ascii_lowercase().ends_with(".md") {
            continue;
        }
        let file_type = entry
            .file_type()
            .map_err(|_| Error::new("unavailable", "cannot inspect assignment brief"))?;
        if file_type.is_file() {
            candidates.push(name);
        }
    }
    candidates.sort();
    Ok(candidates)
}

fn read_ready_brief(
    root: &Path,
    ready_root: &Path,
    name: &str,
) -> eliot_core::Result<Option<String>> {
    let marker_path = ready_root.join(name);
    let marker_time = match fs::symlink_metadata(&marker_path) {
        Ok(marker) if marker.is_file() && !is_link(&marker) && marker.len() == 0 => {
            modified(&marker)?
        }
        Ok(_) => return Ok(None),
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(_) => {
            return Err(Error::new(
                "unavailable",
                "cannot inspect brief ready marker",
            ));
        }
    };
    let path = root.join(name);
    let Some(mut file) = open_brief(&path)? else {
        return Ok(None);
    };
    let before = file
        .metadata()
        .map_err(|_| Error::new("unavailable", "cannot inspect assignment brief"))?;
    let written_at = modified(&before)?;
    if !before.is_file() || is_link(&before) || written_at > marker_time {
        return Ok(None);
    }
    let mut bytes = Vec::new();
    (&mut file)
        .take((MAX_EVENT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::new("unavailable", "cannot read assignment brief"))?;
    let after = file
        .metadata()
        .map_err(|_| Error::new("unavailable", "cannot inspect assignment brief"))?;
    if before.len() != after.len() || written_at != modified(&after)? {
        return Ok(None);
    }
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
    Ok(Some(text))
}

fn open_brief(path: &Path) -> eliot_core::Result<Option<File>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    match options.open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(_) => Err(Error::new("unavailable", "cannot open assignment brief")),
    }
}

fn modified(metadata: &Metadata) -> eliot_core::Result<SystemTime> {
    metadata.modified().map_err(|_| {
        Error::new(
            "unavailable",
            "cannot read assignment file modification time",
        )
    })
}

fn is_link(metadata: &Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}
