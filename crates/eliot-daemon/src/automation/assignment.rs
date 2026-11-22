//! Settings-backed folder assignment uses the normal timer launch path.
//! Publish a closed brief with an empty marker or observe its stability.
//! Store records the selected name in the launch admission transaction.

use super::Router;
use eliot_core::{Error, SubmitCommand, limits::MAX_EVENT_BYTES};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, Metadata, OpenOptions},
    io::{ErrorKind, Read},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};

const MAX_BRIEF_ENTRIES: usize = 500;
const READY_DIRECTORY: &str = ".ready";
const STABILITY_WINDOW: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct BriefVersion {
    size: u64,
    modified: SystemTime,
}

impl BriefVersion {
    fn read(metadata: &Metadata) -> eliot_core::Result<Self> {
        Ok(Self {
            size: metadata.len(),
            modified: modified(metadata)?,
        })
    }
}

#[derive(Default)]
pub(crate) struct Observations {
    queues: HashMap<String, QueueObservation>,
}

#[derive(Default)]
struct QueueObservation {
    folder: PathBuf,
    briefs: HashMap<String, (BriefVersion, Instant)>,
}

impl Observations {
    pub(crate) fn retain_queues(&mut self, active: &HashSet<&str>) {
        self.queues.retain(|key, _| active.contains(key.as_str()));
    }

    pub(crate) fn stable_names(
        &mut self,
        key: &str,
        folder: &Path,
        versions: &[(String, BriefVersion)],
    ) -> HashSet<String> {
        let observation = self.queues.entry(key.to_owned()).or_default();
        if observation.folder != folder {
            observation.folder = folder.to_owned();
            observation.briefs.clear();
        }
        let now = Instant::now();
        let mut next = HashMap::with_capacity(versions.len());
        let mut stable = HashSet::new();
        for (name, version) in versions {
            let since = observation
                .briefs
                .get(name)
                .filter(|(observed, _)| observed == version)
                .map_or(now, |(_, since)| *since);
            if now.duration_since(since) >= STABILITY_WINDOW {
                stable.insert(name.clone());
            }
            next.insert(name.clone(), (*version, since));
        }
        observation.briefs = next;
        stable
    }
}

pub(crate) async fn prepare(
    router: &Router,
    mut request: SubmitCommand,
    include: Option<&str>,
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
    let ready_root = match fs::symlink_metadata(&ready_root) {
        Ok(metadata) if metadata.is_dir() && !is_link(&metadata) => Some(ready_root),
        Ok(_) => {
            return Err(Error::new(
                "not_allowed",
                "assignment .ready must be a regular directory",
            ));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(_) => {
            return Err(Error::new(
                "unavailable",
                "cannot inspect assignment .ready directory",
            ));
        }
    };
    let (candidates, requires_include) = router
        .unadmitted_briefs(key, brief_names(&root)?, include)
        .await?;
    let mut versions = Vec::with_capacity(candidates.len());
    for name in candidates {
        match fs::symlink_metadata(root.join(&name)) {
            Ok(metadata) if metadata.is_file() && !is_link(&metadata) => {
                versions.push((name, BriefVersion::read(&metadata)?));
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(_) => return Err(Error::new("unavailable", "cannot inspect assignment brief")),
        }
    }
    let stable = router.stable_assignment_briefs(key, &root, &versions);
    let mut selected = None;
    let mut settling = false;
    for (name, version) in versions {
        if include.is_some_and(|included| included != name) {
            continue;
        }
        let published = match &ready_root {
            Some(directory) => marker_is_ready(&directory.join(&name), version)?,
            None => false,
        };
        if (published || stable.contains(&name))
            && let Some(text) = read_unchanged_brief(&root.join(&name), version)?
        {
            selected = Some((name, text));
            break;
        }
        settling = true;
    }
    let (brief, text) =
        selected.ok_or_else(|| match (settling, requires_include && include.is_none()) {
            (true, _) => Error::new("brief_settling", "assignment briefs are still settling"),
            (false, true) => Error::new(
                "brief_requires_include",
                "an older brief requires explicit Owner inclusion",
            ),
            (false, false) => Error::new("not_found", "assignment folder has no new brief"),
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
    if let Some(name) = include {
        request.args["assignment"]["include"] = Value::String(name.to_owned());
    }
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

fn marker_is_ready(path: &Path, version: BriefVersion) -> eliot_core::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(marker) if marker.is_file() && !is_link(&marker) && marker.len() == 0 => {
            Ok(modified(&marker)? >= version.modified)
        }
        Ok(_) => Ok(false),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(false),
        Err(_) => Err(Error::new(
            "unavailable",
            "cannot inspect brief ready marker",
        )),
    }
}

fn read_unchanged_brief(path: &Path, version: BriefVersion) -> eliot_core::Result<Option<String>> {
    let Some(mut file) = open_brief(path)? else {
        return Ok(None);
    };
    let before = file
        .metadata()
        .map_err(|_| Error::new("unavailable", "cannot inspect assignment brief"))?;
    if !before.is_file() || is_link(&before) || BriefVersion::read(&before)? != version {
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
    if BriefVersion::read(&after)? != version {
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
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ,
        };
        options
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .share_mode(FILE_SHARE_READ);
    }
    match options.open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        #[cfg(windows)]
        Err(error)
            if error.raw_os_error()
                == Some(windows_sys::Win32::Foundation::ERROR_SHARING_VIOLATION as i32) =>
        {
            Ok(None)
        }
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
