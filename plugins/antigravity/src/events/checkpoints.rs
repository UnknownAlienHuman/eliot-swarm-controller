use serde::Deserialize;
use std::{
    fs::{self, File, Metadata},
    io::{Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    time::SystemTime,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

const MAX_TRANSCRIPT_READ_CHUNK_BYTES: usize = 256 * 1024;
const MAX_TRANSCRIPT_ROW_BYTES: usize = 256 * 1024;
const ANCHOR_BYTES: usize = 64;

pub(super) struct Checkpoints {
    path: PathBuf,
    offset: u64,
    last_step: Option<u64>,
    needs_seed: bool,
    line: Vec<u8>,
    discard_line: bool,
    skip_partial_row: bool,
    observed_len: Option<u64>,
    observed_modified: Option<SystemTime>,
    observed_identity: Option<FileIdentity>,
    prefix_anchor: Option<Vec<u8>>,
    cursor_anchor: Vec<u8>,
}

#[derive(Deserialize)]
struct TranscriptStep<'a> {
    step_index: u64,
    source: &'a str,
    #[serde(rename = "type")]
    kind: &'a str,
    status: &'a str,
}

#[cfg(unix)]
#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(not(unix))]
#[derive(Clone, Copy, PartialEq, Eq)]
struct FileIdentity;

impl Checkpoints {
    pub(super) fn bind(root: &Path, conversation: &str, resumed: bool) -> Option<Self> {
        Some(Self::new(
            root.join(relative_transcript(conversation)?),
            resumed,
        ))
    }

    pub(super) fn from_hook(
        path: &Path,
        eligible_root: &Path,
        conversation: &str,
        resumed: bool,
    ) -> Option<Self> {
        let relative = relative_transcript(conversation)?;
        if !path.is_absolute()
            || path.components().any(|part| part == Component::ParentDir)
            || !eligible_root.is_absolute()
        {
            return None;
        }
        eliot_plugin::edit_path::reject_links(eligible_root).ok()?;
        eliot_plugin::edit_path::reject_links(path).ok()?;
        let root = fs::canonicalize(eligible_root).ok()?;
        if !root.is_dir() {
            return None;
        }
        let transcript = fs::canonicalize(path).ok()?;
        let expected = root.join(relative);
        if transcript != expected || !transcript.starts_with(&root) {
            return None;
        }
        Some(Self::new(transcript, resumed))
    }

    fn new(path: PathBuf, resumed: bool) -> Self {
        let mut reader = Self {
            path,
            offset: 0,
            last_step: None,
            needs_seed: resumed,
            line: Vec::new(),
            discard_line: false,
            skip_partial_row: false,
            observed_len: None,
            observed_modified: None,
            observed_identity: None,
            prefix_anchor: None,
            cursor_anchor: Vec::new(),
        };
        if resumed {
            reader.seed_if_available();
        }
        reader
    }

    /// Consume newly appended, complete transcript rows. Each forward file
    /// read has a fixed byte limit; an incomplete or oversized row is never
    /// parsed as a checkpoint.
    pub(super) fn read_new(&mut self) -> Vec<u64> {
        let Some(mut file) = self.open() else {
            return Vec::new();
        };
        let Ok(metadata) = file.metadata() else {
            return Vec::new();
        };
        if !metadata.is_file() {
            return Vec::new();
        }
        if self.needs_seed || self.was_rewritten(&mut file, &metadata) {
            self.seed_file(&mut file, &metadata);
            return Vec::new();
        }

        let checkpoints = self.scan_to(&mut file, metadata.len(), true);
        self.remember_file(&mut file, &metadata);
        checkpoints
    }

    fn open(&self) -> Option<File> {
        eliot_plugin::edit_path::reject_links(&self.path).ok()?;
        File::open(&self.path).ok()
    }

    fn seed_if_available(&mut self) {
        let Some(mut file) = self.open() else {
            return;
        };
        let Ok(metadata) = file.metadata() else {
            return;
        };
        if metadata.is_file() {
            self.seed_file(&mut file, &metadata);
        }
    }

    /// Establish a no-replay baseline. If a trailing row is incomplete, keep
    /// its bytes and suppress that row if it is completed after the seed.
    fn seed_file(&mut self, file: &mut File, metadata: &Metadata) {
        self.offset = 0;
        self.last_step = None;
        self.line.clear();
        self.discard_line = false;
        self.skip_partial_row = false;
        self.prefix_anchor = None;
        self.cursor_anchor.clear();
        self.scan_to(file, metadata.len(), false);
        self.skip_partial_row = !self.line.is_empty() || self.discard_line;
        self.needs_seed = false;
        self.remember_file(file, metadata);
    }

    fn was_rewritten(&self, file: &mut File, metadata: &Metadata) -> bool {
        let Some(previous_len) = self.observed_len else {
            return false;
        };
        let len = metadata.len();
        if len < self.offset || len < previous_len {
            return true;
        }
        if self
            .observed_identity
            .zip(file_identity(metadata))
            .is_some_and(|(previous, current)| previous != current)
        {
            return true;
        }
        let modified = metadata.modified().ok();
        if len == previous_len
            && self.observed_modified.is_some()
            && modified != self.observed_modified
        {
            return true;
        }
        !self.anchors_match(file)
    }

    fn anchors_match(&self, file: &mut File) -> bool {
        if let Some(anchor) = &self.prefix_anchor
            && read_window(file, 0, anchor.len()).as_ref() != Some(anchor)
        {
            return false;
        }
        if !self.cursor_anchor.is_empty() {
            let start = self.offset.saturating_sub(self.cursor_anchor.len() as u64);
            if read_window(file, start, self.cursor_anchor.len()).as_ref()
                != Some(&self.cursor_anchor)
            {
                return false;
            }
        }
        true
    }

    fn scan_to(&mut self, file: &mut File, end: u64, emit: bool) -> Vec<u64> {
        let mut checkpoints = Vec::new();
        if file.seek(SeekFrom::Start(self.offset)).is_err() {
            return checkpoints;
        }
        let mut chunk = vec![0; MAX_TRANSCRIPT_READ_CHUNK_BYTES];
        while self.offset < end {
            let read_limit =
                (end - self.offset).min(MAX_TRANSCRIPT_READ_CHUNK_BYTES as u64) as usize;
            let read = match file.read(&mut chunk[..read_limit]) {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            };
            for byte in &chunk[..read] {
                self.offset += 1;
                self.push_byte(*byte, emit, &mut checkpoints);
            }
        }
        checkpoints
    }

    fn push_byte(&mut self, byte: u8, emit: bool, checkpoints: &mut Vec<u64>) {
        if self.discard_line {
            if byte == b'\n' {
                self.discard_line = false;
                self.skip_partial_row = false;
            }
            return;
        }
        if byte == b'\n' {
            let suppressed = std::mem::take(&mut self.skip_partial_row);
            if !suppressed {
                self.consume_line(emit, checkpoints);
            }
            self.line.clear();
            return;
        }
        if self.line.len() == MAX_TRANSCRIPT_ROW_BYTES {
            self.line.clear();
            self.discard_line = true;
            return;
        }
        self.line.push(byte);
    }

    fn consume_line(&mut self, emit: bool, checkpoints: &mut Vec<u64>) {
        let Ok(step) = serde_json::from_slice::<TranscriptStep<'_>>(&self.line) else {
            return;
        };
        if self
            .last_step
            .is_some_and(|previous| step.step_index <= previous)
        {
            return;
        }
        self.last_step = Some(step.step_index);
        if emit && step.source == "SYSTEM" && step.kind == "CHECKPOINT" && step.status == "DONE" {
            checkpoints.push(step.step_index);
        }
    }

    fn remember_file(&mut self, file: &mut File, metadata: &Metadata) {
        self.observed_len = Some(metadata.len());
        self.observed_modified = metadata.modified().ok();
        self.observed_identity = file_identity(metadata);
        let anchor_len = self.offset.min(ANCHOR_BYTES as u64) as usize;
        self.prefix_anchor = (anchor_len > 0)
            .then(|| read_window(file, 0, anchor_len))
            .flatten();
        self.cursor_anchor = if anchor_len > 0 {
            read_window(file, self.offset - anchor_len as u64, anchor_len).unwrap_or_default()
        } else {
            Vec::new()
        };
    }
}

fn file_identity(metadata: &Metadata) -> Option<FileIdentity> {
    #[cfg(unix)]
    {
        Some(FileIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        None
    }
}

fn read_window(file: &mut File, start: u64, len: usize) -> Option<Vec<u8>> {
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes).ok()?;
    Some(bytes)
}

fn relative_transcript(conversation: &str) -> Option<PathBuf> {
    if conversation.is_empty()
        || conversation.len() > 128
        || !conversation
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return None;
    }
    Some(
        Path::new("antigravity-cli")
            .join("brain")
            .join(conversation)
            .join(".system_generated")
            .join("logs")
            .join("transcript.jsonl"),
    )
}
