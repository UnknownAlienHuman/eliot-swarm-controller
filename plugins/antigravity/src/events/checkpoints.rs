use serde::Deserialize;
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
};

const MAX_TRANSCRIPT_TAIL_BYTES: u64 = 256 * 1024;

pub(super) struct Checkpoints {
    path: PathBuf,
    last_step: Option<u64>,
    seeded: bool,
}

#[derive(Deserialize)]
struct TranscriptStep<'a> {
    step_index: u64,
    source: &'a str,
    #[serde(rename = "type")]
    kind: &'a str,
    status: &'a str,
}

impl Checkpoints {
    pub(super) fn bind(root: &Path, conversation: &str, resumed: bool) -> Option<Self> {
        Some(Self::new(
            root.join(relative_transcript(conversation)?),
            resumed,
        ))
    }

    pub(super) fn from_hook(path: &Path, conversation: &str, resumed: bool) -> Option<Self> {
        let relative = relative_transcript(conversation)?;
        if !path.is_absolute()
            || !path.ends_with(&relative)
            || path.components().any(|part| part == Component::ParentDir)
        {
            return None;
        }
        Some(Self::new(path.to_owned(), resumed))
    }

    fn new(path: PathBuf, resumed: bool) -> Self {
        let mut reader = Self {
            path,
            last_step: None,
            seeded: !resumed,
        };
        if resumed {
            reader.read_new();
        }
        reader
    }

    /// Read only complete retained transcript rows. The checkpoint summary
    /// stays in the native transcript and never enters a Swarm event.
    pub(super) fn read_new(&mut self) -> Vec<u64> {
        let Some(bytes) = read_tail(&self.path) else {
            return Vec::new();
        };
        let mut latest = self.last_step;
        let mut checkpoints = Vec::new();
        for line in bytes.split_inclusive(|byte| *byte == b'\n') {
            if line.last() != Some(&b'\n') {
                break;
            }
            let Ok(step) = serde_json::from_slice::<TranscriptStep<'_>>(line) else {
                continue;
            };
            if latest.is_some_and(|previous| step.step_index <= previous) {
                continue;
            }
            latest = Some(step.step_index);
            if self.seeded
                && step.source == "SYSTEM"
                && step.kind == "CHECKPOINT"
                && step.status == "DONE"
            {
                checkpoints.push(step.step_index);
            }
        }
        self.last_step = latest;
        self.seeded |= latest.is_some();
        checkpoints
    }
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

fn read_tail(path: &Path) -> Option<Vec<u8>> {
    eliot_plugin::edit_path::reject_links(path).ok()?;
    let mut file = File::open(path).ok()?;
    let metadata = file.metadata().ok()?;
    if !metadata.is_file() {
        return None;
    }
    let start = metadata.len().saturating_sub(MAX_TRANSCRIPT_TAIL_BYTES);
    file.seek(SeekFrom::Start(start)).ok()?;
    let mut bytes = Vec::new();
    file.take(MAX_TRANSCRIPT_TAIL_BYTES)
        .read_to_end(&mut bytes)
        .ok()?;
    if start != 0 {
        let first_boundary = bytes.iter().position(|byte| *byte == b'\n')?;
        bytes.drain(..=first_boundary);
    }
    Some(bytes)
}
