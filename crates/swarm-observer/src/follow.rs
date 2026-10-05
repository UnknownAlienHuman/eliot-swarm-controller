//! Explicit local follow for the optional metadata recorder files.
//!
//! The cursor is a `(segment, byte offset)` in this diagnostic file stream,
//! not a Store journal cursor. If retention removes a requested segment the
//! reader emits an explicit gap. Each output line is a closed decoded record;
//! a slow stdout consumer blocks only this CLI and retains at most one bounded
//! input line.

use crate::{DiagnosticRecord, MAX_DIAGNOSTIC_BYTES, decode_line};
use serde::Serialize;
use std::{
    fs::{self, File},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};
use swarm_contracts::error::{Error, Result};

const MAX_SEGMENTS_PER_SCAN: usize = 4096;
const MAX_DIRECTORY_ENTRIES_PER_SCAN: usize = 8192;
const FOLLOW_POLL: Duration = Duration::from_millis(250);

#[derive(Clone, Copy, Debug, Serialize)]
pub struct FileCursor {
    pub segment: u64,
    pub offset: u64,
}

#[derive(Serialize)]
struct RecordEnvelope<'a> {
    cursor: FileCursor,
    record: &'a DiagnosticRecord,
}

#[derive(Serialize)]
struct GapEnvelope {
    gap: Gap,
    cursor: FileCursor,
}

#[derive(Serialize)]
struct Gap {
    reason: &'static str,
    from_segment: Option<u64>,
    to_segment: Option<u64>,
}

struct Segment {
    index: u64,
    path: PathBuf,
    length: u64,
}

/// Follow retained local JSONL segments. With no cursor, starts at the end of
/// the newest segment so callers see newly recorded events only. `follow=false`
/// drains currently available records and returns; `follow=true` waits for
/// segment growth. `follow` is an explicit CLI operation, never a host worker.
pub fn follow_local(
    directory: &Path,
    start: Option<FileCursor>,
    follow: bool,
    output: &mut impl Write,
) -> Result<()> {
    if !directory.is_absolute() {
        return Err(error(
            "OBSERVER_PATH_INVALID",
            "observer path must be absolute",
        ));
    }
    let explicit_cursor = start.is_some();
    let mut waiting_for_first_segment = start.is_none();
    let mut validate_boundary = explicit_cursor;
    let mut cursor = start;
    loop {
        let segments = list_segments(directory)?;
        if cursor.is_none() {
            if let Some(last) = segments.last() {
                cursor = Some(FileCursor {
                    segment: last.index,
                    offset: last.length,
                });
                waiting_for_first_segment = false;
            } else {
                cursor = Some(FileCursor {
                    segment: 0,
                    offset: 0,
                });
            }
            if !follow && segments.is_empty() {
                return Ok(());
            }
        }
        if waiting_for_first_segment && let Some(last) = segments.last() {
            cursor = Some(FileCursor {
                segment: last.index,
                offset: last.length,
            });
            waiting_for_first_segment = false;
        }
        let mut current = cursor.expect("cursor initialized above");
        if explicit_cursor && segments.is_empty() {
            return Err(error(
                "OBSERVER_CURSOR_UNAVAILABLE",
                "observer cursor cannot be checked because no retained segment exists",
            ));
        }
        if !segments.is_empty() && current.segment < segments[0].index {
            write_gap(
                output,
                Gap {
                    reason: "retention_gap",
                    from_segment: Some(current.segment),
                    to_segment: Some(segments[0].index),
                },
                FileCursor {
                    segment: segments[0].index,
                    offset: 0,
                },
            )?;
            current = FileCursor {
                segment: segments[0].index,
                offset: 0,
            };
            cursor = Some(current);
            validate_boundary = true;
        }
        if segments
            .last()
            .is_some_and(|last| current.segment > last.index)
        {
            return Err(error(
                "OBSERVER_CURSOR_INVALID",
                "observer cursor is ahead of retained segments",
            ));
        }

        let Some(segment) = segments
            .iter()
            .find(|segment| segment.index == current.segment)
        else {
            if let Some(next) = segments
                .iter()
                .find(|segment| segment.index > current.segment)
            {
                write_gap(
                    output,
                    Gap {
                        reason: "segment_missing",
                        from_segment: Some(current.segment),
                        to_segment: Some(next.index),
                    },
                    FileCursor {
                        segment: next.index,
                        offset: 0,
                    },
                )?;
                cursor = Some(FileCursor {
                    segment: next.index,
                    offset: 0,
                });
                validate_boundary = true;
                continue;
            }
            if !follow {
                return Ok(());
            }
            thread::sleep(FOLLOW_POLL);
            continue;
        };

        if current.offset > segment.length
            || (validate_boundary && !is_record_boundary(&segment.path, current.offset)?)
        {
            return Err(error(
                "OBSERVER_CURSOR_INVALID",
                "observer cursor is outside a record boundary",
            ));
        }
        if current.offset == segment.length {
            if let Some(next) = segments
                .iter()
                .find(|candidate| candidate.index > current.segment)
            {
                if next.index > current.segment.saturating_add(1) {
                    write_gap(
                        output,
                        Gap {
                            reason: "segment_gap",
                            from_segment: Some(current.segment.saturating_add(1)),
                            to_segment: Some(next.index),
                        },
                        FileCursor {
                            segment: next.index,
                            offset: 0,
                        },
                    )?;
                }
                cursor = Some(FileCursor {
                    segment: next.index,
                    offset: 0,
                });
                validate_boundary = true;
                continue;
            }
            if !follow {
                return Ok(());
            }
            thread::sleep(FOLLOW_POLL);
            continue;
        }

        let file = open_segment(&segment.path)?;
        let mut reader = BufReader::new(file);
        reader
            .seek(SeekFrom::Start(current.offset))
            .map_err(|_| error("OBSERVER_FILE_UNAVAILABLE", "observer segment seek failed"))?;
        let mut line = Vec::with_capacity(MAX_DIAGNOSTIC_BYTES);
        let Some(line_result) = read_bounded_line(&mut reader, &mut line)? else {
            if !follow {
                write_gap(
                    output,
                    Gap {
                        reason: "incomplete_tail",
                        from_segment: Some(current.segment),
                        to_segment: None,
                    },
                    current,
                )?;
                return Ok(());
            }
            thread::sleep(FOLLOW_POLL);
            continue;
        };
        let (oversized, content_bytes, newline_consumed) = line_result;
        if !newline_consumed && follow {
            thread::sleep(FOLLOW_POLL);
            continue;
        }
        let end_offset = current
            .offset
            .saturating_add(content_bytes)
            .saturating_add(u64::from(newline_consumed));
        let next_cursor = FileCursor {
            segment: current.segment,
            offset: end_offset,
        };
        if oversized {
            write_gap(
                output,
                Gap {
                    reason: "record_too_large",
                    from_segment: Some(current.segment),
                    to_segment: None,
                },
                next_cursor,
            )?;
            cursor = Some(next_cursor);
            validate_boundary = true;
            continue;
        }
        match decode_line(&line) {
            Ok(record) => write_record(output, &record, next_cursor)?,
            Err(_) => write_gap(
                output,
                Gap {
                    reason: "record_invalid",
                    from_segment: Some(current.segment),
                    to_segment: None,
                },
                next_cursor,
            )?,
        }
        cursor = Some(next_cursor);
        validate_boundary = true;
    }
}

fn list_segments(directory: &Path) -> Result<Vec<Segment>> {
    let metadata = match fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => {
            return Err(error(
                "OBSERVER_PATH_UNAVAILABLE",
                "observer directory is unavailable",
            ));
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(error(
            "OBSERVER_PATH_INVALID",
            "observer path must be a regular directory",
        ));
    }
    let entries = fs::read_dir(directory).map_err(|_| {
        error(
            "OBSERVER_PATH_UNAVAILABLE",
            "observer directory is unavailable",
        )
    })?;
    let mut segments = Vec::new();
    let mut scanned = 0_usize;
    for entry in entries {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_DIRECTORY_ENTRIES_PER_SCAN {
            return Err(error(
                "OBSERVER_SEGMENT_LIMIT",
                "observer directory scan exceeds its bound",
            ));
        }
        let entry = entry.map_err(|_| {
            error(
                "OBSERVER_PATH_UNAVAILABLE",
                "observer directory scan failed",
            )
        })?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(digits) = name
            .strip_prefix("diagnostics-")
            .and_then(|name| name.strip_suffix(".jsonl"))
        else {
            continue;
        };
        if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
            continue;
        }
        let index = digits.parse::<u64>().map_err(|_| {
            error(
                "OBSERVER_SEGMENT_INVALID",
                "observer segment name is invalid",
            )
        })?;
        let metadata = fs::symlink_metadata(entry.path()).map_err(|_| {
            error(
                "OBSERVER_FILE_UNAVAILABLE",
                "observer segment metadata is unavailable",
            )
        })?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(error(
                "OBSERVER_PATH_INVALID",
                "observer segment is not a regular file",
            ));
        }
        segments.push(Segment {
            index,
            path: entry.path(),
            length: metadata.len(),
        });
        if segments.len() > MAX_SEGMENTS_PER_SCAN {
            return Err(error(
                "OBSERVER_SEGMENT_LIMIT",
                "observer segment scan exceeds its bound",
            ));
        }
    }
    segments.sort_by_key(|segment| segment.index);
    Ok(segments)
}

fn open_segment(path: &Path) -> Result<File> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        error(
            "OBSERVER_FILE_UNAVAILABLE",
            "observer segment is unavailable",
        )
    })?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(error(
            "OBSERVER_PATH_INVALID",
            "observer segment is not a regular file",
        ));
    }
    File::open(path).map_err(|_| {
        error(
            "OBSERVER_FILE_UNAVAILABLE",
            "observer segment is unavailable",
        )
    })
}

fn is_record_boundary(path: &Path, offset: u64) -> Result<bool> {
    if offset == 0 {
        return Ok(true);
    }
    let mut file = open_segment(path)?;
    if offset
        > file
            .metadata()
            .map_err(|_| {
                error(
                    "OBSERVER_FILE_UNAVAILABLE",
                    "observer segment metadata is unavailable",
                )
            })?
            .len()
    {
        return Ok(false);
    }
    file.seek(SeekFrom::Start(offset - 1))
        .map_err(|_| error("OBSERVER_FILE_UNAVAILABLE", "observer segment seek failed"))?;
    let mut byte = [0_u8; 1];
    file.read_exact(&mut byte)
        .map_err(|_| error("OBSERVER_FILE_UNAVAILABLE", "observer segment read failed"))?;
    Ok(byte[0] == b'\n')
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> Result<Option<(bool, u64, bool)>> {
    line.clear();
    let mut observed_bytes = 0_u64;
    let mut oversized = false;
    let mut saw_content = false;
    loop {
        let (consumed, ended) = {
            let available = reader
                .fill_buf()
                .map_err(|_| error("OBSERVER_FILE_UNAVAILABLE", "observer segment read failed"))?;
            if available.is_empty() {
                return Ok(saw_content.then_some((oversized, observed_bytes, false)));
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let consumed = newline.map_or(available.len(), |position| position + 1);
            let content_bytes = if newline.is_some() {
                consumed - 1
            } else {
                consumed
            };
            if content_bytes > 0 {
                saw_content = true;
                observed_bytes = observed_bytes.saturating_add(content_bytes as u64);
                if !oversized {
                    if line.len().saturating_add(content_bytes) > MAX_DIAGNOSTIC_BYTES {
                        oversized = true;
                        line.clear();
                    } else {
                        line.extend_from_slice(&available[..content_bytes]);
                    }
                }
            }
            (consumed, newline.is_some())
        };
        reader.consume(consumed);
        if ended {
            return Ok(Some((oversized, observed_bytes, true)));
        }
    }
}

fn write_record(
    output: &mut impl Write,
    record: &DiagnosticRecord,
    cursor: FileCursor,
) -> Result<()> {
    serde_json::to_writer(&mut *output, &RecordEnvelope { cursor, record })
        .map_err(|_| error("OBSERVER_FOLLOW_OUTPUT_FAILED", "observer output failed"))?;
    output
        .write_all(b"\n")
        .map_err(|_| error("OBSERVER_FOLLOW_OUTPUT_FAILED", "observer output failed"))?;
    output
        .flush()
        .map_err(|_| error("OBSERVER_FOLLOW_OUTPUT_FAILED", "observer output failed"))
}

fn write_gap(output: &mut impl Write, gap: Gap, cursor: FileCursor) -> Result<()> {
    serde_json::to_writer(&mut *output, &GapEnvelope { gap, cursor })
        .map_err(|_| error("OBSERVER_FOLLOW_OUTPUT_FAILED", "observer output failed"))?;
    output
        .write_all(b"\n")
        .map_err(|_| error("OBSERVER_FOLLOW_OUTPUT_FAILED", "observer output failed"))?;
    output
        .flush()
        .map_err(|_| error("OBSERVER_FOLLOW_OUTPUT_FAILED", "observer output failed"))
}

fn error(code: &'static str, message: &'static str) -> Error {
    Error::new(code, message)
}
