//! Bounded JSONL framing and domain-validation verdicts for durable journals.
//!
//! This module does not repair files or replay records. Callers must use their
//! existing domain decoder to validate each complete frame, then decide what
//! recovery policy is safe for their journal.

use std::ops::Range;

use sha2::{Digest, Sha256};
use swarm_contracts::error::{Error, Result};

/// The result of framing a journal and validating every complete record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonlScanVerdict {
    /// Every byte belongs to a newline-terminated record accepted by the
    /// caller's validator. Each range includes its terminating `\n`.
    Complete {
        complete_ranges: Vec<Range<usize>>,
        valid_bytes: usize,
    },
    /// Every complete record was accepted, followed by a bounded nonempty
    /// suffix without a newline. The suffix bytes are not exposed; callers
    /// can retain only its length and digest as diagnostic evidence.
    TornLastFrame {
        complete_ranges: Vec<Range<usize>>,
        valid_bytes: usize,
        tail_digest: String,
        tail_bytes: usize,
    },
    /// A complete frame failed domain validation or a frame exceeded the
    /// supplied bound. The byte offset is the beginning of the damaged frame;
    /// earlier frames may be useful for diagnostics but must not be treated
    /// as a recovered journal.
    Damaged {
        valid_prefix_bytes: usize,
        reason: JsonlDamage,
    },
}

/// A fail-closed JSONL damage classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonlDamage {
    /// The caller's current record decoder rejected a newline-terminated
    /// frame, including a malformed final complete frame.
    InvalidCompleteRecord,
    /// A newline-terminated frame exceeded the configured byte bound.
    OversizedCompleteRecord {
        record_bytes: usize,
        max_record_bytes: usize,
    },
    /// An unterminated final suffix exceeded the configured byte bound.
    OversizedTornTail {
        tail_bytes: usize,
        max_record_bytes: usize,
    },
}

/// Validate complete newline-terminated frames and classify EOF.
///
/// `max_record_bytes` counts the terminating newline for complete records.
/// The callback receives the exact frame bytes and its range in `bytes`. It
/// should validate with the adapter's existing decoder and avoid committing
/// caller state until this function returns `Complete` or `TornLastFrame`;
/// any `Damaged` verdict invalidates the journal as a whole. A torn suffix is
/// never passed to the decoder. This function does not truncate, discard, or
/// replay anything.
pub fn scan_jsonl<F>(
    bytes: &[u8],
    max_record_bytes: usize,
    mut validate_complete_record: F,
) -> Result<JsonlScanVerdict>
where
    F: FnMut(Range<usize>, &[u8]) -> Result<()>,
{
    if max_record_bytes == 0 {
        return Err(Error::invalid("JSONL record bound must be nonzero"));
    }

    let mut complete_ranges = Vec::new();
    let mut record_start = 0;
    while record_start < bytes.len() {
        let newline = bytes[record_start..].iter().position(|byte| *byte == b'\n');
        let Some(newline) = newline else {
            let tail_bytes = bytes.len() - record_start;
            if tail_bytes > max_record_bytes {
                return Ok(JsonlScanVerdict::Damaged {
                    valid_prefix_bytes: record_start,
                    reason: JsonlDamage::OversizedTornTail {
                        tail_bytes,
                        max_record_bytes,
                    },
                });
            }
            let tail_digest = format!("{:x}", Sha256::digest(&bytes[record_start..]));
            return Ok(JsonlScanVerdict::TornLastFrame {
                complete_ranges,
                valid_bytes: record_start,
                tail_digest,
                tail_bytes,
            });
        };

        let record_end = record_start + newline + 1;
        let record_bytes = record_end - record_start;
        if record_bytes > max_record_bytes {
            return Ok(JsonlScanVerdict::Damaged {
                valid_prefix_bytes: record_start,
                reason: JsonlDamage::OversizedCompleteRecord {
                    record_bytes,
                    max_record_bytes,
                },
            });
        }

        let range = record_start..record_end;
        if validate_complete_record(range.clone(), &bytes[range.clone()]).is_err() {
            return Ok(JsonlScanVerdict::Damaged {
                valid_prefix_bytes: record_start,
                reason: JsonlDamage::InvalidCompleteRecord,
            });
        }
        complete_ranges.push(range);
        record_start = record_end;
    }

    Ok(JsonlScanVerdict::Complete {
        complete_ranges,
        valid_bytes: bytes.len(),
    })
}
