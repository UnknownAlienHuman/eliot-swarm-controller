use std::{
    env,
    ffi::OsString,
    io::{self, BufRead, BufReader},
    path::PathBuf,
};
use swarm_observer::{MAX_DIAGNOSTIC_BYTES, Recorder, RecorderConfig};

fn usage() -> ! {
    eprintln!(
        "usage: swarm-observer <absolute-private-directory> [--queue-records N] [--queue-bytes N] [--max-record-bytes N] [--segment-bytes N] [--retention-bytes N] [--retention-days N]"
    );
    std::process::exit(64);
}

fn next_u64(args: &mut impl Iterator<Item = OsString>) -> u64 {
    args.next()
        .and_then(|value| value.to_str().and_then(|text| text.parse().ok()))
        .unwrap_or_else(|| usage())
}

fn parse_args() -> RecorderConfig {
    let mut args = env::args_os().skip(1);
    let Some(directory) = args.next() else {
        usage()
    };
    let mut config = RecorderConfig {
        directory: PathBuf::from(directory),
        queue_records: 256,
        queue_bytes: 1_048_576,
        max_record_bytes: MAX_DIAGNOSTIC_BYTES,
        segment_bytes: 16_777_216,
        retention_bytes: 134_217_728,
        retention_days: 7,
    };
    while let Some(flag) = args.next() {
        match flag.to_str() {
            Some("--queue-records") => {
                config.queue_records =
                    usize::try_from(next_u64(&mut args)).unwrap_or_else(|_| usage());
            }
            Some("--queue-bytes") => {
                config.queue_bytes =
                    usize::try_from(next_u64(&mut args)).unwrap_or_else(|_| usage());
            }
            Some("--max-record-bytes") => {
                config.max_record_bytes =
                    usize::try_from(next_u64(&mut args)).unwrap_or_else(|_| usage());
            }
            Some("--segment-bytes") => config.segment_bytes = next_u64(&mut args),
            Some("--retention-bytes") => config.retention_bytes = next_u64(&mut args),
            Some("--retention-days") => config.retention_days = next_u64(&mut args),
            _ => usage(),
        }
    }
    config
}

/// Consume one newline-delimited record while retaining at most the schema
/// maximum. The byte count continues to advance while an overlong line is
/// discarded, so the loss counters reflect the full record content length
/// (excluding its newline delimiter).
struct ReadLineError {
    observed_bytes: u64,
    has_record: bool,
}

fn read_bounded_line<R: BufRead>(
    reader: &mut R,
    line: &mut Vec<u8>,
) -> Result<Option<(bool, u64)>, ReadLineError> {
    line.clear();
    let mut observed_bytes = 0_u64;
    let mut oversized = false;
    let mut saw_content = false;
    loop {
        let (consumed, ended) = {
            let available = reader.fill_buf().map_err(|_| ReadLineError {
                observed_bytes,
                has_record: saw_content,
            })?;
            if available.is_empty() {
                return Ok(saw_content.then_some((oversized, observed_bytes)));
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
            return Ok(Some((oversized, observed_bytes)));
        }
    }
}

fn main() {
    let config = parse_args();
    let recorder = match Recorder::start(config) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("{}", serde_json::json!({"error":error.code}));
            std::process::exit(1);
        }
    };
    let stdin = io::stdin();
    let mut input = BufReader::new(stdin.lock());
    let mut line = Vec::with_capacity(MAX_DIAGNOSTIC_BYTES);
    let mut input_failed = false;
    loop {
        match read_bounded_line(&mut input, &mut line) {
            Ok(Some((true, observed_bytes))) => recorder.record_dropped_input(observed_bytes),
            Ok(Some((false, _))) => {
                let _ = recorder.append_line(&line);
            }
            Ok(None) => break,
            Err(error) => {
                if error.has_record {
                    recorder.record_dropped_input(error.observed_bytes);
                }
                input_failed = true;
                break;
            }
        }
    }
    let (stats, shutdown_result) = recorder.shutdown_with_status();
    eprintln!(
        "{}",
        serde_json::json!({
            "accepted_records": stats.accepted_records,
            "written_records": stats.written_records,
            "written_bytes": stats.written_bytes,
            "dropped_records": stats.dropped_records,
            "dropped_bytes": stats.dropped_bytes,
            "durability_unknown_records": stats.durability_unknown_records,
            "durability_unknown_bytes": stats.durability_unknown_bytes,
            "sink_failures": stats.sink_failures,
            "pending_records": stats.pending_records,
            "pending_bytes": stats.pending_bytes
        })
    );
    if input_failed {
        eprintln!("{}", serde_json::json!({"error":"OBSERVER_INPUT_FAILED"}));
    }
    if input_failed || shutdown_result.is_err() || stats.pending_records != 0 {
        std::process::exit(1);
    }
}
