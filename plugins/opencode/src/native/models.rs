//! Read-only OpenCode model catalogue from the frozen native child.
//!
//! The plugin runs its frozen snapshot `cmd` with the exact argv `models`
//! (observed `opencode models`, opencode 2.2.7): no session, no turn and no
//! inference. donor: `plugins/antigravity/src/native/models.rs`
//! (`run_models`) for the bounded child pattern (exact argv, stdin null,
//! piped stdout with an output cap, stderr discarded, kill-and-reap under a
//! deadline, static error strings). The installed `--help` documents
//! `models` as "List all available models" (the read-only surface);
//! `GET /openapi.json` of `opencode serve` describes the per-session
//! `GET /api/model` inventory instead, which needs a live session and is
//! therefore not the catalogue reader.

use std::{ffi::OsStr, process::Stdio, time::Duration};

use eliot_core::{Error, Result};
use tokio::{
    io::AsyncReadExt,
    process::Command,
    time::{Instant, timeout, timeout_at},
};

/// Exact argv for the catalogue child: `models`, nothing else.
pub(crate) fn models_argv() -> [&'static str; 1] {
    ["models"]
}

/// Output cap for the catalogue read: the observed catalogue is tens of KiB
/// (408 ids), so 512 KiB leaves headroom while bounding a misbehaving child.
pub(crate) const MAX_MODELS_BYTES: usize = 512 * 1024;

/// Maximum catalogue ids, mirroring `MAX_HARNESS_MODELS` in Core.
pub(crate) const MAX_MODELS: usize = 1024;

/// Maximum model id bytes, mirroring `MAX_HARNESS_MODEL_ID_BYTES` in Core.
pub(crate) const MAX_MODEL_ID_BYTES: usize = 256;

/// Grace for reaping a catalogue child after the kill signal.
const KILL_GRACE: Duration = Duration::from_secs(2);

/// Run the frozen native exe with exact argv `models` and return its
/// `provider/model` ids verbatim, in wire order. Bounded by `deadline` and
/// [`MAX_MODELS_BYTES`]; stderr is discarded like every other helper, so
/// failures are static typed strings naming the `models` surface.
pub(crate) async fn run_models(program: &OsStr, deadline: Instant) -> Result<Vec<String>> {
    let mut command = Command::new(program);
    command
        .args(models_argv())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|_| Error::new("native_spawn", "OpenCode models could not start"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("native_spawn", "OpenCode models could not start"))?;
    let mut buffer = Vec::new();
    let read = async {
        let mut limited = (&mut stdout).take((MAX_MODELS_BYTES + 1) as u64);
        limited.read_to_end(&mut buffer).await
    };
    match timeout_at(deadline, read).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode models did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_readback",
                "OpenCode models metadata was unreadable",
            ));
        }
        Ok(Ok(_)) => {}
    }
    if buffer.len() > MAX_MODELS_BYTES {
        kill_and_reap(&mut child).await;
        return Err(Error::new(
            "native_readback",
            "OpenCode models metadata was unreadable",
        ));
    }
    let status = match timeout_at(deadline, child.wait()).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode models did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            return Err(Error::new(
                "native_readback",
                "OpenCode models metadata was unreadable",
            ));
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() {
        return Err(Error::new(
            "native_readback",
            "OpenCode models did not return a catalogue",
        ));
    }
    let text = String::from_utf8(buffer)
        .map_err(|_| Error::new("native_readback", "OpenCode models metadata was unreadable"))?;
    parse_models_output(&text)
}

/// Parse `opencode models` stdout verbatim: one `provider/model` id per
/// line (nested segments such as `modal/deepseek-ai/DeepSeek-V4.1-Flash`
/// stay intact). Ids keep their exact bytes (a trailing `\r` is a line
/// ending, not part of the id); blank lines are skipped, never
/// interpreted. Anything else malformed (controls, whitespace inside an id)
/// fails the read instead of guessed.
pub(crate) fn parse_models_output(text: &str) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    for line in text.lines() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.trim().is_empty() {
            continue;
        }
        if line.len() > MAX_MODEL_ID_BYTES
            || line.chars().any(char::is_control)
            || line.chars().any(char::is_whitespace)
        {
            return Err(Error::new(
                "source_integrity",
                "OpenCode models catalogue is malformed",
            ));
        }
        ids.push(line.to_owned());
        if ids.len() > MAX_MODELS {
            return Err(Error::new(
                "source_integrity",
                "OpenCode models catalogue exceeds 1024 entries",
            ));
        }
    }
    Ok(ids)
}

async fn kill_and_reap(child: &mut tokio::process::Child) -> bool {
    // `start_kill` signals only; `kill` would already wait unboundedly for
    // exit (tokio `Child::kill` awaits `wait`), so the single bounded wait
    // below is the only blocking call in cleanup.
    let _ = child.start_kill();
    timeout(KILL_GRACE, child.wait())
        .await
        .is_ok_and(|wait| wait.is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Recorded `opencode models` shape (opencode 2.2.7): one provider/model
    // id per line, nested segments intact, no display names.
    const RECORDED: &str = "modal/deepseek-ai/DeepSeek-V4.1-Flash\n\
        modal/meta-models/Muse-Spark-Open-816B-A42B\n\
        opencode-go/deepseek-v4.1-flash\n\
        opencode-go/gpt-6-luna\n";

    #[test]
    fn recorded_catalogue_parses_ids_verbatim() {
        let ids = parse_models_output(RECORDED).expect("recorded output parses");
        assert_eq!(
            ids,
            vec![
                "modal/deepseek-ai/DeepSeek-V4.1-Flash",
                "modal/meta-models/Muse-Spark-Open-816B-A42B",
                "opencode-go/deepseek-v4.1-flash",
                "opencode-go/gpt-6-luna",
            ]
        );
    }

    #[test]
    fn blank_and_crlf_lines_are_skipped() {
        let ids = parse_models_output("opencode-go/beam-free\r\n\n  \nopencode-go/kimi-k3\r\n")
            .expect("blanks are skipped");
        assert_eq!(ids, vec!["opencode-go/beam-free", "opencode-go/kimi-k3"]);
    }

    #[test]
    fn whitespace_inside_id_is_malformed() {
        let error = parse_models_output("opencode go/model\n").unwrap_err();
        assert_eq!(error.code, "source_integrity");
        let error = parse_models_output(" provider/model\n").unwrap_err();
        assert_eq!(error.code, "source_integrity");
    }

    #[test]
    fn catalogue_caps_at_1024_entries() {
        let mut text = String::new();
        for index in 0..=MAX_MODELS {
            text.push_str(&format!("provider/model-{index}\n"));
        }
        let error = parse_models_output(&text).unwrap_err();
        assert_eq!(error.code, "source_integrity");
        assert!(error.message.contains("1024"));
    }

    #[test]
    fn empty_catalogue_reads_empty_without_inference() {
        assert_eq!(
            parse_models_output("").expect("empty parses"),
            Vec::<String>::new()
        );
    }
}
