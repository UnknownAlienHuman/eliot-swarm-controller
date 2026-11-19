//! Read-only Antigravity model catalogue from the frozen native child.
//!
//! The plugin runs its frozen snapshot `cmd` with the exact argv `models`
//! (observed `agy models`, agy 1.5.9): no session, no turn and no inference.
//! donor: `plugins/opencode/src/native/auth.rs` (`run_list`) for the bounded
//! child pattern (exact argv, stdin null, piped stdout with an output cap,
//! stderr discarded, kill-and-reap under a deadline, static error strings).
//! The child environment overlays the mapped launch variables onto the
//! inherited plugin environment, the same construction as the launch path
//! (see `crate::profile::reader_environment`).

use std::{
    ffi::{OsStr, OsString},
    process::Stdio,
    time::Duration,
};

use eliot_core::api::{HarnessModelEntry, harness::native_model_display_name};
use eliot_core::{Error, Result};
use eliot_plugin::native_lifecycle::kill_and_reap;
use tokio::{
    io::AsyncReadExt,
    process::Command,
    time::{Instant, timeout_at},
};

/// Exact argv for the catalogue child: `models`, nothing else.
pub(crate) fn models_argv() -> [&'static str; 1] {
    ["models"]
}

/// How long the catalogue child may run before it is killed and reaped.
pub(crate) const MODELS_TIMEOUT: Duration = Duration::from_secs(25);

/// Output cap for the catalogue read: the observed catalogue is under 1 KiB,
/// so 64 KiB leaves ample headroom while bounding a misbehaving child.
pub(crate) const MAX_MODELS_BYTES: usize = 64 * 1024;

/// Maximum catalogue ids, mirroring `MAX_HARNESS_MODELS` in Core.
pub(crate) const MAX_MODELS: usize = 1024;

/// Maximum model id bytes, mirroring `MAX_HARNESS_MODEL_ID_BYTES` in Core.
pub(crate) const MAX_MODEL_ID_BYTES: usize = 256;

/// Run the frozen native exe with exact argv `models` and return its entries
/// in wire order. Bounded by `deadline` and [`MAX_MODELS_BYTES`];
/// stderr is discarded like every other helper, so failures are static typed
/// strings naming the `models` surface, never native text.
pub(crate) async fn run_models(
    program: &OsStr,
    environment: &[(OsString, OsString)],
    deadline: Instant,
) -> Result<Vec<HarnessModelEntry>> {
    let mut command = Command::new(program);
    command
        .args(models_argv())
        .envs(environment.iter().map(|(name, value)| (name, value)))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|_| Error::new("native_spawn", "Antigravity models could not start"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("native_spawn", "Antigravity models could not start"))?;
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
                "Antigravity models did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_readback",
                "Antigravity models metadata was unreadable",
            ));
        }
        Ok(Ok(_)) => {}
    }
    if buffer.len() > MAX_MODELS_BYTES {
        kill_and_reap(&mut child).await;
        return Err(Error::new(
            "native_readback",
            "Antigravity models metadata was unreadable",
        ));
    }
    let status = match timeout_at(deadline, child.wait()).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "Antigravity models did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            return Err(Error::new(
                "native_readback",
                "Antigravity models metadata was unreadable",
            ));
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() {
        return Err(Error::new(
            "native_readback",
            "Antigravity models did not return a catalogue",
        ));
    }
    let text = String::from_utf8(buffer).map_err(|_| {
        Error::new(
            "native_readback",
            "Antigravity models metadata was unreadable",
        )
    })?;
    parse_models_output(&text)
}

/// Parse `agy models` stdout verbatim: one `<id>\t<display name>` line per
/// model. Blank lines and tab-free status lines are skipped. Malformed ids
/// fail the read and unusable display names are omitted.
pub(crate) fn parse_models_output(text: &str) -> Result<Vec<HarnessModelEntry>> {
    let mut models = Vec::new();
    for line in text.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Some((id, name)) = line.split_once('\t') else {
            continue;
        };
        if id.is_empty()
            || id.len() > MAX_MODEL_ID_BYTES
            || id.chars().any(char::is_control)
            || id.chars().any(char::is_whitespace)
        {
            return Err(Error::new(
                "source_integrity",
                "Antigravity models catalogue is malformed",
            ));
        }
        models.push(HarnessModelEntry {
            id: id.to_owned(),
            display_name: native_model_display_name(name).map(str::to_owned),
            default: false,
            efforts: None,
            default_effort: None,
            source: None,
        });
        if models.len() > MAX_MODELS {
            return Err(Error::new(
                "source_integrity",
                "Antigravity models catalogue exceeds 1024 entries",
            ));
        }
    }
    Ok(models)
}
