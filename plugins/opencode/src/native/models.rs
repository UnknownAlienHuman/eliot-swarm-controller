//! Read-only OpenCode model catalogue from the frozen native child.
//!
//! The plugin runs its frozen snapshot `cmd` with `models --standalone`
//! (observed `opencode models --standalone`, opencode 2.2.7): no session, no turn and no
//! inference. The bounded child mechanics (exact argv, stdin null, piped
//! stdout with an output cap, stderr discarded, kill-and-reap under a
//! deadline, static error strings) live in
//! `eliot_plugin::native_cli::run_bounded_utf8`. The daemon owns one reader Job per
//! invocation and confirms family closure before returning its result. The
//! installed `--help` documents `models --standalone` as a read-only surface;
//! `GET /openapi.json` of `opencode serve` describes the per-session
//! `GET /api/model` inventory instead, which needs a live session and is
//! therefore not the catalogue reader.

use std::{ffi::OsStr, path::Path};

use super::apply_environment;
use eliot_core::api::harness::native_model_display_name;
use eliot_core::{Error, Result};
/// Maximum model id bytes, mirroring `MAX_HARNESS_MODEL_ID_BYTES` in Core;
/// shared with the other catalogue readers through `eliot-plugin`.
pub(crate) use eliot_plugin::native_cli::MAX_MODEL_ID_BYTES;
use eliot_plugin::native_cli::{BoundedReadLabels, run_bounded_utf8, valid_model_id};
use serde_json::Value;
#[expect(
    clippy::disallowed_types,
    reason = "OpenCode model discovery returns a command owned by the native adapter."
)]
use tokio::process::Command;
use tokio::time::Instant;

/// Keep the private server under the reader Job, never the background service.
pub(crate) fn models_argv() -> [&'static str; 2] {
    ["models", "--standalone"]
}

/// Output cap for the catalogue read: the observed catalogue is tens of KiB
/// (408 ids), so 512 KiB leaves headroom while bounding a misbehaving child.
pub(crate) const MAX_MODELS_BYTES: usize = 512 * 1024;

/// Maximum catalogue ids, mirroring `MAX_HARNESS_MODELS` in Core.
pub(crate) const MAX_MODELS: usize = 1024;

/// Exact argv for the model metadata read: `api model.list --standalone`. The
/// same read-only native surface as `models --standalone` (no shared server, no
/// session, no turn, no inference): it answers the installed `model.list`
/// operation (`GET /api/model`) against a private server, so
/// it sees the same authenticated providers as `models --standalone`.
pub(crate) fn model_list_argv() -> [&'static str; 3] {
    ["api", "model.list", "--standalone"]
}

/// Output cap for the metadata read: the observed inventory is ~320 KiB
/// UTF-8 (425 entries), so 2 MiB leaves headroom while bounding a
/// misbehaving child.
pub(crate) const MAX_MODEL_LIST_BYTES: usize = 2 * 1024 * 1024;

/// Maximum variant ids kept per model; the installed inventory peaks at
/// seven, so anything larger is a corrupt read, never a wider wardrobe.
pub(crate) const MAX_VARIANTS_PER_MODEL: usize = 64;

/// One catalogue model's presentation label and selectable variant ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeModelMetadata {
    /// Native `provider/model` id, verbatim.
    pub id: String,
    /// Verbatim `Model.Info.name` when present and valid.
    pub display_name: Option<String>,
    /// Selectable `Model.Variant` ids, when the model reports a variants array.
    pub variants: Option<Vec<String>>,
}

/// Native catalogue commands share the profile workspace and environment.
pub(crate) struct ReaderContext<'a> {
    pub workspace: &'a Path,
    pub environment: &'a [(std::ffi::OsString, std::ffi::OsString)],
    pub xdg: Option<(&'a Path, &'a Path)>,
    /// Typed local-provider reads must not accept native config overrides.
    pub local_provider: bool,
}

impl ReaderContext<'_> {
    #[expect(
        clippy::disallowed_types,
        reason = "This builder configures an OpenCode native model-query command."
    )]
    fn command(&self, program: &OsStr) -> Command {
        #[expect(
            clippy::disallowed_methods,
            reason = "This statement constructs a process owned by this module."
        )]
        let mut command = Command::new(program);
        command.current_dir(self.workspace);
        apply_environment(&mut command, self.environment, self.local_provider);
        if let Some((config_home, data_home)) = self.xdg {
            command
                .env("XDG_CONFIG_HOME", config_home)
                .env("XDG_DATA_HOME", data_home);
        }
        command
    }
}

/// Run the frozen native exe with exact argv `api model.list --standalone` and return
/// one entry per reported model with metadata, in wire
/// order. Bounded by `deadline` and [`MAX_MODEL_LIST_BYTES`]; like
/// [`run_models`] this is read-only (no session, no inference) and any
/// failure is a static typed string naming the `model.list` surface. The
/// daemon closes this reader's whole family before deciding whether an
/// optional metadata failure permits the ids-only catalogue.
pub(crate) async fn run_model_metadata(
    program: &OsStr,
    deadline: Instant,
    context: &ReaderContext<'_>,
) -> Result<Vec<NativeModelMetadata>> {
    let mut command = context.command(program);
    command.args(model_list_argv());
    let text = run_bounded_utf8(
        command,
        MAX_MODEL_LIST_BYTES,
        deadline,
        &BoundedReadLabels {
            start: "OpenCode model.list could not start",
            deadline: "OpenCode model.list did not finish before its deadline",
            unreadable: "OpenCode model.list metadata was unreadable",
            status: "OpenCode model.list did not return a catalogue",
        },
    )
    .await?;
    parse_model_list_output(&text)
}

/// Parse `opencode api model.list --standalone` stdout: the installed `{"data":
/// [Model.Info]}` envelope (a bare array decodes too). Entries join
/// `providerID`/`id` verbatim, keep `name` as the display name, and map
/// `variants` to selectable ids in wire order. Entries with an unusable
/// identity or no usable metadata are skipped.
pub(crate) fn parse_model_list_output(text: &str) -> Result<Vec<NativeModelMetadata>> {
    let value: Value = serde_json::from_str(text).map_err(|_| {
        Error::new(
            "native_readback",
            "OpenCode model.list metadata was unreadable",
        )
    })?;
    let items = value
        .get("data")
        .unwrap_or(&value)
        .as_array()
        .ok_or_else(|| {
            Error::new(
                "native_readback",
                "OpenCode model.list metadata was unreadable",
            )
        })?;
    if items.len() > MAX_MODELS {
        return Err(Error::new(
            "source_integrity",
            "OpenCode model.list catalogue exceeds 1024 entries",
        ));
    }
    let mut entries = Vec::with_capacity(items.len());
    for item in items {
        let provider = item
            .get("providerID")
            .and_then(Value::as_str)
            .filter(|provider| !provider.is_empty());
        let id = item
            .get("id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty());
        let (Some(provider), Some(id)) = (provider, id) else {
            continue;
        };
        if !valid_model_id(provider)
            || !valid_model_id(id)
            || provider.len() + id.len() + 1 > MAX_MODEL_ID_BYTES
        {
            continue;
        }
        let display_name = item
            .get("name")
            .and_then(Value::as_str)
            .and_then(native_model_display_name)
            .map(str::to_owned);
        let variants = item.get("variants").and_then(Value::as_array);
        if variants.is_some_and(|variants| variants.len() > MAX_VARIANTS_PER_MODEL) {
            return Err(Error::new(
                "source_integrity",
                "OpenCode model.list reports too many variants",
            ));
        }
        let variants = variants.map(|variants| {
            let mut ids = Vec::with_capacity(variants.len());
            for variant in variants {
                let Some(variant) = variant
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|variant| {
                        !variant.is_empty()
                            && variant.len() <= 128
                            && !variant.chars().any(char::is_control)
                            && !variant.chars().any(char::is_whitespace)
                    })
                else {
                    continue;
                };
                if !ids.iter().any(|known: &String| known == variant) {
                    ids.push(variant.to_owned());
                }
            }
            ids
        });
        if display_name.is_none() && variants.is_none() {
            continue;
        }
        entries.push(NativeModelMetadata {
            id: format!("{provider}/{id}"),
            display_name,
            variants,
        });
    }
    Ok(entries)
}

/// Run the frozen native exe with exact argv `models --standalone` and return its
/// `provider/model` ids verbatim, in wire order. Bounded by `deadline` and
/// [`MAX_MODELS_BYTES`]; stderr is discarded like every other helper, so
/// failures are static typed strings naming the `models` surface.
pub(crate) async fn run_models(
    program: &OsStr,
    deadline: Instant,
    context: &ReaderContext<'_>,
) -> Result<Vec<String>> {
    let mut command = context.command(program);
    command.args(models_argv());
    let text = run_bounded_utf8(
        command,
        MAX_MODELS_BYTES,
        deadline,
        &BoundedReadLabels {
            start: "OpenCode models could not start",
            deadline: "OpenCode models did not finish before its deadline",
            unreadable: "OpenCode models metadata was unreadable",
            status: "OpenCode models did not return a catalogue",
        },
    )
    .await?;
    parse_models_output(&text)
}

/// Parse `opencode models --standalone` stdout verbatim: one `provider/model` id per
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
        if !valid_model_id(line) {
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
