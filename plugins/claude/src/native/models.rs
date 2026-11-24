//! No-prompt metadata read over the Claude Code initialize control request.

use std::{ffi::OsString, path::Path, time::Duration};

use eliot_core::{Error, Result, SecretPolicy, api::native_profile::CLAUDE_EFFORT_LEVELS};
use eliot_plugin::{
    native_transport::NativeLaunch,
    protocol::{ModelListParams, ModelListPart},
};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::{ClaudeProcess, types::InitializeReadback};

pub(crate) async fn read(raw: &Value, workspace: &Path) -> Result<Value> {
    let params: ModelListParams = serde_json::from_value(raw.clone())
        .map_err(|_| Error::new("invalid", "Claude model.list params are invalid"))?;
    if params.part != ModelListPart::Models || params.profile["harness"].as_str() != Some("claude")
    {
        return Err(Error::new(
            "unsupported",
            "Claude model.list requires a Claude models request",
        ));
    }
    let cmd = params.profile["cmd"]
        .as_str()
        .filter(|cmd| Path::new(cmd).is_absolute())
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                "Claude model.list requires a frozen native cmd",
            )
        })?;
    // The daemon gates Claude reads on the canonical route and forwards the
    // exact private environment it verified.
    let mapped = eliot_plugin::session::target_environment(
        params.target_environment.as_ref(),
        params.profile.get("pass_env"),
        "Claude",
    )?;
    let mut variables: Vec<(OsString, OsString)> = mapped
        .variables
        .into_iter()
        .filter(|(name, _)| {
            !name
                .to_string_lossy()
                .eq_ignore_ascii_case("ENABLE_CLAUDEAI_MCP_SERVERS")
        })
        .collect();
    variables.push((
        OsString::from("ENABLE_CLAUDEAI_MCP_SERVERS"),
        OsString::from("false"),
    ));
    let policy = SecretPolicy::new(mapped.secrets.iter().cloned());

    let args = [
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--no-session-persistence",
        "--safe-mode",
        "--tools",
        "",
        "--strict-mcp-config",
        "--mcp-config",
        r#"{"mcpServers":{}}"#,
    ];
    let mut native = ClaudeProcess::spawn(
        NativeLaunch {
            program: OsString::from(cmd),
            args: args.into_iter().map(OsString::from).collect(),
            cwd: workspace.to_owned(),
            environment: variables,
        },
        policy,
    )
    .await
    .map_err(inventory_unavailable_if_not_dispatched)?;

    let reply = native
        .client()
        .control(
            "initialize",
            json!({}),
            Instant::now() + Duration::from_secs(12),
        )
        .await;
    native
        .terminate(true, Instant::now() + Duration::from_secs(3))
        .await?;
    project_inventory(reply.map_err(inventory_unavailable_if_not_dispatched)?)
}

fn inventory_unavailable_if_not_dispatched(error: Error) -> Error {
    match error.code.as_str() {
        "native_spawn_refused" | "native_not_dispatched" => Error::new(
            "unavailable",
            "Claude model.list could not dispatch the native inventory read",
        ),
        _ => error,
    }
}

fn project_inventory(reply: Value) -> Result<Value> {
    let readback: InitializeReadback = serde_json::from_value(reply).map_err(|_| {
        Error::new(
            "source_integrity",
            "Claude model.list received an invalid inventory",
        )
    })?;
    if readback.models.len() > 1024 {
        return Err(Error::new(
            "source_integrity",
            "Claude model.list inventory exceeds 1024 entries",
        ));
    }
    if readback.models.is_empty() {
        return Err(Error::new(
            "unavailable",
            "Claude model.list received no native inventory",
        ));
    }
    let models: Vec<_> = readback
        .models
        .into_iter()
        .filter_map(|model| {
            let id = model.value.filter(|id| !id.trim().is_empty())?;
            Some(json!({
                "id": id,
                "resolved_model": model.resolved_model,
                "display_name": model.display_name,
                "efforts": model.supported_effort_levels,
            }))
        })
        .collect();
    if models.is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Claude model.list inventory contains no usable model ids",
        ));
    }
    // Initialize reports choices and capabilities, not whether inference is
    // currently entitled, and list order is not default-model evidence.
    Ok(json!({"models": models, "default": null}))
}

/// Provenance marker for the frozen Claude catalogue entries below: the ids
/// are documented aliases and full model names, not a live native read.
const CLAUDE_CATALOGUE_SOURCE: &str = "documented_alias";

/// Frozen Claude Code `--model` aliases from the vendor model configuration
/// ([Model configuration](https://code.claude.com/docs/en/model-config),
/// checked 2026-11-16; installed CLI 2.1.325 `--help` names `fable`,
/// `opus` and `sonnet` as examples): `best` resolves like `opus`; `fable`
/// selects the Fable model; `sonnet[1m]`/`opus[1m]` request the 1M context
/// window; `opusplan` runs plan mode on Opus then executes on Sonnet, with
/// the documented `opusplan[1m]` variant for 1M context in both phases.
/// `default` is deliberately absent: the doc marks it a special value that
/// clears any override rather than a model alias, and omitting `--model`
/// already selects the CLI default (read back from initialize/stream).
const CLAUDE_DOCUMENTED_ALIASES: &[&str] = &[
    "best",
    "fable",
    "sonnet",
    "opus",
    "haiku",
    "sonnet[1m]",
    "opus[1m]",
    "opusplan",
    "opusplan[1m]",
];

/// Frozen full Claude model ids, each shown in the same vendor doc as a
/// `/model`, `--model`, settings, fallback or allowlist selection
/// (checked 2026-11-16): pinning a version instead of following an alias.
/// `claude-haiku-5-5` is documented as `Its model ID is ... To use it, run
/// /model claude-haiku-5-5 ... or claude --model claude-haiku-5-5`.
const CLAUDE_DOCUMENTED_MODEL_IDS: &[&str] = &[
    "claude-fable-5-2",
    "claude-fable-5-1",
    "claude-fable-5",
    "claude-opus-5-5",
    "claude-opus-5",
    "claude-opus-4-8",
    "claude-opus-4-8[1m]",
    "claude-opus-4-6",
    "claude-opus-4-6[1m]",
    "claude-opus-4-1",
    "claude-sonnet-5",
    "claude-sonnet-4-6",
    "claude-sonnet-4-6[1m]",
    "claude-sonnet-4-5",
    "claude-sonnet-4-5-20250929",
    "claude-haiku-5-5",
    "claude-haiku-4-5",
];

/// Documented model selections with unknown route availability.
pub(crate) fn documented_models() -> Vec<eliot_core::api::HarnessModelEntry> {
    let efforts: Vec<String> = CLAUDE_EFFORT_LEVELS
        .iter()
        .map(|effort| (*effort).to_owned())
        .collect();
    CLAUDE_DOCUMENTED_ALIASES
        .iter()
        .chain(CLAUDE_DOCUMENTED_MODEL_IDS.iter())
        .map(|id| eliot_core::api::HarnessModelEntry {
            id: (*id).to_owned(),
            display_name: None,
            default: false,
            efforts: Some(efforts.clone()),
            default_effort: None,
            source: Some(CLAUDE_CATALOGUE_SOURCE.to_owned()),
        })
        .collect()
}
