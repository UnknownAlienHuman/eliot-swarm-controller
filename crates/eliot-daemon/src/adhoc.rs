//! Ad-hoc Owner launch: freeze a profile-equivalent snapshot from built-in
//! per-harness defaults plus flags, with deterministic executable discovery.
//! Native subagents follow the same harness defaults as stored profiles.
//!
//! Owning contract: docs/spec/configuration.md (ad-hoc launch) and the five
//! harness pages. Profiles stay the durable explicit record; ad-hoc is the
//! Owner tool that freezes an equivalent snapshot. Broader authority stays
//! profile-only: this module only emits each harness's ordinary safe posture
//! and never widens it.
//!
//! Requested vs effective: an omitted `--model`/`--effort` is frozen as an
//! absent key (requested-absent). The plugin resolves the harness native
//! default at start and reports the effective value through its existing
//! readback; no substitution is ever made for an unavailable requested model.

use core::api::harness::{AdhocAccess, HarnessCapabilities};
use eliot_core as core;
use eliot_plugin::install::{
    InstallDiscovery, InstallStrategy, cli_identity_matches, discovery_for, find_on_path,
    is_native_executable, is_script_wrapper, local_roots, native_exe_name, parse_cli_version,
    path_search_dirs, probe_candidate_paths_with,
};
use eliot_plugin::session::split_provider_model;
use eliot_plugin::sync::LockExt;
use serde_json::{Map, Value, json};
use std::path::Path;

pub(crate) const ADHOC_PROFILE_PREFIX: &str = "adhoc-";

pub(crate) const DISCOVERY_BUDGET: std::time::Duration = std::time::Duration::from_secs(90);
use std::time::{Duration, Instant};

pub(crate) fn discovery_timeout() -> core::Error {
    core::Error::new("timeout", "native discovery deadline elapsed")
}

fn discovery_remaining(deadline: Instant) -> core::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(discovery_timeout)
}

pub(crate) fn adhoc_profile_name(harness: &str) -> String {
    format!("{ADHOC_PROFILE_PREFIX}{harness}")
}

pub(crate) fn validate_adhoc_harness(harness: &str) -> core::Result<String> {
    let trimmed = harness.trim();
    if core::api::is_supported_harness(trimmed) {
        Ok(trimmed.to_owned())
    } else {
        Err(core::Error::new(
            "unsupported",
            format!("unsupported launch harness {harness}"),
        ))
    }
}

fn validate_adhoc_model(model: &str) -> core::Result<String> {
    let trimmed = model.trim();
    if trimmed.is_empty() || trimmed.len() > 256 || trimmed.chars().any(char::is_control) {
        return Err(core::Error::new(
            "profile_error",
            "ad-hoc model must be a non-empty string of at most 256 characters with no controls",
        ));
    }
    Ok(trimmed.to_owned())
}

/// Closed Claude Code `--effort` vocabulary, mirroring the profile freeze
/// (`claude_snapshot_from_entry` in config.rs): `claude --help` documents
/// `--effort <level>` and the frozen profile accepts exactly these levels.
pub use core::api::native_profile::CLAUDE_EFFORT_LEVELS;

/// Provenance marker for the frozen Claude catalogue entries below: the ids
/// are documented aliases and full model names, not a live native read.
pub const CLAUDE_CATALOGUE_SOURCE: &str = "documented_alias";

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
pub const CLAUDE_DOCUMENTED_ALIASES: &[&str] = &[
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
pub const CLAUDE_DOCUMENTED_MODEL_IDS: &[&str] = &[
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

/// Build the frozen `harness.models` entries for Claude. No native
/// catalogue exists (verified read-only against installed 2.1.325 on
/// 2026-11-16: no `models` subcommand, `--model` names aliases or a full
/// model name, settings `model`/`availableModels` is an allowlist rather
/// than an inventory, and the stream-json `initialize` reply reports only
/// the session model), so the documented aliases and full ids above are
/// returned verbatim, each marked `documented_alias` with the CLI effort
/// vocabulary attached. Availability is never asserted here: quota stays
/// unsupported for Claude, so the unified catalogue reads `unknown`.
pub(crate) fn claude_documented_models() -> Vec<core::api::HarnessModelEntry> {
    let efforts: Vec<String> = CLAUDE_EFFORT_LEVELS
        .iter()
        .map(|effort| (*effort).to_owned())
        .collect();
    CLAUDE_DOCUMENTED_ALIASES
        .iter()
        .chain(CLAUDE_DOCUMENTED_MODEL_IDS.iter())
        .map(|id| core::api::HarnessModelEntry {
            id: (*id).to_owned(),
            display_name: None,
            default: false,
            efforts: Some(efforts.clone()),
            default_effort: None,
            source: Some(CLAUDE_CATALOGUE_SOURCE.to_owned()),
        })
        .collect()
}

fn validate_adhoc_effort(harness: &str, effort: &str) -> core::Result<String> {
    // OpenCode efforts are the installed per-model `Model.Variant` ids
    // (the TUI "Variant" switch), so any well-formed value freezes and the
    // live native catalogue proves membership at launch.
    if !matches!(harness, "codex" | "muse" | "claude" | "opencode") {
        return Err(core::Error::new(
            "unsupported",
            format!("the {harness} slice has no reasoning-effort mapping"),
        ));
    }
    let trimmed = effort.trim();
    if trimmed.is_empty() || trimmed.len() > 128 || trimmed.chars().any(char::is_control) {
        return Err(core::Error::new(
            "profile_error",
            "ad-hoc effort must be non-empty, at most 128 characters and contain no controls",
        ));
    }
    if harness == "claude" && !CLAUDE_EFFORT_LEVELS.contains(&trimmed) {
        return Err(core::Error::new(
            "profile_error",
            "unknown Claude effort level (low, medium, high, xhigh, or max)",
        ));
    }
    Ok(trimmed.to_owned())
}

/// Longest accepted ad-hoc auth selector, in bytes. Mirrors the plugin's
/// `IntegrationSelector` rule so the frozen value always passes the
/// profile-side check.
const MAX_AUTH_SELECTOR_BYTES: usize = 128;

/// Ad-hoc auth shape: only the OpenCode slice transfers a canonical auth
/// integration, so only it accepts `--auth`. The syntax mirrors the
/// profile's `auth_integration` selector.
pub(crate) fn validate_adhoc_auth(harness: &str, auth: &str) -> core::Result<String> {
    if harness != "opencode" {
        return Err(core::Error::new(
            "unsupported",
            format!("the {harness} slice has no auth-integration mapping"),
        ));
    }
    let trimmed = auth.trim();
    if trimmed.is_empty()
        || trimmed.len() > MAX_AUTH_SELECTOR_BYTES
        || trimmed.starts_with('-')
        || trimmed.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(core::Error::new(
            "profile_error",
            "ad-hoc auth must be a non-empty selector of at most 128 characters with no leading dash or controls",
        ));
    }
    Ok(trimmed.to_owned())
}

/// Whether an ad-hoc snapshot resolves an OpenCode auth selector.
/// Catalogue reads transfer no credential, so they resolve nothing; launches
/// always freeze `auth_integration` (explicit, defaulted, or refused).
#[derive(Clone, Copy)]
pub(crate) enum AdhocAuth<'a> {
    /// Catalogue read: skip auth entirely.
    Skip,
    /// Launch: freeze `auth_integration` from this explicit selector.
    Resolve(Option<&'a str>),
}

/// Build and freeze an ad-hoc snapshot from per-harness safe defaults plus
/// the requested model/effort/auth. `cmd` is the already-discovered absolute
/// native path for harnesses that require one, or `None` for Codex (which
/// launches `codex` by name). An omitted model/effort stays absent so the
/// plugin applies the harness native default and reports it separately.
/// `auth` is OpenCode-only: an explicit selector freezes verbatim, an
/// omitted one defaults to the model provider prefix when the native auth
/// list holds exactly that id, and anything else refuses before spawning.
/// Catalogue snapshots (`AdhocAuth::Skip`) never touch auth.
pub(crate) fn adhoc_snapshot(
    harness: &str,
    model: Option<&str>,
    effort: Option<&str>,
    cmd: Option<String>,
    auth: AdhocAuth<'_>,
    access: Option<&str>,
) -> core::Result<Value> {
    let harness = validate_adhoc_harness(harness)?;
    let model = model
        .filter(|value| !value.trim().is_empty())
        .map(validate_adhoc_model)
        .transpose()?;
    let effort = effort
        .filter(|value| !value.trim().is_empty())
        .map(|value| validate_adhoc_effort(&harness, value))
        .transpose()?;
    // Catalogue snapshots (`AdhocAuth::Skip`) freeze no model: the `agy
    // models` reader needs only the frozen cmd. Launches still require an
    // explicit model.
    let catalogue_only = matches!(auth, AdhocAuth::Skip);
    let auth = match auth {
        AdhocAuth::Skip => None,
        AdhocAuth::Resolve(explicit) => Some(
            explicit
                .filter(|value| !value.trim().is_empty())
                .map(|value| validate_adhoc_auth(&harness, value))
                .transpose()?,
        ),
    };
    if harness == "antigravity" && model.is_none() && !catalogue_only {
        return Err(core::Error::new(
            "profile_error",
            "ad-hoc antigravity launch requires an explicit model",
        ));
    }
    if let Some(model) = model.as_deref()
        && harness == "opencode"
        && split_provider_model(model).is_none()
    {
        return Err(core::Error::new(
            "profile_error",
            "ad-hoc opencode model must be provider/model with non-empty slash segments and no variant suffix",
        ));
    }

    let access = access
        .filter(|value| !value.trim().is_empty())
        .map(AdhocAccess::parse)
        .transpose()?;
    if catalogue_only && access.is_some() {
        return Err(core::Error::new(
            "invalid",
            "catalogue reads take no access level",
        ));
    }
    let profile_name = adhoc_profile_name(&harness);
    let mut snapshot = Map::new();
    snapshot.insert("harness".to_owned(), Value::String(harness.clone()));
    match harness.as_str() {
        "codex" => {
            if let Some(cmd) = cmd {
                let validated = validate_native_cmd(&profile_name, &cmd)?;
                snapshot.insert("cmd".to_owned(), Value::String(validated));
            }
        }
        "claude" => {
            let cmd = cmd.ok_or_else(|| {
                core::Error::new(
                    "profile_error",
                    "ad-hoc claude launch requires a discovered native cmd",
                )
            })?;
            let validated = validate_claude_cmd(&profile_name, &cmd)?;
            snapshot.insert("cmd".to_owned(), Value::String(validated));
        }
        "muse" => {
            let cmd = cmd.ok_or_else(|| {
                core::Error::new(
                    "profile_error",
                    "ad-hoc muse launch requires a discovered native cmd",
                )
            })?;
            let validated = validate_native_cmd(&profile_name, &cmd)?;
            snapshot.insert("cmd".to_owned(), Value::String(validated));
        }
        "opencode" => {
            let cmd = cmd.ok_or_else(|| {
                core::Error::new(
                    "profile_error",
                    "ad-hoc opencode launch requires a discovered native cmd",
                )
            })?;
            let validated = validate_native_cmd(&profile_name, &cmd)?;
            // Launch only: an explicit selector freezes verbatim; an omitted
            // one defaults to the model provider prefix only on an exact
            // auth-list id match, and anything else refuses before spawning.
            // Catalogue snapshots (`AdhocAuth::Skip`) freeze no selector: the
            // read transfers no credential. The plugin's pre-serve transfer
            // gate is unchanged: it still requires the frozen integration to
            // hold exactly one canonical credential.
            let selector = match auth {
                None => None,
                Some(explicit) => Some(match explicit {
                    Some(selector) => selector,
                    None => {
                        let ids = opencode_auth_ids(&validated).ok_or_else(|| {
                            core::Error::new(
                                "profile_error",
                                "ad-hoc opencode launch needs --auth <integration>; the native auth list could not be read",
                            )
                        })?;
                        resolve_opencode_default(model.as_deref(), &ids)?
                    }
                }),
            };
            snapshot.insert("cmd".to_owned(), Value::String(validated));
            if let Some(selector) = selector {
                snapshot.insert("auth_integration".to_owned(), Value::String(selector));
            }
        }
        "antigravity" => {
            let cmd = cmd.ok_or_else(|| {
                core::Error::new(
                    "profile_error",
                    "ad-hoc antigravity launch requires a discovered native cmd",
                )
            })?;
            let validated = validate_native_cmd(&profile_name, &cmd)?;
            // CLI identity before any launch: an IDE-named cmd is refused
            // here, never spawned as an agent server.
            if !cli_identity_matches_decl(&harness, &validated) {
                return Err(core::Error::new(
                    "profile_error",
                    format!(
                        "ad-hoc {profile_name}: cmd points at the Antigravity IDE ({validated}), which does not implement the required native ACP/model CLI and is never launched; use the `agy` CLI"
                    ),
                ));
            }
            snapshot.insert("cmd".to_owned(), Value::String(validated));
        }
        _ => {
            return Err(core::Error::new(
                "unsupported",
                format!("unsupported launch harness {harness}"),
            ));
        }
    }
    if let Some(model) = model {
        snapshot.insert("model".to_owned(), Value::String(model));
    }
    if let Some(effort) = effort {
        snapshot.insert("effort".to_owned(), Value::String(effort));
    }
    let defaults = HarnessCapabilities::of(&harness)
        .adhoc_launch
        .ok_or_else(|| {
            core::Error::new(
                "unsupported",
                format!("unsupported launch harness {harness}"),
            )
        })?;
    if let Some(access) = access {
        snapshot.insert("access".to_owned(), Value::from(access.as_str()));
    }
    (defaults.permissions)(&mut snapshot, access).map_err(|error| {
        core::Error::new(
            error.code.clone(),
            format!("ad-hoc {profile_name}: {}", error.message),
        )
    })?;
    snapshot.insert("mcp".to_owned(), Value::from(defaults.mcp.as_str()));
    snapshot.insert("mcp_allow".to_owned(), Value::Array(Vec::new()));
    snapshot.insert("child_profiles".to_owned(), Value::Array(Vec::new()));
    let native_subagents = crate::config::native_subagents_default(&harness).ok_or_else(|| {
        core::Error::new(
            "unsupported",
            format!("ad-hoc native_subagents has no default mapping for {harness}"),
        )
    })?;
    snapshot.insert(
        "native_subagents".to_owned(),
        Value::String(native_subagents.to_owned()),
    );
    snapshot.insert("pass_env".to_owned(), json!(defaults.pass_env));
    snapshot.insert("scratch".to_owned(), json!(["target/", "node_modules/"]));
    snapshot.insert(
        "capacity_scopes".to_owned(),
        json!([{"scope": format!("profile:{profile_name}")}]),
    );
    Ok(Value::Object(snapshot))
}

/// Ad-hoc cmd shape: one explicit executable path, never a shell command.
pub(crate) fn validate_native_cmd(profile: &str, cmd: &str) -> core::Result<String> {
    let cmd = crate::config::validate_executable_name(cmd).map_err(|class| {
        core::Error::new("profile_error", format!("ad-hoc {profile}: cmd {class}"))
    })?;
    if is_script_wrapper(cmd) {
        return Err(core::Error::new(
            "profile_error",
            format!(
                "ad-hoc {profile}: cmd must name one native executable path, not a .cmd/.bat/.ps1/.js wrapper"
            ),
        ));
    }
    Ok(cmd.to_owned())
}

/// Claude cmd rule: absolute native path, never a bare name or wrapper.
/// A wrapper would orphan the real CLI on stop, when only the wrapper PID
/// is owned.
fn validate_claude_cmd(profile: &str, cmd: &str) -> core::Result<String> {
    let cmd = validate_native_cmd(profile, cmd)?;
    if !Path::new(&cmd).is_absolute() {
        return Err(core::Error::new(
            "profile_error",
            format!(
                "ad-hoc {profile}: cmd must be an absolute native executable path, not a bare command name"
            ),
        ));
    }
    if cfg!(windows) && !cmd.to_ascii_lowercase().ends_with(".exe") {
        return Err(core::Error::new(
            "profile_error",
            format!("ad-hoc {profile}: cmd must name the native .exe, not a script wrapper"),
        ));
    }
    Ok(cmd)
}

/// Async `discover_cmd_for_harness` for Tokio handlers: candidate
/// enumeration plus the bounded `--version` probes runs on the blocking
/// pool, so slow native CLIs never stall async command/event service while
/// cached catalogue data already exists.
pub(crate) async fn discover_cmd_until(
    harness: &str,
    deadline: Instant,
) -> core::Result<Option<String>> {
    let harness = validate_adhoc_harness(harness)?;
    blocking_discovery(deadline, move || {
        discover_cmd_for_harness(&harness, deadline)
    })
    .await?
}

pub(crate) async fn blocking_discovery<T: Send + 'static>(
    deadline: Instant,
    work: impl FnOnce() -> T + Send + 'static,
) -> core::Result<T> {
    discovery_remaining(deadline)?;
    let mut task = tokio::task::spawn_blocking(move || {
        discovery_remaining(deadline)?;
        Ok(work())
    });
    match tokio::time::timeout_at(deadline.into(), &mut task).await {
        Ok(result) => {
            result.map_err(|_| core::Error::new("internal", "native discovery was interrupted"))?
        }
        Err(_) => {
            task.abort();
            Err(discovery_timeout())
        }
    }
}

/// Whether `cmd` satisfies the harness's declared CLI identity gate.
/// Harnesses without an identity gate accept any resolved executable.
/// Adapter-owned declarations are registered in `eliot_plugin::install` by
/// the `eliot` composition root; the daemon holds no per-harness branch.
fn cli_identity_matches_decl(harness: &str, cmd: &str) -> bool {
    discovery_for(harness).is_ok_and(|decl| {
        decl.cli_identity
            .is_none_or(|stem| cli_identity_matches(cmd, stem))
    })
}

/// Resolve the installed executable from the owning plugin's declaration.
/// Codex's default launch keeps its native PATH resolution; readers pin the
/// resolved executable before probing.
fn discover_cmd_for_harness(harness: &str, deadline: Instant) -> core::Result<Option<String>> {
    discovery_remaining(deadline)?;
    let decl = discovery_for(harness)?;
    match decl.strategy {
        InstallStrategy::ByName => Ok(None),
        InstallStrategy::FirstExisting | InstallStrategy::MetadataVersion(_) => {
            discover_first_existing(&decl)
        }
        InstallStrategy::NewestVerified => {
            discover_newest_verified(&decl, deadline).map(|candidate| Some(candidate.cmd))
        }
    }
}

/// Take the first enumerated candidate that exists. Candidates never run
/// here; existence and naming checks alone decide.
fn discover_first_existing(decl: &InstallDiscovery) -> core::Result<Option<String>> {
    let paths = (decl.candidates)()?;
    for path in &paths {
        let text = path.to_string_lossy().into_owned();
        if path.is_absolute() && path.is_file() && !is_script_wrapper(&text) {
            return Ok(Some(text));
        }
    }
    let searched = (decl.searched)();
    if let (Some(subpath), Some(missing)) = (decl.ide_hint_subpath, decl.ide_missing) {
        for root in local_roots() {
            let mut ide = root;
            for segment in subpath {
                ide = ide.join(segment);
            }
            if ide.is_file() {
                return Err(missing(&ide.to_string_lossy(), &searched));
            }
        }
    }
    Err((decl.not_found)(!paths.is_empty(), &searched))
}

/// Probe the enumerated candidates with `--version` and take the newest
/// parseable result. Filesystem hits are cheap, process launches are
/// bounded by the shared candidate cap.
fn discover_newest_verified(
    decl: &InstallDiscovery,
    deadline: Instant,
) -> core::Result<core::api::HarnessCandidate> {
    let paths = (decl.candidates)()?;
    let mut timed_out_version = None;
    let candidates = probe_candidate_paths_with(paths, |cmd| {
        match probe_native_version(cmd, None, deadline) {
            Ok(version) => version,
            Err(error) => {
                if error.code == "timeout"
                    && let Some(label_of) = decl.timeout_label
                    && let Some(labelled) =
                        label_of(cmd).and_then(|label| parse_cli_version(&label))
                {
                    timed_out_version = timed_out_version.max(Some(labelled));
                }
                None
            }
        }
    });
    discovery_remaining(deadline)?;
    let searched = (decl.searched)();
    let best = verified_candidate_from_candidates(&candidates)
        .ok_or_else(|| (decl.not_found)(!candidates.is_empty(), &searched))?;
    let version = best.version.as_deref().and_then(parse_cli_version);
    if let (Some(timeout_newer), Some(version)) = (decl.timeout_newer, version)
        && timed_out_version.is_some_and(|timed_out| timed_out > version)
    {
        return Err(timeout_newer());
    }
    Ok(best.clone())
}

/// Check the presence of an explicit command without probing or executing it.
pub(crate) fn profile_command_exists(cmd: &str) -> bool {
    let path = Path::new(cmd);
    if path.is_absolute() || path.components().count() != 1 {
        return path.is_file();
    }
    let windows_exe = (cfg!(windows) && path.extension().is_none()).then(|| native_exe_name(cmd));
    path_search_dirs().iter().any(|dir| {
        dir.join(path).is_file()
            || windows_exe
                .as_ref()
                .is_some_and(|exe| dir.join(exe).is_file())
    })
}

/// Discover native Claude CLI installations and probe their versions.
/// Enumeration comes from the Claude plugin's install declaration; the
/// bounded `--version` probe loop stays shared here.
pub(crate) fn discover_claude_candidates(deadline: Instant) -> Vec<core::api::HarnessCandidate> {
    let paths = discovery_for("claude")
        .ok()
        .and_then(|decl| (decl.candidates)().ok())
        .unwrap_or_default();
    probe_candidate_paths_with(paths, |cmd| {
        probe_native_version(cmd, None, deadline).ok().flatten()
    })
}

/// Select the newest verified Claude candidate through the shared
/// newest-verified engine over the plugin's declaration.
fn discover_verified_claude(deadline: Instant) -> core::Result<core::api::HarnessCandidate> {
    let decl = discovery_for("claude")?;
    discover_newest_verified(&decl, deadline)
}

fn verified_candidate_from_candidates(
    candidates: &[core::api::HarnessCandidate],
) -> Option<&core::api::HarnessCandidate> {
    // Discovery sorts parseable versions newest-first and leaves failed,
    // timed-out, or unparseable probes at the end. Skip those entries rather
    // than letting one obscure the newest version that was verified.
    candidates.iter().find(|candidate| {
        candidate
            .version
            .as_deref()
            .and_then(parse_cli_version)
            .is_some()
    })
}

pub(crate) fn is_latest_claude_selector(cmd: &str) -> bool {
    cmd.trim().eq_ignore_ascii_case("latest")
}

pub(crate) fn claude_candidate_available() -> bool {
    discovery_for("claude").ok().is_some_and(|decl| {
        (decl.candidates)()
            .unwrap_or_default()
            .iter()
            .any(|path| is_native_executable(path))
    })
}

pub(crate) fn write_profile_command_snapshot(
    snapshot: &mut Value,
    candidate: core::api::HarnessCandidate,
) -> core::Result<()> {
    let profile = snapshot.as_object_mut().ok_or_else(|| {
        core::Error::new("profile_error", "launch profile snapshot must be an object")
    })?;
    profile.insert("cmd".to_owned(), Value::String(candidate.cmd));
    if let Some(version) = candidate.version {
        profile.insert("cmd_version".to_owned(), Value::String(version));
    } else {
        profile.remove("cmd_version");
    }
    Ok(())
}

/// Build one shared refusal for an explicit pin that no longer resolves.
/// `replacement` is one bounded candidate from the same native discovery path
/// used for launch; the repair always omits `cmd` so future launches follow
/// the installed executable.
pub(crate) fn missing_profile_command_refusal(
    profile: &str,
    harness: &str,
    configured_cmd: &str,
    replacement: Option<&core::api::HarnessCandidate>,
) -> core::Error {
    let fallback = if harness == "claude" {
        format!(
            "omit cmd from profiles.{profile} to follow the installed version, or use {}",
            claude_latest_settings_command(profile)
        )
    } else {
        format!("omit cmd from profiles.{profile} to follow the installed version")
    };
    let replacement_note = match replacement {
        Some(candidate) => {
            let version = candidate
                .version
                .as_deref()
                .map(|version| format!(" version {version}"))
                .unwrap_or_default();
            format!(
                "an installed native {harness}{version} was found at {}",
                core::display_path(&candidate.cmd)
            )
        }
        None => format!("no installed native {harness} replacement was discovered"),
    };
    core::AdmissionRefusal::ProfileCommand.error(format!(
        "{profile}: configured {harness} executable was not found at {}; {replacement_note}; {fallback}",
        core::display_path(configured_cmd)
    ))
}

/// Resolve a named profile's configured executable pin, or discover the
/// installed native executable when the profile omits `cmd`. The returned
/// candidate is frozen into the launch snapshot by the caller.
pub(crate) async fn resolve_profile_command(
    profile: &str,
    harness: &str,
    cmd: Option<&str>,
) -> core::Result<core::api::HarnessCandidate> {
    resolve_profile_command_until(profile, harness, cmd, Instant::now() + DISCOVERY_BUDGET).await
}

/// A confined launch keeps the executable authority of the source generation.
/// Check explicit pins by path and carry the frozen command facts without
/// running an executable during authorization.
pub(crate) fn inherit_profile_command(
    snapshot: &mut Value,
    source: &Value,
    harness: &str,
) -> core::Result<()> {
    let source_cmd = source["cmd"].as_str();
    if let Some(cmd) = explicit_profile_command(snapshot, harness)
        && Some(cmd) != source_cmd
        && (source_cmd.is_none()
            || resolve_native_executable(cmd, harness).ok().as_deref() != source_cmd)
    {
        return Err(core::Error::new(
            "not_allowed",
            "profile executable differs from the frozen launch authority",
        ));
    }
    let profile = snapshot.as_object_mut().ok_or_else(|| {
        core::Error::new("profile_error", "launch profile snapshot must be an object")
    })?;
    for key in ["cmd", "cmd_version"] {
        if let Some(value) = source.get(key) {
            profile.insert(key.to_owned(), value.clone());
        } else {
            profile.remove(key);
        }
    }
    Ok(())
}

pub(crate) fn explicit_profile_command<'a>(snapshot: &'a Value, harness: &str) -> Option<&'a str> {
    snapshot["cmd"]
        .as_str()
        .filter(|cmd| !(harness == "claude" && is_latest_claude_selector(cmd)))
}

pub(crate) async fn resolve_profile_command_until(
    profile: &str,
    harness: &str,
    cmd: Option<&str>,
    deadline: Instant,
) -> core::Result<core::api::HarnessCandidate> {
    if harness == "claude" {
        return resolve_claude_profile_command_until(profile, cmd, deadline).await;
    }
    let Some(cmd) = cmd else {
        return discover_profile_command_candidate(harness, deadline).await;
    };
    match reader_executable(cmd, harness, deadline).await {
        Ok((cmd, version)) => Ok(core::api::HarnessCandidate { cmd, version }),
        Err(_) if !profile_command_exists(cmd) => {
            let replacement = discover_profile_command_candidate(harness, deadline)
                .await
                .ok();
            Err(missing_profile_command_refusal(
                profile,
                harness,
                cmd,
                replacement.as_ref(),
            ))
        }
        Err(error) => Err(error),
    }
}

async fn discover_profile_command_candidate(
    harness: &str,
    deadline: Instant,
) -> core::Result<core::api::HarnessCandidate> {
    let selected = discover_cmd_until(harness, deadline)
        .await?
        .unwrap_or_else(|| harness.to_owned());
    let (cmd, version) = reader_executable(&selected, harness, deadline).await?;
    Ok(core::api::HarnessCandidate { cmd, version })
}

async fn resolve_claude_profile_command_until(
    profile: &str,
    cmd: Option<&str>,
    deadline: Instant,
) -> core::Result<core::api::HarnessCandidate> {
    let profile = profile.to_owned();
    let cmd = cmd.map(str::to_owned);
    blocking_discovery(deadline, move || {
        resolve_claude_profile_command_sync(&profile, cmd.as_deref(), deadline)
    })
    .await?
}

fn resolve_claude_profile_command_sync(
    profile: &str,
    cmd: Option<&str>,
    deadline: Instant,
) -> core::Result<core::api::HarnessCandidate> {
    if let Some(cmd) = cmd.filter(|cmd| !is_latest_claude_selector(cmd)) {
        let cmd = validate_claude_cmd(profile, cmd)?;
        if is_native_executable(Path::new(&cmd)) {
            return Ok(core::api::HarnessCandidate {
                version: probe_native_version(&cmd, None, deadline).ok().flatten(),
                cmd,
            });
        }
        let candidates = discover_claude_candidates(deadline);
        return Err(missing_claude_profile_command(profile, &cmd, &candidates));
    }

    discover_verified_claude(deadline)
        .map_err(|error| core::Error::new(error.code, format!("{profile}: {}", error.message)))
}

fn missing_claude_profile_command(
    profile: &str,
    cmd: &str,
    candidates: &[core::api::HarnessCandidate],
) -> core::Error {
    let replacement = verified_candidate_from_candidates(candidates);
    missing_profile_command_refusal(profile, "claude", cmd, replacement)
}

pub(crate) fn claude_latest_settings_command(profile: &str) -> String {
    format!(
        "swarm settings claude-cmd {} --latest",
        shell_quote_settings_arg(profile)
    )
}

fn shell_quote_settings_arg(value: &str) -> String {
    let apostrophe = char::from(39);
    if cfg!(windows) {
        format!("'{}'", value.replace(apostrophe, "''"))
    } else {
        format!("'{}'", value.replace(apostrophe, "'\\''"))
    }
}

/// Inventory entry for Claude from one candidate pass, so `harness.list`
/// does not enumerate and version every install twice.
pub(crate) fn claude_installation(candidates: &[core::api::HarnessCandidate]) -> Value {
    if let Some(best) = verified_candidate_from_candidates(candidates) {
        let version = best.version.clone().map_or(Value::Null, Value::String);
        json!({
            "harness": "claude",
            "installed": true,
            "cmd": best.cmd.clone(),
            "version": version,
        })
    } else {
        let error = discovery_for("claude")
            .ok()
            .map(|decl| (decl.not_found)(!candidates.is_empty(), &(decl.searched)()))
            .unwrap_or_else(|| {
                core::Error::new(
                    "profile_error",
                    "Standalone Claude CLI was not found; select a native executable explicitly with --cmd",
                )
            });
        json!({
            "harness": "claude",
            "installed": false,
            "cmd": Value::Null,
            "version": Value::Null,
            "error": error.message,
            "code": error.code,
        })
    }
}
/// Resolve and check the exact executable before any probe. An IDE symlink
/// cannot pass the CLI name check on its resolved target.
fn resolve_native_executable(cmd: &str, harness: &str) -> core::Result<String> {
    let decl = discovery_for(harness)?;
    let identity = decl.cli_identity;
    if let Some(stem) = identity
        && !cli_identity_matches(cmd, stem)
    {
        return Err(decl
            .ide_unsupported
            .map_or_else(native_cmd_unavailable, |refused| refused(cmd)));
    }
    let input = Path::new(cmd);
    let path = if input.is_absolute() {
        input.to_path_buf()
    } else if input.components().count() == 1 {
        let name = if cfg!(windows) && input.extension().is_none() {
            native_exe_name(cmd)
        } else {
            cmd.to_owned()
        };
        find_on_path(&name)
            .filter(|path| path.is_absolute())
            .ok_or_else(native_cmd_unavailable)?
    } else {
        return Err(native_cmd_unavailable());
    };
    let canonical = std::fs::canonicalize(&path).map_err(|_| native_cmd_unavailable())?;
    if let Some(stem) = identity
        && !cli_identity_matches(&canonical.to_string_lossy(), stem)
    {
        return Err(decl
            .ide_unsupported
            .map_or_else(native_cmd_unavailable, |refused| refused(cmd)));
    }
    if !is_native_executable(&canonical)
        || (!decl.allow_script_wrapper && is_script_wrapper(&canonical.to_string_lossy()))
    {
        return Err(native_cmd_unavailable());
    }
    // Keep ordinary platform path spelling; Windows native CLIs may reject
    // extended-length paths. The canonical target was checked above.
    let resolved = canonical.to_str().ok_or_else(native_cmd_unavailable)?;
    Ok(if cfg!(windows) {
        if let Some(unc) = resolved.strip_prefix(r"\\?\UNC\") {
            format!(r"\\{unc}")
        } else {
            resolved
                .strip_prefix(r"\\?\")
                .unwrap_or(resolved)
                .to_owned()
        }
    } else {
        resolved.to_owned()
    })
}

fn native_cmd_unavailable() -> core::Error {
    core::Error::new(
        "unsupported",
        "command does not resolve to an installed native executable",
    )
}

/// The installed CLI carries its upstream module name. Screen the file
/// before execution so an IDE copied to a capability-named file is refused.
fn has_cli_module(program: &str, module: &str, deadline: Instant) -> bool {
    use std::io::Read;
    let module = module.as_bytes();
    let Ok(mut file) = std::fs::File::open(program) else {
        return false;
    };
    if !file
        .metadata()
        .is_ok_and(|metadata| metadata.len() <= 512 * 1024 * 1024)
    {
        return false;
    }
    let mut bytes = vec![0; 64 * 1024];
    let mut carry = 0;
    while Instant::now() < deadline {
        let Ok(read) = file.read(&mut bytes[carry..]) else {
            return false;
        };
        if read == 0 {
            return false;
        }
        let end = carry + read;
        if bytes[..end]
            .windows(module.len())
            .any(|window| window == module)
        {
            return true;
        }
        carry = (module.len() - 1).min(end);
        bytes.copy_within(end - carry..end, 0);
    }
    false
}

pub(crate) async fn reader_executable(
    cmd: &str,
    harness: &str,
    deadline: Instant,
) -> core::Result<(String, Option<String>)> {
    let cmd = cmd.to_owned();
    let harness = harness.to_owned();
    let verify = discovery_for(&harness).ok();
    blocking_discovery(deadline, move || {
        let cmd = resolve_native_executable(&cmd, &harness)?;
        let version = match verify.map(|decl| decl.strategy) {
            Some(InstallStrategy::MetadataVersion(read)) => read(Path::new(&cmd)),
            _ => probe_optional_native_version(&cmd, verify.as_ref(), deadline)?,
        };
        if let Some(probe) = verify.and_then(|decl| decl.capability_probe)
            && version.is_none()
        {
            return Err(core::Error::new("unsupported", probe.refusal));
        }
        Ok((cmd, version))
    })
    .await?
}

/// Select the newest Claude reader with a successful parseable `--version`
/// under the caller's deadline, reusing its version from the same pass.
pub(crate) async fn claude_reader_executable(
    deadline: Instant,
) -> core::Result<core::api::HarnessCandidate> {
    blocking_discovery(deadline, move || {
        let best = discover_verified_claude(deadline)?;
        let cmd = resolve_native_executable(&best.cmd, "claude")?;
        Ok(core::api::HarnessCandidate {
            cmd,
            version: best.version.clone(),
        })
    })
    .await?
}

/// Async `harness_installation` for Tokio handlers: the blocking inventory
/// read runs on the blocking pool. An interrupted pool (runtime shutdown)
/// reports the harness as not installed with its cause, like a failed
/// discovery, instead of stalling the handler.
pub(crate) async fn harness_installation_async(harness: &str) -> Value {
    let owned = harness.to_owned();
    let deadline = Instant::now() + DISCOVERY_BUDGET;
    blocking_discovery(deadline, move || harness_installation(&owned, deadline))
        .await
        .unwrap_or_else(|error| installation_error(harness, error))
}

mod check_inventory;
pub(crate) use check_inventory::CheckInventory;

fn installation_error(harness: &str, error: core::Error) -> Value {
    json!({"harness": harness, "installed": false, "cmd": null, "version": null,
        "error": error.message, "code": error.code})
}

/// Blocking inventory. A shared deadline limits each next native probe.
pub(crate) fn harness_installation(harness: &str, deadline: Instant) -> Value {
    let read = || -> core::Result<Value> {
        let discovered = discover_cmd_for_harness(harness, deadline)?;
        let verify = discovery_for(harness).ok();
        let cmd = resolve_native_executable(discovered.as_deref().unwrap_or(harness), harness)?;
        let version = match verify.map(|decl| decl.strategy) {
            Some(InstallStrategy::MetadataVersion(read)) => read(Path::new(&cmd)),
            _ => probe_optional_native_version(&cmd, verify.as_ref(), deadline)?,
        };
        if let Some(probe) = verify.and_then(|decl| decl.capability_probe)
            && version.is_none()
        {
            return Err(core::Error::new("unsupported", probe.refusal));
        }
        Ok(json!({"harness": harness, "installed": true, "cmd": cmd, "version": version}))
    };
    read().unwrap_or_else(|error| installation_error(harness, error))
}

/// Bound on one `<exe> --version` probe: a hung native executable must not
/// stall `harness.list` for every harness.
const VERSION_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Memoized `<exe> --version` first lines: one passive discovery pass
/// (harness list, catalogue pages, routing checks) reuses the same
/// executable identity instead of relaunching every native CLI per page.
/// Entries expire after `VERSION_CACHE_TTL`; a changed installation is
/// re-probed on the next miss, so the refresh path stays deliberate without
/// starting native CLIs on every cached read.
const VERSION_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(300);
/// Bound on memoized executable identities.
const VERSION_CACHE_MAX: usize = 64;

#[derive(Clone, Hash, PartialEq, Eq)]
struct VersionKey {
    program: String,
    modified: Option<std::time::SystemTime>,
    size: u64,
    adapter_verification: Option<&'static str>,
}

impl VersionKey {
    fn read(program: &str, verify: Option<&InstallDiscovery>) -> Option<Self> {
        let metadata = Path::new(program).metadata().ok()?;
        Some(Self {
            program: program.to_owned(),
            modified: metadata.modified().ok(),
            size: metadata.len(),
            adapter_verification: verify
                .filter(|decl| decl.capability_probe.is_some())
                .map(|decl| decl.name),
        })
    }
}

type VersionResult = core::Result<Option<String>>;
type VersionCache = std::collections::HashMap<VersionKey, (Option<String>, Instant)>;

#[derive(Default)]
struct VersionProbes {
    cache: VersionCache,
    inflight: std::collections::HashMap<VersionKey, std::sync::Arc<InflightVersionProbe>>,
}

static VERSION_PROBES: std::sync::LazyLock<std::sync::Mutex<VersionProbes>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(VersionProbes::default()));

#[derive(Default)]
struct InflightVersionProbe {
    state: std::sync::Mutex<Option<VersionResult>>,
    ready: std::sync::Condvar,
}

enum VersionProbeRole {
    Cached(Option<String>),
    Leader(std::sync::Arc<InflightVersionProbe>),
    Follower(std::sync::Arc<InflightVersionProbe>),
}

fn probe_optional_native_version(
    program: &str,
    verify: Option<&InstallDiscovery>,
    deadline: Instant,
) -> VersionResult {
    match probe_native_version(program, verify, deadline) {
        Err(error) if error.code == "timeout" && Instant::now() < deadline => Ok(None),
        result => result,
    }
}

fn probe_native_version(
    program: &str,
    verify: Option<&InstallDiscovery>,
    deadline: Instant,
) -> VersionResult {
    discovery_remaining(deadline)?;
    let path = Path::new(program);
    if !path.is_absolute() || !is_native_executable(path) {
        return Err(native_cmd_unavailable());
    }
    let key = VersionKey::read(program, verify).ok_or_else(native_cmd_unavailable)?;
    let leader = match version_inflight_join(&key) {
        VersionProbeRole::Cached(result) => return Ok(result),
        VersionProbeRole::Leader(probe) => probe,
        VersionProbeRole::Follower(probe) => return version_inflight_wait(&probe, deadline),
    };
    let result = probe_version_uncached(program, verify, deadline);
    version_inflight_finish(key, &leader, result.clone());
    result
}

fn version_inflight_join(key: &VersionKey) -> VersionProbeRole {
    let mut probes = VERSION_PROBES.lock_recover();
    if let Some((version, at)) = probes.cache.get(key)
        && at.elapsed() < VERSION_CACHE_TTL
    {
        return VersionProbeRole::Cached(version.clone());
    }
    if let Some(probe) = probes.inflight.get(key) {
        return VersionProbeRole::Follower(probe.clone());
    }
    let probe = std::sync::Arc::new(InflightVersionProbe::default());
    probes.inflight.insert(key.clone(), probe.clone());
    VersionProbeRole::Leader(probe)
}

fn version_inflight_wait(probe: &InflightVersionProbe, deadline: Instant) -> VersionResult {
    let mut state = probe.state.lock_recover();
    loop {
        let remaining = discovery_remaining(deadline)?;
        if let Some(result) = &*state {
            return result.clone();
        }
        let (next, _) = probe
            .ready
            .wait_timeout(state, remaining)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state = next;
    }
}

fn version_inflight_finish(key: VersionKey, probe: &InflightVersionProbe, result: VersionResult) {
    let mut probes = VERSION_PROBES.lock_recover();
    probes.inflight.remove(&key);
    if let Ok(version) = &result {
        if probes.cache.len() >= VERSION_CACHE_MAX {
            probes
                .cache
                .retain(|_, (_, at)| at.elapsed() < VERSION_CACHE_TTL);
            if probes.cache.len() >= VERSION_CACHE_MAX {
                probes.cache.clear();
            }
        }
        probes.cache.insert(key, (version.clone(), Instant::now()));
    }
    *probe.state.lock_recover() = Some(result);
    probe.ready.notify_all();
}

fn probe_version_uncached(
    program: &str,
    verify: Option<&InstallDiscovery>,
    deadline: Instant,
) -> VersionResult {
    let capability = verify.and_then(|decl| decl.capability_probe);
    if let Some(probe) = capability {
        let has_module = has_cli_module(program, probe.executable_marker, deadline);
        discovery_remaining(deadline)?;
        if !has_module {
            return Ok(None);
        }
        let help = native_capture(
            program,
            probe.args,
            deadline.min(Instant::now() + VERSION_PROBE_TIMEOUT),
        )?;
        discovery_remaining(deadline)?;
        // The binary module identity and successful subcommand invocation
        // establish the CLI surface. Help prose and option spelling are not
        // version gates; launch still supplies its explicit configured argv.
        if !help.is_some_and(|capture| capture.status.success()) {
            return Ok(None);
        }
    }
    crate::logging::record(
        &core::SecretPolicy::default(),
        "harness.version_probe",
        &[("method", "--version")],
        None,
    );
    let bytes = native_capture(
        program,
        &["--version"],
        deadline.min(Instant::now() + VERSION_PROBE_TIMEOUT),
    )?
    .filter(|capture| capture.status.success() && capture.stdout_complete)
    .map(|capture| capture.stdout);
    discovery_remaining(deadline)?;
    Ok(bytes.as_deref().and_then(|bytes| {
        let text = std::str::from_utf8(bytes).ok()?.lines().next()?.trim();
        (!text.is_empty()
            && text.len() <= 256
            && (capability.is_none() || parse_cli_version(text).is_some()))
        .then(|| text.to_owned())
    }))
}

/// Bridge synchronous installation/profile resolution to the same bounded
/// process owner used by inference and actions. A joined thread permits calls
/// both inside a Tokio runtime and during synchronous configuration loading.
/// No process handle, pipe reader or runtime survives this call.
fn native_read(program: &str, args: &[&str], deadline: Instant) -> Option<Vec<u8>> {
    native_capture(program, args, deadline)
        .ok()
        .flatten()
        .map(|capture| capture.stdout)
}

#[expect(
    clippy::disallowed_types,
    reason = "This discovery helper captures one native command through bounded custody."
)]
fn native_capture(
    program: &str,
    args: &[&str],
    deadline: Instant,
) -> core::Result<Option<crate::scripts::process::ProcessCapture>> {
    use crate::scripts::process::{ProcessFailure, ProcessOutcome, run_owned_process};
    if !Path::new(program).is_absolute() || !is_native_executable(Path::new(program)) {
        return Ok(None);
    }
    use std::sync::Arc;

    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("native-discovery".to_owned())
            .spawn_scoped(scope, || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| {
                        core::Error::new("unavailable", "native discovery runtime could not start")
                    })?;
                runtime.block_on(async {
                    let budget = discovery_remaining(deadline)?;
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "This statement constructs a process owned by this module."
                    )]
                    let mut command = tokio::process::Command::new(program);
                    command
                        .args(args)
                        .env_clear()
                        .envs(crate::router::minimal_plugin_environment());
                    let (_cancel, cancellation) = tokio::sync::watch::channel(false);
                    match run_owned_process(
                        command,
                        Vec::new(),
                        budget,
                        Arc::new(core::SecretPolicy::default()),
                        cancellation,
                    )
                    .await
                    {
                        ProcessOutcome::Completed(capture) => Ok(Some(capture)),
                        ProcessOutcome::Failed {
                            reason: ProcessFailure::Timeout,
                            ..
                        } => Err(discovery_timeout()),
                        _ => Ok(None),
                    }
                })
            })
            .ok()?
            .join()
            .ok()
    })
    .unwrap_or(Ok(None))
}

/// Bound on one `<exe> auth list` read: a hung native executable must not
/// stall ad-hoc launch admission for every Owner.
const AUTH_LIST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Largest accepted `auth list` document. List output is metadata (hundreds
/// of bytes observed); anything larger is unreadable, never truncated.
const MAX_AUTH_LIST_BYTES: usize = 64 * 1024;

/// Read-only native integration ids from `<exe> auth list --format json
/// --standalone`: the canonical store the plugin's pre-serve transfer also
/// reads. Only nonsecret ids leave this function; connection bodies stay
/// unread. `None` when the helper cannot be spawned, times out, exits
/// non-zero, or its document is unreadable.
pub(crate) fn opencode_auth_ids(program: &str) -> Option<Vec<String>> {
    let stdout = native_read(
        program,
        &["auth", "list", "--format", "json", "--standalone"],
        Instant::now() + AUTH_LIST_TIMEOUT,
    )?;
    if stdout.len() > MAX_AUTH_LIST_BYTES {
        return None;
    }
    let entries: Vec<Value> = serde_json::from_slice(&stdout).ok()?;
    Some(
        entries
            .iter()
            .filter_map(|entry| entry.get("id")?.as_str())
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .collect(),
    )
}

/// Human-countable integration list for refusal messages: ids only, no
/// secrets, bounded with a remainder count.
fn describe_integrations(ids: &[String]) -> String {
    const SHOWN: usize = 32;
    if ids.is_empty() {
        return "(none)".to_owned();
    }
    let shown: Vec<&str> = ids.iter().take(SHOWN).map(String::as_str).collect();
    let mut text = shown.join(", ");
    if ids.len() > SHOWN {
        text.push_str(&format!(" (+{} more)", ids.len() - SHOWN));
    }
    text
}

/// Default an omitted ad-hoc OpenCode auth selector to the model provider
/// prefix, but only on an exact native auth-list id match. Never picks
/// another provider's credential: a missing prefix, an absent model, or an
/// unreadable list refuses with a typed error naming `--auth` and the
/// available integration ids.
fn resolve_opencode_default(model: Option<&str>, ids: &[String]) -> core::Result<String> {
    let prefix = model
        .and_then(split_provider_model)
        .map(|(provider, _)| provider.to_owned());
    match prefix {
        Some(prefix) if ids.iter().any(|id| id == &prefix) => Ok(prefix),
        Some(prefix) => Err(core::Error::new(
            "profile_error",
            format!(
                "ad-hoc opencode launch needs --auth <integration> for provider '{prefix}'; available integrations: {}",
                describe_integrations(ids)
            ),
        )),
        None => Err(core::Error::new(
            "profile_error",
            format!(
                "ad-hoc opencode launch needs --auth <integration>; available integrations: {}",
                describe_integrations(ids)
            ),
        )),
    }
}
