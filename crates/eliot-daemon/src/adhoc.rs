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

use core::api::native_profile::{CodexApprovalPolicy, CodexPermissions};
use eliot_core as core;
use eliot_plugin::session::split_provider_model;
use eliot_plugin::sync::LockExt;
use serde_json::{Map, Value, json};
use std::path::{Path, PathBuf};

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
pub const CLAUDE_EFFORT_LEVELS: &[&str] = &["low", "medium", "high", "xhigh", "max"];

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

/// Ad-hoc access levels for Owner launch: `ask` routes every tool or
/// permission request to the Owner through the existing pending-request
/// path (`swarm approve`/`deny`); `auto` lets the harness apply its safe
/// automatic posture; `full` selects the harness's full-access posture,
/// exactly the current full-access profiles. Documented in
/// `docs/spec/configuration.md` (access mapping) and per harness in
/// `docs/harnesses/*.md`.
pub(crate) const ADHOC_ACCESS_LEVELS: &[&str] = &["ask", "auto", "full"];

pub(crate) fn validate_adhoc_access(access: &str) -> core::Result<String> {
    let trimmed = access.trim();
    if ADHOC_ACCESS_LEVELS.contains(&trimmed) {
        Ok(trimmed.to_owned())
    } else {
        Err(core::Error::new(
            "invalid",
            "ad-hoc access must be ask, auto or full",
        ))
    }
}

/// Freeze the native permission result for one access level into the ad-hoc
/// snapshot. The frozen native fields are exactly the profile fields a
/// stored full-access profile uses, so launch-time effective settings report
/// the same values. `ask` always routes approvals through the existing
/// pending-request path; levels a harness cannot express refuse with a typed
/// `unsupported` instead of silently weakening.
fn apply_adhoc_access(
    snapshot: &mut Map<String, Value>,
    harness: &str,
    access: &str,
    profile_name: &str,
) -> core::Result<()> {
    snapshot.insert("access".to_owned(), Value::String(access.to_owned()));
    let invalid = || core::Error::new("invalid", "ad-hoc access must be ask, auto or full");
    let text = |value: &str| Value::String(value.to_owned());
    match harness {
        "codex" => {
            let (permissions, approval_policy) = match access {
                "ask" => (
                    CodexPermissions::WorkspaceWrite,
                    CodexApprovalPolicy::OnRequest,
                ),
                "auto" => (CodexPermissions::WorkspaceWrite, CodexApprovalPolicy::Never),
                "full" => (
                    CodexPermissions::DangerFullAccess,
                    CodexApprovalPolicy::Never,
                ),
                _ => return Err(invalid()),
            };
            snapshot.insert("permissions".to_owned(), text(permissions.as_str()));
            snapshot.insert("approval_policy".to_owned(), text(approval_policy.as_str()));
            Ok(())
        }
        "claude" => {
            // The frozen `permission_mode` is the CLI `--permission-mode`
            // argv the Claude plugin pushes; `manual` routes `can_use_tool`
            // through the pending-request path, `auto` approves safe edits,
            // `bypassPermissions` is the full-access posture.
            let mode = match access {
                "ask" => "manual",
                "auto" => "auto",
                "full" => "bypassPermissions",
                _ => return Err(invalid()),
            };
            snapshot.insert("permission_mode".to_owned(), text(mode));
            Ok(())
        }
        "muse" => {
            let (approval, permissions) = match access {
                "ask" => ("onRequest", CodexPermissions::WorkspaceWrite),
                "auto" => ("promptUnmatched", CodexPermissions::WorkspaceWrite),
                "full" => ("allowAll", CodexPermissions::DangerFullAccess),
                _ => return Err(invalid()),
            };
            snapshot.insert("approval".to_owned(), text(approval));
            snapshot.insert("permissions".to_owned(), text(permissions.as_str()));
            Ok(())
        }
        "opencode" => match access {
            // OpenCode exposes no verified distinct native permission knob
            // today: ask and auto share the native permission-reply surface
            // (permission requests stay pending for the Owner). `full` has no
            // full-access profile mapping, so it refuses instead of silently
            // widening.
            "ask" | "auto" => Ok(()),
            "full" => Err(core::Error::new(
                "unsupported",
                "the opencode slice has no full-access permission mapping",
            )),
            _ => Err(invalid()),
        },
        "antigravity" => {
            // `false` keeps the ACP `session/request_permission` round-trip
            // that bridges to `swarm approve`/`deny`; `true` is the
            // full-access skip flag.
            let skip = match access {
                "ask" | "auto" => false,
                "full" => true,
                _ => return Err(invalid()),
            };
            snapshot.insert("dangerously_skip_permissions".to_owned(), Value::Bool(skip));
            Ok(())
        }
        _ => Err(core::Error::new(
            "unsupported",
            format!("unsupported launch harness {harness}"),
        )),
    }
    .map_err(|error| {
        if error.code == "invalid" {
            error
        } else {
            core::Error::new(
                error.code.clone(),
                format!("ad-hoc {profile_name}: {}", error.message),
            )
        }
    })
}

/// Longest accepted ad-hoc auth selector, in bytes. Mirrors the plugin's
/// `IntegrationSelector` rule so the frozen value always passes the
/// profile-side check.
const MAX_AUTH_SELECTOR_BYTES: usize = 128;

/// Ad-hoc auth shape: only the OpenCode slice transfers a canonical auth
/// integration, so only it accepts `--auth`. The syntax mirrors the
/// profile's `auth_integration` selector.
fn validate_adhoc_auth(harness: &str, auth: &str) -> core::Result<String> {
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
        .map(validate_adhoc_access)
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
            if !antigravity_cli_identity(&validated) {
                return Err(core::Error::new(
                    "profile_error",
                    format!(
                        "ad-hoc {profile_name}: cmd points at the Antigravity IDE ({validated}), which does not implement the required native ACP/model CLI and is never launched; use the `agy` CLI"
                    ),
                ));
            }
            snapshot.insert("cmd".to_owned(), Value::String(validated));
            snapshot.insert(
                "dangerously_skip_permissions".to_owned(),
                Value::Bool(false),
            );
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
    if matches!(harness.as_str(), "codex" | "claude" | "muse") {
        snapshot.insert(
            "permissions".to_owned(),
            Value::String(CodexPermissions::default().as_str().to_owned()),
        );
        snapshot.insert(
            "approval_policy".to_owned(),
            Value::String(CodexApprovalPolicy::default().as_str().to_owned()),
        );
    }
    // Access mapping overwrites the safe defaults above with the exact
    // native permission result for the requested level, freezing both the
    // level and its native fields. Omitted access keeps the safe defaults.
    if let Some(access) = access.as_deref() {
        apply_adhoc_access(&mut snapshot, &harness, access, &profile_name)?;
    }
    // Claude can enforce the same isolated MCP policy as a saved profile.
    let mcp = if harness == "claude" {
        "minimal"
    } else {
        "inherit"
    };
    snapshot.insert("mcp".to_owned(), Value::String(mcp.to_owned()));
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
    let pass_env = if harness == "claude" {
        json!(["CLAUDE_CONFIG_DIR"])
    } else {
        json!([])
    };
    snapshot.insert("pass_env".to_owned(), pass_env);
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

/// Returns true when the path names a script wrapper that must never be
/// frozen as a native executable.
pub(crate) fn is_script_wrapper(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".cmd")
        || lower.ends_with(".bat")
        || lower.ends_with(".ps1")
        || lower.ends_with(".js")
}

/// Unix discovery accepts ELF or Mach-O payloads, including resolved
/// explicit commands. Windows keeps its native executable naming checks.
pub(crate) fn is_native_executable(path: &std::path::Path) -> bool {
    if !path.is_file() {
        return false;
    }
    if cfg!(windows) {
        return true;
    }
    if is_script_wrapper(&path.to_string_lossy()) {
        return false;
    }
    has_native_executable_magic(path)
}

fn has_native_executable_magic(path: &std::path::Path) -> bool {
    use std::io::Read;
    let mut header = [0u8; 4];
    if std::fs::File::open(path)
        .and_then(|mut file| file.read_exact(&mut header))
        .is_err()
    {
        return false;
    }
    matches!(
        header,
        [0x7f, b'E', b'L', b'F'] // ELF
            | [0xfe, 0xed, 0xfa, 0xce] // Mach-O 32-bit
            | [0xfe, 0xed, 0xfa, 0xcf] // Mach-O 64-bit
            | [0xce, 0xfa, 0xed, 0xfe] // Mach-O 32-bit, byte-swapped
            | [0xcf, 0xfa, 0xed, 0xfe] // Mach-O 64-bit, byte-swapped
            | [0xca, 0xfe, 0xba, 0xbe] // Mach-O universal (fat)
            | [0xbe, 0xba, 0xfe, 0xca] // Mach-O universal, byte-swapped
    )
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

/// Resolve the installed executable. Codex's default launch keeps its native
/// PATH resolution; readers pin the resolved executable before probing.
fn discover_cmd_for_harness(harness: &str, deadline: Instant) -> core::Result<Option<String>> {
    discovery_remaining(deadline)?;
    match harness {
        "codex" => Ok(None),
        "claude" => discover_verified_claude(deadline).map(|candidate| Some(candidate.cmd)),
        "muse" => discover_muse_cmd(Path::new(r"C:\Tools\MuseCode")).map(Some),
        "opencode" => discover_opencode_cmd().map(Some),
        "antigravity" => discover_antigravity_cmd().map(Some),
        _ => Err(core::Error::new(
            "unsupported",
            "unsupported native discovery harness",
        )),
    }
}

fn path_search_dirs() -> Vec<PathBuf> {
    std::env::var_os("PATH").map_or_else(Vec::new, |paths| std::env::split_paths(&paths).collect())
}

/// Native executable file name for `stem` on this platform: `stem.exe` on
/// Windows, the bare stem on Unix. The supported Linux image installs the
/// vendors' binaries under `/usr/local/bin` (see docker/Dockerfile), so no
/// `.exe`/`.cmd` wrapper name is ever probed there.
fn native_exe_name(stem: &str) -> String {
    if cfg!(windows) {
        format!("{stem}.exe")
    } else {
        stem.to_owned()
    }
}

pub(crate) fn find_on_path(file: &str) -> Option<PathBuf> {
    let target = file.to_ascii_lowercase();
    for dir in path_search_dirs() {
        let candidate = dir.join(file);
        if is_native_executable(&candidate)
            && candidate
                .file_name()
                .is_some_and(|name| name.to_string_lossy().to_ascii_lowercase() == target)
        {
            return Some(candidate);
        }
    }
    None
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

fn user_profile() -> Option<PathBuf> {
    std::env::var_os("USERPROFILE")
        .or_else(|| std::env::var_os("HOME"))
        .map(PathBuf::from)
}

/// Well-known data roots for one Windows shell folder, without probing the
/// filesystem: the owning environment variable first, then the
/// `USERPROFILE`-derived default. The daemon environment may strip
/// `APPDATA`/`LOCALAPPDATA` while the native installs still exist under the
/// profile, so discovery must consult the derived roots too.
fn data_roots(
    variable: Option<std::ffi::OsString>,
    home: Option<std::ffi::OsString>,
    leaf: &str,
) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(dir) = variable {
        roots.push(PathBuf::from(dir));
    }
    if let Some(home) = home {
        let derived = PathBuf::from(home).join("AppData").join(leaf);
        if !roots.contains(&derived) {
            roots.push(derived);
        }
    }
    roots
}

fn local_roots() -> Vec<PathBuf> {
    data_roots(
        std::env::var_os("LOCALAPPDATA").or_else(|| std::env::var_os("APPDATA")),
        user_profile().map(|path| path.into_os_string()),
        "Local",
    )
}

fn roaming_roots() -> Vec<PathBuf> {
    data_roots(
        std::env::var_os("APPDATA"),
        user_profile().map(|path| path.into_os_string()),
        "Roaming",
    )
}

/// Newest-first ordering key for `<exe> --version` output: the first
/// `major.minor.patch` numeric triple on the bounded first line
/// (`2.1.325 (Claude Code)`, `1.4.20-R8619.1`).
fn parse_cli_version(text: &str) -> Option<CliVersion> {
    let first = text.lines().next().unwrap_or_default();
    for token in first.split(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '-')) {
        let core = token.split('-').next().unwrap_or(token);
        let mut parts = core.split('.');
        let triple = match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(major), Some(minor), Some(patch), None) => {
                match (major.parse(), minor.parse(), patch.parse()) {
                    (Ok(major), Ok(minor), Ok(patch)) => Some((major, minor, patch)),
                    _ => None,
                }
            }
            _ => None,
        };
        if triple.is_some() {
            return triple;
        }
    }
    None
}

type CliVersion = (u64, u64, u64);
type VersionedCliPath = (PathBuf, Option<CliVersion>);

/// Discover native Claude CLI installations and probe their versions.
pub(crate) fn discover_claude_candidates(deadline: Instant) -> Vec<core::api::HarnessCandidate> {
    probe_claude_installations(deadline).0
}

fn probe_claude_installations(
    deadline: Instant,
) -> (Vec<core::api::HarnessCandidate>, Option<CliVersion>) {
    let mut timed_out_version = None;
    let candidates = probe_candidate_paths_with(claude_candidate_paths(), |cmd| {
        match probe_native_version(cmd, false, deadline) {
            Ok(version) => version,
            Err(error) => {
                if error.code == "timeout" {
                    let labelled = Path::new(cmd)
                        .ancestors()
                        .take(4)
                        .find(|entry| {
                            entry
                                .parent()
                                .and_then(Path::file_name)
                                .is_some_and(|parent| {
                                    parent.eq_ignore_ascii_case("claude-code")
                                        || parent == "versions"
                                })
                        })
                        .and_then(Path::file_name)
                        .and_then(|name| name.to_str())
                        .and_then(parse_cli_version);
                    timed_out_version = timed_out_version.max(labelled);
                }
                None
            }
        }
    });
    (candidates, timed_out_version)
}

fn discover_verified_claude(deadline: Instant) -> core::Result<core::api::HarnessCandidate> {
    let (candidates, timed_out_version) = probe_claude_installations(deadline);
    discovery_remaining(deadline)?;
    let (best, version) = verified_claude_candidate_from_candidates(&candidates)
        .and_then(|candidate| Some((candidate, parse_cli_version(candidate.version.as_deref()?)?)))
        .ok_or_else(|| claude_discovery_error(&candidates))?;
    if timed_out_version.is_some_and(|timed_out| timed_out > version) {
        return Err(core::Error::new(
            "timeout",
            "a newer installed Claude CLI timed out during --version",
        ));
    }
    Ok(best.clone())
}

fn versioned_claude_paths(
    root: &Path,
    executable: &str,
    nested_hash: bool,
) -> Vec<VersionedCliPath> {
    let Ok(entries) = std::fs::read_dir(root) else {
        return Vec::new();
    };
    let mut versions = entries
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            (path.is_dir() || (!nested_hash && is_native_executable(&path))).then(|| {
                let version = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(parse_cli_version);
                (path, version)
            })
        })
        .collect::<Vec<_>>();
    versions.sort_by(|(left_path, left_version), (right_path, right_version)| {
        right_version
            .cmp(left_version)
            .then_with(|| left_path.cmp(right_path))
    });

    let mut candidates = Vec::new();
    for (version_path, version) in versions {
        if nested_hash {
            let Ok(hash_entries) = std::fs::read_dir(&version_path) else {
                continue;
            };
            let mut hash_dirs = hash_entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.is_dir())
                .collect::<Vec<_>>();
            hash_dirs.sort();
            for hash_dir in hash_dirs {
                let path = hash_dir.join(executable);
                if is_native_executable(&path) {
                    candidates.push((path, version));
                }
            }
        } else {
            let path = if version_path.is_file() {
                version_path
            } else {
                version_path.join(executable)
            };
            if is_native_executable(&path) {
                candidates.push((path, version));
            }
        }
    }
    candidates
}

/// Native versioned roots precede PATH so semver candidates are probed first.
/// Standard user locations follow. Unix layouts follow vendor installers:
/// the supported Linux image symlinks `/usr/local/bin/claude` at the
/// official installer root (see docker/Dockerfile).
fn claude_candidate_paths() -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut push = |path: PathBuf| {
        if !paths.contains(&path) {
            paths.push(path);
        }
    };
    let executable = native_exe_name("claude");
    let mut versioned = Vec::new();
    if cfg!(windows) {
        for roaming in roaming_roots() {
            versioned.extend(versioned_claude_paths(
                &roaming.join("Claude").join("claude-code"),
                &executable,
                true,
            ));
        }
    }
    if let Some(home) = user_profile() {
        versioned.extend(versioned_claude_paths(
            &home
                .join(".local")
                .join("share")
                .join("claude")
                .join("versions"),
            &executable,
            false,
        ));
    }
    versioned.sort_by(|(left_path, left_version), (right_path, right_version)| {
        right_version
            .cmp(left_version)
            .then_with(|| left_path.cmp(right_path))
    });
    for (path, _) in versioned {
        push(path);
    }
    if cfg!(windows) {
        for dir in path_search_dirs() {
            if !dir.is_absolute() {
                continue;
            }
            let exe = dir.join(native_exe_name("claude"));
            if exe.is_file() {
                push(exe);
            }
        }
        if let Some(home) = user_profile() {
            push(
                home.join(".local")
                    .join("bin")
                    .join(native_exe_name("claude")),
            );
        }
        for roaming in roaming_roots() {
            push(roaming.join("npm").join(native_exe_name("claude")));
        }
        for local in local_roots() {
            push(
                local
                    .join("Programs")
                    .join("claude")
                    .join(native_exe_name("claude")),
            );
        }
    } else {
        for dir in path_search_dirs() {
            // Absolute native payloads only: a relative PATH entry is never
            // probed, and an extensionless script never counts as native.
            if !dir.is_absolute() {
                continue;
            }
            let exe = dir.join("claude");
            if is_native_executable(&exe) {
                push(exe);
            }
        }
        let fixed = Path::new("/usr/local/bin/claude");
        if is_native_executable(fixed) {
            push(fixed.to_path_buf());
        }
        if let Some(home) = user_profile() {
            push(home.join(".local").join("bin").join("claude"));
        }
    }
    paths
}

/// Version candidate paths, newest first, probing at most
/// [`MAX_DISCOVERY_CANDIDATE_PROBES`] native executables: filesystem hits
/// are cheap, process launches are not. Paths past the cap stay listed
/// without a version so inventory stays complete while launches stay
/// bounded. Automatic selection accepts only candidates with a successful,
/// parseable `--version`; inventory retains every path and any raw first line.
/// The stable sort keeps parsed versions newest-first and other paths in
/// discovery order at the end.
fn probe_candidate_paths_with(
    paths: Vec<PathBuf>,
    mut probe: impl FnMut(&str) -> Option<String>,
) -> Vec<core::api::HarnessCandidate> {
    /// One discovered install plus its parsed `--version` triple, if any.
    type VersionedCandidate = (core::api::HarnessCandidate, Option<(u64, u64, u64)>);
    // Absolute native payloads only, before any probe: a relative candidate
    // is never launched for `--version`, and on Unix an extensionless script
    // never counts as a native executable.
    let existing: Vec<PathBuf> = paths
        .into_iter()
        .filter(|path| path.is_absolute() && is_native_executable(path))
        .collect();
    let mut candidates: Vec<VersionedCandidate> = existing
        .iter()
        .take(MAX_DISCOVERY_CANDIDATE_PROBES)
        .map(|path| {
            let cmd = path.to_string_lossy().into_owned();
            let version = probe(&cmd);
            (
                core::api::HarnessCandidate {
                    cmd,
                    version: version.clone(),
                },
                version.as_deref().and_then(parse_cli_version),
            )
        })
        .collect();
    let mut unversioned: Vec<VersionedCandidate> = existing
        .iter()
        .skip(MAX_DISCOVERY_CANDIDATE_PROBES)
        .map(|path| {
            (
                core::api::HarnessCandidate {
                    cmd: path.to_string_lossy().into_owned(),
                    version: None,
                },
                None,
            )
        })
        .collect();
    candidates.append(&mut unversioned);
    candidates.retain(|(candidate, _)| {
        Path::new(&candidate.cmd).is_absolute() && !is_script_wrapper(&candidate.cmd)
    });
    // Stable sort: versioned newest-first, unversioned last in discovery
    // order. Ties keep the stable priority order above.
    candidates.sort_by(|left, right| match (&left.1, &right.1) {
        (Some(left), Some(right)) => right.cmp(left),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    candidates
        .into_iter()
        .map(|(candidate, _)| candidate)
        .collect()
}

fn claude_search_locations() -> Vec<String> {
    if cfg!(not(windows)) {
        let mut searched = vec![
            "claude on PATH".to_owned(),
            "/usr/local/bin/claude".to_owned(),
        ];
        if let Some(home) = user_profile() {
            searched.push(
                home.join(".local")
                    .join("bin")
                    .join("claude")
                    .to_string_lossy()
                    .into_owned(),
            );
            searched.push(
                home.join(".local")
                    .join("share")
                    .join("claude")
                    .join("versions")
                    .join("<version>")
                    .join("claude")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        searched.dedup();
        return searched;
    }
    let mut searched = vec!["claude.exe on PATH".to_owned()];
    if let Some(home) = user_profile() {
        searched.push(
            home.join(".local")
                .join("bin")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
        searched.push(
            home.join(".local")
                .join("share")
                .join("claude")
                .join("versions")
                .join("<version>")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    for roaming in roaming_roots() {
        searched.push(
            roaming
                .join("Claude")
                .join("claude-code")
                .join("<version>")
                .join("<hash>")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
        searched.push(
            roaming
                .join("npm")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    for local in local_roots() {
        searched.push(
            local
                .join("Programs")
                .join("claude")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    searched.dedup();
    searched
}

fn verified_claude_candidate_from_candidates(
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

fn claude_discovery_error(candidates: &[core::api::HarnessCandidate]) -> core::Error {
    let detail = if candidates.is_empty() {
        "Standalone Claude CLI was not found"
    } else {
        "Claude CLI executables were found, but none had a successful parseable --version result"
    };
    core::Error::new(
        "profile_error",
        format!(
            "{detail}; searched {}. Install Claude Code or select a native executable explicitly with --cmd",
            claude_search_locations().join(", ")
        ),
    )
}

pub(crate) fn is_latest_claude_selector(cmd: &str) -> bool {
    cmd.trim().eq_ignore_ascii_case("latest")
}

pub(crate) fn claude_candidate_available() -> bool {
    claude_candidate_paths()
        .iter()
        .any(|path| is_native_executable(path))
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
                version: probe_native_version(&cmd, false, deadline).ok().flatten(),
                cmd,
            });
        }
        let candidates = discover_claude_candidates(deadline);
        return Err(missing_claude_profile_command(profile, &cmd, &candidates));
    }

    discover_verified_claude(deadline)
        .map_err(|error| core::Error::new(error.code, format!("{profile}: {}", error.message)))
}

pub(crate) async fn ensure_claude_profile_command(profile: &str, cmd: &str) -> core::Result<()> {
    let profile = profile.to_owned();
    let cmd = cmd.to_owned();
    let deadline = Instant::now() + DISCOVERY_BUDGET;
    blocking_discovery(deadline, move || {
        let cmd = validate_claude_cmd(&profile, &cmd)?;
        if is_native_executable(Path::new(&cmd)) {
            return Ok(());
        }
        let candidates = discover_claude_candidates(deadline);
        Err(missing_claude_profile_command(&profile, &cmd, &candidates))
    })
    .await?
}

fn missing_claude_profile_command(
    profile: &str,
    cmd: &str,
    candidates: &[core::api::HarnessCandidate],
) -> core::Error {
    let replacement = verified_claude_candidate_from_candidates(candidates);
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
    if let Some(best) = verified_claude_candidate_from_candidates(candidates) {
        let version = best.version.clone().map_or(Value::Null, Value::String);
        json!({
            "harness": "claude",
            "installed": true,
            "cmd": best.cmd.clone(),
            "version": version,
        })
    } else {
        let error = claude_discovery_error(candidates);
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

/// Candidate native executables for an npm-installed OpenCode, in priority
/// order: the shim target
/// (`<shim-dir>/node_modules/@opencode/cli/bin/opencode.exe`) for every
/// discovered `opencode.cmd` shim, then each known package-local path under
/// every roaming root (the shim itself must never be frozen).
fn opencode_candidates(shim_dirs: &[PathBuf], roaming: &[PathBuf]) -> Vec<PathBuf> {
    const PACKAGE: &[&str] = &["node_modules", "@opencode", "cli"];
    fn package_bin(root: &Path, extra: &[&str]) -> PathBuf {
        let mut path = root.to_owned();
        for segment in PACKAGE.iter().chain(extra.iter()) {
            path = path.join(segment);
        }
        path.join("bin").join("opencode.exe")
    }
    let mut candidates = Vec::new();
    let mut push = |path: PathBuf| {
        if !candidates.contains(&path) {
            candidates.push(path);
        }
    };
    for dir in shim_dirs {
        push(package_bin(dir, &[]));
    }
    for root in roaming {
        let npm = root.join("npm");
        push(package_bin(&npm, &[]));
        for arch in ["cli-windows-x64", "cli-windows-x64-baseline"] {
            push(package_bin(&npm, &["node_modules", "@opencode", arch]));
        }
    }
    candidates
}

fn opencode_shim_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let mut push = |dir: PathBuf| {
        if !dirs.contains(&dir) {
            dirs.push(dir);
        }
    };
    for dir in path_search_dirs() {
        if dir.join("opencode.cmd").is_file() {
            push(dir);
        }
    }
    for root in roaming_roots() {
        push(root.join("npm"));
    }
    dirs
}

fn discover_opencode_cmd() -> core::Result<String> {
    if cfg!(windows) {
        return discover_opencode_cmd_windows();
    }
    discover_opencode_cmd_unix()
}

/// Unix OpenCode: the bare `opencode` binary. The supported Linux image
/// symlinks `/usr/local/bin/opencode` at the official 2.2.7 install root
/// (see docker/Dockerfile); the Windows npm `.cmd` shim layout does not
/// exist here and is never consulted.
fn discover_opencode_cmd_unix() -> core::Result<String> {
    let mut searched = vec!["opencode on PATH".to_owned()];
    let mut candidates = Vec::new();
    for dir in path_search_dirs() {
        let exe = dir.join("opencode");
        if is_native_executable(&exe) {
            candidates.push(exe);
        }
    }
    for fixed in [
        Path::new("/usr/local/bin/opencode").to_path_buf(),
        Path::new("/opt/opencode/.opencode/bin/opencode").to_path_buf(),
    ] {
        searched.push(fixed.to_string_lossy().into_owned());
        if is_native_executable(&fixed) && !candidates.contains(&fixed) {
            candidates.push(fixed);
        }
    }
    if let Some(home) = user_profile() {
        let exe = home.join(".opencode").join("bin").join("opencode");
        searched.push(exe.to_string_lossy().into_owned());
        if is_native_executable(&exe) && !candidates.contains(&exe) {
            candidates.push(exe);
        }
    }
    for candidate in &candidates {
        if candidate.is_file() {
            let path = candidate.to_string_lossy().into_owned();
            if !is_script_wrapper(&path) && Path::new(&path).is_absolute() {
                return Ok(path);
            }
        }
    }
    Err(core::Error::new(
        "unsupported",
        format!(
            "opencode native executable was not found; searched {}",
            searched.join(", ")
        ),
    ))
}

fn discover_opencode_cmd_windows() -> core::Result<String> {
    // Follow the npm shim to the native exe inside the package: the shim
    // itself (opencode.cmd) must never be frozen.
    let roaming = roaming_roots();
    let candidates = opencode_candidates(&opencode_shim_dirs(), &roaming);
    let mut searched: Vec<String> = candidates
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
    for candidate in &candidates {
        if candidate.is_file() {
            let path = candidate.to_string_lossy().into_owned();
            if !is_script_wrapper(&path) {
                return Ok(path);
            }
        }
    }
    if let Some(found) = find_on_path("opencode.exe") {
        let path = found.to_string_lossy().into_owned();
        if !is_script_wrapper(&path) && Path::new(&path).is_absolute() {
            return Ok(path);
        }
        searched.push("opencode.exe on PATH (wrapper or non-absolute)".to_owned());
    } else {
        searched.push("opencode.exe on PATH".to_owned());
    }
    Err(core::Error::new(
        "unsupported",
        format!(
            "opencode native executable was not found; searched {}",
            searched.join(", ")
        ),
    ))
}

fn current_muse_version(launcher_dir: &Path) -> Option<String> {
    for name in ["muse-version", ".muse-version"] {
        let file = launcher_dir.join(name);
        if let Ok(text) = std::fs::read_to_string(&file) {
            let version = text.trim().to_owned();
            if !version.is_empty()
                && version.len() <= 64
                && !version.chars().any(char::is_control)
                && !version.contains(['/', '\\', ':', '*', '?', '"', '<', '>', '|'])
            {
                return Some(version);
            }
        }
    }
    None
}

fn discover_muse_cmd(launcher_dir: &Path) -> core::Result<String> {
    if cfg!(windows) {
        return discover_muse_cmd_windows(launcher_dir);
    }
    let _ = launcher_dir;
    discover_muse_cmd_unix()
}

/// Unix Muse: the native payload installs directly (the supported Linux
/// image puts the official release-manifest binary at `/usr/local/bin/muse`
/// with no launcher selection, see docker/Dockerfile), so `PATH` plus the
/// documented absolute path win. No `--version` probe runs here; the binary
/// is validated on use.
fn discover_muse_cmd_unix() -> core::Result<String> {
    let mut searched = vec!["muse on PATH".to_owned(), "/usr/local/bin/muse".to_owned()];
    if let Some(found) = find_on_path("muse") {
        let path = found.to_string_lossy().into_owned();
        if !is_script_wrapper(&path) && Path::new(&path).is_absolute() {
            return Ok(path);
        }
        searched.push(format!("muse on PATH (wrapper or non-absolute: {path})"));
    }
    let fixed = Path::new("/usr/local/bin/muse");
    if is_native_executable(fixed) {
        return Ok(fixed.to_string_lossy().into_owned());
    }
    Err(core::Error::new(
        "profile_error",
        format!(
            "muse native executable was not found; searched {}",
            searched.join(", ")
        ),
    ))
}

fn discover_muse_cmd_windows(launcher_dir: &Path) -> core::Result<String> {
    let version = current_muse_version(launcher_dir).ok_or_else(|| {
        core::Error::new(
            "profile_error",
            format!(
                "muse launcher version is unknown; searched {}",
                launcher_dir.join(".muse-version").to_string_lossy()
            ),
        )
    })?;
    // Never resolve `muse`/`muse.cmd` from PATH: that lands on the launcher
    // wrapper whose PID would orphan the real `muse serve` host on stop.
    let file_name = format!("muse-bin-{version}.exe");
    let native = launcher_dir.join(&file_name);
    if native.is_file() {
        return Ok(native.to_string_lossy().into_owned());
    }
    Err(core::Error::new(
        "profile_error",
        format!(
            "muse native executable for launcher version {version} was not found; searched {}",
            native.to_string_lossy()
        ),
    ))
}

fn discover_antigravity_cmd() -> core::Result<String> {
    // The `agy` CLI serves both `acp` (launch) and `models` (catalogue);
    // prefer it over the Electron app path, which boots the IDE and serves
    // neither (observed 2026-11-15: `Antigravity.exe --version` starts the
    // app instead of printing a version, while `agy --version` prints 1.5.9
    // and `agy models` lists the catalogue).
    if let Some(found) = find_on_path("agy.exe").or_else(|| find_on_path("agy")) {
        let path = found.to_string_lossy().into_owned();
        if !is_script_wrapper(&path) && Path::new(&path).is_absolute() {
            return Ok(path);
        }
    }
    let mut searched: Vec<String> = vec!["agy.exe on PATH".to_owned()];
    if cfg!(not(windows)) {
        searched.push("agy on PATH".to_owned());
    }
    // An IDE-only install is reported with actionable guidance: name where
    // the IDE was seen so the message points at the fix without launching it.
    let mut ide_hint: Option<String> = None;
    for local in local_roots() {
        let native = local
            .join("Programs")
            .join("Antigravity")
            .join("Antigravity.exe");
        searched.push(native.to_string_lossy().into_owned());
        if ide_hint.is_none() && native.is_file() {
            ide_hint = Some(native.to_string_lossy().into_owned());
        }
    }
    Err(antigravity_missing_cli(&searched, ide_hint.as_deref()))
}

/// Antigravity CLI identity from the file name alone, before any probe: the
/// stem must be `agy`. The `Antigravity` IDE executable (or any other name)
/// never implements the native ACP/model CLI and is refused without ever
/// being launched.
pub(crate) fn antigravity_cli_identity(cmd: &str) -> bool {
    let file = cmd
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(cmd)
        .to_ascii_lowercase();
    file.strip_suffix(".exe").unwrap_or(&file) == "agy"
}

/// Typed refusal for an IDE-named Antigravity cmd: named for guidance only,
/// never returned, executed or version-probed.
pub(crate) fn antigravity_ide_unsupported(cmd: &str) -> core::Error {
    core::Error::new(
        "unsupported",
        format!(
            "antigravity cmd points at the Antigravity IDE ({cmd}), which does not implement the required native ACP/model CLI and is never launched for discovery; install the `agy` CLI and put it on PATH"
        ),
    )
}

/// Resolve and check the exact executable before any probe. An IDE symlink
/// cannot pass the CLI name check on its resolved target.
fn resolve_native_executable(cmd: &str, harness: &str) -> core::Result<String> {
    let antigravity = harness == "antigravity";
    if antigravity && !antigravity_cli_identity(cmd) {
        return Err(antigravity_ide_unsupported(cmd));
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
    if antigravity && !antigravity_cli_identity(&canonical.to_string_lossy()) {
        return Err(antigravity_ide_unsupported(cmd));
    }
    if !is_native_executable(&canonical)
        || (harness != "codex" && is_script_wrapper(&canonical.to_string_lossy()))
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

fn antigravity_capability_error() -> core::Error {
    core::Error::new(
        "unsupported",
        "executable did not establish Antigravity CLI ACP capability",
    )
}

fn agy_help_has_acp(help: &[u8]) -> bool {
    std::str::from_utf8(help).is_ok_and(|help| {
        help.contains("--stdio")
            && help.contains("--dangerously-skip-permissions")
            && help.lines().any(|line| {
                let mut words = line.split_whitespace();
                words.next() == Some("Usage:")
                    && matches!(words.next(), Some("agy" | "agy.exe"))
                    && words.next() == Some("acp")
            })
    })
}

/// The installed CLI carries its upstream Go module name. Screen the file
/// before execution so an IDE copied to a file named agy is also refused.
fn has_agy_cli_module(program: &str, deadline: Instant) -> bool {
    use std::io::Read;
    const MODULE: &[u8] = b"google-antigravity/antigravity-cli";
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
            .windows(MODULE.len())
            .any(|window| window == MODULE)
        {
            return true;
        }
        carry = (MODULE.len() - 1).min(end);
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
    let agy = harness == "antigravity";
    blocking_discovery(deadline, move || {
        let cmd = resolve_native_executable(&cmd, &harness)?;
        let version = probe_optional_native_version(&cmd, agy, deadline)?;
        if agy && version.is_none() {
            return Err(antigravity_capability_error());
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

/// Error for an `agy`-less install. The IDE path, when seen, is named for
/// guidance only: it is never returned, executed or version-probed.
fn antigravity_missing_cli(searched: &[String], ide: Option<&str>) -> core::Error {
    core::Error::new(
        "unsupported",
        match ide {
            Some(ide) => format!(
                "antigravity native CLI `agy` was not found on PATH; the Antigravity IDE at {ide} does not implement the required native ACP/model CLI and is never launched for discovery. Install the `agy` CLI and put it on PATH; searched {}",
                searched.join(", ")
            ),
            None => format!(
                "antigravity native executable was not found; searched {}",
                searched.join(", ")
            ),
        },
    )
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

fn installation_error(harness: &str, error: core::Error) -> Value {
    json!({"harness": harness, "installed": false, "cmd": null, "version": null,
        "error": error.message, "code": error.code})
}

/// Blocking inventory. A shared deadline limits each next native probe.
pub(crate) fn harness_installation(harness: &str, deadline: Instant) -> Value {
    let read = || -> core::Result<Value> {
        let discovered = discover_cmd_for_harness(harness, deadline)?;
        let agy = harness == "antigravity";
        let cmd = resolve_native_executable(discovered.as_deref().unwrap_or(harness), harness)?;
        let version = probe_optional_native_version(&cmd, agy, deadline)?;
        if agy && version.is_none() {
            return Err(antigravity_capability_error());
        }
        Ok(json!({"harness": harness, "installed": true, "cmd": cmd, "version": version}))
    };
    read().unwrap_or_else(|error| installation_error(harness, error))
}

/// Bound on one `<exe> --version` probe: a hung native executable must not
/// stall `harness.list` for every harness.
const VERSION_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Cap on version probes per candidate enumeration: filesystem hits are
/// cheap, process launches are not; see [`probe_candidate_paths`].
pub(crate) const MAX_DISCOVERY_CANDIDATE_PROBES: usize = 8;

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
    agy: bool,
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

fn probe_optional_native_version(program: &str, agy: bool, deadline: Instant) -> VersionResult {
    match probe_native_version(program, agy, deadline) {
        Err(error) if error.code == "timeout" && Instant::now() < deadline => Ok(None),
        result => result,
    }
}

fn probe_native_version(program: &str, agy: bool, deadline: Instant) -> VersionResult {
    discovery_remaining(deadline)?;
    let path = Path::new(program);
    if !path.is_absolute() || !is_native_executable(path) {
        return Err(native_cmd_unavailable());
    }
    let metadata = path.metadata().map_err(|_| native_cmd_unavailable())?;
    let key = VersionKey {
        program: program.to_owned(),
        modified: metadata.modified().ok(),
        size: metadata.len(),
        agy,
    };
    let leader = match version_inflight_join(&key) {
        VersionProbeRole::Cached(result) => return Ok(result),
        VersionProbeRole::Leader(probe) => probe,
        VersionProbeRole::Follower(probe) => return version_inflight_wait(&probe, deadline),
    };
    let result = probe_version_uncached(program, agy, deadline);
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

fn probe_version_uncached(program: &str, agy: bool, deadline: Instant) -> VersionResult {
    if agy {
        let has_module = has_agy_cli_module(program, deadline);
        discovery_remaining(deadline)?;
        if !has_module {
            return Ok(None);
        }
        let help = native_capture(
            program,
            &["acp", "--help"],
            deadline.min(Instant::now() + VERSION_PROBE_TIMEOUT),
        )?;
        discovery_remaining(deadline)?;
        if !help.is_some_and(|capture| {
            agy_help_has_acp(&capture.stdout) || agy_help_has_acp(capture.stderr.text.as_bytes())
        }) {
            return Ok(None);
        }
    }
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
        (!text.is_empty() && text.len() <= 256 && (!agy || parse_cli_version(text).is_some()))
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
fn opencode_auth_ids(program: &str) -> Option<Vec<String>> {
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
