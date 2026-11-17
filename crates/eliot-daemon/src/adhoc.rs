//! Ad-hoc Owner launch: freeze a profile-equivalent snapshot from built-in
//! per-harness defaults plus flags, with deterministic executable discovery.
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

use eliot_core as core;
use serde_json::{Map, Value, json};
use std::path::{Path, PathBuf};

pub(crate) const ADHOC_PROFILE_PREFIX: &str = "adhoc-";
pub(crate) const SUPPORTED_HARNESSES: &[&str] =
    &["codex", "claude", "muse", "opencode", "antigravity"];

pub(crate) fn adhoc_profile_name(harness: &str) -> String {
    format!("{ADHOC_PROFILE_PREFIX}{harness}")
}

fn split_provider_model(model: &str) -> Option<(&str, &str)> {
    // First-slash split like profiles: nested ids such as
    // `openrouter/stealth/crouton-alpha` are allowed; a `#variant` suffix,
    // empty segments and controls are not.
    let (provider, id) = model.split_once('/')?;
    if provider.is_empty()
        || id.is_empty()
        || id.starts_with('/')
        || id.ends_with('/')
        || model.contains('#')
        || model.contains(char::is_control)
        || model.chars().any(char::is_whitespace)
    {
        return None;
    }
    Some((provider, id))
}

pub(crate) fn validate_adhoc_harness(harness: &str) -> core::Result<String> {
    let trimmed = harness.trim();
    if SUPPORTED_HARNESSES.contains(&trimmed) {
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
    match harness {
        "codex" => match access {
            "ask" => {
                snapshot.insert(
                    "permissions".to_owned(),
                    Value::String("workspace-write".to_owned()),
                );
                snapshot.insert(
                    "approval_policy".to_owned(),
                    Value::String("on-request".to_owned()),
                );
                Ok(())
            }
            "auto" => {
                snapshot.insert(
                    "permissions".to_owned(),
                    Value::String("workspace-write".to_owned()),
                );
                snapshot.insert(
                    "approval_policy".to_owned(),
                    Value::String("never".to_owned()),
                );
                Ok(())
            }
            "full" => {
                snapshot.insert(
                    "permissions".to_owned(),
                    Value::String("danger-full-access".to_owned()),
                );
                snapshot.insert(
                    "approval_policy".to_owned(),
                    Value::String("never".to_owned()),
                );
                Ok(())
            }
            _ => Err(core::Error::new(
                "invalid",
                "ad-hoc access must be ask, auto or full",
            )),
        },
        "claude" => {
            // The frozen `permission_mode` is the CLI `--permission-mode`
            // argv the Claude plugin pushes; `manual` routes `can_use_tool`
            // through the pending-request path, `auto` approves safe edits,
            // `bypassPermissions` is the full-access posture.
            let mode = match access {
                "ask" => "manual",
                "auto" => "auto",
                "full" => "bypassPermissions",
                _ => {
                    return Err(core::Error::new(
                        "invalid",
                        "ad-hoc access must be ask, auto or full",
                    ));
                }
            };
            snapshot.insert("permission_mode".to_owned(), Value::String(mode.to_owned()));
            Ok(())
        }
        "muse" => match access {
            "ask" => {
                snapshot.insert("approval".to_owned(), Value::String("onRequest".to_owned()));
                snapshot.insert(
                    "permissions".to_owned(),
                    Value::String("workspace-write".to_owned()),
                );
                Ok(())
            }
            "auto" => {
                snapshot.insert(
                    "approval".to_owned(),
                    Value::String("promptUnmatched".to_owned()),
                );
                snapshot.insert(
                    "permissions".to_owned(),
                    Value::String("workspace-write".to_owned()),
                );
                Ok(())
            }
            "full" => {
                snapshot.insert("approval".to_owned(), Value::String("allowAll".to_owned()));
                snapshot.insert(
                    "permissions".to_owned(),
                    Value::String("danger-full-access".to_owned()),
                );
                Ok(())
            }
            _ => Err(core::Error::new(
                "invalid",
                "ad-hoc access must be ask, auto or full",
            )),
        },
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
            _ => Err(core::Error::new(
                "invalid",
                "ad-hoc access must be ask, auto or full",
            )),
        },
        "antigravity" => match access {
            // `false` keeps the ACP `session/request_permission` round-trip
            // that bridges to `swarm approve`/`deny`; `true` is the
            // full-access skip flag.
            "ask" | "auto" => {
                snapshot.insert(
                    "dangerously_skip_permissions".to_owned(),
                    Value::Bool(false),
                );
                Ok(())
            }
            "full" => {
                snapshot.insert("dangerously_skip_permissions".to_owned(), Value::Bool(true));
                Ok(())
            }
            _ => Err(core::Error::new(
                "invalid",
                "ad-hoc access must be ask, auto or full",
            )),
        },
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
            if let Some(model) = model {
                snapshot.insert("model".to_owned(), Value::String(model));
            }
            if let Some(effort) = effort {
                snapshot.insert("effort".to_owned(), Value::String(effort));
            }
            snapshot.insert(
                "permissions".to_owned(),
                Value::String("workspace-write".to_owned()),
            );
            snapshot.insert(
                "approval_policy".to_owned(),
                Value::String("never".to_owned()),
            );
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
            if let Some(model) = model {
                snapshot.insert("model".to_owned(), Value::String(model));
            }
            // Closed `--effort` vocabulary, validated above; the Claude
            // plugin pushes it as the CLI `--effort` flag.
            if let Some(effort) = effort {
                snapshot.insert("effort".to_owned(), Value::String(effort));
            }
            snapshot.insert(
                "permissions".to_owned(),
                Value::String("workspace-write".to_owned()),
            );
            snapshot.insert(
                "approval_policy".to_owned(),
                Value::String("never".to_owned()),
            );
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
            if let Some(model) = model {
                snapshot.insert("model".to_owned(), Value::String(model));
            }
            if let Some(effort) = effort {
                snapshot.insert("effort".to_owned(), Value::String(effort));
            }
            snapshot.insert(
                "permissions".to_owned(),
                Value::String("workspace-write".to_owned()),
            );
            snapshot.insert(
                "approval_policy".to_owned(),
                Value::String("never".to_owned()),
            );
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
            if let Some(model) = model {
                snapshot.insert("model".to_owned(), Value::String(model));
            }
            // The frozen effort selects the installed variant id on launch
            // (`Model.Ref.variant`); the plugin gates it against the live
            // native catalogue before the session exists.
            if let Some(effort) = effort {
                snapshot.insert("effort".to_owned(), Value::String(effort));
            }
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
            snapshot.insert("cmd".to_owned(), Value::String(validated));
            if let Some(model) = model {
                snapshot.insert("model".to_owned(), Value::String(model));
            }
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
    // Access mapping overwrites the safe defaults above with the exact
    // native permission result for the requested level, freezing both the
    // level and its native fields. Omitted access keeps the safe defaults.
    if let Some(access) = access.as_deref() {
        apply_adhoc_access(&mut snapshot, &harness, access, &profile_name)?;
    }
    // Safe defaults shared by every ad-hoc snapshot: an explicit `inherit`
    // MCP opt-in (ad-hoc spans harnesses without a minimal mapping, so the
    // new minimal default cannot apply here), no allow list, no child
    // allow-list (children reuse the parent snapshot only), empty pass_env,
    // default scratch, and a profile-only capacity scope. No setup, route,
    // account, proxy, or elevated permissions are ever frozen here.
    snapshot.insert("mcp".to_owned(), Value::String("inherit".to_owned()));
    snapshot.insert("mcp_allow".to_owned(), Value::Array(Vec::new()));
    snapshot.insert("child_profiles".to_owned(), Value::Array(Vec::new()));
    snapshot.insert(
        "native_subagents".to_owned(),
        Value::String("allow".to_owned()),
    );
    snapshot.insert("pass_env".to_owned(), Value::Array(Vec::new()));
    snapshot.insert("scratch".to_owned(), json!(["target/", "node_modules/"]));
    snapshot.insert(
        "capacity_scopes".to_owned(),
        json!([{"scope": format!("profile:{profile_name}")}]),
    );
    Ok(Value::Object(snapshot))
}

/// Ad-hoc cmd shape: one explicit executable path, never a shell command.
/// Mirrors the profile `cmd` rule without a config source for errors.
fn validate_native_cmd(profile: &str, cmd: &str) -> core::Result<String> {
    let cmd = cmd.trim();
    if cmd.is_empty() || cmd.len() > 1024 {
        return Err(core::Error::new(
            "profile_error",
            format!("ad-hoc {profile}: cmd must be 1-1024 characters"),
        ));
    }
    const BACKSLASH: char = '\\';
    let file = cmd.rsplit(['/', BACKSLASH]).next().unwrap_or(cmd);
    let lower = file.to_ascii_lowercase();
    let stem = lower.strip_suffix(".exe").unwrap_or(&lower);
    const SHELLS: &[&str] = &[
        "sh",
        "bash",
        "dash",
        "zsh",
        "fish",
        "cmd",
        "powershell",
        "pwsh",
    ];
    let has_path_separator = cmd.contains('/') || cmd.contains(BACKSLASH);
    if SHELLS.contains(&stem)
        || (cmd.chars().any(char::is_whitespace)
            && (!has_path_separator || file.chars().any(char::is_whitespace)))
        || cmd.chars().any(char::is_control)
    {
        return Err(core::Error::new(
            "profile_error",
            format!("ad-hoc {profile}: cmd must name one executable path, not a shell command"),
        ));
    }
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

/// Resolve the installed native executable for a harness. Codex resolves by
/// name (`codex`) through the existing PATH lookup at spawn, so this returns
/// `None`. Every other harness returns an absolute native path, or a
/// deterministic `unsupported`/`profile_error` naming what was searched.
pub(crate) fn discover_cmd_for_harness(harness: &str) -> core::Result<Option<String>> {
    let harness = validate_adhoc_harness(harness)?;
    match harness.as_str() {
        "codex" => Ok(None),
        "claude" => discover_claude_cmd().map(Some),
        "muse" => discover_muse_cmd(Path::new(r"C:\Tools\MuseCode")).map(Some),
        "opencode" => discover_opencode_cmd().map(Some),
        "antigravity" => discover_antigravity_cmd().map(Some),
        _ => Err(core::Error::new(
            "unsupported",
            format!("unsupported launch harness {harness}"),
        )),
    }
}

fn path_search_dirs() -> Vec<PathBuf> {
    std::env::var_os("PATH").map_or_else(Vec::new, |paths| std::env::split_paths(&paths).collect())
}

pub(crate) fn find_on_path(file: &str) -> Option<PathBuf> {
    let target = file.to_ascii_lowercase();
    for dir in path_search_dirs() {
        let candidate = dir.join(file);
        if candidate.is_file()
            && candidate
                .file_name()
                .is_some_and(|name| name.to_string_lossy().to_ascii_lowercase() == target)
        {
            return Some(candidate);
        }
    }
    None
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

fn sorted_child_dirs(parent: &Path) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(entries) = std::fs::read_dir(parent) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            }
        }
    }
    dirs.sort();
    dirs
}

/// Desktop-app bundles under one roaming root:
/// `<root>/Claude/claude-code/<version>/<hash>/claude.exe`.
fn claude_bundle_exes(roaming: &Path) -> Vec<PathBuf> {
    let mut exes = Vec::new();
    for version in sorted_child_dirs(&roaming.join("Claude").join("claude-code")) {
        for bundle in sorted_child_dirs(&version) {
            exes.push(bundle.join("claude.exe"));
        }
    }
    exes
}

/// Newest-first ordering key for `<exe> --version` output: the first
/// `major.minor.patch` numeric triple on the bounded first line
/// (`2.1.325 (Claude Code)`, `1.4.20-R8619.1`).
fn parse_cli_version(text: &str) -> Option<(u64, u64, u64)> {
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

/// Every discovered `claude.exe` install, newest first: `claude.exe` hits on
/// `PATH` (stable order), the standard user locations, then the desktop-app
/// bundles. Each entry carries its own bounded `--version`.
pub(crate) fn discover_claude_candidates() -> Vec<core::api::HarnessCandidate> {
    let mut paths: Vec<PathBuf> = Vec::new();
    let mut push = |path: PathBuf| {
        if !paths.contains(&path) {
            paths.push(path);
        }
    };
    for dir in path_search_dirs() {
        let exe = dir.join("claude.exe");
        if exe.is_file() {
            push(exe);
        }
    }
    if let Some(home) = user_profile() {
        push(home.join(".local").join("bin").join("claude.exe"));
    }
    for roaming in roaming_roots() {
        push(roaming.join("npm").join("claude.exe"));
    }
    for local in local_roots() {
        push(local.join("Programs").join("claude").join("claude.exe"));
    }
    for roaming in roaming_roots() {
        for exe in claude_bundle_exes(&roaming) {
            push(exe);
        }
    }
    /// One discovered install plus its parsed `--version` triple, if any.
    type VersionedCandidate = (core::api::HarnessCandidate, Option<(u64, u64, u64)>);
    let mut candidates: Vec<VersionedCandidate> = paths
        .into_iter()
        .filter(|path| path.is_file())
        .map(|path| {
            let cmd = path.to_string_lossy().into_owned();
            let version = native_version_string(&cmd);
            (
                core::api::HarnessCandidate {
                    cmd,
                    version: version.clone(),
                },
                version.as_deref().and_then(parse_cli_version),
            )
        })
        .filter(|(candidate, _)| {
            Path::new(&candidate.cmd).is_absolute() && !is_script_wrapper(&candidate.cmd)
        })
        .collect();
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
    let mut searched = vec!["claude.exe on PATH".to_owned()];
    if let Some(home) = user_profile() {
        searched.push(
            home.join(".local")
                .join("bin")
                .join("claude.exe")
                .to_string_lossy()
                .into_owned(),
        );
    }
    for roaming in roaming_roots() {
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
    }
    searched.dedup();
    searched
}

fn discover_claude_cmd() -> core::Result<String> {
    let candidates = discover_claude_candidates();
    if let Some(best) = candidates.first() {
        return Ok(best.cmd.clone());
    }
    Err(core::Error::new(
        "profile_error",
        format!(
            "claude native executable was not found; searched {}",
            claude_search_locations().join(", ")
        ),
    ))
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

type MuseVersionedPath = (Option<(u64, u64, u64)>, String);

pub(crate) fn discover_muse_candidates(launcher_dir: &Path) -> Vec<String> {
    let mut versioned: Vec<MuseVersionedPath> = Vec::new();
    let Ok(entries) = std::fs::read_dir(launcher_dir) else {
        return Vec::new();
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.starts_with("muse-bin-") || !name.ends_with(".exe") {
            continue;
        }
        if !path.is_file() {
            continue;
        }
        let version_text = name
            .strip_prefix("muse-bin-")
            .and_then(|r| r.strip_suffix(".exe"))
            .unwrap_or_default();
        let triple = parse_muse_version_triple(version_text);
        versioned.push((triple, path.to_string_lossy().into_owned()));
    }
    versioned.sort_by(|l, r| match (&l.0, &r.0) {
        (Some(a), Some(b)) => b.cmp(a),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => l.1.cmp(&r.1),
    });
    versioned.into_iter().map(|(_, c)| c).collect()
}

fn parse_muse_version_triple(text: &str) -> Option<(u64, u64, u64)> {
    let core = text.split('-').next().unwrap_or(text);
    let mut parts = core.split('.');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(a), Some(b), Some(c), None) => match (a.parse(), b.parse(), c.parse()) {
            (Ok(a), Ok(b), Ok(c)) => Some((a, b, c)),
            _ => None,
        },
        _ => None,
    }
}

const MUSE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(8);
const MUSE_SELECTION_BUDGET: std::time::Duration = std::time::Duration::from_secs(20);
const MUSE_PROBE_FRAME_BYTES: usize = 1024 * 1024;
const MUSE_PROBE_TOTAL_BYTES: u64 = 4 * 1024 * 1024;
const MUSE_PROBE_FRAMES: usize = 100;

fn default_muse_launcher_dir() -> std::path::PathBuf {
    Path::new("C:/Tools/MuseCode").to_path_buf()
}

pub(crate) async fn probe_muse_catalogue_nonempty(cmd: &str) -> bool {
    let mut command = tokio::process::Command::new(cmd);
    command.arg("serve");
    probe_muse_command(command, MUSE_PROBE_TIMEOUT).await
}

async fn probe_muse_command(
    mut command: tokio::process::Command,
    budget: std::time::Duration,
) -> bool {
    command
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    // Assign the Job before the suspended process resumes. On cancellation,
    // OwnedChild drops the Job and Tokio kills/reaps the direct child as well.
    let Ok(mut child) = crate::platform::custody::OwnedChild::spawn(command) else {
        return false;
    };
    let probe = async { muse_probe_catalogue(child.take_stdin()?, child.take_stdout()?).await };
    let nonempty = tokio::time::timeout(budget, probe)
        .await
        .ok()
        .flatten()
        .unwrap_or(false);
    let terminated = child.terminate().is_ok();
    let reaped = tokio::time::timeout(std::time::Duration::from_secs(2), child.wait())
        .await
        .is_ok_and(|result| result.is_ok());
    nonempty && terminated && reaped
}

async fn muse_probe_catalogue(
    mut stdin: impl tokio::io::AsyncWrite + Unpin,
    stdout: impl tokio::io::AsyncRead + Unpin,
) -> Option<bool> {
    use futures_util::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::codec::{FramedRead, LinesCodec};

    // Cap bytes at the reader, line length before String/JSON allocation, and
    // frames across both initialize and model/list (including notifications).
    let mut lines = FramedRead::with_capacity(
        stdout.take(MUSE_PROBE_TOTAL_BYTES),
        LinesCodec::new_with_max_length(MUSE_PROBE_FRAME_BYTES),
        4096,
    );
    let init = json!({
        "jsonrpc": "2.0", "id": "muse-bootstrap-1", "method": "initialize",
        "params": {
            "clientInfo": {"name": "eliot", "version": env!("CARGO_PKG_VERSION"), "title": "Eliot"},
            "capabilities": {"userInputDialogs": true}
        }
    });
    let mut frame = serde_json::to_vec(&init).ok()?;
    frame.push(b'\n');
    stdin.write_all(&frame).await.ok()?;
    let mut initialized = false;
    for _ in 0..MUSE_PROBE_FRAMES {
        let line = lines.next().await?.ok()?;
        let value: Value = serde_json::from_str(&line).ok()?;
        let id = value.get("id").and_then(Value::as_str);
        if !initialized && id == Some("muse-bootstrap-1") {
            if value.get("error").is_some() || value.get("result").is_none() {
                return None;
            }
            stdin
                .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"initialized\"}\n")
                .await
                .ok()?;
            stdin.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":\"muse-bootstrap-2\",\"method\":\"model/list\",\"params\":{}}\n").await.ok()?;
            initialized = true;
        } else if initialized && id == Some("muse-bootstrap-2") {
            return Some(
                value.get("error").is_none()
                    && value
                        .get("result")
                        .and_then(|result| result.get("models"))
                        .and_then(Value::as_array)
                        .is_some_and(|models| !models.is_empty()),
            );
        }
    }
    None
}

pub(crate) async fn select_muse_hot_start_cmd() -> (Option<String>, Vec<String>, bool) {
    let candidates = discover_muse_candidates(&default_muse_launcher_dir());
    select_muse_with_probe(&candidates, MUSE_SELECTION_BUDGET, |cmd| async move {
        probe_muse_catalogue_nonempty(&cmd).await
    })
    .await
}

async fn select_muse_with_probe<F, Fut>(
    candidates: &[String],
    budget: std::time::Duration,
    mut probe: F,
) -> (Option<String>, Vec<String>, bool)
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + budget;
    let mut skipped = Vec::new();
    for cmd in candidates {
        if tokio::time::Instant::now() >= deadline {
            return (None, skipped, true);
        }
        match tokio::time::timeout_at(deadline, probe(cmd.clone())).await {
            Ok(true) => return (Some(cmd.clone()), skipped, false),
            Ok(false) => skipped.push(cmd.clone()),
            Err(_) => {
                skipped.push(cmd.clone());
                return (None, skipped, true);
            }
        }
    }
    (None, skipped, false)
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
    for local in local_roots() {
        let native = local
            .join("Programs")
            .join("Antigravity")
            .join("Antigravity.exe");
        searched.push(native.to_string_lossy().into_owned());
        if native.is_file() {
            return Ok(native.to_string_lossy().into_owned());
        }
    }
    Err(core::Error::new(
        "unsupported",
        format!(
            "antigravity native executable was not found; searched {}",
            searched.join(", ")
        ),
    ))
}

/// Read-only harness installation facts. No model turn and no inference:
/// only filesystem presence plus a bounded `--version` read.
pub(crate) fn harness_installation(harness: &str) -> Value {
    match discover_cmd_for_harness(harness) {
        Ok(Some(cmd)) => {
            let version = native_version(&cmd);
            json!({
                "harness": harness,
                "installed": true,
                "cmd": cmd,
                "version": version,
            })
        }
        Ok(None) => {
            // Codex resolves `codex` by name at spawn; report PATH presence.
            let installed = find_on_path("codex.exe")
                .or_else(|| find_on_path("codex"))
                .is_some();
            json!({
                "harness": harness,
                "installed": installed,
                "cmd": Value::Null,
                "version": if installed { native_version("codex") } else { Value::Null },
            })
        }
        Err(error) => json!({
            "harness": harness,
            "installed": false,
            "cmd": Value::Null,
            "version": Value::Null,
            "error": error.message,
            "code": error.code,
        }),
    }
}

/// Bound on one `<exe> --version` probe: a hung native executable must not
/// stall `harness.list` for every harness.
const VERSION_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Bounded `<exe> --version` first line, or `None` when the probe fails.
/// A hung native executable must not stall `harness.list`: only the child
/// this probe started is ever killed on timeout.
pub(crate) fn native_version_string(program: &str) -> Option<String> {
    use std::io::Read;
    use std::process::{Command, Stdio};
    let Ok(mut child) = Command::new(program)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
    else {
        return None;
    };
    let deadline = std::time::Instant::now() + VERSION_PROBE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            // Timed out or unwaitable: kill only the child this probe started.
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
        }
    };
    let mut stdout = String::new();
    if let Some(mut pipe) = child.stdout.take() {
        let _ = pipe.read_to_string(&mut stdout);
    }
    match status {
        Some(status) if status.success() => {
            let text = stdout.lines().next().unwrap_or_default().trim().to_owned();
            if text.is_empty() || text.len() > 256 {
                None
            } else {
                Some(text)
            }
        }
        _ => None,
    }
}

fn native_version(program: &str) -> Value {
    native_version_string(program).map_or(Value::Null, Value::String)
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
    use std::io::Read;
    use std::process::{Command, Stdio};
    let mut child = Command::new(program)
        .args(["auth", "list", "--format", "json", "--standalone"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = std::time::Instant::now() + AUTH_LIST_TIMEOUT;
    let exited = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status.success(),
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    if !exited {
        return None;
    }
    let mut stdout = Vec::new();
    let read_ok = child
        .stdout
        .take()
        .is_some_and(|mut pipe| pipe.read_to_end(&mut stdout).is_ok());
    if !read_ok || stdout.len() > MAX_AUTH_LIST_BYTES {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn adhoc_snapshot_freezes_flags_plus_safe_defaults() {
        let snapshot = adhoc_snapshot(
            "codex",
            Some("gpt-6-luna"),
            Some("medium"),
            None,
            AdhocAuth::Resolve(None),
            None,
        )
        .unwrap();
        assert_eq!(snapshot["harness"], json!("codex"));
        assert_eq!(snapshot["model"], json!("gpt-6-luna"));
        assert_eq!(snapshot["effort"], json!("medium"));
        assert_eq!(snapshot["permissions"], json!("workspace-write"));
        assert_eq!(snapshot["approval_policy"], json!("never"));
        assert_eq!(snapshot["mcp"], json!("inherit"));
        assert_eq!(snapshot["mcp_allow"], json!([]));
        assert_eq!(snapshot["child_profiles"], json!([]));
        assert_eq!(snapshot["pass_env"], json!([]));
        assert!(snapshot.get("cmd").is_none());
        // Broader authority is never frozen ad-hoc.
        assert!(snapshot.get("proxy").is_none());
        assert!(snapshot.get("account").is_none());
        assert!(snapshot.get("route_binding").is_none());
        assert!(snapshot.get("setup").is_none());
    }

    #[test]
    fn adhoc_snapshot_omits_absent_model_and_effort_for_native_default() {
        let snapshot =
            adhoc_snapshot("codex", None, None, None, AdhocAuth::Resolve(None), None).unwrap();
        assert!(snapshot.get("model").is_none());
        assert!(snapshot.get("effort").is_none());
        let snapshot = adhoc_snapshot(
            "muse",
            None,
            None,
            Some(r"C:\Tools\MuseCode\muse-bin-1.exe".to_owned()),
            AdhocAuth::Resolve(None),
            None,
        )
        .unwrap();
        assert!(snapshot.get("model").is_none());
        assert!(snapshot.get("effort").is_none());
        // Claude omits both so the CLI default applies.
        let snapshot = adhoc_snapshot(
            "claude",
            None,
            None,
            Some(r"C:\bin\claude.exe".to_owned()),
            AdhocAuth::Resolve(None),
            None,
        )
        .unwrap();
        assert!(snapshot.get("model").is_none());
        assert!(snapshot.get("effort").is_none());
    }

    #[test]
    fn adhoc_snapshot_rejects_broader_effort_mappings() {
        let error = adhoc_snapshot(
            "antigravity",
            Some("model"),
            Some("medium"),
            Some(r"C:\bin\native.exe".to_owned()),
            AdhocAuth::Resolve(None),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, "unsupported");
    }

    #[test]
    fn adhoc_opencode_freezes_effort_for_the_native_variant() {
        let snapshot = adhoc_snapshot(
            "opencode",
            Some("opencode/deepseek-v4.1-flash-free"),
            Some("low"),
            Some(r"C:\bin\opencode.exe".to_owned()),
            AdhocAuth::Resolve(Some("opencode-go")),
            None,
        )
        .unwrap();
        assert_eq!(snapshot["effort"], json!("low"));
        assert_eq!(
            snapshot["model"],
            json!("opencode/deepseek-v4.1-flash-free")
        );
        assert_eq!(snapshot["auth_integration"], json!("opencode-go"));
    }

    #[test]
    fn adhoc_claude_freezes_the_closed_effort_vocabulary() {
        for level in ["low", "medium", "high", "xhigh", "max"] {
            let snapshot = adhoc_snapshot(
                "claude",
                None,
                Some(level),
                Some(r"C:\bin\claude.exe".to_owned()),
                AdhocAuth::Resolve(None),
                None,
            )
            .unwrap();
            assert_eq!(snapshot["effort"], json!(level));
        }
        let error = adhoc_snapshot(
            "claude",
            None,
            Some("extreme"),
            Some(r"C:\bin\claude.exe".to_owned()),
            AdhocAuth::Resolve(None),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn adhoc_snapshot_rejects_unknown_harness() {
        let error =
            adhoc_snapshot("fake", None, None, None, AdhocAuth::Resolve(None), None).unwrap_err();
        assert_eq!(error.code, "unsupported");
    }

    #[test]
    fn adhoc_snapshot_requires_antigravity_model() {
        let error = adhoc_snapshot(
            "antigravity",
            None,
            None,
            Some(r"C:\bin\Antigravity.exe".to_owned()),
            AdhocAuth::Resolve(None),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn adhoc_snapshot_rejects_opencode_slashless_model() {
        let error = adhoc_snapshot(
            "opencode",
            Some("no-slash"),
            None,
            Some(r"C:\bin\opencode.exe".to_owned()),
            AdhocAuth::Resolve(None),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn adhoc_opencode_freezes_explicit_auth_verbatim() {
        // Explicit --auth needs no native read: the frozen value passes the
        // profile-side selector rule and the plugin transfer gates it.
        let snapshot = adhoc_snapshot(
            "opencode",
            Some("opencode/deepseek-v4.1-flash-free"),
            None,
            Some(r"C:\bin\opencode.exe".to_owned()),
            AdhocAuth::Resolve(Some("opencode-go")),
            None,
        )
        .unwrap();
        assert_eq!(snapshot["auth_integration"], json!("opencode-go"));
        assert_eq!(
            snapshot["model"],
            json!("opencode/deepseek-v4.1-flash-free")
        );
        for bad in ["", "  ", "-flag", "has\ncontrol"] {
            assert_eq!(
                validate_adhoc_auth("opencode", bad).unwrap_err().code,
                "profile_error",
                "{bad:?}"
            );
        }
        assert_eq!(
            validate_adhoc_auth("codex", "openrouter").unwrap_err().code,
            "unsupported"
        );
    }

    #[test]
    fn adhoc_catalog_snapshot_freezes_antigravity_cmd_without_model() {
        // The `agy models` reader needs only the frozen cmd: a catalogue
        // snapshot freezes no model, while launches still require one.
        let snapshot = adhoc_snapshot(
            "antigravity",
            None,
            None,
            Some(r"C:\bin\agy.exe".to_owned()),
            AdhocAuth::Skip,
            None,
        )
        .unwrap();
        assert_eq!(snapshot["harness"], json!("antigravity"));
        assert_eq!(snapshot["cmd"], json!(r"C:\bin\agy.exe"));
        assert!(snapshot.get("model").is_none());
    }

    #[test]
    fn adhoc_catalog_snapshot_skips_auth_without_native_read() {
        // Catalogue reads transfer no credential: no `auth_integration` key
        // is frozen and no native helper runs, even for OpenCode.
        let snapshot = adhoc_snapshot(
            "opencode",
            None,
            None,
            Some(r"C:\bin\opencode.exe".to_owned()),
            AdhocAuth::Skip,
            None,
        )
        .unwrap();
        assert!(snapshot.get("auth_integration").is_none());
        assert_eq!(snapshot["cmd"], json!(r"C:\bin\opencode.exe"));
    }

    #[test]
    fn adhoc_opencode_defaults_auth_to_provider_prefix_on_exact_id() {
        let ids = ["opencode-go".to_owned(), "openrouter".to_owned()];
        assert_eq!(
            resolve_opencode_default(Some("opencode-go/model"), &ids).unwrap(),
            "opencode-go"
        );
        // Nested model ids split at the first slash only.
        assert_eq!(
            resolve_opencode_default(Some("openrouter/stealth/crouton-alpha"), &ids).unwrap(),
            "openrouter"
        );
    }

    #[test]
    fn adhoc_opencode_refuses_auth_without_exact_id_match() {
        let ids = ["opencode-go".to_owned(), "openrouter".to_owned()];
        // The provider prefix is absent: the refusal names --auth and lists
        // the available integration ids, never picking another provider.
        let error =
            resolve_opencode_default(Some("opencode/deepseek-v4.1-flash-free"), &ids).unwrap_err();
        assert_eq!(error.code, "profile_error");
        assert!(error.message.contains("--auth"), "{}", error.message);
        assert!(error.message.contains("opencode-go"), "{}", error.message);
        assert!(error.message.contains("openrouter"), "{}", error.message);
        assert!(
            !error.message.contains("opencode/deepseek"),
            "{}",
            error.message
        );
        // An absent model cannot default either.
        let error = resolve_opencode_default(None, &ids).unwrap_err();
        assert_eq!(error.code, "profile_error");
        assert!(error.message.contains("--auth"), "{}", error.message);
        // An empty store lists (none) instead of naming a secret-free blank.
        let error = resolve_opencode_default(Some("opencode/m"), &[]).unwrap_err();
        assert!(error.message.contains("(none)"), "{}", error.message);
    }

    #[test]
    fn wrapper_paths_are_never_native_executables() {
        for wrapper in [
            r"C:\bin\claude.cmd",
            r"C:\bin\tool.bat",
            r"C:\bin\tool.ps1",
            r"C:\bin\tool.js",
        ] {
            assert!(is_script_wrapper(wrapper), "{wrapper}");
        }
        assert!(!is_script_wrapper(r"C:\bin\claude.exe"));
        assert!(!is_script_wrapper(
            r"C:\Tools\MuseCode\muse-bin-1.4.20-R8619.1.exe"
        ));
        let error = adhoc_snapshot(
            "claude",
            None,
            None,
            Some(r"C:\bin\claude.cmd".to_owned()),
            AdhocAuth::Resolve(None),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn muse_discovery_chooses_the_launcher_marked_version() {
        let dir = std::env::temp_dir().join(format!(
            "eliot-adhoc-muse-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        let _temp_1 = crate::test_support::TempDir::own(dir.clone());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(".muse-version"), "9.9.9-R1\n").unwrap();
        let native = dir.join("muse-bin-9.9.9-R1.exe");
        std::fs::write(&native, b"fake").unwrap();
        // A stale wrapper must not win over the marked native binary.
        std::fs::write(dir.join("muse.cmd"), b"@echo off").unwrap();
        let resolved = discover_muse_cmd(&dir).unwrap();
        assert_eq!(resolved, native.to_string_lossy());
    }

    #[test]
    fn muse_hot_start_discovers_newest_build_first() {
        let dir = std::env::temp_dir().join(format!(
            "eliot-adhoc-muse-catalogue-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|e| e.as_nanos())
                .unwrap_or(0)
        ));
        let _temp_2 = crate::test_support::TempDir::own(dir.clone());
        std::fs::create_dir_all(&dir).unwrap();
        let old = dir.join("muse-bin-1.4.0-R4161.1.exe");
        let new = dir.join("muse-bin-1.4.20-R8619.1.exe");
        std::fs::write(&old, b"fake").unwrap();
        std::fs::write(&new, b"fake").unwrap();
        let candidates = discover_muse_candidates(&dir);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0], new.to_string_lossy());
        assert_eq!(candidates[1], old.to_string_lossy());
    }

    #[tokio::test]
    async fn muse_selection_stops_at_first_good_build_and_has_one_budget() {
        use std::{cell::RefCell, time::Duration};
        let candidates = vec![
            "new-empty".to_owned(),
            "good".to_owned(),
            "older".to_owned(),
        ];
        let called = RefCell::new(Vec::new());
        let selection = select_muse_with_probe(&candidates, Duration::from_secs(1), |cmd| {
            called.borrow_mut().push(cmd.clone());
            async move { cmd == "good" }
        })
        .await;
        assert_eq!(
            selection,
            (Some("good".to_owned()), vec!["new-empty".to_owned()], false)
        );
        assert_eq!(*called.borrow(), candidates[..2]);

        let started = tokio::time::Instant::now();
        let called = RefCell::new(Vec::new());
        let selection = select_muse_with_probe(&candidates, Duration::from_millis(50), |cmd| {
            called.borrow_mut().push(cmd);
            async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                false
            }
        })
        .await;
        assert_eq!(selection, (None, candidates[..2].to_vec(), true));
        assert_eq!(*called.borrow(), candidates[..2]);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn muse_probe_refuses_oversize_line_before_reading_the_rest() {
        let bytes = vec![b'x'; MUSE_PROBE_FRAME_BYTES * 4];
        let mut input = bytes.as_slice();
        assert_eq!(
            muse_probe_catalogue(tokio::io::sink(), &mut input).await,
            None
        );
        let consumed = bytes.len() - input.len();
        assert!(consumed > MUSE_PROBE_FRAME_BYTES);
        // FramedRead may read one buffer beyond the threshold, but never the
        // complete oversized line, nor a String/JSON value for it.
        assert!(consumed <= MUSE_PROBE_FRAME_BYTES * 2);
    }

    #[tokio::test]
    async fn muse_probe_caps_frames_during_initialize_and_catalogue() {
        let notification = "{\"jsonrpc\":\"2.0\",\"method\":\"noise\"}\n";
        let init = "{\"id\":\"muse-bootstrap-1\",\"result\":{}}\n";
        let catalogue =
            "{\"id\":\"muse-bootstrap-2\",\"result\":{\"models\":[{\"id\":\"model\"}]}}\n";
        for prefix in ["", init] {
            let input = format!(
                "{prefix}{}{init}{catalogue}",
                notification.repeat(MUSE_PROBE_FRAMES)
            );
            assert_eq!(
                muse_probe_catalogue(tokio::io::sink(), input.as_bytes()).await,
                None
            );
        }
        let input = format!("{init}{catalogue}");
        assert_eq!(
            muse_probe_catalogue(tokio::io::sink(), input.as_bytes()).await,
            Some(true)
        );
    }

    #[tokio::test]
    async fn muse_probe_caps_total_input_bytes() {
        let line = format!(
            "{{\"method\":\"noise\",\"padding\":\"{}\"}}\n",
            "x".repeat(700_000)
        );
        let bytes = line.repeat(8);
        let mut input = bytes.as_bytes();
        assert_eq!(
            muse_probe_catalogue(tokio::io::sink(), &mut input).await,
            None
        );
        assert_eq!((bytes.len() - input.len()) as u64, MUSE_PROBE_TOTAL_BYTES);
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn muse_probe_timeout_and_cancellation_leave_no_child() {
        use crate::platform::windows::{process_start_time, recorded_process_alive};
        use std::time::Duration;
        for cancel in [false, true] {
            let pid_file =
                std::env::temp_dir().join(format!("eliot-muse-probe-{}.pid", ulid::Ulid::new()));
            // A real waiting OS process, not a native harness/model fixture.
            let mut command = tokio::process::Command::new("powershell.exe");
            command.args(["-NoProfile", "-NonInteractive", "-Command",
                "[IO.File]::WriteAllText($env:ELIOT_MUSE_PROBE_WAIT_PID, [string]$PID); Start-Sleep -Seconds 60"])
                .env("ELIOT_MUSE_PROBE_WAIT_PID", &pid_file);
            // PowerShell can take several seconds to start under a parallel
            // test load; the probe bound must outlast that start, and the
            // PID wait must cover it, or the assertion races process start.
            let task = tokio::spawn(probe_muse_command(command, Duration::from_secs(12)));
            let pid = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if let Ok(text) = std::fs::read_to_string(&pid_file)
                        && let Ok(pid) = text.parse::<u32>()
                    {
                        break pid;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("probe process starts");
            let birth = process_start_time(pid);
            assert!(recorded_process_alive(pid, birth).unwrap());
            if cancel {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                assert!(!task.await.unwrap());
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                while recorded_process_alive(pid, birth).unwrap() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("probe child must be gone, including on cancellation");
            std::fs::remove_file(&pid_file).unwrap();
        }
    }

    #[test]
    fn muse_discovery_without_a_marked_version_fails_honestly() {
        let dir = std::env::temp_dir().join(format!(
            "eliot-adhoc-muse-missing-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        let _temp_3 = crate::test_support::TempDir::own(dir.clone());
        std::fs::create_dir_all(&dir).unwrap();
        let error = discover_muse_cmd(&dir).unwrap_err();
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn adhoc_snapshot_freezes_an_explicit_cmd_verbatim() {
        // The `--cmd` override pins the native binary (e.g. the Muse 1.4.0
        // binary the launcher no longer marks current); discovery is
        // bypassed and the path is frozen exactly as given.
        let pinned = r"C:\Tools\MuseCode\muse-bin-1.4.0-R4161.1.exe";
        let snapshot = adhoc_snapshot(
            "muse",
            None,
            None,
            Some(pinned.to_owned()),
            AdhocAuth::Resolve(None),
            None,
        )
        .unwrap();
        assert_eq!(snapshot["cmd"], json!(pinned));
        let error = adhoc_snapshot(
            "muse",
            None,
            None,
            Some(r"C:\Tools\MuseCode\muse.cmd".to_owned()),
            AdhocAuth::Resolve(None),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code, "profile_error");
    }

    #[test]
    fn data_roots_fall_back_to_the_profile_derived_default() {
        use std::ffi::OsString;
        // A stripped daemon environment keeps no APPDATA, yet the installs
        // still exist under the profile: the derived root must be consulted.
        let roots = data_roots(None, Some(OsString::from(r"C:\Users\qa")), "Roaming");
        assert_eq!(roots, vec![PathBuf::from(r"C:\Users\qa\AppData\Roaming")]);
        let roots = data_roots(
            Some(OsString::from(r"D:\Custom\AppData\Roaming")),
            Some(OsString::from(r"C:\Users\qa")),
            "Roaming",
        );
        assert_eq!(
            roots,
            vec![
                PathBuf::from(r"D:\Custom\AppData\Roaming"),
                PathBuf::from(r"C:\Users\qa\AppData\Roaming"),
            ]
        );
        // No double entry when the variable already names the default.
        let roots = data_roots(
            Some(OsString::from(r"C:\Users\qa\AppData\Roaming")),
            Some(OsString::from(r"C:\Users\qa")),
            "Roaming",
        );
        assert_eq!(roots, vec![PathBuf::from(r"C:\Users\qa\AppData\Roaming")]);
    }

    #[test]
    fn opencode_candidates_follow_the_shim_target_first() {
        let shim_dir = PathBuf::from(r"C:\pkg\npm");
        let roaming = PathBuf::from(r"C:\Users\qa\AppData\Roaming");
        let candidates = opencode_candidates(
            std::slice::from_ref(&shim_dir),
            std::slice::from_ref(&roaming),
        );
        // The shim target is independent of the deep arch layout: the shim
        // at `<shim-dir>/opencode.cmd` delegates to
        // `<shim-dir>/node_modules/@opencode/cli/bin/opencode.exe`.
        assert_eq!(
            candidates.first().expect("shim target first"),
            &shim_dir
                .join("node_modules")
                .join("@opencode")
                .join("cli")
                .join("bin")
                .join("opencode.exe")
        );
        // The known package-local paths follow under every roaming root.
        assert!(
            candidates.contains(
                &roaming
                    .join("npm")
                    .join("node_modules")
                    .join("@opencode")
                    .join("cli")
                    .join("node_modules")
                    .join("@opencode")
                    .join("cli-windows-x64")
                    .join("bin")
                    .join("opencode.exe")
            )
        );
        assert!(
            candidates.contains(
                &roaming
                    .join("npm")
                    .join("node_modules")
                    .join("@opencode")
                    .join("cli")
                    .join("node_modules")
                    .join("@opencode")
                    .join("cli-windows-x64-baseline")
                    .join("bin")
                    .join("opencode.exe")
            )
        );
    }

    #[test]
    fn parse_cli_version_reads_the_leading_triple() {
        assert_eq!(
            parse_cli_version("2.1.325 (Claude Code)"),
            Some((2, 1, 325))
        );
        assert_eq!(
            parse_cli_version("Muse Code 1.4.20 (1.4.20-R8619.1)"),
            Some((1, 4, 20))
        );
        assert_eq!(parse_cli_version("not a version"), None);
        assert_eq!(parse_cli_version(""), None);
    }

    #[test]
    fn access_mapping_freezes_each_harness_snapshot() {
        // Codex maps ask/auto/full to approval_policy plus sandbox.
        let ask = adhoc_snapshot(
            "codex",
            Some("gpt-6-luna"),
            Some("low"),
            None,
            AdhocAuth::Resolve(None),
            Some("ask"),
        )
        .unwrap();
        assert_eq!(ask["access"], json!("ask"));
        assert_eq!(ask["permissions"], json!("workspace-write"));
        assert_eq!(ask["approval_policy"], json!("on-request"));
        let auto = adhoc_snapshot(
            "codex",
            Some("gpt-6-luna"),
            Some("low"),
            None,
            AdhocAuth::Resolve(None),
            Some("auto"),
        )
        .unwrap();
        assert_eq!(auto["permissions"], json!("workspace-write"));
        assert_eq!(auto["approval_policy"], json!("never"));
        let full = adhoc_snapshot(
            "codex",
            Some("gpt-6-luna"),
            Some("low"),
            None,
            AdhocAuth::Resolve(None),
            Some("full"),
        )
        .unwrap();
        assert_eq!(full["permissions"], json!("danger-full-access"));
        assert_eq!(full["approval_policy"], json!("never"));
        // Claude maps to permission_mode.
        let ask = adhoc_snapshot(
            "claude",
            Some("claude-haiku-5-5"),
            None,
            Some(r"C:\Tools\claude.exe".to_owned()),
            AdhocAuth::Resolve(None),
            Some("ask"),
        )
        .unwrap();
        assert_eq!(ask["permission_mode"], json!("manual"));
        let full = adhoc_snapshot(
            "claude",
            Some("claude-haiku-5-5"),
            None,
            Some(r"C:\Tools\claude.exe".to_owned()),
            AdhocAuth::Resolve(None),
            Some("full"),
        )
        .unwrap();
        assert_eq!(full["permission_mode"], json!("bypassPermissions"));
        // Muse maps to approval plus permissions.
        let ask = adhoc_snapshot(
            "muse",
            Some("muse-spark-1.3"),
            None,
            Some(r"C:\Tools\muse.exe".to_owned()),
            AdhocAuth::Resolve(None),
            Some("ask"),
        )
        .unwrap();
        assert_eq!(ask["approval"], json!("onRequest"));
        assert_eq!(ask["permissions"], json!("workspace-write"));
        let full = adhoc_snapshot(
            "muse",
            Some("muse-spark-1.3"),
            None,
            Some(r"C:\Tools\muse.exe".to_owned()),
            AdhocAuth::Resolve(None),
            Some("full"),
        )
        .unwrap();
        assert_eq!(full["approval"], json!("allowAll"));
        assert_eq!(full["permissions"], json!("danger-full-access"));
        // Antigravity maps ask/auto to the ACP permission path and full to skip.
        let ask = adhoc_snapshot(
            "antigravity",
            Some("gemini-4-hydrogen-low"),
            None,
            Some(r"C:\Tools\agy.exe".to_owned()),
            AdhocAuth::Resolve(None),
            Some("ask"),
        )
        .unwrap();
        assert_eq!(ask["dangerously_skip_permissions"], json!(false));
        let full = adhoc_snapshot(
            "antigravity",
            Some("gemini-4-hydrogen-low"),
            None,
            Some(r"C:\Tools\agy.exe".to_owned()),
            AdhocAuth::Resolve(None),
            Some("full"),
        )
        .unwrap();
        assert_eq!(full["dangerously_skip_permissions"], json!(true));
        // OpenCode ask/auto share the permission-reply surface.
        let auto = adhoc_snapshot(
            "opencode",
            Some("opencode/deepseek-v4.1-flash-free"),
            None,
            Some(r"C:\Tools\opencode.exe".to_owned()),
            AdhocAuth::Resolve(Some("opencode-go")),
            Some("auto"),
        )
        .unwrap();
        assert_eq!(auto["access"], json!("auto"));
    }

    #[test]
    fn access_refuses_unsupported_levels_without_silent_weakening() {
        let error = adhoc_snapshot(
            "opencode",
            Some("opencode/deepseek-v4.1-flash-free"),
            None,
            Some(r"C:\Tools\opencode.exe".to_owned()),
            AdhocAuth::Resolve(Some("opencode-go")),
            Some("full"),
        )
        .unwrap_err();
        assert_eq!(error.code, "unsupported");
        let error = adhoc_snapshot(
            "codex",
            Some("gpt-6-luna"),
            None,
            None,
            AdhocAuth::Resolve(None),
            Some("sometimes"),
        )
        .unwrap_err();
        assert_eq!(error.code, "invalid");
    }

    #[test]
    fn claude_bundle_exes_enumerate_the_versioned_layout() {
        let dir = std::env::temp_dir().join(format!(
            "eliot-adhoc-claude-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        let _temp_4 = crate::test_support::TempDir::own(dir.clone());
        let first = dir
            .join("Claude")
            .join("claude-code")
            .join("2.1.321")
            .join("d020ce1b5d30");
        let second = dir
            .join("Claude")
            .join("claude-code")
            .join("2.1.325")
            .join("806e0a45e6f5");
        std::fs::create_dir_all(&first).unwrap();
        std::fs::create_dir_all(&second).unwrap();
        let exes = claude_bundle_exes(&dir);
        assert_eq!(
            exes,
            vec![first.join("claude.exe"), second.join("claude.exe"),]
        );
    }
}
