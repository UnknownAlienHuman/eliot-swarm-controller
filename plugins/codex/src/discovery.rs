//! Probe store selection and verify readers or controlled startup against its pin.
//! This module never creates a controlled thread or publishes lifecycle facts.

use crate::{
    host::{contextual_error, native_scope},
    native::{
        NativeClient, NativeFrame, NativeLaunch, NativeProcess,
        startup_retry::{
            StartupFailureEvidence, exhausted_start_failure, native_start_failed, next_retry_count,
            randomized_retry_delay,
        },
        store_policy::ensure_store_pin_supported,
        types::{
            NativeClientInfo, NativeDiscoveryConfigReadResponse, NativeDiscoverySourceKind,
            NativeDiscoveryStatus, NativeDiscoveryThread, NativeInitializeParams,
            NativeInitializeResponse, NativeModelEntry, NativeModelListParams,
            NativeModelListResponse, NativeThreadListParams, NativeThreadListResponse,
        },
    },
};
use eliot_core::{
    Error, NativeStoreBinding, Result, SecretPolicy,
    api::{
        DiscoverCoverage, DiscoverPage, DiscoverSubject, MAX_DISCOVER_PAGE_BYTES,
        MAX_DISCOVER_PAGE_ENTRIES,
    },
    canonical_json_sha256,
};
use eliot_plugin::{DiscoverParams, MAX_EVENT_BYTES};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    ffi::OsString,
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep_until, timeout_at},
};

pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(25);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const STARTUP_EXIT_CONFIRMATION: Duration = Duration::from_millis(500);
const MAX_PROFILE_BYTES: usize = 128 * 1024;
const MAX_CURSOR_BYTES: usize = 16 * 1024;
const MAX_NATIVE_CURSOR_BYTES: usize = 8 * 1024;
const MAX_REFERENCE_BYTES: usize = 256;
const MAX_TEXT_BYTES: usize = 4096;
const CURSOR_PREFIX: &str = "codex-discover-v1:";

const SOURCE_KINDS: [NativeDiscoverySourceKind; 10] = [
    NativeDiscoverySourceKind::Cli,
    NativeDiscoverySourceKind::Vscode,
    NativeDiscoverySourceKind::Exec,
    NativeDiscoverySourceKind::AppServer,
    NativeDiscoverySourceKind::SubAgent,
    NativeDiscoverySourceKind::SubAgentReview,
    NativeDiscoverySourceKind::SubAgentCompact,
    NativeDiscoverySourceKind::SubAgentThreadSpawn,
    NativeDiscoverySourceKind::SubAgentOther,
    NativeDiscoverySourceKind::Unknown,
];

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    native_scope: String,
    binding: String,
    store_binding: String,
    native_cursor: String,
}

#[derive(Serialize)]
struct StoreSelection {
    configured: Option<String>,
    origin: Option<Value>,
    environment: Option<String>,
    root: PathBuf,
}

pub(crate) struct StoreContext {
    scope: String,
    binding: String,
    home: PathBuf,
    root: PathBuf,
    pin_verified: bool,
}

impl StoreContext {
    pub(crate) fn scope(&self) -> &str {
        &self.scope
    }

    pub(crate) fn binding(&self) -> &str {
        &self.binding
    }

    pub(crate) fn home(&self) -> &Path {
        &self.home
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    fn native_binding(&self, metadata_secrets: &[String]) -> Result<NativeStoreBinding> {
        let binding = NativeStoreBinding {
            home: self.home.to_str().ok_or_else(unverified_store)?.to_owned(),
            root: self.root.to_str().ok_or_else(unverified_store)?.to_owned(),
            native_scope: self.scope.clone(),
        };
        reject_secrets(
            &serde_json::to_value(&binding).map_err(|_| unverified_store())?,
            metadata_secrets,
        )?;
        Ok(binding)
    }
}

/// The host supplies the prepared workspace in launch.cwd. Capture broad diagnostic
/// secrets independently from the credential-only metadata checks.
pub(crate) async fn profile_store(
    launch: &NativeLaunch,
    environment: &Value,
    metadata_secrets: &[String],
    diagnostic_secrets: &[String],
    deadline: Instant,
) -> Result<NativeStoreBinding> {
    probe_store(
        launch,
        environment,
        metadata_secrets,
        diagnostic_secrets,
        deadline,
    )
    .await?
    .native_binding(metadata_secrets)
}

/// Worker-visible native MCP surface for a minimal policy: the effective
/// `mcp_servers` entries plus the effective `plugins` states, both read
/// from one short-lived metadata reader (initialize, one `config/read`
/// with the worker's cwd, close; it never starts a thread). The worker's
/// per-server transport-carrying `enabled=false` overrides and per-plugin
/// `enabled=false` overrides are computed from this surface before the
/// long-lived process is born. The reader is pinned to the frozen
/// native-store binding the same way the worker launch is pinned, and the
/// pin is verified before the enumeration `config/read` is trusted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveMcpSurface {
    pub(crate) servers: Vec<crate::mcp::EffectiveMcpServer>,
    pub(crate) plugins: Vec<crate::mcp::EffectiveMcpPlugin>,
}

/// Enumerate the worker-visible native MCP surface (servers plus plugin
/// states) with one short-lived metadata reader.
pub(crate) async fn read_effective_mcp_surface(
    launch: &NativeLaunch,
    binding: &NativeStoreBinding,
    metadata_secrets: &[String],
    diagnostic_secrets: &[String],
    deadline: Instant,
) -> Result<EffectiveMcpSurface> {
    let cwd = launch.cwd.clone();
    let pinned = pinned_launch_for_binding(launch, binding)?;
    let (mut probe, drainer, initialized) =
        start_initialized_reader(pinned, metadata_secrets, diagnostic_secrets, deadline).await?;
    let probe_result: Result<EffectiveMcpSurface> = async {
        let client = probe.client();
        verify_initialized_store(
            &client,
            &initialized,
            binding,
            &cwd,
            metadata_secrets,
            deadline,
        )
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "native_scope_unverified",
                "Codex MCP server enumeration reader is outside the frozen native store",
            )
        })?;
        let config = client
            .call(
                "config/read",
                json!({"cwd": launch.cwd, "includeLayers": false}),
                deadline,
            )
            .await
            .map_err(|error| {
                contextual_error(
                    error,
                    "native_readback",
                    "Codex MCP server enumeration failed before worker spawn",
                )
            })?;
        let entries = mcp_server_entries(&config)?;
        let plugins = mcp_plugin_states(&config)?;
        reject_secrets(
            &serde_json::to_value(
                entries
                    .iter()
                    .map(|entry| &entry.name)
                    .chain(plugins.iter().map(|plugin| &plugin.id))
                    .collect::<Vec<_>>(),
            )
            .map_err(|_| invalid_page())?,
            metadata_secrets,
        )?;
        Ok(EffectiveMcpSurface {
            servers: entries,
            plugins,
        })
    }
    .await;
    finish_reader(&mut probe, Some(drainer)).await?;
    let probe_result = probe_result.map_err(|error| probe.client().diagnostic_error(error));
    drop(probe);
    probe_result
}

/// Read-only native model catalogue: a short-lived metadata reader runs
/// initialize, pages `model/list`, then closes. It never starts a thread,
/// sends input, or performs inference.
pub(crate) async fn list_models(
    launch: NativeLaunch,
    metadata_secrets: &[String],
    diagnostic_secrets: &[String],
    deadline: Instant,
) -> Result<Vec<NativeModelEntry>> {
    let (mut probe, drainer, _initialized) =
        start_initialized_reader(launch, metadata_secrets, diagnostic_secrets, deadline).await?;
    let probe_result: Result<Vec<NativeModelEntry>> = async {
        let client = probe.client();
        let mut entries: Vec<NativeModelEntry> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut seen_cursors = HashSet::new();
        for _ in 0..100usize {
            let response = client
                .call(
                    "model/list",
                    serde_json::to_value(NativeModelListParams {
                        cursor: cursor.clone(),
                        include_hidden: Some(true),
                        limit: Some(100),
                    })
                    .map_err(|_| invalid_page())?,
                    deadline,
                )
                .await
                .map_err(|error| {
                    contextual_error(
                        error,
                        "native_readback",
                        "Codex model/list did not return a catalogue page",
                    )
                })?;
            let page: NativeModelListResponse =
                serde_json::from_value(response).map_err(|_| invalid_page())?;
            entries.extend(page.data);
            match page.next_cursor {
                Some(next) if !next.is_empty() && seen_cursors.insert(next.clone()) => {
                    cursor = Some(next);
                }
                Some(_) => {
                    return Err(Error::new(
                        "native_readback",
                        "Codex model/list returned a repeated or invalid cursor",
                    ));
                }
                None => break,
            }
        }
        let value = serde_json::to_value(&entries).map_err(|_| invalid_page())?;
        reject_secrets(&value, metadata_secrets)?;
        Ok(entries)
    }
    .await;
    finish_reader(&mut probe, Some(drainer)).await?;
    let probe_result = probe_result.map_err(|error| probe.client().diagnostic_error(error));
    drop(probe);
    probe_result
}

/// Read-only native account quota: a short-lived metadata reader calls
/// `account/rateLimits/read`, plus `account/usage/read` when useful, then
/// closes. It never starts a thread, sends input, or performs inference.
/// Only quota signals are projected; account identity (including any email
/// from `account/read`, which is never called) is omitted.
pub(crate) async fn read_quota(
    launch: NativeLaunch,
    metadata_secrets: &[String],
    diagnostic_secrets: &[String],
    deadline: Instant,
) -> Result<Value> {
    let (mut probe, drainer, _initialized) =
        start_initialized_reader(launch, metadata_secrets, diagnostic_secrets, deadline).await?;
    let probe_result: Result<Value> = async {
        let client = probe.client();
        let rates = client
            .call("account/rateLimits/read", json!({}), deadline)
            .await
            .map_err(|error| {
                contextual_error(
                    error,
                    "native_readback",
                    "Codex account/rateLimits/read did not return quota",
                )
            })?;
        reject_secrets(&rates, metadata_secrets)?;
        let mut details = project_rate_limits(&rates)?;
        // Best-effort enrichment: token activity when the native surface
        // offers it. A missing method or malformed payload never fails the
        // primary rate-limit readback.
        if let Ok(usage) = client.call("account/usage/read", json!({}), deadline).await
            && reject_secrets(&usage, metadata_secrets).is_ok()
            && let Some(token_usage) = project_token_usage(&usage)
            && let Value::Object(map) = &mut details
        {
            map.insert("tokenUsage".to_owned(), token_usage);
        }
        let value = serde_json::to_value(&details).map_err(|_| invalid_page())?;
        reject_secrets(&value, metadata_secrets)?;
        if serde_json::to_vec(&value)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX)
            > 8192
        {
            return Err(Error::new(
                "native_readback",
                "Codex account quota readback exceeds 8192 encoded bytes",
            ));
        }
        Ok(details)
    }
    .await;
    finish_reader(&mut probe, Some(drainer)).await?;
    let probe_result = probe_result.map_err(|error| probe.client().diagnostic_error(error));
    drop(probe);
    probe_result
}

/// Project only quota signals from `account/rateLimits/read`. Account
/// identifiers and any identity fields are dropped; only windows, used
/// percent, reset time, credits and plan survive.
fn project_rate_limits(raw: &Value) -> Result<Value> {
    let raw = raw.as_object().ok_or_else(invalid_page)?;
    let Some(rate_limits) = raw.get("rateLimits") else {
        return Err(invalid_page());
    };
    if !rate_limits.is_object() {
        return Err(invalid_page());
    }
    let mut details = serde_json::Map::new();
    details.insert(
        "rateLimits".to_owned(),
        project_rate_limit_snapshot(rate_limits)?,
    );
    if let Some(by_limit) = raw.get("rateLimitsByLimitId") {
        match by_limit {
            Value::Null => {}
            Value::Object(map) => {
                if map.len() > 32 {
                    return Err(invalid_page());
                }
                let mut projected = serde_json::Map::new();
                for (key, snapshot) in map {
                    check_text(key, MAX_REFERENCE_BYTES, true).map_err(|_| invalid_page())?;
                    if !snapshot.is_object() {
                        return Err(invalid_page());
                    }
                    projected.insert(key.clone(), project_rate_limit_snapshot(snapshot)?);
                }
                details.insert("rateLimitsByLimitId".to_owned(), Value::Object(projected));
            }
            _ => return Err(invalid_page()),
        }
    }
    if let Some(allowed) = raw.get("ordinaryUsageAllowed") {
        match allowed {
            Value::Null | Value::Bool(_) => {
                details.insert("ordinaryUsageAllowed".to_owned(), allowed.clone());
            }
            _ => return Err(invalid_page()),
        }
    }
    if let Some(credits) = raw.get("rateLimitResetCredits") {
        match credits {
            Value::Null => {
                details.insert("rateLimitResetCredits".to_owned(), Value::Null);
            }
            Value::Object(_) => {
                details.insert(
                    "rateLimitResetCredits".to_owned(),
                    project_reset_credits(credits)?,
                );
            }
            _ => return Err(invalid_page()),
        }
    }
    Ok(Value::Object(details))
}

fn project_rate_limit_snapshot(snapshot: &Value) -> Result<Value> {
    let snapshot = snapshot.as_object().ok_or_else(invalid_page)?;
    let mut out = serde_json::Map::new();
    for key in ["primary", "secondary"] {
        match snapshot.get(key) {
            None | Some(Value::Null) => {}
            Some(window) => {
                out.insert(key.to_owned(), project_rate_window(window)?);
            }
        }
    }
    match snapshot.get("planType") {
        None | Some(Value::Null) => {}
        Some(Value::String(plan)) => {
            check_text(plan, 64, true).map_err(|_| invalid_page())?;
            out.insert("planType".to_owned(), Value::String(plan.clone()));
        }
        Some(_) => return Err(invalid_page()),
    }
    for key in ["limitId", "limitName"] {
        match snapshot.get(key) {
            None | Some(Value::Null) => {}
            Some(Value::String(name)) => {
                check_text(name, 128, true).map_err(|_| invalid_page())?;
                out.insert(key.to_owned(), Value::String(name.clone()));
            }
            Some(_) => return Err(invalid_page()),
        }
    }
    match snapshot.get("credits") {
        None | Some(Value::Null) => {}
        Some(credits) => {
            out.insert("credits".to_owned(), project_credits(credits)?);
        }
    }
    Ok(Value::Object(out))
}

fn project_rate_window(window: &Value) -> Result<Value> {
    let window = window.as_object().ok_or_else(invalid_page)?;
    let used = window
        .get("usedPercent")
        .and_then(Value::as_i64)
        .ok_or_else(invalid_page)?;
    if !(0..=1000).contains(&used) {
        return Err(invalid_page());
    }
    let mut out = serde_json::Map::new();
    out.insert("usedPercent".to_owned(), Value::from(used));
    match window.get("windowDurationMins") {
        None | Some(Value::Null) => {}
        Some(value) => {
            let mins = value.as_i64().ok_or_else(invalid_page)?;
            if mins <= 0 || mins > 1_000_000 {
                return Err(invalid_page());
            }
            out.insert("windowDurationMins".to_owned(), Value::from(mins));
        }
    }
    match window.get("resetsAt") {
        None | Some(Value::Null) => {}
        Some(value) => {
            let resets = value.as_i64().ok_or_else(invalid_page)?;
            if resets < 0 {
                return Err(invalid_page());
            }
            out.insert("resetsAt".to_owned(), Value::from(resets));
        }
    }
    Ok(Value::Object(out))
}

fn project_credits(credits: &Value) -> Result<Value> {
    let credits = credits.as_object().ok_or_else(invalid_page)?;
    let mut out = serde_json::Map::new();
    for key in ["hasCredits", "unlimited"] {
        let flag = credits
            .get(key)
            .and_then(Value::as_bool)
            .ok_or_else(invalid_page)?;
        out.insert(key.to_owned(), Value::from(flag));
    }
    match credits.get("balance") {
        None | Some(Value::Null) => {}
        Some(Value::String(balance)) => {
            check_text(balance, 64, true).map_err(|_| invalid_page())?;
            out.insert("balance".to_owned(), Value::String(balance.clone()));
        }
        Some(_) => return Err(invalid_page()),
    }
    Ok(Value::Object(out))
}

fn project_reset_credits(credits: &Value) -> Result<Value> {
    let credits = credits.as_object().ok_or_else(invalid_page)?;
    let available = credits
        .get("availableCount")
        .and_then(Value::as_i64)
        .ok_or_else(invalid_page)?;
    if !(0..=1_000_000).contains(&available) {
        return Err(invalid_page());
    }
    let mut out = serde_json::Map::new();
    out.insert("availableCount".to_owned(), Value::from(available));
    match credits.get("credits") {
        None | Some(Value::Null) => {}
        Some(Value::Array(rows)) => {
            if rows.len() > 32 {
                return Err(invalid_page());
            }
            let mut projected_rows = Vec::with_capacity(rows.len());
            for row in rows {
                projected_rows.push(project_reset_credit(row)?);
            }
            out.insert("credits".to_owned(), Value::Array(projected_rows));
        }
        Some(_) => return Err(invalid_page()),
    }
    Ok(Value::Object(out))
}

fn project_reset_credit(row: &Value) -> Result<Value> {
    let row = row.as_object().ok_or_else(invalid_page)?;
    let mut out = serde_json::Map::new();
    for key in ["status", "resetType"] {
        match row.get(key) {
            Some(Value::String(value)) => {
                check_text(value, 64, true).map_err(|_| invalid_page())?;
                out.insert(key.to_owned(), Value::String(value.clone()));
            }
            _ => return Err(invalid_page()),
        }
    }
    Ok(Value::Object(out))
}

fn project_token_usage(raw: &Value) -> Option<Value> {
    let raw = raw.as_object()?;
    let summary = raw.get("summary")?.as_object()?;
    let mut out = serde_json::Map::new();
    for key in [
        "lifetimeTokens",
        "currentStreakDays",
        "longestStreakDays",
        "peakDailyTokens",
        "longestRunningTurnSec",
    ] {
        match summary.get(key) {
            None | Some(Value::Null) => {}
            Some(value) => {
                let number = value.as_i64()?;
                if number < 0 {
                    return None;
                }
                out.insert(key.to_owned(), Value::from(number));
            }
        }
    }
    match raw.get("dailyUsageBuckets") {
        None | Some(Value::Null) => {}
        Some(Value::Array(buckets)) => {
            if buckets.len() > 31 {
                return None;
            }
            let mut projected = Vec::with_capacity(buckets.len());
            for bucket in buckets {
                let bucket = bucket.as_object()?;
                let date = bucket.get("startDate")?.as_str()?;
                if date.len() > 32 || date.trim().is_empty() {
                    return None;
                }
                let tokens = bucket.get("tokens")?.as_i64()?;
                if tokens < 0 {
                    return None;
                }
                projected.push(json!({"startDate": date, "tokens": tokens}));
            }
            out.insert("dailyUsageBuckets".to_owned(), Value::Array(projected));
        }
        Some(_) => return None,
    }
    Some(Value::Object(out))
}

/// Project the effective `mcp_servers` entries: each table key plus the
/// `command`/`url` transport the native `-c` bootstrap validates, so the
/// minimal spawn can re-emit the transport alongside `enabled=false`.
/// Entry tables that are not objects, or carry a non-string `command`/`url`,
/// fail the enumeration instead of projecting a partial transport.
fn mcp_server_entries(config_response: &Value) -> Result<Vec<crate::mcp::EffectiveMcpServer>> {
    const MAX_ENUMERATED_SERVERS: usize = 1024;
    const MAX_ENUMERATED_NAME_BYTES: usize = 256;
    let config = config_response
        .get("config")
        .filter(|config| config.is_object())
        .ok_or_else(|| {
            Error::new(
                "native_readback",
                "Codex MCP server enumeration failed before worker spawn",
            )
        })?;
    match config.get("mcp_servers") {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Object(servers)) => {
            if servers.len() > MAX_ENUMERATED_SERVERS {
                return Err(too_large());
            }
            let mut entries = Vec::with_capacity(servers.len());
            for (name, entry) in servers {
                check_text(name, MAX_ENUMERATED_NAME_BYTES, true).map_err(|_| {
                    Error::new(
                        "native_readback",
                        "Codex MCP server enumeration failed before worker spawn",
                    )
                })?;
                let table = entry.as_object().ok_or_else(|| {
                    Error::new(
                        "native_readback",
                        "Codex MCP server enumeration failed before worker spawn",
                    )
                })?;
                for key in ["command", "url"] {
                    if table
                        .get(key)
                        .is_some_and(|value| !value.is_null() && !value.is_string())
                    {
                        return Err(Error::new(
                            "native_readback",
                            "Codex MCP server enumeration failed before worker spawn",
                        ));
                    }
                }
                entries.push(crate::mcp::EffectiveMcpServer {
                    name: name.clone(),
                    command: table
                        .get("command")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    url: table.get("url").and_then(Value::as_str).map(str::to_owned),
                });
            }
            entries.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(entries)
        }
        Some(_) => Err(Error::new(
            "native_readback",
            "Codex MCP server enumeration failed before worker spawn",
        )),
    }
}

/// Project the effective `plugins` states: each table id plus whether it
/// is enabled, so the minimal spawn can suppress plugin-backed MCP servers
/// (which never appear in `mcp_servers`) with their own `enabled=false`
/// overrides. Absent means no controllable plugins; entry tables that are
/// not objects, or carry a non-boolean `enabled`, fail the enumeration
/// instead of projecting a partial surface. A missing `enabled` flag means
/// enabled: the installed schema defaults plugin entries to enabled.
fn mcp_plugin_states(config_response: &Value) -> Result<Vec<crate::mcp::EffectiveMcpPlugin>> {
    const MAX_ENUMERATED_PLUGINS: usize = 1024;
    const MAX_ENUMERATED_PLUGIN_ID_BYTES: usize = 256;
    let config = config_response
        .get("config")
        .filter(|config| config.is_object())
        .ok_or_else(|| {
            Error::new(
                "native_readback",
                "Codex MCP plugin enumeration failed before worker spawn",
            )
        })?;
    match config.get("plugins") {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Object(plugins)) => {
            if plugins.len() > MAX_ENUMERATED_PLUGINS {
                return Err(too_large());
            }
            let mut states = Vec::with_capacity(plugins.len());
            for (id, entry) in plugins {
                check_text(id, MAX_ENUMERATED_PLUGIN_ID_BYTES, true).map_err(|_| {
                    Error::new(
                        "native_readback",
                        "Codex MCP plugin enumeration failed before worker spawn",
                    )
                })?;
                let table = entry.as_object().ok_or_else(|| {
                    Error::new(
                        "native_readback",
                        "Codex MCP plugin enumeration failed before worker spawn",
                    )
                })?;
                let enabled = match table.get("enabled") {
                    None | Some(Value::Null) => true,
                    Some(Value::Bool(enabled)) => *enabled,
                    Some(_) => {
                        return Err(Error::new(
                            "native_readback",
                            "Codex MCP plugin enumeration failed before worker spawn",
                        ));
                    }
                };
                states.push(crate::mcp::EffectiveMcpPlugin {
                    id: id.clone(),
                    enabled,
                });
            }
            states.sort_by(|left, right| left.id.cmp(&right.id));
            Ok(states)
        }
        Some(_) => Err(Error::new(
            "native_readback",
            "Codex MCP plugin enumeration failed before worker spawn",
        )),
    }
}

pub(crate) fn pinned_launch_for_binding(
    launch: &NativeLaunch,
    binding: &NativeStoreBinding,
) -> Result<NativeLaunch> {
    let (_, root) = binding_paths(binding)?;
    let mut pinned = pinned_launch(launch, root)?;
    pinned.environment.retain(|(name, _)| {
        let name = name.to_string_lossy();
        if cfg!(windows) {
            !name.eq_ignore_ascii_case("CODEX_HOME")
        } else {
            name != "CODEX_HOME"
        }
    });
    pinned
        .environment
        .push((OsString::from("CODEX_HOME"), OsString::from(&binding.home)));
    Ok(pinned)
}

/// Verify on the controlled process after its single initialize/initialized pair.
/// The caller retains the normal event reader; no discard drainer is installed.
pub(crate) async fn verify_initialized_store(
    client: &NativeClient,
    initialized: &NativeInitializeResponse,
    binding: &NativeStoreBinding,
    cwd: &Path,
    metadata_secrets: &[String],
    deadline: Instant,
) -> Result<()> {
    let (home, root) = binding_paths(binding)?;
    reject_secrets(
        &serde_json::to_value(binding).map_err(|_| unverified_store())?,
        metadata_secrets,
    )?;
    let observed = read_store(client, &json!({}), &initialized.codex_home, cwd, deadline).await?;
    validate_pin(home, root, &observed)
}

fn binding_paths(binding: &NativeStoreBinding) -> Result<(&Path, &Path)> {
    for path in [&binding.home, &binding.root] {
        check_text(path, MAX_TEXT_BYTES, true).map_err(|_| unverified_store())?;
    }
    check_text(&binding.native_scope, MAX_REFERENCE_BYTES, true).map_err(|_| unverified_store())?;
    let home = Path::new(&binding.home);
    let root = Path::new(&binding.root);
    if !home.is_absolute() || !root.is_absolute() {
        return Err(unverified_store());
    }
    Ok((home, root))
}

pub(crate) async fn discover(
    params: DiscoverParams,
    launch: NativeLaunch,
    diagnostic_secrets: Vec<String>,
    metadata_secrets: Vec<String>,
) -> Result<Value> {
    let deadline = Instant::now() + READ_TIMEOUT;
    if !(1..=MAX_DISCOVER_PAGE_ENTRIES).contains(&params.limit) {
        return Err(Error::new("invalid", "discovery limit must be 1..1000"));
    }
    if encoded_len(&params.profile)? > MAX_PROFILE_BYTES {
        return Err(too_large());
    }
    if let Some(scope) = &params.native_scope {
        check_text(scope, MAX_REFERENCE_BYTES, true)?;
    }
    let binding = request_binding(
        &params.profile,
        &params.target_environment,
        params.archived,
        params.limit,
    )?;
    let cursor = decode_cursor(
        params.cursor.as_deref(),
        &binding,
        params.native_scope.as_deref(),
    )?;
    let store = probe_store(
        &launch,
        &params.target_environment,
        &metadata_secrets,
        &diagnostic_secrets,
        deadline,
    )
    .await?;
    validate_store(params.native_scope.as_deref(), cursor.as_ref(), &store)?;
    let (mut query, drainer) = open_pinned_reader(
        &launch,
        &params.target_environment,
        &metadata_secrets,
        &diagnostic_secrets,
        &store,
        deadline,
    )
    .await?;
    let result = read_page(
        &query.client(),
        &params,
        cursor.as_ref(),
        &binding,
        &store,
        &metadata_secrets,
        deadline,
    )
    .await;
    finish_reader(&mut query, Some(drainer)).await?;
    result.map_err(|error| query.client().diagnostic_error(error))
}

pub(crate) async fn probe_store(
    launch: &NativeLaunch,
    environment: &Value,
    metadata_secrets: &[String],
    diagnostic_secrets: &[String],
    deadline: Instant,
) -> Result<StoreContext> {
    let (mut probe, drainer, initialized) = start_initialized_reader(
        NativeLaunch {
            program: launch.program.clone(),
            args: launch.args.clone(),
            cwd: launch.cwd.clone(),
            environment: launch.environment.clone(),
        },
        metadata_secrets,
        diagnostic_secrets,
        deadline,
    )
    .await?;
    let probe_result: Result<StoreContext> = async {
        let client = probe.client();
        read_store(
            &client,
            environment,
            &initialized.codex_home,
            &launch.cwd,
            deadline,
        )
        .await
    }
    .await;
    finish_reader(&mut probe, Some(drainer)).await?;
    let probe_result = probe_result.map_err(|error| probe.client().diagnostic_error(error));
    drop(probe);
    probe_result
}

pub(crate) async fn open_pinned_reader(
    launch: &NativeLaunch,
    environment: &Value,
    metadata_secrets: &[String],
    diagnostic_secrets: &[String],
    store: &StoreContext,
    deadline: Instant,
) -> Result<(NativeProcess, JoinHandle<()>)> {
    // thread_store is fixed at process startup (pinned message_processor.rs:301-304).
    // A config/read reload must not relabel one store's page with another root.
    let pinned = pinned_launch(launch, store.root())?;
    let (mut query, drainer, initialized) =
        start_initialized_reader(pinned, metadata_secrets, diagnostic_secrets, deadline).await?;
    let result = async {
        let client = query.client();
        let pinned_store = read_store(
            &client,
            environment,
            &initialized.codex_home,
            &launch.cwd,
            deadline,
        )
        .await?;
        validate_pin(store.home(), store.root(), &pinned_store)
    }
    .await;
    match result {
        Ok(()) => Ok((query, drainer)),
        Err(error) => {
            finish_reader(&mut query, Some(drainer)).await?;
            Err(query.client().diagnostic_error(error))
        }
    }
}

/// Spawn and initialize a metadata reader before any caller can issue a
/// configuration, thread, or session request. A startup failure is definite
/// only after the reader is reaped and its complete output is drained.
async fn start_initialized_reader(
    launch: NativeLaunch,
    metadata_secrets: &[String],
    diagnostic_secrets: &[String],
    deadline: Instant,
) -> Result<(NativeProcess, JoinHandle<()>, NativeInitializeResponse)> {
    let mut retries_used = 0;
    loop {
        if Instant::now() >= deadline {
            // This refusal is before spawn; no native process or session exists.
            return Err(native_start_failed());
        }
        let (mut native, drainer, native_frame_seen) =
            spawn_reader(&launch, diagnostic_secrets, deadline).await?;
        let client = native.client();
        let mut initialize_response_received = false;
        let mut initialize_request_failed = false;
        let initialized = initialize_reader(
            &client,
            metadata_secrets,
            deadline,
            &mut initialize_response_received,
            &mut initialize_request_failed,
        )
        .await;
        let initialized = match initialized {
            Ok(initialized) => initialized,
            Err(initialize_error) => {
                // Observe a natural exit before cleanup can terminate the process.
                let natural_exit = wait_for_natural_startup_exit(&mut native, deadline).await;
                finish_reader(&mut native, Some(drainer)).await?;
                let stderr = client.stderr_snapshot();
                let evidence = StartupFailureEvidence {
                    initialize_request_failed,
                    initialize_response_received,
                    process_exited_unsuccessfully: natural_exit
                        .is_some_and(|status| !status.success()),
                    output_fully_drained: true,
                    thread_or_session_request_sent: false,
                    native_frame_seen: native_frame_seen.load(Ordering::Acquire),
                    stderr: &stderr,
                };
                if let Some(next_retry_count) = next_retry_count(retries_used, evidence) {
                    retries_used = next_retry_count;
                    sleep_until((Instant::now() + randomized_retry_delay()).min(deadline)).await;
                    continue;
                }
                if let Some(error) = exhausted_start_failure(retries_used, evidence, true) {
                    return Err(client.diagnostic_error(error));
                }
                return Err(client.diagnostic_error(initialize_error));
            }
        };

        // A received initialize response or initialized notification failure is
        // never eligible for retry. No session or thread request has run yet.
        if let Err(error) = notify_initialized(&client, deadline).await {
            finish_reader(&mut native, Some(drainer)).await?;
            if native_frame_seen.load(Ordering::Acquire) {
                return Err(client.diagnostic_error(Error::new(
                    "unknown",
                    "Codex native startup emitted an unexpected native frame",
                )));
            }
            return Err(client.diagnostic_error(error));
        }
        return Ok((native, drainer, initialized));
    }
}

async fn spawn_reader(
    launch: &NativeLaunch,
    diagnostic_secrets: &[String],
    deadline: Instant,
) -> Result<(NativeProcess, JoinHandle<()>, Arc<AtomicBool>)> {
    if Instant::now() >= deadline {
        return Err(native_start_failed());
    }
    let mut native = NativeProcess::spawn(
        NativeLaunch {
            program: launch.program.clone(),
            args: launch.args.clone(),
            cwd: launch.cwd.clone(),
            environment: launch.environment.clone(),
        },
        SecretPolicy::new(diagnostic_secrets.iter().cloned()),
    )
    .await
    .map_err(|error| {
        if error.code == "native_spawn_refused" {
            native_start_failed()
        } else {
            error
        }
    })?;
    let client = native.client();
    let mut frames = match native.take_frames() {
        Ok(frames) => frames,
        Err(error) => {
            finish_reader(&mut native, None).await?;
            return Err(error);
        }
    };
    let native_frame_seen = Arc::new(AtomicBool::new(false));
    let task_native_frame_seen = Arc::clone(&native_frame_seen);
    let drainer = tokio::spawn(async move {
        while let Some(frame) = frames.recv().await {
            task_native_frame_seen.store(true, Ordering::Release);
            if let NativeFrame::Request { id, method, params } = frame {
                // A reader never services auth refresh, approvals or tool requests.
                let _ = client
                    .reject_unsupported_request(id, &method, &params, deadline)
                    .await;
            }
        }
    });
    Ok((native, drainer, native_frame_seen))
}

async fn wait_for_natural_startup_exit(
    native: &mut NativeProcess,
    deadline: Instant,
) -> Option<std::process::ExitStatus> {
    let exit_deadline = (Instant::now() + STARTUP_EXIT_CONFIRMATION).min(deadline);
    match timeout_at(exit_deadline, native.transport.wait_for_process_exit()).await {
        Ok(Ok(status)) => Some(status),
        Ok(Err(_)) | Err(_) => None,
    }
}

pub(crate) async fn finish_reader(
    native: &mut NativeProcess,
    drainer: Option<JoinHandle<()>>,
) -> Result<()> {
    // One shared cleanup deadline, including when selection/readback was rejected.
    let cleanup_deadline = Instant::now() + CLEANUP_TIMEOUT;
    let reaped = native.terminate(false, cleanup_deadline).await;
    let output_drained = native.await_output_drained(cleanup_deadline).await;
    let notifications_drained = finish_drainer(drainer, cleanup_deadline).await;
    if reaped.is_err() || output_drained.is_err() || !notifications_drained {
        return Err(native.client().diagnostic_error(Error::new(
            "unknown",
            "Codex discovery reader cleanup is unconfirmed",
        )));
    }
    Ok(())
}

async fn initialize_reader(
    client: &NativeClient,
    secrets: &[String],
    deadline: Instant,
    response_received: &mut bool,
    request_failed: &mut bool,
) -> Result<NativeInitializeResponse> {
    let init = NativeInitializeParams {
        client_info: NativeClientInfo {
            name: "eliot-discover".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            title: Some("Eliot discovery".to_owned()),
        },
        capabilities: None,
    };
    let params = serde_json::to_value(init).map_err(|_| invalid_page())?;
    let initialized = match client.call("initialize", params, deadline).await {
        Ok(initialized) => initialized,
        Err(error) => {
            *request_failed = true;
            return Err(contextual_error(
                error,
                "native_readback",
                "Codex discovery initialize failed",
            ));
        }
    };
    *response_received = true;
    reject_secrets(&initialized, secrets)?;
    let initialized: NativeInitializeResponse =
        serde_json::from_value(initialized).map_err(|_| invalid_page())?;
    check_text(&initialized.codex_home, MAX_TEXT_BYTES, true)?;
    if initialized.platform_family.is_empty()
        || initialized.platform_os.is_empty()
        || initialized.user_agent.is_empty()
    {
        return Err(invalid_page());
    }
    Ok(initialized)
}

async fn notify_initialized(client: &NativeClient, deadline: Instant) -> Result<()> {
    client
        .notify("initialized", None, deadline)
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "native_readback",
                "Codex discovery initialized failed",
            )
        })
}

async fn read_store(
    client: &NativeClient,
    environment: &Value,
    reported_home: &str,
    reader_cwd: &Path,
    deadline: Instant,
) -> Result<StoreContext> {
    // Codex 979011409de0a60b52f179721948e65531d26144:
    // config_manager_service.rs:119-149 returns effective ConfigToml for cwd,
    // including trusted project layers and exact requirements. Config.additional preserves
    // sqlite_home; config/loader/mod.rs:554-561 resolves paths per layer first.
    let config = client
        .call(
            "config/read",
            json!({"cwd": reader_cwd, "includeLayers": true}),
            deadline,
        )
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "native_scope_unverified",
                "Codex native store selection could not be verified",
            )
        })?;
    let requirements = client
        .call("configRequirements/read", json!({}), deadline)
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "native_scope_unverified",
                "Codex native store selection could not be verified",
            )
        })?;
    ensure_store_pin_supported(&config, &requirements)?;
    let selection = {
        let config: NativeDiscoveryConfigReadResponse =
            serde_json::from_value(config).map_err(|_| unverified_store())?;
        select_store(&config, environment, Path::new(reported_home), reader_cwd)?
    };
    let reported_home = reported_home.to_owned();
    timeout_at(
        deadline,
        tokio::task::spawn_blocking(move || verify_store(&reported_home, &selection)),
    )
    .await
    .map_err(|_| unverified_store())?
    .map_err(|_| unverified_store())?
}

fn pinned_launch(launch: &NativeLaunch, root: &Path) -> Result<NativeLaunch> {
    let root = root.to_str().ok_or_else(unverified_store)?;
    // serde_json emits escapes valid in a TOML basic string; argv goes directly
    // to Codex, never through a shell. Pin before process-scoped store creation.
    let quoted = serde_json::to_string(root).map_err(|_| unverified_store())?;
    let mut args = vec![
        OsString::from("-c"),
        OsString::from(format!("sqlite_home={quoted}")),
    ];
    args.extend(launch.args.iter().cloned());
    Ok(NativeLaunch {
        program: launch.program.clone(),
        args,
        cwd: launch.cwd.clone(),
        environment: launch.environment.clone(),
    })
}

fn validate_pin(home: &Path, root: &Path, pinned: &StoreContext) -> Result<()> {
    // Requirements/managed config outrank CLI overrides. An absent sessionFlags
    // origin or another effective root is not proof that this process honored the pin.
    if !pinned.pin_verified || home != pinned.home() || root != pinned.root() {
        return Err(unverified_store());
    }
    Ok(())
}

async fn read_page(
    client: &NativeClient,
    params: &DiscoverParams,
    cursor: Option<&Cursor>,
    binding: &str,
    store: &StoreContext,
    secrets: &[String],
    deadline: Instant,
) -> Result<Value> {
    // Scope and cursor remain bound to the original selection, not the forced
    // query process's synthetic sessionFlags layer.
    let scope = &store.scope;

    let query = NativeThreadListParams {
        cursor: cursor.map(|cursor| cursor.native_cursor.clone()),
        limit: params.limit as u32,
        sort_key: "updated_at".to_owned(),
        sort_direction: "desc".to_owned(),
        source_kinds: SOURCE_KINDS.to_vec(),
        model_providers: Vec::new(),
        archived: params.archived,
        use_state_db_only: true,
    };
    let response = client
        .call(
            "thread/list",
            serde_json::to_value(query).map_err(|_| invalid_page())?,
            deadline,
        )
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "native_readback",
                "Codex discovery thread/list failed",
            )
        })?;
    reject_secrets(&response, secrets)?;
    let page: NativeThreadListResponse =
        serde_json::from_value(response).map_err(|_| invalid_page())?;
    if page.data.len() > params.limit {
        return Err(invalid_page());
    }
    let mut ids = HashSet::with_capacity(page.data.len());
    let mut subjects = Vec::with_capacity(page.data.len());
    for thread in page.data {
        if !ids.insert(thread.id.clone()) {
            return Err(Error::new(
                "source_integrity",
                "Codex discovery page repeats a native reference",
            ));
        }
        let subject = project_subject(thread)?;
        if encoded_len(&subject)? > MAX_EVENT_BYTES {
            return Err(too_large());
        }
        subjects.push(subject);
    }
    let complete = page.next_cursor.is_none();
    let next_cursor = if let Some(next) = page.next_cursor {
        check_text(&next, MAX_NATIVE_CURSOR_BYTES, true)?;
        if cursor.is_some_and(|cursor| cursor.native_cursor == next) {
            return Err(Error::new(
                "source_integrity",
                "Codex discovery cursor did not advance",
            ));
        }
        let encoded = format!(
            "{CURSOR_PREFIX}{}",
            serde_json::to_string(&Cursor {
                version: 1,
                native_scope: scope.clone(),
                binding: binding.to_owned(),
                store_binding: store.binding.clone(),
                native_cursor: next,
            })
            .map_err(|_| invalid_page())?
        );
        if encoded.len() > MAX_CURSOR_BYTES {
            return Err(too_large());
        }
        Some(encoded)
    } else {
        None
    };
    let value = serde_json::to_value(DiscoverPage {
        harness: "codex".to_owned(),
        native_scope: scope.clone(),
        subjects,
        next_cursor,
        coverage: DiscoverCoverage {
            kind: "state_db_only".to_owned(),
            complete,
        },
    })
    .map_err(|_| invalid_page())?;
    reject_secrets(&value, secrets)?;
    if encoded_len(&value)? > MAX_DISCOVER_PAGE_BYTES {
        return Err(too_large());
    }
    Ok(value)
}

pub(crate) fn project_subject(thread: NativeDiscoveryThread) -> Result<DiscoverSubject> {
    check_text(&thread.id, MAX_REFERENCE_BYTES, true)?;
    check_text(&thread.cwd, MAX_TEXT_BYTES, false)?;
    Ok(DiscoverSubject {
        native_ref: thread.id,
        workspace: (!thread.cwd.is_empty()).then_some(thread.cwd),
        state: observed_state(thread.status).to_owned(),
        created_at: Some(milliseconds(thread.created_at)?),
        updated_at: Some(milliseconds(thread.updated_at)?),
        source: Some(source_label(&thread.source).to_owned()),
        ephemeral: Some(thread.ephemeral),
    })
}

fn select_store(
    config: &NativeDiscoveryConfigReadResponse,
    environment: &Value,
    codex_home: &Path,
    reader_cwd: &Path,
) -> Result<StoreSelection> {
    if !codex_home.is_absolute() || !reader_cwd.is_absolute() {
        return Err(unverified_store());
    }
    let origin = config.origins.get("sqlite_home").cloned();
    let effective = config.config.as_object().ok_or_else(unverified_store)?;
    let configured = match effective.get("sqlite_home") {
        // Native resolves config paths against their layer, not the reader cwd.
        Some(Value::String(path)) if Path::new(path).is_absolute() && !path.contains('\0') => {
            Some(path.clone())
        }
        Some(Value::Null) if origin.is_none() => None,
        _ => return Err(unverified_store()),
    };
    let environment = sqlite_environment(environment, cfg!(windows))?;
    let root = match configured.as_deref().or(environment.as_deref()) {
        Some(raw) => {
            check_text(raw, MAX_TEXT_BYTES, true).map_err(|_| unverified_store())?;
            let path = Path::new(raw);
            // Native expands home and normalizes '..' lexically before filesystem
            // resolution. Do not substitute a potentially different canonical path.
            if raw == "~"
                || raw.starts_with("~/")
                || cfg!(windows) && raw.starts_with("~\\")
                || path.components().any(|part| part == Component::ParentDir)
            {
                return Err(unverified_store());
            }
            if path.is_absolute() {
                path.to_owned()
            } else if path.has_root()
                || path
                    .components()
                    .any(|part| matches!(part, Component::Prefix(_)))
            {
                return Err(unverified_store());
            } else {
                reader_cwd.join(path)
            }
        }
        None => codex_home.to_owned(),
    };
    Ok(StoreSelection {
        configured,
        origin,
        environment,
        root,
    })
}

fn sqlite_environment(environment: &Value, windows: bool) -> Result<Option<String>> {
    let environment = environment.as_object().ok_or_else(unverified_store)?;
    let mut matches = environment.iter().filter(|(name, _)| {
        if windows {
            name.eq_ignore_ascii_case("CODEX_SQLITE_HOME")
        } else {
            name.as_str() == "CODEX_SQLITE_HOME"
        }
    });
    let Some((_, value)) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(unverified_store());
    }
    let raw = value.as_str().ok_or_else(unverified_store)?;
    // Pinned core/src/config/mod.rs:272-281 passes the trimmed value, not raw.
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_owned()))
    }
}

fn verify_store(reported_home: &str, selection: &StoreSelection) -> Result<StoreContext> {
    fn canonical_directory(path: &Path) -> Result<PathBuf> {
        let path = path.canonicalize().map_err(|_| unverified_store())?;
        if !path.is_absolute() || !path.is_dir() {
            return Err(unverified_store());
        }
        Ok(path)
    }
    let home = canonical_directory(Path::new(reported_home))?;
    let root = canonical_directory(&selection.root)?;
    let scope = store_scope(reported_home, &home, &root)?;
    let binding = fingerprint(json!({
        "domain": "eliot/codex/discovery/store-selection/v1",
        "home": home.to_str().ok_or_else(unverified_store)?,
        "store": root.to_str().ok_or_else(unverified_store)?,
        "selection": selection,
    }))?;
    let pin_verified = selection.configured.is_some()
        && selection
            .origin
            .as_ref()
            .and_then(|origin| origin.pointer("/name/type"))
            .and_then(Value::as_str)
            == Some("sessionFlags");
    Ok(StoreContext {
        scope,
        binding,
        home,
        root,
        pin_verified,
    })
}

fn store_scope(reported_home: &str, home: &Path, root: &Path) -> Result<String> {
    if home == root {
        return Ok(native_scope(reported_home));
    }
    Ok(format!(
        "codex-store:v1:{}",
        fingerprint(json!({
            "domain": "eliot/codex/discovery/store-scope/v1",
            "home": home.to_str().ok_or_else(unverified_store)?,
            "store": root.to_str().ok_or_else(unverified_store)?,
        }))?
    ))
}

fn validate_store(
    scope: Option<&str>,
    cursor: Option<&Cursor>,
    store: &StoreContext,
) -> Result<()> {
    if scope.is_some_and(|scope| scope != store.scope())
        || cursor.is_some_and(|cursor| {
            cursor.native_scope != store.scope() || cursor.store_binding != store.binding()
        })
    {
        return Err(cursor_mismatch());
    }
    Ok(())
}

fn request_binding(
    profile: &Value,
    target_environment: &Value,
    archived: bool,
    limit: usize,
) -> Result<String> {
    let context = json!({
        "profile": profile,
        // Resolved environment values contribute only to the digest, never the cursor payload.
        "target_environment": target_environment,
        "method": "thread/list",
        "sortKey": "updated_at",
        "sortDirection": "desc",
        "sourceKinds": SOURCE_KINDS,
        "modelProviders": [],
        "archived": archived,
        "useStateDbOnly": true,
        // Native cursor stability across page-size changes is not assumed.
        "limit": limit,
    });
    fingerprint(context)
}

pub(crate) fn fingerprint(context: Value) -> Result<String> {
    Ok(canonical_json_sha256(&context)
        .map_err(|_| invalid_page())?
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn decode_cursor(raw: Option<&str>, binding: &str, scope: Option<&str>) -> Result<Option<Cursor>> {
    let Some(raw) = raw else { return Ok(None) };
    if raw.len() > MAX_CURSOR_BYTES {
        return Err(too_large());
    }
    let cursor: Cursor = serde_json::from_str(
        raw.strip_prefix(CURSOR_PREFIX)
            .ok_or_else(cursor_mismatch)?,
    )
    .map_err(|_| cursor_mismatch())?;
    check_text(&cursor.native_scope, MAX_REFERENCE_BYTES, true)?;
    check_text(&cursor.native_cursor, MAX_NATIVE_CURSOR_BYTES, true)?;
    if cursor.store_binding.len() != 64
        || !cursor
            .store_binding
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(cursor_mismatch());
    }
    if cursor.version != 1
        || cursor.binding != binding
        || scope.is_some_and(|scope| scope != cursor.native_scope)
    {
        return Err(cursor_mismatch());
    }
    Ok(Some(cursor))
}

fn observed_state(status: NativeDiscoveryStatus) -> &'static str {
    match status {
        NativeDiscoveryStatus::Idle => "idle",
        NativeDiscoveryStatus::Active { active_flags } => {
            if active_flags
                .iter()
                .any(|flag| !matches!(flag.as_str(), "waitingOnApproval" | "waitingOnUserInput"))
            {
                "unknown"
            } else if active_flags.is_empty() {
                "busy"
            } else {
                "waiting"
            }
        }
        // Reader-local unloaded/error state is never external stop evidence.
        _ => "unknown",
    }
}

fn source_label(source: &Value) -> &'static str {
    match source.as_str() {
        Some("cli") => return "cli",
        Some("vscode") => return "vscode",
        Some("exec") => return "exec",
        Some("appServer") => return "appServer",
        Some("unknown") => return "unknown",
        _ => {}
    }
    if let Some(subagent) = source.get("subAgent") {
        return match subagent.as_str() {
            Some("review") => "subAgentReview",
            Some("compact") => "subAgentCompact",
            _ if subagent.get("thread_spawn").is_some() => "subAgentThreadSpawn",
            _ if subagent.get("other").is_some() => "subAgentOther",
            _ => "subAgent",
        };
    }
    "unknown"
}

pub(crate) fn milliseconds(seconds: i64) -> Result<i64> {
    seconds.checked_mul(1000).ok_or_else(invalid_page)
}

pub(crate) fn reject_secrets(value: &Value, secrets: &[String]) -> Result<()> {
    if SecretPolicy::new(secrets.iter().cloned()).contains_value(value) {
        return Err(Error::new(
            "secret_in_payload",
            "Codex discovery response contains a known secret",
        ));
    }
    Ok(())
}

async fn finish_drainer(task: Option<JoinHandle<()>>, deadline: Instant) -> bool {
    let Some(mut task) = task else { return true };
    if task.is_finished() {
        return task.await.is_ok();
    }
    match timeout_at(deadline, &mut task).await {
        Ok(result) => result.is_ok(),
        Err(_) => {
            task.abort();
            let _ = task.await;
            false
        }
    }
}

fn check_text(value: &str, max: usize, nonempty: bool) -> Result<()> {
    if value.len() > max {
        return Err(too_large());
    }
    if value.contains('\0') || nonempty && value.trim().is_empty() {
        return Err(invalid_page());
    }
    Ok(())
}

fn encoded_len(value: &impl Serialize) -> Result<usize> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| invalid_page())
}

fn cursor_mismatch() -> Error {
    Error::new(
        "cursor_mismatch",
        "Codex discovery cursor does not match its profile, environment, store, scope or query",
    )
}

fn unverified_store() -> Error {
    Error::new(
        "native_scope_unverified",
        "Codex discovery could not verify its effective home and SQLite store",
    )
}

fn invalid_page() -> Error {
    Error::new(
        "source_integrity",
        "Codex discovery metadata does not match the installed schema",
    )
}

fn too_large() -> Error {
    Error::new(
        "frame_too_large",
        "Codex discovery metadata exceeds its bound",
    )
}

#[cfg(test)]
mod tests {
    use super::{
        CURSOR_PREFIX, Cursor, NativeLaunch, READ_TIMEOUT, StoreContext, binding_paths,
        decode_cursor, fingerprint, mcp_plugin_states, mcp_server_entries, native_scope,
        pinned_launch, pinned_launch_for_binding, read_effective_mcp_surface, request_binding,
        select_store, sqlite_environment, start_initialized_reader, store_scope, validate_pin,
        validate_store,
    };
    use crate::native::types::NativeDiscoveryConfigReadResponse;
    use eliot_core::{NativeStoreBinding, SecretPolicy};
    use serde_json::json;
    use std::{ffi::OsString, path::Path, time::Duration};

    #[tokio::test]
    async fn expired_startup_deadline_is_definite_before_any_native_spawn() {
        let launch = NativeLaunch {
            program: "codex-must-not-be-executed".into(),
            args: Vec::new(),
            cwd: std::env::current_dir().unwrap(),
            environment: Vec::new(),
        };
        let error =
            match start_initialized_reader(launch, &[], &[], tokio::time::Instant::now()).await {
                Ok(_) => panic!("expired startup must not spawn a native process"),
                Err(error) => error,
            };
        assert_eq!(error.code, "native_start_failed");
    }

    #[tokio::test]
    async fn nonexistent_native_executable_is_a_definite_spawn_refusal() {
        let program =
            std::env::temp_dir().join(format!("eliot-codex-missing-{}", ulid::Ulid::new()));
        let launch = NativeLaunch {
            program: program.into_os_string(),
            args: Vec::new(),
            cwd: std::env::current_dir().unwrap(),
            environment: Vec::new(),
        };
        let error = match start_initialized_reader(
            launch,
            &[],
            &[],
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        {
            Ok(_) => panic!("a unique missing executable must not spawn"),
            Err(error) => error,
        };
        assert_eq!(error.code, "native_start_failed");
    }

    #[test]
    fn controlled_store_binding_pins_relative_workspace_selection_and_round_trips() {
        let base = Path::new(if cfg!(windows) {
            r"C:\binding"
        } else {
            "/binding"
        });
        let home = base.join("home");
        let workspace = base.join("prepared workspace");
        let config: NativeDiscoveryConfigReadResponse =
            serde_json::from_value(json!({"config":{"sqlite_home":null}, "origins":{}})).unwrap();
        let selection = select_store(
            &config,
            &json!({"CODEX_SQLITE_HOME":" sqlite-data "}),
            &home,
            &workspace,
        )
        .unwrap();
        assert_eq!(selection.root, workspace.join("sqlite-data"));
        let mut observed = StoreContext {
            scope: store_scope(home.to_str().unwrap(), &home, &selection.root).unwrap(),
            binding: "private-discovery-only".to_owned(),
            home,
            root: selection.root,
            pin_verified: true,
        };
        let binding = observed.native_binding(&[]).unwrap();
        let encoded = serde_json::to_value(&binding).unwrap();
        assert_eq!(encoded.as_object().unwrap().len(), 3);
        let binding: NativeStoreBinding = serde_json::from_value(encoded).unwrap();
        let launch = NativeLaunch {
            program: "stable-codex".into(),
            args: ["app-server", "--listen", "stdio://"]
                .map(OsString::from)
                .to_vec(),
            cwd: workspace,
            environment: vec![
                (
                    if cfg!(windows) {
                        "codex_home"
                    } else {
                        "CODEX_HOME"
                    }
                    .into(),
                    "late-home".into(),
                ),
                ("CODEX_SQLITE_HOME".into(), "late-store".into()),
            ],
        };
        let pinned = pinned_launch_for_binding(&launch, &binding).unwrap();
        assert_eq!(pinned.program, launch.program);
        assert_eq!(pinned.cwd, launch.cwd);
        assert_eq!(&pinned.args[2..], launch.args.as_slice());
        let quoted = pinned.args[1]
            .to_str()
            .unwrap()
            .strip_prefix("sqlite_home=")
            .unwrap();
        assert_eq!(
            serde_json::from_str::<String>(quoted).unwrap(),
            binding.root
        );
        let homes: Vec<_> = pinned
            .environment
            .iter()
            .filter(|(name, _)| {
                let name = name.to_string_lossy();
                if cfg!(windows) {
                    name.eq_ignore_ascii_case("CODEX_HOME")
                } else {
                    name == "CODEX_HOME"
                }
            })
            .collect();
        assert_eq!(homes.len(), 1);
        assert_eq!(homes[0].1, OsString::from(&binding.home));
        assert_eq!(pinned.environment[0], launch.environment[1]);
        let (home, root) = binding_paths(&binding).unwrap();
        // Verification consumes canonical paths and pin origin, not private cursor digests/scope.
        observed.scope = "synthetic-readback-scope".to_owned();
        assert!(validate_pin(home, root, &observed).is_ok());
        observed.home = base.join("wrong-home");
        assert!(validate_pin(home, root, &observed).is_err());
        observed.home = home.to_owned();
        observed.root = base.join("wrong-store");
        assert!(validate_pin(home, root, &observed).is_err());
        observed.root = root.to_owned();
        observed.pin_verified = false;
        assert!(validate_pin(home, root, &observed).is_err());
        let mut relative = binding.clone();
        relative.root = "relative-store".to_owned();
        assert!(pinned_launch_for_binding(&launch, &relative).is_err());
    }

    #[tokio::test]
    async fn minimal_enumeration_reader_is_pinned_before_any_spawn() {
        let workspace = Path::new(if cfg!(windows) {
            r"C:\fixture\workspace"
        } else {
            "/fixture/workspace"
        });
        let launch = NativeLaunch {
            program: "codex-not-executed".into(),
            args: vec![OsString::from("app-server")],
            cwd: workspace.to_path_buf(),
            environment: vec![(OsString::from("CODEX_HOME"), OsString::from("owner-home"))],
        };
        // A non-absolute frozen binding is rejected at pin time, before any
        // metadata reader process spawns.
        let relative: NativeStoreBinding = serde_json::from_value(json!({
            "home": "relative-home",
            "root": "relative-root",
            "native_scope": "fixture-scope",
        }))
        .unwrap();
        let error = read_effective_mcp_surface(
            &launch,
            &relative,
            &[],
            &[],
            tokio::time::Instant::now() + READ_TIMEOUT,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "native_scope_unverified");
        // A well-formed absolute binding passes the pin, so the failure moves
        // past it to spawning the (absent here) reader program.
        let home = if cfg!(windows) {
            r"C:\fixture\frozen-home"
        } else {
            "/fixture/frozen-home"
        };
        let root = if cfg!(windows) {
            r"C:\fixture\frozen-root"
        } else {
            "/fixture/frozen-root"
        };
        let binding: NativeStoreBinding = serde_json::from_value(json!({
            "home": home,
            "root": root,
            "native_scope": "fixture-scope",
        }))
        .unwrap();
        let error = read_effective_mcp_surface(
            &launch,
            &binding,
            &[],
            &[],
            tokio::time::Instant::now() + READ_TIMEOUT,
        )
        .await
        .unwrap_err();
        assert_ne!(error.code, "native_scope_unverified");
    }

    #[test]
    fn controlled_store_binding_keeps_metadata_credentials_separate_from_diagnostic_paths() {
        let home = Path::new(if cfg!(windows) {
            r"C:\binding\home"
        } else {
            "/binding/home"
        })
        .to_owned();
        let store = StoreContext {
            scope: native_scope(home.to_str().unwrap()),
            binding: "private-discovery-only".to_owned(),
            home: home.clone(),
            root: home,
            pin_verified: false,
        };
        let path = store.home.to_str().unwrap().to_owned();
        let metadata_secrets = vec!["fixture-credential".to_owned()];
        let diagnostic_secrets = vec![path.clone(), "fixture-credential".to_owned()];
        assert!(store.native_binding(&metadata_secrets).is_ok());
        assert_eq!(
            SecretPolicy::new(&diagnostic_secrets).redact_text(&path),
            "[REDACTED]"
        );
        // A credential variable contributing the same literal must remain protected.
        let error = store.native_binding(&[path]).unwrap_err();
        assert_eq!(error.code, "secret_in_payload");
    }

    #[test]
    fn discovery_fingerprint_retains_compact_json_lowercase_hex() {
        // Independently hashed literal JSON, not a second canonicalizer. This
        // is a fingerprint representation fixture; query/domain builders stay
        // with their existing callers and cursor/environment binding tests.
        assert_eq!(
            fingerprint(json!({
                "target_environment": {"DUMMY": "é", "CODEX_HOME": r"C:\fixture\home"},
                "sourceKinds": ["cli", "exec"],
                "profile": {"model": "gpt-6-luna", "effort": "medium"},
                "method": "thread/list",
            }))
            .unwrap(),
            "40e348a754ff47a2fae7f2daf7339e8c27addab028eb7031c8b636da02e5b88c"
        );
    }

    #[test]
    fn discovery_cursor_rejects_changed_target_environment_before_io() {
        let profile = json!({"harness": "codex"});
        let environment = json!({
            "CODEX_HOME": "same-codex-home",
            "CODEX_SQLITE_HOME": "original-database",
        });
        let scope = "codex:test-scope";
        let binding =
            request_binding(&profile, &environment, false, 100).expect("original binding");
        let cursor = format!(
            "{CURSOR_PREFIX}{}",
            serde_json::to_string(&Cursor {
                version: 1,
                native_scope: scope.to_owned(),
                binding: binding.clone(),
                store_binding: "a".repeat(64),
                native_cursor: "native-next-page".to_owned(),
            })
            .expect("cursor encoding")
        );
        assert!(!cursor.contains("same-codex-home"));
        assert!(!cursor.contains("original-database"));
        assert!(decode_cursor(Some(&cursor), &binding, Some(scope)).is_ok());

        let changed_environment = json!({
            "CODEX_HOME": "same-codex-home",
            "CODEX_SQLITE_HOME": "different-database",
        });
        let changed_binding =
            request_binding(&profile, &changed_environment, false, 100).expect("changed binding");
        // These are the pure preflight helpers called before NativeProcess::spawn.
        let error = decode_cursor(Some(&cursor), &changed_binding, Some(scope))
            .err()
            .expect("changed environment must reject the old cursor");
        assert_eq!(error.code, "cursor_mismatch");
    }

    #[test]
    fn discovery_store_selection_preserves_default_and_fences_config_and_cwd() {
        let base = Path::new(if cfg!(windows) {
            r"C:\discovery"
        } else {
            "/discovery"
        });
        let home = base.join("home");
        let cwd = base.join("reader-one");
        let next_cwd = base.join("reader-two");
        let reported_home = home.to_str().expect("fixture home");
        let config: NativeDiscoveryConfigReadResponse =
            serde_json::from_value(json!({"config": {"sqlite_home": null}, "origins": {}}))
                .expect("default config");
        let default = select_store(&config, &json!({}), &home, &cwd).expect("default selector");
        assert_eq!(default.root, home);
        assert_eq!(
            store_scope(reported_home, &home, &home).expect("default scope"),
            native_scope(reported_home)
        );
        let missing: NativeDiscoveryConfigReadResponse =
            serde_json::from_value(json!({"config": {}, "origins": {}})).expect("missing selector");
        let error = select_store(&missing, &json!({}), &home, &cwd)
            .err()
            .expect("missing selector must remain unverified");
        assert_eq!(error.code, "native_scope_unverified");

        let configured_root = base.join(" configured-store ");
        let mut configured: NativeDiscoveryConfigReadResponse = serde_json::from_value(json!({
            "config": {"sqlite_home": configured_root},
            "origins": {"sqlite_home": {"version": "fixture"}},
        }))
        .expect("configured selector");
        let environment = json!({"CODEX_SQLITE_HOME": "environment-store"});
        let selected = select_store(&configured, &environment, &home, &cwd).expect("config wins");
        assert_eq!(selected.root, configured_root);
        let other =
            select_store(&configured, &environment, &home, &next_cwd).expect("new reader cwd");
        let scope = store_scope(reported_home, &home, &selected.root).expect("configured scope");
        let other_scope = store_scope(reported_home, &home, &other.root).expect("other scope");
        assert_eq!(scope, other_scope);
        assert!(scope.starts_with("codex-store:v1:"));
        configured.config["sqlite_home"] = json!("relative-config-store");
        assert!(select_store(&configured, &environment, &home, &cwd).is_err());

        let env_selected =
            select_store(&config, &environment, &home, &cwd).expect("environment selector");
        let next_env =
            select_store(&config, &environment, &home, &next_cwd).expect("relative env cwd");
        assert_eq!(env_selected.root, cwd.join("environment-store"));
        assert_ne!(env_selected.root, next_env.root);
        let env_scope = store_scope(reported_home, &home, &env_selected.root).expect("env scope");
        let next_scope = store_scope(reported_home, &home, &next_env.root).expect("new env scope");
        let cursor = Cursor {
            version: 1,
            native_scope: env_scope,
            binding: "profile-query".to_owned(),
            store_binding: "a".repeat(64),
            native_cursor: "next-page".to_owned(),
        };
        assert!(
            validate_store(
                None,
                Some(&cursor),
                &StoreContext {
                    scope: next_scope,
                    binding: "a".repeat(64),
                    home: home.clone(),
                    root: next_env.root.clone(),
                    pin_verified: false,
                }
            )
            .is_err()
        );
        assert!(
            validate_store(
                None,
                Some(&cursor),
                &StoreContext {
                    scope: cursor.native_scope.clone(),
                    binding: "b".repeat(64),
                    home: home.clone(),
                    root: env_selected.root.clone(),
                    pin_verified: false,
                }
            )
            .is_err()
        );
        configured.config["sqlite_home"] = serde_json::Value::Null;
        assert!(select_store(&configured, &environment, &home, &cwd).is_err());
        configured
            .config
            .as_object_mut()
            .expect("config object")
            .remove("sqlite_home");
        assert!(select_store(&configured, &environment, &home, &cwd).is_err());

        assert!(
            select_store(
                &config,
                &json!({"CODEX_SQLITE_HOME": "~/store"}),
                &home,
                &cwd
            )
            .is_err()
        );
        assert!(
            select_store(
                &config,
                &json!({"CODEX_SQLITE_HOME": "../store"}),
                &home,
                &cwd
            )
            .is_err()
        );
        assert_eq!(
            sqlite_environment(&json!({"codex_sqlite_home": " db "}), true).expect("Windows env"),
            Some("db".to_owned())
        );
        assert!(
            sqlite_environment(
                &json!({"CODEX_SQLITE_HOME": "one", "codex_sqlite_home": "two"}),
                true
            )
            .is_err()
        );
    }

    #[test]
    fn mcp_server_entries_projects_only_the_table_keys() {
        assert!(
            mcp_server_entries(&json!({"config": {}}))
                .unwrap()
                .is_empty()
        );
        assert!(
            mcp_server_entries(&json!({"config": {"mcp_servers": null}}))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            mcp_server_entries(&json!({"config": {"mcp_servers": {"b": {}, "a": {}}}}))
                .unwrap()
                .into_iter()
                .map(|entry| entry.name)
                .collect::<Vec<_>>(),
            vec!["a".to_owned(), "b".to_owned()]
        );
        for response in [
            json!({}),
            json!({"config": null}),
            json!({"config": {"mcp_servers": []}}),
            json!({"config": {"mcp_servers": {"": {}}}}),
        ] {
            assert!(mcp_server_entries(&response).is_err());
        }
    }

    #[test]
    fn mcp_server_entries_project_command_and_url_transports() {
        let entries = mcp_server_entries(&json!({"config": {"mcp_servers": {
            "stdio": {"command": "s.exe", "args": ["stdio"]},
            "remote": {"url": "https://remote.example/mcp"},
            "bare": {},
        }}}))
        .unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| (
                    entry.name.as_str(),
                    entry.command.as_deref(),
                    entry.url.as_deref()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("bare", None, None),
                ("remote", None, Some("https://remote.example/mcp")),
                ("stdio", Some("s.exe"), None),
            ]
        );
        // Non-object tables and non-string transports fail the enumeration
        // instead of projecting a partial transport.
        for response in [
            json!({"config": {"mcp_servers": {"bad": []}}}),
            json!({"config": {"mcp_servers": {"bad": {"command": 1}}}}),
            json!({"config": {"mcp_servers": {"bad": {"url": {"nested": true}}}}}),
        ] {
            assert!(mcp_server_entries(&response).is_err());
        }
        // Null transports project as absent, like a bare table.
        let entries = mcp_server_entries(&json!({"config": {"mcp_servers": {
            "nulled": {"command": null, "url": null},
        }}}))
        .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "nulled");
        assert!(entries[0].command.is_none());
        assert!(entries[0].url.is_none());
    }

    #[test]
    fn mcp_plugin_states_project_id_and_enabled_flag() {
        assert!(
            mcp_plugin_states(&json!({"config": {}}))
                .unwrap()
                .is_empty()
        );
        assert!(
            mcp_plugin_states(&json!({"config": {"plugins": null}}))
                .unwrap()
                .is_empty()
        );
        let states = mcp_plugin_states(&json!({"config": {"plugins": {
            "b@openai-bundled": {"enabled": true},
            "a@openai-bundled": {"enabled": false},
            "defaulted@openai-bundled": {},
        }}}))
        .unwrap();
        assert_eq!(
            states
                .iter()
                .map(|plugin| (plugin.id.as_str(), plugin.enabled))
                .collect::<Vec<_>>(),
            vec![
                ("a@openai-bundled", false),
                ("b@openai-bundled", true),
                ("defaulted@openai-bundled", true),
            ]
        );
        // Non-object tables and non-boolean flags fail the enumeration
        // instead of projecting a partial surface.
        for response in [
            json!({"config": {"plugins": []}}),
            json!({"config": {"plugins": {"bad": []}}}),
            json!({"config": {"plugins": {"bad": {"enabled": "yes"}}}}),
        ] {
            assert!(mcp_plugin_states(&response).is_err());
        }
    }

    #[test]
    fn discovery_query_pins_original_store_across_config_reload() {
        let base = Path::new(if cfg!(windows) {
            r"C:\discovery"
        } else {
            "/discovery"
        });
        let home = base.join("home \"quoted\"");
        let cwd = base.join("private-reader");
        let selected = StoreContext {
            scope: native_scope(home.to_str().expect("fixture home")),
            binding: "original-selection".to_owned(),
            home: home.clone(),
            root: home.clone(),
            pin_verified: false,
        };
        let launch = NativeLaunch {
            program: OsString::from("codex"),
            args: ["app-server", "--listen", "stdio://"]
                .map(OsString::from)
                .to_vec(),
            cwd: cwd.clone(),
            environment: vec![(
                OsString::from("CODEX_SQLITE_HOME"),
                OsString::from("environment-store"),
            )],
        };
        let pinned = pinned_launch(&launch, selected.root()).expect("pinned launch");
        assert_eq!(pinned.program, launch.program);
        assert_eq!(pinned.cwd, cwd);
        assert_eq!(pinned.environment, launch.environment);
        assert_eq!(pinned.args[0].to_str(), Some("-c"));
        assert_eq!(&pinned.args[2..], launch.args.as_slice());
        let argument = pinned.args[1].to_str().expect("pin argument");
        let quoted = argument
            .strip_prefix("sqlite_home=")
            .expect("store override");
        let decoded: String = serde_json::from_str(quoted).expect("quoted store round trip");
        assert_eq!(decoded, selected.root.to_str().expect("selected root"));

        let changed_root = base.join("changed-store");
        let changed: NativeDiscoveryConfigReadResponse = serde_json::from_value(json!({
            "config": {"sqlite_home": changed_root},
            "origins": {"sqlite_home": {"name": {"type": "user"}, "version": "changed"}},
        }))
        .expect("changed config");
        let changed_selection =
            select_store(&changed, &json!({}), &home, &cwd).expect("changed selection");
        assert_ne!(changed_selection.root, selected.root);
        // A reload cannot alter the already constructed per-process argv.
        assert_eq!(decoded, home.to_str().expect("original store"));
        let mut query = StoreContext {
            scope: "synthetic-session-flags".to_owned(),
            binding: "synthetic-selection".to_owned(),
            home: home.clone(),
            root: selected.root.clone(),
            pin_verified: true,
        };
        assert!(validate_pin(selected.home(), selected.root(), &query).is_ok());
        assert_eq!(selected.scope, native_scope(home.to_str().expect("home")));
        assert_eq!(selected.binding, "original-selection");
        query.root = changed_selection.root;
        assert!(validate_pin(selected.home(), selected.root(), &query).is_err());
        query.root = selected.root.clone();
        query.pin_verified = false;
        assert!(validate_pin(selected.home(), selected.root(), &query).is_err());
    }
}
