//! Probe selection, then read metadata through a reader pinned to that exact store.
//! No controlled thread or lifecycle publication.

use crate::{
    host::{contextual_error, native_scope},
    native::{
        NativeClient, NativeFrame, NativeLaunch, NativeProcess,
        store_policy::ensure_store_pin_supported,
        types::{
            NativeClientInfo, NativeDiscoveryConfigReadResponse, NativeDiscoverySourceKind,
            NativeDiscoveryStatus, NativeDiscoveryThread, NativeInitializeParams,
            NativeInitializeResponse, NativeThreadListParams, NativeThreadListResponse,
        },
    },
};
use eliot_core::{Error, Result, SecretPolicy, canonical_json_sha256};
use eliot_plugin::{
    DiscoverCoverage, DiscoverPage, DiscoverParams, DiscoverSubject, MAX_DISCOVER_PAGE_BYTES,
    MAX_DISCOVER_PAGE_ENTRIES, MAX_EVENT_BYTES,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    ffi::OsString,
    path::{Component, Path, PathBuf},
    time::Duration,
};
use tokio::{
    task::JoinHandle,
    time::{Instant, timeout_at},
};

pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(25);
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
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
}

pub(crate) async fn discover(
    params: DiscoverParams,
    launch: NativeLaunch,
    secrets: Vec<String>,
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
    let binding = request_binding(&params.profile, &params.target_environment, params.limit)?;
    let cursor = decode_cursor(
        params.cursor.as_deref(),
        &binding,
        params.native_scope.as_deref(),
    )?;
    let store = probe_store(&launch, &params.target_environment, &secrets, deadline).await?;
    validate_store(params.native_scope.as_deref(), cursor.as_ref(), &store)?;
    let (mut query, drainer) = open_pinned_reader(
        &launch,
        &params.target_environment,
        &secrets,
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
        &secrets,
        deadline,
    )
    .await;
    finish_reader(&mut query, Some(drainer)).await?;
    result.map_err(|error| query.client().diagnostic_error(error))
}

pub(crate) async fn probe_store(
    launch: &NativeLaunch,
    environment: &Value,
    secrets: &[String],
    deadline: Instant,
) -> Result<StoreContext> {
    let (mut probe, drainer) = start_reader(
        NativeLaunch {
            program: launch.program.clone(),
            args: launch.args.clone(),
            cwd: launch.cwd.clone(),
            environment: launch.environment.clone(),
        },
        secrets,
        deadline,
    )
    .await?;
    let probe_result: Result<StoreContext> = async {
        let client = probe.client();
        let initialized = initialize_reader(&client, secrets, deadline).await?;
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
    secrets: &[String],
    store: &StoreContext,
    deadline: Instant,
) -> Result<(NativeProcess, JoinHandle<()>)> {
    // thread_store is fixed at process startup (pinned message_processor.rs:301-304).
    // A config/read reload must not relabel one store's page with another root.
    let pinned = pinned_launch(launch, store)?;
    let (mut query, drainer) = start_reader(pinned, secrets, deadline).await?;
    let result = async {
        let client = query.client();
        let initialized = initialize_reader(&client, secrets, deadline).await?;
        let pinned_store = read_store(
            &client,
            environment,
            &initialized.codex_home,
            &launch.cwd,
            deadline,
        )
        .await?;
        validate_pin(store, &pinned_store)
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

async fn start_reader(
    launch: NativeLaunch,
    secrets: &[String],
    deadline: Instant,
) -> Result<(NativeProcess, JoinHandle<()>)> {
    if Instant::now() >= deadline {
        return Err(Error::new(
            "native_readback",
            "Codex discovery read deadline elapsed",
        ));
    }
    let mut native =
        NativeProcess::spawn(launch, SecretPolicy::new(secrets.iter().cloned())).await?;
    let client = native.client();
    let mut frames = match native.take_frames() {
        Ok(frames) => frames,
        Err(error) => {
            finish_reader(&mut native, None).await?;
            return Err(error);
        }
    };
    let drainer = tokio::spawn(async move {
        while let Some(frame) = frames.recv().await {
            if let NativeFrame::Request { id, method, params } = frame {
                // A reader never services auth refresh, approvals or tool requests.
                let _ = client
                    .reject_unsupported_request(id, &method, &params, deadline)
                    .await;
            }
        }
    });
    Ok((native, drainer))
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
) -> Result<NativeInitializeResponse> {
    let init = NativeInitializeParams {
        client_info: NativeClientInfo {
            name: "eliot-discover".to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            title: Some("Eliot discovery".to_owned()),
        },
        capabilities: None,
    };
    let initialized = client
        .call(
            "initialize",
            serde_json::to_value(init).map_err(|_| invalid_page())?,
            deadline,
        )
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "native_readback",
                "Codex discovery initialize failed",
            )
        })?;
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
    client
        .notify("initialized", None, deadline)
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "native_readback",
                "Codex discovery initialized failed",
            )
        })?;
    Ok(initialized)
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

fn pinned_launch(launch: &NativeLaunch, store: &StoreContext) -> Result<NativeLaunch> {
    let root = store.root().to_str().ok_or_else(unverified_store)?;
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

fn validate_pin(selected: &StoreContext, pinned: &StoreContext) -> Result<()> {
    // Requirements/managed config outrank CLI overrides. An absent sessionFlags
    // origin or another effective root is not proof that this process honored the pin.
    if !pinned.pin_verified || selected.home() != pinned.home() || selected.root() != pinned.root()
    {
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
        archived: false,
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

fn request_binding(profile: &Value, target_environment: &Value, limit: usize) -> Result<String> {
    let context = json!({
        "profile": profile,
        // Resolved environment values contribute only to the digest, never the cursor payload.
        "target_environment": target_environment,
        "method": "thread/list",
        "sortKey": "updated_at",
        "sortDirection": "desc",
        "sourceKinds": SOURCE_KINDS,
        "modelProviders": [],
        "archived": false,
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
        CURSOR_PREFIX, Cursor, NativeLaunch, StoreContext, decode_cursor, fingerprint,
        native_scope, pinned_launch, request_binding, select_store, sqlite_environment,
        store_scope, validate_pin, validate_store,
    };
    use crate::native::types::NativeDiscoveryConfigReadResponse;
    use serde_json::json;
    use std::{ffi::OsString, path::Path};

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
        let binding = request_binding(&profile, &environment, 100).expect("original binding");
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
            request_binding(&profile, &changed_environment, 100).expect("changed binding");
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
        let pinned = pinned_launch(&launch, &selected).expect("pinned launch");
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
        assert!(validate_pin(&selected, &query).is_ok());
        assert_eq!(selected.scope, native_scope(home.to_str().expect("home")));
        assert_eq!(selected.binding, "original-selection");
        query.root = changed_selection.root;
        assert!(validate_pin(&selected, &query).is_err());
        query.root = selected.root.clone();
        query.pin_verified = false;
        assert!(validate_pin(&selected, &query).is_err());
    }
}
