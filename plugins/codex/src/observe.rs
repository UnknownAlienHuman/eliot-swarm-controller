//! Exact-subject, read-only Codex observation. This module never controls a subject or
//! publishes lifecycle events; it owns only its pinned reader process.

#[path = "rollout.rs"]
mod rollout;

use crate::{
    discovery::{
        StoreContext, fingerprint, finish_reader, milliseconds, open_pinned_reader, probe_store,
        project_subject,
    },
    native::{
        NativeClient, NativeLaunch, NativeProcess,
        types::{NativeHistoryItemsResponse, NativeObservedThreadReadResponse},
    },
};
use eliot_core::{Error, Result};
use eliot_plugin::{
    MAX_OBSERVATION_PAGE_BYTES, ObservationPage, ObservationQuery, ObserverAttachParams,
    ObserverDetachParams, ObserverSnapshotParams, protocol::MAX_OBSERVATION_CURSOR_BYTES,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};
use tokio::{task::JoinHandle, time::Instant};

const CURSOR_PREFIX: &str = "codex-history-v1:";
const MAX_NATIVE_CURSOR_BYTES: usize = 8 * 1024;
const MAX_ID_BYTES: usize = 256;
const MAX_PATH_BYTES: usize = 4096;
const MAX_PROFILE_BYTES: usize = 128 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryCursor {
    version: u8,
    binding: String,
    native_cursor: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryTurnsResponse {
    data: Vec<HistoryTurn>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryTurn {
    id: String,
    status: String,
    #[serde(default)]
    started_at: Option<i64>,
    #[serde(default)]
    completed_at: Option<i64>,
    #[serde(default)]
    duration_ms: Option<i64>,
    items: Vec<Value>,
    #[serde(default)]
    items_view: Option<String>,
}

struct SubjectRead {
    projection: Value,
    rollout_path: Option<PathBuf>,
    cli_version: String,
}

pub(crate) struct ObserverSession {
    params: ObserverAttachParams,
    store: StoreContext,
    rollout_path: Option<PathBuf>,
    rollout_cli_version: String,
    secrets: Vec<String>,
    reader: Option<NativeProcess>,
    drainer: Option<JoinHandle<()>>,
    attach_result: Value,
    detach_command_id: Option<String>,
    detach_result: Option<Value>,
    reader_closed: bool,
    shutting_down: bool,
}

impl ObserverSession {
    pub(crate) async fn attach(
        params: ObserverAttachParams,
        launch: NativeLaunch,
        secrets: Vec<String>,
        deadline: Instant,
    ) -> Result<(Self, Value)> {
        validate_attach(&params, &secrets)?;
        if encoded_len(&params.profile)? > MAX_PROFILE_BYTES
            || encoded_len(&params.target_environment)? > MAX_PROFILE_BYTES
        {
            return Err(too_large());
        }

        let store = probe_store(&launch, &params.target_environment, &secrets, deadline).await?;
        if store.scope() != params.native_scope {
            return Err(Error::new(
                "source_integrity",
                "Codex observation scope does not match its proven store",
            ));
        }

        let (mut reader, drainer) = open_pinned_reader(
            &launch,
            &params.target_environment,
            &secrets,
            &store,
            deadline,
        )
        .await?;
        let client = reader.client();
        let subject_read = match read_subject(&client, &params.native_ref, deadline).await {
            Ok(subject) => subject,
            Err(error) => {
                drop(client);
                if finish_reader(&mut reader, Some(drainer)).await.is_err() {
                    return Err(reader_cleanup_unknown());
                }
                return Err(error);
            }
        };
        drop(client);
        let SubjectRead {
            projection: subject,
            rollout_path,
            cli_version,
        } = subject_read;

        let metadata = match native_store_identity(&store).map(|identity| {
            json!({
                "subject": subject,
                "native_store_identity": identity,
            })
        }) {
            Ok(metadata) => metadata,
            Err(error) => {
                if finish_reader(&mut reader, Some(drainer)).await.is_err() {
                    return Err(reader_cleanup_unknown());
                }
                return Err(error);
            }
        };
        let attach_result = match bounded(
            json!({
                "agent_id": params.agent_id,
                "generation": params.generation,
                "command_id": params.command_id,
                "native_scope": params.native_scope,
                "native_ref": params.native_ref,
                "state": "attached",
                "observation_state": "unknown",
                "metadata": metadata,
                "coverage": {"kind": "native_metadata", "complete": true, "live": false},
            }),
            &secrets,
        ) {
            Ok(result) => result,
            Err(error) => {
                if finish_reader(&mut reader, Some(drainer)).await.is_err() {
                    return Err(reader_cleanup_unknown());
                }
                return Err(error);
            }
        };

        let session = Self {
            params,
            store,
            rollout_path,
            rollout_cli_version: cli_version,
            secrets,
            reader: Some(reader),
            drainer: Some(drainer),
            attach_result: attach_result.clone(),
            detach_command_id: None,
            detach_result: None,
            reader_closed: false,
            shutting_down: false,
        };
        Ok((session, attach_result))
    }

    /// Return the original successful attach result for an exact command retry.
    pub(crate) fn repeat_attach(&self, params: &ObserverAttachParams) -> Result<Value> {
        self.binding(
            &params.agent_id,
            params.generation,
            &params.native_scope,
            &params.native_ref,
        )?;
        if params.command_id != self.params.command_id
            || params.mode != self.params.mode
            || params.profile != self.params.profile
            || params.target_environment != self.params.target_environment
            || params.source_plan != self.params.source_plan
            || params.source_state != self.params.source_state
        {
            return Err(Error::new(
                "id_conflict",
                "Codex observer attach id was reused with another binding",
            ));
        }
        Ok(self.attach_result.clone())
    }

    pub(crate) async fn snapshot(
        &mut self,
        params: &ObserverSnapshotParams,
        deadline: Instant,
    ) -> Result<Value> {
        self.binding(
            &params.agent_id,
            params.generation,
            &params.native_scope,
            &params.native_ref,
        )?;
        if self.reader_closed || self.shutting_down || self.detach_command_id.is_some() {
            return Err(Error::new("not_found", "Codex observer reader is closed"));
        }
        params.observation.validate()?;
        let query = &params.observation;
        if !matches!(query.source.as_str(), "native" | "rollout") {
            return Err(unsupported());
        }
        match query.view.as_str() {
            "subject" if query.turn_id.is_some() || query.cursor.is_some() => {
                return Err(invalid());
            }
            "turns" if query.turn_id.is_some() => return Err(invalid()),
            "items" => validate_id(query.turn_id.as_deref().ok_or_else(invalid)?)?,
            _ => {}
        }
        if let Some(turn_id) = &query.turn_id {
            reject_immutable_secret(turn_id, &self.secrets)?;
        }

        let binding = self.page_binding(query)?;
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                "Codex observation deadline elapsed before read dispatch",
            ));
        }
        let client = self
            .reader
            .as_ref()
            .map(|reader| reader.client())
            .ok_or_else(|| Error::new("not_found", "Codex observer reader is unavailable"))?;
        let (entries, next_cursor, coverage) = if query.source == "native" {
            let cursor = decode_cursor(query.cursor.as_deref(), &binding)?;
            if let Some(cursor) = &cursor {
                reject_immutable_secret(&cursor.native_cursor, &self.secrets)?;
            }
            let (entries, next_cursor) = self
                .read_native_entries(&client, query, cursor.as_ref(), &binding, deadline)
                .await?;
            let complete = next_cursor.is_none();
            (
                entries,
                next_cursor,
                json!({
                    "kind": if query.view == "subject" {"native_metadata"} else {"persisted_history"},
                    "complete": complete,
                    "live": false,
                }),
            )
        } else {
            let path = self.rollout_path.clone().ok_or_else(rollout_unavailable)?;
            let home = self.store.home().to_path_buf();
            let native_ref = self.params.native_ref.clone();
            let cli_version = self.rollout_cli_version.clone();
            let binding = binding.clone();
            let secrets = self.secrets.clone();
            let std_deadline = deadline.into_std();
            let request = rollout::RolloutReadRequest {
                path,
                codex_home: home,
                native_ref,
                cli_version,
                query: (*query).clone(),
                binding,
                secrets,
                deadline: std_deadline,
            };
            let page = tokio::task::spawn_blocking(move || rollout::read_page(request))
                .await
                .map_err(|_| Error::new("unknown", "Codex rollout reader task failed"))??;
            (page.entries, page.next_cursor, page.coverage)
        };
        let page = ObservationPage {
            agent_id: self.params.agent_id.clone(),
            generation: self.params.generation,
            native_scope: self.params.native_scope.clone(),
            native_ref: self.params.native_ref.clone(),
            source: query.source.clone(),
            view: query.view.clone(),
            entries,
            next_cursor,
            coverage,
        };
        page.validate()?;
        bounded(
            serde_json::to_value(page).map_err(|_| integrity())?,
            &self.secrets,
        )
    }

    pub(crate) async fn detach(
        &mut self,
        params: &ObserverDetachParams,
        _deadline: Instant,
    ) -> Result<Value> {
        self.binding(
            &params.agent_id,
            params.generation,
            &params.native_scope,
            &params.native_ref,
        )?;
        validate_id(&params.command_id)?;
        reject_immutable_secret(&params.command_id, &self.secrets)?;

        if let Some(command_id) = &self.detach_command_id
            && command_id != &params.command_id
        {
            return Err(Error::new(
                "id_conflict",
                "Codex observer detach is bound to another command",
            ));
        }
        if let Some(result) = &self.detach_result {
            return Ok(result.clone());
        }
        self.detach_command_id = Some(params.command_id.clone());

        let state = if self.close_reader().await.is_ok() {
            "detached"
        } else {
            "unknown"
        };
        let result = bounded(
            json!({
                "agent_id": params.agent_id,
                "generation": params.generation,
                "command_id": params.command_id,
                "native_scope": params.native_scope,
                "native_ref": params.native_ref,
                "state": state,
                "observation_state": "unknown",
            }),
            &self.secrets,
        )?;
        self.detach_result = Some(result.clone());
        Ok(result)
    }

    pub(crate) async fn shutdown(&mut self, _deadline: Instant) -> Result<()> {
        self.shutting_down = true;
        self.close_reader().await
    }

    async fn close_reader(&mut self) -> Result<()> {
        let Some(reader) = self.reader.as_mut() else {
            self.reader_closed = true;
            return Ok(());
        };
        let drainer = self.drainer.take();
        finish_reader(reader, drainer).await?;
        self.reader = None;
        self.reader_closed = true;
        Ok(())
    }

    fn binding(&self, agent: &str, generation: i64, scope: &str, native_ref: &str) -> Result<()> {
        if agent != self.params.agent_id || generation != self.params.generation {
            return Err(Error::new(
                "stale_generation",
                "Codex observer identity does not match its attachment",
            ));
        }
        if scope != self.params.native_scope || native_ref != self.params.native_ref {
            return Err(Error::new(
                "source_integrity",
                "Codex observer subject does not match its attachment",
            ));
        }
        Ok(())
    }

    fn page_binding(&self, query: &ObservationQuery) -> Result<String> {
        let method = match (query.source.as_str(), query.view.as_str()) {
            ("rollout", _) => "rollout/jsonl",
            (_, "subject") => "thread/read",
            (_, "turns") => "thread/turns/list",
            (_, "items") => "thread/items/list",
            _ => return Err(invalid()),
        };
        let direction = if query.source == "rollout" {
            "asc"
        } else if query.view == "turns" {
            "desc"
        } else {
            "asc"
        };
        let mut context = json!({
            "domain": "eliot/codex/history/v1",
            "agent_id": self.params.agent_id,
            "generation": self.params.generation,
            "native_scope": self.store.scope(),
            "native_ref": self.params.native_ref,
            "profile": self.params.profile,
            "target_environment": self.params.target_environment,
            "store_binding": self.store.binding(),
            "source": query.source,
            "method": method,
            "view": query.view,
            "turn_id": query.turn_id,
            "limit": query.limit,
            "direction": direction,
            "items_view": "notLoaded",
        });
        if query.source == "rollout" {
            if let Some(object) = context.as_object_mut() {
                object.remove("items_view");
            }
            context["rollout_cli_version"] = json!(self.rollout_cli_version);
        }
        fingerprint(context)
    }

    async fn read_native_entries(
        &self,
        client: &NativeClient,
        query: &ObservationQuery,
        cursor: Option<&HistoryCursor>,
        binding: &str,
        deadline: Instant,
    ) -> Result<(Vec<Value>, Option<String>)> {
        let (entries, next_cursor) = match query.view.as_str() {
            "subject" => (
                vec![
                    read_subject(client, &self.params.native_ref, deadline)
                        .await?
                        .projection,
                ],
                None,
            ),
            "turns" => self.read_turns(client, query, cursor, deadline).await?,
            "items" => self.read_items(client, query, cursor, deadline).await?,
            _ => return Err(invalid()),
        };
        let next_cursor = match next_cursor {
            Some(next) => {
                validate_native_cursor(&next)?;
                if cursor.is_some_and(|cursor| cursor.native_cursor == next) {
                    return Err(integrity());
                }
                reject_immutable_secret(&next, &self.secrets)?;
                let encoded = format!(
                    "{CURSOR_PREFIX}{}",
                    serde_json::to_string(&HistoryCursor {
                        version: 1,
                        binding: binding.to_owned(),
                        native_cursor: next,
                    })
                    .map_err(|_| integrity())?
                );
                if encoded.len() > MAX_OBSERVATION_CURSOR_BYTES {
                    return Err(too_large());
                }
                Some(encoded)
            }
            None => None,
        };
        Ok((entries, next_cursor))
    }

    async fn read_turns(
        &self,
        client: &NativeClient,
        query: &ObservationQuery,
        cursor: Option<&HistoryCursor>,
        deadline: Instant,
    ) -> Result<(Vec<Value>, Option<String>)> {
        let raw = client
            .call(
                "thread/turns/list",
                json!({
                    "threadId": self.params.native_ref,
                    "cursor": cursor.map(|cursor| &cursor.native_cursor),
                    "limit": query.limit,
                    "sortDirection": "desc",
                    "itemsView": "notLoaded",
                }),
                deadline,
            )
            .await
            .map_err(history_error)?;
        let response: HistoryTurnsResponse =
            serde_json::from_value(raw).map_err(|_| integrity())?;
        if response.data.len() > query.limit {
            return Err(integrity());
        }
        let mut ids = HashSet::with_capacity(response.data.len());
        let mut entries = Vec::with_capacity(response.data.len());
        for turn in response.data {
            validate_id(&turn.id)?;
            if !ids.insert(turn.id.clone())
                || turn
                    .items_view
                    .as_deref()
                    .is_some_and(|view| view != "notLoaded")
                || !turn.items.is_empty()
                || !matches!(
                    turn.status.as_str(),
                    "completed" | "failed" | "interrupted" | "inProgress"
                )
                || turn.duration_ms.is_some_and(|duration| duration < 0)
            {
                return Err(integrity());
            }
            reject_immutable_secret(&turn.id, &self.secrets)?;
            let value = json!({
                "turn_id": turn.id,
                "status": if turn.status == "inProgress" {"in_progress"} else {&turn.status},
                "started_at": turn.started_at.map(milliseconds).transpose()?,
                "completed_at": turn.completed_at.map(milliseconds).transpose()?,
                "duration_ms": turn.duration_ms,
            });
            entries.push(value);
        }
        Ok((entries, response.next_cursor))
    }

    async fn read_items(
        &self,
        client: &NativeClient,
        query: &ObservationQuery,
        cursor: Option<&HistoryCursor>,
        deadline: Instant,
    ) -> Result<(Vec<Value>, Option<String>)> {
        let turn_id = query.turn_id.as_deref().ok_or_else(invalid)?;
        let raw = client
            .call(
                "thread/items/list",
                json!({
                    "threadId": self.params.native_ref,
                    "turnId": turn_id,
                    "cursor": cursor.map(|cursor| &cursor.native_cursor),
                    "limit": query.limit,
                    "sortDirection": "asc",
                }),
                deadline,
            )
            .await
            .map_err(history_error)?;
        let response: NativeHistoryItemsResponse =
            serde_json::from_value(raw).map_err(|_| integrity())?;
        if response.data.len() > query.limit {
            return Err(integrity());
        }
        let mut ids = HashSet::with_capacity(response.data.len());
        let mut entries = Vec::with_capacity(response.data.len());
        for entry in response.data {
            validate_id(&entry.turn_id)?;
            validate_id(&entry.item.id)?;
            if entry.turn_id != turn_id || !ids.insert(entry.item.id.clone()) {
                return Err(integrity());
            }
            reject_immutable_secret(&entry.turn_id, &self.secrets)?;
            reject_immutable_secret(&entry.item.id, &self.secrets)?;
            let value = json!({
                "turn_id": entry.turn_id,
                "item_id": entry.item.id,
                "kind": item_kind(&entry.item.kind),
                "status": entry.item.status.as_deref().map(item_status),
                "started_at": entry.started_at_ms,
                "completed_at": entry.completed_at_ms,
            });
            entries.push(value);
        }
        Ok((entries, response.next_cursor))
    }
}

fn validate_attach(params: &ObserverAttachParams, secrets: &[String]) -> Result<()> {
    if params.mode != "observe"
        || !params.profile.is_object()
        || !params.target_environment.is_object()
        || params.generation <= 0
    {
        return Err(invalid());
    }
    for value in [
        params.agent_id.as_str(),
        params.command_id.as_str(),
        params.native_scope.as_str(),
        params.native_ref.as_str(),
    ] {
        validate_id(value)?;
        reject_immutable_secret(value, secrets)?;
    }
    Ok(())
}

async fn read_subject(
    client: &NativeClient,
    native_ref: &str,
    deadline: Instant,
) -> Result<SubjectRead> {
    let raw = client
        .call(
            "thread/read",
            json!({"threadId": native_ref, "includeTurns": false}),
            deadline,
        )
        .await
        .map_err(history_error)?;
    let cli_version = raw
        .pointer("/thread/cliVersion")
        .and_then(Value::as_str)
        .filter(|version| !version.trim().is_empty())
        .ok_or_else(integrity)?
        .to_owned();
    validate_id(&cli_version)?;
    let response: NativeObservedThreadReadResponse =
        serde_json::from_value(raw).map_err(|_| integrity())?;
    if response.thread.metadata.id != native_ref {
        return Err(integrity());
    }
    if let Some(path) = &response.thread.path {
        validate_path(path)?;
    }
    let native_path = response.thread.path;
    let rollout_path = native_path.as_deref().map(PathBuf::from);
    let mut subject = serde_json::to_value(project_subject(response.thread.metadata)?)
        .map_err(|_| integrity())?;
    let object = subject.as_object_mut().ok_or_else(integrity)?;
    object.insert("native_path".to_owned(), json!(native_path));
    object.insert("cli_version".to_owned(), json!(cli_version.clone()));
    Ok(SubjectRead {
        projection: subject,
        rollout_path,
        cli_version,
    })
}

fn native_store_identity(store: &StoreContext) -> Result<Value> {
    let home = store.home().to_str().ok_or_else(unverified_store)?;
    let root = store.root().to_str().ok_or_else(unverified_store)?;
    validate_path(home)?;
    validate_path(root)?;
    validate_id(store.scope())?;
    validate_id(store.binding())?;
    Ok(json!({
        "scope": store.scope(),
        "binding": store.binding(),
        "home": home,
        "root": root,
    }))
}

fn decode_cursor(raw: Option<&str>, binding: &str) -> Result<Option<HistoryCursor>> {
    let Some(raw) = raw else { return Ok(None) };
    if raw.len() > MAX_OBSERVATION_CURSOR_BYTES {
        return Err(too_large());
    }
    let cursor: HistoryCursor = serde_json::from_str(
        raw.strip_prefix(CURSOR_PREFIX)
            .ok_or_else(cursor_mismatch)?,
    )
    .map_err(|_| cursor_mismatch())?;
    if cursor.native_cursor.trim().is_empty() || cursor.native_cursor.chars().any(char::is_control)
    {
        return Err(cursor_mismatch());
    }
    if cursor.native_cursor.len() > MAX_NATIVE_CURSOR_BYTES {
        return Err(too_large());
    }
    if cursor.version != 1 || cursor.binding != binding {
        return Err(cursor_mismatch());
    }
    Ok(Some(cursor))
}

fn item_kind(kind: &str) -> &str {
    match kind {
        "userMessage"
        | "hookPrompt"
        | "agentMessage"
        | "functionCallOutput"
        | "plan"
        | "reasoning"
        | "commandExecution"
        | "fileChange"
        | "mcpToolCall"
        | "dynamicToolCall"
        | "collabAgentToolCall"
        | "subAgentActivity"
        | "webSearch"
        | "imageView"
        | "sleep"
        | "imageGeneration"
        | "enteredReviewMode"
        | "exitedReviewMode"
        | "contextCompaction" => kind,
        _ => "unknown",
    }
}

fn item_status(status: &str) -> &str {
    match status {
        "inProgress" => "in_progress",
        "completed" | "failed" | "declined" | "interrupted" => status,
        _ => "unknown",
    }
}

fn validate_id(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > MAX_ID_BYTES || value.chars().any(char::is_control)
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_native_cursor(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(integrity());
    }
    if value.len() > MAX_NATIVE_CURSOR_BYTES {
        return Err(too_large());
    }
    Ok(())
}

fn validate_path(value: &str) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > MAX_PATH_BYTES
        || value.chars().any(char::is_control)
        || !Path::new(value).is_absolute()
    {
        return Err(integrity());
    }
    Ok(())
}

fn bounded(mut value: Value, secrets: &[String]) -> Result<Value> {
    sanitize_value(&mut value, secrets, false)?;
    if encoded_len(&value)? > MAX_OBSERVATION_PAGE_BYTES {
        return Err(too_large());
    }
    Ok(value)
}

fn sanitize_value(value: &mut Value, secrets: &[String], protected: bool) -> Result<()> {
    match value {
        Value::String(text) => sanitize_text(text, secrets, protected),
        Value::Array(values) => {
            for value in values {
                sanitize_value(value, secrets, protected)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                reject_immutable_secret(key, secrets)?;
                sanitize_value(value, secrets, protected || protected_key(key))?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn sanitize_text(value: &mut String, secrets: &[String], protected: bool) -> Result<()> {
    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
        let matches = if secret.len() < 4 {
            value.as_str() == secret.as_str()
        } else {
            value.contains(secret.as_str())
        };
        if !matches {
            continue;
        }
        if protected {
            return Err(secret_in_payload());
        }
        if secret.len() < 4 {
            *value = "[REDACTED]".to_owned();
        } else {
            *value = value.replace(secret.as_str(), "[REDACTED]");
        }
    }
    Ok(())
}

fn reject_immutable_secret(value: &str, secrets: &[String]) -> Result<()> {
    let mut value = value.to_owned();
    sanitize_text(&mut value, secrets, true)
}

fn protected_key(key: &str) -> bool {
    matches!(
        key,
        "agent_id"
            | "command_id"
            | "native_scope"
            | "native_ref"
            | "scope"
            | "binding"
            | "store_binding"
            | "home"
            | "root"
            | "turn_id"
            | "item_id"
            | "next_cursor"
            | "cursor"
    )
}

fn encoded_len(value: &Value) -> Result<usize> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| integrity())
}

fn history_error(error: Error) -> Error {
    let message = error.message.to_ascii_lowercase();
    if error.code == "native_rejected"
        && (message.contains("method not found") || message.contains("unknown method"))
    {
        return Error::new(
            "unsupported",
            "Codex native history read is unsupported by this app-server",
        );
    }
    Error::new(
        "history_gap",
        "Codex exact persisted readback is unavailable or unsupported",
    )
}

fn invalid() -> Error {
    Error::new("invalid", "Codex observation request is invalid")
}
fn integrity() -> Error {
    Error::new(
        "source_integrity",
        "Codex observation metadata does not match the exact subject or page",
    )
}
fn unsupported() -> Error {
    Error::new(
        "unsupported",
        "Codex observation source or native paging operation is unsupported",
    )
}
fn too_large() -> Error {
    Error::new(
        "frame_too_large",
        "Codex observation data exceeds its configured bound",
    )
}
fn cursor_mismatch() -> Error {
    Error::new(
        "cursor_mismatch",
        "Codex history cursor does not match its exact subject, generation, profile, store or query",
    )
}
fn rollout_unavailable() -> Error {
    Error::new(
        "history_gap",
        "Codex thread/read did not provide an exact rollout path",
    )
}
fn secret_in_payload() -> Error {
    Error::new(
        "secret_in_payload",
        "Codex observation binding contains a known secret",
    )
}
fn unverified_store() -> Error {
    Error::new(
        "unverified_store",
        "Codex observation store identity could not be represented exactly",
    )
}
fn reader_cleanup_unknown() -> Error {
    Error::new("unknown", "Codex observer reader cleanup is unconfirmed")
}
