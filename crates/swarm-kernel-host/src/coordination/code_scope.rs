//! Typed advisory code-scope inputs. These requests never lock files or run Git.

use crate::{
    error::{Error, Result},
    model,
};
use serde_json::Value;
use std::collections::BTreeSet;
use swarm_contracts::coordination_limits as limits;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeMode {
    ExclusiveEdit,
    SharedEdit,
    ReadReview,
}

impl ScopeMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ExclusiveEdit => "exclusive_edit",
            Self::SharedEdit => "shared_edit",
            Self::ReadReview => "read_review",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProposeRequest {
    pub client_request_id: String,
    pub task_id: String,
    pub task_revision: Option<i64>,
    pub attempt_id: String,
    pub assignment_id: Option<String>,
    pub mode: ScopeMode,
    pub paths: Vec<String>,
    pub symbols: Vec<String>,
    pub interfaces: Vec<String>,
    pub baseline_candidate_ref: String,
    pub reason: String,
    pub suggested_expires_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct AcceptRequest {
    pub client_request_id: String,
    pub scope_intent_id: String,
    pub expected_state_revision: i64,
    pub proposal_digest: String,
    pub mode: Option<ScopeMode>,
    pub paths: Option<Vec<String>>,
    pub symbols: Option<Vec<String>>,
    pub interfaces: Option<Vec<String>>,
    pub expires_at_ms: Option<i64>,
    pub reason: String,
    pub acknowledge_broad_scope: bool,
    pub override_scope_intent_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct ReleaseRequest {
    pub client_request_id: String,
    pub scope_intent_id: String,
    pub expected_state_revision: i64,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ReadRequest {
    pub task_id: String,
    pub task_revision: Option<i64>,
    pub attempt_id: Option<String>,
    pub scope_intent_id: Option<String>,
    pub client_id: Option<String>,
    pub path: Option<String>,
    pub symbol: Option<String>,
    pub interface: Option<String>,
    pub after_scope_id: Option<String>,
    pub limit: i64,
}

fn envelope(value: &Value, fields: &[&str]) -> Result<()> {
    model::fields(value, fields)?;
    if model::canonical(value)?.len() > limits::MAX_COORDINATION_REQUEST_BYTES {
        return Err(Error::new(
            "PAYLOAD_TOO_LARGE",
            "code-scope request exceeds the byte bound",
        ));
    }
    Ok(())
}

fn text(value: &Value, key: &str, max: usize) -> Result<String> {
    let text = model::text(value, key)?;
    if text.trim().is_empty() || text.len() > max || text.chars().any(char::is_control) {
        return Err(Error::invalid(format!(
            "{key} must be bounded nonblank text"
        )));
    }
    Ok(text.to_owned())
}

fn optional_text(value: &Value, key: &str, max: usize) -> Result<Option<String>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => text(value, key, max).map(Some),
    }
}

fn scope_id(value: &Value, key: &str) -> Result<String> {
    let id = text(value, key, limits::MAX_IDENTIFIER_BYTES)?;
    if id
        .strip_prefix("cscope-")
        .and_then(|tail| uuid::Uuid::parse_str(tail).ok())
        .is_none()
    {
        return Err(Error::invalid(format!(
            "{key} must be an exact cscope UUID"
        )));
    }
    Ok(id)
}

fn optional_scope_id(value: &Value, key: &str) -> Result<Option<String>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => scope_id(value, key).map(Some),
    }
}

fn positive_optional(value: &Value, key: &str) -> Result<Option<i64>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(_) => model::positive(value, key).map(Some),
    }
}

fn timestamp(value: &Value, key: &str) -> Result<Option<i64>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(raw) => raw
            .as_i64()
            .filter(|time| *time >= 0)
            .map(Some)
            .ok_or_else(|| {
                Error::invalid(format!("{key} must be a nonnegative timestamp or null"))
            }),
    }
}

fn mode(value: &Value, key: &str) -> Result<ScopeMode> {
    match model::text(value, key)? {
        "exclusive_edit" => Ok(ScopeMode::ExclusiveEdit),
        "shared_edit" => Ok(ScopeMode::SharedEdit),
        "read_review" => Ok(ScopeMode::ReadReview),
        _ => Err(Error::invalid(
            "scope mode must be exclusive_edit, shared_edit or read_review",
        )),
    }
}

/// Normalize separators only. Unsupported globs remain visible for conservative
/// unknown classification; normalization never grants a Git pathspec or scope.
pub(crate) fn normalize_path(input: &str) -> Result<String> {
    if input.trim().is_empty()
        || input.len() > limits::MAX_REFERENCE_BYTES
        || input.chars().any(char::is_control)
    {
        return Err(Error::invalid("scope path must be bounded nonblank text"));
    }
    let path = input.replace('\\', "/");
    if path.starts_with('/')
        || path.contains(':')
        || path.split('/').any(|part| {
            part.is_empty() || part == "." || part == ".." || part.eq_ignore_ascii_case(".git")
        })
    {
        return Err(Error::invalid(
            "scope path must be repository-relative without traversal or .git",
        ));
    }
    Ok(path)
}

fn terms(value: &Value, key: &str, paths: bool) -> Result<Vec<String>> {
    let values = value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid(format!("{key} must be an array")))?;
    let mut distinct = BTreeSet::new();
    for item in values {
        let item = item
            .as_str()
            .ok_or_else(|| Error::invalid(format!("{key} must contain text")))?;
        if item.trim().is_empty()
            || item.len() > limits::MAX_REFERENCE_BYTES
            || item.chars().any(char::is_control)
        {
            return Err(Error::invalid(format!(
                "{key} contains invalid bounded text"
            )));
        }
        distinct.insert(if paths {
            normalize_path(item)?
        } else {
            item.to_owned()
        });
    }
    Ok(distinct.into_iter().collect())
}

fn optional_terms(value: &Value, key: &str, paths: bool) -> Result<Option<Vec<String>>> {
    match value.get(key) {
        None => Ok(None),
        Some(_) => terms(value, key, paths).map(Some),
    }
}

pub(crate) fn parse_propose_request(value: &Value) -> Result<ProposeRequest> {
    envelope(
        value,
        &[
            "client_request_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "assignment_id",
            "mode",
            "paths",
            "symbols",
            "interfaces",
            "baseline_candidate_ref",
            "reason",
            "suggested_expires_at_ms",
        ],
    )?;
    let paths = terms(value, "paths", true)?;
    let symbols = terms(value, "symbols", false)?;
    let interfaces = terms(value, "interfaces", false)?;
    if paths.is_empty() && symbols.is_empty() && interfaces.is_empty() {
        return Err(Error::invalid(
            "a scope proposal must name a path, symbol or interface",
        ));
    }
    Ok(ProposeRequest {
        client_request_id: text(
            value,
            "client_request_id",
            limits::MAX_CLIENT_REQUEST_ID_BYTES,
        )?,
        task_id: text(value, "task_id", limits::MAX_IDENTIFIER_BYTES)?,
        task_revision: positive_optional(value, "task_revision")?,
        attempt_id: text(value, "attempt_id", limits::MAX_IDENTIFIER_BYTES)?,
        assignment_id: optional_text(value, "assignment_id", limits::MAX_IDENTIFIER_BYTES)?,
        mode: mode(value, "mode")?,
        paths,
        symbols,
        interfaces,
        baseline_candidate_ref: text(value, "baseline_candidate_ref", limits::MAX_REFERENCE_BYTES)?,
        reason: text(value, "reason", limits::MAX_REASON_BYTES)?,
        suggested_expires_at_ms: timestamp(value, "suggested_expires_at_ms")?,
    })
}

pub(crate) fn parse_accept_request(value: &Value) -> Result<AcceptRequest> {
    envelope(
        value,
        &[
            "client_request_id",
            "scope_intent_id",
            "expected_state_revision",
            "proposal_digest",
            "mode",
            "paths",
            "symbols",
            "interfaces",
            "expires_at_ms",
            "reason",
            "acknowledge_broad_scope",
            "override_scope_intent_ids",
        ],
    )?;
    let proposal_digest = text(value, "proposal_digest", 64)?;
    if proposal_digest.len() != 64
        || !proposal_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::invalid(
            "proposal_digest must be a lowercase SHA-256",
        ));
    }
    let acknowledge_broad_scope = match value.get("acknowledge_broad_scope") {
        None => false,
        Some(raw) => raw
            .as_bool()
            .ok_or_else(|| Error::invalid("acknowledge_broad_scope must be boolean"))?,
    };
    let override_scope_intent_ids = match value.get("override_scope_intent_ids") {
        None => Vec::new(),
        Some(_) => {
            let ids = terms(value, "override_scope_intent_ids", false)?;
            for id in &ids {
                let raw = serde_json::json!({"id":id});
                scope_id(&raw, "id")?;
            }
            ids
        }
    };
    Ok(AcceptRequest {
        client_request_id: text(
            value,
            "client_request_id",
            limits::MAX_CLIENT_REQUEST_ID_BYTES,
        )?,
        scope_intent_id: scope_id(value, "scope_intent_id")?,
        expected_state_revision: model::positive(value, "expected_state_revision")?,
        proposal_digest,
        mode: if value.get("mode").is_some() {
            Some(mode(value, "mode")?)
        } else {
            None
        },
        paths: optional_terms(value, "paths", true)?,
        symbols: optional_terms(value, "symbols", false)?,
        interfaces: optional_terms(value, "interfaces", false)?,
        expires_at_ms: timestamp(value, "expires_at_ms")?,
        reason: text(value, "reason", limits::MAX_REASON_BYTES)?,
        acknowledge_broad_scope,
        override_scope_intent_ids,
    })
}

pub(crate) fn parse_release_request(value: &Value) -> Result<ReleaseRequest> {
    envelope(
        value,
        &[
            "client_request_id",
            "scope_intent_id",
            "expected_state_revision",
            "reason",
        ],
    )?;
    Ok(ReleaseRequest {
        client_request_id: text(
            value,
            "client_request_id",
            limits::MAX_CLIENT_REQUEST_ID_BYTES,
        )?,
        scope_intent_id: scope_id(value, "scope_intent_id")?,
        expected_state_revision: model::positive(value, "expected_state_revision")?,
        reason: text(value, "reason", limits::MAX_REASON_BYTES)?,
    })
}

pub(crate) fn parse_read_request(value: &Value) -> Result<ReadRequest> {
    envelope(
        value,
        &[
            "task_id",
            "task_revision",
            "attempt_id",
            "scope_intent_id",
            "client_id",
            "path",
            "symbol",
            "interface",
            "after_scope_id",
            "limit",
        ],
    )?;
    let limit = match value.get("limit") {
        None => limits::DEFAULT_READ_PAGE_SIZE,
        Some(raw) => raw
            .as_i64()
            .filter(|limit| (1..=limits::MAX_READ_PAGE_SIZE).contains(limit))
            .ok_or_else(|| Error::invalid("limit must be from 1 through 50"))?,
    };
    let path = optional_text(value, "path", limits::MAX_REFERENCE_BYTES)?
        .map(|path| normalize_path(&path))
        .transpose()?;
    Ok(ReadRequest {
        task_id: text(value, "task_id", limits::MAX_IDENTIFIER_BYTES)?,
        task_revision: positive_optional(value, "task_revision")?,
        attempt_id: optional_text(value, "attempt_id", limits::MAX_IDENTIFIER_BYTES)?,
        scope_intent_id: optional_scope_id(value, "scope_intent_id")?,
        client_id: optional_text(value, "client_id", limits::MAX_CLIENT_ID_BYTES)?,
        path,
        symbol: optional_text(value, "symbol", limits::MAX_REFERENCE_BYTES)?,
        interface: optional_text(value, "interface", limits::MAX_REFERENCE_BYTES)?,
        after_scope_id: optional_scope_id(value, "after_scope_id")?,
        limit,
    })
}

pub(crate) fn validate_mutation(method: &str, value: &Value) -> Result<()> {
    match method {
        "code.scope.propose" => {
            parse_propose_request(value)?;
        }
        "code.scope.accept" => {
            parse_accept_request(value)?;
        }
        "code.scope.release" => {
            parse_release_request(value)?;
        }
        _ => return Err(Error::new("METHOD_NOT_FOUND", method)),
    }
    Ok(())
}
