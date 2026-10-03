//! Typed request validation for durable, scoped coordination watches.
//!
//! Watch matching is intentionally restricted to facts that already exist in
//! Store. This module has no runtime, mailbox, or scheduling side effects.

use crate::{
    error::{Error, Result},
    model,
};
use serde_json::Value;

pub const MAX_PAGE_SIZE: i64 = 50;
pub const DEFAULT_PAGE_SIZE: i64 = 20;
pub const MAX_WATCH_TTL_MS: i64 = 30 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone)]
pub(crate) struct CreateRequest {
    pub task_id: Option<String>,
    pub task_revision: Option<i64>,
    pub attempt_id: Option<String>,
    pub watch_kind: String,
    pub operation_id: String,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct CancelRequest {
    pub watch_id: String,
}

#[derive(Debug, Clone)]
pub(crate) enum Mutation {
    Create(CreateRequest),
    Cancel(CancelRequest),
}

#[derive(Debug, Clone)]
pub(crate) struct ListRequest {
    pub task_id: Option<String>,
    pub task_revision: Option<i64>,
    pub attempt_id: Option<String>,
    pub limit: i64,
    pub after_watch_id: Option<String>,
}

pub fn validate_mutation(method: &str, value: &Value) -> Result<()> {
    parse_mutation(method, value).map(|_| ())
}

pub(crate) fn parse_mutation(method: &str, value: &Value) -> Result<Mutation> {
    match method {
        "coordination.watch.create" => parse_create(value).map(Mutation::Create),
        "coordination.watch.cancel" => parse_cancel(value).map(Mutation::Cancel),
        _ => Err(Error::new(
            "METHOD_NOT_FOUND",
            format!("{method} is not a coordination watch mutation"),
        )),
    }
}

pub(crate) fn parse_list(value: &Value) -> Result<ListRequest> {
    model::fields(
        value,
        &[
            "task_id",
            "task_revision",
            "attempt_id",
            "limit",
            "after_watch_id",
        ],
    )?;
    let (task_id, task_revision, attempt_id) = parse_scope_fields(value)?;
    let limit = crate::coordination::parse_page(value.get("limit"), DEFAULT_PAGE_SIZE)?;
    let after_watch_id = optional_identifier(value.get("after_watch_id"), "after_watch_id")?;
    Ok(ListRequest {
        task_id,
        task_revision,
        attempt_id,
        limit,
        after_watch_id,
    })
}

fn parse_create(value: &Value) -> Result<CreateRequest> {
    model::fields(
        value,
        &[
            "client_request_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "watch_kind",
            "address",
            "expires_at_ms",
            "delivery",
            "one_shot",
        ],
    )?;
    let _client_request_id = model::text(value, "client_request_id")?;
    let (task_id, task_revision, attempt_id) = parse_scope_fields(value)?;
    let watch_kind = model::text(value, "watch_kind")?.to_owned();
    if watch_kind != "operation_terminal" {
        return Err(Error::new(
            "WATCH_KIND_UNSUPPORTED",
            format!("watch kind {watch_kind:?} has no authoritative Store fact source"),
        ));
    }
    let address = value
        .get("address")
        .ok_or_else(|| Error::invalid("address is required"))?;
    model::fields(address, &["operation_id"])?;
    let operation_id = identifier(model::text(address, "operation_id")?, "operation_id")?;
    let expires_at_ms = model::positive(value, "expires_at_ms")?;
    if model::text(value, "delivery")? != "mailbox_header" {
        return Err(Error::new(
            "WATCH_DELIVERY_UNSUPPORTED",
            "only silent mailbox_header delivery is currently supported",
        ));
    }
    if value.get("one_shot").and_then(Value::as_bool) != Some(true) {
        return Err(Error::invalid("one_shot must be true"));
    }
    Ok(CreateRequest {
        task_id,
        task_revision,
        attempt_id,
        watch_kind,
        operation_id,
        expires_at_ms,
    })
}

fn parse_cancel(value: &Value) -> Result<CancelRequest> {
    model::fields(value, &["client_request_id", "watch_id"])?;
    let _client_request_id = model::text(value, "client_request_id")?;
    Ok(CancelRequest {
        watch_id: identifier(model::text(value, "watch_id")?, "watch_id")?,
    })
}

fn parse_scope_fields(value: &Value) -> Result<(Option<String>, Option<i64>, Option<String>)> {
    let task_id = optional_identifier(value.get("task_id"), "task_id")?;
    let task_revision = match value.get("task_revision") {
        None | Some(Value::Null) => None,
        Some(_) => Some(model::positive(value, "task_revision")?),
    };
    let attempt_id = optional_identifier(value.get("attempt_id"), "attempt_id")?;
    let supplied = usize::from(task_id.is_some())
        + usize::from(task_revision.is_some())
        + usize::from(attempt_id.is_some());
    if supplied != 0 && supplied != 3 {
        return Err(Error::invalid(
            "task_id, task_revision, and attempt_id must be supplied together",
        ));
    }
    Ok((task_id, task_revision, attempt_id))
}

fn optional_identifier(value: Option<&Value>, name: &str) -> Result<Option<String>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => identifier(text, name).map(Some),
        Some(_) => Err(Error::invalid(format!(
            "{name} must be nonempty text or null"
        ))),
    }
}

fn identifier(value: &str, name: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > 128
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(format!(
            "{name} must be 1..=128 bytes without whitespace"
        )));
    }
    Ok(value.to_owned())
}
