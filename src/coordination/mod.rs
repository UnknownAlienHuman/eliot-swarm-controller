//! Assignment-scoped coordination primitives and bounded relevance keys.
//!
//! Coordination is durable local state. None of the helpers in this module
//! starts a model turn, changes a Task assignment, or grants authority.

use crate::{
    error::{Error, Result},
    model,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_CARD_BYTES: usize = 32 * 1024;
pub const MAX_MESSAGE_BYTES: usize = 8 * 1024;
pub const MAX_PAGE_SIZE: i64 = 50;
pub const MAX_INBOX_SCAN: i64 = 1000;

/// Closed, side-effect-free validation for the supported coordination
/// mutations. Store authorization and current Task/Attempt checks remain the
/// authority at application time.
pub fn validate_mutation(method: &str, value: &Value) -> Result<()> {
    let allowed: &[&str] = match method {
        "coordination.participant.register" => &[
            "client_request_id",
            "client_id",
            "token_hash",
            "task_id",
            "task_revision",
            "attempt_id",
            "participation_basis",
            "binding_id",
            "binding_generation",
            "native_session_id",
            "display_alias",
            "inbound_policy",
            "review_profile",
        ],
        "coordination.participant.disable" => {
            &["client_request_id", "client_id", "expected_grant_revision"]
        }
        "coordination.work_card.publish" => &["client_request_id", "fields"],
        "coordination.work_card.withdraw" => &["client_request_id"],
        "coordination.contract_card.publish" => &["client_request_id", "contract_key", "fields"],
        "coordination.contract_card.withdraw" => &["client_request_id", "contract_key"],
        "coordination.send" => &["client_request_id", "recipient", "body"],
        _ => return Err(Error::new("METHOD_NOT_FOUND", method)),
    };
    model::fields(value, allowed)?;
    model::text(value, "client_request_id")?;
    match method {
        "coordination.participant.register" => {
            let client_id = model::text(value, "client_id")?;
            let token_hash = model::text(value, "token_hash")?;
            if client_id.is_empty()
                || client_id.len() > 128
                || client_id
                    .bytes()
                    .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            {
                return Err(Error::invalid(
                    "client_id must be 1..=128 bytes without whitespace",
                ));
            }
            if token_hash.len() != 64 || !token_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(Error::invalid("token_hash must be SHA-256 hex"));
            }
            model::text(value, "task_id")?;
            model::positive(value, "task_revision")?;
            model::text(value, "attempt_id")?;
            let basis = value
                .get("participation_basis")
                .ok_or_else(|| Error::invalid("participation_basis is required"))?;
            model::fields(basis, &["kind", "assignment_id", "review_scope"])?;
            match model::text(basis, "kind")? {
                "attempt_owner" => {
                    if basis
                        .get("assignment_id")
                        .is_some_and(|item| !item.is_null())
                        || basis
                            .get("review_scope")
                            .is_some_and(|item| !item.is_null())
                    {
                        return Err(Error::invalid(
                            "attempt_owner basis cannot carry assignment or review scope",
                        ));
                    }
                }
                "producer_ref" => {
                    model::text(basis, "assignment_id")?;
                    if basis
                        .get("review_scope")
                        .is_some_and(|item| !item.is_null())
                    {
                        return Err(Error::invalid(
                            "producer_ref basis cannot carry review_scope",
                        ));
                    }
                }
                "sponsored_reviewer" => {
                    if basis
                        .get("assignment_id")
                        .is_some_and(|item| !item.is_null())
                    {
                        return Err(Error::invalid(
                            "sponsored_reviewer cannot name a producer assignment",
                        ));
                    }
                    let scope = basis.get("review_scope").ok_or_else(|| {
                        Error::invalid("sponsored_reviewer requires review_scope")
                    })?;
                    model::fields(
                        scope,
                        &[
                            "review_assignment_id",
                            "task_id",
                            "attempt_id",
                            "task_revision",
                            "submission_ref",
                            "candidate_ref",
                        ],
                    )?;
                    if !scope
                        .get("review_assignment_id")
                        .ok_or_else(|| {
                            Error::invalid(
                                "review_assignment_id must be explicitly null before binding",
                            )
                        })?
                        .is_null()
                    {
                        return Err(Error::invalid(
                            "pre-registration review_assignment_id must be null",
                        ));
                    }
                    model::text(scope, "task_id")?;
                    model::text(scope, "attempt_id")?;
                    model::positive(scope, "task_revision")?;
                    model::text(scope, "submission_ref")?;
                    model::text(scope, "candidate_ref")?;
                }
                _ => return Err(Error::invalid("unsupported participation_basis.kind")),
            }
            for name in [
                "binding_id",
                "native_session_id",
                "display_alias",
                "inbound_policy",
                "review_profile",
            ] {
                if let Some(item) = value.get(name)
                    && !item.is_null()
                    && !item.as_str().is_some_and(|text| !text.trim().is_empty())
                {
                    return Err(Error::invalid(format!(
                        "{name} must be nonempty text or null"
                    )));
                }
            }
            match (value.get("binding_id"), value.get("binding_generation")) {
                (None | Some(Value::Null), None | Some(Value::Null)) => {}
                (Some(Value::String(binding)), Some(_)) if !binding.trim().is_empty() => {
                    model::positive(value, "binding_generation")?;
                }
                _ => {
                    return Err(Error::invalid(
                        "binding_id and binding_generation must be supplied together",
                    ));
                }
            }
            if let Some(policy) = value.get("inbound_policy").and_then(Value::as_str)
                && !["pull_only", "safe_boundary", "hold", "refuse"].contains(&policy)
            {
                return Err(Error::invalid("unsupported inbound_policy"));
            }
            Ok(())
        }
        "coordination.participant.disable" => {
            model::text(value, "client_id")?;
            if value.get("expected_grant_revision").is_some() {
                model::positive(value, "expected_grant_revision")?;
            }
            Ok(())
        }
        "coordination.work_card.publish" => validate_card_fields(
            "work",
            value
                .get("fields")
                .ok_or_else(|| Error::invalid("fields is required"))?,
        ),
        "coordination.contract_card.publish" => {
            let key = model::text(value, "contract_key")?;
            if key.len() > 256 || key.bytes().any(|byte| byte.is_ascii_control()) {
                return Err(Error::invalid(
                    "contract_key must be at most 256 bytes without controls",
                ));
            }
            validate_card_fields(
                "contract",
                value
                    .get("fields")
                    .ok_or_else(|| Error::invalid("fields is required"))?,
            )
        }
        "coordination.contract_card.withdraw" => {
            let key = model::text(value, "contract_key")?;
            if key.len() > 256 || key.bytes().any(|byte| byte.is_ascii_control()) {
                return Err(Error::invalid(
                    "contract_key must be at most 256 bytes without controls",
                ));
            }
            Ok(())
        }
        "coordination.work_card.withdraw" => Ok(()),
        "coordination.send" => {
            let recipient = model::text(value, "recipient")?;
            if recipient.is_empty()
                || recipient.len() > 128
                || recipient
                    .bytes()
                    .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            {
                return Err(Error::invalid(
                    "recipient must be 1..=128 bytes without whitespace",
                ));
            }
            let body = value
                .get("body")
                .filter(|body| !body.is_null())
                .ok_or_else(|| Error::invalid("body must be a non-null JSON value"))?;
            let bytes = model::canonical(body)?.len();
            if bytes > MAX_MESSAGE_BYTES {
                return Err(Error::invalid(format!(
                    "body exceeds the {MAX_MESSAGE_BYTES}-byte limit"
                )));
            }
            Ok(())
        }
        _ => unreachable!("method was checked above"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TermKind {
    Contract,
    Path,
    Symbol,
    Interface,
}

impl TermKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Contract => "contract",
            Self::Path => "path",
            Self::Symbol => "symbol",
            Self::Interface => "interface",
        }
    }
}

/// A collision-resistant partition key for one exact Task/Attempt edition.
pub(crate) fn scope_id(task_id: &str, task_revision: i64, attempt_id: &str) -> Result<String> {
    let value = serde_json::json!({
        "task_id": task_id,
        "task_revision": task_revision,
        "attempt_id": attempt_id,
    });
    Ok(model::digest(model::canonical(&value)?.as_bytes()))
}

/// Hex encoding keeps opaque participant IDs out of SQLite key syntax and
/// makes exact prefix scans safe without LIKE escaping.
pub(crate) fn key_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

pub(crate) fn participant_prefix(scope: &str) -> String {
    format!("coordination:participant:{scope}:")
}

pub(crate) fn mailbox_prefix(scope: &str, recipient: &str) -> String {
    format!("coordination:mailbox:{scope}:{}:", key_component(recipient))
}

pub(crate) fn mailbox_key(
    scope: &str,
    recipient: &str,
    created_at_ms: i64,
    operation_id: &str,
) -> String {
    format!(
        "{}{created_at_ms:020}:{}",
        mailbox_prefix(scope, recipient),
        key_component(operation_id)
    )
}

pub(crate) fn card_prefix(scope: &str) -> String {
    format!("coordination:card:{scope}:")
}

pub(crate) fn relevance_prefix(scope: &str, kind: TermKind, term: &str) -> String {
    format!(
        "coordination:relevance:{scope}:{}:{}:",
        kind.as_str(),
        key_component(term)
    )
}

pub(crate) fn card_key(scope: &str, card_kind: &str, identity: &str, client_id: &str) -> String {
    format!(
        "{}{}:{}:{}",
        card_prefix(scope),
        card_kind,
        key_component(identity),
        key_component(client_id)
    )
}

pub(crate) fn participant_key(scope: &str, client_id: &str) -> String {
    format!(
        "{}{client}",
        participant_prefix(scope),
        client = key_component(client_id)
    )
}

pub(crate) fn pending_review_profile_prefix(
    review_scope: &Value,
    sponsor_client_id: &str,
    profile: &str,
) -> Result<String> {
    let digest = model::digest(model::canonical(review_scope)?.as_bytes());
    Ok(format!(
        "coordination:review-profile:{digest}:{}:{}:",
        key_component(sponsor_client_id),
        key_component(profile),
    ))
}

pub(crate) fn pending_review_profile_key(
    review_scope: &Value,
    sponsor_client_id: &str,
    profile: &str,
    client_id: &str,
) -> Result<String> {
    Ok(format!(
        "{}{client}",
        pending_review_profile_prefix(review_scope, sponsor_client_id, profile)?,
        client = key_component(client_id),
    ))
}

pub(crate) fn relevance_key(
    scope: &str,
    kind: TermKind,
    term: &str,
    client_id: &str,
    card_kind: &str,
    identity: &str,
) -> String {
    format!(
        "{}{}:{}:{}",
        relevance_prefix(scope, kind, term),
        key_component(client_id),
        card_kind,
        key_component(identity)
    )
}

pub(crate) fn validate_card_size(value: &Value) -> Result<()> {
    let bytes = model::canonical(value)?.len();
    if bytes > MAX_CARD_BYTES {
        return Err(Error::invalid(format!(
            "card content exceeds the {MAX_CARD_BYTES}-byte limit"
        )));
    }
    Ok(())
}

pub(crate) fn validate_card_fields(kind: &str, value: &Value) -> Result<()> {
    let allowed: &[&str] = match kind {
        "work" => &[
            "provides",
            "requires",
            "planned_scopes",
            "assumptions",
            "known_contract_gaps",
            "integration_points",
            "contract_keys",
            "paths",
            "symbols",
            "interfaces",
        ],
        "contract" => &[
            "role",
            "version",
            "producer",
            "consumer",
            "carrier",
            "contract",
            "inputs",
            "outputs",
            "serialization",
            "ownership",
            "availability",
            "limits",
            "result_disposition",
            "retry_semantics",
            "canonical_sources",
            "paths",
            "symbols",
            "interfaces",
        ],
        _ => return Err(Error::invalid("unknown card kind")),
    };
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid("fields must be an object"))?;
    for name in object.keys() {
        if !allowed.contains(&name.as_str()) {
            return Err(Error::invalid(format!("unknown {kind} card field: {name}")));
        }
    }
    if kind == "contract"
        && let Some(role) = value.get("role")
        && !matches!(role.as_str(), Some("producer" | "consumer" | "carrier"))
    {
        return Err(Error::invalid(
            "contract card role must be producer, consumer, or carrier",
        ));
    }
    for name in [
        "provides",
        "requires",
        "planned_scopes",
        "assumptions",
        "known_contract_gaps",
        "integration_points",
        "contract_keys",
        "paths",
        "symbols",
        "interfaces",
        "canonical_sources",
    ] {
        if let Some(items) = value.get(name) {
            if !items.is_array() {
                return Err(Error::invalid(format!("{name} must be an array")));
            }
            let items = items.as_array().expect("array was checked");
            if items.len() > 100 {
                return Err(Error::invalid(format!(
                    "{name} may contain at most 100 items"
                )));
            }
            for item in items {
                if matches!(name, "planned_scopes" | "integration_points") && item.is_object() {
                    let fields = item.as_object().expect("object was checked");
                    if fields.is_empty()
                        || fields.keys().any(|field| {
                            !["contract_key", "path", "symbol", "interface"]
                                .contains(&field.as_str())
                        })
                        || fields
                            .values()
                            .any(|term| !term.as_str().is_some_and(|term| !term.trim().is_empty()))
                    {
                        return Err(Error::invalid(format!(
                            "{name} object entries require exact nonempty relationship fields"
                        )));
                    }
                } else if !item.as_str().is_some_and(|text| !text.trim().is_empty()) {
                    return Err(Error::invalid(format!(
                        "{name} entries must be nonempty strings or exact relationship objects"
                    )));
                }
            }
        }
    }
    validate_card_size(value)
}

/// Extract only exact structured relationships. Free-form status text is not
/// interpreted as an ownership edge.
pub(crate) fn indexed_terms(
    fields: &Value,
    card_kind: &str,
) -> BTreeMap<TermKind, BTreeSet<String>> {
    let mut terms = BTreeMap::<TermKind, BTreeSet<String>>::new();
    let mut add = |kind: TermKind, value: &str| {
        if !value.trim().is_empty() {
            terms.entry(kind).or_default().insert(value.to_owned());
        }
    };
    for name in ["contract_keys", "provides", "requires"] {
        for item in string_terms(fields.get(name)) {
            add(TermKind::Contract, item);
        }
    }
    if card_kind == "contract"
        && let Some(contract_key) = fields.get("contract_key").and_then(Value::as_str)
    {
        add(TermKind::Contract, contract_key);
    }
    for name in ["paths", "symbols", "interfaces"] {
        let kind = match name {
            "paths" => TermKind::Path,
            "symbols" => TermKind::Symbol,
            _ => TermKind::Interface,
        };
        for item in string_terms(fields.get(name)) {
            add(kind, item);
        }
    }
    for item in fields
        .get("planned_scopes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(path) = item.as_str() {
            add(TermKind::Path, path);
            continue;
        }
        for (field, kind) in [
            ("path", TermKind::Path),
            ("symbol", TermKind::Symbol),
            ("interface", TermKind::Interface),
            ("contract_key", TermKind::Contract),
        ] {
            if let Some(term) = item.get(field).and_then(Value::as_str) {
                add(kind, term);
            }
        }
    }
    for field in ["contract_key", "path", "symbol", "interface"] {
        let Some(items) = fields.get("integration_points").and_then(Value::as_array) else {
            break;
        };
        let kind = match field {
            "contract_key" => TermKind::Contract,
            "path" => TermKind::Path,
            "symbol" => TermKind::Symbol,
            _ => TermKind::Interface,
        };
        for item in items {
            if let Some(term) = item.get(field).and_then(Value::as_str) {
                add(kind, term);
            }
        }
    }
    terms
}

fn string_terms(value: Option<&Value>) -> Vec<&str> {
    value
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|term| !term.trim().is_empty())
        .collect()
}

pub(crate) fn parse_page(limit: Option<&Value>, default: i64) -> Result<i64> {
    let limit = match limit {
        None => default,
        Some(value) => value
            .as_i64()
            .ok_or_else(|| Error::invalid("limit must be an integer"))?,
    };
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(Error::invalid(format!(
            "limit must be in 1..={MAX_PAGE_SIZE}"
        )));
    }
    Ok(limit)
}

pub(crate) fn optional_cursor(value: &Value, name: &str) -> Result<Option<String>> {
    match value.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.len() <= 256 => Ok(Some(value.clone())),
        Some(_) => Err(Error::invalid(format!("{name} must be a string or null"))),
    }
}
