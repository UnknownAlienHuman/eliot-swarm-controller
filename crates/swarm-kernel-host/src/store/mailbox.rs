//! Immutable mailbox admission shared by legacy and typed coordination sends.
use super::{meta, operations};
use crate::{
    error::{Error, Result},
    model::{self, Principal},
};
use rusqlite::{Connection, OptionalExtension, Transaction};
use serde_json::{Value, json};

const DELIVERY_ID_ALLOCATION_ATTEMPTS: usize = 16;

/// A single authenticated coordination sender and exactly one recipient.
pub(super) struct DeliveryRequest<'a> {
    pub(super) sender: &'a Principal,
    pub(super) recipient_id: &'a str,
    pub(super) payload_kind: &'static str,
    pub(super) payload_version: u32,
    pub(super) payload: Value,
    pub(super) message_id: String,
    pub(super) reply_to: Value,
    pub(super) admission_deadline_ms: Value,
    pub(super) delivery_deadline_ms: Value,
    pub(super) reply_deadline_ms: Value,
}

/// Admit one typed payload and derive its digest at the mailbox authority boundary.
/// Callers validate Thread membership; mailbox revalidates reply identity and direction.
pub(super) fn admit_delivery(
    tx: &Transaction<'_>,
    operation_id: &str,
    request: DeliveryRequest<'_>,
) -> Result<Value> {
    if !request.payload.is_object()
        || request.payload_kind.is_empty()
        || request.payload.get("recipients").is_some()
    {
        return Err(Error::invalid(
            "mailbox delivery requires one typed payload and one recipient",
        ));
    }
    if request.payload_version == 0 {
        return Err(Error::invalid("mailbox payload version must be positive"));
    }
    if !request.message_id.starts_with("cmsg-") || request.message_id == operation_id {
        return Err(Error::invalid(
            "coordination message identity must be distinct from its Operation identity",
        ));
    }
    if request.payload.get("message_id").and_then(Value::as_str)
        != Some(request.message_id.as_str())
    {
        return Err(Error::invalid(
            "typed payload message_id must match the delivery message identity",
        ));
    }
    if request.recipient_id.is_empty() {
        return Err(Error::invalid(
            "mailbox recipient must be an exact client id",
        ));
    }
    model::text(&request.payload, "thread_id")?;
    let facts = delivery_facts(tx, &request.sender.client_id, request.recipient_id)?;
    if request.payload["sender_actor"] != facts.actor
        || request.payload["recipient_actor"] != facts.recipient_actor
    {
        return Err(Error::invalid(
            "typed payload actors must match the authenticated parties",
        ));
    }
    validate_typed_reply(tx, &request)?;
    let payload_digest = format!(
        "sha256:{}",
        model::digest(model::canonical(&request.payload)?.as_bytes())
    );

    admit_delivery_core(
        tx,
        operation_id,
        DeliveryCoreRequest {
            sender_id: request.sender.client_id.clone(),
            recipient_id: request.recipient_id.to_owned(),
            body: DeliveryBody::Typed {
                message_id: request.message_id,
                payload_kind: request.payload_kind,
                payload_version: request.payload_version,
                payload: request.payload,
                payload_digest,
                reply_to: request.reply_to,
                admission_deadline_ms: request.admission_deadline_ms,
                delivery_deadline_ms: request.delivery_deadline_ms,
                reply_deadline_ms: request.reply_deadline_ms,
            },
        },
        facts,
    )
}

/// The historical message.send adapter. Its digest basis and receipt fields
/// stay byte-for-byte equivalent at the JSON-value level to the old path.
pub(super) fn apply_message_send(
    tx: &Transaction<'_>,
    sender_id: &str,
    value: &Value,
    operation_id: &str,
) -> Result<Value> {
    model::fields(
        value,
        &[
            "client_request_id",
            "recipient",
            "text",
            "in_reply_to",
            "in_reply_to_digest",
            "admission_deadline_ms",
            "delivery_deadline_ms",
            "reply_deadline_ms",
        ],
    )?;
    let recipient = model::text(value, "recipient")?;
    let body = model::text(value, "text")?;
    let facts = delivery_facts(tx, sender_id, recipient)?;
    let payload_digest = model::message_payload_digest(sender_id, recipient, body)?;

    let mut reply_to = Value::Null;
    if let Some(reply) = value.get("in_reply_to").and_then(Value::as_str) {
        // A reply addresses the original delivery by delivery_id where present,
        // otherwise by the historical operation id.
        let prior = match find_delivery(tx, reply)? {
            Some(original) => original,
            None => operations::get_operation(tx, reply)?,
        };
        if !legacy_reply_matches(&prior, sender_id, recipient, reply) {
            return Err(Error::invalid(
                "reply does not match the sender and recipient of that message",
            ));
        }
        model::verify_payload_digest_claim(
            prior["result"]["payload_digest"].as_str(),
            value.get("in_reply_to_digest").and_then(Value::as_str),
        )?;
        reply_to = model::message_reply_reference(&prior["result"]);
    }

    admit_delivery_core(
        tx,
        operation_id,
        DeliveryCoreRequest {
            sender_id: sender_id.to_owned(),
            recipient_id: recipient.to_owned(),
            body: DeliveryBody::LegacyText {
                text: body.to_owned(),
                payload_digest,
                in_reply_to: value.get("in_reply_to").cloned().unwrap_or(Value::Null),
                reply_to,
                admission_deadline_ms: model::deadline(value, "admission_deadline_ms")?,
                delivery_deadline_ms: model::deadline(value, "delivery_deadline_ms")?,
                reply_deadline_ms: model::deadline(value, "reply_deadline_ms")?,
            },
        },
        facts,
    )
}

/// Finds only immutable mailbox Operations that actually retain a delivery ID.
/// Legacy coordination wrappers remain addressable where their settled receipt
/// contains that identity; feedback and check facts have no admitted path here.
pub(super) fn find_delivery(db: &Connection, delivery_id: &str) -> Result<Option<Value>> {
    // Fail-closed sentinel first (same scope as the legacy lookup): the
    // pre-index query evaluated bare json_* over every in-scope settled
    // receipt, so malformed result_json in scope THREW instead of answering
    // absent. This bounded probe over mailbox_settled_invalid_result (015)
    // re-raises that authentic SQLite error before any lookup answer. NULL
    // result_json is not malformed and keeps its absent behavior. The added
    // CASE equality below is byte-identical to the
    // mailbox_settled_delivery_identity index expression; the retained
    // json_type/length/json_extract predicates keep typed-text semantics
    // unchanged while the CASE predicate provides the seek.
    let _: Option<String> = db
        .query_row(
            "SELECT json_extract(result_json,'$.delivery_id') FROM operations WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state='settled' AND result_json IS NOT NULL AND NOT json_valid(result_json) LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    let mut statement = db.prepare(
        "SELECT operation_id FROM operations \
         WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') \
           AND state='settled' \
           AND json_type(result_json,'$.delivery_id')='text' \
           AND length(json_extract(result_json,'$.delivery_id'))>0 \
           AND json_extract(result_json,'$.delivery_id')=?1 \
           AND CASE WHEN json_valid(result_json) THEN json_extract(result_json, '$.delivery_id') END=?1 \
         ORDER BY operation_id LIMIT 2",
    )?;
    let mut rows = statement.query([delivery_id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let operation_id: String = row.get(0)?;
    if rows.next()?.is_some() {
        return Err(Error::new(
            "MAILBOX_DELIVERY_AMBIGUOUS",
            "delivery identity resolves to more than one immutable Operation",
        ));
    }
    Ok(Some(operations::get_operation(db, &operation_id)?))
}

/// Cancellation is a separate immutable Operation bound to the exact delivery
/// identity and digest. It never rewrites the original delivery or workflow.
pub(super) fn cancel_message(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
) -> Result<Value> {
    model::fields(
        value,
        &[
            "client_request_id",
            "delivery_id",
            "payload_digest",
            "reason",
        ],
    )?;
    let delivery_id = model::text(value, "delivery_id")?;
    let claimed = model::text(value, "payload_digest")?;
    let original = find_delivery(tx, delivery_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", format!("Delivery {delivery_id}")))?;
    if original["result"]["sender"] != principal.client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "only the original sender can cancel a delivery",
        ));
    }
    model::verify_payload_digest_claim(
        original["result"]["payload_digest"].as_str(),
        Some(claimed),
    )?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM operations WHERE method='message.cancel' AND state='settled' \
             AND json_extract(result_json,'$.cancellation.delivery_id')=?1",
            [delivery_id],
            |row| row.get(0),
        )
        .optional()?;
    if existing.is_some() {
        return Err(Error::conflict("delivery is already cancelled"));
    }
    let sender_registration = meta(tx, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "sender is not registered"))?;
    Ok(json!({
        "operation_id":operation_id,
        "cancellation":{"delivery_id":delivery_id,"payload_digest":claimed},
        "cancelled_by":model::message_actor(&sender_registration,&principal.client_id),
        "reason":value.get("reason").cloned().unwrap_or(Value::Null),
        "original_record_changed":false,
        "delivery":"durable_mailbox_only"
    }))
}

struct DeliveryCoreRequest {
    sender_id: String,
    recipient_id: String,
    body: DeliveryBody,
}

enum DeliveryBody {
    LegacyText {
        text: String,
        payload_digest: String,
        in_reply_to: Value,
        reply_to: Value,
        admission_deadline_ms: Value,
        delivery_deadline_ms: Value,
        reply_deadline_ms: Value,
    },
    Typed {
        message_id: String,
        payload_kind: &'static str,
        payload_version: u32,
        payload: Value,
        payload_digest: String,
        reply_to: Value,
        admission_deadline_ms: Value,
        delivery_deadline_ms: Value,
        reply_deadline_ms: Value,
    },
}

fn admit_delivery_core(
    tx: &Transaction<'_>,
    operation_id: &str,
    request: DeliveryCoreRequest,
    facts: DeliveryFacts,
) -> Result<Value> {
    let distinct_message_id = match &request.body {
        DeliveryBody::LegacyText { .. } => operation_id,
        DeliveryBody::Typed { message_id, .. } => message_id,
    };
    let delivery_id = allocate_delivery_id(tx, operation_id, distinct_message_id)?;
    match request.body {
        DeliveryBody::LegacyText {
            text,
            payload_digest,
            in_reply_to,
            reply_to,
            admission_deadline_ms,
            delivery_deadline_ms,
            reply_deadline_ms,
        } => Ok(json!({
            "operation_id":operation_id,
            "message_id":operation_id,
            "delivery_id":delivery_id,
            "sender":request.sender_id,
            "recipient":request.recipient_id,
            "source_scope":facts.source_scope,
            "target_scope":facts.target_scope,
            "actor":facts.actor,
            "payload_digest":payload_digest,
            "admission_deadline_ms":admission_deadline_ms,
            "delivery_deadline_ms":delivery_deadline_ms,
            "reply_deadline_ms":reply_deadline_ms,
            "text":text,
            "in_reply_to":in_reply_to,
            "reply_to":reply_to,
            "cancellation":Value::Null,
            "delivery":"durable_mailbox_only"
        })),
        DeliveryBody::Typed {
            message_id,
            payload_kind,
            payload_version,
            payload,
            payload_digest,
            reply_to,
            admission_deadline_ms,
            delivery_deadline_ms,
            reply_deadline_ms,
        } => Ok(json!({
            "operation_id":operation_id,
            "message_id":message_id,
            "delivery_id":delivery_id,
            "sender":request.sender_id,
            "recipient":request.recipient_id,
            "source_scope":facts.source_scope,
            "target_scope":facts.target_scope,
            "actor":facts.actor,
            "payload_kind":payload_kind,
            "payload_version":payload_version,
            "payload":payload,
            "payload_digest":payload_digest,
            "admission_deadline_ms":admission_deadline_ms,
            "delivery_deadline_ms":delivery_deadline_ms,
            "reply_deadline_ms":reply_deadline_ms,
            "reply_to":reply_to,
            "cancellation":Value::Null,
            "delivery":"durable_mailbox_only"
        })),
    }
}

struct DeliveryFacts {
    source_scope: Value,
    target_scope: Value,
    actor: Value,
    recipient_actor: Value,
}

fn delivery_facts(
    tx: &Transaction<'_>,
    sender_id: &str,
    recipient_id: &str,
) -> Result<DeliveryFacts> {
    let recipient_registration = meta(tx, &format!("client:{recipient_id}"))?
        .ok_or_else(|| Error::new("NOT_FOUND", "recipient is not registered"))?;
    let sender_registration = meta(tx, &format!("client:{sender_id}"))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "sender is not registered"))?;
    Ok(DeliveryFacts {
        source_scope: model::message_scope(&sender_registration, sender_id),
        target_scope: model::message_scope(&recipient_registration, recipient_id),
        actor: model::message_actor(&sender_registration, sender_id),
        recipient_actor: model::message_actor(&recipient_registration, recipient_id),
    })
}

fn allocate_delivery_id(db: &Connection, operation_id: &str, message_id: &str) -> Result<String> {
    for _ in 0..DELIVERY_ID_ALLOCATION_ATTEMPTS {
        let delivery_id = model::new_id();
        if delivery_id != operation_id
            && delivery_id != message_id
            && !mailbox_identity_exists(db, &delivery_id)?
        {
            return Ok(delivery_id);
        }
    }
    Err(Error::new(
        "MAILBOX_DELIVERY_ID_EXHAUSTED",
        "could not allocate a distinct mailbox delivery identity",
    ))
}

fn mailbox_identity_exists(db: &Connection, identity: &str) -> Result<bool> {
    // Corruption guard first (fail closed, preserves legacy error semantics):
    // the pre-index OR query evaluated json_extract over every in-scope
    // settled receipt, so malformed result_json in scope THREW instead of
    // answering absent. This bounded probe over the invalid-row sentinel
    // index (015 migration) re-raises that authentic SQLite error before any
    // absence answer. NULL result_json is not malformed -- json_extract(NULL)
    // is NULL -- and keeps its historical absent behavior.
    let _: Option<String> = db
        .query_row(
            "SELECT json_extract(result_json,'$.delivery_id') FROM operations WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state='settled' AND result_json IS NOT NULL AND NOT json_valid(result_json) LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    // Branch 1: exact operation identity (existing PRIMARY KEY seek).
    let by_id: Option<i64> = db
        .query_row(
            "SELECT 1 FROM operations WHERE operation_id=?1 LIMIT 1",
            [identity],
            |row| row.get(0),
        )
        .optional()?;
    if by_id.is_some() {
        return Ok(true);
    }
    // Branches 2-3: legacy delivery_id / message_id receipt refs. Each branch
    // is one bounded seek over its own NON-UNIQUE partial expression index
    // (015 migration): duplicate and coalesced historical refs stay valid. A
    // single composite (method,state,delivery,message) index cannot bound a
    // message_id seek while delivery_id is unconstrained, so each identity
    // has an independent index. Malformed in-scope rows never reach these
    // seeks: the sentinel probe above throws first, as the legacy query did.
    // Expression text below must stay byte-identical to the matching index
    // definition.
    for selector in [
        "CASE WHEN json_valid(result_json) THEN json_extract(result_json, '$.delivery_id') END",
        "CASE WHEN json_valid(result_json) THEN json_extract(result_json, '$.message_id') END",
    ] {
        let found: Option<i64> = db
            .query_row(
                &format!(
                    "SELECT 1 FROM operations WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state='settled' AND {selector}=?1 LIMIT 1"
                ),
                [identity],
                |row| row.get(0),
            )
            .optional()?;
        if found.is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn validate_typed_reply(tx: &Transaction<'_>, request: &DeliveryRequest<'_>) -> Result<()> {
    let in_reply_to = match request.payload.get("in_reply_to") {
        None | Some(Value::Null) => None,
        Some(Value::String(message_id)) => Some(message_id.as_str()),
        Some(_) => {
            return Err(Error::invalid(
                "typed in_reply_to must be a message id or null",
            ));
        }
    };
    match (in_reply_to, request.reply_to.is_null()) {
        (None, true) => Ok(()),
        (None, false) => Err(Error::invalid(
            "reply_to requires a typed in_reply_to message id",
        )),
        (Some(_), true) => Err(Error::invalid(
            "typed in_reply_to requires an exact reply_to delivery reference",
        )),
        (Some(message_id), false) => {
            model::fields(&request.reply_to, &["delivery_id", "payload_digest"])?;
            let delivery_id = model::text(&request.reply_to, "delivery_id")?;
            let digest = model::text(&request.reply_to, "payload_digest")?;
            let prior = find_delivery(tx, delivery_id)?
                .ok_or_else(|| Error::new("NOT_FOUND", format!("Delivery {delivery_id}")))?;
            if prior["method"] != "coordination.message.send"
                || prior["result"]["message_id"] != message_id
                || prior["result"]["payload"]["thread_id"] != request.payload["thread_id"]
                || prior["result"]["sender"].as_str() != Some(request.recipient_id)
                || prior["result"]["recipient"].as_str() != Some(request.sender.client_id.as_str())
            {
                return Err(Error::invalid(
                    "reply target does not identify a message in the same coordination thread",
                ));
            }
            model::verify_payload_digest_claim(
                prior["result"]["payload_digest"].as_str(),
                Some(digest),
            )
        }
    }
}

fn legacy_reply_matches(prior: &Value, sender_id: &str, recipient_id: &str, reply: &str) -> bool {
    let method = prior["method"].as_str().unwrap_or_default();
    let mailbox_method = matches!(
        method,
        "message.send" | "coordination.message.send" | "coordination.send" | "coordination.consult"
    );
    let valid_identity = (mailbox_method
        && (prior["operation_id"] == reply || prior["result"]["delivery_id"].is_string()))
        || (method == "task.request_changes" && prior["result"]["applied"] == true)
        || (method == "task.invalidate_acceptance" && prior["result"]["message_id"] == reply);
    valid_identity
        && prior["result"]["recipient"] == sender_id
        && prior["result"]["sender"] == recipient_id
}
#[cfg(test)]
mod mailbox_identity_index_tests {
    use super::*;

    const MIGRATION_SQL: &str = include_str!("../../migrations/015_mailbox_delivery_identity.sql");

    /// Byte-identical copy of the pre-index existence query. Malformed
    /// result_json in scope made this statement THROW (json_extract on
    /// malformed JSON is an error, not NULL); the indexed reader must
    /// preserve that error instead of answering absent.
    const LEGACY_IDENTITY_SQL: &str = "SELECT 1 FROM operations WHERE operation_id=?1 OR (method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state='settled' AND (json_extract(result_json,'$.delivery_id')=?1 OR json_extract(result_json,'$.message_id')=?1)) LIMIT 1";

    fn fixture_db() -> rusqlite::Connection {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        db.execute_batch(MIGRATION_SQL).unwrap();
        db.execute_batch(MIGRATION_SQL).unwrap();
        db
    }

    fn seed_op(db: &rusqlite::Connection, id: &str, method: &str, state: &str, result: &Value) {
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,settled_at_ms,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,'{}','{}',?5,?6,CASE WHEN ?5 IN ('settled','rejected','cancelled') THEN 1 ELSE NULL END,1,1,1)",
            rusqlite::params![
                id,
                format!("caller-{id}"),
                format!("req-{id}"),
                method,
                state,
                serde_json::to_string(result).unwrap(),
            ],
        )
        .unwrap();
    }

    fn seed_null_result_op(db: &rusqlite::Connection) {
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,settled_at_ms,due_at_ms,created_at_ms,updated_at_ms) VALUES('op-5','caller-op-5','req-op-5','message.send','{}','{}','queued',NULL,NULL,1,1,1)",
            [],
        )
        .unwrap();
    }

    fn seed_malformed_op(db: &rusqlite::Connection, id: &str, method: &str, state: &str) {
        // CHECK(result_json IS NULL OR json_valid(result_json)) forbids this
        // insert; the pragma admits one retained-corruption row so the guard
        // path is exercised end to end. PRAGMA is connection-scoped and reset
        // immediately after the insert.
        db.execute_batch("PRAGMA ignore_check_constraints=ON;")
            .unwrap();
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,settled_at_ms,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,'{}','{}',?5,'{',CASE WHEN ?5 IN ('settled','rejected','cancelled') THEN 1 ELSE NULL END,1,1,1)",
            rusqlite::params![
                id,
                format!("caller-{id}"),
                format!("req-{id}"),
                method,
                state,
            ],
        )
        .unwrap();
        db.execute_batch("PRAGMA ignore_check_constraints=OFF;")
            .unwrap();
    }

    fn seed_fixture(db: &rusqlite::Connection) {
        seed_op(
            db,
            "op-1",
            "message.send",
            "settled",
            &serde_json::json!({"delivery_id": "d-1", "message_id": "m-1"}),
        );
        seed_op(
            db,
            "op-2",
            "message.send",
            "settled",
            &serde_json::json!({"delivery_id": "d-1", "message_id": "m-2"}),
        );
        seed_op(
            db,
            "op-3",
            "message.send",
            "queued",
            &serde_json::json!({"delivery_id": "d-3", "message_id": "m-3"}),
        );
        seed_op(
            db,
            "op-4",
            "task.create",
            "settled",
            &serde_json::json!({"delivery_id": "d-4"}),
        );
    }

    #[test]
    fn settled_identities_found_and_others_refused() {
        let db = fixture_db();
        seed_fixture(&db);
        assert!(mailbox_identity_exists(&db, "op-1").unwrap());
        assert!(mailbox_identity_exists(&db, "d-1").unwrap());
        assert!(mailbox_identity_exists(&db, "m-1").unwrap());
        assert!(mailbox_identity_exists(&db, "m-2").unwrap());
        assert!(!mailbox_identity_exists(&db, "d-3").unwrap());
        assert!(!mailbox_identity_exists(&db, "m-3").unwrap());
        assert!(!mailbox_identity_exists(&db, "d-4").unwrap());
        assert!(!mailbox_identity_exists(&db, "no-such-identity").unwrap());
    }

    #[test]
    fn duplicate_historical_refs_remain_valid() {
        let db = fixture_db();
        seed_fixture(&db);
        assert!(mailbox_identity_exists(&db, "d-1").unwrap());
        let count: i64 = db
            .query_row(
                "SELECT count(*) FROM operations WHERE method='message.send' AND state='settled' AND json_extract(result_json,'$.delivery_id')='d-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn malformed_in_scope_surfaces_store_error_not_absence() {
        let db = fixture_db();
        seed_fixture(&db);
        seed_null_result_op(&db);
        seed_malformed_op(&db, "op-bad", "message.send", "settled");
        // Historical proof: the pre-index OR query threw on malformed
        // in-scope JSON instead of answering.
        let legacy: rusqlite::Result<Option<i64>> = db
            .query_row(LEGACY_IDENTITY_SQL, ["d-1"], |row| row.get(0))
            .optional();
        assert!(
            legacy.is_err(),
            "legacy query must throw on malformed in-scope JSON"
        );
        // The indexed reader preserves that: error, never a false absence --
        // even for identities that do (or do not) exist.
        assert!(mailbox_identity_exists(&db, "d-1").is_err());
        assert!(mailbox_identity_exists(&db, "no-such-identity").is_err());
    }

    #[test]
    fn malformed_out_of_scope_and_null_stay_absent() {
        let db = fixture_db();
        seed_fixture(&db);
        seed_null_result_op(&db);
        seed_malformed_op(&db, "op-bad-method", "task.create", "settled");
        seed_malformed_op(&db, "op-bad-state", "message.send", "queued");
        // Same scope bound as the legacy query: out-of-scope rows never fed
        // json_extract, so neither the legacy query nor the reader throws.
        let legacy: Option<i64> = db
            .query_row(LEGACY_IDENTITY_SQL, ["d-1"], |row| row.get(0))
            .optional()
            .unwrap();
        assert!(legacy.is_some());
        assert!(mailbox_identity_exists(&db, "d-1").unwrap());
        assert!(mailbox_identity_exists(&db, "op-5").unwrap());
        assert!(!mailbox_identity_exists(&db, "dangling-identity").unwrap());
    }

    #[test]
    fn branch_plans_use_index_seeks() {
        let db = fixture_db();
        for n in 0..1000 {
            seed_op(
                &db,
                &format!("bulk-{n}"),
                "message.send",
                "settled",
                &serde_json::json!({"delivery_id": format!("bulk-d-{n}"), "message_id": format!("bulk-m-{n}")}),
            );
        }
        db.execute_batch("ANALYZE;").unwrap();
        for (selector, index) in [
            (
                "CASE WHEN json_valid(result_json) THEN json_extract(result_json, '$.delivery_id') END",
                "mailbox_settled_delivery_identity",
            ),
            (
                "CASE WHEN json_valid(result_json) THEN json_extract(result_json, '$.message_id') END",
                "mailbox_settled_message_identity",
            ),
        ] {
            let sql = format!(
                "EXPLAIN QUERY PLAN SELECT 1 FROM operations WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state='settled' AND {selector}=?1 LIMIT 1"
            );
            let mut statement = db.prepare(&sql).unwrap();
            let plan: String = statement
                .query_map(["probe"], |row| {
                    Ok(format!(
                        "{}|{}|{}|{}",
                        row.get::<_, i64>(0).unwrap_or(-1),
                        row.get::<_, i64>(1).unwrap_or(-1),
                        row.get::<_, i64>(2).unwrap_or(-1),
                        row.get::<_, String>(3).unwrap_or_default(),
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<String>>>()
                .unwrap()
                .join("\n");
            assert!(
                plan.contains("SEARCH") && plan.contains(index),
                "branch must seek {index}, got: {plan}"
            );
            assert!(!plan.contains("SCAN"), "branch must not scan, got: {plan}");
        }
        // Corruption sentinel: the probe must resolve through the invalid-row
        // index, never a base-table scan. A scan of that partial index stays
        // bounded to malformed rows (normally zero); the name assertion is the
        // row-visit bound.
        let mut sentinel_stmt = db
            .prepare(
                "EXPLAIN QUERY PLAN SELECT json_extract(result_json,'$.delivery_id') FROM operations WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state='settled' AND result_json IS NOT NULL AND NOT json_valid(result_json) LIMIT 1",
            )
            .unwrap();
        let sentinel_plan: String = sentinel_stmt
            .query_map([], |row| {
                Ok(format!(
                    "{}|{}|{}|{}",
                    row.get::<_, i64>(0).unwrap_or(-1),
                    row.get::<_, i64>(1).unwrap_or(-1),
                    row.get::<_, i64>(2).unwrap_or(-1),
                    row.get::<_, String>(3).unwrap_or_default(),
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap()
            .join(" | ");
        assert!(
            sentinel_plan.contains("mailbox_settled_invalid_result"),
            "sentinel probe must use the invalid-row index, got: {sentinel_plan}"
        );
    }
    #[test]
    fn find_delivery_lookup_uses_delivery_index_seek() {
        let db = fixture_db();
        for n in 0..200 {
            seed_op(
                &db,
                &format!("bulk-{n}"),
                "message.send",
                "settled",
                &serde_json::json!({"delivery_id": format!("bulk-d-{n}"), "message_id": format!("bulk-m-{n}")}),
            );
        }
        db.execute_batch("ANALYZE;").unwrap();
        let sql = "EXPLAIN QUERY PLAN SELECT operation_id FROM operations WHERE method IN ('message.send','coordination.message.send','coordination.send','coordination.consult') AND state='settled' AND json_type(result_json,'$.delivery_id')='text' AND length(json_extract(result_json,'$.delivery_id'))>0 AND json_extract(result_json,'$.delivery_id')=?1 AND CASE WHEN json_valid(result_json) THEN json_extract(result_json, '$.delivery_id') END=?1 ORDER BY operation_id LIMIT 2";
        let mut stmt = db.prepare(sql).unwrap();
        let plan: String = stmt
            .query_map(["probe"], |row| {
                Ok(format!(
                    "{}|{}|{}|{}",
                    row.get::<_, i64>(0).unwrap_or(-1),
                    row.get::<_, i64>(1).unwrap_or(-1),
                    row.get::<_, i64>(2).unwrap_or(-1),
                    row.get::<_, String>(3).unwrap_or_default(),
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap()
            .join("\n");
        assert!(
            plan.contains("SEARCH") && plan.contains("mailbox_settled_delivery_identity"),
            "delivery lookup must seek delivery index, got: {plan}"
        );
        assert!(!plan.contains("SCAN"), "delivery lookup must not scan, got: {plan}");
    }
    #[test]
    fn find_delivery_preserves_ambiguity_typedtext_failclosed() {
        let db = fixture_db();
        seed_op(&db, "op-f1", "message.send", "settled", &serde_json::json!({"delivery_id": "d-find-1", "message_id": "m-find-1"}));
        let found = find_delivery(&db, "d-find-1").unwrap().expect("unique delivery must resolve");
        assert_eq!(found["operation_id"], "op-f1");
        assert_eq!(found["result"]["delivery_id"], "d-find-1");
        assert!(find_delivery(&db, "no-such-delivery").unwrap().is_none());
        seed_op(&db, "op-dup-a", "message.send", "settled", &serde_json::json!({"delivery_id": "d-dup-1"}));
        seed_op(&db, "op-dup-b", "message.send", "settled", &serde_json::json!({"delivery_id": "d-dup-1"}));
        let err = find_delivery(&db, "d-dup-1").unwrap_err();
        assert_eq!(err.code, "MAILBOX_DELIVERY_AMBIGUOUS");
        seed_op(&db, "op-num", "message.send", "settled", &serde_json::json!({"delivery_id": 123}));
        assert!(find_delivery(&db, "123").unwrap().is_none());
        seed_op(&db, "op-empty", "message.send", "settled", &serde_json::json!({"delivery_id": ""}));
        assert!(find_delivery(&db, "").unwrap().is_none());
        seed_malformed_op(&db, "op-bad-in", "message.send", "settled");
        assert!(find_delivery(&db, "d-find-1").is_err());
        let clean = fixture_db();
        seed_op(&clean, "op-f1", "message.send", "settled", &serde_json::json!({"delivery_id": "d-find-1"}));
        seed_malformed_op(&clean, "op-bad-method", "task.create", "settled");
        seed_malformed_op(&clean, "op-bad-state", "message.send", "queued");
        assert!(find_delivery(&clean, "d-find-1").unwrap().is_some());
    }
}
