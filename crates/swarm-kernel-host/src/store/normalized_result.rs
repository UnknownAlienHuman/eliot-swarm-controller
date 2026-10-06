//! Generic descriptor-gated result pages. Admission validates the live
//! descriptor and seals an exact producer snapshot; all later page and
//! candidate readback validates against that immutable snapshot, not the
//! descriptor's current enabled state or mutable Attempt lifecycle.

use super::{module_handshake, operations, results, runtime, tasks};
use crate::{
    artifacts::{ArtifactRecord, MAX_PAGE_BYTES, ResultPage},
    error::{Error, Result},
    model::{self},
    runtime::{EffectOutcome, RuntimeOutcome},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use swarm_contracts::{
    module_contract::{normalized_result_context_schema, normalized_result_page_schema},
    runtime::{
        ModuleReceiptIdentity, NormalizedResultOriginContext, NormalizedResultPageSource,
        NormalizedResultProducerOrigin,
    },
};

/// This check is used only while admitting a new result action. Historical
/// page verification deliberately does not call it.
pub(super) fn enabled(db: &Connection, binding: &Value) -> Result<bool> {
    let Some(selector) = binding["observation"].get("module_contract_selector") else {
        return Ok(false);
    };
    let identity = module_handshake::retained_contract_identity(
        db,
        model::text(binding, "module_artifact_id")?,
        Some(selector),
    )?
    .ok_or_else(|| {
        Error::new(
            "MODULE_DESCRIPTOR_MISSING",
            "selected binding has no retained module descriptor",
        )
    })?;
    let context = identity
        .command_schemas
        .contains(&normalized_result_context_schema());
    let page = identity
        .event_schemas
        .contains(&normalized_result_page_schema());
    let dispatch_context = identity
        .command_schemas
        .contains(&swarm_contracts::module_contract::task_dispatch_context_schema());
    let dispatch_admission = identity
        .event_schemas
        .contains(&swarm_contracts::module_contract::task_dispatch_admission_schema());
    if context != page {
        return Err(Error::new(
            "MODULE_CONTRACT_INCOMPATIBLE",
            "normalized result pages require both context and page schemas",
        ));
    }
    if dispatch_context != dispatch_admission || (context && !dispatch_context) {
        return Err(Error::new(
            "MODULE_CONTRACT_INCOMPATIBLE",
            "normalized result pages require the normalized dispatch producer contract",
        ));
    }
    Ok(context)
}

/// Built-in result codecs keep their existing versioned payloads. Any other
/// selector on an opted-in descriptor uses the shared normalized page.
pub(super) fn uses_generic_selector(selector: &Value) -> bool {
    !matches!(
        selector.get("kind").and_then(Value::as_str),
        Some("input_status" | "antigravity_status" | "command_status" | "claude_assistant_result")
    )
}

/// Build the origin once, during user-command admission. The caller persists
/// this exact value in effective_request_json before the Operation is queued.
pub(super) fn admitted_origin(
    db: &Connection,
    binding: &Value,
    attempt: &Value,
    request: &Value,
) -> Result<Value> {
    if !enabled(db, binding)? {
        return Err(Error::new(
            "MODULE_CONTRACT_INCOMPATIBLE",
            "normalized result was not admitted by this descriptor",
        ));
    }
    let dispatch_id = model::text(&request["selector"], "input_operation_id")?;
    let binding_id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let attempt_id = model::text(attempt, "attempt_id")?;
    let dispatch = operations::get_operation(db, dispatch_id)?;
    if dispatch["method"] != "task.dispatch"
        || dispatch["operation_id"] != dispatch_id
        || dispatch["task_id"] != attempt["task_id"]
        || dispatch["attempt_id"] != attempt_id
        || dispatch["binding_id"] != binding_id
        || dispatch["binding_generation"] != generation
        || attempt["binding_id"] != binding_id
        || attempt["binding_generation"] != generation
        || attempt["start_operation_id"] != dispatch_id
    {
        return Err(Error::new(
            "RESULT_ORIGIN_INVALID",
            "selected dispatch is not the exact producer for this bound Attempt",
        ));
    }
    let task = tasks::get_task(db, model::text(attempt, "task_id")?)?;
    if task["state"] != "open"
        || task["revision"] != attempt["task_revision"]
        || task["current_attempt_id"] != attempt_id
        || !attempt["released_at_ms"].is_null()
    {
        return Err(Error::new(
            "STALE_ASSIGNMENT",
            "new result admission requires the current unreleased Task Attempt",
        ));
    }

    let target_raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
        params![dispatch_id, binding_id, generation],
        |row| row.get(0),
    )?;
    let target_request: Value = serde_json::from_str(&target_raw)?;
    if target_request["attempt_id"] != attempt_id {
        return Err(Error::new(
            "RESULT_ORIGIN_INVALID",
            "stored dispatch request names another Attempt",
        ));
    }
    let target_digest = model::digest(model::canonical(&target_request)?.as_bytes());
    let text = model::text(&target_request, "text")?;
    let text_digest = model::digest(text.as_bytes());
    let text_bytes = u64::try_from(text.len())
        .map_err(|_| Error::invalid("dispatch text length is out of range"))?;
    let snapshot_digest = model::digest(model::canonical(&attempt["task_snapshot"])?.as_bytes());
    let producers = attempt["producers"].as_array().ok_or_else(|| {
        Error::new(
            "RESULT_ORIGIN_INVALID",
            "Attempt producer list is malformed",
        )
    })?;
    let retained: Vec<&Value> = producers
        .iter()
        .filter(|producer| {
            producer["assignment_id"] == dispatch_id
                && producer["dispatch_operation_id"] == dispatch_id
                && producer["admission_kind"] == "normalized_task_dispatch"
        })
        .collect();
    if retained.len() != 1 {
        return Err(Error::new(
            "RESULT_ORIGIN_UNAVAILABLE",
            "result requires one durable normalized dispatch producer",
        ));
    }
    let retained = retained[0];
    let producer = producer_origin(retained)?;
    if producer.attempt_id != attempt_id
        || producer.task_id != attempt["task_id"]
        || producer.task_revision != attempt["task_revision"]
        || producer.task_snapshot_sha256 != snapshot_digest
        || producer.source_text_sha256 != text_digest
        || producer.source_text_bytes != text_bytes
        || producer.completion_condition != retained["completion_condition"]
        || producer.execution_complete != (retained["execution_complete"] == true)
        || producer.task_completion != retained["task_completion"]
        || producer.disposition != retained["disposition"]
    {
        return Err(Error::new(
            "RESULT_ORIGIN_INVALID",
            "retained dispatch receipt differs from the original request and Attempt snapshot",
        ));
    }

    // New admission still checks the typed dispatch receipt against the
    // currently selected descriptor. Later reads use the sealed receipt and
    // never require that descriptor to remain enabled.
    validate_receipt(db, binding, &producer.module_receipt)?;
    let origin = NormalizedResultOriginContext {
        schema_version: 1,
        binding_id: binding_id.to_owned(),
        binding_generation: generation,
        task_id: model::text(attempt, "task_id")?.to_owned(),
        task_revision: model::positive(attempt, "task_revision")?,
        task_snapshot_sha256: snapshot_digest,
        attempt_id: attempt_id.to_owned(),
        target_operation_id: dispatch_id.to_owned(),
        target_input_sha256: target_digest,
        selector_sha256: model::digest(model::canonical(&request["selector"])?.as_bytes()),
        producer,
    };
    origin.validate().map_err(|_| {
        Error::new(
            "RESULT_ORIGIN_INVALID",
            "normalized result origin is invalid",
        )
    })?;
    Ok(serde_json::to_value(origin)?)
}

/// Validate a new or replayed page using only the result Operation's original
/// request, sealed effective request, and immutable target Operation. This is
/// safe after binding disable/release or Attempt disposition changes.
pub(super) fn validate_admitted_operation(
    db: &Connection,
    operation_id: &str,
    origin_value: &Value,
) -> Result<NormalizedResultOriginContext> {
    let origin: NormalizedResultOriginContext = serde_json::from_value(origin_value.clone())
        .map_err(|_| Error::new("RESULT_ORIGIN_INVALID", "sealed result origin is malformed"))?;
    origin
        .validate()
        .map_err(|_| Error::new("RESULT_ORIGIN_INVALID", "sealed result origin is invalid"))?;
    let op = operations::get_operation(db, operation_id)?;
    let (original_raw, effective_raw): (String, String) = db.query_row(
        "SELECT original_request_json,effective_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let original: Value = serde_json::from_str(&original_raw)?;
    let effective: Value = serde_json::from_str(&effective_raw)?;
    let selector_digest = model::digest(model::canonical(&original["selector"])?.as_bytes());
    if op["method"] != "agent.result"
        || op["operation_id"] != operation_id
        || op["binding_id"] != origin.binding_id
        || op["binding_generation"] != origin.binding_generation
        || op["task_id"] != origin.task_id
        || op["attempt_id"] != origin.attempt_id
        || !matches!(
            op["state"].as_str(),
            Some("queued" | "sending" | "native_accepted" | "outcome_unknown" | "settled")
        )
        || original["selector"]["input_operation_id"] != origin.target_operation_id
        || !uses_generic_selector(&original["selector"])
        || origin.selector_sha256 != selector_digest
        || effective["normalized_result_origin"] != origin_value.clone()
    {
        return Err(Error::new(
            "RESULT_ORIGIN_INVALID",
            "Operation differs from its exact admitted normalized result request",
        ));
    }
    let target = operations::get_operation(db, &origin.target_operation_id)?;
    let target_raw: Option<String> = db
        .query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
            params![origin.target_operation_id, origin.binding_id, origin.binding_generation],
            |row| row.get(0),
        )
        .optional()?;
    let target_request: Value = serde_json::from_str(
        &target_raw
            .ok_or_else(|| Error::new("RESULT_ORIGIN_INVALID", "target request is absent"))?,
    )?;
    let target_digest = model::digest(model::canonical(&target_request)?.as_bytes());
    let text = model::text(&target_request, "text")?;
    let text_digest = model::digest(text.as_bytes());
    let text_bytes = u64::try_from(text.len())
        .map_err(|_| Error::invalid("dispatch source text length is out of range"))?;
    let attempt = tasks::get_attempt(db, &origin.attempt_id)?;
    let producers = attempt["producers"].as_array().ok_or_else(|| {
        Error::new(
            "RESULT_ORIGIN_INVALID",
            "retained Attempt producers are malformed",
        )
    })?;
    let matching: Vec<&Value> = producers
        .iter()
        .filter(|producer| {
            producer["dispatch_operation_id"] == origin.target_operation_id
                && producer["admission_kind"] == "normalized_task_dispatch"
        })
        .collect();
    let retained = matching.first().copied().ok_or_else(|| {
        Error::new(
            "RESULT_ORIGIN_INVALID",
            "durable normalized dispatch producer is absent",
        )
    })?;
    if matching.len() != 1
        || target["method"] != "task.dispatch"
        || target["task_id"] != origin.task_id
        || target["attempt_id"] != origin.attempt_id
        || target["binding_id"] != origin.binding_id
        || target["binding_generation"] != origin.binding_generation
        || target_request["attempt_id"] != origin.attempt_id
        || target_digest != origin.target_input_sha256
        || text_digest != origin.producer.source_text_sha256
        || text_bytes != origin.producer.source_text_bytes
        || attempt["attempt_id"] != origin.attempt_id
        || attempt["task_id"] != origin.task_id
        || attempt["task_revision"] != origin.task_revision
        || attempt["binding_id"] != origin.binding_id
        || attempt["binding_generation"] != origin.binding_generation
        || model::digest(model::canonical(&attempt["task_snapshot"])?.as_bytes())
            != origin.task_snapshot_sha256
        || !same_retained_producer_identity(retained, &origin.producer)?
    {
        return Err(Error::new(
            "RESULT_ORIGIN_INVALID",
            "sealed origin differs from the retained target, Attempt, or producer",
        ));
    }
    Ok(origin)
}

pub(super) fn validate_source(
    db: &Connection,
    operation_id: &str,
    source_value: &Value,
) -> Result<(NormalizedResultPageSource, Value)> {
    let source: NormalizedResultPageSource =
        serde_json::from_value(source_value.clone()).map_err(|_| {
            Error::new(
                "RESULT_PROVENANCE_INVALID",
                "normalized source is malformed",
            )
        })?;
    source.validate().map_err(|_| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "normalized source fields are invalid",
        )
    })?;
    let origin_value = serde_json::to_value(&source.origin)?;
    let origin = validate_admitted_operation(db, operation_id, &origin_value)?;
    let (original_raw, effective_raw): (String, String) = db.query_row(
        "SELECT original_request_json,effective_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    let original: Value = serde_json::from_str(&original_raw)?;
    let effective: Value = serde_json::from_str(&effective_raw)?;
    let result_digest = model::digest(model::canonical(&original)?.as_bytes());
    if source.result_operation_id != operation_id || source.result_input_sha256 != result_digest {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "source differs from the exact result Operation request",
        ));
    }
    let target = operations::get_operation(db, &source.origin.target_operation_id)?;
    let target_raw: Option<String> = db
        .query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1 AND binding_id=?2 AND binding_generation=?3",
            params![source.origin.target_operation_id, source.origin.binding_id, source.origin.binding_generation],
            |row| row.get(0),
        )
        .optional()?;
    let target_request: Value = serde_json::from_str(
        &target_raw
            .ok_or_else(|| Error::new("RESULT_ORIGIN_INVALID", "target request is absent"))?,
    )?;
    let target_digest = model::digest(model::canonical(&target_request)?.as_bytes());
    if target["method"] != "task.dispatch"
        || target["task_id"] != source.origin.task_id
        || target["attempt_id"] != source.origin.attempt_id
        || target["binding_id"] != source.origin.binding_id
        || target["binding_generation"] != source.origin.binding_generation
        || target_request["attempt_id"] != source.origin.attempt_id
        || target_digest != origin.target_input_sha256
        || !same_module(
            &source.result_module_receipt,
            &source.origin.producer.module_receipt,
        )
        || source.result_module_receipt.operation_id != operation_id
        || source.result_module_receipt.input_sha256 != result_digest
        || source.result_module_receipt.binding_id != source.origin.binding_id
        || source.result_module_receipt.binding_generation != source.origin.binding_generation
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "sealed source does not match the retained Task dispatch and module receipts",
        ));
    }
    if let Some(expected) = effective.get("normalized_result_payload_identity")
        && (expected["sha256"] != source.payload_sha256
            || expected["byte_length"] != source.payload_bytes
            || expected["complete"] != true)
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "page payload differs from the exact admitted native capture digest",
        ));
    }
    Ok((source, origin_value))
}

pub(super) fn validate_page(page: &ResultPage, metadata: &Value, bytes: &[u8]) -> Result<Value> {
    let source: NormalizedResultPageSource =
        serde_json::from_value(page.source.clone()).map_err(|_| {
            Error::new(
                "RESULT_PROVENANCE_INVALID",
                "normalized source is malformed",
            )
        })?;
    source.validate().map_err(|_| {
        Error::new(
            "RESULT_PROVENANCE_INVALID",
            "normalized source fields are invalid",
        )
    })?;
    let end = page
        .offset_bytes
        .checked_add(page.byte_length)
        .ok_or_else(|| Error::invalid("normalized result range overflow"))?;
    let origin = serde_json::to_value(&source.origin)?;
    if page.total_bytes != source.payload_bytes
        || metadata["operation_id"] != source.result_operation_id
        || metadata["normalized_result_origin"] != origin
        || page.byte_length != bytes.len() as u64
        || page.byte_length > MAX_PAGE_BYTES as u64
        || page.page_sha256 != model::digest(bytes)
        || end > source.payload_bytes
        || page.eof != (end == source.payload_bytes)
        || metadata["requested_offset"].as_u64() != Some(page.offset_bytes)
        || page.byte_length > metadata["requested_length"].as_u64().unwrap_or(0)
    {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "page range differs from its sealed source and admitted request",
        ));
    }
    if page.offset_bytes == 0 && page.eof && model::digest(bytes) != source.payload_sha256 {
        return Err(Error::new(
            "RESULT_PROVENANCE_INVALID",
            "complete result bytes differ from the declared full payload digest",
        ));
    }
    Ok(origin)
}

/// Validate a candidate or readable page against the exact retained result
/// Operation. `attempt` is only the caller's current authorization scope; no
/// historical producer or descriptor is reloaded from mutable live state.
pub(super) fn validate_page_artifact(
    db: &Connection,
    attempt: &Value,
    page: &ArtifactRecord,
) -> Result<bool> {
    if page.kind != "native_result_page"
        || page.metadata["source"]["schema_id"]
            != swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
    {
        return Ok(false);
    }
    let (source, origin_value) = validate_source(
        db,
        model::text(&page.metadata, "operation_id")?,
        &page.metadata["source"],
    )?;
    check_attempt_scope(attempt, &source.origin)?;
    if page.metadata["binding_id"] != source.origin.binding_id
        || page.metadata["generation"] != source.origin.binding_generation
        || page.metadata["normalized_result_origin"] != origin_value
        || page.metadata["page_sha256"] != page.content_digest
        || page.metadata["byte_length"] != page.byte_length
    {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "result page differs from its retained origin or bytes",
        ));
    }
    let result_operation = operations::get_operation(db, &source.result_operation_id)?;
    if result_operation["state"] != "settled"
        || result_operation["result"]["outcome"] != "applied"
        || result_operation["result"]["details"]["artifact_ref"] != page.artifact_id
        || result_operation["result"]["details"]["source"] != page.metadata["source"]
    {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "page is not the immutable artifact retained by its result Operation",
        ));
    }
    Ok(true)
}

/// Return true only for a whole-body normalized page or an assembled artifact
/// whose existing assembler verified coverage and SHA-256.
pub(super) fn validate_candidate_origin(
    db: &Connection,
    attempt: &Value,
    candidate: &ArtifactRecord,
) -> Result<bool> {
    let (source, page_ids, assembled) = match candidate.kind.as_str() {
        "native_result_page"
            if candidate.metadata["source"]["schema_id"]
                == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID =>
        {
            (
                candidate.metadata["source"].clone(),
                vec![candidate.artifact_id.clone()],
                false,
            )
        }
        "native_result" if candidate.metadata["coverage"] == "complete" => {
            let source = candidate.metadata["identity"]["source"].clone();
            if source["schema_id"]
                != swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
            {
                return Ok(false);
            }
            let ids = candidate.metadata["parts"]
                .as_array()
                .ok_or_else(|| Error::new("CANDIDATE_SCOPE", "assembly has no parts"))?
                .iter()
                .map(|part| model::text(part, "artifact_ref").map(str::to_owned))
                .collect::<Result<Vec<_>>>()?;
            (source, ids, true)
        }
        _ => return Ok(false),
    };
    let origin: NormalizedResultOriginContext = serde_json::from_value(source["origin"].clone())
        .map_err(|_| Error::new("CANDIDATE_SCOPE", "normalized origin is malformed"))?;
    origin
        .validate()
        .map_err(|_| Error::new("CANDIDATE_SCOPE", "normalized origin is invalid"))?;
    check_attempt_scope(attempt, &origin)?;
    let normalized_source_identity = normalized_page_identity(&source);
    let candidate_identity = if assembled {
        &candidate.metadata["identity"]
    } else {
        &candidate.metadata
    };
    let candidate_generation = if assembled {
        &candidate_identity["binding_generation"]
    } else {
        &candidate_identity["generation"]
    };
    if candidate_identity["binding_id"] != origin.binding_id
        || candidate_generation.as_i64() != Some(origin.binding_generation)
        || (!assembled && candidate_identity["selector"] != candidate.metadata["selector"])
    {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "candidate identity differs from its retained normalized source origin",
        ));
    }
    if source["payload_bytes"].as_u64() != Some(candidate.byte_length)
        || source["payload_sha256"].as_str() != Some(candidate.content_digest.as_str())
        || (assembled
            && (candidate.metadata["coverage"] != "complete"
                || candidate.metadata["identity"]["total_bytes"] != candidate.byte_length))
        || (!assembled
            && (candidate.metadata["offset_bytes"] != 0
                || candidate.metadata["total_bytes"] != candidate.byte_length
                || candidate.metadata["eof"] != true))
    {
        return Err(Error::new(
            "CANDIDATE_INCOMPLETE",
            "normalized candidate does not cover its exact full payload digest",
        ));
    }
    if page_ids.is_empty() {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "candidate has no result pages",
        ));
    }
    let total = source["payload_bytes"]
        .as_u64()
        .ok_or_else(|| Error::new("CANDIDATE_SCOPE", "normalized payload length is malformed"))?;
    if assembled {
        validate_assembly_origin(db, candidate, &page_ids, total, &normalized_source_identity)?;
    }
    let mut expected_offset = 0u64;
    for (index, page_id) in page_ids.iter().enumerate() {
        let page = results::get(db, page_id)?;
        let end = expected_offset
            .checked_add(page.byte_length)
            .ok_or_else(|| Error::new("CANDIDATE_SCOPE", "result page range overflows"))?;
        let part_matches = !assembled
            || (candidate.metadata["parts"][index]["offset_bytes"].as_u64()
                == Some(expected_offset)
                && candidate.metadata["parts"][index]["byte_length"].as_u64()
                    == Some(page.byte_length)
                && candidate.metadata["parts"][index]["sha256"].as_str()
                    == Some(page.content_digest.as_str()));
        if !validate_page_artifact(db, attempt, &page)?
            || normalized_page_identity(&page.metadata["source"]) != normalized_source_identity
            || page.metadata["selector"] != candidate_identity["selector"]
            || page.metadata["offset_bytes"].as_u64() != Some(expected_offset)
            || page.metadata["total_bytes"].as_u64() != Some(total)
            || page.metadata["eof"].as_bool() != Some(end == total)
            || !part_matches
        {
            return Err(Error::new(
                "CANDIDATE_SCOPE",
                "assembled pages differ from the exact normalized result origin",
            ));
        }
        expected_offset = end;
    }
    if assembled && expected_offset != total {
        return Err(Error::new(
            "CANDIDATE_INCOMPLETE",
            "assembled normalized result pages do not cover the declared payload",
        ));
    }
    Ok(true)
}

fn normalized_page_identity(source: &Value) -> Value {
    let mut identity = source.clone();
    if let Some(fields) = identity.as_object_mut() {
        fields.remove("result_operation_id");
        fields.remove("result_input_sha256");
        fields.remove("result_module_receipt");
        fields.remove("whole_digest_verified");
    }
    if let Some(producer) = identity
        .get_mut("origin")
        .and_then(|origin| origin.get_mut("producer"))
        .and_then(Value::as_object_mut)
    {
        // This is a comparison key only. Persisted per-page sources keep and
        // validate their exact lifecycle tuple; only cross-page identity omits
        // these reconciliation-mutable observations.
        producer.insert(
            "completion_condition".to_owned(),
            json!("native_input_admitted"),
        );
        producer.insert("execution_complete".to_owned(), json!(false));
        producer.insert("task_completion".to_owned(), json!("unknown"));
        producer.insert("disposition".to_owned(), json!("admitted"));
    }
    identity
}

fn validate_assembly_origin(
    db: &Connection,
    candidate: &ArtifactRecord,
    page_ids: &[String],
    total_bytes: u64,
    source_identity: &Value,
) -> Result<()> {
    let assembly_id = model::text(&candidate.metadata, "assembly_operation_id")?;
    let operation = operations::get_operation(db, assembly_id)?;
    if operation["method"] != "artifact.assemble"
        || operation["state"] != "settled"
        || operation["result"]["outcome"] != "applied"
        || operation["result"]["details"]["artifact_ref"] != candidate.artifact_id
        || operation["result"]["details"]["metadata"]["identity"] != candidate.metadata["identity"]
        || operation["result"]["details"]["metadata"]["sha256"] != candidate.content_digest
        || operation["result"]["details"]["byte_length"] != candidate.byte_length
        || normalized_page_identity(&candidate.metadata["identity"]["source"]) != *source_identity
        || candidate.metadata["identity"]["total_bytes"] != total_bytes
        || candidate.metadata["sha256"] != candidate.content_digest
        || candidate.metadata["byte_length"] != candidate.byte_length
        || candidate.metadata["native_digest_verified"] != false
        || candidate.metadata["normalized_payload_digest_verified"] != true
    {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "normalized assembly is not the exact retained whole-result artifact",
        ));
    }
    let request_raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [assembly_id],
        |row| row.get(0),
    )?;
    let request: Value = serde_json::from_str(&request_raw)?;
    let requested: Vec<String> = request["page_refs"]
        .as_array()
        .ok_or_else(|| Error::new("CANDIDATE_SCOPE", "assembly request has no page refs"))?
        .iter()
        .map(|id| {
            id.as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::new("CANDIDATE_SCOPE", "assembly page ref is malformed"))
        })
        .collect::<Result<_>>()?;
    if requested != page_ids {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "assembly pages differ from the exact retained assembly request",
        ));
    }
    Ok(())
}

fn same_retained_producer_identity(
    retained: &Value,
    sealed: &NormalizedResultProducerOrigin,
) -> Result<bool> {
    let module_receipt: ModuleReceiptIdentity =
        serde_json::from_value(retained["module_receipt"].clone()).map_err(|_| {
            Error::new(
                "RESULT_ORIGIN_INVALID",
                "retained producer module receipt is malformed",
            )
        })?;
    let native_input_id = match retained.get("native_input_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(id)) => Some(id.clone()),
        _ => {
            return Err(Error::new(
                "RESULT_ORIGIN_INVALID",
                "retained producer native input ID is malformed",
            ));
        }
    };
    Ok(
        model::text(retained, "assignment_id")? == sealed.assignment_id
            && model::text(retained, "dispatch_operation_id")? == sealed.dispatch_operation_id
            && model::text(retained, "attempt_id")? == sealed.attempt_id
            && model::text(retained, "task_id")? == sealed.task_id
            && model::positive(retained, "task_revision")? == sealed.task_revision
            && model::text(retained, "task_snapshot_sha256")? == sealed.task_snapshot_sha256
            && model::text(retained, "source_text_sha256")? == sealed.source_text_sha256
            && retained["source_text_bytes"].as_u64() == Some(sealed.source_text_bytes)
            && model::text(retained, "native_payload_sha256")? == sealed.native_payload_sha256
            && retained["native_payload_bytes"].as_u64() == Some(sealed.native_payload_bytes)
            && native_input_id == sealed.native_input_id
            && module_receipt == sealed.module_receipt,
    )
}

fn check_attempt_scope(attempt: &Value, origin: &NormalizedResultOriginContext) -> Result<()> {
    if attempt["task_id"] != origin.task_id
        || attempt["task_revision"] != origin.task_revision
        || attempt["attempt_id"] != origin.attempt_id
        || attempt["binding_id"] != origin.binding_id
        || attempt["binding_generation"] != origin.binding_generation
    {
        return Err(Error::new(
            "CANDIDATE_SCOPE",
            "normalized result belongs to another Task Attempt or binding generation",
        ));
    }
    Ok(())
}

fn producer_origin(retained: &Value) -> Result<NormalizedResultProducerOrigin> {
    Ok(NormalizedResultProducerOrigin {
        assignment_id: model::text(retained, "assignment_id")?.to_owned(),
        dispatch_operation_id: model::text(retained, "dispatch_operation_id")?.to_owned(),
        attempt_id: model::text(retained, "attempt_id")?.to_owned(),
        task_id: model::text(retained, "task_id")?.to_owned(),
        task_revision: model::positive(retained, "task_revision")?,
        task_snapshot_sha256: model::text(retained, "task_snapshot_sha256")?.to_owned(),
        source_text_sha256: model::text(retained, "source_text_sha256")?.to_owned(),
        source_text_bytes: retained["source_text_bytes"].as_u64().ok_or_else(|| {
            Error::new(
                "RESULT_ORIGIN_INVALID",
                "producer source length is malformed",
            )
        })?,
        native_payload_sha256: model::text(retained, "native_payload_sha256")?.to_owned(),
        native_payload_bytes: retained["native_payload_bytes"].as_u64().ok_or_else(|| {
            Error::new(
                "RESULT_ORIGIN_INVALID",
                "producer payload length is malformed",
            )
        })?,
        completion_condition: model::text(retained, "completion_condition")?.to_owned(),
        execution_complete: retained["execution_complete"].as_bool().ok_or_else(|| {
            Error::new(
                "RESULT_ORIGIN_INVALID",
                "producer execution status is malformed",
            )
        })?,
        task_completion: model::text(retained, "task_completion")?.to_owned(),
        disposition: model::text(retained, "disposition")?.to_owned(),
        native_input_id: retained["native_input_id"].as_str().map(str::to_owned),
        module_receipt: serde_json::from_value(retained["module_receipt"].clone()).map_err(
            |_| {
                Error::new(
                    "RESULT_ORIGIN_INVALID",
                    "producer module receipt is malformed",
                )
            },
        )?,
    })
}

fn validate_receipt(
    db: &Connection,
    binding: &Value,
    receipt: &ModuleReceiptIdentity,
) -> Result<()> {
    let binding_id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let outcome = RuntimeOutcome {
        operation_id: receipt.operation_id.clone(),
        outcome: EffectOutcome::Unknown,
        native_scope_key: binding["native_scope_key"].as_str().map(str::to_owned),
        native_root_id: binding["native_root_id"].as_str().map(str::to_owned),
        turn_id: None,
        native_input_id: None,
        details: json!({"module_receipt":receipt}),
    };
    runtime::validate_module_receipt_for_operation(db, binding_id, generation, binding, &outcome)?;
    Ok(())
}

fn same_module(left: &ModuleReceiptIdentity, right: &ModuleReceiptIdentity) -> bool {
    left.module_id == right.module_id
        && left.artifact == right.artifact
        && left.protocol == right.protocol
        && left.binding_id == right.binding_id
        && left.binding_generation == right.binding_generation
}
