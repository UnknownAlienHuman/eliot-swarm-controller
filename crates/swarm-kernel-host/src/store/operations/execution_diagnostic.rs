//! Closed execution cards backed by the exact retained dispatch Assignment.
use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

type AttemptAssignmentRow = (
    String,
    i64,
    Option<String>,
    Option<i64>,
    Option<String>,
    String,
);
type ExecutionObservationRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
    String,
);

fn execution_diagnostic_damage(message: &'static str) -> Error {
    Error::new("OBJECT_SCOPE_DAMAGED", message)
}

fn valid_opencode_stage(stage: &str) -> bool {
    matches!(
        stage,
        "execution_succeeded"
            | "execution_failed"
            | "execution_interrupted"
            | "inbox_cancelled_before_delivery"
            | "native_terminal"
    )
}

fn valid_native_error_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

pub(super) fn for_operation(
    db: &Connection,
    operation_id: &str,
    retained: &Value,
    native_refs_damaged: bool,
) -> Result<Option<Value>> {
    if native_refs_damaged {
        return Err(execution_diagnostic_damage(
            "optional Operation native refs are malformed",
        ));
    }
    let refs = &retained["native_refs"];
    let task_id = model::text(retained, "task_id")?;
    let attempt_id = model::text(retained, "attempt_id")?;
    let binding_id = model::text(retained, "binding_id")?;
    let binding_generation = model::positive(retained, "binding_generation")?;
    let attempt: Option<AttemptAssignmentRow> = db
        .query_row(
            "SELECT task_id,task_revision,binding_id,binding_generation,start_operation_id,producers_json \
             FROM attempts WHERE attempt_id=?1",
            [attempt_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((
        attempt_task,
        attempt_revision,
        attempt_binding,
        attempt_generation,
        start,
        raw_producers,
    )) = attempt
    else {
        return Err(execution_diagnostic_damage(
            "exact task.dispatch Attempt is not retained",
        ));
    };
    if attempt_task != task_id
        || attempt_binding.as_deref() != Some(binding_id)
        || attempt_generation != Some(binding_generation)
    {
        return Err(execution_diagnostic_damage(
            "Operation and frozen Attempt binding generations disagree",
        ));
    }
    let producers: Vec<Value> = serde_json::from_str(&raw_producers)
        .map_err(|_| execution_diagnostic_damage("Attempt producer assignments are malformed"))?;
    let assigned: Vec<&Value> = producers
        .iter()
        .filter(|producer| {
            producer["assignment_id"].as_str() == Some(operation_id)
                && producer
                    .get("dispatch_operation_id")
                    .is_none_or(|value| value.as_str() == Some(operation_id))
        })
        .collect();
    if assigned.is_empty() {
        if !refs["input_execution"].is_null()
            || refs["execution_shape"] == crate::runtime::batch::EXECUTION_SHAPE
        {
            return Err(execution_diagnostic_damage(
                "terminal readback has no exact Attempt Assignment",
            ));
        }
        return Ok(None);
    }
    if assigned.len() != 1 || start.as_deref() != Some(operation_id) {
        return Err(execution_diagnostic_damage(
            "task.dispatch Assignment is duplicated or is not the frozen Attempt start",
        ));
    }
    let producer = assigned[0];
    for (field, expected) in [
        ("attempt_id", json!(attempt_id)),
        ("task_id", json!(task_id)),
        ("task_revision", json!(attempt_revision)),
        ("binding_id", json!(binding_id)),
        ("binding_generation", json!(binding_generation)),
    ] {
        if producer.get(field).is_some_and(|value| !value.is_null()) && producer[field] != expected
        {
            return Err(execution_diagnostic_damage(
                "Attempt Assignment identity differs from its Operation",
            ));
        }
    }
    if producer["execution_shape"] == crate::runtime::batch::EXECUTION_SHAPE {
        return command_task_dispatch_diagnostic(
            db,
            operation_id,
            retained,
            refs,
            producer,
            binding_id,
            binding_generation,
        );
    }
    opencode_task_dispatch_diagnostic(
        db,
        operation_id,
        refs,
        producer,
        binding_id,
        binding_generation,
    )
}

fn command_task_dispatch_diagnostic(
    db: &Connection,
    operation_id: &str,
    retained: &Value,
    refs: &Value,
    producer: &Value,
    binding_id: &str,
    binding_generation: i64,
) -> Result<Option<Value>> {
    if refs["execution_shape"] != crate::runtime::batch::EXECUTION_SHAPE
        || refs["dispatch_operation_id"] != operation_id
        || producer["dispatch_operation_id"] != operation_id
    {
        return Err(execution_diagnostic_damage(
            "sessionless result does not name the exact dispatch Operation",
        ));
    }
    let route_raw: Option<String> = db
        .query_row(
            "SELECT route_json FROM bindings WHERE binding_id=?1 AND generation=?2",
            params![binding_id, binding_generation],
            |row| row.get(0),
        )
        .optional()?;
    let route_raw = route_raw.ok_or_else(|| {
        execution_diagnostic_damage(
            "exact Command binding generation is unavailable for diagnostic projection",
        )
    })?;
    let route: Value = serde_json::from_str(&route_raw)
        .map_err(|_| execution_diagnostic_damage("retained Command route is malformed"))?;
    if !crate::runtime::batch::is_command_route(&route) {
        return Ok(None);
    }
    let result = &retained["result"];
    let details = &result["details"];
    if result["operation_id"] != operation_id
        || details["execution_shape"] != crate::runtime::batch::EXECUTION_SHAPE
    {
        return Err(execution_diagnostic_damage(
            "Command result is not linked to the exact dispatch Operation",
        ));
    }
    let terminal = &producer["terminal_evidence"];
    let completion = details["completion_condition"]
        .as_str()
        .ok_or_else(|| execution_diagnostic_damage("Command completion condition is malformed"))?;
    if completion != "native_result_observed" {
        if terminal["completion_condition"] != completion {
            return Err(execution_diagnostic_damage(
                "Command result and Attempt producer completion conditions disagree",
            ));
        }
        return Ok(None);
    }
    if terminal["completion_condition"] != completion
        || terminal["result_subtype"] != details["result_subtype"]
        || terminal["exit_code"] != details["exit_code"]
    {
        return Err(execution_diagnostic_damage(
            "Command terminal producer differs from its Operation result",
        ));
    }
    let subtype = details["result_subtype"].as_str();
    if subtype.is_some_and(|value| !matches!(value, "success" | "error" | "max_turns"))
        || (!details["result_subtype"].is_null() && subtype.is_none())
    {
        return Err(execution_diagnostic_damage(
            "Command result subtype is outside its closed vocabulary",
        ));
    }
    let exit_code = details["exit_code"].as_i64();
    if exit_code.is_some_and(|value| !(0..=255).contains(&value))
        || (!details["exit_code"].is_null() && exit_code.is_none())
    {
        return Err(execution_diagnostic_damage(
            "Command exit code is outside its bounded integer range",
        ));
    }
    if subtype.is_none() || exit_code.is_none() {
        return Err(execution_diagnostic_damage(
            "Command terminal receipt is missing bounded result fields",
        ));
    }
    let outcome = result["outcome"].as_str();
    let coherent_outcome = matches!(
        (outcome, subtype, exit_code),
        (Some("applied"), Some("success"), Some(0))
            | (
                Some("rejected"),
                Some("error"),
                Some(1 | 3 | 4 | 5 | 6 | 7 | 9 | 10 | 130)
            )
            | (Some("rejected"), Some("max_turns"), Some(8))
    );
    if !coherent_outcome {
        return Err(execution_diagnostic_damage(
            "Command Operation outcome, result subtype, and exit code disagree",
        ));
    }
    let mut card = serde_json::Map::new();
    if let Some(subtype) = subtype {
        card.insert("result_subtype".into(), json!(subtype));
    }
    if let Some(exit_code) = exit_code {
        card.insert("exit_code".into(), json!(exit_code));
    }
    Ok(Some(Value::Object(card)))
}

fn opencode_task_dispatch_diagnostic(
    db: &Connection,
    operation_id: &str,
    refs: &Value,
    producer: &Value,
    binding_id: &str,
    binding_generation: i64,
) -> Result<Option<Value>> {
    let terminal = &producer["terminal_evidence"];
    let proof = &refs["input_execution"];
    if !terminal.is_object() {
        if matches!(
            proof["disposition"].as_str(),
            Some("completed" | "failed" | "cancelled")
        ) {
            return Err(execution_diagnostic_damage(
                "OpenCode terminal proof has no exact Attempt producer evidence",
            ));
        }
        return Ok(None);
    }
    if !proof.is_object() {
        return Err(execution_diagnostic_damage(
            "OpenCode Attempt terminal evidence has no retained input proof",
        ));
    }
    let observation_id = terminal["observation_id"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(|| execution_diagnostic_damage("terminal EventRef has no observation ID"))?;
    let observation: Option<ExecutionObservationRow> = db
        .query_row(
            "SELECT source_stream_id,source_event_key,operation_id,binding_id,binding_generation,kind,payload_json \
             FROM observations WHERE observation_id=?1",
            [observation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((
        stream,
        key,
        observed_operation,
        observed_binding,
        observed_generation,
        kind,
        payload,
    )) = observation
    else {
        return Err(execution_diagnostic_damage(
            "OpenCode terminal observation is not retained",
        ));
    };
    let expected_stream = format!("opencode-execution:{binding_id}:{binding_generation}");
    let expected_key = format!("{operation_id}:{}", model::digest(payload.as_bytes()));
    let proof: Value = serde_json::from_str(&payload).map_err(|_| {
        execution_diagnostic_damage("OpenCode terminal observation payload is malformed")
    })?;
    let native_terminal = &proof["terminal"];
    let event = &native_terminal["event"];
    if !producer["native_run_id"].is_null()
        && producer["native_run_id"].as_str().is_none_or(str::is_empty)
    {
        return Err(execution_diagnostic_damage(
            "OpenCode Assignment run ID is malformed",
        ));
    }
    if stream != expected_stream
        || key != expected_key
        || observed_operation.as_deref() != Some(operation_id)
        || observed_binding.as_deref() != Some(binding_id)
        || observed_generation != Some(binding_generation)
        || kind != "opencode.input_execution"
        || proof != refs["input_execution"]
        || proof["operation_id"] != operation_id
        || proof["native_input_id"] != producer["native_input_id"]
        || proof["native_session_id"] != producer["native_session_id"]
        || (producer["native_run_id"].is_string()
            && proof["native_run_id"] != producer["native_run_id"])
        || proof["disposition"] != native_terminal["outcome"]
        || producer["disposition"] != proof["disposition"]
        || !matches!(
            native_terminal["outcome"].as_str(),
            Some("completed" | "failed" | "cancelled")
        )
        || terminal["event"] != *event
        || terminal["stage"] != native_terminal["stage"]
        || terminal["error_code"] != native_terminal["error_code"]
        || event
            .get("id")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        || event
            .get("seq")
            .and_then(Value::as_u64)
            .is_none_or(|seq| seq == 0)
    {
        return Err(execution_diagnostic_damage(
            "OpenCode terminal observation differs from the exact Attempt Assignment",
        ));
    }
    let stage = terminal["stage"]
        .as_str()
        .filter(|stage| valid_opencode_stage(stage))
        .ok_or_else(|| execution_diagnostic_damage("OpenCode terminal stage is invalid"))?;
    let mut card = serde_json::Map::new();
    card.insert("stage".into(), json!(stage));
    if !terminal["error_code"].is_null() {
        let code = terminal["error_code"]
            .as_str()
            .filter(|code| valid_native_error_code(code))
            .ok_or_else(|| execution_diagnostic_damage("OpenCode error code is invalid"))?;
        card.insert("error_code".into(), json!(code));
    }
    Ok(Some(Value::Object(card)))
}
