//! Task-specific native run identity. Registration observes work; it never starts it.
use super::{operations, tasks};
use crate::{
    error::{Error, Result},
    model::{self, Principal},
    runtime::{RuntimeOutcome, batch},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

fn observation(
    db: &Connection,
    binding: &Value,
    requested: Option<i64>,
) -> Result<Option<(i64, Value, i64)>> {
    let id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let selected = requested.or_else(|| binding["observation"]["native_observation_id"].as_i64());
    let row: Option<(i64, String, i64)> = if let Some(selected) = selected {
        db.query_row(
            "SELECT observation_id,payload_json,recorded_at_ms FROM observations
             WHERE observation_id=?1 AND binding_id=?2 AND binding_generation=?3 AND kind='runtime.state'",
            params![selected, id, generation],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).optional()?
    } else if binding["observation"]["native"].is_object() {
        // Existing databases predate the materialized observation pointer. Match
        // the actual projection, not the newest row (which may be a stale event).
        db.query_row(
            "SELECT observation_id,payload_json,recorded_at_ms FROM observations
             WHERE binding_id=?1 AND binding_generation=?2 AND kind='runtime.state'
               AND payload_json=?3 ORDER BY observation_id DESC LIMIT 1",
            params![
                id,
                generation,
                model::canonical(&binding["observation"]["native"])?
            ],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?
    } else {
        None
    };
    match row {
        Some((id, raw, time)) => Ok(Some((id, serde_json::from_str(&raw)?, time))),
        None if selected.is_some() => Err(Error::new(
            "OBSERVATION_NOT_FOUND",
            "no runtime observation at that ID on this binding/generation",
        )),
        None => Ok(None),
    }
}

/// Pages one immutable retained observation; reaching its end says nothing about
/// children the native adapter has never observed.
pub(super) fn family(db: &Connection, v: &Value) -> Result<Value> {
    model::fields(
        v,
        &[
            "binding_id",
            "generation",
            "observation_id",
            "after",
            "limit",
        ],
    )?;
    let binding = operations::get_binding(
        db,
        model::text(v, "binding_id")?,
        model::positive(v, "generation")?,
    )?;
    let selected = v
        .get("observation_id")
        .map(|_| model::positive(v, "observation_id"))
        .transpose()?;
    let (limit, after) = super::page(v)?;
    if after > 0 && selected.is_none() {
        return Err(Error::invalid(
            "reuse observation_id when reading the next family page",
        ));
    }
    let Some((id, state, at)) = observation(db, &binding, selected)? else {
        return Ok(
            json!({"available":false,"family_completeness":"unknown","items":null,"next_after":null}),
        );
    };
    let children = state["observed_children"].as_array().ok_or_else(|| {
        Error::new(
            "OBSERVATION_INCOMPLETE",
            "observation has no child inventory",
        )
    })?;
    let after =
        usize::try_from(after).map_err(|_| Error::invalid("after exceeds platform range"))?;
    if after > children.len() {
        return Err(Error::invalid("after exceeds this observation's inventory"));
    }
    let end = children.len().min(after.saturating_add(limit as usize));
    // Post-projection limits (§8.1) apply to the projected member page:
    // an oversized member becomes an explicit gap reference at its own
    // position, and a budget stop shrinks the page — next_after then
    // resumes exactly at the first unemitted member.
    let entries: Vec<Value> = children[after..end].to_vec();
    let limited = super::projection::limit_items(entries, super::projection::family_gap_reference)?;
    let end = after + limited.consumed;
    let page = &limited.items;
    let turns: Vec<&Value> = state["turns"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|t| {
            t["sessionId"] == state["native_root_id"]
                || page
                    .iter()
                    .filter(|c| c.get("gap").is_none())
                    .any(|c| c["sessionId"] == t["sessionId"])
        })
        .collect();
    // §13 #10: a member that disappeared from the newest native
    // enumeration is retained by the snapshot with observed_now=false.
    // The frame names those members as retained stale — retained and
    // unknown, never silently dropped and never marked terminal.
    let retained_stale_members: Vec<Value> = children
        .iter()
        .filter(|c| c["observed_now"] == false)
        .filter_map(|c| c["sessionId"].as_str().map(|s| json!(s)))
        .collect();
    let frame = super::projection::frame(
        "family_observation",
        json!({"observation_id": id, "after": after,
               "next_after": if end < children.len() { Some(end) } else { None }}),
        &limited,
        limit,
        after > 0,
        end < children.len(),
        state["family_completeness"] == "complete" && limited.gap_count == 0,
        retained_stale_members,
    )?;
    Ok(
        json!({"available":true,"observation_id":id,"observed_at_ms":at,
        "binding_id":binding["binding_id"],"generation":binding["generation"],
        "connection":binding["observation"]["connection"],
        "family_completeness":state["family_completeness"],"gaps":state["gaps"],
        "native_root_id":state["native_root_id"],"root":state["session"],
        "items":page,"turns":turns,"retained_child_count":children.len(),
        "enumeration_complete":end==children.len(),
        "next_after":if end<children.len(){Some(end)}else{None},
        "projection":frame}),
    )
}

fn matching_run<'a>(state: &'a Value, session: &str, run: &str) -> Option<&'a Value> {
    let turns = state["turns"].as_array().into_iter().flatten();
    let child_turns = state["observed_children"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| &c["last_turn"]);
    let mut observed = None;
    for t in turns.chain(child_turns) {
        if t["sessionId"] == session && t["turnId"] == run {
            // A child can appear both in `turns` and `last_turn`. Identical
            // copies are one fact; conflicting copies cannot select a terminal
            // disposition by array order or discharge the producer.
            if observed.is_some_and(|previous| previous != t) {
                return None;
            }
            observed = Some(t);
        }
    }
    observed
}

#[cfg(test)]
mod receipt_ambiguity_tests {
    use super::*;

    #[test]
    fn identical_run_copies_settle_once_but_conflicts_remain_unresolved() {
        let terminal = json!({"sessionId":"session","turnId":"turn",
            "terminal":"completed","event":"turn.completed","viewCursor":"cursor"});
        let producer =
            json!({"native_session_id":"session","native_run_id":"turn","disposition":"admitted"});
        let mut consistent = producer.clone();
        apply_evidence(
            &mut consistent,
            &json!({"turns":[terminal.clone()],
            "observed_children":[{"sessionId":"session","last_turn":terminal.clone()}]}),
            Some(1),
        );
        assert_eq!(consistent["disposition"], "completed");
        let mut conflicted = terminal.clone();
        conflicted["terminal"] = json!("failed");
        for turns in [
            json!([terminal.clone(), conflicted.clone()]),
            json!([conflicted.clone(), terminal.clone()]),
        ] {
            let mut unresolved = producer.clone();
            apply_evidence(&mut unresolved, &json!({"turns":turns}), Some(2));
            assert_eq!(unresolved["disposition"], "admitted");
        }
        let mut unresolved = producer;
        apply_evidence(
            &mut unresolved,
            &json!({"turns":[terminal],
            "observed_children":[{"sessionId":"session","last_turn":conflicted}]}),
            Some(3),
        );
        assert_eq!(unresolved["disposition"], "admitted");
    }

    #[test]
    fn child_terminal_diagnostics_are_bounded_and_match_exact_run() {
        let terminal = json!({
            "sessionId":"ses_child",
            "turnId":"run_child",
            "terminal":"failed",
            "event":"evt_child_failed",
            "viewCursor":3,
            "stage":"execution_failed",
            "error_code":"NATIVE_CHILD_FAILED"
        });
        let state = json!({
            "turns":[],
            "observed_children":[{"sessionId":"ses_child","last_turn":terminal}]
        });
        let producer = json!({
            "native_session_id":"ses_child",
            "native_run_id":"run_child",
            "disposition":"admitted"
        });

        let mut valid = producer.clone();
        apply_evidence(&mut valid, &state, Some(7));
        assert_eq!(valid["disposition"], "failed");
        assert_eq!(valid["terminal_evidence"]["stage"], "execution_failed");
        assert_eq!(
            valid["terminal_evidence"]["error_code"],
            "NATIVE_CHILD_FAILED"
        );

        for (field, value) in [
            ("stage", json!("raw stage text")),
            ("error_code", json!("raw native error text")),
            ("error_code", json!("A".repeat(65))),
        ] {
            let mut invalid_state = state.clone();
            invalid_state["observed_children"][0]["last_turn"][field] = value;
            let mut sanitized = producer.clone();
            apply_evidence(&mut sanitized, &invalid_state, Some(8));
            assert_eq!(sanitized["disposition"], "failed");
            assert_eq!(sanitized["terminal_evidence"][field], Value::Null);
            assert_eq!(sanitized["terminal_evidence"]["observation_id"], 8);
        }

        let mut wrong_run = producer;
        wrong_run["native_run_id"] = json!("run_other");
        apply_evidence(&mut wrong_run, &state, Some(9));
        assert_eq!(wrong_run["disposition"], "admitted");
        assert!(wrong_run["terminal_evidence"].is_null());
    }
}
fn run_observed(state: &Value, session: &str, run: &str) -> bool {
    let member = state["native_root_id"] == session
        || state["observed_children"]
            .as_array()
            .is_some_and(|children| children.iter().any(|c| c["sessionId"] == session));
    member
        && (matching_run(state, session, run).is_some()
            || (state["session"]["sessionId"] == session
                && state["session"]["activeTurnId"] == run)
            || state["observed_children"]
                .as_array()
                .is_some_and(|children| {
                    children.iter().any(|c| {
                        c["sessionId"] == session
                            && c["snapshot"]["sessionId"] == session
                            && c["snapshot"]["activeTurnId"] == run
                    })
                }))
}

fn bounded_terminal_stage(value: &Value) -> Value {
    value
        .as_str()
        .filter(|stage| {
            matches!(
                *stage,
                "execution_succeeded"
                    | "execution_failed"
                    | "execution_interrupted"
                    | "inbox_cancelled_before_delivery"
            )
        })
        .map(|stage| json!(stage))
        .unwrap_or(Value::Null)
}

fn bounded_terminal_error_code(value: &Value) -> Value {
    let Some(code) = value.as_str().filter(|code| {
        !code.is_empty()
            && code.len() <= 64
            && code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
            && code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    }) else {
        return Value::Null;
    };
    json!(code)
}

/// Terminal facts address an exact run; missing IDs never compare equal as null.
/// A terminal fact does not accept a Task or release a native session.
pub(super) fn apply_evidence(producer: &mut Value, state: &Value, observation_id: Option<i64>) {
    crate::runtime::prepared::apply_input_execution(producer, state, observation_id);
    let Some(session) = producer["native_session_id"]
        .as_str()
        .filter(|x| !x.is_empty())
    else {
        return;
    };
    let Some(run) = producer["native_run_id"].as_str().filter(|x| !x.is_empty()) else {
        return;
    };
    let Some(event) = matching_run(state, session, run) else {
        return;
    };
    let Some(terminal) = event["terminal"]
        .as_str()
        .filter(|s| matches!(*s, "completed" | "failed" | "cancelled"))
    else {
        return;
    };
    if matches!(
        producer["disposition"].as_str(),
        Some("completed" | "failed" | "cancelled")
    ) {
        return;
    }
    producer["disposition"] = json!(terminal);
    producer["terminal_evidence"] = json!({
        "observation_id":observation_id,
        "event":event["event"],
        "view_cursor":event["viewCursor"],
        "stage":bounded_terminal_stage(&event["stage"]),
        "error_code":bounded_terminal_error_code(&event["error_code"])
    });
}

/// Record a one-shot producer by its dispatch Operation, without pretending
/// that a vendor session is a native root or that a batch has a turn ID.
pub(super) fn record_batch(
    tx: &Transaction<'_>,
    operation: &Value,
    outcome: &RuntimeOutcome,
    now: i64,
) -> Result<Value> {
    if operation["method"] != "task.dispatch"
        || outcome.operation_id != model::text(operation, "operation_id")?
    {
        return Err(Error::invalid(
            "batch producer must belong to its exact task.dispatch Operation",
        ));
    }
    let attempt_id = model::text(operation, "attempt_id")?;
    let attempt = tasks::get_attempt(tx, attempt_id)?;
    if !attempt["released_at_ms"].is_null()
        || matches!(
            attempt["state"].as_str(),
            Some("accepted" | "failed" | "cancelled" | "superseded")
        )
        || attempt["start_operation_id"] != outcome.operation_id
    {
        return Err(Error::conflict(
            "batch terminal evidence cannot be attached to a resolved or differently started Attempt",
        ));
    }
    let producer = batch::dispatch_producer(outcome);
    let mut producers: Vec<Value> = serde_json::from_value(attempt["producers"].clone())?;
    if let Some(existing) = producers
        .iter()
        .find(|item| item["dispatch_operation_id"] == outcome.operation_id)
    {
        if model::canonical(existing)? != model::canonical(&producer)? {
            return Err(Error::conflict(
                "batch dispatch Operation already has different producer evidence",
            ));
        }
        return Ok(existing.clone());
    }
    producers.push(producer.clone());
    tx.execute(
        "UPDATE attempts SET state=CASE WHEN state='reserved' THEN 'running' ELSE state END,producers_json=?2,updated_at_ms=?3 WHERE attempt_id=?1 AND released_at_ms IS NULL",
        params![attempt_id, model::canonical(&json!(producers))?, now],
    )?;
    Ok(producer)
}

/// Record the normalized descriptor-backed dispatch receipt for the exact
/// Attempt that owns the Operation. The allowlisted execution facts distinguish
/// input admission from one completed native turn and never complete a Task.
pub(super) fn record_task_dispatch(
    tx: &Transaction<'_>,
    operation: &Value,
    outcome: &RuntimeOutcome,
    admission: &swarm_contracts::runtime::TaskDispatchAdmissionReceipt,
    now: i64,
) -> Result<Value> {
    if operation["method"] != "task.dispatch"
        || outcome.operation_id != model::text(operation, "operation_id")?
        || admission.operation_id != outcome.operation_id
        || admission.binding_id != model::text(operation, "binding_id")?
        || admission.binding_generation != model::positive(operation, "binding_generation")?
    {
        return Err(Error::invalid(
            "normalized dispatch producer must belong to its exact task.dispatch Operation",
        ));
    }
    let attempt_id = model::text(operation, "attempt_id")?.to_owned();
    let attempt = tasks::get_attempt(tx, &attempt_id)?;
    if !attempt["released_at_ms"].is_null()
        || matches!(
            attempt["state"].as_str(),
            Some("accepted" | "failed" | "cancelled" | "superseded")
        )
        || attempt["start_operation_id"] != outcome.operation_id
        || admission.attempt_id != attempt_id
        || admission.task_id != model::text(&attempt, "task_id")?
        || admission.task_revision != model::positive(&attempt, "task_revision")?
    {
        return Err(Error::conflict(
            "normalized dispatch admission cannot be attached to a resolved or differently started Attempt",
        ));
    }

    let reported_completion = outcome.details["completion_condition"].as_str();
    let reported_execution_complete = outcome.details.get("execution_complete");
    let reported_task_completion = outcome.details.get("task_completion");
    let (completion_condition, execution_complete, disposition) = if reported_completion
        == Some("native_turn_completed")
    {
        if reported_execution_complete.and_then(Value::as_bool) != Some(true)
            || reported_task_completion.is_some_and(|value| value != "unknown")
            || !matches!(outcome.outcome, crate::runtime::EffectOutcome::Applied)
        {
            return Err(Error::new(
                "TASK_DISPATCH_ADMISSION_INVALID",
                "completed-turn producer facts are inconsistent",
            ));
        }
        ("native_turn_completed", true, "completed")
    } else {
        if reported_completion.is_some_and(|condition| {
            !matches!(
                condition,
                "native_input_admitted" | "native_result_observed"
            )
        }) || reported_execution_complete.is_some_and(|value| value != false)
            || reported_task_completion.is_some_and(|value| value != "unknown")
            || !matches!(
                outcome.outcome,
                crate::runtime::EffectOutcome::Applied | crate::runtime::EffectOutcome::Accepted
            )
        {
            return Err(Error::new(
                "TASK_DISPATCH_ADMISSION_INVALID",
                "dispatch producer may claim input admission only, not native or Task completion",
            ));
        }
        ("native_input_admitted", false, "admitted")
    };
    let task_completion = "unknown";

    let producer = json!({
        "assignment_id": outcome.operation_id,
        "dispatch_operation_id": outcome.operation_id,
        "attempt_id": admission.attempt_id,
        "task_id": admission.task_id,
        "task_revision": admission.task_revision,
        "task_snapshot_sha256": admission.task_snapshot_sha256,
        "source_text_sha256": admission.source_text_sha256,
        "source_text_bytes": admission.source_text_bytes,
        "native_session_id": outcome.native_root_id,
        "native_input_id": admission.native_input_id,
        "native_payload_sha256": admission.native_payload_sha256,
        "native_payload_bytes": admission.native_payload_bytes,
        "module_receipt": admission.module_receipt,
        "admission_kind": "normalized_task_dispatch",
        "completion_condition": completion_condition,
        "execution_complete": execution_complete,
        "task_completion": task_completion,
        "disposition": disposition
    });
    let mut producers: Vec<Value> = serde_json::from_value(attempt["producers"].clone())?;
    if let Some(index) = producers.iter().position(|item| {
        item["assignment_id"] == outcome.operation_id
            || item["dispatch_operation_id"] == outcome.operation_id
    }) {
        let mut existing_identity = producers[index].clone();
        let mut producer_identity = producer.clone();
        if let Some(fields) = existing_identity.as_object_mut() {
            fields.remove("native_session_id");
        }
        if let Some(fields) = producer_identity.as_object_mut() {
            fields.remove("native_session_id");
        }
        if model::canonical(&existing_identity)? != model::canonical(&producer_identity)? {
            return Err(Error::conflict(
                "normalized dispatch Operation already has different producer evidence",
            ));
        }
        if !producers[index]["native_session_id"].is_null()
            && !producer["native_session_id"].is_null()
            && producers[index]["native_session_id"] != producer["native_session_id"]
        {
            return Err(Error::conflict(
                "normalized dispatch Operation changed its native session identity",
            ));
        }
        if producers[index]["native_session_id"].is_null()
            && !producer["native_session_id"].is_null()
        {
            producers[index]["native_session_id"] = producer["native_session_id"].clone();
            tx.execute(
                "UPDATE attempts SET producers_json=?2,updated_at_ms=?3 WHERE attempt_id=?1 AND released_at_ms IS NULL",
                params![&attempt_id, model::canonical(&json!(producers))?, now],
            )?;
        }
        return Ok(producers[index].clone());
    }
    producers.push(producer.clone());
    tx.execute(
        "UPDATE attempts SET state=CASE WHEN state='reserved' THEN 'running' ELSE state END,producers_json=?2,updated_at_ms=?3 WHERE attempt_id=?1 AND released_at_ms IS NULL",
        params![&attempt_id, model::canonical(&json!(producers))?, now],
    )?;
    Ok(producer)
}

pub(super) fn bind(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    operation: &str,
    now: i64,
) -> Result<Value> {
    let attempt_id = model::text(v, "attempt_id")?;
    let assignment = model::text(v, "assignment_id")?;
    let session = model::text(v, "native_session_id")?;
    let run = model::text(v, "native_run_id")?;
    let evidence_id = model::positive(v, "observation_id")?;
    let a = tasks::get_attempt(tx, attempt_id)?;
    super::gm::require_attempt_control(tx, p, &a)?;
    if !a["released_at_ms"].is_null()
        || matches!(
            a["state"].as_str(),
            Some("accepted" | "failed" | "cancelled" | "superseded")
        )
    {
        return Err(Error::conflict(
            "cannot attach work to a terminal or released Attempt",
        ));
    }
    let binding = operations::get_binding(
        tx,
        model::text(&a, "binding_id")?,
        model::positive(&a, "binding_generation")?,
    )?;
    if !binding["released_at_ms"].is_null() {
        return Err(Error::new("BINDING_CLOSED", "binding is released"));
    }
    let (_, state, _) = observation(tx, &binding, Some(evidence_id))?
        .ok_or_else(|| Error::new("OBSERVATION_NOT_FOUND", "native evidence is missing"))?;
    if !binding["native_root_id"].is_string()
        || state["native_root_id"] != binding["native_root_id"]
        || state["native_scope_key"] != binding["native_scope_key"]
        || !run_observed(&state, session, run)
    {
        return Err(Error::new(
            "PRODUCER_NOT_OBSERVED",
            "that exact native run is not evidenced in this binding's recorded family",
        ));
    }
    let mut producers: Vec<Value> = serde_json::from_value(a["producers"].clone())?;
    let index = if let Some(index) = producers
        .iter()
        .position(|x| x["assignment_id"] == assignment)
    {
        if producers[index]["native_session_id"] != session
            || producers[index]["native_run_id"] != run
        {
            return Err(Error::conflict(
                "assignment identity cannot be rebound to another native run",
            ));
        }
        index
    } else {
        if producers
            .iter()
            .any(|x| x["native_session_id"] == session && x["native_run_id"] == run)
        {
            return Err(Error::conflict(
                "this Attempt already tracks that run under another assignment ID",
            ));
        }
        producers.push(
            json!({"assignment_id":assignment,"native_session_id":session,"native_run_id":run,
            "disposition":"admitted","observed_in":evidence_id}),
        );
        producers.len() - 1
    };
    apply_evidence(&mut producers[index], &state, Some(evidence_id));
    // The requested observation may predate a terminal already in the current view.
    apply_evidence(
        &mut producers[index],
        &binding["observation"]["native"],
        binding["observation"]["native_observation_id"].as_i64(),
    );
    tx.execute("UPDATE attempts SET producers_json=?2,state=CASE WHEN state='reserved' THEN 'running' ELSE state END,updated_at_ms=?3 WHERE attempt_id=?1",
        params![attempt_id,model::canonical(&json!(producers))?,now])?;
    tx.execute("UPDATE operations SET task_id=?2,attempt_id=?3,binding_id=?4,binding_generation=?5 WHERE operation_id=?1",
        params![operation,a["task_id"].as_str(),attempt_id,a["binding_id"].as_str(),a["binding_generation"].as_i64()])?;
    super::capacity::sync_attempt(tx, attempt_id, now)?;
    Ok(
        json!({"operation_id":operation,"attempt_id":attempt_id,"producer":producers[index],"native_start_sent":false}),
    )
}
