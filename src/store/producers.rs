//! Task-specific native run identity. Registration observes work; it never starts it.
use super::{operations, tasks};
use crate::{
    error::{Error, Result},
    model::{self, Principal},
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
    let page = &children[after..end];
    let turns: Vec<&Value> = state["turns"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|t| {
            t["sessionId"] == state["native_root_id"]
                || page.iter().any(|c| c["sessionId"] == t["sessionId"])
        })
        .collect();
    Ok(
        json!({"available":true,"observation_id":id,"observed_at_ms":at,
        "binding_id":binding["binding_id"],"generation":binding["generation"],
        "connection":binding["observation"]["connection"],
        "family_completeness":state["family_completeness"],"gaps":state["gaps"],
        "native_root_id":state["native_root_id"],"root":state["session"],
        "items":page,"turns":turns,"retained_child_count":children.len(),
        "enumeration_complete":end==children.len(),
        "next_after":if end<children.len(){Some(end)}else{None}}),
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
            if matches!(
                t["terminal"].as_str(),
                Some("completed" | "failed" | "cancelled")
            ) {
                return Some(t);
            }
            observed = Some(t);
        }
    }
    observed
}
fn matching_input<'a>(state: &'a Value, session: &str, input: &str) -> Option<&'a Value> {
    let turns = state["turns"].as_array().into_iter().flatten();
    let child_turns = state["observed_children"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| &c["last_turn"]);
    let mut found = None;
    for turn in turns.chain(child_turns) {
        if turn["sessionId"] == session && turn["inputId"] == input {
            if found.is_some() {
                return None;
            }
            found = Some(turn);
        }
    }
    found
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

/// Terminal facts address an exact run; missing IDs never compare equal as null.
/// A terminal fact does not accept a Task or release a native session.
pub(super) fn apply_evidence(producer: &mut Value, state: &Value, observation_id: Option<i64>) {
    let Some(session) = producer["native_session_id"]
        .as_str()
        .filter(|x| !x.is_empty())
        .map(str::to_owned)
    else {
        return;
    };
    if matches!(
        producer["disposition"].as_str(),
        Some("completed" | "failed" | "cancelled")
    ) {
        return;
    }
    if producer["native_run_id"]
        .as_str()
        .is_none_or(|run| run.is_empty())
    {
        let Some(input) = producer["native_input_id"]
            .as_str()
            .filter(|input| !input.is_empty())
            .map(str::to_owned)
        else {
            return;
        };
        let Some(event) = matching_input(state, &session, &input) else {
            return;
        };
        if event["operationId"] != producer["assignment_id"] {
            return;
        }
        let Some(run) = event["turnId"].as_str().filter(|run| !run.is_empty()) else {
            return;
        };
        producer["native_run_id"] = json!(run);
        producer["correlation_evidence"] = json!({
            "observation_id":observation_id,
            "input_id":input,
            "operation_id":event["operationId"],
            "run_id":run,
            "identity_kind":event["identityKind"],
            "event":event["event"]
        });
    }
    let Some(run) = producer["native_run_id"].as_str().filter(|x| !x.is_empty()) else {
        return;
    };
    let Some(event) = matching_run(state, &session, run) else {
        return;
    };
    let Some(terminal) = event["terminal"]
        .as_str()
        .filter(|s| matches!(*s, "completed" | "failed" | "cancelled"))
    else {
        return;
    };
    producer["disposition"] = json!(terminal);
    producer["terminal_evidence"] = json!({
        "observation_id":observation_id,
        "event":event["event"],
        "view_cursor":event["viewCursor"],
        "identity_kind":event["identityKind"],
        "native_outcome":event["nativeOutcome"]
    });
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
    p.owns(model::text(&a, "owner_id")?)?;
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
    Ok(
        json!({"operation_id":operation,"attempt_id":attempt_id,"producer":producers[index],"native_start_sent":false}),
    )
}
