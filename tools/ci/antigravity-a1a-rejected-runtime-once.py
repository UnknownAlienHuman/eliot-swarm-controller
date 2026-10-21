from pathlib import Path

producers_path = Path("crates/swarm-kernel-host/src/store/producers.rs")
producers = producers_path.read_text(encoding="utf-8")
producer_marker = "\npub(super) fn bind(\n"
if producers.count(producer_marker) != 1:
    raise SystemExit("producer bind boundary changed")
producer_helper = r'''

/// Apply a validated warm-stream terminal fact to the producer that was
/// already admitted for this exact dispatch. A terminal receipt does not need
/// to repeat the normalized dispatch admission: Store already retained that
/// immutable identity when native input was accepted.
pub(super) fn record_warm_terminal(
    tx: &Transaction<'_>,
    operation: &Value,
    outcome: &RuntimeOutcome,
    terminal: &Value,
    now: i64,
) -> Result<Value> {
    if operation["method"] != "task.dispatch"
        || outcome.operation_id != model::text(operation, "operation_id")?
        || terminal["assignment_id"] != outcome.operation_id
        || outcome.details["completion_condition"] != "native_terminal_result_observed"
    {
        return Err(Error::invalid(
            "warm terminal producer must belong to its exact task.dispatch Operation",
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
    {
        return Err(Error::conflict(
            "warm terminal producer cannot update a resolved or differently started Attempt",
        ));
    }
    let disposition = terminal["disposition"]
        .as_str()
        .filter(|value| matches!(*value, "completed" | "failed" | "cancelled"))
        .ok_or_else(|| Error::invalid("warm terminal producer has no closed disposition"))?;
    let outcome_matches = match disposition {
        "completed" => matches!(outcome.outcome, crate::runtime::EffectOutcome::Applied),
        "failed" | "cancelled" => {
            matches!(outcome.outcome, crate::runtime::EffectOutcome::Rejected)
        }
        _ => false,
    };
    if !outcome_matches
        || !terminal["local_execution_ref"].is_object()
        || terminal["native_session_id"].as_str() != outcome.native_root_id.as_deref()
        || terminal["local_execution_ref"]["input_operation_id"] != outcome.operation_id
    {
        return Err(Error::invalid(
            "warm terminal producer differs from its validated terminal outcome",
        ));
    }

    let mut producers: Vec<Value> = serde_json::from_value(attempt["producers"].clone())?;
    let matching = producers
        .iter()
        .enumerate()
        .filter(|(_, producer)| {
            producer["assignment_id"] == outcome.operation_id
                || producer["dispatch_operation_id"] == outcome.operation_id
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        return Err(Error::conflict(
            "dispatch has more than one retained producer identity",
        ));
    }

    let producer = if let Some(index) = matching.first().copied() {
        if !producers[index]["native_session_id"].is_null()
            && producers[index]["native_session_id"] != terminal["native_session_id"]
        {
            return Err(Error::conflict(
                "warm terminal producer changed its native session identity",
            ));
        }
        if let Some(existing) = producers[index]["disposition"].as_str()
            && matches!(existing, "completed" | "failed" | "cancelled")
            && existing != disposition
        {
            return Err(Error::conflict(
                "warm terminal disposition conflicts with retained producer evidence",
            ));
        }
        producers[index]["dispatch_operation_id"] = json!(outcome.operation_id);
        producers[index]["native_session_id"] = terminal["native_session_id"].clone();
        producers[index]["completion_condition"] = json!("native_terminal_result_observed");
        producers[index]["execution_complete"] = json!(false);
        producers[index]["task_completion"] = json!("unknown");
        producers[index]["disposition"] = json!(disposition);
        producers[index]["local_execution_ref"] = terminal["local_execution_ref"].clone();
        producers[index].clone()
    } else {
        let mut producer = terminal.clone();
        producer["dispatch_operation_id"] = json!(outcome.operation_id);
        producer["completion_condition"] = json!("native_terminal_result_observed");
        producer["execution_complete"] = json!(false);
        producer["task_completion"] = json!("unknown");
        producers.push(producer.clone());
        producer
    };

    let changed = tx.execute(
        "UPDATE attempts SET state=CASE WHEN state='reserved' THEN 'running' ELSE state END,producers_json=?2,updated_at_ms=?3 WHERE attempt_id=?1 AND released_at_ms IS NULL",
        params![attempt_id, model::canonical(&json!(producers))?, now],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "warm terminal producer Attempt changed before retention",
        ));
    }
    Ok(producer)
}
'''
producers = producers.replace(producer_marker, producer_helper + producer_marker, 1)
producers_path.write_text(producers, encoding="utf-8")

runtime_path = Path("crates/swarm-kernel-host/src/store/runtime.rs")
runtime = runtime_path.read_text(encoding="utf-8")
old = '''    // The normalized dispatch already retained one producer for this exact
    // Operation. Its warm receipt is validated above and retained on the
    // Operation; appending another producer would duplicate its identity.
    if normalized_dispatch_admission.is_none()
        && let Some(producer) = warm_producer
    {
        let attempt = model::text(&o, "attempt_id")?;
        tx.execute(
            "UPDATE attempts SET state=CASE WHEN state='reserved' THEN 'running' ELSE state END,producers_json=json_insert(producers_json,'$[#]',json(?2)),updated_at_ms=?3 WHERE attempt_id=?1 AND released_at_ms IS NULL",
            params![attempt, model::canonical(&producer)?, now],
        )?;
    }
'''
new = '''    // Native input admission and terminal execution are two observations of
    // the same dispatch producer. A rejected terminal does not repeat its
    // admission receipt; update the already retained identity instead of
    // appending a second producer.
    if normalized_dispatch_admission.is_none()
        && let Some(producer) = warm_producer
    {
        producers::record_warm_terminal(&tx, &o, &r, &producer, now)?;
    }
'''
if runtime.count(old) != 1:
    raise SystemExit("normalized warm producer fallback changed")
runtime = runtime.replace(old, new, 1)
runtime_path.write_text(runtime, encoding="utf-8")
