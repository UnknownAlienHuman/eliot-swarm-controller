from pathlib import Path

# Preserve the self-contained warm terminal disposition when the normalized
# dispatch admission is merged into the Attempt producer.
producers_path = Path("crates/swarm-kernel-host/src/store/producers.rs")
text = producers_path.read_text(encoding="utf-8")
function_start = text.index("pub(super) fn record_task_dispatch(")
function_end = text.index("\npub(super) fn bind(", function_start)
function = text[function_start:function_end]

lifecycle_start = function.index(
    "    let (completion_condition, execution_complete, disposition) = if reported_completion"
)
lifecycle_end = function.index('    let task_completion = "unknown";')
new_lifecycle = r'''    let warm_terminal = reported_completion == Some("native_terminal_result_observed");
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
    } else if warm_terminal {
        let terminal = terminal_observation.ok_or_else(|| {
            Error::new(
                "TASK_DISPATCH_ADMISSION_INVALID",
                "warm terminal dispatch requires its validated self-contained producer",
            )
        })?;
        let disposition = terminal["disposition"].as_str().filter(|value| {
            matches!(*value, "completed" | "failed" | "cancelled")
        }).ok_or_else(|| {
            Error::new(
                "TASK_DISPATCH_ADMISSION_INVALID",
                "warm terminal producer has no closed disposition",
            )
        })?;
        let outcome_matches = match disposition {
            "completed" => matches!(outcome.outcome, crate::runtime::EffectOutcome::Applied),
            "failed" | "cancelled" => {
                matches!(outcome.outcome, crate::runtime::EffectOutcome::Rejected)
            }
            _ => false,
        };
        if reported_execution_complete.is_some_and(|value| value != false)
            || reported_task_completion.is_some_and(|value| value != "unknown")
            || !outcome_matches
        {
            return Err(Error::new(
                "TASK_DISPATCH_ADMISSION_INVALID",
                "warm terminal producer facts are inconsistent",
            ));
        }
        ("native_terminal_result_observed", false, disposition)
    } else {
        if reported_completion.is_some_and(|condition| {
            !matches!(condition, "native_input_admitted" | "native_result_observed")
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
'''
function = function[:lifecycle_start] + new_lifecycle + function[lifecycle_end:]

if function.count("    let producer = json!({") != 1:
    raise SystemExit("normalized producer constructor changed")
function = function.replace("    let producer = json!({", "    let mut producer = json!({", 1)

producer_end = '''    });
    let mut producers: Vec<Value> = serde_json::from_value(attempt["producers"].clone())?;'''
if function.count(producer_end) != 1:
    raise SystemExit("normalized producer tail changed")
function = function.replace(
    producer_end,
    '''    });
    if let Some(terminal) = terminal_observation {
        producer["local_execution_ref"] = terminal["local_execution_ref"].clone();
    }
    let mut producers: Vec<Value> = serde_json::from_value(attempt["producers"].clone())?;''',
    1,
)

old_skip = '''                "native_session_id" | "completion_condition" | "execution_complete" | "disposition"
'''
new_skip = '''                "native_session_id"
                    | "completion_condition"
                    | "execution_complete"
                    | "disposition"
                    | "local_execution_ref"
'''
if function.count(old_skip) != 1:
    raise SystemExit("producer lifecycle comparison changed")
function = function.replace(old_skip, new_skip, 1)

old_update = '''        if execution_complete {
            if matches!(
                producers[index]["disposition"].as_str(),
                Some("failed" | "cancelled")
            ) {
                return Err(Error::conflict(
                    "completed dispatch conflicts with retained terminal producer evidence",
                ));
            }
            for field in ["completion_condition", "execution_complete", "disposition"] {
                if producers[index][field] != producer[field] {
                    producers[index][field] = producer[field].clone();
                    changed = true;
                }
            }
        }
'''
new_update = '''        if execution_complete || warm_terminal {
            if let Some(existing) = producers[index]["disposition"].as_str()
                && matches!(existing, "completed" | "failed" | "cancelled")
                && existing != disposition
            {
                return Err(Error::conflict(
                    "dispatch terminal disposition conflicts with retained producer evidence",
                ));
            }
            for field in [
                "completion_condition",
                "execution_complete",
                "disposition",
                "local_execution_ref",
            ] {
                if producers[index][field] != producer[field] {
                    producers[index][field] = producer[field].clone();
                    changed = true;
                }
            }
        }
'''
if function.count(old_update) != 1:
    raise SystemExit("producer lifecycle update changed")
function = function.replace(old_update, new_update, 1)

text = text[:function_start] + function + text[function_end:]
producers_path.write_text(text, encoding="utf-8")

# Remove the old observation prerequisite from the smaller receipt regressions.
receipt_path = Path("crates/swarm-kernel-host/src/store/receipt_tests.rs")
text = receipt_path.read_text(encoding="utf-8")
start = text.index("fn warm_receipt(")
end = text.index("fn first_claude_receipt()", start)
replacement = r'''fn warm_receipt(_db: &mut Connection, status: &str) -> RuntimeOutcome {
    let fingerprint = json!({
        "input_operation_id":"dispatch",
        "native_conversation_id":"session",
        "bridge_boot_id":"boot",
        "result_ordinal":1,
        "response_sha256":model::digest(b"reply"),
        "status":status
    });
    RuntimeOutcome {
        operation_id: "dispatch".into(),
        outcome: if status == "SUCCESS" {
            EffectOutcome::Applied
        } else {
            EffectOutcome::Rejected
        },
        native_root_id: Some("session".into()),
        native_scope_key: Some("scope".into()),
        turn_id: None,
        native_input_id: None,
        details: json!({
            "completion_condition":"native_terminal_result_observed",
            "local_execution_ref":fingerprint
        }),
    }
}

#[test]
fn warm_terminal_receipts_settle_execution_without_accepting_task_or_inventing_run() {
    for (status, disposition, operation_state) in [
        ("SUCCESS", "completed", "settled"),
        ("ERROR", "failed", "rejected"),
        ("CANCELED", "cancelled", "rejected"),
    ] {
        let (mut db, p) = database(warm_stream::RUNTIME, warm_stream::ARTIFACT_ID, true);
        let outcome = warm_receipt(&mut db, status);
        runtime::outcome(&mut db, &p, &serde_json::to_value(outcome).unwrap()).unwrap();
        let attempt = tasks::get_attempt(&db, "attempt").unwrap();
        assert_eq!(attempt["producers"][0]["disposition"], disposition);
        assert!(attempt["producers"][0]["native_run_id"].is_null());
        assert!(attempt["producers"][0]["native_input_id"].is_null());
        assert_eq!(
            attempt["producers"][0]["local_execution_ref"]["bridge_boot_id"],
            "boot"
        );
        assert!(
            attempt["producers"][0]["local_execution_ref"]
                .get("observation_id")
                .is_none()
        );
        let operation = operations::get_operation(&db, "dispatch").unwrap();
        assert_eq!(operation["state"], operation_state);
        assert!(operation["native_refs"]["local_execution_ref"].is_object());
        assert_eq!(tasks::get_task(&db, "task").unwrap()["state"], "open");
    }
}

#[test]
fn warm_forged_boot_receipt_rolls_back_operation_and_producer() {
    let (mut db, p) = database(warm_stream::RUNTIME, warm_stream::ARTIFACT_ID, true);
    let mut outcome = warm_receipt(&mut db, "SUCCESS");
    outcome.details["local_execution_ref"]["bridge_boot_id"] = json!("forged-boot");
    let error = runtime::outcome(&mut db, &p, &serde_json::to_value(outcome).unwrap()).unwrap_err();
    assert_eq!(error.code, "NATIVE_IDENTITY_MISMATCH");
    assert_eq!(
        operations::get_operation(&db, "dispatch").unwrap()["state"],
        "sending"
    );
    assert_eq!(
        tasks::get_attempt(&db, "attempt").unwrap()["producers"],
        json!([])
    );
}

'''
text = text[:start] + replacement + text[end:]
receipt_path.write_text(text, encoding="utf-8")
