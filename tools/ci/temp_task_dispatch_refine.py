from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one replacement, found {count}")
    return text.replace(old, new)


operations_path = Path("crates/swarm-kernel-host/src/store/operations.rs")
operations = operations_path.read_text(encoding="utf-8")
operations = replace_once(
    operations,
    '''            "SELECT json_object(                'method',method,                'state',state,                'task_id',task_id,                'attempt_id',attempt_id,                'binding_id',binding_id,                'binding_generation',binding_generation,                'prerequisite_operation_id',prerequisite_operation_id,                'original_request',json(original_request_json))              FROM operations WHERE operation_id=?1",''',
    '''            r#"SELECT json_object(
                'method',method,
                'state',state,
                'task_id',task_id,
                'attempt_id',attempt_id,
                'binding_id',binding_id,
                'binding_generation',binding_generation,
                'prerequisite_operation_id',prerequisite_operation_id,
                'original_request',json(original_request_json)
            )
            FROM operations
            WHERE operation_id=?1"#,''',
    "existing dispatch SQL",
)
operations = replace_once(
    operations,
    '''    let requested_prerequisite = match request.get("prerequisite_operation_id") {
        None | Some(Value::Null) => None,
        Some(_) => Some(model::text(request, "prerequisite_operation_id")?),
    };
    if retained["prerequisite_operation_id"].as_str() != requested_prerequisite {
        return Err(Error::new(
            "ATTEMPT_START_CORRUPT",
            "retained task.dispatch prerequisite column disagrees with its request",
        ));
    }
''',
    '''    let retained_prerequisite = retained["prerequisite_operation_id"]
        .as_str()
        .map(str::to_owned);
    let original_prerequisite = match original.get("prerequisite_operation_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) if !value.is_empty() => Some(value.as_str()),
        Some(_) => {
            return Err(Error::new(
                "ATTEMPT_START_CORRUPT",
                "retained task.dispatch request has an invalid prerequisite",
            ));
        }
    };
    if retained_prerequisite.as_deref() != original_prerequisite {
        return Err(Error::new(
            "ATTEMPT_START_CORRUPT",
            "retained task.dispatch prerequisite column disagrees with its request",
        ));
    }
    let requested_prerequisite = match request.get("prerequisite_operation_id") {
        None | Some(Value::Null) => None,
        Some(_) => Some(model::text(request, "prerequisite_operation_id")?),
    };
    if original_prerequisite != requested_prerequisite {
        return Err(Error::new(
            "ATTEMPT_DISPATCH_CONFLICT",
            "initial delivery already exists with a different setup prerequisite",
        ));
    }
''',
    "prerequisite classification",
)
operations = replace_once(
    operations,
    '''        "UPDATE operations          SET task_id=?2,attempt_id=?3,prerequisite_operation_id=?4,             effective_request_json=json_set(effective_request_json,'$.semantic_reuse',json(?5)),             updated_at_ms=?6          WHERE operation_id=?1 AND method='task.dispatch' AND state='queued'            AND task_id IS NULL AND attempt_id IS NULL            AND binding_id IS NULL AND binding_generation IS NULL            AND json_type(effective_request_json,'$.semantic_reuse') IS NULL",''',
    '''        r#"UPDATE operations
        SET task_id=?2,
            attempt_id=?3,
            prerequisite_operation_id=?4,
            effective_request_json=json_set(
                effective_request_json,
                '$.semantic_reuse',
                json(?5)
            ),
            updated_at_ms=?6
        WHERE operation_id=?1
          AND method='task.dispatch'
          AND state='queued'
          AND task_id IS NULL
          AND attempt_id IS NULL
          AND binding_id IS NULL
          AND binding_generation IS NULL
          AND json_type(effective_request_json,'$.semantic_reuse') IS NULL"#,''',
    "reuse update SQL",
)
operations = replace_once(
    operations,
    '''    if a["state"] != "reserved" || !a["released_at_ms"].is_null() {
''',
    '''    if a["state"] != "reserved" {
''',
    "redundant release check",
)
operations_path.write_text(operations, encoding="utf-8")

test_path = Path("crates/swarm-kernel-host/src/store/gm_continuation_tests.rs")
test = test_path.read_text(encoding="utf-8")
test = replace_once(
    test,
    '''            "SELECT json_object(                'state',state,                'task_id',task_id,                'attempt_id',attempt_id,                'binding_id',binding_id,                'binding_generation',binding_generation,                'effective',json(effective_request_json),                'result',json(result_json))              FROM operations WHERE operation_id=?1",''',
    '''            r#"SELECT json_object(
                'state',state,
                'task_id',task_id,
                'attempt_id',attempt_id,
                'binding_id',binding_id,
                'binding_generation',binding_generation,
                'effective',json(effective_request_json),
                'result',json(result_json)
            )
            FROM operations
            WHERE operation_id=?1"#,''',
    "test operation SQL",
)
test = replace_once(
    test,
    '''            "SELECT payload_json FROM observations              WHERE operation_id=?1 AND kind='task.dispatch'",''',
    '''            "SELECT payload_json FROM observations \
             WHERE operation_id=?1 AND kind='task.dispatch'",''',
    "test observation SQL",
)
test_path.write_text(test, encoding="utf-8")
