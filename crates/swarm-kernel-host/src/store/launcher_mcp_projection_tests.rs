use super::{digest_json, project_provider_request, project_session_context};
use serde_json::{Value, json};

fn challenge() -> Value {
    json!({"issued_at_ms":100,"expires_at_ms":200})
}

fn selected_model() -> Value {
    json!({"id":"step-5-preview-free","providerID":"opencode","variant":"high"})
}

// Exact pre-input producer stage shape, also retained by native edition R.
fn unknown_stages() -> (Value, Value) {
    (
        json!({"status":"unknown","stage":null,"observed_at_ms":null,
            "agent":null,"tools":[],"reason_code":null,"digest":null}),
        json!({"status":"unknown","transport":null,"stage":null,"kind":null,
            "observed_at_ms":null,"agent":null,"model":null,"tools":[],
            "reason_code":null,"digest":null}),
    )
}

fn observed_stages() -> (Value, Value) {
    let tools = json!([{"name":"task.get","description":"Read current Task",
        "input_schema":{"type":"object"}}]);
    let digest = digest_json(&tools).unwrap();
    (
        json!({"status":"observed","stage":"session_context_hook",
            "observed_at_ms":150,"agent":"build","tools":tools,
            "reason_code":null,"digest":digest}),
        json!({"status":"observed","transport":"http","stage":"before_transport",
            "kind":"primary","observed_at_ms":150,"agent":"build",
            "model":selected_model(),"tools":tools,"reason_code":null,"digest":digest}),
    )
}

#[test]
fn native_unknown_stage_shape_remains_unknown_before_input() {
    let (context, provider) = unknown_stages();
    let projected_context = project_session_context(&context, &challenge()).unwrap();
    let projected_provider =
        project_provider_request(&provider, &challenge(), &selected_model()).unwrap();
    assert_eq!(projected_context["status"], "unknown");
    assert_eq!(projected_provider["status"], "unknown");
    assert_eq!(projected_context["tools"], json!([]));
    assert_eq!(projected_provider["tools"], json!([]));
    assert!(projected_context.get("digest").is_none());
    assert!(projected_provider.get("digest").is_none());
}

#[test]
fn native_observed_and_unsupported_stage_digests_are_validated() {
    let (mut context, mut provider) = observed_stages();
    let projected = project_session_context(&context, &challenge()).unwrap();
    assert_eq!(projected["status"], "observed");
    assert!(projected["tools"][0].get("description").is_none());
    assert_eq!(
        project_provider_request(&provider, &challenge(), &selected_model()).unwrap()["status"],
        "observed"
    );
    for stage in [&mut context, &mut provider] {
        stage["status"] = json!("unsupported");
        stage["tools"] = json!([]);
        stage["digest"] = json!(digest_json(&json!([])).unwrap());
    }
    context["reason_code"] = json!("context_schema_unrecognized");
    provider["reason_code"] = json!("tools_field_unrecognized");
    assert_eq!(
        project_session_context(&context, &challenge()).unwrap()["status"],
        "unsupported"
    );
    assert_eq!(
        project_provider_request(&provider, &challenge(), &selected_model()).unwrap()["status"],
        "unsupported"
    );
}

#[test]
fn native_stage_missing_forged_and_unknown_nonnull_digests_are_refused() {
    let (context, provider) = observed_stages();
    for replacement in [None, Some(Value::Null), Some(json!("sha256:bad"))] {
        let mut changed_context = context.clone();
        let mut changed_provider = provider.clone();
        for stage in [&mut changed_context, &mut changed_provider] {
            if let Some(value) = &replacement {
                stage["digest"] = value.clone();
            } else {
                stage.as_object_mut().unwrap().remove("digest");
            }
        }
        assert!(project_session_context(&changed_context, &challenge()).is_err());
        assert!(
            project_provider_request(&changed_provider, &challenge(), &selected_model()).is_err()
        );
    }
    let (mut context, mut provider) = unknown_stages();
    context["digest"] = json!(digest_json(&json!([])).unwrap());
    provider["digest"] = context["digest"].clone();
    assert!(project_session_context(&context, &challenge()).is_err());
    assert!(project_provider_request(&provider, &challenge(), &selected_model()).is_err());
}

#[test]
fn native_stage_digest_does_not_bypass_model_time_or_closed_shape() {
    let (context, provider) = observed_stages();
    for (field, value) in [("observed_at_ms", json!(201)), ("unexpected", json!(true))] {
        let mut changed_context = context.clone();
        let mut changed_provider = provider.clone();
        changed_context[field] = value.clone();
        changed_provider[field] = value;
        assert!(project_session_context(&changed_context, &challenge()).is_err());
        assert!(
            project_provider_request(&changed_provider, &challenge(), &selected_model()).is_err()
        );
    }
    let mut changed_provider = provider;
    changed_provider["model"]["variant"] = json!("low");
    assert!(project_provider_request(&changed_provider, &challenge(), &selected_model()).is_err());
}
