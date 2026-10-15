use super::{digest_json, project_provider_request, project_session_context};
use serde_json::{Value, json};

#[test]
fn actual_mcp_producer_roundtrips_through_store_consumer_and_admission() {
    use crate::{
        config::{Config, McpToolProfile},
        model,
        native_mcp::{AssignmentContext, AssignmentSeed},
        runtime::opencode_v2::{ModelRef, Options, mcp_tools},
        store::{
            launcher_dispatch::preinput_contract_tests::offline_mcp_packet_fixture,
            launcher_native_mcp::OwnedServiceExpectation,
        },
    };
    use std::{path::PathBuf, sync::Arc};

    let context = offline_mcp_packet_fixture(None).unwrap();
    let scope = &context["assignment"];
    let assignment = AssignmentContext::new(AssignmentSeed {
        task_id: scope["task_id"].as_str().unwrap().into(),
        task_revision: scope["task_revision"].as_i64().unwrap(),
        attempt_id: scope["attempt_id"].as_str().unwrap().into(),
        binding_id: scope["binding_id"].as_str().unwrap().into(),
        binding_generation: scope["binding_generation"].as_i64().unwrap(),
        native_session_id: scope["native_session_id"].as_str().unwrap().into(),
        participant_id: scope["participant_id"].as_str().unwrap().into(),
        profile: McpToolProfile::Participant,
        grant_revision: scope["grant_revision"].as_i64().unwrap(),
        basis_kind: "attempt_owner".into(),
        assignment_id: None,
        review_assignment_id: None,
    })
    .unwrap();
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let options = Options {
        service_id: context["service"]["id"].as_str().unwrap().into(),
        connection_file: directory.join("offline-unused-connection.json"),
        expected_version: "2.0.7".into(),
        directory,
        model: ModelRef {
            id: context["model"]["id"].as_str().unwrap().into(),
            provider_id: context["model"]["provider_id"].as_str().unwrap().into(),
            variant: context["model"]["variant"].as_str().unwrap().into(),
        },
    };
    let pid = u32::try_from(context["service"]["pid"].as_u64().unwrap()).unwrap();
    let server_name = context["install"]["server_name"].as_str().unwrap();
    let (produced, metadata) =
        mcp_tools::offline_preinput_proof_fixture(assignment.clone(), &options, pid, server_name)
            .unwrap();
    // Cross the actual JSON serialization boundary. Never recreate the MCP
    // proof or its stage shape/digests in this test.
    let wire = serde_json::to_vec(&produced).unwrap();
    let retained_tools: Value = serde_json::from_slice(&wire).unwrap();
    assert!(
        retained_tools["session_context"]
            .get("digest")
            .unwrap()
            .is_null()
    );
    assert!(
        retained_tools["provider_request"]
            .get("digest")
            .unwrap()
            .is_null()
    );
    let location_digest = format!(
        "sha256:{}",
        model::digest(options.directory.to_str().unwrap().as_bytes())
    );
    // Synthetic retained launch/install context supplies no live authority;
    // the serialized MCP evidence above is entirely producer-owned.
    let intent = json!({
        "schema_version":1,"kind":"opencode_v2_mcp_install_intent",
        "registration":"location_scoped_in_memory","expected_version":"2.0.7",
        "assignment":assignment.as_value(),"service_id":options.service_id,
        "native_tool_set":"unknown","provider_request_context":"unknown",
        "model_consumption":"unknown","dispatch_permitted":false,
        "server_name":server_name,"command_sha256":context["install"]["command_sha256"],
        "location_sha256":location_digest,
    });
    let facts = super::LaunchFacts {
        operation_id: context["launch_operation_id"].as_str().unwrap().into(),
        identity_digest: context["launch_identity_digest"].as_str().unwrap().into(),
        participant_id: assignment.participant_id().into(),
        credential_ref: "offline-unused-credential".into(),
        profile_config_ref: "offline-unused-profile".into(),
        assignment,
        options,
        owned_service: Some(OwnedServiceExpectation::offline_proof_fixture(pid)),
        config: Arc::new(Config::default()),
    };
    let mut record = json!({
        "schema_version":1,"kind":"launcher_native_mcp_tools",
        "operation_id":facts.operation_id,"launch_identity_digest":facts.identity_digest,
        "assignment":facts.assignment.as_value(),"dispatch_permitted":false,
        "service":{"id":facts.options.service_id,"version":"2.0.7","pid":pid,"directory_sha256":location_digest},
        "install":{"state":"registered","intent":intent,"readback":{
            "intent":intent,"runtime_entry_present":true,"service_process_id":pid,
            "service_version":"2.0.7","runtime_status":"connected",
            "dispatch_permitted":false,"matches_prepared_command":"unknown",
            "runtime_config_readback":"not_exposed_by_pinned_api"}},
        "challenge":{"state":"observed","metadata":metadata},
        "tools_readback":retained_tools,
    });
    let capability = super::validate_connection_record(&record, &facts, None).unwrap();
    let packet = offline_mcp_packet_fixture(Some(capability)).unwrap();
    assert_eq!(packet["capability"]["session_context_state"], "unknown");
    assert_eq!(packet["capability"]["provider_request_state"], "unknown");
    assert_eq!(packet["selection"]["model"], facts.options.model.id);

    // A producer-field regression must fail before admission, even with a
    // valid recomputed enclosing evidence digest.
    record["tools_readback"]["session_context"]
        .as_object_mut()
        .unwrap()
        .remove("digest");
    let mut evidence = record["tools_readback"].clone();
    evidence.as_object_mut().unwrap().remove("evidence_digest");
    record["tools_readback"]["evidence_digest"] = json!(digest_json(&evidence).unwrap());
    assert!(super::validate_connection_record(&record, &facts, None).is_err());
}

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
