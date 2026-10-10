use serde_json::{Value, json};

use super::{LaunchParent, build_packet, digest_value, validate_parent_tuple};

const LAUNCH_OPERATION_ID: &str = "launch-preinput-packet-fixture";
const TASK_ID: &str = "task-preinput-packet-fixture";
const ATTEMPT_ID: &str = "attempt-preinput-packet-fixture";
const BINDING_ID: &str = "binding-preinput-packet-fixture";
const ROUTE_ALIAS: &str = "preinput-packet-route";
const PARTICIPANT_ID: &str = "participant-preinput-packet-fixture";
const NATIVE_ROOT_ID: &str = "native-root-preinput-packet-fixture";
const SERVER_NAME: &str = "fixture-native-mcp-server";
const SERVICE_ID: &str = "fixture-native-mcp-service";
const PROVIDER_ID: &str = "fixture-provider";
const MODEL_ID: &str = "fixture-model";
const MODEL_VARIANT: &str = "fixture-variant";

struct PacketFixture {
    parent: LaunchParent,
    task: Value,
    attempt: Value,
    binding: Value,
    proof: Value,
}

fn packet_fixture() -> PacketFixture {
    let contracts = crate::mcp::participant_core_tool_contracts()
        .expect("load the canonical Participant core-tool contracts");
    let tools = contracts
        .iter()
        .map(|contract| {
            json!({
                "server":SERVER_NAME,
                "name":contract["name"].clone(),
                "input_schema":contract["input_schema"].clone(),
            })
        })
        .collect::<Vec<_>>();
    let plan_digest = digest_value(&json!({"plan":"pre-input packet fixture"}))
        .expect("digest the fixture launch plan");
    let manifest = json!({
        "state":"awaiting_native_mcp",
        "plan_digest":plan_digest,
        "task":{
            "task_id":TASK_ID,
            "observed_revision":7,
            "attempt_id":ATTEMPT_ID,
        },
        "binding":{
            "binding_id":BINDING_ID,
            "generation":3,
            "operation_id":"binding-operation-preinput-fixture",
            "native_root_id":NATIVE_ROOT_ID,
        },
        "participant":{
            "role":"participant",
            "participation_basis":{"kind":"attempt_owner"},
            "client_id":PARTICIPANT_ID,
            "grant_revision":5,
        },
        "runtime":{
            "dispatch_permitted":false,
            "purpose":"pre-input packet regression",
            "route":{"alias":ROUTE_ALIAS},
        },
        "progress":{"task_dispatch":"not_started"},
    });
    let task = json!({
        "task_id":TASK_ID,
        "state":"open",
        "revision":7,
        "current_attempt_id":ATTEMPT_ID,
    });
    let attempt = json!({
        "task_id":TASK_ID,
        "task_revision":7,
        "attempt_id":ATTEMPT_ID,
        "binding_id":BINDING_ID,
        "binding_generation":3,
        "state":"reserved",
        "start_owner":"controller",
        "start_operation_id":null,
        "released_at_ms":null,
        "task_snapshot":{"task_id":TASK_ID,"revision":7,"title":"Fixture task"},
    });
    let binding = json!({
        "state":"ready",
        "released_at_ms":null,
        "route":{
            "alias":ROUTE_ALIAS,
            "owned_service":{
                "service_id":SERVICE_ID,
                "model":{
                    "providerID":PROVIDER_ID,
                    "id":MODEL_ID,
                    "variant":MODEL_VARIANT,
                },
            },
        },
    });
    let parent = LaunchParent {
        operation_id: LAUNCH_OPERATION_ID.to_owned(),
        state: "queued".to_owned(),
        task_id: Some(TASK_ID.to_owned()),
        attempt_id: Some(ATTEMPT_ID.to_owned()),
        binding_id: Some(BINDING_ID.to_owned()),
        binding_generation: Some(3),
        effective_request_json: "{}".to_owned(),
        effective: json!({}),
        manifest,
    };

    let expected_assignment = json!({
        "task_id":TASK_ID,
        "task_revision":7,
        "attempt_id":ATTEMPT_ID,
        "binding_id":BINDING_ID,
        "binding_generation":3,
        "native_session_id":NATIVE_ROOT_ID,
        "participant_id":PARTICIPANT_ID,
        "mcp_profile":"participant",
        "grant_revision":5,
        "participation_basis":"attempt_owner",
        "assignment_id":null,
        "review_assignment_id":null,
    });
    let assignment_digest = digest_value(&expected_assignment)
        .expect("digest the exact synthetic attempt-owner assignment");
    let native_discovered = json!({
        "status":"observed",
        "observed_at_ms":10_000,
        "tools":tools,
    });
    let session_context = json!({
        "status":"unknown",
        "stage":null,
        "observed_at_ms":null,
        "tools":[],
    });
    let provider_request = json!({
        "status":"unknown",
        "transport":null,
        "stage":null,
        "observed_at_ms":null,
        "tools":[],
    });
    let native_discovered_digest =
        digest_value(&native_discovered).expect("digest the synthetic server-scoped inventory");
    let session_context_digest =
        digest_value(&session_context).expect("digest the unknown session-context hook");
    let provider_request_digest =
        digest_value(&provider_request).expect("digest the unknown provider-request hook");
    let evidence_digest = digest_value(&json!({
        "native_discovered":native_discovered,
        "session_context":session_context,
        "provider_request":provider_request,
    }))
    .expect("digest the synthetic observation evidence");
    let identity_digest = digest_value(&json!({"identity":"pre-input fixture"}))
        .expect("digest the synthetic launch identity");
    let process_identity_digest = digest_value(&json!({"process_id":41}))
        .expect("digest the synthetic service process identity");
    let command_sha256 =
        digest_value(&json!("fixture command")).expect("digest the synthetic registered command");
    let location_sha256 =
        digest_value(&json!("fixture location")).expect("digest the synthetic registered location");
    let module_sha256 =
        digest_value(&json!("fixture module")).expect("digest the synthetic native module");
    let proof = json!({
        "schema_version":1,
        "kind":"launcher_native_mcp_dispatch_capability",
        "launch_operation_id":LAUNCH_OPERATION_ID,
        "dispatch_operation_id":null,
        "launch_identity_digest":identity_digest,
        "assignment_digest":assignment_digest,
        "evidence_digest":evidence_digest,
        "native_discovered_digest":native_discovered_digest,
        "assignment":{
            "task_id":TASK_ID,
            "task_revision":7,
            "attempt_id":ATTEMPT_ID,
            "binding_id":BINDING_ID,
            "binding_generation":3,
            "participant_id":PARTICIPANT_ID,
            "grant_revision":5,
            "native_session_id":NATIVE_ROOT_ID,
        },
        "service":{
            "id":SERVICE_ID,
            "pid":41,
            "version":"2.0.7",
            "process_identity_digest":process_identity_digest,
        },
        "model":{
            "id":MODEL_ID,
            "provider_id":PROVIDER_ID,
            "variant":MODEL_VARIANT,
        },
        "install":{
            "server_name":SERVER_NAME,
            "command_sha256":command_sha256,
            "location_sha256":location_sha256,
            "state":"registered",
            "runtime_config_readback":"not_exposed_by_pinned_api",
            "matches_prepared_command":"unknown",
        },
        "capability":{
            "identity_digest":identity_digest,
            "evidence_digest":evidence_digest,
            "native_discovered_digest":native_discovered_digest,
            "session_context_digest":session_context_digest,
            "provider_request_digest":provider_request_digest,
            "service_id":SERVICE_ID,
            "service_version":"2.0.7",
            "plugin_id":"eliot.native-mcp-proof.v1",
            "module_sha256":module_sha256,
        },
        "native_discovered":native_discovered,
        "session_context":session_context,
        "session_context_digest":session_context_digest,
        "provider_request":provider_request,
        "provider_request_digest":provider_request_digest,
        "dispatch_permitted":false,
        "model_consumed":"unknown",
    });

    PacketFixture {
        parent,
        task,
        attempt,
        binding,
        proof,
    }
}

fn assert_current_parent_tuple(fixture: &PacketFixture) {
    validate_parent_tuple(
        &fixture.parent,
        &fixture.attempt,
        &fixture.task,
        &fixture.binding,
        None,
    )
    .expect("fixture must preserve the exact queued launch/Task/Attempt/binding tuple");
}

fn refresh_synthetic_observation_digests(proof: &mut Value) {
    let native_discovered_digest = digest_value(&proof["native_discovered"])
        .expect("refresh the synthetic native inventory digest");
    proof["native_discovered_digest"] = json!(native_discovered_digest);
    proof["capability"]["native_discovered_digest"] = json!(native_discovered_digest);

    let evidence_digest = digest_value(&json!({
        "native_discovered":proof["native_discovered"].clone(),
        "session_context":proof["session_context"].clone(),
        "provider_request":proof["provider_request"].clone(),
    }))
    .expect("refresh the synthetic observation evidence digest");
    proof["evidence_digest"] = json!(evidence_digest);
    proof["capability"]["evidence_digest"] = json!(evidence_digest);
}

fn assert_capability_rejection(result: crate::error::Result<Value>, expected_message: &str) {
    let error = result.expect_err("pre-input packet fixture should be rejected");
    assert_eq!(error.code, "NATIVE_MCP_CAPABILITY_UNAVAILABLE");
    assert_eq!(error.message, expected_message);
}

#[test]
fn packet_accepts_exact_server_scoped_participant_schemas_with_unknown_hooks() {
    let fixture = packet_fixture();
    assert_current_parent_tuple(&fixture);
    assert_eq!(fixture.proof["session_context"]["status"], "unknown");
    assert!(fixture.proof["session_context"]["stage"].is_null());
    assert!(fixture.proof["session_context"]["observed_at_ms"].is_null());
    assert_eq!(fixture.proof["session_context"]["tools"], json!([]));
    assert_eq!(fixture.proof["provider_request"]["status"], "unknown");
    assert!(fixture.proof["provider_request"]["transport"].is_null());
    assert!(fixture.proof["provider_request"]["stage"].is_null());
    assert!(fixture.proof["provider_request"]["observed_at_ms"].is_null());
    assert_eq!(fixture.proof["provider_request"]["tools"], json!([]));

    let contracts = crate::mcp::participant_core_tool_contracts()
        .expect("load the canonical Participant core-tool contracts");
    let expected_names = contracts
        .iter()
        .map(|contract| contract["name"].clone())
        .collect::<Vec<_>>();
    let packet = build_packet(
        &fixture.parent,
        &fixture.attempt,
        &fixture.binding,
        &fixture.proof,
    )
    .expect("accept the exact server-scoped Participant inventory");

    assert_eq!(packet["schema_version"], 1);
    assert_eq!(
        packet["capability"]["required_core_schemas"],
        json!(expected_names)
    );
    assert_eq!(packet["capability"]["session_context_state"], "unknown");
    assert_eq!(packet["capability"]["provider_request_state"], "unknown");
}

#[test]
fn packet_rejects_changed_core_tool_schema() {
    let fixture = packet_fixture();
    assert_current_parent_tuple(&fixture);
    let mut proof = fixture.proof;
    proof["native_discovered"]["tools"][0]["input_schema"]["x-regression-mutated"] = json!(true);
    refresh_synthetic_observation_digests(&mut proof);

    assert_capability_rejection(
        build_packet(&fixture.parent, &fixture.attempt, &fixture.binding, &proof),
        "observed Participant tool schema differs from the canonical contract",
    );
}

#[test]
fn packet_rejects_missing_required_core_tool() {
    let fixture = packet_fixture();
    assert_current_parent_tuple(&fixture);
    let mut proof = fixture.proof;
    proof["native_discovered"]["tools"]
        .as_array_mut()
        .expect("fixture inventory is an array")
        .remove(0);
    refresh_synthetic_observation_digests(&mut proof);

    assert_capability_rejection(
        build_packet(&fixture.parent, &fixture.attempt, &fixture.binding, &proof),
        "required Participant tool is not observed",
    );
}

#[test]
fn packet_rejects_duplicate_server_scoped_tool() {
    let fixture = packet_fixture();
    assert_current_parent_tuple(&fixture);
    let mut proof = fixture.proof;
    let tools = proof["native_discovered"]["tools"]
        .as_array_mut()
        .expect("fixture inventory is an array");
    let duplicate = tools[0].clone();
    tools.push(duplicate);
    refresh_synthetic_observation_digests(&mut proof);

    assert_capability_rejection(
        build_packet(&fixture.parent, &fixture.attempt, &fixture.binding, &proof),
        "native inventory contains a duplicate or unscoped tool",
    );
}

#[test]
fn packet_rejects_tool_from_another_server() {
    let fixture = packet_fixture();
    assert_current_parent_tuple(&fixture);
    let mut proof = fixture.proof;
    proof["native_discovered"]["tools"][0]["server"] = json!("another-native-mcp-server");
    refresh_synthetic_observation_digests(&mut proof);

    assert_capability_rejection(
        build_packet(&fixture.parent, &fixture.attempt, &fixture.binding, &proof),
        "native inventory contains a duplicate or unscoped tool",
    );
}

#[test]
fn packet_rejects_server_prefixed_core_tool_name_without_mapping() {
    let fixture = packet_fixture();
    assert_current_parent_tuple(&fixture);
    let mut proof = fixture.proof;
    let tool = &mut proof["native_discovered"]["tools"][0];
    let name = tool["name"]
        .as_str()
        .expect("canonical Participant tool has a name")
        .to_owned();
    tool["name"] = json!(format!("{SERVER_NAME}__{name}"));
    refresh_synthetic_observation_digests(&mut proof);

    assert_capability_rejection(
        build_packet(&fixture.parent, &fixture.attempt, &fixture.binding, &proof),
        "required Participant tool is not observed",
    );
}

#[test]
fn packet_rejects_assignment_identity_mismatch() {
    let fixture = packet_fixture();
    assert_current_parent_tuple(&fixture);
    let mut proof = fixture.proof;
    proof["assignment"]["task_id"] = json!("another-task");

    assert_capability_rejection(
        build_packet(&fixture.parent, &fixture.attempt, &fixture.binding, &proof),
        "native MCP proof is not current for this launch assignment",
    );
}

#[test]
fn parent_tuple_rejects_binding_route_scope_mismatch() {
    let mut fixture = packet_fixture();
    fixture.binding["route"]["alias"] = json!("another-route");

    let error = validate_parent_tuple(
        &fixture.parent,
        &fixture.attempt,
        &fixture.task,
        &fixture.binding,
        None,
    )
    .expect_err("a changed binding route must not satisfy the launch tuple");
    assert_eq!(error.code, "STALE_LAUNCH_DISPATCH_SCOPE");
}
