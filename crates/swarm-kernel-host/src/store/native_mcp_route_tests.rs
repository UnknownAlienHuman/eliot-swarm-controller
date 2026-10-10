//! Focused tests for deriving native MCP route identity from owned-service
//! readback while keeping externally configured routes exact.
use super::*;
use crate::runtime::opencode_v2::{Options, RUNTIME};
use serde_json::{Value, json};
use std::path::PathBuf;

const SERVICE_ID: &str = "owned-opencode-fixture";
const SERVICE_VERSION: &str = "2.0.7";
const EXPECTED_PID: u32 = 42_731;

fn paths() -> (PathBuf, PathBuf) {
    let root = std::env::temp_dir().join("native-mcp-route-identity-fixture");
    (root.join("workspace"), root.join("connection.json"))
}

fn native_options(service_id: Option<&str>, expected_version: Option<&str>) -> Value {
    let (directory, connection_file) = paths();
    let mut options = json!({
        "directory":directory,
        "connection_file":connection_file,
        "model":{
            "id":"fixture/model",
            "providerID":"fixture-provider",
            "variant":"default",
        },
    });
    if let Some(service_id) = service_id {
        options["service_id"] = json!(service_id);
    }
    if let Some(expected_version) = expected_version {
        options["expected_version"] = json!(expected_version);
    }
    options
}

fn route(native_options: Value) -> Value {
    json!({
        "runtime":RUNTIME,
        "module_artifact_id":crate::runtime::opencode_v2::ARTIFACT_ID,
        "native_options":native_options,
    })
}

fn source(identity: &ReadbackRouteIdentity, pid: u32) -> Value {
    json!({
        "runtime":RUNTIME,
        "api_contract":MCP_API_CONTRACT,
        "api_method":"GET /api/mcp",
        "api_scope":"configured_server_connection_status_only",
        "service_id":identity.service_id,
        "service_identity_basis":SERVICE_IDENTITY_BASIS,
        "service_version":identity.expected_version,
        "service_pid":pid,
        "directory_sha256":format!(
            "sha256:{}",
            model::digest(identity.directory.as_bytes())
        ),
    })
}

#[test]
fn owned_route_uses_verified_identity_when_external_service_fields_are_absent() {
    let external = native_options(None, None);
    assert!(external.get("service_id").is_none());
    assert!(external.get("expected_version").is_none());
    let route = route(json!({"directory":external["directory"]}));
    let options = Options {
        service_id: SERVICE_ID.to_owned(),
        connection_file: external["connection_file"]
            .as_str()
            .map(PathBuf::from)
            .expect("fixture connection path"),
        expected_version: SERVICE_VERSION.to_owned(),
        directory: external["directory"]
            .as_str()
            .map(PathBuf::from)
            .expect("fixture workspace path"),
        model: serde_json::from_value(external["model"].clone()).unwrap(),
    };
    let identity =
        owned_readback_route_identity(options, SERVICE_ID, SERVICE_VERSION, EXPECTED_PID).unwrap();
    let payload_source = source(&identity, EXPECTED_PID);

    validate_readback_source_identity(&route, &payload_source, &identity).unwrap();
}

#[test]
fn owned_route_rejects_mismatched_or_invalid_verified_process_identity() {
    let native = native_options(None, None);
    let route = route(json!({"directory":native["directory"]}));
    let options = Options {
        service_id: SERVICE_ID.to_owned(),
        connection_file: native["connection_file"]
            .as_str()
            .map(PathBuf::from)
            .expect("fixture connection path"),
        expected_version: SERVICE_VERSION.to_owned(),
        directory: native["directory"]
            .as_str()
            .map(PathBuf::from)
            .expect("fixture workspace path"),
        model: serde_json::from_value(native["model"].clone()).unwrap(),
    };
    let identity =
        owned_readback_route_identity(options.clone(), SERVICE_ID, SERVICE_VERSION, EXPECTED_PID)
            .unwrap();
    let mismatched_source = source(&identity, EXPECTED_PID + 1);
    assert!(validate_readback_source_identity(&route, &mismatched_source, &identity).is_err());

    assert!(
        owned_readback_route_identity(options.clone(), SERVICE_ID, SERVICE_VERSION, 0,).is_err()
    );
    assert!(
        owned_readback_route_identity(
            options.clone(),
            "different-verified-service",
            SERVICE_VERSION,
            EXPECTED_PID,
        )
        .is_err()
    );
    assert!(owned_readback_route_identity(options, SERVICE_ID, "2.0.8", EXPECTED_PID,).is_err());
}

#[test]
fn external_route_keeps_configured_service_identity_and_positive_pid_contract() {
    let native = native_options(Some("external-opencode"), Some(SERVICE_VERSION));
    let route = route(native.clone());
    let identity = external_readback_route_identity(&route).unwrap();
    assert_eq!(identity.service_id, "external-opencode");
    assert_eq!(identity.expected_version, SERVICE_VERSION);
    assert_eq!(identity.expected_process_id, None);
    let valid_source = source(&identity, EXPECTED_PID);
    validate_readback_source_identity(&route, &valid_source, &identity).unwrap();

    let mut wrong_service = valid_source.clone();
    wrong_service["service_id"] = json!(SERVICE_ID);
    assert!(validate_readback_source_identity(&route, &wrong_service, &identity).is_err());
    let mut wrong_version = valid_source.clone();
    wrong_version["service_version"] = json!("2.0.8");
    assert!(validate_readback_source_identity(&route, &wrong_version, &identity).is_err());
    let mut missing_pid = valid_source;
    missing_pid["service_pid"] = json!(0);
    assert!(validate_readback_source_identity(&route, &missing_pid, &identity).is_err());
}

#[test]
fn standalone_route_requires_module_readback_provenance_and_rejects_cross_pairs() {
    let mut standalone = route(native_options(Some(SERVICE_ID), Some(SERVICE_VERSION)));
    standalone["runtime"] = json!("module");
    standalone["module_artifact_id"] = json!("eliot-opencode-v2.rust-http.1");
    let identity = external_readback_route_identity(&standalone).unwrap();
    let builtin_source = source(&identity, EXPECTED_PID);
    assert!(validate_readback_source_identity(&standalone, &builtin_source, &identity).is_err());
    let mut module_source = builtin_source;
    module_source["runtime"] = json!("module");
    module_source["module_artifact_id"] = json!("eliot-opencode-v2.rust-http.1");
    assert!(validate_readback_source_identity(&standalone, &module_source, &identity).is_err());
    module_source["module_operation_id"] = json!("assigned-session-readback-child");
    validate_readback_source_identity(&standalone, &module_source, &identity).unwrap();
    for field in [
        "service_pid",
        "service_version",
        "directory_sha256",
        "module_artifact_id",
    ] {
        let mut stale = module_source.clone();
        stale[field] = Value::Null;
        assert!(
            validate_readback_source_identity(&standalone, &stale, &identity).is_err(),
            "{field}"
        );
    }
    for (runtime, artifact) in [
        ("opencode_v2", "eliot-opencode-v2.rust-http.1"),
        ("module", crate::runtime::opencode_v2::ARTIFACT_ID),
    ] {
        let mut cross = standalone.clone();
        cross["runtime"] = json!(runtime);
        cross["module_artifact_id"] = json!(artifact);
        assert!(validate_readback_source_identity(&cross, &module_source, &identity).is_err());
    }
}
