use super::NativeServerIdentity;
use serde_json::{Value, json};

#[test]
fn codex_desktop_initialize_fixture_accepts_supported_contract_and_fails_closed() {
    let actual: Value = serde_json::from_str(include_str!(
        "../fixtures/codex-app-server-initialize-0.162.0-alpha.17.2.json"
    ))
    .expect("actual Codex Desktop initialize fixture is valid JSON");
    let identity = NativeServerIdentity::from_initialize_response(&actual)
        .expect("documented Codex Desktop initialize response is supported");
    assert_eq!(identity.reported_product, "Codex Desktop");
    assert_eq!(identity.reported_version, "0.162.0-alpha.17.2");
    assert_eq!(identity.reported_platform_family, "windows");
    assert_eq!(identity.reported_platform_os, "windows");
    let report = identity.report();
    assert_eq!(
        report["source"],
        "initialize.userAgent Codex product/version prefix"
    );
    assert_eq!(report["peer_report_only"], true);
    assert_eq!(report["process_attestation"], false);
    assert_eq!(
        report["server_capabilities"],
        "not_reported_by_initialize_response"
    );
    assert_eq!(
        report["client_declared_capabilities"]["experimentalApi"],
        false
    );

    let mcp_style_only = json!({
        "serverInfo": {"name": "codex", "version": "0.162.0-alpha.17.2"}
    });
    assert!(NativeServerIdentity::from_initialize_response(&mcp_style_only).is_none());

    let client_info_only = json!({
        "clientInfo": {"name": "Codex Desktop", "version": "0.162.0-alpha.17.2"},
        "codexHome": "C:\\Users\\fixture\\.codex",
        "platformFamily": "windows",
        "platformOs": "windows"
    });
    assert!(NativeServerIdentity::from_initialize_response(&client_info_only).is_none());

    let mut cli_peer = actual.clone();
    cli_peer["userAgent"] = json!(
        "Codex CLI/0.162.0-alpha.17.2 (Windows 10.0.26300; x86_64) dumb (swarm_private_store_qualification; 0.1.0)"
    );
    let cli_identity = NativeServerIdentity::from_initialize_response(&cli_peer)
        .expect("Codex CLI product/version userAgent is supported");
    assert_eq!(cli_identity.reported_product, "Codex CLI");
    assert_eq!(cli_identity.reported_version, "0.162.0-alpha.17.2");

    let mut unknown_peer = actual.clone();
    unknown_peer["userAgent"] = json!("Other Server/1.2.3 (client; 0.1.0)");
    unknown_peer["serverInfo"] = json!({"name":"Codex Desktop", "version":"0.162.0-alpha.17.2"});
    assert!(NativeServerIdentity::from_initialize_response(&unknown_peer).is_none());

    let mut missing_product = actual.clone();
    missing_product["userAgent"] = json!("0.162.0-alpha.17.2 (Windows; x86_64)");
    assert!(NativeServerIdentity::from_initialize_response(&missing_product).is_none());

    let mut missing_version = actual.clone();
    missing_version["userAgent"] = json!("Codex CLI/ (Windows; x86_64)");
    assert!(NativeServerIdentity::from_initialize_response(&missing_version).is_none());

    let mut malformed_peer = actual;
    malformed_peer["platformOs"] = json!(42);
    assert!(NativeServerIdentity::from_initialize_response(&malformed_peer).is_none());

    let mut unsupported_version = serde_json::from_str::<Value>(include_str!(
        "../fixtures/codex-app-server-initialize-0.162.0-alpha.17.2.json"
    ))
    .expect("actual Codex Desktop initialize fixture is valid JSON");
    unsupported_version["userAgent"] = json!("Codex Desktop/dev-build (client; 0.1.0)");
    assert!(NativeServerIdentity::from_initialize_response(&unsupported_version).is_none());

    let mut relative_home = serde_json::from_str::<Value>(include_str!(
        "../fixtures/codex-app-server-initialize-0.162.0-alpha.17.2.json"
    ))
    .expect("actual Codex Desktop initialize fixture is valid JSON");
    relative_home["codexHome"] = json!(".codex");
    assert!(NativeServerIdentity::from_initialize_response(&relative_home).is_none());

    assert!(!super::is_absolute_windows_path(r"\\"));
    assert!(!super::is_absolute_windows_path("//"));
    assert!(super::is_absolute_windows_path(r"C:\Users\fixture\.codex"));
    assert!(super::is_absolute_windows_path(r"\\server\share\codex"));
}

#[cfg(windows)]
#[test]
fn windows_native_path_identity_normalizes_extended_drive_and_unc_paths_without_merging_workspaces()
{
    let expected = r"\\?\C:\SwarmFixture\empty-workspace";
    let observed = r"C:\SwarmFixture\empty-workspace";
    let different_workspace = r"C:\SwarmFixture\other-workspace";
    assert_eq!(
        super::normalize_path(expected),
        super::normalize_path(observed)
    );
    assert_ne!(
        super::normalize_path(observed),
        super::normalize_path(different_workspace)
    );

    assert_eq!(
        super::normalize_path(r"\\?\UNC\server\share\repo"),
        super::normalize_path(r"\\server\share\repo")
    );
    assert_ne!(
        super::normalize_path(r"\\server\share\repo"),
        super::normalize_path(r"\\server\other-share\repo")
    );
    assert_eq!(
        super::normalize_path(r"\\?\C:\"),
        super::normalize_path(r"C:\")
    );
    assert!(super::normalize_path(r"\\.\C:\workspace").is_none());
    assert!(super::normalize_path(r"C:relative").is_none());
}

#[cfg(windows)]
#[tokio::test]
#[ignore = "attaches to the explicitly supplied live Codex App Server and reads one existing thread"]
async fn live_existing_empty_thread_matches_native_identity_and_workspace() {
    let endpoint = std::env::var("SWARM_CODEX_QUALIFICATION_LIVE_ENDPOINT")
        .expect("live qualification endpoint is explicitly supplied");
    let thread_id = std::env::var("SWARM_CODEX_QUALIFICATION_LIVE_ROOT")
        .expect("existing native root ID is explicitly supplied");
    let expected_workspace = std::env::var("SWARM_CODEX_QUALIFICATION_LIVE_WORKSPACE")
        .expect("expected workspace is explicitly supplied");

    let mut native = super::NativeClient::attach_for_existing_thread_readback(
        &endpoint,
        super::NativeEventSink::new(),
    )
    .await
    .expect("verified live Codex App Server accepts its initialize handshake");
    assert!(native.identity_is_known());
    let identity = native
        .server_identity
        .as_ref()
        .expect("initialize response contains a supported peer-reported identity");
    assert_eq!(identity.reported_product, "Codex Desktop");
    assert_eq!(identity.reported_version, "0.162.0-alpha.17.2");

    let response = native
        .read_thread(&thread_id)
        .await
        .expect("existing empty native thread remains readable");
    let thread = &response["thread"];
    assert_eq!(thread["id"].as_str(), Some(thread_id.as_str()));
    assert_eq!(thread["modelProvider"], "opencodex");
    assert_eq!(thread["model"], "gpt-6.1-sol");
    assert_eq!(
        thread["cwd"].as_str().and_then(super::normalize_path),
        super::normalize_path(&expected_workspace)
    );
}
