use super::{NativeServerIdentity, Url};
use serde_json::{Value, json};

fn identity_for(
    codex_home: &str,
    user_agent: &str,
    platform_family: &str,
    platform_os: &str,
) -> NativeServerIdentity {
    let mut initialized: Value = serde_json::from_str(include_str!(
        "../fixtures/codex-app-server-initialize-0.162.0-alpha.17.2.json"
    ))
    .expect("initialize fixture is valid JSON");
    initialized["codexHome"] = json!(codex_home);
    initialized["userAgent"] = json!(user_agent);
    initialized["platformFamily"] = json!(platform_family);
    initialized["platformOs"] = json!(platform_os);
    NativeServerIdentity::from_initialize_response(&initialized)
        .expect("initialize identity is supported")
}

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

#[test]
fn native_scope_key_uses_store_home_across_local_endpoint_aliases() {
    let home = r"C:\SwarmFixture\codex-home";
    let desktop = identity_for(
        home,
        "Codex Desktop/0.162.0-alpha.17.2 (Windows; x86_64)",
        "windows",
        "windows",
    );
    let cli = identity_for(
        home,
        "Codex CLI/0.163.0 (Windows; x86_64)",
        "windows",
        "windows",
    );
    let first_endpoint = Url::parse("ws://127.0.0.1:4500").unwrap();
    let alias_endpoint = Url::parse("wss://localhost:4501/app-server").unwrap();
    assert!(super::is_local_app_server_endpoint(&first_endpoint));
    assert!(super::is_local_app_server_endpoint(&alias_endpoint));
    let first_scope = desktop.scope_key_for_home(home).unwrap();
    assert_eq!(first_scope, cli.scope_key_for_home(home).unwrap());

    let independent_store = identity_for(
        r"C:\SwarmFixture\other-codex-home",
        "Codex Desktop/0.162.0-alpha.17.2 (Windows; x86_64)",
        "windows",
        "windows",
    );
    assert_ne!(
        first_scope,
        independent_store
            .scope_key_for_home(&independent_store.codex_home)
            .unwrap()
    );
}

#[test]
fn native_scope_key_normalizes_windows_unc_and_extended_path_aliases() {
    let extended = identity_for(
        r"\\?\UNC\Server\Share\CodexHome\",
        "Codex Desktop/0.162.0-alpha.17.2 (Windows; x86_64)",
        "windows",
        "windows",
    );
    let plain = identity_for(
        r"\\server\share\codexhome",
        "Codex Desktop/0.162.0-alpha.17.2 (Windows; x86_64)",
        "windows",
        "windows",
    );
    assert_eq!(
        extended.scope_key_for_home(&extended.codex_home),
        plain.scope_key_for_home(&plain.codex_home)
    );

    let other_share = identity_for(
        r"\\server\other-share\codexhome",
        "Codex Desktop/0.162.0-alpha.17.2 (Windows; x86_64)",
        "windows",
        "windows",
    );
    assert_ne!(
        extended.scope_key_for_home(&extended.codex_home),
        other_share.scope_key_for_home(&other_share.codex_home)
    );
}

#[test]
fn native_scope_key_preserves_unix_home_case_and_rejects_remote_endpoint_identity() {
    let mixed_case = identity_for(
        "/home/SwarmFixture/.codex/",
        "Codex Desktop/0.162.0-alpha.17.2 (Linux; x86_64)",
        "unix",
        "linux",
    );
    let same_store = identity_for(
        "/home/SwarmFixture/.codex",
        "Codex Desktop/0.162.0-alpha.17.2 (Linux; x86_64)",
        "unix",
        "linux",
    );
    let different_case = identity_for(
        "/home/swarmfixture/.codex",
        "Codex Desktop/0.162.0-alpha.17.2 (Linux; x86_64)",
        "unix",
        "linux",
    );
    let local_endpoint = Url::parse("ws://[::1]:4500").unwrap();
    assert!(super::is_local_app_server_endpoint(&local_endpoint));
    let scope = mixed_case
        .scope_key_for_home(&mixed_case.codex_home)
        .unwrap();
    assert_eq!(
        Some(scope.as_str()),
        same_store
            .scope_key_for_home(&same_store.codex_home)
            .as_deref()
    );
    assert_ne!(
        Some(scope.as_str()),
        different_case
            .scope_key_for_home(&different_case.codex_home)
            .as_deref()
    );

    let remote_endpoint = Url::parse("wss://codex.example:4500").unwrap();
    assert_eq!(None, mixed_case.native_scope_key(&remote_endpoint));
    let query_endpoint = Url::parse("ws://localhost:4500?token=opaque").unwrap();
    assert_eq!(None, mixed_case.native_scope_key(&query_endpoint));
}

#[test]
fn native_scope_key_requires_a_local_directory_and_resolves_home_path_aliases() {
    let directory = std::env::temp_dir().join(format!("swarm-codex-home-{}", uuid::Uuid::new_v4()));
    let home = directory.join("store");
    let child = directory.join("child");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir(&child).unwrap();
    let home_text = home.to_str().unwrap();
    let identity = identity_for(
        home_text,
        "Codex CLI/0.163.0 (fixture)",
        std::env::consts::FAMILY,
        std::env::consts::OS,
    );
    let alias = child.join("..").join("store");
    let alias_identity = identity_for(
        alias.to_str().unwrap(),
        "Codex Desktop/0.162.0 (fixture)",
        std::env::consts::FAMILY,
        std::env::consts::OS,
    );
    let endpoint = Url::parse("ws://127.0.0.1:4500").unwrap();
    let alias_endpoint = Url::parse("ws://localhost:4501/app-server").unwrap();
    let scope = identity.native_scope_key(&endpoint).unwrap();
    assert_eq!(
        Some(scope.as_str()),
        alias_identity.native_scope_key(&alias_endpoint).as_deref()
    );
    let missing = identity_for(
        directory.join("missing").to_str().unwrap(),
        "Codex CLI/0.163.0",
        std::env::consts::FAMILY,
        std::env::consts::OS,
    );
    assert!(missing.native_scope_key(&endpoint).is_none());
    let file = directory.join("not-a-home");
    std::fs::write(&file, b"fixture").unwrap();
    let file_identity = identity_for(
        file.to_str().unwrap(),
        "Codex CLI/0.163.0",
        std::env::consts::FAMILY,
        std::env::consts::OS,
    );
    assert!(file_identity.native_scope_key(&endpoint).is_none());
    assert!(
        identity
            .native_scope_key(&Url::parse("ws://remote.example:4500").unwrap())
            .is_none()
    );
    let foreign_platform = if cfg!(windows) {
        ("unix", "linux", "/")
    } else {
        ("windows", "windows", "C:\\")
    };
    let foreign = identity_for(
        foreign_platform.2,
        "Codex CLI/0.163.0",
        foreign_platform.0,
        foreign_platform.1,
    );
    assert!(foreign.native_scope_key(&endpoint).is_none());
    std::fs::remove_dir_all(&directory).unwrap();
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
