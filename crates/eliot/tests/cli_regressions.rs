#![cfg(any(windows, target_os = "linux"))]

mod support;
#[cfg(windows)]
#[path = "support/windows_process.rs"]
mod windows_process;

use eliot_core::api::{
    AgentListResponse, CommandSubmitRequest, DaemonStatusRequest, DaemonStatusResponse,
    EventsReadRequest, EventsReadResponse, EventsSubscribeRequest, EventsSubscribeResponse,
    MAX_OBSERVATION_CURSOR_BYTES, PROCESS_CURSOR_MAX_BYTES, WhoResponse,
};
use serde_json::{Value, json};
use std::time::Duration;
use std::{fs, path::Path, process::Command};
use support::Captured;
use support::{AMBIENT_IDENTITY_ENV_KEYS, CLI, Fixture, cli, run, scrub_ambient_identity};

fn public_rpc(
    runtime: &tokio::runtime::Runtime,
    fixture: &Fixture,
    method: &str,
    params: Value,
) -> eliot_core::Result<Value> {
    let owner_credential =
        fs::read_to_string(fixture.data.join("owner.token")).expect("fixture owner token");
    runtime.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            eliot_daemon::client_call_with_owner_credential(
                &fixture.data,
                method,
                params,
                &owner_credential,
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("{method} exceeded the bounded public RPC deadline"))
    })
}

#[test]
fn explicit_owner_credential_works_over_actual_ipc_with_renamed_token_file() {
    let mut fixture = Fixture::new("explicit owner credential IPC");
    fixture.start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("public RPC runtime");

    let token_path = fixture.data.join("owner.token");
    let token = fs::read_to_string(&token_path).expect("capture fixture owner token");
    let backup_path = fixture.data.join("owner.token.renamed");
    fs::rename(&token_path, &backup_path).expect("temporarily rename owner.token");

    struct TokenRestoreGuard<'a> {
        from: &'a Path,
        to: &'a Path,
        active: bool,
    }
    impl<'a> Drop for TokenRestoreGuard<'a> {
        fn drop(&mut self) {
            if self.active {
                let _ = fs::rename(self.from, self.to);
            }
        }
    }
    let mut guard = TokenRestoreGuard {
        from: &backup_path,
        to: &token_path,
        active: true,
    };

    let correct_result = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            eliot_daemon::client_call_with_owner_credential(
                &fixture.data,
                "daemon.status",
                json!({}),
                &token,
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("daemon.status timed out"))
    });

    let wrong_token = format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new());
    let wrong_result = runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(5),
            eliot_daemon::client_call_with_owner_credential(
                &fixture.data,
                "daemon.status",
                json!({}),
                &wrong_token,
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("daemon.status timed out"))
    });

    // Restore file BEFORE assertions/stop
    fs::rename(&backup_path, &token_path).expect("restore owner.token");
    guard.active = false;

    let value = correct_result.expect("correct explicit credential must work over actual IPC");
    let status: DaemonStatusResponse =
        serde_json::from_value(value).expect("typed DaemonStatusResponse");
    DaemonStatusRequest {}
        .validate_response(&status)
        .expect("valid DaemonStatusResponse");
    assert_eq!(status.state, "running");
    assert_eq!(status.agents, 0);

    let error = wrong_result.expect_err("wrong valid-shaped credential must get not_allowed");
    assert_eq!(error.code, "not_allowed");
}

fn assert_invalid_read_rpc(
    runtime: &tokio::runtime::Runtime,
    fixture: &Fixture,
    method: &str,
    field: &str,
    params: Value,
) {
    let error = match public_rpc(runtime, fixture, method, params) {
        Err(error) => error,
        Ok(_) => panic!("{method} accepted malformed {field}"),
    };
    assert_eq!(error.code, "invalid", "{method} malformed {field}");
}

fn assert_public_mutation_grammar(fixture: &Fixture, runtime: &tokio::runtime::Runtime) {
    let id = ulid::Ulid::new().to_string();
    for params in [
        json!({"verb":"send","args":{}}),
        json!({"id":id,"verb":true,"args":{}}),
        json!({"id":id,"verb":"unused-verb","args":null}),
        json!({"id":id,"verb":"unused-verb","args":[]}),
        json!({"id":id,"verb":"unused-verb","args":{},"extra":true}),
    ] {
        assert_invalid_read_rpc(
            runtime,
            fixture,
            "command.submit",
            "mutation grammar",
            params,
        );
    }
    for params in [
        json!({"id":id,"verb":"unused-verb"}),
        json!({"id":id,"verb":"unused-verb","target":null,"expected_generation":null,
            "expected_turn_id":null,"request_id":null}),
        json!({"id":id,"verb":"unused-verb","expected_generation":-1}),
    ] {
        let request: CommandSubmitRequest =
            serde_json::from_value(params.clone()).expect("existing submit grammar");
        assert_eq!(request.args, json!({}));
        let error = match public_rpc(runtime, fixture, "command.submit", params) {
            Err(error) => error,
            Ok(_) => panic!("unsupported verb admitted an effect"),
        };
        assert_eq!(
            error.code, "unsupported",
            "shared grammar changed downstream admission errors"
        );
    }
    for params in [
        json!({"mode":"drain"}),
        json!({"id":id,"mode":false}),
        json!({"id":id,"mode":"restart"}),
        json!({"id":id,"mode":"drain","extra":true}),
    ] {
        assert_invalid_read_rpc(
            runtime,
            fixture,
            "daemon.shutdown",
            "mutation grammar",
            params,
        );
    }
    assert_eq!(fixture.json(&["status"])["admission"], "open");
}

#[test]
fn cli_launch_rejects_selector_credential_collision_before_persistence() {
    let fixture = Fixture::new("selector credential collision");
    let credential = format!("test-selector-credential-{}", ulid::Ulid::new());

    let mut data_argument = fixture.data.as_os_str().to_os_string();
    data_argument.push(std::path::MAIN_SEPARATOR_STR);
    let mut daemon_command = Command::new(CLI);
    scrub_ambient_identity(&mut daemon_command);
    daemon_command
        .arg("--data-dir")
        .arg(data_argument)
        .args(["daemon", "--foreground"])
        .env("CODEX_HOME", credential.as_str())
        .env("CODEX_SQLITE_HOME", credential.as_str())
        .env("EPHEMERAL_TOKEN", credential.as_str());
    let mut daemon = Captured::spawn(daemon_command);
    let startup_deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(exit) = daemon.child.try_wait().expect("foreground daemon status") {
            let (stdout, stderr) = daemon.output(Duration::from_secs(2));
            let safe_output = format!("stdout: {stdout}\nstderr: {stderr}")
                .replace(credential.as_str(), "[REDACTED_TEST_CREDENTIAL]");
            let bounded_output = safe_output.chars().take(2048).collect::<String>();
            panic!("foreground daemon exited during startup ({exit}): {bounded_output}");
        }
        let status = cli(&fixture.data, &["status"]);
        if status.status.success() {
            assert_eq!(status.json()["state"], "running");
            break;
        }
        assert!(
            std::time::Instant::now() < startup_deadline,
            "foreground daemon readiness deadline"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    fs::write(
        fixture.data.join("config.toml"),
        r#"
[profiles.privacy-audit]
harness = "codex"
model = "test-model"
pass_env = ["CODEX_HOME", "CODEX_SQLITE_HOME", "EPHEMERAL_TOKEN"]
"#,
    )
    .expect("write privacy regression profile under the initialized private root");

    let mut launch = Command::new(CLI);
    scrub_ambient_identity(&mut launch);
    launch
        .arg("--data-dir")
        .arg(&fixture.data)
        .args([
            "--json",
            "launch",
            "--profile",
            "privacy-audit",
            "--workspace",
        ])
        .arg(&fixture.workspace);
    let reply = run(launch);
    assert_eq!(reply.status.code(), Some(3), "launch must be rejected");
    let diagnostic = reply.stderr.to_ascii_lowercase();
    assert!(
        diagnostic.contains("secret_in_payload") && diagnostic.contains("credential"),
        "launch did not report a secret-payload credential rejection"
    );
    assert!(!reply.stdout.contains(&credential));
    assert!(!reply.stderr.contains(&credential));

    assert_eq!(fixture.json(&["status"])["agents"], 0);
    assert!(fixture.events().is_empty(), "rejected launch wrote events");

    let stop = cli(&fixture.data, &["daemon", "stop", "--force"]);
    assert!(
        matches!(stop.status.code(), Some(0 | 6)),
        "daemon cleanup request failed"
    );
    assert!(
        daemon
            .wait(Duration::from_secs(20))
            .expect("owned daemon shutdown deadline")
            .success(),
        "owned daemon returned nonzero"
    );
    let (_, daemon_stderr) = daemon.output(Duration::from_secs(2));
    assert!(!daemon_stderr.contains("panicked"), "daemon panicked");

    let secret_bytes = credential.as_bytes();
    assert!(
        fixture.data.join("eliot.db").is_file(),
        "daemon Store was not created"
    );
    for path in [
        fixture.data.join("eliot.db"),
        fixture.data.join("eliot.db-wal"),
        fixture.data.join("eliot.db-shm"),
        fixture.data.join("logs/daemon.log"),
    ] {
        if path.is_file() {
            let bytes = fs::read(path).expect("read persisted fixture file");
            assert!(
                !bytes
                    .windows(secret_bytes.len())
                    .any(|window| window == secret_bytes),
                "test credential was persisted"
            );
        }
    }
}

#[test]
fn public_agent_views_validate_without_mutation() {
    let mut fixture = Fixture::new("typed Agent view validation");
    fixture.start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("public view RPC runtime");
    let before: EventsReadResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "events.read", json!({}))
            .unwrap_or_else(|_| panic!("view baseline failed")),
    )
    .expect("typed event boundary");
    assert_eq!(before.high_water, 0);
    assert_public_mutation_grammar(&fixture, &runtime);
    for method in [
        "agent.get",
        "agent.list",
        "agent.processes",
        "agent.stats",
        "agent.git",
        "agent.edits",
        "daemon.status",
    ] {
        for params in [
            Value::Null,
            json!([]),
            json!(true),
            json!("object-required"),
        ] {
            assert_invalid_read_rpc(&runtime, &fixture, method, "object params", params);
        }
        let mut params = if matches!(method, "agent.list" | "daemon.status") {
            json!({})
        } else {
            json!({"id":"a1"})
        };
        params["extra"] = json!(true);
        assert_invalid_read_rpc(&runtime, &fixture, method, "unknown key", params);
    }
    for method in [
        "agent.get",
        "agent.processes",
        "agent.stats",
        "agent.git",
        "agent.edits",
    ] {
        assert_invalid_read_rpc(&runtime, &fixture, method, "missing id", json!({}));
        for id in [
            Value::Null,
            json!(1),
            json!(false),
            json!([]),
            json!({}),
            json!(""),
            json!(" \t\n"),
        ] {
            assert_invalid_read_rpc(&runtime, &fixture, method, "id type", json!({"id":id}));
        }
    }
    for method in ["agent.processes", "agent.stats", "agent.git", "agent.edits"] {
        for generation in [
            json!(-1),
            json!(u64::MAX),
            json!("0"),
            json!(false),
            json!(1.0),
            json!([]),
            json!({}),
        ] {
            assert_invalid_read_rpc(
                &runtime,
                &fixture,
                method,
                "generation type",
                json!({"id":"a1","generation":generation}),
            );
        }
    }
    for method in ["agent.list", "agent.processes", "agent.edits"] {
        for limit in [
            Value::Null,
            json!(0),
            json!(-1),
            json!(1001),
            json!("1"),
            json!(true),
            json!(1.0),
            json!(u64::MAX),
        ] {
            let mut params = if method == "agent.list" {
                json!({})
            } else {
                json!({"id":"a1"})
            };
            params["limit"] = limit;
            assert_invalid_read_rpc(&runtime, &fixture, method, "limit", params);
        }
    }
    for observations in [Value::Null, json!(0), json!("false"), json!([]), json!({})] {
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.list",
            "observations",
            json!({"observations":observations}),
        );
    }
    for method in ["agent.list", "agent.processes"] {
        for after in [
            json!(1),
            json!(true),
            json!([]),
            json!({}),
            json!(""),
            json!(" \t"),
        ] {
            let mut params = if method == "agent.list" {
                json!({})
            } else {
                json!({"id":"a1"})
            };
            params["after"] = after;
            assert_invalid_read_rpc(&runtime, &fixture, method, "after type", params);
        }
    }
    for scope in [
        json!(1),
        json!(true),
        json!([]),
        json!({}),
        json!(""),
        json!(" "),
        json!("other"),
        json!(" agent "),
    ] {
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.processes",
            "scope",
            json!({"id":"a1","scope":scope}),
        );
    }
    for after in [
        json!(1),
        json!(true),
        json!([]),
        json!({}),
        json!(""),
        json!("a\0b"),
        json!("é".repeat(32 * 1024 + 1)),
    ] {
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.edits",
            "edit cursor",
            json!({"id":"a1","after":after}),
        );
    }
    let cursor = json!({"agent":"a1","generation":0,"scope":"agent",
        "identity":{"host_boot_id":"boot","pid":1,"start_time":1}});
    for after in [
        "not-json".to_owned(),
        "[]".to_owned(),
        "x".repeat(PROCESS_CURSOR_MAX_BYTES + 1),
        serde_json::to_string(&cursor["identity"]).expect("obsolete identity cursor"),
    ] {
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.processes",
            "cursor wrapper",
            json!({"id":"a1","after":after}),
        );
    }
    for (field, value) in [
        ("host_boot_id", json!("")),
        ("host_boot_id", json!("é".repeat(257))),
        ("host_boot_id", json!("boot\n")),
        ("pid", json!(0)),
        ("pid", json!(u64::from(u32::MAX) + 1)),
        ("start_time", json!(0)),
        ("start_time", json!(-1)),
    ] {
        let mut invalid = cursor.clone();
        invalid["identity"][field] = value;
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.processes",
            "cursor identity",
            json!({"id":"a1","after":serde_json::to_string(&invalid).expect("invalid cursor wire")}),
        );
    }
    let mut unknown = cursor.clone();
    unknown["extra"] = json!(true);
    assert_invalid_read_rpc(
        &runtime,
        &fixture,
        "agent.processes",
        "cursor unknown key",
        json!({"id":"a1","after":serde_json::to_string(&unknown).expect("cursor wire")}),
    );

    for (method, params) in [
        ("agent.get", json!({"id":"a1"})),
        ("agent.stats", json!({"id":"a1","generation":null})),
        ("agent.git", json!({"id":"a1","generation":null})),
        (
            "agent.processes",
            json!({"id":"a1","generation":null,"scope":null,"after":null}),
        ),
        (
            "agent.processes",
            json!({"id":"a1","scope":"workspace","generation":null}),
        ),
        (
            "agent.processes",
            json!({"id":"a1","after":serde_json::to_string(&cursor).expect("valid cursor wire")}),
        ),
        (
            "agent.edits",
            json!({"id":"a1","generation":null,"after":null}),
        ),
        ("agent.edits", json!({"id":"a1","after":" "})),
        (
            "agent.edits",
            json!({"id":"a1","after":" ../literal\\path// "}),
        ),
        (
            "agent.edits",
            json!({"id":"a1","after":"é".repeat(32 * 1024)}),
        ),
    ] {
        let error = match public_rpc(&runtime, &fixture, method, params) {
            Err(error) => error,
            Ok(_) => panic!("{method} invented an Agent"),
        };
        assert_eq!(
            error.code, "not_found",
            "{method} tightened valid null/cursor grammar"
        );
    }
    for observations in [None, Some(false), Some(true)] {
        let mut params = json!({"after":null});
        if let Some(value) = observations {
            params["observations"] = json!(value);
        }
        let raw = public_rpc(&runtime, &fixture, "agent.list", params)
            .unwrap_or_else(|_| panic!("valid empty Agent list failed"));
        let reply: AgentListResponse =
            serde_json::from_value(raw.clone()).expect("typed empty list");
        assert!(reply.agents.is_empty());
        assert!(reply.next.is_none());
        assert!(
            serde_json::to_value(reply).expect("list wire") == raw,
            "typed list changed wire fields"
        );
    }
    let raw = public_rpc(&runtime, &fixture, "daemon.status", json!({}))
        .unwrap_or_else(|_| panic!("typed status read failed"));
    let status: DaemonStatusResponse = serde_json::from_value(raw.clone()).expect("typed status");
    DaemonStatusRequest::default()
        .validate_response(&status)
        .expect("matching empty status");
    assert_eq!(status.agents, 0);
    assert!(
        serde_json::to_value(status).expect("status wire") == raw,
        "typed status changed nullable fields"
    );
    let after: EventsReadResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "events.read", json!({}))
            .unwrap_or_else(|_| panic!("view event readback failed")),
    )
    .expect("typed unchanged event boundary");
    assert_eq!(after.high_water, before.high_water);
    assert_eq!(after.events, before.events);
    fixture.stop(false);
}

#[test]
fn public_native_metadata_queries_validate_without_admitting_agents() {
    let mut fixture = Fixture::new("typed native metadata validation");
    fixture.start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("public RPC runtime");
    let before: EventsReadResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "events.read", json!({}))
            .unwrap_or_else(|_| panic!("baseline metadata event read failed")),
    )
    .expect("typed event boundary");
    assert_eq!(before.high_water, 0);
    let bad_strings = [
        Value::Null,
        json!(1),
        json!(true),
        json!([]),
        json!({}),
        json!(""),
        json!(" \t\n"),
    ];
    assert_invalid_read_rpc(
        &runtime,
        &fixture,
        "agent.discover",
        "missing profile",
        json!({}),
    );
    for value in &bad_strings {
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.discover",
            "profile",
            json!({"profile":value}),
        );
    }
    for field in ["harness", "native_scope", "cursor"] {
        for value in &bad_strings[1..] {
            let mut params = json!({"profile":"codex"});
            params[field] = value.clone();
            assert_invalid_read_rpc(&runtime, &fixture, "agent.discover", field, params);
        }
    }
    assert_invalid_read_rpc(
        &runtime,
        &fixture,
        "agent.discover",
        "scope byte bound",
        json!({"profile":"codex","native_scope":"é".repeat(257)}),
    );
    for field in ["native_scope", "cursor"] {
        for control in ['\0', '\n', '\u{7f}'] {
            let mut params = json!({"profile":"codex"});
            params[field] = json!(format!("nonempty{control}value"));
            assert_invalid_read_rpc(
                &runtime,
                &fixture,
                "agent.discover",
                "scope/cursor controls",
                params,
            );
        }
    }
    assert_invalid_read_rpc(
        &runtime,
        &fixture,
        "agent.discover",
        "cursor byte bound",
        json!({"profile":"codex","cursor":"é".repeat(MAX_OBSERVATION_CURSOR_BYTES / 2 + 1)}),
    );
    assert_invalid_read_rpc(
        &runtime,
        &fixture,
        "agent.discover",
        "unknown field",
        json!({"profile":"codex","extra":true}),
    );

    let history = json!({"id":"a1","source":"native","view":"turns"});
    for field in ["id", "source", "view"] {
        let mut missing = history.clone();
        missing
            .as_object_mut()
            .expect("request object")
            .remove(field);
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.history",
            "missing required string",
            missing,
        );
        for value in &bad_strings {
            let mut params = history.clone();
            params[field] = value.clone();
            assert_invalid_read_rpc(&runtime, &fixture, "agent.history", field, params);
        }
    }
    for generation in [
        json!(-1),
        json!(u64::MAX),
        json!("0"),
        json!(true),
        json!(1.0),
        json!([]),
        json!({}),
    ] {
        let mut params = history.clone();
        params["generation"] = generation;
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.history",
            "optional generation",
            params,
        );
    }
    for field in ["turn_id", "cursor"] {
        for value in &bad_strings[1..] {
            let mut params = history.clone();
            if field == "turn_id" {
                params["view"] = json!("items");
            }
            params[field] = value.clone();
            assert_invalid_read_rpc(&runtime, &fixture, "agent.history", field, params);
        }
    }
    for source in ["unsupported", "native "] {
        let mut params = history.clone();
        params["source"] = json!(source);
        assert_invalid_read_rpc(&runtime, &fixture, "agent.history", "source domain", params);
    }
    for view in ["unsupported", "turns "] {
        let mut params = history.clone();
        params["view"] = json!(view);
        assert_invalid_read_rpc(&runtime, &fixture, "agent.history", "view domain", params);
    }
    for turn_id in [None, Some(Value::Null)] {
        let mut params = json!({"id":"a1","source":"native","view":"items"});
        if let Some(value) = turn_id {
            params["turn_id"] = value;
        }
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.history",
            "items requires turn",
            params,
        );
    }
    for view in ["subject", "turns"] {
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.history",
            "other views forbid turn",
            json!({"id":"a1","source":"native","view":view,"turn_id":"turn-1"}),
        );
    }
    for turn_id in [
        "turn\0id".to_owned(),
        "turn\nid".to_owned(),
        "é".repeat(MAX_OBSERVATION_CURSOR_BYTES / 2 + 1),
    ] {
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.history",
            "turn byte/control bound",
            json!({"id":"a1","source":"native","view":"items","turn_id":turn_id}),
        );
    }
    let mut oversized_cursor = history.clone();
    oversized_cursor["cursor"] = json!("é".repeat(MAX_OBSERVATION_CURSOR_BYTES / 2 + 1));
    assert_invalid_read_rpc(
        &runtime,
        &fixture,
        "agent.history",
        "cursor byte bound",
        oversized_cursor,
    );
    let mut extra = history.clone();
    extra["extra"] = json!(true);
    assert_invalid_read_rpc(&runtime, &fixture, "agent.history", "unknown field", extra);
    for limit in [
        Value::Null,
        json!(0),
        json!(1001),
        json!(-1),
        json!(true),
        json!("1"),
        json!(1.0),
        json!(u64::MAX),
    ] {
        for (method, mut params) in [
            ("agent.discover", json!({"profile":"codex"})),
            ("agent.history", history.clone()),
        ] {
            params["limit"] = limit.clone();
            assert_invalid_read_rpc(&runtime, &fixture, method, "limit", params);
        }
    }
    // Null optionals and cursor controls permitted by the private query grammar
    // reach Agent lookup. No Agent exists, so these must not fail as bad shapes.
    for params in [
        json!({"id":"a1","source":"native","view":"turns","generation":null,"turn_id":null,"cursor":null}),
        json!({"id":"a1","source":"native","view":"turns","cursor":"\0nonempty"}),
        json!({"id":"a1","source":"native","view":"turns","cursor":"é".repeat(MAX_OBSERVATION_CURSOR_BYTES / 2)}),
        json!({"id":"a1","source":"rollout","view":"subject","generation":0}),
        json!({"id":"a1","source":"native","view":"items","turn_id":"é".repeat(MAX_OBSERVATION_CURSOR_BYTES / 2)}),
    ] {
        let error = match public_rpc(&runtime, &fixture, "agent.history", params) {
            Err(error) => error,
            Ok(_) => panic!("history invented an observer"),
        };
        assert_eq!(
            error.code, "not_found",
            "valid history grammar was tightened"
        );
    }
    let status = fixture.json(&["status"]);
    for field in [
        "agents",
        "owned_plugins",
        "discovery_readers",
        "observer_readers",
    ] {
        assert_eq!(status[field], 0);
    }
    let after: EventsReadResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "events.read", json!({}))
            .unwrap_or_else(|_| panic!("event read after metadata queries failed")),
    )
    .expect("typed unchanged event boundary");
    assert_eq!(after.high_water, before.high_water);
    assert_eq!(after.events, before.events);
    fixture.stop(false);
}

#[test]
fn public_result_queries_reject_bad_shapes_without_mutation() {
    let mut fixture = Fixture::new("typed result validation");
    fixture.start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("public RPC runtime");
    let before: EventsReadResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "events.read", json!({}))
            .unwrap_or_else(|_| panic!("baseline event read failed")),
    )
    .expect("typed event boundary");
    assert_eq!(before.high_water, 0);
    let bad_ids = [
        Value::Null,
        json!(1),
        json!(true),
        json!([]),
        json!({}),
        json!(""),
        json!(" \t\r\n"),
    ];
    for method in ["command.get", "agent.result"] {
        assert_invalid_read_rpc(&runtime, &fixture, method, "missing id", json!({}));
        for id in &bad_ids {
            assert_invalid_read_rpc(&runtime, &fixture, method, "id", json!({"id":id}));
        }
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            method,
            "unknown field",
            json!({"id":"a1","extra":true}),
        );
    }
    let turn_query = json!({"agent":"a1","generation":0,"turn_id":"turn-1"});
    for field in ["agent", "turn_id"] {
        let mut missing = turn_query.clone();
        missing
            .as_object_mut()
            .expect("request object")
            .remove(field);
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "turn.get",
            "missing identifier",
            missing,
        );
        for value in &bad_ids {
            let mut params = turn_query.clone();
            params[field] = value.clone();
            assert_invalid_read_rpc(&runtime, &fixture, "turn.get", field, params);
        }
    }
    let mut missing = turn_query.clone();
    missing
        .as_object_mut()
        .expect("request object")
        .remove("generation");
    assert_invalid_read_rpc(
        &runtime,
        &fixture,
        "turn.get",
        "missing generation",
        missing,
    );
    for generation in [
        Value::Null,
        json!("0"),
        json!(true),
        json!(-1),
        json!(i64::MIN),
        json!(u64::MAX),
        json!(0.5),
        json!(1.0),
        json!([]),
        json!({}),
    ] {
        let mut params = turn_query.clone();
        params["generation"] = generation.clone();
        assert_invalid_read_rpc(&runtime, &fixture, "turn.get", "generation", params);
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.result",
            "optional generation",
            json!({"id":"a1","generation":generation}),
        );
    }
    let mut extra = turn_query.clone();
    extra["extra"] = json!(true);
    assert_invalid_read_rpc(&runtime, &fixture, "turn.get", "unknown field", extra);
    for turn_id in &bad_ids {
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.result",
            "optional turn_id",
            json!({"id":"a1","turn_id":turn_id}),
        );
    }
    for limit in [
        Value::Null,
        json!(0),
        json!(-1),
        json!(1001),
        json!("1"),
        json!(true),
        json!(1.0),
        json!([]),
        json!({}),
        json!(u64::MAX),
    ] {
        for exact in [false, true] {
            let mut params = json!({"id":"a1","limit":limit});
            if exact {
                params["turn_id"] = json!("turn-1");
            }
            assert_invalid_read_rpc(
                &runtime,
                &fixture,
                "agent.result",
                "limit including exact turn",
                params,
            );
        }
    }
    // Valid grammar reaches record lookup; generation zero and non-ULID command
    // selectors do not gain another validation rule or arbitrary length cap.
    for (method, params) in [
        ("command.get", json!({"id":"missing-command"})),
        ("command.get", json!({"id":"x".repeat(4097)})),
        ("turn.get", turn_query),
        ("agent.result", json!({"id":"a1"})),
        (
            "agent.result",
            json!({"id":"a1","generation":0,"limit":1000}),
        ),
    ] {
        let error = match public_rpc(&runtime, &fixture, method, params) {
            Err(error) => error,
            Ok(_) => panic!("{method} invented a missing record"),
        };
        assert_eq!(
            error.code, "not_found",
            "{method} rejected valid read grammar"
        );
    }
    let after: EventsReadResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "events.read", json!({}))
            .unwrap_or_else(|_| panic!("event read after malformed queries failed")),
    )
    .expect("typed unchanged event boundary");
    assert_eq!(after.high_water, before.high_water);
    assert_eq!(after.next, before.next);
    assert_eq!(after.events, before.events);
    fixture.stop(false);
}

#[test]
fn public_event_requests_validate_semantic_values_without_mutation() {
    let mut fixture = Fixture::new("typed event validation");
    fixture.start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("public RPC runtime");
    let raw = public_rpc(&runtime, &fixture, "events.read", json!({}))
        .unwrap_or_else(|_| panic!("default events.read failed"));
    let before: EventsReadResponse = serde_json::from_value(raw).expect("typed read response");
    assert_eq!(before.high_water, 0);
    assert_eq!(before.next, 0);
    assert!(before.events.is_empty());
    assert!(!before.truncated);
    let read_defaults = EventsReadRequest::default();
    assert_eq!(read_defaults.after, 0);
    assert_eq!(read_defaults.limit, 100);
    let subscribe_defaults = EventsSubscribeRequest::default();
    assert_eq!(subscribe_defaults.after, 0);
    assert_eq!(subscribe_defaults.limit, 100);
    assert_eq!(subscribe_defaults.wait_ms, 0);

    for method in ["events.read", "events.subscribe"] {
        for after in [
            Value::Null,
            json!(0.5),
            json!(1.0),
            json!("0"),
            json!(true),
            json!(false),
            json!(-1),
            json!(i64::MIN),
            json!(u64::MAX),
            json!([]),
            json!({}),
        ] {
            assert_invalid_read_rpc(&runtime, &fixture, method, "after", json!({"after": after}));
        }
        for limit in [
            Value::Null,
            json!(0),
            json!(-1),
            json!(1001),
            json!(1.0),
            json!("1"),
            json!(true),
            json!([]),
            json!({}),
            json!(u64::MAX),
        ] {
            assert_invalid_read_rpc(&runtime, &fixture, method, "limit", json!({"limit": limit}));
        }
        for filter in [
            Value::Null,
            json!(false),
            json!(1),
            json!("all"),
            json!([]),
            json!({"extra": []}),
            json!([[], [], []]),
            json!([["a1"], [], []]),
        ] {
            assert_invalid_read_rpc(
                &runtime,
                &fixture,
                method,
                "filter",
                json!({"filter": filter}),
            );
        }
        for field in ["agents", "kinds", "sources"] {
            for values in [
                Value::Null,
                json!("a1"),
                json!(true),
                json!([1]),
                json!([null]),
                json!([""]),
            ] {
                let mut filter = json!({});
                filter[field] = values;
                assert_invalid_read_rpc(
                    &runtime,
                    &fixture,
                    method,
                    "filter list",
                    json!({"filter": filter}),
                );
            }
            let mut filter = json!({});
            filter[field] = json!(vec!["repeated"; 1001]);
            assert_invalid_read_rpc(
                &runtime,
                &fixture,
                method,
                "raw filter count",
                json!({"filter": filter}),
            );
        }
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            method,
            "unknown request field",
            json!({"unknown": true}),
        );
        let raw = public_rpc(
            &runtime,
            &fixture,
            method,
            json!({"after": 0, "limit": 1000, "filter": {}}),
        )
        .unwrap_or_else(|_| panic!("{method} rejected valid empty query"));
        assert_eq!(raw["next"], 0);
        assert_eq!(raw["high_water"], before.high_water);
        assert_eq!(raw["events"].as_array().expect("events").len(), 0);
    }
    for wait_ms in [
        Value::Null,
        json!(-1),
        json!(0.5),
        json!(1.0),
        json!("0"),
        json!(true),
        json!([]),
        json!({}),
        serde_json::from_str::<Value>("18446744073709551616").expect("overflowing integer JSON"),
    ] {
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "events.subscribe",
            "wait_ms",
            json!({"wait_ms": wait_ms}),
        );
    }
    assert_invalid_read_rpc(
        &runtime,
        &fixture,
        "events.read",
        "subscribe-only field",
        json!({"wait_ms": 0}),
    );
    let unsupported = match public_rpc(&runtime, &fixture, "events.unimplemented", json!({})) {
        Err(error) => error,
        Ok(_) => panic!("unknown event method unexpectedly succeeded"),
    };
    assert_eq!(unsupported.code, "unsupported");
    let raw = public_rpc(&runtime, &fixture, "events.subscribe", json!({}))
        .unwrap_or_else(|_| panic!("default events.subscribe failed"));
    let subscribed: EventsSubscribeResponse =
        serde_json::from_value(raw).expect("typed subscribe response");
    assert!(subscribed.events.is_empty());
    assert_eq!(subscribed.next, 0);
    assert_eq!(subscribed.high_water, before.high_water);
    assert!(!subscribed.truncated);
    assert!(!subscribed.lagged);
    let raw = public_rpc(&runtime, &fixture, "events.read", json!({}))
        .unwrap_or_else(|_| panic!("read after malformed requests failed"));
    let after: EventsReadResponse = serde_json::from_value(raw).expect("typed unchanged replay");
    assert_eq!(after.high_water, before.high_water);
    assert_eq!(after.next, before.next);
    assert_eq!(after.events, before.events);
    fixture.stop(false);
}

#[test]
fn foreground_startup_and_cli_help() {
    for argument in ["--version", "--help"] {
        let mut command = Command::new(CLI);
        scrub_ambient_identity(&mut command);
        command.arg(argument);
        let reply = run(command);
        assert!(reply.status.success(), "{}", reply.stderr);
        assert!(reply.stdout.contains("swarm"));
    }
    let mut fixture = Fixture::new("foreground capture");
    fixture.start();
    fixture.stop(false);
}

#[cfg(windows)]
#[test]
fn background_startup_releases_capture_before_daemon_exit() {
    let mut fixture = Fixture::new("background capture");
    // Until the birth-bound background handle proves exit, never remove its data.
    fixture.retain_data = true;
    let mut data_argument = fixture.data.as_os_str().to_os_string();
    data_argument.push(std::path::MAIN_SEPARATOR_STR);
    let mut command = Command::new(CLI);
    scrub_ambient_identity(&mut command);
    command
        .arg("--data-dir")
        .arg(data_argument)
        .args(["--json", "daemon"]);
    let mut starter = Captured::spawn(command);
    assert!(
        starter
            .wait(Duration::from_secs(40))
            .expect("background starter deadline")
            .success()
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let info = loop {
        if let Ok(bytes) = fs::read(fixture.data.join("daemon.json")) {
            break serde_json::from_slice::<Value>(&bytes).expect("daemon metadata");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "background daemon metadata deadline"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    let held = windows_process::Daemon::open(&fixture.data, &info);
    let status = fixture.json(&["status"]);
    assert_eq!(status["state"], "running");
    assert_eq!(
        status["data_dir"].as_str().expect("status data_dir"),
        fixture.data.to_string_lossy(),
        "status must show the resolved data directory"
    );
    let (stdout, _) = starter.output(Duration::from_secs(5));
    let frames: Vec<Value> = stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("starter JSON frame"))
        .collect();
    assert!(
        frames.iter().any(|frame| frame["state"] == "starting"),
        "starter must report starting: {stdout}"
    );
    let ready = frames
        .iter()
        .find(|frame| frame["state"] == "ready")
        .expect("starter must report ready after the readiness wait");
    let ready_dir = ready["data_dir"]
        .as_str()
        .expect("ready data_dir")
        .trim_end_matches(['/', '\\']);
    let expected_dir = fixture
        .data
        .to_string_lossy()
        .trim_end_matches(['/', '\\'])
        .to_owned();
    assert_eq!(
        ready_dir, expected_dir,
        "starter ready must carry the resolved data directory"
    );
    assert_eq!(
        ready["boot_id"].as_str().expect("ready boot_id"),
        info["boot_id"].as_str().expect("daemon boot id"),
        "starter ready must carry the daemon boot id"
    );
    assert_eq!(
        held.wait(Duration::ZERO).unwrap(),
        None,
        "capture EOF must precede daemon shutdown"
    );
    assert_eq!(fixture.json(&["status"])["state"], "running");
    let stop = cli(&fixture.data, &["daemon", "stop", "--force"]);
    assert!(
        matches!(stop.status.code(), Some(0 | 6)),
        "shutdown reply: {}",
        stop.stderr
    );
    assert_eq!(held.wait(Duration::from_secs(15)).unwrap(), Some(0));
    fixture.retain_data = false;
}

#[test]
fn status_reports_resolved_data_dir() {
    let mut fixture = Fixture::new("status data dir");
    fixture.start();
    let status = fixture.json(&["status"]);
    assert_eq!(status["state"], "running");
    assert_eq!(
        status["data_dir"].as_str().expect("status data_dir"),
        fixture.data.to_string_lossy(),
        "status must show the resolved data directory"
    );
    fixture.stop(false);
}

#[test]
fn connect_failure_hint_points_at_recorded_failure() {
    use std::io::Write as _;
    let mut fixture = Fixture::new("connect hint");
    fixture.start();
    fixture.stop(false);
    let log_path = fixture.data.join("logs").join("daemon.log");
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_millis() as u64;
    let record = serde_json::json!({
        "ts": now_ms,
        "code": "daemon.failed",
        "error_code": "frame_too_large",
    });
    let mut log = fs::OpenOptions::new()
        .append(true)
        .open(&log_path)
        .expect("append daemon log");
    writeln!(log, "{record}").expect("write failure record");
    drop(log);
    let owner_token =
        fs::read_to_string(fixture.data.join("owner.token")).expect("fixture owner token");

    let reply = cli(&fixture.data, &["status"]);
    assert!(!reply.status.success(), "status without a daemon must fail");
    assert!(
        reply.stderr.contains("hint: data_dir="),
        "connect error must carry a data-dir hint: {}",
        reply.stderr
    );
    assert!(
        reply.stderr.contains("frame_too_large"),
        "hint must include the last daemon.failed code: {}",
        reply.stderr
    );
    assert!(
        reply.stderr.contains("daemon log present"),
        "hint must say whether the daemon log exists: {}",
        reply.stderr
    );
    assert!(
        !reply.stderr.contains(&owner_token),
        "connect hint must never print secrets"
    );
    assert!(
        !reply.stdout.contains(&owner_token),
        "connect hint must never print secrets on stdout"
    );
}

#[test]
fn resume_wait_rejects_nonpositive_timeout() {
    let mut fixture = Fixture::new("resume timeout");
    fixture.start();
    let reply = cli(
        &fixture.data,
        &["resume", "a1", "--wait", "--timeout-seconds", "0"],
    );
    assert_eq!(reply.status.code(), Some(2), "stderr: {}", reply.stderr);
    assert!(
        reply
            .stderr
            .contains("--timeout-seconds must be greater than zero"),
        "resume --wait must honor --timeout-seconds: {}",
        reply.stderr
    );
    fixture.stop(false);
}

#[test]
fn owner_var_cas_conflict_and_global_scope() {
    let mut fixture = Fixture::new("owner var cas");
    fixture.start();
    let set = fixture.json(&["var", "set", "--scope", "project:app", "branch", "main"]);
    assert_eq!(set["version"], 1);
    assert_eq!(set["set_by"], "owner");

    let conflict = cli(
        &fixture.data,
        &[
            "var",
            "set",
            "--scope",
            "project:app",
            "--expect-version",
            "0",
            "branch",
            "stale",
        ],
    );
    assert_eq!(conflict.status.code(), Some(3));
    assert!(conflict.stderr.contains("version_conflict"));

    let read = fixture.json(&["var", "get", "--scope", "project:app", "branch"]);
    assert_eq!(read["value"], "main");
    assert_eq!(read["version"], 1);

    let bumped = fixture.json(&[
        "var",
        "set",
        "--scope",
        "project:app",
        "--expect-version",
        "1",
        "--note",
        "why",
        "branch",
        "dev",
    ]);
    assert_eq!(bumped["version"], 2);

    let global = fixture.json(&[
        "var",
        "set",
        "--scope",
        "global",
        "--as-json",
        "flag",
        "true",
    ]);
    assert_eq!(global["version"], 1);
    let gread = fixture.json(&["var", "get", "--scope", "global", "flag"]);
    assert_eq!(gread["value"], true);

    let events = fixture.events();
    let changed: Vec<_> = events
        .iter()
        .filter(|event| event["kind"] == "var.changed")
        .collect();
    assert_eq!(changed.len(), 3);
    assert_eq!(changed[0]["data"]["key"], "branch");
    assert_eq!(changed[0]["data"]["version"], 1);
    assert_eq!(changed[2]["data"]["scope"], "global");
    fixture.stop(false);
}

#[test]
fn fixture_cli_under_synthetic_ambient_identity_helper() {
    let mut fixture = Fixture::new("synthetic ambient identity helper");
    fixture.start();
    let status = fixture.json(&["status"]);
    assert_eq!(status["state"], "running");
    let who = fixture.json(&["who"]);
    let who_response: WhoResponse = serde_json::from_value(who).expect("typed WhoResponse");
    assert!(who_response.workers.is_empty());
    fixture.stop(false);
}

#[test]
fn fixture_cli_scrubs_ambient_identity_under_nested_runner() {
    let outer_identity: Vec<(String, Option<std::ffi::OsString>)> = AMBIENT_IDENTITY_ENV_KEYS
        .iter()
        .map(|&key| (key.to_owned(), std::env::var_os(key)))
        .collect();

    let self_exe = std::env::current_exe().expect("test binary path");
    let mut command = Command::new(self_exe);
    command.args([
        "--exact",
        "fixture_cli_under_synthetic_ambient_identity_helper",
        "--nocapture",
        "--test-threads=1",
    ]);
    for &key in AMBIENT_IDENTITY_ENV_KEYS {
        command.env(key, format!("synthetic-{key}-value"));
    }

    let reply = run(command);
    assert!(
        reply.status.success(),
        "nested test under synthetic ambient identity failed (exit {:?}):\nstdout:\n{}\nstderr:\n{}",
        reply.status.code(),
        reply.stdout,
        reply.stderr,
    );

    for (key, original_value) in outer_identity {
        let current = std::env::var_os(&key);
        assert!(
            current == original_value,
            "outer ambient context for {key} changed"
        );
    }
}
