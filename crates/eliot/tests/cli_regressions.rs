#![cfg(any(windows, target_os = "linux"))]

mod support;
#[cfg(windows)]
#[path = "support/windows_process.rs"]
mod windows_process;

use eliot_core::api::{
    AgentDiscoverRequest, AgentEditsRequest, AgentEditsResponse, AgentGetRequest, AgentGetResponse,
    AgentGitRequest, AgentGitResponse, AgentHistoryRequest, AgentListRequest, AgentListResponse,
    AgentProcessesRequest, AgentProcessesResponse, AgentResultRequest, AgentResultResponse,
    AgentStatsRequest, AgentStatsResponse, CommandGetRequest, CommandSubmitRequest,
    CommandSubmitResponse, DaemonShutdownRequest, DaemonShutdownResponse, DaemonStatusRequest,
    DaemonStatusResponse, DiscoverPage, EventsFilter, EventsReadRequest, EventsReadResponse,
    EventsSubscribeRequest, EventsSubscribeResponse, MAX_OBSERVATION_CURSOR_BYTES, ObservationPage,
    PROCESS_CURSOR_MAX_BYTES, ShutdownMode, TurnGetRequest,
};
use eliot_core::{Command as CoreCommand, CommandState, Turn as CoreTurn, TurnState};
use serde_json::{Value, json};
use std::time::Duration;
use std::{collections::HashSet, fs, path::Path, process::Command};
use support::Captured;
use support::{CLI, Fixture, cli, run};

fn public_rpc(
    runtime: &tokio::runtime::Runtime,
    fixture: &Fixture,
    method: &str,
    params: Value,
) -> eliot_core::Result<Value> {
    runtime.block_on(async {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            eliot_daemon::client_call(&fixture.data, method, params),
        )
        .await
        .unwrap_or_else(|_| panic!("{method} exceeded the bounded public RPC deadline"))
    })
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

fn retained_submission(
    fixture: &Fixture,
    runtime: &tokio::runtime::Runtime,
    request: &CommandSubmitRequest,
) -> CommandSubmitResponse {
    let raw = public_rpc(
        runtime,
        fixture,
        "command.submit",
        serde_json::to_value(request).expect("submit wire"),
    )
    .unwrap_or_else(|_| panic!("same-ID submission did not return its retained command"));
    let object = raw.as_object().expect("submit object");
    assert_eq!(object.len(), 6);
    for field in [
        "command_id",
        "agent",
        "generation",
        "state",
        "outcome",
        "linked_command",
    ] {
        assert!(object.contains_key(field), "submit omitted {field}");
    }
    let response: CommandSubmitResponse =
        serde_json::from_value(raw.clone()).expect("typed submit response");
    request
        .validate_response(&response)
        .expect("retained submit binding");
    assert!(
        serde_json::to_value(&response).expect("retained response wire") == raw,
        "typed submit changed nullable or opaque outcome fields"
    );
    response
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

fn assert_public_agent_views(
    fixture: &Fixture,
    runtime: &tokio::runtime::Runtime,
    agent: &str,
    generation: i64,
    observer: &str,
) {
    let commands_before: Vec<_> = fixture
        .events()
        .into_iter()
        .filter(|event| {
            event["kind"]
                .as_str()
                .is_some_and(|kind| kind.starts_with("command."))
        })
        .collect();
    let get = AgentGetRequest::new(agent);
    let raw = public_rpc(
        runtime,
        fixture,
        "agent.get",
        serde_json::to_value(&get).expect("get request"),
    )
    .unwrap_or_else(|_| panic!("Agent view failed"));
    let reply: AgentGetResponse = serde_json::from_value(raw.clone()).expect("typed Agent view");
    get.validate_response(&reply).expect("matching Agent view");
    assert_eq!(reply.agent.generation, generation);
    assert!(!reply.source_states.is_empty());
    assert!(
        serde_json::to_value(reply).expect("get wire") == raw,
        "typed show changed source states or nulls"
    );
    for observations in [false, true] {
        let request = AgentListRequest {
            after: None,
            limit: 1000,
            observations,
        };
        let raw = public_rpc(
            runtime,
            fixture,
            "agent.list",
            serde_json::to_value(&request).expect("list request"),
        )
        .unwrap_or_else(|_| panic!("Agent list failed"));
        let reply: AgentListResponse =
            serde_json::from_value(raw.clone()).expect("typed Agent list");
        request
            .validate_response(&reply)
            .expect("matching observation selection");
        assert_eq!(reply.agents.len(), 2);
        assert!(
            reply
                .agents
                .iter()
                .all(|row| row.observation.is_some() == observations)
        );
        for row in raw["agents"].as_array().expect("raw rows") {
            assert_eq!(row.get("observation").is_some(), observations);
        }
        assert!(
            serde_json::to_value(reply).expect("list wire") == raw,
            "typed list changed observation presence"
        );
    }
    let raw = public_rpc(runtime, fixture, "agent.list", json!({"limit":1}))
        .unwrap_or_else(|_| panic!("default Agent list failed"));
    assert!(raw["agents"][0].get("observation").is_none());
    let next = raw["next"].as_str().expect("first list cursor");
    let continued: AgentListResponse = serde_json::from_value(
        public_rpc(
            runtime,
            fixture,
            "agent.list",
            json!({"after":next,"limit":1}),
        )
        .unwrap_or_else(|_| panic!("Agent list continuation failed")),
    )
    .expect("typed continued list");
    assert_eq!(continued.agents.len(), 1);
    assert_ne!(continued.agents[0].agent.id, raw["agents"][0]["id"]);

    let stats_request = AgentStatsRequest::new(observer);
    let raw = public_rpc(
        runtime,
        fixture,
        "agent.stats",
        serde_json::to_value(&stats_request).expect("stats request"),
    )
    .unwrap_or_else(|_| panic!("detached observer stats failed"));
    let stats = raw["stats"].as_object().expect("cached stats fields");
    assert_eq!(stats["available"], false);
    assert_eq!(stats["ttl_ms"], 4000);
    for field in [
        "sampled_at",
        "age_ms",
        "complete",
        "cpu_time_ns",
        "rss_bytes",
        "samples",
    ] {
        assert!(
            stats.contains_key(field) && stats[field].is_null(),
            "unavailable stats fabricated {field}"
        );
    }
    assert!(raw["workspace"].is_null());
    let reply: AgentStatsResponse =
        serde_json::from_value(raw.clone()).expect("typed unavailable stats");
    stats_request
        .validate_response(&reply)
        .expect("matching unavailable stats");
    assert!(
        serde_json::to_value(reply).expect("stats wire") == raw,
        "typed stats changed freshness/null fields"
    );
    let git_request = AgentGitRequest::new(observer);
    let raw = public_rpc(
        runtime,
        fixture,
        "agent.git",
        serde_json::to_value(&git_request).expect("Git request"),
    )
    .unwrap_or_else(|_| panic!("unavailable Git view failed"));
    assert_eq!(raw["available"], false);
    let reply: AgentGitResponse =
        serde_json::from_value(raw.clone()).expect("typed unavailable Git");
    git_request
        .validate_response(&reply)
        .expect("matching unavailable Git");
    assert!(
        serde_json::to_value(reply).expect("Git wire") == raw,
        "typed Git changed unavailable wire fields"
    );
    let raw = public_rpc(runtime, fixture, "daemon.status", json!({}))
        .unwrap_or_else(|_| panic!("daemon status failed"));
    let status: DaemonStatusResponse =
        serde_json::from_value(raw.clone()).expect("typed daemon status");
    DaemonStatusRequest::default()
        .validate_response(&status)
        .expect("matching daemon status");
    assert_eq!(status.agents, 2);
    assert_eq!(status.observer_readers, 0);
    assert!(
        serde_json::to_value(status).expect("status wire") == raw,
        "typed status changed nullable wire fields"
    );

    let process_request = AgentProcessesRequest {
        generation: Some(generation),
        limit: 1,
        ..AgentProcessesRequest::new(agent)
    };
    let raw = public_rpc(
        runtime,
        fixture,
        "agent.processes",
        serde_json::to_value(&process_request).expect("process request"),
    )
    .unwrap_or_else(|_| panic!("Agent process read failed"));
    assert!(raw.get("scope").is_none());
    let process_page: AgentProcessesResponse =
        serde_json::from_value(raw.clone()).expect("typed processes");
    process_request
        .validate_response(&process_page)
        .expect("matching process page");
    assert!(!process_page.processes.is_empty());
    assert!(
        serde_json::to_value(&process_page).expect("process wire") == raw,
        "typed processes changed default scope wire"
    );
    let cursor = process_page
        .next
        .as_deref()
        .expect("nonempty page birth cursor");
    let continuation = AgentProcessesRequest {
        after: Some(cursor.to_owned()),
        ..process_request.clone()
    };
    let continued: AgentProcessesResponse = serde_json::from_value(
        public_rpc(
            runtime,
            fixture,
            "agent.processes",
            serde_json::to_value(&continuation).expect("process continuation"),
        )
        .unwrap_or_else(|_| panic!("unchanged process cursor failed")),
    )
    .expect("typed process continuation");
    continuation
        .validate_response(&continued)
        .expect("matching process continuation");
    let parsed: Value = serde_json::from_str(cursor).expect("opaque process cursor wrapper");
    for (field, value) in [
        ("agent", json!(observer)),
        ("generation", json!(generation - 1)),
        ("scope", json!("workspace")),
    ] {
        let mut invalid = parsed.clone();
        invalid[field] = value;
        assert_invalid_read_rpc(
            runtime,
            fixture,
            "agent.processes",
            "resolved cursor binding",
            json!({"id":agent,"after":serde_json::to_string(&invalid).expect("mismatched cursor")}),
        );
    }
    assert_invalid_read_rpc(
        runtime,
        fixture,
        "agent.processes",
        "query scope binding",
        json!({"id":agent,"scope":"workspace","after":cursor}),
    );
    let mut overflow = parsed;
    overflow["identity"]["start_time"] = json!(u64::MAX);
    assert_invalid_read_rpc(
        runtime,
        fixture,
        "agent.processes",
        "stored birth range",
        json!({"id":agent,"generation":generation,"after":serde_json::to_string(&overflow).expect("overflow birth cursor")}),
    );
    let workspace_request = AgentProcessesRequest {
        scope: Some("workspace".to_owned()),
        ..AgentProcessesRequest::new(agent)
    };
    let raw = public_rpc(
        runtime,
        fixture,
        "agent.processes",
        serde_json::to_value(&workspace_request).expect("workspace process request"),
    )
    .unwrap_or_else(|_| panic!("exact managed target process view failed"));
    let workspace_page: AgentProcessesResponse =
        serde_json::from_value(raw.clone()).expect("typed workspace processes");
    workspace_request
        .validate_response(&workspace_page)
        .expect("matching managed target owner");
    assert_eq!(workspace_page.generation, generation);
    assert_eq!(workspace_page.scope.as_deref(), Some("workspace"));
    assert!(
        serde_json::to_value(workspace_page).expect("workspace process wire") == raw,
        "typed workspace scope was lost"
    );

    for method in ["agent.stats", "agent.git", "agent.processes", "agent.edits"] {
        let error = match public_rpc(
            runtime,
            fixture,
            method,
            json!({"id":agent,"generation":generation + 1}),
        ) {
            Err(error) => error,
            Ok(_) => panic!("{method} accepted an unadmitted future generation"),
        };
        assert_eq!(error.code, "stale_generation");
    }
    let future = match public_rpc(
        runtime,
        fixture,
        "agent.processes",
        json!({"id":agent,"scope":"workspace","generation":generation + 1}),
    ) {
        Err(error) => error,
        Ok(_) => panic!("workspace read bypassed exact preparation target"),
    };
    assert_eq!(future.code, "stale_generation");
    for method in ["agent.stats", "agent.git"] {
        let error = match public_rpc(
            runtime,
            fixture,
            method,
            json!({"id":agent,"generation":generation - 1}),
        ) {
            Err(error) => error,
            Ok(_) => panic!("{method} accepted a historical current-only query"),
        };
        assert_eq!(error.code, "stale_generation");
    }
    for method in ["agent.processes", "agent.edits"] {
        let raw = public_rpc(
            runtime,
            fixture,
            method,
            json!({"id":agent,"generation":generation - 1}),
        )
        .unwrap_or_else(|_| panic!("{method} rejected owner historical query"));
        assert_eq!(raw["generation"], generation - 1);
    }
    for after in [" ", " note.txt "] {
        let edits_request = AgentEditsRequest {
            after: Some(after.to_owned()),
            ..AgentEditsRequest::new(agent)
        };
        let wire = serde_json::to_value(&edits_request).expect("literal edit cursor");
        assert_eq!(wire["after"], after);
        let raw = public_rpc(runtime, fixture, "agent.edits", wire)
            .unwrap_or_else(|_| panic!("literal edit cursor was trimmed or normalized"));
        let reply: AgentEditsResponse = serde_json::from_value(raw.clone()).expect("typed edits");
        edits_request
            .validate_response(&reply)
            .expect("matching edit cursor result");
        assert!(reply.edits.is_empty());
        assert!(
            serde_json::to_value(reply).expect("edit wire") == raw,
            "typed edits changed null next"
        );
    }
    // Public grammar preserves the literal string; the existing Store path gate
    // still rejects it rather than normalizing it into a different cursor.
    let rejected_path = AgentEditsRequest {
        after: Some("../literal\\path".to_owned()),
        ..AgentEditsRequest::new(agent)
    };
    let wire = serde_json::to_value(rejected_path).expect("literal rejected cursor");
    assert_eq!(wire["after"], "../literal\\path");
    assert_invalid_read_rpc(
        runtime,
        fixture,
        "agent.edits",
        "unchanged Store path validation",
        wire,
    );
    let generation_argument = generation.to_string();
    for args in [
        vec!["status"],
        vec!["ls"],
        vec!["top"],
        vec!["show", observer],
        vec![
            "processes",
            agent,
            "--generation",
            &generation_argument,
            "--limit",
            "1",
        ],
        vec!["stats", observer],
        vec!["git-status", observer],
        vec!["edits", agent, "--after", " "],
    ] {
        let query = cli(&fixture.data, &args);
        assert_eq!(query.status.code(), Some(0), "{} read failed", args[0]);
        assert!(
            !query.stderr.lines().any(|line| line.starts_with("id=")),
            "{} used mutation metadata",
            args[0]
        );
        let wire = query.json();
        match args[0] {
            "status" => {
                let reply: DaemonStatusResponse = serde_json::from_value(wire).expect("CLI status");
                DaemonStatusRequest::default()
                    .validate_response(&reply)
                    .expect("CLI status binding");
            }
            "ls" | "top" => {
                let reply: AgentListResponse = serde_json::from_value(wire).expect("CLI list");
                AgentListRequest {
                    observations: args[0] == "top",
                    ..AgentListRequest::default()
                }
                .validate_response(&reply)
                .expect("CLI observation selection");
            }
            "show" => {
                let reply: AgentGetResponse = serde_json::from_value(wire).expect("CLI show");
                AgentGetRequest::new(observer)
                    .validate_response(&reply)
                    .expect("CLI show binding");
            }
            "processes" => {
                let reply: AgentProcessesResponse =
                    serde_json::from_value(wire).expect("CLI processes");
                process_request
                    .validate_response(&reply)
                    .expect("CLI process binding");
            }
            "stats" => {
                let reply: AgentStatsResponse = serde_json::from_value(wire).expect("CLI stats");
                stats_request
                    .validate_response(&reply)
                    .expect("CLI stats binding");
            }
            "git-status" => {
                let reply: AgentGitResponse = serde_json::from_value(wire).expect("CLI Git");
                git_request
                    .validate_response(&reply)
                    .expect("CLI Git binding");
            }
            "edits" => {
                let reply: AgentEditsResponse = serde_json::from_value(wire).expect("CLI edits");
                AgentEditsRequest {
                    after: Some(" ".to_owned()),
                    ..AgentEditsRequest::new(agent)
                }
                .validate_response(&reply)
                .expect("CLI edit binding");
            }
            _ => unreachable!(),
        }
    }
    let commands_after: Vec<_> = fixture
        .events()
        .into_iter()
        .filter(|event| {
            event["kind"]
                .as_str()
                .is_some_and(|kind| kind.starts_with("command."))
        })
        .collect();
    assert!(
        commands_after == commands_before,
        "view queries changed command/effect history"
    );
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
            let mut params = json!({"profile":"fake"});
            params[field] = value.clone();
            assert_invalid_read_rpc(&runtime, &fixture, "agent.discover", field, params);
        }
    }
    assert_invalid_read_rpc(
        &runtime,
        &fixture,
        "agent.discover",
        "scope byte bound",
        json!({"profile":"fake","native_scope":"é".repeat(257)}),
    );
    for field in ["native_scope", "cursor"] {
        for control in ['\0', '\n', '\u{7f}'] {
            let mut params = json!({"profile":"fake"});
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
        json!({"profile":"fake","cursor":"é".repeat(MAX_OBSERVATION_CURSOR_BYTES / 2 + 1)}),
    );
    assert_invalid_read_rpc(
        &runtime,
        &fixture,
        "agent.discover",
        "unknown field",
        json!({"profile":"fake","extra":true}),
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
            ("agent.discover", json!({"profile":"fake"})),
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
    let scope_error = match public_rpc(
        &runtime,
        &fixture,
        "agent.discover",
        json!({"profile":"fake","native_scope":"é".repeat(256)}),
    ) {
        Err(error) => error,
        Ok(_) => panic!("fake discovery accepted another scope"),
    };
    assert_eq!(
        scope_error.code, "not_found",
        "512-byte scope was rejected before fixture selection"
    );
    let raw = public_rpc(
        &runtime,
        &fixture,
        "agent.discover",
        json!({"profile":"fake","harness":null,"native_scope":null,"cursor":null}),
    )
    .unwrap_or_else(|_| panic!("null discovery optionals did not act as absent"));
    let page: DiscoverPage = serde_json::from_value(raw).expect("typed discovery page");
    assert_eq!(page.harness, "fake");
    assert_eq!(page.subjects.len(), 3);
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
fn public_result_queries_read_one_exact_fake_turn() {
    let mut fixture = Fixture::new("typed result replay");
    fixture.start();
    let launched = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
    ]);
    let agent = launched["target"].as_str().expect("fake Agent");
    let generation = launched["generation"].as_i64().expect("fake generation");
    let send_id = ulid::Ulid::new().to_string();
    let turn_value = fixture.json(&[
        "--id",
        &send_id,
        "send",
        agent,
        "--generation",
        &generation.to_string(),
        "--wait",
        "--timeout-seconds",
        "10",
        "--",
        "typed-result-echo",
    ]);
    let turn: CoreTurn = serde_json::from_value(turn_value).expect("typed waited turn");
    assert_eq!(turn.agent, agent);
    assert_eq!(turn.generation, generation);
    assert_eq!(turn.state, TurnState::Completed);
    assert_eq!(turn.final_text.as_deref(), Some("typed-result-echo"));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("public RPC runtime");
    let command_request = CommandGetRequest::new(&send_id);
    let command: CoreCommand = serde_json::from_value(
        public_rpc(
            &runtime,
            &fixture,
            "command.get",
            serde_json::to_value(&command_request).expect("command request"),
        )
        .unwrap_or_else(|_| panic!("exact command query failed")),
    )
    .expect("typed command");
    command_request
        .validate_response(&command)
        .expect("matching command response");
    let literal_id = match public_rpc(
        &runtime,
        &fixture,
        "command.get",
        json!({"id":format!(" {send_id} ")}),
    ) {
        Err(error) => error,
        Ok(_) => panic!("read selector was normalized to another literal identifier"),
    };
    assert_eq!(literal_id.code, "not_found");
    assert_eq!(command.state, CommandState::Applied);
    assert_eq!(command.target.as_deref(), Some(agent));
    assert_eq!(command.generation, Some(generation));
    assert_eq!(command.turn_id.as_deref(), Some(turn.turn_id.as_str()));
    let turn_request = TurnGetRequest::new(agent, generation, &turn.turn_id);
    let queried: CoreTurn = serde_json::from_value(
        public_rpc(
            &runtime,
            &fixture,
            "turn.get",
            serde_json::to_value(&turn_request).expect("turn request"),
        )
        .unwrap_or_else(|_| panic!("exact turn query failed")),
    )
    .expect("typed turn");
    turn_request
        .validate_response(&queried)
        .expect("matching turn response");
    assert_eq!(queried, turn);
    let mut request = AgentResultRequest::new(agent);
    assert_eq!(request.limit, 100);
    let params = serde_json::to_value(&request).expect("result request");
    assert!(params.get("generation").is_none());
    assert!(params.get("turn_id").is_none());
    let result: AgentResultResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "agent.result", params)
            .unwrap_or_else(|_| panic!("current result query failed")),
    )
    .expect("typed current result");
    request
        .validate_response(&result)
        .expect("matching current result");
    assert_eq!(result.generation, generation);
    assert_eq!(result.turns, vec![turn.clone()]);
    request.generation = Some(generation);
    request.turn_id = Some(turn.turn_id.clone());
    request.limit = 1;
    let result: AgentResultResponse = serde_json::from_value(
        public_rpc(
            &runtime,
            &fixture,
            "agent.result",
            serde_json::to_value(&request).expect("exact result request"),
        )
        .unwrap_or_else(|_| panic!("exact result query failed")),
    )
    .expect("typed exact result");
    request
        .validate_response(&result)
        .expect("matching exact result");
    assert_eq!(result.turns, vec![turn.clone()]);
    for limit in [
        Value::Null,
        json!(0),
        json!(1001),
        json!("1"),
        json!(true),
        json!(1.0),
    ] {
        assert_invalid_read_rpc(
            &runtime,
            &fixture,
            "agent.result",
            "known exact turn limit",
            json!({"id":agent,"generation":generation,"turn_id":turn.turn_id,"limit":limit}),
        );
    }
    let query = cli(&fixture.data, &["cmd", "show", &send_id]);
    assert_eq!(query.status.code(), Some(0));
    assert!(
        !query.stderr.lines().any(|line| line.starts_with("id=")),
        "read query used mutation metadata"
    );
    let cli_command: CoreCommand = serde_json::from_value(query.json()).expect("typed CLI command");
    command_request
        .validate_response(&cli_command)
        .expect("matching CLI command");
    let cli_result: AgentResultResponse = serde_json::from_value(fixture.json(&[
        "result",
        agent,
        "--generation",
        &generation.to_string(),
        "--turn-id",
        &turn.turn_id,
        "--limit",
        "1",
    ]))
    .expect("typed CLI exact result");
    request
        .validate_response(&cli_result)
        .expect("matching CLI result");
    assert_eq!(cli_result.turns, vec![turn]);
    let mut submission = CommandSubmitRequest::new(&send_id, "send");
    submission.target = Some(agent.to_owned());
    submission.expected_generation = Some(generation);
    submission.args = command.args.clone();
    for _ in 0..2 {
        let retained = retained_submission(&fixture, &runtime, &submission);
        assert_eq!(retained.state, CommandState::Applied);
        assert_eq!(retained.generation, command.generation);
        assert_eq!(retained.outcome, command.outcome);
        assert!(retained.linked_command.is_none());
    }
    let lowercase_id = send_id.to_ascii_lowercase();
    let repeated = cli(
        &fixture.data,
        &[
            "--id",
            &lowercase_id,
            "send",
            agent,
            "--generation",
            &generation.to_string(),
            "--",
            "typed-result-echo",
        ],
    );
    assert_eq!(repeated.status.code(), Some(0));
    assert!(
        repeated
            .stderr
            .lines()
            .any(|line| line == format!("id={send_id}")),
        "CLI did not report its normalized explicit mutation ID"
    );
    let retained: CommandSubmitResponse =
        serde_json::from_value(repeated.json()).expect("typed CLI retained submit");
    submission
        .validate_response(&retained)
        .expect("matching normalized CLI submission");
    assert_eq!(retained.command_id, send_id);
    assert_eq!(retained.outcome, command.outcome);
    let raw_case = CommandSubmitRequest::new(&lowercase_id, "send");
    assert_eq!(
        serde_json::to_value(raw_case).expect("raw mutation ID wire")["id"],
        lowercase_id,
        "RPC grammar normalized an explicit ID"
    );
    let mut changed = submission;
    changed.args = json!({"text":"must-not-be-dispatched"});
    let conflict = match public_rpc(
        &runtime,
        &fixture,
        "command.submit",
        serde_json::to_value(changed).expect("conflicting submit"),
    ) {
        Err(error) => error,
        Ok(_) => panic!("same mutation ID accepted another input"),
    };
    assert_eq!(conflict.code, "id_conflict");
    assert_eq!(
        fault_dispatches(&fixture.events(), &send_id),
        1,
        "read queries resubmitted input"
    );
    let failed_id = ulid::Ulid::new().to_string();
    let failed = cli(
        &fixture.data,
        &[
            "--id",
            &failed_id,
            "send",
            agent,
            "--generation",
            &generation.to_string(),
            "--wait",
            "--timeout-seconds",
            "10",
            "--",
            "[fake:native-turn-error]",
        ],
    );
    assert_eq!(failed.status.code(), Some(1));
    let failed_turn: CoreTurn = serde_json::from_str(&failed.stdout).expect("failed terminal turn");
    assert_eq!(failed_turn.agent, agent);
    assert_eq!(failed_turn.generation, generation);
    assert_eq!(failed_turn.state, TurnState::Completed);
    assert_eq!(failed_turn.status.as_deref(), Some("failed"));
    assert!(failed_turn.final_text.is_none());
    let failed_result: AgentResultResponse = serde_json::from_value(fixture.json(&[
        "result",
        agent,
        "--generation",
        &generation.to_string(),
        "--turn-id",
        &failed_turn.turn_id,
        "--limit",
        "1",
    ]))
    .expect("failed turn result read");
    assert_eq!(failed_result.turns, vec![failed_turn.clone()]);
    let applied = fixture.wait_command(&failed_id, "applied");
    assert_eq!(applied["turn_id"], failed_turn.turn_id);
    let events = fixture.events();
    assert_eq!(fault_dispatches(&events, &failed_id), 1);
    let terminal: Vec<_> = events
        .iter()
        .filter(|event| {
            event["agent"] == agent
                && event["generation"] == generation
                && event["kind"] == "turn.completed"
                && event["data"]["turn_id"] == failed_turn.turn_id
        })
        .collect();
    assert_eq!(terminal.len(), 1);
    assert_eq!(terminal[0]["data"]["command_id"], failed_id);
    assert_eq!(terminal[0]["data"]["status"], "failed");
    let diagnostic = &terminal[0]["data"]["native_error"];
    assert_eq!(
        diagnostic,
        &json!({
            "message":"fake native request failed (HTTP 401)", "truncated":false
        })
    );
    assert!(serde_json::to_vec(diagnostic).unwrap().len() <= 4 * 1024);
    let stop = fixture.json(&["stop", agent, "--generation", &generation.to_string()]);
    fixture.wait_command(
        stop["command_id"].as_str().expect("stop command"),
        "applied",
    );
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
fn public_event_replay_has_typed_pages_and_accepts_clamped_wait() {
    let mut fixture = Fixture::new("typed event replay");
    fixture.start();
    let launched = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
    ]);
    let agent = launched["target"].as_str().expect("fake Agent");
    let stopped = fixture.json(&["stop", agent]);
    fixture.wait_command(
        stopped["command_id"].as_str().expect("stop command"),
        "applied",
    );
    // Reopen stopped history with no live publishers during replay assertions.
    fixture.stop(false);
    fixture.start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("public RPC runtime");
    let raw = public_rpc(&runtime, &fixture, "events.read", json!({}))
        .unwrap_or_else(|_| panic!("default persisted replay failed"));
    let replay: EventsReadResponse = serde_json::from_value(raw).expect("typed persisted events");
    assert!(replay.events.len() > 1);
    assert!(
        replay
            .events
            .windows(2)
            .all(|events| events[0].seq < events[1].seq)
    );
    assert_eq!(replay.next, replay.high_water);
    assert!(!replay.truncated);
    let raw = public_rpc(
        &runtime,
        &fixture,
        "events.read",
        json!({"after": 0, "limit": 1}),
    )
    .unwrap_or_else(|_| panic!("bounded replay failed"));
    let first: EventsReadResponse = serde_json::from_value(raw).expect("typed first page");
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.events[0], replay.events[0]);
    assert_eq!(first.next, first.events[0].seq);
    assert_eq!(first.high_water, replay.high_water);
    assert!(first.truncated);
    let request = EventsSubscribeRequest {
        after: 0,
        limit: 1,
        wait_ms: u64::MAX,
        filter: EventsFilter::default(),
    };
    let params = serde_json::to_value(request).expect("request wire");
    let decoded: EventsSubscribeRequest =
        serde_json::from_value(params.clone()).expect("clamped request");
    assert_eq!(decoded.wait_ms, 30_000);
    let raw = public_rpc(&runtime, &fixture, "events.subscribe", params)
        .unwrap_or_else(|_| panic!("oversized wait was rejected instead of clamped"));
    let subscribed: EventsSubscribeResponse =
        serde_json::from_value(raw).expect("typed available subscribe page");
    assert_eq!(subscribed.events, first.events);
    assert_eq!(subscribed.next, first.next);
    assert_eq!(subscribed.high_water, replay.high_water);
    assert!(subscribed.truncated);
    assert!(!subscribed.lagged);
    for field in ["agents", "kinds", "sources"] {
        let mut filter = json!({});
        filter[field] = json!(vec![" "; 1000]);
        let raw = public_rpc(&runtime, &fixture, "events.read", json!({"filter": filter}))
            .unwrap_or_else(|_| panic!("nonempty raw filter entries were tightened"));
        let empty: EventsReadResponse = serde_json::from_value(raw).expect("typed filtered replay");
        assert!(empty.events.is_empty());
        assert_eq!(empty.next, replay.high_water);
        assert_eq!(empty.high_water, replay.high_water);
        assert!(!empty.truncated);
    }
    let raw = public_rpc(
        &runtime,
        &fixture,
        "events.subscribe",
        json!({"filter": {"agents": ["a01"]}}),
    )
    .unwrap_or_else(|_| panic!("noncanonical Agent filter was tightened"));
    let empty: EventsSubscribeResponse =
        serde_json::from_value(raw).expect("typed nonmatching filter");
    assert!(empty.events.is_empty());
    assert_eq!(empty.next, replay.high_water);
    assert_eq!(empty.high_water, replay.high_water);
    assert!(!empty.lagged);
    fixture.stop(false);
}

#[test]
fn malformed_after_dispatch_remains_unknown_across_restart() {
    let mut fixture = Fixture::new("transport fault");
    fixture.start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("retained submission RPC runtime");
    let launched = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
    ]);
    assert_eq!(launched["state"], "applied");
    let agent = launched["target"].as_str().unwrap();
    let generation = launched["generation"].as_i64().unwrap().to_string();
    let handoff_id = ulid::Ulid::new().to_string();
    let handoff_turn = send_fake_handoff(
        &fixture,
        agent,
        generation.parse().expect("captured generation"),
        &handoff_id,
    );
    let fault_id = ulid::Ulid::new().to_string();
    let fault = cli(
        &fixture.data,
        &[
            "--id",
            &fault_id,
            "send",
            "--generation",
            &generation,
            "--wait",
            "--timeout-seconds",
            "10",
            agent,
            "--",
            "[fake:malformed-after-dispatch]",
        ],
    );
    let query = cli(&fixture.data, &["cmd", "show", &fault_id]);
    assert_eq!(
        query.status.code(),
        Some(0),
        "successful Unknown read must exit zero"
    );
    assert!(
        !query.stderr.lines().any(|line| line.starts_with("id=")),
        "read query used mutation metadata"
    );
    let mut unknown = query.json();
    let typed_unknown: CoreCommand =
        serde_json::from_value(unknown.clone()).expect("typed Unknown command");
    CommandGetRequest::new(&fault_id)
        .validate_response(&typed_unknown)
        .expect("matching Unknown read");
    assert_eq!(typed_unknown.state, CommandState::Unknown);
    assert_eq!(
        unknown["state"],
        "unknown",
        "send exit {:?}; private diagnostics deliberately omitted",
        fault.status.code()
    );
    assert_eq!(fault.status.code(), Some(4), "unknown must have CLI exit 4");
    assert!(unknown["turn_id"].is_null());
    let diagnostic_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !has_masked_plugin_line(&unknown["outcome"]) {
        assert!(
            std::time::Instant::now() < diagnostic_deadline,
            "Unknown outcome omitted its sanitized Plugin stderr line"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
        unknown = fixture.json(&["cmd", "show", &fault_id]);
        assert_eq!(unknown["state"], "unknown");
    }
    let owner_token = fs::read_to_string(fixture.data.join("owner.token")).unwrap();
    let outcome_text = serde_json::to_string(&unknown["outcome"]).unwrap();
    assert!(
        !outcome_text.contains(&owner_token),
        "owner credential entered the outcome"
    );
    let log_deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let log = fs::read_to_string(fixture.data.join("logs/daemon.log")).unwrap();
        assert!(log.len() <= 1024 * 1024, "daemon log exceeded its bound");
        assert!(
            !log.contains(&owner_token),
            "owner credential entered the daemon log"
        );
        if log.lines().any(|line| {
            serde_json::from_str::<Value>(line).is_ok_and(|record| has_masked_plugin_line(&record))
        }) {
            break;
        }
        assert!(
            std::time::Instant::now() < log_deadline,
            "daemon log omitted the sanitized Plugin stderr line"
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let mut retry_request = CommandSubmitRequest::new(&fault_id, "send");
    retry_request.target = Some(agent.to_owned());
    retry_request.expected_generation = Some(generation.parse().expect("captured generation"));
    retry_request.args = json!({"text":"[fake:malformed-after-dispatch]"});
    let retry = retained_submission(&fixture, &runtime, &retry_request);
    assert_eq!(retry.state, CommandState::Unknown);
    assert_eq!(retry.outcome.as_ref(), Some(&unknown["outcome"]));
    assert!(has_masked_plugin_line(
        retry.outcome.as_ref().expect("retained diagnostics")
    ));
    let blocked_before = assert_input_blocked(&fixture, agent, &generation);
    let dispatches_before = fault_dispatches(&fixture.events(), &fault_id);
    assert_eq!(
        dispatches_before, 1,
        "the malformed reply must follow one actual dispatch"
    );

    fixture.stop(true);
    fixture.start();
    let retained = fixture.json(&["cmd", "show", &fault_id]);
    assert_eq!(retained["state"], "unknown");
    assert_eq!(retained["target"], agent);
    assert_eq!(retained["generation"], unknown["generation"]);
    assert!(retained["turn_id"].is_null());
    assert!(
        has_masked_plugin_line(&retained["outcome"]),
        "restart lost the sanitized diagnostic"
    );
    let retry = retained_submission(&fixture, &runtime, &retry_request);
    assert_eq!(retry.state, CommandState::Unknown);
    assert_eq!(retry.outcome.as_ref(), Some(&retained["outcome"]));
    let blocked_after = assert_input_blocked(&fixture, agent, &generation);
    let status = fixture.json(&["status"]);
    assert!(
        status["unsettled_commands"]
            .as_array()
            .unwrap()
            .iter()
            .any(|command| command["id"] == fault_id && command["state"] == "unknown")
    );
    let events = fixture.events();
    assert_eq!(
        fault_dispatches(&events, &fault_id),
        dispatches_before,
        "restart replayed dispatched input"
    );
    for id in [&blocked_before, &blocked_after] {
        assert_eq!(
            fault_dispatches(&events, id),
            0,
            "unknown gate dispatched later input"
        );
    }
    assert!(!events.iter().any(|event| event["kind"] == "turn.started" && event["data"]["command_id"] == fault_id));
    assert!(
        events.iter().any(|event| event["agent"] == agent
            && event["generation"] == handoff_turn.generation
            && event["kind"] == "source.gap"
            && event["data"]["source"] == "transcript"),
        "transport loss must record the uncovered tail on its current owner"
    );
    assert_eq!(fault_dispatches(&events, &handoff_id), 1);
    assert_eq!(
        fixture.json(&["result", agent, "--generation", &generation])["turns"],
        json!([handoff_turn]),
        "uncertain input must not create another turn or replay the handoff turn"
    );
    fixture.stop(true);
}

fn has_masked_plugin_line(value: &Value) -> bool {
    value["diagnostics"].as_array().is_some_and(|diagnostics| {
        diagnostics.iter().any(|diagnostic| {
            diagnostic["source"] == "plugin"
                && diagnostic["snapshot"]["text"].as_str().is_some_and(|text| {
                    text.lines().any(|line| {
                        line.strip_prefix("fake_stderr_before_malformed=") == Some("[REDACTED]")
                    })
                })
        })
    })
}

fn assert_input_blocked(fixture: &Fixture, agent: &str, generation: &str) -> String {
    let id = ulid::Ulid::new().to_string();
    let reply = cli(
        &fixture.data,
        &[
            "--id",
            &id,
            "send",
            "--generation",
            generation,
            agent,
            "--",
            "must not be dispatched",
        ],
    );
    if reply.status.success() {
        let queued = fixture.json(&["cmd", "show", &id]);
        assert!(
            matches!(queued["state"].as_str(), Some("accepted" | "queued")),
            "later input escaped the unknown gate: {queued}"
        );
        assert!(queued["turn_id"].is_null());
    } else {
        assert_eq!(
            reply.status.code(),
            Some(3),
            "gate rejection must be definite: {}",
            reply.stderr
        );
    }
    id
}

fn fault_dispatches(events: &[Value], id: &str) -> usize {
    events
        .iter()
        .filter(|event| event["kind"] == "command.dispatched" && event["data"]["command_id"] == id)
        .count()
}

fn send_fake_handoff(fixture: &Fixture, agent: &str, generation: i64, id: &str) -> CoreTurn {
    let turn: CoreTurn = serde_json::from_value(fixture.json(&[
        "--id",
        id,
        "send",
        agent,
        "--generation",
        &generation.to_string(),
        "--wait",
        "--timeout-seconds",
        "10",
        "--",
        "[fake:source-handoff]",
    ]))
    .expect("typed handoff input turn");
    assert_eq!(turn.agent, agent);
    assert_eq!(turn.generation, generation);
    assert_eq!(turn.state, TurnState::Completed);
    assert_eq!(turn.status.as_deref(), Some("completed"));
    assert_eq!(turn.final_text.as_deref(), Some("[fake:source-handoff]"));
    turn
}

fn assert_fake_source_handoff(
    fixture: &Fixture,
    runtime: &tokio::runtime::Runtime,
    agent: &str,
    generation: i64,
) {
    let before: AgentGetResponse =
        serde_json::from_value(fixture.json(&["show", agent])).expect("before handoff view");
    let families = ["message", "session", "turn"];
    let unrelated_before: HashSet<_> = before
        .source_states
        .iter()
        .flat_map(|source| &source.owners)
        .filter(|owner| !families.contains(&owner.family.as_str()))
        .map(|owner| {
            (
                owner.family.clone(),
                owner.source.clone(),
                owner.mechanism.clone(),
            )
        })
        .collect();
    assert!(
        !unrelated_before.is_empty(),
        "fixture must retain unrelated source ownership"
    );

    let marker_id = ulid::Ulid::new().to_string();
    let marker_turn = send_fake_handoff(fixture, agent, generation, &marker_id);
    let mut retry = CommandSubmitRequest::new(&marker_id, "send");
    retry.target = Some(agent.to_owned());
    retry.expected_generation = Some(generation);
    retry.args = json!({"text":"[fake:source-handoff]"});
    let retained = retained_submission(fixture, runtime, &retry);
    assert_eq!(retained.state, CommandState::Applied);
    let command = fixture.json(&["cmd", "show", &marker_id]);
    assert_eq!(command["turn_id"], marker_turn.turn_id);
    assert_eq!(
        retained.outcome,
        command
            .get("outcome")
            .filter(|value| !value.is_null())
            .cloned()
    );

    let next_id = ulid::Ulid::new().to_string();
    let next_turn: CoreTurn = serde_json::from_value(fixture.json(&[
        "--id",
        &next_id,
        "send",
        agent,
        "--generation",
        &generation.to_string(),
        "--wait",
        "--timeout-seconds",
        "10",
        "--",
        "after-source-handoff",
    ]))
    .expect("typed successor input turn");
    assert_eq!(next_turn.state, TurnState::Completed);
    assert_eq!(next_turn.status.as_deref(), Some("completed"));
    assert_eq!(
        next_turn.final_text.as_deref(),
        Some("after-source-handoff")
    );
    assert_ne!(marker_turn.turn_id, next_turn.turn_id);

    let raw = fixture.json(&["show", agent]);
    let view: AgentGetResponse = serde_json::from_value(raw.clone()).expect("after handoff view");
    assert_eq!(
        serde_json::to_value(&view.source_states).unwrap(),
        raw["source_states"],
        "public source states must retain their existing shape without private ACK fields"
    );
    let predecessor = view
        .source_states
        .iter()
        .find(|source| source.source == "stream")
        .unwrap();
    assert_eq!(predecessor.state, "open");
    assert!(predecessor.families.is_empty());
    assert!(predecessor.owners.is_empty());
    let successor = view
        .source_states
        .iter()
        .find(|source| source.source == "transcript")
        .unwrap();
    assert_eq!(successor.state, "open");
    assert_eq!(successor.families, families);
    assert_eq!(successor.owners.len(), families.len());
    for owner in &successor.owners {
        assert!(families.contains(&owner.family.as_str()));
        assert_eq!(owner.source, "transcript");
        assert_eq!(owner.mechanism, "fake_recovery");
    }
    let unrelated_after: HashSet<_> = view
        .source_states
        .iter()
        .flat_map(|source| &source.owners)
        .filter(|owner| !families.contains(&owner.family.as_str()))
        .map(|owner| {
            (
                owner.family.clone(),
                owner.source.clone(),
                owner.mechanism.clone(),
            )
        })
        .collect();
    // Independent monitors may register another family while these CLI calls
    // run; the handoff must preserve every unrelated binding already present.
    assert!(unrelated_before.is_subset(&unrelated_after));

    let all_events = fixture.events();
    let events: Vec<_> = all_events
        .iter()
        .filter(|event| event["agent"] == agent && event["generation"] == generation)
        .collect();
    let handoffs: Vec<_> = events
        .iter()
        .filter(|event| event["kind"] == "source.handoff")
        .collect();
    assert_eq!(handoffs.len(), 1);
    let handoff = handoffs[0];
    assert_eq!(handoff["data"]["source"], "stream");
    assert_eq!(handoff["data"]["successor"], "transcript");
    assert_eq!(handoff["data"]["mechanism"], "fake_recovery");
    assert_eq!(handoff["data"]["families"], json!(families));
    assert!(handoff["data"]["cursor"].is_null());
    let gaps: Vec<_> = events
        .iter()
        .filter(|event| {
            event["kind"] == "source.gap"
                && event["data"]["gap_id"] == handoff["data"]["gap"]["gap_id"]
        })
        .collect();
    assert_eq!(gaps.len(), 1);
    let gap = gaps[0];
    assert_eq!(gap["data"]["source"], "stream");
    assert_eq!(gap["data"]["families"], json!(families));
    for reason in [&gap["data"]["reason"], &handoff["data"]["gap"]["reason"]] {
        assert!(
            reason.as_str() == Some("fake_source_handoff_fixture [REDACTED]"),
            "handoff gap reason was not sanitized; actual text deliberately omitted"
        );
    }
    assert_eq!(
        predecessor.gap_id.as_deref(),
        gap["data"]["gap_id"].as_str()
    );
    assert_eq!(predecessor.gap_seq, gap["seq"].as_i64());
    assert!(gap["seq"].as_i64().unwrap() < handoff["seq"].as_i64().unwrap());

    for (id, turn) in [(&marker_id, &marker_turn), (&next_id, &next_turn)] {
        assert_eq!(fault_dispatches(&all_events, id), 1);
        let facts: Vec<_> = events
            .iter()
            .filter(|event| {
                event["data"]["turn_id"] == turn.turn_id
                    && matches!(
                        event["kind"].as_str(),
                        Some("turn.started" | "message.completed" | "turn.completed")
                    )
            })
            .collect();
        assert_eq!(
            facts
                .iter()
                .map(|event| event["kind"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["turn.started", "message.completed", "turn.completed"]
        );
        assert!(facts.iter().all(|event| event["source"] == "transcript"
            && event["native_id"].is_string()
            && event["seq"].as_i64().unwrap() > handoff["seq"].as_i64().unwrap()));
        let result = fixture.json(&[
            "result",
            agent,
            "--generation",
            &generation.to_string(),
            "--turn-id",
            &turn.turn_id,
            "--limit",
            "1",
        ]);
        assert_eq!(result["turns"], json!([turn]));
    }
}

#[test]
fn fake_discovery_observation_and_worktree_lifecycle() {
    let mut fixture = Fixture::new("portable lifecycle");
    assert!(
        fixture
            .git(&["init", "--initial-branch=main"])
            .status
            .success()
    );
    fs::write(fixture.workspace.join("note.txt"), "baseline\n").unwrap();
    assert!(fixture.git(&["add", "--", "note.txt"]).status.success());
    assert!(
        fixture
            .git(&["commit", "-m", "Local worktree smoke fixture"])
            .status
            .success()
    );
    let head = fixture.git(&["rev-parse", "--verify", "HEAD"]);
    assert!(head.status.success());
    let base = head.stdout.trim();
    fixture.start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("public metadata RPC runtime");
    let discovery_before: EventsReadResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "events.read", json!({}))
            .unwrap_or_else(|_| panic!("discovery baseline event read failed")),
    )
    .expect("typed discovery boundary");
    let status = fixture.json(&["status"]);
    let custody = &status["workspace_custody"];
    assert_eq!(custody["scope"], "current_host");
    assert_eq!(
        custody["capability"],
        if cfg!(windows) {
            "windows_job_exact"
        } else {
            "unix_process_group_degraded"
        }
    );
    assert_eq!(custody["exact"], cfg!(windows));
    assert_eq!(custody["orphan_possible"], !cfg!(windows));

    let first = fixture.json(&["discover", "--profile", "fake", "--limit", "1"]);
    let typed_first: DiscoverPage =
        serde_json::from_value(first.clone()).expect("typed CLI discovery page");
    assert_eq!(first["harness"], "fake");
    assert_eq!(first["subjects"].as_array().unwrap().len(), 1);
    assert_eq!(first["coverage"]["complete"], false);
    let scope = first["native_scope"].as_str().unwrap();
    let cursor = first["next_cursor"].as_str().unwrap();
    let rest = fixture.json(&[
        "discover",
        "--profile",
        "fake",
        "--scope",
        scope,
        "--cursor",
        cursor,
        "--limit",
        "2",
    ]);
    assert_eq!(rest["subjects"].as_array().unwrap().len(), 2);
    assert!(rest["next_cursor"].is_null());
    assert_eq!(rest["coverage"]["complete"], true);
    let discovery_request = AgentDiscoverRequest {
        profile: "fake".to_owned(),
        harness: None,
        native_scope: None,
        cursor: None,
        limit: 1,
    };
    let wire = serde_json::to_value(&discovery_request).expect("discovery request wire");
    for field in ["harness", "native_scope", "cursor"] {
        assert!(wire.get(field).is_none());
    }
    let null_page: DiscoverPage = serde_json::from_value(
        public_rpc(
            &runtime,
            &fixture,
            "agent.discover",
            json!({
                "profile":"fake","harness":null,"native_scope":null,"cursor":null,"limit":1,
            }),
        )
        .unwrap_or_else(|_| panic!("null discovery optionals did not act as absent")),
    )
    .expect("typed null-option discovery page");
    assert_eq!(
        serde_json::to_value(null_page).expect("neutral page"),
        first
    );
    let continuation = AgentDiscoverRequest {
        native_scope: Some(typed_first.native_scope),
        cursor: typed_first.next_cursor,
        limit: 2,
        ..discovery_request
    };
    let replay: DiscoverPage = serde_json::from_value(
        public_rpc(
            &runtime,
            &fixture,
            "agent.discover",
            serde_json::to_value(continuation).expect("opaque continuation"),
        )
        .unwrap_or_else(|_| panic!("unchanged discovery cursor failed")),
    )
    .expect("typed continued discovery page");
    assert_eq!(
        serde_json::to_value(replay).expect("neutral continued page"),
        rest
    );
    let references: HashSet<_> = [
        first["subjects"].as_array().unwrap(),
        rest["subjects"].as_array().unwrap(),
    ]
    .into_iter()
    .flatten()
    .map(|subject| subject["native_ref"].as_str().unwrap())
    .collect();
    assert_eq!(references.len(), 3);
    let status = fixture.json(&["status"]);
    for field in ["agents", "owned_plugins", "discovery_readers"] {
        assert_eq!(status[field], 0);
    }
    let discovery_after: EventsReadResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "events.read", json!({}))
            .unwrap_or_else(|_| panic!("discovery event readback failed")),
    )
    .expect("typed discovery readback boundary");
    assert_eq!(discovery_after.high_water, discovery_before.high_water);

    let attached = fixture.json(&[
        "attach",
        "--profile",
        "fake",
        "--harness",
        "fake",
        "--scope",
        scope,
        "--ref",
        "fake-discovered-1",
    ]);
    let observer = attached["agent"].as_str().unwrap();
    let attach_command = fixture.wait_command(attached["command_id"].as_str().unwrap(), "applied");
    let view = fixture.json(&["show", observer]);
    assert_eq!(view["state"], "unknown");
    assert_eq!(view["origin"], "external");
    assert_eq!(view["observe_only"], true);
    assert_eq!(view["own_identity"], false);
    assert!(view["active_turn"].is_null());
    let observed_generation = view["generation"].as_i64().unwrap();
    let generation_argument = observed_generation.to_string();
    let history_before: EventsReadResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "events.read", json!({}))
            .unwrap_or_else(|_| panic!("private history baseline failed")),
    )
    .expect("typed private-history boundary");
    let first_history = fixture.json(&[
        "history",
        observer,
        "--generation",
        &generation_argument,
        "--view",
        "turns",
        "--limit",
        "1",
    ]);
    let rest_history = fixture.json(&[
        "history",
        observer,
        "--generation",
        &generation_argument,
        "--source",
        "native",
        "--view",
        "turns",
        "--cursor",
        first_history["next_cursor"].as_str().unwrap(),
        "--limit",
        "1",
    ]);
    let turns: HashSet<_> = [
        first_history["entries"].as_array().unwrap(),
        rest_history["entries"].as_array().unwrap(),
    ]
    .into_iter()
    .flatten()
    .map(|turn| turn["turn_id"].as_str().unwrap())
    .collect();
    assert_eq!(turns.len(), 2);
    assert!(rest_history["next_cursor"].is_null());
    let typed_history: ObservationPage =
        serde_json::from_value(first_history.clone()).expect("typed CLI private page");
    let null_history: ObservationPage = serde_json::from_value(
        public_rpc(
            &runtime,
            &fixture,
            "agent.history",
            json!({
                "id":observer,"generation":null,"source":"native","view":"turns",
                "turn_id":null,"cursor":null,"limit":1,
            }),
        )
        .unwrap_or_else(|_| panic!("null history optionals did not act as absent")),
    )
    .expect("typed null-option private page");
    assert_eq!(
        serde_json::to_value(null_history).expect("neutral private page"),
        first_history
    );
    let history_request = AgentHistoryRequest {
        id: observer.to_owned(),
        generation: None,
        source: "native".to_owned(),
        view: "turns".to_owned(),
        turn_id: None,
        cursor: None,
        limit: 1,
    };
    let wire = serde_json::to_value(&history_request).expect("history request wire");
    for field in ["generation", "turn_id", "cursor"] {
        assert!(wire.get(field).is_none());
    }
    assert_eq!(wire["source"], "native");
    let continuation = AgentHistoryRequest {
        generation: Some(observed_generation),
        cursor: typed_history.next_cursor,
        ..history_request
    };
    let private_rest: ObservationPage = serde_json::from_value(
        public_rpc(
            &runtime,
            &fixture,
            "agent.history",
            serde_json::to_value(continuation).expect("opaque private cursor"),
        )
        .unwrap_or_else(|_| panic!("unchanged history cursor failed")),
    )
    .expect("typed continued private page");
    assert_eq!(
        serde_json::to_value(private_rest).expect("neutral continued private page"),
        rest_history
    );
    for (source, view, turn_id) in [
        ("native", "subject", None),
        ("rollout", "items", Some("fake-observed-turn-1")),
    ] {
        let request = AgentHistoryRequest {
            id: observer.to_owned(),
            generation: Some(observed_generation),
            source: source.to_owned(),
            view: view.to_owned(),
            turn_id: turn_id.map(str::to_owned),
            cursor: None,
            limit: 1,
        };
        let page: ObservationPage = serde_json::from_value(
            public_rpc(
                &runtime,
                &fixture,
                "agent.history",
                serde_json::to_value(request).expect("private query"),
            )
            .unwrap_or_else(|_| panic!("valid subject/items private query failed")),
        )
        .expect("typed subject/items private page");
        assert_eq!(page.agent_id, observer);
        assert_eq!(page.generation, observed_generation);
        assert_eq!(page.source, source);
        assert_eq!(page.view, view);
        assert_eq!(page.entries.len(), 1);
    }
    let stale = match public_rpc(
        &runtime,
        &fixture,
        "agent.history",
        json!({
            "id":observer,"generation":observed_generation + 1,"source":"native","view":"turns",
        }),
    ) {
        Err(error) => error,
        Ok(_) => panic!("history crossed the current observer generation"),
    };
    assert_eq!(stale.code, "stale_generation");
    let history_after: EventsReadResponse = serde_json::from_value(
        public_rpc(&runtime, &fixture, "events.read", json!({}))
            .unwrap_or_else(|_| panic!("private history event readback failed")),
    )
    .expect("typed private-history readback boundary");
    assert_eq!(
        history_after.high_water, history_before.high_water,
        "private pages published durable events"
    );
    assert_eq!(
        fixture.json(&["cmd", "show", attached["command_id"].as_str().unwrap()]),
        attach_command,
        "private pages changed command acknowledgement"
    );
    assert_eq!(fixture.json(&["status"])["agents"], 1);
    assert_eq!(fixture.json(&["status"])["owned_plugins"], 0);
    assert!(
        fixture.json(&[
            "result",
            observer,
            "--generation",
            &generation_argument,
            "--limit",
            "1"
        ])["turns"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        !cli(
            &fixture.data,
            &["send", observer, "forbidden-observe-input"]
        )
        .status
        .success()
    );
    let detach = fixture.json(&["detach", observer]);
    let detached = fixture.wait_command(detach["command_id"].as_str().unwrap(), "applied");
    assert_eq!(detached["outcome"]["generation"], observed_generation + 1);
    let view = fixture.json(&["show", observer]);
    assert_eq!(view["generation"], observed_generation + 1);
    assert_eq!(view["state"], "unknown");
    assert_eq!(view["observe_only"], true);
    assert_eq!(view["own_identity"], false);
    let historical_request = AgentResultRequest {
        id: observer.to_owned(),
        generation: Some(observed_generation),
        turn_id: None,
        limit: 1,
    };
    let historical: AgentResultResponse = serde_json::from_value(fixture.json(&[
        "result",
        observer,
        "--generation",
        &generation_argument,
        "--limit",
        "1",
    ]))
    .expect("typed historical observer result");
    historical_request
        .validate_response(&historical)
        .expect("historical result permits current Agent metadata");
    assert_eq!(historical.generation, observed_generation);
    assert_eq!(
        historical.agent_metadata.generation,
        observed_generation + 1
    );
    assert!(historical.turns.is_empty());
    assert!(
        fixture
            .events()
            .iter()
            .any(|event| event["kind"] == "source.closed"
                && event["agent"] == observer
                && event["generation"] == observed_generation
                && event["data"]["source"] == "observe")
    );
    assert_eq!(fixture.json(&["status"])["observer_readers"], 0);

    let turn = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
        "--worktree",
        "codex/ci-smoke",
        "--base",
        base,
        "--timeout-seconds",
        "20",
        "--wait",
        "--",
        "ci-worktree-echo",
    ]);
    assert_eq!(turn["state"], "completed");
    assert_eq!(turn["status"], "completed");
    assert_eq!(turn["final_text"], "ci-worktree-echo");
    let agent = turn["agent"].as_str().unwrap();
    let generation = turn["generation"].as_i64().unwrap();
    assert!(generation > 0);
    let generation_argument = generation.to_string();
    let result = fixture.json(&[
        "result",
        agent,
        "--generation",
        &generation_argument,
        "--turn-id",
        turn["turn_id"].as_str().unwrap(),
        "--limit",
        "1",
    ]);
    assert_eq!(result["turns"].as_array().unwrap().len(), 1);
    assert_eq!(result["turns"][0]["state"], "completed");
    assert_eq!(result["turns"][0]["final_text"], "ci-worktree-echo");
    let view = fixture.json(&["show", agent]);
    assert_eq!(view["workspace_phase"], "ready");
    assert_eq!(view["branch"], "codex/ci-smoke");
    let worktree = Path::new(view["workspace"].as_str().unwrap())
        .canonicalize()
        .unwrap();
    assert!(worktree.starts_with(fixture.data.canonicalize().unwrap().join("worktrees")));
    assert_public_agent_views(&fixture, &runtime, agent, generation, observer);
    assert_fake_source_handoff(&fixture, &runtime, agent, generation);
    let stop = fixture.json(&["stop", agent, "--generation", &generation_argument]);
    fixture.wait_command(stop["command_id"].as_str().unwrap(), "applied");
    let stopped = fixture.json(&["show", agent]);
    assert_eq!(stopped["state"], "stopped");
    assert_eq!(stopped["generation"], generation);
    let mut after = None;
    let mut exited = 0;
    for _ in 0..8 {
        let mut args = vec![
            "processes",
            agent,
            "--generation",
            &generation_argument,
            "--limit",
            "1000",
        ];
        if let Some(cursor) = after.as_deref() {
            args.extend(["--after", cursor]);
        }
        let page = fixture.json(&args);
        for process in page["processes"].as_array().unwrap() {
            assert_eq!(process["owner"], format!("agent:{agent}#{generation}"));
            assert_eq!(
                process["state"], "exited",
                "stop retained live/unknown owned custody: {process}"
            );
            exited += 1;
        }
        let next = page["next"].as_str().map(str::to_owned);
        if next.is_none() {
            after = None;
            break;
        }
        assert_ne!(next, after, "owned-process cursor did not advance");
        after = next;
    }
    assert!(
        after.is_none(),
        "owned-process readback exceeded its page bound"
    );
    assert!(exited > 0, "stop had no retained owned-process evidence");
    let drained: AgentGetResponse =
        serde_json::from_value(fixture.json(&["show", agent])).expect("drained source view");
    for name in ["stream", "transcript"] {
        let source = drained
            .source_states
            .iter()
            .find(|source| source.source == name)
            .unwrap();
        assert_eq!(
            source.state, "closed",
            "native source survived confirmed process exit"
        );
    }
    let ended = fixture.events();
    let ended: Vec<_> = ended
        .iter()
        .filter(|event| {
            event["agent"] == agent
                && event["generation"] == generation
                && event["kind"] == "session.ended"
        })
        .collect();
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0]["source"], "transcript");
    let removed = fixture.json(&[
        "worktree",
        "remove",
        agent,
        "--generation",
        &generation_argument,
    ]);
    assert_eq!(removed["state"], "applied");
    assert_eq!(
        fs::symlink_metadata(&worktree).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
    assert!(
        fixture
            .git(&[
                "show-ref",
                "--verify",
                "--quiet",
                "refs/heads/codex/ci-smoke"
            ])
            .status
            .success()
    );
    fixture.stop(false);
}

#[test]
fn fake_observer_live_rollout_preserves_unknown_and_detaches_reader() {
    const ORIGINAL_TS: i64 = 1_730_000_000_000;
    let mut fixture = Fixture::new("observer live rollout");
    fixture.start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("observer live RPC runtime");
    let attached = fixture.json(&[
        "attach",
        "--profile",
        "fake",
        "--harness",
        "fake",
        "--scope",
        "fake:discovery",
        "--ref",
        "fake-discovered-2",
    ]);
    let observer = attached["agent"].as_str().unwrap();
    let attach_id = attached["command_id"].as_str().unwrap();
    let attach_command = fixture.wait_command(attach_id, "applied");
    let generation = attach_command["generation"].as_i64().unwrap();
    let watch_path = fixture.root.join("observer-watch.ndjson");
    let mut command = Command::new(CLI);
    command
        .arg("--data-dir")
        .arg(&fixture.data)
        .args(["--json", "watch", observer]);
    let mut watch = Captured::spawn_stdout_file(command, &watch_path);

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let events = loop {
        let events: Vec<_> = fixture
            .events()
            .into_iter()
            .filter(|event| event["agent"] == observer && event["generation"] == generation)
            .collect();
        if events.iter().any(|event| {
            event["kind"] == "turn.completed"
                && event["data"]["turn_id"] == "fake-observed-live-turn"
        }) {
            break events;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "observer live publication deadline"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    let view: AgentGetResponse = serde_json::from_value(fixture.json(&["show", observer]))
        .expect("typed live observer view");
    assert_eq!(view.agent.state, eliot_core::AgentState::Unknown);
    assert!(view.agent.observe_only);
    assert!(!view.agent.own_identity);
    assert!(view.agent.active_turn.is_none());
    assert!(view.agent.pending.is_empty());
    let predecessor = view
        .source_states
        .iter()
        .find(|source| source.source == "observe")
        .unwrap();
    assert_eq!(predecessor.state, "open");
    assert_eq!(predecessor.health, "ok");
    assert_eq!(predecessor.families, ["session"]);
    let successor = view
        .source_states
        .iter()
        .find(|source| source.source == "transcript")
        .unwrap();
    assert_eq!(successor.state, "open");
    assert_eq!(successor.families, ["message", "turn"]);
    assert!(
        successor
            .owners
            .iter()
            .all(|owner| owner.source == "transcript" && owner.mechanism == "rollout_tail")
    );
    let handoffs: Vec<_> = events
        .iter()
        .filter(|event| event["kind"] == "source.handoff")
        .collect();
    assert_eq!(handoffs.len(), 1);
    let handoff = handoffs[0];
    assert_eq!(handoff["data"]["source"], "observe");
    assert_eq!(handoff["data"]["successor"], "transcript");
    assert_eq!(handoff["data"]["mechanism"], "rollout_tail");
    assert_eq!(handoff["data"]["families"], json!(["message", "turn"]));
    let gap = events
        .iter()
        .find(|event| {
            event["kind"] == "source.gap"
                && event["data"]["gap_id"] == handoff["data"]["gap"]["gap_id"]
        })
        .unwrap();
    assert_eq!(
        predecessor.gap_id.as_deref(),
        gap["data"]["gap_id"].as_str()
    );
    assert_eq!(predecessor.gap_seq, gap["seq"].as_i64());
    let applied: Vec<_> = events
        .iter()
        .filter(|event| event["kind"] == "command.applied")
        .collect();
    assert_eq!(applied.len(), 1, "live facts acknowledged another command");
    assert_eq!(applied[0]["data"]["command_id"], attach_id);
    assert!(
        applied[0]["seq"].as_i64().unwrap() < gap["seq"].as_i64().unwrap(),
        "publication began before the reader's attach RESULT was settled"
    );
    assert!(gap["seq"].as_i64().unwrap() < handoff["seq"].as_i64().unwrap());
    let health: Vec<_> = events
        .iter()
        .filter(|event| event["kind"] == "source.health")
        .collect();
    assert_eq!(
        health
            .iter()
            .map(|event| event["data"]["health"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["stale", "ok"],
        "same health must not publish another event"
    );
    assert!(
        health
            .iter()
            .all(|event| event["data"]["source"] == "observe")
    );
    let facts: Vec<_> = events
        .iter()
        .filter(|event| {
            event["source"] == "transcript" && event["data"]["turn_id"] == "fake-observed-live-turn"
        })
        .collect();
    assert_eq!(facts.len(), 3);
    for (event, (kind, ts)) in facts.iter().zip([
        ("turn.started", ORIGINAL_TS + 1),
        ("message.completed", ORIGINAL_TS + 2),
        ("turn.completed", ORIGINAL_TS + 3),
    ]) {
        assert_eq!(event["kind"], kind);
        assert_eq!(event["ts"], ts);
        assert!(event["seq"].as_i64().unwrap() > handoff["seq"].as_i64().unwrap());
        assert!(event["data"].get("command_id").is_none());
        assert!(event["data"].get("request_id").is_none());
    }
    let request = TurnGetRequest::new(observer, generation, "fake-observed-live-turn");
    let turn: CoreTurn = serde_json::from_value(
        public_rpc(
            &runtime,
            &fixture,
            "turn.get",
            serde_json::to_value(&request).unwrap(),
        )
        .unwrap_or_else(|_| panic!("observed turn read failed")),
    )
    .expect("typed observed turn");
    request
        .validate_response(&turn)
        .expect("observed turn binding");
    assert_eq!(turn.state, TurnState::Completed);
    assert_eq!(turn.final_text.as_deref(), Some("fake observed live reply"));
    assert_eq!(turn.started_at, Some(ORIGINAL_TS - 1_000));
    assert_eq!(turn.completed_at, Some(ORIGINAL_TS));
    let result = fixture.json(&[
        "result",
        observer,
        "--generation",
        &generation.to_string(),
        "--turn-id",
        "fake-observed-live-turn",
        "--limit",
        "1",
    ]);
    assert_eq!(result["turns"], json!([turn]));
    assert_eq!(result["agent_metadata"]["state"], "unknown");

    let history = fixture.json(&["history", observer, "--view", "turns", "--limit", "1"]);
    assert_eq!(history["entries"][0]["turn_id"], "fake-observed-turn-1");
    let after_history = fixture.events();
    let after_facts: Vec<_> = after_history
        .iter()
        .filter(|event| {
            event["agent"] == observer
                && event["generation"] == generation
                && event["source"] == "transcript"
                && event["data"]["turn_id"] == "fake-observed-live-turn"
        })
        .collect();
    assert_eq!(after_facts, facts, "private history published live facts");
    assert_eq!(
        fixture.json(&["cmd", "show", attach_id]),
        attach_command,
        "private history changed the attach command"
    );
    let stale = public_rpc(
        &runtime,
        &fixture,
        "agent.history",
        json!({
            "id":observer,"generation":generation+1,"source":"native","view":"turns"
        }),
    )
    .expect_err("history must remain generation fenced");
    assert_eq!(stale.code, "stale_generation");
    let forbidden_id = ulid::Ulid::new().to_string();
    assert!(
        !cli(
            &fixture.data,
            &[
                "--id",
                &forbidden_id,
                "send",
                observer,
                "--",
                "forbidden-live-observer-input"
            ]
        )
        .status
        .success()
    );

    let detach = fixture.json(&["detach", observer]);
    fixture.wait_command(detach["command_id"].as_str().unwrap(), "applied");
    let final_events = fixture.events();
    for source in ["observe", "transcript"] {
        assert_eq!(
            final_events
                .iter()
                .filter(|event| event["agent"] == observer
                    && event["generation"] == generation
                    && event["kind"] == "source.closed"
                    && event["data"]["source"] == source)
                .count(),
            1
        );
    }
    assert!(
        !final_events
            .iter()
            .any(|event| event["agent"] == observer && event["kind"] == "session.ended"),
        "reader detach ended the external session"
    );
    assert_eq!(fault_dispatches(&final_events, &forbidden_id), 0);
    let detached = fixture.json(&["show", observer]);
    assert_eq!(detached["generation"], generation + 1);
    assert_eq!(detached["state"], "unknown");
    assert_eq!(detached["own_identity"], false);
    assert_eq!(fixture.json(&["status"])["observer_readers"], 0);

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let watched = loop {
        let lines = fs::read_to_string(&watch_path).expect("read owned watch output");
        let events: Vec<Value> = lines
            .lines()
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect();
        if events.iter().any(|event| {
            event["kind"] == "source.closed"
                && event["generation"] == generation
                && event["data"]["source"] == "transcript"
        }) {
            break events;
        }
        assert!(
            watch
                .child
                .try_wait()
                .expect("watch process status")
                .is_none(),
            "watch exited before replaying the observer source closure"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "watch output deadline"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    watch
        .child
        .kill()
        .expect("stop exact test-owned watch process");
    watch
        .wait(Duration::from_secs(5))
        .expect("wait exact watch exit");
    let _ = watch.output(Duration::from_secs(2));
    for fact in facts {
        assert!(
            watched.iter().any(|event| event == fact),
            "watch changed an original native fact"
        );
    }
    let subjects = fixture.json(&["discover", "--profile", "fake", "--scope", "fake:discovery"]);
    assert!(
        subjects["subjects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|subject| subject["native_ref"] == "fake-discovered-2"),
        "detaching a reader removed its external subject"
    );
    fixture.stop(false);
}

#[test]
fn shutdown_retains_drain_result_and_force_waits_for_owned_exit() {
    let mut fixture = Fixture::new("typed shutdown retention");
    fixture.start();
    let first = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
    ]);
    let second = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
    ]);
    let first_agent = first["target"].as_str().expect("first live fake");
    let second_agent = second["target"].as_str().expect("anchor live fake");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("shutdown RPC runtime");
    let drain = DaemonShutdownRequest::new(ulid::Ulid::new().to_string(), ShutdownMode::Drain);
    let original = public_rpc(
        &runtime,
        &fixture,
        "daemon.shutdown",
        serde_json::to_value(&drain).expect("drain request"),
    )
    .unwrap_or_else(|_| panic!("blocked drain was not admitted"));
    let reply: DaemonShutdownResponse =
        serde_json::from_value(original.clone()).expect("typed drain response");
    drain
        .validate_response(&reply)
        .expect("matching blocked drain");
    assert_eq!(reply.state, CommandState::Dispatched);
    for agent in [first_agent, second_agent] {
        assert!(
            original["blockers"]
                .as_array()
                .expect("saved blockers")
                .iter()
                .any(|blocker| blocker["kind"] == "agent_live" && blocker["agent"] == agent)
        );
    }
    let stop = fixture.json(&["stop", first_agent]);
    fixture.wait_command(
        stop["command_id"].as_str().expect("stop command"),
        "applied",
    );
    let status = fixture.json(&["status"]);
    assert_eq!(status["admission"], "draining");
    let current = status["blockers"].as_array().expect("current blockers");
    assert!(
        !current
            .iter()
            .any(|blocker| blocker["kind"] == "agent_live" && blocker["agent"] == first_agent)
    );
    assert!(
        current
            .iter()
            .any(|blocker| blocker["kind"] == "agent_live" && blocker["agent"] == second_agent)
    );
    let retained = public_rpc(
        &runtime,
        &fixture,
        "daemon.shutdown",
        serde_json::to_value(&drain).expect("same drain request"),
    )
    .unwrap_or_else(|_| panic!("same drain ID did not return saved blockers"));
    let retained_reply: DaemonShutdownResponse =
        serde_json::from_value(retained.clone()).expect("typed retained drain");
    drain
        .validate_response(&retained_reply)
        .expect("saved response remains valid after status changes");
    assert!(
        retained == original,
        "same drain ID recomputed its retained response"
    );
    let conflicting = DaemonShutdownRequest::new(&drain.id, ShutdownMode::Force);
    let conflict = match public_rpc(
        &runtime,
        &fixture,
        "daemon.shutdown",
        serde_json::to_value(conflicting).expect("conflicting mode"),
    ) {
        Err(error) => error,
        Ok(_) => panic!("shutdown ID changed mode"),
    };
    assert_eq!(conflict.code, "id_conflict");
    assert_eq!(fixture.json(&["status"])["admission"], "draining");

    let force_id = ulid::Ulid::new().to_string();
    let force = DaemonShutdownRequest::new(&force_id, ShutdownMode::Force);
    let forced = cli(
        &fixture.data,
        &["--id", &force_id, "daemon", "stop", "--force"],
    );
    assert_eq!(
        forced.status.code(),
        Some(6),
        "force acknowledges dispatch, not exit"
    );
    let force_reply: DaemonShutdownResponse =
        serde_json::from_str(&forced.stdout).expect("typed CLI force acknowledgement");
    force
        .validate_response(&force_reply)
        .expect("matching force dispatch");
    assert_eq!(force_reply.state, CommandState::Dispatched);
    assert!(force_reply.blockers.is_none());
    assert!(force_reply.store_write_faulted.is_none());
    // Confirmation comes from the exact owned process handle, not the ACK.
    fixture.finish_shutdown();
    fixture.start();
    let retained: DaemonShutdownResponse = serde_json::from_value(
        public_rpc(
            &runtime,
            &fixture,
            "daemon.shutdown",
            serde_json::to_value(&force).expect("retained force request"),
        )
        .unwrap_or_else(|_| panic!("force result did not survive daemon replacement")),
    )
    .expect("typed retained force");
    force
        .validate_response(&retained)
        .expect("matching retained force");
    assert_eq!(retained, force_reply);
    fixture.finish_shutdown();
}

#[test]
fn foreground_startup_and_cli_help() {
    for argument in ["--version", "--help"] {
        let mut command = Command::new(CLI);
        command.arg(argument);
        let reply = run(command);
        assert!(reply.status.success(), "{}", reply.stderr);
        assert!(reply.stdout.contains("eliot"));
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
    command
        .arg("--data-dir")
        .arg(data_argument)
        .args(["--json", "daemon"]);
    let mut starter = Captured::spawn(command);
    assert!(
        starter
            .wait(Duration::from_secs(5))
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
    assert_eq!(fixture.json(&["status"])["state"], "running");
    let (stdout, _) = starter.output(Duration::from_secs(5));
    assert_eq!(
        serde_json::from_str::<Value>(&stdout).unwrap()["state"],
        "starting"
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
fn cli_named_fake_account_capacity_rejects_without_admission_then_reuses_id_after_release() {
    let mut fixture = Fixture::new("named fake account capacity");
    fixture.start();
    fs::write(
        fixture.data.join("config.toml"),
        r#"
[profiles.fake-first]
harness = "fake"
account = "shared"

[profiles.fake-second]
harness = "fake"
account = "shared"

[accounts.shared]
max_live = 1
"#,
    )
    .expect("write named Fake account-cap profiles");

    let workspace = fixture.workspace.to_str().unwrap().to_owned();
    let first_id = ulid::Ulid::new().to_string();
    let first_args = [
        "--id",
        first_id.as_str(),
        "launch",
        "--profile",
        "fake-first",
        "--workspace",
        workspace.as_str(),
    ];
    let first_reply = cli(&fixture.data, &first_args);
    // This named Fake profile has no credential-bearing fields. Mask fixture paths and
    // bound stderr so a failure exposes its cause without leaking unrelated path data.
    let data_path = fixture.data.to_string_lossy();
    let workspace_path = fixture.workspace.to_string_lossy();
    let safe_stderr = first_reply
        .stderr
        .replace(data_path.as_ref(), "[DATA]")
        .replace(workspace_path.as_ref(), "[WORKSPACE]")
        .chars()
        .take(512)
        .collect::<String>();
    assert!(
        first_reply.status.success(),
        "first Fake launch failed with exit {:?}: {safe_stderr}",
        first_reply.status.code()
    );
    let first: CoreCommand =
        serde_json::from_str(&first_reply.stdout).expect("first named Fake launch JSON");
    assert_eq!(first.id, first_id);
    assert_eq!(first.state, CommandState::Applied);
    let first_agent = first.target.clone().expect("first controlled Agent");
    let first_generation = first.generation.expect("first generation");
    assert_eq!(first_generation, 1);
    fixture.wait_command(&first_id, "applied");
    let first_readback: CoreCommand =
        serde_json::from_value(fixture.json(&["cmd", "show", &first_id]))
            .expect("typed first command readback");
    assert_eq!(first_readback, first);

    let stable_agent = |view: &AgentGetResponse| {
        (
            view.agent.id.clone(),
            view.agent.generation,
            view.agent.profile.clone(),
            view.agent.native_ref.clone(),
            view.agent.profile_snapshot.clone(),
        )
    };
    let first_agent_before_retry: AgentGetResponse =
        serde_json::from_value(fixture.json(&["show", &first_agent]))
            .expect("typed first Agent view");
    let stable_first_agent = stable_agent(&first_agent_before_retry);

    let first_dispatches = |events: &[Value]| {
        events
            .iter()
            .filter(|event| {
                event["kind"] == "command.dispatched" && event["data"]["command_id"] == first_id
            })
            .count()
    };
    let events_before_first_retry = fixture.events();
    assert_eq!(first_dispatches(&events_before_first_retry), 1);
    let first_retry: CoreCommand =
        serde_json::from_value(fixture.json(&first_args)).expect("typed retained launch result");
    assert_eq!(
        first_retry, first,
        "same-ID replay of an admitted launch did not return its retained result"
    );
    assert_eq!(
        first_dispatches(&fixture.events()),
        1,
        "same-ID replay dispatched the admitted launch again"
    );
    let first_agent_after_retry: AgentGetResponse =
        serde_json::from_value(fixture.json(&["show", &first_agent]))
            .expect("typed Agent view after retained retry");
    assert_eq!(
        stable_agent(&first_agent_after_retry),
        stable_first_agent,
        "same-ID replay changed stable Agent identity or frozen profile fields"
    );

    let blocked_id = ulid::Ulid::new().to_string();
    let blocked_args = [
        "--id",
        blocked_id.as_str(),
        "launch",
        "--profile",
        "fake-second",
        "--workspace",
        workspace.as_str(),
    ];
    let blocked = cli(&fixture.data, &blocked_args);
    assert_eq!(
        blocked.status.code(),
        Some(3),
        "full account cap must reject before dispatch"
    );
    assert!(
        blocked.stderr.contains("capacity_unavailable"),
        "full account cap did not return capacity_unavailable"
    );
    let missing = cli(&fixture.data, &["cmd", "show", &blocked_id]);
    assert_eq!(missing.status.code(), Some(1));
    assert!(
        missing.stderr.contains("not_found"),
        "cap-rejected request unexpectedly has a command row"
    );
    assert_eq!(fixture.json(&["status"])["agents"], 1);
    let events_after_rejection = fixture.events();
    assert!(
        events_after_rejection
            .iter()
            .all(|event| event["data"]["command_id"] != blocked_id),
        "unadmitted cap rejection wrote a command event"
    );
    assert_eq!(
        first_dispatches(&events_after_rejection),
        1,
        "unadmitted cap rejection changed the admitted launch dispatch count"
    );

    let generation = first_generation.to_string();
    let stop: CommandSubmitResponse =
        serde_json::from_value(fixture.json(&["stop", &first_agent, "--generation", &generation]))
            .expect("typed first stop submission");
    fixture.wait_command(&stop.command_id, "applied");
    assert_eq!(fixture.json(&["show", &first_agent])["state"], "stopped");
    // Fake has one known owned root; this bounded page verifies that root only.
    let processes = fixture.json(&[
        "processes",
        &first_agent,
        "--generation",
        &generation,
        "--limit",
        "1000",
    ]);
    let processes = processes["processes"]
        .as_array()
        .expect("stopped generation process page");
    assert!(
        !processes.is_empty(),
        "Fake launch had no owned process evidence"
    );
    assert!(
        processes.iter().all(|process| process["state"] == "exited"),
        "known Fake root was not exited before cap release"
    );

    fixture.stop(false);
    fixture.start();

    let first_after_restart: CoreCommand = serde_json::from_value(fixture.json(&first_args))
        .expect("typed retained launch after restart");
    assert_eq!(
        first_after_restart, first,
        "restart changed the retained admitted launch result"
    );
    let first_readback_after_restart: CoreCommand =
        serde_json::from_value(fixture.json(&["cmd", "show", &first_id]))
            .expect("typed command readback after restart");
    assert_eq!(
        first_readback_after_restart, first_readback,
        "restart changed the admitted launch command readback"
    );
    assert_eq!(fixture.json(&["show", &first_agent])["state"], "stopped");
    assert_eq!(
        first_dispatches(&fixture.events()),
        1,
        "restart replayed the admitted launch"
    );

    // This ID was rejected before admission, so it has no retained command binding
    // and may be submitted after the proven closed generation releases its slot.
    let second: CoreCommand =
        serde_json::from_value(fixture.json(&blocked_args)).expect("typed second launch");
    assert_eq!(second.id, blocked_id);
    assert_eq!(second.state, CommandState::Applied);
    let second_agent = second.target.clone().expect("second controlled Agent");
    let second_stop: CommandSubmitResponse =
        serde_json::from_value(fixture.json(&["stop", &second_agent]))
            .expect("typed second stop submission");
    fixture.wait_command(&second_stop.command_id, "applied");
    fixture.stop(false);
}
