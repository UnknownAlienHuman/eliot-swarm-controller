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
    PROCESS_CURSOR_MAX_BYTES, ProjectRegistrationView, ProviderClearHoldResponse,
    ProviderHoldsResponse, RuleStateView, ShutdownMode, TimerSchedule, TimerStateView,
    TurnGetRequest,
};
use eliot_core::{
    AgentState, Command as CoreCommand, CommandState, ProviderHoldState, ProviderRestrictionKind,
    Turn as CoreTurn, TurnState,
};
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

fn wait_fake_send_turn_result(
    fixture: &Fixture,
    agent: &str,
    generation: i64,
    command: &CoreCommand,
    expected_text: &str,
) -> CoreTurn {
    assert_eq!(command.verb, "send");
    assert_eq!(command.target.as_deref(), Some(agent));
    assert_eq!(command.expected_generation, Some(generation));
    assert_eq!(command.generation, Some(generation));
    let turn_id = command
        .turn_id
        .as_deref()
        .expect("SEND child retains its exact turn ID");
    let request = AgentResultRequest {
        id: agent.to_owned(),
        generation: Some(generation),
        turn_id: Some(turn_id.to_owned()),
        limit: 1,
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let events = fixture.events();
        let terminal = events.iter().find(|event| {
            event["kind"] == "turn.completed"
                && event["agent"] == agent
                && event["generation"] == generation
                && event["data"]["command_id"] == command.id
                && event["data"]["turn_id"] == turn_id
        });
        if terminal.is_some() {
            let response: AgentResultResponse = serde_json::from_value(fixture.json(&[
                "result",
                agent,
                "--generation",
                &generation.to_string(),
                "--turn-id",
                turn_id,
                "--limit",
                "1",
            ]))
            .expect("typed exact SEND result");
            request
                .validate_response(&response)
                .expect("SEND result remains bound to its command turn");
            let [turn] = response.turns.as_slice() else {
                panic!("exact SEND result did not return one turn");
            };
            assert_eq!(turn.state, TurnState::Completed);
            assert_eq!(turn.final_text.as_deref(), Some(expected_text));
            return turn.clone();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "SEND command {} did not produce its exact terminal turn",
            command.id
        );
        std::thread::sleep(Duration::from_millis(25));
    }
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
    assert!(
        history_after.high_water >= history_before.high_water,
        "private-history event readback regressed"
    );
    assert!(
        !history_after.truncated,
        "event attribution requires the complete bounded event page"
    );
    let process_owner = format!("agent:{observer}#{observed_generation}");
    for event in history_after
        .events
        .iter()
        .filter(|event| event.seq > history_before.high_water)
    {
        // The Windows OS sampler can publish process transitions or health
        // changes after this baseline; private history pages cannot publish.
        let process_monitor_event = matches!(
            event.kind.as_str(),
            "process.started" | "process.exited" | "process.unknown"
        ) && event.agent.as_deref() == Some(observer)
            && event.generation == Some(observed_generation)
            && event.source == "os"
            && event.data["owner"].as_str() == Some(process_owner.as_str());
        let os_health_event = event.kind == "source.health"
            && event.agent.as_deref() == Some(observer)
            && event.generation == Some(observed_generation)
            && event.source == "daemon"
            && event.data["source"] == "os";
        assert!(
            process_monitor_event || os_health_event,
            "private history readback coincided with an event outside this observer's OS monitor: {event:?}"
        );
        let serialized = serde_json::to_string(event).expect("serialize attributed monitor event");
        for private_content in [
            "fake-discovered-1",
            "fake-observed-turn-1",
            "fake-observed-turn-2",
            "fake observation history fixture",
        ] {
            assert!(
                !serialized.contains(private_content),
                "private observation content was published in event {}",
                event.seq
            );
        }
    }
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
    // This named Fake profile has no credential-bearing fields. Mask fixture paths and
    // bound/sanitize stderr so a failure exposes its phase without leaking path data.
    let data_path = fixture.data.to_string_lossy().into_owned();
    let workspace_path = workspace.clone();
    let command_from_reply = |phase: &str, reply: support::Reply| -> CoreCommand {
        let safe_stderr = reply
            .stderr
            .replace(&data_path, "[DATA]")
            .replace(&workspace_path, "[WORKSPACE]")
            .chars()
            .take(512)
            .map(|character| {
                if character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .collect::<String>();
        assert!(
            reply.status.success(),
            "{phase} failed with exit {:?}: {safe_stderr}",
            reply.status.code()
        );
        serde_json::from_str(&reply.stdout)
            .unwrap_or_else(|_| panic!("{phase} returned invalid CoreCommand JSON; stdout omitted"))
    };
    let first = command_from_reply("initial launch", cli(&fixture.data, &first_args));
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
    let first_retry = command_from_reply(
        "same-ID replay while cap is full",
        cli(&fixture.data, &first_args),
    );
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

    let first_after_restart = command_from_reply(
        "same-ID replay after restart",
        cli(&fixture.data, &first_args),
    );
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

#[test]
fn cli_provider_restrictions_gate_fake_work_and_owner_clear_permanent_holds() {
    let mut fixture = Fixture::new("provider restriction CLI");
    fixture.start();
    fs::write(
        fixture.data.join("config.toml"),
        r#"
[profiles.fake-provider]
harness = "fake"
account = "shared"

[profiles.fake-next]
harness = "fake"
account = "shared"

[accounts.shared]
max_live = 3
"#,
    )
    .expect("write named Fake provider-scope profiles");

    let workspace = fixture.workspace.to_string_lossy().into_owned();
    let data_path = fixture.data.to_string_lossy().into_owned();
    let parse_command = |phase: &str, reply: support::Reply| -> CoreCommand {
        let safe_stderr = reply
            .stderr
            .replace(&data_path, "[DATA]")
            .replace(&workspace, "[WORKSPACE]")
            .chars()
            .take(512)
            .map(|character| {
                if character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .collect::<String>();
        assert!(
            reply.status.success(),
            "{phase} failed with exit {:?}: {safe_stderr}",
            reply.status.code()
        );
        serde_json::from_str(&reply.stdout)
            .unwrap_or_else(|_| panic!("{phase} returned invalid CoreCommand JSON; stdout omitted"))
    };
    let data_dir = fixture.data.clone();
    let assert_unadmitted = |id: &str| {
        let missing = cli(&data_dir, &["cmd", "show", id]);
        assert_eq!(missing.status.code(), Some(1));
        assert!(missing.stderr.contains("not_found"));
    };
    let read_holds = |fixture: &Fixture| -> ProviderHoldsResponse {
        serde_json::from_value(fixture.json(&["provider", "holds", "--limit", "10"]))
            .expect("typed provider hold page")
    };
    let restriction_event_count =
        |events: &[Value],
         agent: &str,
         generation: i64,
         command: &str,
         turn: &str,
         kind: ProviderRestrictionKind| {
            events
                .iter()
                .filter(|event| {
                    event["kind"] == "turn.completed"
                        && event["agent"].as_str() == Some(agent)
                        && event["generation"].as_i64() == Some(generation)
                        && event["data"]["command_id"].as_str() == Some(command)
                        && event["data"]["turn_id"].as_str() == Some(turn)
                        && event["data"]["provider_restriction"]["kind"].as_str()
                            == Some(kind.as_str())
                })
                .count()
        };
    let has_command_event = |events: &[Value], command: &str| {
        events
            .iter()
            .any(|event| event["data"]["command_id"].as_str() == Some(command))
    };

    let launch_id = ulid::Ulid::new().to_string();
    let launch_args = [
        "--id",
        launch_id.as_str(),
        "launch",
        "--profile",
        "fake-provider",
        "--workspace",
        workspace.as_str(),
    ];
    let launch = parse_command("initial Fake launch", cli(&fixture.data, &launch_args));
    assert_eq!(launch.id, launch_id);
    assert_eq!(launch.state, CommandState::Applied);
    let agent = launch.target.clone().expect("controlled Fake Agent");
    let generation = launch.generation.expect("Fake generation");
    assert_eq!(generation, 1);
    fixture.wait_command(&launch_id, "applied");

    let generation_arg = generation.to_string();
    let rate_id = ulid::Ulid::new().to_string();
    let rate_marker = "[eliot_fake_provider_rate_limit]";
    let rate_reply = cli(
        &fixture.data,
        &[
            "--id",
            &rate_id,
            "send",
            &agent,
            "--generation",
            &generation_arg,
            "--wait",
            "--timeout-seconds",
            "10",
            "--",
            rate_marker,
        ],
    );
    assert_eq!(rate_reply.status.code(), Some(1));
    let rate_turn: CoreTurn =
        serde_json::from_str(&rate_reply.stdout).expect("typed failed rate-limit turn");
    assert_eq!(rate_turn.agent, agent);
    assert_eq!(rate_turn.generation, generation);
    assert_eq!(rate_turn.state, TurnState::Completed);
    assert_eq!(rate_turn.status.as_deref(), Some("failed"));
    assert!(rate_turn.final_text.is_none());
    fixture.wait_command(&rate_id, "applied");
    let rate_command: CoreCommand =
        serde_json::from_value(fixture.json(&["cmd", "show", &rate_id]))
            .expect("typed normal SEND command");
    assert!(!rate_command.probe);
    assert_eq!(rate_command.args, json!({"text":rate_marker}));
    let events = fixture.events();
    assert_eq!(
        restriction_event_count(
            &events,
            &agent,
            generation,
            &rate_id,
            &rate_turn.turn_id,
            ProviderRestrictionKind::RateLimit,
        ),
        1,
        "rate hold did not come from its exact structured turn completion"
    );

    let rate_page = read_holds(&fixture);
    assert_eq!(rate_page.holds.len(), 1);
    assert_eq!(rate_page.next.as_deref(), Some("account:shared"));
    let terminal_rate_page: ProviderHoldsResponse = serde_json::from_value(fixture.json(&[
        "provider",
        "holds",
        "--after",
        "account:shared",
        "--limit",
        "10",
    ]))
    .expect("terminal provider hold page");
    assert!(terminal_rate_page.holds.is_empty());
    assert_eq!(terminal_rate_page.next, None);
    assert_eq!(rate_page.holds[0].scope, "account:shared");
    assert_eq!(rate_page.holds[0].kind, ProviderRestrictionKind::RateLimit);
    assert_eq!(rate_page.holds[0].state, ProviderHoldState::Held);
    assert_eq!(rate_page.holds[0].probe_command, None);

    let blocked_send_id = ulid::Ulid::new().to_string();
    let blocked_send = cli(
        &fixture.data,
        &[
            "--id",
            &blocked_send_id,
            "send",
            &agent,
            "--generation",
            &generation_arg,
            "--",
            "ordinary held input",
        ],
    );
    assert_eq!(blocked_send.status.code(), Some(3));
    assert!(blocked_send.stderr.contains("provider_held"));
    assert_unadmitted(&blocked_send_id);

    let blocked_launch_id = ulid::Ulid::new().to_string();
    let blocked_launch = cli(
        &fixture.data,
        &[
            "--id",
            &blocked_launch_id,
            "launch",
            "--profile",
            "fake-next",
            "--workspace",
            workspace.as_str(),
        ],
    );
    assert_eq!(blocked_launch.status.code(), Some(3));
    assert!(blocked_launch.stderr.contains("provider_held"));
    assert_unadmitted(&blocked_launch_id);
    assert_eq!(fixture.json(&["status"])["agents"], 1);
    let events = fixture.events();
    assert!(!has_command_event(&events, &blocked_send_id));
    assert!(!has_command_event(&events, &blocked_launch_id));

    let probe_id = ulid::Ulid::new().to_string();
    let probe_text = "successful explicit rate-limit probe";
    let probe_turn: CoreTurn = serde_json::from_value(fixture.json(&[
        "--id",
        &probe_id,
        "send",
        "--probe",
        &agent,
        "--generation",
        &generation_arg,
        "--wait",
        "--timeout-seconds",
        "10",
        "--",
        probe_text,
    ]))
    .expect("typed successful explicit probe turn");
    assert_eq!(probe_turn.agent, agent);
    assert_eq!(probe_turn.generation, generation);
    assert_eq!(probe_turn.state, TurnState::Completed);
    assert_eq!(probe_turn.status.as_deref(), Some("completed"));
    assert_eq!(probe_turn.final_text.as_deref(), Some(probe_text));
    fixture.wait_command(&probe_id, "applied");
    let probe_command: CoreCommand =
        serde_json::from_value(fixture.json(&["cmd", "show", &probe_id]))
            .expect("typed probe command");
    assert!(probe_command.probe);
    assert_eq!(probe_command.args, json!({"text":probe_text}));
    assert_eq!(
        probe_command.turn_id.as_deref(),
        Some(probe_turn.turn_id.as_str())
    );
    let cleared_rate = read_holds(&fixture);
    assert!(cleared_rate.holds.is_empty());
    assert_eq!(cleared_rate.next, None);

    for (kind, marker, evidence, changed_evidence) in [
        (
            ProviderRestrictionKind::Auth,
            "[eliot_fake_provider_auth]",
            "credentials_updated",
            "configuration_updated",
        ),
        (
            ProviderRestrictionKind::ModelRemoved,
            "[eliot_fake_provider_model_removed]",
            "configuration_updated",
            "owner_verified",
        ),
    ] {
        let restriction_id = ulid::Ulid::new().to_string();
        let restriction_reply = cli(
            &fixture.data,
            &[
                "--id",
                &restriction_id,
                "send",
                &agent,
                "--generation",
                &generation_arg,
                "--wait",
                "--timeout-seconds",
                "10",
                "--",
                marker,
            ],
        );
        assert_eq!(restriction_reply.status.code(), Some(1));
        let restriction_turn: CoreTurn = serde_json::from_str(&restriction_reply.stdout)
            .expect("typed failed permanent-restriction turn");
        assert_eq!(restriction_turn.agent, agent);
        assert_eq!(restriction_turn.generation, generation);
        assert_eq!(restriction_turn.state, TurnState::Completed);
        assert_eq!(restriction_turn.status.as_deref(), Some("failed"));
        fixture.wait_command(&restriction_id, "applied");
        let events = fixture.events();
        assert_eq!(
            restriction_event_count(
                &events,
                &agent,
                generation,
                &restriction_id,
                &restriction_turn.turn_id,
                kind,
            ),
            1,
            "permanent hold did not come from its exact structured turn completion"
        );
        let hold_page = read_holds(&fixture);
        assert_eq!(hold_page.holds.len(), 1);
        assert_eq!(hold_page.holds[0].scope, "account:shared");
        assert_eq!(hold_page.holds[0].kind, kind);
        assert_eq!(hold_page.holds[0].state, ProviderHoldState::Held);
        assert_eq!(hold_page.holds[0].probe_command, None);

        let blocked_probe_id = ulid::Ulid::new().to_string();
        let blocked_probe = cli(
            &fixture.data,
            &[
                "--id",
                &blocked_probe_id,
                "send",
                "--probe",
                &agent,
                "--generation",
                &generation_arg,
                "--",
                "permanent holds are not probe eligible",
            ],
        );
        assert_eq!(blocked_probe.status.code(), Some(3));
        assert!(blocked_probe.stderr.contains("provider_held"));
        assert_unadmitted(&blocked_probe_id);

        let stale_clear_id = ulid::Ulid::new().to_string();
        let stale_probe_id = ulid::Ulid::new().to_string();
        let stale_clear = cli(
            &fixture.data,
            &[
                "--id",
                &stale_clear_id,
                "provider",
                "clear-hold",
                "account:shared",
                "--corrective-evidence",
                evidence,
                "--expected-probe-command",
                &stale_probe_id,
            ],
        );
        assert!(
            !stale_clear.status.success(),
            "permanent hold accepted a mismatched probe CAS"
        );
        let still_held = read_holds(&fixture);
        assert_eq!(still_held.holds.len(), 1);
        assert_eq!(still_held.holds[0].kind, kind);
        assert_eq!(still_held.holds[0].probe_command, None);

        let clear_id = ulid::Ulid::new().to_string();
        let clear_args = [
            "--id",
            clear_id.as_str(),
            "provider",
            "clear-hold",
            "account:shared",
            "--corrective-evidence",
            evidence,
        ];
        let cleared: ProviderClearHoldResponse = serde_json::from_value(fixture.json(&clear_args))
            .expect("typed owner corrective clear");
        assert_eq!(cleared.id, clear_id);
        assert_eq!(cleared.scope, "account:shared");
        assert!(cleared.cleared);
        let repeated: ProviderClearHoldResponse = serde_json::from_value(fixture.json(&clear_args))
            .expect("idempotent owner corrective clear");
        assert_eq!(repeated, cleared);

        let changed_clear = cli(
            &fixture.data,
            &[
                "--id",
                &clear_id,
                "provider",
                "clear-hold",
                "account:shared",
                "--corrective-evidence",
                changed_evidence,
            ],
        );
        assert_eq!(changed_clear.status.code(), Some(3));
        assert!(changed_clear.stderr.contains("id_conflict"));
        assert!(read_holds(&fixture).holds.is_empty());
    }

    let stop: CommandSubmitResponse =
        serde_json::from_value(fixture.json(&["stop", &agent, "--generation", &generation_arg]))
            .expect("typed Fake stop submission");
    fixture.wait_command(&stop.command_id, "applied");
    fixture.stop(false);
}

#[test]
fn cli_fake_rule_stop_keeps_cursor_and_does_not_replay_after_restart() {
    let mut fixture = Fixture::new("event rule restart");
    fixture.start();
    fixture.stop(false);

    let rules = fixture.data.join("rules");
    fs::create_dir_all(&rules).expect("create rules under initialized private data root");
    fs::write(
        rules.join("stop-on-result.toml"),
        r#"
[[rule]]
id = "stop-on-result"
on = { event = "turn.completed", match = { "subject.harness" = "fake" } }
do = [{ action = "stop", target = "${event.agent}", expected_generation = "${event.generation}" }]
"#,
    )
    .expect("write owner-approved global rule");

    fixture.start();
    let rule_id = "stop-on-result";
    let load_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let loaded: RuleStateView = loop {
        let reply = cli(&fixture.data, &["rule", "show", rule_id]);
        if reply.status.success() {
            break serde_json::from_str(&reply.stdout).expect("typed startup-loaded rule state");
        }
        assert!(
            std::time::Instant::now() < load_deadline,
            "startup-loaded rule state deadline"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(loaded.id, rule_id);
    assert!(loaded.enabled);
    assert!(loaded.paused_reason.is_none());

    let launched = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
    ]);
    let agent = launched["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launched["generation"].as_i64().expect("Fake generation");
    let generation_arg = generation.to_string();
    let send_id = ulid::Ulid::new().to_string();
    let turn: CoreTurn = serde_json::from_value(fixture.json(&[
        "--id",
        &send_id,
        "send",
        &agent,
        "--generation",
        &generation_arg,
        "--wait",
        "--timeout-seconds",
        "10",
        "--",
        "rule-trigger-result",
    ]))
    .expect("typed Fake result turn");
    assert_eq!(turn.agent, agent);
    assert_eq!(turn.generation, generation);
    assert_eq!(turn.state, TurnState::Completed);

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let (state, events, fired) = loop {
        let state: RuleStateView = serde_json::from_value(fixture.json(&["rule", "show", rule_id]))
            .expect("typed rule cursor readback");
        let events = fixture.events();
        let terminal = events.iter().find(|event| {
            event["kind"] == "turn.completed"
                && event["agent"] == agent
                && event["generation"] == generation
                && event["data"]["command_id"] == send_id
        });
        let fired = events
            .iter()
            .find(|event| event["kind"] == "rule.fired" && event["data"]["rule"] == rule_id)
            .cloned();
        if let (Some(terminal), Some(fired)) = (terminal, fired)
            && state.current_cursor >= terminal["seq"].as_i64().expect("terminal event seq")
        {
            break (state, events, fired);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rule cursor and firing event deadline"
        );
        std::thread::sleep(Duration::from_millis(25));
    };

    assert_eq!(fired["data"]["revision"], state.revision);
    let terminal_event = events
        .iter()
        .find(|event| {
            event["kind"] == "turn.completed"
                && event["agent"] == agent
                && event["generation"] == generation
                && event["data"]["command_id"] == send_id
        })
        .expect("exact result event");
    assert_eq!(
        fired["data"]["event_seq"], terminal_event["seq"],
        "rule firing is bound to this result event"
    );
    assert_eq!(fired["data"]["rule"], rule_id);
    let parent_id = fired["data"]["parent_command"]
        .as_str()
        .expect("firing parent command id")
        .to_owned();
    let child_id = fired["data"]["child_command"]
        .as_str()
        .expect("firing child command id")
        .to_owned();
    let parent: CoreCommand = serde_json::from_value(fixture.wait_command(&parent_id, "applied"))
        .expect("typed Store-only rule parent");
    assert_eq!(parent.id, parent_id);
    assert_eq!(parent.verb, "rule");
    assert_eq!(parent.target, None);
    assert_eq!(parent.expected_generation, None);
    assert_eq!(parent.generation, None);
    assert_eq!(
        parent.args,
        json!({
            "rule": rule_id,
            "revision": state.revision,
            "event_seq": terminal_event["seq"],
        })
    );
    assert_eq!(
        parent.outcome,
        Some(json!({"children":[child_id]})),
        "parent outcome preserves its exact fixed child set"
    );
    let child: CoreCommand = serde_json::from_value(fixture.wait_command(&child_id, "applied"))
        .expect("typed ordinary stop child");
    assert_eq!(child.id, child_id);
    assert_eq!(child.verb, "stop");
    assert_eq!(child.target.as_deref(), Some(agent.as_str()));
    assert_eq!(child.expected_generation, Some(generation));
    assert_eq!(child.generation, Some(generation));
    assert_eq!(child.state, CommandState::Applied);
    assert_eq!(
        fault_dispatches(&events, &parent_id),
        0,
        "parent is Store-only"
    );
    assert_eq!(fault_dispatches(&events, &child_id), 1);
    let stopped: AgentGetResponse =
        serde_json::from_value(fixture.json(&["show", &agent])).expect("typed stopped Agent");
    assert_eq!(stopped.agent.state, AgentState::Stopped);
    let events = fixture.events();

    let fired_before_restart: Vec<_> = events
        .iter()
        .filter(|event| event["kind"] == "rule.fired" && event["data"]["rule"] == rule_id)
        .cloned()
        .collect();
    assert_eq!(fired_before_restart, vec![fired]);
    let command_events_before_restart: Vec<_> = events
        .iter()
        .filter(|event| {
            let command_id = event["data"]["command_id"].as_str();
            command_id.is_some_and(|id| id == parent_id.as_str() || id == child_id.as_str())
        })
        .cloned()
        .collect();

    fixture.stop(false);
    fixture.start();
    let resume_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let resumed: RuleStateView = loop {
        let reply = cli(&fixture.data, &["rule", "show", rule_id]);
        if reply.status.success() {
            break serde_json::from_str(&reply.stdout).expect("typed resumed rule state");
        }
        assert!(
            std::time::Instant::now() < resume_deadline,
            "resumed rule state deadline"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(resumed.id, state.id);
    assert_eq!(resumed.revision, state.revision);
    assert_eq!(resumed.enabled, state.enabled);
    assert_eq!(resumed.paused_reason, state.paused_reason);
    assert!(
        resumed.current_cursor >= state.current_cursor,
        "restart rewound the persisted rule cursor"
    );
    let after_restart = fixture.events();
    let fired_after_restart: Vec<_> = after_restart
        .iter()
        .filter(|event| event["kind"] == "rule.fired" && event["data"]["rule"] == rule_id)
        .cloned()
        .collect();
    assert_eq!(fired_after_restart, fired_before_restart);
    assert_eq!(fault_dispatches(&after_restart, &child_id), 1);
    let command_events_after_restart: Vec<_> = after_restart
        .iter()
        .filter(|event| {
            let command_id = event["data"]["command_id"].as_str();
            command_id.is_some_and(|id| id == parent_id.as_str() || id == child_id.as_str())
        })
        .cloned()
        .collect();
    assert_eq!(
        command_events_after_restart, command_events_before_restart,
        "restart admitted or dispatched another rule command"
    );
    let parent_after_restart: CoreCommand =
        serde_json::from_value(fixture.json(&["cmd", "show", parent_id.as_str()]))
            .expect("retained rule parent");
    let child_after_restart: CoreCommand =
        serde_json::from_value(fixture.json(&["cmd", "show", child_id.as_str()]))
            .expect("retained rule child");
    assert_eq!(parent_after_restart, parent);
    assert_eq!(child_after_restart, child);
    let stopped_after_restart: AgentGetResponse =
        serde_json::from_value(fixture.json(&["show", &agent])).expect("retained stopped Agent");
    assert_eq!(stopped_after_restart.agent.state, AgentState::Stopped);
    fixture.stop(false);
}

#[test]
fn cli_fake_timer_tick_and_manual_replay_survive_restart() {
    let mut fixture = Fixture::new("timer restart");
    fixture.start();
    fixture.stop(false);

    let rules = fixture.data.join("rules");
    fs::create_dir_all(&rules).expect("create timers under initialized private data root");
    let rule_config = concat!(
        "[[rule]]\n",
        "id = \"scheduled-stop\"\n",
        "on = { every = \"10s\" }\n",
        "do = [{ action = \"stop\", target = \"a1\", expected_generation = 1 }]\n\n",
        "[[rule]]\n",
        "id = \"manual-stop\"\n",
        "on = { every = \"1h\" }\n",
        "do = [{ action = \"stop\", target = \"a2\", expected_generation = 1 }]\n",
    );
    fs::write(rules.join("timers.toml"), rule_config)
        .expect("write timer rules with literal Agent generations");

    let epoch_ms = || {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock is after the Unix epoch")
                .as_millis(),
        )
        .expect("current epoch milliseconds fit signed range")
    };
    let registration_started_ms = epoch_ms();
    fixture.start();
    let scheduled: TimerStateView =
        serde_json::from_value(fixture.json(&["timer", "show", "scheduled-stop"]))
            .expect("typed scheduled timer state");
    let registration_readback_ms = epoch_ms();
    assert_eq!(scheduled.id, "scheduled-stop");
    assert!(scheduled.enabled);
    assert!(
        (registration_started_ms..=registration_readback_ms).contains(&scheduled.anchor),
        "omitted anchor was not captured during timer registration"
    );
    assert_eq!(
        scheduled.schedule,
        TimerSchedule::Interval {
            period_ms: 10_000,
            anchor: None,
        }
    );
    assert_eq!(scheduled.next_due_at, Some(scheduled.anchor + 10_000));
    assert_eq!(scheduled.last_key, None);

    let manual_before: TimerStateView =
        serde_json::from_value(fixture.json(&["timer", "show", "manual-stop"]))
            .expect("typed manual timer state");
    assert_eq!(
        manual_before.schedule,
        TimerSchedule::Interval {
            period_ms: 3_600_000,
            anchor: None,
        }
    );
    assert_eq!(
        manual_before.next_due_at,
        Some(manual_before.anchor + 3_600_000)
    );
    assert_eq!(manual_before.last_key, None);

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
    assert_eq!(first["target"], "a1");
    assert_eq!(first["generation"], 1);
    assert_eq!(second["target"], "a2");
    assert_eq!(second["generation"], 1);

    let manual_id = ulid::Ulid::new().to_string();
    let manual_parent: CoreCommand =
        serde_json::from_value(fixture.json(&["--id", &manual_id, "timer", "run", "manual-stop"]))
            .expect("typed manual timer parent");
    assert_eq!(manual_parent.id, manual_id);
    assert_eq!(manual_parent.args["rule"], "manual-stop");
    let manual_children = manual_parent
        .outcome
        .as_ref()
        .expect("manual parent child set")["children"]
        .as_array()
        .expect("manual parent children array");
    assert_eq!(manual_children.len(), 1);
    let manual_child_id = manual_children[0]
        .as_str()
        .expect("manual stop child id")
        .to_owned();
    let manual_child: CoreCommand =
        serde_json::from_value(fixture.wait_command(&manual_child_id, "applied"))
            .expect("applied manual stop child");
    assert_eq!(manual_child.verb, "stop");
    assert_eq!(manual_child.target.as_deref(), Some("a2"));
    assert_eq!(manual_child.expected_generation, Some(1));
    assert_eq!(manual_child.generation, Some(1));

    let replayed_parent: CoreCommand =
        serde_json::from_value(fixture.json(&["--id", &manual_id, "timer", "run", "manual-stop"]))
            .expect("same-ID manual timer replay");
    assert_eq!(replayed_parent, manual_parent);
    let manual_after: TimerStateView =
        serde_json::from_value(fixture.json(&["timer", "show", "manual-stop"]))
            .expect("manual timer schedule after replay");
    assert_eq!(manual_after.anchor, manual_before.anchor);
    assert_eq!(manual_after.last_key, manual_before.last_key);
    assert_eq!(manual_after.next_due_at, manual_before.next_due_at);

    let stop_dispatches = |events: &[Value], agent: &str| {
        events
            .iter()
            .filter(|event| {
                event["kind"] == "command.dispatched"
                    && event["agent"] == agent
                    && event["generation"] == 1
                    && event["data"]["verb"] == "stop"
            })
            .count()
    };
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let (scheduled_after_fire, scheduled_stop_id) = loop {
        let state: TimerStateView =
            serde_json::from_value(fixture.json(&["timer", "show", "scheduled-stop"]))
                .expect("scheduled timer readback");
        let events = fixture.events();
        let stop = events.iter().find(|event| {
            event["kind"] == "command.dispatched"
                && event["agent"] == "a1"
                && event["generation"] == 1
                && event["data"]["verb"] == "stop"
        });
        if let (Some(_), Some(stop)) = (state.last_key, stop) {
            break (
                state,
                stop["data"]["command_id"]
                    .as_str()
                    .expect("scheduled stop command id")
                    .to_owned(),
            );
        }
        assert!(
            std::time::Instant::now() < deadline,
            "scheduled timer stop deadline"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert!(scheduled_after_fire.last_key.unwrap() >= scheduled.anchor + 10_000);
    let scheduled_child: CoreCommand =
        serde_json::from_value(fixture.wait_command(&scheduled_stop_id, "applied"))
            .expect("applied scheduled stop child");
    assert_eq!(scheduled_child.verb, "stop");
    assert_eq!(scheduled_child.target.as_deref(), Some("a1"));
    assert_eq!(scheduled_child.expected_generation, Some(1));
    assert_eq!(scheduled_child.generation, Some(1));
    let stopped_first: AgentGetResponse =
        serde_json::from_value(fixture.json(&["show", "a1"])).expect("stopped scheduled Agent");
    assert_eq!(stopped_first.agent.state, AgentState::Stopped);
    let events_before_restart = fixture.events();
    assert_eq!(stop_dispatches(&events_before_restart, "a1"), 1);
    assert_eq!(stop_dispatches(&events_before_restart, "a2"), 1);

    fixture.stop(false);
    fixture.start();
    let scheduled_after_restart: TimerStateView =
        serde_json::from_value(fixture.json(&["timer", "show", "scheduled-stop"]))
            .expect("scheduled timer state after restart");
    assert_eq!(scheduled_after_restart.id, scheduled_after_fire.id);
    assert_eq!(
        scheduled_after_restart.schedule_revision,
        scheduled_after_fire.schedule_revision
    );
    assert_eq!(
        scheduled_after_restart.schedule,
        scheduled_after_fire.schedule
    );
    assert_eq!(scheduled_after_restart.anchor, scheduled_after_fire.anchor);
    assert_eq!(
        scheduled_after_restart.enabled,
        scheduled_after_fire.enabled
    );
    assert!(scheduled_after_restart.last_key.is_some());
    assert!(
        scheduled_after_restart.last_key.unwrap() >= scheduled_after_fire.last_key.unwrap(),
        "restart rewound the scheduled UTC slot key"
    );
    let manual_after_restart: TimerStateView =
        serde_json::from_value(fixture.json(&["timer", "show", "manual-stop"]))
            .expect("manual timer state after restart");
    assert_eq!(
        manual_after_restart.schedule_revision,
        manual_before.schedule_revision
    );
    assert_eq!(manual_after_restart.schedule, manual_before.schedule);
    assert_eq!(manual_after_restart.anchor, manual_before.anchor);
    assert_eq!(manual_after_restart.last_key, manual_before.last_key);
    assert_eq!(manual_after_restart.next_due_at, manual_before.next_due_at);
    let events_after_restart = fixture.events();
    assert_eq!(stop_dispatches(&events_after_restart, "a1"), 1);
    assert_eq!(stop_dispatches(&events_after_restart, "a2"), 1);
    let scheduled_after_restart_command: CoreCommand =
        serde_json::from_value(fixture.json(&["cmd", "show", &scheduled_stop_id]))
            .expect("retained scheduled stop child");
    assert_eq!(scheduled_after_restart_command, scheduled_child);
    fixture.stop(false);
}

#[test]
fn cli_event_rule_matching_reload_and_invalid_retention_survive_restart() {
    let mut fixture = Fixture::new("event rule reload");
    fixture.start();
    fixture.stop(false);

    let send_ids = [
        ulid::Ulid::new().to_string(),
        ulid::Ulid::new().to_string(),
        ulid::Ulid::new().to_string(),
        ulid::Ulid::new().to_string(),
    ];
    let rule_source = |commands: &[&str], agents: &[&str]| {
        let command_values = commands
            .iter()
            .map(|command| format!("\"{command}\""))
            .collect::<Vec<_>>()
            .join(", ");
        let agent_values = agents
            .iter()
            .map(|agent| format!("\"{agent}\""))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            concat!(
                "[[rule]]\n",
                "id = \"stop-applied-send\"\n",
                "on = {{ event = \"command.applied\", match = {{ ",
                "\"event.kind\" = \"glob:command.*\", ",
                "\"event.data.state\" = \"re:^applied$\", ",
                "\"event.data.command_id\" = {{ one_of = [{commands}] }}, ",
                "\"event.agent\" = {{ one_of = [{agents}] }}, ",
                "\"event.generation\" = {{ exists = true }}, ",
                "\"subject.harness\" = \"fake\" }} }}\n",
                "do = [{{ action = \"stop\", target = \"${{event.agent}}\", ",
                "expected_generation = \"${{event.generation}}\" }}]\n",
            ),
            commands = command_values,
            agents = agent_values,
        )
    };
    let rule_id = "stop-applied-send";
    let source_v1 = rule_source(&[send_ids[0].as_str()], &["a1"]);
    let rule_path = fixture.data.join("rules").join("send-stop.toml");
    fs::create_dir_all(rule_path.parent().unwrap()).expect("create private rule directory");
    fs::write(&rule_path, &source_v1).expect("write initial committed-event rule");
    fixture.start();

    let read_rule = |fixture: &Fixture| -> RuleStateView {
        serde_json::from_value(fixture.json(&["rule", "show", rule_id]))
            .expect("typed current/retiring rule view")
    };
    let initial = read_rule(&fixture);
    assert_eq!(initial.id, rule_id);
    assert!(initial.enabled);
    assert!(initial.retiring_revision.is_none());

    let launch_fake = |fixture: &Fixture, expected_agent: &str| {
        let launched = fixture.json(&[
            "launch",
            "--profile",
            "fake",
            "--workspace",
            fixture.workspace.to_str().unwrap(),
        ]);
        assert_eq!(launched["target"], expected_agent);
        assert_eq!(launched["generation"], 1);
        fixture.wait_command(
            launched["id"].as_str().expect("launch command id"),
            "applied",
        );
    };
    let submit_send = |fixture: &Fixture, id: &str, agent: &str, text: &str, wait: bool| {
        let reply = if wait {
            cli(
                &fixture.data,
                &[
                    "--id",
                    id,
                    "send",
                    agent,
                    "--generation",
                    "1",
                    "--wait",
                    "--timeout-seconds",
                    "10",
                    "--",
                    text,
                ],
            )
        } else {
            cli(
                &fixture.data,
                &["--id", id, "send", agent, "--generation", "1", "--", text],
            )
        };
        assert!(
            reply.status.success(),
            "Fake send was not admitted/completed"
        );
        fixture.wait_command(id, "applied");
    };
    let wait_applied_event = |fixture: &Fixture, command_id: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(event) = fixture.events().into_iter().find(|event| {
                event["kind"] == "command.applied" && event["data"]["command_id"] == command_id
            }) {
                return event["seq"].as_i64().expect("applied event sequence");
            }
            assert!(
                std::time::Instant::now() < deadline,
                "command.applied event deadline"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    };
    let wait_rule_cursor = |fixture: &Fixture, through: i64| {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let state = read_rule(fixture);
            if state.current_cursor >= through {
                return state;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "rule event cursor deadline"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    };
    let wait_firing = |fixture: &Fixture, event_seq: i64, revision: i64, agent: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let events = fixture.events();
            if let Some(firing) = events.iter().find(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == rule_id
                    && event["data"]["event_seq"] == event_seq
            }) {
                assert_eq!(firing["data"]["revision"], revision);
                let parent_id = firing["data"]["parent_command"]
                    .as_str()
                    .expect("firing parent id");
                let child_id = firing["data"]["child_command"]
                    .as_str()
                    .expect("firing child id");
                let parent: CoreCommand =
                    serde_json::from_value(fixture.json(&["cmd", "show", parent_id]))
                        .expect("typed Store-only rule parent");
                assert_eq!(parent.verb, "rule");
                assert_eq!(parent.args["rule"], rule_id);
                assert_eq!(
                    parent.args["event_seq"], event_seq,
                    "parent is bound to the triggering committed event"
                );
                assert_eq!(
                    parent.outcome.as_ref().expect("fixed child set")["children"],
                    json!([child_id])
                );
                let child: CoreCommand =
                    serde_json::from_value(fixture.wait_command(child_id, "applied"))
                        .expect("typed rule stop child");
                assert_eq!(child.verb, "stop");
                assert_eq!(child.target.as_deref(), Some(agent));
                assert_eq!(child.expected_generation, Some(1));
                assert_eq!(child.generation, Some(1));
                assert_eq!(fault_dispatches(&fixture.events(), child_id), 1);
                return (firing.clone(), child);
            }
            assert!(
                std::time::Instant::now() < deadline,
                "rule firing event deadline"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    };
    let stop_dispatches = |events: &[Value], agent: &str| {
        events
            .iter()
            .filter(|event| {
                event["kind"] == "command.dispatched"
                    && event["agent"] == agent
                    && event["generation"] == 1
                    && event["data"]["verb"] == "stop"
            })
            .count()
    };

    launch_fake(&fixture, "a1");
    let send1 = send_ids[0].as_str();
    submit_send(&fixture, send1, "a1", "reload-v1-trigger", false);
    let send1_seq = wait_applied_event(&fixture, send1);
    let first = wait_firing(&fixture, send1_seq, initial.revision, "a1");
    let stopped1: AgentGetResponse =
        serde_json::from_value(fixture.json(&["show", "a1"])).expect("typed first stopped Agent");
    assert_eq!(stopped1.agent.state, AgentState::Stopped);

    launch_fake(&fixture, "a2");
    let send2 = send_ids[1].as_str();
    submit_send(&fixture, send2, "a2", "pre-boundary-trigger", true);
    let send2_seq = wait_applied_event(&fixture, send2);
    let old_state_through_boundary = wait_rule_cursor(&fixture, send2_seq);
    let boundary = fixture
        .events()
        .last()
        .and_then(|event| event["seq"].as_i64())
        .expect("captured event high-water");
    let state_at_high_water = wait_rule_cursor(&fixture, boundary);
    let source_v2 = rule_source(
        &[
            send_ids[1].as_str(),
            send_ids[2].as_str(),
            send_ids[3].as_str(),
        ],
        &["a2", "a3", "a4"],
    );
    assert_eq!(old_state_through_boundary.revision, initial.revision);
    assert_eq!(state_at_high_water.revision, initial.revision);
    assert!(state_at_high_water.retiring_revision.is_none());
    fs::write(&rule_path, &source_v2).expect("write semantic rule revision");

    let reload_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let current_v2 = loop {
        let state = read_rule(&fixture);
        if let Some(retiring_revision) = state.retiring_revision {
            assert_eq!(retiring_revision, initial.revision);
            assert_eq!(state.retire_through, Some(boundary));
            assert!(state.retiring_cursor.is_some());
        }
        if state.revision > initial.revision
            && state.retiring_revision.is_none()
            && state.current_cursor >= boundary
        {
            break state;
        }
        assert!(
            std::time::Instant::now() < reload_deadline,
            "semantic rule revision retirement deadline"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(current_v2.retire_through, None);
    let events_after_boundary = fixture.events();
    assert_eq!(stop_dispatches(&events_after_boundary, "a2"), 0);
    assert!(
        !events_after_boundary.iter().any(|event| {
            event["kind"] == "rule.fired"
                && event["data"]["rule"] == rule_id
                && event["data"]["event_seq"] == send2_seq
        }),
        "new definition reinterpreted a pre-boundary event"
    );
    let still_running2: AgentGetResponse = serde_json::from_value(fixture.json(&["show", "a2"]))
        .expect("typed second Agent before new-definition event");
    assert_ne!(still_running2.agent.state, AgentState::Stopped);

    launch_fake(&fixture, "a3");
    let send3 = send_ids[2].as_str();
    submit_send(&fixture, send3, "a3", "reload-v2-trigger", false);
    let send3_seq = wait_applied_event(&fixture, send3);
    let third = wait_firing(&fixture, send3_seq, current_v2.revision, "a3");

    let log_path = fixture.data.join("logs/daemon.log");
    let previous_log_lines = fs::read_to_string(&log_path)
        .expect("read daemon log before invalid reload")
        .lines()
        .count();
    let invalid_source = format!("{source_v2}\nextra_unknown_field = true\n");
    fs::write(&rule_path, invalid_source).expect("write invalid rule revision");
    let invalid_reload_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let reload_diagnostic = loop {
        let log = fs::read_to_string(&log_path).expect("read bounded daemon diagnostics");
        let diagnostic = log
            .lines()
            .skip(previous_log_lines)
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|record| {
                record["error_code"] == "invalid"
                    && record["code"].as_str().is_some_and(|code| {
                        let code = code.to_ascii_lowercase();
                        code.contains("rule") || code.contains("reload")
                    })
            });
        if let Some(diagnostic) = diagnostic {
            break diagnostic;
        }
        let retained = read_rule(&fixture);
        assert_eq!(retained.revision, current_v2.revision);
        assert!(retained.enabled);
        assert!(retained.retiring_revision.is_none());
        assert!(
            std::time::Instant::now() < invalid_reload_deadline,
            "invalid reload diagnostic deadline"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(reload_diagnostic["error_code"], "invalid");
    let retained_v2 = read_rule(&fixture);
    assert_eq!(retained_v2.revision, current_v2.revision);
    assert!(retained_v2.enabled);
    assert!(retained_v2.paused_reason.is_none());
    assert!(retained_v2.retiring_revision.is_none());

    launch_fake(&fixture, "a4");
    let send4 = send_ids[3].as_str();
    submit_send(
        &fixture,
        send4,
        "a4",
        "invalid-reload-keeps-last-valid",
        false,
    );
    let send4_seq = wait_applied_event(&fixture, send4);
    let fourth = wait_firing(&fixture, send4_seq, current_v2.revision, "a4");
    fs::write(&rule_path, &source_v2).expect("restore last valid rule file before restart");

    let events_before_restart = fixture.events();
    let firings_before_restart: Vec<_> = events_before_restart
        .iter()
        .filter(|event| event["kind"] == "rule.fired" && event["data"]["rule"] == rule_id)
        .cloned()
        .collect();
    assert_eq!(firings_before_restart.len(), 3);
    for seq in [send1_seq, send3_seq, send4_seq] {
        assert!(
            firings_before_restart
                .iter()
                .any(|event| event["data"]["event_seq"] == seq)
        );
    }
    assert!(
        !firings_before_restart
            .iter()
            .any(|event| event["data"]["event_seq"] == send2_seq)
    );
    for agent in ["a1", "a3", "a4"] {
        assert_eq!(stop_dispatches(&events_before_restart, agent), 1);
        let view: AgentGetResponse = serde_json::from_value(fixture.json(&["show", agent]))
            .expect("typed rule-stopped Agent before drain");
        assert_eq!(view.agent.state, AgentState::Stopped);
    }
    assert_eq!(stop_dispatches(&events_before_restart, "a2"), 0);
    let unmatched: AgentGetResponse = serde_json::from_value(fixture.json(&["show", "a2"]))
        .expect("typed intentionally unmatched Agent");
    assert_ne!(unmatched.agent.state, AgentState::Stopped);
    let cleanup: CommandSubmitResponse =
        serde_json::from_value(fixture.json(&["stop", "a2", "--generation", "1"]))
            .expect("stop the exact unmatched test Agent before drain");
    assert_eq!(cleanup.agent.as_deref(), Some("a2"));
    assert_eq!(cleanup.generation, Some(1));
    let cleanup_command: CoreCommand =
        serde_json::from_value(fixture.wait_command(&cleanup.command_id, "applied"))
            .expect("applied cleanup stop for unmatched Agent");
    let stopped_unmatched: AgentGetResponse =
        serde_json::from_value(fixture.json(&["show", "a2"])).expect("typed cleanup-stopped Agent");
    assert_eq!(stopped_unmatched.agent.state, AgentState::Stopped);

    fixture.stop(false);
    fixture.start();
    let resumed = read_rule(&fixture);
    assert_eq!(resumed.revision, current_v2.revision);
    assert!(resumed.retiring_revision.is_none());
    assert!(resumed.current_cursor >= retained_v2.current_cursor);
    let events_after_restart = fixture.events();
    let firings_after_restart: Vec<_> = events_after_restart
        .iter()
        .filter(|event| event["kind"] == "rule.fired" && event["data"]["rule"] == rule_id)
        .cloned()
        .collect();
    assert_eq!(firings_after_restart, firings_before_restart);
    for agent in ["a1", "a3", "a4"] {
        assert_eq!(stop_dispatches(&events_after_restart, agent), 1);
    }
    assert_eq!(stop_dispatches(&events_after_restart, "a2"), 1);
    assert_eq!(
        fault_dispatches(&events_after_restart, &cleanup.command_id),
        1
    );
    for (child, expected_agent) in [(&first.1, "a1"), (&third.1, "a3"), (&fourth.1, "a4")] {
        let retained: CoreCommand =
            serde_json::from_value(fixture.json(&["cmd", "show", &child.id]))
                .expect("retained rule stop child after restart");
        assert_eq!(&retained, child);
        assert_eq!(retained.target.as_deref(), Some(expected_agent));
    }
    let cleanup_after_restart: CoreCommand =
        serde_json::from_value(fixture.json(&["cmd", "show", &cleanup.command_id]))
            .expect("retained cleanup stop after restart");
    assert_eq!(cleanup_after_restart, cleanup_command);
    fixture.stop(false);
}

#[test]
fn cli_project_rule_catchup_enforces_scope_limit_and_replays_same_id() {
    let mut fixture = Fixture::new("project rule history catchup");
    let diagnostic_data = fixture.data.clone();
    let bounded_text = |text: &str| text.chars().take(4096).collect::<String>();
    let checked_json = |reply: support::Reply, operation: &str| -> Value {
        let stdout = bounded_text(&reply.stdout);
        let stderr = bounded_text(&reply.stderr);
        assert!(
            reply.status.success(),
            "{operation} exited {:?}; bounded stdout={stdout:?}; bounded stderr={stderr:?}",
            reply.status.code()
        );
        serde_json::from_str(&reply.stdout).unwrap_or_else(|error| {
            panic!(
                "{operation} returned invalid JSON ({error}); bounded stdout={stdout:?}; bounded stderr={stderr:?}"
            )
        })
    };
    assert!(
        fixture
            .git(&["init", "--initial-branch=main"])
            .status
            .success()
    );
    fs::write(fixture.workspace.join("seed.txt"), "project baseline\n")
        .expect("write project baseline");
    assert!(fixture.git(&["add", "--", "seed.txt"]).status.success());
    assert!(
        fixture
            .git(&["commit", "-m", "Project scope fixture"])
            .status
            .success()
    );

    let other_project = fixture.root.join("other repo");
    fs::create_dir(&other_project).expect("create second registered checkout");
    let mut init_other = Command::new("git");
    init_other
        .current_dir(&other_project)
        .args(["init", "--initial-branch=main", "--quiet"])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env(
            "GIT_CONFIG_GLOBAL",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
        )
        .env("GIT_TERMINAL_PROMPT", "0");
    for (name, _) in std::env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("GIT_")
        {
            init_other.env_remove(name);
        }
    }
    assert!(
        run(init_other).status.success(),
        "initialize second checkout"
    );

    // Create the private data root before installing owner configuration.
    fixture.start();
    fixture.stop(false);

    let config = format!(
        concat!(
            "[projects.approved]\n",
            "path = {:?}\nprofiles = [\"fake\"]\nactions = [\"stop\"]\n\n",
            "[projects.other]\n",
            "path = {:?}\nprofiles = [\"fake\"]\nactions = [\"stop\"]\n",
        ),
        fixture.workspace.to_string_lossy(),
        other_project.to_string_lossy(),
    );
    fs::write(fixture.data.join("config.toml"), config)
        .expect("write owner project registrations under the private root");
    let approved_rules = fixture.workspace.join(".eliot").join("rules");
    let other_rules = other_project.join(".eliot").join("rules");
    fs::create_dir_all(&approved_rules).expect("create approved project rule directory");
    fs::create_dir_all(&other_rules).expect("create second project rule directory");

    fixture.start();
    let registration_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let listed: Vec<ProjectRegistrationView> = loop {
        let reply = cli(&diagnostic_data, &["project", "list"]);
        let stdout = bounded_text(&reply.stdout);
        let stderr = bounded_text(&reply.stderr);
        if reply.status.success() {
            let projects: Vec<ProjectRegistrationView> =
                serde_json::from_value(checked_json(reply, "project.list"))
                    .expect("typed owner project registrations");
            if projects.len() == 2 {
                break projects;
            }
        }
        assert!(
            std::time::Instant::now() < registration_deadline,
            "project registration readback deadline; bounded stdout={stdout:?}; bounded stderr={stderr:?}"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    for project in &listed {
        project.validate().expect("valid project registration view");
    }
    let approved_reply = cli(&diagnostic_data, &["project", "show", "approved"]);
    let approved: ProjectRegistrationView =
        serde_json::from_value(checked_json(approved_reply, "project.show approved"))
            .expect("typed approved project readback");
    assert_eq!(approved.id, "approved");
    assert_eq!(approved.profiles, vec!["fake".to_owned()]);
    assert_eq!(approved.actions, vec!["stop".to_owned()]);

    let base = fixture.git(&["rev-parse", "--verify", "HEAD"]);
    assert!(base.status.success());
    let base = base.stdout.trim();
    let launch_reply = cli(
        &diagnostic_data,
        &[
            "launch",
            "--profile",
            "fake",
            "--workspace",
            fixture.workspace.to_str().unwrap(),
            "--worktree",
            "codex/project-scope-history",
            "--base",
            base,
        ],
    );
    let launch = checked_json(launch_reply, "Fake launch");
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let generation_arg = generation.to_string();
    let after_seq = fixture
        .events()
        .last()
        .and_then(|event| event["seq"].as_i64())
        .unwrap_or(0);

    let send_ids = [ulid::Ulid::new().to_string(), ulid::Ulid::new().to_string()];
    for (index, send_id) in send_ids.iter().enumerate() {
        let text = format!("project-history-{index}");
        let send_reply = cli(
            &diagnostic_data,
            &[
                "--id",
                send_id,
                "send",
                &agent,
                "--generation",
                &generation_arg,
                "--wait",
                "--timeout-seconds",
                "10",
                "--",
                &text,
            ],
        );
        let turn: CoreTurn = serde_json::from_value(checked_json(send_reply, "Fake send"))
            .expect("typed Fake history turn");
        assert_eq!(turn.agent, agent);
        assert_eq!(turn.generation, generation);
        assert_eq!(turn.state, TurnState::Completed);
    }
    let history = fixture.events();
    let send_events: Vec<_> = history
        .iter()
        .filter(|event| {
            event["kind"] == "command.applied"
                && send_ids
                    .iter()
                    .any(|id| event["data"]["command_id"] == id.as_str())
        })
        .collect();
    assert_eq!(send_events.len(), 2, "both fixed send events are retained");
    assert!(send_events.iter().all(|event| {
        event["agent"] == agent
            && event["generation"] == generation
            && event["subject"]["harness"] == "fake"
    }));
    let through_seq = send_events
        .iter()
        .filter_map(|event| event["seq"].as_i64())
        .max()
        .expect("finite history range end");
    assert!(through_seq > after_seq);

    let project_rule = |id: &str| {
        format!(
            concat!(
                "[[rule]]\n",
                "id = \"{id}\"\n",
                "on = {{ event = \"command.applied\", match = {{ ",
                "\"event.data.command_id\" = {{ one_of = [\"{first}\", \"{second}\"] }}, ",
                "\"event.agent\" = \"{agent}\", ",
                "\"event.generation\" = {{ exists = true }}, ",
                "\"subject.harness\" = \"fake\" }} }}\n",
                "limit = {{ max = 1, per = \"agent\", window = \"1h\" }}\n",
                "do = [{{ action = \"stop\", target = \"${{event.agent}}\", ",
                "expected_generation = \"${{event.generation}}\" }}]\n",
            ),
            id = id,
            first = send_ids[0],
            second = send_ids[1],
            agent = agent,
        )
    };
    fs::write(
        approved_rules.join("history-stop.toml"),
        project_rule("project-history-stop"),
    )
    .expect("write rule only in the approved checkout");
    fs::write(
        other_rules.join("foreign-stop.toml"),
        project_rule("foreign-history-stop"),
    )
    .expect("write cross-project rule in the second registered checkout");

    let wait_rule = |id: &str, project: &str| -> RuleStateView {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let reply = cli(
                &diagnostic_data,
                &["rule", "show", id, "--project", project],
            );
            let stdout = bounded_text(&reply.stdout);
            let stderr = bounded_text(&reply.stderr);
            if reply.status.success() {
                let state: RuleStateView =
                    serde_json::from_value(checked_json(reply, "project rule.get"))
                        .expect("typed project rule state");
                assert_eq!(state.id, id);
                assert_eq!(state.project.as_deref(), Some(project));
                return state;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "project rule reload deadline; bounded stdout={stdout:?}; bounded stderr={stderr:?}"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    };
    let initial = wait_rule("project-history-stop", "approved");
    wait_rule("foreign-history-stop", "other");
    let no_owner_fallback = cli(&diagnostic_data, &["rule", "show", "project-history-stop"]);
    assert_eq!(no_owner_fallback.status.code(), Some(1));
    let no_owner_stderr = bounded_text(&no_owner_fallback.stderr);
    assert!(no_owner_stderr.contains("not_found"), "{no_owner_stderr}");

    let after_arg = after_seq.to_string();
    let through_arg = through_seq.to_string();
    let denied_id = ulid::Ulid::new().to_string();
    let denied = cli(
        &diagnostic_data,
        &[
            "--id",
            &denied_id,
            "rule",
            "catch-up",
            "foreign-history-stop",
            "--after",
            &after_arg,
            "--through",
            &through_arg,
            "--project",
            "other",
        ],
    );
    let denied_stdout = bounded_text(&denied.stdout);
    let denied_stderr = bounded_text(&denied.stderr);
    assert_eq!(
        denied.status.code(),
        Some(3),
        "cross-project catch-up must be denied before command admission; bounded stdout={denied_stdout:?}; bounded stderr={denied_stderr:?}"
    );
    assert!(
        denied_stdout.is_empty(),
        "denied catch-up returned a command: {denied_stdout:?}"
    );
    assert!(denied_stderr.contains("not_allowed"), "{denied_stderr}");
    let unadmitted = cli(&diagnostic_data, &["cmd", "show", &denied_id]);
    assert_eq!(unadmitted.status.code(), Some(1));
    let unadmitted_stderr = bounded_text(&unadmitted.stderr);
    assert!(
        unadmitted_stderr.contains("not_found"),
        "{unadmitted_stderr}"
    );
    let denial_events = fixture.events();
    assert_eq!(fault_dispatches(&denial_events, &denied_id), 0);
    assert!(!denial_events.iter().any(|event| {
        event["data"]["command_id"] == denied_id
            || event["data"]["parent_command"] == denied_id
            || (event["kind"] == "rule.fired"
                && event["data"]["rule"] == "foreign-history-stop"
                && event["data"]["project"] == "other")
    }));
    let agent_reply = cli(&diagnostic_data, &["show", &agent]);
    let agent_after_denial: AgentGetResponse = serde_json::from_value(checked_json(
        agent_reply,
        "Agent readback after cross-project denial",
    ))
    .expect("typed Agent readback after cross-project denial");
    assert_ne!(agent_after_denial.agent.state, AgentState::Stopped);

    let catchup_id = ulid::Ulid::new().to_string();
    let catchup_args = [
        "--id",
        catchup_id.as_str(),
        "rule",
        "catch-up",
        "project-history-stop",
        "--after",
        after_arg.as_str(),
        "--through",
        through_arg.as_str(),
        "--project",
        "approved",
    ];
    let catchup_reply = cli(&diagnostic_data, &catchup_args);
    let catchup_value = checked_json(catchup_reply, "approved finite history catch-up");
    let catchup: CoreCommand =
        serde_json::from_value(catchup_value).expect("typed finite history catch-up command");
    assert_eq!(catchup.id, catchup_id);
    assert_eq!(catchup.verb, "rule.catch_up");
    assert_eq!(catchup.args["rule"], "project-history-stop");
    assert_eq!(catchup.args["project"], "approved");
    assert_eq!(catchup.args["after_seq"], after_seq);
    assert_eq!(catchup.args["through_seq"], through_seq);
    assert_eq!(catchup.args["revision"], initial.revision);

    let expected_fired_seq = send_events[0]["seq"].as_i64().expect("first matched event");
    let expected_rate_seq = send_events[1]["seq"]
        .as_i64()
        .expect("second matched event");
    let result_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let (fired, rate_rejection) = loop {
        // Rule markers are daemon-owned and may have no Agent; read the unfiltered
        // event stream and bind markers to this parent, revision and input range.
        let events = fixture.events();
        let fired = events.iter().find(|event| {
            event["kind"] == "rule.fired"
                && event["data"]["rule"] == "project-history-stop"
                && event["data"]["project"] == "approved"
                && event["data"]["parent_command"] == catchup_id
                && event["data"]["revision"] == initial.revision
                && event["data"]["event_seq"] == expected_fired_seq
        });
        let rejected = events.iter().find(|event| {
            event["kind"] == "rule.rejected"
                && event["data"]["rule"] == "project-history-stop"
                && event["data"]["project"] == "approved"
                && event["data"]["parent_command"] == catchup_id
                && event["data"]["revision"] == initial.revision
                && event["data"]["event_seq"] == expected_rate_seq
                && event["data"]["code"] == "rate_limited"
        });
        if let (Some(fired), Some(rejected)) = (fired, rejected) {
            break (fired.clone(), rejected.clone());
        }
        if std::time::Instant::now() >= result_deadline {
            let parent_children = catchup
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.get("children"))
                .cloned()
                .unwrap_or(Value::Null);
            let child_readbacks = parent_children
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(|id| {
                    let reply = cli(&diagnostic_data, &["cmd", "show", id]);
                    if reply.status.success() {
                        let value = checked_json(reply, "deadline child readback");
                        json!({
                            "id": value["id"],
                            "state": value["state"],
                            "target": value["target"],
                            "generation": value["generation"],
                            "outcome": value["outcome"],
                        })
                    } else {
                        json!({
                            "id": id,
                            "exit": reply.status.code(),
                            "stderr": bounded_text(&reply.stderr),
                        })
                    }
                })
                .collect::<Vec<_>>();
            let agent_reply = cli(&diagnostic_data, &["show", &agent]);
            let agent_state = if agent_reply.status.success() {
                let value = checked_json(agent_reply, "deadline Agent readback");
                let view: AgentGetResponse =
                    serde_json::from_value(value).expect("typed deadline Agent readback");
                json!({
                    "id": view.agent.id,
                    "generation": view.agent.generation,
                    "state": serde_json::to_value(view.agent.state).unwrap(),
                    "profile": view.agent.profile,
                })
            } else {
                json!({
                    "exit": agent_reply.status.code(),
                    "stderr": bounded_text(&agent_reply.stderr),
                })
            };
            let markers = events
                .iter()
                .filter(|event| {
                    matches!(event["kind"].as_str(), Some("rule.fired" | "rule.rejected"))
                        && event["data"]["rule"] == "project-history-stop"
                        && event["data"]["project"] == "approved"
                        && event["data"]["parent_command"] == catchup_id
                        && event["data"]["revision"] == initial.revision
                        && [expected_fired_seq, expected_rate_seq]
                            .contains(&event["data"]["event_seq"].as_i64().unwrap_or(-1))
                })
                .map(|event| {
                    json!({
                        "seq": event["seq"],
                        "kind": event["kind"],
                        "agent": event["agent"],
                        "generation": event["generation"],
                        "event_seq": event["data"]["event_seq"],
                        "code": event["data"]["code"],
                        "parent_command": event["data"]["parent_command"],
                    })
                })
                .collect::<Vec<_>>();
            let fired_count = markers
                .iter()
                .filter(|event| event["kind"] == "rule.fired")
                .count();
            let rate_limited_count = markers
                .iter()
                .filter(|event| event["kind"] == "rule.rejected" && event["code"] == "rate_limited")
                .count();
            let matched_inputs = send_events
                .iter()
                .map(|event| {
                    json!({
                        "seq": event["seq"],
                        "command_id": event["data"]["command_id"],
                        "agent": event["agent"],
                        "generation": event["generation"],
                        "harness": event["subject"]["harness"],
                    })
                })
                .collect::<Vec<_>>();
            panic!(
                "finite catch-up deadline; parent_id={catchup_id}; parent_state={:?}; parent_args={}; parent_outcome_children={parent_children}; child_readbacks={child_readbacks:?}; agent={agent_state}; matched_inputs={matched_inputs:?}; fired_count={fired_count}; rate_limited_count={rate_limited_count}; observed_markers={markers:?}",
                catchup.state, catchup.args,
            );
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(fired["data"]["event_seq"], expected_fired_seq);
    assert_eq!(rate_rejection["data"]["event_seq"], expected_rate_seq);
    let child_id = fired["data"]["child_command"]
        .as_str()
        .expect("one admitted action child")
        .to_owned();
    assert_eq!(
        catchup.outcome.as_ref().unwrap()["children"],
        json!([child_id]),
        "catch-up parent keeps its fixed admitted-child set"
    );
    let child_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let child: CoreCommand = loop {
        let reply = cli(&diagnostic_data, &["cmd", "show", &child_id]);
        let value = checked_json(reply, "catch-up stop child readback");
        let command: CoreCommand =
            serde_json::from_value(value).expect("typed catch-up stop child");
        if command.state == CommandState::Applied {
            break command;
        }
        assert!(
            matches!(
                &command.state,
                CommandState::Accepted | CommandState::Queued | CommandState::Dispatched
            ),
            "catch-up stop child settled unexpectedly: {:?}",
            command.state
        );
        assert!(
            std::time::Instant::now() < child_deadline,
            "catch-up stop child did not apply"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(child.id, child_id);
    let events_after_catchup = fixture.events();
    assert_eq!(fault_dispatches(&events_after_catchup, &child_id), 1);

    let replay_reply = cli(&diagnostic_data, &catchup_args);
    let replay_value = checked_json(replay_reply, "same-ID history catch-up replay");
    let replay: CoreCommand =
        serde_json::from_value(replay_value).expect("typed idempotent catch-up replay");
    assert_eq!(replay, catchup);
    let after_replay = fixture.events();
    assert_eq!(
        after_replay
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == "project-history-stop"
                    && event["data"]["project"] == "approved"
            })
            .count(),
        1,
        "same external ID did not repeat the action"
    );
    assert_eq!(
        after_replay
            .iter()
            .filter(|event| {
                event["kind"] == "rule.rejected"
                    && event["data"]["rule"] == "project-history-stop"
                    && event["data"]["project"] == "approved"
                    && event["data"]["code"] == "rate_limited"
            })
            .count(),
        1,
        "same external ID did not repeat the stored limit result"
    );
    assert_eq!(fault_dispatches(&after_replay, &child_id), 1);
    let resumed = wait_rule("project-history-stop", "approved");
    assert_eq!(resumed.revision, initial.revision);
    assert!(resumed.current_cursor >= initial.current_cursor);
    fixture.stop(false);
}

#[test]
fn cli_project_rule_send_turn_echo_self_suppression_and_scope() {
    const SEND_TEXT: &str = "project-rule-echo";

    let mut fixture = Fixture::new("project rule send");
    assert!(
        fixture
            .git(&["init", "--initial-branch=main"])
            .status
            .success()
    );
    fs::write(
        fixture.workspace.join("seed.txt"),
        "project SEND baseline\n",
    )
    .expect("write project baseline");
    assert!(fixture.git(&["add", "--", "seed.txt"]).status.success());
    assert!(
        fixture
            .git(&["commit", "-m", "Project SEND fixture"])
            .status
            .success()
    );

    let other_project = fixture.root.join("other repo");
    fs::create_dir(&other_project).expect("create second registered checkout");
    let mut init_other = Command::new("git");
    init_other
        .current_dir(&other_project)
        .args(["init", "--initial-branch=main", "--quiet"])
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env(
            "GIT_CONFIG_GLOBAL",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
        )
        .env("GIT_TERMINAL_PROMPT", "0");
    for (name, _) in std::env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("GIT_")
        {
            init_other.env_remove(name);
        }
    }
    assert!(
        run(init_other).status.success(),
        "initialize second checkout"
    );

    // Initialize the private data root before installing owner project config.
    fixture.start();
    fixture.stop(false);
    let config = format!(
        concat!(
            "[projects.approved]\n",
            "path = {:?}\nprofiles = [\"fake\"]\nactions = [\"send\"]\n\n",
            "[projects.other]\n",
            "path = {:?}\nprofiles = [\"fake\"]\nactions = [\"send\"]\n",
        ),
        fixture.workspace.to_string_lossy(),
        other_project.to_string_lossy(),
    );
    fs::write(fixture.data.join("config.toml"), config).expect("write owner project registrations");
    let approved_rules = fixture.workspace.join(".eliot").join("rules");
    let other_rules = other_project.join(".eliot").join("rules");
    fs::create_dir_all(&approved_rules).expect("create approved project rules");
    fs::create_dir_all(&other_rules).expect("create other project rules");

    fixture.start();
    let base = fixture.git(&["rev-parse", "--verify", "HEAD"]);
    assert!(base.status.success());
    let base = base.stdout.trim();
    let launch = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
        "--worktree",
        "codex/project-send-approved",
        "--base",
        base,
    ]);
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let generation_arg = generation.to_string();
    let after_seq = fixture
        .events()
        .last()
        .and_then(|event| event["seq"].as_i64())
        .unwrap_or(0);

    let seed_id = ulid::Ulid::new().to_string();
    let seed_turn: CoreTurn = serde_json::from_value(fixture.json(&[
        "--id",
        &seed_id,
        "send",
        &agent,
        "--generation",
        &generation_arg,
        "--wait",
        "--timeout-seconds",
        "10",
        "--",
        "project-send-seed",
    ]))
    .expect("typed Fake seed turn");
    assert_eq!(seed_turn.agent, agent);
    assert_eq!(seed_turn.generation, generation);
    assert_eq!(seed_turn.state, TurnState::Completed);
    let seed_history = fixture.events();
    let seed_event = seed_history
        .iter()
        .find(|event| {
            event["kind"] == "turn.completed"
                && event["agent"] == agent
                && event["generation"] == generation
                && event["data"]["command_id"] == seed_id
        })
        .expect("exact Fake seed completion event");
    assert_eq!(seed_event["subject"]["harness"], "fake");
    let through_seq = seed_event["seq"]
        .as_i64()
        .expect("seed completion sequence");
    assert!(through_seq > after_seq);

    let rule_source = |rule_id: &str, command_id: Option<&str>, text: &str| {
        let command_match = command_id.map_or_else(String::new, |command_id| {
            format!(", \"event.data.command_id\" = \"{command_id}\"")
        });
        format!(
            concat!(
                "[[rule]]\n",
                "id = \"{id}\"\n",
                "on = {{ event = \"turn.completed\", match = {{ ",
                "\"subject.harness\" = \"fake\", \"event.agent\" = \"{agent}\"{command_match} }} }}\n",
                "do = [{{ action = \"send\", target = \"${{event.agent}}\", ",
                "expected_generation = \"${{event.generation}}\", text = \"{text}\" }}]\n",
            ),
            id = rule_id,
            agent = agent,
            command_match = command_match,
            text = text,
        )
    };
    fs::write(
        approved_rules.join("reply.toml"),
        rule_source("project-send-echo", None, SEND_TEXT),
    )
    .expect("write project SEND rule without allow_self override");
    fs::write(
        other_rules.join("foreign.toml"),
        rule_source("foreign-send", Some(&seed_id), "foreign-send-must-not-run"),
    )
    .expect("write physical-scope denial rule");

    let wait_rule = |id: &str, project: &str| -> RuleStateView {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let reply = cli(&fixture.data, &["rule", "show", id, "--project", project]);
            if reply.status.success() {
                let state: RuleStateView =
                    serde_json::from_str(&reply.stdout).expect("typed project SEND rule");
                assert_eq!(state.id, id);
                assert_eq!(state.project.as_deref(), Some(project));
                return state;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "project SEND rule load deadline; bounded stderr={:?}",
                reply.stderr.chars().take(4096).collect::<String>()
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    };
    let initial = wait_rule("project-send-echo", "approved");
    wait_rule("foreign-send", "other");
    assert!(initial.current_cursor >= through_seq);

    let after_arg = after_seq.to_string();
    let through_arg = through_seq.to_string();
    let denied_id = ulid::Ulid::new().to_string();
    let denied = cli(
        &fixture.data,
        &[
            "--id",
            &denied_id,
            "rule",
            "catch-up",
            "foreign-send",
            "--after",
            &after_arg,
            "--through",
            &through_arg,
            "--project",
            "other",
        ],
    );
    assert_eq!(
        denied.status.code(),
        Some(3),
        "foreign-project SEND must be denied; bounded stdout={:?}; bounded stderr={:?}",
        denied.stdout.chars().take(4096).collect::<String>(),
        denied.stderr.chars().take(4096).collect::<String>()
    );
    assert!(denied.stderr.contains("not_allowed"));
    let missing_denial = cli(&fixture.data, &["cmd", "show", &denied_id]);
    assert_eq!(missing_denial.status.code(), Some(1));
    assert!(missing_denial.stderr.contains("not_found"));
    assert_ne!(fixture.json(&["show", &agent])["state"], "stopped");
    assert_eq!(fault_dispatches(&fixture.events(), &denied_id), 0);

    let catchup_id = ulid::Ulid::new().to_string();
    let catchup_args = [
        "--id",
        catchup_id.as_str(),
        "rule",
        "catch-up",
        "project-send-echo",
        "--after",
        after_arg.as_str(),
        "--through",
        through_arg.as_str(),
        "--project",
        "approved",
    ];
    let catchup_reply = cli(&fixture.data, &catchup_args);
    assert!(
        catchup_reply.status.success(),
        "approved project SEND catch-up failed; bounded stdout={:?}; bounded stderr={:?}",
        catchup_reply.stdout.chars().take(4096).collect::<String>(),
        catchup_reply.stderr.chars().take(4096).collect::<String>()
    );
    let catchup: CoreCommand =
        serde_json::from_str(&catchup_reply.stdout).expect("typed approved SEND catch-up parent");
    let child_ids: Vec<String> =
        serde_json::from_value(catchup.outcome.as_ref().unwrap()["children"].clone())
            .expect("fixed project SEND child set");
    assert_eq!(child_ids.len(), 1);
    let child_id = child_ids[0].clone();
    let child: CoreCommand = serde_json::from_value(fixture.wait_command(&child_id, "applied"))
        .expect("typed ordinary Router SEND child");
    assert_eq!(child.verb, "send");
    assert_eq!(child.target.as_deref(), Some(agent.as_str()));
    assert_eq!(child.expected_generation, Some(generation));
    let turn_id = child
        .turn_id
        .clone()
        .expect("SEND child retains its exact turn");
    assert_eq!(child.generation, Some(generation));

    let result_request = AgentResultRequest {
        id: agent.clone(),
        generation: Some(generation),
        turn_id: Some(turn_id.clone()),
        limit: 1,
    };
    let result: AgentResultResponse = serde_json::from_value(fixture.json(&[
        "result",
        &agent,
        "--generation",
        &generation_arg,
        "--turn-id",
        &turn_id,
        "--limit",
        "1",
    ]))
    .expect("typed exact SEND turn result");
    result_request
        .validate_response(&result)
        .expect("SEND child and result are bound to the exact turn");
    assert_eq!(result.turns.len(), 1);
    let turn = &result.turns[0];
    assert_eq!(turn.turn_id, turn_id);
    assert_eq!(turn.agent, agent);
    assert_eq!(turn.generation, generation);
    assert_eq!(turn.state, TurnState::Completed);
    assert_eq!(turn.final_text.as_deref(), Some(SEND_TEXT));

    let progress_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let (own_terminal_seq, events_after_self) = loop {
        let events = fixture.events();
        let own_terminal = events.iter().find(|event| {
            event["kind"] == "turn.completed"
                && event["agent"] == agent
                && event["generation"] == generation
                && event["data"]["command_id"] == child_id
                && event["data"]["turn_id"] == turn_id
        });
        if let Some(own_terminal) = own_terminal {
            let terminal_seq = own_terminal["seq"]
                .as_i64()
                .expect("rule SEND terminal seq");
            let reply = cli(
                &fixture.data,
                &["rule", "show", "project-send-echo", "--project", "approved"],
            );
            if reply.status.success() {
                let state: RuleStateView =
                    serde_json::from_str(&reply.stdout).expect("typed project rule cursor");
                if state.current_cursor >= terminal_seq {
                    break (terminal_seq, events);
                }
            }
        }
        assert!(
            std::time::Instant::now() < progress_deadline,
            "project rule did not scan its own SEND terminal event"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    let fires: Vec<_> = events_after_self
        .iter()
        .filter(|event| {
            event["kind"] == "rule.fired"
                && event["data"]["rule"] == "project-send-echo"
                && event["data"]["project"] == "approved"
        })
        .collect();
    assert_eq!(
        fires.len(),
        1,
        "default allow_self suppressed send feedback"
    );
    assert_eq!(fires[0]["data"]["event_seq"], through_seq);
    assert_eq!(fires[0]["data"]["child_command"], child_id);
    assert!(!events_after_self.iter().any(|event| {
        event["kind"] == "rule.rejected"
            && event["data"]["rule"] == "project-send-echo"
            && event["data"]["project"] == "approved"
            && event["data"]["event_seq"] == own_terminal_seq
    }));

    let replay: CoreCommand = serde_json::from_value(fixture.json(&catchup_args))
        .expect("same-ID project SEND catch-up replay");
    assert_eq!(replay, catchup);
    let after_replay = fixture.events();
    assert_eq!(
        after_replay
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == "project-send-echo"
                    && event["data"]["project"] == "approved"
            })
            .count(),
        1
    );
    assert_eq!(fault_dispatches(&after_replay, &child_id), 1);

    let stop_id = ulid::Ulid::new().to_string();
    let cleanup: CommandSubmitResponse = serde_json::from_value(fixture.json(&[
        "--id",
        &stop_id,
        "stop",
        &agent,
        "--generation",
        &generation_arg,
    ]))
    .expect("owner stop for completed project SEND Agent");
    fixture.wait_command(&cleanup.command_id, "applied");
    fixture.stop(false);
}

#[test]
fn cli_fake_rule_send_unknown_after_dispatch_is_not_replayed() {
    let mut fixture = Fixture::new("rule send unknown");
    fixture.start();
    fixture.stop(false);
    let rules = fixture.data.join("rules");
    fs::create_dir_all(&rules).expect("create global rule directory");
    fixture.start();

    let launch = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
    ]);
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let generation_arg = generation.to_string();
    let after_seq = fixture
        .events()
        .last()
        .and_then(|event| event["seq"].as_i64())
        .unwrap_or(0);
    let seed_id = ulid::Ulid::new().to_string();
    let seed_text = "rule-unknown-seed";
    let seed_turn: CoreTurn = serde_json::from_value(fixture.json(&[
        "--id",
        &seed_id,
        "send",
        &agent,
        "--generation",
        &generation_arg,
        "--wait",
        "--timeout-seconds",
        "10",
        "--",
        seed_text,
    ]))
    .expect("typed Fake seed turn");
    assert_eq!(seed_turn.agent, agent);
    assert_eq!(seed_turn.generation, generation);
    assert_eq!(seed_turn.state, TurnState::Completed);
    let seed_event = fixture
        .events()
        .into_iter()
        .find(|event| {
            event["kind"] == "turn.completed"
                && event["agent"] == agent
                && event["generation"] == generation
                && event["data"]["command_id"] == seed_id
        })
        .expect("exact Fake seed completion event");
    let through_seq = seed_event["seq"]
        .as_i64()
        .expect("seed completion sequence");
    assert!(through_seq > after_seq);

    fs::write(
        rules.join("unknown-send.toml"),
        format!(
            concat!(
                "[[rule]]\n",
                "id = \"unknown-send\"\n",
                "on = {{ event = \"turn.completed\", match = {{ ",
                "\"subject.harness\" = \"fake\", \"event.agent\" = \"{agent}\" }} }}\n",
                "do = [{{ action = \"send\", target = \"${{event.agent}}\", ",
                "expected_generation = \"${{event.generation}}\", ",
                "text = \"[fake:malformed-after-dispatch]\" }}]\n",
            ),
            agent = agent,
        ),
    )
    .expect("write rule using the existing Fake uncertain-after-dispatch input");

    let load_deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let reply = cli(&fixture.data, &["rule", "show", "unknown-send"]);
        if reply.status.success() {
            break;
        }
        assert!(
            std::time::Instant::now() < load_deadline,
            "unknown-send rule load deadline; bounded stderr={:?}",
            reply.stderr.chars().take(4096).collect::<String>()
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    let after_arg = after_seq.to_string();
    let through_arg = through_seq.to_string();
    let catchup_id = ulid::Ulid::new().to_string();
    let catchup_args = [
        "--id",
        catchup_id.as_str(),
        "rule",
        "catch-up",
        "unknown-send",
        "--after",
        after_arg.as_str(),
        "--through",
        through_arg.as_str(),
    ];
    let catchup_reply = cli(&fixture.data, &catchup_args);
    assert!(
        catchup_reply.status.success(),
        "Fake unknown catch-up failed; bounded stdout={:?}; bounded stderr={:?}",
        catchup_reply.stdout.chars().take(4096).collect::<String>(),
        catchup_reply.stderr.chars().take(4096).collect::<String>()
    );
    let catchup: CoreCommand =
        serde_json::from_str(&catchup_reply.stdout).expect("typed unknown-send catch-up parent");
    let child_ids: Vec<String> =
        serde_json::from_value(catchup.outcome.as_ref().unwrap()["children"].clone())
            .expect("fixed unknown-send child set");
    assert_eq!(child_ids.len(), 1);
    let child_id = child_ids[0].clone();

    let unknown_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let unknown: CoreCommand = loop {
        let reply = cli(&fixture.data, &["cmd", "show", &child_id]);
        if reply.status.success() {
            let command: CoreCommand =
                serde_json::from_str(&reply.stdout).expect("typed Fake child readback");
            if command.state == CommandState::Unknown {
                break command;
            }
            assert!(matches!(
                command.state,
                CommandState::Accepted | CommandState::Queued | CommandState::Dispatched
            ));
        }
        assert!(
            std::time::Instant::now() < unknown_deadline,
            "Fake child did not settle to Unknown; bounded stderr={:?}",
            reply.stderr.chars().take(4096).collect::<String>()
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(unknown.id, child_id);
    assert_eq!(unknown.target.as_deref(), Some(agent.as_str()));
    assert_eq!(unknown.expected_generation, Some(generation));
    assert_eq!(unknown.turn_id, None);
    assert_eq!(fault_dispatches(&fixture.events(), &child_id), 1);

    let replay: CoreCommand = serde_json::from_value(fixture.json(&catchup_args))
        .expect("same-ID Unknown catch-up replay");
    assert_eq!(replay, catchup);
    let unknown_after_replay: CoreCommand =
        serde_json::from_value(fixture.json(&["cmd", "show", &child_id]))
            .expect("retained Unknown child after replay");
    assert_eq!(unknown_after_replay.id, unknown.id);
    assert_eq!(unknown_after_replay.principal, unknown.principal);
    assert_eq!(unknown_after_replay.method, unknown.method);
    assert_eq!(unknown_after_replay.verb, unknown.verb);
    assert_eq!(unknown_after_replay.target, unknown.target);
    assert_eq!(
        unknown_after_replay.expected_generation,
        unknown.expected_generation
    );
    assert_eq!(unknown_after_replay.generation, unknown.generation);
    assert_eq!(unknown_after_replay.args, unknown.args);
    assert_eq!(unknown_after_replay.turn_id, unknown.turn_id);
    assert_eq!(unknown_after_replay.request_id, unknown.request_id);
    assert_eq!(unknown_after_replay.created_at, unknown.created_at);
    assert_eq!(unknown_after_replay.probe, unknown.probe);
    assert_eq!(unknown_after_replay.state, CommandState::Unknown);
    assert_eq!(
        unknown_after_replay
            .outcome
            .as_ref()
            .and_then(|outcome| outcome["reason"].as_str()),
        Some("owner_marked_lost"),
        "same-ID replay retained the exact Unknown reason"
    );
    let events_after_replay = fixture.events();
    assert_eq!(fault_dispatches(&events_after_replay, &child_id), 1);
    assert_eq!(
        events_after_replay
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == "unknown-send"
                    && event["data"]["parent_command"] == catchup_id
            })
            .count(),
        1,
        "same-ID Unknown replay did not repeat its firing"
    );
    fixture.stop(true);
}

#[test]
fn cli_fake_history_catchup_dispatches_siblings_in_order_and_replays() {
    let mut fixture = Fixture::new("ordered rule catch-up");
    fixture.start();
    fixture.stop(false);

    let rules = fixture.data.join("rules");
    fs::create_dir_all(&rules).expect("create global rule directory");
    let second_workspace = fixture.root.join("second workspace");
    fs::create_dir(&second_workspace).expect("create second Fake workspace");
    fixture.start();

    let workspaces = [fixture.workspace.clone(), second_workspace];
    let mut agents = Vec::new();
    for workspace in &workspaces {
        let launched = fixture.json(&[
            "launch",
            "--profile",
            "fake",
            "--workspace",
            workspace.to_str().unwrap(),
        ]);
        agents.push((
            launched["target"].as_str().expect("Fake Agent").to_owned(),
            launched["generation"].as_i64().expect("Fake generation"),
        ));
    }
    assert_ne!(agents[0].0, agents[1].0);
    let after_seq = fixture
        .events()
        .last()
        .and_then(|event| event["seq"].as_i64())
        .unwrap_or(0);

    let send_ids = [ulid::Ulid::new().to_string(), ulid::Ulid::new().to_string()];
    for (index, (agent, generation)) in agents.iter().enumerate() {
        let text = format!("ordered-history-{index}");
        let turn: CoreTurn = serde_json::from_value(fixture.json(&[
            "--id",
            &send_ids[index],
            "send",
            agent,
            "--generation",
            &generation.to_string(),
            "--wait",
            "--timeout-seconds",
            "10",
            "--",
            &text,
        ]))
        .expect("typed ordered-history Fake turn");
        assert_eq!(turn.agent, *agent);
        assert_eq!(turn.generation, *generation);
        assert_eq!(turn.state, TurnState::Completed);
    }

    let history = fixture.events();
    let send_events: Vec<_> = history
        .iter()
        .filter(|event| {
            event["kind"] == "command.applied"
                && send_ids
                    .iter()
                    .any(|id| event["data"]["command_id"] == id.as_str())
        })
        .collect();
    assert_eq!(send_events.len(), 2);
    for (index, event) in send_events.iter().enumerate() {
        assert_eq!(event["agent"], agents[index].0);
        assert_eq!(event["generation"], agents[index].1);
        assert_eq!(event["subject"]["harness"], "fake");
    }
    let through_seq = send_events
        .iter()
        .filter_map(|event| event["seq"].as_i64())
        .max()
        .expect("finite ordered-history range end");
    assert!(through_seq > after_seq);

    let command_values = send_ids
        .iter()
        .map(|id| format!("\"{id}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let agent_values = agents
        .iter()
        .map(|(agent, _)| format!("\"{agent}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let source = format!(
        concat!(
            "[[rule]]\n",
            "id = \"ordered-stop\"\n",
            "on = {{ event = \"command.applied\", match = {{ ",
            "\"event.data.command_id\" = {{ one_of = [{commands}] }}, ",
            "\"event.agent\" = {{ one_of = [{agents}] }}, ",
            "\"event.generation\" = {{ exists = true }}, ",
            "\"subject.harness\" = \"fake\" }} }}\n",
            "limit = {{ max = 2, per = \"rule\", window = \"1h\" }}\n",
            "do = [{{ action = \"stop\", target = \"${{event.agent}}\", ",
            "expected_generation = \"${{event.generation}}\" }}]\n",
        ),
        commands = command_values,
        agents = agent_values,
    );
    fs::write(rules.join("ordered-stop.toml"), source).expect("write ordered sibling history rule");

    let load_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let state: RuleStateView = loop {
        let reply = cli(&fixture.data, &["rule", "show", "ordered-stop"]);
        if reply.status.success() {
            break serde_json::from_str(&reply.stdout).expect("typed ordered rule state");
        }
        assert!(
            std::time::Instant::now() < load_deadline,
            "ordered history rule load deadline; stderr: {}",
            reply.stderr
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert!(state.current_cursor >= through_seq);

    let after_arg = after_seq.to_string();
    let through_arg = through_seq.to_string();
    let catchup_id = ulid::Ulid::new().to_string();
    let catchup_args = [
        "--id",
        catchup_id.as_str(),
        "rule",
        "catch-up",
        "ordered-stop",
        "--after",
        after_arg.as_str(),
        "--through",
        through_arg.as_str(),
    ];
    let catchup_reply = cli(&fixture.data, &catchup_args);
    assert!(
        catchup_reply.status.success(),
        "ordered sibling catch-up failed; bounded stdout={:?}; bounded stderr={:?}",
        catchup_reply.stdout.chars().take(4096).collect::<String>(),
        catchup_reply.stderr.chars().take(4096).collect::<String>()
    );
    let catchup: CoreCommand =
        serde_json::from_str(&catchup_reply.stdout).expect("typed ordered catch-up parent");
    assert_eq!(catchup.id, catchup_id);
    assert_eq!(catchup.args["rule"], "ordered-stop");
    assert_eq!(catchup.args["after_seq"], after_seq);
    assert_eq!(catchup.args["through_seq"], through_seq);
    assert_eq!(catchup.args["revision"], state.revision);
    let child_ids: Vec<String> =
        serde_json::from_value(catchup.outcome.as_ref().unwrap()["children"].clone())
            .expect("fixed ordered catch-up child IDs");
    assert_eq!(child_ids.len(), 2, "both ordered stops were admitted");

    let mut children = Vec::new();
    for child_id in &child_ids {
        let child: CoreCommand = serde_json::from_value(fixture.wait_command(child_id, "applied"))
            .expect("typed completed ordered stop child");
        children.push(child);
    }
    for (index, child) in children.iter().enumerate() {
        assert_eq!(child.id, child_ids[index]);
        assert_eq!(child.state, CommandState::Applied);
        assert_eq!(child.target.as_deref(), Some(agents[index].0.as_str()));
        assert_eq!(child.expected_generation, Some(agents[index].1));
    }

    let events = fixture.events();
    let mut fired: Vec<_> = events
        .iter()
        .filter(|event| {
            event["kind"] == "rule.fired"
                && event["data"]["rule"] == "ordered-stop"
                && event["data"]["parent_command"] == catchup_id
        })
        .collect();
    fired.sort_by_key(|event| event["data"]["event_seq"].as_i64().unwrap_or(i64::MAX));
    assert_eq!(fired.len(), 2);
    for index in 0..2 {
        assert_eq!(fired[index]["data"]["event_seq"], send_events[index]["seq"]);
        assert_eq!(fired[index]["data"]["child_command"], child_ids[index]);
        assert_eq!(fault_dispatches(&events, &child_ids[index]), 1);
        let stopped: AgentGetResponse =
            serde_json::from_value(fixture.json(&["show", &agents[index].0]))
                .expect("typed stopped Fake Agent");
        assert_eq!(stopped.agent.state, AgentState::Stopped);
        assert_eq!(stopped.agent.generation, agents[index].1);
    }
    let first_applied_seq = events
        .iter()
        .find(|event| {
            event["kind"] == "command.applied" && event["data"]["command_id"] == child_ids[0]
        })
        .and_then(|event| event["seq"].as_i64())
        .expect("first stop applied event");
    let second_dispatched_seq = events
        .iter()
        .find(|event| {
            event["kind"] == "command.dispatched" && event["data"]["command_id"] == child_ids[1]
        })
        .and_then(|event| event["seq"].as_i64())
        .expect("second stop dispatched event");
    assert!(
        second_dispatched_seq > first_applied_seq,
        "second sibling dispatched at {second_dispatched_seq} before predecessor applied at {first_applied_seq}"
    );

    let replay: CoreCommand = serde_json::from_value(fixture.json(&catchup_args))
        .expect("same-ID ordered catch-up replay");
    assert_eq!(replay, catchup);
    let after_replay = fixture.events();
    assert_eq!(
        after_replay
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == "ordered-stop"
                    && event["data"]["parent_command"] == catchup_id
            })
            .count(),
        2,
        "same-ID replay did not duplicate firing markers"
    );
    for child_id in &child_ids {
        assert_eq!(fault_dispatches(&after_replay, child_id), 1);
    }
    fixture.stop(false);
}

#[test]
fn cli_fake_event_rule_interrupt_binds_exact_turn_and_rejects_stale_catchup() {
    const HOLD_TURN: &str = "[fake:hold-turn]";
    const RULE_ID: &str = "interrupt-first-held-turn";
    const HISTORY_RULE_ID: &str = "interrupt-first-held-turn-history";

    let mut fixture = Fixture::new("event rule interrupt");
    fixture.start();
    fixture.stop(false);

    let first_send_id = ulid::Ulid::new().to_string();
    let rules = fixture.data.join("rules");
    fs::create_dir_all(&rules).expect("create global event-rule directory");
    let rule_source = |rule: &str| {
        format!(
            concat!(
                "[[rule]]\n",
                "id = \"{rule}\"\n",
                "on = {{ event = \"turn.started\", match = {{ ",
                "\"subject.harness\" = \"fake\", ",
                "\"event.data.command_id\" = \"{command}\" }} }}\n",
                "do = [{{ action = \"interrupt\", target = \"${{event.agent}}\", ",
                "expected_generation = \"${{event.generation}}\", ",
                "expected_turn_id = \"${{data.turn_id}}\" }}]\n",
            ),
            rule = rule,
            command = first_send_id,
        )
    };
    fs::write(rules.join("interrupt.toml"), rule_source(RULE_ID))
        .expect("write event-bound exact-turn interrupt rule");
    fixture.start();

    let rule_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let initial_rule: RuleStateView = loop {
        let reply = cli(&fixture.data, &["rule", "show", RULE_ID]);
        if reply.status.success() {
            break serde_json::from_str(&reply.stdout).expect("typed event interrupt rule");
        }
        assert!(
            std::time::Instant::now() < rule_deadline,
            "event interrupt rule load deadline; bounded stderr={:?}",
            reply.stderr.chars().take(4096).collect::<String>()
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(initial_rule.id, RULE_ID);
    assert!(initial_rule.enabled);

    let launch = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
    ]);
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let generation_arg = generation.to_string();

    let first_reply = cli(
        &fixture.data,
        &[
            "--id",
            &first_send_id,
            "send",
            &agent,
            "--generation",
            &generation_arg,
            "--wait",
            "--timeout-seconds",
            "10",
            "--",
            HOLD_TURN,
        ],
    );
    assert!(
        first_reply.status.success(),
        "held Fake turn did not complete through event interrupt; bounded stdout={:?}; bounded stderr={:?}",
        first_reply.stdout.chars().take(4096).collect::<String>(),
        first_reply.stderr.chars().take(4096).collect::<String>()
    );
    let first_send: CoreTurn =
        serde_json::from_str(&first_reply.stdout).expect("typed interrupted Fake turn");
    assert_eq!(first_send.agent, agent);
    assert_eq!(first_send.generation, generation);
    assert_eq!(first_send.state, TurnState::Completed);
    assert_eq!(first_send.status.as_deref(), Some("interrupted"));

    let after_first = fixture.events();
    let first_started = after_first
        .iter()
        .find(|event| {
            event["kind"] == "turn.started" && event["data"]["command_id"] == first_send_id
        })
        .expect("first held-turn start event");
    let first_event_seq = first_started["seq"].as_i64().expect("first start sequence");
    let first_turn_id = first_started["data"]["turn_id"]
        .as_str()
        .expect("first exact turn ID")
        .to_owned();
    assert_eq!(first_send.turn_id, first_turn_id);
    assert_eq!(fault_dispatches(&after_first, &first_send_id), 1);
    assert_eq!(
        after_first
            .iter()
            .filter(|event| {
                event["kind"] == "turn.completed"
                    && event["data"]["command_id"] == first_send_id
                    && event["data"]["turn_id"] == first_turn_id
            })
            .count(),
        1,
        "interrupt normalized one terminal event for the exact held turn"
    );
    let first_fires: Vec<_> = after_first
        .iter()
        .filter(|event| {
            event["kind"] == "rule.fired"
                && event["data"]["rule"] == RULE_ID
                && event["data"]["event_seq"] == first_event_seq
        })
        .collect();
    assert_eq!(first_fires.len(), 1, "only the first held turn matched");
    let interrupt_id = first_fires[0]["data"]["child_command"]
        .as_str()
        .expect("interrupt child ID")
        .to_owned();
    let interrupt: CoreCommand =
        serde_json::from_value(fixture.wait_command(&interrupt_id, "applied"))
            .expect("applied ordinary Router interrupt child");
    assert_eq!(interrupt.verb, "interrupt");
    assert_eq!(interrupt.target.as_deref(), Some(agent.as_str()));
    assert_eq!(interrupt.expected_generation, Some(generation));
    assert_eq!(
        interrupt
            .outcome
            .as_ref()
            .expect("retained interrupt outcome")["turn_id"],
        first_turn_id,
        "interrupt outcome retains the event-bound turn"
    );
    assert_eq!(interrupt.args, json!({}));
    assert!(fixture.json(&["show", &agent])["active_turn"].is_null());

    // Register this rule only after its matching event and live interrupt have
    // completed, so its explicit catch-up has no pre-existing deterministic child.
    fs::write(
        rules.join("interrupt-history.toml"),
        rule_source(HISTORY_RULE_ID),
    )
    .expect("write catch-up-only interrupt rule");
    let history_rule_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let history_rule: RuleStateView = loop {
        let reply = cli(&fixture.data, &["rule", "show", HISTORY_RULE_ID]);
        if reply.status.success() {
            break serde_json::from_str(&reply.stdout).expect("typed catch-up-only rule");
        }
        assert!(
            std::time::Instant::now() < history_rule_deadline,
            "catch-up-only rule load deadline; bounded stderr={:?}",
            reply.stderr.chars().take(4096).collect::<String>()
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(history_rule.id, HISTORY_RULE_ID);
    assert!(history_rule.enabled);
    assert!(history_rule.current_cursor >= first_event_seq);
    assert!(!fixture.events().iter().any(|event| {
        event["kind"] == "rule.fired"
            && event["data"]["rule"] == HISTORY_RULE_ID
            && event["data"]["event_seq"] == first_event_seq
    }));

    let second_send_id = ulid::Ulid::new().to_string();
    let second_reply = cli(
        &fixture.data,
        &[
            "--id",
            &second_send_id,
            "send",
            &agent,
            "--generation",
            &generation_arg,
            "--",
            HOLD_TURN,
        ],
    );
    assert!(
        matches!(second_reply.status.code(), Some(0 | 6)),
        "second held-turn submission failed; bounded stdout={:?}; bounded stderr={:?}",
        second_reply.stdout.chars().take(4096).collect::<String>(),
        second_reply.stderr.chars().take(4096).collect::<String>()
    );
    let second_submit: CommandSubmitResponse = serde_json::from_str(&second_reply.stdout)
        .expect("typed second otherwise-valid held-turn submission");
    assert_eq!(second_submit.command_id, second_send_id);
    assert_eq!(second_submit.agent.as_deref(), Some(agent.as_str()));
    assert_eq!(second_submit.generation, Some(generation));
    assert_eq!(
        second_reply.status.code(),
        Some(match second_submit.state {
            CommandState::Accepted | CommandState::Queued | CommandState::Applied => 0,
            CommandState::Dispatched => 6,
            state => panic!("second held-turn submission returned {state:?}"),
        }),
        "second held-turn CLI exit matches its retained command state"
    );

    let second_started_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let second_started = loop {
        let events = fixture.events();
        if let Some(event) = events.iter().find(|event| {
            event["kind"] == "turn.started" && event["data"]["command_id"] == second_send_id
        }) {
            break event.clone();
        }
        assert!(
            std::time::Instant::now() < second_started_deadline,
            "second held-turn start deadline"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    let second_event_seq = second_started["seq"]
        .as_i64()
        .expect("second start sequence");
    let second_turn_id = second_started["data"]["turn_id"]
        .as_str()
        .expect("second exact turn ID")
        .to_owned();
    assert!(second_event_seq > first_event_seq);
    assert_ne!(second_turn_id, first_turn_id);
    let second_command: CoreCommand =
        serde_json::from_value(fixture.wait_command(&second_send_id, "applied"))
            .expect("second held SEND remains an associated turn");
    assert_eq!(
        second_command.turn_id.as_deref(),
        Some(second_turn_id.as_str())
    );
    assert_eq!(
        fixture.json(&["show", &agent])["active_turn"],
        second_turn_id
    );

    let cursor_deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let live_state: RuleStateView =
            serde_json::from_value(fixture.json(&["rule", "show", RULE_ID]))
                .expect("event interrupt rule cursor");
        let history_state: RuleStateView =
            serde_json::from_value(fixture.json(&["rule", "show", HISTORY_RULE_ID]))
                .expect("catch-up-only rule cursor");
        if live_state.current_cursor >= second_event_seq
            && history_state.current_cursor >= second_event_seq
        {
            assert_eq!(history_state.revision, history_rule.revision);
            break;
        }
        assert!(
            std::time::Instant::now() < cursor_deadline,
            "event interrupt rule did not scan the second nonmatching turn"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    let after_arg = (first_event_seq - 1).to_string();
    let through_arg = first_event_seq.to_string();
    let catchup_id = ulid::Ulid::new().to_string();
    let catchup_args = [
        "--id",
        catchup_id.as_str(),
        "rule",
        "catch-up",
        HISTORY_RULE_ID,
        "--after",
        after_arg.as_str(),
        "--through",
        through_arg.as_str(),
    ];
    let catchup_reply = cli(&fixture.data, &catchup_args);
    assert_eq!(
        catchup_reply.status.code(),
        Some(0),
        "catch-up-only stale-turn rejection returns a typed parent; bounded stdout={:?}; bounded stderr={:?}",
        catchup_reply.stdout.chars().take(4096).collect::<String>(),
        catchup_reply.stderr.chars().take(4096).collect::<String>()
    );
    let catchup: CoreCommand = serde_json::from_str(&catchup_reply.stdout)
        .expect("typed stale-turn finite history catch-up parent");
    assert_eq!(catchup.id, catchup_id);
    assert_eq!(catchup.verb, "rule.catch_up");
    assert_eq!(catchup.state, CommandState::Applied);
    assert_eq!(catchup.args["rule"], HISTORY_RULE_ID);
    assert_eq!(catchup.args["after_seq"], first_event_seq - 1);
    assert_eq!(catchup.args["through_seq"], first_event_seq);
    assert_eq!(catchup.args["revision"], history_rule.revision);
    let catchup_outcome = catchup.outcome.as_ref().expect("rejected catch-up outcome");
    assert_eq!(catchup_outcome["children"], json!([]));
    assert!(catchup_outcome.get("code").is_none());

    let after_catchup = fixture.events();
    assert!(after_catchup.iter().any(|event| {
        event["kind"] == "rule.rejected"
            && event["data"]["rule"] == HISTORY_RULE_ID
            && event["data"]["parent_command"] == catchup_id
            && event["data"]["event_seq"] == first_event_seq
            && event["data"]["code"] == "stale_generation"
    }));
    assert_eq!(
        fixture.json(&["show", &agent])["active_turn"],
        second_turn_id
    );
    assert_eq!(
        after_catchup
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == HISTORY_RULE_ID
                    && event["data"]["event_seq"] == second_event_seq
            })
            .count(),
        0,
        "the second held turn did not match the first command's event filter"
    );

    let replay_reply = cli(&fixture.data, &catchup_args);
    assert_eq!(
        replay_reply.status.code(),
        Some(0),
        "same-ID stale catch-up replay returns the retained parent; bounded stdout={:?}; bounded stderr={:?}",
        replay_reply.stdout.chars().take(4096).collect::<String>(),
        replay_reply.stderr.chars().take(4096).collect::<String>()
    );
    let replay: CoreCommand =
        serde_json::from_str(&replay_reply.stdout).expect("same-ID stale-turn catch-up replay");
    assert_eq!(replay, catchup);
    let after_replay = fixture.events();
    assert_eq!(
        after_replay
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == RULE_ID
                    && event["data"]["event_seq"] == first_event_seq
            })
            .count(),
        1,
        "same-ID catch-up replay did not repeat the original live interrupt"
    );
    assert_eq!(
        after_replay
            .iter()
            .filter(|event| {
                event["kind"] == "rule.fired"
                    && event["data"]["rule"] == HISTORY_RULE_ID
                    && event["data"]["event_seq"] == first_event_seq
            })
            .count(),
        0,
        "catch-up-only rule did not fire live on its pre-registration event"
    );
    assert_eq!(
        after_replay
            .iter()
            .filter(|event| {
                event["kind"] == "rule.rejected"
                    && event["data"]["rule"] == HISTORY_RULE_ID
                    && event["data"]["parent_command"] == catchup_id
                    && event["data"]["event_seq"] == first_event_seq
            })
            .count(),
        1,
        "same-ID stale catch-up replay did not repeat rejection"
    );
    assert_eq!(fault_dispatches(&after_replay, &interrupt_id), 1);
    assert_eq!(fault_dispatches(&after_replay, &catchup_id), 0);
    assert_eq!(fault_dispatches(&after_replay, &second_send_id), 1);
    assert_eq!(
        fixture.json(&["show", &agent])["active_turn"],
        second_turn_id
    );

    let stop_id = ulid::Ulid::new().to_string();
    let stop: CommandSubmitResponse = serde_json::from_value(fixture.json(&[
        "--id",
        &stop_id,
        "stop",
        &agent,
        "--generation",
        &generation_arg,
    ]))
    .expect("owner stop for second held turn");
    fixture.wait_command(&stop.command_id, "applied");
    fixture.stop(false);
}

#[test]
fn cli_fake_timer_send_scheduled_and_manual_replay_keep_exact_turns() {
    const SEND_TEXT: &str = "timer-send-echo";

    let mut fixture = Fixture::new("timer send");
    fixture.start();
    fixture.stop(false);
    let rules = fixture.data.join("rules");
    fs::create_dir_all(&rules).expect("create timer rule directory");
    fixture.start();

    let launch = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture.workspace.to_str().unwrap(),
    ]);
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let generation_arg = generation.to_string();

    let period_ms = 60_000_i64;
    let now_ms = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after the Unix epoch")
            .as_millis(),
    )
    .expect("epoch milliseconds fit signed range");
    let anchor_ms = now_ms - period_ms + 12_000;
    let rule_id = "periodic-send";
    let rule_source = format!(
        concat!(
            "[[rule]]\n",
            "id = \"{id}\"\n",
            "on = {{ every = \"1m\", anchor = {anchor} }}\n",
            "do = [{{ action = \"send\", target = \"{agent}\", ",
            "expected_generation = {generation}, text = \"{text}\" }}]\n",
        ),
        id = rule_id,
        anchor = anchor_ms,
        agent = agent,
        generation = generation,
        text = SEND_TEXT,
    );
    fs::write(rules.join("periodic-send.toml"), rule_source)
        .expect("write literal timer SEND action");

    let load_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let timer_before: TimerStateView = loop {
        let reply = cli(&fixture.data, &["timer", "show", rule_id]);
        if reply.status.success() {
            break serde_json::from_str(&reply.stdout).expect("typed scheduled SEND timer");
        }
        assert!(
            std::time::Instant::now() < load_deadline,
            "timer SEND load deadline; bounded stderr={:?}",
            reply.stderr.chars().take(4096).collect::<String>()
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(timer_before.id, rule_id);
    assert_eq!(
        timer_before.schedule,
        TimerSchedule::Interval {
            period_ms,
            anchor: Some(anchor_ms),
        }
    );
    assert_eq!(timer_before.last_key, None);
    let first_due = timer_before.next_due_at.expect("scheduled SEND next due");
    assert!(first_due > now_ms);

    let manual_id = ulid::Ulid::new().to_string();
    let manual_args = ["--id", manual_id.as_str(), "timer", "run", rule_id];
    let manual_parent: CoreCommand =
        serde_json::from_value(fixture.json(&manual_args)).expect("typed manual SEND timer parent");
    assert_eq!(manual_parent.id, manual_id);
    assert_eq!(manual_parent.verb, "timer.run");
    assert_eq!(manual_parent.args["rule"], rule_id);
    let manual_children: Vec<String> =
        serde_json::from_value(manual_parent.outcome.as_ref().unwrap()["children"].clone())
            .expect("manual SEND fixed child set");
    assert_eq!(manual_children.len(), 1);
    let manual_child_id = manual_children[0].clone();
    let manual_child: CoreCommand =
        serde_json::from_value(fixture.wait_command(&manual_child_id, "applied"))
            .expect("applied manual SEND child");
    let manual_turn =
        wait_fake_send_turn_result(&fixture, &agent, generation, &manual_child, SEND_TEXT);
    let manual_turn_id = manual_turn.turn_id.clone();

    let replay: CoreCommand = serde_json::from_value(fixture.json(&manual_args))
        .expect("same-ID manual SEND timer replay");
    assert_eq!(replay, manual_parent);
    assert_eq!(fault_dispatches(&fixture.events(), &manual_child_id), 1);
    let timer_after_manual: TimerStateView =
        serde_json::from_value(fixture.json(&["timer", "show", rule_id]))
            .expect("timer state after manual SEND");
    assert_eq!(timer_after_manual.last_key, timer_before.last_key);
    assert_eq!(timer_after_manual.next_due_at, timer_before.next_due_at);

    let scheduled_deadline = std::time::Instant::now() + Duration::from_secs(20);
    let (timer_after_slot, scheduled_event) = loop {
        let timer: TimerStateView =
            serde_json::from_value(fixture.json(&["timer", "show", rule_id]))
                .expect("scheduled SEND timer progress");
        let event = fixture.events().into_iter().find(|event| {
            event["kind"] == "rule.fired"
                && event["data"]["rule"] == rule_id
                && event["data"]["schedule_revision"] == timer.schedule_revision
        });
        if let (Some(_last_key), Some(event)) = (timer.last_key, event) {
            break (timer, event);
        }
        assert!(
            std::time::Instant::now() < scheduled_deadline,
            "scheduled SEND slot deadline; first_due={first_due}"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    let scheduled_at = scheduled_event["data"]["scheduled_at_ms"]
        .as_i64()
        .expect("scheduled SEND UTC slot");
    assert_eq!(timer_after_slot.last_key, Some(scheduled_at));
    assert!(
        timer_after_slot
            .next_due_at
            .is_some_and(|next| next > scheduled_at)
    );
    let scheduled_child_id = scheduled_event["data"]["child_command"]
        .as_str()
        .expect("scheduled SEND child ID")
        .to_owned();
    assert_ne!(scheduled_child_id, manual_child_id);
    let scheduled_child: CoreCommand =
        serde_json::from_value(fixture.wait_command(&scheduled_child_id, "applied"))
            .expect("applied scheduled SEND child");
    let scheduled_turn =
        wait_fake_send_turn_result(&fixture, &agent, generation, &scheduled_child, SEND_TEXT);
    assert_ne!(scheduled_turn.turn_id, manual_turn_id);

    let events = fixture.events();
    assert_eq!(fault_dispatches(&events, &manual_child_id), 1);
    assert_eq!(fault_dispatches(&events, &scheduled_child_id), 1);
    let stop_id = ulid::Ulid::new().to_string();
    let stop: CommandSubmitResponse = serde_json::from_value(fixture.json(&[
        "--id",
        &stop_id,
        "stop",
        &agent,
        "--generation",
        &generation_arg,
    ]))
    .expect("owner stop for completed timer SEND Agent");
    fixture.wait_command(&stop.command_id, "applied");
    fixture.stop(false);
}

#[test]
fn cli_event_rule_steers_exact_held_turn_in_global_and_project_scopes() {
    const PROJECT_ID: &str = "steer-fixture";
    const GLOBAL_RULE_ID: &str = "global-event-steer";
    const PROJECT_RULE_ID: &str = "project-event-steer";
    const HOLD_TURN: &str = "[fake:hold-turn]";
    const GLOBAL_TEXT: &str = "global-event-steer-text";
    const PROJECT_TEXT: &str = "project-event-steer-text";

    let mut fixture = Fixture::new("steer");
    assert!(
        fixture
            .git(&["init", "--initial-branch=main"])
            .status
            .success(),
        "initialize registered Steer project"
    );
    fs::write(fixture.workspace.join("seed.txt"), "steer fixture\n")
        .expect("write registered project seed");
    fs::create_dir_all(fixture.workspace.join(".eliot").join("rules"))
        .expect("create registered project rule directory");
    assert!(fixture.git(&["add", "--all"]).status.success());
    assert!(
        fixture
            .git(&["commit", "-m", "Steer event fixture"])
            .status
            .success(),
        "commit registered Steer project"
    );

    fixture.start();
    fixture.stop(false);
    let global_rules = fixture.data.join("rules");
    let project_rules = fixture.workspace.join(".eliot").join("rules");
    fs::create_dir_all(&global_rules).expect("create global rule directory");
    fs::create_dir_all(&project_rules).expect("create registered project rule directory");

    let project_path = serde_json::to_string(&fixture.workspace.display().to_string())
        .expect("encode registered project path");
    fs::write(
        fixture.data.join("config.toml"),
        format!(
            concat!(
                "[projects.{project_id}]\n",
                "path = {path}\n",
                "profiles = [\"fake\"]\n",
                "actions = [\"interrupt\", \"send\", \"steer\", \"stop\"]\n",
            ),
            project_id = PROJECT_ID,
            path = project_path,
        ),
    )
    .expect("write Owner Project Steer approval");

    let global_send_id = ulid::Ulid::new().to_string();
    let project_send_id = ulid::Ulid::new().to_string();
    let encode = |value: &str| serde_json::to_string(value).expect("encode TOML string");
    let steer_rule = |rule_id: &str, send_id: &str, text: &str| {
        format!(
            concat!(
                "[[rule]]\n",
                "id = {rule_id}\n",
                "on = {{ event = \"turn.started\", match = {{ ",
                "\"subject.harness\" = \"fake\", ",
                "\"event.data.command_id\" = {command_id} }} }}\n",
                "do = [{{ action = \"steer\", target = {target}, ",
                "expected_generation = {generation}, expected_turn_id = {turn_id}, ",
                "text = {text} }}]\n",
            ),
            rule_id = encode(rule_id),
            command_id = encode(send_id),
            target = encode("${event.agent}"),
            generation = encode("${event.generation}"),
            turn_id = encode("${event.data.turn_id}"),
            text = encode(text),
        )
    };
    fs::write(
        global_rules.join("global-steer.toml"),
        steer_rule(GLOBAL_RULE_ID, &global_send_id, GLOBAL_TEXT),
    )
    .expect("write Owner global Steer rule");
    fs::write(
        project_rules.join("project-steer.toml"),
        steer_rule(PROJECT_RULE_ID, &project_send_id, PROJECT_TEXT),
    )
    .expect("write registered Project Steer rule");

    fixture.start();
    let registration: ProjectRegistrationView =
        serde_json::from_value(fixture.json(&["project", "show", PROJECT_ID]))
            .expect("typed Steer project registration");
    assert!(
        registration.actions.iter().any(|action| action == "steer"),
        "project registration carries the Steer action approval"
    );

    let mut rule_states = Vec::new();
    for (rule_id, project_id) in [(GLOBAL_RULE_ID, None), (PROJECT_RULE_ID, Some(PROJECT_ID))] {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let state: RuleStateView = loop {
            let reply = match project_id {
                Some(project_id) => cli(
                    &fixture.data,
                    &["rule", "show", rule_id, "--project", project_id],
                ),
                None => cli(&fixture.data, &["rule", "show", rule_id]),
            };
            if reply.status.success() {
                break serde_json::from_str(&reply.stdout).expect("typed event Steer rule");
            }
            assert!(
                std::time::Instant::now() < deadline,
                "event Steer rule load deadline; bounded stderr={:?}",
                reply.stderr.chars().take(2048).collect::<String>()
            );
            std::thread::sleep(Duration::from_millis(25));
        };
        assert_eq!(state.id, rule_id);
        assert_eq!(state.project.as_deref(), project_id);
        rule_states.push(state);
    }

    let launch = fixture.json(&[
        "launch",
        "--profile",
        "fake",
        "--workspace",
        fixture
            .workspace
            .to_str()
            .expect("registered workspace path"),
    ]);
    let agent = launch["target"].as_str().expect("Fake Agent").to_owned();
    let generation = launch["generation"].as_i64().expect("Fake generation");
    let generation_arg = generation.to_string();
    let cases = [
        (
            None,
            GLOBAL_RULE_ID,
            global_send_id.as_str(),
            GLOBAL_TEXT,
            0_usize,
        ),
        (
            Some(PROJECT_ID),
            PROJECT_RULE_ID,
            project_send_id.as_str(),
            PROJECT_TEXT,
            1_usize,
        ),
    ];
    let mut retained = Vec::new();
    for (project_id, rule_id, send_id, steer_text, state_index) in cases {
        let rule_state = &rule_states[state_index];
        let send_reply = cli(
            &fixture.data,
            &[
                "--id",
                send_id,
                "send",
                &agent,
                "--generation",
                &generation_arg,
                "--",
                HOLD_TURN,
            ],
        );
        assert!(
            send_reply.status.success(),
            "Fake hold-turn SEND failed; bounded stdout={:?}; bounded stderr={:?}",
            send_reply.stdout.chars().take(2048).collect::<String>(),
            send_reply.stderr.chars().take(2048).collect::<String>()
        );
        let send: CoreCommand = serde_json::from_value(fixture.wait_command(send_id, "applied"))
            .expect("applied Fake hold-turn SEND");
        assert_eq!(send.id, send_id);
        assert_eq!(send.verb, "send");
        assert_eq!(send.target.as_deref(), Some(agent.as_str()));
        assert_eq!(send.expected_generation, Some(generation));
        let turn_id = send
            .turn_id
            .as_deref()
            .expect("held SEND turn ID")
            .to_owned();

        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let (started, firing, event_seq) = loop {
            let events = fixture.events();
            let started = events
                .iter()
                .find(|event| {
                    event["kind"] == "turn.started"
                        && event["data"]["command_id"] == send_id
                        && event["agent"] == agent
                        && event["generation"] == generation
                })
                .cloned();
            if let Some(started) = started {
                let event_seq = started["seq"].as_i64().expect("held turn start sequence");
                if let Some(firing) = events
                    .iter()
                    .find(|event| {
                        event["kind"] == "rule.fired"
                            && event["data"]["rule"] == rule_id
                            && event["data"]["event_seq"] == event_seq
                    })
                    .cloned()
                {
                    break (started, firing, event_seq);
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "event Steer firing deadline for {rule_id}"
            );
            std::thread::sleep(Duration::from_millis(25));
        };
        assert_eq!(started["subject"]["harness"], "fake");
        assert_eq!(started["data"]["turn_id"], turn_id);
        assert_eq!(firing["data"]["revision"], rule_state.revision);
        match project_id {
            Some(project_id) => assert_eq!(firing["data"]["project"], project_id),
            None => assert!(firing["data"].get("project").is_none_or(Value::is_null)),
        }

        let parent_id = firing["data"]["parent_command"]
            .as_str()
            .expect("Steer rule parent command")
            .to_owned();
        let steer_id = firing["data"]["child_command"]
            .as_str()
            .expect("Steer child command")
            .to_owned();
        let principal = project_id.map_or_else(
            || "owner".to_owned(),
            |project_id| format!("project:{project_id}"),
        );
        let parent: CoreCommand =
            serde_json::from_value(fixture.wait_command(&parent_id, "applied"))
                .expect("applied event Steer parent");
        assert_eq!(parent.verb, "rule");
        assert_eq!(parent.principal, principal);
        assert_eq!(parent.args["rule"], rule_id);
        assert_eq!(
            parent.outcome.as_ref().expect("Steer parent outcome")["children"],
            json!([steer_id])
        );
        let steer: CoreCommand = serde_json::from_value(fixture.wait_command(&steer_id, "applied"))
            .expect("applied Router Steer child");
        assert_eq!(steer.id, steer_id);
        assert_eq!(steer.verb, "steer");
        assert_eq!(steer.state, CommandState::Applied);
        assert_eq!(steer.principal, principal);
        assert_eq!(steer.target.as_deref(), Some(agent.as_str()));
        assert_eq!(steer.expected_generation, Some(generation));
        assert_eq!(steer.generation, Some(generation));
        assert_eq!(steer.turn_id.as_deref(), Some(turn_id.as_str()));
        assert_eq!(steer.args, json!({"text": steer_text}));
        assert_eq!(
            steer.outcome.as_ref().expect("retained control outcome")["turn_id"],
            turn_id
        );

        let events_after_steer = fixture.events();
        assert_eq!(fault_dispatches(&events_after_steer, &steer_id), 1);
        assert_eq!(
            events_after_steer
                .iter()
                .filter(|event| {
                    event["kind"] == "command.applied" && event["data"]["command_id"] == steer_id
                })
                .count(),
            1,
            "the applied control outcome is durably readable"
        );
        assert_eq!(
            events_after_steer
                .iter()
                .filter(|event| {
                    event["kind"] == "turn.started" && event["data"]["command_id"] == send_id
                })
                .count(),
            1,
            "Steer keeps the original turn without starting another"
        );
        assert!(!events_after_steer.iter().any(|event| {
            event["kind"] == "turn.completed"
                && event["data"]["command_id"] == send_id
                && event["data"]["turn_id"] == turn_id
        }));
        assert_eq!(
            fixture.json(&["show", &agent])["active_turn"].as_str(),
            Some(turn_id.as_str()),
            "Steer leaves the original Fake turn active"
        );

        let interrupt_id = ulid::Ulid::new().to_string();
        let interrupt_reply = cli(
            &fixture.data,
            &[
                "--id",
                &interrupt_id,
                "interrupt",
                &agent,
                "--generation",
                &generation_arg,
                "--turn",
                &turn_id,
            ],
        );
        assert!(
            interrupt_reply.status.success(),
            "exact Fake interrupt cleanup failed; bounded stdout={:?}; bounded stderr={:?}",
            interrupt_reply
                .stdout
                .chars()
                .take(2048)
                .collect::<String>(),
            interrupt_reply
                .stderr
                .chars()
                .take(2048)
                .collect::<String>()
        );
        let interrupt: CoreCommand =
            serde_json::from_value(fixture.wait_command(&interrupt_id, "applied"))
                .expect("applied exact-turn interrupt cleanup");
        assert_eq!(interrupt.turn_id.as_deref(), Some(turn_id.as_str()));
        let after_interrupt = fixture.events();
        let terminal = after_interrupt
            .iter()
            .find(|event| {
                event["kind"] == "turn.completed"
                    && event["data"]["command_id"] == send_id
                    && event["data"]["turn_id"] == turn_id
            })
            .expect("interrupt terminal event for the original turn");
        assert_eq!(terminal["data"]["status"], "interrupted");
        assert!(fixture.json(&["show", &agent])["active_turn"].is_null());

        retained.push((
            rule_id.to_owned(),
            event_seq,
            parent_id,
            parent,
            steer,
            send_id.to_owned(),
        ));
    }

    let stop_id = ulid::Ulid::new().to_string();
    let stop_reply = cli(
        &fixture.data,
        &[
            "--id",
            &stop_id,
            "stop",
            &agent,
            "--generation",
            &generation_arg,
        ],
    );
    assert!(
        stop_reply.status.success(),
        "exact Fake stop cleanup failed; bounded stdout={:?}; bounded stderr={:?}",
        stop_reply.stdout.chars().take(2048).collect::<String>(),
        stop_reply.stderr.chars().take(2048).collect::<String>()
    );
    fixture.wait_command(&stop_id, "applied");
    fixture.stop(false);
    fixture.start();

    let events_after_restart = fixture.events();
    for (rule_id, event_seq, parent_id, parent, steer, send_id) in &retained {
        let retained_parent: CoreCommand =
            serde_json::from_value(fixture.json(&["cmd", "show", parent_id]))
                .expect("retained Steer rule parent");
        let retained_steer: CoreCommand =
            serde_json::from_value(fixture.json(&["cmd", "show", &steer.id]))
                .expect("retained Steer child");
        assert_eq!(&retained_parent, parent);
        assert_eq!(&retained_steer, steer);
        assert_eq!(
            events_after_restart
                .iter()
                .filter(|event| {
                    event["kind"] == "rule.fired"
                        && event["data"]["rule"].as_str() == Some(rule_id.as_str())
                        && event["data"]["event_seq"].as_i64() == Some(*event_seq)
                        && event["data"]["parent_command"].as_str() == Some(parent_id.as_str())
                })
                .count(),
            1,
            "restart did not refire the retained turn.started event"
        );
        assert_eq!(fault_dispatches(&events_after_restart, &steer.id), 1);
        assert_eq!(fault_dispatches(&events_after_restart, send_id), 1);
    }
    fixture.stop(false);
}
