#![cfg(any(windows, target_os = "linux"))]

mod support;
#[cfg(windows)]
#[path = "support/windows_process.rs"]
mod windows_process;

use eliot_core::api::{
    EventsFilter, EventsReadRequest, EventsReadResponse, EventsSubscribeRequest,
    EventsSubscribeResponse,
};
use serde_json::{Value, json};
#[cfg(windows)]
use std::time::Duration;
use std::{collections::HashSet, fs, path::Path, process::Command};
#[cfg(windows)]
use support::Captured;
use support::{CLI, Fixture, cli, run};

fn public_event_rpc(
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

fn assert_invalid_event_rpc(
    runtime: &tokio::runtime::Runtime,
    fixture: &Fixture,
    method: &str,
    field: &str,
    params: Value,
) {
    let error = match public_event_rpc(runtime, fixture, method, params) {
        Err(error) => error,
        Ok(_) => panic!("{method} accepted malformed {field}"),
    };
    assert_eq!(error.code, "invalid", "{method} malformed {field}");
}

#[test]
fn public_event_requests_validate_semantic_values_without_mutation() {
    let mut fixture = Fixture::new("typed event validation");
    fixture.start();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("public RPC runtime");
    let raw = public_event_rpc(&runtime, &fixture, "events.read", json!({}))
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
            assert_invalid_event_rpc(&runtime, &fixture, method, "after", json!({"after": after}));
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
            assert_invalid_event_rpc(&runtime, &fixture, method, "limit", json!({"limit": limit}));
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
            assert_invalid_event_rpc(
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
                assert_invalid_event_rpc(
                    &runtime,
                    &fixture,
                    method,
                    "filter list",
                    json!({"filter": filter}),
                );
            }
            let mut filter = json!({});
            filter[field] = json!(vec!["repeated"; 1001]);
            assert_invalid_event_rpc(
                &runtime,
                &fixture,
                method,
                "raw filter count",
                json!({"filter": filter}),
            );
        }
        assert_invalid_event_rpc(
            &runtime,
            &fixture,
            method,
            "unknown request field",
            json!({"unknown": true}),
        );
        let raw = public_event_rpc(
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
        assert_invalid_event_rpc(
            &runtime,
            &fixture,
            "events.subscribe",
            "wait_ms",
            json!({"wait_ms": wait_ms}),
        );
    }
    assert_invalid_event_rpc(
        &runtime,
        &fixture,
        "events.read",
        "subscribe-only field",
        json!({"wait_ms": 0}),
    );
    let unsupported = match public_event_rpc(&runtime, &fixture, "events.unimplemented", json!({}))
    {
        Err(error) => error,
        Ok(_) => panic!("unknown event method unexpectedly succeeded"),
    };
    assert_eq!(unsupported.code, "unsupported");
    let raw = public_event_rpc(&runtime, &fixture, "events.subscribe", json!({}))
        .unwrap_or_else(|_| panic!("default events.subscribe failed"));
    let subscribed: EventsSubscribeResponse =
        serde_json::from_value(raw).expect("typed subscribe response");
    assert!(subscribed.events.is_empty());
    assert_eq!(subscribed.next, 0);
    assert_eq!(subscribed.high_water, before.high_water);
    assert!(!subscribed.truncated);
    assert!(!subscribed.lagged);
    let raw = public_event_rpc(&runtime, &fixture, "events.read", json!({}))
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
    let raw = public_event_rpc(&runtime, &fixture, "events.read", json!({}))
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
    let raw = public_event_rpc(
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
    let raw = public_event_rpc(&runtime, &fixture, "events.subscribe", params)
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
        let raw = public_event_rpc(&runtime, &fixture, "events.read", json!({"filter": filter}))
            .unwrap_or_else(|_| panic!("nonempty raw filter entries were tightened"));
        let empty: EventsReadResponse = serde_json::from_value(raw).expect("typed filtered replay");
        assert!(empty.events.is_empty());
        assert_eq!(empty.next, replay.high_water);
        assert_eq!(empty.high_water, replay.high_water);
        assert!(!empty.truncated);
    }
    let raw = public_event_rpc(
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
    let mut unknown = fixture.json(&["cmd", "show", &fault_id]);
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
        fixture.json(&["result", agent, "--generation", &generation])["turns"]
            .as_array()
            .unwrap()
            .is_empty()
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
    fixture.wait_command(attached["command_id"].as_str().unwrap(), "applied");
    let view = fixture.json(&["show", observer]);
    assert_eq!(view["state"], "unknown");
    assert_eq!(view["origin"], "external");
    assert_eq!(view["observe_only"], true);
    assert_eq!(view["own_identity"], false);
    assert!(view["active_turn"].is_null());
    let observed_generation = view["generation"].as_i64().unwrap();
    let generation_argument = observed_generation.to_string();
    let first_history = fixture.json(&[
        "history",
        observer,
        "--generation",
        &generation_argument,
        "--source",
        "native",
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
