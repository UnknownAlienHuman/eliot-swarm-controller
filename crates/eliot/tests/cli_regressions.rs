#![cfg(any(windows, target_os = "linux"))]

mod support;
#[cfg(windows)]
#[path = "support/windows_process.rs"]
mod windows_process;

use serde_json::Value;
#[cfg(windows)]
use std::time::Duration;
use std::{collections::HashSet, fs, path::Path, process::Command};
#[cfg(windows)]
use support::Captured;
use support::{CLI, Fixture, cli, run};

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
    let unknown = fixture.json(&["cmd", "show", &fault_id]);
    assert_eq!(
        unknown["state"],
        "unknown",
        "send exit {:?}, stderr: {}; command: {unknown}",
        fault.status.code(),
        fault.stderr
    );
    assert_eq!(fault.status.code(), Some(4), "unknown must have CLI exit 4");
    assert!(unknown["turn_id"].is_null());
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
