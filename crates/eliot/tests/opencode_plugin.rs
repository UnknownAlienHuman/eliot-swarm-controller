//! OpenCode plugin protocol tests with a scripted fake HTTP/SSE server.
//!
//! This target runs with `harness = false` (see `[[test]]` in Cargo.toml) so
//! the test binary can re-execute itself as the fake `opencode serve`: the
//! plugin spawns `cmd` with serve arguments, and a libtest harness would
//! corrupt that process model. When `ELIOT_FAKE_OPENCODE_SCRIPT` is set,
//! `main` runs the fixture server instead of the scenarios.
//!
//! Fixture payloads follow the installed OpenCode v2 API generation
//! (`opencode serve` 2.0.32, routes under `/api/...`, `{"data": ...}`
//! success bodies, `{"_tag": ...}` errors, Basic auth as
//! `opencode:OPENCODE_PASSWORD`, SSE envelopes with durable sequences);
//! the plugin under test is the real `swarm plugin opencode` binary driven
//! over the Eliot plugin protocol, with this file emulating the host side
//! (`process.put`, `events.put`, `source.gap`). No model is ever called.

use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader as AsyncBufReader};
use tokio::sync::{Mutex, broadcast, mpsc, oneshot};

const CLI: &str = env!("CARGO_BIN_EXE_swarm");
const MODEL: &str = "test-provider/test-model";
const OP_TIMEOUT: Duration = Duration::from_secs(15);
/// Hard per-scenario bound: generous next to the 15 s operation bound, but
/// a hang fails the run instead of living for hours (note 61).
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(120);
/// Grace for an owned fake server to exit after its scenario ends before
/// the run calls it orphaned.
const ORPHAN_GRACE: Duration = Duration::from_secs(5);
/// Backstop lifetime of one fake server: it exits itself well past the
/// scenario timeout, so even a hard-cancelled run cannot orphan it for hours.
const FAKE_MAX_LIFETIME: Duration = Duration::from_secs(180);

fn main() {
    if let Ok(script) = std::env::var("ELIOT_FAKE_OPENCODE_SCRIPT") {
        fake_server_main(&script);
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    runtime.block_on(async {
        run_scenario(
            "launch/send/snapshot/stop",
            scenario_launch_send_snapshot_stop,
        )
        .await;
        run_scenario(
            "failed turn reports its native error",
            scenario_failed_turn_reports_native_error,
        )
        .await;
        run_scenario(
            "model mismatch is rejected before session.ready",
            scenario_model_mismatch_is_rejected_before_ready,
        )
        .await;
        run_scenario(
            "location mismatch is rejected before session.ready",
            scenario_location_mismatch_is_rejected_before_ready,
        )
        .await;
        run_scenario(
            "prompt 400 is rejected without a second write",
            scenario_prompt_400_is_rejected_without_rewrite,
        )
        .await;
        run_scenario(
            "lost prompt reply is unknown without a second write",
            scenario_lost_prompt_reply_is_unknown_without_rewrite,
        )
        .await;
        run_scenario(
            "stop of a gone session applies",
            scenario_stop_of_gone_session_applies,
        )
        .await;
        run_scenario(
            "invalid profile rejected before spawn",
            scenario_invalid_profile_rejected_before_spawn,
        )
        .await;
        run_scenario(
            "SSE loss during send gaps the source",
            scenario_sse_loss_during_send_gaps,
        )
        .await;
        run_scenario(
            "resume reattaches the retained session on the stable store",
            scenario_resume_reattaches_retained_session,
        )
        .await;
        run_scenario(
            "resume aborts a retained running execution and drops its terminal",
            scenario_resume_aborts_retained_running_execution,
        )
        .await;
        run_scenario(
            "resume of a deleted session is not_found",
            scenario_resume_of_deleted_session_is_not_found,
        )
        .await;
        run_scenario(
            "resume refuses pending native requests",
            scenario_resume_refuses_pending_requests,
        )
        .await;
        run_scenario(
            "resume realigns model and agent with readback",
            scenario_resume_realigns_model_and_agent,
        )
        .await;
        run_scenario(
            "permission approve and deny round trip",
            scenario_permission_approve_deny_roundtrip,
        )
        .await;
        run_scenario(
            "form answer applies and deny cancels",
            scenario_form_answer_and_deny_cancel,
        )
        .await;
        run_scenario(
            "stale choice is rejected before any write",
            scenario_stale_choice_rejected_without_write,
        )
        .await;
        run_scenario(
            "server-refused answer is rejected and retryable",
            scenario_server_refused_answer_is_rejected,
        )
        .await;
        run_scenario(
            "lost reply is unknown without a second write",
            scenario_lost_reply_is_unknown_without_rewrite,
        )
        .await;
        run_scenario(
            "reply to a settled request closes it",
            scenario_reply_to_settled_request_closes_it,
        )
        .await;
        run_scenario(
            "interrupt applies and the terminal settles",
            scenario_interrupt_applies_and_terminal_settles,
        )
        .await;
        run_scenario(
            "interrupt after the terminal is stale",
            scenario_interrupt_after_terminal_is_stale,
        )
        .await;
        run_scenario(
            "request details commit redacted",
            scenario_request_details_commit_redacted,
        )
        .await;
    });
    println!("opencode_plugin: all scenarios passed");
}

/// Run one scenario under the hard per-scenario timeout. On success the
/// scenario's owned fake servers must already be dead (verified, not
/// assumed); on timeout they are killed by exact recorded PID and the run
/// fails with the scenario name. The plugin child itself always dies by
/// handle: dropping the timed-out future drops every `Host`, whose
/// `kill_on_drop` child ends it Ã¢â‚¬â€ on every exit path, including panics.
async fn run_scenario<F, Fut>(name: &'static str, make: F)
where
    F: FnOnce(OwnedChildren) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let owned = OwnedChildren::default();
    match tokio::time::timeout(SCENARIO_TIMEOUT, make(owned.clone())).await {
        Ok(()) => {
            owned.assert_all_dead(name).await;
            println!("ok - {name}");
        }
        Err(_) => {
            let recorded = owned.snapshot();
            owned.kill_live();
            panic!(
                "opencode_plugin scenario '{name}' timed out after {SCENARIO_TIMEOUT:?}; owned fake PIDs were {recorded:?}"
            );
        }
    }
}

/// PIDs of fake `opencode serve` processes this scenario owns. The plugin
/// reports its server PID through `process.put`; the test records it here
/// so scenario cleanup can kill exactly that process (never by name). The
/// plugin child itself is owned by handle (`kill_on_drop`) and is never in
/// this registry: it is reaped by `shutdown_child`, and killing a reaped
/// PID could hit an unrelated process after PID reuse.
#[derive(Clone, Default)]
struct OwnedChildren(Arc<std::sync::Mutex<Vec<u32>>>);

impl OwnedChildren {
    fn record(&self, pid: u32) {
        let mut pids = self.0.lock().expect("owned PID registry");
        if !pids.contains(&pid) {
            pids.push(pid);
        }
    }

    fn snapshot(&self) -> Vec<u32> {
        self.0.lock().expect("owned PID registry").clone()
    }

    /// Kill every still-live recorded fake by its exact PID. Best-effort
    /// per PID; returns the PIDs a kill was attempted on.
    fn kill_live(&self) -> Vec<u32> {
        let mut killed = Vec::new();
        for pid in self.snapshot() {
            if pid_is_live(pid) {
                kill_exact_pid(pid);
                killed.push(pid);
            }
        }
        killed
    }

    /// Success-path verification: every owned fake must already be dead
    /// (the plugin's stop/shutdown terminates its server; a failed start
    /// discards it). A survivor past the grace is killed by exact PID and
    /// fails the run: never silently orphaned.
    async fn assert_all_dead(&self, scenario: &'static str) {
        let deadline = tokio::time::Instant::now() + ORPHAN_GRACE;
        for pid in self.snapshot() {
            while pid_is_live(pid) && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            if pid_is_live(pid) {
                kill_exact_pid(pid);
                panic!(
                    "opencode_plugin scenario '{scenario}' orphaned its fake server (PID {pid}); killed it and failing"
                );
            }
        }
    }
}

impl Drop for OwnedChildren {
    fn drop(&mut self) {
        // RAII backstop for unwind paths (an assert or OP_TIMEOUT panic
        // inside the scenario skips both `kill_live` and `assert_all_dead`):
        // kill any still-live recorded fake by exact PID. The lock is taken
        // without panicking: a Drop must never panic, and a poisoned
        // registry means a scenario already failed loudly.
        let pids = self.0.lock().map(|guard| guard.clone()).unwrap_or_default();
        for pid in pids {
            if pid_is_live(pid) {
                kill_exact_pid(pid);
            }
        }
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut std::ffi::c_void;
    fn TerminateProcess(process: *mut std::ffi::c_void, code: u32) -> i32;
    fn GetExitCodeProcess(process: *mut std::ffi::c_void, code: *mut u32) -> i32;
}

/// Kill exactly `pid`. Best-effort: an already-exited PID is success (the
/// process is gone, which is the goal).
#[cfg(windows)]
#[allow(unsafe_code)]
fn kill_exact_pid(pid: u32) {
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    const PROCESS_TERMINATE: u32 = 0x0001;
    // SAFETY: null-checked; the returned handle is owned exactly once via
    // `OwnedHandle`, never inherited, closed on drop.
    let raw = unsafe { OpenProcess(PROCESS_TERMINATE, 0, pid) };
    if raw.is_null() {
        return;
    }
    // SAFETY: `OpenProcess` returned one owned, non-null process handle.
    let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
    // SAFETY: the handle is live; the exit code is arbitrary for a test kill.
    unsafe {
        TerminateProcess(handle.as_raw_handle(), 1);
    }
}

/// Whether `pid` names a live process right now. Unopenable counts as gone:
/// our own scenario children are always openable while alive.
#[cfg(windows)]
#[allow(unsafe_code)]
fn pid_is_live(pid: u32) -> bool {
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    const STILL_ACTIVE: u32 = 259;
    // SAFETY: as in `kill_exact_pid`.
    let raw = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if raw.is_null() {
        return false;
    }
    // SAFETY: `OpenProcess` returned one owned, non-null process handle.
    let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
    let mut code = 0;
    // SAFETY: the handle is live and `code` is valid writable storage.
    if unsafe { GetExitCodeProcess(handle.as_raw_handle(), &mut code) } == 0 {
        return false;
    }
    code == STILL_ACTIVE
}

/// Kill exactly `pid`. Best-effort: an already-exited PID is success (the
/// process is gone, which is the goal).
#[cfg(unix)]
#[allow(unsafe_code)]
fn kill_exact_pid(pid: u32) {
    // SAFETY: `kill` with SIGKILL on an exact scenario-owned PID has no
    // memory effects; ESRCH (already gone) is success for our purpose.
    let _ = unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
}

/// Whether `pid` names a live process right now. Unopenable counts as gone:
/// our own scenario children are always signallable while alive.
#[cfg(unix)]
#[allow(unsafe_code)]
fn pid_is_live(pid: u32) -> bool {
    // SAFETY: signal 0 probes only; see `kill_exact_pid`.
    if unsafe { libc::kill(pid as libc::pid_t, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

// ---------------------------------------------------------------------------
// Fake OpenCode server fixture (runs as a child of the plugin under test)
// ---------------------------------------------------------------------------

/// Script knobs as JSON in `ELIOT_FAKE_OPENCODE_SCRIPT`. Defaults describe
/// the happy path; scenarios override single fields.
#[derive(Debug, Clone)]
struct Script {
    session_id: String,
    create_model: Option<Value>,
    create_dir: Option<String>,
    prompt_status: u16,
    prompt_drop: bool,
    terminal: String,
    assistant_text: Option<String>,
    error: Option<Value>,
    delete_status: u16,
    drop_sse_on_prompt: bool,
    /// One ask bundle per prompt (1-based): `{permission?, form?}` emitted
    /// between the started and terminal envelopes of that prompt.
    asks: Vec<Value>,
    /// Prompt admits and starts the execution but emits no terminal; the
    /// execution stays live until `session.interrupt` aborts it.
    manual_terminal: bool,
    /// Prompt emits no terminal; the first applied reply or cancel emits
    /// the scripted terminal instead (the execution resumes past the
    /// request and completes). Implies the prompt holds its terminal.
    terminal_on_reply: bool,
    /// A retained session the fake boots with (the stable-store stand-in
    /// for resume): `{id, model_id, provider_id, agent?, directory, seq,
    /// live?}` (`live` seeds a still-running retained execution).
    seed_session: Option<Value>,
    /// Pending permission seeds at boot: `{id, action, resources, message?}`.
    seed_permissions: Vec<Value>,
    /// Pending form seeds at boot: `{id, title, fields}`.
    seed_forms: Vec<Value>,
    /// Settle asked requests natively at once without emitting their
    /// settle events (the lost-ephemeral case for reply-to-gone).
    ghost_settle: bool,
    /// Drop reply/cancel connections without a response (lost reply).
    reply_drop: bool,
    /// Installed agent ids for `GET /api/agent`.
    agents: Vec<String>,
}

impl Script {
    fn parse(raw: &str) -> Self {
        let value: Value = serde_json::from_str(raw).expect("script JSON");
        let get = |key: &str| value.get(key).cloned().unwrap_or(Value::Null);
        let text = |key: &str, default: &str| get(key).as_str().unwrap_or(default).to_owned();
        Script {
            session_id: text("session_id", "ses_test00000000000000000001"),
            create_model: match get("create_model") {
                Value::Null => None,
                model => Some(model),
            },
            create_dir: match get("create_dir") {
                Value::Null => None,
                Value::String(dir) => Some(dir),
                _ => None,
            },
            prompt_status: get("prompt_status").as_u64().unwrap_or(200) as u16,
            prompt_drop: get("prompt_drop").as_bool().unwrap_or(false),
            terminal: text("terminal", "succeeded"),
            assistant_text: match get("assistant_text") {
                Value::Null => None,
                Value::String(text) => Some(text),
                _ => None,
            },
            error: match get("error") {
                Value::Null => None,
                error => Some(error),
            },
            delete_status: get("delete_status").as_u64().unwrap_or(204) as u16,
            drop_sse_on_prompt: get("drop_sse_on_prompt").as_bool().unwrap_or(false),
            asks: get("asks").as_array().cloned().unwrap_or_default(),
            manual_terminal: get("manual_terminal").as_bool().unwrap_or(false),
            terminal_on_reply: get("terminal_on_reply").as_bool().unwrap_or(false),
            seed_session: match get("seed_session") {
                Value::Null => None,
                seed => Some(seed),
            },
            seed_permissions: get("seed_permissions")
                .as_array()
                .cloned()
                .unwrap_or_default(),
            seed_forms: get("seed_forms").as_array().cloned().unwrap_or_default(),
            ghost_settle: get("ghost_settle").as_bool().unwrap_or(false),
            reply_drop: get("reply_drop").as_bool().unwrap_or(false),
            agents: get("agents")
                .as_array()
                .map(|agents| {
                    agents
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_else(|| vec!["build".to_owned(), "general".to_owned()]),
        }
    }
}

struct Fixture {
    script: Script,
    expected_auth: String,
    log_path: PathBuf,
    sessions: Mutex<HashMap<String, FixtureSession>>,
    deleted: Mutex<std::collections::HashSet<String>>,
    permissions: Mutex<HashMap<String, Vec<Value>>>,
    forms: Mutex<HashMap<String, Vec<Value>>>,
    settled_forms: Mutex<std::collections::HashSet<(String, String)>>,
    inbox_counter: Mutex<u64>,
    events: broadcast::Sender<String>,
    sse_close: broadcast::Sender<()>,
    config_path: String,
}

struct FixtureSession {
    model: Value,
    agent: Option<String>,
    directory: String,
    seq: u64,
    inbox_id: Option<String>,
    terminal: Option<String>,
    live: bool,
}

fn fake_server_main(script_raw: &str) {
    let script = Script::parse(script_raw);
    let mut port: Option<u16> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--port"
            && let Some(value) = args.next()
        {
            port = value.parse().ok();
        }
    }
    let port = port.expect("fixture --port");
    let password = std::env::var("OPENCODE_PASSWORD").expect("fixture password");
    assert!(!password.is_empty(), "fixture password is empty");
    let config_home = std::env::var("XDG_CONFIG_HOME").expect("fixture XDG_CONFIG_HOME");
    let data_home = std::env::var("XDG_DATA_HOME").expect("fixture XDG_DATA_HOME");
    let log_path = PathBuf::from(std::env::var("ELIOT_FAKE_OPENCODE_LOG").expect("fixture log"));
    let report_path =
        PathBuf::from(std::env::var("ELIOT_FAKE_OPENCODE_REPORT").expect("fixture report"));
    // The plugin generates this file; the server only reads it. Its presence
    // and content prove the per-Agent config isolation end to end.
    let config_path = Path::new(&config_home)
        .join("opencode")
        .join("opencode.jsonc");
    let config_text = std::fs::read_to_string(&config_path).expect("fixture reads its config");
    let config_json: Value = serde_json::from_str(&config_text).expect("fixture config JSON");
    let config_model = config_json
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    assert!(
        !config_model.is_empty(),
        "fixture config names the profile model"
    );
    std::fs::write(
        &report_path,
        serde_json::to_string(&json!({
            "config_path": config_path.to_string_lossy(),
            "config_model": config_model,
            "config_mcp": config_json.get("mcp").cloned().unwrap_or(Value::Null),
            "has_password": true,
            "data_home": data_home,
            "port": port,
        }))
        .expect("report JSON"),
    )
    .expect("write fixture report");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("fixture runtime");
    runtime.block_on(async move {
        // Backstop: one scenario never needs its fake past SCENARIO_TIMEOUT,
        // so the fake exits itself rather than orphaning for hours when the
        // harness is cancelled hard and test-side cleanup never runs.
        tokio::spawn(async {
            tokio::time::sleep(FAKE_MAX_LIFETIME).await;
            std::process::exit(3);
        });
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("fixture bind");
        let (events, _) = broadcast::channel::<String>(64);
        let (sse_close, _) = broadcast::channel::<()>(1);
        let fixture = Arc::new(Fixture {
            script,
            expected_auth: format!(
                "Basic {}",
                base64_encode(format!("opencode:{password}").as_bytes())
            ),
            log_path,
            sessions: Mutex::new(HashMap::new()),
            deleted: Mutex::new(std::collections::HashSet::new()),
            permissions: Mutex::new(HashMap::new()),
            forms: Mutex::new(HashMap::new()),
            settled_forms: Mutex::new(std::collections::HashSet::new()),
            inbox_counter: Mutex::new(0),
            events,
            sse_close,
            config_path: config_path.to_string_lossy().into_owned(),
        });
        seed_fixture(&fixture).await;
        // The installed server prints its listening line on stdout.
        println!("server listening on http://127.0.0.1:{port}");
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let fixture = Arc::clone(&fixture);
            tokio::spawn(async move {
                serve_connection(fixture, stream).await;
            });
        }
    });
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let mut value: u32 = 0;
        for (index, byte) in chunk.iter().enumerate() {
            value |= (*byte as u32) << (16 - 8 * index);
        }
        out.push(ALPHABET[(value >> 18) as usize & 63] as char);
        out.push(ALPHABET[(value >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(value >> 6) as usize & 63] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[value as usize & 63] as char);
        } else {
            out.push('=');
        }
    }
    out
}

async fn serve_connection(fixture: Arc<Fixture>, stream: tokio::net::TcpStream) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = AsyncBufReader::new(reader);
    let mut head = Vec::new();
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => return,
            Ok(_) => {}
            Err(_) => return,
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        head.push(line);
        if head.len() > 64 || head.iter().map(|line| line.len()).sum::<usize>() > 16 * 1024 {
            return;
        }
    }
    if head.is_empty() {
        return;
    }
    let request_line = head[0].trim_end().to_owned();
    let mut parts = request_line.splitn(3, ' ');
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    let mut content_length: usize = 0;
    let mut authorization = String::new();
    for line in &head[1..] {
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
            && let Ok(length) = value.trim().parse::<usize>()
        {
            content_length = length.min(1024 * 1024);
        }
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("authorization")
        {
            authorization = value.trim().to_owned();
        }
    }
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        let mut read = 0;
        while read < content_length {
            match reader.read(&mut body[read..]).await {
                Ok(0) => return,
                Ok(count) => read += count,
                Err(_) => return,
            }
        }
    }
    if authorization != fixture.expected_auth {
        let payload = json!({"_tag": "UnauthorizedError", "message": "Authentication required"});
        write_response(&mut writer, 401, &payload.to_string()).await;
        return;
    }
    let path = target.split('?').next().unwrap_or_default().to_owned();
    let query = target.split_once('?').map(|(_, query)| query.to_owned());
    fixture.log(&method, &path, &body).await;
    if path == "/api/event" {
        serve_sse(fixture, writer).await;
        return;
    }
    let payload = route(&fixture, &method, &path, query.as_deref(), &body).await;
    match payload {
        Some((status, text)) => write_response(&mut writer, status, &text).await,
        None => {
            // A dropped handler (the lost-reply script): close the
            // connection without a response, so the client sees a
            // transport failure instead of waiting out its deadline.
        }
    }
}

async fn write_response(writer: &mut tokio::net::tcp::OwnedWriteHalf, status: u16, body: &str) {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "Error",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = writer.write_all(head.as_bytes()).await;
    let _ = writer.write_all(body.as_bytes()).await;
}

async fn serve_sse(fixture: Arc<Fixture>, mut writer: tokio::net::tcp::OwnedWriteHalf) {
    const HEAD: &str = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\nTransfer-Encoding: chunked\r\n\r\n";
    if writer.write_all(HEAD.as_bytes()).await.is_err() {
        return;
    }
    let mut receiver = fixture.events.subscribe();
    let mut closer = fixture.sse_close.subscribe();
    let connected = json!({"id": "evt_fixture_connected", "type": "server.connected", "data": {}});
    if write_chunk(&mut writer, &format!("data: {connected}\n\n"))
        .await
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            biased;
            _ = closer.recv() => {
                // Simulated transport loss: close the stream mid-turn.
                let _ = writer.shutdown().await;
                return;
            }
            next = receiver.recv() => {
                match next {
                    Ok(envelope) => {
                        if write_chunk(&mut writer, &format!("data: {envelope}\n\n"))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
        }
    }
}

async fn write_chunk(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    payload: &str,
) -> std::io::Result<()> {
    writer
        .write_all(format!("{:x}\r\n", payload.len()).as_bytes())
        .await?;
    writer.write_all(payload.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    writer.flush().await
}

/// Route one fixture request. `None` drops the connection (lost reply).
async fn route(
    fixture: &Fixture,
    method: &str,
    path: &str,
    query: Option<&str>,
    body: &[u8],
) -> Option<(u16, String)> {
    if method == "GET" && path == "/api/config" {
        let payload = json!([
            {"type": "document", "path": fixture.config_path,
             "info": {"model": {"providerID": "test-provider", "model": "test-model"}, "mcp": {}}},
            {"type": "directory", "path": fixture.config_path.rsplit(['/', '\\']).nth(1).unwrap_or_default()},
        ]);
        return Some((200, payload.to_string()));
    }
    if method == "POST" && path == "/api/session" {
        let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let id = fixture.script.session_id.clone();
        let model = fixture.script.create_model.clone().unwrap_or_else(|| {
            // Echo the requested model in the installed spelling; the
            // request may use either `providerID` or `providerId`.
            let requested = request.get("model").cloned().unwrap_or(Value::Null);
            let provider = requested
                .get("providerID")
                .or_else(|| requested.get("providerId"))
                .cloned()
                .unwrap_or(Value::Null);
            json!({
                "id": requested.get("id").cloned().unwrap_or(Value::Null),
                "providerID": provider,
            })
        });
        let directory = fixture.script.create_dir.clone().unwrap_or_else(|| {
            request
                .get("location")
                .and_then(|location| location.get("directory"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        });
        let mut sessions = fixture.sessions.lock().await;
        let seq = 0;
        sessions.insert(
            id.clone(),
            FixtureSession {
                model: model.clone(),
                agent: None,
                directory: directory.clone(),
                seq: seq + 1,
                inbox_id: None,
                terminal: None,
                live: false,
            },
        );
        drop(sessions);
        let created = json!({
            "id": format!("evt_{id}_created"),
            "created": 1000,
            "type": "session.created",
            "location": {"directory": directory},
            "data": {"sessionID": id},
            "durable": {"aggregateID": id, "seq": seq, "version": 1},
        });
        let _ = fixture.events.send(created.to_string());
        let payload = json!({"data": {
            "id": id,
            "model": model,
            "time": {"created": 1000, "updated": 1000},
            "location": {"directory": directory},
        }});
        return Some((200, payload.to_string()));
    }
    if let Some(rest) = path.strip_prefix("/api/session/") {
        let (id, tail) = match rest.split_once('/') {
            Some((id, tail)) => (id, format!("/{tail}")),
            None => (rest, String::new()),
        };
        if fixture.deleted.lock().await.contains(id) {
            return Some((404, not_found(id)));
        }
        if !fixture.sessions.lock().await.contains_key(id) {
            return Some((404, not_found(id)));
        }
        if method == "GET" && tail.is_empty() {
            let sessions = fixture.sessions.lock().await;
            let session = sessions.get(id).expect("fixture session");
            let mut info = json!({
                "id": id,
                "model": session.model,
                "time": {"created": 1000, "updated": 1000},
                "location": {"directory": session.directory},
            });
            if let Some(agent) = session.agent.clone() {
                info["agent"] = Value::String(agent);
            }
            let payload = json!({"data": info});
            return Some((200, payload.to_string()));
        }
        if method == "POST" && tail == "/interrupt" {
            let live = {
                let sessions = fixture.sessions.lock().await;
                sessions.get(id).expect("fixture session").live
            };
            if !live {
                return Some((200, json!({"interrupted": false}).to_string()));
            }
            {
                let mut sessions = fixture.sessions.lock().await;
                let session = sessions.get_mut(id).expect("fixture session");
                session.live = false;
                session.terminal = Some("session.execution.interrupted".to_owned());
            }
            let seq = next_seq(fixture, id).await;
            let mut data = json!({"sessionID": id});
            data["reason"] = Value::String("user".to_owned());
            let terminal = json!({
                "id": format!("evt_{id}_terminal_{seq}"),
                "created": 2005,
                "type": "session.execution.interrupted",
                "data": data,
                "durable": {"aggregateID": id, "seq": seq, "version": 1},
            });
            let _ = fixture.events.send(terminal.to_string());
            return Some((200, json!({"interrupted": true}).to_string()));
        }
        if method == "GET" && tail == "/permission" {
            let permissions = fixture.permissions.lock().await;
            let pending = permissions.get(id).cloned().unwrap_or_default();
            return Some((200, json!({"data": pending}).to_string()));
        }
        if method == "GET" && tail == "/form" {
            let forms = fixture.forms.lock().await;
            let pending = forms.get(id).cloned().unwrap_or_default();
            return Some((200, json!({"data": pending}).to_string()));
        }
        if method == "GET" && tail == "/inbox" {
            return Some((200, json!({"data": []}).to_string()));
        }
        if method == "POST" && tail == "/model" {
            let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let model = request.get("model").cloned().unwrap_or(Value::Null);
            {
                let mut sessions = fixture.sessions.lock().await;
                let session = sessions.get_mut(id).expect("fixture session");
                session.model = model.clone();
            }
            let seq = next_seq(fixture, id).await;
            let selected = json!({
                "id": format!("evt_{id}_model_{seq}"),
                "created": 3000,
                "type": "session.model.selected",
                "data": {"sessionID": id, "model": model},
                "durable": {"aggregateID": id, "seq": seq, "version": 1},
            });
            let _ = fixture.events.send(selected.to_string());
            return Some((204, String::new()));
        }
        if method == "POST" && tail == "/agent" {
            let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let agent = request
                .get("agent")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            {
                let mut sessions = fixture.sessions.lock().await;
                let session = sessions.get_mut(id).expect("fixture session");
                session.agent = Some(agent.clone());
            }
            let seq = next_seq(fixture, id).await;
            let selected = json!({
                "id": format!("evt_{id}_agent_{seq}"),
                "created": 3000,
                "type": "session.agent.selected",
                "data": {"sessionID": id, "agent": agent},
                "durable": {"aggregateID": id, "seq": seq, "version": 1},
            });
            let _ = fixture.events.send(selected.to_string());
            return Some((204, String::new()));
        }
        if let Some(request_id) = tail
            .strip_prefix("/permission/")
            .and_then(|rest| rest.strip_suffix("/reply"))
        {
            if fixture.script.reply_drop {
                return None;
            }
            if method != "POST" {
                return Some((404, not_found(id)));
            }
            let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let decision = request
                .get("decision")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !["once", "always", "reject"].contains(&decision) {
                return Some((400, String::new()));
            }
            let mut permissions = fixture.permissions.lock().await;
            let pending = permissions.entry(id.to_owned()).or_default();
            if !pending
                .iter()
                .any(|entry| entry.get("id").and_then(Value::as_str) == Some(request_id))
            {
                return Some((404, not_found(id)));
            }
            // Installed behavior: a reject settles every pending permission
            // of the session; once/always settle only the targeted one.
            let settled = if decision == "reject" {
                std::mem::take(pending)
            } else {
                let position = pending
                    .iter()
                    .position(|entry| entry.get("id").and_then(Value::as_str) == Some(request_id))
                    .expect("pending permission");
                vec![pending.remove(position)]
            };
            drop(permissions);
            for entry in settled {
                let replied = json!({
                    "id": format!("evt_replied_{}", entry.get("id").and_then(Value::as_str).unwrap_or_default()),
                    "created": 4000,
                    "type": "permission.replied",
                    "data": {
                        "sessionID": id,
                        "requestID": entry.get("id").cloned().unwrap_or(Value::Null),
                        "reply": decision,
                    },
                });
                let _ = fixture.events.send(replied.to_string());
            }
            maybe_emit_reply_terminal(fixture, id).await;
            return Some((204, String::new()));
        }
        if let Some(form_id) = tail
            .strip_prefix("/form/")
            .and_then(|rest| rest.strip_suffix("/reply"))
        {
            if fixture.script.reply_drop {
                return None;
            }
            if method != "POST" {
                return Some((404, not_found(id)));
            }
            let mut forms = fixture.forms.lock().await;
            let pending = forms.entry(id.to_owned()).or_default();
            let position = pending
                .iter()
                .position(|entry| entry.get("id").and_then(Value::as_str) == Some(form_id));
            let Some(position) = position else {
                if fixture
                    .settled_forms
                    .lock()
                    .await
                    .contains(&(id.to_owned(), form_id.to_owned()))
                {
                    return Some((409, String::new()));
                }
                return Some((404, not_found(id)));
            };
            let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let answer = request.get("answer").cloned().unwrap_or(Value::Null);
            if !form_answer_valid(&pending[position], &answer) {
                return Some((400, String::new()));
            }
            pending.remove(position);
            drop(forms);
            fixture
                .settled_forms
                .lock()
                .await
                .insert((id.to_owned(), form_id.to_owned()));
            let replied = json!({
                "id": format!("evt_form_replied_{form_id}"),
                "created": 4000,
                "type": "form.replied",
                "data": {"id": form_id, "sessionID": id, "answer": answer},
            });
            let _ = fixture.events.send(replied.to_string());
            maybe_emit_reply_terminal(fixture, id).await;
            return Some((204, String::new()));
        }
        if let Some(form_id) = tail.strip_prefix("/form/") {
            if method != "DELETE" || form_id.contains('/') {
                return Some((404, not_found(id)));
            }
            if fixture.script.reply_drop {
                return None;
            }
            let mut forms = fixture.forms.lock().await;
            let pending = forms.entry(id.to_owned()).or_default();
            let Some(position) = pending
                .iter()
                .position(|entry| entry.get("id").and_then(Value::as_str) == Some(form_id))
            else {
                return Some((404, not_found(id)));
            };
            pending.remove(position);
            drop(forms);
            let cancelled = json!({
                "id": format!("evt_form_cancelled_{form_id}"),
                "created": 4000,
                "type": "form.cancelled",
                "data": {"id": form_id, "sessionID": id},
            });
            let _ = fixture.events.send(cancelled.to_string());
            maybe_emit_reply_terminal(fixture, id).await;
            return Some((204, String::new()));
        }
        if method == "DELETE" && tail.is_empty() {
            if fixture.script.delete_status != 204 {
                // The session was already gone: DELETE reports 404 and the
                // session stays absent for the follow-up readback.
                fixture.deleted.lock().await.insert(id.to_owned());
                return Some((fixture.script.delete_status, not_found(id)));
            }
            fixture.deleted.lock().await.insert(id.to_owned());
            let seq = next_seq(fixture, id).await;
            let deleted = json!({
                "id": format!("evt_{id}_deleted"),
                "created": 9000,
                "type": "session.deleted",
                "data": {"sessionID": id},
                "durable": {"aggregateID": id, "seq": seq, "version": 2},
            });
            let _ = fixture.events.send(deleted.to_string());
            return Some((204, String::new()));
        }
        if method == "POST" && tail == "/prompt" {
            if fixture.script.prompt_drop {
                fixture.log_marker(0).await;
                return None;
            }
            if fixture.script.prompt_status != 200 {
                // The installed server refuses invalid writes with an
                // empty 400; the fixture matches it.
                fixture.log_marker(0).await;
                return Some((fixture.script.prompt_status, String::new()));
            }
            let request: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
            let text = request
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let mut counter = fixture.inbox_counter.lock().await;
            *counter += 1;
            let prompt_index = *counter as usize;
            let inbox_id = format!("msg_fixture_{prompt_index:03}");
            drop(counter);
            {
                let mut sessions = fixture.sessions.lock().await;
                let session = sessions.get_mut(id).expect("fixture session");
                session.inbox_id = Some(inbox_id.clone());
                session.terminal = Some(fixture.script.terminal.clone());
                session.live = true;
            }
            if fixture.script.drop_sse_on_prompt {
                // Admit the input, then lose the stream before any
                // association fact flows: the send must settle unknown.
                let _ = fixture.sse_close.send(());
                fixture.log_marker(0).await;
                let payload = json!({"data": {
                    "id": inbox_id,
                    "sessionID": id,
                    "time": {"created": 2000},
                    "type": "user",
                    "payload": {"text": text},
                    "delivery": "steer",
                }});
                return Some((200, payload.to_string()));
            }
            let enqueued_seq = next_seq(fixture, id).await;
            let started_seq = next_seq(fixture, id).await;
            let delivered_seq = next_seq(fixture, id).await;
            let started_id = format!("evt_{id}_started_{enqueued_seq}");
            let mut envelopes = vec![
                json!({
                    "id": format!("evt_{id}_enqueued_{enqueued_seq}"),
                    "created": 2000,
                    "type": "session.inbox.enqueued",
                    "data": {"sessionID": id, "inboxID": inbox_id,
                             "item": {"type": "user", "payload": {"text": text}, "delivery": "steer"}},
                    "durable": {"aggregateID": id, "seq": enqueued_seq, "version": 1},
                }),
                json!({
                    "id": started_id,
                    "created": 2001,
                    "type": "session.execution.started",
                    "data": {"sessionID": id},
                    "durable": {"aggregateID": id, "seq": started_seq, "version": 1},
                }),
                json!({
                    "id": format!("evt_{id}_delivered_{delivered_seq}"),
                    "created": 2002,
                    "type": "session.inbox.delivered",
                    "data": {"sessionID": id, "inboxID": inbox_id},
                    "durable": {"aggregateID": id, "seq": delivered_seq, "version": 1},
                }),
            ];
            envelopes.extend(emit_asks(fixture, id, prompt_index).await);
            if !fixture.script.manual_terminal && !fixture.script.terminal_on_reply {
                let terminal_seq = next_seq(fixture, id).await;
                envelopes.push(terminal_envelope(fixture, id, terminal_seq));
                let mut sessions = fixture.sessions.lock().await;
                sessions.get_mut(id).expect("fixture session").live = false;
            }
            let emitted = envelopes.len();
            for envelope in envelopes {
                let _ = fixture.events.send(envelope.to_string());
            }
            fixture.log_marker(emitted).await;
            let payload = json!({"data": {
                "id": inbox_id,
                "sessionID": id,
                "time": {"created": 2000},
                "type": "user",
                "payload": {"text": text},
                "delivery": "steer",
            }});
            return Some((200, payload.to_string()));
        }
        if method == "GET" && tail == "/message" {
            let _ = query;
            let sessions = fixture.sessions.lock().await;
            let session = sessions.get(id).expect("fixture session");
            let Some(inbox_id) = session.inbox_id.clone() else {
                let payload = json!({"data": [], "cursor": {"previous": null, "next": null}});
                return Some((200, payload.to_string()));
            };
            let terminal = session.terminal.clone().unwrap_or_default();
            let outcome = match terminal.as_str() {
                "session.execution.succeeded" => "succeeded",
                "session.execution.interrupted" => "interrupted",
                _ => "failed",
            };
            let mut data = vec![idle_message(fixture, outcome)];
            if let Some(text) = fixture.script.assistant_text.clone() {
                data.push(json!({
                    "id": "msg_fixture_assistant_001",
                    "time": {"created": 2003, "completed": 2004},
                    "type": "assistant",
                    "agent": "build",
                    "model": {"id": "test-model", "providerID": "test-provider"},
                    "content": [{"type": "text", "text": text}],
                }));
            }
            data.push(json!({
                "id": inbox_id,
                "time": {"created": 2000},
                "text": "fixture input",
                "type": "user",
            }));
            let payload = json!({"data": data, "cursor": {"previous": null, "next": null}});
            return Some((200, payload.to_string()));
        }
    }
    if method == "GET" && path == "/api/agent" {
        let data = fixture
            .script
            .agents
            .iter()
            .map(|agent| json!({"id": agent}))
            .collect::<Vec<_>>();
        return Some((200, json!({"data": data}).to_string()));
    }
    let payload = json!({"_tag": "UnknownError", "message": "fixture has no such route"});
    Some((404, payload.to_string()))
}

/// With `terminal_on_reply`, the first applied reply or cancel resumes
/// the live execution to its scripted terminal.
async fn maybe_emit_reply_terminal(fixture: &Fixture, id: &str) {
    if !fixture.script.terminal_on_reply {
        return;
    }
    let live = {
        let sessions = fixture.sessions.lock().await;
        sessions
            .get(id)
            .map(|session| session.live)
            .unwrap_or(false)
    };
    if !live {
        return;
    }
    {
        let mut sessions = fixture.sessions.lock().await;
        sessions.get_mut(id).expect("fixture session").live = false;
    }
    let seq = next_seq(fixture, id).await;
    let terminal = terminal_envelope(fixture, id, seq);
    let _ = fixture.events.send(terminal.to_string());
}

/// Emit this prompt's scripted asks as ephemeral request envelopes,
/// storing their pending entries first. With `ghost_settle` the entries
/// settle natively at once without their settle events (the lost
/// ephemeral), so a later reply meets a gone target.
async fn emit_asks(fixture: &Fixture, id: &str, prompt_index: usize) -> Vec<Value> {
    let Some(bundle) = fixture.script.asks.get(prompt_index - 1).cloned() else {
        return Vec::new();
    };
    let mut envelopes = Vec::new();
    for seed in ask_seeds(&bundle, "permission") {
        let entry = permission_entry(id, &seed);
        let asked = json!({
            "id": format!("evt_asked_{}", entry.get("id").and_then(Value::as_str).unwrap_or_default()),
            "created": 2003,
            "type": "permission.asked",
            "data": entry,
        });
        fixture
            .permissions
            .lock()
            .await
            .entry(id.to_owned())
            .or_default()
            .push(entry);
        envelopes.push(asked);
    }
    for seed in ask_seeds(&bundle, "form") {
        let entry = form_entry(id, &seed);
        let created = json!({
            "id": format!("evt_form_created_{}", entry.get("id").and_then(Value::as_str).unwrap_or_default()),
            "created": 2003,
            "type": "form.created",
            "data": {"form": entry},
        });
        fixture
            .forms
            .lock()
            .await
            .entry(id.to_owned())
            .or_default()
            .push(entry);
        envelopes.push(created);
    }
    if fixture.script.ghost_settle {
        fixture
            .permissions
            .lock()
            .await
            .entry(id.to_owned())
            .or_default()
            .clear();
        let mut forms = fixture.forms.lock().await;
        if let Some(pending) = forms.get_mut(id) {
            for entry in pending.drain(..) {
                if let Some(form_id) = entry.get("id").and_then(Value::as_str) {
                    fixture
                        .settled_forms
                        .lock()
                        .await
                        .insert((id.to_owned(), form_id.to_owned()));
                }
            }
        }
    }
    envelopes
}

/// One ask bundle's seeds for a family: a single object or an array.
fn ask_seeds(bundle: &Value, family: &str) -> Vec<Value> {
    match bundle.get(family) {
        Some(Value::Array(seeds)) => seeds.clone(),
        Some(seed) if !seed.is_null() => vec![seed.clone()],
        _ => Vec::new(),
    }
}

fn idle_message(fixture: &Fixture, outcome: &str) -> Value {
    let mut message = json!({
        "id": "msg_fixture_idle_001",
        "time": {"created": 2005},
        "type": "idle",
        "outcome": outcome,
    });
    if let Some(error) = fixture.script.error.clone() {
        message["error"] = error;
    }
    message
}

fn terminal_envelope(fixture: &Fixture, id: &str, seq: u64) -> Value {
    let terminal = fixture.script.terminal.clone();
    let mut data = json!({"sessionID": id});
    if terminal == "session.execution.failed"
        && let Some(error) = fixture.script.error.clone()
    {
        data["error"] = error;
    }
    if terminal == "session.execution.interrupted" {
        data["reason"] = Value::String("user".to_owned());
    }
    json!({
        "id": format!("evt_{id}_terminal_{seq}"),
        "created": 2005,
        "type": terminal,
        "data": data,
        "durable": {"aggregateID": id, "seq": seq, "version": 1},
    })
}

async fn next_seq(fixture: &Fixture, id: &str) -> u64 {
    let mut sessions = fixture.sessions.lock().await;
    let session = sessions.get_mut(id).expect("fixture session");
    let seq = session.seq;
    session.seq += 1;
    seq
}

/// The installed server answers missing subjects with an EMPTY 404 on
/// every session/permission/form route; the fixture matches it.
fn not_found(_id: &str) -> String {
    String::new()
}

/// Boot the seeded retained session and its pending requests: the
/// stable-store stand-in a resumed generation reattaches to.
async fn seed_fixture(fixture: &Fixture) {
    let Some(seed) = fixture.script.seed_session.clone() else {
        return;
    };
    let id = seed
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if id.is_empty() {
        return;
    }
    let model = json!({
        "id": seed.get("model_id").cloned().unwrap_or(Value::Null),
        "providerID": seed.get("provider_id").cloned().unwrap_or(Value::Null),
    });
    fixture.sessions.lock().await.insert(
        id.clone(),
        FixtureSession {
            model,
            agent: seed.get("agent").and_then(Value::as_str).map(str::to_owned),
            directory: seed
                .get("directory")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            seq: seed.get("seq").and_then(Value::as_u64).unwrap_or(0),
            inbox_id: None,
            terminal: None,
            live: seed.get("live").and_then(Value::as_bool).unwrap_or(false),
        },
    );
    if !fixture.script.seed_permissions.is_empty() {
        let pending = fixture
            .script
            .seed_permissions
            .iter()
            .map(|seed| permission_entry(&id, seed))
            .collect::<Vec<_>>();
        fixture.permissions.lock().await.insert(id.clone(), pending);
    }
    if !fixture.script.seed_forms.is_empty() {
        let pending = fixture
            .script
            .seed_forms
            .iter()
            .map(|seed| form_entry(&id, seed))
            .collect::<Vec<_>>();
        fixture.forms.lock().await.insert(id.clone(), pending);
    }
}

/// One stored permission request in the installed `Permission.Request`
/// shape.
fn permission_entry(session_id: &str, seed: &Value) -> Value {
    json!({
        "id": seed.get("id").cloned().unwrap_or(Value::Null),
        "sessionID": session_id,
        "action": seed.get("action").cloned().unwrap_or(json!("read")),
        "resources": seed.get("resources").cloned().unwrap_or(json!([])),
        "save": seed.get("save").cloned().unwrap_or(json!([])),
        "message": seed.get("message").cloned().unwrap_or(Value::Null),
    })
}

/// One stored form in the installed `Form.Info` shape.
fn form_entry(session_id: &str, seed: &Value) -> Value {
    json!({
        "id": seed.get("id").cloned().unwrap_or(Value::Null),
        "sessionID": session_id,
        "title": seed.get("title").cloned().unwrap_or(json!("")),
        "fields": seed.get("fields").cloned().unwrap_or(json!([])),
    })
}

/// The installed server refuses answers outside the offered schema with an
/// empty 400: unknown keys, non-offered option values, and mistyped values.
fn form_answer_valid(form: &Value, answer: &Value) -> bool {
    let Some(answers) = answer.as_object() else {
        return false;
    };
    let fields = form
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let offered = fields
        .iter()
        .filter_map(|field| {
            field
                .get("key")
                .and_then(Value::as_str)
                .map(|key| (key, field))
        })
        .collect::<std::collections::HashMap<_, _>>();
    for (key, value) in answers {
        let Some(field) = offered.get(key.as_str()) else {
            return false;
        };
        let field_type = field.get("type").and_then(Value::as_str).unwrap_or("");
        let typed = match field_type {
            "boolean" => value.is_boolean(),
            "number" | "integer" => value.is_number(),
            "multiselect" => value.is_array(),
            _ => value.is_string(),
        };
        if !typed {
            return false;
        }
        let options = field
            .get("options")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let custom = field
            .get("custom")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if options.is_empty() || custom {
            continue;
        }
        let values = match value {
            Value::String(one) => vec![one.as_str()],
            Value::Array(many) => many.iter().filter_map(Value::as_str).collect(),
            _ => return false,
        };
        for value in values {
            if !options
                .iter()
                .any(|option| option.get("value").and_then(Value::as_str) == Some(value))
            {
                return false;
            }
        }
    }
    true
}

impl Fixture {
    async fn log(&self, method: &str, path: &str, body: &[u8]) {
        // The workspace tokio has no `fs` feature; a tiny blocking append
        // is fine for the fixture log.
        use std::io::Write;
        let entry = json!({
            "method": method,
            "path": path,
            "body": String::from_utf8_lossy(body).into_owned(),
        });
        let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
        else {
            return;
        };
        let _ = file.write_all(format!("{entry}\n").as_bytes());
    }

    /// Quiesce marker for the prompt handler: proves the handler ran and how
    /// many SSE envelopes it emitted, so the test drains deterministically.
    async fn log_marker(&self, emitted: usize) {
        use std::io::Write;
        let entry = json!({"marker": "prompt-quiesced", "emitted": emitted});
        let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
        else {
            return;
        };
        let _ = file.write_all(format!("{entry}\n").as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Host-side driver (emulates the daemon: process.put, events.put, source.gap)
// ---------------------------------------------------------------------------

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, String>>>>>;

struct Host {
    writes: Option<mpsc::Sender<String>>,
    pending: Pending,
    inbound: mpsc::Receiver<HostRequest>,
    next_id: u64,
    next_seq: i64,
    child: tokio::process::Child,
    stderr: Option<tokio::task::JoinHandle<String>>,
    /// Fake server request log: quiesce markers are polled from here.
    log: PathBuf,
    /// Scenario-owned fake PIDs (from `process.put`): timeout, success, and
    /// unwind paths kill exactly these, never by name.
    owned: OwnedChildren,
}

/// What proves the plugin is quiescent after the call response arrived.
/// No fixed sleeps: every drain ends on a causal signal.
#[derive(Clone, Copy)]
enum Quiesce {
    /// Drain inbound until this commit kind arrives (bounded by OP_TIMEOUT).
    UntilKind(&'static str),
    /// The fake logged its prompt marker for this call and the plugin emits
    /// nothing for it: wait for a marker past the pre-call log end, assert
    /// it emitted nothing, then inbound must already be empty.
    SilenceAfterMarker,
    /// No fake I/O and no plugin output are possible for this call, so
    /// inbound must already be empty. Used for replays (no second write),
    /// reads, local validation, and calls after a full drain.
    SilenceNow,
}

struct HostRequest {
    id: Value,
    method: String,
    params: Value,
}

struct Scenario {
    root: PathBuf,
    agent_id: String,
    host: Host,
}

impl Host {
    async fn spawn(root: &Path, owned: OwnedChildren) -> Self {
        let mut command = tokio::process::Command::new(CLI);
        command
            .arg("--data-dir")
            .arg(root.join("data"))
            .arg("plugin")
            .arg("opencode")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            command.creation_flags(0x0800_0000);
        }
        // The plugin child must never mistake itself for the fixture.
        command.env_remove("ELIOT_FAKE_OPENCODE_SCRIPT");
        let mut child = command.spawn().expect("spawn plugin opencode");
        let stdin = child.stdin.take().expect("plugin stdin");
        let stdout = child.stdout.take().expect("plugin stdout");
        let stderr = child.stderr.take().expect("plugin stderr");
        let stderr_task = tokio::spawn(async move {
            let mut text = String::new();
            let mut stderr = stderr;
            let _ = stderr.read_to_string(&mut text).await;
            text.chars().take(2048).collect()
        });
        let (writes, mut write_rx) = mpsc::channel::<String>(32);
        tokio::spawn(async move {
            let mut stdin = stdin;
            while let Some(line) = write_rx.recv().await {
                if stdin.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
                if stdin.write_all(b"\n").await.is_err() {
                    break;
                }
                if stdin.flush().await.is_err() {
                    break;
                }
            }
        });
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let (inbound_tx, inbound) = mpsc::channel::<HostRequest>(32);
        let reader_pending = Arc::clone(&pending);
        tokio::spawn(async move {
            let mut lines = AsyncBufReader::new(stdout).lines();
            loop {
                let line = match lines.next_line().await {
                    Ok(Some(line)) => line,
                    _ => break,
                };
                if line.trim().is_empty() {
                    continue;
                }
                let frame: Value = match serde_json::from_str(&line) {
                    Ok(frame) => frame,
                    Err(_) => continue,
                };
                if let Some(method) = frame.get("method").and_then(Value::as_str) {
                    let _ = inbound_tx
                        .send(HostRequest {
                            id: frame.get("id").cloned().unwrap_or(Value::Null),
                            method: method.to_owned(),
                            params: frame.get("params").cloned().unwrap_or(Value::Null),
                        })
                        .await;
                    continue;
                }
                let id = frame
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let sender = reader_pending.lock().await.remove(&id);
                let Some(sender) = sender else { continue };
                if let Some(result) = frame.get("result") {
                    let _ = sender.send(Ok(result.clone()));
                } else {
                    // Log the full error for failure diagnostics; scenarios
                    // assert on the code only.
                    eprintln!("plugin error response: {frame}");
                    let code = frame
                        .get("error")
                        .and_then(|error| error.get("data"))
                        .and_then(|data| data.get("code"))
                        .and_then(Value::as_str)
                        .unwrap_or("rpc_error")
                        .to_owned();
                    let _ = sender.send(Err(code));
                }
            }
        });
        Self {
            writes: Some(writes),
            pending,
            inbound,
            next_id: 0,
            next_seq: 1,
            child,
            stderr: Some(stderr_task),
            log: root.join("requests.jsonl"),
            owned,
        }
    }

    fn writer(&self) -> mpsc::Sender<String> {
        self.writes.clone().expect("host write queue")
    }

    /// Pump one host call, acking every inbound batch while it is in
    /// flight. Native commits may land on either side of the response, so
    /// the pumped `(kind, data)` pairs return with the outcome and the
    /// scenario asserts on both. After the response, `quiesce` proves the
    /// plugin emitted everything it will for this call.
    async fn pump_call(
        &mut self,
        method: &str,
        params: Value,
        timeout: Duration,
        quiesce: Quiesce,
    ) -> (Result<Value, String>, Vec<(String, Value)>) {
        let log_lines = log_line_count(&self.log);
        let rx = self.begin_call(method, params).await;
        // One deadline for the whole call: inbound traffic and the response
        // share it, so a legitimately slow call (the lost-reply script)
        // never trips a per-recv timeout while waiting.
        let deadline = tokio::time::Instant::now() + timeout;
        let mut rx = std::pin::pin!(tokio::time::timeout_at(deadline, rx));
        let mut committed: Vec<(String, Value)> = Vec::new();
        let outcome = loop {
            tokio::select! {
                biased;
                request = tokio::time::timeout_at(deadline, self.inbound.recv()) => {
                    match request {
                        Ok(Some(request)) => {
                            committed.push(ack_inbound(self, &request, method).await);
                        }
                        Ok(None) => panic!("plugin request channel closed during {method}"),
                        Err(_) => break (&mut rx).await,
                    }
                }
                result = &mut rx => break result,
            }
        };
        let outcome = outcome
            .unwrap_or_else(|_| {
                panic!(
                    "host call deadline during {method}: no response for {timeout:?} (committed so far: {:?})",
                    kinds(&committed)
                )
            })
            .expect("host response channel");
        match quiesce {
            Quiesce::UntilKind(kind) => {
                // Commits causally after the response (the terminal pair
                // follows `applied`): drain until the terminal kind lands.
                // The whole drain shares one deadline, so a plugin that
                // keeps committing without the terminal kind fails the run
                // instead of draining forever.
                let drain_deadline = tokio::time::Instant::now() + timeout;
                while committed.last().map(|(kind, _)| kind.as_str()) != Some(kind) {
                    let request = tokio::time::timeout_at(drain_deadline, self.inbound.recv())
                        .await
                        .expect("quiescent commit deadline")
                        .expect("plugin request channel");
                    committed.push(ack_inbound(self, &request, method).await);
                }
            }
            Quiesce::SilenceAfterMarker => {
                // The refused/dropped prompt emits nothing natively: the
                // fake marker proves the handler ran without emitting, so
                // inbound must already be empty.
                let marker = wait_for_prompt_marker(&self.log, log_lines, timeout).await;
                assert_eq!(
                    marker.get("emitted").and_then(Value::as_u64),
                    Some(0),
                    "refused prompt emits no envelopes: {marker}"
                );
                assert!(
                    self.inbound.try_recv().is_err(),
                    "expected no commits during {method}"
                );
            }
            Quiesce::SilenceNow => {
                assert!(
                    self.inbound.try_recv().is_err(),
                    "expected no commits during {method}"
                );
            }
        }
        (outcome, committed)
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, String> {
        let (outcome, committed) = self
            .pump_call(method, params, OP_TIMEOUT, Quiesce::SilenceNow)
            .await;
        assert!(
            committed.is_empty(),
            "expected no commits during {method}: {committed:?}"
        );
        outcome
    }

    async fn begin_call(
        &mut self,
        method: &str,
        params: Value,
    ) -> oneshot::Receiver<Result<Value, String>> {
        self.next_id += 1;
        let id = format!("host-call-{}", self.next_id);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);
        let frame = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        // Bounded like every other host wait: a stuck writer task must fail
        // the run, never park the scenario forever.
        tokio::time::timeout(OP_TIMEOUT, self.writer().send(frame.to_string()))
            .await
            .expect("host write deadline")
            .expect("host write queue");
        rx
    }

    async fn respond(&self, id: &Value, result: Value) {
        let frame = json!({"jsonrpc": "2.0", "id": id, "result": result});
        tokio::time::timeout(OP_TIMEOUT, self.writer().send(frame.to_string()))
            .await
            .expect("host write deadline")
            .expect("host write queue");
    }

    async fn shutdown_child(&mut self) -> String {
        drop(self.writes.take());
        let status = tokio::time::timeout(OP_TIMEOUT, self.child.wait())
            .await
            .expect("plugin exit deadline")
            .expect("plugin wait");
        assert!(status.success(), "plugin exit status: {status}");
        // The stderr EOF join is bounded too: the pipe stays open while any
        // inheritor of the plugin's stderr (such as an orphaned fake server)
        // is alive, and an unbounded join here is exactly the 0-CPU hang.
        tokio::time::timeout(OP_TIMEOUT, self.stderr.take().unwrap())
            .await
            .expect("plugin stderr EOF deadline")
            .unwrap_or_default()
    }
}

/// Ack one `events.put` batch, returning its single `(kind, data)`.
/// Ack one inbound plugin request during a pumped call, returning its
/// committed `(kind, data)` pair.
async fn ack_inbound(host: &mut Host, request: &HostRequest, method: &str) -> (String, Value) {
    match request.method.as_str() {
        "events.put" => ack_events_put(host, request).await,
        "source.gap" => {
            let gap_id = request
                .params
                .get("gap_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let seq = host.next_seq;
            host.next_seq += 1;
            host.respond(&request.id, json!({"gap_id": gap_id, "seq": seq}))
                .await;
            ("source.gap".to_owned(), request.params.clone())
        }
        other => panic!("unexpected host request during {method}: {other}"),
    }
}

fn log_line_count(log: &Path) -> usize {
    std::fs::read_to_string(log)
        .unwrap_or_default()
        .lines()
        .count()
}

/// Wait for the fake's prompt marker past `after_lines`: the fixture proves
/// its prompt handler ran (and how many envelopes it emitted) instead of
/// the test guessing with a sleep.
async fn wait_for_prompt_marker(log: &Path, after_lines: usize, timeout: Duration) -> Value {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let text = std::fs::read_to_string(log).unwrap_or_default();
        for line in text.lines().skip(after_lines) {
            if let Ok(entry) = serde_json::from_str::<Value>(line)
                && entry.get("marker").and_then(Value::as_str) == Some("prompt-quiesced")
            {
                return entry;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("fake prompt marker did not arrive");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn ack_events_put(host: &mut Host, request: &HostRequest) -> (String, Value) {
    let events = request
        .params
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    assert_eq!(events.len(), 1, "one native record per batch");
    let pair = (
        events[0]
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        events[0].get("data").cloned().unwrap_or(Value::Null),
    );
    let first = host.next_seq;
    host.next_seq += 1;
    host.respond(
        &request.id,
        json!({
            "first_seq": first, "last_seq": first,
            "cursor": request.params.get("cursor").cloned().unwrap_or(Value::Null),
        }),
    )
    .await;
    pair
}

struct PendingStart {
    root: PathBuf,
    agent_id: String,
    host: Host,
    params: Value,
    generation: i64,
}

struct FailedAttempt {
    root: PathBuf,
    host: Host,
    params: Value,
}

impl Scenario {
    async fn start(script: &Value, profile: Value, owned: OwnedChildren) -> Self {
        let agent_id = ulid::Ulid::new().to_string();
        let root: PathBuf = std::env::temp_dir().join(format!(
            "eliot-opencode-plugin-test-{}",
            ulid::Ulid::new().to_string().to_lowercase()
        ));
        std::fs::create_dir_all(root.join("data")).unwrap();
        std::fs::create_dir_all(root.join("workspace")).unwrap();
        let (pending, rx) = Self::setup_start_in(
            &root,
            "instance",
            "requests.jsonl",
            "report.json",
            script,
            profile,
            agent_id,
            1,
            None,
            None,
            owned,
        )
        .await;
        pending.finish(rx).await.0
    }

    async fn setup_start(
        script: &Value,
        profile: Value,
        agent_id: String,
        generation: i64,
        owned: OwnedChildren,
    ) -> (PendingStart, oneshot::Receiver<Result<Value, String>>) {
        let root: PathBuf = std::env::temp_dir().join(format!(
            "eliot-opencode-plugin-test-{}",
            ulid::Ulid::new().to_string().to_lowercase()
        ));
        std::fs::create_dir_all(root.join("data")).unwrap();
        std::fs::create_dir_all(root.join("workspace")).unwrap();
        Self::setup_start_in(
            &root,
            "instance",
            "requests.jsonl",
            "report.json",
            script,
            profile,
            agent_id,
            generation,
            None,
            None,
            owned,
        )
        .await
    }

    /// Start a generation in an explicit root/instance with its own fake
    /// log/report files. Resume scenarios share the root (the per-Agent
    /// store lives beside the instance dirs) with a fresh instance dir,
    /// generation number, and resume subject per generation.
    #[allow(clippy::too_many_arguments)]
    async fn setup_start_in(
        root: &Path,
        instance: &str,
        log_file: &str,
        report_file: &str,
        script: &Value,
        profile: Value,
        agent_id: String,
        generation: i64,
        resume: Option<(Value, Value)>,
        credential: Option<String>,
        owned: OwnedChildren,
    ) -> (PendingStart, oneshot::Receiver<Result<Value, String>>) {
        let root = root.to_owned();
        std::fs::create_dir_all(root.join(instance)).unwrap();
        let mut host = Host::spawn(&root, owned).await;
        let credential = credential.unwrap_or_else(|| format!("credential-{}", ulid::Ulid::new()));
        let manifest = host
            .call(
                "initialize",
                json!({
                    "protocol": "eliot-plugin/1",
                    "credential": credential,
                    "instance_dir": root.join(instance).to_string_lossy(),
                    "config": {},
                }),
            )
            .await
            .expect("initialize");
        assert_eq!(
            manifest.get("name").and_then(Value::as_str),
            Some("opencode")
        );
        assert_eq!(
            manifest.get("protocol").and_then(Value::as_str),
            Some("eliot-plugin/1")
        );
        assert_eq!(
            manifest["verbs"],
            json!([
                "launch",
                "resume",
                "send",
                "interrupt",
                "approve",
                "deny",
                "answer",
                "stop"
            ])
        );
        assert_eq!(
            manifest["sources"]["controlled"],
            json!({"message": "stream", "request": "stream", "session": "stream", "turn": "stream"})
        );
        let start_id = ulid::Ulid::new().to_string();
        let self_exe = std::env::current_exe().expect("test executable");
        let mut full_profile = profile;
        full_profile["cmd"] = Value::String(self_exe.to_string_lossy().into_owned());
        let (resume_from, source_state) = resume.unwrap_or((Value::Null, Value::Null));
        let params = json!({
            "agent_id": agent_id,
            "generation": generation,
            "command_id": start_id,
            "profile": full_profile,
            "workspace": root.join("workspace").to_string_lossy(),
            "target_environment": {
                "ELIOT_FAKE_OPENCODE_SCRIPT": script.to_string(),
                "ELIOT_FAKE_OPENCODE_LOG": root.join(log_file).to_string_lossy(),
                "ELIOT_FAKE_OPENCODE_REPORT": root.join(report_file).to_string_lossy(),
            },
            "note": null,
            "source_plan": {"session": "stream", "turn": "stream", "message": "stream", "request": "stream"},
            "source_state": source_state,
            "resume_from": resume_from,
            "initial": null,
        });
        let rx = host.begin_call("agent.start", params.clone()).await;
        (
            PendingStart {
                root,
                agent_id,
                host,
                params,
                generation,
            },
            rx,
        )
    }

    fn report(&self) -> Value {
        let text =
            std::fs::read_to_string(self.root.join("report.json")).expect("fake server report");
        serde_json::from_str(&text).expect("fake server report JSON")
    }

    fn requests(&self) -> Vec<Value> {
        let text = std::fs::read_to_string(self.root.join("requests.jsonl")).unwrap_or_default();
        text.lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).expect("request log JSON"))
            .collect()
    }
}

impl PendingStart {
    async fn finish(mut self, rx: oneshot::Receiver<Result<Value, String>>) -> (Scenario, Value) {
        let deadline = tokio::time::Instant::now() + OP_TIMEOUT;
        let mut rx = std::pin::pin!(tokio::time::timeout_at(deadline, rx));
        let mut ready_data: Option<Value> = None;
        let ready = loop {
            tokio::select! {
                biased;
                request = tokio::time::timeout_at(deadline, self.host.inbound.recv()) => {
                    let request = match request {
                        Ok(Some(request)) => request,
                        Ok(None) => panic!("plugin request channel closed during start"),
                        Err(_) => break (&mut rx).await,
                    };
                    match request.method.as_str() {
                        "process.put" => {
                            answer_process_put(&mut self.host, &self.agent_id, self.generation, &request).await;
                        }
                        "events.put" => {
                            let (kind, data) = ack_events_put(&mut self.host, &request).await;
                            assert_eq!(kind, "session.ready");
                            assert!(ready_data.is_none(), "session.ready commits once");
                            ready_data = Some(data);
                        }
                        other => panic!("unexpected host request during start: {other}"),
                    }
                }
                result = &mut rx => break result,
            }
        };
        let ready = ready
            .expect("agent.start deadline")
            .expect("agent.start response channel")
            .expect("agent.start ready");
        let start_id = self
            .params
            .get("command_id")
            .and_then(Value::as_str)
            .expect("start command id");
        assert_eq!(ready.get("state").and_then(Value::as_str), Some("ready"));
        assert_eq!(
            ready.get("agent_id").and_then(Value::as_str),
            Some(self.agent_id.as_str())
        );
        assert_eq!(
            ready.get("generation").and_then(Value::as_i64),
            Some(self.generation)
        );
        assert_eq!(
            ready.get("command_id").and_then(Value::as_str),
            Some(start_id)
        );
        let ready_data = ready_data.expect("session.ready batch");
        let native_scope = ready_data
            .get("native_scope")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(
            native_scope.starts_with("opencode-store:"),
            "native scope derives from the per-Agent store: {native_scope}"
        );
        let native_ref = ready_data
            .get("native_ref")
            .and_then(Value::as_str)
            .unwrap_or_default();
        assert!(!native_ref.is_empty());
        assert_eq!(
            ready.get("native_scope").and_then(Value::as_str),
            Some(native_scope)
        );
        assert_eq!(
            ready.get("native_ref").and_then(Value::as_str),
            Some(native_ref)
        );
        assert_eq!(
            ready["requested"].get("model").and_then(Value::as_str),
            Some(MODEL)
        );
        assert_eq!(
            ready["effective"].get("model").and_then(Value::as_str),
            Some(MODEL)
        );
        assert_eq!(
            ready["effective"]
                .get("model_verified")
                .and_then(Value::as_bool),
            Some(true)
        );
        (
            Scenario {
                root: self.root,
                agent_id: self.agent_id,
                host: self.host,
            },
            ready,
        )
    }

    async fn finish_expecting_error(
        mut self,
        rx: oneshot::Receiver<Result<Value, String>>,
        expected_code: &str,
    ) -> FailedAttempt {
        let deadline = tokio::time::Instant::now() + OP_TIMEOUT;
        let mut rx = std::pin::pin!(tokio::time::timeout_at(deadline, rx));
        let outcome = loop {
            tokio::select! {
                biased;
                request = tokio::time::timeout_at(deadline, self.host.inbound.recv()) => {
                    let request = match request {
                        Ok(Some(request)) => request,
                        Ok(None) => panic!("plugin request channel closed during start"),
                        Err(_) => break (&mut rx).await,
                    };
                    match request.method.as_str() {
                        "process.put" => {
                            answer_process_put(&mut self.host, &self.agent_id, self.generation, &request).await;
                        }
                        "source.gap" => {
                            let gap_id = request.params.get("gap_id").and_then(Value::as_str).unwrap_or_default().to_owned();
                            let seq = self.host.next_seq;
                            self.host.next_seq += 1;
                            self.host.respond(&request.id, json!({"gap_id": gap_id, "seq": seq})).await;
                        }
                        "events.put" => panic!("no session.ready may commit on a failed start"),
                        other => panic!("unexpected host request during start: {other}"),
                    }
                }
                result = &mut rx => break result,
            }
        };
        let outcome = outcome
            .expect("agent.start deadline")
            .expect("agent.start response channel");
        assert_eq!(outcome, Err(expected_code.to_owned()));
        FailedAttempt {
            root: self.root,
            host: self.host,
            params: self.params,
        }
    }
}

impl FailedAttempt {
    async fn retry(&mut self, expected_code: &str) {
        let outcome = self.host.call("agent.start", self.params.clone()).await;
        assert_eq!(outcome, Err(expected_code.to_owned()));
    }
}

async fn answer_process_put(
    host: &mut Host,
    agent_id: &str,
    generation: i64,
    request: &HostRequest,
) {
    let pid = request
        .params
        .get("pid")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    assert!(pid > 0);
    // The owned fake server, by exact PID: timeout, success, and unwind
    // paths kill exactly this process (never by name).
    host.owned
        .record(u32::try_from(pid).expect("server PID fits u32"));
    assert_eq!(
        request.params.get("agent_id").and_then(Value::as_str),
        Some(agent_id)
    );
    assert_eq!(
        request.params.get("generation").and_then(Value::as_i64),
        Some(generation)
    );
    assert_eq!(
        request.params.get("role").and_then(Value::as_str),
        Some("harness")
    );
    host.respond(
        &request.id,
        json!({
            "identity": {"host_boot_id": "test-boot", "pid": pid, "start_time": 1},
            "owner": format!("agent:{agent_id}#{generation}"),
            "role": "harness",
            "state": "running",
            "exit_code": null,
            "updated_at": 1,
        }),
    )
    .await;
}

fn opencode_profile() -> Value {
    json!({
        "harness": "opencode",
        "model": MODEL,
        "pass_env": ["ELIOT_FAKE_OPENCODE_SCRIPT", "ELIOT_FAKE_OPENCODE_LOG", "ELIOT_FAKE_OPENCODE_REPORT"],
    })
}

fn script(fields: Value) -> Value {
    let mut base = json!({
        "terminal": "session.execution.succeeded",
        "assistant_text": "PONG",
    });
    for (key, value) in fields.as_object().cloned().unwrap_or_default() {
        base[key] = value;
    }
    base
}

/// One send round trip. Returns the send id, the applied turn id, and every
/// committed `(kind, data)` in commit order (commits may land on either
/// side of the response).
async fn send_turn(
    scenario: &mut Scenario,
    generation: i64,
    text: &str,
) -> (String, String, Vec<(String, Value)>) {
    let send_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": generation,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": text},
                "turn_id": null,
                "request_id": null,
            }),
            OP_TIMEOUT,
            Quiesce::UntilKind("turn.completed"),
        )
        .await;
    let applied = outcome.expect("send applies");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    let turn_id = applied
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("applied turn id")
        .to_owned();
    assert!(!turn_id.is_empty());
    (send_id, turn_id, committed)
}

async fn stop_agent(scenario: &mut Scenario, generation: i64, force: bool) -> Vec<(String, Value)> {
    let stop_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "agent.stop",
            json!({
                "agent_id": agent_id,
                "generation": generation,
                "command_id": stop_id,
                "force": force,
            }),
            OP_TIMEOUT,
            Quiesce::UntilKind("session.ended"),
        )
        .await;
    let stopped = outcome.expect("stop applies");
    assert_eq!(
        stopped.get("state").and_then(Value::as_str),
        Some("applied")
    );
    committed
}

fn kinds(committed: &[(String, Value)]) -> Vec<&str> {
    committed.iter().map(|(kind, _)| kind.as_str()).collect()
}

/// One send against a held terminal (`manual_terminal` /
/// `terminal_on_reply`): drains through the first `request.opened` and
/// returns the send, turn and request ids with every commit so far.
async fn send_until_ask(
    scenario: &mut Scenario,
    generation: i64,
    text: &str,
) -> (String, String, String, Vec<(String, Value)>) {
    let send_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": generation,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": text},
                "turn_id": null,
                "request_id": null,
            }),
            OP_TIMEOUT,
            Quiesce::UntilKind("request.opened"),
        )
        .await;
    let applied = outcome.expect("send applies");
    let turn_id = applied
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("applied turn id")
        .to_owned();
    let request_id = committed
        .iter()
        .find(|(kind, _)| kind == "request.opened")
        .and_then(|(_, data)| data.get("request_id").and_then(Value::as_str))
        .expect("opened request id")
        .to_owned();
    (send_id, turn_id, request_id, committed)
}

/// One control round trip (interrupt/approve/deny/answer) with the given
/// quiesce. Returns the command id, the outcome and every commit so far.
async fn control(
    scenario: &mut Scenario,
    generation: i64,
    verb: &str,
    turn_id: Option<&str>,
    request_id: Option<&str>,
    args: Value,
    quiesce: Quiesce,
) -> (String, Result<Value, String>, Vec<(String, Value)>) {
    control_as(
        scenario,
        generation,
        &ulid::Ulid::new().to_string(),
        verb,
        turn_id,
        request_id,
        args,
        quiesce,
    )
    .await
}

/// One control round trip under an explicit command id (for retries).
#[allow(clippy::too_many_arguments)]
async fn control_as(
    scenario: &mut Scenario,
    generation: i64,
    command_id: &str,
    verb: &str,
    turn_id: Option<&str>,
    request_id: Option<&str>,
    args: Value,
    quiesce: Quiesce,
) -> (String, Result<Value, String>, Vec<(String, Value)>) {
    let agent_id = scenario.agent_id.clone();
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": generation,
                "command_id": command_id,
                "verb": verb,
                "args": args,
                "turn_id": turn_id,
                "request_id": request_id,
            }),
            OP_TIMEOUT,
            quiesce,
        )
        .await;
    (command_id.to_owned(), outcome, committed)
}

/// Drain inbound until `kind` commits (bounded): the follow-up commits a
/// `terminal_on_reply` settle or an interrupt terminal lands after the
/// control response quiesced.
async fn drain_until(host: &mut Host, kind: &str) -> Vec<(String, Value)> {
    let deadline = tokio::time::Instant::now() + OP_TIMEOUT;
    let mut committed = Vec::new();
    loop {
        let request = tokio::time::timeout_at(deadline, host.inbound.recv())
            .await
            .expect("follow-up commit deadline")
            .expect("plugin request channel");
        committed.push(ack_inbound(host, &request, "drain").await);
        if committed.last().map(|(kind, _)| kind.as_str()) == Some(kind) {
            return committed;
        }
    }
}

fn report_at(root: &Path, file: &str) -> Value {
    let text = std::fs::read_to_string(root.join(file)).expect("fake server report");
    serde_json::from_str(&text).expect("fake server report JSON")
}

fn requests_at(root: &Path, file: &str) -> Vec<Value> {
    let text = std::fs::read_to_string(root.join(file)).unwrap_or_default();
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("request log JSON"))
        .collect()
}

fn count_requests(requests: &[Value], method: &str, suffix: &str) -> usize {
    requests
        .iter()
        .filter(|entry| {
            entry.get("method").and_then(Value::as_str) == Some(method)
                && entry
                    .get("path")
                    .and_then(Value::as_str)
                    .is_some_and(|path| path.ends_with(suffix))
        })
        .count()
}

async fn scenario_launch_send_snapshot_stop(owned: OwnedChildren) {
    let mut scenario = Scenario::start(&script(json!({})), opencode_profile(), owned).await;
    // The fixture proves the per-Agent isolation: its config path sits
    // under the plugin instance dir and names the profile model.
    let report = scenario.report();
    let config_path = report
        .get("config_path")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        config_path.contains("instance"),
        "per-Agent config path: {config_path}"
    );
    assert_eq!(
        report.get("config_model").and_then(Value::as_str),
        Some(MODEL)
    );
    assert_eq!(
        report.get("config_mcp").cloned().unwrap_or(Value::Null),
        json!({})
    );
    assert_eq!(
        report.get("has_password").and_then(Value::as_bool),
        Some(true)
    );

    let (send_id, turn_id, committed) = send_turn(&mut scenario, 1, "reply PONG").await;
    assert_eq!(
        kinds(&committed),
        ["turn.started", "message.completed", "turn.completed"]
    );
    assert_eq!(
        committed[0].1.get("command_id").and_then(Value::as_str),
        Some(send_id.as_str())
    );
    assert_eq!(
        committed[0].1.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert_eq!(
        committed[1].1.get("text").and_then(Value::as_str),
        Some("PONG")
    );
    assert_eq!(
        committed[2].1.get("status").and_then(Value::as_str),
        Some("completed")
    );
    assert_eq!(
        committed[2].1.get("final_text").and_then(Value::as_str),
        Some("PONG")
    );

    // Snapshot after the terminal turn: idle, exact turn facts, known result.
    let agent_id = scenario.agent_id.clone();
    let snapshot = scenario
        .host
        .call(
            "agent.snapshot",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "turn_id": null,
            }),
        )
        .await
        .expect("snapshot");
    assert_eq!(snapshot.get("state").and_then(Value::as_str), Some("idle"));
    assert_eq!(
        snapshot["last_turn"].get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert_eq!(
        snapshot["last_turn"]
            .get("final_text")
            .and_then(Value::as_str),
        Some("PONG")
    );
    assert_eq!(
        snapshot.get("result_known").and_then(Value::as_bool),
        Some(true)
    );

    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);

    // Exactly one prompt reached the server; stop closed the session
    // (DELETE) and proved its absence (GET 404).
    let requests = scenario.requests();
    let prompts = requests
        .iter()
        .filter(|entry| {
            entry.get("method").and_then(Value::as_str) == Some("POST")
                && entry
                    .get("path")
                    .and_then(Value::as_str)
                    .is_some_and(|path| path.ends_with("/prompt"))
        })
        .count();
    assert_eq!(prompts, 1);
    let mut saw_delete = false;
    let mut saw_get_after_delete = false;
    for entry in &requests {
        let method = entry
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let path = entry
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if method == "DELETE" && path.starts_with("/api/session/") {
            saw_delete = true;
        }
        if saw_delete && method == "GET" && path.starts_with("/api/session/") {
            saw_get_after_delete = true;
        }
    }
    assert!(saw_delete, "stop issues the native close");
    assert!(saw_get_after_delete, "stop proves absence after the close");

    let shutdown = scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    assert_eq!(
        shutdown.get("state").and_then(Value::as_str),
        Some("shutdown")
    );
    scenario.host.shutdown_child().await;
}

async fn scenario_failed_turn_reports_native_error(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({
            "terminal": "session.execution.failed",
            "assistant_text": null,
            "error": {"type": "provider.no-route", "message": "Model unavailable: x/y"},
        })),
        opencode_profile(),
        owned,
    )
    .await;
    let (send_id, turn_id, committed) = send_turn(&mut scenario, 1, "doomed").await;
    // No assistant text: no message.completed, and the failed turn carries
    // the exact native error.
    assert_eq!(kinds(&committed), ["turn.started", "turn.completed"]);
    assert_eq!(
        committed[1].1.get("command_id").and_then(Value::as_str),
        Some(send_id.as_str())
    );
    assert_eq!(
        committed[1].1.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert_eq!(
        committed[1].1.get("status").and_then(Value::as_str),
        Some("failed")
    );
    assert_eq!(
        committed[1].1["native_error"]
            .get("message")
            .and_then(Value::as_str),
        Some("Model unavailable: x/y")
    );
    assert!(
        committed[1].1.get("final_text").is_none(),
        "no text was produced: {}",
        committed[1].1
    );
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario.host.call("shutdown", json!({})).await.unwrap();
    scenario.host.shutdown_child().await;
}

async fn scenario_model_mismatch_is_rejected_before_ready(owned: OwnedChildren) {
    let script = script(json!({
        "create_model": {"id": "other-model", "providerID": "other-provider"},
    }));
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) =
        Scenario::setup_start(&script, opencode_profile(), agent_id, 1, owned).await;
    let mut failed = pending.finish_expecting_error(rx, "profile_error").await;
    failed.retry("profile_error").await;
    // The readback mismatch best-effort deletes the created session: the
    // fake saw the native close for exactly one created session.
    let text = std::fs::read_to_string(failed.root.join("requests.jsonl")).unwrap_or_default();
    let mut creates = 0;
    let mut deletes = 0;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let entry: Value = serde_json::from_str(line).expect("request log JSON");
        let method = entry
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let path = entry
            .get("path")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if method == "POST" && path == "/api/session" {
            creates += 1;
        }
        if method == "DELETE" && path.starts_with("/api/session/") {
            deletes += 1;
        }
    }
    assert_eq!((creates, deletes), (1, 1));
}

async fn scenario_location_mismatch_is_rejected_before_ready(owned: OwnedChildren) {
    let other = std::env::temp_dir()
        .join("eliot-opencode-plugin-test-elsewhere")
        .to_string_lossy()
        .into_owned();
    let script = script(json!({ "create_dir": other }));
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) =
        Scenario::setup_start(&script, opencode_profile(), agent_id, 1, owned).await;
    let mut failed = pending.finish_expecting_error(rx, "profile_error").await;
    failed.retry("profile_error").await;
}

async fn scenario_prompt_400_is_rejected_without_rewrite(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({ "prompt_status": 400 })),
        opencode_profile(),
        owned,
    )
    .await;
    let send_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let params = json!({
        "agent_id": agent_id,
        "generation": 1,
        "command_id": send_id,
        "verb": "send",
        "args": {"text": "refused"},
        "turn_id": null,
        "request_id": null,
    });
    for attempt in 0..2 {
        // The first prompt hits the fake (its marker proves it emitted
        // nothing); the replay sends no second write.
        let quiesce = if attempt == 0 {
            Quiesce::SilenceAfterMarker
        } else {
            Quiesce::SilenceNow
        };
        let (outcome, committed) = scenario
            .host
            .pump_call("agent.command", params.clone(), OP_TIMEOUT, quiesce)
            .await;
        let rejected = outcome.expect("rejected prompt answers");
        assert_eq!(
            rejected.get("state").and_then(Value::as_str),
            Some("rejected")
        );
        assert!(committed.is_empty(), "no turn opens: {committed:?}");
    }
    let prompts = scenario
        .requests()
        .iter()
        .filter(|entry| {
            entry
                .get("path")
                .and_then(Value::as_str)
                .is_some_and(|path| path.ends_with("/prompt"))
        })
        .count();
    assert_eq!(prompts, 1, "the replay sends no second write");
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario.host.call("shutdown", json!({})).await.unwrap();
    scenario.host.shutdown_child().await;
}

async fn scenario_lost_prompt_reply_is_unknown_without_rewrite(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({ "prompt_drop": true })),
        opencode_profile(),
        owned,
    )
    .await;
    let send_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let params = json!({
        "agent_id": agent_id,
        "generation": 1,
        "command_id": send_id,
        "verb": "send",
        "args": {"text": "dropped"},
        "turn_id": null,
        "request_id": null,
    });
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "agent.command",
            params.clone(),
            OP_TIMEOUT,
            Quiesce::SilenceAfterMarker,
        )
        .await;
    assert_eq!(outcome, Err("unknown".to_owned()));
    assert!(committed.is_empty());
    // The replay is unknown without new native I/O.
    let (outcome, _) = scenario
        .host
        .pump_call("agent.command", params, OP_TIMEOUT, Quiesce::SilenceNow)
        .await;
    assert_eq!(outcome, Err("unknown".to_owned()));
    let prompts = scenario
        .requests()
        .iter()
        .filter(|entry| {
            entry
                .get("path")
                .and_then(Value::as_str)
                .is_some_and(|path| path.ends_with("/prompt"))
        })
        .count();
    assert_eq!(prompts, 1, "exactly one prompt write");
    // Shutdown ends the unstopped generation, so it commits session.ended.
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "shutdown",
            json!({}),
            OP_TIMEOUT,
            Quiesce::UntilKind("session.ended"),
        )
        .await;
    assert_eq!(
        outcome
            .expect("shutdown")
            .get("state")
            .and_then(Value::as_str),
        Some("shutdown")
    );
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario.host.shutdown_child().await;
}

async fn scenario_stop_of_gone_session_applies(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({ "delete_status": 404 })),
        opencode_profile(),
        owned,
    )
    .await;
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario.host.call("shutdown", json!({})).await.unwrap();
    scenario.host.shutdown_child().await;
}

async fn scenario_invalid_profile_rejected_before_spawn(owned: OwnedChildren) {
    // (cmd wrapper rejection lives in the daemon config layer, next to the
    // Muse rule it mirrors; the plugin trusts the frozen profile there.)
    let cases: Vec<(Value, Option<&str>)> = vec![
        (json!({"harness": "opencode", "model": MODEL}), None),
        (
            json!({
                "harness": "opencode",
                "model": "no-slash-model",
                "cmd": "C:/Tools/opencode.exe",
                "pass_env": [],
            }),
            None,
        ),
        (
            json!({
                "harness": "opencode",
                "model": MODEL,
                "cmd": "C:/Tools/opencode.exe",
                "pass_env": [],
            }),
            Some("missing-workspace"),
        ),
    ];
    for (profile, workspace_override) in cases {
        let agent_id = ulid::Ulid::new().to_string();
        let root: PathBuf = std::env::temp_dir().join(format!(
            "eliot-opencode-plugin-test-{}",
            ulid::Ulid::new().to_string().to_lowercase()
        ));
        std::fs::create_dir_all(root.join("data")).unwrap();
        std::fs::create_dir_all(root.join("instance")).unwrap();
        std::fs::create_dir_all(root.join("workspace")).unwrap();
        let workspace = match workspace_override {
            Some(name) => root.join(name).to_string_lossy().into_owned(),
            None => root.join("workspace").to_string_lossy().into_owned(),
        };
        let mut host = Host::spawn(&root, owned.clone()).await;
        host.call(
            "initialize",
            json!({
                "protocol": "eliot-plugin/1",
                "credential": format!("credential-{}", ulid::Ulid::new()),
                "instance_dir": root.join("instance").to_string_lossy(),
                "config": {},
            }),
        )
        .await
        .expect("initialize");
        let outcome = host
            .call(
                "agent.start",
                json!({
                    "agent_id": agent_id,
                    "generation": 1,
                    "command_id": ulid::Ulid::new().to_string(),
                    "profile": profile,
                    "workspace": workspace,
                    "target_environment": {},
                    "note": null,
                    "source_plan": {"session": "stream", "turn": "stream", "message": "stream", "request": "stream"},
                    "source_state": null,
                    "resume_from": null,
                    "initial": null,
                }),
            )
            .await;
        assert_eq!(outcome, Err("profile_error".to_owned()), "{profile}");
        // No server spawned: the plugin never got past validation, and no
        // per-Agent config was generated.
        assert!(
            !root.join("instance").join("config").exists(),
            "no config without a spawn"
        );
        host.call("shutdown", json!({})).await.unwrap();
        host.shutdown_child().await;
    }
}

async fn scenario_sse_loss_during_send_gaps(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({ "drop_sse_on_prompt": true })),
        opencode_profile(),
        owned,
    )
    .await;
    // The prompt is admitted (200) but the stream dies before any
    // association fact flows: the send settles unknown and the loss gaps
    // the source. The gap may land on either side of the response.
    let send_id = ulid::Ulid::new().to_string();
    let agent_id = scenario.agent_id.clone();
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "agent.command",
            json!({
                "agent_id": agent_id,
                "generation": 1,
                "command_id": send_id,
                "verb": "send",
                "args": {"text": "lost"},
                "turn_id": null,
                "request_id": null,
            }),
            OP_TIMEOUT,
            Quiesce::UntilKind("source.gap"),
        )
        .await;
    assert_eq!(outcome, Err("unknown".to_owned()));
    let gap = committed
        .iter()
        .find(|(kind, _)| kind == "source.gap")
        .expect("the stream loss gaps the source");
    assert_eq!(gap.1.get("source").and_then(Value::as_str), Some("stream"));
    let reason = gap
        .1
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        reason.contains("event stream closed"),
        "loss reason names the stream: {reason}"
    );
    // Exactly one prompt write; the admitted input is never repeated.
    let prompts = scenario
        .requests()
        .iter()
        .filter(|entry| {
            entry
                .get("path")
                .and_then(Value::as_str)
                .is_some_and(|path| path.ends_with("/prompt"))
        })
        .count();
    assert_eq!(prompts, 1);
    // Shutdown ends the unstopped generation, so it commits session.ended.
    let (outcome, committed) = scenario
        .host
        .pump_call(
            "shutdown",
            json!({}),
            OP_TIMEOUT,
            Quiesce::UntilKind("session.ended"),
        )
        .await;
    assert_eq!(
        outcome
            .expect("shutdown")
            .get("state")
            .and_then(Value::as_str),
        Some("shutdown")
    );
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario.host.shutdown_child().await;
}

/// Build the daemon resume subject for a retained generation-1 ready: the
/// exact scope/ref plus the retained source readback with its completed
/// turn.
fn resume_subject(agent_id: &str, ready: &Value, turn_id: &str) -> (Value, Value) {
    let resume_from = json!({
        "native_scope": ready.get("native_scope").cloned().unwrap_or(Value::Null),
        "native_ref": ready.get("native_ref").cloned().unwrap_or(Value::Null),
    });
    let source_state = json!({
        "retained_generation": 1,
        "retained_source": {
            "agent_id": agent_id,
            "generation": 1,
            "source": "stream",
            "families": ["session", "turn", "message"],
        },
        "current_turn": {
            "turn_id": turn_id, "state": "completed", "status": "completed",
            "started_at": 1, "completed_at": 2,
        },
    });
    (resume_from, source_state)
}

async fn scenario_resume_reattaches_retained_session(owned: OwnedChildren) {
    let root: PathBuf = std::env::temp_dir().join(format!(
        "eliot-opencode-resume-test-{}",
        ulid::Ulid::new().to_string().to_lowercase()
    ));
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::create_dir_all(root.join("workspace")).unwrap();
    let agent_id = ulid::Ulid::new().to_string();
    let workspace = root.join("workspace").to_string_lossy().into_owned();
    // Generation 1 runs one completed turn, then dies without stopping
    // (the Failed/Lost shape): its plugin is dropped, so kill_on_drop ends
    // it while the retained session survives in the per-Agent store.
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance",
        "requests.jsonl",
        "report.json",
        &script(json!({})),
        opencode_profile(),
        agent_id.clone(),
        1,
        None,
        None,
        owned.clone(),
    )
    .await;
    let (mut gen1, ready1) = pending.finish(rx).await;
    let (_, turn_id, _) = send_turn(&mut gen1, 1, "retained work").await;
    let retained_ref = ready1
        .get("native_ref")
        .and_then(Value::as_str)
        .expect("retained ref")
        .to_owned();
    let retained_scope = ready1
        .get("native_scope")
        .and_then(Value::as_str)
        .expect("retained scope")
        .to_owned();
    let home1 = report_at(&root, "report.json")["data_home"]
        .as_str()
        .expect("gen-1 data home")
        .to_owned();
    let resume = resume_subject(&agent_id, &ready1, &turn_id);
    drop(gen1);
    // Generation 2 reattaches: the seeded fake stands in for the persisted
    // store, continuing the aggregate at the honest next sequence.
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance2",
        "requests2.jsonl",
        "report2.json",
        &script(json!({
            "seed_session": {
                "id": retained_ref,
                "model_id": "test-model",
                "provider_id": "test-provider",
                "directory": workspace,
                "seq": 5,
            },
        })),
        opencode_profile(),
        agent_id.clone(),
        2,
        Some(resume),
        None,
        owned,
    )
    .await;
    let (mut gen2, ready2) = pending.finish(rx).await;
    assert_eq!(
        ready2.get("native_ref").and_then(Value::as_str),
        Some(retained_ref.as_str()),
        "resume reattaches the exact retained session without replay"
    );
    assert_eq!(
        ready2.get("native_scope").and_then(Value::as_str),
        Some(retained_scope.as_str())
    );
    let home2 = report_at(&root, "report2.json")["data_home"]
        .as_str()
        .expect("gen-2 data home")
        .to_owned();
    assert_eq!(home1, home2, "both generations serve the stable store");
    assert!(
        home2.contains(&format!("opencode-{agent_id}")),
        "stable store is per Agent: {home2}"
    );
    let requests = requests_at(&root, "requests2.jsonl");
    assert_eq!(
        count_requests(&requests, "POST", "/prompt"),
        0,
        "resume never replays the prompt"
    );
    // The adopted live edge tracks the continued aggregate: no gap, and a
    // post-resume send completes on it.
    let (_, _, committed) = send_turn(&mut gen2, 2, "continued work").await;
    assert_eq!(
        kinds(&committed),
        ["turn.started", "message.completed", "turn.completed"]
    );
    let committed = stop_agent(&mut gen2, 2, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    gen2.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen2.host.shutdown_child().await;
}

async fn scenario_resume_aborts_retained_running_execution(owned: OwnedChildren) {
    let root: PathBuf = std::env::temp_dir().join(format!(
        "eliot-opencode-resume-abort-test-{}",
        ulid::Ulid::new().to_string().to_lowercase()
    ));
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::create_dir_all(root.join("workspace")).unwrap();
    let agent_id = ulid::Ulid::new().to_string();
    let workspace = root.join("workspace").to_string_lossy().into_owned();
    // Generation 1 runs one completed turn, then dies without stopping.
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance",
        "requests.jsonl",
        "report.json",
        &script(json!({})),
        opencode_profile(),
        agent_id.clone(),
        1,
        None,
        None,
        owned.clone(),
    )
    .await;
    let (mut gen1, ready1) = pending.finish(rx).await;
    let (_, turn_id, _) = send_turn(&mut gen1, 1, "retained work").await;
    let retained_ref = ready1
        .get("native_ref")
        .and_then(Value::as_str)
        .expect("retained ref")
        .to_owned();
    let resume = resume_subject(&agent_id, &ready1, &turn_id);
    drop(gen1);
    // Generation 2 finds the retained execution still running: the resume
    // quiesce aborts it (two interrupts: the abort plus the drain confirm)
    // and the dead generation's terminal drops silently — no gap, no replay.
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance2",
        "requests2.jsonl",
        "report2.json",
        &script(json!({
            "seed_session": {
                "id": retained_ref,
                "model_id": "test-model",
                "provider_id": "test-provider",
                "directory": workspace,
                "seq": 5,
                "live": true,
            },
        })),
        opencode_profile(),
        agent_id.clone(),
        2,
        Some(resume),
        None,
        owned,
    )
    .await;
    let (mut gen2, ready2) = pending.finish(rx).await;
    assert_eq!(
        ready2.get("native_ref").and_then(Value::as_str),
        Some(retained_ref.as_str()),
        "resume reattaches the exact retained session without replay"
    );
    let requests = requests_at(&root, "requests2.jsonl");
    assert_eq!(count_requests(&requests, "POST", "/interrupt"), 2);
    assert_eq!(
        count_requests(&requests, "POST", "/prompt"),
        0,
        "resume never replays the prompt"
    );
    // A post-resume send completes on the adopted edge without a gap.
    let (_, _, committed) = send_turn(&mut gen2, 2, "continued work").await;
    assert_eq!(
        kinds(&committed),
        ["turn.started", "message.completed", "turn.completed"]
    );
    let committed = stop_agent(&mut gen2, 2, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    gen2.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen2.host.shutdown_child().await;
}

async fn scenario_resume_of_deleted_session_is_not_found(owned: OwnedChildren) {
    let root: PathBuf = std::env::temp_dir().join(format!(
        "eliot-opencode-resume-gone-test-{}",
        ulid::Ulid::new().to_string().to_lowercase()
    ));
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::create_dir_all(root.join("workspace")).unwrap();
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance",
        "requests.jsonl",
        "report.json",
        &script(json!({})),
        opencode_profile(),
        agent_id.clone(),
        1,
        None,
        None,
        owned.clone(),
    )
    .await;
    let (mut gen1, ready1) = pending.finish(rx).await;
    let (_, turn_id, _) = send_turn(&mut gen1, 1, "doomed work").await;
    let resume = resume_subject(&agent_id, &ready1, &turn_id);
    // A clean stop deletes the native session: nothing remains to resume.
    stop_agent(&mut gen1, 1, false).await;
    gen1.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen1.host.shutdown_child().await;
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance2",
        "requests2.jsonl",
        "report2.json",
        &script(json!({})),
        opencode_profile(),
        agent_id.clone(),
        2,
        Some(resume),
        None,
        owned,
    )
    .await;
    let mut failed = pending.finish_expecting_error(rx, "not_found").await;
    failed.retry("not_found").await;
    // The preflight read the retained id and proved it absent.
    let requests = requests_at(&root, "requests2.jsonl");
    assert!(
        requests.iter().any(|entry| {
            entry.get("method").and_then(Value::as_str) == Some("GET")
                && entry
                    .get("path")
                    .and_then(Value::as_str)
                    .is_some_and(|path| path.starts_with("/api/session/ses_"))
        }),
        "resume read the retained session"
    );
}

async fn scenario_resume_refuses_pending_requests(owned: OwnedChildren) {
    let root: PathBuf = std::env::temp_dir().join(format!(
        "eliot-opencode-resume-busy-test-{}",
        ulid::Ulid::new().to_string().to_lowercase()
    ));
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::create_dir_all(root.join("workspace")).unwrap();
    let agent_id = ulid::Ulid::new().to_string();
    let workspace = root.join("workspace").to_string_lossy().into_owned();
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance",
        "requests.jsonl",
        "report.json",
        &script(json!({})),
        opencode_profile(),
        agent_id.clone(),
        1,
        None,
        None,
        owned.clone(),
    )
    .await;
    let (mut gen1, ready1) = pending.finish(rx).await;
    let (_, turn_id, _) = send_turn(&mut gen1, 1, "retained work").await;
    let retained_ref = ready1
        .get("native_ref")
        .and_then(Value::as_str)
        .expect("retained ref")
        .to_owned();
    let resume = resume_subject(&agent_id, &ready1, &turn_id);
    drop(gen1);
    // The retained session still holds a permission and a form: the resume
    // quiesces first, then refuses without adopting either.
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance2",
        "requests2.jsonl",
        "report2.json",
        &script(json!({
            "seed_session": {
                "id": retained_ref,
                "model_id": "test-model",
                "provider_id": "test-provider",
                "directory": workspace,
                "seq": 5,
            },
            "seed_permissions": [
                {"id": "per_stuck1", "action": "read", "resources": ["stuck.env"]},
            ],
            "seed_forms": [
                {"id": "frm_stuck1", "title": "stuck?", "fields": [
                    {"key": "yn", "type": "boolean"},
                ]},
            ],
        })),
        opencode_profile(),
        agent_id.clone(),
        2,
        Some(resume),
        None,
        owned,
    )
    .await;
    let mut failed = pending.finish_expecting_error(rx, "unknown").await;
    failed.retry("unknown").await;
    let requests = requests_at(&root, "requests2.jsonl");
    assert_eq!(count_requests(&requests, "POST", "/interrupt"), 1);
    assert_eq!(count_requests(&requests, "GET", "/permission"), 1);
    assert_eq!(count_requests(&requests, "GET", "/form"), 1);
}

async fn scenario_resume_realigns_model_and_agent(owned: OwnedChildren) {
    let root: PathBuf = std::env::temp_dir().join(format!(
        "eliot-opencode-resume-config-test-{}",
        ulid::Ulid::new().to_string().to_lowercase()
    ));
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::create_dir_all(root.join("workspace")).unwrap();
    let agent_id = ulid::Ulid::new().to_string();
    let workspace = root.join("workspace").to_string_lossy().into_owned();
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance",
        "requests.jsonl",
        "report.json",
        &script(json!({})),
        opencode_profile(),
        agent_id.clone(),
        1,
        None,
        None,
        owned.clone(),
    )
    .await;
    let (mut gen1, ready1) = pending.finish(rx).await;
    let (_, turn_id, _) = send_turn(&mut gen1, 1, "retained work").await;
    let retained_ref = ready1
        .get("native_ref")
        .and_then(Value::as_str)
        .expect("retained ref")
        .to_owned();
    let resume = resume_subject(&agent_id, &ready1, &turn_id);
    drop(gen1);
    // The retained session runs another model and agent: resume switches
    // both to the frozen profile (agent "general" must be installed) and
    // proves the effective values with a fresh readback.
    let mut profile = opencode_profile();
    profile["agent"] = Value::String("general".to_owned());
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance2",
        "requests2.jsonl",
        "report2.json",
        &script(json!({
            "seed_session": {
                "id": retained_ref,
                "model_id": "other-model",
                "provider_id": "other-provider",
                "agent": "build",
                "directory": workspace,
                "seq": 5,
            },
        })),
        profile,
        agent_id.clone(),
        2,
        Some(resume),
        None,
        owned,
    )
    .await;
    let (mut gen2, ready2) = pending.finish(rx).await;
    assert_eq!(
        ready2.get("native_ref").and_then(Value::as_str),
        Some(retained_ref.as_str())
    );
    assert_eq!(
        ready2["effective"].get("model").and_then(Value::as_str),
        Some("test-provider/test-model")
    );
    assert_eq!(
        ready2["effective"].get("agent").and_then(Value::as_str),
        Some("general")
    );
    assert_eq!(
        ready2["effective"]
            .get("agent_verified")
            .and_then(Value::as_bool),
        Some(true)
    );
    let requests = requests_at(&root, "requests2.jsonl");
    assert_eq!(count_requests(&requests, "GET", "/api/agent"), 1);
    assert_eq!(count_requests(&requests, "POST", "/model"), 1);
    assert_eq!(count_requests(&requests, "POST", "/agent"), 1);
    // The configure confirmations flow through the adopted edge; a
    // post-resume send still completes without a gap.
    let (_, _, committed) = send_turn(&mut gen2, 2, "continued work").await;
    assert_eq!(
        kinds(&committed),
        ["turn.started", "message.completed", "turn.completed"]
    );
    let committed = stop_agent(&mut gen2, 2, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    gen2.host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    gen2.host.shutdown_child().await;
}

async fn scenario_permission_approve_deny_roundtrip(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({
            "manual_terminal": true,
            "asks": [
                {"permission": [
                    {"id": "per_a1", "action": "read", "resources": ["a.env"]},
                    {"id": "per_b1", "action": "edit", "resources": ["b.txt"]},
                ]},
                {"permission": [
                    {"id": "per_c1", "action": "read", "resources": ["c.env"]},
                    {"id": "per_d1", "action": "read", "resources": ["d.env"]},
                ]},
            ],
        })),
        opencode_profile(),
        owned,
    )
    .await;
    // First turn: two asks open; `once` settles only its own request.
    let (_, turn1, first, _) = send_until_ask(&mut scenario, 1, "first asks").await;
    assert_eq!(first, "per_a1");
    let opened = drain_until(&mut scenario.host, "request.opened").await;
    let second = opened
        .iter()
        .find(|(kind, _)| kind == "request.opened")
        .and_then(|(_, data)| data.get("request_id").and_then(Value::as_str))
        .expect("second opened id");
    assert_eq!(second, "per_b1");
    let (approve_id, outcome, committed) = control(
        &mut scenario,
        1,
        "approve",
        Some(&turn1),
        Some("per_a1"),
        json!({"choice_id": "always"}),
        Quiesce::UntilKind("request.resolved"),
    )
    .await;
    let applied = outcome.expect("approve applies");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(kinds(&committed), ["request.resolved"]);
    assert_eq!(
        committed[0].1.get("command_id").and_then(Value::as_str),
        Some(approve_id.as_str())
    );
    // The sibling survives `once`-style selects: deny rejects it.
    let (deny_id, outcome, committed) = control(
        &mut scenario,
        1,
        "deny",
        Some(&turn1),
        Some("per_b1"),
        json!({}),
        Quiesce::UntilKind("request.resolved"),
    )
    .await;
    let applied = outcome.expect("deny applies");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(kinds(&committed), ["request.resolved"]);
    assert_eq!(
        committed[0].1.get("command_id").and_then(Value::as_str),
        Some(deny_id.as_str())
    );
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "interrupt",
        Some(&turn1),
        None,
        json!({}),
        Quiesce::UntilKind("turn.completed"),
    )
    .await;
    assert_eq!(
        outcome
            .expect("interrupt applies")
            .get("state")
            .and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(kinds(&committed), ["message.completed", "turn.completed"]);
    // Second turn: a `reject` settles every pending permission â€” the
    // sibling closes without a command binding.
    let (_, turn2, first, _) = send_until_ask(&mut scenario, 1, "second asks").await;
    assert_eq!(first, "per_c1");
    drain_until(&mut scenario.host, "request.opened").await;
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "deny",
        Some(&turn2),
        Some("per_c1"),
        json!({}),
        Quiesce::UntilKind("request.closed"),
    )
    .await;
    assert_eq!(
        outcome
            .expect("deny applies")
            .get("state")
            .and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(kinds(&committed), ["request.resolved", "request.closed"]);
    assert_eq!(
        committed[0].1.get("request_id").and_then(Value::as_str),
        Some("per_c1")
    );
    assert!(committed[0].1.get("command_id").is_some());
    assert_eq!(
        committed[1].1.get("request_id").and_then(Value::as_str),
        Some("per_d1")
    );
    assert!(committed[1].1.get("command_id").is_none());
    let (_, outcome, _) = control(
        &mut scenario,
        1,
        "interrupt",
        Some(&turn2),
        None,
        json!({}),
        Quiesce::UntilKind("turn.completed"),
    )
    .await;
    assert_eq!(
        outcome
            .expect("interrupt applies")
            .get("state")
            .and_then(Value::as_str),
        Some("applied")
    );
    // Exactly three reply writes ran (always, reject, reject): every other
    // reply round tripped without new native I/O.
    let requests = requests_at(&scenario.root, "requests.jsonl");
    assert_eq!(count_requests(&requests, "POST", "/reply"), 3);
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_form_answer_and_deny_cancel(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({
            "terminal_on_reply": true,
            "asks": [
                {"form": {"id": "frm_ans1", "title": "Pick one", "fields": [
                    {"key": "choice", "title": "Pick", "type": "string",
                     "options": [{"value": "a", "label": "Alpha"},
                                 {"value": "b", "label": "Beta"}]},
                    {"key": "note", "type": "string"},
                ]}},
                {"form": {"id": "frm_can1", "title": "Skip me", "fields": [
                    {"key": "t", "type": "string"},
                ]}},
            ],
        })),
        opencode_profile(),
        owned,
    )
    .await;
    let (_, turn1, asked, _) = send_until_ask(&mut scenario, 1, "first form").await;
    assert_eq!(asked, "frm_ans1");
    let (answer_id, outcome, committed) = control(
        &mut scenario,
        1,
        "answer",
        Some(&turn1),
        Some("frm_ans1"),
        json!({"answers": {"choice": "a", "note": "free text"}}),
        Quiesce::UntilKind("request.resolved"),
    )
    .await;
    assert_eq!(
        outcome
            .expect("answer applies")
            .get("state")
            .and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(kinds(&committed), ["request.resolved"]);
    assert_eq!(
        committed[0].1.get("command_id").and_then(Value::as_str),
        Some(answer_id.as_str())
    );
    // The execution resumes past the answer and completes on the stream.
    let rest = drain_until(&mut scenario.host, "turn.completed").await;
    assert_eq!(kinds(&rest), ["message.completed", "turn.completed"]);
    let (_, turn2, asked, _) = send_until_ask(&mut scenario, 1, "second form").await;
    assert_eq!(asked, "frm_can1");
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "deny",
        Some(&turn2),
        Some("frm_can1"),
        json!({}),
        Quiesce::UntilKind("request.closed"),
    )
    .await;
    assert_eq!(
        outcome
            .expect("deny cancels")
            .get("state")
            .and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(kinds(&committed), ["request.closed"]);
    assert!(
        committed[0].1.get("command_id").is_none(),
        "a native cancel carries no Eliot command binding"
    );
    let rest = drain_until(&mut scenario.host, "turn.completed").await;
    assert_eq!(kinds(&rest), ["message.completed", "turn.completed"]);
    // One answer write plus one native cancel ran.
    let requests = requests_at(&scenario.root, "requests.jsonl");
    assert_eq!(count_requests(&requests, "POST", "/reply"), 1);
    assert_eq!(count_requests(&requests, "DELETE", "/form/frm_can1"), 1);
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_stale_choice_rejected_without_write(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({
            "terminal_on_reply": true,
            "asks": [
                {"form": {"id": "frm_stale1", "title": "Pick one", "fields": [
                    {"key": "choice", "type": "string",
                     "options": [{"value": "a", "label": "Alpha"}]},
                ]}},
            ],
        })),
        opencode_profile(),
        owned,
    )
    .await;
    let (_, turn_id, asked, _) = send_until_ask(&mut scenario, 1, "stale choice").await;
    assert_eq!(asked, "frm_stale1");
    // A non-offered option never reaches the wire: rejected, silent.
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "answer",
        Some(&turn_id),
        Some("frm_stale1"),
        json!({"answers": {"choice": "zzz"}}),
        Quiesce::SilenceNow,
    )
    .await;
    let rejected = outcome.expect("stale answer is rejected, not failed");
    assert_eq!(
        rejected.get("state").and_then(Value::as_str),
        Some("rejected")
    );
    assert!(committed.is_empty());
    let requests = requests_at(&scenario.root, "requests.jsonl");
    assert_eq!(count_requests(&requests, "POST", "/reply"), 0);
    // The offered answer still applies afterwards: the lane never latched.
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "answer",
        Some(&turn_id),
        Some("frm_stale1"),
        json!({"answers": {"choice": "a"}}),
        Quiesce::UntilKind("request.resolved"),
    )
    .await;
    assert_eq!(
        outcome
            .expect("offered answer applies")
            .get("state")
            .and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(kinds(&committed), ["request.resolved"]);
    drain_until(&mut scenario.host, "turn.completed").await;
    let requests = requests_at(&scenario.root, "requests.jsonl");
    assert_eq!(count_requests(&requests, "POST", "/reply"), 1);
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_server_refused_answer_is_rejected(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({
            "terminal_on_reply": true,
            "asks": [
                {"form": {"id": "frm_type1", "title": "Flag?", "fields": [
                    {"key": "flag", "type": "boolean"},
                ]}},
            ],
        })),
        opencode_profile(),
        owned,
    )
    .await;
    let (_, turn_id, asked, _) = send_until_ask(&mut scenario, 1, "typed answer").await;
    assert_eq!(asked, "frm_type1");
    // The client stages keys and options; the server owns value types and
    // answers an empty 400: rejected, and the request stays open.
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "answer",
        Some(&turn_id),
        Some("frm_type1"),
        json!({"answers": {"flag": "yes"}}),
        Quiesce::SilenceNow,
    )
    .await;
    let rejected = outcome.expect("mistyped answer is rejected, not failed");
    assert_eq!(
        rejected.get("state").and_then(Value::as_str),
        Some("rejected")
    );
    assert!(committed.is_empty());
    let requests = requests_at(&scenario.root, "requests.jsonl");
    assert_eq!(count_requests(&requests, "POST", "/reply"), 1);
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "answer",
        Some(&turn_id),
        Some("frm_type1"),
        json!({"answers": {"flag": true}}),
        Quiesce::UntilKind("request.resolved"),
    )
    .await;
    assert_eq!(
        outcome
            .expect("typed answer applies")
            .get("state")
            .and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(kinds(&committed), ["request.resolved"]);
    drain_until(&mut scenario.host, "turn.completed").await;
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_lost_reply_is_unknown_without_rewrite(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({
            "terminal_on_reply": true,
            "reply_drop": true,
            "asks": [
                {"permission": {"id": "per_lost1", "action": "read",
                                "resources": ["lost.env"]}},
            ],
        })),
        opencode_profile(),
        owned,
    )
    .await;
    let (_, turn_id, asked, _) = send_until_ask(&mut scenario, 1, "lost reply").await;
    assert_eq!(asked, "per_lost1");
    let command_id = ulid::Ulid::new().to_string();
    let (_, outcome, committed) = control_as(
        &mut scenario,
        1,
        &command_id,
        "approve",
        Some(&turn_id),
        Some("per_lost1"),
        json!({}),
        Quiesce::SilenceNow,
    )
    .await;
    let unknown = outcome.expect("lost reply settles unknown, not failed");
    assert_eq!(
        unknown.get("state").and_then(Value::as_str),
        Some("unknown")
    );
    assert!(committed.is_empty());
    // The same command retries unknown without a second write.
    let (_, outcome, committed) = control_as(
        &mut scenario,
        1,
        &command_id,
        "approve",
        Some(&turn_id),
        Some("per_lost1"),
        json!({}),
        Quiesce::SilenceNow,
    )
    .await;
    let unknown = outcome.expect("retry settles unknown, not failed");
    assert_eq!(
        unknown.get("state").and_then(Value::as_str),
        Some("unknown")
    );
    assert!(committed.is_empty());
    let requests = requests_at(&scenario.root, "requests.jsonl");
    assert_eq!(count_requests(&requests, "POST", "/reply"), 1);
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_reply_to_settled_request_closes_it(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({
            "manual_terminal": true,
            "ghost_settle": true,
            "asks": [
                {"permission": {"id": "per_ghost1", "action": "read",
                                "resources": ["ghost.env"]}},
            ],
        })),
        opencode_profile(),
        owned,
    )
    .await;
    let (_, turn_id, asked, _) = send_until_ask(&mut scenario, 1, "ghost ask").await;
    assert_eq!(asked, "per_ghost1");
    // The settle event was lost, so the request is open locally but gone
    // natively: the reply meets a 404, rejects, and closes the phantom
    // instead of rotting it open.
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "approve",
        Some(&turn_id),
        Some("per_ghost1"),
        json!({}),
        Quiesce::UntilKind("request.closed"),
    )
    .await;
    let rejected = outcome.expect("ghost reply is rejected, not failed");
    assert_eq!(
        rejected.get("state").and_then(Value::as_str),
        Some("rejected")
    );
    assert_eq!(kinds(&committed), ["request.closed"]);
    assert_eq!(
        committed[0].1.get("request_id").and_then(Value::as_str),
        Some("per_ghost1")
    );
    assert!(committed[0].1.get("command_id").is_none());
    // A second reply finds nothing pending.
    let (_, outcome, _) = control(
        &mut scenario,
        1,
        "approve",
        Some(&turn_id),
        Some("per_ghost1"),
        json!({}),
        Quiesce::SilenceNow,
    )
    .await;
    assert_eq!(
        outcome.expect_err("closed request is not_found"),
        "not_found"
    );
    let (_, outcome, _) = control(
        &mut scenario,
        1,
        "interrupt",
        Some(&turn_id),
        None,
        json!({}),
        Quiesce::UntilKind("turn.completed"),
    )
    .await;
    assert_eq!(
        outcome
            .expect("interrupt applies")
            .get("state")
            .and_then(Value::as_str),
        Some("applied")
    );
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_interrupt_applies_and_terminal_settles(owned: OwnedChildren) {
    let mut scenario = Scenario::start(
        &script(json!({ "manual_terminal": true })),
        opencode_profile(),
        owned,
    )
    .await;
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "send",
        None,
        None,
        json!({"text": "long work"}),
        Quiesce::UntilKind("turn.started"),
    )
    .await;
    let applied = outcome.expect("send applies");
    let turn_id = applied
        .get("turn_id")
        .and_then(Value::as_str)
        .expect("applied turn id")
        .to_owned();
    assert_eq!(kinds(&committed), ["turn.started"]);
    // The interrupt aborts the exact live execution; its `interrupted`
    // terminal settles the turn on the stream with the interrupted status.
    let (interrupt_id, outcome, committed) = control(
        &mut scenario,
        1,
        "interrupt",
        Some(&turn_id),
        None,
        json!({}),
        Quiesce::UntilKind("turn.completed"),
    )
    .await;
    let applied = outcome.expect("interrupt applies");
    assert_eq!(
        applied.get("state").and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(
        applied.get("command_id").and_then(Value::as_str),
        Some(interrupt_id.as_str())
    );
    assert_eq!(
        applied.get("turn_id").and_then(Value::as_str),
        Some(turn_id.as_str())
    );
    assert!(applied.get("request_id").is_none());
    assert_eq!(kinds(&committed), ["message.completed", "turn.completed"]);
    assert_eq!(
        committed[1].1.get("status").and_then(Value::as_str),
        Some("interrupted")
    );
    let requests = requests_at(&scenario.root, "requests.jsonl");
    assert_eq!(count_requests(&requests, "POST", "/interrupt"), 1);
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_interrupt_after_terminal_is_stale(owned: OwnedChildren) {
    let mut scenario = Scenario::start(&script(json!({})), opencode_profile(), owned).await;
    let (_, turn_id, _) = send_turn(&mut scenario, 1, "quick work").await;
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "interrupt",
        Some(&turn_id),
        None,
        json!({}),
        Quiesce::SilenceNow,
    )
    .await;
    assert_eq!(outcome.expect_err("finished turn is stale"), "stale_turn");
    assert!(committed.is_empty());
    // A bogus turn id is stale too, and neither wrote anything.
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "interrupt",
        Some("evt_bogus_turn"),
        None,
        json!({}),
        Quiesce::SilenceNow,
    )
    .await;
    assert_eq!(outcome.expect_err("bogus turn is stale"), "stale_turn");
    assert!(committed.is_empty());
    let requests = requests_at(&scenario.root, "requests.jsonl");
    assert_eq!(count_requests(&requests, "POST", "/interrupt"), 0);
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}

async fn scenario_request_details_commit_redacted(owned: OwnedChildren) {
    let credential = format!("redact-me-{}", ulid::Ulid::new());
    let root: PathBuf = std::env::temp_dir().join(format!(
        "eliot-opencode-redact-test-{}",
        ulid::Ulid::new().to_string().to_lowercase()
    ));
    std::fs::create_dir_all(root.join("data")).unwrap();
    std::fs::create_dir_all(root.join("workspace")).unwrap();
    let agent_id = ulid::Ulid::new().to_string();
    let (pending, rx) = Scenario::setup_start_in(
        &root,
        "instance",
        "requests.jsonl",
        "report.json",
        &script(json!({
            "terminal_on_reply": true,
            "asks": [
                {"permission": {"id": "per_sec1", "action": "read",
                                "resources": ["secret.env"],
                                "message": format!("token {credential} inside")}},
            ],
        })),
        opencode_profile(),
        agent_id.clone(),
        1,
        None,
        Some(credential.clone()),
        owned,
    )
    .await;
    let (mut scenario, _) = pending.finish(rx).await;
    let (_, turn_id, asked, committed) = send_until_ask(&mut scenario, 1, "secret ask").await;
    assert_eq!(asked, "per_sec1");
    // The committed offer masks the credential while the raw in-memory
    // offer keeps matching: the reply still applies.
    let opened = committed
        .iter()
        .find(|(kind, _)| kind == "request.opened")
        .map(|(_, data)| data)
        .expect("opened details");
    let encoded = serde_json::to_string(opened).expect("encode opened");
    assert!(!encoded.contains(&credential), "{encoded}");
    let (_, outcome, committed) = control(
        &mut scenario,
        1,
        "approve",
        Some(&turn_id),
        Some("per_sec1"),
        json!({}),
        Quiesce::UntilKind("request.resolved"),
    )
    .await;
    assert_eq!(
        outcome
            .expect("approve applies")
            .get("state")
            .and_then(Value::as_str),
        Some("applied")
    );
    assert_eq!(kinds(&committed), ["request.resolved"]);
    drain_until(&mut scenario.host, "turn.completed").await;
    let committed = stop_agent(&mut scenario, 1, false).await;
    assert_eq!(kinds(&committed), ["session.ended"]);
    scenario
        .host
        .call("shutdown", json!({}))
        .await
        .expect("shutdown");
    scenario.host.shutdown_child().await;
}
