//! Panic capture for the daemon and every plugin binary.
//!
//! The same swarm binary serves the daemon (`swarm daemon --foreground`)
//! and every plugin (`swarm plugin <harness>`), so one hook with a
//! mode-aware code covers both: `daemon.panicked` in the daemon,
//! `plugin.panicked` in plugin mode. The hook records the thread,
//! `file:line`, a payload-omitted notice and a bounded payload-free
//! backtrace. The daemon writes the marker file under its crash directory
//! and echoes one `eliot_crash` JSON line to stderr (a WMI-started daemon has
//! no console, so the file is the durable path). A plugin writes no file: it
//! reports on its owned stderr pipe as `eliot_crash <nonce> <pid> <json>`,
//! with the per-spawn nonce the daemon wrote as the first stdin line, and the
//! observing daemon records only reports bound to that spawn. The daemon's
//! crash directory is set explicitly by its entry point; no environment
//! variable selects it. The hook never touches the
//! Store, never allocates unboundedly, and never panics itself: logging
//! and settlement of the marker happen after restart (daemon) or in the
//! observing daemon (plugin), never inside the hook. Arbitrary panic
//! payloads are never classified as safe, even when no policy is available.
//!
//! Markers are consumed once: daemon startup loads the latest
//! `daemon.panicked` marker into `daemon.status` and deletes it; status
//! reads consume validated `plugin.panicked` markers. Plugin error
//! settlement also reads only the exact owned plugin's stderr pipe.

use eliot_core::{
    SecretPolicy,
    api::{CrashMarker, truncate_bytes},
};
use eliot_plugin::sync::LockExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, TryLockError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

static INSTALLED: AtomicBool = AtomicBool::new(false);
/// Per-spawn nonce a plugin received as its first stdin line from its daemon.
static PLUGIN_CRASH_NONCE: OnceLock<String> = OnceLock::new();
/// Stderr line tag of a crash report.
pub const PLUGIN_CRASH_LINE: &str = "eliot_crash";
const MAX_MARKERS_PER_CODE: usize = 64;
const MAX_MARKER_BYTES: usize = 64 * 1024;
const DAEMON_MARKER_LOCK_WAIT: Duration = Duration::from_millis(50);
static DAEMON_MARKER_WRITER: Mutex<()> = Mutex::new(());
static PLUGIN_MARKER_WRITER: Mutex<()> = Mutex::new(());
static MARKER_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// In-process crash directory, set once at startup by the owning binary.
/// The panic hook reads only this explicit directory; no environment
/// variable selects it.
static CRASH_DIR: OnceLock<PathBuf> = OnceLock::new();

#[cfg(debug_assertions)]
pub(crate) const DAEMON_PANIC_TEST_METHOD: &str = "daemon.panic_test";

/// Run the installed hook to completion before aborting this daemon.
#[cfg(debug_assertions)]
pub(crate) fn panic_and_abort() -> ! {
    let _ = std::panic::catch_unwind(|| panic!("debug daemon panic-test trigger"));
    std::process::abort()
}

/// Remember the private marker directory for this process. First call wins;
/// later calls are ignored so a re-executed entry point cannot redirect
/// crash markers mid-run.
pub fn set_crash_dir(dir: &Path) {
    let _ = CRASH_DIR.set(dir.to_owned());
}

/// Read the per-spawn crash nonce: the first stdin line the owning daemon
/// writes before any RPC. It reads through the process-wide buffered stdin
/// that the plugin peer also reads, so later bytes stay available to it.
pub fn read_plugin_crash_nonce() {
    use std::io::{BufRead, Read};
    let mut line = String::new();
    if std::io::stdin()
        .lock()
        .take(130)
        .read_line(&mut line)
        .is_ok()
    {
        set_plugin_crash_nonce(line.trim_end_matches(['\r', '\n']));
    }
}

/// Remember the per-spawn crash nonce. Only a bounded alphanumeric value is
/// accepted; first call wins.
fn set_plugin_crash_nonce(nonce: &str) {
    if !nonce.is_empty()
        && nonce.len() <= 64
        && nonce.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        let _ = PLUGIN_CRASH_NONCE.set(nonce.to_owned());
    }
}

/// Marker directory under the data root. Private to the data directory,
/// like the daemon log.
pub fn crash_dir(data_root: &Path) -> PathBuf {
    data_root.join("crashes")
}

/// Install the process panic hook once for this binary: `plugin` selects
/// `plugin.panicked`, otherwise `daemon.panicked`. Later calls are no-ops.
pub fn install_panic_hook(plugin: bool) {
    install_panic_hook_for(if plugin {
        "plugin.panicked"
    } else {
        "daemon.panicked"
    });
}

/// Install the process panic hook once. Later calls are no-ops, so CLI
/// entry points and tests cannot stack hooks.
fn install_panic_hook_for(code: &'static str) {
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    // Capture known environment credentials before a panic, not while unwinding.
    let policy = SecretPolicy::new(std::env::vars().filter_map(|(name, value)| {
        let name = name.to_ascii_uppercase();
        (name.contains("TOKEN")
            || name.contains("SECRET")
            || name.contains("PASSWORD")
            || name.contains("API_KEY")
            || name.contains("CREDENTIAL"))
        .then_some(value)
    }));
    let plugin = code == "plugin.panicked";
    std::panic::set_hook(Box::new(move |info| {
        let marker = capture(code, info, &policy);
        let line = serde_json::to_string(&marker).unwrap_or_else(|_| "{}".to_owned());
        if plugin {
            // The owned stderr pipe is the plugin's only crash channel.
            match PLUGIN_CRASH_NONCE.get() {
                Some(nonce) => {
                    eprintln!("{PLUGIN_CRASH_LINE} {nonce} {} {line}", std::process::id())
                }
                None => eprintln!("{PLUGIN_CRASH_LINE} {line}"),
            }
            return;
        }
        if let Some(dir) = CRASH_DIR.get() {
            write_marker(dir, &marker);
        }
        eprintln!("{PLUGIN_CRASH_LINE} {line}");
    }));
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn capture(code: &str, info: &std::panic::PanicHookInfo<'_>, policy: &SecretPolicy) -> CrashMarker {
    let location = info
        .location()
        .map(|location| format!("{}:{}", location.file(), location.line()))
        .unwrap_or_else(|| "unknown".to_owned());
    let current = std::thread::current();
    // User-defined thread names are arbitrary strings just like payloads.
    let thread = match current.name().unwrap_or("unnamed") {
        name @ ("main" | "tokio-runtime-worker" | "unnamed") => name,
        _ => "unclassified thread name omitted",
    };
    let backtrace = format!("{}", std::backtrace::Backtrace::force_capture());
    CrashMarker {
        code: code.to_owned(),
        thread: truncate_bytes(&policy.redact_text(thread), CrashMarker::MAX_THREAD_CHARS).0,
        location: truncate_bytes(
            &policy.redact_text(&location),
            CrashMarker::MAX_LOCATION_CHARS,
        )
        .0,
        message: "panic payload omitted (unclassified)".to_owned(),
        backtrace: truncate_bytes(
            &policy.redact_text(&backtrace),
            CrashMarker::MAX_BACKTRACE_CHARS,
        )
        .0,
        timestamp: now_ms(),
    }
}

/// Best-effort marker write. Missing directories are created; each panic
/// code has its own file cap so plugin panics cannot starve daemon markers.
/// Never fails the panicking process further.
pub(crate) fn write_marker(dir: &Path, marker: &CrashMarker) {
    let _writer = if marker.code == "daemon.panicked" {
        // Give concurrent panics a chance to write a distinct marker, with
        // a bounded wait in case the first writer is stuck in filesystem I/O.
        let until = Instant::now() + DAEMON_MARKER_LOCK_WAIT;
        loop {
            match DAEMON_MARKER_WRITER.try_lock() {
                Ok(writer) => break writer,
                Err(TryLockError::Poisoned(error)) => break error.into_inner(),
                Err(TryLockError::WouldBlock) if Instant::now() < until => {
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(TryLockError::WouldBlock) => return,
            }
        }
    } else {
        PLUGIN_MARKER_WRITER.lock_recover()
    };
    #[cfg(windows)]
    let private = crate::platform::windows::ensure_private_root(dir);
    #[cfg(unix)]
    let private = crate::platform::unix::ensure_private_root(dir);
    #[cfg(not(any(windows, unix)))]
    let private: std::io::Result<()> = Err(std::io::ErrorKind::Unsupported.into());
    if private.is_err() {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let prefix = format!("{}-", marker.code);
    let existing_markers = entries
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let name = entry.file_name();
            name.to_str()
                .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".json"))
                && entry.file_type().is_ok_and(|kind| kind.is_file())
        })
        .take(MAX_MARKERS_PER_CODE)
        .count();
    if existing_markers >= MAX_MARKERS_PER_CODE {
        return;
    }
    let name = format!(
        "{}-{}-{}-{}.json",
        marker.code,
        std::process::id(),
        marker.timestamp,
        MARKER_SEQUENCE.fetch_add(1, Ordering::Relaxed),
    );
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-' || byte == b'_')
    {
        return;
    }
    if let Ok(bytes) = serde_json::to_vec(marker)
        && bytes.len() <= MAX_MARKER_BYTES
    {
        // Reuse the existing private create/write/sync/atomic-rename owner.
        let _ = crate::platform::data_dir::replace_private_file(dir, &name, &bytes);
    }
}

/// Read and consume (delete) every valid marker of one code, oldest first.
/// Published but malformed markers are discarded. Temporary files and
/// files with an inconclusive read failure stay available for later reads.
fn take_markers(dir: &Path, code: &str, policy: &SecretPolicy) -> Vec<CrashMarker> {
    let mut markers = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return markers;
    };
    let prefix = format!("{code}-");
    let mut paths: Vec<PathBuf> = entries
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".json"))
        })
        .collect();
    paths.sort();
    for path in paths {
        let parsed = match crate::platform::data_dir::read_private_bytes(&path, MAX_MARKER_BYTES) {
            Ok(bytes) => serde_json::from_slice::<CrashMarker>(&bytes)
                .ok()
                .filter(|marker| marker.code == code && marker.validate().is_ok()),
            Err(error) if error.code == "source_integrity" => None,
            Err(_) => continue,
        };
        if std::fs::remove_file(path).is_ok() {
            if let Some(marker) = parsed {
                markers.push(marker);
            } else {
                crate::logging::record(
                    policy,
                    "daemon.crash_marker_discarded",
                    &[("operation", code)],
                    None,
                );
            }
        }
    }
    markers.sort_by_key(|marker| marker.timestamp);
    markers
}

/// Previous-run crash for `daemon.status` and startup output: the latest
/// consumed `daemon.panicked` marker, redacted with the daemon secrets.
/// Consumption is once-only: files are deleted on read.
pub(crate) fn load_previous_crash(data_root: &Path, policy: &SecretPolicy) -> Option<CrashMarker> {
    let mut markers = take_markers(&crash_dir(data_root), "daemon.panicked", policy);
    markers.pop().map(|marker| redact_marker(marker, policy))
}

/// Read, redact and consume plugin panic markers for the status snapshot.
pub(crate) fn take_plugin_crashes(data_root: &Path, policy: &SecretPolicy) -> Vec<CrashMarker> {
    take_markers(&crash_dir(data_root), "plugin.panicked", policy)
        .into_iter()
        .map(|marker| redact_marker(marker, policy))
        .collect()
}

fn redact_marker(mut marker: CrashMarker, policy: &SecretPolicy) -> CrashMarker {
    marker.message = policy.redact_text(&marker.message);
    marker.backtrace = policy.redact_text(&marker.backtrace);
    marker.location = policy.redact_text(&marker.location);
    marker.thread = policy.redact_text(&marker.thread);
    marker.thread = truncate_bytes(&marker.thread, CrashMarker::MAX_THREAD_CHARS).0;
    marker.location = truncate_bytes(&marker.location, CrashMarker::MAX_LOCATION_CHARS).0;
    marker.message = truncate_bytes(&marker.message, CrashMarker::MAX_MESSAGE_CHARS).0;
    marker.backtrace = truncate_bytes(&marker.backtrace, CrashMarker::MAX_BACKTRACE_CHARS).0;
    marker
}
