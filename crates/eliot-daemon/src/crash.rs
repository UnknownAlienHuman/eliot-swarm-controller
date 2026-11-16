//! Panic capture for the daemon and every plugin binary.
//!
//! The same swarm binary serves the daemon (`swarm daemon --foreground`)
//! and every plugin (`swarm plugin <harness>`), so one hook with a
//! mode-aware code covers both: `daemon.panicked` in the daemon,
//! `plugin.panicked` in plugin mode. The hook records the thread,
//! `file:line`, a payload-omitted notice and a bounded payload-free
//! backtrace, writes the marker file under `ELIOT_CRASH_DIR`, and echoes
//! one `eliot_crash` JSON line to stderr (a WMI-started daemon has no
//! console, so the file is the durable path). The hook never touches the
//! Store, never allocates unboundedly, and never panics itself: logging
//! and settlement of the marker happen after restart (daemon) or in the
//! observing daemon (plugin), never inside the hook. Arbitrary panic
//! payloads are never classified as safe, even when no policy is available.
//!
//! Markers are consumed once: daemon startup loads the latest
//! `daemon.panicked` marker into `daemon.status` and deletes it; plugin
//! error settlement reads only the exact owned plugin's stderr pipe.

use eliot_core::{SecretPolicy, api::CrashMarker};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static INSTALLED: AtomicBool = AtomicBool::new(false);

/// In-process crash directory, set once at startup by the owning binary.
/// The panic hook prefers this over the `ELIOT_CRASH_DIR` environment so
/// startup never needs an unsafe environment write.
static CRASH_DIR: OnceLock<PathBuf> = OnceLock::new();

/// Remember the private marker directory for this process. First call wins;
/// later calls are ignored so a re-executed entry point cannot redirect
/// crash markers mid-run.
pub fn set_crash_dir(dir: &Path) {
    let _ = CRASH_DIR.set(dir.to_owned());
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
    std::panic::set_hook(Box::new(move |info| {
        let marker = capture(code, info, &policy);
        let from_process = CRASH_DIR.get().cloned();
        let from_env = std::env::var("ELIOT_CRASH_DIR").ok().map(PathBuf::from);
        if let Some(dir) = from_process.or(from_env) {
            write_marker(&dir, &marker);
        }
        let line = serde_json::to_string(&marker).unwrap_or_else(|_| "{}".to_owned());
        eprintln!("eliot_crash {line}");
    }));
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn bound_text(text: &str, limit: usize) -> String {
    let mut end = text.len().min(limit);
    while !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    text[..end].to_owned()
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
        thread: bound_text(&policy.redact_text(thread), CrashMarker::MAX_THREAD_CHARS),
        location: bound_text(
            &policy.redact_text(&location),
            CrashMarker::MAX_LOCATION_CHARS,
        ),
        message: "panic payload omitted (unclassified)".to_owned(),
        backtrace: bound_text(
            &policy.redact_text(&backtrace),
            CrashMarker::MAX_BACKTRACE_CHARS,
        ),
        timestamp: now_ms(),
    }
}

/// Best-effort marker write. Missing directories are created; beyond a
/// small file cap new markers are dropped rather than growing the
/// directory. Never fails the panicking process further.
pub(crate) fn write_marker(dir: &Path, marker: &CrashMarker) {
    #[cfg(windows)]
    let private = crate::platform::windows::ensure_private_root(dir);
    #[cfg(unix)]
    let private = crate::platform::unix::ensure_private_root(dir);
    #[cfg(not(any(windows, unix)))]
    let private: std::io::Result<()> = Err(std::io::ErrorKind::Unsupported.into());
    if private.is_err() {
        return;
    }
    if std::fs::read_dir(dir)
        .map(|entries| entries.count() > 64)
        .unwrap_or(true)
    {
        return;
    }
    let name = format!(
        "{}-{}-{}.json",
        marker.code,
        std::process::id(),
        marker.timestamp
    );
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-' || byte == b'_')
    {
        return;
    }
    if let Ok(bytes) = serde_json::to_vec(marker)
        && bytes.len() <= 64 * 1024
    {
        // Reuse the existing private create/write/sync/atomic-rename owner.
        let _ = crate::platform::data_dir::replace_private_file(dir, &name, &bytes);
    }
}

/// Read and consume (delete) every marker of one code, oldest first.
/// Temporary or incomplete files are ignored, never deleted by a reader.
pub(crate) fn take_markers(dir: &Path, code: &str) -> Vec<CrashMarker> {
    let mut markers = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return markers;
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| {
                    name.starts_with(&format!("{code}-")) && name.ends_with(".json")
                })
        })
        .collect();
    paths.sort();
    for path in paths {
        let parsed = std::fs::metadata(&path)
            .ok()
            .filter(|metadata| metadata.len() <= 64 * 1024 && metadata.is_file())
            .and_then(|_| std::fs::read(&path).ok())
            .and_then(|bytes| serde_json::from_slice::<CrashMarker>(&bytes).ok())
            .filter(|marker| marker.code == code && marker.validate().is_ok());
        if let Some(marker) = parsed {
            let _ = std::fs::remove_file(&path);
            markers.push(marker);
        }
    }
    markers.sort_by_key(|marker| marker.timestamp);
    markers
}

/// Previous-run crash for `daemon.status` and startup output: the latest
/// consumed `daemon.panicked` marker, redacted with the daemon secrets.
/// Consumption is once-only: files are deleted on read.
pub(crate) fn load_previous_crash(data_root: &Path, policy: &SecretPolicy) -> Option<CrashMarker> {
    let mut markers = take_markers(&crash_dir(data_root), "daemon.panicked");
    markers.pop().map(|mut marker| {
        marker.message = policy.redact_text(&marker.message);
        marker.backtrace = policy.redact_text(&marker.backtrace);
        marker.thread = bound_text(&marker.thread, CrashMarker::MAX_THREAD_CHARS);
        marker.location = bound_text(&marker.location, CrashMarker::MAX_LOCATION_CHARS);
        marker.message = bound_text(&marker.message, CrashMarker::MAX_MESSAGE_CHARS);
        marker.backtrace = bound_text(&marker.backtrace, CrashMarker::MAX_BACKTRACE_CHARS);
        marker
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn panic_probe() {
        if let Ok(dir) = std::env::var("K68_PANIC_PROBE_DIR") {
            set_crash_dir(Path::new(&dir));
            install_panic_hook(false);
            let _ = std::panic::catch_unwind(|| panic!("private-token-k68-panic"));
        }
    }

    #[test]
    fn panic_payload_never_reaches_either_sink() {
        let dir = std::env::temp_dir().join(format!("k68-panic-{}", ulid::Ulid::new()));
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash::tests::panic_probe", "--nocapture"])
            .env("K68_PANIC_PROBE_DIR", &dir)
            .output()
            .unwrap();
        let stderr = String::from_utf8_lossy(&output.stderr);
        let markers = take_markers(&dir, "daemon.panicked");
        assert_eq!(markers.len(), 1);
        assert!(
            !serde_json::to_string(&markers[0])
                .unwrap()
                .contains("private-token-k68-panic")
        );
        assert!(!stderr.contains("private-token-k68-panic"));
        assert!(markers[0].location.contains("crash.rs:"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn incomplete_publication_is_ignored_without_deletion() {
        let dir = std::env::temp_dir().join(format!("k68-partial-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let temp = dir.join("daemon.panicked-1-1.tmp");
        std::fs::write(&temp, b"{\"code\":").unwrap();
        assert!(take_markers(&dir, "daemon.panicked").is_empty());
        assert!(temp.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn markers_round_trip_and_consume_once() {
        let dir = std::env::temp_dir().join(format!("k68-crash-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let marker = CrashMarker {
            code: "daemon.panicked".to_owned(),
            thread: "main".to_owned(),
            location: "src/main.rs:1".to_owned(),
            message: "explicit panic".to_owned(),
            backtrace: "frames".to_owned(),
            timestamp: 1_700_000_000_000,
        };
        write_marker(&dir, &marker);
        let taken = take_markers(&dir, "daemon.panicked");
        assert_eq!(taken, vec![marker]);
        assert!(take_markers(&dir, "daemon.panicked").is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn unix_crash_marker_is_current_user_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("eliot-crash-mode-{}", ulid::Ulid::new()));
        let marker = CrashMarker {
            code: "daemon.panicked".to_owned(),
            thread: "main".to_owned(),
            location: "src/main.rs:1".to_owned(),
            message: "explicit panic".to_owned(),
            backtrace: "frames".to_owned(),
            timestamp: 1_700_000_000_000,
        };
        write_marker(&dir, &marker);
        let entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .collect();
        assert_eq!(entries.len(), 1, "one crash marker was written");
        let mode = entries[0].metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "crash marker is current-user-only");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_redacts_and_keeps_latest() {
        let root = std::env::temp_dir().join(format!("k68-crash2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let dir = crash_dir(&root);
        let policy = SecretPolicy::new(["s3cr3t"]);
        for (timestamp, message) in [(1, "old s3cr3t"), (2, "new s3cr3t")] {
            write_marker(
                &dir,
                &CrashMarker {
                    code: "daemon.panicked".to_owned(),
                    thread: "main".to_owned(),
                    location: "src/main.rs:1".to_owned(),
                    message: message.to_owned(),
                    backtrace: String::new(),
                    timestamp,
                },
            );
        }
        let loaded = load_previous_crash(&root, &policy).expect("latest marker loads");
        assert_eq!(loaded.message, "new [REDACTED]");
        assert_eq!(loaded.timestamp, 2);
        assert!(load_previous_crash(&root, &policy).is_none());
        let _ = std::fs::remove_dir_all(&root);
    }
}
