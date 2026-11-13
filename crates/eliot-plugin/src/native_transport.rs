//! Protocol-independent native child transport for harness plugins.
//!
//! Process spawn and custody, the stdin writer queue, the bounded framed-line
//! pump, output drain and termination are identical for every stdio harness;
//! only line framing and request/response correlation differ. The JSON-RPC
//! layer ([`crate::native_stdio`]) and the stream-json layer (Claude) build
//! on this module so those pieces exist exactly once.

use std::{
    collections::HashMap,
    ffi::OsString,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::{FutureExt, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    process::{ChildStdin, ChildStdout, Command},
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};
use tokio_util::{
    bytes::BytesMut,
    codec::{Decoder, FramedRead, FramedWrite, LinesCodec},
};

use eliot_core::{Error, Result, SecretPolicy, StderrSnapshot, StderrSource};

use crate::{StderrCapture, StderrView, spawn_stderr, sync::LockExt};

/// Default inbound native frame cap (1 MiB), kept by every adapter without
/// evidence of larger protocol frames.
pub const DEFAULT_MAX_NATIVE_FRAME_BYTES: usize = 1024 * 1024;
/// Largest per-adapter inbound frame cap the pump accepts (16 MiB).
pub const MAX_SUPPORTED_NATIVE_FRAME_BYTES: usize = 16 * 1024 * 1024;
/// Leading bytes of an oversized line kept for envelope diagnosis. The
/// JSON-RPC `method`/`id` head always fits; bodies never do.
const OVERSIZED_PREFIX_CAP: usize = 2048;
/// Longest recorded JSON-RPC method name. Protocol tokens are short; longer
/// or non-token input is treated as not parseable, never stored.
const OVERSIZED_METHOD_CAP: usize = 64;
const NATIVE_EVENT_QUEUE: usize = 64;
const NATIVE_EXIT_DRAIN: Duration = Duration::from_secs(1);

/// Kernel process birth identity. `start_time` is measured in the same OS
/// clock domain as [`process_birth_boundary`]. On Windows, `process_start_key`
/// preserves unique process identity independently of that ordering clock.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessBirthIdentity {
    pub pid: u32,
    pub start_time: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub process_start_key: Option<u64>,
}

/// Read this process's immutable kernel birth identity.
pub fn current_process_birth_identity() -> Result<ProcessBirthIdentity> {
    process_birth_identity(std::process::id())
}

/// Read a live process's kernel birth identity for the given PID.
pub fn process_birth_identity(pid: u32) -> Result<ProcessBirthIdentity> {
    if pid == 0 {
        return Err(Error::new(
            "process_identity",
            "process id must be positive",
        ));
    }
    let (start_time, process_start_key) = process_birth::birth(pid)?;
    Ok(ProcessBirthIdentity {
        pid,
        start_time,
        process_start_key,
    })
}

/// Capture a prompt opening boundary in the same OS clock domain as process
/// birth identities, before dispatching the native prompt request.
pub fn process_birth_boundary() -> Result<u64> {
    process_birth::boundary()
}

#[cfg(windows)]
#[allow(unsafe_code)]
mod process_birth {
    use std::{ffi::c_void, mem::size_of, ptr};

    use eliot_core::{Error, Result};
    use windows_sys::Win32::{
        Foundation::{
            CloseHandle, FARPROC, HANDLE, HMODULE, STATUS_BUFFER_OVERFLOW,
            STATUS_INFO_LENGTH_MISMATCH,
        },
        System::Threading::{OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION},
    };

    const PROCESS_TELEMETRY_ID_INFORMATION_CLASS: i32 = 64;
    const MAX_PROCESS_TELEMETRY_BUFFER_BYTES: usize = 1024 * 1024;
    const MAX_PROCESS_TELEMETRY_QUERY_ATTEMPTS: usize = 3;

    // Microsoft documents this NT telemetry record, but ships no public SDK
    // header for it. Keep the local C-layout declaration aligned with that
    // documented field order; use only the process key and birth-time prefix.
    #[repr(C)]
    #[derive(Default)]
    #[allow(dead_code)]
    struct ProcessTelemetryIdInformation {
        header_size: u32,
        process_id: u32,
        process_start_key: u64,
        create_time: u64,
        create_interrupt_time: u64,
        create_unbiased_interrupt_time: u64,
        process_sequence_number: u64,
        session_create_time: u64,
        session_id: u32,
        boot_id: u32,
        image_checksum: u32,
        image_time_date_stamp: u32,
        user_sid_offset: u32,
        image_path_offset: u32,
        package_name_offset: u32,
        relative_app_name_offset: u32,
        command_line_offset: u32,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    #[allow(dead_code)]
    struct ProcessTelemetryBirthPrefix {
        header_size: u32,
        process_id: u32,
        process_start_key: u64,
        create_time: u64,
        create_interrupt_time: u64,
        create_unbiased_interrupt_time: u64,
    }

    type FarProc = unsafe extern "system" fn() -> isize;
    type NtQueryInformationProcess =
        unsafe extern "system" fn(HANDLE, i32, *mut c_void, u32, *mut u32) -> i32;
    type QueryUnbiasedInterruptTime = unsafe extern "system" fn(*mut u64) -> i32;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "LoadLibraryW"]
        fn load_library_w(name: *const u16) -> HMODULE;
        #[link_name = "FreeLibrary"]
        fn free_library(module: HMODULE) -> i32;
        #[link_name = "GetProcAddress"]
        fn get_proc_address(module: HMODULE, name: *const u8) -> FARPROC;
    }

    struct OwnedModule(HMODULE);

    impl Drop for OwnedModule {
        fn drop(&mut self) {
            // SAFETY: LoadLibraryW returned the reference owned by this value.
            unsafe { free_library(self.0) };
        }
    }

    struct OwnedProcess(HANDLE);

    impl Drop for OwnedProcess {
        fn drop(&mut self) {
            // SAFETY: this handle is uniquely owned by `OwnedProcess`.
            unsafe { CloseHandle(self.0) };
        }
    }

    fn unsupported(message: impl Into<String>) -> Error {
        Error::new("unsupported", message)
    }

    fn load_module(name: &str) -> Result<OwnedModule> {
        let mut wide_name: Vec<u16> = name.encode_utf16().collect();
        wide_name.push(0);
        // SAFETY: `wide_name` is NUL-terminated and remains alive for the call.
        let module = unsafe { load_library_w(wide_name.as_ptr()) };
        if module.is_null() {
            return Err(unsupported(format!("Windows module {name} is unavailable")));
        }
        Ok(OwnedModule(module))
    }

    fn resolve_proc(module: HMODULE, name: &'static [u8], label: &str) -> Result<FarProc> {
        // SAFETY: `name` is a NUL-terminated export name.
        unsafe { get_proc_address(module, name.as_ptr()) }
            .ok_or_else(|| unsupported(format!("Windows {label} export is unavailable")))
    }

    fn query_process_telemetry(handle: HANDLE, pid: u32) -> Result<(u64, u64)> {
        let ntdll = load_module("ntdll.dll")?;
        let raw = resolve_proc(
            ntdll.0,
            b"NtQueryInformationProcess\0",
            "NtQueryInformationProcess",
        )?;
        // SAFETY: winternl.h and windows-sys declare this exact system ABI; the
        // function is resolved dynamically as Microsoft requires for ntdll.
        let query: NtQueryInformationProcess = unsafe { std::mem::transmute(raw) };

        let record_size = size_of::<ProcessTelemetryIdInformation>();
        let mut output = vec![0u64; record_size.div_ceil(size_of::<u64>())];

        for attempt in 0..MAX_PROCESS_TELEMETRY_QUERY_ATTEMPTS {
            output.fill(0);
            // ProcessInformation is output-only; leave HeaderSize zeroed for
            // Windows to report the fixed header actually returned.
            let buffer_size = output.len() * size_of::<u64>();
            let mut returned = 0u32;
            // SAFETY: the buffer is writable/aligned, its length is passed, and
            // `returned` is a valid output pointer.
            let status = unsafe {
                query(
                    handle,
                    PROCESS_TELEMETRY_ID_INFORMATION_CLASS,
                    output.as_mut_ptr().cast(),
                    u32::try_from(buffer_size).expect("bounded telemetry buffer fits u32"),
                    &mut returned,
                )
            };

            if status >= 0 {
                let returned = usize::try_from(returned).unwrap_or(0);
                if returned < size_of::<ProcessTelemetryBirthPrefix>() || returned > buffer_size {
                    return Err(unsupported(
                        "Windows process telemetry returned an invalid length",
                    ));
                }

                // SAFETY: success and ReturnLength prove the prefix is initialized
                // in this aligned u64-backed buffer.
                let birth =
                    unsafe { ptr::read(output.as_ptr().cast::<ProcessTelemetryBirthPrefix>()) };
                let returned_header_size = usize::try_from(birth.header_size).unwrap_or(0);
                let minimum_header_size = size_of::<ProcessTelemetryBirthPrefix>();
                if returned_header_size < minimum_header_size || returned_header_size > returned {
                    return Err(unsupported(
                        "Windows process telemetry returned an invalid HeaderSize",
                    ));
                }
                if birth.process_id != pid {
                    return Err(unsupported(
                        "Windows process telemetry returned a different process identity",
                    ));
                }
                return Ok((
                    birth.create_unbiased_interrupt_time,
                    birth.process_start_key,
                ));
            }

            if status != STATUS_BUFFER_OVERFLOW && status != STATUS_INFO_LENGTH_MISMATCH {
                return Err(unsupported(format!(
                    "Windows process birth ordering is unavailable (NtQueryInformationProcess status 0x{:08X})",
                    status as u32
                )));
            }

            let required = usize::try_from(returned)
                .map_err(|_| unsupported("Windows telemetry record size is invalid"))?;
            if required <= buffer_size || required > MAX_PROCESS_TELEMETRY_BUFFER_BYTES {
                return Err(unsupported(
                    "Windows process birth ordering has an invalid required telemetry size",
                ));
            }
            if attempt + 1 == MAX_PROCESS_TELEMETRY_QUERY_ATTEMPTS {
                break;
            }
            output.resize(required.div_ceil(size_of::<u64>()), 0);
        }

        Err(unsupported(
            "Windows process birth telemetry did not fit after bounded ReturnLength retries",
        ))
    }

    pub fn birth(pid: u32) -> Result<(u64, Option<u64>)> {
        // SAFETY: no pointers are passed; only query rights are requested.
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            return Err(Error::new(
                "process_identity",
                "could not open process to verify its birth identity",
            ));
        }
        let handle = OwnedProcess(handle);
        let (start_time, process_start_key) = query_process_telemetry(handle.0, pid)?;
        Ok((start_time, Some(process_start_key)))
    }

    pub fn boundary() -> Result<u64> {
        let kernel32 = load_module("kernel32.dll")?;
        let raw = resolve_proc(
            kernel32.0,
            b"QueryUnbiasedInterruptTime\0",
            "QueryUnbiasedInterruptTime",
        )?;
        // SAFETY: the function's documented ABI takes one writable u64 and
        // returns BOOL; the address came from kernel32's named export.
        let query: QueryUnbiasedInterruptTime = unsafe { std::mem::transmute(raw) };
        let mut time = 0u64;
        // SAFETY: `time` is a valid writable output value.
        if unsafe { query(&mut time) } == 0 {
            return Err(unsupported(
                "Windows unbiased interrupt-time boundary is unavailable",
            ));
        }
        Ok(time)
    }
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
mod process_birth {
    use std::{fs, io};

    use eliot_core::{Error, Result};

    pub fn birth(pid: u32) -> Result<(u64, Option<u64>)> {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat"))
            .map_err(|_| Error::new("process_identity", "could not read process birth identity"))?;
        let fields = stat
            .rfind(')')
            .and_then(|end| stat.get(end + 1..))
            .ok_or_else(|| Error::new("process_identity", "process stat is malformed"))?;
        let start_time = fields
            .split_whitespace()
            .nth(19)
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| Error::new("process_identity", "process start time is unavailable"))?;
        Ok((start_time, None))
    }

    pub fn boundary() -> Result<u64> {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime initializes the writable timespec. CLOCK_BOOTTIME
        // uses the same boot-relative clock as Linux /proc starttime.
        if unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut time) } != 0 {
            return Err(Error::new(
                "process_identity",
                io::Error::last_os_error().to_string(),
            ));
        }
        // SAFETY: sysconf takes no pointers and reports the kernel USER_HZ.
        let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if ticks_per_second <= 0 {
            return Err(Error::new(
                "process_identity",
                "kernel process clock rate is unavailable",
            ));
        }
        let seconds = u64::try_from(time.tv_sec)
            .map_err(|_| Error::new("process_identity", "boot clock is invalid"))?;
        let nanos = u64::try_from(time.tv_nsec)
            .map_err(|_| Error::new("process_identity", "boot clock is invalid"))?;
        Ok(seconds
            .saturating_mul(ticks_per_second as u64)
            .saturating_add(nanos.saturating_mul(ticks_per_second as u64) / 1_000_000_000))
    }
}

#[cfg(not(any(windows, target_os = "linux")))]
mod process_birth {
    use eliot_core::{Error, Result};

    pub fn birth(_pid: u32) -> Result<(u64, Option<u64>)> {
        Err(Error::new(
            "unsupported",
            "kernel process birth identity is unavailable on this platform",
        ))
    }

    pub fn boundary() -> Result<u64> {
        Err(Error::new(
            "unsupported",
            "kernel process birth boundary is unavailable on this platform",
        ))
    }
}

/// Display nouns for transport diagnostics, e.g. `("Muse", "Muse session
/// host")`. The protocol layers pass their exact historical labels so
/// shared errors read exactly as before.
#[derive(Clone, Copy)]
pub struct NativeLabels {
    pub short: &'static str,
    pub server: &'static str,
}

pub struct NativeLaunch {
    pub program: OsString,
    pub args: Vec<OsString>,
    pub cwd: PathBuf,
    pub environment: Vec<(OsString, OsString)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamState {
    Open,
    Overflow,
    Closed,
    Malformed(NativeStreamFault),
}

/// Bounded, nonsecret cause of a StreamState::Malformed transition.
/// Fixed classes only: no frame bytes, bodies, or credentials cross here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeStreamFault {
    /// The bounded line reader rejected a line over its configured limit.
    /// Carries the measured length, the limit, and whatever the bounded
    /// prefix proved about the JSON-RPC envelope.
    FrameOversized(OversizedFrame),
    /// The line reader returned `InvalidData`, including its UTF-8 decoder path.
    FrameInvalidData,
    /// The line reader returned an I/O error other than `InvalidData`.
    FrameIo,
    /// A framed line was not valid JSON.
    Json,
    /// A parsed record did not match its protocol envelope.
    Envelope,
    /// Stdout stayed open past the post-exit drain deadline.
    Drain,
    /// A malformed record without a finer proven kind.
    Unclassified,
}

impl NativeStreamFault {
    pub fn label(self) -> &'static str {
        match self {
            Self::FrameOversized(_) => "frame_oversized",
            Self::FrameInvalidData => "frame_invalid_data",
            Self::FrameIo => "frame_io",
            Self::Json => "invalid_json",
            Self::Envelope => "invalid_envelope",
            Self::Drain => "exit_drain",
            Self::Unclassified => "unclassified",
        }
    }

    /// Bounded, secret-free gap detail. Identical to [`label`](Self::label)
    /// except an oversized frame appends its measured length, the limit, and
    /// whatever the bounded prefix proved (envelope kind, token-filtered
    /// method). Never carries frame bytes, bodies, or credentials.
    pub fn describe(self) -> String {
        match self {
            Self::FrameOversized(detail) => detail.describe(),
            _ => self.label().to_owned(),
        }
    }
}

/// What the bounded oversized-line prefix proved about the frame's JSON-RPC
/// envelope. Presence-only: id values are never recorded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OversizedFrameKind {
    /// No top-level `method` or `id` key parsed from the prefix.
    Unknown,
    /// Top-level `method` without `id`: a server-to-client notification.
    Notification,
    /// Top-level `method` and `id`: a server-to-client request.
    Request,
    /// Top-level `id` without `method`: a response to one of our own calls.
    Response,
}

impl OversizedFrameKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Notification => "notification",
            Self::Request => "request",
            Self::Response => "response",
        }
    }
}

/// Bounded, secret-free description of one over-limit native frame.
/// Fixed shape only: byte counts, the envelope kind the bounded prefix
/// proved, and a token-filtered method name when one parsed. The method
/// filter admits only protocol-token characters, so free-text bodies can
/// never be recorded here even when the frame is malformed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OversizedFrame {
    limit_bytes: u32,
    length_bytes: u64,
    kind: OversizedFrameKind,
    method_len: u8,
    method_bytes: [u8; OVERSIZED_METHOD_CAP],
}

impl OversizedFrame {
    /// Diagnose from a bounded line prefix (the reader always captures one).
    pub fn parse(limit_bytes: usize, length_bytes: u64, prefix: &[u8]) -> Self {
        let (kind, method) = parse_oversized_prefix(prefix);
        Self::new(limit_bytes, length_bytes, kind, method.as_deref())
    }

    /// Fallback when the line itself is in hand but no prefix was captured.
    /// Defensive only: the bounded reader always captures a prefix.
    pub fn unparsed(limit_bytes: usize, length_bytes: u64) -> Self {
        Self::new(limit_bytes, length_bytes, OversizedFrameKind::Unknown, None)
    }

    fn new(
        limit_bytes: usize,
        length_bytes: u64,
        kind: OversizedFrameKind,
        method: Option<&str>,
    ) -> Self {
        let mut method_bytes = [0u8; OVERSIZED_METHOD_CAP];
        let mut method_len = 0u8;
        if let Some(name) = method
            && !name.is_empty()
            && name.len() <= OVERSIZED_METHOD_CAP
            && name.bytes().all(is_oversized_method_char)
        {
            method_bytes[..name.len()].copy_from_slice(name.as_bytes());
            method_len = name.len() as u8;
        }
        Self {
            limit_bytes: u32::try_from(limit_bytes).unwrap_or(u32::MAX),
            length_bytes,
            kind,
            method_len,
            method_bytes,
        }
    }

    pub fn limit_bytes(self) -> u32 {
        self.limit_bytes
    }

    pub fn length_bytes(self) -> u64 {
        self.length_bytes
    }

    pub fn kind(self) -> OversizedFrameKind {
        self.kind
    }

    /// The parsed JSON-RPC method, if one parsed and passed the token
    /// filter. `None` means absent or not parseable, never a redaction.
    pub fn method(&self) -> Option<&str> {
        if self.method_len == 0 {
            return None;
        }
        // Token-filtered ASCII by construction, so this cannot fail.
        std::str::from_utf8(&self.method_bytes[..usize::from(self.method_len)]).ok()
    }

    /// Bounded one-line diagnosis, e.g.
    /// `frame_oversized len=1834521 limit=8388608 kind=response` or
    /// `frame_oversized len=2100000 limit=8388608 kind=notification method=item/completed`.
    pub fn describe(self) -> String {
        let mut text = format!(
            "frame_oversized len={} limit={} kind={}",
            self.length_bytes,
            self.limit_bytes,
            self.kind.as_str(),
        );
        if self.method_len != 0
            && let Ok(method) =
                std::str::from_utf8(&self.method_bytes[..usize::from(self.method_len)])
        {
            text.push_str(" method=");
            text.push_str(method);
        }
        text
    }
}

fn is_oversized_method_char(byte: u8) -> bool {
    matches!(
        byte,
        b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'/' | b'.' | b'_' | b'-'
    )
}

/// Parse a bounded line prefix for the JSON-RPC envelope head. Tracks brace
/// depth and string state so nested values or string bodies cannot fake
/// top-level `method`/`id` keys. Returns the envelope kind and the raw
/// method text (charsets are enforced by [`OversizedFrame`], not here).
fn parse_oversized_prefix(prefix: &[u8]) -> (OversizedFrameKind, Option<String>) {
    let mut rest = prefix;
    while rest.first().is_some_and(|byte| byte.is_ascii_whitespace()) {
        rest = &rest[1..];
    }
    if rest.first() != Some(&b'{') {
        return (OversizedFrameKind::Unknown, None);
    }
    let mut depth = 0u32;
    let mut in_string = false;
    let mut escaped = false;
    let mut has_id = false;
    let mut method: Option<String> = None;
    let mut index = 0;
    while index < prefix.len() {
        let byte = prefix[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        match byte {
            b'"' if depth == 1 => {
                let Some((key, after_key)) = read_oversized_json_string(prefix, index) else {
                    // Truncated or escaped: resume inside the string.
                    in_string = true;
                    index += 1;
                    continue;
                };
                let mut next = after_key;
                while next < prefix.len() && prefix[next].is_ascii_whitespace() {
                    next += 1;
                }
                if prefix.get(next) != Some(&b':') {
                    index = after_key;
                    continue;
                }
                if key == "id" {
                    has_id = true;
                } else if key == "method" && method.is_none() {
                    let mut value_at = next + 1;
                    while value_at < prefix.len() && prefix[value_at].is_ascii_whitespace() {
                        value_at += 1;
                    }
                    if prefix.get(value_at) == Some(&b'"')
                        && let Some((value, _)) = read_oversized_json_string(prefix, value_at)
                        && !value.is_empty()
                        && value.len() <= OVERSIZED_METHOD_CAP
                    {
                        method = Some(value);
                    }
                }
                index = after_key;
            }
            b'"' => {
                in_string = true;
                index += 1;
            }
            b'{' | b'[' => {
                depth = depth.saturating_add(1);
                index += 1;
            }
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
                index += 1;
            }
            _ => {
                index += 1;
            }
        }
    }
    let kind = match (method.is_some(), has_id) {
        (true, true) => OversizedFrameKind::Request,
        (true, false) => OversizedFrameKind::Notification,
        (false, true) => OversizedFrameKind::Response,
        (false, false) => OversizedFrameKind::Unknown,
    };
    (kind, method)
}

/// Read one JSON string starting at the opening quote. Rejects escapes,
/// control bytes, non-UTF-8, and truncation: envelope keys and method
/// tokens never need them, so anything else is simply not parseable.
fn read_oversized_json_string(bytes: &[u8], start: usize) -> Option<(String, usize)> {
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut end = start + 1;
    while end < bytes.len() {
        match bytes[end] {
            b'"' => {
                let raw = bytes.get(start + 1..end)?;
                if raw.contains(&b'\\') {
                    return None;
                }
                let text = std::str::from_utf8(raw).ok()?.to_owned();
                return Some((text, end + 1));
            }
            0x00..=0x1F => return None,
            _ => end += 1,
        }
    }
    None
}

/// One owned native child: process waiter, stdin writer queue, stdout-reader
/// task ownership, stderr capture. The protocol layer spawns its own reader
/// task over the returned stdout (framing is its business) and hands the
/// task back for drain ownership.
pub struct NativeChild {
    pid: Option<u32>,
    kill: Option<oneshot::Sender<()>>,
    process_exit: watch::Receiver<Option<ProcessExit>>,
    process_task: Option<tokio::task::JoinHandle<()>>,
    writer: NativeWriter,
    output_reader: Option<tokio::task::JoinHandle<Result<()>>>,
    output_reader_result: Option<Result<()>>,
    /// Present until [`NativeChild::await_output_drained`] finishes stderr.
    pub stderr: Option<StderrCapture>,
    stderr_view: StderrView,
    policy: SecretPolicy,
    labels: NativeLabels,
}

#[derive(Clone)]
struct ProcessExit {
    status: ExitStatus,
    observed_at: Instant,
}

/// One stdin writer queue. Cloned into the protocol client; every write and
/// close serializes on the queue with the caller's absolute deadline. The
/// queue empties on input shutdown or child exit.
#[derive(Clone)]
pub struct NativeWriter {
    writer: Arc<tokio::sync::Mutex<Option<FramedWrite<ChildStdin, LinesCodec>>>>,
    process_exited: Arc<AtomicBool>,
    reader_closed: Arc<AtomicBool>,
    labels: NativeLabels,
}

/// Correlated request/response calls: id minting plus the pending map. The
/// protocol layer decides the wire envelope and the response mapping.
#[derive(Clone)]
pub struct PendingCalls {
    calls: Arc<StdMutex<HashMap<String, oneshot::Sender<Result<Value>>>>>,
    next_id: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
    id_prefix: &'static str,
    labels: NativeLabels,
}

pub struct PendingGuard {
    pending: PendingCalls,
    id: String,
}

/// What one decoded stream record means to the pump.
pub enum PumpAction<Frame> {
    /// A protocol frame for the event task (subject to the queue bound).
    Emit(Frame),
    /// A correlated reply for one pending call.
    Route { key: String, result: Result<Value> },
    /// The stream is unusable from here on; the pump gaps and fails pending.
    Malformed(String),
    /// A record the protocol ignores (e.g. an echo of our own input).
    Skip,
}

/// How the pump delivers emitted frames when the bounded queue is full.
/// JSON-RPC callers select [`PumpMode::LossyOverflow`] so reply routing stays
/// nonblocking; only the stream-json caller with no RPC replies selects
/// [`PumpMode::LosslessBackpressure`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PumpMode {
    /// `try_send`; on a full queue latch overflow and drop later emits.
    LossyOverflow,
    /// Wait for queue space holding at most one frame, respecting receiver
    /// closure and the existing exit-drain deadline.
    LosslessBackpressure,
}

/// Read one retained process exit. The waiter publishes `Some(status)`
/// then drops its sender; Tokio reports closure even when that final
/// value is retained, so a closed channel is reread and only a still-
/// `None` value concludes unknown.
fn read_process_exit(
    process_exit: &watch::Receiver<Option<ProcessExit>>,
    short: &'static str,
) -> Result<Option<ExitStatus>> {
    let status = process_exit.borrow().as_ref().map(|exit| exit.status);
    if status.is_some() {
        return Ok(status);
    }
    if process_exit.has_changed().is_err() {
        let status = process_exit.borrow().as_ref().map(|exit| exit.status);
        if status.is_some() {
            return Ok(status);
        }
        return Err(Error::new(
            "unknown",
            format!("{short} process exit could not be confirmed"),
        ));
    }
    Ok(None)
}

impl NativeChild {
    pub async fn spawn(
        launch: NativeLaunch,
        policy: SecretPolicy,
        labels: NativeLabels,
    ) -> Result<(Self, ChildStdout)> {
        let mut command = Command::new(&launch.program);
        command
            .args(&launch.args)
            .current_dir(&launch.cwd)
            .envs(launch.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(|error| {
            Error::new(
                "native_spawn",
                policy.redact_text(&format!(
                    "could not start the configured {}: {error}",
                    labels.server
                )),
            )
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            Error::new(
                "native_spawn",
                format!("{} stdin was not piped", labels.server),
            )
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            Error::new(
                "native_spawn",
                format!("{} stdout was not piped", labels.server),
            )
        })?;
        let stderr = child.stderr.take().ok_or_else(|| {
            Error::new(
                "native_spawn",
                format!("{} stderr was not piped", labels.server),
            )
        })?;
        let stderr = spawn_stderr(stderr, Arc::new(policy.clone()));
        let pid = child.id();
        let (exit_tx, process_exit) = watch::channel(None);
        let (kill, mut kill_rx) = oneshot::channel();
        let writer = NativeWriter::new(stdin, labels);
        let exit_writer = writer.clone();
        let process_task = tokio::spawn(async move {
            let result = tokio::select! {
                status = child.wait() => status,
                _ = &mut kill_rx => {
                    match child.start_kill() {
                        Ok(()) => child.wait().await,
                        Err(kill_error) => match child.try_wait() {
                            Ok(Some(status)) => Ok(status),
                            Ok(None) => Err(kill_error),
                            Err(wait_error) => Err(wait_error),
                        },
                    }
                }
            };
            exit_writer.mark_process_exited();
            if let Ok(status) = result {
                exit_tx.send_replace(Some(ProcessExit {
                    status,
                    observed_at: Instant::now(),
                }));
            }
        });
        let child = Self {
            pid,
            kill: Some(kill),
            process_exit,
            process_task: Some(process_task),
            writer,
            output_reader: None,
            output_reader_result: None,
            stderr_view: stderr.view(),
            stderr: Some(stderr),
            policy,
            labels,
        };
        Ok((child, stdout))
    }

    pub fn set_output_reader(&mut self, reader: tokio::task::JoinHandle<Result<()>>) {
        self.output_reader = Some(reader);
    }

    pub fn writer(&self) -> NativeWriter {
        self.writer.clone()
    }

    /// The actual native child PID; birth and custody are verified by the host.
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    fn process_exit_signal(&self) -> watch::Receiver<Option<ProcessExit>> {
        self.process_exit.clone()
    }

    /// True only after the owned child waiter observed its OS exit status.
    pub fn process_exit_observed(&self) -> bool {
        self.process_exit.borrow().is_some()
    }

    /// Wait for the owned child waiter to observe its OS exit without
    /// terminating the child or imposing a deadline.
    pub async fn wait_for_process_exit(&mut self) -> Result<ExitStatus> {
        self.wait_process_exit().await
    }

    /// True once the stdout reader task has ended, whether it ended cleanly or
    /// with a typed stream fault. Callers can require this before owner cleanup.
    pub fn output_reader_finished(&self) -> bool {
        self.output_reader_result.is_some()
            || self
                .output_reader
                .as_ref()
                .is_some_and(|reader| reader.is_finished())
    }

    /// Positively observe this child exit, then wait for its owned stdout
    /// reader to finish. Each wait is bounded by the existing exit-drain
    /// window; this never terminates the child or infers exit from silence.
    pub async fn confirm_exit_and_output_drained(&mut self) -> bool {
        match timeout_at(Instant::now() + NATIVE_EXIT_DRAIN, self.wait_process_exit()).await {
            Ok(Ok(_status)) => {}
            Ok(Err(_)) | Err(_) => return false,
        }

        let _ = self
            .await_output_drained(Instant::now() + NATIVE_EXIT_DRAIN)
            .await;
        self.process_exit_observed() && self.writer.reader_closed() && self.output_reader_finished()
    }

    pub fn stderr_snapshot(&self) -> StderrSnapshot {
        self.stderr_view.snapshot()
    }

    pub fn stderr_view(&self) -> StderrView {
        self.stderr_view.clone()
    }

    pub fn diagnostic_error(&self, mut error: Error) -> Error {
        error.message = self.policy.redact_text(&error.message);
        error.with_stderr(StderrSource::Harness, self.stderr_snapshot())
    }

    /// Wait for the single stdout reader to finish; a timeout leaves that same
    /// reader running so a later wait can observe its completion without reuse.
    pub async fn await_output_drained(&mut self, deadline: Instant) -> Result<()> {
        let result = self.await_stdout_drained(deadline).await;
        if let Some(stderr) = self.stderr.take() {
            let _ = stderr.finish(deadline).await;
        }
        result.map_err(|error| self.diagnostic_error(error))
    }

    /// Wait passively for the owned stdout reader task to finish, retaining
    /// its result so the bounded drain helper can confirm the full endpoint.
    pub async fn wait_for_output_reader_finished(&mut self) -> Result<()> {
        let result = self.await_stdout_drained_until(None).await;
        result.map_err(|error| self.diagnostic_error(error))
    }

    async fn await_stdout_drained(&mut self, deadline: Instant) -> Result<()> {
        self.await_stdout_drained_until(Some(deadline)).await
    }

    async fn await_stdout_drained_until(&mut self, deadline: Option<Instant>) -> Result<()> {
        if let Some(result) = &self.output_reader_result {
            return result.clone();
        }
        let reader_result = {
            let reader = self.output_reader.as_mut().ok_or_else(|| {
                Error::new(
                    "unknown",
                    format!("{} stdout reader is unavailable", self.labels.short),
                )
            })?;
            if reader.is_finished() {
                (&mut *reader).await
            } else if let Some(deadline) = deadline {
                match timeout_at(deadline, reader).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(Error::new(
                            "unknown",
                            format!(
                                "{} stdout reader did not drain before its deadline",
                                self.labels.short
                            ),
                        ));
                    }
                }
            } else {
                (&mut *reader).await
            }
        };
        let result = match reader_result {
            Ok(result) => result,
            Err(error) => Err(Error::new(
                "unknown",
                format!("{} stdout reader task failed: {error}", self.labels.short),
            )),
        };
        drop(self.output_reader.take());
        self.output_reader_result = Some(result.clone());
        result
    }

    fn process_exit_status(&self) -> Result<Option<ExitStatus>> {
        read_process_exit(&self.process_exit, self.labels.short)
    }

    async fn wait_process_exit(&mut self) -> Result<ExitStatus> {
        loop {
            if let Some(status) = self.process_exit_status()? {
                return Ok(status);
            }
            if self.process_exit.changed().await.is_err() {
                // Closed without a prior observed change. Reread the
                // retained final value before concluding unknown.
                if let Some(status) = self.process_exit.borrow().as_ref().map(|exit| exit.status) {
                    return Ok(status);
                }
                return Err(Error::new(
                    "unknown",
                    format!("{} process exit could not be confirmed", self.labels.short),
                ));
            }
        }
    }

    /// Flush input, then end the owned process. The graceful close only fires
    /// when the process is still running; the kill path only fires when the
    /// graceful close cannot be confirmed.
    pub async fn terminate(&mut self, force: bool, deadline: Instant) -> Result<ExitStatus> {
        self.terminate_inner(force, deadline, false).await
    }

    /// Drop stdin so the child really sees EOF, then end the owned process.
    /// Same sequence as [`NativeChild::terminate`], but the graceful close
    /// delivers EOF instead of only flushing.
    pub async fn terminate_dropping_input(
        &mut self,
        force: bool,
        deadline: Instant,
    ) -> Result<ExitStatus> {
        self.terminate_inner(force, deadline, true).await
    }

    async fn terminate_inner(
        &mut self,
        force: bool,
        deadline: Instant,
        drop_input: bool,
    ) -> Result<ExitStatus> {
        if let Some(status) = self.process_exit_status()? {
            self.await_output_drained(deadline).await?;
            return Ok(status);
        }

        if !force {
            let close_deadline = (Instant::now() + Duration::from_millis(500)).min(deadline);
            if drop_input {
                let _ = self.writer.shutdown_input(close_deadline).await;
            } else {
                let _ = self.writer.close_input(close_deadline).await;
            }
            let grace_deadline = (Instant::now() + Duration::from_secs(2)).min(deadline);
            if let Ok(Ok(status)) = timeout_at(grace_deadline, self.wait_process_exit()).await {
                self.await_output_drained(deadline).await?;
                return Ok(status);
            }
        }

        if let Some(status) = self.process_exit_status()? {
            self.await_output_drained(deadline).await?;
            return Ok(status);
        }

        if Instant::now() >= deadline {
            return Err(Error::new(
                "unknown",
                format!(
                    "{} process termination outcome is unknown",
                    self.labels.short
                ),
            ));
        }
        if let Some(kill) = self.kill.take() {
            let _ = kill.send(());
        }
        let status = timeout_at(deadline, self.wait_process_exit())
            .await
            .map_err(|_| {
                Error::new(
                    "unknown",
                    format!("{} process exit could not be confirmed", self.labels.short),
                )
            })??;
        self.await_output_drained(deadline).await?;
        Ok(status)
    }
}

impl Drop for NativeChild {
    fn drop(&mut self) {
        // Fence every cloned writer (and, via the shared flag, the pending
        // router) before aborting either task: fresh writes are refused
        // `native_not_dispatched`, already-written calls stay Unknown.
        self.writer.mark_reader_closed();
        if let Some(reader) = &self.output_reader {
            reader.abort();
        }
        if let Some(process) = self.process_task.take() {
            process.abort();
        }
        // StderrCapture owns and cancels its reader; client views cannot keep it alive.
    }
}

impl NativeWriter {
    pub fn new(stdin: ChildStdin, labels: NativeLabels) -> Self {
        Self {
            writer: Arc::new(tokio::sync::Mutex::new(Some(FramedWrite::new(
                stdin,
                LinesCodec::new(),
            )))),
            process_exited: Arc::new(AtomicBool::new(false)),
            reader_closed: Arc::new(AtomicBool::new(false)),
            labels,
        }
    }

    /// Mark the stdout reader fatally ended (EOF, codec/UTF-8/IO fault,
    /// oversize without a proven line boundary, or exit-drain deadline).
    /// Future writes are refused before dispatch; in-flight writes already
    /// queued keep their unknown outcome through the pending calls.
    pub fn mark_reader_closed(&self) {
        self.reader_closed.store(true, Ordering::Release);
    }

    pub fn reader_closed(&self) -> bool {
        self.reader_closed.load(Ordering::Acquire)
    }

    /// The shared reader-closed flag. `PendingCalls` is bound to the same
    /// flag so writer fencing and response-router closure are one boundary.
    pub(crate) fn reader_closed_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.reader_closed)
    }

    fn mark_process_exited(&self) {
        self.process_exited.store(true, Ordering::Release);
        if let Ok(mut writer) = self.writer.try_lock() {
            *writer = None;
        }
    }

    /// True once the owned child waiter finished (exit or wait error)
    /// and fenced input. Conservative input unavailability only; it does
    /// NOT prove an OS exit. Protocol layers use it to distinguish an
    /// unavailable destination (unknown) from a live pre-dispatch
    /// refusal (rejected).
    pub fn process_unavailable(&self) -> bool {
        self.process_exited.load(Ordering::Acquire)
    }

    /// One already-encoded frame. Raw errors: the protocol layer maps them
    /// through its own diagnostics. Only a queued write proves dispatch; a
    /// lost write is unknown, never silently dropped.
    pub async fn send(&self, encoded: String, deadline: Instant) -> Result<()> {
        if self.process_exited.load(Ordering::Acquire) {
            return Err(self.process_exited_error());
        }
        if self.reader_closed() {
            return Err(self.reader_closed_error());
        }
        let mut writer = match timeout_at(deadline, self.writer.lock()).await {
            Ok(writer) => writer,
            Err(_) => {
                return Err(Error::new(
                    "native_not_dispatched",
                    format!(
                        "{} write deadline elapsed before dispatch",
                        self.labels.short
                    ),
                ));
            }
        };
        if self.process_exited.load(Ordering::Acquire) {
            *writer = None;
            return Err(self.process_exited_error());
        }
        if self.reader_closed() {
            return Err(self.reader_closed_error());
        }
        let Some(writer) = writer.as_mut() else {
            return Err(Error::new(
                "native_not_dispatched",
                format!(
                    "{} stdin is shut down; write was not dispatched",
                    self.labels.short
                ),
            ));
        };
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                format!(
                    "{} write deadline elapsed before dispatch",
                    self.labels.short
                ),
            ));
        }
        match timeout_at(deadline, writer.send(encoded)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Error::new(
                "unknown",
                format!("{} write outcome is unknown", self.labels.server),
            )),
        }
    }

    fn process_exited_error(&self) -> Error {
        Error::new(
            "native_not_dispatched",
            format!("{} process exited before input dispatch", self.labels.short),
        )
    }

    fn reader_closed_error(&self) -> Error {
        Error::new(
            "native_not_dispatched",
            format!(
                "{} response reader closed before input dispatch",
                self.labels.short
            ),
        )
    }

    /// Flush input. This does not close the pipe: the stdin handle stays
    /// queued until [`NativeWriter::shutdown_input`] drops it.
    pub async fn close_input(&self, deadline: Instant) -> Result<()> {
        let mut writer = match timeout_at(deadline, self.writer.lock()).await {
            Ok(writer) => writer,
            Err(_) => {
                return Err(Error::new(
                    "native_not_dispatched",
                    format!(
                        "{} input close deadline elapsed before dispatch",
                        self.labels.short
                    ),
                ));
            }
        };
        let Some(writer) = writer.as_mut() else {
            return Ok(());
        };
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                format!(
                    "{} input close deadline elapsed before dispatch",
                    self.labels.short
                ),
            ));
        }
        match timeout_at(deadline, SinkExt::<String>::close(writer)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) | Err(_) => Err(Error::new(
                "unknown",
                format!("{} input close outcome is unknown", self.labels.server),
            )),
        }
    }

    /// Flush input and drop the stdin handle so the child really sees EOF.
    /// Idempotent: input stays shut. The flush result is irrelevant — after
    /// the drop the pipe is closed either way.
    pub async fn shutdown_input(&self, deadline: Instant) -> Result<()> {
        let mut guard = match timeout_at(deadline, self.writer.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                return Err(Error::new(
                    "native_not_dispatched",
                    format!(
                        "{} input shutdown deadline elapsed before dispatch",
                        self.labels.short
                    ),
                ));
            }
        };
        let Some(writer) = guard.as_mut() else {
            return Ok(());
        };
        let _ = timeout_at(deadline, SinkExt::<String>::close(writer)).await;
        *guard = None;
        Ok(())
    }
}

impl PendingCalls {
    pub fn new(id_prefix: &'static str, labels: NativeLabels, closed: Arc<AtomicBool>) -> Self {
        Self {
            calls: Arc::new(StdMutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(0)),
            closed,
            id_prefix,
            labels,
        }
    }

    pub fn mint_id(&self) -> Result<String> {
        let sequence = self
            .next_id
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| {
                Error::new(
                    "internal",
                    format!("{} request id space exhausted", self.labels.short),
                )
            })?;
        Ok(format!("{}-{sequence}", self.id_prefix))
    }

    /// Register one pending reply; the guard unregisters it unless the reply
    /// arrives first.
    pub fn insert(&self, id: String) -> (PendingGuard, oneshot::Receiver<Result<Value>>) {
        let (response_tx, response_rx) = oneshot::channel();
        self.calls.lock_recover().insert(id.clone(), response_tx);
        (
            PendingGuard {
                pending: self.clone(),
                id,
            },
            response_rx,
        )
    }

    pub fn route(&self, key: &str, result: Result<Value>) {
        let sender = self.calls.lock_recover().remove(key);
        if let Some(sender) = sender {
            let _ = sender.send(result);
        }
    }

    /// Whether the reader fatally ended and registration is closed. Future
    /// registrations are refused by the writer boundary before dispatch.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Fatal reader end: atomically close registration and fail every
    /// pending call Unknown. A registration racing this call either lands
    /// before the drain (and is failed Unknown) or after it (and its write
    /// is refused `native_not_dispatched` by the closed writer boundary).
    pub fn fail_all(&self, error: Error) {
        let pending = {
            let mut pending = self.calls.lock_recover();
            self.closed.store(true, Ordering::Release);
            std::mem::take(&mut *pending)
        };
        for (_, sender) in pending {
            let _ = sender.send(Err(error.clone()));
        }
    }

    /// Record-local fault: fail only the currently pending calls Unknown and
    /// keep registration open so the same reader routes later records.
    pub fn fail_current(&self, error: Error) {
        let pending = {
            let mut pending = self.calls.lock_recover();
            std::mem::take(&mut *pending)
        };
        for (_, sender) in pending {
            let _ = sender.send(Err(error.clone()));
        }
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.calls.lock_recover().remove(&self.id);
    }
}

/// Bounded native event queue between the stdout pump and the event task.
pub fn native_frame_channel<Frame>() -> (mpsc::Sender<Frame>, mpsc::Receiver<Frame>) {
    mpsc::channel(NATIVE_EVENT_QUEUE)
}

pub fn encode_frame(server: &'static str, message: &impl Serialize) -> Result<String> {
    let encoded = serde_json::to_string(message).map_err(|_| {
        Error::new(
            "native_encode",
            format!("{server} frame could not be encoded"),
        )
    })?;
    if encoded.len().saturating_add(1) > DEFAULT_MAX_NATIVE_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            format!("{server} frame exceeds 1 MiB"),
        ));
    }
    Ok(encoded)
}

/// Spawn a native child with its stdout pump armed in one step: process
/// birth, the correlated-call map, the bounded frame queue, the stream-state
/// watch and the reader task handed back for drain ownership. Both protocol
/// layers (JSON-RPC and stream-json) use this so the wiring exists exactly
/// once; each builds its own client over the returned writer/pending pair.
/// The caller selects the pump mode explicitly: JSON-RPC keeps
/// [`PumpMode::LossyOverflow`], only the reply-less stream-json caller
/// selects [`PumpMode::LosslessBackpressure`]. The caller also selects the
/// inbound frame cap: [`DEFAULT_MAX_NATIVE_FRAME_BYTES`] unless the
/// adapter's native protocol documents larger frames (bounded by
/// [`MAX_SUPPORTED_NATIVE_FRAME_BYTES`]).
pub async fn spawn_pumped<Frame, Classify>(
    launch: NativeLaunch,
    policy: SecretPolicy,
    labels: NativeLabels,
    id_prefix: &'static str,
    mode: PumpMode,
    max_frame_bytes: usize,
    classify: Classify,
) -> Result<(
    NativeChild,
    PendingCalls,
    mpsc::Receiver<Frame>,
    watch::Receiver<StreamState>,
)>
where
    Frame: Send + 'static,
    Classify: FnMut(Value) -> PumpAction<Frame> + Send + 'static,
{
    if max_frame_bytes == 0 || max_frame_bytes > MAX_SUPPORTED_NATIVE_FRAME_BYTES {
        return Err(Error::new(
            "invalid",
            format!(
                "{server} native frame limit is out of bounds",
                server = labels.server
            ),
        ));
    }
    let (mut transport, stdout) = NativeChild::spawn(launch, policy, labels).await?;
    let process_exit = transport.process_exit_signal();
    let pending = PendingCalls::new(id_prefix, labels, transport.writer().reader_closed_flag());
    let (frame_tx, frame_rx) = native_frame_channel();
    let (stream_tx, stream_rx) = watch::channel(StreamState::Open);
    let output_reader = tokio::spawn(pump_framed_lines(
        stdout,
        frame_tx,
        stream_tx,
        pending.clone(),
        transport.writer(),
        process_exit,
        mode,
        max_frame_bytes,
        classify,
    ));
    transport.set_output_reader(output_reader);
    Ok((transport, pending, frame_rx, stream_rx))
}

fn finish_exit_drain_deadline(
    overflowed: bool,
    stream: &watch::Sender<StreamState>,
    pending: &PendingCalls,
    writer: &NativeWriter,
    server: &str,
    recorded_fault: bool,
) -> Result<()> {
    writer.mark_reader_closed();
    if overflowed {
        stream.send_replace(StreamState::Overflow);
        let error = Error::new(
            "unknown",
            format!("{server} event queue overflowed before exit drain ended"),
        );
        pending.fail_all(error.clone());
        return Err(error);
    }
    // Keep a sticky record-local fault: drain must not erase it with a
    // Drain overwrite before a busy consumer observes it.
    if !recorded_fault {
        stream.send_replace(StreamState::Malformed(NativeStreamFault::Drain));
    }
    pending.fail_all(Error::new(
        "unknown",
        format!("{server} stdout remained open after its process exit drain deadline"),
    ));
    // The owner sees `Malformed` and records the uncovered-tail gap.
    Ok(())
}

enum HeldSend {
    Sent,
    Closed,
    DeadlineOverflow,
}

/// Wait for queue space for one held frame. Respects receiver closure and
/// the existing exit-drain deadline; never touches the line reader.
/// Delivery is attempted first with synchronous `try_send` (deterministic:
/// `Sent`/`Closed`/`Full` with the exact frame returned); the deadline
/// applies only once the queue is proven full and the bounded async wait
/// begins. Polling `send` once (`now_or_never`) is not a substitute: its
/// `Pending` can follow capacity acquisition under the cooperative budget.
async fn send_held_frame<Frame>(
    frames: &mpsc::Sender<Frame>,
    frame: Frame,
    exit_deadline: &mut Option<Instant>,
    process_exit: &mut watch::Receiver<Option<ProcessExit>>,
) -> HeldSend {
    let held = match frames.try_send(frame) {
        Ok(()) => return HeldSend::Sent,
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return HeldSend::Closed,
        Err(error) => error.into_inner(),
    };
    let mut send = Box::pin(frames.send(held));
    loop {
        match *exit_deadline {
            Some(deadline) => {
                if Instant::now() >= deadline {
                    return HeldSend::DeadlineOverflow;
                }
                tokio::select! {
                    result = &mut send => {
                        return if result.is_err() { HeldSend::Closed } else { HeldSend::Sent };
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        return HeldSend::DeadlineOverflow;
                    }
                }
            }
            None => {
                tokio::select! {
                    result = &mut send => {
                        return if result.is_err() { HeldSend::Closed } else { HeldSend::Sent };
                    }
                    _ = process_exit.changed() => {
                        *exit_deadline = Some(process_exit.borrow().as_ref().map(|exit| {
                            exit.observed_at + NATIVE_EXIT_DRAIN
                        }).unwrap_or_else(|| Instant::now() + NATIVE_EXIT_DRAIN));
                    }
                }
            }
        }
    }
}

/// What one raw stdout line decode produced. Oversized lines carry their
/// measured length and a bounded head prefix; nothing else is retained.
#[derive(Debug)]
enum BoundedLineError {
    Oversized { length_bytes: u64, prefix: Vec<u8> },
    Io(std::io::Error),
}

impl From<std::io::Error> for BoundedLineError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Length-checked NDJSON line codec with bounded oversized diagnosis.
/// Lines within the limit decode exactly like [`LinesCodec`] (trailing
/// `\r` stripped, UTF-8 enforced). A line over the limit is discarded
/// through its newline while counting its total bytes and keeping only the
/// first [`OVERSIZED_PREFIX_CAP`] bytes; the pump reports it once as
/// [`NativeStreamFault::FrameOversized`]. Partial lines split across reads
/// accumulate a counter plus the bounded prefix only, never the body, so a
/// multi-gigabyte line still holds kilobytes of memory.
#[derive(Debug)]
struct BoundedLineCodec {
    max_frame_bytes: usize,
    discarding: Option<OversizedAccum>,
}

#[derive(Debug)]
struct OversizedAccum {
    total: u64,
    prefix: Vec<u8>,
}

impl BoundedLineCodec {
    fn new(max_frame_bytes: usize) -> Self {
        Self {
            max_frame_bytes,
            discarding: None,
        }
    }

    fn oversized(&mut self, total: u64, prefix: Vec<u8>) -> BoundedLineError {
        debug_assert!(prefix.len() <= OVERSIZED_PREFIX_CAP);
        BoundedLineError::Oversized {
            length_bytes: total,
            prefix,
        }
    }

    fn decode_content(
        &self,
        mut content: &[u8],
    ) -> std::result::Result<Option<String>, BoundedLineError> {
        if content.last() == Some(&b'\r') {
            content = &content[..content.len().saturating_sub(1)];
        }
        match std::str::from_utf8(content) {
            Ok(valid) => Ok(Some(valid.to_owned())),
            Err(_) => Err(BoundedLineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "native line is not valid UTF-8",
            ))),
        }
    }
}

impl Decoder for BoundedLineCodec {
    type Item = String;
    type Error = BoundedLineError;

    fn decode(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<String>, BoundedLineError> {
        if let Some(newline) = src.iter().position(|byte| *byte == b'\n') {
            let end = newline.saturating_add(1);
            if let Some(mut accum) = self.discarding.take() {
                let total = accum.total.saturating_add(end as u64);
                let take = OVERSIZED_PREFIX_CAP
                    .saturating_sub(accum.prefix.len())
                    .min(newline);
                accum.prefix.extend_from_slice(&src[..take]);
                let _ = src.split_to(end);
                return Err(self.oversized(total, accum.prefix));
            }
            if newline.saturating_add(1) > self.max_frame_bytes {
                let take = newline.min(OVERSIZED_PREFIX_CAP);
                let prefix = src[..take].to_vec();
                let _ = src.split_to(end);
                return Err(self.oversized(end as u64, prefix));
            }
            let line = src.split_to(end);
            return self.decode_content(&line[..newline]);
        }
        if let Some(accum) = self.discarding.as_mut() {
            accum.total = accum.total.saturating_add(src.len() as u64);
            let take = OVERSIZED_PREFIX_CAP
                .saturating_sub(accum.prefix.len())
                .min(src.len());
            accum.prefix.extend_from_slice(&src[..take]);
            src.clear();
            return Ok(None);
        }
        if src.len() > self.max_frame_bytes {
            // No newline yet and already over the limit: oversized whatever
            // follows. Discard what is buffered, count the rest as it arrives.
            let chunk = src.split_to(src.len());
            let take = chunk.len().min(OVERSIZED_PREFIX_CAP);
            self.discarding = Some(OversizedAccum {
                total: chunk.len() as u64,
                prefix: chunk[..take].to_vec(),
            });
        }
        Ok(None)
    }

    fn decode_eof(
        &mut self,
        src: &mut BytesMut,
    ) -> std::result::Result<Option<String>, BoundedLineError> {
        if src.is_empty() {
            if let Some(accum) = self.discarding.take() {
                return Err(self.oversized(accum.total, accum.prefix));
            }
            return Ok(None);
        }
        if let Some(mut accum) = self.discarding.take() {
            accum.total = accum.total.saturating_add(src.len() as u64);
            let take = OVERSIZED_PREFIX_CAP
                .saturating_sub(accum.prefix.len())
                .min(src.len());
            accum.prefix.extend_from_slice(&src[..take]);
            src.clear();
            return Err(self.oversized(accum.total, accum.prefix));
        }
        if src.len().saturating_add(1) > self.max_frame_bytes {
            let take = src.len().min(OVERSIZED_PREFIX_CAP);
            let prefix = src[..take].to_vec();
            let total = src.len() as u64;
            src.clear();
            return Err(self.oversized(total, prefix));
        }
        let line = src.split_to(src.len());
        self.decode_content(&line)
    }
}

/// Bounded stdout line pump: length-checked NDJSON lines, protocol
/// classification of each parsed record, one overflow gap, and a bounded
/// post-exit drain before any loss conclusion. The classifier owns every
/// protocol decision; the pump owns every transport decision.
#[allow(clippy::too_many_arguments)]
async fn pump_framed_lines<Frame, Classify>(
    stdout: ChildStdout,
    frames: mpsc::Sender<Frame>,
    stream: watch::Sender<StreamState>,
    pending: PendingCalls,
    writer: NativeWriter,
    mut process_exit: watch::Receiver<Option<ProcessExit>>,
    mode: PumpMode,
    max_frame_bytes: usize,
    mut classify: Classify,
) -> Result<()>
where
    Frame: Send + 'static,
    Classify: FnMut(Value) -> PumpAction<Frame>,
{
    let server = writer.labels.server;
    let mut reader = FramedRead::new(stdout, BoundedLineCodec::new(max_frame_bytes));
    let mut overflowed = false;
    let mut exit_deadline = process_exit
        .borrow()
        .as_ref()
        .map(|exit| exit.observed_at + NATIVE_EXIT_DRAIN);
    if exit_deadline.is_none() && process_exit.has_changed().is_err() {
        exit_deadline = Some(Instant::now() + NATIVE_EXIT_DRAIN);
    }
    let mut post_deadline_ready_lines = 0;
    let mut recorded_fault = false;
    loop {
        let line = match exit_deadline {
            Some(deadline) if Instant::now() >= deadline => {
                if post_deadline_ready_lines >= NATIVE_EVENT_QUEUE {
                    return finish_exit_drain_deadline(
                        overflowed,
                        &stream,
                        &pending,
                        &writer,
                        server,
                        recorded_fault,
                    );
                }
                match reader.next().now_or_never() {
                    Some(line) => {
                        post_deadline_ready_lines += 1;
                        line
                    }
                    None => {
                        return finish_exit_drain_deadline(
                            overflowed,
                            &stream,
                            &pending,
                            &writer,
                            server,
                            recorded_fault,
                        );
                    }
                }
            }
            Some(deadline) => {
                tokio::select! {
                    // Prefer ready output while time remains. The loop checks
                    // the deadline before every poll, so continuous output
                    // cannot starve it.
                    biased;
                    line = reader.next() => line,
                    _ = tokio::time::sleep_until(deadline) => continue,
                }
            }
            None => {
                tokio::select! {
                    line = reader.next() => line,
                    _ = process_exit.changed() => {
                        exit_deadline = Some(
                            process_exit
                                .borrow()
                                .as_ref()
                                .map(|exit| exit.observed_at + NATIVE_EXIT_DRAIN)
                                .unwrap_or_else(|| Instant::now() + NATIVE_EXIT_DRAIN),
                        );
                        continue;
                    }
                }
            }
        };
        let Some(line) = line else {
            break;
        };
        let line = match line {
            Ok(line) if line.len().saturating_add(1) <= max_frame_bytes => line,
            Ok(line) => {
                // Unreachable through BoundedLineCodec, which rejects
                // over-limit lines with a captured prefix; kept so no
                // oversized line can pass undiagnosed.
                let detail =
                    OversizedFrame::unparsed(max_frame_bytes, line.len().saturating_add(1) as u64);
                return Err(mark_stream_malformed(
                    stream,
                    pending,
                    &writer,
                    NativeStreamFault::FrameOversized(detail),
                    &format!(
                        "{server} stream ended on an oversized frame ({})",
                        detail.describe()
                    ),
                ));
            }
            Err(BoundedLineError::Oversized {
                length_bytes,
                prefix,
            }) => {
                let detail = OversizedFrame::parse(max_frame_bytes, length_bytes, &prefix);
                return Err(mark_stream_malformed(
                    stream,
                    pending,
                    &writer,
                    NativeStreamFault::FrameOversized(detail),
                    &format!(
                        "{server} stream ended on an oversized frame ({})",
                        detail.describe()
                    ),
                ));
            }
            Err(BoundedLineError::Io(error)) => {
                let fault = native_codec_fault(error);
                return Err(mark_stream_malformed(
                    stream,
                    pending,
                    &writer,
                    fault,
                    &format!("{server} line codec failed ({})", fault.describe()),
                ));
            }
        };
        let value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(_) => {
                // Record-local: the bounded UTF-8 line boundary is proven, so
                // record the typed fault, fail current pending Unknown, and
                // keep this same reader routing later well-formed records.
                record_stream_fault(
                    &stream,
                    &pending,
                    NativeStreamFault::Json,
                    &format!("{server} stream contained invalid JSON"),
                );
                recorded_fault = true;
                continue;
            }
        };
        let mut receiver_closed = false;
        match classify(value) {
            PumpAction::Emit(frame) => {
                if overflowed {
                    continue;
                }
                match mode {
                    PumpMode::LossyOverflow => {
                        if frames.try_send(frame).is_err() {
                            overflowed = true;
                            stream.send_replace(StreamState::Overflow);
                        }
                    }
                    PumpMode::LosslessBackpressure => {
                        match send_held_frame(&frames, frame, &mut exit_deadline, &mut process_exit)
                            .await
                        {
                            HeldSend::Sent => {}
                            HeldSend::Closed => receiver_closed = true,
                            HeldSend::DeadlineOverflow => overflowed = true,
                        }
                    }
                }
            }
            PumpAction::Route { key, result } => {
                pending.route(&key, result);
            }
            PumpAction::Malformed(message) => {
                // Record-local envelope fault: same bounded-line boundary as
                // invalid JSON above; record typed fault, fail current
                // pending Unknown, continue with later well-formed records.
                record_stream_fault(&stream, &pending, NativeStreamFault::Envelope, &message);
                recorded_fault = true;
            }
            PumpAction::Skip => {}
        }
        if receiver_closed {
            // The frame consumer is gone; stop like EOF so pending calls
            // fail honestly below instead of waiting forever.
            break;
        }
    }

    if overflowed {
        writer.mark_reader_closed();
        stream.send_replace(StreamState::Overflow);
        let error = Error::new(
            "unknown",
            format!("{server} event queue overflowed before stdout reached EOF"),
        );
        pending.fail_all(error.clone());
        return Err(error);
    }

    writer.mark_reader_closed();
    // Sticky fault: EOF must not overwrite a recorded record-local fault
    // with Closed before a busy consumer observes it (planned-stop
    // consumers suppress Closed gaps, which would erase the fault).
    if !recorded_fault {
        stream.send_replace(StreamState::Closed);
    }
    pending.fail_all(Error::new(
        "unknown",
        format!("{server} stream closed while an outcome may be pending"),
    ));
    Ok(())
}

fn native_codec_fault(error: std::io::Error) -> NativeStreamFault {
    if error.kind() == std::io::ErrorKind::InvalidData {
        NativeStreamFault::FrameInvalidData
    } else {
        NativeStreamFault::FrameIo
    }
}

fn mark_stream_malformed(
    stream: watch::Sender<StreamState>,
    pending: PendingCalls,
    writer: &NativeWriter,
    fault: NativeStreamFault,
    message: &str,
) -> Error {
    writer.mark_reader_closed();
    stream.send_replace(StreamState::Malformed(fault));
    let error = Error::new("unknown", message);
    pending.fail_all(error.clone());
    error
}

/// Record-local fault: typed fault becomes the stream state (gap recorded by
/// the consumer), current pending calls fail Unknown, registration stays
/// open and the same reader continues with later well-formed records.
fn record_stream_fault(
    stream: &watch::Sender<StreamState>,
    pending: &PendingCalls,
    fault: NativeStreamFault,
    message: &str,
) {
    stream.send_replace(StreamState::Malformed(fault));
    pending.fail_current(Error::new("unknown", message));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    fn exit_status() -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(0)
    }

    #[cfg(windows)]
    fn exit_status() -> ExitStatus {
        use std::os::windows::process::ExitStatusExt;
        ExitStatus::from_raw(0)
    }

    #[test]
    fn publish_then_close_retains_positive_exit() {
        // The waiter publishes `Some(status)` then drops its sender.
        // Tokio reports closure even with the final value retained; the
        // read must still prove the exit, not conclude unknown.
        let (exit_tx, exit_rx) = watch::channel(None);
        exit_tx.send_replace(Some(ProcessExit {
            status: exit_status(),
            observed_at: Instant::now(),
        }));
        drop(exit_tx);
        assert!(exit_rx.has_changed().is_err());
        let status = read_process_exit(&exit_rx, "Test").expect("retained exit");
        assert!(status.is_some_and(|status| status.success()));
    }

    #[test]
    fn close_without_publish_stays_unknown() {
        // No publish before the drop: there is no exit to prove.
        let (exit_tx, exit_rx) = watch::channel(None);
        drop(exit_tx);
        let error = read_process_exit(&exit_rx, "Test").expect_err("unknown exit");
        assert_eq!(error.code, "unknown");
    }

    #[test]
    fn open_without_publish_stays_pending() {
        // The waiter has not published and still holds its sender.
        let (_exit_tx, exit_rx) = watch::channel(None);
        let status = read_process_exit(&exit_rx, "Test").expect("pending");
        assert!(status.is_none());
    }

    #[test]
    fn oversized_prefix_parses_notification_method() {
        let prefix =
            b"{\"jsonrpc\":\"2.0\",\"method\":\"item/completed\",\"params\":{\"sessionId\":\"s\"}}";
        let detail = OversizedFrame::parse(1024 * 1024, 2_000_000, prefix);
        assert_eq!(detail.length_bytes(), 2_000_000);
        assert_eq!(detail.limit_bytes(), 1024 * 1024);
        assert_eq!(detail.kind(), OversizedFrameKind::Notification);
        assert_eq!(detail.method(), Some("item/completed"));
        let text = detail.describe();
        assert!(text.contains("len=2000000"), "{text}");
        assert!(text.contains("limit=1048576"), "{text}");
        assert!(text.contains("kind=notification"), "{text}");
        assert!(text.contains("method=item/completed"), "{text}");
        assert_eq!(
            NativeStreamFault::FrameOversized(detail).label(),
            "frame_oversized"
        );
    }

    #[test]
    fn oversized_prefix_parses_response_without_method() {
        let prefix = br#"{"jsonrpc":"2.0","id":"muse-42","result":{"session":{"sessionId":"s"}}"#;
        let detail = OversizedFrame::parse(8 * 1024 * 1024, 9_000_000, prefix);
        assert_eq!(detail.kind(), OversizedFrameKind::Response);
        assert_eq!(detail.method(), None);
        let text = detail.describe();
        assert!(text.contains("kind=response"), "{text}");
        assert!(!text.contains("method="), "{text}");
    }

    #[test]
    fn oversized_prefix_parses_request_with_id() {
        let prefix = br#"{"jsonrpc":"2.0","id":7,"method":"approval/request","params":{}}"#;
        let detail = OversizedFrame::parse(1024 * 1024, 2_000_000, prefix);
        assert_eq!(detail.kind(), OversizedFrameKind::Request);
        assert_eq!(detail.method(), Some("approval/request"));
    }

    #[test]
    fn oversized_detail_never_carries_body_or_secrets() {
        // Free-text bodies fail the token filter, so no method is recorded,
        // and the description never contains body bytes.
        let secret = "sekrit body payload 12345";
        let prefix =
            format!(r#"{{"jsonrpc":"2.0","method":"{secret}","params":{{"text":"{secret}"}}}}"#);
        let detail = OversizedFrame::parse(1024 * 1024, 5_000_000, prefix.as_bytes());
        assert_eq!(detail.method(), None);
        let text = detail.describe();
        assert!(!text.contains(secret), "{text}");
        assert!(text.len() < 256, "{text}");

        // A token-shaped method records only the token itself, never the
        // surrounding body or the id value.
        let prefix = format!(
            r#"{{"jsonrpc":"2.0","id":"muse-9","method":"sekrit-token","params":{{"text":"{secret}"}}}}"#
        );
        let detail = OversizedFrame::parse(1024 * 1024, 5_000_000, prefix.as_bytes());
        assert_eq!(detail.kind(), OversizedFrameKind::Request);
        assert_eq!(detail.method(), Some("sekrit-token"));
        let text = detail.describe();
        assert!(!text.contains(secret), "{text}");
        assert!(!text.contains("muse-9"), "{text}");
        assert!(text.len() < 256, "{text}");
    }

    #[test]
    fn oversized_prefix_ignores_nested_method_keys() {
        let prefix = br#"{"jsonrpc":"2.0","id":"muse-1","result":{"method":"not-a-real-method","items":[]}}"#;
        let detail = OversizedFrame::parse(1024 * 1024, 2_000_000, prefix);
        assert_eq!(detail.kind(), OversizedFrameKind::Response);
        assert_eq!(detail.method(), None);
    }

    #[test]
    fn bounded_codec_reports_split_oversized_line_once() {
        use tokio_util::codec::Decoder;
        let mut codec = BoundedLineCodec::new(32);
        let mut src = BytesMut::from(&b"{\"jsonrpc\":\"2.0\",\"method\":\""[..]);
        assert!(codec.decode(&mut src).expect("need more").is_none());
        src.extend_from_slice(b"item/completed\",\"params\":{\"text\":\"");
        assert!(codec.decode(&mut src).expect("need more").is_none());
        src.extend_from_slice(&[b'x'; 64]);
        assert!(codec.decode(&mut src).expect("need more").is_none());
        src.extend_from_slice(b"\"}}\"\n");
        let error = codec.decode(&mut src).expect_err("oversized");
        let BoundedLineError::Oversized {
            length_bytes,
            prefix,
        } = error
        else {
            panic!("expected oversized, got Io");
        };
        assert!(length_bytes > 32, "{length_bytes}");
        assert!(prefix.len() <= OVERSIZED_PREFIX_CAP);
        assert!(prefix.starts_with(b"{\"jsonrpc\""));
        // The line was consumed through its newline: a following well-formed
        // line still decodes.
        src.extend_from_slice(b"{}\n");
        let line = codec
            .decode(&mut src)
            .expect("valid line")
            .expect("one line");
        assert_eq!(line, "{}");
    }

    #[test]
    fn bounded_codec_within_limit_matches_lines_codec() {
        use tokio_util::codec::Decoder;
        let mut codec = BoundedLineCodec::new(1024 * 1024);
        let mut src = BytesMut::from(&b"{}\r\n"[..]);
        let line = codec
            .decode(&mut src)
            .expect("valid line")
            .expect("one line");
        assert_eq!(line, "{}");
        let mut src = BytesMut::from(&b"{\"a\":1}\n trailing"[..]);
        let line = codec
            .decode(&mut src)
            .expect("valid line")
            .expect("one line");
        assert_eq!(line, "{\"a\":1}");
    }

    #[test]
    fn bounded_codec_rejects_invalid_utf8_like_lines_codec() {
        use tokio_util::codec::Decoder;
        let mut codec = BoundedLineCodec::new(1024 * 1024);
        let mut src = BytesMut::from(&[0xFF, 0xFE, b'\n'][..]);
        let error = codec.decode(&mut src).expect_err("invalid utf-8");
        let BoundedLineError::Io(error) = error else {
            panic!("expected Io variant");
        };
        assert_eq!(
            native_codec_fault(error).label(),
            NativeStreamFault::FrameInvalidData.label()
        );
    }
}
