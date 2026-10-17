//! Test-only failure evidence. These records never authorize or release custody.
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::Command,
};

const TRACE_LIMIT: usize = 64 * 1024;
const TRACE_EVENTS: usize = 12;
pub(super) const OWNER_WITNESS_ENV: &str = "ELIOT_FORGE_OWNER_IMAGE_WITNESS";
const OWNER_PHASES: &[&str] = &[
    "entered",
    "group_ready",
    "image_observation",
    "image_observed",
    "executable_pinned",
    "valid_owner_checked",
    "invalid_claims_checked",
    "roundtrip_checked",
    "assertions_complete",
];
const IMAGE_REASONS: &[&str] = &[
    "not_applicable",
    "unclassified",
    "image_path_not_unicode",
    "image_path_boundary",
    "image_file_boundary",
    "image_size_overflow",
    "image_hash_boundary",
    "image_hash_changed",
    "proc_stat_invalid",
    "proc_stat_incomplete",
    "proc_group_invalid",
    "pidfd_unobservable",
    "boot_id_unavailable",
    "exe_path_not_absolute",
    "post_hash_identity_changed",
    "process_gone",
    "process_executable_gone",
    "process_birth_changed_on_open",
    "process_gone_during_inventory",
];

fn classify_image_error(code: &str, message: &str) -> &'static str {
    // Exact literals from process_group.rs, never substrings or raw diagnostics.
    match (code, message) {
        ("PROCESS_IDENTITY", "process executable path is not valid Unicode") => {
            "image_path_not_unicode"
        }
        ("PROCESS_IDENTITY", "process executable path is outside the supported boundary") => {
            "image_path_boundary"
        }
        ("PROCESS_IDENTITY", "process executable is outside the supported file boundary") => {
            "image_file_boundary"
        }
        ("PROCESS_IDENTITY", "process image size overflow") => "image_size_overflow",
        ("PROCESS_IDENTITY", "process executable exceeds the supported hash boundary") => {
            "image_hash_boundary"
        }
        ("PROCESS_IDENTITY", "process executable changed during bounded hashing") => {
            "image_hash_changed"
        }
        ("PROCESS_IDENTITY", "invalid proc stat") => "proc_stat_invalid",
        ("PROCESS_IDENTITY", "incomplete proc stat") => "proc_stat_incomplete",
        ("PROCESS_IDENTITY", "invalid process group") => "proc_group_invalid",
        ("PROCESS_IDENTITY", "cannot observe process pidfd") => "pidfd_unobservable",
        ("PROCESS_IDENTITY", "kernel boot identity is unavailable") => "boot_id_unavailable",
        ("PROCESS_IDENTITY", "kernel executable path is not absolute") => "exe_path_not_absolute",
        // This source branch combines birth/liveness/path/device/inode checks;
        // its reason does not identify which individual condition failed.
        (
            "PROCESS_IDENTITY",
            "process birth or executable image changed during bounded hashing",
        ) => "post_hash_identity_changed",
        ("PROCESS_GONE", "process is no longer live") => "process_gone",
        ("PROCESS_GONE", "process executable disappeared") => "process_executable_gone",
        ("PROCESS_GONE", "process birth changed while opening its executable") => {
            "process_birth_changed_on_open"
        }
        ("PROCESS_GONE", "process exited during inventory") => "process_gone_during_inventory",
        _ => "unclassified",
    }
}

fn read_bounded(path: &Path, limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take((limit + 1) as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn read_class(error: &io::Error) -> &'static str {
    if error.kind() == io::ErrorKind::NotFound {
        "not_recorded"
    } else {
        "read_error"
    }
}

pub(super) fn attach_git_trace(command: &mut Command, directory: &Path) -> PathBuf {
    let path = directory.join(format!("{}.jsonl", crate::model::new_id()));
    command
        .env("GIT_TRACE2_EVENT", &path)
        .env("GIT_TRACE2_CONFIG_PARAMS", "")
        .env("GIT_TRACE2_ENV_VARS", "");
    path
}

pub(super) fn git_trace_summary(path: &Path) -> Value {
    match read_bounded(path, TRACE_LIMIT) {
        Ok(bytes) => classify_git_trace(&bytes),
        Err(error) => json!({"read":read_class(&error)}),
    }
}

fn classify_git_trace(bytes: &[u8]) -> Value {
    // Trace2 includes arbitrary argv, paths, messages and optional environment
    // data. Project only closed classifications; never echo a parsed object.
    // Git's api-trace2 documents child_id as local to its emitting process.
    // We deliberately do not infer a root/child match from file ordering.
    let mut events = std::collections::VecDeque::new();
    let mut malformed = 0_usize;
    let mut seen = 0_usize;
    let mut last_event = "none";
    for line in bytes[..bytes.len().min(TRACE_LIMIT)].split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_slice::<Value>(line) else {
            malformed += 1;
            continue;
        };
        if !value.is_object() || value["event"].as_str().is_none() {
            malformed += 1;
            continue;
        }
        seen += 1;
        let event = match value["event"].as_str() {
            Some("start") => "start",
            Some("cmd_name") => "cmd_name",
            Some("child_start") => "child_start",
            Some("child_exit") => "child_exit",
            Some("exit") => "exit",
            Some("atexit") => "atexit",
            Some("error") => "error",
            Some("signal") => "signal",
            _ => "other",
        };
        last_event = event;
        if event == "other" {
            continue;
        }
        let mut record = json!({"event":event});
        if event == "cmd_name" {
            record["command"] = json!(match value["name"].as_str() {
                Some("init") => "init",
                Some("commit") => "commit",
                Some("branch") => "branch",
                Some("rev-parse") => "rev-parse",
                Some("fetch") => "fetch",
                Some("remote") => "remote",
                Some("merge-base") => "merge-base",
                _ => "other",
            });
        }
        if matches!(event, "child_start" | "child_exit") {
            record["child_id"] = json!(
                value["child_id"]
                    .as_u64()
                    .filter(|id| *id <= u32::MAX.into())
            );
        }
        if event == "child_start" {
            record["class"] = json!(match value["child_class"].as_str() {
                Some("hook") => "hook",
                Some("editor") => "editor",
                Some("pager") => "pager",
                Some(class) if class.starts_with("transport/") => "transport",
                _ => "other",
            });
            record["hook"] = json!(match value["hook_name"].as_str() {
                Some("pre-commit") => "pre-commit",
                Some("prepare-commit-msg") => "prepare-commit-msg",
                Some("commit-msg") => "commit-msg",
                Some("post-commit") => "post-commit",
                Some("reference-transaction") => "reference-transaction",
                _ => "other",
            });
        }
        if matches!(event, "child_exit" | "exit" | "atexit") {
            record["code"] = json!(
                value["code"]
                    .as_i64()
                    .filter(|code| i32::try_from(*code).is_ok())
            );
        }
        if event == "signal" {
            record["signal"] = json!(value["signo"].as_u64().filter(|signal| *signal <= 255));
        }
        if events.len() == TRACE_EVENTS {
            events.pop_front();
        }
        events.push_back(record);
    }
    json!({
        "read":"ok", "truncated":bytes.len() > TRACE_LIMIT,
        "events_seen":seen, "malformed_lines":malformed,
        "last_event":last_event, "tail":events,
    })
}

#[derive(Clone)]
pub(super) struct OwnerImageWitness(pub(super) PathBuf);

impl OwnerImageWitness {
    pub(super) fn from_environment() -> Self {
        Self(
            std::env::var_os(OWNER_WITNESS_ENV)
                .expect("parent supplies witness directory")
                .into(),
        )
    }

    fn write_once(&self, name: &str, bytes: &[u8]) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.0.join(name))?;
        file.write_all(bytes)?;
        file.sync_all()
    }

    pub(super) fn phase(&self, phase: &str) {
        assert!(OWNER_PHASES.contains(&phase));
        self.write_once(phase, b"recorded\n")
            .expect("persist fixture phase");
        eprintln!("FORGE_OWNER_IMAGE_PHASE {phase}");
    }

    pub(super) fn install_panic_witness(&self) {
        let witness = self.clone();
        // Only the isolated child installs this hook. Never serialize panic
        // payloads, locations, executable paths or the original error message.
        std::panic::set_hook(Box::new(move |_| {
            let _ = witness.write_once("panic", b"recorded\n");
        }));
    }

    #[cfg(target_os = "linux")]
    pub(super) fn observe_self_exe_metadata(&self) {
        // Observe the running executable through the opened kernel reference,
        // not a configured path. This is a separate pre-call metadata sample.
        let metadata = match File::open("/proc/self/exe").and_then(|file| file.metadata()) {
            Ok(metadata) => json!({
                "read_status":"ok", "is_file":metadata.is_file(), "len_bytes":metadata.len(),
            }),
            Err(_) => json!({"read_status":"read_error", "is_file":null, "len_bytes":null}),
        };
        self.write_once("self_exe_metadata", metadata.to_string().as_bytes())
            .expect("persist self executable metadata");
    }

    pub(super) fn image_returned(&self, result: Result<(), (&str, &str)>) {
        let classification = match result {
            Ok(()) => "returned_ok",
            Err(("PROCESS_GONE", _)) => "returned_error_PROCESS_GONE",
            Err(("PROCESS_IDENTITY", _)) => "returned_error_PROCESS_IDENTITY",
            Err(("IO_ERROR", _)) => "returned_error_IO_ERROR",
            Err(_) => "returned_error_other",
        };
        self.write_once("image_result", classification.as_bytes())
            .expect("persist image observation return");
        let reason = match result {
            Ok(()) => "not_applicable",
            Err((code, message)) => classify_image_error(code, message),
        };
        self.write_once("image_reason", reason.as_bytes())
            .expect("persist closed image observation reason");
    }

    fn self_exe_metadata_summary(&self) -> Value {
        let unavailable = |status| json!({"read_status":status, "is_file":null, "len_bytes":null});
        let bytes = match read_bounded(&self.0.join("self_exe_metadata"), 256) {
            Ok(bytes) => bytes,
            Err(error) => return unavailable(read_class(&error)),
        };
        if bytes.len() > 256 {
            return unavailable("invalid");
        }
        match serde_json::from_slice::<Value>(&bytes) {
            Ok(value)
                if value["read_status"] == "ok"
                    && value["is_file"].is_boolean()
                    && value["len_bytes"].as_u64().is_some() =>
            {
                json!({
                    "read_status":"ok", "is_file":value["is_file"],
                    "len_bytes":value["len_bytes"],
                })
            }
            Ok(value) if value["read_status"] == "read_error" => unavailable("read_error"),
            _ => unavailable("invalid"),
        }
    }

    pub(super) fn summary(&self) -> Value {
        let mut phase = "not_recorded";
        let mut invalid_phase = false;
        for &candidate in OWNER_PHASES {
            match read_bounded(&self.0.join(candidate), 32) {
                Ok(bytes) if bytes == b"recorded\n" => phase = candidate,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                _ => invalid_phase = true,
            }
        }
        let image_return = match read_bounded(&self.0.join("image_result"), 64) {
            Ok(bytes) => match bytes.as_slice() {
                b"returned_ok" => "returned_ok",
                b"returned_error_PROCESS_GONE" => "returned_error_PROCESS_GONE",
                b"returned_error_PROCESS_IDENTITY" => "returned_error_PROCESS_IDENTITY",
                b"returned_error_IO_ERROR" => "returned_error_IO_ERROR",
                b"returned_error_other" => "returned_error_other",
                _ => "invalid",
            },
            Err(ref error) => read_class(error),
        };
        let panic = match read_bounded(&self.0.join("panic"), 32) {
            Ok(bytes) if bytes == b"recorded\n" => "recorded",
            Ok(_) => "invalid",
            Err(ref error) => read_class(error),
        };
        let image_reason = match read_bounded(&self.0.join("image_reason"), 64) {
            Ok(bytes) => IMAGE_REASONS
                .iter()
                .find(|reason| bytes == reason.as_bytes())
                .copied()
                .unwrap_or("invalid"),
            Err(ref error) => read_class(error),
        };
        json!({
            "phase":phase, "invalid_phase":invalid_phase,
            "image_return":image_return, "panic":panic,
            "image_reason":image_reason, "self_exe_metadata":self.self_exe_metadata_summary(),
        })
    }
}

#[test]
fn forge_failure_witness_trace_projection_drops_untrusted_fields_and_bounds_input() {
    let records = [
        json!({"event":"start", "argv":["SECRET_ARG"], "env":"SECRET_ENV"}),
        json!({"event":"child_start", "child_id":2, "child_class":"hook", "hook_name":"commit-msg", "argv":["SECRET_PATH"]}),
        json!({"event":"child_exit", "child_id":2, "code":1}),
        json!({"event":"exit", "code":"SECRET_CODE", "msg":"SECRET_MESSAGE"}),
        json!({"event":"SECRET_EVENT"}),
        json!({"event":"child_start", "child_id":"SECRET_ID", "child_class":"SECRET_CLASS", "hook_name":"SECRET_HOOK"}),
        json!({"event":"cmd_name", "name":"SECRET_COMMAND"}),
    ];
    let bytes = records
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    let summary = classify_git_trace(bytes.as_bytes());
    assert!(!summary.to_string().contains("SECRET"));
    assert_eq!(summary["tail"][1]["hook"], "commit-msg");
    assert_eq!(summary["tail"][2]["code"], 1);
    assert_eq!(summary["tail"][3]["code"], Value::Null);
    assert_eq!(summary["tail"][4]["class"], "other");
    assert_eq!(summary["tail"][4]["hook"], "other");
    assert_eq!(summary["tail"][4]["child_id"], Value::Null);
    assert_eq!(summary["tail"][5]["command"], "other");

    let mut large = b"{\"event\":\"start\"}\n".repeat(TRACE_EVENTS + 3);
    large.resize(TRACE_LIMIT, b'\n');
    large.extend_from_slice(b"{\"event\":\"exit\",\"code\":0}\n");
    let bounded = classify_git_trace(&large);
    assert_eq!(bounded["truncated"], true);
    let tail = bounded["tail"].as_array().unwrap();
    assert_eq!(tail.len(), TRACE_EVENTS);
    assert!(tail.iter().all(|event| event["event"] == "start"));
    assert!(bounded.to_string().len() < 4096);
    assert_eq!(
        classify_git_trace(b"not-json\nnull\n{\"event\":false}\n")["malformed_lines"],
        3,
    );
}

#[test]
fn forge_failure_witness_owner_return_is_distinct_from_missing_and_invalid_evidence() {
    let directory = std::env::temp_dir().join(format!("forge-witness-{}", crate::model::new_id()));
    fs::create_dir(&directory).unwrap();
    let witness = OwnerImageWitness(directory.clone());
    witness.phase("image_observation");
    let absent = witness.summary();
    assert_eq!(absent["phase"], "image_observation");
    assert_eq!(absent["image_return"], "not_recorded");
    assert_eq!(absent["panic"], "not_recorded");
    assert_eq!(absent["image_reason"], "not_recorded");
    assert_eq!(
        absent["self_exe_metadata"],
        json!({"read_status":"not_recorded", "is_file":null, "len_bytes":null}),
    );
    witness.image_returned(Err(("IO_ERROR", "SECRET_ERROR /private/path")));
    witness.write_once("panic", b"recorded\n").unwrap();
    let returned = witness.summary();
    assert_eq!(returned["image_return"], "returned_error_IO_ERROR");
    assert_eq!(returned["image_reason"], "unclassified");
    assert_eq!(returned["panic"], "recorded");
    assert!(!returned.to_string().contains("SECRET"));
    let identity_directory = directory.join("identity");
    fs::create_dir(&identity_directory).unwrap();
    let identity_witness = OwnerImageWitness(identity_directory);
    identity_witness.image_returned(Err(("PROCESS_IDENTITY", "SECRET_IDENTITY_ERROR")));
    let identity = identity_witness.summary();
    assert_eq!(identity["image_return"], "returned_error_PROCESS_IDENTITY");
    assert_eq!(identity["image_reason"], "unclassified");
    assert!(!identity.to_string().contains("SECRET"));
    let boundary = "process executable is outside the supported file boundary";
    assert_eq!(
        classify_image_error("PROCESS_IDENTITY", boundary),
        "image_file_boundary"
    );
    assert_eq!(classify_image_error("IO_ERROR", boundary), "unclassified");
    assert_eq!(
        classify_image_error("PROCESS_IDENTITY", &format!("{boundary} SECRET_SUFFIX")),
        "unclassified",
    );
    assert_eq!(
        classify_image_error(
            "PROCESS_IDENTITY",
            "process birth or executable image changed during bounded hashing",
        ),
        "post_hash_identity_changed",
    );
    fs::write(directory.join("image_result"), b"SECRET_ERROR".repeat(100)).unwrap();
    fs::write(directory.join("image_reason"), b"SECRET_REASON").unwrap();
    fs::write(
        directory.join("self_exe_metadata"),
        br#"{"read_status":"ok","is_file":true,"len_bytes":"SECRET_SIZE"}"#,
    )
    .unwrap();
    let invalid = witness.summary();
    assert_eq!(invalid["image_return"], "invalid");
    assert_eq!(invalid["image_reason"], "invalid");
    assert_eq!(
        invalid["self_exe_metadata"],
        json!({"read_status":"invalid", "is_file":null, "len_bytes":null}),
    );
    assert!(!invalid.to_string().contains("SECRET"));
    fs::remove_dir_all(directory).unwrap();
}
