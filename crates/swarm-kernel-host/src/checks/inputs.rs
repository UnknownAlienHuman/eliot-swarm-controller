//! Resolve and fingerprint every CheckRunner input before Store admission.
//! This module has no database dependency; callers revalidate its facts in the
//! reservation transaction before the scheduler can advance.
use super::{
    model::{CheckProfile, Parser},
    scope::{self, CargoGraph, ScopeMode, ScopePlan},
    source::{self, VerifiedSource},
};
use crate::{
    error::{Error, Result},
    model,
    platform::process_group::{Group, spawned_identity},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

const PROBE_MAGIC: &[u8; 8] = b"SWPRB01\0";
const PROBE_VERSION: u32 = 1;
const PROBE_TIMEOUT_MS: u64 = 30_000;
const PROBE_STDOUT_LIMIT: usize = 16 * 1024 * 1024;
const PROBE_STDERR_LIMIT: usize = 64 * 1024;
const PROBE_REQUEST_LIMIT: usize = 1024 * 1024;
const PROBE_RESPONSE_LIMIT: usize = PROBE_STDOUT_LIMIT + PROBE_STDERR_LIMIT + 1024;
const PROBE_CLEANUP_GRACE: Duration = Duration::from_secs(5);
const PROBE_CLEANUP_RETRY: Duration = Duration::from_millis(250);
const PROBE_MAX_CLEANUP_REQUESTS: u8 = 3;
const PROBE_CAPTURE_DRAIN_GRACE: Duration = Duration::from_secs(5);
const PROBE_HELPER_WAIT_GRACE: Duration = Duration::from_secs(45);
type ProbeFn = fn(&Path, &[String], &BTreeMap<String, String>, &Path) -> Result<Vec<u8>>;
#[cfg(test)]
pub(crate) type TestProbeFn =
    fn(&Path, &[String], &BTreeMap<String, String>, &Path) -> Result<Vec<u8>>;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeRequest {
    probe_version: u32,
    program: PathBuf,
    args: Vec<String>,
    cwd: PathBuf,
    timeout_ms: u64,
    stdout_limit: usize,
    stderr_limit: usize,
}

#[derive(Debug)]
struct ProbeResponse {
    success: bool,
    timed_out: bool,
    output_limited: bool,
    exit_code: Option<i32>,
    group_empty: bool,
    message: String,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

struct TempProbeDir(PathBuf, bool);

impl TempProbeDir {
    fn preserve(&mut self) {
        self.1 = true;
    }

    fn allow_cleanup(&mut self) {
        self.1 = false;
    }
}

impl Drop for TempProbeDir {
    fn drop(&mut self) {
        if self.1 {
            return;
        }
        for name in [
            "request.json",
            "probe-owner.json",
            "stdout.partial",
            "stderr.partial",
        ] {
            let _ = fs::remove_file(self.0.join(name));
        }
        let _ = fs::remove_dir(&self.0);
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedCheckPlan {
    pub resolved_inputs: Value,
    pub scope_plan: ScopePlan,
    pub input_fingerprint: String,
}

impl ResolvedCheckPlan {
    /// Record additional DB-provenance widening only when the computed plan is
    /// already wide. This keeps the plan and its fingerprint in sync.
    pub fn add_widening_reason(&mut self, reason: &str) -> Result<()> {
        if self.scope_plan.mode != ScopeMode::Wide || reason.trim().is_empty() {
            return Err(Error::new(
                "CHECK_SCOPE_PLAN",
                "an external widening reason requires an already-wide resolved plan",
            ));
        }
        if !self
            .scope_plan
            .widening_reasons
            .iter()
            .any(|existing| existing == reason)
        {
            self.scope_plan.widening_reasons.push(reason.to_owned());
            self.scope_plan.widening_reasons.sort();
        }
        self.resolved_inputs["scope_plan"] = serde_json::to_value(&self.scope_plan)?;
        self.input_fingerprint = fingerprint(&self.resolved_inputs, &self.scope_plan)?;
        self.resolved_inputs["input_fingerprint"] = json!(self.input_fingerprint);
        Ok(())
    }
}

/// Environment values passed to a CheckRunner process. The same builder is used
/// during planning and again by the worker before command spawn.
pub(crate) fn effective_environment(profile: &CheckProfile) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for name in builtin_environment_names()
        .iter()
        .copied()
        .chain(profile.inherit_env.iter().map(String::as_str))
    {
        if let Some((key, value)) = std::env::vars().find(|(key, _)| env_key_eq(key, name)) {
            values.insert(key, value);
        }
    }
    for (key, value) in &profile.environment {
        if cfg!(windows) {
            values.retain(|existing, _| !existing.eq_ignore_ascii_case(key));
        }
        values.insert(key.clone(), value.clone());
    }
    values
}

// Keep defaults target-specific: an absent platform-only variable is still a
// configured name and would otherwise be treated as opaque on this host.
#[cfg(windows)]
fn builtin_environment_names() -> &'static [&'static str] {
    &[
        "PATH",
        "SystemRoot",
        "WINDIR",
        "USERPROFILE",
        "HOME",
        "LOCALAPPDATA",
        "APPDATA",
        "TEMP",
        "TMP",
        "TMPDIR",
        "RUSTUP_HOME",
        "CARGO_HOME",
    ]
}

#[cfg(not(windows))]
fn builtin_environment_names() -> &'static [&'static str] {
    &[
        "PATH",
        "HOME",
        "TEMP",
        "TMP",
        "TMPDIR",
        "RUSTUP_HOME",
        "CARGO_HOME",
    ]
}

fn env_key_eq(left: &str, right: &str) -> bool {
    if cfg!(windows) {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

fn normalized_env_name(name: &str) -> String {
    if cfg!(windows) {
        name.to_ascii_uppercase()
    } else {
        name.to_string()
    }
}

pub(crate) fn environment_identity(
    profile: &CheckProfile,
    candidate: &VerifiedSource,
) -> Result<Value> {
    let values = effective_environment(profile);
    let (nonsecret_value_sha256, opaque_names, configured_names) =
        environment_values_identity(profile, &values);
    let present_names: BTreeSet<_> = values
        .keys()
        .map(|name| normalized_env_name(name))
        .collect();
    let descriptor_sha256 = source::content_descriptor_sha256(candidate)?;
    let profile_sha256 = profile_identity_sha256(profile)?;
    let external_cargo_config_unverified = profile.parser == Parser::CargoJson
        && has_unversioned_cargo_config(candidate, profile, &values);
    Ok(json!({
        "configured_names":configured_names,
        "present_names":present_names,
        "nonsecret_value_sha256":nonsecret_value_sha256,
        "opaque_names":opaque_names,
        "external_cargo_config_unverified":external_cargo_config_unverified,
        // These controller values are set only in the worker. Their identities
        // are semantic and content-only; no capture/check path is hashed.
        "controller_inputs":{
            "cargo_target_resource":profile.resource.to_ascii_lowercase(),
            "profile_identity_sha256":profile_sha256,
            "candidate_content_sha256":candidate.content_sha256,
            "candidate_descriptor_sha256":descriptor_sha256,
        },
    }))
}

fn config_path_exists_or_unknown(path: &Path) -> bool {
    match fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
}

fn config_matches_capture(candidate: &VerifiedSource, captured_root: &Path, path: &Path) -> bool {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return false;
    }
    let Ok(relative) = path.strip_prefix(captured_root) else {
        return false;
    };
    let Some(relative) = relative.to_str() else {
        return false;
    };
    if !candidate
        .manifest
        .files
        .iter()
        .any(|file| file.path == relative.replace('\\', "/"))
    {
        return false;
    }
    let Ok(expected) = fs::read(candidate.directory.join(relative)) else {
        return false;
    };
    fs::read(path).is_ok_and(|actual| actual == expected)
}

fn captured_config_has_external_include(candidate: &VerifiedSource) -> bool {
    candidate.manifest.files.iter().any(|file| {
        if !matches!(file.path.as_str(), ".cargo/config" | ".cargo/config.toml") {
            return false;
        }
        let config = candidate.directory.join(&file.path);
        let Ok(bytes) = fs::read(config) else {
            return true;
        };
        let Ok(text) = std::str::from_utf8(&bytes) else {
            return true;
        };
        let Ok(parsed) = toml::from_str::<toml::Value>(text) else {
            return true;
        };
        // Cargo's `include` can load files outside the immutable capture.
        // Until those paths have their own versioned identity, treat them as
        // unknown even when the root config itself is captured.
        parsed.get("include").is_some()
    })
}

fn has_unversioned_cargo_config(
    candidate: &VerifiedSource,
    profile: &CheckProfile,
    env: &BTreeMap<String, String>,
) -> bool {
    if captured_config_has_external_include(candidate) {
        return true;
    }
    let Ok(source_root) = fs::canonicalize(&candidate.directory) else {
        return true;
    };
    let Some(data_root) = source_root.parent().and_then(Path::parent) else {
        return true;
    };
    let Ok((execution_root, _)) = execution_paths(data_root, profile, candidate) else {
        return true;
    };
    for captured_root in [source_root, execution_root] {
        let mut directory = Some(captured_root.clone());
        while let Some(current) = directory {
            for name in ["config.toml", "config"] {
                let path = current.join(".cargo").join(name);
                if !config_path_exists_or_unknown(&path) {
                    continue;
                }
                // Config files in the verified source or its content workspace
                // are trusted only when they match the exact captured file.
                // Ancestor or stale workspace files are unversioned host state.
                if !config_matches_capture(candidate, &captured_root, &path) {
                    return true;
                }
            }
            directory = current.parent().map(Path::to_path_buf);
        }
    }

    let configured_cargo_home = env
        .iter()
        .find(|(name, _)| env_key_eq(name, "CARGO_HOME"))
        .map(|(_, value)| value);
    let cargo_homes: Vec<PathBuf> = if let Some(value) = configured_cargo_home {
        if value.is_empty() {
            return true;
        }
        let path = PathBuf::from(value);
        if !path.is_absolute() {
            return true;
        }
        vec![path]
    } else {
        let mut homes = Vec::new();
        for name in ["HOME", "USERPROFILE"] {
            if let Some((_, value)) = env.iter().find(|(key, _)| env_key_eq(key, name)) {
                if value.is_empty() {
                    return true;
                }
                let path = PathBuf::from(value);
                if !path.is_absolute() {
                    return true;
                }
                homes.push(path.join(".cargo"));
            }
        }
        homes
    };
    if cargo_homes.is_empty() {
        return true;
    }
    cargo_homes.iter().any(|cargo_home| {
        ["config.toml", "config"]
            .iter()
            .any(|name| config_path_exists_or_unknown(&cargo_home.join(name)))
    })
}

fn builtin_nonsecret_environment(name: &str) -> bool {
    matches!(
        normalized_env_name(name).as_str(),
        "PATH"
            | "SYSTEMROOT"
            | "WINDIR"
            | "USERPROFILE"
            | "HOME"
            | "LOCALAPPDATA"
            | "APPDATA"
            | "TEMP"
            | "TMP"
            | "TMPDIR"
            | "RUSTUP_HOME"
            | "CARGO_HOME"
            | "CARGO_TARGET_DIR"
    )
}

fn environment_values_identity(
    profile: &CheckProfile,
    values: &BTreeMap<String, String>,
) -> (BTreeMap<String, Value>, Vec<String>, Vec<String>) {
    let mut configured: BTreeSet<String> = builtin_environment_names()
        .iter()
        .map(|name| normalized_env_name(name))
        .collect();
    configured.extend(
        profile
            .inherit_env
            .iter()
            .map(|name| normalized_env_name(name)),
    );
    configured.extend(
        profile
            .environment
            .keys()
            .map(|name| normalized_env_name(name)),
    );

    let declared_safe: BTreeSet<_> = profile
        .fingerprint_env
        .iter()
        .map(|name| normalized_env_name(name))
        .collect();
    let mut nonsecret_value_sha256 = BTreeMap::new();
    let mut opaque_names = Vec::new();
    for name in &configured {
        let safe = builtin_nonsecret_environment(name) || declared_safe.contains(name);
        let value = values
            .iter()
            .find(|(actual, _)| normalized_env_name(actual) == *name)
            .map(|(_, value)| value);
        if safe {
            nonsecret_value_sha256.insert(
                name.clone(),
                value.map_or(Value::Null, |value| json!(model::digest(value.as_bytes()))),
            );
        } else {
            opaque_names.push(name.clone());
        }
    }
    (
        nonsecret_value_sha256,
        opaque_names,
        configured.into_iter().collect(),
    )
}

/// A redacted profile identity for Store freshness and result validation. It
/// hashes only values explicitly classified as non-secret; opaque values are
/// represented by their names and always disable completed-result reuse.
pub(crate) fn profile_identity(profile: &CheckProfile) -> Result<Value> {
    let values = effective_environment(profile);
    let (nonsecret_value_sha256, opaque_names, configured_names) =
        environment_values_identity(profile, &values);
    let versioned_external_inputs: BTreeMap<_, _> = profile
        .versioned_inputs
        .iter()
        .map(|(name, value)| (name.clone(), model::digest(value.as_bytes())))
        .collect();
    let identity = json!({
        "version":1,
        "profile_id":profile.profile_id,
        "profile_revision":profile.profile_revision,
        "executable":profile.executable,
        "args":profile.args,
        "parser":profile.parser,
        "resource":profile.resource,
        "expected_targets":sorted_unique(&profile.expected_targets),
        "reproducible":profile.reproducible,
        "configured_environment_names":configured_names,
        "fingerprint_env":profile.fingerprint_env.iter().map(|name|normalized_env_name(name)).collect::<BTreeSet<_>>(),
        "nonsecret_environment_sha256":nonsecret_value_sha256,
        "opaque_environment_names":opaque_names,
        "versioned_external_inputs":versioned_external_inputs,
    });
    Ok(identity)
}

pub(crate) fn profile_identity_sha256(profile: &CheckProfile) -> Result<String> {
    Ok(model::digest(
        model::canonical(&profile_identity(profile)?)?.as_bytes(),
    ))
}

/// Stable working and candidate-descriptor paths. The directory is private to
/// this CheckRunner resource/profile/content tuple, so `current_dir()` and the
/// injected candidate path do not expose an artifact, Attempt or CheckRun ID.
pub(crate) fn execution_paths(
    data_dir: &Path,
    profile: &CheckProfile,
    candidate: &VerifiedSource,
) -> Result<(PathBuf, PathBuf)> {
    if candidate.content_sha256.len() != 64
        || !candidate
            .content_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::new(
            "CHECK_INPUTS_STALE",
            "candidate content identity is malformed",
        ));
    }
    let profile_sha256 = profile_identity_sha256(profile)?;
    let base = data_dir
        .join("check-inputs")
        .join(profile.resource.to_ascii_lowercase())
        .join(profile_sha256);
    let workspace = base.join(&candidate.content_sha256);
    let descriptor = base.join(format!("{}.candidate.json", candidate.content_sha256));
    Ok((workspace, descriptor))
}

pub(crate) fn execution_workspace_identity(
    profile: &CheckProfile,
    candidate: &VerifiedSource,
) -> Result<Value> {
    Ok(json!({
        "version":1,
        "resource":profile.resource.to_ascii_lowercase(),
        "profile_identity_sha256":profile_identity_sha256(profile)?,
        "candidate_content_sha256":candidate.content_sha256,
        "candidate_descriptor_sha256":source::content_descriptor_sha256(candidate)?,
    }))
}

pub(crate) fn verify_runtime_environment(
    profile: &CheckProfile,
    candidate: &VerifiedSource,
    expected: &Value,
) -> Result<()> {
    let actual = environment_identity(profile, candidate)?;
    if &actual != expected {
        return Err(Error::new(
            "CHECK_INPUTS_STALE",
            "effective inherited environment changed after input resolution",
        ));
    }
    Ok(())
}

pub(crate) fn verify_executable(resolved_inputs: &Value) -> Result<PathBuf> {
    let path = resolved_inputs["executable"]["path"]
        .as_str()
        .ok_or_else(|| Error::new("CHECK_INPUTS_STALE", "resolved executable path is missing"))?;
    let expected = resolved_inputs["executable"]["sha256"]
        .as_str()
        .ok_or_else(|| {
            Error::new(
                "CHECK_INPUTS_STALE",
                "resolved executable digest is missing",
            )
        })?;
    let path = PathBuf::from(path);
    if !path.is_file() || sha256_file(&path)? != expected {
        return Err(Error::new(
            "CHECK_INPUTS_STALE",
            "configured check executable changed after input resolution",
        ));
    }
    Ok(path)
}

fn executable(program: &Path, env: &BTreeMap<String, String>) -> Result<PathBuf> {
    if program.is_absolute() {
        if program.is_file() {
            return std::fs::canonicalize(program).map_err(Into::into);
        }
        return Err(Error::new(
            "CHECK_EXECUTABLE_MISSING",
            program.display().to_string(),
        ));
    }
    if program.components().count() != 1 {
        return Err(Error::invalid(
            "check executable must be absolute or a PATH program name",
        ));
    }
    let path = env
        .iter()
        .find(|(key, _)| env_key_eq(key, "PATH"))
        .map(|(_, value)| value)
        .ok_or_else(|| Error::new("CHECK_EXECUTABLE_MISSING", "PATH is unavailable"))?;
    for directory in std::env::split_paths(path) {
        if !directory.is_absolute() {
            continue;
        }
        let path = directory.join(program);
        #[cfg(windows)]
        let path = if path.extension().is_none() {
            path.with_extension("exe")
        } else {
            path
        };
        if path.is_file() {
            return std::fs::canonicalize(path).map_err(Into::into);
        }
    }
    Err(Error::new(
        "CHECK_EXECUTABLE_MISSING",
        program.display().to_string(),
    ))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn prefix_args(args: &[String]) -> Vec<String> {
    args.iter()
        .take_while(|arg| arg.starts_with('+'))
        .cloned()
        .collect()
}

fn command_output(
    program: &Path,
    args: &[String],
    env: &BTreeMap<String, String>,
    cwd: &Path,
) -> Result<Vec<u8>> {
    let current_exe = std::env::current_exe()?;
    let request = ProbeRequest {
        probe_version: PROBE_VERSION,
        program: program.to_path_buf(),
        args: args.to_vec(),
        cwd: cwd.to_path_buf(),
        timeout_ms: PROBE_TIMEOUT_MS,
        stdout_limit: PROBE_STDOUT_LIMIT,
        stderr_limit: PROBE_STDERR_LIMIT,
    };
    let temp_root = std::env::temp_dir();
    let temp_dir = temp_root.join(format!("swarm-check-probe-{}", model::new_id()));
    fs::create_dir(&temp_dir)?;
    let mut temp_guard = TempProbeDir(temp_dir.clone(), false);
    let request_path = temp_guard.0.join("request.json");
    let request_bytes = model::canonical(&json!({"check_probe":request}))?.into_bytes();
    let mut request_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&request_path)?;
    request_file.write_all(&request_bytes)?;
    request_file.sync_all()?;
    drop(request_file);

    (|| -> Result<Vec<u8>> {
        let mut helper = Command::new(current_exe);
        helper
            .arg("check-worker")
            .arg("--file")
            .arg(&request_path)
            .env_clear()
            .envs(env)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            // This pipe-only probe never needs a Windows console.
            helper.creation_flags(windows_sys::Win32::System::Threading::DETACHED_PROCESS);
        }
        let mut child = helper.spawn()?;
        temp_guard.preserve();
        let helper_process = spawned_identity(child.id()).ok();
        let Some(stdout) = child.stdout.take() else {
            let _ = stop_probe_helper(&mut child);
            return Err(Error::new(
                "CHECK_INPUT_RESOLUTION",
                format!(
                    "probe response pipe missing{}",
                    probe_owner_diagnostic(
                        &temp_guard.0.join("probe-owner.json"),
                        helper_process.as_ref()
                    )
                ),
            ));
        };
        let reader = match thread::Builder::new()
            .name("check-probe-response".into())
            .spawn(move || read_bounded(stdout, PROBE_RESPONSE_LIMIT))
        {
            Ok(reader) => reader,
            Err(_) => {
                let _ = stop_probe_helper(&mut child);
                return Err(Error::new(
                    "CHECK_INPUT_RESOLUTION",
                    format!(
                        "cannot start bounded input probe response reader{}",
                        probe_owner_diagnostic(
                            &temp_guard.0.join("probe-owner.json"),
                            helper_process.as_ref()
                        )
                    ),
                ));
            }
        };
        let Some(status) = wait_owned_child(&mut child, PROBE_HELPER_WAIT_GRACE) else {
            let _ = stop_probe_helper(&mut child);
            return Err(Error::new(
                "CHECK_PROBE_CLEANUP_PENDING",
                format!(
                    "input probe helper did not exit within its bounded wait{}",
                    probe_owner_diagnostic(
                        &temp_guard.0.join("probe-owner.json"),
                        helper_process.as_ref()
                    )
                ),
            ));
        };
        let reader_deadline = Instant::now() + PROBE_CAPTURE_DRAIN_GRACE;
        while !reader.is_finished() && Instant::now() < reader_deadline {
            thread::sleep(Duration::from_millis(10));
        }
        if !reader.is_finished() {
            drop(reader);
            return Err(Error::new(
                "CHECK_PROBE_CAPTURE_PENDING",
                format!(
                    "input probe response capture did not finish within its bound{}",
                    probe_owner_diagnostic(
                        &temp_guard.0.join("probe-owner.json"),
                        helper_process.as_ref()
                    )
                ),
            ));
        }
        let response = reader.join().map_err(|_| {
            Error::new("CHECK_INPUT_RESOLUTION", "probe response reader panicked")
        })??;
        if !status.success() {
            return Err(Error::new(
                "CHECK_INPUT_RESOLUTION",
                format!(
                    "owned input probe helper exited without a response{}",
                    probe_owner_diagnostic(
                        &temp_guard.0.join("probe-owner.json"),
                        helper_process.as_ref()
                    )
                ),
            ));
        }
        let response = decode_probe_response(&response)?;
        if !response.group_empty {
            retain_probe_partial(&temp_guard.0, "stdout.partial", &response.stdout)?;
            retain_probe_partial(&temp_guard.0, "stderr.partial", &response.stderr)?;
            return Err(Error::new(
                "CHECK_PROBE_CLEANUP_PENDING",
                format!(
                    "input probe did not prove its process group empty; {}; partial_stdout_bytes={}; partial_stderr_bytes={}{}",
                    response.message,
                    response.stdout.len(),
                    response.stderr.len(),
                    probe_owner_diagnostic(
                        &temp_guard.0.join("probe-owner.json"),
                        helper_process.as_ref()
                    )
                ),
            ));
        }
        // The helper wrote this response only after exact group-empty and
        // successful disarm. Only now may the request and owner evidence go.
        temp_guard.allow_cleanup();
        if !response.success {
            let reason = if response.timed_out {
                "input metadata/version probe exceeded its deadline".to_string()
            } else if response.output_limited {
                "input metadata/version probe exceeded its streaming output limit".to_string()
            } else if !response.message.is_empty() {
                response.message.clone()
            } else {
                format!("input probe exited with {:?}", response.exit_code)
            };
            let detail = String::from_utf8_lossy(&response.stderr)
                .chars()
                .take(1500)
                .collect::<String>();
            return Err(Error::new(
                "CHECK_INPUT_RESOLUTION",
                if detail.is_empty() {
                    reason
                } else {
                    format!("{reason}: {detail}")
                },
            ));
        }
        Ok(response.stdout)
    })()
}

fn retain_probe_partial(directory: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    let path = directory.join(name);
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn wait_owned_child(
    child: &mut std::process::Child,
    grace: Duration,
) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + grace;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

fn stop_probe_helper(child: &mut std::process::Child) -> Option<std::process::ExitStatus> {
    if let Some(status) = wait_owned_child(child, Duration::ZERO) {
        return Some(status);
    }
    let _ = child.kill();
    wait_owned_child(child, PROBE_CLEANUP_GRACE)
}

fn probe_owner_diagnostic(path: &Path, helper_process: Option<&Value>) -> String {
    let inner_process = fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    let mut facts = serde_json::Map::new();
    if let Some(process) = helper_process {
        facts.insert("helper_direct_process".into(), process.clone());
    }
    if let Some(process) = inner_process {
        facts.insert("probe_process_group".into(), process);
    }
    if facts.is_empty() {
        String::new()
    } else {
        format!("; owner_evidence={}", Value::Object(facts))
    }
}

fn read_bounded(mut reader: impl Read, limit: usize) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(limit.min(64 * 1024));
    let mut buffer = [0u8; 8192];
    let mut exceeded = false;
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        if !exceeded {
            if result.len().saturating_add(count) > limit {
                exceeded = true;
            } else {
                result.extend_from_slice(&buffer[..count]);
            }
        }
        // Keep draining even after the response cap so the helper cannot block.
    }
    if exceeded {
        return Err(Error::new(
            "CHECK_INPUT_RESOLUTION",
            "input probe response exceeded its frame limit",
        ));
    }
    Ok(result)
}

fn encode_probe_response(response: &ProbeResponse, mut writer: impl Write) -> Result<()> {
    let message = response.message.as_bytes();
    if response.stdout.len() > PROBE_STDOUT_LIMIT
        || response.stderr.len() > PROBE_STDERR_LIMIT
        || message.len() > 1024
    {
        return Err(Error::new(
            "CHECK_INPUT_RESOLUTION",
            "internal probe response exceeded its protocol limit",
        ));
    }
    let flags = u8::from(response.success)
        | (u8::from(response.timed_out) << 1)
        | (u8::from(response.output_limited) << 2)
        | (u8::from(response.group_empty) << 3);
    writer.write_all(PROBE_MAGIC)?;
    writer.write_all(&[flags])?;
    writer.write_all(&response.exit_code.unwrap_or(-1).to_le_bytes())?;
    writer.write_all(&(response.stdout.len() as u32).to_le_bytes())?;
    writer.write_all(&(response.stderr.len() as u32).to_le_bytes())?;
    writer.write_all(&(message.len() as u16).to_le_bytes())?;
    writer.write_all(&response.stdout)?;
    writer.write_all(&response.stderr)?;
    writer.write_all(message)?;
    writer.flush()?;
    Ok(())
}

fn decode_probe_response(bytes: &[u8]) -> Result<ProbeResponse> {
    const HEADER_LEN: usize = 8 + 1 + 4 + 4 + 4 + 2;
    if bytes.len() < HEADER_LEN || &bytes[..8] != PROBE_MAGIC {
        return Err(Error::new(
            "CHECK_INPUT_RESOLUTION",
            "owned input probe returned an invalid response frame",
        ));
    }
    let flags = bytes[8];
    let exit_code = i32::from_le_bytes(bytes[9..13].try_into().unwrap());
    let stdout_len = u32::from_le_bytes(bytes[13..17].try_into().unwrap()) as usize;
    let stderr_len = u32::from_le_bytes(bytes[17..21].try_into().unwrap()) as usize;
    let message_len = u16::from_le_bytes(bytes[21..23].try_into().unwrap()) as usize;
    let expected = HEADER_LEN
        .checked_add(stdout_len)
        .and_then(|len| len.checked_add(stderr_len))
        .and_then(|len| len.checked_add(message_len))
        .ok_or_else(|| Error::invalid("input probe response length overflow"))?;
    if stdout_len > PROBE_STDOUT_LIMIT
        || stderr_len > PROBE_STDERR_LIMIT
        || message_len > 1024
        || expected != bytes.len()
    {
        return Err(Error::new(
            "CHECK_INPUT_RESOLUTION",
            "owned input probe returned an invalid response length",
        ));
    }
    let stdout_start = HEADER_LEN;
    let stderr_start = stdout_start + stdout_len;
    let message_start = stderr_start + stderr_len;
    Ok(ProbeResponse {
        success: flags & 1 != 0,
        timed_out: flags & 2 != 0,
        output_limited: flags & 4 != 0,
        group_empty: flags & 8 != 0,
        exit_code: (exit_code >= 0).then_some(exit_code),
        stdout: bytes[stdout_start..stderr_start].to_vec(),
        stderr: bytes[stderr_start..message_start].to_vec(),
        message: String::from_utf8_lossy(&bytes[message_start..]).into_owned(),
    })
}

fn read_limited_pipe(
    mut pipe: impl Read,
    limit: usize,
    overflow: Arc<AtomicBool>,
    capture: Arc<Mutex<ProbeCapture>>,
) {
    let mut buffer = [0u8; 8192];
    loop {
        let count = match pipe.read(&mut buffer) {
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => {
                probe_capture_lock(&capture).error = Some("capture_read_failed".into());
                return;
            }
        };
        if count == 0 {
            return;
        }
        let mut output = probe_capture_lock(&capture);
        let room = limit.saturating_sub(output.bytes.len());
        let keep = room.min(count);
        output.bytes.extend_from_slice(&buffer[..keep]);
        if keep != count {
            overflow.store(true, Ordering::Release);
        }
        drop(output);
        // Continue draining after the cap so a child cannot block on a full pipe.
    }
}

#[derive(Debug, Default, Clone)]
struct ProbeCapture {
    bytes: Vec<u8>,
    error: Option<String>,
}

fn probe_capture_lock(capture: &Mutex<ProbeCapture>) -> std::sync::MutexGuard<'_, ProbeCapture> {
    capture
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn finish_probe_reader(reader: thread::JoinHandle<()>, grace: Duration) -> bool {
    let deadline = Instant::now() + grace;
    while !reader.is_finished() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    if !reader.is_finished() {
        drop(reader);
        return false;
    }
    reader.join().is_ok()
}

struct ProbeOwner {
    group: Group,
}

impl ProbeOwner {
    fn release(&mut self) -> Result<()> {
        let deadline = Instant::now() + PROBE_CLEANUP_GRACE;
        let mut requests = 0u8;
        let mut last_request = None;
        let mut last_error = None;
        loop {
            match self.group.children_empty() {
                Ok(true) => match self.group.disarm() {
                    Ok(()) => {
                        return Ok(());
                    }
                    Err(error) => last_error = Some(cleanup_error_diagnostic(&error)),
                },
                Ok(false) => {}
                Err(error) => last_error = Some(cleanup_error_diagnostic(&error)),
            }
            if requests < PROBE_MAX_CLEANUP_REQUESTS
                && last_request.is_none_or(|last: Instant| last.elapsed() >= PROBE_CLEANUP_RETRY)
            {
                requests += 1;
                last_request = Some(Instant::now());
                if let Err(error) = self.group.cancel_children() {
                    last_error = Some(cleanup_error_diagnostic(&error));
                }
            }
            if Instant::now() >= deadline {
                let process = serde_json::to_string(&self.group.identity)
                    .unwrap_or_else(|_| "unavailable".into());
                return Err(Error::new(
                    "CHECK_PROBE_CLEANUP_PENDING",
                    format!(
                        "input probe process group departure is unconfirmed; owner={process}; {}",
                        last_error.unwrap_or_else(|| "no terminal observation".into())
                    ),
                ));
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

fn cleanup_error_diagnostic(error: &Error) -> String {
    let code = error
        .code
        .chars()
        .take(64)
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    let message = if error.message.contains('/')
        || error.message.contains('\\')
        || error
            .message
            .as_bytes()
            .windows(2)
            .any(|pair| pair[0].is_ascii_alphabetic() && pair[1] == b':')
    {
        "OS diagnostic redacted"
    } else {
        error.message.as_str()
    };
    let message = message
        .chars()
        .filter(|character| character.is_ascii() && !character.is_ascii_control())
        .take(512)
        .collect::<String>();
    let code = if code.is_empty() {
        "UNKNOWN".to_string()
    } else {
        code
    };
    format!("cleanup_error[{code}]: {message}")
}

/// Internal `check-worker` protocol route. The process group is owned by this
/// helper before it spawns Cargo/rustup/version probes; only bounded pipe data
/// crosses back to the resolver.
pub(crate) fn run_probe(file: &Path) -> Result<()> {
    let request_value = read_bounded(File::open(file)?, PROBE_REQUEST_LIMIT)?;
    let parsed = serde_json::from_slice::<Value>(&request_value)
        .ok()
        .and_then(|value| {
            serde_json::from_value::<ProbeRequest>(value["check_probe"].clone()).ok()
        });
    let response = match parsed {
        Some(request) => execute_probe(request, &file.with_file_name("probe-owner.json")),
        None => ProbeResponse {
            success: false,
            timed_out: false,
            output_limited: false,
            exit_code: None,
            group_empty: false,
            message: "invalid internal CheckRunner probe request".into(),
            stdout: Vec::new(),
            stderr: Vec::new(),
        },
    };
    encode_probe_response(&response, std::io::stdout().lock())
}

fn execute_probe(request: ProbeRequest, owner_path: &Path) -> ProbeResponse {
    let invalid = |message: &str| ProbeResponse {
        success: false,
        timed_out: false,
        output_limited: false,
        exit_code: None,
        group_empty: false,
        message: message.into(),
        stdout: Vec::new(),
        stderr: Vec::new(),
    };
    if request.probe_version != PROBE_VERSION
        || !request.program.is_absolute()
        || !request.cwd.is_absolute()
        || request.args.iter().any(|arg| arg.contains('\0'))
        || request.timeout_ms == 0
        || request.timeout_ms > PROBE_TIMEOUT_MS
        || request.stdout_limit == 0
        || request.stdout_limit > PROBE_STDOUT_LIMIT
        || request.stderr_limit == 0
        || request.stderr_limit > PROBE_STDERR_LIMIT
    {
        return invalid("invalid internal CheckRunner probe bounds or path");
    }
    let group = match Group::enter(&model::new_id()) {
        Ok(group) => group,
        Err(error) => return invalid(&format!("cannot own input probe process group: {error}")),
    };
    let mut owner = ProbeOwner { group };
    if let Err(error) = persist_probe_owner(owner_path, &owner.group.identity) {
        let cleanup = owner.release();
        return ProbeResponse {
            success: false,
            timed_out: false,
            output_limited: false,
            exit_code: None,
            group_empty: cleanup.is_ok(),
            message: format!("cannot persist input probe process identity: {error}"),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
    }
    let mut command = Command::new(&request.program);
    command
        .args(&request.args)
        .current_dir(&request.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // A pipe-only input resolver must not allocate console infrastructure
        // that can outlive its direct process and enter the owned Job.
        command.creation_flags(windows_sys::Win32::System::Threading::DETACHED_PROCESS);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let cleanup = owner.release();
            return ProbeResponse {
                success: false,
                timed_out: false,
                output_limited: false,
                exit_code: None,
                group_empty: cleanup.is_ok(),
                message: format!("unable to start input probe: {error}"),
                stdout: Vec::new(),
                stderr: Vec::new(),
            };
        }
    };
    let overflow = Arc::new(AtomicBool::new(false));
    let stdout_capture = Arc::new(Mutex::new(ProbeCapture::default()));
    let stderr_capture = Arc::new(Mutex::new(ProbeCapture::default()));
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_reader = stdout.and_then(|pipe| {
        let overflow = Arc::clone(&overflow);
        let capture = Arc::clone(&stdout_capture);
        thread::Builder::new()
            .name("check-probe-stdout".into())
            .spawn(move || read_limited_pipe(pipe, request.stdout_limit, overflow, capture))
            .ok()
    });
    let stderr_reader = stderr.and_then(|pipe| {
        let overflow = Arc::clone(&overflow);
        let capture = Arc::clone(&stderr_capture);
        thread::Builder::new()
            .name("check-probe-stderr".into())
            .spawn(move || read_limited_pipe(pipe, request.stderr_limit, overflow, capture))
            .ok()
    });
    if stdout_reader.is_none() || stderr_reader.is_none() {
        drop(child);
        let cleanup = owner.release();
        if let Some(reader) = stdout_reader {
            let _ = finish_probe_reader(reader, PROBE_CAPTURE_DRAIN_GRACE);
        }
        if let Some(reader) = stderr_reader {
            let _ = finish_probe_reader(reader, PROBE_CAPTURE_DRAIN_GRACE);
        }
        return ProbeResponse {
            success: false,
            timed_out: false,
            output_limited: false,
            exit_code: None,
            group_empty: cleanup.is_ok(),
            message: cleanup.err().map_or_else(
                || "cannot start bounded input probe readers".into(),
                |error| format!("cannot start bounded input probe readers; {error}"),
            ),
            stdout: probe_capture_lock(&stdout_capture).bytes.clone(),
            stderr: probe_capture_lock(&stderr_capture).bytes.clone(),
        };
    }
    let stdout_reader = stdout_reader.unwrap();
    let stderr_reader = stderr_reader.unwrap();

    let deadline = Instant::now() + Duration::from_millis(request.timeout_ms);
    let mut timed_out = false;
    let mut output_limited = false;
    let mut exit_status = None;
    loop {
        if overflow.load(Ordering::Acquire) {
            output_limited = true;
            let _ = owner.group.cancel_children();
            break;
        }
        match child.try_wait() {
            Ok(Some(status)) => {
                exit_status = Some(status);
                break;
            }
            Ok(None) => {}
            Err(_) => {
                let _ = owner.group.cancel_children();
                break;
            }
        }
        if Instant::now() >= deadline {
            timed_out = true;
            let _ = owner.group.cancel_children();
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    // On Windows, Job accounting may still include the just-finished direct
    // child while its process handle is retained. Drop it before checking for
    // actual remaining Job members; the Job owner remains live throughout.
    drop(child);
    // Check and clear the owned process group before joining the pipe readers.
    // A child can outlive the command while retaining an inherited pipe; joining
    // first would then wait for EOF and could miss that child after it exits.
    let (had_descendants, membership_diagnostic) = match owner.group.children_empty() {
        Ok(empty) => (!empty, None),
        Err(error) => (false, Some(cleanup_error_diagnostic(&error))),
    };
    let cleanup = owner.release();
    let group_empty = cleanup.is_ok();
    let cleanup_diagnostic = cleanup.as_ref().err().map(cleanup_error_diagnostic);
    let stdout_reader_ok = finish_probe_reader(stdout_reader, PROBE_CAPTURE_DRAIN_GRACE);
    let stderr_reader_ok = finish_probe_reader(stderr_reader, PROBE_CAPTURE_DRAIN_GRACE);
    let stdout_state = probe_capture_lock(&stdout_capture).clone();
    let stderr_state = probe_capture_lock(&stderr_capture).clone();
    let reader_errors = [stdout_state.error.as_deref(), stderr_state.error.as_deref()]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let readers_ok = stdout_reader_ok && stderr_reader_ok && reader_errors.is_empty();
    let stdout = stdout_state.bytes;
    let stderr = stderr_state.bytes;
    let exit_code = exit_status.as_ref().and_then(|status| status.code());
    let child_succeeded = exit_status.as_ref().is_some_and(|status| status.success());
    let membership_failed = membership_diagnostic.is_some();
    let success = group_empty
        && !timed_out
        && !output_limited
        && !had_descendants
        && !membership_failed
        && readers_ok
        && child_succeeded;
    ProbeResponse {
        success,
        timed_out,
        output_limited,
        exit_code,
        group_empty,
        message: match (membership_diagnostic, cleanup_diagnostic, had_descendants) {
            (Some(membership), Some(cleanup), _) => {
                format!(
                    "input probe process group membership check failed; {membership}; {cleanup}"
                )
            }
            (Some(membership), None, _) => {
                format!("input probe process group membership check failed; {membership}")
            }
            (None, Some(diagnostic), true) => {
                format!("input probe left descendants after its command exited; {diagnostic}")
            }
            (None, Some(diagnostic), false) => {
                format!("input probe process group could not be released; {diagnostic}")
            }
            (None, None, true) => "input probe left descendants after its command exited".into(),
            (None, None, false) if !group_empty => {
                "input probe process group could not be released".into()
            }
            (None, None, false) if !readers_ok => format!(
                "input probe output reader did not finish cleanly within its bound: {}",
                reader_errors.join(",")
            ),
            _ => String::new(),
        },
        stdout,
        stderr,
    }
}

fn persist_probe_owner(path: &Path, process: &Value) -> Result<()> {
    let bytes = model::canonical(process)?.into_bytes();
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn version_probe(
    program: &Path,
    prefix: &[String],
    env: &BTreeMap<String, String>,
    cwd: &Path,
    probe: ProbeFn,
) -> Result<String> {
    let mut args = prefix.to_vec();
    args.push("--version".into());
    let output = probe(program, &args, env, cwd)?;
    Ok(String::from_utf8(output)
        .map_err(|_| Error::new("CHECK_INPUT_RESOLUTION", "tool version output is not UTF-8"))?
        .trim()
        .to_string())
}

fn rustc_identity(
    cargo_args: &[String],
    env: &BTreeMap<String, String>,
    cwd: &Path,
    probe: ProbeFn,
) -> Result<Value> {
    let explicit_cargo_toolchain = prefix_args(cargo_args);
    if let Some(rustc_value) = env
        .iter()
        .find(|(key, _)| env_key_eq(key, "RUSTC"))
        .map(|(_, value)| value)
    {
        let rustc = PathBuf::from(rustc_value);
        let rustc = executable(&rustc, env)?;
        let version = probe(&rustc, &["--version".into(), "--verbose".into()], env, cwd)?;
        return rustc_result(rustc, version);
    }
    if !explicit_cargo_toolchain.is_empty() {
        // Cargo +toolchain may select a different compiler than the `rustc` PATH
        // shim. Do not claim reusable identity unless rustup confirms that compiler.
        let rustup = executable(Path::new("rustup"), env)?;
        let toolchain = explicit_cargo_toolchain[0].trim_start_matches('+');
        let args = vec![
            "run".into(),
            toolchain.into(),
            "rustc".into(),
            "--version".into(),
            "--verbose".into(),
        ];
        let selected = probe(&rustup, &args, env, cwd)?;
        let args = vec![
            "which".into(),
            "--toolchain".into(),
            toolchain.into(),
            "rustc".into(),
        ];
        let path = probe(&rustup, &args, env, cwd)?;
        let path = String::from_utf8(path)
            .map_err(|_| Error::new("CHECK_INPUT_RESOLUTION", "rustup path output is not UTF-8"))?
            .trim()
            .to_string();
        let binary = std::fs::canonicalize(path)?;
        return Ok(json!({
            "binary_path":binary,
            "binary_sha256":sha256_file(&binary)?,
            "version":String::from_utf8(selected).map_err(|_| Error::new("CHECK_INPUT_RESOLUTION","rustc version output is not UTF-8"))?.trim(),
        }));
    }
    let rustc = executable(Path::new("rustc"), env)?;
    let version = probe(&rustc, &["--version".into(), "--verbose".into()], env, cwd)?;
    rustc_result(rustc, version)
}

fn rustc_result(rustc: PathBuf, version: Vec<u8>) -> Result<Value> {
    let version = String::from_utf8(version)
        .map_err(|_| {
            Error::new(
                "CHECK_INPUT_RESOLUTION",
                "rustc version output is not UTF-8",
            )
        })?
        .trim()
        .to_string();
    Ok(json!({
        "binary_path":rustc,
        "binary_sha256":sha256_file(&rustc)?,
        "version":version,
    }))
}

fn cargo_metadata(
    executable: &Path,
    profile: &CheckProfile,
    env: &BTreeMap<String, String>,
    source: &VerifiedSource,
    probe: ProbeFn,
) -> Result<CargoGraph> {
    let prefix = prefix_args(&profile.args);
    let mut args = prefix;
    args.extend([
        "metadata".into(),
        "--no-deps".into(),
        "--format-version".into(),
        "1".into(),
        "--locked".into(),
        "--offline".into(),
        "--manifest-path".into(),
        source
            .directory
            .join("Cargo.toml")
            .to_string_lossy()
            .to_string(),
    ]);
    let bytes = probe(executable, &args, env, &source.directory)?;
    let metadata: Value = serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("CHECK_METADATA", "Cargo metadata was not valid JSON"))?;
    CargoGraph::parse(&metadata, &source.directory)
}

fn is_cargo_executable(path: &Path) -> bool {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .is_some_and(|name| name.eq_ignore_ascii_case("cargo"))
}

fn has_flag(args: &[String], flag: &str) -> bool {
    args.iter()
        .take_while(|arg| arg.as_str() != "--")
        .any(|arg| arg == flag || arg.starts_with(&format!("{flag}=")))
}

fn replace_package_scope(args: &[String], plan: &ScopePlan) -> Vec<String> {
    let Some(command_index) = args.iter().position(|arg| !arg.starts_with('+')) else {
        return args.to_vec();
    };
    let (prefix, tail) = args.split_at(command_index + 1);
    let mut retained = Vec::new();
    let mut skip_next = false;
    let mut before_compiler = Vec::new();
    let mut compiler = Vec::new();
    let mut after_separator = false;
    for arg in tail {
        if after_separator {
            compiler.push(arg.clone());
            continue;
        }
        if arg == "--" {
            after_separator = true;
            continue;
        }
        if skip_next {
            skip_next = false;
            continue;
        }
        if arg == "--package" || arg == "--exclude" || arg == "-p" {
            skip_next = true;
            continue;
        }
        if arg == "--workspace"
            || arg == "--all"
            || arg.starts_with("--package=")
            || arg.starts_with("--exclude=")
            || (arg.starts_with("-p") && arg.len() > 2)
        {
            continue;
        }
        before_compiler.push(arg.clone());
    }
    match plan.mode {
        ScopeMode::Narrowed => {
            for package in &plan.selected_packages {
                before_compiler.push("--package".into());
                before_compiler.push(package.clone());
            }
        }
        ScopeMode::Wide => {
            before_compiler.push("--workspace".into());
        }
    }
    retained.extend_from_slice(prefix);
    retained.extend(before_compiler);
    if after_separator {
        retained.push("--".into());
        retained.extend(compiler);
    }
    retained
}

fn cargo_target(args: &[String]) -> Option<String> {
    for (index, arg) in args.iter().enumerate() {
        if arg == "--target" {
            return args.get(index + 1).cloned();
        }
        if let Some(target) = arg.strip_prefix("--target=") {
            return Some(target.into());
        }
    }
    None
}

fn sorted_unique(values: &[String]) -> Vec<String> {
    values
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn has_versioned_build_environment(profile: &CheckProfile) -> bool {
    profile
        .versioned_inputs
        .get("build_environment")
        .is_some_and(|identity| !identity.trim().is_empty())
}

fn fingerprint(resolved_inputs: &Value, scope_plan: &ScopePlan) -> Result<String> {
    let fingerprint_input = json!({
        "version": 1,
        "candidate_content_sha256": resolved_inputs["candidate_content_sha256"],
        "baseline_content_sha256": resolved_inputs["baseline_content_sha256"],
        "profile_id": resolved_inputs["profile_id"],
        "profile_revision": resolved_inputs["profile_revision"],
        "profile_sha256": resolved_inputs["profile_sha256"],
        "resolved_argv": resolved_inputs["argv"],
        "expected_targets": resolved_inputs["expected_targets"],
        "cargo_target": resolved_inputs["cargo_target"],
        "platform": resolved_inputs["platform"],
        "executable": resolved_inputs["executable"],
        "toolchain": resolved_inputs["toolchain"],
        "cargo_metadata": resolved_inputs["cargo_metadata"],
        "environment": resolved_inputs["environment"],
        "execution_workspace": resolved_inputs["execution_workspace"],
        "versioned_external_inputs": resolved_inputs["versioned_external_inputs"],
        "scope_plan": scope_plan,
    });
    Ok(model::digest(
        model::canonical(&fingerprint_input)?.as_bytes(),
    ))
}

/// Resolve source, Cargo reverse scope, actual argv, toolchain and environment
/// before Store admission. Artifact/Task/Attempt/Operation IDs never enter the
/// returned reusable identity.
pub fn resolve(
    profile: &CheckProfile,
    candidate: &VerifiedSource,
    baseline: Option<&VerifiedSource>,
) -> Result<ResolvedCheckPlan> {
    resolve_with_probe(profile, candidate, baseline, command_output)
}

fn resolve_with_probe(
    profile: &CheckProfile,
    candidate: &VerifiedSource,
    baseline: Option<&VerifiedSource>,
    probe: ProbeFn,
) -> Result<ResolvedCheckPlan> {
    let env = effective_environment(profile);
    let configured_executable = executable(&profile.executable, &env)?;
    let executable_sha256 = sha256_file(&configured_executable)?;
    let executable_version = version_probe(
        &configured_executable,
        &prefix_args(&profile.args),
        &env,
        &candidate.directory,
        probe,
    )
    .ok();

    let mut candidate_graph = None;
    let mut baseline_graph = None;
    let cargo_eligible =
        profile.parser == Parser::CargoJson && is_cargo_executable(&configured_executable);
    let environment = environment_identity(profile, candidate)?;
    let external_cargo_config_unverified = cargo_eligible
        && (environment["external_cargo_config_unverified"] == true
            || baseline.is_some_and(|source| has_unversioned_cargo_config(source, profile, &env)));
    if cargo_eligible && !external_cargo_config_unverified {
        candidate_graph =
            cargo_metadata(&configured_executable, profile, &env, candidate, probe).ok();
        if let Some(baseline) = baseline {
            baseline_graph =
                cargo_metadata(&configured_executable, profile, &env, baseline, probe).ok();
        }
    }
    let mut scope_plan = scope::analyze(
        candidate,
        baseline,
        candidate_graph.as_ref(),
        baseline_graph.as_ref(),
        &profile.expected_targets,
    );
    if external_cargo_config_unverified {
        scope_plan
            .widening_reasons
            .push("external_cargo_config_unverified".into());
        scope_plan.widening_reasons.sort();
    }
    let mut argv = profile.args.clone();
    if cargo_eligible {
        argv = replace_package_scope(&argv, &scope_plan);
    }
    let expected_targets = if scope_plan.mode == ScopeMode::Narrowed {
        scope_plan.selected_targets.clone()
    } else {
        sorted_unique(&profile.expected_targets)
    };
    let mut cache_disabled_reasons = BTreeSet::new();
    if !profile.reproducible {
        cache_disabled_reasons.insert("profile_not_reproducible".to_string());
    }
    if let Some(opaque_names) = environment["opaque_names"].as_array() {
        for name in opaque_names.iter().filter_map(Value::as_str) {
            cache_disabled_reasons.insert(format!("opaque_environment:{name}"));
        }
    }
    if executable_version.is_none() {
        cache_disabled_reasons.insert("executable_version_unavailable".to_string());
    }
    if !cargo_eligible && profile.parser == Parser::CargoJson {
        cache_disabled_reasons.insert("cargo_executable_unverified".to_string());
    }
    if cargo_eligible && !has_versioned_build_environment(profile) {
        cache_disabled_reasons.insert("cargo_external_tools_unversioned".to_string());
    }
    if external_cargo_config_unverified {
        cache_disabled_reasons.insert("external_cargo_config_unverified".into());
    }
    if profile.parser == Parser::CargoJson
        && !has_flag(&argv, "--offline")
        && !has_flag(&argv, "--frozen")
    {
        cache_disabled_reasons.insert("cargo_network_not_disabled".to_string());
    }
    if scope_plan.workspace_graph_sha256.is_none() && profile.parser == Parser::CargoJson {
        cache_disabled_reasons.insert("workspace_graph_unverified".to_string());
    }
    if profile.parser == Parser::CargoJson
        && scope_plan.widening_reasons.iter().any(|reason| {
            matches!(
                reason.as_str(),
                "dependency_graph_incomplete"
                    | "baseline_dependency_graph_incomplete"
                    | "baseline_metadata_unavailable"
                    | "cargo_metadata_unavailable"
            )
        })
    {
        cache_disabled_reasons.insert("workspace_graph_incomplete".to_string());
    }
    let rustc = if cargo_eligible {
        match rustc_identity(&profile.args, &env, &candidate.directory, probe) {
            Ok(identity) => Some(identity),
            Err(_) => {
                cache_disabled_reasons.insert("toolchain_unverified".to_string());
                None
            }
        }
    } else {
        None
    };
    let versioned_external_inputs: BTreeMap<_, _> = profile
        .versioned_inputs
        .iter()
        .map(|(name, value)| (name.clone(), model::digest(value.as_bytes())))
        .collect();
    let mut resolved_inputs = json!({
        "version": 1,
        "profile_id": profile.profile_id,
        "profile_revision": profile.profile_revision,
        "profile_sha256": profile_identity_sha256(profile)?,
        "profile_identity": profile_identity(profile)?,
        "reproducible": profile.reproducible,
        "cache_reusable": profile.reproducible && cache_disabled_reasons.is_empty(),
        "cache_disabled_reasons": cache_disabled_reasons,
        "candidate_content_sha256": candidate.content_sha256,
        "baseline_content_sha256": baseline.map(|source| source.content_sha256.clone()),
        "executable": {
            "path": configured_executable,
            "sha256": executable_sha256,
            "version": executable_version,
        },
        "toolchain": rustc,
        "argv": argv,
        "expected_targets": expected_targets,
        "cargo_target": cargo_target(&argv),
        "environment": environment,
        "platform": {"os": std::env::consts::OS,"architecture":std::env::consts::ARCH},
        "versioned_external_inputs": versioned_external_inputs,
    });
    resolved_inputs["cargo_metadata"] = json!({
        "candidate_graph_sha256": candidate_graph.as_ref().map(|graph| &graph.fingerprint),
        "baseline_graph_sha256": baseline_graph.as_ref().map(|graph| &graph.fingerprint),
        "scope_graph_sha256": scope_plan.workspace_graph_sha256,
        "external_cargo_config_unverified": external_cargo_config_unverified,
    });
    resolved_inputs["execution_workspace"] = execution_workspace_identity(profile, candidate)?;
    resolved_inputs["scope_plan"] = serde_json::to_value(&scope_plan)?;
    let input_fingerprint = fingerprint(&resolved_inputs, &scope_plan)?;
    resolved_inputs["input_fingerprint"] = json!(input_fingerprint);
    Ok(ResolvedCheckPlan {
        resolved_inputs,
        scope_plan,
        input_fingerprint,
    })
}

/// Test-only seam for Store integration tests; production always uses the
/// current binary's bounded, process-group-owned probe helper.
#[cfg(test)]
pub(crate) fn resolve_with_probe_for_test(
    profile: &CheckProfile,
    candidate: &VerifiedSource,
    baseline: Option<&VerifiedSource>,
    probe: TestProbeFn,
) -> Result<ResolvedCheckPlan> {
    resolve_with_probe(profile, candidate, baseline, probe)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checks::{
        model::Parser,
        source::{SourceFile, SourceManifest},
    };

    fn profile(reproducible: bool) -> CheckProfile {
        CheckProfile {
            profile_id: "strict".into(),
            profile_revision: "v1".into(),
            executable: std::env::current_exe().unwrap(),
            args: Vec::new(),
            parser: Parser::ExitCode,
            resource: "checks".into(),
            environment: BTreeMap::new(),
            inherit_env: Vec::new(),
            expected_targets: Vec::new(),
            reproducible,
            fingerprint_env: Vec::new(),
            versioned_inputs: BTreeMap::new(),
        }
    }

    fn fake_probe(
        _program: &Path,
        _args: &[String],
        _env: &BTreeMap<String, String>,
        _cwd: &Path,
    ) -> Result<Vec<u8>> {
        Ok(b"check resolver test tool 1.0\n".to_vec())
    }

    fn fake_probe_without_metadata(
        program: &Path,
        args: &[String],
        env: &BTreeMap<String, String>,
        cwd: &Path,
    ) -> Result<Vec<u8>> {
        assert!(!args.iter().any(|arg| arg == "metadata"));
        fake_probe(program, args, env, cwd)
    }

    fn fake_cargo_probe(
        _program: &Path,
        args: &[String],
        _env: &BTreeMap<String, String>,
        cwd: &Path,
    ) -> Result<Vec<u8>> {
        if args.iter().any(|arg| arg == "metadata") {
            let package_id = "workspace_pkg 0.1.0";
            return Ok(serde_json::to_vec(&json!({
                "packages": [{
                    "id": package_id,
                    "name": "workspace_pkg",
                    "version": "0.1.0",
                    "manifest_path": cwd.join("Cargo.toml"),
                    "targets": [{
                        "name": "required-target",
                        "kind": ["bin"],
                        "src_path": cwd.join("src/main.rs"),
                    }],
                    "dependencies": [],
                }],
                "workspace_members": [package_id],
                "workspace_root": cwd,
            }))?);
        }
        Ok(b"tool 1.0\n".to_vec())
    }

    fn isolated_cargo_test_root(label: &str) -> PathBuf {
        #[cfg(windows)]
        let mut bases = vec![std::env::temp_dir()];
        #[cfg(not(windows))]
        let bases = vec![std::env::temp_dir()];
        #[cfg(windows)]
        if let Some(system_root) = std::env::var_os("SystemRoot") {
            bases.insert(0, PathBuf::from(system_root).join("Temp"));
        }
        for base in bases {
            let Ok(base) = fs::canonicalize(base) else {
                continue;
            };
            let mut ancestor = Some(base.as_path());
            let mut configured = false;
            while let Some(directory) = ancestor {
                for config in ["config.toml", "config"] {
                    match fs::symlink_metadata(directory.join(".cargo").join(config)) {
                        Ok(_) => configured = true,
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(_) => configured = true,
                    }
                }
                if configured {
                    break;
                }
                ancestor = directory.parent();
            }
            if configured {
                continue;
            }
            let root = base.join(format!("{label}-{}", model::new_id()));
            if fs::create_dir(&root).is_ok() {
                if let Ok(root) = fs::canonicalize(root) {
                    return root;
                }
            }
        }
        panic!("no writable temporary root without ancestor Cargo configuration");
    }

    fn resolve_test(
        profile: &CheckProfile,
        candidate: &VerifiedSource,
        baseline: Option<&VerifiedSource>,
    ) -> ResolvedCheckPlan {
        resolve_with_probe(profile, candidate, baseline, fake_probe).unwrap()
    }

    fn captured_config_source(directory: &Path, include: bool) -> VerifiedSource {
        let config_path = directory.join(".cargo/config.toml");
        fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        fs::write(
            &config_path,
            if include {
                "include = ['../../host-config.toml']\n"
            } else {
                "[build]\nrustflags = []\n"
            },
        )
        .unwrap();
        let bytes = fs::read(&config_path).unwrap();
        VerifiedSource {
            manifest: SourceManifest {
                version: 1,
                commit: "commit".into(),
                tree: "tree".into(),
                files: vec![SourceFile {
                    path: ".cargo/config.toml".into(),
                    mode: "100644".into(),
                    object_id: "object".into(),
                    byte_length: bytes.len() as u64,
                    sha256: model::digest(&bytes),
                }],
            },
            content_sha256: model::digest(b"captured Cargo config fixture"),
            directory: fs::canonicalize(directory).unwrap(),
        }
    }

    #[test]
    fn external_cargo_config_and_unknown_includes_force_unversioned_state() {
        let root = isolated_cargo_test_root("swarm-cargo-config");
        let source_dir = root.join("data/sources/source-content");
        let cargo_home = root.join("isolated-cargo-home");
        let data_root = root.join("data");
        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(&cargo_home).unwrap();
        fs::create_dir_all(&data_root).unwrap();
        let env = BTreeMap::from([(
            "CARGO_HOME".into(),
            cargo_home.to_string_lossy().into_owned(),
        )]);
        let profile = profile(true);
        let external = root.join(".cargo/config.toml");
        fs::create_dir_all(external.parent().unwrap()).unwrap();
        fs::write(&external, "[build]\nrustflags = []\n").unwrap();
        let captured = captured_config_source(&source_dir, false);
        assert!(has_unversioned_cargo_config(&captured, &profile, &env));

        fs::remove_file(external).unwrap();
        let cargo_home_config = cargo_home.join("config.toml");
        fs::write(&cargo_home_config, "[build]\nrustflags = []\n").unwrap();
        assert!(has_unversioned_cargo_config(&captured, &profile, &env));
        fs::remove_file(cargo_home_config).unwrap();
        assert!(!has_unversioned_cargo_config(&captured, &profile, &env));

        let (content_workspace, _) = execution_paths(&data_root, &profile, &captured).unwrap();
        fs::create_dir_all(content_workspace.join(".cargo")).unwrap();
        fs::write(content_workspace.join(".cargo/config.toml"), "[build]\n").unwrap();
        assert!(has_unversioned_cargo_config(&captured, &profile, &env));
        fs::remove_dir_all(&content_workspace).unwrap();
        let included = captured_config_source(&source_dir, true);
        assert!(has_unversioned_cargo_config(&included, &profile, &env));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unresolved_cargo_config_widens_resolver_and_disables_reuse() {
        let root =
            std::env::temp_dir().join(format!("swarm-cargo-config-plan-{}", model::new_id()));
        let source_dir = root.join("data/sources/source-content");
        let cargo_home = root.join("isolated-cargo-home");
        let cargo_executable = root.join("cargo");
        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(&cargo_home).unwrap();
        fs::write(&cargo_executable, b"fake cargo executable").unwrap();
        let mut profile = profile(true);
        profile.parser = Parser::CargoJson;
        profile.executable = cargo_executable;
        profile.args = vec![
            "clippy".into(),
            "--locked".into(),
            "--offline".into(),
            "--message-format=json".into(),
        ];
        profile.expected_targets = vec!["required-target".into()];
        profile.environment.insert(
            "CARGO_HOME".into(),
            cargo_home.to_string_lossy().into_owned(),
        );
        let captured = captured_config_source(&source_dir, false);
        let external = root.join(".cargo/config.toml");
        fs::create_dir_all(external.parent().unwrap()).unwrap();
        fs::write(&external, "[build]\nrustflags = []\n").unwrap();

        let plan =
            resolve_with_probe(&profile, &captured, None, fake_probe_without_metadata).unwrap();
        assert_eq!(plan.scope_plan.mode, ScopeMode::Wide);
        assert!(
            plan.scope_plan
                .widening_reasons
                .contains(&"external_cargo_config_unverified".into())
        );
        assert_eq!(plan.resolved_inputs["cache_reusable"], false);
        assert!(
            plan.resolved_inputs["cache_disabled_reasons"]
                .as_array()
                .unwrap()
                .iter()
                .any(|reason| reason == "external_cargo_config_unverified")
        );

        fs::remove_file(external).unwrap();
        fs::write(cargo_home.join("config.toml"), "[build]\nrustflags = []\n").unwrap();
        let cargo_home_plan =
            resolve_with_probe(&profile, &captured, None, fake_probe_without_metadata).unwrap();
        assert_eq!(cargo_home_plan.scope_plan.mode, ScopeMode::Wide);
        assert_eq!(cargo_home_plan.resolved_inputs["cache_reusable"], false);
        fs::remove_file(cargo_home.join("config.toml")).unwrap();

        let included = captured_config_source(&source_dir, true);
        let included_plan =
            resolve_with_probe(&profile, &included, None, fake_probe_without_metadata).unwrap();
        assert_eq!(included_plan.scope_plan.mode, ScopeMode::Wide);
        assert_eq!(included_plan.resolved_inputs["cache_reusable"], false);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn cargo_reuse_requires_a_versioned_external_build_environment() {
        let root = isolated_cargo_test_root("swarm-build-env");
        let source_dir = root.join("data/sources/source-content");
        let cargo_home = root.join("cargo-home");
        let home = root.join("home");
        let cargo_executable = root.join(if cfg!(windows) { "cargo.exe" } else { "cargo" });
        fs::create_dir_all(&source_dir).unwrap();
        fs::create_dir_all(&cargo_home).unwrap();
        fs::create_dir_all(&home).unwrap();
        fs::write(
            source_dir.join("Cargo.toml"),
            "[package]\nname='workspace_pkg'\nversion='0.1.0'\n",
        )
        .unwrap();
        fs::create_dir_all(source_dir.join("src")).unwrap();
        fs::write(source_dir.join("src/main.rs"), "fn main() {}\n").unwrap();
        fs::write(&cargo_executable, b"fake cargo executable").unwrap();
        let manifest_bytes = fs::read(source_dir.join("Cargo.toml")).unwrap();
        let main_bytes = fs::read(source_dir.join("src/main.rs")).unwrap();
        let manifest_files = vec![
            SourceFile {
                path: "Cargo.toml".into(),
                mode: "100644".into(),
                object_id: model::digest(&manifest_bytes),
                byte_length: manifest_bytes.len() as u64,
                sha256: model::digest(&manifest_bytes),
            },
            SourceFile {
                path: "src/main.rs".into(),
                mode: "100644".into(),
                object_id: model::digest(&main_bytes),
                byte_length: main_bytes.len() as u64,
                sha256: model::digest(&main_bytes),
            },
        ];
        let content_files: Vec<_> = manifest_files
            .iter()
            .map(|file| {
                json!({
                    "path":file.path,
                    "mode":file.mode,
                    "byte_length":file.byte_length,
                    "sha256":file.sha256,
                })
            })
            .collect();
        let content_sha256 = model::digest(
            model::canonical(&json!({"version":1,"files":content_files}))
                .unwrap()
                .as_bytes(),
        );
        let candidate = VerifiedSource {
            manifest: SourceManifest {
                version: 1,
                commit: "capture-id-is-not-reuse-identity".into(),
                tree: "tree-id-is-not-reuse-identity".into(),
                files: manifest_files,
            },
            content_sha256,
            directory: fs::canonicalize(&source_dir).unwrap(),
        };
        let mut profile = profile(true);
        profile.parser = Parser::CargoJson;
        profile.executable = cargo_executable.clone();
        profile.args = vec![
            "clippy".into(),
            "--locked".into(),
            "--offline".into(),
            "--message-format=json".into(),
        ];
        profile.expected_targets = vec!["required-target".into()];
        profile.environment = BTreeMap::from([
            (
                "CARGO_HOME".into(),
                cargo_home.to_string_lossy().into_owned(),
            ),
            ("HOME".into(), home.to_string_lossy().into_owned()),
            ("USERPROFILE".into(), home.to_string_lossy().into_owned()),
            (
                "RUSTC".into(),
                cargo_executable.to_string_lossy().into_owned(),
            ),
        ]);
        profile.fingerprint_env = vec!["RUSTC".into()];

        let missing = resolve_with_probe(&profile, &candidate, None, fake_cargo_probe).unwrap();
        assert!(
            missing.resolved_inputs["cache_disabled_reasons"]
                .as_array()
                .unwrap()
                .iter()
                .any(|reason| reason == "cargo_external_tools_unversioned")
        );

        profile.versioned_inputs.insert(
            "build_environment".into(),
            "sha256:immutable-builder-image-v7".into(),
        );
        let versioned = resolve_with_probe(&profile, &candidate, None, fake_cargo_probe).unwrap();
        assert!(
            !versioned.resolved_inputs["cache_disabled_reasons"]
                .as_array()
                .unwrap()
                .iter()
                .any(|reason| reason == "cargo_external_tools_unversioned")
        );
        assert_eq!(versioned.resolved_inputs["cache_reusable"], true);
        assert_ne!(missing.input_fingerprint, versioned.input_fingerprint);
        assert_eq!(
            versioned.resolved_inputs["versioned_external_inputs"]["build_environment"],
            model::digest(b"sha256:immutable-builder-image-v7")
        );
        assert!(
            !model::canonical(&versioned.resolved_inputs)
                .unwrap()
                .contains("immutable-builder-image-v7")
        );
        let _ = fs::remove_dir_all(root);
    }

    fn source(content_hash: &str, artifact_id: &str) -> VerifiedSource {
        VerifiedSource {
            manifest: SourceManifest {
                version: 1,
                commit: artifact_id.into(),
                tree: "tree".into(),
                files: vec![SourceFile {
                    path: "src/lib.rs".into(),
                    mode: "100644".into(),
                    object_id: artifact_id.into(),
                    byte_length: 1,
                    sha256: content_hash.into(),
                }],
            },
            content_sha256: content_hash.into(),
            directory: std::env::current_dir().unwrap(),
        }
    }

    #[test]
    fn reusable_fingerprint_ignores_capture_ids_but_tracks_source_and_profile() {
        let base_profile = profile(true);
        let a = source("same-content", "artifact-a");
        let b = source("same-content", "artifact-b");
        let plan_a = resolve_test(&base_profile, &a, None);
        let plan_b = resolve_test(&base_profile, &b, None);
        assert_eq!(plan_a.resolved_inputs["cache_reusable"], true);
        assert!(
            !plan_a.resolved_inputs["cache_disabled_reasons"]
                .as_array()
                .unwrap()
                .iter()
                .any(|reason| reason == "workspace_graph_incomplete")
        );
        assert_eq!(plan_a.input_fingerprint, plan_b.input_fingerprint);
        assert_ne!(
            plan_a.input_fingerprint,
            resolve_test(
                &base_profile,
                &source("changed-content", "artifact-c"),
                None
            )
            .input_fingerprint
        );
        let mut changed_profile = base_profile;
        changed_profile.profile_revision = "v2".into();
        assert_ne!(
            plan_a.input_fingerprint,
            resolve_test(&changed_profile, &a, None).input_fingerprint
        );
        let mut external_v1 = profile(true);
        external_v1
            .versioned_inputs
            .insert("service_snapshot".into(), "sha256:one".into());
        let mut external_v2 = external_v1.clone();
        external_v2
            .versioned_inputs
            .insert("service_snapshot".into(), "sha256:two".into());
        let external_plan = resolve_test(&external_v1, &a, None);
        let changed_external_plan = resolve_test(&external_v2, &a, None);
        assert_ne!(
            external_plan.input_fingerprint,
            changed_external_plan.input_fingerprint
        );
        assert_ne!(
            external_plan.resolved_inputs["versioned_external_inputs"],
            changed_external_plan.resolved_inputs["versioned_external_inputs"]
        );
    }

    #[test]
    fn reuse_is_disabled_by_default_and_environment_identity_contains_only_digests() {
        let profile = profile(false);
        let source = source("content", "artifact");
        let plan = resolve_test(&profile, &source, None);
        assert_eq!(plan.resolved_inputs["cache_reusable"], false);
        assert!(
            plan.resolved_inputs["cache_disabled_reasons"]
                .as_array()
                .unwrap()
                .iter()
                .any(|reason| reason == "profile_not_reproducible")
        );
        let identity = environment_identity(&profile, &source).unwrap();
        assert!(identity["nonsecret_value_sha256"].get("PATH").is_some());
        assert!(
            identity["nonsecret_value_sha256"]
                .get("PATH")
                .unwrap()
                .as_str()
                .unwrap()
                .len()
                == 64
        );
        assert!(identity.get("PATH").is_none());
    }

    #[test]
    fn opaque_environment_values_are_never_hashed_and_disable_reuse() {
        let mut profile = profile(true);
        profile
            .environment
            .insert("API_TOKEN".into(), "credential-value-must-not-hash".into());
        let source = source("content", "artifact");
        let identity = environment_identity(&profile, &source).unwrap();
        assert!(
            identity["opaque_names"]
                .as_array()
                .unwrap()
                .iter()
                .any(|name| name == "API_TOKEN")
        );
        assert!(
            identity["nonsecret_value_sha256"]
                .get("API_TOKEN")
                .is_none()
        );
        assert!(
            !model::canonical(&identity)
                .unwrap()
                .contains("credential-value-must-not-hash")
        );
        let plan = resolve_test(&profile, &source, None);
        assert_eq!(plan.resolved_inputs["cache_reusable"], false);
        assert!(
            plan.resolved_inputs["cache_disabled_reasons"]
                .as_array()
                .unwrap()
                .iter()
                .any(|reason| reason == "opaque_environment:API_TOKEN")
        );

        profile.fingerprint_env.push("API_TOKEN".into());
        let safe_identity = environment_identity(&profile, &source).unwrap();
        assert!(
            safe_identity["nonsecret_value_sha256"]
                .get("API_TOKEN")
                .is_some()
        );
        assert!(
            !safe_identity["opaque_names"]
                .as_array()
                .unwrap()
                .iter()
                .any(|name| name == "API_TOKEN")
        );
    }

    #[test]
    fn external_baseline_reason_updates_the_stored_scope_and_fingerprint_together() {
        let profile = profile(false);
        let candidate = source("content", "artifact");
        let mut plan = resolve_test(&profile, &candidate, None);
        let before = plan.input_fingerprint.clone();
        plan.add_widening_reason("baseline_source_unverified")
            .unwrap();
        assert_ne!(before, plan.input_fingerprint);
        assert_eq!(
            plan.resolved_inputs["input_fingerprint"],
            plan.input_fingerprint
        );
        assert_eq!(plan.resolved_inputs["scope_plan"], json!(plan.scope_plan));
        assert!(
            plan.scope_plan
                .widening_reasons
                .contains(&"baseline_source_unverified".into())
        );
    }

    #[test]
    fn narrowed_cargo_argv_replaces_untrusted_scope_flags_and_preserves_compiler_args() {
        let plan = ScopePlan {
            version: 1,
            mode: ScopeMode::Narrowed,
            selected_packages: vec!["core".into()],
            excluded_packages: vec!["other".into()],
            changed_paths: vec!["core/src/lib.rs".into()],
            required_targets: vec![],
            selected_targets: vec![],
            target_identities: BTreeMap::new(),
            excluded_required_targets: vec![],
            coverage_gaps: vec![],
            widening_reasons: vec![],
            workspace_graph_sha256: Some("graph".into()),
        };
        let args = vec![
            "clippy".into(),
            "--workspace".into(),
            "--lib".into(),
            "--".into(),
            "-D".into(),
            "warnings".into(),
        ];
        assert_eq!(
            replace_package_scope(&args, &plan),
            [
                "clippy",
                "--lib",
                "--package",
                "core",
                "--",
                "-D",
                "warnings"
            ]
        );
    }
}
