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
    platform::process_group::{Group, departed_empty, spawned_departed, spawned_identity},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};
use swarm_process::capture::{CaptureStream, PollableRead};

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
const PROBE_HELPER_RESPONSE_FILE: &str = "helper-response.capture";
#[cfg(test)]
const PROBE_CUSTODY_CONTROL_ENV: &str = "ELIOT_PROBE_CUSTODY_CONTROL_DIR";
#[cfg(test)]
const PROBE_CUSTODY_FIXTURE_GRACE_ENV: &str = "ELIOT_PROBE_CUSTODY_FIXTURE_GRACE";
#[cfg(test)]
const PROBE_CUSTODY_MEMBER_OBSERVED_FILE: &str = "helper-live-member-observed.json";
pub(crate) const PROBE_STDOUT_CAPTURE_FILE: &str = "probe-stdout.capture";
pub(crate) const PROBE_STDERR_CAPTURE_FILE: &str = "probe-stderr.capture";
#[cfg(test)]
pub(crate) type TestProbeFn =
    fn(&Path, &[String], &BTreeMap<String, String>, &Path) -> Result<Vec<u8>>;

#[cfg(test)]
type TestProbeCleanupOverrides =
    BTreeMap<(String, String), (TestProbeFn, Option<ProbeCleanupOwner>)>;

#[cfg(test)]
static TEST_PROBE_CLEANUP_OVERRIDES: std::sync::OnceLock<
    std::sync::Mutex<TestProbeCleanupOverrides>,
> = std::sync::OnceLock::new();

#[cfg(test)]
pub(crate) struct TestProbeCleanupOverrideGuard {
    profile_id: String,
    profile_revision: String,
}

#[cfg(test)]
pub(crate) fn override_probe_cleanup_for_test(
    profile: &CheckProfile,
    probe: TestProbeFn,
    owner: ProbeCleanupOwner,
) -> TestProbeCleanupOverrideGuard {
    override_probe_for_test_with_owner(profile, probe, Some(owner))
}

#[cfg(test)]
pub(crate) fn override_probe_for_test(
    profile: &CheckProfile,
    probe: TestProbeFn,
) -> TestProbeCleanupOverrideGuard {
    override_probe_for_test_with_owner(profile, probe, None)
}

#[cfg(test)]
fn override_probe_for_test_with_owner(
    profile: &CheckProfile,
    probe: TestProbeFn,
    owner: Option<ProbeCleanupOwner>,
) -> TestProbeCleanupOverrideGuard {
    let key = (profile.profile_id.clone(), profile.profile_revision.clone());
    let mut overrides = TEST_PROBE_CLEANUP_OVERRIDES
        .get_or_init(|| std::sync::Mutex::new(BTreeMap::new()))
        .lock()
        .expect("probe cleanup test override lock");
    assert!(
        !overrides.contains_key(&key),
        "probe cleanup test override already exists for this profile"
    );
    overrides.insert(key.clone(), (probe, owner));
    TestProbeCleanupOverrideGuard {
        profile_id: key.0,
        profile_revision: key.1,
    }
}

#[cfg(test)]
impl Drop for TestProbeCleanupOverrideGuard {
    fn drop(&mut self) {
        if let Some(overrides) = TEST_PROBE_CLEANUP_OVERRIDES.get()
            && let Ok(mut overrides) = overrides.lock()
        {
            overrides.remove(&(self.profile_id.clone(), self.profile_revision.clone()));
        }
    }
}

#[cfg(test)]
fn take_probe_cleanup_override(
    profile: &CheckProfile,
) -> Option<(TestProbeFn, Option<ProbeCleanupOwner>)> {
    TEST_PROBE_CLEANUP_OVERRIDES
        .get()?
        .lock()
        .ok()?
        .remove(&(profile.profile_id.clone(), profile.profile_revision.clone()))
}

enum ProbeExecutor {
    Production,
    #[cfg(test)]
    Test(TestProbeFn),
    #[cfg(test)]
    TestWithCleanup {
        probe: TestProbeFn,
        owner: Option<Box<ProbeCleanupOwner>>,
    },
}

struct ProbeContext {
    executor: ProbeExecutor,
    cleanup_owner: Option<ProbeCleanupOwner>,
}

impl ProbeContext {
    fn production() -> Self {
        Self {
            executor: ProbeExecutor::Production,
            cleanup_owner: None,
        }
    }

    #[cfg(test)]
    fn test(probe: TestProbeFn) -> Self {
        Self {
            executor: ProbeExecutor::Test(probe),
            cleanup_owner: None,
        }
    }

    #[cfg(test)]
    fn test_with_cleanup(probe: TestProbeFn, owner: Option<ProbeCleanupOwner>) -> Self {
        Self {
            executor: ProbeExecutor::TestWithCleanup {
                probe,
                owner: owner.map(Box::new),
            },
            cleanup_owner: None,
        }
    }

    fn run(
        &mut self,
        program: &Path,
        args: &[String],
        env: &BTreeMap<String, String>,
        cwd: &Path,
    ) -> Result<Vec<u8>> {
        if self
            .cleanup_owner
            .as_ref()
            .is_some_and(ProbeCleanupOwner::is_pending)
        {
            return Err(Error::new(
                "CHECK_PROBE_CLEANUP_PENDING",
                "a prior input probe helper remains in Store custody",
            ));
        }
        match &mut self.executor {
            ProbeExecutor::Production => {
                let (result, owner) = command_output_with_cleanup(program, args, env, cwd);
                self.cleanup_owner = owner;
                result
            }
            #[cfg(test)]
            ProbeExecutor::Test(probe) => probe(program, args, env, cwd),
            #[cfg(test)]
            ProbeExecutor::TestWithCleanup { probe, owner } => {
                let result = probe(program, args, env, cwd);
                self.cleanup_owner = owner.take().map(|owner| *owner);
                result
            }
        }
    }
}

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

/// Custody retained by Store when an input-probe helper or its response pipe
/// outlives the bounded foreground phase.
/// Exact check-target identity retained with probe cleanup custody.
///
/// Resource is the warm-target isolation key (`check-inputs/<resource>`), and
/// workspace identity is the exact profile/candidate namespace. Shared resources
/// conflict even when profile IDs differ. An absent scope is unidentified and
/// fails closed. Child custody is released only after poll proves departure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProbeCleanupScope {
    pub resource: String,
    pub workspace_identity: Value,
}

impl ProbeCleanupScope {
    pub(crate) fn resource_for_profile(profile: &CheckProfile) -> Option<String> {
        let resource = profile.resource.to_ascii_lowercase();
        if resource.is_empty() || resource.len() > 100 {
            return None;
        }
        Some(resource)
    }

    pub(crate) fn for_candidate(
        profile: &CheckProfile,
        candidate: &VerifiedSource,
    ) -> Option<Self> {
        let resource = Self::resource_for_profile(profile)?;
        if profile.profile_id.is_empty() || profile.profile_revision.trim().is_empty() {
            return None;
        }
        let workspace_identity = execution_workspace_identity(profile, candidate).ok()?;
        Some(Self {
            resource,
            workspace_identity,
        })
    }

    pub(crate) fn overlaps(&self, other: &Self) -> bool {
        self.resource == other.resource || self.workspace_identity == other.workspace_identity
    }
}

/// True when incoming must stay held behind retained. Unknown scope on either
/// side (damaged/unidentified) fails closed and holds.
pub(crate) fn probe_scopes_conflict(
    retained: Option<&ProbeCleanupScope>,
    incoming: Option<&ProbeCleanupScope>,
) -> bool {
    match (retained, incoming) {
        (Some(a), Some(b)) => a.overlaps(b),
        _ => true,
    }
}

pub(crate) fn probe_resource_conflicts(
    retained: Option<&ProbeCleanupScope>,
    incoming_resource: Option<&str>,
) -> bool {
    match (retained, incoming_resource) {
        (Some(retained), Some(incoming)) => retained.resource == incoming,
        _ => true,
    }
}

pub(crate) struct ProbeCleanupOwner {
    custody: ProbeHelperCustody,
    scope: Option<ProbeCleanupScope>,
}

impl ProbeCleanupOwner {
    pub(crate) fn poll(&mut self) {
        self.custody.poll();
    }

    pub(crate) fn is_pending(&self) -> bool {
        !self.custody.cleanup_complete
    }

    pub(crate) fn scope(&self) -> Option<&ProbeCleanupScope> {
        self.scope.as_ref()
    }

    pub(crate) fn set_scope(&mut self, scope: Option<ProbeCleanupScope>) {
        self.scope = scope;
    }

    /// True when this pending owner holds incoming. Non-pending owners never
    /// hold; unknown scope on either side holds (fail-closed). Actual Child,
    /// pipe and family custody is released only via poll-proven departure.
    pub(crate) fn conflicts_with(&self, incoming: Option<&ProbeCleanupScope>) -> bool {
        if !self.is_pending() {
            return false;
        }
        probe_scopes_conflict(self.scope.as_ref(), incoming)
    }

    pub(crate) fn conflicts_with_resource(&self, incoming: Option<&str>) -> bool {
        if !self.is_pending() {
            return false;
        }
        probe_resource_conflicts(self.scope.as_ref(), incoming)
    }
}

/// Build a pending owner around a live test child so Store fixtures exercise
/// the same poll, install, conflict, and departure path as production custody.
#[cfg(test)]
pub(crate) fn pending_probe_cleanup_owner_for_test(
    child: Child,
    directory: PathBuf,
    scope: Option<ProbeCleanupScope>,
) -> ProbeCleanupOwner {
    let child_pid = child.id();
    ProbeCleanupOwner {
        custody: ProbeHelperCustody {
            child,
            child_pid,
            launch_identity: None,
            stdout: None,
            response_file: None,
            capture: None,
            directory,
            family_identity: None,
            family_token: None,
            #[cfg(windows)]
            family_query: None,
            exit_status: None,
            status_observation_failed: false,
            capture_setup_failed: true,
            response_group_empty: None,
            family_departed_proven: false,
            cleanup_complete: false,
        },
        scope,
    }
}

impl Drop for ProbeCleanupOwner {
    fn drop(&mut self) {
        if self.custody.cleanup_complete && self.custody.response_group_empty == Some(true) {
            self.custody.capture.take();
            self.custody.stdout.take();
            self.custody.response_file.take();
            cleanup_probe_directory(&self.custody.directory);
        }
    }
}

struct ProbeHelperCustody {
    child: Child,
    child_pid: u32,
    launch_identity: Option<Value>,
    stdout: Option<ChildStdout>,
    response_file: Option<File>,
    capture: Option<CaptureStream<ProbeHelperStdout>>,
    directory: PathBuf,
    family_identity: Option<Value>,
    family_token: Option<String>,
    #[cfg(windows)]
    family_query: Option<Group>,
    exit_status: Option<ExitStatus>,
    status_observation_failed: bool,
    capture_setup_failed: bool,
    response_group_empty: Option<bool>,
    family_departed_proven: bool,
    cleanup_complete: bool,
}

impl ProbeHelperCustody {
    fn poll(&mut self) {
        if self.cleanup_complete {
            return;
        }
        self.start_capture_if_possible();
        if let Some(capture) = self.capture.as_mut() {
            capture.poll();
        }
        if self.exit_status.is_none() {
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    self.exit_status = Some(status);
                    self.status_observation_failed = false;
                }
                Ok(None) => {}
                Err(_) => self.status_observation_failed = true,
            }
        }
        self.load_family_identity();
        #[cfg(windows)]
        self.retain_family_query();
        if self
            .capture
            .as_ref()
            .is_some_and(CaptureStream::is_finished)
        {
            self.load_response_group_state();
        }
        if self.exit_status.is_none() || !self.family_departed_once() {
            return;
        }
        self.family_departed_proven = true;
        if self
            .capture
            .as_ref()
            .is_some_and(CaptureStream::is_finished)
        {
            self.cleanup_complete = true;
        } else if self.capture.is_none() {
            // The exact process family and direct Child are gone. Close any
            // unusable pipe handles without treating that closure as EOF.
            self.stdout.take();
            self.response_file.take();
            self.capture_setup_failed = true;
            self.cleanup_complete = true;
        }
    }

    fn start_capture_if_possible(&mut self) {
        if self.capture.is_some() || self.stdout.is_none() || self.response_file.is_none() {
            return;
        }
        let configured = self
            .stdout
            .as_ref()
            .is_some_and(|stdout| stdout.make_nonblocking().is_ok());
        if !configured {
            return;
        }
        let Some(stdout) = self.stdout.take() else {
            self.capture_setup_failed = true;
            return;
        };
        let Some(file) = self.response_file.take() else {
            self.stdout = Some(stdout);
            self.capture_setup_failed = true;
            return;
        };
        match CaptureStream::new(
            self.directory.join(PROBE_HELPER_RESPONSE_FILE),
            file,
            ProbeHelperStdout(stdout),
            PROBE_RESPONSE_LIMIT as u64,
        ) {
            Ok(capture) => self.capture = Some(capture),
            Err(_) => self.capture_setup_failed = true,
        }
    }

    fn load_response_group_state(&mut self) {
        if self.response_group_empty.is_some()
            || self.capture.as_ref().is_none_or(|capture| {
                !capture.is_finished() || capture.truncated() || capture.reader_failed()
            })
        {
            return;
        }
        let response_path = self.directory.join(PROBE_HELPER_RESPONSE_FILE);
        let Ok(file) = File::open(response_path) else {
            return;
        };
        let Ok(bytes) = read_bounded(file, PROBE_RESPONSE_LIMIT) else {
            return;
        };
        self.response_group_empty = decode_probe_response(&bytes)
            .ok()
            .map(|response| response.group_empty);
    }

    fn request_stop(&mut self) {
        if self.exit_status.is_none() {
            let _ = self.child.kill();
        }
    }

    fn load_family_identity(&mut self) {
        if self.family_identity.is_some() {
            return;
        }
        let owner_path = self.directory.join("probe-owner.json");
        let Ok(file) = File::open(owner_path) else {
            return;
        };
        let Ok(bytes) = read_bounded(file, 16 * 1024) else {
            return;
        };
        let Ok(identity) = serde_json::from_slice::<Value>(&bytes) else {
            return;
        };
        if identity["purpose"] != "check"
            || identity["pid"].as_u64() != Some(u64::from(self.child_pid))
            || self
                .launch_identity
                .as_ref()
                .is_some_and(|launch| launch["pid"].as_u64() != identity["pid"].as_u64())
        {
            return;
        }
        let Ok(token) = probe_group_token(&identity) else {
            return;
        };
        #[cfg(windows)]
        if self.launch_identity.as_ref().is_some_and(|launch| {
            launch["creation_filetime"].as_u64().is_none()
                || launch["creation_filetime"] != identity["creation_filetime"]
        }) {
            return;
        }
        self.family_identity = Some(identity);
        self.family_token = Some(token);
    }

    #[cfg(windows)]
    fn retain_family_query(&mut self) {
        if self.family_query.is_none()
            && let (Some(identity), Some(token)) =
                (self.family_identity.as_ref(), self.family_token.as_deref())
        {
            self.family_query = Group::open_check_query(identity, token).ok();
        }
    }

    fn family_departed_once(&self) -> bool {
        let (Some(identity), Some(token)) =
            (self.family_identity.as_ref(), self.family_token.as_deref())
        else {
            // execute_probe persists this file before it can spawn the
            // requested executable and never removes it. After the exact
            // helper Child exits, absence proves that no nested family was
            // launched; it does not stand in for a departed-family receipt.
            return self.exit_status.is_some()
                && matches!(
                    fs::symlink_metadata(self.directory.join("probe-owner.json")),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound
                );
        };
        #[cfg(windows)]
        if self.exit_status.is_some() {
            if let Some(query) = self.family_query.as_ref() {
                return query.family_empty().is_ok_and(|empty| empty);
            }
            // A fast helper can publish its complete, bounded response and
            // disarm before the first parent poll. That response is carried
            // by this exact owned Child's pipe; it proves cleanup independently
            // of a now-vanished name. Missing/partial/false responses never do.
            if self.response_group_empty == Some(true) {
                return true;
            }
        }
        let group_departed = departed_empty(identity, token).is_ok_and(|departed| departed);
        let launch_departed = match self.launch_identity.as_ref() {
            Some(launch) => spawned_departed(launch, token).is_ok_and(|departed| departed),
            None => self.exit_status.is_some(),
        };
        group_departed && launch_departed
    }

    fn into_output(mut self) -> Result<Vec<u8>> {
        if !self.cleanup_complete || !self.family_departed_proven {
            return Err(Error::new(
                "CHECK_PROBE_CLEANUP_PENDING",
                "input probe helper custody is not releasable before exact family departure and pipe completion",
            ));
        }
        if self.status_observation_failed {
            return Err(Error::new(
                "CHECK_PROBE_CLEANUP_PENDING",
                "input probe helper exit status observation failed",
            ));
        }
        let status = self.exit_status.take().ok_or_else(|| {
            Error::new(
                "CHECK_PROBE_CLEANUP_PENDING",
                "input probe helper exit was not observed",
            )
        })?;
        if !status.success() {
            return Err(Error::new(
                "CHECK_INPUT_RESOLUTION",
                "owned input probe helper exited without a response",
            ));
        }
        if self.capture_setup_failed {
            return Err(Error::new(
                "CHECK_PROBE_CAPTURE_FAILED",
                "bounded input probe response capture could not be started",
            ));
        }
        let capture = self.capture.take().ok_or_else(|| {
            Error::new(
                "CHECK_PROBE_CAPTURE_FAILED",
                "bounded input probe response capture is unavailable",
            )
        })?;
        let receipt = capture.finish();
        if !receipt.capture_complete || receipt.capture_error.is_some() || receipt.truncated {
            return Err(Error::new(
                "CHECK_PROBE_CAPTURE_FAILED",
                "bounded input probe response was incomplete or exceeded its frame limit",
            ));
        }
        read_bounded(
            File::open(self.directory.join(PROBE_HELPER_RESPONSE_FILE))?,
            PROBE_RESPONSE_LIMIT,
        )
    }
}

struct ProbeHelperStdout(ChildStdout);

impl Read for ProbeHelperStdout {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.0.read(buffer)
    }
}

impl PollableRead for ProbeHelperStdout {
    fn make_nonblocking(&self) -> std::io::Result<()> {
        self.0.make_nonblocking()
    }

    fn read_available(
        &mut self,
        buffer: &mut [u8],
    ) -> std::io::Result<swarm_process::capture::PipeRead> {
        self.0.read_available(buffer)
    }
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
        cleanup_probe_directory(&self.0);
    }
}

fn cleanup_probe_directory(directory: &Path) {
    for name in [
        "request.json",
        "probe-owner.json",
        PROBE_HELPER_RESPONSE_FILE,
        PROBE_STDOUT_CAPTURE_FILE,
        PROBE_STDERR_CAPTURE_FILE,
        "stdout.partial",
        "stderr.partial",
    ] {
        let _ = fs::remove_file(directory.join(name));
    }
    let _ = fs::remove_dir(directory);
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
    execution_paths_for_layout(
        data_dir,
        profile,
        candidate,
        EXECUTION_WORKSPACE_LAYOUT_VERSION,
    )
}

/// Select deterministic physical paths from the retained plan's layout version.
pub(crate) fn execution_paths_for_layout(
    data_dir: &Path,
    profile: &CheckProfile,
    verified: &VerifiedSource,
    layout_version: u32,
) -> Result<(PathBuf, PathBuf)> {
    if verified.content_sha256.len() != 64
        || !verified
            .content_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::new(
            "CHECK_INPUTS_STALE",
            "candidate content identity is malformed",
        ));
    }
    let base = data_dir
        .join("check-inputs")
        .join(profile.resource.to_ascii_lowercase());
    let (base, workspace_key) = match layout_version {
        1 => (
            base.join(profile_identity_sha256(profile)?),
            verified.content_sha256.clone(),
        ),
        EXECUTION_WORKSPACE_LAYOUT_VERSION => {
            // One full digest binds the profile, resource and candidate without
            // two nested digest directories exceeding Windows' cwd boundary.
            let workspace_key = model::digest(
                model::canonical(&execution_workspace_identity(profile, verified)?)?.as_bytes(),
            );
            (base, workspace_key)
        }
        _ => {
            return Err(Error::new(
                "CHECK_INPUTS_STALE",
                "execution workspace layout version is unsupported",
            ));
        }
    };
    let workspace = base.join(&workspace_key);
    let descriptor = base.join(format!("{workspace_key}.candidate.json"));
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

fn command_output_with_cleanup(
    program: &Path,
    args: &[String],
    env: &BTreeMap<String, String>,
    cwd: &Path,
) -> (Result<Vec<u8>>, Option<ProbeCleanupOwner>) {
    let current_exe = match std::env::current_exe() {
        Ok(path) => path,
        Err(error) => return (Err(error.into()), None),
    };
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
    if let Err(error) = fs::create_dir(&temp_dir) {
        return (Err(error.into()), None);
    }
    let mut temp_guard = TempProbeDir(temp_dir.clone(), false);
    let request_path = temp_guard.0.join("request.json");
    let request_bytes = match model::canonical(&json!({"check_probe":request})) {
        Ok(bytes) => bytes.into_bytes(),
        Err(error) => return (Err(error), None),
    };
    let mut request_file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&request_path)
    {
        Ok(file) => file,
        Err(error) => return (Err(error.into()), None),
    };
    if let Err(error) = request_file
        .write_all(&request_bytes)
        .and_then(|()| request_file.sync_all())
    {
        return (Err(error.into()), None);
    }
    drop(request_file);
    let response_path = temp_guard.0.join(PROBE_HELPER_RESPONSE_FILE);
    let response_file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&response_path)
    {
        Ok(file) => file,
        Err(error) => return (Err(error.into()), None),
    };
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
    let mut child = match helper.spawn() {
        Ok(child) => child,
        Err(error) => return (Err(error.into()), None),
    };
    temp_guard.preserve();
    let child_pid = child.id();
    let launch_identity = spawned_identity(child_pid).ok();
    let stdout = child.stdout.take();
    let mut custody = ProbeHelperCustody {
        child,
        child_pid,
        launch_identity,
        stdout,
        response_file: Some(response_file),
        capture: None,
        directory: temp_guard.0.clone(),
        family_identity: None,
        family_token: None,
        #[cfg(windows)]
        family_query: None,
        exit_status: None,
        status_observation_failed: false,
        capture_setup_failed: false,
        response_group_empty: None,
        family_departed_proven: false,
        cleanup_complete: false,
    };
    if custody.stdout.is_none() {
        custody.capture_setup_failed = true;
        custody.request_stop();
    }

    let helper_deadline = Instant::now() + PROBE_HELPER_WAIT_GRACE;
    let mut drain_deadline = None;
    let mut kill_deadline = None;
    loop {
        custody.poll();
        if custody.exit_status.is_some() && drain_deadline.is_none() {
            drain_deadline = Some(Instant::now() + PROBE_CAPTURE_DRAIN_GRACE);
        }
        if custody.cleanup_complete {
            let mut response_group_empty_proven = false;
            let output = custody.into_output().and_then(|bytes| {
                let response = decode_probe_response(&bytes)?;
                if !response.group_empty {
                    retain_probe_partial(&temp_guard.0, "stdout.partial", &response.stdout)?;
                    retain_probe_partial(&temp_guard.0, "stderr.partial", &response.stderr)?;
                    return Err(Error::new(
                        "CHECK_PROBE_CLEANUP_PENDING",
                        format!(
                            "input probe did not prove its process group empty; {}; partial_stdout_bytes={}; partial_stderr_bytes={}",
                            response.message,
                            response.stdout.len(),
                            response.stderr.len()
                        ),
                    ));
                }
                response_group_empty_proven = true;
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
            });
            if response_group_empty_proven {
                temp_guard.allow_cleanup();
            }
            return (output, None);
        }

        let now = Instant::now();
        if kill_deadline.is_none() && now >= helper_deadline {
            custody.request_stop();
            kill_deadline = Some(now + PROBE_CLEANUP_GRACE);
        }
        let drain_expired = drain_deadline.is_some_and(|deadline| now >= deadline);
        let cleanup_expired = kill_deadline.is_some_and(|deadline| now >= deadline);
        if drain_expired || cleanup_expired {
            let reason = if drain_expired {
                "input probe response pipe remained open after the bounded drain grace"
            } else {
                "input probe helper family remains live after bounded stop grace"
            };
            let error = Error::new(
                "CHECK_PROBE_CLEANUP_PENDING",
                format!(
                    "{reason}{}",
                    probe_owner_diagnostic(
                        &temp_guard.0.join("probe-owner.json"),
                        custody.launch_identity.as_ref()
                    )
                ),
            );
            temp_guard.preserve();
            return (
                Err(error),
                Some(ProbeCleanupOwner {
                    custody,
                    scope: None,
                }),
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
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

fn probe_group_token(identity: &Value) -> Result<String> {
    if identity["scope"] == "linux_process_group" {
        return Ok(String::new());
    }
    let job_name = identity["job_name"]
        .as_str()
        .ok_or_else(|| Error::invalid("input probe Job name is missing"))?;
    job_name
        .strip_prefix(r"Global\EliotSwarmCheck-")
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Error::invalid("input probe Job name is invalid"))
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

struct ProbeOwner {
    group: Group,
}

impl ProbeOwner {
    fn release(&mut self) -> Result<()> {
        let deadline = Instant::now() + PROBE_CLEANUP_GRACE;
        let mut requests = 0u8;
        // Give a direct-exit descendant the existing cleanup grace to close
        // inherited output pipes naturally before the first cancellation.
        // The parent GitHelperCustody continues polling while this helper
        // retains the exact Group and child handles.
        let mut next_cancel = Instant::now() + probe_initial_cleanup_grace();
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
            if requests < PROBE_MAX_CLEANUP_REQUESTS && Instant::now() >= next_cancel {
                requests += 1;
                next_cancel = Instant::now() + PROBE_CLEANUP_RETRY;
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

#[cfg(test)]
fn record_probe_custody_member_observed_for_test(
    owner: &ProbeOwner,
    direct_child_exit_code: Option<i32>,
) {
    if std::env::var(PROBE_CUSTODY_FIXTURE_GRACE_ENV).as_deref() != Ok("nested") {
        return;
    }
    let Some(control) = std::env::var_os(PROBE_CUSTODY_CONTROL_ENV) else {
        return;
    };
    let path = PathBuf::from(control).join(PROBE_CUSTODY_MEMBER_OBSERVED_FILE);
    let mut file = match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => return,
        Err(error) => panic!("write exact probe-family observation receipt: {error}"),
    };
    let receipt = json!({
        "probe_group_identity": owner.group.identity,
        "direct_child_exit_code": direct_child_exit_code,
        "group_member_observed_after_direct_exit": true,
    });
    file.write_all(
        model::canonical(&receipt)
            .expect("encode probe-family observation receipt")
            .as_bytes(),
    )
    .expect("persist probe-family observation receipt");
    file.sync_all()
        .expect("sync probe-family observation receipt");
}

fn probe_initial_cleanup_grace() -> Duration {
    #[cfg(test)]
    if std::env::var("HOOK_GIT_PROBE_FIXTURE_MODE").as_deref() == Ok("descendant")
        || std::env::var(PROBE_CUSTODY_FIXTURE_GRACE_ENV).as_deref() == Ok("nested")
    {
        return Duration::from_secs(2);
    }
    PROBE_CLEANUP_RETRY
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
        let cleanup = release_probe_owner(&mut owner);
        return ProbeResponse {
            success: false,
            timed_out: false,
            output_limited: false,
            exit_code: None,
            group_empty: cleanup,
            message: format!("cannot persist input probe process identity: {error}"),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
    }
    let stdout_path = owner_path.with_file_name(PROBE_STDOUT_CAPTURE_FILE);
    let stderr_path = owner_path.with_file_name(PROBE_STDERR_CAPTURE_FILE);
    let stdout_file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stdout_path)
    {
        Ok(file) => file,
        Err(error) => {
            let group_empty = release_probe_owner(&mut owner);
            return ProbeResponse {
                success: false,
                timed_out: false,
                output_limited: false,
                exit_code: None,
                group_empty,
                message: format!("cannot create bounded input probe stdout capture: {error}"),
                stdout: Vec::new(),
                stderr: Vec::new(),
            };
        }
    };
    let stderr_file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&stderr_path)
    {
        Ok(file) => file,
        Err(error) => {
            let _ = fs::remove_file(&stdout_path);
            let group_empty = release_probe_owner(&mut owner);
            return ProbeResponse {
                success: false,
                timed_out: false,
                output_limited: false,
                exit_code: None,
                group_empty,
                message: format!("cannot create bounded input probe stderr capture: {error}"),
                stdout: Vec::new(),
                stderr: Vec::new(),
            };
        }
    };
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
            let group_empty = release_probe_owner(&mut owner);
            return ProbeResponse {
                success: false,
                timed_out: false,
                output_limited: false,
                exit_code: None,
                group_empty,
                message: format!("unable to start input probe: {error}"),
                stdout: Vec::new(),
                stderr: Vec::new(),
            };
        }
    };
    let (Some(stdout), Some(stderr)) = (child.stdout.take(), child.stderr.take()) else {
        let status = stop_probe_command(&mut child, &owner);
        let group_empty = release_probe_owner(&mut owner);
        return ProbeResponse {
            success: false,
            timed_out: false,
            output_limited: false,
            exit_code: status.code(),
            group_empty,
            message: "input probe child pipes were unavailable".into(),
            stdout: Vec::new(),
            stderr: Vec::new(),
        };
    };
    let mut stdout_capture = match swarm_process::capture::CaptureStream::new(
        stdout_path.clone(),
        stdout_file,
        stdout,
        request.stdout_limit as u64,
    ) {
        Ok(capture) => capture,
        Err(_) => {
            let status = stop_probe_command(&mut child, &owner);
            let group_empty = release_probe_owner(&mut owner);
            return ProbeResponse {
                success: false,
                timed_out: false,
                output_limited: false,
                exit_code: status.code(),
                group_empty,
                message: "input probe stdout capture could not be made pollable".into(),
                stdout: Vec::new(),
                stderr: Vec::new(),
            };
        }
    };
    let mut stderr_capture = match swarm_process::capture::CaptureStream::new(
        stderr_path.clone(),
        stderr_file,
        stderr,
        request.stderr_limit as u64,
    ) {
        Ok(capture) => capture,
        Err(_) => {
            let status = stop_probe_command(&mut child, &owner);
            let group_empty = release_probe_owner(&mut owner);
            return ProbeResponse {
                success: false,
                timed_out: false,
                output_limited: false,
                exit_code: status.code(),
                group_empty,
                message: "input probe stderr capture could not be made pollable".into(),
                stdout: Vec::new(),
                stderr: Vec::new(),
            };
        }
    };

    let deadline = Instant::now() + Duration::from_millis(request.timeout_ms);
    let mut timed_out = false;
    let mut output_limited = false;
    let mut status_observation_failed = false;
    let mut next_cancel = Instant::now();
    let exit_status = loop {
        stdout_capture.poll();
        stderr_capture.poll();
        output_limited |= stdout_capture.truncated() || stderr_capture.truncated();
        let capture_failed = stdout_capture.reader_failed() || stderr_capture.reader_failed();
        if !timed_out && Instant::now() >= deadline {
            timed_out = true;
        }
        let cancel_requested = timed_out || output_limited || capture_failed;
        if cancel_requested && Instant::now() >= next_cancel {
            let _ = owner.group.cancel_children();
            next_cancel = Instant::now() + PROBE_CLEANUP_RETRY;
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {}
            Err(_) => {
                status_observation_failed = true;
                let _ = owner.group.cancel_children();
                next_cancel = Instant::now() + PROBE_CLEANUP_RETRY;
            }
        }
        thread::sleep(Duration::from_millis(10));
    };
    let mut had_descendants = false;
    let mut membership_diagnostic = None;
    let group_empty = loop {
        stdout_capture.poll();
        stderr_capture.poll();
        output_limited |= stdout_capture.truncated() || stderr_capture.truncated();
        match owner.group.children_empty() {
            Ok(empty) => {
                had_descendants |= !empty;
                #[cfg(test)]
                if !empty {
                    record_probe_custody_member_observed_for_test(&owner, exit_status.code());
                }
            }
            Err(error) => membership_diagnostic = Some(cleanup_error_diagnostic(&error)),
        }
        if release_probe_owner_once(&mut owner)
            && stdout_capture.is_finished()
            && stderr_capture.is_finished()
        {
            break true;
        }
        thread::sleep(Duration::from_millis(10));
    };
    let stdout_receipt = stdout_capture.finish();
    let stderr_receipt = stderr_capture.finish();
    output_limited |= stdout_receipt.truncated || stderr_receipt.truncated;
    let readers_ok = stdout_receipt.capture_complete
        && stderr_receipt.capture_complete
        && stdout_receipt.capture_error.is_none()
        && stderr_receipt.capture_error.is_none();
    let stdout = read_probe_capture(&stdout_path, request.stdout_limit).unwrap_or_default();
    let stderr = read_probe_capture(&stderr_path, request.stderr_limit).unwrap_or_default();
    let exit_code = exit_status.code();
    let child_succeeded = exit_status.success();
    let membership_failed = membership_diagnostic.is_some();
    let success = group_empty
        && !timed_out
        && !output_limited
        && !had_descendants
        && !membership_failed
        && !status_observation_failed
        && readers_ok
        && child_succeeded;
    ProbeResponse {
        success,
        timed_out,
        output_limited,
        exit_code,
        group_empty,
        message: if let Some(membership) = membership_diagnostic {
            format!("input probe process group membership check failed; {membership}")
        } else if had_descendants {
            "input probe left descendants after its command exited".into()
        } else if status_observation_failed {
            "input probe direct exit observation recovered after a transient status error".into()
        } else if !readers_ok {
            "input probe output capture ended without natural EOF or complete bytes".into()
        } else if !group_empty {
            "input probe process group could not be released".into()
        } else {
            String::new()
        },
        stdout,
        stderr,
    }
}

fn stop_probe_command(child: &mut Child, owner: &ProbeOwner) -> ExitStatus {
    let mut next_cancel = Instant::now();
    loop {
        if Instant::now() >= next_cancel {
            let _ = owner.group.cancel_children();
            next_cancel = Instant::now() + PROBE_CLEANUP_RETRY;
        }
        match child.try_wait() {
            Ok(Some(status)) => return status,
            Ok(None) | Err(_) => thread::sleep(Duration::from_millis(10)),
        }
    }
}

fn release_probe_owner(owner: &mut ProbeOwner) -> bool {
    loop {
        if release_probe_owner_once(owner) {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn release_probe_owner_once(owner: &mut ProbeOwner) -> bool {
    owner.release().is_ok()
}

fn read_probe_capture(path: &Path, limit: usize) -> Result<Vec<u8>> {
    read_bounded(File::open(path)?, limit)
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
    probe: &mut ProbeContext,
) -> Result<String> {
    let mut args = prefix.to_vec();
    args.push("--version".into());
    let output = probe.run(program, &args, env, cwd)?;
    Ok(String::from_utf8(output)
        .map_err(|_| Error::new("CHECK_INPUT_RESOLUTION", "tool version output is not UTF-8"))?
        .trim()
        .to_string())
}

fn rustc_identity(
    cargo_args: &[String],
    env: &BTreeMap<String, String>,
    cwd: &Path,
    probe: &mut ProbeContext,
) -> Result<Value> {
    let explicit_cargo_toolchain = prefix_args(cargo_args);
    if let Some(rustc_value) = env
        .iter()
        .find(|(key, _)| env_key_eq(key, "RUSTC"))
        .map(|(_, value)| value)
    {
        let rustc = PathBuf::from(rustc_value);
        let rustc = executable(&rustc, env)?;
        let version = probe.run(&rustc, &["--version".into(), "--verbose".into()], env, cwd)?;
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
        let selected = probe.run(&rustup, &args, env, cwd)?;
        let args = vec![
            "which".into(),
            "--toolchain".into(),
            toolchain.into(),
            "rustc".into(),
        ];
        let path = probe.run(&rustup, &args, env, cwd)?;
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
    let version = probe.run(&rustc, &["--version".into(), "--verbose".into()], env, cwd)?;
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
    probe: &mut ProbeContext,
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
    let bytes = probe.run(executable, &args, env, &source.directory)?;
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

// Physical layout version 1 used nested profile/candidate digests; version 2
// uses the compact digest of the unchanged semantic execution_workspace identity.
const EXECUTION_WORKSPACE_LAYOUT_VERSION: u32 = 2;
const LEGACY_INPUT_FINGERPRINT_VERSION: u32 = 1;
const INPUT_FINGERPRINT_VERSION: u32 = 2;

fn fingerprint(resolved_inputs: &Value, scope_plan: &ScopePlan) -> Result<String> {
    // Keep the version-1 preimage for durable plans written before physical
    // layout provenance was recorded. New plans hash both the version-2
    // preimage and the separate layout descriptor.
    let layout = resolved_inputs.get("execution_layout");
    let version = if layout.is_some() {
        INPUT_FINGERPRINT_VERSION
    } else {
        LEGACY_INPUT_FINGERPRINT_VERSION
    };
    let mut fingerprint_input = json!({
        "version": version,
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
    if let Some(layout) = layout {
        fingerprint_input["execution_layout"] = layout.clone();
    }
    Ok(model::digest(
        model::canonical(&fingerprint_input)?.as_bytes(),
    ))
}

/// Resolve source, Cargo reverse scope, actual argv, toolchain and environment
/// before Store admission. Artifact/Task/Attempt/Operation IDs never enter the
/// returned reusable identity.
pub(crate) fn resolve_with_cleanup(
    profile: &CheckProfile,
    candidate: &VerifiedSource,
    baseline: Option<&VerifiedSource>,
) -> (Result<ResolvedCheckPlan>, Option<ProbeCleanupOwner>) {
    #[cfg(test)]
    let mut probe = match take_probe_cleanup_override(profile) {
        Some((probe, owner)) => ProbeContext::test_with_cleanup(probe, owner),
        None => ProbeContext::production(),
    };
    #[cfg(not(test))]
    let mut probe = ProbeContext::production();
    let result = resolve_with_context(profile, candidate, baseline, &mut probe);
    let owner = probe.cleanup_owner;
    let result = if owner.as_ref().is_some_and(ProbeCleanupOwner::is_pending) {
        Err(Error::new(
            "CHECK_PROBE_CLEANUP_PENDING",
            "input probe helper custody remains pending in Store",
        ))
    } else {
        result
    };
    (result, owner)
}

#[cfg(test)]
fn resolve_with_probe(
    profile: &CheckProfile,
    candidate: &VerifiedSource,
    baseline: Option<&VerifiedSource>,
    probe: TestProbeFn,
) -> Result<ResolvedCheckPlan> {
    let mut probe = ProbeContext::test(probe);
    resolve_with_context(profile, candidate, baseline, &mut probe)
}

fn resolve_with_context(
    profile: &CheckProfile,
    candidate: &VerifiedSource,
    baseline: Option<&VerifiedSource>,
    probe: &mut ProbeContext,
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
    resolved_inputs["execution_layout"] = json!({
        "version": EXECUTION_WORKSPACE_LAYOUT_VERSION,
    });
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
pub(crate) mod tests {
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
            if fs::create_dir(&root).is_ok()
                && let Ok(root) = fs::canonicalize(root)
            {
                return root;
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
    fn execution_paths_for_layout_selects_exact_legacy_and_compact_paths() {
        let data_dir = Path::new("layout-data");
        let mut profile = profile(true);
        profile.resource = "Checks".into();
        let candidate = source(&model::digest(b"candidate-content"), "artifact-a");
        let base = data_dir.join("check-inputs").join("checks");
        let legacy_parent = base.join(profile_identity_sha256(&profile).unwrap());
        let legacy = execution_paths_for_layout(data_dir, &profile, &candidate, 1).unwrap();
        assert_eq!(
            legacy,
            (
                legacy_parent.join(&candidate.content_sha256),
                legacy_parent.join(format!("{}.candidate.json", candidate.content_sha256)),
            )
        );

        let workspace_key = model::digest(
            model::canonical(&execution_workspace_identity(&profile, &candidate).unwrap())
                .unwrap()
                .as_bytes(),
        );
        let compact = execution_paths_for_layout(data_dir, &profile, &candidate, 2).unwrap();
        assert_eq!(
            compact,
            (
                base.join(&workspace_key),
                base.join(format!("{workspace_key}.candidate.json")),
            )
        );
        assert_eq!(compact.0.file_name().unwrap().to_str().unwrap().len(), 64);
        assert_eq!(
            execution_paths(data_dir, &profile, &candidate).unwrap(),
            compact
        );
        assert_ne!(legacy, compact);

        for layout_version in [0, 3, u32::MAX] {
            assert_eq!(
                execution_paths_for_layout(data_dir, &profile, &candidate, layout_version)
                    .unwrap_err()
                    .code,
                "CHECK_INPUTS_STALE"
            );
        }
        for content_hash in ["short".to_owned(), "g".repeat(64)] {
            let malformed = source(&content_hash, "artifact-invalid");
            for layout_version in [1, 2] {
                assert_eq!(
                    execution_paths_for_layout(data_dir, &profile, &malformed, layout_version)
                        .unwrap_err()
                        .code,
                    "CHECK_INPUTS_STALE"
                );
            }
        }
    }

    #[test]
    fn execution_paths_for_layout_keeps_profile_resource_and_content_namespaces_separate() {
        let data_dir = Path::new("layout-data");
        let base_profile = profile(true);
        let candidate = source(&model::digest(b"candidate-content"), "artifact-a");
        let recaptured = source(&candidate.content_sha256, "artifact-b");
        let changed_candidate = source(&model::digest(b"changed-content"), "artifact-c");
        let mut revised_profile = base_profile.clone();
        revised_profile.profile_revision = "v2".into();
        let mut other_resource = base_profile.clone();
        other_resource.resource = "other-checks".into();
        let mut case_changed_resource = base_profile.clone();
        case_changed_resource.resource = "CHECKS".into();

        let mut paths = BTreeSet::new();
        for layout_version in [1, 2] {
            assert_eq!(
                execution_paths_for_layout(data_dir, &base_profile, &candidate, layout_version)
                    .unwrap(),
                execution_paths_for_layout(data_dir, &base_profile, &recaptured, layout_version)
                    .unwrap()
            );
            for (profile, candidate) in [
                (&base_profile, &candidate),
                (&revised_profile, &candidate),
                (&other_resource, &candidate),
                (&case_changed_resource, &candidate),
                (&base_profile, &changed_candidate),
            ] {
                let (workspace, descriptor) =
                    execution_paths_for_layout(data_dir, profile, candidate, layout_version)
                        .unwrap();
                assert!(paths.insert(workspace), "workspace namespace collision");
                assert!(paths.insert(descriptor), "descriptor namespace collision");
            }
        }
    }

    #[test]
    fn probe_cleanup_scope_holds_overlapping_or_unidentified_targets_only() {
        let base_profile = profile(true);
        let candidate = source(&model::digest(b"candidate-content"), "artifact-a");
        let retained = ProbeCleanupScope::for_candidate(&base_profile, &candidate).unwrap();
        assert_eq!(
            retained.workspace_identity,
            execution_workspace_identity(&base_profile, &candidate).unwrap()
        );

        let mut other_profile_same_resource = base_profile.clone();
        other_profile_same_resource.profile_id = "other-profile".into();
        let same_resource =
            ProbeCleanupScope::for_candidate(&other_profile_same_resource, &candidate).unwrap();
        assert!(retained.overlaps(&same_resource));

        let mut independent_profile = base_profile.clone();
        independent_profile.resource = "other-checks".into();
        let independent =
            ProbeCleanupScope::for_candidate(&independent_profile, &candidate).unwrap();
        assert!(!retained.overlaps(&independent));

        let same_workspace_different_resource = ProbeCleanupScope {
            resource: "other-checks".into(),
            workspace_identity: retained.workspace_identity.clone(),
        };
        assert!(retained.overlaps(&same_workspace_different_resource));
        assert!(!probe_scopes_conflict(Some(&retained), Some(&independent)));
        assert!(probe_scopes_conflict(None, Some(&retained)));
        assert!(probe_scopes_conflict(Some(&retained), None));
        assert!(!probe_resource_conflicts(
            Some(&retained),
            Some("other-checks")
        ));
        assert!(probe_resource_conflicts(Some(&retained), Some("checks")));
        assert!(probe_resource_conflicts(None, Some("other-checks")));
        assert!(probe_resource_conflicts(Some(&retained), None));
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
    fn execution_workspace_layout_version_invalidates_reusable_cache_provenance() {
        let profile = profile(true);
        let candidate = source("same-content", "artifact-a");
        let plan = resolve_test(&profile, &candidate, None);
        assert_eq!(plan.resolved_inputs["cache_reusable"], true);

        let mut legacy_inputs = plan.resolved_inputs.clone();
        legacy_inputs
            .as_object_mut()
            .unwrap()
            .remove("execution_layout");
        let legacy_fingerprint = fingerprint(&legacy_inputs, &plan.scope_plan).unwrap();

        assert_ne!(legacy_fingerprint, plan.input_fingerprint);
        assert_eq!(
            plan.resolved_inputs["execution_workspace"],
            execution_workspace_identity(&profile, &candidate).unwrap()
        );
        assert_eq!(plan.resolved_inputs["execution_workspace"]["version"], 1);
        assert_eq!(
            plan.resolved_inputs["execution_layout"],
            json!({"version": EXECUTION_WORKSPACE_LAYOUT_VERSION})
        );
        assert!(legacy_inputs.get("execution_layout").is_none());
        assert_eq!(
            plan.resolved_inputs["input_fingerprint"],
            json!(plan.input_fingerprint)
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

    const PROBE_CUSTODY_REQUEST_ENV: &str = "ELIOT_PROBE_CUSTODY_REQUEST";
    const PROBE_CUSTODY_TARGET_ENV: &str = "ELIOT_PROBE_CUSTODY_TARGET_ENTRY";
    const PROBE_CUSTODY_DESCENDANT_ENV: &str = "ELIOT_PROBE_CUSTODY_DESCENDANT_ENTRY";

    fn probe_custody_file(control: &Path, name: &str) -> PathBuf {
        control.join(name)
    }

    fn write_probe_custody_identity(path: &Path, pid: u32) {
        let identity = crate::platform::process_group::process_birth_identity(pid)
            .expect("observe process birth identity")
            .expect("process must be live when its identity is recorded");
        fs::write(
            path,
            serde_json::to_vec(&identity).expect("serialize process birth identity"),
        )
        .expect("persist process birth identity");
    }

    fn read_probe_custody_identity(path: &Path) -> Value {
        serde_json::from_slice(&fs::read(path).expect("read process birth identity"))
            .expect("decode process birth identity")
    }

    fn wait_probe_custody_file(path: &Path, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while !path.exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        path.exists()
    }

    #[test]
    fn probe_nested_helper_entry() {
        let Some(request) = std::env::var_os(PROBE_CUSTODY_REQUEST_ENV) else {
            return;
        };
        let result = run_probe(Path::new(&request));
        std::process::exit(if result.is_ok() { 0 } else { 1 });
    }

    #[test]
    fn probe_nested_target_entry() {
        if std::env::var(PROBE_CUSTODY_TARGET_ENV).as_deref() != Ok("1") {
            return;
        }
        let control = PathBuf::from(
            std::env::var_os(PROBE_CUSTODY_CONTROL_ENV).expect("probe custody control directory"),
        );
        write_probe_custody_identity(
            &probe_custody_file(&control, "target.identity.json"),
            std::process::id(),
        );
        let executable = std::env::current_exe().expect("test executable path");
        let descendant = Command::new(executable)
            .args([
                "--exact",
                "checks::inputs::tests::probe_nested_descendant_entry",
                "--quiet",
                "--nocapture",
            ])
            .env(PROBE_CUSTODY_CONTROL_ENV, &control)
            .env(PROBE_CUSTODY_DESCENDANT_ENV, "1")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn real nested probe descendant");
        write_probe_custody_identity(
            &probe_custody_file(&control, "descendant.identity.json"),
            descendant.id(),
        );
        assert!(wait_probe_custody_file(
            &probe_custody_file(&control, "descendant.ready"),
            Duration::from_secs(5)
        ));
        fs::write(probe_custody_file(&control, "target.ready"), b"ready")
            .expect("publish target readiness");
        assert!(wait_probe_custody_file(
            &probe_custody_file(&control, "target.exit"),
            Duration::from_secs(60)
        ));
        std::io::stdout()
            .write_all(b"probe-nested-direct-output\n")
            .expect("write direct probe bytes");
        std::process::exit(0);
    }

    #[test]
    fn probe_nested_descendant_entry() {
        if std::env::var(PROBE_CUSTODY_DESCENDANT_ENV).as_deref() != Ok("1") {
            return;
        }
        let control = PathBuf::from(
            std::env::var_os(PROBE_CUSTODY_CONTROL_ENV).expect("probe custody control directory"),
        );
        fs::write(probe_custody_file(&control, "descendant.ready"), b"ready")
            .expect("publish nested descendant readiness");
        assert!(wait_probe_custody_file(
            &probe_custody_file(&control, "descendant.exit"),
            Duration::from_secs(60)
        ));
        std::io::stdout()
            .write_all(b"probe-nested-descendant-output\n")
            .expect("write nested descendant bytes");
    }

    pub(crate) struct ProbeCustodyFixture {
        directory: PathBuf,
        control: PathBuf,
        helper_pid: u32,
        helper_launch_identity: Option<Value>,
        helper_family_token: Option<String>,
        #[cfg(windows)]
        helper_family_query: Option<Group>,
    }

    impl ProbeCustodyFixture {
        pub(crate) fn helper_pid(&self) -> u32 {
            self.helper_pid
        }

        pub(crate) fn helper_launch_identity(&self) -> &Value {
            self.helper_launch_identity
                .as_ref()
                .expect("capture helper launch identity before handing off custody")
        }

        pub(crate) fn process_identity(&self, name: &str) -> Value {
            read_probe_custody_identity(&probe_custody_file(&self.control, name))
        }

        pub(crate) fn process_departed(&self, identity: &Value) -> bool {
            if identity["scope"] == "launcher_spawned_process" {
                let Some(token) = self.helper_family_token.as_deref() else {
                    return false;
                };
                return crate::platform::process_group::spawned_departed(identity, token)
                    .is_ok_and(|departed| departed);
            }
            let Some(pid) = identity["pid"]
                .as_u64()
                .and_then(|pid| u32::try_from(pid).ok())
            else {
                return false;
            };
            crate::platform::process_group::process_birth_identity(pid)
                .is_ok_and(|current| current.as_ref() != Some(identity))
        }

        pub(crate) fn release_target(&self) {
            fs::write(probe_custody_file(&self.control, "target.exit"), b"exit")
                .expect("allow direct probe process to exit");
        }

        pub(crate) fn release_descendant(&self) {
            fs::write(
                probe_custody_file(&self.control, "descendant.exit"),
                b"exit",
            )
            .expect("allow nested process to exit");
        }

        pub(crate) fn release_all(&self) {
            self.release_target();
            self.release_descendant();
        }

        fn cleanup(&self) {
            self.release_all();
            let owner_path = self.directory.join("probe-owner.json");
            let deadline = Instant::now() + Duration::from_secs(8);
            let mut family_empty = false;
            while Instant::now() < deadline {
                if let Ok(bytes) = fs::read(&owner_path) {
                    if let Ok(identity) = serde_json::from_slice::<Value>(&bytes)
                        && let Ok(token) = probe_group_token(&identity)
                        && departed_empty(&identity, &token).is_ok_and(|empty| empty)
                    {
                        family_empty = true;
                        break;
                    }
                } else if !owner_path.exists()
                    && crate::platform::process_group::process_birth_identity(self.helper_pid)
                        .is_ok_and(|identity| identity.is_none())
                {
                    family_empty = true;
                    break;
                }
                thread::sleep(Duration::from_millis(10));
            }
            if family_empty {
                let _ = fs::remove_dir_all(&self.directory);
                let _ = fs::remove_dir_all(&self.control);
            }
        }
    }

    impl Drop for ProbeCustodyFixture {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[cfg(any(windows, target_os = "linux"))]
    pub(crate) fn pending_probe_custody_fixture(
        scope: ProbeCleanupScope,
    ) -> (ProbeCleanupOwner, ProbeCustodyFixture) {
        let directory =
            std::env::temp_dir().join(format!("swarm-probe-custody-{}", model::new_id()));
        let control = std::env::temp_dir().join(format!("swarm-probe-control-{}", model::new_id()));
        fs::create_dir(&directory).expect("create probe protocol directory");
        fs::create_dir(&control).expect("create probe control directory");
        let request_path = directory.join("request.json");
        let response_path = directory.join(PROBE_HELPER_RESPONSE_FILE);
        let request = ProbeRequest {
            probe_version: PROBE_VERSION,
            program: std::env::current_exe().expect("test executable path"),
            args: vec![
                "--exact".into(),
                "checks::inputs::tests::probe_nested_target_entry".into(),
                "--quiet".into(),
                "--nocapture".into(),
            ],
            cwd: std::env::current_dir().expect("test working directory"),
            timeout_ms: PROBE_TIMEOUT_MS,
            stdout_limit: PROBE_STDOUT_LIMIT,
            stderr_limit: PROBE_STDERR_LIMIT,
        };
        fs::write(
            &request_path,
            model::canonical(&json!({"check_probe":request})).expect("encode nested probe request"),
        )
        .expect("write nested probe request");
        let response_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&response_path)
            .expect("create helper response capture");
        let executable = std::env::current_exe().expect("test executable path");
        let mut child = Command::new(executable)
            .args([
                "--exact",
                "checks::inputs::tests::probe_nested_helper_entry",
                "--quiet",
                "--nocapture",
            ])
            .env(PROBE_CUSTODY_REQUEST_ENV, &request_path)
            .env(PROBE_CUSTODY_CONTROL_ENV, &control)
            .env(PROBE_CUSTODY_TARGET_ENV, "1")
            .env("ELIOT_PROBE_CUSTODY_FIXTURE_GRACE", "nested")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn real CheckRunner probe helper");
        let helper_pid = child.id();
        let mut fixture = ProbeCustodyFixture {
            directory: directory.clone(),
            control: control.clone(),
            helper_pid,
            helper_launch_identity: None,
            helper_family_token: None,
            #[cfg(windows)]
            helper_family_query: None,
        };
        let helper_launch_identity =
            spawned_identity(helper_pid).expect("capture helper launch identity");
        fixture.helper_launch_identity = Some(helper_launch_identity.clone());
        let stdout = child.stdout.take().expect("capture helper stdout pipe");
        let custody = ProbeHelperCustody {
            child,
            child_pid: helper_pid,
            launch_identity: Some(helper_launch_identity.clone()),
            stdout: Some(stdout),
            response_file: Some(response_file),
            capture: None,
            directory: directory.clone(),
            family_identity: None,
            family_token: None,
            #[cfg(windows)]
            family_query: None,
            exit_status: None,
            status_observation_failed: false,
            capture_setup_failed: false,
            response_group_empty: None,
            family_departed_proven: false,
            cleanup_complete: false,
        };
        let mut owner = ProbeCleanupOwner {
            custody,
            scope: Some(scope),
        };
        let target_identity_path = probe_custody_file(&control, "target.identity.json");
        let descendant_identity_path = probe_custody_file(&control, "descendant.identity.json");
        let target_ready_path = probe_custody_file(&control, "target.ready");
        let identities_deadline = Instant::now() + Duration::from_secs(10);
        while (!target_identity_path.exists()
            || !descendant_identity_path.exists()
            || !target_ready_path.exists())
            && Instant::now() < identities_deadline
        {
            owner.poll();
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            target_identity_path.exists()
                && descendant_identity_path.exists()
                && target_ready_path.exists(),
            "real target and nested process publish identities and become ready"
        );
        assert!(
            owner.custody.capture.is_some() && !owner.custody.capture_setup_failed,
            "the real helper stdout capture is active at Store handoff"
        );
        assert!(
            owner.is_pending(),
            "live nested helper custody stays pending"
        );
        fixture.helper_family_token = owner.custody.family_token.clone();
        assert!(
            fixture.helper_family_token.is_some(),
            "capture the helper's exact native-family token before Store handoff"
        );
        #[cfg(windows)]
        {
            // Store may release its owner before this fixture independently
            // checks departure. Keep that exact Job observable throughout both
            // observations instead of treating a vanished name as empty.
            fixture.helper_family_query = Some(
                Group::open_check_query(
                    owner
                        .custody
                        .family_identity
                        .as_ref()
                        .expect("helper family"),
                    fixture
                        .helper_family_token
                        .as_deref()
                        .expect("helper token"),
                )
                .expect("retain the fixture's exact helper Job observation"),
            );
        }
        (owner, fixture)
    }

    #[cfg(any(windows, target_os = "linux"))]
    #[test]
    fn probe_custody_keeps_nested_family_and_capture_until_exact_departure() {
        let profile = profile(false);
        let candidate = source(
            &model::digest(b"probe-custody-candidate"),
            "probe-custody-candidate",
        );
        let scope = ProbeCleanupScope::for_candidate(&profile, &candidate)
            .expect("derive exact CheckRunner resource/workspace scope");
        let (mut owner, fixture) = pending_probe_custody_fixture(scope.clone());
        let directory = fixture.directory.clone();
        let control = fixture.control.clone();
        let helper_pid = fixture.helper_pid();
        let launch_identity = fixture.helper_launch_identity().clone();
        let response_path = directory.join(PROBE_HELPER_RESPONSE_FILE);

        let target_identity_path = probe_custody_file(&control, "target.identity.json");
        let descendant_identity_path = probe_custody_file(&control, "descendant.identity.json");
        let target_ready_path = probe_custody_file(&control, "target.ready");
        let identities_deadline = Instant::now() + Duration::from_secs(10);
        while (!target_identity_path.exists()
            || !descendant_identity_path.exists()
            || !target_ready_path.exists())
            && Instant::now() < identities_deadline
        {
            owner.poll();
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            target_identity_path.exists(),
            "target process published its exact identity"
        );
        assert!(
            descendant_identity_path.exists(),
            "nested process published its exact identity"
        );
        assert!(
            target_ready_path.exists(),
            "target and nested process are ready"
        );
        let target_identity = read_probe_custody_identity(&target_identity_path);
        let descendant_identity = read_probe_custody_identity(&descendant_identity_path);
        let target_pid = target_identity["pid"].as_u64().unwrap() as u32;
        let descendant_pid = descendant_identity["pid"].as_u64().unwrap() as u32;
        assert_eq!(
            crate::platform::process_group::process_birth_identity(descendant_pid)
                .expect("observe nested process identity"),
            Some(descendant_identity.clone()),
            "the nested process is a live, exact native process instance"
        );

        fs::write(probe_custody_file(&control, "target.exit"), b"exit")
            .expect("allow direct probe process to exit");
        let target_exit_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            owner.poll();
            let current = crate::platform::process_group::process_birth_identity(target_pid)
                .expect("observe direct process exit");
            if current.as_ref() != Some(&target_identity) {
                break;
            }
            assert!(
                Instant::now() < target_exit_deadline,
                "direct probe process must exit"
            );
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(
            crate::platform::process_group::process_birth_identity(descendant_pid)
                .expect("observe nested process remains live"),
            Some(descendant_identity.clone()),
            "the nested process outlives the observed direct exit"
        );
        let family_identity = owner
            .custody
            .family_identity
            .as_ref()
            .expect("retain helper's native family identity")
            .clone();
        let family_token = owner
            .custody
            .family_token
            .as_deref()
            .expect("retain helper's native family token")
            .to_owned();
        assert!(
            !departed_empty(&family_identity, &family_token).expect("observe live helper family")
        );
        let member_observed_path = probe_custody_file(&control, PROBE_CUSTODY_MEMBER_OBSERVED_FILE);
        assert!(
            wait_probe_custody_file(&member_observed_path, Duration::from_secs(5)),
            "real helper observes its native family nonempty after direct-child exit"
        );
        let member_observed: Value =
            serde_json::from_slice(&fs::read(member_observed_path).expect("read helper receipt"))
                .expect("decode helper receipt");
        assert_eq!(
            member_observed["probe_group_identity"], family_identity,
            "the helper receipt is tied to the retained native-family identity"
        );
        assert_eq!(
            member_observed["group_member_observed_after_direct_exit"],
            true
        );
        assert_eq!(member_observed["direct_child_exit_code"], 0);
        assert_eq!(
            crate::platform::process_group::process_birth_identity(descendant_pid)
                .expect("observe exact descendant after helper observation")
                .as_ref(),
            Some(&descendant_identity),
            "the exact descendant remains live when the helper observes its family"
        );
        assert!(
            owner.is_pending(),
            "live nested process retains CheckRunner custody"
        );
        assert_eq!(owner.scope(), Some(&scope));
        assert!(
            owner.conflicts_with(None),
            "unidentified incoming scope fails closed"
        );
        let mut same_resource_profile = profile.clone();
        same_resource_profile.profile_id = "different-profile".into();
        let same_resource_scope =
            ProbeCleanupScope::for_candidate(&same_resource_profile, &candidate)
                .expect("derive same-resource scope");
        assert!(owner.conflicts_with(Some(&same_resource_scope)));
        let mut independent_profile = profile.clone();
        independent_profile.resource = "independent-checks".into();
        let independent_scope = ProbeCleanupScope::for_candidate(&independent_profile, &candidate)
            .expect("derive independent resource scope");
        assert!(!owner.conflicts_with(Some(&independent_scope)));
        owner.poll();

        fs::write(probe_custody_file(&control, "descendant.exit"), b"exit")
            .expect("allow nested process to exit");
        let cleanup_deadline = Instant::now() + Duration::from_secs(10);
        while owner.is_pending() && Instant::now() < cleanup_deadline {
            owner.poll();
            if owner.is_pending() {
                thread::sleep(Duration::from_millis(5));
            }
        }
        assert!(
            !owner.is_pending(),
            "capture EOF and native family departure release custody"
        );
        let helper_status = owner
            .custody
            .exit_status
            .as_ref()
            .expect("observe helper exit");
        assert!(
            helper_status.success(),
            "helper exit status is observed and successful: {helper_status}"
        );
        assert!(owner.custody.family_departed_proven);
        assert!(
            departed_empty(&family_identity, &family_token)
                .expect("observe exact family departure")
        );
        assert!(
            crate::platform::process_group::process_birth_identity(descendant_pid)
                .expect("observe nested process departure")
                .as_ref()
                != Some(&descendant_identity)
        );
        let capture = owner
            .custody
            .capture
            .take()
            .expect("retain helper capture receipt");
        let capture_receipt = capture.finish();
        assert!(capture_receipt.capture_complete);
        assert!(!capture_receipt.truncated);
        assert!(capture_receipt.capture_error.is_none());
        let response_bytes = read_bounded(
            File::open(&response_path).expect("open captured probe response"),
            PROBE_RESPONSE_LIMIT,
        )
        .expect("read bounded probe response");
        // libtest writes a runner preamble to the helper stdout around the real
        // protocol frame; isolate that frame for assertions while leaving owner
        // classification fail-closed on the unmodified captured bytes.
        let frame_start = response_bytes
            .windows(PROBE_MAGIC.len())
            .position(|window| window == PROBE_MAGIC)
            .expect("find actual helper response frame in captured bytes");
        const RESPONSE_HEADER_LEN: usize = 8 + 1 + 4 + 4 + 4 + 2;
        let header_end = frame_start + RESPONSE_HEADER_LEN;
        assert!(header_end <= response_bytes.len());
        let stdout_len = u32::from_le_bytes(
            response_bytes[frame_start + 13..frame_start + 17]
                .try_into()
                .unwrap(),
        ) as usize;
        let stderr_len = u32::from_le_bytes(
            response_bytes[frame_start + 17..frame_start + 21]
                .try_into()
                .unwrap(),
        ) as usize;
        let message_len = u16::from_le_bytes(
            response_bytes[frame_start + 21..frame_start + 23]
                .try_into()
                .unwrap(),
        ) as usize;
        let frame_end = header_end + stdout_len + stderr_len + message_len;
        assert_eq!(
            frame_end,
            response_bytes.len(),
            "response frame consumes the helper tail"
        );
        let response = decode_probe_response(&response_bytes[frame_start..frame_end])
            .expect("decode actual nested probe response");
        assert_eq!(
            owner.custody.response_group_empty, None,
            "unframed helper bytes do not become cleanup proof"
        );
        assert!(
            !response.success,
            "a direct exit with a live descendant is not clean success"
        );
        assert_eq!(response.exit_code, Some(0));
        assert!(response.group_empty);
        assert!(
            response
                .message
                .contains("left descendants after its command exited")
        );
        assert!(String::from_utf8_lossy(&response.stdout).contains("probe-nested-direct-output"));
        assert!(
            String::from_utf8_lossy(&response.stdout).contains("probe-nested-descendant-output")
        );
        let malformed_frame_error = decode_probe_response(&response_bytes[..frame_start])
            .expect_err("the libtest preamble remains an invalid protocol frame");
        assert_eq!(malformed_frame_error.code, "CHECK_INPUT_RESOLUTION");
        assert!(
            owner.custody.response_group_empty.is_none(),
            "a malformed full stream supplies no inner-family departure receipt"
        );
        let temp_guard = TempProbeDir(directory.clone(), true);
        let evidence = json!({
            "helper_pid":helper_pid,
            "helper_launch_identity":launch_identity,
            "family_identity":family_identity,
            "family_token":family_token,
            "target_identity":target_identity,
            "descendant_identity":descendant_identity,
            "target_exit_observed_before_descendant_release":true,
            "descendant_departed":true,
            "probe_exit_code":response.exit_code,
            "helper_exit_code":helper_status.code(),
            "capture_bytes_written":capture_receipt.bytes_written,
            "capture_bytes_observed":capture_receipt.bytes_observed,
            "capture_sha256":capture_receipt.sha256,
            "capture_complete":capture_receipt.capture_complete,
            "capture_truncated":capture_receipt.truncated,
            "owner_pending_before_release":true,
            "owner_pending_after_release":owner.is_pending(),
            "unknown_full_stream_response_disposition":owner.custody.response_group_empty.is_none(),
            "malformed_frame_is_error":true,
            "malformed_frame_temp_evidence_preserved":temp_guard.1,
            "resource":scope.resource.clone(),
            "workspace_identity":scope.workspace_identity.clone(),
        });
        eprintln!("probe_custody_evidence={evidence}");
        drop(owner);
        drop(temp_guard);
        assert!(
            directory.exists(),
            "malformed response preserves private custody evidence"
        );
        drop(fixture);
        assert!(
            !directory.exists(),
            "the real fixture cleans up after known family departure"
        );
    }
}
