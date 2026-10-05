use crate::{
    error::{Error, Result},
    model,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CheckConfig {
    pub enabled: bool,
    pub max_running: usize,
    pub git_executable: PathBuf,
    pub profiles: Vec<CheckProfile>,
    /// Optional operator-pinned, separately built process adapter. When absent
    /// the current in-process worker remains the migration-compatible path.
    /// A selected pin is verified on every launch and never falls back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<ExecutorPin>,
}
impl Default for CheckConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_running: 2,
            git_executable: "git".into(),
            profiles: Vec::new(),
            executor: None,
        }
    }
}

/// Identity pin for the standalone `swarm-checks` process. This is trusted
/// local controller configuration; a Manager cannot choose an executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutorPin {
    pub executable: PathBuf,
    pub sha256: String,
    pub artifact_id: String,
    pub version: String,
}

impl ExecutorPin {
    pub fn validate_shape(&self) -> Result<()> {
        if !self.executable.is_absolute()
            || !opaque_atom(&self.artifact_id)
            || !version_atom(&self.version)
            || !canonical_sha256(&self.sha256)
        {
            return Err(Error::invalid("standalone checks executor pin is invalid"));
        }
        Ok(())
    }
}

fn opaque_atom(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-+".contains(&byte))
}

fn version_atom(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._+-".contains(&byte))
}

fn canonical_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Parser {
    ExitCode,
    CargoJson,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckProfile {
    pub profile_id: String,
    pub profile_revision: String,
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub parser: Parser,
    /// Profiles sharing this name share one warm target directory, never concurrent writers.
    pub resource: String,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub inherit_env: Vec<String>,
    /// Cargo artifact target names, not guessed from a worker's claims.
    #[serde(default)]
    pub expected_targets: Vec<String>,
    /// Enables completed-result reuse only when every resolved input is versioned.
    /// Trusted profiles remain non-reusable unless they opt in explicitly.
    #[serde(default)]
    pub reproducible: bool,
    /// Environment names whose values the trusted profile explicitly declares
    /// non-secret and stable enough to include as digests in reusable identity.
    /// Other non-built-in environment inputs disable reuse without hashing or
    /// persisting their values.
    #[serde(default)]
    pub fingerprint_env: Vec<String>,
    /// Named, immutable identities for external inputs such as container images
    /// or service snapshots. Cargo reuse requires `build_environment` to attest
    /// external tools that CheckRunner does not discover individually. Values
    /// are fingerprinted by digest, never copied into the resolved receipt.
    #[serde(default)]
    pub versioned_inputs: BTreeMap<String, String>,
}
fn name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
impl CheckConfig {
    pub fn validate(&self) -> Result<()> {
        if self.max_running == 0 || self.git_executable.as_os_str().is_empty() {
            return Err(Error::invalid(
                "checks require max_running > 0 and git_executable",
            ));
        }
        if let Some(executor) = &self.executor {
            executor.validate_shape()?;
        }
        let mut ids = BTreeSet::new();
        for p in &self.profiles {
            if !name(&p.profile_id)
                || !name(&p.resource)
                || p.profile_revision.trim().is_empty()
                || p.executable.as_os_str().is_empty()
                || !ids.insert((&p.profile_id, &p.profile_revision))
            {
                return Err(Error::invalid(
                    "invalid or duplicate check profile identity/resource",
                ));
            }
            let mut env = BTreeSet::new();
            for key in p.environment.keys().chain(&p.inherit_env) {
                let upper = key.to_ascii_uppercase();
                let explicit_cargo_target = upper == "CARGO_TARGET_DIR"
                    && key == "CARGO_TARGET_DIR"
                    && p.parser == Parser::CargoJson
                    && p.environment.contains_key("CARGO_TARGET_DIR");
                if key.is_empty()
                    || key.contains(['=', '\0'])
                    || !env.insert(upper.clone())
                    || upper == "SWARM_CANDIDATE_FILE"
                    || (upper == "CARGO_TARGET_DIR" && !explicit_cargo_target)
                {
                    return Err(Error::invalid(
                        "duplicate, reserved or invalid check environment key",
                    ));
                }
            }
            if let Some(target_dir) = p.environment.get("CARGO_TARGET_DIR") {
                let target_path = PathBuf::from(target_dir);
                if !target_path.is_absolute()
                    || target_dir.chars().any(char::is_control)
                    || target_path
                        .components()
                        .any(|component| matches!(component, std::path::Component::ParentDir))
                {
                    return Err(Error::invalid(
                        "CARGO_TARGET_DIR must be an absolute normalized Cargo profile path",
                    ));
                }
            }
            if p.environment.values().any(|v| v.contains('\0'))
                || p.args.iter().any(|s| s.contains('\0'))
            {
                return Err(Error::invalid("check arguments/environment contain a NUL"));
            }
            let mut fingerprint_env = BTreeSet::new();
            for key in &p.fingerprint_env {
                let upper = key.to_ascii_uppercase();
                if key.is_empty()
                    || key.contains(['=', '\0'])
                    || !fingerprint_env.insert(upper.clone())
                    || upper == "SWARM_CANDIDATE_FILE"
                    || !p
                        .environment
                        .keys()
                        .chain(&p.inherit_env)
                        .any(|declared| declared.eq_ignore_ascii_case(key))
                {
                    return Err(Error::invalid(
                        "fingerprint_env must uniquely name declared, non-reserved environment inputs",
                    ));
                }
            }
            if p.versioned_inputs.iter().any(|(key, value)| {
                !name(key) || value.trim().is_empty() || value.len() > 512 || value.contains('\0')
            }) {
                return Err(Error::invalid(
                    "versioned check inputs require a valid name and a nonempty immutable identity of at most 512 bytes",
                ));
            }
            if p.parser == Parser::CargoJson {
                let command = p
                    .args
                    .iter()
                    .find(|s| !s.starts_with('+'))
                    .map(String::as_str);
                if !matches!(command, Some("build" | "check" | "clippy"))
                    || !p.args.iter().any(|s| s == "--message-format=json")
                    || !p.args.iter().any(|s| s == "--locked")
                    || p.expected_targets.is_empty()
                {
                    return Err(Error::invalid(
                        "cargo_json profiles require build/check/clippy, --locked, --message-format=json and expected_targets",
                    ));
                }
                if p.args
                    .iter()
                    .any(|s| s == "--target-dir" || s.starts_with("--target-dir="))
                {
                    return Err(Error::invalid(
                        "target directory is owned by the CheckRunner resource",
                    ));
                }
                if p.args.iter().any(|s| {
                    s == "--manifest-path"
                        || s.starts_with("--manifest-path=")
                        || s == "--config"
                        || s.starts_with("--config=")
                }) {
                    return Err(Error::invalid(
                        "Cargo checks use the captured workspace and its captured configuration",
                    ));
                }
            }
        }
        Ok(())
    }
    pub fn profile(&self, id: &str, revision: &str) -> Result<CheckProfile> {
        if !self.enabled {
            return Err(Error::new(
                "CHECKS_DISABLED",
                "enable checks in the local controller configuration",
            ));
        }
        self.profiles
            .iter()
            .find(|p| p.profile_id == id && p.profile_revision == revision)
            .cloned()
            .ok_or_else(|| Error::new("CHECK_PROFILE_NOT_FOUND", "unknown check profile/revision"))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureRequest {
    pub client_request_id: String,
    pub attempt_id: String,
    pub expected_revision: i64,
    pub repository: PathBuf,
    pub commit: String,
}
impl CaptureRequest {
    pub fn parse(v: &Value) -> Result<Self> {
        let r: Self = serde_json::from_value(v.clone())?;
        model::text(v, "client_request_id")?;
        model::text(v, "attempt_id")?;
        if r.expected_revision < 1
            || !r.repository.is_absolute()
            || !matches!(r.commit.len(), 40 | 64)
            || !r.commit.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(Error::invalid(
                "capture requires an absolute repository path and exact full commit object ID",
            ));
        }
        Ok(r)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckRequest {
    pub client_request_id: String,
    pub attempt_id: String,
    pub candidate_ref: String,
    pub profile_id: String,
    pub profile_revision: String,
}
impl CheckRequest {
    pub fn parse(v: &Value) -> Result<Self> {
        let r: Self = serde_json::from_value(v.clone())?;
        for field in [
            "client_request_id",
            "attempt_id",
            "candidate_ref",
            "profile_id",
            "profile_revision",
        ] {
            model::text(v, field)?;
        }
        Ok(r)
    }
}
