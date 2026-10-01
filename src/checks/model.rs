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
}
impl Default for CheckConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_running: 2,
            git_executable: "git".into(),
            profiles: Vec::new(),
        }
    }
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
                if key.is_empty()
                    || key.contains(['=', '\0'])
                    || !env.insert(upper.clone())
                    || matches!(upper.as_str(), "CARGO_TARGET_DIR" | "SWARM_CANDIDATE_FILE")
                {
                    return Err(Error::invalid(
                        "duplicate, reserved or invalid check environment key",
                    ));
                }
            }
            if p.environment.values().any(|v| v.contains('\0'))
                || p.args.iter().any(|s| s.contains('\0'))
            {
                return Err(Error::invalid("check arguments/environment contain a NUL"));
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
