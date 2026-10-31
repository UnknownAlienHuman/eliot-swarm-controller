//! Typed request and result payloads for the connected native Git fetch action.
//!
//! These DTOs are pure data. Repository registration, transport construction,
//! process ownership, Store admission, and effect settlement belong to callers.

use crate::{Error, FrozenSetupFile, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

/// Default for the optional rule-authored fetch timeout.
pub const DEFAULT_GIT_FETCH_TIMEOUT_MS: i64 = 10 * 60 * 1_000;

/// Literal rule input. The Router freezes registration and transport state before
/// admitting a command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitFetchDefinitionArgs {
    pub repository: String,
    pub remote: String,
    pub source_ref: String,
    pub tracking_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
}

impl GitFetchDefinitionArgs {
    pub fn validate(&self) -> Result<()> {
        validate_literal(&self.repository, "repository")?;
        validate_literal(&self.remote, "remote")?;
        if self.remote.starts_with('-') || self.remote.chars().any(char::is_whitespace) {
            return Err(invalid("git.fetch remote is not a safe literal name"));
        }
        validate_full_ref(&self.source_ref, "source_ref")?;
        validate_tracking_ref(&self.tracking_ref, "tracking_ref")?;
        if let Some(timeout) = &self.timeout {
            validate_literal(timeout, "timeout")?;
        }
        Ok(())
    }

    /// Build the stable caller payload using the daemon's parsed effective timeout.
    pub fn binding_args(&self, timeout_ms: i64) -> Result<Value> {
        self.validate()?;
        canonical_git_fetch_binding_args(
            &self.repository,
            &self.remote,
            &self.source_ref,
            &self.tracking_ref,
            timeout_ms,
        )
    }
}

/// Frozen nonsecret intent persisted in the ordinary command args before dispatch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenGitFetch {
    pub repository: String,
    pub registration_revision: i64,
    pub canonical_checkout: String,
    pub common_dir: String,
    pub remote: String,
    pub endpoint: String,
    pub source_ref: String,
    pub advertised_oid: String,
    pub tracking_ref: String,
    /// `None` means the tracking ref was absent when the intent was frozen.
    #[serde(deserialize_with = "required_expected_old_oid")]
    pub expected_old_oid: Option<String>,
    pub timeout_ms: i64,
    /// When present, this is the receipt for the explicitly approved manager helper.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_helper: Option<FrozenSetupFile>,
}

impl FrozenGitFetch {
    pub fn validate(&self) -> Result<()> {
        validate_literal(&self.repository, "repository")?;
        validate_literal(&self.remote, "remote")?;
        if self.remote.starts_with('-') || self.remote.chars().any(char::is_whitespace) {
            return Err(invalid("git.fetch remote is not a safe literal name"));
        }
        if self.registration_revision < 0 {
            return Err(invalid(
                "git.fetch registration_revision must be nonnegative",
            ));
        }
        validate_absolute_path(&self.canonical_checkout, "canonical_checkout")?;
        validate_absolute_path(&self.common_dir, "common_dir")?;
        validate_literal(&self.endpoint, "endpoint")?;
        if self.endpoint.starts_with('-') || endpoint_has_userinfo(&self.endpoint) {
            return Err(invalid(
                "git.fetch endpoint cannot be an option or contain URL credentials",
            ));
        }
        validate_full_ref(&self.source_ref, "source_ref")?;
        validate_oid(&self.advertised_oid, "advertised_oid")?;
        validate_tracking_ref(&self.tracking_ref, "tracking_ref")?;
        if let Some(oid) = &self.expected_old_oid {
            validate_oid(oid, "expected_old_oid")?;
        }
        if self.timeout_ms <= 0 {
            return Err(invalid("git.fetch timeout_ms must be positive"));
        }
        if let Some(receipt) = &self.credential_helper {
            validate_setup_file(receipt)?;
        }
        Ok(())
    }

    /// Project only caller-selected fields; frozen registration and remote
    /// observations remain retained first-admission intent, not the request digest.
    pub fn binding_args(&self) -> Result<Value> {
        self.validate()?;
        canonical_git_fetch_binding_args(
            &self.repository,
            &self.remote,
            &self.source_ref,
            &self.tracking_ref,
            self.timeout_ms,
        )
    }
}

/// Readback after a successful fetch and tracking-ref compare-and-swap.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitFetchOutcome {
    pub tracking_ref: String,
    pub tracking_oid: String,
}

impl GitFetchOutcome {
    pub fn validate(&self) -> Result<()> {
        validate_tracking_ref(&self.tracking_ref, "tracking_ref")?;
        validate_oid(&self.tracking_oid, "tracking_oid")
    }

    /// Prove that the applied readback is the exact frozen fetch target.
    pub fn validate_for(&self, frozen: &FrozenGitFetch) -> Result<()> {
        self.validate()?;
        frozen.validate()?;
        if self.tracking_ref != frozen.tracking_ref || self.tracking_oid != frozen.advertised_oid {
            return Err(invalid(
                "git.fetch readback differs from its frozen tracking target",
            ));
        }
        Ok(())
    }
}

fn validate_literal(value: &str, field: &str) -> Result<()> {
    if value.trim().is_empty() || value.contains("${") || value.chars().any(char::is_control) {
        return Err(invalid(format!(
            "git.fetch {field} must be nonempty literal text without control characters"
        )));
    }
    Ok(())
}

fn canonical_git_fetch_binding_args(
    repository: &str,
    remote: &str,
    source_ref: &str,
    tracking_ref: &str,
    timeout_ms: i64,
) -> Result<Value> {
    if timeout_ms <= 0 {
        return Err(invalid("git.fetch timeout_ms must be positive"));
    }
    let args = GitFetchDefinitionArgs {
        repository: repository.to_owned(),
        remote: remote.to_owned(),
        source_ref: source_ref.to_owned(),
        tracking_ref: tracking_ref.to_owned(),
        timeout: Some(format!("{timeout_ms}ms")),
    };
    args.validate()?;
    serde_json::to_value(args).map_err(|_| invalid("git.fetch binding args cannot be encoded"))
}

fn validate_absolute_path(value: &str, field: &str) -> Result<()> {
    validate_literal(value, field)?;
    if !Path::new(value).is_absolute() {
        return Err(invalid(format!("git.fetch {field} must be absolute")));
    }
    Ok(())
}

fn validate_full_ref(value: &str, field: &str) -> Result<()> {
    validate_literal(value, field)?;
    let invalid_character = value
        .chars()
        .any(|character| matches!(character, '~' | '^' | ':' | '?' | '*' | '[' | '\\'));
    if !value.starts_with("refs/")
        || value.ends_with('/')
        || value.ends_with('.')
        || value.contains("//")
        || value.contains("..")
        || value.contains("@{")
        || invalid_character
        || value.bytes().any(|byte| byte <= b' ' || byte == 0x7f)
        || value.split('/').any(|component| {
            component.is_empty() || component.starts_with('.') || component.ends_with(".lock")
        })
    {
        return Err(invalid(format!(
            "git.fetch {field} must be a full valid Git ref"
        )));
    }
    Ok(())
}

fn validate_tracking_ref(value: &str, field: &str) -> Result<()> {
    if !value
        .strip_prefix("refs/remotes/")
        .is_some_and(|suffix| !suffix.is_empty())
    {
        return Err(invalid(format!(
            "git.fetch {field} must be under refs/remotes/"
        )));
    }
    validate_full_ref(value, field)
}

fn validate_oid(value: &str, field: &str) -> Result<()> {
    if !matches!(value.len(), 40 | 64) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(format!(
            "git.fetch {field} must be an exact Git object ID"
        )));
    }
    Ok(())
}

fn validate_setup_file(file: &FrozenSetupFile) -> Result<()> {
    validate_absolute_path(&file.canonical_path, "credential helper receipt path")?;
    if file.sha256.len() != 64 || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid("git.fetch credential helper receipt is malformed"));
    }
    Ok(())
}

fn endpoint_has_userinfo(endpoint: &str) -> bool {
    if let Some((_, rest)) = endpoint.split_once("://") {
        let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
        return authority.contains('@');
    }
    endpoint
        .split(['/', '\\'])
        .next()
        .is_some_and(|authority| authority.contains('@'))
}

fn required_expected_old_oid<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let oid = Option::<String>::deserialize(deserializer)?;
    if let Some(oid) = &oid {
        validate_oid(oid, "expected_old_oid").map_err(serde::de::Error::custom)?;
    }
    Ok(oid)
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new("invalid", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn definition() -> GitFetchDefinitionArgs {
        GitFetchDefinitionArgs {
            repository: "project-one".to_owned(),
            remote: "origin".to_owned(),
            source_ref: "refs/heads/main".to_owned(),
            tracking_ref: "refs/remotes/origin/main".to_owned(),
            timeout: None,
        }
    }

    fn test_absolute_paths() -> (String, String, String) {
        #[cfg(windows)]
        {
            (
                r"C:\repo".to_owned(),
                r"C:\repo\.git".to_owned(),
                r"C:\Git\mingw64\bin\git-credential-manager.exe".to_owned(),
            )
        }
        #[cfg(not(windows))]
        {
            (
                "/tmp/repo".to_owned(),
                "/tmp/repo/.git".to_owned(),
                "/usr/local/bin/git-credential-manager".to_owned(),
            )
        }
    }

    fn frozen_fetch() -> FrozenGitFetch {
        let (canonical_checkout, common_dir, _) = test_absolute_paths();
        FrozenGitFetch {
            repository: "project-one".to_owned(),
            registration_revision: 4,
            canonical_checkout,
            common_dir,
            remote: "origin".to_owned(),
            endpoint: "https://example.test/repo.git".to_owned(),
            source_ref: "refs/heads/main".to_owned(),
            advertised_oid: "a".repeat(40),
            tracking_ref: "refs/remotes/origin/main".to_owned(),
            expected_old_oid: None,
            timeout_ms: DEFAULT_GIT_FETCH_TIMEOUT_MS,
            credential_helper: None,
        }
    }

    #[test]
    fn definition_accepts_literal_full_refs_and_optional_timeout() {
        let mut input = definition();
        input.validate().unwrap();
        input.timeout = Some("2m".to_owned());
        input.validate().unwrap();
        assert_eq!(DEFAULT_GIT_FETCH_TIMEOUT_MS, 600_000);
    }

    #[test]
    fn definition_binding_args_normalize_equivalent_timeouts_and_bind_callers() {
        let omitted = definition()
            .binding_args(DEFAULT_GIT_FETCH_TIMEOUT_MS)
            .unwrap();
        let mut explicit_default = definition();
        explicit_default.timeout = Some("10m".to_owned());
        assert_eq!(
            omitted,
            explicit_default
                .binding_args(DEFAULT_GIT_FETCH_TIMEOUT_MS)
                .unwrap()
        );
        assert_eq!(omitted["timeout"], "600000ms");

        let mut sixty_seconds = definition();
        sixty_seconds.timeout = Some("60s".to_owned());
        let mut one_minute = definition();
        one_minute.timeout = Some("60000ms".to_owned());
        let normalized = sixty_seconds.binding_args(60_000).unwrap();
        assert_eq!(normalized, one_minute.binding_args(60_000).unwrap());
        assert_eq!(normalized["timeout"], "60000ms");
        assert_eq!(
            sixty_seconds.binding_args(120_000).unwrap()["timeout"],
            "120000ms"
        );
        assert!(definition().binding_args(0).is_err());

        let baseline = definition().binding_args(60_000).unwrap();
        let mut variants = Vec::new();
        let mut changed = definition();
        changed.repository = "project-two".to_owned();
        variants.push(changed);
        let mut changed = definition();
        changed.remote = "upstream".to_owned();
        variants.push(changed);
        let mut changed = definition();
        changed.source_ref = "refs/heads/release".to_owned();
        variants.push(changed);
        let mut changed = definition();
        changed.tracking_ref = "refs/remotes/origin/release".to_owned();
        variants.push(changed);
        for variant in variants {
            assert_ne!(variant.binding_args(60_000).unwrap(), baseline);
        }
    }

    #[test]
    fn definition_rejects_templates_option_names_and_non_full_refs() {
        let mut input = definition();
        input.repository = "${event.data.repository}".to_owned();
        assert_eq!(input.validate().unwrap_err().code, "invalid");

        let mut input = definition();
        input.remote = "--upload-pack=evil".to_owned();
        assert_eq!(input.validate().unwrap_err().code, "invalid");

        for source_ref in [
            "main",
            "refs/heads/../main",
            "refs/heads/x.lock",
            "refs/heads/x?y",
        ] {
            let mut input = definition();
            input.source_ref = source_ref.to_owned();
            assert_eq!(input.validate().unwrap_err().code, "invalid");
        }

        for tracking_ref in ["refs/heads/main", "refs/tags/v1"] {
            let mut input = definition();
            input.tracking_ref = tracking_ref.to_owned();
            assert_eq!(input.validate().unwrap_err().code, "invalid");
        }

        let mut input = definition();
        input.tracking_ref = "${subject.branch}".to_owned();
        assert_eq!(input.validate().unwrap_err().code, "invalid");

        let mut wire = serde_json::to_value(definition()).unwrap();
        wire["force"] = Value::Bool(true);
        assert!(serde_json::from_value::<GitFetchDefinitionArgs>(wire).is_err());
    }

    #[test]
    fn frozen_fetch_requires_explicit_absent_oid_and_valid_fences() {
        let valid = frozen_fetch();
        valid.validate().unwrap();

        let mut value = serde_json::to_value(&valid).unwrap();
        assert_eq!(value["expected_old_oid"], Value::Null);
        value.as_object_mut().unwrap().remove("expected_old_oid");
        assert!(serde_json::from_value::<FrozenGitFetch>(value).is_err());

        let mut invalid_fetch = frozen_fetch();
        invalid_fetch.registration_revision = -1;
        assert_eq!(invalid_fetch.validate().unwrap_err().code, "invalid");

        let mut invalid_fetch = frozen_fetch();
        invalid_fetch.timeout_ms = 0;
        assert_eq!(invalid_fetch.validate().unwrap_err().code, "invalid");

        let mut invalid_fetch = frozen_fetch();
        invalid_fetch.advertised_oid = "abc".to_owned();
        assert_eq!(invalid_fetch.validate().unwrap_err().code, "invalid");

        let mut invalid_fetch = frozen_fetch();
        invalid_fetch.expected_old_oid = Some("nope".to_owned());
        assert_eq!(invalid_fetch.validate().unwrap_err().code, "invalid");

        for tracking_ref in ["refs/heads/main", "refs/tags/v1"] {
            let mut invalid_fetch = frozen_fetch();
            invalid_fetch.tracking_ref = tracking_ref.to_owned();
            assert_eq!(invalid_fetch.validate().unwrap_err().code, "invalid");
        }

        let mut invalid_fetch = frozen_fetch();
        invalid_fetch.endpoint = "https://user:secret@example.test/r".to_owned();
        assert_eq!(invalid_fetch.validate().unwrap_err().code, "invalid");
    }

    #[test]
    fn outcome_is_exact_frozen_tracking_ref_readback() {
        let fetch = frozen_fetch();
        let outcome = GitFetchOutcome {
            tracking_ref: fetch.tracking_ref.clone(),
            tracking_oid: fetch.advertised_oid.clone(),
        };
        outcome.validate_for(&fetch).unwrap();

        let wrong_oid = GitFetchOutcome {
            tracking_oid: "b".repeat(40),
            ..outcome.clone()
        };
        assert_eq!(wrong_oid.validate_for(&fetch).unwrap_err().code, "invalid");
        let wrong_ref = GitFetchOutcome {
            tracking_ref: "refs/remotes/origin/other".to_owned(),
            ..outcome
        };
        assert_eq!(wrong_ref.validate_for(&fetch).unwrap_err().code, "invalid");
    }

    #[test]
    fn frozen_binding_args_exclude_first_admission_metadata() {
        let original = frozen_fetch();
        let original_binding = original.binding_args().unwrap();
        assert_eq!(
            original_binding,
            definition().binding_args(original.timeout_ms).unwrap()
        );
        let mut changed = original.clone();
        changed.registration_revision += 1;
        changed.canonical_checkout = Path::new(&original.canonical_checkout)
            .join("different-checkout")
            .display()
            .to_string();
        changed.common_dir = Path::new(&original.common_dir)
            .join("different-common")
            .display()
            .to_string();
        changed.endpoint = "https://other.example.test/different.git".to_owned();
        changed.advertised_oid = "c".repeat(40);
        changed.expected_old_oid = Some("d".repeat(40));
        let (_, _, helper_path) = test_absolute_paths();
        changed.credential_helper = Some(FrozenSetupFile {
            canonical_path: helper_path,
            size: 23,
            sha256: "e".repeat(64),
        });

        assert_eq!(changed.binding_args().unwrap(), original_binding);
        let canonical_timeout = format!("{}ms", changed.timeout_ms);
        assert_eq!(
            changed.binding_args().unwrap()["timeout"].as_str(),
            Some(canonical_timeout.as_str())
        );

        let mut changed_caller = original;
        changed_caller.tracking_ref = "refs/remotes/origin/release".to_owned();
        assert_ne!(changed_caller.binding_args().unwrap(), original_binding);
    }

    #[test]
    fn credential_receipt_is_nonsecret_and_frozen_by_path_size_and_digest() {
        let mut fetch = frozen_fetch();
        let (_, _, helper_path) = test_absolute_paths();
        fetch.credential_helper = Some(FrozenSetupFile {
            canonical_path: helper_path,
            size: 17,
            sha256: "b".repeat(64),
        });
        fetch.validate().unwrap();

        let serialized: Value = serde_json::to_value(&fetch).unwrap();
        let sha256 = serialized["credential_helper"]["sha256"]
            .as_str()
            .expect("serialized helper receipt digest");
        assert_eq!(sha256.len(), 64);
        assert!(sha256.bytes().all(|byte| byte == b'b'));
        assert!(serialized["credential_helper"].get("secret").is_none());
    }
}
