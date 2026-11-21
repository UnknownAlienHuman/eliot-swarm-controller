//! Owner-only editing of top-level automation rule files.
use serde::{Deserialize, Serialize};

use super::REDACTION_MARKER;
use crate::{Error, Result};

/// The loader's maximum size for one rule file.
pub const MAX_RULE_FILE_BYTES: usize = 128 * 1024;
/// Maximum UTF-8 byte length of one top-level rule filename.
pub const MAX_RULE_PATH_BYTES: usize = 255 * 4;
pub const RULE_REVISION_HEX_LEN: usize = 64;

fn validate_project(project: &Option<String>) -> Result<()> {
    if project
        .as_deref()
        .is_some_and(|project| project.trim().is_empty())
    {
        return Err(Error::new("invalid", "project must not be blank"));
    }
    Ok(())
}

fn validate_path(path: &str) -> Result<()> {
    let Some(stem) = path.strip_suffix(".toml") else {
        return Err(Error::new(
            "invalid",
            "rule path must be a top-level .toml filename",
        ));
    };
    if stem.is_empty()
        || matches!(stem, "." | "..")
        || path.len() > MAX_RULE_PATH_BYTES
        || path.starts_with(['/', '\\'])
        || path.contains(['/', '\\', ':', '\0'])
        || path.chars().any(char::is_control)
    {
        return Err(Error::new(
            "invalid",
            "rule path must be a bounded top-level .toml filename",
        ));
    }
    Ok(())
}

fn validate_revision(revision: &str) -> Result<()> {
    if revision.len() != RULE_REVISION_HEX_LEN
        || !revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::new(
            "invalid",
            "rule revision must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn validate_identity(project: &Option<String>, path: &str) -> Result<()> {
    validate_project(project)?;
    validate_path(path)
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleReadRequest {
    #[serde(default)]
    pub project: Option<String>,
    pub path: String,
}

impl RuleReadRequest {
    pub fn validate(&self) -> Result<()> {
        validate_identity(&self.project, &self.path)
    }

    pub fn validate_response(&self, reply: &RuleReadResponse) -> Result<()> {
        self.validate()?;
        validate_revision(&reply.revision)?;
        if reply.project != self.project || reply.path != self.path {
            return Err(Error::new("invalid", "rule read names another file"));
        }
        if reply.size != reply.content.len() as u64
            || reply.content.len() > MAX_RULE_FILE_BYTES
            || reply.content.contains('\0')
            || reply.content.contains(REDACTION_MARKER)
            || reply.revision != super::inference::sha256_hex(reply.content.as_bytes())
        {
            return Err(Error::new(
                "invalid",
                "rule read content is oversized or unsafe",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleReadResponse {
    #[serde(default)]
    pub project: Option<String>,
    pub path: String,
    /// Lowercase SHA-256 of the exact file bytes.
    pub revision: String,
    pub size: u64,
    /// UTF-8 text as stored, without line-ending normalization.
    pub content: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleSaveRequest {
    #[serde(default)]
    pub project: Option<String>,
    pub path: String,
    /// Absent means create-only. Present means replace only that exact revision.
    #[serde(default)]
    pub expected_revision: Option<String>,
    pub content: String,
}

impl RuleSaveRequest {
    pub fn validate(&self) -> Result<()> {
        validate_identity(&self.project, &self.path)?;
        if let Some(revision) = &self.expected_revision {
            validate_revision(revision)?;
        }
        if self.content.len() > MAX_RULE_FILE_BYTES {
            return Err(Error::new("invalid", "rule file exceeds the 128 KiB bound"));
        }
        if self.content.contains('\0') {
            return Err(Error::new("invalid", "rule content must not contain NUL"));
        }
        if self.content.contains(REDACTION_MARKER) {
            return Err(Error::new(
                "invalid",
                "rule content carries a redaction marker",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &RuleSaveResponse) -> Result<()> {
        validate_revision(&reply.revision)?;
        if reply.project != self.project
            || reply.path != self.path
            || !reply.saved
            || reply.revision != super::inference::sha256_hex(self.content.as_bytes())
        {
            return Err(Error::new(
                "invalid",
                "rule save response does not match its request",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleSaveResponse {
    #[serde(default)]
    pub project: Option<String>,
    pub path: String,
    /// True once the exact bytes have been atomically published.
    pub saved: bool,
    /// SHA-256 of the bytes now on disk.
    pub revision: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleDeleteRequest {
    #[serde(default)]
    pub project: Option<String>,
    pub path: String,
    pub expected_revision: String,
}

impl RuleDeleteRequest {
    pub fn validate(&self) -> Result<()> {
        validate_identity(&self.project, &self.path)?;
        validate_revision(&self.expected_revision)
    }

    pub fn validate_response(&self, reply: &RuleDeleteResponse) -> Result<()> {
        if reply.project != self.project || reply.path != self.path || !reply.deleted {
            return Err(Error::new(
                "invalid",
                "rule delete response does not match its request",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleDeleteResponse {
    #[serde(default)]
    pub project: Option<String>,
    pub path: String,
    /// True once the exact file revision has been removed.
    pub deleted: bool,
}
