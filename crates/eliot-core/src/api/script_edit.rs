//! Owner script editing: list, read and save existing script entries.
//!
//! Script entries live only under the approved Run roots: the global
//! `<data_dir>/scripts/` directory and each registered project's canonical
//! `.eliot/scripts/` directory. These reads and the save address an existing
//! regular file by its root (`project`, absent = global) and a relative
//! `path`; they never create, rename or delete files and never execute
//! anything. Running a script stays with the existing Run path (`timer.run`
//! of a timer whose `run` action names the entry).

use serde::{Deserialize, Serialize};

use super::{CONFIG_REVISION_HEX_LEN, REDACTION_MARKER};
use crate::{Error, Result};

/// Largest script the editor reads or writes.
pub const MAX_SCRIPT_EDIT_BYTES: usize = 256 * 1024;
/// Filesystem entries scanned per root at most, including refused entries.
pub const MAX_SCRIPT_ENTRIES: usize = 500;
/// Encoded list page budget, leaving room for the RPC envelope.
pub const MAX_SCRIPT_PAGE_BYTES: usize = 64 * 1024;
/// Longest relative entry path.
pub const MAX_SCRIPT_PATH_BYTES: usize = 1024;

fn validate_entry(project: &Option<String>, path: &str) -> Result<()> {
    if project
        .as_deref()
        .is_some_and(|project| project.trim().is_empty())
    {
        return Err(Error::new("invalid", "project must not be blank"));
    }
    let bad = path.trim().is_empty()
        || path.len() > MAX_SCRIPT_PATH_BYTES
        || path.starts_with(['/', '\\'])
        || path.contains(':')
        || path.chars().any(char::is_control)
        || path
            .split(['/', '\\'])
            .any(|part| matches!(part, "." | "..") || part.is_empty());
    if bad {
        return Err(Error::new(
            "invalid",
            "script path must be a relative path below its root without '..', drive or empty parts",
        ));
    }
    Ok(())
}

fn validate_revision(revision: &str) -> Result<()> {
    if revision.len() != CONFIG_REVISION_HEX_LEN
        || !revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::new(
            "invalid",
            "script revision must be 64 lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

/// Request for `script.list`. Cursor positions are listing hints, not snapshots.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptListRequest {
    #[serde(default)]
    pub cursor: Option<String>,
}

impl ScriptListRequest {
    pub fn position(&self) -> Result<(usize, usize)> {
        let Some(cursor) = &self.cursor else {
            return Ok((0, 0));
        };
        if cursor.len() > 32 {
            return Err(Error::new("invalid", "script cursor is too long"));
        }
        let (root, entry) = cursor
            .split_once(':')
            .ok_or_else(|| Error::new("invalid", "invalid script cursor"))?;
        let root = root
            .parse::<usize>()
            .map_err(|_| Error::new("invalid", "invalid script root cursor"))?;
        let entry = entry
            .parse::<usize>()
            .map_err(|_| Error::new("invalid", "invalid script entry cursor"))?;
        if entry > MAX_SCRIPT_ENTRIES || cursor != &format!("{root}:{entry}") {
            return Err(Error::new("invalid", "invalid script cursor"));
        }
        Ok((root, entry))
    }

    pub fn validate(&self) -> Result<()> {
        self.position().map(|_| ())
    }

    pub fn validate_response(&self, reply: &ScriptListResponse) -> Result<()> {
        if serde_json::to_vec(reply)
            .map_err(|_| Error::new("invalid", "script page cannot be encoded"))?
            .len()
            > MAX_SCRIPT_PAGE_BYTES
        {
            return Err(Error::new("invalid", "script page exceeds byte budget"));
        }
        if let Some(next) = &reply.next_cursor {
            let position = ScriptListRequest {
                cursor: Some(next.clone()),
            }
            .position()?;
            if position <= self.position()? {
                return Err(Error::new("invalid", "script cursor did not advance"));
            }
        }
        for root in &reply.roots {
            if root.available == root.reason.is_some() {
                return Err(Error::new(
                    "invalid",
                    "a script root carries a reason exactly when it is unavailable",
                ));
            }
            if root.entries.len() > MAX_SCRIPT_ENTRIES {
                return Err(Error::new("invalid", "script root lists too many entries"));
            }
            let mut seen = std::collections::HashSet::new();
            for entry in &root.entries {
                validate_entry(&root.project, &entry.path)?;
                if !seen.insert(entry.path.as_str()) {
                    return Err(Error::new("invalid", "script root repeats an entry"));
                }
            }
        }
        Ok(())
    }
}

/// One listed entry: path relative to its root, with `/` separators.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptEntryView {
    pub path: String,
    pub size: u64,
}

/// One approved root and its regular-file entries.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptRootView {
    /// Registered project id; absent for the global root.
    #[serde(default)]
    pub project: Option<String>,
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub entries: Vec<ScriptEntryView>,
    /// The scan budget was reached; further entries may exist.
    pub truncated: bool,
}

/// Response for `script.list`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptListResponse {
    pub roots: Vec<ScriptRootView>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// Request for `script.read`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptReadRequest {
    #[serde(default)]
    pub project: Option<String>,
    pub path: String,
}

impl ScriptReadRequest {
    pub fn validate(&self) -> Result<()> {
        validate_entry(&self.project, &self.path)
    }

    pub fn validate_response(&self, reply: &ScriptReadResponse) -> Result<()> {
        validate_revision(&reply.revision)?;
        if reply.project != self.project || reply.path != self.path {
            return Err(Error::new("invalid", "script read names another entry"));
        }
        match (&reply.content, reply.editable, &reply.reason) {
            (Some(content), true, None) => {
                if content.len() > MAX_SCRIPT_EDIT_BYTES || content.contains(REDACTION_MARKER) {
                    return Err(Error::new(
                        "invalid",
                        "script content is oversized or carries a redaction marker",
                    ));
                }
                Ok(())
            }
            (None, false, Some(_)) => Ok(()),
            _ => Err(Error::new(
                "invalid",
                "script content is returned exactly when it is editable; otherwise a reason is given",
            )),
        }
    }
}

/// Response for `script.read`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptReadResponse {
    #[serde(default)]
    pub project: Option<String>,
    pub path: String,
    /// Lowercase hex SHA-256 of the file bytes.
    pub revision: String,
    pub size: u64,
    /// Canonical path without a verbatim prefix, for "Open in editor".
    pub display_path: String,
    pub editable: bool,
    /// UTF-8 text without a byte-order mark, when editable.
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Request for `script.save`: replace one existing entry's bytes.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptSaveRequest {
    #[serde(default)]
    pub project: Option<String>,
    pub path: String,
    /// Revision the content was edited from; a different file is `conflict`.
    pub expected_revision: String,
    pub content: String,
    /// Validate only.
    #[serde(default)]
    pub dry_run: bool,
}

impl ScriptSaveRequest {
    pub fn validate(&self) -> Result<()> {
        validate_entry(&self.project, &self.path)?;
        validate_revision(&self.expected_revision)?;
        if self.content.len() > MAX_SCRIPT_EDIT_BYTES {
            return Err(Error::new("invalid", "script content exceeds 256 KiB"));
        }
        if self.content.contains('\0') {
            return Err(Error::new(
                "invalid",
                "script content must be text without NUL",
            ));
        }
        if self.content.contains(REDACTION_MARKER) {
            return Err(Error::new(
                "invalid",
                "script content carries a redaction marker; secrets are never saved through this path",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &ScriptSaveResponse) -> Result<()> {
        validate_revision(&reply.revision)?;
        if self.dry_run && reply.saved {
            return Err(Error::new("invalid", "a dry run must not save"));
        }
        Ok(())
    }
}

/// Response for `script.save`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptSaveResponse {
    /// True when the file holds the content now (written or already equal).
    pub saved: bool,
    /// Current file revision after the call.
    pub revision: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_cursors_and_page_bytes_are_bounded_and_must_advance() {
        let request = ScriptListRequest {
            cursor: Some("3:2".into()),
        };
        assert_eq!(request.position().unwrap(), (3, 2));
        for cursor in ["", "x", "03:0", "1:501", "-1:0", "1:0:0"] {
            assert!(
                ScriptListRequest {
                    cursor: Some(cursor.into())
                }
                .validate()
                .is_err()
            );
        }
        let mut reply = ScriptListResponse {
            roots: Vec::new(),
            next_cursor: Some("3:3".into()),
        };
        request.validate_response(&reply).unwrap();
        reply.next_cursor = Some("3:2".into());
        assert!(request.validate_response(&reply).is_err());
        reply.next_cursor = None;
        reply.roots.push(ScriptRootView {
            project: None,
            available: false,
            reason: Some("x".repeat(MAX_SCRIPT_PAGE_BYTES)),
            entries: Vec::new(),
            truncated: false,
        });
        assert!(request.validate_response(&reply).is_err());
    }

    #[test]
    fn entry_paths_stay_relative_and_inside() {
        assert!(validate_entry(&None, "deploy/check.ps1").is_ok());
        for bad in [
            "",
            "/abs.ps1",
            "C:/x.ps1",
            "a/../b.ps1",
            "a//b.ps1",
            "\\\\host\\x",
        ] {
            assert!(validate_entry(&None, bad).is_err(), "{bad}");
        }
        assert!(validate_entry(&Some(" ".to_owned()), "x.ps1").is_err());
    }

    #[test]
    fn save_rejects_nul_markers_and_bad_revisions() {
        let mut request = ScriptSaveRequest {
            project: None,
            path: "x.ps1".to_owned(),
            expected_revision: "a".repeat(64),
            content: "Write-Output 1\n".to_owned(),
            dry_run: false,
        };
        assert!(request.validate().is_ok());
        request.content.push('\0');
        assert!(request.validate().is_err());
        request.content = "[REDACTED]".to_owned();
        assert!(request.validate().is_err());
        request.content = String::new();
        request.expected_revision = "A".repeat(64);
        assert!(request.validate().is_err());
    }

    #[test]
    fn read_response_returns_content_only_when_editable() {
        let request = ScriptReadRequest {
            project: None,
            path: "x.ps1".to_owned(),
        };
        let withheld = ScriptReadResponse {
            project: None,
            path: "x.ps1".to_owned(),
            revision: "0".repeat(64),
            size: 10,
            display_path: "C:/d/scripts/x.ps1".to_owned(),
            editable: false,
            content: None,
            reason: Some("binary".to_owned()),
        };
        assert!(request.validate_response(&withheld).is_ok());
        let inconsistent = ScriptReadResponse {
            content: Some(String::new()),
            ..withheld
        };
        assert!(request.validate_response(&inconsistent).is_err());
    }
}
