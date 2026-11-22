//! Bounded launch-time inventory of native project instruction files.

use super::sources::{self, SourceReadError};
use eliot_core::api::{
    INSTRUCTION_FILE_PATHS, InstructionDeliveryView, InstructionFileStatus, InstructionFileView,
    InstructionReadStatus, InstructionSourceStatus, inference::sha256_hex,
};
use serde_json::Value;
use std::path::Path;

const MAX_CATALOG_BYTES: u64 = 8 * 1024 * 1024;

pub(super) struct Capture {
    pub facts: InstructionDeliveryView,
    pub should_deliver_primary: bool,
}

pub(super) fn capture(workspace: Option<&str>, harness: &str, profile: &Value) -> Capture {
    let root = workspace.and_then(|workspace| std::fs::canonicalize(workspace).ok());
    let source_workspace = workspace.map(ToOwned::to_owned);
    let mut remaining = MAX_CATALOG_BYTES;
    let files = INSTRUCTION_FILE_PATHS
        .iter()
        .map(|path| {
            let (native_read_status, native_read_version) = native_read(harness, path, profile);
            let mut file = match root.as_deref() {
                Some(root) => catalog_file(root, path, &mut remaining),
                None => InstructionFileView {
                    path: (*path).to_owned(),
                    status: InstructionFileStatus::WorkspaceUnavailable,
                    size_bytes: None,
                    sha256: None,
                    native_read_status,
                    native_read_version: native_read_version.clone(),
                    delivery_status: InstructionSourceStatus::NotNeeded,
                },
            };
            file.native_read_status = native_read_status;
            file.native_read_version = native_read_version;
            file
        })
        .collect::<Vec<_>>();
    let primary = [
        "AGENTS.md",
        "AGENTS.override.md",
        "CLAUDE.md",
        ".claude/CLAUDE.md",
        "GEMINI.md",
        "CLAUDE.local.md",
        ".claude/CLAUDE.local.md",
    ]
    .into_iter()
    .find(|candidate| {
        files.iter().any(|file| {
            file.path == *candidate
                && !matches!(
                    file.status,
                    InstructionFileStatus::Missing | InstructionFileStatus::WorkspaceUnavailable
                )
        })
    })
    .unwrap_or("AGENTS.md");
    let mut facts = InstructionDeliveryView {
        source_workspace,
        primary_path: primary.to_owned(),
        files,
    };
    let primary = facts
        .files
        .iter_mut()
        .find(|file| file.path == facts.primary_path);
    let should_deliver_primary = primary.is_some_and(|file| {
        if file.status == InstructionFileStatus::WorkspaceUnavailable {
            file.delivery_status = InstructionSourceStatus::Unavailable;
            return false;
        }
        if file.status == InstructionFileStatus::Missing {
            file.delivery_status = InstructionSourceStatus::NotNeeded;
            return false;
        }
        let needed = file.native_read_status != InstructionReadStatus::Documented;
        if !needed {
            file.delivery_status = InstructionSourceStatus::NotNeeded;
        }
        needed
    });
    Capture {
        facts,
        should_deliver_primary,
    }
}

fn catalog_file(root: &Path, path: &str, remaining: &mut u64) -> InstructionFileView {
    let joined = root.join(path);
    let canonical = match std::fs::canonicalize(&joined) {
        Ok(path) => path,
        Err(error) => {
            let status = if error.kind() == std::io::ErrorKind::NotFound {
                InstructionFileStatus::Missing
            } else {
                InstructionFileStatus::Unreadable
            };
            return empty_file(path, status);
        }
    };
    if !canonical.starts_with(root) {
        return empty_file(path, InstructionFileStatus::OutsideWorkspace);
    }
    let metadata = match std::fs::metadata(&canonical) {
        Ok(metadata) => metadata,
        Err(_) => return empty_file(path, InstructionFileStatus::Unreadable),
    };
    let size = metadata.len();
    if !metadata.is_file() {
        return InstructionFileView {
            path: path.to_owned(),
            status: InstructionFileStatus::NotRegular,
            size_bytes: Some(size),
            sha256: None,
            native_read_status: InstructionReadStatus::Unknown,
            native_read_version: None,
            delivery_status: InstructionSourceStatus::NotNeeded,
        };
    }
    if size > MAX_CATALOG_BYTES || size > *remaining {
        return InstructionFileView {
            path: path.to_owned(),
            status: InstructionFileStatus::TooLarge,
            size_bytes: Some(size),
            sha256: None,
            native_read_status: InstructionReadStatus::Unknown,
            native_read_version: None,
            delivery_status: InstructionSourceStatus::NotNeeded,
        };
    }
    let bytes = match sources::read_source(root, path) {
        Ok(bytes) => bytes,
        Err(error) => {
            let status = match error {
                SourceReadError::NotFound => InstructionFileStatus::Missing,
                SourceReadError::OutsideWorkspace => InstructionFileStatus::OutsideWorkspace,
                SourceReadError::NotRegular => InstructionFileStatus::NotRegular,
                SourceReadError::TooLarge(_) => InstructionFileStatus::TooLarge,
                SourceReadError::Unreadable => InstructionFileStatus::Unreadable,
            };
            return InstructionFileView {
                path: path.to_owned(),
                status,
                size_bytes: (status != InstructionFileStatus::Missing).then_some(size),
                sha256: None,
                native_read_status: InstructionReadStatus::Unknown,
                native_read_version: None,
                delivery_status: InstructionSourceStatus::NotNeeded,
            };
        }
    };
    let actual_size = bytes.len() as u64;
    if actual_size > *remaining {
        return InstructionFileView {
            path: path.to_owned(),
            status: InstructionFileStatus::TooLarge,
            size_bytes: Some(actual_size),
            sha256: None,
            native_read_status: InstructionReadStatus::Unknown,
            native_read_version: None,
            delivery_status: InstructionSourceStatus::NotNeeded,
        };
    }
    *remaining = (*remaining).saturating_sub(actual_size);
    let digest = sha256_hex(&bytes);
    let status = if std::str::from_utf8(&bytes).is_ok_and(|text| !text.contains('\0')) {
        InstructionFileStatus::Present
    } else {
        InstructionFileStatus::Binary
    };
    InstructionFileView {
        path: path.to_owned(),
        status,
        size_bytes: Some(bytes.len() as u64),
        sha256: Some(digest),
        native_read_status: InstructionReadStatus::Unknown,
        native_read_version: None,
        delivery_status: InstructionSourceStatus::NotNeeded,
    }
}

fn empty_file(path: &str, status: InstructionFileStatus) -> InstructionFileView {
    InstructionFileView {
        path: path.to_owned(),
        status,
        size_bytes: None,
        sha256: None,
        native_read_status: InstructionReadStatus::Unknown,
        native_read_version: None,
        delivery_status: InstructionSourceStatus::NotNeeded,
    }
}

fn native_read(
    harness: &str,
    path: &str,
    profile: &Value,
) -> (InstructionReadStatus, Option<String>) {
    let version = profile
        .get("cmd_version")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let status = match (harness, path) {
        ("claude", "AGENTS.md") => match version.as_deref().and_then(version_at_least_2_1_277) {
            Some(true) => InstructionReadStatus::Conditional,
            Some(false) => InstructionReadStatus::NotNative,
            None => InstructionReadStatus::Unknown,
        },
        (
            "claude",
            "CLAUDE.md" | "CLAUDE.local.md" | ".claude/CLAUDE.md" | ".claude/CLAUDE.local.md",
        ) => InstructionReadStatus::Conditional,
        ("codex", "AGENTS.md" | "AGENTS.override.md") => {
            if profile.get("_provider_probe") == Some(&Value::Bool(true)) {
                InstructionReadStatus::NotNative
            } else {
                InstructionReadStatus::Documented
            }
        }
        ("muse", "AGENTS.md" | "AGENTS.override.md") => InstructionReadStatus::NotNative,
        ("opencode", "AGENTS.md") => InstructionReadStatus::Documented,
        ("opencode", "CLAUDE.md") => InstructionReadStatus::Conditional,
        ("antigravity", "GEMINI.md") => InstructionReadStatus::Unknown,
        _ => InstructionReadStatus::Unknown,
    };
    let version = if harness == "claude"
        && matches!(
            path,
            "AGENTS.md"
                | "CLAUDE.md"
                | "CLAUDE.local.md"
                | ".claude/CLAUDE.md"
                | ".claude/CLAUDE.local.md"
        ) {
        version
    } else {
        None
    };
    (status, version)
}

fn version_at_least_2_1_277(version: &str) -> Option<bool> {
    let mut pieces = version
        .split_whitespace()
        .next()?
        .split('.')
        .map(str::parse::<u64>);
    let major = pieces.next()?.ok()?;
    let minor = pieces.next()?.ok()?;
    let patch = pieces.next()?.ok()?;
    Some((major, minor, patch) >= (2, 1, 277))
}

pub(super) fn apply_delivery(capture: &mut Capture, delivery: super::sources::SourceDelivery) {
    if let Some(file) = capture
        .facts
        .files
        .iter_mut()
        .find(|file| file.path == delivery.path)
    {
        file.status = delivery.file_status;
        file.size_bytes = delivery.size_bytes;
        file.sha256 = delivery.sha256;
        file.delivery_status = delivery.status;
    }
}

pub(super) fn mark_unavailable(capture: &mut Capture) {
    if let Some(file) = capture
        .facts
        .files
        .iter_mut()
        .find(|file| file.path == capture.facts.primary_path)
    {
        file.delivery_status = InstructionSourceStatus::Unavailable;
    }
}
