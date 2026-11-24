//! Bounded launch-time inventory of native project instruction files.

use super::sources::{self, SourceReadError};
use eliot_core::api::{
    HarnessCapabilities, INSTRUCTION_FILE_PATHS, InstructionDeliveryView, InstructionFileStatus,
    InstructionFileView, InstructionReadStatus, InstructionSourceStatus, inference::sha256_hex,
};
use eliot_plugin::MAX_EVENT_BYTES;
use serde_json::Value;
use std::path::Path;

const MAX_CATALOG_BYTES: u64 = 8 * 1024 * 1024;

pub(super) struct Capture {
    pub facts: InstructionDeliveryView,
    pub should_deliver_primary: bool,
    source_path: Option<String>,
}

/// Append trusted owner guidance after any quoted project-source blocks. This
/// is an instruction block, not untrusted project data, and counts UTF-8 bytes.
pub(super) fn append_owner_rules(
    input: &str,
    revision: &str,
    digest: &str,
    rules: &str,
) -> Result<String, eliot_core::Error> {
    let quoted = eliot_core::fence_untrusted_text(rules);
    let block = format!(
        "Apply the Swarm owner rules in this block (settings revision {revision}, SHA-256 {digest}).\n{quoted}"
    );
    let delivered = sources::append_block(input, &block);
    if delivered.len() > MAX_EVENT_BYTES {
        return Err(eliot_core::Error::new(
            "frame_too_large",
            "owner rules and first input exceed the 64 KiB persisted-text limit",
        ));
    }
    Ok(delivered)
}

pub(super) fn capture(workspace: Option<&str>, harness: &str, profile: &Value) -> Capture {
    let root = workspace.and_then(|workspace| std::fs::canonicalize(workspace).ok());
    let source_workspace = workspace.map(ToOwned::to_owned);
    let native_read = HarnessCapabilities::of(harness).project_instruction_read;
    let mut remaining = MAX_CATALOG_BYTES;
    let files = INSTRUCTION_FILE_PATHS
        .iter()
        .map(|path| {
            let mut file = match root.as_deref() {
                Some(root) => catalog_file(root, path, &mut remaining),
                None => empty_file(path, InstructionFileStatus::WorkspaceUnavailable),
            };
            if let Some(native_read) = native_read {
                (file.native_read_status, file.native_read_version) =
                    native_read(path, profile, file.size_bytes);
            }
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
    let source_path = root.as_deref().and_then(|root| {
        let path = sources::resolve_source(root, primary).ok()?;
        Some(path.strip_prefix(root).ok()?.to_string_lossy().into_owned())
    });
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
        source_path,
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
            delivery_redacted: false,
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
            delivery_redacted: false,
        };
    }
    let bytes = match sources::read_source(root, path) {
        Ok((bytes, _)) => bytes,
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
                delivery_redacted: false,
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
            delivery_redacted: false,
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
        delivery_redacted: false,
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
        delivery_redacted: false,
    }
}

pub(super) fn apply_delivery(capture: &mut Capture, delivery: super::sources::SourceDelivery) {
    if let Some(file) = capture
        .facts
        .files
        .iter_mut()
        .find(|file| file.path == capture.facts.primary_path)
    {
        file.status = delivery.file_status;
        file.size_bytes = delivery.size_bytes;
        file.sha256 = delivery.sha256;
        file.delivery_status = delivery.status;
        file.delivery_redacted = delivery.redacted;
    }
}

pub(super) fn reuse_explicit_delivery(capture: &mut Capture, retained: &Value) {
    let Some(path) = capture.source_path.as_deref() else {
        return;
    };
    let Some(digest) = capture
        .facts
        .files
        .iter()
        .find(|file| file.path == capture.facts.primary_path)
        .and_then(|file| file.sha256.as_deref())
    else {
        return;
    };
    let Some(deliveries) = retained.as_array() else {
        return;
    };
    let delivery = deliveries
        .iter()
        .filter_map(|value| serde_json::from_value::<sources::SourceDelivery>(value.clone()).ok())
        .find(|delivery| {
            (delivery.path == path || cfg!(windows) && delivery.path.eq_ignore_ascii_case(path))
                && delivery.sha256.as_deref() == Some(digest)
                && delivery.file_status == InstructionFileStatus::Present
                && matches!(
                    delivery.status,
                    InstructionSourceStatus::Complete | InstructionSourceStatus::Truncated
                )
        });
    if let Some(delivery) = delivery {
        apply_delivery(capture, delivery);
        capture.should_deliver_primary = false;
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
