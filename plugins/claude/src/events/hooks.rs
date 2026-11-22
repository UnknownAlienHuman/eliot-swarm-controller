//! Synchronous hook translation, redaction and response framing.

use eliot_core::{ContextEditPath, SecretPolicy};
use eliot_plugin::native_transport::encode_frame;
use serde_json::Value;
use std::{path::Path, time::Duration};
use tokio::time::Instant;

use crate::native::types::{
    HookCallbackResponse, OutboundControlResponse, OutboundControlResponseBody,
};

pub(crate) const TOOL_HOOK_TIMEOUT_SECONDS: u64 = 3;
/// Char bounds for the optional tool-hook progress diagnostics. The
/// committed event envelope stays under the shared `MAX_EVENT_BYTES` bound;
/// these per-field caps keep each diagnostic small on its own. Redaction runs
/// before truncation so a cap can never split a configured secret.
const MAX_HOOK_TOOL_NAME_CHARS: usize = 64;
const MAX_HOOK_TOOL_ID_CHARS: usize = 128;
const MAX_HOOK_OPERATION_CHARS: usize = 32;
const MAX_HOOK_PATH_CHARS: usize = 512;
/// Compose a decision-relevant plain-text summary of a `can_use_tool` ask:
/// the tool name plus its semantic string arguments (command, description,
/// cwd, file/path targets) flattened from the native input. This is plain
/// text, not a native record: transport ids and the verbatim input stay
/// private in the pending struct above. No summary cap beyond the owning
/// event-size bound: truncation would hide decision-critical suffixes and,
/// if done before redaction, could split a configured secret and defeat
/// full-secret redaction. Callers redact this composed text before the
/// existing event-size decision.
pub(super) fn approval_summary(tool_name: &str, input: Option<&Value>) -> String {
    let mut parts = vec![format!("{tool_name} tool permission request")];
    let mut flattened = Vec::new();
    collect_summary_args(input, &mut flattened);
    flattened.sort_by(|a, b| {
        summary_rank(&a.0)
            .cmp(&summary_rank(&b.0))
            .then_with(|| a.0.cmp(&b.0))
            .then_with(|| a.1.cmp(&b.1))
    });
    // One field per line; a multi-line value (a file edit, a script) starts
    // on its own line so it reads as a block instead of running on.
    for (key, value) in flattened {
        let label = summary_label(&key);
        if value.contains('\n') {
            parts.push(format!("{label}:\n{}", value.trim_end()));
        } else {
            parts.push(format!("{label}: {value}"));
        }
    }
    parts.join("\n")
}

/// Known tool-input keys in display order, with their owner-facing labels.
/// Unknown keys sort after these and keep their raw key as the label.
const SUMMARY_FIELDS: [(&str, &str); 15] = [
    ("file_path", "File"),
    ("notebook_path", "Notebook"),
    ("path", "Path"),
    ("paths", "Paths"),
    ("command", "Command"),
    ("description", "Description"),
    ("url", "URL"),
    ("query", "Query"),
    ("pattern", "Pattern"),
    ("prompt", "Prompt"),
    ("old_string", "Replace"),
    ("new_string", "With"),
    ("content", "Content"),
    ("cell_id", "Cell"),
    ("new_source", "New source"),
];

fn summary_rank(key: &str) -> usize {
    SUMMARY_FIELDS
        .iter()
        .position(|(raw, _)| *raw == key)
        .unwrap_or(SUMMARY_FIELDS.len())
}

fn summary_label(key: &str) -> &str {
    SUMMARY_FIELDS
        .iter()
        .find(|(raw, _)| *raw == key)
        .map_or(key, |(_, label)| label)
}

/// Flatten the string leaves of a native input value into owned (key,
/// value) pairs, keeping the field label for string arrays (e.g. a path
/// array reads "paths: <joined>") so operation fields survive without a
/// JSON dump. Dropped: transport envelope ids and any non-string leaf.
fn collect_summary_args(input: Option<&Value>, out: &mut Vec<(String, String)>) {
    collect_summary_args_labeled(None, input, out);
}

fn collect_summary_args_labeled(
    label: Option<&str>,
    input: Option<&Value>,
    out: &mut Vec<(String, String)>,
) {
    let Some(input) = input else { return };
    match input {
        Value::String(text) if !text.trim().is_empty() => {
            if let Some(label) = label {
                out.push((label.to_owned(), text.clone()));
            }
        }
        Value::Object(map) => {
            for (key, value) in map {
                match value {
                    Value::String(text) if !text.trim().is_empty() => {
                        out.push((key.clone(), text.clone()));
                    }
                    Value::Object(_) | Value::Array(_) => {
                        collect_summary_args_labeled(Some(key), Some(value), out)
                    }
                    _ => {}
                }
            }
        }
        Value::Array(items) => {
            let strings: Vec<String> = items
                .iter()
                .filter_map(Value::as_str)
                .filter(|text| !text.trim().is_empty())
                .map(ToOwned::to_owned)
                .collect();
            if !strings.is_empty()
                && let Some(label) = label
            {
                out.push((label.to_owned(), strings.join(", ")));
            }
            for item in items {
                collect_summary_args_labeled(label, Some(item), out);
            }
        }
        _ => {}
    }
}

/// Extract the optional bounded progress diagnostics for one Claude tool hook
/// input. Only the native tool name, the already-matched invocation id, a
/// closed-vocabulary operation kind, the workspace-relative path, and only for
/// successful PostToolUse writes the bounded native `file_path` survive. Full
/// command lines, file contents and every other argument are dropped here,
/// never persisted. Every kept field is redacted before its char cap, so a cap
/// cannot split a configured secret.
pub(super) fn hook_tool_facts(
    input: &Value,
    native_tool_id: &str,
    workspace: &Path,
    policy: &SecretPolicy,
    include_edit_path: bool,
) -> HookToolFacts {
    let tool_name = input
        .get("tool_name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|name| !name.is_empty());
    let operation = tool_name.and_then(hook_operation_for_tool);
    let edit_path = (include_edit_path && operation.as_deref() == Some("write"))
        .then(|| hook_edit_path(input.get("tool_input"), workspace, policy))
        .flatten();
    HookToolFacts {
        operation,
        path: (!include_edit_path)
            .then(|| hook_tool_path(input.get("tool_input"), workspace, policy))
            .flatten(),
        edit_path,
        tool: tool_name
            .map(|name| truncate_chars(&policy.redact_text(name), MAX_HOOK_TOOL_NAME_CHARS)),
        native_tool_id: {
            let id = native_tool_id.trim();
            (!id.is_empty())
                .then(|| truncate_chars(&policy.redact_text(id), MAX_HOOK_TOOL_ID_CHARS))
        },
    }
}

/// Closed operation vocabulary derived from the native tool name only, never
/// from arguments. File readers map to `read`, file writers to `write`, and
/// shell execution to `exec`; every other tool carries no operation, so no
/// novel native surface invents a persisted kind.
fn hook_operation_for_tool(tool_name: &str) -> Option<String> {
    let operation = match tool_name.to_ascii_lowercase().as_str() {
        "read" | "glob" | "grep" => "read",
        "edit" | "write" | "notebookedit" => "write",
        "bash" => "exec",
        _ => return None,
    };
    (operation.len() <= MAX_HOOK_OPERATION_CHARS).then(|| operation.to_owned())
}

/// A single safe file path from the native `tool_input` object, or `None`.
/// Only the top-level `file_path`/`path` string leaves are read; nested
/// objects, arrays and every other key (commands, contents, queries) are
/// ignored. The kept path is normalized workspace-relative exactly like a
/// native file hint: an out-of-workspace path is dropped, never persisted.
fn hook_tool_path(
    input: Option<&Value>,
    workspace: &Path,
    policy: &SecretPolicy,
) -> Option<String> {
    let candidate = input
        .and_then(Value::as_object)
        .and_then(|fields| {
            fields
                .get("file_path")
                .or_else(|| fields.get("path"))
                .and_then(Value::as_str)
        })
        .map(str::trim)
        .filter(|path| !path.is_empty())?;
    let relative = normalize_hook_path(workspace, candidate)?;
    let safe = policy.redact_text(&relative);
    (!safe.trim().is_empty()).then(|| truncate_chars(&safe, MAX_HOOK_PATH_CHARS))
}

fn hook_edit_path(
    input: Option<&Value>,
    workspace: &Path,
    policy: &SecretPolicy,
) -> Option<ContextEditPath> {
    let path = input
        .and_then(Value::as_object)
        .and_then(|fields| fields.get("file_path").or_else(|| fields.get("path")))
        .and_then(Value::as_str)
        .filter(|path| !path.trim().is_empty())?;
    Some(
        match eliot_plugin::edit_path::normalize(&workspace.to_string_lossy(), path) {
            Ok(relative) if relative.len() <= 4096 && policy.redact_text(&relative) == relative => {
                ContextEditPath::Relative { path: relative }
            }
            _ => ContextEditPath::outside(&workspace.join(path).to_string_lossy(), policy),
        },
    )
}

/// Lexical workspace-relative normalization mirroring the Store's native
/// file-hint normalization: portable separators, lexical `.`/`..` resolution,
/// case-insensitive prefix match on Windows roots, and `None` for any path
/// that escapes the workspace or is not a clean relative path. No filesystem
/// access: containment and symlink checks belong to the daemon.
fn normalize_hook_path(workspace: &Path, path: &str) -> Option<String> {
    fn portable(value: &str) -> String {
        let value = value.replace('\\', "/");
        if let Some(rest) = value.strip_prefix("//?/UNC/") {
            format!("//{rest}")
        } else {
            value.strip_prefix("//?/").unwrap_or(&value).to_owned()
        }
    }
    fn parts(value: &str) -> Option<Vec<&str>> {
        let mut out = Vec::new();
        for part in value
            .split('/')
            .filter(|part| !part.is_empty() && *part != ".")
        {
            if part == ".." {
                out.pop()?;
            } else {
                out.push(part);
            }
        }
        Some(out)
    }
    if path.is_empty() || path.contains('\0') {
        return None;
    }
    let workspace = portable(&workspace.to_string_lossy());
    let path = portable(path);
    let windows = workspace.starts_with("//") || workspace.as_bytes().get(1) == Some(&b':');
    let absolute = path.starts_with('/') || path.as_bytes().get(1) == Some(&b':');
    let base = parts(&workspace)?;
    let candidate = parts(&path)?;
    let relative = if absolute {
        if candidate.len() <= base.len()
            || !base.iter().zip(&candidate).all(|(base, candidate)| {
                if windows {
                    base.eq_ignore_ascii_case(candidate)
                } else {
                    base == candidate
                }
            })
        {
            return None;
        }
        candidate[base.len()..].join("/")
    } else {
        candidate.join("/")
    };
    if relative.is_empty()
        || relative.contains('\\')
        || relative.contains('\0')
        || relative
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        return None;
    }
    #[cfg(windows)]
    if relative.split('/').any(|part| part.contains(':')) {
        return None;
    }
    Some(relative)
}

/// Truncate to a char bound on a char boundary. The input is already
/// redacted, so no configured secret can be split by the cap.
fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_owned();
    }
    value.chars().take(max_chars).collect()
}

/// Optional bounded progress diagnostics for one tool-hook fact.
/// Every field is redacted and capped; absent native data stays absent, never
/// an empty string. Only these fields are ever persisted from the native
/// hook input: full command lines, contents and remaining arguments are
/// dropped at extraction.
#[derive(Clone, Debug, Default)]
pub(super) struct HookToolFacts {
    /// Bounded native tool name (`tool_name`).
    tool: Option<String>,
    /// Bounded native invocation id (the matched `tool_use_id`).
    native_tool_id: Option<String>,
    /// Closed-vocabulary operation kind derived from the tool name.
    operation: Option<String>,
    /// Workspace-relative file path, normalized like native file hints.
    path: Option<String>,
    edit_path: Option<ContextEditPath>,
}

impl HookToolFacts {
    pub(super) fn has_edit(&self) -> bool {
        self.edit_path.is_some()
    }

    pub(super) fn attach(&self, data: &mut Value) {
        let Some(fields) = data.as_object_mut() else {
            return;
        };
        if let Some(tool) = &self.tool {
            fields.insert("tool".to_owned(), Value::String(tool.clone()));
        }
        if let Some(native_tool_id) = &self.native_tool_id {
            fields.insert(
                "native_tool_id".to_owned(),
                Value::String(native_tool_id.clone()),
            );
        }
        if let Some(operation) = &self.operation {
            fields.insert("operation".to_owned(), Value::String(operation.clone()));
        }
        if let Some(path) = &self.path {
            fields.insert("path".to_owned(), Value::String(path.clone()));
        }
        if let Some(edit_path) = &self.edit_path {
            fields.insert("edit_path".to_owned(), serde_json::json!(edit_path));
        }
    }
}

pub(super) fn hook_deadline(deadline_ms: Option<u64>) -> Instant {
    let limit = TOOL_HOOK_TIMEOUT_SECONDS * 1000;
    let timeout_ms = deadline_ms.unwrap_or(limit).min(limit);
    Instant::now() + Duration::from_millis(timeout_ms)
}

pub(super) fn hook_callback_response_fits(
    request_id: &str,
    response: &HookCallbackResponse,
) -> bool {
    encode_frame(
        "Claude",
        &OutboundControlResponse {
            kind: "control_response",
            response: OutboundControlResponseBody {
                subtype: "success",
                request_id: request_id.to_owned(),
                response: response.clone(),
            },
        },
    )
    .is_ok()
}
