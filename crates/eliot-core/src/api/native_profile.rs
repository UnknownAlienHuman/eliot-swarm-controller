//! The native profile vocabulary each harness maps, owned once. The daemon's
//! profile builders validate against it, the plugins decode it, and the
//! desktop lists it. Frozen profiles keep the plain string values.

use serde::{Deserialize, Serialize};

/// Codex `permissions`, its sandbox mode. An omitted value is
/// `workspace-write`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexPermissions {
    ReadOnly,
    #[default]
    WorkspaceWrite,
    DangerFullAccess,
}

impl CodexPermissions {
    pub const ALL: [Self; 3] = [Self::ReadOnly, Self::WorkspaceWrite, Self::DangerFullAccess];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
            Self::DangerFullAccess => "danger-full-access",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|mode| mode.as_str() == value)
    }
}

/// Codex `approval_policy`. An omitted value is `never`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CodexApprovalPolicy {
    #[default]
    Never,
    OnRequest,
}

impl CodexApprovalPolicy {
    pub const ALL: [Self; 2] = [Self::Never, Self::OnRequest];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::OnRequest => "on-request",
        }
    }
}

/// Codex `collaboration_mode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CodexCollaborationMode {
    Default,
    Plan,
}

impl CodexCollaborationMode {
    pub const ALL: [Self; 2] = [Self::Default, Self::Plan];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Plan => "plan",
        }
    }
}

/// The Codex-specific fields of a frozen profile, decoded once at the plugin
/// boundary. Other keys of the snapshot are read by their own owners.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct CodexProfileFields {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub daemon_model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub collaboration_mode: Option<CodexCollaborationMode>,
    #[serde(default)]
    pub permissions: CodexPermissions,
    #[serde(default)]
    pub approval_policy: CodexApprovalPolicy,
    #[serde(default)]
    pub native_subagents: Option<String>,
}

/// Claude Code `--permission-mode` values (profile key `approval`).
pub const CLAUDE_PERMISSION_MODES: [&str; 6] = [
    "manual",
    "auto",
    "acceptEdits",
    "plan",
    "dontAsk",
    "bypassPermissions",
];

/// Claude Code `--effort` levels: the CLI's closed vocabulary.
pub const CLAUDE_EFFORTS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

/// Muse `approval` modes.
pub const MUSE_APPROVALS: [&str; 4] = ["onRequest", "promptUnmatched", "denyUnmatched", "allowAll"];

/// Muse `permissions`: the subset of the Codex sandbox modes Muse maps.
pub const MUSE_PERMISSIONS: [CodexPermissions; 2] = [
    CodexPermissions::WorkspaceWrite,
    CodexPermissions::DangerFullAccess,
];

/// Antigravity `mode` values.
pub const ANTIGRAVITY_MODES: [&str; 3] = ["plan", "auto", "accept-edits"];

/// The effort tiers the desktop offers for Codex and Muse when the harness
/// catalogue gives none.
pub const COMMON_EFFORTS: [&str; 4] = ["low", "medium", "high", "xhigh"];
