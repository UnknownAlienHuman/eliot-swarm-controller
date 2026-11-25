//! The native profile vocabulary each harness maps, owned once. The daemon's
//! profile builders validate against it, the plugins decode it, and the
//! desktop lists it. Frozen profiles keep the plain string values.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Typed vendor-facing part of a named profile. The daemon retains the
/// source location and validates all shared fields.
pub struct NativeProfileInput<'a> {
    pub model: Option<&'a str>,
    pub effort: Option<&'a str>,
    pub approval: Option<&'a str>,
    pub collaboration_mode: Option<&'a str>,
    pub permissions: Option<&'a str>,
    pub approval_policy: Option<&'a str>,
    pub project_doc_max_bytes: Option<u64>,
    pub auth_integration: Option<&'a str>,
    pub mode: Option<&'a str>,
    pub dangerously_skip_permissions: Option<bool>,
    pub local_provider: Option<&'a crate::local_provider::LocalProvider>,
}

pub struct NativeProfileRefusal {
    pub field: &'static str,
    pub reason: String,
    pub code: &'static str,
}

impl NativeProfileRefusal {
    pub fn new(field: &'static str, reason: impl Into<String>, code: &'static str) -> Self {
        Self {
            field,
            reason: reason.into(),
            code,
        }
    }
}

pub type NativeProfileResult<T> = Result<T, NativeProfileRefusal>;
type MapNativeFields =
    fn(&NativeProfileInput<'_>, &mut Map<String, Value>) -> NativeProfileResult<()>;
type MapNativeRoute =
    fn(&NativeProfileInput<'_>, &[String], &mut Map<String, Value>) -> NativeProfileResult<()>;

#[derive(Clone, Copy, Debug)]
pub enum ProfileEffort {
    Freeform,
    Choices {
        allowed: &'static [&'static str],
        refusal: &'static str,
    },
    Adapter,
}

/// Adapter-owned validation, defaults and native field projection.
#[derive(Clone, Copy, Debug)]
pub struct NativeProfileMapping {
    pub name: &'static str,
    pub model: Option<fn(&str) -> NativeProfileResult<()>>,
    pub effort: ProfileEffort,
    pub fields: MapNativeFields,
    pub route: Option<MapNativeRoute>,
    pub mcp_minimal: bool,
    pub mcp_allow: bool,
    pub control_before_mcp: bool,
}

/// Native subagent policy for one built-in harness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeSubagentsPolicy {
    /// Whether omitted `native_subagents` resolves to `allow`.
    pub default_allowed: bool,
    /// Whether the harness can disable native subagents for one session.
    pub can_deny: bool,
}

impl NativeSubagentsPolicy {
    /// Build a capability that denies native subagents by default.
    pub const fn deny_by_default() -> Self {
        Self {
            default_allowed: false,
            can_deny: true,
        }
    }

    /// Build a capability that preserves native subagents by default when
    /// the harness cannot disable them for one session.
    pub const fn allow_by_default() -> Self {
        Self {
            default_allowed: true,
            can_deny: false,
        }
    }

    /// The serialized value for this harness's omitted profile setting.
    pub const fn default_value(self) -> &'static str {
        if self.default_allowed {
            "allow"
        } else {
            "deny"
        }
    }
}

/// Shared profile sandbox permissions. An omitted value is
/// `workspace-write`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxPermissions {
    ReadOnly,
    #[default]
    WorkspaceWrite,
    DangerFullAccess,
}

impl SandboxPermissions {
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

/// Shared profile approval policy. An omitted value is `never`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxApprovalPolicy {
    #[default]
    Never,
    OnRequest,
}

impl SandboxApprovalPolicy {
    pub const ALL: [Self; 2] = [Self::Never, Self::OnRequest];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Never => "never",
            Self::OnRequest => "on-request",
        }
    }
}

/// Shared profile collaboration mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CollaborationMode {
    Default,
    Plan,
}

impl CollaborationMode {
    pub const ALL: [Self; 2] = [Self::Default, Self::Plan];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Plan => "plan",
        }
    }
}

/// The effort tiers the desktop offers for Codex and Muse when the harness
/// catalogue gives none.
pub const COMMON_EFFORTS: [&str; 4] = ["low", "medium", "high", "xhigh"];
