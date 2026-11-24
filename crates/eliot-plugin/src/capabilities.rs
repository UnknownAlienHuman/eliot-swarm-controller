//! Shared application of adapter-declared profile and instruction defaults.

use eliot_core::api::harness::{
    AdhocAccess, AdhocLaunchDefaults, AdhocModel, AdhocPermissions, NativeQuotaReadback,
    ProjectInstructionRead, QuotaProjectionFormat,
};
use eliot_core::api::native_profile::{CodexApprovalPolicy, CodexPermissions};
use eliot_core::{Error, McpPolicy, Result, api::InstructionReadStatus};
use serde_json::{Map, Value};
use std::path::Path;

pub const fn instruction_files(
    paths: &'static [(&'static str, InstructionReadStatus)],
) -> ProjectInstructionRead {
    ProjectInstructionRead {
        paths,
        report_version: false,
        byte_limit: None,
    }
}

pub const fn quota_readback(
    projection: QuotaProjectionFormat,
    description: fn(&Value) -> &'static str,
) -> NativeQuotaReadback {
    NativeQuotaReadback {
        projection,
        method: "quota.read",
        adhoc_integration: None,
        description,
        unsupported_note: None,
        route_refusal: None,
    }
}

/// Validate the shape of an explicitly pinned standalone native executable.
pub fn absolute_native_command_refusal(cmd: &str) -> Option<&'static str> {
    if !Path::new(cmd).is_absolute() {
        Some("must be an absolute native executable path, not a bare command name")
    } else if cfg!(windows) && !cmd.to_ascii_lowercase().ends_with(".exe") {
        Some("must name the native .exe, not a script wrapper")
    } else {
        None
    }
}

pub const fn sandbox_permissions(
    full_changes_sandbox: bool,
    field: &'static str,
    values: [&'static str; 3],
) -> AdhocPermissions {
    AdhocPermissions::Sandbox {
        full_changes_sandbox,
        field,
        values,
    }
}

pub const fn adhoc_launch(
    permissions: AdhocPermissions,
    installed_profile_mcp: McpPolicy,
) -> AdhocLaunchDefaults {
    AdhocLaunchDefaults {
        permissions,
        model: AdhocModel::NativeDefault,
        mcp: McpPolicy::Inherit,
        pass_env: &[],
        installed_profile_mcp,
    }
}

pub fn apply_adhoc_permissions(
    snapshot: &mut Map<String, Value>,
    access: Option<AdhocAccess>,
    permissions: AdhocPermissions,
) -> Result<()> {
    let full = access == Some(AdhocAccess::Full);
    match permissions {
        AdhocPermissions::Sandbox {
            full_changes_sandbox,
            field,
            values,
        } => {
            let sandbox = if full && full_changes_sandbox {
                CodexPermissions::DangerFullAccess
            } else {
                CodexPermissions::default()
            };
            snapshot.insert("permissions".to_owned(), Value::from(sandbox.as_str()));
            snapshot.insert(
                "approval_policy".to_owned(),
                Value::from(CodexApprovalPolicy::default().as_str()),
            );
            if let Some(access) = access {
                let [ask, auto, full] = values;
                let value = match access {
                    AdhocAccess::Ask => ask,
                    AdhocAccess::Auto => auto,
                    AdhocAccess::Full => full,
                };
                snapshot.insert(field.to_owned(), Value::from(value));
            }
        }
        AdhocPermissions::FullFlag(field) => {
            snapshot.insert(field.to_owned(), Value::Bool(full));
        }
        AdhocPermissions::RefuseFull(refusal) if full => {
            return Err(Error::new("unsupported", refusal));
        }
        AdhocPermissions::RefuseFull(_) => {}
    }
    Ok(())
}

pub fn project_instruction_read(
    rules: ProjectInstructionRead,
    path: &str,
    profile: &Value,
    size_bytes: Option<u64>,
) -> (InstructionReadStatus, Option<String>) {
    use InstructionReadStatus::{Documented, NotNative, Partial, Unknown};
    let Some((_, mut status)) = rules.paths.iter().copied().find(|(name, _)| *name == path) else {
        return (Unknown, None);
    };
    if status == Documented
        && let Some(limit) = rules.byte_limit
    {
        status = match limit(profile) {
            0 => NotNative,
            limit if size_bytes.is_some_and(|bytes| bytes > limit) => Partial,
            _ => Documented,
        };
    }
    let version = rules
        .report_version
        .then(|| profile["cmd_version"].as_str().map(str::to_owned))
        .flatten();
    (status, version)
}
