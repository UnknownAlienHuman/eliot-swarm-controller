//! Read-only OpenCode provider quota: the Console sign-in path (`opencode`
//! integration) and the legacy Go path (`opencode-go`).
//!
//! Vendor behavior (pinned v2.2.7, anomalyco/opencode `fedc6f2`):
//! - `packages/protocol/src/groups/usage.ts`: `GET
//!   /api/experimental/usage/limits` returns the Console budgets plus the Go
//!   allowance of the signed-in workspace, or null when it is not signed in.
//! - `packages/core/src/usage-limits.ts`: the operation reads the ACTIVE
//!   `opencode` integration connection and resolves it (refreshing a stale
//!   credential) before deriving the workspace binding, so a stored export
//!   read directly can fail where the native operation succeeds.
//! - `packages/cli/src/commands/commands.ts`: `opencode api <operation>
//!   --standalone` runs the operation against a private server, so quota
//!   custody (which has no running serve) still resolves natively.
//!
//! Quota custody has no running serve, so the Console sign-in path shells
//! the installed `api experimental.usage.limits --standalone` operation
//! (see `native::usage`): the native connection resolution, credential
//! refresh and workspace binding apply unchanged. No inference probe is
//! ever issued and no secret is logged. Selectors without a verified
//! read-only path stay `unsupported`.
use eliot_core::Result;
use eliot_plugin::quota as q;
use serde_json::{Value, json};
pub(crate) async fn read(raw: &Value) -> Result<Value> {
    let params = q::decode(raw)?;
    let profile = &params.profile;
    let selector = profile
        .get("auth_integration")
        .and_then(Value::as_str)
        .unwrap_or("opencode-go");
    if selector == "opencode-go" {
        return read_go_legacy(profile, &params.environment).await;
    }
    if selector == "opencode" {
        return read_console(profile, &params.environment).await;
    }
    Ok(
        json!({"status":"unsupported","note":"Selected OpenCode integration has no verified read-only quota path; supported selectors are opencode (Console sign-in) and opencode-go (Go credential)","details":{}}),
    )
}

/// The legacy Go-only path: the exported Go key authenticates the public Go
/// usage URL. Unchanged behavior for the `opencode-go` selector.
async fn read_go_legacy(profile: &Value, environment: &Value) -> Result<Value> {
    let token = crate::native::auth::quota_credential(profile, Some(environment)).await?;
    let body = q::read("https://opencode.ai/zen/go/v1/usage", &token, None, &[]).await?;
    let mut windows = Vec::new();
    for (key, label, minutes) in [
        ("rolling", "5-hour", 300),
        ("weekly", "Weekly", 10080),
        ("monthly", "Monthly", 43200),
    ] {
        if let Some(bucket) = body.pointer(&format!("/usage/{key}"))
            && let Some(window) = q::window(
                label,
                bucket.get("percent").and_then(Value::as_f64),
                q::reset(bucket.get("resetsAt")),
                Some(minutes),
            )
        {
            windows.push(window);
        }
    }
    q::result(windows, "OpenCode Go usage")
}

/// The Console sign-in path: the installed
/// `api experimental.usage.limits --standalone` operation answers the
/// Console budgets plus the Go allowance with native connection resolution
/// (active connection plus credential refresh), or null when the location
/// is not signed in. Null maps to `unsupported`: there is no workspace to
/// show quotas for.
async fn read_console(profile: &Value, environment: &Value) -> Result<Value> {
    let program = profile
        .get("cmd")
        .and_then(Value::as_str)
        .ok_or_else(|| q::environment("OpenCode native executable is unavailable"))?;
    let info = crate::native::usage::run_usage_limits(
        std::ffi::OsStr::new(program),
        environment.as_object(),
        crate::native::usage::child_deadline(),
    )
    .await
    .map_err(|error| {
        let spawned = error.code == "native_spawn";
        let message = *error.message;
        if spawned {
            eliot_core::Error::new("quota_unavailable", message).with_origin(
                eliot_core::FailureOrigin::Environment,
                "native credential read unavailable",
            )
        } else {
            eliot_core::Error::new("quota_unavailable", message).with_origin(
                eliot_core::FailureOrigin::Provider,
                "provider quota read failed",
            )
        }
    })?;
    let Some(info) = info else {
        return Ok(
            json!({"status":"unsupported","note":"OpenCode Console sign-in has no workspace to report quotas for (the location is not signed in)","details":{}}),
        );
    };
    q::result(
        crate::native::usage::usage_windows(&info),
        "OpenCode Console usage",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unknown_selector_stays_unsupported() {
        let raw = json!({
            "profile": {"auth_integration": "other"},
            "environment": {},
        });
        let value = read(&raw).await.expect("unsupported answers");
        assert_eq!(
            value.get("status").and_then(Value::as_str),
            Some("unsupported")
        );
    }
}
