//! Read-only OpenCode provider quota: the Console sign-in path (`opencode`
//! integration) and the legacy Go path (`opencode-go`).
//!
//! Vendor behavior (pinned v2.2.7, anomalyco/opencode `fedc6f2`):
//! - `packages/protocol/src/groups/usage.ts`: `GET
//!   /api/experimental/usage/limits` returns the Console budgets plus the Go
//!   allowance of the signed-in workspace, or null when it is not signed in.
//! - `packages/core/src/usage-limits.ts`: the operation reads the ACTIVE
//!   `opencode` integration connection and resolves it (refreshing a stale
//!   credential once the Console plugin has registered its OAuth refresh)
//!   before deriving the workspace binding, so a stored export read
//!   directly can fail where the native operation succeeds. A one-shot
//!   cold boot can serve the operation before that registration lands (see
//!   `native::usage`); that failure surfaces here as `quota_unavailable`
//!   (unknown), never as ready windows.
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
    let selector = profile.get("auth_integration").and_then(Value::as_str);
    if profile.get("provider").is_some() {
        return Ok(
            json!({"status":"unsupported","note":"Custom OpenCode provider fragment has no verified provider quota reader","details":{}}),
        );
    }
    if selector == Some("opencode-go") {
        return read_go_legacy(profile, &params.environment).await;
    }
    if selector == Some("opencode") {
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
    let mut reply = q::result(windows, "OpenCode Go usage")?;
    reply["details"]["integration"] = json!("opencode-go");
    Ok(reply)
}

/// The Console sign-in path: the installed
/// `api experimental.usage.limits --standalone` operation answers the
/// Console budgets plus the Go allowance with native connection resolution
/// (active connection plus credential refresh once registered), or null
/// when the location is not signed in. Null maps to `unsupported`: there
/// is no workspace to show quotas for. Any other failure (cold-start
/// refresh race, boot failure, malformed output) maps to
/// `quota_unavailable`: an unread quota is unknown, never a ready or
/// unsupported answer.
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
    let mut reply = q::result(
        crate::native::usage::usage_windows(&info),
        "OpenCode Console usage",
    )?;
    reply["details"]["integration"] = json!("opencode");
    reply["details"]["consoleWindows"] = json!(crate::native::usage::scoped_budget_windows(&info));
    if serde_json::to_vec(&reply["details"]).is_ok_and(|encoded| {
        encoded.len() > eliot_core::api::harness::MAX_HARNESS_QUOTA_DETAILS_BYTES
    }) && let Some(details) = reply["details"].as_object_mut()
    {
        // Retain the display windows. A partial set of scope evidence
        // cannot establish a routing verdict for this workspace.
        details.remove("consoleWindows");
    }
    Ok(reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn omitted_and_adopted_selectors_do_not_read_go_quota() {
        for profile in [
            json!({"provider":{"k173d-fixture":{"package":"p"}}}),
            json!({"provider":{"k173d-fixture":{"package":"p"}},"auth_integration":"opencode-go"}),
            json!({}),
        ] {
            let raw = json!({"profile":profile,"environment":{}});
            assert_eq!(read(&raw).await.unwrap()["status"], "unsupported");
        }
    }

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

    #[tokio::test]
    async fn unreadable_console_quota_reports_unknown() {
        // A one-shot boot that never starts (missing binary) is an
        // unknown read, never a ready or unsupported answer: the adapter
        // must not trust what it could not read.
        let raw = json!({
            "profile": {"auth_integration": "opencode", "cmd": "/definitely/not/here/eliot-opencode-stub"},
            "environment": {},
        });
        let error = read(&raw).await.expect_err("unreadable quota fails");
        assert_eq!(error.code, "quota_unavailable");
    }
}
