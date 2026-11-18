//! Read-only OpenCode provider quota: the Console sign-in path (`opencode`
//! integration) and the legacy Go path (`opencode-go`).
//!
//! Vendor behavior (pinned v2.2.7, anomalyco/opencode `fedc6f2`):
//! - `packages/protocol/src/groups/usage.ts`: `GET
//!   /api/experimental/usage/limits` returns the Console budgets plus the Go
//!   allowance of the signed-in workspace, or null when it is not signed in.
//! - `packages/core/src/usage-limits.ts`: the operation reads the active
//!   `opencode` integration connection (OAuth access token or API key),
//!   takes the workspace id from credential `metadata.orgID`, reads Console
//!   `GET {server}/api/policies/current-budgets` with `authorization:
//!   Bearer <token>` plus `x-org-id`, and Go `GET
//!   {origin}/zen/go/v1/usage` with the same bearer plus
//!   `x-opencode-org-id` (403 on either means no subscription there, never
//!   an error).
//! - `packages/core/src/console-access.ts`: the Console server comes from
//!   credential `metadata.server` (default `https://opencode.ai/console`;
//!   the retired `console.opencode.ai` host is rewritten path-for-path).
//!
//! Quota custody has no running serve, so the native endpoint itself is not
//! callable here: this module performs exactly the endpoint's underlying
//! reads directly against Console with the same auth and workspace binding,
//! in the existing read-only `auth export` custody. No inference probe is
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

/// The Console sign-in path: credential metadata binds the workspace, the
/// same bearer reads budgets and the Go allowance with the native headers.
async fn read_console(profile: &Value, environment: &Value) -> Result<Value> {
    let credential =
        crate::native::auth::quota_credential_for(profile, Some(environment), "opencode").await?;
    // The workspace id is the Console binding: budgets authenticate through
    // it and the Go read sends it as `x-opencode-org-id`, exactly like the
    // native operation. No binding means no workspace to read.
    let workspace = credential
        .org_id
        .clone()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| q::provider("OpenCode Console sign-in has no workspace binding"))?;
    let origin = console_origin(&credential.server)
        .ok_or_else(|| q::environment("OpenCode Console server is invalid"))?;
    let budgets_url = format!(
        "{}/api/policies/current-budgets",
        credential.server.trim_end_matches('/')
    );
    let go_url = format!("{origin}/zen/go/v1/usage");
    let budgets = console_get(
        &budgets_url,
        &credential.bearer,
        &[("x-org-id", workspace.as_str())],
    )
    .await?
    .unwrap_or(Value::Array(Vec::new()));
    let go = console_get(
        &go_url,
        &credential.bearer,
        &[("x-opencode-org-id", workspace.as_str())],
    )
    .await?;
    q::result(
        console_windows(&budgets, go.as_ref()),
        "OpenCode Console usage",
    )
}

/// The URL origin (`scheme://host`) of the Console server: the Go usage
/// read shares the Console's origin. `None` on any malformed server, so a
/// hostile credential value can never smuggle a path or query into the URL.
fn console_origin(server: &str) -> Option<String> {
    let (scheme, rest) = server.split_once("://")?;
    if scheme.is_empty() || rest.is_empty() {
        return None;
    }
    let host = rest.split('/').next()?;
    if host.is_empty() {
        return None;
    }
    Some(format!("{scheme}://{host}"))
}

/// Bounded read-only Console GET: no redirects, a 1 MiB cap, and static
/// errors only (the bearer travels in the header, never in errors or logs).
/// 403 is not an error: Console refuses a workspace the member has no
/// subscription in, which simply has no allowance to show.
async fn console_get(url: &str, bearer: &str, headers: &[(&str, &str)]) -> Result<Option<Value>> {
    if url.len() > 4096 {
        return Err(q::provider("provider quota URL exceeds its bound"));
    }
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .connect_timeout(std::time::Duration::from_secs(4))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| q::environment("quota HTTP client is unavailable"))?;
    let mut request = client
        .get(url)
        .bearer_auth(bearer)
        .header("Accept", "application/json");
    for (key, value) in headers {
        request = request.header(*key, *value);
    }
    let mut response = request
        .send()
        .await
        .map_err(|_| q::provider("provider quota transport failed or timed out"))?;
    if response.status() == reqwest::StatusCode::FORBIDDEN {
        return Ok(None);
    }
    if !response.status().is_success() {
        return Err(eliot_core::Error::new(
            "quota_unavailable",
            format!(
                "provider quota returned HTTP {}",
                response.status().as_u16()
            ),
        )
        .with_origin(eliot_core::FailureOrigin::Provider, "provider HTTP status"));
    }
    if response
        .content_length()
        .is_some_and(|size| size > 1024 * 1024)
    {
        return Err(q::provider("provider quota response exceeds 1 MiB"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| q::provider("provider quota response is unreadable"))?
    {
        if bytes.len() + chunk.len() > 1024 * 1024 {
            return Err(q::provider("provider quota response exceeds 1 MiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|_| q::provider("provider quota response schema is unavailable"))
}

/// Project Console budgets plus the Go allowance into quota windows. Pure
/// so the mapping is pinned without network: Go meters keep the legacy
/// 5-hour/Weekly/Monthly labels; each budget contributes its spend share of
/// its dollar limit. Entries without usable numbers are skipped, never
/// zero-filled.
fn console_windows(budgets: &Value, go: Option<&Value>) -> Vec<Value> {
    let mut windows = Vec::new();
    if let Some(go) = go {
        for (key, label, minutes) in [
            ("rolling", "5-hour", 300),
            ("weekly", "Weekly", 10080),
            ("monthly", "Monthly", 43200),
        ] {
            if let Some(bucket) = go.pointer(&format!("/usage/{key}"))
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
    }
    if let Some(list) = budgets.as_array() {
        for budget in list {
            let percent = match (
                budget.get("spentDollars").and_then(Value::as_f64),
                budget.get("limitDollars").and_then(Value::as_f64),
            ) {
                (Some(spent), Some(limit)) if limit > 0.0 => {
                    Some((spent / limit * 100.0).clamp(0.0, 100.0))
                }
                _ => None,
            };
            let label = format!(
                "Console {}",
                budget
                    .get("per")
                    .and_then(Value::as_str)
                    .unwrap_or("budget")
            );
            if let Some(window) = q::window(&label, percent, q::reset(budget.get("resetsAt")), None)
            {
                windows.push(window);
            }
        }
    }
    windows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_origin_keeps_only_scheme_and_host() {
        assert_eq!(
            console_origin("https://opencode.ai/console").as_deref(),
            Some("https://opencode.ai")
        );
        assert_eq!(
            console_origin("https://opencode.ai").as_deref(),
            Some("https://opencode.ai")
        );
        assert!(console_origin("not-a-url").is_none());
        assert!(console_origin("https://").is_none());
        assert!(console_origin("https:///path").is_none());
        assert!(console_origin("").is_none());
    }

    #[test]
    fn console_windows_projects_go_and_budgets() {
        let go = json!({"usage": {
            "rolling": {"status": "ok", "percent": 12.5, "resetsAt": "2026-11-18T00:00:00Z"},
            "weekly": {"status": "rate-limited", "percent": 80.0, "resetsAt": "2026-11-24T00:00:00Z"},
            "monthly": {"status": "ok", "percent": 3.0, "resetsAt": "2026-12-01T00:00:00Z"},
        }});
        let budgets = json!([
            {"ruleID": "r1", "limitDollars": 100.0, "spentDollars": 25.0,
             "per": "weekly", "resetsAt": "2026-11-24T00:00:00Z"},
            {"ruleID": "r2"},
        ]);
        let windows = console_windows(&budgets, Some(&go));
        assert_eq!(windows.len(), 4, "{windows:?}");
        assert_eq!(windows[0]["label"], "5-hour");
        assert_eq!(windows[0]["usedPercent"], 12.5);
        assert!(windows[0]["resetsAtMs"].is_number(), "{windows:?}");
        assert_eq!(windows[1]["label"], "Weekly");
        assert_eq!(windows[3]["label"], "Console weekly");
        assert_eq!(windows[3]["usedPercent"], 25.0);
        // No Go allowance and no usable budgets project to no windows.
        assert!(console_windows(&json!([]), None).is_empty());
        assert!(console_windows(&json!([{"ruleID": "r2"}]), None).is_empty());
    }
}
