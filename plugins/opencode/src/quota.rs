//! OpenCodex 2.84.0 vendor-probes-key.ts: pattern only, no copied source.
use eliot_core::Result;
use eliot_plugin::quota as q;
use serde_json::Value;
pub(crate) async fn read(raw: &Value) -> Result<Value> {
    let params = q::decode(raw)?;
    let profile = &params.profile;
    let selector = profile
        .get("auth_integration")
        .and_then(Value::as_str)
        .unwrap_or("opencode-go");
    if selector != "opencode-go" {
        return Ok(
            serde_json::json!({"status":"unsupported","note":"Selected OpenCode integration has no verified read-only quota endpoint; Zen rate limits require inference and are not probed","details":{}}),
        );
    }
    let token = crate::native::auth::quota_credential(profile, Some(&params.environment)).await?;
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
