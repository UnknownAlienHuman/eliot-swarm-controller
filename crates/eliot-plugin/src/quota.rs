//! Bounded read-only provider transport. Provider URLs and projection belong to plugins.
use eliot_core::{Error, FailureOrigin, Result};
use serde_json::{Value, json};
use std::{
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};

pub fn environment(message: &'static str) -> Error {
    Error::new("quota_unavailable", message).with_origin(
        FailureOrigin::Environment,
        "native credential read unavailable",
    )
}
pub fn provider(message: &'static str) -> Error {
    Error::new("quota_unavailable", message)
        .with_origin(FailureOrigin::Provider, "provider quota read failed")
}
pub fn home() -> Result<PathBuf> {
    std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .ok_or_else(|| environment("native home is unavailable"))
}
pub fn read_json(path: &Path) -> Result<Value> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| environment("native credential file is unavailable"))?
        .take(256 * 1024 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| environment("native credential file is unreadable"))?;
    if bytes.len() > 256 * 1024 {
        return Err(environment("native credential file exceeds its bound"));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| environment("native credential schema is unavailable"))
}
/// No redirects, no provider error bodies, no URL/transport errors containing secrets.
pub async fn read(
    url: &'static str,
    token: &str,
    body: Option<Value>,
    headers: &[(&'static str, &'static str)],
) -> Result<Value> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(8))
        .connect_timeout(Duration::from_secs(4))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| environment("quota HTTP client is unavailable"))?;
    let mut request = if let Some(body) = body {
        client
            .post(url)
            .header("Content-Type", "application/json")
            .body(body.to_string())
    } else {
        client.get(url)
    };
    request = request
        .bearer_auth(token)
        .header("Accept", "application/json");
    for (key, value) in headers {
        request = request.header(*key, *value);
    }
    let mut response = request
        .send()
        .await
        .map_err(|_| provider("provider quota transport failed or timed out"))?;
    if !response.status().is_success() {
        return Err(Error::new(
            "quota_unavailable",
            format!(
                "provider quota returned HTTP {}",
                response.status().as_u16()
            ),
        )
        .with_origin(FailureOrigin::Provider, "provider HTTP status"));
    }
    if response
        .content_length()
        .is_some_and(|size| size > 1024 * 1024)
    {
        return Err(provider("provider quota response exceeds 1 MiB"));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| provider("provider quota response is unreadable"))?
    {
        if bytes.len() + chunk.len() > 1024 * 1024 {
            return Err(provider("provider quota response exceeds 1 MiB"));
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| provider("provider quota response schema is unavailable"))
}
pub fn reset(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(number) => number.as_i64().and_then(|at| {
            if at < 0 {
                None
            } else if at < 100_000_000_000 {
                at.checked_mul(1000)
            } else {
                Some(at)
            }
        }),
        Value::String(text) => chrono::DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|at| at.timestamp_millis())
            .filter(|at| *at >= 0),
        _ => None,
    }
}
/// Unix milliseconds of a native RFC 3339 frame timestamp.
pub fn rfc3339_ms(value: Option<&Value>) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(value?.as_str()?)
        .ok()
        .map(|at| at.timestamp_millis())
        .filter(|at| *at > 0)
}
pub fn window(
    label: &str,
    percent: Option<f64>,
    reset: Option<i64>,
    minutes: Option<i64>,
) -> Option<Value> {
    let used = percent.filter(|used| used.is_finite() && *used >= 0.0 && *used <= 100.0)?;
    Some(json!({"label":label,"usedPercent":used,"resetsAtMs":reset,"windowDurationMins":minutes}))
}
pub fn result(windows: Vec<Value>, source: &'static str) -> Result<Value> {
    if windows.is_empty() {
        return Err(provider("provider quota returned no recognized windows"));
    }
    if windows.len() > 32 {
        return Err(provider("provider quota exceeds 32 windows"));
    }
    Ok(json!({"details":{"windows":windows,"source":source,"state":"ready","origin":"provider"}}))
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "Windows Credential Manager access requires this narrow FFI call."
)]
pub fn antigravity_credential() -> Result<Value> {
    use windows_sys::Win32::Security::Credentials::{
        CRED_TYPE_GENERIC, CREDENTIALW, CredFree, CredReadW,
    };
    let target: Vec<u16> = "gemini:antigravity\0".encode_utf16().collect();
    let mut credential: *mut CREDENTIALW = std::ptr::null_mut();
    // Same native Windows Credential Manager entry as AGY's go-keyring store.
    if unsafe { CredReadW(target.as_ptr(), CRED_TYPE_GENERIC, 0, &mut credential) } == 0 {
        return Err(environment(
            "Antigravity native keyring credential is unavailable",
        ));
    }
    let result = unsafe {
        let row = &*credential;
        if row.CredentialBlobSize > 256 * 1024 || row.CredentialBlob.is_null() {
            Err(environment(
                "Antigravity native keyring credential exceeds its bound",
            ))
        } else {
            serde_json::from_slice(std::slice::from_raw_parts(
                row.CredentialBlob,
                row.CredentialBlobSize as usize,
            ))
            .map_err(|_| environment("Antigravity native keyring credential schema is unavailable"))
        }
    };
    unsafe { CredFree(credential.cast()) };
    result
}
#[cfg(not(windows))]
pub fn antigravity_credential() -> Result<Value> {
    Err(environment(
        "Antigravity native credential reader is currently Windows-only",
    ))
}

pub fn decode(params: &Value) -> Result<crate::protocol::QuotaReadParams> {
    let params: crate::protocol::QuotaReadParams = serde_json::from_value(params.clone())
        .map_err(|_| environment("quota.read request is invalid"))?;
    if !params.profile.is_object()
        || !params.environment.is_object()
        || params
            .environment
            .as_object()
            .is_some_and(|env| env.values().any(|v| !v.is_string()))
    {
        return Err(environment("quota.read profile/environment is invalid"));
    }
    Ok(params)
}
