//! Bounded readiness reads for explicitly configured adopted routes.
//! This module never sends client/admin credentials and never owns the service.

use eliot_core::{Error, Result, RouteBinding};
use reqwest::{Client, Url, redirect, retry};
use serde_json::Value;
use std::time::Duration;
use tokio::time::{Instant, timeout_at};

pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(25);
const MAX_READINESS_BYTES: usize = 16 * 1024;

pub(crate) async fn check_profile_ready(profile: &Value) -> Result<()> {
    let Some(value) = profile.get("route_binding") else {
        return Ok(());
    };
    let binding: RouteBinding = serde_json::from_value(value.clone())
        .map_err(|_| Error::new("source_integrity", "frozen route binding is malformed"))?;
    check_ready(&binding, Instant::now() + READ_TIMEOUT).await
}

async fn check_ready(binding: &RouteBinding, until: Instant) -> Result<()> {
    let url = readiness_url(binding)?;
    let client = Client::builder()
        .no_proxy()
        .redirect(redirect::Policy::none())
        .retry(retry::never())
        .build()
        .map_err(|_| Error::new("unavailable", "cannot initialize route readiness client"))?;

    timeout_at(until, async {
        // Readiness proves availability only, not authentication or inference.
        // It is a public GET; never attach the model key or management credentials.
        let mut response = client
            .get(url)
            .send()
            .await
            .map_err(|_| Error::new("unavailable", "route readiness request failed"))?;
        if !response.status().is_success() {
            return Err(Error::new(
                "unavailable",
                format!("route is not ready (HTTP {})", response.status().as_u16()),
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_READINESS_BYTES as u64)
        {
            return Err(Error::new(
                "unavailable",
                "route readiness response exceeds its byte bound",
            ));
        }

        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Error::new("unavailable", "cannot read route readiness response"))?
        {
            if body.len().saturating_add(chunk.len()) > MAX_READINESS_BYTES {
                return Err(Error::new(
                    "unavailable",
                    "route readiness response exceeds its byte bound",
                ));
            }
            body.extend_from_slice(&chunk);
        }
        let status: Value = serde_json::from_slice(&body)
            .map_err(|_| Error::new("unavailable", "route readiness response is malformed"))?;
        if !readiness_healthy(&status) {
            return Err(Error::new("unavailable", "route has not reported ready"));
        }
        Ok(())
    })
    .await
    .map_err(|_| Error::new("unavailable", "route readiness deadline elapsed"))?
}

/// Adopted-backend readiness: the OpenCodex contract (`status == "ready"`)
/// plus the healthy shapes Strata and llama.cpp actually serve
/// (`status == "ok"`). A Strata health body also carries a separate
/// `loaded` flag distinguishing reachable service from loaded model;
/// readiness proves the service only, so `loaded: false` stays healthy
/// here and surfaces through backend metadata instead of this gate.
fn readiness_healthy(status: &Value) -> bool {
    matches!(
        status.get("status").and_then(Value::as_str),
        Some("ready") | Some("ok")
    )
}

fn readiness_url(binding: &RouteBinding) -> Result<Url> {
    binding
        .validate()
        .map_err(|_| Error::new("source_integrity", "frozen route binding is invalid"))?;
    let endpoint = Url::parse(&binding.endpoint)
        .map_err(|_| Error::new("source_integrity", "frozen route endpoint is invalid"))?;
    endpoint
        .join(&binding.readiness_path)
        .map_err(|_| Error::new("source_integrity", "frozen route readiness path is invalid"))
}
