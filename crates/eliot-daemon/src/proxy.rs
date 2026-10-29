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
        if status.get("status").and_then(Value::as_str) != Some("ready") {
            return Err(Error::new("unavailable", "route has not reported ready"));
        }
        Ok(())
    })
    .await
    .map_err(|_| Error::new("unavailable", "route readiness deadline elapsed"))?
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::timeout,
    };

    fn binding(endpoint: String) -> RouteBinding {
        RouteBinding {
            name: "opencodex".to_owned(),
            endpoint: endpoint.clone(),
            readiness_path: "/readyz".to_owned(),
            provider_base_url: format!("{endpoint}/v1"),
            client_key_env_name: Some("LOCAL_CLIENT".to_owned()),
        }
    }

    async fn read_request(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let read = stream.read(&mut buffer).await.unwrap();
            if read == 0 || request.len().saturating_add(read) > 8 * 1024 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
        }
        String::from_utf8_lossy(&request).to_ascii_lowercase()
    }

    async fn send_response(stream: &mut TcpStream, status: &str, headers: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
    }

    async fn probe_response(status: &'static str, body: &'static str) -> (Result<()>, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            send_response(&mut stream, status, "", body).await;
            request
        });
        let result = check_ready(&binding(endpoint), Instant::now() + Duration::from_secs(3)).await;
        (result, server.await.unwrap())
    }

    #[tokio::test]
    async fn readiness_requires_http_success_and_status_ready_without_credentials() {
        let (ready, request) = probe_response("200 OK", r#"{"status":"ready"}"#).await;
        ready.unwrap();
        assert!(request.starts_with("get /readyz http/1.1\r\n"));
        assert!(!request.contains("authorization:"));
        assert!(!request.contains("local_client"));

        let (http_error, _) = probe_response("401 Unauthorized", r#"{"status":"ready"}"#).await;
        assert_eq!(http_error.unwrap_err().code, "unavailable");

        let (unready, _) = probe_response("200 OK", r#"{"status":"starting"}"#).await;
        assert_eq!(unready.unwrap_err().code, "unavailable");

        let (malformed, _) = probe_response("200 OK", "not-json").await;
        assert_eq!(malformed.unwrap_err().code, "unavailable");
    }

    #[tokio::test]
    async fn readiness_rejects_redirects_and_oversized_bodies() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            send_response(
                &mut stream,
                "302 Found",
                "Location: /redirected\r\n",
                r#"{"status":"ready"}"#,
            )
            .await;
            let followed = timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_ok();
            (request, followed)
        });
        assert!(
            check_ready(&binding(endpoint), Instant::now() + Duration::from_secs(3))
                .await
                .is_err()
        );
        let (request, followed) = server.await.unwrap();
        assert!(request.starts_with("get /readyz http/1.1\r\n"));
        assert!(!followed);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let oversized = "x".repeat(MAX_READINESS_BYTES + 1);
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_request(&mut stream).await;
            send_response(&mut stream, "200 OK", "", &oversized).await;
        });
        let error = check_ready(&binding(endpoint), Instant::now() + Duration::from_secs(3))
            .await
            .unwrap_err();
        assert_eq!(error.code, "unavailable");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn direct_profile_has_no_readiness_request() {
        check_profile_ready(&serde_json::json!({"harness":"codex"}))
            .await
            .unwrap();
    }
}
