mod execution_log;
use super::{Options, valid_id};
use crate::error::{Error, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use reqwest::{Client, Method, Url, header};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    io::Read,
    net::{IpAddr, SocketAddr},
    path::{Component, Path},
    time::Duration,
};

const MAX_BODY: usize = 4 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

// This is an ELIOT connection record, not a guessed version of service.json.
// An operator may update it when the externally owned service changes address.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionRecord {
    schema_version: u32,
    endpoint: String,
    pid: u32,
    username: String,
    password: String,
}

#[derive(Clone)]
pub(crate) struct Service {
    client: Client,
    endpoint: Url,
    pub(crate) pid: u32,
    pub(crate) version: String,
}
#[derive(Deserialize)]
struct ServerInfo {
    version: String,
    pid: u32,
    urls: Vec<String>,
    paths: ServerPaths,
}
#[derive(Deserialize)]
struct ServerPaths {
    tmp: String,
}
#[derive(Deserialize)]
pub(super) struct Data<T> {
    pub data: T,
}

pub(super) struct RawBody {
    pub bytes: Vec<u8>,
    pub media_type: String,
}

pub(super) fn decode<T: DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).map_err(|_| {
        Error::new(
            "NATIVE_SCHEMA_ERROR",
            "native response does not match the selected V2 contract",
        )
    })
}
fn record(path: &Path) -> Result<ConnectionRecord> {
    if !std::fs::symlink_metadata(path).is_ok_and(|m| m.is_file() && m.len() <= 65536) {
        return Err(Error::new(
            "NATIVE_CONNECTION_FILE",
            "connection record must be a bounded regular file, not a link or pipe",
        ));
    }
    let file = std::fs::File::open(path).map_err(|_| {
        Error::new(
            "NATIVE_CONNECTION_FILE",
            "cannot read the configured connection record",
        )
    })?;
    if !file
        .metadata()
        .is_ok_and(|m| m.is_file() && m.len() <= 65536)
    {
        return Err(Error::new(
            "NATIVE_CONNECTION_FILE",
            "connection record must be a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(65537)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::new("NATIVE_CONNECTION_FILE", "connection record read failed"))?;
    if bytes.len() > 65536 {
        return Err(Error::new(
            "NATIVE_CONNECTION_FILE",
            "connection record exceeds limit",
        ));
    }
    let record: ConnectionRecord = serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("NATIVE_CONNECTION_FILE", "invalid connection record schema"))?;
    if record.schema_version != 1
        || record.pid == 0
        || record.username.is_empty()
        || record.username.contains(':')
        || record.password.is_empty()
    {
        return Err(Error::new(
            "NATIVE_CONNECTION_FILE",
            "explicit PID and Basic authentication are required",
        ));
    }
    Ok(record)
}

pub(super) fn endpoint(text: &str) -> Result<Url> {
    let url =
        Url::parse(text).map_err(|_| Error::new("NATIVE_ENDPOINT", "invalid service endpoint"))?;
    let local = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if url.scheme() != "http"
        || !local
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || url.port_or_known_default().is_none_or(|p| p == 0)
    {
        return Err(Error::new(
            "NATIVE_ENDPOINT",
            "only credential-free loopback HTTP origins are supported",
        ));
    }
    Ok(url)
}
impl Service {
    pub(crate) async fn connect(options: &Options) -> Result<Self> {
        let path = options.connection_file.clone();
        let record = tokio::task::spawn_blocking(move || record(&path))
            .await
            .map_err(|_| Error::new("NATIVE_CONNECTION_FILE", "connection reader stopped"))??;
        let endpoint = endpoint(&record.endpoint)?;
        let mut auth = header::HeaderValue::from_str(&format!(
            "Basic {}",
            STANDARD.encode(format!("{}:{}", record.username, record.password))
        ))
        .map_err(|_| Error::new("NATIVE_CONNECTION_FILE", "invalid authentication value"))?;
        auth.set_sensitive(true);
        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, auth);
        headers.insert(
            header::ACCEPT,
            header::HeaderValue::from_static("application/json"),
        );
        let mut builder = Client::builder()
            .default_headers(headers)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(3))
            .http1_only();
        // Resolve localhost explicitly; no DNS lookup can redirect a credential.
        if endpoint.host_str() == Some("localhost") {
            builder = builder.resolve(
                "localhost",
                SocketAddr::from((
                    [127, 0, 0, 1],
                    endpoint.port_or_known_default().unwrap_or(80),
                )),
            );
        }
        let service = Self {
            client: builder
                .build()
                .map_err(|_| Error::new("NATIVE_TRANSPORT", "HTTP client initialization failed"))?,
            endpoint,
            pid: record.pid,
            version: options.expected_version.clone(),
        };
        service.verify().await?;
        Ok(service)
    }
    pub(crate) async fn verify(&self) -> Result<()> {
        let info: ServerInfo = decode(self.get("/api/info", &[]).await?)?;
        if info.pid != self.pid
            || info.version != self.version
            || info.urls.is_empty()
            || info.paths.tmp.is_empty()
        {
            return Err(Error::new(
                "NATIVE_INSTANCE_CHANGED",
                "native service identity/version differs from the explicit connection record",
            ));
        }
        Ok(())
    }
    pub(super) async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        self.request(Method::GET, path, query, None).await
    }
    pub(super) async fn post(&self, path: &str, body: Value) -> Result<Value> {
        self.request(Method::POST, path, &[], Some(body)).await
    }
    /// POST for native endpoints whose contract declares no payload at all
    /// (e.g. `session.background`): never invent a body shape.
    pub(super) async fn post_no_body(&self, path: &str) -> Result<Value> {
        self.request(Method::POST, path, &[], None).await
    }
    pub(super) async fn put(&self, path: &str, body: Value) -> Result<Value> {
        self.request(Method::PUT, path, &[], Some(body)).await
    }
    pub(super) async fn delete(&self, path: &str) -> Result<Value> {
        self.request(Method::DELETE, path, &[], None).await
    }
    /// Read one exact file through OpenCode's location-confined fs.read route.
    /// Callers must derive `relative` from a native result descriptor; this API
    /// deliberately does not accept an arbitrary URL or absolute path.
    pub(super) async fn get_location_file(
        &self,
        relative: &Path,
        directory: &Path,
        limit: usize,
    ) -> Result<RawBody> {
        if limit == 0 || relative.as_os_str().is_empty() {
            return Err(Error::invalid("invalid native file read boundary"));
        }
        let directory = directory.to_str().ok_or_else(|| {
            Error::new(
                "NATIVE_LOCATION_MISMATCH",
                "native directory is not valid Unicode",
            )
        })?;
        let mut url = self.endpoint.clone();
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                Error::new(
                    "NATIVE_ENDPOINT",
                    "native endpoint cannot contain path segments",
                )
            })?;
            segments.clear();
            segments.push("api").push("fs").push("read");
            for component in relative.components() {
                let Component::Normal(segment) = component else {
                    return Err(Error::new(
                        "RESULT_TOOL_FILE_OUTSIDE_SCOPE",
                        "native result file path is not a relative location path",
                    ));
                };
                let segment = segment.to_str().ok_or_else(|| {
                    Error::new(
                        "NATIVE_TOOL_FILE_SCHEMA",
                        "native result path is not valid Unicode",
                    )
                })?;
                segments.push(segment);
            }
        }
        url.query_pairs_mut()
            .append_pair("location[directory]", directory);
        if url.origin() != self.endpoint.origin() {
            return Err(Error::new(
                "NATIVE_ENDPOINT",
                "cross-origin native request refused",
            ));
        }
        let mut response = self
            .client
            .get(url)
            .header(header::ACCEPT, header::HeaderValue::from_static("*/*"))
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|_| transport_error(false))?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::new(
                "NATIVE_READ_FAILED",
                format!("HTTP {}", status.as_u16()),
            ));
        }
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(Error::new(
                "RESULT_TOOL_FILE_LIMIT",
                "native result file exceeds the configured boundary",
            ));
        }
        let media_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.len() <= 256)
            .ok_or_else(|| {
                Error::new(
                    "NATIVE_TOOL_FILE_SCHEMA",
                    "native file response has no bounded content type",
                )
            })?
            .to_owned();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| transport_error(false))? {
            if bytes.len().saturating_add(chunk.len()) > limit {
                return Err(Error::new(
                    "RESULT_TOOL_FILE_LIMIT",
                    "native result file exceeds the configured boundary",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(RawBody { bytes, media_type })
    }
    async fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        body: Option<Value>,
    ) -> Result<Value> {
        let effect = method != Method::GET;
        let mut url = self
            .endpoint
            .join(path)
            .map_err(|_| Error::new("NATIVE_ENDPOINT", "invalid native route"))?;
        if url.origin() != self.endpoint.origin() {
            return Err(Error::new(
                "NATIVE_ENDPOINT",
                "cross-origin native request refused",
            ));
        }
        if !query.is_empty() {
            url.query_pairs_mut()
                .extend_pairs(query.iter().map(|(k, v)| (*k, v.as_str())));
        }
        let mut request = self.client.request(method, url).timeout(REQUEST_TIMEOUT);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let mut response = request.send().await.map_err(|_| transport_error(effect))?;
        let status = response.status();
        if !status.is_success() {
            // Never save a reflected error body or follow a redirect with credentials.
            let code = if effect
                && matches!(
                    status.as_u16(),
                    400 | 401 | 403 | 404 | 405 | 409 | 413 | 422
                ) {
                "NATIVE_REJECTED"
            } else if effect {
                "NATIVE_OUTCOME_UNKNOWN"
            } else {
                "NATIVE_READ_FAILED"
            };
            return Err(Error::new(code, format!("HTTP {}", status.as_u16())));
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_BODY as u64)
        {
            return Err(Error::new(
                "NATIVE_RESPONSE_LIMIT",
                "native response exceeds the configured boundary",
            ));
        }
        if status == reqwest::StatusCode::NO_CONTENT {
            return Ok(Value::Null);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| transport_error(effect))?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_BODY {
                return Err(Error::new(
                    "NATIVE_RESPONSE_LIMIT",
                    "native response exceeds the configured boundary",
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes)
            .map_err(|_| Error::new("NATIVE_SCHEMA_ERROR", "invalid native JSON response"))
    }
    pub(crate) async fn session(&self, id: &str) -> Result<Value> {
        valid_id(id, "ses")?;
        let response: Data<Value> = decode(self.get(&format!("/api/session/{id}"), &[]).await?)?;
        super::snapshot::validate_session(&response.data, Some(id))?;
        Ok(response.data)
    }
}
fn transport_error(effect: bool) -> Error {
    Error::new(
        if effect {
            "NATIVE_OUTCOME_UNKNOWN"
        } else {
            "NATIVE_UNAVAILABLE"
        },
        "native transport failed; no automatic mutation replay",
    )
}

/// One invalidation stream per service client, not per native binding. The donor
/// parser owns SSE framing. Events are never retained as conversation history.
#[derive(Clone, serde::Serialize)]
pub(crate) struct EventState {
    pub connected: bool,
    pub revision: u64,
    pub gaps: u64,
    pub last_gap: Option<&'static str>,
}
pub(crate) struct EventReader {
    pub state: tokio::sync::watch::Receiver<EventState>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for EventReader {
    fn drop(&mut self) {
        // Cancels only our GET stream; it cannot cancel a native session or POST.
        self.task.abort();
    }
}
impl Service {
    pub(crate) fn events(&self, mut stop: tokio::sync::watch::Receiver<bool>) -> EventReader {
        let service = self.clone();
        let (tx, state) = tokio::sync::watch::channel(EventState {
            connected: false,
            revision: 0,
            gaps: 0,
            last_gap: None,
        });
        let task = tokio::spawn(async move {
            loop {
                if *stop.borrow() {
                    break;
                }
                let read = service.event_connection(&tx);
                let reason = tokio::select! {
                    _ = stop.changed() => break,
                    reason = read => reason,
                };
                tx.send_modify(|s| {
                    s.connected = false;
                    s.revision = s.revision.saturating_add(1);
                    s.gaps = s.gaps.saturating_add(1);
                    s.last_gap = Some(reason);
                });
                // Reconnect only the read transport. Never send Last-Event-ID:
                // the selected native stream does not promise durable replay.
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {},
                }
            }
        });
        EventReader { state, task }
    }
    async fn event_connection(&self, tx: &tokio::sync::watch::Sender<EventState>) -> &'static str {
        use eventsource_stream::Eventsource;
        use futures_util::StreamExt;
        if self.verify().await.is_err() {
            return "SSE_IDENTITY_UNAVAILABLE";
        }
        let Ok(url) = self.endpoint.join("/api/event") else {
            return "SSE_ENDPOINT";
        };
        let request = self
            .client
            .get(url)
            .header(header::ACCEPT, "text/event-stream");
        let response = match tokio::time::timeout(REQUEST_TIMEOUT, request.send()).await {
            Ok(Ok(response))
                if response.status().is_success()
                    && response
                        .headers()
                        .get(header::CONTENT_TYPE)
                        .and_then(|h| h.to_str().ok())
                        .and_then(|s| s.split(';').next())
                        == Some("text/event-stream") =>
            {
                response
            }
            _ => return "SSE_UNAVAILABLE",
        };
        tx.send_modify(|s| {
            s.connected = true;
            s.revision = s.revision.saturating_add(1);
        });
        // Bound even an unterminated adversarial frame before it reaches the
        // parser. Recycling a 16 MiB read stream is an explicit gap/readback,
        // not a claimed lossless replay. No custom SSE codec is maintained here.
        let mut bytes = 0usize;
        let stream = response
            .bytes_stream()
            .map(move |chunk| {
                let chunk = chunk.map_err(|_| std::io::Error::other("SSE transport"))?;
                bytes = bytes.saturating_add(chunk.len());
                if bytes > 16 * 1024 * 1024 {
                    return Err(std::io::Error::other("SSE bound"));
                }
                Ok(chunk)
            })
            .eventsource();
        futures_util::pin_mut!(stream);
        while let Some(event) = stream.next().await {
            match event {
                Ok(event) if event.event == "effect/httpapi/stream/failure" => {
                    return "SSE_NATIVE_FAILURE";
                }
                Ok(event) if serde_json::from_str::<Value>(&event.data).is_ok() => {
                    tx.send_modify(|s| s.revision = s.revision.saturating_add(1));
                }
                Ok(_) => return "SSE_SCHEMA_GAP",
                Err(_) => return "SSE_PARSE_OR_TRANSPORT_GAP",
            }
        }
        "SSE_EOF"
    }
}
