//! Private loopback Streamable HTTP transport for the existing MCP facade.
//!
//! This local bootstrap route maps one configured bearer secret to one fixed
//! ELIOT credential and restricted MCP profile. It is not Cloudflare Access,
//! OAuth, or external-client authentication. The application API and its
//! credential checks remain authoritative; HTTP metadata never selects an
//! identity or profile.

use crate::{
    config::{Config, McpToolProfile},
    error::{Error, Result},
    mcp,
    model::Credential,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::{
    Request, Response, StatusCode,
    body::Incoming,
    header::{AUTHORIZATION, CONTENT_TYPE},
    server::conn::http1,
    service::service_fn,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use std::{convert::Infallible, future::poll_fn, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
    time::timeout,
};
use tokio_util::sync::CancellationToken;
use tower_service::Service;

const MCP_PATH: &str = "/mcp";
const MAX_GATEWAY_BODY_BYTES: usize = 1_048_576;
const MIN_BEARER_TOKEN_BYTES: usize = 32;
const MAX_BEARER_TOKEN_BYTES: usize = 512;
const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
const MAX_HTTP_HEADERS: usize = 64;
const MAX_CONNECTIONS: usize = 32;
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

type HttpBody = BoxBody<Bytes, Infallible>;
type HttpResponse = Response<HttpBody>;

/// Serve the one profile configured under `[gateway]` until interrupted.
///
/// The bearer secret is only a local transport gate. It is mapped once at
/// startup to `gateway.profile` and the supplied ELIOT credential. Requests
/// cannot choose either value. Ending this process closes this route without
/// changing the ELIOT host or cancelling work it has already admitted.
pub async fn run(config: Config, credential: Credential, bearer_token: String) -> Result<()> {
    if !config.gateway.enabled {
        return Err(Error::new(
            "CONFIG_ERROR",
            "the local Remote Agent Gateway is disabled",
        ));
    }

    let bind: SocketAddr = config.gateway.bind.parse().map_err(|_| {
        Error::new(
            "CONFIG_ERROR",
            "gateway bind must be a loopback socket address",
        )
    })?;
    if !bind.ip().is_loopback() {
        return Err(Error::new(
            "CONFIG_ERROR",
            "the local Remote Agent Gateway may bind only to a loopback address",
        ));
    }
    if config.gateway.max_body_bytes < 1024
        || config.gateway.max_body_bytes > MAX_GATEWAY_BODY_BYTES
        || config.gateway.max_body_bytes > config.ipc.max_frame_bytes
        || !(1..=300).contains(&config.gateway.request_timeout_seconds)
    {
        return Err(Error::new(
            "CONFIG_ERROR",
            "gateway body limit or request timeout is outside the supported bounds",
        ));
    }
    validate_bearer_token(bearer_token.as_bytes())?;

    let profile_name = config.gateway.profile.clone();
    let profile = config
        .mcp
        .selected_tool_profile(Some(&profile_name), &credential.client_id)?;
    if profile == McpToolProfile::Full {
        return Err(Error::new(
            "CONFIG_ERROR",
            "the local Remote Agent Gateway requires a restricted MCP profile",
        ));
    }

    // Validate the fixed credential/profile binding before opening the socket.
    drop(mcp::profiled_facade(
        &config,
        credential.clone(),
        Some(&profile_name),
    )?);

    let listener = TcpListener::bind(bind).await.map_err(|_| {
        Error::new(
            "GATEWAY_BIND_FAILED",
            "could not bind the loopback gateway socket",
        )
    })?;
    let request_timeout = Duration::from_secs(config.gateway.request_timeout_seconds);
    let bearer_token: Arc<[u8]> = Arc::from(bearer_token.into_bytes());
    let shutdown = CancellationToken::new();

    let factory_config = config.clone();
    let factory_credential = credential;
    let factory_profile = profile_name;
    let service_factory = move || {
        mcp::profiled_facade(
            &factory_config,
            factory_credential.clone(),
            Some(&factory_profile),
        )
        .map_err(|_| std::io::Error::other("configured MCP facade is unavailable"))
    };
    let rmcp_config = StreamableHttpServerConfig::default()
        .with_max_request_body_bytes(config.gateway.max_body_bytes)
        .with_cancellation_token(shutdown.clone())
        .with_allowed_origins([
            "http://localhost:*",
            "http://127.0.0.1:*",
            "http://[::1]:*",
            "https://localhost:*",
            "https://127.0.0.1:*",
            "https://[::1]:*",
        ])
        .enforce_origin_validation();
    // Leave RMCP's loopback Host allowlist and negotiated session/protocol
    // behavior in force. Its body limit is enforced while streaming, even
    // without a Content-Length header or when that header is false.
    let mcp_service = StreamableHttpService::new(
        service_factory,
        Arc::new(LocalSessionManager::default()),
        rmcp_config,
    );
    let permits = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connections = JoinSet::new();
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);

    loop {
        tokio::select! {
            _ = &mut signal => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = match accepted {
                    Ok(accepted) => accepted,
                    Err(_) => {
                        shutdown.cancel();
                        connections.abort_all();
                        while connections.join_next().await.is_some() {}
                        return Err(Error::new(
                            "GATEWAY_ACCEPT_FAILED",
                            "the loopback gateway socket failed",
                        ));
                    }
                };
                let Ok(permit) = permits.clone().try_acquire_owned() else {
                    drop(stream);
                    continue;
                };
                let service = mcp_service.clone();
                let token = bearer_token.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    serve_connection(stream, service, token, request_timeout).await;
                });
            }
        }
    }

    // This route-level switch only stops the HTTP facade. Accepted ELIOT work
    // remains owned by the host. A dropped client response can have an unknown
    // outcome and must be reconciled by its caller-owned request ID.
    shutdown.cancel();
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn serve_connection(
    stream: TcpStream,
    mcp_service: StreamableHttpService<mcp::ProfiledFacade, LocalSessionManager>,
    bearer_token: Arc<[u8]>,
    request_timeout: Duration,
) {
    let handler = service_fn(move |request: Request<Incoming>| {
        let service = mcp_service.clone();
        let token = bearer_token.clone();
        async move {
            let response = if request.uri().path() != MCP_PATH || request.uri().query().is_some() {
                response(StatusCode::NOT_FOUND, b"Not found", false)
            } else if !authorized(request.headers(), &token) {
                response(StatusCode::UNAUTHORIZED, b"Unauthorized", true)
            } else {
                let mut service = service;
                let dispatch = async {
                    poll_fn(|cx| Service::<Request<Incoming>>::poll_ready(&mut service, cx))
                        .await?;
                    Service::<Request<Incoming>>::call(&mut service, request).await
                };
                // This deadline covers request-body intake and MCP dispatch up
                // to creation of the HTTP response. RMCP owns subsequent SSE
                // body lifetime; active streams are bounded by MAX_CONNECTIONS
                // and end on client disconnect or route shutdown.
                match timeout(request_timeout, dispatch).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(never)) => match never {},
                    Err(_) => response(
                        StatusCode::GATEWAY_TIMEOUT,
                        b"No MCP response arrived before the request deadline. The operation outcome may be unknown; reconcile with ELIOT using the caller-owned request ID before retrying.",
                        false,
                    ),
                }
            };
            Ok::<_, Infallible>(response)
        }
    });

    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::default())
        .header_read_timeout(Some(HEADER_READ_TIMEOUT))
        .max_buf_size(MAX_HTTP_HEADER_BYTES)
        .max_headers(MAX_HTTP_HEADERS)
        .keep_alive(false);
    let _ = builder
        .serve_connection(TokioIo::new(stream), handler)
        .await;
}

fn validate_bearer_token(token: &[u8]) -> Result<()> {
    if !(MIN_BEARER_TOKEN_BYTES..=MAX_BEARER_TOKEN_BYTES).contains(&token.len())
        || token
            .iter()
            .any(|byte| !byte.is_ascii_graphic() || *byte == b',')
    {
        return Err(Error::new(
            "CONFIG_ERROR",
            "gateway bearer secret must be 32-512 visible ASCII bytes without commas",
        ));
    }
    Ok(())
}

fn authorized(headers: &hyper::HeaderMap, expected_token: &[u8]) -> bool {
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let Some(header) = values.next() else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let value = header.as_bytes();
    const PREFIX: &[u8] = b"Bearer ";
    if value.len() <= PREFIX.len() || !value[..PREFIX.len()].eq_ignore_ascii_case(PREFIX) {
        return false;
    }
    let supplied_token = &value[PREFIX.len()..];
    if supplied_token
        .iter()
        .any(|byte| !byte.is_ascii_graphic() || *byte == b',')
    {
        return false;
    }
    constant_time_eq(supplied_token, expected_token)
}

/// Compare a fixed maximum number of bytes so token contents and length do
/// not cause an early return. Header syntax and duplicate-header checks are
/// deliberately performed separately because those values are not secrets.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..MAX_BEARER_TOKEN_BYTES {
        let a = left.get(index).copied().unwrap_or_default();
        let b = right.get(index).copied().unwrap_or_default();
        difference |= usize::from(a ^ b);
    }
    difference == 0
}

fn response(status: StatusCode, body: &'static [u8], challenge: bool) -> HttpResponse {
    let mut response = Response::new(Full::new(Bytes::from_static(body)).boxed());
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        hyper::http::HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    if challenge {
        response.headers_mut().insert(
            hyper::http::header::WWW_AUTHENTICATE,
            hyper::http::HeaderValue::from_static("Bearer"),
        );
    }
    response
}
