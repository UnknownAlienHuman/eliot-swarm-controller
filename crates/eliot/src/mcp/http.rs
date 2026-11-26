//! Loopback-only StreamableHTTP frontend reusing the existing MCP handler.
//!
//! Same single `swarm` tool and existing authenticated daemon path as stdio.
//! The listener binds 127.0.0.1 only, with signed Access-assertion validation
//! before every SDK request, including initialization. Cloudflared's JWT
//! check protects its route separately; loopback is not an auth boundary.
//! This module builds no OAuth or principal framework, and never treats
//! `clientInfo` (an untrusted client label) as authentication. Request
//! bodies keep the daemon 4 MiB frame bound.
//!
//! `--public-url` (required for managed children) only widens the SDK
//! `Host`/`Origin` checks to the tunnel's public host/origin; without it
//! the server stays loopback-only. Each initialized HTTP session also
//! spawns one task delivering committed owner events (`x.user.notify`,
//! `x.user.agent.attention`) as `notifications/message`; normalized
//! payloads only, no raw args/env/credentials. After the bind, exactly one
//! [`McpHttpReady`] line is printed to stdout for the managed parent. That
//! frame proves a local listener only, never public tunnel/auth readiness.
//! Diagnostics go to stderr.

use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, RwLock, atomic::AtomicBool},
};

#[allow(
    deprecated,
    reason = "The required rmcp 3.9 logging surface remains functional under SEP-2577."
)] // SEP-2577: logging is deprecated but functional in rmcp 3.9.
use rmcp::model::LoggingLevel;

use axum::{
    Router,
    extract::State,
    http::{Request, Response},
    routing::post,
};
use eliot_plugin::protocol::McpHttpReady;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::{local::LocalSessionManager, never::NeverSessionManager},
};
use tokio_util::sync::CancellationToken;

use super::origin_auth::{OriginAuth, SessionLifetime, VerifiedRequest};
use super::{AgentHttpIdentity, EliotMcpHandler};

/// MCP route served locally; the owner-managed tunnel maps the public URL
/// onto it. Single route by design: no arbitrary routes. Also the required
/// path of `--public-url`.
const MCP_HTTP_PATH: &str = "/mcp";
/// Port implied by an `https` public URL without an explicit port.
const HTTPS_DEFAULT_PORT: u16 = 443;

type HttpMcpService = StreamableHttpService<EliotMcpHandler, LocalSessionManager>;
type AgentHttpService = StreamableHttpService<EliotMcpHandler, NeverSessionManager>;

#[derive(Clone)]
struct HttpState {
    service: HttpMcpService,
    auth: Arc<OriginAuth>,
}

/// Serve the shared MCP handler over StreamableHTTP on 127.0.0.1:`port`
/// (`0` selects an ephemeral port; the actual address is printed in the
/// ready frame). `public_url` widens the SDK `Host`/`Origin` checks to the
/// tunnel's public host/origin; `None` keeps a manual loopback-only server.
/// Managed deployments always pass the tunnel URL. With `managed_child`,
/// stdin EOF also triggers a clean shutdown so the owning parent can stop
/// the server by closing the pipe; otherwise Ctrl-C is the only stop signal.
pub async fn run_http(
    data_dir: PathBuf,
    port: u16,
    managed_child: bool,
    public_url: Option<String>,
) -> Result<(), String> {
    let sessions = Arc::new(LocalSessionManager::default());
    let auth = Arc::new(OriginAuth::load(&data_dir, sessions.clone()).await?);
    let public = public_url.as_deref().map(parse_public_url).transpose()?;
    let listener =
        tokio::net::TcpListener::bind(SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port)))
            .await
            .map_err(|error| format!("mcp http bind failed: {error}"))?;
    let bound = listener
        .local_addr()
        .map_err(|error| format!("mcp http bound address is unknown: {error}"))?;
    let bound_port = bound.port();
    let mut allowed_hosts = vec![
        format!("127.0.0.1:{bound_port}"),
        format!("localhost:{bound_port}"),
    ];
    let mut allowed_origins = vec![
        format!("http://127.0.0.1:{bound_port}"),
        format!("http://localhost:{bound_port}"),
    ];
    if let Some(public) = &public {
        allowed_hosts.push(public.host.clone());
        allowed_origins.push(public.origin.clone());
    }
    let cancel = CancellationToken::new();
    // Native rmcp session negotiation (SDK default, no override): stateful
    // sessions for pre-2026 clients, stateless for 2026-07-28+ per SEP-2567.
    let config = StreamableHttpServerConfig::default()
        .with_max_request_body_bytes(eliot_daemon::MAX_RPC_FRAME_BYTES)
        .with_allowed_hosts(allowed_hosts)
        .with_allowed_origins(allowed_origins)
        .with_cancellation_token(cancel.clone());
    let factory_shutdown = cancel.clone();
    #[allow(
        deprecated,
        reason = "The required rmcp 3.9 logging surface remains functional under SEP-2577."
    )] // SEP-2577: LoggingLevel default is deprecated but functional in rmcp 3.9.
    let service = StreamableHttpService::new(
        move || {
            Ok(EliotMcpHandler {
                data_dir: data_dir.clone(),
                notify_owner_events: true,
                require_agent: false,
                agent_http: false,
                shutdown: factory_shutdown.clone(),
                notify_spawned: Arc::new(AtomicBool::new(false)),
                session_lifetime: Arc::new(SessionLifetime::default()),
                log_level: Arc::new(RwLock::new(LoggingLevel::Info)),
            })
        },
        sessions,
        config,
    );
    // Direct route (never nested: nesting can drop the `Host` header the
    // SDK validates). POST carries JSON-RPC, GET carries the SSE stream,
    // DELETE closes a session.
    let app = Router::new()
        .route(
            MCP_HTTP_PATH,
            post(serve_mcp).get(serve_mcp).delete(serve_mcp),
        )
        .with_state(HttpState { service, auth });
    let frame = McpHttpReady {
        address: bound.to_string(),
    }
    .encode_frame()
    .map_err(|error| error.to_string())?;
    print!("{frame}");
    use std::io::Write as _;
    std::io::stdout()
        .flush()
        .map_err(|error| format!("mcp http ready frame was not delivered: {error}"))?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(managed_child, cancel))
        .await
        .map_err(|error| format!("mcp http serve failed: {error}"))?;
    Ok(())
}

/// Shared loopback-only Agent MCP bridge owned by the foreground daemon.
/// Requests carry exact Agent credentials and are checked by the daemon API
/// on every HTTP POST. The listener itself holds no credential registry.
pub struct AgentMcpBridge {
    listener: tokio::net::TcpListener,
    data_dir: PathBuf,
    address: String,
}

impl AgentMcpBridge {
    pub async fn bind(data_dir: PathBuf) -> Result<Self, String> {
        let listener =
            tokio::net::TcpListener::bind(SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)))
                .await
                .map_err(|error| format!("Agent MCP bridge bind failed: {error}"))?;
        let address = listener
            .local_addr()
            .map_err(|error| format!("Agent MCP bridge address unavailable: {error}"))?
            .to_string();
        Ok(Self {
            listener,
            data_dir,
            address,
        })
    }

    pub fn url(&self) -> String {
        format!("http://{}/mcp", self.address)
    }

    pub async fn serve(self) -> Result<(), String> {
        let sessions = Arc::new(NeverSessionManager::default());
        let cancel = CancellationToken::new();
        let factory_shutdown = cancel.clone();
        let factory_data = self.data_dir.clone();
        #[allow(
            deprecated,
            reason = "The shared handler retains the rmcp 3.9 logging field for Owner sessions."
        )]
        let service = StreamableHttpService::new(
            move || {
                Ok(EliotMcpHandler {
                    data_dir: factory_data.clone(),
                    notify_owner_events: false,
                    require_agent: true,
                    agent_http: true,
                    shutdown: factory_shutdown.clone(),
                    notify_spawned: Arc::new(AtomicBool::new(false)),
                    session_lifetime: Arc::new(SessionLifetime::default()),
                    log_level: Arc::new(RwLock::new(LoggingLevel::Info)),
                })
            },
            sessions,
            StreamableHttpServerConfig::default()
                .with_max_request_body_bytes(eliot_daemon::MAX_RPC_FRAME_BYTES)
                .with_legacy_session_mode(false)
                .with_stateless_protocol_metadata_required(false)
                .with_allowed_hosts(vec![self.address.clone()])
                .with_allowed_origins(vec![format!("http://{}", self.address)])
                .with_cancellation_token(cancel),
        );
        let state = AgentHttpState {
            service,
            data_dir: self.data_dir,
        };
        let app = Router::new()
            .route(MCP_HTTP_PATH, post(serve_agent_mcp))
            .with_state(state);
        axum::serve(self.listener, app)
            .await
            .map_err(|error| format!("Agent MCP bridge stopped: {error}"))
    }
}

#[derive(Clone)]
struct AgentHttpState {
    service: AgentHttpService,
    data_dir: PathBuf,
}

async fn serve_agent_mcp(
    State(state): State<AgentHttpState>,
    mut request: Request<axum::body::Body>,
) -> Response<axum::body::Body> {
    use axum::http::header::HeaderName;
    let Some(identity) = agent_http_identity(request.headers()) else {
        return rejected(axum::http::StatusCode::UNAUTHORIZED);
    };
    if eliot_daemon::verify_explicit_agent_identity(
        &state.data_dir,
        &identity.agent_id,
        identity.generation,
        &identity.token,
    )
    .await
    .is_err()
    {
        return rejected(axum::http::StatusCode::UNAUTHORIZED);
    }
    request
        .headers_mut()
        .remove(HeaderName::from_static("x-eliot-agent-id"));
    request
        .headers_mut()
        .remove(HeaderName::from_static("x-eliot-agent-generation"));
    request
        .headers_mut()
        .remove(axum::http::header::AUTHORIZATION);
    request.extensions_mut().insert(identity);
    use tower_service::Service as _;
    let mut service = state.service;
    if std::future::poll_fn(|cx| {
        <AgentHttpService as tower_service::Service<Request<axum::body::Body>>>::poll_ready(
            &mut service,
            cx,
        )
    })
    .await
    .is_err()
    {
        return internal_error();
    }
    match service.call(request).await {
        Ok(response) => response.map(axum::body::Body::new),
        Err(error) => match error {},
    }
}

fn agent_http_identity(headers: &axum::http::HeaderMap) -> Option<AgentHttpIdentity> {
    let read = |name: &str| headers.get(name)?.to_str().ok();
    let agent_id = read("x-eliot-agent-id").filter(|value| !value.trim().is_empty())?;
    let generation = read("x-eliot-agent-generation")?
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)?;
    let token = read(axum::http::header::AUTHORIZATION.as_str())?
        .strip_prefix("Bearer ")
        .filter(|value| !value.is_empty() && !value.chars().any(char::is_whitespace))?;
    Some(AgentHttpIdentity {
        agent_id: agent_id.to_owned(),
        generation,
        token: token.to_owned(),
    })
}

/// Public host/origin derived from `--public-url`, in SDK allowlist form.
struct PublicEndpoint {
    /// Bare public host for the SDK `Host` check (any port).
    host: String,
    /// Full origin with an explicit port for the SDK `Origin` check.
    origin: String,
}

/// Parse `--public-url` with the SDK stack's own URI type. Only `https`
/// URLs with the [`MCP_HTTP_PATH`] path are accepted; userinfo and
/// fragments are rejected. The origin always carries an explicit port so
/// matching never relies on the SDK's deprecated portless-origin behavior.
fn parse_public_url(raw: &str) -> Result<PublicEndpoint, String> {
    if raw.contains('#') {
        return Err("mcp http public URL must not contain a fragment".to_owned());
    }
    let uri: axum::http::Uri = raw
        .parse()
        .map_err(|_| "mcp http public URL is not a valid URI".to_owned())?;
    if uri.scheme_str() != Some("https") {
        return Err("mcp http public URL must use https".to_owned());
    }
    let authority = uri
        .authority()
        .ok_or_else(|| "mcp http public URL must include a host".to_owned())?;
    if authority.as_str().contains('@') {
        return Err("mcp http public URL must not include userinfo".to_owned());
    }
    if uri.path() != MCP_HTTP_PATH {
        return Err("mcp http public URL path must be /mcp".to_owned());
    }
    let host = authority.host();
    if host.is_empty() {
        return Err("mcp http public URL must include a host".to_owned());
    }
    let port = authority.port_u16().unwrap_or(HTTPS_DEFAULT_PORT);
    Ok(PublicEndpoint {
        host: host.to_owned(),
        origin: format!("https://{host}:{port}"),
    })
}

async fn serve_mcp(
    State(state): State<HttpState>,
    mut request: Request<axum::body::Body>,
) -> Response<axum::body::Body> {
    let method = request.method().clone();
    let verified: VerifiedRequest = match state
        .auth
        .authenticate(request.headers_mut(), &method)
        .await
    {
        Ok(verified) => verified,
        Err(rejection) => {
            state
                .auth
                .log_rejection(rejection, cf_ray(request.headers()), None);
            return rejected(rejection.status());
        }
    };
    let ray = cf_ray(request.headers()).map(str::to_owned);
    request.extensions_mut().insert(verified.clone());
    use tower_service::Service as _;
    let mut service = state.service;
    let ready = std::future::poll_fn(|context| {
        <HttpMcpService as tower_service::Service<Request<axum::body::Body>>>::poll_ready(
            &mut service,
            context,
        )
    })
    .await;
    if ready.is_err() {
        return internal_error();
    }
    match service.call(request).await {
        Ok(response) => {
            if let Err(rejection) = state
                .auth
                .finish(&verified, &method, response.status(), response.headers())
                .await
            {
                state.auth.log_rejection(
                    rejection,
                    ray.as_deref(),
                    Some(&verified.context.call_id),
                );
                return rejected(rejection.status());
            }
            // A persistent GET stream must not outlive the credential that
            // authorized that HTTP request, even if its MCP session survives.
            use futures_util::StreamExt as _;
            let deadline = tokio::time::Instant::from_std(verified.request_deadline);
            response.map(|body| {
                axum::body::Body::from_stream(
                    axum::body::Body::new(body)
                        .into_data_stream()
                        .take_until(tokio::time::sleep_until(deadline)),
                )
            })
        }
        Err(error) => match error {},
    }
}

fn cf_ray(headers: &axum::http::HeaderMap) -> Option<&str> {
    let mut rays = headers.get_all("cf-ray").iter();
    let ray = rays.next()?.to_str().ok()?;
    if rays.next().is_some() {
        return None;
    }
    let (id, location) = ray
        .split_once('-')
        .map_or((ray, None), |(id, location)| (id, Some(location)));
    (id.len() == 16
        && id.bytes().all(|byte| byte.is_ascii_hexdigit())
        && location.is_none_or(|location| {
            location.len() == 3 && location.bytes().all(|byte| byte.is_ascii_uppercase())
        }))
    .then_some(ray)
}

fn rejected(status: axum::http::StatusCode) -> Response<axum::body::Body> {
    Response::builder()
        .status(status)
        .body(axum::body::Body::empty())
        .unwrap_or_default()
}

async fn shutdown_signal(managed_child: bool, cancel: CancellationToken) {
    if managed_child {
        use tokio::io::AsyncReadExt as _;
        let stdin_eof = async {
            let _ = tokio::io::stdin().read_to_end(&mut Vec::new()).await;
        };
        tokio::select! {
            () = stdin_eof => {},
            _ = tokio::signal::ctrl_c() => {},
        }
    } else if tokio::signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
    cancel.cancel();
}

fn internal_error() -> Response<axum::body::Body> {
    Response::builder()
        .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        .body(axum::body::Body::empty())
        .unwrap_or_default()
}
