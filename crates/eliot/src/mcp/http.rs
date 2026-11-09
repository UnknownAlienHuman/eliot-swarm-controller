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

#[allow(deprecated)] // SEP-2577: logging is deprecated but functional in rmcp 3.9.
use rmcp::model::LoggingLevel;

use axum::{
    Router,
    extract::State,
    http::{Request, Response},
    routing::post,
};
use eliot_plugin::protocol::McpHttpReady;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use tokio_util::sync::CancellationToken;

use super::EliotMcpHandler;
use super::origin_auth::{OriginAuth, SessionLifetime, VerifiedRequest};

/// MCP route served locally; the owner-managed tunnel maps the public URL
/// onto it. Single route by design: no arbitrary routes. Also the required
/// path of `--public-url`.
const MCP_HTTP_PATH: &str = "/mcp";
/// Port implied by an `https` public URL without an explicit port.
const HTTPS_DEFAULT_PORT: u16 = 443;

type HttpMcpService = StreamableHttpService<EliotMcpHandler, LocalSessionManager>;

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
    #[allow(deprecated)] // SEP-2577: LoggingLevel default is deprecated but functional in rmcp 3.9.
    let service = StreamableHttpService::new(
        move || {
            Ok(EliotMcpHandler {
                data_dir: data_dir.clone(),
                notify_owner_events: true,
                require_agent: false,
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
        Err(status) => return rejected(status),
    };
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
            if let Err(status) = state
                .auth
                .finish(&verified, &method, response.status(), response.headers())
                .await
            {
                return rejected(status);
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
