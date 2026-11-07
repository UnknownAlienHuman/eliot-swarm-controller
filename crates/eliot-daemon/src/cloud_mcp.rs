//! Owner-managed Cloudflare Tunnel for the loopback StreamableHTTP MCP server.
//!
//! The module owns exactly two children: the configured `swarm mcp --http`
//! command and one `cloudflared tunnel run`. Cloudflare Access performs the
//! external OAuth/JWT check; this module does not implement an auth framework.

use crate::platform::{CustodyGuarantee, DataDir, OwnedChild};
use eliot_core::{
    Error, Result,
    api::{CloudMcpCustody, CloudMcpHealth, CloudMcpStatus},
};
use eliot_plugin::{
    protocol::{MAX_MCP_HTTP_READY_BYTES, McpHttpReady},
    sync::LockExt,
};
#[cfg(unix)]
use std::fs::OpenOptions;
use std::{
    fs::{self, File},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Mutex, OnceLock},
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    process::{ChildStdin, ChildStdout, Command},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
const REAP_TIMEOUT: Duration = Duration::from_secs(10);
const MCP_HTTP_PATH: &str = "/mcp";
#[cfg(windows)]
const WINDOWS_JOB_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// The narrow process and Cloudflare settings used by this runtime.
///
/// The module constructs the managed HTTP MCP argv from the supplied data
/// directory and hostname. No environment map is accepted: each child gets a
/// cleared, minimal OS env.
pub struct CloudMcpConfig {
    pub http_mcp_executable: PathBuf,
    pub cloudflared_executable: PathBuf,
    pub tunnel_id: String,
    pub credentials_file: PathBuf,
    pub hostname: String,
    pub access_team_name: String,
    pub access_audience_tag: String,
}

/// Core's typed daemon-status view is also this module's current snapshot.
pub type CloudMcpSnapshot = CloudMcpStatus;

static CURRENT: OnceLock<Mutex<Option<CloudMcpSnapshot>>> = OnceLock::new();

/// Return the latest in-memory cloud service status, if it has been started.
pub fn service_snapshot() -> Option<CloudMcpSnapshot> {
    current_slot().lock_recover().clone()
}

/// Supervise the HTTP MCP child and its dedicated `cloudflared` tunnel.
///
/// The ready line is bounded and decoded using the shared plugin protocol.
/// Shutdown closes the HTTP child's stdin first, allowing its managed-child
/// EOF handler to exit cleanly. Every spawned child remains in `OwnedChild`
/// custody until its leader is reaped; Windows additionally waits for its Job
/// to empty. Unix custody is deliberately reported as degraded.
pub async fn run(
    config: CloudMcpConfig,
    data_dir: &Path,
    shutdown: CancellationToken,
) -> Result<()> {
    let data_dir = DataDir::open_existing(data_dir)?;
    let data_root = data_dir.path().to_path_buf();
    let public_url = validate_config(&config)?;
    let runtime_url = public_url.clone();
    let mut status = StatusGuard::begin(CloudMcpSnapshot {
        public_url,
        health: CloudMcpHealth::StartingHttp,
        custody: platform_custody(),
    })?;

    let result = supervise(config, data_root, runtime_url, shutdown, &status).await;
    status.finish(if result.is_ok() {
        CloudMcpHealth::Stopped
    } else {
        CloudMcpHealth::Failed
    });
    result
}

async fn supervise(
    config: CloudMcpConfig,
    data_root: PathBuf,
    public_url: String,
    shutdown: CancellationToken,
    status: &StatusGuard,
) -> Result<()> {
    if shutdown.is_cancelled() {
        status.set(CloudMcpHealth::Stopping);
        return Ok(());
    }

    let mut http = spawn_http(&config, &data_root, &public_url)?;
    status.set_custody(map_custody(http.guarantee()));
    let Some(http_stdin) = http.take_stdin() else {
        settle_child(&mut http, None, false, false).await?;
        return Err(Error::new(
            "cloud_mcp_spawn",
            "managed MCP stdin pipe is unavailable",
        ));
    };
    let Some(mut http_stdout) = http.take_stdout() else {
        settle_child(&mut http, Some(http_stdin), false, false).await?;
        return Err(Error::new(
            "cloud_mcp_spawn",
            "managed MCP stdout pipe is unavailable",
        ));
    };

    enum Startup {
        Ready(Result<McpHttpReady>),
        Shutdown,
        HttpExited(Result<std::process::ExitStatus>),
    }

    let startup = tokio::select! {
        _ = shutdown.cancelled() => Startup::Shutdown,
        result = timeout(READY_TIMEOUT, read_ready(&mut http_stdout)) => {
            Startup::Ready(match result {
                Ok(result) => result,
                Err(_) => Err(Error::new("cloud_mcp_ready_timeout", "managed MCP readiness timed out")),
            })
        }
        result = http.wait() => Startup::HttpExited(result),
    };
    drop(http_stdout);

    let ready = match startup {
        Startup::Ready(Ok(ready)) => ready,
        Startup::Shutdown => {
            status.set(CloudMcpHealth::Stopping);
            settle_child(&mut http, Some(http_stdin), false, true).await?;
            return Ok(());
        }
        Startup::HttpExited(result) => {
            status.set(CloudMcpHealth::Stopping);
            settle_child(&mut http, Some(http_stdin), result.is_ok(), false).await?;
            return Err(Error::new(
                "cloud_mcp_http_exit",
                "managed MCP HTTP child exited before readiness",
            ));
        }
        Startup::Ready(Err(error)) => {
            status.set(CloudMcpHealth::Stopping);
            settle_child(&mut http, Some(http_stdin), false, false).await?;
            return Err(error);
        }
    };

    status.set(CloudMcpHealth::StartingTunnel);
    let tunnel_config = match write_tunnel_config(&config, &data_root, &ready) {
        Ok(config) => config,
        Err(error) => {
            status.set(CloudMcpHealth::Stopping);
            settle_child(&mut http, Some(http_stdin), false, false).await?;
            return Err(error);
        }
    };
    let mut tunnel = match spawn_tunnel(&config, &data_root, &tunnel_config.path) {
        Ok(tunnel) => tunnel,
        Err(error) => {
            status.set(CloudMcpHealth::Stopping);
            settle_child(&mut http, Some(http_stdin), false, false).await?;
            return Err(error);
        }
    };
    status.set_custody(combine_custody(http.guarantee(), tunnel.guarantee()));
    status.set(CloudMcpHealth::Running);

    enum Supervision {
        Shutdown,
        HttpExited(Result<std::process::ExitStatus>),
        TunnelExited(Result<std::process::ExitStatus>),
    }

    let event = tokio::select! {
        _ = shutdown.cancelled() => Supervision::Shutdown,
        result = http.wait() => Supervision::HttpExited(result),
        result = tunnel.wait() => Supervision::TunnelExited(result),
    };
    status.set(CloudMcpHealth::Stopping);

    match event {
        Supervision::Shutdown => {
            let tunnel_result = settle_child(&mut tunnel, None, false, false).await;
            let http_result = settle_child(&mut http, Some(http_stdin), false, true).await;
            combine_cleanup(tunnel_result, http_result)
        }
        Supervision::HttpExited(result) => {
            let http_result =
                settle_child(&mut http, Some(http_stdin), result.is_ok(), false).await;
            let tunnel_result = settle_child(&mut tunnel, None, false, false).await;
            combine_cleanup(http_result, tunnel_result)?;
            Err(Error::new(
                "cloud_mcp_http_exit",
                "managed MCP HTTP child exited while the tunnel was running",
            ))
        }
        Supervision::TunnelExited(result) => {
            let tunnel_result = settle_child(&mut tunnel, None, result.is_ok(), false).await;
            let http_result = settle_child(&mut http, Some(http_stdin), false, true).await;
            combine_cleanup(tunnel_result, http_result)?;
            Err(Error::new(
                "cloud_mcp_tunnel_exit",
                "cloudflared exited while the HTTP MCP child was running",
            ))
        }
    }
}

fn spawn_http(config: &CloudMcpConfig, data_root: &Path, public_url: &str) -> Result<OwnedChild> {
    let mut command = Command::new(&config.http_mcp_executable);
    command
        .arg("--data-dir")
        .arg(data_root)
        .arg("mcp")
        .arg("--http")
        .arg("--port")
        .arg("0")
        .arg("--managed-child")
        .arg("--public-url")
        .arg(public_url)
        .current_dir(data_root)
        .env_clear()
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    add_minimal_os_environment(&mut command)?;
    OwnedChild::spawn(command)
}

fn spawn_tunnel(
    config: &CloudMcpConfig,
    data_root: &Path,
    config_file: &Path,
) -> Result<OwnedChild> {
    let mut command = Command::new(&config.cloudflared_executable);
    command
        .arg("tunnel")
        .arg("--config")
        .arg(config_file)
        .arg("run")
        .arg(&config.tunnel_id)
        .current_dir(data_root)
        .env_clear()
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    add_minimal_os_environment(&mut command)?;
    OwnedChild::spawn(command)
}

fn add_minimal_os_environment(command: &mut Command) -> Result<()> {
    #[cfg(windows)]
    {
        let system_root = std::env::var_os("SystemRoot")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| Error::new("cloud_mcp_environment", "SystemRoot is unavailable"))?;
        command.env("SystemRoot", system_root);
    }
    #[cfg(not(windows))]
    let _ = command;
    Ok(())
}

async fn read_ready(stdout: &mut ChildStdout) -> Result<McpHttpReady> {
    let mut frame = [0_u8; MAX_MCP_HTTP_READY_BYTES];
    let mut length = 0;
    loop {
        if length == frame.len() {
            return Err(Error::new(
                "frame_too_large",
                "managed MCP readiness frame exceeds 256 bytes",
            ));
        }
        let read = stdout
            .read(&mut frame[length..length + 1])
            .await
            .map_err(|_| {
                Error::new(
                    "cloud_mcp_ready_read",
                    "failed to read managed MCP readiness",
                )
            })?;
        if read == 0 {
            return Err(Error::new(
                "cloud_mcp_ready_read",
                "managed MCP child closed stdout before readiness",
            ));
        }
        length += read;
        if frame[length - 1] == b'\n' {
            break;
        }
    }

    let line = std::str::from_utf8(&frame[..length])
        .map_err(|_| Error::new("invalid", "managed MCP readiness is not UTF-8"))?;
    let ready = McpHttpReady::decode_frame(line)?;
    ready.validate()?;
    let address: SocketAddr = ready
        .address
        .parse()
        .map_err(|_| Error::new("invalid", "managed MCP address is invalid"))?;
    if address.port() == 0 {
        return Err(Error::new(
            "invalid",
            "managed MCP listener reported an unbound port",
        ));
    }
    Ok(ready)
}

struct PrivateTunnelConfig {
    path: PathBuf,
}

impl Drop for PrivateTunnelConfig {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn write_tunnel_config(
    config: &CloudMcpConfig,
    data_root: &Path,
    ready: &McpHttpReady,
) -> Result<PrivateTunnelConfig> {
    let credentials_path = credentials_path_scalar(&config.credentials_file)?;
    let address: SocketAddr = ready
        .address
        .parse()
        .map_err(|_| Error::new("invalid", "managed MCP address is invalid"))?;
    let origin = format!("http://{address}");
    let contents = format!(
        "tunnel: {}\ncredentials-file: {}\ningress:\n  - hostname: {}\n    service: {}\n    originRequest:\n      access:\n        required: true\n        teamName: {}\n        audTag:\n          - {}\n  - service: http_status:404\n",
        yaml_scalar(&config.tunnel_id),
        yaml_scalar(&credentials_path),
        yaml_scalar(&config.hostname),
        yaml_scalar(&origin),
        yaml_scalar(&config.access_team_name),
        yaml_scalar(&config.access_audience_tag),
    );

    let path = data_root.join(format!(".cloud-mcp-{}.yml", ulid::Ulid::new()));
    let mut file = create_private_file(&path)
        .map_err(|_| Error::new("cloud_mcp_config", "failed to create private tunnel config"))?;
    let guard = PrivateTunnelConfig { path };
    file.write_all(contents.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|_| Error::new("cloud_mcp_config", "failed to write private tunnel config"))?;
    drop(file);
    Ok(guard)
}

fn create_private_file(path: &Path) -> std::io::Result<File> {
    #[cfg(windows)]
    {
        crate::platform::windows::create_private_file_new(path)
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
        options.open(path)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "private files are unsupported on this platform",
        ))
    }
}

fn yaml_scalar(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

pub(crate) fn validate_config(config: &CloudMcpConfig) -> Result<String> {
    if !config.http_mcp_executable.is_absolute()
        || !config.cloudflared_executable.is_absolute()
        || !config.credentials_file.is_absolute()
        || !valid_tunnel_id(&config.tunnel_id)
        || !valid_hostname(&config.hostname)
        || !valid_label(&config.access_team_name)
        || config.access_audience_tag.len() != 64
        || !config
            .access_audience_tag
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::new(
            "invalid",
            "cloud MCP configuration is malformed",
        ));
    }
    let _ = credentials_path_scalar(&config.credentials_file)?;

    let public_url = format!("https://{}{MCP_HTTP_PATH}", config.hostname);
    Ok(public_url)
}

fn credentials_path_scalar(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .ok_or_else(|| Error::new("invalid", "credentials path must be valid Unicode"))?
        .replace('\\', "/");
    if value.chars().any(char::is_control) {
        return Err(Error::new(
            "invalid",
            "credentials path contains unsupported control characters",
        ));
    }
    Ok(value)
}

fn valid_tunnel_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

fn valid_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value.as_bytes()[0].is_ascii_alphanumeric()
        && value.as_bytes()[value.len() - 1].is_ascii_alphanumeric()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_hostname(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
}

fn map_custody(custody: CustodyGuarantee) -> CloudMcpCustody {
    #[cfg(windows)]
    {
        match custody {
            CustodyGuarantee::Exact => CloudMcpCustody::WindowsJobExact,
        }
    }
    #[cfg(unix)]
    {
        match custody {
            CustodyGuarantee::Degraded => CloudMcpCustody::UnixProcessGroupDegraded,
            CustodyGuarantee::Unknown => CloudMcpCustody::Unknown,
        }
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = custody;
        CloudMcpCustody::Unknown
    }
}

fn combine_custody(http: CustodyGuarantee, tunnel: CustodyGuarantee) -> CloudMcpCustody {
    #[cfg(windows)]
    {
        if http == CustodyGuarantee::Exact && tunnel == CustodyGuarantee::Exact {
            CloudMcpCustody::WindowsJobExact
        } else {
            CloudMcpCustody::Unknown
        }
    }
    #[cfg(unix)]
    {
        if http == CustodyGuarantee::Degraded && tunnel == CustodyGuarantee::Degraded {
            CloudMcpCustody::UnixProcessGroupDegraded
        } else {
            CloudMcpCustody::Unknown
        }
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = (http, tunnel);
        CloudMcpCustody::Unknown
    }
}

fn platform_custody() -> CloudMcpCustody {
    #[cfg(windows)]
    {
        CloudMcpCustody::WindowsJobExact
    }
    #[cfg(unix)]
    {
        CloudMcpCustody::UnixProcessGroupDegraded
    }
    #[cfg(not(any(windows, unix)))]
    {
        CloudMcpCustody::Unknown
    }
}

async fn settle_child(
    child: &mut OwnedChild,
    stdin: Option<ChildStdin>,
    already_reaped: bool,
    graceful: bool,
) -> Result<()> {
    drop(stdin);
    let mut reaped = already_reaped;
    let mut wait_error = None;

    if !reaped && graceful {
        match timeout(SHUTDOWN_GRACE, child.wait()).await {
            Ok(Ok(_)) => reaped = true,
            Ok(Err(error)) => wait_error = Some(error),
            Err(_) => {}
        }
    }

    if !reaped {
        let terminate_error = child.terminate().err();
        match timeout(REAP_TIMEOUT, child.wait()).await {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(terminate_error.or(wait_error).unwrap_or_else(|| {
                    Error::new(
                        "cloud_mcp_cleanup",
                        "managed child did not exit after termination",
                    )
                }));
            }
        }
    }

    #[cfg(windows)]
    if !child.wait_family_empty(WINDOWS_JOB_DRAIN_TIMEOUT).await? {
        return Err(Error::new(
            "process_custody_unknown",
            "managed child Job did not become empty before the cleanup deadline",
        ));
    }
    Ok(())
}

fn combine_cleanup(first: Result<()>, second: Result<()>) -> Result<()> {
    match (first, second) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), _) | (_, Err(error)) => Err(error),
    }
}

fn current_slot() -> &'static Mutex<Option<CloudMcpSnapshot>> {
    CURRENT.get_or_init(|| Mutex::new(None))
}

struct StatusGuard {
    completed: bool,
}

impl StatusGuard {
    fn begin(snapshot: CloudMcpSnapshot) -> Result<Self> {
        let mut current = current_slot().lock_recover();
        if current.as_ref().is_some_and(|snapshot| {
            matches!(
                snapshot.health,
                CloudMcpHealth::StartingHttp
                    | CloudMcpHealth::StartingTunnel
                    | CloudMcpHealth::Running
                    | CloudMcpHealth::Stopping
            )
        }) {
            return Err(Error::new(
                "cloud_mcp_already_running",
                "a cloud MCP runtime is already active",
            ));
        }
        *current = Some(snapshot);
        Ok(Self { completed: false })
    }

    fn set(&self, health: CloudMcpHealth) {
        if let Some(snapshot) = current_slot().lock_recover().as_mut() {
            snapshot.health = health;
        }
    }

    fn set_custody(&self, custody: CloudMcpCustody) {
        if let Some(snapshot) = current_slot().lock_recover().as_mut() {
            snapshot.custody = custody;
        }
    }

    fn finish(&mut self, health: CloudMcpHealth) {
        self.set(health);
        self.completed = true;
    }
}

impl Drop for StatusGuard {
    fn drop(&mut self) {
        if !self.completed
            && let Some(snapshot) = current_slot().lock_recover().as_mut()
        {
            snapshot.health = CloudMcpHealth::Failed;
        }
    }
}
