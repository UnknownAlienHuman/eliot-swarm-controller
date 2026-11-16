//! Owner-managed Cloudflare Tunnel for the loopback StreamableHTTP MCP server.
//!
//! The module owns exactly two children: the configured `swarm mcp --http`
//! command and one `cloudflared tunnel run`. Cloudflare Access performs the
//! external OAuth/JWT check; this module does not implement an auth framework.

use crate::platform::{CustodyGuarantee, DataDir, OwnedChild};
use eliot_core::{
    Error, Result, SecretPolicy, StderrSnapshot, StderrSource,
    api::{CloudMcpCustody, CloudMcpHealth, CloudMcpStatus},
};
use eliot_plugin::{
    StderrCapture,
    protocol::{MAX_MCP_HTTP_READY_BYTES, McpHttpReady},
    spawn_stderr,
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
    sync::{Arc, Mutex, OnceLock},
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
    let policy = Arc::new(supervisor_policy(&config));
    let mut status = StatusGuard::begin(CloudMcpSnapshot {
        public_url,
        health: CloudMcpHealth::StartingHttp,
        custody: platform_custody(),
        http_stderr: None,
        tunnel_stderr: None,
    })?;

    let result = supervise(config, data_root, runtime_url, shutdown, &status, policy).await;
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
    policy: Arc<SecretPolicy>,
) -> Result<()> {
    if shutdown.is_cancelled() {
        status.set(CloudMcpHealth::Stopping);
        return Ok(());
    }

    let mut http = spawn_http(&config, &data_root, &public_url)?;
    status.set_custody(map_custody(http.guarantee()));
    let Some(http_stderr) = take_capture(&mut http, &policy) else {
        settle_child(&mut http, None, false, false).await?;
        return Err(Error::new(
            "cloud_mcp_spawn",
            "managed MCP stderr pipe is unavailable",
        ));
    };
    let Some(http_stdin) = http.take_stdin() else {
        settle_child(&mut http, None, false, false).await?;
        let tail = http_stderr.view().snapshot();
        status.set_http_stderr(tail.clone());
        return Err(
            Error::new("cloud_mcp_spawn", "managed MCP stdin pipe is unavailable")
                .with_stderr(StderrSource::Harness, tail),
        );
    };
    let Some(mut http_stdout) = http.take_stdout() else {
        settle_child(&mut http, Some(http_stdin), false, false).await?;
        let tail = http_stderr.view().snapshot();
        status.set_http_stderr(tail.clone());
        return Err(
            Error::new("cloud_mcp_spawn", "managed MCP stdout pipe is unavailable")
                .with_stderr(StderrSource::Harness, tail),
        );
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
            status.set_http_stderr(http_stderr.view().snapshot());
            return Ok(());
        }
        Startup::HttpExited(result) => {
            status.set(CloudMcpHealth::Stopping);
            settle_child(&mut http, Some(http_stdin), result.is_ok(), false).await?;
            let tail = http_stderr.view().snapshot();
            status.set_http_stderr(tail.clone());
            return Err(Error::new(
                "cloud_mcp_http_exit",
                "managed MCP HTTP child exited before readiness",
            )
            .with_stderr(StderrSource::Harness, tail));
        }
        Startup::Ready(Err(error)) => {
            status.set(CloudMcpHealth::Stopping);
            settle_child(&mut http, Some(http_stdin), false, false).await?;
            let tail = http_stderr.view().snapshot();
            status.set_http_stderr(tail.clone());
            return Err(error.with_stderr(StderrSource::Harness, tail));
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
            let tail = http_stderr.view().snapshot();
            status.set_http_stderr(tail.clone());
            return Err(error.with_stderr(StderrSource::Harness, tail));
        }
    };
    let Some(tunnel_stderr) = take_capture(&mut tunnel, &policy) else {
        status.set(CloudMcpHealth::Stopping);
        let tunnel_result = settle_child(&mut tunnel, None, false, false).await;
        let http_result = settle_child(&mut http, Some(http_stdin), false, false).await;
        combine_cleanup(tunnel_result, http_result)?;
        return Err(Error::new(
            "cloud_mcp_spawn",
            "cloudflared stderr pipe is unavailable",
        ));
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
            status.set_http_stderr(http_stderr.view().snapshot());
            status.set_tunnel_stderr(tunnel_stderr.view().snapshot());
            combine_cleanup(tunnel_result, http_result)
        }
        Supervision::HttpExited(result) => {
            let http_result =
                settle_child(&mut http, Some(http_stdin), result.is_ok(), false).await;
            let tunnel_result = settle_child(&mut tunnel, None, false, false).await;
            combine_cleanup(http_result, tunnel_result)?;
            let tail = http_stderr.view().snapshot();
            status.set_http_stderr(tail.clone());
            status.set_tunnel_stderr(tunnel_stderr.view().snapshot());
            Err(Error::new(
                "cloud_mcp_http_exit",
                "managed MCP HTTP child exited while the tunnel was running",
            )
            .with_stderr(StderrSource::Harness, tail))
        }
        Supervision::TunnelExited(result) => {
            let tunnel_result = settle_child(&mut tunnel, None, result.is_ok(), false).await;
            let http_result = settle_child(&mut http, Some(http_stdin), false, true).await;
            combine_cleanup(tunnel_result, http_result)?;
            let tail = tunnel_stderr.view().snapshot();
            status.set_http_stderr(http_stderr.view().snapshot());
            status.set_tunnel_stderr(tail.clone());
            Err(Error::new(
                "cloud_mcp_tunnel_exit",
                "cloudflared exited while the HTTP MCP child was running",
            )
            .with_stderr(StderrSource::Harness, tail))
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
        .stderr(Stdio::piped());
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
        .stderr(Stdio::piped());
    add_minimal_os_environment(&mut command)?;
    OwnedChild::spawn(command)
}

/// Supervisor-owned stderr capture for one managed child, following the
/// plugin host: the pipe is drained by an owned task while the capture lives
/// in the supervisor, and views stay readable after the child exits.
fn take_capture(child: &mut OwnedChild, policy: &Arc<SecretPolicy>) -> Option<StderrCapture> {
    child
        .take_stderr()
        .map(|pipe| spawn_stderr(pipe, Arc::clone(policy)))
}

/// Secrets for supervisor-owned stderr capture: the credential path itself
/// plus the secret string values inside the credentials file. Hostnames,
/// tunnel IDs and team names stay visible so tails keep their value.
/// Never persisted: the policy only redacts captured bytes in memory.
fn supervisor_policy(config: &CloudMcpConfig) -> SecretPolicy {
    let raw = config.credentials_file.to_string_lossy().into_owned();
    let mut secrets = vec![raw.replace('\\', "/")];
    if secrets[0] != raw {
        secrets.push(raw);
    }
    if let Ok(raw) = fs::read(&config.credentials_file)
        && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&raw)
    {
        collect_secret_strings(&value, &mut secrets);
    }
    SecretPolicy::new(secrets)
}

fn collect_secret_strings(value: &serde_json::Value, out: &mut Vec<String>) {
    const MAX_SECRET_VALUES: usize = 32;
    const MAX_SECRET_LEN: usize = 4096;
    match value {
        serde_json::Value::String(text) => {
            if out.len() < MAX_SECRET_VALUES && !text.is_empty() && text.len() <= MAX_SECRET_LEN {
                out.push(text.clone());
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_secret_strings(item, out);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values() {
                collect_secret_strings(item, out);
            }
        }
        _ => {}
    }
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

    fn set_http_stderr(&self, snapshot: StderrSnapshot) {
        if let Some(status) = current_slot().lock_recover().as_mut() {
            status.http_stderr = Some(snapshot);
        }
    }

    fn set_tunnel_stderr(&self, snapshot: StderrSnapshot) {
        if let Some(status) = current_slot().lock_recover().as_mut() {
            status.tunnel_stderr = Some(snapshot);
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

#[cfg(test)]
mod supervisor_tail_tests {
    use super::*;
    use serde_json::json;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("eliot-cloud-mcp-{tag}-{}", ulid::Ulid::new()));
            std::fs::create_dir_all(&path).expect("create fixture dir");
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn test_config(credentials_file: PathBuf) -> CloudMcpConfig {
        CloudMcpConfig {
            http_mcp_executable: PathBuf::from("C:/tools/swarm.exe"),
            cloudflared_executable: PathBuf::from("C:/tools/cloudflared.exe"),
            tunnel_id: "01234567-89ab-cdef-0123-456789abcdef".to_owned(),
            credentials_file,
            hostname: "k95-test.example.com".to_owned(),
            access_team_name: "testteam".to_owned(),
            access_audience_tag: "ab".repeat(32),
        }
    }

    #[test]
    fn supervisor_policy_redacts_credential_paths_and_file_secrets() {
        let dir = TempDir::new("policy");
        let path = dir.0.join("credentials.json");
        let secret = "k95-fake-tunnel-secret-value";
        std::fs::write(
            &path,
            format!(r#"{{"TunnelSecret": "{secret}", "AccountTag": "team"}}"#),
        )
        .expect("write fixture credentials");
        let policy = supervisor_policy(&test_config(path.clone()));
        let displayed = path.to_string_lossy().replace('\\', "/");
        let probe = format!("failed reading {displayed} with {secret}");
        let redacted = policy.redact_text(&probe);
        assert!(
            !redacted.contains(secret),
            "file secret is redacted: {redacted}"
        );
        assert!(
            !redacted.contains(&displayed),
            "credential path is redacted: {redacted}"
        );
        assert!(
            redacted.contains("[REDACTED]"),
            "redaction is visible: {redacted}"
        );
    }

    #[tokio::test]
    async fn failing_child_exit_yields_redacted_tail() {
        let dir = TempDir::new("tail");
        let path = dir.0.join("credentials.json");
        let secret = "k95-fake-tail-secret-1";
        std::fs::write(&path, format!(r#"{{"TunnelSecret": "{secret}"}}"#))
            .expect("write fixture credentials");
        let policy = Arc::new(supervisor_policy(&test_config(path)));
        #[cfg(windows)]
        let mut command = {
            let mut command = Command::new("cmd.exe");
            command
                .arg("/C")
                .arg(format!("echo {secret} 1>&2 & exit 1"));
            command
        };
        #[cfg(unix)]
        let mut command = {
            let mut command = Command::new("/bin/sh");
            command.arg("-c").arg(format!("echo {secret} >&2; exit 1"));
            command
        };
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = OwnedChild::spawn(command).expect("spawn failing fixture child");
        let capture = take_capture(&mut child, &policy).expect("capture fixture stderr");
        let exit = child.wait().await.expect("reap fixture child");
        assert!(!exit.success(), "the fixture child fails");
        let tail = capture
            .finish(tokio::time::Instant::now() + Duration::from_secs(5))
            .await;
        assert!(tail.eof, "the drain reaches EOF: {tail:?}");
        assert!(
            !tail.text.contains(secret),
            "the tail never carries the secret: {}",
            tail.text
        );
        let error = Error::new(
            "cloud_mcp_tunnel_exit",
            "cloudflared exited while the HTTP MCP child was running",
        )
        .with_stderr(StderrSource::Harness, tail);
        let encoded = serde_json::to_value(&error).expect("encode error");
        let text = encoded
            .pointer("/diagnostics/0/snapshot/text")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        assert!(
            !text.contains(secret),
            "the diagnostic tail is redacted: {text}"
        );
        assert!(
            text.contains("[REDACTED]"),
            "the diagnostic tail keeps the failure line: {text}"
        );
    }

    #[test]
    fn cloud_mcp_status_roundtrips_optional_tails() {
        let status = CloudMcpStatus {
            public_url: "https://k95-test.example.com/mcp".to_owned(),
            health: CloudMcpHealth::Failed,
            custody: CloudMcpCustody::Unknown,
            http_stderr: Some(StderrSnapshot {
                text: "boom".to_owned(),
                truncated: false,
                eof: true,
                read_error: false,
            }),
            tunnel_stderr: None,
        };
        let encoded = serde_json::to_value(&status).expect("encode status");
        let back: CloudMcpStatus = serde_json::from_value(encoded).expect("decode status");
        assert_eq!(back, status);
        let legacy = json!({
            "public_url": "https://k95-test.example.com/mcp",
            "health": "failed",
            "custody": "unknown",
        });
        let back: CloudMcpStatus = serde_json::from_value(legacy).expect("legacy status decodes");
        assert!(
            back.http_stderr.is_none() && back.tunnel_stderr.is_none(),
            "tails stay absent until the supervisor records them"
        );
    }
}
