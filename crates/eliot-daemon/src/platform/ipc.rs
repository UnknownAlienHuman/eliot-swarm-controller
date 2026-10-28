use crate::platform::data_dir::{DaemonLock, DataDir};
use eliot_core::{Error, Result};
#[cfg(unix)]
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::ReadBuf;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::Instant;

/// Common async byte stream returned by either local IPC transport.
pub trait IpcStreamIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> IpcStreamIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

pub struct IpcStream(Box<dyn IpcStreamIo>);

impl AsyncRead for IpcStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(self.get_mut().0.as_mut()).poll_read(context, buffer)
    }
}

impl AsyncWrite for IpcStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(self.get_mut().0.as_mut()).poll_write(context, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(self.get_mut().0.as_mut()).poll_flush(context)
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(self.get_mut().0.as_mut()).poll_shutdown(context)
    }
}

/// One private local listener. Binding requires the matching daemon lock.
pub struct IpcListener {
    endpoint: String,
    #[cfg(unix)]
    listener: tokio::net::UnixListener,
    #[cfg(unix)]
    socket_identity: (u64, u64),
    #[cfg(windows)]
    next_server: Option<tokio::net::windows::named_pipe::NamedPipeServer>,
}

impl IpcListener {
    pub async fn bind(data_dir: &DataDir, lock: &DaemonLock) -> Result<Self> {
        data_dir.check_lock(lock)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

            let socket_path = data_dir.path().join("daemon.sock");
            let endpoint = socket_path.to_str().ok_or_else(|| {
                Error::new(
                    "invalid_endpoint",
                    "Unix socket path cannot be represented as UTF-8",
                )
            })?;
            if endpoint.len() > 107 {
                return Err(Error::new(
                    "invalid_endpoint",
                    "Unix socket path exceeds the platform limit",
                ));
            }
            prepare_socket_path(&socket_path).await?;
            let listener = tokio::net::UnixListener::bind(&socket_path)
                .map_err(|error| io_error("bind Unix daemon socket", error))?;
            std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(0o600))
                .map_err(|error| io_error("restrict Unix daemon socket", error))?;
            let metadata = std::fs::symlink_metadata(&socket_path)
                .map_err(|error| io_error("inspect Unix daemon socket", error))?;
            if !metadata.file_type().is_socket() || metadata.uid() != super::unix::effective_uid() {
                return Err(Error::new(
                    "unsafe_ipc_endpoint",
                    "bound Unix endpoint is not an owned socket",
                ));
            }
            Ok(Self {
                endpoint: endpoint.to_owned(),
                listener,
                socket_identity: (metadata.dev(), metadata.ino()),
            })
        }

        #[cfg(windows)]
        {
            let endpoint = format!(r"\\.\pipe\eliot-{}", ulid::Ulid::new());
            let next_server = Some(super::windows::create_pipe_server(&endpoint, true)?);
            Ok(Self {
                endpoint,
                next_server,
            })
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = (data_dir, lock);
            Err(Error::new(
                "platform_unsupported",
                "local IPC is implemented for Windows and Unix",
            ))
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub async fn accept(&mut self) -> Result<IpcStream> {
        #[cfg(unix)]
        {
            let (stream, _) = self
                .listener
                .accept()
                .await
                .map_err(|error| io_error("accept Unix daemon connection", error))?;
            Ok(IpcStream(Box::new(stream)))
        }

        #[cfg(windows)]
        {
            if self.next_server.is_none() {
                self.next_server = Some(super::windows::create_pipe_server(&self.endpoint, false)?);
            }
            {
                let server = self.next_server.as_ref().ok_or_else(|| {
                    Error::new("platform_io", "named-pipe listener instance is unavailable")
                })?;
                server
                    .connect()
                    .await
                    .map_err(|error| io_error("accept named-pipe connection", error))?;
            }

            let next_server = super::windows::create_pipe_server(&self.endpoint, false)?;
            let connected_server = self.next_server.replace(next_server).ok_or_else(|| {
                Error::new(
                    "platform_io",
                    "connected named-pipe listener instance disappeared",
                )
            })?;
            Ok(IpcStream(Box::new(connected_server)))
        }

        #[cfg(not(any(unix, windows)))]
        {
            Err(Error::new(
                "platform_unsupported",
                "local IPC is implemented for Windows and Unix",
            ))
        }
    }
}

impl Drop for IpcListener {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let path = PathBuf::from(&self.endpoint);
            if let Ok(metadata) = std::fs::symlink_metadata(&path)
                && (metadata.dev(), metadata.ino()) == self.socket_identity
                && metadata.uid() == super::unix::effective_uid()
            {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

impl IpcStream {
    pub async fn connect(endpoint: &str, until: Instant) -> Result<Self> {
        #[cfg(unix)]
        {
            let _ = until;
            let stream = tokio::net::UnixStream::connect(endpoint)
                .await
                .map_err(|error| io_error("connect Unix daemon socket", error))?;
            Ok(IpcStream(Box::new(stream)))
        }

        #[cfg(windows)]
        {
            use std::time::Duration;
            use tokio::net::windows::named_pipe::ClientOptions;
            use tokio::time::sleep_until;
            use windows_sys::Win32::Foundation::ERROR_PIPE_BUSY;

            loop {
                if Instant::now() >= until {
                    return Err(Error::new(
                        "unknown",
                        "daemon RPC exceeded its absolute deadline",
                    ));
                }
                match ClientOptions::new().open(endpoint) {
                    Ok(client) => return Ok(IpcStream(Box::new(client))),
                    Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => {
                        // Tokio's documented connect pattern: wait for the next
                        // listener instance. No request bytes have been written.
                        sleep_until((Instant::now() + Duration::from_millis(50)).min(until)).await;
                    }
                    Err(error) => return Err(io_error("connect daemon named pipe", error)),
                }
            }
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = (endpoint, until);
            Err(Error::new(
                "platform_unsupported",
                "local IPC is implemented for Windows and Unix",
            ))
        }
    }
}

#[cfg(unix)]
async fn prepare_socket_path(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error("inspect Unix daemon socket path", error)),
    };
    if !metadata.file_type().is_socket() || metadata.uid() != super::unix::effective_uid() {
        return Err(Error::new(
            "unsafe_ipc_endpoint",
            "refusing to replace a non-socket or another user's endpoint",
        ));
    }

    match tokio::net::UnixStream::connect(path).await {
        Ok(_stream) => Err(Error::new(
            "daemon_already_running",
            "the existing Unix endpoint accepts connections",
        )),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) =>
        {
            std::fs::remove_file(path)
                .map_err(|error| io_error("remove stale Unix daemon socket", error))
        }
        Err(error) => Err(io_error("probe existing Unix daemon socket", error)),
    }
}

fn io_error(action: &str, error: std::io::Error) -> Error {
    Error::new("platform_io", format!("{action}: {error}"))
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use futures_util::{StreamExt, stream::FuturesUnordered};
    use std::{collections::HashSet, time::Duration};
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::windows::named_pipe::ClientOptions,
        time::timeout_at,
    };
    use windows_sys::Win32::Foundation::ERROR_PIPE_BUSY;

    fn listener() -> IpcListener {
        let endpoint = format!(r"\\.\pipe\eliot-ipc-test-{}", ulid::Ulid::new());
        let next_server = Some(super::super::windows::create_pipe_server(&endpoint, true).unwrap());
        IpcListener {
            endpoint,
            next_server,
        }
    }

    #[tokio::test]
    async fn busy_pipe_concurrent_clients_connect_then_write_each_rpc_once() {
        let mut listener = listener();
        let endpoint = listener.endpoint.clone();
        // Occupy the sole instance before clients begin. Every manually-polled
        // connect must encounter PIPE_BUSY before accept can replace it.
        let blocker = ClientOptions::new().open(&endpoint).unwrap();
        match ClientOptions::new().open(&endpoint) {
            Err(error) => assert_eq!(error.raw_os_error(), Some(ERROR_PIPE_BUSY as i32)),
            Ok(_) => panic!("expected the occupied pipe instance to be busy"),
        }
        let until = Instant::now() + Duration::from_secs(5);
        let mut pending = Vec::new();
        for id in 0..8_u64 {
            let mut connect = Box::pin(IpcStream::connect(&endpoint, until));
            assert!(futures_util::poll!(&mut connect).is_pending());
            pending.push((id, connect));
        }
        let clients = async {
            let mut calls = FuturesUnordered::new();
            for (id, connect) in pending {
                calls.push(async move {
                    let stream = connect.await.unwrap();
                    let mut io = BufReader::new(stream);
                    let frame = format!("{}\n", serde_json::json!({"jsonrpc":"2.0", "id":id, "method":"test.read", "params":{}}));
                    io.get_mut().write_all(frame.as_bytes()).await.unwrap();
                    let mut response = String::new();
                    assert!(io.read_line(&mut response).await.unwrap() > 0);
                    assert_eq!(serde_json::from_str::<serde_json::Value>(&response).unwrap()["id"], id);
                });
            }
            while let Some(()) = calls.next().await {}
        };
        let server = async {
            let occupied = listener.accept().await.unwrap();
            drop(blocker);
            drop(occupied);
            let mut seen = HashSet::new();
            for _ in 0..8 {
                let mut io = BufReader::new(listener.accept().await.unwrap());
                let mut frame = String::new();
                assert!(io.read_line(&mut frame).await.unwrap() > 0);
                let request: serde_json::Value = serde_json::from_str(&frame).unwrap();
                let id = request["id"].as_u64().unwrap();
                assert!(seen.insert(id), "an RPC request was replayed");
                io.get_mut()
                    .write_all(format!("{{\"id\":{id}}}\n").as_bytes())
                    .await
                    .unwrap();
                let mut extra = String::new();
                assert_eq!(
                    io.read_line(&mut extra).await.unwrap(),
                    0,
                    "extra RPC bytes after the single request"
                );
            }
            assert_eq!(seen.len(), 8);
        };
        timeout_at(until, async {
            tokio::join!(clients, server);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn busy_pipe_connect_stops_at_original_deadline_without_writing() {
        let listener = listener();
        let _occupied = ClientOptions::new().open(listener.endpoint()).unwrap();
        let error = match IpcStream::connect(
            listener.endpoint(),
            Instant::now() + Duration::from_millis(5),
        )
        .await
        {
            Err(error) => error,
            Ok(_) => panic!("occupied pipe cannot accept the timed-out connection"),
        };
        assert_eq!(error.code, "unknown");
        assert_eq!(error.message, "daemon RPC exceeded its absolute deadline");
    }

    #[tokio::test]
    async fn missing_pipe_is_not_retried_as_busy() {
        let endpoint = format!(r"\\.\pipe\eliot-ipc-missing-{}", ulid::Ulid::new());
        let error =
            match IpcStream::connect(&endpoint, Instant::now() + Duration::from_secs(5)).await {
                Err(error) => error,
                Ok(_) => panic!("missing pipe unexpectedly connected"),
            };
        assert_eq!(error.code, "platform_io");
    }
}
