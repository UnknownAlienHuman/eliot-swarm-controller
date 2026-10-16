//! Bounded controller for the exact pinned SDK bridge harness. The child is
//! created by this verified module process and inherits its managed owner
//! group; its direct process is never force-killed or replaced on ambiguity.

use crate::config::NativeOptions;
use serde_json::Value;
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    process::Stdio,
};
use swarm_contracts::error::{Error, Result};
use swarm_process::{private_permissions, write_private_new};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, Command},
    sync::mpsc,
};

const MAX_COMMAND_BYTES: usize = 1_100_000;
const MAX_FRAME_BYTES: usize = 700_000;

#[derive(Debug)]
pub enum HarnessFrame {
    Message(Value),
    StdoutEof,
    Exited { code: Option<i32> },
    ReadFailure,
}

pub struct NativeHarness {
    stdin: ChildStdin,
}

impl NativeHarness {
    pub fn spawn(
        options: NativeOptions,
        bridge_path: PathBuf,
        tx: mpsc::Sender<HarnessFrame>,
    ) -> Result<Self> {
        if !bridge_path.is_absolute() || !bridge_path.is_file() {
            return Err(Error::new(
                "SDK_HARNESS_MISSING",
                "installed SDK harness is unavailable",
            ));
        }
        let node_entrypoint = node_entrypoint_path(&bridge_path);
        let mut command = Command::new(&options.node_executable);
        command
            .arg(&node_entrypoint)
            .current_dir(&options.workspace_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            // Provider stderr may contain user prompts or local data; it is
            // discarded instead of copied into the adapter or Store logs.
            .stderr(Stdio::null());
        copy_safe_environment(&mut command, &options);
        let mut child = command.spawn().map_err(|_| {
            Error::new(
                "SDK_HARNESS_START",
                "pinned Node harness could not be started",
            )
        })?;
        let Some(stdin) = child.stdin.take() else {
            spawn_child_waiter(child, tx);
            return Err(Error::new(
                "SDK_HARNESS_START",
                "harness input pipe is unavailable",
            ));
        };
        let Some(stdout) = child.stdout.take() else {
            drop(stdin);
            spawn_child_waiter(child, tx);
            return Err(Error::new(
                "SDK_HARNESS_START",
                "harness output pipe is unavailable",
            ));
        };
        spawn_output_reader(stdout, tx.clone());
        spawn_child_waiter(child, tx);
        Ok(Self { stdin })
    }

    #[cfg(test)]
    pub fn for_test_stdio(stdin: ChildStdin) -> Self {
        Self { stdin }
    }

    pub async fn send(&mut self, value: Value) -> Result<()> {
        let bytes = serde_json::to_vec(&value)?;
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(Error::new(
                "SDK_HARNESS_COMMAND_BOUNDARY",
                "harness command exceeds its size boundary",
            ));
        }
        self.stdin
            .write_all(&bytes)
            .await
            .map_err(|_| Error::new("SDK_HARNESS_PIPE", "harness command delivery is uncertain"))?;
        self.stdin
            .write_all(b"\n")
            .await
            .map_err(|_| Error::new("SDK_HARNESS_PIPE", "harness command delivery is uncertain"))?;
        self.stdin
            .flush()
            .await
            .map_err(|_| Error::new("SDK_HARNESS_PIPE", "harness command delivery is uncertain"))
    }
}

#[cfg(windows)]
fn node_entrypoint_path(path: &Path) -> PathBuf {
    use std::os::windows::ffi::{OsStrExt, OsStringExt};

    let wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
    let prefix = [b'\\' as u16, b'\\' as u16, b'?' as u16, b'\\' as u16];
    let Some(rest) = wide.strip_prefix(&prefix) else {
        return path.to_path_buf();
    };
    let is_drive_path = rest.len() >= 3
        && rest[1] == b':' as u16
        && matches!(rest[2], value if value == b'\\' as u16 || value == b'/' as u16)
        && matches!(rest[0], 65..=90 | 97..=122);
    if is_drive_path {
        return PathBuf::from(std::ffi::OsString::from_wide(rest));
    }
    let is_unc_path = rest.len() >= 4
        && matches!(rest[0], 85 | 117)
        && matches!(rest[1], 78 | 110)
        && matches!(rest[2], 67 | 99)
        && matches!(rest[3], value if value == b'\\' as u16 || value == b'/' as u16);
    if is_unc_path {
        let mut normalized = vec![b'\\' as u16, b'\\' as u16];
        normalized.extend_from_slice(&rest[4..]);
        return PathBuf::from(std::ffi::OsString::from_wide(&normalized));
    }
    path.to_path_buf()
}

#[cfg(not(windows))]
fn node_entrypoint_path(path: &Path) -> PathBuf {
    path.to_path_buf()
}

/// Materialize the small pinned protocol shim inside this binding owner's
/// private state directory. The standard module installer only copies the
/// executable and descriptor; the executable hash therefore pins these bytes.
pub fn materialize(directory: &Path) -> Result<PathBuf> {
    fs::create_dir_all(directory).map_err(|_| {
        Error::new(
            "SDK_HARNESS_PATH",
            "private SDK harness directory is unavailable",
        )
    })?;
    let metadata = fs::symlink_metadata(directory).map_err(|_| {
        Error::new(
            "SDK_HARNESS_PATH",
            "private SDK harness directory is unavailable",
        )
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            "SDK_HARNESS_PATH",
            "SDK harness location is not a private directory",
        ));
    }
    private_permissions(directory, true)?;
    let bridge_path = directory.join("bridge.mjs");
    ensure_embedded_file(
        &bridge_path,
        include_str!("../sdk-harness/bridge.mjs").as_bytes(),
    )?;
    ensure_embedded_file(
        &directory.join("prepared-query.mjs"),
        include_bytes!("../sdk-harness/prepared-query.mjs"),
    )?;
    ensure_embedded_file(
        &directory.join("interaction-projection.mjs"),
        include_bytes!("../sdk-harness/interaction-projection.mjs"),
    )?;
    Ok(bridge_path)
}

fn ensure_embedded_file(path: &Path, expected: &[u8]) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || metadata.len() > expected.len() as u64
            {
                return Err(Error::new(
                    "SDK_HARNESS_INTEGRITY",
                    "existing SDK harness file is not the pinned regular file",
                ));
            }
            let mut file = File::open(path).map_err(|_| {
                Error::new("SDK_HARNESS_INTEGRITY", "SDK harness file cannot be read")
            })?;
            let mut bytes = Vec::with_capacity(expected.len());
            file.by_ref()
                .take(expected.len() as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| {
                    Error::new("SDK_HARNESS_INTEGRITY", "SDK harness file cannot be read")
                })?;
            if bytes.as_slice() != expected {
                return Err(Error::new(
                    "SDK_HARNESS_INTEGRITY",
                    "existing SDK harness bytes differ from the executable pin",
                ));
            }
            private_permissions(path, false)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            write_private_new(path, expected).map_err(|_| {
                Error::new(
                    "SDK_HARNESS_PATH",
                    "pinned SDK harness file cannot be created",
                )
            })?;
        }
        Err(_) => {
            return Err(Error::new(
                "SDK_HARNESS_PATH",
                "SDK harness file cannot be inspected",
            ));
        }
    }
    Ok(())
}

fn copy_safe_environment(command: &mut Command, options: &NativeOptions) {
    swarm_process::module_owner::apply_native_launch_environment(command.as_std_mut());
    command.env("CLAUDE_CONFIG_DIR", &options.claude_config_dir);
}

fn spawn_output_reader(stdout: tokio::process::ChildStdout, tx: mpsc::Sender<HarnessFrame>) {
    tokio::spawn(async move {
        forward_output(stdout, tx).await;
    });
}

async fn forward_output<R: AsyncRead + Unpin>(stdout: R, tx: mpsc::Sender<HarnessFrame>) {
    let mut reader = BufReader::new(stdout);
    loop {
        let line = match read_bounded_line(&mut reader, MAX_FRAME_BYTES).await {
            Ok(Some(line)) => line,
            Ok(None) => {
                let _ = tx.send(HarnessFrame::StdoutEof).await;
                return;
            }
            Err(()) => {
                let _ = tx.send(HarnessFrame::ReadFailure).await;
                drain_output(&mut reader).await;
                return;
            }
        };
        let value = match serde_json::from_slice(&line) {
            Ok(value) => value,
            Err(_) => {
                let _ = tx.send(HarnessFrame::ReadFailure).await;
                drain_output(&mut reader).await;
                return;
            }
        };
        if tx.send(HarnessFrame::Message(value)).await.is_err() {
            return;
        }
    }
}

async fn drain_output<R: AsyncRead + Unpin>(reader: &mut R) {
    let mut scratch = [0u8; 8192];
    while matches!(reader.read(&mut scratch).await, Ok(count) if count > 0) {}
}

async fn read_bounded_line<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    maximum: usize,
) -> std::result::Result<Option<Vec<u8>>, ()> {
    let mut line = Vec::new();
    loop {
        let (take, found_newline, eof) = {
            let buffer = reader.fill_buf().await.map_err(|_| ())?;
            if buffer.is_empty() {
                (0, false, true)
            } else {
                let length = buffer
                    .iter()
                    .position(|byte| *byte == b'\n')
                    .map_or(buffer.len(), |index| index + 1);
                if line.len().saturating_add(length) > maximum {
                    return Err(());
                }
                line.extend_from_slice(&buffer[..length]);
                (
                    length,
                    buffer.get(length.saturating_sub(1)) == Some(&b'\n'),
                    false,
                )
            }
        };
        if eof {
            return if line.is_empty() { Ok(None) } else { Err(()) };
        }
        reader.consume(take);
        if found_newline {
            line.pop();
            return Ok(Some(line));
        }
    }
}

fn spawn_child_waiter(mut child: Child, tx: mpsc::Sender<HarnessFrame>) {
    tokio::spawn(async move {
        let code = child.wait().await.ok().and_then(|status| status.code());
        let _ = tx.send(HarnessFrame::Exited { code }).await;
    });
}

#[cfg(test)]
mod output_reader_tests {
    use super::{HarnessFrame, forward_output};
    use tokio::{
        io::{AsyncWriteExt, duplex},
        sync::mpsc,
    };

    #[tokio::test]
    async fn clean_stdout_eof_emits_a_distinct_terminal_frame() {
        let (mut writer, reader) = duplex(1024);
        let (tx, mut rx) = mpsc::channel(4);
        let reader_task = tokio::spawn(forward_output(reader, tx));

        writer
            .write_all(b"{\"kind\":\"diagnostic\"}\n")
            .await
            .expect("write one complete harness frame");
        writer
            .shutdown()
            .await
            .expect("close the harness stdout stream");

        assert!(matches!(
            rx.recv().await,
            Some(HarnessFrame::Message(value)) if value["kind"] == "diagnostic"
        ));
        assert!(matches!(rx.recv().await, Some(HarnessFrame::StdoutEof)));
        reader_task.await.expect("finish the bounded output reader");
    }
}

#[cfg(all(test, windows))]
mod startup_node_entrypoint_path_tests {
    use super::node_entrypoint_path;
    use std::path::PathBuf;

    #[test]
    fn claude_minimal_regression_strips_extended_drive_prefix_for_node() {
        let extended = PathBuf::from(r"\\?\C:\Program Files\Eliot\sdk-harness\bridge.mjs");
        assert_eq!(
            node_entrypoint_path(&extended),
            PathBuf::from(r"C:\Program Files\Eliot\sdk-harness\bridge.mjs")
        );
    }

    #[test]
    fn claude_minimal_regression_normalizes_extended_unc_path_for_node() {
        let extended = PathBuf::from(r"\\?\UNC\server\share\sdk-harness\bridge.mjs");
        assert_eq!(
            node_entrypoint_path(&extended),
            PathBuf::from(r"\\server\share\sdk-harness\bridge.mjs")
        );
    }
}
