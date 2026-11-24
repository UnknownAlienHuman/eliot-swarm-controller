//! Shared bounded I/O capture and native family-quiescence wait.

#[cfg(any(windows, target_os = "linux"))]
use crate::platform::OwnedChild;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt};

pub(crate) struct BoundedCapture {
    pub(crate) bytes: Vec<u8>,
    pub(crate) exceeded: bool,
}

pub(crate) async fn capture_bounded<R: AsyncRead + Unpin>(
    reader: R,
    limit: usize,
    initial_capacity: usize,
) -> io::Result<BoundedCapture> {
    let mut bytes = Vec::with_capacity(limit.min(initial_capacity));
    reader
        .take(limit.saturating_add(1) as u64)
        .read_to_end(&mut bytes)
        .await?;
    let exceeded = bytes.len() > limit;
    bytes.truncate(limit);
    Ok(BoundedCapture { bytes, exceeded })
}

#[cfg(any(windows, target_os = "linux"))]
pub(crate) async fn wait_family_empty(
    child: &mut OwnedChild,
    timeout: std::time::Duration,
) -> eliot_core::Result<bool> {
    #[cfg(windows)]
    {
        child.wait_family_empty(timeout).await
    }
    #[cfg(target_os = "linux")]
    {
        child.wait_group_empty_except_leader(timeout).await
    }
}
