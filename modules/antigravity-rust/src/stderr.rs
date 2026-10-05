use tokio::io::{AsyncRead, AsyncReadExt};

const READ_CHUNK_BYTES: usize = 4096;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StderrSummary {
    pub bytes_discarded: u64,
    pub read_failed: bool,
}

/// Drain stderr so the native process cannot block on a full pipe. Raw stderr
/// is discarded because it may repeat prompts, local paths, or credentials;
/// only a bounded count is retained for manager diagnostics.
pub async fn drain<R>(mut reader: R) -> StderrSummary
where
    R: AsyncRead + Unpin,
{
    let mut summary = StderrSummary::default();
    let mut buffer = [0_u8; READ_CHUNK_BYTES];
    loop {
        match reader.read(&mut buffer).await {
            Ok(0) => return summary,
            Ok(bytes) => {
                summary.bytes_discarded = summary.bytes_discarded.saturating_add(bytes as u64);
            }
            Err(_) => {
                summary.read_failed = true;
                return summary;
            }
        }
    }
}
