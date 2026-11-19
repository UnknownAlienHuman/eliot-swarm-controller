use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;

fn frame_too_large() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "frame_too_large")
}

fn checked_line(mut line: usize, max: usize, bytes: &[u8]) -> io::Result<usize> {
    for &b in bytes {
        line = line.checked_add(1).ok_or_else(frame_too_large)?;
        if line > max {
            return Err(frame_too_large());
        }
        if b == b'\n' {
            line = 0;
        }
    }
    Ok(line)
}

fn advance(line: usize, bytes: &[u8]) -> usize {
    let mut line = line;
    for &b in bytes {
        line += 1;
        if b == b'\n' {
            line = 0;
        }
    }
    line
}

/// Rejects bytes that would push the current outgoing line past `max`.
///
/// `max` includes the trailing newline. Only bytes the inner writer
/// reports as written are counted, and a chunk is validated before any of
/// it is forwarded.
pub struct BoundedLineWriter<W> {
    inner: W,
    max: usize,
    line: usize,
}

impl<W: AsyncWrite + Unpin> BoundedLineWriter<W> {
    pub fn new(inner: W, max: usize) -> Self {
        Self {
            inner,
            max,
            line: 0,
        }
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for BoundedLineWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        checked_line(this.line, this.max, buf)?;
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = result {
            this.line = advance(this.line, &buf[..n]);
        }
        result
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let mut line = this.line;
        for buf in bufs {
            line = checked_line(line, this.max, buf)?;
        }
        let result = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        if let Poll::Ready(Ok(mut n)) = result {
            let mut line = this.line;
            for buf in bufs {
                if n == 0 {
                    break;
                }
                let take = n.min(buf.len());
                line = advance(line, &buf[..take]);
                n -= take;
            }
            this.line = line;
        }
        result
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner).poll_shutdown(cx)
    }
}
