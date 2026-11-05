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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::task::Wake;

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    struct Capture {
        max_chunk: usize,
        pending_once: bool,
        forwarded: Vec<u8>,
    }

    impl Capture {
        fn new(max_chunk: usize) -> Self {
            Self {
                max_chunk,
                pending_once: false,
                forwarded: Vec::new(),
            }
        }

        fn poll_pending_once(&mut self, cx: &Context<'_>) -> bool {
            if self.pending_once {
                self.pending_once = false;
                cx.waker().wake_by_ref();
                true
            } else {
                false
            }
        }
    }

    impl AsyncWrite for Capture {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.poll_pending_once(cx) {
                return Poll::Pending;
            }
            let n = buf.len().min(this.max_chunk);
            this.forwarded.extend_from_slice(&buf[..n]);
            Poll::Ready(Ok(n))
        }

        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.poll_pending_once(cx) {
                return Poll::Pending;
            }
            let requested = bufs.iter().map(|buf| buf.len()).sum::<usize>();
            let n = requested.min(this.max_chunk);
            let mut remaining = n;
            for buf in bufs {
                let count = buf.len().min(remaining);
                this.forwarded.extend_from_slice(&buf[..count]);
                remaining -= count;
                if remaining == 0 {
                    break;
                }
            }
            Poll::Ready(Ok(n))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_once<W: AsyncWrite + Unpin>(
        w: &mut BoundedLineWriter<W>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let waker = std::task::Waker::from(Arc::new(NoopWaker));
        let mut cx = Context::from_waker(&waker);
        Pin::new(w).poll_write(&mut cx, buf)
    }

    #[test]
    fn partial_write_counts_only_actual_bytes() {
        let mut w = BoundedLineWriter::new(Capture::new(2), 4);
        assert!(matches!(poll_once(&mut w, b"abc\n"), Poll::Ready(Ok(2))));
        assert_eq!(w.inner.forwarded.as_slice(), b"ab");
        assert_eq!(w.line, 2);
        assert!(matches!(poll_once(&mut w, b"c\n"), Poll::Ready(Ok(2))));
        assert_eq!(w.inner.forwarded.as_slice(), b"abc\n");
        assert_eq!(w.line, 0);
    }

    #[test]
    fn pending_once_then_accepts_same_frame() {
        let mut inner = Capture::new(3);
        inner.pending_once = true;
        let mut w = BoundedLineWriter::new(inner, 4);
        assert!(matches!(poll_once(&mut w, b"abc"), Poll::Pending));
        assert!(w.inner.forwarded.is_empty());
        assert!(matches!(poll_once(&mut w, b"abc"), Poll::Ready(Ok(3))));
        assert_eq!(w.inner.forwarded.as_slice(), b"abc");
        assert_eq!(w.line, 3);
    }

    #[test]
    fn vectored_partial_counts_actual_bytes_and_oversize_is_not_forwarded() {
        let mut w = BoundedLineWriter::new(Capture::new(3), 4);
        let waker = std::task::Waker::from(Arc::new(NoopWaker));
        let mut cx = Context::from_waker(&waker);
        let slices = [IoSlice::new(b"ab"), IoSlice::new(b"c\n")];
        assert!(matches!(
            Pin::new(&mut w).poll_write_vectored(&mut cx, &slices),
            Poll::Ready(Ok(3))
        ));
        assert_eq!(w.inner.forwarded.as_slice(), b"abc");
        assert_eq!(w.line, 3);
        let newline = [IoSlice::new(b"\n")];
        assert!(matches!(
            Pin::new(&mut w).poll_write_vectored(&mut cx, &newline),
            Poll::Ready(Ok(1))
        ));
        assert_eq!(w.inner.forwarded.as_slice(), b"abc\n");
        assert_eq!(w.line, 0);

        let mut oversized = BoundedLineWriter::new(Capture::new(5), 4);
        let slices = [IoSlice::new(b"abc"), IoSlice::new(b"de")];
        let error = Pin::new(&mut oversized).poll_write_vectored(&mut cx, &slices);
        assert!(matches!(
            error,
            Poll::Ready(Err(ref error)) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert!(oversized.inner.forwarded.is_empty());
        assert_eq!(oversized.line, 0);
    }

    #[test]
    fn newline_resets_and_boundary_is_enforced() {
        let mut w = BoundedLineWriter::new(Capture::new(16), 4);
        assert!(matches!(poll_once(&mut w, b"abc\n"), Poll::Ready(Ok(4))));
        assert_eq!(w.inner.forwarded.as_slice(), b"abc\n");
        assert_eq!(w.line, 0);
        assert!(matches!(poll_once(&mut w, b"abcd"), Poll::Ready(Ok(4))));
        assert_eq!(w.inner.forwarded.as_slice(), b"abc\nabcd");
        assert_eq!(w.line, 4);
        assert!(matches!(poll_once(&mut w, b"e"), Poll::Ready(Err(_))));
        assert_eq!(w.inner.forwarded.as_slice(), b"abc\nabcd");
    }
}
