use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use crate::{private_permissions, remove_private_durable, sync_parent_directory};
use sha2::{Digest, Sha256};
use swarm_contracts::{Error, Result};

/// Maximum bytes retained for either output stream.
pub const MAX_CAPTURE_BYTES_PER_STREAM: u64 = 64 * 1024 * 1024;

const READ_BUFFER_BYTES: usize = 16 * 1024;

/// Result of one nonblocking read attempt from a child pipe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipeRead {
    Data(usize),
    Pending,
    Eof,
}

/// Result of one nonblocking write attempt to a child pipe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipeWrite {
    Data(usize),
    Pending,
    Closed,
}

/// A child-pipe reader configured so each read attempt returns promptly.
pub trait PollableRead: Read {
    fn make_nonblocking(&self) -> io::Result<()>;
    fn read_available(&mut self, buffer: &mut [u8]) -> io::Result<PipeRead>;
}

/// A child-pipe writer configured so each write attempt returns promptly.
pub trait PollableWrite: Write {
    fn make_nonblocking(&self) -> io::Result<()>;
    fn write_available(&mut self, buffer: &[u8]) -> io::Result<PipeWrite>;
}

/// Immutable facts about the bytes accepted by one streaming output writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureReceipt {
    pub path: PathBuf,
    pub started: bool,
    pub bytes_written: u64,
    pub bytes_observed: u64,
    pub sha256: String,
    pub truncated: bool,
    pub capture_complete: bool,
    pub capture_error: Option<String>,
}

impl CaptureReceipt {
    /// Evidence for a stream that was created but could not be fully captured.
    pub fn incomplete(path: PathBuf, error: &str) -> Self {
        Self {
            path,
            started: true,
            bytes_written: 0,
            bytes_observed: 0,
            sha256: empty_sha256(),
            truncated: false,
            capture_complete: false,
            capture_error: Some(error.to_owned()),
        }
    }

    /// Evidence for a stream whose child command never started.
    pub fn not_started(path: PathBuf) -> Self {
        Self {
            path,
            started: false,
            bytes_written: 0,
            bytes_observed: 0,
            sha256: empty_sha256(),
            truncated: false,
            capture_complete: false,
            capture_error: Some("capture_not_started".to_owned()),
        }
    }
}

/// One synchronous owner for a pipe and its durable output file.
///
/// The execution loop polls this owner alongside process state. It retains only
/// the configured file prefix and never holds output proportional to the
/// complete child stream in memory.
pub struct CaptureStream<R: PollableRead> {
    path: PathBuf,
    file: File,
    reader: R,
    limit: u64,
    bytes_written: u64,
    bytes_observed: u64,
    truncated: bool,
    capture_complete: bool,
    reader_finished: bool,
    reader_failed: bool,
    write_failed: bool,
    capture_error: Option<String>,
    digest: Sha256,
}

impl<R: PollableRead> CaptureStream<R> {
    pub fn new(path: PathBuf, file: File, reader: R, limit: u64) -> io::Result<Self> {
        if limit == 0 || limit > MAX_CAPTURE_BYTES_PER_STREAM {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "capture limit is outside the package bound",
            ));
        }
        reader.make_nonblocking()?;
        Ok(Self {
            path,
            file,
            reader,
            limit,
            bytes_written: 0,
            bytes_observed: 0,
            truncated: false,
            capture_complete: false,
            reader_finished: false,
            reader_failed: false,
            write_failed: false,
            capture_error: None,
            digest: Sha256::new(),
        })
    }

    /// Reads and persists at most one bounded chunk, keeping process polling
    /// responsive even when a child continuously produces output.
    pub fn poll(&mut self) -> bool {
        if self.reader_finished {
            return false;
        }
        let mut buffer = [0u8; READ_BUFFER_BYTES];
        match self.reader.read_available(&mut buffer) {
            Ok(PipeRead::Pending) => false,
            Ok(PipeRead::Eof) => {
                self.capture_complete = true;
                self.reader_finished = true;
                true
            }
            Ok(PipeRead::Data(0)) => {
                self.capture_complete = true;
                self.reader_finished = true;
                true
            }
            Ok(PipeRead::Data(read)) if read <= buffer.len() => {
                self.bytes_observed = match self.bytes_observed.checked_add(read as u64) {
                    Some(total) => total,
                    None => {
                        self.record_error("capture_length_overflow");
                        u64::MAX
                    }
                };
                if self.bytes_observed > self.limit {
                    self.truncated = true;
                }
                self.write_prefix(&buffer[..read]);
                true
            }
            Ok(PipeRead::Data(_)) | Err(_) => {
                self.reader_finished = true;
                self.reader_failed = true;
                self.record_error("capture_read_failed");
                true
            }
        }
    }

    pub fn is_finished(&self) -> bool {
        self.reader_finished
    }

    pub fn truncated(&self) -> bool {
        self.truncated
    }

    pub fn reader_failed(&self) -> bool {
        self.reader_failed
    }

    pub fn finish(mut self) -> CaptureReceipt {
        if !self.reader_finished {
            self.record_error("capture_drain_timeout");
        }
        if self.file.sync_all().is_err() {
            self.record_error("capture_sync_failed");
        }
        let expected_written = self.bytes_observed.min(self.limit);
        let complete = self.capture_complete
            && self.capture_error.is_none()
            && self.bytes_written == expected_written;
        CaptureReceipt {
            path: self.path,
            started: true,
            bytes_written: self.bytes_written,
            bytes_observed: self.bytes_observed,
            sha256: format!("{:x}", self.digest.finalize()),
            truncated: self.truncated,
            capture_complete: complete,
            capture_error: self.capture_error,
        }
    }

    fn write_prefix(&mut self, bytes: &[u8]) {
        let remaining = self.limit.saturating_sub(self.bytes_written);
        let retained = usize::try_from(remaining.min(bytes.len() as u64))
            .expect("retained chunk length fits usize");
        if retained == 0 || self.write_failed {
            return;
        }

        let mut offset = 0;
        while offset < retained {
            match self.file.write(&bytes[offset..retained]) {
                Ok(0) => {
                    self.write_failed = true;
                    self.record_error("capture_write_failed");
                    break;
                }
                Ok(written) => {
                    let end = offset + written;
                    self.digest.update(&bytes[offset..end]);
                    self.bytes_written = self.bytes_written.saturating_add(written as u64);
                    offset = end;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => {
                    self.write_failed = true;
                    self.record_error("capture_write_failed");
                    break;
                }
            }
        }
    }

    fn record_error(&mut self, code: &str) {
        if self.capture_error.is_none() {
            self.capture_error = Some(code.to_owned());
        }
    }
}

/// Synchronous owner for both output pipes in one child execution.
pub struct CapturePair<O: PollableRead, E: PollableRead> {
    stdout_path: PathBuf,
    stdout: Option<CaptureStream<O>>,
    stderr_path: PathBuf,
    stderr: Option<CaptureStream<E>>,
}

impl<O: PollableRead, E: PollableRead> CapturePair<O, E> {
    pub fn new(
        stdout_path: PathBuf,
        stdout: Option<CaptureStream<O>>,
        stderr_path: PathBuf,
        stderr: Option<CaptureStream<E>>,
    ) -> Self {
        Self {
            stdout_path,
            stdout,
            stderr_path,
            stderr,
        }
    }

    /// Polls each live stream once, so neither pipe can monopolize the caller.
    pub fn poll(&mut self) -> bool {
        let stdout_progress = self.stdout.as_mut().is_some_and(CaptureStream::poll);
        let stderr_progress = self.stderr.as_mut().is_some_and(CaptureStream::poll);
        stdout_progress || stderr_progress
    }

    pub fn stdout_finished(&self) -> bool {
        self.stdout.as_ref().is_none_or(CaptureStream::is_finished)
    }

    pub fn stderr_finished(&self) -> bool {
        self.stderr.as_ref().is_none_or(CaptureStream::is_finished)
    }

    pub fn has_setup_failure(&self) -> bool {
        self.stdout.is_none() || self.stderr.is_none()
    }

    pub fn truncated(&self) -> bool {
        self.stdout.as_ref().is_some_and(CaptureStream::truncated)
            || self.stderr.as_ref().is_some_and(CaptureStream::truncated)
    }

    pub fn has_reader_failure(&self) -> bool {
        self.stdout
            .as_ref()
            .is_some_and(CaptureStream::reader_failed)
            || self
                .stderr
                .as_ref()
                .is_some_and(CaptureStream::reader_failed)
    }

    pub fn finish(self) -> (CaptureReceipt, CaptureReceipt) {
        (
            self.stdout.map_or_else(
                || CaptureReceipt::incomplete(self.stdout_path, "capture_setup_failed"),
                CaptureStream::finish,
            ),
            self.stderr.map_or_else(
                || CaptureReceipt::incomplete(self.stderr_path, "capture_setup_failed"),
                CaptureStream::finish,
            ),
        )
    }
}

/// Creates the streaming output file with the same private-file policy used by
/// the process layer, then syncs its directory entry before native execution.
pub fn create_output(path: &Path) -> Result<File> {
    let failed = || {
        Error::new(
            "PROCESS_CAPTURE_CREATE_FAILED",
            "could not create private process output",
        )
    };
    sync_parent_directory(path).map_err(|_| failed())?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path).map_err(|_| failed())?;
    if private_permissions(path, false).is_err() || sync_parent_directory(path).is_err() {
        drop(file);
        let _ = remove_private_durable(path);
        return Err(failed());
    }
    Ok(file)
}

pub fn empty_sha256() -> String {
    format!("{:x}", Sha256::digest([]))
}

#[cfg(target_os = "linux")]
impl<T: Read + std::os::fd::AsRawFd> PollableRead for T {
    fn make_nonblocking(&self) -> io::Result<()> {
        const F_GETFL: i32 = 3;
        const F_SETFL: i32 = 4;
        const O_NONBLOCK: i32 = 0x800;
        unsafe extern "C" {
            fn fcntl(fd: i32, command: i32, ...) -> i32;
        }
        // SAFETY: fcntl reads and updates flags on this live pipe descriptor.
        let flags = unsafe { fcntl(self.as_raw_fd(), F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: F_SETFL accepts the current flags plus O_NONBLOCK.
        if unsafe { fcntl(self.as_raw_fd(), F_SETFL, flags | O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn read_available(&mut self, buffer: &mut [u8]) -> io::Result<PipeRead> {
        match self.read(buffer) {
            Ok(0) => Ok(PipeRead::Eof),
            Ok(read) => Ok(PipeRead::Data(read)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(PipeRead::Pending),
            Err(error) => Err(error),
        }
    }
}

#[cfg(target_os = "linux")]
impl<T: Write + std::os::fd::AsRawFd> PollableWrite for T {
    fn make_nonblocking(&self) -> io::Result<()> {
        const F_GETFL: i32 = 3;
        const F_SETFL: i32 = 4;
        const O_NONBLOCK: i32 = 0x800;
        unsafe extern "C" {
            fn fcntl(fd: i32, command: i32, ...) -> i32;
        }
        // SAFETY: fcntl reads and updates flags on this live child-pipe descriptor.
        let flags = unsafe { fcntl(self.as_raw_fd(), F_GETFL) };
        if flags < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: F_SETFL accepts the current flags plus O_NONBLOCK.
        if unsafe { fcntl(self.as_raw_fd(), F_SETFL, flags | O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn write_available(&mut self, buffer: &[u8]) -> io::Result<PipeWrite> {
        match self.write(buffer) {
            Ok(0) => Ok(PipeWrite::Closed),
            Ok(written) => Ok(PipeWrite::Data(written)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(PipeWrite::Pending),
            Err(error) if error.kind() == io::ErrorKind::BrokenPipe => Ok(PipeWrite::Closed),
            Err(error) => Err(error),
        }
    }
}

#[cfg(windows)]
impl<T: Read + std::os::windows::io::AsRawHandle> PollableRead for T {
    fn make_nonblocking(&self) -> io::Result<()> {
        Ok(())
    }

    fn read_available(&mut self, buffer: &mut [u8]) -> io::Result<PipeRead> {
        use std::ffi::c_void;
        unsafe extern "system" {
            fn PeekNamedPipe(
                pipe: *mut c_void,
                buffer: *mut c_void,
                buffer_size: u32,
                bytes_read: *mut u32,
                total_available: *mut u32,
                bytes_left: *mut u32,
            ) -> i32;
            fn GetLastError() -> u32;
        }
        let mut available = 0u32;
        // SAFETY: the handle is a live child pipe and output points to local storage.
        let ok = unsafe {
            PeekNamedPipe(
                self.as_raw_handle().cast(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                &mut available,
                std::ptr::null_mut(),
            )
        };
        if ok == 0 {
            let code = unsafe { GetLastError() };
            if code == 109 {
                return Ok(PipeRead::Eof);
            }
            return Err(io::Error::from_raw_os_error(code as i32));
        }
        if available == 0 {
            return Ok(PipeRead::Pending);
        }
        let bound = buffer.len().min(available as usize);
        match self.read(&mut buffer[..bound]) {
            Ok(0) => Ok(PipeRead::Eof),
            Ok(read) => Ok(PipeRead::Data(read)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(PipeRead::Pending),
            Err(error) => Err(error),
        }
    }
}

#[cfg(windows)]
impl<T: Write + std::os::windows::io::AsRawHandle> PollableWrite for T {
    fn make_nonblocking(&self) -> io::Result<()> {
        use std::ffi::c_void;
        unsafe extern "system" {
            fn SetNamedPipeHandleState(
                pipe: *mut c_void,
                mode: *const u32,
                maximum_collection_count: *const u32,
                collection_timeout: *const u32,
            ) -> i32;
        }
        const PIPE_NOWAIT: u32 = 0x00000001;
        // SAFETY: the handle is the live parent end of the child stdin pipe.
        let ok = unsafe {
            SetNamedPipeHandleState(
                self.as_raw_handle().cast(),
                &PIPE_NOWAIT,
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn write_available(&mut self, buffer: &[u8]) -> io::Result<PipeWrite> {
        match self.write(buffer) {
            // PIPE_NOWAIT on a byte pipe may successfully write zero bytes
            // when its buffer is full. Only an explicit broken-pipe result
            // proves that the reader has closed its end.
            Ok(0) => Ok(PipeWrite::Pending),
            Ok(written) => Ok(PipeWrite::Data(written)),
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    || error.raw_os_error() == Some(232) =>
            {
                Ok(PipeWrite::Pending)
            }
            Err(error)
                if error.kind() == io::ErrorKind::BrokenPipe
                    || error.raw_os_error() == Some(109) =>
            {
                Ok(PipeWrite::Closed)
            }
            Err(error) => Err(error),
        }
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
impl<T: Read> PollableRead for T {
    fn make_nonblocking(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded pipe capture is unsupported on this platform",
        ))
    }

    fn read_available(&mut self, _buffer: &mut [u8]) -> io::Result<PipeRead> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded pipe capture is unsupported on this platform",
        ))
    }
}

#[cfg(not(any(target_os = "linux", windows)))]
impl<T: Write> PollableWrite for T {
    fn make_nonblocking(&self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded child input is unsupported on this platform",
        ))
    }

    fn write_available(&mut self, _buffer: &[u8]) -> io::Result<PipeWrite> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "bounded child input is unsupported on this platform",
        ))
    }
}

#[cfg(all(test, any(target_os = "linux", windows)))]
mod tests {
    use std::{
        fs,
        io::{self, Read},
        path::PathBuf,
    };

    use sha2::{Digest, Sha256};

    use super::{CaptureStream, PipeRead, PollableRead, create_output};

    struct FixtureReader {
        bytes: Vec<u8>,
        offset: usize,
        remain_pending_after_data: bool,
    }

    impl Read for FixtureReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let count = buffer
                .len()
                .min(self.bytes.len().saturating_sub(self.offset));
            buffer[..count].copy_from_slice(&self.bytes[self.offset..self.offset + count]);
            self.offset += count;
            Ok(count)
        }
    }

    impl PollableRead for FixtureReader {
        fn make_nonblocking(&self) -> io::Result<()> {
            Ok(())
        }

        fn read_available(&mut self, buffer: &mut [u8]) -> io::Result<PipeRead> {
            if self.offset < self.bytes.len() {
                let count = buffer.len().min(self.bytes.len() - self.offset);
                buffer[..count].copy_from_slice(&self.bytes[self.offset..self.offset + count]);
                self.offset += count;
                Ok(PipeRead::Data(count))
            } else if self.remain_pending_after_data {
                Ok(PipeRead::Pending)
            } else {
                Ok(PipeRead::Eof)
            }
        }
    }

    struct FixtureDirectory(PathBuf);

    impl FixtureDirectory {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("swarm-check-capture-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).expect("create capture fixture directory");
            Self(path)
        }
    }

    impl Drop for FixtureDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn streams_only_the_configured_prefix_and_hashes_the_written_bytes() {
        let directory = FixtureDirectory::new();
        let path = directory.0.join("stdout");
        let limit = 31_337_u64;
        let bytes = (0..512 * 1024)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let reader = FixtureReader {
            bytes: bytes.clone(),
            offset: 0,
            remain_pending_after_data: false,
        };
        let file = create_output(&path).expect("create private output");
        let mut capture = CaptureStream::new(path.clone(), file, reader, limit)
            .expect("initialize pollable capture");
        while !capture.is_finished() {
            capture.poll();
        }

        let result = capture.finish();
        let expected = &bytes[..limit as usize];
        assert_eq!(fs::read(&path).expect("read captured prefix"), expected);
        assert_eq!(result.bytes_observed, bytes.len() as u64);
        assert_eq!(result.bytes_written, limit);
        assert_eq!(result.sha256, format!("{:x}", Sha256::digest(expected)));
        assert!(result.truncated);
        assert!(result.capture_complete);
    }

    #[test]
    fn pending_reader_keeps_durable_partial_bytes_and_finishes_incomplete() {
        let directory = FixtureDirectory::new();
        let path = directory.0.join("stderr");
        let bytes = b"partial durable stderr".to_vec();
        let reader = FixtureReader {
            bytes: bytes.clone(),
            offset: 0,
            remain_pending_after_data: true,
        };
        let file = create_output(&path).expect("create private output");
        let mut capture = CaptureStream::new(path.clone(), file, reader, 128)
            .expect("initialize pollable capture");
        assert!(capture.poll());
        assert!(!capture.poll());
        assert!(!capture.is_finished());
        assert_eq!(fs::read(&path).expect("read durable partial bytes"), bytes);

        let result = capture.finish();
        assert_eq!(result.bytes_written, bytes.len() as u64);
        assert_eq!(result.bytes_observed, bytes.len() as u64);
        assert_eq!(result.sha256, format!("{:x}", Sha256::digest(&bytes)));
        assert!(!result.capture_complete);
        assert_eq!(
            result.capture_error.as_deref(),
            Some("capture_drain_timeout")
        );
    }

    #[cfg(windows)]
    #[test]
    fn full_nonblocking_pipe_is_pending_not_closed() {
        use std::{
            ffi::c_void,
            fs::File,
            os::windows::io::{FromRawHandle, OwnedHandle},
        };

        use super::{PipeWrite, PollableWrite};

        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn CreatePipe(
                read_pipe: *mut *mut c_void,
                write_pipe: *mut *mut c_void,
                pipe_attributes: *const c_void,
                size: u32,
            ) -> i32;
        }

        let mut read_pipe = std::ptr::null_mut();
        let mut write_pipe = std::ptr::null_mut();
        // SAFETY: both output pointers are valid local storage; null attributes
        // request non-inheritable handles owned by this fixture.
        let created =
            unsafe { CreatePipe(&mut read_pipe, &mut write_pipe, std::ptr::null(), 64 * 1024) };
        assert_ne!(created, 0, "create anonymous pipe");

        // SAFETY: CreatePipe succeeded and each returned handle is wrapped once.
        let _reader = unsafe { OwnedHandle::from_raw_handle(read_pipe) };
        // SAFETY: CreatePipe succeeded and transfers this unique write handle.
        let mut writer = unsafe { File::from_raw_handle(write_pipe) };
        PollableWrite::make_nonblocking(&writer).expect("set PIPE_NOWAIT");

        // Write on an empty slice succeeds with zero bytes; it must not signal
        // that the live pipe has closed.
        assert_eq!(
            writer.write_available(&[]).expect("poll empty write"),
            PipeWrite::Pending
        );

        let chunk = [0x5a; 16 * 1024];
        let mut written_total = 0usize;
        let mut full = false;
        while written_total < 1024 * 1024 {
            let remaining = (1024 * 1024 - written_total).min(chunk.len());
            match writer
                .write_available(&chunk[..remaining])
                .expect("poll nonblocking pipe write")
            {
                PipeWrite::Data(written) => {
                    assert!(written > 0, "a zero-byte write is pending");
                    written_total += written;
                }
                PipeWrite::Pending => {
                    full = true;
                    break;
                }
                PipeWrite::Closed => panic!("live reader pipe was reported closed"),
            }
        }

        assert!(written_total > 0, "pipe accepted no bytes");
        assert!(full, "pipe did not become full within the bounded fixture");
        assert_eq!(
            writer
                .write_available(b"x")
                .expect("poll full nonblocking pipe"),
            PipeWrite::Pending
        );
    }
}
