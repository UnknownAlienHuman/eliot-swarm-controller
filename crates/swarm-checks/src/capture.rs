pub(super) use swarm_process::{
    CapturePair, CaptureReceipt, CaptureStream, PollableRead, create_output,
};

#[cfg(test)]
pub(super) use swarm_process::PipeRead;

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
}
