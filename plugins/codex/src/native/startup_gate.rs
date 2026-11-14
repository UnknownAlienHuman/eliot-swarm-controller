//! Cross-process serialization for Codex metadata-reader startup.

use eliot_core::{Error, Result};
use std::{
    fs::{File, OpenOptions, TryLockError},
    path::Path,
    time::Duration,
};
use tokio::time::{Instant, sleep_until};

const LOCK_FILE: &str = "eliot-codex-state-init-v1.lock";
const RETRY_INTERVAL: Duration = Duration::from_millis(10);

/// Holds the conservative temp-namespace lock until the metadata startup is complete.
pub(crate) struct StartupGate {
    _file: File,
}

impl StartupGate {
    pub(crate) async fn acquire(deadline: Instant) -> Result<Self> {
        let path = std::env::temp_dir().join(LOCK_FILE);
        Self::acquire_at(&path, deadline).await
    }

    async fn acquire_at(path: &Path, deadline: Instant) -> Result<Self> {
        loop {
            if Instant::now() >= deadline {
                return Err(startup_gate_error());
            }

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(|_| startup_gate_error())?;
            if Instant::now() >= deadline {
                return Err(startup_gate_error());
            }
            match file.try_lock() {
                Ok(()) if Instant::now() < deadline => return Ok(Self { _file: file }),
                Ok(()) => return Err(startup_gate_error()),
                Err(TryLockError::WouldBlock) => drop(file),
                Err(TryLockError::Error(_)) => return Err(startup_gate_error()),
            }

            sleep_until((Instant::now() + RETRY_INTERVAL).min(deadline)).await;
        }
    }
}

fn startup_gate_error() -> Error {
    Error::new(
        "native_readback",
        "Codex metadata startup gate could not be acquired",
    )
}

#[cfg(test)]
mod tests {
    use super::StartupGate;
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    };
    use tokio::time::{Instant, timeout};

    static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn lock_path() -> (Self, PathBuf) {
            let directory = std::env::temp_dir().join(format!(
                "eliot-codex-startup-gate-{}-{}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed),
            ));
            fs::create_dir(&directory).expect("create unique test directory");
            let path = directory.join("startup.lock");
            (Self(directory), path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn independent_handles_serialize_and_release_the_gate() {
        let (_directory, path) = TestDirectory::lock_path();
        let first = StartupGate::acquire_at(&path, Instant::now() + Duration::from_secs(1))
            .await
            .expect("first handle acquires the gate");
        let second = StartupGate::acquire_at(&path, Instant::now() + Duration::from_secs(1));
        tokio::pin!(second);
        assert!(
            timeout(Duration::from_millis(50), &mut second)
                .await
                .is_err()
        );

        drop(first);
        let second = timeout(Duration::from_secs(1), second)
            .await
            .expect("second handle proceeds after release")
            .expect("second handle acquires the released gate");
        drop(second);
    }

    #[tokio::test]
    async fn held_gate_expires_without_truncating_or_removing_the_lock_file() {
        let (_directory, path) = TestDirectory::lock_path();
        fs::write(&path, b"persistent lock identity").unwrap();
        let first = StartupGate::acquire_at(&path, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let error = match StartupGate::acquire_at(&path, Instant::now() + Duration::from_millis(30))
            .await
        {
            Ok(_) => panic!("held gate must expire"),
            Err(error) => error,
        };
        assert_eq!(error.code, "native_readback");
        assert!(!error.message.contains(path.to_str().unwrap()));
        drop(first);
        assert_eq!(fs::read(&path).unwrap(), b"persistent lock identity");
        let _next = StartupGate::acquire_at(&path, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancelling_a_waiter_leaves_no_lock_behind() {
        let (_directory, path) = TestDirectory::lock_path();
        let first = StartupGate::acquire_at(&path, Instant::now() + Duration::from_secs(1))
            .await
            .expect("first handle acquires the gate");
        let waiter_path = path.clone();
        let waiter = tokio::spawn(async move {
            StartupGate::acquire_at(&waiter_path, Instant::now() + Duration::from_secs(5)).await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!waiter.is_finished(), "waiter remains behind the held gate");
        waiter.abort();
        let _ = waiter.await;

        drop(first);
        let next = timeout(
            Duration::from_secs(1),
            StartupGate::acquire_at(&path, Instant::now() + Duration::from_secs(1)),
        )
        .await
        .expect("fresh waiter proceeds after cancelled waiter")
        .expect("cancelled waiter retained no lock");
        drop(next);
    }
}
