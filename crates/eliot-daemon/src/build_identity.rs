//! Process-lifetime identity for the exact executable serving this daemon.

use eliot_core::api::DaemonBuildIdentity;
use std::{path::Path, sync::OnceLock};

static BUILD_IDENTITY: OnceLock<DaemonBuildIdentity> = OnceLock::new();

pub(crate) fn current() -> &'static DaemonBuildIdentity {
    BUILD_IDENTITY.get_or_init(|| {
        let executable = std::env::current_exe().ok();
        let executable_directory = executable
            .as_deref()
            .and_then(Path::parent)
            .map(|path| eliot_core::display_path(&path.to_string_lossy()));
        // Read the running image even if an operator has replaced its path.
        #[cfg(target_os = "linux")]
        let executable_sha256 = crate::operator::binary_sha256(Path::new("/proc/self/exe")).ok();
        #[cfg(not(target_os = "linux"))]
        let executable_sha256 = executable
            .as_deref()
            .and_then(|path| crate::operator::binary_sha256(path).ok());
        DaemonBuildIdentity {
            version: env!("ELIOT_BUILD_VERSION").to_owned(),
            executable_directory,
            git_sha: option_env!("ELIOT_BUILD_GIT_SHA")
                .filter(|sha| !sha.is_empty())
                .map(str::to_owned),
            executable_sha256,
        }
    })
}
