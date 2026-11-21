//! Per-user daemon startup registration for supported operating systems.
use super::support::Failure;
use std::path::Path;

#[cfg(windows)]
mod windows;

pub(crate) async fn install(data_dir: &Path, json_output: bool) -> Result<u8, Failure> {
    #[cfg(windows)]
    {
        windows::install(data_dir, json_output).await
    }

    #[cfg(target_os = "linux")]
    {
        let _ = (data_dir, json_output);
        Err(Failure::new(
            2,
            "Swarm does not install distro services on Linux; run the supported nonroot Docker image with a Docker restart policy such as --restart unless-stopped",
        ))
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = (data_dir, json_output);
        Err(Failure::new(
            2,
            "daemon autostart is supported on Windows and the Linux Docker image only",
        ))
    }
}

pub(crate) async fn uninstall(data_dir: &Path, json_output: bool) -> Result<u8, Failure> {
    #[cfg(windows)]
    {
        windows::uninstall(data_dir, json_output).await
    }

    #[cfg(target_os = "linux")]
    {
        let _ = (data_dir, json_output);
        Err(Failure::new(
            2,
            "Swarm does not install distro services on Linux; remove the restart policy from the supported Docker container configuration",
        ))
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = (data_dir, json_output);
        Err(Failure::new(
            2,
            "daemon autostart is supported on Windows and the Linux Docker image only",
        ))
    }
}
