//! Per-user daemon startup registration for supported operating systems.
use super::support::Failure;
use std::path::Path;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(windows)]
mod windows;

pub(crate) async fn install(
    data_dir: &Path,
    json_output: bool,
    dry_run: bool,
) -> Result<u8, Failure> {
    #[cfg(windows)]
    {
        windows::install(data_dir, json_output, dry_run).await
    }

    #[cfg(target_os = "linux")]
    {
        linux::install(data_dir, json_output, dry_run).await
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = (data_dir, json_output, dry_run);
        Err(Failure::new(
            2,
            "daemon autostart is supported on Windows and Linux",
        ))
    }
}

pub(crate) async fn uninstall(
    data_dir: &Path,
    json_output: bool,
    dry_run: bool,
) -> Result<u8, Failure> {
    #[cfg(windows)]
    {
        windows::uninstall(data_dir, json_output, dry_run).await
    }

    #[cfg(target_os = "linux")]
    {
        linux::uninstall(data_dir, json_output, dry_run).await
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = (data_dir, json_output, dry_run);
        Err(Failure::new(
            2,
            "daemon autostart is supported on Windows and Linux",
        ))
    }
}
