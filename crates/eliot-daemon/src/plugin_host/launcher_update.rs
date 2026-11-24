//! A best-effort machine-level vendor launcher check. Starting or stopping
//! an Agent never waits for this task or owns its process.

use std::{process::Stdio, time::Duration};

use eliot_core::SecretPolicy;
use eliot_plugin::{install::discovery_for, native_lifecycle::kill_and_reap};
use windows_sys::Win32::System::Threading::CREATE_NO_WINDOW;

use crate::{
    logging,
    platform::windows::{try_acquire_launcher_check, with_job_breakaway},
};

const LAUNCHER_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) fn schedule(harness: &str, agent_id: &str, command_id: &str) {
    let Ok(adapter) = discovery_for(harness) else {
        return;
    };
    let Some(due) = adapter.launcher_update else {
        return;
    };
    let (agent_id, command_id) = (agent_id.to_owned(), command_id.to_owned());
    tokio::spawn(async move {
        let policy = SecretPolicy::new(std::iter::empty::<&str>());
        let log = |code, fields: &[(&str, u64)]| {
            logging::record_debug(
                &policy,
                code,
                &[("agent", &agent_id), ("command", &command_id)],
                fields,
            );
        };
        let Ok(Some(lock)) = try_acquire_launcher_check(adapter.name) else {
            log("daemon.launcher_check.lock_unavailable", &[]);
            return;
        };
        let Ok(Some(launch)) = tokio::task::spawn_blocking(due).await else {
            log("daemon.launcher_check.skipped", &[]);
            return;
        };
        let Some(directory) = launch.program.parent() else {
            log("daemon.launcher_check.skipped", &[]);
            return;
        };
        // The daemon is the direct owner of the version-only launcher.
        // Null I/O leaves its vendor background worker independent even
        // after the short-lived launcher exits or is reaped.
        #[expect(
            clippy::disallowed_methods,
            reason = "The daemon owns one bounded background vendor launcher check."
        )]
        #[expect(
            clippy::disallowed_types,
            reason = "The launcher check is a short-lived process outside Agent custody."
        )]
        let mut command = tokio::process::Command::new(&launch.program);
        command
            .args(launch.args)
            .current_dir(directory)
            .env_clear()
            .envs(std::env::vars_os().filter(|(name, _)| {
                [
                    "SystemRoot",
                    "SystemDrive",
                    "ProgramFiles",
                    "ProgramData",
                    "USERPROFILE",
                    "LOCALAPPDATA",
                    "PATH",
                    "TEMP",
                    "PATHEXT",
                ]
                .iter()
                .any(|base| name.eq_ignore_ascii_case(base))
            }))
            .envs(launch.environment)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let Ok(mut child) =
            with_job_breakaway(|flags| command.creation_flags(CREATE_NO_WINDOW | flags).spawn())
        else {
            log("daemon.launcher_check.spawn_failed", &[]);
            return;
        };
        log(
            "daemon.launcher_check.started",
            &[("pid", u64::from(child.id().unwrap_or_default()))],
        );
        match tokio::time::timeout(LAUNCHER_TIMEOUT, child.wait()).await {
            Ok(Ok(status)) => log(
                "daemon.launcher_check.exited",
                &[("success", u64::from(status.success()))],
            ),
            _ => {
                let reaped = kill_and_reap(&mut child).await;
                log(
                    "daemon.launcher_check.wait_unconfirmed",
                    &[("reaped", u64::from(reaped))],
                );
            }
        }
        drop(lock);
    });
}
