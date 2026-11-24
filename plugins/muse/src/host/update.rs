//! The vendor launcher owns installation updates. Owned MSP hosts always
//! use the native executable already resolved for their generation.

use std::{
    fs::{File, OpenOptions},
    io::Read,
    path::Path,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use eliot_plugin::{debug, native_lifecycle::kill_and_reap};
use tokio::time::timeout;

use crate::install::LAUNCHER_DIR;

const INTERVAL_ENV: &str = "MUSE_UPDATE_INTERVAL_SECONDS";
const DEFAULT_INTERVAL_SECS: u64 = 3600;
const LAUNCHER_TIMEOUT: Duration = Duration::from_secs(15);

pub(super) fn schedule(instance_dir: &str, agent_id: &str, command_id: &str) {
    let Some(root) = Path::new(instance_dir).parent() else {
        return;
    };
    let lock_path = root.join(".muse-launcher-check.lock");
    let agent_id = agent_id.to_owned();
    let command_id = command_id.to_owned();
    let log = move |code, fields: &[(&str, u64)]| {
        debug::record(
            code,
            &[
                ("agent_id", agent_id.as_str()),
                ("command_id", command_id.as_str()),
            ],
            fields,
        );
    };
    let root = Path::new(LAUNCHER_DIR);
    let launcher = root.join("muse.cmd");
    if !launcher.is_file() {
        log("plugin.muse.launcher_check.missing", &[]);
        return;
    }
    let interval = std::env::var(INTERVAL_ENV)
        .ok()
        .and_then(|value| parse_seconds(&value))
        .unwrap_or(DEFAULT_INTERVAL_SECS);
    let Ok(lock) = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
    else {
        log("plugin.muse.launcher_check.lock_unavailable", &[]);
        return;
    };
    // Serialize the due check across this daemon's Agent plugins. Hold the
    // lock until the launcher has had a chance to write its own timestamp.
    if lock.try_lock().is_err() {
        log("plugin.muse.launcher_check.lock_busy", &[]);
        return;
    }
    let checked_at = read_marker(&root.join(".muse-update-checked-at"))
        .as_deref()
        .and_then(parse_seconds);
    let Ok(now) = SystemTime::now().duration_since(UNIX_EPOCH) else {
        log("plugin.muse.launcher_check.clock_unavailable", &[]);
        return;
    };
    if checked_at.is_some_and(|checked_at| {
        now.as_secs()
            .checked_sub(checked_at)
            .is_none_or(|age| age < interval)
    }) {
        log(
            "plugin.muse.launcher_check.fresh",
            &[("interval_secs", interval)],
        );
        return;
    }
    let channel = read_marker(&root.join(".muse-channel"));
    // This child keeps the plugin's OS process custody. Its output is
    // irrelevant to the session and the vendor worker inherits null I/O.
    #[expect(
        clippy::disallowed_methods,
        reason = "Owns one bounded vendor launcher check inside the Muse plugin."
    )]
    #[expect(
        clippy::disallowed_types,
        reason = "The launcher is a one-shot native child, separate from MSP."
    )]
    let mut command = tokio::process::Command::new(launcher);
    command
        .arg("--version")
        .current_dir(root)
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
        .env(INTERVAL_ENV, interval.to_string())
        .env("MUSE_LOGIN", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .creation_flags(0x0800_0000) // CREATE_NO_WINDOW
        .kill_on_drop(true);
    if let Some(channel) = channel
        .as_deref()
        .filter(|channel| matches!(*channel, "muse-stable" | "muse-canary"))
    {
        command.env("MUSE_CHANNEL", channel);
    }
    let Ok(mut child) = command.spawn() else {
        log("plugin.muse.launcher_check.spawn_failed", &[]);
        return;
    };
    log(
        "plugin.muse.launcher_check.started",
        &[
            ("pid", u64::from(child.id().unwrap_or_default())),
            ("interval_secs", interval),
        ],
    );
    tokio::spawn(async move {
        let _lock = lock;
        match timeout(LAUNCHER_TIMEOUT, child.wait()).await {
            Ok(Ok(status)) => log(
                "plugin.muse.launcher_check.exited",
                &[("success", u64::from(status.success()))],
            ),
            _ => {
                let reaped = kill_and_reap(&mut child).await;
                log(
                    "plugin.muse.launcher_check.wait_unconfirmed",
                    &[("reaped", u64::from(reaped))],
                );
            }
        }
    });
}

fn read_marker(path: &Path) -> Option<String> {
    let mut value = String::new();
    File::open(path)
        .ok()?
        .take(64)
        .read_to_string(&mut value)
        .ok()?;
    Some(value.trim_start_matches('\u{feff}').trim().to_owned())
}

fn parse_seconds(value: &str) -> Option<u64> {
    if !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    // The installed launcher parses both fields as signed 64-bit integers.
    u64::try_from(value.parse::<i64>().ok()?).ok()
}
