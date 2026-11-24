//! Translate the Windows launcher's cadence and active channel.

use std::{
    fs::File,
    io::Read,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use eliot_plugin::install::LauncherUpdate;

use super::LAUNCHER_DIR;

const INTERVAL_ENV: &str = "MUSE_UPDATE_INTERVAL_SECONDS";
const DEFAULT_INTERVAL_SECS: u64 = 3600;

pub(super) fn due_launcher_update() -> Option<LauncherUpdate> {
    let root = Path::new(LAUNCHER_DIR);
    let program = root.join("muse.cmd");
    if !program.is_file() {
        return None;
    }
    let interval = std::env::var(INTERVAL_ENV)
        .ok()
        .and_then(|value| parse_seconds(&value))
        .unwrap_or(DEFAULT_INTERVAL_SECS);
    let checked_at = read_marker(&root.join(".muse-update-checked-at"))
        .as_deref()
        .and_then(parse_seconds);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    if checked_at.is_some_and(|checked_at| {
        now.as_secs()
            .checked_sub(checked_at)
            .is_none_or(|age| age < interval)
    }) {
        return None;
    }
    let mut environment = vec![
        (INTERVAL_ENV, interval.to_string()),
        ("MUSE_LOGIN", "0".to_owned()),
    ];
    if let Some(channel) = read_marker(&root.join(".muse-channel"))
        .filter(|channel| matches!(channel.as_str(), "muse-stable" | "muse-canary"))
    {
        environment.push(("MUSE_CHANNEL", channel));
    }
    Some(LauncherUpdate {
        program,
        args: &["--version"],
        environment,
    })
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
