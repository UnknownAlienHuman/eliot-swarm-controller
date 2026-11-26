//! Dispatch a daemon-claimed sign-in in the Owner's desktop session.

use eliot_core::api::{
    HarnessCapabilities,
    auth_login::{
        AuthConsoleIdentity, AuthDesktopClaimResponse, AuthDesktopReportRequest,
        AuthDesktopReportResponse, AuthDesktopState,
    },
    harness::{NativeAuthCheck, NativeAuthState},
};
use eliot_daemon::{VendorLoginConsole, prepare_vendor_login_console};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::io::AsyncReadExt;

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const PROBE_INTERVAL: Duration = Duration::from_secs(10);
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_STATUS_BYTES: u64 = 64 * 1024;

/// No daemon answers (the window's daemon link is starting it, or shows
/// the daemon screen): the poll asks nothing meanwhile.
static DAEMON_DOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// The window's daemon link says whether a daemon answers.
pub(crate) fn set_daemon_down(down: bool) {
    DAEMON_DOWN.store(down, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn start() {
    if std::thread::Builder::new()
        .name("swarm-vendor-sign-in".to_owned())
        .spawn(|| crate::client::block_on(run()))
        .is_err()
    {
        crate::startup::note("vendor sign-in worker could not start");
    }
}

struct Attempt {
    data_dir: PathBuf,
    harness: String,
    revision: String,
    attempt: String,
    console: Option<VendorLoginConsole>,
    open_reported: bool,
}

impl Attempt {
    async fn advance(&mut self) -> bool {
        let state = match &self.console {
            None => AuthDesktopState::Failed,
            Some(console) if console.exited() => AuthDesktopState::Closed,
            Some(_) if self.open_reported => return false,
            Some(_) => AuthDesktopState::Open,
        };
        if report(
            &self.data_dir,
            &self.harness,
            &self.revision,
            Some(&self.attempt),
            state,
            None,
        )
        .await
        .is_err()
        {
            return false;
        }
        if matches!(state, AuthDesktopState::Open) {
            self.open_reported = true;
            return false;
        }
        true
    }
}

async fn run() {
    let mut interval = tokio::time::interval(POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut attempts = Vec::<Attempt>::new();
    let mut next_probe = HashMap::<(String, String), Instant>::new();
    let mut previous_data_dir = None;
    // Failed polls in a row (no daemon answering): the next waits longer.
    let mut pace = crate::live::Pace::default();
    // The data directory whose daemon refused this desktop (`not_allowed`):
    // a definite answer, so that daemon is asked no more; a new data
    // directory or a daemon that went down and came back is asked again.
    let mut refused: Option<PathBuf> = None;
    loop {
        interval.tick().await;
        let mut index = 0;
        while index < attempts.len() {
            if attempts[index].advance().await {
                attempts.swap_remove(index);
            } else {
                index += 1;
            }
        }
        if DAEMON_DOWN.load(std::sync::atomic::Ordering::Relaxed) {
            // The daemon that refused may come back as another release
            // with other rules: ask it again once it answers.
            refused = None;
            continue;
        }
        let Ok(data_dir) = crate::client::data_dir() else {
            continue;
        };
        if previous_data_dir.as_ref() != Some(&data_dir) {
            next_probe.clear();
            refused = None;
            previous_data_dir = Some(data_dir.clone());
        }
        if refused.as_ref() == Some(&data_dir) {
            continue;
        }
        let pending = match crate::client::auth_desktop_poll(&data_dir).await {
            Ok(pending) => pending,
            // This daemon does not let this desktop run native sign-ins: a
            // definite refusal, never retried; quietly.
            Err(error) if error.code == "not_allowed" => {
                refused = Some(data_dir);
                continue;
            }
            // A daemon that is down or unavailable is asked again, later.
            Err(_) => {
                let poll_ms = u64::try_from(POLL_INTERVAL.as_millis()).unwrap_or(u64::MAX);
                interval.reset_after(Duration::from_millis(pace.fail(poll_ms)));
                continue;
            }
        };
        pace.reset();
        next_probe.retain(|(harness, revision), _| {
            pending
                .pending
                .iter()
                .any(|work| &work.harness == harness && &work.revision == revision)
        });
        for work in pending.pending {
            let Some(capability) = HarnessCapabilities::of(&work.harness).native_auth else {
                continue;
            };
            if work.probe
                && let Some(check) = capability.signed_in_check
            {
                let key = (work.harness.clone(), work.revision.clone());
                if next_probe.get(&key).is_some_and(|at| Instant::now() < *at) {
                    continue;
                }
                let state = match probe(check).await {
                    NativeAuthState::SignedIn => Some(AuthDesktopState::SignedIn),
                    NativeAuthState::SignedOut => Some(AuthDesktopState::SignedOut),
                    NativeAuthState::Unknown => None,
                };
                next_probe.insert(key, Instant::now() + PROBE_INTERVAL);
                if let Some(state) = state {
                    let reported =
                        report(&data_dir, &work.harness, &work.revision, None, state, None).await;
                    if reported.is_err() || matches!(state, AuthDesktopState::SignedIn) {
                        continue;
                    }
                }
            }
            let Some(command) = capability.login.filter(|_| work.can_claim) else {
                continue;
            };
            let claimed =
                crate::client::auth_desktop_claim(&data_dir, &work.harness, &work.revision).await;
            if let Ok(AuthDesktopClaimResponse {
                attempt: Some(attempt),
                start_by_ms: Some(start_by_ms),
            }) = claimed
            {
                let console =
                    match prepare_vendor_login_console(command, &work.revision, start_by_ms) {
                        Ok(prepared) => {
                            let accepted = report_prepared(
                                &data_dir,
                                &work.harness,
                                &work.revision,
                                &attempt,
                                prepared.identity(),
                            )
                            .await;
                            if !accepted {
                                continue;
                            }
                            match prepared.start() {
                                Ok(console) => Some(console),
                                Err(error) if error.code == "unknown" => continue,
                                Err(_) => None,
                            }
                        }
                        Err(error) if matches!(error.code.as_str(), "busy" | "stale_item") => {
                            continue;
                        }
                        Err(_) => None,
                    };
                let mut attempt = Attempt {
                    data_dir: data_dir.clone(),
                    harness: work.harness,
                    revision: work.revision,
                    attempt,
                    console,
                    open_reported: false,
                };
                if !attempt.advance().await {
                    attempts.push(attempt);
                }
            }
        }
    }
}

/// Read back the same receipt while the caller still owns the suspended console.
async fn report_prepared(
    data_dir: &Path,
    harness: &str,
    revision: &str,
    attempt: &str,
    console: AuthConsoleIdentity,
) -> bool {
    for _ in 0..3 {
        let receipt = tokio::time::timeout(
            Duration::from_secs(2),
            report(
                data_dir,
                harness,
                revision,
                Some(attempt),
                AuthDesktopState::Prepared,
                Some(console),
            ),
        )
        .await;
        match receipt {
            Ok(Ok(receipt)) => return receipt.accepted,
            Ok(Err(error)) if !matches!(error.code.as_str(), "unknown" | "unavailable") => {
                return false;
            }
            _ => {}
        }
    }
    false
}

async fn report(
    data_dir: &Path,
    harness: &str,
    revision: &str,
    attempt: Option<&str>,
    state: AuthDesktopState,
    console: Option<AuthConsoleIdentity>,
) -> eliot_core::Result<AuthDesktopReportResponse> {
    crate::client::auth_desktop_report(
        data_dir,
        &AuthDesktopReportRequest {
            harness: harness.to_owned(),
            revision: revision.to_owned(),
            attempt: attempt.map(str::to_owned),
            state,
            console,
        },
    )
    .await
}

async fn probe(check: NativeAuthCheck) -> NativeAuthState {
    tokio::time::timeout(PROBE_TIMEOUT, probe_command(check))
        .await
        .ok()
        .flatten()
        .unwrap_or(NativeAuthState::Unknown)
}

#[expect(
    clippy::disallowed_types,
    reason = "This function owns the bounded native auth status process."
)]
async fn probe_command(check: NativeAuthCheck) -> Option<NativeAuthState> {
    let (exe, args) = (check.command.resolve)().filter(|(exe, _)| exe.is_absolute())?;
    #[expect(
        clippy::disallowed_methods,
        reason = "Construct the adapter's read-only status command."
    )]
    let mut command = tokio::process::Command::new(exe);
    let mut child = command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .creation_flags(0x0800_0000) // CREATE_NO_WINDOW
        .spawn()
        .ok()?;
    let mut stdout = child.stdout.take()?.take(MAX_STATUS_BYTES + 1);
    let mut stderr = child.stderr.take()?.take(MAX_STATUS_BYTES + 1);
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let (status, _, _) = tokio::try_join!(
        child.wait(),
        stdout.read_to_end(&mut out),
        stderr.read_to_end(&mut err)
    )
    .ok()?;
    if out.len() as u64 > MAX_STATUS_BYTES || err.len() as u64 > MAX_STATUS_BYTES {
        return None;
    }
    Some((check.classify)(status.code(), &out, &err))
}
