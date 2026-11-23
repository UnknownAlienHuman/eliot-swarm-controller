//! Swarm's own post-update harness check (#932 item 3).
//!
//! After a vendor harness update, Swarm runs one short scenario for that
//! harness and installed version on a daemon-owned isolated Agent: launch, a
//! turn with a tool call, steer, stop, resume. The scenario uses the cheapest
//! configured model for the harness, runs at most once per harness and
//! version inside a hard time limit, and never touches production Agents,
//! worktrees or the Owner's harness sessions. A new installed version is
//! always accepted and marked "not checked"; it is never refused. Only a
//! failed check removes the harness from automatic routing until a later
//! check passes; explicit Owner launches still work.

/// Ordered scenario steps. The check records the first step that fails with
/// the vendor's error. `select_profile` covers choosing the cheapest
/// configured model before any launch; it fails when no profile exists.
pub const CHECK_STEPS: &[&str] = &[
    "select_profile",
    "launch",
    "turn_with_tool_call",
    "steer",
    "stop",
    "resume",
];

/// Stored check outcomes in `harness_checks.outcome`.
pub const OUTCOME_NOT_CHECKED: &str = "not_checked";
pub const OUTCOME_PASSED: &str = "passed";
pub const OUTCOME_FAILED: &str = "failed";

/// Stored reasons for `not_checked` in `harness_checks.not_checked_reason`.
pub const NOT_CHECKED_NEW_VERSION: &str = "new_version";
pub const NOT_CHECKED_QUOTA: &str = "quota";

/// Snapshot marker naming the daemon-owned check Agent's harness and
/// version, mirroring the provider-probe launch argument.
pub const HARNESS_CHECK_LAUNCH_ARG: &str = "_harness_check";

/// Hard wall-clock budget for one harness check run, in seconds.
pub const HARNESS_CHECK_TIMEOUT_SECS: u64 = 600;

/// At most one harness check runs at a time: checks spend real model quota,
/// so they never run in parallel.
pub const HARNESS_CHECK_BATCH_LIMIT: usize = 1;

/// How often the daemon looks for installed versions with a due check, in
/// seconds. Version changes are rare; the tick only runs read-only `--version`
/// probes until a check is due.
pub const HARNESS_CHECK_TICK_SECS: u64 = 60;

/// Stale-claim horizon in milliseconds: a live run always finishes inside
/// the check timeout, so an older claim belongs to a dead daemon.
pub const HARNESS_CHECK_STALE_ATTEMPT_MS: i64 = (HARNESS_CHECK_TIMEOUT_SECS as i64 + 300) * 1000;

/// Cooldown before a quota-deferred check may run again, in seconds.
pub const HARNESS_CHECK_QUOTA_RETRY_SECS: i64 = 900;

/// Prompt for the scenario's tool-call turn: read-only, no file writes.
pub const HARNESS_CHECK_TOOL_PROMPT: &str = "Use one harness tool to show the current working directory and list its entries, then reply with exactly CHECK-OK. Do not modify files.";

/// Follow-up text for the scenario's steer step.
pub const HARNESS_CHECK_STEER_TEXT: &str = "Steer check: acknowledge with exactly STEER-OK.";

/// Follow-up text for the scenario's resume step.
pub const HARNESS_CHECK_RESUME_TEXT: &str = "Resume check: reply with exactly RESUME-OK.";

/// Maximum stored installed/checked version length.
pub const MAX_HARNESS_CHECK_VERSION_BYTES: usize = 256;

/// Maximum stored vendor failure text length.
pub const MAX_HARNESS_CHECK_ERROR_BYTES: usize = 1024;

/// Cheapest-model rank for one configured model id: lower runs first.
/// Explicit free tiers sort before cheap named families (`haiku`, `mini`,
/// `flash`, `nano`), which sort before anything unrecognized; known
/// flagship families (`sonnet`, `opus`, `pro`, `max`) sort last. Ties break
/// on shorter model ids. This is a documented heuristic, not a price feed:
///
/// the stand's configured profiles are the only candidates, so the cheapest
/// configured model is the lowest-ranked one.
pub fn cheapest_model_rank(model: &str) -> (u8, usize) {
    let folded = model.trim().to_ascii_lowercase();
    let tier = if folded.contains("free") {
        0
    } else if folded.contains("haiku") {
        1
    } else if folded.contains("mini") {
        2
    } else if folded.contains("flash") || folded.contains("nano") {
        3
    } else if folded.contains("sonnet")
        || folded.contains("opus")
        || folded.contains("pro")
        || folded.contains("max")
    {
        6
    } else {
        4
    };
    (tier, folded.len())
}

/// True for a terminal per-version outcome: the version must not run again.
pub fn is_terminal_outcome(outcome: &str) -> bool {
    matches!(outcome, OUTCOME_PASSED | OUTCOME_FAILED)
}

/// True for an error code that reports provider quota rather than a harness
/// failure: the check stays "not checked" and runs again after the hold ends.
pub fn is_quota_refusal(code: &str) -> bool {
    matches!(
        code,
        "provider_held" | "quota_exhausted" | "quota_wait" | "rate_limited"
    )
}
