//! Read-only OpenCode Console usage limits through the installed native
//! operation (`donor: plugins/opencode/src/native/models.rs` for the
//! bounded child pattern: exact argv, stdin null, piped stdout with an
//! output cap, stderr discarded, kill-and-reap under a deadline, static
//! error strings).
//!
//! Vendor behavior (pinned v2.2.7, anomalyco/opencode `fedc6f2`):
//! - `packages/protocol/src/groups/usage.ts`: `experimental.usage.limits`
//!   answers `GET /api/experimental/usage/limits` with the Console budgets
//!   plus the Go allowance of the signed-in workspace, or null when it is
//!   not signed in.
//! - `packages/core/src/usage-limits.ts`: the operation reads the ACTIVE
//!   `opencode` integration connection and resolves it (refreshing an
//!   expiring OAuth credential) before deriving the workspace binding.
//!   Reading the exported bearer directly can fail after token expiry.
//! - `packages/cli/src/commands/commands.ts`: `opencode api <operation>
//!   --standalone` runs the operation against a private server, so quota
//!   custody (which has no running serve) still resolves natively.
//!
//! Cold-start honesty (pinned `packages/core/src/integration.ts`
//! `connection.resolve`): the resolver returns the stored OAuth credential
//! unchanged while the OAuth method implementation is not registered, and
//! a one-shot `--standalone` boot can serve the operation before the
//! Console provider plugin finishes registering its refresh. The adapter
//! cannot observe that registration, so its contract is deliberately
//! narrow: a successful read means the live Console calls accepted the
//! credential (both fetches run against the live service with it, so the
//! data is current as of the read); any failure — an expired-token 401, a
//! boot failure, malformed output — maps to a typed unknown
//! (`native_spawn`/`native_readback`), never to ready windows; only an
//! explicit `data: null` (no active connection) maps to unsupported.
//!
//! The projection keeps the adapter's existing window labels: Go meters map
//! to 5-hour/Weekly/Monthly windows, each Console budget contributes its
//! spend share of its dollar limit. Entries without usable numbers are
//! skipped, never zero-filled.

use std::{ffi::OsStr, process::Stdio, time::Duration};

use eliot_core::{Error, Result};
use eliot_plugin::native_lifecycle::kill_and_reap;
use serde_json::{Value, json};
use tokio::{
    io::AsyncReadExt,
    process::Command,
    time::{Instant, timeout_at},
};

/// Exact argv for the native usage-limits read: `api
/// experimental.usage.limits --standalone`. Read-only (no session, no turn,
/// no inference): it answers the installed `experimental.usage.limits`
/// operation against a private server with native credential resolution.
pub(crate) fn usage_limits_argv() -> [&'static str; 3] {
    ["api", "experimental.usage.limits", "--standalone"]
}

/// Output cap for the usage-limits read: the installed `Info` is a few KiB
/// (one workspace, three Go meters, a handful of budgets), so 512 KiB
/// leaves headroom while bounding a misbehaving child.
pub(crate) const MAX_USAGE_BYTES: usize = 512 * 1024;

/// Bound for the whole native usage-limits child: booting the private
/// server plus the native 15 s operation budget.
const USAGE_DEADLINE: Duration = Duration::from_secs(20);

/// One Console budget in the installed `UsageLimits.Info` shape: the spend
/// share of its dollar limit projects one window.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UsageBudget {
    pub limit: f64,
    pub spent: f64,
    pub per: String,
    pub scope: Option<String>,
    pub coverage: Option<String>,
    pub models: Vec<(String, Option<f64>)>,
    pub resets_at: Option<i64>,
}

/// The installed `UsageLimits.Info` subset the adapter projects: the Go
/// allowance meters plus the Console budgets. `None` means the location is
/// not signed in (the operation answers `data: null`).
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct UsageInfo {
    pub rolling: Option<(f64, Option<i64>)>,
    pub weekly: Option<(f64, Option<i64>)>,
    pub monthly: Option<(f64, Option<i64>)>,
    pub budgets: Vec<UsageBudget>,
}

/// Run the frozen native exe with exact argv
/// `api experimental.usage.limits --standalone` and return the installed
/// usage info, or `None` when the location is not signed in. Bounded by
/// [`USAGE_DEADLINE`] and [`MAX_USAGE_BYTES`]; like the catalogue readers
/// this is read-only (no session, no inference) and any failure is a static
/// typed string naming the `usage.limits` surface. The child inherits no
/// extra environment beyond `environment` (frozen profile environment, for
/// example proxy configuration); native config and credential homes stay
/// inherited so the operation resolves the owner's active connection.
pub(crate) async fn run_usage_limits(
    program: &OsStr,
    environment: Option<&serde_json::Map<String, Value>>,
    deadline: Instant,
) -> Result<Option<UsageInfo>> {
    let mut command = Command::new(program);
    command
        .args(usage_limits_argv())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if let Some(environment) = environment {
        for (key, value) in environment {
            if let Some(value) = value.as_str() {
                command.env(key, value);
            }
        }
    }
    let mut child = command
        .spawn()
        .map_err(|_| Error::new("native_spawn", "OpenCode usage.limits could not start"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::new("native_spawn", "OpenCode usage.limits could not start"))?;
    let mut buffer = Vec::new();
    let read = async {
        let mut limited = (&mut stdout).take((MAX_USAGE_BYTES + 1) as u64);
        limited.read_to_end(&mut buffer).await
    };
    match timeout_at(deadline, read).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode usage.limits did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_readback",
                "OpenCode usage.limits output was unreadable",
            ));
        }
        Ok(Ok(_)) => {}
    }
    if buffer.len() > MAX_USAGE_BYTES {
        kill_and_reap(&mut child).await;
        return Err(Error::new(
            "native_readback",
            "OpenCode usage.limits output was unreadable",
        ));
    }
    let status = match timeout_at(deadline, child.wait()).await {
        Err(_) => {
            kill_and_reap(&mut child).await;
            return Err(Error::new(
                "native_spawn",
                "OpenCode usage.limits did not finish before its deadline",
            ));
        }
        Ok(Err(_)) => {
            return Err(Error::new(
                "native_readback",
                "OpenCode usage.limits output was unreadable",
            ));
        }
        Ok(Ok(status)) => status,
    };
    if !status.success() {
        return Err(Error::new(
            "native_readback",
            "OpenCode usage.limits did not return usage",
        ));
    }
    let text = String::from_utf8(buffer).map_err(|_| {
        Error::new(
            "native_readback",
            "OpenCode usage.limits output was unreadable",
        )
    })?;
    parse_usage_limits_output(&text)
}

/// Parse `opencode api experimental.usage.limits --standalone` stdout: the
/// installed `{"data": UsageLimits.Info | null}` envelope. `null` means the
/// location is not signed in. Go meters keep `percent` plus `resetsAt`;
/// budgets keep the dollar `limit`/`spent` plus `per` and `resetsAt`.
/// Anything else malformed fails the read instead of guessed: a body
/// without `data` (for example an error envelope on a failed call the
/// exit-code gate somehow missed) is unknown, never "not signed in".
pub(crate) fn parse_usage_limits_output(text: &str) -> Result<Option<UsageInfo>> {
    let value: Value = serde_json::from_str(text).map_err(|_| {
        Error::new(
            "native_readback",
            "OpenCode usage.limits output was unreadable",
        )
    })?;
    let Some(data) = value.get("data") else {
        return Err(Error::new(
            "native_readback",
            "OpenCode usage.limits output was unreadable",
        ));
    };
    if data.is_null() {
        return Ok(None);
    }
    let info = data.as_object().ok_or_else(|| {
        Error::new(
            "native_readback",
            "OpenCode usage.limits output was unreadable",
        )
    })?;
    let mut usage = UsageInfo::default();
    if let Some(go) = info.get("go").and_then(Value::as_object) {
        usage.rolling = go_meter(go.get("rolling"));
        usage.weekly = go_meter(go.get("weekly"));
        usage.monthly = go_meter(go.get("monthly"));
    }
    if let Some(budgets) = info.get("budgets").and_then(Value::as_array) {
        if budgets.len() > 64 {
            return Err(Error::new(
                "source_integrity",
                "OpenCode usage.limits reports too many budgets",
            ));
        }
        for budget in budgets {
            let Some(budget) = budget.as_object() else {
                continue;
            };
            let (Some(limit), Some(spent)) = (
                budget.get("limit").and_then(Value::as_f64),
                budget.get("spent").and_then(Value::as_f64),
            ) else {
                continue;
            };
            if !limit.is_finite() || !spent.is_finite() || limit <= 0.0 {
                continue;
            }
            usage.budgets.push(UsageBudget {
                limit,
                spent,
                per: budget
                    .get("per")
                    .and_then(Value::as_str)
                    .unwrap_or("budget")
                    .to_owned(),
                scope: budget
                    .get("scope")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                coverage: budget
                    .get("coverage")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                models: budget
                    .get("models")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|model| {
                        let provider = model.get("providerID")?.as_str()?;
                        let id = model.get("modelID")?.as_str()?;
                        let full = format!("{provider}/{id}");
                        eliot_plugin::session::split_provider_model(&full)?;
                        let spent = model
                            .get("spent")
                            .and_then(Value::as_f64)
                            .filter(|spent| spent.is_finite() && *spent >= 0.0);
                        Some((full, spent))
                    })
                    .collect(),
                resets_at: eliot_plugin::quota::reset(budget.get("resetsAt")),
            });
        }
    }
    Ok(Some(usage))
}

/// One Go meter as `(percent, resetsAt)`: `percent` is required, `resetsAt`
/// is optional. Unusable meters are skipped, never zero-filled.
fn go_meter(value: Option<&Value>) -> Option<(f64, Option<i64>)> {
    let meter = value?.as_object()?;
    let percent = meter.get("percent").and_then(Value::as_f64)?;
    if !percent.is_finite() {
        return None;
    }
    Some((percent, eliot_plugin::quota::reset(meter.get("resetsAt"))))
}

/// Project the native usage info into quota windows. Go meters keep the
/// legacy 5-hour/Weekly/Monthly labels; each budget contributes its spend
/// share of its dollar limit. Entries without usable numbers are skipped,
/// never zero-filled.
pub(crate) fn usage_windows(info: &UsageInfo) -> Vec<Value> {
    let mut windows = Vec::new();
    for (meter, label, minutes) in [
        (info.rolling, "5-hour", 300),
        (info.weekly, "Weekly", 10080),
        (info.monthly, "Monthly", 43200),
    ] {
        if let Some((percent, resets_at)) = meter
            && let Some(window) =
                eliot_plugin::quota::window(label, Some(percent), resets_at, Some(minutes))
        {
            windows.push(window);
        }
    }
    for budget in &info.budgets {
        let percent = (budget.spent / budget.limit * 100.0).clamp(0.0, 100.0);
        let label = format!("Console {}", budget.per);
        if let Some(window) =
            eliot_plugin::quota::window(&label, Some(percent), budget.resets_at, None)
        {
            windows.push(window);
        }
    }
    windows
}

/// Normalized routing evidence from the pinned UsageLimits.Budget schema.
/// The top spend from a per-model budget is display-only; decisions use
/// the measured spend of the exact model. A set budget can cover the whole
/// Console account or a set of provider/model IDs.
pub(crate) fn scoped_budget_windows(info: &UsageInfo) -> Vec<Value> {
    let mut windows = Vec::new();
    for budget in &info.budgets {
        let Some(scope @ ("each" | "combined")) = budget.scope.as_deref() else {
            continue;
        };
        let mut push = |spent: f64, coverage: &str, models: Vec<&str>| {
            let percent = (spent / budget.limit * 100.0).clamp(0.0, 100.0);
            if let Some(mut window) =
                eliot_plugin::quota::window("Console budget", Some(percent), budget.resets_at, None)
            {
                window["scope"] = json!(scope);
                window["coverage"] = json!(coverage);
                window["models"] = json!(models);
                windows.push(window);
            }
        };
        match (budget.per.as_str(), budget.coverage.as_deref()) {
            ("set", Some("all")) => push(budget.spent, "all", Vec::new()),
            ("set", Some("models")) if !budget.models.is_empty() => {
                push(
                    budget.spent,
                    "models",
                    budget
                        .models
                        .iter()
                        .map(|(model, _)| model.as_str())
                        .collect(),
                );
            }
            ("model", Some("all" | "models")) => {
                for (model, spent) in &budget.models {
                    if let Some(spent) = spent {
                        push(*spent, "models", vec![model]);
                    }
                }
            }
            _ => {}
        }
    }
    windows
}

/// Latest deadline this module starts its own child under when the caller
/// passes none: booting the private server plus the native 15 s operation
/// budget.
pub(crate) fn child_deadline() -> Instant {
    Instant::now() + USAGE_DEADLINE
}
