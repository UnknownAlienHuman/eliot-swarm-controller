//! Standalone Store scheduler client. It never holds Manager credentials.

use std::{env, path::PathBuf, time::Duration};
use swarm_automation::{
    ADMIT_METHOD, DuePage, PAGE_METHOD, READY_FRAME, WorkerConfig, has_due_source,
    read_worker_config_file, wait_ms,
};
use swarm_client::{Client, IpcConfig};
use swarm_contracts::{
    DeclaredServiceScope,
    error::{Error, Result},
};
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    if let Err(error) = run_cli().await {
        eprintln!("swarm-automation-worker: {}", error.code);
        std::process::exit(2);
    }
}

async fn run_cli() -> Result<()> {
    let mut args = env::args_os().skip(1);
    let mode = args
        .next()
        .and_then(|arg| arg.into_string().ok())
        .ok_or_else(|| Error::invalid("expected run --config <private-file>"))?;
    if mode != "run" {
        return Err(Error::invalid("expected run --config <private-file>"));
    }
    let flag = args
        .next()
        .and_then(|arg| arg.into_string().ok())
        .ok_or_else(|| Error::invalid("missing --config"))?;
    if flag != "--config" {
        return Err(Error::invalid("expected --config"));
    }
    let path = args
        .next()
        .map(PathBuf::from)
        .ok_or_else(|| Error::invalid("missing worker config path"))?;
    if args.next().is_some() {
        return Err(Error::invalid("unexpected worker argument"));
    }
    let config = read_worker_config_file(&path)?;
    let _service_owner =
        swarm_process::Group::enter_service(&config.service_owner_token, config.scope.purpose)?;
    {
        use std::io::Write;
        let mut stdout = std::io::stdout().lock();
        stdout
            .write_all(READY_FRAME)
            .and_then(|()| stdout.flush())
            .map_err(|_| {
                Error::new(
                    "AUTOMATION_WORKER_READY_SIGNAL_FAILED",
                    "scheduler service readiness could not be signaled",
                )
            })?;
    }
    run_worker(config).await
}

async fn run_worker(config: WorkerConfig) -> Result<()> {
    // Run one shared timer/queue. Reconnect/readback is the only recovery path;
    // no action-specific task or client retry loop is created.
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);
    loop {
        let page_result = tokio::select! {
            result = &mut signal => {
                result.map_err(|_| Error::new("AUTOMATION_WORKER_SIGNAL_FAILED", "shutdown signal could not be read"))?;
                return Ok(());
            }
            page = read_page(&config) => page,
        };
        let page = match page_result {
            Ok(page) => page,
            Err(error) if retryable(&error.code) => {
                eprintln!(
                    "swarm-automation-worker: {}; rereading Store page",
                    error.code
                );
                sleep(Duration::from_millis(1_000)).await;
                continue;
            }
            Err(error) => return Err(error),
        };
        page.validate(&config.scope).map_err(|_| {
            Error::new(
                "AUTOMATION_PAGE_INVALID",
                "Store returned an invalid scheduler page",
            )
        })?;
        if has_due_source(&page) {
            let params = page.admit_params().map_err(|_| {
                Error::new(
                    "AUTOMATION_PAGE_INVALID",
                    "scheduler page could not form a bounded admission request",
                )
            })?;
            let result = tokio::select! {
                signal_result = &mut signal => {
                    signal_result.map_err(|_| Error::new("AUTOMATION_WORKER_SIGNAL_FAILED", "shutdown signal could not be read"))?;
                    return Ok(());
                }
                result = call_once(&config, ADMIT_METHOD, params) => result,
            };
            match result {
                Ok(value) if valid_admit_receipt(&value, &config.scope) => {
                    if value["disposition"] == "reconcilers_completed"
                        && value["next_due_at_ms"]
                            .as_i64()
                            .is_some_and(|due| due <= page.observed_at_ms)
                    {
                        // A retained pending cause can remain due after the
                        // Store records its reason. Bound the shared pulse so
                        // it does not spin while the legacy reader reconciles.
                        sleep(Duration::from_millis(250)).await;
                    }
                    continue;
                }
                Ok(_) => {
                    return Err(Error::new(
                        "AUTOMATION_ADMIT_RECEIPT_INVALID",
                        "Store returned an invalid scheduler receipt",
                    ));
                }
                // Do not replay the previous page on an uncertain reply. The
                // next iteration reads authoritative source cursors first;
                // each domain's stable slot/receipt coalesces a committed cut.
                Err(error) if retryable(&error.code) || error.code == "AUTOMATION_PAGE_STALE" => {
                    eprintln!(
                        "swarm-automation-worker: {}; rereading Store page",
                        error.code
                    );
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        tokio::select! {
            result = &mut signal => {
                result.map_err(|_| Error::new("AUTOMATION_WORKER_SIGNAL_FAILED", "shutdown signal could not be read"))?;
                return Ok(());
            }
            _ = sleep(Duration::from_millis(wait_ms(&page))) => {}
        }
    }
}

async fn read_page(config: &WorkerConfig) -> Result<DuePage> {
    let value = call_once(
        config,
        PAGE_METHOD,
        serde_json::json!({
            "scope":config.scope,
        }),
    )
    .await?;
    serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_PAGE_INVALID",
            "Store scheduler page does not match its fixed schema",
        )
    })
}

async fn call_once(
    config: &WorkerConfig,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value> {
    let mut client = Client::connect(
        &PathBuf::from(&config.store_root),
        &config.credential,
        &IpcConfig::default(),
    )
    .await?;
    client.request(method, params).await
}

fn valid_admit_receipt(value: &serde_json::Value, scope: &DeclaredServiceScope) -> bool {
    value["schema_version"] == 1
        && value["scope"] == serde_json::json!(scope)
        && matches!(
            value["disposition"].as_str(),
            Some("reconcilers_completed" | "already_observed" | "page_stale")
        )
        && value["next_due_at_ms"].as_i64().is_none_or(|due| due >= 0)
}

fn retryable(code: &str) -> bool {
    matches!(
        code,
        "HOST_UNAVAILABLE"
            | "IO_ERROR"
            | "OUTCOME_UNKNOWN"
            | "STORE_CLOSED"
            | "DISCONNECTED"
            | "AUTOMATION_SERVICE_OWNER_PENDING"
    )
}
