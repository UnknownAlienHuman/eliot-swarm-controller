//! Host lifecycle receipts survive the IPC endpoint and the manager's chat.
use super::{meta, set_meta};
use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const CURRENT: &str = "host:lifecycle:v1";
const LAST_EXIT: &str = "host:last-exit:v1";
const LATEST_FAILURE: &str = "host:latest-failure:v1";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Lifecycle {
    schema_version: u8,
    host_epoch: i64,
    state: State,
    started_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum State {
    Starting,
    Running,
    Stopped,
    Failed,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Exit {
    schema_version: u8,
    host_epoch: Option<i64>,
    observed_at_ms: i64,
    error_code: Option<String>,
    manager_action_required: bool,
    retry_authorized: bool,
}

fn epoch(db: &Connection) -> Result<i64> {
    meta(db, "host_epoch")?
        .and_then(|v| v.as_i64())
        .filter(|epoch| *epoch > 0)
        .ok_or_else(|| Error::new("HOST_LIFECYCLE_INVALID", "host epoch is missing"))
}

fn load(db: &Connection) -> Result<Option<Lifecycle>> {
    meta(db, CURRENT)?
        .map(|value| {
            let record: Lifecycle = serde_json::from_value(value).map_err(|_| {
                Error::new(
                    "HOST_LIFECYCLE_INVALID",
                    "host lifecycle receipt is invalid",
                )
            })?;
            if record.schema_version != 1
                || record.host_epoch <= 0
                || record.started_at_ms < 0
                || record.updated_at_ms < record.started_at_ms
            {
                return Err(Error::new(
                    "HOST_LIFECYCLE_INVALID",
                    "host lifecycle receipt is invalid",
                ));
            }
            Ok(record)
        })
        .transpose()
}

fn retain_exit(tx: &Transaction<'_>, receipt: &Exit) -> Result<()> {
    let value = json!(receipt);
    set_meta(tx, LAST_EXIT, &value)?;
    if receipt.manager_action_required {
        set_meta(tx, LATEST_FAILURE, &value)?;
    }
    let key = format!(
        "{}:{}",
        receipt.host_epoch.unwrap_or(0),
        receipt.observed_at_ms
    );
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,kind,payload_json,recorded_at_ms) VALUES('controller:host-lifecycle',?1,'host.exit',?2,?3)",
        params![key, model::canonical(&value)?, receipt.observed_at_ms],
    )?;
    Ok(())
}

/// Called after acquiring the exclusive data-root lock and starting the Store.
/// A previous active marker proves interruption, but does not identify its cause.
pub(super) fn start(tx: &Transaction<'_>, now: i64) -> Result<()> {
    let current_epoch = epoch(tx)?;
    match load(tx) {
        Ok(Some(previous)) if matches!(previous.state, State::Starting | State::Running) => {
            retain_exit(
                tx,
                &Exit {
                    schema_version: 1,
                    host_epoch: Some(previous.host_epoch),
                    observed_at_ms: now,
                    error_code: Some("HOST_INTERRUPTED".into()),
                    manager_action_required: true,
                    retry_authorized: false,
                },
            )?;
        }
        Err(error) if error.code != "STORE_ERROR" && error.code != "STORE_CLOSED" => {
            // Keep the original malformed fact for inspection; recovery must
            // not make an optional lifecycle receipt prevent host startup.
            let original: Option<String> = tx
                .query_row(
                    "SELECT value_json FROM meta WHERE key=?1",
                    [CURRENT],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(original) = original {
                set_meta(
                    tx,
                    "host:invalid-lifecycle:v1",
                    &json!({"raw_value_json": original}),
                )?;
            }
            retain_exit(
                tx,
                &Exit {
                    schema_version: 1,
                    host_epoch: None,
                    observed_at_ms: now,
                    error_code: Some("HOST_LIFECYCLE_INVALID".into()),
                    manager_action_required: true,
                    retry_authorized: false,
                },
            )?;
        }
        Err(error) => return Err(error),
        _ => {}
    }
    set_meta(
        tx,
        CURRENT,
        &json!(Lifecycle {
            schema_version: 1,
            host_epoch: current_epoch,
            state: State::Starting,
            started_at_ms: now,
            updated_at_ms: now
        }),
    )
}

pub(super) fn ready(tx: &Transaction<'_>, now: i64) -> Result<()> {
    let mut current = load(tx)?
        .ok_or_else(|| Error::new("HOST_LIFECYCLE_INVALID", "host startup receipt is missing"))?;
    if current.host_epoch != epoch(tx)? || current.state != State::Starting {
        return Err(Error::new(
            "HOST_LIFECYCLE_INVALID",
            "host startup receipt is not current",
        ));
    }
    current.state = State::Running;
    current.updated_at_ms = now;
    set_meta(tx, CURRENT, &json!(current))
}

pub(super) fn finish(tx: &Transaction<'_>, error_code: Option<&str>, now: i64) -> Result<()> {
    let mut current = load(tx)?
        .ok_or_else(|| Error::new("HOST_LIFECYCLE_INVALID", "host startup receipt is missing"))?;
    if current.host_epoch != epoch(tx)?
        || !matches!(current.state, State::Starting | State::Running)
    {
        return Err(Error::new(
            "HOST_LIFECYCLE_INVALID",
            "host exit receipt is not current",
        ));
    }
    // Codes are controller identifiers. Do not persist error messages, paths,
    // process output, connection strings, request bodies or credentials here.
    let error_code = error_code.map(|code| {
        if !code.is_empty()
            && code.len() <= 64
            && code
                .bytes()
                .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
        {
            code.to_owned()
        } else {
            "HOST_FAILED".to_owned()
        }
    });
    current.state = if error_code.is_some() {
        State::Failed
    } else {
        State::Stopped
    };
    current.updated_at_ms = now;
    retain_exit(
        tx,
        &Exit {
            schema_version: 1,
            host_epoch: Some(current.host_epoch),
            observed_at_ms: now,
            manager_action_required: error_code.is_some(),
            error_code,
            retry_authorized: false,
        },
    )?;
    set_meta(tx, CURRENT, &json!(current))
}

fn exit_receipt(db: &Connection, key: &str) -> Result<Option<Value>> {
    meta(db, key)?
        .map(|value| {
            let receipt: Exit = serde_json::from_value(value).map_err(|_| {
                Error::new("HOST_LIFECYCLE_INVALID", "host exit receipt is invalid")
            })?;
            if receipt.schema_version != 1
                || receipt.observed_at_ms < 0
                || receipt.retry_authorized
                || receipt.host_epoch.is_some_and(|epoch| epoch <= 0)
                || receipt.error_code.as_ref().is_some_and(|code| {
                    code.is_empty()
                        || code.len() > 64
                        || !code
                            .bytes()
                            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
                })
                || receipt.manager_action_required != receipt.error_code.is_some()
            {
                return Err(Error::new(
                    "HOST_LIFECYCLE_INVALID",
                    "host exit receipt is invalid",
                ));
            }
            Ok(json!(receipt))
        })
        .transpose()
}

fn status_value(value: Result<Option<Value>>) -> Result<Value> {
    match value {
        Ok(value) => Ok(value.unwrap_or(Value::Null)),
        Err(error)
            if matches!(
                error.code.as_str(),
                "HOST_LIFECYCLE_INVALID" | "INVALID_PARAMS"
            ) =>
        {
            Ok(
                json!({"status":"invalid","error_code":"HOST_LIFECYCLE_INVALID","manager_action_required":true,"retry_authorized":false}),
            )
        }
        Err(error) => Err(error),
    }
}

pub(super) fn status(db: &Connection) -> Result<Value> {
    let current = status_value(load(db).map(|value| value.map(|record| json!(record))))?;
    let last_exit = status_value(exit_receipt(db, LAST_EXIT))?;
    let latest_failure = status_value(exit_receipt(db, LATEST_FAILURE))?;
    Ok(
        json!({"current":current,"last_exit":last_exit,"latest_failure":latest_failure,
        "failure_history":"retained; a later graceful exit does not acknowledge or erase an earlier failure",
        "required_readback":"operation.get before retrying admitted work"}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_current_is_archived_raw_and_replaced_without_exposure() {
        let mut db = Connection::open_in_memory().expect("open in-memory database");
        db.execute_batch(
            "CREATE TABLE meta(key TEXT PRIMARY KEY, value_json TEXT NOT NULL);
             CREATE TABLE observations(
                 source_stream_id TEXT NOT NULL,
                 source_event_key TEXT NOT NULL,
                 kind TEXT NOT NULL,
                 payload_json TEXT NOT NULL,
                 recorded_at_ms INTEGER NOT NULL
             );",
        )
        .expect("create lifecycle tables");

        let secret_marker = "host-lifecycle-secret-marker";
        let raw = format!(
            "{{\"schema_version\":1,\"state\":\"running\",\"diagnostic\":\"{secret_marker}\""
        );
        set_meta(&db, "host_epoch", &json!(7)).expect("set current epoch");
        db.execute(
            "INSERT INTO meta(key, value_json) VALUES(?1, ?2)",
            params![CURRENT, raw],
        )
        .expect("insert malformed lifecycle fact");

        let tx = db
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .expect("begin recovery transaction");
        start(&tx, 1_000).expect("recover malformed optional lifecycle fact");
        tx.commit().expect("commit recovery transaction");

        let current: Lifecycle = serde_json::from_value(
            meta(&db, CURRENT)
                .expect("read current lifecycle")
                .expect("new current lifecycle exists"),
        )
        .expect("new current lifecycle is valid");
        assert_eq!(current.host_epoch, 7);
        assert!(matches!(current.state, State::Starting));

        let archived = meta(&db, "host:invalid-lifecycle:v1")
            .expect("read malformed fact archive")
            .expect("malformed fact was archived");
        assert_eq!(archived["raw_value_json"].as_str(), Some(raw.as_str()));

        let last_exit = meta(&db, LAST_EXIT)
            .expect("read lifecycle diagnosis")
            .expect("lifecycle diagnosis exists");
        assert_eq!(
            last_exit["error_code"].as_str(),
            Some("HOST_LIFECYCLE_INVALID")
        );

        let public_status = status(&db).expect("public lifecycle status remains readable");
        let public_status = serde_json::to_string(&public_status).expect("serialize status");
        assert!(!public_status.contains(secret_marker));
        assert!(public_status.len() < 1_024);
    }
}
