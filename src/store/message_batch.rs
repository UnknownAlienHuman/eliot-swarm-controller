//! Bounded FIFO batching for the one high-volume external mutation.
//!
//! The writer queue identifies `message.send` explicitly; no arbitrary Store
//! closure is coalesced. Each request keeps its own authorization, request
//! identity, and mutation savepoint, while one FULL-synchronous transaction
//! commits the contiguous batch before any caller receives a reply.
use super::{current_principal, mutate_in_transaction};
use crate::{
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, Transaction, TransactionBehavior};
use serde_json::Value;
use tokio::sync::oneshot;

pub(super) const MAX_BATCH_SIZE: usize = 16;
const SAVEPOINT_SQL: &str = "SAVEPOINT message_send_batch_item";
const ROLLBACK_ITEM_SQL: &str =
    "ROLLBACK TO message_send_batch_item; RELEASE message_send_batch_item";
const RELEASE_ITEM_SQL: &str = "RELEASE message_send_batch_item";

pub(super) struct Request {
    pub(super) principal: Principal,
    pub(super) params: Value,
    pub(super) response: oneshot::Sender<Result<Value>>,
}

pub(super) fn process(db: &mut Connection, requests: Vec<Request>, config: &Config) {
    if requests.is_empty() {
        return;
    }

    let tx = match db.transaction_with_behavior(TransactionBehavior::Immediate) {
        Ok(tx) => tx,
        Err(error) => {
            fail_all(requests, error.into());
            return;
        }
    };
    let mut outcomes = Vec::with_capacity(requests.len());
    let mut fatal = None;

    for request in &requests {
        if let Err(error) = tx.execute_batch(SAVEPOINT_SQL) {
            fatal = Some(error.into());
            break;
        }

        match process_one(&tx, request, config) {
            Ok(outcome) => {
                // The inner result is the existing durable apply receipt,
                // including rejected receipts. Only an outer error means
                // its savepoint or ledger could not be safely persisted.
                if let Err(error) = tx.execute_batch(RELEASE_ITEM_SQL) {
                    fatal = Some(error.into());
                    break;
                }
                outcomes.push(outcome);
            }
            Err(error) if error.code == "STORE_ERROR" => {
                // A database failure must roll back every request in this
                // batch; no earlier per-item result has been acknowledged.
                fatal = Some(error);
                break;
            }
            Err(error) => {
                // Validation, authorization, and request-id conflicts are
                // local to one request. Remove any partial item work and let
                // later FIFO items proceed in the same outer transaction.
                if let Err(rollback_error) = tx.execute_batch(ROLLBACK_ITEM_SQL) {
                    fatal = Some(rollback_error.into());
                    break;
                }
                outcomes.push(Err(error));
            }
        }
    }

    if let Some(error) = fatal {
        drop(tx);
        fail_all(requests, error);
        return;
    }

    if let Err(error) = tx.commit() {
        fail_all(requests, error.into());
        return;
    }

    for (request, outcome) in requests.into_iter().zip(outcomes) {
        let _ = request.response.send(outcome);
    }
}

fn process_one(tx: &Transaction<'_>, request: &Request, config: &Config) -> Result<Result<Value>> {
    let principal = current_principal(tx, request.principal.clone())?;
    if principal.role == Role::Module {
        return Ok(Err(Error::new(
            "FORBIDDEN",
            "module credentials serve only their native binding",
        )));
    }
    principal.require_writer()?;
    mutate_in_transaction(
        tx,
        &principal,
        "message.send",
        &request.params,
        config,
        model::now_ms()?,
    )
}

fn fail_all(requests: Vec<Request>, error: Error) {
    for request in requests {
        let _ = request.response.send(Err(error.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use serde_json::json;

    fn queued(principal: &Principal, params: Value) -> (Request, oneshot::Receiver<Result<Value>>) {
        let (response, receive) = oneshot::channel();
        (
            Request {
                principal: principal.clone(),
                params,
                response,
            },
            receive,
        )
    }

    #[test]
    fn batch_keeps_request_receipts_and_isolates_one_conflict_and_rejection() {
        let mut db = Connection::open_in_memory().unwrap();
        db.pragma_update(None, "foreign_keys", "ON").unwrap();
        db.pragma_update(None, "synchronous", "FULL").unwrap();
        db.execute_batch(crate::store::SCHEMA).unwrap();
        for client_id in ["alice", "bob"] {
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    format!("client:{client_id}"),
                    r#"{"role":"manager","disabled":false}"#
                ],
            )
            .unwrap();
        }
        let principal = Principal {
            link_id: "fixture-link".into(),
            client_id: "alice".into(),
            role: Role::Manager,
        };
        let first = json!({
            "client_request_id":"same-request",
            "recipient":"bob",
            "text":"first body"
        });
        let changed = json!({
            "client_request_id":"same-request",
            "recipient":"bob",
            "text":"changed body"
        });
        let rejected = json!({
            "client_request_id":"rejected-request",
            "recipient":"unknown",
            "text":"domain rejection"
        });
        let following = json!({
            "client_request_id":"following-request",
            "recipient":"bob",
            "text":"after rejection"
        });
        let inputs = [first.clone(), first, changed, rejected, following];
        let (requests, receives): (Vec<_>, Vec<_>) = inputs
            .into_iter()
            .map(|input| queued(&principal, input))
            .unzip();

        process(&mut db, requests, &Config::default());
        let replies = receives
            .into_iter()
            .map(|receive| receive.blocking_recv().unwrap())
            .collect::<Vec<_>>();

        let first_reply = replies[0].as_ref().unwrap();
        assert_eq!(replies[1].as_ref().unwrap(), first_reply);
        assert_eq!(replies[2].as_ref().unwrap_err().code, "REQUEST_ID_CONFLICT");
        assert_eq!(replies[3].as_ref().unwrap_err().code, "NOT_FOUND");
        assert!(
            replies[4].is_ok(),
            "a later valid send survives item errors"
        );

        let operations: i64 = db
            .query_row(
                "SELECT count(*) FROM operations WHERE caller_id='alice'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let observations: i64 = db
            .query_row("SELECT count(*) FROM observations", [], |row| row.get(0))
            .unwrap();
        let rejected_state: String = db
            .query_row(
                "SELECT state FROM operations WHERE caller_id='alice' AND client_request_id='rejected-request'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(operations, 3, "exact replay and conflict add no operation");
        assert_eq!(
            observations, 2,
            "only successful first admissions retain observations"
        );
        assert_eq!(rejected_state, "rejected");

        db.execute_batch(
            "CREATE TRIGGER reject_message_observation BEFORE INSERT ON observations \
             WHEN NEW.kind='message.send' BEGIN SELECT RAISE(ABORT,'fixture failure'); END",
        )
        .unwrap();
        let fatal_inputs = [
            json!({"client_request_id":"fatal-first","recipient":"bob","text":"rolled back"}),
            json!({"client_request_id":"fatal-second","recipient":"bob","text":"not processed"}),
        ];
        let (requests, receives): (Vec<_>, Vec<_>) = fatal_inputs
            .into_iter()
            .map(|input| queued(&principal, input))
            .unzip();
        process(&mut db, requests, &Config::default());
        let failures = receives
            .into_iter()
            .map(|receive| receive.blocking_recv().unwrap())
            .collect::<Vec<_>>();
        assert!(failures.iter().all(|result| {
            result
                .as_ref()
                .is_err_and(|error| error.code == "STORE_ERROR")
        }));
        let operations_after_failure: i64 = db
            .query_row(
                "SELECT count(*) FROM operations WHERE caller_id='alice'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let observations_after_failure: i64 = db
            .query_row("SELECT count(*) FROM observations", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            operations_after_failure, 3,
            "failed batch rolls back all writes"
        );
        assert_eq!(observations_after_failure, 2);
    }
}
