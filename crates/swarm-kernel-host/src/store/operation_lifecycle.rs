//! One private Operation lifecycle parser and existing-row loader for R45.
//!
//! This file answers only one question: where the generic Operation state
//! machine is. It deliberately does not decide whether a domain effect applied,
//! may be retried, or satisfies a semantic slot. Each migrated domain supplies
//! its own method/request/link/result evidence on top of this parser.
//!
//! An unknown state string is retained-data corruption, never a new implicit
//! state, and never a generic retry authorization.

use crate::error::{Error, Result};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde_json::Value;

/// Where the generic Operation state machine is.
///
/// This is the complete closed set the persisted operations.state column can
/// hold. It carries no domain meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperationLifecycleState {
    Queued,
    Sending,
    NativeAccepted,
    OutcomeUnknown,
    Settled,
    Rejected,
    Cancelled,
}

impl OperationLifecycleState {
    /// Parse one persisted state string. Unknown values are corruption.
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "sending" => Ok(Self::Sending),
            "native_accepted" => Ok(Self::NativeAccepted),
            "outcome_unknown" => Ok(Self::OutcomeUnknown),
            "settled" => Ok(Self::Settled),
            "rejected" => Ok(Self::Rejected),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(Error::new(
                "AUTOMATION_OPERATION_CORRUPT",
                format!("retained Operation has an unknown lifecycle state: {other}"),
            )),
        }
    }

    /// The exact persisted string for this state.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Sending => "sending",
            Self::NativeAccepted => "native_accepted",
            Self::OutcomeUnknown => "outcome_unknown",
            Self::Settled => "settled",
            Self::Rejected => "rejected",
            Self::Cancelled => "cancelled",
        }
    }
}

/// How an R45 consumer locates one exact retained Operation.
pub(crate) enum ExistingAutomaticOperationLookup<'a> {
    OperationId(&'a str),
    ClientRequest {
        caller_id: &'a str,
        client_request_id: &'a str,
    },
}

/// One exact retained Operation row decoded at the Store boundary.
///
/// Raw request JSON remains alongside its parsed value so semantic slot checks
/// can compare the original immutable bytes. The effective request, original
/// request, and result are each parsed once here; domain consumers still own
/// their method-specific context and outcome validation.
#[derive(Debug)]
pub(crate) struct ExistingAutomaticOperation {
    pub(crate) operation_id: String,
    pub(crate) caller_id: String,
    pub(crate) client_request_id: String,
    pub(crate) method: String,
    pub(crate) original_request_json: String,
    pub(crate) original_request: Value,
    pub(crate) effective_request_json: String,
    pub(crate) effective_request: Value,
    pub(crate) state: OperationLifecycleState,
    pub(crate) task_id: Option<String>,
    pub(crate) attempt_id: Option<String>,
    pub(crate) binding_id: Option<String>,
    pub(crate) binding_generation: Option<i64>,
    pub(crate) sent_at_ms: Option<i64>,
    pub(crate) settled_at_ms: Option<i64>,
    /// `None` means no result column value was retained. A blank or malformed
    /// result is corruption, never an absent result or an in-flight shortcut.
    pub(crate) result: Option<Value>,
}

impl ExistingAutomaticOperation {
    /// Load by immutable Operation identity or by the caller's semantic request
    /// identity, then decode the complete persisted row once.
    pub(crate) fn load(
        db: &Connection,
        lookup: ExistingAutomaticOperationLookup<'_>,
    ) -> Result<Option<Self>> {
        let row = match lookup {
            ExistingAutomaticOperationLookup::OperationId(operation_id) => db
                .query_row(
                    "SELECT operation_id,caller_id,client_request_id,method, \
                            original_request_json,effective_request_json,state,result_json, \
                            task_id,attempt_id,binding_id,binding_generation,sent_at_ms,settled_at_ms \
                     FROM operations WHERE operation_id=?1",
                    [operation_id],
                    RawAutomaticOperation::read,
                )
                .optional()?,
            ExistingAutomaticOperationLookup::ClientRequest {
                caller_id,
                client_request_id,
            } => db
                .query_row(
                    "SELECT operation_id,caller_id,client_request_id,method, \
                            original_request_json,effective_request_json,state,result_json, \
                            task_id,attempt_id,binding_id,binding_generation,sent_at_ms,settled_at_ms \
                     FROM operations WHERE caller_id=?1 AND client_request_id=?2",
                    params![caller_id, client_request_id],
                    RawAutomaticOperation::read,
                )
                .optional()?,
        };
        row.map(Self::decode_row).transpose()
    }

    fn decode_row(row: RawAutomaticOperation) -> Result<Self> {
        let state = OperationLifecycleState::parse(&row.state)?;
        validate_timestamps(state, row.sent_at_ms, row.settled_at_ms)?;
        validate_binding_identity(row.binding_id.as_deref(), row.binding_generation)?;
        let original_request = parse_json_column(
            &row.original_request_json,
            "retained Operation original request is not valid JSON",
        )?;
        let effective_request_json = row.effective_request_json;
        let effective_request = parse_json_column(
            &effective_request_json,
            "retained Operation effective request is not valid JSON",
        )?;
        let result = row
            .result_json
            .as_deref()
            .map(|raw| parse_json_column(raw, "retained Operation result is not valid JSON"))
            .transpose()?;
        Ok(Self {
            operation_id: row.operation_id,
            caller_id: row.caller_id,
            client_request_id: row.client_request_id,
            method: row.method,
            original_request_json: row.original_request_json,
            original_request,
            effective_request_json,
            effective_request,
            state,
            task_id: row.task_id,
            attempt_id: row.attempt_id,
            binding_id: row.binding_id,
            binding_generation: row.binding_generation,
            sent_at_ms: row.sent_at_ms,
            settled_at_ms: row.settled_at_ms,
            result,
        })
    }

    /// Unit-test constructor for focused classifier tests. Store consumers use
    /// `load`, which always supplies and validates the full persisted row.
    #[cfg(test)]
    pub(crate) fn decode(
        operation_id: String,
        state: String,
        result_json: Option<String>,
        sent_at_ms: Option<i64>,
        settled_at_ms: Option<i64>,
    ) -> Result<Self> {
        let state = OperationLifecycleState::parse(&state)?;
        validate_timestamps(state, sent_at_ms, settled_at_ms)?;
        let result = result_json
            .as_deref()
            .map(|raw| parse_json_column(raw, "retained Operation result is not valid JSON"))
            .transpose()?;
        Ok(Self {
            operation_id,
            caller_id: String::new(),
            client_request_id: String::new(),
            method: String::new(),
            original_request_json: String::new(),
            original_request: Value::Null,
            effective_request_json: String::new(),
            effective_request: Value::Null,
            state,
            task_id: None,
            attempt_id: None,
            binding_id: None,
            binding_generation: None,
            sent_at_ms,
            settled_at_ms,
            result,
        })
    }

    /// The host-owned `task.accept`, Forge publication, and managed-label
    /// producers do not bind these Operations to a native execution binding.
    /// Method-specific consumers use this after the generic pair-shape check.
    pub(crate) fn has_unbound_execution_scope(&self) -> bool {
        self.binding_id.is_none() && self.binding_generation.is_none()
    }
}

struct RawAutomaticOperation {
    operation_id: String,
    caller_id: String,
    client_request_id: String,
    method: String,
    original_request_json: String,
    effective_request_json: String,
    state: String,
    result_json: Option<String>,
    task_id: Option<String>,
    attempt_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    sent_at_ms: Option<i64>,
    settled_at_ms: Option<i64>,
}

impl RawAutomaticOperation {
    fn read(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            operation_id: row.get(0)?,
            caller_id: row.get(1)?,
            client_request_id: row.get(2)?,
            method: row.get(3)?,
            original_request_json: row.get(4)?,
            effective_request_json: row.get(5)?,
            state: row.get(6)?,
            result_json: row.get(7)?,
            task_id: row.get(8)?,
            attempt_id: row.get(9)?,
            binding_id: row.get(10)?,
            binding_generation: row.get(11)?,
            sent_at_ms: row.get(12)?,
            settled_at_ms: row.get(13)?,
        })
    }
}

fn parse_json_column(raw: &str, message: &str) -> Result<Value> {
    serde_json::from_str(raw).map_err(|_| Error::new("AUTOMATION_OPERATION_CORRUPT", message))
}

fn validate_binding_identity(binding_id: Option<&str>, generation: Option<i64>) -> Result<()> {
    let valid = match (binding_id, generation) {
        (None, None) => true,
        (Some(binding_id), Some(generation)) => !binding_id.trim().is_empty() && generation > 0,
        _ => false,
    };
    if !valid {
        return Err(Error::new(
            "AUTOMATION_OPERATION_CORRUPT",
            "retained Operation binding identity is incomplete or invalid",
        ));
    }
    Ok(())
}

/// Validate the persisted timestamp shape without assigning domain meaning to
/// either timestamp. A retained unknown outcome can be recorded before the
/// generic dispatch timestamp exists, so only `sending` and `native_accepted`
/// require `sent_at_ms`; consumers use the timestamp itself as dispatch proof.
fn validate_timestamps(
    state: OperationLifecycleState,
    sent_at_ms: Option<i64>,
    settled_at_ms: Option<i64>,
) -> Result<()> {
    let invalid = sent_at_ms.is_some_and(|timestamp| timestamp < 0)
        || settled_at_ms.is_some_and(|timestamp| timestamp < 0)
        || sent_at_ms
            .zip(settled_at_ms)
            .is_some_and(|(sent, settled)| sent > settled)
        || match state {
            OperationLifecycleState::Queued => sent_at_ms.is_some() || settled_at_ms.is_some(),
            OperationLifecycleState::Sending | OperationLifecycleState::NativeAccepted => {
                sent_at_ms.is_none() || settled_at_ms.is_some()
            }
            OperationLifecycleState::OutcomeUnknown => settled_at_ms.is_some(),
            OperationLifecycleState::Settled
            | OperationLifecycleState::Rejected
            | OperationLifecycleState::Cancelled => settled_at_ms.is_none(),
        };
    if invalid {
        return Err(Error::new(
            "AUTOMATION_OPERATION_CORRUPT",
            "retained Operation lifecycle state and timestamps are incoherent",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ExistingAutomaticOperation, OperationLifecycleState};

    #[test]
    fn unknown_outcome_may_precede_dispatch_timestamp() {
        let retained = ExistingAutomaticOperation::decode(
            "op-unknown".to_owned(),
            "outcome_unknown".to_owned(),
            Some(r#"{"outcome":"unknown"}"#.to_owned()),
            None,
            None,
        )
        .expect("unknown may be recorded before generic dispatch");

        assert_eq!(retained.state, OperationLifecycleState::OutcomeUnknown);
        assert_eq!(retained.sent_at_ms, None);
    }

    #[test]
    fn malformed_result_json_is_corruption() {
        let error = ExistingAutomaticOperation::decode(
            "op-invalid".to_owned(),
            "queued".to_owned(),
            Some("not-json".to_owned()),
            None,
            None,
        )
        .expect_err("malformed retained result JSON is corruption");

        assert_eq!(error.code, "AUTOMATION_OPERATION_CORRUPT");
    }
}
