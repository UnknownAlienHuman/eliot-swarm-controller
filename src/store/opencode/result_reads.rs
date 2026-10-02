//! Result recovery repeats only scoped GET reads, never native input admission.
use super::{Store, original, runtime};
use crate::{
    error::{Error, Result},
    model::Principal,
    runtime::{
        EffectOutcome, RuntimeCommand, RuntimeOutcome,
        opencode_v2::{self as oc, Options, Service},
    },
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::json;
use std::time::Duration;

pub(super) fn next_read(
    db: &Connection,
    p: &Principal,
    after: &str,
) -> Result<Option<RuntimeCommand>> {
    let (id, generation, _) = runtime::scope(db, p, true)?;
    let query = |after: &str| -> Result<Option<String>> {
        Ok(db.query_row("SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND method='agent.result' AND state IN ('sending','native_accepted','outcome_unknown') AND operation_id>?3 ORDER BY operation_id LIMIT 1",
            params![id, generation, after], |r| r.get(0)).optional()?)
    };
    let next = match query(after)? {
        Some(id) => Some(id),
        None if !after.is_empty() => query("")?,
        None => None,
    };
    next.map(|id| original(db, p, &id)).transpose()
}

impl Store {
    pub(super) async fn oc_result(
        &self,
        p: &Principal,
        service: &Service,
        options: &Options,
        command: &RuntimeCommand,
    ) -> Result<()> {
        let source = if command.input["selector"]["kind"] == "input_interval" {
            let target =
                crate::model::text(&command.input["selector"], "input_operation_id")?.to_owned();
            let p = p.clone();
            Some(
                self.run(move |db| {
                    let command = original(db, &p, &target)?;
                    let op = super::operations::get_operation(db, &target)?;
                    if !matches!(
                        op["state"].as_str(),
                        Some("sending" | "native_accepted" | "outcome_unknown" | "settled")
                    ) {
                        return Err(Error::invalid("selected input operation was not sent"));
                    }
                    Ok(command)
                })
                .await?,
            )
        } else {
            None
        };
        let page = tokio::time::timeout(
            Duration::from_secs(20),
            service.read_result(command, options, source.as_ref()),
        )
        .await
        .map_err(|_| {
            Error::new(
                "RESULT_READ_TIMEOUT",
                "bounded native result read did not finish",
            )
        })??;
        // Reuse the existing no-overwrite file publication and scoped SQLite
        // registration. Neither a GET nor an artifact is a Task acceptance.
        self.persist_result(
            p.clone(),
            json!({"operation_id":command.operation_id,"page":page}),
        )
        .await?;
        Ok(())
    }
    pub(super) async fn oc_result_outcome(
        &self,
        p: &Principal,
        service: &Service,
        options: &Options,
        command: &RuntimeCommand,
    ) -> bool {
        if let Err(error) = self.oc_result(p, service, options, command).await {
            let outcome = RuntimeOutcome {
                operation_id: command.operation_id.clone(),
                outcome: if matches!(
                    error.code.as_str(),
                    "INVALID_PARAMS" | "UNSUPPORTED_RESULT_KIND" | "FORBIDDEN"
                ) {
                    EffectOutcome::Rejected
                } else {
                    EffectOutcome::Unknown
                },
                native_scope_key: Some(options.scope()),
                native_root_id: command.native_root_id.clone(),
                turn_id: None,
                native_input_id: None,
                details: oc::diagnostic(&error),
            };
            if let Err(error) = self.record_oc_outcome(p, outcome).await {
                eprintln!("OpenCode result receipt: {}", error.code);
            }
            false
        } else {
            true
        }
    }
}
