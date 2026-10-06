use crate::{
    automation::authorization::{ManualCheckRunContext, manual_run_now_check_request_id},
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

use super::{Store, current_principal, mutate_in_transaction_with_check_plan, receipt_result};

impl Store {
    pub(super) async fn schedule_run_now(
        &self,
        principal: Principal,
        params: Value,
    ) -> Result<Value> {
        model::validate_mutation("schedule.run_now", &params)?;
        let project_id = model::text(&params, "project_id")?.to_owned();
        let automation_id = model::text(&params, "automation_id")?.to_owned();
        let client_request_id = model::text(&params, "client_request_id")?.to_owned();
        let invocation_identity = json!({
            "schema_version":1,
            "manager_id":principal.client_id.clone(),
            "client_request_id":client_request_id.clone(),
            "project_id":project_id.clone(),
            "automation_id":automation_id.clone(),
        });
        let check_request_id = manual_run_now_check_request_id(
            &principal.client_id,
            &client_request_id,
            &project_id,
            &automation_id,
        )?;

        // The deterministic manual ID resolves a lost reply before current
        // entry/target checks, so a retry never admits a second CheckRun.
        let p = principal.clone();
        let request_id = check_request_id.clone();
        let identity = invocation_identity.clone();
        if let Some(existing) = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                if p.role != Role::Manager {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "schedule.run_now requires a Manager principal",
                    ));
                }
                p.require_writer()?;
                let row: Option<(String, String)> = db
                    .query_row(
                        "SELECT method,effective_request_json FROM operations \
                         WHERE caller_id=?1 AND client_request_id=?2",
                        params![p.client_id, request_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                let Some((method, effective_json)) = row else {
                    return Ok(None);
                };
                if method != "check.run" {
                    return Err(Error::new(
                        "REQUEST_ID_CONFLICT",
                        "manual invocation identity is already used by another method",
                    ));
                }
                let effective: Value = serde_json::from_str(&effective_json)?;
                if effective.get("manual_run_now") != Some(&identity) {
                    return Err(Error::new(
                        "REQUEST_ID_CONFLICT",
                        "manual invocation identity belongs to a different request",
                    ));
                }
                receipt_result(&effective["receipt"]).map(Some)
            })
            .await?
        {
            return Ok(existing);
        }

        let p = principal.clone();
        let context_project_id = project_id.clone();
        let context_automation_id = automation_id.clone();
        let context_request_id = client_request_id.clone();
        let context = self
            .run(move |db| {
                let current = current_principal(db, p)?;
                ManualCheckRunContext::from_committed_entry(
                    db,
                    &current,
                    &context_project_id,
                    &context_automation_id,
                    &context_request_id,
                )
            })
            .await?;

        let check_params = context.request_params()?;
        model::validate_mutation("check.run", &check_params)?;
        let resolution = self
            .resolve_check_plan(principal.clone(), check_params.clone())
            .await?;

        let p = principal.clone();
        let v = check_params;
        let config = self.config.clone();
        let request_id = check_request_id;
        let identity = invocation_identity.clone();
        let identity_json = model::canonical(&invocation_identity)?;
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = current_principal(&tx, p)?;
                if current.role != Role::Manager {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "schedule.run_now requires a Manager principal",
                    ));
                }
                let existing: Option<(String, String)> = tx
                    .query_row(
                        "SELECT method,effective_request_json FROM operations \
                         WHERE caller_id=?1 AND client_request_id=?2",
                        params![current.client_id, request_id],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()?;
                if let Some((method, effective_json)) = existing {
                    if method != "check.run" {
                        return Err(Error::new(
                            "REQUEST_ID_CONFLICT",
                            "manual invocation identity is already used by another method",
                        ));
                    }
                    let effective: Value = serde_json::from_str(&effective_json)?;
                    if effective.get("manual_run_now") != Some(&identity) {
                        return Err(Error::new(
                            "REQUEST_ID_CONFLICT",
                            "manual invocation identity belongs to a different request",
                        ));
                    }
                    let existing = receipt_result(&effective["receipt"]);
                    tx.commit()?;
                    return existing;
                }
                context.require_current_check_target(&tx, &current)?;
                let result = mutate_in_transaction_with_check_plan(
                    &tx,
                    &current,
                    "check.run",
                    &v,
                    &config,
                    model::now_ms()?,
                    Some(&resolution),
                )?;
                let operation_id: Option<String> = tx
                    .query_row(
                        "SELECT operation_id FROM operations \
                         WHERE caller_id=?1 AND client_request_id=?2 AND method='check.run'",
                        params![current.client_id, request_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                if let Some(operation_id) = operation_id {
                    tx.execute(
                        "UPDATE operations SET effective_request_json=json_set(\
                             effective_request_json,'$.manual_run_now',json(?2)) \
                         WHERE operation_id=?1",
                        params![operation_id, identity_json],
                    )?;
                }
                tx.commit()?;
                result
            })
            .await;
        if result.as_ref().is_ok_and(|value| value["cached"] != true) {
            self.changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
        result
    }
}
