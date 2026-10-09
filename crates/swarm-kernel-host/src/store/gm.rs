//! Current GM designation, epoch and handover.
//!
//! The client role lives in the `client:*` meta records; the current GM is a
//! separate designation with its own epoch (implementation-v6 §4, agent_swarm
//! §10). Identity and the GM role are different things: a stable client
//! principal may hold the designation, lose it at handover, and keep its own
//! mailbox cursors and manager work either way.
//!
//! Handover changes only this meta record. It never changes the owner of an
//! existing manager Attempt, never restarts native work and never cancels
//! already admitted effects; pending decisions survive it. A former GM keeps
//! no GM-only rights through the regular API afterwards. Module credentials
//! can never hold the designation.
//!
//! Wake is deliberately not a push invented here: no native input or Channels
//! path is qualified for the GM entrypoint, so the explicit mode is
//! checkpoint poll over `report.delta`/`message.read` (implementation-v6
//! §"GM wake"). The host runs no hidden model polls.
use super::{meta, operations, set_meta};
use crate::{
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, Transaction};
use serde_json::{Value, json};

/// A handover retains one bounded attention page. The successor must read the
/// authoritative projections again; this receipt is a transfer checkpoint,
/// not a mailbox reassignment or a complete native-family snapshot.
const HANDOVER_ATTENTION_LIMIT: i64 = 20;

/// Wake mode exposed in `host.status` until a native push path is qualified.
pub(super) const WAKE_MODE: &str = "checkpoint_poll";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentGm {
    pub(crate) client_id: String,
    pub(crate) epoch: i64,
    binding: Option<GmBindingRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GmBindingRef {
    binding_id: String,
    generation: i64,
}

impl CurrentGm {
    fn facts(&self) -> Value {
        json!({
            "client_id": self.client_id,
            "epoch": self.epoch,
            "binding_id": self.binding.as_ref().map(|binding| &binding.binding_id),
            "binding_generation": self.binding.as_ref().map(|binding| binding.generation),
        })
    }
}

#[derive(Debug)]
enum DesignationState {
    NeverDesignated,
    Current(CurrentGm),
    MissingAfterHistory { high_water: i64 },
    Damaged { high_water: i64 },
    StaleRegistration { current: CurrentGm, high_water: i64 },
}

fn designation_error(code: &str) -> Error {
    Error::new(
        code,
        "GM designation requires verified local Operator recovery",
    )
}

fn bounded_identity(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)?
        .as_str()
        .filter(|text| {
            !text.trim().is_empty() && text.len() <= 128 && !text.chars().any(char::is_control)
        })
        .map(str::to_owned)
}

fn parse_designation(value: &Value) -> Option<CurrentGm> {
    if !value.is_object() {
        return None;
    }
    let client_id = bounded_identity(value, "client_id")?;
    let epoch = value.get("epoch")?.as_i64().filter(|epoch| *epoch > 0)?;
    let binding_id = value.get("binding_id").filter(|value| !value.is_null());
    let generation = value
        .get("binding_generation")
        .filter(|value| !value.is_null());
    let binding = match (binding_id, generation) {
        (None, None) => None,
        (Some(_), Some(generation)) => Some(GmBindingRef {
            binding_id: bounded_identity(value, "binding_id")?,
            generation: generation.as_i64().filter(|generation| *generation > 0)?,
        }),
        _ => return None,
    };
    Some(CurrentGm {
        client_id,
        epoch,
        binding,
    })
}

fn registered_manager(db: &Connection, client_id: &str) -> Result<bool> {
    Ok(
        meta(db, &format!("client:{client_id}"))?.is_some_and(|registration| {
            registration.is_object()
                && registration["role"] == "manager"
                && matches!(
                    registration.get("disabled"),
                    None | Some(Value::Bool(false))
                )
        }),
    )
}

/// Successful immutable handover receipts are the existing durable epoch ledger.
/// A malformed successful receipt is damage, not a row to skip during MAX.
fn handover_epoch_high_water(db: &Connection) -> Result<i64> {
    let (high_water, damaged): (i64, i64) = db.query_row(
        "WITH retained AS (
             SELECT operation_id,
                    CASE WHEN json_valid(result_json) THEN result_json ELSE '{}' END AS result,
                    CASE WHEN json_valid(original_request_json) THEN original_request_json ELSE '{}' END AS request,
                    json_valid(result_json) AND json_valid(original_request_json) AS valid_json
             FROM operations WHERE method='gm.handover' AND state='settled'
         ), history AS (
             SELECT json_extract(result,'$.gm_epoch') AS epoch,
                    COALESCE(valid_json AND json_type(result) = 'object'
                     AND json_type(result,'$.operation_id') = 'text'
                     AND json_extract(result,'$.operation_id') = operation_id
                     AND json_type(result,'$.client_id') = 'text'
                     AND length(trim(json_extract(result,'$.client_id'))) BETWEEN 1 AND 128
                     AND json_type(request,'$.client_id') = 'text'
                     AND json_extract(request,'$.client_id') = json_extract(result,'$.client_id')
                     AND json_type(result,'$.gm_epoch') = 'integer'
                     AND json_extract(result,'$.gm_epoch') > 0, 0) AS valid
             FROM retained
         ) SELECT COALESCE(MAX(CASE WHEN valid=1 THEN epoch END),0),
                  COALESCE(SUM(CASE WHEN valid=1 THEN 0 ELSE 1 END),0) FROM history",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if damaged != 0 {
        return Err(designation_error("GM_EPOCH_HISTORY_DAMAGED"));
    }
    Ok(high_water)
}

fn designation_state(db: &Connection) -> Result<DesignationState> {
    let high_water = handover_epoch_high_water(db)?;
    let Some(value) = meta(db, "gm")? else {
        return Ok(if high_water == 0 {
            DesignationState::NeverDesignated
        } else {
            DesignationState::MissingAfterHistory { high_water }
        });
    };
    let Some(current) = parse_designation(&value) else {
        // A surviving positive integer fence is a conservative recovery
        // floor, never an authority grant. Do not reissue it after damage.
        let high_water = value
            .get("epoch")
            .and_then(Value::as_i64)
            .filter(|epoch| *epoch > 0)
            .map_or(high_water, |epoch| high_water.max(epoch));
        return Ok(DesignationState::Damaged { high_water });
    };
    if current.epoch < high_water {
        return Ok(DesignationState::Damaged { high_water });
    }
    if !registered_manager(db, &current.client_id)? {
        // A structurally valid legacy epoch remains a fence even if its
        // registration was subsequently disabled or removed.
        return Ok(DesignationState::StaleRegistration {
            high_water: high_water.max(current.epoch),
            current,
        });
    }
    Ok(DesignationState::Current(current))
}

pub(crate) fn current(db: &Connection) -> Result<Option<CurrentGm>> {
    match designation_state(db)? {
        DesignationState::NeverDesignated => Ok(None),
        DesignationState::Current(current) => Ok(Some(current)),
        DesignationState::MissingAfterHistory { .. } => {
            Err(designation_error("GM_DESIGNATION_MISSING"))
        }
        DesignationState::Damaged { .. } => Err(designation_error("GM_DESIGNATION_DAMAGED")),
        DesignationState::StaleRegistration { .. } => {
            Err(designation_error("GM_REGISTRATION_STALE"))
        }
    }
}

fn is_designation_damage(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "GM_DESIGNATION_MISSING"
            | "GM_DESIGNATION_DAMAGED"
            | "GM_REGISTRATION_STALE"
            | "GM_EPOCH_HISTORY_DAMAGED"
    )
}

/// Reads may retain their own object grant during GM damage. Damage never
/// supplies the current-GM shortcut; genuine Store failures remain errors.
pub(crate) fn read_current(db: &Connection) -> Result<Option<CurrentGm>> {
    match current(db) {
        Err(error) if is_designation_damage(&error) => Ok(None),
        result => result,
    }
}

pub(crate) fn current_epoch(db: &Connection) -> Result<i64> {
    Ok(current(db)?.map_or(0, |current| current.epoch))
}

pub(crate) fn require_current_manager(db: &Connection, manager_id: &str) -> Result<i64> {
    current(db)?
        .filter(|current| current.client_id == manager_id)
        .map(|current| current.epoch)
        .ok_or_else(|| Error::new("FORBIDDEN", "current registered GM authority required"))
}

pub(crate) fn authority_facts(db: &Connection) -> Result<Value> {
    Ok(current(db)?.map_or(Value::Null, |current| current.facts()))
}

pub(super) fn status(db: &Connection) -> Result<Value> {
    let state = match designation_state(db) {
        Err(error) if is_designation_damage(&error) => {
            return Ok(json!({"state":"damaged", "error_code":error.code,"epoch_high_water":null}));
        }
        state => state?,
    };
    Ok(match state {
        DesignationState::NeverDesignated => json!({"state":"none","client_id":null,"epoch":0}),
        DesignationState::Current(current) => {
            let mut facts = current.facts();
            // Continuity checkpoints are diagnostics, not authority. Preserve
            // the retained handover context in status while identity and fence
            // always come from the validated designation above.
            if let Some(retained) = meta(db, "gm")? {
                for field in [
                    "handover_operation_id",
                    "previous_client_id",
                    "previous_gm_epoch",
                    "previous_binding_id",
                    "previous_binding_generation",
                    "authority_changed",
                    "session_binding_changed",
                    "designation_recovered",
                    "recovery_reason",
                    "epoch_high_water_before",
                    "resync",
                ] {
                    if let Some(value) = retained.get(field) {
                        facts[field] = value.clone();
                    }
                }
            }
            facts["state"] = json!("current");
            facts
        }
        DesignationState::MissingAfterHistory { high_water } => {
            json!({"state":"missing_after_history","error_code":"GM_DESIGNATION_MISSING","epoch_high_water":high_water})
        }
        DesignationState::Damaged { high_water } => {
            json!({"state":"damaged","error_code":"GM_DESIGNATION_DAMAGED","epoch_high_water":high_water})
        }
        DesignationState::StaleRegistration { high_water, .. } => {
            json!({"state":"stale_registration","error_code":"GM_REGISTRATION_STALE","epoch_high_water":high_water})
        }
    })
}

#[cfg(test)]
pub(super) fn record(db: &Connection) -> Result<Option<Value>> {
    meta(db, "gm")
}

/// Authority for GM-only operations: the local operator, or the client that
/// currently holds the GM designation. Checks happen both at admission and at
/// dispatch/begin, so a principal rotation between the two revokes the old GM.
pub(super) fn require_authority(db: &Connection, p: &Principal) -> Result<()> {
    let principal = super::current_principal(db, p.clone())?;
    match principal.role {
        Role::Operator => super::require_local_operator(db, &principal.client_id),
        Role::Manager => require_current_manager(db, &principal.client_id).map(|_| ()),
        _ => Err(Error::new(
            "FORBIDDEN",
            "GM or local Operator authority required",
        )),
    }
}

/// Admit control of an Attempt by its original owner or the verified local
/// Operator. A different Manager may control it only while they are the
/// current GM and it is still the exact unreleased Attempt of the current
/// Task revision and project.
pub(super) fn require_attempt_control(
    db: &Connection,
    p: &Principal,
    attempt: &Value,
) -> Result<()> {
    let current = super::current_principal(db, p.clone())?;
    let attempt_id = model::text(attempt, "attempt_id")?;
    let attempt = super::tasks::get_attempt(db, attempt_id)?;
    let owner_id = model::text(&attempt, "owner_id")?;
    if current.owns(owner_id).is_ok() {
        return Ok(());
    }
    if current.role != Role::Manager {
        return Err(Error::new(
            "FORBIDDEN",
            "attempt control requires its owner or the current GM",
        ));
    }
    require_authority(db, &current)?;

    let task_id = model::text(&attempt, "task_id")?;
    let task = super::tasks::get_task(db, task_id)?;
    if task["current_attempt_id"] != attempt_id
        || !attempt["released_at_ms"].is_null()
        || task["revision"] != attempt["task_revision"]
    {
        return Err(Error::new(
            "ATTEMPT_SCOPE_STALE",
            "current GM control requires the exact current unreleased Attempt and Task revision",
        ));
    }
    let project_id = model::text(&task, "project_id")?;
    if !crate::automation::authorization::current_manager_has_task_scope(
        db, &current, task_id, project_id,
    )? {
        return Err(Error::new(
            "FORBIDDEN",
            "current GM lacks scope for this Task and project",
        ));
    }
    Ok(())
}

fn mailbox_watermark(db: &Connection, client: Option<&str>) -> Result<i64> {
    let Some(client) = client else {
        return Ok(0);
    };
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations WHERE kind IN ('message.send','task.feedback','check.completed') AND json_extract(payload_json,'$.recipient')=?1",
        [client],
        |row| row.get(0),
    )?)
}

fn resync_checkpoint(db: &Connection, previous: Option<&Value>, successor: &str) -> Result<Value> {
    let report_cursor: i64 = db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations",
        [],
        |row| row.get(0),
    )?;
    let attention = super::capacity::attention_report(db, HANDOVER_ATTENTION_LIMIT, 0)?;
    let attention_digest = model::digest(model::canonical(&attention)?.as_bytes());
    Ok(json!({
        "report_watermark": report_cursor,
        "previous_mailbox_watermark": mailbox_watermark(db, previous.and_then(|gm| gm["client_id"].as_str()))?,
        "successor_mailbox_watermark": mailbox_watermark(db, Some(successor))?,
        // Consumers own their read cursors outside the host. A latest
        // observation is not acknowledgement of existing/unread mail.
        "watermarks_are_acknowledgements": false,
        "resync_after": {"report": 0, "successor_mailbox": 0},
        "attention_snapshot": attention,
        "attention_snapshot_digest": attention_digest,
        "resync_required": true,
        "authoritative_reads": ["report.delta", "message.read", "report.attention"],
        "historical_mail_reassigned": false,
        "wake_mode": WAKE_MODE,
    }))
}

/// Designate a registered client as the current GM, optionally naming the
/// native binding its GM session runs on. The client principal defines GM
/// authority; binding changes update only the session pointer and preserve
/// the epoch.
pub(super) fn handover(tx: &Transaction<'_>, p: &Principal, v: &Value, id: &str) -> Result<Value> {
    require_authority(tx, p)?;
    model::fields(
        v,
        &[
            "client_request_id",
            "client_id",
            "binding_id",
            "binding_generation",
        ],
    )?;
    let client = model::text(v, "client_id")?;
    if bounded_identity(v, "client_id").is_none() {
        return Err(Error::invalid(
            "GM client identity must be bounded nonempty text",
        ));
    }
    let target = meta(tx, &format!("client:{client}"))?
        .ok_or_else(|| Error::new("NOT_FOUND", "GM client is not registered"))?;
    if target["disabled"] == true {
        return Err(Error::new("UNAUTHORIZED", "GM client is disabled"));
    }
    if !registered_manager(tx, client)? {
        return Err(Error::new(
            "FORBIDDEN",
            "GM target must be an enabled registered Manager",
        ));
    }
    let (binding_id, binding_generation) = match (v.get("binding_id"), v.get("binding_generation"))
    {
        (None, None) => (Value::Null, Value::Null),
        (Some(_), Some(_)) => {
            if bounded_identity(v, "binding_id").is_none() {
                return Err(Error::invalid(
                    "GM binding identity must be bounded nonempty text",
                ));
            }
            let binding = operations::get_binding(
                tx,
                model::text(v, "binding_id")?,
                model::positive(v, "binding_generation")?,
            )?;
            if !binding["released_at_ms"].is_null() {
                return Err(Error::new(
                    "BINDING_CLOSED",
                    "GM binding is already released",
                ));
            }
            (v["binding_id"].clone(), v["binding_generation"].clone())
        }
        _ => {
            return Err(Error::invalid(
                "binding_id and binding_generation must be given together",
            ));
        }
    };
    let designation = designation_state(tx)?;
    let (previous, epoch_high_water_before, designation_recovered, recovery_reason) =
        match designation {
            DesignationState::NeverDesignated => (None, 0, false, None),
            DesignationState::Current(current) => {
                let epoch = current.epoch;
                (Some(current.facts()), epoch, false, None)
            }
            DesignationState::MissingAfterHistory { high_water } => {
                (None, high_water, true, Some("missing_after_history"))
            }
            DesignationState::Damaged { high_water } => {
                if high_water == 0 {
                    return Err(designation_error("GM_EPOCH_HISTORY_UNAVAILABLE"));
                }
                (None, high_water, true, Some("damaged_current"))
            }
            DesignationState::StaleRegistration {
                current,
                high_water,
            } => (
                Some(current.facts()),
                high_water,
                true,
                Some("stale_registration"),
            ),
        };
    if designation_recovered {
        // require_authority above permits only the independently refreshed
        // local Operator to reach recovery; no damaged client ID is a grant.
        super::require_local_operator(tx, &p.client_id)?;
    }
    if let Some(prev) = &previous
        && !designation_recovered
        && prev["client_id"] == client
        && prev["binding_id"] == binding_id
        && prev["binding_generation"] == binding_generation
    {
        return Err(Error::conflict("GM designation is unchanged"));
    }
    let authority_changed =
        designation_recovered || previous.as_ref().is_none_or(|gm| gm["client_id"] != client);
    let session_binding_changed = match &previous {
        Some(gm) => {
            gm["binding_id"] != binding_id || gm["binding_generation"] != binding_generation
        }
        None => !binding_id.is_null(),
    };
    let epoch = if authority_changed {
        epoch_high_water_before
            .checked_add(1)
            .ok_or_else(|| Error::new("EPOCH_OVERFLOW", "GM epoch exhausted"))?
    } else {
        epoch_high_water_before
    };
    let previous_binding_id = previous
        .as_ref()
        .map_or(Value::Null, |gm| gm["binding_id"].clone());
    let previous_binding_generation = previous
        .as_ref()
        .map_or(Value::Null, |gm| gm["binding_generation"].clone());
    // Captured in the same transaction as the designation. Watermarks name
    // committed facts before the handover observation. They never advance a
    // consumer cursor or discard older, potentially unread historical mail.
    let resync = resync_checkpoint(tx, previous.as_ref(), client)?;
    let designation = json!({
        "client_id": client,
        "binding_id": binding_id,
        "binding_generation": binding_generation,
        "epoch": epoch,
        "handover_operation_id": id,
        "previous_client_id": previous.as_ref().map_or(Value::Null, |gm| gm["client_id"].clone()),
        "previous_gm_epoch": previous.as_ref().map_or(Value::Null, |gm| gm["epoch"].clone()),
        "previous_binding_id": previous_binding_id,
        "previous_binding_generation": previous_binding_generation,
        "authority_changed": authority_changed,
        "designation_recovered": designation_recovered,
        "recovery_reason": recovery_reason,
        "epoch_high_water_before": epoch_high_water_before,
        "session_binding_changed": session_binding_changed,
        "resync": resync,
    });
    set_meta(tx, "gm", &designation)?;
    Ok(json!({
        "operation_id": id,
        "client_id": client,
        "binding_id": binding_id,
        "binding_generation": binding_generation,
        "gm_epoch": epoch,
        "previous_client_id": previous.as_ref().map_or(Value::Null, |gm| gm["client_id"].clone()),
        "previous_gm_epoch": previous.as_ref().map_or(Value::Null, |gm| gm["epoch"].clone()),
        "previous_binding_id": previous_binding_id,
        "previous_binding_generation": previous_binding_generation,
        "authority_changed": authority_changed,
        "designation_recovered": designation_recovered,
        "recovery_reason": recovery_reason,
        "epoch_high_water_before": epoch_high_water_before,
        "session_binding_changed": session_binding_changed,
        "resync": resync,
        "manager_tasks_automatically_cancelled": false,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    fn database() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        for client in ["old", "new", "owner", "unrelated"] {
            set_meta(
                &db,
                &format!("client:{client}"),
                &json!({"role":"manager","disabled":false}),
            )
            .unwrap();
        }
        db
    }

    fn binding(db: &Connection, id: &str, lane: &str) {
        db.execute(
            "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
             VALUES(?1,1,?2,'test-module','test-artifact','ready','{}','{}',1)",
            params![id, lane],
        )
        .unwrap();
    }

    fn attempt(db: &Connection, attempt_id: &str, owner_id: &str) -> Value {
        db.execute(
            "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
             VALUES('task-1','project-1',1,'open','{}',1,1)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,created_at_ms,updated_at_ms) \
             VALUES(?1,'task-1',1,'{}',?2,'controller','running','[]',1,1)",
            params![attempt_id, owner_id],
        )
        .unwrap();
        super::super::tasks::get_attempt(db, attempt_id).unwrap()
    }

    fn principal(client: &str, role: Role) -> Principal {
        Principal {
            link_id: "test-link".into(),
            client_id: client.into(),
            role,
        }
    }

    fn observation(db: &Connection, recipient: &str) -> i64 {
        db.execute(
            "INSERT INTO observations(source_stream_id,kind,payload_json,recorded_at_ms) VALUES('test-mail','message.send',?1,1)",
            params![model::canonical(&json!({"recipient":recipient,"text":"historical"})).unwrap()],
        ).unwrap();
        db.last_insert_rowid()
    }

    #[test]
    fn handover_freezes_distinct_watermarks_without_reassigning_mail() {
        let mut db = database();
        set_meta(
            &db,
            "gm",
            &json!({"client_id":"old","binding_id":null,"binding_generation":null,"epoch":3}),
        )
        .unwrap();
        let old_cursor = observation(&db, "old");
        let new_cursor = observation(&db, "new");
        let report_cursor = observation(&db, "unrelated");
        let tx = db.transaction().unwrap();
        let receipt = handover(
            &tx,
            &principal("old", Role::Manager),
            &json!({"client_id":"new"}),
            "handover-1",
        )
        .unwrap();
        assert_eq!(receipt["gm_epoch"], 4);
        assert_eq!(receipt["resync"]["report_watermark"], report_cursor);
        assert_eq!(receipt["resync"]["previous_mailbox_watermark"], old_cursor);
        assert_eq!(receipt["resync"]["successor_mailbox_watermark"], new_cursor);
        assert_eq!(receipt["resync"]["watermarks_are_acknowledgements"], false);
        assert_eq!(receipt["resync"]["resync_after"]["successor_mailbox"], 0);
        assert_eq!(receipt["resync"]["historical_mail_reassigned"], false);
        assert_eq!(receipt["resync"]["wake_mode"], WAKE_MODE);
        assert_eq!(
            receipt["resync"]["attention_snapshot_digest"],
            model::digest(
                model::canonical(&receipt["resync"]["attention_snapshot"])
                    .unwrap()
                    .as_bytes()
            )
        );
        assert_eq!(record(&tx).unwrap().unwrap()["resync"], receipt["resync"]);
        assert_eq!(
            require_authority(&tx, &principal("old", Role::Manager))
                .unwrap_err()
                .code,
            "FORBIDDEN"
        );
        assert!(require_authority(&tx, &principal("new", Role::Manager)).is_ok());
        tx.commit().unwrap();
        let still_old: String = db.query_row("SELECT json_extract(payload_json,'$.recipient') FROM observations WHERE observation_id=?1", [old_cursor], |row| row.get(0)).unwrap();
        assert_eq!(still_old, "old");
    }

    #[test]
    fn successor_cannot_self_claim_or_change_designation_by_reading() {
        let mut db = database();
        let original =
            json!({"client_id":"old","binding_id":null,"binding_generation":null,"epoch":7});
        set_meta(&db, "gm", &original).unwrap();
        let tx = db.transaction().unwrap();
        let error = handover(
            &tx,
            &principal("new", Role::Manager),
            &json!({"client_id":"new"}),
            "bad-self-claim",
        )
        .unwrap_err();
        assert_eq!(error.code, "FORBIDDEN");
        assert_eq!(record(&tx).unwrap().unwrap(), original);
        let readback = resync_checkpoint(&tx, Some(&original), "new").unwrap();
        assert_eq!(readback["report_watermark"], 0);
        assert_eq!(record(&tx).unwrap().unwrap(), original);
    }

    #[test]
    fn same_client_binding_changes_preserve_gm_epoch_but_principal_rotation_advances_it() {
        let mut db = database();
        binding(&db, "gm-session-old", "lane-old");
        binding(&db, "gm-session-new", "lane-new");
        set_meta(
            &db,
            "gm",
            &json!({
                "client_id":"old",
                "binding_id":"gm-session-old",
                "binding_generation":1,
                "epoch":7
            }),
        )
        .unwrap();

        let tx = db.transaction().unwrap();
        let rebind = handover(
            &tx,
            &principal("old", Role::Manager),
            &json!({
                "client_id":"old",
                "binding_id":"gm-session-new",
                "binding_generation":1
            }),
            "same-client-rebind",
        )
        .unwrap();
        assert_eq!(rebind["gm_epoch"], 7);
        assert_eq!(rebind["authority_changed"], false);
        assert_eq!(rebind["session_binding_changed"], true);
        assert_eq!(rebind["previous_binding_id"], "gm-session-old");
        assert_eq!(rebind["previous_binding_generation"], 1);
        assert_eq!(rebind["resync"]["resync_required"], true);
        assert_eq!(record(&tx).unwrap().unwrap()["epoch"], 7);
        assert!(require_authority(&tx, &principal("old", Role::Manager)).is_ok());

        let unchanged = handover(
            &tx,
            &principal("old", Role::Manager),
            &json!({
                "client_id":"old",
                "binding_id":"gm-session-new",
                "binding_generation":1
            }),
            "same-client-same-session",
        )
        .unwrap_err();
        assert_eq!(unchanged.code, "CONFLICT");

        let detach = handover(
            &tx,
            &principal("old", Role::Manager),
            &json!({"client_id":"old"}),
            "same-client-detach",
        )
        .unwrap();
        assert_eq!(detach["gm_epoch"], 7);
        assert_eq!(detach["authority_changed"], false);
        assert_eq!(detach["session_binding_changed"], true);
        assert_eq!(detach["previous_binding_id"], "gm-session-new");
        assert_eq!(detach["previous_binding_generation"], 1);
        assert_eq!(record(&tx).unwrap().unwrap()["epoch"], 7);

        let rotate = handover(
            &tx,
            &principal("old", Role::Manager),
            &json!({"client_id":"new"}),
            "different-client-handover",
        )
        .unwrap();
        assert_eq!(rotate["gm_epoch"], 8);
        assert_eq!(rotate["authority_changed"], true);
        assert_eq!(rotate["session_binding_changed"], false);
        assert_eq!(rotate["previous_client_id"], "old");
        assert_eq!(rotate["previous_gm_epoch"], 7);
        assert!(require_authority(&tx, &principal("old", Role::Manager)).is_err());
        assert!(require_authority(&tx, &principal("new", Role::Manager)).is_ok());
        tx.commit().unwrap();
    }

    #[test]
    fn attempt_control_requires_current_gm_scope_and_exact_live_attempt() {
        let mut db = database();
        let attempt = attempt(&db, "attempt-1", "owner");
        set_meta(
            &db,
            "gm",
            &json!({"client_id":"old","binding_id":null,"binding_generation":null,"epoch":1}),
        )
        .unwrap();
        let tx = db.transaction().unwrap();
        handover(
            &tx,
            &principal("old", Role::Manager),
            &json!({"client_id":"new"}),
            "promote-successor",
        )
        .unwrap();
        assert!(require_attempt_control(&tx, &principal("new", Role::Manager), &attempt).is_ok());
        assert_eq!(
            require_attempt_control(&tx, &principal("unrelated", Role::Manager), &attempt)
                .unwrap_err()
                .code,
            "FORBIDDEN"
        );
        // Existing owner authority is preserved even when delegated GM scope
        // later becomes stale.
        assert!(require_attempt_control(&tx, &principal("owner", Role::Manager), &attempt).is_ok());
        tx.commit().unwrap();

        db.execute("UPDATE tasks SET revision=2 WHERE task_id='task-1'", [])
            .unwrap();
        assert_eq!(
            require_attempt_control(&db, &principal("new", Role::Manager), &attempt)
                .unwrap_err()
                .code,
            "ATTEMPT_SCOPE_STALE"
        );
        assert!(require_attempt_control(&db, &principal("owner", Role::Manager), &attempt).is_ok());

        db.execute("UPDATE tasks SET revision=1 WHERE task_id='task-1'", [])
            .unwrap();
        db.execute(
            "UPDATE attempts SET state='failed',released_at_ms=2 WHERE attempt_id='attempt-1'",
            [],
        )
        .unwrap();
        let released = super::super::tasks::get_attempt(&db, "attempt-1").unwrap();
        assert_eq!(
            require_attempt_control(&db, &principal("new", Role::Manager), &released)
                .unwrap_err()
                .code,
            "ATTEMPT_SCOPE_STALE"
        );
        assert!(
            require_attempt_control(&db, &principal("owner", Role::Manager), &released).is_ok()
        );
    }
}
