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

/// The current designation record, or `None` before the first handover. With
/// no designation, GM-only authority rests with the local operator alone.
pub(super) fn record(db: &Connection) -> Result<Option<Value>> {
    meta(db, "gm")
}

/// Authority for GM-only operations: the local operator, or the client that
/// currently holds the GM designation. Checks happen both at admission and at
/// dispatch/begin, so a rotation between the two revokes the old GM there.
pub(super) fn require_authority(db: &Connection, p: &Principal) -> Result<()> {
    if p.role == Role::Operator {
        return Ok(());
    }
    if let Some(gm) = record(db)?
        && gm["client_id"] == p.client_id
    {
        return Ok(());
    }
    Err(Error::new("FORBIDDEN", "GM or operator authority required"))
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
/// native binding its GM session runs on. The epoch advances by exactly one
/// per actual rotation and never otherwise moves.
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
    let target = meta(tx, &format!("client:{client}"))?
        .ok_or_else(|| Error::new("NOT_FOUND", "GM client is not registered"))?;
    if target["disabled"] == true {
        return Err(Error::new("UNAUTHORIZED", "GM client is disabled"));
    }
    let role: Role = serde_json::from_value(target["role"].clone())?;
    if matches!(role, Role::Module | Role::Scheduler) {
        return Err(Error::new(
            "FORBIDDEN",
            "module and internal scheduler principals cannot become GM",
        ));
    }
    let (binding_id, binding_generation) = match (v.get("binding_id"), v.get("binding_generation"))
    {
        (None, None) => (Value::Null, Value::Null),
        (Some(_), Some(_)) => {
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
    let previous = record(tx)?;
    if let Some(prev) = &previous
        && prev["client_id"] == client
        && prev["binding_id"] == binding_id
        && prev["binding_generation"] == binding_generation
    {
        return Err(Error::conflict("GM designation is unchanged"));
    }
    let epoch = previous
        .as_ref()
        .and_then(|gm| gm["epoch"].as_i64())
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Error::new("EPOCH_OVERFLOW", "GM epoch exhausted"))?;
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
        for client in ["old", "new"] {
            set_meta(
                &db,
                &format!("client:{client}"),
                &json!({"role":"manager","disabled":false}),
            )
            .unwrap();
        }
        db
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
}
