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
    if role == Role::Module {
        return Err(Error::new(
            "FORBIDDEN",
            "module credentials cannot become GM",
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
    let designation = json!({
        "client_id": client,
        "binding_id": binding_id,
        "binding_generation": binding_generation,
        "epoch": epoch,
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
        "manager_tasks_automatically_cancelled": false,
    }))
}
