use super::{Config, Error, Principal, Result, Role};
use crate::model;
use rusqlite::Connection;
use serde_json::{Value, json};

const JOURNAL_STREAM: &str = "observations";
const CURSOR_SPACE: &str = "observation_id";

/// The Manager monitor is read-only and runs on the existing status-reader
/// transaction. A Manager is identified by its registered client identity;
/// current GM authority is deliberately not consulted for this view.
fn require_manager(db: &Connection, p: &Principal) -> Result<()> {
    match p.role {
        Role::Operator => super::require_local_operator(db, &p.client_id),
        Role::Manager => Ok(()),
        _ => Err(Error::new("FORBIDDEN", "manager authority required")),
    }
}

fn journal_cut(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations",
        [],
        |row| row.get(0),
    )?)
}

fn visibility_scope(p: &Principal) -> &'static str {
    match p.role {
        Role::Operator => "local_operator_visibility",
        Role::Manager => "authenticated_manager_visibility",
        _ => "unavailable",
    }
}

pub(super) fn snapshot(
    db: &Connection,
    p: &Principal,
    params: &Value,
    _config: &Config,
) -> Result<Value> {
    require_manager(db, p)?;
    model::fields(params, &["limit"])?;
    let scope = visibility_scope(p);

    // The first read establishes the deferred reader transaction's snapshot.
    // All state projections and the cut below therefore describe one committed
    // view; writes racing this call receive observation IDs after this cut.
    let cut = journal_cut(db)?;
    let host = super::read(db, p, "host.status", &json!({}), _config)?;
    let dashboard = super::launcher::dashboard(db, p, params)?;
    let captured_at_ms = model::now_ms()?;

    Ok(json!({
        "schema_version": 1,
        "captured_at_ms": captured_at_ms,
        "state": {
            "host": host,
            "dashboard": dashboard,
        },
        "journal_cut": {
            "stream": JOURNAL_STREAM,
            "cursor_space": CURSOR_SPACE,
            "cursor": cut,
            "scope": scope,
            "captured_in_read_transaction": true,
        },
        "coverage": {
            "scope": scope,
            "state": "captured_transaction",
            "native": "retained_observations_only",
        },
        "retention": {
            "status": "durable_store",
            "cursor_expired": false,
            "policy": "no_age_or_byte_expiry_for_report_journal",
            "gap_recovery": "monitor.snapshot",
        },
        "continuation": {
            "method": "monitor.follow",
            "after": cut,
            "recovery": "read_retained_visible_observations_after_journal_cut",
        },
    }))
}

pub(super) fn follow(
    db: &Connection,
    p: &Principal,
    params: &Value,
    config: &Config,
) -> Result<Value> {
    require_manager(db, p)?;
    model::fields(params, &["after", "limit"])?;
    let scope = visibility_scope(p);
    let (limit, after) = super::page(params)?;
    let request = json!({"after": after, "limit": limit});
    let mut page = super::read(db, p, "report.delta", &request, config)?;
    let cut = journal_cut(db)?;
    let projection = page
        .get("projection")
        .cloned()
        .ok_or_else(|| Error::new("STORE_MONITOR_INVALID", "report projection is missing"))?;
    let has_newer = projection
        .get("has_newer")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let has_older = projection
        .get("has_older")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let coverage_complete = projection
        .get("coverage_complete")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let gap_count = projection
        .get("gap_count")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let next_cursor = page
        .get("next_cursor")
        .and_then(Value::as_i64)
        .unwrap_or(after);
    let cursor_ahead = after > cut;
    let lag_state = if cursor_ahead {
        "ahead_of_cut"
    } else if has_newer {
        "behind"
    } else {
        "current"
    };
    let current_state = if cursor_ahead {
        "unknown"
    } else if has_newer {
        "behind"
    } else {
        "current"
    };

    let mut gaps = Vec::new();
    if cursor_ahead {
        gaps.push(json!({
            "kind": "cursor_ahead_of_cut",
            "after": after,
            "cut": cut,
            "recovery": "monitor.snapshot",
        }));
    }
    if gap_count > 0 {
        gaps.push(json!({
            "kind": "projection_gap",
            "count": gap_count,
            "reason": projection.get("gap_reason").cloned().unwrap_or(Value::Null),
            "recovery": "read_the_retained_detached_reference",
        }));
    }

    let monitor = json!({
        "journal": {
            "stream": JOURNAL_STREAM,
            "cursor_space": CURSOR_SPACE,
            "after": after,
            "next_cursor": next_cursor,
            "cut": cut,
            "scope": scope,
            "captured_in_read_transaction": true,
        },
        "retention": {
            "status": "durable_store",
            "cursor_expired": false,
            "policy": "no_age_or_byte_expiry_for_report_journal",
            "gap_recovery": "monitor.snapshot",
        },
        "lag": {
            "state": lag_state,
            "has_newer": has_newer,
        },
        "coverage": {
            "scope": scope,
            "page": if coverage_complete { "complete" } else { "partial" },
            "current": current_state,
            "has_older": has_older,
            "has_newer": has_newer,
        },
        "gap": {
            "present": !gaps.is_empty(),
            "count": gaps.len(),
            "reason": if cursor_ahead {
                Value::String("cursor_ahead_of_cut".into())
            } else {
                projection.get("gap_reason").cloned().unwrap_or(Value::Null)
            },
            "recovery": "monitor.snapshot",
        },
        "gaps": gaps,
    });
    page.as_object_mut()
        .ok_or_else(|| Error::new("STORE_MONITOR_INVALID", "report page is not an object"))?
        .insert("monitor".into(), monitor);
    Ok(page)
}
