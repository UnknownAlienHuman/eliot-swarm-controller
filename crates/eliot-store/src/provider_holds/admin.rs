use super::super::{Error, Result, sql_error};
use eliot_core::{
    ProviderHold, ProviderHoldState, ProviderRestrictionKind,
    api::{ProviderHoldsRequest, ProviderHoldsResponse},
};
use rusqlite::{Connection, OptionalExtension, Row, Transaction, params};

type RawHold = (String, String, Option<i64>, Option<String>, String, i64);

pub(super) fn get(connection: &Connection, scope: &str) -> Result<Option<ProviderHold>> {
    let raw = connection
        .query_row(
            "SELECT scope, kind, retry_after, probe_command, state, updated_at
             FROM provider_holds WHERE scope = ?1",
            [scope],
            raw_hold_from_row,
        )
        .optional()
        .map_err(sql_error)?;
    raw.map(hold_from_raw).transpose()
}

pub(super) fn list_page(
    connection: &Connection,
    after: Option<&str>,
    limit: usize,
) -> Result<ProviderHoldsResponse> {
    let request = ProviderHoldsRequest {
        after: after.map(str::to_owned),
        limit: Some(limit),
    };
    request.validate()?;
    let sql_limit = i64::try_from(request.effective_limit())
        .map_err(|_| Error::new("invalid", "provider hold page limit is invalid"))?;

    let mut statement = connection
        .prepare(
            "SELECT scope, kind, retry_after, probe_command, state, updated_at
             FROM provider_holds
             WHERE (?1 IS NULL OR scope > ?1)
             ORDER BY scope
             LIMIT ?2",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![after, sql_limit], raw_hold_from_row)
        .map_err(sql_error)?;
    let mut holds = Vec::new();
    for row in rows {
        holds.push(hold_from_raw(row.map_err(sql_error)?)?);
    }

    let response = ProviderHoldsResponse {
        next: holds.last().map(|hold| hold.scope.clone()),
        holds,
    };
    request.validate_response(&response)?;
    Ok(response)
}

pub(super) fn clear_hold_tx(
    transaction: &Transaction<'_>,
    scope: &str,
    expected_probe: Option<&str>,
) -> Result<bool> {
    let Some(hold) = get(transaction, scope)? else {
        return if expected_probe.is_none() {
            Ok(false)
        } else {
            Err(Error::new(
                "bad_state",
                "provider hold probe binding is no longer present",
            ))
        };
    };

    if hold.state == ProviderHoldState::Probing && hold.probe_command.is_some() {
        return Err(Error::new("bad_state", "provider hold probe is unresolved"));
    }
    if hold.probe_command.as_deref() != expected_probe {
        return Err(Error::new(
            "bad_state",
            "provider hold probe binding changed",
        ));
    }

    let changed = transaction
        .execute(
            "DELETE FROM provider_holds
             WHERE scope = ?1 AND state = 'held' AND probe_command IS NULL",
            [scope],
        )
        .map_err(sql_error)?;
    Ok(changed == 1)
}

fn raw_hold_from_row(row: &Row<'_>) -> rusqlite::Result<RawHold> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}

fn hold_from_raw(raw: RawHold) -> Result<ProviderHold> {
    let (scope, kind, retry_after, probe_command, state, updated_at) = raw;
    let kind = match kind.as_str() {
        "rate_limit" => ProviderRestrictionKind::RateLimit,
        "quota" => ProviderRestrictionKind::Quota,
        "auth" => ProviderRestrictionKind::Auth,
        "model_removed" => ProviderRestrictionKind::ModelRemoved,
        _ => return Err(Error::new("internal", "unknown provider hold kind")),
    };
    let state = match state.as_str() {
        "held" => ProviderHoldState::Held,
        "probing" => ProviderHoldState::Probing,
        _ => return Err(Error::new("internal", "unknown provider hold state")),
    };
    let hold = ProviderHold {
        scope,
        kind,
        retry_after,
        probe_command,
        state,
        updated_at,
    };
    hold.validate()
        .map_err(|_| Error::new("internal", "provider hold row is invalid"))?;
    Ok(hold)
}
