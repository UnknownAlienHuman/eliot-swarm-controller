//! Compact, retention-coupled facts for the context portion of usage summaries.
use crate::{Result, encode_json, sql_error};
use eliot_core::{Error, SessionUsage};
use rusqlite::{Transaction, params};
use serde::Deserialize;
use serde_json::Value;

const MISSING: &str = "retained session usage event has no usage";
const MALFORMED: &str = "retained session usage data is malformed";

/// Backfill using the same JSON-path extraction as the former read path.
/// Retain malformed results as read errors, so an unrelated row cannot block
/// Store open and a scope containing the row still fails in receipt order.
pub(crate) fn backfill(tx: &Transaction<'_>) -> Result<()> {
    tx.execute_batch(
        "CREATE TABLE usage_context_observations (
             seq INTEGER PRIMARY KEY REFERENCES events(seq) ON DELETE CASCADE,
             agent INTEGER NOT NULL,
             generation INTEGER NOT NULL,
             ts INTEGER NOT NULL,
             tokens INTEGER,
             error TEXT
         );
         CREATE INDEX usage_context_observation_pairs
           ON usage_context_observations(agent, generation);",
    )
    .map_err(sql_error)?;
    let mut read = tx
        .prepare(
            "SELECT seq, agent, generation, ts, json_type(data, '$.usage'),
                    json_extract(data, '$.usage')
             FROM events INDEXED BY events_kind
             WHERE kind='session.usage' AND agent IS NOT NULL AND generation IS NOT NULL",
        )
        .map_err(sql_error)?;
    let mut rows = read.query([]).map_err(sql_error)?;
    while let Some(row) = rows.next().map_err(sql_error)? {
        let kind: Option<String> = row.get(4).map_err(sql_error)?;
        let usage = match kind.as_deref() {
            None => Err(Error::new("internal", MISSING)),
            Some("object" | "array") => serde_json::from_str::<SessionUsage>(
                row.get_ref(5)
                    .map_err(sql_error)?
                    .as_str()
                    .map_err(|_| Error::new("internal", MALFORMED))?,
            )
            .map_err(|_| Error::new("internal", MALFORMED)),
            Some(_) => Err(Error::new("internal", MALFORMED)),
        };
        insert(
            tx,
            row.get(0).map_err(sql_error)?,
            row.get(1).map_err(sql_error)?,
            row.get(2).map_err(sql_error)?,
            row.get(3).map_err(sql_error)?,
            usage,
        )?;
    }
    Ok(())
}

pub(crate) fn record(
    tx: &Transaction<'_>,
    seq: i64,
    agent: i64,
    generation: i64,
    ts: i64,
    data: &Value,
) -> Result<()> {
    let usage = match data.get("usage") {
        None => Err(Error::new("internal", MISSING)),
        Some(value @ (Value::Object(_) | Value::Array(_))) => {
            SessionUsage::deserialize(value).map_err(|_| Error::new("internal", MALFORMED))
        }
        Some(_) => Err(Error::new("internal", MALFORMED)),
    };
    insert(tx, seq, agent, generation, ts, usage)
}

fn insert(
    tx: &Transaction<'_>,
    seq: i64,
    agent: i64,
    generation: i64,
    ts: i64,
    usage: Result<SessionUsage>,
) -> Result<()> {
    let (tokens, error) = match usage.and_then(|usage| {
        usage.validate()?;
        Ok(usage.valid_context_used_tokens())
    }) {
        Ok(tokens) => (tokens, None),
        Err(error) => (None, Some(encode_json(&error)?)),
    };
    tx.prepare_cached(
        "INSERT INTO usage_context_observations(seq, agent, generation, ts, tokens, error)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .map_err(sql_error)?
    .execute(params![seq, agent, generation, ts, tokens, error])
    .map_err(sql_error)?;
    Ok(())
}
