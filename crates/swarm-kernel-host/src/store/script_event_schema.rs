//! Digest-pinned migration for event-only script invocations.
//!
//! The deployed O6 script schema remains the authority for script bundles and
//! runs.  This additive migration changes only the Task/Attempt scope tuple on
//! `script_runs`; it does not add a second run table or runner.  On every Store
//! open, the installer compares the live 004/009 schema with a same-SQLite
//! reference schema and checks the corresponding PRAGMA metadata.

use super::{meta, set_meta};
use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, Transaction};
use serde_json::json;

const SCRIPT_SCHEMA: &str = include_str!("../../migrations/004_scripts.sql");
const EVENT_SCOPE_SCHEMA: &str = include_str!("../../migrations/009_script_event_scope.sql");
const EVENT_SCOPE_MARKER: &str = "schema_extension:scripts:event_scope:v1";
const SCRIPT_SCOPE_TABLE: &str = "script_runs";
const SCRIPT_TABLES: [&str; 3] = ["scripts", "script_revisions", SCRIPT_SCOPE_TABLE];

type TableColumnShape = (i64, String, String, i64, Option<String>, i64, i64);
type ForeignKeyShape = (
    i64,
    i64,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
);
type IndexListShape = (String, i64, String, i64);
type IndexColumnShape = (i64, i64, Option<String>, i64, String, i64);
type SchemaObjectShape = (String, String, String, Option<String>);

#[derive(Debug, PartialEq, Eq)]
enum DdlToken {
    Keyword(String),
    Identifier(String),
    StringLiteral(String),
    Number(String),
    Operator(String),
    Punctuation(char),
}

/// Install or verify the event-scope migration in the caller's Store
/// transaction. The immutable 004 marker is checked first; the live 004
/// tables are then revalidated before either accepting or applying 009.
pub(super) fn install(tx: &Transaction<'_>) -> Result<()> {
    require_digest(
        tx,
        "schema_extension:scripts:v1",
        SCRIPT_SCHEMA,
        "scripts:v1",
    )?;
    let expected = json!(model::digest(EVENT_SCOPE_SCHEMA.as_bytes()));
    match meta(tx, EVENT_SCOPE_MARKER)? {
        Some(digest) if digest == expected => verify_shape(tx, true),
        Some(_) => Err(Error::new(
            "SCHEMA_MISMATCH",
            "script event-scope migration content differs",
        )),
        None => {
            verify_shape(tx, false)?;
            ensure_table_absent(tx, "script_runs_event_scope")?;
            tx.execute_batch(EVENT_SCOPE_SCHEMA)?;
            verify_shape(tx, true)?;
            set_meta(tx, EVENT_SCOPE_MARKER, &expected)
        }
    }
}

fn require_digest(tx: &Transaction<'_>, key: &str, source: &str, description: &str) -> Result<()> {
    let expected = json!(model::digest(source.as_bytes()));
    match meta(tx, key)? {
        Some(actual) if actual == expected => Ok(()),
        Some(_) => Err(Error::new(
            "SCHEMA_MISMATCH",
            format!("{description} schema content differs"),
        )),
        None => Err(Error::new(
            "SCHEMA_MISMATCH",
            format!("{description} schema marker is missing"),
        )),
    }
}

fn verify_shape(db: &Connection, event_scope: bool) -> Result<()> {
    for table in SCRIPT_TABLES {
        ensure_table_present(db, table)?;
    }
    ensure_table_absent(db, "script_runs_event_scope")?;

    let expected = reference_schema(event_scope)?;
    for table in SCRIPT_TABLES {
        verify_schema_objects(db, &expected, table)?;
        verify_table_xinfo(db, &expected, table)?;
        verify_foreign_keys(db, &expected, table)?;
        verify_indexes(db, &expected, table)?;
    }
    Ok(())
}

/// Build the expected live schema with this binary's bundled SQLite engine.
/// No production connection or persistent database is opened or changed.
fn reference_schema(event_scope: bool) -> Result<Connection> {
    let db = Connection::open_in_memory()?;
    db.pragma_update(None, "foreign_keys", "ON")?;
    // The table rebuild checks its parent references even with no run rows.
    // Install the real empty parent schema in this disposable connection.
    db.execute_batch(super::SCHEMA)?;
    db.execute_batch(SCRIPT_SCHEMA)?;
    if event_scope {
        db.execute_batch(EVENT_SCOPE_SCHEMA)?;
    }
    Ok(db)
}

fn verify_schema_objects(actual: &Connection, expected: &Connection, table: &str) -> Result<()> {
    let actual_objects = schema_objects(actual, table)?;
    let expected_objects = schema_objects(expected, table)?;
    if actual_objects.len() != expected_objects.len() {
        return Err(schema_mismatch(format!("{table} schema objects differ")));
    }

    for (actual, expected) in actual_objects.iter().zip(&expected_objects) {
        if actual.0 != expected.0 || actual.1 != expected.1 || actual.2 != expected.2 {
            return Err(schema_mismatch(format!(
                "{table} schema object names differ"
            )));
        }
        let sql_matches = match (&actual.3, &expected.3) {
            (None, None) => true,
            (Some(actual), Some(expected)) => canonical_ddl(actual)
                .zip(canonical_ddl(expected))
                .is_some_and(|(actual, expected)| actual == expected),
            _ => false,
        };
        if !sql_matches {
            return Err(schema_mismatch(format!(
                "{table} schema definition differs"
            )));
        }
    }
    Ok(())
}

fn schema_objects(db: &Connection, table: &str) -> Result<Vec<SchemaObjectShape>> {
    let mut statement = db.prepare(
        "SELECT type,name,tbl_name,sql FROM sqlite_schema \
         WHERE (type='table' AND name=?1) \
            OR (type IN ('index','trigger') AND tbl_name=?1) \
         ORDER BY type,name",
    )?;
    let rows = statement.query_map([table], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<SchemaObjectShape>>>()?)
}

fn verify_table_xinfo(actual: &Connection, expected: &Connection, table: &str) -> Result<()> {
    if table_xinfo(actual, table)? != table_xinfo(expected, table)? {
        return Err(schema_mismatch(format!("{table} column metadata differs")));
    }
    Ok(())
}

fn table_xinfo(db: &Connection, table: &str) -> Result<Vec<TableColumnShape>> {
    let mut statement = db.prepare(&format!("PRAGMA table_xinfo({})", quote_identifier(table)))?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<TableColumnShape>>>()?)
}

fn verify_foreign_keys(actual: &Connection, expected: &Connection, table: &str) -> Result<()> {
    if foreign_keys(actual, table)? != foreign_keys(expected, table)? {
        return Err(schema_mismatch(format!("{table} foreign keys differ")));
    }
    Ok(())
}

fn foreign_keys(db: &Connection, table: &str) -> Result<Vec<ForeignKeyShape>> {
    let mut statement = db.prepare(&format!(
        "PRAGMA foreign_key_list({})",
        quote_identifier(table)
    ))?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
            row.get(7)?,
        ))
    })?;
    let mut result = rows.collect::<rusqlite::Result<Vec<ForeignKeyShape>>>()?;
    result.sort();
    Ok(result)
}

fn verify_indexes(actual: &Connection, expected: &Connection, table: &str) -> Result<()> {
    let actual_indexes = index_list(actual, table)?;
    let expected_indexes = index_list(expected, table)?;
    if actual_indexes != expected_indexes {
        return Err(schema_mismatch(format!("{table} indexes differ")));
    }

    for (name, _, _, _) in actual_indexes {
        if index_xinfo(actual, &name)? != index_xinfo(expected, &name)? {
            return Err(schema_mismatch(format!(
                "{table} index {name} columns differ"
            )));
        }
    }
    Ok(())
}

fn index_list(db: &Connection, table: &str) -> Result<Vec<IndexListShape>> {
    let mut statement = db.prepare(&format!("PRAGMA index_list({})", quote_identifier(table)))?;
    let rows = statement.query_map([], |row| {
        Ok((row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?))
    })?;
    let mut result = rows.collect::<rusqlite::Result<Vec<IndexListShape>>>()?;
    result.sort();
    Ok(result)
}

fn index_xinfo(db: &Connection, index: &str) -> Result<Vec<IndexColumnShape>> {
    let mut statement = db.prepare(&format!("PRAGMA index_xinfo({})", quote_identifier(index)))?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
        ))
    })?;
    let mut result = rows.collect::<rusqlite::Result<Vec<IndexColumnShape>>>()?;
    result.sort();
    Ok(result)
}

fn canonical_ddl(sql: &str) -> Option<Vec<DdlToken>> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut cursor = 0;

    while cursor < bytes.len() {
        let byte = bytes[cursor];
        if byte.is_ascii_whitespace() {
            cursor += 1;
            continue;
        }
        if bytes.get(cursor..cursor + 2) == Some(b"--") {
            cursor += 2;
            while cursor < bytes.len() && bytes[cursor] != b'\n' {
                cursor += 1;
            }
            continue;
        }
        if bytes.get(cursor..cursor + 2) == Some(b"/*") {
            cursor += 2;
            let comment_end = bytes[cursor..]
                .windows(2)
                .position(|window| window == b"*/")?;
            cursor += comment_end + 2;
            continue;
        }

        match byte {
            b'\'' => {
                let (value, next) = quoted_value(bytes, cursor, b'\'', b'\'')?;
                tokens.push(DdlToken::StringLiteral(String::from_utf8(value).ok()?));
                cursor = next;
            }
            b'"' | b'`' => {
                let (value, next) = quoted_value(bytes, cursor, byte, byte)?;
                tokens.push(DdlToken::Identifier(
                    String::from_utf8(value).ok()?.to_ascii_lowercase(),
                ));
                cursor = next;
            }
            b'[' => {
                let (value, next) = quoted_value(bytes, cursor, b'[', b']')?;
                tokens.push(DdlToken::Identifier(
                    String::from_utf8(value).ok()?.to_ascii_lowercase(),
                ));
                cursor = next;
            }
            byte if is_identifier_start(byte) => {
                let start = cursor;
                cursor += 1;
                while cursor < bytes.len() && is_identifier_continue(bytes[cursor]) {
                    cursor += 1;
                }
                let word = std::str::from_utf8(&bytes[start..cursor])
                    .ok()?
                    .to_ascii_lowercase();
                if is_sql_keyword(&word) {
                    tokens.push(DdlToken::Keyword(word));
                } else {
                    tokens.push(DdlToken::Identifier(word));
                }
            }
            byte if byte.is_ascii_digit() => {
                let start = cursor;
                cursor += 1;
                while cursor < bytes.len()
                    && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'.')
                {
                    cursor += 1;
                }
                if cursor < bytes.len()
                    && matches!(bytes[cursor], b'+' | b'-')
                    && cursor > start
                    && matches!(bytes[cursor - 1], b'e' | b'E')
                {
                    cursor += 1;
                    while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                        cursor += 1;
                    }
                }
                tokens.push(DdlToken::Number(
                    std::str::from_utf8(&bytes[start..cursor])
                        .ok()?
                        .to_ascii_lowercase(),
                ));
            }
            _ => {
                if let Some(operator) = ddl_operator(&bytes[cursor..]) {
                    tokens.push(DdlToken::Operator(operator.to_owned()));
                    cursor += operator.len();
                } else if byte.is_ascii_punctuation() {
                    tokens.push(DdlToken::Punctuation(char::from(byte)));
                    cursor += 1;
                } else {
                    return None;
                }
            }
        }
    }
    Some(tokens)
}

fn quoted_value(bytes: &[u8], start: usize, opening: u8, closing: u8) -> Option<(Vec<u8>, usize)> {
    if bytes.get(start) != Some(&opening) {
        return None;
    }
    let mut cursor = start + 1;
    let mut value = Vec::new();
    while cursor < bytes.len() {
        if bytes[cursor] == closing {
            if bytes.get(cursor + 1) == Some(&closing) {
                value.push(closing);
                cursor += 2;
            } else {
                return Some((value, cursor + 1));
            }
        } else {
            value.push(bytes[cursor]);
            cursor += 1;
        }
    }
    None
}

fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_' || byte >= 0x80
}

fn is_identifier_continue(byte: u8) -> bool {
    is_identifier_start(byte) || byte.is_ascii_digit() || byte == b'$'
}

fn is_sql_keyword(word: &str) -> bool {
    // Keywords present in the pinned 004/009 DDL; other bare words are
    // identifiers and may be equivalent to quoted identifiers.
    matches!(
        word,
        "alter"
            | "and"
            | "check"
            | "create"
            | "drop"
            | "foreign"
            | "from"
            | "in"
            | "index"
            | "insert"
            | "into"
            | "is"
            | "key"
            | "not"
            | "null"
            | "on"
            | "or"
            | "primary"
            | "references"
            | "rename"
            | "select"
            | "strict"
            | "table"
            | "to"
            | "unique"
            | "where"
    )
}

fn ddl_operator(bytes: &[u8]) -> Option<&'static str> {
    ["->>", "<=", ">=", "<>", "!=", "==", "||", "<<", ">>", "->"]
        .into_iter()
        .find(|operator| bytes.starts_with(operator.as_bytes()))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn schema_mismatch(message: String) -> Error {
    Error::new("SCHEMA_MISMATCH", message)
}

fn ensure_table_present(db: &Connection, table: &str) -> Result<()> {
    let present: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
        [table],
        |row| row.get(0),
    )?;
    if !present {
        return Err(schema_mismatch(format!("{table} table is missing")));
    }
    Ok(())
}

fn ensure_table_absent(db: &Connection, table: &str) -> Result<()> {
    let present: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
        [table],
        |row| row.get(0),
    )?;
    if present {
        return Err(schema_mismatch(format!("unexpected {table} table exists")));
    }
    Ok(())
}
