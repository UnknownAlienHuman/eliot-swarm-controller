//! One durable self-check fact per harness (#932 item 3): the installed
//! version and when that version last passed Swarm's own scenario.
//!
//! A new installed version is always accepted and marked "not checked"; it is
//! never refused. At most two attempts exist per harness and version: a
//! terminal `passed`/`failed` outcome binds its version and never runs again,
//! while inconclusive outcomes can retry once. A repeated attributable failure
//! blocks automatic routing until a subsequent pass.

use super::{Error, Result, Store, commit_transaction, sql_error};
use eliot_core::{
    self as core, HARNESS_CHECK_QUOTA_RETRY_SECS, MAX_HARNESS_CHECK_ERROR_BYTES,
    MAX_HARNESS_CHECK_VERSION_BYTES, NOT_CHECKED_NEW_VERSION, NOT_CHECKED_QUOTA, OUTCOME_FAILED,
    OUTCOME_NOT_CHECKED, OUTCOME_PASSED,
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

/// One stored harness self-check fact.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HarnessCheckRow {
    pub harness: String,
    pub installed_version: Option<String>,
    pub outcome: String,
    pub not_checked_reason: Option<String>,
    pub checked_version: Option<String>,
    pub checked_at: i64,
    pub check_profile: Option<String>,
    pub failure_step: Option<String>,
    pub failure_error: Option<String>,
    pub updated_at: i64,
}

impl HarnessCheckRow {
    pub fn checked_outcome(&self) -> core::api::HarnessCheckOutcome {
        use core::api::HarnessCheckOutcome;
        match (self.version_checked(), self.outcome.as_str()) {
            (true, OUTCOME_PASSED) => HarnessCheckOutcome::Passed,
            (true, OUTCOME_FAILED) => HarnessCheckOutcome::Failed,
            _ => HarnessCheckOutcome::NotChecked,
        }
    }

    /// True when the current installed version already has its recorded run.
    pub fn version_checked(&self) -> bool {
        let (Some(installed), Some(checked)) = (
            self.installed_version.as_deref(),
            self.checked_version.as_deref(),
        ) else {
            return false;
        };
        core::is_terminal_outcome(&self.outcome)
            && core::same_harness_version(&self.harness, installed, checked)
    }

    /// True when a check run is due for the current installed version.
    pub fn check_due(&self, now: i64) -> bool {
        if self
            .installed_version
            .as_deref()
            .is_none_or(|v| v.is_empty())
        {
            return false;
        }
        if self.version_checked() {
            return false;
        }
        if self.outcome == OUTCOME_NOT_CHECKED
            && self.not_checked_reason.as_deref() == Some(NOT_CHECKED_QUOTA)
            && now.saturating_sub(self.checked_at)
                < HARNESS_CHECK_QUOTA_RETRY_SECS.saturating_mul(1000)
        {
            return false;
        }
        true
    }
}

fn bound_version(version: &str) -> Result<String> {
    let trimmed = version.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_HARNESS_CHECK_VERSION_BYTES {
        return Err(Error::new("invalid", "harness version is invalid"));
    }
    Ok(trimmed.to_owned())
}

fn bound_error(error: &str) -> Result<String> {
    let trimmed = error.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_HARNESS_CHECK_ERROR_BYTES {
        return Err(Error::new("invalid", "harness check error is invalid"));
    }
    Ok(trimmed.to_owned())
}

fn check_harness(harness: &str) -> Result<()> {
    if !core::api::is_supported_harness(harness) {
        return Err(Error::new("invalid", "unsupported harness"));
    }
    Ok(())
}

fn check_step(step: &str) -> Result<()> {
    if !core::CHECK_STEPS.contains(&step) {
        return Err(Error::new("invalid", "harness check step is invalid"));
    }
    Ok(())
}

/// One step failure, with a stable key only when the vendor supplied an
/// attributable refusal for this exact checked operation.
pub struct HarnessCheckFailure<'a> {
    pub step: &'a str,
    pub message: &'a str,
    pub key: Option<&'a str>,
    pub quota: bool,
    pub preflight: bool,
}

fn row(row: &rusqlite::Row<'_>) -> rusqlite::Result<HarnessCheckRow> {
    Ok(HarnessCheckRow {
        harness: row.get(0)?,
        installed_version: row.get::<_, Option<String>>(1)?.filter(|v| !v.is_empty()),
        outcome: row.get(2)?,
        not_checked_reason: row.get(3)?,
        checked_version: row.get::<_, Option<String>>(4)?.filter(|v| !v.is_empty()),
        checked_at: row.get(5)?,
        check_profile: row.get(6)?,
        failure_step: row.get(7)?,
        failure_error: row.get(8)?,
        updated_at: row.get(9)?,
    })
}

const COLUMNS: &str = "harness, installed_version, outcome, not_checked_reason, \
     checked_version, checked_at, check_profile, failure_step, failure_error, updated_at";

impl Store {
    /// Record the installed version probe for one harness. A changed version
    /// is accepted and marked "not checked", never refused. Returns true when
    /// a check run is due for the current installed version.
    pub fn note_harness_installed(
        &mut self,
        harness: &str,
        version: Option<&str>,
        now: i64,
    ) -> Result<bool> {
        check_harness(harness)?;
        let version = version
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(bound_version)
            .transpose()?;
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let current: Option<HarnessCheckRow> = tx
                .query_row(
                    &format!("SELECT {COLUMNS} FROM harness_checks WHERE harness = ?1"),
                    params![harness],
                    row,
                )
                .optional()
                .map_err(sql_error)?;
            let previous = current
                .as_ref()
                .and_then(|row| row.installed_version.clone());
            if let Some(current) = current {
                // A missing probe does not invalidate a completed run.
                if let Some(version) = &version
                    && previous
                        .as_deref()
                        .is_none_or(|before| !core::same_harness_version(harness, before, version))
                    && harness_check_idle(&tx, harness)?
                {
                    if current.version_checked()
                        && let Some(checked) = current.checked_version.as_deref()
                    {
                        tx.execute(
                            "INSERT INTO harness_check_history
                                 (harness, version_key, checked_version, checked_at, outcome,
                                  check_profile, failure_step, failure_error)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                             ON CONFLICT (harness, version_key) DO NOTHING",
                            params![
                                harness,
                                core::normalize_harness_version(harness, checked),
                                checked,
                                current.checked_at,
                                current.outcome,
                                current.check_profile,
                                current.failure_step,
                                current.failure_error,
                            ],
                        )
                        .map_err(sql_error)?;
                    }
                    tx.execute(
                        "UPDATE harness_checks SET installed_version = ?2,
                             outcome = ?3, not_checked_reason = ?4,
                             checked_version = '', checked_at = 0, check_profile = NULL,
                             failure_step = NULL, failure_error = NULL,
                             attempted_version = NULL, attempted_at = 0, updated_at = ?5
                         WHERE harness = ?1",
                        params![
                            harness,
                            version,
                            OUTCOME_NOT_CHECKED,
                            NOT_CHECKED_NEW_VERSION,
                            now
                        ],
                    )
                    .map_err(sql_error)?;
                    tx.execute(
                        "UPDATE harness_checks
                         SET (checked_version, checked_at, outcome, check_profile,
                              failure_step, failure_error, not_checked_reason, automatic_blocked) =
                             (history.checked_version, history.checked_at, history.outcome,
                              history.check_profile, history.failure_step, history.failure_error,
                              NULL, history.outcome = 'failed')
                         FROM harness_check_history AS history
                         WHERE harness_checks.harness = ?1 AND history.harness = ?1
                           AND history.version_key = ?2",
                        params![harness, core::normalize_harness_version(harness, version)],
                    )
                    .map_err(sql_error)?;
                }
            } else {
                tx.execute(
                    "INSERT INTO harness_checks(harness, installed_version, outcome,
                         not_checked_reason, checked_at, updated_at)
                     VALUES (?1, COALESCE(?2, ''), ?3, ?4, 0, ?5)",
                    params![
                        harness,
                        version,
                        OUTCOME_NOT_CHECKED,
                        NOT_CHECKED_NEW_VERSION,
                        now
                    ],
                )
                .map_err(sql_error)?;
            }
            let due: HarnessCheckRow = tx
                .query_row(
                    &format!("SELECT {COLUMNS} FROM harness_checks WHERE harness = ?1"),
                    params![harness],
                    row,
                )
                .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(due.check_due(now))
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Reserve a run before admitting any native launch. A crash cannot
    /// replenish this version's budget or consume a third model run.
    pub fn claim_harness_check(
        &mut self,
        harness: &str,
        version: &str,
        profile: &str,
        now: i64,
    ) -> Result<bool> {
        check_harness(harness)?;
        let version = bound_version(version)?;
        // An empty profile marks a `select_profile` failure: no configured
        // profile existed to claim the run with.
        if profile.len() > core::api::harness::MAX_HARNESS_PROFILE_BYTES
            || profile.chars().any(char::is_control)
        {
            return Err(Error::new("invalid", "harness check profile is invalid"));
        }
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let current: Option<HarnessCheckRow> = tx
                .query_row(
                    &format!("SELECT {COLUMNS} FROM harness_checks WHERE harness = ?1"),
                    params![harness],
                    row,
                )
                .optional()
                .map_err(sql_error)?;
            let Some(current) = current else {
                return Err(Error::new("invalid", "harness check fact is missing"));
            };
            let attempted: Option<String> = tx
                .query_row(
                    "SELECT attempted_version FROM harness_checks WHERE harness = ?1",
                    params![harness],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if attempted.is_some()
                || !current.check_due(now)
                || current
                    .installed_version
                    .as_deref()
                    .is_none_or(|installed| {
                        !core::same_harness_version(harness, installed, &version)
                    })
            {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(false);
            }
            let reserved = tx
                .execute(
                    "INSERT INTO harness_check_attempts(harness, version_key, attempts)
                 VALUES (?1, ?2, 1)
                 ON CONFLICT(harness, version_key) DO UPDATE SET attempts = attempts + 1
                 WHERE attempts < ?3 AND retry_at <= ?4",
                    params![
                        harness,
                        core::normalize_harness_version(harness, &version),
                        core::HARNESS_CHECK_MAX_ATTEMPTS,
                        now,
                    ],
                )
                .map_err(sql_error)?;
            if reserved == 0 {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(false);
            }
            tx.execute(
                "UPDATE harness_checks SET attempted_version = ?2, attempted_at = ?3,
                     check_profile = ?4, updated_at = ?3 WHERE harness = ?1",
                params![harness, version, now, profile],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(true)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    fn record_harness_outcome(
        &mut self,
        harness: &str,
        version: &str,
        profile: &str,
        failure: Option<HarnessCheckFailure<'_>>,
        now: i64,
    ) -> Result<()> {
        check_harness(harness)?;
        let version = bound_version(version)?;
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let (outcome, reason, step, message) = if let Some(failure) = failure {
                check_step(failure.step)?;
                let version_key = core::normalize_harness_version(harness, &version);
                let (attempts, first): (i64, Option<String>) = tx
                    .query_row(
                        "SELECT attempts, first_failure FROM harness_check_attempts
                     WHERE harness = ?1 AND version_key = ?2",
                        params![harness, version_key],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .map_err(sql_error)?;
                let confirmed = !failure.quota
                    && attempts == core::HARNESS_CHECK_MAX_ATTEMPTS
                    && failure.key.is_some()
                    && first.as_deref() == failure.key;
                if confirmed {
                    (
                        OUTCOME_FAILED,
                        None,
                        Some(failure.step),
                        Some(bound_error(failure.message)?),
                    )
                } else {
                    let attempts = attempts - i64::from(failure.quota && failure.preflight);
                    let reason = if attempts >= core::HARNESS_CHECK_MAX_ATTEMPTS {
                        core::NOT_CHECKED_ATTEMPT_LIMIT
                    } else if failure.quota {
                        NOT_CHECKED_QUOTA
                    } else if failure.key.is_some() {
                        core::NOT_CHECKED_UNCONFIRMED
                    } else {
                        core::NOT_CHECKED_TRANSIENT
                    };
                    let cooldown = if failure.quota {
                        HARNESS_CHECK_QUOTA_RETRY_SECS
                    } else {
                        core::HARNESS_CHECK_RETRY_SECS
                    };
                    tx.execute(
                        "UPDATE harness_check_attempts SET attempts = ?3, retry_at = ?4,
                            first_failure = CASE WHEN ?3 = 1 AND NOT ?6 THEN ?5 ELSE first_failure END
                         WHERE harness = ?1 AND version_key = ?2",
                        params![
                            harness,
                            version_key,
                            attempts,
                            now.saturating_add(cooldown * 1000),
                            failure.key, failure.quota && failure.preflight,
                        ],
                    )
                    .map_err(sql_error)?;
                    (OUTCOME_NOT_CHECKED, Some(reason), None, None)
                }
            } else {
                (OUTCOME_PASSED, None, None, None)
            };
            let checked_version = if outcome == OUTCOME_NOT_CHECKED {
                ""
            } else {
                &version
            };
            let changed = tx
                .execute(
                    "UPDATE harness_checks SET outcome = ?2, not_checked_reason = ?3,
                     checked_version = ?4, checked_at = ?5, check_profile = ?6,
                     failure_step = ?7, failure_error = ?8,
                     attempted_version = NULL, attempted_at = 0, updated_at = ?5,
                     automatic_blocked = CASE ?2 WHEN 'failed' THEN 1 WHEN 'passed' THEN 0
                                             ELSE automatic_blocked END
                 WHERE harness = ?1 AND attempted_version = ?9",
                    params![
                        harness,
                        outcome,
                        reason,
                        checked_version,
                        now,
                        profile,
                        step,
                        message,
                        version,
                    ],
                )
                .map_err(sql_error)?;
            if changed != 1 {
                return Err(Error::new(
                    "invalid",
                    "harness check run was not claimed for this version",
                ));
            }
            commit_transaction(tx, &self.write_faulted)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn record_harness_check_passed(
        &mut self,
        harness: &str,
        version: &str,
        profile: &str,
        now: i64,
    ) -> Result<()> {
        self.record_harness_outcome(harness, version, profile, None, now)
    }

    pub fn record_harness_check_failure(
        &mut self,
        harness: &str,
        version: &str,
        profile: &str,
        failure: HarnessCheckFailure<'_>,
        now: i64,
    ) -> Result<()> {
        self.record_harness_outcome(harness, version, profile, Some(failure), now)
    }

    /// Read a structured failure code committed for the exact native turn.
    pub fn harness_check_turn_error_code(&self, turn: &core::Turn) -> Result<Option<String>> {
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT json_extract(data, '$.native_error.code') FROM events
             WHERE agent = ?1 AND generation = ?2 AND kind = 'turn.completed'
               AND json_extract(data, '$.turn_id') = ?3
             ORDER BY seq DESC LIMIT 1",
                params![
                    core::parse_agent_id(&turn.agent)?,
                    turn.generation,
                    turn.turn_id
                ],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map(Option::flatten)
            .map_err(sql_error)
    }

    /// Read one harness self-check fact.
    pub fn get_harness_check(&self, harness: &str) -> Result<Option<HarnessCheckRow>> {
        check_harness(harness)?;
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                &format!("SELECT {COLUMNS} FROM harness_checks WHERE harness = ?1"),
                params![harness],
                row,
            )
            .optional()
            .map_err(sql_error)
    }

    /// Read every harness self-check fact, ordered by harness.
    pub fn list_harness_checks(&self) -> Result<Vec<HarnessCheckRow>> {
        self.ensure_committed_reads()?;
        let mut stmt = self
            .connection
            .prepare_cached(&format!(
                "SELECT {COLUMNS} FROM harness_checks ORDER BY harness"
            ))
            .map_err(sql_error)?;
        stmt.query_map([], row)
            .map_err(sql_error)?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(sql_error)
    }

    /// Find a live diagnostic Agent for the exact installed version.
    pub fn active_harness_check_agent(
        &self,
        harness: &str,
        version: &str,
    ) -> Result<Option<String>> {
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT id FROM agents WHERE harness = ?1
               AND json_extract(profile_snapshot, '$._harness_check.version') = ?2
               AND state IN ('created','starting','idle','busy','waiting')
             ORDER BY id DESC LIMIT 1",
                params![harness, version],
                |row| Ok(super::format_agent_id(row.get(0)?)),
            )
            .optional()
            .map_err(sql_error)
    }

    /// True after a failed check, until a later run passes.
    pub fn harness_check_blocks_automatic(&self, harness: &str) -> Result<bool> {
        check_harness(harness)?;
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT automatic_blocked FROM harness_checks WHERE harness = ?1",
                [harness],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
            .map(Option::unwrap_or_default)
    }

    /// Installed versions with a check run due, ordered by harness, at most
    /// `limit`. The table holds one row per supported harness, so a full read
    /// with a Rust-side due filter stays bounded.
    pub fn due_harness_checks(&self, now: i64, limit: usize) -> Result<Vec<(String, String)>> {
        let mut due = Vec::new();
        for row in self.list_harness_checks()? {
            if due.len() == limit {
                break;
            }
            if row.check_due(now)
                && let Some(version) = row.installed_version
            {
                let available = self
                    .connection
                    .query_row(
                        "SELECT attempts < ?3 AND retry_at <= ?4 FROM harness_check_attempts
                     WHERE harness = ?1 AND version_key = ?2",
                        params![
                            row.harness,
                            core::normalize_harness_version(&row.harness, &version),
                            core::HARNESS_CHECK_MAX_ATTEMPTS,
                            now
                        ],
                        |row| row.get::<_, bool>(0),
                    )
                    .optional()
                    .map_err(sql_error)?
                    .unwrap_or(true);
                if available {
                    due.push((row.harness, version));
                }
            }
        }
        Ok(due)
    }

    /// When the maintenance worker has no active drive, surviving claims
    /// belong to an interrupted check. A new check uses a fresh Agent.
    pub fn release_interrupted_harness_checks(&mut self, now: i64) -> Result<Vec<String>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let released = {
                let mut stmt = tx
                    .prepare_cached(
                        "UPDATE harness_checks SET attempted_version = NULL, attempted_at = 0,
                         updated_at = ?1 WHERE attempted_version IS NOT NULL RETURNING harness",
                    )
                    .map_err(sql_error)?;
                stmt.query_map([now], |row| row.get(0))
                    .map_err(sql_error)?
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(sql_error)?
            };
            commit_transaction(tx, &self.write_faulted)?;
            Ok(released)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Record an eligibility deferral without claiming or spending a run.
    pub fn defer_harness_check_profile(
        &mut self,
        harness: &str,
        reason: &str,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = self
            .connection
            .execute(
                "UPDATE harness_checks SET not_checked_reason = ?2, updated_at = ?3
             WHERE harness = ?1 AND attempted_version IS NULL AND outcome = 'not_checked'
               AND not_checked_reason IS NOT ?2",
                params![harness, reason, now],
            )
            .map(|_| ())
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Native tool calls observed in one exact turn. The shared native-event
    /// path counts `tool.started` per turn; the check's tool-call step passes
    /// only when the scenario turn actually used a tool.
    pub fn turn_tool_calls(&self, agent: &str, generation: i64, turn_id: &str) -> Result<i64> {
        let agent_num = core::parse_agent_id(agent)?;
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT tool_calls FROM turns
                 WHERE agent = ?1 AND generation = ?2 AND turn_id = ?3",
                params![agent_num, generation, turn_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
            .map(|calls| calls.unwrap_or(0))
    }
}

fn harness_check_idle(db: &rusqlite::Connection, harness: &str) -> Result<bool> {
    db.query_row(
        "SELECT attempted_version IS NULL FROM harness_checks WHERE harness = ?1",
        [harness],
        |row| row.get(0),
    )
    .map_err(sql_error)
}
