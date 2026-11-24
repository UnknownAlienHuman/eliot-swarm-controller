use super::*;
use eliot_core::api::auth_login::{
    AuthDesktopReportRequest, AuthDesktopState, AuthDesktopWork, AuthLoginStatus,
};
use eliot_core::api::{OwnerAction, OwnerInboxItem, OwnerInboxKind, SwarmLiveness};

const COOLDOWN_MS: i64 = 5 * 60 * 1000;

impl Store {
    pub(super) fn migrate_auth_login(&mut self, version: i64) -> Result<()> {
        let tx = self.connection.transaction().map_err(sql_error)?;
        tx.execute_batch(
            "CREATE TABLE harness_auth (
                harness TEXT PRIMARY KEY,
                revision TEXT NOT NULL,
                phase TEXT NOT NULL CHECK (phase IN
                    ('queued', 'claimed', 'open', 'closed', 'failed', 'unsupported')),
                first_seen INTEGER NOT NULL,
                attempt TEXT,
                retry_at INTEGER NOT NULL DEFAULT 0,
                signed_out INTEGER NOT NULL,
                verified_signed_out INTEGER NOT NULL DEFAULT 0,
                resolved INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE harness_auth_holds (
                harness TEXT NOT NULL REFERENCES harness_auth(harness),
                scope TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(harness, scope)
             );",
        )
        .map_err(sql_error)?;
        tx.execute(
            "INSERT INTO schema_migrations(version) VALUES (?1)",
            [version],
        )
        .map_err(sql_error)?;
        tx.commit().map_err(sql_error)
    }

    pub fn request_harness_login(
        &mut self,
        harness: &str,
        revision: Option<&str>,
        manual: bool,
        supported: bool,
        now: i64,
    ) -> Result<AuthLoginStatus> {
        self.ensure_writer_healthy()?;
        let result =
            (|| {
                let tx = self.connection.transaction().map_err(sql_error)?;
                let previous: Option<(String, String, i64, bool)> = tx.query_row(
                "SELECT revision, phase, retry_at, resolved FROM harness_auth WHERE harness=?1",
                [harness],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            ).optional().map_err(sql_error)?;
                if let Some(revision) = revision
                    && previous
                        .as_ref()
                        .is_none_or(|row| row.0 != revision || row.3)
                {
                    return Err(Error::new(
                        "stale_item",
                        "harness sign-in item changed or cleared",
                    ));
                }
                let status = match previous {
                    Some((_, phase, _, _)) if phase == "open" => AuthLoginStatus::Open,
                    Some((_, phase, _, _)) if matches!(phase.as_str(), "claimed" | "queued") => {
                        AuthLoginStatus::Queued
                    }
                    Some((_, phase, retry_at, resolved))
                        if !resolved && (!manual || retry_at > now) && phase != "unsupported" =>
                    {
                        AuthLoginStatus::Cooldown
                    }
                    previous => {
                        let new_revision = ulid::Ulid::new().to_string();
                        let reset = previous.is_none_or(|(_, _, _, resolved)| resolved);
                        let phase = if supported { "queued" } else { "unsupported" };
                        tx.execute(
                        "INSERT INTO harness_auth(harness, revision, phase, first_seen, signed_out)
                         VALUES (?1, ?2, ?3, ?4, ?5)
                         ON CONFLICT(harness) DO UPDATE SET
                             revision=excluded.revision, phase=excluded.phase, attempt=NULL,
                             first_seen=CASE WHEN ?6 THEN excluded.first_seen ELSE first_seen END,
                             signed_out=CASE WHEN ?6 THEN excluded.signed_out
                                             ELSE signed_out OR excluded.signed_out END,
                             verified_signed_out=CASE WHEN ?6 THEN 0 ELSE verified_signed_out END,
                             resolved=0",
                        params![harness, new_revision, phase, now, !manual, reset],
                    ).map_err(sql_error)?;
                        AuthLoginStatus::Queued
                    }
                };
                if !manual {
                    tx.execute(
                        "UPDATE harness_auth SET signed_out=1, resolved=0
                     WHERE harness=?1 AND (signed_out=0 OR resolved=1)",
                        [harness],
                    )
                    .map_err(sql_error)?;
                }
                commit_transaction(tx, &self.write_faulted)?;
                Ok(status)
            })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn harness_auth_pending(&self, now: i64) -> Result<Vec<AuthDesktopWork>> {
        self.ensure_committed_reads()?;
        let mut statement = self
            .connection
            .prepare_cached(
                "SELECT harness, revision, phase, retry_at FROM harness_auth
             WHERE resolved=0 AND phase!='unsupported' ORDER BY harness",
            )
            .map_err(sql_error)?;
        statement
            .query_map([], |row| {
                let phase: String = row.get(2)?;
                let retry_at: i64 = row.get(3)?;
                Ok(AuthDesktopWork {
                    harness: row.get(0)?,
                    revision: row.get(1)?,
                    probe: true,
                    can_claim: phase == "queued" && retry_at <= now,
                })
            })
            .map_err(sql_error)?
            .collect::<std::result::Result<_, _>>()
            .map_err(sql_error)
    }

    pub fn claim_harness_login(
        &mut self,
        harness: &str,
        revision: &str,
        now: i64,
    ) -> Result<Option<String>> {
        self.ensure_writer_healthy()?;
        let attempt = ulid::Ulid::new().to_string();
        let result = self
            .connection
            .execute(
                "UPDATE harness_auth SET phase='claimed', attempt=?3, retry_at=?4
             WHERE harness=?1 AND revision=?2 AND resolved=0 AND phase='queued' AND retry_at<=?5",
                params![
                    harness,
                    revision,
                    attempt,
                    now.saturating_add(COOLDOWN_MS),
                    now
                ],
            )
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        Ok((result? == 1).then_some(attempt))
    }

    pub fn report_harness_login(
        &mut self,
        report: &AuthDesktopReportRequest,
        now: i64,
    ) -> Result<(bool, Vec<Event>)> {
        use AuthDesktopState::{Closed, Failed, Open, SignedIn, SignedOut};
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self.connection.transaction().map_err(sql_error)?;
            let verified_outage = matches!(report.state, SignedIn)
                && tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM harness_auth
                     WHERE harness=?1 AND revision=?2 AND verified_signed_out=1)",
                        params![report.harness, report.revision],
                        |row| row.get::<_, bool>(0),
                    )
                    .map_err(sql_error)?;
            let changed = match report.state {
                SignedIn | SignedOut if report.attempt.is_none() => tx.execute(
                    "UPDATE harness_auth
                     SET resolved=?3, signed_out=NOT ?3,
                         verified_signed_out=verified_signed_out OR NOT ?3,
                         phase=CASE WHEN ?3 AND phase='queued' THEN 'closed' ELSE phase END
                     WHERE harness=?1 AND revision=?2 AND resolved=0 AND (?3 OR verified_signed_out=0)",
                    params![
                        report.harness,
                        report.revision,
                        matches!(report.state, SignedIn)
                    ],
                ),
                Open | Closed | Failed if report.attempt.is_some() => {
                    let phase = match report.state {
                        Open => "open",
                        Closed => "closed",
                        _ => "failed",
                    };
                    tx.execute(
                        "UPDATE harness_auth SET phase=?4
                         WHERE harness=?1 AND revision=?2 AND attempt=?3 AND
                             (phase='claimed' OR (phase='open' AND ?4='closed'))",
                        params![report.harness, report.revision, report.attempt, phase],
                    )
                }
                _ => return Err(Error::new("invalid", "incomplete sign-in report binding")),
            }
            .map_err(sql_error)?;
            let resolved = changed == 1 && matches!(report.state, SignedIn);
            let mut events = if resolved {
                settle_auth_holds(&tx, &report.harness, verified_outage, now)?
            } else {
                Vec::new()
            };
            if changed == 1 && matches!(report.state, Open) {
                let signed_out: bool = tx
                    .query_row(
                        "SELECT signed_out FROM harness_auth WHERE harness=?1",
                        [&report.harness],
                        |row| row.get(0),
                    )
                    .map_err(sql_error)?;
                let message = if signed_out {
                    format!(
                        "{} is signed out. The sign-in window is open.",
                        report.harness
                    )
                } else {
                    format!("The {} sign-in window is open.", report.harness)
                };
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "x.user.notify",
                    "user",
                    None,
                    None,
                    json!({"title":"Harness sign-in", "message":message}),
                )?);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok((resolved, events))
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn harness_auth_held(&self, harness: &str) -> Result<bool> {
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM harness_auth
             WHERE harness=?1 AND resolved=0 AND signed_out=1 AND phase!='unsupported')",
                [harness],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    pub fn bind_harness_auth_hold(
        &mut self,
        harness: &str,
        scope: &str,
        updated_at: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = self
            .connection
            .execute(
                "INSERT INTO harness_auth_holds(harness, scope, updated_at)
             SELECT ?1, scope, updated_at FROM provider_holds
             WHERE scope=?2 AND updated_at=?3 AND kind='auth' AND state='held'
                 AND probe_command IS NULL AND probe_outcome_unknown=0
                 AND json_extract(source, '$.harness')=?1
             ON CONFLICT(harness, scope) DO UPDATE SET updated_at=excluded.updated_at",
                params![harness, scope, updated_at],
            )
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        result.map(|_| ())
    }
}

fn settle_auth_holds(
    tx: &Transaction<'_>,
    harness: &str,
    verified_outage: bool,
    now: i64,
) -> Result<Vec<Event>> {
    let scopes = if verified_outage {
        let mut statement = tx
            .prepare(
                "DELETE FROM provider_holds WHERE kind='auth' AND state='held'
                 AND probe_command IS NULL AND probe_outcome_unknown=0
                 AND json_extract(source, '$.harness')=?1
                 AND (scope, updated_at) IN
                     (SELECT scope, updated_at FROM harness_auth_holds WHERE harness=?1)
             RETURNING scope",
            )
            .map_err(sql_error)?;
        statement
            .query_map([harness], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?
    } else {
        Vec::new()
    };
    tx.execute("DELETE FROM harness_auth_holds WHERE harness=?1", [harness])
        .map_err(sql_error)?;
    scopes
        .into_iter()
        .map(|scope| {
            insert_daemon_event(
                tx,
                now,
                "provider.hold_cleared",
                "provider",
                None,
                None,
                json!({"scope": scope, "kind": "auth", "reason": "native_sign_in_verified"}),
            )
        })
        .collect()
}

pub(super) fn project_harness_auth(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    _liveness: &mut SwarmLiveness,
    keys: &[&dyn rusqlite::ToSql],
) -> Result<()> {
    let mut statement = db.prepare_cached(if keys.is_empty() {
        "SELECT harness, revision, phase, first_seen, signed_out FROM harness_auth WHERE resolved=0"
    } else {
        "SELECT harness, revision, phase, first_seen, signed_out FROM harness_auth WHERE resolved=0 AND harness=?1"
    }).map_err(sql_error)?;
    let mut rows = statement.query(keys).map_err(sql_error)?;
    while let Some(row) = rows.next().map_err(sql_error)? {
        let harness: String = row.get(0).map_err(sql_error)?;
        let revision: String = row.get(1).map_err(sql_error)?;
        let phase: String = row.get(2).map_err(sql_error)?;
        let first_seen: i64 = row.get(3).map_err(sql_error)?;
        let signed_out: bool = row.get(4).map_err(sql_error)?;
        let detail = match phase.as_str() {
            "open" => "The sign-in window is open. Complete sign-in with the vendor.",
            "queued" => "Swarm is waiting for the interactive desktop to handle sign-in.",
            "claimed" => "Swarm is waiting for the desktop to confirm the sign-in window.",
            "closed" => "The sign-in window closed. Swarm is checking the vendor sign-in status.",
            "failed" => "The desktop could not open the vendor sign-in window.",
            _ => "This installation has no supported interactive vendor sign-in command.",
        };
        let choices = (phase != "unsupported")
            .then(|| OwnerAction {
                choice: "sign_in".to_owned(),
                method: "auth.login".to_owned(),
                args: json!({"harness": harness, "revision": revision}),
            })
            .into_iter()
            .collect();
        items.push(OwnerInboxItem {
            kind: OwnerInboxKind::HarnessAuth,
            subject: harness,
            revision,
            age_ms: now.saturating_sub(first_seen),
            reason: if signed_out {
                "auth_required"
            } else {
                "auth_requested"
            }
            .to_owned(),
            detail: Some(detail.to_owned()),
            recommended: None,
            choices,
            instructions: None,
        });
    }
    Ok(())
}
