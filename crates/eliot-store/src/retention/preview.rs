//! Read-only forecast with each protection evaluated at the snapshot boundary.
use super::*;
use crate::cleanup::{
    CLEANUP_PLAN_AGE, REMOVED_ARTIFACT, artifact_tombstone_select, cleanup_metadata_cutoff,
    settled_cleanup_plan_select, stale_cleanup_plan_select,
};
use eliot_core::api::{RetentionPreviewRequest, RetentionPreviewResponse, RetentionPreviewRow};
use rusqlite::named_params;
use std::collections::HashSet;

const NONE: &str = "SELECT NULL WHERE 0";
const ALL_ROWS: i64 = -1;

impl Store {
    pub fn preview_retention(
        &self,
        request: &RetentionPreviewRequest,
        now: i64,
    ) -> Result<RetentionPreviewResponse> {
        request.validate()?;
        let cutoffs = retention_cutoffs(
            request.event_window_seconds(),
            request.processes_seconds,
            request.commands_days,
            request.mail_days,
            now,
        )?;
        // The CLI opens a query-only connection and begins a deferred snapshot.
        let db = &self.connection;
        let current_pruned_through = coverage_pruned_through(db)?;
        let (events, projected_pruned_through, first_kept_event) =
            events(db, current_pruned_through, cutoffs.events)?;
        let mut rows = vec![
            events,
            processes(db, cutoffs.processes)?,
            commands(db, cutoffs.commands)?,
        ];
        let plan_cutoff = cleanup_metadata_cutoff(now);
        let expiring_plans = format!(
            "SELECT * FROM ({}) UNION ALL SELECT * FROM ({})",
            settled_cleanup_plan_select(),
            stale_cleanup_plan_select(),
        );
        let kept_plans =
            format!("(SELECT body,apply_id FROM cleanup_plans WHERE id NOT IN ({expiring_plans}))");

        let mut plan_row = summary(
            db,
            "cleanup_plans",
            "cleanup ledger",
            "SELECT id,json_extract(body,'$.created_at') FROM cleanup_plans",
            &expiring_plans,
            named_params! {":plan_cutoff": plan_cutoff, ":plan_limit": ALL_ROWS},
        )?;
        let held = count(
            db,
            &format!("SELECT COUNT(*) FROM cleanup_plans p WHERE {CLEANUP_PLAN_AGE}"),
            named_params! {":plan_cutoff": plan_cutoff},
        )? - plan_row.would_delete;
        protect(&mut plan_row, "live_or_uncertain_receipt", held);
        rows.push(plan_row);

        let mut tombstones = summary(
            db,
            "artifacts",
            "removed tombstones",
            &format!(
                "SELECT id,json_extract(body,'$.created_at') FROM artifacts a WHERE {REMOVED_ARTIFACT}"
            ),
            &artifact_tombstone_select(&kept_plans),
            named_params! {":plan_cutoff": plan_cutoff, ":plan_limit": ALL_ROWS, ":tombstone_limit": ALL_ROWS},
        )?;
        let kept = tombstones.kept;
        protect(&mut tombstones, "custody_or_ledger_anchor", kept);
        rows.push(tombstones);

        let write_ledger = "SELECT id,created_at FROM writes";
        let digest_ledger = "SELECT id,created_at FROM completion_digests";
        if let Some(cutoff) = cutoffs.commands {
            let mut write_row = summary(
                db,
                "writes",
                "mutation receipts",
                write_ledger,
                &prune_write_select(&kept_plans),
                named_params! {
                    ":write_cutoff": cutoff,
                    ":write_limit": ALL_ROWS,
                    ":running_goal": crate::goals::goal_round_running_outcome_json()?,
                    ":plan_cutoff": plan_cutoff,
                    ":plan_limit": ALL_ROWS,
                },
            )?;
            let old = count(
                db,
                &format!("SELECT COUNT(*) FROM writes WHERE {WRITE_AGE}"),
                named_params! {":write_cutoff": cutoff},
            )?;
            let separate = count(
                db,
                &format!(
                    "SELECT COUNT(*) FROM writes WHERE {WRITE_AGE} AND NOT ({WRITE_INCLUDED})"
                ),
                named_params! {":write_cutoff": cutoff},
            )?;
            let kept = old - write_row.would_delete - separate;
            protect(&mut write_row, "live_receipt", kept);
            protect(&mut write_row, "separate_inference_policy", separate);
            if let Some(days) = request.commands_days {
                let proposed = now.saturating_sub(days.saturating_mul(MS_PER_DAY));
                protect(
                    &mut write_row,
                    "retry",
                    count(
                        db,
                        "SELECT COUNT(*) FROM writes WHERE created_at>?1 AND created_at<=?2",
                        params![cutoff, proposed],
                    )?,
                );
            }
            rows.push(write_row);

            let mut digests = summary(
                db,
                "completion_digests",
                "completion digests",
                digest_ledger,
                &prune_digest_select(),
                params![cutoff, ALL_ROWS],
            )?;
            protect(
                &mut digests,
                "pending_delivery",
                count(
                    db,
                    &format!(
                        "SELECT COUNT(*) FROM completion_digests WHERE NOT ({DIGEST_DELIVERED}) AND created_at<=?1"
                    ),
                    [cutoff],
                )?,
            );
            rows.push(digests);
        } else {
            rows.push(summary(
                db,
                "writes",
                "mutation receipts",
                write_ledger,
                NONE,
                [],
            )?);
            rows.push(summary(
                db,
                "completion_digests",
                "completion digests",
                digest_ledger,
                NONE,
                [],
            )?);
        }
        mail(db, cutoffs.mail, &mut rows)?;

        Ok(RetentionPreviewResponse {
            as_of_ms: now,
            rows,
            current_pruned_through,
            projected_pruned_through,
            first_kept_event_seq: first_kept_event.map(|(seq, _)| seq),
            first_kept_event_ms: first_kept_event.map(|(_, ms)| ms),
        })
    }
}

fn count(db: &Connection, sql: &str, args: impl rusqlite::Params) -> Result<i64> {
    db.query_row(sql, args, |row| row.get(0)).map_err(sql_error)
}

fn protect(row: &mut RetentionPreviewRow, reason: &str, count: i64) {
    if count > 0 {
        row.protected.insert(reason.to_owned(), count);
    }
}

fn row(
    table: &str,
    class: &str,
    would_delete: i64,
    kept: i64,
    oldest_kept_ms: Option<i64>,
) -> RetentionPreviewRow {
    RetentionPreviewRow {
        table: table.to_owned(),
        class: class.to_owned(),
        would_delete,
        kept,
        protected: BTreeMap::new(),
        oldest_kept_ms,
    }
}

// Both queries expose only an identity and its retained clock. The selected
// IDs are the Store's deletion SELECTs with their batch limits lifted.
// Materialize them once so a forced sweep index never runs once per ledger row.
fn summary(
    db: &Connection,
    table: &str,
    class: &str,
    ledger: &str,
    selected: &str,
    args: impl rusqlite::Params,
) -> Result<RetentionPreviewRow> {
    db.query_row(
        &format!(
            "WITH retained(id,ts) AS ({ledger}), selected(id) AS MATERIALIZED ({selected})
         SELECT count(selected.id),count(*)-count(selected.id),
                min(CASE WHEN selected.id IS NULL THEN retained.ts END)
         FROM retained LEFT JOIN selected ON selected.id=retained.id"
        ),
        args,
        |result| {
            Ok(row(
                table,
                class,
                result.get(0)?,
                result.get(1)?,
                result.get(2)?,
            ))
        },
    )
    .map_err(sql_error)
}

// Row counts, projected replay floor, and the first replayable sequence and time.
type EventPreview = (RetentionPreviewRow, i64, Option<(i64, i64)>);

fn events(db: &Connection, current: i64, cutoff: Option<i64>) -> Result<EventPreview> {
    let high_water = count(db, "SELECT COALESCE(MAX(seq),?1) FROM events", [current])?;
    let boundary = match cutoff {
        Some(cutoff) => event_prune_end(db, current, high_water, cutoff)?,
        None => EventPruneBoundary {
            end: current,
            blocker: None,
        },
    };
    let (deleted, projected): (i64, i64) = db
        .query_row(
            "SELECT count(*),COALESCE(MAX(seq),?1) FROM events WHERE seq>?1 AND seq<=?2",
            params![current, boundary.end],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(sql_error)?;
    let (total, oldest): (i64, Option<i64>) = db
        .query_row(
            "SELECT count(*),min(CASE WHEN seq<=?1 OR seq>?2 THEN ts END) FROM events",
            params![current, projected],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .map_err(sql_error)?;
    let first_kept = db
        .query_row(
            "SELECT seq,ts FROM events WHERE seq>?1 ORDER BY seq LIMIT 1",
            [projected],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let mut result = row(
        "events",
        "contiguous event prefix",
        deleted,
        total - deleted,
        oldest,
    );
    if let Some(cutoff) = cutoff {
        protect(
            &mut result,
            "below_current_floor",
            count(
                db,
                "SELECT count(*) FROM events WHERE seq<=?1 AND ts<=?2",
                params![current, cutoff],
            )?,
        );
    }
    if let (Some(cutoff), Some(reason)) = (cutoff, boundary.blocker) {
        protect(
            &mut result,
            reason,
            count(
                db,
                "SELECT count(*) FROM events WHERE seq>?1 AND ts<=?2",
                params![projected, cutoff],
            )?,
        );
    }
    Ok((result, projected, first_kept))
}

fn processes(db: &Connection, cutoff: Option<i64>) -> Result<RetentionPreviewRow> {
    let ledger = "SELECT rowid,updated_at FROM processes";
    let Some(cutoff) = cutoff else {
        return summary(db, "processes", "process ledger", ledger, NONE, []);
    };
    let candidates = process_retention_candidates(false);
    let mut result = summary(
        db,
        "processes",
        "process ledger",
        ledger,
        &format!("SELECT rowid FROM ({candidates}) WHERE eligible"),
        [cutoff],
    )?;
    let age = count(
        db,
        &format!("SELECT count(*) FROM processes WHERE {PROCESS_AGE}"),
        [cutoff],
    )?;
    let custody = count(
        db,
        &format!("SELECT count(*) FROM ({candidates}) WHERE NOT eligible"),
        [cutoff],
    )?;
    let outside = age - custody - result.would_delete;
    protect(&mut result, "owned_or_nonexited_evidence", outside);
    protect(&mut result, "custody", custody);
    Ok(result)
}

fn commands(db: &Connection, cutoff: Option<i64>) -> Result<RetentionPreviewRow> {
    let Some(cutoff) = cutoff else {
        return summary(
            db,
            "commands",
            "whole command sets",
            "SELECT id,created_at FROM commands",
            NONE,
            [],
        );
    };
    let mut deleted = HashSet::new();
    let mut kept = BTreeMap::new();
    let mut select = db
        .prepare(PRUNE_COMMAND_CANDIDATES_FIRST)
        .map_err(sql_error)?;
    let mut roots = select.query(params![cutoff, ALL_ROWS]).map_err(sql_error)?;
    while let Some(root) = roots.next().map_err(sql_error)? {
        let root: String = root.get(0).map_err(sql_error)?;
        if let Some(members) = bounded_command_set(db, &root)? {
            if let Some(reason) = command_set_protection(&members, cutoff) {
                *kept.entry(reason.to_owned()).or_insert(0) += members.len() as i64;
            } else {
                deleted.extend(members.into_iter().map(|m| m.id));
            }
        } else {
            let members = count(
                db,
                "WITH RECURSIVE tree(id) AS (
                SELECT ?1 UNION SELECT c.id FROM commands c JOIN tree t ON c.parent_command=t.id
            ) SELECT count(*) FROM tree",
                [root],
            )?;
            *kept.entry("whole_set_limit".to_owned()).or_insert(0) += members;
        }
    }
    let mut sql = db
        .prepare("SELECT id,created_at FROM commands ORDER BY created_at,id")
        .map_err(sql_error)?;
    let mut found = sql.query([]).map_err(sql_error)?;
    let mut oldest = None;
    while let Some(candidate) = found.next().map_err(sql_error)? {
        let id: String = candidate.get(0).map_err(sql_error)?;
        if !deleted.contains(&id) {
            oldest = Some(candidate.get(1).map_err(sql_error)?);
            break;
        }
    }
    Ok(RetentionPreviewRow {
        protected: kept,
        ..row(
            "commands",
            "whole command sets",
            deleted.len() as i64,
            count(db, "SELECT count(*) FROM commands", [])? - deleted.len() as i64,
            oldest,
        )
    })
}

fn mail(db: &Connection, cutoff: Option<i64>, rows: &mut Vec<RetentionPreviewRow>) -> Result<()> {
    let acknowledged =
        format!("SELECT delivery_id,admitted_at FROM messages WHERE {MAIL_ACKNOWLEDGED}");
    let advisory = format!(
        "SELECT delivery_id,admitted_at FROM messages WHERE {MAIL_UNREAD} AND {LEGACY_ATTENTION_ADVISORY_MAIL_PREDICATE}"
    );
    let unread = format!(
        "SELECT delivery_id,admitted_at FROM messages WHERE {MAIL_UNREAD} AND NOT ({LEGACY_ATTENTION_ADVISORY_MAIL_PREDICATE})"
    );
    let wake_ledger = "SELECT rowid,woken_at FROM attention_wakes";
    let Some(cutoff) = cutoff else {
        rows.push(summary(
            db,
            "messages",
            "acknowledged",
            &acknowledged,
            NONE,
            [],
        )?);
        rows.push(summary(
            db,
            "messages",
            "legacy advisories",
            &advisory,
            NONE,
            [],
        )?);
        rows.push(summary(
            db,
            "messages",
            "ordinary unread",
            &unread,
            NONE,
            [],
        )?);
        rows.push(summary(
            db,
            "attention_wakes",
            "thread wake ledger",
            wake_ledger,
            NONE,
            [],
        )?);
        return Ok(());
    };
    let legacy_candidates = legacy_advisory_retention_candidates(false);
    let legacy_selected = format!("SELECT delivery_id FROM ({legacy_candidates}) WHERE eligible");
    let ack_selected = prune_acknowledged_mail_select();
    let mut ack = summary(
        db,
        "messages",
        "acknowledged",
        &acknowledged,
        &ack_selected,
        params![cutoff, ALL_ROWS],
    )?;
    protect(
        &mut ack,
        "inline_ack",
        count(
            db,
            &format!(
                "SELECT count(*) FROM messages WHERE {MAIL_ACKNOWLEDGED} AND {ACK_AGE} AND NOT ({ACK_UNPINNED})"
            ),
            [cutoff],
        )?,
    );
    rows.push(ack);
    let mut legacy = summary(
        db,
        "messages",
        "legacy advisories",
        &advisory,
        &legacy_selected,
        [cutoff],
    )?;
    let held = count(
        db,
        &format!("SELECT count(*) FROM ({legacy_candidates}) WHERE NOT eligible"),
        [cutoff],
    )?;
    protect(&mut legacy, "live_recipient", held);
    rows.push(legacy);
    let mut ordinary = summary(db, "messages", "ordinary unread", &unread, NONE, [])?;
    protect(
        &mut ordinary,
        "unread",
        count(
            db,
            &format!("SELECT count(*) FROM ({unread}) a WHERE {UNREAD_AGE}"),
            [cutoff],
        )?,
    );
    rows.push(ordinary);

    let remaining = format!(
        "(SELECT thread FROM messages WHERE delivery_id NOT IN (
        SELECT * FROM ({ack_selected}) UNION ALL {legacy_selected}
    ))"
    );
    rows.push(summary(
        db,
        "attention_wakes",
        "thread wake ledger",
        wake_ledger,
        &orphan_attention_wake_select(&remaining),
        params![cutoff, ALL_ROWS],
    )?);
    Ok(())
}
