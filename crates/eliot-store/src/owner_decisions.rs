use super::{
    Error, Result, Store, TransactionBehavior, canonical, command_binding, commit_transaction,
    decode_json, digest_id_bound, encode_json, sql_error, validate_id_age, write_binding,
};
use eliot_core::api::{
    CommandWithoutDispatcherStatus, OwnerAction, OwnerDecideRequest, OwnerInboxItem,
    OwnerInboxItemRef, OwnerInboxKind, SwarmLiveness,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::Duration,
};

const METHOD: &str = "owner.decide";

/// The existing writes table binds the decision ID and its exact inner
/// mutation ID. An admitted command replays through its own ordinary receipt.
#[derive(Serialize, Deserialize)]
pub enum OwnerDecisionAdmission {
    Snoozed,
    Action { id: String, action: OwnerAction },
}

pub type StuckStates = HashMap<String, StuckState>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StuckState {
    pub reason: String,
    due_at: Option<i64>,
}

impl StuckState {
    fn due_at(item: &OwnerInboxItem, now: i64) -> Option<i64> {
        // An overdue timer or assignment can advance to a different
        // occurrence between samples without changing the reason.
        (item.subject.starts_with("timer:") || item.subject.starts_with("assignment:"))
            .then(|| now.saturating_sub(item.age_ms))
    }
}

pub struct OwnerInboxSweep {
    pub candidates: StuckStates,
    pub retired_snoozes: Vec<OwnerInboxItemRef>,
}

pub struct OwnerProjection {
    pub items: Vec<OwnerInboxItem>,
    pub liveness: SwarmLiveness,
    pub command_without_dispatcher: CommandWithoutDispatcherStatus,
    pub goals_without_progress: usize,
    pub repeated_failure_inbox_items: usize,
}

pub(crate) fn is_reportable(item: &OwnerInboxItem, now: i64, confirmed: &StuckStates) -> bool {
    item.kind != OwnerInboxKind::Stuck
        // A persisted interrupted fence already establishes the wait.
        || item.reason == super::owner_inbox::SWITCH_FENCE_NEEDS_OWNER
        // Repeated failures are already grouped by cause in their projection;
        // waiting for a second liveness sample would delay the alert.
        || item.reason == super::owner_inbox::REPEATED_FAILURE_REASON
        || confirmed.get(&item.subject).is_some_and(|state| {
            state.reason == item.reason && state.due_at == StuckState::due_at(item, now)
        })
}

fn inbox_snoozes(connection: &Connection) -> Result<Vec<OwnerInboxItemRef>> {
    let mut statement = connection
        .prepare_cached("SELECT kind, subject, revision FROM owner_inbox_snoozes")
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(sql_error)?;
    rows.map(|row| {
        let (kind, subject, revision) = row.map_err(sql_error)?;
        let kind = OwnerInboxKind::ALL
            .into_iter()
            .find(|value| value.as_str() == kind)
            .ok_or_else(|| Error::new("corrupt", "invalid inbox snooze kind"))?;
        Ok(OwnerInboxItemRef {
            kind,
            subject,
            revision,
        })
    })
    .collect()
}

fn read_inbox_snapshot<T>(path: &Path, read: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    #[cfg(windows)]
    let path = &eliot_core::verbatim_path(path);
    let connection =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(sql_error)?;
    connection
        .busy_timeout(Duration::from_secs(1))
        .map_err(sql_error)?;
    let tx = connection.unchecked_transaction().map_err(sql_error)?;
    read(&tx)
}

impl Store {
    /// Check that a sampled reason still names the current command before mailing its issuer.
    pub fn command_still_stuck(&self, subject: &str, reason: &str, now: i64) -> Result<bool> {
        Ok(
            super::owner_inbox::project_item(
                &self.connection,
                OwnerInboxKind::Stuck,
                subject,
                now,
            )?
            .is_some_and(|item| item.reason == reason),
        )
    }

    /// Fleet counts include snoozed items. The inbox contains visible items.
    pub fn read_owner_projection(
        path: &Path,
        now: i64,
        confirmed: &StuckStates,
    ) -> Result<OwnerProjection> {
        read_inbox_snapshot(path, |connection| {
            let (mut items, mut liveness) = super::owner_inbox::project(connection, now)?;
            items.retain(|item| {
                if is_reportable(item, now, confirmed) {
                    return true;
                }
                liveness.stuck = liveness.stuck.saturating_sub(1);
                if let Some(count) = liveness.reasons.get_mut(&item.reason) {
                    *count = count.saturating_sub(1);
                }
                false
            });
            liveness.reasons.retain(|_, count| *count > 0);
            let mut command_without_dispatcher_count = 0;
            let mut oldest_command_without_dispatcher: Option<&OwnerInboxItem> = None;
            for item in items.iter().filter(|item| {
                item.kind == OwnerInboxKind::Stuck && item.reason == "command_without_dispatcher"
            }) {
                command_without_dispatcher_count += 1;
                if oldest_command_without_dispatcher
                    .is_none_or(|oldest| item.age_ms > oldest.age_ms)
                {
                    oldest_command_without_dispatcher = Some(item);
                }
            }
            let command_without_dispatcher = match oldest_command_without_dispatcher {
                Some(item) => {
                    let id = item
                        .subject
                        .strip_prefix("command:")
                        .filter(|id| !id.is_empty())
                        .ok_or_else(|| Error::new("corrupt", "invalid command liveness subject"))?;
                    CommandWithoutDispatcherStatus {
                        count: command_without_dispatcher_count,
                        oldest_id: Some(id.to_owned()),
                        oldest_age_ms: Some(item.age_ms),
                    }
                }
                None => CommandWithoutDispatcherStatus::default(),
            };
            let goals_without_progress = items
                .iter()
                .filter(|item| {
                    item.kind == OwnerInboxKind::BlockedGoal
                        || (item.kind == OwnerInboxKind::Stuck && item.subject.starts_with("goal:"))
                })
                .count();
            let repeated_failure_inbox_items = items
                .iter()
                .filter(|item| item.reason == super::owner_inbox::REPEATED_FAILURE_REASON)
                .count();
            let snoozes = inbox_snoozes(connection)?;
            let snoozed = snoozes
                .iter()
                .map(|r| (r.kind, r.subject.as_str(), r.revision.as_str()))
                .collect::<HashSet<_>>();
            items.retain(|item| {
                !snoozed.contains(&(item.kind, item.subject.as_str(), item.revision.as_str()))
            });
            Ok(OwnerProjection {
                items,
                liveness,
                command_without_dispatcher,
                goals_without_progress,
                repeated_failure_inbox_items,
            })
        })
    }

    pub fn read_owner_inbox_sweep(path: &Path, now: i64) -> Result<OwnerInboxSweep> {
        read_inbox_snapshot(path, |connection| {
            let (items, _) = super::owner_inbox::project(connection, now)?;
            let subjects = items
                .iter()
                .map(|item| (item.kind, item.subject.as_str()))
                .collect::<HashSet<_>>();
            let retired_snoozes = inbox_snoozes(connection)?
                .into_iter()
                .filter(|row| !subjects.contains(&(row.kind, row.subject.as_str())))
                .take(64)
                .collect();
            let candidates = items
                .iter()
                .filter(|item| {
                    item.kind == OwnerInboxKind::Stuck
                        && item.reason != super::owner_inbox::REPEATED_FAILURE_REASON
                })
                .map(|item| {
                    (
                        item.subject.clone(),
                        StuckState {
                            reason: item.reason.clone(),
                            due_at: StuckState::due_at(item, now),
                        },
                    )
                })
                .collect();
            Ok(OwnerInboxSweep {
                candidates,
                retired_snoozes,
            })
        })
    }

    pub fn prune_owner_inbox_snoozes(
        &mut self,
        rows: &[OwnerInboxItemRef],
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            for row in rows {
                if super::owner_inbox::project_item(&tx, row.kind, &row.subject, now)?.is_none() {
                    tx.execute(
                        "DELETE FROM owner_inbox_snoozes WHERE kind=?1 AND subject=?2 AND revision=?3",
                        params![row.kind.as_str(), row.subject, row.revision],
                    ).map_err(sql_error)?;
                }
            }
            commit_transaction(tx, &self.write_faulted)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn admit_owner_decision(
        &mut self,
        request: &OwnerDecideRequest,
        action_id: String,
        now: i64,
        confirmed: &StuckStates,
    ) -> Result<OwnerDecisionAdmission> {
        self.ensure_writer_healthy()?;
        let result = self.admit_owner_decision_inner(request, action_id, now, confirmed);
        self.fault_on_persistence_error(&result);
        result
    }

    fn admit_owner_decision_inner(
        &mut self,
        request: &OwnerDecideRequest,
        action_id: String,
        now: i64,
        confirmed: &StuckStates,
    ) -> Result<OwnerDecisionAdmission> {
        let digest = canonical::digest(&serde_json::json!(request))?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if let Some((principal, method, stored_digest, result)) = write_binding(&tx, &request.id)? {
            if principal != "owner" || method != METHOD || stored_digest != digest {
                return Err(decision_id_conflict());
            }
            return serde_json::from_value(decode_json(&result)?)
                .map_err(|_| Error::new("internal", "stored owner decision is invalid"));
        }
        if command_binding(&tx, &request.id)?.is_some() || digest_id_bound(&tx, &request.id)? {
            return Err(decision_id_conflict());
        }
        validate_id_age(&request.id, now)?;
        let item = current_item(&tx, &request.item, now)?;
        if !is_reportable(&item, now, confirmed) {
            return Err(stale_item());
        }
        let admission = if request.choice == "snooze" {
            if request.args.is_some() {
                return Err(Error::new("invalid", "snooze accepts no arguments"));
            }
            tx.execute(
                "INSERT INTO owner_inbox_snoozes(kind, subject, revision) VALUES (?1, ?2, ?3)
                 ON CONFLICT(kind, subject) DO UPDATE SET revision = excluded.revision",
                params![item.kind.as_str(), item.subject, item.revision],
            )
            .map_err(sql_error)?;
            OwnerDecisionAdmission::Snoozed
        } else {
            let mut action = if request.choice == "recommended" {
                item.recommended
            } else {
                item.choices
                    .into_iter()
                    .find(|action| action.choice == request.choice)
            }
            .ok_or_else(|| Error::new("invalid", "choice is unavailable for this item"))?;
            complete_action(
                &mut action,
                request.args.as_ref().and_then(Value::as_object),
            )?;
            OwnerDecisionAdmission::Action {
                id: action_id,
                action,
            }
        };
        let result = encode_json(&admission)?;
        if result.len() > super::MAX_EVENT_BYTES {
            return Err(Error::new("frame_too_large", "owner choice exceeds 64 KiB"));
        }
        tx.execute(
            "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
             VALUES (?1, 'owner', ?2, ?3, ?4, ?5)",
            params![request.id, METHOD, digest, result, now],
        )
        .map_err(sql_error)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(admission)
    }

    /// The caller holds the daemon writer through the original mutation's
    /// admission. Before any effect, the same item must still be current.
    pub fn check_owner_decision(
        &self,
        item: &OwnerInboxItemRef,
        action_id: &str,
        now: i64,
    ) -> Result<()> {
        if command_binding(&self.connection, action_id)?.is_some()
            || write_binding(&self.connection, action_id)?.is_some()
        {
            // The original mutation validates the retained ID's method and
            // payload before replaying the result.
            return Ok(());
        }
        current_item(&self.connection, item, now).map(|_| ())
    }
}

fn current_item(
    connection: &Connection,
    item: &OwnerInboxItemRef,
    now: i64,
) -> Result<OwnerInboxItem> {
    let snooze: Option<String> = connection
        .query_row(
            "SELECT revision FROM owner_inbox_snoozes WHERE kind = ?1 AND subject = ?2",
            params![item.kind.as_str(), item.subject],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if snooze.as_ref() != Some(&item.revision)
        && let Some(current) =
            super::owner_inbox::project_item(connection, item.kind, &item.subject, now)?
        && current.matches(item)
    {
        return Ok(current);
    }
    Err(stale_item())
}

fn stale_item() -> Error {
    Error::new(
        "stale_item",
        "owner inbox item changed; read the inbox again",
    )
}

fn decision_id_conflict() -> Error {
    Error::new(
        "id_conflict",
        "decision id is already bound to another mutation",
    )
}

fn complete_action(action: &mut OwnerAction, supplied: Option<&Map<String, Value>>) -> Result<()> {
    let approval = action.method == "command.submit"
        && matches!(action.args["verb"].as_str(), Some("approve" | "deny"));
    let (target, supplied, permitted): (&mut Value, Option<&Map<String, Value>>, &[&str]) =
        match action.method.as_str() {
            "command.resolve" | "turn.resolve" => (&mut action.args, supplied, &["reason"]),
            "provider.clear_hold" => (&mut action.args, supplied, &["corrective_evidence"]),
            "command.submit" if approval || action.args["verb"] == "answer" => {
                let supplied = match supplied {
                    Some(args) if args.contains_key("args") && args.len() == 1 => Some(
                        args.get("args")
                            .and_then(Value::as_object)
                            .ok_or_else(invalid_args)?,
                    ),
                    other => other,
                };
                let fields: &[&str] = if approval {
                    &["choice_id", "feedback"]
                } else {
                    &["answers"]
                };
                (&mut action.args["args"], supplied, fields)
            }
            _ => (&mut action.args, supplied, &[]),
        };
    if let Some(supplied) = supplied {
        for (key, value) in supplied {
            if !permitted.contains(&key.as_str())
                || !target.get(key).is_some_and(Value::is_null)
                || value.is_null()
            {
                return Err(invalid_args());
            }
            target[key] = value.clone();
        }
    }
    if approval {
        eliot_core::requests::ApprovalArgs::parse(target)?;
    } else if permitted
        .iter()
        .any(|key| target.get(key).is_some_and(Value::is_null))
    {
        return Err(Error::new(
            "invalid",
            "choice requires arguments shown in the inbox",
        ));
    }
    Ok(())
}

fn invalid_args() -> Error {
    Error::new(
        "invalid",
        "decision arguments may only fill the choice's input fields",
    )
}
