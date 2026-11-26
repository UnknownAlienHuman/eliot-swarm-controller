use super::owner_inbox::OwnerInboxItemSink;
use super::{
    Error, Result, Store, TransactionBehavior, canonical, command_binding, commit_transaction,
    decode_json, digest_id_bound, encode_json, insert_write_tx, sql_error, validate_id_age,
    write_binding,
};
use eliot_core::api::{
    CommandWithoutDispatcherStatus, OwnerAction, OwnerDecideRequest, OwnerInboxItem,
    OwnerInboxItemRef, OwnerInboxKind, SwarmLiveness,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::Path,
    time::Duration,
};

const METHOD: &str = "owner.decide";
mod work_not_in_git;
pub(super) use work_not_in_git::recorded_missing_work;
pub use work_not_in_git::{MissingWork, WorkCommitProof, WorkCommitRequest};

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
    pub owner_inbox: BTreeMap<OwnerInboxKind, usize>,
    pub waiting_over_24h: usize,
    pub liveness: SwarmLiveness,
    pub command_without_dispatcher: CommandWithoutDispatcherStatus,
    pub goals_without_progress: usize,
    pub repeated_failure_inbox_items: usize,
}

pub struct OwnerInboxPage {
    pub after: Option<String>,
    pub aged_only: bool,
    pub limit: usize,
}

struct OwnerProjectionSink<'s, 'c> {
    now: i64,
    confirmed: &'c StuckStates,
    snoozed: HashSet<(OwnerInboxKind, &'s str, &'s str)>,
    page: Option<OwnerInboxPage>,
    items: Vec<OwnerInboxItem>,
    page_groups: HashMap<String, OwnerInboxItem>,
    counted_groups: HashSet<String>,
    owner_inbox: BTreeMap<OwnerInboxKind, usize>,
    waiting_over_24h: usize,
    unreportable_stuck: usize,
    unreportable_reasons: HashMap<String, usize>,
    command_without_dispatcher_count: usize,
    oldest_command_without_dispatcher: Option<(i64, String)>,
    goals_without_progress: usize,
    repeated_failure_inbox_items: usize,
}

impl OwnerProjectionSink<'_, '_> {
    fn push_page_item(&mut self, item: OwnerInboxItem) {
        let Some(page) = &self.page else {
            return;
        };
        if let Some(after) = &page.after
            && item.page_cursor() <= *after
        {
            return;
        }
        let index = self
            .items
            .partition_point(|existing| existing.page_key() < item.page_key());
        if index > page.limit {
            return;
        }
        if self.items.len() > page.limit {
            self.items.pop();
        }
        self.items.insert(index, item);
    }

    fn collect_page_item(&mut self, mut item: OwnerInboxItem) {
        let Some(page) = &self.page else { return };
        if page.aged_only && !item.waiting_over_24h() {
            return;
        }
        let Some(group) = &item.group else {
            self.push_page_item(item);
            return;
        };
        match self.page_groups.entry(group.id.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(item);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let current = entry.get_mut();
                let affected = current.group.as_ref().map_or(1, |group| group.affected) + 1;
                if item.age_ms > current.age_ms
                    || (item.age_ms == current.age_ms && item.subject < current.subject)
                {
                    std::mem::swap(current, &mut item);
                }
                if let Some(group) = &mut current.group {
                    group.affected = affected;
                }
            }
        }
    }
}

fn count_inbox_item(
    counts: &mut BTreeMap<OwnerInboxKind, usize>,
    groups: &mut HashSet<String>,
    item: &OwnerInboxItem,
) {
    if item
        .group
        .as_ref()
        .is_none_or(|group| groups.insert(group.id.clone()))
    {
        *counts.entry(item.kind).or_default() += 1;
    }
}

fn is_snoozed(item: &OwnerInboxItem, snoozed: &HashSet<(OwnerInboxKind, &str, &str)>) -> bool {
    !item.has_unknown_outcome()
        && snoozed.contains(&(item.kind, item.subject.as_str(), item.revision.as_str()))
}

impl OwnerInboxItemSink for OwnerProjectionSink<'_, '_> {
    fn push(&mut self, item: OwnerInboxItem) {
        if !is_reportable(&item, self.now, self.confirmed) {
            self.unreportable_stuck += 1;
            *self.unreportable_reasons.entry(item.reason).or_default() += 1;
            return;
        }

        self.waiting_over_24h += usize::from(item.waiting_over_24h());
        if item.kind == OwnerInboxKind::Stuck && item.reason == "command_without_dispatcher" {
            self.command_without_dispatcher_count += 1;
            // project() sorts by kind and subject before selecting the oldest;
            // retain that first subject when ages tie.
            if self
                .oldest_command_without_dispatcher
                .as_ref()
                .is_none_or(|(age, subject)| {
                    item.age_ms > *age || (item.age_ms == *age && item.subject < *subject)
                })
            {
                self.oldest_command_without_dispatcher = Some((item.age_ms, item.subject.clone()));
            }
        }

        if is_snoozed(&item, &self.snoozed) {
            return;
        }
        count_inbox_item(&mut self.owner_inbox, &mut self.counted_groups, &item);
        if item.kind == OwnerInboxKind::BlockedGoal
            || (item.kind == OwnerInboxKind::Stuck && item.subject.starts_with("goal:"))
        {
            self.goals_without_progress += 1;
        }
        if item.reason == super::owner_inbox::REPEATED_FAILURE_REASON {
            self.repeated_failure_inbox_items += 1;
        }
        self.collect_page_item(item);
    }
}

struct OwnerInboxCountSink<'s, 'c> {
    counts: BTreeMap<OwnerInboxKind, usize>,
    groups: HashSet<String>,
    snoozed: HashSet<(OwnerInboxKind, &'s str, &'s str)>,
    confirmed: &'c StuckStates,
    now: i64,
}

impl OwnerInboxItemSink for OwnerInboxCountSink<'_, '_> {
    fn push(&mut self, item: OwnerInboxItem) {
        if !is_reportable(&item, self.now, self.confirmed) || is_snoozed(&item, &self.snoozed) {
            return;
        }
        count_inbox_item(&mut self.counts, &mut self.groups, &item);
    }
}

pub(crate) fn is_reportable(item: &OwnerInboxItem, now: i64, confirmed: &StuckStates) -> bool {
    item.kind != OwnerInboxKind::Stuck
        // A persisted interrupted fence already establishes the wait.
        || item.reason == super::owner_inbox::SWITCH_FENCE_NEEDS_OWNER
        // Repeated failures are already grouped by cause in their projection;
        // waiting for a second liveness sample would delay the alert.
        || item.reason == super::owner_inbox::REPEATED_FAILURE_REASON
        // The startup probe-recovery attempt already establishes the wait for
        // a fenced unknown artifact; a second sample would delay the alert.
        || item.reason == super::owner_inbox::PROBE_RECOVERY_NEEDS_OWNER
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

fn repeat_occurrence_retained(connection: &Connection, row: &OwnerInboxItemRef) -> Result<bool> {
    if row.kind == OwnerInboxKind::Stuck
        && let Some(cause) = row.subject.strip_prefix("repeat:")
    {
        // The repeat worker can renew a quiet deadline from an observation
        // queued before it elapsed. Keep the snooze until the row is removed.
        return connection
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM repeat_failure_alerts WHERE cause = ?1)",
                [cause],
                |row| row.get(0),
            )
            .map_err(sql_error);
    }
    Ok(false)
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
    /// Return the retained rows closing the existing SEND outcome gate for a
    /// queued send, for read-only diagnosis and Owner projections.
    pub fn queued_send_gate_blockers(
        &self,
        command_id: &str,
    ) -> Result<Vec<super::SendOutcomeGateBlocker>> {
        super::queued_send_gate_blockers(&self.connection, command_id)
    }

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

    /// Liveness, backlog age and command-without-dispatcher counts include snoozed items.
    /// Goal and repeated-failure counts reflect the visible inbox items.
    pub fn read_owner_projection(
        path: &Path,
        now: i64,
        confirmed: &StuckStates,
        page: Option<OwnerInboxPage>,
    ) -> Result<OwnerProjection> {
        read_inbox_snapshot(path, |connection| {
            Self::owner_projection(connection, now, confirmed, page)
        })
    }

    /// Read the Owner view inside the caller's read-only snapshot.
    pub fn owner_projection_from_snapshot(
        &self,
        now: i64,
        confirmed: &StuckStates,
        page: Option<OwnerInboxPage>,
    ) -> Result<OwnerProjection> {
        Self::owner_projection(&self.connection, now, confirmed, page)
    }

    fn owner_projection(
        connection: &Connection,
        now: i64,
        confirmed: &StuckStates,
        page: Option<OwnerInboxPage>,
    ) -> Result<OwnerProjection> {
        let snoozes = inbox_snoozes(connection)?;
        let snoozed = snoozes
            .iter()
            .map(|r| (r.kind, r.subject.as_str(), r.revision.as_str()))
            .collect();
        let mut sink = OwnerProjectionSink {
            now,
            confirmed,
            snoozed,
            page,
            items: Vec::new(),
            page_groups: HashMap::new(),
            counted_groups: HashSet::new(),
            owner_inbox: OwnerInboxKind::ALL
                .into_iter()
                .map(|kind| (kind, 0))
                .collect(),
            waiting_over_24h: 0,
            unreportable_stuck: 0,
            unreportable_reasons: HashMap::new(),
            command_without_dispatcher_count: 0,
            oldest_command_without_dispatcher: None,
            goals_without_progress: 0,
            repeated_failure_inbox_items: 0,
        };
        let mut liveness = super::owner_inbox::project_into(connection, now, &mut sink)?;
        for item in std::mem::take(&mut sink.page_groups).into_values() {
            sink.push_page_item(item);
        }
        liveness.stuck = liveness.stuck.saturating_sub(sink.unreportable_stuck);
        for (reason, count) in &sink.unreportable_reasons {
            if let Some(current) = liveness.reasons.get_mut(reason) {
                *current = current.saturating_sub(*count);
            }
        }
        liveness.reasons.retain(|_, count| *count > 0);
        let command_without_dispatcher = match sink.oldest_command_without_dispatcher {
            Some((age_ms, subject)) => {
                let id = subject
                    .strip_prefix("command:")
                    .filter(|id| !id.is_empty())
                    .ok_or_else(|| Error::new("corrupt", "invalid command liveness subject"))?;
                CommandWithoutDispatcherStatus {
                    count: sink.command_without_dispatcher_count,
                    oldest_id: Some(id.to_owned()),
                    oldest_age_ms: Some(age_ms),
                }
            }
            None => CommandWithoutDispatcherStatus::default(),
        };
        Ok(OwnerProjection {
            items: sink.items,
            owner_inbox: sink.owner_inbox,
            waiting_over_24h: sink.waiting_over_24h,
            liveness,
            command_without_dispatcher,
            goals_without_progress: sink.goals_without_progress,
            repeated_failure_inbox_items: sink.repeated_failure_inbox_items,
        })
    }

    /// Count the same visible, unsnoozed inbox projection inside the caller's
    /// read-only snapshot. Pooled sidebar polls reuse the warm reader
    /// connection instead of opening a fresh one per call; the projection,
    /// confirmation and snooze rules are unchanged.
    pub fn owner_inbox_counts_from_snapshot(
        &self,
        now: i64,
        confirmed: &StuckStates,
    ) -> Result<BTreeMap<OwnerInboxKind, usize>> {
        Self::owner_inbox_counts(&self.connection, now, confirmed)
    }

    fn owner_inbox_counts(
        connection: &Connection,
        now: i64,
        confirmed: &StuckStates,
    ) -> Result<BTreeMap<OwnerInboxKind, usize>> {
        let snoozes = inbox_snoozes(connection)?;
        let snoozed = snoozes
            .iter()
            .map(|row| (row.kind, row.subject.as_str(), row.revision.as_str()))
            .collect();
        let mut sink = OwnerInboxCountSink {
            counts: OwnerInboxKind::ALL
                .into_iter()
                .map(|kind| (kind, 0))
                .collect(),
            groups: HashSet::new(),
            snoozed,
            confirmed,
            now,
        };
        super::owner_inbox::project_into(connection, now, &mut sink)?;
        Ok(sink.counts)
    }

    pub fn read_owner_inbox_sweep(path: &Path, now: i64) -> Result<OwnerInboxSweep> {
        read_inbox_snapshot(path, |connection| {
            let (items, _) = super::owner_inbox::project(connection, now)?;
            let subjects = items
                .iter()
                .map(|item| (item.kind, item.subject.as_str()))
                .collect::<HashSet<_>>();
            let mut retired_snoozes = Vec::new();
            for row in inbox_snoozes(connection)? {
                if !subjects.contains(&(row.kind, row.subject.as_str()))
                    && !repeat_occurrence_retained(connection, &row)?
                {
                    retired_snoozes.push(row);
                    if retired_snoozes.len() == 64 {
                        break;
                    }
                }
            }
            let candidates = items
                .iter()
                .filter(|item| {
                    item.kind == OwnerInboxKind::Stuck
                        && item.reason != super::owner_inbox::REPEATED_FAILURE_REASON
                        // A fenced probe artifact is already reportable from the
                        // startup recovery attempt; a second liveness sample only
                        // duplicates the daemon log line.
                        && item.reason != super::owner_inbox::PROBE_RECOVERY_NEEDS_OWNER
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
                if !repeat_occurrence_retained(&tx, row)?
                    && super::owner_inbox::project_item(&tx, row.kind, &row.subject, now)?.is_none()
                {
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
            if item.has_unknown_outcome() {
                return Err(Error::new(
                    "bad_state",
                    "an unknown outcome remains visible until resolved",
                ));
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
        insert_write_tx(&tx, &request.id, "owner", METHOD, &digest, &result, now)
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
            "checkpoint.restore.abandon" => (&mut action.args, supplied, &["confirm", "reason"]),
            "workspace.verify_work_commit" => {
                (&mut action.args, supplied, &["retained_ref", "commit"])
            }
            "provider.clear_hold" => (&mut action.args, supplied, &["corrective_evidence"]),
            "artifact.reconcile" => (&mut action.args, supplied, &["release_custody"]),
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
