//! Typed, manager-scoped automation definitions and patch semantics.

use super::work_dispatch::WorkDispatchLaunchSettings;
use super::{
    actions::{AutomationCause, AutomationStep, supported_action_for},
    event_rules::{self, EventRule},
    intake::EventReceipt,
};
use crate::error::{Error, Result};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub(crate) const ENTRY_SCHEMA_VERSION: u32 = 1;
pub(crate) const MAX_AUTOMATIONS_PER_SCOPE: usize = 64;
pub(crate) const MAX_CHANGES_PER_APPLY: usize = 32;
pub(crate) const MAX_AUTOMATION_ID_BYTES: usize = 64;
pub(crate) const MAX_META_RECORD_BYTES: usize = 64 * 1024;
const TRANSFER_RECORD_PREFIX: &str = "automation:v1:transfer:record:";
const TRANSFER_SOURCE_PREFIX: &str = "automation:v1:transfer:source:";
const TRANSFER_TARGET_PREFIX: &str = "automation:v1:transfer:target:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationScope {
    /// Queue/pool filtering is recorded, but this source baseline has no
    /// work-pool resolver. A non-empty value is therefore a visible gap and
    /// blocks dispatch instead of widening the scope to the whole project.
    pub(crate) work_pool_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReviewSettings {
    #[serde(default)]
    pub(crate) profile: Option<String>,
    #[serde(default = "one_reviewer")]
    pub(crate) required_reviewers: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublicationSettings {
    pub(crate) target_ref: String,
    pub(crate) expected_old_ref: Option<String>,
    pub(crate) expected_create: bool,
}

/// One explicitly selected desired Issue label after exact candidate acceptance.
/// Repository and Issue identity come from the registered source's Task map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GithubProjectionSettings {
    pub(crate) source_id: String,
    pub(crate) label: String,
    pub(crate) present: bool,
}

impl GithubProjectionSettings {
    pub(crate) fn validate(&self) -> Result<()> {
        crate::github::protocol::validate_source_id(&self.source_id)?;
        crate::github::protocol::validate_managed_label(&self.label)
    }
}

/// Optional post-commit trigger for an existing ReviewDispatch action.
/// Presence selects one setup-issued HookSource; `AutomationEntry.enabled`
/// remains the sole enable switch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HookCommitSettings {
    pub(crate) source_id: String,
}

impl HookCommitSettings {
    fn validate(&self) -> Result<()> {
        let parsed = uuid::Uuid::parse_str(&self.source_id)
            .map_err(|_| Error::invalid("hook_commit.source_id must be a HookSource UUID"))?;
        if parsed.to_string() != self.source_id {
            return Err(Error::invalid(
                "hook_commit.source_id must use the canonical HookSource UUID form",
            ));
        }
        Ok(())
    }
}

impl PublicationSettings {
    fn validate(&self) -> Result<()> {
        if !crate::forge::valid_branch_ref(&self.target_ref)
            || self.expected_create == self.expected_old_ref.is_some()
            || self
                .expected_old_ref
                .as_deref()
                .is_some_and(|value| !crate::forge::valid_object_id(value))
        {
            return Err(Error::invalid(
                "publication settings require a valid full branch ref and exactly one of expected_old_ref or expected_create=true",
            ));
        }
        Ok(())
    }
}

/// One manager-selected shared Goal for terminal-turn progression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GoalProgressionSettings {
    pub(crate) goal_id: String,
}

/// One explicitly selected active script target for manager-selected durable
/// event triggers. Revision is captured from the active script head only when
/// the normal run admission is prepared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScriptRunSettings {
    pub(crate) script_id: String,
}

impl ScriptRunSettings {
    fn validate(&self) -> Result<()> {
        crate::scripts::manifest::validate_script_id(&self.script_id)
    }
}

impl GoalProgressionSettings {
    fn validate(&self) -> Result<()> {
        validate_name(
            &self.goal_id,
            "goal_progression.goal_id",
            crate::goals::MAX_GOAL_ID_BYTES,
        )
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AutomationEntry {
    pub(crate) schema_version: u32,
    pub(crate) automation_id: String,
    pub(crate) owner_manager_id: String,
    pub(crate) project_id: String,
    pub(crate) revision: i64,
    pub(crate) enabled: bool,
    #[serde(default)]
    pub(crate) preset: Option<String>,
    pub(crate) scope: AutomationScope,
    #[serde(default)]
    pub(crate) steps: Vec<AutomationStep>,
    #[serde(default)]
    pub(crate) work_dispatch: Option<WorkDispatchLaunchSettings>,
    #[serde(default)]
    pub(crate) goal_progression: Option<GoalProgressionSettings>,
    #[serde(default)]
    pub(crate) script_run: Option<ScriptRunSettings>,
    #[serde(default)]
    pub(crate) publication: Option<PublicationSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) github_projection: Option<GithubProjectionSettings>,
    #[serde(default)]
    pub(crate) cron: Option<crate::scheduler::CronSettings>,
    #[serde(default)]
    pub(crate) hook_commit: Option<HookCommitSettings>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) event_rules: Option<Vec<EventRule>>,
    pub(crate) review: ReviewSettings,
    pub(crate) created_at_ms: i64,
    pub(crate) updated_at_ms: i64,
}

fn one_reviewer() -> u32 {
    1
}

impl AutomationEntry {
    pub(crate) fn new(
        owner_manager_id: &str,
        project_id: &str,
        automation_id: &str,
        now_ms: i64,
    ) -> Self {
        Self {
            schema_version: ENTRY_SCHEMA_VERSION,
            automation_id: automation_id.to_owned(),
            owner_manager_id: owner_manager_id.to_owned(),
            project_id: project_id.to_owned(),
            revision: 1,
            enabled: false,
            preset: None,
            scope: AutomationScope { work_pool_id: None },
            steps: Vec::new(),
            work_dispatch: None,
            goal_progression: None,
            script_run: None,
            publication: None,
            github_projection: None,
            cron: None,
            hook_commit: None,
            event_rules: None,
            review: ReviewSettings {
                profile: None,
                required_reviewers: 1,
            },
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        }
    }

    pub(crate) fn value(&self) -> Result<Value> {
        serde_json::to_value(self).map_err(Into::into)
    }

    pub(crate) fn capability_gaps(&self) -> Vec<Value> {
        let mut gaps = Vec::new();
        for step in &self.steps {
            if let Some(gap) = step.capability_gap() {
                gaps.push(gap);
            }
        }
        if self.steps.contains(&AutomationStep::GithubProjection)
            && self.github_projection.is_none()
        {
            gaps.push(json!({
                "code":"github_projection_settings_required",
                "step":"github_projection",
                "reason":"select one registered source_id, Eliot label and explicit present state"
            }));
        }
        if self.scope.work_pool_id.is_some() {
            gaps.push(json!({
                "code":"work_pool_scope_unavailable",
                "reason":"the current Task source has no committed work-pool membership reader"
            }));
        }
        if self.steps.contains(&AutomationStep::GoalProgression) && self.goal_progression.is_none()
        {
            gaps.push(json!({
                "code":"goal_progression_settings_required",
                "step":"goal_progression",
                "reason":"select the exact existing shared Goal ID before enabling progression"
            }));
        }
        if self.steps.contains(&AutomationStep::ScriptRun) && self.script_run.is_none() {
            gaps.push(json!({
                "code":"script_run_settings_required",
                "step":"script_run",
                "reason":"select one owned script id and at least one exact durable event source/kind rule"
            }));
        } else if self.steps.contains(&AutomationStep::ScriptRun)
            && !self.script_run_event_rule_selected()
        {
            gaps.push(json!({
                "code":"script_run_trigger_required",
                "step":"script_run",
                "reason":"select at least one exact source_id/event_kind ScriptRun rule; unknown future kinds remain idle until a safe event becomes visible"
            }));
        }
        if self.steps.contains(&AutomationStep::WorkDispatch) {
            if self.work_dispatch.is_none() {
                gaps.push(json!({
                    "code":"work_dispatch_settings_required",
                    "step":"work_dispatch",
                    "reason":"select every launcher setting explicitly; no route, profile, workspace policy, or budget default is inferred"
                }));
            } else {
                gaps.push(json!({
                    "code":"launch_effect_qualification_pending",
                    "step":"work_dispatch",
                    "reason":"WorkDispatch can admit the normal retained launch Operation; hosted route, workspace, MCP, and productive runtime readiness remain separate launch gates"
                }));
            }
        }
        if self.steps.contains(&AutomationStep::ReviewDispatch) {
            if self.review.profile.is_none() {
                gaps.push(json!({
                    "code":"review_profile_required",
                    "step":"review_dispatch",
                    "reason":"select a registered reviewer profile before enabling review dispatch"
                }));
            }
            if self.review.required_reviewers != 1 {
                gaps.push(json!({
                    "code":"reviewer_count_unsupported",
                    "step":"review_dispatch",
                    "required_reviewers":self.review.required_reviewers,
                    "supported_reviewers":1
                }));
            }
            if self.review.profile.is_some()
                && supported_action_for(AutomationStep::ReviewDispatch).is_none()
            {
                gaps.push(json!({
                    "code":"review_assignment_consumer_unavailable",
                    "step":"review_dispatch"
                }));
            }
        }
        if self.steps.contains(&AutomationStep::Publication) && self.publication.is_none() {
            gaps.push(json!({
                "code":"publication_settings_required",
                "step":"publication",
                "reason":"select an exact target ref and expected old ref or explicit create before enabling publication"
            }));
        }
        if self.steps.contains(&AutomationStep::CheckRun) && self.cron.is_none() {
            gaps.push(json!({
                "code":"cron_settings_required",
                "step":"check_run",
                "reason":"select a validated cron calendar and exact CheckRun target before enabling cron"
            }));
        }
        gaps
    }

    pub(crate) fn review_dispatch_ready(&self) -> bool {
        self.enabled
            && self.steps.contains(&AutomationStep::ReviewDispatch)
            && self.task_submission_review_rule_selected()
            && self.review.profile.is_some()
            && self.review.required_reviewers == 1
            && self.scope.work_pool_id.is_none()
    }

    pub(crate) fn goal_progression_ready(&self) -> bool {
        self.enabled
            && self.steps.contains(&AutomationStep::GoalProgression)
            && self.goal_progression.is_some()
            && self.scope.work_pool_id.is_none()
    }
    pub(crate) fn work_dispatch_ready(&self) -> bool {
        self.enabled
            && self.steps.contains(&AutomationStep::WorkDispatch)
            && self.work_dispatch.is_some()
            && self.scope.work_pool_id.is_none()
    }

    pub(crate) fn publication_ready(&self) -> bool {
        self.enabled
            && self.steps.contains(&AutomationStep::Publication)
            && self.publication.is_some()
            && self.scope.work_pool_id.is_none()
    }

    pub(crate) fn github_projection_ready(&self) -> bool {
        self.enabled
            && self.steps.contains(&AutomationStep::GithubProjection)
            && self.github_projection.is_some()
            && self.scope.work_pool_id.is_none()
    }

    pub(crate) fn check_run_ready(&self) -> bool {
        self.enabled
            && self.steps.contains(&AutomationStep::CheckRun)
            && self.cron.is_some()
            && self.scope.work_pool_id.is_none()
    }

    pub(crate) fn script_run_ready(&self) -> bool {
        self.enabled
            && self.steps.contains(&AutomationStep::ScriptRun)
            && self.script_run.is_some()
            && self.script_run_event_rule_selected()
            && self.scope.work_pool_id.is_none()
    }

    pub(crate) fn task_submission_review_rule_selected(&self) -> bool {
        self.event_rules.as_ref().is_none_or(|rules| {
            rules.iter().any(|rule| {
                rule.action == super::event_rules::EventRuleAction::ReviewDispatch
                    && rule.is_task_submission_applied()
            })
        })
    }

    pub(crate) fn script_run_event_rule_selected(&self) -> bool {
        self.event_rules.as_ref().is_some_and(|rules| {
            rules.iter().any(|rule| {
                rule.action == super::event_rules::EventRuleAction::ScriptRun
                    && rule.validate_shape().is_ok()
            })
        })
    }

    pub(crate) fn accepts_task_submission_review_event(
        &self,
        receipt: &EventReceipt,
        cause: &AutomationCause,
    ) -> bool {
        self.event_rules.as_ref().is_none_or(|rules| {
            rules.iter().any(|rule| {
                rule.action == super::event_rules::EventRuleAction::ReviewDispatch
                    && rule.matches_receipt(receipt, cause)
            })
        })
    }

    pub(crate) fn accepts_task_submission_script_run_event(
        &self,
        receipt: &EventReceipt,
        cause: &AutomationCause,
    ) -> bool {
        self.event_rules.as_ref().is_some_and(|rules| {
            rules.iter().any(|rule| {
                rule.action == super::event_rules::EventRuleAction::ScriptRun
                    && rule.matches_receipt(receipt, cause)
            })
        })
    }

    pub(crate) fn accepts_script_run_event(
        &self,
        source_id: &str,
        event_kind: &str,
        status: Option<super::event_rules::EventStatus>,
    ) -> bool {
        self.event_rules.as_ref().is_some_and(|rules| {
            rules
                .iter()
                .any(|rule| rule.matches_safe_event(source_id, event_kind, status))
        })
    }

    pub(crate) fn selects_script_run_source_kind(&self, source_id: &str, event_kind: &str) -> bool {
        self.event_rules.as_ref().is_some_and(|rules| {
            rules
                .iter()
                .any(|rule| rule.selects_script_run_source_kind(source_id, event_kind))
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigChange {
    pub(crate) automation_id: String,
    pub(crate) expected_revision: i64,
    pub(crate) include_existing: bool,
    pub(crate) patch: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigRequest {
    pub(crate) project_id: String,
    pub(crate) preview_digest: Option<String>,
    pub(crate) changes: Vec<ConfigChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransferRequest {
    pub(crate) project_id: String,
    pub(crate) former_owner_manager_id: String,
    pub(crate) automation_id: String,
    pub(crate) expected_revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TransferProvenance {
    pub(crate) schema_version: u32,
    pub(crate) transfer_operation_id: String,
    pub(crate) project_id: String,
    pub(crate) automation_id: String,
    pub(crate) former_owner_manager_id: String,
    pub(crate) new_owner_manager_id: String,
    pub(crate) former_owner_revision: i64,
    pub(crate) new_owner_revision: i64,
    pub(crate) created_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferPointer {
    schema_version: u32,
    transfer_operation_id: String,
}

impl TransferRequest {
    pub(crate) fn parse(value: &Value) -> Result<Self> {
        crate::model::fields(
            value,
            &[
                "client_request_id",
                "project_id",
                "former_owner_manager_id",
                "automation_id",
                "expected_revision",
            ],
        )?;
        let project_id = crate::model::text(value, "project_id")?.to_owned();
        let former_owner_manager_id =
            crate::model::text(value, "former_owner_manager_id")?.to_owned();
        let automation_id = crate::model::text(value, "automation_id")?.to_owned();
        let expected_revision = value
            .get("expected_revision")
            .and_then(Value::as_i64)
            .ok_or_else(|| Error::invalid("expected_revision must be an integer"))?;
        if project_id.len() > 128 || project_id.chars().any(char::is_control) {
            return Err(Error::invalid("project_id is invalid"));
        }
        validate_name(&former_owner_manager_id, "former_owner_manager_id", 128)?;
        validate_automation_id(&automation_id)?;
        if expected_revision <= 0 {
            return Err(Error::invalid("expected_revision must be positive"));
        }
        Ok(Self {
            project_id,
            former_owner_manager_id,
            automation_id,
            expected_revision,
        })
    }
}

pub(crate) fn transfer_record_key(operation_id: &str) -> Result<String> {
    validate_name(operation_id, "transfer_operation_id", 128)?;
    Ok(format!("{TRANSFER_RECORD_PREFIX}{operation_id}"))
}

pub(crate) fn transfer_source_key(
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<String> {
    validate_automation_id(automation_id)?;
    Ok(format!(
        "{TRANSFER_SOURCE_PREFIX}{}:{automation_id}",
        scope_digest(owner, project)?
    ))
}

pub(crate) fn transfer_target_key(
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<String> {
    validate_automation_id(automation_id)?;
    Ok(format!(
        "{TRANSFER_TARGET_PREFIX}{}:{automation_id}",
        scope_digest(owner, project)?
    ))
}

pub(crate) fn transfer_pointer_value(operation_id: &str) -> Result<Value> {
    validate_name(operation_id, "transfer_operation_id", 128)?;
    Ok(serde_json::to_value(TransferPointer {
        schema_version: 1,
        transfer_operation_id: operation_id.to_owned(),
    })?)
}

pub(crate) fn transfer_record(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<TransferProvenance>> {
    let Some(value) = read_record(
        db,
        &transfer_record_key(operation_id)?,
        "automation ownership transfer",
    )?
    else {
        return Ok(None);
    };
    let record: TransferProvenance = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "automation ownership transfer fields are invalid",
        )
    })?;
    validate_transfer_record(&record, operation_id)?;
    Ok(Some(record))
}

pub(crate) fn transfer_from_source(
    db: &Connection,
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<Option<TransferProvenance>> {
    let Some(operation_id) =
        transfer_pointer(db, &transfer_source_key(owner, project, automation_id)?)?
    else {
        return Ok(None);
    };
    let record = transfer_record(db, &operation_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "automation retirement pointer has no transfer record",
        )
    })?;
    if record.former_owner_manager_id != owner
        || record.project_id != project
        || record.automation_id != automation_id
    {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "automation retirement pointer crosses its source scope",
        ));
    }
    Ok(Some(record))
}

pub(crate) fn transfer_into_target(
    db: &Connection,
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<Option<TransferProvenance>> {
    let Some(operation_id) =
        transfer_pointer(db, &transfer_target_key(owner, project, automation_id)?)?
    else {
        return Ok(None);
    };
    let record = transfer_record(db, &operation_id)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "automation incoming pointer has no transfer record",
        )
    })?;
    if record.new_owner_manager_id != owner
        || record.project_id != project
        || record.automation_id != automation_id
    {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "automation incoming pointer crosses its destination scope",
        ));
    }
    Ok(Some(record))
}

/// Return newest-to-oldest transfer provenance for the current entry. Each
/// edge is read through the sealed destination index and its single record.
pub(crate) fn transfer_lineage(
    db: &Connection,
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<Vec<TransferProvenance>> {
    let mut current_owner = owner.to_owned();
    let mut seen = BTreeSet::from([current_owner.clone()]);
    let mut lineage = Vec::new();
    while let Some(record) = transfer_into_target(db, &current_owner, project, automation_id)? {
        if !seen.insert(record.former_owner_manager_id.clone()) {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "automation ownership transfer lineage contains a cycle",
            ));
        }
        current_owner.clone_from(&record.former_owner_manager_id);
        lineage.push(record);
    }
    Ok(lineage)
}

/// Resolve the terminal owner of an old entry through explicit transfer
/// records. This is lineage only; callers must still revalidate current GM,
/// entry, Task and Operation scope before continuing an effect.
pub(crate) fn transfer_successors(
    db: &Connection,
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<Vec<TransferProvenance>> {
    let mut current_owner = owner.to_owned();
    let mut seen = BTreeSet::from([current_owner.clone()]);
    let mut lineage = Vec::new();
    while let Some(record) = transfer_from_source(db, &current_owner, project, automation_id)? {
        if !seen.insert(record.new_owner_manager_id.clone()) {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "automation ownership transfer lineage contains a cycle",
            ));
        }
        current_owner.clone_from(&record.new_owner_manager_id);
        lineage.push(record);
    }
    Ok(lineage)
}

pub(crate) fn require_not_transferred(
    db: &Connection,
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<()> {
    if transfer_from_source(db, owner, project, automation_id)?.is_some() {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_RETIRED",
            "a transferred former-owner snapshot cannot be edited or re-enabled",
        ));
    }
    Ok(())
}

pub(crate) fn validate_transfer_record(
    record: &TransferProvenance,
    operation_id: &str,
) -> Result<()> {
    if record.schema_version != 1
        || record.transfer_operation_id != operation_id
        || record.former_owner_manager_id == record.new_owner_manager_id
        || record.former_owner_revision <= 0
        || record.new_owner_revision != record.former_owner_revision.saturating_add(1)
        || record.created_at_ms < 0
        || validate_name(
            &record.former_owner_manager_id,
            "former_owner_manager_id",
            128,
        )
        .is_err()
        || validate_name(&record.new_owner_manager_id, "new_owner_manager_id", 128).is_err()
        || validate_name(&record.project_id, "project_id", 128).is_err()
        || validate_automation_id(&record.automation_id).is_err()
    {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "automation ownership transfer identity or revision is invalid",
        ));
    }
    Ok(())
}

fn transfer_pointer(db: &Connection, key: &str) -> Result<Option<String>> {
    let Some(value) = read_record(db, key, "automation transfer index")? else {
        return Ok(None);
    };
    let pointer: TransferPointer = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "automation transfer index fields are invalid",
        )
    })?;
    if pointer.schema_version != 1
        || validate_name(&pointer.transfer_operation_id, "transfer_operation_id", 128).is_err()
    {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "automation transfer index identity is invalid",
        ));
    }
    Ok(Some(pointer.transfer_operation_id))
}

pub(crate) fn parse_request(value: &Value, applying: bool) -> Result<ConfigRequest> {
    let allowed = if applying {
        &[
            "client_request_id",
            "project_id",
            "changes",
            "preview_digest",
        ][..]
    } else {
        &["project_id", "changes"][..]
    };
    crate::model::fields(value, allowed)?;
    let project_id = crate::model::text(value, "project_id")?.to_owned();
    if project_id.len() > 128 || project_id.chars().any(char::is_control) {
        return Err(Error::invalid("project_id is invalid"));
    }
    let raw_changes = value
        .get("changes")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::invalid("changes must be an array"))?;
    if raw_changes.is_empty() || raw_changes.len() > MAX_CHANGES_PER_APPLY {
        return Err(Error::invalid(format!(
            "changes must contain 1..={MAX_CHANGES_PER_APPLY} entries"
        )));
    }
    let mut seen = BTreeSet::new();
    let mut changes = Vec::with_capacity(raw_changes.len());
    for raw in raw_changes {
        crate::model::fields(
            raw,
            &[
                "automation_id",
                "expected_revision",
                "include_existing",
                "patch",
            ],
        )?;
        let automation_id = crate::model::text(raw, "automation_id")?.to_owned();
        validate_automation_id(&automation_id)?;
        if !seen.insert(automation_id.clone()) {
            return Err(Error::invalid("automation IDs in changes must be unique"));
        }
        let expected_revision = raw
            .get("expected_revision")
            .and_then(Value::as_i64)
            .ok_or_else(|| Error::invalid("expected_revision must be an integer"))?;
        if expected_revision < 0 {
            return Err(Error::invalid("expected_revision cannot be negative"));
        }
        let include_existing = raw
            .get("include_existing")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if raw.get("include_existing").is_some() && raw["include_existing"].as_bool().is_none() {
            return Err(Error::invalid("include_existing must be a boolean"));
        }
        let patch = raw
            .get("patch")
            .filter(|patch| patch.is_object())
            .cloned()
            .ok_or_else(|| Error::invalid("patch must be an object"))?;
        changes.push(ConfigChange {
            automation_id,
            expected_revision,
            include_existing,
            patch,
        });
    }
    let preview_digest = match value.get("preview_digest") {
        Some(Value::String(digest)) if applying => {
            if digest.len() != 64 || !digest.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(Error::invalid(
                    "preview_digest must be a SHA-256 hex digest",
                ));
            }
            Some(digest.to_ascii_lowercase())
        }
        Some(_) => return Err(Error::invalid("preview_digest must be text")),
        None => None,
    };
    Ok(ConfigRequest {
        project_id,
        preview_digest,
        changes,
    })
}

pub(crate) fn changes_digest(owner: &str, request: &ConfigRequest) -> Result<String> {
    let value = json!({
        "schema_version":1,
        "owner_manager_id":owner,
        "project_id":request.project_id,
        "changes":request.changes.iter().map(|change| json!({
            "automation_id":change.automation_id,
            "expected_revision":change.expected_revision,
            "include_existing":change.include_existing,
            "patch":change.patch
        })).collect::<Vec<_>>()
    });
    Ok(crate::model::digest(
        crate::model::canonical(&value)?.as_bytes(),
    ))
}

pub(crate) fn apply_patch(
    current: &AutomationEntry,
    patch: &Value,
    now_ms: i64,
) -> Result<AutomationEntry> {
    let object = patch
        .as_object()
        .ok_or_else(|| Error::invalid("patch must be an object"))?;
    let mut next = current.clone();
    for (field, value) in object {
        match field.as_str() {
            "enabled" => {
                next.enabled = value
                    .as_bool()
                    .ok_or_else(|| Error::invalid("enabled must be a boolean"))?;
            }
            "preset" => {
                next.preset = optional_text(value, "preset", 64)?;
                if next
                    .preset
                    .as_deref()
                    .is_some_and(|preset| preset != "reviewed_delivery")
                {
                    return Err(Error::new(
                        "AUTOMATION_CAPABILITY_GAP",
                        "only the reviewed_delivery preset is registered",
                    ));
                }
            }
            "scope" => patch_scope(&mut next.scope, value)?,
            "steps" => next.steps = parse_steps(value)?,
            "goal_progression" => patch_goal_progression(&mut next.goal_progression, value)?,
            "script_run" => patch_script_run(&mut next.script_run, value)?,
            "work_dispatch" => patch_work_dispatch(&mut next.work_dispatch, value)?,
            "publication" => patch_publication(&mut next.publication, value)?,
            "github_projection" => patch_github_projection(&mut next.github_projection, value)?,
            "cron" => patch_cron(&mut next.cron, value)?,
            "hook_commit" => patch_hook_commit(&mut next.hook_commit, value)?,
            "event_rules" => next.event_rules = event_rules::parse_settings(value)?,
            "review" => patch_review(&mut next.review, value)?,
            _ => return Err(Error::invalid(format!("unknown automation field: {field}"))),
        }
    }
    if next != *current {
        next.revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::new("REVISION_OVERFLOW", "automation revision exhausted"))?;
        next.updated_at_ms = now_ms;
    }
    Ok(next)
}

fn patch_script_run(settings: &mut Option<ScriptRunSettings>, patch: &Value) -> Result<()> {
    if patch.is_null() {
        *settings = None;
        return Ok(());
    }
    let parsed: ScriptRunSettings = serde_json::from_value(patch.clone())
        .map_err(|_| Error::invalid("script_run requires exactly one script_id"))?;
    parsed.validate()?;
    *settings = Some(parsed);
    Ok(())
}

fn patch_hook_commit(settings: &mut Option<HookCommitSettings>, patch: &Value) -> Result<()> {
    if patch.is_null() {
        *settings = None;
        return Ok(());
    }
    let parsed: HookCommitSettings = serde_json::from_value(patch.clone())
        .map_err(|_| Error::invalid("hook_commit requires exactly one source_id"))?;
    parsed.validate()?;
    *settings = Some(parsed);
    Ok(())
}

fn patch_publication(settings: &mut Option<PublicationSettings>, patch: &Value) -> Result<()> {
    if patch.is_null() {
        *settings = None;
        return Ok(());
    }
    if !patch.is_object() {
        return Err(Error::invalid(
            "publication patch must be an object or null",
        ));
    }
    let mut merged = match settings {
        Some(settings) => serde_json::to_value(settings)?,
        None => json!({}),
    };
    merge_object_patch(&mut merged, patch)?;
    let parsed: PublicationSettings = serde_json::from_value(merged)
        .map_err(|_| Error::invalid("invalid publication settings"))?;
    parsed.validate()?;
    *settings = Some(parsed);
    Ok(())
}

fn patch_github_projection(
    settings: &mut Option<GithubProjectionSettings>,
    patch: &Value,
) -> Result<()> {
    if patch.is_null() {
        *settings = None;
        return Ok(());
    }
    if !patch.is_object() {
        return Err(Error::invalid(
            "github_projection patch must be an object or null",
        ));
    }
    let mut merged = match settings {
        Some(settings) => serde_json::to_value(settings)?,
        None => json!({}),
    };
    merge_object_patch(&mut merged, patch)?;
    let parsed: GithubProjectionSettings = serde_json::from_value(merged)
        .map_err(|_| Error::invalid("github_projection requires source_id, label and present"))?;
    parsed.validate()?;
    *settings = Some(parsed);
    Ok(())
}

fn patch_cron(settings: &mut Option<crate::scheduler::CronSettings>, patch: &Value) -> Result<()> {
    if patch.is_null() {
        *settings = None;
        return Ok(());
    }
    if !patch.is_object() {
        return Err(Error::invalid("cron patch must be an object or null"));
    }
    let mut merged = match settings {
        Some(settings) => serde_json::to_value(settings)?,
        None => json!({}),
    };
    merge_object_patch(&mut merged, patch)?;
    let parsed: crate::scheduler::CronSettings =
        serde_json::from_value(merged).map_err(|_| Error::invalid("invalid cron settings"))?;
    crate::scheduler::validate_cron_settings(&parsed)?;
    *settings = Some(parsed);
    Ok(())
}

fn patch_goal_progression(
    settings: &mut Option<GoalProgressionSettings>,
    patch: &Value,
) -> Result<()> {
    if patch.is_null() {
        *settings = None;
        return Ok(());
    }
    let parsed: GoalProgressionSettings = serde_json::from_value(patch.clone())
        .map_err(|_| Error::invalid("goal_progression requires exactly one goal_id"))?;
    parsed.validate()?;
    *settings = Some(parsed);
    Ok(())
}
fn patch_work_dispatch(
    settings: &mut Option<WorkDispatchLaunchSettings>,
    patch: &Value,
) -> Result<()> {
    if patch.is_null() {
        *settings = None;
        return Ok(());
    }
    if !patch.is_object() {
        return Err(Error::invalid(
            "work_dispatch patch must be an object or null",
        ));
    }
    let mut merged = match settings {
        Some(settings) => serde_json::to_value(settings)?,
        None => json!({}),
    };
    merge_object_patch(&mut merged, patch)?;
    *settings = Some(WorkDispatchLaunchSettings::parse(&merged)?);
    Ok(())
}

/// Merge nested objects and replace arrays/scalars with the patch value.
/// `null` is an explicit value, which is required for nullable launcher
/// model/effort selectors; removing the whole settings object uses top-level
/// `work_dispatch: null`.
fn merge_object_patch(target: &mut Value, patch: &Value) -> Result<()> {
    let patch_object = patch
        .as_object()
        .ok_or_else(|| Error::invalid("nested WorkDispatch patch must be an object"))?;
    let target_object = target
        .as_object_mut()
        .ok_or_else(|| Error::invalid("stored WorkDispatch settings are not an object"))?;
    for (key, value) in patch_object {
        if value.is_object() {
            match target_object.get_mut(key) {
                Some(existing) if existing.is_object() => merge_object_patch(existing, value)?,
                _ => {
                    let mut nested = json!({});
                    merge_object_patch(&mut nested, value)?;
                    target_object.insert(key.clone(), nested);
                }
            }
        } else {
            target_object.insert(key.clone(), value.clone());
        }
    }
    Ok(())
}

fn patch_scope(scope: &mut AutomationScope, value: &Value) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid("scope patch must be an object"))?;
    for (field, value) in object {
        match field.as_str() {
            "work_pool_id" => scope.work_pool_id = optional_text(value, "scope.work_pool_id", 128)?,
            _ => return Err(Error::invalid(format!("unknown scope field: {field}"))),
        }
    }
    Ok(())
}

fn patch_review(review: &mut ReviewSettings, value: &Value) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid("review patch must be an object"))?;
    for (field, value) in object {
        match field.as_str() {
            "profile" => {
                review.profile = optional_text(value, "review.profile", 64)?;
                if let Some(profile) = &review.profile {
                    validate_name(profile, "review.profile", 64)?;
                }
            }
            "required_reviewers" => {
                let count = value.as_u64().ok_or_else(|| {
                    Error::invalid("required_reviewers must be a positive integer")
                })?;
                if count == 0 || count > u32::MAX as u64 {
                    return Err(Error::invalid("required_reviewers is out of range"));
                }
                review.required_reviewers = count as u32;
            }
            _ => return Err(Error::invalid(format!("unknown review field: {field}"))),
        }
    }
    Ok(())
}

fn parse_steps(value: &Value) -> Result<Vec<AutomationStep>> {
    let items = value
        .as_array()
        .ok_or_else(|| Error::invalid("steps must be an array"))?;
    if items.len() > 16 {
        return Err(Error::invalid("steps exceeds the supported entry bound"));
    }
    let mut seen = BTreeSet::new();
    let mut steps = Vec::with_capacity(items.len());
    for item in items {
        let name = item
            .as_str()
            .ok_or_else(|| Error::invalid("each automation step must be text"))?;
        let step = AutomationStep::parse(name)?;
        if !seen.insert(step) {
            return Err(Error::invalid("steps must not contain duplicates"));
        }
        steps.push(step);
    }
    Ok(steps)
}

fn optional_text(value: &Value, name: &str, max_bytes: usize) -> Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    let text = value
        .as_str()
        .ok_or_else(|| Error::invalid(format!("{name} must be text or null")))?;
    validate_name(text, name, max_bytes)?;
    Ok(Some(text.to_owned()))
}

pub(crate) fn validate_automation_id(value: &str) -> Result<()> {
    validate_name(value, "automation_id", MAX_AUTOMATION_ID_BYTES)?;
    if value
        .bytes()
        .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')))
    {
        return Err(Error::invalid(
            "automation_id may contain only ASCII letters, digits, hyphen, underscore, and dot",
        ));
    }
    Ok(())
}

fn validate_name(value: &str, name: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(Error::invalid(format!("{name} is invalid")));
    }
    Ok(())
}

pub(crate) fn validate_entry(entry: &AutomationEntry) -> Result<()> {
    if entry.schema_version != ENTRY_SCHEMA_VERSION || entry.revision <= 0 {
        return Err(Error::new(
            "AUTOMATION_RECORD_INVALID",
            "stored automation identity or revision is invalid",
        ));
    }
    validate_automation_id(&entry.automation_id)
        .and_then(|()| validate_name(&entry.owner_manager_id, "owner_manager_id", 128))
        .and_then(|()| validate_name(&entry.project_id, "project_id", 128))
        .map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored automation identity is invalid",
            )
        })?;
    let unique_steps = entry.steps.iter().copied().collect::<BTreeSet<_>>();
    if entry.steps.len() > 16 || unique_steps.len() != entry.steps.len() {
        return Err(Error::new(
            "AUTOMATION_RECORD_INVALID",
            "stored automation steps are duplicated or exceed their bound",
        ));
    }
    if entry.review.required_reviewers == 0 {
        return Err(Error::new(
            "AUTOMATION_RECORD_INVALID",
            "stored reviewer count must be positive",
        ));
    }
    if let Some(profile) = entry.review.profile.as_deref() {
        validate_name(profile, "review.profile", 64).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored review profile is invalid",
            )
        })?;
    }
    if let Some(work_pool) = entry.scope.work_pool_id.as_deref() {
        validate_name(work_pool, "scope.work_pool_id", 128).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored work-pool scope is invalid",
            )
        })?;
    }
    if let Some(settings) = entry.goal_progression.as_ref() {
        settings.validate().map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored Goal progression settings do not identify one bounded Goal ID",
            )
        })?;
    }
    if let Some(settings) = entry.script_run.as_ref() {
        settings.validate().map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored script_run settings do not identify one valid script",
            )
        })?;
        if !entry.steps.contains(&AutomationStep::ScriptRun) {
            return Err(Error::new(
                "AUTOMATION_RECORD_INVALID",
                "script_run settings require the script_run action",
            ));
        }
    }
    if let Some(settings) = entry.work_dispatch.as_ref() {
        let value = serde_json::to_value(settings).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored work-dispatch settings cannot be serialized",
            )
        })?;
        WorkDispatchLaunchSettings::parse(&value).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored work-dispatch settings do not match the launcher request contract",
            )
        })?;
    }
    if let Some(settings) = entry.publication.as_ref() {
        settings.validate().map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored publication settings do not match the exact Forge target contract",
            )
        })?;
    }
    if let Some(settings) = entry.github_projection.as_ref() {
        settings.validate().map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored GitHub projection settings do not select one registered-source Eliot label",
            )
        })?;
    }
    if let Some(settings) = entry.cron.as_ref() {
        crate::scheduler::validate_cron_settings(settings).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored cron settings do not match the validated calendar and CheckRun contract",
            )
        })?;
    }
    if let Some(settings) = entry.hook_commit.as_ref() {
        if !entry.steps.contains(&AutomationStep::ReviewDispatch) {
            return Err(Error::new(
                "AUTOMATION_RECORD_INVALID",
                "hook_commit requires the existing review_dispatch action",
            ));
        }
        settings.validate().map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored HookCommit source identity is invalid",
            )
        })?;
    }
    if let Some(rules) = entry.event_rules.as_ref() {
        event_rules::validate(rules, &entry.steps).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_INVALID",
                "stored event rules must use a registered source/action and selected action step",
            )
        })?;
    }
    if let Some(preset) = entry.preset.as_deref()
        && preset != "reviewed_delivery"
    {
        return Err(Error::new(
            "AUTOMATION_RECORD_INVALID",
            "stored automation preset is not registered",
        ));
    }
    Ok(())
}

pub(crate) fn scope_digest(owner: &str, project: &str) -> Result<String> {
    Ok(crate::model::digest(
        crate::model::canonical(&json!([owner, project]))?.as_bytes(),
    ))
}

pub(crate) fn entry_prefix(owner: &str, project: &str) -> Result<String> {
    Ok(format!(
        "automation:v1:entry:{}:",
        scope_digest(owner, project)?
    ))
}

pub(crate) fn entry_key(owner: &str, project: &str, automation_id: &str) -> Result<String> {
    validate_automation_id(automation_id)?;
    Ok(format!("{}{automation_id}", entry_prefix(owner, project)?))
}

pub(crate) fn dispatch_state_key(
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<String> {
    validate_automation_id(automation_id)?;
    Ok(format!(
        "automation:v1:dispatch:{}:{automation_id}",
        scope_digest(owner, project)?
    ))
}

/// Independent bounded applied-submission cursor for the ScriptRun action.
/// It shares O1's registered intake but cannot rewind ReviewDispatch state.
pub(crate) fn script_dispatch_state_key(
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<String> {
    validate_automation_id(automation_id)?;
    Ok(format!(
        "automation:v1:script_dispatch:{}:{automation_id}",
        scope_digest(owner, project)?
    ))
}

pub(crate) fn operation_link_key(operation_id: &str) -> Result<String> {
    validate_name(operation_id, "operation_id", 128)?;
    Ok(format!("automation:v1:operation:{operation_id}"))
}

pub(crate) fn entry_operation_prefix(
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<String> {
    validate_automation_id(automation_id)?;
    Ok(format!(
        "automation:v1:operation_by_entry:{}:{automation_id}:",
        scope_digest(owner, project)?
    ))
}

pub(crate) fn entry_operation_key(
    owner: &str,
    project: &str,
    automation_id: &str,
    operation_id: &str,
) -> Result<String> {
    validate_name(operation_id, "operation_id", 128)?;
    Ok(format!(
        "{}{operation_id}",
        entry_operation_prefix(owner, project, automation_id)?
    ))
}

pub(crate) fn seal_record(value: &Value) -> Result<Value> {
    let canonical = crate::model::canonical(value)?;
    if canonical.len() > MAX_META_RECORD_BYTES {
        return Err(Error::new(
            "AUTOMATION_RECORD_TOO_LARGE",
            "automation metadata record exceeds its durable bound",
        ));
    }
    Ok(json!({
        "schema_version":1,
        "sha256":crate::model::digest(canonical.as_bytes()),
        "record":value
    }))
}

pub(crate) fn open_record(value: Value, label: &str) -> Result<Value> {
    if value["schema_version"] != 1 {
        return Err(Error::new(
            "AUTOMATION_RECORD_VERSION",
            format!("{label} metadata schema is unsupported"),
        ));
    }
    let record = value
        .get("record")
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_RECORD_CORRUPT",
                format!("{label} has no record"),
            )
        })?
        .clone();
    let digest = value["sha256"].as_str().ok_or_else(|| {
        Error::new(
            "AUTOMATION_RECORD_CORRUPT",
            format!("{label} has no digest"),
        )
    })?;
    let canonical = crate::model::canonical(&record)?;
    if crate::model::digest(canonical.as_bytes()) != digest {
        return Err(Error::new(
            "AUTOMATION_RECORD_CORRUPT",
            format!("{label} digest does not match its retained record"),
        ));
    }
    Ok(record)
}

pub(crate) fn read_record(db: &Connection, key: &str, label: &str) -> Result<Option<Value>> {
    let raw: Option<String> = db
        .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
            row.get(0)
        })
        .optional()?;
    raw.map(|raw| {
        let value: Value = serde_json::from_str(&raw).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_CORRUPT",
                format!("{label} JSON is invalid"),
            )
        })?;
        open_record(value, label)
    })
    .transpose()
}

pub(crate) fn write_record(db: &Connection, key: &str, value: &Value) -> Result<()> {
    let sealed = seal_record(value)?;
    db.execute(
        "INSERT INTO meta(key,value_json) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
        rusqlite::params![key, crate::model::canonical(&sealed)?],
    )?;
    Ok(())
}

pub(crate) fn load_entry(
    db: &Connection,
    owner: &str,
    project: &str,
    automation_id: &str,
) -> Result<Option<AutomationEntry>> {
    let key = entry_key(owner, project, automation_id)?;
    let Some(value) = read_record(db, &key, "automation entry")? else {
        return Ok(None);
    };
    let entry: AutomationEntry = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_RECORD_CORRUPT",
            "automation entry fields are invalid",
        )
    })?;
    validate_entry(&entry)?;
    if entry.owner_manager_id != owner
        || entry.project_id != project
        || entry.automation_id != automation_id
    {
        return Err(Error::new(
            "AUTOMATION_RECORD_CORRUPT",
            "automation entry identity does not match its metadata key",
        ));
    }
    Ok(Some(entry))
}
