//! Typed, manager-scoped automation definitions and patch semantics.

use super::actions::{AutomationStep, supported_action_for};
use super::work_dispatch::WorkDispatchLaunchSettings;
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
    pub(crate) publication: Option<PublicationSettings>,
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
            publication: None,
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
        if self.scope.work_pool_id.is_some() {
            gaps.push(json!({
                "code":"work_pool_scope_unavailable",
                "reason":"the current Task source has no committed work-pool membership reader"
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
        gaps
    }

    pub(crate) fn review_dispatch_ready(&self) -> bool {
        self.enabled
            && self.steps.contains(&AutomationStep::ReviewDispatch)
            && self.review.profile.is_some()
            && self.review.required_reviewers == 1
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
            "work_dispatch" => patch_work_dispatch(&mut next.work_dispatch, value)?,
            "publication" => patch_publication(&mut next.publication, value)?,
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
