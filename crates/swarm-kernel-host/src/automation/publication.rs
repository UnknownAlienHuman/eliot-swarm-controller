//! Typed manager-on-behalf authority for automatic accepted-candidate
//! publication. This value is built only from a retained applied acceptance
//! fact and a currently enabled, current-GM-owned automation entry.

use super::{authorization, config};
use crate::{
    config::Config,
    error::{Error, Result},
    forge::{self, PublishRefRequest},
    model::{self},
};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const ACTION: &str = "forge.publish_ref";
const ACCEPTANCE_KIND: &str = "task.acceptance";
type ReadbackOperationRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
);

/// Exact acceptance provenance carried by a publication reservation.
///
/// The fields stay private and this type has no request deserializer. The
/// only constructors re-read either the acceptance Observation or the
/// immutable Operation/link records.
#[derive(Debug, Clone)]
pub(crate) struct PublicationContext {
    technical_requester_id: String,
    effective_manager_id: String,
    automation_id: String,
    automation_revision: i64,
    project_id: String,
    gm_epoch: i64,
    acceptance_observation_id: i64,
    accepted_operation_id: String,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    submission_ref: String,
    candidate_ref: String,
    policy_revision: String,
    canonical_repository: String,
    target_ref: String,
    expected_old_ref: Option<String>,
    expected_create: bool,
    activation_cut: i64,
    historical_replay_authorized: bool,
    committed_operation_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PublicationCause {
    kind: String,
    observation_id: i64,
    operation_id: String,
    id: String,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    submission_ref: String,
    candidate_ref: String,
    canonical_repository: String,
    gm_epoch: i64,
    policy_revision: String,
    target_ref: String,
    expected_old_ref: Option<String>,
    expected_create: bool,
    activation_cut: i64,
    historical_replay_authorized: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PublicationAttribution {
    schema_version: u32,
    technical_requester_id: String,
    effective_manager_id: String,
    automation_id: String,
    automation_revision: i64,
    project_id: String,
    action: String,
    cause: PublicationCause,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RetainedPublicationOperationLink {
    schema_version: u32,
    operation_id: String,
    technical_requester_id: String,
    effective_manager_id: String,
    automation_id: String,
    automation_revision: i64,
    project_id: String,
    action: String,
    cause: PublicationCause,
    linked_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcceptedCandidate {
    pub(crate) accepted_operation_id: String,
    pub(crate) task_id: String,
    pub(crate) project_id: String,
    pub(crate) task_revision: i64,
    pub(crate) attempt_id: String,
    pub(crate) submission_ref: String,
    pub(crate) candidate_ref: String,
}

struct CommittedPublicationOperationRow {
    caller_id: String,
    method: String,
    original_request_json: String,
    effective_request_json: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    result_json: Option<String>,
}

struct AcceptedOperationRow {
    method: String,
    state: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    original_request_json: String,
    result_json: Option<String>,
}

struct AcceptedTaskRow {
    project_id: String,
    revision: i64,
    state: String,
    accepted_attempt_id: Option<String>,
    accepted_operation_id: Option<String>,
    accepted_revision: Option<i64>,
    accepted_candidate_ref: Option<String>,
}

struct AcceptedAttemptRow {
    task_id: String,
    task_revision: i64,
    state: String,
    released_at_ms: Option<i64>,
    submission_ref: Option<String>,
    candidate_ref: Option<String>,
    task_snapshot_json: String,
}

struct SubmissionOperationRow {
    method: String,
    state: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    result_json: Option<String>,
    effective_request_json: String,
}

impl PublicationContext {
    /// Construct an admission context by reloading and validating the exact
    /// committed acceptance event. `historical_replay_authorized` is true
    /// only while the per-entry cursor is inside an explicit include-existing
    /// activation window.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_acceptance_observation(
        db: &Connection,
        entry: &config::AutomationEntry,
        observation_id: i64,
        accepted_operation_id: &str,
        activation_cut: i64,
        historical_replay_authorized: bool,
        launcher_config: &Config,
    ) -> Result<Self> {
        validate_entry_authority(db, entry)?;
        let candidate =
            accepted_candidate_from_observation(db, observation_id, accepted_operation_id)?;
        if (observation_id <= activation_cut) != historical_replay_authorized {
            return Err(Error::new(
                "AUTOMATION_PUBLICATION_ACTIVATION_MISMATCH",
                "acceptance event is outside the retained publication activation window",
            ));
        }
        let gm_epoch = current_gm_epoch_for(db, &entry.owner_manager_id)?;
        let settings = entry.publication.as_ref().ok_or_else(|| {
            Error::new(
                "AUTOMATION_PUBLICATION_SETTINGS_REQUIRED",
                "selected publication has no explicit target/effect settings",
            )
        })?;
        if !launcher_config.forge.enabled {
            return Err(Error::new(
                "FORGE_DISABLED",
                "automatic publication is waiting for the scoped Forge service to be enabled",
            ));
        }
        let project = launcher_config.forge.project(&entry.project_id)?;
        if !project
            .target_refs
            .iter()
            .any(|target| target == &settings.target_ref)
        {
            return Err(Error::new(
                "FORGE_POLICY_MISMATCH",
                "publication target is not present in the trusted project mapping",
            ));
        }
        let context = Self {
            technical_requester_id: authorization::AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
            effective_manager_id: entry.owner_manager_id.clone(),
            automation_id: entry.automation_id.clone(),
            automation_revision: entry.revision,
            project_id: entry.project_id.clone(),
            gm_epoch,
            acceptance_observation_id: observation_id,
            accepted_operation_id: candidate.accepted_operation_id,
            task_id: candidate.task_id,
            task_revision: candidate.task_revision,
            attempt_id: candidate.attempt_id,
            submission_ref: candidate.submission_ref,
            candidate_ref: candidate.candidate_ref,
            policy_revision: project.policy_revision.clone(),
            canonical_repository: forge::canonical_repository(&project.canonical_repository)?,
            target_ref: settings.target_ref.clone(),
            expected_old_ref: settings.expected_old_ref.clone(),
            expected_create: settings.expected_create,
            activation_cut,
            historical_replay_authorized,
            committed_operation_id: None,
        };
        if candidate.project_id != context.project_id {
            return Err(Error::new(
                "FORGE_ACCEPTANCE_STALE",
                "accepted Task no longer belongs to the automation project",
            ));
        }
        context.require_current(db)?;
        Ok(context)
    }

    /// Rehydrate only the exact committed Forge Operation and its immutable
    /// on-behalf metadata. This deliberately does not require current GM or
    /// Task authority: an already admitted unknown effect must remain
    /// readback-only after revocation or acceptance invalidation.
    pub(crate) fn from_committed_operation(db: &Connection, operation_id: &str) -> Result<Self> {
        let row: Option<CommittedPublicationOperationRow> = db
            .query_row(
                "SELECT caller_id,method,original_request_json,effective_request_json,task_id,attempt_id,result_json \
                 FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| {
                    Ok(CommittedPublicationOperationRow {
                        caller_id: row.get(0)?,
                        method: row.get(1)?,
                        original_request_json: row.get(2)?,
                        effective_request_json: row.get(3)?,
                        task_id: row.get(4)?,
                        attempt_id: row.get(5)?,
                        result_json: row.get(6)?,
                    })
                },
            )
            .optional()?;
        let Some(CommittedPublicationOperationRow {
            caller_id,
            method,
            original_request_json: original_json,
            effective_request_json: effective_json,
            task_id,
            attempt_id,
            result_json,
        }) = row
        else {
            return Err(Error::new(
                "NOT_FOUND",
                "publication Operation was not found",
            ));
        };
        if method != ACTION || caller_id != authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication Operation is not the retained internal Forge action",
            ));
        }
        let effective: Value = serde_json::from_str(&effective_json).map_err(|_| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication Operation effective request is invalid",
            )
        })?;
        model::fields(
            &effective,
            &["publication_intent", "automation_on_behalf", "receipt"],
        )
        .map_err(|_| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication Operation effective request has unexpected fields",
            )
        })?;
        let attribution: PublicationAttribution =
            serde_json::from_value(effective["automation_on_behalf"].clone()).map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "publication Operation has no valid retained automation attribution",
                )
            })?;
        let cause = attribution.cause.clone();
        let mut context = Self::from_attribution(attribution)?;
        context.committed_operation_id = Some(operation_id.to_owned());
        let link_key = config::operation_link_key(operation_id)?;
        let link = config::read_record(db, &link_key, "publication on-behalf Operation link")?
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "publication Operation has no indexed on-behalf attribution",
                )
            })?;
        // linked_at_ms is immutable but chosen at reservation, so validate
        // every field other than that timestamp against the effective link.
        let parsed_link: RetainedPublicationOperationLink =
            serde_json::from_value(link).map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "publication Operation link fields are invalid",
                )
            })?;
        let expected: RetainedPublicationOperationLink = serde_json::from_value(
            context.operation_link_value(operation_id, parsed_link.linked_at_ms),
        )?;
        if parsed_link != expected || parsed_link.cause != cause {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication Operation link differs from its retained effective request",
            ));
        }
        let entry_key = config::entry_operation_key(
            context.effective_manager_id(),
            context.project_id(),
            context.automation_id(),
            operation_id,
        )?;
        if config::read_record(db, &entry_key, "publication entry Operation link")?
            != Some(context.operation_link_value(operation_id, parsed_link.linked_at_ms))
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication entry index differs from its retained Operation link",
            ));
        }
        if effective["automation_on_behalf"] != context.linkage_value() {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication effective request differs from its retained attribution",
            ));
        }
        let request_value: Value = serde_json::from_str(&original_json)?;
        let request = PublishRefRequest::parse(&request_value)?;
        context.require_request_matches(&request)?;
        if task_id.as_deref() != Some(context.task_id.as_str())
            || attempt_id.as_deref() != Some(context.attempt_id.as_str())
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication Operation task/Attempt columns differ from its accepted candidate",
            ));
        }
        let result: Value = result_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()?
            .unwrap_or(Value::Null);
        let receipt = effective["receipt"].clone();
        model::fields(&receipt, &["ok", "value"]).map_err(|_| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication Operation retained receipt is invalid",
            )
        })?;
        if receipt["ok"] != true
            || receipt["value"]["operation_id"] != operation_id
            || (!result.is_null() && result["operation_id"] != operation_id)
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication Operation receipt differs from its exact result",
            ));
        }
        let intent: crate::forge::PublicationIntent =
            serde_json::from_value(effective["publication_intent"].clone()).map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "publication Operation has no valid immutable Forge intent",
                )
            })?;
        intent.validate()?;
        if intent.operation_id != operation_id
            || intent.project_id != context.project_id
            || intent.canonical_repository != context.canonical_repository
            || intent.attempt_id != context.attempt_id
            || intent.task_revision != context.task_revision
            || intent.admitted_gm_epoch != context.gm_epoch
            || intent.submission_ref != context.submission_ref
            || intent.accepted_operation_id != context.accepted_operation_id
            || intent.candidate_ref != context.candidate_ref
            || intent.policy_revision != context.policy_revision
            || intent.target_ref != context.target_ref
            || intent.expected_old_ref != context.expected_old_ref
            || intent.expected_create != context.expected_create
            || intent.force
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication intent differs from its retained exact acceptance slot",
            ));
        }
        let accepted_candidate = accepted_candidate_from_observation(
            db,
            context.acceptance_observation_id,
            &context.accepted_operation_id,
        )?;
        if accepted_candidate.task_id != context.task_id
            || accepted_candidate.project_id != context.project_id
            || accepted_candidate.task_revision != context.task_revision
            || accepted_candidate.attempt_id != context.attempt_id
            || accepted_candidate.submission_ref != context.submission_ref
            || accepted_candidate.candidate_ref != context.candidate_ref
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication link differs from its exact applied acceptance source",
            ));
        }
        let source: Option<(Option<String>, String)> = db
            .query_row(
                "SELECT content_digest,metadata_json FROM artifacts WHERE artifact_id=?1 AND kind='source_snapshot'",
                [&context.candidate_ref],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((candidate_digest, candidate_metadata_raw)) = source else {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication source candidate artifact is missing",
            ));
        };
        let candidate_metadata: Value =
            serde_json::from_str(&candidate_metadata_raw).map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "publication source candidate metadata is invalid",
                )
            })?;
        if candidate_digest.as_deref() != Some(intent.candidate_sha256.as_str())
            || candidate_metadata["task_id"] != context.task_id
            || candidate_metadata["attempt_id"] != context.attempt_id
            || candidate_metadata["task_revision"] != context.task_revision
            || candidate_metadata["commit"] != intent.commit
            || candidate_metadata["tree"] != intent.tree
            || candidate_metadata["coverage"] != "complete"
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication intent candidate digest or source metadata differs from its artifact",
            ));
        }
        Ok(context)
    }

    fn from_attribution(attribution: PublicationAttribution) -> Result<Self> {
        let cause = attribution.cause;
        if attribution.schema_version != 1
            || attribution.action != ACTION
            || attribution.technical_requester_id
                != authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
            || attribution.automation_revision <= 0
            || cause.kind != ACCEPTANCE_KIND
            || cause.observation_id <= 0
            || cause.operation_id.is_empty()
            || cause.id != cause.operation_id
            || cause.task_id.is_empty()
            || cause.task_revision <= 0
            || cause.attempt_id.is_empty()
            || cause.submission_ref.is_empty()
            || cause.candidate_ref.is_empty()
            || cause.canonical_repository.is_empty()
            || cause.gm_epoch <= 0
            || cause.policy_revision.is_empty()
            || cause.target_ref.is_empty()
            || attribution.effective_manager_id.is_empty()
            || attribution.project_id.is_empty()
            || attribution.automation_id.is_empty()
            || cause.activation_cut < 0
            || (cause.observation_id <= cause.activation_cut) != cause.historical_replay_authorized
            || cause.expected_create == cause.expected_old_ref.is_some()
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication attribution identity is invalid",
            ));
        }
        let canonical_repository = forge::canonical_repository(&cause.canonical_repository)
            .map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "publication canonical repository is invalid",
                )
            })?;
        if canonical_repository != cause.canonical_repository {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication canonical repository is not normalized",
            ));
        }
        Ok(Self {
            technical_requester_id: attribution.technical_requester_id,
            effective_manager_id: attribution.effective_manager_id,
            automation_id: attribution.automation_id,
            automation_revision: attribution.automation_revision,
            project_id: attribution.project_id,
            gm_epoch: cause.gm_epoch,
            acceptance_observation_id: cause.observation_id,
            accepted_operation_id: cause.operation_id,
            task_id: cause.task_id,
            task_revision: cause.task_revision,
            attempt_id: cause.attempt_id,
            submission_ref: cause.submission_ref,
            candidate_ref: cause.candidate_ref,
            policy_revision: cause.policy_revision,
            canonical_repository,
            target_ref: cause.target_ref,
            expected_old_ref: cause.expected_old_ref,
            expected_create: cause.expected_create,
            activation_cut: cause.activation_cut,
            historical_replay_authorized: cause.historical_replay_authorized,
            committed_operation_id: None,
        })
    }

    /// Check all current write prerequisites. Forge calls this only before a
    /// new push; it must not gate readback of an already uncertain effect.
    pub(crate) fn require_current(&self, db: &Connection) -> Result<()> {
        if let Some(continuation) = self.transfer_continuation(db)? {
            let candidate = accepted_candidate_from_observation(
                db,
                self.acceptance_observation_id,
                &self.accepted_operation_id,
            )?;
            if candidate.task_id != self.task_id
                || candidate.project_id != self.project_id
                || candidate.task_revision != self.task_revision
                || candidate.attempt_id != self.attempt_id
                || candidate.submission_ref != self.submission_ref
                || candidate.candidate_ref != self.candidate_ref
            {
                return Err(Error::new(
                    "FORGE_ACCEPTANCE_STALE",
                    "retained acceptance no longer identifies the exact publication candidate",
                ));
            }
            validate_current_accepted_candidate(
                db,
                &candidate,
                &self.project_id,
                &self.policy_revision,
            )?;
            if continuation.historical_owner_id() != self.effective_manager_id
                || continuation.historical_revision() != self.automation_revision
                || continuation.project_id() != self.project_id
                || continuation.automation_id() != self.automation_id
                || continuation.action() != ACTION
                || continuation.task_id() != self.task_id
                || continuation.current_entry().owner_manager_id != continuation.current_owner_id()
                || !entry_matches_publication_settings(continuation.current_entry(), self)
            {
                return Err(Error::new(
                    "AUTOMATION_TRANSFER_SCOPE",
                    "current GM transfer does not preserve the exact retained publication action",
                ));
            }
            return Ok(());
        }
        authorization::require_registered_manager(db, &self.effective_manager_id)?;
        let entry = config::load_entry(
            db,
            &self.effective_manager_id,
            &self.project_id,
            &self.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "publication automation entry was removed"))?;
        if !entry.publication_ready()
            || entry.revision != self.automation_revision
            || entry.publication.as_ref().is_none_or(|settings| {
                settings.target_ref != self.target_ref
                    || settings.expected_old_ref != self.expected_old_ref
                    || settings.expected_create != self.expected_create
            })
        {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "current entry no longer admits this exact publication target",
            ));
        }
        let gm_epoch = current_gm_epoch_for(db, &self.effective_manager_id)?;
        if gm_epoch != self.gm_epoch {
            return Err(Error::new(
                "AUTOMATION_CURRENT_GM_REQUIRED",
                "current GM or epoch changed after publication admission",
            ));
        }
        let candidate = accepted_candidate_from_observation(
            db,
            self.acceptance_observation_id,
            &self.accepted_operation_id,
        )?;
        if candidate.task_id != self.task_id
            || candidate.project_id != self.project_id
            || candidate.task_revision != self.task_revision
            || candidate.attempt_id != self.attempt_id
            || candidate.submission_ref != self.submission_ref
            || candidate.candidate_ref != self.candidate_ref
        {
            return Err(Error::new(
                "FORGE_ACCEPTANCE_STALE",
                "retained acceptance no longer identifies the exact publication candidate",
            ));
        }
        if !authorization::current_manager_id_has_task_scope(
            db,
            &self.effective_manager_id,
            &self.task_id,
            &self.project_id,
        )? {
            return Err(Error::new(
                "FORBIDDEN",
                "current manager no longer has Task/project scope for publication",
            ));
        }
        validate_current_accepted_candidate(
            db,
            &candidate,
            &self.project_id,
            &self.policy_revision,
        )?;
        Ok(())
    }

    /// A transfer grant can only be rehydrated while this exact retained
    /// publication Operation remains queued and unsent. Forge carries the
    /// returned typed grant across its atomic begin transition and rechecks
    /// that same grant before crossing the write boundary.
    pub(crate) fn transfer_continuation(
        &self,
        db: &Connection,
    ) -> Result<Option<authorization::TransferContinuation>> {
        let Some(operation_id) = self.committed_operation_id.as_deref() else {
            return Ok(None);
        };
        let continuation = authorization::current_transfer_continuation(
            db,
            operation_id,
            ACTION,
            super::actions::AutomationStep::Publication,
            &self.task_id,
        )?;
        let Some(continuation) = continuation else {
            return Ok(None);
        };
        if continuation.historical_owner_id() != self.effective_manager_id
            || continuation.historical_revision() != self.automation_revision
            || continuation.project_id() != self.project_id
            || continuation.automation_id() != self.automation_id
            || continuation.action() != ACTION
            || continuation.task_id() != self.task_id
            || continuation.current_entry().owner_manager_id != continuation.current_owner_id()
            || !entry_matches_publication_settings(continuation.current_entry(), self)
        {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_SCOPE",
                "current GM transfer does not preserve the exact retained publication action",
            ));
        }
        Ok(Some(continuation))
    }

    /// Verify a grant captured before `begin` moved the Operation from queued
    /// to sending. This does not create a grant from a sending or uncertain
    /// Operation; the store boundary validates the typed pre-begin grant.
    pub(crate) fn require_prepared_transfer_continuation(
        &self,
        db: &Connection,
        continuation: &authorization::TransferContinuation,
    ) -> Result<()> {
        let operation_id = self.committed_operation_id.as_deref().ok_or_else(|| {
            Error::new(
                "AUTOMATION_TRANSFER_SCOPE",
                "publication transfer grant has no retained Operation identity",
            )
        })?;
        let current = continuation.revalidate_publication_prewrite(db, operation_id)?;
        if current.historical_owner_id() != self.effective_manager_id
            || current.historical_revision() != self.automation_revision
            || current.project_id() != self.project_id
            || current.automation_id() != self.automation_id
            || current.action() != ACTION
            || current.task_id() != self.task_id
            || current.current_entry().owner_manager_id != current.current_owner_id()
            || !entry_matches_publication_settings(current.current_entry(), self)
        {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_SCOPE",
                "prepared current GM transfer no longer preserves the exact publication action",
            ));
        }
        let candidate = accepted_candidate_from_observation(
            db,
            self.acceptance_observation_id,
            &self.accepted_operation_id,
        )?;
        if candidate.task_id != self.task_id
            || candidate.project_id != self.project_id
            || candidate.task_revision != self.task_revision
            || candidate.attempt_id != self.attempt_id
            || candidate.submission_ref != self.submission_ref
            || candidate.candidate_ref != self.candidate_ref
        {
            return Err(Error::new(
                "FORGE_ACCEPTANCE_STALE",
                "retained acceptance no longer identifies the exact publication candidate",
            ));
        }
        validate_current_accepted_candidate(
            db,
            &candidate,
            &self.project_id,
            &self.policy_revision,
        )?;
        Ok(())
    }

    /// Readback after a possible Forge write uses only the immutable retained
    /// Operation/link/request/pinned-origin facts and current scoped Forge
    /// mapping. It deliberately has no live Manager, GM, Attempt or current
    /// acceptance dependency and cannot authorize a new publication.
    pub(crate) fn require_readback_authority(
        &self,
        db: &Connection,
        launcher_config: &Config,
    ) -> Result<()> {
        let operation_id = self.committed_operation_id.as_deref().ok_or_else(|| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication readback has no retained Operation identity",
            )
        })?;
        let operation: Option<ReadbackOperationRow> = db
            .query_row(
                "SELECT caller_id,method,state,task_id,attempt_id,original_request_json \
                 FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .optional()?;
        let Some((caller_id, method, state, task_id, attempt_id, original_request_json)) =
            operation
        else {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication readback Operation is missing",
            ));
        };
        if caller_id != authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
            || method != ACTION
            || !matches!(state.as_str(), "sending" | "outcome_unknown")
            || task_id.as_deref() != Some(self.task_id.as_str())
            || attempt_id.as_deref() != Some(self.attempt_id.as_str())
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication readback Operation no longer retains its exact state or origin",
            ));
        }
        let request_value: Value = serde_json::from_str(&original_request_json)?;
        let request = PublishRefRequest::parse(&request_value)?;
        self.require_request_matches(&request)?;
        let project = launcher_config.forge.project(&self.project_id)?;
        if forge::canonical_repository(&project.canonical_repository)? != self.canonical_repository
            || project.policy_revision != self.policy_revision
            || !launcher_config.forge.enabled
            || !project
                .target_refs
                .iter()
                .any(|target| target == &self.target_ref)
        {
            return Err(Error::new(
                "FORGE_CONFIG_CHANGED",
                "current scoped Forge mapping no longer admits exact retained ref readback",
            ));
        }
        Ok(())
    }

    pub(crate) fn linkage_value(&self) -> Value {
        json!({
            "schema_version":1,
            "technical_requester_id":self.technical_requester_id,
            "effective_manager_id":self.effective_manager_id,
            "automation_id":self.automation_id,
            "automation_revision":self.automation_revision,
            "project_id":self.project_id,
            "action":ACTION,
            "cause":self.cause()
        })
    }

    pub(crate) fn operation_link_value(&self, operation_id: &str, linked_at_ms: i64) -> Value {
        json!({
            "schema_version":1,
            "operation_id":operation_id,
            "technical_requester_id":self.technical_requester_id,
            "effective_manager_id":self.effective_manager_id,
            "automation_id":self.automation_id,
            "automation_revision":self.automation_revision,
            "project_id":self.project_id,
            "action":ACTION,
            "cause":self.cause(),
            "linked_at_ms":linked_at_ms
        })
    }

    pub(crate) fn request_value(&self) -> Result<Value> {
        let request = PublishRefRequest {
            client_request_id: self.client_request_id()?,
            attempt_id: self.attempt_id.clone(),
            expected_revision: self.task_revision,
            submission_ref: self.submission_ref.clone(),
            accepted_operation_id: self.accepted_operation_id.clone(),
            candidate_ref: self.candidate_ref.clone(),
            expected_policy_revision: self.policy_revision.clone(),
            target_ref: self.target_ref.clone(),
            expected_old_ref: self.expected_old_ref.clone(),
            expected_create: self.expected_create,
        };
        let value = serde_json::to_value(request)?;
        PublishRefRequest::parse(&value)?;
        Ok(value)
    }

    /// The manual/automatic Forge slot excludes random Operation, trigger and
    /// automation-revision identity. Forge adds its canonical repository
    /// mapping when it claims this value.
    pub(crate) fn semantic_slot_value(&self) -> Value {
        json!({
            "project_id":self.project_id,
            "canonical_repository":self.canonical_repository,
            "candidate_ref":self.candidate_ref,
            "target_ref":self.target_ref,
            "expected_old_ref":self.expected_old_ref,
            "expected_create":self.expected_create
        })
    }

    fn client_request_id(&self) -> Result<String> {
        let identity = json!({
            "slot":self.semantic_slot_value(),
            "accepted_operation_id":self.accepted_operation_id,
            "effective_manager_id":self.effective_manager_id,
            "action":ACTION
        });
        Ok(format!(
            "auto-publish-{}",
            model::digest(model::canonical(&identity)?.as_bytes())
        ))
    }

    fn require_request_matches(&self, request: &PublishRefRequest) -> Result<()> {
        if request.client_request_id != self.client_request_id()?
            || request.attempt_id != self.attempt_id
            || request.expected_revision != self.task_revision
            || request.submission_ref != self.submission_ref
            || request.accepted_operation_id != self.accepted_operation_id
            || request.candidate_ref != self.candidate_ref
            || request.expected_policy_revision != self.policy_revision
            || request.target_ref != self.target_ref
            || request.expected_old_ref != self.expected_old_ref
            || request.expected_create != self.expected_create
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "publication request differs from its retained acceptance and settings",
            ));
        }
        Ok(())
    }

    fn cause(&self) -> PublicationCause {
        PublicationCause {
            kind: ACCEPTANCE_KIND.to_owned(),
            observation_id: self.acceptance_observation_id,
            operation_id: self.accepted_operation_id.clone(),
            id: self.accepted_operation_id.clone(),
            task_id: self.task_id.clone(),
            task_revision: self.task_revision,
            attempt_id: self.attempt_id.clone(),
            submission_ref: self.submission_ref.clone(),
            candidate_ref: self.candidate_ref.clone(),
            canonical_repository: self.canonical_repository.clone(),
            gm_epoch: self.gm_epoch,
            policy_revision: self.policy_revision.clone(),
            target_ref: self.target_ref.clone(),
            expected_old_ref: self.expected_old_ref.clone(),
            expected_create: self.expected_create,
            activation_cut: self.activation_cut,
            historical_replay_authorized: self.historical_replay_authorized,
        }
    }

    pub(crate) fn technical_requester_id(&self) -> &str {
        &self.technical_requester_id
    }

    pub(crate) fn effective_manager_id(&self) -> &str {
        &self.effective_manager_id
    }

    pub(crate) fn automation_id(&self) -> &str {
        &self.automation_id
    }

    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }

    pub(crate) fn gm_epoch(&self) -> i64 {
        self.gm_epoch
    }

    pub(crate) fn accepted_operation_id(&self) -> &str {
        &self.accepted_operation_id
    }

    pub(crate) fn task_revision(&self) -> i64 {
        self.task_revision
    }

    pub(crate) fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    pub(crate) fn submission_ref(&self) -> &str {
        &self.submission_ref
    }

    pub(crate) fn candidate_ref(&self) -> &str {
        &self.candidate_ref
    }

    pub(crate) fn policy_revision(&self) -> &str {
        &self.policy_revision
    }

    pub(crate) fn canonical_repository(&self) -> &str {
        &self.canonical_repository
    }

    pub(crate) fn target_ref(&self) -> &str {
        &self.target_ref
    }

    pub(crate) fn expected_old_ref(&self) -> Option<&str> {
        self.expected_old_ref.as_deref()
    }

    pub(crate) fn expected_create(&self) -> bool {
        self.expected_create
    }
}

fn entry_matches_publication_settings(
    entry: &config::AutomationEntry,
    context: &PublicationContext,
) -> bool {
    entry.project_id == context.project_id
        && entry.automation_id == context.automation_id
        && entry.publication_ready()
        && entry.publication.as_ref().is_some_and(|settings| {
            settings.target_ref == context.target_ref
                && settings.expected_old_ref == context.expected_old_ref
                && settings.expected_create == context.expected_create
        })
}

fn validate_entry_authority(db: &Connection, entry: &config::AutomationEntry) -> Result<()> {
    config::validate_entry(entry)?;
    if !entry.publication_ready() {
        return Err(Error::new(
            "AUTOMATION_ACTION_UNAVAILABLE",
            "entry does not currently admit publication",
        ));
    }
    authorization::require_registered_manager(db, &entry.owner_manager_id)?;
    let current = config::load_entry(
        db,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?
    .ok_or_else(|| Error::new("FORBIDDEN", "publication automation entry was removed"))?;
    if current.value()? != entry.value()? {
        return Err(Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "current publication entry differs from the retained entry revision",
        ));
    }
    Ok(())
}

fn current_gm_epoch_for(db: &Connection, manager_id: &str) -> Result<i64> {
    let gm: Option<(String, i64)> = db
        .query_row(
            "SELECT json_extract(value_json,'$.client_id'),json_extract(value_json,'$.epoch') \
             FROM meta WHERE key='gm'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (gm_id, epoch) = gm.ok_or_else(|| {
        Error::new(
            "AUTOMATION_CURRENT_GM_REQUIRED",
            "automatic publication requires a designated current GM",
        )
    })?;
    if epoch <= 0 || gm_id != manager_id {
        return Err(Error::new(
            "AUTOMATION_CURRENT_GM_REQUIRED",
            "automatic publication requires the entry owner to be current GM",
        ));
    }
    Ok(epoch)
}

pub(crate) fn accepted_candidate_from_observation(
    db: &Connection,
    observation_id: i64,
    accepted_operation_id: &str,
) -> Result<AcceptedCandidate> {
    let event: Option<(Option<String>, Option<String>, String, String)> = db
        .query_row(
            "SELECT source_event_key,operation_id,kind,payload_json FROM observations \
             WHERE observation_id=?1 AND source_stream_id='controller:acceptance'",
            [observation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((source_event_key, operation_id, kind, payload_json)) = event else {
        return Err(Error::new(
            "AUTOMATION_FACT_MISSING",
            "publication acceptance Observation was not found",
        ));
    };
    let payload: Value = serde_json::from_str(&payload_json).map_err(|_| {
        Error::new(
            "AUTOMATION_FACT_CORRUPT",
            "publication acceptance Observation payload is invalid",
        )
    })?;
    if kind != ACCEPTANCE_KIND
        || operation_id.as_deref() != Some(accepted_operation_id)
        || source_event_key.as_deref() != Some(format!("accept:{accepted_operation_id}").as_str())
        || payload["outcome"] != "applied"
        || payload["task_accepted"] != true
        || payload["operation_id"] != accepted_operation_id
        || payload["acceptance_operation_id"] != accepted_operation_id
    {
        return Err(Error::new(
            "AUTOMATION_FACT_NOT_APPLIED",
            "publication requires the exact applied acceptance fact",
        ));
    }
    let task_id = model::text(&payload, "task_id")?.to_owned();
    let task_revision = model::positive(&payload, "task_revision")?;
    let attempt_id = model::text(&payload, "attempt_id")?.to_owned();
    let submission_ref = model::text(&payload, "submission_ref")?.to_owned();
    let candidate_ref = model::text(&payload, "candidate_ref")?.to_owned();
    let accepted_operation: Option<AcceptedOperationRow> = db
        .query_row(
            "SELECT method,state,task_id,attempt_id,original_request_json,result_json \
         FROM operations WHERE operation_id=?1",
            [accepted_operation_id],
            |row| {
                Ok(AcceptedOperationRow {
                    method: row.get(0)?,
                    state: row.get(1)?,
                    task_id: row.get(2)?,
                    attempt_id: row.get(3)?,
                    original_request_json: row.get(4)?,
                    result_json: row.get(5)?,
                })
            },
        )
        .optional()?;
    let Some(AcceptedOperationRow {
        method,
        state,
        task_id: operation_task_id,
        attempt_id: operation_attempt_id,
        original_request_json: request_json,
        result_json,
    }) = accepted_operation
    else {
        return Err(Error::new(
            "AUTOMATION_FACT_MISSING",
            "acceptance Operation was not found",
        ));
    };
    let result_json = result_json.ok_or_else(|| {
        Error::new(
            "AUTOMATION_FACT_NOT_APPLIED",
            "acceptance Operation has no retained result",
        )
    })?;
    let result: Value = serde_json::from_str(&result_json).map_err(|_| {
        Error::new(
            "AUTOMATION_FACT_CORRUPT",
            "acceptance Operation result is invalid",
        )
    })?;
    let request_value: Value = serde_json::from_str(&request_json)
        .map_err(|_| Error::new("AUTOMATION_FACT_CORRUPT", "acceptance request is invalid"))?;
    let accepted_request = crate::acceptance::AcceptRequest::parse(&request_value)?;
    if method != "task.accept"
        || state != "settled"
        || operation_task_id.as_deref() != Some(task_id.as_str())
        || operation_attempt_id.as_deref() != Some(attempt_id.as_str())
        || result != payload
        || result["outcome"] != "applied"
        || result["task_accepted"] != true
        || result["operation_id"] != accepted_operation_id
        || result["acceptance_operation_id"] != accepted_operation_id
        || result["task_id"] != task_id
        || result["task_revision"] != task_revision
        || result["attempt_id"] != attempt_id
        || result["submission_ref"] != submission_ref
        || result["candidate_ref"] != candidate_ref
        || accepted_request.attempt_id != attempt_id
        || accepted_request.expected_revision != task_revision
        || accepted_request.submission_ref != submission_ref
        || accepted_request.candidate_ref != candidate_ref
    {
        return Err(Error::new(
            "AUTOMATION_FACT_NOT_APPLIED",
            "acceptance Operation no longer matches its exact applied Observation",
        ));
    }

    let project_id: Option<String> = db
        .query_row(
            "SELECT project_id FROM tasks WHERE task_id=?1",
            [&task_id],
            |row| row.get(0),
        )
        .optional()?;
    let project_id = project_id
        .ok_or_else(|| Error::new("AUTOMATION_FACT_MISSING", "accepted Task was not found"))?;
    let attempt: Option<(String, i64, Option<String>, Option<String>)> = db
        .query_row(
            "SELECT task_id,task_revision,submission_ref,candidate_ref FROM attempts WHERE attempt_id=?1",
            [&attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    if !attempt.is_some_and(
        |(saved_task, saved_revision, saved_submission, saved_candidate)| {
            saved_task == task_id
                && saved_revision == task_revision
                && saved_submission.as_deref() == Some(submission_ref.as_str())
                && saved_candidate.as_deref() == Some(candidate_ref.as_str())
        },
    ) {
        return Err(Error::new(
            "FORGE_ACCEPTANCE_STALE",
            "acceptance Operation references a missing or mismatched retained Attempt",
        ));
    }
    Ok(AcceptedCandidate {
        accepted_operation_id: accepted_operation_id.to_owned(),
        task_id,
        project_id,
        task_revision,
        attempt_id,
        submission_ref,
        candidate_ref,
    })
}

/// Confirm that a retained applied acceptance is still the Task's exact
/// current candidate before admitting a new automated side effect. This
/// intentionally does not require Forge publication policy or artifacts.
pub(crate) fn validate_current_accepted_candidate_for_projection(
    db: &Connection,
    candidate: &AcceptedCandidate,
    expected_project_id: &str,
) -> Result<()> {
    let task: Option<(String, i64, String, Option<String>, Option<String>, Option<i64>, Option<String>)> = db
        .query_row(
            "SELECT project_id,revision,state,accepted_attempt_id,accepted_operation_id,accepted_revision,accepted_candidate_ref FROM tasks WHERE task_id=?1",
            [&candidate.task_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
        )
        .optional()?;
    let attempt: Option<(String, i64, String, Option<i64>, Option<String>, Option<String>)> = db
        .query_row(
            "SELECT task_id,task_revision,state,released_at_ms,submission_ref,candidate_ref FROM attempts WHERE attempt_id=?1",
            [&candidate.attempt_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )
        .optional()?;
    let invalidated: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM observations WHERE source_stream_id='controller:acceptance' AND source_event_key=?1 AND kind='task.acceptance_invalidated')",
        [format!("invalidate:{}", candidate.accepted_operation_id)],
        |row| row.get(0),
    )?;
    let current = task.is_some_and(
        |(
            project_id,
            revision,
            state,
            accepted_attempt,
            accepted_operation,
            accepted_revision,
            accepted_candidate,
        )| {
            project_id == expected_project_id
                && candidate.project_id == expected_project_id
                && revision == candidate.task_revision
                && state == "accepted"
                && accepted_attempt.as_deref() == Some(candidate.attempt_id.as_str())
                && accepted_operation.as_deref() == Some(candidate.accepted_operation_id.as_str())
                && accepted_revision == Some(candidate.task_revision)
                && accepted_candidate.as_deref() == Some(candidate.candidate_ref.as_str())
        },
    );
    let attempt_matches = attempt.is_some_and(
        |(task_id, revision, state, released_at, submission_ref, candidate_ref)| {
            task_id == candidate.task_id
                && revision == candidate.task_revision
                && state == "accepted"
                && released_at.is_none()
                && submission_ref.as_deref() == Some(candidate.submission_ref.as_str())
                && candidate_ref.as_deref() == Some(candidate.candidate_ref.as_str())
        },
    );
    if invalidated || !current || !attempt_matches {
        return Err(Error::new(
            "FORGE_ACCEPTANCE_STALE",
            "accepted Task/Attempt no longer names this exact current candidate",
        ));
    }
    Ok(())
}

fn validate_current_accepted_candidate(
    db: &Connection,
    candidate: &AcceptedCandidate,
    expected_project_id: &str,
    policy_revision: &str,
) -> Result<()> {
    let task: Option<AcceptedTaskRow> = db
        .query_row(
            "SELECT project_id,revision,state,accepted_attempt_id,accepted_operation_id,accepted_revision,accepted_candidate_ref \
             FROM tasks WHERE task_id=?1",
            [&candidate.task_id],
            |row| {
                Ok(AcceptedTaskRow {
                    project_id: row.get(0)?,
                    revision: row.get(1)?,
                    state: row.get(2)?,
                    accepted_attempt_id: row.get(3)?,
                    accepted_operation_id: row.get(4)?,
                    accepted_revision: row.get(5)?,
                    accepted_candidate_ref: row.get(6)?,
                })
            },
        )
        .optional()?;
    let attempt: Option<AcceptedAttemptRow> = db
        .query_row(
            "SELECT task_id,task_revision,state,released_at_ms,submission_ref,candidate_ref,task_snapshot_json \
             FROM attempts WHERE attempt_id=?1",
            [&candidate.attempt_id],
            |row| {
                Ok(AcceptedAttemptRow {
                    task_id: row.get(0)?,
                    task_revision: row.get(1)?,
                    state: row.get(2)?,
                    released_at_ms: row.get(3)?,
                    submission_ref: row.get(4)?,
                    candidate_ref: row.get(5)?,
                    task_snapshot_json: row.get(6)?,
                })
            },
        )
        .optional()?;
    let Some(AcceptedTaskRow {
        project_id,
        revision,
        state,
        accepted_attempt_id: accepted_attempt,
        accepted_operation_id: accepted_operation,
        accepted_revision,
        accepted_candidate_ref: accepted_candidate,
    }) = task
    else {
        return Err(Error::new(
            "FORGE_ACCEPTANCE_STALE",
            "accepted Task was removed",
        ));
    };
    let Some(AcceptedAttemptRow {
        task_id: attempt_task,
        task_revision: attempt_revision,
        state: attempt_state,
        released_at_ms: released_at,
        submission_ref,
        candidate_ref,
        task_snapshot_json: snapshot_raw,
    }) = attempt
    else {
        return Err(Error::new(
            "FORGE_ACCEPTANCE_STALE",
            "accepted Attempt was removed",
        ));
    };
    let invalidated: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM observations WHERE source_stream_id='controller:acceptance' \
         AND source_event_key=?1 AND kind='task.acceptance_invalidated')",
        [format!("invalidate:{}", candidate.accepted_operation_id)],
        |row| row.get(0),
    )?;
    let snapshot: Value = serde_json::from_str(&snapshot_raw).map_err(|_| {
        Error::new(
            "FORGE_ACCEPTANCE_STALE",
            "accepted Attempt snapshot is invalid",
        )
    })?;
    if invalidated
        || project_id != expected_project_id
        || candidate.project_id != expected_project_id
        || revision != candidate.task_revision
        || state != "accepted"
        || accepted_attempt.as_deref() != Some(candidate.attempt_id.as_str())
        || accepted_operation.as_deref() != Some(candidate.accepted_operation_id.as_str())
        || accepted_revision != Some(candidate.task_revision)
        || accepted_candidate.as_deref() != Some(candidate.candidate_ref.as_str())
        || attempt_task != candidate.task_id
        || attempt_revision != candidate.task_revision
        || attempt_state != "accepted"
        || released_at.is_some()
        || submission_ref.as_deref() != Some(candidate.submission_ref.as_str())
        || candidate_ref.as_deref() != Some(candidate.candidate_ref.as_str())
        || snapshot["spec"]["owner_policy_id"] != policy_revision
    {
        return Err(Error::new(
            "FORGE_ACCEPTANCE_STALE",
            "accepted Task/Attempt no longer names the exact current candidate and policy",
        ));
    }

    let submission: Option<(String, Option<String>, String)> = db
        .query_row(
            "SELECT kind,content_digest,metadata_json FROM artifacts WHERE artifact_id=?1",
            [&candidate.submission_ref],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((submission_kind, submission_digest, submission_metadata_raw)) = submission else {
        return Err(Error::new(
            "FORGE_SUBMISSION_MISMATCH",
            "accepted submission artifact was removed",
        ));
    };
    let submission_metadata: Value =
        serde_json::from_str(&submission_metadata_raw).map_err(|_| {
            Error::new(
                "FORGE_SUBMISSION_MISMATCH",
                "accepted submission metadata is invalid",
            )
        })?;
    let submission_operation_id = model::text(&submission_metadata, "operation_id")?;
    let submission_operation: Option<SubmissionOperationRow> = db
        .query_row(
            "SELECT method,state,task_id,attempt_id,result_json,effective_request_json \
             FROM operations WHERE operation_id=?1",
            [submission_operation_id],
            |row| {
                Ok(SubmissionOperationRow {
                    method: row.get(0)?,
                    state: row.get(1)?,
                    task_id: row.get(2)?,
                    attempt_id: row.get(3)?,
                    result_json: row.get(4)?,
                    effective_request_json: row.get(5)?,
                })
            },
        )
        .optional()?;
    let Some(SubmissionOperationRow {
        method: submission_method,
        state: submission_state,
        task_id: submission_task_id,
        attempt_id: submission_attempt_id,
        result_json: submission_result_raw,
        effective_request_json: submission_effective_raw,
    }) = submission_operation
    else {
        return Err(Error::new(
            "FORGE_SUBMISSION_MISMATCH",
            "submission Operation was removed",
        ));
    };
    let submission_result: Value = submission_result_raw
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?
        .unwrap_or(Value::Null);
    let submission_effective: Value = serde_json::from_str(&submission_effective_raw)?;
    let submission_document = submission_effective
        .get("submission_document")
        .ok_or_else(|| {
            Error::new(
                "FORGE_SUBMISSION_MISMATCH",
                "submission document is missing",
            )
        })?;
    let submission_document_digest =
        model::digest(model::canonical(submission_document)?.as_bytes());

    let source: Option<(String, Option<String>, String)> = db
        .query_row(
            "SELECT kind,content_digest,metadata_json FROM artifacts WHERE artifact_id=?1",
            [&candidate.candidate_ref],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((source_kind, source_digest, source_metadata_raw)) = source else {
        return Err(Error::new(
            "FORGE_CANDIDATE_MISMATCH",
            "accepted source snapshot was removed",
        ));
    };
    let source_metadata: Value = serde_json::from_str(&source_metadata_raw).map_err(|_| {
        Error::new(
            "FORGE_CANDIDATE_MISMATCH",
            "accepted source snapshot metadata is invalid",
        )
    })?;
    let source_digest = source_digest.ok_or_else(|| {
        Error::new(
            "FORGE_CANDIDATE_MISMATCH",
            "accepted source snapshot has no content digest",
        )
    })?;
    if submission_kind != "task_submission"
        || submission_digest.as_deref() != Some(submission_document_digest.as_str())
        || submission_document["operation_id"] != submission_operation_id
        || submission_document["task_id"] != candidate.task_id
        || submission_document["attempt_id"] != candidate.attempt_id
        || submission_document["task_revision"] != candidate.task_revision
        || submission_document["candidate_ref"] != candidate.candidate_ref
        || submission_document["candidate_sha256"] != source_digest
        || submission_method != "task.submit"
        || submission_state != "settled"
        || submission_task_id.as_deref() != Some(candidate.task_id.as_str())
        || submission_attempt_id.as_deref() != Some(candidate.attempt_id.as_str())
        || submission_result["operation_id"] != submission_operation_id
        || submission_result["outcome"] != "applied"
        || submission_result["submission_ref"] != candidate.submission_ref
        || submission_result["candidate_ref"] != candidate.candidate_ref
        || submission_result["attempt_id"] != candidate.attempt_id
        || source_kind != "source_snapshot"
        || source_metadata["task_id"] != candidate.task_id
        || source_metadata["attempt_id"] != candidate.attempt_id
        || source_metadata["task_revision"] != candidate.task_revision
        || source_metadata["coverage"] != "complete"
    {
        return Err(Error::new(
            "FORGE_SUBMISSION_MISMATCH",
            "accepted submission provenance does not identify the exact source snapshot",
        ));
    }
    Ok(())
}
