//! Store authority for one explicitly configured, fresh owned OpenCode service.
//!
//! This module is the only place that can mint an owned-service intent and a
//! one-shot start permit. The general OpenCode `native_options` route is never
//! interpreted as authority to create, adopt, or restart a process.

use super::{Store, workspace};
use crate::{
    config::{Config, OwnedOpenCodeServiceConfig, Route},
    error::{Error, Result},
    model,
    runtime::opencode_v2::{
        Options,
        owned_service::{
            OwnedServiceHandle, OwnedServiceIntent, OwnedServiceOrigin, OwnedServiceReadback,
            OwnedServiceRoute, OwnedServiceSeed, OwnedServiceStartFailure, OwnedServiceStartPermit,
            prepare_owned_service, prepare_owned_service_with_provider_auth, start_foreground,
        },
    },
    workspace::LeaseAuthorityRef,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::path::PathBuf;
use tokio::sync::watch;

const MAX_PROOF_BYTES: usize = 8 * 1024;
const MAX_ID_BYTES: usize = 256;
const PINNED_SERVICE_VERSION: &str = "2.0.7";

type DepartureAttemptRow = (String, i64, String, Option<String>, Option<i64>);
type HistoricalLeaseRow = (
    String,
    String,
    i64,
    String,
    i64,
    String,
    String,
    Option<String>,
    i64,
    String,
    String,
    String,
    String,
);

/// A verified private projection for a currently live owned binding. It is
/// intentionally crate-private: callers use it to connect/read the service,
/// never to expose filesystem paths, endpoints, or credentials to a client.
pub(crate) struct VerifiedOwnedServiceBinding {
    route: OwnedServiceRoute,
    service_id: String,
    service_version: String,
    owner_nonce: String,
    process_id: u32,
    process_birth_token: String,
    executable_sha256: String,
    endpoint_digest: String,
    connection_digest: String,
    config_digest: String,
    plugin_module_sha256: String,
    proof_digest: String,
    identity_digest: String,
}

impl VerifiedOwnedServiceBinding {
    pub(crate) fn route(&self) -> &OwnedServiceRoute {
        &self.route
    }
    pub(crate) fn options(&self) -> Options {
        self.route.options()
    }
    pub(crate) fn service_id(&self) -> &str {
        &self.service_id
    }
    pub(crate) fn service_version(&self) -> &str {
        &self.service_version
    }
    pub(crate) fn owner_nonce(&self) -> &str {
        &self.owner_nonce
    }
    pub(crate) fn process_id(&self) -> u32 {
        self.process_id
    }
    pub(crate) fn process_birth_token(&self) -> &str {
        &self.process_birth_token
    }
    pub(crate) fn executable_sha256(&self) -> &str {
        &self.executable_sha256
    }
    pub(crate) fn endpoint_digest(&self) -> &str {
        &self.endpoint_digest
    }
    pub(crate) fn connection_digest(&self) -> &str {
        &self.connection_digest
    }
    pub(crate) fn config_digest(&self) -> &str {
        &self.config_digest
    }
    pub(crate) fn plugin_module_sha256(&self) -> &str {
        &self.plugin_module_sha256
    }
    pub(crate) fn proof_digest(&self) -> &str {
        &self.proof_digest
    }
    pub(crate) fn identity_digest(&self) -> &str {
        &self.identity_digest
    }
}

#[derive(Debug, Clone)]
struct OwnedStartRow {
    launch_operation_id: String,
    open_operation_id: String,
    binding_id: String,
    binding_generation: i64,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    lease_id: String,
    lease_generation: i64,
    technical_requester_id: String,
    effective_manager_id: String,
    service_id: String,
    service_version: String,
    route_digest: String,
    binding_digest: String,
    owner_nonce: String,
    intent_digest: String,
    state: String,
    process_id: Option<i64>,
    process_birth_token: Option<String>,
    executable_sha256: Option<String>,
    proof_json: String,
    updated_at_ms: i64,
}

struct OpeningScope {
    actor: super::launcher::LaunchActor,
    launch_operation_id: String,
    open_operation_id: String,
    open_operation_state: String,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    lease: LeaseAuthorityRef,
    workspace_directory: PathBuf,
    binding_id: String,
    binding_generation: i64,
    binding_digest: String,
    service_config: OwnedOpenCodeServiceConfig,
    actor_manifest: Value,
}

struct StartAdmission {
    actor: super::launcher::LaunchActor,
    route: OwnedServiceRoute,
    intent: OwnedServiceIntent,
    row: OwnedStartRow,
}

struct DepartureCandidate {
    row: OwnedStartRow,
    service_config: Option<OwnedOpenCodeServiceConfig>,
    workspace_directory: Option<PathBuf>,
}

impl Store {
    /// Recover a process that was already durably observed for an active
    /// binding. This path deliberately has no launch actor, intent, start
    /// permit, or process-control capability: it only authenticates the exact
    /// retained process and then rechecks the current binding scope.
    async fn readback_observed_owned_opencode_service(
        &self,
        binding_id: &str,
        generation: i64,
        stopping: &watch::Receiver<bool>,
    ) -> Result<Option<OwnedServiceHandle>> {
        let binding_id = binding_id.to_owned();
        let config = self.config.clone();
        let binding_id_for_read = binding_id.clone();
        let retained = self
            .run(move |db| {
                let binding = binding_row(db, &binding_id_for_read, generation)?;
                if !matches!(binding.state.as_str(), "ready" | "reconciling") {
                    return Ok(None);
                }
                let row =
                    load_start_row(db, &binding_id_for_read, generation)?.ok_or_else(|| {
                        Error::new(
                            "OWNED_SERVICE_NOT_OBSERVED",
                            "active owned binding has no startup receipt",
                        )
                    })?;
                if row.state != "service_observed" {
                    return Err(recovery_required(&row.state));
                }
                let verified =
                    owned_service_for_binding(db, &config, &binding_id_for_read, generation)?
                        .ok_or_else(|| corrupt("active owned binding lost its explicit route"))?;
                let proof: Value = serde_json::from_str(&row.proof_json)
                    .map_err(|_| corrupt("retained owned service proof is invalid JSON"))?;
                Ok(Some((
                    verified.route().clone(),
                    verified.owner_nonce().to_owned(),
                    verified.identity_digest().to_owned(),
                    proof,
                    row,
                )))
            })
            .await?;
        let Some((route, owner_nonce, identity_digest, expected_proof, expected_row)) = retained
        else {
            return Ok(None);
        };
        if *stopping.borrow() {
            return Err(shutdown_error());
        }

        // This runtime call only reads the private owner/connection/config
        // receipts, validates PID birth and image identity, and authenticates
        // the existing service. It cannot spawn or stop a process.
        let handle =
            crate::runtime::opencode_v2::owned_service::readback_retained(&route, &expected_proof)
                .await?;
        if model::canonical(&handle.readback().store_proof())? != model::canonical(&expected_proof)?
        {
            return Err(corrupt(
                "recovered process differs from its retained readback",
            ));
        }

        // The route and current Task/Attempt/lease may have changed while the
        // network readback ran. Revalidate before returning the service handle.
        let config = self.config.clone();
        let binding_id_for_check = binding_id.clone();
        self.run(move |db| {
            let binding = binding_row(db, &binding_id_for_check, generation)?;
            if !matches!(binding.state.as_str(), "ready" | "reconciling")
                || binding.released_at_ms.is_some()
            {
                return Err(scope_changed());
            }
            let row = load_start_row(db, &binding_id_for_check, generation)?
                .ok_or_else(|| corrupt("owned service readback row disappeared"))?;
            if row.state != "service_observed" {
                return Err(recovery_required(&row.state));
            }
            verify_start_row(&row, &expected_row)?;
            let current_proof: Value = serde_json::from_str(&row.proof_json)
                .map_err(|_| corrupt("retained owned service proof is invalid JSON"))?;
            if model::canonical(&current_proof)? != model::canonical(&expected_proof)? {
                return Err(corrupt(
                    "retained owned service proof changed during recovery",
                ));
            }
            let verified =
                owned_service_for_binding(db, &config, &binding_id_for_check, generation)?
                    .ok_or_else(|| corrupt("active owned binding lost its explicit route"))?;
            if verified.identity_digest() != identity_digest.as_str()
                || verified.owner_nonce() != owner_nonce.as_str()
            {
                return Err(scope_changed());
            }
            Ok(())
        })
        .await?;
        if *stopping.borrow() {
            return Err(shutdown_error());
        }
        Ok(Some(handle))
    }

    async fn close_owned_service_and_reconcile(&self, handle: OwnedServiceHandle) {
        if handle.close_gracefully().await.is_ok() {
            let _ = self.reconcile_owned_opencode_departures_once().await;
        }
    }

    /// Resolve the exact queued `agent.open` service, reserve its immutable
    /// owner nonce, then cross one durable CAS before permitting a process
    /// start. Existing unknown/observed rows are never replayed.
    pub(crate) async fn ensure_owned_opencode_service(
        &self,
        binding_id: &str,
        generation: i64,
        stopping: watch::Receiver<bool>,
    ) -> Result<OwnedServiceHandle> {
        if *stopping.borrow() {
            return Err(shutdown_error());
        }
        if let Some(handle) = self
            .readback_observed_owned_opencode_service(binding_id, generation, &stopping)
            .await?
        {
            return Ok(handle);
        }
        let binding_id = binding_id.to_owned();
        let config = self.config.clone();
        let binding_id_for_read = binding_id.clone();
        let (scope, existing) = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let scope = opening_scope(&tx, &config, &binding_id_for_read, generation)?;
                let existing = load_start_row(&tx, &binding_id_for_read, generation)?;
                tx.commit()?;
                Ok((scope, existing))
            })
            .await?;
        let owner_nonce = existing
            .as_ref()
            .map(|row| row.owner_nonce.clone())
            .unwrap_or_else(model::new_id);
        // Route canonicalization and pinned binary/script hashing perform
        // filesystem reads, so keep them outside the Store writer transaction.
        let admission = make_admission(&scope, owner_nonce)?;
        let expected_row = admission.row.clone();
        let expected_workspace_directory = admission.route.workspace_directory().to_path_buf();
        let expected_actor = admission.actor.clone();
        let config = self.config.clone();
        let binding_id_for_reserve = binding_id.clone();
        let row = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current_scope =
                    opening_scope(&tx, &config, &binding_id_for_reserve, generation)?;
                verify_scope_for_admission(
                    &current_scope,
                    &expected_actor,
                    &expected_row,
                    &expected_workspace_directory,
                )?;
                match load_start_row(&tx, &binding_id_for_reserve, generation)? {
                    Some(row) => {
                        verify_start_row(&row, &expected_row)?;
                        tx.commit()?;
                        Ok(row)
                    }
                    None => {
                        insert_start_reservation(&tx, &expected_row)?;
                        let row = load_start_row(&tx, &binding_id_for_reserve, generation)?
                            .ok_or_else(|| corrupt("owned service reservation was not retained"))?;
                        verify_start_row(&row, &expected_row)?;
                        tx.commit()?;
                        Ok(row)
                    }
                }
            })
            .await?;
        let admission = StartAdmission { row, ..admission };

        if admission.row.state != "reserved" {
            if admission.row.state == "failed_no_effect" {
                return Err(recovery_required(&admission.row.state));
            }
            let expected_proof: Value = serde_json::from_str(&admission.row.proof_json)
                .map_err(|_| corrupt("retained owned service proof is invalid JSON"))?;
            let handle = crate::runtime::opencode_v2::owned_service::readback_existing(
                &admission.route,
                &admission.intent,
                &expected_proof,
            )
            .await?
            .ok_or_else(|| recovery_required(&admission.row.state))?;
            let observed = handle.readback().store_proof();
            if admission.row.state == "outcome_unknown" {
                let route = admission.route;
                let intent = admission.intent;
                let row = admission.row.clone();
                let proof = observed.clone();
                if let Err(error) = self
                    .run(move |db| persist_observed_proof(db, &row, &route, &intent, &proof))
                    .await
                {
                    self.close_owned_service_and_reconcile(handle).await;
                    return Err(error);
                }
            } else {
                let existing: Value = serde_json::from_str(&admission.row.proof_json)
                    .map_err(|_| corrupt("retained owned service proof is invalid JSON"))?;
                verify_readback_value(&observed, &admission.route, &admission.row)?;
                if model::canonical(&observed)? != model::canonical(&existing)? {
                    return Err(corrupt(
                        "live owned service differs from the exact retained readback",
                    ));
                }
            }
            let config = self.config.clone();
            let binding_id = binding_id.clone();
            let open_id = admission.row.open_operation_id.clone();
            if let Err(error) = self
                .run(move |db| {
                    validate_owned_open_dispatch(db, &config, &binding_id, generation, &open_id)
                })
                .await
            {
                self.close_owned_service_and_reconcile(handle).await;
                return Err(error);
            }
            if *stopping.borrow() {
                self.close_owned_service_and_reconcile(handle).await;
                return Err(shutdown_error());
            }
            return Ok(handle);
        }

        // Preparation is effect-free. It may read pinned source files but does
        // not create files, contact OpenCode, or admit native work.
        let prepared = match admission.route.credential_ref() {
            Some(credential_ref) => {
                let auth_source = self
                    .config
                    .opencode_provider_auth_source(credential_ref, admission.route.model_ref())?;
                prepare_owned_service_with_provider_auth(
                    &admission.route,
                    &admission.intent,
                    auth_source.as_deref(),
                )?
            }
            None => prepare_owned_service(&admission.route, &admission.intent)?,
        };
        if *stopping.borrow() {
            return Err(shutdown_error());
        }
        // Validate and mint the opaque, one-shot token before the durable
        // unknown boundary so token-construction failure cannot strand it.
        // The token stays local and is not handed to the runtime until the CAS
        // below commits successfully.
        let prepared_config_digest = prepared.config_digest().to_owned();
        let permit = OwnedServiceStartPermit::from_store_admission(&admission.intent, &prepared)?;

        // A stop racing this transaction leaves the durable row `reserved`,
        // which can be safely resumed with the same nonce. After this commit,
        // we must await and reconcile the one effect rather than cancel it.
        let config = self.config.clone();
        let binding_id_for_start = binding_id.clone();
        let launch_id = admission.row.launch_operation_id.clone();
        let route_digest = admission.row.route_digest.clone();
        let intent_digest = admission.row.intent_digest.clone();
        let expected_actor = admission.actor.clone();
        let expected_row = admission.row.clone();
        let expected_workspace_directory = admission.route.workspace_directory().to_path_buf();
        let stop_before_start = stopping.clone();
        self.run(move |db| {
            if *stop_before_start.borrow() {
                return Err(shutdown_error());
            }
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let current_scope = opening_scope(&tx, &config, &binding_id_for_start, generation)?;
            verify_scope_for_admission(
                &current_scope,
                &expected_actor,
                &expected_row,
                &expected_workspace_directory,
            )?;
            let current_row = load_start_row(&tx, &binding_id_for_start, generation)?
                .ok_or_else(|| corrupt("owned service reservation disappeared"))?;
            verify_start_row(&current_row, &expected_row)?;
            if current_row.state != "reserved" {
                return Err(recovery_required(&current_row.state));
            }
            if super::meta(&tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"]
                != "enabled"
            {
                return Err(Error::new(
                    "ADMISSION_DISABLED",
                    "new work is disabled for this owned service start",
                ));
            }
            let changed = tx.execute(
                "UPDATE owned_service_starts SET state='outcome_unknown',updated_at_ms=?4
                 WHERE launch_operation_id=?1 AND binding_id=?2 AND binding_generation=?3
                   AND state='reserved' AND intent_nonce=?5 AND route_digest=?6 AND intent_digest=?7",
                params![
                    launch_id,
                    binding_id_for_start,
                    generation,
                    model::now_ms()?,
                    expected_row.owner_nonce,
                    route_digest,
                    intent_digest,
                ],
            )?;
            if changed != 1 {
                return Err(Error::conflict("owned service one-shot start reservation changed"));
            }
            tx.commit()?;
            Ok(())
        })
        .await?;

        // The permit was validated before the `unknown` boundary but stayed
        // local until that boundary committed. A failure from here onward is
        // never interpreted as permission to retry the spawn.
        let handle = match start_foreground(prepared, permit).await {
            Ok(handle) => handle,
            Err(OwnedServiceStartFailure::ProvenNoEffect(no_effect)) => {
                if !no_effect.matches(&admission.intent, &prepared_config_digest) {
                    return Err(corrupt(
                        "pre-spawn no-effect proof differs from the exact Store launch intent",
                    ));
                }

                let row = admission.row.clone();
                let config = self.config.clone();
                let binding_id = row.binding_id.clone();
                let expected_actor = admission.actor.clone();
                let expected_row = row.clone();
                let workspace_directory = admission.route.workspace_directory().to_path_buf();
                let scope_current = self
                    .run(move |db| {
                        let scope = opening_scope(&*db, &config, &binding_id, generation)?;
                        verify_scope_for_admission(
                            &scope,
                            &expected_actor,
                            &expected_row,
                            &workspace_directory,
                        )
                    })
                    .await
                    .is_ok();
                let proof_json = failed_no_effect_proof(
                    &row,
                    &prepared_config_digest,
                    no_effect.safe_code(),
                    scope_current,
                )?;
                self.run(move |db| persist_failed_no_effect(db, &row, &proof_json))
                    .await?;
                return Err(Error::new(
                    no_effect.safe_code(),
                    "owned service failed before helper spawn; no process was started",
                ));
            }
            Err(OwnedServiceStartFailure::Unknown(error)) => return Err(error),
        };
        let proof = handle.readback().store_proof();
        let route = admission.route;
        let intent = admission.intent;
        let row = admission.row;
        let open_id = row.open_operation_id.clone();
        let proof_for_store = proof.clone();
        let persist_result = self
            .run(move |db| persist_observed_proof(db, &row, &route, &intent, &proof_for_store))
            .await;
        if let Err(error) = persist_result {
            self.close_owned_service_and_reconcile(handle).await;
            return Err(error);
        }

        // Proof persistence precedes the final current-authority check. If a
        // manager, Attempt, lease, binding, or launch authorization went stale
        // after the native effect began, retain the process fence and stop the
        // service gracefully; never erase or repeat its observed effect.
        let config = self.config.clone();
        let binding_id_for_check = binding_id.clone();
        let final_check = self
            .run(move |db| {
                validate_owned_open_dispatch(
                    db,
                    &config,
                    &binding_id_for_check,
                    generation,
                    &open_id,
                )
            })
            .await;
        if let Err(error) = final_check {
            self.close_owned_service_and_reconcile(handle).await;
            return Err(error);
        }
        if *stopping.borrow() {
            self.close_owned_service_and_reconcile(handle).await;
            return Err(shutdown_error());
        }
        Ok(handle)
    }

    /// Reconcile at most 32 uncertain/observed starts without requiring a
    /// current Task, manager, Attempt, or held lease. Cleanup proves only that
    /// the exact retained service process departed; it never spawns or adopts.
    pub(crate) async fn reconcile_owned_opencode_departures_once(&self) -> Result<usize> {
        let (candidates, cursor_base) = self.run(|db| departure_batch(db)).await?;
        let mut departed_count = 0usize;
        for (index, candidate) in candidates.into_iter().enumerate() {
            let cursor_ms = cursor_base
                .checked_add(
                    i64::try_from(index + 1).map_err(|_| corrupt("departure cursor overflow"))?,
                )
                .ok_or_else(|| corrupt("departure cursor overflow"))?;
            let row = candidate.row;
            let mut committed = false;
            if let (Some(service_config), Some(workspace_directory)) =
                (candidate.service_config, candidate.workspace_directory)
                && let Ok(base_route) = OwnedServiceRoute::from_config(&service_config)
                && let Ok(route) = base_route.for_launch(&row.owner_nonce, &workspace_directory)
                && route.service_id() == row.service_id
                && route.version() == row.service_version
                && let Ok(route_digest) = route.route_digest()
                && route_digest == row.route_digest
            {
                let stored_proof = serde_json::from_str::<Value>(&row.proof_json);
                if let Ok(stored_proof) = stored_proof
                    && (row.state != "service_observed"
                        || verify_readback_value(&stored_proof, &route, &row).is_ok())
                    && (row.state != "outcome_unknown"
                        || stored_proof
                            .as_object()
                            .is_some_and(serde_json::Map::is_empty))
                    && let Ok(Some(evidence)) =
                        crate::runtime::opencode_v2::owned_service::observe_departure(
                            &route,
                            &stored_proof,
                        )
                {
                    let departure_proof = evidence.store_proof();
                    if let Ok((process_id, birth_token, binary_sha256)) =
                        validate_departure_proof(&departure_proof, &route, &row, &stored_proof)
                    {
                        let envelope = json!({
                            "schema_version":1,
                            "status":"service_departed",
                            "service_proof":stored_proof,
                            "departure":departure_proof,
                        });
                        if let Ok(canonical) = model::canonical(&envelope)
                            && canonical.len() <= MAX_PROOF_BYTES
                        {
                            let expected_row = row.clone();
                            let expected_proof_json = row.proof_json.clone();
                            let departed_proof_json = canonical;
                            let updated = self
                                .run(move |db| {
                                    persist_departure(
                                        db,
                                        &expected_row,
                                        &expected_proof_json,
                                        &departed_proof_json,
                                        process_id,
                                        &birth_token,
                                        &binary_sha256,
                                        cursor_ms,
                                    )
                                })
                                .await?;
                            if updated {
                                departed_count += 1;
                                committed = true;
                            }
                        }
                    }
                }
            }
            if !committed {
                let row = row.clone();
                let proof_json = row.proof_json.clone();
                self.run(move |db| touch_departure_candidate(db, &row, &proof_json, cursor_ms))
                    .await?;
            }
        }
        Ok(departed_count)
    }
}

/// Read the exact private owned-service projection for a live binding. The
/// route must be explicitly owned in current configuration and equal the
/// immutable route stored with the binding. External `native_options` is not
/// consulted for owned bindings.
pub(crate) fn owned_service_for_binding(
    db: &Connection,
    config: &Config,
    binding_id: &str,
    generation: i64,
) -> Result<Option<VerifiedOwnedServiceBinding>> {
    let binding = binding_row(db, binding_id, generation)?;
    let stored_route = parse_route(&binding.route_json)?;
    let route = config.route(&stored_route.alias)?;
    if route.owned_service.is_none() {
        current_route(config, &stored_route)?;
        if load_start_row(db, binding_id, generation)?.is_some() {
            return Err(scope_changed());
        }
        return Ok(None);
    }
    let row = load_start_row(db, binding_id, generation)?.ok_or_else(|| {
        Error::new(
            "OWNED_SERVICE_NOT_OBSERVED",
            "owned service has no retained startup proof",
        )
    })?;
    if row.state != "service_observed" {
        return Err(recovery_required(&row.state));
    }
    let workspace_directory =
        retained_scope(db, binding_id, generation, &binding, &stored_route, &row)?;
    let configured = current_owned_route(config, &stored_route, &workspace_directory)?;
    let definition = configured.owned_service.clone().ok_or_else(scope_changed)?;
    let base_route = OwnedServiceRoute::from_config(&definition)?;
    let route = base_route.for_launch(&row.owner_nonce, &workspace_directory)?;
    let proof: Value = serde_json::from_str(&row.proof_json)
        .map_err(|_| corrupt("owned service readback proof is invalid JSON"))?;
    let verified = verify_readback_value(&proof, &route, &row)?;
    Ok(Some(verified))
}

/// Private adapter for callers that need the existing OpenCode `Options`
/// shape. For an owned route this succeeds only after strict retained process
/// readback validation. It never parses `native_options` as owned authority.
pub(crate) fn effective_options_for_binding(
    db: &Connection,
    config: &Config,
    binding_id: &str,
    generation: i64,
) -> Result<Option<Options>> {
    owned_service_for_binding(db, config, binding_id, generation)
        .map(|binding| binding.map(|binding| binding.options()))
}

/// Recheck the one pre-send boundary for an owned `agent.open`: the exact
/// launch/open pair must still be queued, its binding must remain opening with
/// no native root, and a strict service-observed row must match that scope.
/// Never call this after NativeRoot creation or Task dispatch.
pub(crate) fn validate_owned_open_dispatch(
    db: &Connection,
    config: &Config,
    binding_id: &str,
    generation: i64,
    open_operation_id: &str,
) -> Result<()> {
    let scope = opening_scope(db, config, binding_id, generation)?;
    if scope.open_operation_id != open_operation_id {
        return Err(scope_changed());
    }
    let row = load_start_row(db, binding_id, generation)?.ok_or_else(|| {
        Error::new(
            "OWNED_SERVICE_NOT_OBSERVED",
            "owned service start receipt is missing",
        )
    })?;
    if row.state != "service_observed"
        || row.open_operation_id != open_operation_id
        || row.launch_operation_id != scope.launch_operation_id
    {
        return Err(recovery_required(&row.state));
    }
    verify_scope_for_admission(&scope, &scope.actor, &row, &scope.workspace_directory)?;
    let proof: Value = serde_json::from_str(&row.proof_json)
        .map_err(|_| corrupt("owned service readback proof is invalid JSON"))?;
    let route = OwnedServiceRoute::from_config(&scope.service_config)?
        .for_launch(&row.owner_nonce, &scope.workspace_directory)?;
    validate_readback_proof_fields(&proof, &row, &route)?;
    Ok(())
}

fn opening_scope(
    db: &Connection,
    config: &Config,
    binding_id: &str,
    generation: i64,
) -> Result<OpeningScope> {
    if binding_id.is_empty() || binding_id.len() > MAX_ID_BYTES || generation <= 0 {
        return Err(Error::invalid("owned service binding identity is invalid"));
    }
    let binding = binding_row(db, binding_id, generation)?;
    if binding.state != "opening"
        || binding.released_at_ms.is_some()
        || binding.native_root_id.is_some()
        || binding.native_scope_key.is_some()
    {
        return Err(scope_changed());
    }
    let stored_route = parse_route(&binding.route_json)?;
    let parent_rows = launch_rows_for_binding(db, binding_id, generation)?;
    if parent_rows.len() != 1 {
        return Err(scope_changed());
    }
    let parent = &parent_rows[0];
    let task_id = parent
        .task_id
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or_else(scope_changed)?;
    let attempt_id = parent
        .attempt_id
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or_else(scope_changed)?;
    if parent.state != "queued" {
        return Err(scope_changed());
    }
    let actor = super::launcher::launch_actor(db, &parent.operation_id)?;
    let manifest: Value = serde_json::from_str(&parent.effective_request_json)
        .map_err(|_| corrupt("launch manifest is invalid JSON"))?;
    let manifest = manifest
        .get("launch_manifest")
        .ok_or_else(|| corrupt("launch manifest is missing"))?;
    let task_revision = manifest["task"]["observed_revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| corrupt("launch Task revision is missing"))?;
    let open_operation_id = manifest["binding"]["operation_id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| corrupt("queued agent.open identity is missing"))?
        .to_owned();
    if stored_route.alias != manifest["runtime"]["route"]
        || binding.lane_id
            != format!(
                "launch-{}",
                manifest["workspace"]["lease_authority"]["lease_id"]
                    .as_str()
                    .unwrap_or_default()
            )
        || binding.module_artifact_id != crate::runtime::opencode_v2::ARTIFACT_ID
        || manifest["task"]["task_id"] != task_id
        || manifest["task"]["attempt_id"] != attempt_id
        || manifest["binding"]["binding_id"] != binding_id
        || manifest["binding"]["generation"] != generation
    {
        return Err(scope_changed());
    }
    let open = operation_row(db, &open_operation_id)?.ok_or_else(|| {
        Error::new(
            "LAUNCH_OPEN_MISSING",
            "queued agent.open Operation is missing",
        )
    })?;
    if open.method != "agent.open"
        || open.state != "queued"
        || open.caller_id != parent.caller_id
        || open.prerequisite_operation_id.as_deref() != Some(parent.operation_id.as_str())
        || open.task_id.as_deref() != Some(task_id.as_str())
        || open.attempt_id.as_deref() != Some(attempt_id.as_str())
        || open.binding_id.as_deref() != Some(binding_id)
        || open.binding_generation != Some(generation)
    {
        return Err(scope_changed());
    }
    let lease: LeaseAuthorityRef =
        serde_json::from_value(manifest["workspace"]["lease_authority"].clone())
            .map_err(|_| corrupt("launch held-lease authority is invalid"))?;
    let held = workspace::held_lease_for_operation(db, &parent.operation_id)?
        .ok_or_else(|| Error::new("WORKSPACE_LEASE_STALE", "launch no longer has a held lease"))?;
    if held != lease
        || lease.state != "held"
        || lease.operation_id != parent.operation_id
        || lease.task_id != task_id
        || lease.task_revision != task_revision
        || lease.attempt_id.as_deref() != Some(attempt_id.as_str())
        || lease.owner_client_id != actor.effective_manager_id()
        || lease.generation <= 0
        || lease.binding_digest.is_empty()
    {
        return Err(scope_changed());
    }
    workspace::get_lease_view(db, &lease)?;
    let workspace_directory = held_workspace_path(db, &lease)?;
    let configured = current_owned_route(config, &stored_route, &workspace_directory)?;
    let service_config = configured.owned_service.clone().ok_or_else(|| {
        Error::new(
            "OWNED_SERVICE_ROUTE_REQUIRED",
            "route does not declare a fresh owned service",
        )
    })?;
    actor.require_opening_launch_attempt(
        db,
        &parent.operation_id,
        &task_id,
        task_revision,
        &attempt_id,
        binding_id,
        generation,
    )?;
    Ok(OpeningScope {
        actor,
        launch_operation_id: parent.operation_id.clone(),
        open_operation_id,
        open_operation_state: open.state,
        task_id,
        task_revision,
        attempt_id,
        lease,
        workspace_directory,
        binding_id: binding_id.to_owned(),
        binding_generation: generation,
        binding_digest: manifest["workspace"]["lease_authority"]["binding_digest"]
            .as_str()
            .filter(|digest| !digest.is_empty())
            .ok_or_else(|| corrupt("launch binding digest is missing"))?
            .to_owned(),
        service_config,
        actor_manifest: manifest["actor"].clone(),
    })
}

fn make_admission(scope: &OpeningScope, owner_nonce: String) -> Result<StartAdmission> {
    let base_route = OwnedServiceRoute::from_config(&scope.service_config)?;
    let route = base_route.for_launch(&owner_nonce, &scope.workspace_directory)?;
    let route_digest = route.route_digest()?;
    let service_id = route.service_id().to_owned();
    let service_version = route.version().to_owned();
    if service_version != PINNED_SERVICE_VERSION {
        return Err(Error::new(
            "OWNED_SERVICE_VERSION",
            "fresh owned OpenCode version is not pinned",
        ));
    }
    let intent_digest = intent_digest(
        &scope.launch_operation_id,
        &scope.open_operation_id,
        &scope.binding_id,
        scope.binding_generation,
        &scope.task_id,
        scope.task_revision,
        &scope.attempt_id,
        &scope.lease.lease_id,
        scope.lease.generation,
        scope.actor.technical_requester_id(),
        scope.actor.effective_manager_id(),
        &service_id,
        &service_version,
        &route_digest,
        &scope.binding_digest,
        &owner_nonce,
        &scope.actor_manifest,
    )?;
    let intent = OwnedServiceIntent::from_store_admission(OwnedServiceSeed {
        launch_operation_id: scope.launch_operation_id.clone(),
        open_operation_id: scope.open_operation_id.clone(),
        open_operation_state: scope.open_operation_state.clone(),
        actor: scope.actor.clone(),
        task_id: scope.task_id.clone(),
        task_revision: scope.task_revision,
        attempt_id: scope.attempt_id.clone(),
        lease_id: scope.lease.lease_id.clone(),
        lease_state: scope.lease.state.clone(),
        lease_generation: scope.lease.generation,
        binding_id: scope.binding_id.clone(),
        binding_state: "opening".to_owned(),
        binding_generation: scope.binding_generation,
        binding_digest: scope.binding_digest.clone(),
        service_id: service_id.clone(),
        service_version: service_version.clone(),
        route_digest: route_digest.clone(),
        owner_nonce: owner_nonce.clone(),
        origin: OwnedServiceOrigin::FreshOwnedService,
    })?;
    let row = OwnedStartRow {
        launch_operation_id: scope.launch_operation_id.clone(),
        open_operation_id: scope.open_operation_id.clone(),
        binding_id: scope.binding_id.clone(),
        binding_generation: scope.binding_generation,
        task_id: scope.task_id.clone(),
        task_revision: scope.task_revision,
        attempt_id: scope.attempt_id.clone(),
        lease_id: scope.lease.lease_id.clone(),
        lease_generation: scope.lease.generation,
        technical_requester_id: scope.actor.technical_requester_id().to_owned(),
        effective_manager_id: scope.actor.effective_manager_id().to_owned(),
        service_id,
        service_version,
        route_digest,
        binding_digest: scope.binding_digest.clone(),
        owner_nonce,
        intent_digest,
        state: "reserved".to_owned(),
        process_id: None,
        process_birth_token: None,
        executable_sha256: None,
        proof_json: "{}".to_owned(),
        updated_at_ms: 0,
    };
    Ok(StartAdmission {
        actor: scope.actor.clone(),
        route,
        intent,
        row,
    })
}

fn insert_start_reservation(tx: &Transaction<'_>, row: &OwnedStartRow) -> Result<()> {
    tx.execute(
        "INSERT INTO owned_service_starts
            (launch_operation_id,open_operation_id,binding_id,binding_generation,task_id,
             task_revision,attempt_id,lease_id,lease_generation,technical_requester_id,
             effective_manager_id,service_id,service_version,route_digest,binding_digest,
             intent_nonce,intent_digest,state,proof_json,created_at_ms,updated_at_ms)
         VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,'reserved','{}',?18,?18)",
        params![
            row.launch_operation_id,
            row.open_operation_id,
            row.binding_id,
            row.binding_generation,
            row.task_id,
            row.task_revision,
            row.attempt_id,
            row.lease_id,
            row.lease_generation,
            row.technical_requester_id,
            row.effective_manager_id,
            row.service_id,
            row.service_version,
            row.route_digest,
            row.binding_digest,
            row.owner_nonce,
            row.intent_digest,
            model::now_ms()?,
        ],
    )?;
    Ok(())
}

fn verify_start_row(row: &OwnedStartRow, expected: &OwnedStartRow) -> Result<()> {
    if row.launch_operation_id != expected.launch_operation_id
        || row.open_operation_id != expected.open_operation_id
        || row.binding_id != expected.binding_id
        || row.binding_generation != expected.binding_generation
        || row.task_id != expected.task_id
        || row.task_revision != expected.task_revision
        || row.attempt_id != expected.attempt_id
        || row.lease_id != expected.lease_id
        || row.lease_generation != expected.lease_generation
        || row.technical_requester_id != expected.technical_requester_id
        || row.effective_manager_id != expected.effective_manager_id
        || row.service_id != expected.service_id
        || row.service_version != expected.service_version
        || row.route_digest != expected.route_digest
        || row.binding_digest != expected.binding_digest
        || row.owner_nonce != expected.owner_nonce
        || row.intent_digest != expected.intent_digest
    {
        return Err(corrupt(
            "retained owned service reservation differs from its admitted scope",
        ));
    }
    Ok(())
}

fn verify_scope_for_admission(
    scope: &OpeningScope,
    expected_actor: &super::launcher::LaunchActor,
    expected: &OwnedStartRow,
    expected_workspace_directory: &std::path::Path,
) -> Result<()> {
    if scope.launch_operation_id != expected.launch_operation_id
        || scope.open_operation_id != expected.open_operation_id
        || scope.binding_id != expected.binding_id
        || scope.binding_generation != expected.binding_generation
        || scope.task_id != expected.task_id
        || scope.task_revision != expected.task_revision
        || scope.attempt_id != expected.attempt_id
        || scope.lease.lease_id != expected.lease_id
        || scope.lease.generation != expected.lease_generation
        || scope.lease.binding_digest != expected.binding_digest
        || scope.actor.technical_requester_id() != expected.technical_requester_id
        || scope.actor.effective_manager_id() != expected.effective_manager_id
        || scope.workspace_directory != expected_workspace_directory
        || !scope.actor.same_authority_identity(expected_actor)
        || !matches!(scope.open_operation_state.as_str(), "queued")
    {
        return Err(scope_changed());
    }
    let digest = intent_digest(
        &scope.launch_operation_id,
        &scope.open_operation_id,
        &scope.binding_id,
        scope.binding_generation,
        &scope.task_id,
        scope.task_revision,
        &scope.attempt_id,
        &scope.lease.lease_id,
        scope.lease.generation,
        scope.actor.technical_requester_id(),
        scope.actor.effective_manager_id(),
        &expected.service_id,
        &expected.service_version,
        &expected.route_digest,
        &scope.binding_digest,
        &expected.owner_nonce,
        &scope.actor_manifest,
    )?;
    if digest != expected.intent_digest {
        return Err(scope_changed());
    }
    Ok(())
}

fn failed_no_effect_proof(
    row: &OwnedStartRow,
    config_digest: &str,
    safe_code: &str,
    scope_current: bool,
) -> Result<String> {
    if safe_code != "OWNED_SERVICE_PRE_SPAWN_NO_EFFECT" || !is_sha256(config_digest) {
        return Err(corrupt("pre-spawn no-effect proof is malformed"));
    }
    let proof = json!({
        "schema_version":1,
        "status":"failed_no_effect",
        "safe_code":safe_code,
        "owner_nonce":row.owner_nonce,
        "route_digest":row.route_digest,
        "intent_digest":row.intent_digest,
        "config_digest":config_digest,
        "process_spawn_attempted":false,
        "scope_revalidation":if scope_current { "current" } else { "stale_or_unavailable" },
    });
    let canonical = model::canonical(&proof)?;
    if canonical.len() > MAX_PROOF_BYTES {
        return Err(corrupt(
            "pre-spawn no-effect proof exceeds its storage bound",
        ));
    }
    Ok(canonical)
}

fn persist_failed_no_effect(
    db: &mut Connection,
    row: &OwnedStartRow,
    proof_json: &str,
) -> Result<()> {
    if proof_json.len() > MAX_PROOF_BYTES {
        return Err(corrupt(
            "pre-spawn no-effect proof exceeds its storage bound",
        ));
    }
    let proof: Value = serde_json::from_str(proof_json)
        .map_err(|_| corrupt("pre-spawn no-effect proof is invalid JSON"))?;
    let canonical = model::canonical(&proof)?;
    let object = proof
        .as_object()
        .ok_or_else(|| corrupt("pre-spawn no-effect proof is not an object"))?;
    let expected_fields = [
        "schema_version",
        "status",
        "safe_code",
        "owner_nonce",
        "route_digest",
        "intent_digest",
        "config_digest",
        "process_spawn_attempted",
        "scope_revalidation",
    ];
    if canonical != proof_json
        || object.len() != expected_fields.len()
        || expected_fields
            .iter()
            .any(|field| !object.contains_key(*field))
        || proof["schema_version"] != 1
        || proof["status"] != "failed_no_effect"
        || proof["safe_code"] != "OWNED_SERVICE_PRE_SPAWN_NO_EFFECT"
        || proof["owner_nonce"] != row.owner_nonce
        || proof["route_digest"] != row.route_digest
        || proof["intent_digest"] != row.intent_digest
        || !proof["config_digest"].as_str().is_some_and(is_sha256)
        || proof["process_spawn_attempted"] != false
        || !matches!(
            proof["scope_revalidation"].as_str(),
            Some("current" | "stale_or_unavailable")
        )
    {
        return Err(corrupt(
            "pre-spawn no-effect proof differs from its exact launch row",
        ));
    }

    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current =
        load_start_row(&tx, &row.binding_id, row.binding_generation)?.ok_or_else(|| {
            corrupt("owned service reservation disappeared before no-effect retention")
        })?;
    verify_start_row(&current, row)?;
    if current.state != "outcome_unknown"
        || current.proof_json != "{}"
        || current.process_id.is_some()
        || current.process_birth_token.is_some()
        || current.executable_sha256.is_some()
    {
        return Err(Error::conflict(
            "owned service startup no longer has an empty unknown-effect reservation",
        ));
    }
    let changed = tx.execute(
        "UPDATE owned_service_starts
         SET state='failed_no_effect',proof_json=?1,updated_at_ms=?2
         WHERE launch_operation_id=?3 AND binding_id=?4 AND binding_generation=?5
           AND state='outcome_unknown' AND intent_nonce=?6 AND route_digest=?7 AND intent_digest=?8
           AND process_id IS NULL AND process_birth_token IS NULL AND executable_sha256 IS NULL
           AND proof_json='{}'",
        params![
            canonical,
            model::now_ms()?,
            row.launch_operation_id,
            row.binding_id,
            row.binding_generation,
            row.owner_nonce,
            row.route_digest,
            row.intent_digest,
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "owned service no-effect state transition lost its exact reservation CAS",
        ));
    }
    tx.commit()?;
    Ok(())
}

fn persist_observed_proof(
    db: &mut Connection,
    row: &OwnedStartRow,
    route: &OwnedServiceRoute,
    intent: &OwnedServiceIntent,
    proof: &Value,
) -> Result<()> {
    let checked = OwnedServiceReadback::from_store_value(proof, route, intent)?;
    let canonical = model::canonical(proof)?;
    if canonical.len() > MAX_PROOF_BYTES
        || checked.service_id() != row.service_id
        || checked.version() != row.service_version
        || checked.owner_nonce() != row.owner_nonce
        || checked.route_digest() != row.route_digest
        || checked.pid() == 0
        || checked.birth_token().is_empty()
        || !is_sha256(checked.binary_sha256())
    {
        return Err(corrupt(
            "owned service process readback differs from its one-shot intent",
        ));
    }
    let process_id = i64::from(checked.pid());
    let process_birth_token = checked.birth_token().to_owned();
    let executable_sha256 = checked.binary_sha256().to_owned();
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current = load_start_row(&tx, &row.binding_id, row.binding_generation)?
        .ok_or_else(|| corrupt("owned service reservation disappeared after process start"))?;
    if current.state != "outcome_unknown"
        || current.launch_operation_id != row.launch_operation_id
        || current.open_operation_id != row.open_operation_id
        || current.owner_nonce != row.owner_nonce
        || current.route_digest != row.route_digest
        || current.intent_digest != row.intent_digest
    {
        return Err(corrupt(
            "owned service unknown-effect fence changed before proof retention",
        ));
    }
    let changed = tx.execute(
        "UPDATE owned_service_starts SET state='service_observed',process_id=?2,
             process_birth_token=?3,executable_sha256=?4,proof_json=?5,updated_at_ms=?6
         WHERE launch_operation_id=?1 AND binding_id=?7 AND binding_generation=?8
           AND state='outcome_unknown' AND intent_nonce=?9 AND route_digest=?10 AND intent_digest=?11",
        params![
            row.launch_operation_id,
            process_id,
            process_birth_token,
            executable_sha256,
            canonical,
            model::now_ms()?,
            row.binding_id,
            row.binding_generation,
            row.owner_nonce,
            row.route_digest,
            row.intent_digest,
        ],
    )?;
    if changed != 1 {
        return Err(corrupt(
            "owned service process proof was not durably retained",
        ));
    }
    tx.commit()?;
    Ok(())
}

fn retained_scope(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    binding: &BindingRow,
    stored_route: &Route,
    row: &OwnedStartRow,
) -> Result<PathBuf> {
    if binding.released_at_ms.is_some()
        || !matches!(binding.state.as_str(), "opening" | "ready" | "reconciling")
        || binding.native_root_id.is_some() != binding.native_scope_key.is_some()
    {
        return Err(scope_changed());
    }
    let parent = operation_row(db, &row.launch_operation_id)?
        .ok_or_else(|| corrupt("owned service parent launch is missing"))?;
    let open = operation_row(db, &row.open_operation_id)?
        .ok_or_else(|| corrupt("owned service open Operation is missing"))?;
    if parent.method != "swarm.launch"
        || parent.caller_id != row.technical_requester_id
        || parent.task_id.as_deref() != Some(row.task_id.as_str())
        || parent.attempt_id.as_deref() != Some(row.attempt_id.as_str())
        || parent.binding_id.as_deref() != Some(binding_id)
        || parent.binding_generation != Some(generation)
        || open.method != "agent.open"
        || open.caller_id != row.technical_requester_id
        || open.prerequisite_operation_id.as_deref() != Some(row.launch_operation_id.as_str())
        || open.task_id.as_deref() != Some(row.task_id.as_str())
        || open.attempt_id.as_deref() != Some(row.attempt_id.as_str())
        || open.binding_id.as_deref() != Some(binding_id)
        || open.binding_generation != Some(generation)
        || !matches!(
            open.state.as_str(),
            "queued" | "sending" | "native_accepted" | "settled" | "outcome_unknown"
        )
    {
        return Err(corrupt(
            "owned service launch/open linkage does not match its retained startup row",
        ));
    }
    let manifest_value: Value = serde_json::from_str(&parent.effective_request_json)
        .map_err(|_| corrupt("owned service launch manifest is invalid"))?;
    let manifest = manifest_value
        .get("launch_manifest")
        .ok_or_else(|| corrupt("owned service launch manifest is missing"))?;
    let task_revision = manifest["task"]["observed_revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| corrupt("owned service retained Task revision is invalid"))?;
    if task_revision != row.task_revision
        || manifest["task"]["task_id"] != row.task_id
        || manifest["task"]["attempt_id"] != row.attempt_id
        || manifest["binding"]["operation_id"] != row.open_operation_id
        || manifest["binding"]["binding_id"] != binding_id
        || manifest["binding"]["generation"] != generation
        || manifest["runtime"]["route"] != stored_route.alias
    {
        return Err(corrupt(
            "owned service parent manifest differs from its startup row",
        ));
    }
    validate_actor_link(db, &parent, manifest, row)?;
    let child_original: Value = serde_json::from_str(&open.original_request_json)
        .map_err(|_| corrupt("owned service child request is invalid"))?;
    let expected_child = json!({
        "client_request_id":format!("launch:{}:open", row.launch_operation_id),
        "lane_id":format!("launch-{}", row.lease_id),
        "route":stored_route.alias,
    });
    if model::canonical(&child_original)? != model::canonical(&expected_child)? {
        return Err(corrupt(
            "owned service open child request differs from its parent",
        ));
    }
    let child_effective: Value = serde_json::from_str(&open.effective_request_json)
        .map_err(|_| corrupt("owned service child linkage is invalid"))?;
    if child_effective["operation_contract"]["parent_launch_operation_id"]
        != row.launch_operation_id
        || child_effective["route"]["alias"] != stored_route.alias
        || child_effective["workspace_lease"]["lease_id"] != row.lease_id
        || child_effective["workspace_lease"]["generation"] != row.lease_generation
        || child_effective["workspace_lease"]["binding_digest"] != row.binding_digest
        || child_effective["receipt"]["value"]["operation_id"] != row.open_operation_id
        || child_effective["receipt"]["value"]["binding_id"] != binding_id
        || child_effective["receipt"]["value"]["generation"] != generation
    {
        return Err(corrupt(
            "owned service open child receipt differs from its startup row",
        ));
    }
    let lease: LeaseAuthorityRef =
        serde_json::from_value(manifest["workspace"]["lease_authority"].clone())
            .map_err(|_| corrupt("owned service held-lease authority is invalid"))?;
    if lease.lease_id != row.lease_id
        || lease.generation != row.lease_generation
        || lease.binding_digest != row.binding_digest
        || lease.task_id != row.task_id
        || lease.task_revision != row.task_revision
        || lease.attempt_id.as_deref() != Some(row.attempt_id.as_str())
        || lease.owner_client_id != row.effective_manager_id
        || lease.operation_id != row.launch_operation_id
        || lease.state != "held"
    {
        return Err(corrupt(
            "owned service lease identity differs from its startup row",
        ));
    }
    let current_lease = workspace::held_lease_for_operation(db, &row.launch_operation_id)?
        .ok_or_else(|| {
            Error::new(
                "WORKSPACE_LEASE_STALE",
                "owned service lease is no longer held",
            )
        })?;
    if current_lease != lease {
        return Err(scope_changed());
    }
    workspace::get_lease_view(db, &lease)?;
    let workspace_directory = held_workspace_path(db, &lease)?;
    let task: Option<(i64, String, Option<String>)> = db
        .query_row(
            "SELECT revision,state,(SELECT attempt_id FROM attempts WHERE task_id=tasks.task_id
                AND released_at_ms IS NULL ORDER BY attempt_id LIMIT 1)
             FROM tasks WHERE task_id=?1",
            [&row.task_id],
            |record| Ok((record.get(0)?, record.get(1)?, record.get(2)?)),
        )
        .optional()?;
    let Some((current_revision, task_state, current_attempt)) = task else {
        return Err(scope_changed());
    };
    if current_revision != row.task_revision
        || task_state != "open"
        || current_attempt.as_deref() != Some(row.attempt_id.as_str())
    {
        return Err(scope_changed());
    }
    let attempt_matches: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM attempts WHERE attempt_id=?1 AND task_id=?2
           AND task_revision=?3 AND owner_id=?4 AND released_at_ms IS NULL
           AND binding_id=?5 AND binding_generation=?6)",
        params![
            row.attempt_id,
            row.task_id,
            row.task_revision,
            row.effective_manager_id,
            binding_id,
            generation,
        ],
        |record| record.get(0),
    )?;
    if !attempt_matches {
        return Err(scope_changed());
    }
    Ok(workspace_directory)
}

fn validate_actor_link(
    db: &Connection,
    parent: &OperationRow,
    manifest: &Value,
    row: &OwnedStartRow,
) -> Result<()> {
    if manifest["actor"]["client_id"] != row.technical_requester_id
        || manifest["actor"]["effective_manager_id"] != row.effective_manager_id
    {
        return Err(corrupt(
            "owned service actor attribution differs from its parent manifest",
        ));
    }
    match manifest["actor"]["kind"].as_str() {
        Some("direct") => {
            if row.technical_requester_id != row.effective_manager_id
                || parent.caller_id != row.technical_requester_id
                || !matches!(
                    manifest["actor"]["role"].as_str(),
                    Some("manager" | "operator")
                )
                || manifest["actor"]["link_id"]
                    .as_str()
                    .is_none_or(str::is_empty)
            {
                return Err(corrupt("direct owned service actor attribution is invalid"));
            }
        }
        Some("work_dispatch") => {
            let link =
                super::automation_work_dispatch::operation_link(db, &row.launch_operation_id)?
                    .ok_or_else(|| corrupt("owned service WorkDispatch link is missing"))?;
            if link.operation_id != row.launch_operation_id
                || link.technical_requester_id != row.technical_requester_id
                || link.effective_manager_id != row.effective_manager_id
                || link.task_id != row.task_id
                || link.task_revision != row.task_revision
                || link.project_id != manifest["task"]["project_id"]
                || link
                    .attempt_id
                    .as_deref()
                    .is_some_and(|attempt_id| attempt_id != row.attempt_id)
                || (link.attempt_id.is_none() && manifest["task"]["attempt_action"] != "claim_new")
                || link.action != "swarm.launch"
                || manifest["actor"]["automation_id"] != link.automation_id
                || manifest["actor"]["automation_revision"] != link.automation_revision
                || manifest["actor"]["semantic_slot_id"] != link.semantic_slot_id
            {
                return Err(corrupt(
                    "owned service WorkDispatch attribution is not exact",
                ));
            }
        }
        _ => return Err(corrupt("owned service actor kind is invalid")),
    }
    Ok(())
}

fn verify_readback_value(
    proof: &Value,
    route: &OwnedServiceRoute,
    row: &OwnedStartRow,
) -> Result<VerifiedOwnedServiceBinding> {
    validate_readback_proof_fields(proof, row, route)?;
    let canonical = model::canonical(proof)?;
    if route.service_id() != row.service_id
        || route.version() != row.service_version
        || route.route_digest()? != row.route_digest
    {
        return Err(corrupt(
            "owned service readback route differs from its pinned configuration",
        ));
    }
    let process = &proof["process"];
    let process_id = process["pid"].as_u64().unwrap_or_default() as u32;
    let process_birth_token = process["birth_token"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let executable_sha256 = process["binary_sha256"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let service_id = proof["service_id"].as_str().unwrap_or_default().to_owned();
    let service_version = proof["service_version"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let owner_nonce = proof["owner_nonce"].as_str().unwrap_or_default().to_owned();
    let endpoint_digest = proof["endpoint_digest"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let connection_digest = proof["connection_digest"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let config_digest = proof["config_digest"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let plugin_module_sha256 = proof["plugin_module_sha256"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let proof_digest = model::digest(canonical.as_bytes());
    let options = route.options();
    let options_digest =
        model::digest(model::canonical(&serde_json::to_value(&options)?)?.as_bytes());
    let identity_digest = model::digest(
        model::canonical(&json!({
            "service_id":service_id,
            "service_version":service_version,
            "owner_nonce":owner_nonce,
            "process_id":process_id,
            "process_birth_token":process_birth_token,
            "executable_sha256":executable_sha256,
            "endpoint_digest":endpoint_digest,
            "connection_digest":connection_digest,
            "config_digest":config_digest,
            "plugin_module_sha256":plugin_module_sha256,
            "options_digest":options_digest,
        }))?
        .as_bytes(),
    );
    Ok(VerifiedOwnedServiceBinding {
        route: route.clone(),
        service_id,
        service_version,
        owner_nonce,
        process_id,
        process_birth_token,
        executable_sha256,
        endpoint_digest,
        connection_digest,
        config_digest,
        plugin_module_sha256,
        proof_digest,
        identity_digest: format!("sha256:{identity_digest}"),
    })
}

fn validate_readback_proof_fields(
    proof: &Value,
    row: &OwnedStartRow,
    route: &OwnedServiceRoute,
) -> Result<()> {
    // Runtime validation also binds an optional provider proof to this exact
    // route, model and process. Its presence is mandatory only for auth routes.
    OwnedServiceReadback::from_retained_value(proof, route)?;
    let canonical = model::canonical(proof)?;
    let fields = proof
        .as_object()
        .ok_or_else(|| corrupt("owned service proof is not an object"))?;
    let expected_fields = [
        "schema_version",
        "status",
        "service_id",
        "service_version",
        "owner_nonce",
        "route_digest",
        "process",
        "endpoint_digest",
        "connection_digest",
        "config_digest",
        "plugin_module_sha256",
        "plugin_entrypoint_sha256",
        "server_program_sha256",
        "bun_sha256",
        "readiness_observed",
        "plugin_loaded",
        "dispatch_permitted",
    ];
    if canonical.len() > MAX_PROOF_BYTES
        || fields.len() != expected_fields.len() + usize::from(fields.contains_key("provider_auth"))
        || fields
            .keys()
            .any(|key| !expected_fields.contains(&key.as_str()) && key != "provider_auth")
        || expected_fields.iter().any(|key| !fields.contains_key(*key))
        || proof["schema_version"] != 1
        || proof["status"] != "ready"
        || proof["service_id"] != row.service_id
        || proof["service_version"] != row.service_version
        || proof["owner_nonce"] != row.owner_nonce
        || proof["route_digest"] != row.route_digest
        || proof["readiness_observed"] != true
        || proof["plugin_loaded"] != "unknown"
        || proof["dispatch_permitted"] != false
        || !is_sha256_value(&proof["endpoint_digest"])
        || !is_sha256_value(&proof["connection_digest"])
        || !is_sha256_value(&proof["config_digest"])
        || !is_sha256_value(&proof["plugin_module_sha256"])
        || !is_sha256_value(&proof["plugin_entrypoint_sha256"])
        || !is_sha256_value(&proof["server_program_sha256"])
        || !is_sha256_value(&proof["bun_sha256"])
        || proof["bun_sha256"] != route.bun_sha256()
        || proof["server_program_sha256"] != route.server_program_sha256()
    {
        return Err(corrupt(
            "owned service readback does not match its pinned route and schema",
        ));
    }
    let process = proof["process"]
        .as_object()
        .ok_or_else(|| corrupt("owned service process proof is invalid"))?;
    if process.len() != 3
        || process
            .get("pid")
            .and_then(Value::as_u64)
            .is_none_or(|pid| pid == 0 || pid > u32::MAX as u64)
        || process
            .get("birth_token")
            .and_then(Value::as_str)
            .is_none_or(|token| !is_sha256(token))
        || process
            .get("binary_sha256")
            .and_then(Value::as_str)
            .is_none_or(|digest| !is_sha256(digest))
    {
        return Err(corrupt("owned service process identity proof is invalid"));
    }
    let process_id = process["pid"].as_u64().unwrap_or_default() as u32;
    let process_birth_token = process["birth_token"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let executable_sha256 = process["binary_sha256"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    if row.process_id != Some(i64::from(process_id))
        || row.process_birth_token.as_deref() != Some(process_birth_token.as_str())
        || row.executable_sha256.as_deref() != Some(executable_sha256.as_str())
    {
        return Err(corrupt(
            "owned service proof differs from retained process identity columns",
        ));
    }
    Ok(())
}

fn load_start_row(
    db: &Connection,
    binding_id: &str,
    generation: i64,
) -> Result<Option<OwnedStartRow>> {
    db.query_row(
        "SELECT launch_operation_id,open_operation_id,binding_id,binding_generation,task_id,
             task_revision,attempt_id,lease_id,lease_generation,technical_requester_id,
             effective_manager_id,service_id,service_version,route_digest,binding_digest,
             intent_nonce,intent_digest,state,process_id,process_birth_token,executable_sha256,proof_json,
             updated_at_ms
         FROM owned_service_starts WHERE binding_id=?1 AND binding_generation=?2",
        params![binding_id, generation],
        |row| {
            Ok(OwnedStartRow {
                launch_operation_id: row.get(0)?,
                open_operation_id: row.get(1)?,
                binding_id: row.get(2)?,
                binding_generation: row.get(3)?,
                task_id: row.get(4)?,
                task_revision: row.get(5)?,
                attempt_id: row.get(6)?,
                lease_id: row.get(7)?,
                lease_generation: row.get(8)?,
                technical_requester_id: row.get(9)?,
                effective_manager_id: row.get(10)?,
                service_id: row.get(11)?,
                service_version: row.get(12)?,
                route_digest: row.get(13)?,
                binding_digest: row.get(14)?,
                owner_nonce: row.get(15)?,
                intent_digest: row.get(16)?,
                state: row.get(17)?,
                process_id: row.get(18)?,
                process_birth_token: row.get(19)?,
                executable_sha256: row.get(20)?,
                proof_json: row.get(21)?,
                updated_at_ms: row.get(22)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn departure_batch(db: &Connection) -> Result<(Vec<DepartureCandidate>, i64)> {
    let maximum_updated_at: i64 = db.query_row(
        "SELECT COALESCE(MAX(updated_at_ms),0) FROM owned_service_starts
         WHERE state IN ('outcome_unknown','service_observed')",
        [],
        |row| row.get(0),
    )?;
    let cursor_base = maximum_updated_at
        .checked_add(1)
        .ok_or_else(|| corrupt("owned service departure cursor overflow"))?;
    let ids = {
        let mut statement = db.prepare(
            "SELECT binding_id,binding_generation FROM owned_service_starts
             WHERE state IN ('outcome_unknown','service_observed')
             ORDER BY updated_at_ms,launch_operation_id LIMIT 32",
        )?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let mut candidates = Vec::with_capacity(ids.len());
    for (binding_id, generation) in ids {
        let Some(row) = load_start_row(db, &binding_id, generation)? else {
            continue;
        };
        let source = departure_source(db, &row).ok();
        let (service_config, workspace_directory) = source
            .map(|(config, path)| (Some(config), Some(path)))
            .unwrap_or((None, None));
        candidates.push(DepartureCandidate {
            row,
            service_config,
            workspace_directory,
        });
    }
    Ok((candidates, cursor_base))
}

/// Reconstruct the immutable launch route and workspace path for cleanup.
/// This intentionally reads historical provenance only: current manager,
/// automation, Attempt, registration, and lease state cannot erase cleanup
/// authority for a process that was already admitted.
fn departure_source(
    db: &Connection,
    row: &OwnedStartRow,
) -> Result<(OwnedOpenCodeServiceConfig, PathBuf)> {
    if !matches!(row.state.as_str(), "outcome_unknown" | "service_observed") {
        return Err(scope_changed());
    }
    let binding = binding_row(db, &row.binding_id, row.binding_generation)?;
    let stored_route = parse_route(&binding.route_json)?;
    let service_config = stored_route
        .owned_service
        .clone()
        .ok_or_else(|| corrupt("departure binding has no explicit owned service route"))?;
    if binding.lane_id != format!("launch-{}", row.lease_id)
        || binding.module_artifact_id != crate::runtime::opencode_v2::ARTIFACT_ID
    {
        return Err(corrupt(
            "departure binding is outside the retained launch scope",
        ));
    }

    let parent = operation_row(db, &row.launch_operation_id)?
        .ok_or_else(|| corrupt("departure launch Operation is missing"))?;
    let open = operation_row(db, &row.open_operation_id)?
        .ok_or_else(|| corrupt("departure open Operation is missing"))?;
    if parent.method != "swarm.launch"
        || parent.caller_id != row.technical_requester_id
        || parent.task_id.as_deref() != Some(row.task_id.as_str())
        || parent.attempt_id.as_deref() != Some(row.attempt_id.as_str())
        || parent.binding_id.as_deref() != Some(row.binding_id.as_str())
        || parent.binding_generation != Some(row.binding_generation)
        || open.method != "agent.open"
        || open.caller_id != row.technical_requester_id
        || open.prerequisite_operation_id.as_deref() != Some(row.launch_operation_id.as_str())
        || open.task_id.as_deref() != Some(row.task_id.as_str())
        || open.attempt_id.as_deref() != Some(row.attempt_id.as_str())
        || open.binding_id.as_deref() != Some(row.binding_id.as_str())
        || open.binding_generation != Some(row.binding_generation)
    {
        return Err(corrupt("departure Operation linkage is not exact"));
    }
    let parent_request: Value = serde_json::from_str(&parent.effective_request_json)
        .map_err(|_| corrupt("departure launch manifest is invalid"))?;
    let manifest = parent_request
        .get("launch_manifest")
        .ok_or_else(|| corrupt("departure launch manifest is missing"))?;
    let task_revision = manifest["task"]["observed_revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| corrupt("departure Task revision is missing"))?;
    if task_revision != row.task_revision
        || manifest["task"]["task_id"] != row.task_id
        || manifest["task"]["attempt_id"] != row.attempt_id
        || manifest["binding"]["operation_id"] != row.open_operation_id
        || manifest["binding"]["binding_id"] != row.binding_id
        || manifest["binding"]["generation"] != row.binding_generation
        || manifest["runtime"]["route"] != stored_route.alias
        || manifest["workspace"]["lease_authority"]["lease_id"] != row.lease_id
        || binding.lane_id != format!("launch-{}", row.lease_id)
    {
        return Err(corrupt(
            "departure launch manifest differs from its retained row",
        ));
    }
    validate_actor_link(db, &parent, manifest, row)?;

    let expected_child = json!({
        "client_request_id":format!("launch:{}:open", row.launch_operation_id),
        "lane_id":format!("launch-{}", row.lease_id),
        "route":stored_route.alias,
    });
    let child_original: Value = serde_json::from_str(&open.original_request_json)
        .map_err(|_| corrupt("departure open request is invalid"))?;
    let child_effective: Value = serde_json::from_str(&open.effective_request_json)
        .map_err(|_| corrupt("departure open receipt is invalid"))?;
    if model::canonical(&child_original)? != model::canonical(&expected_child)?
        || child_effective["operation_contract"]["parent_launch_operation_id"]
            != row.launch_operation_id
        || child_effective["route"]["alias"] != stored_route.alias
        || child_effective["workspace_lease"]["lease_id"] != row.lease_id
        || child_effective["workspace_lease"]["generation"] != row.lease_generation
        || child_effective["workspace_lease"]["binding_digest"] != row.binding_digest
        || child_effective["receipt"]["value"]["operation_id"] != row.open_operation_id
        || child_effective["receipt"]["value"]["binding_id"] != row.binding_id
        || child_effective["receipt"]["value"]["generation"] != row.binding_generation
    {
        return Err(corrupt("departure child receipt differs from its launch"));
    }

    let lease: LeaseAuthorityRef =
        serde_json::from_value(manifest["workspace"]["lease_authority"].clone())
            .map_err(|_| corrupt("departure historical lease authority is invalid"))?;
    if lease.lease_id != row.lease_id
        || lease.generation != row.lease_generation
        || lease.binding_digest != row.binding_digest
        || lease.task_id != row.task_id
        || lease.task_revision != row.task_revision
        || lease.attempt_id.as_deref() != Some(row.attempt_id.as_str())
        || lease.owner_client_id != row.effective_manager_id
        || lease.operation_id != row.launch_operation_id
        || lease.state != "held"
    {
        return Err(corrupt(
            "departure historical lease differs from its retained row",
        ));
    }
    let attempt: Option<DepartureAttemptRow> = db
        .query_row(
            "SELECT task_id,task_revision,owner_id,binding_id,binding_generation
             FROM attempts WHERE attempt_id=?1",
            [&row.attempt_id],
            |record| {
                Ok((
                    record.get(0)?,
                    record.get(1)?,
                    record.get(2)?,
                    record.get(3)?,
                    record.get(4)?,
                ))
            },
        )
        .optional()?;
    let Some((attempt_task, attempt_revision, attempt_owner, attempt_binding, attempt_generation)) =
        attempt
    else {
        return Err(corrupt("departure Attempt provenance is missing"));
    };
    if attempt_task != row.task_id
        || attempt_revision != row.task_revision
        || attempt_owner != row.effective_manager_id
        || attempt_binding.as_deref() != Some(row.binding_id.as_str())
        || attempt_generation != Some(row.binding_generation)
    {
        return Err(corrupt(
            "departure Attempt differs from its retained launch provenance",
        ));
    }
    let workspace_directory = historical_workspace_path(db, &lease)?;
    let recomputed_intent = intent_digest(
        &row.launch_operation_id,
        &row.open_operation_id,
        &row.binding_id,
        row.binding_generation,
        &row.task_id,
        row.task_revision,
        &row.attempt_id,
        &row.lease_id,
        row.lease_generation,
        &row.technical_requester_id,
        &row.effective_manager_id,
        &row.service_id,
        &row.service_version,
        &row.route_digest,
        &row.binding_digest,
        &row.owner_nonce,
        &manifest["actor"],
    )?;
    if recomputed_intent != row.intent_digest {
        return Err(corrupt(
            "departure intent digest does not match immutable provenance",
        ));
    }
    Ok((service_config, workspace_directory))
}

fn historical_workspace_path(db: &Connection, lease: &LeaseAuthorityRef) -> Result<PathBuf> {
    let row: Option<HistoricalLeaseRow> = db
        .query_row(
            "SELECT workspace_path,registration_id,registration_generation,project_id,task_revision,
                    operation_id,owner_client_id,attempt_id,generation,baseline_commit,branch_ref,
                    worktree_handle,binding_digest
             FROM workspace_leases WHERE lease_id=?1 AND task_id=?2",
            params![lease.lease_id, lease.task_id],
            |record| {
                Ok((
                    record.get(0)?, record.get(1)?, record.get(2)?, record.get(3)?, record.get(4)?,
                    record.get(5)?, record.get(6)?, record.get(7)?, record.get(8)?, record.get(9)?,
                    record.get(10)?, record.get(11)?, record.get(12)?,
                ))
            },
        )
        .optional()?;
    let Some((
        path,
        registration_id,
        registration_generation,
        project_id,
        task_revision,
        operation_id,
        owner_client_id,
        attempt_id,
        generation,
        baseline_commit,
        branch_ref,
        worktree_handle,
        binding_digest,
    )) = row
    else {
        return Err(scope_changed());
    };
    if registration_id != lease.registration_id
        || registration_generation != lease.registration_generation
        || project_id != lease.project_id
        || task_revision != lease.task_revision
        || operation_id != lease.operation_id
        || owner_client_id != lease.owner_client_id
        || attempt_id != lease.attempt_id
        || generation != lease.generation
        || baseline_commit != lease.baseline_commit
        || branch_ref != lease.branch_ref
        || worktree_handle != lease.worktree_handle
        || binding_digest != lease.binding_digest
    {
        return Err(corrupt(
            "historical workspace lease identity differs from launch provenance",
        ));
    }
    let path = PathBuf::from(path);
    if !path.is_absolute() || path.to_str().is_none() {
        return Err(scope_changed());
    }
    Ok(path)
}

fn validate_departure_proof(
    proof: &Value,
    route: &OwnedServiceRoute,
    row: &OwnedStartRow,
    previous_proof: &Value,
) -> Result<(i64, String, String)> {
    let canonical = model::canonical(proof)?;
    let object = proof
        .as_object()
        .ok_or_else(|| corrupt("owned service departure proof is not an object"))?;
    let expected_fields = [
        "schema_version",
        "status",
        "service_id",
        "service_version",
        "owner_nonce",
        "route_digest",
        "process",
        "owner_receipt_sha256",
        "stop_receipts_sha256",
        "listener_closed",
        "runtime_disposed",
        "connection_absent",
        "process_absent",
    ];
    if canonical.len() > MAX_PROOF_BYTES
        || object.len() != expected_fields.len()
        || expected_fields
            .iter()
            .any(|field| !object.contains_key(*field))
        || proof["schema_version"] != 1
        || proof["status"] != "departed"
        || proof["service_id"] != row.service_id
        || proof["service_version"] != row.service_version
        || proof["owner_nonce"] != row.owner_nonce
        || proof["route_digest"] != row.route_digest
        || proof["listener_closed"] != true
        || proof["runtime_disposed"] != true
        || proof["connection_absent"] != true
        || proof["process_absent"] != true
        || !is_sha256_value(&proof["owner_receipt_sha256"])
        || !is_sha256_value(&proof["stop_receipts_sha256"])
        || route.service_id() != row.service_id
        || route.version() != row.service_version
        || route.route_digest()? != row.route_digest
    {
        return Err(corrupt(
            "owned service departure proof differs from its exact route",
        ));
    }
    let process = proof["process"]
        .as_object()
        .ok_or_else(|| corrupt("owned service departure process proof is missing"))?;
    if process.len() != 3
        || process
            .keys()
            .any(|key| !["pid", "birth_token", "binary_sha256"].contains(&key.as_str()))
        || process
            .get("pid")
            .and_then(Value::as_u64)
            .is_none_or(|pid| pid == 0 || pid > u32::MAX as u64)
        || process
            .get("birth_token")
            .and_then(Value::as_str)
            .is_none_or(|token| !is_sha256(token))
        || process.get("binary_sha256").and_then(Value::as_str) != Some(route.bun_sha256())
    {
        return Err(corrupt(
            "owned service departure process identity is malformed",
        ));
    }
    let process_id = i64::from(process["pid"].as_u64().unwrap_or_default() as u32);
    let birth_token = process["birth_token"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let binary_sha256 = process["binary_sha256"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    if row.state == "service_observed" {
        verify_readback_value(previous_proof, route, row)?;
        if previous_proof["process"]["pid"] != process["pid"]
            || previous_proof["process"]["birth_token"] != process["birth_token"]
            || previous_proof["process"]["binary_sha256"] != process["binary_sha256"]
        {
            return Err(corrupt(
                "departure evidence refers to a different process incarnation",
            ));
        }
    } else if row.state == "outcome_unknown" {
        if !previous_proof
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
            || row.process_id.is_some()
            || row.process_birth_token.is_some()
            || row.executable_sha256.is_some()
        {
            return Err(corrupt(
                "unknown owned service row contains partial process evidence",
            ));
        }
    } else {
        return Err(recovery_required(&row.state));
    }
    if row.process_id.is_some_and(|pid| pid != process_id)
        || row
            .process_birth_token
            .as_deref()
            .is_some_and(|token| token != birth_token)
        || row
            .executable_sha256
            .as_deref()
            .is_some_and(|digest| digest != binary_sha256)
    {
        return Err(corrupt(
            "departure process identity differs from retained columns",
        ));
    }
    Ok((process_id, birth_token, binary_sha256))
}

// Keep each exact prior-row and observed-process CAS input explicit.
#[allow(clippy::too_many_arguments)]
fn persist_departure(
    db: &mut Connection,
    row: &OwnedStartRow,
    previous_proof_json: &str,
    departure_proof_json: &str,
    process_id: i64,
    birth_token: &str,
    binary_sha256: &str,
    cursor_ms: i64,
) -> Result<bool> {
    if departure_proof_json.len() > MAX_PROOF_BYTES {
        return Err(corrupt(
            "owned service departure envelope exceeds its storage bound",
        ));
    }
    let changed = db.execute(
        "UPDATE owned_service_starts
         SET state='service_departed',process_id=?1,process_birth_token=?2,
             executable_sha256=?3,proof_json=?4,updated_at_ms=?5
         WHERE launch_operation_id=?6 AND binding_id=?7 AND binding_generation=?8
           AND state=?9 AND intent_nonce=?10 AND route_digest=?11 AND intent_digest=?12
           AND proof_json=?13 AND updated_at_ms=?14",
        params![
            process_id,
            birth_token,
            binary_sha256,
            departure_proof_json,
            cursor_ms,
            row.launch_operation_id,
            row.binding_id,
            row.binding_generation,
            row.state,
            row.owner_nonce,
            row.route_digest,
            row.intent_digest,
            previous_proof_json,
            row.updated_at_ms,
        ],
    )?;
    Ok(changed == 1)
}

fn touch_departure_candidate(
    db: &mut Connection,
    row: &OwnedStartRow,
    proof_json: &str,
    cursor_ms: i64,
) -> Result<()> {
    db.execute(
        "UPDATE owned_service_starts SET updated_at_ms=?1
         WHERE launch_operation_id=?2 AND binding_id=?3 AND binding_generation=?4
           AND state=?5 AND intent_nonce=?6 AND route_digest=?7 AND intent_digest=?8
           AND proof_json=?9 AND updated_at_ms=?10",
        params![
            cursor_ms,
            row.launch_operation_id,
            row.binding_id,
            row.binding_generation,
            row.state,
            row.owner_nonce,
            row.route_digest,
            row.intent_digest,
            proof_json,
            row.updated_at_ms,
        ],
    )?;
    Ok(())
}

fn held_workspace_path(db: &Connection, lease: &LeaseAuthorityRef) -> Result<PathBuf> {
    let raw: Option<String> = db
        .query_row(
            "SELECT workspace_path FROM workspace_leases WHERE lease_id=?1 AND generation=?2
               AND state='held' AND registration_generation=?3 AND task_id=?4 AND task_revision=?5
               AND operation_id=?6 AND owner_client_id=?7 AND attempt_id=?8 AND binding_digest=?9",
            params![
                lease.lease_id,
                lease.generation,
                lease.registration_generation,
                lease.task_id,
                lease.task_revision,
                lease.operation_id,
                lease.owner_client_id,
                lease.attempt_id,
                lease.binding_digest,
            ],
            |row| row.get(0),
        )
        .optional()?;
    let path = PathBuf::from(raw.ok_or_else(scope_changed)?);
    if !path.is_absolute() || path.to_str().is_none() {
        return Err(scope_changed());
    }
    Ok(path)
}

struct BindingRow {
    lane_id: String,
    module_artifact_id: String,
    state: String,
    native_scope_key: Option<String>,
    native_root_id: Option<String>,
    route_json: String,
    released_at_ms: Option<i64>,
}

fn binding_row(db: &Connection, binding_id: &str, generation: i64) -> Result<BindingRow> {
    db.query_row(
        "SELECT lane_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,released_at_ms
         FROM bindings WHERE binding_id=?1 AND generation=?2",
        params![binding_id, generation],
        |row| {
            Ok(BindingRow {
                lane_id: row.get(0)?,
                module_artifact_id: row.get(1)?,
                state: row.get(2)?,
                native_scope_key: row.get(3)?,
                native_root_id: row.get(4)?,
                route_json: row.get(5)?,
                released_at_ms: row.get(6)?,
            })
        },
    )
    .optional()?
    .ok_or_else(|| Error::new("BINDING_CLOSED", "owned service binding is missing"))
}

fn parse_route(raw: &str) -> Result<Route> {
    serde_json::from_str(raw).map_err(|_| corrupt("binding route configuration is invalid"))
}

fn current_route(config: &Config, stored: &Route) -> Result<Route> {
    let current = config.route(&stored.alias)?;
    if current.runtime != crate::runtime::opencode_v2::RUNTIME
        || current.module_artifact_id != crate::runtime::opencode_v2::ARTIFACT_ID
        || model::canonical(&serde_json::to_value(&current)?)?
            != model::canonical(&serde_json::to_value(stored)?)?
    {
        return Err(scope_changed());
    }
    Ok(current)
}

fn current_owned_route(
    config: &Config,
    stored: &Route,
    expected_workspace_directory: &std::path::Path,
) -> Result<Route> {
    let mut current = config.route(&stored.alias)?;
    if current.runtime != crate::runtime::opencode_v2::RUNTIME
        || current.module_artifact_id != crate::runtime::opencode_v2::ARTIFACT_ID
        || current.owned_service.is_none()
    {
        return Err(scope_changed());
    }
    let workspace_directory = expected_workspace_directory
        .to_str()
        .filter(|path| !path.is_empty())
        .ok_or_else(scope_changed)?;
    if stored
        .native_options
        .get("directory")
        .and_then(Value::as_str)
        != Some(workspace_directory)
    {
        return Err(scope_changed());
    }
    let native_options = current
        .native_options
        .as_object_mut()
        .ok_or_else(scope_changed)?;
    native_options.insert(
        "directory".to_owned(),
        Value::String(workspace_directory.to_owned()),
    );
    if model::canonical(&serde_json::to_value(&current)?)?
        != model::canonical(&serde_json::to_value(stored)?)?
    {
        return Err(scope_changed());
    }
    Ok(current)
}

fn launch_rows_for_binding(
    db: &Connection,
    binding_id: &str,
    generation: i64,
) -> Result<Vec<OperationRow>> {
    let mut statement = db.prepare(
        "SELECT operation_id,caller_id,method,state,task_id,attempt_id,binding_id,binding_generation,
                prerequisite_operation_id,original_request_json,effective_request_json
         FROM operations WHERE method='swarm.launch' AND binding_id=?1 AND binding_generation=?2
         ORDER BY created_at_ms,operation_id LIMIT 2",
    )?;
    statement
        .query_map(params![binding_id, generation], operation_from_row)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(Into::into)
}

struct OperationRow {
    operation_id: String,
    caller_id: String,
    method: String,
    state: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    prerequisite_operation_id: Option<String>,
    original_request_json: String,
    effective_request_json: String,
}

fn operation_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OperationRow> {
    Ok(OperationRow {
        operation_id: row.get(0)?,
        caller_id: row.get(1)?,
        method: row.get(2)?,
        state: row.get(3)?,
        task_id: row.get(4)?,
        attempt_id: row.get(5)?,
        binding_id: row.get(6)?,
        binding_generation: row.get(7)?,
        prerequisite_operation_id: row.get(8)?,
        original_request_json: row.get(9)?,
        effective_request_json: row.get(10)?,
    })
}

fn operation_row(db: &Connection, operation_id: &str) -> Result<Option<OperationRow>> {
    db.query_row(
        "SELECT operation_id,caller_id,method,state,task_id,attempt_id,binding_id,binding_generation,
                prerequisite_operation_id,original_request_json,effective_request_json
         FROM operations WHERE operation_id=?1",
        [operation_id],
        operation_from_row,
    )
    .optional()
    .map_err(Into::into)
}

// These fields are the canonical immutable authority tuple, in one place.
#[allow(clippy::too_many_arguments)]
fn intent_digest(
    launch_operation_id: &str,
    open_operation_id: &str,
    binding_id: &str,
    binding_generation: i64,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    lease_id: &str,
    lease_generation: i64,
    technical_requester_id: &str,
    effective_manager_id: &str,
    service_id: &str,
    service_version: &str,
    route_digest: &str,
    binding_digest: &str,
    owner_nonce: &str,
    actor: &Value,
) -> Result<String> {
    let value = json!({
        "origin":"fresh_owned_service",
        "launch_operation_id":launch_operation_id,
        "open_operation_id":open_operation_id,
        "binding":{"binding_id":binding_id,"generation":binding_generation,"digest":binding_digest},
        "task":{"task_id":task_id,"revision":task_revision,"attempt_id":attempt_id},
        "workspace_lease":{"lease_id":lease_id,"generation":lease_generation,"state":"held"},
        "actor":{"technical_requester_id":technical_requester_id,"effective_manager_id":effective_manager_id,"manifest":actor},
        "service":{"id":service_id,"version":service_version,"route_digest":route_digest,"owner_nonce":owner_nonce},
    });
    Ok(model::digest(model::canonical(&value)?.as_bytes()))
}

fn recovery_required(state: &str) -> Error {
    Error::new(
        "OWNED_SERVICE_RECOVERY_REQUIRED",
        format!("owned service state {state} is retained; automatic spawn replay is forbidden"),
    )
}

fn shutdown_error() -> Error {
    Error::new(
        "STORE_SHUTTING_DOWN",
        "owned service start was not admitted during shutdown",
    )
}

fn scope_changed() -> Error {
    Error::new(
        "OWNED_SERVICE_SCOPE_STALE",
        "owned service launch scope is no longer current",
    )
}

fn corrupt(message: &str) -> Error {
    Error::new("OWNED_SERVICE_RECEIPT_CORRUPT", message)
}

fn is_sha256_value(value: &Value) -> bool {
    value.as_str().is_some_and(is_sha256)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
