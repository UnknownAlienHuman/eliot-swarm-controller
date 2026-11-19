//! Advisory work claims and `who` (L5.2).
//!
//! Claims live in the Store; the Router only authenticates the caller,
//! rejects known credentials in payloads, publishes committed events, and
//! routes at most one coalesced generic conflict notice per holder through
//! the ordinary `send` path. A notice carries only the overlap count, never
//! paths; every suppression gate simply skips it. `who` composes existing
//! L3 projections per current worker — current-generation edits still
//! dirty, dirty files of a singly owned worktree, committed paths against
//! the frozen base the workspace records, live claims, and optional bounded
//! recent history — plus unattributed dirty files of shared checkouts.

use super::{
    EVENT_RESPONSE_ENTRIES_MAX_BYTES, Principal, Router, coordination::decode_coordination_request,
    principal_name,
};
use crate::common::now_ms;
use eliot_core::{
    Agent, Error, GitPathStatus, Result,
    api::{
        ClaimListRequest, ClaimListResponse, ClaimReleaseRequest, ClaimSetRequest, ClaimView,
        GitView, MAX_WHO_PATHS, WhoRequest, WhoResponse, WhoSharedWorkspace, WhoWorker,
    },
};
use eliot_store::{NoticePending, WhoWorkerRow};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};

fn truncate_paths(mut paths: Vec<String>) -> Vec<String> {
    paths.truncate(MAX_WHO_PATHS);
    paths
}

/// Project-relative paths of one dirty set, in observation order.
fn dirty_paths(dirty: &[GitPathStatus]) -> Vec<String> {
    dirty.iter().map(|path| path.path.clone()).collect()
}

/// Dirty paths of one dirty set as a lookup set, for edit attribution.
fn dirty_path_set(dirty: &[GitPathStatus]) -> HashSet<&str> {
    dirty.iter().map(|path| path.path.as_str()).collect()
}

/// Store-locked inputs for one `who` read: the roster, the project's live
/// claims, workspace ownership counts and the worker Agents.
struct WhoInputs {
    rows: Vec<WhoWorkerRow>,
    truncated: bool,
    claims: ClaimListResponse,
    counts: HashMap<String, i64>,
    agents: HashMap<String, Agent>,
}

impl Router {
    pub(super) async fn claim_set(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: ClaimSetRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "claim.set",
            ClaimSetRequest::validate,
        )?;
        let name = principal_name(principal);
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.claim_set(&name, &request, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        // The Store proves the response binding before retaining it. Only a
        // fresh admission notifies holders; a replay carries no events and
        // must not re-notify.
        if !change.events.is_empty() {
            self.notify_claim_conflicts(&change.value.conflicts).await;
        }
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "claim could not be encoded"))
    }

    pub(super) async fn claim_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: ClaimListRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "claim.list",
            ClaimListRequest::validate,
        )?;
        let name = principal_name(principal);
        // The Store proves the response binding before returning it.
        let response: ClaimListResponse =
            self.inner
                .store
                .lock()
                .await
                .claim_list(&name, &request, now_ms())?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "claim list could not be encoded"))
    }

    pub(super) async fn claim_release(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ClaimReleaseRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "claim.release",
            ClaimReleaseRequest::validate,
        )?;
        let name = principal_name(principal);
        let change = {
            let mut store = self.inner.store.lock().await;
            let change = store.claim_release(&name, &request, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        // The Store proves the response binding before retaining it.
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "claim release could not be encoded"))
    }

    pub(super) async fn who(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: WhoRequest =
            decode_coordination_request(params, &self.inner.secrets, "who", WhoRequest::validate)?;
        let name = principal_name(principal);
        let inputs = self
            .gather_who_inputs(&name, request.project.as_deref(), now_ms())
            .await?;
        let WhoInputs {
            rows,
            truncated: roster_truncated,
            claims,
            counts,
            agents,
        } = inputs;
        let mut by_agent: HashMap<String, Vec<ClaimView>> = HashMap::new();
        for claim in claims.claims {
            by_agent.entry(claim.agent.clone()).or_default().push(claim);
        }
        let mut truncated = roster_truncated || claims.truncated;
        // The caller's own row carries the self mark, so an Agent can
        // answer "which one am I" from `who` alone. Owner/Run callers
        // authenticate without an Agent row and mark nothing.
        let caller = match principal {
            Principal::Agent { id, .. } => Some(id.as_str()),
            _ => None,
        };

        let mut workers = Vec::new();
        let mut encoded_bytes = 0_usize;
        let mut workspace_dirty: HashMap<String, Vec<String>> = HashMap::new();
        for row in &rows {
            let Some(agent) = agents.get(&row.id) else {
                continue;
            };
            let git = match self.inner.monitoring.git_view(agent).await {
                Ok(value) => value,
                Err(error) => GitView::unavailable(error.code),
            };
            record_workspace_dirty(&mut workspace_dirty, row.workspace.as_deref(), &git);
            let mut worker = build_who_worker(
                row,
                &git,
                by_agent.remove(row.id.as_str()).unwrap_or_default(),
                &counts,
                request.want_history(),
            )?;
            worker.is_self = caller == Some(worker.agent.as_str());
            let row_bytes = serde_json::to_vec(&worker)
                .map_err(|_| Error::new("internal", "cannot encode who worker"))?
                .len();
            if encoded_bytes.saturating_add(row_bytes + 1) > EVENT_RESPONSE_ENTRIES_MAX_BYTES {
                if workers.is_empty() {
                    return Err(Error::new(
                        "frame_too_large",
                        "who worker exceeds the page byte limit",
                    ));
                }
                truncated = true;
                break;
            }
            encoded_bytes += row_bytes + 1;
            workers.push(worker);
        }

        let (shared, shared_truncated) =
            build_shared_workspaces(&workers, &counts, &mut workspace_dirty, &mut encoded_bytes)?;
        truncated = truncated || shared_truncated;

        let response = WhoResponse {
            workers,
            shared,
            truncated,
        };
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "who response could not be encoded"))
    }

    /// Roster, live claims, ownership counts and worker Agents under one
    /// Store lock. Verified edits are not re-read here: each worker's Git
    /// observation already carries them.
    async fn gather_who_inputs(
        &self,
        principal: &str,
        project: Option<&str>,
        now: i64,
    ) -> Result<WhoInputs> {
        let store = self.inner.store.lock().await;
        let (rows, truncated) = store.who_workers(principal, project)?;
        let claims = store.claim_list(
            principal,
            &ClaimListRequest {
                project: project.map(str::to_owned),
                agent: None,
            },
            now,
        )?;
        let counts = store.workspace_owner_counts()?;
        let mut agents = HashMap::new();
        for row in &rows {
            agents.insert(
                row.id.clone(),
                store
                    .get_agent(&row.id)?
                    .ok_or_else(|| Error::new("internal", "who worker vanished"))?,
            );
        }
        Ok(WhoInputs {
            rows,
            truncated,
            claims,
            counts,
            agents,
        })
    }

    pub(super) async fn notify_claim_conflicts(
        &self,
        conflicts: &[eliot_core::api::ClaimConflict],
    ) {
        let mut holders: BTreeMap<&str, i64> = BTreeMap::new();
        for conflict in conflicts {
            *holders.entry(conflict.agent.as_str()).or_insert(0) += 1;
        }
        for (holder, overlapping) in holders {
            self.offer_notice(holder, NoticePending::Claim { overlapping })
                .await;
        }
    }
}

/// One roster row: verified current-generation edits still dirty in the
/// cached Git view, singly-owned dirty files, frozen-base committed paths,
/// live claims and optional bounded history.
fn build_who_worker(
    row: &WhoWorkerRow,
    git: &GitView,
    claims: Vec<ClaimView>,
    counts: &HashMap<String, i64>,
    want_history: bool,
) -> Result<WhoWorker> {
    let (available, dirty, complete, committed, base_oid, commits) = match git {
        GitView::Data(observation) => (
            observation.available,
            observation.dirty.as_ref().map(|dirty| dirty_paths(dirty)),
            observation.complete,
            observation.committed_paths.clone().unwrap_or_default(),
            observation.base_oid.clone(),
            observation.commits.clone().unwrap_or_default(),
        ),
        GitView::Unavailable { .. } | GitView::Error { .. } => {
            (false, None, None, Vec::new(), None, Vec::new())
        }
    };
    let singly_owned = row
        .workspace
        .as_ref()
        .is_some_and(|workspace| counts.get(workspace) == Some(&1));
    let (attributed_edits, edits_truncated) = attributed_edits(git);
    let worker = WhoWorker {
        agent: row.id.clone(),
        generation: row.generation,
        is_self: false,
        state: row.state,
        chat: row.chat,
        project: row.project.clone(),
        workspace: row.workspace.clone(),
        singly_owned,
        git_available: available,
        attribution_complete: available && dirty.is_some() && complete == Some(true),
        edits_truncated,
        attributed_edits,
        dirty: if singly_owned {
            truncate_paths(dirty.unwrap_or_default())
        } else {
            Vec::new()
        },
        committed_paths: truncate_paths(committed),
        base_oid,
        claims,
        recent_history: if want_history { commits } else { Vec::new() },
    };
    worker.validate()?;
    Ok(worker)
}

/// Verified edits of the observation's generation whose paths are still
/// dirty, with a truncation flag when the observation or the `who` bound
/// cut the list.
fn attributed_edits(git: &GitView) -> (Vec<String>, bool) {
    let GitView::Data(observation) = git else {
        return (Vec::new(), false);
    };
    let dirty: HashSet<&str> = observation
        .dirty
        .as_ref()
        .map(|dirty| dirty_path_set(dirty))
        .unwrap_or_default();
    let mut attributed: Vec<String> = observation
        .edits
        .iter()
        .filter(|edit| observation.available && dirty.contains(edit.path.as_str()))
        .map(|edit| edit.path.clone())
        .collect();
    attributed.sort();
    attributed.dedup();
    let truncated = observation.edits_truncated || observation.edits.len() > MAX_WHO_PATHS;
    attributed.truncate(MAX_WHO_PATHS);
    (attributed, truncated)
}

/// Raw dirty files of one workspace for the shared section, first worker
/// wins: every in-scope owner sees the same cached snapshot.
fn record_workspace_dirty(
    workspace_dirty: &mut HashMap<String, Vec<String>>,
    workspace: Option<&str>,
    git: &GitView,
) {
    let GitView::Data(observation) = git else {
        return;
    };
    let (Some(workspace), Some(dirty)) = (workspace, observation.dirty.as_ref()) else {
        return;
    };
    workspace_dirty
        .entry(workspace.to_owned())
        .or_insert_with(|| truncate_paths(dirty_paths(dirty)));
}

/// Unattributed dirty files of shared checkouts with two or more in-scope
/// owners, under the shared response byte budget. Returns the entries and
/// whether the budget cut the list.
fn build_shared_workspaces(
    workers: &[WhoWorker],
    counts: &HashMap<String, i64>,
    workspace_dirty: &mut HashMap<String, Vec<String>>,
    encoded_bytes: &mut usize,
) -> Result<(Vec<WhoSharedWorkspace>, bool)> {
    let mut shared = Vec::new();
    let mut groups: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    for worker in workers {
        let Some(workspace) = worker.workspace.as_deref() else {
            continue;
        };
        if counts.get(workspace).is_some_and(|owners| *owners > 1) {
            groups
                .entry(workspace)
                .or_default()
                .push(worker.agent.as_str());
        }
    }
    for (workspace, agents) in groups {
        if agents.len() < 2 {
            continue;
        }
        let entry = WhoSharedWorkspace {
            workspace: workspace.to_owned(),
            agents: agents.into_iter().map(str::to_owned).collect(),
            dirty: workspace_dirty.remove(workspace).unwrap_or_default(),
        };
        entry.validate()?;
        let row_bytes = serde_json::to_vec(&entry)
            .map_err(|_| Error::new("internal", "cannot encode shared workspace"))?
            .len();
        if encoded_bytes.saturating_add(row_bytes + 1) > EVENT_RESPONSE_ENTRIES_MAX_BYTES {
            return Ok((shared, true));
        }
        *encoded_bytes += row_bytes + 1;
        shared.push(entry);
    }
    Ok((shared, false))
}
