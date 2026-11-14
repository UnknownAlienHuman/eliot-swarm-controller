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
        ClaimListRequest, ClaimListResponse, ClaimReleaseRequest, ClaimSetRequest,
        ClaimSetResponse, ClaimView, GitView, MAX_WHO_PATHS, WhoRequest, WhoResponse,
        WhoSharedWorkspace, WhoWorker,
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
            self.notify_conflict_holders(&change.value).await;
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
            let worker = build_who_worker(
                row,
                &git,
                by_agent.remove(row.id.as_str()).unwrap_or_default(),
                &counts,
                request.want_history(),
            )?;
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

    async fn notify_conflict_holders(&self, response: &ClaimSetResponse) {
        let mut holders: BTreeMap<&str, i64> = BTreeMap::new();
        for conflict in &response.conflicts {
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
        state: row.state,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Bus;
    use eliot_core::{
        FileChangeHint, GitCommit, GitPathStatus, GitSnapshot, NativeEvent, SourceBatch,
        SubmitCommand,
        api::{ClaimListResponse, ClaimReleaseResponse, ClaimSetResponse},
    };
    use serde_json::json;
    use std::path::PathBuf;
    use tokio::sync::Mutex as AsyncMutex;

    fn router() -> Router {
        let store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        Router::new(
            PathBuf::from("synthetic-data-root"),
            "owner-credential".to_owned(),
            std::sync::Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        )
    }

    async fn ready_agent(router: &Router, project: &str, workspace: &str) -> String {
        let store_handle = std::sync::Arc::clone(&router.inner.store);
        let mut store = store_handle.lock().await;
        let now = now_ms();
        let launch_id = ulid::Ulid::new().to_string();
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({
                        "project": project,
                        "workspace": workspace,
                        // Codex owns the tool family, so the edit-seeding
                        // test can commit native fileChange events.
                        "harness": "codex",
                        "profile": "codex",
                        "profile_snapshot": {},
                    }),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch");
        let agent = admission.command.target.expect("launch target");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        crate::test_support::install_fixture_sources(&mut store, &agent, &launch_id, 1, now);
        store
            .put_batch(
                SourceBatch {
                    batch_id: ulid::Ulid::new().to_string(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: ulid::Ulid::new().to_string(),
                        data: json!({"command_id":launch_id,
                            "native_scope": format!("{launch_id}:scope"),
                            "native_ref": format!("{launch_id}:ref")}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, eliot_core::CommandState::Applied, None, now)
            .expect("settle launch");
        agent
    }

    fn agent_principal(agent: &str, generation: i64) -> Principal {
        Principal::Agent {
            id: agent.to_owned(),
            generation,
        }
    }

    /// Admitted `send` commands still unsettled for either holder. The
    /// harness never dispatches, so every routed notice stays counted here;
    /// settled launches never appear.
    fn holder_notice_sends(store: &eliot_store::Store, a: &str, b: &str) -> Result<usize> {
        Ok(store
            .list_unsettled_commands(100)?
            .into_iter()
            .filter(|command| {
                command.verb == "send"
                    && command
                        .target
                        .as_deref()
                        .is_some_and(|target| target == a || target == b)
            })
            .count())
    }

    /// Seed one verified native edit through the real ingestion path: an
    /// authoritative completed `fileChange` tool event, consumed by a Git
    /// observation whose dirty set still contains the path.
    async fn seed_native_edit(router: &Router, agent: &str, workspace: &str, path: &str, now: i64) {
        let store_handle = std::sync::Arc::clone(&router.inner.store);
        let mut store = store_handle.lock().await;
        store
            .ensure_git_source(agent, 1, workspace, now)
            .expect("bind git source");
        let send_id = ulid::Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: send_id.clone(),
                    target: Some(agent.to_owned()),
                    selector: None,
                    verb: "send".to_owned(),
                    args: json!({"text": "edit the file"}),
                    expected_generation: None,
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit send");
        store.dispatch(&send_id, now).expect("dispatch send");
        let committed = store
            .put_batch(
                SourceBatch {
                    batch_id: ulid::Ulid::new().to_string(),
                    agent_id: agent.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![
                        NativeEvent {
                            ts: None,
                            kind: "turn.started".to_owned(),
                            native_id: ulid::Ulid::new().to_string(),
                            data: json!({
                                "command_id": send_id,
                                "turn_id": "synthetic-turn",
                            }),
                        },
                        NativeEvent {
                            ts: Some(now),
                            kind: "tool.completed".to_owned(),
                            native_id: ulid::Ulid::new().to_string(),
                            data: json!({
                                "tool": "fileChange",
                                "status": "completed",
                                "turn_id": "synthetic-turn",
                                "changed_paths": [path],
                            }),
                        },
                    ],
                    cursor: None,
                },
                now,
            )
            .expect("commit native file change");
        let tool_seq = committed
            .events
            .iter()
            .find(|event| event.kind == "tool.completed")
            .expect("committed tool event")
            .seq;
        let through =
            u64::try_from(store.high_water().expect("read high water")).expect("sequence fits u64");
        let snapshot = GitSnapshot {
            workspace: workspace.to_owned(),
            head: None,
            branch: None,
            dirty: vec![GitPathStatus {
                path: path.to_owned(),
                status: "modified".to_owned(),
                original_path: None,
            }],
            commits: Vec::new(),
            clean_paths: Vec::new(),
            observed_at: now,
            complete: true,
            history_complete: true,
            history_gap: false,
            base_oid: None,
            committed_paths: Vec::new(),
            base_complete: false,
        };
        store
            .record_git_observation(
                agent,
                1,
                &snapshot,
                &[FileChangeHint {
                    path: path.to_owned(),
                    event_seq: u64::try_from(tool_seq).expect("sequence fits u64"),
                    ts: now,
                }],
                through,
                now,
            )
            .expect("consume native hint into attribution");
    }

    fn cached_snapshot(workspace: &str, dirty: Vec<&str>, commits: Vec<GitCommit>) -> GitSnapshot {
        GitSnapshot {
            workspace: workspace.to_owned(),
            head: Some("a".repeat(40)),
            branch: Some("main".to_owned()),
            dirty: dirty
                .into_iter()
                .map(|path| GitPathStatus {
                    path: path.to_owned(),
                    status: "modified".to_owned(),
                    original_path: None,
                })
                .collect(),
            commits,
            clean_paths: Vec::new(),
            observed_at: now_ms(),
            complete: true,
            history_complete: true,
            history_gap: false,
            base_oid: None,
            committed_paths: Vec::new(),
            base_complete: false,
        }
    }

    #[tokio::test]
    async fn agent_claim_roundtrip_reports_conflicts_without_live_notice() {
        let router = router();
        let a = ready_agent(&router, "app", "ws-a").await;
        let b = ready_agent(&router, "app", "ws-b").await;

        let first: ClaimSetResponse = serde_json::from_value(
            router
                .claim_set(
                    &json!({
                        "id": ulid::Ulid::new().to_string(),
                        "glob": "src/**",
                        "note": "refactor",
                    }),
                    &agent_principal(&a, 1),
                )
                .await
                .expect("A claims its area"),
        )
        .expect("decode claim response");
        assert_eq!(first.agent, a);
        assert!(first.conflicts.is_empty());

        // B's overlapping claim reports A's area in-band.
        let second: ClaimSetResponse = serde_json::from_value(
            router
                .claim_set(
                    &json!({
                        "id": ulid::Ulid::new().to_string(),
                        "glob": "src/main.rs",
                    }),
                    &agent_principal(&b, 1),
                )
                .await
                .expect("B claims an overlapping file"),
        )
        .expect("decode claim response");
        assert_eq!(second.conflicts.len(), 1);
        assert_eq!(second.conflicts[0].agent, a);
        assert_eq!(second.conflicts[0].glob, "src/**");

        let listed: ClaimListResponse = serde_json::from_value(
            router
                .claim_list(&json!({}), &agent_principal(&b, 1))
                .await
                .expect("B lists project claims"),
        )
        .expect("decode claim list");
        assert_eq!(listed.claims.len(), 2);

        // No live plugin owner exists in this harness, so the conflict
        // notice is skipped and nothing is recorded.
        let store = router.inner.store.lock().await;
        assert_eq!(store.notice_state(&a).unwrap(), None);
        drop(store);

        let released: ClaimReleaseResponse = serde_json::from_value(
            router
                .claim_release(
                    &json!({
                        "id": ulid::Ulid::new().to_string(),
                        "glob": "src/**",
                    }),
                    &agent_principal(&a, 1),
                )
                .await
                .expect("A releases"),
        )
        .expect("decode release response");
        assert_eq!(released.agent, a);
        let listed: ClaimListResponse = serde_json::from_value(
            router
                .claim_list(&json!({}), &agent_principal(&b, 1))
                .await
                .expect("list after release"),
        )
        .expect("decode claim list");
        assert_eq!(listed.claims.len(), 1);
        assert_eq!(listed.claims[0].agent, b);
    }

    #[tokio::test]
    async fn claim_set_notifies_each_conflict_holder_once_and_replay_sends_none() {
        let router = router();
        let a = ready_agent(&router, "app", "ws-a").await;
        let b = ready_agent(&router, "app", "ws-b").await;
        let c = ready_agent(&router, "app", "ws-c").await;
        // Both holders route notices; the claimant needs no live owner.
        router
            .inner
            .plugin_host
            .mark_generation_controlled_for_test(&a, 1);
        router
            .inner
            .plugin_host
            .mark_generation_controlled_for_test(&b, 1);

        router
            .claim_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "glob": "src/a/**",
                }),
                &agent_principal(&a, 1),
            )
            .await
            .expect("A claims its area");
        router
            .claim_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "glob": "src/b/**",
                }),
                &agent_principal(&b, 1),
            )
            .await
            .expect("B claims its area");

        // C's broad claim conflicts with both holders in-band.
        let id = ulid::Ulid::new().to_string();
        let first: ClaimSetResponse = serde_json::from_value(
            router
                .claim_set(
                    &json!({ "id": id, "glob": "src/**" }),
                    &agent_principal(&c, 1),
                )
                .await
                .expect("C claims the broad area"),
        )
        .expect("decode claim response");
        assert_eq!(first.conflicts.len(), 2);

        // Each holder got exactly one generic notice through the ordinary
        // send path; the text carries the overlap count, never paths.
        let mut store = router.inner.store.lock().await;
        for holder in [&a, &b] {
            let state = store
                .notice_state(holder)
                .expect("read notice state")
                .expect("holder notified");
            let open = state.open_notice_command.expect("open notice recorded");
            let command = store
                .get_command(&open)
                .expect("read notice command")
                .expect("notice command admitted");
            assert_eq!(command.verb, "send");
            assert_eq!(command.target.as_deref(), Some(holder.as_str()));
            assert_eq!(
                command.args["text"],
                json!("1 overlapping work claim; run swarm who")
            );
        }
        assert_eq!(
            holder_notice_sends(&store, &a, &b).expect("count notices"),
            2
        );

        // Free both holders' notice gates before replaying: without this
        // the open notice command would suppress a second notify on its
        // own and the replay assertion below would pass even with the
        // router's replay guard removed. With the gates free, only the
        // guard — no events, no notify — keeps the send count at two.
        for holder in [&a, &b] {
            let open = store
                .notice_state(holder)
                .expect("read notice state")
                .and_then(|state| state.open_notice_command)
                .expect("open notice recorded");
            store
                .notice_release(holder, &open)
                .expect("free the notice gate");
        }
        drop(store);

        // A replayed claim.set returns the retained response and routes no
        // further notice: the same two sends, nothing new.
        let replay: ClaimSetResponse = serde_json::from_value(
            router
                .claim_set(
                    &json!({ "id": id, "glob": "src/**" }),
                    &agent_principal(&c, 1),
                )
                .await
                .expect("replay returns the retained response"),
        )
        .expect("decode replay response");
        assert_eq!(replay, first);
        let store = router.inner.store.lock().await;
        assert_eq!(
            holder_notice_sends(&store, &a, &b).expect("count notices"),
            2
        );
    }

    #[tokio::test]
    async fn who_composes_claims_edits_and_single_ownership() {
        let router = router();
        let a = ready_agent(&router, "app", "ws-a").await;
        let b = ready_agent(&router, "app", "ws-a").await;
        let d = ready_agent(&router, "app", "ws-d").await;
        ready_agent(&router, "other", "ws-other").await;
        let now = now_ms();

        seed_native_edit(&router, &a, "ws-a", "note.txt", now).await;
        router
            .claim_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "glob": "src/**",
                }),
                &agent_principal(&a, 1),
            )
            .await
            .expect("A claims");
        router.inner.monitoring.inject_git_snapshot(
            "ws-a",
            cached_snapshot(
                "ws-a",
                vec!["note.txt", "scratch.tmp"],
                vec![GitCommit {
                    oid: "b".repeat(40),
                    paths: vec!["note.txt".to_owned()],
                }],
            ),
        );
        router.inner.monitoring.inject_git_snapshot(
            "ws-d",
            cached_snapshot("ws-d", vec!["solo.txt"], Vec::new()),
        );

        // B reads who and sees A's area: the verified edit still dirty plus
        // the advisory claim. The shared checkout's raw dirty files stay
        // unattributed but visible under `shared`.
        let seen: WhoResponse = serde_json::from_value(
            router
                .who(&json!({}), &agent_principal(&b, 1))
                .await
                .expect("B reads who"),
        )
        .expect("decode who response");
        assert!(!seen.truncated);
        assert_eq!(seen.workers.len(), 3);
        let worker_a = seen
            .workers
            .iter()
            .find(|worker| worker.agent == a)
            .expect("A listed");
        assert_eq!(worker_a.generation, 1);
        assert!(!worker_a.singly_owned);
        assert!(worker_a.git_available);
        assert!(worker_a.attribution_complete);
        assert_eq!(worker_a.attributed_edits, vec!["note.txt".to_owned()]);
        assert!(worker_a.dirty.is_empty());
        assert_eq!(worker_a.claims.len(), 1);
        assert_eq!(worker_a.claims[0].glob, "src/**");
        assert!(worker_a.recent_history.is_empty());
        let worker_d = seen
            .workers
            .iter()
            .find(|worker| worker.agent == d)
            .expect("D listed");
        assert!(worker_d.singly_owned);
        assert_eq!(worker_d.dirty, vec!["solo.txt".to_owned()]);
        assert_eq!(seen.shared.len(), 1);
        assert_eq!(seen.shared[0].workspace, "ws-a");
        assert_eq!(seen.shared[0].agents, vec![a.clone(), b.clone()]);
        assert_eq!(seen.shared[0].dirty.len(), 2);

        // Recent history appears only when asked.
        let with_history: WhoResponse = serde_json::from_value(
            router
                .who(&json!({"history": true}), &agent_principal(&b, 1))
                .await
                .expect("B reads who with history"),
        )
        .expect("decode who response");
        let worker_a = with_history
            .workers
            .iter()
            .find(|worker| worker.agent == a)
            .expect("A listed");
        assert_eq!(worker_a.recent_history.len(), 1);
        assert_eq!(
            worker_a.recent_history[0].paths,
            vec!["note.txt".to_owned()]
        );

        // The owner sees every project; an Agent never does.
        let owned: WhoResponse = serde_json::from_value(
            router
                .who(&json!({}), &Principal::Owner)
                .await
                .expect("owner reads who"),
        )
        .expect("decode who response");
        assert_eq!(owned.workers.len(), 4);
        let error = router
            .who(&json!({"project": "other"}), &agent_principal(&b, 1))
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
    }

    #[tokio::test]
    async fn who_releases_claims_on_generation_change() {
        let router = router();
        let a = ready_agent(&router, "app", "ws-a").await;
        router
            .claim_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "glob": "src/**",
                }),
                &agent_principal(&a, 1),
            )
            .await
            .expect("A claims in generation 1");

        // Prove the old custody closed with a terminal native fact and owned
        // exit evidence, then resume: generation-1's claim is not reused.
        let now = now_ms();
        let store_handle = std::sync::Arc::clone(&router.inner.store);
        let mut store = store_handle.lock().await;
        crate::test_support::install_live_owned_process_source(&mut store, &a, 1, now);
        store
            .put_batch(
                SourceBatch {
                    batch_id: ulid::Ulid::new().to_string(),
                    agent_id: a.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ended".to_owned(),
                        native_id: ulid::Ulid::new().to_string(),
                        data: json!({}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit terminal session fact");
        crate::test_support::simulate_owned_process_exit_proof(&mut store, &a, 1, now);
        let resume_id = ulid::Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: resume_id.clone(),
                    target: Some(a.clone()),
                    selector: None,
                    verb: "resume".to_owned(),
                    args: json!({}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit resume");
        store.dispatch(&resume_id, now).expect("dispatch resume");
        crate::test_support::install_fixture_sources(&mut store, &a, &resume_id, 2, now);
        let current = store
            .get_agent(&a)
            .expect("read resumed Agent")
            .expect("resumed Agent exists");
        assert_eq!(current.generation, 2);
        store
            .put_batch(
                SourceBatch {
                    batch_id: ulid::Ulid::new().to_string(),
                    agent_id: a.clone(),
                    generation: 2,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: ulid::Ulid::new().to_string(),
                        data: json!({"command_id":resume_id,
                            "native_scope": current.native_scope,
                            "native_ref": current.native_ref}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit resumed session ready");
        store
            .settle(&resume_id, eliot_core::CommandState::Applied, None, now)
            .expect("settle resume");
        drop(store);

        // The old generation is never presented: generation 2 reads an
        // empty claim, and the stale generation-1 principal is refused.
        let seen: WhoResponse = serde_json::from_value(
            router
                .who(&json!({}), &agent_principal(&a, 2))
                .await
                .expect("generation 2 reads who"),
        )
        .expect("decode who response");
        assert_eq!(seen.workers.len(), 1);
        assert_eq!(seen.workers[0].generation, 2);
        assert!(seen.workers[0].claims.is_empty());
        let error = router
            .who(&json!({}), &agent_principal(&a, 1))
            .await
            .unwrap_err();
        assert_eq!(error.code, "stale_generation");
    }

    #[tokio::test]
    async fn claims_enforce_authority_and_reject_secrets() {
        let router = router();
        let a = ready_agent(&router, "app", "ws-a").await;

        let error = router
            .claim_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "glob": "owner-credential/src/**",
                }),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "secret_in_payload");

        let error = router
            .claim_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "glob": "src/**",
                }),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "invalid");

        let error = router
            .claim_set(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "glob": "src/**",
                }),
                &agent_principal(&a, 0),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "stale_generation");

        let error = router
            .claim_list(&json!({"project": "other"}), &agent_principal(&a, 1))
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
    }
}
