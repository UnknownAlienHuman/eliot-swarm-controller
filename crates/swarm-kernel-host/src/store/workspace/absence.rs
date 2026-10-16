//! Read-only retirement of an exact superseded, pre-native workspace launch.
//! This never recreates a worktree or grants stale Task mutation authority.

use super::*;
use crate::store::{Store, launcher, tasks};
use rusqlite::TransactionBehavior;

struct PendingReadback {
    ticket: WorkspaceLeaseTicket,
    snapshot: Value,
}

fn no_native_work(db: &Connection, row: &LeaseRow) -> Result<bool> {
    let Some(attempt_id) = row.attempt_id.as_deref() else {
        return Ok(false);
    };
    let attempt = tasks::get_attempt(db, attempt_id)?;
    if attempt["task_id"] != row.task_id
        || attempt["task_revision"] != row.task_revision
        || attempt["owner_id"] != row.owner_client_id
        || !attempt["start_operation_id"].is_null()
        || !attempt["binding_id"].is_null()
        || !attempt["binding_generation"].is_null()
        || attempt["producers"]
            .as_array()
            .is_none_or(|items| !items.is_empty())
    {
        return Ok(false);
    }
    let linked: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM operations WHERE operation_id<>?1 AND
           (prerequisite_operation_id=?1 OR (attempt_id=?2 AND
             (binding_id IS NOT NULL OR binding_generation IS NOT NULL
              OR method LIKE 'agent.%' OR method='task.dispatch'
              OR state IN ('sending','native_accepted','outcome_unknown')))))
         OR EXISTS(SELECT 1 FROM owned_service_starts WHERE lease_id=?3)
         OR EXISTS(SELECT 1 FROM check_runs WHERE attempt_id=?2
                   AND resource_claimed_at_ms IS NOT NULL AND resource_released_at_ms IS NULL)",
        params![row.operation_id, attempt_id, row.lease_id],
        |record| record.get(0),
    )?;
    Ok(!linked)
}

fn pending(
    db: &Connection,
    config: &Config,
    operation_id: &str,
) -> Result<Option<PendingReadback>> {
    let ids = {
        let mut statement = db.prepare(
            "SELECT lease_id FROM workspace_leases WHERE operation_id=?1 AND state='stale'
             AND CASE WHEN json_valid(clean_state_json) THEN
               json_extract(clean_state_json,'$.prior_lease_state')='outcome_unknown'
             ELSE 0 END ORDER BY lease_id LIMIT 2",
        )?;
        statement
            .query_map([operation_id], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    if ids.len() != 1 {
        return Ok(None);
    }
    let row = lease_row(db, &ids[0])?;
    let Some(operation) = unknown_workspace_operation_snapshot(db, &row)? else {
        return Ok(None);
    };
    let Some(attempt_id) = row.attempt_id.as_deref() else {
        return Ok(None);
    };
    let task = tasks::get_task(db, &row.task_id)?;
    let attempt = tasks::get_attempt(db, attempt_id)?;
    if task["state"] != "open"
        || task["project_id"] != row.project_id
        || task["revision"]
            .as_i64()
            .is_none_or(|revision| revision <= row.task_revision)
        || task["current_attempt_id"].as_str() != Some(attempt_id)
        || attempt["state"] != "reserved"
        || !attempt["released_at_ms"].is_null()
        || !no_native_work(db, &row)?
    {
        return Ok(None);
    }
    let actor = launcher::workspace_readback_actor(db, operation_id)?;
    if actor.effective_manager_id() != row.owner_client_id {
        return Ok(None);
    }
    let current_registration = get_registration(db, &row.project_id, config)?;
    if current_registration.registration_id != row.registration_id
        || !current_registration
            .allowed_roots
            .iter()
            .any(|root| row.workspace_path.parent() == Some(root.as_path()))
    {
        return Ok(None);
    }
    // A restored operator configuration can have a later generation, but its
    // exact roots/repository/digest must reconstruct the original reservation.
    // Changing roots never grants access to an old root implicitly.
    let mut historical_registration = current_registration.clone();
    historical_registration.generation = row.registration_generation;
    let plan = plan_from_row(&row)?;
    let facts = RegistrationRow {
        registration_id: historical_registration.registration_id.clone(),
        allowed_roots: historical_registration.allowed_roots.clone(),
        registration_digest: historical_registration.registration_digest.clone(),
        generation: historical_registration.generation,
        state: "active".into(),
    };
    if lease_intent_digest(
        &row.lease_id,
        &facts,
        row.generation,
        &plan,
        &row.branch_ref,
        &row.worktree_handle,
        &path_text(&row.workspace_path)?,
    )? != row.binding_digest
    {
        return Ok(None);
    }
    let snapshot = json!({
        "operation":operation,
        "lease":{
            "id":row.lease_id,"generation":row.generation,"digest":row.binding_digest,
            "registration_id":row.registration_id,"registration_generation":row.registration_generation,
            "state":row.state,"clean_state":row.clean_state,
            "path":path_text(&row.workspace_path)?,"branch_ref":row.branch_ref,
            "task_id":row.task_id,"task_revision":row.task_revision,"attempt_id":row.attempt_id,
            "plan_digest":row.plan_digest,"owner_id":row.owner_client_id,
        },
        "current_registration_generation":current_registration.generation,
        "current_registration_digest":current_registration.registration_digest,
        "current_task_revision":task["revision"],"current_task_state":task["state"],
        "attempt":attempt,
    });
    Ok(Some(PendingReadback {
        ticket: ticket_from_row(row, historical_registration)?,
        snapshot,
    }))
}

pub(in crate::store) fn lease_has_absence_resolution(
    db: &Connection,
    lease_id: &str,
    generation: i64,
    binding_digest: &str,
) -> Result<bool> {
    let row = lease_row(db, lease_id)?;
    let proof = &row.clean_state["workspace_absence_readback"];
    if row.generation != generation
        || row.binding_digest != binding_digest
        || row.clean_state["status"] != "workspace_absence_verified"
        || proof["schema_version"] != 1
        || proof["kind"] != "workspace_absence_readback"
        || proof["lease_id"] != row.lease_id
        || proof["lease_generation"] != row.generation
        || proof["intent_digest"] != row.binding_digest
        || proof["registration_id"] != row.registration_id
        || proof["registration_generation"] != row.registration_generation
        || proof["project_id"] != row.project_id
        || proof["owner_client_id"] != row.owner_client_id
        || proof["branch_ref"] != row.branch_ref
        || proof["worktree_handle"] != row.worktree_handle
        || proof["allowed_paths"] != json!(row.allowed_paths)
        || proof["allowed_symbols"] != json!(row.allowed_symbols)
        || proof["workspace_path_sha256"]
            != model::digest(path_text(&row.workspace_path)?.as_bytes())
        || proof["operation_id"] != row.operation_id
        || proof["task_id"] != row.task_id
        || proof["task_revision"] != row.task_revision
        || proof["attempt_id"] != json!(row.attempt_id)
        || proof["plan_digest"] != row.plan_digest
        || proof["path_absent"] != true
        || proof["branch_absent"] != true
        || proof["worktree_absent"] != true
        || proof["historical_filesystem_effect"] != "unknown"
        || !proof["source_head"]
            .as_str()
            .is_some_and(crate::forge::valid_object_id)
        || proof["verified_at_ms"]
            .as_i64()
            .is_none_or(|value| value < 0)
        || !no_native_work(db, &row)?
    {
        return Ok(false);
    }
    let operation = super::super::operations::get_operation(db, &row.operation_id)?;
    let effective: String = db.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [&row.operation_id],
        |record| record.get(0),
    )?;
    let effective: Value = serde_json::from_str(&effective)?;
    let manifest = &effective["launch_manifest"];
    Ok(operation["state"] == "settled"
        && operation["binding_id"].is_null()
        && operation["result"]["launch_state"] == "blocked"
        && operation["result"]["failure"]["code"] == "workspace_absent_after_readback"
        && operation["result"]["native_effect"] == "not_attempted"
        && operation["result"]["workspace_absence_readback"] == *proof
        && manifest["state"] == "blocked"
        && manifest["failure"]["code"] == "workspace_absent_after_readback"
        && manifest["runtime"]["native_effect"] == "not_attempted"
        && manifest["workspace_absence_readback"] == *proof)
}

impl Store {
    async fn hold_absence_readback(&self, operation_id: &str, error: Error) -> Result<bool> {
        if !error.code.starts_with("WORKSPACE_")
            && !matches!(
                error.code.as_str(),
                "FORBIDDEN" | "STALE_LAUNCH" | "CONFLICT" | "NOT_FOUND"
            )
        {
            return Err(error);
        }
        // A candidate's authority loss must not stop the outer scan cursor.
        self.record_launch_failure(operation_id.to_owned(), error.code)
            .await?;
        Ok(true)
    }

    pub(in crate::store) async fn reconcile_absent_workspace_launch(
        &self,
        operation_id: &str,
    ) -> Result<bool> {
        let id = operation_id.to_owned();
        let config = self.config.clone();
        let candidate = match self.run(move |db| pending(db, &config, &id)).await {
            Ok(candidate) => candidate,
            Err(error) => return self.hold_absence_readback(operation_id, error).await,
        };
        let Some(candidate) = candidate else {
            return Ok(false);
        };
        let config = self.config.clone();
        let fs_ticket = candidate.ticket.clone();
        let evidence = self
            .workspace_git_io(
                &candidate.ticket.plan.project_id,
                &candidate.ticket.reservation.branch_ref,
                move |git| {
                    let project = config
                        .forge
                        .projects
                        .get(&fs_ticket.plan.project_id)
                        .ok_or_else(|| {
                            Error::new("WORKSPACE_UNREGISTERED", "readback mapping is unavailable")
                        })?;
                    workspace::read_absent_workspace(
                        git,
                        &config.forge,
                        project,
                        &fs_ticket.registration,
                        &fs_ticket.reservation,
                        &fs_ticket.plan,
                    )
                },
            )
            .await;
        let evidence = match evidence {
            Ok(evidence) => evidence,
            Err(error) => {
                self.record_launch_failure(operation_id.to_owned(), error.code)
                    .await?;
                return Ok(true);
            }
        };
        let config = self.config.clone();
        let id = operation_id.to_owned();
        let committed = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = pending(&tx, &config, &id)?
                    .ok_or_else(|| Error::conflict("absence readback lost its eligible launch"))?;
                if current.snapshot != candidate.snapshot {
                    return Err(Error::conflict("absence readback snapshot changed"));
                }
                let now = model::now_ms()?;
                let mut proof =
                    evidence.readback(&current.ticket.registration, &current.ticket.reservation)?;
                proof["operation_id"] = json!(id);
                proof["task_id"] = json!(current.ticket.plan.task_id);
                proof["task_revision"] = json!(current.ticket.plan.task_revision);
                proof["attempt_id"] = json!(current.ticket.plan.attempt_id);
                proof["plan_digest"] = json!(current.ticket.plan.plan_digest);
                proof["verified_at_ms"] = json!(now);
                proof["snapshot_sha256"] = json!(model::digest(
                    model::canonical(&current.snapshot)?.as_bytes()
                ));
                launcher::settle_absent_workspace(&tx, &id, proof.clone(), now)?;
                let clean = model::canonical(&json!({
                    "status":"workspace_absence_verified","prior_lease_state":"outcome_unknown",
                    "workspace_absence_readback":proof,
                    "prior_clean_state":current.snapshot["lease"]["clean_state"],
                }))?;
                let changed = tx.execute(
                    "UPDATE workspace_leases SET clean_state_json=?4,updated_at_ms=?5
                WHERE lease_id=?1 AND generation=?2 AND binding_digest=?3 AND state='stale'",
                    params![
                        current.ticket.reservation.lease_id,
                        current.ticket.reservation.generation,
                        current.ticket.reservation.intent_digest,
                        clean,
                        now
                    ],
                )?;
                if changed != 1 {
                    return Err(Error::conflict("absence lease CAS failed"));
                }
                tx.commit()?;
                Ok(())
            })
            .await;
        if let Err(error) = committed {
            return self.hold_absence_readback(operation_id, error).await;
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        forge::ForgeProject,
        platform::{DataRoot, bootstrap_credential},
        store::StoreOwner,
        workspace::WorkspaceProjectConfig,
    };
    use std::{process::Command, sync::Arc};

    fn git(executable: &Path, repository: &Path, args: &[&str]) -> String {
        let output = Command::new(executable)
            .arg("-C")
            .arg(repository)
            .args(args)
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    #[tokio::test]
    async fn superseded_reserved_launch_requires_real_absence_before_normal_release() {
        let root = std::env::temp_dir().join(format!("swarm-absence-{}", model::new_id()));
        let repo = root.join("repo");
        let workspaces = root.join("workspaces");
        let data = root.join("store");
        for path in [&repo, &workspaces, &data] {
            std::fs::create_dir_all(path).unwrap();
            crate::platform::private_owned_directory(path).unwrap();
        }
        let repo = repo.canonicalize().unwrap();
        let workspaces = workspaces.canonicalize().unwrap();
        let git_name = if cfg!(windows) { "git.exe" } else { "git" };
        let executable = std::env::split_paths(&std::env::var_os("PATH").unwrap())
            .map(|path| path.join(git_name))
            .find(|path| path.is_file())
            .unwrap()
            .canonicalize()
            .unwrap();
        git(&executable, &repo, &["init", "--quiet"]);
        git(
            &executable,
            &repo,
            &["config", "user.name", "Absence Fixture"],
        );
        git(
            &executable,
            &repo,
            &["config", "user.email", "fixture@example.invalid"],
        );
        git(
            &executable,
            &repo,
            &["config", "core.hooksPath", "/dev/null"],
        );
        std::fs::write(repo.join("fixture.txt"), "source\n").unwrap();
        git(&executable, &repo, &["add", "fixture.txt"]);
        git(&executable, &repo, &["commit", "--quiet", "-m", "fixture"]);
        git(
            &executable,
            &repo,
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/fixture/absence.git",
            ],
        );
        let baseline = git(&executable, &repo, &["rev-parse", "HEAD"]);
        let mut config = Config::default();
        config.storage.data_dir = data.clone();
        config.forge.git_executable = executable.clone();
        config.forge.projects.insert(
            "absence".into(),
            ForgeProject {
                canonical_repository: "github.com/fixture/absence".into(),
                repository_path: repo.clone(),
                remote_name: "origin".into(),
                policy_revision: "fixture".into(),
                target_refs: Vec::new(),
            },
        );
        config.workspace.projects.insert(
            "absence".into(),
            WorkspaceProjectConfig {
                allowed_roots: vec![workspaces.clone()],
            },
        );
        let data_root = DataRoot::acquire(&data).unwrap();
        let credential = bootstrap_credential(&data_root.path).unwrap();
        let owner = StoreOwner::start(data_root, Arc::new(config.clone()), credential.clone())
            .await
            .unwrap();
        let operator = owner.store.authenticate(credential).await.unwrap();
        owner.store.initialize_workspace_authority().await.unwrap();
        let created = owner
            .store
            .call(
                operator.clone(),
                "task.create".into(),
                json!({
                    "client_request_id":model::new_id(),"project_id":"absence",
                    "spec":{"objective":"fixture","phase":"implementation",
                        "requirements":[{"id":"R1","statement":"fixture"}],
                        "dependencies":[],"source_refs":[],"source_index":[],
                        "owner_policy_id":crate::policy::OWNER_POLICY_V1_ID}
                }),
            )
            .await
            .unwrap();
        let task_id = created["task_id"].as_str().unwrap().to_owned();
        let claimed = owner
            .store
            .call(
                operator.clone(),
                "task.claim".into(),
                json!({
                    "client_request_id":model::new_id(),"task_id":task_id,"expected_revision":1,
                    "owner_id":operator.client_id,"start_owner":"controller"
                }),
            )
            .await
            .unwrap();
        let attempt_id = claimed["attempt_id"].as_str().unwrap().to_owned();
        let operation_id = model::new_id();
        let lease_id = model::new_id();
        let branch_ref = format!("refs/heads/codex/swarm/{lease_id}");
        let branch_name = branch_ref.strip_prefix("refs/heads/").unwrap().to_owned();
        let handle = format!("wt-{lease_id}");
        let workspace_path = workspaces.join(&handle);
        let plan_digest = format!("sha256:{}", "a".repeat(64));
        owner.store.run({
            let operator = operator.clone(); let config = config.clone();
            let task_id = task_id.clone(); let attempt_id = attempt_id.clone();
            let operation_id = operation_id.clone(); let lease_id = lease_id.clone();
            let branch_ref = branch_ref.clone(); let handle = handle.clone();
            let workspace_path = workspace_path.clone(); let baseline = baseline.clone();
            move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let registration = get_registration(&tx, "absence", &config)?;
                let plan = WorkspaceLeasePlan {
                    project_id:"absence".into(),task_id:task_id.clone(),task_revision:1,
                    operation_id:operation_id.clone(),plan_digest:plan_digest.clone(),
                    owner_client_id:operator.client_id.clone(),attempt_id:Some(attempt_id.clone()),
                    allowed_paths:vec!["fixture.txt".into()],allowed_symbols:Vec::new(),
                    expected_baseline_commit:Some(baseline.clone()),
                };
                let facts = RegistrationRow {
                    registration_id:registration.registration_id.clone(),allowed_roots:registration.allowed_roots.clone(),
                    registration_digest:registration.registration_digest.clone(),generation:registration.generation,state:"active".into(),
                };
                let digest = lease_intent_digest(&lease_id,&facts,1,&plan,&branch_ref,&handle,&path_text(&workspace_path)?)?;
                let request = json!({"client_request_id":operation_id,"plan_digest":plan_digest,
                    "task_id":task_id,"expected_task_revision":1,"route":"fixture","agent_profile":"fixture",
                    "mcp_profile":"fixture","mcp_surface":"participant-core","workspace_policy":"fixture",
                    "requested_model":null,"requested_effort":null,
                    "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},"stop_conditions":[],"purpose":"fixture"});
                let manifest = json!({"manifest_version":"eliot-launch-manifest-v1","state":"pending_workspace","plan_digest":plan_digest,
                    "actor":{"kind":"direct","client_id":operator.client_id,"role":"operator","link_id":operator.link_id},
                    "task":{"task_id":task_id,"project_id":"absence","expected_revision":1,"observed_revision":1,"attempt_id":attempt_id},
                    "runtime":{"native_effect":"not_attempted"},"progress":{"workspace_lease":"preparing"}});
                tx.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,due_at_ms,created_at_ms,updated_at_ms)
                    VALUES(?1,?2,?1,'swarm.launch',?3,?4,?5,?6,'queued',1,1,1)",
                    params![operation_id,operator.client_id,model::canonical(&request)?,model::canonical(&json!({"launch_manifest":manifest}))?,task_id,attempt_id])?;
                tx.execute("INSERT INTO workspace_leases(lease_id,registration_id,registration_generation,project_id,task_id,task_revision,operation_id,plan_digest,owner_client_id,attempt_id,allowed_paths_json,allowed_symbols_json,baseline_commit,branch_ref,worktree_handle,workspace_path,clean_state_json,generation,binding_digest,state,created_at_ms,updated_at_ms)
                    VALUES(?1,?2,?3,'absence',?4,1,?5,?6,?7,?8,'[\"fixture.txt\"]','[]',?9,?10,?11,?12,'{\"status\":\"pending\"}',1,?13,'preparing',1,1)",
                    params![lease_id,registration.registration_id,registration.generation,task_id,operation_id,plan_digest,operator.client_id,attempt_id,baseline,branch_ref,handle,path_text(&workspace_path)?,digest])?;
                tx.commit()?; Ok(())
            }
        }).await.unwrap();
        owner
            .store
            .record_launch_failure(operation_id.clone(), "WORKSPACE_ACL".into())
            .await
            .unwrap();
        let before_failure = owner
            .store
            .run({
                let id = operation_id.clone();
                move |db| meta(db, &format!("launcher:failure:{id}"))
            })
            .await
            .unwrap();
        let current = owner
            .store
            .call(
                operator.clone(),
                "task.get".into(),
                json!({"task_id":task_id}),
            )
            .await
            .unwrap();
        owner
            .store
            .call(
                operator.clone(),
                "task.revise".into(),
                json!({
                    "client_request_id":model::new_id(),"task_id":task_id,"expected_revision":1,
                    "spec":current["spec"].clone()
                }),
            )
            .await
            .unwrap();
        owner
            .store
            .reconcile_workspace_lifecycle_once()
            .await
            .unwrap();
        // Restoring the same operator roots advances registration generation;
        // its original intent must still reconstruct exactly, not be guessed.
        owner
            .store
            .run(move |db| {
                db.execute(
                    "UPDATE workspace_registrations SET generation=3 WHERE project_id='absence'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let profile_key = format!("client:{}", operator.client_id);
        let original_profile = owner
            .store
            .run({
                let key = profile_key.clone();
                move |db| meta(db, &key)
            })
            .await
            .unwrap()
            .unwrap();
        owner
            .store
            .run({
                let key = profile_key.clone();
                let mut profile = original_profile.clone();
                move |db| {
                    profile["disabled"] = json!(true);
                    super::super::super::set_meta(db, &key, &profile)
                }
            })
            .await
            .unwrap();
        assert!(
            owner
                .store
                .reconcile_absent_workspace_launch(&operation_id)
                .await
                .unwrap(),
            "candidate authority loss must not abort the shared reconciliation scan"
        );
        owner
            .store
            .run(move |db| super::super::super::set_meta(db, &profile_key, &original_profile))
            .await
            .unwrap();
        owner
            .store
            .run(move |db| {
                db.execute(
                    "UPDATE workspace_registrations SET state='revoked' WHERE project_id='absence'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        assert!(
            owner
                .store
                .reconcile_absent_workspace_launch(&operation_id)
                .await
                .unwrap(),
            "a revoked workspace registration holds this candidate without pinning the scan"
        );
        owner
            .store
            .run(move |db| {
                db.execute(
                    "UPDATE workspace_registrations SET state='active' WHERE project_id='absence'",
                    [],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        std::fs::write(&workspace_path, "present").unwrap();
        assert!(
            owner
                .store
                .reconcile_absent_workspace_launch(&operation_id)
                .await
                .unwrap()
        );
        let state = owner
            .store
            .run({
                let id = operation_id.clone();
                move |db| super::super::super::operations::get_operation(db, &id)
            })
            .await
            .unwrap();
        assert_eq!(state["state"], "outcome_unknown");
        std::fs::remove_file(&workspace_path).unwrap();
        git(&executable, &repo, &["branch", &branch_name]);
        assert!(
            owner
                .store
                .reconcile_absent_workspace_launch(&operation_id)
                .await
                .unwrap()
        );
        let state = owner
            .store
            .run({
                let id = operation_id.clone();
                move |db| super::super::super::operations::get_operation(db, &id)
            })
            .await
            .unwrap();
        assert_eq!(state["state"], "outcome_unknown");
        git(&executable, &repo, &["branch", "-D", &branch_name]);
        // Even a queued linked native open blocks this pre-native-only path.
        owner.store.run({let parent=operation_id.clone(); let attempt=attempt_id.clone(); move |db| {
            db.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,attempt_id,prerequisite_operation_id,state,due_at_ms,created_at_ms,updated_at_ms)
                VALUES('absence-native-child','operator','absence-native-child','agent.open','{}','{}',?1,?2,'queued',1,1,1)",params![attempt,parent])?;
            Ok(())
        }}).await.unwrap();
        assert!(
            !owner
                .store
                .reconcile_absent_workspace_launch(&operation_id)
                .await
                .unwrap()
        );
        owner.store.run(move |db| {
            db.execute("DELETE FROM operations WHERE operation_id='absence-native-child' AND state='queued'",[])?;
            Ok(())
        }).await.unwrap();
        assert!(
            owner
                .store
                .reconcile_absent_workspace_launch(&operation_id)
                .await
                .unwrap()
        );
        let state = owner
            .store
            .run({
                let id = operation_id.clone();
                move |db| super::super::super::operations::get_operation(db, &id)
            })
            .await
            .unwrap();
        assert_eq!(state["state"], "settled");
        assert_eq!(
            state["result"]["failure"]["code"],
            "workspace_absent_after_readback"
        );
        let failure = owner
            .store
            .run({
                let id = operation_id.clone();
                move |db| meta(db, &format!("launcher:failure:{id}"))
            })
            .await
            .unwrap();
        assert_eq!(
            failure.as_ref().unwrap()["first_failure"],
            before_failure.as_ref().unwrap()["first_failure"]
        );
        assert_eq!(
            before_failure.as_ref().unwrap()["first_failure"]["code"],
            "WORKSPACE_ACL"
        );
        owner.store.call(operator,"attempt.release".into(),json!({"client_request_id":model::new_id(),"attempt_id":attempt_id,"outcome":"failed","reason":"verified absence","assignment_closed":true})).await.unwrap();
        owner
            .store
            .reconcile_workspace_lifecycle_once()
            .await
            .unwrap();
        let (lease_state, native_inputs) = owner.store.run(move |db| {
            let row=lease_row(db,&lease_id)?;
            let count:i64=db.query_row("SELECT count(*) FROM operations WHERE method IN ('agent.open','task.dispatch')",[],|r|r.get(0))?;
            Ok((row.state,count))
        }).await.unwrap();
        assert_eq!(lease_state, "released");
        assert_eq!(
            native_inputs, 0,
            "absence recovery never opens or replays a native session"
        );
        owner.close().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
