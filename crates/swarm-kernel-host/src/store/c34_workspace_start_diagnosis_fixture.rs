//! Store-only regression fixture for retaining the first safe workspace
//! failure beside the latest readback observation.
//!
//! It does not start Git, a provider, or an owned service. The exact Git
//! failure is captured separately by the unrun private reproducer.

#[cfg(test)]
mod tests {
    use crate::{
        config::Config,
        error::Result,
        model::{self, Credential, Principal, Role},
        platform::{DataRoot, bootstrap_credential},
        store::{Store, StoreOwner},
    };
    use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
    use serde_json::{Value, json};
    use std::{path::Path, sync::Arc};

    const OPERATION_ID: &str = "c34-workspace-first-failure";
    const FAILURE_KEY: &str = "launcher:failure:c34-workspace-first-failure";

    async fn start_store(directory: &Path) -> (StoreOwner, Credential, Principal) {
        let root = DataRoot::acquire(directory).expect("acquire temporary Store root");
        let credential = bootstrap_credential(&root.path).expect("create Operator credential");
        let mut config = Config::default();
        config.storage.data_dir = directory.to_path_buf();
        let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
            .await
            .expect("start Store owner");
        let operator = owner
            .store
            .authenticate(credential.clone())
            .await
            .expect("authenticate local Operator");
        (owner, credential, operator)
    }

    fn manager(client_id: &str) -> Principal {
        Principal {
            link_id: format!("fixture-link-{client_id}"),
            client_id: client_id.to_owned(),
            role: Role::Manager,
        }
    }

    fn read_meta_raw(db: &Connection, key: &str) -> Result<Option<String>> {
        Ok(db
            .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
                row.get(0)
            })
            .optional()?)
    }

    async fn seed_workspace_launch(store: &Store) -> Result<()> {
        store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let now = model::now_ms()?;
                let manifest = json!({
                    "launch_manifest": {
                        "state": "pending_workspace",
                        "plan_digest": "fixture-plan-digest",
                        "task": {
                            "task_id": "c34-fixture-task",
                            "observed_revision": 1,
                            "attempt_id": null
                        },
                        "runtime": {"native_effect": "not_attempted"},
                        "progress": {"workspace_lease": "preparing"},
                        "failure": null
                    }
                });
                tx.execute(
                    "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
                     VALUES('c34-fixture-task','c34-fixture-project',1,'open','{}',?1,?1)",
                    [now],
                )?;
                tx.execute(
                    "INSERT INTO workspace_registrations(\
                        registration_id,project_id,trusted_repository,repository_path,\
                        allowed_roots_json,registration_digest,generation,authorized_by,\
                        state,created_at_ms,updated_at_ms\
                     ) VALUES('c34-fixture-registration','c34-fixture-project',\
                        'fixture-repository','C:/fixture/repository','[\"C:/fixture/worktrees\"]',\
                        'fixture-registration-digest',1,'fixture-manager','active',?1,?1)",
                    [now],
                )?;
                tx.execute(
                    "INSERT INTO operations(\
                        operation_id,caller_id,client_request_id,method,\
                        original_request_json,effective_request_json,state,\
                        due_at_ms,created_at_ms,updated_at_ms\
                     ) VALUES(?1,'fixture-old-manager','c34-fixture-request','swarm.launch',\
                        '{}',?2,'queued',?3,?3,?3)",
                    params![OPERATION_ID, model::canonical(&manifest)?, now],
                )?;
                tx.execute(
                    "INSERT INTO workspace_leases(\
                        lease_id,registration_id,registration_generation,project_id,\
                        task_id,task_revision,operation_id,plan_digest,owner_client_id,\
                        allowed_paths_json,allowed_symbols_json,baseline_commit,branch_ref,\
                        worktree_handle,workspace_path,clean_state_json,generation,\
                        binding_digest,state,created_at_ms,updated_at_ms\
                     ) VALUES('c34-fixture-lease','c34-fixture-registration',1,\
                        'c34-fixture-project','c34-fixture-task',1,?1,'fixture-plan-digest',\
                        'fixture-old-manager','[]','[]','fixture-commit',\
                        'refs/heads/codex/swarm/c34-fixture','wt-c34-fixture',\
                        'C:/fixture/worktrees/wt-c34-fixture','{}',1,\
                        'fixture-binding-digest','preparing',?2,?2)",
                    params![OPERATION_ID, now],
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn repeated_workspace_failure_keeps_first_and_latest_manager_readback() {
        let directory = std::env::temp_dir().join(format!("swarm-c34-failure-{}", model::new_id()));
        std::fs::create_dir_all(&directory).expect("create temporary Store directory");
        let (owner, _credential, _operator) = start_store(&directory).await;
        seed_workspace_launch(&owner.store)
            .await
            .expect("seed an actual queued launch with a preparing workspace lease");

        owner
            .store
            .record_launch_failure(OPERATION_ID.to_owned(), "WORKSPACE_GIT_UNKNOWN".to_owned())
            .await
            .expect("retain the initial bounded Git failure");
        owner
            .store
            .record_launch_failure(OPERATION_ID.to_owned(), "WORKSPACE_PATH".to_owned())
            .await
            .expect("retain the later readback failure without replacing the first");

        let current = manager("fixture-current-manager");
        let former = manager("fixture-former-manager");
        let unrelated = manager("fixture-unrelated-manager");
        let current_for_read = current.clone();
        let former_for_read = former.clone();
        let unrelated_for_read = unrelated.clone();
        let readback = owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                super::super::set_meta(
                    &tx,
                    "gm",
                    &json!({"client_id":"fixture-current-manager","epoch":2}),
                )?;
                tx.commit()?;

                let retained_raw = read_meta_raw(db, FAILURE_KEY)?.ok_or_else(|| {
                    crate::error::Error::new("STORE_ERROR", "failure metadata missing")
                })?;
                let retained: Value = serde_json::from_str(&retained_raw)?;
                let current = super::super::operations::get_operation_for_current_manager(
                    db,
                    &current_for_read,
                    OPERATION_ID,
                )?;
                let former = super::super::operations::get_operation_for_current_manager(
                    db,
                    &former_for_read,
                    OPERATION_ID,
                )?;
                let unrelated = super::super::operations::get_operation_for_current_manager(
                    db,
                    &unrelated_for_read,
                    OPERATION_ID,
                )?;
                let ordinary = super::super::operations::get_operation(db, OPERATION_ID)?;
                let lease_state: String = db.query_row(
                    "SELECT state FROM workspace_leases WHERE operation_id=?1",
                    [OPERATION_ID],
                    |row| row.get(0),
                )?;
                Ok(json!({
                    "retained":retained,
                    "current":current,
                    "former":former,
                    "unrelated":unrelated,
                    "ordinary":ordinary,
                    "lease_state":lease_state,
                }))
            })
            .await
            .expect("read retained manager diagnostics");

        assert_eq!(
            readback["retained"]["first_failure"]["code"],
            "WORKSPACE_GIT_UNKNOWN"
        );
        assert_eq!(
            readback["retained"]["first_failure"]["classification"],
            "workspace_effect_unknown"
        );
        assert_eq!(readback["retained"]["code"], "WORKSPACE_PATH");
        assert_eq!(
            readback["retained"]["classification"],
            "workspace_effect_unknown"
        );
        assert_eq!(readback["lease_state"], "outcome_unknown");
        for view in ["current", "former", "unrelated", "ordinary"] {
            assert_eq!(readback[view]["state"], "outcome_unknown");
            assert_eq!(readback[view]["result"]["native_effect"], "unknown");
        }
        assert_eq!(
            readback["current"]["workspace_failure_readback"]["first_retained_failure"]["code"],
            "WORKSPACE_GIT_UNKNOWN"
        );
        assert_eq!(
            readback["current"]["workspace_failure_readback"]["latest_failure"]["code"],
            "WORKSPACE_PATH"
        );
        for view in ["former", "unrelated", "ordinary"] {
            assert!(readback[view].get("workspace_failure_readback").is_none());
        }

        owner.close().await.expect("close Store owner");
        std::fs::remove_dir_all(&directory).expect("remove the exact fixture Store directory");
    }
    async fn seed_named_workspace_launch(store: &Store, operation_id: &str) -> Result<()> {
        let operation_id = operation_id.to_owned();
        let task_id = format!("{operation_id}-task");
        let project_id = format!("{operation_id}-project");
        let registration_id = format!("{operation_id}-registration");
        let lease_id = format!("{operation_id}-lease");
        let owner_client_id = format!("{operation_id}-manager");
        let branch_ref = format!("refs/heads/codex/swarm/{operation_id}");
        let worktree_handle = format!("wt-{operation_id}");
        let workspace_path = format!("C:/fixture/worktrees/{worktree_handle}");
        store
        .run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let now = model::now_ms()?;
            let manifest = json!({
                "launch_manifest": {
                    "state": "pending_workspace",
                    "plan_digest": "fixture-plan-digest",
                    "task": {
                        "task_id": task_id.clone(),
                        "observed_revision": 1,
                        "attempt_id": null
                    },
                    "runtime": {"native_effect": "not_attempted"},
                    "progress": {"workspace_lease": "preparing"},
                    "failure": null
                }
            });
            tx.execute(
                "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
                 VALUES(?1,?2,1,'open','{}',?3,?3)",
                params![task_id, project_id, now],
            )?;
            tx.execute(
                "INSERT INTO workspace_registrations(\
                    registration_id,project_id,trusted_repository,repository_path,\
                    allowed_roots_json,registration_digest,generation,authorized_by,\
                    state,created_at_ms,updated_at_ms\
                 ) VALUES(?1,?2,'fixture-repository','C:/fixture/repository',\
                    '[\"C:/fixture/worktrees\"]','fixture-registration-digest',1,\
                    'fixture-manager','active',?3,?3)",
                params![registration_id, project_id, now],
            )?;
            tx.execute(
                "INSERT INTO operations(\
                    operation_id,caller_id,client_request_id,method,\
                    original_request_json,effective_request_json,state,\
                    due_at_ms,created_at_ms,updated_at_ms\
                 ) VALUES(?1,?2,?3,'swarm.launch','{}',?4,'queued',?5,?5,?5)",
                params![
                    operation_id,
                    owner_client_id,
                    format!("{operation_id}-request"),
                    model::canonical(&manifest)?,
                    now,
                ],
            )?;
            tx.execute(
                "INSERT INTO workspace_leases(\
                    lease_id,registration_id,registration_generation,project_id,\
                    task_id,task_revision,operation_id,plan_digest,owner_client_id,\
                    allowed_paths_json,allowed_symbols_json,baseline_commit,branch_ref,\
                    worktree_handle,workspace_path,clean_state_json,generation,\
                    binding_digest,state,created_at_ms,updated_at_ms\
                 ) VALUES(?1,?2,1,?3,?4,1,?5,'fixture-plan-digest',?6,\
                    '[]','[]','fixture-commit',?7,?8,?9,'{}',1,\
                    'fixture-binding-digest','preparing',?10,?10)",
                params![
                    lease_id,
                    registration_id,
                    project_id,
                    task_id,
                    operation_id,
                    owner_client_id,
                    branch_ref,
                    worktree_handle,
                    workspace_path,
                    now,
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn closed_git_path_admission_is_actionable_and_legacy_unknown_stays_unknown() {
        const CLOSED_OPERATION_ID: &str = "c34-git-path-closed";
        const LEGACY_OPERATION_ID: &str = "c34-git-path-legacy";
        let directory =
            std::env::temp_dir().join(format!("swarm-c34-admission-{}", model::new_id()));
        std::fs::create_dir_all(&directory).expect("create temporary Store directory");
        let (owner, _credential, _operator) = start_store(&directory).await;

        seed_named_workspace_launch(&owner.store, CLOSED_OPERATION_ID)
            .await
            .expect("seed the closed admission launch");
        owner
            .store
            .record_launch_failure(
                CLOSED_OPERATION_ID.to_owned(),
                "WORKSPACE_GIT_PATH_TOO_LONG".to_owned(),
            )
            .await
            .expect("settle the pre-effect path admission failure");

        seed_named_workspace_launch(&owner.store, LEGACY_OPERATION_ID)
            .await
            .expect("seed the legacy unknown launch");
        owner
            .store
            .record_launch_failure(
                LEGACY_OPERATION_ID.to_owned(),
                "WORKSPACE_GIT_UNKNOWN".to_owned(),
            )
            .await
            .expect("retain the legacy unknown failure");
        owner
            .store
            .record_launch_failure(
                LEGACY_OPERATION_ID.to_owned(),
                "WORKSPACE_GIT_PATH_TOO_LONG".to_owned(),
            )
            .await
            .expect("retain the later observation without replaying admission closure");

        let current = manager("fixture-current-manager");
        let current_for_read = current.clone();
        let readback = owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                super::super::set_meta(
                    &tx,
                    "gm",
                    &json!({"client_id":"fixture-current-manager","epoch":2}),
                )?;
                tx.commit()?;
                let closed = super::super::operations::get_operation_for_current_manager(
                    db,
                    &current_for_read,
                    CLOSED_OPERATION_ID,
                )?;
                let legacy = super::super::operations::get_operation_for_current_manager(
                    db,
                    &current_for_read,
                    LEGACY_OPERATION_ID,
                )?;
                let closed_lease = db.query_row(
                    "SELECT state,clean_state_json FROM workspace_leases WHERE operation_id=?1",
                    [CLOSED_OPERATION_ID],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )?;
                let legacy_lease: String = db.query_row(
                    "SELECT state FROM workspace_leases WHERE operation_id=?1",
                    [LEGACY_OPERATION_ID],
                    |row| row.get(0),
                )?;
                Ok(json!({
                    "closed": closed,
                    "legacy": legacy,
                    "closed_lease_state": closed_lease.0,
                    "closed_clean_state": serde_json::from_str::<Value>(&closed_lease.1)?,
                    "legacy_lease_state": legacy_lease,
                }))
            })
            .await
            .expect("read exact Manager diagnostics");

        assert_eq!(readback["closed"]["state"], "settled");
        assert_eq!(readback["closed"]["result"]["launch_state"], "blocked");
        assert_eq!(
            readback["closed"]["result"]["native_effect"],
            "not_attempted"
        );
        assert_eq!(readback["closed_lease_state"], "stale");
        assert_eq!(
            readback["closed_clean_state"]["prior_lease_state"],
            "preparing"
        );
        assert_eq!(
            readback["closed"]["workspace_failure_readback"]["status"],
            "admission_rejected"
        );
        assert_eq!(
            readback["closed"]["workspace_failure_readback"]["reason_code"],
            "WORKSPACE_GIT_PATH_TOO_LONG"
        );
        assert_eq!(
            readback["closed"]["workspace_failure_readback"]["manager_action"],
            "shorten the configured workspace root and submit a new launch"
        );
        assert_eq!(
            readback["closed"]["workspace_failure_readback"]["retry_authorized"],
            false
        );
        assert_eq!(
            readback["closed"]["workspace_failure_readback"]["first_retained_failure"]["code"],
            "WORKSPACE_GIT_PATH_TOO_LONG"
        );
        assert_eq!(readback["legacy"]["state"], "outcome_unknown");
        assert_eq!(readback["legacy"]["result"]["native_effect"], "unknown");
        assert_eq!(readback["legacy_lease_state"], "outcome_unknown");
        assert_eq!(
            readback["legacy"]["workspace_failure_readback"]["status"],
            "readback_required"
        );
        assert_eq!(
            readback["legacy"]["workspace_failure_readback"]["first_retained_failure"]["code"],
            "WORKSPACE_GIT_UNKNOWN"
        );
        assert_eq!(
            readback["legacy"]["workspace_failure_readback"]["latest_failure"]["code"],
            "WORKSPACE_GIT_PATH_TOO_LONG"
        );
        assert_eq!(
            readback["legacy"]["workspace_failure_readback"]["latest_failure"]["classification"],
            "workspace_effect_unknown"
        );

        owner.close().await.expect("close Store owner");
        std::fs::remove_dir_all(&directory).expect("remove the exact fixture Store directory");
    }
}
