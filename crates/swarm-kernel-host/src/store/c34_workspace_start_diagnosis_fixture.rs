//! Store workspace-diagnosis and revoked-lease recovery fixtures.
//!
//! Recovery uses the existing bounded local Git readback path; no provider or
//! owned service is started.

#[cfg(test)]
mod tests {
    use crate::{
        config::Config,
        error::Result,
        forge::ForgeProject,
        model::{self, Credential, Principal, Requirement, Role, Scope, TaskSpec},
        platform::{DataRoot, bootstrap_credential},
        store::{Store, StoreOwner},
        workspace::{WorkspaceProjectConfig, WorkspaceRegistration},
    };
    use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
    use serde_json::{Value, json};
    use std::{
        path::{Path, PathBuf},
        process::Command,
        sync::Arc,
    };

    const OPERATION_ID: &str = "c34-workspace-first-failure";
    const FAILURE_KEY: &str = "launcher:failure:c34-workspace-first-failure";

    async fn start_store(directory: &Path) -> (StoreOwner, Credential, Principal) {
        let mut config = Config::default();
        config.storage.data_dir = directory.to_path_buf();
        start_store_with_config(directory, config).await
    }

    async fn start_store_with_config(
        directory: &Path,
        mut config: Config,
    ) -> (StoreOwner, Credential, Principal) {
        let root = DataRoot::acquire(directory).expect("acquire temporary Store root");
        let credential = bootstrap_credential(&root.path).expect("create Operator credential");
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

    fn fixture_git_executable() -> PathBuf {
        let name = if cfg!(windows) { "git.exe" } else { "git" };
        std::env::split_paths(&std::env::var_os("PATH").expect("PATH is set"))
            .map(|directory| directory.join(name))
            .find(|candidate| candidate.is_file())
            .and_then(|candidate| candidate.canonicalize().ok())
            .expect("Git executable is available for the fixture")
    }

    fn run_fixture_git(git: &Path, repository: &Path, hooks: &Path, args: &[&str]) -> String {
        let output = Command::new(git)
            .arg("-c")
            .arg(format!("core.hooksPath={}", hooks.display()))
            .arg("-C")
            .arg(repository)
            .args(args)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .output()
            .expect("run fixture Git command");
        assert!(
            output.status.success(),
            "fixture Git command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("fixture Git output is UTF-8")
            .trim()
            .to_owned()
    }

    fn fixture_git_path_argument(path: &Path) -> String {
        assert!(path.is_absolute(), "fixture Git paths must be absolute");
        #[cfg(windows)]
        {
            use std::path::{Component, Prefix};

            let original = path.to_str().expect("fixture Git path is UTF-8");
            let mut components = path.components();
            let Some(Component::Prefix(prefix)) = components.next() else {
                panic!("fixture Git path must have a Windows prefix");
            };
            let mut converted = match prefix.kind() {
                Prefix::Disk(_) | Prefix::UNC(_, _) => return original.to_owned(),
                Prefix::VerbatimDisk(drive) => format!("{}:\\", char::from(drive)),
                Prefix::VerbatimUNC(server, share) => {
                    let server = server.to_str().expect("fixture UNC server is UTF-8");
                    let share = share.to_str().expect("fixture UNC share is UTF-8");
                    format!("\\\\{server}\\{share}\\")
                }
                Prefix::Verbatim(_) | Prefix::DeviceNS(_) => {
                    panic!("fixture Git path uses an unsupported Windows prefix")
                }
            };
            let mut saw_root = false;
            for component in components {
                match component {
                    Component::RootDir if !saw_root => saw_root = true,
                    Component::Normal(part) if saw_root => {
                        let part = part.to_str().expect("fixture Git path component is UTF-8");
                        if !converted.ends_with('\\') {
                            converted.push('\\');
                        }
                        converted.push_str(part);
                    }
                    Component::CurDir => {}
                    _ => panic!("fixture Git path cannot be normalized safely"),
                }
            }
            assert!(saw_root, "fixture Git path root is missing");
            converted
        }
        #[cfg(not(windows))]
        {
            path.to_str().expect("fixture Git path is UTF-8").to_owned()
        }
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

    async fn public_operation_read(
        store: &Store,
        principal: &Principal,
        operation_id: &str,
    ) -> Result<Value> {
        store
            .call(
                principal.clone(),
                "operation.get".to_owned(),
                json!({"operation_id":operation_id}),
            )
            .await
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
    async fn repeated_workspace_failure_keeps_first_and_latest_public_readback() {
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
        let retained_and_lease = owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                super::super::set_meta(
                    &tx,
                    "gm",
                    &json!({"client_id":"fixture-current-manager","epoch":2}),
                )?;
                for client_id in [
                    "fixture-current-manager",
                    "fixture-former-manager",
                    "fixture-unrelated-manager",
                ] {
                    super::super::set_meta(
                        &tx,
                        &format!("client:{client_id}"),
                        &json!({"role":"manager","disabled":false}),
                    )?;
                }
                tx.commit()?;

                let retained_raw = read_meta_raw(db, FAILURE_KEY)?.ok_or_else(|| {
                    crate::error::Error::new("STORE_ERROR", "failure metadata missing")
                })?;
                let retained: Value = serde_json::from_str(&retained_raw)?;
                let lease_state: String = db.query_row(
                    "SELECT state FROM workspace_leases WHERE operation_id=?1",
                    [OPERATION_ID],
                    |row| row.get(0),
                )?;
                Ok(json!({"retained":retained,"lease_state":lease_state}))
            })
            .await
            .expect("read retained failure metadata and lease state");

        let current_receipt = public_operation_read(&owner.store, &current, OPERATION_ID)
            .await
            .expect("current GM receives the bounded Task-scoped launch receipt");
        assert_eq!(current_receipt["operation_id"], OPERATION_ID);
        assert_eq!(current_receipt["method"], "swarm.launch");
        assert_eq!(current_receipt["state"], "outcome_unknown");
        assert_eq!(current_receipt["task_id"], "c34-fixture-task");
        assert!(current_receipt["attempt_id"].is_null());
        assert_eq!(
            current_receipt["result"],
            json!({"operation_id":OPERATION_ID,"launch_state":"outcome_unknown","state":"outcome_unknown","failure":{"code":"workspace_effect_unknown"}})
        );
        assert!(current_receipt.get("workspace_failure_readback").is_none());
        assert!(current_receipt.get("diagnostic").is_none());

        for manager in [&former, &unrelated] {
            let error = public_operation_read(&owner.store, manager, OPERATION_ID)
                .await
                .expect_err("former and unrelated Managers remain outside Task scope");
            assert_eq!(error.code, "NOT_FOUND");
        }
        let operation = public_operation_read(&owner.store, &_operator, OPERATION_ID)
            .await
            .expect("read the retained launch through the public local-Operator path");
        let readback = json!({
            "retained":retained_and_lease["retained"],
            "lease_state":retained_and_lease["lease_state"],
            "operation":operation,
        });

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
        assert_eq!(readback["operation"]["state"], "outcome_unknown");
        assert_eq!(
            readback["operation"]["result"],
            json!({"operation_id":OPERATION_ID,"launch_state":"outcome_unknown","state":"outcome_unknown","failure":{"code":"workspace_effect_unknown"}})
        );
        assert!(
            readback["operation"]["workspace_failure_readback"].is_object(),
            "local Operator retains the bounded workspace diagnostic"
        );
        assert_eq!(
            readback["operation"]["workspace_failure_readback"]["first_retained_failure"]["code"],
            "WORKSPACE_GIT_UNKNOWN"
        );
        assert_eq!(
            readback["operation"]["workspace_failure_readback"]["latest_failure"]["code"],
            "WORKSPACE_PATH"
        );

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
        let retained_lease_state = owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                super::super::set_meta(
                    &tx,
                    "gm",
                    &json!({"client_id":"fixture-current-manager","epoch":2}),
                )?;
                super::super::set_meta(
                    &tx,
                    "client:fixture-current-manager",
                    &json!({"role":"manager","disabled":false}),
                )?;
                tx.commit()?;
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
                    "closed_lease_state": closed_lease.0,
                    "closed_clean_state": serde_json::from_str::<Value>(&closed_lease.1)?,
                    "legacy_lease_state": legacy_lease,
                }))
            })
            .await
            .expect("read exact lease recovery state");

        let current_receipt = public_operation_read(&owner.store, &current, CLOSED_OPERATION_ID)
            .await
            .expect("current GM receives the bounded Task receipt without diagnostics");
        assert_eq!(current_receipt["operation_id"], CLOSED_OPERATION_ID);
        assert_eq!(current_receipt["method"], "swarm.launch");
        assert_eq!(current_receipt["state"], "settled");
        assert_eq!(
            current_receipt["task_id"],
            format!("{CLOSED_OPERATION_ID}-task")
        );
        assert!(current_receipt["attempt_id"].is_null());
        assert_eq!(
            current_receipt["result"],
            json!({"operation_id":CLOSED_OPERATION_ID,"launch_state":"blocked","state":"settled","failure":{"code":"workspace_admission_rejected"}})
        );
        assert!(current_receipt.get("workspace_failure_readback").is_none());
        assert!(current_receipt.get("diagnostic").is_none());
        let closed = public_operation_read(&owner.store, &_operator, CLOSED_OPERATION_ID)
            .await
            .expect("read closed launch through the public local-Operator path");
        let legacy = public_operation_read(&owner.store, &_operator, LEGACY_OPERATION_ID)
            .await
            .expect("read legacy launch through the public local-Operator path");
        let readback = json!({
            "closed":closed,
            "legacy":legacy,
            "closed_lease_state":retained_lease_state["closed_lease_state"],
            "closed_clean_state":retained_lease_state["closed_clean_state"],
            "legacy_lease_state":retained_lease_state["legacy_lease_state"],
        });

        assert_eq!(readback["closed"]["state"], "settled");
        assert_eq!(
            readback["closed"]["result"],
            json!({"operation_id":CLOSED_OPERATION_ID,"launch_state":"blocked","state":"settled","failure":{"code":"workspace_admission_rejected"}})
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
        assert_eq!(
            readback["legacy"]["result"],
            json!({"operation_id":LEGACY_OPERATION_ID,"launch_state":"outcome_unknown","state":"outcome_unknown","failure":{"code":"workspace_effect_unknown"}})
        );
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

    #[tokio::test]
    async fn revoked_legacy_unknown_workspace_lease_stays_fenced_in_store_lifecycle() {
        const OPERATION_ID: &str = "c34-revoked-legacy-unknown";
        let directory =
            std::env::temp_dir().join(format!("swarm-c34-stale-unknown-{}", model::new_id()));
        std::fs::create_dir_all(&directory).expect("create temporary Store directory");
        let (owner, _credential, operator) = start_store(&directory).await;
        seed_named_workspace_launch(&owner.store, OPERATION_ID)
            .await
            .expect("seed the retained launch and exact workspace lease");
        owner
            .store
            .record_launch_failure(OPERATION_ID.to_owned(), "WORKSPACE_GIT_UNKNOWN".to_owned())
            .await
            .expect("retain the unknown workspace effect");

        owner
            .store
            .initialize_workspace_authority()
            .await
            .expect("revoke the removed fixture registration through Store");
        let sweep = owner
            .store
            .reconcile_workspace_lifecycle_once()
            .await
            .expect("run the real bounded Store workspace lifecycle caller");
        assert_eq!(sweep["released"], 0);

        let retained = owner
            .store
            .run(move |db| {
                let (
                    lease_state,
                    clean_state,
                    lease_generation,
                    attempt_id,
                    registration_state,
                    registration_generation,
                    operation_state,
                    operation_result,
                ): (
                    String,
                    String,
                    i64,
                    Option<String>,
                    String,
                    i64,
                    String,
                    String,
                ) = db.query_row(
                    "SELECT l.state,l.clean_state_json,l.registration_generation,l.attempt_id,
                            r.state,r.generation,o.state,o.result_json
                     FROM workspace_leases l
                     JOIN workspace_registrations r ON r.registration_id=l.registration_id
                     JOIN operations o ON o.operation_id=l.operation_id
                     WHERE l.operation_id=?1",
                    [OPERATION_ID],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                            row.get(6)?,
                            row.get(7)?,
                        ))
                    },
                )?;
                Ok(json!({
                    "lease_state": lease_state,
                    "clean_state": serde_json::from_str::<Value>(&clean_state)?,
                    "lease_registration_generation": lease_generation,
                    "attempt_id": attempt_id,
                    "registration_state": registration_state,
                    "registration_generation": registration_generation,
                    "operation_state": operation_state,
                    "operation_result": serde_json::from_str::<Value>(&operation_result)?,
                }))
            })
            .await
            .expect("read back the exact retained lease and Operation");

        assert_eq!(retained["lease_state"], "stale");
        assert_eq!(retained["clean_state"]["status"], "registration_revoked");
        assert_eq!(
            retained["clean_state"]["prior_lease_state"],
            "outcome_unknown"
        );
        assert_eq!(
            retained["clean_state"]["native_effect_status"],
            "possible_or_unknown"
        );
        assert_eq!(retained["lease_registration_generation"], 1);
        assert!(retained["attempt_id"].is_null());
        assert_eq!(retained["registration_state"], "revoked");
        assert_eq!(retained["registration_generation"], 2);
        assert_eq!(retained["operation_state"], "outcome_unknown");
        assert_eq!(
            retained["operation_result"]["failure"]["code"],
            "workspace_effect_unknown"
        );
        let receipt = public_operation_read(&owner.store, &operator, OPERATION_ID)
            .await
            .expect("read the preserved historical unknown through Store");
        assert_eq!(receipt["state"], "outcome_unknown");
        assert_eq!(
            receipt["result"]["failure"]["code"],
            "workspace_effect_unknown"
        );

        owner.close().await.expect("close Store owner");
        std::fs::remove_dir_all(&directory).expect("remove the exact fixture Store directory");
    }

    // Observe one exact fixture Operation in a consistent read transaction.
    // Do not normalize stored JSON, persist proof, or repeat filesystem readback.
    async fn revoked_workspace_readback_diagnostic(
        store: &Store,
        config: &Config,
        operation_id: &str,
    ) -> Result<Value> {
        let config = config.clone();
        let operation_id = operation_id.to_owned();
        store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Deferred)?;
                let raw: String = tx.query_row(
                    "SELECT json_object(
                        'identity',json_object(
                            'registration',json_object(
                                'registration_id',r.registration_id,'project_id',r.project_id,
                                'trusted_repository',r.trusted_repository,
                                'repository_path',r.repository_path,
                                'allowed_roots_json',r.allowed_roots_json,
                                'registration_digest',r.registration_digest),
                            'lease',json_object(
                                'lease_id',l.lease_id,'registration_id',l.registration_id,
                                'registration_generation',l.registration_generation,
                                'generation',l.generation,'binding_digest',l.binding_digest,
                                'project_id',l.project_id,'task_id',l.task_id,
                                'task_revision',l.task_revision,'operation_id',l.operation_id,
                                'plan_digest',l.plan_digest,'owner_client_id',l.owner_client_id,
                                'attempt_id',l.attempt_id,'allowed_paths_json',l.allowed_paths_json,
                                'allowed_symbols_json',l.allowed_symbols_json,
                                'baseline_commit',l.baseline_commit,'branch_ref',l.branch_ref,
                                'worktree_handle',l.worktree_handle,'workspace_path',l.workspace_path),
                            'task',json_object(
                                'task_id',t.task_id,'project_id',t.project_id,'revision',t.revision,
                                'spec_json',t.spec_json)),
                        'state',json_object(
                            'registration_state',r.state,'registration_generation',r.generation,
                            'lease_state',l.state,'clean_state_raw',l.clean_state_json,
                            'task_state',t.state,'operation_state',o.state,
                            'unreleased_attempt_count',(SELECT count(*) FROM attempts a
                                WHERE a.task_id=t.task_id AND a.released_at_ms IS NULL),
                            'caller_id',o.caller_id,'client_request_id',o.client_request_id,
                            'method',o.method,'task_id',o.task_id,'attempt_id',o.attempt_id,
                            'binding_id',o.binding_id,'binding_generation',o.binding_generation,
                            'original_request_raw',o.original_request_json,
                            'effective_request_raw',o.effective_request_json,'result_raw',o.result_json))
                     FROM workspace_leases l
                     JOIN workspace_registrations r ON r.registration_id=l.registration_id
                     JOIN tasks t ON t.task_id=l.task_id
                     JOIN operations o ON o.operation_id=l.operation_id
                     WHERE l.operation_id=?1 LIMIT 1",
                    [&operation_id],
                    |row| row.get(0),
                )?;
                let mut snapshot: Value = serde_json::from_str(&raw)?;
                let clean_raw = model::text(&snapshot["state"], "clean_state_raw")?.to_owned();
                let clean: Value = serde_json::from_str(&clean_raw)?;
                let canonical = model::canonical(&clean)?;
                snapshot["clean_state"] = clean;
                snapshot["canonical_clean_state"] = json!(canonical);
                snapshot["raw_clean_state_is_canonical"] = json!(clean_raw == canonical);
                snapshot["retained_failure_raw"] = json!(read_meta_raw(
                    &tx,
                    &format!("launcher:failure:{operation_id}"),
                )?);
                // Use the production selector, including its immutable intent
                // digest and revoked-registration/unknown-Operation validation.
                let ticket = super::super::workspace::pending_lease_for_operation(
                    &tx, &config, &operation_id,
                );
                snapshot["production_ticket"] = match ticket {
                    Ok(Some(ticket)) => {
                        // Probe the old canonical operand and the repaired raw
                        // operand under the unchanged six identity predicates and
                        // stale-state fence. This observation never rewrites state.
                        let (identity_rows, canonical_matches, raw_matches): (i64, i64, i64) =
                            tx.query_row(
                                "SELECT count(*),COALESCE(MAX(clean_state_json=?7),0),
                                        COALESCE(MAX(clean_state_json=?8),0)
                                 FROM workspace_leases
                                 WHERE lease_id=?1 AND registration_id=?2 AND registration_generation=?3
                                   AND generation=?4 AND binding_digest=?5 AND operation_id=?6
                                   AND state='stale'",
                                params![
                                    ticket.reservation.lease_id,
                                    ticket.registration.registration_id,
                                    ticket.registration.generation,
                                    ticket.reservation.generation,
                                    ticket.reservation.intent_digest,
                                    ticket.plan.operation_id,
                                    canonical,
                                    clean_raw,
                                ],
                                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                            )?;
                        json!({
                            "status":"available",
                            "lease_state":ticket.state,
                            "original_registration_generation":ticket.registration.generation,
                            "cas_predicates":{
                                "identity_and_stale_state_rows":identity_rows,
                                "clean_state_json_equals_canonical_prior_json":canonical_matches == 1,
                                "clean_state_json_equals_raw_retained_json":raw_matches == 1,
                            },
                        })
                    }
                    Ok(None) => json!({"status":"unavailable"}),
                    Err(error) => {
                        json!({"status":"error","code":error.code,"message":error.message})
                    }
                };
                tx.commit()?;
                Ok(snapshot)
            })
            .await
    }

    #[tokio::test]
    async fn revoked_unknown_workspace_lease_resolves_only_after_exact_git_readback() {
        const OPERATION_ID: &str = "c34-revoked-workspace-readback";
        const PROJECT_ID: &str = "c34-revoked-workspace-project";
        const TASK_ID: &str = "c34-revoked-workspace-task";
        const CANONICAL_REPOSITORY: &str = "github.com/owner/c34-revoked-workspace";
        const REMOTE_URL: &str = "https://github.com/owner/c34-revoked-workspace.git";

        let fixture_root =
            std::env::temp_dir().join(format!("swarm-c34-revoked-readback-{}", model::new_id()));
        let repository = fixture_root.join("repository");
        let workspace_root = fixture_root.join("workspaces");
        let empty_hooks = fixture_root.join("empty-hooks");
        let data_directory = fixture_root.join("store");
        for directory in [&repository, &workspace_root, &empty_hooks, &data_directory] {
            std::fs::create_dir_all(directory).expect("create isolated fixture directory");
            crate::platform::private_owned_directory(directory)
                .expect("set fixture directory owner and restrict it to its workspace owner");
        }
        let repository = std::fs::canonicalize(repository).expect("canonicalize fixture repo");
        let workspace_root =
            std::fs::canonicalize(workspace_root).expect("canonicalize fixture workspace root");
        let empty_hooks = std::fs::canonicalize(empty_hooks).expect("canonicalize hooks path");
        let git = fixture_git_executable();
        run_fixture_git(&git, &repository, &empty_hooks, &["init", "--quiet"]);
        run_fixture_git(
            &git,
            &repository,
            &empty_hooks,
            &["config", "user.name", "C34 Readback Fixture"],
        );
        run_fixture_git(
            &git,
            &repository,
            &empty_hooks,
            &["config", "user.email", "c34-readback@example.invalid"],
        );
        std::fs::create_dir_all(repository.join("src")).expect("create fixture source directory");
        std::fs::write(repository.join("src/lib.rs"), "pub fn retained() {}\n")
            .expect("write the exact scoped source file");
        run_fixture_git(&git, &repository, &empty_hooks, &["add", "src/lib.rs"]);
        run_fixture_git(
            &git,
            &repository,
            &empty_hooks,
            &["commit", "--quiet", "-m", "C34 workspace readback fixture"],
        );
        run_fixture_git(&git, &repository, &empty_hooks, &["branch", "-M", "main"]);
        run_fixture_git(
            &git,
            &repository,
            &empty_hooks,
            &["remote", "add", "origin", REMOTE_URL],
        );
        let baseline_commit = run_fixture_git(
            &git,
            &repository,
            &empty_hooks,
            &["rev-parse", "--verify", "HEAD^{commit}"],
        );

        let registration_id = format!("{OPERATION_ID}-registration");
        let lease_id = format!("{OPERATION_ID}-lease");
        let client_request_id = format!("{OPERATION_ID}-request");
        let plan_digest = format!("sha256:{}", "a".repeat(64));
        let branch_ref = format!("refs/heads/codex/swarm/{lease_id}");
        let branch_name = format!("codex/swarm/{lease_id}");
        let worktree_handle = format!("wt-{lease_id}");
        let workspace_path = workspace_root.join(&worktree_handle);
        let allowed_paths = vec!["src/lib.rs".to_owned()];

        let mut config = Config::default();
        config.forge.enabled = true;
        config.forge.git_executable = git.clone();
        config.forge.projects.insert(
            PROJECT_ID.to_owned(),
            ForgeProject {
                canonical_repository: CANONICAL_REPOSITORY.to_owned(),
                repository_path: repository.clone(),
                remote_name: "origin".to_owned(),
                policy_revision: "c34-readback-v1".to_owned(),
                target_refs: vec!["refs/heads/main".to_owned()],
            },
        );
        config.workspace.projects.insert(
            PROJECT_ID.to_owned(),
            WorkspaceProjectConfig {
                allowed_roots: vec![workspace_root.clone()],
            },
        );
        let registration = WorkspaceRegistration::from_config(
            PROJECT_ID,
            &config.workspace,
            &config.forge,
            registration_id.clone(),
            1,
        )
        .expect("derive the original exact registration generation");
        // The Store configuration represents the post-revocation state. The
        // retained database row carries the original registration snapshot.
        config.workspace.projects.remove(PROJECT_ID);
        let (owner, _credential, operator) =
            start_store_with_config(&data_directory, config.clone()).await;

        let task_spec = TaskSpec {
            acceptance: None,
            objective: "Reconcile one revoked workspace lease".to_owned(),
            phase: "implementation".to_owned(),
            requirements: vec![Requirement {
                id: "R1".to_owned(),
                statement: "Retain the exact historical unknown launch result".to_owned(),
                verification: None,
            }],
            dependencies: Vec::new(),
            scope: Some(Scope {
                initial_paths: allowed_paths.clone(),
                forbidden_paths: Vec::new(),
                prerequisite_policy: None,
            }),
            source_refs: Vec::new(),
            owner_policy_id: Some(crate::policy::OWNER_POLICY_V1_ID.to_owned()),
            source_index: Vec::new(),
            baseline_candidate_ref: None,
        };
        task_spec
            .validate()
            .expect("validate exact retained Task scope");
        let spec_json = model::canonical(&json!(task_spec)).expect("serialize retained Task spec");
        let request = json!({
            "client_request_id":client_request_id,
            "plan_digest":plan_digest,
            "task_id":TASK_ID,
            "expected_task_revision":1,
            "route":"c34-readback-route",
            "agent_profile":"c34-readback-agent",
            "mcp_profile":"c34-readback-profile",
            "mcp_surface":"participant-core",
            "workspace_policy":"c34-readback-policy",
            "requested_model":Value::Null,
            "requested_effort":Value::Null,
            "budget":{"max_turns":Value::Null,"max_duration_ms":Value::Null,"max_cost_units":Value::Null},
            "stop_conditions":[],
            "purpose":"post-revocation workspace readback fixture",
        });
        let request_preview = json!({
            "task_id":TASK_ID,
            "expected_task_revision":1,
            "route":"c34-readback-route",
            "agent_profile":"c34-readback-agent",
            "mcp_profile":"c34-readback-profile",
            "mcp_surface":"participant-core",
            "workspace_policy":"c34-readback-policy",
            "requested_model":Value::Null,
            "requested_effort":Value::Null,
            "budget":{"max_turns":Value::Null,"max_duration_ms":Value::Null,"max_cost_units":Value::Null},
            "stop_conditions":[],
            "purpose":"post-revocation workspace readback fixture",
        });
        let actor = json!({
            "kind":"direct",
            "client_id":operator.client_id,
            "role":"operator",
            "link_id":operator.link_id,
        });
        // Production readback authority re-verifies the exact retained
        // terminal launch state. The seed therefore carries the same
        // post-failure manifest that the unknown effect produced, rather
        // than the pre-effect pending snapshot.
        let manifest = json!({
            "manifest_version":"eliot-launch-manifest-v1",
            "state":"outcome_unknown",
            "plan_digest":plan_digest,
            "request":request_preview,
            "actor":actor,
            "task":{
                "task_id":TASK_ID,
                "project_id":PROJECT_ID,
                "expected_revision":1,
                "observed_revision":1,
                "attempt_id":Value::Null,
            },
            "runtime":{"native_effect":"unknown","route":{"alias":"c34-readback-route"}},
            "progress":{"workspace_lease":"readback_required"},
            "failure":{"code":"workspace_effect_unknown"},
            "effects":"workspace_or_binding_effect_requires_exact_readback",
        });
        let effective = json!({
            "operation_contract":{
                "effect_scope":"one_exact_launch_plan",
                "completion_condition":"workspace_binding_dispatch_and_capability_readback",
                "replay_policy":"same_request_id_returns_retained_launch_receipt; unknown_effects_require_readback",
                "contract_revision":"swarm-launch-v1",
            },
            "launch_manifest":manifest,
            "launch_plan_authority":{},
        });
        let retained_result = json!({
            "operation_id":OPERATION_ID,
            "launch_state":"outcome_unknown",
            "state":"outcome_unknown",
            "plan_digest":plan_digest,
            "task_id":TASK_ID,
            "task_revision":1,
            "attempt_id":Value::Null,
            "native_effect":"unknown",
            "failure":{"code":"workspace_effect_unknown"},
            "gaps":["launch_progress_requires_operator_readback_or_configuration_repair"],
        });
        let intent_digest = format!(
            "sha256:{}",
            model::digest(
                model::canonical(&json!({
                    "version":1,
                    "lease_id":lease_id,
                    "registration_id":registration.registration_id,
                    "registration_generation":registration.generation,
                    "registration_digest":registration.registration_digest,
                    "generation":1,
                    "project_id":PROJECT_ID,
                    "task_id":TASK_ID,
                    "task_revision":1,
                    "operation_id":OPERATION_ID,
                    "plan_digest":plan_digest,
                    "owner_client_id":operator.client_id,
                    "attempt_id":Value::Null,
                    "allowed_paths":allowed_paths,
                    "allowed_symbols":Vec::<String>::new(),
                    "expected_baseline_commit":baseline_commit,
                    "branch_ref":branch_ref,
                    "worktree_handle":worktree_handle,
                    "workspace_path":workspace_path.to_str().expect("UTF-8 worktree path"),
                }))
                .expect("canonicalize exact lease intent")
                .as_bytes()
            )
        );
        owner
            .store
            .run({
                let registration = registration.clone();
                let workspace_path = workspace_path.clone();
                let allowed_paths = allowed_paths.clone();
                let request = request.clone();
                let effective = effective.clone();
                let retained_result = retained_result.clone();
                let intent_digest = intent_digest.clone();
                let baseline_commit = baseline_commit.clone();
                let branch_ref = branch_ref.clone();
                let worktree_handle = worktree_handle.clone();
                let operator = operator.clone();
                move |db| {
                    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    let now = model::now_ms()?;
                    super::super::set_meta(
                        &tx,
                        "execution_mode",
                        &json!({"new_work":"enabled"}),
                    )?;
                    tx.execute(
                        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms)
                         VALUES(?1,?2,1,'open',?3,?4,?4)",
                        params![TASK_ID, PROJECT_ID, spec_json, now],
                    )?;
                    tx.execute(
                        "INSERT INTO workspace_registrations(
                            registration_id,project_id,trusted_repository,repository_path,
                            allowed_roots_json,registration_digest,generation,authorized_by,
                            state,created_at_ms,updated_at_ms
                         ) VALUES(?1,?2,?3,?4,?5,?6,1,?7,'active',?8,?8)",
                        params![
                            registration.registration_id,
                            PROJECT_ID,
                            registration.trusted_repository,
                            registration.repository_path.to_str().expect("UTF-8 repository path"),
                            model::canonical(&json!(registration.allowed_roots.iter().map(|path| path.to_str().expect("UTF-8 workspace root")).collect::<Vec<_>>()))?,
                            registration.registration_digest,
                            operator.client_id,
                            now,
                        ],
                    )?;
                    tx.execute(
                        "INSERT INTO operations(
                            operation_id,caller_id,client_request_id,method,
                            original_request_json,effective_request_json,task_id,attempt_id,
                            state,result_json,due_at_ms,created_at_ms,updated_at_ms
                         ) VALUES(?1,?2,?3,'swarm.launch',?4,?5,?6,NULL,?9,?7,?8,?8,?8)",
                        params![
                            OPERATION_ID,
                            operator.client_id,
                            client_request_id,
                            model::canonical(&request)?,
                            model::canonical(&effective)?,
                            TASK_ID,
                            model::canonical(&retained_result)?,
                            now,
                            "outcome_unknown",
                        ],
                    )?;
                    tx.execute(
                        "INSERT INTO workspace_leases(
                            lease_id,registration_id,registration_generation,project_id,
                            task_id,task_revision,operation_id,plan_digest,owner_client_id,
                            allowed_paths_json,allowed_symbols_json,baseline_commit,branch_ref,
                            worktree_handle,workspace_path,clean_state_json,generation,
                            binding_digest,state,created_at_ms,updated_at_ms
                         ) VALUES(?1,?2,1,?3,?4,1,?5,?6,?7,?8,'[]',?9,?10,?11,?12,'{}',1,?13,'preparing',?14,?14)",
                        params![
                            lease_id,
                            registration.registration_id,
                            PROJECT_ID,
                            TASK_ID,
                            OPERATION_ID,
                            plan_digest,
                            operator.client_id,
                            model::canonical(&json!(allowed_paths))?,
                            baseline_commit,
                            branch_ref,
                            worktree_handle,
                            workspace_path.to_str().expect("UTF-8 worktree path"),
                            intent_digest,
                            now,
                        ],
                    )?;
                    tx.commit()?;
                    Ok(())
                }
            })
            .await
            .expect("seed the exact preparing lease and retained unknown launch");

        let seeded_state =
            revoked_workspace_readback_diagnostic(&owner.store, &config, OPERATION_ID)
                .await
                .expect("read the exact seeded immutable workspace identity");

        let workspace_git_path = fixture_git_path_argument(&workspace_path);
        run_fixture_git(
            &git,
            &repository,
            &empty_hooks,
            &[
                "worktree",
                "add",
                "-b",
                branch_name.as_str(),
                "--",
                workspace_git_path.as_str(),
                baseline_commit.as_str(),
            ],
        );
        assert_eq!(
            std::fs::canonicalize(Path::new(&workspace_git_path))
                .expect("canonicalize normalized Git worktree path"),
            std::fs::canonicalize(&workspace_path)
                .expect("canonicalize retained lease worktree path"),
            "normalized Git path must resolve to the exact canonical workspace path"
        );
        owner
            .store
            .record_launch_failure(OPERATION_ID.to_owned(), "WORKSPACE_GIT_UNKNOWN".to_owned())
            .await
            .expect("retain the real launch as outcome unknown before revocation");
        let before_revocation =
            revoked_workspace_readback_diagnostic(&owner.store, &config, OPERATION_ID)
                .await
                .expect("read retained identity after the initial unknown failure");
        owner
            .store
            .initialize_workspace_authority()
            .await
            .expect("revoke the exact original registration generation");

        let before_reconcile =
            revoked_workspace_readback_diagnostic(&owner.store, &config, OPERATION_ID)
                .await
                .expect("read raw revoked state before filesystem reconciliation");

        let reconciled = owner
            .store
            .reconcile_launches_once()
            .await
            .expect("run the production Store selector and filesystem readback producer");
        // This boundary is after the production caller's later record_launch_failure,
        // if any. An uncommitted filesystem/persistence transaction is not observed.
        let after_reconcile =
            revoked_workspace_readback_diagnostic(&owner.store, &config, OPERATION_ID)
                .await
                .expect("read raw state after reconciliation and its retained failure");
        let diagnostic = json!({
            "immutable_identity_matches_seed":{
                "after_initial_failure":before_revocation["identity"] == seeded_state["identity"],
                "before_reconcile":before_reconcile["identity"] == seeded_state["identity"],
                "after_reconcile_and_failure":after_reconcile["identity"] == seeded_state["identity"],
            },
            "seeded_state":seeded_state,
            "after_initial_failure_before_revocation":before_revocation,
            "before_filesystem_reconcile":before_reconcile,
            "after_reconcile_and_record_launch_failure":after_reconcile,
        });
        let retained_readback = public_operation_read(&owner.store, &operator, OPERATION_ID)
            .await
            .expect("read exact post-revocation launch diagnostics");
        assert_eq!(
            reconciled["progressed"], 1,
            "reconcile={reconciled}, operation={retained_readback}, raw_readback_diagnostic={diagnostic}"
        );
        // This count describes retained launch outcomes, not live writers.
        // Filesystem proof must not invent a terminal for the old Operation.
        assert_eq!(reconciled["outcome_unknown"], 1);
        let proof_before_lifecycle = owner
            .store
            .run(move |db| {
                let (lease_state, clean_state_raw, operation_state): (String, String, String) = db
                    .query_row(
                        "SELECT l.state,l.clean_state_json,o.state
                         FROM workspace_leases l JOIN operations o USING(operation_id)
                         WHERE l.operation_id=?1",
                        [OPERATION_ID],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )?;
                Ok(json!({
                    "lease_state":lease_state,
                    "clean_state":serde_json::from_str::<Value>(&clean_state_raw)?,
                    "operation_state":operation_state,
                }))
            })
            .await
            .expect("read the actual Store-produced post-revocation proof");
        assert_eq!(proof_before_lifecycle["lease_state"], "stale");
        assert_eq!(proof_before_lifecycle["operation_state"], "outcome_unknown");
        assert_eq!(
            proof_before_lifecycle["clean_state"]["post_revocation_readback"]["status"],
            "verified"
        );
        assert_eq!(
            proof_before_lifecycle["clean_state"]["post_revocation_readback"]["filesystem_readback"]
                ["status"],
            "reconciled_clean"
        );

        let sweep = owner
            .store
            .reconcile_workspace_lifecycle_once()
            .await
            .expect("run the existing lifecycle proof consumer");
        assert_eq!(sweep["released"], 1);
        let final_state =
            owner
                .store
                .run(move |db| {
                    let (lease_state, clean_state_raw, registration_state, registration_generation):
                    (String, String, String, i64) = db.query_row(
                    "SELECT l.state,l.clean_state_json,r.state,r.generation
                     FROM workspace_leases l
                     JOIN workspace_registrations r ON r.registration_id=l.registration_id
                     WHERE l.operation_id=?1",
                    [OPERATION_ID],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )?;
                    Ok(json!({
                        "lease_state":lease_state,
                        "clean_state":serde_json::from_str::<Value>(&clean_state_raw)?,
                        "registration_state":registration_state,
                        "registration_generation":registration_generation,
                    }))
                })
                .await
                .expect("read the exact lifecycle decision");
        let receipt = public_operation_read(&owner.store, &operator, OPERATION_ID)
            .await
            .expect("read the unchanged historical launch result");
        assert_eq!(final_state["lease_state"], "released");
        assert_eq!(final_state["registration_state"], "revoked");
        assert_eq!(final_state["registration_generation"], 2);
        let lifecycle = &final_state["clean_state"]["lease_lifecycle"];
        // The first lifecycle event is stored directly; subsequent events
        // retain previous/latest history through the same production writer.
        let latest = lifecycle.get("latest").unwrap_or(lifecycle);
        assert_eq!(
            latest["reason_code"],
            "revoked_unknown_workspace_readback_verified"
        );
        assert_eq!(
            latest["native_effect_status"],
            "workspace_readback_verified_operation_outcome_unknown_retained"
        );
        assert_eq!(receipt["state"], "outcome_unknown");
        assert_eq!(
            receipt["result"]["failure"]["code"],
            "workspace_effect_unknown"
        );
        assert!(
            workspace_path.is_dir(),
            "readback must not delete the worktree"
        );

        owner.close().await.expect("close Store owner");
        std::fs::remove_dir_all(&fixture_root).expect("remove the exact fixture directory");
    }
}
