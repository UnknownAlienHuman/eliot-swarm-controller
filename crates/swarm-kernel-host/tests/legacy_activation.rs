//! Retained domain damage crosses the real host activation and IPC boundary.
#![cfg(any(target_os = "windows", target_os = "linux"))]

use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use swarm_kernel_host::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    config::Config,
    ipc,
    model::{self, Credential},
    platform::{DataRoot, bootstrap_credential},
    policy::OWNER_POLICY_V2_ID,
    store::StoreOwner,
};

const CHECK_DAMAGE: &str = "{legacy-host-check-damage";
const LAUNCH_DAMAGE: &str = "{legacy-host-launch-damage";
const ENTRY_DAMAGE: &str = "{legacy-host-entry-damage";

struct Fixture {
    directory: PathBuf,
    config: Config,
    config_path: PathBuf,
    credential: Credential,
    check_id: String,
    launch_id: String,
    entry_key: String,
}

impl Fixture {
    async fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("legacy-activation-{}", model::new_id()));
        fs::create_dir(&directory).unwrap();
        let directory = fs::canonicalize(directory).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = directory.join("state");
        config.checks.enabled = false;
        config.routes.clear();
        config.schedules.clear();
        config.forge.enabled = false;
        config.automation_scheduler.enabled = false;
        let root = DataRoot::acquire(&config.storage.data_dir).unwrap();
        let credential = bootstrap_credential(&root.path).unwrap();
        let owner = StoreOwner::start(root, Arc::new(config.clone()), credential.clone())
            .await
            .unwrap();
        let operator = owner.store.authenticate(credential.clone()).await.unwrap();
        let manager_credential = Credential {
            client_id: format!("activation-manager-{}", model::new_id()),
            token: model::new_id().repeat(2),
        };
        owner.store.call(operator, "client.register".to_owned(), json!({
            "client_request_id":model::new_id(), "client_id":manager_credential.client_id,
            "role":"manager", "token_hash":model::digest(manager_credential.token.as_bytes()),
        })).await.unwrap();
        let manager = owner.store.authenticate(manager_credential).await.unwrap();
        let task = owner.store.call(manager.clone(), "task.create".to_owned(), json!({
            "client_request_id":model::new_id(), "project_id":"legacy-host-fixture",
            "spec":{"objective":"Retain a damaged CheckRun for exact isolation","phase":"implementation","owner_policy_id":OWNER_POLICY_V2_ID,
                "requirements":[{"id":"isolate","statement":"Retained damage creates no CheckRun effect"}]},
        })).await.unwrap();
        let attempt = owner
            .store
            .call(
                manager.clone(),
                "task.claim".to_owned(),
                json!({
                    "client_request_id":model::new_id(), "task_id":task["task_id"],
                    "expected_revision":1, "start_owner":"native_manager",
                }),
            )
            .await
            .unwrap();
        owner.close().await.unwrap();

        let check_id = model::new_id();
        let check_operation_id = model::new_id();
        let launch_id = model::new_id();
        let scope_digest = model::digest(
            model::canonical(&json!([credential.client_id, "legacy-host-fixture"]))
                .unwrap()
                .as_bytes(),
        );
        let entry_key = format!("automation:v1:entry:{scope_digest}:damaged-entry");
        let candidate_bytes = b"retained CheckRun damage fixture";
        let candidate_id = format!("source-{}", model::digest(candidate_bytes));
        let candidate = ArtifactRecord {
            artifact_id: candidate_id.clone(),
            relative_path: format!("artifacts/{candidate_id}.bin"),
            kind: "source_snapshot".to_owned(),
            byte_length: candidate_bytes.len() as u64,
            content_digest: model::digest(candidate_bytes),
            metadata: json!({"attempt_id":attempt["attempt_id"],"task_revision":1,"fixture":"retained CheckRun damage"}),
        };
        ArtifactFiles::new(&config.storage.data_dir)
            .unwrap()
            .publish(&candidate, candidate_bytes)
            .unwrap();
        let mut db = Connection::open(config.storage.data_dir.join("swarm.db")).unwrap();
        // Inject only into this fresh, closed Store. These exact invalid retained
        // bytes model disk damage; they are never submitted as admitted work.
        db.pragma_update(None, "ignore_check_constraints", true)
            .unwrap();
        let tx = db.transaction().unwrap();
        tx.execute(
            "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,?3,?4,?5,1,?6)",
            params![candidate.artifact_id, candidate.relative_path, candidate.kind, i64::try_from(candidate.byte_length).unwrap(), candidate.content_digest, model::canonical(&candidate.metadata).unwrap()],
        ).unwrap();
        tx.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,due_at_ms,created_at_ms,updated_at_ms) \
             VALUES(?1,?2,?1,'check.run','{}','{}',?3,?4,'queued',1,1,1)",
            params![check_operation_id, manager.client_id, task["task_id"].as_str().unwrap(), attempt["attempt_id"].as_str().unwrap()],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO check_runs(check_id,operation_id,attempt_id,candidate_ref,cache_key,resource_key,spec_json,state,created_at_ms) \
             VALUES(?1,?2,?3,?4,'damaged-cache','damaged-resource',?5,'queued',1)",
            params![check_id, check_operation_id, attempt["attempt_id"].as_str().unwrap(), candidate_id, CHECK_DAMAGE],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
             VALUES(?1,?2,?1,'swarm.launch','{}',?3,'queued',1,1,1)",
            params![launch_id, credential.client_id, LAUNCH_DAMAGE],
        )
        .unwrap();
        tx.execute(
            "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
            params![entry_key, ENTRY_DAMAGE],
        )
        .unwrap();
        tx.commit().unwrap();
        db.pragma_update(None, "ignore_check_constraints", false)
            .unwrap();
        drop(db);
        let config_path = directory.join("controller.toml");
        fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();
        Self {
            directory,
            config,
            config_path,
            credential,
            check_id,
            launch_id,
            entry_key,
        }
    }

    fn start(&self) -> HostChild {
        let mut command = Command::new(env!("CARGO_BIN_EXE_swarm-kernel-host"));
        command
            .arg("--config")
            .arg(&self.config_path)
            .args(["host", "--stop-on-stdin-eof"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(fs::File::create(self.directory.join("host.stderr")).unwrap());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        HostChild(command.spawn().unwrap())
    }

    async fn call(&self, method: &str, request: Value) -> Value {
        tokio::time::timeout(
            Duration::from_secs(5),
            ipc::call(
                &self.config.storage.data_dir,
                &self.credential,
                method,
                request,
                &self.config.ipc,
            ),
        )
        .await
        .expect("the real host IPC listener remains responsive")
        .unwrap()
    }

    fn quarantine(&self) -> Vec<(String, String)> {
        let db = Connection::open(self.config.storage.data_dir.join("swarm.db")).unwrap();
        db.busy_timeout(Duration::from_secs(2)).unwrap();
        let mut statement = db
            .prepare(
                "SELECT key,value_json FROM meta WHERE key LIKE 'checks:pending:v1:quarantine:%' \
                 OR key LIKE 'launcher:v1:quarantine:native_mcp:%' \
                 OR key LIKE 'automation:v1:publication:quarantine:%' ORDER BY key",
            )
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Only this fixture's UUID directory, after its own child is joined.
        if std::thread::panicking() {
            eprintln!("retained failed host fixture: {}", self.directory.display());
            return;
        }
        let _ = fs::remove_dir_all(&self.directory);
    }
}

struct HostChild(Child);

impl HostChild {
    fn assert_running(&mut self) {
        assert!(
            self.0.try_wait().unwrap().is_none(),
            "subject-local damage stopped the real host"
        );
    }

    async fn stop(&mut self) {
        drop(self.0.stdin.take());
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success(), "owned host shutdown failed: {status}");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("owned host joins after stdin EOF");
    }
}

impl Drop for HostChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[tokio::test]
async fn legacy_poison_activation_preserves_real_host_listener_and_exact_quarantine() {
    let fixture = Fixture::new().await;
    let mut child = fixture.start();
    let markers = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            child.assert_running();
            let markers = fixture.quarantine();
            if markers.iter().any(|(key, _)| {
                key == &format!(
                    "checks:pending:v1:quarantine:{}",
                    model::digest(fixture.check_id.as_bytes())
                )
            }) && markers
                .iter()
                .any(|(key, _)| key.starts_with("launcher:v1:quarantine:native_mcp:"))
                && markers
                    .iter()
                    .any(|(key, _)| key.starts_with("automation:v1:publication:quarantine:"))
            {
                break markers;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("each lazy worker reaches its existing exact quarantine owner");
    let status = fixture.call("host.status", json!({})).await;
    assert_eq!(status["host_lifecycle"]["current"]["state"], "running");

    let admitted = fixture
        .call(
            "task.create",
            json!({"client_request_id":model::new_id(),"project_id":"unrelated-live-ipc",
                "spec":{"objective":"Independent work survives retained damage","phase":"implementation",
                    "requirements":[{"id":"alive","statement":"The real host admits unrelated work"}]}}),
        )
        .await;
    assert!(admitted["task_id"].as_str().is_some());
    for _ in 0..10 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        child.assert_running();
        assert_eq!(
            fixture.quarantine(),
            markers,
            "unchanged evidence is write-stable"
        );
        let status = fixture.call("host.status", json!({})).await;
        assert_eq!(status["host_lifecycle"]["current"]["state"], "running");
    }

    let db = Connection::open(fixture.config.storage.data_dir.join("swarm.db")).unwrap();
    for (query, identity, expected) in [
        (
            "SELECT spec_json FROM check_runs WHERE check_id=?1",
            &fixture.check_id,
            CHECK_DAMAGE,
        ),
        (
            "SELECT effective_request_json FROM operations WHERE operation_id=?1",
            &fixture.launch_id,
            LAUNCH_DAMAGE,
        ),
        (
            "SELECT value_json FROM meta WHERE key=?1",
            &fixture.entry_key,
            ENTRY_DAMAGE,
        ),
    ] {
        let retained: String = db.query_row(query, [identity], |row| row.get(0)).unwrap();
        assert_eq!(
            retained, expected,
            "quarantine preserves exact source bytes"
        );
    }
    drop(db);
    child.stop().await;
}
