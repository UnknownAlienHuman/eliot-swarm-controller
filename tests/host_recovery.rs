//! Exercise a real host process, its durable Store, and a replacement manager.
use eliot_swarm_controller::{
    config::Config,
    ipc,
    model::{self, Credential},
    platform::{DataRoot, bootstrap_credential},
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Fixture {
    directory: PathBuf,
    config_path: PathBuf,
    config: Config,
    operator: Credential,
}

fn kernel_host_executable() -> PathBuf {
    let compatibility_launcher = PathBuf::from(env!("CARGO_BIN_EXE_swarm-host"));
    compatibility_launcher.with_file_name(if cfg!(windows) {
        "swarm-kernel-host.exe"
    } else {
        "swarm-kernel-host"
    })
}

impl Fixture {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("swarm-recovery-{}", model::new_id()));
        fs::create_dir(&directory).unwrap();
        let directory = fs::canonicalize(directory).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = directory.join("state");
        let root = DataRoot::acquire(&config.storage.data_dir).unwrap();
        let operator = bootstrap_credential(&root.path).unwrap();
        drop(root);
        let config_path = directory.join("config.toml");
        fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();
        Self {
            directory,
            config_path,
            config,
            operator,
        }
    }

    fn start(&self, sequence: u8) -> HostChild {
        // The wrapper is a public compatibility coordinate. Start the actual
        // kernel image so crash/EOF ownership and PID assertions cover the
        // process that owns DataRoot, Store and IPC.
        let mut command = Command::new(kernel_host_executable());
        command
            .arg("--config")
            .arg(&self.config_path)
            .args(["host", "--stop-on-stdin-eof"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(
                fs::File::create(self.directory.join(format!("host-{sequence}.stderr"))).unwrap(),
            );
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x08000000);
        }
        HostChild(command.spawn().unwrap())
    }

    async fn call(&self, credential: &Credential, method: &str, params: Value) -> Value {
        tokio::time::timeout(
            Duration::from_secs(5),
            ipc::call(
                &self.config.storage.data_dir,
                credential,
                method,
                params,
                &self.config.ipc,
            ),
        )
        .await
        .expect("host request timed out")
        .unwrap()
    }

    async fn ready(&self, child: &mut HostChild) -> Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "private test host exited during startup"
            );
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                ipc::call(
                    &self.config.storage.data_dir,
                    &self.operator,
                    "host.status",
                    json!({}),
                    &self.config.ipc,
                ),
            )
            .await;
            if let Ok(Ok(status)) = result
                && status["host_lifecycle"]["current"]["state"] == "running"
            {
                return status;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "private test host readiness timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // This fixture created the UUID directory; all its child handles have
        // been dropped first. Never enumerate or terminate unrelated hosts.
        let _ = fs::remove_dir_all(&self.directory);
    }
}

struct HostChild(Child);

impl HostChild {
    fn crash(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }

    async fn stop(&mut self) {
        drop(self.0.stdin.take());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "private test host failed graceful shutdown"
                );
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "private test host shutdown timed out"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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
async fn interrupted_host_retains_task_and_operation_for_successor_manager() {
    let fixture = Fixture::new();
    let mut first = fixture.start(1);
    let initial_status = fixture.ready(&mut first).await;
    let first_epoch = initial_status["host_epoch"].as_i64().unwrap();
    let manager_a = Credential {
        client_id: "manager-a".into(),
        token: model::new_id().repeat(2),
    };
    let manager_b = Credential {
        client_id: "manager-b".into(),
        token: model::new_id().repeat(2),
    };
    for credential in [&manager_a, &manager_b] {
        fixture
            .call(
                &fixture.operator,
                "client.register",
                json!({
                    "client_request_id": model::new_id(), "client_id": credential.client_id,
                    "role": "manager", "token_hash": model::digest(credential.token.as_bytes()),
                }),
            )
            .await;
    }
    fixture
        .call(
            &fixture.operator,
            "gm.handover",
            json!({
                "client_request_id": model::new_id(), "client_id": manager_a.client_id,
            }),
        )
        .await;
    let request = json!({
        "client_request_id": model::new_id(), "project_id": "recovery-fixture",
        "spec": {"objective": "Retain admitted work across a host crash", "phase": "implementation",
            "requirements": [{"id": "retained", "statement": "Task and Operation survive restart"}]},
    });
    let admission = fixture
        .call(&fixture.operator, "task.create", request.clone())
        .await;
    let operation_id = admission["operation_id"].as_str().unwrap();
    let task_id = admission["task_id"].as_str().unwrap();
    let before = fixture
        .call(
            &manager_a,
            "operation.get",
            json!({"operation_id": operation_id}),
        )
        .await;
    assert_eq!(before["result"], admission);
    let task_before = fixture
        .call(&manager_a, "task.get", json!({"task_id": task_id}))
        .await;

    first.crash();
    let unavailable = ipc::Client::connect(
        &fixture.config.storage.data_dir,
        &manager_b,
        &fixture.config.ipc,
    )
    .await
    .err()
    .expect("killed private host still accepted IPC");
    assert_eq!(unavailable.code, "HOST_UNAVAILABLE");

    let mut second = fixture.start(2);
    let recovered = fixture.ready(&mut second).await;
    assert!(recovered["host_epoch"].as_i64().unwrap() > first_epoch);
    let failure = recovered["host_lifecycle"]["latest_failure"].clone();
    assert_eq!(failure["error_code"], "HOST_INTERRUPTED");
    assert_eq!(failure["host_epoch"], first_epoch);
    assert_eq!(failure["manager_action_required"], true);
    assert_eq!(failure["retry_authorized"], false);
    fixture
        .call(
            &fixture.operator,
            "gm.handover",
            json!({
                "client_request_id": model::new_id(), "client_id": manager_b.client_id,
            }),
        )
        .await;
    assert_eq!(
        fixture
            .call(
                &manager_b,
                "operation.get",
                json!({"operation_id": operation_id})
            )
            .await,
        before
    );
    assert_eq!(
        fixture
            .call(&manager_b, "task.get", json!({"task_id": task_id}))
            .await,
        task_before
    );
    assert_eq!(
        fixture
            .call(&fixture.operator, "task.create", request)
            .await,
        admission
    );
    assert_eq!(
        fixture.call(&manager_b, "host.status", json!({})).await["tasks"],
        1
    );
    second.stop().await;

    let mut third = fixture.start(3);
    let restarted = fixture.ready(&mut third).await;
    assert_eq!(
        restarted["host_lifecycle"]["last_exit"]["error_code"],
        Value::Null
    );
    assert_eq!(
        restarted["host_lifecycle"]["last_exit"]["manager_action_required"],
        false
    );
    assert_eq!(restarted["host_lifecycle"]["latest_failure"], failure);
    assert_eq!(
        fixture
            .call(&manager_b, "task.get", json!({"task_id": task_id}))
            .await,
        task_before
    );
    third.stop().await;
}
