use super::*;
use serde_json::json;
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

const BINDING_ID: &str = "fixture-binding";
const GENERATION: i64 = 1;
const NATIVE_SCOPE: &str = "fixture-scope";
const ROUTE_SHA256: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new() -> Self {
        Self(
            std::env::temp_dir().join(format!("swarm-opencode-retention-{}", uuid::Uuid::new_v4())),
        )
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct PreserveOnFailureRoot(PathBuf, bool);

impl PreserveOnFailureRoot {
    fn new() -> Self {
        Self(
            std::env::temp_dir().join(format!(
                "swarm-opencode-native-startup-{}",
                uuid::Uuid::new_v4()
            )),
            false,
        )
    }
}

impl Drop for PreserveOnFailureRoot {
    fn drop(&mut self) {
        if self.1 {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}

fn fixture_intent(operation_id: &str, method: &str) -> OperationIntent {
    let module_receipt: swarm_contracts::runtime::ModuleReceiptIdentity =
        serde_json::from_value(json!({
            "schema_version":1,
            "module_id":"eliot.opencode",
            "artifact":{"artifact_id":"fixture-opencode","version":"0.1.0"},
            "protocol":{"major":1,"minor":0},
            "binding_id":BINDING_ID,
            "binding_generation":GENERATION,
            "operation_id":operation_id,
            "input_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        }))
        .unwrap();
    OperationIntent {
        version: 2,
        operation_id: operation_id.to_owned(),
        module_receipt,
        method: method.to_owned(),
        binding_id: BINDING_ID.to_owned(),
        generation: GENERATION,
        native_scope_key: NATIVE_SCOPE.to_owned(),
        native_root_id: None,
        native_input_id: None,
        prompt_sha256: None,
        prompt_bytes: None,
        reconcile_target_operation_id: None,
        result_input_status: None,
        result_assistant: None,
        dispatch_admission: None,
        route_sha256: ROUTE_SHA256.to_owned(),
        model: json!({"id":"fixture-model"}),
        marker: json!({"fixture":"retention"}),
    }
}

fn write_acknowledged_reply(journal: &Journal, operation_id: &str, resolved: Option<bool>) {
    let intent = fixture_intent(operation_id, "agent.reply");
    journal.write_intent(&intent).unwrap();
    let details = resolved.map_or_else(|| json!({}), |value| json!({"resolved":value}));
    let outcome = json!({
        "operation_id":operation_id,
        "outcome":"applied",
        "native_scope_key":NATIVE_SCOPE,
        "details":details
    });
    journal.queue_outcome_value(&outcome).unwrap();
    journal.acknowledge_outcome(&outcome).unwrap();
    journal
        .remove_pending(
            &journal
                .outbox
                .join(format!("outcome-{}.json", operation_key(operation_id))),
        )
        .unwrap();
}

fn write_legacy_acknowledged_reply(journal: &Journal, operation_id: &str) {
    write_legacy_acknowledged_reply_for_scope(journal, operation_id, NATIVE_SCOPE, ROUTE_SHA256);
}

fn write_legacy_acknowledged_reply_for_scope(
    journal: &Journal,
    operation_id: &str,
    native_scope: &str,
    route_sha256: &str,
) {
    let mut intent = fixture_intent(operation_id, "agent.reply");
    intent.native_scope_key = native_scope.to_owned();
    intent.route_sha256 = route_sha256.to_owned();
    let outcome = json!({
        "operation_id":operation_id,
        "outcome":"applied",
        "native_scope_key":native_scope,
        "details":{}
    });
    let outcome_sha256 = digest_json(&outcome).unwrap();
    let records = [
        JournalRecord {
            version: 2,
            operation_id: operation_id.to_owned(),
            kind: "intent".to_owned(),
            intent: Some(intent),
            outcome: None,
            outcome_sha256: None,
            result_params: None,
            result_sha256: None,
        },
        JournalRecord {
            version: 2,
            operation_id: operation_id.to_owned(),
            kind: "outcome".to_owned(),
            intent: None,
            outcome: Some(outcome),
            outcome_sha256: Some(outcome_sha256.clone()),
            result_params: None,
            result_sha256: None,
        },
        JournalRecord {
            version: 2,
            operation_id: operation_id.to_owned(),
            kind: "acknowledged".to_owned(),
            intent: None,
            outcome: None,
            outcome_sha256: Some(outcome_sha256),
            result_params: None,
            result_sha256: None,
        },
    ];
    let mut bytes = Vec::new();
    for record in records {
        bytes.extend(serde_json::to_vec(&record).unwrap());
        bytes.push(b'\n');
    }
    fs::write(
        journal
            .operations
            .join(format!("{}.jsonl", operation_key(operation_id))),
        bytes,
    )
    .unwrap();
}

#[derive(Clone)]
struct StartupJournalProbe {
    root: PathBuf,
    native_scope: String,
    store_unresolved: Option<String>,
}

impl StartupJournalProbe {
    fn saved_cursor(&self) -> Option<serde_json::Value> {
        serde_json::from_slice(&fs::read(self.root.join("retention-cursor.json")).ok()?).ok()
    }

    fn inventory_complete(&self) -> bool {
        self.saved_cursor()
            .is_some_and(|cursor| cursor["startup_complete"] == true)
    }

    fn legacy_migration_complete(&self) -> bool {
        self.saved_cursor().is_some_and(|cursor| {
            cursor["startup_complete"] == true && cursor["legacy_migration_version"] == 1
        })
    }
}

fn startup_fixture_contract() -> swarm_contracts::module_contract::ModuleContractClaim {
    use swarm_contracts::{
        module_catalog::{ModuleDescriptor, ProtocolVersion},
        module_contract::ModuleContractClaim,
    };

    let mut descriptor: serde_json::Value =
        serde_json::from_str(include_str!("../../registration/descriptor.template.json")).unwrap();
    descriptor["launch"]["executable"] = json!(std::env::current_exe().unwrap());
    let descriptor: ModuleDescriptor = serde_json::from_value(descriptor).unwrap();
    ModuleContractClaim::from_descriptor(&descriptor, ProtocolVersion { major: 1, minor: 0 })
        .unwrap()
}

fn startup_fixture_bootstrap(
    root: &Path,
    host_data_dir: &Path,
    state_dir: &Path,
    native_options: &crate::NativeOptions,
    contract: &swarm_contracts::module_contract::ModuleContractClaim,
    boot_id: &str,
) -> crate::OwnedBootstrap {
    crate::OwnedBootstrap {
        config: crate::AdapterConfig {
            schema_version: 1,
            host_data_dir: host_data_dir.to_owned(),
            credential_file: root.join("fixture-credential.json"),
            state_dir: state_dir.to_owned(),
            binding_id: BINDING_ID.to_owned(),
            generation: GENERATION,
            module_artifact_id: crate::ARTIFACT_ID.to_owned(),
            native_options: native_options.clone(),
            owned_native: None,
            ipc: swarm_client::IpcConfig::default(),
        },
        credential: swarm_contracts::Credential {
            client_id: "pr69-native-startup-fixture".to_owned(),
            token: "fixture-token".to_owned(),
        },
        worker: swarm_process::module_owner::VerifiedModuleWorker {
            owner_record: json!({
                "token":"fixture-owner-token",
                "process":{"pid":std::process::id()}
            }),
            boot_id: boot_id.to_owned(),
            process_image_identity: json!({"fixture":"source-run-owned"}),
        },
        contract: contract.clone(),
    }
}

fn startup_fixture_hello(
    config: &crate::AdapterConfig,
    claim: &serde_json::Value,
) -> serde_json::Value {
    json!({
        "binding_id":config.binding_id,
        "generation":config.generation,
        "route":{
            "runtime":crate::RUNTIME,
            "module_artifact_id":crate::ARTIFACT_ID,
            "workspace_option":"directory",
            "native_options":serde_json::to_value(&config.native_options).unwrap()
        },
        "native_root_id":null,
        "native_scope_key":null,
        "module_contract_negotiation":{
            "status":"negotiated",
            "module_id":claim["module_id"],
            "artifact":claim["artifact"],
            "protocol":claim["protocol"],
            "capabilities":claim["capabilities"],
            "config_schema":claim["config_schema"],
            "command_schemas":claim["command_schemas"],
            "event_schemas":claim["event_schemas"],
            "effects_authorized_by_descriptor":false
        }
    })
}

async fn serve_startup_fixture_connection(
    stream: swarm_kernel_host::ipc::Stream,
    hello: serde_json::Value,
    probe: StartupJournalProbe,
    events: mpsc::UnboundedSender<serde_json::Value>,
) -> std::io::Result<()> {
    let (read, mut write) = tokio::io::split(stream);
    let mut reader = BufReader::new(read);
    let mut line = String::new();
    let mut session_boot_id = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Ok(());
        }
        let request: serde_json::Value =
            serde_json::from_str(&line).map_err(std::io::Error::other)?;
        let method = request["method"].as_str().unwrap_or_default();
        let params = request["params"].clone();
        if method == "module.hello" {
            session_boot_id = params["boot_id"].as_str().unwrap_or_default().to_owned();
        }
        let inventory_complete = probe.inventory_complete();
        let legacy_migration_complete = probe.legacy_migration_complete();
        let result = match method {
            "client.hello" => json!({
                "client_id":params["client_id"],
                "role":"module",
                "protocol_version":1
            }),
            "module.hello" => {
                let mut response = hello.clone();
                if let Some(operation_id) = probe.store_unresolved.as_deref() {
                    let root = expected_root_id(BINDING_ID, GENERATION);
                    response["native_root_id"] = json!(root);
                    response["native_scope_key"] = json!(probe.native_scope);
                    response["startup_recovery"] = store_startup_manifest(
                        &session_boot_id,
                        Some(&root),
                        &[(operation_id, "agent.reply")],
                    );
                }
                response
            }
            "module.next" => json!({"command":null}),
            "module.outcome" | "module.observe" | "module.result" => {
                json!({"recorded":true})
            }
            _ => json!({"recorded":true}),
        };
        let response = json!({
            "jsonrpc":"2.0",
            "id":request["id"],
            "result":result
        });
        write
            .write_all(&serde_json::to_vec(&response).map_err(std::io::Error::other)?)
            .await?;
        write.write_all(b"\n").await?;
        write.flush().await?;
        if method != "client.hello" {
            let _ = events.send(json!({
                "method":method,
                "boot_id":session_boot_id,
                "params":params,
                "inventory_complete":inventory_complete,
                "legacy_migration_complete":legacy_migration_complete
            }));
        }
    }
}

async fn serve_startup_fixture(
    mut listener: swarm_kernel_host::ipc::Listener,
    config: crate::AdapterConfig,
    claim: serde_json::Value,
    probe: StartupJournalProbe,
    events: mpsc::UnboundedSender<serde_json::Value>,
) -> std::io::Result<()> {
    loop {
        let stream = listener.accept().await.map_err(|error| {
            std::io::Error::other(format!("startup fixture IPC accept failed: {error}"))
        })?;
        match serve_startup_fixture_connection(
            stream,
            startup_fixture_hello(&config, &claim),
            probe.clone(),
            events.clone(),
        )
        .await
        {
            Ok(()) => {}
            Err(error) if startup_fixture_peer_closed(&error) => {
                eprintln!("startup fixture IPC peer disconnected: {error:?}");
            }
            Err(error) => {
                return Err(std::io::Error::new(
                    error.kind(),
                    format!("startup fixture IPC connection failed: {error:?}"),
                ));
            }
        }
    }
}

fn startup_fixture_peer_closed(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::UnexpectedEof
    )
}

async fn next_startup_fixture_event(
    events: &mut mpsc::UnboundedReceiver<serde_json::Value>,
    server: &mut tokio::task::JoinHandle<std::io::Result<()>>,
) -> serde_json::Value {
    match tokio::time::timeout(Duration::from_secs(30), events.recv()).await {
        Ok(Some(event)) => event,
        Ok(None) => {
            let server_result = server.await;
            panic!("startup fixture event channel closed; server result: {server_result:?}");
        }
        Err(_) => panic!("source adapter startup fixture call did not arrive within 30 seconds"),
    }
}

async fn assert_startup_fixture_report(
    connection: (
        &mut mpsc::UnboundedReceiver<serde_json::Value>,
        &mut tokio::task::JoinHandle<std::io::Result<()>>,
    ),
    journal: &StartupJournalProbe,
    boot_id: &str,
    unresolved_id: &str,
    acknowledged_ids: &std::collections::BTreeSet<String>,
    expect_complete_at_hello: bool,
    minimum_incomplete_next_calls: usize,
) -> usize {
    let (events, server) = connection;
    let mut hello_seen = false;
    let mut incomplete_next_calls = 0usize;
    loop {
        let event = next_startup_fixture_event(events, server).await;
        let method = event["method"].as_str().unwrap_or_default();
        let params = &event["params"];
        let legacy_migration_complete = event["legacy_migration_complete"] == true;
        match method {
            "module.hello" if params["boot_id"] == boot_id => {
                assert_eq!(
                    legacy_migration_complete, expect_complete_at_hello,
                    "hello observes the persisted migration state for this boot"
                );
                hello_seen = true;
            }
            "module.next" if !legacy_migration_complete => {
                incomplete_next_calls += 1;
            }
            "module.outcome" => {
                let operation_id = params["operation_id"].as_str().unwrap_or_default();
                assert!(
                    !acknowledged_ids.contains(operation_id),
                    "ACKed history must not be replayed to the host"
                );
            }
            "module.observe"
                if params["state"]["boot_id"] == boot_id
                    && params["state"]["journal_recovery"].is_object() =>
            {
                assert_eq!(event["inventory_complete"], true);
                let recovery = &params["state"]["journal_recovery"];
                assert_eq!(recovery["coverage"], "complete");
                assert_eq!(recovery["native_replay_permitted"], false);
                let entries = recovery["entries"].as_array().unwrap();
                let operation_ids = entries
                    .iter()
                    .filter_map(|entry| entry["operation_id"].as_str())
                    .collect::<std::collections::BTreeSet<_>>();
                assert_eq!(entries.len(), 1);
                assert_eq!(
                    operation_ids,
                    std::collections::BTreeSet::from([unresolved_id])
                );
                assert!(hello_seen, "hello precedes recovery observation");
                assert!(
                    incomplete_next_calls >= minimum_incomplete_next_calls,
                    "module.next service calls match the expected migration window behavior"
                );
                assert!(journal.inventory_complete());
                return incomplete_next_calls;
            }
            _ => {}
        }
    }
}

async fn wait_for_startup_fixture_next(
    events: &mut mpsc::UnboundedReceiver<serde_json::Value>,
    server: &mut tokio::task::JoinHandle<std::io::Result<()>>,
    boot_id: &str,
) {
    loop {
        let event = next_startup_fixture_event(events, server).await;
        if event["method"] == "module.next" && event["boot_id"] == boot_id {
            return;
        }
    }
}

#[tokio::test]
// Drives the source `run_owned` loop through local IPC framing; bootstrap and
// host replies are fixture-owned, so this does not qualify supervisor launch.
async fn run_owned_keeps_module_service_live_through_256_legacy_records_and_recovers_exact_memo_after_restart()
 {
    let mut root = PreserveOnFailureRoot::new();
    let host_data_dir = root.0.join("host-ipc");
    fs::create_dir_all(&host_data_dir).unwrap();
    let state_dir = root.0.join("adapter-state");
    let native_options = crate::NativeOptions {
        service_id: "pr69-startup-fixture".to_owned(),
        connection_file: root.0.join("no-native-service-connection.json"),
        directory: root.0.clone(),
        model: crate::ModelRef {
            id: "fixture-model".to_owned(),
            provider_id: "fixture-provider".to_owned(),
            variant: "fixture-variant".to_owned(),
        },
    };
    let native_scope = native_options.scope_key();
    let route_sha256 = digest_json(&serde_json::to_value(&native_options).unwrap()).unwrap();
    let journal = Journal::open(
        &state_dir,
        BINDING_ID,
        GENERATION,
        &native_scope,
        &route_sha256,
    )
    .unwrap();
    let acknowledged_ids = (0..256)
        .map(|index| format!("pr69-legacy-acked-{index:03}"))
        .collect::<std::collections::BTreeSet<_>>();
    for operation_id in &acknowledged_ids {
        write_legacy_acknowledged_reply_for_scope(
            &journal,
            operation_id,
            &native_scope,
            &route_sha256,
        );
    }
    let unresolved_id = "pr69-unresolved-memo-exact";
    let mut unresolved = fixture_intent(unresolved_id, "agent.reply");
    unresolved.native_scope_key = native_scope.clone();
    unresolved.route_sha256 = route_sha256.clone();
    journal.write_intent(&unresolved).unwrap();
    assert!(journal.startup_inventory_required().unwrap());
    assert!(!journal.startup_recovery_complete());
    drop(journal);

    let _journal_capture = crate::startup_poll_tests::capture_next_startup_journal(&state_dir);

    let claim = startup_fixture_contract();
    let claim_value = serde_json::to_value(&claim).unwrap();
    let probe = StartupJournalProbe {
        root: state_dir.clone(),
        native_scope,
        store_unresolved: None,
    };
    let config = crate::AdapterConfig {
        schema_version: 1,
        host_data_dir: host_data_dir.clone(),
        credential_file: root.0.join("fixture-credential.json"),
        state_dir: state_dir.clone(),
        binding_id: BINDING_ID.to_owned(),
        generation: GENERATION,
        module_artifact_id: crate::ARTIFACT_ID.to_owned(),
        native_options: native_options.clone(),
        owned_native: None,
        ipc: swarm_client::IpcConfig::default(),
    };
    let listener = swarm_kernel_host::ipc::Listener::bind(&host_data_dir).unwrap();
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let mut server = tokio::spawn(serve_startup_fixture(
        listener,
        config.clone(),
        claim_value,
        probe.clone(),
        events_tx,
    ));

    let first_boot_id = "pr69-native-startup-first-boot";
    let first_run = tokio::spawn(crate::run_owned(startup_fixture_bootstrap(
        &root.0,
        &host_data_dir,
        &state_dir,
        &native_options,
        &claim,
        first_boot_id,
    )));
    let incomplete_next_calls = assert_startup_fixture_report(
        (&mut events_rx, &mut server),
        &probe,
        first_boot_id,
        unresolved_id,
        &acknowledged_ids,
        false,
        2,
    )
    .await;
    assert!(incomplete_next_calls >= 2);
    wait_for_startup_fixture_next(&mut events_rx, &mut server, first_boot_id).await;
    first_run.abort();
    assert!(first_run.await.unwrap_err().is_cancelled());

    let captured_journal = crate::startup_poll_tests::take_captured_startup_journal()
        .expect("capture the journal used by the source run_owned loop");
    let mut retention_passes = 0usize;
    loop {
        let summary = captured_journal
            .reclaim_acknowledged(BINDING_ID, GENERATION, &probe.native_scope)
            .unwrap();
        retention_passes += 1;
        if summary.queue_drained {
            break;
        }
        assert!(retention_passes < 16, "bounded retention queue drains");
    }
    assert!(retention_passes > 0);
    let final_cursor: serde_json::Value =
        serde_json::from_slice(&fs::read(state_dir.join("retention-cursor.json")).unwrap())
            .unwrap();
    assert_eq!(final_cursor["legacy_migration_version"], 1);
    drop(captured_journal);

    let second_boot_id = "pr69-native-startup-restart-boot";
    let second_run = tokio::spawn(crate::run_owned(startup_fixture_bootstrap(
        &root.0,
        &host_data_dir,
        &state_dir,
        &native_options,
        &claim,
        second_boot_id,
    )));
    assert_startup_fixture_report(
        (&mut events_rx, &mut server),
        &probe,
        second_boot_id,
        unresolved_id,
        &acknowledged_ids,
        true,
        0,
    )
    .await;
    wait_for_startup_fixture_next(&mut events_rx, &mut server, second_boot_id).await;
    second_run.abort();
    assert!(second_run.await.unwrap_err().is_cancelled());

    server.abort();
    let _ = server.await;
    root.1 = true;
}

fn write_acknowledged_reply_with_root(journal: &Journal, operation_id: &str, root_id: &str) {
    let mut intent = fixture_intent(operation_id, "agent.reply");
    intent.native_root_id = Some(root_id.to_owned());
    journal.write_intent(&intent).unwrap();
    let outcome = json!({
        "operation_id":operation_id,
        "outcome":"applied",
        "native_scope_key":NATIVE_SCOPE,
        "native_root_id":root_id,
        "details":{}
    });
    journal.queue_outcome_value(&outcome).unwrap();
    journal.acknowledge_outcome(&outcome).unwrap();
    journal
        .remove_pending(
            &journal
                .outbox
                .join(format!("outcome-{}.json", operation_key(operation_id))),
        )
        .unwrap();
}

fn operation_path(root: &Path, operation_id: &str) -> PathBuf {
    root.join("operations")
        .join(format!("{}.jsonl", operation_key(operation_id)))
}

#[test]
fn retention_pass_resumes_cursor_after_reopen_and_keeps_unresolved_reconcile_target() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();

    let target_id = "retention-target-agent-reply";
    write_acknowledged_reply(&journal, target_id, None);

    let reconcile_id = "retention-unresolved-reconcile";
    let mut reconcile_intent = fixture_intent(reconcile_id, "agent.reconcile");
    reconcile_intent.reconcile_target_operation_id = Some(target_id.to_owned());
    journal.write_intent(&reconcile_intent).unwrap();
    let reconcile_outcome = json!({
        "operation_id":reconcile_id,
        "outcome":"applied",
        "native_scope_key":NATIVE_SCOPE,
        "details":{
            "module_receipt":reconcile_intent.module_receipt.clone(),
            "target_operation_id":target_id,
            "completion_condition":"native_readback_recorded",
            "resolved":false,
            "native_replay":false
        }
    });
    journal.queue_outcome_value(&reconcile_outcome).unwrap();
    journal.acknowledge_outcome(&reconcile_outcome).unwrap();
    journal
        .remove_pending(
            &journal
                .outbox
                .join(format!("outcome-{}.json", operation_key(reconcile_id))),
        )
        .unwrap();

    let safe_ids: Vec<String> = (0..34)
        .map(|index| format!("retention-safe-{index:02}"))
        .collect();
    for operation_id in &safe_ids {
        write_acknowledged_reply(&journal, operation_id, None);
    }

    journal.recover_outbox().unwrap();
    let first = journal
        .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();
    assert_eq!(first.examined, 32);
    assert!((31..=32).contains(&first.namespace_removed));
    assert!(!first.queue_drained);
    assert!(
        safe_ids
            .iter()
            .any(|id| !operation_path(&root.0, id).exists())
    );
    assert!(operation_path(&root.0, target_id).exists());
    assert!(operation_path(&root.0, reconcile_id).exists());

    let saved_cursor = journal
        .retention
        .lock()
        .unwrap()
        .cursor_for_test()
        .map(str::to_owned);
    assert!(saved_cursor.is_some());
    drop(journal);

    let reopened =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert_eq!(
        reopened
            .retention
            .lock()
            .unwrap()
            .cursor_for_test()
            .map(str::to_owned),
        saved_cursor
    );
    reopened.recover_outbox().unwrap();
    let second = reopened
        .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();
    assert_eq!(second.examined, 34 - first.namespace_removed);
    assert_eq!(second.namespace_removed, 34 - first.namespace_removed);
    assert!(
        second.queue_drained,
        "the referenced reconcile target remains indexed but is not a reclaim candidate"
    );
    assert_eq!(first.namespace_removed + second.namespace_removed, 34);

    for operation_id in safe_ids {
        assert!(!operation_path(&root.0, &operation_id).exists());
    }
    assert!(operation_path(&root.0, target_id).exists());
    assert!(operation_path(&root.0, reconcile_id).exists());
    assert_eq!(
        reopened.load(reconcile_id).unwrap().outcome.unwrap()["details"]["resolved"],
        false
    );
}

#[test]
fn retention_keeps_unknown_removal_across_empty_pass_and_reopen() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();

    let operation_id = "retention-unknown-removal";
    let root_id = expected_root_id(BINDING_ID, GENERATION);
    write_acknowledged_reply_with_root(&journal, operation_id, &root_id);
    assert!(journal.recover_outbox().unwrap().is_empty());

    let operation_path = operation_path(&root.0, operation_id);
    let outbox_path = journal
        .outbox
        .join(format!("outcome-{}.json", operation_key(operation_id)));
    let acknowledged = journal.load(operation_id).unwrap();
    assert_eq!(
        acknowledged.acknowledged_sha256.as_deref(),
        acknowledged.outcome_sha256.as_deref()
    );
    let first = super::retention::reclaim_acknowledged_with_remover(
        &journal,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        |path| {
            let cursor_bytes = fs::read(root.0.join("retention-cursor.json")).unwrap();
            let cursor: serde_json::Value = serde_json::from_slice(&cursor_bytes).unwrap();
            assert_eq!(cursor["removal_durability_unknown"].as_bool(), Some(true));
            let outcome = swarm_process::remove_private_acknowledged(path)?;
            Ok(swarm_process::RemovalOutcome {
                removed: outcome.removed,
                durability: swarm_process::RemovalDurability::Unknown,
            })
        },
    )
    .unwrap();

    assert_eq!(first.namespace_removed, 1);
    assert!(first.queue_drained);
    assert!(first.durability_unknown);
    assert!(!operation_path.exists());
    assert!(!outbox_path.exists());
    assert_eq!(
        journal
            .native_root_checkpoint_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap()
            .as_deref(),
        Some(root_id.as_str())
    );
    drop(journal);

    let reopened =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert!(
        reopened
            .retention
            .lock()
            .unwrap()
            .removal_durability_unknown_for_test()
    );
    assert!(reopened.recover_outbox().unwrap().is_empty());
    let second = reopened
        .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();

    assert_eq!(second.examined, 0);
    assert_eq!(second.namespace_removed, 0);
    assert!(second.queue_drained);
    assert!(second.durability_unknown);
    assert!(!operation_path.exists());
    assert!(!outbox_path.exists());
    assert_eq!(
        reopened
            .native_root_checkpoint_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap()
            .as_deref(),
        Some(root_id.as_str())
    );
}

#[test]
fn opencode_root_checkpoint_is_durable_before_source_delete_crash_boundary() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let operation_id = "retention-checkpoint-before-delete";
    let root_id = expected_root_id(BINDING_ID, GENERATION);
    write_acknowledged_reply_with_root(&journal, operation_id, &root_id);
    journal.recover_outbox().unwrap();
    let path = operation_path(&root.0, operation_id);
    let original = fs::read(&path).unwrap();

    let crashed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        super::retention::reclaim_acknowledged_with_test_hook(
            &journal,
            BINDING_ID,
            GENERATION,
            NATIVE_SCOPE,
            |candidate| {
                assert_eq!(candidate, path.as_path());
                let checkpoint = fs::read(root.0.join("native-checkpoints.jsonl"))?;
                assert!(
                    String::from_utf8_lossy(&checkpoint).contains(&root_id),
                    "checkpoint readback precedes the simulated process crash"
                );
                panic!("simulated crash after durable root checkpoint")
            },
            |_| panic!("source removal must not run before the crash boundary"),
        )
    }));
    assert!(crashed.is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    drop(journal);

    let reopened =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert_eq!(
        reopened
            .native_root_checkpoint_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap()
            .as_deref(),
        Some(root_id.as_str())
    );
    let history = reopened.load(operation_id).unwrap();
    assert_eq!(history.acknowledged_sha256, history.outcome_sha256);
    assert!(!history.native_replay_permitted());
    assert_eq!(fs::read(&path).unwrap(), original);
}

#[test]
fn opencode_cursor_publication_failure_keeps_acknowledged_source_before_delete() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let operation_id = "retention-cursor-publication-failure";
    let root_id = expected_root_id(BINDING_ID, GENERATION);
    write_acknowledged_reply_with_root(&journal, operation_id, &root_id);
    journal.recover_outbox().unwrap();
    let path = operation_path(&root.0, operation_id);
    let original = fs::read(&path).unwrap();
    let cursor = root.0.join("retention-cursor.json");

    let error = super::retention::reclaim_acknowledged_with_test_hook(
        &journal,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        |_| {
            let checkpoint = fs::read(root.0.join("native-checkpoints.jsonl"))?;
            assert!(String::from_utf8_lossy(&checkpoint).contains(&root_id));
            fs::remove_file(&cursor)?;
            fs::create_dir(&cursor)?;
            Ok(())
        },
        |_| panic!("cursor failure must prevent the source delete"),
    )
    .unwrap_err();

    assert_eq!(fs::read(&path).unwrap(), original);
    let history = journal.load(operation_id).unwrap();
    assert_eq!(history.acknowledged_sha256, history.outcome_sha256);
    assert!(!history.native_replay_permitted());
    assert_eq!(error.code, "ADAPTER_STATE");
}

#[test]
fn opencode_journal_change_between_classification_and_delete_is_retained() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let operation_id = "retention-change-before-delete";
    let root_id = expected_root_id(BINDING_ID, GENERATION);
    write_acknowledged_reply_with_root(&journal, operation_id, &root_id);
    journal.recover_outbox().unwrap();
    let path = operation_path(&root.0, operation_id);
    let original = fs::read(&path).unwrap();
    let mutation = b"changed-after-classification\n";

    let error = super::retention::reclaim_acknowledged_with_test_hook(
        &journal,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        |candidate| {
            assert_eq!(candidate, path.as_path());
            fs::write(candidate, mutation)?;
            Ok(())
        },
        |_| panic!("digest mismatch must prevent the shared removal primitive"),
    )
    .unwrap_err();

    assert_eq!(error.code, "ADAPTER_JOURNAL");
    assert!(path.exists(), "changed custody bytes must stay present");
    assert_eq!(fs::read(&path).unwrap(), mutation);
    assert_ne!(fs::read(&path).unwrap(), original);
}

#[test]
fn opencode_cleanup_failure_preserves_store_ack_and_never_authorizes_replay() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let operation_id = "retention-cleanup-failure-no-replay";
    let root_id = expected_root_id(BINDING_ID, GENERATION);
    write_acknowledged_reply_with_root(&journal, operation_id, &root_id);
    journal.recover_outbox().unwrap();
    let path = operation_path(&root.0, operation_id);
    let original = fs::read(&path).unwrap();
    let outbox = root
        .0
        .join("outbox")
        .join(format!("outcome-{}.json", operation_key(operation_id)));

    let error = super::retention::reclaim_acknowledged_with_remover(
        &journal,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        |_| {
            Err(Error::new(
                "INJECTED_REMOVE_FAILURE",
                "simulated cleanup boundary failure",
            ))
        },
    )
    .unwrap_err();

    assert_eq!(error.code, "INJECTED_REMOVE_FAILURE");
    assert_eq!(fs::read(&path).unwrap(), original);
    assert!(!outbox.exists());
    let history = journal.load(operation_id).unwrap();
    assert_eq!(history.acknowledged_sha256, history.outcome_sha256);
    assert!(history.outcome.is_some());
    assert!(!history.native_replay_permitted());
}

#[test]
fn retention_legacy_or_corrupt_cursor_never_means_confirmed_removal() {
    let legacy_cursor = serde_json::to_vec(&json!({
        "version":1,
        "binding_id":BINDING_ID,
        "generation":GENERATION,
        "native_scope_key":NATIVE_SCOPE,
        "route_sha256":ROUTE_SHA256,
        "cursor":null
    }))
    .unwrap();

    let legacy_root = FixtureRoot::new();
    let journal = Journal::open(
        &legacy_root.0,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        ROUTE_SHA256,
    )
    .unwrap();
    drop(journal);
    fs::write(legacy_root.0.join("retention-cursor.json"), legacy_cursor).unwrap();
    let reopened = Journal::open(
        &legacy_root.0,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        ROUTE_SHA256,
    )
    .unwrap();
    assert!(
        reopened
            .retention
            .lock()
            .unwrap()
            .removal_durability_unknown_for_test()
    );
    drop(reopened);

    let corrupt_root = FixtureRoot::new();
    let journal = Journal::open(
        &corrupt_root.0,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        ROUTE_SHA256,
    )
    .unwrap();
    drop(journal);
    fs::write(corrupt_root.0.join("retention-cursor.json"), b"{not-json").unwrap();
    let reopened = Journal::open(
        &corrupt_root.0,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        ROUTE_SHA256,
    )
    .unwrap();
    assert!(
        reopened
            .retention
            .lock()
            .unwrap()
            .removal_durability_unknown_for_test()
    );
}

#[test]
fn retention_v3_complete_cursor_migrates_to_versioned_sparse_startup_barrier() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert!(journal.recover_outbox().unwrap().is_empty());
    assert!(journal.startup_recovery_complete());
    drop(journal);

    let cursor_path = root.0.join("retention-cursor.json");
    let mut cursor: serde_json::Value =
        serde_json::from_slice(&fs::read(&cursor_path).unwrap()).unwrap();
    assert_eq!(cursor["version"].as_u64(), Some(4));
    cursor["version"] = json!(3);
    cursor
        .as_object_mut()
        .unwrap()
        .remove("legacy_migration_version");
    fs::write(&cursor_path, serde_json::to_vec(&cursor).unwrap()).unwrap();

    let reopened =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert!(reopened.startup_inventory_required().unwrap());
    assert_eq!(
        reopened
            .retention
            .lock()
            .unwrap()
            .legacy_migration_version_for_test(),
        0
    );
    let before = reopened.startup_scan_counts();
    assert!(reopened.recover_outbox().unwrap().is_empty());
    let after = reopened.startup_scan_counts();
    assert_eq!(after.operations_listed - before.operations_listed, 0);
    assert_eq!(
        reopened
            .retention
            .lock()
            .unwrap()
            .legacy_migration_version_for_test(),
        1
    );
    assert!(reopened.startup_recovery_complete());
}

/// Startup recovery stays bounded on a large acknowledged history with a
/// sparse unresolved tail: every inventory call parses at most one window,
/// the persisted startup cursor resumes across restarts, retention
/// converges through bounded retries, and the exact sparse tail survives
/// with ACK truth intact and no native replay.
#[test]
fn startup_recovery_bounds_large_history_and_resumes_sparse_tail() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();

    for index in 0..100 {
        write_acknowledged_reply(&journal, &format!("startup-bulk-{index:03}"), None);
    }
    let unknown_id = "startup-unknown-intent";
    journal
        .write_intent(&fixture_intent(unknown_id, "agent.reply"))
        .unwrap();
    let pending_id = "startup-pending-outcome";
    journal
        .write_intent(&fixture_intent(pending_id, "agent.reply"))
        .unwrap();
    journal
        .queue_outcome_value(&json!({
            "operation_id": pending_id,
            "outcome": "applied",
            "native_scope_key": NATIVE_SCOPE,
            "details": {}
        }))
        .unwrap();
    let target_id = "startup-reconcile-target";
    write_acknowledged_reply(&journal, target_id, None);
    let reconcile_id = "startup-reconcile-open";
    let mut reconcile_intent = fixture_intent(reconcile_id, "agent.reconcile");
    reconcile_intent.reconcile_target_operation_id = Some(target_id.to_owned());
    journal.write_intent(&reconcile_intent).unwrap();
    let reconcile_outcome = json!({
        "operation_id": reconcile_id,
        "outcome": "applied",
        "native_scope_key": NATIVE_SCOPE,
        "details": {
            "module_receipt": reconcile_intent.module_receipt.clone(),
            "target_operation_id": target_id,
            "completion_condition": "native_readback_recorded",
            "resolved": false,
            "native_replay": false
        }
    });
    journal.queue_outcome_value(&reconcile_outcome).unwrap();
    journal.acknowledge_outcome(&reconcile_outcome).unwrap();
    journal
        .remove_pending(
            &journal
                .outbox
                .join(format!("outcome-{}.json", operation_key(reconcile_id))),
        )
        .unwrap();

    // First boot classifies exactly one window and keeps the tail pending.
    let boot_counts = journal.startup_scan_counts();
    assert_eq!(boot_counts.operation_bodies_parsed, 0);
    let first = journal.recover_outbox().unwrap();
    let after_first = journal.startup_scan_counts();
    assert_eq!(
        after_first.operation_bodies_parsed - boot_counts.operation_bodies_parsed,
        32
    );
    assert!(first.len() <= 32);
    assert!(!journal.startup_recovery_complete());
    let saved_cursor = journal
        .retention
        .lock()
        .unwrap()
        .startup_cursor_for_test()
        .map(str::to_owned);
    assert!(saved_cursor.is_some());

    // A restart resumes from the persisted position under the same bound.
    drop(journal);
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert_eq!(
        journal
            .retention
            .lock()
            .unwrap()
            .startup_cursor_for_test()
            .map(str::to_owned),
        saved_cursor
    );
    let boot_second = journal.startup_scan_counts();
    journal.recover_outbox().unwrap();
    let after_second = journal.startup_scan_counts();
    assert!(after_second.operation_bodies_parsed - boot_second.operation_bodies_parsed <= 32);

    // Retention converges through bounded retries: no call parses more
    // than one window, and the sparse tail is never deleted or replayed.
    for _ in 0..16 {
        let before = journal.startup_scan_counts();
        let summary = journal
            .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap();
        let after = journal.startup_scan_counts();
        assert!(
            after.operation_bodies_parsed - before.operation_bodies_parsed <= 32,
            "inventory parses stay within one window per call"
        );
        if summary.queue_drained && journal.startup_recovery_complete() {
            break;
        }
    }
    assert!(journal.startup_recovery_complete());
    let drained = journal
        .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();
    assert!(drained.queue_drained);

    for index in 0..100 {
        assert!(
            !operation_path(&root.0, &format!("startup-bulk-{index:03}")).exists(),
            "acknowledged bulk history is reclaimed"
        );
    }
    assert!(operation_path(&root.0, unknown_id).exists());
    assert!(operation_path(&root.0, pending_id).exists());
    assert!(operation_path(&root.0, target_id).exists());
    assert!(operation_path(&root.0, reconcile_id).exists());
    assert!(journal.load(unknown_id).unwrap().recovery_unknown());
    assert!(!journal.load(unknown_id).unwrap().native_replay_permitted());
    let pending_history = journal.load(pending_id).unwrap();
    assert!(pending_history.outcome.is_some());
    assert!(pending_history.acknowledged_sha256.is_none());
    assert!(!pending_history.native_replay_permitted());
    assert!(
        journal
            .outbox
            .join(format!("outcome-{}.json", operation_key(pending_id)))
            .exists(),
        "the unacked delivery stays pending"
    );
    assert_eq!(
        journal.load(reconcile_id).unwrap().outcome.unwrap()["details"]["resolved"],
        false
    );
    assert!(journal.startup_scan_counts().unresolved_found >= 1);
    // Progress lives only in the shared cursor file: no second index.
    assert!(!root.0.join("startup-cursor.json").exists());
    let cursor_bytes = fs::read(root.0.join("retention-cursor.json")).unwrap();
    let cursor: serde_json::Value = serde_json::from_slice(&cursor_bytes).unwrap();
    assert_eq!(cursor["version"].as_u64(), Some(4));
    assert_eq!(cursor["legacy_migration_version"].as_u64(), Some(1));
}

/// Cold legacy inventory is prepared off the service loop; body classification
/// remains windowed, while a write-ahead memo preserves cursor progress and
/// makes an append visible to the later exact-closure pass.
#[test]
fn cold_legacy_before_hello_is_bounded_and_append_resumes() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();

    for index in 0..256 {
        write_legacy_acknowledged_reply(&journal, &format!("cold-legacy-{index:03}"));
    }
    let unresolved_id = "cold-legacy-unresolved";
    journal
        .write_intent(&fixture_intent(unresolved_id, "agent.reply"))
        .unwrap();
    let torn_id = "cold-legacy-torn-tail";
    journal
        .write_intent(&fixture_intent(torn_id, "agent.reply"))
        .unwrap();
    let torn_path = operation_path(&root.0, torn_id);
    std::fs::OpenOptions::new()
        .append(true)
        .open(&torn_path)
        .unwrap()
        .write_all(b"{")
        .unwrap();
    let torn_bytes = fs::read(&torn_path).unwrap();
    let address_dir = root.0.join(retention::ADDRESS_CHECKPOINT_DIR);
    for entry in fs::read_dir(&address_dir).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    drop(journal);

    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert!(journal.recover_outbox_before_hello().unwrap().is_empty());
    let before_hello = journal.startup_scan_counts();
    assert_eq!(before_hello.operations_listed, 0);
    assert_eq!(before_hello.operation_bodies_parsed, 0);
    assert!(!journal.startup_recovery_complete());
    let targeted = journal.load(unresolved_id).unwrap();
    assert_eq!(
        targeted.intent.as_ref().unwrap().operation_id,
        unresolved_id
    );
    assert!(targeted.recovery_unknown());
    assert!(!targeted.native_replay_permitted());

    // Preparing all names reads metadata only. The service can poll while
    // this work runs; a body window remains capped at 32 entries.
    journal.prepare_startup_inventory().unwrap();
    assert_eq!(journal.startup_scan_counts().operations_listed, 258);
    assert_eq!(journal.startup_scan_counts().operation_bodies_parsed, 0);
    let first = journal.recover_outbox().unwrap();
    assert!(first.len() <= 32);
    assert_eq!(journal.startup_scan_counts().operation_bodies_parsed, 32);
    assert_eq!(journal.startup_scan_counts().operations_listed, 258);
    let saved_cursor = journal
        .retention
        .lock()
        .unwrap()
        .startup_cursor_for_test()
        .map(str::to_owned);
    assert!(saved_cursor.is_some());
    drop(journal);

    // Restart first restores the cursor. An append preserves that traversal
    // position; its existing address memo is read after the legacy census.
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert_eq!(
        journal
            .retention
            .lock()
            .unwrap()
            .startup_cursor_for_test()
            .map(str::to_owned),
        saved_cursor
    );
    journal.prepare_startup_inventory().unwrap();
    let appended_id = "cold-legacy-appended-after-restart";
    let cursor_before_append = journal
        .retention
        .lock()
        .unwrap()
        .startup_cursor_for_test()
        .map(str::to_owned);
    journal
        .write_intent(&fixture_intent(appended_id, "agent.reply"))
        .unwrap();
    assert_eq!(
        journal
            .retention
            .lock()
            .unwrap()
            .startup_cursor_for_test()
            .map(str::to_owned),
        cursor_before_append
    );

    for _ in 0..16 {
        let before = journal.startup_scan_counts();
        let recovered = journal.recover_outbox().unwrap();
        let after = journal.startup_scan_counts();
        assert!(recovered.len() <= 32);
        assert!(after.operation_bodies_parsed - before.operation_bodies_parsed <= 32);
        assert_eq!(
            after.operations_listed - before.operations_listed,
            0,
            "one in-process migration reuses its single sorted pathname snapshot"
        );
        if journal.startup_recovery_complete() {
            break;
        }
    }
    assert!(
        journal.startup_recovery_complete(),
        "recovery remains incomplete: scan={:?}, cursor={:?}",
        journal.startup_scan_counts(),
        journal.retention.lock().unwrap().startup_cursor_for_test()
    );
    assert_eq!(
        journal
            .retention
            .lock()
            .unwrap()
            .legacy_migration_version_for_test(),
        0,
        "bounded inventory alone cannot publish the steady-startup barrier"
    );
    assert_eq!(
        journal
            .load(appended_id)
            .unwrap()
            .intent
            .as_ref()
            .map(|intent| intent.operation_id.as_str()),
        Some(appended_id)
    );
    let appended = journal.load(appended_id).unwrap();
    assert!(appended.recovery_unknown());
    assert!(!appended.native_replay_permitted());
    assert!(operation_path(&root.0, appended_id).exists());
    let torn = journal.load(torn_id).unwrap();
    assert!(torn.torn_tail_evidence.is_some());
    assert!(!torn.native_replay_permitted());
    assert_eq!(fs::read(&torn_path).unwrap(), torn_bytes);

    // The compatibility scan is a versioned barrier: old safe candidates
    // drain before v1 is published; after it, restart opens only exact sparse
    // address IDs and does not enumerate the legacy pathname inventory.
    for _ in 0..16 {
        let summary = journal
            .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap();
        if summary.queue_drained
            && journal
                .retention
                .lock()
                .unwrap()
                .legacy_migration_version_for_test()
                == 1
        {
            break;
        }
    }
    assert_eq!(
        journal
            .retention
            .lock()
            .unwrap()
            .legacy_migration_version_for_test(),
        1
    );
    for index in 0..256 {
        assert!(!operation_path(&root.0, &format!("cold-legacy-{index:03}")).exists());
    }
    drop(journal);

    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert!(!journal.startup_inventory_required().unwrap());
    let before_steady = journal.startup_scan_counts();
    let sparse = journal.recover_outbox().unwrap();
    let after_steady = journal.startup_scan_counts();
    assert!(journal.startup_recovery_complete());
    assert_eq!(sparse.len(), 3);
    assert_eq!(
        after_steady.operations_listed - before_steady.operations_listed,
        0
    );
    assert_eq!(
        after_steady.operation_bodies_parsed - before_steady.operation_bodies_parsed,
        3,
        "steady restart parses only unresolved, damaged, and appended exact operation IDs"
    );
    assert!(
        !journal
            .load(unresolved_id)
            .unwrap()
            .native_replay_permitted()
    );
    assert!(!journal.load(torn_id).unwrap().native_replay_permitted());
    assert_eq!(fs::read(&torn_path).unwrap(), torn_bytes);
    assert!(!journal.load(appended_id).unwrap().native_replay_permitted());
}

fn store_startup_manifest(boot_id: &str, root: Option<&str>, operations: &[(&str, &str)]) -> Value {
    json!({
        "schema_version":1,
        "binding_id":BINDING_ID,
        "generation":GENERATION,
        "worker_boot_id":boot_id,
        "native_root_id":root,
        "complete":true,
        "unresolved_operations":operations.iter().map(|(id, method)| json!({
            "operation_id":id,"method":method,"state":"outcome_unknown"
        })).collect::<Vec<_>>()
    })
}

#[tokio::test]
async fn store_startup_connected_hello_recovers_current_root_and_exact_unresolved_before_census() {
    let root = FixtureRoot::new();
    let host_data_dir = root.0.join("host-ipc");
    fs::create_dir_all(&host_data_dir).unwrap();
    let state_dir = root.0.join("adapter-state");
    let native_options = crate::NativeOptions {
        service_id: "store-front-connected".to_owned(),
        connection_file: root.0.join("no-native-service.json"),
        directory: root.0.clone(),
        model: crate::ModelRef {
            id: "fixture".to_owned(),
            provider_id: "fixture".to_owned(),
            variant: "fixture".to_owned(),
        },
    };
    let native_scope = native_options.scope_key();
    let route_sha256 = digest_json(&serde_json::to_value(&native_options).unwrap()).unwrap();
    let journal = Journal::open(
        &state_dir,
        BINDING_ID,
        GENERATION,
        &native_scope,
        &route_sha256,
    )
    .unwrap();
    for index in 0..256 {
        write_legacy_acknowledged_reply_for_scope(
            &journal,
            &format!("connected-front-old-{index:03}"),
            &native_scope,
            &route_sha256,
        );
    }
    let unresolved_id = "connected-front-exact-unknown";
    let mut intent = fixture_intent(unresolved_id, "agent.reply");
    intent.native_scope_key = native_scope.clone();
    intent.route_sha256 = route_sha256.clone();
    journal.write_intent(&intent).unwrap();
    drop(journal);
    let journal = Journal::open(
        &state_dir,
        BINDING_ID,
        GENERATION,
        &native_scope,
        &route_sha256,
    )
    .unwrap();
    let contract = startup_fixture_contract();
    let bootstrap = startup_fixture_bootstrap(
        &root.0,
        &host_data_dir,
        &state_dir,
        &native_options,
        &contract,
        "connected-front-boot",
    );
    let listener = swarm_kernel_host::ipc::Listener::bind(&host_data_dir).unwrap();
    let (send, _receive) = mpsc::unbounded_channel();
    let server = tokio::spawn(serve_startup_fixture(
        listener,
        bootstrap.config.clone(),
        serde_json::to_value(&contract).unwrap(),
        StartupJournalProbe {
            root: state_dir.clone(),
            native_scope: native_scope.clone(),
            store_unresolved: Some(unresolved_id.to_owned()),
        },
        send,
    ));
    let host = crate::HostSession {
        config: &bootstrap.config,
        credential: &bootstrap.credential,
        journal: &journal,
        claim: &contract,
        boot_id: "connected-front-boot".to_owned(),
        hello_base: json!({"boot_id":"connected-front-boot","module_artifact_id":crate::ARTIFACT_ID}),
        root_hint: std::sync::Mutex::new(None),
        ipc_link: tokio::sync::Mutex::new(None),
        outcome_observer: None,
        retention_retry_pending: std::sync::atomic::AtomicBool::new(false),
    };
    let recovered = host.hello_retry().await.unwrap().unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].operation_id.as_deref(), Some(unresolved_id));
    assert!(!recovered[0].history.native_replay_permitted());
    let expected = expected_root_id(BINDING_ID, GENERATION);
    assert_eq!(
        host.root_hint().unwrap().as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(
        journal
            .native_root_checkpoint_for_hello(BINDING_ID, GENERATION, &native_scope)
            .unwrap()
            .as_deref(),
        Some(expected.as_str())
    );
    assert_eq!(journal.startup_scan_counts().operations_listed, 0);
    assert_eq!(journal.startup_scan_counts().operation_bodies_parsed, 1);
    assert!(!journal.startup_recovery_complete());
    server.abort();
    let _ = server.await;
}

#[test]
fn store_startup_recovers_cold_legacy_front_without_lifetime_census() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    for index in 0..256 {
        write_legacy_acknowledged_reply(&journal, &format!("store-front-old-{index:03}"));
    }
    let unresolved_id = "store-front-unresolved";
    let target_id = "store-front-referenced";
    write_legacy_acknowledged_reply(&journal, target_id);
    let mut unresolved = fixture_intent(unresolved_id, "agent.reconcile");
    unresolved.reconcile_target_operation_id = Some(target_id.to_owned());
    journal.write_intent(&unresolved).unwrap();
    let pending_id = "store-front-applied-lost-ack";
    journal
        .write_intent(&fixture_intent(pending_id, "agent.reply"))
        .unwrap();
    journal.queue_outcome_value(&json!({
        "operation_id":pending_id,"outcome":"applied","native_scope_key":NATIVE_SCOPE,"details":{}
    })).unwrap();
    let orphan = journal.operations.join("unmanifested-damaged.jsonl");
    fs::write(&orphan, b"invalid retained orphan").unwrap();
    drop(journal);

    // Repeat a crash before legacy reclamation. Neither first cold startup
    // nor the restart may reclassify the 256 ACKed historical bodies.
    for boot_id in ["store-front-boot-1", "store-front-boot-2"] {
        let journal =
            Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
        let manifest = store_startup_manifest(boot_id, None, &[(unresolved_id, "agent.reconcile")]);
        let recovered = journal
            .recover_store_startup(&manifest, boot_id, None)
            .unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].operation_id.as_deref(), Some(unresolved_id));
        assert!(!recovered[0].history.native_replay_permitted());
        let counts = journal.startup_scan_counts();
        assert_eq!(counts.operations_listed, 0);
        assert_eq!(counts.operation_bodies_parsed, 3);
        assert_eq!(counts.outbox_entries_scanned, 1);
        assert!(
            journal
                .pending_items()
                .unwrap()
                .iter()
                .any(|(_, item)| item.key == pending_id)
        );
        assert!(
            !journal.startup_recovery_complete(),
            "Store absence never proves deletion safety"
        );
        assert_eq!(
            journal
                .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
                .unwrap_err()
                .code,
            "ADAPTER_RETENTION_STATE"
        );
        assert!(operation_path(&root.0, target_id).exists());
        assert!(orphan.exists());
        assert!(operation_path(&root.0, "store-front-old-000").exists());
    }
}

#[test]
fn store_startup_rejects_incomplete_and_foreign_front_without_deletion() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let operation_id = "store-front-damaged";
    journal
        .write_intent(&fixture_intent(operation_id, "agent.reply"))
        .unwrap();
    let manifest = store_startup_manifest("current-boot", None, &[(operation_id, "agent.reply")]);
    for (field, value) in [
        ("complete", json!(false)),
        ("worker_boot_id", json!("stale-boot")),
        ("binding_id", json!("foreign-binding")),
        ("generation", json!(2)),
        ("native_root_id", json!("foreign-root")),
    ] {
        let mut invalid = manifest.clone();
        invalid[field] = value;
        assert_eq!(
            journal
                .recover_store_startup(&invalid, "current-boot", None)
                .unwrap_err()
                .code,
            "HOST_STARTUP_RECOVERY"
        );
    }
    let mut overflow = manifest.clone();
    overflow["unresolved_operations"] = json!(
        (0..=4096)
            .map(|index| json!({
                "operation_id":format!("overflow-{index}"),
                "method":"agent.reply",
                "state":"outcome_unknown"
            }))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        journal
            .recover_store_startup(&overflow, "current-boot", None)
            .unwrap_err()
            .code,
        "HOST_STARTUP_RECOVERY"
    );
    assert_eq!(journal.startup_scan_counts().operation_bodies_parsed, 0);
    assert!(operation_path(&root.0, operation_id).exists());
    assert!(!journal.startup_recovery_complete());
}

#[test]
fn store_startup_holds_corrupt_selected_records_and_recovers_other_deliveries() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let native_root = expected_root_id(BINDING_ID, GENERATION);

    let corrupt_journal_id = "store-front-corrupt-journal";
    let mut corrupt_journal_intent = fixture_intent(corrupt_journal_id, "agent.reply");
    corrupt_journal_intent.native_root_id = Some(native_root.clone());
    journal.write_intent(&corrupt_journal_intent).unwrap();
    let corrupt_journal_path = operation_path(&root.0, corrupt_journal_id);
    let corrupt_journal_bytes = b"{selected journal damage}\n";
    fs::write(&corrupt_journal_path, corrupt_journal_bytes).unwrap();

    let corrupt_outbox_id = "store-front-corrupt-outbox";
    let mut corrupt_outbox_intent = fixture_intent(corrupt_outbox_id, "agent.reply");
    corrupt_outbox_intent.native_root_id = Some(native_root.clone());
    journal.write_intent(&corrupt_outbox_intent).unwrap();
    let corrupt_outbox_path = root
        .0
        .join("outbox")
        .join(format!("outcome-{}.json", operation_key(corrupt_outbox_id)));
    let corrupt_outbox_bytes = b"{selected outbox damage}";
    fs::write(&corrupt_outbox_path, corrupt_outbox_bytes).unwrap();

    let unresolved_id = "store-front-unrelated-unknown";
    let mut unresolved_intent = fixture_intent(unresolved_id, "agent.reply");
    unresolved_intent.native_root_id = Some(native_root.clone());
    journal.write_intent(&unresolved_intent).unwrap();

    let pending_id = "store-front-unrelated-pending";
    let mut pending_intent = fixture_intent(pending_id, "agent.reply");
    pending_intent.native_root_id = Some(native_root.clone());
    journal.write_intent(&pending_intent).unwrap();
    let pending_outcome = json!({
        "operation_id":pending_id,
        "outcome":"applied",
        "native_scope_key":NATIVE_SCOPE,
        "native_root_id":native_root,
        "details":{}
    });
    journal.queue_outcome_value(&pending_outcome).unwrap();

    let manifest = store_startup_manifest(
        "current-boot",
        Some(&expected_root_id(BINDING_ID, GENERATION)),
        &[
            (corrupt_journal_id, "agent.reply"),
            (corrupt_outbox_id, "agent.reply"),
            (unresolved_id, "agent.reply"),
        ],
    );
    let recovered = journal
        .recover_store_startup(
            &manifest,
            "current-boot",
            Some(&expected_root_id(BINDING_ID, GENERATION)),
        )
        .unwrap();
    let recovered_ids = recovered
        .iter()
        .filter_map(|state| state.operation_id.as_deref())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        recovered_ids,
        BTreeSet::from([corrupt_outbox_id, unresolved_id])
    );
    assert!(recovered.iter().all(|state| {
        state.history.recovery_unknown() && !state.history.native_replay_permitted()
    }));
    assert_eq!(
        fs::read(&corrupt_journal_path).unwrap(),
        corrupt_journal_bytes
    );
    assert_eq!(
        fs::read(&corrupt_outbox_path).unwrap(),
        corrupt_outbox_bytes
    );
    assert_eq!(journal.startup_scan_counts().operations_listed, 0);

    let pending_items = journal
        .pending_items_excluding(&std::collections::HashSet::new())
        .unwrap();
    let (_, pending_item) = pending_items
        .iter()
        .find(|(_, item)| item.key == pending_id)
        .expect("the unrelated valid pending delivery remains available");
    journal
        .validate_pending_outcome(
            &root
                .0
                .join("outbox")
                .join(format!("outcome-{}.json", operation_key(pending_id))),
            pending_item,
        )
        .unwrap();
    assert_eq!(journal.startup_damaged_record_observations(), 3);
}

#[test]
fn store_startup_rejects_unclassifiable_outbox_identity_without_mutation() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let operation_id = "store-front-outbox-identity";
    journal
        .write_intent(&fixture_intent(operation_id, "agent.reply"))
        .unwrap();
    let path = root
        .0
        .join("outbox")
        .join(format!("outcome-{}.json", operation_key(operation_id)));
    let bytes = serde_json::to_vec(&PendingItem {
        kind: "outcome".to_owned(),
        key: "foreign-operation".to_owned(),
        payload: json!({"operation_id":"foreign-operation","outcome":"applied"}),
    })
    .unwrap();
    fs::write(&path, &bytes).unwrap();
    let manifest = store_startup_manifest("current-boot", None, &[(operation_id, "agent.reply")]);

    assert_eq!(
        journal
            .recover_store_startup(&manifest, "current-boot", None)
            .unwrap_err()
            .code,
        "ADAPTER_OUTBOX"
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

#[test]
fn store_startup_keeps_decoded_journal_identity_and_root_mismatches_fail_closed() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let selected_id = "store-front-selected-identity";
    let foreign_id = "store-front-foreign-identity";
    let mut foreign_intent = fixture_intent(foreign_id, "agent.reply");
    foreign_intent.native_root_id = Some(expected_root_id(BINDING_ID, GENERATION));
    let identity_record = JournalRecord {
        version: 2,
        operation_id: foreign_id.to_owned(),
        kind: "intent".to_owned(),
        intent: Some(foreign_intent),
        outcome: None,
        outcome_sha256: None,
        result_params: None,
        result_sha256: None,
    };
    let identity_path = operation_path(&root.0, selected_id);
    let mut identity_bytes = serde_json::to_vec(&identity_record).unwrap();
    identity_bytes.push(b'\n');
    fs::write(&identity_path, &identity_bytes).unwrap();
    let identity_manifest =
        store_startup_manifest("current-boot", None, &[(selected_id, "agent.reply")]);
    assert_eq!(
        journal
            .recover_store_startup(&identity_manifest, "current-boot", None)
            .unwrap_err()
            .code,
        "ADAPTER_JOURNAL"
    );
    assert_eq!(fs::read(&identity_path).unwrap(), identity_bytes);

    let root_id = expected_root_id(BINDING_ID, GENERATION);
    let root_mismatch_id = "store-front-damaged-root-mismatch";
    let mut root_mismatch_intent = fixture_intent(root_mismatch_id, "agent.reply");
    root_mismatch_intent.native_root_id = Some("ses_swarm_foreign-root".to_owned());
    journal.write_intent(&root_mismatch_intent).unwrap();
    let root_mismatch_path = operation_path(&root.0, root_mismatch_id);
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&root_mismatch_path)
        .unwrap();
    file.write_all(b"{damaged suffix}\n").unwrap();
    let root_mismatch_bytes = fs::read(&root_mismatch_path).unwrap();
    let root_manifest = store_startup_manifest(
        "current-boot",
        Some(&root_id),
        &[(root_mismatch_id, "agent.reply")],
    );
    assert_eq!(
        journal
            .recover_store_startup(&root_manifest, "current-boot", Some(&root_id))
            .unwrap_err()
            .code,
        "NATIVE_IDENTITY_MISMATCH"
    );
    assert_eq!(fs::read(&root_mismatch_path).unwrap(), root_mismatch_bytes);
}

/// Steady state after convergence and drain: reopening reads only the
/// durable closure with targeted loads. Listed entries stay at zero while
/// bodies parsed equal the exact sparse tail, independent of lifetime
/// history.
#[test]
fn steady_recovery_reads_only_the_sparse_tail() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    for index in 0..100 {
        write_acknowledged_reply(&journal, &format!("steady-bulk-{index:03}"), None);
    }
    let unknown_id = "steady-unknown-intent";
    journal
        .write_intent(&fixture_intent(unknown_id, "agent.reply"))
        .unwrap();
    journal.recover_outbox().unwrap();
    for _ in 0..16 {
        let summary = journal
            .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap();
        if summary.queue_drained && journal.startup_recovery_complete() {
            break;
        }
    }
    assert!(journal.startup_recovery_complete());
    drop(journal);

    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let before = journal.startup_scan_counts();
    let states = journal.recover_outbox().unwrap();
    let after = journal.startup_scan_counts();
    assert!(journal.startup_recovery_complete());
    assert_eq!(
        after.operations_listed - before.operations_listed,
        0,
        "steady recovery performs no directory enumeration"
    );
    assert_eq!(
        after.operation_bodies_parsed - before.operation_bodies_parsed,
        1,
        "steady recovery parses exactly the sparse tail"
    );
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].operation_id.as_deref(), Some(unknown_id));
}

/// A durable compact root checkpoint answers hello without reparsing
/// operation bodies, while the pending-bounded outbox scan still runs.
#[test]
fn hello_root_checkpoint_skips_operations_rescan() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let root_id = expected_root_id(BINDING_ID, GENERATION);
    write_acknowledged_reply_with_root(&journal, "hello-root-evidence", &root_id);
    journal.recover_outbox().unwrap();
    let reclaimed = journal
        .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();
    assert_eq!(reclaimed.namespace_removed, 1);
    assert_eq!(
        journal
            .native_root_checkpoint_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap()
            .as_deref(),
        Some(root_id.as_str())
    );

    for index in 0..100 {
        write_acknowledged_reply(&journal, &format!("hello-bulk-{index:03}"), None);
    }
    let pending_id = "hello-pending-outcome";
    journal
        .write_intent(&fixture_intent(pending_id, "agent.reply"))
        .unwrap();
    journal
        .queue_outcome_value(&json!({
            "operation_id": pending_id,
            "outcome": "applied",
            "native_scope_key": NATIVE_SCOPE,
            "details": {}
        }))
        .unwrap();
    // Converge first so the reopened hello answers from the checkpoint
    // plus the bounded dirty set instead of the lifetime history.
    journal.recover_outbox().unwrap();
    for _ in 0..16 {
        let summary = journal
            .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap();
        if summary.queue_drained && journal.startup_recovery_complete() {
            break;
        }
    }
    assert!(journal.startup_recovery_complete());
    drop(journal);

    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let before = journal.startup_scan_counts();
    let root_hint = journal
        .native_root_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();
    let after = journal.startup_scan_counts();
    assert_eq!(root_hint.as_deref(), Some(root_id.as_str()));
    assert_eq!(
        after.operations_listed - before.operations_listed,
        0,
        "hello performs no operations-directory enumeration in steady state"
    );
    assert_eq!(
        after.operation_bodies_parsed - before.operation_bodies_parsed,
        0,
        "hello checks address memos plus outbox, not operation bodies, for conflicts"
    );
    assert_eq!(
        after.outbox_entries_scanned - before.outbox_entries_scanned,
        1,
        "the pending outbox receipt is still consulted"
    );
}

/// A self-consistent but foreign root fails closed at classification and
/// at hello: steady state never binds it, and nothing is deleted around it.
#[test]
fn conflicting_operation_root_fails_closed() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    write_acknowledged_reply(&journal, "conflict-healthy", None);
    let mut bad_intent = fixture_intent("conflict-foreign-root", "agent.reply");
    bad_intent.native_root_id =
        Some("ses_swarm_ffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned());
    journal.write_intent(&bad_intent).unwrap();
    let bad_outcome = json!({
        "operation_id": "conflict-foreign-root",
        "outcome": "applied",
        "native_scope_key": NATIVE_SCOPE,
        "details": {}
    });
    journal.queue_outcome_value(&bad_outcome).unwrap();
    journal.acknowledge_outcome(&bad_outcome).unwrap();
    journal
        .remove_pending(&journal.outbox.join(format!(
            "outcome-{}.json",
            operation_key("conflict-foreign-root")
        )))
        .unwrap();
    let classification = journal.recover_outbox().unwrap_err();
    assert_eq!(classification.code, "NATIVE_IDENTITY_MISMATCH");
    let hello = journal
        .native_root_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap_err();
    assert_eq!(hello.code, "NATIVE_IDENTITY_MISMATCH");
    assert!(operation_path(&root.0, "conflict-healthy").exists());
    assert!(operation_path(&root.0, "conflict-foreign-root").exists());
}
