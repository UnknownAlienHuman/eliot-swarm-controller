use super::*;
use crate::{
    config::{Config, Route},
    error::{Error, Result},
    model::{self, Principal, Role},
    platform::{DataRoot, bootstrap_credential},
};
use rusqlite::{TransactionBehavior, params};
use serde_json::{Value, json};
use std::{
    io::{BufRead, BufReader, Read, Write},
    path::PathBuf,
    process::{Command, Stdio},
    sync::Arc,
    thread,
};

const ARTIFACT_ID: &str = "claude-agent-sdk-0.3.287-bridge.5";
const BINDING_ID: &str = "bridge5-result-binding";
const MODULE_CLIENT_ID: &str = "bridge5-result-module";
const MODULE_LINK_ID: &str = "bridge5-result-link-1";
const BRIDGE_BOOT_ID: &str = "bridge5-result-boot-1";
const NATIVE_SESSION_ID: &str = "ses_bridge5_result_fixture";
const NATIVE_SCOPE_KEY: &str = "claude:/fixture/project";
const TASK_ID: &str = "bridge5-result-task";
const ATTEMPT_ID: &str = "bridge5-result-attempt";
const DISPATCH_OPERATION_ID: &str = "bridge5-result-dispatch";
const RESULT_TEXT: &str = "Bridge.5 reads the exact pinned SDK result frame.";
const DISPATCH_TEXT: &str = "Return the exact result fixture.";

struct TempStore(PathBuf);

impl TempStore {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("claude-bridge5-result-store-{}", model::new_id()));
        std::fs::create_dir_all(&path).expect("create isolated result Store root");
        Self(path.canonicalize().expect("canonicalize result Store root"))
    }
}

impl Drop for TempStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct FixtureChild(std::process::Child);

impl Drop for FixtureChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn publish_page_in_bridge(
    store: &super::Store,
    module: &Principal,
    command: &Value,
    result_frame_uuid: &str,
    native_input_id: &str,
) -> Result<Value> {
    let origin = &command["input"]["claude_result_origin"];
    let input = json!({
        "command":command,
        "module_contract":{
            "module_id":"claude",
            "artifact":origin["descriptor"]["artifact"],
            "protocol":origin["descriptor"]["protocol"]
        },
        "init_frame_uuid":"sdk-init-result-fixture",
        "model":"claude-sonnet-fixture",
        "result_message":{
            "type":"result",
            "subtype":"success",
            "uuid":result_frame_uuid,
            "session_id":origin["native_session_id"],
            "user_message_uuid":native_input_id,
            "user_message_uuids":[native_input_id],
            "result_index":0,
            "is_error":false,
            "stop_reason":"end_turn",
            "result":RESULT_TEXT
        }
    });
    let input_line = model::canonical(&input)?;
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../modules/claude/fixtures/build-store-result-page.mjs");
    let mut child = FixtureChild(
        Command::new("node")
            .arg(script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| Error::new("FIXTURE_NODE_UNAVAILABLE", error.to_string()))?,
    );
    let mut child_stdin = child.0.stdin.take().ok_or_else(|| {
        Error::new(
            "FIXTURE_NODE_STDIN",
            "result-page producer stdin is unavailable",
        )
    })?;
    let stdout = child.0.stdout.take().ok_or_else(|| {
        Error::new(
            "FIXTURE_NODE_STDOUT",
            "result-page producer stdout is unavailable",
        )
    })?;
    let stderr = child.0.stderr.take().ok_or_else(|| {
        Error::new(
            "FIXTURE_NODE_STDERR",
            "result-page producer stderr is unavailable",
        )
    })?;
    let stderr_reader = thread::spawn(move || {
        let mut text = String::new();
        let _ = BufReader::new(stderr).read_to_string(&mut text);
        text
    });
    writeln!(child_stdin, "{input_line}")
        .map_err(|error| Error::new("FIXTURE_NODE_STDIN", error.to_string()))?;

    let mut stdout = BufReader::new(stdout);
    let mut request_line = String::new();
    let read = stdout
        .read_line(&mut request_line)
        .map_err(|error| Error::new("FIXTURE_NODE_STDOUT", error.to_string()))?;
    if read == 0 {
        return Err(Error::new(
            "FIXTURE_NODE_RESULT_PAGE",
            "Bridge.5 fixture exited before publishing module.result",
        ));
    }
    let request: Value = serde_json::from_str(&request_line)
        .map_err(|error| Error::new("FIXTURE_NODE_RESULT_PAGE", error.to_string()))?;
    if request["type"] != "module.result" || request["method"] != "module.result" {
        return Err(Error::new(
            "FIXTURE_NODE_RESULT_PAGE",
            "Bridge.5 fixture did not publish the module.result method",
        ));
    }
    let method = model::text(&request, "method")?.to_owned();
    let payload = json!({"operation_id":request["operation_id"],"page":request["page"]});
    let acknowledgement = match store.call(module.clone(), method, payload).await {
        Ok(acknowledgement) => acknowledgement,
        Err(error) => {
            if let Ok(reply) = model::canonical(&json!({"type":"error","code":error.code})) {
                let _ = writeln!(child_stdin, "{reply}");
            }
            drop(child_stdin);
            let _ = child.0.wait();
            let _ = stderr_reader.join();
            return Err(error);
        }
    };
    let reply = model::canonical(&json!({"type":"ack","acknowledgement":acknowledgement}))?;
    writeln!(child_stdin, "{reply}")
        .map_err(|error| Error::new("FIXTURE_NODE_STDIN", error.to_string()))?;
    drop(child_stdin);

    let mut terminal_line = String::new();
    let read = stdout
        .read_line(&mut terminal_line)
        .map_err(|error| Error::new("FIXTURE_NODE_STDOUT", error.to_string()))?;
    if read == 0 {
        return Err(Error::new(
            "FIXTURE_NODE_RESULT_PAGE",
            "Bridge.5 fixture exited before consuming the Store ACK",
        ));
    }
    let terminal: Value = serde_json::from_str(&terminal_line)
        .map_err(|error| Error::new("FIXTURE_NODE_RESULT_PAGE", error.to_string()))?;
    let status = child
        .0
        .wait()
        .map_err(|error| Error::new("FIXTURE_NODE_EXIT", error.to_string()))?;
    let stderr = stderr_reader
        .join()
        .unwrap_or_else(|_| "fixture stderr reader panicked".to_owned());
    if !status.success() {
        return Err(Error::new("FIXTURE_NODE_RESULT_PAGE", stderr));
    }
    if terminal["type"] != "terminal"
        || terminal["acknowledgement"] != acknowledgement
        || terminal["replay_call_count"] != 0
    {
        return Err(Error::new(
            "FIXTURE_NODE_RESULT_PAGE",
            "Bridge.5 fixture did not retain the exact Store ACK or replay fence",
        ));
    }
    Ok(json!({
        "page":request["page"],
        "execution":terminal["execution"],
        "store_ack":acknowledgement,
        "terminal":terminal
    }))
}

#[tokio::test]
async fn bridge5_sdk_result_page_is_store_acked_readable_and_retired_boot_fenced() -> Result<()> {
    let temp = TempStore::new();
    let root = DataRoot::acquire(&temp.0).expect("acquire fixture Store root");
    let credential = bootstrap_credential(&root.path).expect("bootstrap fixture manager");
    let workspace = std::env::current_dir()
        .expect("read fixture workspace")
        .to_string_lossy()
        .into_owned();
    let route = Route {
        alias: "claude-bridge5-result-fixture".to_owned(),
        runtime: "module".to_owned(),
        module_artifact_id: ARTIFACT_ID.to_owned(),
        enabled: true,
        native_options: json!({"workspaceRoot":workspace,"modelId":"sonnet"}),
        workspace_option: Some("workspaceRoot".to_owned()),
        owned_service: None,
        admission_policy: None,
    };
    let mut config = Config::default();
    config.storage.data_dir = temp.0.clone();
    config.routes.push(route.clone());
    let owner = StoreOwner::start(root, Arc::new(config.clone()), credential.clone())
        .await
        .expect("start isolated Store");
    let caller = owner
        .store
        .authenticate(credential)
        .await
        .expect("authenticate fixture manager");

    let fixture_route = route.clone();
    let fixture_config = config.clone();
    let caller_id = caller.client_id.clone();
    let fixture_caller = caller.clone();
    let module_client_id = MODULE_CLIENT_ID.to_owned();
    owner
        .store
        .run_fixture(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let mut descriptor_value: Value = serde_json::from_str(include_str!(
                "../../../../modules/claude/module-descriptor.template.json"
            ))?;
            descriptor_value["enabled"] = json!(true);
            descriptor_value["launch"]["executable"] = json!(
                std::env::current_exe()
                    .map_err(|error| Error::new("FIXTURE_EXE_UNAVAILABLE", error.to_string()))?
                    .to_string_lossy()
                    .into_owned()
            );
            let descriptor: swarm_contracts::module_catalog::ModuleDescriptor =
                serde_json::from_value(descriptor_value).map_err(|_| {
                    Error::new("MODULE_DESCRIPTOR_INVALID", "fixture bridge.5 descriptor is malformed")
                })?;
            let registered = super::module_handshake::register_trusted_descriptor(&tx, descriptor)?;
            let selection = super::module_handshake::select_route(
                &tx,
                &fixture_caller,
                &json!({
                    "route_alias":fixture_route.alias,
                    "module_id":"claude",
                    "artifact_id":ARTIFACT_ID,
                    "version":"5",
                    "expected_catalog_revision":registered["catalog_revision"]
                }),
                &fixture_config,
                "bridge5-result-route-selection",
            )?;

            let route_json = model::canonical(&json!(fixture_route))?;
            let state_json = model::canonical(&json!({
                "module_contract_selector":selection["selection"],
                "module_client_id":module_client_id.clone(),
                "module_link_id":MODULE_LINK_ID,
                "bridge_boot_id":BRIDGE_BOOT_ID,
                "connection":"connected",
                "opening_evidence":{
                    "completion_condition":"native_executor_prepared",
                    "native_session_state":"prepared",
                    "pre_input_executor_ready":true,
                    "bridge_boot_id":BRIDGE_BOOT_ID,
                    "describe":{"session_id":null}
                }
            }))?;
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,'bridge5-result-lane','bridge5-result-instance',?2,'ready',NULL,NULL,?3,?4,1)",
                params![BINDING_ID, ARTIFACT_ID, route_json, state_json],
            )?;
            super::set_meta(
                &tx,
                &format!("client:{module_client_id}"),
                &json!({
                    "role":"module",
                    "disabled":false,
                    "internal_only":false,
                    "binding_id":BINDING_ID,
                    "binding_generation":1
                }),
            )?;

            let source_text = "The selected fixture source is stable.";
            let spec = json!({
                "objective":"Return the exact result fixture.",
                "phase":"verification",
                "source_refs":["fixture:result-source"],
                "source_index":[{
                    "source_ref":"fixture:result-source",
                    "revision":"result-source-v1",
                    "content_sha256":model::digest(source_text.as_bytes()),
                    "text":source_text,
                    "status":"selected",
                    "gap_reason":null
                }]
            });
            let snapshot = json!({
                "spec":spec,
                "revision":1,
                "dependency_acceptances":[],
                "baseline_candidate":{"status":"wide","reason":"baseline_not_configured"},
                "brief":{"objective":"Return the exact result fixture.","phase":"verification"}
            });
            tx.execute(
                "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
                 VALUES(?1,'bridge5-result-project',1,'open',?2,1,1)",
                params![TASK_ID, model::canonical(&spec)?],
            )?;
            tx.execute(
                "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,producers_json,created_at_ms,updated_at_ms) \
                 VALUES(?1,?2,1,?3,?4,'controller',?5,1,'reserved','[]',1,1)",
                params![
                    ATTEMPT_ID,
                    TASK_ID,
                    model::canonical(&snapshot)?,
                    caller_id,
                    BINDING_ID
                ],
            )?;
            let attempt = super::tasks::get_attempt(&tx, ATTEMPT_ID)?;
            let prompt = super::task_prompt::build(&attempt, DISPATCH_TEXT, None)?;
            let request = json!({
                "client_request_id":"bridge5-result-dispatch-request",
                "binding_id":BINDING_ID,
                "generation":1,
                "attempt_id":ATTEMPT_ID,
                "text":DISPATCH_TEXT
            });
            let effective = json!({
                "route":fixture_route,
                "input":DISPATCH_TEXT,
                "task_snapshot":snapshot,
                "task_prompt":prompt,
                "operation_contract":{"task_prompt":{
                    "contract_revision":swarm_contracts::task_prompt::TASK_PROMPT_CONTRACT_REVISION
                }}
            });
            tx.execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,due_at_ms,created_at_ms,updated_at_ms) \
                 VALUES(?1,?2,'bridge5-result-dispatch-request','task.dispatch',?3,?4,?5,?6,?7,1,'queued',1,1,1)",
                params![
                    DISPATCH_OPERATION_ID,
                    caller_id,
                    model::canonical(&request)?,
                    model::canonical(&effective)?,
                    TASK_ID,
                    ATTEMPT_ID,
                    BINDING_ID,
                ],
            )?;
            tx.execute(
                "UPDATE attempts SET start_operation_id=?2 WHERE attempt_id=?1",
                params![ATTEMPT_ID, DISPATCH_OPERATION_ID],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .expect("select the shipped bridge.5 descriptor and seed one exact Task dispatch");

    let module = Principal {
        link_id: MODULE_LINK_ID.to_owned(),
        client_id: MODULE_CLIENT_ID.to_owned(),
        role: Role::Module,
    };
    let dispatch_delivery = owner
        .store
        .call(module.clone(), "module.next".to_owned(), json!({}))
        .await
        .expect("deliver Task dispatch through the selected module route");
    let dispatch = dispatch_delivery["command"].clone();
    assert_eq!(dispatch["method"], "task.dispatch");
    assert_eq!(dispatch["route"]["runtime"], "module");
    assert_eq!(dispatch["route"]["module_artifact_id"], ARTIFACT_ID);

    let context = dispatch["input"]["task_dispatch_context"].clone();
    let prompt = dispatch["input"]["task_prompt"].clone();
    let input_id = uuid::Uuid::new_v4().to_string();
    let module_receipt = json!({
        "schema_version":1,
        "module_id":"claude",
        "artifact":{"artifact_id":ARTIFACT_ID,"version":"5","build_id":null},
        "protocol":{"major":1,"minor":0},
        "binding_id":BINDING_ID,
        "binding_generation":1,
        "operation_id":DISPATCH_OPERATION_ID,
        "input_sha256":dispatch["input_sha256"]
    });
    let dispatch_admission = json!({
        "schema_version":1,
        "module_receipt":module_receipt,
        "operation_id":DISPATCH_OPERATION_ID,
        "binding_id":BINDING_ID,
        "binding_generation":1,
        "worker_boot_id":BRIDGE_BOOT_ID,
        "attempt_id":context["attempt_id"],
        "task_id":context["task_id"],
        "task_revision":context["task_revision"],
        "task_snapshot_sha256":context["task_snapshot_sha256"],
        "source_text_sha256":context["source_text_sha256"],
        "source_text_bytes":context["source_text_bytes"],
        "native_payload_sha256":prompt["prompt_sha256"],
        "native_payload_bytes":prompt["prompt_bytes"],
        "native_input_id":input_id
    });
    let dispatch_outcome = json!({
        "operation_id":DISPATCH_OPERATION_ID,
        "outcome":"applied",
        "native_root_id":NATIVE_SESSION_ID,
        "native_scope_key":NATIVE_SCOPE_KEY,
        "turn_id":null,
        "native_input_id":input_id,
        "details":{
            "module_receipt":module_receipt,
            "dispatch_admission":dispatch_admission,
            "completion_condition":"native_input_admitted",
            "evidence":"native_frame_echo",
            "execution_complete":false,
            "task_completion":"unknown",
            "user_message_uuid":input_id,
            "initial_task_dispatch":true,
            "bridge_boot_id":BRIDGE_BOOT_ID,
            "native_frame_session_id":NATIVE_SESSION_ID,
            "system_init_session_id":NATIVE_SESSION_ID,
            "native_scope_key":NATIVE_SCOPE_KEY,
            "prompt_sha256":prompt["prompt_sha256"],
            "prompt_bytes":prompt["prompt_bytes"]
        }
    });
    let dispatch_ack = owner
        .store
        .call(
            module.clone(),
            "module.outcome".to_owned(),
            dispatch_outcome,
        )
        .await
        .expect("record the exact Bridge.5 native input admission");
    assert_eq!(dispatch_ack["recorded"], true);

    let result_request = json!({
        "client_request_id":"bridge5-result-page-request",
        "binding_id":BINDING_ID,
        "generation":1,
        "selector":{
            "kind":"claude_assistant_result",
            "input_operation_id":DISPATCH_OPERATION_ID,
            "session_id":NATIVE_SESSION_ID
        },
        "offset_bytes":0,
        "length_bytes":65_536
    });
    let queued_result = owner
        .store
        .call(caller.clone(), "agent.result".to_owned(), result_request)
        .await
        .expect("admit result against this binding, generation and native root");
    let result_operation_id = model::text(&queued_result, "operation_id")?.to_owned();
    let result_delivery = owner
        .store
        .call(module.clone(), "module.next".to_owned(), json!({}))
        .await
        .expect("deliver the result Operation to the selected Bridge.5 module");
    let result_command = result_delivery["command"].clone();
    assert_eq!(result_command["operation_id"], result_operation_id);
    assert_eq!(result_command["method"], "agent.result");
    assert_eq!(
        result_command["input"]["claude_result_origin"]["bridge_boot_id"],
        BRIDGE_BOOT_ID
    );
    let dispatch_operation_id_for_origin = DISPATCH_OPERATION_ID.to_owned();
    let dispatch_operation = owner
        .store
        .run(move |db| super::operations::get_operation(db, &dispatch_operation_id_for_origin))
        .await?;
    let origin = &result_command["input"]["claude_result_origin"];
    let dispatch_result = &dispatch_operation["result"];
    assert_eq!(dispatch_result["outcome"], "applied");
    assert_eq!(
        dispatch_result["native_root_id"],
        origin["native_session_id"]
    );
    assert_eq!(
        dispatch_result["native_scope_key"],
        origin["native_scope_key"]
    );
    assert_eq!(
        dispatch_result["details"]["bridge_boot_id"],
        origin["bridge_boot_id"]
    );
    assert_eq!(
        dispatch_result["details"]["completion_condition"],
        "native_input_admitted"
    );
    assert_eq!(dispatch_result["details"]["execution_complete"], false);
    assert_eq!(dispatch_result["details"]["task_completion"], "unknown");
    let dispatched_receipt: swarm_contracts::runtime::ModuleReceiptIdentity =
        serde_json::from_value(dispatch_result["details"]["module_receipt"].clone())?;
    let sealed_receipt: swarm_contracts::runtime::ModuleReceiptIdentity =
        serde_json::from_value(origin["target_module_receipt"].clone())?;
    assert_eq!(dispatched_receipt, sealed_receipt);
    let dispatched_admission: swarm_contracts::runtime::TaskDispatchAdmissionReceipt =
        serde_json::from_value(dispatch_result["details"]["dispatch_admission"].clone())?;
    let sealed_admission: swarm_contracts::runtime::TaskDispatchAdmissionReceipt =
        serde_json::from_value(origin["dispatch_admission"].clone())?;
    assert_eq!(dispatched_admission, sealed_admission);

    let module_page = publish_page_in_bridge(
        &owner.store,
        &module,
        &result_command,
        "sdk-result-frame-1",
        &input_id,
    )
    .await?;
    let page = module_page["page"].clone();
    assert_eq!(
        module_page["execution"]["result_frame_uuid"],
        "sdk-result-frame-1"
    );
    assert_eq!(
        page["source"]["target_module_receipt"],
        origin["target_module_receipt"]
    );
    let result_ack = module_page["store_ack"].clone();
    assert_eq!(module_page["terminal"]["replay_call_count"], 0);
    assert_eq!(result_ack["recorded"], true);
    let artifact_ref = model::text(&result_ack, "artifact_ref")?.to_owned();

    let operation_id_for_read = result_operation_id.clone();
    let (operation, effective_raw) = owner
        .store
        .run(move |db| {
            let operation = super::operations::get_operation(db, &operation_id_for_read)?;
            let raw: String = db.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                [&operation_id_for_read],
                |row| row.get(0),
            )?;
            Ok((operation, raw))
        })
        .await
        .expect("read back the settled result Operation");
    let effective: Value = serde_json::from_str(&effective_raw)?;
    assert_eq!(operation["state"], "settled");
    assert_eq!(operation["result"]["outcome"], "applied");
    assert_eq!(operation["result"]["details"]["artifact_ref"], artifact_ref);
    assert_eq!(
        effective["claude_result_origin"]["bridge_boot_id"],
        BRIDGE_BOOT_ID
    );

    let artifact_read = owner
        .store
        .call(
            caller.clone(),
            "artifact.read".to_owned(),
            json!({
                "artifact_id":artifact_ref,
                "offset_bytes":0,
                "length_bytes":RESULT_TEXT.len()
            }),
        )
        .await
        .expect("read the exact retained Store result bytes");
    assert_eq!(artifact_read["content"], RESULT_TEXT);
    assert_eq!(artifact_read["eof"], true);

    let replacement_link = "bridge5-result-link-2";
    let replacement_boot = "bridge5-result-boot-2";
    owner
        .store
        .run_fixture(move |db| {
            db.execute(
                "UPDATE bindings SET state='reconciling',state_json=json_set(state_json,'$.previous_bridge_boot_id',?2,'$.bridge_boot_id',?3,'$.module_link_id',?4,'$.connection','connected') WHERE binding_id=?1 AND generation=1",
                params![BINDING_ID, BRIDGE_BOOT_ID, replacement_boot, replacement_link],
            )?;
            Ok(())
        })
        .await
        .expect("record the accepted replacement boot/link in Store state");
    let replacement_module = Principal {
        link_id: replacement_link.to_owned(),
        client_id: MODULE_CLIENT_ID.to_owned(),
        role: Role::Module,
    };
    let stale_page = owner
        .store
        .call(
            replacement_module,
            "module.result".to_owned(),
            json!({"operation_id":result_operation_id,"page":page}),
        )
        .await
        .expect_err("a page from the replaced boot must not publish through the new link");
    assert_eq!(stale_page.code, "CLAUDE_RESULT_RETIRED_BOOT");
    let operation_id_for_final_read = result_operation_id.clone();
    let final_operation = owner
        .store
        .run(move |db| super::operations::get_operation(db, &operation_id_for_final_read))
        .await
        .expect("verify stale delivery left the exact settled result unchanged");
    assert_eq!(final_operation["state"], "settled");
    assert_eq!(
        final_operation["result"]["details"]["artifact_ref"],
        artifact_ref
    );
    Ok(())
}
