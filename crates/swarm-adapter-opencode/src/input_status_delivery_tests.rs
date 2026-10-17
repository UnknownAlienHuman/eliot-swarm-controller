//! Production input-status renderer -> real Store::call("module.result").
//! Native facts below are synthetic fixture inputs, not native qualification.
//! All bindings, credentials, command digests and result Operations are admitted
//! through production Store APIs; no private Store hooks or native calls are used.

use super::*;
use std::fs;
use swarm_contracts::{module_catalog::ModuleDescriptor, module_catalog::ProtocolVersion};
use swarm_kernel_host::{
    config::{Config as HostConfig, Route},
    model::Principal,
    platform::{DataRoot, bootstrap_credential},
    store::StoreOwner,
};

const ROUTE: &str = "input-status-delivery-fixture";

struct DeliveryFixture {
    directory: PathBuf,
    owner: StoreOwner,
    operator: Principal,
    module: Principal,
    claim: ModuleContractClaim,
    options: NativeOptions,
    target: OperationIntent,
    target_before: Value,
    message_sha256: String,
}

impl DeliveryFixture {
    async fn start() -> Self {
        let directory =
            std::env::temp_dir().join(format!("swarm-input-status-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let directory = directory.canonicalize().unwrap();
        let workspace = directory.join("workspace");
        fs::create_dir(&workspace).unwrap();
        let options = NativeOptions {
            service_id: "input-status-fixture".into(),
            connection_file: directory.join("unused-native-connection.json"),
            directory: workspace,
            model: None,
        };
        let mut descriptor: Value =
            serde_json::from_str(include_str!("../registration/descriptor.template.json")).unwrap();
        descriptor["enabled"] = json!(true);
        descriptor["launch"]["executable"] = json!(std::env::current_exe().unwrap());
        let descriptor: ModuleDescriptor = serde_json::from_value(descriptor).unwrap();
        descriptor.validate().unwrap();
        let claim = ModuleContractClaim::from_descriptor(
            &descriptor,
            ProtocolVersion { major: 1, minor: 0 },
        )
        .unwrap();
        let data_root = DataRoot::acquire(&directory.join("host")).unwrap();
        let operator_credential = bootstrap_credential(&data_root.path).unwrap();
        let mut config = HostConfig::default();
        config.storage.data_dir = data_root.path.clone();
        config.routes = vec![Route {
            alias: ROUTE.into(),
            runtime: RUNTIME.into(),
            module_artifact_id: ARTIFACT_ID.into(),
            enabled: true,
            native_options: serde_json::to_value(&options).unwrap(),
            workspace_option: None,
            owned_service: None,
            admission_policy: None,
        }];
        let owner = StoreOwner::start(data_root, Arc::new(config), operator_credential.clone())
            .await
            .unwrap();
        let operator = owner.store.authenticate(operator_credential).await.unwrap();
        let supervisor = owner
            .store
            .authenticate(owner.module_supervisor_credential())
            .await
            .unwrap();
        store_call(
            &owner,
            &supervisor,
            "module.descriptor.register",
            json!({"descriptor":descriptor}),
        )
        .await;
        mutate(
            &owner,
            &operator,
            "host.mode",
            json!({"new_work":"enabled"}),
        )
        .await;
        let catalog = store_call(&owner, &operator, "module.catalog.get", json!({})).await;
        mutate(
            &owner,
            &operator,
            "module.route.select",
            json!({
                "route_alias":ROUTE, "module_id":claim.module_id,
                "artifact_id":claim.artifact.artifact_id, "version":claim.artifact.version,
                "expected_catalog_revision":catalog["catalog_revision"],
            }),
        )
        .await;
        let opening = mutate(
            &owner,
            &operator,
            "agent.open",
            json!({
                "lane_id":"input-status-fixture-lane", "route":ROUTE,
            }),
        )
        .await;
        let binding = opening["binding_id"].as_str().unwrap().to_owned();
        let generation = opening["generation"].as_i64().unwrap();
        let module_credential = Credential {
            client_id: "input-status-module".into(),
            token: format!("{}{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4()),
        };
        mutate(
            &owner,
            &operator,
            "client.register",
            json!({
                "client_id":module_credential.client_id, "role":"module",
                "token_hash":swarm_kernel_host::model::digest(module_credential.token.as_bytes()),
                "binding_id":binding, "binding_generation":generation,
            }),
        )
        .await;
        let module = owner.store.authenticate(module_credential).await.unwrap();
        let boot = uuid::Uuid::new_v4().to_string();
        store_call(
            &owner,
            &module,
            "module.hello",
            json!({
                "boot_id":boot, "module_artifact_id":ARTIFACT_ID, "module_contract":claim,
                "native_ready":false,
            }),
        )
        .await;
        let open = next_command(&owner, &module, opening["operation_id"].as_str().unwrap()).await;
        assert_eq!(open.method, "agent.open");
        let open_intent = intent_for(&open, &claim, &options, None, None).unwrap();
        // A synthetic module receipt establishes only this disposable Store
        // binding. No executable, native service or provider is started.
        store_call(
            &owner,
            &module,
            "module.outcome",
            serde_json::to_value(applied_from_intent(&open_intent, None).unwrap()).unwrap(),
        )
        .await;
        let native_root = root_id(&binding, generation);
        store_call(
            &owner,
            &module,
            "module.hello",
            json!({
                "boot_id":boot, "module_artifact_id":ARTIFACT_ID, "module_contract":claim,
                "native_root_id":native_root, "native_scope_key":options.scope_key(),
                "native_ready":true,
            }),
        )
        .await;
        let sending = mutate(
            &owner,
            &operator,
            "agent.send",
            json!({
                "binding_id":binding, "generation":generation,
                "text":"Input delivery fixture; no native execution", "delivery":"next_turn",
            }),
        )
        .await;
        let command =
            next_command(&owner, &module, sending["operation_id"].as_str().unwrap()).await;
        assert_eq!(command.method, "agent.send");
        let text = command.input["text"].as_str().unwrap();
        let target = intent_for(
            &command,
            &claim,
            &options,
            Some(input_id(&command.operation_id)),
            Some(text),
        )
        .unwrap();
        let message_sha256 = digest_json(&json!({
            "id":target.native_input_id, "type":"user", "text":text,
            "metadata":{"eliot":target.marker},
        }))
        .unwrap();
        store_call(
            &owner,
            &module,
            "module.outcome",
            serde_json::to_value(
                unknown_from_intent(&target, "FIXTURE_NATIVE_HISTORY_UNAVAILABLE").unwrap(),
            )
            .unwrap(),
        )
        .await;
        let target_before = operation(&owner, &operator, &target.operation_id).await;
        assert_eq!(target_before["state"], "outcome_unknown");
        Self {
            directory,
            owner,
            operator,
            module,
            claim,
            options,
            target,
            target_before,
            message_sha256,
        }
    }

    fn expected_body(&self) -> Vec<u8> {
        // Independent seven-field consumer contract. Correlation diagnostics
        // belong to source metadata, not these canonical body bytes.
        serde_json::to_vec(&json!({
            "status":"native_input_admitted", "input_operation_id":self.target.operation_id,
            "native_session_id":self.target.native_root_id,
            "native_input_id":self.target.native_input_id,
            "input_message_sha256":self.message_sha256,
            "task_completion":"unknown", "execution_complete":false,
        }))
        .unwrap()
    }

    async fn rendered_page(&self, offset: usize, length: usize) -> (RuntimeCommand, Value) {
        let requested = mutate(
            &self.owner,
            &self.operator,
            "agent.result",
            json!({
                "binding_id":self.target.binding_id, "generation":self.target.generation,
                "selector":{"kind":"input_status", "input_operation_id":self.target.operation_id,
                    "session_id":self.target.native_root_id},
                "offset_bytes":offset, "length_bytes":length,
            }),
        )
        .await;
        let command = next_command(
            &self.owner,
            &self.module,
            requested["operation_id"].as_str().unwrap(),
        )
        .await;
        assert_eq!(command.method, "agent.result");
        let receipt = module_receipt::for_target_intent(
            &self.claim,
            &self.target,
            &self.target.operation_id,
            command.target_input_sha256.as_deref(),
            &self.target.binding_id,
            self.target.generation,
        )
        .unwrap();
        let mut intent = intent_for(&command, &self.claim, &self.options, None, None).unwrap();
        intent.result_input_status = Some(ResultInputStatusIntent {
            input_operation_id: self.target.operation_id.clone(),
            native_session_id: self.target.native_root_id.clone().unwrap(),
            native_input_id: self.target.native_input_id.clone().unwrap(),
            target_module_receipt: receipt.clone(),
        });
        let page = input_status_result_page(
            &command,
            &intent,
            &self.target,
            &receipt,
            &self.message_sha256,
        )
        .unwrap();
        assert_eq!(
            page["page"]["source"]["assistant_result_correlation"],
            "not_exposed"
        );
        assert_eq!(
            page["page"]["source"]["assistant_result_correlation_reason"],
            "assistant_message_has_no_input_parent_in_public_projection"
        );
        (command, page)
    }

    async fn deliver(&self, page: &Value) -> swarm_kernel_host::error::Result<Value> {
        self.owner
            .store
            .call(self.module.clone(), "module.result".into(), page.clone())
            .await
    }

    async fn assert_artifact(&self, ack: &Value, expected: &[u8]) {
        assert_eq!(ack["recorded"], true);
        let artifact = ack["artifact_ref"].as_str().unwrap();
        let read = store_call(
            &self.owner,
            &self.operator,
            "artifact.read",
            json!({
                "artifact_id":artifact, "offset_bytes":0, "length_bytes":65_536,
            }),
        )
        .await;
        assert_eq!(read["content"].as_str().unwrap().as_bytes(), expected);
        assert_eq!(read["eof"], true);
        assert_eq!(
            operation(&self.owner, &self.operator, &self.target.operation_id).await,
            self.target_before,
            "a result read must not settle or change its unknown source send"
        );
    }

    async fn close(self) {
        self.owner.close().await.unwrap();
        fs::remove_dir_all(self.directory).unwrap();
    }
}

async fn store_call(
    owner: &StoreOwner,
    principal: &Principal,
    method: &str,
    params: Value,
) -> Value {
    owner
        .store
        .call(principal.clone(), method.into(), params)
        .await
        .unwrap_or_else(|error| panic!("fixture Store {method}: {}", error.code))
}

async fn mutate(
    owner: &StoreOwner,
    principal: &Principal,
    method: &str,
    mut params: Value,
) -> Value {
    params["client_request_id"] = json!(uuid::Uuid::new_v4().to_string());
    store_call(owner, principal, method, params).await
}

async fn next_command(owner: &StoreOwner, module: &Principal, expected: &str) -> RuntimeCommand {
    let next = tokio::time::timeout(
        Duration::from_secs(3),
        store_call(owner, module, "module.next", json!({})),
    )
    .await
    .expect("the admitted command was not delivered");
    assert_eq!(next["command"]["operation_id"], expected, "{next}");
    serde_json::from_value(next["command"].clone()).unwrap()
}

async fn operation(owner: &StoreOwner, operator: &Principal, id: &str) -> Value {
    store_call(owner, operator, "operation.get", json!({"operation_id":id})).await
}

fn replace_full_body(page: &mut Value, bytes: &[u8]) {
    page["page"]["content_base64"] = json!(STANDARD.encode(bytes));
    page["page"]["byte_length"] = json!(bytes.len());
    page["page"]["total_bytes"] = json!(bytes.len());
    page["page"]["page_sha256"] = json!(format!("{:x}", Sha256::digest(bytes)));
    page["page"]["eof"] = json!(true);
}

#[tokio::test]
async fn input_status_renderer_full_page_reaches_store_and_legacy_extra_body_is_rejected() {
    let fixture = DeliveryFixture::start().await;
    let expected = fixture.expected_body();
    let (command, page) = fixture.rendered_page(0, 65_536).await;
    let mut old_body: Value = serde_json::from_slice(&expected).unwrap();
    old_body["assistant_result_correlation"] = json!("not_exposed");
    old_body["assistant_result_correlation_reason"] =
        json!("assistant_message_has_no_input_parent_in_public_projection");
    let mut old_page = page.clone();
    replace_full_body(&mut old_page, &serde_json::to_vec(&old_body).unwrap());
    let error = fixture.deliver(&old_page).await.expect_err(
        "validly hashed old body with correlation fields must fail Store provenance validation",
    );
    assert_eq!(error.code, "RESULT_PROVENANCE_INVALID");
    assert_eq!(
        operation(&fixture.owner, &fixture.operator, &command.operation_id).await["state"],
        "sending"
    );

    // Submit the unmodified production renderer output, not a repaired copy.
    // This reaches the real Store validator and fails on the pre-0.7.3 bug.
    let ack = fixture
        .deliver(&page)
        .await
        .expect("production full-page renderer must satisfy Store");
    assert_eq!(
        STANDARD
            .decode(page["page"]["content_base64"].as_str().unwrap())
            .unwrap(),
        expected
    );
    fixture.assert_artifact(&ack, &expected).await;
    assert_eq!(
        operation(&fixture.owner, &fixture.operator, &command.operation_id).await["state"],
        "settled"
    );
    let replay = fixture.deliver(&page).await.unwrap();
    assert_eq!(replay["replayed"], true);
    assert_eq!(replay["artifact_ref"], ack["artifact_ref"]);
    fixture.assert_artifact(&replay, &expected).await;
    fixture.close().await;
}

#[tokio::test]
async fn input_status_renderer_exact_slices_are_store_accepted_without_changing_source() {
    let fixture = DeliveryFixture::start().await;
    let expected = fixture.expected_body();
    for (offset, length) in [(0, 17), (13, 29), (expected.len() - 19, 64)] {
        let (command, page) = fixture.rendered_page(offset, length).await;
        let end = expected.len().min(offset + length);
        let ack = fixture
            .deliver(&page)
            .await
            .expect("production sliced-page renderer must satisfy Store");
        assert_eq!(page["page"]["offset_bytes"], offset);
        assert_eq!(page["page"]["total_bytes"], expected.len());
        assert_eq!(page["page"]["eof"], end == expected.len());
        assert_eq!(
            STANDARD
                .decode(page["page"]["content_base64"].as_str().unwrap())
                .unwrap(),
            expected[offset..end]
        );
        fixture.assert_artifact(&ack, &expected[offset..end]).await;
        assert_eq!(
            operation(&fixture.owner, &fixture.operator, &command.operation_id).await["state"],
            "settled"
        );
    }
    fixture.close().await;
}
