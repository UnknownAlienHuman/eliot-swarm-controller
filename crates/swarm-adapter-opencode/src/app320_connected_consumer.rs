use super::*;
use crate::{
    journal::Journal, module_runtime::OwnedBootstrap, native_owner::NativeOwnerController,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use swarm_contracts::{
    Credential,
    module_catalog::{LaunchValue, ModuleDescriptor},
};
use swarm_kernel_host::{
    config::{Config as HostConfig, McpProfileConfig, McpToolProfile, Route},
    forge::ForgeProject,
    platform::{load_credential, private_permissions},
    workspace::WorkspaceProjectConfig,
};

type FixtureResult<T> = std::result::Result<T, String>;

const PLUGIN_MODULE: &[u8] = include_bytes!("../../../modules/opencode/native-mcp-proof.mjs");
const PLUGIN_ENTRY: &[u8] = include_bytes!("../../../modules/opencode/index.mjs");
const PACKAGE_JSON: &[u8] = include_bytes!("../../../modules/opencode/package.json");
const PACKAGE_LOCK: &[u8] = include_bytes!("../../../modules/opencode/package-lock.json");
const ROUTE_ALIAS: &str = "app320-connected-fixture";
const PROJECT_ID: &str = "app320-owned-launch";
const HOST_CHILD: &str = "app320_connected_consumer::app320_host_startup_child";
const ADAPTER_CHILD: &str = "app320_connected_consumer::app320_owned_adapter_child";

#[derive(serde::Serialize)]
struct CaseEvidence {
    scenario: &'static str,
    operation_id: String,
    binding_id: String,
    generation: i64,
    workspace_directory: PathBuf,
    owner_nonce: String,
    outcome: Value,
    operation_readback: Value,
    observed_outcomes: Vec<Value>,
    acknowledged_sha256: String,
    pending_count: usize,
    key_posts: u64,
    root_posts: u64,
    connection_absent: bool,
    child_released: bool,
    ipc_listener_gone: bool,
}

#[tokio::test]
async fn app320_provider_auth_survives_real_store_outcome_ack_for_both_failures() {
    run_case(false).await.unwrap();
    run_case(true).await.unwrap();
}

// These ignored helpers are subprocess roles, not independently qualifying tests.
// Only the public consumer above creates their private inputs and checks their results.
#[tokio::test]
#[ignore = "spawned with pipe stdin by the APP320 connected consumer only"]
async fn app320_host_startup_child() {
    let path = std::env::var_os("ELIOT_APP320_HOST_CONFIG")
        .map(PathBuf::from)
        .expect("APP320 host child requires its private config path");
    let mut config: HostConfig = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    // The public embedding accepts Config. Resolve the configured Forge/workspace
    // paths just as the foreground loader does; startup itself creates authority.
    config.forge.resolve_paths(path.parent().unwrap()).unwrap();
    config
        .workspace
        .resolve_paths(path.parent().unwrap())
        .unwrap();
    config.workspace.validate(&config.forge).unwrap();
    swarm_kernel_host::host::run_on_stdin_eof(config)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "launched only by the real module supervisor and module-owner helper"]
async fn app320_owned_adapter_child() {
    let case_path = std::env::var_os("ELIOT_APP320_CASE_FILE")
        .map(PathBuf::from)
        .expect("APP320 adapter child requires the descriptor-supplied case path");
    let case: Value = serde_json::from_slice(&fs::read(&case_path).unwrap()).unwrap();
    let evidence_path = PathBuf::from(case["evidence_file"].as_str().unwrap());
    // --skip is a standard libtest option. Its harmless nonmatching filter carries
    // the supervisor-produced ModuleHostConfigPath without inventing a --config
    // option for libtest or constructing an adapter bootstrap in the parent.
    let mut args = std::env::args_os().skip(1);
    let host_path = loop {
        let arg = args
            .next()
            .expect("typed supervisor host-config argument is missing");
        if arg == "--skip" {
            break PathBuf::from(args.next().expect("typed host-config path is missing"));
        }
    };
    let result = run_owned_case(&host_path, &case).await;
    let evidence = match &result {
        Ok(evidence) => serde_json::to_value(evidence).unwrap(),
        // Never print/store an arbitrary native error, credential or auth path.
        Err(_) => json!({"fixture_failure":"APP320_OWNED_COMPOSITION_FAILED"}),
    };
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&evidence_path)
        .expect("one owned worker must exclusively publish the case result");
    use std::io::Write;
    output
        .write_all(&serde_json::to_vec(&evidence).unwrap())
        .unwrap();
    output.sync_all().unwrap();
    assert!(
        result.is_ok(),
        "APP320 owned composition failed; no raw secret diagnostic retained"
    );
}

async fn run_case(invalid_model_catalog: bool) -> FixtureResult<()> {
    let scratch = unique_directory("eliot-app320-owned-connected")?;
    private_permissions(&scratch, true).map_err(display)?;
    let host_data_dir = scratch.join("host");
    let repository = scratch.join("repository");
    let workspace_root = scratch.join("workspaces");
    let hooks = scratch.join("empty-hooks");
    for directory in [&host_data_dir, &repository, &workspace_root, &hooks] {
        fs::create_dir(directory).map_err(display)?;
        private_permissions(directory, true).map_err(display)?;
    }
    let git = fixture_git_executable()?;
    fixture_git(&git, &repository, &hooks, &["init", "--quiet"])?;
    fixture_git(
        &git,
        &repository,
        &hooks,
        &["config", "user.name", "APP320 Fixture"],
    )?;
    fixture_git(
        &git,
        &repository,
        &hooks,
        &["config", "user.email", "app320@example.invalid"],
    )?;
    fs::create_dir(repository.join("src")).map_err(display)?;
    fs::write(repository.join("src/target.rs"), "pub fn target() {}\n").map_err(display)?;
    fixture_git(&git, &repository, &hooks, &["add", "src/target.rs"])?;
    fixture_git(
        &git,
        &repository,
        &hooks,
        &["commit", "--quiet", "-m", "APP320 workspace fixture"],
    )?;
    fixture_git(&git, &repository, &hooks, &["branch", "-M", "main"])?;
    // Identity only. No fetch, push or provider/network operation is performed.
    fixture_git(
        &git,
        &repository,
        &hooks,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/owner/app320-owned-launch.git",
        ],
    )?;

    let auth_source = scratch.join("auth.json");
    let provider_key = format!("app320-fixture-key-{}", uuid::Uuid::new_v4());
    fs::write(
        &auth_source,
        serde_json::to_vec(&json!({
            "openai":{"type":"api","key":provider_key}
        }))
        .map_err(display)?,
    )
    .map_err(display)?;
    private_permissions(&auth_source, false).map_err(display)?;
    let bun_executable = configured_bun()?;
    let install_root = scratch.join("native-install");
    let server_sha256 =
        write_installed_closure(&install_root, &test_server_source(invalid_model_catalog))?;
    let host_executable =
        install_root.join(format!("swarm-kernel-host{}", std::env::consts::EXE_SUFFIX));
    let supervisor = configured_binary("ELIOT_OPENCODE_TEST_SUPERVISOR")?;
    let owner_helper = configured_binary("ELIOT_OPENCODE_TEST_OWNER_HELPER")?;
    fs::copy(
        &supervisor,
        install_root.join(format!("swarm-supervisor{}", std::env::consts::EXE_SUFFIX)),
    )
    .map_err(display)?;
    let evidence_file = scratch.join("case-evidence.json");
    let case_file = scratch.join("case.json");
    fs::write(
        &case_file,
        serde_json::to_vec(&json!({
            "invalid_model_catalog":invalid_model_catalog,
            "evidence_file":evidence_file,
            "admission_file":scratch.join("admission-evidence.json"),
            "execute_file":scratch.join("execute-after-public-readback"),
            "workspace_root":workspace_root,
            "native_base":scratch.join("native-state")
        }))
        .map_err(display)?,
    )
    .map_err(display)?;
    let descriptor_file = write_fixture_descriptor(&install_root, &host_executable, &case_file)?;
    let mut config = HostConfig::default();
    config.storage.data_dir = host_data_dir.clone();
    config.forge.enabled = true;
    config.forge.git_executable = git;
    config.forge.projects.insert(
        PROJECT_ID.to_owned(),
        ForgeProject {
            canonical_repository: "github.com/owner/app320-owned-launch".to_owned(),
            repository_path: repository.clone(),
            remote_name: "origin".to_owned(),
            policy_revision: "owner-policy-v1".to_owned(),
            target_refs: vec!["refs/heads/main".to_owned()],
        },
    );
    config.workspace.projects.insert(
        PROJECT_ID.to_owned(),
        WorkspaceProjectConfig {
            allowed_roots: vec![workspace_root.clone()],
        },
    );
    config.mcp.profiles.insert(
        "app320-participant".to_owned(),
        McpProfileConfig {
            tool_profile: McpToolProfile::Participant,
            expected_client_id: "app320-participant-template".to_owned(),
            surface: Some("participant-core".to_owned()),
            deferred_groups: Vec::new(),
            manual_tools: Vec::new(),
        },
    );
    config.routes.push(Route {
        alias: ROUTE_ALIAS.to_owned(),
        runtime: RUNTIME.to_owned(),
        module_artifact_id: ARTIFACT_ID.to_owned(),
        enabled: true,
        native_options: json!({"directory":repository}),
        workspace_option: Some("directory".to_owned()),
        admission_policy: None,
        owned_service: Some(
            serde_json::from_value(json!({
                "origin":"fresh_owned_service",
                "service_id":format!("app320-{}", uuid::Uuid::new_v4().simple()),
                "model":{"id":"fixture-model","providerID":"openai","variant":"fixture-variant"},
                "model_catalog":"offline",
                "credential_ref":"credential:app320-fixture",
                "bun_executable":bun_executable,
                "bun_sha256":sha256_file(&bun_executable)?,
                "server_program":install_root.join("resources/modules/opencode/serve.mjs"),
                "server_program_sha256":server_sha256,
                "state_root":scratch.join("native-state"),
                "port":0
            }))
            .map_err(display)?,
        ),
    });
    config.module_supervisor.enabled = true;
    config.module_supervisor.install_root = Some(install_root);
    config.module_supervisor.descriptor_files = vec![descriptor_file];
    config.module_supervisor.owner_helper_sha256 = Some(sha256_file(&owner_helper)?);
    config.module_supervisor.owner_helper = Some(owner_helper);
    config
        .module_supervisor
        .protected_files
        .insert("credential:app320-fixture".to_owned(), auth_source.clone());
    let mut config_value = serde_json::to_value(&config).map_err(display)?;
    // These local authorized path references are intentionally skip_serializing
    // in Config. Retain them explicitly in the private fixture startup input,
    // never in the descriptor, Store metadata, evidence or console output.
    config_value["opencode_provider_auth_sources"] = json!({
        "credential:app320-fixture":{"provider_id":"openai","auth_file":auth_source}
    });
    let config_file = scratch.join("host-config.json");
    fs::write(
        &config_file,
        serde_json::to_vec(&config_value).map_err(display)?,
    )
    .map_err(display)?;
    let mut host = tokio::process::Command::new(&host_executable)
        .args(["--exact", HOST_CHILD, "--ignored", "--nocapture"])
        .env("ELIOT_APP320_HOST_CONFIG", &config_file)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(false)
        .spawn()
        .map_err(display)?;
    let host_stdin = host
        .stdin
        .take()
        .ok_or_else(|| "host child stdin is unavailable".to_owned())?;
    let execution = async {
        let operator = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = host.try_wait().map_err(display)? {
                    return Err(format!("public host startup exited before IPC readiness: {status}"));
                }
                if let Ok(credential) = load_credential(&host_data_dir.join("operator.json")) {
                    if call_host(&host_data_dir, &credential, "module.catalog.get", json!({})).await.is_ok() {
                        break Ok(credential);
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.map_err(|_| "public host startup did not become ready".to_owned())??;
        let catalog = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let catalog = call_host(&host_data_dir, &operator, "module.catalog.get", json!({})).await?;
                if let Some(descriptor) = catalog["descriptors"].as_array().and_then(|items| items.iter().find(|item| {
                    item["module_id"] == "eliot.opencode.v2" && item["artifact"]["artifact_id"] == ARTIFACT_ID
                        && item["artifact"]["version"] == ARTIFACT_VERSION && item["enabled"] == true
                })) {
                    call_host(&host_data_dir, &operator, "module.route.select", json!({
                        "client_request_id":uuid::Uuid::new_v4().to_string(),
                        "route_alias":ROUTE_ALIAS,"module_id":descriptor["module_id"],
                        "artifact_id":descriptor["artifact"]["artifact_id"],"version":descriptor["artifact"]["version"],
                        "expected_catalog_revision":catalog["catalog_revision"]
                    })).await?;
                    break Ok::<_, String>(catalog);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.map_err(|_| "real supervisor descriptor registration was not available".to_owned())??;
        if !catalog["catalog_revision"].is_number() {
            return Err("actual catalog omitted its revision".to_owned());
        }
        let task = call_host(&host_data_dir, &operator, "task.create", json!({
            "client_request_id":uuid::Uuid::new_v4().to_string(),"project_id":PROJECT_ID,
            "spec":{"objective":"Retain provider-auth proof after owned native failure",
                "phase":"implementation","requirements":[{"id":"R1","statement":"Retain exact post-effect evidence"}],
                "scope":{"initial_paths":["src/target.rs"]},"owner_policy_id":"owner-policy-v1"}
        })).await?;
        let mut request = json!({
            "task_id":task["task_id"],"expected_task_revision":1,"route":ROUTE_ALIAS,
            "agent_profile":"app320-agent","mcp_profile":"app320-participant","mcp_surface":"participant-core",
            "workspace_policy":"manager_owned_worktree","requested_model":null,"requested_effort":null,
            "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
            "stop_conditions":[],"purpose":"implementation"
        });
        let preview = call_host(&host_data_dir, &operator, "swarm.launch.preview", request.clone()).await?;
        if !preview["hard_blocks"].as_array().is_some_and(Vec::is_empty) {
            return Err("real public owned launch preview reported a blocker".to_owned());
        }
        request["plan_digest"] = preview["plan_digest"].clone();
        request["client_request_id"] = json!(uuid::Uuid::new_v4().to_string());
        let launch = call_host(&host_data_dir, &operator, "swarm.launch", request).await?;
        let launch_id = launch["operation_id"].as_str().ok_or_else(|| "public launch omitted its operation ID".to_owned())?;
        // Read the immutable child association before its rejected ACK causes
        // the launcher to replace progress with a terminal failure projection.
        let admission = tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                if let Ok(bytes) = fs::read(scratch.join("admission-evidence.json")) {
                    if let Ok(value) = serde_json::from_slice::<Value>(&bytes) { break Ok::<_, String>(value); }
                }
                if evidence_file.is_file() { return Err("owned adapter failed before public launch readback".to_owned()); }
                if host.try_wait().map_err(display)?.is_some() { return Err("host exited before admitted command readback".to_owned()); }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.map_err(|_| "real supervisor did not deliver its owned command".to_owned())??;
        let launch_readback = call_host(&host_data_dir, &operator, "operation.get", json!({"operation_id":launch_id})).await?;
        let command_readback = call_host(&host_data_dir, &operator, "operation.get", json!({"operation_id":admission["operation_id"]})).await?;
        if command_readback["method"] != "agent.open" || command_readback["task_id"] != task["task_id"]
            || command_readback["attempt_id"] != launch_readback["attempt_id"]
            || !command_readback["attempt_id"].as_str().is_some_and(|id| !id.is_empty())
            || command_readback["prerequisite_operation_id"] != launch_id
            || command_readback["binding_id"] != admission["binding_id"]
            || command_readback["binding_generation"] != admission["generation"] {
            return Err("public launch did not retain this exact admitted agent.open/Task/Attempt/binding".to_owned());
        }
        fs::write(scratch.join("execute-after-public-readback"), b"continue\n").map_err(display)?;
        let mut evidence = tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                if let Ok(bytes) = fs::read(&evidence_file) {
                    if let Ok(evidence) = serde_json::from_slice::<Value>(&bytes) { break Ok::<_, String>(evidence); }
                }
                if host.try_wait().map_err(display)?.is_some() { return Err("host exited before owned worker evidence".to_owned()); }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.map_err(|_| "owned worker did not publish bounded connected evidence".to_owned())??;
        if evidence.get("fixture_failure").is_some() { return Err("actual owned composition failed; private scratch retained".to_owned()); }
        let operation_id = evidence["operation_id"].as_str().ok_or_else(|| "worker evidence omitted its actual command".to_owned())?;
        let operation = call_host(&host_data_dir, &operator, "operation.get", json!({"operation_id":operation_id})).await?;
        if operation["method"] != "agent.open" || operation["task_id"] != task["task_id"]
            || operation["binding_id"] != evidence["binding_id"] || operation["binding_generation"] != evidence["generation"]
            || operation["attempt_id"] != launch_readback["attempt_id"] || operation["prerequisite_operation_id"] != launch_id
            || admission["operation_id"] != operation_id || admission["owner_nonce"] != evidence["owner_nonce"]
            || operation["state"] != "rejected" || operation["result"]["outcome"] != "rejected" {
            return Err("public readback did not bind the rejected command to this exact owned launch".to_owned());
        }
        if operation["result"].get("details").is_some() || operation["result"]["provider_auth"].is_object() {
            return Err("public operation.get exposed private outcome details".to_owned());
        }
        let public_bytes = serde_json::to_vec(&operation).map_err(display)?;
        let evidence_bytes = serde_json::to_vec(&evidence).map_err(display)?;
        for bytes in [&public_bytes, &evidence_bytes] {
            if contains_bytes(bytes, provider_key.as_bytes()) || contains_bytes(bytes, auth_source.to_string_lossy().as_bytes()) {
                return Err("provider secret or auth source path entered public/evidence output".to_owned());
            }
        }
        evidence["operation_readback"] = operation;
        Ok((operator, evidence))
    }.await;
    // EOF is local to this host child; it invokes production graceful shutdown.
    drop(host_stdin);
    let stopped = tokio::time::timeout(Duration::from_secs(60), host.wait()).await;
    let host_gone = matches!(&stopped, Ok(Ok(_)));
    let host_ok = matches!(&stopped, Ok(Ok(status)) if status.success());
    let mut errors = Vec::new();
    if !host_ok {
        errors.push("public host child did not confirm successful graceful shutdown".to_owned());
    }
    let mut case_ok = false;
    match execution {
        Ok((operator, mut evidence)) => {
            let ipc_listener_gone = swarm_client::ModuleLink::connect(
                &host_data_dir,
                &operator,
                &swarm_client::IpcConfig::default(),
            )
            .await
            .is_err();
            if !ipc_listener_gone {
                errors.push("host IPC listener still accepts authenticated connections".to_owned());
            }
            evidence["ipc_listener_gone"] = json!(ipc_listener_gone);
            case_ok = evidence["child_released"] == true
                && evidence["connection_absent"] == true
                && ipc_listener_gone;
            if !case_ok {
                errors.push("owned child cleanup evidence is incomplete".to_owned());
            }
        }
        Err(error) => errors.push(error),
    }
    // Never remove custody/receipts after an unknown host/native teardown.
    if host_gone && case_ok && errors.is_empty() {
        fs::remove_file(&auth_source).map_err(display)?;
        fs::remove_dir_all(&scratch).map_err(display)?;
        Ok(())
    } else {
        Err(format!(
            "APP320 composition or cleanup failed; private scratch preserved: {}",
            errors.join("; ")
        ))
    }
}

async fn call_host(
    root: &Path,
    credential: &Credential,
    method: &str,
    params: Value,
) -> FixtureResult<Value> {
    swarm_client::call(
        root,
        credential,
        method,
        params,
        &swarm_client::IpcConfig::default(),
    )
    .await
    .map_err(display)
}

fn configured_binary(variable: &str) -> FixtureResult<PathBuf> {
    let path = std::env::var_os(variable)
        .map(PathBuf::from)
        .ok_or_else(|| format!("manager must supply the built {variable} executable"))?;
    if !path.is_absolute() {
        return Err(format!("{variable} must be absolute"));
    }
    let path = fs::canonicalize(path).map_err(display)?;
    if !fs::metadata(&path).map_err(display)?.is_file() {
        return Err(format!("{variable} must be a regular executable"));
    }
    Ok(path)
}

fn fixture_git_executable() -> FixtureResult<PathBuf> {
    let name = format!("git{}", std::env::consts::EXE_SUFFIX);
    std::env::split_paths(
        &std::env::var_os("PATH").ok_or_else(|| "Git PATH is unavailable".to_owned())?,
    )
    .map(|directory| directory.join(&name))
    .find(|path| path.is_file())
    .ok_or_else(|| "Git executable is unavailable for local workspace setup".to_owned())?
    .canonicalize()
    .map_err(display)
}

fn fixture_git(git: &Path, repository: &Path, hooks: &Path, args: &[&str]) -> FixtureResult<()> {
    let output = Command::new(git)
        .arg("-c")
        .arg(format!("core.hooksPath={}", hooks.display()))
        .arg("-C")
        .arg(repository)
        .args(args)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(display)?;
    if !output.success() {
        return Err("isolated local fixture Git operation failed".to_owned());
    }
    Ok(())
}

fn write_fixture_descriptor(
    install_root: &Path,
    executable: &Path,
    case_file: &Path,
) -> FixtureResult<PathBuf> {
    let directory = install_root.join("adapter");
    fs::create_dir(&directory).map_err(display)?;
    let descriptor_file = directory.join("module-descriptor.json");
    let executable_sha256 = sha256_file(executable)?;
    let mut descriptor: Value =
        serde_json::from_str(include_str!("../registration/descriptor.template.json"))
            .map_err(display)?;
    descriptor["enabled"] = json!(true);
    descriptor["launch"]["executable"] = json!(executable);
    descriptor["launch"]["executable_sha256"] = json!(executable_sha256);
    descriptor["launch"]["argv"] = json!([
        {"kind":"literal","value":"--exact"},{"kind":"literal","value":ADAPTER_CHILD},
        {"kind":"literal","value":"--ignored"},{"kind":"literal","value":"--nocapture"},
        {"kind":"literal","value":"--skip"},LaunchValue::ModuleHostConfigPath { schema_version: 1 }
    ]);
    descriptor["launch"]["environment"] = json!([
        {"name":"ELIOT_APP320_CASE_FILE","value":{"kind":"literal","value":case_file}}
    ]);
    let typed: ModuleDescriptor = serde_json::from_value(descriptor.clone()).map_err(display)?;
    typed.validate().map_err(display)?;
    let bytes = serde_json::to_vec(&descriptor).map_err(display)?;
    fs::write(&descriptor_file, &bytes).map_err(display)?;
    // Fixture install consistency only. The production supervisor independently
    // checks both real copied images and descriptor bytes; this is not a product
    // install/native dependency qualification receipt.
    fs::write(directory.join("install-receipt.json"), serde_json::to_vec(&json!({
        "schema_version":1,"format":"eliot.module_install_receipt.v1",
        "module_id":typed.module_id,"artifact_id":typed.artifact.artifact_id,
        "version":typed.artifact.version,"build_id":typed.artifact.build_id,
        "source_file":std::env::current_exe().map_err(display)?,"installed_file":executable,
        "source_sha256":executable_sha256,"staged_sha256":executable_sha256,"installed_sha256":executable_sha256,
        "descriptor_file":descriptor_file,"descriptor_sha256":sha256(&bytes)
    })).map_err(display)?).map_err(display)?;
    Ok(descriptor_file)
}

async fn run_owned_case(host_path: &Path, case: &Value) -> FixtureResult<CaseEvidence> {
    let invalid_model_catalog = case["invalid_model_catalog"]
        .as_bool()
        .ok_or_else(|| "case omitted its scenario".to_owned())?;
    let scenario = if invalid_model_catalog {
        "post-bootstrap-preflight-schema-rejection"
    } else {
        "post-bootstrap-root-create-http-409"
    };
    let OwnedBootstrap {
        config: adapter_config,
        credential,
        worker,
        contract: claim,
    } = OwnedBootstrap::from_config_path(host_path).map_err(display)?;
    let native_options = adapter_config.native_options.clone();
    let owned_native = adapter_config
        .owned_native
        .clone()
        .ok_or_else(|| "real demand omitted fresh owned authority".to_owned())?;
    let allowed_workspace_root = PathBuf::from(
        case["workspace_root"]
            .as_str()
            .ok_or_else(|| "case omitted workspace root".to_owned())?,
    );
    let native_base = PathBuf::from(
        case["native_base"]
            .as_str()
            .ok_or_else(|| "case omitted native base".to_owned())?,
    );
    if !native_options
        .directory
        .starts_with(&allowed_workspace_root)
        || native_options.directory == allowed_workspace_root
        || native_options.model.id != "fixture-model"
        || native_options.model.provider_id != "openai"
        || native_options.model.variant != "fixture-variant"
        || owned_native.state_root != native_base.join("launches").join(&owned_native.owner_nonce)
    {
        return Err(
            "protected demand did not retain its exact admitted workspace/owner scope".to_owned(),
        );
    }
    let auth_source = owned_native
        .provider_auth
        .as_ref()
        .ok_or_else(|| "protected demand omitted auth source".to_owned())?
        .source_file
        .clone();
    let source: Value =
        serde_json::from_slice(&fs::read(&auth_source).map_err(display)?).map_err(display)?;
    let provider_key = source["openai"]["key"]
        .as_str()
        .ok_or_else(|| "private synthetic auth source omitted its key".to_owned())?
        .to_owned();
    let binding_id = adapter_config.binding_id.clone();
    let generation = adapter_config.generation;
    let journal_dir = adapter_config.state_dir.clone();
    let route_sha256 =
        digest_json(&serde_json::to_value(&native_options).map_err(display)?).map_err(display)?;
    let journal = Journal::open(
        &journal_dir,
        &binding_id,
        generation,
        &native_options.scope_key(),
        &route_sha256,
    )
    .map_err(display)?;
    let mut native_owner = NativeOwnerController::new(adapter_config.owned_native.clone());
    let observer = Arc::new(Mutex::new(Vec::<Value>::new()));
    let boot_id = worker.boot_id.clone();
    let owner = &worker.owner_record;
    let session = HostSession {
        config: &adapter_config,
        credential: &credential,
        journal: &journal,
        claim: &claim,
        boot_id: boot_id.clone(),
        hello_base: json!({"boot_id":boot_id,"module_artifact_id":ARTIFACT_ID,"native_ready":false,
            "managed_owner":{"token":owner["token"],"process":owner["process"]}}),
        root_hint: Mutex::new(None),
        ipc_link: AsyncMutex::new(None),
        outcome_observer: Some(Arc::clone(&observer)),
        retention_retry_pending: AtomicBool::new(false),
    };
    let execution = async {
        session.hello_retry().await.map_err(display)?;
        let command: RuntimeCommand = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let next = session.call("module.next", json!({})).await.map_err(display)?;
                if let Some(command) = next.get("command").filter(|value| !value.is_null()) {
                    break serde_json::from_value(command.clone()).map_err(display);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.map_err(|_| "actual module.next did not supply the admitted command".to_owned())??;
        if command.method != "agent.open" || command.binding_id != binding_id || command.generation != generation
            || command.route["native_options"]["directory"] != json!(native_options.directory) {
            return Err("actual module.next command differs from protected owned demand".to_owned());
        }
        verify_command_scope(&adapter_config, &command, &claim).map_err(display)?;
        let operation_id = command.operation_id.clone();
        let admission_path = PathBuf::from(case["admission_file"].as_str().ok_or_else(|| "case omitted admission handshake path".to_owned())?);
        let execute_path = PathBuf::from(case["execute_file"].as_str().ok_or_else(|| "case omitted execute handshake path".to_owned())?);
        fs::write(admission_path, serde_json::to_vec(&json!({
            "operation_id":operation_id,"binding_id":binding_id,"generation":generation,
            "owner_nonce":owned_native.owner_nonce,"workspace_directory":native_options.directory
        })).map_err(display)?).map_err(display)?;
        tokio::time::timeout(Duration::from_secs(30), async {
            while !execute_path.is_file() { tokio::time::sleep(Duration::from_millis(50)).await; }
        }).await.map_err(|_| "parent did not confirm actual public admission readback".to_owned())?;
        handle_command(&session, &command, &mut native_owner).await.map_err(display)?;

        let history = journal.load(&operation_id).map_err(display)?;
        let outcome = history
            .outcome
            .clone()
            .ok_or_else(|| "Journal did not retain the RuntimeOutcome".to_owned())?;
        let expected_root = root_id(&binding_id, generation);
        let expected_model_value = serde_json::to_value(&native_options.model).map_err(display)?;
        let expected_model_digest = sha256(
            crate::native::canonical_json(&expected_model_value)
                .map_err(display)?
                .as_bytes(),
        );
        let expected_connection_metadata_digest = sha256(
            crate::native::canonical_json(&json!({"credential_id":"fixture-credential"}))
                .map_err(display)?
                .as_bytes(),
        );
        let ready = &outcome["details"]["owned_service_ready"];
        let ready_pid = ready["process"]["pid"]
            .as_u64()
            .ok_or_else(|| format!("{scenario}: outcome omitted the actual owned child PID"))?;
        let ready_birth_token = ready["process"]["birth_token"]
            .as_str()
            .ok_or_else(|| format!("{scenario}: outcome omitted the actual owner birth token"))?;
        let ready_service_id = ready["service_id"]
            .as_str()
            .ok_or_else(|| format!("{scenario}: outcome omitted the actual service identity"))?;
        let ready_service_version = ready["service_version"]
            .as_str()
            .ok_or_else(|| format!("{scenario}: outcome omitted the actual service version"))?;
        let ready_owner_nonce = ready["owner_nonce"]
            .as_str()
            .ok_or_else(|| format!("{scenario}: outcome omitted the actual owner nonce"))?;
        if ready_service_id != native_options.service_id.as_str()
            || ready_owner_nonce != owned_native.owner_nonce.as_str()
        {
            return Err(format!("{scenario}: ready receipt differs from the admitted owned route"));
        }
        let expected_provider_auth = json!({
            "status":"stored_unverified",
            "provider_id":"openai",
            "credential_ref":"credential:app320-fixture",
            "model_digest":expected_model_digest,
            "service_id":ready_service_id,
            "service_version":ready_service_version,
            "owner_nonce_digest":sha256(ready_owner_nonce.as_bytes()),
            "process":{
                "pid":ready_pid,
                "birth_token":ready_birth_token
            },
            "integration_id":"openai",
            "key_method":"key",
            "connection_metadata_digest":expected_connection_metadata_digest
        });
        if outcome["outcome"] != "rejected"
            || outcome["details"]["provider_auth"] != expected_provider_auth
            || outcome["details"]["owned_service_ready_only"] != true
            || outcome["details"]["owned_service_ready"]["owner_nonce"] != owned_native.owner_nonce
        {
            return Err(format!(
                "{scenario}: Journal outcome changed or omitted fields from the exact provider-auth proof"
            ));
        }
        if invalid_model_catalog {
            if !outcome["native_root_id"].is_null()
                || outcome["details"]["diagnostic_code"] != "NATIVE_MODEL_SCHEMA"
                || outcome["details"]["native_replay"] != false
            {
                return Err("preflight rejection changed its root or diagnostic semantics".to_owned());
            }
        } else if outcome["native_root_id"] != expected_root
            || outcome["details"]["diagnostic_code"] != "NATIVE_REJECTED"
            || outcome["details"]["native_http_failure"]["status"] != 409
            || outcome["details"]["native_replay"] != false
        {
            return Err("root-create rejection lost its root or rejection semantics".to_owned());
        }

        let serialized_outcome = serde_json::to_vec(&outcome).map_err(display)?;
        let journal_bytes = read_tree_bytes(&journal_dir)?;
        let source_text = auth_source.to_string_lossy();
        if contains_bytes(&serialized_outcome, provider_key.as_bytes())
            || contains_bytes(&serialized_outcome, source_text.as_bytes())
            || contains_bytes(&journal_bytes, provider_key.as_bytes())
            || contains_bytes(&journal_bytes, source_text.as_bytes())
        {
            return Err("provider key or source path entered durable outcome evidence".to_owned());
        }

        let observed_outcomes = observer
            .lock()
            .map_err(|_| "module.outcome observer mutex was poisoned".to_owned())?
            .clone();
        if observed_outcomes.len() != 1 || observed_outcomes[0] != outcome {
            return Err("production HostSession did not send the exact outcome once".to_owned());
        }
        let serialized_ipc = serde_json::to_vec(&observed_outcomes[0]).map_err(display)?;
        if contains_bytes(&serialized_ipc, provider_key.as_bytes())
            || contains_bytes(&serialized_ipc, source_text.as_bytes())
        {
            return Err("provider key or source path entered module.outcome IPC".to_owned());
        }

        let acknowledged_sha256 = history
            .acknowledged_sha256
            .clone()
            .ok_or_else(|| "Journal did not retain the Store outcome ACK".to_owned())?;
        let expected_ack = digest_json(&outcome).map_err(display)?;
        if acknowledged_sha256 != expected_ack {
            return Err("Journal ACK digest differs from the saved RuntimeOutcome".to_owned());
        }
        let pending_count = journal.pending_items().map_err(display)?.len();
        if pending_count != 0 {
            return Err("production Store ACK did not drain the real Journal outbox".to_owned());
        }
        Ok(CaseEvidence {
            scenario,
            operation_id,
            binding_id,
            generation,
            workspace_directory: native_options.directory.clone(),
            owner_nonce: owned_native.owner_nonce.clone(),
            outcome,
            operation_readback: Value::Null,
            observed_outcomes,
            acknowledged_sha256,
            pending_count,
            key_posts: 0,
            root_posts: 0,
            connection_absent: false,
            child_released: false,
            ipc_listener_gone: false,
        })
    }.await;
    let owner_stop = native_owner.shutdown().await.map_err(display);
    let child_released = !native_owner.owns_child();
    let native_state = &owned_native.state_root;
    let connection_absent = matches!(fs::symlink_metadata(&native_options.connection_file), Err(error) if error.kind() == std::io::ErrorKind::NotFound);
    let service_observation = fs::read(native_state.join("fixture-observation.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    let stop_receipt = fs::read(native_state.join("stop.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    let mut errors = Vec::new();
    if owner_stop.is_err() {
        errors.push("native owner shutdown was not confirmed".to_owned());
    }
    if !child_released {
        errors.push("native owner still retains its exact child".to_owned());
    }
    match execution {
        Ok(mut evidence) => {
            let key_posts = service_observation
                .as_ref()
                .and_then(|value| value["provider_key_posts"].as_u64())
                .unwrap_or_default();
            let root_posts = service_observation
                .as_ref()
                .and_then(|value| value["root_create_posts"].as_u64())
                .unwrap_or_default();
            let expected_root_posts = u64::from(!invalid_model_catalog);
            if key_posts != 1 || root_posts != expected_root_posts {
                errors.push("native HTTP effect counts differ from the scenario".to_owned());
            }
            if !connection_absent
                || stop_receipt.as_ref().is_none_or(|receipt| {
                    receipt["status"] != "stopped"
                        || receipt["owner_nonce"] != owned_native.owner_nonce
                        || receipt["pid"].as_u64()
                            != evidence.outcome["details"]["owned_service_ready"]["process"]["pid"]
                                .as_u64()
                        || receipt["connection_absent"] != true
                        || receipt["process_exit_code"] != 0
                        || receipt["provider_key_posts"] != 1
                        || receipt["root_create_posts"] != expected_root_posts
                })
            {
                errors
                    .push("exact native stop receipt/connection cleanup is incomplete".to_owned());
            }
            if let Some(observation) = service_observation {
                let endpoint = observation["endpoint"].as_str().unwrap_or_default();
                if stop_receipt
                    .as_ref()
                    .is_none_or(|receipt| receipt["endpoint"] != endpoint)
                {
                    errors.push("native stop receipt names another endpoint".to_owned());
                }
                if !endpoint_listener_gone(endpoint).await {
                    errors.push("native HTTP endpoint still accepts connections".to_owned());
                }
            } else {
                errors.push("native HTTP count-only stop evidence is missing".to_owned());
            }
            evidence.key_posts = key_posts;
            evidence.root_posts = root_posts;
            evidence.connection_absent = connection_absent;
            evidence.child_released = child_released;
            // Parent checks authenticated IPC absence only after graceful host EOF.
            if errors.is_empty() {
                Ok(evidence)
            } else {
                Err(errors.join("; "))
            }
        }
        Err(error) => {
            errors.push(error);
            Err(errors.join("; "))
        }
    }
}

fn unique_directory(prefix: &str) -> FixtureResult<PathBuf> {
    let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&path).map_err(display)?;
    fs::canonicalize(path).map_err(display)
}

fn configured_bun() -> FixtureResult<PathBuf> {
    let path = std::env::var_os("ELIOT_OPENCODE_TEST_BUN")
        .map(PathBuf::from)
        .ok_or_else(|| {
            "set ELIOT_OPENCODE_TEST_BUN to the exact Bun 1.4.0 executable before running this fixture"
                .to_owned()
        })?;
    let path = fs::canonicalize(path).map_err(display)?;
    let metadata = fs::symlink_metadata(&path).map_err(display)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err("ELIOT_OPENCODE_TEST_BUN must resolve to a regular executable".to_owned());
    }
    Ok(path)
}

fn write_installed_closure(install_root: &Path, server_source: &str) -> FixtureResult<String> {
    let resource_root = install_root.join("resources/modules/opencode");
    fs::create_dir_all(resource_root.join("node_modules")).map_err(display)?;
    let binary_name = format!("swarm-kernel-host{}", std::env::consts::EXE_SUFFIX);
    let executable = std::env::current_exe().map_err(display)?;
    let executable_path = install_root.join(&binary_name);
    fs::copy(executable, &executable_path).map_err(display)?;
    let executable_sha256 = sha256_file(&executable_path)?;
    let executable_bytes = fs::metadata(&executable_path).map_err(display)?.len();

    let fixture_node_module = b"module.exports = 'app320-fixture';\n";
    fs::write(
        resource_root.join("node_modules/fixture.js"),
        fixture_node_module,
    )
    .map_err(display)?;
    let node_file_sha256 = sha256(fixture_node_module);
    let node_file_bytes = fixture_node_module.len() as u64;
    let node_version = "22.0.0";
    let node_executable_sha256 = "d".repeat(64);
    let node_file = "node.exe";
    let npm_version = "10.0.0";
    let npm_cli_sha256 = "e".repeat(64);
    let npm_cli_file = "npm-cli.js";
    let tree_sha256 = dependency_tree_digest("fixture.js", node_file_bytes, &node_file_sha256);

    let resources: [(&str, &str, &[u8]); 5] = [
        ("serve.mjs", "server_program", server_source.as_bytes()),
        ("native-mcp-proof.mjs", "plugin_module", PLUGIN_MODULE),
        ("index.mjs", "plugin_entry", PLUGIN_ENTRY),
        ("package.json", "dependency_manifest", PACKAGE_JSON),
        ("package-lock.json", "dependency_lock", PACKAGE_LOCK),
    ];
    let mut resource_rows = Vec::with_capacity(resources.len());
    for (name, role, bytes) in resources {
        fs::write(resource_root.join(name), bytes).map_err(display)?;
        let hash = sha256(bytes);
        resource_rows.push(json!({
            "path":name,
            "file":format!("resources/modules/opencode/{name}"),
            "role":role,
            "bytes":bytes.len(),
            "source_sha256":hash,
            "artifact_sha256":hash,
        }));
    }
    let package_json_sha256 = sha256(PACKAGE_JSON);
    let package_lock_sha256 = sha256(PACKAGE_LOCK);
    let file_count = 1_u64;
    let entry_count = 1_u64;
    let total_bytes = node_file_bytes;
    let closure = json!({
        "schema_version":1,
        "format":"eliot.opencode_dependency_closure.v1",
        "coordinate":"swarm-kernel-host-opencode-resources",
        "package_json_sha256":package_json_sha256.clone(),
        "package_lock_sha256":package_lock_sha256.clone(),
        "tree_sha256":tree_sha256.clone(),
        "node":{
            "version":node_version,
            "executable_sha256":node_executable_sha256.clone(),
            "file":node_file,
        },
        "npm":{
            "version":npm_version,
            "cli_sha256":npm_cli_sha256.clone(),
            "file":npm_cli_file,
        },
        "files":[{
            "path":"fixture.js",
            "bytes":node_file_bytes,
            "sha256":node_file_sha256,
        }],
        "file_count":file_count,
        "entry_count":entry_count,
        "total_bytes":total_bytes,
    });
    let closure_bytes = serde_json::to_vec(&closure).map_err(display)?;
    fs::write(
        resource_root.join("dependency-closure.json"),
        &closure_bytes,
    )
    .map_err(display)?;
    let closure_sha256 = sha256(&closure_bytes);
    let closure_pin = json!({
        "schema_version":1,
        "manifest_file":"dependency-closure.json",
        "coordinate":"swarm-kernel-host-opencode-resources",
        "manifest_sha256":closure_sha256,
        "package_json_sha256":package_json_sha256,
        "package_lock_sha256":package_lock_sha256,
        "tree_sha256":tree_sha256,
        "node_version":node_version,
        "node_executable_sha256":node_executable_sha256,
        "node_file":node_file,
        "npm_version":npm_version,
        "npm_cli_sha256":npm_cli_sha256,
        "npm_cli_file":npm_cli_file,
        "entry_count":entry_count,
        "file_count":file_count,
        "total_bytes":total_bytes,
    });
    let dependency_policy = json!({
        "node_modules":"installer_generated_locked_closure",
        "install_command":"npm ci --ignore-scripts",
        "closure_manifest_file":"dependency-closure.json",
        "manager":"npm",
        "output_directory":"node_modules",
        "timeout_seconds":1800,
        "arguments":["ci","--ignore-scripts","--no-audit","--no-fund","--no-progress","--loglevel=error"],
        "max_files":100000,
        "max_entries":200000,
        "max_total_bytes":1073741824_u64,
        "max_manifest_bytes":33554432,
    });
    let installed_resources = json!({
        "coordinate":"swarm-kernel-host-opencode-resources",
        "installed_relative_root":"resources/modules/opencode",
        "repository_relative_root":"modules/opencode",
        "dependency_policy":dependency_policy,
        "files":resource_rows.clone(),
    });
    let build_manifest = json!({
        "schema_version":1,
        "format":"eliot.module_build_manifest.v1",
        "build":{
            "package_name":"swarm-kernel-host",
            "role":"host_runtime",
            "package_manifest":"crates/swarm-kernel-host/Cargo.toml",
            "profile":"release",
            "binary_targets":["swarm-kernel-host"],
        },
        "artifacts":[{
            "target_name":"swarm-kernel-host",
            "file":format!("bin/{binary_name}"),
            "source_sha256":executable_sha256.clone(),
            "artifact_sha256":executable_sha256.clone(),
            "bytes":executable_bytes,
        }],
        "source":{
            "package_manifest_path":"crates/swarm-kernel-host/Cargo.toml",
            "package_manifest_sha256":"a".repeat(64),
            "workspace_cargo_toml_sha256":"b".repeat(64),
            "cargo_toml_sha256":"b".repeat(64),
        },
        "resources":installed_resources,
    });
    let build_manifest_bytes = serde_json::to_vec(&build_manifest).map_err(display)?;
    let build_manifest_sha256 = sha256(&build_manifest_bytes);
    fs::write(
        install_root.join("swarm-kernel-host.build-manifest.json"),
        &build_manifest_bytes,
    )
    .map_err(display)?;
    let install_receipt = json!({
        "schema_version":1,
        "format":"eliot.frontend_install_receipt.v1",
        "role":"host_runtime",
        "package_name":"swarm-kernel-host",
        "package_manifest":"crates/swarm-kernel-host/Cargo.toml",
        "binary_target":"swarm-kernel-host",
        "binary_file":binary_name,
        "executable_sha256":executable_sha256,
        "build_manifest_file":"swarm-kernel-host.build-manifest.json",
        "build_manifest_sha256":build_manifest_sha256,
        "package_manifest_sha256":"a".repeat(64),
        "workspace_cargo_toml_sha256":"b".repeat(64),
        "cargo_toml_sha256":"b".repeat(64),
        "resources":{
            "coordinate":"swarm-kernel-host-opencode-resources",
            "installed_relative_root":"resources/modules/opencode",
            "files":resource_rows,
            "dependency_closure":closure_pin,
        },
    });
    let receipt_bytes = serde_json::to_vec(&install_receipt).map_err(display)?;
    fs::write(
        install_root.join("swarm-kernel-host.install-receipt.json"),
        receipt_bytes,
    )
    .map_err(display)?;
    Ok(sha256(server_source.as_bytes()))
}

fn test_server_source(invalid_model_catalog: bool) -> String {
    r#"import { createHash } from "node:crypto";
import { createServer } from "node:http";
import { readFile, unlink, writeFile } from "node:fs/promises";
import path from "node:path";

const INVALID_MODEL_CATALOG = __APP320_INVALID_MODEL__;
const arg = (name) => {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : undefined;
};
if (Bun.version !== "1.4.0") throw new Error("APP320 fixture requires Bun 1.4.0");
const stateRoot = path.resolve(arg("--state-root"));
const passwordFile = path.resolve(arg("--password-file"));
const ownerNonce = arg("--owner-nonce");
const port = Number(arg("--port"));
const workspace = path.resolve(arg("--workspace-directory"));
const connectionFile = path.join(stateRoot, "connection.json");
const ownerFile = path.join(stateRoot, "owner.json");
const stopFile = path.join(stateRoot, "stop.json");
const observationFile = path.join(stateRoot, "fixture-observation.json");
const password = (await readFile(passwordFile, "utf8")).replace(/[\r\n]+$/, "");
let providerKeyPosts = 0;
let rootCreatePosts = 0;
const routes = [];
let endpoint = "";
const expectedAuthorization = `Basic ${Buffer.from(`opencode:${password}`, "utf8").toString("base64")}`;
const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url, endpoint || "http://127.0.0.1");
    routes.push(`${request.method} ${url.pathname}`);
    if (request.headers.authorization !== expectedAuthorization) {
      response.writeHead(401).end();
      return;
    }
    const directory = url.searchParams.get("location[directory]") || workspace;
    const send = (status, value) => {
      response.writeHead(status, { "content-type": "application/json" });
      response.end(JSON.stringify(value));
    };
    if (request.method === "GET" && url.pathname === "/api/info") {
      send(200, {
        version: "2.0.7",
        pid: process.pid,
        urls: [endpoint],
        paths: { tmp: path.join(stateRoot, "tmp") },
      });
      return;
    }
    if (request.method === "GET" && url.pathname === "/api/integration") {
      send(200, {
        location: { directory },
        data: [{
          id: "openai",
          methods: [{ type: "key" }],
          connections: providerKeyPosts === 0 ? [] : [{ type: "credential", id: "fixture-credential" }],
        }],
      });
      return;
    }
    if (request.method === "GET" && url.pathname === "/api/model") {
      send(200, {
        location: { directory },
        data: INVALID_MODEL_CATALOG
          ? [{ id: "fixture-model", providerID: "openai", enabled: true, variants: { malformed: true } }]
          : [{ id: "fixture-model", providerID: "openai", enabled: true, variants: [{ id: "fixture-variant" }] }],
      });
      return;
    }
    if (request.method === "GET" && url.pathname === "/api/location") {
      send(200, { directory, project: { id: "app320-fixture-project" } });
      return;
    }
    if (request.method === "POST" && url.pathname === "/api/integration/openai/connect/key") {
      for await (const _chunk of request) { /* consume without retaining key bytes */ }
      providerKeyPosts += 1;
      send(200, null);
      return;
    }
    if (request.method === "POST" && url.pathname === "/api/session") {
      for await (const _chunk of request) { /* consume without retaining request data */ }
      rootCreatePosts += 1;
      send(409, { message: "APP320 fixture root creation rejected" });
      return;
    }
    for await (const _chunk of request) { /* drain unsupported request bodies */ }
    send(404, { message: "APP320 fixture route not found" });
  } catch {
    if (!response.headersSent) response.writeHead(500);
    response.end();
  }
});
server.keepAliveTimeout = 500;
await new Promise((resolve, reject) => {
  server.once("error", reject);
  server.listen(port, "127.0.0.1", resolve);
});
const address = server.address();
endpoint = `http://127.0.0.1:${address.port}`;
const connection = {
  schema_version: 1,
  endpoint,
  pid: process.pid,
  username: "opencode",
  password,
};
const connectionBytes = `${JSON.stringify(connection, null, 2)}\n`;
const connectionSha256 = createHash("sha256").update(connectionBytes).digest("hex");
await writeFile(connectionFile, connectionBytes, { mode: 0o600 });
const owner = {
  schema_version: 1,
  status: "ready",
  owner_nonce: ownerNonce,
  runtime: "bun",
  runtime_version: Bun.version,
  native_server: "@opencode/server",
  native_server_version: "2.0.7",
  pid: process.pid,
  native_reported_pid: process.pid,
  endpoint,
  state_root: stateRoot,
  connection_file: connectionFile,
  connection_sha256: connectionSha256,
};
await writeFile(ownerFile, `${JSON.stringify(owner, null, 2)}\n`, { mode: 0o600 });
let stopping = false;
function stopFromOwnerEnd() {
  if (stopping) return;
  stopping = true;
  server.close(async () => {
    try {
      server.closeAllConnections?.();
      await unlink(connectionFile);
      owner.status = "stopped";
      owner.connection_absent = true;
      owner.process_exit_code = 0;
      await writeFile(ownerFile, `${JSON.stringify(owner, null, 2)}\n`, { mode: 0o600 });
      const observation = {
        schema_version: 1,
        provider_key_posts: providerKeyPosts,
        root_create_posts: rootCreatePosts,
        route_identities: routes,
        endpoint,
      };
      await writeFile(observationFile, `${JSON.stringify(observation, null, 2)}\n`, { mode: 0o600 });
      await writeFile(stopFile, `${JSON.stringify({
        schema_version: 1,
        status: "stopped",
        owner_nonce: ownerNonce,
        pid: process.pid,
        connection_absent: true,
        process_exit_code: 0,
        provider_key_posts: providerKeyPosts,
        root_create_posts: rootCreatePosts,
        endpoint,
      }, null, 2)}\n`, { mode: 0o600 });
    } catch {
      process.exitCode = 1;
    }
  });
  server.closeAllConnections?.();
}
process.stdin.once("end", stopFromOwnerEnd);
process.stdin.resume();
"#.replace(
        "__APP320_INVALID_MODEL__",
        if invalid_model_catalog { "true" } else { "false" },
    )
}

fn dependency_tree_digest(path: &str, bytes: u64, hash: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(path.as_bytes());
    hasher.update([0]);
    hasher.update(bytes.to_string().as_bytes());
    hasher.update([0]);
    hasher.update(hash.as_bytes());
    hasher.update(b"\n");
    format!("{:x}", hasher.finalize())
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> FixtureResult<String> {
    fs::read(path).map(|bytes| sha256(&bytes)).map_err(display)
}

fn read_tree_bytes(root: &Path) -> FixtureResult<Vec<u8>> {
    let mut files = Vec::new();
    collect_files(root, &mut files)?;
    files.sort();
    let mut contents = Vec::new();
    for path in files {
        contents.extend(fs::read(&path).map_err(display)?);
        contents.push(0);
    }
    Ok(contents)
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>) -> FixtureResult<()> {
    for entry in fs::read_dir(root).map_err(display)? {
        let path = entry.map_err(display)?.path();
        let metadata = fs::symlink_metadata(&path).map_err(display)?;
        if metadata.is_dir() {
            collect_files(&path, files)?;
        } else if metadata.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

async fn endpoint_listener_gone(endpoint: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(endpoint) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    let Some(port) = url.port_or_known_default() else {
        return false;
    };
    tokio::net::TcpStream::connect((host, port)).await.is_err()
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}
